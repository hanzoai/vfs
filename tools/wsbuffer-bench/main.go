// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command wsbuffer-bench runs the reproducible WSBuffer (HIP-1344, Refs #58)
// comparative benchmark suite against real on-disk OS journal segment files.
//
// It compares three strategies on identical workloads:
//   - baseline_journal_main       (unmodified journal.Files / journal.Open)
//   - always_copy_buffered_writer (stages every write through the buffer, including large aligned)
//   - wsbuffer_adaptive_bypass    (absorbs small unaligned writes, zero-copy bypasses large aligned)
//
// Every run captures real wall-clock throughput, syscall.Getrusage CPU time,
// Linux /proc/self/io (syscw, wchar, write_bytes), os.Stat file sizes, and
// exact page-level physical write amplification.
//
//	go run ./tools/wsbuffer-bench
//	go run ./tools/wsbuffer-bench -json
//	go run ./tools/wsbuffer-bench -out docs/research/hip-1344-wsbuffer/raw_results.json
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanzoai/vfs/journal"
)

type procIO struct {
	Rchar      uint64 `json:"rchar"`
	Wchar      uint64 `json:"wchar"`
	Syscr      uint64 `json:"syscr"`
	Syscw      uint64 `json:"syscw"`
	ReadBytes  uint64 `json:"read_bytes"`
	WriteBytes uint64 `json:"write_bytes"`
}

func readProcIO() (procIO, bool) {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return procIO{}, false
	}
	defer f.Close()

	var ioStats procIO
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.SplitN(sc.Text(), ":", 2)
		if len(parts) != 2 {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			continue
		}
		switch strings.TrimSpace(parts[0]) {
		case "rchar":
			ioStats.Rchar = v
		case "wchar":
			ioStats.Wchar = v
		case "syscr":
			ioStats.Syscr = v
		case "syscw":
			ioStats.Syscw = v
		case "read_bytes":
			ioStats.ReadBytes = v
		case "write_bytes":
			ioStats.WriteBytes = v
		}
	}
	return ioStats, true
}

func diffProcIO(a, b procIO) procIO {
	sub := func(x, y uint64) uint64 {
		if y >= x {
			return y - x
		}
		return 0
	}
	return procIO{
		Rchar:      sub(a.Rchar, b.Rchar),
		Wchar:      sub(a.Wchar, b.Wchar),
		Syscr:      sub(a.Syscr, b.Syscr),
		Syscw:      sub(a.Syscw, b.Syscw),
		ReadBytes:  sub(a.ReadBytes, b.ReadBytes),
		WriteBytes: sub(a.WriteBytes, b.WriteBytes),
	}
}

type cpuUsage struct {
	user time.Duration
	sys  time.Duration
}

func readCPU() cpuUsage {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return cpuUsage{}
	}
	return cpuUsage{
		user: time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond,
		sys:  time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond,
	}
}

type obsDisk struct {
	journal.Disk
	pageSize      int64
	writeOps      atomic.Uint64
	writeBytes    atomic.Uint64
	physicalPages atomic.Uint64
	syncOps       atomic.Uint64
}

func newObsDisk(d journal.Disk, pageSize int) *obsDisk {
	return &obsDisk{Disk: d, pageSize: int64(pageSize)}
}

func (d *obsDisk) Create(name string, size int64) (journal.File, error) {
	f, err := d.Disk.Create(name, size)
	if err != nil {
		return nil, err
	}
	return &obsFile{File: f, d: d}, nil
}

func (d *obsDisk) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	if err != nil {
		return nil, err
	}
	return &obsFile{File: f, d: d}, nil
}

func (d *obsDisk) Warnings() []string {
	if w, ok := d.Disk.(interface{ Warnings() []string }); ok {
		return w.Warnings()
	}
	return nil
}

type obsFile struct {
	journal.File
	d *obsDisk
}

func (f *obsFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	if n > 0 {
		firstPg := off / f.d.pageSize
		lastPg := (off + int64(n) - 1) / f.d.pageSize
		f.d.writeOps.Add(1)
		f.d.writeBytes.Add(uint64(n))
		f.d.physicalPages.Add(uint64(lastPg - firstPg + 1))
	}
	return n, err
}

func (f *obsFile) Sync() error {
	f.d.syncOps.Add(1)
	return f.File.Sync()
}

type Result struct {
	Workload          string  `json:"workload"`
	Strategy          string  `json:"strategy"`
	CapacityBytes     int     `json:"capacity_bytes"`
	LogicalOps        uint64  `json:"logical_ops"`
	LogicalBytes      uint64  `json:"logical_bytes"`
	PhysicalWriteOps  uint64  `json:"physical_write_ops"`
	PhysicalBytes     uint64  `json:"physical_write_bytes"`
	PhysicalPages     uint64  `json:"physical_pages"`
	PhysicalWriteAmp  float64 `json:"physical_write_amp"`
	DirectBypassOps   uint64  `json:"direct_bypass_ops"`
	DirectBypassBytes uint64  `json:"direct_bypass_bytes"`
	ScrapAbsorbedOps  uint64  `json:"scrap_absorbed_ops"`
	ScrapCopyBytes    uint64  `json:"scrap_copy_bytes"`
	CoalescedHits     uint64  `json:"coalesced_hits"`
	FlushOps          uint64  `json:"flush_ops"`
	SyncOps           uint64  `json:"sync_ops"`
	MaxDirtyBytes     uint64  `json:"max_dirty_bytes"`
	WallMs            float64 `json:"wall_ms"`
	CPUUserMs         float64 `json:"cpu_user_ms"`
	CPUSysMs          float64 `json:"cpu_sys_ms"`
	CPUTotalMs        float64 `json:"cpu_total_ms"`
	OpsPerSec         float64 `json:"ops_per_sec"`
	ThroughputMBS     float64 `json:"throughput_mb_s"`
	ProcIOSyscw       uint64  `json:"proc_io_syscw"`
	ProcIOWchar       uint64  `json:"proc_io_wchar"`
	ProcIOWriteBytes  uint64  `json:"proc_io_write_bytes"`
	SegmentFileBytes  int64   `json:"segment_file_bytes"`
}

type Report struct {
	Benchmark       string   `json:"benchmark"`
	HIP             string   `json:"hip"`
	Issue           string   `json:"issue"`
	PaperReference  string   `json:"paper_reference"`
	TimestampUTC    string   `json:"timestamp_utc"`
	GoVersion       string   `json:"go_version"`
	OS              string   `json:"os"`
	Arch            string   `json:"arch"`
	NumCPU          int      `json:"num_cpu"`
	PageSize        int      `json:"page_size"`
	DefaultCapBytes int      `json:"default_capacity_bytes"`
	Results         []Result `json:"results"`
	CapacitySweep   []Result `json:"capacity_sweep"`
}

type strategySpec struct {
	name string
	cfg  *journal.WSBufferConfig
}

func strategies(capBytes int) []strategySpec {
	always := journal.WSBufferConfig{
		PageSize:        4096,
		Capacity:        capBytes,
		BypassThreshold: 4096,
		AlwaysCopy:      true,
	}
	adaptive := journal.WSBufferConfig{
		PageSize:        4096,
		Capacity:        capBytes,
		BypassThreshold: 4096,
		AlwaysCopy:      false,
	}
	return []strategySpec{
		{name: "baseline_journal_main", cfg: nil},
		{name: "always_copy_buffered_writer", cfg: &always},
		{name: "wsbuffer_adaptive_bypass", cfg: &adaptive},
	}
}

func runFileWorkload(
	baseDir, workload string,
	strat strategySpec,
	segSize int64,
	fn func(f journal.File) (logicalOps, logicalBytes uint64, err error),
) (Result, error) {
	runDir := filepath.Join(baseDir, fmt.Sprintf("%s-%s-%d", workload, strat.name, time.Now().UnixNano()))
	raw, err := journal.Files(runDir)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		raw.Close()
		os.RemoveAll(runDir)
	}()

	obs := newObsDisk(raw, 4096)
	var disk journal.Disk = obs
	var wsDisk *journal.WSBufferDisk
	capBytes := 0
	if strat.cfg != nil {
		wsDisk = journal.WrapWSBuffer(obs, *strat.cfg)
		disk = wsDisk
		capBytes = wsDisk.Config().Capacity
	}

	const segName = "000000.seg"
	f, err := disk.Create(segName, segSize)
	if err != nil {
		return Result{}, err
	}
	// Reset observer counters after Create zero-fill so measurements reflect only the workload.
	obs.writeOps.Store(0)
	obs.writeBytes.Store(0)
	obs.physicalPages.Store(0)
	obs.syncOps.Store(0)

	ioBefore, hasProcIO := readProcIO()
	cpuBefore := readCPU()
	t0 := time.Now()

	logicalOps, logicalBytes, err := fn(f)
	if err != nil {
		f.Close()
		return Result{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return Result{}, err
	}

	elapsed := time.Since(t0)
	cpuAfter := readCPU()
	ioAfter, _ := readProcIO()

	var wsStats journal.WSBufferStats
	if wsDisk != nil {
		wsStats = wsDisk.Stats()
	}
	if err := f.Close(); err != nil {
		return Result{}, err
	}

	info, err := os.Stat(filepath.Join(runDir, segName))
	if err != nil {
		return Result{}, err
	}

	var ioDelta procIO
	if hasProcIO {
		ioDelta = diffProcIO(ioBefore, ioAfter)
	}

	physOps := obs.writeOps.Load()
	physBytes := obs.writeBytes.Load()
	physPages := obs.physicalPages.Load()
	syncOps := obs.syncOps.Load()
	amp := 0.0
	if logicalBytes > 0 {
		amp = (float64(physPages) * 4096.0) / float64(logicalBytes)
	}
	wallSec := elapsed.Seconds()
	opsPerSec := 0.0
	mbPerSec := 0.0
	if wallSec > 0 {
		opsPerSec = float64(logicalOps) / wallSec
		mbPerSec = float64(logicalBytes) / wallSec / 1e6
	}
	cpuUser := (cpuAfter.user - cpuBefore.user).Seconds() * 1e3
	cpuSys := (cpuAfter.sys - cpuBefore.sys).Seconds() * 1e3

	return Result{
		Workload:          workload,
		Strategy:          strat.name,
		CapacityBytes:     capBytes,
		LogicalOps:        logicalOps,
		LogicalBytes:      logicalBytes,
		PhysicalWriteOps:  physOps,
		PhysicalBytes:     physBytes,
		PhysicalPages:     physPages,
		PhysicalWriteAmp:  amp,
		DirectBypassOps:   wsStats.DirectBypassOps,
		DirectBypassBytes: wsStats.DirectBypassBytes,
		ScrapAbsorbedOps:  wsStats.ScrapAbsorbedOps,
		ScrapCopyBytes:    wsStats.ScrapCopyBytes,
		CoalescedHits:     wsStats.CoalescedHits,
		FlushOps:          wsStats.FlushOps,
		SyncOps:           syncOps,
		MaxDirtyBytes:     wsStats.MaxDirtyBytes,
		WallMs:            wallSec * 1e3,
		CPUUserMs:         cpuUser,
		CPUSysMs:          cpuSys,
		CPUTotalMs:        cpuUser + cpuSys,
		OpsPerSec:         opsPerSec,
		ThroughputMBS:     mbPerSec,
		ProcIOSyscw:       ioDelta.Syscw,
		ProcIOWchar:       ioDelta.Wchar,
		ProcIOWriteBytes:  ioDelta.WriteBytes,
		SegmentFileBytes:  info.Size(),
	}, nil
}

func runJournalEndToEnd(baseDir, workload string, strat strategySpec, numRecords int, recSize int, syncEvery int, paced bool) (Result, error) {
	runDir := filepath.Join(baseDir, fmt.Sprintf("%s-%s-%d", workload, strat.name, time.Now().UnixNano()))
	raw, err := journal.Files(runDir)
	if err != nil {
		return Result{}, err
	}
	defer os.RemoveAll(runDir)

	obs := newObsDisk(raw, 4096)
	capBytes := 0
	if strat.cfg != nil {
		capBytes = strat.cfg.Capacity
	}

	j, err := journal.Open(runDir, journal.Options{
		Capacity: 32 << 20,
		Segment:  8 << 20,
		Disk:     obs,
		WSBuffer: strat.cfg,
	})
	if err != nil {
		return Result{}, err
	}

	// Reset counters after Open's segment preallocation.
	obs.writeOps.Store(0)
	obs.writeBytes.Store(0)
	obs.physicalPages.Store(0)
	obs.syncOps.Store(0)

	payload := bytes.Repeat([]byte{0x4B}, recSize)
	ctx := context.Background()

	ioBefore, hasProcIO := readProcIO()
	cpuBefore := readCPU()
	t0 := time.Now()

	var lastSeq uint64
	for i := 0; i < numRecords; i++ {
		seq, err := j.Append(ctx, payload)
		if err != nil {
			j.Close()
			return Result{}, err
		}
		lastSeq = seq
		if paced {
			// Yield to model interactive Volume.mutate / FUSE request spacing where
			// the background journal writer drains each small record before the next arrives.
			runtime.Gosched()
		}
		if syncEvery > 0 && (i+1)%syncEvery == 0 {
			if err := j.Sync(ctx, seq); err != nil {
				j.Close()
				return Result{}, err
			}
		}
	}
	if err := j.Sync(ctx, lastSeq); err != nil {
		j.Close()
		return Result{}, err
	}

	elapsed := time.Since(t0)
	cpuAfter := readCPU()
	ioAfter, _ := readProcIO()

	jStats := j.Stats()
	wsStats := j.WSBufferStats()
	if err := j.Close(); err != nil {
		return Result{}, err
	}

	var segSize int64
	entries, _ := os.ReadDir(runDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".seg") {
			if info, err := e.Info(); err == nil {
				segSize += info.Size()
			}
		}
	}

	var ioDelta procIO
	if hasProcIO {
		ioDelta = diffProcIO(ioBefore, ioAfter)
	}
	logicalBytes := jStats.Bytes
	physOps := obs.writeOps.Load()
	physBytes := obs.writeBytes.Load()
	physPages := obs.physicalPages.Load()
	syncOps := obs.syncOps.Load()
	amp := (float64(physPages) * 4096.0) / float64(max(logicalBytes, 1))
	wallSec := elapsed.Seconds()
	cpuUser := (cpuAfter.user - cpuBefore.user).Seconds() * 1e3
	cpuSys := (cpuAfter.sys - cpuBefore.sys).Seconds() * 1e3

	return Result{
		Workload:          workload,
		Strategy:          strat.name,
		CapacityBytes:     capBytes,
		LogicalOps:        jStats.Records,
		LogicalBytes:      logicalBytes,
		PhysicalWriteOps:  physOps,
		PhysicalBytes:     physBytes,
		PhysicalPages:     physPages,
		PhysicalWriteAmp:  amp,
		DirectBypassOps:   wsStats.DirectBypassOps,
		DirectBypassBytes: wsStats.DirectBypassBytes,
		ScrapAbsorbedOps:  wsStats.ScrapAbsorbedOps,
		ScrapCopyBytes:    wsStats.ScrapCopyBytes,
		CoalescedHits:     wsStats.CoalescedHits,
		FlushOps:          wsStats.FlushOps,
		SyncOps:           syncOps,
		MaxDirtyBytes:     wsStats.MaxDirtyBytes,
		WallMs:            wallSec * 1e3,
		CPUUserMs:         cpuUser,
		CPUSysMs:          cpuSys,
		CPUTotalMs:        cpuUser + cpuSys,
		OpsPerSec:         float64(jStats.Records) / wallSec,
		ThroughputMBS:     float64(logicalBytes) / wallSec / 1e6,
		ProcIOSyscw:       ioDelta.Syscw,
		ProcIOWchar:       ioDelta.Wchar,
		ProcIOWriteBytes:  ioDelta.WriteBytes,
		SegmentFileBytes:  segSize,
	}, nil
}

func main() {
	dirFlag := flag.String("dir", "", "scratch directory on real disk (default os.MkdirTemp)")
	asJSON := flag.Bool("json", false, "emit JSON to stdout")
	outPath := flag.String("out", "", "write JSON report to file path")
	flag.Parse()

	baseDir := *dirFlag
	if baseDir == "" {
		var err error
		baseDir, err = os.MkdirTemp("", "wsbuffer-bench-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "mkdirtemp: %v\n", err)
			os.Exit(1)
		}
		defer os.RemoveAll(baseDir)
	}

	report := Report{
		Benchmark:       "hanzoai/vfs WSBuffer Partial-Write Absorption",
		HIP:             "HIP-1344",
		Issue:           "hanzoai/vfs#58",
		PaperReference:  "WSBuffer (Zhan et al., USENIX FAST '26, https://www.usenix.org/conference/fast26/presentation/zhan)",
		TimestampUTC:    time.Now().UTC().Format(time.RFC3339),
		GoVersion:       runtime.Version(),
		OS:              runtime.GOOS,
		Arch:            runtime.GOARCH,
		NumCPU:          runtime.NumCPU(),
		PageSize:        journal.DefaultWSPageSize,
		DefaultCapBytes: journal.DefaultWSBufferCapacity,
	}

	mustAdd := func(r Result, err error) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "benchmark error: %v\n", err)
			os.Exit(1)
		}
		report.Results = append(report.Results, r)
	}

	// 1. Small unaligned micro-writes: 128 B, 512 B, 1 KiB, 2 KiB.
	smallSpecs := []struct {
		name string
		size int
		ops  int
	}{
		{"small_unaligned_128B", 128, 16384}, // 2 MiB
		{"small_unaligned_512B", 512, 8192},  // 4 MiB
		{"small_unaligned_1KiB", 1024, 8192}, // 8 MiB
		{"small_unaligned_2KiB", 2048, 4096}, // 8 MiB
	}
	for _, sp := range smallSpecs {
		buf := bytes.Repeat([]byte{0x55}, sp.size)
		for _, strat := range strategies(16 << 10) {
			r, err := runFileWorkload(baseDir, sp.name, strat, 16<<20, func(f journal.File) (uint64, uint64, error) {
				off := int64(80)
				for i := 0; i < sp.ops; i++ {
					if _, err := f.WriteAt(buf, off); err != nil {
						return 0, 0, err
					}
					off += int64(sp.size)
				}
				return uint64(sp.ops), uint64(sp.ops * sp.size), nil
			})
			mustAdd(r, err)
		}
	}

	// 2. Random 4 KiB partial writes (512 B partial updates across 16 hot 4 KiB pages).
	{
		const (
			ops      = 16384
			writeSz  = 512
			hotPages = 16
		)
		buf := bytes.Repeat([]byte{0x3A}, writeSz)
		for _, strat := range strategies(64 << 10) {
			r, err := runFileWorkload(baseDir, "random_4KiB_partial_512B", strat, 4<<20, func(f journal.File) (uint64, uint64, error) {
				rng := rand.New(rand.NewPCG(1337, 42))
				for i := 0; i < ops; i++ {
					pg := int64(rng.IntN(hotPages))
					sub := int64(rng.IntN(4096-writeSz)) &^ 7
					if _, err := f.WriteAt(buf, pg*4096+sub); err != nil {
						return 0, 0, err
					}
				}
				return ops, ops * writeSz, nil
			})
			mustAdd(r, err)
		}
	}

	// 3. Mixed workload: 70% small unaligned 256 B + 20% 16 KiB aligned + 10% 64 KiB aligned.
	{
		const ops = 6000
		smallBuf := bytes.Repeat([]byte{0x11}, 256)
		medBuf := bytes.Repeat([]byte{0x22}, 16<<10)
		bigBuf := bytes.Repeat([]byte{0x33}, 64<<10)
		for _, strat := range strategies(16 << 10) {
			r, err := runFileWorkload(baseDir, "mixed_70_small_30_large", strat, 64<<20, func(f journal.File) (uint64, uint64, error) {
				smallOff := int64(80)
				largeOff := int64(4 << 20)
				var total uint64
				for i := 0; i < ops; i++ {
					switch i % 10 {
					case 7, 8:
						if largeOff+int64(len(medBuf)) > 64<<20 {
							largeOff = 4 << 20
						}
						if _, err := f.WriteAt(medBuf, largeOff); err != nil {
							return 0, 0, err
						}
						largeOff += int64(len(medBuf))
						total += uint64(len(medBuf))
					case 9:
						if largeOff+int64(len(bigBuf)) > 64<<20 {
							largeOff = 4 << 20
						}
						if _, err := f.WriteAt(bigBuf, largeOff); err != nil {
							return 0, 0, err
						}
						largeOff += int64(len(bigBuf))
						total += uint64(len(bigBuf))
					default:
						if _, err := f.WriteAt(smallBuf, smallOff); err != nil {
							return 0, 0, err
						}
						smallOff += int64(len(smallBuf))
						total += uint64(len(smallBuf))
					}
				}
				return ops, total, nil
			})
			mustAdd(r, err)
		}
	}

	// 4. Large aligned streaming writes: 16 KiB and 64 KiB (verifying zero-copy bypass).
	largeSpecs := []struct {
		name string
		size int
		ops  int
	}{
		{"large_aligned_16KiB", 16 << 10, 4096}, // 64 MiB
		{"large_aligned_64KiB", 64 << 10, 1024}, // 64 MiB
	}
	for _, sp := range largeSpecs {
		buf := bytes.Repeat([]byte{0x7C}, sp.size)
		for _, strat := range strategies(16 << 10) {
			r, err := runFileWorkload(baseDir, sp.name, strat, 64<<20, func(f journal.File) (uint64, uint64, error) {
				var off int64
				for i := 0; i < sp.ops; i++ {
					if _, err := f.WriteAt(buf, off); err != nil {
						return 0, 0, err
					}
					off += int64(sp.size)
				}
				return uint64(sp.ops), uint64(sp.ops * sp.size), nil
			})
			mustAdd(r, err)
		}
	}

	// 5a. End-to-end Journal burst (256 B x 2048 records, Sync every 64 records).
	for _, strat := range strategies(16 << 10) {
		r, err := runJournalEndToEnd(baseDir, "journal_e2e_burst_256B", strat, 2048, 256, 64, false)
		mustAdd(r, err)
	}
	// 5b. End-to-end Journal paced interactive appends (256 B x 1024 records, Sync every 64 records).
	for _, strat := range strategies(16 << 10) {
		r, err := runJournalEndToEnd(baseDir, "journal_e2e_paced_256B", strat, 1024, 256, 64, true)
		mustAdd(r, err)
	}

	// 6. Honest Negative Regime 1: sync_every_write_fsync_barrier (256 B + fdatasync per write).
	{
		const ops = 200
		buf := bytes.Repeat([]byte{0x9E}, 256)
		for _, strat := range strategies(16 << 10) {
			r, err := runFileWorkload(baseDir, "neg_sync_every_write_fsync_barrier", strat, 4<<20, func(f journal.File) (uint64, uint64, error) {
				off := int64(80)
				for i := 0; i < ops; i++ {
					if _, err := f.WriteAt(buf, off); err != nil {
						return 0, 0, err
					}
					if err := f.Sync(); err != nil {
						return 0, 0, err
					}
					off += 256
				}
				return ops, ops * 256, nil
			})
			mustAdd(r, err)
		}
	}

	// 7. Honest Negative Regime 2: oversized_scrap_buffer_memory_pressure sweep.
	{
		const ops = 16384 // 4 MiB of 256 B unaligned appends
		buf := bytes.Repeat([]byte{0x6D}, 256)
		for _, capBytes := range []int{4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20} {
			cfg := journal.WSBufferConfig{
				PageSize:        4096,
				Capacity:        capBytes,
				BypassThreshold: 4096,
			}
			r, err := runFileWorkload(
				baseDir,
				"neg_oversized_scrap_buffer_memory_pressure",
				strategySpec{name: fmt.Sprintf("wsbuffer_cap_%dKiB", capBytes>>10), cfg: &cfg},
				8<<20,
				func(f journal.File) (uint64, uint64, error) {
					off := int64(80)
					for i := 0; i < ops; i++ {
						if _, err := f.WriteAt(buf, off); err != nil {
							return 0, 0, err
						}
						off += 256
					}
					return ops, ops * 256, nil
				},
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "sweep error: %v\n", err)
				os.Exit(1)
			}
			report.CapacitySweep = append(report.CapacitySweep, r)
		}
	}

	if *outPath != "" {
		if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", *outPath, err)
			os.Exit(1)
		}
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*outPath, append(data, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", *outPath, err)
			os.Exit(1)
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
		return
	}

	fmt.Printf("WSBuffer Comparative Benchmark (HIP-1344, hanzoai/vfs#58)\n")
	fmt.Printf("Host: %s/%s (%d CPUs), Go: %s, PageSize: %d B\n\n", report.OS, report.Arch, report.NumCPU, report.GoVersion, report.PageSize)
	fmt.Printf("| Workload | Strategy | Phys Write Ops | Phys Write Amp | Direct Bypass Ops | Scrap Copy (KiB) | /proc syscw | Wall (ms) | CPU (ms) | Throughput (MB/s) |\n")
	fmt.Printf("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range report.Results {
		fmt.Printf("| %s | %s | %d | %.2fx | %d | %.1f | %d | %.2f | %.2f | %.1f |\n",
			r.Workload, r.Strategy, r.PhysicalWriteOps, r.PhysicalWriteAmp,
			r.DirectBypassOps, float64(r.ScrapCopyBytes)/1024.0, r.ProcIOSyscw,
			r.WallMs, r.CPUTotalMs, r.ThroughputMBS)
	}
	fmt.Printf("\nNegative Regime 2: Oversized Scrap Buffer Capacity Sweep (256 B x 16,384 = 4 MiB)\n")
	fmt.Printf("| Capacity | Phys Write Ops | Phys Write Amp | Max Dirty (KiB) | Flush Ops | Wall (ms) | Throughput (MB/s) |\n")
	fmt.Printf("|---|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range report.CapacitySweep {
		fmt.Printf("| %d KiB | %d | %.4fx | %.1f | %d | %.2f | %.1f |\n",
			r.CapacityBytes>>10, r.PhysicalWriteOps, r.PhysicalWriteAmp,
			float64(r.MaxDirtyBytes)/1024.0, r.FlushOps, r.WallMs, r.ThroughputMBS)
	}
}
