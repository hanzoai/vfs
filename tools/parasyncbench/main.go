// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command parasyncbench measures ParaSync parallel delta restore against
// matched parallel and serial full restores on real VFS checkpoints (HIP-1344,
// Bounty #64).
//
//	go run ./tools/parasyncbench
//	go run ./tools/parasyncbench -csv docs/parasync.csv
//	go run ./tools/parasyncbench -full -csv docs/parasync.csv
package main

import (
	"context"
	"encoding/csv"
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
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	_ "github.com/hanzoai/vfs/pkg/backend/s3"
)

type benchBackend struct {
	backend.Backend
	delay        time.Duration
	gets         atomic.Int64
	getRanges    atomic.Int64
	stats        atomic.Int64
	puts         atomic.Int64
	putIfAbsents atomic.Int64
	putIfVers    atomic.Int64
}

func newBenchBackend(be backend.Backend, delay time.Duration) *benchBackend {
	return &benchBackend{Backend: be, delay: delay}
}

func (b *benchBackend) sleep(ctx context.Context) error {
	if b.delay <= 0 {
		return nil
	}
	t := time.NewTimer(b.delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (b *benchBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if err := b.sleep(ctx); err != nil {
		return nil, "", err
	}
	b.gets.Add(1)
	return b.Backend.Get(ctx, key, max)
}

func (b *benchBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := b.sleep(ctx); err != nil {
		return nil, err
	}
	b.getRanges.Add(1)
	return b.Backend.GetRange(ctx, key, off, n)
}

func (b *benchBackend) Put(ctx context.Context, key string, data []byte) error {
	if err := b.sleep(ctx); err != nil {
		return err
	}
	b.puts.Add(1)
	return b.Backend.Put(ctx, key, data)
}

func (b *benchBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if err := b.sleep(ctx); err != nil {
		return "", err
	}
	b.puts.Add(1)
	b.putIfAbsents.Add(1)
	return b.Backend.PutIfAbsent(ctx, key, data)
}

func (b *benchBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if err := b.sleep(ctx); err != nil {
		return "", err
	}
	b.puts.Add(1)
	b.putIfVers.Add(1)
	return b.Backend.PutIfVersion(ctx, key, data, v)
}

func (b *benchBackend) Stat(ctx context.Context, key string) (int64, error) {
	if err := b.sleep(ctx); err != nil {
		return 0, err
	}
	b.stats.Add(1)
	return b.Backend.Stat(ctx, key)
}

func (b *benchBackend) totalOps() int64 {
	return b.gets.Load() + b.getRanges.Load() + b.stats.Load() + b.puts.Load()
}

type benchRow struct {
	Workload           string  `json:"workload"`
	Backend            string  `json:"backend"`
	Mode               string  `json:"mode"`
	Streams            int     `json:"streams"`
	SimulatedRTTMs     float64 `json:"simulated_rtt_ms"`
	Files              int     `json:"files"`
	LogicalMB          float64 `json:"logical_mb"`
	FullSnapshotBytes  int64   `json:"full_snapshot_bytes"`
	BytesTransferred   int64   `json:"bytes_transferred"`
	BytesSavedPct      float64 `json:"bytes_saved_pct"`
	VerifyBytesRead    int64   `json:"verify_bytes_read"`
	VerifyGets         int64   `json:"verify_gets"`
	ChunksVerified     int     `json:"chunks_verified"`
	ShardsCopied       int     `json:"shards_copied"`
	ShardsPruned       int     `json:"shards_pruned"`
	TotalSourceShards  int     `json:"total_source_shards"`
	PacksCopied        int     `json:"packs_copied"`
	PacksPruned        int     `json:"packs_pruned"`
	TotalSourcePacks   int     `json:"total_source_packs"`
	SourceGets         int64   `json:"source_gets"`
	DestPuts           int64   `json:"dest_puts"`
	DestStats          int64   `json:"dest_stats"`
	TotalBackendOps    int64   `json:"total_backend_ops"`
	WallMs             float64 `json:"wall_ms"`
	CPUMs              float64 `json:"cpu_ms"`
	PeakGoroutines     int     `json:"peak_goroutines"`
	AllocBytes         uint64  `json:"alloc_bytes"`
	HeapInuseDeltaByte int64   `json:"heap_inuse_delta_bytes"`
}

type workloadFixture struct {
	name      string
	files     int
	logicalMB float64
	srcBE     backend.Backend
	baseBE    backend.Backend // checkpoint 1 store (used to seed delta targets cleanly)
	cipher    vfs.Cipher
}

func deterministicBytes(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed*7919+1))
	b := make([]byte, n)
	for i := 0; i+8 <= len(b); i += 8 {
		v := r.Uint64()
		b[i] = byte(v)
		b[i+1] = byte(v >> 8)
		b[i+2] = byte(v >> 16)
		b[i+3] = byte(v >> 24)
		b[i+4] = byte(v >> 32)
		b[i+5] = byte(v >> 40)
		b[i+6] = byte(v >> 48)
		b[i+7] = byte(v >> 56)
	}
	for i := len(b) - (len(b) % 8); i < len(b); i++ {
		b[i] = byte(r.Uint32())
	}
	return b
}

func writeFile(ctx context.Context, v *vfs.Volume, dir vfs.InodeID, name string, data []byte) (vfs.Inode, error) {
	in, err := v.Create(ctx, dir, name, 0o644)
	if err != nil {
		return vfs.Inode{}, err
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		return vfs.Inode{}, err
	}
	defer f.Close()
	if _, err := f.WriteAt(ctx, data, 0); err != nil {
		return vfs.Inode{}, err
	}
	return in, nil
}

func overwriteFile(ctx context.Context, v *vfs.Volume, dir vfs.InodeID, name string, data []byte) error {
	in, err := v.Lookup(ctx, dir, name)
	if err != nil {
		return err
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt(ctx, data, 0)
	return err
}

func buildFixture(ctx context.Context, scratch string, name string, useFile bool, fullScale bool) (*workloadFixture, error) {
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		return nil, err
	}
	c := vfs.Hybrid(key)

	openStore := func(tag string) (backend.Backend, error) {
		if useFile {
			dir := filepath.Join(scratch, name+"-"+tag)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
			return backend.Open(ctx, "file://"+filepath.ToSlash(dir))
		}
		return storetest.NewMem(), nil
	}

	srcBE, err := openStore("src")
	if err != nil {
		return nil, err
	}
	baseBE, err := openStore("base")
	if err != nil {
		return nil, err
	}
	jDir := filepath.Join(scratch, name+"-journal")
	if err := vfs.Create(ctx, vfs.Config{Backend: srcBE, Cipher: c}); err != nil {
		return nil, err
	}
	v, err := vfs.Attach(ctx, vfs.Config{
		Backend:         srcBE,
		Cipher:          c,
		Round:           1,
		Journal:         jDir,
		JournalCapacity: 512 << 20,
		Disk:            storetest.Disk,
	})
	if err != nil {
		return nil, err
	}
	defer v.Close()

	var files int
	var logicalBytes int64

	switch name {
	case "packed_append":
		packs := 5
		chunkBlob := 2 << 20
		if fullScale {
			packs = 4
			chunkBlob = 16<<20 + 512<<10 // > packTarget (16 MiB) per file
		}
		for i := range packs {
			data := deterministicBytes(uint64(100+i), chunkBlob)
			if _, err := writeFile(ctx, v, vfs.RootInode, fmt.Sprintf("model-part-%02d.bin", i), data); err != nil {
				return nil, err
			}
			if err := v.Flush(ctx); err != nil {
				return nil, err
			}
			files++
			logicalBytes += int64(len(data))
		}
		if _, err := vfs.RestoreDelta(ctx, srcBE, baseBE, c, vfs.RestoreOptions{Round: 1, Streams: 8}); err != nil {
			return nil, err
		}
		// Add 1 new packed file in checkpoint 2.
		deltaData := deterministicBytes(999, chunkBlob)
		if _, err := writeFile(ctx, v, vfs.RootInode, "model-part-delta.bin", deltaData); err != nil {
			return nil, err
		}
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}
		files++
		logicalBytes += int64(len(deltaData))

	case "inline_localized":
		dirs, perDir, fileSz := 10, 60, 2048
		if fullScale {
			dirs, perDir, fileSz = 16, 80, 4096
		}
		dirIDs := make([]vfs.InodeID, dirs)
		for d := range dirs {
			din, err := v.Mkdir(ctx, vfs.RootInode, fmt.Sprintf("d%02d", d), 0o755)
			if err != nil {
				return nil, err
			}
			dirIDs[d] = din.ID
			for f := range perDir {
				if _, err := writeFile(ctx, v, din.ID, fmt.Sprintf("f%03d", f), deterministicBytes(uint64(d*1000+f), fileSz)); err != nil {
					return nil, err
				}
				files++
				logicalBytes += int64(fileSz)
			}
		}
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}
		if _, err := vfs.RestoreDelta(ctx, srcBE, baseBE, c, vfs.RestoreOptions{Round: 1, Streams: 8}); err != nil {
			return nil, err
		}
		// Localized edit: update 5% of files all inside directory d00.
		edits := max(files/20, 4)
		edits = min(edits, perDir)
		for f := range edits {
			if err := overwriteFile(ctx, v, dirIDs[0], fmt.Sprintf("f%03d", f), deterministicBytes(uint64(50000+f), fileSz)); err != nil {
				return nil, err
			}
		}
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}

	case "inline_scattered_neg":
		// Negative / granularity limit 1: edit ~1 file in every directory (or spread
		// uniformly across the [tData] key range) so nearly every 64 KiB shard is dirtied.
		dirs, perDir, fileSz := 10, 60, 4096
		if fullScale {
			dirs, perDir, fileSz = 16, 80, 4096
		}
		dirIDs := make([]vfs.InodeID, dirs)
		for d := range dirs {
			din, err := v.Mkdir(ctx, vfs.RootInode, fmt.Sprintf("d%02d", d), 0o755)
			if err != nil {
				return nil, err
			}
			dirIDs[d] = din.ID
			for f := range perDir {
				if _, err := writeFile(ctx, v, din.ID, fmt.Sprintf("f%03d", f), deterministicBytes(uint64(d*1000+f), fileSz)); err != nil {
					return nil, err
				}
				files++
				logicalBytes += int64(fileSz)
			}
		}
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}
		if _, err := vfs.RestoreDelta(ctx, srcBE, baseBE, c, vfs.RestoreOptions{Round: 1, Streams: 8}); err != nil {
			return nil, err
		}
		// Edit every 15th file across all directories (~6.6% of files) -> touches almost every 64 KiB shard!
		for d := range dirs {
			for f := 0; f < perDir; f += 15 {
				if err := overwriteFile(ctx, v, dirIDs[d], fmt.Sprintf("f%03d", f), deterministicBytes(uint64(80000+d*100+f), fileSz)); err != nil {
					return nil, err
				}
			}
		}
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}

	case "pack_granularity_neg":
		// Negative / granularity limit 2: dst is at checkpoint 1 (2 MiB base).
		// Intermediate checkpoint 2 packs 14 MiB ephemeral scratch + 300 KiB retained
		// file into ONE ~14.3 MiB pack P2. Checkpoint 3 removes the 14 MiB ephemeral file.
		// Restoring checkpoint 1 -> checkpoint 3 must copy the entire ~14.3 MiB
		// ciphertext pack P2 even though only 300 KiB of live logical delta remains!
		baseBlob := deterministicBytes(700, 2<<20)
		if _, err := writeFile(ctx, v, vfs.RootInode, "base.bin", baseBlob); err != nil {
			return nil, err
		}
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}
		if _, err := vfs.RestoreDelta(ctx, srcBE, baseBE, c, vfs.RestoreOptions{Round: 1, Streams: 8}); err != nil {
			return nil, err
		}
		ephemSize := 14 << 20
		keepSize := 300 << 10
		if _, err := writeFile(ctx, v, vfs.RootInode, "ephemeral.bin", deterministicBytes(701, ephemSize)); err != nil {
			return nil, err
		}
		if _, err := writeFile(ctx, v, vfs.RootInode, "retained.bin", deterministicBytes(702, keepSize)); err != nil {
			return nil, err
		}
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}
		if err := v.Remove(ctx, vfs.RootInode, "ephemeral.bin"); err != nil {
			return nil, err
		}
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}
		files = 2
		logicalBytes = int64(len(baseBlob) + keepSize)

	case "noop_identical":
		dirs, perDir, fileSz := 8, 50, 4096
		for d := range dirs {
			din, err := v.Mkdir(ctx, vfs.RootInode, fmt.Sprintf("d%02d", d), 0o755)
			if err != nil {
				return nil, err
			}
			for f := range perDir {
				if _, err := writeFile(ctx, v, din.ID, fmt.Sprintf("f%03d", f), deterministicBytes(uint64(d*1000+f), fileSz)); err != nil {
					return nil, err
				}
				files++
				logicalBytes += int64(fileSz)
			}
		}
		if _, err := writeFile(ctx, v, vfs.RootInode, "pack.bin", deterministicBytes(303, 2<<20)); err != nil {
			return nil, err
		}
		files++
		logicalBytes += 2 << 20
		if err := v.Flush(ctx); err != nil {
			return nil, err
		}
		if _, err := vfs.RestoreDelta(ctx, srcBE, baseBE, c, vfs.RestoreOptions{Round: 1, Streams: 8}); err != nil {
			return nil, err
		}
	}

	return &workloadFixture{
		name:      name,
		files:     files,
		logicalMB: float64(logicalBytes) / (1 << 20),
		srcBE:     srcBE,
		baseBE:    baseBE,
		cipher:    c,
	}, nil
}

func cloneBackend(ctx context.Context, src, dst backend.Backend) error {
	objs, errCh := src.List(ctx, "")
	for o := range objs {
		b, _, err := src.Get(ctx, o.Key, backend.Unbounded)
		if err != nil {
			return err
		}
		if err := dst.Put(ctx, o.Key, b); err != nil {
			return err
		}
	}
	return <-errCh
}

func runMode(ctx context.Context, scratch string, fx *workloadFixture, beKind string, mode string, streams int, rtt time.Duration, round uint64, runSeq int) (benchRow, error) {
	var dstRaw backend.Backend
	var err error
	if beKind == "file" {
		dir := filepath.Join(scratch, fmt.Sprintf("run-%s-%s-s%d-r%d-%d", fx.name, mode, streams, rtt.Milliseconds(), runSeq))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return benchRow{}, err
		}
		dstRaw, err = backend.Open(ctx, "file://"+filepath.ToSlash(dir))
		if err != nil {
			return benchRow{}, err
		}
	} else {
		dstRaw = storetest.NewMem()
	}
	defer dstRaw.Close()

	if strings.HasPrefix(mode, "delta") {
		if err := cloneBackend(ctx, fx.baseBE, dstRaw); err != nil {
			return benchRow{}, err
		}
		if err := vfs.Reseal(ctx, vfs.Config{Backend: dstRaw, Cipher: fx.cipher, Round: 2}, fx.baseBE.String()); err != nil {
			return benchRow{}, err
		}
	}

	srcWrap := newBenchBackend(fx.srcBE, rtt)
	dstWrap := newBenchBackend(dstRaw, rtt)

	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	stats, err := vfs.RestoreDelta(ctx, srcWrap, dstWrap, fx.cipher, vfs.RestoreOptions{
		Round:   round,
		Streams: streams,
	})
	if err != nil {
		return benchRow{}, fmt.Errorf("%s %s (streams=%d rtt=%v): %w", fx.name, mode, streams, rtt, err)
	}

	runtime.ReadMemStats(&m1)
	savedPct := 0.0
	if stats.FullSnapshotBytes > 0 {
		savedPct = (1.0 - float64(stats.BytesTransferred)/float64(stats.FullSnapshotBytes)) * 100.0
	}

	return benchRow{
		Workload:           fx.name,
		Backend:            beKind,
		Mode:               mode,
		Streams:            streams,
		SimulatedRTTMs:     float64(rtt.Microseconds()) / 1000.0,
		Files:              fx.files,
		LogicalMB:          fx.logicalMB,
		FullSnapshotBytes:  stats.FullSnapshotBytes,
		BytesTransferred:   stats.BytesTransferred,
		BytesSavedPct:      savedPct,
		VerifyBytesRead:    stats.VerifyBytesRead,
		VerifyGets:         stats.VerifyGets,
		ChunksVerified:     stats.ChunksVerified,
		ShardsCopied:       stats.ShardsCopied,
		ShardsPruned:       stats.Plan.ShardsPruned,
		TotalSourceShards:  stats.Plan.TotalSourceShards,
		PacksCopied:        stats.PacksCopied,
		PacksPruned:        stats.Plan.PacksPruned,
		TotalSourcePacks:   stats.Plan.TotalSourcePacks,
		SourceGets:         stats.SourceGets,
		DestPuts:           stats.DestPuts,
		DestStats:          stats.DestStats,
		TotalBackendOps:    srcWrap.totalOps() + dstWrap.totalOps(),
		WallMs:             float64(stats.Duration.Microseconds()) / 1000.0,
		CPUMs:              float64(stats.CPUTime.Microseconds()) / 1000.0,
		PeakGoroutines:     stats.PeakGoroutines,
		AllocBytes:         m1.TotalAlloc - m0.TotalAlloc,
		HeapInuseDeltaByte: int64(m1.HeapInuse) - int64(m0.HeapInuse),
	}, nil
}

func writeCSV(path string, rows []benchRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	header := []string{
		"workload", "backend", "mode", "streams", "simulated_rtt_ms",
		"files", "logical_mb", "full_snapshot_bytes", "bytes_transferred", "bytes_saved_pct",
		"verify_bytes_read", "verify_gets", "chunks_verified",
		"shards_copied", "shards_pruned", "total_source_shards",
		"packs_copied", "packs_pruned", "total_source_packs",
		"source_gets", "dest_puts", "dest_stats", "total_backend_ops",
		"wall_ms", "cpu_ms", "peak_goroutines", "alloc_bytes", "heap_inuse_delta_bytes",
	}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, r := range rows {
		rec := []string{
			r.Workload,
			r.Backend,
			r.Mode,
			strconv.Itoa(r.Streams),
			fmt.Sprintf("%.3f", r.SimulatedRTTMs),
			strconv.Itoa(r.Files),
			fmt.Sprintf("%.3f", r.LogicalMB),
			strconv.FormatInt(r.FullSnapshotBytes, 10),
			strconv.FormatInt(r.BytesTransferred, 10),
			fmt.Sprintf("%.2f", r.BytesSavedPct),
			strconv.FormatInt(r.VerifyBytesRead, 10),
			strconv.FormatInt(r.VerifyGets, 10),
			strconv.Itoa(r.ChunksVerified),
			strconv.Itoa(r.ShardsCopied),
			strconv.Itoa(r.ShardsPruned),
			strconv.Itoa(r.TotalSourceShards),
			strconv.Itoa(r.PacksCopied),
			strconv.Itoa(r.PacksPruned),
			strconv.Itoa(r.TotalSourcePacks),
			strconv.FormatInt(r.SourceGets, 10),
			strconv.FormatInt(r.DestPuts, 10),
			strconv.FormatInt(r.DestStats, 10),
			strconv.FormatInt(r.TotalBackendOps, 10),
			fmt.Sprintf("%.3f", r.WallMs),
			fmt.Sprintf("%.3f", r.CPUMs),
			strconv.Itoa(r.PeakGoroutines),
			strconv.FormatUint(r.AllocBytes, 10),
			strconv.FormatInt(r.HeapInuseDeltaByte, 10),
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func main() {
	full := flag.Bool("full", false, "run full-scale 16 MiB pack fixture")
	streams := flag.Int("streams", 8, "concurrent transfer streams for matched parallel baseline and delta")
	rttsFlag := flag.String("rtts", "0ms,2ms", "comma-separated per-call delays (0ms=unthrottled real, >0=injected simulation delay)")
	backendKind := flag.String("backend", "mem,file", "comma-separated backends: mem, file")
	csvPath := flag.String("csv", "", "optional CSV output path")
	asJSON := flag.Bool("json", false, "emit JSON output")
	flag.Parse()

	var rtts []time.Duration
	for _, s := range strings.Split(*rttsFlag, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parasyncbench: invalid rtt %q: %v\n", s, err)
			os.Exit(1)
		}
		rtts = append(rtts, d)
	}
	var backends []string
	for _, b := range strings.Split(*backendKind, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			backends = append(backends, b)
		}
	}

	scratch, err := os.MkdirTemp("", "parasyncbench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "parasyncbench: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(scratch)

	ctx := context.Background()
	workloads := []string{
		"packed_append",
		"inline_localized",
		"inline_scattered_neg",
		"pack_granularity_neg",
		"noop_identical",
	}

	var rows []benchRow
	runSeq := 0
	for _, beKind := range backends {
		useFile := beKind == "file"
		for _, wl := range workloads {
			fx, err := buildFixture(ctx, scratch, wl, useFile, *full)
			if err != nil {
				fmt.Fprintf(os.Stderr, "parasyncbench build %s/%s: %v\n", beKind, wl, err)
				os.Exit(1)
			}
			for _, rtt := range rtts {
				// Skip slow serial/file+sim-RTT combos only? No, keep measurements complete & honest!
				// On file backend, keep unthrottled (0ms) unless explicitly requested on small set.
				if beKind == "file" && rtt > 0 {
					continue
				}
				type spec struct {
					mode    string
					streams int
					round   uint64
				}
				specs := []spec{
					{"full_s1", 1, 1},
					{"full_par", *streams, 1},
					{"delta_s1", 1, 3},
					{"delta_par", *streams, 3},
				}
				for _, sp := range specs {
					runSeq++
					row, err := runMode(ctx, scratch, fx, beKind, sp.mode, sp.streams, rtt, sp.round, runSeq)
					if err != nil {
						fmt.Fprintf(os.Stderr, "parasyncbench: %v\n", err)
						os.Exit(1)
					}
					rows = append(rows, row)
				}
			}
			_ = fx.srcBE.Close()
			_ = fx.baseBE.Close()
		}
	}

	if *csvPath != "" {
		if err := writeCSV(*csvPath, rows); err != nil {
			fmt.Fprintf(os.Stderr, "parasyncbench csv: %v\n", err)
			os.Exit(1)
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rows)
		return
	}

	fmt.Printf("ParaSync Parallel Delta Restore Benchmark (HIP-1344, Bounty #64)\n")
	fmt.Printf("================================================================\n")
	fmt.Printf("Parallel streams: %d | Go: %s | GOMAXPROCS: %d\n\n", *streams, runtime.Version(), runtime.GOMAXPROCS(0))
	fmt.Printf("| Workload | Backend | Mode | Streams | SimRTT(ms) | Transferred / Full (B) | Saved%% | VerifyRead(B) | VerifyGets | ChunksVer | Shards(C/P) | Packs(C/P) | SrcGets | Ops | Wall(ms) | CPU(ms) | PeakG |\n")
	fmt.Printf("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---:|\n")
	for _, r := range rows {
		fmt.Printf("| %s | %s | %s | %d | %.1f | %d / %d | %.1f%% | %d | %d | %d | %d/%d | %d/%d | %d | %d | %.2f | %.2f | %d |\n",
			r.Workload, r.Backend, r.Mode, r.Streams, r.SimulatedRTTMs,
			r.BytesTransferred, r.FullSnapshotBytes, r.BytesSavedPct,
			r.VerifyBytesRead, r.VerifyGets, r.ChunksVerified,
			r.ShardsCopied, r.ShardsPruned, r.PacksCopied, r.PacksPruned,
			r.SourceGets, r.TotalBackendOps, r.WallMs, r.CPUMs, r.PeakGoroutines)
	}
}
