package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"syscall"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/wsbuffer"
)

// BenchResult records one benchmark run's measured performance, CPU, and WAF metrics.
type BenchResult struct {
	Workload             string  `json:"workload"`
	Category             string  `json:"category"` // "primary", "mixed", "negative"
	Engine               string  `json:"engine"`
	Ops                  int     `json:"ops"`
	SyncBatch            int     `json:"sync_batch"`
	LogicalBytes         uint64  `json:"logical_bytes"`
	ElapsedMs            float64 `json:"elapsed_ms"`
	ThroughputMiBs       float64 `json:"throughput_mib_s"`
	IOPS                 float64 `json:"iops"`
	CPUNsPerOp           float64 `json:"cpu_ns_per_op"`
	UserCPUMs            float64 `json:"user_cpu_ms"`
	SysCPUMs             float64 `json:"sys_cpu_ms"`
	AllocsPerOp          float64 `json:"allocs_per_op"`
	AllocBytesPerOp      float64 `json:"alloc_bytes_per_op"`
	WALPhysicalBytes     uint64  `json:"wal_physical_bytes"`
	BackingPhysicalBytes uint64  `json:"backing_physical_bytes"`
	TotalPhysicalBytes   uint64  `json:"total_physical_bytes"`
	WAF                  float64 `json:"waf"`
	WALWAF               float64 `json:"wal_waf"`
	BackingWAF           float64 `json:"backing_waf"`
	PhysicalWrites       uint64  `json:"physical_writes"`
	Fsyncs               uint64  `json:"fsyncs"`
	CoalescedWrites      uint64  `json:"coalesced_writes"`
	BypassWrites         uint64  `json:"bypass_writes"`
	BackpressureFlushes  uint64  `json:"backpressure_flushes"`
	Verified             bool    `json:"verified"`
}

// ReportBundle is the full JSON artifact emitted by cmd/wsbuffer-bench.
type ReportBundle struct {
	GeneratedAt string        `json:"generated_at"`
	GoVersion   string        `json:"go_version"`
	GOOS        string        `json:"goos"`
	GOARCH      string        `json:"goarch"`
	NumCPU      int           `json:"num_cpu"`
	WorkDir     string        `json:"work_dir"`
	Reps        int           `json:"reps"`
	Quick       bool          `json:"quick"`
	Results     []BenchResult `json:"results"`
}

type writeOp struct {
	off  int64
	size int
	seed byte
}

// engine abstracts plain-disk, upstream journal on main, and wsbuffer variants
// over the exact same storage directory and workload sequence.
type engine interface {
	WriteAt(ctx context.Context, p []byte, off int64) error
	ReadAt(p []byte, off int64) (int, error)
	Sync(ctx context.Context) error
	Stats() engineStats
	Close() error
}

type engineStats struct {
	walWrites, walBytes, walSyncs             uint64
	backingWrites, backingBytes, backingSyncs uint64
	coalescedWrites                           uint64
	bypassWrites                              uint64
	backpressureFlushes                       uint64
}

// ---- Engine 1: plain-disk (no WAL, direct pwrite + fdatasync reference) ----

type plainDiskEngine struct {
	f      *os.File
	writes uint64
	bytes  uint64
	syncs  uint64
}

func openPlainDisk(dir string, prealloc int64) (*plainDiskEngine, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "data.bin"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	if err := preallocFile(f, prealloc); err != nil {
		f.Close()
		return nil, err
	}
	return &plainDiskEngine{f: f}, nil
}

func (e *plainDiskEngine) WriteAt(_ context.Context, p []byte, off int64) error {
	n, err := e.f.WriteAt(p, off)
	if n > 0 {
		e.writes++
		e.bytes += uint64(n)
	}
	return err
}

func (e *plainDiskEngine) ReadAt(p []byte, off int64) (int, error) {
	return e.f.ReadAt(p, off)
}

func (e *plainDiskEngine) Sync(_ context.Context) error {
	e.syncs++
	return syscall.Fdatasync(int(e.f.Fd()))
}

func (e *plainDiskEngine) Stats() engineStats {
	return engineStats{
		backingWrites: e.writes,
		backingBytes:  e.bytes,
		backingSyncs:  e.syncs,
	}
}

func (e *plainDiskEngine) Close() error {
	return e.f.Close()
}

// ---- Engine 2: journal-main (real github.com/hanzoai/vfs/journal on main) ----

type journalMainEngine struct {
	j            *journal.Journal
	disk         *wsbuffer.CountingDisk
	f            *os.File
	lastSeq      uint64
	published    uint64
	sinceReclaim int64
	segSize      int64
	recBuf       []byte
	bWrites      uint64
	bBytes       uint64
	bSyncs       uint64
}

func openJournalMain(dir string, prealloc, segSize, capacity int64) (*journalMainEngine, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	jdir := filepath.Join(dir, "journal")
	if err := os.MkdirAll(jdir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "data.bin"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	if err := preallocFile(f, prealloc); err != nil {
		f.Close()
		return nil, err
	}
	baseDisk, err := journal.Files(jdir)
	if err != nil {
		f.Close()
		return nil, err
	}
	cd := wsbuffer.NewCountingDisk(baseDisk)
	j, err := journal.Open(jdir, journal.Options{
		Capacity: capacity,
		Segment:  segSize,
		Lineage:  []byte("bench-journal-main"),
		Disk:     cd,
	})
	if err != nil {
		f.Close()
		return nil, err
	}
	return &journalMainEngine{
		j:       j,
		disk:    cd,
		f:       f,
		segSize: segSize,
	}, nil
}

func (e *journalMainEngine) WriteAt(ctx context.Context, p []byte, off int64) error {
	// Standard per-write WAL record: [off(8B) | len(4B) | payload]
	need := 12 + len(p)
	if cap(e.recBuf) < need {
		e.recBuf = make([]byte, need)
	}
	rec := e.recBuf[:need]
	binary.LittleEndian.PutUint64(rec[0:8], uint64(off))
	binary.LittleEndian.PutUint32(rec[8:12], uint32(len(p)))
	copy(rec[12:], p)

	seq, err := e.j.Append(ctx, rec)
	if err == journal.ErrFull {
		_ = e.j.Reclaim()
		seq, err = e.j.Append(ctx, rec)
	}
	if err != nil {
		return err
	}
	e.lastSeq = seq
	e.sinceReclaim += int64(need) + 28

	n, err := e.f.WriteAt(p, off)
	if n > 0 {
		e.bWrites++
		e.bBytes += uint64(n)
	}
	return err
}

func (e *journalMainEngine) ReadAt(p []byte, off int64) (int, error) {
	return e.f.ReadAt(p, off)
}

func (e *journalMainEngine) Sync(ctx context.Context) error {
	if e.lastSeq > e.published {
		if err := e.j.Sync(ctx, e.lastSeq); err != nil {
			return err
		}
	}
	e.bSyncs++
	if err := syscall.Fdatasync(int(e.f.Fd())); err != nil {
		return err
	}
	if e.lastSeq > e.published {
		if err := e.j.Publish(e.lastSeq); err != nil {
			return err
		}
		e.published = e.lastSeq
		if e.sinceReclaim >= e.segSize/2 {
			if err := e.j.Reclaim(); err != nil {
				return err
			}
			e.sinceReclaim = 0
		}
	}
	return nil
}

func (e *journalMainEngine) Stats() engineStats {
	return engineStats{
		walWrites:     e.disk.Writes.Load(),
		walBytes:      e.disk.Bytes.Load(),
		walSyncs:      e.disk.Syncs.Load(),
		backingWrites: e.bWrites,
		backingBytes:  e.bBytes,
		backingSyncs:  e.bSyncs,
	}
}

func (e *journalMainEngine) Close() error {
	err1 := e.j.Close()
	err2 := e.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// ---- Engine 3: wsbuffer (with JournalWAL, optional no-bypass or custom cap) ----

type wsbufferEngine struct {
	s *wsbuffer.Store
}

func openWSBuffer(dir string, prealloc int64, bypassThreshold int, maxScrapBytes int64, segSize, capacity int64) (*wsbufferEngine, error) {
	s, err := wsbuffer.Open(dir, wsbuffer.Config{
		PageSize:        4096,
		BypassThreshold: bypassThreshold,
		MaxScrapBytes:   maxScrapBytes,
		PreallocBytes:   prealloc,
		WALMode:         wsbuffer.WALModeJournal,
		JournalSegment:  segSize,
		JournalCapacity: capacity,
	})
	if err != nil {
		return nil, err
	}
	return &wsbufferEngine{s: s}, nil
}

func (e *wsbufferEngine) WriteAt(ctx context.Context, p []byte, off int64) error {
	_, err := e.s.WriteAt(ctx, p, off)
	return err
}

func (e *wsbufferEngine) ReadAt(p []byte, off int64) (int, error) {
	return e.s.ReadAt(p, off)
}

func (e *wsbufferEngine) Sync(ctx context.Context) error {
	return e.s.Sync(ctx)
}

func (e *wsbufferEngine) Stats() engineStats {
	st := e.s.Stats()
	return engineStats{
		walWrites:           st.WALPhysicalWrites,
		walBytes:            st.WALPhysicalBytes,
		walSyncs:            st.WALSyncs,
		backingWrites:       st.BackingPhysicalWrites,
		backingBytes:        st.BackingPhysicalBytes,
		backingSyncs:        st.BackingSyncs,
		coalescedWrites:     st.CoalescedWrites,
		bypassWrites:        st.BypassWrites,
		backpressureFlushes: st.BackpressureFlushes,
	}
}

func (e *wsbufferEngine) Close() error {
	return e.s.Close()
}

func preallocFile(f *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	zeros := make([]byte, 1<<20)
	for off := int64(0); off < size; {
		n := size - off
		if n > int64(len(zeros)) {
			n = int64(len(zeros))
		}
		if _, err := f.WriteAt(zeros[:n], off); err != nil {
			return err
		}
		off += n
	}
	return syscall.Fdatasync(int(f.Fd()))
}

func getRusageTime() (userNs, sysNs int64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	return ru.Utime.Nano(), ru.Stime.Nano()
}

func fillPayload(buf []byte, seed byte) {
	for i := range buf {
		buf[i] = byte((int(seed)*37 + i*13 + (i >> 7)) & 0xff)
	}
}

func runSingleMeasurement(
	workRoot, workload, category, engineName string,
	ops []writeOp,
	syncBatch int,
	prealloc int64,
	verifyOracle []byte,
	openFn func(dir string) (engine, error),
) (BenchResult, error) {
	runDir := filepath.Join(workRoot, fmt.Sprintf("%s-%s-%d", workload, engineName, time.Now().UnixNano()))
	defer os.RemoveAll(runDir)

	eng, err := openFn(runDir)
	if err != nil {
		return BenchResult{}, fmt.Errorf("open %s/%s: %w", workload, engineName, err)
	}
	defer eng.Close()

	// Prebuild payload pool by size to exclude payload generation from measured CPU/allocs.
	payloads := make(map[int][16][]byte)
	var logicalBytes uint64
	for _, op := range ops {
		logicalBytes += uint64(op.size)
		if _, ok := payloads[op.size]; !ok {
			var arr [16][]byte
			for k := 0; k < 16; k++ {
				arr[k] = make([]byte, op.size)
				fillPayload(arr[k], byte(k+1))
			}
			payloads[op.size] = arr
		}
	}

	// Capture baseline counters right after open (excludes prealloc).
	baseStats := eng.Stats()
	runtime.GC()
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)
	u0, s0 := getRusageTime()
	t0 := time.Now()

	ctx := context.Background()
	for i, op := range ops {
		p := payloads[op.size][int(op.seed)&15]
		if err := eng.WriteAt(ctx, p, op.off); err != nil {
			return BenchResult{}, fmt.Errorf("write %d (%s/%s): %w", i, workload, engineName, err)
		}
		if syncBatch > 0 && (i+1)%syncBatch == 0 {
			if err := eng.Sync(ctx); err != nil {
				return BenchResult{}, fmt.Errorf("sync %d (%s/%s): %w", i, workload, engineName, err)
			}
		}
	}
	if err := eng.Sync(ctx); err != nil {
		return BenchResult{}, fmt.Errorf("final sync (%s/%s): %w", workload, engineName, err)
	}

	elapsed := time.Since(t0)
	u1, s1 := getRusageTime()
	runtime.ReadMemStats(&mAfter)
	endStats := eng.Stats()

	verified := true
	if verifyOracle != nil {
		got := make([]byte, len(verifyOracle))
		n, rerr := eng.ReadAt(got, 0)
		if rerr != nil && rerr != io.EOF {
			return BenchResult{}, fmt.Errorf("verify read (%s/%s): %w", workload, engineName, rerr)
		}
		if n != len(verifyOracle) || !bytes.Equal(got, verifyOracle) {
			verified = false
			return BenchResult{}, fmt.Errorf("oracle verification FAILED for %s/%s (n=%d want=%d)", workload, engineName, n, len(verifyOracle))
		}
	}

	walBytes := endStats.walBytes - baseStats.walBytes
	backingBytes := endStats.backingBytes - baseStats.backingBytes
	totalPhys := walBytes + backingBytes
	physWrites := (endStats.walWrites - baseStats.walWrites) + (endStats.backingWrites - baseStats.backingWrites)
	fsyncs := (endStats.walSyncs - baseStats.walSyncs) + (endStats.backingSyncs - baseStats.backingSyncs)

	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1e-9
	}
	userNs := max(int64(0), u1-u0)
	sysNs := max(int64(0), s1-s0)
	cpuNs := float64(userNs + sysNs)

	var allocsDiff, allocBytesDiff uint64
	if mAfter.Mallocs >= mBefore.Mallocs {
		allocsDiff = mAfter.Mallocs - mBefore.Mallocs
	}
	if mAfter.TotalAlloc >= mBefore.TotalAlloc {
		allocBytesDiff = mAfter.TotalAlloc - mBefore.TotalAlloc
	}

	return BenchResult{
		Workload:             workload,
		Category:             category,
		Engine:               engineName,
		Ops:                  len(ops),
		SyncBatch:            syncBatch,
		LogicalBytes:         logicalBytes,
		ElapsedMs:            elapsed.Seconds() * 1000,
		ThroughputMiBs:       (float64(logicalBytes) / (1024 * 1024)) / sec,
		IOPS:                 float64(len(ops)) / sec,
		CPUNsPerOp:           cpuNs / float64(len(ops)),
		UserCPUMs:            float64(userNs) / 1e6,
		SysCPUMs:             float64(sysNs) / 1e6,
		AllocsPerOp:          float64(allocsDiff) / float64(len(ops)),
		AllocBytesPerOp:      float64(allocBytesDiff) / float64(len(ops)),
		WALPhysicalBytes:     walBytes,
		BackingPhysicalBytes: backingBytes,
		TotalPhysicalBytes:   totalPhys,
		WAF:                  float64(totalPhys) / float64(logicalBytes),
		WALWAF:               float64(walBytes) / float64(logicalBytes),
		BackingWAF:           float64(backingBytes) / float64(logicalBytes),
		PhysicalWrites:       physWrites,
		Fsyncs:               fsyncs,
		CoalescedWrites:      endStats.coalescedWrites,
		BypassWrites:         endStats.bypassWrites,
		BackpressureFlushes:  endStats.backpressureFlushes,
		Verified:             verified,
	}, nil
}

func runMedianMeasurement(
	reps int,
	workRoot, workload, category, engineName string,
	ops []writeOp,
	syncBatch int,
	prealloc int64,
	verifyOracle []byte,
	openFn func(dir string) (engine, error),
) (BenchResult, error) {
	if reps < 1 {
		reps = 1
	}
	runs := make([]BenchResult, 0, reps)
	for r := 0; r < reps; r++ {
		res, err := runSingleMeasurement(workRoot, workload, category, engineName, ops, syncBatch, prealloc, verifyOracle, openFn)
		if err != nil {
			return BenchResult{}, err
		}
		runs = append(runs, res)
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].ThroughputMiBs < runs[j].ThroughputMiBs
	})
	return runs[len(runs)/2], nil
}

// ---- Workload Generators ----

// genHotRandom4K generates 4 KiB aligned random writes with 80/20 hot-set locality.
func genHotRandom4K(seed int64, count int, spaceBytes int64) []writeOp {
	rng := rand.New(rand.NewSource(seed))
	totalPages := int(spaceBytes / 4096)
	hotPages := max(16, totalPages/8) // 12.5% of space receives 80% of updates
	ops := make([]writeOp, count)
	for i := 0; i < count; i++ {
		var pg int
		if rng.Intn(100) < 80 {
			pg = rng.Intn(hotPages)
		} else {
			pg = rng.Intn(totalPages)
		}
		ops[i] = writeOp{off: int64(pg) * 4096, size: 4096, seed: byte(i & 15)}
	}
	return ops
}

// genUniformRandom4K generates uniformly scattered 4 KiB random writes across spaceBytes.
func genUniformRandom4K(seed int64, count int, spaceBytes int64) []writeOp {
	rng := rand.New(rand.NewSource(seed))
	totalPages := int(spaceBytes / 4096)
	ops := make([]writeOp, count)
	for i := 0; i < count; i++ {
		pg := rng.Intn(totalPages)
		ops[i] = writeOp{off: int64(pg) * 4096, size: 4096, seed: byte(i & 15)}
	}
	return ops
}

// genPartialUnaligned generates unaligned partial writes (e.g., 512 B or 1.5 KiB)
// with SQLite WAL / metadata hot-region locality.
func genPartialUnaligned(seed int64, count, size int, spaceBytes int64) []writeOp {
	rng := rand.New(rand.NewSource(seed))
	hotSpan := max(int64(size*32), spaceBytes/8)
	ops := make([]writeOp, count)
	for i := 0; i < count; i++ {
		var off int64
		if rng.Intn(100) < 80 {
			off = rng.Int63n(hotSpan - int64(size))
		} else {
			off = rng.Int63n(spaceBytes - int64(size))
		}
		// Guarantee unaligned offset.
		if off%4096 == 0 {
			off += 128
		}
		ops[i] = writeOp{off: off, size: size, seed: byte(i & 15)}
	}
	return ops
}

// genAlignedSequential generates large page-aligned sequential writes wrapping spaceBytes.
func genAlignedSequential(count, size int, spaceBytes int64) []writeOp {
	ops := make([]writeOp, count)
	var off int64
	for i := 0; i < count; i++ {
		ops[i] = writeOp{off: off, size: size, seed: byte(i & 15)}
		off += int64(size)
		if off+int64(size) > spaceBytes {
			off = 0
		}
	}
	return ops
}

// genMixedOverlap generates interleaved 4 KiB unaligned writes (75%) and
// 256 KiB aligned writes (25%) over overlapping address space, plus an exact
// reference oracle byte slice for post-run linearizability verification.
func genMixedOverlap(seed int64, count int, spaceBytes int64) ([]writeOp, []byte) {
	rng := rand.New(rand.NewSource(seed))
	oracle := make([]byte, spaceBytes)
	var payloads4K [16][]byte
	var payloads256K [16][]byte
	for k := 0; k < 16; k++ {
		payloads4K[k] = make([]byte, 4096)
		fillPayload(payloads4K[k], byte(k+1))
		payloads256K[k] = make([]byte, 256<<10)
		fillPayload(payloads256K[k], byte(k+1))
	}

	ops := make([]writeOp, count)
	slots256K := int(spaceBytes / (256 << 10))
	for i := 0; i < count; i++ {
		s := byte(i & 15)
		if i%4 == 3 {
			off := int64(rng.Intn(slots256K)) * (256 << 10)
			ops[i] = writeOp{off: off, size: 256 << 10, seed: s}
			copy(oracle[off:off+(256<<10)], payloads256K[s])
		} else {
			off := rng.Int63n(spaceBytes - 4096)
			if off%4096 == 0 {
				off += 512
			}
			ops[i] = writeOp{off: off, size: 4096, seed: s}
			copy(oracle[off:off+4096], payloads4K[s])
		}
	}
	return ops, oracle
}

// RunSuite executes the complete matched-baseline benchmark suite.
func RunSuite(workRoot string, reps int, quick bool) (ReportBundle, error) {
	if err := os.MkdirAll(workRoot, 0o700); err != nil {
		return ReportBundle{}, err
	}

	scale := func(full, fast int) int {
		if quick {
			return fast
		}
		return full
	}

	const (
		segSize  = 8 << 20
		capacity = 64 << 20
	)

	type engineSpec struct {
		name   string
		openFn func(dir string) (engine, error)
	}

	type workloadSpec struct {
		name      string
		category  string
		ops       []writeOp
		syncBatch int
		prealloc  int64
		oracle    []byte
		engines   []engineSpec
	}

	stdEngines := func(prealloc int64, includeNoBypass bool) []engineSpec {
		list := []engineSpec{
			{"plain-disk", func(dir string) (engine, error) { return openPlainDisk(dir, prealloc) }},
			{"journal-main", func(dir string) (engine, error) { return openJournalMain(dir, prealloc, segSize, capacity) }},
		}
		if includeNoBypass {
			list = append(list, engineSpec{
				"wsbuffer-no-bypass",
				func(dir string) (engine, error) {
					return openWSBuffer(dir, prealloc, 16<<20, 4<<20, segSize, capacity)
				},
			})
		}
		list = append(list, engineSpec{
			"wsbuffer-journal",
			func(dir string) (engine, error) {
				return openWSBuffer(dir, prealloc, 64<<10, 4<<20, segSize, capacity)
			},
		})
		return list
	}

	mixedOps, mixedOracle := genMixedOverlap(404, scale(1024, 160), 8<<20)

	workloads := []workloadSpec{
		// 1. Primary 4 KiB random writes (buffered burst, final sync)
		{
			name:      "4k-rand-burst",
			category:  "primary",
			ops:       genHotRandom4K(101, scale(8192, 1024), 8<<20),
			syncBatch: 0,
			prealloc:  8 << 20,
			engines:   stdEngines(8<<20, false),
		},
		// 2. Primary 4 KiB random writes (durable fsync every 64 ops)
		{
			name:      "4k-rand-durable-batch64",
			category:  "primary",
			ops:       genHotRandom4K(102, scale(4096, 512), 4<<20),
			syncBatch: 64,
			prealloc:  4 << 20,
			engines:   stdEngines(4<<20, false),
		},
		// 3. 512 B unaligned partial writes (metadata / SQLite WAL-like)
		{
			name:      "partial-512b-unaligned",
			category:  "primary",
			ops:       genPartialUnaligned(201, scale(8192, 1024), 512, 256<<10),
			syncBatch: 128,
			prealloc:  256 << 10,
			engines:   stdEngines(256<<10, false),
		},
		// 4. 1.5 KiB unaligned cross-page partial writes
		{
			name:      "partial-1536b-unaligned",
			category:  "primary",
			ops:       genPartialUnaligned(202, scale(4096, 512), 1536, 512<<10),
			syncBatch: 64,
			prealloc:  512 << 10,
			engines:   stdEngines(512<<10, false),
		},
		// 5. Large aligned sequential writes: 64 KiB
		{
			name:      "large-seq-64k-aligned",
			category:  "primary",
			ops:       genAlignedSequential(scale(512, 64), 64<<10, 16<<20),
			syncBatch: 64,
			prealloc:  16 << 20,
			engines:   stdEngines(16<<20, true),
		},
		// 6. Large aligned sequential writes: 256 KiB
		{
			name:      "large-seq-256k-aligned",
			category:  "primary",
			ops:       genAlignedSequential(scale(256, 32), 256<<10, 16<<20),
			syncBatch: 32,
			prealloc:  16 << 20,
			engines:   stdEngines(16<<20, true),
		},
		// 7. Large aligned sequential writes: 1 MiB (also Negative Regime #2 vs plain-disk!)
		{
			name:      "large-seq-1m-aligned",
			category:  "negative",
			ops:       genAlignedSequential(scale(64, 16), 1<<20, 16<<20),
			syncBatch: 8,
			prealloc:  16 << 20,
			engines:   stdEngines(16<<20, true),
		},
		// 8. Mixed 4 KiB unaligned + 256 KiB aligned interleaved with oracle check
		{
			name:      "mixed-4k-unaligned-256k-aligned",
			category:  "mixed",
			ops:       mixedOps,
			syncBatch: 64,
			prealloc:  8 << 20,
			oracle:    mixedOracle,
			engines:   stdEngines(8<<20, false),
		},
		// 9. Negative Regime #1: synchronous fsync after every single 4 KiB write (batch=1)
		{
			name:      "neg-sync-every-4k",
			category:  "negative",
			ops:       genHotRandom4K(501, scale(256, 64), 4<<20),
			syncBatch: 1,
			prealloc:  4 << 20,
			engines:   stdEngines(4<<20, false),
		},
		// 10. Negative Regime #3: undersized scrap buffer (64 KiB) under wide 16 MiB random scatter
		{
			name:      "neg-undersized-scrap-scatter",
			category:  "negative",
			ops:       genUniformRandom4K(503, scale(4096, 512), 16<<20),
			syncBatch: 0,
			prealloc:  16 << 20,
			engines: []engineSpec{
				{"journal-main", func(dir string) (engine, error) { return openJournalMain(dir, 16<<20, segSize, capacity) }},
				{"wsbuffer-cap-64k", func(dir string) (engine, error) { return openWSBuffer(dir, 16<<20, 64<<10, 64<<10, segSize, capacity) }},
				{"wsbuffer-cap-4m", func(dir string) (engine, error) { return openWSBuffer(dir, 16<<20, 64<<10, 4<<20, segSize, capacity) }},
			},
		},
	}

	bundle := ReportBundle{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		GoVersion:   runtime.Version(),
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		NumCPU:      runtime.NumCPU(),
		WorkDir:     workRoot,
		Reps:        reps,
		Quick:       quick,
	}

	for _, wl := range workloads {
		for _, eng := range wl.engines {
			res, err := runMedianMeasurement(reps, workRoot, wl.name, wl.category, eng.name, wl.ops, wl.syncBatch, wl.prealloc, wl.oracle, eng.openFn)
			if err != nil {
				return bundle, err
			}
			bundle.Results = append(bundle.Results, res)
		}
	}
	return bundle, nil
}

func printTable(w io.Writer, results []BenchResult) {
	fmt.Fprintf(w, "%-32s %-18s %9s %9s %9s %8s %7s %8s %7s\n",
		"WORKLOAD", "ENGINE", "MiB/s", "IOPS", "CPU ns/op", "B/op", "WAF", "WAL-WAF", "FSYNCS")
	for _, r := range results {
		fmt.Fprintf(w, "%-32s %-18s %9.2f %9.0f %9.0f %8.0f %7.3f %8.3f %7d\n",
			r.Workload, r.Engine, r.ThroughputMiBs, r.IOPS, r.CPUNsPerOp,
			r.AllocBytesPerOp, r.WAF, r.WALWAF, r.Fsyncs)
	}
}

func main() {
	var (
		dirFlag  = flag.String("dir", "", "local storage directory for matched benchmarks (default: temp dir)")
		jsonOut  = flag.String("json", "", "path to write raw JSON results")
		repsFlag = flag.Int("reps", 3, "repetitions per workload/engine (median reported)")
		quick    = flag.Bool("quick", false, "run reduced op counts for fast verification")
	)
	flag.Parse()

	workDir := *dirFlag
	cleanup := false
	if workDir == "" {
		tmp, err := os.MkdirTemp("", "wsbuffer-bench-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "mkdtemp: %v\n", err)
			os.Exit(1)
		}
		workDir = tmp
		cleanup = true
	}
	if cleanup {
		defer os.RemoveAll(workDir)
	}

	bundle, err := RunSuite(workDir, *repsFlag, *quick)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}

	printTable(os.Stdout, bundle.Results)

	if *jsonOut != "" {
		if err := os.MkdirAll(filepath.Dir(*jsonOut), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir json dir: %v\n", err)
			os.Exit(1)
		}
		raw, err := json.MarshalIndent(bundle, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal json: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*jsonOut, append(raw, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write json: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", *jsonOut)
	}
}
