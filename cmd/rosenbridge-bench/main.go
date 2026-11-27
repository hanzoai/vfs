// Command rosenbridge-bench executes reproducible OS-backed and canonical
// hanzoai/vfs (*vfs.Volume) benchmarks comparing the canonical Gofer baseline
// against the conditional RosenBridge lower-copy fast path, including negative
// regimes (tiny 64B metadata/stat-heavy workloads and saturated-ring fallback)
// and a 7-point isolation/security verification suite.
//
// Usage:
//
//	go run ./cmd/rosenbridge-bench -seed 1345 -out docs/research/hip-1345-rosenbridge/results.json
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/rosenbridge"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

type procIO struct {
	Available  bool
	RChar      uint64
	WChar      uint64
	Syscr      uint64
	Syscw      uint64
	ReadBytes  uint64
	WriteBytes uint64
}

func readProcIO() procIO {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return procIO{}
	}
	defer f.Close()
	out := procIO{Available: true}
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
			out.RChar = v
		case "wchar":
			out.WChar = v
		case "syscr":
			out.Syscr = v
		case "syscw":
			out.Syscw = v
		case "read_bytes":
			out.ReadBytes = v
		case "write_bytes":
			out.WriteBytes = v
		}
	}
	return out
}

type WorkloadResult struct {
	Workload              string  `json:"workload"`
	Mode                  string  `json:"mode"`
	BlockBytes            int     `json:"block_bytes"`
	Ops                   int     `json:"ops"`
	TotalBytes            int64   `json:"total_bytes"`
	RuntimeMs             float64 `json:"runtime_ms"`
	ThroughputMBs         float64 `json:"throughput_mb_s"`
	IOPS                  float64 `json:"iops"`
	P50LatencyUs          float64 `json:"p50_latency_us"`
	P95LatencyUs          float64 `json:"p95_latency_us"`
	P99LatencyUs          float64 `json:"p99_latency_us"`
	InstrumentedSyscalls  int64   `json:"instrumented_syscalls"`
	SyscallsPerOp         float64 `json:"syscalls_per_op"`
	KernelProcIOSyscalls  uint64  `json:"kernel_proc_io_syscalls"`
	KernelProcSyscallsOp  float64 `json:"kernel_proc_syscalls_per_op"`
	KernelProcRCharDelta  uint64  `json:"kernel_proc_rchar_delta"`
	KernelProcWCharDelta  uint64  `json:"kernel_proc_wchar_delta"`
	TotalCopies           int64   `json:"total_copies"`
	CopiesPerOp           float64 `json:"copies_per_op"`
	HeapAllocBytes        uint64  `json:"heap_alloc_bytes"`
	HeapAllocBytesPerOp   float64 `json:"heap_alloc_bytes_per_op"`
	HeapMallocs           uint64  `json:"heap_mallocs"`
	HeapMallocsPerOp      float64 `json:"heap_mallocs_per_op"`
	FastPathOps           int64   `json:"fast_path_ops"`
	GoferOps              int64   `json:"gofer_ops"`
	FallbackSmallIO       int64   `json:"fallback_small_io,omitempty"`
	FallbackUnsupportedOp int64   `json:"fallback_unsupported_op,omitempty"`
	FallbackRingFull      int64   `json:"fallback_ring_full,omitempty"`
}

type IsolationCheck struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

type BenchmarkReport struct {
	GeneratedAtUTC  string                     `json:"generated_at_utc"`
	Seed            uint64                     `json:"seed"`
	GoVersion       string                     `json:"go_version"`
	GOOS            string                     `json:"goos"`
	GOARCH          string                     `json:"goarch"`
	NumCPU          int                        `json:"num_cpu"`
	ProcIOAvailable bool                       `json:"proc_io_available"`
	GoferProfile    rosenbridge.ProfileSummary `json:"gofer_bottleneck_profile"`
	Workloads       []WorkloadResult           `json:"workloads"`
	IsolationChecks []IsolationCheck           `json:"isolation_checks"`
	IsolationPassed int                        `json:"isolation_passed"`
	IsolationTotal  int                        `json:"isolation_total"`
}

func percentile(sorted []int64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx]) / 1000.0
}

func measureRun(
	workload, mode string,
	blockBytes, ops int,
	fn func(lats []int64) (totalBytes int64, stats rosenbridge.RouterStats, err error),
) (WorkloadResult, error) {
	runtime.GC()
	var msBefore, msAfter runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	ioBefore := readProcIO()

	lats := make([]int64, ops)
	t0 := time.Now()
	totalBytes, stats, err := fn(lats)
	elapsed := time.Since(t0)

	ioAfter := readProcIO()
	runtime.ReadMemStats(&msAfter)
	if err != nil {
		return WorkloadResult{}, err
	}

	slices.Sort(lats)
	runtimeMs := float64(elapsed.Nanoseconds()) / 1e6
	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1e-9
	}

	instSyscalls := stats.Gofer.TotalSyscalls + stats.FastPath.Syscalls
	totalCopies := stats.Gofer.Copies + stats.FastPath.Copies

	var procSc, rcharDelta, wcharDelta uint64
	if ioBefore.Available && ioAfter.Available {
		if ioAfter.Syscr >= ioBefore.Syscr {
			procSc += ioAfter.Syscr - ioBefore.Syscr
		}
		if ioAfter.Syscw >= ioBefore.Syscw {
			procSc += ioAfter.Syscw - ioBefore.Syscw
		}
		if ioAfter.RChar >= ioBefore.RChar {
			rcharDelta = ioAfter.RChar - ioBefore.RChar
		}
		if ioAfter.WChar >= ioBefore.WChar {
			wcharDelta = ioAfter.WChar - ioBefore.WChar
		}
	}

	var allocDelta, mallocDelta uint64
	if msAfter.TotalAlloc >= msBefore.TotalAlloc {
		allocDelta = msAfter.TotalAlloc - msBefore.TotalAlloc
	}
	if msAfter.Mallocs >= msBefore.Mallocs {
		mallocDelta = msAfter.Mallocs - msBefore.Mallocs
	}

	opF := float64(max(ops, 1))
	return WorkloadResult{
		Workload:              workload,
		Mode:                  mode,
		BlockBytes:            blockBytes,
		Ops:                   ops,
		TotalBytes:            totalBytes,
		RuntimeMs:             round3(runtimeMs),
		ThroughputMBs:         round2((float64(totalBytes) / (1024 * 1024)) / sec),
		IOPS:                  round1(float64(ops) / sec),
		P50LatencyUs:          round2(percentile(lats, 0.50)),
		P95LatencyUs:          round2(percentile(lats, 0.95)),
		P99LatencyUs:          round2(percentile(lats, 0.99)),
		InstrumentedSyscalls:  instSyscalls,
		SyscallsPerOp:         round2(float64(instSyscalls) / opF),
		KernelProcIOSyscalls:  procSc,
		KernelProcSyscallsOp:  round2(float64(procSc) / opF),
		KernelProcRCharDelta:  rcharDelta,
		KernelProcWCharDelta:  wcharDelta,
		TotalCopies:           totalCopies,
		CopiesPerOp:           round2(float64(totalCopies) / opF),
		HeapAllocBytes:        allocDelta,
		HeapAllocBytesPerOp:   round1(float64(allocDelta) / opF),
		HeapMallocs:           mallocDelta,
		HeapMallocsPerOp:      round2(float64(mallocDelta) / opF),
		FastPathOps:           stats.FastPath.Ops,
		GoferOps:              stats.Gofer.Ops,
		FallbackSmallIO:       stats.Gate.FallbackSmallIO,
		FallbackUnsupportedOp: stats.Gate.FallbackUnsupportedOp,
		FallbackRingFull:      stats.Gate.FallbackRingFull,
	}, nil
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round2(v float64) float64 { return math.Round(v*100) / 100 }
func round3(v float64) float64 { return math.Round(v*1000) / 1000 }

func deterministicData(seed uint64, n int) []byte {
	rng := rand.New(rand.NewPCG(seed, seed*7919+1))
	b := make([]byte, n)
	for i := 0; i < n; i += 8 {
		v := rng.Uint64()
		for j := 0; j < 8 && i+j < n; j++ {
			b[i+j] = byte(v >> (8 * j))
		}
	}
	return b
}

func runOSReadBench(ctx context.Context, baseDir string, blockSize, ops int, mode string, fileSize int) (WorkloadResult, error) {
	store, err := rosenbridge.NewOSBackingStore(baseDir)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer store.Close()

	cfg := rosenbridge.DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	if mode == "gofer_baseline" {
		cfg.Enabled = false
	}
	router, err := rosenbridge.NewRouter(store, cfg)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer router.Close()

	if _, err := router.RegisterTenant("bench-tenant", 32, uint64(max(blockSize, 65536))); err != nil {
		return WorkloadResult{}, err
	}

	// Prime FD cache with one unmeasured stat/open so steady-state compares IO path fairly.
	if _, _, err := store.ReadAt(ctx, "bench-tenant", "seq.bin", make([]byte, 4096), 0); err != nil {
		return WorkloadResult{}, err
	}

	return measureRun(fmt.Sprintf("os_seq_read_%dk", blockSize/1024), mode, blockSize, ops, func(lats []int64) (int64, rosenbridge.RouterStats, error) {
		var total int64
		buf := make([]byte, blockSize)
		for i := 0; i < ops; i++ {
			off := int64((i * blockSize) % (fileSize - blockSize + 1))
			t0 := time.Now()
			switch mode {
			case "gofer_baseline", "rosenbridge_slice":
				n, _, err := router.ReadAt(ctx, "bench-tenant", "seq.bin", buf, off)
				if err != nil {
					return 0, rosenbridge.RouterStats{}, err
				}
				total += int64(n)
			case "rosenbridge_slab_1copy":
				data, rel, _, err := router.ReadSlab(ctx, "bench-tenant", "seq.bin", off, blockSize)
				if err != nil {
					return 0, rosenbridge.RouterStats{}, err
				}
				// Touch first and last byte to ensure guest actually reads slab pages.
				_ = data[0] ^ data[len(data)-1]
				total += int64(len(data))
				rel()
			}
			lats[i] = time.Since(t0).Nanoseconds()
		}
		return total, router.Stats(), nil
	})
}

func runOSWriteBench(ctx context.Context, seed uint64, blockSize, ops int, mode string) (WorkloadResult, error) {
	dir, err := os.MkdirTemp("", "rosenbridge-write-*")
	if err != nil {
		return WorkloadResult{}, err
	}
	defer os.RemoveAll(dir)

	store, err := rosenbridge.NewOSBackingStore(dir)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer store.Close()

	cfg := rosenbridge.DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	if mode == "gofer_baseline" {
		cfg.Enabled = false
	}
	router, err := rosenbridge.NewRouter(store, cfg)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer router.Close()

	ch, err := router.RegisterTenant("bench-tenant", 32, uint64(max(blockSize, 65536)))
	if err != nil {
		return WorkloadResult{}, err
	}

	payload := deterministicData(seed+17, blockSize)
	// Create target file and pre-fault slab slot 0 ahead of steady-state timing (models VM boot-time RAM mapping).
	if _, _, err := store.WriteAt(ctx, "bench-tenant", "write.bin", payload, 0, false); err != nil {
		return WorkloadResult{}, err
	}
	if idx, _, s, err := ch.Slab().AcquireSlot("bench-tenant", ch.Token(), uint64(blockSize)); err == nil {
		clear(s)
		ch.Slab().ReleaseSlot(idx)
	}

	return measureRun(fmt.Sprintf("os_seq_write_%dk", blockSize/1024), mode, blockSize, ops, func(lats []int64) (int64, rosenbridge.RouterStats, error) {
		var total int64
		for i := 0; i < ops; i++ {
			off := int64(i * blockSize)
			t0 := time.Now()
			switch mode {
			case "gofer_baseline", "rosenbridge_slice":
				n, _, err := router.WriteAt(ctx, "bench-tenant", "write.bin", payload, off, false)
				if err != nil {
					return 0, rosenbridge.RouterStats{}, err
				}
				total += int64(n)
			case "rosenbridge_slab_1copy":
				n, _, err := router.WriteSlab(ctx, "bench-tenant", "write.bin", off, blockSize, false, func(dst []byte) int {
					// Producer writes directly into the shared mmap slab (1 copy total: slab -> backing pwrite).
					for p := 0; p < len(dst); p += rosenbridge.PageSize {
						dst[p] = byte(i + p)
					}
					dst[len(dst)-1] = byte(i ^ 0x5a)
					return len(dst)
				})
				if err != nil {
					return 0, rosenbridge.RouterStats{}, err
				}
				total += int64(n)
			}
			lats[i] = time.Since(t0).Nanoseconds()
		}
		return total, router.Stats(), nil
	})
}

func runCanonicalVFSBench(ctx context.Context, seed uint64, blockSize, ops int, mode string) (WorkloadResult, error) {
	dir, err := os.MkdirTemp("", "rosenbridge-vfs-*")
	if err != nil {
		return WorkloadResult{}, err
	}
	defer os.RemoveAll(dir)

	be, err := backend.Open(ctx, "file://"+filepath.Join(dir, "store"))
	if err != nil {
		return WorkloadResult{}, err
	}
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		return WorkloadResult{}, err
	}
	vcfg := vfs.Config{
		Backend:         be,
		Cipher:          vfs.Hybrid(id),
		Round:           0,
		Journal:         filepath.Join(dir, "journal"),
		JournalCapacity: 128 << 20,
	}
	if err := vfs.Create(ctx, vcfg); err != nil {
		return WorkloadResult{}, err
	}
	vcfg.Round = 1
	vol, err := vfs.Attach(ctx, vcfg)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer vol.Close()

	vfsStore := rosenbridge.NewVFSBackingStore()
	if err := vfsStore.MountTenant("vfs-tenant", vol); err != nil {
		return WorkloadResult{}, err
	}
	defer vfsStore.Close()

	// Seed a 4 MiB file inside the canonical post-quantum-sealed *vfs.Volume.
	const seedSize = 4 << 20
	seedBuf := deterministicData(seed+99, seedSize)
	if _, _, err := vfsStore.WriteAt(ctx, "vfs-tenant", "dataset/corpus.bin", seedBuf, 0, true); err != nil {
		return WorkloadResult{}, err
	}

	gcfg := rosenbridge.DefaultGateConfig()
	gcfg.RequireProfilerConfirmation = false
	if mode == "gofer_baseline" {
		gcfg.Enabled = false
	}
	router, err := rosenbridge.NewRouter(vfsStore, gcfg)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer router.Close()

	if _, err := router.RegisterTenant("vfs-tenant", 32, uint64(max(blockSize, 65536))); err != nil {
		return WorkloadResult{}, err
	}

	return measureRun("canonical_vfs_read_64k", mode, blockSize, ops, func(lats []int64) (int64, rosenbridge.RouterStats, error) {
		var total int64
		buf := make([]byte, blockSize)
		for i := 0; i < ops; i++ {
			off := int64((i * blockSize) % (seedSize - blockSize + 1))
			t0 := time.Now()
			switch mode {
			case "gofer_baseline", "rosenbridge_slice":
				n, _, err := router.ReadAt(ctx, "vfs-tenant", "dataset/corpus.bin", buf, off)
				if err != nil {
					return 0, rosenbridge.RouterStats{}, err
				}
				total += int64(n)
			case "rosenbridge_slab_1copy":
				data, rel, _, err := router.ReadSlab(ctx, "vfs-tenant", "dataset/corpus.bin", off, blockSize)
				if err != nil {
					return 0, rosenbridge.RouterStats{}, err
				}
				_ = data[0] ^ data[len(data)-1]
				total += int64(len(data))
				rel()
			}
			lats[i] = time.Since(t0).Nanoseconds()
		}
		return total, router.Stats(), nil
	})
}

func runNegativeTinyMetadataBench(ctx context.Context, baseDir string, seed uint64, ops int, mode string, fileSize int) (WorkloadResult, error) {
	store, err := rosenbridge.NewOSBackingStore(baseDir)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer store.Close()

	cfg := rosenbridge.DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	switch mode {
	case "gofer_baseline":
		cfg.Enabled = false
	case "rosenbridge_gated_fallback":
		// Default MinTransferBytes=4096 forces 64B reads & Stat onto canonical gofer
		// without ever provisioning a tenant mmap slab!
		cfg.Enabled = true
	case "ungated_fastpath_negative_control":
		// Intentionally disable size/sequentiality gating and eagerly provision+teardown
		// a per-tenant 2 MiB mmap slab per short-lived metadata burst to measure setup overhead.
		cfg.Enabled = true
		cfg.MinTransferBytes = 1
		cfg.BulkBypassBytes = 1
	}

	router, err := rosenbridge.NewRouter(store, cfg)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer router.Close()

	// Warm FD cache.
	if _, _, err := store.ReadAt(ctx, "bench-tenant", "seq.bin", make([]byte, 64), 0); err != nil {
		return WorkloadResult{}, err
	}

	rng := rand.New(rand.NewPCG(seed+500, 9999))
	return measureRun("negative_tiny_64b_and_stat", mode, 64, ops, func(lats []int64) (int64, rosenbridge.RouterStats, error) {
		var total int64
		var extraFP rosenbridge.ChannelMetrics
		buf := make([]byte, 64)
		// Model 20 short-lived metadata/tiny-IO bursts (ops/20 ops each).
		// Ungated fast path eagerly provisions & tears down a 2 MiB mmap slab per burst;
		// gated fallback never triggers fast path and pays zero slab setup/teardown cost.
		const bursts = 20
		perBurst := max(ops/bursts, 1)
		opIdx := 0
		for b := 0; b < bursts && opIdx < ops; b++ {
			const tenant = "bench-tenant"
			var ephem *rosenbridge.RosenBridgeChannel
			if mode == "ungated_fastpath_negative_control" {
				var err error
				ephem, err = rosenbridge.NewRosenBridgeChannel(tenant, store, 32, 65536)
				if err != nil {
					return 0, rosenbridge.RouterStats{}, err
				}
			}
			for j := 0; j < perBurst && opIdx < ops; j++ {
				t0 := time.Now()
				if opIdx%5 != 0 {
					// 80% metadata Stat (unsupported on data-plane fast path).
					if _, _, err := router.Stat(ctx, tenant, "seq.bin"); err != nil {
						if ephem != nil {
							_ = ephem.Close()
						}
						return 0, rosenbridge.RouterStats{}, err
					}
				} else {
					// 20% tiny 64B random read.
					off := int64(rng.IntN(fileSize - 64))
					if ephem != nil {
						n, err := ephem.ReadAt(ctx, "seq.bin", buf, off, false)
						if err != nil {
							_ = ephem.Close()
							return 0, rosenbridge.RouterStats{}, err
						}
						total += int64(n)
					} else {
						n, _, err := router.ReadAt(ctx, tenant, "seq.bin", buf, off)
						if err != nil {
							return 0, rosenbridge.RouterStats{}, err
						}
						total += int64(n)
					}
				}
				lats[opIdx] = time.Since(t0).Nanoseconds()
				opIdx++
			}
			if ephem != nil {
				m := ephem.Metrics()
				extraFP.Ops += m.Ops
				extraFP.Bytes += m.Bytes
				extraFP.Syscalls += m.Syscalls
				extraFP.Copies += m.Copies
				_ = ephem.Close()
			}
		}
		st := router.Stats()
		st.FastPath.Ops += extraFP.Ops
		st.FastPath.Bytes += extraFP.Bytes
		st.FastPath.Syscalls += extraFP.Syscalls
		st.FastPath.Copies += extraFP.Copies
		return total, st, nil
	})
}

func runRingSaturationFallbackBench(ctx context.Context, baseDir string, ops int) (WorkloadResult, error) {
	store, err := rosenbridge.NewOSBackingStore(baseDir)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer store.Close()

	cfg := rosenbridge.DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	router, err := rosenbridge.NewRouter(store, cfg)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer router.Close()

	// 2-slot ring, saturate both slots to force graceful fallback to gofer.
	if _, err := router.RegisterTenant("bench-tenant", 2, 65536); err != nil {
		return WorkloadResult{}, err
	}
	_, rel1, _, err := router.ReadSlab(ctx, "bench-tenant", "seq.bin", 0, 16384)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer rel1()
	_, rel2, _, err := router.ReadSlab(ctx, "bench-tenant", "seq.bin", 16384, 16384)
	if err != nil {
		return WorkloadResult{}, err
	}
	defer rel2()

	return measureRun("negative_ring_saturated_fallback", "rosenbridge_auto_fallback", 16384, ops, func(lats []int64) (int64, rosenbridge.RouterStats, error) {
		var total int64
		buf := make([]byte, 16384)
		for i := 0; i < ops; i++ {
			t0 := time.Now()
			n, reason, err := router.ReadAt(ctx, "bench-tenant", "seq.bin", buf, int64(i*4096%(1<<20)))
			if err != nil {
				return 0, rosenbridge.RouterStats{}, err
			}
			if reason != rosenbridge.FallbackRingFull {
				return 0, rosenbridge.RouterStats{}, fmt.Errorf("expected FallbackRingFull, got %s", reason)
			}
			total += int64(n)
			lats[i] = time.Since(t0).Nanoseconds()
		}
		return total, router.Stats(), nil
	})
}

func runIsolationSuite(ctx context.Context) []IsolationCheck {
	checks := make([]IsolationCheck, 0, 7)

	dir, err := os.MkdirTemp("", "rosenbridge-iso-*")
	if err != nil {
		return []IsolationCheck{{Name: "setup", Passed: false, Detail: err.Error()}}
	}
	defer os.RemoveAll(dir)

	outsideDir, err := os.MkdirTemp("", "rosenbridge-outside-*")
	if err != nil {
		return []IsolationCheck{{Name: "setup_outside", Passed: false, Detail: err.Error()}}
	}
	defer os.RemoveAll(outsideDir)
	outsideFile := filepath.Join(outsideDir, "secret.key")
	_ = os.WriteFile(outsideFile, []byte("HOST-SECRET"), 0o600)

	store, err := rosenbridge.NewOSBackingStore(dir)
	if err != nil {
		return []IsolationCheck{{Name: "store", Passed: false, Detail: err.Error()}}
	}
	cfg := rosenbridge.DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	router, err := rosenbridge.NewRouter(store, cfg)
	if err != nil {
		_ = store.Close()
		return []IsolationCheck{{Name: "router", Passed: false, Detail: err.Error()}}
	}

	chA, _ := router.RegisterTenant("tenant-a", 8, 65536)
	chB, _ := router.RegisterTenant("tenant-b", 8, 65536)
	_, _, _ = store.WriteAt(ctx, "tenant-a", "data.bin", bytes.Repeat([]byte("A"), 16384), 0, false)
	rootA, _ := store.TenantRoot("tenant-a")
	_ = os.Symlink(outsideFile, filepath.Join(rootA, "link.key"))

	// 1. Cross-tenant slab acquisition rejected.
	_, _, _, err = chA.Slab().AcquireSlot("tenant-b", chB.Token(), 4096)
	checks = append(checks, IsolationCheck{
		Name:   "cross_tenant_slab_access_rejected",
		Passed: errors.Is(err, rosenbridge.ErrTenantMismatch),
		Detail: fmt.Sprintf("err=%v errno=%v", err, rosenbridge.Errno(err)),
	})

	// 2. Forged tenant token descriptor rejected.
	_, err = chA.SubmitRawDescriptor(ctx, rosenbridge.Descriptor{
		Op: rosenbridge.OpRead, TenantID: "tenant-a", TenantToken: chA.Token() ^ 0x1,
		Path: "data.bin", FileOffset: 0, SlabOffset: 0, Length: 4096, DeclaredLen: 4096,
	})
	checks = append(checks, IsolationCheck{
		Name:   "forged_tenant_token_rejected",
		Passed: errors.Is(err, rosenbridge.ErrTenantMismatch),
		Detail: fmt.Sprintf("err=%v errno=%v", err, rosenbridge.Errno(err)),
	})

	// 3. Out-of-bounds / uint64-overflow descriptor rejected.
	_, err = chA.SubmitRawDescriptor(ctx, rosenbridge.Descriptor{
		Op: rosenbridge.OpRead, TenantID: "tenant-a", TenantToken: chA.Token(),
		Path: "data.bin", FileOffset: 0, SlabOffset: math.MaxUint64 - 512, Length: 4096, DeclaredLen: 4096,
	})
	checks = append(checks, IsolationCheck{
		Name:   "descriptor_uint64_overflow_rejected",
		Passed: errors.Is(err, rosenbridge.ErrDescriptorOutOfBounds),
		Detail: fmt.Sprintf("err=%v errno=%v", err, rosenbridge.Errno(err)),
	})

	// 4. Overlapping in-flight slab descriptor rejected.
	_, _ = chA.Slab().ReserveRange("tenant-a", chA.Token(), 0, 8192)
	_, err = chA.SubmitRawDescriptor(ctx, rosenbridge.Descriptor{
		Op: rosenbridge.OpRead, TenantID: "tenant-a", TenantToken: chA.Token(),
		Path: "data.bin", FileOffset: 0, SlabOffset: 4096, Length: 8192, DeclaredLen: 8192,
	})
	chA.Slab().ReleaseRange(0, 8192)
	checks = append(checks, IsolationCheck{
		Name:   "overlapping_slab_descriptor_rejected",
		Passed: errors.Is(err, rosenbridge.ErrDescriptorOverlap),
		Detail: fmt.Sprintf("err=%v errno=%v", err, rosenbridge.Errno(err)),
	})

	// 5. Truncated descriptor rejected.
	_, err = chA.SubmitRawDescriptor(ctx, rosenbridge.Descriptor{
		Op: rosenbridge.OpRead, TenantID: "tenant-a", TenantToken: chA.Token(),
		Path: "data.bin", FileOffset: 0, SlabOffset: 0, Length: 4096, DeclaredLen: 2048,
	})
	checks = append(checks, IsolationCheck{
		Name:   "truncated_descriptor_rejected",
		Passed: errors.Is(err, rosenbridge.ErrDescriptorTruncated),
		Detail: fmt.Sprintf("err=%v errno=%v", err, rosenbridge.Errno(err)),
	})

	// 6. Symlink & path traversal escape rejected on both fast path and gofer.
	_, _, err1 := router.ReadAt(ctx, "tenant-a", "link.key", make([]byte, 16384), 0)
	_, err2 := router.Gofer().ReadAt(ctx, "tenant-a", "../tenant-a/data.bin", make([]byte, 4096), 0)
	checks = append(checks, IsolationCheck{
		Name:   "symlink_and_traversal_confinement",
		Passed: errors.Is(err1, rosenbridge.ErrPathEscape) && errors.Is(err2, rosenbridge.ErrPathEscape),
		Detail: fmt.Sprintf("symlink_err=%v traversal_err=%v", err1, err2),
	})

	// 7. Zero leaked FDs and zero leaked mmap regions after Close.
	_ = router.Close()
	_ = store.Close()
	mA, fA := chA.Slab().OpenResources()
	mB, fB := chB.Slab().OpenResources()
	openFDs := store.OpenFDs()
	checks = append(checks, IsolationCheck{
		Name:   "zero_leaked_fds_and_mmaps",
		Passed: mA == 0 && fA == 0 && mB == 0 && fB == 0 && openFDs == 0,
		Detail: fmt.Sprintf("slabA=(mmap:%d,fd:%d) slabB=(mmap:%d,fd:%d) storeFDs=%d", mA, fA, mB, fB, openFDs),
	})

	return checks
}

func main() {
	var (
		seed   = flag.Uint64("seed", 1345, "deterministic PRNG seed")
		out    = flag.String("out", "docs/research/hip-1345-rosenbridge/results.json", "output JSON receipts path")
		readMB = flag.Int("read-mb", 64, "total MiB read in bulk sequential read benchmarks")
		wrMB   = flag.Int("write-mb", 32, "total MiB written in bulk sequential write benchmarks")
		vfsMB  = flag.Int("vfs-mb", 64, "total MiB read in canonical *vfs.Volume benchmarks")
		negOps = flag.Int("neg-ops", 2000, "operations in tiny 64B/stat negative-regime benchmark")
	)
	flag.Parse()

	ctx := context.Background()

	// Seed a shared 16 MiB OS-backed file so Gofer baseline and RosenBridge read identical OS storage.
	baseDir, err := os.MkdirTemp("", "rosenbridge-bench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdirtemp: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(baseDir)

	const backingFileSize = 16 << 20
	seedStore, err := rosenbridge.NewOSBackingStore(baseDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewOSBackingStore: %v\n", err)
		os.Exit(1)
	}
	seedBytes := deterministicData(*seed, backingFileSize)
	if _, _, err := seedStore.WriteAt(ctx, "bench-tenant", "seq.bin", seedBytes, 0, true); err != nil {
		fmt.Fprintf(os.Stderr, "seed write: %v\n", err)
		os.Exit(1)
	}
	_ = seedStore.Close()

	// Step 1: Profile canonical Gofer first ("Only after the gofer path is measured as a bottleneck").
	profStore, err := rosenbridge.NewOSBackingStore(baseDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "profStore: %v\n", err)
		os.Exit(1)
	}
	profGateCfg := rosenbridge.DefaultGateConfig()
	profGateCfg.RequireProfilerConfirmation = true
	profGateCfg.MinProfileSamples = 64
	profRouter, err := rosenbridge.NewRouter(profStore, profGateCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "profRouter: %v\n", err)
		os.Exit(1)
	}
	profBuf := make([]byte, 65536)
	for i := 0; i < 64; i++ {
		if _, _, err := profRouter.ReadAt(ctx, "bench-tenant", "seq.bin", profBuf, int64(i*65536)); err != nil {
			fmt.Fprintf(os.Stderr, "profile read %d: %v\n", i, err)
			os.Exit(1)
		}
	}
	goferProfile := profRouter.Gate().Profiler().Summary()
	_ = profRouter.Close()
	_ = profStore.Close()

	var results []WorkloadResult

	// Step 2: OS-backed 64 KiB sequential reads (Gofer vs RosenBridge Slice vs RosenBridge 1-Copy Slab).
	ops64k := (*readMB << 20) / 65536
	for _, mode := range []string{"gofer_baseline", "rosenbridge_slice", "rosenbridge_slab_1copy"} {
		r, err := runOSReadBench(ctx, baseDir, 65536, ops64k, mode, backingFileSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "64k read %s: %v\n", mode, err)
			os.Exit(1)
		}
		results = append(results, r)
	}

	// Step 3: OS-backed 256 KiB sequential reads.
	ops256k := (*readMB << 20) / 262144
	for _, mode := range []string{"gofer_baseline", "rosenbridge_slice", "rosenbridge_slab_1copy"} {
		r, err := runOSReadBench(ctx, baseDir, 262144, ops256k, mode, backingFileSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "256k read %s: %v\n", mode, err)
			os.Exit(1)
		}
		results = append(results, r)
	}

	// Step 4: OS-backed 64 KiB sequential writes.
	wrOps64k := (*wrMB << 20) / 65536
	for _, mode := range []string{"gofer_baseline", "rosenbridge_slice", "rosenbridge_slab_1copy"} {
		r, err := runOSWriteBench(ctx, *seed, 65536, wrOps64k, mode)
		if err != nil {
			fmt.Fprintf(os.Stderr, "64k write %s: %v\n", mode, err)
			os.Exit(1)
		}
		results = append(results, r)
	}

	// Step 5: Canonical hanzoai/vfs (*vfs.Volume) 64 KiB sequential reads.
	vfsOps := (*vfsMB << 20) / 65536
	for _, mode := range []string{"gofer_baseline", "rosenbridge_slice", "rosenbridge_slab_1copy"} {
		r, err := runCanonicalVFSBench(ctx, *seed, 65536, vfsOps, mode)
		if err != nil {
			fmt.Fprintf(os.Stderr, "canonical vfs %s: %v\n", mode, err)
			os.Exit(1)
		}
		results = append(results, r)
	}

	// Step 6: Negative Regime 1 — Tiny 64B random reads + Stat (Gofer vs Gated Fallback vs Ungated Control).
	for _, mode := range []string{"gofer_baseline", "rosenbridge_gated_fallback", "ungated_fastpath_negative_control"} {
		r, err := runNegativeTinyMetadataBench(ctx, baseDir, *seed, *negOps, mode, backingFileSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "negative tiny %s: %v\n", mode, err)
			os.Exit(1)
		}
		results = append(results, r)
	}

	// Step 7: Negative Regime 2 — Saturated Ring automatic fallback.
	satRes, err := runRingSaturationFallbackBench(ctx, baseDir, 200)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ring saturation: %v\n", err)
		os.Exit(1)
	}
	results = append(results, satRes)

	// Step 8: 7-point Isolation & Security verification suite.
	isoChecks := runIsolationSuite(ctx)
	isoPassed := 0
	for _, c := range isoChecks {
		if c.Passed {
			isoPassed++
		}
	}

	report := BenchmarkReport{
		GeneratedAtUTC:  time.Now().UTC().Format(time.RFC3339),
		Seed:            *seed,
		GoVersion:       runtime.Version(),
		GOOS:            runtime.GOOS,
		GOARCH:          runtime.GOARCH,
		NumCPU:          runtime.NumCPU(),
		ProcIOAvailable: readProcIO().Available,
		GoferProfile:    goferProfile,
		Workloads:       results,
		IsolationChecks: isoChecks,
		IsolationPassed: isoPassed,
		IsolationTotal:  len(isoChecks),
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir out dir: %v\n", err)
		os.Exit(1)
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal json: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, append(raw, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", *out, err)
		os.Exit(1)
	}

	// Print concise human-readable summary.
	fmt.Printf("RosenBridge HIP-1345 Benchmark (seed=%d, %s/%s, %s, %d CPUs, /proc/self/io=%v)\n",
		report.Seed, report.GOOS, report.GOARCH, report.GoVersion, report.NumCPU, report.ProcIOAvailable)
	fmt.Printf("Gofer Bottleneck Profiler: confirmed=%v boundary_overhead=%.1f%% avg_syscalls=%.2f avg_copies=%.2f p50=%.2fus p99=%.2fus\n\n",
		goferProfile.BottleneckConfirmed,
		goferProfile.BoundaryOverheadRatio*100,
		goferProfile.AvgSyscallsPerOp,
		goferProfile.AvgCopiesPerOp,
		goferProfile.P50LatencyUs,
		goferProfile.P99LatencyUs)

	fmt.Printf("%-32s %-34s %8s %9s %8s %7s %7s %7s %9s\n",
		"WORKLOAD", "MODE", "TIME(ms)", "MB/s", "IOPS", "SC/op", "PROC/op", "CPY/op", "ALLOC/op")
	fmt.Println(strings.Repeat("-", 128))
	for _, r := range results {
		fmt.Printf("%-32s %-34s %8.2f %9.1f %8.0f %7.2f %7.2f %7.2f %9.0f\n",
			r.Workload, r.Mode, r.RuntimeMs, r.ThroughputMBs, r.IOPS,
			r.SyscallsPerOp, r.KernelProcSyscallsOp, r.CopiesPerOp, r.HeapAllocBytesPerOp)
	}
	fmt.Printf("\nIsolation Checks: %d/%d PASSED (%.1f%%)\n",
		isoPassed, len(isoChecks), 100.0*float64(isoPassed)/float64(len(isoChecks)))
	fmt.Printf("Wrote raw JSON receipts to %s\n", *out)

	if isoPassed != len(isoChecks) {
		os.Exit(1)
	}
}
