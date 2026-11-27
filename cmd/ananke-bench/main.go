// Command ananke-bench runs reproducible OS-backed crash-injection and
// steady-state benchmarks comparing Ananke-style transparent FUSE daemon
// recovery against a matched cold-remount baseline for HIP-1345 (hanzoai/vfs#53/#65).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"syscall"
	"time"

	"github.com/hanzoai/vfs/internal/fuserecovery"
)

type LatencyStats struct {
	Count int     `json:"count"`
	MinMS float64 `json:"min_ms"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
	AvgMS float64 `json:"avg_ms"`
}

type MTTRComparison struct {
	WorkspaceFiles           int          `json:"workspace_files"`
	OpenHandles              int          `json:"open_handles"`
	OrphanHandles            int          `json:"orphan_handles"`
	Iterations               int          `json:"iterations"`
	AnankeRestartMTTR        LatencyStats `json:"ananke_buffered_restart_mttr"`
	AnankeSyncRestartMTTR    LatencyStats `json:"ananke_sync_restart_mttr"`
	ColdRemountServiceMTTR   LatencyStats `json:"cold_remount_service_mttr"`
	ColdRemountEndToEndMTTR  LatencyStats `json:"cold_remount_end_to_end_mttr"`
	AnankeHandleSurvivalPct  float64      `json:"ananke_handle_survival_pct"`
	AnankeOrphanSurvivalPct  float64      `json:"ananke_orphan_survival_pct"`
	ColdHandleSurvivalPct    float64      `json:"cold_handle_survival_pct"`
	ColdOrphanSurvivalPct    float64      `json:"cold_orphan_survival_pct"`
	AnankeUnmounts           uint64       `json:"ananke_unmounts"`
	ColdUnmounts             uint64       `json:"cold_unmounts"`
	SpeedupP50VsColdEndToEnd float64      `json:"speedup_p50_vs_cold_end_to_end"`
	SpeedupP99VsColdEndToEnd float64      `json:"speedup_p99_vs_cold_end_to_end"`
}

type FsyncDurabilityResult struct {
	CrashesInjected           int     `json:"crashes_injected"`
	ActiveFiles               int     `json:"active_files"`
	FsyncsAcknowledged        int     `json:"fsyncs_acknowledged"`
	FsyncsVerified            int     `json:"fsyncs_verified"`
	FsyncsLost                int     `json:"fsyncs_lost"`
	DurabilityRatePct         float64 `json:"durability_rate_pct"`
	UnflushedDirtyStaged      int     `json:"unflushed_dirty_staged"`
	UnflushedDirtyDropped     int     `json:"unflushed_dirty_dropped"`
	Unmounts                  uint64  `json:"unmounts"`
	RemainingOpenFDsAfterTest int     `json:"remaining_open_fds_after_test"`
}

type HandleMatrixEntry struct {
	Scenario         string `json:"scenario"`
	ExpectedBehavior string `json:"expected_behavior"`
	ObservedBehavior string `json:"observed_behavior"`
	Passed           bool   `json:"passed"`
}

type ModeBenchmark struct {
	Mode              string       `json:"mode"`
	Operations        int          `json:"operations"`
	TotalElapsedMS    float64      `json:"total_elapsed_ms"`
	ThroughputOpsSec  float64      `json:"throughput_ops_sec"`
	OpLatency         LatencyStats `json:"op_latency"`
	OverheadVsBasePct float64      `json:"overhead_vs_baseline_pct"`
	CheckpointSaves   uint64       `json:"checkpoint_saves"`
	CheckpointBytes   uint64       `json:"checkpoint_bytes"`
}

type SteadyStateResult struct {
	BalancedWorkload     []ModeBenchmark `json:"balanced_rw_fsync_workload"`
	HighChurnTinyOpenNeg []ModeBenchmark `json:"high_churn_tiny_open_negative_regime"`
}

type NegativeRegimeEntry struct {
	Scenario             string `json:"scenario"`
	ReturnedError        string `json:"returned_error"`
	ExpectedDegraded     bool   `json:"expected_degraded"`
	DegradedFallbacks    uint64 `json:"degraded_fallbacks"`
	ColdRemountsRequired uint64 `json:"cold_remounts_required"`
	MountAlive           bool   `json:"mount_alive"`
	Passed               bool   `json:"passed"`
}

type HostEnvironment struct {
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	GoVersion string `json:"go_version"`
	NumCPU    int    `json:"num_cpu"`
}

type BenchResults struct {
	SchemaVersion   string                `json:"schema_version"`
	HIP             string                `json:"hip"`
	TimestampUTC    string                `json:"timestamp_utc"`
	Seed            int64                 `json:"seed"`
	HonestyNote     string                `json:"honesty_note"`
	Environment     HostEnvironment       `json:"environment"`
	MTTR            MTTRComparison        `json:"mttr_comparison"`
	FsyncDurability FsyncDurabilityResult `json:"fsync_durability"`
	HandleMatrix    []HandleMatrixEntry   `json:"open_handle_matrix"`
	SteadyState     SteadyStateResult     `json:"steady_state_overhead"`
	NegativeRegimes []NegativeRegimeEntry `json:"negative_regimes"`
}

func main() {
	outPath := flag.String("out", "docs/research/hip-1345-ananke/results.json", "output JSON file path")
	seed := flag.Int64("seed", 1345, "deterministic PRNG seed")
	crashes := flag.Int("crashes", 150, "number of injected process crashes for fsync durability test")
	mttrIters := flag.Int("mttr-iters", 30, "iterations for MTTR recovery comparison")
	flag.Parse()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(*seed))

	fmt.Printf("Running HIP-1345 Ananke FUSE Daemon Recovery Benchmark (seed=%d, crashes=%d, mttr-iters=%d)\n",
		*seed, *crashes, *mttrIters)

	mttrRes, err := runMTTRBenchmark(ctx, rng, *mttrIters)
	if err != nil {
		fatal("MTTR benchmark failed: %v", err)
	}
	fmt.Printf("[1/5] MTTR: Ananke(buffered) p50=%.3fms p99=%.3fms | Ananke(sync) p50=%.3fms | Cold E2E p50=%.3fms p99=%.3fms (Ananke handles=%.1f%% vs Cold=%.1f%%)\n",
		mttrRes.AnankeRestartMTTR.P50MS, mttrRes.AnankeRestartMTTR.P99MS,
		mttrRes.AnankeSyncRestartMTTR.P50MS,
		mttrRes.ColdRemountEndToEndMTTR.P50MS, mttrRes.ColdRemountEndToEndMTTR.P99MS,
		mttrRes.AnankeHandleSurvivalPct, mttrRes.ColdHandleSurvivalPct)

	durRes, err := runFsyncDurabilityBenchmark(ctx, rng, *crashes)
	if err != nil {
		fatal("Fsync durability benchmark failed: %v", err)
	}
	fmt.Printf("[2/5] Fsync Durability: %d/%d verified (lost=%d, %.1f%%), unflushed dirty dropped=%d/%d, leaked FDs=%d\n",
		durRes.FsyncsVerified, durRes.FsyncsAcknowledged, durRes.FsyncsLost, durRes.DurabilityRatePct,
		durRes.UnflushedDirtyDropped, durRes.UnflushedDirtyStaged, durRes.RemainingOpenFDsAfterTest)

	matrixRes, err := runHandleMatrixBenchmark(ctx)
	if err != nil {
		fatal("Handle matrix benchmark failed: %v", err)
	}
	matrixPass := 0
	for _, e := range matrixRes {
		if e.Passed {
			matrixPass++
		}
	}
	fmt.Printf("[3/5] Open-Handle Continuity Matrix: %d/%d scenarios passed\n", matrixPass, len(matrixRes))

	steadyRes, err := runSteadyStateBenchmark(ctx, rng)
	if err != nil {
		fatal("Steady-state benchmark failed: %v", err)
	}
	fmt.Printf("[4/5] Steady-State Overhead measured (balanced & high-churn negative regime)\n")

	negRes, err := runNegativeRegimesBenchmark(ctx)
	if err != nil {
		fatal("Negative regimes benchmark failed: %v", err)
	}
	negPass := 0
	for _, n := range negRes {
		if n.Passed {
			negPass++
		}
	}
	fmt.Printf("[5/5] Negative Regimes: %d/%d scenarios verified\n", negPass, len(negRes))

	results := BenchResults{
		SchemaVersion: "1.0",
		HIP:           "HIP-1345 / HIP-1344 (#53 / #65)",
		TimestampUTC:  time.Now().UTC().Format(time.RFC3339),
		Seed:          *seed,
		HonestyNote:   "All figures are measured on the authoring Linux host using real OS-backed files, real fsync(), and an in-process supervisor /dev/fuse channel harness (no kernel /dev/fuse mount required). They represent clean-room authoring-host results, not Hanzo production SLA claims until reproduced on Hanzo target hardware.",
		Environment: HostEnvironment{
			GOOS:      runtime.GOOS,
			GOARCH:    runtime.GOARCH,
			GoVersion: runtime.Version(),
			NumCPU:    runtime.NumCPU(),
		},
		MTTR:            mttrRes,
		FsyncDurability: durRes,
		HandleMatrix:    matrixRes,
		SteadyState:     steadyRes,
		NegativeRegimes: negRes,
	}

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fatal("mkdir output dir: %v", err)
	}
	encoded, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fatal("marshal json: %v", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(*outPath, encoded, 0o644); err != nil {
		fatal("write %s: %v", *outPath, err)
	}
	fmt.Printf("Wrote raw results to %s (%d bytes)\n", *outPath, len(encoded))
}

func elapsedMS(start time.Time) float64 {
	d := time.Since(start)
	if d < 0 {
		return 0
	}
	return float64(d.Microseconds()) / 1000.0
}

func runMTTRBenchmark(ctx context.Context, rng *rand.Rand, iters int) (MTTRComparison, error) {
	const (
		workspaceFiles = 500
		openHandles    = 20
		orphanHandles  = 4
		tenant         = "tenant-bench"
	)

	tmp, err := os.MkdirTemp("", "ananke-mttr-*")
	if err != nil {
		return MTTRComparison{}, err
	}
	defer os.RemoveAll(tmp)

	backing, err := fuserecovery.NewOSBackingStore(filepath.Join(tmp, "backing"))
	if err != nil {
		return MTTRComparison{}, err
	}
	stateDir := filepath.Join(tmp, "state")
	sup, err := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
		SessionID:      "bench-mttr-sess",
		MountPath:      "/mnt/hanzo-bench",
		StateDir:       stateDir,
		Backing:        backing,
		CheckpointMode: fuserecovery.CheckpointBuffered,
	})
	if err != nil {
		return MTTRComparison{}, err
	}
	defer sup.Close()

	d, err := sup.Start(ctx)
	if err != nil {
		return MTTRComparison{}, err
	}

	var reqID uint64 = 1
	openPaths := make([]string, 0, openHandles-orphanHandles)
	activeHandles := make([]uint64, 0, openHandles)
	payload := make([]byte, 4096)
	rng.Read(payload)

	// Populate 500 files across 20 directories; keep 20 handles open (4 unlinked orphans).
	for i := 0; i < workspaceFiles; i++ {
		rel := fmt.Sprintf("dir%02d/file-%04d.dat", i%20, i)
		reqID++
		_, h, err := d.Create(reqID, tenant, rel, 0o644, uint32(os.O_RDWR|os.O_CREATE))
		if err != nil {
			return MTTRComparison{}, err
		}
		reqID++
		if _, _, err := d.Write(reqID, tenant, h.Handle, 0, payload[:512]); err != nil {
			return MTTRComparison{}, err
		}
		if i < openHandles {
			reqID++
			if _, err := d.Fsync(reqID, tenant, h.Handle); err != nil {
				return MTTRComparison{}, err
			}
			activeHandles = append(activeHandles, h.Handle)
			if i < orphanHandles {
				reqID++
				if err := d.Unlink(reqID, tenant, rel); err != nil {
					return MTTRComparison{}, err
				}
			} else {
				openPaths = append(openPaths, rel)
			}
		} else {
			if err := d.Release(tenant, h.Handle); err != nil {
				return MTTRComparison{}, err
			}
		}
	}

	anankeSamples := make([]float64, 0, iters)
	anankeSurvivedHandles := 0
	anankeSurvivedOrphans := 0

	for i := 0; i < iters; i++ {
		sup.InjectProcessCrash()
		t0 := time.Now()
		d2, rep, err := sup.Recover(ctx)
		dt := elapsedMS(t0)
		if err != nil {
			return MTTRComparison{}, fmt.Errorf("ananke recover iter %d: %w", i, err)
		}
		anankeSamples = append(anankeSamples, dt)
		anankeSurvivedHandles += rep.HandlesRestored
		anankeSurvivedOrphans += rep.OrphansRestored

		if _, err := d2.Read(tenant, activeHandles[0], 0, 16); err != nil {
			return MTTRComparison{}, fmt.Errorf("orphan handle unreadable iter %d: %w", i, err)
		}
		if _, err := d2.Read(tenant, activeHandles[len(activeHandles)-1], 0, 16); err != nil {
			return MTTRComparison{}, fmt.Errorf("regular handle unreadable iter %d: %w", i, err)
		}
	}
	anankeUnmounts := sup.Channel().UnmountCount()

	// Also measure Ananke recovery in strict disk-sync CheckpointSync mode.
	syncSamples := make([]float64, 0, iters)
	syncSup, err := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
		SessionID:      sup.Channel().SessionID(),
		MountPath:      sup.Channel().MountPath(),
		StateDir:       stateDir,
		Backing:        backing,
		Channel:        sup.Channel(),
		CheckpointMode: fuserecovery.CheckpointSync,
	})
	if err != nil {
		return MTTRComparison{}, err
	}
	for i := 0; i < iters; i++ {
		sup.InjectProcessCrash()
		t0 := time.Now()
		dSync, _, err := syncSup.Recover(ctx)
		dt := elapsedMS(t0)
		if err != nil {
			return MTTRComparison{}, fmt.Errorf("ananke sync recover iter %d: %w", i, err)
		}
		syncSamples = append(syncSamples, dt)
		_ = dSync.Close()
	}

	// Matched Cold Remount Baseline on the identical backing directory.
	coldServiceSamples := make([]float64, 0, iters)
	coldE2ESamples := make([]float64, 0, iters)

	coldSup, err := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
		SessionID:      "bench-cold-sess",
		MountPath:      "/mnt/hanzo-cold",
		Backing:        backing,
		CheckpointMode: fuserecovery.CheckpointDisabled,
	})
	if err != nil {
		return MTTRComparison{}, err
	}
	defer coldSup.Close()
	if _, err := coldSup.Start(ctx); err != nil {
		return MTTRComparison{}, err
	}

	for i := 0; i < iters; i++ {
		coldSup.InjectProcessCrash()
		t0 := time.Now()
		cd, _, err := coldSup.ColdRemountRecover(ctx, tenant)
		svcMS := elapsedMS(t0)
		if err != nil {
			return MTTRComparison{}, fmt.Errorf("cold remount iter %d: %w", i, err)
		}
		// Application must re-lookup and re-open all non-orphan files after ENOTCONN
		// (orphans are permanently unrecoverable on cold remount).
		for _, p := range openPaths {
			ino, err := cd.Lookup(tenant, p)
			if err != nil {
				return MTTRComparison{}, fmt.Errorf("cold reopen lookup %q: %w", p, err)
			}
			if _, err := cd.Open(tenant, ino.Inode, ino.Generation, uint32(os.O_RDWR)); err != nil {
				return MTTRComparison{}, fmt.Errorf("cold reopen %q: %w", p, err)
			}
		}
		e2eMS := elapsedMS(t0)
		coldServiceSamples = append(coldServiceSamples, svcMS)
		coldE2ESamples = append(coldE2ESamples, e2eMS)
	}

	anankeStats := computeLatencyStats(anankeSamples)
	anankeSyncStats := computeLatencyStats(syncSamples)
	coldSvcStats := computeLatencyStats(coldServiceSamples)
	coldE2EStats := computeLatencyStats(coldE2ESamples)

	speedupP50 := 0.0
	if anankeStats.P50MS > 0 {
		speedupP50 = round3(coldE2EStats.P50MS / anankeStats.P50MS)
	}
	speedupP99 := 0.0
	if anankeStats.P99MS > 0 {
		speedupP99 = round3(coldE2EStats.P99MS / anankeStats.P99MS)
	}

	return MTTRComparison{
		WorkspaceFiles:           workspaceFiles,
		OpenHandles:              openHandles,
		OrphanHandles:            orphanHandles,
		Iterations:               iters,
		AnankeRestartMTTR:        anankeStats,
		AnankeSyncRestartMTTR:    anankeSyncStats,
		ColdRemountServiceMTTR:   coldSvcStats,
		ColdRemountEndToEndMTTR:  coldE2EStats,
		AnankeHandleSurvivalPct:  round2(100.0 * float64(anankeSurvivedHandles) / float64(iters*openHandles)),
		AnankeOrphanSurvivalPct:  round2(100.0 * float64(anankeSurvivedOrphans) / float64(iters*orphanHandles)),
		ColdHandleSurvivalPct:    0.0,
		ColdOrphanSurvivalPct:    0.0,
		AnankeUnmounts:           anankeUnmounts,
		ColdUnmounts:             coldSup.Channel().UnmountCount(),
		SpeedupP50VsColdEndToEnd: speedupP50,
		SpeedupP99VsColdEndToEnd: speedupP99,
	}, nil
}

func runFsyncDurabilityBenchmark(ctx context.Context, rng *rand.Rand, crashes int) (FsyncDurabilityResult, error) {
	const (
		numFiles = 4
		tenant   = "tenant-durability"
	)
	tmp, err := os.MkdirTemp("", "ananke-dur-*")
	if err != nil {
		return FsyncDurabilityResult{}, err
	}
	defer os.RemoveAll(tmp)

	backing, err := fuserecovery.NewOSBackingStore(filepath.Join(tmp, "backing"))
	if err != nil {
		return FsyncDurabilityResult{}, err
	}
	sup, err := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
		SessionID:      "bench-dur-sess",
		MountPath:      "/mnt/hanzo-dur",
		StateDir:       filepath.Join(tmp, "state"),
		Backing:        backing,
		CheckpointMode: fuserecovery.CheckpointBuffered,
	})
	if err != nil {
		return FsyncDurabilityResult{}, err
	}
	defer sup.Close()

	d, err := sup.Start(ctx)
	if err != nil {
		return FsyncDurabilityResult{}, err
	}

	handles := make([]uint64, numFiles)
	expected := make([]*bytes.Buffer, numFiles)
	var reqID uint64 = 1

	for i := 0; i < numFiles; i++ {
		reqID++
		_, h, err := d.Create(reqID, tenant, fmt.Sprintf("wal-%d.bin", i), 0o644, uint32(os.O_RDWR|os.O_CREATE))
		if err != nil {
			return FsyncDurabilityResult{}, err
		}
		handles[i] = h.Handle
		expected[i] = &bytes.Buffer{}
	}

	acked := 0
	verified := 0
	lost := 0
	dirtyStaged := 0
	dirtyDropped := 0

	for c := 0; c < crashes; c++ {
		fi := c % numFiles
		chunk := make([]byte, 128+rng.Intn(256))
		rng.Read(chunk)

		off := int64(expected[fi].Len())
		reqID++
		if _, _, err := d.Write(reqID, tenant, handles[fi], off, chunk); err != nil {
			return FsyncDurabilityResult{}, err
		}
		reqID++
		if _, err := d.Fsync(reqID, tenant, handles[fi]); err != nil {
			return FsyncDurabilityResult{}, err
		}
		acked++
		expected[fi].Write(chunk)

		// Stage an unacknowledged dirty tail right before SIGKILL.
		dirtyTail := []byte("UNFLUSHED-DIRTY-BYTES")
		reqID++
		if _, _, err := d.Write(reqID, tenant, handles[fi], int64(expected[fi].Len()), dirtyTail); err != nil {
			return FsyncDurabilityResult{}, err
		}
		dirtyStaged++

		sup.InjectProcessCrash()
		d, _, err = sup.Recover(ctx)
		if err != nil {
			return FsyncDurabilityResult{}, fmt.Errorf("recover crash %d: %w", c, err)
		}

		got, err := d.Read(tenant, handles[fi], 0, expected[fi].Len()+len(dirtyTail)+32)
		if err != nil {
			return FsyncDurabilityResult{}, fmt.Errorf("read after crash %d: %w", c, err)
		}
		if bytes.Equal(got, expected[fi].Bytes()) {
			verified++
			dirtyDropped++
		} else {
			lost++
		}
	}

	for _, fh := range handles {
		if err := d.Release(tenant, fh); err != nil {
			return FsyncDurabilityResult{}, err
		}
	}

	rate := 0.0
	if acked > 0 {
		rate = round2(100.0 * float64(verified) / float64(acked))
	}
	return FsyncDurabilityResult{
		CrashesInjected:           crashes,
		ActiveFiles:               numFiles,
		FsyncsAcknowledged:        acked,
		FsyncsVerified:            verified,
		FsyncsLost:                lost,
		DurabilityRatePct:         rate,
		UnflushedDirtyStaged:      dirtyStaged,
		UnflushedDirtyDropped:     dirtyDropped,
		Unmounts:                  sup.Channel().UnmountCount(),
		RemainingOpenFDsAfterTest: d.OpenFDCount(),
	}, nil
}

func runHandleMatrixBenchmark(ctx context.Context) ([]HandleMatrixEntry, error) {
	tmp, err := os.MkdirTemp("", "ananke-matrix-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)

	backing, err := fuserecovery.NewOSBackingStore(filepath.Join(tmp, "backing"))
	if err != nil {
		return nil, err
	}
	sup, err := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
		SessionID:      "bench-matrix",
		MountPath:      "/mnt/hanzo-matrix",
		StateDir:       filepath.Join(tmp, "state"),
		Backing:        backing,
		CheckpointMode: fuserecovery.CheckpointSync,
	})
	if err != nil {
		return nil, err
	}
	defer sup.Close()

	d1, err := sup.Start(ctx)
	if err != nil {
		return nil, err
	}

	var entries []HandleMatrixEntry

	// 1. Regular R/W handle
	_, hReg, err := d1.Create(1, "tenant-a", "regular.dat", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		return nil, err
	}
	_, _, _ = d1.Write(2, "tenant-a", hReg.Handle, 0, []byte("reg-v1"))
	_, _ = d1.Fsync(3, "tenant-a", hReg.Handle)

	// 2. Append + O_SYNC handle for idempotent retry deduplication
	_, hApp, err := d1.Create(4, "tenant-a", "append.log", 0o644, uint32(os.O_RDWR|os.O_CREATE|os.O_APPEND|syscall.O_SYNC))
	if err != nil {
		return nil, err
	}
	_, _, _ = d1.Write(5, "tenant-a", hApp.Handle, 0, []byte("A1\n"))

	// 3. Unlinked-open orphan handle
	_, hOrph, err := d1.Create(6, "tenant-a", "orphan.tmp", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		return nil, err
	}
	_, _, _ = d1.Write(7, "tenant-a", hOrph.Handle, 0, []byte("orphan-bytes"))
	_, _ = d1.Fsync(8, "tenant-a", hOrph.Handle)
	_ = d1.Unlink(9, "tenant-a", "orphan.tmp")

	// 4. Stale inode (deleted externally during crash)
	_, hStale, err := d1.Create(10, "tenant-a", "stale.dat", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		return nil, err
	}
	_, _, _ = d1.Write(11, "tenant-a", hStale.Handle, 0, []byte("gone"))
	_, _ = d1.Fsync(12, "tenant-a", hStale.Handle)

	sup.InjectProcessCrash()
	_ = backing.UnlinkFile("stale.dat")

	d2, _, err := sup.Recover(ctx)
	if err != nil {
		return nil, err
	}

	// Verify 1: regular R/W
	gotReg, rerr := d2.Read("tenant-a", hReg.Handle, 0, 32)
	okReg := rerr == nil && string(gotReg) == "reg-v1"
	entries = append(entries, HandleMatrixEntry{
		Scenario:         "regular_rw_open_handle",
		ExpectedBehavior: "Survives SIGKILL without unmount; reads acknowledged bytes",
		ObservedBehavior: fmt.Sprintf("read=%q err=%v", string(gotReg), rerr),
		Passed:           okReg,
	})

	// Verify 2: O_APPEND retry deduplication
	_, dedup, aerr := d2.Write(5, "tenant-a", hApp.Handle, 0, []byte("A1\n"))
	gotApp, _ := d2.Read("tenant-a", hApp.Handle, 0, 32)
	okApp := aerr == nil && dedup && string(gotApp) == "A1\n"
	entries = append(entries, HandleMatrixEntry{
		Scenario:         "append_handle_idempotent_retry",
		ExpectedBehavior: "Retried reqID=5 deduplicated; no duplicate append",
		ObservedBehavior: fmt.Sprintf("dedup=%v content=%q err=%v", dedup, string(gotApp), aerr),
		Passed:           okApp,
	})

	// Verify 3: Unlinked-open orphan
	gotOrph, oerr := d2.Read("tenant-a", hOrph.Handle, 0, 32)
	relErr := d2.Release("tenant-a", hOrph.Handle)
	okOrph := oerr == nil && string(gotOrph) == "orphan-bytes" && relErr == nil
	entries = append(entries, HandleMatrixEntry{
		Scenario:         "unlinked_open_orphan_handle",
		ExpectedBehavior: "Remains readable across crash via .ananke-orphans and cleans up on Release",
		ObservedBehavior: fmt.Sprintf("read=%q release_err=%v", string(gotOrph), relErr),
		Passed:           okOrph,
	})

	// Verify 4: Stale inode -> ESTALE
	_, serr := d2.Read("tenant-a", hStale.Handle, 0, 16)
	okStale := errors.Is(serr, fuserecovery.ErrStaleInode) && fuserecovery.Errno(serr) == syscall.ESTALE
	entries = append(entries, HandleMatrixEntry{
		Scenario:         "stale_deleted_inode_rejection",
		ExpectedBehavior: "Returns ErrStaleInode (POSIX ESTALE)",
		ObservedBehavior: fmt.Sprintf("err=%v errno=%v", serr, fuserecovery.Errno(serr)),
		Passed:           okStale,
	})

	// Verify 5: Cross-tenant isolation -> EACCES
	_, terr := d2.Read("tenant-evil", hReg.Handle, 0, 16)
	okTenant := errors.Is(terr, fuserecovery.ErrTenantMismatch) && fuserecovery.Errno(terr) == syscall.EACCES
	entries = append(entries, HandleMatrixEntry{
		Scenario:         "cross_tenant_handle_isolation",
		ExpectedBehavior: "Rejects foreign tenant with ErrTenantMismatch (POSIX EACCES)",
		ObservedBehavior: fmt.Sprintf("err=%v errno=%v", terr, fuserecovery.Errno(terr)),
		Passed:           okTenant,
	})

	return entries, nil
}

func runSteadyStateBenchmark(ctx context.Context, rng *rand.Rand) (SteadyStateResult, error) {
	modes := []fuserecovery.CheckpointMode{
		fuserecovery.CheckpointDisabled,
		fuserecovery.CheckpointBuffered,
		fuserecovery.CheckpointSync,
	}

	payload := make([]byte, 4096)
	rng.Read(payload)

	// Workload A: Balanced Create/Write(4x4KiB)/Fsync/Read/Close (100 ops per mode).
	const balancedOps = 100
	balanced := make([]ModeBenchmark, 0, len(modes))
	var baseThroughput float64

	for _, mode := range modes {
		tmp, err := os.MkdirTemp("", "ananke-steady-*")
		if err != nil {
			return SteadyStateResult{}, err
		}
		backing, err := fuserecovery.NewOSBackingStore(filepath.Join(tmp, "backing"))
		if err != nil {
			os.RemoveAll(tmp)
			return SteadyStateResult{}, err
		}
		sup, err := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
			SessionID:      "steady-" + mode.String(),
			MountPath:      "/mnt/hanzo-steady",
			StateDir:       filepath.Join(tmp, "state"),
			Backing:        backing,
			CheckpointMode: mode,
		})
		if err != nil {
			os.RemoveAll(tmp)
			return SteadyStateResult{}, err
		}
		d, err := sup.Start(ctx)
		if err != nil {
			sup.Close()
			os.RemoveAll(tmp)
			return SteadyStateResult{}, err
		}

		latencies := make([]float64, 0, balancedOps)
		var reqID uint64 = 1
		startAll := time.Now()
		for i := 0; i < balancedOps; i++ {
			opStart := time.Now()
			rel := fmt.Sprintf("w/f-%04d.dat", i)
			reqID++
			_, h, err := d.Create(reqID, "tenant-bench", rel, 0o644, uint32(os.O_RDWR|os.O_CREATE))
			if err != nil {
				sup.Close()
				os.RemoveAll(tmp)
				return SteadyStateResult{}, err
			}
			for b := 0; b < 4; b++ {
				reqID++
				if _, _, err := d.Write(reqID, "tenant-bench", h.Handle, int64(b*len(payload)), payload); err != nil {
					sup.Close()
					os.RemoveAll(tmp)
					return SteadyStateResult{}, err
				}
			}
			reqID++
			if _, err := d.Fsync(reqID, "tenant-bench", h.Handle); err != nil {
				sup.Close()
				os.RemoveAll(tmp)
				return SteadyStateResult{}, err
			}
			if _, err := d.Read("tenant-bench", h.Handle, 0, 512); err != nil {
				sup.Close()
				os.RemoveAll(tmp)
				return SteadyStateResult{}, err
			}
			if err := d.Release("tenant-bench", h.Handle); err != nil {
				sup.Close()
				os.RemoveAll(tmp)
				return SteadyStateResult{}, err
			}
			latencies = append(latencies, elapsedMS(opStart))
		}
		totalMS := elapsedMS(startAll)
		tput := float64(balancedOps) / (totalMS / 1000.0)
		if mode == fuserecovery.CheckpointDisabled {
			baseThroughput = tput
		}
		overheadPct := 0.0
		if baseThroughput > 0 && mode != fuserecovery.CheckpointDisabled {
			overheadPct = round2(((baseThroughput - tput) / baseThroughput) * 100.0)
		}
		var saves, bytesWr uint64
		if st := sup.StateStore(); st != nil {
			saves, bytesWr = st.Stats()
		}
		sup.Close()
		os.RemoveAll(tmp)

		balanced = append(balanced, ModeBenchmark{
			Mode:              mode.String(),
			Operations:        balancedOps,
			TotalElapsedMS:    round3(totalMS),
			ThroughputOpsSec:  round2(tput),
			OpLatency:         computeLatencyStats(latencies),
			OverheadVsBasePct: overheadPct,
			CheckpointSaves:   saves,
			CheckpointBytes:   bytesWr,
		})
	}

	// Workload B (Negative Regime): High-churn tiny open/stat/close without fsync (150 ops).
	const churnOps = 150
	churn := make([]ModeBenchmark, 0, len(modes))
	var churnBaseTput float64

	for _, mode := range modes {
		tmp, err := os.MkdirTemp("", "ananke-churn-*")
		if err != nil {
			return SteadyStateResult{}, err
		}
		backing, err := fuserecovery.NewOSBackingStore(filepath.Join(tmp, "backing"))
		if err != nil {
			os.RemoveAll(tmp)
			return SteadyStateResult{}, err
		}
		sup, err := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
			SessionID:      "churn-" + mode.String(),
			MountPath:      "/mnt/hanzo-churn",
			StateDir:       filepath.Join(tmp, "state"),
			Backing:        backing,
			CheckpointMode: mode,
		})
		if err != nil {
			os.RemoveAll(tmp)
			return SteadyStateResult{}, err
		}
		d, err := sup.Start(ctx)
		if err != nil {
			sup.Close()
			os.RemoveAll(tmp)
			return SteadyStateResult{}, err
		}

		// Pre-create 10 tiny files to hammer open/read/close.
		inodes := make([]fuserecovery.InodeRecord, 10)
		for i := 0; i < 10; i++ {
			ino, h, err := d.Create(uint64(i+1), "tenant-churn", fmt.Sprintf("tiny-%d.txt", i), 0o644, uint32(os.O_RDWR|os.O_CREATE))
			if err != nil {
				sup.Close()
				os.RemoveAll(tmp)
				return SteadyStateResult{}, err
			}
			_, _, _ = d.Write(uint64(100+i), "tenant-churn", h.Handle, 0, []byte("ok"))
			_, _ = d.Fsync(uint64(200+i), "tenant-churn", h.Handle)
			_ = d.Release("tenant-churn", h.Handle)
			inodes[i] = ino
		}

		latencies := make([]float64, 0, churnOps)
		startAll := time.Now()
		for i := 0; i < churnOps; i++ {
			opStart := time.Now()
			ino := inodes[i%len(inodes)]
			h, err := d.Open("tenant-churn", ino.Inode, ino.Generation, uint32(os.O_RDONLY))
			if err != nil {
				sup.Close()
				os.RemoveAll(tmp)
				return SteadyStateResult{}, err
			}
			if _, err := d.Read("tenant-churn", h.Handle, 0, 4); err != nil {
				sup.Close()
				os.RemoveAll(tmp)
				return SteadyStateResult{}, err
			}
			if err := d.Release("tenant-churn", h.Handle); err != nil {
				sup.Close()
				os.RemoveAll(tmp)
				return SteadyStateResult{}, err
			}
			latencies = append(latencies, elapsedMS(opStart))
		}
		totalMS := elapsedMS(startAll)
		tput := float64(churnOps) / (totalMS / 1000.0)
		if mode == fuserecovery.CheckpointDisabled {
			churnBaseTput = tput
		}
		overheadPct := 0.0
		if churnBaseTput > 0 && mode != fuserecovery.CheckpointDisabled {
			overheadPct = round2(((churnBaseTput - tput) / churnBaseTput) * 100.0)
		}
		var saves, bytesWr uint64
		if st := sup.StateStore(); st != nil {
			saves, bytesWr = st.Stats()
		}
		sup.Close()
		os.RemoveAll(tmp)

		churn = append(churn, ModeBenchmark{
			Mode:              mode.String(),
			Operations:        churnOps,
			TotalElapsedMS:    round3(totalMS),
			ThroughputOpsSec:  round2(tput),
			OpLatency:         computeLatencyStats(latencies),
			OverheadVsBasePct: overheadPct,
			CheckpointSaves:   saves,
			CheckpointBytes:   bytesWr,
		})
	}

	return SteadyStateResult{
		BalancedWorkload:     balanced,
		HighChurnTinyOpenNeg: churn,
	}, nil
}

func runNegativeRegimesBenchmark(ctx context.Context) ([]NegativeRegimeEntry, error) {
	var out []NegativeRegimeEntry

	// 1. Host / Kernel Crash
	{
		tmp, err := os.MkdirTemp("", "ananke-neg-k-*")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(tmp)
		backing, _ := fuserecovery.NewOSBackingStore(filepath.Join(tmp, "backing"))
		sup, _ := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
			SessionID:      "neg-kernel",
			MountPath:      "/mnt/neg-kernel",
			StateDir:       filepath.Join(tmp, "state"),
			Backing:        backing,
			CheckpointMode: fuserecovery.CheckpointSync,
		})
		defer sup.Close()
		_, _ = sup.Start(ctx)
		sup.InjectKernelCrash()
		_, _, rerr := sup.Recover(ctx)
		tel := sup.Metrics()
		out = append(out, NegativeRegimeEntry{
			Scenario:             "host_kernel_crash_vs_process_crash",
			ReturnedError:        fmt.Sprint(rerr),
			ExpectedDegraded:     true,
			DegradedFallbacks:    tel.DegradedFallbacks,
			ColdRemountsRequired: tel.ColdRemountsRequired,
			MountAlive:           sup.Channel().MountAlive(),
			Passed:               errors.Is(rerr, fuserecovery.ErrHandoffUnavailable) && tel.DegradedFallbacks == 1 && !sup.Channel().MountAlive(),
		})
	}

	// 2. /dev/fuse FD Handoff Unavailable
	{
		tmp, err := os.MkdirTemp("", "ananke-neg-h-*")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(tmp)
		backing, _ := fuserecovery.NewOSBackingStore(filepath.Join(tmp, "backing"))
		sup, _ := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
			SessionID:      "neg-handoff",
			MountPath:      "/mnt/neg-handoff",
			StateDir:       filepath.Join(tmp, "state"),
			Backing:        backing,
			CheckpointMode: fuserecovery.CheckpointSync,
		})
		defer sup.Close()
		_, _ = sup.Start(ctx)
		sup.DisableHandoff()
		sup.InjectProcessCrash()
		_, _, rerr := sup.Recover(ctx)
		tel := sup.Metrics()
		out = append(out, NegativeRegimeEntry{
			Scenario:             "fuse_fd_handoff_unavailable",
			ReturnedError:        fmt.Sprint(rerr),
			ExpectedDegraded:     true,
			DegradedFallbacks:    tel.DegradedFallbacks,
			ColdRemountsRequired: tel.ColdRemountsRequired,
			MountAlive:           sup.Channel().MountAlive(),
			Passed:               errors.Is(rerr, fuserecovery.ErrHandoffUnavailable) && tel.DegradedFallbacks == 1 && tel.ColdRemountsRequired == 1,
		})
	}

	return out, nil
}

func computeLatencyStats(samples []float64) LatencyStats {
	if len(samples) == 0 {
		return LatencyStats{}
	}
	cp := append([]float64(nil), samples...)
	sort.Float64s(cp)
	sum := 0.0
	for _, v := range cp {
		sum += v
	}
	return LatencyStats{
		Count: len(cp),
		MinMS: round3(cp[0]),
		P50MS: round3(percentile(cp, 0.50)),
		P95MS: round3(percentile(cp, 0.95)),
		P99MS: round3(percentile(cp, 0.99)),
		MaxMS: round3(cp[len(cp)-1]),
		AvgMS: round3(sum / float64(len(cp))),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
func round3(v float64) float64 { return math.Round(v*1000) / 1000 }

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
