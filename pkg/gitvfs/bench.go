// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package gitvfs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
)

// BenchConfig parameterizes a reproducible native-git-on-vfs benchmark run.
type BenchConfig struct {
	SrcRepo        string        `json:"src_repo"`         // real git repo (e.g., hanzoai/vfs)
	ExtraFiles     int           `json:"extra_files"`      // deterministic corpus expansion files
	BulkStageFiles int           `json:"bulk_stage_files"` // files staged in batched-fsync experiment
	Seed           int64         `json:"seed"`             // deterministic PRNG seed
	Repetitions    int           `json:"repetitions"`      // repetitions per mode (median reported)
	COWSandboxes   int           `json:"cow_sandboxes"`    // N sandboxes cloned from one VolumeSnapshot
	SparseCones    []string      `json:"sparse_cones"`     // cone paths for sparse checkout mode
	UseFUSEMount   bool          `json:"use_fuse_mount"`   // run git directly over FUSE-mounted *vfs.Volume
	MountHelperBin string        `json:"-"`                // self binary path for --serve-mount child
	PutDelay       time.Duration `json:"put_delay_ns"`     // optional simulated S3 PUT RTT
}

// OpMeasurement records wall time and object-store PUT/byte deltas for one
// git operation.
type OpMeasurement struct {
	WallMs       float64 `json:"wall_ms"`
	TotalPuts    int64   `json:"total_puts"`
	PackPuts     int64   `json:"pack_puts"`
	MetaPuts     int64   `json:"meta_puts"`
	HeadPuts     int64   `json:"head_puts"`
	PutBytes     int64   `json:"put_bytes"`
	JournalSyncs int64   `json:"journal_syncs"`
}

// ModeResult captures side-by-side acceptance metrics for one benchmark mode.
type ModeResult struct {
	Mode               string                   `json:"mode"`
	StorageLayer       string                   `json:"storage_layer"` // "plain-nvme", "vfs-fuse", "vfs-volume"
	ProfileName        string                   `json:"profile_name"`
	CloneToFirstCmdMs  float64                  `json:"clone_to_first_cmd_ms"`
	CloneMs            float64                  `json:"clone_ms"`
	StatusColdMs       float64                  `json:"status_cold_ms"`
	StatusWarmMs       float64                  `json:"status_warm_ms"`
	CheckoutMs         float64                  `json:"checkout_ms"`
	CommitMs           float64                  `json:"commit_ms"`
	PushMs             float64                  `json:"push_ms"`
	CommitAndPushMs    float64                  `json:"commit_and_push_ms"`
	FlushMs            float64                  `json:"flush_ms"`
	OwnGitObjects      int                      `json:"own_git_objects"`
	OwnGitKB           int                      `json:"own_git_kb"`
	TrackedFiles       int                      `json:"tracked_files"`
	WorktreeFiles      int                      `json:"worktree_files"`
	JournalFsyncs      int64                    `json:"journal_fsyncs"`
	JournalFsyncP50Us  float64                  `json:"journal_fsync_p50_us"`
	FsyncRatioVsNVMe   float64                  `json:"fsync_ratio_vs_nvme"`
	TotalVFSBackendPut int64                    `json:"total_vfs_puts"`
	TotalVFSPutBytes   int64                    `json:"total_vfs_put_bytes"`
	PackPuts           int64                    `json:"pack_puts"`
	MetaPuts           int64                    `json:"meta_puts"`
	HeadPuts           int64                    `json:"head_puts"`
	Ops                map[string]OpMeasurement `json:"ops"`
	AlternatesVerified bool                     `json:"alternates_verified,omitempty"`
}

// FsyncBatchRow compares loose-object fsync strategies during bulk staging.
type FsyncBatchRow struct {
	Storage          string  `json:"storage"`
	Strategy         string  `json:"strategy"`
	StagedFiles      int     `json:"staged_files"`
	AddWallMs        float64 `json:"add_wall_ms"`
	CommitWallMs     float64 `json:"commit_wall_ms"`
	StraceFsyncs     int64   `json:"strace_fsyncs"`
	JournalSyncs     int64   `json:"journal_syncs"`
	JournalSyncP50Us float64 `json:"journal_sync_p50_us"`
	FsyncRatioVsNVMe float64 `json:"fsync_ratio_vs_nvme"`
	VFSBackendPuts   int64   `json:"vfs_backend_puts"`
}

// COWSandboxResult reports O(1) clone-by-reference across N sandboxes.
type COWSandboxResult struct {
	Sandboxes             int     `json:"sandboxes"`
	BaseFiles             int     `json:"base_files"`
	BaseBytes             int64   `json:"base_bytes"`
	BaseBuildMs           float64 `json:"base_build_ms"`
	BaseTotalPuts         int64   `json:"base_total_puts"`
	BasePackPuts          int64   `json:"base_pack_puts"`
	BaseMetaPuts          int64   `json:"base_meta_puts"`
	MedianCloneMs         float64 `json:"median_clone_ms"`
	MaxCloneMs            float64 `json:"max_clone_ms"`
	CloneHeadPutsEach     int64   `json:"clone_head_puts_each"`
	ClonePackPutsEach     int64   `json:"clone_pack_puts_each"`
	CloneMetaPutsEach     int64   `json:"clone_meta_puts_each"`
	BaseWritesDuringClone int64   `json:"base_writes_during_clone"`
	PostCloneStatusMs     float64 `json:"post_clone_status_ms"`
	PostCloneCommitPushMs float64 `json:"post_clone_commit_push_ms"`
	DeltaFlushPuts        int64   `json:"delta_flush_puts"`
	DeltaFlushBytes       int64   `json:"delta_flush_bytes"`
}

// NegativeRegimeResult documents an explicit empirical negative regime or hazard.
type NegativeRegimeResult struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Summary       string         `json:"summary"`
	BaselineLabel string         `json:"baseline_label"`
	BaselineMs    float64        `json:"baseline_ms"`
	VariantLabel  string         `json:"variant_label"`
	VariantMs     float64        `json:"variant_ms"`
	RegressionPct float64        `json:"regression_pct"`
	Details       map[string]any `json:"details"`
}

// BenchReport is the complete machine-readable benchmark output for HIP-1342 #29.
type BenchReport struct {
	Issue             string                 `json:"issue"`
	HIP               string                 `json:"hip"`
	GeneratedAt       time.Time              `json:"generated_at"`
	GoVersion         string                 `json:"go_version"`
	GitVersion        string                 `json:"git_version"`
	OS                string                 `json:"os"`
	Arch              string                 `json:"arch"`
	CPUs              int                    `json:"cpus"`
	FSMonitorSupport  bool                   `json:"fsmonitor_supported"`
	FUSEMountUsed     bool                   `json:"fuse_mount_used"`
	Config            BenchConfig            `json:"config"`
	CorpusCommitSHA   string                 `json:"corpus_commit_sha"`
	CorpusFiles       int                    `json:"corpus_files"`
	NVMeProbe         NVMeProbeResult        `json:"nvme_probe"`
	ForgeStatus       ForgeStatus            `json:"forge_status"`
	ClosePublishCheck CloseAndPublishResult  `json:"close_publish_check"`
	Modes             []ModeResult           `json:"modes"`
	FsyncBatchMatrix  []FsyncBatchRow        `json:"fsync_batch_matrix"`
	COWClone          COWSandboxResult       `json:"cow_clone"`
	NegativeRegimes   []NegativeRegimeResult `json:"negative_regimes"`
}

// RunBenchmark executes the complete HIP-1342 #29 benchmark suite and returns
// a structured report ready for JSON and Markdown rendering.
func RunBenchmark(ctx context.Context, cfg BenchConfig) (*BenchReport, error) {
	if cfg.SrcRepo == "" {
		return nil, fmt.Errorf("gitvfs: BenchConfig.SrcRepo is required")
	}
	if cfg.Repetitions <= 0 {
		cfg.Repetitions = 3
	}
	if cfg.Seed == 0 {
		cfg.Seed = 1342
	}
	if cfg.COWSandboxes <= 0 {
		cfg.COWSandboxes = 4
	}
	if cfg.BulkStageFiles <= 0 {
		cfg.BulkStageFiles = 300
	}
	if len(cfg.SparseCones) == 0 {
		cfg.SparseCones = []string{"pkg/gitvfs", "corpus/core"}
	}

	workRoot, err := os.MkdirTemp("", "gitvfs-bench-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workRoot)

	journalRoot, journalCleanup, err := NewNonVolatileJournalDir(workRoot)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: journal dir: %w", err)
	}
	defer journalCleanup()

	runner, err := NewRunner(filepath.Join(workRoot, "git-env"))
	if err != nil {
		return nil, err
	}

	// 1. Plain NVMe baseline probe on this exact host.
	nvmeProbe, err := ProbeNVMe(workRoot, 40)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: NVMe probe: %w", err)
	}

	// 2. Build bare local forge from real repo + deterministic corpus expansion.
	forgeDir := filepath.Join(workRoot, "forge.git")
	forge, err := InitForgeWithCorpus(ctx, runner, cfg.SrcRepo, forgeDir, cfg.ExtraFiles, cfg.Seed)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: init forge: %w", err)
	}
	forgeStatus, err := forge.Verify(ctx)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: verify forge: %w", err)
	}

	// Count corpus files in forge default branch.
	lsTreeRes, err := runner.Run(ctx, forge.Path, "ls-tree", "-r", "--name-only", forge.DefaultBranch)
	if err != nil {
		return nil, err
	}
	corpusFiles := len(splitNonEmptyLines(lsTreeRes.Stdout))

	// 3. Create tenant-scoped read-only alternates store.
	altStoreDir := filepath.Join(workRoot, "alternates", "tenant-a", "vfs.git")
	altStore, err := NewAlternatesStore(ctx, runner, "tenant-a", "vfs", forge.FileURL(), altStoreDir)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: create alternates store: %w", err)
	}
	if err := altStore.SealReadOnly(); err != nil {
		return nil, fmt.Errorf("gitvfs: seal alternates store: %w", err)
	}
	defer func() { _ = altStore.UnsealWritable() }()

	// 4. Verify zero synchronous S3/store PUTs on clean Close and automatic
	//    publication within 5s on a real *vfs.Volume.
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		return nil, err
	}
	cipher := vfs.Hybrid(key)
	closeCheck, err := runClosePublishVerification(ctx, cipher, workRoot, journalRoot, cfg.PutDelay)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: close/publish verification: %w", err)
	}

	useFUSE := cfg.UseFUSEMount && FUSEMountAvailable() && cfg.MountHelperBin != ""

	// 5. Run side-by-side modes across repetitions and take medians.
	type modeSpec struct {
		name     string
		onVFS    bool
		profile  Profile
		useAlt   bool
		altStore *AlternatesStore
	}
	specs := []modeSpec{
		{name: "plain-nvme-untuned", onVFS: false, profile: UntunedProfile()},
		{name: "plain-nvme-tuned", onVFS: false, profile: TunedFastProfile()},
		{name: "vfs-untuned", onVFS: true, profile: UntunedProfile()},
		{name: "vfs-tuned", onVFS: true, profile: TunedFastProfile()},
		{name: "vfs-tuned-batched-fsync", onVFS: true, profile: TunedProfile()},
		{name: "vfs-reference-alternates", onVFS: true, profile: ReferenceProfile(), useAlt: true, altStore: altStore},
		{name: "vfs-reference-sparse", onVFS: true, profile: SparseReferenceProfile(cfg.SparseCones...), useAlt: true, altStore: altStore},
	}

	var modeResults []ModeResult
	for _, sp := range specs {
		var runs []ModeResult
		for rep := range cfg.Repetitions {
			repDir := filepath.Join(workRoot, fmt.Sprintf("run-%s-%d", sp.name, rep))
			var mr ModeResult
			if !sp.onVFS {
				mr, err = runNVMeMode(ctx, runner, forge, sp.name, sp.profile, sp.altStore, repDir, nvmeProbe)
			} else if useFUSE {
				mr, err = runVFSFUSEMode(ctx, runner, forge, sp.name, sp.profile, sp.altStore, repDir, journalRoot, key.String(), cfg.MountHelperBin, cfg.PutDelay, nvmeProbe)
			} else {
				mr, err = runVFSDirectMode(ctx, runner, forge, cipher, sp.name, sp.profile, sp.altStore, repDir, journalRoot, cfg.PutDelay, nvmeProbe)
			}
			if err != nil {
				return nil, fmt.Errorf("gitvfs: mode %s rep %d: %w", sp.name, rep, err)
			}
			if sp.useAlt {
				if err := altStore.VerifyUnmodified(); err != nil {
					return nil, fmt.Errorf("gitvfs: alternates modified in %s: %w", sp.name, err)
				}
				mr.AlternatesVerified = true
			}
			runs = append(runs, mr)
		}
		modeResults = append(modeResults, medianModeResult(runs))
	}

	// 6. Batched vs Unbatched fsync bulk-staging comparison (NVMe + VFS).
	fsyncRows, err := runFsyncBatchMatrix(ctx, runner, forge, cipher, workRoot, journalRoot, key.String(), cfg.MountHelperBin, useFUSE, cfg.BulkStageFiles, cfg.PutDelay, nvmeProbe)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: fsync batch matrix: %w", err)
	}

	// 7. O(1) Copy-on-Write Volume Snapshot Clone-by-Reference across N sandboxes.
	cowRes, err := runCOWCloneBench(ctx, runner, forge, cipher, workRoot, journalRoot, cfg.COWSandboxes)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: COW clone bench: %w", err)
	}

	// 8. Honest Negative Regimes.
	negRegimes, err := runNegativeRegimes(ctx, runner, forge, workRoot, cfg.Seed)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: negative regimes: %w", err)
	}

	return &BenchReport{
		Issue:             "hanzoai/vfs#29",
		HIP:               "HIP-1342",
		GeneratedAt:       time.Now().UTC(),
		GoVersion:         runtime.Version(),
		GitVersion:        runner.GitVersion(ctx),
		OS:                runtime.GOOS,
		Arch:              runtime.GOARCH,
		CPUs:              runtime.NumCPU(),
		FSMonitorSupport:  runner.FSMonitorSupported(ctx),
		FUSEMountUsed:     useFUSE,
		Config:            cfg,
		CorpusCommitSHA:   forgeStatus.DefaultBranchCommitID,
		CorpusFiles:       corpusFiles,
		NVMeProbe:         nvmeProbe,
		ForgeStatus:       forgeStatus,
		ClosePublishCheck: closeCheck,
		Modes:             modeResults,
		FsyncBatchMatrix:  fsyncRows,
		COWClone:          cowRes,
		NegativeRegimes:   negRegimes,
	}, nil
}

func runClosePublishVerification(ctx context.Context, cipher vfs.Cipher, workRoot, journalRoot string, putDelay time.Duration) (CloseAndPublishResult, error) {
	storeDir := filepath.Join(workRoot, "close-check-store")
	jDir := filepath.Join(journalRoot, "close-check-journal")
	rawBe, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return CloseAndPublishResult{}, err
	}
	cntBe := NewCountingBackend(rawBe)
	cntBe.PutDelay = putDelay
	cfg := vfs.Config{
		Backend:         cntBe,
		Cipher:          cipher,
		Round:           1,
		Journal:         jDir,
		JournalCapacity: 64 << 20,
		Bound:           1 * time.Second, // well within the 5s HIP-1342 target
	}
	if err := vfs.Create(ctx, cfg); err != nil {
		_ = cntBe.Close()
		return CloseAndPublishResult{}, err
	}
	vol, err := vfs.Attach(ctx, cfg)
	if err != nil {
		_ = cntBe.Close()
		return CloseAndPublishResult{}, err
	}
	defer vol.Close()

	payload := []byte(strings.Repeat("hanzo-vfs-git-close-check\n", 1024)) // ~26 KiB (chunked)
	return VerifyCloseAndPublishBound(ctx, vol, cntBe, "git-verify/object.bin", payload, 5*time.Second)
}

func runGitSequence(ctx context.Context, r *Runner, forge *Forge, profile Profile, altStore *AlternatesStore, repoDir, sandboxID string, snapshotFn func() (BackendMetrics, JournalMetrics, error), flushFn func() error) (ModeResult, error) {
	ops := make(map[string]OpMeasurement)
	snap := func() (BackendMetrics, JournalMetrics) {
		if snapshotFn == nil {
			return BackendMetrics{}, JournalMetrics{}
		}
		b, j, _ := snapshotFn()
		return b, j
	}
	recordOp := func(name string, dur time.Duration, b0, b1 BackendMetrics, j0, j1 JournalMetrics) {
		db := b1.Delta(b0)
		ops[name] = OpMeasurement{
			WallMs:       float64(dur.Microseconds()) / 1000.0,
			TotalPuts:    db.TotalPuts,
			PackPuts:     db.PackPuts,
			MetaPuts:     db.MetaPuts,
			HeadPuts:     db.HeadPuts,
			PutBytes:     db.PutBytes,
			JournalSyncs: j1.Syncs - j0.Syncs,
		}
	}

	bStart, jStart := snap()

	// 1. Clone (with optional alternates & profile settings).
	b0, j0 := snap()
	sc, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:   "tenant-a",
		SandboxID:  sandboxID,
		RepoID:     "vfs",
		RemoteURL:  forge.FileURL(),
		TargetDir:  repoDir,
		Profile:    profile,
		Alternates: altStore,
	})
	if err != nil {
		return ModeResult{}, err
	}
	if altStore != nil {
		defer altStore.RemoveBorrower(sandboxID)
	}
	b1, j1 := snap()
	recordOp("clone", sc.CloneDuration, b0, b1, j0, j1)

	ownCounts, err := sc.CountOwnObjects(ctx)
	if err != nil {
		return ModeResult{}, err
	}

	// 2. Cold git status (immediately after clone -> defines clone-to-first-command).
	b0, j0 = snap()
	stColdRes, err := r.Run(ctx, repoDir, "status", "--porcelain")
	if err != nil {
		return ModeResult{}, err
	}
	b1, j1 = snap()
	recordOp("status_cold", stColdRes.Duration, b0, b1, j0, j1)

	// 3. Warm git status (repeated status on unchanged tree; exercises untrackedCache).
	b0, j0 = snap()
	stWarmRes, err := r.Run(ctx, repoDir, "status", "--porcelain")
	if err != nil {
		return ModeResult{}, err
	}
	b1, j1 = snap()
	recordOp("status_warm", stWarmRes.Duration, b0, b1, j0, j1)

	// 4. Fetch & checkout bulk branch, then switch back to default branch.
	if forge.BulkBranch != "" {
		fetchArgs := []string{"fetch", "--quiet", "--depth", "1", "origin", forge.BulkBranch + ":refs/remotes/origin/" + forge.BulkBranch}
		if _, err := r.Run(ctx, repoDir, fetchArgs...); err != nil {
			return ModeResult{}, err
		}
		b0, j0 = snap()
		tCo := time.Now()
		if _, err := r.Run(ctx, repoDir, "checkout", "--quiet", "-B", forge.BulkBranch, "refs/remotes/origin/"+forge.BulkBranch); err != nil {
			return ModeResult{}, err
		}
		if _, err := r.Run(ctx, repoDir, "checkout", "--quiet", forge.DefaultBranch); err != nil {
			return ModeResult{}, err
		}
		coDur := time.Since(tCo)
		b1, j1 = snap()
		recordOp("checkout", coDur, b0, b1, j0, j1)
	}

	// 5. Edit + git add + git commit + git push to bare forge.
	b0, j0 = snap()
	editPath := filepath.Join("pkg", "gitvfs", "bench_edit_"+sandboxID+".txt")
	if len(profile.SparseCones) > 0 {
		editPath = filepath.Join(profile.SparseCones[0], "bench_edit_"+sandboxID+".txt")
	}
	pushRes, err := sc.CommitAndPush(ctx, forge, editPath, []byte(fmt.Sprintf("sandbox %s edit at %d\n", sandboxID, time.Now().UnixNano())), "bench: sandbox commit", "bench-"+sandboxID)
	if err != nil {
		return ModeResult{}, err
	}
	b1, j1 = snap()
	recordOp("commit_and_push", pushRes.CommitTime+pushRes.PushTime, b0, b1, j0, j1)

	// 6. Explicit Flush to observe total object-store PUTs & bytes for the sequence.
	var flushDur time.Duration
	if flushFn != nil {
		b0, j0 = snap()
		tFlush := time.Now()
		if err := flushFn(); err != nil {
			return ModeResult{}, err
		}
		flushDur = time.Since(tFlush)
		b1, j1 = snap()
		recordOp("flush", flushDur, b0, b1, j0, j1)
	}

	bEnd, jEnd := snap()
	totalB := bEnd.Delta(bStart)

	lsFilesRes, _ := r.Run(ctx, repoDir, "ls-files")
	trackedCount := len(splitNonEmptyLines(lsFilesRes.Stdout))
	worktreeCount := countWorktreeFiles(repoDir)

	return ModeResult{
		ProfileName:        profile.Name,
		CloneToFirstCmdMs:  ops["clone"].WallMs + ops["status_cold"].WallMs,
		CloneMs:            ops["clone"].WallMs,
		StatusColdMs:       ops["status_cold"].WallMs,
		StatusWarmMs:       ops["status_warm"].WallMs,
		CheckoutMs:         ops["checkout"].WallMs,
		CommitMs:           float64(pushRes.CommitTime.Microseconds()) / 1000.0,
		PushMs:             float64(pushRes.PushTime.Microseconds()) / 1000.0,
		CommitAndPushMs:    float64((pushRes.CommitTime + pushRes.PushTime).Microseconds()) / 1000.0,
		FlushMs:            float64(flushDur.Microseconds()) / 1000.0,
		OwnGitObjects:      ownCounts.OwnObjects,
		OwnGitKB:           ownCounts.OwnKB,
		TrackedFiles:       trackedCount,
		WorktreeFiles:      worktreeCount,
		JournalFsyncs:      jEnd.Syncs - jStart.Syncs,
		JournalFsyncP50Us:  jEnd.SyncP50Us,
		TotalVFSBackendPut: totalB.TotalPuts,
		TotalVFSPutBytes:   totalB.PutBytes,
		PackPuts:           totalB.PackPuts,
		MetaPuts:           totalB.MetaPuts,
		HeadPuts:           totalB.HeadPuts,
		Ops:                ops,
	}, nil
}

func runNVMeMode(ctx context.Context, r *Runner, forge *Forge, name string, profile Profile, altStore *AlternatesStore, repDir string, nvme NVMeProbeResult) (ModeResult, error) {
	if err := os.MkdirAll(repDir, 0o755); err != nil {
		return ModeResult{}, err
	}
	repoDir := filepath.Join(repDir, "repo")
	mr, err := runGitSequence(ctx, r, forge, profile, altStore, repoDir, name+"-"+filepath.Base(repDir), nil, nil)
	if err != nil {
		return ModeResult{}, err
	}
	mr.Mode = name
	mr.StorageLayer = "plain-nvme"
	mr.JournalFsyncP50Us = nvme.FsyncP50Us
	mr.FsyncRatioVsNVMe = 1.0
	return mr, nil
}

func runVFSFUSEMode(ctx context.Context, r *Runner, forge *Forge, name string, profile Profile, altStore *AlternatesStore, repDir, journalRoot, keyStr, helperBin string, putDelay time.Duration, nvme NVMeProbeResult) (ModeResult, error) {
	storeDir := filepath.Join(repDir, "store")
	mp := filepath.Join(repDir, "mnt")
	jDir := filepath.Join(journalRoot, filepath.Base(repDir))
	if err := os.MkdirAll(mp, 0o755); err != nil {
		return ModeResult{}, err
	}
	serveArgs := []string{
		"--serve-mount",
		"--store-dir", storeDir,
		"--mountpoint", mp,
		"--journal-dir", jDir,
		"--round", "1",
		"--put-delay", putDelay.String(),
	}
	mc, err := StartMountedVolumeChild(helperBin, serveArgs, mp, keyStr)
	if err != nil {
		return ModeResult{}, err
	}
	defer func() { _ = mc.Stop() }()

	snapFn := func() (BackendMetrics, JournalMetrics, error) {
		m, err := mc.Metrics()
		return m.Backend, m.Journal, err
	}
	repoDir := filepath.Join(mp, "repo")
	mr, err := runGitSequence(ctx, r, forge, profile, altStore, repoDir, name+"-"+filepath.Base(repDir), snapFn, mc.Flush)
	if err != nil {
		return ModeResult{}, err
	}
	mr.Mode = name
	mr.StorageLayer = "vfs-fuse"
	if nvme.FsyncP50Us > 0 && mr.JournalFsyncP50Us > 0 {
		mr.FsyncRatioVsNVMe = mr.JournalFsyncP50Us / nvme.FsyncP50Us
	}
	return mr, nil
}

func runVFSDirectMode(ctx context.Context, r *Runner, forge *Forge, cipher vfs.Cipher, name string, profile Profile, altStore *AlternatesStore, repDir, journalRoot string, putDelay time.Duration, nvme NVMeProbeResult) (ModeResult, error) {
	storeDir := filepath.Join(repDir, "store")
	workDir := filepath.Join(repDir, "work")
	jDir := filepath.Join(journalRoot, filepath.Base(repDir))
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return ModeResult{}, err
	}

	rawBe, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return ModeResult{}, err
	}
	cntBe := NewCountingBackend(rawBe)
	cntBe.PutDelay = putDelay
	jt := NewJournalTracker()
	cfg := vfs.Config{
		Backend:         cntBe,
		Cipher:          cipher,
		Round:           1,
		Journal:         jDir,
		JournalCapacity: 256 << 20,
		Bound:           5 * time.Second,
		Disk:            jt.DiskFactory(),
	}
	if err := vfs.Create(ctx, cfg); err != nil {
		_ = cntBe.Close()
		return ModeResult{}, err
	}
	vol, err := vfs.Attach(ctx, cfg)
	if err != nil {
		_ = cntBe.Close()
		return ModeResult{}, err
	}
	defer vol.Close()

	repoDir := filepath.Join(workDir, "repo")
	snapFn := func() (BackendMetrics, JournalMetrics, error) {
		return cntBe.Snapshot(), jt.Snapshot(), nil
	}
	flushFn := func() error {
		// Mirror the resulting repository tree into *vfs.Volume, sync to journal, and flush.
		if _, _, err := IngestDirToVolume(ctx, vol, repoDir); err != nil {
			return err
		}
		if err := vol.Sync(ctx); err != nil {
			return err
		}
		return vol.Flush(ctx)
	}
	mr, err := runGitSequence(ctx, r, forge, profile, altStore, repoDir, name+"-"+filepath.Base(repDir), snapFn, flushFn)
	if err != nil {
		return ModeResult{}, err
	}
	mr.Mode = name
	mr.StorageLayer = "vfs-volume"
	if nvme.FsyncP50Us > 0 && mr.JournalFsyncP50Us > 0 {
		mr.FsyncRatioVsNVMe = mr.JournalFsyncP50Us / nvme.FsyncP50Us
	}
	return mr, nil
}

func runFsyncBatchMatrix(ctx context.Context, r *Runner, forge *Forge, cipher vfs.Cipher, workRoot, journalRoot, keyStr, helperBin string, useFUSE bool, nFiles int, putDelay time.Duration, nvme NVMeProbeResult) ([]FsyncBatchRow, error) {
	type strat struct {
		name string
		prof Profile
	}
	defProf := TunedProfile()
	defProf.Fsync = FsyncDefault
	defProf.FsyncMethod = FsyncMethodDefault

	strats := []strat{
		{name: "default (no loose-object fsync)", prof: defProf},
		{name: "core.fsync=committed (unbatched)", prof: UnbatchedFsyncProfile()},
		{name: "core.fsync=committed + core.fsyncMethod=batch", prof: BatchedFsyncProfile()},
	}
	hasStrace := false
	if _, err := exec.LookPath("strace"); err == nil {
		hasStrace = true
	}

	var rows []FsyncBatchRow

	// Measure on Plain NVMe AND on VFS (FUSE if available, else direct Volume).
	storages := []string{"plain-nvme"}
	if useFUSE {
		storages = append(storages, "vfs-fuse")
	} else {
		storages = append(storages, "vfs-volume")
	}

	for _, storage := range storages {
		for idx, st := range strats {
			caseDir := filepath.Join(workRoot, fmt.Sprintf("fsync-%s-%d", storage, idx))
			var repoDir string
			var mc *MountedVolumeClient
			var vol *vfs.Volume
			var cntBe *CountingBackend
			var jt *JournalTracker

			switch storage {
			case "plain-nvme":
				repoDir = filepath.Join(caseDir, "repo")
			case "vfs-fuse":
				mp := filepath.Join(caseDir, "mnt")
				if err := os.MkdirAll(mp, 0o755); err != nil {
					return nil, err
				}
				args := []string{
					"--serve-mount",
					"--store-dir", filepath.Join(caseDir, "store"),
					"--mountpoint", mp,
					"--journal-dir", filepath.Join(journalRoot, fmt.Sprintf("fsync-fuse-%d", idx)),
					"--round", "1",
					"--put-delay", putDelay.String(),
				}
				var err error
				mc, err = StartMountedVolumeChild(helperBin, args, mp, keyStr)
				if err != nil {
					return nil, err
				}
				repoDir = filepath.Join(mp, "repo")
			case "vfs-volume":
				repoDir = filepath.Join(caseDir, "repo")
				rawBe, err := openFileBackend(ctx, filepath.Join(caseDir, "store"))
				if err != nil {
					return nil, err
				}
				cntBe = NewCountingBackend(rawBe)
				cntBe.PutDelay = putDelay
				jt = NewJournalTracker()
				vcfg := vfs.Config{
					Backend:         cntBe,
					Cipher:          cipher,
					Round:           1,
					Journal:         filepath.Join(journalRoot, fmt.Sprintf("fsync-vol-%d", idx)),
					JournalCapacity: 128 << 20,
					Bound:           5 * time.Second,
					Disk:            jt.DiskFactory(),
				}
				if err := vfs.Create(ctx, vcfg); err != nil {
					_ = cntBe.Close()
					return nil, err
				}
				vol, err = vfs.Attach(ctx, vcfg)
				if err != nil {
					_ = cntBe.Close()
					return nil, err
				}
			}

			// Clone minimal repo with target profile.
			if _, err := CloneByReference(ctx, r, SandboxCloneOptions{
				RemoteURL: forge.FileURL(),
				TargetDir: repoDir,
				Profile:   st.prof,
			}); err != nil {
				if mc != nil {
					_ = mc.Stop()
				}
				if vol != nil {
					_ = vol.Close()
				}
				return nil, err
			}

			// Create nFiles new small files to trigger loose-object creation on git add.
			stageDir := filepath.Join(repoDir, "bulk_stage")
			if err := os.MkdirAll(stageDir, 0o755); err != nil {
				return nil, err
			}
			for i := range nFiles {
				p := filepath.Join(stageDir, fmt.Sprintf("f_%04d.txt", i))
				if err := os.WriteFile(p, []byte(fmt.Sprintf("payload-%d-batch-test\n", i)), 0o644); err != nil {
					return nil, err
				}
			}

			var jBefore JournalMetrics
			var bBefore BackendMetrics
			if mc != nil {
				_ = mc.Flush()
				m, _ := mc.Metrics()
				jBefore, bBefore = m.Journal, m.Backend
			} else if vol != nil {
				_ = vol.Flush(ctx)
				jBefore, bBefore = jt.Snapshot(), cntBe.Snapshot()
			}

			// Timed git add -A.
			addRes, err := r.Run(ctx, repoDir, "add", "-A")
			if err != nil {
				return nil, err
			}
			// Timed git commit.
			commitRes, err := r.Run(ctx, repoDir, "commit", "--quiet", "-m", "bulk stage")
			if err != nil {
				return nil, err
			}

			// In direct-volume mode, mirror the newly created loose objects into *vfs.Volume
			// using per-object fsync (unbatched) vs single batched fsync (batch) or no fsync (default).
			if vol != nil {
				looseFiles := listLooseObjectFiles(filepath.Join(repoDir, ".git", "objects"))
				jt.ResetSamples()
				for i, lf := range looseFiles {
					data, rerr := os.ReadFile(lf)
					if rerr != nil {
						continue
					}
					syncEach := st.prof.Fsync == FsyncCommitted && st.prof.FsyncMethod != FsyncMethodBatch
					if _, werr := WriteVolumeFile(ctx, vol, fmt.Sprintf("git-objects/obj-%05d", i), data, 0o644, syncEach); werr != nil {
						return nil, werr
					}
				}
				if st.prof.Fsync == FsyncCommitted && st.prof.FsyncMethod == FsyncMethodBatch {
					if err := vol.Sync(ctx); err != nil {
						return nil, err
					}
				}
			}

			var jAfter JournalMetrics
			var bAfter BackendMetrics
			if mc != nil {
				m, _ := mc.Metrics()
				jAfter, bAfter = m.Journal, m.Backend
				_ = mc.Stop()
			} else if vol != nil {
				jAfter, bAfter = jt.Snapshot(), cntBe.Snapshot()
				_ = vol.Close()
			}

			// Optional exact syscall fsync count on NVMe using strace -c -f on a fresh small duplicate.
			var straceFsyncs int64 = -1
			if storage == "plain-nvme" && hasStrace {
				straceFsyncs = countGitAddFsyncsStrace(ctx, r, forge, st.prof, filepath.Join(workRoot, fmt.Sprintf("strace-%d", idx)), min(nFiles, 150))
			}

			ratio := 1.0
			p50Us := nvme.FsyncP50Us
			if storage != "plain-nvme" {
				p50Us = jAfter.SyncP50Us
				if nvme.FsyncP50Us > 0 && p50Us > 0 {
					ratio = p50Us / nvme.FsyncP50Us
				} else {
					ratio = 0
				}
			}

			rows = append(rows, FsyncBatchRow{
				Storage:          storage,
				Strategy:         st.name,
				StagedFiles:      nFiles,
				AddWallMs:        float64(addRes.Duration.Microseconds()) / 1000.0,
				CommitWallMs:     float64(commitRes.Duration.Microseconds()) / 1000.0,
				StraceFsyncs:     straceFsyncs,
				JournalSyncs:     jAfter.Syncs - jBefore.Syncs,
				JournalSyncP50Us: p50Us,
				FsyncRatioVsNVMe: ratio,
				VFSBackendPuts:   bAfter.Delta(bBefore).TotalPuts,
			})
		}
	}
	return rows, nil
}

func runCOWCloneBench(ctx context.Context, r *Runner, forge *Forge, cipher vfs.Cipher, workRoot, journalRoot string, nSandboxes int) (COWSandboxResult, error) {
	baseCheckout := filepath.Join(workRoot, "cow-base-checkout")
	prof := SnapshotCloneProfile()
	if _, err := CloneByReference(ctx, r, SandboxCloneOptions{
		RemoteURL: forge.FileURL(),
		TargetDir: baseCheckout,
		Profile:   prof,
	}); err != nil {
		return COWSandboxResult{}, err
	}
	// Warm index once before taking snapshot (HIP-1342 §6.4).
	if _, err := r.Run(ctx, baseCheckout, "status", "--porcelain"); err != nil {
		return COWSandboxResult{}, err
	}
	headRes, err := r.Run(ctx, baseCheckout, "rev-parse", "HEAD")
	if err != nil {
		return COWSandboxResult{}, err
	}
	headSHA := strings.TrimSpace(headRes.Stdout)

	snap, err := CreateVolumeSnapshot(
		ctx, cipher, "tenant-a", "vfs",
		baseCheckout,
		filepath.Join(workRoot, "cow-base-store"),
		filepath.Join(journalRoot, "cow-base-journal"),
		headSHA, forge.DefaultBranch,
	)
	if err != nil {
		return COWSandboxResult{}, err
	}
	defer snap.Close()

	baseBefore := snap.BaseBackend.Snapshot()
	cloneTimes := make([]float64, 0, nSandboxes)
	var headPutsEach, packPutsEach, metaPutsEach int64
	var postStatusMs, postCommitPushMs float64
	var deltaFlushPuts, deltaFlushBytes int64

	for i := range nSandboxes {
		sbID := fmt.Sprintf("cow-sb-%02d", i)
		cv, err := CloneVolumeByReference(
			ctx, snap, "tenant-a", sbID,
			filepath.Join(workRoot, "cow-delta-"+sbID),
			filepath.Join(journalRoot, "cow-journal-"+sbID),
		)
		if err != nil {
			return COWSandboxResult{}, err
		}
		cloneMs := float64(cv.CloneDuration.Microseconds()) / 1000.0
		cloneTimes = append(cloneTimes, cloneMs)
		if i == 0 {
			headPutsEach = cv.ClonePuts.HeadPuts
			packPutsEach = cv.ClonePuts.PackPuts
			metaPutsEach = cv.ClonePuts.MetaPuts

			// Verify round-trip git status + commit + push from cloned sandbox + delta flush PUTs.
			exportDir := filepath.Join(workRoot, "cow-export-0")
			if _, _, err := ExportVolumeToDir(ctx, cv.Volume, exportDir); err != nil {
				_ = cv.Volume.Close()
				return COWSandboxResult{}, err
			}
			// Measure first status with checkStat=minimal + trustctime=false.
			stRes, err := r.Run(ctx, exportDir, "status", "--porcelain")
			if err != nil {
				_ = cv.Volume.Close()
				return COWSandboxResult{}, err
			}
			postStatusMs = float64(stRes.Duration.Microseconds()) / 1000.0

			// Write a sandbox delta file inside the COW Volume and flush to measure delta PUTs.
			b0 := cv.DeltaBackend.Snapshot()
			if _, err := WriteVolumeFile(ctx, cv.Volume, "pkg/gitvfs/cow_delta.txt", []byte("cow sandbox edit\n"), 0o644, true); err != nil {
				_ = cv.Volume.Close()
				return COWSandboxResult{}, err
			}
			if err := cv.Volume.Flush(ctx); err != nil {
				_ = cv.Volume.Close()
				return COWSandboxResult{}, err
			}
			dFlush := cv.DeltaBackend.Snapshot().Delta(b0)
			deltaFlushPuts = dFlush.TotalPuts
			deltaFlushBytes = dFlush.PutBytes

			// Commit & push back to bare forge.
			sc := &SandboxClone{
				TenantID:  "tenant-a",
				SandboxID: sbID,
				RepoID:    "vfs",
				Dir:       exportDir,
				Profile:   prof,
				runner:    r,
			}
			pr, err := sc.CommitAndPush(ctx, forge, "pkg/gitvfs/cow_delta.txt", []byte("cow sandbox edit\n"), "cow: sandbox commit", "cow-push")
			if err != nil {
				_ = cv.Volume.Close()
				return COWSandboxResult{}, err
			}
			postCommitPushMs = float64((pr.CommitTime + pr.PushTime).Microseconds()) / 1000.0
		}
		if err := cv.Volume.Close(); err != nil {
			return COWSandboxResult{}, err
		}
	}

	baseAfter := snap.BaseBackend.Snapshot().Delta(baseBefore)
	sort.Float64s(cloneTimes)

	return COWSandboxResult{
		Sandboxes:             nSandboxes,
		BaseFiles:             snap.Meta.FileCount,
		BaseBytes:             snap.Meta.TotalBytes,
		BaseBuildMs:           float64(snap.BuildTime.Microseconds()) / 1000.0,
		BaseTotalPuts:         snap.BuildPuts.TotalPuts,
		BasePackPuts:          snap.BuildPuts.PackPuts,
		BaseMetaPuts:          snap.BuildPuts.MetaPuts,
		MedianCloneMs:         medianFloat64(cloneTimes),
		MaxCloneMs:            cloneTimes[len(cloneTimes)-1],
		CloneHeadPutsEach:     headPutsEach,
		ClonePackPutsEach:     packPutsEach,
		CloneMetaPutsEach:     metaPutsEach,
		BaseWritesDuringClone: baseAfter.TotalPuts + baseAfter.Deletes,
		PostCloneStatusMs:     postStatusMs,
		PostCloneCommitPushMs: postCommitPushMs,
		DeltaFlushPuts:        deltaFlushPuts,
		DeltaFlushBytes:       deltaFlushBytes,
	}, nil
}

func runNegativeRegimes(ctx context.Context, r *Runner, forge *Forge, workRoot string, seed int64) ([]NegativeRegimeResult, error) {
	var out []NegativeRegimeResult

	// NR-1: Tiny 5-file repo where commit-graph + multi-pack-index + untrackedCache setup adds overhead.
	tinyBare := filepath.Join(workRoot, "tiny-bare.git")
	tinySrc := filepath.Join(workRoot, "tiny-src")
	if err := os.MkdirAll(tinySrc, 0o755); err != nil {
		return nil, err
	}
	if _, err := r.Run(ctx, tinySrc, "init", "--quiet"); err != nil {
		return nil, err
	}
	for i := range 5 {
		if err := os.WriteFile(filepath.Join(tinySrc, fmt.Sprintf("f%d.txt", i)), []byte(fmt.Sprintf("tiny %d\n", i)), 0o644); err != nil {
			return nil, err
		}
	}
	if _, err := r.Run(ctx, tinySrc, "add", "-A"); err != nil {
		return nil, err
	}
	if _, err := r.Run(ctx, tinySrc, "commit", "--quiet", "-m", "init tiny"); err != nil {
		return nil, err
	}
	tinyForge, err := InitForgeFromRepo(ctx, r, tinySrc, tinyBare)
	if err != nil {
		return nil, err
	}
	var tinyPlainTimes, tinyTunedTimes []float64
	plainProf := Profile{Name: "plain-tiny", Depth: 0, SingleBranch: true}
	tunedProf := TunedProfile()
	tunedProf.Depth = 0
	for i := range 3 {
		d1 := filepath.Join(workRoot, fmt.Sprintf("tiny-plain-%d", i))
		t0 := time.Now()
		if _, err := CloneByReference(ctx, r, SandboxCloneOptions{RemoteURL: tinyForge.FileURL(), TargetDir: d1, Profile: plainProf}); err != nil {
			return nil, err
		}
		if _, err := r.Run(ctx, d1, "status", "--porcelain"); err != nil {
			return nil, err
		}
		tinyPlainTimes = append(tinyPlainTimes, float64(time.Since(t0).Microseconds())/1000.0)

		d2 := filepath.Join(workRoot, fmt.Sprintf("tiny-tuned-%d", i))
		t1 := time.Now()
		if _, err := CloneByReference(ctx, r, SandboxCloneOptions{RemoteURL: tinyForge.FileURL(), TargetDir: d2, Profile: tunedProf}); err != nil {
			return nil, err
		}
		if _, err := r.Run(ctx, d2, "status", "--porcelain"); err != nil {
			return nil, err
		}
		tinyTunedTimes = append(tinyTunedTimes, float64(time.Since(t1).Microseconds())/1000.0)
	}
	plainMed := medianFloat64(tinyPlainTimes)
	tunedMed := medianFloat64(tinyTunedTimes)
	out = append(out, NegativeRegimeResult{
		ID:            "NR-1",
		Name:          "Tiny 5-file repository setup overhead",
		Summary:       "Running commit-graph + multi-pack-index + local config tuning on a 5-file repo adds extra git process invocations without recovering the cost on status/checkout.",
		BaselineLabel: "plain clone + status (5 files)",
		BaselineMs:    plainMed,
		VariantLabel:  "tuned clone + commit-graph + MIDX + status (5 files)",
		VariantMs:     tunedMed,
		RegressionPct: pctChange(plainMed, tunedMed),
		Details:       map[string]any{"files": 5, "seed": seed},
	})

	// NR-2: Full-history `git log -S` over blobless partial clone (`--filter=blob:none`)
	// triggers on-demand promisor fetches back to the forge.
	fullHistDir := filepath.Join(workRoot, "nr2-full")
	bloblessDir := filepath.Join(workRoot, "nr2-blobless")
	if _, err := r.Run(ctx, workRoot, "clone", "--quiet", forge.FileURL(), fullHistDir); err != nil {
		return nil, err
	}
	if _, err := r.Run(ctx, workRoot, "clone", "--quiet", "--filter=blob:none", forge.FileURL(), bloblessDir); err != nil {
		return nil, err
	}
	fullLogRes, err := r.Run(ctx, fullHistDir, "log", "-n", "25", "-S", "Volume", "--oneline")
	if err != nil {
		return nil, err
	}
	bloblessLogRes, err := r.Run(ctx, bloblessDir, "log", "-n", "25", "-S", "Volume", "--oneline")
	if err != nil {
		return nil, err
	}
	fullMs := float64(fullLogRes.Duration.Microseconds()) / 1000.0
	bloblessMs := float64(bloblessLogRes.Duration.Microseconds()) / 1000.0
	out = append(out, NegativeRegimeResult{
		ID:            "NR-2",
		Name:          "Full-history git log -S over blobless partial clone (--filter=blob:none)",
		Summary:       "History content search (`git log -S`) on a blobless partial clone forces synchronous on-demand promisor fetches for historical blobs from the forge, regressing wall time vs a full clone.",
		BaselineLabel: "full clone `git log -n 25 -S Volume`",
		BaselineMs:    fullMs,
		VariantLabel:  "blobless partial clone `git log -n 25 -S Volume`",
		VariantMs:     bloblessMs,
		RegressionPct: pctChange(fullMs, bloblessMs),
		Details:       map[string]any{"matches_full": len(splitNonEmptyLines(fullLogRes.Stdout)), "matches_blobless": len(splitNonEmptyLines(bloblessLogRes.Stdout))},
	})

	// NR-3: Shared alternates `git gc --prune=now` hazard on live borrowers.
	hazAltDir := filepath.Join(workRoot, "nr3-alt.git")
	hazStore, err := NewAlternatesStore(ctx, r, "tenant-haz", "vfs", forge.FileURL(), hazAltDir)
	if err != nil {
		return nil, err
	}
	hazCloneDir := filepath.Join(workRoot, "nr3-borrower")
	hazClone, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:   "tenant-haz",
		SandboxID:  "sb-haz",
		RepoID:     "vfs",
		RemoteURL:  forge.FileURL(),
		TargetDir:  hazCloneDir,
		Profile:    ReferenceProfile(),
		Alternates: hazStore,
	})
	if err != nil {
		return nil, err
	}
	// SafePrune MUST refuse while borrower is live.
	safePruneBlocked := false
	if err := hazStore.SafePrune(ctx); err != nil {
		safePruneBlocked = true
	}
	// Forced prune breaks live borrower fsck!
	if err := hazStore.ForcePruneAllRefsForHazardTest(ctx); err != nil {
		return nil, err
	}
	_, fsckAfterPruneErr := r.Run(ctx, hazClone.Dir, "fsck", "--no-dangling")
	borrowerCorrupted := fsckAfterPruneErr != nil
	out = append(out, NegativeRegimeResult{
		ID:            "NR-3",
		Name:          "Shared alternates git gc --prune=now hazard",
		Summary:       "Pruning unreachable objects in a shared alternates store while live non-dissociated sandboxes still reference it corrupts borrower git fsck. SafePrune() blocks pruning until all borrowers dissociate.",
		BaselineLabel: "borrower fsck before store prune (healthy)",
		BaselineMs:    0,
		VariantLabel:  "borrower fsck after forced store prune (broken)",
		VariantMs:     0,
		RegressionPct: 0,
		Details: map[string]any{
			"safe_prune_blocked_with_active_borrower": safePruneBlocked,
			"borrower_fsck_failed_after_forced_prune": borrowerCorrupted,
		},
	})

	return out, nil
}

func countGitAddFsyncsStrace(ctx context.Context, r *Runner, forge *Forge, prof Profile, dir string, nFiles int) int64 {
	repoDir := filepath.Join(dir, "repo")
	if _, err := CloneByReference(ctx, r, SandboxCloneOptions{
		RemoteURL: forge.FileURL(),
		TargetDir: repoDir,
		Profile:   prof,
	}); err != nil {
		return -1
	}
	stageDir := filepath.Join(repoDir, "s")
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return -1
	}
	for i := range nFiles {
		_ = os.WriteFile(filepath.Join(stageDir, fmt.Sprintf("f_%04d.txt", i)), []byte(fmt.Sprintf("trace-%d\n", i)), 0o644)
	}
	outFile := filepath.Join(dir, "strace.txt")
	tctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(tctx, "strace", "-f", "-c", "-o", outFile, "-e", "trace=fsync,fdatasync", r.GitBin, "-c", "core.fsmonitor=false", "add", "-A")
	cmd.Dir = repoDir
	cmd.Env = r.env()
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		return -1
	}
	raw, err := os.ReadFile(outFile)
	if err != nil {
		return -1
	}
	var total int64
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 5 {
			sys := fields[len(fields)-1]
			if sys == "fsync" || sys == "fdatasync" {
				if c, err := strconv.ParseInt(fields[3], 10, 64); err == nil {
					total += c
				}
			}
		}
	}
	return total
}

func listLooseObjectFiles(objectsDir string) []string {
	var out []string
	entries, err := os.ReadDir(objectsDir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) != 2 {
			continue
		}
		if e.Name() == "in" || e.Name() == "pa" {
			continue
		}
		sub := filepath.Join(objectsDir, e.Name())
		subEnts, err := os.ReadDir(sub)
		if err != nil {
			continue
		}
		for _, se := range subEnts {
			if !se.IsDir() {
				out = append(out, filepath.Join(sub, se.Name()))
			}
		}
	}
	sort.Strings(out)
	return out
}

func countWorktreeFiles(repoDir string) int {
	n := 0
	_ = filepath.WalkDir(repoDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && d.Name() == ".git" {
			return filepath.SkipDir
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	return n
}

func medianFloat64(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2.0
}

func medianModeResult(runs []ModeResult) ModeResult {
	if len(runs) == 1 {
		return runs[0]
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].CloneToFirstCmdMs < runs[j].CloneToFirstCmdMs
	})
	return runs[len(runs)/2]
}

func pctChange(base, variant float64) float64 {
	if base <= 0 {
		return 0
	}
	return ((variant - base) / base) * 100.0
}

// RenderMarkdown renders BenchReport into a clean, publication-ready HIP-1342
// Markdown report with plain NVMe alongside every number.
func (r *BenchReport) RenderMarkdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Native Git on VFS — Benchmark & Architecture Report (%s, %s)\n\n", r.Issue, r.HIP)
	fmt.Fprintf(&b, "Generated: `%s`  \n", r.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "Host: `%s/%s`, `%d` CPUs, `%s`, `%s`  \n", r.OS, r.Arch, r.CPUs, r.GoVersion, r.GitVersion)
	fmt.Fprintf(&b, "Corpus: real `hanzoai/vfs` repository (`%s`, `+%d` deterministic files, seed `%d`, total `%d` tracked files)  \n", shortSHA(r.CorpusCommitSHA), r.Config.ExtraFiles, r.Config.Seed, r.CorpusFiles)
	fmt.Fprintf(&b, "VFS Execution Path: `fuse_mount=%v`, `fsmonitor_supported=%v`, `repetitions=%d`\n\n", r.FUSEMountUsed, r.FSMonitorSupport, r.Config.Repetitions)

	b.WriteString("## 1. Plain NVMe Baseline Probe & Forge Verification\n\n")
	b.WriteString("| Primitive (Plain Local NVMe) | Latency (us) |\n|---|---:|\n")
	fmt.Fprintf(&b, "| `fsync` (4 KiB file, p50) | %.1f |\n", r.NVMeProbe.FsyncP50Us)
	fmt.Fprintf(&b, "| `fsync` (4 KiB file, p95) | %.1f |\n", r.NVMeProbe.FsyncP95Us)
	fmt.Fprintf(&b, "| `fsync` (4 KiB file, mean) | %.1f |\n", r.NVMeProbe.FsyncMeanUs)
	fmt.Fprintf(&b, "| `create + write(4 KiB)` (p50) | %.1f |\n", r.NVMeProbe.CreateP50Us)
	fmt.Fprintf(&b, "| `stat` (p50) | %.1f |\n", r.NVMeProbe.StatP50Us)
	fmt.Fprintf(&b, "| `rename` (p50) | %.1f |\n", r.NVMeProbe.RenameP50Us)
	fmt.Fprintf(&b, "| `unlink` (p50) | %.1f |\n\n", r.NVMeProbe.UnlinkP50Us)

	b.WriteString("### Server-Side Bare Forge Verification\n\n")
	fmt.Fprintf(&b, "- `protocol.version`: `%s`\n", r.ForgeStatus.ProtocolVersion)
	fmt.Fprintf(&b, "- `uploadpack.allowFilter`: `%v`, `uploadpack.allowAnySHA1InWant`: `%v`\n", r.ForgeStatus.AllowFilter, r.ForgeStatus.AllowAnySHA1InWant)
	fmt.Fprintf(&b, "- Pack reachability `.bitmap`: `%v` (`%d` pack, `%d` loose objects)\n", r.ForgeStatus.HasBitmapIndex, r.ForgeStatus.PackCount, r.ForgeStatus.LooseObjectCount)
	fmt.Fprintf(&b, "- `commit-graph` (`--reachable --changed-paths`): `%v`, `multi-pack-index`: `%v`, `git fsck --full`: `%v`\n\n", r.ForgeStatus.HasCommitGraph, r.ForgeStatus.HasMultiPackIndex, r.ForgeStatus.FsckClean)

	b.WriteString("### S3-Backed Disk Invariants (`*vfs.Volume`)\n\n")
	fmt.Fprintf(&b, "- **Zero synchronous store PUTs on clean `Write` + `Sync` + `Close`**: `%v` (`%d` PUTs during close)\n", r.ClosePublishCheck.ZeroSyncPutOnClose, r.ClosePublishCheck.PutsDuringWriteSyncClose)
	fmt.Fprintf(&b, "- **Acknowledged data published to store within `<= 5 s`**: `%v` (observed `%.1f ms`, `%d` PUTs, `%d` bytes)\n\n", r.ClosePublishCheck.PublishedWithinBound, r.ClosePublishCheck.PublishLatencyMs, r.ClosePublishCheck.PublishPuts, r.ClosePublishCheck.PublishBytes)

	b.WriteString("## 2. Side-by-Side Git Workload Results (vs Plain NVMe)\n\n")
	b.WriteString("| Mode | Layer | Clone-to-First-Cmd (ms) | Status Cold (ms) | Status Warm (ms) | Checkout (ms) | Commit+Push (ms) | Own Git Objects | VFS Store PUTs | VFS PUT Bytes | Journal fsyncs | fsync Ratio vs NVMe |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, m := range r.Modes {
		ratioStr := "-"
		if m.FsyncRatioVsNVMe > 0 {
			ratioStr = fmt.Sprintf("%.2fx", m.FsyncRatioVsNVMe)
		}
		fmt.Fprintf(&b, "| `%s` | `%s` | %.1f | %.1f | %.1f | %.1f | %.1f | %d (%d KiB) | %d (p:%d/m:%d/h:%d) | %d | %d | %s |\n",
			m.Mode, m.StorageLayer, m.CloneToFirstCmdMs, m.StatusColdMs, m.StatusWarmMs, m.CheckoutMs, m.CommitAndPushMs,
			m.OwnGitObjects, m.OwnGitKB, m.TotalVFSBackendPut, m.PackPuts, m.MetaPuts, m.HeadPuts, m.TotalVFSPutBytes, m.JournalFsyncs, ratioStr)
	}
	b.WriteByte('\n')

	b.WriteString("### Per-Operation VFS Object-Store PUTs, Bytes & Journal Syncs\n\n")
	b.WriteString("| Mode | Operation | Wall (ms) | Store PUTs (pack/meta/head) | PUT Bytes | Journal Syncs |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|\n")
	opOrder := []string{"clone", "status_cold", "status_warm", "checkout", "commit_and_push", "flush"}
	for _, m := range r.Modes {
		if m.StorageLayer == "plain-nvme" {
			continue
		}
		for _, opName := range opOrder {
			op, ok := m.Ops[opName]
			if !ok {
				continue
			}
			fmt.Fprintf(&b, "| `%s` | `%s` | %.1f | %d (%d/%d/%d) | %d | %d |\n",
				m.Mode, opName, op.WallMs, op.TotalPuts, op.PackPuts, op.MetaPuts, op.HeadPuts, op.PutBytes, op.JournalSyncs)
		}
	}
	b.WriteByte('\n')

	b.WriteString("## 3. Batched vs Unbatched `core.fsync=committed` Bulk Staging\n\n")
	b.WriteString("| Storage | Strategy | Staged Files | `git add` (ms) | `git commit` (ms) | `strace` fsyncs (150-file trace) | Journal Syncs | Journal fsync p50 (us) | fsync Ratio vs NVMe |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, row := range r.FsyncBatchMatrix {
		straceStr := "n/a"
		if row.StraceFsyncs >= 0 {
			straceStr = strconv.FormatInt(row.StraceFsyncs, 10)
		}
		ratioStr := "-"
		if row.FsyncRatioVsNVMe > 0 {
			ratioStr = fmt.Sprintf("%.2fx", row.FsyncRatioVsNVMe)
		}
		fmt.Fprintf(&b, "| `%s` | %s | %d | %.1f | %.1f | %s | %d | %.1f | %s |\n",
			row.Storage, row.Strategy, row.StagedFiles, row.AddWallMs, row.CommitWallMs, straceStr, row.JournalSyncs, row.JournalSyncP50Us, ratioStr)
	}
	b.WriteByte('\n')

	b.WriteString("## 4. O(1) Copy-on-Write `*vfs.Volume` Snapshot Clone-by-Reference\n\n")
	fmt.Fprintf(&b, "- Base snapshot: `%d` files (`%d` bytes), built in `%.1f ms` (`%d` total PUTs: `%d` pack, `%d` meta)\n",
		r.COWClone.BaseFiles, r.COWClone.BaseBytes, r.COWClone.BaseBuildMs, r.COWClone.BaseTotalPuts, r.COWClone.BasePackPuts, r.COWClone.BaseMetaPuts)
	fmt.Fprintf(&b, "- Cloned `%d` isolated sandboxes via `COWBackend` + `vfs.Reseal`: median `%.2f ms` (max `%.2f ms`)\n",
		r.COWClone.Sandboxes, r.COWClone.MedianCloneMs, r.COWClone.MaxCloneMs)
	fmt.Fprintf(&b, "- Per-sandbox clone PUTs: **`%d` head PUT, `%d` pack PUTs, `%d` meta PUTs** (`%d` mutations to read-only base backend)\n",
		r.COWClone.CloneHeadPutsEach, r.COWClone.ClonePackPutsEach, r.COWClone.CloneMetaPutsEach, r.COWClone.BaseWritesDuringClone)
	fmt.Fprintf(&b, "- Post-clone first `git status` (`core.checkStat=minimal`, `core.trustctime=false`): `%.1f ms`; commit+push to forge: `%.1f ms`; delta flush: `%d` PUTs (`%d` bytes)\n\n",
		r.COWClone.PostCloneStatusMs, r.COWClone.PostCloneCommitPushMs, r.COWClone.DeltaFlushPuts, r.COWClone.DeltaFlushBytes)

	b.WriteString("## 5. Honest Negative Regimes & Operational Hazards\n\n")
	b.WriteString("| ID | Regime / Hazard | Baseline | Variant | Delta / Outcome |\n|---|---|---|---|---|\n")
	for _, nr := range r.NegativeRegimes {
		var outcome string
		if nr.BaselineMs > 0 || nr.VariantMs > 0 {
			outcome = fmt.Sprintf("%.1f ms -> %.1f ms (%+.1f%%)", nr.BaselineMs, nr.VariantMs, nr.RegressionPct)
		} else {
			detJSON, _ := json.Marshal(nr.Details)
			outcome = string(detJSON)
		}
		fmt.Fprintf(&b, "| %s | **%s** — %s | %s | %s | %s |\n",
			nr.ID, nr.Name, nr.Summary, nr.BaselineLabel, nr.VariantLabel, outcome)
	}
	b.WriteByte('\n')

	b.WriteString("## 6. Reproducing\n\n")
	b.WriteString("```sh\n")
	b.WriteString("# Direct *vfs.Volume + plain NVMe benchmark (no root or FUSE required)\n")
	b.WriteString("go run ./cmd/gitvfs-bench --repo . --out-dir docs/research/hip-1342-native-git-on-vfs\n\n")
	b.WriteString("# Real kernel FUSE-mounted *vfs.Volume + plain NVMe benchmark (Linux user namespace)\n")
	b.WriteString("go build -o /tmp/fusermount3 ./tools/fusemount\n")
	b.WriteString("go build -tags fuse -o /tmp/gitvfs-bench ./cmd/gitvfs-bench\n")
	b.WriteString("unshare -Urm env PATH=/tmp:$PATH /tmp/gitvfs-bench --fuse --repo . --out-dir docs/research/hip-1342-native-git-on-vfs\n")
	b.WriteString("```\n")
	return b.String()
}

func shortSHA(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
