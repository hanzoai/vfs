// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cetofs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

const benchJournalCapacity int64 = 64 << 20

// SuiteConfig configures the reproducible CETOFS benchmark suite.
type SuiteConfig struct {
	// ScratchDir is the base directory on local disk for Plain NVMe, file://
	// backend stores, and VFS journals. If empty, a temporary directory is
	// created and removed on completion.
	ScratchDir string
	// SimulatedRTT is the simulated intra-region S3 network RTT per backend RPC
	// (default 2ms, matching intra-region Hanzo S3).
	SimulatedRTT time.Duration
	// CheckoutFiles is the number of files in checkout_5000 and git_status
	// (default 5000).
	CheckoutFiles int
	// NpmPackages is the number of packages in npm_ci (default 60).
	NpmPackages int
	// GoPackages is the number of packages in go_build (default 25).
	GoPackages int
}

// DefaultSuiteConfig returns the canonical HIP-1345 benchmark configuration
// (5,000-file checkout & git_status, 60-package npm_ci, 25-package go_build,
// 2ms intra-region S3 RTT).
func DefaultSuiteConfig() SuiteConfig {
	return SuiteConfig{
		SimulatedRTT:  2 * time.Millisecond,
		CheckoutFiles: 5000,
		NpmPackages:   60,
		GoPackages:    25,
	}
}

func (c SuiteConfig) normalize() SuiteConfig {
	if c.CheckoutFiles <= 0 {
		c.CheckoutFiles = 5000
	}
	if c.NpmPackages <= 0 {
		c.NpmPackages = 60
	}
	if c.GoPackages <= 0 {
		c.GoPackages = 25
	}
	return c
}

// StackMetrics holds the measured runtime, per-boundary request counts,
// requests-per-app-op ratios, and latency distributions for one execution stack.
type StackMetrics struct {
	Stack               string           `json:"stack"`
	AppOps              int64            `json:"app_ops"`
	WallTimeMs          float64          `json:"wall_time_ms"`
	AppOpLatency        LatencyStats     `json:"app_op_latency"`
	Boundary            BoundarySnapshot `json:"boundary"`
	FUSEReqsPerAppOp    float64          `json:"fuse_reqs_per_app_op"`
	BackendReqsPerAppOp float64          `json:"backend_reqs_per_app_op"`
	TotalReqsPerAppOp   float64          `json:"total_reqs_per_app_op"`
	Optimizer           OptimizerStats   `json:"optimizer,omitempty"`
}

// WorkloadReport compares Plain NVMe, Unoptimized FUSE/Gofer->VFS->S3, and
// CETOFS-Optimized FUSE/Gofer->VFS->S3 for a single canonical workload.
type WorkloadReport struct {
	Name                   string       `json:"name"`
	Description            string       `json:"description"`
	AppOps                 int64        `json:"app_ops"`
	PlainNVMe              StackMetrics `json:"plain_nvme"`
	Unoptimized            StackMetrics `json:"unoptimized"`
	Optimized              StackMetrics `json:"optimized"`
	FUSEReqReductionPct    float64      `json:"fuse_req_reduction_pct"`
	BackendReqReductionPct float64      `json:"backend_req_reduction_pct"`
	TotalReqReductionPct   float64      `json:"total_req_reduction_pct"`
	WallTimeSpeedupPct     float64      `json:"wall_time_speedup_pct"`
}

// NegativeRegimeReport captures the explicit negative / degradation regimes
// where CETOFS caching or aggressive coalescing hurts or forces extra retries.
type NegativeRegimeReport struct {
	// Sub-regime A: Multi-writer CAS contention on fenced Head.
	CASIterations              int     `json:"cas_iterations"`
	UncachedCASBackendReqs     int64   `json:"uncached_cas_backend_reqs"`
	CachedContendedBackendReqs int64   `json:"cached_contended_backend_reqs"`
	CachedContendedConflicts   int64   `json:"cached_contended_conflicts"`
	CASRequestOverheadPct      float64 `json:"cas_request_overhead_pct"`
	UncachedCASWallMs          float64 `json:"uncached_cas_wall_ms"`
	CachedContendedWallMs      float64 `json:"cached_contended_wall_ms"`
	CASWallOverheadPct         float64 `json:"cas_wall_overhead_pct"`
	CASAuthorityPreserved      bool    `json:"cas_authority_preserved"`

	// Sub-regime B: External mutation staleness under AttrTimeout before
	// generation invalidation, followed by immediate recovery on invalidation.
	StaleReadsBeforeInvalidation int  `json:"stale_reads_before_invalidation"`
	FreshAfterInvalidation       bool `json:"fresh_after_invalidation"`

	// Sub-regime C: Random non-adjacent pack reads (aggressive vs adaptive coalescing).
	RandomReadOps                int     `json:"random_read_ops"`
	PlainNVMeRandomReadMs        float64 `json:"plain_nvme_random_read_ms"`
	UnoptimizedRandomBytesRead   int64   `json:"unoptimized_random_bytes_read"`
	AggressiveCoalesceBytesRead  int64   `json:"aggressive_coalesce_bytes_read"`
	AggressiveReadAmplificationX float64 `json:"aggressive_read_amplification_x"`
	AdaptiveCoalesceBytesRead    int64   `json:"adaptive_coalesce_bytes_read"`
	AdaptiveReadAmplificationX   float64 `json:"adaptive_read_amplification_x"`
	AdaptiveRandomBackendReqs    int64   `json:"adaptive_random_backend_reqs"`
	UnoptimizedRandomBackendReqs int64   `json:"unoptimized_random_backend_reqs"`
	AggressiveRandomBackendReqs  int64   `json:"aggressive_random_backend_reqs"`
}

// SuiteReport is the complete JSON receipt written by cmd/cetofs-bench.
type SuiteReport struct {
	GeneratedAt    string               `json:"generated_at"`
	GoVersion      string               `json:"go_version"`
	GOOS           string               `json:"goos"`
	GOARCH         string               `json:"goarch"`
	NumCPU         int                  `json:"num_cpu"`
	SimulatedRTTMs float64              `json:"simulated_rtt_ms"`
	CheckoutFiles  int                  `json:"checkout_files"`
	Workloads      []WorkloadReport     `json:"workloads"`
	NegativeRegime NegativeRegimeReport `json:"negative_regime"`
	Guarantees     []GuaranteeRecord    `json:"guarantees"`
}

// RunSuite executes all 4 canonical workloads (npm_ci, checkout_5000,
// git_status, go_build) plus the negative multi-writer invalidation regime
// across Plain NVMe, Unoptimized VFS, and CETOFS-Optimized VFS.
func RunSuite(ctx context.Context, cfg SuiteConfig) (*SuiteReport, error) {
	cfg = cfg.normalize()
	baseDir := cfg.ScratchDir
	cleanup := false
	if baseDir == "" {
		var err error
		baseDir, err = os.MkdirTemp("", "cetofs-bench-*")
		if err != nil {
			return nil, fmt.Errorf("cetofs: create temp dir: %w", err)
		}
		cleanup = true
	}
	if cleanup {
		defer os.RemoveAll(baseDir)
	}

	// Canonical post-quantum Hybrid cipher (X25519 + ML-KEM-768).
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		return nil, fmt.Errorf("cetofs: generate hybrid identity: %w", err)
	}
	cipher := vfs.Hybrid(id)

	var workloads []WorkloadReport

	npmRep, err := runNpmCI(ctx, filepath.Join(baseDir, "npm_ci"), cipher, cfg)
	if err != nil {
		return nil, fmt.Errorf("npm_ci: %w", err)
	}
	workloads = append(workloads, npmRep)

	// checkout_5000 creates the repository reused identically by git_status.
	checkoutDir := filepath.Join(baseDir, "repo_5000")
	checkoutRep, repos, err := runCheckout5000(ctx, checkoutDir, cipher, cfg)
	if err != nil {
		return nil, fmt.Errorf("checkout_5000: %w", err)
	}
	workloads = append(workloads, checkoutRep)

	gitRep, err := runGitStatus(ctx, repos, cipher, cfg)
	if err != nil {
		return nil, fmt.Errorf("git_status: %w", err)
	}
	workloads = append(workloads, gitRep)

	goRep, err := runGoBuild(ctx, filepath.Join(baseDir, "go_build"), cipher, cfg)
	if err != nil {
		return nil, fmt.Errorf("go_build: %w", err)
	}
	workloads = append(workloads, goRep)

	negRep, err := runNegativeRegime(ctx, filepath.Join(baseDir, "negative"), cipher, cfg)
	if err != nil {
		return nil, fmt.Errorf("negative_regime: %w", err)
	}

	return &SuiteReport{
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
		GoVersion:      runtime.Version(),
		GOOS:           runtime.GOOS,
		GOARCH:         runtime.GOARCH,
		NumCPU:         runtime.NumCPU(),
		SimulatedRTTMs: float64(cfg.SimulatedRTT) / float64(time.Millisecond),
		CheckoutFiles:  cfg.CheckoutFiles,
		Workloads:      workloads,
		NegativeRegime: negRep,
		Guarantees:     GuaranteeCatalog(),
	}, nil
}

// --- Deterministic synthetic payload generator (zero external fixtures) ---

func deterministicBytes(seed uint64, size int) []byte {
	out := make([]byte, size)
	var hdr [16]byte
	binary.LittleEndian.PutUint64(hdr[0:8], seed)
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(size))
	h := blake3.New()
	_, _ = h.Write(hdr[:])
	h.Expand(out)
	return out
}

func openFileBackend(ctx context.Context, dir string) (backend.Backend, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return backend.Open(ctx, "file://"+filepath.ToSlash(dir))
}

// copyStore copies a file:// backend store directory cleanly so unoptimized
// and optimized runs operate over the exact same Head.Seed, shards, and packs.
func copyStore(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		if rel == ".lock" || rel == ".tmp" {
			return nil
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
}

func buildStackMetrics(stack string, appOps int64, wall time.Duration, appLat *latencyRecorder, snap BoundarySnapshot, opt OptimizerStats) StackMetrics {
	m := StackMetrics{
		Stack:        stack,
		AppOps:       appOps,
		WallTimeMs:   float64(wall) / float64(time.Millisecond),
		AppOpLatency: appLat.snapshot(),
		Boundary:     snap,
		Optimizer:    opt,
	}
	if appOps > 0 {
		m.FUSEReqsPerAppOp = float64(snap.FUSEBoundaryRequests) / float64(appOps)
		m.BackendReqsPerAppOp = float64(snap.BackendBoundaryRequests) / float64(appOps)
		m.TotalReqsPerAppOp = float64(snap.TotalRequests) / float64(appOps)
	}
	return m
}

func pctReduction(before, after float64) float64 {
	if before <= 0 {
		return 0
	}
	return (before - after) / before * 100.0
}

func finalizeWorkload(name, desc string, appOps int64, nvme, unopt, opt StackMetrics) WorkloadReport {
	return WorkloadReport{
		Name:                   name,
		Description:            desc,
		AppOps:                 appOps,
		PlainNVMe:              nvme,
		Unoptimized:            unopt,
		Optimized:              opt,
		FUSEReqReductionPct:    pctReduction(float64(unopt.Boundary.FUSEBoundaryRequests), float64(opt.Boundary.FUSEBoundaryRequests)),
		BackendReqReductionPct: pctReduction(float64(unopt.Boundary.BackendBoundaryRequests), float64(opt.Boundary.BackendBoundaryRequests)),
		TotalReqReductionPct:   pctReduction(float64(unopt.Boundary.TotalRequests), float64(opt.Boundary.TotalRequests)),
		WallTimeSpeedupPct:     pctReduction(unopt.WallTimeMs, opt.WallTimeMs),
	}
}

func mergeOptStats(a, b OptimizerStats) OptimizerStats {
	return OptimizerStats{
		AttrHits:            a.AttrHits + b.AttrHits,
		AttrMisses:          a.AttrMisses + b.AttrMisses,
		LookupHits:          a.LookupHits + b.LookupHits,
		LookupMisses:        a.LookupMisses + b.LookupMisses,
		DirHits:             a.DirHits + b.DirHits,
		DirMisses:           a.DirMisses + b.DirMisses,
		OpenStatSkipped:     a.OpenStatSkipped + b.OpenStatSkipped,
		MetaInvalidations:   a.MetaInvalidations + b.MetaInvalidations,
		HeadVersionHits:     a.HeadVersionHits + b.HeadVersionHits,
		HeadReadHits:        a.HeadReadHits + b.HeadReadHits,
		HeadCASConflicts:    a.HeadCASConflicts + b.HeadCASConflicts,
		ShardCacheHits:      a.ShardCacheHits + b.ShardCacheHits,
		ShardCacheMisses:    a.ShardCacheMisses + b.ShardCacheMisses,
		ImmutableStatHits:   a.ImmutableStatHits + b.ImmutableStatHits,
		ListCacheHits:       a.ListCacheHits + b.ListCacheHits,
		ListCacheMisses:     a.ListCacheMisses + b.ListCacheMisses,
		RangeCacheHits:      a.RangeCacheHits + b.RangeCacheHits,
		RangeCoalescedSpans: a.RangeCoalescedSpans + b.RangeCoalescedSpans,
		RangeReadAheadBytes: a.RangeReadAheadBytes + b.RangeReadAheadBytes,
	}
}

func drainList(ctx context.Context, be backend.Backend, prefix string) (int, error) {
	ch, errCh := be.List(ctx, prefix)
	count := 0
	for range ch {
		count++
	}
	if err := <-errCh; err != nil {
		return count, err
	}
	return count, nil
}

// --- Workload 1: npm_ci ---

func runNpmCI(ctx context.Context, dir string, cipher vfs.Cipher, cfg SuiteConfig) (WorkloadReport, error) {
	pkgs := cfg.NpmPackages
	lockBytes := deterministicBytes(1001, 96<<10) // 96 KiB (>16 KiB -> chunked into pack/)
	rootPkgBytes := deterministicBytes(1002, 2048)

	nvmeMetrics, err := runNpmCINVMe(filepath.Join(dir, "nvme"), pkgs, rootPkgBytes, lockBytes)
	if err != nil {
		return WorkloadReport{}, err
	}
	workStore := filepath.Join(dir, "work_store")
	emptyBackup := filepath.Join(dir, "empty_backup")
	if err := createEmptyVolume(ctx, workStore, cipher); err != nil {
		return WorkloadReport{}, err
	}
	if err := copyStore(workStore, emptyBackup); err != nil {
		return WorkloadReport{}, err
	}
	unoptMetrics, err := runNpmCIVFS(ctx, workStore, filepath.Join(dir, "unopt_journal"), cipher, cfg, pkgs, rootPkgBytes, lockBytes, false)
	if err != nil {
		return WorkloadReport{}, err
	}
	if err := os.RemoveAll(workStore); err != nil {
		return WorkloadReport{}, err
	}
	if err := copyStore(emptyBackup, workStore); err != nil {
		return WorkloadReport{}, err
	}
	optMetrics, err := runNpmCIVFS(ctx, workStore, filepath.Join(dir, "opt_journal"), cipher, cfg, pkgs, rootPkgBytes, lockBytes, true)
	if err != nil {
		return WorkloadReport{}, err
	}

	return finalizeWorkload(
		"npm_ci",
		fmt.Sprintf("Package tree unpack (%d packages + 96 KiB lockfile + chunked dist bundles) + 2 cold worker module-resolution passes", pkgs),
		unoptMetrics.AppOps,
		nvmeMetrics, unoptMetrics, optMetrics,
	), nil
}

func runNpmCINVMe(root string, pkgs int, rootPkg, lockBytes []byte) (StackMetrics, error) {
	if err := os.MkdirAll(filepath.Join(root, "node_modules"), 0o755); err != nil {
		return StackMetrics{}, err
	}
	var lat latencyRecorder
	var appOps int64
	start := time.Now()

	// Phase 1: Unpack.
	opStart := time.Now()
	if err := os.WriteFile(filepath.Join(root, "package.json"), rootPkg, 0o644); err != nil {
		return StackMetrics{}, err
	}
	if err := os.WriteFile(filepath.Join(root, "package-lock.json"), lockBytes, 0o644); err != nil {
		return StackMetrics{}, err
	}
	lat.add(time.Since(opStart))
	appOps += 2

	for i := range pkgs {
		pkgDir := filepath.Join(root, "node_modules", fmt.Sprintf("pkg-%03d", i))
		libDir := filepath.Join(pkgDir, "lib")
		opStart := time.Now()
		if err := os.MkdirAll(libDir, 0o755); err != nil {
			return StackMetrics{}, err
		}
		if err := os.WriteFile(filepath.Join(pkgDir, "package.json"), deterministicBytes(uint64(2000+i), 1024), 0o644); err != nil {
			return StackMetrics{}, err
		}
		if err := os.WriteFile(filepath.Join(libDir, "index.js"), deterministicBytes(uint64(3000+i), 2048), 0o644); err != nil {
			return StackMetrics{}, err
		}
		if i%3 == 0 {
			if err := os.WriteFile(filepath.Join(libDir, "bundle.js"), deterministicBytes(uint64(4000+i), 36<<10), 0o644); err != nil {
				return StackMetrics{}, err
			}
		}
		lat.add(time.Since(opStart))
		appOps++
	}

	// Phase 2: Two worker module-resolution & manifest/bundle read passes.
	for range 2 {
		opStart := time.Now()
		if _, err := os.Stat(filepath.Join(root, "package.json")); err != nil {
			return StackMetrics{}, err
		}
		if _, err := os.ReadFile(filepath.Join(root, "package-lock.json")); err != nil {
			return StackMetrics{}, err
		}
		lat.add(time.Since(opStart))
		appOps++

		for i := range pkgs {
			opStart := time.Now()
			p := filepath.Join(root, "node_modules", fmt.Sprintf("pkg-%03d", i), "package.json")
			if _, err := os.Stat(p); err != nil {
				return StackMetrics{}, err
			}
			if _, err := os.ReadFile(p); err != nil {
				return StackMetrics{}, err
			}
			if i%3 == 0 {
				bp := filepath.Join(root, "node_modules", fmt.Sprintf("pkg-%03d", i), "lib", "bundle.js")
				if _, err := os.ReadFile(bp); err != nil {
					return StackMetrics{}, err
				}
			}
			lat.add(time.Since(opStart))
			appOps++
		}
	}
	return buildStackMetrics("plain_nvme", appOps, time.Since(start), &lat, BoundarySnapshot{}, OptimizerStats{}), nil
}

func createEmptyVolume(ctx context.Context, storeDir string, cipher vfs.Cipher) error {
	be, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return err
	}
	defer be.Close()
	return vfs.Create(ctx, vfs.Config{Backend: be, Cipher: cipher})
}

func runNpmCIVFS(ctx context.Context, storeDir, journalDir string, cipher vfs.Cipher, cfg SuiteConfig, pkgs int, rootPkg, lockBytes []byte, optimized bool) (StackMetrics, error) {
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		return StackMetrics{}, err
	}

	rawBE, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return StackMetrics{}, err
	}
	rec := NewAuditRecorder()
	latBE := NewLatencyBackend(rawBE, UniformLatencyConfig(cfg.SimulatedRTT))
	instBE := NewInstrumentedBackend(latBE, rec)

	var volBE backend.Backend = instBE
	var optBE *OptimizedBackend
	optCfg := UnoptimizedConfig()
	if optimized {
		optCfg = DefaultOptimizerConfig()
		optCfg.HeadCacheTTL = 5 * time.Second
		optBE = NewOptimizedBackend(instBE, optCfg)
		volBE = optBE
	}

	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend:         volBE,
		Cipher:          cipher,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: benchJournalCapacity,
		Cache:           4 << 20,
	})
	if err != nil {
		_ = volBE.Close()
		return StackMetrics{}, err
	}
	rec.Reset()

	bridge := NewGoferBridge(vol, rec)
	var gfs GoferFS = bridge
	var cachedBridge *CachedGoferBridge
	if optimized {
		cachedBridge = NewCachedGoferBridge(bridge, optCfg)
		optBE.OnHeadInvalidation(cachedBridge.InvalidateAll)
		gfs = cachedBridge
	}

	var lat latencyRecorder
	var appOps int64
	start := time.Now()

	// Phase 1: Unpack tree into Volume.
	opStart := time.Now()
	if err := writeVFSFile(ctx, gfs, vfs.RootInode, "package.json", rootPkg); err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}
	if err := writeVFSFile(ctx, gfs, vfs.RootInode, "package-lock.json", lockBytes); err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}
	nmIno, err := gfs.Mkdir(ctx, vfs.RootInode, "node_modules", 0o755)
	if err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}
	lat.add(time.Since(opStart))
	appOps += 2

	for i := range pkgs {
		opStart := time.Now()
		pkgName := fmt.Sprintf("pkg-%03d", i)
		pIno, err := gfs.Mkdir(ctx, nmIno.ID, pkgName, 0o755)
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		libIno, err := gfs.Mkdir(ctx, pIno.ID, "lib", 0o755)
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		if err := writeVFSFile(ctx, gfs, pIno.ID, "package.json", deterministicBytes(uint64(2000+i), 1024)); err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		if err := writeVFSFile(ctx, gfs, libIno.ID, "index.js", deterministicBytes(uint64(3000+i), 2048)); err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		if i%3 == 0 {
			if err := writeVFSFile(ctx, gfs, libIno.ID, "bundle.js", deterministicBytes(uint64(4000+i), 36<<10)); err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
		}
		lat.add(time.Since(opStart))
		appOps++
	}
	if err := gfs.Flush(ctx); err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}

	var optStats OptimizerStats
	if cachedBridge != nil {
		optStats = mergeOptStats(optStats, cachedBridge.Stats())
	}
	if optBE != nil {
		optStats = mergeOptStats(optStats, optBE.Stats())
	}
	if err := vol.Close(); err != nil {
		return StackMetrics{}, err
	}

	// Phase 2: Two cold sandbox worker views (fresh vfs.Open each) sharing one
	// backend client, simulating npm postinstall / Node module resolution.
	rawBE2, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return StackMetrics{}, err
	}
	latBE2 := NewLatencyBackend(rawBE2, UniformLatencyConfig(cfg.SimulatedRTT))
	instBE2 := NewInstrumentedBackend(latBE2, rec)
	var readBE backend.Backend = instBE2
	var optBE2 *OptimizedBackend
	if optimized {
		optBE2 = NewOptimizedBackend(instBE2, optCfg)
		readBE = optBE2
	}
	// Wrap in non-closing adapter so worker 1 Close() does not close shared readBE.
	sharedBE := &nonClosingBackend{Backend: readBE}

	for worker := range 2 {
		// Repeated prefix listings (pack/meta inventory audit on worker boot).
		if _, err := drainList(ctx, sharedBE, "meta/"); err != nil {
			_ = readBE.Close()
			return StackMetrics{}, err
		}
		if _, err := drainList(ctx, sharedBE, "pack/"); err != nil {
			_ = readBE.Close()
			return StackMetrics{}, err
		}

		rVol, err := vfs.Open(ctx, vfs.Config{
			Backend: sharedBE,
			Cipher:  cipher,
			Cache:   8 << 20, // ample VFS LRU within a worker; cold between workers
		})
		if err != nil {
			_ = readBE.Close()
			return StackMetrics{}, err
		}
		rBridge := NewGoferBridge(rVol, rec)
		var rFS GoferFS = rBridge
		var rCached *CachedGoferBridge
		if optimized {
			rCached = NewCachedGoferBridge(rBridge, optCfg)
			optBE2.OnHeadInvalidation(rCached.InvalidateAll)
			rFS = rCached
		}

		opStart := time.Now()
		if _, err := rFS.Resolve(ctx, "/package.json"); err != nil {
			_ = rVol.Close()
			_ = readBE.Close()
			return StackMetrics{}, err
		}
		lockIno, err := rFS.Resolve(ctx, "/package-lock.json")
		if err != nil {
			_ = rVol.Close()
			_ = readBE.Close()
			return StackMetrics{}, err
		}
		if _, err := readVFSFile(ctx, rFS, lockIno); err != nil {
			_ = rVol.Close()
			_ = readBE.Close()
			return StackMetrics{}, err
		}
		lat.add(time.Since(opStart))
		appOps++

		for i := range pkgs {
			opStart := time.Now()
			in, err := rFS.Resolve(ctx, fmt.Sprintf("/node_modules/pkg-%03d/package.json", i))
			if err != nil {
				_ = rVol.Close()
				_ = readBE.Close()
				return StackMetrics{}, err
			}
			// Node resolver stats package.json again before reading.
			if _, err := rFS.GetAttr(ctx, in.ID); err != nil {
				_ = rVol.Close()
				_ = readBE.Close()
				return StackMetrics{}, err
			}
			if _, err := readVFSFile(ctx, rFS, in); err != nil {
				_ = rVol.Close()
				_ = readBE.Close()
				return StackMetrics{}, err
			}
			if i%3 == 0 {
				binIno, err := rFS.Resolve(ctx, fmt.Sprintf("/node_modules/pkg-%03d/lib/bundle.js", i))
				if err != nil {
					_ = rVol.Close()
					_ = readBE.Close()
					return StackMetrics{}, err
				}
				if _, err := readVFSFile(ctx, rFS, binIno); err != nil {
					_ = rVol.Close()
					_ = readBE.Close()
					return StackMetrics{}, err
				}
			}
			lat.add(time.Since(opStart))
			appOps++
		}

		if rCached != nil {
			optStats = mergeOptStats(optStats, rCached.Stats())
		}
		if err := rVol.Close(); err != nil {
			_ = readBE.Close()
			return StackMetrics{}, err
		}
		_ = worker
	}

	if optBE2 != nil {
		optStats = mergeOptStats(optStats, optBE2.Stats())
	}
	if err := readBE.Close(); err != nil {
		return StackMetrics{}, err
	}

	stackName := "unoptimized_vfs"
	if optimized {
		stackName = "cetofs_optimized_vfs"
	}
	return buildStackMetrics(stackName, appOps, time.Since(start), &lat, rec.Snapshot(), optStats), nil
}

// --- Workload 2: checkout_5000 ---

type checkoutRepos struct {
	nvmeDir      string
	sharedStore  string
	dirCount     int
	filesPerDir  int
	totalFiles   int
	chunkedPaths []string
}

func runCheckout5000(ctx context.Context, dir string, cipher vfs.Cipher, cfg SuiteConfig) (WorkloadReport, checkoutRepos, error) {
	totalFiles := cfg.CheckoutFiles
	dirCount := 50
	if totalFiles < 50 {
		dirCount = 5
	}
	filesPerDir := totalFiles / dirCount
	if filesPerDir < 1 {
		filesPerDir = 1
	}
	actualTotal := dirCount * filesPerDir
	chunkStride := 50
	if actualTotal < 500 {
		chunkStride = 10 // ensures >=10 chunked files in compact unit tests too
	}

	workStore := filepath.Join(dir, "work_store")
	emptyBackup := filepath.Join(dir, "empty_backup")
	repos := checkoutRepos{
		nvmeDir:     filepath.Join(dir, "nvme"),
		sharedStore: workStore,
		dirCount:    dirCount,
		filesPerDir: filesPerDir,
		totalFiles:  actualTotal,
	}
	for d := range dirCount {
		for f := range filesPerDir {
			idx := d*filesPerDir + f
			if idx%chunkStride == 0 {
				repos.chunkedPaths = append(repos.chunkedPaths, fmt.Sprintf("/pkg/mod-%02d/file-%04d.go", d, f))
			}
		}
	}

	nvmeMetrics, err := runCheckoutNVMe(repos.nvmeDir, dirCount, filesPerDir, chunkStride)
	if err != nil {
		return WorkloadReport{}, repos, err
	}
	if err := createEmptyVolume(ctx, workStore, cipher); err != nil {
		return WorkloadReport{}, repos, err
	}
	if err := copyStore(workStore, emptyBackup); err != nil {
		return WorkloadReport{}, repos, err
	}
	unoptMetrics, err := runCheckoutVFS(ctx, workStore, filepath.Join(dir, "unopt_journal"), cipher, cfg, dirCount, filesPerDir, chunkStride, false)
	if err != nil {
		return WorkloadReport{}, repos, err
	}
	if err := os.RemoveAll(workStore); err != nil {
		return WorkloadReport{}, repos, err
	}
	if err := copyStore(emptyBackup, workStore); err != nil {
		return WorkloadReport{}, repos, err
	}
	optMetrics, err := runCheckoutVFS(ctx, workStore, filepath.Join(dir, "opt_journal"), cipher, cfg, dirCount, filesPerDir, chunkStride, true)
	if err != nil {
		return WorkloadReport{}, repos, err
	}

	rep := finalizeWorkload(
		"checkout_5000",
		fmt.Sprintf("%d-file repository checkout (%d dirs x %d files: mkdir/resolve/create/write/getattr/setattr/flush + fenced-Head checkpoints)", actualTotal, dirCount, filesPerDir),
		unoptMetrics.AppOps,
		nvmeMetrics, unoptMetrics, optMetrics,
	)
	return rep, repos, nil
}

func runCheckoutNVMe(root string, dirCount, filesPerDir, chunkStride int) (StackMetrics, error) {
	if err := os.MkdirAll(filepath.Join(root, "pkg"), 0o755); err != nil {
		return StackMetrics{}, err
	}
	var lat latencyRecorder
	var appOps int64
	start := time.Now()
	now := time.Now()

	for d := range dirCount {
		sub := filepath.Join(root, "pkg", fmt.Sprintf("mod-%02d", d))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			return StackMetrics{}, err
		}
		for f := range filesPerDir {
			opStart := time.Now()
			idx := d*filesPerDir + f
			size := 512 + (idx%4)*256
			if idx%chunkStride == 0 {
				size = 24 << 10 // >16 KiB chunked fixture
			}
			p := filepath.Join(sub, fmt.Sprintf("file-%04d.go", f))
			if err := os.WriteFile(p, deterministicBytes(uint64(10000+idx), size), 0o644); err != nil {
				return StackMetrics{}, err
			}
			if _, err := os.Stat(p); err != nil {
				return StackMetrics{}, err
			}
			if err := os.Chtimes(p, now, now); err != nil {
				return StackMetrics{}, err
			}
			lat.add(time.Since(opStart))
			appOps++
		}
	}
	return buildStackMetrics("plain_nvme", appOps, time.Since(start), &lat, BoundarySnapshot{}, OptimizerStats{}), nil
}

func runCheckoutVFS(ctx context.Context, storeDir, journalDir string, cipher vfs.Cipher, cfg SuiteConfig, dirCount, filesPerDir, chunkStride int, optimized bool) (StackMetrics, error) {
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		return StackMetrics{}, err
	}
	rawBE, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return StackMetrics{}, err
	}
	rec := NewAuditRecorder()
	latBE := NewLatencyBackend(rawBE, UniformLatencyConfig(cfg.SimulatedRTT))
	instBE := NewInstrumentedBackend(latBE, rec)

	var volBE backend.Backend = instBE
	var optBE *OptimizedBackend
	optCfg := UnoptimizedConfig()
	if optimized {
		optCfg = DefaultOptimizerConfig()
		optCfg.HeadCacheTTL = 5 * time.Second
		optBE = NewOptimizedBackend(instBE, optCfg)
		volBE = optBE
	}

	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend:         volBE,
		Cipher:          cipher,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: benchJournalCapacity,
		Cache:           16 << 20,
	})
	if err != nil {
		_ = volBE.Close()
		return StackMetrics{}, err
	}
	rec.Reset()

	bridge := NewGoferBridge(vol, rec)
	var gfs GoferFS = bridge
	var cachedBridge *CachedGoferBridge
	if optimized {
		cachedBridge = NewCachedGoferBridge(bridge, optCfg)
		optBE.OnHeadInvalidation(cachedBridge.InvalidateAll)
		gfs = cachedBridge
	}

	var lat latencyRecorder
	var appOps int64
	start := time.Now()
	now := time.Now()
	mode := fs.FileMode(0o644)

	pkgIno, err := gfs.Mkdir(ctx, vfs.RootInode, "pkg", 0o755)
	if err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}

	flushEvery := max(dirCount/4, 1)
	for d := range dirCount {
		modName := fmt.Sprintf("mod-%02d", d)
		if _, err := gfs.Mkdir(ctx, pkgIno.ID, modName, 0o755); err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		dirPath := "/pkg/" + modName
		for f := range filesPerDir {
			opStart := time.Now()
			// Resolve parent directory path before creating each file,
			// mirroring git checkout / FUSE path traversal.
			parentIno, err := gfs.Resolve(ctx, dirPath)
			if err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			idx := d*filesPerDir + f
			size := 512 + (idx%4)*256
			if idx%chunkStride == 0 {
				size = 24 << 10 // >16 KiB -> packed into pack/
			}
			fname := fmt.Sprintf("file-%04d.go", f)
			in, fh, err := gfs.Create(ctx, parentIno.ID, fname, 0o644)
			if err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			data := deterministicBytes(uint64(10000+idx), size)
			if _, err := gfs.Write(ctx, fh, in.ID, data, 0); err != nil {
				_ = fh.Close()
				_ = vol.Close()
				return StackMetrics{}, err
			}
			if err := fh.Close(); err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			if _, err := gfs.GetAttr(ctx, in.ID); err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			if _, err := gfs.SetAttr(ctx, in.ID, vfs.Change{Mode: &mode, Mtime: &now}); err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			lat.add(time.Since(opStart))
			appOps++
		}
		if (d+1)%flushEvery == 0 || d == dirCount-1 {
			if err := gfs.Flush(ctx); err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			// Checkpoint Head version check: unoptimized issues GET head + List("pack/")
			// on each batch; optimized serves Head from cache and invalidates cleanly.
			if _, _, err := volBE.Get(ctx, "head", 64<<10); err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
		}
	}

	var optStats OptimizerStats
	if cachedBridge != nil {
		optStats = mergeOptStats(optStats, cachedBridge.Stats())
	}
	if optBE != nil {
		optStats = mergeOptStats(optStats, optBE.Stats())
	}
	if err := vol.Close(); err != nil {
		return StackMetrics{}, err
	}
	stackName := "unoptimized_vfs"
	if optimized {
		stackName = "cetofs_optimized_vfs"
	}
	return buildStackMetrics(stackName, appOps, time.Since(start), &lat, rec.Snapshot(), optStats), nil
}

// --- Workload 3: git_status ---

func runGitStatus(ctx context.Context, repos checkoutRepos, cipher vfs.Cipher, cfg SuiteConfig) (WorkloadReport, error) {
	nvmeMetrics, err := runGitStatusNVMe(repos)
	if err != nil {
		return WorkloadReport{}, err
	}
	// Both unoptimized and optimized read the EXACT SAME committed store for an
	// exact apple-to-apple comparison over identical shards and pack layouts!
	unoptMetrics, err := runGitStatusVFS(ctx, repos.sharedStore, repos, cipher, cfg, false)
	if err != nil {
		return WorkloadReport{}, err
	}
	optMetrics, err := runGitStatusVFS(ctx, repos.sharedStore, repos, cipher, cfg, true)
	if err != nil {
		return WorkloadReport{}, err
	}
	return finalizeWorkload(
		"git_status",
		fmt.Sprintf("Recursive 2-pass tree walk + stat across %d files + selective BLAKE3 content hashing across %d chunked files", repos.totalFiles, len(repos.chunkedPaths)),
		unoptMetrics.AppOps,
		nvmeMetrics, unoptMetrics, optMetrics,
	), nil
}

func runGitStatusNVMe(repos checkoutRepos) (StackMetrics, error) {
	var lat latencyRecorder
	var appOps int64
	start := time.Now()

	for range 2 {
		pkgDir := filepath.Join(repos.nvmeDir, "pkg")
		dirs, err := os.ReadDir(pkgDir)
		if err != nil {
			return StackMetrics{}, err
		}
		for _, de := range dirs {
			sub := filepath.Join(pkgDir, de.Name())
			entries, err := os.ReadDir(sub)
			if err != nil {
				return StackMetrics{}, err
			}
			for _, fe := range entries {
				opStart := time.Now()
				if _, err := os.Lstat(filepath.Join(sub, fe.Name())); err != nil {
					return StackMetrics{}, err
				}
				lat.add(time.Since(opStart))
				appOps++
			}
		}
	}

	for _, rel := range repos.chunkedPaths {
		opStart := time.Now()
		b, err := os.ReadFile(filepath.Join(repos.nvmeDir, filepath.FromSlash(rel)))
		if err != nil {
			return StackMetrics{}, err
		}
		_ = blake3.Sum(b)
		lat.add(time.Since(opStart))
		appOps++
	}
	return buildStackMetrics("plain_nvme", appOps, time.Since(start), &lat, BoundarySnapshot{}, OptimizerStats{}), nil
}

func runGitStatusVFS(ctx context.Context, storeDir string, repos checkoutRepos, cipher vfs.Cipher, cfg SuiteConfig, optimized bool) (StackMetrics, error) {
	rawBE, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return StackMetrics{}, err
	}
	rec := NewAuditRecorder()
	latBE := NewLatencyBackend(rawBE, UniformLatencyConfig(cfg.SimulatedRTT))
	instBE := NewInstrumentedBackend(latBE, rec)

	var volBE backend.Backend = instBE
	var optBE *OptimizedBackend
	optCfg := UnoptimizedConfig()
	if optimized {
		optCfg = DefaultOptimizerConfig()
		optCfg.HeadCacheTTL = 5 * time.Second
		optBE = NewOptimizedBackend(instBE, optCfg)
		volBE = optBE
	}

	// Ample VFS LRU (16 MiB) avoids pathological intra-directory thrashing while
	// still starting completely cold from S3 on open.
	vol, err := vfs.Open(ctx, vfs.Config{
		Backend: volBE,
		Cipher:  cipher,
		Cache:   16 << 20,
	})
	if err != nil {
		_ = volBE.Close()
		return StackMetrics{}, err
	}

	bridge := NewGoferBridge(vol, rec)
	var gfs GoferFS = bridge
	var cachedBridge *CachedGoferBridge
	if optimized {
		cachedBridge = NewCachedGoferBridge(bridge, optCfg)
		optBE.OnHeadInvalidation(cachedBridge.InvalidateAll)
		gfs = cachedBridge
	}

	var lat latencyRecorder
	var appOps int64
	start := time.Now()

	// Two-pass recursive walk: git index refresh + untracked worktree scan.
	for range 2 {
		pkgIno, err := gfs.Resolve(ctx, "/pkg")
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		dirEnts, _, err := gfs.ReadDir(ctx, pkgIno.ID)
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		for _, de := range dirEnts {
			subIno, err := gfs.Lookup(ctx, pkgIno.ID, de.Name)
			if err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			fileEnts, _, err := gfs.ReadDir(ctx, subIno.ID)
			if err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			for _, fe := range fileEnts {
				opStart := time.Now()
				in, err := gfs.Lookup(ctx, subIno.ID, fe.Name)
				if err != nil {
					_ = vol.Close()
					return StackMetrics{}, err
				}
				if _, err := gfs.GetAttr(ctx, in.ID); err != nil {
					_ = vol.Close()
					return StackMetrics{}, err
				}
				lat.add(time.Since(opStart))
				appOps++
			}
		}
	}

	// Selective content hash check on chunked (>16 KiB) files packed adjacently in pack/.
	for _, rel := range repos.chunkedPaths {
		opStart := time.Now()
		in, err := gfs.Resolve(ctx, rel)
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		data, err := readVFSFile(ctx, gfs, in)
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		_ = blake3.Sum(data)
		lat.add(time.Since(opStart))
		appOps++
	}

	var optStats OptimizerStats
	if cachedBridge != nil {
		optStats = mergeOptStats(optStats, cachedBridge.Stats())
	}
	if optBE != nil {
		optStats = mergeOptStats(optStats, optBE.Stats())
	}
	if err := vol.Close(); err != nil {
		return StackMetrics{}, err
	}
	stackName := "unoptimized_vfs"
	if optimized {
		stackName = "cetofs_optimized_vfs"
	}
	return buildStackMetrics(stackName, appOps, time.Since(start), &lat, rec.Snapshot(), optStats), nil
}

// --- Workload 4: go_build ---

func runGoBuild(ctx context.Context, dir string, cipher vfs.Cipher, cfg SuiteConfig) (WorkloadReport, error) {
	pkgs := cfg.GoPackages
	const filesPerPkg = 8

	// Seed source repository once for Plain NVMe and once for VFS, then clone
	// the exact same seeded store for unopt & opt (identical Head.Seed & packs).
	nvmeDir := filepath.Join(dir, "nvme")
	if err := seedGoWorkspaceNVMe(nvmeDir, pkgs, filesPerPkg); err != nil {
		return WorkloadReport{}, err
	}
	nvmeMetrics, err := runGoBuildNVMe(nvmeDir, pkgs, filesPerPkg)
	if err != nil {
		return WorkloadReport{}, err
	}

	workStore := filepath.Join(dir, "work_store")
	backupStore := filepath.Join(dir, "backup_store")
	if err := seedGoWorkspaceVFS(ctx, workStore, filepath.Join(dir, "seed_j"), cipher, pkgs, filesPerPkg); err != nil {
		return WorkloadReport{}, err
	}
	if err := copyStore(workStore, backupStore); err != nil {
		return WorkloadReport{}, err
	}
	unoptMetrics, err := runGoBuildVFS(ctx, workStore, filepath.Join(dir, "build_unopt_j"), cipher, cfg, pkgs, filesPerPkg, false)
	if err != nil {
		return WorkloadReport{}, err
	}

	if err := os.RemoveAll(workStore); err != nil {
		return WorkloadReport{}, err
	}
	if err := copyStore(backupStore, workStore); err != nil {
		return WorkloadReport{}, err
	}
	optMetrics, err := runGoBuildVFS(ctx, workStore, filepath.Join(dir, "build_opt_j"), cipher, cfg, pkgs, filesPerPkg, true)
	if err != nil {
		return WorkloadReport{}, err
	}

	return finalizeWorkload(
		"go_build",
		fmt.Sprintf("Multi-package Go build (%d packages x %d files: repeated go.mod/header/import lookups, chunked source reads, .a object writes & final binary link)", pkgs, filesPerPkg),
		unoptMetrics.AppOps,
		nvmeMetrics, unoptMetrics, optMetrics,
	), nil
}

func seedGoWorkspaceNVMe(root string, pkgs, filesPerPkg int) error {
	if err := os.MkdirAll(filepath.Join(root, "internal"), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "go.mod"), deterministicBytes(5001, 1024), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "go.sum"), deterministicBytes(5002, 48<<10), 0o644); err != nil {
		return err
	}
	for p := range pkgs {
		pdir := filepath.Join(root, "internal", fmt.Sprintf("pkg%02d", p))
		if err := os.MkdirAll(pdir, 0o755); err != nil {
			return err
		}
		for f := range filesPerPkg {
			size := 3072
			if f == 0 {
				size = 32 << 10 // generated table file (>16 KiB -> pack/)
			}
			if err := os.WriteFile(filepath.Join(pdir, fmt.Sprintf("src_%02d.go", f)), deterministicBytes(uint64(6000+p*100+f), size), 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

func seedGoWorkspaceVFS(ctx context.Context, storeDir, journalDir string, cipher vfs.Cipher, pkgs, filesPerPkg int) error {
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		return err
	}
	be, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return err
	}
	if err := vfs.Create(ctx, vfs.Config{Backend: be, Cipher: cipher}); err != nil {
		_ = be.Close()
		return err
	}
	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend:         be,
		Cipher:          cipher,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: benchJournalCapacity,
		Cache:           16 << 20,
	})
	if err != nil {
		_ = be.Close()
		return err
	}
	gfs := NewGoferBridge(vol, NewAuditRecorder())
	if err := writeVFSFile(ctx, gfs, vfs.RootInode, "go.mod", deterministicBytes(5001, 1024)); err != nil {
		_ = vol.Close()
		return err
	}
	if err := writeVFSFile(ctx, gfs, vfs.RootInode, "go.sum", deterministicBytes(5002, 48<<10)); err != nil {
		_ = vol.Close()
		return err
	}
	intIno, err := gfs.Mkdir(ctx, vfs.RootInode, "internal", 0o755)
	if err != nil {
		_ = vol.Close()
		return err
	}
	for p := range pkgs {
		pIno, err := gfs.Mkdir(ctx, intIno.ID, fmt.Sprintf("pkg%02d", p), 0o755)
		if err != nil {
			_ = vol.Close()
			return err
		}
		for f := range filesPerPkg {
			size := 3072
			if f == 0 {
				size = 32 << 10
			}
			if err := writeVFSFile(ctx, gfs, pIno.ID, fmt.Sprintf("src_%02d.go", f), deterministicBytes(uint64(6000+p*100+f), size)); err != nil {
				_ = vol.Close()
				return err
			}
		}
	}
	if err := vol.Flush(ctx); err != nil {
		_ = vol.Close()
		return err
	}
	return vol.Close()
}

func runGoBuildNVMe(root string, pkgs, filesPerPkg int) (StackMetrics, error) {
	objDir := filepath.Join(root, "build", "obj")
	if err := os.MkdirAll(objDir, 0o755); err != nil {
		return StackMetrics{}, err
	}
	var lat latencyRecorder
	var appOps int64
	start := time.Now()

	for p := range pkgs {
		opStart := time.Now()
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
			return StackMetrics{}, err
		}
		if _, err := os.ReadFile(filepath.Join(root, "go.mod")); err != nil {
			return StackMetrics{}, err
		}
		if _, err := os.Stat(filepath.Join(root, "go.sum")); err != nil {
			return StackMetrics{}, err
		}
		if _, err := os.Stat(filepath.Join(root, "internal", "pkg00", "src_00.go")); err != nil {
			return StackMetrics{}, err
		}
		lat.add(time.Since(opStart))
		appOps++

		pdir := filepath.Join(root, "internal", fmt.Sprintf("pkg%02d", p))
		if _, err := os.ReadDir(pdir); err != nil {
			return StackMetrics{}, err
		}
		for f := range filesPerPkg {
			opStart := time.Now()
			b, err := os.ReadFile(filepath.Join(pdir, fmt.Sprintf("src_%02d.go", f)))
			if err != nil {
				return StackMetrics{}, err
			}
			_ = blake3.Sum(b)
			lat.add(time.Since(opStart))
			appOps++
		}
		opStart = time.Now()
		if err := os.WriteFile(filepath.Join(objDir, fmt.Sprintf("pkg%02d.a", p)), deterministicBytes(uint64(8000+p), 24<<10), 0o644); err != nil {
			return StackMetrics{}, err
		}
		lat.add(time.Since(opStart))
		appOps++
	}

	opStart := time.Now()
	for p := range pkgs {
		if _, err := os.ReadFile(filepath.Join(objDir, fmt.Sprintf("pkg%02d.a", p))); err != nil {
			return StackMetrics{}, err
		}
	}
	if err := os.WriteFile(filepath.Join(root, "build", "app"), deterministicBytes(9999, 256<<10), 0o755); err != nil {
		return StackMetrics{}, err
	}
	lat.add(time.Since(opStart))
	appOps++
	return buildStackMetrics("plain_nvme", appOps, time.Since(start), &lat, BoundarySnapshot{}, OptimizerStats{}), nil
}

func runGoBuildVFS(ctx context.Context, storeDir, journalDir string, cipher vfs.Cipher, cfg SuiteConfig, pkgs, filesPerPkg int, optimized bool) (StackMetrics, error) {
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		return StackMetrics{}, err
	}
	rawBE, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return StackMetrics{}, err
	}
	rec := NewAuditRecorder()
	latBE := NewLatencyBackend(rawBE, UniformLatencyConfig(cfg.SimulatedRTT))
	instBE := NewInstrumentedBackend(latBE, rec)

	var volBE backend.Backend = instBE
	var optBE *OptimizedBackend
	optCfg := UnoptimizedConfig()
	if optimized {
		optCfg = DefaultOptimizerConfig()
		optCfg.HeadCacheTTL = 5 * time.Second
		optBE = NewOptimizedBackend(instBE, optCfg)
		volBE = optBE
	}

	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend:         volBE,
		Cipher:          cipher,
		Round:           2,
		Journal:         journalDir,
		JournalCapacity: benchJournalCapacity,
		Cache:           16 << 20, // ample VFS LRU; cold source pack/shards cross backend once
	})
	if err != nil {
		_ = volBE.Close()
		return StackMetrics{}, err
	}
	rec.Reset()

	bridge := NewGoferBridge(vol, rec)
	var gfs GoferFS = bridge
	var cachedBridge *CachedGoferBridge
	if optimized {
		cachedBridge = NewCachedGoferBridge(bridge, optCfg)
		optBE.OnHeadInvalidation(cachedBridge.InvalidateAll)
		gfs = cachedBridge
	}

	var lat latencyRecorder
	var appOps int64
	start := time.Now()

	buildIno, err := gfs.Mkdir(ctx, vfs.RootInode, "build", 0o755)
	if err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}
	objIno, err := gfs.Mkdir(ctx, buildIno.ID, "obj", 0o755)
	if err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}

	for p := range pkgs {
		opStart := time.Now()
		modIno, err := gfs.Resolve(ctx, "/go.mod")
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		if _, err := readVFSFile(ctx, gfs, modIno); err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		if _, err := gfs.Resolve(ctx, "/go.sum"); err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		if _, err := gfs.Resolve(ctx, "/internal/pkg00/src_00.go"); err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		lat.add(time.Since(opStart))
		appOps++

		pkgPath := fmt.Sprintf("/internal/pkg%02d", p)
		pDirIno, err := gfs.Resolve(ctx, pkgPath)
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		ents, _, err := gfs.ReadDir(ctx, pDirIno.ID)
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		for _, fe := range ents {
			opStart := time.Now()
			in, err := gfs.Lookup(ctx, pDirIno.ID, fe.Name)
			if err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			b, err := readVFSFile(ctx, gfs, in)
			if err != nil {
				_ = vol.Close()
				return StackMetrics{}, err
			}
			_ = blake3.Sum(b)
			lat.add(time.Since(opStart))
			appOps++
		}

		opStart = time.Now()
		if err := writeVFSFile(ctx, gfs, objIno.ID, fmt.Sprintf("pkg%02d.a", p), deterministicBytes(uint64(8000+p), 24<<10)); err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		lat.add(time.Since(opStart))
		appOps++
	}

	// Flush compiled objects before link.
	if err := gfs.Flush(ctx); err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}

	// Link phase: resolve and read every compiled .a archive, emit final binary, flush.
	opStart := time.Now()
	for p := range pkgs {
		aIno, err := gfs.Resolve(ctx, fmt.Sprintf("/build/obj/pkg%02d.a", p))
		if err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
		if _, err := readVFSFile(ctx, gfs, aIno); err != nil {
			_ = vol.Close()
			return StackMetrics{}, err
		}
	}
	if err := writeVFSFile(ctx, gfs, buildIno.ID, "app", deterministicBytes(9999, 256<<10)); err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}
	if err := gfs.Flush(ctx); err != nil {
		_ = vol.Close()
		return StackMetrics{}, err
	}
	lat.add(time.Since(opStart))
	appOps++

	var optStats OptimizerStats
	if cachedBridge != nil {
		optStats = mergeOptStats(optStats, cachedBridge.Stats())
	}
	if optBE != nil {
		optStats = mergeOptStats(optStats, optBE.Stats())
	}
	if err := vol.Close(); err != nil {
		return StackMetrics{}, err
	}
	stackName := "unoptimized_vfs"
	if optimized {
		stackName = "cetofs_optimized_vfs"
	}
	return buildStackMetrics(stackName, appOps, time.Since(start), &lat, rec.Snapshot(), optStats), nil
}

// --- Negative Regime: Multi-Writer CAS Contention + TTL Staleness + Random Pack Reads ---

func runNegativeRegime(ctx context.Context, dir string, cipher vfs.Cipher, cfg SuiteConfig) (NegativeRegimeReport, error) {
	const casIters = 20
	var rep NegativeRegimeReport
	rep.CASIterations = casIters
	rep.CASAuthorityPreserved = true

	// Sub-regime A: Multi-writer Head CAS contention.
	runCAS := func(enableHeadCache bool) (int64, int64, float64, error) {
		sub := "uncached"
		if enableHeadCache {
			sub = "cached"
		}
		raw, err := openFileBackend(ctx, filepath.Join(dir, "cas_"+sub))
		if err != nil {
			return 0, 0, 0, err
		}
		defer raw.Close()

		if _, err := raw.PutIfAbsent(ctx, "head", []byte("head-init-0")); err != nil {
			return 0, 0, 0, err
		}

		rec := NewAuditRecorder()
		latBE := NewLatencyBackend(raw, UniformLatencyConfig(cfg.SimulatedRTT))
		inst := NewInstrumentedBackend(latBE, rec)
		optCfg := UnoptimizedConfig()
		optCfg.EnableHeadVersionCache = enableHeadCache
		opt := NewOptimizedBackend(inst, optCfg)

		if _, _, err := opt.Get(ctx, "head", 64<<10); err != nil {
			return 0, 0, 0, err
		}
		rec.Reset()

		start := time.Now()
		for i := range casIters {
			// External Writer B advances head directly on the authoritative store.
			_, curVer, err := raw.Get(ctx, "head", 64<<10)
			if err != nil {
				return 0, 0, 0, err
			}
			if _, err := raw.PutIfVersion(ctx, "head", []byte(fmt.Sprintf("writerB-%d", i)), curVer); err != nil {
				return 0, 0, 0, err
			}

			// Writer A attempts ConditionalPutHead.
			payload := []byte(fmt.Sprintf("writerA-%d", i))
			if _, err := opt.ConditionalPutHead(ctx, 64<<10, payload, nil); err != nil {
				return 0, 0, 0, err
			}
		}
		wallMs := float64(time.Since(start)) / float64(time.Millisecond)
		return rec.Snapshot().BackendBoundaryRequests, opt.Stats().HeadCASConflicts, wallMs, nil
	}

	uncachedReqs, _, uncachedMs, err := runCAS(false)
	if err != nil {
		return rep, err
	}
	cachedReqs, conflicts, cachedMs, err := runCAS(true)
	if err != nil {
		return rep, err
	}
	rep.UncachedCASBackendReqs = uncachedReqs
	rep.CachedContendedBackendReqs = cachedReqs
	rep.CachedContendedConflicts = conflicts
	if uncachedReqs > 0 {
		rep.CASRequestOverheadPct = float64(cachedReqs-uncachedReqs) / float64(uncachedReqs) * 100.0
	}
	rep.UncachedCASWallMs = uncachedMs
	rep.CachedContendedWallMs = cachedMs
	if uncachedMs > 0 {
		rep.CASWallOverheadPct = (cachedMs - uncachedMs) / uncachedMs * 100.0
	}
	if conflicts != casIters {
		rep.CASAuthorityPreserved = false
	}

	// Sub-regime B: External mutation under AttrTimeout -> stale read until
	// Head-generation invalidation fires.
	rawMeta, err := openFileBackend(ctx, filepath.Join(dir, "meta_ttl"))
	if err != nil {
		return rep, err
	}
	jDir := filepath.Join(dir, "meta_ttl_j")
	if err := os.MkdirAll(jDir, 0o755); err != nil {
		_ = rawMeta.Close()
		return rep, err
	}
	if err := vfs.Create(ctx, vfs.Config{Backend: rawMeta, Cipher: cipher}); err != nil {
		_ = rawMeta.Close()
		return rep, err
	}
	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend:         rawMeta,
		Cipher:          cipher,
		Round:           1,
		Journal:         jDir,
		JournalCapacity: benchJournalCapacity,
	})
	if err != nil {
		_ = rawMeta.Close()
		return rep, err
	}
	recMeta := NewAuditRecorder()
	bridge := NewGoferBridge(vol, recMeta)
	cached := NewCachedGoferBridge(bridge, DefaultOptimizerConfig())

	in, fh, err := cached.Create(ctx, vfs.RootInode, "shared.txt", 0o644)
	if err != nil {
		_ = vol.Close()
		return rep, err
	}
	if _, err := cached.Write(ctx, fh, in.ID, []byte("v1"), 0); err != nil {
		_ = fh.Close()
		_ = vol.Close()
		return rep, err
	}
	if _, err := cached.GetAttr(ctx, in.ID); err != nil {
		_ = fh.Close()
		_ = vol.Close()
		return rep, err
	}
	// External mutation bypassing cached bridge directly on *vfs.Volume.
	if _, err := fh.WriteAt(ctx, []byte("v2-longer-content"), 0); err != nil {
		_ = fh.Close()
		_ = vol.Close()
		return rep, err
	}
	_ = fh.Close()

	staleCount := 0
	for range 3 {
		got, err := cached.GetAttr(ctx, in.ID)
		if err != nil {
			_ = vol.Close()
			return rep, err
		}
		if got.Size == 2 {
			staleCount++
		}
	}
	rep.StaleReadsBeforeInvalidation = staleCount

	cached.InvalidateAll()
	fresh, err := cached.GetAttr(ctx, in.ID)
	if err != nil {
		_ = vol.Close()
		return rep, err
	}
	rep.FreshAfterInvalidation = (fresh.Size == 17)
	_ = vol.Close()

	// Sub-regime C: 40 random non-adjacent 4 KiB reads across a 16 MiB pack.
	const randOps = 40
	const packSize = 16 << 20
	const readSize = 4 << 10
	rep.RandomReadOps = randOps
	packData := deterministicBytes(7777, packSize)

	offsets := make([]int64, randOps)
	for i := range randOps {
		slot := (i*17 + 3) % randOps
		offsets[i] = int64(slot) * (400 << 10)
	}

	nvmePack := filepath.Join(dir, "nvme_pack.bin")
	if err := os.WriteFile(nvmePack, packData, 0o644); err != nil {
		return rep, err
	}
	fNVMe, err := os.Open(nvmePack)
	if err != nil {
		return rep, err
	}
	nvmeStart := time.Now()
	tmpBuf := make([]byte, readSize)
	for _, off := range offsets {
		if _, err := fNVMe.ReadAt(tmpBuf, off); err != nil {
			_ = fNVMe.Close()
			return rep, err
		}
	}
	rep.PlainNVMeRandomReadMs = float64(time.Since(nvmeStart)) / float64(time.Millisecond)
	_ = fNVMe.Close()

	rawPack, err := openFileBackend(ctx, filepath.Join(dir, "pack_rand"))
	if err != nil {
		return rep, err
	}
	defer rawPack.Close()
	packKey := "pack/ab/0123456789abcdef0123456789abcdef"
	if err := rawPack.Put(ctx, packKey, packData); err != nil {
		return rep, err
	}

	runRandomMode := func(cfgOpt OptimizerConfig) (bytesRead, reqs int64, err error) {
		r := NewAuditRecorder()
		inst := NewInstrumentedBackend(rawPack, r)
		var be backend.Backend = inst
		if cfgOpt.EnableRangeCoalescing {
			be = NewOptimizedBackend(inst, cfgOpt)
		}
		for _, off := range offsets {
			b, err := be.GetRange(ctx, packKey, off, readSize)
			if err != nil {
				return 0, 0, err
			}
			if len(b) != readSize {
				return 0, 0, fmt.Errorf("short range read: %d", len(b))
			}
		}
		snap := r.Snapshot()
		return snap.BackendBytesRead, snap.BackendBoundaryRequests, nil
	}

	unoptBytes, unoptReqs, err := runRandomMode(UnoptimizedConfig())
	if err != nil {
		return rep, err
	}
	aggCfg := DefaultOptimizerConfig()
	aggCfg.AlwaysReadAhead = true
	aggCfg.ReadAheadWindow = 512 << 10
	aggBytes, aggReqs, err := runRandomMode(aggCfg)
	if err != nil {
		return rep, err
	}
	adaptCfg := DefaultOptimizerConfig()
	adaptCfg.AlwaysReadAhead = false
	adaptCfg.EnableSequentialReadAhead = true
	adaptBytes, adaptReqs, err := runRandomMode(adaptCfg)
	if err != nil {
		return rep, err
	}

	rep.UnoptimizedRandomBytesRead = unoptBytes
	rep.UnoptimizedRandomBackendReqs = unoptReqs
	rep.AggressiveCoalesceBytesRead = aggBytes
	rep.AggressiveRandomBackendReqs = aggReqs
	rep.AdaptiveCoalesceBytesRead = adaptBytes
	rep.AdaptiveRandomBackendReqs = adaptReqs
	if unoptBytes > 0 {
		rep.AggressiveReadAmplificationX = float64(aggBytes) / float64(unoptBytes)
		rep.AdaptiveReadAmplificationX = float64(adaptBytes) / float64(unoptBytes)
	}
	return rep, nil
}

// nonClosingBackend wraps a backend.Backend so closing one vfs.Open handle
// leaves the shared backend client open for subsequent worker views.
type nonClosingBackend struct {
	backend.Backend
}

func (n *nonClosingBackend) Close() error { return nil }

// --- VFS file I/O helpers ---

func writeVFSFile(ctx context.Context, gfs GoferFS, dir vfs.InodeID, name string, data []byte) error {
	in, f, err := gfs.Create(ctx, dir, name, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = gfs.Write(ctx, f, in.ID, data, 0)
	return err
}

func readVFSFile(ctx context.Context, gfs GoferFS, in vfs.Inode) ([]byte, error) {
	f, err := gfs.Open(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, in.Size)
	if in.Size == 0 {
		return buf, nil
	}
	var off int64
	for off < int64(in.Size) {
		n, err := gfs.Read(ctx, f, in.ID, buf[off:], off)
		off += int64(n)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
	}
	return buf[:off], nil
}
