// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cofs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

// ProbeBackend wraps a backend.Backend to count GET requests and optionally
// inject per-GET object-store network RTT during cold-clone startup tests.
type ProbeBackend struct {
	backend.Backend
	getDelay time.Duration
	gets     atomic.Int64
	metaGets atomic.Int64
	extGets  atomic.Int64
}

// NewProbeBackend wraps be with optional cold-GET latency injection.
func NewProbeBackend(be backend.Backend, getDelay time.Duration) *ProbeBackend {
	return &ProbeBackend{Backend: be, getDelay: getDelay}
}

func (p *ProbeBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if p.getDelay > 0 {
		time.Sleep(p.getDelay)
	}
	p.gets.Add(1)
	if len(key) >= 5 && key[:5] == "meta/" {
		p.metaGets.Add(1)
	} else if len(key) >= 4 && key[:4] == "ext/" {
		p.extGets.Add(1)
	}
	return p.Backend.Get(ctx, key, max)
}

// Reset clears request counters and sets getDelay.
func (p *ProbeBackend) Reset(delay time.Duration) {
	p.getDelay = delay
	p.gets.Store(0)
	p.metaGets.Store(0)
	p.extGets.Store(0)
}

// Counts returns total GETs, meta/ shard GETs, and ext/ GETs.
func (p *ProbeBackend) Counts() (total, meta, ext int64) {
	return p.gets.Load(), p.metaGets.Load(), p.extGets.Load()
}

// ColdCloneResult captures cold-start latency and S3 GET counts when cloning
// an immutable base image and running a startup lookup burst.
type ColdCloneResult struct {
	BaseFiles            int     `json:"base_files"`
	StartupPositivePaths int     `json:"startup_positive_paths"`
	StartupENOENTPaths   int     `json:"startup_enoent_paths"`
	IndexBytes           int     `json:"index_bytes"`
	BytesPerFile         float64 `json:"bytes_per_file"`
	BuildMs              float64 `json:"index_build_ms"`
	SimulatedS3RTTMs     float64 `json:"simulated_s3_rtt_ms"`
	PlainNVMeColdMs      float64 `json:"plain_nvme_cold_ms"`
	UnindexedColdMs      float64 `json:"unindexed_cold_ms"`
	UnindexedTotalGets   int64   `json:"unindexed_total_gets"`
	UnindexedMetaGets    int64   `json:"unindexed_meta_gets"`
	CoFSColdMs           float64 `json:"cofs_cold_ms"`
	CoFSTotalGets        int64   `json:"cofs_total_gets"`
	CoFSMetaGets         int64   `json:"cofs_meta_gets"`
	ColdStartSpeedupPct  float64 `json:"cold_start_speedup_pct"`
}

// LookupLatencyResult compares warm positive Resolve, warm Lookup, and ENOENT
// lookup latency across Plain NVMe, unindexed vfs.Volume, and CoFS Index.
type LookupLatencyResult struct {
	Iterations            int     `json:"iterations"`
	PlainNVMeResolveNs    float64 `json:"plain_nvme_resolve_ns"`
	UnindexedResolveNs    float64 `json:"unindexed_resolve_ns"`
	CoFSResolveNs         float64 `json:"cofs_resolve_ns"`
	ResolveFasterPctVsVFS float64 `json:"resolve_faster_pct_vs_vfs"`
	PlainNVMeENOENTNs     float64 `json:"plain_nvme_enoent_ns"`
	UnindexedENOENTNs     float64 `json:"unindexed_enoent_ns"`
	CoFSENOENTNs          float64 `json:"cofs_enoent_ns"`
	ENOENTFasterPctVsVFS  float64 `json:"enoent_faster_pct_vs_vfs"`
	PlainNVMeComponentNs  float64 `json:"plain_nvme_component_ns"`
	UnindexedComponentNs  float64 `json:"unindexed_component_ns"`
	CoFSComponentNs       float64 `json:"cofs_component_ns"`
	ComponentFasterPctVFS float64 `json:"component_faster_pct_vs_vfs"`
}

// WorkspaceBoundaryResult captures the negative control where 100% of lookups
// target newly mutated files in the upper mutable workspace.
type WorkspaceBoundaryResult struct {
	MutatedFiles       int     `json:"mutated_files"`
	DirectWorkspaceNs  float64 `json:"direct_workspace_ns"`
	LayeredWorkspaceNs float64 `json:"layered_workspace_ns"`
	OverheadNs         float64 `json:"overhead_ns"`
}

// SuiteReport bundles all CoFS benchmark metrics for CLI and JSON reporting.
type SuiteReport struct {
	Scale             string                  `json:"scale"`
	ColdClone         ColdCloneResult         `json:"cold_clone"`
	WarmLookups       LookupLatencyResult     `json:"warm_lookups"`
	WorkspaceBoundary WorkspaceBoundaryResult `json:"workspace_boundary"`
}

// RunSuite executes the full CoFS comparative benchmark suite against Plain
// NVMe and unindexed vfs.Volume.
func RunSuite(ctx context.Context, scratchRoot string, fullScale bool) (SuiteReport, error) {
	if scratchRoot == "" {
		var err error
		scratchRoot, err = os.MkdirTemp("", "cofs-bench-*")
		if err != nil {
			return SuiteReport{}, err
		}
		defer os.RemoveAll(scratchRoot)
	}

	toolchains, pkgsPerTC, filesPerPkg := 5, 16, 25 // 2,000 files at depth 6
	iters := 10000
	s3RTT := 2 * time.Millisecond
	scaleName := "standard"
	if fullScale {
		toolchains, pkgsPerTC, filesPerPkg = 8, 40, 25 // 8,000 files at depth 6
		iters = 25000
		scaleName = "full"
	}

	nvmeRoot := filepath.Join(scratchRoot, "nvme-base")
	storeDir := filepath.Join(scratchRoot, "vfs-store")
	_ = os.MkdirAll(nvmeRoot, 0o755)
	_ = os.MkdirAll(storeDir, 0o755)

	rawBE, err := backend.Open(ctx, "file://"+filepath.ToSlash(storeDir))
	if err != nil {
		return SuiteReport{}, err
	}
	probe := NewProbeBackend(rawBE, 0)
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		return SuiteReport{}, err
	}
	ciph := vfs.Hybrid(key)
	if err := vfs.Create(ctx, vfs.Config{Backend: probe, Cipher: ciph}); err != nil {
		return SuiteReport{}, err
	}
	writer, err := vfs.Attach(ctx, vfs.Config{Backend: probe, Cipher: ciph, Round: 1})
	if err != nil {
		return SuiteReport{}, err
	}

	// Build realistic depth-6 base image hierarchy (/usr/lib/tc-XX/sub/pkg-XXXX/file-XX.go)
	// identically on Plain NVMe and in vfs.Volume.
	usrV, _ := writer.Mkdir(ctx, vfs.RootInode, "usr", 0o755)
	libV, _ := writer.Mkdir(ctx, usrV.ID, "lib", 0o755)
	payload := []byte("package runtime // immutable base image toolchain symbol\n")

	var allPaths []string
	var sampleDir vfs.InodeID
	var sampleNames []string

	for tc := range toolchains {
		tcName := fmt.Sprintf("tc-%02d", tc)
		tcV, _ := writer.Mkdir(ctx, libV.ID, tcName, 0o755)
		subV, _ := writer.Mkdir(ctx, tcV.ID, "packages", 0o755)
		for p := range pkgsPerTC {
			pName := fmt.Sprintf("pkg-%04d", p)
			pV, _ := writer.Mkdir(ctx, subV.ID, pName, 0o755)
			nvmePkg := filepath.Join(nvmeRoot, "usr", "lib", tcName, "packages", pName)
			_ = os.MkdirAll(nvmePkg, 0o755)
			if tc == 0 && p == 0 {
				sampleDir = pV.ID
			}
			for f := range filesPerPkg {
				fName := fmt.Sprintf("mod-%03d.go", f)
				fIn, _ := writer.Create(ctx, pV.ID, fName, 0o644)
				fh, _ := writer.Open(ctx, fIn.ID)
				_, _ = fh.WriteAt(ctx, payload, 0)
				_ = fh.Close()
				_ = os.WriteFile(filepath.Join(nvmePkg, fName), payload, 0o644)
				allPaths = append(allPaths, fmt.Sprintf("/usr/lib/%s/packages/%s/%s", tcName, pName, fName))
				if tc == 0 && p == 0 {
					sampleNames = append(sampleNames, fName)
				}
			}
		}
	}
	if err := writer.Flush(ctx); err != nil {
		return SuiteReport{}, err
	}

	// Build and persist sealed CoFS index into ext/cofs/<tag>.
	snapKey := []byte("base-image-v1")
	tBuild := time.Now()
	builtIdx, err := Build(ctx, writer, snapKey)
	if err != nil {
		return SuiteReport{}, err
	}
	rawIdx, err := builtIdx.MarshalBinary()
	if err != nil {
		return SuiteReport{}, err
	}
	if err := writer.PutExt(ctx, ExtName, snapKey, rawIdx); err != nil {
		return SuiteReport{}, err
	}
	buildMs := float64(time.Since(tBuild).Microseconds()) / 1000.0

	// Prepare startup lookup burst: 250 spread positive paths + 250 ENOENT PATH misses.
	const burstN = 250
	posBurst := make([]string, burstN)
	missBurst := make([]string, burstN)
	for i := range burstN {
		posBurst[i] = allPaths[(i*97)%len(allPaths)]
		missBurst[i] = fmt.Sprintf("/usr/lib/tc-%02d/packages/pkg-%04d/missing-%04d.so",
			i%toolchains, (i*13)%pkgsPerTC, i)
	}

	// 1A. Plain NVMe startup burst.
	tNVMeCold := time.Now()
	for i := range burstN {
		_, _ = os.Stat(filepath.Join(nvmeRoot, filepath.FromSlash(posBurst[i])))
		_, _ = os.Stat(filepath.Join(nvmeRoot, filepath.FromSlash(missBurst[i])))
	}
	nvmeColdMs := float64(time.Since(tNVMeCold).Microseconds()) / 1000.0

	// 1B. Cold cloned Volume WITHOUT CoFS index (tiny shard cache / cold open over S3 RTT).
	probe.Reset(s3RTT)
	tUncold := time.Now()
	roPlain, err := vfs.Open(ctx, vfs.Config{Backend: probe, Cipher: ciph, Cache: 256 << 20})
	if err != nil {
		return SuiteReport{}, err
	}
	for i := range burstN {
		if _, err := roPlain.Resolve(ctx, posBurst[i]); err != nil {
			return SuiteReport{}, err
		}
		_, _ = roPlain.Resolve(ctx, missBurst[i])
	}
	unindexedColdMs := float64(time.Since(tUncold).Microseconds()) / 1000.0
	uTotalGets, uMetaGets, _ := probe.Counts()

	// 1C. Cold cloned Volume WITH CoFS index (1 head GET + 1 ext/cofs GET, 0 meta/ shard GETs).
	probe.Reset(s3RTT)
	tCoFSCold := time.Now()
	roCoFS, err := vfs.Open(ctx, vfs.Config{Backend: probe, Cipher: ciph, Cache: 256 << 20})
	if err != nil {
		return SuiteReport{}, err
	}
	loadedIdx, err := LoadFromVolume(ctx, roCoFS, snapKey)
	if err != nil {
		return SuiteReport{}, err
	}
	for i := range burstN {
		if _, err := loadedIdx.Resolve(posBurst[i]); err != nil {
			return SuiteReport{}, err
		}
		_, _ = loadedIdx.Resolve(missBurst[i])
	}
	cofsColdMs := float64(time.Since(tCoFSCold).Microseconds()) / 1000.0
	cTotalGets, cMetaGets, _ := probe.Counts()

	coldSpeedupPct := 0.0
	if unindexedColdMs > 0 {
		coldSpeedupPct = (1.0 - cofsColdMs/unindexedColdMs) * 100.0
	}

	// 2. Warm in-memory lookup latency (zero S3 network delay, pure CPU + cache path).
	probe.Reset(0)
	// Warm roPlain shard cache completely over posBurst + missBurst first so warm comparison is fair.
	for i := range burstN {
		_, _ = roPlain.Resolve(ctx, posBurst[i])
		_, _ = roPlain.Resolve(ctx, missBurst[i])
	}
	for _, nm := range sampleNames {
		_, _ = roPlain.Lookup(ctx, sampleDir, nm)
	}

	nvmeIter := min(iters, 4000)
	t0 := time.Now()
	for i := range nvmeIter {
		_, _ = os.Stat(filepath.Join(nvmeRoot, filepath.FromSlash(posBurst[i%burstN])))
	}
	nvmeResolveNs := float64(time.Since(t0).Nanoseconds()) / float64(nvmeIter)

	t0 = time.Now()
	for i := range iters {
		if _, err := roPlain.Resolve(ctx, posBurst[i%burstN]); err != nil {
			return SuiteReport{}, err
		}
	}
	vfsResolveNs := float64(time.Since(t0).Nanoseconds()) / float64(iters)

	t0 = time.Now()
	for i := range iters {
		if _, err := loadedIdx.Resolve(posBurst[i%burstN]); err != nil {
			return SuiteReport{}, err
		}
	}
	cofsResolveNs := float64(time.Since(t0).Nanoseconds()) / float64(iters)

	// ENOENT misses (warm).
	t0 = time.Now()
	for i := range nvmeIter {
		_, _ = os.Stat(filepath.Join(nvmeRoot, filepath.FromSlash(missBurst[i%burstN])))
	}
	nvmeMissNs := float64(time.Since(t0).Nanoseconds()) / float64(nvmeIter)

	t0 = time.Now()
	for i := range iters {
		_, _ = roPlain.Resolve(ctx, missBurst[i%burstN])
	}
	vfsMissNs := float64(time.Since(t0).Nanoseconds()) / float64(iters)

	t0 = time.Now()
	for i := range iters {
		_, _ = loadedIdx.Resolve(missBurst[i%burstN])
	}
	cofsMissNs := float64(time.Since(t0).Nanoseconds()) / float64(iters)

	// Single-component (dirIno, name) Lookup (FUSE LOOKUP hot path).
	nvmeSampleDir := filepath.Join(nvmeRoot, "usr", "lib", "tc-00", "packages", "pkg-0000")
	t0 = time.Now()
	for i := range nvmeIter {
		_, _ = os.Stat(filepath.Join(nvmeSampleDir, sampleNames[i%len(sampleNames)]))
	}
	nvmeCompNs := float64(time.Since(t0).Nanoseconds()) / float64(nvmeIter)

	t0 = time.Now()
	for i := range iters {
		if _, err := roPlain.Lookup(ctx, sampleDir, sampleNames[i%len(sampleNames)]); err != nil {
			return SuiteReport{}, err
		}
	}
	vfsCompNs := float64(time.Since(t0).Nanoseconds()) / float64(iters)

	t0 = time.Now()
	for i := range iters {
		if _, err := loadedIdx.Lookup(sampleDir, sampleNames[i%len(sampleNames)]); err != nil {
			return SuiteReport{}, err
		}
	}
	cofsCompNs := float64(time.Since(t0).Nanoseconds()) / float64(iters)

	// 3. Negative Control: 100% Mutable Workspace Hits (where CoFS base index cannot help).
	wsDir := filepath.Join(scratchRoot, "ws-store")
	_ = os.MkdirAll(wsDir, 0o755)
	wsBE, err := backend.Open(ctx, "file://"+filepath.ToSlash(wsDir))
	if err != nil {
		return SuiteReport{}, err
	}
	if err := vfs.Create(ctx, vfs.Config{Backend: wsBE, Cipher: ciph}); err != nil {
		return SuiteReport{}, err
	}
	wsVol, err := vfs.Attach(ctx, vfs.Config{Backend: wsBE, Cipher: ciph, Round: 1})
	if err != nil {
		return SuiteReport{}, err
	}
	defer wsVol.Close()

	layered := NewLayeredResolver(loadedIdx, wsVol)
	wsSub, _ := wsVol.Mkdir(ctx, vfs.RootInode, "workspace", 0o755)
	const wsFiles = 64
	wsPaths := make([]string, wsFiles)
	for i := range wsFiles {
		nm := fmt.Sprintf("edit-%03d.go", i)
		_, _ = wsVol.Create(ctx, wsSub.ID, nm, 0o644)
		p := "/workspace/" + nm
		wsPaths[i] = p
		layered.MarkWorkspaceWrite(p)
	}
	_ = wsVol.Flush(ctx)

	wsIter := 5000
	t0 = time.Now()
	for i := range wsIter {
		_, _ = wsVol.Resolve(ctx, wsPaths[i%wsFiles])
	}
	directWSNs := float64(time.Since(t0).Nanoseconds()) / float64(wsIter)

	t0 = time.Now()
	for i := range wsIter {
		_, _, _ = layered.Resolve(ctx, wsPaths[i%wsFiles])
	}
	layeredWSNs := float64(time.Since(t0).Nanoseconds()) / float64(wsIter)

	_ = writer.Close()

	return SuiteReport{
		Scale: scaleName,
		ColdClone: ColdCloneResult{
			BaseFiles:            len(allPaths),
			StartupPositivePaths: burstN,
			StartupENOENTPaths:   burstN,
			IndexBytes:           len(rawIdx),
			BytesPerFile:         float64(len(rawIdx)) / float64(len(allPaths)),
			BuildMs:              buildMs,
			SimulatedS3RTTMs:     float64(s3RTT.Milliseconds()),
			PlainNVMeColdMs:      nvmeColdMs,
			UnindexedColdMs:      unindexedColdMs,
			UnindexedTotalGets:   uTotalGets,
			UnindexedMetaGets:    uMetaGets,
			CoFSColdMs:           cofsColdMs,
			CoFSTotalGets:        cTotalGets,
			CoFSMetaGets:         cMetaGets,
			ColdStartSpeedupPct:  coldSpeedupPct,
		},
		WarmLookups: LookupLatencyResult{
			Iterations:            iters,
			PlainNVMeResolveNs:    nvmeResolveNs,
			UnindexedResolveNs:    vfsResolveNs,
			CoFSResolveNs:         cofsResolveNs,
			ResolveFasterPctVsVFS: (1.0 - cofsResolveNs/vfsResolveNs) * 100.0,
			PlainNVMeENOENTNs:     nvmeMissNs,
			UnindexedENOENTNs:     vfsMissNs,
			CoFSENOENTNs:          cofsMissNs,
			ENOENTFasterPctVsVFS:  (1.0 - cofsMissNs/vfsMissNs) * 100.0,
			PlainNVMeComponentNs:  nvmeCompNs,
			UnindexedComponentNs:  vfsCompNs,
			CoFSComponentNs:       cofsCompNs,
			ComponentFasterPctVFS: (1.0 - cofsCompNs/vfsCompNs) * 100.0,
		},
		WorkspaceBoundary: WorkspaceBoundaryResult{
			MutatedFiles:       wsFiles,
			DirectWorkspaceNs:  directWSNs,
			LayeredWorkspaceNs: layeredWSNs,
			OverheadNs:         layeredWSNs - directWSNs,
		},
	}, nil
}
