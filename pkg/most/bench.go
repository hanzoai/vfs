// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package most

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// ProbeBackend wraps a backend.Backend to count GET/GetRange requests and
// optionally inject an explicitly labeled simulated S3 GET RTT.
type ProbeBackend struct {
	backend.Backend
	getDelay time.Duration
	gets     atomic.Int64
}

// NewProbeBackend wraps be with optional simulated GET delay (0 for unthrottled).
func NewProbeBackend(be backend.Backend, delay time.Duration) *ProbeBackend {
	return &ProbeBackend{Backend: be, getDelay: delay}
}

func (p *ProbeBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if p.getDelay > 0 {
		time.Sleep(p.getDelay)
	}
	p.gets.Add(1)
	return p.Backend.Get(ctx, key, max)
}

func (p *ProbeBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if p.getDelay > 0 {
		time.Sleep(p.getDelay)
	}
	p.gets.Add(1)
	return p.Backend.GetRange(ctx, key, off, n)
}

func (p *ProbeBackend) Gets() int64 { return p.gets.Load() }

func (p *ProbeBackend) Reset(delay time.Duration) {
	p.getDelay = delay
	p.gets.Store(0)
}

// BenchmarkRow captures one reproducible measurement row for JSON/CSV/Markdown.
type BenchmarkRow struct {
	Workload            string  `json:"workload"`
	System              string  `json:"system"`
	InjectedDelayMs     float64 `json:"injected_delay_ms"`
	Agents              int     `json:"agents"`
	Ops                 int64   `json:"ops"`
	BlockBytes          int     `json:"block_bytes"`
	TotalWallMs         float64 `json:"total_wall_ms"`
	ThroughputOpsSec    float64 `json:"throughput_ops_sec"`
	P50Us               float64 `json:"p50_us"`
	P95Us               float64 `json:"p95_us"`
	P99Us               float64 `json:"p99_us"`
	BackendGets         int64   `json:"backend_gets"`
	RAMHits             int64   `json:"ram_hits"`
	NVMeHits            int64   `json:"nvme_hits"`
	MirrorPromotions    int64   `json:"mirror_promotions"`
	TierMigrationWrites int64   `json:"tier_migration_writes"`
	NVMeWrites          int64   `json:"nvme_writes"`
	ResidentRAMBytes    int64   `json:"resident_ram_bytes"`
	ResidentNVMeBytes   int64   `json:"resident_nvme_bytes"`
	HeapAllocDeltaBytes int64   `json:"heap_alloc_delta_bytes"`
	TotalAllocDeltaB    int64   `json:"total_alloc_delta_bytes"`
	NumGCDelta          uint32  `json:"num_gc_delta"`
}

// SuiteReport holds all real unthrottled and labeled-RTT MOST benchmark rows.
type SuiteReport struct {
	Scale  string         `json:"scale"`
	GOOS   string         `json:"goos"`
	GOARCH string         `json:"goarch"`
	NumCPU int            `json:"num_cpu"`
	Rows   []BenchmarkRow `json:"rows"`
}

type benchCipher struct {
	id   string
	aead cipher.AEAD
}

func newBenchCipher(seed string) (Cipher, error) {
	sum := sha256.Sum256([]byte(seed))
	aead, err := chacha20poly1305.NewX(sum[:])
	if err != nil {
		return nil, err
	}
	return &benchCipher{id: hex.EncodeToString(sum[:8]), aead: aead}, nil
}

func (c *benchCipher) Seal(plain []byte) ([]byte, string, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, "", err
	}
	return c.aead.Seal(nonce, nonce, plain, []byte(c.id)), c.id, nil
}

func (c *benchCipher) Open(sealed []byte, keyID string) ([]byte, error) {
	if keyID != c.id {
		return nil, fmt.Errorf("unknown key %s", keyID)
	}
	ns := c.aead.NonceSize()
	if len(sealed) < ns {
		return nil, errors.New("short ciphertext")
	}
	return c.aead.Open(nil, sealed[:ns], sealed[ns:], []byte(c.id))
}

func deterministicBlock(seed uint64, size int) []byte {
	r := mrand.New(mrand.NewPCG(seed, seed*7919+17))
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// RunBenchmarkSuite executes matched comparisons of NoCache, PolicyTierMigration,
// and PolicySelectiveMirror across Zipfian shared toolchains, noisy-neighbor isolation,
// and a 100%-one-hit negative boundary control, at both 0ms unthrottled and 2ms labeled RTT.
func RunBenchmarkSuite(ctx context.Context, scratchDir string, full bool) (SuiteReport, error) {
	if scratchDir == "" {
		var err error
		scratchDir, err = os.MkdirTemp("", "most-bench-*")
		if err != nil {
			return SuiteReport{}, err
		}
		defer os.RemoveAll(scratchDir)
	}

	scale := "standard"
	numHot := 96
	numScan := 320
	blockSize := 8192
	ramCap := int64(24 * blockSize)   // 25% of hot toolchain fits in RAM
	nvmeCap := int64(512 * blockSize) // holds full hot corpus + scans
	agents := 4
	opsPerAgent := 600
	if full {
		scale = "full"
		numHot = 192
		numScan = 640
		blockSize = 16384
		ramCap = int64(48 * blockSize)
		nvmeCap = int64(1024 * blockSize)
		agents = 8
		opsPerAgent = 1000
	}

	var rows []BenchmarkRow

	// 1. Unthrottled (0 ms) Zipfian Shared Toolchain + Cold Scan Pollution.
	for _, sys := range []struct {
		name      string
		useEngine bool
		policy    Policy
	}{
		{name: "no_cache", useEngine: false},
		{name: "tier_migration", useEngine: true, policy: PolicyTierMigration},
		{name: "most_selective_mirror", useEngine: true, policy: PolicySelectiveMirror},
	} {
		row, err := runZipfWorkload(ctx, scratchDir, "zipf_shared_toolchain_unthrottled", sys.name, sys.useEngine, sys.policy,
			0, numHot, numScan, blockSize, ramCap, nvmeCap, agents, opsPerAgent)
		if err != nil {
			return SuiteReport{}, err
		}
		rows = append(rows, row)
	}

	// 2. Same Zipfian workload with explicitly labeled 2ms simulated S3 GET RTT (smaller op count so suite stays fast).
	rttOps := 120
	if full {
		rttOps = 200
	}
	for _, sys := range []struct {
		name      string
		useEngine bool
		policy    Policy
	}{
		{name: "no_cache", useEngine: false},
		{name: "tier_migration", useEngine: true, policy: PolicyTierMigration},
		{name: "most_selective_mirror", useEngine: true, policy: PolicySelectiveMirror},
	} {
		row, err := runZipfWorkload(ctx, scratchDir, "zipf_shared_toolchain_sim2ms", sys.name, sys.useEngine, sys.policy,
			2*time.Millisecond, numHot, numScan, blockSize, ramCap, nvmeCap, agents, rttOps)
		if err != nil {
			return SuiteReport{}, err
		}
		rows = append(rows, row)
	}

	// 3. Multi-tenant noisy-neighbor isolation (Tenant A hot working set vs Tenant B cold flood).
	for _, policy := range []Policy{PolicyTierMigration, PolicySelectiveMirror} {
		row, err := runNoisyNeighborWorkload(ctx, scratchDir, policy, blockSize)
		if err != nil {
			return SuiteReport{}, err
		}
		rows = append(rows, row)
	}

	// 4. Negative / Boundary Control: 100% 1-hit sequential cold scan (0 reuse).
	scanCount := 240
	if full {
		scanCount = 480
	}
	for _, sys := range []struct {
		name      string
		useEngine bool
		policy    Policy
	}{
		{name: "no_cache", useEngine: false},
		{name: "tier_migration", useEngine: true, policy: PolicyTierMigration},
		{name: "most_selective_mirror", useEngine: true, policy: PolicySelectiveMirror},
	} {
		row, err := runZeroReuseNegativeControl(ctx, scratchDir, sys.name, sys.useEngine, sys.policy, scanCount, blockSize, ramCap, nvmeCap)
		if err != nil {
			return SuiteReport{}, err
		}
		rows = append(rows, row)
	}

	return SuiteReport{
		Scale:  scale,
		GOOS:   runtime.GOOS,
		GOARCH: runtime.GOARCH,
		NumCPU: runtime.NumCPU(),
		Rows:   rows,
	}, nil
}

func runZipfWorkload(
	ctx context.Context,
	scratchDir, workload, sysName string,
	useEngine bool, policy Policy,
	simDelay time.Duration,
	numHot, numScan, blockSize int,
	ramCap, nvmeCap int64,
	agents, opsPerAgent int,
) (BenchmarkRow, error) {
	sub, err := os.MkdirTemp(scratchDir, "zipf-*")
	if err != nil {
		return BenchmarkRow{}, err
	}
	defer os.RemoveAll(sub)

	nvmeDir := filepath.Join(sub, "nvme")
	probe := NewProbeBackend(storetest.NewMem(), 0)

	// Populate immutable objects before timing (0ms delay during setup).
	files := make(map[string][]byte, numHot+numScan)
	for i := range numHot {
		k := fmt.Sprintf("tc/%04d", i)
		b := deterministicBlock(uint64(100+i), blockSize)
		files[k] = b
		if !useEngine {
			if err := probe.Put(ctx, k, b); err != nil {
				return BenchmarkRow{}, err
			}
		}
	}
	for i := range numScan {
		k := fmt.Sprintf("scan/%04d", i)
		b := deterministicBlock(uint64(10000+i), blockSize)
		files[k] = b
		if !useEngine {
			if err := probe.Put(ctx, k, b); err != nil {
				return BenchmarkRow{}, err
			}
		}
	}

	var eng *Engine
	if useEngine {
		ciph, err := newBenchCipher("zipf-bench")
		if err != nil {
			return BenchmarkRow{}, err
		}
		eng, err = NewEngine(probe, Config{
			Policy:               policy,
			NVMeDir:              nvmeDir,
			TotalRAMBytes:        ramCap,
			TotalNVMeBytes:       nvmeCap,
			HotThreshold:         2.0,
			HostCacheCipher:      ciph,
			SharedManifestCipher: ciph,
		})
		if err != nil {
			return BenchmarkRow{}, err
		}
		defer eng.Close()
		for a := range agents {
			if err := eng.RegisterTenant(fmt.Sprintf("ag-%d", a), fmt.Sprintf("v-%d", a), ciph, ramCap, nvmeCap); err != nil {
				return BenchmarkRow{}, err
			}
		}
		if _, err := eng.PublishSharedArtifact(ctx, "zipf-toolchain", files, ciph); err != nil {
			return BenchmarkRow{}, err
		}
	}

	// Precompute Zipfian CDF (alpha=1.15).
	cdf := make([]float64, numHot)
	var sumW float64
	for i := range numHot {
		w := 1.0 / math.Pow(float64(i+1), 1.15)
		sumW += w
		cdf[i] = sumW
	}
	sampleZipf := func(rng *mrand.Rand) int {
		u := rng.Float64() * sumW
		lo, hi := 0, numHot-1
		for lo < hi {
			mid := (lo + hi) / 2
			if cdf[mid] < u {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo
	}

	probe.Reset(simDelay)
	runtime.GC()
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	var noCacheLat latencyRecorder
	var wg sync.WaitGroup
	tStart := time.Now()
	for a := range agents {
		wg.Add(1)
		go func(agentIdx int) {
			defer wg.Done()
			rng := mrand.New(mrand.NewPCG(uint64(700+agentIdx), uint64(1300+agentIdx)))
			tid := fmt.Sprintf("ag-%d", agentIdx)
			scanBase := agentIdx * (numScan / agents)
			scanIdx := 0
			for step := range opsPerAgent {
				var key string
				if step%4 == 0 && scanIdx < numScan/agents {
					key = fmt.Sprintf("scan/%04d", scanBase+scanIdx)
					scanIdx++
				} else {
					key = fmt.Sprintf("tc/%04d", sampleZipf(rng))
				}
				if useEngine {
					if _, err := eng.Get(ctx, tid, ScopeSharedImmutable, key, backend.Unbounded); err != nil {
						return
					}
				} else {
					opT := time.Now()
					_, _, _ = probe.Get(ctx, key, backend.Unbounded)
					noCacheLat.record(time.Since(opT))
				}
			}
		}(a)
	}
	wg.Wait()
	wall := time.Since(tStart)
	runtime.ReadMemStats(&mAfter)

	totalOps := int64(agents * opsPerAgent)
	row := BenchmarkRow{
		Workload:            workload,
		System:              sysName,
		InjectedDelayMs:     float64(simDelay.Microseconds()) / 1000.0,
		Agents:              agents,
		Ops:                 totalOps,
		BlockBytes:          blockSize,
		TotalWallMs:         float64(wall.Microseconds()) / 1000.0,
		ThroughputOpsSec:    float64(totalOps) / wall.Seconds(),
		BackendGets:         probe.Gets(),
		HeapAllocDeltaBytes: int64(mAfter.HeapAlloc) - int64(mBefore.HeapAlloc),
		TotalAllocDeltaB:    int64(mAfter.TotalAlloc - mBefore.TotalAlloc),
		NumGCDelta:          mAfter.NumGC - mBefore.NumGC,
	}
	if useEngine {
		st := eng.Stats()
		row.P50Us = float64(st.P50.Nanoseconds()) / 1000.0
		row.P95Us = float64(st.P95.Nanoseconds()) / 1000.0
		row.P99Us = float64(st.P99.Nanoseconds()) / 1000.0
		row.BackendGets = st.BackendGets
		row.RAMHits = st.RAMHits
		row.NVMeHits = st.NVMeHits
		row.MirrorPromotions = st.MirrorPromotions
		row.TierMigrationWrites = st.TierMigrationWrites
		row.NVMeWrites = st.NVMeWrites
		row.ResidentRAMBytes = st.ResidentRAMBytes
		row.ResidentNVMeBytes = st.ResidentNVMeBytes
	} else {
		p50, p95, p99 := noCacheLat.percentiles()
		row.P50Us = float64(p50.Nanoseconds()) / 1000.0
		row.P95Us = float64(p95.Nanoseconds()) / 1000.0
		row.P99Us = float64(p99.Nanoseconds()) / 1000.0
	}
	return row, nil
}

func runNoisyNeighborWorkload(ctx context.Context, scratchDir string, policy Policy, blockSize int) (BenchmarkRow, error) {
	sub, err := os.MkdirTemp(scratchDir, "nn-*")
	if err != nil {
		return BenchmarkRow{}, err
	}
	defer os.RemoveAll(sub)

	probe := NewProbeBackend(storetest.NewMem(), 0)

	// Shared host pool is tight (32 blocks total RAM), while quotas allow A=16 blocks, B=16 blocks.
	// Note: under PolicyTierMigration, Tenant B's 1-hit cold scan churns B's RAM + writes demotions;
	// under PolicySelectiveMirror, Tenant A's working set stays untouched and B's cold scan never enters RAM!
	const (
		hotA   = 16
		floodB = 160
	)
	ramCap := int64(32 * blockSize)
	nvmeCap := int64(512 * blockSize)
	eng, err := NewEngine(probe, Config{
		Policy:         policy,
		NVMeDir:        filepath.Join(sub, "nvme"),
		TotalRAMBytes:  ramCap,
		TotalNVMeBytes: nvmeCap,
		HotThreshold:   2.0,
	})
	if err != nil {
		return BenchmarkRow{}, err
	}
	defer eng.Close()

	cA, _ := newBenchCipher("tenant-a")
	cB, _ := newBenchCipher("tenant-b")
	_ = eng.RegisterTenant("tenant-a", "va", cA, int64(hotA*blockSize), nvmeCap/2)
	_ = eng.RegisterTenant("tenant-b", "vb", cB, int64(hotA*blockSize), nvmeCap/2)

	for i := range hotA {
		if err := eng.Put(ctx, "tenant-a", ScopePrivate, fmt.Sprintf("a-%02d", i), deterministicBlock(uint64(i+1), blockSize)); err != nil {
			return BenchmarkRow{}, err
		}
		for range 3 {
			if _, err := eng.Get(ctx, "tenant-a", ScopePrivate, fmt.Sprintf("a-%02d", i), backend.Unbounded); err != nil {
				return BenchmarkRow{}, err
			}
		}
	}
	for i := range floodB {
		if err := eng.Put(ctx, "tenant-b", ScopePrivate, fmt.Sprintf("b-%03d", i), deterministicBlock(uint64(1000+i), blockSize)); err != nil {
			return BenchmarkRow{}, err
		}
	}

	eng.ResetStats()
	probe.Reset(0)
	runtime.GC()
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	tStart := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for r := range 120 {
			_, _ = eng.Get(ctx, "tenant-a", ScopePrivate, fmt.Sprintf("a-%02d", r%hotA), backend.Unbounded)
		}
	}()
	go func() {
		defer wg.Done()
		for i := range floodB {
			_, _ = eng.Get(ctx, "tenant-b", ScopePrivate, fmt.Sprintf("b-%03d", i), backend.Unbounded)
		}
	}()
	wg.Wait()
	wall := time.Since(tStart)
	runtime.ReadMemStats(&mAfter)

	stA := eng.TenantStats("tenant-a")
	st := eng.Stats()
	sysName := "most_selective_mirror"
	if policy == PolicyTierMigration {
		sysName = "tier_migration"
	}
	return BenchmarkRow{
		Workload:            "noisy_neighbor_tenant_a",
		System:              sysName,
		InjectedDelayMs:     0,
		Agents:              2,
		Ops:                 stA.TotalReads,
		BlockBytes:          blockSize,
		TotalWallMs:         float64(wall.Microseconds()) / 1000.0,
		ThroughputOpsSec:    float64(st.TotalReads) / wall.Seconds(),
		P50Us:               float64(stA.P50.Nanoseconds()) / 1000.0,
		P95Us:               float64(stA.P95.Nanoseconds()) / 1000.0,
		P99Us:               float64(stA.P99.Nanoseconds()) / 1000.0,
		BackendGets:         stA.BackendGets,
		RAMHits:             stA.RAMHits,
		NVMeHits:            stA.NVMeHits,
		MirrorPromotions:    st.MirrorPromotions,
		TierMigrationWrites: st.TierMigrationWrites,
		NVMeWrites:          st.NVMeWrites,
		ResidentRAMBytes:    stA.ResidentRAMBytes,
		ResidentNVMeBytes:   stA.ResidentNVMeBytes,
		HeapAllocDeltaBytes: int64(mAfter.HeapAlloc) - int64(mBefore.HeapAlloc),
		TotalAllocDeltaB:    int64(mAfter.TotalAlloc - mBefore.TotalAlloc),
		NumGCDelta:          mAfter.NumGC - mBefore.NumGC,
	}, nil
}

func runZeroReuseNegativeControl(
	ctx context.Context,
	scratchDir, sysName string,
	useEngine bool, policy Policy,
	scanCount, blockSize int,
	ramCap, nvmeCap int64,
) (BenchmarkRow, error) {
	sub, err := os.MkdirTemp(scratchDir, "neg-*")
	if err != nil {
		return BenchmarkRow{}, err
	}
	defer os.RemoveAll(sub)

	probe := NewProbeBackend(storetest.NewMem(), 0)
	files := make(map[string][]byte, scanCount)
	for i := range scanCount {
		k := fmt.Sprintf("once/%04d", i)
		b := deterministicBlock(uint64(90000+i), blockSize)
		files[k] = b
		if !useEngine {
			if err := probe.Put(ctx, k, b); err != nil {
				return BenchmarkRow{}, err
			}
		}
	}

	var eng *Engine
	if useEngine {
		ciph, _ := newBenchCipher("neg-ctl")
		eng, err = NewEngine(probe, Config{
			Policy:               policy,
			NVMeDir:              filepath.Join(sub, "nvme"),
			TotalRAMBytes:        ramCap,
			TotalNVMeBytes:       nvmeCap,
			HotThreshold:         2.0,
			HostCacheCipher:      ciph,
			SharedManifestCipher: ciph,
		})
		if err != nil {
			return BenchmarkRow{}, err
		}
		defer eng.Close()
		_ = eng.RegisterTenant("neg", "vol-neg", ciph, ramCap, nvmeCap)
		if _, err := eng.PublishSharedArtifact(ctx, "neg-scan", files, ciph); err != nil {
			return BenchmarkRow{}, err
		}
	}

	probe.Reset(0)
	runtime.GC()
	var mBefore, mAfter runtime.MemStats
	runtime.ReadMemStats(&mBefore)
	var noCacheLat latencyRecorder

	tStart := time.Now()
	for i := range scanCount {
		k := fmt.Sprintf("once/%04d", i)
		if useEngine {
			if _, err := eng.Get(ctx, "neg", ScopeSharedImmutable, k, backend.Unbounded); err != nil {
				return BenchmarkRow{}, err
			}
		} else {
			opT := time.Now()
			_, _, _ = probe.Get(ctx, k, backend.Unbounded)
			noCacheLat.record(time.Since(opT))
		}
	}
	wall := time.Since(tStart)
	runtime.ReadMemStats(&mAfter)

	row := BenchmarkRow{
		Workload:            "negative_control_zero_reuse_scan",
		System:              sysName,
		InjectedDelayMs:     0,
		Agents:              1,
		Ops:                 int64(scanCount),
		BlockBytes:          blockSize,
		TotalWallMs:         float64(wall.Microseconds()) / 1000.0,
		ThroughputOpsSec:    float64(scanCount) / wall.Seconds(),
		BackendGets:         probe.Gets(),
		HeapAllocDeltaBytes: int64(mAfter.HeapAlloc) - int64(mBefore.HeapAlloc),
		TotalAllocDeltaB:    int64(mAfter.TotalAlloc - mBefore.TotalAlloc),
		NumGCDelta:          mAfter.NumGC - mBefore.NumGC,
	}
	if useEngine {
		st := eng.Stats()
		row.P50Us = float64(st.P50.Nanoseconds()) / 1000.0
		row.P95Us = float64(st.P95.Nanoseconds()) / 1000.0
		row.P99Us = float64(st.P99.Nanoseconds()) / 1000.0
		row.BackendGets = st.BackendGets
		row.RAMHits = st.RAMHits
		row.NVMeHits = st.NVMeHits
		row.MirrorPromotions = st.MirrorPromotions
		row.TierMigrationWrites = st.TierMigrationWrites
		row.NVMeWrites = st.NVMeWrites
		row.ResidentRAMBytes = st.ResidentRAMBytes
		row.ResidentNVMeBytes = st.ResidentNVMeBytes
	} else {
		p50, p95, p99 := noCacheLat.percentiles()
		row.P50Us = float64(p50.Nanoseconds()) / 1000.0
		row.P95Us = float64(p95.Nanoseconds()) / 1000.0
		row.P99Us = float64(p99.Nanoseconds()) / 1000.0
	}
	return row, nil
}

// WriteCSV writes BenchmarkRows to w in RFC 4180 CSV format.
func WriteCSV(w io.Writer, rows []BenchmarkRow) error {
	cw := csv.NewWriter(w)
	header := []string{
		"workload", "system", "injected_delay_ms", "agents", "ops", "block_bytes",
		"total_wall_ms", "throughput_ops_sec", "p50_us", "p95_us", "p99_us",
		"backend_gets", "ram_hits", "nvme_hits", "mirror_promotions",
		"tier_migration_writes", "nvme_writes", "resident_ram_bytes", "resident_nvme_bytes",
		"heap_alloc_delta_bytes", "total_alloc_delta_bytes", "num_gc_delta",
	}
	if err := cw.Write(header); err != nil {
		return err
	}
	for _, r := range rows {
		rec := []string{
			r.Workload,
			r.System,
			fmt.Sprintf("%.2f", r.InjectedDelayMs),
			strconv.Itoa(r.Agents),
			strconv.FormatInt(r.Ops, 10),
			strconv.Itoa(r.BlockBytes),
			fmt.Sprintf("%.3f", r.TotalWallMs),
			fmt.Sprintf("%.1f", r.ThroughputOpsSec),
			fmt.Sprintf("%.2f", r.P50Us),
			fmt.Sprintf("%.2f", r.P95Us),
			fmt.Sprintf("%.2f", r.P99Us),
			strconv.FormatInt(r.BackendGets, 10),
			strconv.FormatInt(r.RAMHits, 10),
			strconv.FormatInt(r.NVMeHits, 10),
			strconv.FormatInt(r.MirrorPromotions, 10),
			strconv.FormatInt(r.TierMigrationWrites, 10),
			strconv.FormatInt(r.NVMeWrites, 10),
			strconv.FormatInt(r.ResidentRAMBytes, 10),
			strconv.FormatInt(r.ResidentNVMeBytes, 10),
			strconv.FormatInt(r.HeapAllocDeltaBytes, 10),
			strconv.FormatInt(r.TotalAllocDeltaB, 10),
			strconv.FormatUint(uint64(r.NumGCDelta), 10),
		}
		if err := cw.Write(rec); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
