// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package thinkahead

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

func openTempBackend(t *testing.T, opts CountingOptions) (*CountingBackend, string) {
	t.Helper()
	dir := t.TempDir()
	be, err := backend.Open(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("backend.Open: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return NewCountingBackend(be, opts), dir
}

func TestTraceWireCodecAndS6Bounds(t *testing.T) {
	ids := make([]ChunkID, 12)
	for i := range ids {
		ids[i] = SumChunk([]byte(fmt.Sprintf("chunk-%d", i)))
	}
	raw := EncodeTraceWire(ids)
	if len(raw) != 1+12*32 {
		t.Fatalf("encoded length = %d, want %d", len(raw), 1+12*32)
	}
	if raw[0] != TraceWireVersion {
		t.Fatalf("version byte = 0x%02x, want 0x%02x", raw[0], TraceWireVersion)
	}
	dec, err := DecodeTraceWire(raw)
	if err != nil {
		t.Fatalf("DecodeTraceWire: %v", err)
	}
	if len(dec) != len(ids) {
		t.Fatalf("decoded len = %d, want %d", len(dec), len(ids))
	}
	for i := range ids {
		if dec[i] != ids[i] {
			t.Fatalf("decoded[%d] mismatch", i)
		}
	}

	// Unknown version byte must be rejected (S6.2).
	badVer := append([]byte{0x02}, raw[1:]...)
	if _, err := DecodeTraceWire(badVer); !errors.Is(err, ErrInvalidTrace) {
		t.Fatalf("expected ErrInvalidTrace for version 0x02, got %v", err)
	}
	// Truncated trailing bytes must be rejected.
	if _, err := DecodeTraceWire(raw[:len(raw)-5]); !errors.Is(err, ErrInvalidTrace) {
		t.Fatalf("expected ErrInvalidTrace for truncated payload, got %v", err)
	}
	// S6.2 cap: 1,000,001 entries must truncate to 1,000,000 (32,000,001 bytes).
	if testing.Short() {
		return
	}
	huge := make([]ChunkID, MaxTraceEntries+1)
	huge[0] = ids[0]
	huge[MaxTraceEntries-1] = ids[1]
	huge[MaxTraceEntries] = ids[2]
	wireHuge := EncodeTraceWire(huge)
	if len(wireHuge) != MaxTraceBytes {
		t.Fatalf("capped wire length = %d, want %d", len(wireHuge), MaxTraceBytes)
	}
}

func TestLiveStraceTraceCollectionAndRepeatability(t *testing.T) {
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace not installed")
	}
	repoDir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("abs repoDir: %v", err)
	}
	scope := ScopeKey{
		TenantID:     "tenant-ci",
		RepoHash:     "hanzoai-vfs",
		WorkflowName: "go_list_live",
	}
	cfg := LiveTraceConfig{
		Scope:     scope,
		Dir:       repoDir,
		Command:   []string{"go", "list", "./pkg/backend/..."},
		ChunkSize: DefaultChunkSize,
		MaxEvents: 4000,
		Timeout:   20 * time.Second,
	}
	cfg.RunID = "live-run-1"
	run1, err := CollectLiveTrace(context.Background(), cfg)
	if err != nil {
		t.Fatalf("CollectLiveTrace run1: %v", err)
	}
	cfg.RunID = "live-run-2"
	run2, err := CollectLiveTrace(context.Background(), cfg)
	if err != nil {
		t.Fatalf("CollectLiveTrace run2: %v", err)
	}
	if run1.Provenance != ProvenanceLiveStraceVM || run2.Provenance != ProvenanceLiveStraceVM {
		t.Fatalf("unexpected provenance: %s / %s", run1.Provenance, run2.Provenance)
	}
	if len(run1.UniqueOrder) < 5 || len(run2.UniqueOrder) < 5 {
		t.Fatalf("expected >= 5 unique chunks from live strace, got %d and %d", len(run1.UniqueOrder), len(run2.UniqueOrder))
	}
	rep := AnalyzePredictability(run1, run2, "live_repeat")
	if rep.RunN1CoveredByRunN < 0.70 {
		t.Fatalf("expected live repeat coverage >= 0.70, got %.3f (shared=%d/%d)", rep.RunN1CoveredByRunN, rep.SharedChunks, rep.RunN1UniqueChunks)
	}
}

func TestPredictabilityRepeatVsEdit(t *testing.T) {
	profiles := DefaultWorkflowProfiles()
	if len(profiles) != 6 {
		t.Fatalf("expected 6 workflow profiles, got %d", len(profiles))
	}
	for i, prof := range profiles {
		scope := ScopeKey{TenantID: "t1", RepoHash: "repo-" + prof.Name, WorkflowName: prof.Name}
		runN, _ := GenerateManifestTrace(scope, prof, "run-N", int64(100+i))

		// Identical repeat with parallel scheduling jitter.
		runN1Ident, _ := PerturbTrace(runN, "run-N+1-repeat", 0, prof.OrderRepeat, int64(200+i))
		repIdent := AnalyzePredictability(runN, runN1Ident, "repeat_identical")
		if repIdent.RunN1CoveredByRunN < 0.999 {
			t.Fatalf("%s identical repeat recall = %.4f, want 1.0", prof.Name, repIdent.RunN1CoveredByRunN)
		}

		// Realistic 3-file code edit / dependency bump.
		runN1Edit, _ := PerturbTrace(runN, "run-N+1-edit3", 3, prof.OrderRepeat, int64(300+i))
		repEdit := AnalyzePredictability(runN, runN1Edit, "after_3_file_edit")
		if repEdit.RunN1CoveredByRunN < 0.90 {
			t.Fatalf("%s 3-file edit recall = %.4f, want >= 0.90", prof.Name, repEdit.RunN1CoveredByRunN)
		}
		if repEdit.JaccardSimilarity < 0.85 {
			t.Fatalf("%s 3-file edit Jaccard = %.4f, want >= 0.85", prof.Name, repEdit.JaccardSimilarity)
		}
		if prof.Name == "go_build_test" && repEdit.TransitionPredictability >= 0.50 {
			t.Fatalf("expected parallel go_build_test Markov-1 exact transition < 0.50, got %.3f", repEdit.TransitionPredictability)
		}
	}
}

func TestThinkAheadPrefetchHitRateAndTailLatency(t *testing.T) {
	ctx := context.Background()
	cb, _ := openTempBackend(t, CountingOptions{})
	store := NewChunkObjectStore(cb)

	prof := DefaultWorkflowProfiles()[2] // go_build_test
	prof.Files = 60
	prof.AvgChunksPerFile = 2
	scope := ScopeKey{TenantID: "tenant-alpha", RepoHash: "repo-go", WorkflowName: prof.Name}

	runN, payloadsN := GenerateManifestTrace(scope, prof, "run-N", 42)
	runN1, payloadsEdit := PerturbTrace(runN, "run-N+1", 2, prof.OrderRepeat, 43)

	allPayloads := make(map[ChunkID][]byte, len(payloadsN)+len(payloadsEdit))
	for k, v := range payloadsN {
		allPayloads[k] = v
	}
	for k, v := range payloadsEdit {
		allPayloads[k] = v
	}
	allOrder := append(append([]ChunkID{}, runN.UniqueOrder...), runN1.UniqueOrder...)
	if err := store.PutPack(ctx, allOrder, allPayloads); err != nil {
		t.Fatalf("PutPack: %v", err)
	}

	// Build file-chunk resolver for sequential read-ahead baseline.
	fileMap := make(map[string]ChunkID, len(runN1.Events))
	for _, ev := range runN1.Events {
		fileMap[fmt.Sprintf("%d:%d", ev.FileID, ev.ChunkIdx)] = ev.ChunkID
	}
	resolver := func(fid uint64, idx int64) (ChunkID, bool) {
		id, ok := fileMap[fmt.Sprintf("%d:%d", fid, idx)]
		return id, ok
	}

	learner := NewLearner(0.7)
	learner.RecordTrace(runN)
	plan := learner.Plan(scope, 0)

	runPolicy := func(policy PolicyMode) (EngineStats, time.Duration, BackendCounts) {
		cb.Reset(CountingOptions{SimulatedRTT: 1500 * time.Microsecond, LinkBytesPerSec: 750 << 20})
		eng := NewEngine(store, learner, Config{
			Policy:              policy,
			MaxCacheBytes:       32 << 20,
			MaxUnreadBytes:      16 << 20,
			MaxPrefetchInflight: 16,
			MaxMergeBytes:       1 << 20,
			VerifyContentHash:   true,
			FileChunkResolver:   resolver,
		})
		defer eng.Detach()

		start := time.Now()
		if policy == PolicyThinkAhead {
			eng.StartPlan(ctx, scope, plan)
			// Brief head-start mirroring process startup (ELF load / CLI parse).
			time.Sleep(8 * time.Millisecond)
		}
		for _, ev := range runN1.Events {
			// Simulate realistic per-chunk CPU/parse work between reads.
			time.Sleep(150 * time.Microsecond)
			got, _, _, err := eng.ReadChunk(ctx, scope, ev.FileID, ev.ChunkIdx, ev.ChunkID)
			if err != nil {
				t.Fatalf("policy %s ReadChunk: %v", policy, err)
			}
			if !bytes.Equal(got, allPayloads[ev.ChunkID]) {
				t.Fatalf("policy %s chunk byte mismatch", policy)
			}
		}
		wall := time.Since(start)
		st := eng.Stats()
		eng.Detach()
		return st, wall, cb.Counts()
	}

	stCold, wallCold, _ := runPolicy(PolicyNoPrefetch)
	stSeq, _, _ := runPolicy(PolicySequentialReadahead)
	stTA, wallTA, _ := runPolicy(PolicyThinkAhead)

	if stTA.HashMismatchCount != 0 {
		t.Fatalf("unexpected hash mismatches: %d", stTA.HashMismatchCount)
	}
	if stTA.HitRate <= stCold.HitRate+0.50 {
		t.Fatalf("ThinkAhead hit rate %.3f did not beat cold %.3f by >= 0.50", stTA.HitRate, stCold.HitRate)
	}
	if stTA.HitRate <= stSeq.HitRate+0.30 {
		t.Fatalf("ThinkAhead hit rate %.3f did not beat sequential readahead %.3f by >= 0.30", stTA.HitRate, stSeq.HitRate)
	}
	if wallTA >= wallCold {
		t.Fatalf("ThinkAhead wall %v was not faster than cold %v", wallTA, wallCold)
	}
	if stTA.WaitP50Us >= stCold.WaitP50Us/2 {
		t.Fatalf("ThinkAhead p50 wait %.1fus not < half of cold %.1fus", stTA.WaitP50Us, stCold.WaitP50Us)
	}
}

func TestMultiTenantIsolationZeroLeakage(t *testing.T) {
	ctx := context.Background()
	cb, _ := openTempBackend(t, CountingOptions{})
	store := NewChunkObjectStore(cb)

	sharedBytes := makeDeterministicChunk("shared", "secret.go", 0, 16<<10, 99)
	sharedID := SumChunk(sharedBytes)
	if err := store.PutChunk(ctx, sharedID, sharedBytes); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	scopeA := ScopeKey{TenantID: "tenant-A", RepoHash: "repo-shared", WorkflowName: "go_build"}
	scopeB := ScopeKey{TenantID: "tenant-B", RepoHash: "repo-shared", WorkflowName: "go_build"}

	learner := NewLearner(0.7)
	learner.RecordSequence(scopeA, []ChunkID{sharedID})

	// Tenant B must not see Tenant A's learned plan.
	if learner.HasPlan(scopeB) {
		t.Fatalf("cross-tenant leak: learner exposed tenant-A plan to tenant-B")
	}
	if planB := learner.Plan(scopeB, 0); planB != nil {
		t.Fatalf("cross-tenant leak: Plan returned non-nil for tenant-B")
	}

	eng := NewEngine(store, learner, Config{
		Policy:              PolicyThinkAhead,
		MaxCacheBytes:       8 << 20,
		MaxUnreadBytes:      4 << 20,
		MaxPrefetchInflight: 4,
		VerifyContentHash:   true,
	})
	defer eng.Detach()

	// Starting Tenant A's plan under Tenant B's scope must be a no-op.
	planA := learner.Plan(scopeA, 0)
	eng.StartPlan(ctx, scopeB, planA)
	time.Sleep(10 * time.Millisecond)

	// Tenant A reads and caches the shared chunk.
	_, outA1, _, err := eng.ReadChunk(ctx, scopeA, 1, 0, sharedID)
	if err != nil || outA1 != OutcomeMiss {
		t.Fatalf("tenant-A first read: out=%v err=%v", outA1, err)
	}
	_, outA2, _, err := eng.ReadChunk(ctx, scopeA, 1, 0, sharedID)
	if err != nil || outA2 != OutcomeHit {
		t.Fatalf("tenant-A second read: out=%v err=%v", outA2, err)
	}

	// Tenant B reading the EXACT SAME ChunkID must still miss (zero timing/cache leak).
	_, outB1, _, err := eng.ReadChunk(ctx, scopeB, 1, 0, sharedID)
	if err != nil {
		t.Fatalf("tenant-B first read err: %v", err)
	}
	if outB1 != OutcomeMiss {
		t.Fatalf("cross-tenant cache leak: tenant-B got %s on chunk cached by tenant-A", outB1)
	}
}

func TestSegmentedCacheProtectsWorkingSetS5(t *testing.T) {
	const chunkSz = 4096
	const numProt = 8
	cache := NewSegmentedCache(numProt * chunkSz)
	sh := ScopeKey{TenantID: "t1", RepoHash: "r1", WorkflowName: "w1"}.DeriveKey()

	// Fill cache completely with protected (demand-read) chunks.
	protIDs := make([]ChunkID, numProt)
	for i := 0; i < numProt; i++ {
		buf := makeDeterministicChunk("r1", fmt.Sprintf("prot-%d", i), 0, chunkSz, 1)
		id := SumChunk(buf)
		protIDs[i] = id
		cache.AddDemand(ScopedChunkKey{ScopeHash: sh, ID: id}, buf)
	}

	// Attempt to insert 2x cache capacity of prefetched chunks.
	for i := 0; i < numProt*2; i++ {
		buf := makeDeterministicChunk("r1", fmt.Sprintf("pref-%d", i), 0, chunkSz, 2)
		id := SumChunk(buf)
		if added := cache.AddPrefetch(ScopedChunkKey{ScopeHash: sh, ID: id}, buf); added {
			t.Fatalf("AddPrefetch(%d) succeeded despite full protected cache", i)
		}
	}

	st := cache.Stats()
	if st.EvictedProtectedCount != 0 {
		t.Fatalf("S5.1 violated: EvictedProtectedCount = %d, want 0", st.EvictedProtectedCount)
	}
	for i, id := range protIDs {
		if !cache.Contains(ScopedChunkKey{ScopeHash: sh, ID: id}) {
			t.Fatalf("protected chunk %d was evicted by prefetch", i)
		}
	}

	// S5.2: Pinned (dirty/unpublished) chunk must survive even demand eviction.
	pinnedKey := ScopedChunkKey{ScopeHash: sh, ID: protIDs[0]}
	if !cache.Pin(pinnedKey) {
		t.Fatalf("Pin failed")
	}
	for i := 0; i < numProt*2; i++ {
		buf := makeDeterministicChunk("r1", fmt.Sprintf("dem-%d", i), 0, chunkSz, 3)
		cache.AddDemand(ScopedChunkKey{ScopeHash: sh, ID: SumChunk(buf)}, buf)
	}
	if !cache.Contains(pinnedKey) {
		t.Fatalf("S5.2 violated: pinned chunk was evicted")
	}
	cache.Unpin(pinnedKey)
}

type corruptingFetcher struct {
	ChunkFetcher
	corruptID ChunkID
}

func (f *corruptingFetcher) FetchChunk(ctx context.Context, id ChunkID) ([]byte, error) {
	b, err := f.ChunkFetcher.FetchChunk(ctx, id)
	if err != nil {
		return nil, err
	}
	cp := append([]byte(nil), b...)
	if id == f.corruptID && len(cp) > 0 {
		cp[0] ^= 0xff
	}
	return cp, nil
}

func TestContentHashVerificationRejectsCorruptChunk(t *testing.T) {
	ctx := context.Background()
	cb, _ := openTempBackend(t, CountingOptions{})
	store := NewChunkObjectStore(cb)

	data := makeDeterministicChunk("r1", "a.go", 0, 8192, 7)
	id := SumChunk(data)
	if err := store.PutChunk(ctx, id, data); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	cf := &corruptingFetcher{ChunkFetcher: store, corruptID: id}
	scope := ScopeKey{TenantID: "t1", RepoHash: "r1", WorkflowName: "w1"}
	eng := NewEngine(cf, nil, Config{
		Policy:            PolicyThinkAhead,
		VerifyContentHash: true,
	})
	defer eng.Detach()

	_, _, _, err := eng.ReadChunk(ctx, scope, 1, 0, id)
	if !errors.Is(err, ErrChunkCorrupt) {
		t.Fatalf("expected ErrChunkCorrupt, got %v", err)
	}
	if eng.Cache().Contains(ScopedChunkKey{ScopeHash: scope.DeriveKey(), ID: id}) {
		t.Fatalf("corrupt chunk must not be cached")
	}
	if eng.Stats().HashMismatchCount == 0 {
		t.Fatalf("expected HashMismatchCount > 0")
	}
}

func TestAdaptiveConfidenceThrottleNegativeRegime2(t *testing.T) {
	ctx := context.Background()
	cb, _ := openTempBackend(t, CountingOptions{})
	store := NewChunkObjectStore(cb)

	scope := ScopeKey{TenantID: "t1", RepoHash: "repo-refactor", WorkflowName: "cargo_check"}
	prof := DefaultWorkflowProfiles()[3]
	prof.Files = 60
	prof.AvgChunksPerFile = 2

	// Stale trace from old major version (0% overlap with new refactored repo).
	staleTrace, stalePayloads := GenerateManifestTrace(scope, prof, "old-v1", 1001)
	newTrace, newPayloads := GenerateManifestTrace(
		ScopeKey{TenantID: "t1", RepoHash: "repo-refactor-v2", WorkflowName: "cargo_check"},
		prof, "new-v2", 2002,
	)
	newTrace.Scope = scope

	all := make(map[ChunkID][]byte, len(stalePayloads)+len(newPayloads))
	for k, v := range stalePayloads {
		all[k] = v
	}
	for k, v := range newPayloads {
		all[k] = v
	}
	order := append(append([]ChunkID{}, staleTrace.UniqueOrder...), newTrace.UniqueOrder...)
	if err := store.PutPack(ctx, order, all); err != nil {
		t.Fatalf("PutPack: %v", err)
	}

	learner := NewLearner(0.7)
	learner.RecordTrace(staleTrace)
	stalePlan := learner.Plan(scope, 0)

	runMismatch := func(policy PolicyMode) (EngineStats, BackendCounts) {
		cb.Reset(CountingOptions{SimulatedRTT: 1 * time.Millisecond})
		eng := NewEngine(store, learner, Config{
			Policy:              policy,
			MaxCacheBytes:       32 << 20,
			MaxUnreadBytes:      16 << 20,
			MaxPrefetchInflight: 4,
			MinConfidence:       0.25,
			ConfidenceWindow:    16,
			VerifyContentHash:   true,
		})
		defer eng.Detach()
		eng.StartPlan(ctx, scope, stalePlan)
		for _, ev := range newTrace.Events {
			if _, _, _, err := eng.ReadChunk(ctx, scope, ev.FileID, ev.ChunkIdx, ev.ChunkID); err != nil {
				t.Fatalf("ReadChunk: %v", err)
			}
		}
		st := eng.Stats()
		eng.Detach()
		return st, cb.Counts()
	}

	stUnthrottled, cntUnthrottled := runMismatch(PolicyThinkAheadNoThrottle)
	stAdaptive, cntAdaptive := runMismatch(PolicyThinkAhead)

	if !stAdaptive.ThrottledPaused || stAdaptive.ThrottledEvents == 0 {
		t.Fatalf("adaptive confidence gate failed to trigger on 0%%-overlap stale trace: %+v", stAdaptive)
	}
	if cntAdaptive.PrefetchGets >= cntUnthrottled.PrefetchGets {
		t.Fatalf("expected adaptive throttle to reduce wasted prefetch GETs: adaptive=%d unthrottled=%d",
			cntAdaptive.PrefetchGets, cntUnthrottled.PrefetchGets)
	}
	if stAdaptive.WastedBytes >= stUnthrottled.WastedBytes {
		t.Fatalf("expected adaptive throttle to reduce wasted bytes: adaptive=%d unthrottled=%d",
			stAdaptive.WastedBytes, stUnthrottled.WastedBytes)
	}
}

func TestNegativeRegimes1And3(t *testing.T) {
	ctx := context.Background()
	cb, _ := openTempBackend(t, CountingOptions{})
	store := NewChunkObjectStore(cb)

	prof := DefaultWorkflowProfiles()[0] // npm_ci
	prof.Files = 40
	prof.AvgChunksPerFile = 2
	scope := ScopeKey{TenantID: "t1", RepoHash: "repo-neg", WorkflowName: prof.Name}
	tr, payloads := GenerateManifestTrace(scope, prof, "run-0", 77)
	if err := store.PutPack(ctx, tr.UniqueOrder, payloads); err != nil {
		t.Fatalf("PutPack: %v", err)
	}

	// Negative Regime 1: Cold First Run (N=0, no prior trace).
	learner := NewLearner(0.7)
	cb.Reset(CountingOptions{SimulatedRTT: 500 * time.Microsecond})
	eng0 := NewEngine(store, learner, Config{Policy: PolicyThinkAhead, VerifyContentHash: true})
	eng0.StartPlan(ctx, scope, learner.Plan(scope, 0)) // nil plan
	for _, ev := range tr.Events {
		if _, _, _, err := eng0.ReadChunk(ctx, scope, ev.FileID, ev.ChunkIdx, ev.ChunkID); err != nil {
			t.Fatalf("ReadChunk N=0: %v", err)
		}
	}
	st0 := eng0.Stats()
	eng0.Detach()
	if st0.PrefetchIssued != 0 || cb.Counts().PrefetchGets != 0 {
		t.Fatalf("Neg-1 cold first run issued unexpected prefetches: %d / %d", st0.PrefetchIssued, cb.Counts().PrefetchGets)
	}

	// Negative Regime 3: Undersized node cache (CacheBytes < WorkingSet / 4).
	learner.RecordTrace(tr)
	plan := learner.Plan(scope, 0)
	tinyCache := tr.UniqueBytes / 5
	if tinyCache < 128<<10 {
		tinyCache = 128 << 10
	}
	cb.Reset(CountingOptions{SimulatedRTT: 500 * time.Microsecond})
	engTight := NewEngine(store, learner, Config{
		Policy:              PolicyThinkAhead,
		MaxCacheBytes:       tinyCache,
		MaxUnreadBytes:      tinyCache / 2,
		MaxPrefetchInflight: 8,
		VerifyContentHash:   true,
	})
	engTight.StartPlan(ctx, scope, plan)
	time.Sleep(5 * time.Millisecond)
	for _, ev := range tr.Events {
		if _, _, _, err := engTight.ReadChunk(ctx, scope, ev.FileID, ev.ChunkIdx, ev.ChunkID); err != nil {
			t.Fatalf("ReadChunk tight cache: %v", err)
		}
	}
	stTight := engTight.Stats()
	engTight.Detach()
	if stTight.Cache.UsedBytes > tinyCache {
		t.Fatalf("cache exceeded bound: used=%d max=%d", stTight.Cache.UsedBytes, tinyCache)
	}
}

func TestUnreachableIgnoredSingleflightAndDetachS2S4(t *testing.T) {
	ctx := context.Background()
	cb, _ := openTempBackend(t, CountingOptions{SimulatedRTT: 2 * time.Millisecond})
	store := NewChunkObjectStore(cb)

	validPayloads := make(map[ChunkID][]byte)
	var planIDs []ChunkID
	for i := 0; i < 10; i++ {
		b := makeDeterministicChunk("r", fmt.Sprintf("f-%d", i), 0, 8192, 5)
		id := SumChunk(b)
		validPayloads[id] = b
		planIDs = append(planIDs, id)
	}
	if err := store.PutPack(ctx, planIDs, validPayloads); err != nil {
		t.Fatalf("PutPack: %v", err)
	}
	// Append 4 unreachable chunks to the plan (S2.1).
	for i := 0; i < 4; i++ {
		planIDs = append(planIDs, SumChunk([]byte(fmt.Sprintf("unreachable-%d", i))))
	}

	scope := ScopeKey{TenantID: "t1", RepoHash: "r", WorkflowName: "w"}
	learner := NewLearner(0.7)
	learner.RecordSequence(scope, planIDs)

	eng := NewEngine(store, learner, Config{
		Policy:              PolicyThinkAhead,
		MaxCacheBytes:       4 << 20,
		MaxUnreadBytes:      2 << 20,
		MaxPrefetchInflight: 4,
		VerifyContentHash:   true,
	})

	// S2.2: 12 concurrent demand readers of the same uncached chunk -> exactly 1 GET.
	cb.Reset(CountingOptions{SimulatedRTT: 5 * time.Millisecond})
	target := planIDs[0]
	var wg sync.WaitGroup
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, _, _, err := eng.ReadChunk(ctx, scope, 1, 0, target)
			if err != nil || !bytes.Equal(got, validPayloads[target]) {
				t.Errorf("concurrent ReadChunk failed: %v", err)
			}
		}()
	}
	wg.Wait()
	if gets := cb.Counts().TotalGets(); gets != 1 {
		t.Fatalf("S2.2 singleflight violated: 12 concurrent readers issued %d GETs, want 1", gets)
	}

	// S2.1: Start plan with 4 unreachable chunks -> ignored count must reach 4.
	eng.StartPlan(ctx, scope, learner.Plan(scope, 0))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if eng.Stats().PrefetchIgnored >= 4 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ig := eng.Stats().PrefetchIgnored; ig != 4 {
		t.Fatalf("S2.1 PrefetchIgnored = %d, want 4", ig)
	}

	// S4.2: Publication lag > bound/2 cancels active prefetch plans.
	eng.NotifyUnpublishedAge(3*time.Second, 5*time.Second)
	eng.NotifyUnpublishedAge(1*time.Second, 5*time.Second)

	// S4.3: After Detach returns, ZERO prefetch requests occur.
	eng.Detach()
	afterDetach := cb.Counts().TotalGets()
	time.Sleep(20 * time.Millisecond)
	if now := cb.Counts().TotalGets(); now != afterDetach {
		t.Fatalf("S4.3 violated: %d GETs occurred after Detach", now-afterDetach)
	}
}

func TestConcurrentReadersAndPrefetchersRace(t *testing.T) {
	ctx := context.Background()
	cb, _ := openTempBackend(t, CountingOptions{SimulatedRTT: 200 * time.Microsecond})
	store := NewChunkObjectStore(cb)

	prof := DefaultWorkflowProfiles()[1]
	prof.Files = 30
	prof.AvgChunksPerFile = 2
	scope := ScopeKey{TenantID: "race-t", RepoHash: "race-r", WorkflowName: prof.Name}
	tr, payloads := GenerateManifestTrace(scope, prof, "race-run", 88)
	if err := store.PutPack(ctx, tr.UniqueOrder, payloads); err != nil {
		t.Fatalf("PutPack: %v", err)
	}

	learner := NewLearner(0.7)
	learner.RecordTrace(tr)
	eng := NewEngine(store, learner, Config{
		Policy:              PolicyThinkAhead,
		MaxCacheBytes:       2 << 20,
		MaxUnreadBytes:      1 << 20,
		MaxPrefetchInflight: 8,
		MaxMergeBytes:       512 << 10,
		VerifyContentHash:   true,
	})
	eng.StartPlan(ctx, scope, learner.Plan(scope, 0))

	var wg sync.WaitGroup
	var errCount atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < len(tr.Events); i += 8 {
				ev := tr.Events[i]
				got, _, _, err := eng.ReadChunk(ctx, scope, ev.FileID, ev.ChunkIdx, ev.ChunkID)
				if err != nil || len(got) != int(ev.Length) {
					errCount.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	eng.Detach()
	if errCount.Load() != 0 {
		t.Fatalf("concurrent race test had %d read errors", errCount.Load())
	}
}

func TestLiveVolumeEndToEndTracePersistenceAndWarming(t *testing.T) {
	ctx := context.Background()
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("GenerateHybridIdentity: %v", err)
	}
	cipher := vfs.Hybrid(id)

	cb, _ := openTempBackend(t, CountingOptions{})
	jdir := t.TempDir()
	cfg := vfs.Config{
		Backend: cb,
		Cipher:  cipher,
		Round:   1,
		Journal: jdir,
		Bound:   5 * time.Second,
		Cache:   32 << 20,
	}
	if err := vfs.Create(ctx, cfg); err != nil {
		t.Fatalf("vfs.Create: %v", err)
	}
	vol, err := vfs.Attach(ctx, cfg)
	if err != nil {
		t.Fatalf("vfs.Attach: %v", err)
	}

	// Write 16 chunked files (each 80 KiB > 16 KiB inline threshold so they live in pack/).
	const numFiles = 16
	const fileSize = 80 << 10
	inodes := make([]vfs.InodeID, numFiles)
	expected := make([][]byte, numFiles)
	for i := 0; i < numFiles; i++ {
		in, err := vol.Create(ctx, vfs.RootInode, fmt.Sprintf("mod-%02d.bin", i), 0o644)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		f, err := vol.Open(ctx, in.ID)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		payload := makeDeterministicChunk("live-vol", fmt.Sprintf("mod-%02d", i), 0, fileSize, int64(i))
		if _, err := f.WriteAt(ctx, payload, 0); err != nil {
			t.Fatalf("WriteAt: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("file Close: %v", err)
		}
		inodes[i] = in.ID
		expected[i] = payload
	}
	if err := vol.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	pubStart := time.Now()
	if err := vol.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if pubDur := time.Since(pubStart); pubDur > 5*time.Second {
		t.Fatalf("publication exceeded 5s bound: %v", pubDur)
	}

	// Persist both S6 chunk-ID trace and warm hints sealed under Volume.PutExt.
	scope := ScopeKey{TenantID: "tenant-live", RepoHash: "repo-live", WorkflowName: "go_build_test"}
	chunkIDs := make([]ChunkID, numFiles)
	for i, b := range expected {
		chunkIDs[i] = SumChunk(b)
	}
	if err := SaveTraceToVolume(ctx, vol, scope, chunkIDs); err != nil {
		t.Fatalf("SaveTraceToVolume: %v", err)
	}

	vpRec := NewVolumePrefetcher(vol, scope, 8)
	for _, ino := range inodes {
		vpRec.ObserveRead(ino, 0, fileSize)
	}
	// Let recorder goroutine drain before saving hints.
	time.Sleep(10 * time.Millisecond)
	if err := vpRec.SaveHints(ctx); err != nil {
		t.Fatalf("SaveHints: %v", err)
	}
	vpRec.Close()

	// Verify clean Close issues ZERO synchronous S3 requests.
	beforeClose := cb.Counts()
	if err := vol.Close(); err != nil {
		t.Fatalf("vol.Close: %v", err)
	}
	afterClose := cb.Counts()
	if afterClose.Puts != beforeClose.Puts || afterClose.PutIfVersions != beforeClose.PutIfVersions || afterClose.TotalGets() != beforeClose.TotalGets() {
		t.Fatalf("clean Close issued synchronous S3 requests: before=%+v after=%+v", beforeClose, afterClose)
	}

	// Reopen Volume read-only on a fresh cold cache with 2ms simulated S3 RTT.
	dir := cb.Backend.String()
	rawDir := dir[len("file://"):]
	beCold, err := backend.Open(ctx, "file://"+rawDir)
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	defer beCold.Close()
	cbCold := NewCountingBackend(beCold, CountingOptions{SimulatedRTT: 2 * time.Millisecond})

	// S6 security check: a different volume key MUST fail to open/unseal the trace.
	wrongID, _ := age.GenerateHybridIdentity()
	if _, err := vfs.Open(ctx, vfs.Config{Backend: cbCold, Cipher: vfs.Hybrid(wrongID)}); err == nil {
		t.Fatalf("expected wrong cipher key to fail opening volume")
	}

	// Run 1: Cold open WITHOUT prefetch.
	cbCold.Reset(CountingOptions{SimulatedRTT: 2 * time.Millisecond})
	volCold, err := vfs.Open(ctx, vfs.Config{Backend: cbCold, Cipher: cipher, Cache: 32 << 20})
	if err != nil {
		t.Fatalf("vfs.Open cold: %v", err)
	}
	buf := make([]byte, fileSize)
	coldStart := time.Now()
	for i, ino := range inodes {
		f, err := volCold.Open(ctx, ino)
		if err != nil {
			t.Fatalf("cold Open: %v", err)
		}
		if _, err := f.ReadAt(ctx, buf, 0); err != nil {
			t.Fatalf("cold ReadAt: %v", err)
		}
		_ = f.Close()
		if !bytes.Equal(buf, expected[i]) {
			t.Fatalf("cold read mismatch on file %d", i)
		}
	}
	coldDur := time.Since(coldStart)
	coldCounts := cbCold.Counts()
	_ = volCold.Close()

	// Run 2: Cold open WITH ThinkAhead VolumePrefetcher (loaded from Volume.GetExt).
	beWarm, err := backend.Open(ctx, "file://"+rawDir)
	if err != nil {
		t.Fatalf("reopen backend warm: %v", err)
	}
	defer beWarm.Close()
	cbWarm := NewCountingBackend(beWarm, CountingOptions{SimulatedRTT: 2 * time.Millisecond})
	volWarm, err := vfs.Open(ctx, vfs.Config{Backend: cbWarm, Cipher: cipher, Cache: 32 << 20})
	if err != nil {
		t.Fatalf("vfs.Open warm: %v", err)
	}
	loadedIDs, err := LoadTraceFromVolume(ctx, volWarm, scope)
	if err != nil || len(loadedIDs) != numFiles {
		t.Fatalf("LoadTraceFromVolume: len=%d err=%v", len(loadedIDs), err)
	}
	vpWarm := NewVolumePrefetcher(volWarm, scope, 8)
	nHints, err := vpWarm.StartWarmFromVolume(ctx)
	if err != nil || nHints != numFiles {
		t.Fatalf("StartWarmFromVolume: hints=%d err=%v", nHints, err)
	}
	vpWarm.WaitWarm()

	warmStart := time.Now()
	for i, ino := range inodes {
		f, err := volWarm.Open(ctx, ino)
		if err != nil {
			t.Fatalf("warm Open: %v", err)
		}
		if _, err := f.ReadAt(ctx, buf, 0); err != nil {
			t.Fatalf("warm ReadAt: %v", err)
		}
		_ = f.Close()
		if !bytes.Equal(buf, expected[i]) {
			t.Fatalf("warm read mismatch on file %d", i)
		}
	}
	warmDur := time.Since(warmStart)
	warmCounts := cbWarm.Counts()
	vpWarm.Close()
	_ = volWarm.Close()

	if warmDur >= coldDur {
		t.Fatalf("expected warmed Volume read (%v) < cold Volume read (%v)", warmDur, coldDur)
	}
	if warmCounts.DemandGets >= coldCounts.DemandGets {
		t.Fatalf("expected fewer demand GETs after warming: warm=%d cold=%d", warmCounts.DemandGets, coldCounts.DemandGets)
	}
	t.Logf("live *vfs.Volume cold=%v (demandGETs=%d) vs ThinkAhead-warmed=%v (demandGETs=%d, prefetchGETs=%d)",
		coldDur, coldCounts.DemandGets, warmDur, warmCounts.DemandGets, warmCounts.PrefetchGets)
	_ = os.ErrNotExist
}
