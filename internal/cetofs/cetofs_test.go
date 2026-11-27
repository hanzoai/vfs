// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cetofs

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

func testCipher(t *testing.T) vfs.Cipher {
	t.Helper()
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("generate hybrid identity: %v", err)
	}
	return vfs.Hybrid(id)
}

func makeShardKey(data []byte) string {
	sum := blake3.Sum(data)
	h := hex.EncodeToString(sum[:])
	return "meta/" + h[:2] + "/" + h
}

func TestGuaranteeCatalogCompleteness(t *testing.T) {
	classes := AllRedundancyClasses()
	cat := GuaranteeCatalog()
	if len(cat) != len(classes) {
		t.Fatalf("GuaranteeCatalog has %d records, want %d", len(cat), len(classes))
	}
	for _, cls := range classes {
		rec, ok := GuaranteeFor(cls)
		if !ok {
			t.Fatalf("missing GuaranteeRecord for redundancy class %q", cls)
		}
		if rec.Boundary == "" || rec.Optimization == "" || rec.PreviousGuarantee == "" ||
			rec.PreservedGuarantee == "" || rec.NegativeRegime == "" || rec.SafetyInvariant == "" {
			t.Fatalf("incomplete GuaranteeRecord for %q: %+v", cls, rec)
		}
	}
}

func TestAuditRecorderCountsLatenciesAndClassifiers(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	rec := NewAuditRecorder()
	inst := NewInstrumentedBackend(mem, rec)

	// 1. Repeated immutable shard reads.
	shardPayload := []byte("sealed-shard-content")
	sKey := makeShardKey(shardPayload)
	if err := inst.Put(ctx, sKey, shardPayload); err != nil {
		t.Fatalf("put shard: %v", err)
	}
	for range 3 {
		if _, _, err := inst.Get(ctx, sKey, 4096); err != nil {
			t.Fatalf("get shard: %v", err)
		}
	}

	// 2. Repeated Head read + redundant pre-PUT GET before conditional PUT.
	v0, err := inst.PutIfAbsent(ctx, "head", []byte("h0"))
	if err != nil {
		t.Fatalf("putifabsent head: %v", err)
	}
	if _, _, err := inst.Get(ctx, "head", 4096); err != nil {
		t.Fatalf("get head: %v", err)
	}
	if _, err := inst.PutIfVersion(ctx, "head", []byte("h1"), v0); err != nil {
		t.Fatalf("putifversion head: %v", err)
	}

	// 3. Repeated List on same prefix without intervening mutation.
	for range 2 {
		if _, err := drainList(ctx, inst, "meta/"); err != nil {
			t.Fatalf("list: %v", err)
		}
	}

	// 4. Uncoalesced adjacent RangeGETs on same pack.
	packKey := "pack/aa/00112233445566778899aabbccddeeff"
	if err := inst.Put(ctx, packKey, make([]byte, 256<<10)); err != nil {
		t.Fatalf("put pack: %v", err)
	}
	if _, err := inst.GetRange(ctx, packKey, 0, 16<<10); err != nil {
		t.Fatalf("getrange 1: %v", err)
	}
	if _, err := inst.GetRange(ctx, packKey, 16<<10, 16<<10); err != nil {
		t.Fatalf("getrange 2: %v", err)
	}
	if _, err := inst.GetRange(ctx, packKey, 40<<10, 16<<10); err != nil {
		t.Fatalf("getrange 3: %v", err)
	}

	// 5. Boundary 1 repeated GetAttr / Lookup / ReadDir / RedundantOpenStat.
	rec.RecordGetAttr(1, 10*time.Microsecond, nil)
	rec.RecordGetAttr(1, 20*time.Microsecond, nil)
	rec.RecordOpenStat(1, 15*time.Microsecond, nil)
	rec.RecordLookup(1, "a", 2, 12*time.Microsecond, nil)
	rec.RecordLookup(1, "a", 2, 18*time.Microsecond, nil)
	rec.RecordReadDir(1, 25*time.Microsecond, nil)
	rec.RecordReadDir(1, 30*time.Microsecond, nil)

	snap := rec.Snapshot()
	if snap.RedundancyCounts[RedundancyRepeatedShardRead] != 2 {
		t.Fatalf("RepeatedShardRead = %d, want 2", snap.RedundancyCounts[RedundancyRepeatedShardRead])
	}
	if snap.RedundancyCounts[RedundancyRepeatedHeadRead] != 1 {
		t.Fatalf("RepeatedHeadRead = %d, want 1", snap.RedundancyCounts[RedundancyRepeatedHeadRead])
	}
	if snap.RedundancyCounts[RedundancyPrePutGet] != 1 {
		t.Fatalf("RedundancyPrePutGet = %d, want 1", snap.RedundancyCounts[RedundancyPrePutGet])
	}
	if snap.RedundancyCounts[RedundancyRepeatedList] != 1 {
		t.Fatalf("RedundancyRepeatedList = %d, want 1", snap.RedundancyCounts[RedundancyRepeatedList])
	}
	if snap.RedundancyCounts[RedundancyUncoalescedRangeGet] != 2 {
		t.Fatalf("UncoalescedRangeGet = %d, want 2", snap.RedundancyCounts[RedundancyUncoalescedRangeGet])
	}
	if snap.RedundancyCounts[RedundancyRepeatedGetAttr] != 1 {
		t.Fatalf("RepeatedGetAttr = %d, want 1", snap.RedundancyCounts[RedundancyRepeatedGetAttr])
	}
	if snap.RedundancyCounts[RedundancyRedundantOpenStat] != 1 {
		t.Fatalf("RedundantOpenStat = %d, want 1", snap.RedundancyCounts[RedundancyRedundantOpenStat])
	}
	if snap.RedundancyCounts[RedundancyRepeatedLookup] != 1 {
		t.Fatalf("RepeatedLookup = %d, want 1", snap.RedundancyCounts[RedundancyRepeatedLookup])
	}
	if snap.RedundancyCounts[RedundancyRepeatedReadDir] != 1 {
		t.Fatalf("RepeatedReadDir = %d, want 1", snap.RedundancyCounts[RedundancyRepeatedReadDir])
	}

	lat := snap.OpLatencies[OpGetAttr]
	if lat.Count != 3 || lat.MinUs > lat.P50Us || lat.P50Us > lat.P95Us || lat.P95Us > lat.P99Us || lat.P99Us > lat.MaxUs {
		t.Fatalf("invalid latency stats ordering: %+v", lat)
	}
}

func TestCASAuthorityAndFencedHeadMonotonicity(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	rec := NewAuditRecorder()
	inst := NewInstrumentedBackend(mem, rec)

	optCfg := DefaultOptimizerConfig()
	optCfg.EnableHeadVersionCache = true
	optCfg.HeadCacheTTL = 5 * time.Second
	opt := NewOptimizedBackend(inst, optCfg)

	var invalidations atomic.Int64
	opt.OnHeadInvalidation(func() { invalidations.Add(1) })

	// 1. Writer 1 owns Round 1 via canonical pkg/fence.Store over OptimizedBackend.
	f1 := fence.New(opt, 64<<10)
	if err := f1.Create(ctx, "head", []byte("genesis")); err != nil {
		t.Fatalf("f1 create: %v", err)
	}
	if err := f1.Put(ctx, "head", []byte("r1-v1"), 1); err != nil {
		t.Fatalf("f1 put r1-v1: %v", err)
	}
	if err := f1.Put(ctx, "head", []byte("r1-v2"), 1); err != nil {
		t.Fatalf("f1 put r1-v2: %v", err)
	}

	// Verify OptimizedBackend holds a cached Head version now.
	if _, ok := opt.CachedHeadVersion(); !ok {
		t.Fatalf("expected cached head version after Writer 1 commits")
	}

	// 2. Writer 2 takes over at Round 2 directly on the authoritative store.
	f2 := fence.New(mem, 64<<10)
	if err := f2.Claim(ctx, "head", 2, nil); err != nil {
		t.Fatalf("f2 claim round 2: %v", err)
	}
	if err := f2.Put(ctx, "head", []byte("r2-authoritative"), 2); err != nil {
		t.Fatalf("f2 put r2-authoritative: %v", err)
	}

	// 3. Deposed Writer 1 tries to commit at Round 1 through OptimizedBackend:
	//    MUST fail with fence.ErrStaleRound, invalidate cache, and leave Writer 2 untouched!
	err := f1.Put(ctx, "head", []byte("r1-stale-clobber"), 1)
	if !errors.Is(err, fence.ErrStaleRound) {
		t.Fatalf("deposed writer put err = %v, want fence.ErrStaleRound", err)
	}
	if opt.Stats().HeadCASConflicts == 0 {
		t.Fatalf("expected HeadCASConflicts > 0 after deposed writer conflict")
	}
	if invalidations.Load() == 0 {
		t.Fatalf("expected OnHeadInvalidation callback to fire on CAS conflict")
	}

	// Confirm the authoritative store holds Writer 2's exact bytes and Round 2.
	payload, round, err := f2.Get(ctx, "head")
	if err != nil {
		t.Fatalf("f2 get: %v", err)
	}
	if round != 2 || string(payload) != "r2-authoritative" {
		t.Fatalf("store corrupted: round=%d payload=%q, want round=2 payload=r2-authoritative", round, payload)
	}

	// 4. Verify ConditionalPutHead skips redundant pre-PUT GET in steady state,
	//    and retries cleanly on external conflict.
	rec.Reset()
	mem2 := storetest.NewMem()
	inst2 := NewInstrumentedBackend(mem2, rec)
	opt2 := NewOptimizedBackend(inst2, optCfg)
	if _, err := opt2.PutIfAbsent(ctx, "head", []byte("init")); err != nil {
		t.Fatalf("init head: %v", err)
	}
	rec.Reset()
	for i := range 5 {
		if _, err := opt2.ConditionalPutHead(ctx, 64<<10, []byte(fmt.Sprintf("seq-%d", i)), nil); err != nil {
			t.Fatalf("steady conditional put %d: %v", i, err)
		}
	}
	snap := rec.Snapshot()
	if snap.OpCounts[OpBackendGet] != 0 || snap.OpCounts[OpBackendConditionalPut] != 5 {
		t.Fatalf("steady state expected 0 GET + 5 ConditionalPut, got GET=%d CondPut=%d",
			snap.OpCounts[OpBackendGet], snap.OpCounts[OpBackendConditionalPut])
	}

	// Inject external write -> ConditionalPutHead must detect ErrConflict,
	// run validate against authoritative bytes, and succeed on retry.
	_, curV, err := mem2.Get(ctx, "head", 64<<10)
	if err != nil {
		t.Fatalf("mem2 get: %v", err)
	}
	if _, err := mem2.PutIfVersion(ctx, "head", []byte("external-writer"), curV); err != nil {
		t.Fatalf("external put: %v", err)
	}
	var sawExternal string
	if _, err := opt2.ConditionalPutHead(ctx, 64<<10, []byte("after-external"), func(cur []byte) error {
		sawExternal = string(cur)
		return nil
	}); err != nil {
		t.Fatalf("conditional put after external conflict: %v", err)
	}
	if sawExternal != "external-writer" {
		t.Fatalf("validate saw %q, want %q", sawExternal, "external-writer")
	}
}

func TestLocalMutationImmediateInvalidationAndTTL(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cipher := testCipher(t)
	rawBE, err := openFileBackend(ctx, filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("open file backend: %v", err)
	}
	if err := vfs.Create(ctx, vfs.Config{Backend: rawBE, Cipher: cipher}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend: rawBE,
		Cipher:  cipher,
		Round:   1,
		Journal: filepath.Join(dir, "journal"),
	})
	if err != nil {
		t.Fatalf("attach volume: %v", err)
	}
	defer vol.Close()

	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	rec := NewAuditRecorder()
	bridge := NewGoferBridge(vol, rec)
	cfg := DefaultOptimizerConfig()
	cfg.AttrTimeout = 2 * time.Second
	cfg.EntryTimeout = 2 * time.Second
	cfg.DirCacheTTL = 2 * time.Second
	cfg.SkipRedundantOpenStat = true
	cfg.Clock = clock
	cached := NewCachedGoferBridge(bridge, cfg)

	// Create file and warm caches.
	in, f, err := cached.Create(ctx, vfs.RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := cached.Write(ctx, f, in.ID, []byte("hello"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Immediate Lookup/Resolve after Write MUST reflect size 5 (never stale size 0 from Create).
	lookedUp, err := cached.Lookup(ctx, vfs.RootInode, "hello.txt")
	if err != nil || lookedUp.Size != 5 {
		t.Fatalf("Lookup after write = (size=%d, err=%v), want size=5", lookedUp.Size, err)
	}
	readBack, err := readVFSFile(ctx, cached, lookedUp)
	if err != nil || string(readBack) != "hello" {
		t.Fatalf("readVFSFile after Create+Write+Lookup = (%q, %v), want %q", readBack, err, "hello")
	}
	// Immediate GetAttr after Write MUST reflect size 5 (not stale size 0).
	got, err := cached.GetAttr(ctx, in.ID)
	if err != nil || got.Size != 5 {
		t.Fatalf("GetAttr after write = (size=%d, err=%v), want size=5", got.Size, err)
	}

	// Second GetAttr hits cache (zero new VFS crossings).
	before := rec.Snapshot().OpCounts[OpGetAttr]
	if _, err := cached.GetAttr(ctx, in.ID); err != nil {
		t.Fatalf("cached GetAttr: %v", err)
	}
	if rec.Snapshot().OpCounts[OpGetAttr] != before {
		t.Fatalf("expected cached GetAttr hit without VFS crossing")
	}

	// Open with SkipRedundantOpenStat skips vol.Stat on cached regular file.
	f2, err := cached.Open(ctx, in.ID)
	if err != nil {
		t.Fatalf("cached Open: %v", err)
	}
	_ = f2.Close()
	if cached.Stats().OpenStatSkipped != 2 {
		t.Fatalf("OpenStatSkipped = %d, want 2", cached.Stats().OpenStatSkipped)
	}

	// Open on cached directory still returns vfs.ErrIsDir.
	if _, err := cached.GetAttr(ctx, vfs.RootInode); err != nil {
		t.Fatalf("GetAttr root: %v", err)
	}
	if _, err := cached.Open(ctx, vfs.RootInode); !errors.Is(err, vfs.ErrIsDir) {
		t.Fatalf("Open dir err = %v, want vfs.ErrIsDir", err)
	}

	// ReadDir cache hit, then immediate invalidation on Remove.
	ents1, _, err := cached.ReadDir(ctx, vfs.RootInode)
	if err != nil || len(ents1) != 1 {
		t.Fatalf("ReadDir 1 = %v, %v", ents1, err)
	}
	rdBefore := rec.Snapshot().OpCounts[OpReadDir]
	ents2, _, err := cached.ReadDir(ctx, vfs.RootInode)
	if err != nil || len(ents2) != 1 || rec.Snapshot().OpCounts[OpReadDir] != rdBefore {
		t.Fatalf("expected cached ReadDir hit")
	}
	_ = f.Close()
	if err := cached.Remove(ctx, vfs.RootInode, "hello.txt"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	ents3, _, err := cached.ReadDir(ctx, vfs.RootInode)
	if err != nil || len(ents3) != 0 {
		t.Fatalf("ReadDir after Remove = %v (err=%v), want empty", ents3, err)
	}
	if _, err := cached.Lookup(ctx, vfs.RootInode, "hello.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Lookup after Remove = %v, want fs.ErrNotExist", err)
	}

	// Deterministic TTL expiry: advance past AttrTimeout -> forces fresh VFS crossing.
	before = rec.Snapshot().OpCounts[OpGetAttr]
	advance(3 * time.Second)
	if _, err := cached.GetAttr(ctx, vfs.RootInode); err != nil {
		t.Fatalf("GetAttr after TTL expiry: %v", err)
	}
	if rec.Snapshot().OpCounts[OpGetAttr] != before+1 {
		t.Fatalf("expected VFS crossing after AttrTimeout expiry")
	}
}

func TestShardCacheIntegrityAndListInvalidation(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	rec := NewAuditRecorder()
	inst := NewInstrumentedBackend(mem, rec)
	opt := NewOptimizedBackend(inst, DefaultOptimizerConfig())

	// 1. Valid content-addressed shard is cached after first Get.
	validBytes := []byte("valid-immutable-shard-bytes")
	validKey := makeShardKey(validBytes)
	if err := mem.Put(ctx, validKey, validBytes); err != nil {
		t.Fatalf("put valid shard: %v", err)
	}
	got1, _, err := opt.Get(ctx, validKey, 4096)
	if err != nil || !bytes.Equal(got1, validBytes) {
		t.Fatalf("first Get failed: %v", err)
	}
	// Mutate caller's returned slice -> must NOT corrupt cache!
	got1[0] ^= 0xff

	got2, _, err := opt.Get(ctx, validKey, 4096)
	if err != nil || !bytes.Equal(got2, validBytes) {
		t.Fatalf("second Get corrupted or failed: got=%q err=%v", got2, err)
	}
	if rec.Snapshot().OpCounts[OpBackendGet] != 1 {
		t.Fatalf("expected 1 backend Get for cached shard, got %d", rec.Snapshot().OpCounts[OpBackendGet])
	}
	// Max bound still enforced on cache hit.
	if _, _, err := opt.Get(ctx, validKey, 4); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("Get with tiny max err = %v, want backend.ErrTooLarge", err)
	}

	// 2. Tampered bytes under a content-addressed meta/ key fail closed and are NEVER cached.
	tamperedKey := makeShardKey([]byte("expected-authentic-bytes"))
	if err := mem.Put(ctx, tamperedKey, []byte("corrupted-tampered-bytes")); err != nil {
		t.Fatalf("put tampered: %v", err)
	}
	if _, _, err := opt.Get(ctx, tamperedKey, 4096); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("tampered shard Get err = %v, want ErrIntegrityMismatch", err)
	}
	if _, _, err := opt.Get(ctx, tamperedKey, 4096); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("second tampered shard Get err = %v, want ErrIntegrityMismatch", err)
	}

	// 3. List cache hits and immediate local mutation invalidation.
	n1, err := drainList(ctx, opt, "meta/")
	if err != nil || n1 != 2 {
		t.Fatalf("list 1 = (%d, %v), want 2", n1, err)
	}
	n2, err := drainList(ctx, opt, "meta/")
	if err != nil || n2 != 2 {
		t.Fatalf("list 2 = (%d, %v), want 2", n2, err)
	}
	if rec.Snapshot().OpCounts[OpBackendList] != 1 {
		t.Fatalf("expected 1 backend List (second hit cache), got %d", rec.Snapshot().OpCounts[OpBackendList])
	}
	// Local Put under "meta/" must immediately invalidate the cached listing.
	extra := []byte("third-shard")
	if err := opt.Put(ctx, makeShardKey(extra), extra); err != nil {
		t.Fatalf("put extra: %v", err)
	}
	n3, err := drainList(ctx, opt, "meta/")
	if err != nil || n3 != 3 {
		t.Fatalf("list after Put = (%d, %v), want 3", n3, err)
	}
}

func TestCoalesceRangesOverflowAndPackEOFClamping(t *testing.T) {
	// 1. Overflow and invalid inputs MUST be rejected.
	invalidCases := [][]RangeRequest{
		{{Off: -1, Len: 10}},
		{{Off: 0, Len: -5}},
		{{Off: math.MaxInt64 - 5, Len: 10}},
	}
	for i, tc := range invalidCases {
		if _, err := CoalesceRanges(tc, 1024, 4096); !errors.Is(err, ErrInvalidRange) {
			t.Fatalf("case %d: err = %v, want ErrInvalidRange", i, err)
		}
	}
	if _, err := CoalesceRanges([]RangeRequest{{Off: 0, Len: 10}}, -1, 100); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("negative maxGap err = %v, want ErrInvalidRange", err)
	}
	if _, err := CoalesceRanges([]RangeRequest{{Off: 0, Len: 10}}, 0, 0); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("zero maxWindow err = %v, want ErrInvalidRange", err)
	}

	// 2. Unsorted, overlapping, gap-bridged, and window-split ranges.
	reqs := []RangeRequest{
		{Off: 200, Len: 50},   // idx 0 (bridges with 0..100 if gap<=100 -> 0..250)
		{Off: 0, Len: 100},    // idx 1
		{Off: 50, Len: 80},    // idx 2 (overlaps 0..100 -> 0..130)
		{Off: 1000, Len: 40},  // idx 3 (gap > 100 -> new span)
		{Off: 2000, Len: 600}, // idx 4 (larger than maxWindow=500 -> own span intact)
	}
	spans, err := CoalesceRanges(reqs, 100, 500)
	if err != nil {
		t.Fatalf("CoalesceRanges: %v", err)
	}
	if len(spans) != 3 {
		t.Fatalf("got %d spans, want 3: %+v", len(spans), spans)
	}
	if spans[0].Off != 0 || spans[0].Len != 250 || len(spans[0].Members) != 3 {
		t.Fatalf("span[0] = %+v, want off=0 len=250 members=3", spans[0])
	}
	if spans[1].Off != 1000 || spans[1].Len != 40 {
		t.Fatalf("span[1] = %+v, want off=1000 len=40", spans[1])
	}
	if spans[2].Off != 2000 || spans[2].Len != 600 {
		t.Fatalf("span[2] = %+v, want off=2000 len=600", spans[2])
	}

	// 3. GetRanges + adaptive GetRange EOF clamping against real backend.
	ctx := context.Background()
	mem := storetest.NewMem()
	rec := NewAuditRecorder()
	inst := NewInstrumentedBackend(mem, rec)
	cfg := DefaultOptimizerConfig()
	cfg.ReadAheadWindow = 512 << 10
	opt := NewOptimizedBackend(inst, cfg)

	packData := deterministicBytes(4242, 300<<10) // smaller than ReadAheadWindow (512 KiB)!
	packKey := "pack/cc/fedcba9876543210fedcba9876543210"
	if err := mem.Put(ctx, packKey, packData); err != nil {
		t.Fatalf("put pack: %v", err)
	}

	// Batch GetRanges merges 4 adjacent reads into 1 backend GetRange and preserves order.
	batchReqs := []RangeRequest{
		{Off: 48 << 10, Len: 16 << 10},
		{Off: 0, Len: 16 << 10},
		{Off: 32 << 10, Len: 16 << 10},
		{Off: 16 << 10, Len: 16 << 10},
	}
	gotBatch, err := opt.GetRanges(ctx, packKey, batchReqs)
	if err != nil {
		t.Fatalf("GetRanges: %v", err)
	}
	for i, r := range batchReqs {
		want := packData[r.Off : r.Off+r.Len]
		if !bytes.Equal(gotBatch[i], want) {
			t.Fatalf("batch[%d] mismatch", i)
		}
	}
	if rec.Snapshot().OpCounts[OpBackendRangeGet] != 1 {
		t.Fatalf("expected 1 coalesced RangeGET for 4 adjacent ranges, got %d", rec.Snapshot().OpCounts[OpBackendRangeGet])
	}

	// Sequential GetRange near EOF clamps cleanly to 300 KiB (never errors past EOF!).
	rec.Reset()
	opt2 := NewOptimizedBackend(inst, cfg)
	const chunk = 32 << 10
	for off := int64(0); off+chunk <= int64(len(packData)); off += chunk {
		got, err := opt2.GetRange(ctx, packKey, off, chunk)
		if err != nil {
			t.Fatalf("GetRange off=%d: %v", off, err)
		}
		if !bytes.Equal(got, packData[off:off+chunk]) {
			t.Fatalf("GetRange off=%d data mismatch", off)
		}
	}
	// 9 sequential 32 KiB reads -> 1 initial read + 1 clamped read-ahead = 2 backend RangeGETs!
	if got := rec.Snapshot().OpCounts[OpBackendRangeGet]; got > 2 {
		t.Fatalf("sequential GetRange issued %d backend RangeGETs, want <= 2", got)
	}

	// Out-of-bounds GetRange still returns an error.
	if _, err := opt2.GetRange(ctx, packKey, int64(len(packData))-10, 20); err == nil {
		t.Fatalf("expected error for out-of-bounds GetRange past EOF")
	}
}

func TestConcurrentAccessRaceFree(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cipher := testCipher(t)
	rawBE, err := openFileBackend(ctx, filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	rec := NewAuditRecorder()
	inst := NewInstrumentedBackend(rawBE, rec)
	opt := NewOptimizedBackend(inst, DefaultOptimizerConfig())

	if err := vfs.Create(ctx, vfs.Config{Backend: opt, Cipher: cipher}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend: opt,
		Cipher:  cipher,
		Round:   1,
		Journal: filepath.Join(dir, "journal"),
	})
	if err != nil {
		t.Fatalf("attach volume: %v", err)
	}
	defer vol.Close()

	bridge := NewGoferBridge(vol, rec)
	cached := NewCachedGoferBridge(bridge, DefaultOptimizerConfig())
	opt.OnHeadInvalidation(cached.InvalidateAll)

	// Seed a few files and a pack object.
	for i := range 8 {
		if err := writeVFSFile(ctx, cached, vfs.RootInode, fmt.Sprintf("f-%d.txt", i), deterministicBytes(uint64(i+1), 4096)); err != nil {
			t.Fatalf("seed file %d: %v", i, err)
		}
	}
	packKey := "pack/dd/00112233445566778899aabbccddeeff"
	if err := opt.Put(ctx, packKey, deterministicBytes(99, 256<<10)); err != nil {
		t.Fatalf("seed pack: %v", err)
	}

	var wg sync.WaitGroup
	const workers = 16
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for iter := range 20 {
				name := fmt.Sprintf("f-%d.txt", (id+iter)%8)
				if in, err := cached.Lookup(ctx, vfs.RootInode, name); err == nil {
					_, _ = cached.GetAttr(ctx, in.ID)
				}
				_, _, _ = cached.ReadDir(ctx, vfs.RootInode)
				off := int64((iter%8)*16) << 10
				_, _ = opt.GetRange(ctx, packKey, off, 16<<10)
				if iter%5 == 0 {
					cached.InvalidateAll()
				}
				if id == 0 && iter%4 == 0 {
					if in, err := cached.Lookup(ctx, vfs.RootInode, name); err == nil {
						if fh, err := cached.Open(ctx, in.ID); err == nil {
							_, _ = cached.Write(ctx, fh, in.ID, []byte("updated"), 0)
							_ = fh.Close()
						}
					}
				}
			}
		}(w)
	}
	wg.Wait()
	if snap := rec.Snapshot(); snap.TotalRequests == 0 {
		t.Fatalf("expected non-zero recorded requests")
	}
}

func TestNegativeMultiWriterRegime(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultSuiteConfig()
	cfg.SimulatedRTT = 0 // fast deterministic unit test
	rep, err := runNegativeRegime(ctx, t.TempDir(), testCipher(t), cfg)
	if err != nil {
		t.Fatalf("runNegativeRegime: %v", err)
	}
	if !rep.CASAuthorityPreserved {
		t.Fatalf("CASAuthorityPreserved = false")
	}
	if rep.CachedContendedBackendReqs <= rep.UncachedCASBackendReqs {
		t.Fatalf("expected multi-writer contention to increase backend reqs: cached=%d uncached=%d",
			rep.CachedContendedBackendReqs, rep.UncachedCASBackendReqs)
	}
	if rep.CachedContendedConflicts != int64(rep.CASIterations) {
		t.Fatalf("conflicts = %d, want %d", rep.CachedContendedConflicts, rep.CASIterations)
	}
	if rep.StaleReadsBeforeInvalidation != 3 || !rep.FreshAfterInvalidation {
		t.Fatalf("TTL staleness / invalidation recovery mismatch: %+v", rep)
	}
	if rep.AggressiveReadAmplificationX <= 10.0 {
		t.Fatalf("expected high read amplification for aggressive read-ahead on random reads, got %.2fx",
			rep.AggressiveReadAmplificationX)
	}
	if rep.AdaptiveReadAmplificationX > 1.05 {
		t.Fatalf("expected adaptive coalescing to avoid read amplification on random reads, got %.2fx",
			rep.AdaptiveReadAmplificationX)
	}
}

func TestEndToEndFourWorkloads(t *testing.T) {
	ctx := context.Background()
	cfg := SuiteConfig{
		ScratchDir:    t.TempDir(),
		SimulatedRTT:  0, // fast unit-test execution
		CheckoutFiles: 200,
		NpmPackages:   12,
		GoPackages:    6,
	}
	rep, err := RunSuite(ctx, cfg)
	if err != nil {
		t.Fatalf("RunSuite: %v", err)
	}
	if len(rep.Workloads) != 4 {
		t.Fatalf("got %d workloads, want 4", len(rep.Workloads))
	}
	wantNames := []string{"npm_ci", "checkout_5000", "git_status", "go_build"}
	for i, w := range rep.Workloads {
		if w.Name != wantNames[i] {
			t.Fatalf("workload[%d] = %q, want %q", i, w.Name, wantNames[i])
		}
		if w.PlainNVMe.AppOps == 0 || w.Unoptimized.AppOps == 0 || w.Optimized.AppOps == 0 {
			t.Fatalf("workload %s has zero app ops", w.Name)
		}
		if w.Optimized.Boundary.FUSEBoundaryRequests >= w.Unoptimized.Boundary.FUSEBoundaryRequests {
			t.Fatalf("workload %s did not reduce FUSE boundary requests: unopt=%d opt=%d",
				w.Name, w.Unoptimized.Boundary.FUSEBoundaryRequests, w.Optimized.Boundary.FUSEBoundaryRequests)
		}
		if w.Optimized.Boundary.BackendBoundaryRequests >= w.Unoptimized.Boundary.BackendBoundaryRequests {
			t.Fatalf("workload %s did not reduce Backend boundary requests: unopt=%d opt=%d",
				w.Name, w.Unoptimized.Boundary.BackendBoundaryRequests, w.Optimized.Boundary.BackendBoundaryRequests)
		}
	}
}
