// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/backend"
)

func raskTestKey(t testing.TB) *age.HybridIdentity {
	t.Helper()
	k, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// raskScratchDir returns a non-volatile temporary directory suitable for
// journal.Files (falling back from t.TempDir() if OS temp is tmpfs).
func raskScratchDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	probe, err := journal.Files(filepath.Join(dir, "probe"))
	if err == nil {
		_ = probe.Close()
		_ = os.Remove(filepath.Join(dir, "probe"))
		return dir
	}
	if !errors.Is(err, journal.ErrVolatile) {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	alt, err := os.MkdirTemp(wd, ".scratch-rask-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(alt) })
	return alt
}

func assertRaskMatchesOracle(t testing.TB, what string, rask *RaskRunIndex, oracle *BaselineRunSlice) {
	t.Helper()
	if err := rask.verifyInvariants(); err != nil {
		t.Fatalf("%s: verifyInvariants: %v", what, err)
	}
	got := rask.AllRuns()
	want := oracle.AllRuns()
	if rask.Len() != oracle.Len() {
		t.Fatalf("%s: Len()=%d, want %d", what, rask.Len(), oracle.Len())
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s: runs differ: got %d runs %v, want %d runs %v",
			what, len(got), got[:min(8, len(got))], len(want), want[:min(8, len(want))])
	}
}

func TestRASKBoundaryAnd4GiBCases(t *testing.T) {
	rask := NewRaskRunIndex()
	oracle := NewBaselineRunSlice()

	// Empty & degenerate checks.
	if rask.Contains(0) || rask.Overlaps(0, 100) || rask.Overlaps(10, 10) || rask.Overlaps(20, 10) {
		t.Fatal("empty index returned true")
	}
	rask.PunchRange(0, 100)
	rask.Truncate(0)
	rask.InsertRange(10, 10)
	rask.InsertRange(20, 10)
	assertRaskMatchesOracle(t, "empty-degenerate", rask, oracle)

	// Exact 4 GiB boundary inside one compact leaf (maxEnd - base <= math.MaxUint32).
	rask.InsertRange(0, 10)
	oracle.InsertRange(0, 10)
	rask.InsertRange(uint64(math.MaxUint32)-10, uint64(math.MaxUint32))
	oracle.InsertRange(uint64(math.MaxUint32)-10, uint64(math.MaxUint32))
	assertRaskMatchesOracle(t, "exact-uint32-max", rask, oracle)
	if st := rask.Stats(); st.CompactLeaves != 1 || st.WideLeaves != 0 {
		t.Fatalf("expected 1 compact leaf at exact MaxUint32 span, got %+v", st)
	}

	// Extending by 1 byte crosses MaxUint32 -> splits or promotes cleanly.
	rask.InsertRange(uint64(math.MaxUint32), uint64(math.MaxUint32)+1)
	oracle.InsertRange(uint64(math.MaxUint32), uint64(math.MaxUint32)+1)
	assertRaskMatchesOracle(t, "cross-uint32-max", rask, oracle)

	// Punching back below MaxUint32 re-compacts if wide.
	rask.PunchRange(uint64(math.MaxUint32), uint64(math.MaxUint32)+1)
	oracle.PunchRange(uint64(math.MaxUint32), uint64(math.MaxUint32)+1)
	assertRaskMatchesOracle(t, "recompact-after-punch", rask, oracle)

	// Rebase left and right within leaf, plus split via punch on full 256-run leaf.
	r2 := NewRaskRunIndex()
	o2 := NewBaselineRunSlice()
	const baseOff = uint64(1)<<50 - 10_000_000 // near 1 PiB
	for i := range 256 {
		a := baseOff + uint64(i)*100
		b := a + 80
		r2.InsertRange(a, b)
		o2.InsertRange(a, b)
	}
	assertRaskMatchesOracle(t, "full-256-leaf", r2, o2)
	// Punch middle of run 0 -> splits full 256-run leaf into 257 runs, AND shifts base!
	r2.PunchRange(baseOff+10, baseOff+20)
	o2.PunchRange(baseOff+10, baseOff+20)
	assertRaskMatchesOracle(t, "split-full-256-via-punch", r2, o2)
	// Punch leading run 0 completely -> rebases leaf.base!
	r2.PunchRange(baseOff, baseOff+15)
	o2.PunchRange(baseOff, baseOff+15)
	assertRaskMatchesOracle(t, "punch-leading-rebase", r2, o2)
	// Insert before current base -> rebases left!
	r2.InsertRange(baseOff-500, baseOff-400)
	o2.InsertRange(baseOff-500, baseOff-400)
	assertRaskMatchesOracle(t, "insert-before-base", r2, o2)

	// Cross-leaf coalescing bridging multiple leaves.
	r3 := NewRaskRunIndex()
	o3 := NewBaselineRunSlice()
	for i := range 700 {
		a := uint64(i) * 20
		b := a + 10
		r3.InsertRange(a, b)
		o3.InsertRange(a, b)
	}
	assertRaskMatchesOracle(t, "multi-leaf-setup", r3, o3)
	// Bridge leaves cleanly across middle leaves.
	r3.InsertRange(2000+10, 12000)
	o3.InsertRange(2000+10, 12000)
	assertRaskMatchesOracle(t, "multi-leaf-bridge", r3, o3)

	// MaxSize and math.MaxUint64 edge ranges.
	r4 := NewRaskRunIndex()
	o4 := NewBaselineRunSlice()
	r4.InsertRange(MaxSize-4096, MaxSize)
	o4.InsertRange(MaxSize-4096, MaxSize)
	r4.InsertRange(0, math.MaxUint64)
	o4.InsertRange(0, math.MaxUint64)
	assertRaskMatchesOracle(t, "max-uint64-full", r4, o4)
	if !r4.Contains(math.MaxUint64-1) || r4.Contains(math.MaxUint64) {
		t.Fatalf("Contains at math.MaxUint64 boundary failed")
	}
	r4.Truncate(MaxSize)
	o4.Truncate(MaxSize)
	assertRaskMatchesOracle(t, "truncate-maxsize", r4, o4)
	r4.Truncate(0)
	o4.Truncate(0)
	assertRaskMatchesOracle(t, "truncate-zero", r4, o4)
}

func TestRASKDifferentialOracle1PiB(t *testing.T) {
	const pib = uint64(1) << 50
	const totalOps = 18_000
	opsPerPhase := totalOps / 3

	clusters := []uint64{
		0,
		1<<32 - 4096,
		1 << 40,
		pib/2 - 1<<31,
		pib - 64<<20,
	}

	for phase := range 3 {
		rng := rand.New(rand.NewPCG(uint64(5900+phase), 0xA11CE59))
		rask := NewRaskRunIndex()
		oracle := NewBaselineRunSlice()

		for step := range opsPerPhase {
			op := rng.IntN(100)
			switch {
			case op < 58:
				// Clustered 1 PiB writes + 4 GiB boundary straddles.
				var a, length uint64
				switch phase {
				case 0:
					base := clusters[rng.IntN(len(clusters))]
					a = base + uint64(rng.IntN(2<<20))
					length = uint64(1 + rng.IntN(64<<10))
				case 1:
					if rng.IntN(8) == 0 {
						// Huge scattered offsets & >4 GiB jumps.
						a = uint64(rng.IntN(1024)) * (pib / 1024)
						length = uint64(1 + rng.IntN(16<<20))
					} else {
						base := clusters[rng.IntN(len(clusters))]
						a = base + uint64(rng.IntN(1<<26))
						length = uint64(1 + rng.IntN(128<<10))
					}
				default:
					// Dense run generation forcing multi-level B+tree splits & cross-leaf coalescing.
					a = uint64(rng.IntN(4000)) * 512
					if rng.IntN(15) == 0 {
						a += pib / 2
					}
					length = uint64(64 + rng.IntN(2048))
				}
				b := min(a+length, pib)
				rask.InsertRange(a, b)
				oracle.InsertRange(a, b)

			case op < 90:
				var a, length uint64
				if oracle.Len() > 0 && rng.IntN(4) != 0 {
					rs := oracle.AllRuns()
					pick := rs[rng.IntN(len(rs))]
					span := pick.b - pick.a
					off := uint64(0)
					if span > 1 {
						off = rng.Uint64N(span)
					}
					a = pick.a + off
					if a > 256 && rng.IntN(3) == 0 {
						a -= uint64(rng.IntN(256))
					}
					length = uint64(1 + rng.IntN(32<<10))
				} else {
					base := clusters[rng.IntN(len(clusters))]
					a = base + uint64(rng.IntN(1<<22))
					length = uint64(1 + rng.IntN(1<<20))
				}
				b := min(a+length, pib)
				rask.PunchRange(a, b)
				oracle.PunchRange(a, b)

			case op < 95:
				// Coalescing adjacent/touching write or wide multi-leaf bridge.
				if rs := oracle.AllRuns(); len(rs) >= 2 {
					idx0 := rng.IntN(len(rs) - 1)
					a := rs[idx0].b
					b := rs[min(len(rs)-1, idx0+1+rng.IntN(5))].a
					if a < b && b-a < (1<<28) {
						rask.InsertRange(a, b)
						oracle.InsertRange(a, b)
					}
				}

			default:
				// Truncate (usually high, occasionally shrinking deeply).
				limit := pib
				if rng.IntN(4) == 0 && oracle.Len() > 0 {
					rs := oracle.AllRuns()
					pick := rs[rng.IntN(len(rs))]
					limit = pick.a + rng.Uint64N(max(1, pick.b-pick.a))
				}
				rask.Truncate(limit)
				oracle.Truncate(limit)
			}

			if step%100 == 99 || step == opsPerPhase-1 {
				assertRaskMatchesOracle(t, fmt.Sprintf("phase=%d step=%d", phase, step), rask, oracle)
				// Differential point & range queries.
				rs := oracle.AllRuns()
				for range 20 {
					var q uint64
					if len(rs) > 0 && rng.IntN(2) == 0 {
						pick := rs[rng.IntN(len(rs))]
						q = pick.a + rng.Uint64N(max(1, pick.b-pick.a))
					} else {
						q = clusters[rng.IntN(len(clusters))] + uint64(rng.IntN(1<<22))
					}
					if rask.Contains(q) != oracle.Contains(q) {
						t.Fatalf("phase=%d step=%d Contains(%d) mismatch: rask=%v oracle=%v",
							phase, step, q, rask.Contains(q), oracle.Contains(q))
					}
					qa := q
					qb := min(pib, qa+uint64(1+rng.IntN(1<<20)))
					if rask.Overlaps(qa, qb) != oracle.Overlaps(qa, qb) {
						t.Fatalf("phase=%d step=%d Overlaps([%d,%d)) mismatch: rask=%v oracle=%v",
							phase, step, qa, qb, rask.Overlaps(qa, qb), oracle.Overlaps(qa, qb))
					}
				}
			}
		}
	}
}

func TestRASKRealVolumeJournalReplay1PiB(t *testing.T) {
	ctx := context.Background()
	const pib = uint64(1) << 50
	jDir := raskScratchDir(t)

	be := storetest.NewMem()
	ciph := Hybrid(raskTestKey(t))
	if err := Create(ctx, Config{Backend: be, Cipher: ciph}); err != nil {
		t.Fatal(err)
	}
	v, err := Attach(ctx, Config{
		Backend:         be,
		Cipher:          ciph,
		Round:           1,
		Journal:         jDir,
		JournalCapacity: 64 << 20,
		Dirty:           1 << 30,
		Overlay:         1 << 20,
		Bound:           time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	in, err := v.Create(ctx, RootInode, "sparse-1pib.img", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(ctx, pib); err != nil {
		t.Fatal(err)
	}

	const secretMarker = "SECRET-RASK-PLAINTEXT-MARKER-59"
	// Sparse & strided bursts across 1 PiB + Punch holes + Truncate.
	bursts := 600
	if !testing.Short() {
		bursts = 1500
	}
	spots := make([]uint64, 0, bursts+8)
	// Include start, 4 GiB boundary, mid-PiB, and near-1 PiB tail.
	special := []struct {
		off  int64
		data []byte
	}{
		{0, []byte(secretMarker)},
		{1<<32 - 32, []byte("cross-4gib-boundary-payload-data-0123456789")},
		{int64(pib/2) - 4096, bytes.Repeat([]byte{0x5A}, 8192)},
		{int64(pib) - 128, []byte("tail-at-one-petabyte")},
	}
	for _, sp := range special {
		if _, err := f.WriteAt(ctx, sp.data, sp.off); err != nil {
			t.Fatal(err)
		}
		spots = append(spots, uint64(sp.off))
	}
	// Strided + clustered bursts across 1 PiB.
	const numClusters = 12
	for i := range bursts {
		cl := uint64(i % numClusters)
		idxInCl := uint64(i / numClusters)
		off := cl*(uint64(64)<<40) + idxInCl*8192 + 4096
		if off+64 > pib {
			continue
		}
		buf := []byte(fmt.Sprintf("burst-%05d-%s", i, secretMarker))
		if _, err := f.WriteAt(ctx, buf, int64(off)); err != nil {
			t.Fatal(err)
		}
		spots = append(spots, off)
		if i%5 == 2 {
			// Punch a hole inside the burst we just wrote.
			if err := f.Punch(ctx, off+8, 12); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Punch part of the 4 GiB boundary write and truncate last 16 bytes at 1 PiB.
	if err := f.Punch(ctx, 1<<32-16, 16); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(ctx, pib-16); err != nil {
		t.Fatal(err)
	}
	if err := v.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Capture live fileState intervals before closing without flush.
	v.mu.RLock()
	headJournal := v.head.Journal
	volID := v.id
	fsLive := v.files[in.ID]
	var liveWritten []run
	for _, s := range fsLive.lv.dirty {
		liveWritten = append(liveWritten, run{a: s.off, b: s.end()})
	}
	liveVoid := slices.Clone(fsLive.lv.void)
	v.mu.RUnlock()

	if headJournal != 0 {
		t.Fatalf("expected un-flushed volume head.Journal=0, got %d", headJournal)
	}
	_ = f.Close()
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}

	// Rule 1 check: journal segments on disk and backend objects never expose plaintext.
	places := journalPlaces(jDir, volID, 1)
	segEntries, err := os.ReadDir(places.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range segEntries {
		raw, err := os.ReadFile(filepath.Join(places.dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(secretMarker)) {
			t.Fatalf("Rule 1 violation: journal segment %s contains plaintext marker", e.Name())
		}
	}
	for objKey, raw := range be.Dump() {
		if bytes.Contains(raw, []byte(secretMarker)) {
			t.Fatalf("Rule 1 violation: backend object %s contains plaintext marker", objKey)
		}
	}

	// Reopen read-only Volume + ledger, replay via l.j.Replay + l.open + decodeChange.
	ro, err := Open(ctx, Config{
		Backend:         be,
		Cipher:          ciph,
		Journal:         jDir,
		JournalCapacity: 64 << 20,
		Bound:           time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	led, err := ro.openLedger(1, ro.head.Journal)
	if err != nil {
		t.Fatal(err)
	}
	defer led.j.Close()

	type decodedSpanOp struct {
		kind changeKind
		off  uint64
		size uint64
	}
	var extracted []decodedSpanOp
	err = led.j.Replay(func(r journal.Record) error {
		plain, err := led.open(r.Data)
		if err != nil {
			return err
		}
		ch, err := decodeChange(plain)
		if err != nil {
			return err
		}
		if ch.ino != in.ID {
			return nil
		}
		switch ch.kind {
		case changeWrite:
			extracted = append(extracted, decodedSpanOp{kind: changeWrite, off: ch.off, size: uint64(len(ch.data))})
		case changePunch:
			extracted = append(extracted, decodedSpanOp{kind: changePunch, off: ch.off, size: ch.size})
		case changeSize:
			extracted = append(extracted, decodedSpanOp{kind: changeSize, size: ch.size})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("journal Replay: %v", err)
	}
	if len(extracted) == 0 {
		t.Fatal("expected non-zero journaled records")
	}

	// Build BaselineRunSlice and RaskRunIndex from the exact replayed journal spans.
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	baseWrite := NewBaselineRunSlice()
	baseVoid := NewBaselineRunSlice()
	var curSize uint64
	for _, op := range extracted {
		switch op.kind {
		case changeWrite:
			baseWrite.InsertRange(op.off, op.off+op.size)
			curSize = max(curSize, op.off+op.size)
		case changePunch:
			a, b := min(op.off, curSize), min(op.off+op.size, curSize)
			if op.off+op.size < op.off {
				b = curSize
			}
			if a < b {
				baseWrite.PunchRange(a, b)
				baseVoid.InsertRange(a, b)
			}
		case changeSize:
			if op.size < curSize {
				baseWrite.Truncate(op.size)
				baseVoid.Truncate(op.size)
			}
			curSize = op.size
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m1)
	baseHeap := max(int64(0), int64(m1.HeapAlloc)-int64(m0.HeapAlloc))

	runtime.GC()
	var r0, r1 runtime.MemStats
	runtime.ReadMemStats(&r0)
	raskWrite := NewRaskRunIndex()
	raskVoid := NewRaskRunIndex()
	curSize = 0
	for _, op := range extracted {
		switch op.kind {
		case changeWrite:
			raskWrite.InsertRange(op.off, op.off+op.size)
			curSize = max(curSize, op.off+op.size)
		case changePunch:
			a, b := min(op.off, curSize), min(op.off+op.size, curSize)
			if op.off+op.size < op.off {
				b = curSize
			}
			if a < b {
				raskWrite.PunchRange(a, b)
				raskVoid.InsertRange(a, b)
			}
		case changeSize:
			if op.size < curSize {
				raskWrite.Truncate(op.size)
				raskVoid.Truncate(op.size)
			}
			curSize = op.size
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&r1)
	raskHeap := max(int64(0), int64(r1.HeapAlloc)-int64(r0.HeapAlloc))

	// 3-way equivalence: RaskRunIndex == BaselineRunSlice == live fileState.
	assertRaskMatchesOracle(t, "journal-replay-written", raskWrite, baseWrite)
	assertRaskMatchesOracle(t, "journal-replay-void", raskVoid, baseVoid)
	if !slices.Equal(raskWrite.AllRuns(), liveWritten) {
		t.Fatalf("replayed written runs (%d) differ from live fileState dirty runs (%d)",
			raskWrite.Len(), len(liveWritten))
	}
	if !slices.Equal(raskVoid.AllRuns(), liveVoid) {
		t.Fatalf("replayed void runs (%d) differ from live fileState void runs (%d)",
			raskVoid.Len(), len(liveVoid))
	}

	// Verify point & overlap queries across all spots.
	for _, s := range spots {
		if raskWrite.Contains(s) != baseWrite.Contains(s) {
			t.Fatalf("Contains(%d) mismatch", s)
		}
		if raskWrite.Overlaps(s, s+64) != baseWrite.Overlaps(s, s+64) {
			t.Fatalf("Overlaps([%d,%d)) mismatch", s, s+64)
		}
	}

	bMean, bP50, bP99 := benchPointLookups(baseWrite.Contains, spots)
	rMean, rP50, rP99 := benchPointLookups(raskWrite.Contains, spots)
	runtime.KeepAlive(baseWrite)
	runtime.KeepAlive(raskWrite)

	nRuns := max(1, raskWrite.Len())
	var minL, maxL int = 1 << 30, 0
	var sampleLens []int
	for l := raskWrite.first; l != nil; l = l.next {
		ln := l.len()
		minL = min(minL, ln)
		maxL = max(maxL, ln)
		if len(sampleLens) < 20 {
			sampleLens = append(sampleLens, ln)
		}
	}
	t.Logf("Real 1 PiB journal replay: records=%d writtenRuns=%d voidRuns=%d leafLen[min=%d,max=%d,first20=%v]", len(extracted), raskWrite.Len(), raskVoid.Len(), minL, maxL, sampleLens)
	t.Logf("  Baseline []run: struct=%d B (%.2f B/run) heap=%d B lookup mean=%.1fns p50=%.1fns p99=%.1fns",
		baseWrite.MemoryBytes(), float64(baseWrite.MemoryBytes())/float64(nRuns), baseHeap, bMean, bP50, bP99)
	t.Logf("  RASK B+tree   : struct=%d B (%.2f B/run) heap=%d B lookup mean=%.1fns p50=%.1fns p99=%.1fns stats=%+v",
		raskWrite.MemoryBytes(), float64(raskWrite.MemoryBytes())/float64(nRuns), raskHeap, rMean, rP50, rP99, raskWrite.Stats())
}

func TestRASKMultiRegimeCompressionAndTradeoffs(t *testing.T) {
	n := 50_000
	if testing.Short() {
		n = 20_000
	}

	// Regime A: Clustered 1 PiB sparse workload -> < 10 B/run compression!
	seqOps, qOffs, qRanges := genClusteredSeqOps(n)
	seqRes, err := measureSyntheticWorkload("synthetic_microbenchmark", "regime_a_clustered_seq", "Clustered 1 PiB seq", "", seqOps, qOffs, qRanges)
	if err != nil {
		t.Fatal(err)
	}
	if !seqRes.ExactMatch {
		t.Fatal("Regime A seq mismatch")
	}
	if seqRes.RaskStructuralBytesPerRun >= 10.0 {
		t.Fatalf("expected Regime A RaskStructuralBytesPerRun < 10.0 B/run, got %.2f B/run", seqRes.RaskStructuralBytesPerRun)
	}
	if seqRes.WideLeaves != 0 {
		t.Fatalf("expected 0 wide leaves in clustered regime, got %d", seqRes.WideLeaves)
	}

	randOps, qOffs2, qRanges2 := genClusteredRandPunchOps(n)
	randRes, err := measureSyntheticWorkload("synthetic_microbenchmark", "regime_a_clustered_rand", "Clustered 1 PiB rand+punch", "", randOps, qOffs2, qRanges2)
	if err != nil {
		t.Fatal(err)
	}
	if !randRes.ExactMatch {
		t.Fatal("Regime A rand mismatch")
	}
	if randRes.RaskStructuralBytesPerRun >= 10.0 {
		t.Fatalf("expected Regime A random+punch RaskStructuralBytesPerRun < 10.0 B/run, got %.2f B/run", randRes.RaskStructuralBytesPerRun)
	}

	// Regime B: Widely scattered (> 4 GiB stride) 1 PiB workload -> exact representation via wide promotion.
	wideOps, qOffs3, qRanges3 := genStridedOps(min(n, 10_000), 5<<30, 4096)
	wideRes, err := measureSyntheticWorkload("synthetic_microbenchmark", "regime_b_strided_gt4g", "5 GiB stride 1 PiB", "", wideOps, qOffs3, qRanges3)
	if err != nil {
		t.Fatal(err)
	}
	if !wideRes.ExactMatch {
		t.Fatal("Regime B wide mismatch")
	}
	if wideRes.WideLeaves == 0 {
		t.Fatalf("expected wide leaves in >4 GiB stride regime")
	}
	// Verify no 1-run leaf explosion (each wide leaf averages many runs, < 25 B/run).
	if wideRes.RaskStructuralBytesPerRun > 25.0 {
		t.Fatalf("unexpected leaf explosion in >4 GiB stride regime: %.2f B/run", wideRes.RaskStructuralBytesPerRun)
	}

	t.Logf("Regime A (clustered seq):  rask=%.2f B/run (heap=%d B) vs baseline=%.2f B/run (heap=%d B) (savings=%.1f%%)",
		seqRes.RaskStructuralBytesPerRun, seqRes.RaskRetainedHeapAllocBytes, seqRes.BaselineStructuralBytesPerRun, seqRes.BaselineRetainedHeapAllocBytes, seqRes.StructuralSavingsPct)
	t.Logf("Regime A (clustered rand): rask=%.2f B/run (heap=%d B) vs baseline=%.2f B/run (heap=%d B) (savings=%.1f%%, build %.2fms vs %.2fms)",
		randRes.RaskStructuralBytesPerRun, randRes.RaskRetainedHeapAllocBytes, randRes.BaselineStructuralBytesPerRun, randRes.BaselineRetainedHeapAllocBytes, randRes.StructuralSavingsPct, randRes.RaskBuildMs, randRes.BaselineBuildMs)
	t.Logf("Regime B (5 GiB stride):   rask=%.2f B/run (heap=%d B) vs baseline=%.2f B/run (heap=%d B) (wideLeaves=%d)",
		wideRes.RaskStructuralBytesPerRun, wideRes.RaskRetainedHeapAllocBytes, wideRes.BaselineStructuralBytesPerRun, wideRes.BaselineRetainedHeapAllocBytes, wideRes.WideLeaves)
}

func TestRASKMaxOffsetAndOverflowGuards(t *testing.T) {
	rask := NewRaskRunIndex()
	oracle := NewBaselineRunSlice()

	// 1. Degenerate wraparound / empty intervals must be ignored cleanly.
	rask.InsertRange(100, 100)
	rask.InsertRange(200, 100)
	rask.InsertRange(math.MaxUint64, 0)
	rask.InsertRange(math.MaxUint64, math.MaxUint64)
	if rask.Len() != 0 {
		t.Fatalf("expected empty index after degenerate intervals, got %d", rask.Len())
	}

	// 2. Exact 4 GiB boundary (1<<32 - 1 and 1<<32) adjacency & coalescing.
	const fourGiB = uint64(1) << 32
	rask.InsertRange(0, 16)
	oracle.InsertRange(0, 16)
	rask.InsertRange(fourGiB-16, fourGiB-1)
	oracle.InsertRange(fourGiB-16, fourGiB-1)
	assertRaskMatchesOracle(t, "4gib-minus-1", rask, oracle)

	// Touching run [1<<32 - 1, 1<<32) coalesces to [1<<32 - 16, 1<<32) which spans
	// exactly 1<<32 bytes from base 0 (end - base == 1<<32 > MaxUint32), triggering
	// gap-split into two compact leaves!
	rask.InsertRange(fourGiB-1, fourGiB)
	oracle.InsertRange(fourGiB-1, fourGiB)
	assertRaskMatchesOracle(t, "4gib-exact-touch", rask, oracle)

	// Crossing 4 GiB boundary with an adjacent run [1<<32, 1<<32 + 64).
	rask.InsertRange(fourGiB, fourGiB+64)
	oracle.InsertRange(fourGiB, fourGiB+64)
	assertRaskMatchesOracle(t, "4gib-cross-coalesce", rask, oracle)

	// 3. MaxSize (1<<62) and math.MaxUint64 - 1024 .. math.MaxUint64 boundary operations.
	rask.InsertRange(MaxSize-2048, MaxSize-1024)
	oracle.InsertRange(MaxSize-2048, MaxSize-1024)
	rask.InsertRange(MaxSize-1024, MaxSize)
	oracle.InsertRange(MaxSize-1024, MaxSize)
	rask.InsertRange(math.MaxUint64-1024, math.MaxUint64-512)
	oracle.InsertRange(math.MaxUint64-1024, math.MaxUint64-512)
	rask.InsertRange(math.MaxUint64-512, math.MaxUint64)
	oracle.InsertRange(math.MaxUint64-512, math.MaxUint64)
	assertRaskMatchesOracle(t, "max-offset-inserts", rask, oracle)

	// Verify Contains / Overlaps / RangeOverlaps at MaxSize and MaxUint64 edges.
	probeOffsets := []uint64{
		0, 15, 16,
		fourGiB - 17, fourGiB - 16, fourGiB - 1, fourGiB, fourGiB + 63, fourGiB + 64,
		MaxSize - 2049, MaxSize - 2048, MaxSize - 1, MaxSize,
		math.MaxUint64 - 1025, math.MaxUint64 - 1024, math.MaxUint64 - 1, math.MaxUint64,
	}
	for _, off := range probeOffsets {
		if rask.Contains(off) != oracle.Contains(off) {
			t.Fatalf("Contains(%d) mismatch at boundary", off)
		}
		hi := off + 32
		if hi < off {
			hi = math.MaxUint64
		}
		if rask.Overlaps(off, hi) != oracle.Overlaps(off, hi) {
			t.Fatalf("Overlaps([%d,%d)) mismatch at boundary", off, hi)
		}
	}

	var gotRange []run
	rask.RangeOverlaps(fourGiB-8, math.MaxUint64, func(r run) {
		gotRange = append(gotRange, r)
	})
	if len(gotRange) != 3 {
		t.Fatalf("expected 3 overlapping runs in [4GiB-8, MaxUint64), got %v", gotRange)
	}

	// Punch hole inside [math.MaxUint64-1024, math.MaxUint64) and truncate back to MaxSize.
	rask.PunchRange(math.MaxUint64-768, math.MaxUint64-256)
	oracle.PunchRange(math.MaxUint64-768, math.MaxUint64-256)
	assertRaskMatchesOracle(t, "punch-near-max-uint64", rask, oracle)

	rask.Truncate(MaxSize)
	oracle.Truncate(MaxSize)
	assertRaskMatchesOracle(t, "truncate-to-maxsize", rask, oracle)
}

func TestRASKSplitMergeChurn50k(t *testing.T) {
	const ops = 50_000
	rask := NewRaskRunIndex()
	oracle := NewBaselineRunSlice()
	rng := rand.New(rand.NewPCG(0x59_C409, 0x1344_2026))

	const clusters = 24
	const clusterStride = uint64(40) << 40 // 40 TiB apart across ~1 PiB

	for step := range ops {
		mode := step % 10
		switch {
		case mode < 5:
			// Clustered small insert (compact leaves & leaf splits).
			c := uint64(rng.IntN(clusters))
			off := c*clusterStride + uint64(rng.IntN(1<<22))*64
			length := uint64(32 + rng.IntN(256))
			rask.InsertRange(off, off+length)
			oracle.InsertRange(off, off+length)
		case mode < 7:
			// Hole punch inside an active cluster (splitting & shrinking leaves).
			c := uint64(rng.IntN(clusters))
			off := c*clusterStride + uint64(rng.IntN(1<<22))*64
			length := uint64(16 + rng.IntN(2048))
			rask.PunchRange(off, off+length)
			oracle.PunchRange(off, off+length)
		case mode == 7:
			// Cross-leaf coalescing bridge inside a cluster.
			c := uint64(rng.IntN(clusters))
			off := c*clusterStride + uint64(rng.IntN(1<<20))*64
			length := uint64(1 << 16)
			rask.InsertRange(off, off+length)
			oracle.InsertRange(off, off+length)
		case mode == 8:
			// Wide-leaf promotion (> 4 GiB span within cluster) followed by punch re-compaction.
			c := uint64(rng.IntN(clusters))
			wideOff := c*clusterStride + (uint64(5) << 30) + uint64(rng.IntN(1024))*4096
			rask.InsertRange(wideOff, wideOff+2048)
			oracle.InsertRange(wideOff, wideOff+2048)
			if step%40 == 8 {
				rask.PunchRange(c*clusterStride+(uint64(5)<<30), c*clusterStride+(uint64(5)<<30)+1024*4096+4096)
				oracle.PunchRange(c*clusterStride+(uint64(5)<<30), c*clusterStride+(uint64(5)<<30)+1024*4096+4096)
			}
		default:
			// Periodic high-offset truncate & re-populate of top cluster.
			if step%500 == 499 {
				cut := uint64(clusters-2) * clusterStride
				rask.Truncate(cut)
				oracle.Truncate(cut)
			} else {
				c := uint64(rng.IntN(clusters))
				off := c*clusterStride + uint64(rng.IntN(1<<22))*64
				rask.InsertRange(off, off+64)
				oracle.InsertRange(off, off+64)
			}
		}

		if step%5000 == 4999 || step == ops-1 {
			assertRaskMatchesOracle(t, fmt.Sprintf("churn-50k-step-%d", step), rask, oracle)
		}
	}
}

func TestRASKProductionFileLayerAdaptiveThresholdParity(t *testing.T) {
	ctx := context.Background()
	jDir := raskScratchDir(t)
	be := storetest.NewMem()
	ciph := Hybrid(raskTestKey(t))

	if err := Create(ctx, Config{Backend: be, Cipher: ciph}); err != nil {
		t.Fatal(err)
	}
	v, err := Attach(ctx, Config{
		Backend:         be,
		Cipher:          ciph,
		Round:           1,
		Journal:         jDir,
		JournalCapacity: 64 << 20,
		Dirty:           1 << 30,
		Overlay:         1 << 20,
		Bound:           time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	in, err := v.Create(ctx, RootInode, "adaptive-rask.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Reference model for [0, regionSize) plus a high 1 PiB tail region.
	const regionSize = 2 << 20 // 2 MiB dense region (> small=16KiB so chunked extent path runs)
	const pibOffset = (uint64(1) << 50) - 65536
	const totalSize = (uint64(1) << 50)
	refLow := make([]byte, regionSize)
	refHigh := make([]byte, 65536)

	for i := range refLow {
		refLow[i] = byte((i*31 + 17) & 0xff)
	}
	for i := range refHigh {
		refHigh[i] = byte((i*73 + 99) & 0xff)
	}

	if err := f.Truncate(ctx, totalSize); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(ctx, refLow, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(ctx, refHigh, int64(pibOffset)); err != nil {
		t.Fatal(err)
	}
	// Publish settled chunks so subsequent Punch operations populate layer.void -> layer.voidRask.
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Punch 1,200 disjoint holes across the settled 2 MiB region and 1 PiB tail,
	// crossing raskVoidPromoteThreshold (512).
	const totalPunches = 1200
	for i := range totalPunches {
		off := uint64(i) * 1536
		const length = 512
		if off+length <= regionSize {
			if err := f.Punch(ctx, off, length); err != nil {
				t.Fatal(err)
			}
			clear(refLow[off : off+length])
		}
		if i == raskVoidPromoteThreshold-2 {
			v.mu.RLock()
			hasRask := f.f.lv.voidRask != nil
			voidLen := len(f.f.lv.void)
			v.mu.RUnlock()
			if hasRask || voidLen != raskVoidPromoteThreshold-1 {
				t.Fatalf("before threshold: expected flat void slice len=%d, got hasRask=%v len=%d",
					raskVoidPromoteThreshold-1, hasRask, voidLen)
			}
		}
		if i == raskVoidPromoteThreshold {
			v.mu.RLock()
			hasRask := f.f.lv.voidRask != nil
			flatNil := f.f.lv.void == nil
			raskLen := f.f.lv.voidLen()
			v.mu.RUnlock()
			if !hasRask || !flatNil || raskLen != raskVoidPromoteThreshold+1 {
				t.Fatalf("at threshold: expected promoted voidRask (len=%d, flatNil=true), got hasRask=%v flatNil=%v len=%d",
					raskVoidPromoteThreshold+1, hasRask, flatNil, raskLen)
			}
		}
	}

	// Also punch holes in the 1 PiB tail and overwrite a few punched ranges.
	for i := range 20 {
		off := uint64(i) * 2048
		if err := f.Punch(ctx, pibOffset+off, 1024); err != nil {
			t.Fatal(err)
		}
		clear(refHigh[off : off+1024])
	}
	patch := []byte("REWRITTEN-OVER-RASK-VOID-HOLE-0123456789ABCDEF")
	for _, off := range []uint64{1536 + 64, 300*1536 + 128, 900*1536 + 256} {
		if _, err := f.WriteAt(ctx, patch, int64(off)); err != nil {
			t.Fatal(err)
		}
		copy(refLow[off:], patch)
	}

	// 1. Verify ReadAt BEFORE Flush (exercises body.plan -> layer.forEachVoid -> voidRask.RangeOverlaps).
	gotLow := make([]byte, regionSize)
	if _, err := f.ReadAt(ctx, gotLow, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotLow, refLow) {
		t.Fatal("ReadAt before Flush differed from reference model in low region")
	}
	gotHigh := make([]byte, len(refHigh))
	if _, err := f.ReadAt(ctx, gotHigh, int64(pibOffset)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotHigh, refHigh) {
		t.Fatal("ReadAt before Flush differed from reference model at 1 PiB tail")
	}

	// 2. Flush generation (exercises cut() -> snap.runs() & snap.voided() with s.fz.voidRask).
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// 3. Verify ReadAt AFTER Flush on settled extents.
	clear(gotLow)
	if _, err := f.ReadAt(ctx, gotLow, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotLow, refLow) {
		t.Fatal("ReadAt after Flush differed from reference model in low region")
	}
	clear(gotHigh)
	if _, err := f.ReadAt(ctx, gotHigh, int64(pibOffset)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotHigh, refHigh) {
		t.Fatal("ReadAt after Flush differed from reference model at 1 PiB tail")
	}

	// 4. Punch 600 more voids to promote again, then Truncate down to 64 KiB (< raskVoidDemoteThreshold = 128)
	// and verify clean demotion back to flat l.void []run.
	for i := range 600 {
		off := uint64(i) * 2048
		if err := f.Punch(ctx, off, 256); err != nil {
			t.Fatal(err)
		}
		if off+256 <= uint64(len(refLow)) {
			clear(refLow[off:min(uint64(len(refLow)), off+256)])
		}
	}
	v.mu.RLock()
	promotedAgain := f.f.lv.voidRask != nil
	v.mu.RUnlock()
	if !promotedAgain {
		t.Fatal("expected voidRask promotion on second punch wave")
	}

	const truncSize = 64 << 10 // 64 KiB -> only 32 punched runs remain (<= 128 demote threshold)
	if err := f.Truncate(ctx, truncSize); err != nil {
		t.Fatal(err)
	}
	refLow = refLow[:truncSize]

	v.mu.RLock()
	demotedToSlice := f.f.lv.voidRask == nil && len(f.f.lv.void) == 32
	v.mu.RUnlock()
	if !demotedToSlice {
		t.Fatalf("expected demotion to flat []run after Truncate(64KiB), got voidRask=%v len(void)=%d",
			f.f.lv.voidRask != nil, len(f.f.lv.void))
	}

	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// 5. Reopen read-only Volume from backend store and verify final settled bytes match refLow.
	ro, err := Open(ctx, Config{Backend: be, Cipher: ciph})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	roFile, err := ro.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer roFile.Close()
	gotFinal := make([]byte, truncSize)
	if _, err := roFile.ReadAt(ctx, gotFinal, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotFinal, refLow) {
		t.Fatal("fresh read-only Volume Open ReadAt differed from reference model after Truncate+Flush")
	}
}

func TestRASKBenchmarkSmoke(t *testing.T) {
	ctx := context.Background()
	jDir := raskScratchDir(t)
	mem := storetest.NewMem()
	rep, err := RunRASKBenchmark(ctx, RASKBenchmarkConfig{
		OpenBackend: func() (backend.Backend, error) { return mem, nil },
		JournalDir:  jDir,
		Short:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.JournalReplay.JournalRecords == 0 {
		t.Fatal("expected non-zero JournalRecords")
	}
	if len(rep.Workloads) != 6 {
		t.Fatalf("expected 6 workloads, got %d", len(rep.Workloads))
	}
	for _, w := range rep.Workloads {
		if !w.ExactMatch {
			t.Fatalf("workload %s failed ExactMatch", w.Name)
		}
	}
}
