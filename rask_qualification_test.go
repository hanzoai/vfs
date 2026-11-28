// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

type raskQualWorkload struct {
	name   string
	ops    []raskOp
	qOffs  []uint64
	qRange [][2]uint64
}

func genRandomJournalReplayOps(n int) ([]raskOp, []uint64, [][2]uint64) {
	const clusters = 16
	const clusterStride = uint64(50) << 40 // 50 TiB apart across ~800 TiB

	seed := uint64(0x517CC1B727220A95)
	nextRand := func() uint64 {
		seed += 0x9E3779B97F4A7C15
		z := seed
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}

	perm := make([]int, n)
	for i := range n {
		perm[i] = i
	}
	for i := n - 1; i > 0; i-- {
		j := int(nextRand() % uint64(i+1))
		perm[i], perm[j] = perm[j], perm[i]
	}

	perCluster := max(1, n/clusters)
	ops := make([]raskOp, 0, n+n/6+2)
	qOffs := make([]uint64, 0, min(4096, n*2))
	qRanges := make([][2]uint64, 0, min(4096, n))
	var maxExtent uint64

	for step, p := range perm {
		c := uint64(p / perCluster)
		j := uint64(p % perCluster)
		off := c*clusterStride + j*16384
		b := off + 4096
		ops = append(ops, raskOp{kind: raskOpInsert, a: off, b: b})
		if b > maxExtent {
			maxExtent = b
		}
		if step%7 == 5 {
			pa := off + 1024
			pb := off + 2048
			ops = append(ops, raskOp{kind: raskOpPunch, a: pa, b: pb})
		}
		if len(qOffs) < 4096 {
			qOffs = append(qOffs, off+512, off+1536)
			qRanges = append(qRanges, [2]uint64{off, off + 3072})
		}
	}

	// Simulate a journal truncate shedding the upper-most cluster tail.
	if maxExtent > clusterStride {
		ops = append(ops, raskOp{kind: raskOpTruncate, a: uint64(clusters-1)*clusterStride + 8192})
	}
	return ops, qOffs, qRanges
}

func applyOpsBaseline(ops []raskOp) *BaselineRunSlice {
	idx := NewBaselineRunSlice()
	for _, op := range ops {
		switch op.kind {
		case raskOpInsert:
			idx.InsertRange(op.a, op.b)
		case raskOpPunch:
			idx.PunchRange(op.a, op.b)
		case raskOpTruncate:
			idx.Truncate(op.a)
		}
	}
	return idx
}

func applyOpsRask(ops []raskOp) *RaskRunIndex {
	idx := NewRaskRunIndex()
	for _, op := range ops {
		switch op.kind {
		case raskOpInsert:
			idx.InsertRange(op.a, op.b)
		case raskOpPunch:
			idx.PunchRange(op.a, op.b)
		case raskOpTruncate:
			idx.Truncate(op.a)
		}
	}
	return idx
}

func qualificationWorkloads(n int) []raskQualWorkload {
	sOps, sQ, sR := genStridedOps(n, 10<<20, 4096)
	wOps, wQ, wR := genStridedOps(n, 5<<30, 4096)
	dOps, dQ, dR := genClusteredRandPunchOps(n)
	qOps, qQ, qR := genClusteredSeqOps(n)
	jOps, jQ, jR := genRandomJournalReplayOps(n)

	return []raskQualWorkload{
		{name: "Sparse_Sub4G", ops: sOps, qOffs: sQ, qRange: sR},
		{name: "Sparse_WideGt4G", ops: wOps, qOffs: wQ, qRange: wR},
		{name: "Dense_RandPunch", ops: dOps, qOffs: dQ, qRange: dR},
		{name: "Sequential_Append", ops: qOps, qOffs: qQ, qRange: qR},
		{name: "Random_JournalReplay", ops: jOps, qOffs: jQ, qRange: jR},
	}
}

// BenchmarkRASKQualification independently benchmarks shipping BaselineRunSlice
// ("Intervals") against RaskRunIndex ("RASK") across identical sparse, dense,
// sequential, and random journal-replay workloads using `go test -bench -benchmem -count=5`.
func BenchmarkRASKQualification(b *testing.B) {
	const opsN = 4096
	for _, w := range qualificationWorkloads(opsN) {
		b.Run(w.name, func(b *testing.B) {
			refBase := applyOpsBaseline(w.ops)
			refRask := applyOpsRask(w.ops)
			if !slices.Equal(refBase.AllRuns(), refRask.AllRuns()) {
				b.Fatalf("%s: parity mismatch baseline(%d) != rask(%d)", w.name, refBase.Len(), refRask.Len())
			}
			runs := max(1, refBase.Len())
			baseBPerRun := float64(refBase.StructuralBytes()) / float64(runs)
			raskBPerRun := float64(refRask.Stats().StructuralBytes) / float64(runs)

			b.Run("Intervals/Build", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(baseBPerRun, "struct_B/run")
				b.ReportMetric(float64(runs), "runs")
				for i := 0; i < b.N; i++ {
					idx := applyOpsBaseline(w.ops)
					raskBenchSink ^= uint64(idx.Len())
				}
			})
			b.Run("RASK/Build", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(raskBPerRun, "struct_B/run")
				b.ReportMetric(float64(runs), "runs")
				for i := 0; i < b.N; i++ {
					idx := applyOpsRask(w.ops)
					raskBenchSink ^= uint64(idx.Len())
				}
			})

			b.Run("Intervals/Contains", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(baseBPerRun, "struct_B/run")
				q := w.qOffs
				qn := len(q)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if refBase.Contains(q[i%qn]) {
						raskBenchSink++
					}
				}
			})
			b.Run("RASK/Contains", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(raskBPerRun, "struct_B/run")
				q := w.qOffs
				qn := len(q)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if refRask.Contains(q[i%qn]) {
						raskBenchSink++
					}
				}
			})

			b.Run("Intervals/Overlaps", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(baseBPerRun, "struct_B/run")
				qr := w.qRange
				qrn := len(qr)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r := qr[i%qrn]
					if refBase.Overlaps(r[0], r[1]) {
						raskBenchSink++
					}
				}
			})
			b.Run("RASK/Overlaps", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(raskBPerRun, "struct_B/run")
				qr := w.qRange
				qrn := len(qr)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r := qr[i%qrn]
					if refRask.Overlaps(r[0], r[1]) {
						raskBenchSink++
					}
				}
			})
		})
	}
}

type raskBatchMemResult struct {
	runs          int
	structBPerRun float64
	heapAllocB    float64
	heapInuseB    float64
	rssDeltaB     int64
}

func measureBatchBaseline(ops []raskOp, copies int) raskBatchMemResult {
	runtime.GC()
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	rss0 := processRSSBytes()

	keep := make([]*BaselineRunSlice, copies)
	for i := range copies {
		keep[i] = applyOpsBaseline(ops)
	}
	runtime.GC()
	runtime.GC()
	var m1Tmp runtime.MemStats
	runtime.ReadMemStats(&m1Tmp)
	m1 = m1Tmp
	rss1 := processRSSBytes()

	runs := max(1, keep[0].Len())
	structB := float64(keep[0].StructuralBytes()) / float64(runs)
	runtime.KeepAlive(keep)

	allocPerCopy := max(float64(0), float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))) / float64(copies)
	inusePerCopy := max(float64(0), float64(int64(m1.HeapInuse)-int64(m0.HeapInuse))) / float64(copies)
	return raskBatchMemResult{
		runs:          keep[0].Len(),
		structBPerRun: structB,
		heapAllocB:    allocPerCopy,
		heapInuseB:    inusePerCopy,
		rssDeltaB:     max(int64(0), rss1-rss0),
	}
}

func measureBatchRask(ops []raskOp, copies int) raskBatchMemResult {
	runtime.GC()
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	rss0 := processRSSBytes()

	keep := make([]*RaskRunIndex, copies)
	for i := range copies {
		keep[i] = applyOpsRask(ops)
	}
	runtime.GC()
	runtime.GC()
	var m1Tmp runtime.MemStats
	runtime.ReadMemStats(&m1Tmp)
	m1 = m1Tmp
	rss1 := processRSSBytes()

	runs := max(1, keep[0].Len())
	structB := float64(keep[0].Stats().StructuralBytes) / float64(runs)
	runtime.KeepAlive(keep)

	allocPerCopy := max(float64(0), float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))) / float64(copies)
	inusePerCopy := max(float64(0), float64(int64(m1.HeapInuse)-int64(m0.HeapInuse))) / float64(copies)
	return raskBatchMemResult{
		runs:          keep[0].Len(),
		structBPerRun: structB,
		heapAllocB:    allocPerCopy,
		heapInuseB:    inusePerCopy,
		rssDeltaB:     max(int64(0), rss1-rss0),
	}
}

func writeHeapPprofSnapshot(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// TestRASKIndependentQualificationAndPprof performs independent structural-vs-RSS/heap
// accounting, captures live runtime/pprof heap profiles for Intervals vs RASK, verifies
// real vfs.Volume 1 PiB journal-replay parity, and sweeps N across regimes to qualify
// the adaptive promotion/demotion thresholds (raskVoidPromoteThreshold / raskVoidDemoteThreshold).
func TestRASKIndependentQualificationAndPprof(t *testing.T) {
	ctx := context.Background()

	// 1. Structural vs retained HeapAlloc / HeapInuse / RSS across N = 8,192 workloads
	//    plus real vfs.Volume 1 PiB disk-backed journal replay.
	const qualN = 8192
	t.Log("=== Qualification Part 1: Structural (B/run) vs Retained Heap & RSS (N=8,192) ===")
	for _, w := range qualificationWorkloads(qualN) {
		res, err := measureSyntheticWorkload("qualification", w.name, w.name, "", w.ops, w.qOffs, w.qRange)
		if err != nil {
			t.Fatalf("%s: %v", w.name, err)
		}
		if !res.ExactMatch {
			t.Fatalf("%s: ExactMatch=false", w.name)
		}
		t.Logf("%-20s | runs=%5d | leaves=%3d/%3d | BaseStruct=%.2f B/run (%6d B) | RaskStruct=%.2f B/run (%6d B) | Sav=%+5.1f%% | BaseHeap=%7d/%7d B (RSS+%7d B) | RaskHeap=%7d/%7d B (RSS+%7d B) | BaseBuild=%7.3fms | RaskBuild=%7.3fms | p50=%.1fns/%.1fns",
			w.name, res.FinalRuns, res.CompactLeaves, res.WideLeaves,
			res.BaselineStructuralBytesPerRun, res.BaselineStructuralBytes,
			res.RaskStructuralBytesPerRun, res.RaskStructuralBytes,
			res.StructuralSavingsPct,
			res.BaselineRetainedHeapAllocBytes, res.BaselineRetainedHeapInuseBytes, res.BaselineRSSDeltaBytes,
			res.RaskRetainedHeapAllocBytes, res.RaskRetainedHeapInuseBytes, res.RaskRSSDeltaBytes,
			res.BaselineBuildMs, res.RaskBuildMs, res.BaselineP50Ns, res.RaskP50Ns)
	}

	// Real vfs.Volume 1 PiB journal replay verification.
	jDir := raskScratchDir(t)
	mem := storetest.NewMem()
	rep, err := RunRASKBenchmark(ctx, RASKBenchmarkConfig{
		OpenBackend: func() (backend.Backend, error) { return mem, nil },
		JournalDir:  jDir,
		Short:       true,
	})
	if err != nil {
		t.Fatalf("RunRASKBenchmark: %v", err)
	}
	jr := rep.JournalReplay
	rw := rep.Workloads[0]
	t.Logf("RealVolumeJournal1PiB | records=%d (w=%d,p=%d,s=%d) | extract=%.2fms | voidSpans=%d (Base=%.2f vs RASK=%.2f B/run) | replayStruct=%.2f vs %.2f B/run (%+.1f%%) | match=%v",
		jr.JournalRecords, jr.WriteRecords, jr.PunchRecords, jr.SizeRecords, jr.ReplayExtractMs,
		jr.VoidSpans, jr.VoidBaselineBPerRun, jr.VoidRaskBPerRun,
		rw.BaselineStructuralBytesPerRun, rw.RaskStructuralBytesPerRun, rw.StructuralSavingsPct, rw.ExactMatch)
	if !rw.ExactMatch || jr.JournalRecords == 0 {
		t.Fatalf("invalid real journal replay result: %+v / %+v", jr, rw)
	}

	// 2. Live pprof heap profile capture (pin multiple ensembles so default 512 KiB
	//    MemProfileRate samples all dominant Interval vs RASK allocations cleanly).
	pprofDir := os.Getenv("RASK_PPROF_DIR")
	if pprofDir == "" {
		pprofDir = t.TempDir()
	} else if err := os.MkdirAll(pprofDir, 0o755); err != nil {
		t.Fatalf("mkdir RASK_PPROF_DIR: %v", err)
	}
	jOpsPprof, _, _ := genRandomJournalReplayOps(4096)
	const ensembleCopies = 64

	runtime.GC()
	runtime.GC()
	pinnedBase := make([]*BaselineRunSlice, ensembleCopies)
	for i := range ensembleCopies {
		pinnedBase[i] = applyOpsBaseline(jOpsPprof)
	}
	runtime.GC()
	runtime.GC()
	baseProfPath := filepath.Join(pprofDir, "baseline_heap.pb.gz")
	if err := writeHeapPprofSnapshot(baseProfPath); err != nil {
		t.Fatalf("write baseline pprof: %v", err)
	}
	stBase, err := os.Stat(baseProfPath)
	if err != nil || stBase.Size() == 0 {
		t.Fatalf("baseline heap pprof invalid: err=%v stat=%+v", err, stBase)
	}
	runtime.KeepAlive(pinnedBase)
	pinnedBase = nil
	runtime.GC()
	runtime.GC()

	pinnedRask := make([]*RaskRunIndex, ensembleCopies)
	for i := range ensembleCopies {
		pinnedRask[i] = applyOpsRask(jOpsPprof)
	}
	runtime.GC()
	runtime.GC()
	raskProfPath := filepath.Join(pprofDir, "rask_heap.pb.gz")
	if err := writeHeapPprofSnapshot(raskProfPath); err != nil {
		t.Fatalf("write rask pprof: %v", err)
	}
	stRask, err := os.Stat(raskProfPath)
	if err != nil || stRask.Size() == 0 {
		t.Fatalf("rask heap pprof invalid: err=%v stat=%+v", err, stRask)
	}
	t.Logf("=== Qualification Part 2: pprof heap profiles written (%s=%d B, %s=%d B) ===",
		baseProfPath, stBase.Size(), raskProfPath, stRask.Size())
	runtime.KeepAlive(pinnedRask)

	// 3. Empirical crossover sweep across N in [8..8192] for memory and latency.
	t.Log("=== Qualification Part 3: Workload Crossover Sweep ===")
	ns := []int{8, 16, 32, 64, 128, 256, 384, 512, 768, 1024, 2048, 4096, 8192}
	firstStructWin := 0
	firstHeapAllocWin := 0
	firstRandBuildWin := 0

	for _, n := range ns {
		copies := max(8, min(2048, 32768/n))
		randOps, _, _ := genClusteredRandPunchOps(n)
		seqOps, _, _ := genStridedOps(n, 64<<10, 4096)

		bMem := measureBatchBaseline(randOps, copies)
		rMem := measureBatchRask(randOps, copies)

		// Latency sweep: random insert+punch vs monotonic append.
		iters := max(2, min(200, 16384/n))
		t0 := time.Now()
		for range iters {
			raskBenchSink ^= uint64(applyOpsBaseline(randOps).Len())
		}
		baseRandMs := float64(time.Since(t0).Nanoseconds()) / float64(iters) / 1e6

		t1 := time.Now()
		for range iters {
			raskBenchSink ^= uint64(applyOpsRask(randOps).Len())
		}
		raskRandMs := float64(time.Since(t1).Nanoseconds()) / float64(iters) / 1e6

		t2 := time.Now()
		for range iters {
			raskBenchSink ^= uint64(applyOpsBaseline(seqOps).Len())
		}
		baseSeqMs := float64(time.Since(t2).Nanoseconds()) / float64(iters) / 1e6

		t3 := time.Now()
		for range iters {
			raskBenchSink ^= uint64(applyOpsRask(seqOps).Len())
		}
		raskSeqMs := float64(time.Since(t3).Nanoseconds()) / float64(iters) / 1e6

		if firstStructWin == 0 && rMem.structBPerRun < bMem.structBPerRun {
			firstStructWin = n
		}
		if firstHeapAllocWin == 0 && rMem.heapAllocB < bMem.heapAllocB && rMem.heapAllocB > 0 {
			firstHeapAllocWin = n
		}
		if firstRandBuildWin == 0 && raskRandMs < baseRandMs {
			firstRandBuildWin = n
		}

		t.Logf("N=%4d | runs=%4d | Struct B/run Base=%.2f RASK=%.2f | HeapAlloc/copy Base=%7.0fB RASK=%7.0fB | RandBuild Base=%6.3fms RASK=%6.3fms | SeqBuild Base=%6.4fms RASK=%6.4fms",
			n, bMem.runs, bMem.structBPerRun, rMem.structBPerRun,
			bMem.heapAllocB, rMem.heapAllocB,
			baseRandMs, raskRandMs, baseSeqMs, raskSeqMs)
	}

	t.Logf("Crossover summary: structural_B/run_crossover_N=%d, heap_alloc_crossover_N=%d, random_build_latency_crossover_N=%d, file.go_thresholds=(promote=%d, demote=%d)",
		firstStructWin, firstHeapAllocWin, firstRandBuildWin, raskVoidPromoteThreshold, raskVoidDemoteThreshold)

	if firstStructWin == 0 || firstStructWin > raskVoidPromoteThreshold {
		t.Fatalf("expected structural crossover before raskVoidPromoteThreshold (%d), got N=%d",
			raskVoidPromoteThreshold, firstStructWin)
	}
	if firstHeapAllocWin == 0 || firstHeapAllocWin > raskVoidPromoteThreshold {
		t.Fatalf("expected heap allocation crossover before raskVoidPromoteThreshold (%d), got N=%d",
			raskVoidPromoteThreshold, firstHeapAllocWin)
	}
	if firstRandBuildWin == 0 || firstRandBuildWin > raskVoidPromoteThreshold {
		t.Fatalf("expected random mutation throughput crossover by raskVoidPromoteThreshold (%d), got N=%d",
			raskVoidPromoteThreshold, firstRandBuildWin)
	}
	if math.IsNaN(float64(raskBenchSink)) {
		t.Fatal("unreachable")
	}
}
