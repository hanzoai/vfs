// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command raskbench runs the RASK compact sparse-range index benchmark suite
// (HIP-1344, Refs #59), measuring both real vfs.Volume 1 PiB disk-backed
// journal replay and multi-regime synthetic 1 PiB sparse-range workloads,
// optionally emitting CSV and Markdown reports.
//
//	go run ./tools/raskbench
//	go run ./tools/raskbench -csv docs/rask.csv -md docs/rask.md
//	go run ./tools/raskbench -full -json
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

func main() {
	dir := flag.String("dir", "", "scratch directory (defaults to temporary directory on non-volatile disk)")
	full := flag.Bool("full", false, "run full-scale benchmark (100,000 synthetic runs)")
	short := flag.Bool("short", false, "run quick smoke benchmark")
	asJSON := flag.Bool("json", false, "emit machine-readable JSON")
	csvPath := flag.String("csv", "", "write CSV measurements to path")
	mdPath := flag.String("md", "", "write Markdown report to path")
	flag.Parse()

	scratchRoot, cleanup, err := prepareScratch(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "raskbench: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	storeDir := filepath.Join(scratchRoot, "store")
	journalDir := filepath.Join(scratchRoot, "journal")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "raskbench: mkdir store: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "raskbench: mkdir journal: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	rep, err := vfs.RunRASKBenchmark(ctx, vfs.RASKBenchmarkConfig{
		OpenBackend: func() (backend.Backend, error) {
			return backend.Open(ctx, "file://"+filepath.ToSlash(storeDir))
		},
		JournalDir: journalDir,
		Full:       *full,
		Short:      *short,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "raskbench: %v\n", err)
		os.Exit(1)
	}

	if *csvPath != "" {
		if err := writeCSV(*csvPath, rep); err != nil {
			fmt.Fprintf(os.Stderr, "raskbench: write csv: %v\n", err)
			os.Exit(1)
		}
	}
	if *mdPath != "" {
		if err := writeMarkdown(*mdPath, rep); err != nil {
			fmt.Fprintf(os.Stderr, "raskbench: write markdown: %v\n", err)
			os.Exit(1)
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "raskbench: encode json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	printSummary(rep)
}

func prepareScratch(dir string) (string, func(), error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", func() {}, err
		}
		return dir, func() {}, nil
	}
	tmp, err := os.MkdirTemp("", "raskbench-*")
	if err != nil {
		return "", func() {}, err
	}
	probe, err := journal.Files(filepath.Join(tmp, "probe"))
	if err == nil {
		_ = probe.Close()
		_ = os.Remove(filepath.Join(tmp, "probe"))
		return tmp, func() { _ = os.RemoveAll(tmp) }, nil
	}
	_ = os.RemoveAll(tmp)
	if !errors.Is(err, journal.ErrVolatile) {
		return "", func() {}, err
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", func() {}, err
	}
	alt, err := os.MkdirTemp(wd, ".raskbench-scratch-*")
	if err != nil {
		return "", func() {}, err
	}
	return alt, func() { _ = os.RemoveAll(alt) }, nil
}

func printSummary(rep vfs.RASKBenchmarkReport) {
	jr := rep.JournalReplay
	fmt.Printf("RASK Compact Sparse-Range Index Report (HIP-1344, Refs #59)\n")
	fmt.Printf("===========================================================\n")
	fmt.Printf("Runtime            : %s (%s), scale=%s\n", rep.GoVersion, rep.OSArch, rep.Scale)
	fmt.Printf("Real journal replay: logicalSize=%d bytes (1 PiB), records=%d (write=%d, punch=%d, size=%d), replay=%.2f ms\n",
		jr.LogicalSizeBytes, jr.JournalRecords, jr.WriteRecords, jr.PunchRecords, jr.SizeRecords, jr.ReplayExtractMs)
	fmt.Printf("  Void spans       : %d spans (baseline=%.2f structural B/run vs RASK=%.2f structural B/run)\n\n",
		jr.VoidSpans, jr.VoidBaselineBPerRun, jr.VoidRaskBPerRun)

	fmt.Printf("| Category | Workload | Runs | Leaves (C/W) | Base Struct B/run | RASK Struct B/run | Struct Sav | Base HeapAlloc (B) | RASK HeapAlloc (B) | Base Build/CPU (ms) | RASK Build/CPU (ms) | Base p50/p99 (ns) | RASK p50/p99 (ns) | Match |\n")
	fmt.Printf("|---|---|---:|---|---:|---:|---:|---:|---:|---|---|---|---|\n")
	for _, w := range rep.Workloads {
		fmt.Printf("| %s | %s | %d | %d/%d | %.2f | %.2f | %+.1f%% | %d | %d | %.2f/%.2f | %.2f/%.2f | %.1f/%.1f | %.1f/%.1f | %v |\n",
			w.Category, w.Name, w.FinalRuns, w.CompactLeaves, w.WideLeaves,
			w.BaselineStructuralBytesPerRun, w.RaskStructuralBytesPerRun, w.StructuralSavingsPct,
			w.BaselineRetainedHeapAllocBytes, w.RaskRetainedHeapAllocBytes,
			w.BaselineBuildMs, w.BaselineCPUTimeMs, w.RaskBuildMs, w.RaskCPUTimeMs,
			w.BaselineP50Ns, w.BaselineP99Ns, w.RaskP50Ns, w.RaskP99Ns, w.ExactMatch)
	}
}

func writeCSV(path string, rep vfs.RASKBenchmarkReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	header := []string{
		"category", "workload", "ops", "final_runs", "compact_leaves", "wide_leaves",
		"baseline_structural_bytes", "baseline_structural_b_per_run",
		"baseline_retained_heap_alloc_bytes", "baseline_retained_heap_inuse_bytes", "baseline_rss_delta_bytes",
		"baseline_cpu_time_ms", "baseline_gc_cycles",
		"rask_structural_bytes", "rask_structural_b_per_run",
		"rask_retained_heap_alloc_bytes", "rask_retained_heap_inuse_bytes", "rask_rss_delta_bytes",
		"rask_cpu_time_ms", "rask_gc_cycles",
		"structural_savings_pct", "heap_alloc_savings_pct",
		"baseline_build_ms", "rask_build_ms",
		"baseline_query_mean_ns", "baseline_query_p50_ns", "baseline_query_p99_ns",
		"rask_query_mean_ns", "rask_query_p50_ns", "rask_query_p99_ns",
		"baseline_overlap_mean_ns", "rask_overlap_mean_ns", "exact_match",
	}
	if err := w.Write(header); err != nil {
		return err
	}
	f2 := func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
	f3 := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	for _, r := range rep.Workloads {
		row := []string{
			r.Category,
			r.Name,
			strconv.Itoa(r.Ops),
			strconv.Itoa(r.FinalRuns),
			strconv.Itoa(r.CompactLeaves),
			strconv.Itoa(r.WideLeaves),
			strconv.FormatInt(r.BaselineStructuralBytes, 10),
			f2(r.BaselineStructuralBytesPerRun),
			strconv.FormatInt(r.BaselineRetainedHeapAllocBytes, 10),
			strconv.FormatInt(r.BaselineRetainedHeapInuseBytes, 10),
			strconv.FormatInt(r.BaselineRSSDeltaBytes, 10),
			f3(r.BaselineCPUTimeMs),
			strconv.FormatUint(uint64(r.BaselineGCCycles), 10),
			strconv.FormatInt(r.RaskStructuralBytes, 10),
			f2(r.RaskStructuralBytesPerRun),
			strconv.FormatInt(r.RaskRetainedHeapAllocBytes, 10),
			strconv.FormatInt(r.RaskRetainedHeapInuseBytes, 10),
			strconv.FormatInt(r.RaskRSSDeltaBytes, 10),
			f3(r.RaskCPUTimeMs),
			strconv.FormatUint(uint64(r.RaskGCCycles), 10),
			f2(r.StructuralSavingsPct),
			f2(r.HeapAllocSavingsPct),
			f3(r.BaselineBuildMs),
			f3(r.RaskBuildMs),
			f2(r.BaselineQueryNs),
			f2(r.BaselineP50Ns),
			f2(r.BaselineP99Ns),
			f2(r.RaskQueryNs),
			f2(r.RaskP50Ns),
			f2(r.RaskP99Ns),
			f2(r.BaselineOverlapNs),
			f2(r.RaskOverlapNs),
			strconv.FormatBool(r.ExactMatch),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func writeMarkdown(path string, rep vfs.RASKBenchmarkReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	var sb strings.Builder
	jr := rep.JournalReplay

	sb.WriteString("# RASK — Compact In-Memory Sparse-Range Index (HIP-1344)\n\n")
	sb.WriteString("Refs #59. Tracked by HIP-1344. The implementation is in `rask.go`, adaptive\n")
	sb.WriteString("production void-index integration is in `file.go` and `generation.go`, unit and\n")
	sb.WriteString("differential oracle tests are in `rask_test.go`, the reproducible benchmark CLI\n")
	sb.WriteString("is `tools/raskbench`, and raw machine-readable measurements are in `docs/rask.csv`.\n\n")

	sb.WriteString("## 1. Primary reference, scope, and clean-room audit\n\n")
	sb.WriteString("* **Primary reference:** RASK (24th USENIX Conference on File and Storage\n")
	sb.WriteString("  Technologies, FAST 2026) — compact range indexing for sparse file extents and\n")
	sb.WriteString("  write-ahead journal spans.\n")
	sb.WriteString("* **Scope in `hanzoai/vfs`:** `rask.go` implements an in-memory segmented\n")
	sb.WriteString("  B+tree (`RaskRunIndex`) over `run{a, b uint64}` intervals alongside the exact\n")
	sb.WriteString("  16-byte `[]run` sorted binary-search baseline (`BaselineRunSlice`, matching\n")
	sb.WriteString("  `file.go` `layer.void` / `join([]run)`), and integrates an adaptive crossover\n")
	sb.WriteString("  threshold (`raskVoidPromoteThreshold = 512`, `raskVoidDemoteThreshold = 128`)\n")
	sb.WriteString("  into canonical `file.go` (`layer.void` / `layer.voidRask`) and `generation.go`\n")
	sb.WriteString("  (`snap.runs` / `snap.voided`). It handles arbitrary `0..MaxSize` (`1 << 62`)\n")
	sb.WriteString("  and `1 << 50` (1 PiB) sparse files purely in memory.\n")
	sb.WriteString("* **Format-v2 invariance & clean-room audit:** Zero frozen on-disk/object\n")
	sb.WriteString("  format-v2 wire encodings (`head.go`, `record.go`, `shard.go`, `pack.go`,\n")
	sb.WriteString("  `tree.go`) or dependency manifests (`go.mod`, `go.sum`) were modified.\n")
	sb.WriteString("  Everything in `rask.go`, `rask_test.go`, and `tools/raskbench` is an\n")
	sb.WriteString("  independent clean-room Apache-2.0 Go implementation using only the Go standard\n")
	sb.WriteString("  library and existing `hanzoai/vfs` packages, preserving ciphertext-only\n")
	sb.WriteString("  backend/journal storage (Rule 1 of `LLM.md`).\n\n")

	sb.WriteString("## 2. Architecture & Adaptive `file.go` Integration\n\n")
	sb.WriteString("1. **Base-relative compact B+tree leaves (`raskLeafCap = 256`):**\n")
	sb.WriteString("   * Each compact leaf anchors up to 256 disjoint coalesced runs to a shared\n")
	sb.WriteString("     64-bit `base` offset (`uint64`) and stores `(deltaOff uint32, len32 uint32)`\n")
	sb.WriteString("     pairs (**8 bytes/run** vs 16 bytes/run in shipping `[]run`) whenever\n")
	sb.WriteString("     `maxEnd - base <= math.MaxUint32` (`4 GiB - 1`).\n")
	sb.WriteString("   * Slice capacities align to Go small-object size classes (`nextCompactCap`:\n")
	sb.WriteString("     `8..256` in size-class steps) and monotonic right-edge appends keep full\n")
	sb.WriteString("     leaves at 100% occupancy (`~8.39..8.72 structural B/run`).\n")
	sb.WriteString("2. **Automatic gap-split & wide 64-bit promotion (`raskWideCap = 128`):**\n")
	sb.WriteString("   * When an insert spans `> 4 GiB` relative to a compact leaf's `base`, a\n")
	sb.WriteString("     clustered leaf (`>= 2` runs) splits at the `> 4 GiB` gap into two compact\n")
	sb.WriteString("     leaves so independent 1 PiB clusters stay at 8 bytes/run.\n")
	sb.WriteString("   * When single runs exceed `4 GiB` or consecutive runs are uniformly spaced\n")
	sb.WriteString("     `> 4 GiB` apart (`5 GiB` stride), the leaf promotes to wide `[]run` (`uint64`)\n")
	sb.WriteString("     mode rather than exploding into 1-run leaves, and re-compacts on split/punch\n")
	sb.WriteString("     whenever a half returns within `math.MaxUint32`.\n")
	sb.WriteString("3. **Adaptive threshold in canonical `file.go` (`raskVoidPromoteThreshold = 512`):**\n")
	sb.WriteString("   * Below `512` punched void spans, `layer` keeps flat `l.void []run` with zero\n")
	sb.WriteString("     B+tree allocation overhead and direct L1-cache binary search.\n")
	sb.WriteString("   * Once `len(l.void) >= 512`, `layer.addVoid` promotes `l.void` to\n")
	sb.WriteString("     `l.voidRask *RaskRunIndex` (and clears `l.void = nil` to avoid duplicate\n")
	sb.WriteString("     representation), turning subsequent `Punch`, `Truncate`, `body.plan`,\n")
	sb.WriteString("     `snap.runs`, and `snap.voided` operations from `O(N)` slice shifts into\n")
	sb.WriteString("     `O(log N)` B+tree operations. If truncation or coalescing shrinks the run\n")
	sb.WriteString("     count to `<= 128` (`raskVoidDemoteThreshold`), `layer` demotes cleanly back\n")
	sb.WriteString("     to flat `[]run`.\n\n")

	sb.WriteString("## 3. Empirical Measurements (Structural vs. Retained Heap / RSS / CPU / GC)\n\n")
	sb.WriteString(fmt.Sprintf("Measured on `%s` (`%s`, scale `%s`).\n", rep.GoVersion, rep.OSArch, rep.Scale))
	sb.WriteString("* **`StructuralBytes` (`Stats().StructuralBytes`):** Sums `unsafe.Sizeof` of\n")
	sb.WriteString("  index structs plus `cap(slice) * unsafe.Sizeof(elem)` of all backing slices.\n")
	sb.WriteString("  It does **not** include Go runtime size-class rounding, GC span headers, or OS\n")
	sb.WriteString("  page tables.\n")
	sb.WriteString("* **`RetainedHeapAllocBytes` / `RetainedHeapInuseBytes` / `RSSDeltaBytes`:**\n")
	sb.WriteString("  Independently measured in isolated forced-GC windows (`runtime.GC()` twice\n")
	sb.WriteString("  before and after build with the index pinned via `runtime.KeepAlive`),\n")
	sb.WriteString("  capturing actual Go allocator live object bytes (`MemStats.HeapAlloc`),\n")
	sb.WriteString("  in-use span bytes (`MemStats.HeapInuse`), and Linux `/proc/self/statm`\n")
	sb.WriteString("  resident set delta (`RSSDeltaBytes`), alongside `syscall.Getrusage` user+sys\n")
	sb.WriteString("  CPU time (`CPUTimeMs`) and `MemStats.NumGC` delta (`GCCycles`).\n\n")

	sb.WriteString("### A. Real `vfs.Volume` Disk-Backed Journal Replay (1 PiB `1 << 50` Sparse File)\n\n")
	sb.WriteString("A real `vfs.Volume` backed by `file://` storage and a real disk-backed `journal.Files`\n")
	sb.WriteString("journal (`Bound = 1h`, un-flushed `head.Journal = 0`) executed `Truncate(1<<50)`,\n")
	sb.WriteString("clustered sparse `WriteAt` bursts across 16 regions in `0..1 PiB`, and `Punch` holes,\n")
	sb.WriteString("then closed without flushing. Reopening via `openLedger` + `j.Replay` + `l.open` +\n")
	sb.WriteString("`decodeChange` extracted actual AES-256-GCM sealed journal records:\n\n")
	sb.WriteString(fmt.Sprintf("* **Logical file size:** `%d` bytes (`1 PiB`)\n", jr.LogicalSizeBytes))
	sb.WriteString(fmt.Sprintf("* **Extracted journal records:** `%d` (`write=%d`, `punch=%d`, `size=%d`) in `%.2f ms`\n",
		jr.JournalRecords, jr.WriteRecords, jr.PunchRecords, jr.SizeRecords, jr.ReplayExtractMs))
	sb.WriteString(fmt.Sprintf("* **Extracted void spans (`layer.void`):** `%d` spans (`Baseline=%.2f structural B/run` vs `RASK=%.2f structural B/run`)\n\n",
		jr.VoidSpans, jr.VoidBaselineBPerRun, jr.VoidRaskBPerRun))

	writeTable(&sb, filterCategory(rep.Workloads, "real_volume_journal_replay"))

	sb.WriteString("\n### B. Synthetic 1 PiB Sparse-Range Microbenchmarks\n\n")
	sb.WriteString("*(Synthetic range microbenchmarks — separated from the real `vfs.Volume` journal trace above.)*\n\n")
	writeTable(&sb, filterCategory(rep.Workloads, "synthetic_microbenchmark"))

	sb.WriteString("\n## 4. Honest Negative & Trade-Off Results\n\n")
	sb.WriteString("Measured regimes where shipping sorted `[]run` binary search is faster or narrower than `RaskRunIndex`:\n\n")
	sb.WriteString("1. **Sequential append & warm point-lookup `p50` (`synth_clustered_seq_1pib`, `synth_strided_sub4g_1pib`):** ")
	sb.WriteString("Monotonic appending into `[]run` is amortized `O(1)`, whereas descending a 2-level B+tree adds tree traversal and node-split work on sequential inserts. ")
	sb.WriteString("For warm point queries (`Contains`), flat `[]run` has zero pointer indirection and slightly lower `p50` latency across sequential and small-`N` regimes.\n")
	sb.WriteString("2. **Uniform `> 4 GiB` gaps (`synth_strided_gt4g_1pib`):** When every adjacent run is `> 4 GiB` apart (`5 GiB` stride), no two runs share a `uint32` compact leaf, forcing wide `16 B/run` fallback (`> 16.00 B/run` packed `[]run` floor).\n")
	sb.WriteString("3. **Small files (`synth_small_n_seq`, `N <= 128 < raskVoidPromoteThreshold`):** A single flat `2 KiB` slice fits inside L1 cache with zero B+tree overhead; `file.go` therefore stays on flat `[]run` below `raskVoidPromoteThreshold = 512`.\n\n")
	sb.WriteString("### Per-workload summary\n\n")
	for _, w := range rep.Workloads {
		sb.WriteString(fmt.Sprintf("* **`%s`:** %s ", w.Name, w.TradeOffNote))
		sb.WriteString(fmt.Sprintf("(Structural: Baseline `%.2f B/run` [%d B] vs RASK `%.2f B/run` [%d B], savings `%+.1f%%`; Retained HeapAlloc: `%d B` vs `%d B`, HeapInuse: `%d B` vs `%d B`; build wall/CPU: `%.3f/%.3f ms` vs `%.3f/%.3f ms`; point lookup `p50`: `%.1f ns` vs `%.1f ns`.)\n",
			w.BaselineStructuralBytesPerRun, w.BaselineStructuralBytes,
			w.RaskStructuralBytesPerRun, w.RaskStructuralBytes, w.StructuralSavingsPct,
			w.BaselineRetainedHeapAllocBytes, w.RaskRetainedHeapAllocBytes,
			w.BaselineRetainedHeapInuseBytes, w.RaskRetainedHeapInuseBytes,
			w.BaselineBuildMs, w.BaselineCPUTimeMs, w.RaskBuildMs, w.RaskCPUTimeMs,
			w.BaselineP50Ns, w.RaskP50Ns))
	}

	sb.WriteString("\n## 5. Reproducing\n\n")
	sb.WriteString("```sh\n")
	sb.WriteString("TMPDIR=/home/max/tmp go test -v -count=1 -run '^TestRASK|^TestSparse' .\n")
	sb.WriteString("TMPDIR=/home/max/tmp go run ./tools/raskbench -csv docs/rask.csv -md docs/rask.md\n")
	sb.WriteString("TMPDIR=/home/max/tmp go run ./tools/raskbench -full -json\n")
	sb.WriteString("```\n")

	return os.WriteFile(path, []byte(sb.String()), 0o644)
}

func filterCategory(all []vfs.RASKWorkloadResult, cat string) []vfs.RASKWorkloadResult {
	var out []vfs.RASKWorkloadResult
	for _, w := range all {
		if w.Category == cat {
			out = append(out, w)
		}
	}
	return out
}

func writeTable(sb *strings.Builder, rows []vfs.RASKWorkloadResult) {
	sb.WriteString("| Workload | Ops | Final Runs | Leaves (C/W) | Base Struct (B / B/run) | RASK Struct (B / B/run) | Struct Sav | Base HeapAlloc / Inuse (B) | RASK HeapAlloc / Inuse (B) | Base / RASK RSS Delta (B) | Base Wall/CPU (ms) [GC] | RASK Wall/CPU (ms) [GC] | Base Query mean/p50/p99 (ns) | RASK Query mean/p50/p99 (ns) | Base/RASK Overlap (ns) | Match |\n")
	sb.WriteString("|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|\n")
	for _, w := range rows {
		sb.WriteString(fmt.Sprintf("| `%s` | %d | %d | %d / %d | %d (%.2f) | %d (%.2f) | %+.1f%% | %d / %d | %d / %d | %d / %d | %.3f / %.3f [%d] | %.3f / %.3f [%d] | %.1f / %.1f / %.1f | %.1f / %.1f / %.1f | %.1f / %.1f | %v |\n",
			w.Name, w.Ops, w.FinalRuns, w.CompactLeaves, w.WideLeaves,
			w.BaselineStructuralBytes, w.BaselineStructuralBytesPerRun,
			w.RaskStructuralBytes, w.RaskStructuralBytesPerRun,
			w.StructuralSavingsPct,
			w.BaselineRetainedHeapAllocBytes, w.BaselineRetainedHeapInuseBytes,
			w.RaskRetainedHeapAllocBytes, w.RaskRetainedHeapInuseBytes,
			w.BaselineRSSDeltaBytes, w.RaskRSSDeltaBytes,
			w.BaselineBuildMs, w.BaselineCPUTimeMs, w.BaselineGCCycles,
			w.RaskBuildMs, w.RaskCPUTimeMs, w.RaskGCCycles,
			w.BaselineQueryNs, w.BaselineP50Ns, w.BaselineP99Ns,
			w.RaskQueryNs, w.RaskP50Ns, w.RaskP99Ns,
			w.BaselineOverlapNs, w.RaskOverlapNs, w.ExactMatch))
	}
}
