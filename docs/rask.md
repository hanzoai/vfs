# RASK — Compact In-Memory Sparse-Range Index (HIP-1344)

Refs #59. Tracked by HIP-1344. The implementation is in `rask.go`, adaptive
production void-index integration is in `file.go` and `generation.go`, unit and
differential oracle tests are in `rask_test.go`, the reproducible benchmark CLI
is `tools/raskbench`, and raw machine-readable measurements are in `docs/rask.csv`.

## 1. Primary reference, scope, and clean-room audit

* **Primary reference:** RASK (24th USENIX Conference on File and Storage
  Technologies, FAST 2026) — compact range indexing for sparse file extents and
  write-ahead journal spans.
* **Scope in `hanzoai/vfs`:** `rask.go` implements an in-memory segmented
  B+tree (`RaskRunIndex`) over `run{a, b uint64}` intervals alongside the exact
  16-byte `[]run` sorted binary-search baseline (`BaselineRunSlice`, matching
  `file.go` `layer.void` / `join([]run)`), and integrates an adaptive crossover
  threshold (`raskVoidPromoteThreshold = 512`, `raskVoidDemoteThreshold = 128`)
  into canonical `file.go` (`layer.void` / `layer.voidRask`) and `generation.go`
  (`snap.runs` / `snap.voided`). It handles arbitrary `0..MaxSize` (`1 << 62`)
  and `1 << 50` (1 PiB) sparse files purely in memory.
* **Format-v2 invariance & clean-room audit:** Zero frozen on-disk/object
  format-v2 wire encodings (`head.go`, `record.go`, `shard.go`, `pack.go`,
  `tree.go`) or dependency manifests (`go.mod`, `go.sum`) were modified.
  Everything in `rask.go`, `rask_test.go`, and `tools/raskbench` is an
  independent clean-room Apache-2.0 Go implementation using only the Go standard
  library and existing `hanzoai/vfs` packages, preserving ciphertext-only
  backend/journal storage (Rule 1 of `LLM.md`).

## 2. Architecture & Adaptive `file.go` Integration

1. **Base-relative compact B+tree leaves (`raskLeafCap = 256`):**
   * Each compact leaf anchors up to 256 disjoint coalesced runs to a shared
     64-bit `base` offset (`uint64`) and stores `(deltaOff uint32, len32 uint32)`
     pairs (**8 bytes/run** vs 16 bytes/run in shipping `[]run`) whenever
     `maxEnd - base <= math.MaxUint32` (`4 GiB - 1`).
   * Slice capacities align to Go small-object size classes (`nextCompactCap`:
     `8..256` in size-class steps) and monotonic right-edge appends keep full
     leaves at 100% occupancy (`~8.39..8.72 structural B/run`).
2. **Automatic gap-split & wide 64-bit promotion (`raskWideCap = 128`):**
   * When an insert spans `> 4 GiB` relative to a compact leaf's `base`, a
     clustered leaf (`>= 2` runs) splits at the `> 4 GiB` gap into two compact
     leaves so independent 1 PiB clusters stay at 8 bytes/run.
   * When single runs exceed `4 GiB` or consecutive runs are uniformly spaced
     `> 4 GiB` apart (`5 GiB` stride), the leaf promotes to wide `[]run` (`uint64`)
     mode rather than exploding into 1-run leaves, and re-compacts on split/punch
     whenever a half returns within `math.MaxUint32`.
3. **Adaptive threshold in canonical `file.go` (`raskVoidPromoteThreshold = 512`):**
   * Below `512` punched void spans, `layer` keeps flat `l.void []run` with zero
     B+tree allocation overhead and direct L1-cache binary search.
   * Once `len(l.void) >= 512`, `layer.addVoid` promotes `l.void` to
     `l.voidRask *RaskRunIndex` (and clears `l.void = nil` to avoid duplicate
     representation), turning subsequent `Punch`, `Truncate`, `body.plan`,
     `snap.runs`, and `snap.voided` operations from `O(N)` slice shifts into
     `O(log N)` B+tree operations. If truncation or coalescing shrinks the run
     count to `<= 128` (`raskVoidDemoteThreshold`), `layer` demotes cleanly back
     to flat `[]run`.

## 3. Empirical Measurements (Structural vs. Retained Heap / RSS / CPU / GC)

Measured on `go1.27.1` (`linux/amd64`, scale `standard`).
* **`StructuralBytes` (`Stats().StructuralBytes`):** Sums `unsafe.Sizeof` of
  index structs plus `cap(slice) * unsafe.Sizeof(elem)` of all backing slices.
  It does **not** include Go runtime size-class rounding, GC span headers, or OS
  page tables.
* **`RetainedHeapAllocBytes` / `RetainedHeapInuseBytes` / `RSSDeltaBytes`:**
  Independently measured in isolated forced-GC windows (`runtime.GC()` twice
  before and after build with the index pinned via `runtime.KeepAlive`),
  capturing actual Go allocator live object bytes (`MemStats.HeapAlloc`),
  in-use span bytes (`MemStats.HeapInuse`), and Linux `/proc/self/statm`
  resident set delta (`RSSDeltaBytes`), alongside `syscall.Getrusage` user+sys
  CPU time (`CPUTimeMs`) and `MemStats.NumGC` delta (`GCCycles`).

### A. Real `vfs.Volume` Disk-Backed Journal Replay (1 PiB `1 << 50` Sparse File)

A real `vfs.Volume` backed by `file://` storage and a real disk-backed `journal.Files`
journal (`Bound = 1h`, un-flushed `head.Journal = 0`) executed `Truncate(1<<50)`,
clustered sparse `WriteAt` bursts across 16 regions in `0..1 PiB`, and `Punch` holes,
then closed without flushing. Reopening via `openLedger` + `j.Replay` + `l.open` +
`decodeChange` extracted actual AES-256-GCM sealed journal records:

* **Logical file size:** `1125899906842624` bytes (`1 PiB`)
* **Extracted journal records:** `2744` (`write=2400`, `punch=343`, `size=1`) in `8.27 ms`
* **Extracted void spans (`layer.void`):** `343` spans (`Baseline=23.95 structural B/run` vs `RASK=18.26 structural B/run`)

| Workload | Ops | Final Runs | Leaves (C/W) | Base Struct (B / B/run) | RASK Struct (B / B/run) | Struct Sav | Base HeapAlloc / Inuse (B) | RASK HeapAlloc / Inuse (B) | Base / RASK RSS Delta (B) | Base Wall/CPU (ms) [GC] | RASK Wall/CPU (ms) [GC] | Base Query mean/p50/p99 (ns) | RASK Query mean/p50/p99 (ns) | Base/RASK Overlap (ns) | Match |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| `real_journal_replay_1pib` | 2743 | 2743 | 16 / 0 | 57368 (20.91) | 23928 (8.72) | +58.3% | 57344 / 57344 | 23888 / 40960 | 0 / 172032 | 2.707 / 3.285 [0] | 0.645 / 0.650 [0] | 92.7 / 89.5 / 124.2 | 98.5 / 94.1 / 180.3 | 116.1 / 98.9 | true |

### B. Synthetic 1 PiB Sparse-Range Microbenchmarks

*(Synthetic range microbenchmarks — separated from the real `vfs.Volume` journal trace above.)*

| Workload | Ops | Final Runs | Leaves (C/W) | Base Struct (B / B/run) | RASK Struct (B / B/run) | Struct Sav | Base HeapAlloc / Inuse (B) | RASK HeapAlloc / Inuse (B) | Base / RASK RSS Delta (B) | Base Wall/CPU (ms) [GC] | RASK Wall/CPU (ms) [GC] | Base Query mean/p50/p99 (ns) | RASK Query mean/p50/p99 (ns) | Base/RASK Overlap (ns) | Match |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| `synth_clustered_seq_1pib` | 50000 | 50000 | 200 / 0 | 950296 (19.01) | 429224 (8.58) | +54.8% | 950272 / 950272 | 429232 / 532480 | 3162112 / 1310720 | 8.105 / 7.340 [1] | 10.011 / 11.445 [2] | 96.6 / 90.0 / 223.1 | 104.3 / 89.8 / 250.8 | 102.2 / 144.5 | true |
| `synth_clustered_rand_1pib` | 60000 | 60000 | 401 / 0 | 1196056 (19.93) | 541112 (9.02) | +54.8% | 1196032 / 1196032 | 541136 / 851968 | 2183168 / 2228224 | 1616.848 / 1457.759 [2] | 49.890 / 36.367 [1] | 183.4 / 180.2 / 200.4 | 286.0 / 199.5 / 843.8 | 376.3 / 235.5 | true |
| `synth_strided_sub4g_1pib` | 50000 | 50000 | 196 / 0 | 950296 (19.01) | 419368 (8.39) | +55.9% | 950272 / 950272 | 419376 / 442368 | 851968 / 888832 | 7.955 / 7.905 [2] | 15.347 / 11.445 [1] | 92.3 / 85.5 / 205.4 | 98.2 / 90.3 / 205.1 | 118.0 / 99.2 | true |
| `synth_strided_gt4g_1pib` | 50000 | 50000 | 0 / 391 | 950296 (19.01) | 838808 (16.78) | +11.7% | 950272 / 950272 | 838864 / 909312 | 1753088 / 352256 | 5.423 / 5.272 [2] | 12.785 / 8.757 [1] | 88.3 / 85.2 / 153.8 | 99.5 / 88.0 / 245.3 | 100.0 / 100.6 | true |
| `synth_small_n_seq` | 128 | 128 | 1 / 0 | 2072 (16.19) | 1136 (8.88) | +45.2% | 2048 / 0 | 1088 / 0 | 0 / 36864 | 0.007 / 0.010 [0] | 0.030 / 0.033 [0] | 38.3 / 34.9 / 69.2 | 40.7 / 36.7 / 91.2 | 27.3 / 34.7 | true |

## 4. Honest Negative & Trade-Off Results

Measured regimes where shipping sorted `[]run` binary search is faster or narrower than `RaskRunIndex`:

1. **Sequential append & warm point-lookup `p50` (`synth_clustered_seq_1pib`, `synth_strided_sub4g_1pib`):** Monotonic appending into `[]run` is amortized `O(1)`, whereas descending a 2-level B+tree adds tree traversal and node-split work on sequential inserts. For warm point queries (`Contains`), flat `[]run` has zero pointer indirection and slightly lower `p50` latency across sequential and small-`N` regimes.
2. **Uniform `> 4 GiB` gaps (`synth_strided_gt4g_1pib`):** When every adjacent run is `> 4 GiB` apart (`5 GiB` stride), no two runs share a `uint32` compact leaf, forcing wide `16 B/run` fallback (`> 16.00 B/run` packed `[]run` floor).
3. **Small files (`synth_small_n_seq`, `N <= 128 < raskVoidPromoteThreshold`):** A single flat `2 KiB` slice fits inside L1 cache with zero B+tree overhead; `file.go` therefore stays on flat `[]run` below `raskVoidPromoteThreshold = 512`.

### Per-workload summary

* **`real_journal_replay_1pib`:** Real vfs.Volume journal records replayed via openLedger + j.Replay + decodeChange; compact leaves pack intra-cluster writes/punches into 8 B/run. (Structural: Baseline `20.91 B/run` [57368 B] vs RASK `8.72 B/run` [23928 B], savings `+58.3%`; Retained HeapAlloc: `57344 B` vs `23888 B`, HeapInuse: `57344 B` vs `40960 B`; build wall/CPU: `2.707/3.285 ms` vs `0.645/0.650 ms`; point lookup `p50`: `89.5 ns` vs `94.1 ns`.)
* **`synth_clustered_seq_1pib`:** RASK compresses clustered runs below 9 B/run; however, flat []run append is O(1) so sequential build and L1/L2 point lookup p50 remain faster on []run. (Structural: Baseline `19.01 B/run` [950296 B] vs RASK `8.58 B/run` [429224 B], savings `+54.8%`; Retained HeapAlloc: `950272 B` vs `429232 B`, HeapInuse: `950272 B` vs `532480 B`; build wall/CPU: `8.105/7.340 ms` vs `10.011/11.445 ms`; point lookup `p50`: `90.0 ns` vs `89.8 ns`.)
* **`synth_clustered_rand_1pib`:** RASK achieves < 9.5 B/run and O(log N) inserts/punches vs O(N) middle slice shifts in []run, while []run retains slightly lower warm point-lookup p50. (Structural: Baseline `19.93 B/run` [1196056 B] vs RASK `9.02 B/run` [541112 B], savings `+54.8%`; Retained HeapAlloc: `1196032 B` vs `541136 B`, HeapInuse: `1196032 B` vs `851968 B`; build wall/CPU: `1616.848/1457.759 ms` vs `49.890/36.367 ms`; point lookup `p50`: `180.2 ns` vs `199.5 ns`.)
* **`synth_strided_sub4g_1pib`:** 10 MiB stride matches TestTinyExtentsCost; 256 runs span 2.5 GiB < 4 GiB so all leaves stay compact (< 8.5 B/run), trading slightly higher sequential build and lookup p50 vs flat []run. (Structural: Baseline `19.01 B/run` [950296 B] vs RASK `8.39 B/run` [419368 B], savings `+55.9%`; Retained HeapAlloc: `950272 B` vs `419376 B`, HeapInuse: `950272 B` vs `442368 B`; build wall/CPU: `7.955/7.905 ms` vs `15.347/11.445 ms`; point lookup `p50`: `85.5 ns` vs `90.3 ns`.)
* **`synth_strided_gt4g_1pib`:** Honest negative/trade-off regime: every run exceeds 4 GiB from neighbors so leaves promote to 16 B/run wide mode (exceeding the packed 16.00 B/run []run floor due to leaf headers, with higher point-lookup p50; lower total bytes vs []run here reflects Go slice growth slack at 50k). (Structural: Baseline `19.01 B/run` [950296 B] vs RASK `16.78 B/run` [838808 B], savings `+11.7%`; Retained HeapAlloc: `950272 B` vs `838864 B`, HeapInuse: `950272 B` vs `909312 B`; build wall/CPU: `5.423/5.272 ms` vs `12.785/8.757 ms`; point lookup `p50`: `85.2 ns` vs `88.0 ns`.)
* **`synth_small_n_seq`:** Honest small-N boundary: flat []run fits in 2 KiB L1 cache with O(1) append, faster sequential build, and faster point/overlap lookups. (Structural: Baseline `16.19 B/run` [2072 B] vs RASK `8.88 B/run` [1136 B], savings `+45.2%`; Retained HeapAlloc: `2048 B` vs `1088 B`, HeapInuse: `0 B` vs `0 B`; build wall/CPU: `0.007/0.010 ms` vs `0.030/0.033 ms`; point lookup `p50`: `34.9 ns` vs `36.7 ns`.)

## 5. Reproducing

```sh
TMPDIR=/home/max/tmp go test -v -count=1 -run '^TestRASK|^TestSparse' .
TMPDIR=/home/max/tmp go run ./tools/raskbench -csv docs/rask.csv -md docs/rask.md
TMPDIR=/home/max/tmp go run ./tools/raskbench -full -json
```
