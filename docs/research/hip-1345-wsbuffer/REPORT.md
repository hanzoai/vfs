# Partial-Write Absorption with Scrap Buffers (WSBuffer, FAST 2026)

Refs `hanzoai/vfs#46`. Tracked by **HIP-1345**.
Library: [`pkg/wsbuffer`](../../../pkg/wsbuffer).
Matched-baseline benchmark harness: [`cmd/wsbuffer-bench`](../../../cmd/wsbuffer-bench).
Raw benchmark data: [`results.json`](./results.json).

---

## 1. Primary Reference, Summary, and License Audit

* **Primary paper:** Zhan et al., *WSBuffer* (Partial-write absorption with scrap buffers and zero-copy direct bypass), 24th USENIX Conference on File and Storage Technologies (**FAST 2026**), https://www.usenix.org/conference/fast26/presentation/zhan.
* **Summary:** Write-ahead journals and copy-on-write filesystems face a dual bottleneck under mixed cloud/sandbox workloads:
  1. **Small and unaligned random writes** (`512 B`, `1.5 KiB`, `4 KiB` metadata updates, SQLite WAL frames, B-tree interior pages) emit per-write journal headers, checksums, syscalls, and redundant superseded overwrites—inflating Write Amplification Factor (`WAF >= 2.0x`) and CPU cost per op.
  2. **Large page-aligned writes** (`64 KiB`, `256 KiB`, `1 MiB` bulk checkpoint/object writes) suffer severe regression if routed through a write-staging buffer (extra `memcpy`, cache pollution, and double writes through both WAL and backing storage).
  WSBuffer resolves both tensions via a **hybrid dual-path architecture**: absorbing small/unaligned writes into fixed-size aligned **scrap buffer slabs** (with in-place page and sub-page extent coalescing before flush) while routing large aligned writes through a **zero-copy direct bypass path** that invalidates superseded scrap extents and emits a compact zero-payload ordering barrier (`56 B`) only when uncheckpointed WAL records exist.
* **Clean-room & License Audit:** All code in `pkg/wsbuffer/`, `cmd/wsbuffer-bench/`, and `docs/research/hip-1345-wsbuffer/` is an independent, clean-room **Apache-2.0** Go implementation written from first principles using only the Go standard library (`context`, `encoding/binary`, `hash/crc32`, `os`, `sync`, `syscall`, etc.) and Hanzo VFS's existing journal package (`github.com/hanzoai/vfs/journal`). **`go.mod` and `go.sum` are completely untouched (zero new external dependencies).**

---

## 2. Reproducible Upstream Baseline on `main`

On `upstream/main` (`ccaa1ff53e4a301b3b13198980fb7bec3f263e79`), the production write-ahead journal resides at `journal/` (`github.com/hanzoai/vfs/journal`; referenced generically as `pkg/journal` in issue `#46`).

Inspection of `journal/journal.go`, `journal/record.go`, and `journal/pipeline.go` establishes three upstream baseline properties:

1. **Per-write copy & framing:** Every `Journal.Append(ctx, payload)` computes CRC32C (`castagnoli`), prefixes a 28-byte `recordHeader`, and copies `payload` into a pooled `batch.data` buffer (`256 KiB` or `4 MiB`), regardless of whether `payload` is a `512 B` partial update or a `1 MiB` page-aligned write.
2. **Unaligned append stream & zero overwrite coalescing:** Segment records start at `dataStart = 80` bytes back-to-back without logical-offset awareness. Repeated `4 KiB` or `512 B` updates to the same logical offset write every superseded version to both the journal segment and the backing file (`WAF >= 2.01x`).
3. **Instrumentation parity:** Both `journal-main` and `wsbuffer-journal` in `cmd/wsbuffer-bench` run against the **exact same `github.com/hanzoai/vfs/journal` implementation**, wrapped with `wsbuffer.CountingDisk` to record exact physical bytes written (`WriteAt`), syscall counts, and `fdatasync` calls on the **exact same local filesystem directory and preallocated backing file**.

### Hardware & Environment

* **OS / Kernel:** `Linux 6.18.57 x86_64` (`linux/amd64`, overlayfs local storage)
* **CPU:** `Intel(R) Xeon(R) CPU @ 2.30GHz` (`2` vCPUs)
* **Toolchain:** `go1.27.1`
* **Protocol:** Median of `3` repetitions per `(workload, engine)` pair (`cmd/wsbuffer-bench -reps 3`), with pre-run `runtime.GC()`, `syscall.Getrusage(RUSAGE_SELF)` (`user + sys` CPU ns/op), `runtime.ReadMemStats` (`allocs/op`, `B/op`), and exact physical byte accounting excluding file preallocation.

---

## 3. Falsifiable Hypothesis, Metrics, and Expected Failure Modes

### Hypothesis

> Placing an aligned scrap-buffer absorption layer with a `64 KiB` zero-copy aligned direct bypass (`pkg/wsbuffer`) in front of `github.com/hanzoai/vfs/journal` will:
> 1. Reduce physical Write Amplification Factor (`WAF = total_physical_bytes / logical_bytes`) by `>= 15%` on durable batched `4 KiB` random writes and by `>= 60%` on buffered `4 KiB` random bursts with locality, while increasing `4 KiB` random-write throughput and reducing CPU `ns/op`.
> 2. Reduce `WAF` by `>= 30%` on `512 B` and `1.5 KiB` unaligned partial writes via sub-page extent merging and batched CRC32C framing.
> 3. Achieve `~1.000x WAF`, `~0 B/op` heap allocations, and `>= 2x` throughput over `journal-main` (and `>= 3x` over a naive no-bypass scrap buffer) on `64 KiB` / `256 KiB` / `1 MiB` aligned sequential writes, while maintaining byte-for-byte linearizability on interleaved overlapping workloads.

### Expected Failure / Negative Regimes (Falsification Boundaries)

1. **Synchronous `fsync` after every single `4 KiB` write (`batch=1`):** Zero coalescing window exists before `Sync()` forces a flush; scrap framing adds slight metadata overhead (`WAF >= journal-main`), and throughput remains strictly `fdatasync`-bound (`~1.0x`).
2. **Strictly large aligned sequential writes (`1 MiB` aligned only) vs `plain-disk`:** Direct bypass avoids the double-copy regression of naive buffering, but cannot beat direct `os.File.WriteAt` (`~1.0x` throughput, slight extent-check CPU overhead vs `plain-disk`).
3. **Undersized scrap buffer (`MaxScrapBytes = 64 KiB`) under wide uniform random scatter (`16 MiB`):** Unique touched pages exceed scrap capacity rapidly, triggering continuous backpressure evictions (`255` flushes) that collapse coalescing and push `WAF` back toward `2.00x`.

---

## 4. Bounded Implementation Architecture

| Path | Responsibility |
|---|---|
| [`pkg/wsbuffer/config.go`](../../../pkg/wsbuffer/config.go) | `Config` validation, `WALMode` (`WALModeJournal`, `WALModeFile`, `WALModeNone`), `Stats`, `RecoveryReport`, typed errors. |
| [`pkg/wsbuffer/slab.go`](../../../pkg/wsbuffer/slab.go) | Fixed-size pooled aligned scrap slabs (`sync.Pool`), O(1) full-page in-place coalescing, sorted non-overlapping sub-page interval merging & full-page promotion, zero-realloc `clear()` flush reset, linearizable `overlayRead`, and direct-bypass `invalidateRange`. |
| [`pkg/wsbuffer/frame.go`](../../../pkg/wsbuffer/frame.go) | Binary CRC32C (`Castagnoli`) WAL codec for `FrameScrapBatch`, compact 56-byte zero-payload `FrameDirectBarrier`, and `FrameCheckpoint`, with torn-tail (`ErrTruncated`) vs corruption (`ErrChecksumMismatch`) separation. |
| [`pkg/wsbuffer/io.go`](../../../pkg/wsbuffer/io.go) | `CountingDisk` wrapper over `github.com/hanzoai/vfs/journal`, `JournalWAL` (atomic `journal.ckpt` watermark persistence + `Publish`/`Reclaim` segment recycling), `FileWAL`, and `osBackingFile` with atomic physical byte/write/sync counters. |
| [`pkg/wsbuffer/wsbuffer.go`](../../../pkg/wsbuffer/wsbuffer.go) | Thread-safe `Store` implementing dual-path `WriteAt`, linearizable `ReadAt`, contiguous-run coalesced `Flush`/`Sync`, `Close`, `AbortCrash`, and crash `recover()` with `subtractBarriers()` anti-clobber replay. |
| [`pkg/wsbuffer/manager.go`](../../../pkg/wsbuffer/manager.go) | Multi-tenant `Manager` isolating volumes by `VolumeID` under a shared `GlobalMaxScrapBytes` budget with automatic largest-victim eviction. |
| [`cmd/wsbuffer-bench/main.go`](../../../cmd/wsbuffer-bench/main.go) | Matched-baseline benchmark harness comparing `plain-disk`, `journal-main`, `wsbuffer-no-bypass`, and `wsbuffer-journal` across 10 workloads (7 primary/mixed + 3 negative regimes). |

---

## 5. Correctness, Concurrency (`-race`), Crash-Recovery, Security & Regression Evidence

All tests pass with the Go race detector enabled (`-race -count=1`):

* **`./pkg/wsbuffer/...` (14 test suites, 19 subtests/tests, `3.79s` under `-race`):**
  * `TestConfigValidation` (6 subtests): rejects non-power-of-two `PageSize`, unaligned `SlabSize`/`BypassThreshold`, undersized `MaxScrapBytes`, negative offsets.
  * `TestScrapCoalescingAndWAF`: verifies in-place 4 KiB coalescing (`>400` coalesced writes, `WAF < 0.50`) and linearizable pre-sync read overlay.
  * `TestSubPageUnalignedMergingAndPromotion`: verifies disjoint, overlapping, cross-page `512 B`/`1.5 KiB` merges, automatic full-page promotion, flush/sync/reopen parity.
  * `TestDirectBypassZeroCopyAndOverlapLinearizability`: verifies mixed small unaligned (including boundary-crossing) + `64 KiB`/`128 KiB` direct bypass writes against byte-for-byte oracle.
  * `TestCrashRecoveryFileWALAndTornTail`: wipes backing file + injects mid-frame torn tail -> recovers all durable extents and truncates torn tail cleanly (`TruncatedTailBytes > 0`).
  * `TestCrashRecoveryDirectBarrierPreventsStaleReplay`: proves `FrameDirectBarrier` + `subtractBarriers()` prevents stale uncheckpointed WAL extents from clobbering newer zero-copy direct writes while preserving non-overlapping prefix bytes.
  * `TestCrashRecoveryJournalWALUncheckpointedReplay`: simulates crash under `WALModeJournal` (real `github.com/hanzoai/vfs/journal`), wipes backing file, reopens, and verifies full journal replay recovery.
  * `TestCRCCorruptionRejection`: verifies header and payload bit-flips fail with `ErrChecksumMismatch` in `DecodeFrame` and `StrictRecovery`.
  * `TestJournalWALIntegrationAndSegmentReclaim`: writes `>3 MiB` across `512 KiB` segments exercising `journal.Publish`, `journal.Reclaim`, atomic `journal.ckpt`, and zero `ErrMismatch` on reopen.
  * `TestConcurrentWritersAndReadersRace`: 8 concurrent workers mixing `512 B`, `4 KiB`, `64 KiB` writes + `ReadAt` + `Sync` with `-race` and post-reopen stripe validation.
  * `TestBoundedMemoryCapAndBackpressure`: enforces `CurrentScrapBytes <= MaxScrapBytes` (`64 KiB` cap over `1 MiB` unique writes, `>= 15` backpressure flushes).
  * `TestMultiTenantVolumeIsolationAndManager`: verifies strict tenant isolation, shared global memory cap eviction, and `ErrVolumeMismatch` rejection on cross-volume WAL open.
  * `TestRandomizedLinearizabilityModelCheck`: 600-step randomized model check mixing all write sizes, reads, flushes, syncs, and clean restarts against ground-truth oracle.
  * `TestBypassZeroAllocations`: verifies `0.00 allocs/op` and exact `1.000x WAF` on the direct bypass path via `testing.AllocsPerRun`.
* **`./cmd/wsbuffer-bench/...` (`TestBenchmarkSuiteQuickAndInvariants`, `-race`):** end-to-end verification of all 10 workloads, oracle verification on mixed overlapping writes, and quantitative WAF/bypass/negative-regime assertions.
* **Upstream regression check (`./journal/...`):** 100% existing `github.com/hanzoai/vfs/journal` tests pass unmodified (`ok github.com/hanzoai/vfs/journal 11.6s`).

---

## 6. Empirical Before/After Results (HIP-1345)

All measurements below are extracted verbatim from [`results.json`](./results.json) (median of 3 runs, same local storage directory).

### A. Primary Acceptance Workload: `4 KiB` Random Writes vs `journal` on `main`

| Workload | Engine | Throughput (`MiB/s`) | IOPS | CPU (`ns/op`) | Alloc (`B/op`) | Total `WAF` | `WAL-WAF` | Phys Writes | Fsyncs | Delta vs `journal-main` |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `4k-rand-burst` (`8192` ops, `32 MiB`) | `plain-disk` | 615.34 | 157,526 | 3,618 | 0 | 1.000 | 0.000 | 8,192 | 1 | reference (no WAL) |
| `4k-rand-burst` (`8192` ops, `32 MiB`) | `journal-main` | 208.04 | 53,257 | 14,130 | 1,650 | 2.010 | 1.010 | 8,666 | 14 | baseline |
| `4k-rand-burst` (`8192` ops, `32 MiB`) | **`wsbuffer-journal`** | **404.78** | **103,625** | **3,162** | **1,437** | **0.409** | **0.205** | **761** | **2** | **`1.95x` throughput, `-77.6%` CPU, `-79.7%` WAF** |
| `4k-rand-durable-batch64` (`4096` ops, `16 MiB`) | `plain-disk` | 44.54 | 11,402 | 15,483 | 0 | 1.000 | 0.000 | 4,096 | 65 | reference (no WAL) |
| `4k-rand-durable-batch64` (`4096` ops, `16 MiB`) | `journal-main` | 28.06 | 7,183 | 21,829 | 384 | 2.010 | 1.010 | 5,270 | 135 | baseline |
| `4k-rand-durable-batch64` (`4096` ops, `16 MiB`) | **`wsbuffer-journal`** | **33.70** | **8,627** | **21,820** | **599** | **1.713** | **0.859** | **2,657** | **130** | **`+20.1%` throughput, `-14.8%` WAF, `-49.6%` phys writes** |

### B. Sub-Page Unaligned Partial Writes (`512 B` and `1.5 KiB`)

| Workload | Engine | Throughput (`MiB/s`) | IOPS | CPU (`ns/op`) | Alloc (`B/op`) | Total `WAF` | `WAL-WAF` | Phys Writes | Delta vs `journal-main` |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| `partial-512b-unaligned` (`8192` ops, batch 128) | `plain-disk` | 23.89 | 48,936 | 4,590 | 0 | 1.000 | 0.000 | 8,192 | reference |
| `partial-512b-unaligned` (`8192` ops, batch 128) | `journal-main` | 12.43 | 25,463 | 9,811 | 119 | 2.078 | 1.078 | 11,735 | baseline |
| `partial-512b-unaligned` (`8192` ops, batch 128) | **`wsbuffer-journal`** | **12.07** | **24,713** | **8,202** | **140** | **1.162** | **0.589** | **2,705** | **`-44.1%` WAF, `-16.4%` CPU, `-77.0%` phys writes** (fsync-bound throughput) |
| `partial-1536b-unaligned` (`4096` ops, batch 64) | `plain-disk` | 42.85 | 29,255 | 7,786 | 0 | 1.000 | 0.000 | 4,096 | reference |
| `partial-1536b-unaligned` (`4096` ops, batch 64) | `journal-main` | 18.89 | 12,893 | 14,424 | 130 | 2.026 | 1.026 | 5,327 | baseline |
| `partial-1536b-unaligned` (`4096` ops, batch 64) | **`wsbuffer-journal`** | **19.07** | **13,020** | **16,788** | **302** | **1.283** | **0.646** | **1,703** | **`-36.7%` WAF, `-68.0%` phys writes, `+1.0%` throughput** |

### C. Large Aligned Sequential Writes (`64 KiB`, `256 KiB`, `1 MiB`) & Bypass Ablation

| Workload | Engine | Throughput (`MiB/s`) | CPU (`ns/op`) | Alloc (`B/op`) | Total `WAF` | `WAL-WAF` | Speedup vs `journal-main` / `no-bypass` |
|---|---|---:|---:|---:|---:|---:|---|
| `large-seq-64k-aligned` (`32 MiB`) | `plain-disk` | 470.55 | 30,025 | 0 | 1.000 | 0.000 | reference |
| `large-seq-64k-aligned` (`32 MiB`) | `journal-main` | 176.21 | 122,664 | 10,489 | 2.001 | 1.001 | baseline |
| `large-seq-64k-aligned` (`32 MiB`) | `wsbuffer-no-bypass` | 123.21 | 172,084 | 20,046 | 2.005 | 1.005 | naive scrap double-copy regression (`0.70x`) |
| `large-seq-64k-aligned` (`32 MiB`) | **`wsbuffer-journal`** | **557.18** | **34,545** | **2** | **1.000** | **0.000** | **`3.16x` vs `journal-main` / `4.52x` vs `no-bypass`, `-71.8%` CPU** |
| `large-seq-256k-aligned` (`64 MiB`) | `plain-disk` | 842.04 | 82,336 | 0 | 1.000 | 0.000 | reference |
| `large-seq-256k-aligned` (`64 MiB`) | `journal-main` | 208.13 | 422,371 | 21,693 | 2.000 | 1.000 | baseline |
| `large-seq-256k-aligned` (`64 MiB`) | `wsbuffer-no-bypass` | 167.05 | 620,105 | 62,214 | 2.005 | 1.005 | naive scrap double-copy regression (`0.80x`) |
| `large-seq-256k-aligned` (`64 MiB`) | **`wsbuffer-journal`** | **867.18** | **99,164** | **3** | **1.000** | **0.000** | **`4.17x` vs `journal-main` / `5.19x` vs `no-bypass`, `-76.5%` CPU** |
| `large-seq-1m-aligned` (`64 MiB`) | `plain-disk` | 888.82 | 343,406 | 0 | 1.000 | 0.000 | reference |
| `large-seq-1m-aligned` (`64 MiB`) | `journal-main` | 231.76 | 2,074,656 | 181,024 | 2.000 | 1.000 | baseline |
| `large-seq-1m-aligned` (`64 MiB`) | `wsbuffer-no-bypass` | 195.11 | 2,270,859 | 380,151 | 2.005 | 1.005 | naive scrap double-copy regression (`0.84x`) |
| `large-seq-1m-aligned` (`64 MiB`) | **`wsbuffer-journal`** | **899.70** | **351,078** | **0** | **1.000** | **0.000** | **`3.88x` vs `journal-main` / `4.61x` vs `no-bypass`, `0 B/op`** |

### D. Mixed Overlapping Workload (`4 KiB` Unaligned + `256 KiB` Aligned, Verified)

| Workload | Engine | Throughput (`MiB/s`) | IOPS | CPU (`ns/op`) | Total `WAF` | `WAL-WAF` | Oracle Verified | Delta vs `journal-main` |
|---|---|---:|---:|---:|---:|---:|:---:|---|
| `mixed-4k-unaligned-256k-aligned` (`1024` ops, `67 MiB`) | `plain-disk` | 676.42 | 10,338 | 24,227 | 1.000 | 0.000 | Yes | reference |
| `mixed-4k-unaligned-256k-aligned` (`1024` ops, `67 MiB`) | `journal-main` | 227.81 | 3,482 | 100,544 | 2.001 | 1.001 | Yes | baseline |
| `mixed-4k-unaligned-256k-aligned` (`1024` ops, `67 MiB`) | **`wsbuffer-journal`** | **460.98** | **7,045** | **39,748** | **1.022** | **0.034** | **Yes** | **`2.02x` throughput, `-60.5%` CPU, `-48.9%` total WAF (`-96.6%` WAL bytes)** |

### E. Honest Negative Regimes

| Negative Regime | Engine | Throughput (`MiB/s`) | CPU (`ns/op`) | Total `WAF` | Backpressure Flushes | Finding |
|---|---|---:|---:|---:|---:|---|
| **#1: Sync after every `4 KiB` write (`batch=1`)** | `plain-disk` | 2.16 | 246,527 | 1.000 | 0 | `fdatasync` floor (257 syncs) |
| **#1: Sync after every `4 KiB` write (`batch=1`)** | `journal-main` | 1.05 | 407,359 | 2.010 | 0 | baseline (513 syncs) |
| **#1: Sync after every `4 KiB` write (`batch=1`)** | `wsbuffer-journal` | 1.09 | 331,914 | **2.022** | 0 | **No coalescing window:** WAF is `+0.6%` higher (`2.022` vs `2.010`) due to per-frame CRC/header overhead; throughput stays fsync-bound (`~1.0x`). |
| **#2: Strictly `1 MiB` aligned sequential vs `plain-disk`** | `plain-disk` | 888.82 | 343,406 | 1.000 | 0 | direct pwrite reference |
| **#2: Strictly `1 MiB` aligned sequential vs `plain-disk`** | `wsbuffer-journal` | 899.70 | **351,078** | 1.000 | 0 | **No speedup over direct disk (`~1.0x`):** direct bypass prevents double-copy regression (`4.61x` faster than `no-bypass`), but adds `+2.2%` CPU check overhead vs raw `plain-disk`. |
| **#3: Undersized scrap cap (`64 KiB`) on `16 MiB` uniform scatter** | `journal-main` | 103.32 | 14,965 | 2.010 | 0 | baseline |
| **#3: Undersized scrap cap (`64 KiB`) on `16 MiB` uniform scatter** | `wsbuffer-cap-64k` | 129.70 | 14,169 | **2.002** | **255** | **Premature eviction:** 255 backpressure flushes leave only `8/4096` coalesced writes, raising WAF to `2.002` (within `0.4%` of baseline) and `3,712 B/op`. |
| **#3: Undersized scrap cap (`64 KiB`) on `16 MiB` uniform scatter** | `wsbuffer-cap-4m` | 120.25 | 14,900 | **1.764** | 3 | Increasing `MaxScrapBytes` to `4 MiB` restores `492` coalesced writes (`-12.2%` WAF vs baseline). |

---

## 7. Reproducing

```sh
# 1. Verify zero regressions in existing journal on main
go test -race -count=1 ./journal/...

# 2. Run WSBuffer unit, concurrency, crash-recovery, and model-check tests
go test -v -race -count=1 ./pkg/wsbuffer/... ./cmd/wsbuffer-bench/...

# 3. Reproduce matched-baseline benchmarks and raw JSON output
go run ./cmd/wsbuffer-bench -reps 3 -json docs/research/hip-1345-wsbuffer/results.json
```
