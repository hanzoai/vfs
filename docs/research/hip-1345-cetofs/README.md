# Redundant Request Audit Across Host/Gofer, VFS, and S3 (CETOFS)

Refs `hanzoai/vfs#44`. Tracked by **HIP-1345**.

* **Canonical owning repository:** `hanzoai/vfs`
* **Core Go package:** `internal/cetofs` (`instrument.go`, `optimizer.go`, `guarantees.go`, `workloads.go`, `cetofs_test.go`)
* **Reproducible benchmark CLI:** `cmd/cetofs-bench` (`main.go`)
* **Machine-readable receipts:** `docs/research/hip-1345-cetofs/results.json`

---

## 1. Primary Reference, Method Transferability, and Clean-Room License Audit

* **Primary reference:** CETOFS — *"Eliminating Redundant Host-Storage Interactions in Disaggregated Storage"* (24th USENIX Conference on File and Storage Technologies, FAST 2026).
* **Why numbers do not transfer, while the method does:** CETOFS targets disaggregated NVMe-over-RDMA block storage where microsecond RDMA round trips dominate host-storage overhead. `hanzoai/vfs` runs over an S3-compatible object store (`hanzoai/s3`) with millisecond-scale HTTP RTTs, immutable content-addressed prolly-tree metadata shards (`meta/<2hex>/<blake3>`), 16 MiB packed chunk blobs (`pack/<2hex>/<id>`), post-quantum hybrid sealing (`X25519 + ML-KEM-768`), and a single round-fenced mutable `head` object enforced via server-side conditional writes (`PutIfVersion` / S3 `If-Match`). Consequently, CETOFS's RDMA microsecond numbers do not transfer, **but its boundary-accounting method transfers directly**: instrument every request crossing each architectural boundary per application operation, classify what is repeated, and delete only redundancy whose guarantee impact is explicitly preserved or strictly bounded.
* **Clean-room and licensing audit:** Everything in `internal/cetofs/`, `cmd/cetofs-bench/`, and `docs/research/hip-1345-cetofs/` is an independent Apache-2.0 clean-room Go implementation written from first principles using only the Go standard library and canonical `github.com/hanzoai/vfs` packages (`vfs`, `internal/blake3`, `pkg/backend`, `pkg/backend/file`, `pkg/fence`). `go.mod` and `go.sum` are completely untouched (zero new external dependencies, zero third-party code or fixtures).

---

## 2. Canonical Architecture & Instrumented Boundary Map

This work introduces **zero second server-side filesystems** and makes **zero modifications to compare-and-set (CAS) authority or the fenced Head** (`pkg/fence/fence.go`, `head.go`). All instrumentation and opt-in optimizations compose cleanly around canonical `*vfs.Volume` and `backend.Backend`:

```text
Application Workload (npm_ci | checkout_5000 | git_status | go_build)
  │
  │  [Visor sandbox boundary: application -> sentry -> gofer lives in hanzoai/visor;
  │   measured here at the host FUSE/Gofer -> *vfs.Volume seam matching pkg/mount/fuse_unix.go]
  ▼
Boundary 1 (fuse_gofer->vfs):
  CachedGoferBridge (opt-in bounded-TTL Entry/Attr/Dir cache + SkipRedundantOpenStat)
    └─ misses & all mutations ─► GoferBridge + AuditRecorder ─► *vfs.Volume (canonical)
                                                                      │
Boundary 2 (vfs->s3_backend):                                         ▼
  OptimizedBackend (opt-in Head ETag tracker, BLAKE3 ShardCache, ListCache, RangeCoalescer)
    └─ misses, writes & ALL CAS PutIfVersion ─► InstrumentedBackend + AuditRecorder
                                                  └─► LatencyBackend (2 ms S3 RTT)
                                                        └─► backend.Backend (file:// / S3)
```

### Key Architectural Invariants Preserved

1. **Exact `headAAD` place binding:** `InstrumentedBackend.String()`, `LatencyBackend.String()`, and `OptimizedBackend.String()` delegate verbatim to the underlying `backend.Backend.String()`, preserving `headAAD(v.be.String())` cryptographic place sealing (`head.go:162`).
2. **Server-side CAS authority untouched:** `OptimizedBackend.PutIfVersion` and `PutIfAbsent` ALWAYS execute directly against the authoritative underlying `backend.Backend`. No conditional write is ever downgraded to unconditional `Put`, and `fence.ErrStaleRound` is never bypassed.
3. **Synchronous local-mutation coherence:** `CachedGoferBridge` separates the dentry cache `(dir, name) -> childIno` from the inode attribute cache `ino -> vfs.Inode`. Any local `Write`, `SetAttr`, `Create`, `Mkdir`, or `Remove` synchronously evicts affected entries before returning, guaranteeing 100% read-your-own-writes coherence (including updated `Size`, `Mode`, and `Mtime` on subsequent `Lookup`/`Resolve`/`GetAttr`).
4. **Fenced-Head invalidation coupling:** Any `backend.ErrConflict` on `head` or observed external `head` version advance atomically bumps `OptimizedBackend.Generation()` and invokes `CachedGoferBridge.InvalidateAll()`.

---

## 3. Guarantee-Preserving Optimizations & Explicit Guarantee Statement Matrix

Per `hanzoai/vfs#44` acceptance criteria, every removed request class is programmatically cataloged in [`internal/cetofs/guarantees.go`](../../../internal/cetofs/guarantees.go) with the exact guarantee it had been protecting (or that it protected none):

| Redundancy Class | Boundary | Optimization Toggle | Guarantee Previously Protected | How Preserved / Bounded | Negative Regime |
|---|---|---|---|---|---|
| `repeated_getattr` | `fuse_gofer->vfs` | `AttrTimeout` (bounded LRU) | Cross-writer inode attribute freshness (`size`, `mtime`, `mode`, `nlink`) at single-syscall granularity. | 100% read-your-own-writes via synchronous eviction on local `Write`/`SetAttr`/`Create`/`Mkdir`/`Remove` + Head generation bump; cross-writer staleness strictly bounded to `AttrTimeout`. | External writer mutating attributes faster than `AttrTimeout` sees stale cached metadata until TTL expiry or Head generation invalidation. |
| `repeated_lookup` | `fuse_gofer->vfs` | `EntryTimeout` (bounded LRU, decoupled from `attrs`) | Immediate visibility of external directory entry creation, unlink, or rename on every path walk. | 100% local namespace coherence via synchronous eviction on `Create`/`Mkdir`/`Remove` + Head generation bump; cross-writer staleness bounded to `EntryTimeout`. | External writer adding/removing names faster than `EntryTimeout` sees stale positive entries until expiry or invalidation. |
| `repeated_readdir` | `fuse_gofer->vfs` | `DirCacheTTL` (bounded LRU caching entries + parent) | Immediate visibility of external directory membership changes on every `readdir` + `parent` lookup. | Immediate synchronous eviction of `DirCache[dir]` on any local child mutation and on Head generation bump; external staleness bounded to `DirCacheTTL`. | Externally churned directories within `DirCacheTTL` observe stale listings until expiry or invalidation. |
| `redundant_open_stat` | `fuse_gofer->vfs` | `SkipRedundantOpenStat` | **None beyond `AttrTimeout`**: `pkg/mount/fuse_unix.go:450` calls `vol.Stat` before `vol.Open` solely to test `in.IsDir()`, but an allocated inode ID never changes file type in place. | No guarantee weakened: cached directory inodes still return `vfs.ErrIsDir`; cache misses or unlinked inodes fall back to `vol.Stat` + `vol.Open` (`fs.ErrNotExist`). | None on valid POSIX volumes; cold cache falls back cleanly. |
| `redundant_pre_put_get` | `vfs->s3_backend` | `EnableHeadVersionCache` | Precondition ETag acquisition for server-side compare-and-set (`PutIfVersion` / S3 `If-Match`) before writing fenced `head`. | **100% CAS authority & fenced-Head monotonicity preserved**: every conditional write still runs server-side `PutIfVersion(key, data, cachedVer)`. Any `backend.ErrConflict` immediately invalidates cache and retries against authoritative `Get` + round check. | Continuous multi-writer CAS contention causes optimistic `PutIfVersion` to fail with `ErrConflict`, adding +1 failed PUT RTT (+50% backend RPCs). |
| `repeated_head_read` | `vfs->s3_backend` | `HeadCacheTTL` (default `0` = strict linearizable) | Linearizable observation of the latest externally committed `head` by independent readers (`Volume.Open`, `fence.Store.Round`). | Default `HeadCacheTTL == 0` keeps 100% linearizability. Opt-in `HeadCacheTTL > 0` bounds staleness to `HeadCacheTTL` and invalidates immediately on local writes or `ErrConflict`. | External Head commits or round takeovers within `HeadCacheTTL` are invisible to read-only viewers until TTL expiry or conflict invalidation. |
| `repeated_shard_read` | `vfs->s3_backend` | `EnableShardCache` (BLAKE3-verified LRU) | **None**: `meta/<2hex>/<blake3hex>` shards are immutable and content-addressed by the BLAKE3 digest of their sealed bytes (`SPECIFICATION 3`). | **100% content integrity preserved**: every cached `meta/` shard is verified against `blake3.Sum(sealed) == keyDigest` before insertion; tampered responses fail closed with `ErrIntegrityMismatch` and are never cached. | One-shot cold scans exceeding `MaxShardCacheBytes` incur bounded LRU eviction overhead with 0% reuse (zero correctness loss). |
| `repeated_list` | `vfs->s3_backend` | `ListCacheTTL` (prefix cache + immutable size learning) | Point-in-time visibility of objects created or deleted externally under the listed prefix (`meta/`, `pack/`). | Immediate synchronous eviction on any local `Put`/`PutIfAbsent`/`PutIfVersion`/`Delete` matching the prefix; external visibility bounded by `ListCacheTTL`. | External writers or garbage collectors mutating objects under the prefix within `ListCacheTTL` see stale listings until expiry. |
| `uncoalesced_range_get` | `vfs->s3_backend` | `EnableRangeCoalescing` + adaptive `EnableSequentialReadAhead` | **None for immutable `pack/` objects**: packs are write-once containers of individually authenticated sealed chunks (`pack.go`). | **100% chunk integrity & `GetRange` EOF contract preserved**: overflow-checked `int64` arithmetic (`ErrInvalidRange`), gap/window caps (`MaxCoalesceGap`, `MaxCoalesceWindow`), and exact pack-size clamping so reads never cross EOF. | Purely random sparse reads suffer severe read amplification (`105.6x`) if non-adaptive read-ahead is forced; adaptive adjacency gating keeps random read amplification at `1.00x`. |

---

## 4. Empirical Evaluation Across Four Canonical Workloads (Plain NVMe Beside Every Number)

All measurements below were captured on `linux/amd64` (2 CPUs, `go1.27.1`) with canonical post-quantum `vfs.Hybrid` (`X25519 + ML-KEM-768`), `file://` object store with `2.00 ms` simulated intra-region Hanzo S3 RTT (`cmd/cetofs-bench -rtt 2ms -files 5000`), and byte-for-byte identical seeded volumes (`Head.Seed`, shards, and packs) between unoptimized and optimized runs. Exact raw receipts are committed in [`results.json`](./results.json).

### 4.1 Summary: All 4 Workloads vs Plain NVMe Baseline

| Workload | Stack | App Ops | Wall (ms) | App Op Latency p50 / p95 / p99 (us) | FUSE->VFS Reqs (per op) | VFS->S3 Reqs (per op) | Total Reqs (per op) | Delta vs Unoptimized |
|---|---|---:|---:|---:|---:|---:|---:|---|
| **`npm_ci`** | Plain NVMe | 184 | 20.76 | 55.3 / 345.5 / 683.1 | 0 (0.00) | 0 (0.00) | 0 (0.00) | local NVMe reference |
| **`npm_ci`** | Unoptimized VFS | 184 | 356.21 | 44.5 / 4,934.1 / 16,001.2 | 1,842 (10.01) | 66 (0.36) | 1,908 (10.37) | baseline |
| **`npm_ci`** | **CETOFS-Optimized** | 184 | **164.01** | **43.6 / 722.9 / 5,286.0** | **1,200 (6.52)** | **18 (0.10)** | **1,218 (6.62)** | **-36.2% reqs (FUSE -34.9%, S3 -72.7%, wall -54.0%)** |
| **`checkout_5000`** | Plain NVMe | 5,000 | 356.27 | 46.2 / 128.3 / 308.5 | 0 (0.00) | 0 (0.00) | 0 (0.00) | local NVMe reference |
| **`checkout_5000`** | Unoptimized VFS | 5,000 | 1,103.68 | 22.5 / 83.9 / 211.7 | 40,056 (8.01) | 110 (0.02) | 40,166 (8.03) | baseline |
| **`checkout_5000`** | **CETOFS-Optimized** | 5,000 | **980.44** | **23.8 / 73.0 / 134.2** | **30,057 (6.01)** | **105 (0.02)** | **30,162 (6.03)** | **-24.9% reqs (FUSE -25.0%, S3 -4.5%, wall -11.2%)** |
| **`git_status`** | Plain NVMe | 10,100 | 64.00 | 3.4 / 5.2 / 44.7 | 0 (0.00) | 0 (0.00) | 0 (0.00) | local NVMe reference |
| **`git_status`** | Unoptimized VFS | 10,100 | 720.66 | 1.9 / 2.9 / 2,730.3 | 20,906 (2.07) | 164 (0.02) | 21,070 (2.09) | baseline |
| **`git_status`** | **CETOFS-Optimized** | 10,100 | **404.90** | **0.6 / 2.7 / 303.2** | **5,303 (0.53)** | **79 (0.01)** | **5,382 (0.53)** | **-74.5% reqs (FUSE -74.6%, S3 -51.8%, wall -43.8%)** |
| **`go_build`** | Plain NVMe | 251 | 19.21 | 29.0 / 191.7 / 386.4 | 0 (0.00) | 0 (0.00) | 0 (0.00) | local NVMe reference |
| **`go_build`** | Unoptimized VFS | 251 | 254.25 | 10.1 / 4,701.9 / 7,248.8 | 1,432 (5.71) | 44 (0.18) | 1,476 (5.88) | baseline |
| **`go_build`** | **CETOFS-Optimized** | 251 | **172.18** | **9.1 / 426.0 / 7,418.9** | **863 (3.44)** | **23 (0.09)** | **886 (3.53)** | **-40.0% reqs (FUSE -39.7%, S3 -47.7%, wall -32.3%)** |

### 4.2 Per-Boundary Operation Breakdown (Before -> After)

| Workload | `GetAttr` | `Lookup` | `ReadDir` | `Open` / `Read` / `Write` | `BackendGet` (`head`+`meta/`) | `BackendRangeGet` (`pack/`) | `BackendHead` | `BackendList` | `BackendPut` / `CondPut` | Bytes Read Parity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| **`npm_ci`** | 446 -> **2** | 524 -> **326** | 0 -> 0 | 304/162/142 (unchanged) | 13 -> **7** | 42 -> **2** | 0 -> 0 | 4 -> **2** | 6 / 1 (unchanged) | 1,794,048 B == 1,794,048 B |
| **`checkout_5000`** | 10,000 -> 10,001 | 10,000 -> **0** | 0 -> 0 | 5,000/0/5,000 (unchanged) | 6 -> **1** | 0 -> 0 | 0 -> 0 | 0 -> 0 | 99 / 5 (unchanged) | 0 B == 0 B (write-only) |
| **`git_status`** | 10,202 -> **1** | 10,402 -> **5,051** | 102 -> **51** | 100/100/0 (unchanged) | 64 -> 64 | 100 -> **10** | 0 -> 5 | 0 -> 0 | 0 / 0 (read-only) | 2,457,600 B == 2,457,600 B |
| **`go_build`** | 375 -> **28** | 450 -> **228** | 25 -> 25 | 276/250/26 (unchanged) | 9 -> 9 | 25 -> **3** | 0 -> 1 | 0 -> 0 | 8 / 2 (unchanged) | 1,996,800 B == 1,996,800 B |

### 4.3 Key Findings & Physical Interpretation

1. **`git_status` sees the largest total request drop (-74.5% total, -74.6% FUSE, -51.8% S3, -88.9% app p99 latency):** Unoptimized two-pass recursive walks over 5,000 files issue 10,202 `GetAttr` and 10,402 `Lookup` calls plus 100 individual `RangeGET`s over 5 packs. Bounded-TTL metadata caching eliminates 15,603 redundant FUSE crossings, and adaptive range coalescing collapses 100 pack `RangeGET`s into 10 coalesced spans (+5 one-time immutable pack `Stat`s), cutting S3 `RangeGET` cumulative wait from `358.19 ms` down to `30.69 ms`.
2. **`npm_ci` achieves the steepest S3 RPC reduction (-72.7% S3 requests, -54.0% wall time, -85.3% app p95 latency):** Across two cold sandbox worker boots reading the installed `node_modules` tree, BLAKE3-verified `ShardCache` removes repeated `meta/` shard GETs (`13 -> 7`), `ListCache` removes duplicate prefix listings (`4 -> 2`) while learning exact pack sizes with **zero** `BackendHead` calls, and `RangeCoalescer` collapses 42 pack `RangeGET`s into **2** (`149.93 ms -> 8.09 ms`).
3. **`checkout_5000` honestly remains write-dominated (-24.9% total requests, -4.5% S3 requests, -11.2% wall time):** All 5,000 `Create` / `Write` / `SetAttr` mutations, all 99 `meta/`+`pack/` `PUT`s, and all 5 conditional fenced-Head `PutIfVersion` commits are mandatory for durability and CAS fencing. CETOFS eliminates 10,000 redundant parent-path `Lookup`s and 5 redundant checkpoint `GET head`s (`6 -> 1`), cutting p99 file-checkout latency from `211.7 us` to `134.2 us`.
4. **`go_build` cuts both boundaries sharply (-39.7% FUSE, -47.7% S3, -32.3% wall time, -90.9% app p95 latency):** Eliminating repeated `go.mod`/`go.sum`/header walks (`GetAttr 375 -> 28`, `Lookup 450 -> 228`, `OpenStatSkipped: 225`) and coalescing 25 adjacent source-pack `RangeGET`s into 3 (`87.41 ms -> 11.04 ms`) delivers a clean 32.3% end-to-end build speedup while preserving 100% read parity (`1,996,800 B`) across all 25 compiled `.a` archives.

---

## 5. Negative Regimes & Boundary Controls

Every optimization has a regime where it degrades performance or introduces bounded staleness. All three negative regimes are explicitly benchmarked in `cmd/cetofs-bench` and tested in `TestNegativeMultiWriterRegime`:

| Negative Sub-Regime | Unoptimized / Control | Optimized / Contended Behavior | Measured Impact | Guarantee Outcome |
|---|---|---|---|---|
| **A. Multi-writer fenced-Head CAS contention** (20 commits where external Writer B advances `head` before every commit by Writer A) | Uncached `Get` + `PutIfVersion`: **40** backend RPCs (`309.84 ms`) | Cached `EnableHeadVersionCache`: **60** backend RPCs (`383.27 ms`, **20/20 `ErrConflict`**) | **+50.0% backend RPCs**, **+23.7% wall time** (failed optimistic CAS + fallback `Get` + retry `PutIfVersion`) | **100% CAS authority preserved** (`cas_authority_preserved: true`); deposed writers still receive `fence.ErrStaleRound` |
| **B. External mutation under `AttrTimeout`** (external writer grows file from `2 B` to `17 B` bypassing local bridge) | Immediate `vol.Stat`: 0 stale reads | Cached `GetAttr` within 5 s `AttrTimeout`: **3/3 stale reads** (`size=2`) until Head-generation invalidation | Bounded staleness up to `AttrTimeout` when external mutation is unobserved | **Immediate fresh recovery** (`size=17`, `fresh_after_invalidation: true`) as soon as `InvalidateAll()` fires |
| **C. Random non-adjacent pack reads** (40 random 4 KiB reads spaced 400 KiB apart across a 16 MiB pack; Plain NVMe `0.063 ms`) | Exact `GetRange`: `163,840 B` (`40` RPCs, `1.00x`) | **Aggressive `AlwaysReadAhead` (512 KiB)**: `17,301,504 B` (**`105.6x` read amp**); **Adaptive default (`EnableSequentialReadAhead`)**: `163,840 B` (`40` RPCs, **`1.00x`**) | Naive non-adaptive read-ahead wastes **105.6x** bandwidth; adaptive adjacency gate incurs **0%** read amplification | Byte-for-byte exact output; zero EOF violations |

**Operational guidance:** Keep `EnableHeadVersionCache` enabled for single-writer leased volumes (the canonical `hanzoai/vfs` ownership model) and disable it on actively contended multi-writer keys; keep `AlwaysReadAhead: false` (default) so random workloads never pay read amplification; tune `AttrTimeout`/`EntryTimeout` according to the acceptable cross-writer staleness budget (or `0` for strict uncached cross-writer mounts).

---

## 6. Verification Evidence, Security Review, and Rollout / Rollback Plan (`PR_CONVERGENCE.md`)

### Reproducing Commands

```sh
# 1. Format and static analysis
gofmt -l ./internal/cetofs ./cmd/cetofs-bench
go vet ./internal/cetofs/... ./cmd/cetofs-bench/...

# 2. Full unit, guarantee-proof, negative-regime, and race-detector suite
go test -race -count=1 -v ./internal/cetofs/...

# 3. Full 5,000-file benchmark suite + JSON receipt regeneration
go run ./cmd/cetofs-bench -rtt 2ms -files 5000 -out docs/research/hip-1345-cetofs/results.json
```

### Verification Summary

* `go vet ./internal/cetofs/... ./cmd/cetofs-bench/...`: **0 issues**
* `go test -race -count=1 -v ./internal/cetofs/...`: **9 / 9 tests PASS, 0 data races**
  * `TestGuaranteeCatalogCompleteness`
  * `TestAuditRecorderCountsLatenciesAndClassifiers`
  * `TestCASAuthorityAndFencedHeadMonotonicity`
  * `TestLocalMutationImmediateInvalidationAndTTL`
  * `TestShardCacheIntegrityAndListInvalidation`
  * `TestCoalesceRangesOverflowAndPackEOFClamping`
  * `TestConcurrentAccessRaceFree`
  * `TestNegativeMultiWriterRegime`
  * `TestEndToEndFourWorkloads`
* `go.mod` / `go.sum`: **untouched** (zero new dependencies).

### Security & Safety Review

* **CAS & Fencing:** Server-side `PutIfVersion` / `PutIfAbsent` is never bypassed; deposed writers at stale rounds fail closed with `fence.ErrStaleRound` (`TestCASAuthorityAndFencedHeadMonotonicity`).
* **Cryptographic Integrity:** `ShardCache` verifies `blake3.Sum(sealed) == keyDigest` before caching any `meta/<2hex>/<64hex>` shard, failing closed with `ErrIntegrityMismatch` on tampered bytes and returning defensive slice copies (`TestShardCacheIntegrityAndListInvalidation`).
* **Integer Overflow & Memory Bounds:** `CoalesceRanges` and `OptimizedBackend.GetRange` reject negative offsets/lengths and signed `int64` overflow (`off > math.MaxInt64 - len`) with `ErrInvalidRange`; sequential read-ahead clamps to exact known pack size (`TestCoalesceRangesOverflowAndPackEOFClamping`). Every cache (`attrs`, `entries`, `dirs`, `shards`, `packSpans`) is strictly bounded by entry or byte LRU caps.
* **Tenant & Key Isolation:** All caching operates per-`OptimizedBackend` / per-mount instance; no cross-volume or cross-tenant shared state exists, and `headAAD` place binding is preserved unchanged.

### Rollout & Rollback Plan

* **Rollout:** All components live in opt-in `internal/cetofs` and `cmd/cetofs-bench` without altering existing `vfs.Volume`, `pkg/fence`, or `pkg/mount` defaults. Production adoption in `pkg/mount` and `hanzoai/visor` can enable individual `OptimizerConfig` flags progressively (`EnableShardCache` -> `EnableRangeCoalescing` -> `SkipRedundantOpenStat` -> bounded `EntryTimeout`/`AttrTimeout`).
* **Rollback:** Set `OptimizerConfig{}` (`UnoptimizedConfig()`) to bypass all caching/coalescing immediately at runtime, or revert the single PR commit with zero on-disk format, Head schema, or S3 key layout changes.
