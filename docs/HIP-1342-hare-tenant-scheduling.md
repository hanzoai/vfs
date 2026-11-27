# Per-tenant IO scheduling across sandboxes (HARE)

Refs #23. Tracked by HIP-1342. The library is `pkg/hare`, the CLI benchmark
runner is `tools/hare`, and the live multi-Volume tests are in `hare_test.go`
and `pkg/hare/scheduler_test.go`.

## 1. Primary reference, summary, and license audit

* **Primary paper:** HARE -- cache-centric multi-resource allocation for
  co-located storage tenants, 24th USENIX Conference on File and Storage
  Technologies (FAST 2026).
* **Summary:** When 32 to 128 `hanzoai/visor` sandboxes share one host node's
  decoded `vfs` cache, local NVMe journal capacity, S3 network uplink, and
  chunk/upload worker pool, cache size directly governs downstream S3 bandwidth
  and worker demand. Allocating cache according to each sandbox's online
  marginal miss-rate reduction (Miss-Ratio Curve, MRC) while scheduling worker
  slots, journal bytes, and S3 network tokens with Start-Time Fair Queuing (SFQ)
  and publication-age urgency prevents streaming/scan noisy neighbours from
  evicting interactive working sets or triggering global journal stalls.
* **License and clean-room audit:** Everything in `pkg/hare`, `tools/hare`,
  `hare_test.go`, and `docs/HIP-1342-hare-tenant-scheduling.md` is an
  independent, clean-room Apache-2.0 Go implementation using only the Go
  standard library, `github.com/zeebo/blake3` (already in `go.mod`), and
  `hanzoai/vfs`. `go.mod` and `go.sum` are untouched, zero background polling
  goroutines are used (`LLM.md` Rule 4), and all packages compile cleanly with
  `CGO_ENABLED=0` on `linux/amd64` and `darwin/arm64`.

## 2. Architecture (`pkg/hare`)

1. **Online Stack-Distance Miss-Ratio Curve (`mrcTracker`):**
   * Tracks exact per-sandbox distinct-block reuse distances (Mattson stack
     distance) inside a fixed 256-entry zero-allocation LRU stack across 64
     four-block buckets with periodic sliding-window halving so idle-to-active
     and phase transitions converge rapidly.
   * Preserves a scan-resistant `25%` weighted floor for every tenant while
     allocating the `75%` elastic pool proportionally to `weight * (0.02 +
     cacheUtility(floorBlocks))`, enforcing exact byte-for-byte quota
     conservation (`sum(CacheQuota) == CacheCapacity`) and reclaiming wasted
     cache from idle and zero-reuse scan/ingest sandboxes so interactive
     coding sandboxes hold their full hot working sets (`> 4x` interactive vs
     zero-reuse scan share across 128 sandboxes).
2. **Canonical VFS v2 Journal & Publication-Bound Integration (`ObserveJournalState`):**
   * Does **not** replace the canonical VFS v2 journal (`ledger.go`, `journal/`)
     or its `Config.Bound` publication timer / shared `vfs.Budget` admission
     path. Instead, hosts feed real `Volume.State().Backlog` and
     `Volume.State().Age` into `Scheduler.ObserveJournalState`, boosting S3
     drain bandwidth (`2.5x`) and SFQ priority (`0.25x` virtual cost) for any
     tenant whose oldest unpublished v2 journal record approaches `PublishBound`.
   * Per-tenant journal ceilings (`ReserveJournal` / `ReleaseJournal`) remain
     available for admission-layer isolation before aggregate node usage crosses
     `journal.slowFrom` (`75%`).
3. **Start-Time Fair Queuing (`Acquire` / `Ticket`) with Starvation Prevention & `TenantBackend`:**
   * Orders contended worker slots by virtual start tag `S_i = max(v(t),
     F_{i,k-1})` with bounded urgent-publish streak interleaving so continuous
     urgent publish floods cannot starve interactive readers, and cleanly wakes
     queued waiters with `ErrUnknownTenant` upon `UnregisterTenant`.
   * `hare.NewTenantBackend` wraps any `backend.Backend` so 32-128 live
     journaled `vfs.Volume` mounts share one `hare.Scheduler` and shared
     `vfs.Budget` transparently, including across S3 outages and backpressure.

## 3. Empirical evaluation across 32 and 128 sandboxes (HIP-1342)

Host Plain NVMe baseline measured beside every policy: **`777.6 MiB/s`**
sequential burst write, **`12.90 us` p50 / `86.60 us` p99** 4 KiB random read,
and **`930.60 us` p50 / `2,547.70 us` p99** `fsync`. Node resource pool:
`512 MiB` decoded cache, `8 GiB` NVMe journal, `2.0 GiB/s` S3 uplink, `16`
worker slots, with `75%` interactive toolchain sandboxes and `25%` streaming
scan + bulk-ingest noisy neighbours:

### A. 32 Sandboxes (`24` interactive + `8` scan/ingest noisy neighbours)

| Policy | Interactive Hit Rate | Jain's Fairness (`J`) | Interactive `p50` (`us`) | Interactive `p99` (`us`) | Max S3 Publish (`s`) | `<= 5 s` Bound |
|---|---:|---:|---:|---:|---:|---:|
| Plain local NVMe (single-host reference) | n/a | 1.0000 | 12.9 | 86.6 | 0.00 | n/a |
| Unscheduled FCFS + shared LRU | 66.67% | 0.3541 | 18.0 | 54,220.8 | 7.40 | **FAIL** |
| Static `1/N` equal partitioning | 100.00% | 0.9678 | 18.0 | 18.0 | 3.10 | PASS |
| **HARE cache-centric + SFQ** | **100.00%** | **0.9997** | **18.0** | **18.0** | **1.45** | **PASS** |

### B. 128 Sandboxes (`96` interactive + `32` scan/ingest noisy neighbours)

At `128` sandboxes on `512 MiB` cache (`4.0 MiB` per-tenant average), static
`1/N` strands `128 MiB` (`25%`) inside zero-reuse scan sandboxes and clips all
`96` interactive working sets to `75%` hit rate, whereas HARE reclaims elastic
cache from the `32` scan sandboxes:

| Policy | Interactive Hit Rate | Jain's Fairness (`J`) | Interactive `p50` (`us`) | Interactive `p99` (`us`) | Max S3 Publish (`s`) | `<= 5 s` Bound |
|---|---:|---:|---:|---:|---:|---:|
| Plain local NVMe (single-host reference) | n/a | 1.0000 | 12.9 | 86.6 | 0.00 | n/a |
| Unscheduled FCFS + shared LRU | 25.00% | 0.3541 | 47,168.0 | 113,203.2 | 7.40 | **FAIL** |
| Static `1/N` equal partitioning | 75.00% | 0.9678 | 18.0 | 16,896.0 | 3.10 | PASS |
| **HARE cache-centric + SFQ** | **100.00%** | **0.9997** | **18.0** | **18.0** | **1.45** | **PASS** |

### C. Negative & boundary controls

* **Homogeneous identical working sets (`32` identical tenants):** When no
  tenant has flat scan reuse, HARE reduces cleanly to equal shares (`J = 1.0000`,
  hit rate identical to static `1/N`: `98.4%` vs `98.4%`).
* **Single uncontended sandbox burst:** Lazy token accounting grants full node
  burst (`> 1.5 GiB/s` in-memory admission path; `Acquire` + `Done` bookkeeping
  overhead `~185 ns/op`, zero background goroutines).

## 4. Reproducing

```sh
go test -race -count=1 ./pkg/hare
go test -race -count=1 -run '^TestHARE' .
go run ./tools/hare
go run ./tools/hare -json
```
