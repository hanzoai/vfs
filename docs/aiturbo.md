# Grouped checkpoint IO planning for bulk publication (AITURBO)

Refs #60. Tracked by HIP-1344. Convergence update responding to maintainer review on PR #113.

* Library: `pkg/aiturbo` (`counting.go`, `planner.go`, `downloader.go`, `aiturbo_test.go`)
* Root VFS / Real S3 integration: `aiturbo_test.go` (`TestAITURBOVolumeAndRealS3Integration`)
* Benchmark CLI: `tools/aiturbobench/main.go`
* Raw measurements: `docs/aiturbo.csv`

## 1. Primary reference, summary, and license audit

* **Primary reference:** AITURBO -- grouped IO planning and high-throughput checkpoint
  publication across cloud object storage, 24th USENIX Conference on File and
  Storage Technologies (FAST '26).
* **Canonical convergence design:** Rather than introducing a second filesystem
  runtime or wire format, `pkg/aiturbo` shares ONE bounded-stream execution path
  across both the matched parallel baseline (`GroupSize = 1`) and AITURBO grouped
  execution (`GroupSize = K`) directly over canonical `pkg/backend.Backend`,
  `pkg/fence.Store`, and `internal/blake3`.
  * **Coalesced immutable packs (`pack/<2hex>/<blake3>`):** Small and medium objects
    below `LargeBypassBytes` (`4 MiB`) are aggregated up to `GroupSize` items or
    `TargetGroupBytes` (`16 MiB`, aligned with `vfs.packTarget`) into a single
    content-addressed pack written once with `backend.PutIfAbsent`. Large items
    bypass coalescing buffers directly.
  * **Immutable data reuse & abort safety:** `PutIfAbsent` conflicts (`backend.ErrConflict`)
    verify exact stored size via `Stat` and count toward immutable reuse rather than
    overwriting. Partial-upload aborts and context cancellations NEVER delete
    content-addressed objects already written, avoiding TOCTOU deletion across
    concurrent publishers.
  * **Strict Head 412 CAS exclusion:** `"head"` writes cannot enter data batches
    (`ErrHeadCoalesceForbidden`) and commit strictly last via `pkg/fence.Store.Put`
    (`PutIfVersion` / HTTP 412 `If-Match`).
  * **Coalesced `GetRange` recovery:** `GroupedDownloader` merges nearby reads inside
    the same pack (within `CoalesceGapBytes = 64 KiB` and `MaxCoalescedRange = 16 MiB`)
    into fewer `GetRange` requests and verifies every item's `BLAKE3` digest.
* **Clean-room license audit:** Independent Apache-2.0 Go implementation using only
  the Go standard library and existing `github.com/hanzoai/vfs` packages. `go.mod`
  and `go.sum` are untouched; builds cleanly with `CGO_ENABLED=0` on `linux/amd64`
  and `darwin/arm64`.

## 2. Matched Baselines & Hypotheses

* **Matched Parallel Baseline (`GroupSize = 1`, `Streams = 8`):** Dispatches each
  object via its own `PutIfAbsent` and fetches each via its own `GetRange` across
  8 concurrent workers, with a fenced `head` commit at the end.
* **AITURBO Policy (`GroupSize = 8`, `Streams = 8`):** Identical scheduler, same 8
  concurrent workers, same `CountingBackend` wire observer, same `fence.Store` Head
  CAS, packing small/medium objects up to `K = 8` per immutable pack and coalescing
  contiguous `GetRange` reads.
* **H1 (Real wire request reduction):** Packing reduces measured `PutIfAbsent` calls
  and S3 HTTP requests proportionally to grouping factor `K` (minus 2 fixed metadata +
  head requests) while adding `< 0.2%` manifest metadata overhead.
* **H2 (Matched-parallel upload/download latency):** Grouping reduces per-object
  commit overhead (`flock` + disk sync on `file://`, HTTP setup + RTT on S3) under
  identical `Streams = 8` concurrency.
* **H3 (Fencing, reuse, & abort safety):** Stale rounds fail with 412 CAS
  (`fence.ErrStaleRound`), overlapping immutable packs reuse existing objects on
  `ErrConflict`, and mid-upload fault/cancellation preserves the prior committed Head
  and leaks zero goroutines.

## 3. Empirical Measurements (Host Outputs — NOT Hanzo Production Expectations)

### 3A. Canonical `file://` Backend (`128 × 64 KiB` = `8,388,608` logical bytes, `Streams = 8`)

Command reproduced cleanly on `linux/amd64`:
`TMPDIR=/home/max/tmp go run ./tools/aiturbobench -backend=file -items=128 -item-size=65536 -streams=8 -group-size=8 -rtts=0,5,15 -csv=docs/aiturbo.csv`

| Mode | Injected RTT | Real Upload Reqs | Upload Bytes | Upload Duration | Real Download Reqs | Download Duration |
|---|---:|---:|---:|---:|---:|---:|
| `baseline-parallel` (`g=1`) | 0 ms (native) | 130 | 8,402,826 | 7,255.02 ms | 130 | 19.21 ms |
| **`aiturbo-g8`** | **0 ms (native)** | **18** | **8,402,826** | **1,344.55 ms (5.40x)** | **18** | **16.05 ms (1.20x)** |
| `baseline-parallel` (`g=1`) | 5 ms (sim) | 130 | 8,402,826 | 6,762.73 ms | 130 | 108.33 ms |
| **`aiturbo-g8`** | **5 ms (sim)** | **18** | **8,402,826** | **1,521.80 ms (4.44x)** | **18** | **37.17 ms (2.91x)** |
| `baseline-parallel` (`g=1`) | 15 ms (sim) | 130 | 8,402,826 | 6,564.19 ms | 130 | 292.95 ms |
| **`aiturbo-g8`** | **15 ms (sim)** | **18** | **8,402,826** | **1,382.63 ms (4.75x)** | **18** | **72.67 ms (4.03x)** |

*Honest limits:* `file://` atomic write cost (`flock` + staging rename) dominates upload time on local storage regardless of 0–15 ms injected RTT; download `0 ms` sees only `1.20x` speedup because local page-cache `pread` dominates over 8-way concurrency, rising to `4.03x` at 15 ms simulated RTT as request round-trips dominate.

### 3B. Canonical S3 HTTP & Real `hanzoai/s3` v1.0.21 Server Measurements

* `TestAITURBOMatchedParallelBaselineOverS3HTTP` (`64 × 16 KiB` = `1 MiB`, `Streams=8` over `storetest.NewS3` + `pkg/backend/s3`):
  * Upload wire writes: `66` requests (`67` total S3 HTTP calls) baseline vs `10` requests (`11` total S3 HTTP calls) AITURBO.
  * Download `GetRange` calls: `64` baseline vs `8` AITURBO (`8.0x`).
* `TestAITURBOVolumeAndRealS3Integration` (`32 × 8 KiB`, `Streams=8`, full `vfs.Volume` + `Hybrid` cipher + real `hanzoai/s3` binary via `tools/ci/s3server.sh`):
  * `storetest-s3`: canonical `vfs.Volume` flush `10` writes (`283,013` bytes); matched parallel baseline `34` PUTs vs AITURBO `6` PUTs and `4` coalesced `GetRange` reads.
  * `real-hanzo-s3`: canonical `vfs.Volume` flush `10` writes (`283,029` bytes); matched parallel baseline `34` PUTs vs AITURBO `6` PUTs and `4` coalesced `GetRange` reads.

## 4. Verification Summary

* `go vet . ./pkg/aiturbo ./tools/aiturbobench` clean.
* `CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./pkg/aiturbo ./tools/aiturbobench` clean.
* `go test -v -race -count=1 ./pkg/aiturbo`: 5/5 tests passing (head separation, matched S3 HTTP baseline, immutable reuse & concurrent abort safety, 412 CAS fencing, partial failure & cancellation leak check).
* `go test -v -count=1 -run '^TestAITURBO' .` with real `hanzoai/s3` on `PATH`: PASS (`storetest-s3` + `real-hanzo-s3`).

## 5. Settlement Coordinates

* **Network:** Base Mainnet (Chain ID 8453)
* **Asset:** USDC
* **Address:** `0xf084cF0076F91AA48C2370F427C399dAA8C1f07F`
