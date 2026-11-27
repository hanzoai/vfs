# ParaSync: Parallel Delta Restore Between Checkpoints

Refs #64. Tracked by HIP-1344. The implementation lives in root `package vfs`
([`parasync.go`](../parasync.go), [`parasync_unix.go`](../parasync_unix.go),
[`parasync_other.go`](../parasync_other.go)), the integration and adversarial
test suite is in [`parasync_test.go`](../parasync_test.go), the reproducible
benchmark runner is [`tools/parasyncbench/main.go`](../tools/parasyncbench/main.go),
and the full raw measurements are in [`docs/parasync.csv`](parasync.csv).

## 1. Paper Audit and Clean-Room Attribution

* **Primary paper:** ParaSync (24th USENIX Conference on File and Storage
  Technologies, FAST '26): parallel delta checkpoint restore/transfer over
  content-addressed tree snapshots and concurrent object transfers.
* **Clean-room Apache-2.0 audit:** Everything in `parasync.go`,
  `parasync_unix.go`, `parasync_other.go`, `parasync_test.go`, and
  `tools/parasyncbench/main.go` is an independent, clean-room Apache-2.0 Go
  implementation authored from first principles for `hanzoai/vfs` using only
  the Go standard library and existing repository packages (`pkg/backend`,
  `pkg/fence`, `internal/blake3`, `internal/storetest`). `go.mod`, `go.sum`,
  `go.work`, `.github/workflows/*`, `hanzo.yml`, `tools/mutate/mutations.json`,
  and frozen format-v2 wire layouts (`head.go`, `record.go`, `shard.go`,
  `pack.go`) are completely untouched.
* **Canonical stack boundaries preserved:** `vfs -> hanzoai/s3 -> visor` is
  unchanged, and Rule 1 of `LLM.md` holds unconditionally: no plaintext file
  paths, file names, or unauthenticated plaintext ever reach backend keys or
  objects, and pack payloads (`pack/<2hex>/<id>`) are transferred as raw
  ciphertext without decryption or re-encryption.

## 2. HIP-1344 Six-Item Design and Safety Contract

1. **Zero format-v2 changes & ciphertext-only pack transport (`RestoreDelta`):**
   * Reuses the canonical object classes (`head`, `meta/<2hex>/<hash>`,
     `pack/<2hex>/<id>`, and optional `ext/parasync/<keyed-hash>`) without
     introducing any second pack, shard, or encryption format.
   * `pack/<2hex>/<id>` objects are read from `src` and written to `dst`
     directly as raw sealed ciphertext via `PutIfAbsent`, preserving identical
     128-bit pack IDs and chunk offsets.
2. **Authenticated planning & O(1) prolly-tree pruning (`PlanDelta`):**
   * Validates the `Cipher` via `check(c)` (`ErrBadCipher`), reads and unseals
     `src` `head` with `headAAD(src.String())` (`ErrUnknownKey`, `ErrCorrupt`,
     `ErrNeeds` fail closed).
   * Attempts to read `dst` `head` with `headAAD(dst.String())`. When absent
     (`backend.ErrNotFound`), sets `HasDst = false` and cleanly degrades into a
     parallel full restore through the single unified code path. When present,
     verifies `dstHead.ID == srcHead.ID` and `dstHead.Seed == srcHead.Seed`,
     failing closed with `ErrCorrupt` on any cross-volume or cross-tenant
     mismatch.
   * Traverses `dstHead.Root` (when `HasDst`) and `srcHead.Root` in bounded
     level-by-level parallel BFS, verifying every visited shard against
     `blake3.Sum(sealed) == ref`, `unseal(c, shardAAD(ID), sealed)`, and strict
     parent/child level monotonicity (`child.level == parent.level - 1`),
     pruning any `src` subtree whose `ref` is already reachable in `dstHead.Root`
     in O(1) without fetching its subtree from `src`.
   * Collects all `[tChunk]` (`decodeWhere`) `(packID, off+n)` requirements
     across both unpruned `src` leaf shards and pruned `dst` leaf shards, checks
     `dst.Stat(packKey(id))` concurrently, and prunes any pack already present
     on `dst` with `size >= requiredEnd`. Computes exact `MetaBytes`,
     `PackBytes`, `FullMetaBytes`, `FullPackBytes`, and `FullSnapshotBytes`.
3. **Canonical write order, bounded concurrency & GC race handling (`RestoreDelta`):**
   * Enforces canonical `LLM.md` Rule 3 ordering: `packs -> shards -> verifyRestoredTree -> head`.
   * Executes pack and shard transfers across at most `opts.Streams` (default
     8) worker goroutines using `PutIfAbsent`, checking `dst.Stat` on
     `backend.ErrConflict` and subsequently verifying every referenced chunk's
     AEAD tag and BLAKE3 plaintext hash in `verifyRestoredTree` before `head`
     CAS.
   * Retains unpruned shard ciphertexts fetched during planning up to a bounded
     `128 MiB` staging budget (`maxParaSyncStagedShardBytes`) so `RestoreDelta`
     avoids double-fetching metadata shards from `src`.
   * If a planned shard or pack on `src` disappears (`backend.ErrNotFound`) due
     to concurrent source checkpoint GC, returns `ErrSourceCheckpointGCed`
     (wrapping both `ErrCorrupt` and `backend.ErrNotFound`) and leaves `dst`
     `head` untouched.
   * If a pruned resident shard or pack on `dst` disappears between `PlanDelta`
     and `verifyRestoredTree` due to concurrent destination GC, `verifyRestoredTree`
     re-fetches the missing object from `src`, writes it via `PutIfAbsent`
     (`Stats.DestGCRecoveredPacks`), and verifies all chunks before advancing
     `head`.
4. **Full end-to-end cryptographic chunk & tree verification (`verifyRestoredTree`):**
   * Before touching `dst` `head`, constructs a read-only `Volume` adopted to
     `srcHead` over a separate verification counting wrapper (`VerifyBytesRead`,
     `VerifyGets`, `ChunksVerified`), loads and verifies every shard reachable
     from `srcHead.Root` on `dst` in parallel, and executes `checkTree(ctx, true)`
     (verifies root inode, every `[tData]` extent's `[tChunk]` entry, and every
     pack length via `dst.Stat`).
   * Because `packID` is a 128-bit random identifier rather than a content hash
     and `checkTree(ctx, true)` checks only `Stat` length, `verifyRestoredTree`
     walks all `[tChunk]` and `[tData]` records in `srcHead.Root`, groups all
     referenced chunks by `packID`, reads the pack bytes from `dst` in parallel
     (`Streams`), and for **every referenced chunk** verifies:
     1. `plain, err := unseal(c, chunkAAD(head.ID, id), frame)` (AEAD tag and
        `(volumeID, chunkID)` AAD context check).
     2. `sumChunk(plain) == id` (canonical BLAKE3 plaintext digest check).
   * Any same-length bit-flipped resident pack or same-length conflicting pack
     substitution fails closed with `ErrCorrupt` before `head` CAS, leaving any
     prior `dst` checkpoint intact.
   * Also supports an optional Cipher-sealed integrity receipt (`SaveVerifyReceipt`
     / `TrustSealedReceipt`) stored under `ext/parasync/<keyed-hash>` and sealed
     with `extAAD(head.ID, "parasync", rest)` binding `(packID, dstVersion,
     packSize, chunkCount)` under `Cipher` when explicitly opted in, while
     keeping full chunk AEAD + BLAKE3 verification ON by default.
5. **Round-fenced atomic head commit & lost Head ACK recovery (`pkg/fence`):**
   * Requires `opts.Round > 0 && opts.Round < math.MaxUint64` (`fs.ErrInvalid`)
     and `opts.Round > recorded` (`ErrStaleRound`), enforced both early on
     `dstHead` and atomically at commit time via `fence.Store.Rewrite`
     (`PutIfAbsent` / `PutIfVersion`, HTTP 412 CAS).
   * Reseals `srcHead` to `headAAD(dst.String())` so moved/restored volumes
     open cleanly at `dst` without exposing plaintext or cross-place tokens.
   * If `dstFence.Rewrite` returns an error (such as a lost HTTP 200 ACK after
     the conditional `head` write landed), `parasyncHeadLanded` inspects
     `dstFence.Get(ctx, "head")`, unseals with `headAAD(dst.String())`, and if
     the stored `Head` matches `plan.SrcHead` with `recorded >= opts.Round`,
     sets `Stats.LostHeadACKRecovered = true` and returns `nil` (matching
     `generation.go:landed`).
6. **Fail-closed error, cancellation & key-rotation semantics:**
   * Mid-pack failure, mid-shard failure, `context.Canceled`, corrupt shard
     bytes, same-length corrupt or substituted pack bytes, mismatched `VolumeID`,
     wrong key (`ErrUnknownKey`), or unknown `Head.Needs` bits (`ErrNeeds`)
     leave any prior `dst` checkpoint completely intact and readable.
   * Key rotation via `Hybrid(newKey, oldKey)` opens source objects sealed under
     retired keys and reseals `dst` `head` under `newKey`.

## 3. Falsifiable Hypotheses

| ID | Hypothesis | Outcome | Evidence (`docs/parasync.csv` & `parasync_test.go`) |
|---|---|---|---|
| **H1** | On multi-pack append/update workloads (`packed_append`), matched parallel `RestoreDelta` (`Streams=8`) cuts transferred bytes by >75% and reduces `SourceGets` and wall time against matched parallel `full_par` (`Streams=8`) across simulated WAN RTT (`2.0 ms`), disk `file://`, HTTP `storetest.NewS3`, and real pinned Hanzo S3 (`ghcr.io/hanzoai/s3` release v1.0.41), while explicitly accounting for full cryptographic chunk verification traffic (`VerifyBytesRead`, `VerifyGets`, `ChunksVerified`). | **Confirmed** | `packed_append` saves `83.30%` transferred bytes (`2,109,547 / 12,632,523 B`), cuts `SourceGets` `8 -> 3`, verifies all `24` chunks (`VerifyBytesRead = 12,632,523 B`, `VerifyGets = 7`), and speeds wall time `1.26x` on `mem` `2.0 ms` sim RTT (`167.89 ms` vs `212.31 ms`), `1.73x` on `file://` (`1,670.63 ms` vs `2,897.83 ms`), `1.51x` on `storetest.S3` `Streams=4` (`251.83 ms` vs `381.01 ms`), and `4.91x` on real Hanzo S3 (`1.12 s` vs `5.48 s`). At unthrottled `0.0 ms` `mem`, `delta_par` and `full_par` are at parity (`214.72 ms` vs `209.28 ms`) because both unseal and BLAKE3-hash all `24` chunks (`12.63 MiB`) in RAM before `head` CAS. |
| **H2** | On localized inline-heavy updates (`inline_localized`, 5% edit in one directory), prolly-tree pruning skips untouched subtrees (>70% shards pruned) and beats `full_par` on `mem`, `file://`, `storetest.NewS3`, and real Hanzo S3. | **Confirmed** | `inline_localized` prunes `19/22` shards on `mem` (`82.70%` bytes saved, `SourceGets` `23 -> 4`, `35.84 ms` vs `50.47 ms` at `0.0 ms`), prunes `26/30` shards on `file://` (`88.76%` saved, `1,073.74 ms` vs `8,402.27 ms`, `7.83x` faster), prunes `71/76` shards on `storetest.S3` (`135.78 ms` vs `214.34 ms`, `1.58x` faster), and prunes `73/75` shards on real Hanzo S3 (`2.25 s` vs `30.80 s`, `13.70x` faster). |
| **H3** | **Negative / shard-granularity boundary (`inline_scattered_neg`):** Scattered small-file edits across all directories dirty most 64 KiB shards, collapsing byte savings and increasing total backend ops and CPU time on `mem`. | **Confirmed** | Editing `~6.6%` of files (1 in 15 per directory) saves only `11.20%` bytes on `mem` (`28/38` shards copied), increases total backend ops (`118 -> 136`), raises CPU (`147.23 ms -> 180.72 ms` at `0.0 ms`; `151.32 ms -> 175.20 ms` at `2.0 ms`), and slows wall time at `2.0 ms` sim RTT (`124.90 ms` vs `116.22 ms`). |
| **H4** | **Negative / 16 MiB pack-granularity boundary (`pack_granularity_neg`):** Skipping an intermediate checkpoint whose single ~15 MiB ciphertext pack mixes ephemeral deleted chunks with a 300 KiB retained file forces copying the whole ciphertext pack (~49x live logical delta amplification) and reading all `17.14 MiB` during chunk verification. | **Confirmed** | Only `300 KiB` of live logical data is new relative to `dst`, yet `delta_par` transfers `15,039,907 B` (`12.28%` saved vs `17,144,499 B` full) and reads `17,144,499 B` (`30` chunks verified) during pre-commit verification. |

## 4. Measured Benchmarks and Matched Parallel Baselines

All measurements below were captured directly on a 2-core Intel Celeron 3865U
host (`1.80 GHz`, `GOMAXPROCS=2`, Go `1.27.1`) using real `backend.Backend`
counters, real `time.Now()`, real `syscall.Getrusage` process CPU time, and
real `runtime.MemStats`. Both transfer traffic (`BytesTransferred`, `SourceGets`)
and destination pre-commit verification traffic (`VerifyBytesRead`,
`VerifyGets`, `ChunksVerified`) are reported separately.

### 4.1 Matched Parallel Baselines (`tools/parasyncbench`, `mem` backend)

| Workload | Sim RTT | Mode (`Streams`) | Transferred / Full (B) | Saved | VerifyRead (B) | VerifyGets | ChunksVer | Shards (C/P) | Packs (C/P) | SrcGets | Total Ops | Wall (ms) | CPU (ms) | PeakG |
|---|---:|---|---:|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---:|
| `packed_append` | `0.0 ms` (real) | `full_s1` (1) | 12,632,523 / 12,632,523 | 0.0% | 12,632,523 | 7 | 24 | 1/0 | 6/0 | 8 | 49 | 423.71 | 246.93 | 1 |
| `packed_append` | `0.0 ms` (real) | `full_par` (8) | 12,632,523 / 12,632,523 | 0.0% | 12,632,523 | 7 | 24 | 1/0 | 6/0 | 8 | 49 | 209.28 | 240.89 | 7 |
| `packed_append` | `0.0 ms` (real) | **`delta_par` (8)** | **2,109,547 / 12,632,523** | **83.3%** | **12,632,523** | **7** | **24** | **1/0** | **1/5** | **3** | **35** | **214.72** | **200.12** | 7 |
| `packed_append` | `2.0 ms` (sim) | `full_s1` (1) | 12,632,523 / 12,632,523 | 0.0% | 12,632,523 | 7 | 24 | 1/0 | 6/0 | 8 | 49 | 394.84 | 253.60 | 1 |
| `packed_append` | `2.0 ms` (sim) | `full_par` (8) | 12,632,523 / 12,632,523 | 0.0% | 12,632,523 | 7 | 24 | 1/0 | 6/0 | 8 | 49 | 212.31 | 260.49 | 7 |
| `packed_append` | `2.0 ms` (sim) | **`delta_par` (8)** | **2,109,547 / 12,632,523** | **83.3%** | **12,632,523** | **7** | **24** | **1/0** | **1/5** | **3** | **35** | **167.89** | **198.65** | 7 |
| `inline_localized` | `0.0 ms` (real) | `full_par` (8) | 1,318,101 / 1,318,101 | 0.0% | 1,318,101 | 22 | 0 | 22/0 | 0/0 | 23 | 70 | 50.47 | 84.90 | 9 |
| `inline_localized` | `0.0 ms` (real) | **`delta_par` (8)** | **228,095 / 1,318,101** | **82.7%** | **1,318,101** | **22** | **0** | **3/19** | **0/0** | **4** | **54** | **35.84** | **60.51** | 9 |
| `inline_localized` | `2.0 ms` (sim) | `full_s1` (1) | 1,318,101 / 1,318,101 | 0.0% | 1,318,101 | 22 | 0 | 22/0 | 0/0 | 23 | 70 | 244.92 | 85.90 | 1 |
| `inline_localized` | `2.0 ms` (sim) | `full_par` (8) | 1,318,101 / 1,318,101 | 0.0% | 1,318,101 | 22 | 0 | 22/0 | 0/0 | 23 | 70 | 63.35 | 77.02 | 9 |
| `inline_localized` | `2.0 ms` (sim) | **`delta_par` (8)** | **228,095 / 1,318,101** | **82.7%** | **1,318,101** | **22** | **0** | **3/19** | **0/0** | **4** | **54** | **68.07** | **62.38** | 9 |
| `inline_scattered_neg` | `0.0 ms` (real) | `full_par` (8) | 2,575,637 / 2,575,637 | 0.0% | 2,575,637 | 38 | 0 | 38/0 | 0/0 | 39 | 118 | 140.59 | 147.23 | 9 |
| `inline_scattered_neg` | `0.0 ms` (real) | `delta_par` (8) | 2,287,089 / 2,575,637 | 11.2% | 2,575,637 | 38 | 0 | 28/10 | 0/0 | 29 | 136 | 129.38 | 180.72 | 9 |
| `inline_scattered_neg` | `2.0 ms` (sim) | `full_par` (8) | 2,575,637 / 2,575,637 | 0.0% | 2,575,637 | 38 | 0 | 38/0 | 0/0 | 39 | 118 | 116.22 | 151.32 | 9 |
| `inline_scattered_neg` | `2.0 ms` (sim) | `delta_par` (8) | 2,287,089 / 2,575,637 | 11.2% | 2,575,637 | 38 | 0 | 28/10 | 0/0 | 29 | 136 | 124.90 | 175.20 | 9 |
| `pack_granularity_neg` | `0.0 ms` (real) | `full_par` (8) | 17,144,499 / 17,144,499 | 0.0% | 17,144,499 | 3 | 30 | 1/0 | 2/0 | 4 | 21 | 305.05 | 342.30 | 3 |
| `pack_granularity_neg` | `0.0 ms` (real) | `delta_par` (8) | 15,039,907 / 17,144,499 | 12.3% | 17,144,499 | 3 | 30 | 1/0 | 1/1 | 3 | 19 | 300.87 | 314.74 | 3 |
| `pack_granularity_neg` | `2.0 ms` (sim) | `full_par` (8) | 17,144,499 / 17,144,499 | 0.0% | 17,144,499 | 3 | 30 | 1/0 | 2/0 | 4 | 21 | 485.67 | 331.80 | 3 |
| `pack_granularity_neg` | `2.0 ms` (sim) | `delta_par` (8) | 15,039,907 / 17,144,499 | 12.3% | 17,144,499 | 3 | 30 | 1/0 | 1/1 | 3 | 19 | 343.05 | 327.92 | 3 |
| `noop_identical` | `0.0 ms` (real) | `full_par` (8) | 3,828,875 / 3,828,875 | 0.0% | 3,828,875 | 29 | 5 | 28/0 | 1/0 | 30 | 95 | 118.55 | 159.27 | 9 |
| `noop_identical` | `0.0 ms` (real) | **`delta_par` (8)** | **0 / 3,828,875** | **100.0%** | **3,828,875** | **29** | **5** | **0/28** | **0/1** | **1** | **64** | **63.98** | **92.33** | 9 |
| `noop_identical` | `2.0 ms` (sim) | `full_par` (8) | 3,828,875 / 3,828,875 | 0.0% | 3,828,875 | 29 | 5 | 28/0 | 1/0 | 30 | 95 | 158.74 | 152.99 | 9 |
| `noop_identical` | `2.0 ms` (sim) | **`delta_par` (8)** | **0 / 3,828,875** | **100.0%** | **3,828,875** | **29** | **5** | **0/28** | **0/1** | **1** | **64** | **96.70** | **96.79** | 9 |

### 4.2 Real Disk (`file://`), HTTP S3 (`storetest.NewS3`), and Pinned Real Hanzo S3 (`ghcr.io/hanzoai/s3` release v1.0.41)

* **`file://` local ext4 disk backend (unthrottled `0.0 ms`, `docs/parasync.csv`):**
  * `packed_append`: `full_par` `2,897.83 ms` (`12,632,523 B` transferred, `12,632,523 B` verified across `24` chunks) vs `delta_par` `1,670.63 ms` (`2,109,547 B` transferred — `83.30%` saved, `12,632,523 B` verified across `24` chunks, `1.73x` faster); `delta_s1` `1,364.29 ms` (`2.12x` faster).
  * `inline_localized`: `full_par` `8,402.27 ms` (`1,332,264 B` transferred, `1,332,264 B` verified) vs `delta_par` `1,073.74 ms` (`149,804 B` transferred — `88.76%` saved, `1,332,264 B` verified, `7.83x` faster).
  * `inline_scattered_neg`: `full_par` `17,860.50 ms` (`2,582,788 B` transferred) vs `delta_par` `5,746.58 ms` (`2,194,036 B` transferred — `15.05%` saved).
  * `pack_granularity_neg`: `full_par` `1,251.45 ms` (`17,148,212 B` transferred, `32` chunks verified) vs `delta_par` `1,604.46 ms` (`15,041,864 B` transferred — `12.28%` saved, `17,148,212 B` verified).
  * `noop_identical`: `full_par` `11,326.62 ms` (`3,835,856 B` transferred, `3` chunks verified) vs `delta_par` `329.73 ms` (`0 B` transferred, `3,835,856 B` verified, **`34.35x` faster** because all 34 shard + 1 pack `fsync` writes are skipped).
* **`storetest.NewS3` over HTTP (`TestParaSyncMatchedBaselineAndRealS3/storetest-s3`, `Streams=4`, loopback):**
  * `inline`: `full` `214.34 ms` (`394.73 ms` CPU, `4,147,848 B` transferred, `4,147,848 B` verify read, `76` verify GETs, `77` src GETs, `232` HTTP reqs) vs `delta` `135.78 ms` (`243.25 ms` CPU, `183,961 B` transferred — `95.56%` saved, `4,147,848 B` verify read, `76` verify GETs, `6` src GETs, `71/76` shards pruned, `166` HTTP reqs).
  * `packed`: `full` `381.01 ms` (`565.00 ms` CPU, `12,638,064 B` transferred, `12,638,064 B` verify read, `7` verify GETs, `27` chunks verified, `6` packs, `8` src GETs, `49` HTTP reqs) vs `delta` `251.83 ms` (`343.63 ms` CPU, `2,108,144 B` transferred — `83.32%` saved, `12,638,064 B` verify read, `7` verify GETs, `27` chunks verified, `1/6` packs copied, `5` pruned, `3` src GETs, `35` HTTP reqs).
* **Pinned real Hanzo S3 (`TestParaSyncMatchedBaselineAndRealS3/hanzo-s3`, `tools/ci/s3server.sh` `ghcr.io/hanzoai/s3@sha256:64527e84...` release v1.0.41, `Streams=4`):**
  * `inline`: `full` `30.80 s` (`388.00 ms` CPU, `4,146,041 B` transferred, `4,146,041 B` verify read, `75` verify GETs, `76` src GETs) vs `delta` `2.25 s` (`217.75 ms` CPU, `111,719 B` transferred — **`97.31%` saved, `13.70x` faster**, `4,146,041 B` verify read, `75` verify GETs, `3` src GETs, `73/75` shards pruned).
  * `packed`: `full` `5.48 s` (`327.27 ms` CPU, `12,634,378 B` transferred, `12,634,378 B` verify read, `7` verify GETs, `25` chunks verified, `6` packs, `8` src GETs) vs `delta` `1.12 s` (`195.09 ms` CPU, `2,109,646 B` transferred — **`83.30%` saved, `4.91x` faster**, `12,634,378 B` verify read, `7` verify GETs, `25` chunks verified, `1/6` packs copied, `5` pruned, `3` src GETs).

## 5. Negative Results & Granularity Limits

1. **Pre-Commit Chunk Verification Traffic (`VerifyBytesRead` vs `BytesTransferred`):**
   Because `packID` is a random 128-bit identifier rather than a content digest,
   stat-only verification (`checkTree(ctx, true)`) cannot detect a same-length
   corrupted or substituted destination pack. By default (`TrustSealedReceipt =
   false`), `verifyRestoredTree` reads all referenced packs from `dst` and
   verifies every chunk's AEAD tag (`chunkAAD(volumeID, chunkID)`) and BLAKE3
   plaintext hash (`sumChunk(plain) == chunkID`) before advancing `head`. While
   `BytesTransferred` from `src` to `dst` drops by `83.30%` on `packed_append`,
   `VerifyBytesRead` on `dst` still equals the full referenced snapshot size
   (`12,632,523 B`, `24` chunks) unless `SaveVerifyReceipt` /
   `TrustSealedReceipt` is explicitly opted into.
2. **64 KiB Prolly-Tree Shard Granularity (`inline_scattered_neg`):**
   VFS stores inline files (`<= 16 KiB`) directly inside `~64 KiB` prolly-tree
   shards (`nodeTarget = 64 << 10`). When mutations scatter across the key
   space (even ~`6.6%` of files), almost every leaf shard changes its BLAKE3
   ref along with ancestor nodes. Because ParaSync still reads and verifies
   `dst`'s existing shards during `PlanDelta` and `verifyRestoredTree`,
   `delta_par` uses **more** CPU (`180.72 ms` vs `147.23 ms`) and more total
   ops (`136` vs `118`) than matched `full_par` on `mem`.
3. **16 MiB Ciphertext Pack Granularity (`pack_granularity_neg`):**
   Large-file chunks are packed into immutable ciphertext `pack/<2hex>/<id>`
   objects up to `16 MiB` (`packTarget = 16 << 20`). To preserve the
   ciphertext-only storage invariant without decrypting or re-encrypting chunks,
   ParaSync transfers whole packs. If `dst` skips an intermediate checkpoint
   that co-located short-lived deleted chunks with a small surviving file in
   one pack, `RestoreDelta` must transfer the full pack (`15,039,907 B` /
   `14.34 MiB` for `300 KiB` of live new file data).
4. **`file://` Single-Lock `fsync` Contention:**
   `pkg/backend/file` serializes mutations under a directory `flock` and calls
   `fsync` per object write. On `file://`, `delta_par` (`Streams=8`) is faster
   than `full_par` (`Streams=8`) when objects are pruned, yet can trail
   `delta_s1` (`Streams=1`) on `packed_append` (`1,670.63 ms` vs `1,364.29 ms`)
   due to lock + disk `fsync` contention across concurrent workers.

## 6. Reproducing

```sh
./tools/ci/s3server.sh /home/max/tmp/s3bin
gofmt -w . && test -z "$(gofmt -l .)"
pkgs=$(go list ./... | grep -v -e /replica -e /internal/sqlitecheck)
go vet $pkgs
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build . ./cmd/... ./pkg/... ./tools/...
TMPDIR=/home/max/tmp PATH="/home/max/tmp/s3bin:$PATH" go test -v -count=1 -run '^TestParaSync' .
TMPDIR=/home/max/tmp go run ./tools/parasyncbench -csv docs/parasync.csv
```
