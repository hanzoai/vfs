# Lifetime-Aware Object Packing (DOGI)

Refs #63. Tracked by HIP-1344. The clean-room lifetime classifier and predictor
library is `pkg/dogi`, canonical format-v2 multi-stream pack construction and
fragmented-pack relocation live directly in root `package vfs` (`pack.go`,
`generation.go`, `volume.go`, `dogi_volume.go`), the CLI benchmark runner is
`tools/dogibench`, unit tests are in `pkg/dogi/dogi_pkg_test.go`, live S3 /
`vfs.Hybrid` compatibility, snapshot-liveness, and fault-injection suites are in
`dogi_test.go`, and the measured dataset is in `docs/dogi.csv`.

## 1. Primary reference, summary, and clean-room license audit

* **Primary paper:** Jeeyun Kim, Seonggyun Oh, Jungwoo Kim, Jisung Park,
  Jaeho Kim, Sungjin Lee, and Sam H. Noh, *"DOGI: Data Placement with
  Oracle-Guided Insights for Log-Structured Systems"*, 24th USENIX Conference on
  File and Storage Technologies (FAST '26).
* **Summary:** In log-structured and append-only pack storage systems, naive
  single-stream FIFO packing co-locates short-lived build temporaries (`.o`,
  `.tmp`, `/target/`, `/node_modules/.cache/`) with long-lived source files,
  checkpoints, and model weights inside the same immutable pack objects. When
  temporaries expire, pack reclamation must read, decrypt, re-pack, and upload
  surviving live chunks (`LiveBytesCopied`), driving up Write Amplification
  Factor (`WAF`) and object-store request load. Guided by an explicit oracle
  upper bound (*NoDaP* in the paper), DOGI uses a lightweight classifier trained
  on write-time observable features to segregate chunks into a small number of
  lifetime streams (`NumStreams = 3`) during initial pack construction while
  weighing misprediction and tail-pack fragmentation costs.
* **Clean-room license and dependency audit:** Everything in `dogi_volume.go`,
  `pkg/dogi`, `tools/dogibench`, `dogi_test.go`, `docs/dogi.md`, and
  `docs/dogi.csv` is an independent, clean-room Apache-2.0 Go implementation
  written from first principles using only the Go standard library and existing
  `hanzoai/vfs` packages (`pkg/backend`, `pkg/backend/s3`, `pkg/backend/file`,
  `pkg/fence`, `internal/blake3`, `internal/storetest`, and `vfs.Hybrid`).
  `go.mod` and `go.sum` are untouched, zero external ML libraries are used, and
  all packages compile cleanly with `CGO_ENABLED=0` on `linux/amd64` and
  `darwin/arm64`.

## 2. Canonical Format-v2 Integration (Zero Custom Wire Formats)

DOGI stream separation operates directly inside the canonical `vfs.Volume`
production packer without introducing any new wire format, private magic bytes,
or separate GC authority:

1. **One canonical layout (`docs/SPECIFICATION.md` §3–§7):**
   * Every checkpoint commits a standard fenced `Head` at `"head"` (`fence\x00`
     header + sealed `Head` payload naming the prolly-tree `Root`).
   * Every metadata record (`[tInode]`, `[tEntry]`, `[tData]`, `[tChunk]`) lives
     in standard content-addressed prolly-tree shards at
     `meta/<2hex>/<64hex-blake3>`.
   * Every pack is named by a canonical **128-bit random `packID`** at
     `pack/<2hex>/<32hex-random-id>` and referenced by `[tChunk][chunkID] ->
     encodeWhere(where{pack, off, n})`.
   * Zero private `DGHD`/`DGMN` headers, zero secondary manifests, and zero
     BLAKE3 pack IDs exist anywhere in the codebase.
2. **Backward & forward reader compatibility (`PackClassifier == nil`):**
   * When `Config.PackClassifier == nil`, `vfs.Volume` uses its standard
     single-stream FIFO `packer.cur` path.
   * When `Config.PackClassifier != nil` (e.g., `*dogi.Classifier`),
     `generation.settleFile` classifies each file into lifetime stream `0..2`
     from write-time observable features (`pathHint`, `ino`, `size`,
     `overwrites`) held in memory and appends sealed frames to `packer.streams[class]`.
   * Because both modes emit identical format-v2 `pack/`, `meta/`, and `head`
     objects, **an unmodified standard `vfs.Open(ctx, Config{Backend: be, Cipher: c})`
     (with `PackClassifier: nil`) opens and reads Volumes written in either mode
     byte-for-byte identically**, and passes `Volume.Check(ctx)` and
     `vfs.Reseal(ctx, Config{Verify: true})`.
3. **Rule 1 (Ciphertext-Only Storage Invariant):** Plaintext logical paths,
   file names, and object bytes are never written to backend keys or unsealed
   objects. Path hints and overwrite counts are tracked strictly in memory on
   `Volume` for write-time stream routing.

## 3. Architecture & Contracts

### A. Clean-Room Held-Out Lifetime Classifier (`pkg/dogi/classifier.go`)
* **Classes:** `LifetimeEphemeral` (`0`), `LifetimeMedium` (`1`),
  `LifetimeLong` (`2`).
* **Zero oracle leakage:** `ObjectMeta` contains strictly write-time observable
  fields (`Path`, `Size`, `OverwriteRate`, `ProcessTag`) and intentionally omits
  any ground-truth TTL or true lifetime label. `Classifier.Classify(ObjectMeta)`
  is a regularized multinomial logistic regression (softmax SGD with L2 decay)
  trained strictly on `Train([]TrainingRecord)` or incrementally via
  `UpdateOnline(TrainingRecord)` over extracted sparse features (`ext:`,
  `topdir:`, `parent:`, `dir:`, `dir2:`, `ow:`, `size:`, `ext+ow:`, `proc:`).
* **`vfs.PackLifetimeClassifier` implementation:** `*dogi.Classifier`,
  `*dogi.OraclePredictor` (explicit *NoDaP* upper-bound reference), and
  `*dogi.AdversarialPredictor` implement
  `ClassifyWrite(pathHint string, ino vfs.InodeID, size uint64, overwrites uint32) int`
  so they plug directly into `vfs.Config.PackClassifier`.

### B. Canonical Pack Utilization & Relocation (`dogi_volume.go`)
* `MeasurePackUtilization(ctx, v *Volume, pinnedRoots ...RootRef)` scans the
  committed prolly tree (`[tData]` live extents vs `[tChunk]` `where` records)
  and `Backend.Stat` to compute exact live vs dead bytes per canonical 128-bit
  `packID`.
* `CompactFragmentedPacks(ctx, v *Volume, maxLiveRatio float64, pinnedRoots ...RootRef)`
  relocates surviving chunks from fragmented packs whose live byte ratio is at
  most `maxLiveRatio` (e.g. `0.60` = at least `40%` dead bytes):
  1. **Snapshot & concurrent reader liveness:** Any pack referenced by any
     `pinnedRoots` (such as an open read-only snapshot `Volume.CommittedRoot()`)
     is strictly pinned and skipped (`PacksPinnedSkipped`). In-flight
     `File.ReadAt` calls on `v` pin the root they planned against via
     `acquireRoot`/`releaseRoot`, and `CompactFragmentedPacks` waits for
     in-flight reads on the superseded root to drain before deleting retired
     packs.
  2. **Relocate-before-delete ordering:** Surviving chunks in eligible
     fragmented packs are read via `GetRange`, unsealed and verified against
     their `chunkID` BLAKE3 hash, packed into new canonical 128-bit random
     `pack/<2hex>/<id>` packs, and uploaded **before** updating the prolly tree.
  3. **Canonical prolly-tree + fenced Head commit:** Updated `[tChunk]` `where`
     records (and deletions of dead `[tChunk]` records from reclaimed packs) are
     applied via `v.t.using(shards).apply(...)` and committed via
     `v.writeHead(ctx, h)` with `v.landed(ctx, h)` lost-ACK recovery.
  4. **Safe reclamation:** Only after the new `Head` is durably committed does
     `CompactFragmentedPacks` delete superseded packs that are unreferenced by
     both the new `Head.Root` and all `pinnedRoots`.

## 4. Held-Out Evaluation Methodology & Matched Baseline

To prevent data leakage or synthetic self-fitting:
1. `GenerateAgentTrace` produces a chronological coding-agent build + workspace
   workload split **40% train / 60% held-out** (`80` train / `120` held-out
   files in `dogibench`; `54` train / `108` held-out in `dogi_test.go`).
2. The held-out 60% uses **disjoint workspace prefixes** (`ws-gamma`,
   `ws-delta`, `ws-epsilon` vs `ws-alpha`, `ws-beta` in training), unseen file
   names, overlapping extensions (`.bin` for both `artifacts/` medium and
   `models/` long; `.log` for both `/tmp/` ephemeral and `/build/reports/`
   medium), and organic ~3–4% TTL noise.
3. All modes execute the exact same held-out operation trace (`Create` +
   `WriteAt`, `Flush`, `Remove`, `CompactFragmentedPacks`) on the **actual
   canonical `vfs.Volume` production packer**, with the same `vfs.Hybrid`
   post-quantum `Cipher`, and verify every resulting Volume with a standard
   unmodified `vfs.Open` (`PackClassifier: nil`) + `Volume.Check(ctx)`.

## 5. Empirical Measurements (`docs/dogi.csv` & `storetest.NewS3`)

### A. Primary Held-Out Evaluation (`tools/dogibench`, Canonical `vfs.Volume`, `PackTarget = 64 KiB`, `file://` + `vfs.Hybrid`, unthrottled)

Held-out classifier accuracy: **92.50%** (`111/120` correct, `0` `Ephemeral <-> Long` severe swaps), `3,231,035 B` logical plaintext (`3,439,835 B` initial sealed pack bytes across `120` chunked files).

| Configuration | Streams | Held-Out Acc | Copied (B) | Physical Pack Written (B) | WAF | InitPacks | GCPacks | TotalPacks | Packs Deleted | Pack OH (Net) | Backend Reqs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `baseline-fifo` | 1 | n/a | 943,481 | 4,383,316 | 1.2743 | 45 | 14 | 59 | 42 | +0.00% | 336 |
| **`dogi-learned`** | **3** | **92.50%** | **226,099 (-76.04%)** | **3,665,934** | **1.0657 (-16.37%)** | **49** | **5** | **54** | **39** | **-8.47%** | **288 (-14.3%)** |
| `dogi-oracle` (NoDaP ref) | 3 | 100.00% | 0 (-100.00%) | 3,439,835 | 1.0000 (-21.52%) | 49 | 0 | 49 | 34 | -16.95% | 300 |

*Initial vs net pack trade-off on the production packer:* before compaction runs, 3-stream packing creates `49` initial packs vs `45` for 1-stream FIFO (`+8.89%` initial partial tail packs across 3 open streams). Once ephemeral build temporaries are unlinked and `CompactFragmentedPacks` runs (`maxLiveRatio = 0.60`), 1-stream FIFO must create `14` relocation packs (`943,481 B` copied) whereas DOGI creates only `5` (`226,099 B` copied), finishing with `-8.47%` fewer total packs (`54` vs `59`) and `-14.3%` fewer total backend requests (`288` vs `336`).

### B. Live HTTP S3 Protocol Server (`TestDOGIHeldOutPredictorOnCanonicalProductionVolume` over `storetest.NewS3`)

Measured with real `s3.New` client over `storetest.NewS3` + `storetest.Strict` + `vfs.Hybrid` on canonical `vfs.Volume` (`54` train / `108` held-out, `95.37%` held-out accuracy, `0` severe swaps, `64 KiB` `PackTarget`):

| Configuration | Copied (B) | WAF | Packs Created | Packs Deleted | S3 HTTP Requests (`srv.Requests()`) |
|---|---:|---:|---:|---:|---:|
| `baseline-fifo-1stream` | 680,925 | 1.2189 | 50 | 33 | 283 |
| `dogi-learned-3stream` | 150,811 (-77.85%) | 1.0485 | 51 | 37 | 268 |
| `dogi-oracle-3stream` | 0 (-100.00%) | 1.0000 | 46 | 31 | 251 |

### C. `PackTarget` Sensitivity Sweep (`docs/dogi.csv`)

| `PackTarget` | Baseline `Copied (B)` | Baseline `WAF` | DOGI `Copied (B)` | DOGI `WAF` | `LiveBytes` Reduction | `WAF` Reduction |
|---|---:|---:|---:|---:|---:|---:|
| `32 KiB` | 1,025,916 | 1.2982 | 155,386 | 1.0452 | -84.85% | -19.49% |
| `64 KiB` | 1,072,533 | 1.3118 | 226,099 | 1.0657 | -78.92% | -18.76% |
| `256 KiB` | 1,586,484 | 1.4612 | 91,422 | 1.0266 | -94.24% | -29.74% |

As `PackTarget` grows (`32 KiB -> 256 KiB`), 1-stream FIFO co-packs more long-lived chunks with expiring temporaries (`WAF` climbs from `1.2982` to `1.4612`), whereas 3-stream DOGI keeps `WAF` near `1.0266–1.0657` (while paying `+4.17%` net pack count at `256 KiB`, `25` vs `24` packs, due to 3 open streams).

### D. Adversarial Misprediction & Negative / Boundary Controls (Retained Honestly)

1. **Adversarial Label-Noise Sweep (`docs/dogi.csv`, `PackTarget = 64 KiB`):**
   * `10%` flip (`80.00%` acc, `7` severe swaps): `Copied = 433,606 B`, `WAF = 1.1261`, `Packs = 56` (`-5.08%`)
   * `25%` flip (`71.67%` acc, `8` severe swaps): `Copied = 497,815 B`, `WAF = 1.1447`, `Packs = 59` (`+0.00%`)
   * `45%` flip (`52.50%` acc, `24` severe swaps — pack overhead crossover): `Copied = 852,984 B`, `WAF = 1.2480`, **`Packs = 61` (`+3.39%` net pack overhead)**
   * **`75%` flip (`25.00%` acc, `43` severe swaps — negative crossover):** `Copied = 857,754 B`, `WAF = 1.2494`, **`Packs = 62` (`+5.08%` net pack overhead)**. Under severe misprediction, multi-stream packing fragments packs across 3 open buffers without clean lifetime isolation.
2. **Mixed-Class Retained Archive (`boundary-no-expiry`, 0 expirations):**
   When files span all 3 lifetime classes (`Ephemeral`/`Medium`/`Long`) but zero deletions occur, neither mode triggers compaction (`LiveBytesCopied = 0`, `WAF = 1.0000` for both), while 3-stream DOGI pays **`+11.36%` pack-count overhead** (`49` vs `44` packs) from flushing up to 3 open tail streams per generation instead of 1.
3. **Labeled Simulated RTT (`simulated-rtt`, `1.000 ms` injected per backend request):**
   `Copied` drops by `-72.19%` (`226,099 B` vs `813,091 B`), `GetRange` requests drop from `26` to `8`, total backend requests drop from `307` to `291`, and wall time drops from `53,258.3 ms` to `43,668.9 ms`.

## 6. Reproducing

```sh
gofmt -l .
go vet . ./cmd/... ./pkg/... ./tools/...
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build . ./cmd/... ./pkg/... ./tools/...
TMPDIR=/home/max/tmp go test -v -count=1 ./pkg/dogi
TMPDIR=/home/max/tmp go test -v -count=1 -run '^TestDOGI' .
TMPDIR=/home/max/tmp go run ./tools/dogibench -rtt 1ms -csv-out docs/dogi.csv
```
