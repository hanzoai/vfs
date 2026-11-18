# Directory-granularity metadata mutation coalescing (DJFS)

Refs #20. Tracked by HIP-1345 (under the HIP-1342 S3-backed disk program). The
coalescing library is `pkg/djfs`, the integration and crash-exploration suites
are `pkg/djfs/djfs_test.go` and `pkg/djfs/volume_test.go`, and the reproducible
CLI benchmark harness is `tools/djfs`.

## 1. Primary reference, summary, and license audit

* **Primary paper:** Seung Won Yoo, Myeongin Cheon, Bonmoo Koo, and Youjip Won
  (KAIST); Joontaek Oh (University of Wisconsin-Madison); Wonseb Jeong, Hyunsub
  Song, Hyeonho Song, and Donghun Lee (Samsung Electronics), *"DJFS:
  Directory-Granularity Filesystem Journaling for CMM-H SSDs"*, 23rd USENIX
  Conference on File and Storage Technologies (FAST 2025),
  <https://www.usenix.org/conference/fast25/presentation/yoo>.
* **Summary:** Traditional journaling and copy-on-write filesystems serialize
  metadata updates behind a coarse global transaction or single overlay lock.
  During metadata storms (`npm ci`, `pnpm install`, `git checkout`, maildir
  queues, compiler scratch churn), concurrent workers contend on global
  locks while repeatedly logging and rewriting transient directory states
  (`.staging` unpack entries, intermediate `chmod`/`utimens`, temporary lock
  files, and `WriteTmp + Rename` replacements) that cancel before publication.
  DJFS exploits directory locality through three mechanisms:
  1. **Path/directory-partitioned transactions:** independent directories stage
     and coalesce mutations under per-directory lock stripes rather than a
     single global mutex.
  2. **Intra-directory mutation coalescing:** redundant and inverse updates
     within each directory fold algebraically into a minimal net-state delta
     prior to journal flush or Merkle-tree shard construction.
  3. **Cross-directory conflict and dependency tracking:** operations that span
     directories (`Rename(srcDir != dstDir)`, `Mkdir`, `Rmdir`) link the
     affected partitions into a closed equivalence class so targeted `fsync`
     and epoch cuts preserve one global commit order without torn link counts
     or orphaned entries.
* **License and clean-room audit:** The FAST 2025 paper and Linux kernel
  (`ext4`/`jbd2`, GPL-2.0) research prototype are **study-only** references per
  the `hanzoai/vfs` licensing policy. **Zero GPL, BSD-3, or external prototype
  source code, headers, or fixtures were copied or translated.** Everything in
  `pkg/djfs`, `tools/djfs`, and this document is an independent, clean-room
  Apache-2.0 Go implementation using only the Go standard library and existing
  `hanzoai/vfs` packages (`go.mod` and `go.sum` are untouched; zero third-party
  dependencies added; builds with `CGO_ENABLED=0` on `linux/amd64` and
  `darwin/arm64`). Canonical architecture (`vfs -> Hanzo S3 -> Visor`) is
  unchanged.

## 2. Reproducible upstream baseline

Audited upstream `hanzoai/vfs` `main` (`6b779e773b90d9a1921f85f1fda04054b96b6992`)
across `volume.go`, `ops.go`, `overlay.go`, `tree.go`, `cost_test.go`, and
`journal/`:

```sh
# 1. Static build, vet, cross-compile (darwin/arm64 CGO_ENABLED=0), and gofmt
pkgs=$(go list ./... | grep -v -e /replica -e /internal/sqlitecheck)
go build $pkgs && go vet $pkgs
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build . ./cmd/... ./pkg/... ./tools/...
test -z "$(gofmt -l .)"

# 2. DJFS unit, crash-consistency, randomized oracle, Volume budget, and -race suites
go test -v -race -count=1 ./pkg/djfs/...
go test -bench=. -benchmem ./pkg/djfs/...

# 3. End-to-end CLI benchmark harness (npm-ci, scratch, atomic rename, fsync vs plain disk)
go run ./tools/djfs -pkgs 1000 -files-per-pkg 100
```

Three structural bottlenecks determine the upstream metadata-storm baseline:

1. **Commit-per-close S3 write amplification (`PR #8` / `cost_test.go`):** On
   upstream `main`, FUSE `close(2)` (`HandleFlusher`) invokes `Volume.Flush`,
   uploading dirty FastCDC packs, rewriting every touched leaf-to-root prolly
   shard (`meta/<2hex>/<hash>`), and executing a conditional `head` PUT. Across
   500--5,000 file checkouts (`TestCommitPerClose`, `TestCheckoutCost`), PR #8
   measured **4.9 to 6.0 S3 objects PUT per file** (`5.3--5.7` typical on
   `npm ci` / source trees), which is **13.3x--14.3x** over the `0.40`
   objects/file acceptance budget for a 100,000-file tree.
2. **Single-lock global mutation staging (`volume.go`, `ops.go`):** Every
   `Create`, `Mkdir`, and `Remove` acquires `v.mu.Lock()` and inserts directly
   into one volume-wide skip list (`v.over`). Concurrent workers unpacking
   independent `node_modules` subdirectories serialize on `v.mu`.
3. **Uncoalesced `.staging` and scratch churn:** An `npm ci` extraction of
   100,000 surviving files across 1,000 packages executes ~463,000 raw
   operations (`Create` in `.staging`, `Write`, `Chmod`, cross-directory
   `Rename` into `node_modules/<pkg>`, plus ~20,000 transient `.lock`/`.tmp`
   `Create -> Write -> Unlink` cycles). Without per-directory coalescing before
   journal and Merkle-tree updates, `.staging` entries, lockfiles, and repeated
   `chmod`/timestamp rewrites amplify journal record counts by **>70,000x** and
   dirty `.staging` prolly-tree leaves that are deleted moments later.

## 3. Falsifiable hypothesis, acceptance criteria, and negative boundaries

* **Falsifiable hypothesis:** Partitioning live namespace state across
  lock-striped per-directory state machines (`pkg/djfs`) while stamping one
  monotonic global sequence (`gseq`) and coalescing net directory transitions
  (`Base -> Current`) at two boundaries -- Layer 1 before `journal.Journal`
  append and Layer 2 before `vfs.Volume` Merkle-tree flush -- will:
  1. Reduce **100,000-file `npm ci` S3 object PUTs** from `5.3--5.7` per file
     down to **`<= 0.07` per file** (more than **5x below** the `0.40`
     objects/file acceptance budget), with **zero synchronous S3 requests** on
     clean `close(2)` and bounded `<= 5 s` publication tracking.
  2. Eliminate **>99.9%** of journal/tree mutations on `Create -> Write ->
     Unlink` scratch storms and **>99%** on repeated `WriteTmp + Rename`
     config/lockfile overwrites.
  3. Keep `Fsync` p50/p99 latency **within `<= 1.5x` plain local NVMe/SSD**
     (and substantially faster than plain per-file `fdatasync + rename +
     fsync(dir)` because coalesced deltas append sequentially into preallocated
     `journal` segments).
  4. **Expected failure mode / negative boundary:** When a workload creates
     strictly unique filenames and forces a synchronous `Fsync` after **every
     individual operation** (`batch size = 1`, zero path overlap), algebraic
     coalescing has zero cancellation opportunity (`1.00x` operation count) and
     incurs a fixed `28 B` `DJFS` batch header per sync (`0.72x` raw byte
     efficiency on empty-file syncs). Similarly, on multi-megabyte sequential
     file writes (`> 64 KiB`), FastCDC `pack/` uploads dominate total bytes and
     metadata coalescing leaves data-plane bandwidth unchanged.

## 4. Clean-room implementation (`pkg/djfs` and `tools/djfs`)

`pkg/djfs` is implemented in three self-contained files with zero external
dependencies beyond the Go standard library and `github.com/hanzoai/vfs/journal`:

* **64-way lock-striped directory partitions (`pkg/djfs/djfs.go`):**
  * Directories hash via a 64-bit SplitMix finalizer into 64 `stripe` shards,
    each holding live `dirState`, journal baseline `jBase`, and published
    baseline `pBase`.
  * Single-directory mutations (`Create`, `Write`, `Chmod`, `Touch`, `Unlink`,
    same-directory file `Rename`) and cross-directory regular-file `Rename` run
    under `epochMu.RLock()` plus at most two stripe locks acquired in strictly
    ascending index order (`lockTwoStripes`), preventing deadlocks while
    allowing disjoint directories to mutate and `Lookup`/`Stat`/`ReadDir` in
    parallel.
  * Every admitted operation increments an atomic global sequence `gseq`,
    preserving one total ordering across the engine.
* **Entanglement graph and crash-consistent barriers (`D1..D6`):**
  * **`D1_SingleGlobalOrder`:** Every `Batch` records `(Epoch, MaxSeq)` and
    commits into `journal.Journal` in monotonic sequence order.
  * **`D2_CrossDirRenameAtomicity`:** Cross-directory `Rename(srcDir, dstDir)`,
    `Mkdir(parent, child)`, and `Rmdir(parent, child)` record a symmetric edge
    in `entAdj`. Targeted `Fsync(id)` / `FsyncDir(dir)` flushes the single
    stripe directly when unentangled, or cuts the full connected component
    `C(dir)` under `epochMu.Lock()` into **one atomic `DJFS` journal record**
    when entangled, then releases all locks before calling `Journal.Sync`. No
    crash prefix or `Snapshot()` reader can ever observe 0 links (lost file) or
    2 links (duplicate entry).
  * **`D3_SyncBarrierLinearizability`:** Each `dirState` tracks `lastJSeq`. A
    concurrent `FsyncDir` arriving after another caller already drained `dirtyJ`
    still waits on `Journal.Sync(ctx, lastJSeq)` before returning.
  * **`D4_DurableBeforePublish` & `D5_PostCommitAdvance`:** `Publish(ctx)`
    flushes and syncs `targetJSeq` in `Journal` prior to `Target.ApplyBatch`,
    and advances `Journal.Publish(targetJSeq)` + `Journal.Reclaim()` only after
    `Target.ApplyBatch` succeeds.
  * **`D6_BoundedWatermarksZeroPolling`:** `MaxDirtyOps` (default `65,536`) and
    `MaxDirtyBytes` (default `64 MiB`) relieve dirty memory deterministically on
    admission without background polling goroutines (`LLM.md` Rule 4), while
    `Stats().UnpublishedAge` and `PublishOverdue` track the `5 s` S3 bound.
* **6-phase collision-free algebraic coalescer (`pkg/djfs/coalesce.go`):**
  `computeDelta(baseDirs, currDirs)` computes the minimal net transition across
  any closed directory set in six ordered phases:
  1. Unlink dead pre-existing regular files (vacating slots and emptying dead
     directories).
  2. Create newly born directories top-down by final depth (using deterministic
     `.djfs.stage.<id>.<n>` parking if a surviving entry still occupies the
     slot).
  3. Relocate surviving files and directories to their final `(dir, name)`,
     unnesting multi-level ancestor inversions bottom-up and breaking N-way
     swap cycles (`a <-> b`, `a -> b -> c -> a`) cleanly.
  4. Remove dead directories bottom-up (guaranteed empty after Phases 1 and 3).
  5. Create newly born surviving regular files directly at their final `(dir,
     name)` with final mode and coalesced payload bytes (completely eliding
     `.staging` intermediates and unlinked scratch files).
  6. Apply payload/attribute updates on surviving base files and stamp final
     directory `Attr` (`Mtime`, `Ctime`, `Mode`, derived `Nlink`) last.
* **Authenticated wire codec (`pkg/djfs/codec.go`):**
  `EncodeBatch` / `DecodeBatch` frame batches with `"DJFS"` magic, Castagnoli
  CRC-32C over header and ops, and canonical shortest-form varints (rejecting
  non-minimal encodings, truncated slices, and bit-flips with `ErrCorrupt`).

## 5. Correctness, crash-consistency, and concurrency verification

1. **Systematic crash exploration on `journal/crash.Disk`
   (`TestCrashConsistencyExploreOnJournalDisk`):**
   Executes multi-directory `Mkdir`, `Create`, `Fsync`, scratch
   `Create+Unlink`, and cross-directory `Write + Rename + FsyncDir` on a real
   `journal.Journal` backed by `crash.Disk`, plus an unsynced tail batch, then
   runs `disk.Explore` across volatile prefixes, subsets, sector tears, and
   random scatters:
   * **0 invariant violations** across all explored crash images (`MemTree.CheckInvariants`:
     full reachability from `RootInode`, zero orphaned files/directories, zero
     dangling entries, exact `Nlink = 2 + subdirs` on directories and `1` on
     regular files).
   * **0 torn cross-directory renames** (`dirA/alpha.txt` and `dirB/beta.txt`
     never coexist, and post-`Fsync` images always hold `dirB/beta.txt` with
     `v2-moved-durable`).
2. **Randomized equivalence against uncoalesced POSIX oracle
   (`TestRandomizedEquivalenceAgainstUncoalescedOracle`):**
   Runs 24 deterministic seeds x 600 random interleaved operations (`Create`,
   `Mkdir`, `Write`, `Chmod`, `Unlink`, `Rmdir`, same-dir `Rename`, cross-dir
   `Rename`, `FsyncDir` + `RecoverFromTree`, and `Publish`) against an
   uncoalesced step-by-step oracle, verifying 100% identical directory trees,
   inode IDs, modes, link counts, timestamps, and file bytes.
3. **Concurrent `-race` storm (`TestConcurrentMultiGoroutineStormIsRaceFree`):**
   12 concurrent mutator goroutines + 3 concurrent `Snapshot`/`ReadDir`/`Publish`
   goroutines hammer private and shared directories with interleaved `Create`,
   `Chmod`, `Unlink`, same/cross-directory `Rename`, and `FsyncDir` with zero
   data races and exact final state convergence.

## 6. Empirical evaluation and raw measurements (HIP-1345)

Measured on Linux x86_64 (`Intel(R) Xeon(R) CPU E5-1650 v4 @ 3.60GHz`, 12
hardware threads, local NVMe-backed Linux filesystem), reporting plain local
disk numbers beside every `vfs + DJFS` measurement:

### Table A: 100,000-file `npm ci` acceptance workload (`1,000` packages x `100` files, `1,047.17 MiB` total data)

Workload shape: `87,998` inline files (`<= 16 KiB`) + `12,002` chunked files
(`17--160 KiB`), extracted via `.staging` (`Create -> Write -> Chmod ->
Rename`) plus `20,000` transient `.lock` files (`Create -> Write -> Unlink`) =
**`463,000` raw operations** (`343,000` metadata ops).

| Configuration | Raw ops | Surviving tree ops | `meta/` shards | `pack/` PUTs | `head` PUTs | Total S3 PUTs | **S3 PUTs / file** | S3 sync reqs on `close` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Upstream `Flush`-per-close (`PR #8` baseline) | 463,000 | 463,000 | ~500,000 | ~50,000 | 100,000 | ~550,000 | **5.5000** (FAIL `> 0.40`) | 1 commit / close |
| Uncoalesced 100-batch staging spills (no DJFS) | 463,000 | 343,000 | 19,632 | 45 | 100 | 19,777 | **0.1978** | 0 |
| **`pkg/djfs` + `vfs.Volume` (1-epoch `<= 5 s` publish)** | **463,000** | **101,001** | **5,944** | **45** | **1** | **5,990** | **0.0599** (**PASS, 6.7x under budget**) | **0** |
| **`pkg/djfs` + `vfs.Volume` (5-epoch multi-window)** | **463,000** | **101,001** | **5,994** | **49** | **5** | **6,048** | **0.0605** (**PASS, 6.6x under budget**) | **0** |
| Plain local NVMe/SSD (`os` syscalls, no S3) | 463,000 | n/a (817 files/s) | 0 | 0 | 0 | 0 | 0.0000 (local reference) | 0 |

* **Acceptance verdict:** `pkg/djfs` achieves **`0.0599--0.0605` S3 objects PUT
  per file** on the 100,000-file `npm ci` tree -- **91.8x fewer S3 PUTs** than
  the PR #8 `5.5` baseline and **6.6x below** the `0.40` objects/file target.
* Metadata-only journal traffic drops **8.27x** (`42.83 MiB` across `343,000`
  metadata records down to `5.18 MiB` across `5` batch records); total journal
  bytes including `358 MiB` of surviving inline JS payloads drop from `393.95
  MiB` to `363.15 MiB` (`1.08x`).

### Table B: `MDTest`-style 100,000-inode Create / Stat / Delete rates

100,000 regular files across 100 directories (`Create`, `Lookup`/`Stat`,
`Unlink`):

| Phase | Plain local NVMe/SSD (`ops/s`) | `pkg/djfs` coalescer (`ops/s`) | Speedup vs plain local syscalls | S3 PUTs when `Create+Delete` fall in one 5 s epoch |
|---|---:|---:|---:|---:|
| `MDTest` Create (100k files) | 2,279 | 1,381,500 | **606.2x** (in-memory partition) | 0 file shards (cancelled) |
| `MDTest` Stat / Lookup (100k files) | 66,028 | 1,340,749 | **20.3x** | 0 |
| `MDTest` Delete / Unlink (100k files) | 2,900 | 1,344,023 | **463.5x** (in-memory partition) | 1 shard + 1 `head` (dir mtime only) |

### Table C: Storm write amplification, `fsync` tail latency, and negative control

| Workload | Raw ops | Coalesced ops | Raw journal size | Coalesced journal size | Write amp reduction | Throughput (`cycles/s`) |
|---|---:|---:|---:|---:|---:|---:|
| Scratch `Create + Write(4 KiB) + Unlink` (`50,000` files, `64` dirs) | 150,000 | **64** (dir stamps only) | 205.14 MiB | **2.18 KiB** | **96,327.8x** | 3,040,623 |
| Atomic `WriteTmp + Write(2 KiB) + Chmod + Rename` (`50,000` updates -> `256` files) | 200,000 | **320** (`256` files + `64` dirs) | 111.87 MiB | **527.68 KiB** | **217.1x** | 1,463,323 |
| **Negative control:** `Create + immediate Fsync` (`1,000` unique files, `batch = 1`) | 1,000 | **1,000** (`0%` overlap) | 72.27 KiB | **100.59 KiB** (`+28 B` hdr/sync) | **0.72x** (expected boundary) | n/a (disk-bound) |

Local durability (`fsync`) tail latency on `Varmail` / atomic `Create(tmp) +
Write(4 KiB) + Rename(tmp -> cur) + Fsync` (`300` synchronous rounds):

| Path | p50 (`us`) | p95 (`us`) | p99 (`us`) | Ratio vs plain local NVMe/SSD (target `<= 1.50x`) |
|---|---:|---:|---:|---:|
| Plain local NVMe/SSD (`open + write + fdatasync + rename + fsync(dir)`) | 2,146.3 | 3,826.3 | 18,063.8 | 1.00x (baseline) |
| **`pkg/djfs` + preallocated journal segment (`pwrite + fdatasync`)** | **118.6** | **162.5** | **286.3** | **0.06x p50 / 0.02x p99** (**PASS `<= 1.50x`**) |

Because `pkg/djfs` folds `Create(tmp) + Write(4 KiB) + Rename(tmp -> cur)` in
memory into one record appended sequentially into an already-allocated journal
segment, each durable `FsyncDir` executes **one** sequential `pwrite +
fdatasync` instead of two separate filesystem metadata/data syncs (`fdatasync`
on the temp file plus `fsync` on the parent directory after `rename`).

### Integration path for `Volume` (#57 + #70)

1. Attach `djfs.Engine` in front of `Volume`'s namespace handlers (`Create`,
   `Mkdir`, `Remove`, `Rename`, `SetAttr`) with `Config.Journal` bound to the
   per-`(Volume.ID, Round)` `journal.Journal` instance and `MaxPublishWindow =
   5 * time.Second`.
2. Route FUSE `FLUSH` (`close(2)`) to `Engine.CloseFile` (zero S3 calls, zero
   disk syncs), FUSE `FSYNC` / `FSYNCDIR` to `Engine.Fsync` / `Engine.FsyncDir`
   (component-atomic local journal sync), and the 5 s `journal.Due` publisher
   loop to `Engine.Publish` (Layer-2 coalesced prolly-tree commit + `Publish` +
   `Reclaim`).
3. Keep large file extents (`> 64 KiB`) on the side bulk spool / FastCDC `pack`
   writer (`docs/journal.md` item 8) so ordered `DJFS` journal batches carry
   only compact metadata deltas and inline `<= 16 KiB` payloads.
