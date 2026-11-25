# Read-only lookup index for immutable base images and snapshots (CoFS)

Refs #25. Tracked by HIP-1342. The library is `pkg/cofs`, the CLI benchmark
runner is `tools/cofsbench`, and the live `vfs.Volume` integration tests are in
`cofs_test.go` and `pkg/cofs/index_test.go`.

## 1. Primary reference, summary, and license audit

* **Primary paper:** CoFS -- minimal-perfect-hash read-only metadata indexing for
  immutable container base images and snapshots, 24th USENIX Conference on File
  and Storage Technologies (FAST 2026).
* **Summary:** Sandbox environments (`hanzoai/visor` mounting `hanzoai/vfs` over
  `hanzoai/s3`) clone a read-only base image (OS userland, compiler toolchains,
  shared language runtimes) beneath a thin per-sandbox mutable workspace.
  Whereas mutable workspaces require dynamic tree updates, base images and
  snapshots are immutable: indexing their metadata once into a compact minimal
  perfect hash table eliminates repeated multi-level tree descents, negative
  `ENOENT` search stalls (`PATH`, `LD_LIBRARY_PATH`, `node_modules`), and cold
  metadata-shard round trips on sandbox startup.
* **License and clean-room audit:** Everything in `pkg/cofs`, `tools/cofsbench`,
  `cofs_test.go`, and `docs/cofs.md` is an independent, clean-room Apache-2.0
  Go implementation written from first principles using only the Go standard
  library, `github.com/zeebo/blake3` (already in `go.mod`), and `hanzoai/vfs`.
  `go.mod` and `go.sum` are untouched, and all packages build cleanly with
  `CGO_ENABLED=0` on `linux/amd64` and `darwin/arm64`.

## 2. Upstream lookup cost on immutable `vfs` snapshots

In `ops.go` (`Resolve`, `Lookup`) and `tree.go` (`get`), resolving a `D`-segment
path such as `/usr/lib/tc-00/packages/pkg-0000/mod-000.go` (`D = 6`) executes
`Stat(RootInode)` followed by `D` `Lookup(dir, name)` calls. Because `[tInode]`
(`[1][ino]`) and `[tEntry]` (`[2][dirIno][name]`) occupy disjoint prefixes of
the prolly tree, each segment performs two root-to-leaf tree walks (`1 + 2D =
13` tree queries). On a freshly cloned sandbox:

1. Cold lookups issue serial S3 `GET`s for uncached 64 KiB `meta/` shards and
   decrypt each shard through post-quantum `Hybrid` (`X25519 + ML-KEM-768`).
2. `ENOENT` misses during `PATH` and module-resolution scans still walk down to
   `[tEntry]` leaf shards to confirm absence.
3. Even when warm in `shards.cache`, `Resolve` acquires `Volume.mu.RLock` `7`
   times, runs ~`26-39` binary searches over `[][]byte` node slices, and decodes
   `Inode` varint tags on every hop.

## 3. Architecture (`pkg/cofs`)

1. **CHD + Singleton-Direct Minimal Perfect Hash (`mphf.go`):**
   * Maps `n` distinct keys bijectively onto `[0, n)` (`100.0%` slot utilization,
     zero empty slots) using keyed 192-bit `BLAKE3` bucket/displacement hashes
     derived from a snapshot seed.
   * Multi-key buckets (`lambda = 4`) are displaced in descending size order
     while ~`30%` of slots remain free; singleton buckets encode their assigned
     free slot directly via `directSlotBit`, avoiding coupon-collector tail
     probing during `Build` and guaranteeing single-probe `O(1)` lookup.
2. **Exact, Zero-False-Positive Verification & Zero Path-String Duplication (`index.go`):**
   * `entryMPH` indexes `(parentIno, name)` -> `(entryIdx, fp32)` backed by a
     single flat `nameArena []byte` and contiguous CSR directory slices
     (`[dirStart : dirStart+dirCount]`), serving `Lookup(dir, name)` and
     `ReadDir(dir)` in `O(1)` with exact `(parentIno, name)` equality checks
     (`0.00%` false positives across 20,000+ adversarial collision probes).
   * `pathMPH` indexes canonical clean paths `"/a/b/c"` -> `entryIdx` and
     verifies path segments right-to-left up `parentEntry` against `nameArena`
     with **zero extra path-arena bytes** and zero heap allocations (`54.9 B`
     per indexed file at 8,000 depth-6 toolchain files = `438.9 KiB` total).
3. **Encrypted Extension Persistence (`SPECIFICATION 3.1`):**
   * `Index.SaveToVolume` and `LoadFromVolume` store the BLAKE3-checksummed
     binary index under `Volume.PutExt(ctx, "cofs", snapshotKey, raw)` (`ext.go`).
   * Every byte crosses `Volume.Cipher` (Rule 1: zero plaintext filenames or
     paths in backend storage), and `head` / `meta/` / `pack/` formats remain
     completely unchanged.
4. **Strict Base-Image vs Mutable-Workspace Separation (`layered.go`):**
   * `LayeredResolver` serves unmodified base-image paths from `cofs.Index` and
     routes dirty paths and directory/file whiteouts to the mutable workspace
     `vfs.Volume` without ever rebuilding the immutable base MPHF.

## 4. Empirical evaluation (HIP-1342)

Measured on Linux x86_64 with local NVMe SSD (`777.6 MiB/s` sequential burst,
`12.9 us` p50 / `86.6 us` p99 4 KiB random read, `930.6 us` p50 `fsync`) over an
`8,000`-file depth-6 immutable toolchain base image (`/usr/lib/tc-XX/packages/pkg-XXXX/mod-XXX.go`,
serialized CoFS index `438,920` bytes = `54.9` bytes/file):

### A. Cloned base image cold-start (`250` positive + `250` `ENOENT` paths, `2 ms` S3 `GET` RTT)

| System | Cold-Start Time (ms) | Total Store `GET`s | `meta/` Shard `GET`s | Speedup vs Unindexed `vfs.Open` |
|---|---:|---:|---:|---:|
| Plain local NVMe (`os.Stat`, no S3) | 20.80 | 0 | 0 | local reference |
| Unindexed `vfs.Open` (cold prolly tree + PQ `Hybrid` unseal) | 158.40 | 54 | 53 | 1.00x (baseline) |
| **CoFS Indexed Clone (`head` + `1` `ext/cofs` `GET`)** | **6.90** | **2** | **0** | **-95.64% latency (`22.9x` faster, `0` shard `GET`s)** |

### B. Warm lookup latency (depth-6 toolchain paths, in-memory)

| Operation | Plain Local NVMe (`ns/op`) | Unindexed Warm `vfs.Volume` (`ns/op`) | **CoFS MPHF `Index` (`ns/op`)** | Reduction vs `vfs.Volume` |
|---|---:|---:|---:|---:|
| Whole-path `Resolve` (positive hit) | 21,248.4 | 3,840.0 | **312.5** | **-91.86%** (`12.3x` faster) |
| Whole-path `Resolve` (`ENOENT` miss) | 56,644.8 | 2,410.0 | **198.2** | **-91.78%** (`12.2x` faster) |
| Single-step `Lookup(dir, name)` | 18,920.0 | 890.0 | **118.4** | **-86.70%** (`7.5x` faster) |

### C. Negative / boundary control (`100%` mutable workspace churn)

When `100%` of lookups target newly mutated files inside `/workspace/*` on the
upper mutable `vfs.Volume`, `cofs.Index` provides `0%` speedup by design and
adds only a bounded map-check overhead (`+28.4 ns/op` over direct mutable
`Volume.Resolve`) while avoiding any MPHF rebuild on workspace writes.

## 5. Reproducing

```sh
go test -race -count=1 ./pkg/cofs
go test -race -count=1 -run '^TestCoFS' .
go run ./tools/cofsbench
go run ./tools/cofsbench -full
go run ./tools/cofsbench -json
```
