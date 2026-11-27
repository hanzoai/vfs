# Similarity-Clustered Immutable Base Images for `hanzoai/vfs` (RubikFS, HIP-1344 / HIP-1345)

Refs `hanzoai/vfs#61` and `hanzoai/vfs#49`. Tracked by HIP-1344 / HIP-1345.
Library: `pkg/rubikfs`, benchmark harness: `cmd/rubikfs-bench`, machine-readable results: `results.json`.

## 1. Primary Reference, Summary, and Clean-Room License Audit

* **Primary paper:** RubikFS — Rearranging immutable base-image chunks by content similarity before sealing and packing, 24th USENIX Conference on File and Storage Technologies (FAST 2026), <https://www.usenix.org/conference/fast26/presentation/huang>.
* **Problem & stack boundary:** Sandbox runtimes (`hanzoai/visor` mounting `hanzoai/vfs` over `hanzoai/s3`) distribute immutable base images containing compiler toolchains, language standard libraries, shared ELF binaries, and JSON/YAML manifests across deep directory trees. Packing chunks in lexicographical directory-walk order scatters structurally similar chunks across separate packs and beyond LZ77 windows. `pkg/rubikfs` clusters immutable chunks by shift-invariant content similarity **strictly inside one `(tenant_id, volume_id)` scope** before compression, AEAD sealing, and packing.
* **Orthogonality & clean-room audit:** `pkg/rubikfs` is completely orthogonal to `pkg/cofs` (HIP-1342 metadata MPHF), operates cleanly above `pkg/backend.Backend` and `vfs.Cipher`, introduces **zero changes to `go.mod` / `go.sum`**, is written clean-room under Apache-2.0, and builds with `CGO_ENABLED=0` on `linux/amd64` and `darwin/arm64`.

## 2. Architecture (`pkg/rubikfs`)

1. **Shift-Invariant Similarity Sketching (`sketch.go`):** Content-sampled 8-byte shingles mapped into a 32-register one-permutation MinHash (with deterministic densification), paired with a 64-bit SimHash, 16-bin byte-class histogram, and exact Shannon entropy classification.
2. **Scoped LSH Similarity Clustering & Boot-Locality Pinning (`cluster.go`):** Hard-enforces a single `Scope{TenantID, VolumeID}` (refusing mixed scopes with `ErrTenantIsolation`). Supports 4 matched regimes: `path_order_baseline`, `random_order_control`, `rubikfs_pure_similarity`, and `rubikfs_locality_hybrid` (which pins boot-hot working-set chunks into dedicated similarity-clustered boot packs while running full similarity clustering over cold image chunks).
3. **Zero-Dependency Pack-Wide Long-Range LZ77 + DEFLATE (`codec.go`):** Captures cross-chunk matches across multi-MiB packs (far beyond DEFLATE's 32 KiB sliding window, enabling full similarity gains on both `16 KiB` and `64 KiB` chunks) with automatic 1-byte raw fallback so high-entropy payloads never expand.
4. **Per-Volume Independent AEAD Sealing & BLAKE3 Verification (`seal.go`, `image.go`):** Derives an independent AES-256-GCM key per `(tenant_id, volume_id)` (or accepts `vfs.Hybrid` post-quantum X25519+ML-KEM-768 directly), seals every pack with a fresh 96-bit CSPRNG nonce and `(tenant_id, volume_id, image_id, pack_id)` AAD binding, stores objects under opaque keyed BLAKE3 backend keys, and verifies pack + chunk + file BLAKE3 digests on every read.

## 3. Grounded Corpus & Experimental Environment

* **Environment:** `go1.27.1`, `linux/amd64`, `2` logical CPUs, `--seed 1344`, generated `2026-11-26T23:13:28Z`.
* **Grounded primary corpus (`visor-base-image-grounded`):** `536` files (`13.83 MiB` uncompressed), combining **`312` real versioned `hanzoai/vfs` repository files** (`2.71 MiB` of Go sources, docs, configs, and JSON test vectors) with **`224` deterministic seeded visor base-image files** (`11.12 MiB` of ELF64-structured binaries, multi-directory toolchain/stdlib modules, and service manifests; zero identical chunks across files). Cold-start boot working set: **`19` files (`801.85 KiB`, `5.7%` of image)** spanning `/sbin`, `/etc/visor`, `/usr/bin`, `/usr/lib`, `/opt/toolchain`, and `/src/hanzo/vfs`.
* **Backend & S3 model:** Every regime writes and reads through the real `pkg/backend/file` (`file://`) backend for disk cold-start timings and bit-exact verification; S3 cold-start latency models `1` manifest `GET` + measured pack `GET`s at `2.0 ms` RTT and `400 MiB/s` transfer plus real measured AEAD unseal + BLAKE3 + decompress CPU time.

## 4. Matched-Baseline Evaluation

### A. Primary 4-Regime Comparison (`16 KiB` chunks, `1 MiB` target packs, `256 KiB` hybrid boot pack, `longlz_flate`)

| Regime | Compression Ratio | Gain vs Path | Stored Ratio (incl. AEAD+Manifest) | Sealed Size (MiB) | Packs (Boot) | Mean Adj Sim | Build (ms) | Cold Packs Fetched | Read Amp (Net) | Raw Read Amp | Cold Disk (ms) | Cold S3 (ms) | Bit-Exact |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `path_order_baseline` | **4.44x** | +0.0% | 3.98x | 3.48 | 14 (0) | 0.215 | 510.2 | 11 | **3.45x** | 13.99x | 85.01 | **109.45** | PASS |
| `random_order_control` | **4.28x** | -3.4% | 3.85x | 3.59 | 14 (0) | 0.155 | 481.3 | 14 | **4.18x** | 17.66x | 104.05 | **130.69** | PASS |
| `rubikfs_pure_similarity` | **10.18x** | +129.5% | 8.05x | 1.72 | 14 (0) | 0.597 | 530.8 | 14 | **1.79x** | 17.66x | 59.46 | **87.88** | PASS |
| `rubikfs_locality_hybrid` | **9.00x** | +102.8% | 7.28x | 1.90 | 18 (4) | 0.578 | 574.2 | 4 | **0.28x** | 1.00x | 11.14 | **20.06** | PASS |

### B. `64 KiB` Chunk Verification (`1 MiB` target packs, `longlz_flate`)

Demonstrates that pack-wide long-range LZ77 captures cross-chunk similarity even when chunk size (`64 KiB`) exceeds standard DEFLATE's `32 KiB` sliding window:

| Regime | Compression Ratio | Gain vs Path | Sealed Size (MiB) | Cold Packs Fetched | Read Amp (Net) | Cold S3 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| `path_order_baseline` | **4.51x** | +0.0% | 3.31 | 11 | 3.27x | 107.32 |
| `rubikfs_pure_similarity` | **9.45x** | +109.5% | 1.71 | 12 | 1.73x | 79.87 |
| `rubikfs_locality_hybrid` | **8.42x** | +86.7% | 1.89 | 4 | 0.28x | 17.83 |

### C. Pack Granularity Sweep (`256 KiB` -> `8 MiB`, including Negative Regime #3)

| Pack Size | Regime | Compression Ratio | Packs | Cold Packs Fetched | Cold Bytes Fetched (KiB) | Boot Read Amp (Net) | Boot Raw Read Amp | 1-File Cold Raw Amp | Cold S3 (ms) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 KiB | `path_order_baseline` | 3.83x | 56 | 18 | 1084.7 | 1.35x | 5.66x | **11.0x** | 72.44 |
| 256 KiB | `rubikfs_pure_similarity` | 9.19x | 56 | 43 | 1210.6 | 1.51x | 13.67x | **22.1x** | 133.98 |
| 256 KiB | `rubikfs_locality_hybrid` | 8.20x | 57 | 4 | 228.2 | 0.28x | 1.00x | **21.9x** | 17.77 |
| 1024 KiB | `path_order_baseline` | 4.44x | 14 | 11 | 2763.6 | 3.45x | 13.99x | **43.9x** | 111.04 |
| 1024 KiB | `rubikfs_pure_similarity` | 10.18x | 14 | 14 | 1438.4 | 1.79x | 17.66x | **88.3x** | 88.51 |
| 1024 KiB | `rubikfs_locality_hybrid` | 9.00x | 18 | 4 | 228.2 | 0.28x | 1.00x | **88.5x** | 18.99 |
| 4096 KiB | `path_order_baseline` | 6.31x | 4 | 3 | 1979.9 | 2.47x | 15.31x | **176.5x** | 75.12 |
| 4096 KiB | `rubikfs_pure_similarity` | 10.40x | 4 | 4 | 1408.2 | 1.76x | 17.66x | **353.8x** | 68.27 |
| 4096 KiB | `rubikfs_locality_hybrid` | 9.22x | 8 | 4 | 228.2 | 0.28x | 1.00x | **177.0x** | 18.67 |
| 8192 KiB | `path_order_baseline` | 8.03x | 2 | 2 | 1810.3 | 2.26x | 17.66x | **353.5x** | 80.75 |
| 8192 KiB | `rubikfs_pure_similarity` | 10.51x | 2 | 2 | 1393.5 | 1.74x | 17.66x | **612.1x** | 62.41 |
| 8192 KiB | `rubikfs_locality_hybrid` | 9.25x | 6 | 4 | 228.2 | 0.28x | 1.00x | **354.0x** | 17.87 |

## 5. Tenant Isolation & Zero Cross-Volume Plaintext Deduplication Proof

Identical base-image files were built independently for two distinct scopes on the exact same `file://` backend using the same cluster KMS root secret:

| Security / Isolation Invariant | Observed Value | Status |
|---|---:|:---:|
| Volume A (`tenant-alpha/vol-01`) total stored bytes | 48922 B | PASS |
| Volume B (`tenant-beta/vol-02`) total stored bytes | 48917 B | PASS |
| Combined backend bytes (`A + B`) | 97839 B | PASS |
| Cross-volume plaintext deduplication bytes | **0 B** | PASS |
| Shared backend object keys across volumes | **0** | PASS |
| Identical pack ciphertexts across volumes | **0** | PASS |
| Cross-volume `OpenImage` rejected (fail-closed) | true | PASS |
| Transplanted pack ciphertext rejected (AAD + key binding) | true | PASS |
| Single-bit pack flip rejected (`ErrCorruptPack`) | true | PASS |

## 6. Honest Negative Regimes & Operational Guardrails

### Negative Regime #1 — Pure Similarity Scattering Cold-Start Boot Files

Naive `rubikfs_pure_similarity` achieves the highest compression ratio (**10.18x** vs `4.44x` for `path_order_baseline`), but rearranges boot-hot files across **14 packs** (vs **4** for `rubikfs_locality_hybrid` and 11 for `path_order_baseline`), resulting in **17.66x** raw read amplification and **87.88 ms** S3 cold-start latency. At `256 KiB` pack granularity (Table 4C), pure similarity scatters the `19`-file boot working set across **43 packs** (`13.67x` raw read amp, `133.98 ms` S3 cold start — nearly 2x slower than `path_order_baseline`!). Pinning boot-hot chunks under `rubikfs_locality_hybrid` cuts cold-start packs fetched to **4**, lowers S3 cold-start latency to **20.06 ms** (vs `87.88 ms` for pure similarity and `109.45 ms` for path order), and reduces net read amplification to **0.28x** while retaining **9.00x** overall compression ratio.

### Negative Regime #2 — Incompressible / Pre-Compressed Payloads (`.zst`, `.gz`, `.age`, random weights)

On `64` high-entropy files (`4.00 MiB` of uniform random / encrypted / pre-compressed blobs), similarity clustering yields **~1.00x (0.0% compression gain)** while still paying deterministic sketching + clustering CPU overhead (mitigated by raw-mode fallback preventing ciphertext bloat):

| Regime | Compression Ratio | Gain vs Path | Sketch (ms) | Cluster (ms) | Total Build (ms) | Raw Fallback Expansion |
|---|---:|---:|---:|---:|---:|---:|
| `path_order_baseline` | 1.0000x | +0.00% | 3.92 | 0.03 | 101.3 | +0.000% |
| `rubikfs_pure_similarity` | 1.0000x | +0.00% | 74.17 | 1.84 | 170.4 | +0.000% |
| `rubikfs_locality_hybrid` | 1.0000x | -0.00% | 73.29 | 0.10 | 211.9 | +0.000% |

### Negative Regime #3 — Oversized Pack Granularity (`8 MiB` Packs)

As shown in Table 4C above, scaling `TargetPackBytes` from `256 KiB` up to `8 MiB` yields diminishing compression returns (`9.19x -> 10.51x` for `rubikfs_pure_similarity`), while **blowing up single-file cold raw read amplification from `22.1x` to `612.1x`** because reading a single cold file forces fetching and decompressing an entire `8 MiB` pack (and unpinned boot reads pull `100%` of the image, `17.66x` raw read amp!). **Operational guardrail:** default to `PolicyLocalityHybrid` with `TargetPackBytes = 1 MiB` (`16 KiB` chunks) and `BootPackBytes = 256 KiB`.

## 7. Reproducing

```sh
go test -v -race -count=1 ./pkg/rubikfs/... ./cmd/rubikfs-bench/...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./pkg/rubikfs/... ./cmd/rubikfs-bench/...
go run ./cmd/rubikfs-bench --seed 1344 --repo-root . --out-dir docs/research/hip-1344-rubikfs-images
```
