# HIP-1350 Research Dossier: Decentralized Model Distribution and Provenance (`hanzoai/vfs#38`)

> **Deliverable:** Clean-room Go package [`internal/modeldist`](../../../internal/modeldist) + reproducible five-dimension benchmark CLI [`tools/modeldist-bench`](../../../tools/modeldist-bench) + raw measurements ([`score_table.md`](./score_table.md), [`raw_results.json`](./raw_results.json)).  
> **Tracks:** `hanzoai/vfs#38`, HIP-1350.  
> **Host disclosure:** All empirical figures below were measured on the authoring Linux VM (`go1.27.1`, `linux/amd64`, 2 vCPU) using real loopback TCP HTTP mirrors and real `pkg/backend/file` VFS storage — not production WAN/NVMe SLA guarantees until reproduced on target cluster hardware.

---

## 1. Primary References & Clean-Room License Audit

### Primary Technical References
1. **IPFS Kubo (Content-Addressed Chunking & Immutable CID Distribution)** — Protocol Labs, *Kubo / IPLD / Content-Addressed DAG Architecture* (`https://github.com/ipfs/kubo`, MIT / Apache-2.0 dual-licensed). Motivates immutable content identifiers (CIDs) decoupling *what* bytes a model contains from *where* those bytes are hosted so independent mirrors can serve chunks without trusting the host.
2. **Zoo Labs User-Owned AI Models & Decentralized Provenance** — Zoo Labs Foundation, *Decentralized / User-Owned Model Lineage, Open Licensing, and Verifiable Weight Distribution* (`https://zoo.ngo`). Motivates binding weights, GGUF quantizations, adapters, tokenizers, and execution profiles to explicit SPDX licenses, semantic versions, and parent provenance CIDs so communities can mirror, audit, and fork models independently.
3. **BLAKE3 & Ed25519 Primitives** — BLAKE3 team (`internal/blake3`, already in `hanzoai/vfs`, Apache-2.0/CC0) and RFC 8032 Ed25519 (`crypto/ed25519` in Go stdlib) for fast verifiable chunk digests and compact publisher signatures.

### Clean-Room & Dependency Audit
- **License:** Clean-room Apache-2.0 Go implementation written from scratch; **zero code, protobuf/IPLD grammars, or test fixtures copied** from IPFS Kubo or external repositories.
- **Zero new dependencies:** `go.mod` and `go.sum` are 100% unmodified. Uses only Go standard library (`crypto/ed25519`, `crypto/sha256`, `encoding/json`, `net/http`, `os`, `sync`) plus existing `hanzoai/vfs` packages (`internal/blake3`, `pkg/backend`, `pkg/backend/file`, and `internal/storetest` in tests/benchmarks).
- **Zero second disk format:** Chunks and manifests map directly onto `hanzoai/vfs`'s existing `pkg/backend.Backend` key space (`model/chunk/<2hex>/<blake3>`, `model/manifest/<2hex>/<cid>`, `model/ref/<model>/<version>`), strictly conforming to `backend.ValidKey`. Format-v2 volume files (`tree.go`, `record.go`, `shard.go`, `spec_test.go`, `pin_test.go`) are completely untouched.
- **Proof-claim discipline (HIP-1350 Rule 3):** This package provides **cryptographic content integrity and publisher authenticity** (BLAKE3 + SHA-256 chunk/root digests and domain-separated Ed25519 manifest signatures). It does **not** claim hardware TEE attestation, zero-knowledge inference proofs, or on-chain economic slashing; those are orthogonal layers.

---

## 2. Reproducible Baselines & Benchmark Commands

Three configurations are compared on identical model payloads and hardware:

1. **Centralized Single-Origin Baseline (`neg_2_single_origin_removed`):** Single HTTP/VFS origin with zero independent mirrors (`$0.09/GB` cloud S3 egress model). When origin is removed, availability drops to `0.0%`.
2. **Unverified Mirror Baseline (`neg_1_unverified_mirror`):** Independent HTTP mirror pull omitting chunk and root digest checks (`UnsafeUnverifiedMirrorPullForNegativeControl`). Demonstrates silent weight poisoning (`0.0%` detection, `100.0%` corrupted chunks accepted).
3. **Decentralized Verified Protocol (`internal/modeldist`):** Signed `ManifestV1` + 3 independent HTTP mirrors (`mirror-1`, `mirror-2`, `mirror-3`) + dual BLAKE3/SHA-256 per-chunk & full-stream root verification + immediate quarantine/failover + atomic disk-checkpointed resume.

### Exact Reproduction Commands
```bash
# 1. Full race-checked acceptance, adversarial, concurrency, and negative-control suite
#    (includes full 2.50 GiB 3-mirror verified stream without -short)
go test -race -v -count=1 ./internal/modeldist/...

# 2. Fast CI-friendly race check (-short scales the multi-GiB stream to 256 MiB)
go test -race -short -v -count=1 ./internal/modeldist/...

# 3. Reproduce raw_results.json and score_table.md (full 2.50 GiB, 640 x 4 MiB chunks)
go run ./tools/modeldist-bench -write-dossier docs/research/hip-1350-modeldist

# 4. Verify zero regressions in hanzoai/vfs root format-v2 specs and cross-platform build
go vet ./internal/modeldist/... ./tools/modeldist-bench/...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./internal/modeldist/... ./tools/modeldist-bench/...
go test -short -run 'TestSpec|TestSparse' .
```

---

## 3. Falsifiable Hypothesis, Five-Dimension Metrics & Failure Modes

### Falsifiable Hypothesis
1. **Multi-GiB Decentralized Availability:** Distributing a **2.50 GiB** (`2,684,354,560` bytes, 640 × 4 MiB chunks) signed model across 3 independent HTTP mirrors and shutting down the origin completely will still achieve **100.0% completion**, **0 origin hits after kill**, **exact BLAKE3 + SHA-256 root match**, balanced chunk distribution (`~213–214` chunks/mirror), and bounded memory (`< 128 MiB` heap) via a bounded reorder window.
2. **Fail-Closed Cryptographic Integrity:** Injecting bit-flips, chunk truncations, forged Ed25519 signatures, untrusted keys, post-sign manifest edits, disallowed SPDX licenses, out-of-range SemVer, and path traversal will be detected at **100.0% (8/8 probes)** — compared to **0.0%** for an unverified mirror baseline — while automatically quarantining poisoned mirrors and failing over to healthy mirrors.
3. **Zero-Waste Resumable Transfers:** Interrupting a transfer at 50% and resuming from the atomic disk checkpoint will resume **100%** of intact verified chunks with **0 redundant chunk re-downloads**, while re-verifying on-disk bytes so local partial-file corruption triggers selective re-fetch rather than silent corruption.
4. **Sovereign Offline Export & Forkability:** Exporting a model to a standard `file://` VFS directory, killing all original infrastructure, serving from the offline export, and verifying an independently signed child manifest linked via `Provenance.ParentManifestCID` will succeed with zero origin dependency and zero second disk format.

### Declared Threat Model & Honest Failure Modes
- **What IS defended:** Malicious or corrupted mirrors (bit-flips, truncation, arbitrary byte substitution, wrong chunk served), forged/tampered manifests, untrusted publishers, disallowed or missing licenses, version downgrade/out-of-range attacks, directory traversal via artifact paths, and mid-transfer crashes or partial local disk corruption.
- **Explicit Failure Modes / Boundaries (Not Defended by This Layer Alone):**
  1. **Total Eclipse (`neg_3_all_mirrors_poisoned`):** If *all* reachable mirrors are simultaneously poisoned or offline (`0` honest replicas), cryptographic verification guarantees **safety** (`0` corrupted bytes written, fail-closed `ErrAllProvidersFailed`), **not liveness** (`0.0%` completion).
  2. **Publisher Private Key Compromise:** If a trusted Ed25519 private key in `VerificationPolicy.TrustedKeys` is stolen, an attacker can sign valid manifests until the key is revoked/rotated; threshold co-signing (`MinValidSignatures >= 2`) mitigates single-key compromise.
  3. **Verification CPU Overhead:** Computing both BLAKE3 and SHA-256 across multi-GiB weights consumes host CPU (`~15.9 s` for 2.50 GiB on 2 vCPUs at `160.8 MB/s`). On ultra-low-power edge devices, single-hash BLAKE3-only mode could improve throughput at the cost of losing SHA-256 interoperability with existing GGUF/OCI tooling.

---

## 4. Bounded Implementation & Protocol Architecture

### Package Layout
- [`internal/modeldist/manifest.go`](../../../internal/modeldist/manifest.go):
  - `ManifestV1` schema supporting all 5 required asset kinds (`zen-weights`, `gguf-variant`, `adapter`, `tokenizer`, `execution-profile`), canonical JSON serialization, domain-separated BLAKE3 `ManifestCID` (`"hanzo-vfs-modeldist-cid-v1\x00"`), domain-separated Ed25519 `SignatureEnvelope` (`"hanzo-vfs-modeldist-manifest-v1\x00"`), multi-signature threshold support, `LicensePolicy` (SPDX allowlist/denylist + optional dataset license check), strict `SemVer` comparator, `Provenance.ParentManifestCID` chain resolution, path-traversal defense, and VFS key mapping (`ChunkKey`, `ManifestKey`, `RefKey`).
- [`internal/modeldist/provider.go`](../../../internal/modeldist/provider.go):
  - `ChunkProvider` and `WritableProvider` interfaces.
  - `VFSStoreProvider` wrapping any `pkg/backend.Backend` (`file://`, `s3://`, `storetest.Mem`) with atomic stats, offline simulation, and tamper hooks.
  - `HTTPMirrorServer` and `HTTPMirrorProvider` speaking plain content-addressed HTTP (`GET/HEAD /v1/chunks/<blake3>`, `GET /v1/manifests/<cid>`, immutable cache headers) with zero Hanzo-specific protocol lock-in.
  - `DeterministicChunkCatalog` + `CatalogBackingProvider` generating unique multi-GiB model streams via `internal/blake3` XOF (`Expand`) in `O(chunkSize)` memory.
- [`internal/modeldist/ingest.go`](../../../internal/modeldist/ingest.go):
  - `IngestArtifact` (streaming dual-hash chunking into any `WritableProvider`), `PublishManifest`, `ReplicateManifest` (transit-verified mirror sync), and `ExportBundle` (sovereign offline export to `pkg/backend/file` + `manifest.json`).
- [`internal/modeldist/client.go`](../../../internal/modeldist/client.go):
  - Fail-closed `Client` (`FetchVerifiedManifest`, `PullToDir`, `PullArtifactStream`) with parallel workers, per-provider bad-chunk quarantine and failover, atomic checkpoint persistence (`CreateTemp` + `Sync` + `Rename`), on-disk chunk re-verification on resume, full-artifact streaming BLAKE3 + SHA-256 root check, bounded reorder window, and `UnsafeUnverifiedMirrorPullForNegativeControl` isolated exclusively as a negative benchmark baseline.

---

## 5. Correctness, Concurrency (`-race`), Security & Regression Evidence

All 8 tests in [`internal/modeldist/modeldist_test.go`](../../../internal/modeldist/modeldist_test.go) pass with `-race` and zero data races:

| Test | Coverage | Result |
|---|---|---|
| `TestMultiGiBDistributionAcrossThreeProviders` | 2.50 GiB (640 × 4 MiB chunks) across 3 HTTP mirrors, origin killed, bounded RAM (`15.5 MiB` heap under `-race`), exact BLAKE3 + SHA-256 roots | PASS (`-race`) |
| `TestOriginRemovedFullBundleReconstruction` | 5 asset kinds on real `file://` VFS backend -> replicated to 3 mirrors -> origin closed & `os.RemoveAll` -> byte-verified reconstruction + VFS staging (`0` origin calls) | PASS (`-race`) |
| `TestRejectModifiedChunkAndFailover` | 1-bit flip on Mirror 1 + truncation on Mirror 2 -> both rejected & quarantined -> 100% clean failover to Mirror 3 | PASS (`-race`) |
| `TestResumeInterruptedTransfer` | Abort at 50% -> resume with `0` redundant re-downloads; plus partial-file disk corruption recovery (re-fetches only corrupted chunk 0) | PASS (`-race`) |
| `TestManifestSecurityLicenseVersionProvenance` | Forged sig, untrusted key, 2-of-2 threshold co-sign, post-sign tamper, disallowed license, bad SemVer, `../` path traversal, parent provenance chain | PASS (`-race`) |
| `TestConcurrentPullsRace` | 8 concurrent clients pulling simultaneously across 3 mirrors (1 poisoned) under `-race` | PASS (`-race`) |
| `TestSovereigntyOfflineExportAndFork` | Offline export to `file://` VFS store -> kill origin -> independent remount + child fork verified against `ParentManifestCID` | PASS (`-race`) |
| `TestHonestNegativeBoundaryRegimes` | `unverified_mirror_negative` (100% silent corruption), `single_origin_removed_negative` (0% availability), `all_mirrors_poisoned_fail_closed` (0 corrupted bytes written) | PASS (`-race`) |

Additional verification gates passed:
- `gofmt -s -w internal/modeldist tools/modeldist-bench` (clean)
- `go vet ./internal/modeldist/... ./tools/modeldist-bench/...` (clean)
- `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./internal/modeldist/... ./tools/modeldist-bench/...` (clean)
- `go test -short -run 'TestSpec|TestSparse' .` (zero regression in root `hanzoai/vfs` format-v2 suite)

---

## 6. Measured Five-Dimension Results & Integration Recommendation

Full auto-generated tables and machine-readable JSON live in [`score_table.md`](./score_table.md) and [`raw_results.json`](./raw_results.json).

### Summary Before / After (Measured Live via `go run ./tools/modeldist-bench`)

| HIP-1350 Dimension | Centralized / Unverified Baseline | `internal/modeldist` (3 Independent Mirrors) | Delta / Evidence |
|---|---:|---:|---|
| **Cost** (`$/1M useful tokens`, 1B tokens/pull) | `$0.00024181` (S3 egress `$0.09/GB` + verify) | `$0.00000022` (community/R2 `$0.00/GB` egress + verify CPU) | **99.9% distribution egress reduction**; verification CPU is `$0.000221`/pull (`15.9 s` on 2 vCPU) |
| **Performance** (2.50 GiB verified stream) | Single-origin bottleneck | **160.8 MB/s**, TTFT **73.29 ms**, p50 **70.01 ms**, p99 **163.87 ms**, **100.0%** completion | Balanced across 3 mirrors (`214 / 213 / 213` chunks), peak heap **74.5 MiB** |
| **Trust** (tamper & forgery detection) | **0.0%** (unverified mirror silently accepts `5/5` corrupted chunks) | **100.0%** (`8/8` adversarial probes detected) | Bit-flips, truncations, forged/untrusted sigs, bad license/SemVer, path traversal all fail closed |
| **Decentralization** (work without origin) | **0.0%** (origin kill -> immediate failure) | **100.0%** (`0` origin hits after kill & `os.RemoveAll`) | 100% of manifest + 640 chunks served by independent mirrors |
| **Sovereignty** (offline export & forkability) | Origin-coupled | **PASS** (export + zero-origin remount + parent-linked fork) | Native `pkg/backend/file` VFS layout (`model/chunk/<2hex>/<blake3>`), zero second disk format |

### Honest Negative & Boundary Regimes
- `neg_1_unverified_mirror`: Omitting chunk digest checks silently accepted `5/5` (`100%`) tampered chunks with `0.0%` detection.
- `neg_2_single_origin_removed`: Centralized single-origin without independent mirrors dropped immediately to `0.0%` completion when origin was shut down.
- `neg_3_all_mirrors_poisoned`: Under total eclipse (all 3 mirrors serving modified chunks), `Client` quarantined all 3 mirrors and failed closed (`ErrAllProvidersFailed` wrapping `ErrChunkDigestMismatch`) with `0` corrupted bytes written and `0.0%` completion — honestly demonstrating that cryptographic verification guarantees **safety**, while **liveness** requires at least one honest mirror.

### Integration Recommendation for `hanzoai/vfs`
1. **Promote `internal/modeldist` to `pkg/modeldist`** once maintainers review the `ManifestV1` JSON field names and key prefix (`model/chunk/<2hex>/<blake3>`, `model/manifest/<2hex>/<cid>`), allowing `cmd/vfs`, Visor sandboxes, and Enso routers to pull verified Zen/GGUF weights directly into any `pkg/backend.Backend` (`s3://` or `file://`) with zero format-v2 changes.
2. **Optional next step:** Wire an HTTP Range / IPFS gateway adapter into `ChunkProvider` so existing IPFS Kubo gateways (`/ipfs/<cid>`) and public S3 mirrors can participate side-by-side in the same provider pool without changing `Client` or `ManifestV1`.
