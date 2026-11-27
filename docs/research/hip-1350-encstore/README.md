# HIP-1350 Research Dossier: Decentralized Encrypted Data, Model Checkpoints & Agent Memory (`hanzoai/vfs#39`)

**Package:** `internal/encstore` (Apache-2.0)  
**Benchmark CLI:** `tools/encstore-bench`  
**Measured Run:** `2026-11-26T09:57:28Z` (`go1.27.1`, `linux/amd64`, `2` vCPU)  

## 1. Protocol Architecture & Canonical VFS Preservation

Content addressing is **not** privacy: publishing unencrypted datasets, model checkpoints, or agent memories on a content-addressed network leaks plaintext payloads and exposes deterministic plaintext hashes irreversibly. `internal/encstore` solves `hanzoai/vfs#39` without introducing a second disk format or any external dependency:

- **Zero new dependencies:** `go.mod` and `go.sum` are 100% untouched. All cryptography uses Go stdlib (`crypto/aes`, `crypto/cipher`, `crypto/ecdh` X25519, `crypto/ed25519`, `crypto/hmac`, `crypto/sha256`, `crypto/rand`) plus existing `hanzoai/vfs` packages (`internal/blake3`, `pkg/backend`, `pkg/backend/file`). Format-v2 volume files are 100% untouched.
- **All 5 artifact kinds:** `dataset`, `model-checkpoint`, `agent-memory`, `research-artifact`, and `execution-receipt`.
- **Canonical VFS key space (`backend.ValidKey`):**
  - Encrypted chunks: `enc/chunk/<2hex>/<ciphertext_blake3>` (keyed strictly by **ciphertext** BLAKE3 digest; plaintext digests never appear in keys, URLs, or unencrypted manifest fields).
  - Signed manifests: `enc/manifest/<2hex>/<manifest_cid>`.
  - Artifact heads: `enc/head/<artifact_id>`.
  - Owner-signed revocation lists: `enc/revoke/<owner_key_id>`.
  - Owner-signed erasure tombstones: `enc/tombstone/<artifact_id>`.
  - Sovereign offline export index: `enc/bundle/index`.
- **Per-chunk AEAD & AAD binding:** Each chunk is encrypted with AES-256-GCM under an HKDF-SHA256 derived per-chunk key (`HKDF(DEK, EpochSalt, artifactID || epoch || chunkIndex)`), fresh 96-bit CSPRNG nonce, and canonical AAD binding `{artifactID, epoch, chunkIndex, kind, spdxLicense, consentHash}`.
- **Encrypted private manifest payload:** Plaintext root BLAKE3, plaintext root SHA-256, per-chunk plaintext BLAKE3 digests, exact plaintext lengths, labels, and Zoo Experience Ledger references live strictly inside AES-256-GCM `EncryptedPrivatePayloadHex`.
- **Data-authorized sharing & forward-secure revocation:** Each authorized peer receives an ephemeral X25519 ECDH + HKDF-SHA256 + AES-256-GCM `KeyEnvelope` signed inside `SignedManifest` by the owner's Ed25519 key. `RevokePeer` increments the monotonic key epoch, omits the revoked peer from all subsequent `KeyEnvelope`s, and publishes an updated `SignedRevocationList`.
- **Ownership, licensing, consent & cryptographic erasure:** Mandatory owner Ed25519 signature, SPDX `LicensePolicy` enforcement, purpose-scoped expiring/revocable `ConsentRecord` (bound into every chunk's AAD), and `CryptoShred` (zeroing local DEK cache, replicating signed `DeletionTombstone`, and deleting reachable blobs).

## 2. Reproducible Baselines & Commands

```bash
# 1. Race-checked unit, adversarial, revocation, crypto-shred & sovereign restore suite
go test -race -v -count=1 ./internal/encstore/... ./tools/encstore-bench/...

# 2. Regenerate raw_results.json, score_table.md, and README.md from live measurements
go run ./tools/encstore-bench -checkpoint-mib 64 -write-dossier docs/research/hip-1350-encstore

# 3. Cross-compile & verify zero VFS format-v2 regressions
go vet ./internal/encstore/... ./tools/encstore-bench/...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./internal/encstore/... ./tools/encstore-bench/...
go test -short -run 'TestSpec|TestSparse' .
```

## 3. Measured 5-Dimension Summary (Exact Parity with `raw_results.json`)

| HIP-1350 Dimension | Measured Result |
|---|---|
| **Cost** | Independent nodes: **`$0.00000081` / 1M useful tokens** vs Centralized S3 egress baseline: **`$0.00056331` / 1M useful tokens** (CPU crypto+verify `$0.00000810`/op over `0.58 s`, amortized across 10M useful tokens/restore) |
| **Performance** | 64 MiB 3-node replicated checkpoint: **128.62 MB/s** decrypt+verify, **750.37 MB/s** encrypt, TTFT **7.05 ms**, p50 **4.36 ms**, p95 **9.26 ms**, p99 **12.11 ms**, peak heap **217.73 MiB**, **100.0%** completion |
| **Trust** | **100.0%** (10/10 adversarial & policy probes blocked fail-closed); **0.0%** plaintext/hash leakage on encrypted nodes vs **100.0%** leakage on unencrypted content-addressed baseline |
| **Decentralization** | **100.0%** completion & restore across independent HTTP/VFS nodes with **0** Hanzo account or control-plane calls |
| **Sovereignty** | Offline VFS export (`true`), zero second disk format (`true`), fresh independent-node restore (`true`), and parent-linked child fork (`true`) |

## 4. Declared Threat Model & Honest Negative / Boundary Regimes

Following HIP-1350 Rule 3, we explicitly separate cryptographic confidentiality/integrity guarantees from availability, hardware attestation, and economic incentives, and benchmark **three honest negative / boundary regimes**:

1. **`neg_1_unencrypted_content_addressed_negative` (Content Addressing Is Not Privacy):** Writing raw unencrypted chunks via `UnsafeUnencryptedContentAddressedPutForNegativeControl` results in **100.0%** plaintext canary and plaintext-hash leakage to any passive storage node or peer. By contrast, `internal/encstore` achieves **0.0%** leakage on raw node disk scans.
2. **`neg_2_revoked_peer_prior_epoch_boundary` (Revocation Applies to Future Key Distribution, Not Retroactive Un-Seeing):** Revoking Peer A increments the epoch and blocks **100.0%** of post-revocation epochs (`epoch >= 2`) both via omitted X25519 `KeyEnvelope`s (`ErrAccessDenied`) and via the owner-signed `SignedRevocationList` (`ErrPeerRevoked`). However, any Epoch 1 ciphertext + `KeyEnvelope` already fetched by Peer A prior to revocation remains locally decryptable by Peer A — no purely cryptographic protocol can erase information already decrypted by a formerly authorized peer.
3. **`neg_3_all_replicas_corrupted_eclipse_boundary` (Safety vs. Liveness Under Total Replica Corruption):** When all 3 independent storage replicas are simultaneously corrupted, `internal/encstore` fails closed with **0** corrupted bytes accepted (**100.0%** integrity safety, `ErrAllReplicasFailed`) and **0.0%** availability. Availability requires at least one honest, reachable replica.

## 5. Proof-Claim Boundaries & External Review Note

- **What is cryptographically proven here:** Confidentiality against passive/unauthorized nodes and post-revocation peers (AES-256-GCM + X25519 ECDH + HKDF-SHA256), integrity and non-malleability of chunk ordering/licensing/consent metadata (BLAKE3 + SHA-256 + GCM AAD), and owner authenticity (Ed25519).
- **What is NOT claimed:** Hardware TEE memory confidentiality on compromised authorized reader hosts, retroactive deletion of already-decrypted copies on malicious peers, or on-chain economic storage-provider slashing.
- **External review:** Per HIP-1350, cryptographic envelope rotation and erasure protocols require independent third-party cryptographic review before safeguarding regulated or high-value production assets.

## 6. Artifacts in This Dossier

- [`README.md`](./README.md) — Architecture, threat model, reproduction commands, and 5-dimension summary.
- [`score_table.md`](./score_table.md) — Tabular 5-dimension scorecard and per-scenario empirical matrix.
- [`raw_results.json`](./raw_results.json) — Complete machine-readable benchmark output.
