# HIP-1350 Measured Score Table — Decentralized Encrypted Data, Checkpoints & Agent Memory (`internal/encstore`)

Generated on authoring host (`go1.27.1`, `linux/amd64`, `2` vCPU) at `2026-11-26T09:57:28Z` via `go run ./tools/encstore-bench -checkpoint-mib 64 -write-dossier docs/research/hip-1350-encstore`.

## 1. HIP-1350 Five-Dimension Scorecard

| Dimension | Unencrypted / Centralized Baseline | Decentralized `internal/encstore` (3 Independent Nodes) | Notes |
|---|---:|---:|---|
| **Cost** (`$/1M useful tokens`, 10M tokens/restore) | `$0.00056331` (S3 egress + crypto) | `$0.00000081` (independent peer + crypto) | Crypto+verify CPU = `$0.00000810`/op (`0.58 s` on 2 vCPU); Host CPU $0.05/hr (4 vCPU) covering AES-256-GCM + HKDF-SHA256 + BLAKE3/SHA-256 + Ed25519; centralized S3 egress $0.09/GiB vs independent peer/R2 $0.00/GiB; amortized over 10M useful tokens per restored checkpoint session. |
| **Performance** (64 MiB encrypted checkpoint) | Single-origin unencrypted | **128.62 MB/s** decrypt+verify (**750.37 MB/s** encrypt), TTFT **7.05 ms**, p50 **4.36 ms**, p95 **9.26 ms**, p99 **12.11 ms**, **100.0%** completion | AES-256-GCM + HKDF-SHA256 + BLAKE3/SHA-256 + Ed25519 over real 3-node loopback TCP HTTP (peak heap **217.73 MiB**) |
| **Trust** (privacy, tampering, revocation, license, consent) | **100.0%** raw plaintext leakage (unencrypted content addressing) | **100.0%** (10/10 adversarial/policy probes blocked), **0.0%** plaintext/hash leakage | Cryptographic confidentiality & integrity separated from availability (see Negative Regimes #1–#3) |
| **Decentralization** (work without Hanzo account/control plane) | **0.0%** (centralized origin coupled) | **100.0%** (**0** Hanzo account/control-plane calls) | 3 independent HTTP `pkg/backend/file` nodes + zero-account offline restore |
| **Sovereignty** (export, restore, zero 2nd format, fork) | Origin-locked | **PASS** (export=true, zero2ndFmt=true, restore=true, fork=true) | Canonical `pkg/backend/file` VFS keys (`enc/chunk/<2hex>/<ct_blake3>`, `enc/manifest/...`) |

## 2. Per-Scenario Empirical Results (Including 3 Honest Negative / Boundary Regimes)

| Scenario | Category | Status | Plaintext B | Ciphertext B | Chunks (Ver/Rej) | Completion | Detection | Leakage | Decentralized | Throughput | Elapsed |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `encrypted_checkpoint_3node_throughput` | acceptance | PASS | 67108864 | 67110976 | 64 / 0 | 100.0% | n/a | n/a | 100.0% | 128.62 MB/s | 2584.01 ms |
| `agent_state_5kind_3node_replicated` | acceptance | PASS | 1310969 | 1311794 | 25 / 0 | 100.0% | n/a | 0.0% | 100.0% | 2.81 MB/s | 444.95 ms |
| `sovereign_export_zero_hanzo_restore` | sovereignty | PASS | 655360 | 655690 | 10 / 0 | 100.0% | n/a | n/a | 100.0% | 1.65 MB/s | 378.01 ms |
| `adversarial_trust_and_policy_matrix` | acceptance | PASS | 196608 | 196707 | 6 / 2 | 100.0% | 100.0% | n/a | 100.0% | 1.66 MB/s | 112.98 ms |
| `revocation_forward_security_epoch2` | acceptance | PASS | 65536 | 65569 | 1 / 0 | 100.0% | 100.0% | n/a | 100.0% | n/a | 77.39 ms |
| `crypto_shred_erasure` | acceptance | PASS | 131072 | 131105 | 0 / 0 | 100.0% | 100.0% | n/a | 100.0% | n/a | 72.38 ms |
| `neg_1_unencrypted_content_addressed_negative` | negative_regime | PASS | 98443 | 0 | 4 / 0 | 100.0% | n/a | 100.0% | 100.0% | n/a | 21.88 ms |
| `neg_2_revoked_peer_prior_epoch_boundary` | negative_regime | PASS | 65536 | 65569 | 1 / 0 | 100.0% | 100.0% | n/a | 100.0% | n/a | 75.96 ms |
| `neg_3_all_replicas_corrupted_eclipse_boundary` | negative_regime | PASS | 0 | 0 | 0 / 6 | 0.0% | 100.0% | n/a | 0.0% | n/a | 92.56 ms |

### Scenario Notes

- **`encrypted_checkpoint_3node_throughput`** (PASS): 64 MiB checkpoint (64 x 1 MiB AES-256-GCM chunks) replicated 3/3 across independent HTTP nodes and verified end-to-end.
- **`agent_state_5kind_3node_replicated`** (PASS): All 5 #39 artifact kinds replicated 3/3; raw node scan found 0/3 canaries and 0/5 plaintext hashes leaked (0.0% leakage).
- **`sovereign_export_zero_hanzo_restore`** (PASS): Exported 5-kind agent state directly to file:// VFS keys (zero second disk format), killed all 3 original nodes, restored on fresh independent node (0 Hanzo calls), and verified child fork lineage.
- **`adversarial_trust_and_policy_matrix`** (PASS): Blocked 10/10 adversarial/policy attacks (unauthorized read, bit-flip, truncation, forged sig, untrusted owner, commercial/SPDX license violations, expired/mismatched/revoked consent).
- **`revocation_forward_security_epoch2`** (PASS): Revoked peer blocked 100% on Epoch 2 (both signed revocation list & omitted X25519 KeyEnvelope); authorized Peer B restored Epoch 2 cleanly.
- **`crypto_shred_erasure`** (PASS): Zeroed local DEK cache (1->0), replicated signed DeletionTombstone 3/3, deleted cooperative blobs, and blocked restore against retained uncooperative copy (ErrTombstoned).
- **`neg_1_unencrypted_content_addressed_negative`** (PASS): Honest Negative Regime #1: raw unencrypted content-addressed chunks leaked 3/3 sensitive memory canaries and 1/1 plaintext CIDs verbatim (100.0% leakage) — proving content addressing is NOT privacy.
- **`neg_2_revoked_peer_prior_epoch_boundary`** (PASS): Honest Negative Regime #2: 100% of post-revocation Epoch 2 artifacts blocked (ErrAccessDenied), while cached Epoch 1 data already received prior to revocation remains locally decryptable by Peer A (cryptographic boundary).
- **`neg_3_all_replicas_corrupted_eclipse_boundary`** (PASS): Honest Negative Regime #3: all 3 replicas poisoned; failed closed with 0 corrupted bytes accepted (6 corrupted chunks rejected, 100% safety) and 0.0% availability (liveness requires >= 1 honest replica).
