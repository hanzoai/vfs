# HIP-1350 Measured Score Table — Decentralized Model Distribution (`internal/modeldist`)

Generated on authoring host (`go1.27.1`, `linux/amd64`, `2` vCPU) at `2026-11-26T09:38:44Z` via `go run ./tools/modeldist-bench`.

## 1. HIP-1350 Five-Dimension Scorecard

| Dimension | Centralized / Unverified Baseline | Decentralized `internal/modeldist` (3 Independent Mirrors) | Notes |
|---|---:|---:|---|
| **Cost** (`$/1M useful tokens`, 1B tokens/pull) | `$0.00024181` (S3 egress + verify) | `$0.00000022` (community/R2 peer + verify) | Verification CPU = `$0.000221`/pull (15.9 s on 2 vCPU); Host CPU $0.05/hr (4 vCPU); centralized S3 egress $0.09/GB; community/R2 mirror egress $0.00/GB; amortized across 1B useful tokens (1,000M tokens) served per pulled replica. |
| **Performance** (2.50 GiB verified stream) | Single-origin bottleneck | **160.8 MB/s**, TTFT **73.29 ms**, p50 **70.01 ms**, p99 **163.87 ms**, **100.0%** completion | Dual BLAKE3 + SHA-256 verification over real loopback TCP HTTP (peak heap **74.5 MiB**) |
| **Trust** (tamper & forgery detection) | **0.0%** (unverified mirror accepts 100% silent corruption) | **100.0%** (8/8 adversarial probes detected) | Ed25519 manifest sigs + dual BLAKE3/SHA-256 chunk & root verification |
| **Decentralization** (work with origin disabled) | **0.0%** (origin kill -> total failure) | **100.0%** (0 origin hits after kill) | 3 independent mirrors serve 100% of manifest + chunks |
| **Sovereignty** (offline export, zero 2nd format, fork) | Origin-coupled | **PASS** (export=true, zero2ndFmt=true, fork=true) | Native `pkg/backend/file` VFS keys (`model/chunk/<2hex>/<blake3>`) |

## 2. Per-Scenario Empirical Results (Including Honest Negative Regimes)

| Scenario | Category | Status | Bytes | Chunks (DL/Res/Rej) | Completion | Detection | Decentralized | Throughput | Elapsed | Notes |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| `multi_gib_3_mirrors` | acceptance | PASS | 2684354560 | 640 / 0 / 0 | 100.0% | n/a | 100.0% | 160.8 MB/s | 15923.5 ms | Every chunk verified with BLAKE3 + SHA-256 over real TCP loopback HTTP; bounded reorder window. |
| `origin_removed_5_asset_bundle` | acceptance | PASS | 2139648 | 13 / 0 / 0 | 100.0% | n/a | 100.0% | 14.4 MB/s | 141.7 ms | Staged chunks simultaneously into native pkg/backend/file VFS store (zero second disk format). |
| `adversarial_trust_matrix` | acceptance | PASS | 1091072 | 13 / 0 / 3 | 100.0% | 100.0% | 100.0% | 18.5 MB/s | 76.0 ms | Detected 8/8 injected attacks; quarantined [adv-bitflip adv-trunc] and failed over cleanly. |
| `resumable_interrupt_50pct` | acceptance | PASS | 4194304 | 8 / 8 / 0 | 100.0% | n/a | 100.0% | 44.7 MB/s | 89.6 ms | Resumed 8/16 verified chunks with 0 redundant re-downloads. |
| `sovereignty_offline_export_fork` | sovereignty | PASS | 1091072 | 13 / 0 / 0 | 100.0% | n/a | 100.0% | 22.3 MB/s | 112.0 ms | Exported directly to file:// VFS store with zero second disk format; child fork verified against parent CID. |
| `neg_1_unverified_mirror` | negative_control | PASS | 528384 | 5 / 0 / 0 | 100.0% | 0.0% | 100.0% | 0.0 MB/s | 8.5 ms | Unverified pull silently accepted 5/5 corrupted chunks (0% detection). |
| `neg_2_single_origin_removed` | negative_control | PASS | 0 | 0 / 0 / 0 | 0.0% | n/a | 0.0% | 0.0 MB/s | 4.4 ms | Centralized baseline drops immediately to 0% availability when origin is removed. |
| `neg_3_all_mirrors_poisoned` | negative_control | PASS | 0 | 0 / 0 / 7 | 0.0% | 100.0% | 0.0% | 0.0 MB/s | 13.0 ms | Cryptographic verification guarantees safety (0 corrupted bytes accepted), not liveness when 0 honest replicas exist. |
