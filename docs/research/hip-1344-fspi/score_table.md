# FS-PI End-to-End Journal Integrity — Measured Score Table (HIP-1344 / HIP-1345)

- **Timestamp (UTC)**: `2026-11-25T13:00:46Z`
- **Host**: `Intel(R) Xeon(R) CPU @ 2.30GHz` (`linux/amd64`, `2` vCPUs, `Linux version 6.18.57`, `go1.27.1`)
- **NVMe Hardware PI Probe (`/sys/block/*/integrity`)**: `supported=false` — none of 10 host block devices (loop0, loop1, loop2, loop3, loop4, loop5, loop6, loop7, pmem0, vda) expose NVMe/T10-DIF hardware protection information; falling back to software FS-PI validation

## 1. Sector Integrity, Pre-Plaintext Rejection & Performance Summary

| Mode | Detection Coverage (7 Faults) | Pre-Plaintext Rejection | Active Forgery Caught (PI Layer) | Write (MB/s) | Read (MB/s) | Write p50 / p99 (us) | Read p50 / p99 (us) | CPU (us/MB) | CPU Overhead vs AEAD (%) | Metadata Overhead (%) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `none` | 0/7 (0.0%) | 0/7 (0.0%) | NO (Bypassed) | 165.90 | 592.75 | 5.02 / 61.81 | 3.93 / 76.11 | 5023.57 | -9.75% | 0.000% |
| `aead_only_baseline` | 5/7 (71.4%) | 0/7 (0.0%) | NO (Bypassed) | 118.75 | 380.65 | 6.24 / 95.65 | 5.32 / 90.28 | 5566.32 | +0.00% | 0.000% |
| `software_crc32c` | 7/7 (100.0%) | 7/7 (100.0%) | NO (Bypassed) | 129.34 | 426.13 | 6.76 / 62.47 | 6.13 / 57.18 | 5980.69 | +7.44% | 0.489% |
| `keyed_blake3_pi` | 7/7 (100.0%) | 7/7 (100.0%) | YES (Pre-Decrypt) | 114.13 | 306.17 | 10.10 / 87.44 | 9.13 / 76.26 | 7909.38 | +42.09% | 0.879% |

## 2. Per-Fault Below-Encryption Detection Matrix

| Fault Class (Injected Directly on Raw Disk Bytes) | `none` | `aead_only_baseline` | `software_crc32c` | `keyed_blake3_pi` |
|---|---|---|---|---|
| `single_bit_flip` | UNDETECTED (Silent Corruption) | CAUGHT at AEAD (1 decrypt call) | CAUGHT (Pre-Decrypt, 0 AEAD calls) | CAUGHT (Pre-Decrypt, 0 AEAD calls) |
| `multi_byte_burst_corruption` | UNDETECTED (Silent Corruption) | CAUGHT at AEAD (1 decrypt call) | CAUGHT (Pre-Decrypt, 0 AEAD calls) | CAUGHT (Pre-Decrypt, 0 AEAD calls) |
| `misdirected_write_block_swap` | UNDETECTED (Silent Corruption) | UNDETECTED (Silent Corruption) | CAUGHT (Pre-Decrypt, 0 AEAD calls) | CAUGHT (Pre-Decrypt, 0 AEAD calls) |
| `stale_lost_write_replay` | UNDETECTED (Silent Corruption) | UNDETECTED (Silent Corruption) | CAUGHT (Pre-Decrypt, 0 AEAD calls) | CAUGHT (Pre-Decrypt, 0 AEAD calls) |
| `cross_tenant_block_transplant` | UNDETECTED (Silent Corruption) | CAUGHT at AEAD (1 decrypt call) | CAUGHT (Pre-Decrypt, 0 AEAD calls) | CAUGHT (Pre-Decrypt, 0 AEAD calls) |
| `zeroed_sector_phantom_write` | UNDETECTED (Silent Corruption) | CAUGHT at AEAD (1 decrypt call) | CAUGHT (Pre-Decrypt, 0 AEAD calls) | CAUGHT (Pre-Decrypt, 0 AEAD calls) |
| `truncated_torn_tail_sector` | UNDETECTED (Silent Corruption) | CAUGHT at AEAD (1 decrypt call) | CAUGHT (Pre-Decrypt, 0 AEAD calls) | CAUGHT (Pre-Decrypt, 0 AEAD calls) |
| `crc_only_active_adversary_forgery` *(Negative Regime #1)* | UNDETECTED (Silent Corruption) | CAUGHT at AEAD (1 decrypt call) | CAUGHT at AEAD (1 decrypt call) | CAUGHT (Pre-Decrypt, 0 AEAD calls) |

## 3. End-to-End `journal.Journal` Pipeline Benchmark (Append + Sync + Replay)

| Requested Mode | Effective Mode | Records (4 KiB) | Append+Sync (MB/s) | Replay (MB/s) | CPU (us/MB) | Sectors Written | Sectors Verified | Segment Metadata Overhead (%) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `none` | `none` | 1024 | 61.75 | 1224.05 | 4322.50 | 0 | 0 | 0.000% |
| `software_crc32c` | `software_crc32c` | 1024 | 42.19 | 376.92 | 11974.25 | 1064 | 1285 | 0.492% |
| `keyed_blake3_pi` | `keyed_blake3_pi` | 1024 | 51.11 | 281.03 | 14734.75 | 1064 | 1285 | 0.883% |
| `hw_nvme_pi_probe` | `keyed_blake3_pi` | 1024 | 46.45 | 296.09 | 14320.75 | 1064 | 1285 | 0.883% |
