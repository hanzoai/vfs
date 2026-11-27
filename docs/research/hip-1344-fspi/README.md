# HIP-1344 / HIP-1345 Research & Implementation Dossier: End-to-End Journal Integrity (FS-PI)

**Issues**: `hanzoai/vfs#55`, `hanzoai/vfs#67`  
**Umbrella HIP**: HIP-1344 (Agent-Optimized Storage and Recovery) / HIP-1345  
**Cipher Composition**: HIP-0302 / AES-256-GCM AEAD envelopes (`ledger.go` + `cipher.go`)  
**Canonical Stack Preserved**: `vfs -> hanzoai/s3 -> visor` (no parallel storage paths or external services introduced)

---

## 1. Primary Paper, Design Summary & License Audit

- **Primary Research**: FS-PI — *End-to-End Storage Integrity with Protection Information*, Gupta et al., USENIX FAST '26 ([https://www.usenix.org/conference/fast26/presentation/gupta](https://www.usenix.org/conference/fast26/presentation/gupta)).
- **Underlying Standards**: SCSI SBC-4 T10-DIF (8-byte Protection Information, CRC-16 polynomial `0x8BB7`) and NVMe 2.0 NVM Command Set Specification §5.2 (Extended LBA / Separate Metadata Protection Information: Guard Tag + Application Tag + Reference Tag).
- **Summary of Design**:
  - Local journals written ahead of object-store publication sit below the encrypted object layer and are exposed to silent media bitrot, misdirected writes (LBA swaps), lost/stale writes (generation replay), zeroed-sector phantom writes, torn tails, and cross-tenant block transplants.
  - FS-PI attaches a per-sector Protection Information tuple (`PITuple`) to every 4 KiB logical sector written through `journal.Disk` / `journal.File`, binding `GuardTag`/`GuardMAC` (data integrity), `AppTag` (tenant + volume domain isolation), `RefTag` (logical block address), and `Generation` (monotonic overwrite epoch persisted in an authenticated per-segment trailer).
  - On `ReadAt`, `FSPIFile` verifies every touched sector's `PITuple` **before** copying bytes into the caller's buffer or invoking the upper HIP-0302 AEAD cipher (`AES-256-GCM`). On any mismatch, the caller buffer is cleared and an `ErrCorrupt`-wrapped `ErrFSPIIntegrity` sentinel is returned with zero AEAD decrypt invocations.
- **License & Patent Audit**:
  - **Code provenance**: 100% clean-room Go implementation written directly for `hanzoai/vfs` (`journal/fspi.go`, `journal/fspi_probe_linux.go`, `journal/fspi_probe_other.go`, `journal/fspi_fault_inject.go`, `journal/fspi_test.go`, `tools/fspi-bench/main.go`). No upstream paper code was copied or vendored.
  - **Dependencies**: Zero new external dependencies (`go.mod` and `go.sum` are completely unchanged). Uses only Go stdlib (`hash/crc32` Castagnoli, `crypto/aes`, `crypto/cipher`, `crypto/subtle`, `syscall`) and `hanzoai/vfs`'s existing Apache-2.0/CC0-1.0 `internal/blake3` SIMD implementation.
  - **Standards / Patent posture**: Castagnoli CRC-32C, T10-DIF CRC-16 (`0x8BB7`), AES-256-GCM (NIST SP 800-38D), and BLAKE3 (CC0-1.0 / Apache-2.0) are unencumbered public standards. Published numbers from the FAST '26 paper belong strictly to the original authors' environments and are not cited as Hanzo claims; all figures below were measured directly on this host.

---

## 2. Matched Baselines & Reproducibility

All experiments run on real host storage (non-volatile filesystem, periodic `fdatasync`) using the reproducible benchmark runner `tools/fspi-bench/main.go`:

```bash
go run ./tools/fspi-bench \
  --sectors 4096 \
  --sync-every 256 \
  --trials 3 \
  --journal-records 1024 \
  --json docs/research/hip-1344-fspi/raw_results.json \
  --md docs/research/hip-1344-fspi/score_table.md
```

- **Host Hardware**: `Intel(R) Xeon(R) CPU @ 2.30GHz` (`linux/amd64`, 2 vCPUs, `Linux version 6.18.57`, `go1.27.1`).
- **NVMe Hardware PI Probe (`/sys/block/*/integrity`)**: `supported=false` — none of the 10 host block devices (`loop0..7`, `pmem0`, `vda`) expose NVMe/T10-DIF hardware protection information; `ModeHardwarePINVMeProbe` honestly reports unavailability and falls back cleanly to software validation.
- **Matched Configurations**:
  1. `none`: Unmodified baseline `journal` / raw 4 KiB sector I/O (no PI, no AEAD).
  2. `aead_only_baseline`: HIP-0302 AES-256-GCM cipher envelope with volume-scoped AAD (matching `hanzoai/vfs` `ledger.go`), no below-encryption sector PI.
  3. `software_crc32c`: Software-only 16-byte NVMe PI (`GuardTag`=CRC32C, `AppTag`, `RefTag`, `Generation`) validated before HIP-0302 AEAD decryption.
  4. `keyed_blake3_pi`: Cryptographic 32-byte keyed BLAKE3-128 PI bound to `(tenant, volume, file, LBA, generation)` validated before HIP-0302 AEAD decryption.
  5. `hw_nvme_pi_probe`: Linux sysfs NVMe/T10-DIF hardware PI probe with clean fallback to `keyed_blake3_pi`.

---

## 3. Falsifiable Hypothesis & Expected Failure Modes

- **Hypothesis**:
  1. Wrapping `journal.Disk` / `journal.File` with opt-in per-sector FS-PI validation (`Options.Integrity`) increases below-encryption corruption detection coverage from **71.4% (5/7)** under volume-scoped `aead_only_baseline` (and **0%** under `none`) to **100.0% (7/7)** across single-bit flip, 64-byte burst, LBA block swap, stale generation replay, cross-tenant block transplant, zeroed sector, and 2 KiB torn tail faults.
  2. Pre-plaintext-exposure rejection rate increases from **0.0% (0/7)** under `aead_only_baseline` to **100.0% (7/7)** (mechanically verified via `DecryptInvocations == 0` on corrupted reads).
  3. On-disk storage metadata overhead stays below **1.0%** (`0.489%` for 16B CRC32C PI; `0.879%` for 32B keyed BLAKE3-128 PI including 64B superheader and authenticated 4B/sector generation trailer).
- **Expected Failure Modes (Documented Honest Negative Regimes)**:
  1. **`crc_only_active_adversary_forgery`**: Unkeyed Castagnoli CRC32C (`ModeSoftwareCRC32C`) detects 100% of accidental media bitrot, swaps, replays, and torn writes, **but an active storage adversary** who modifies both ciphertext bytes and recomputes the unkeyed CRC32C PI tuple bypasses PI-layer validation (`0%` PI-layer rejection of forged CRC, leaking tampered bytes into the AEAD decryptor). `ModeKeyedBLAKE3PI` is required to close this gap (`100%` pre-decryption rejection).
  2. **`no_hw_nvme_pi_on_virtualized_host`**: Standard Linux cloud/VM disks (`vda` virtio-blk, `pmem0`, `loop*`) lack T10-DIF/NVMe metadata formatting in `/sys/block/*/integrity`. Hardware PI offload is therefore unavailable on virtualized hosts, so software validation bears the full CPU cost (`+7.44%` CPU vs AEAD for `software_crc32c`; `+42.09%` CPU vs AEAD for `keyed_blake3_pi` on this host).

---

## 4. Bounded Implementation Summary

- **`journal/fspi.go`**: Opt-in `FSPIDisk` and `FSPIFile` wrapping `journal.Disk` / `journal.File`; `PITuple`, `T10DIFTuple` + `CRC16T10DIF` (`0x8BB7`), `ModeNone`, `ModeSoftwareCRC32C`, `ModeKeyedBLAKE3PI`, `ModeHardwarePINVMeProbe`, `ModeAEADOnlyBaseline`, pooled keyed `blake3.Hasher` and single-sector buffers, atomic `FSPIStats`, and `HIP0302SectorStore` composition helper.
- **`journal/fspi_probe_linux.go` / `journal/fspi_probe_other.go`**: Real Linux `/sys/dev/block` + `/sys/block/*/integrity` hardware PI capability probe with portable non-Linux fallback.
- **`journal/fspi_fault_inject.go`**: Below-encryption fault injection harness operating directly on raw underlying disk bytes for all 7 corruption classes plus `crc_only_active_adversary_forgery`.
- **`journal/journal.go` / `journal/log.go`**: Minimal opt-in `Options.Integrity *FSPIConfig` (`nil` default = zero behavior change) and `Journal.FSPIStats()`.
- **`tools/fspi-bench/main.go`**: Reproducible real-disk benchmark & fault matrix runner.

---

## 5. Correctness, Concurrency, Security & Regression Evidence

All verification commands pass with zero data races and zero regressions:

- `go test -race -v -run '^TestFSPI' ./journal` — verifies:
  - All 7 below-encryption corruption classes across all 4 modes (100% detection & 100% pre-decryption rejection on `ModeSoftwareCRC32C` and `ModeKeyedBLAKE3PI`; exact 5/7 gap on `ModeAEADOnlyBaseline`).
  - Honest Negative Regime #1 (`TestFSPIHonestNegativeCRCActiveAdversaryForgery`): forged unkeyed CRC32C bypasses `ModeSoftwareCRC32C` PI layer while `ModeKeyedBLAKE3PI` rejects it with `ErrFSPIGuardMismatch` and `DecryptInvocations == 0`.
  - Honest Negative Regime #2 (`TestFSPIHonestNegativeHardwareNVMePIProbe`): honest sysfs probe reporting and clean software fallback.
  - Full `Journal` + `crash.Disk` lifecycle (`Append`, `Sync`, `Replay`, `Publish`, `Reclaim`, `Close`, `Reopen`), format safety (`ErrFormat` on plaintext segments), wrong-tenant rejection (`ErrFSPIAppTagMismatch`), and raw bitflip rejection during `Open`.
  - Concurrent multi-tenant isolation (`TestFSPIConcurrentMultiTenantIsolation`) across 3 tenants with concurrent writers, readers, and cross-tenant segment transplant attacks under `-race`.
  - Unaligned multi-sector RMW, monotonic generation persistence across `Close`/`Reopen`, T10-DIF CRC-16 check vector (`0xD0DB`), and real `os.Truncate` torn tail rejection (`ErrFSPITornSector`).
- `go test -short -race ./journal/...` — zero regressions across the entire existing `journal` test suite.
- `go vet ./journal/... ./tools/fspi-bench/... .` — clean.

---

## 6. Before/After Measurements, Raw Results & HIP Integration Recommendation

Full tables and raw JSON live in [`score_table.md`](./score_table.md) and [`raw_results.json`](./raw_results.json). Key 3-trial median results (4,096 × 4 KiB = 16 MiB real-disk I/O with periodic `fdatasync`):

| Mode | Detection (7 Faults) | Pre-Plaintext Rejection | Active Forgery Caught (PI Layer) | Write / Read (MB/s) | Write / Read p50 (us) | CPU (us/MB) | CPU vs AEAD | Metadata Overhead |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `none` | 0/7 (0.0%) | 0/7 (0.0%) | NO | 165.90 / 592.75 | 5.02 / 3.93 | 5,023.57 | -9.75% | 0.000% |
| `aead_only_baseline` | 5/7 (71.4%) | 0/7 (0.0%) | NO | 118.75 / 380.65 | 6.24 / 5.32 | 5,566.32 | +0.00% | 0.000% |
| `software_crc32c` | **7/7 (100.0%)** | **7/7 (100.0%)** | NO *(Neg. Regime #1)* | 129.34 / 426.13 | 6.76 / 6.13 | 5,980.69 | **+7.44%** | **0.489%** |
| `keyed_blake3_pi` | **7/7 (100.0%)** | **7/7 (100.0%)** | **YES (Pre-Decrypt)** | 114.13 / 306.17 | 10.10 / 9.13 | 7,909.38 | **+42.09%** | **0.879%** |

### HIP-1344 / HIP-1345 Integration Recommendation
1. **Accept opt-in `Options.Integrity` (`FSPIDisk` / `FSPIFile`) into `journal`**: Default `nil` incurs zero cost on existing callers while giving multi-tenant agent volumes immediate protection against LBA swaps, stale lost-write replays, and cross-tenant transplants that volume-scoped AEAD misses (`5/7 -> 7/7`, `0% -> 100%` pre-plaintext rejection).
2. **Mode selection guidance**:
   - Use **`ModeKeyedBLAKE3PI`** (default fallback for `ModeHardwarePINVMeProbe`) on multi-tenant / untrusted-storage nodes where active below-encryption tampering or replay is in scope (`< 0.9%` storage overhead, `+3.8 us` p50 per sector, 100% forgery resistance).
   - Use **`ModeSoftwareCRC32C`** only when the threat model is strictly accidental hardware/media corruption under a trusted local block stack (`0.49%` storage overhead, `+7.44%` CPU vs AEAD).
   - Keep **`ModeHardwarePINVMeProbe`** ready for bare-metal NVMe-PI hosts (e.g., T10-DIF / NVMe 2.0 64-bit Guard formatted namespaces) while cleanly falling back to software validation on cloud VMs.
