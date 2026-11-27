# HIP-1345 / HIP-1344 (#54 / #66): Lower-Copy Host-to-Guest I/O Across the Virtualization Boundary (RosenBridge)

- **Tracking Issues**: `hanzoai/vfs#54` (HIP-1345) & `hanzoai/vfs#66` (Bounty #9 under HIP-1344)
- **Canonical Owning Repository**: `hanzoai/vfs` (`internal/rosenbridge/`, `cmd/rosenbridge-bench/`, `docs/research/hip-1345-rosenbridge/`)
- **Target Integration Boundary**: `vfs -> hanzoai/s3 -> visor` (conditional lower-copy data-plane fast path layered *beside*—not replacing—the canonical gofer architecture; leaves `vfs.Volume` wire format, Hanzo S3 layout, Cloud lifecycle/billing, Enso, Zen, and Kai untouched)
- **Raw Benchmark Artifact**: [`results.json`](./results.json) (deterministic seed `1345`, `2026-11-26T15:57:46Z`)

> **Honesty & Environment Scope**: Every measured number in this dossier comes verbatim from [`results.json`](./results.json), generated on the authoring Linux host (`linux/amd64`, kernel `6.18.57`, Go `go1.27.1`, `2` vCPUs, TSC clocksource, real Linux `/proc/self/io` kernel syscall accounting enabled) using real `AF_UNIX SOCK_STREAM` socketpairs, real `mmap(2)` page-aligned per-tenant shared slabs, real OS files (`pread`/`pwrite`/`lstat`), and real post-quantum-sealed canonical `*vfs.Volume` instances (`file://` backend + journal + `vfs.Hybrid`). Published figures in the RosenBridge FAST '26 paper belong solely to the original authors' QEMU/KVM/NVMe environment; authoring-host receipts here are clean-room experimental measurements and **not** Hanzo production SLA claims until qualified on Hanzo target hardware.

---

## 1. Primary Paper Summary & License Audit

### Primary Reference
- **Paper**: *RosenBridge: A Framework for Enabling Express I/O Paths Across the Virtualization Boundary*
- **Authors**: Shi Qiu (Xiamen University), Li Wang (KylinSoft), Jianqin Yan & Ruofan Xiong (Xiamen University), Leping Yang (Shanghai Jiao Tong University), Xin Yao, Renhai Chen & Gong Zhang (Huawei), Dongsheng Li (NUDT), Jiwu Shu (Tsinghua University), Yiming Zhang (Shanghai Jiao Tong University & Xiamen University)
- **Venue**: 24th USENIX Conference on File and Storage Technologies (FAST '26), February 24–26, 2026, Santa Clara, CA, USA (ISBN `978-1-939133-53-3`)
- **URL**: https://www.usenix.org/conference/fast26/presentation/qiu (PDF: https://www.usenix.org/system/files/fast26-qiu.pdf)

### Technical Design Summary
RosenBridge observes that data-intensive workloads inside virtualized sandboxes lose substantial throughput and CPU efficiency crossing the virtualization boundary: standard paravirtualized / gofer I/O stacks pay repeated VM-exits / IPC syscalls, wire framing/marshaling allocations, and multiple intermediate kernel/userspace buffer copies. Rather than compromising host isolation by moving guest logic into the host kernel, RosenBridge:
1. Establishes an isolated host-userspace express I/O data path over shared guest-host memory with strict memory-scope bounds validation and address translation helpers.
2. Connects the fast path directly to the host storage I/O stack with batched descriptor submission, eliminating per-op IPC marshaling and intermediate copies.
3. Enforces strict multi-tenant memory access bounds and collaborative multi-path I/O fairness throttling across tenants so no single guest can read/corrupt another tenant's memory or starve peer sandboxes.

Applying this insight to `hanzoai/vfs` + `visor` under `#54` / `#66`:
- The canonical gofer architecture remains authoritative for metadata (`Stat`, `Lookup`, `Mkdir`, `Unlink`, `Rename`), small/cold I/O, and fallback under saturation.
- **Only after** runtime profiling confirms the gofer IPC boundary is a primary bottleneck (`GoferProfiler.BottleneckConfirmed() == true`) does the conditional `AdmissionGate` route eligible bulk/sequential/hot-file reads and writes onto a strictly per-tenant `mmap`-backed shared ring + page-aligned slab pool (`RosenBridgeChannel`).

### License, Dependency & Patent Audit

| Item | Status | Notes |
|---|---|---|
| Upstream RosenBridge / uBPF code imported | **None (0 lines)** | 100% clean-room Go implementation in `internal/rosenbridge/` |
| License of this submission | **Apache-2.0 / MIT** | Matches `hanzoai/vfs` repository license |
| New external Go dependencies | **None (`go.mod` / `go.sum` untouched)** | `internal/rosenbridge/` uses Go standard library + canonical `github.com/hanzoai/vfs` only (`syscall.Mmap`/`Socketpair`, `sync/atomic`, `crypto/rand`, `hash/crc32`, `os`, `path/filepath`) |
| Canonical VFS interop verified | **Yes** | `internal/rosenbridge/gofer_integration_test.go` and `cmd/rosenbridge-bench` execute both canonical Gofer and conditional RosenBridge directly against real `*vfs.Volume` + local journal + `file://` backend + post-quantum `vfs.Hybrid` cipher |
| Known patent encumbrances | **None identified** | Standard OS/POSIX primitives (`mmap`, `AF_UNIX` socketpair, power-of-two atomic ring buffer, unsigned bounds checks, Castagnoli CRC32C, `lstat` path confinement); no proprietary algorithms copied |

---

## 2. Matched Baselines & Reproducible Commands

### Exact Reproducible Commands
```bash
# 1. Static analysis & formatting verification:
gofmt -l ./internal/rosenbridge ./cmd/rosenbridge-bench
go vet ./internal/rosenbridge/... ./cmd/rosenbridge-bench/...

# 2. Full correctness, isolation, descriptor bounds, symlink confinement,
#    fallback, canonical *vfs.Volume parity, and -race concurrency tests:
go test -race -count=1 -v ./internal/rosenbridge/...

# 3. Deterministic OS-backed + canonical *vfs.Volume + negative-regime
#    benchmark harness (writes raw JSON receipts):
go run ./cmd/rosenbridge-bench -seed 1345 \
  -out docs/research/hip-1345-rosenbridge/results.json
```

### Matched Baselines Evaluated on Identical Storage
1. **Canonical Gofer Baseline (`gofer_baseline`)**: Real host/guest IPC over an OS `AF_UNIX SOCK_STREAM` socketpair (`GoferTransport`), binary wire framing/marshaling, kernel socket buffer copies, and host `BackingStore` execution (`OSBackingStore` or canonical `*vfs.Volume`). Runs on the exact same pre-seeded 16 MiB OS file and 4 MiB sealed `*vfs.Volume` as RosenBridge.
2. **RosenBridge Conditional Fast Path — POSIX Slice API (`rosenbridge_slice`)**: Drop-in `ReadAt`/`WriteAt` via `Router` + `AdmissionGate` + per-tenant page-aligned `mmap` `SlabPool` + atomic `Ring` (2 data copies/op: backing <-> mmap slab <-> caller slice; **0** socketpair syscalls).
3. **RosenBridge Conditional Fast Path — 1-Copy Shared-Slab API (`rosenbridge_slab_1copy`)**: Direct `ReadSlab`/`WriteSlab` where guest consumes/produces bytes directly in its tenant-private page-aligned `mmap` slab slice (**1** data copy/op: backing <-> mmap slab; **0** socketpair syscalls; **0** payload heap copies).
4. **Negative Controls**:
   - `rosenbridge_gated_fallback` vs `ungated_fastpath_negative_control` on an 80% `Stat` + 20% tiny 64B random read workload across 20 short-lived bursts.
   - `negative_ring_saturated_fallback` on a saturated 2-slot tenant ring proving zero-error automatic degradation to canonical gofer.

---

## 3. Falsifiable Hypothesis & Expected Failure Modes

### Hypotheses
- **H1 (Gofer Bottleneck Verification Gate)**: Before enabling RosenBridge, profiling real 64 KiB sequential gofer reads over `AF_UNIX` will confirm boundary overhead `>= 25%` of total op latency, `>= 3` data copies/op, and `>= 6` syscalls/op. Prior to confirmation (`< MinProfileSamples`), `AdmissionGate` MUST keep **100%** of ops on canonical gofer (`FallbackProfilerNotTriggered`).
- **H2 (Syscall, Copy, Allocation & Runtime Reduction on Bulk I/O)**: Once confirmed, RosenBridge on bulk sequential OS-backed reads/writes (64 KiB and 256 KiB) and canonical `*vfs.Volume` reads (64 KiB) will reduce:
  - Real kernel `/proc/self/io` syscalls/op by `>= 80%` (`7–8/op -> 1.0/op` on OS files; `6.0/op -> ~0.0/op` on cached `*vfs.Volume`),
  - Data copies/op from `4.0` to `2.0` (POSIX slice) and `1.0` (shared slab),
  - Per-op heap allocations by `>= 95%`, and
  - Total task runtime by `>= 1.5x` (OS-backed) and `>= 2.5x` (cached `*vfs.Volume`).
- **H3 (Zero Isolation Regression / Fail-Closed Security)**: Strict per-tenant `SlabPool` ownership, cryptographic per-tenant session tokens (`crypto/rand`), unsigned overflow-safe `ValidateDescriptor`, active slab range overlap detection, and segment-by-segment `os.Lstat` symlink confinement (`ConfinePath`) will reject **100%** (`7/7`) of cross-tenant, forged-token, overflow, overlap, truncation, and symlink-escape attacks with zero FD/mmap leaks (`0/0` after `Close`).
- **H4 (Falsifiable Negative Regimes)**:
  1. *Tiny 64B + metadata (`Stat`-heavy) short-lived bursts*: Eagerly provisioning a per-tenant 2 MiB `mmap` slab without `AdmissionGate` will degrade runtime (worse than canonical gofer) and waste host memory. With `AdmissionGate`, **100%** of `Stat` (`FallbackUnsupportedOp`) and 64B reads (`FallbackSmallIO`) remain on canonical gofer with `0` fast-path ops and `0` slab RAM pinned.
  2. *Ring saturation / tenant fairness cap*: When all tenant slab/ring slots are occupied or `MaxTenantInFlightBytes` is reached, `Router` MUST transparently degrade to canonical gofer (`FallbackRingFull` / `FallbackThrottled`) with `0` errors and byte-for-byte data fidelity.

---

## 4. Bounded Implementation Deliverable & Integration Architecture

All code is self-contained inside `internal/rosenbridge/` and `cmd/rosenbridge-bench/` (zero changes to `go.mod`, `go.sum`, or existing `vfs` packages):

```text
Guest Sandbox / Visor Client
       │
       ▼
  rosenbridge.Router
       │
       ├──► AdmissionGate (GoferProfiler confirmed? size >= 4KiB? seq/hot? quota?)
       │        │
       │        ├── NOT admitted / Stat / tiny / cold / ring full / throttled
       │        │        ▼
       │        │   Canonical GoferTransport (AF_UNIX SOCK_STREAM RPC + wire framing)
       │        │        │
       │        └── Admitted (bulk/sequential/hot Read/Write)
       │                 ▼
       │            Per-Tenant RosenBridgeChannel (isolated TenantID + crypto/rand token)
       │              • Atomic cache-line-padded power-of-two Ring (coalesced doorbell)
       │              • Page-aligned MAP_SHARED mmap SlabPool (overlap guard, zero-on-close)
       │              • Fail-closed ValidateDescriptor (overflow/truncation/token/path checks)
       │                 │
       └─────────────────┴──► Shared BackingStore (OSBackingStore | canonical *vfs.Volume)
```

| File | Responsibility |
|---|---|
| `internal/rosenbridge/errors.go` | Typed sentinel errors, `IsSecurityViolation` fail-closed classifier, and POSIX `Errno` mapping |
| `internal/rosenbridge/confinement.go` | `ValidateTenantID`, `NormalizeVirtualPath`, and `os.Lstat` symlink-safe `ConfinePath` |
| `internal/rosenbridge/slab_unix.go` / `slab_other.go` | Per-tenant 4 KiB page-aligned `MAP_SHARED` `mmap(2)` `SlabPool`, overlap detection, zero-on-close, FD/mmap leak counters |
| `internal/rosenbridge/ring.go` | Cache-line-padded atomic `head`/`tail` power-of-two `Ring` and overflow-safe `ValidateDescriptor` |
| `internal/rosenbridge/gate.go` | `GoferProfiler` (syscall/copy/latency/overhead ratio) and conditional `AdmissionGate` with per-tenant fairness throttling |
| `internal/rosenbridge/backing.go` | `BackingStore` interface + confined `OSBackingStore` + canonical per-tenant `*vfs.Volume` `VFSBackingStore` |
| `internal/rosenbridge/socketpair_unix.go` / `socketpair_other.go` | Real OS `AF_UNIX SOCK_STREAM` socketpair (with non-Unix `os.Pipe` fallback) |
| `internal/rosenbridge/transport.go` | `GoferTransport`, per-tenant `RosenBridgeChannel` (`ReadAt`/`WriteAt`/`ReadSlab`/`WriteSlab`), and conditional `Router` |
| `internal/rosenbridge/rosenbridge_test.go` | 9 top-level unit/security/isolation/fallback/concurrency tests (20 total with subtests) under `-race` |
| `internal/rosenbridge/gofer_integration_test.go` | End-to-end parity, durability (`Flush` + `vfs.Open`), symlink rejection, and multi-tenant isolation test on canonical `*vfs.Volume` |
| `cmd/rosenbridge-bench/main.go` | Reproducible benchmark CLI with real `/proc/self/io` syscall deltas, `runtime.MemStats`, negative regimes, and 7-point isolation suite |

---

## 5. Correctness, Concurrency, Security & Isolation Verification Matrix

### Test & Static Analysis Verification
- `go vet ./internal/rosenbridge/... ./cmd/rosenbridge-bench/...`: **0 warnings / 0 errors**.
- `go test -race -count=1 -v ./internal/rosenbridge/...`: **10/10 top-level tests (21/21 including subtests) PASS** in `2.461s` with **0 data races** and **0 leaked FDs/mmaps**.
- `go run ./cmd/rosenbridge-bench`: **7/7 isolation checks PASS (100.0%)**.

### Verified Isolation & Security Matrix (verbatim from [`results.json`](./results.json))

| Check / Attack Vector | Enforcement Mechanism | Expected Error (`Errno`) | Observed Outcome (`results.json`) | Status |
|---|---|---|---|---|
| **Cross-tenant slab access** | Per-tenant `SlabPool` verifies `(tenantID, tenantToken)` on every `AcquireSlot`/`ReserveRange` | `ErrTenantMismatch` (`EACCES`) | `slab owned by "tenant-a", requested by "tenant-b"` | **PASS** |
| **Forged tenant token** | 64-bit `crypto/rand` per-channel session token checked in `ValidateDescriptor` | `ErrTenantMismatch` (`EACCES`) | `descriptor tenant "tenant-a" does not match channel owner "tenant-a"` | **PASS** |
| **Descriptor `uint64` overflow** | Unsigned subtraction check `offset > slabSize - length` & `fileOff > MaxFileSize - length` | `ErrDescriptorOutOfBounds` (`EOVERFLOW`) | `slab [18446744073709551103, +4096) exceeds slab size 524288` | **PASS** |
| **Overlapping in-flight slab range** | Active span tracker in `SlabPool.ReserveRange` rejects any overlapping active descriptor | `ErrDescriptorOverlap` (`EOVERFLOW`) | `[4096, 12288) overlaps active descriptor` | **PASS** |
| **Truncated / mismatched descriptor** | `DeclaredLen != Length` or `Length == 0` rejected fail-closed | `ErrDescriptorTruncated` (`EINVAL`) | `length=4096 declared=2048` | **PASS** |
| **Symlink & `..` traversal escape** | Segment-by-segment `os.Lstat` (`ConfinePath`) + post-open symlink swap re-check + VFS symlink check | `ErrPathEscape` (`EACCES`) | Both leaf symlink and `../tenant-a/data.bin` rejected on fast path & gofer | **PASS** |
| **Zero leaked FDs & `mmap`s** | `SlabPool.Close()` zeroes memory, calls `munmap(2)` + `f.Close()`; `OSBackingStore.Close()` closes all cached FDs | `(mmap:0, fd:0)`, `storeFDs=0` | `slabA=(mmap:0,fd:0) slabB=(mmap:0,fd:0) storeFDs=0` | **PASS** |

---

## 6. Measurements, Negative Regimes & Canonical Rollout Plan

### 6.1 Step 0: Gofer Bottleneck Profiler Receipt (64 × 64 KiB Canonical Gofer Reads)

| Metric | Measured Value (`results.json`) | Gate Threshold | Status |
|---|---:|---:|---|
| Samples collected | `64` (`4 MiB`) | `>= 64` | Met |
| Gofer boundary overhead ratio (`boundary / total`) | **`55.0%`** (`0.5503`) | `>= 25.0%` | **Confirmed** |
| Average syscalls / op | **`7.02`** | — | High IPC syscall overhead |
| Average data copies / op | **`4.00`** | `>= 3.00` | **Confirmed** |
| Average heap alloc / op | **`131,206 B`** | — | Per-op framing/recv allocs |
| Latency p50 / p95 / p99 | `203.70 µs` / `451.85 µs` / `486.23 µs` | — | Baseline recorded |
| `BottleneckConfirmed` | **`true`** | required before activation | **Gate Armed** |

### 6.2 Bulk I/O Performance: Canonical Gofer vs Conditional RosenBridge (verbatim from [`results.json`](./results.json))

| Workload | Mode | Runtime (ms) | Speedup | Throughput (MB/s) | IOPS | p50 (µs) | p99 (µs) | Inst. SC/op | Kernel `/proc/self/io` SC/op | Copies/op | Heap Alloc/op (B) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **`os_seq_read_64k`** (64 MiB, 1024 ops) | `gofer_baseline` | 129.814 | 1.00x | 493.01 | 7,888 | 73.87 | 636.94 | 7.00 | **7.00** | 4.00 | 142,708 |
| | `rosenbridge_slice` | 77.798 | **1.67x** | 822.64 | 13,162 | 59.35 | 312.05 | **1.00** | **1.00** (-85.7%) | **2.00** | **3,074** (-97.8%) |
| | `rosenbridge_slab_1copy` | **70.312** | **1.85x** | **910.23** | **14,564** | **38.59** | **245.85** | **1.00** | **1.00** (-85.7%) | **1.00** (-75%) | **3,146** (-97.8%) |
| **`os_seq_read_256k`** (64 MiB, 256 ops) | `gofer_baseline` | 126.566 | 1.00x | 505.66 | 2,023 | 372.21 | 2,368.45 | 7.00 | **8.01** | 4.00 | 536,840 |
| | `rosenbridge_slice` | 34.600 | **3.66x** | 1,849.69 | 7,399 | 129.38 | 316.59 | **1.00** | **1.01** (-87.4%) | **2.00** | **4,066** (-99.2%) |
| | `rosenbridge_slab_1copy` | **29.087** | **4.35x** | **2,200.29** | **8,801** | **82.29** | **290.84** | **1.00** | **1.01** (-87.4%) | **1.00** (-75%) | **4,137** (-99.2%) |
| **`os_seq_write_64k`** (32 MiB, 512 ops) | `gofer_baseline` | 76.930 | 1.00x | 415.96 | 6,655 | 93.01 | 672.96 | 7.00 | **7.00** | 4.00 | 142,498 |
| | `rosenbridge_slice` | **45.387** | **1.69x** | **705.05** | **11,281** | **52.96** | 267.47 | **1.00** | **1.00** (-85.7%) | **2.00** | **2,980** (-97.9%) |
| | `rosenbridge_slab_1copy` | 49.595 | **1.55x** | 645.23 | 10,324 | 101.40 | **255.66** | **1.00** | **1.00** (-85.7%) | **1.00** (-75%) | **2,980** (-97.9%) |
| **`canonical_vfs_read_64k`** (64 MiB, 1024 ops, `*vfs.Volume`) | `gofer_baseline` | 79.204 | 1.00x | 808.04 | 12,929 | 51.61 | 453.68 | 7.00 | **6.00** | 4.00 | 140,089 |
| | `rosenbridge_slice` | 21.431 | **3.70x** | 2,986.30 | 47,781 | 10.02 | 117.11 | **1.00** | **0.00** (-99.9%) | **2.00** | **446** (-99.7%) |
| | `rosenbridge_slab_1copy` | **16.256** | **4.87x** | **3,936.92** | **62,991** | **8.15** | **100.90** | **1.00** | **0.00** (-99.9%) | **1.00** (-75%) | **522** (-99.6%) |

> **Key Observation on Real Kernel Accounting**: `/proc/self/io` `rchar` + `wchar` confirms the exact data-copy elimination: on `os_seq_read_64k` (64 MiB payload), `gofer_baseline` moved `134.3 MB` `rchar` + `67.2 MB` `wchar` through kernel read/write syscalls (`201.5 MB` kernel byte traffic, 3x payload amplification), whereas `rosenbridge_slab_1copy` moved exactly `67.1 MB` `rchar` + `0 B` `wchar` (1x payload, zero socket amplification). On `os_seq_write_64k`, `rosenbridge_slice` (`45.39 ms`) and `rosenbridge_slab_1copy` (`49.60 ms`) are both ~1.6–1.7x faster than gofer (`76.93 ms`), with the difference between slice and slab on 64 KiB writes falling within 2-vCPU dirty-page writeback jitter because host `pwrite` dominates once socket IPC is eliminated.

### 6.3 Negative Regimes (Why RosenBridge MUST Remain Conditional)

| Workload | Mode | Runtime (ms) | IOPS | Fast-Path Ops | Gofer Ops | Fallback Counts | Conclusion |
|---|---|---:|---:|---:|---:|---|---|
| **`negative_tiny_64b_and_stat`** (2000 ops: 80% `Stat` + 20% 64B random read across 20 bursts) | `gofer_baseline` | **113.856** | **17,566** | 0 | 2,000 | — | Baseline for metadata-heavy sandboxes |
| | `rosenbridge_gated_fallback` | 144.849 | 13,808 | **0** | **2,000** | `small_io=400`, `unsupported_op=1600` | Gate keeps 100% on gofer; **0 MiB** slab RAM pinned (small gate check overhead) |
| | `ungated_fastpath_negative_control` | **174.308** (**1.53x slower**) | **11,474** | 400 | 1,600 | `unsupported_op=1600` | **Negative result**: eager 2 MiB `mmap` slab setup/teardown loses **53%** on runtime and wastes 40 MiB cumulative slab mappings for 25.6 KiB of data |
| **`negative_ring_saturated_fallback`** (200 × 16 KiB reads on saturated 2-slot ring) | `rosenbridge_auto_fallback` | 14.313 | 13,973 | 2 (held) | 200 | `ring_full=200` | **0 errors**, 100% graceful fallback to canonical gofer |

### 6.4 Limitations, Integration Recommendation & Rollout / Rollback Plan

- **Limitations**:
  1. This clean-room experiment measures the host-userspace virtualization boundary using real Linux `AF_UNIX` socketpairs and `MAP_SHARED` `mmap(2)` slabs rather than KVM VM-exits or `virtio-ndp`/uBPF bytecode injection (which require privileged KVM/QEMU hypervisor hooks). On real KVM/Visor VMs where each gofer RPC also incurs VM-exit/VM-entry cost, the relative latency savings of eliminating 6 IPC syscalls per op are expected to be at least as large, but must be re-measured on Hanzo Visor hardware before production claims.
  2. `OSBackingStore` currently re-runs `ConfinePath` (`filepath.EvalSymlinks` + `os.Lstat`) on cached FD reuse to defend against post-open symlink swaps (~3 KiB/op heap alloc in Go stdlib path ops); on Linux `>= 5.6`, Visor production integration should use `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)` to keep the same confinement guarantee with zero path allocations.
- **Integration Recommendation (`HIP-1345` / `HIP-1344`)**:
  - **Adopt as an opt-in, profiler-gated conditional fast path** beside the Visor gofer (`vfs -> hanzoai/s3 -> visor`), never as a gofer replacement.
  - Keep `RequireProfilerConfirmation: true`, `MinTransferBytes: 4096`, `BulkBypassBytes: 16384`, and `MaxTenantInFlightBytes` enabled by default so metadata-heavy and tiny-I/O agent sandboxes never pay slab memory or setup overhead.
- **Rollout & Rollback Plan**:
  - **Rollout**: Gate activation behind runtime flag `GateConfig.Enabled` + `GoferProfiler` confirmation, starting with read-heavy bulk sandboxes (model weight / dataset streaming over `*vfs.Volume`), monitoring `RouterStats` fallback ratios and zero-leak resource gauges.
  - **Instant Rollback**: Call `router.Gate().SetEnabled(false)` (lock-free `atomic.Bool` flip) to route 100% of subsequent I/O back to canonical `GoferTransport` with zero downtime, zero mount teardown, and zero storage-format migration; reverting the PR requires only removing `internal/rosenbridge/`, `cmd/rosenbridge-bench/`, and `docs/research/hip-1345-rosenbridge/`.
