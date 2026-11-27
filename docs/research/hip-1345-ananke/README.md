# HIP-1345 / HIP-1344 (#53 / #65): Transparent Recovery of a Crashed FUSE Daemon (Ananke)

- **Tracking Issues**: `hanzoai/vfs#53` (HIP-1345) & `hanzoai/vfs#65` (Bounty #8 under HIP-1344)
- **Canonical Owning Repository**: `hanzoai/vfs` (`internal/fuserecovery/`, `cmd/ananke-bench/`, `docs/research/hip-1345-ananke/`)
- **Target Integration Boundary**: `vfs -> hanzoai/s3 -> visor` (recovers the host-side `pkg/mount` FUSE daemon process without unmounting the guest sandbox or altering `vfs.Volume` storage format, S3 layout, Cloud billing, Enso, Zen, or Kai)
- **Raw Benchmark Artifact**: [`results.json`](./results.json) (deterministic seed `1345`)

> **Honesty & Environment Scope**: All measured tables in this dossier come verbatim from [`results.json`](./results.json), generated on the authoring Linux host (`linux/amd64`, Go `go1.27.1`, `2` vCPUs) using real OS-backed files, real `fsync(2)` calls, and an in-process supervisor `/dev/fuse` channel harness (no privileged kernel `/dev/fuse` mount required). Published figures in the Ananke FAST '25 paper belong solely to the original authors' environment; authoring-host figures here are clean-room experimental receipts and **not** Hanzo production SLA claims until qualified on Hanzo target hardware.

---

## 1. Primary Paper Summary & License Audit

### Primary Reference
- **Paper**: *Fast, Transparent Filesystem Microkernel Recovery with Ananke* (Best Paper Award, USENIX FAST '25)
- **Authors**: Jing Liu (Microsoft Research), Yifan Dai, Andrea C. Arpaci-Dusseau, and Remzi H. Arpaci-Dusseau (University of Wisconsin–Madison)
- **Venue**: 23rd USENIX Conference on File and Storage Technologies (FAST '25), February 2025
- **URL**: https://www.usenix.org/conference/fast25/presentation/liu-jing

### Technical Design Summary
Ananke demonstrates that a userspace filesystem service crash (process-only failure such as `SIGKILL`, OOM-kill, or panic) is fundamentally different from a full host/kernel crash: the host OS kernel, client processes, and kernel-side VFS/mount handles remain alive. Instead of tearing down the mount (`ENOTCONN`) and forcing a cold remount or guest sandbox reboot, Ananke:
1. Decouples the mount/IPC transport lifetime from the daemon worker process via a restart-resilient supervisor holding the channel/device descriptor.
2. Persists minimal volatile session state (session ID, generation, inode lookup table & `nlookup` refcounts, open file handle table, request deduplication watermarks, and `fsync` barriers) outside the worker's address space.
3. On worker termination, respawns a fresh worker, re-attaches the live channel without unmounting, re-validates inode generations and open handles (including unlinked-but-open orphan files), and replays blocked/retried requests idempotently.

This is explicitly distinct from **storage journal replay** (`vfs#9` / `journal/`): journal replay restores bytes in a `vfs.Volume` after a cold attach, whereas Ananke recovers the **live FUSE filesystem service** so guest processes (agents, SQLite WAL writers, compilers) keep their open file descriptors without seeing `ENOTCONN`.

### License, Dependency & Patent Audit
| Item | Status | Notes |
|---|---|---|
| Upstream Ananke code imported | **None (0 lines)** | 100% clean-room Go implementation in `internal/fuserecovery/` |
| License of this submission | **Apache-2.0 / MIT** | Matches `hanzoai/vfs` repository license |
| New external Go dependencies | **None (`go.mod` / `go.sum` untouched)** | `internal/fuserecovery/` uses Go standard library only (`hash/crc32`, `encoding/binary`, `encoding/json`, `os`, `path/filepath`, `sync`, `syscall`, etc.) |
| Canonical VFS interop verified | **Yes** | `internal/fuserecovery/vfs_integration_test.go` tests recovery directly over canonical `*vfs.Volume` + local journal (`pkg/backend/file` + `age.Hybrid`) |
| Known patent encumbrances | **None identified** | Standard OS/POSIX primitives (CRC32C, atomic rename, fd inheritance, generation counters, idempotency watermarks); no proprietary algorithms copied |

---

## 2. Matched Baselines & Reproducible Commands

### Exact Reproducible Commands
```bash
# 1. Run full correctness, concurrency (-race), security, orphan, ESTALE,
#    multi-tenant, negative-regime, and canonical vfs.Volume integration tests:
go test -race -v ./internal/fuserecovery/...

# 2. Static analysis:
go vet ./internal/fuserecovery/... ./cmd/ananke-bench/...

# 3. Run deterministic crash-injection and steady-state benchmark harness:
go run ./cmd/ananke-bench -seed 1345 -crashes 150 -mttr-iters 30 \
  -out docs/research/hip-1345-ananke/results.json
```

### Matched Baselines Evaluated on Identical Backing Directories
1. **Matched Cold-Remount Baseline (`CheckpointDisabled` + `ColdRemountRecover`)**: Mirrors current `pkg/mount/fuse_unix.go` + `pkg/mount/stop.go` behavior on daemon crash: `/dev/fuse` closes, mount unmounts (`UnmountCount++`), all open handles return `ENOTCONN` (`0%` handle survival), all unlinked-open orphan files are lost (`0%` orphan survival), and the replacement daemon must walk/re-index the backing tree while applications reopen non-orphan paths from scratch.
2. **Ananke Process-Crash Mode (`CheckpointBuffered`)**: Supervisor retains the `/dev/fuse` channel across `SIGKILL`. User file data is durably flushed via real `BackingFile.Sync()` on every `Fsync()`; `SessionState` (CRC32C-framed) is atomically written + renamed in host OS page cache (which survives 100% of process-only `SIGKILL` crashes without per-op state-dir `fsync`).
3. **Ananke Strict Disk-Sync Mode (`CheckpointSync`)**: Same transparent `/dev/fuse` handoff, plus `f.Sync()` + atomic `os.Rename()` + `SyncDir()` on every metadata state checkpoint (quantifies the upper-bound durability cost and exposes the high-churn metadata negative regime).

---

## 3. Falsifiable Hypothesis & Expected Failure Modes

### Hypothesis
- **H1 (Transparent Handle Continuity)**: Holding the `/dev/fuse` transport channel in a supervisor and persisting CRC32C-verified session/inode/handle state enables **100.0%** survival of open regular, append, and unlinked-open (orphan) handles across abrupt `SIGKILL` daemon crashes with **0** unmounts (vs **0.0%** survival and `1` unmount/crash on cold remount).
- **H2 (Zero Loss of Acknowledged `fsync`s)**: Across `150` injected abrupt process crashes with concurrent un-fsynced dirty writes staged immediately prior to crash, **100.0%** (`150/150`, `lost = 0`) of locally acknowledged `fsync()` barriers survive byte-for-byte with verified Castagnoli CRC32C, while unacknowledged dirty tail writes are cleanly dropped without corrupting the durable prefix.
- **H3 (Security & Isolation Continuity)**: Symlink confinement (`ConfinePath` / `ValidateStateDir`) rejects path/symlink escapes (`ErrPathEscape`), deleted/mismatched inodes fail deterministically with `ESTALE`, and multi-tenant handle ownership (`OwnerTenant`) rejects cross-tenant access (`EACCES`) both before and after concurrent restarts.
- **H4 (Falsifiable Performance Trade-off / Negative Regimes)**:
  1. *High-churn tiny-open negative regime*: Full-snapshot state checkpointing on every `Open`/`Release` without `fsync` will severely degrade throughput (`>95%` overhead in `sync` mode), proving that production Visor integration must use shared-memory (`memfd`/mmap) or incremental logging for non-fsync metadata transitions.
  2. *Host/kernel crash & disabled-handoff negative regimes*: When the host kernel crashes or `/dev/fuse` fd handoff is unavailable, transparent recovery MUST fail cleanly with `ErrHandoffUnavailable` and increment explicit degraded-fallback telemetry rather than silently corrupting state.

---

## 4. Bounded Implementation Deliverable

All implementation code is confined to two new packages (zero changes to `go.mod`, `go.sum`, or existing volume wire formats):

| File | Responsibility |
|---|---|
| `internal/fuserecovery/errors.go` | Typed sentinel errors + POSIX `Errno(err)` mapping (`ESTALE`, `EBADF`, `EACCES`, `ENOENT`, `EEXIST`, `ENOTCONN`) |
| `internal/fuserecovery/confinement.go` | Symlink-safe `ConfinePath`, `ValidateStateDir`, and `SyncDir` preventing traversal/symlink escape |
| `internal/fuserecovery/state.go` | `SessionState`, `InodeRecord`, `HandleRecord`, `FsyncBarrier`, bounded `OpReceipt` deduplication window, and `StateStore` (24-byte `ANANKES1` header + Castagnoli CRC32C + atomic temp-file rename + optional `fsync`/`SyncDir`) |
| `internal/fuserecovery/transport.go` | `FuseChannel` & `SupervisorChannel` modeling supervisor-held `/dev/fuse` fd, mount continuity, queued blocked requests, kernel crash, and disabled-handoff degraded fallback |
| `internal/fuserecovery/backing.go` | `BackingStore` interface + `OSBackingStore` (real OS files, real `fsync`, confined `.ananke-orphans/` stash for unlinked-open files) |
| `internal/fuserecovery/daemon.go` | `Daemon` + O(open handles) `RecoverDaemon`: volatile write buffer vs `Fsync` barriers, idempotent `reqID` dedup, orphan persistence & cleanup, stale-inode `ESTALE`, multi-tenant `EACCES`, zero-leak FD tracking |
| `internal/fuserecovery/supervisor.go` | `Supervisor` lifecycle (`Start`, `InjectProcessCrash`, `Recover`, `ColdRemountRecover`, `InjectKernelCrash`, `DisableHandoff`, `Telemetry`) |
| `internal/fuserecovery/recovery_test.go` | 9 test functions (10 subtests) under `-race` covering CRC corruption, symlink escapes, R/W + dirty drops, orphans, `ESTALE`, append/create idempotency, 120-crash zero-loss fsync, concurrent multi-tenant isolation, and negative regimes |
| `internal/fuserecovery/vfs_integration_test.go` | End-to-end test adapting canonical `*vfs.Volume` (local journal + `file://` backend + post-quantum `age.Hybrid` cipher) across Round 1 -> `SIGKILL` -> Round 2 transparent recovery |
| `cmd/ananke-bench/main.go` | Reproducible 5-experiment benchmark & crash-injection CLI writing `results.json` |

---

## 5. Correctness, Concurrency, Security & Open-Handle Matrix

### Verification Evidence
- `go test -race -v ./internal/fuserecovery/...`: **10/10 top-level tests (11/11 including subtests) PASS** in `4.855s` with **0 data races** and **0 leaked file descriptors**.
- `go vet ./internal/fuserecovery/... ./cmd/ananke-bench/...`: **0 warnings / 0 errors**.

### Documented & Verified Open-Handle Behavior Matrix (from `results.json`)

| Scenario | Semantics | Expected Error / Outcome | Observed (`results.json`) | Pass |
|---|---|---|---|---|
| **Regular R/W open handle** | Re-bound via `SessionState.Handles` + `Inodes` across `SIGKILL`; 0 unmounts | Survives restart; reads acknowledged bytes | `read="reg-v1" err=<nil>` | PASS |
| **`O_APPEND \| O_SYNC` retried `reqID`** | `CompletedOps[reqID]` replays recorded `OpReceipt` instead of double-appending | Deduplicated (`dedup=true`), exact single append | `dedup=true content="A1\n" err=<nil>` | PASS |
| **Unlinked-but-open (orphan) file** | `Unlink()` atomically moves file to confined `.ananke-orphans/`; lookup returns `ENOENT`, open `fh` survives crash; final `Release()` deletes orphan | Readable across crash; cleaned up on final `Release()` | `read="orphan-bytes" release_err=<nil>` | PASS |
| **Stale / externally deleted inode** | Generation mismatch or missing backing object marks inode deleted; handle retained without FD | Returns `ErrStaleInode` (`syscall.ESTALE`) | `err=fuserecovery: stale inode errno=stale file handle` | PASS |
| **Unacknowledged dirty writes prior to `fsync`** | Held only in daemon volatile `pendingWrites`; discarded on `SIGKILL` (standard POSIX) | Dropped cleanly; prior acknowledged `fsync` prefix intact | `150/150` dropped; `150/150` acknowledged intact | PASS |
| **Cross-tenant handle/inode access** | `OwnerTenant` verified on every op and persisted in CRC32C state snapshot | Returns `ErrTenantMismatch` (`syscall.EACCES`) | `err=fuserecovery: tenant isolation violation errno=permission denied` | PASS |
| **State file corruption (bit-flip / truncation)** | Castagnoli CRC32C over 20-byte header + payload rejects any mutation | Returns `ErrSessionCorrupt` | Verified in `TestStateStoreCRC32CAndCorruptionDetection` | PASS |
| **Symlink / `..` path escape** | `ConfinePath` & `ValidateStateDir` check every segment with `os.Lstat` | Returns `ErrPathEscape` (`syscall.EACCES`) | Verified in `TestSymlinkConfinementRejectsEscape` | PASS |

---

## 6. Measurements, Negative Regimes & Canonical Visor/VFS Integration Plan

### 6.1 MTTR & Continuity Comparison (`500` files, `20` open handles, `4` orphans, `30` iterations)

| Recovery Path | MTTR p50 (ms) | MTTR p95 (ms) | MTTR p99 (ms) | Open Handle Survival | Orphan Survival | Unmounts (30 crashes) |
|---|---:|---:|---:|---:|---:|---:|
| **Ananke Transparent (`buffered`)** | **6.447** | **10.577** | **12.261** | **100.0%** | **100.0%** | **0** |
| **Ananke Transparent (`sync`)** | 18.881 | 25.084 | 25.950 | **100.0%** | **100.0%** | **0** |
| Cold Remount Baseline (service only) | 2.804 | 4.926 | 11.232 | 0.0% | 0.0% | 30 |
| Cold Remount Baseline (service + app reopen) | 3.115 | 5.712 | 7.557 | 0.0% | 0.0% | 30 |

*Interpretation*: In this local in-process temp-dir harness (where cold remount has zero kernel `fusermount3` teardown or guest VM reboot latency — unlike production `pkg/mount/stop.go` with `busyGrace=5s` + `exitGrace=3s`), Ananke `buffered` recovers in `6.45 ms` p50 (`12.26 ms` p99) while delivering **100.0% open-handle and 100.0% orphan survival with 0 unmounts**, compared to **0.0% handle/orphan survival and 30 forced unmounts** on the cold-remount baseline.

### 6.2 Acknowledged `fsync` Durability Across `150` Injected Process Crashes

| Metric | Measured Value (`results.json`) |
|---|---:|
| Injected `SIGKILL`-equivalent crashes | `150` |
| Active open files | `4` |
| Locally acknowledged `fsync()` calls | `150` |
| Verified intact post-recovery (byte-for-byte & CRC32C) | **`150` (`100.0%`)** |
| Acknowledged `fsync()` calls lost | **`0`** |
| Unflushed dirty tail writes staged & cleanly dropped | `150 / 150` |
| Mountpoint unmounts | **`0`** |
| Leaked file descriptors after test | **`0`** |

### 6.3 Steady-State Checkpoint Overhead & High-Churn Negative Regime

#### Workload A: Balanced Create + 4×4 KiB Write + `fsync` + Read + Close (`100` ops)
| Checkpoint Mode | Throughput (ops/s) | Latency p50 (ms) | Latency p99 (ms) | Overhead vs Baseline | Checkpoint Saves | Bytes Written |
|---|---:|---:|---:|---:|---:|---:|
| `disabled` (baseline) | 103.55 | 8.483 | 20.135 | 0.00% | 0 | 0 |
| `buffered` (Ananke process-crash) | 55.23 | 17.818 | 28.416 | **+46.66%** | 301 | 14,869,209 |
| `sync` (strict per-op disk sync) | 25.60 | 38.230 | 48.779 | **+75.28%** | 301 | 14,868,005 |

#### Workload B (Negative Regime): High-Churn Tiny Open + Read + Close Without `fsync` (`150` ops)
| Checkpoint Mode | Throughput (ops/s) | Latency p50 (ms) | Latency p99 (ms) | Overhead vs Baseline | Checkpoint Saves | Bytes Written |
|---|---:|---:|---:|---:|---:|---:|
| `disabled` (baseline) | 22,546.22 | 0.018 | 1.769 | 0.00% | 0 | 0 |
| `buffered` (full-JSON atomic rename) | 781.78 | 1.010 | 6.762 | **+96.53%** | 331 | 1,888,911 |
| `sync` (per-op `fsync` + `SyncDir`) | 50.79 | 18.769 | 31.420 | **+99.77%** | 331 | 1,887,587 |

### 6.4 Negative Regimes Verified
1. **High-churn tiny-open workload (above)**: Whole-snapshot JSON serialization + atomic file creation on every `Open`/`Release` creates `O(N)` write amplification (`96.53%` overhead in `buffered`, `99.77%` in `sync`). **Actionable design conclusion**: Production integration in `pkg/mount` MUST NOT rewrite a full JSON snapshot on every `Open`/`Release`; instead, it should use a fixed-size `memfd`/`mmap` shared-memory slot table or append-only binary delta ring (compacted lazily), reserving full CRC32C snapshots for periodic/barrier checkpoints.
2. **Host / Kernel Crash (`host_kernel_crash_vs_process_crash`)**: Verified `ErrHandoffUnavailable`, `MountAlive = false`, `DegradedFallbacks = 1`, `ColdRemountsRequired = 1`. Transparent FUSE recovery cannot survive kernel panics; supervisor cleanly signals cold remount (where `vfs.Volume`'s local journal recovers committed data).
3. **`/dev/fuse` FD Handoff Unavailable (`fuse_fd_handoff_unavailable`)**: Verified `ErrHandoffUnavailable`, `DegradedFallbacks = 1`, `ColdRemountsRequired = 1`. When launched without a supervisor holding `/dev/fuse`, the daemon degrades gracefully with explicit telemetry instead of silently hanging or corrupting handles.

### 6.5 Canonical Visor / Hanzo VFS Integration & Rollout Plan
- **Canonical Architecture Preserved**: `vfs` owns volume/journal durability and `pkg/mount`; `hanzoai/s3` owns sealed object storage; `visor` owns sandbox supervision. No parallel storage format or duplicate FUSE daemon is introduced.
- **Integration Path (Next Implementation Step)**:
  1. Have the Visor host sandbox supervisor open `/dev/fuse` and mount the sandbox directory, then pass the `/dev/fuse` fd (`SCM_RIGHTS` / inherited `ExtraFiles`) to `vfs mount` (`pkg/mount/fuse_unix.go`).
  2. Wire `pkg/mount.session` (`nodes` & `handle` tables) to `internal/fuserecovery` backed by a supervisor-owned `memfd`/tmpfs shared-memory delta table for O(1) open/close overhead and CRC32C verification.
  3. On `pkg/mount` worker crash, Visor respawns `vfs mount --attach-fuse-fd=3 --round=<r+1>` which replays the local `vfs.Volume` journal AND re-attaches `SessionState` in milliseconds without unmounting or interrupting the guest sandbox.
  4. **Orphan persistence note for `vfs.Volume`**: Today `vfs/file.go` keeps unlinked-but-open `orphan` files strictly in daemon RAM (`fileState.orphan`). Stashing active orphans under an internal hidden namespace (as demonstrated in `vfs_integration_test.go` via `.ananke-orphans/`) makes orphan handles survive daemon `SIGKILL` cleanly across `vfs.Attach(Round+1)`.
- **Rollout & Rollback**: Feature-flagged behind supervisor fd-handoff capability (`--fuse-recovery=ananke|off`, default `off` until qualified on Hanzo Linux 6.x Visor nodes). Instant rollback: disable flag to revert to standard `pkg/mount.Mount` with zero on-disk `vfs.Volume` format migration.
- **HIP-1345 / HIP-1344 Status Update**: Research + clean-room bounded implementation package (`internal/fuserecovery/`), canonical `*vfs.Volume` integration test, reproducible benchmark harness (`cmd/ananke-bench`), and raw receipts (`results.json`) are **complete** and ready for maintainer hardware qualification and `pkg/mount` wiring.
