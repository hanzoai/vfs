# HIP-1342 (#13): FUSE Writeback Cache and the Write Acknowledgement Contract

Generated: `2026-11-26T21:43:16Z` | Platform: `linux/amd64` (`2` vCPU, `go1.27.1`) | Real Linux Kernel FUSE: `true`

## 1. Formal Write-Acknowledgement Durability Contract

Under Linux `FUSE_WRITEBACK_CACHE`, buffered `write(2)` returns to the application as soon as bytes land in the kernel page cache — **before** the FUSE daemon receives any `FUSE_WRITE` RPC. Measuring the 5 s object-store publication SLA from buffered `write(2)` is therefore impossible and misleading. `pkg/writeback` defines four explicit durability states:

| State | Trigger | Survives Daemon `SIGKILL`? | Starts 5 s Publish Clock? |
|---|---|---|---|
| `BufferedInKernelPageCache` | `write(2)` into Linux page cache | **No** (volatile kernel memory) | **No** |
| `JournaledInDaemon` | `FUSE_WRITE` -> `Volume.WriteAt` -> `journal.Append` | At risk until local WAL sync | **Yes** (journal entry) |
| `FsyncDurable` | `fsync(2)` / `fdatasync(2)` / `syncfs(2)` -> `journal.Sync` | **Yes (100%)** via local journal replay | **Yes** (POSIX boundary) |
| `PublishedToObjectStore` | Background publisher commits object-store `Head` | **Yes (100%)** globally | Completed (`<= 5 s`) |

### Kernel Dirty Page-Cache Exposure & Mandatory Lifecycle Barrier

- **Host Linux VM defaults** (`/proc/sys/vm/dirty_*`): `dirty_ratio=20%`, `dirty_background_ratio=10%`, `dirty_bytes=0`, `dirty_expire=30s` (6x longer than our 5 s publish bound!), `dirty_writeback=5s`.
- **FUSE mount BDI** (`/sys/class/bdi/0:45`): `strict_limit=1`, `max_ratio=1%`, `max_bytes=13260963` (`12.6 MiB`), `min_ratio=0%`.
- **`WritebackConfig` defaults**: `WritebackCache=true`, `MaxWriteBytes=1 MiB` (`FUSE_MAX_PAGES=256`), `DirtyWindowBytes=64 MiB`, `DirtyExpireDuration=1 s`, `PublishBound=5 s`, `SyncFSOnLifecycle=true`, `ZeroSyncS3OnClose=true`.
- **Lifecycle Barrier (`Detach`, `Suspend`, `End`)**: strictly executes `syncfs(mountFD)` -> `vol.Sync(ctx)` -> `vol.Flush(ctx)` before unmounting so even un-`fsync`ed dirty pages in open descriptors survive.

## 2. Benchmark Results vs Plain Local Disk/NVMe

> **Hardware honesty note:** Measurements below were captured on this `2`-vCPU KVM devbox VM (`linux/amd64`, overlayfs over virtio-blk, NOT a bare-metal NVMe node). Every metric is reported directly beside the **plain local disk** baseline measured on the exact same storage directory.

### Plain Local Disk Baseline (`64 MiB` large burst, `12 MiB` 4 KiB coalescing burst, `120` `fsync` ops)

| Metric | Plain Local Disk |
|---|---|
| 4 KiB Buffered Burst (`12 MiB`, within BDI window) | `1304.5 MB/s` (`1.30 GB/s`) |
| 1 MiB Sustained Buffered Burst (`64 MiB`) | `313.9 MB/s` (`0.31 GB/s`) |
| Durable Burst (`64 MiB` `write(2)` + `fsync(2)`) | `261.9 MB/s` (`0.26 GB/s`) |
| `fsync(2)` Latency (p50 / p95 / p99 / max) | `4390.8 us` / `7985.8 us` / `10185.3 us` / `23748.7 us` |
| Sync-Heavy Random 4 KiB (`write` + `fsync`) | `251.8 IOPS` (`1.03 MB/s`, p50 `4220.3 us`) |

### Real Linux Kernel FUSE Matrix (`128 KiB` vs `1 MiB max_write`, Write-Through vs Writeback Cache)

| Configuration | 4 KiB Burst (MB/s / GB/s, RPCs) | 64 MiB Burst (MB/s, vs Plain) | Durable Burst (MB/s) | 64 MiB RPCs / Max Size | `fsync` p50 / p95 / p99 (us) | `fsync` vs Plain | Sync S3 on Close | Journal->S3 Publish | Lifecycle Barrier (`syncfs`/`journal`/`pub`) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| bazil 128 KiB write-through (legacy baseline) | 16.8 / 0.02 (3072 RPCs) | 80.0 (0.25x) | 78.6 | 512 / 128 KiB | 1971.3 / 3533.8 / 4622.6 | 0.45x | 0 | 84 ms (`true`) | `end`: 0.3 / 2.5 / 20.5 ms (100%) |
| bazil 128 KiB writeback cache | 17.5 / 0.02 (96 RPCs) | 57.3 (0.18x) | 44.5 | 515 / 128 KiB | 2308.1 / 6480.2 / 6630.6 | 0.53x | 0 | 113 ms (`true`) | `detach`: 0.0 / 2.9 / 20.1 ms (100%) |
| jacobsa 1 MiB write-through | 8.8 / 0.01 (3072 RPCs) | 66.7 (0.21x) | 66.3 | 64 / 1024 KiB | 2063.1 / 5959.4 / 6664.3 | 0.47x | 0 | 183 ms (`true`) | `end`: 0.4 / 2.8 / 24.9 ms (100%) |
| jacobsa 1 MiB writeback cache (#13 target) | 355.2 / 0.36 (13 RPCs) | 123.5 (0.39x) | 83.3 | 68 / 1024 KiB | 2135.2 / 4620.9 / 8000.6 | 0.49x | 0 | 189 ms (`true`) | `suspend`: 0.9 / 2.6 / 15.9 ms (100%) |

## 3. Real Kernel FUSE Daemon `SIGKILL` & Lifecycle Survival Matrix

| Case | Scenario | Written | `fsync`ed | Buffered-Only | Survived | Lost | Sync S3 on Close | Result | Exact Observed Outcome |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| `buffered_write_no_fsync_sigkill` | Buffered write(2) (no fsync) + daemon SIGKILL before FUSE_WRITE | 262144 B | 131072 B | 131072 B | 131072 B | 131072 B | 0 | **PASS** | Remounted file = 131072 B (exact fsync'd prefix); 131072 B buffered-only tail LOST in volatile kernel page cache |
| `write_fsync_immediate_sigkill` | write(2) + fsync(2) + immediate SIGKILL before S3 publish | 131072 B | 131072 B | 0 B | 131072 B | 0 B | 0 | **PASS** | 100% (131072/131072 B) recovered from local NVMe journal on Round 2 remount |
| `clean_close_zero_sync_s3` | write(2) + clean close(2) (FUSE_FLUSH) | 262144 B | 0 B | 0 B | 262144 B | 0 B | 0 | **PASS** | 0 sync S3 PUTs on close(2); published asynchronously in 532 ms (<= 5000 ms) |
| `write_no_fsync_detach_barrier` | Buffered write(2) (no fsync, open fd) + Detach syncfs barrier | 262144 B | 0 B | 262144 B | 262144 B | 0 B | 0 | **PASS** | 100% (262144/262144 B) drained by syncfs -> journal -> publish in 25.7 ms and verified in cold object store |

## 4. Honest Negative Regimes

1. **NR-1 — Buffered `write(2)` without `fsync` + Daemon `SIGKILL`:** Under `FUSE_WRITEBACK_CACHE`, `131072` bytes acknowledged to the application via `write(2)` remained in the kernel page cache when `SIGKILL` terminated the daemon and were **100% lost** on remount, whereas `131072` previously `fsync(2)`'d bytes survived 100% via local journal replay. This empirically proves the `#13` contract rule: durability and the 5 s publish bound start at journal entry + `fsync`/`syncfs`, **never** at buffered `write(2)`.
2. **NR-2 — Sync-Heavy 4 KiB Random Writes (`fsync` every write):** Plain disk achieved `251.8 IOPS`; FUSE write-through achieved `414.7 IOPS`; FUSE `1 MiB` writeback achieved `375.1 IOPS` (`0.90x` vs write-through). Because every 4 KiB write immediately forces `FUSE_WRITE` + `FUSE_FSYNC`, writeback caching and `1 MiB max_write` cannot coalesce writes and provide essentially zero burst benefit on sync-heavy random workloads.
3. **NR-3 — Unbounded Dirty Page-Cache Exposure -> `Detach`/`Suspend` `syncfs` Tail Stall:** When buffered writes accumulate without periodic flushing, `syncfs(mountFD)` on `Detach`/`Suspend` must synchronously drain the entire dirty window into the daemon journal before publishing:

| Buffered Dirty Window | `syncfs(2)` Drain | Journal Sync | Final S3 Publish | Total Barrier Latency |
|---:|---:|---:|---:|---:|
| `16 MiB` | `60.0 ms` | `73.8 ms` | `62.2 ms` | `196.0 ms` |
| `64 MiB` | `251.4 ms` | `92.0 ms` | `368.0 ms` | `711.4 ms` |
| `128 MiB` | `449.2 ms` | `1397.3 ms` | `314.2 ms` | `2160.7 ms` |

Bounding dirty exposure via `WritebackConfig.DirtyWindowBytes` (default `64 MiB`), `DirtyExpireDuration` (`1 s`), and `DirtyGuard` keeps `Detach`/`Suspend` tail latency predictable.

## 5. Future Follow-Up Evaluation: FUSE Passthrough & FUSE-over-`io_uring`

- **FUSE Passthrough (Linux 6.8+, `FUSE_PASSTHROUGH` / `FUSE_DEV_IOC_BACKING_OPEN`):** Bypasses userspace entirely for `read(2)`/`write(2)` by binding an open FUSE file to an underlying backing file descriptor. For `hanzoai/vfs`, passthrough **cannot** be used directly on encrypted chunked Volume files (every write must pass through AES-256-GCM journal sealing and content-defined chunking, and reads decrypt sealed packs). However, passthrough IS a strong candidate for two narrower follow-ups: (a) **ephemeral scratch/cache files** or unsealed local staging files that are sealed on `fsync`/close, and (b) **immutable pre-decrypted base-image read caches** on local NVMe where the daemon verifies BLAKE3 hashes on populate and then hands the read-only backing FD to the kernel for native NVMe page-cache read speed.
- **FUSE-over-`io_uring` (Linux 6.14+, `FUSE_OVER_IO_URING`):** Replaces per-request `/dev/fuse` `read(2)`/`write(2)` syscalls and single-queue lock contention with per-CPU `io_uring` submission/completion rings and registered shared buffers. Unlike passthrough, **FUSE-over-`io_uring` is 100% compatible with `hanzoai/vfs`'s encrypted journal & chunking pipeline** because every RPC still reaches userspace — it eliminates userspace/kernel copy and `/dev/fuse` mutex bottlenecks on high-core-count NVMe nodes (especially for NR-2 sync-heavy small I/O and high-concurrency metadata workloads). Requires Go bindings or a minimal C/Go ring consumer for `/dev/fuse` `IORING_OP_URING_CMD`, making it a clean P1/P2 follow-up once kernel 6.14+ is standard across fleet nodes.

## 6. Licensing & Clean-Room Audit

All code in `pkg/writeback/`, `pkg/mount/writeback*.go`, and `cmd/writeback-bench/` is an independent clean-room Apache-2.0 Go implementation using **zero new external dependencies** (`go.mod` and `go.sum` are completely unchanged; `github.com/jacobsa/fuse` and `bazil.org/fuse` are already direct dependencies in `go.mod`).
