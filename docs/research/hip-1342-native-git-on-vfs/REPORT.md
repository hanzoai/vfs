# Native Git on VFS — Benchmark & Architecture Report (hanzoai/vfs#29, HIP-1342)

Generated: `2026-11-26T21:18:58Z`  
Host: `linux/amd64`, `2` CPUs, `go1.27.1`, `git version 2.56.1`  
Corpus: real `hanzoai/vfs` repository (`db27b7cf6fef`, `+400` deterministic files, seed `1342`, total `710` tracked files)  
VFS Execution Path: `fuse_mount=true`, `fsmonitor_supported=true`, `repetitions=3`

## 1. Plain NVMe Baseline Probe & Forge Verification

| Primitive (Plain Local NVMe) | Latency (us) |
|---|---:|
| `fsync` (4 KiB file, p50) | 4619.7 |
| `fsync` (4 KiB file, p95) | 8297.2 |
| `fsync` (4 KiB file, mean) | 5570.8 |
| `create + write(4 KiB)` (p50) | 48.5 |
| `stat` (p50) | 8.1 |
| `rename` (p50) | 30.0 |
| `unlink` (p50) | 28.9 |

### Server-Side Bare Forge Verification

- `protocol.version`: `2`
- `uploadpack.allowFilter`: `true`, `uploadpack.allowAnySHA1InWant`: `true`
- Pack reachability `.bitmap`: `true` (`2` pack, `0` loose objects)
- `commit-graph` (`--reachable --changed-paths`): `true`, `multi-pack-index`: `true`, `git fsck --full`: `true`

### S3-Backed Disk Invariants (`*vfs.Volume`)

- **Zero synchronous store PUTs on clean `Write` + `Sync` + `Close`**: `true` (`0` PUTs during close)
- **Acknowledged data published to store within `<= 5 s`**: `true` (observed `540.7 ms`, `3` PUTs, `32289` bytes)

## 2. Side-by-Side Git Workload Results (vs Plain NVMe)

| Mode | Layer | Clone-to-First-Cmd (ms) | Status Cold (ms) | Status Warm (ms) | Checkout (ms) | Commit+Push (ms) | Own Git Objects | VFS Store PUTs | VFS PUT Bytes | Journal fsyncs | fsync Ratio vs NVMe |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `plain-nvme-untuned` | `plain-nvme` | 525.5 | 63.5 | 61.8 | 448.5 | 140.1 | 849 (1062 KiB) | 0 (p:0/m:0/h:0) | 0 | 0 | 1.00x |
| `plain-nvme-tuned` | `plain-nvme` | 420.3 | 37.7 | 38.5 | 97.6 | 102.1 | 849 (1061 KiB) | 0 (p:0/m:0/h:0) | 0 | 0 | 1.00x |
| `vfs-untuned` | `vfs-fuse` | 2320.4 | 485.4 | 265.1 | 1190.2 | 395.3 | 849 (1062 KiB) | 60 (p:2/m:56/h:2) | 6856472 | 11 | 0.54x |
| `vfs-tuned` | `vfs-fuse` | 1565.3 | 357.9 | 107.8 | 1171.4 | 255.1 | 849 (1061 KiB) | 54 (p:2/m:50/h:2) | 6687778 | 8 | 1.36x |
| `vfs-tuned-batched-fsync` | `vfs-fuse` | 1655.2 | 224.9 | 398.1 | 811.1 | 383.7 | 849 (1061 KiB) | 60 (p:2/m:56/h:2) | 6795942 | 17 | 0.53x |
| `vfs-reference-alternates` | `vfs-fuse` | 1903.4 | 352.3 | 68.6 | 1295.6 | 293.7 | 0 (1 KiB) | 74 (p:2/m:70/h:2) | 5318059 | 4 | 0.64x |
| `vfs-reference-sparse` | `vfs-fuse` | 1387.2 | 104.1 | 138.0 | 367.2 | 462.8 | 0 (1 KiB) | 18 (p:2/m:14/h:2) | 1310406 | 4 | 0.49x |

### Per-Operation VFS Object-Store PUTs, Bytes & Journal Syncs

| Mode | Operation | Wall (ms) | Store PUTs (pack/meta/head) | PUT Bytes | Journal Syncs |
|---|---|---:|---:|---:|---:|
| `vfs-untuned` | `clone` | 1835.0 | 0 (0/0/0) | 0 | 4 |
| `vfs-untuned` | `status_cold` | 485.4 | 0 (0/0/0) | 0 | 0 |
| `vfs-untuned` | `status_warm` | 265.1 | 17 (1/16/0) | 3794886 | 0 |
| `vfs-untuned` | `checkout` | 1190.2 | 0 (0/0/0) | 0 | 2 |
| `vfs-untuned` | `commit_and_push` | 395.3 | 0 (0/0/0) | 0 | 0 |
| `vfs-untuned` | `flush` | 196.0 | 30 (1/28/1) | 2228744 | 1 |
| `vfs-tuned` | `clone` | 1207.3 | 0 (0/0/0) | 0 | 3 |
| `vfs-tuned` | `status_cold` | 357.9 | 0 (0/0/0) | 0 | 0 |
| `vfs-tuned` | `status_warm` | 107.8 | 0 (0/0/0) | 0 | 0 |
| `vfs-tuned` | `checkout` | 1171.4 | 31 (1/29/1) | 4954511 | 1 |
| `vfs-tuned` | `commit_and_push` | 255.1 | 0 (0/0/0) | 0 | 0 |
| `vfs-tuned` | `flush` | 185.7 | 23 (1/21/1) | 1733267 | 1 |
| `vfs-tuned-batched-fsync` | `clone` | 1430.3 | 0 (0/0/0) | 0 | 5 |
| `vfs-tuned-batched-fsync` | `status_cold` | 224.9 | 0 (0/0/0) | 0 | 0 |
| `vfs-tuned-batched-fsync` | `status_warm` | 398.1 | 0 (0/0/0) | 0 | 0 |
| `vfs-tuned-batched-fsync` | `checkout` | 811.1 | 31 (1/29/1) | 4953148 | 2 |
| `vfs-tuned-batched-fsync` | `commit_and_push` | 383.7 | 0 (0/0/0) | 0 | 5 |
| `vfs-tuned-batched-fsync` | `flush` | 172.4 | 29 (1/27/1) | 1842794 | 1 |
| `vfs-reference-alternates` | `clone` | 1551.1 | 0 (0/0/0) | 0 | 2 |
| `vfs-reference-alternates` | `status_cold` | 352.3 | 0 (0/0/0) | 0 | 0 |
| `vfs-reference-alternates` | `status_warm` | 68.6 | 0 (0/0/0) | 0 | 0 |
| `vfs-reference-alternates` | `checkout` | 1295.6 | 19 (0/18/1) | 863591 | 1 |
| `vfs-reference-alternates` | `commit_and_push` | 293.7 | 0 (0/0/0) | 0 | 0 |
| `vfs-reference-alternates` | `flush` | 212.9 | 34 (1/32/1) | 1834820 | 1 |
| `vfs-reference-sparse` | `clone` | 1283.1 | 0 (0/0/0) | 0 | 2 |
| `vfs-reference-sparse` | `status_cold` | 104.1 | 0 (0/0/0) | 0 | 0 |
| `vfs-reference-sparse` | `status_warm` | 138.0 | 0 (0/0/0) | 0 | 0 |
| `vfs-reference-sparse` | `checkout` | 367.2 | 0 (0/0/0) | 0 | 0 |
| `vfs-reference-sparse` | `commit_and_push` | 462.8 | 12 (1/10/1) | 1153778 | 1 |
| `vfs-reference-sparse` | `flush` | 38.4 | 6 (1/4/1) | 156628 | 1 |

## 3. Batched vs Unbatched `core.fsync=committed` Bulk Staging

| Storage | Strategy | Staged Files | `git add` (ms) | `git commit` (ms) | `strace` fsyncs (150-file trace) | Journal Syncs | Journal fsync p50 (us) | fsync Ratio vs NVMe |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `plain-nvme` | default (no loose-object fsync) | 200 | 72.6 | 49.1 | 0 | 0 | 4619.7 | 1.00x |
| `plain-nvme` | core.fsync=committed (unbatched) | 200 | 1093.9 | 39.1 | 150 | 0 | 4619.7 | 1.00x |
| `plain-nvme` | core.fsync=committed + core.fsyncMethod=batch | 200 | 103.8 | 61.2 | 1 | 0 | 4619.7 | 1.00x |
| `vfs-fuse` | default (no loose-object fsync) | 200 | 578.4 | 117.7 | n/a | 0 | 2239.4 | 0.48x |
| `vfs-fuse` | core.fsync=committed (unbatched) | 200 | 1728.8 | 166.3 | n/a | 204 | 1959.5 | 0.42x |
| `vfs-fuse` | core.fsync=committed + core.fsyncMethod=batch | 200 | 1012.9 | 88.8 | n/a | 4 | 2525.7 | 0.55x |

## 4. O(1) Copy-on-Write `*vfs.Volume` Snapshot Clone-by-Reference

- Base snapshot: `740` files (`4383388` bytes), built in `517.9 ms` (`32` total PUTs: `1` pack, `27` meta)
- Cloned `4` isolated sandboxes via `COWBackend` + `vfs.Reseal`: median `47.11 ms` (max `58.26 ms`)
- Per-sandbox clone PUTs: **`2` head PUT, `0` pack PUTs, `0` meta PUTs** (`0` mutations to read-only base backend)
- Post-clone first `git status` (`core.checkStat=minimal`, `core.trustctime=false`): `55.0 ms`; commit+push to forge: `115.9 ms`; delta flush: `5` PUTs (`422446` bytes)

## 5. Honest Negative Regimes & Operational Hazards

| ID | Regime / Hazard | Baseline | Variant | Delta / Outcome |
|---|---|---|---|---|
| NR-1 | **Tiny 5-file repository setup overhead** — Running commit-graph + multi-pack-index + local config tuning on a 5-file repo adds extra git process invocations without recovering the cost on status/checkout. | plain clone + status (5 files) | tuned clone + commit-graph + MIDX + status (5 files) | 72.3 ms -> 114.4 ms (+58.1%) |
| NR-2 | **Full-history git log -S over blobless partial clone (--filter=blob:none)** — History content search (`git log -S`) on a blobless partial clone forces synchronous on-demand promisor fetches for historical blobs from the forge, regressing wall time vs a full clone. | full clone `git log -n 25 -S Volume` | blobless partial clone `git log -n 25 -S Volume` | 52.1 ms -> 2751.1 ms (+5183.2%) |
| NR-3 | **Shared alternates git gc --prune=now hazard** — Pruning unreachable objects in a shared alternates store while live non-dissociated sandboxes still reference it corrupts borrower git fsck. SafePrune() blocks pruning until all borrowers dissociate. | borrower fsck before store prune (healthy) | borrower fsck after forced store prune (broken) | {"borrower_fsck_failed_after_forced_prune":true,"safe_prune_blocked_with_active_borrower":true} |

## 6. Reproducing

```sh
# Direct *vfs.Volume + plain NVMe benchmark (no root or FUSE required)
go run ./cmd/gitvfs-bench --repo . --out-dir docs/research/hip-1342-native-git-on-vfs

# Real kernel FUSE-mounted *vfs.Volume + plain NVMe benchmark (Linux user namespace)
go build -o /tmp/fusermount3 ./tools/fusemount
go build -tags fuse -o /tmp/gitvfs-bench ./cmd/gitvfs-bench
unshare -Urm env PATH=/tmp:$PATH /tmp/gitvfs-bench --fuse --repo . --out-dir docs/research/hip-1342-native-git-on-vfs
```
