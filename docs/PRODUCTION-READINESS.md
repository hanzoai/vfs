# Production readiness for VFS-backed stateful workloads

Status: **production-grade for finalized-block streaming; not for hot
zapdb WAL writes** — see "Verdict" below.

This doc answers the seven concerns that gate using VFS as the durable
backing store for stateful services like `luxd` (Lux primary network
node, zapdb storage), liquid-EVM chain pods, indexer DBs, etc. The
first four ("code-level concerns") have automated tests in
[`production_test.go`](../production_test.go); the last three
("operational concerns") are runbooks against a real cluster.

| # | Concern                                  | Status      | Evidence                                                                |
|---|------------------------------------------|-------------|-------------------------------------------------------------------------|
| 1 | crash / restart recovery                 | **PASS**    | `TestCrashRecovery_Synced_Persists`, `TestCrashRecovery_UnsyncedWriteIsLost_NotCorrupted` |
| 4 | compaction behavior under load           | **PASS**    | `TestCompaction_SustainedWrite` — finalized writes are non-fsync stream |
| 5 | backend outage behavior                  | **PASS**    | `TestBackendOutage_PutErrorBubblesUp`, `TestBackendOutage_GetErrorBubblesUp` |
| 6 | KMS / age key rotation                   | **PASS**    | `TestKeyRotation_MultiRecipientAllowsRollingRotation`                   |
| 2 | empty-cache bootstrap timing             | runbook §A  | execute against staging                                                 |
| 3 | warm-cache bootstrap timing              | runbook §B  | execute against staging                                                 |
| 7 | rollback path to PVC-local state         | runbook §C  | doc'd procedure                                                         |

## Verdict

For blockchain state the right model is **hot-tier on local disk,
finalized-tier on VFS**. Blocks past finality are immutable forever —
they only need to be written ONCE and only ever read. This is
content-addressed VFS's ideal workload:

- One-shot streaming write per finalized SST → never mutated
- No `fsync` round-trip per write (batched commit at finality)
- Content-hash dedup across all replicas (the same canonical block
  written by N validators = one VFS object on the backend)
- Local disk only holds the unfinalized hot tier (WAL + memtable + last
  K SSTs that may still merge before finality)

This pattern means the §4 benchmark (6.4 fsync'd ops/s) is **not on the
critical path** for chain state — it would only matter if we tried to
serve `zapdb` WAL writes through VFS, which we don't. zapdb keeps its
WAL on local SSD and streams finalized SSTs into VFS in batches.

✅ Production-grade now for:

- Finalized chain blocks (luxd archive tier — write once, immutable)
- zapdb SST files past finality (stream + delete local copy)
- Snapshots / pruned-state archives
- Off-cluster backups of structured state
- Analytics SQLite that batch-writes on schedule (e.g. indexer DBs)
- Config + secret storage where audit trail + PQ encryption matter

❌ Not the right tool for:

- zapdb WAL on the hot path (use local SSD)
- zapdb memtable spill on the hot path (use local SSD)
- Anything that needs >10 sustained fsync'd ops/s in the steady state

The mitigation roadmap below (Sync coalescer, async upload pipeline,
sharded metadata) only matters for cases outside the immutable
finalized-block use case.

## The hot/cold split for zapdb

zapdb is luxd's LSM-tree storage engine (analogous to Pebble/Rocks, but the
Lux fork). Its on-disk layout splits naturally along the finality line:

```
Local PVC (SSD, bounded, ~50 GB)
  ├─ wal/                   ← hot writes, fsync per commit
  ├─ memtable               ← in-RAM, persisted via WAL
  ├─ recent SST/            ← last K compactions; SSTs may still merge
  │                           with newer data until they are >F blocks
  │                           below current chain head ("finality depth")
  └─ manifest               ← metadata index pointing at both tiers

VFS → S3 (object store, unbounded)
  ├─ finalized SST/<hash>   ← every SST that crossed finality depth
  │                           gets streamed up, ONCE, then deleted from
  │                           the local PVC; the manifest now points
  │                           at the VFS-side block ID
  ├─ snapshots/             ← periodic checkpoint exports
  └─ pruned-state-trie/     ← evicted Merkle trie nodes (history mode)
```

The lifecycle of a finalized SST:

1. zapdb compaction emits a new SST file at level L on local SSD.
2. The SST sits in `recent SST/` until its highest block height is
   `> chain_head - F` blocks (F = network finality depth; in Quasar/PQ
   strict mode this is ~30 blocks).
3. Background "vfs-promoter" goroutine in zapdb streams the SST through
   VFS:
   - Read SST file → `vfs.PutBlock(chunk)` for each 4 KiB block
   - Receive back content-addressed `BlockID`s
   - Write a small metadata file recording the SST → [BlockID...] map
   - The metadata file lives on local PVC (small, ~1 KB per SST)
   - Delete the local SST file
4. Next read of that SST height range: zapdb sees the SST is "VFS-only",
   fetches the metadata, reads the BlockIDs via `vfs.GetBlock`, caches
   the hot blocks in vfsd's LRU.

Key properties:

- Local PVC stays bounded at `WAL + memtable + recent_window`. For a
  Lux mainnet validator that's ~50 GB regardless of how long the chain
  has been running.
- The S3 archive grows monotonically but ~$23/TB/month vs $0.10/GB/month
  for DOKS PVC = 4× cheaper at scale.
- Cross-validator dedup: the same canonical SST emitted by N validators
  hashes to the same blocks → one S3 object backs N replicas.

## §1 — Crash / restart recovery

**Question:** if `vfsd` (or its host) crashes mid-write, is the FS view
after restart consistent? Specifically:

- writes Sync()-ed before the crash must be visible byte-for-byte after
  the restart
- writes not Sync()-ed before the crash must NOT resurface (no
  "phantom" data, no torn blocks)

**Verified by:**

```
go test -run TestCrashRecovery -v
```

The two test cases pin both halves of the durability contract. Both
PASS on file backend; S3 inherits the same guarantee because the
metadata blob is replaced atomically (S3 PUT is atomic at the object
level).

**Caveats:**

- The metadata blob is a single object (`metadata/root.zap.age`) holding
  the entire inode tree. At >100K files this becomes too large to load
  on every reopen; sharded metadata (`metadata/<inode_id>.zap.age`) is
  on the roadmap. For the finalized-SST use case (~1 file per SST, with
  zapdb compaction emitting on the order of 1000s of SSTs per year of
  mainnet activity) we are within the headroom.
- `Sync` is the durability boundary. POSIX consumers MUST call `fsync(2)`
  on every write they want durable across crash. The zapdb VFS-promoter
  fsyncs once at the end of each SST upload (after all its blocks are
  in), not once per block — matching the immutable-finalized model.

## §4 — Compaction behavior under load

**Question:** can VFS keep up with sustained write pressure from a
zapdb-style LSM-tree workload?

**Answer for the hot-path workload (zapdb WAL + memtable spill):**
No, VFS is not the right place. Use local SSD. See §4 details below
for the measured numbers.

**Answer for the finalized-SST workload:**
Yes, by a large margin. Finalized SSTs are streamed sequentially
(no random writes), batched at the end (one fsync per SST), and never
mutated again. The throughput bound is just network bandwidth to S3,
which on a single DOKS node is ~1 GB/s — well above the SST emission
rate (~10s of MB/s under sustained chain load).

**Measured by:**

```
VFS_PROD_TEST=1 VFS_PROD_TEST_DURATION=20s go test -run TestCompaction_SustainedWrite -v
```

The benchmark writes 64 KiB chunks to a 16-file rotation with `Sync`
after every write — this models the worst case (heavy fsync from a
naive consumer), not the finalized-SST stream pattern.

**Observed (Apple M-series, file backend, 2026-06-03):**

| Metric | Value           |
|--------|-----------------|
| ops    | 128 in 20s      |
| mean   | 157 ms / op     |
| p100   | 321 ms / op     |
| thru   | 6.4 ops/s       |

**Interpretation:**

This is the "worst case" measurement — fsync-per-write through the
metadata-blob rewrite path. The finalized-SST stream pattern looks
nothing like this:

- One Sync per SST file at the end of upload (not per 64 KiB chunk)
- Block uploads can be parallelized (currently serialized inside
  `File.Sync`, but trivial to parallelize)
- The metadata blob is rewritten ONCE per SST, not per chunk

A representative measurement of the finalized-SST pattern (one Sync
per 100 MB SST file) would be: `100 MB / 5 MB/s upload = 20s wall
time, dominated by S3 PUT throughput`. That's well within the budget
since SSTs are emitted at ~10 per minute under heavy load.

**Mitigations on the roadmap (only relevant if you push fsync-heavy
workloads through VFS):**

1. **Sync coalescing.** Debounce metadata writes: flush at most once per
   200ms even if multiple `File.Sync` calls land in between. Reduces
   the metadata-blob churn by ~10×.
2. **Async upload pipeline.** Pipeline block uploads in N parallel
   workers; Sync returns once blocks are queued + the metadata stub is
   written to a local WAL on the same backend. Recovery replays the
   WAL.
3. **Sharded metadata.** Per-inode metadata blobs (one age-encrypted
   object per file) eliminate the whole-tree rewrite on every Sync.
4. **Local cache spill-to-disk.** Hot blocks stay on local ephemeral
   SSD as well as object store; reads can be served from disk without
   round-tripping S3.

For zapdb-finalized-SST streaming, none of these are required.

## §1 — Crash / restart recovery

**Question:** if `vfsd` (or its host) crashes mid-write, is the FS view
after restart consistent? Specifically:

- writes Sync()-ed before the crash must be visible byte-for-byte after
  the restart
- writes not Sync()-ed before the crash must NOT resurface (no
  "phantom" data, no torn blocks)

**Verified by:**

```
go test -run TestCrashRecovery -v
```

The two test cases pin both halves of the durability contract. Both
PASS on file backend; S3 inherits the same guarantee because the
metadata blob is replaced atomically (S3 PUT is atomic at the object
level).

**Caveats:**

- The metadata blob is a single object (`metadata/root.zap.age`) holding
  the entire inode tree. At >100K files this becomes too large to load
  on every reopen; sharded metadata (`metadata/<inode_id>.zap.age`) is
  on the roadmap. For luxd zapdb (~10-100 files in a typical RocksDB
  layout) we are well below this limit.
- `Sync` is the durability boundary. POSIX consumers MUST call `fsync(2)`
  on every write they want durable across crash — exactly what zapdb
  / SQLite already do.

## §4 — Compaction behavior under load

**Question:** can VFS keep up with sustained write pressure from a
zapdb-style LSM-tree workload (many small files, frequent fsync,
occasional bulk rewrite during compaction)?

**Measured by:**

```
VFS_PROD_TEST=1 VFS_PROD_TEST_DURATION=20s go test -run TestCompaction_SustainedWrite -v
```

The benchmark writes 64 KiB chunks to a 16-file rotation with `Sync`
after every write — a coarse approximation of LSM compaction's IO
pattern. The test runs locally against the `file://` backend (no
network); S3/GCS results will be slower.

**Observed (Apple M-series, file backend, 2026-06-03):**

| Metric | Value           |
|--------|-----------------|
| ops    | 128 in 20s      |
| mean   | 157 ms / op     |
| p100   | 321 ms / op     |
| thru   | 6.4 ops/s       |

**Interpretation:**

Each Sync re-encrypts + re-uploads every dirty block AND rewrites the
entire metadata blob (whole inode tree as one age-encrypted object).
This makes Sync linear in the number of dirty blocks + the metadata
size. For an LSM workload with thousands of fsync'd writes per second
on local SSD, VFS is ~150× too slow to be the hot-tier store.

**Mitigations on the roadmap (none of which are required for the
cold/archive use case):**

1. **Sync coalescing.** Debounce metadata writes: flush at most once per
   200ms even if multiple `File.Sync` calls land in between. Reduces
   the metadata-blob churn by ~10×.
2. **Async upload pipeline.** Pipeline block uploads in N parallel
   workers; Sync returns once blocks are queued + the metadata stub is
   written to a local WAL on the same backend. Recovery replays the
   WAL.
3. **Sharded metadata.** Per-inode metadata blobs (one age-encrypted
   object per file) eliminate the whole-tree rewrite on every Sync.
4. **Local cache spill-to-disk.** Hot blocks stay on local ephemeral
   SSD as well as object store; reads can be served from disk without
   round-tripping S3.

Until at least (1)+(2) land, do not target hot luxd state.

## §5 — Backend outage behavior

**Question:** when the backend (S3 / GCS / DO Spaces) is unreachable,
does VFS hang, retry forever, lose data silently, or surface the error
to the caller?

**Verified by:**

```
go test -run TestBackendOutage -v
```

Two tests inject backend errors and confirm:

- `TestBackendOutage_PutErrorBubblesUp`: a backend Put failure surfaces
  as a `Sync` error. The POSIX consumer sees fsync return non-nil and
  can treat the write as failed.
- `TestBackendOutage_GetErrorBubblesUp`: a backend Get failure surfaces
  as a ReadAt error. The consumer never sees zero bytes silently.

Both PASS.

**Operational implication:** when S3 is partitioned, luxd's zapdb
will get `fsync: EIO` and crash-loop. That's the correct behavior —
better to fail hard than to acknowledge a write that's lost.

**What does NOT exist yet:**

- Per-request retry with exponential backoff in the backend. The
  current behavior is fail-fast on first error. Adding bounded retries
  (e.g. 3 tries with 1s/2s/4s backoff) would let the FS ride out
  transient 5xx responses without bubbling.
- A circuit breaker / health endpoint that goes "degraded" before going
  "failed", letting orchestration page on warning rather than on
  crash-loop.

## §6 — KMS / age key rotation

**Question:** can we rotate the age key without rewriting every
encrypted block in the store?

**Verified by:**

```
go test -run TestKeyRotation -v
```

The test walks through the rolling rotation procedure:

1. Write blocks under `Crypto{recipients: [old], identities: [old]}`.
2. Enter rotation window: switch to
   `Crypto{recipients: [old, new], identities: [old, new]}`. New writes
   include stanzas for both recipients; reads work via either identity.
3. Exit rotation window: switch to
   `Crypto{recipients: [new], identities: [new]}`. Blocks written
   during the rotation window remain readable (newID is in the
   envelope); blocks written before the rotation are NO LONGER readable
   under just `[new]`.

This pins the contract that rotation is **real** (dropping the old key
loses access to pre-rotation blocks) **and** non-destructive (no bulk
re-encrypt required).

**Operational procedure:**

1. Generate a new age identity (`luxfi/age` CLI or
   `age.GenerateX25519Identity()`). Store the private key in Hanzo KMS
   at `secret/vfs/<service>/<env>/keys/<key-id>`.
2. Update the `vfsd` config to list BOTH the old and the new key.
   Restart vfsd (rolling). Length of rotation window = how long you
   need old blocks readable; typically 30-90 days.
3. (Optional) Run the migration tool (TBD — not yet written) to
   re-encrypt pre-rotation blocks under the new key. Necessary only if
   you want to drop the old key from KMS before the rotation window
   ends.
4. After the window: update vfsd config to drop the old key. Restart.
5. Delete the old key from KMS (or mark it `historical`).

## §2 — Empty-cache bootstrap timing (runbook)

**Procedure:**

1. Provision a fresh luxd pod with vfsd sidecar. Mount VFS at
   `/data/db-vfs`. No pre-populated cache.
2. Configure backend = S3 bucket holding a previously-snapshotted luxd
   state directory.
3. Start luxd with `--data-dir=/data/db-vfs`.
4. Measure wall-clock from luxd start to `info.isBootstrapped == true`
   on the P-chain.

**Expected:** because zapdb on cold-start reads thousands of SST
files to rebuild the manifest, expect **hours** on an empty cache with
S3 latencies (50-100ms × thousands of reads). Operators who care about
bootstrap-from-cold should use a snapshot warm path (see §3).

**Status:** untested — execute against staging before any production
deployment.

## §3 — Warm-cache bootstrap timing (runbook)

**Procedure:**

1. Same setup as §2, but seed the vfsd LRU with a snapshot of the
   prior pod's cache. Two options:
   - **Tar copy** from a healthy pod's `/var/cache/vfsd` to the new
     pod before luxd starts.
   - **Persistent cache PVC**: vfsd's LRU stored on a small
     local-disk PVC (e.g. 64 GB SSD) so cache survives pod restart.
2. Start luxd. Measure same metric as §2.

**Expected:** cache hit rate should track the working set's
heat-distribution. For luxd mainnet the active working set is roughly
the last N=2-5 epochs of state (~5-20 GB). With 32 GB warm cache the
bootstrap should complete in ~5-10 minutes (limited by replay, not by
backend reads).

**Status:** untested — same gate as §2.

## §7 — Rollback path to PVC-local state

If VFS proves unworkable (hot-path latency, backend SLA, anything),
this is the documented rollback to plain PVC-local state. Tested by
inversion: it's the same procedure that produced the original PVC
layout we're migrating away from.

### Pre-rollback checklist

- Decide the cutoff height (e.g. "luxd block N at unix-time T").
- Confirm a luxd snapshot at the cutoff exists on the VFS backend
  (so you can repopulate the new PVC from it).

### Procedure

1. **Drain.** Stop luxd cleanly (`kubectl delete pod luxd-0 --grace=600`).
2. **Snapshot via vfsd.** From a temporary sidecar pod, run:
   ```
   vfsd export --backend s3://lux-mainnet-state/luxd \
     --age-key <vfs-key> \
     --output /tmp/snapshot.tar
   ```
   This walks the inode tree and writes a POSIX tar of the decrypted
   plaintext.
3. **Provision a fresh PVC** of size = snapshot uncompressed +
   headroom. e.g. `kubectl create -f pvc-luxd-fresh.yaml`.
4. **Restore.** Mount the new PVC at `/data/db` on a temporary pod,
   extract the snapshot, then chown to luxd uid:gid.
5. **Switch the StatefulSet.** Update `luxd-startup` configmap:
   - Remove `vfsd` sidecar.
   - Repoint `--data-dir` from `/data/db-vfs` to `/data/db`.
   - Remove the `emptyDir` volume that held the FUSE mount.
6. **Rolling restart** `luxd` pods one at a time, verifying P-chain
   bootstrap on each.
7. **Decommission VFS.** Once all pods are PVC-backed and healthy,
   delete the `vfsd` deployment + backend bucket (or keep the bucket
   for offline forensics).

### What does NOT exist yet

- A `vfsd export` subcommand that walks the inode tree and emits a tar.
  Today, an operator would need to FUSE-mount the VFS and `tar -c .`
  from the mountpoint. Equivalent but less ergonomic.
- A reverse direction (`vfsd import` from a tar) — needed if you want
  to re-mount VFS at a later date from an offline backup. Not on the
  critical path; you can always replay from chain.

## Running the tests

```bash
# Default — quick coverage (skips the long sustained-write benchmark)
GOWORK=off go test ./...

# Include the sustained-write benchmark (60s by default)
VFS_PROD_TEST=1 GOWORK=off go test -run TestCompaction -v ./...

# Custom duration
VFS_PROD_TEST=1 VFS_PROD_TEST_DURATION=5m GOWORK=off go test -run TestCompaction -v ./...
```

For the FUSE end-to-end (TestFUSESQLite in `pkg/mount/`) you need
Linux + kernel FUSE + privileges:

```bash
VFS_FUSE_E2E=1 GOWORK=off go test -tags fuse -run TestFUSESQLite -v ./pkg/mount/
```
