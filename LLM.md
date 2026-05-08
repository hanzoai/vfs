# hanzoai/vfs — AI Engineering Guide

## What this is

S3-backed virtual block filesystem. Local-NVMe hot tier + S3 (or any object
store) cold tier with PQ encryption per block. Stateful services (IAM, BD,
ATS, TA, KMS, onyxd, lqd snapshots) get unlimited disk via S3 without
needing K8s PVC sizing decisions.

Companion to `~/work/hanzo/replicate` — `replicate` streams SQLite WAL
frames to S3 (file-level granularity); `vfs` is block-level granularity
that transparently spills cold pages.

## Architecture

```
                  ┌──────────────────────────────┐
                  │   FUSE / WASI mount point    │  /data/
                  └──────────────┬───────────────┘
                                 │ POSIX read/write
                  ┌──────────────▼───────────────┐
                  │   Block layer (4 KiB pages)  │
                  │   Content-addressable        │
                  │   blake3 hash → block ID     │
                  └──────────────┬───────────────┘
                                 │
                       ┌─────────┴─────────┐
                       ▼                   ▼
            ┌──────────────────┐  ┌──────────────────┐
            │  Local NVMe      │  │  Object backend  │
            │  hot cache       │  │  (s3/gcs/azure/  │
            │  (configurable)  │  │   file://)       │
            │  LRU eviction    │  │  PQ-encrypted    │
            └──────────────────┘  └──────────────────┘
```

Encryption: every block is `age`-encrypted with hybrid X25519 + ML-KEM-768
recipients (via `luxfi/age`) before leaving the local cache. Backends
never see plaintext.

Content-addressable: each block stored at `blocks/<blake3-prefix>/<hash>.zap.age`.
Identical blocks dedupe naturally.

## Layout

```
vfs/
├── cmd/
│   └── vfs/                 # CLI entry point
│       └── main.go
├── pkg/
│   ├── vfs/
│   │   ├── vfs.go           # Top-level FS interface
│   │   ├── block.go         # 4 KiB block + content-addressable hashing
│   │   ├── cache.go         # LRU write-back cache
│   │   └── crypto.go        # luxfi/age PQ encryption per block
│   ├── backend/
│   │   ├── backend.go       # Backend interface (Get/Put/Delete/List)
│   │   ├── file/file.go     # file:// backend (local dev)
│   │   ├── s3/s3.go         # AWS S3 / S3-compatible backend
│   │   ├── gcs/             # (TODO) Google Cloud Storage
│   │   └── azure/           # (TODO) Azure Blob
│   ├── mount/
│   │   ├── fuse_unix.go     # bazil.org/fuse (build tag: fuse + !windows)
│   │   ├── fuse_stub.go     # stub for builds without FUSE
│   │   └── wasi.go          # (TODO) WASI guest mode
│   └── sidecar/
│       └── sidecar.go       # (TODO) K8s sidecar mode
├── tests/                   # E2E tests
├── docs/                    # Architecture docs
├── go.mod
├── Makefile
├── Dockerfile
└── VERSION
```

## CLI

```bash
# Mount an S3-backed VFS (FUSE)
vfs mount /tmp/v \
    --backend s3://bucket/prefix \
    --age-recipient age1xyz... \
    --age-key /etc/vfs/age.key \
    --cache-dir /var/cache/vfs \
    --cache-size 10Gi

# One-shot put/get (no mount; useful for testing)
vfs put /path/to/file --backend file:///tmp/store
vfs get hash --backend file:///tmp/store > /tmp/out

# Stats (cache hit ratio, backend bytes, block count)
vfs stats --cache-dir /var/cache/vfs
```

## K8s sidecar pattern

Same shape as `hanzo/replicate` — runs alongside Base/SQLite services:

```yaml
spec:
  containers:
    - name: liquid-bd
      volumeMounts:
        - name: data
          mountPath: /data        # FUSE-mounted VFS
    - name: vfs-sidecar
      image: ghcr.io/hanzoai/vfs:0.1.0
      args:
        - mount
        - /data
        - --backend=s3://liquidity-vfs/{env}/bd
        - --age-key=/etc/vfs/age.key
      securityContext:
        privileged: true       # FUSE needs CAP_SYS_ADMIN
      volumeMounts:
        - name: data
          mountPath: /data
          mountPropagation: Bidirectional
        - name: vfs-cache
          mountPath: /var/cache/vfs
        - name: vfs-keys
          mountPath: /etc/vfs
          readOnly: true
  volumes:
    - name: data
      emptyDir: {}              # the VFS overlays this
    - name: vfs-cache
      ephemeral:
        volumeClaimTemplate:
          spec:
            accessModes: [ReadWriteOnce]
            storageClassName: premium-rwo
            resources:
              requests:
                storage: 10Gi   # NVMe hot tier
    - name: vfs-keys
      secret:
        secretName: vfs-age-keys
```

## Backend URL grammar

| Scheme | Form | Notes |
|---|---|---|
| `file://` | `file:///tmp/store` | Local dev; no network. |
| `s3://` | `s3://bucket/prefix` | Region from `AWS_REGION` env or IRSA. Endpoint override via `AWS_ENDPOINT_URL` for S3-compat (Hanzo Storage, MinIO, R2). |
| `gcs://` | `gcs://bucket/prefix` | (TODO) |
| `azureblob://` | `azureblob://account/container/prefix` | (TODO) |

## Encryption

Each 4 KiB block is age-encrypted with one or more recipients (X25519 +
optional ML-KEM-768 for hybrid PQ). Recipients are configured at mount
time via `--age-recipient` (repeatable). The decryption key is loaded
from `--age-key` (file path) or `VFS_AGE_KEY` env var.

Hybrid PQ recipients use `luxfi/age` extensions (see
`~/work/lux/age/internal/x25519` and `~/work/lux/age/pq.go`). Plain
classical X25519 also works; mix-and-match is supported.

## Build tags

- `fuse` — compile FUSE mount support (requires `bazil.org/fuse`,
  Linux/macOS; not Windows)
- `wasi` — compile WASI guest mode (TODO)

Default `go build` produces a CLI that supports `put`/`get`/`stats` but
not `mount`. Use `make build-fuse` for the full mount-capable binary.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0.1.0 | Block layer, file:// + s3 backends, age PQ crypto, LRU cache, CLI (put/get/stats), tests | ✅ shipped |
| 0.2.0 | Multi-block File API (ReadAt/WriteAt/Truncate/Sync), FS with inode tree + dirs, persisted metadata blob, **SQLite roundtrip proven** (20 KiB DB → encrypted blocks → restore → `PRAGMA integrity_check ok`) | ✅ shipped |
| 0.3.0 | bazil.org/fuse mount: kernel POSIX VFS calls land at File.{ReadAt,WriteAt,Sync,Truncate}; SQLite opens DB directly on the mountpoint with no copy step. Linux + macOS. | next |
| 0.4.0 | NVMe disk write-back cache: spill LRU evictions to a local fs cache (configurable via `--cache-dir /var/cache/vfs --cache-size 10Gi`). Survives process restarts. | |
| 0.5.0 | gcs + azureblob backends | |
| 0.6.0 | K8s sidecar mode + Helm chart | |
| 0.7.0 | WASI guest mode (browser/dev embedding) | |
| 1.0.0 | Production-hardened: chunked uploads, multipart resume, GC for orphan blocks (block reference-count map at `metadata/refs.zap.age`), metrics (Prometheus / OTel) + SLO targets | |

## Rules

1. NEVER store plaintext blocks on the backend. Encryption happens
   before write, decryption after read; the backend interface only
   sees ciphertext bytes.
2. NEVER allocate disk space proactively for the backend. The whole
   point is "unlimited" — let S3 deal with capacity.
3. NEVER mix block sizes within one filesystem. 4 KiB is canonical;
   future formats bump the on-disk magic byte.
4. NEVER skip age recipient verification on read. A block decryption
   failure is a hard error, not a silent zero.
5. CLAUDE.md / AGENTS.md are symlinks to LLM.md — never commit them.
