# vfs

Object-store abstraction with content-addressed, PQ-encrypted block storage. The storage backplane for HIP-0107 streaming replication.

[![Status](https://img.shields.io/badge/status-beta-blue)]()
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)]()

## Quick start

```bash
make build
./bin/vfs put /tmp/in.txt --backend "s3://my-bucket/vfs-prefix?region=us-east-1" \
  --age-recipient "$RECIPIENT" --age-key /tmp/vfs.key
```

## What this is

`vfs` is a 4 KiB block-level virtual filesystem that hashes every block with blake3-256, encrypts every block with `luxfi/age` (X25519, optionally hybrid PQ via ML-KEM-768), and stores them on a pluggable backend (`file://`, `s3://`, `gcs://`, `azureblob://`). In-memory LRU for the hot tier, object store for the cold tier — every Hanzo Go service that needs unlimited write capacity uses `vfs` instead of pre-sizing a PVC. Already exposes `func Mount()` for HIP-0106 inclusion.

## Specs

Implements:
- HIP-0107 Streaming Replication over VFS
- HIP-0106 Unified Cloud Binary (vfs subsystem — already exposes `Mount()`)

Companion to [hanzoai/replicate](https://github.com/hanzoai/replicate): `replicate` streams SQLite WAL frames file-level; `vfs` does block-level for any file.

## Architecture

```
   write()  ->  vfs.PutBlock  ->  blake3-256 hash  ->  luxfi/age encrypt
                                       |
                          blocks/<2-hex-shard>/<full-hash>.zap.age
                                       |
                  file:// | s3:// | gcs:// | azureblob://
                                       |
                          in-memory LRU (hot)
                                       |
                          FUSE mount (lands in 0.2.0)
```


---

# vfs

S3-backed virtual block filesystem with PQ encryption — unlimited write storage for stateful services.

[![Build](https://github.com/hanzoai/vfs/actions/workflows/build.yml/badge.svg)](https://github.com/hanzoai/vfs/actions/workflows/build.yml)

## Quick start

```bash
# Build
make build

# Generate an age key (one-time)
go run filippo.io/age/cmd/age-keygen > /tmp/vfs.key
RECIPIENT=$(grep 'public key' /tmp/vfs.key | cut -d: -f2 | tr -d ' ')

# Put + get a block (file:// backend, dev only)
echo "hello world" > /tmp/in.txt
ID=$(./bin/vfs put /tmp/in.txt --backend file:///tmp/vfs-store --age-recipient "$RECIPIENT" --age-key /tmp/vfs.key)
echo "block: $ID"
./bin/vfs get "$ID" --backend file:///tmp/vfs-store --age-key /tmp/vfs.key

# Same flow against S3
./bin/vfs put /tmp/in.txt \
    --backend "s3://my-bucket/vfs-prefix?region=us-east-1" \
    --age-recipient "$RECIPIENT" \
    --age-key /tmp/vfs.key
```

## What it is

A 4 KiB block-level virtual filesystem that:

- Hashes every block with **blake3-256** for content addressability + integrity.
- Encrypts every block with **luxfi/age** (X25519, optionally hybrid PQ via ML-KEM-768) before it touches the backend.
- Stores blocks as `blocks/<2-hex-shard>/<full-hash>.zap.age` on a pluggable backend (`file://`, `s3://`, `gcs://` (TODO), `azureblob://` (TODO)).
- Caches recently-used blocks in an in-memory LRU (NVMe write-back cache lands in 0.2.0).
- Mounts as a FUSE filesystem (`make build-fuse`; mount layer lands in 0.2.0).

## Why

Stateful services in K8s usually pre-size a PVC and either over-provision or run out of disk. `vfs` lets a service write as if it has unlimited local NVMe — the hot tier is a configurable local cache, the cold tier is S3 (or any object store), and the PQ-encrypted blocks let the operator put cold pages in any region without leaking content.

Companion to [hanzo/replicate](https://github.com/hanzoai/replicate) — `replicate` streams SQLite WAL frames file-level; `vfs` does block-level for any file system.

See [`LLM.md`](./LLM.md) for the full architecture, K8s sidecar pattern, encryption design, and roadmap.

## License

Apache 2.0
