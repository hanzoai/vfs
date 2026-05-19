# Hanzo VFS — ZAP Schema
#
# Server: vfs (Go) embedded via pkg/vfs.Mount(app, deps) per HIP-0106.
#
# This schema captures the minimum public surface needed for the
# Mount() contract: liveness/readiness + single-block CRUD over ZAP.
# The HTTP surface in pkg/vfs/mount.go stays the source of truth for
# /v1/vfs/* until the typed handlers land via zapc generate. Multi-
# block File API + FUSE mount lifecycle stay outside the ZAP surface;
# they're CLI/sidecar concerns documented in pkg/vfs/CLAUDE.md.
#
# Code generation:
#   zapc generate schema/vfs.zap --lang go --out ./gen/zap/

# ── Health ────────────────────────────────────────────────────────────────

struct HealthRequest

struct HealthResponse
  status    Text
  service   Text
  version   Text

struct ReadyResponse
  status   Text
  service  Text
  version  Text
  reason   Text

# ── Blocks ────────────────────────────────────────────────────────────────

struct PutBlockRequest
  payload  Bytes

struct PutBlockResponse
  id  Text

struct GetBlockRequest
  id  Text

struct GetBlockResponse
  payload  Bytes

struct DeleteBlockRequest
  id  Text

struct DeleteBlockResponse
  ok  Bool

# ── Stats ────────────────────────────────────────────────────────────────

struct StatsRequest

struct StatsResponse
  backend       Text
  cacheBlocks   Int32
  cacheBytes    Int64
  cacheMax      Int64

# ── Service interface ────────────────────────────────────────────────────

interface VFSService
  health (request HealthRequest) -> (response HealthResponse)
  ready  (request HealthRequest) -> (response ReadyResponse)

  putBlock    (request PutBlockRequest)    -> (response PutBlockResponse)
  getBlock    (request GetBlockRequest)    -> (response GetBlockResponse)
  deleteBlock (request DeleteBlockRequest) -> (response DeleteBlockResponse)

  stats (request StatsRequest) -> (response StatsResponse)
