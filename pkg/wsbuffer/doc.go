// Package wsbuffer implements partial-write absorption with aligned scrap
// buffers and zero-copy direct bypass for large aligned writes, inspired by
// WSBuffer (Zhan et al., USENIX FAST 2026,
// https://www.usenix.org/conference/fast26/presentation/zhan).
//
// This is a clean-room Apache-2.0 Go implementation written from first
// principles using only the Go standard library and Hanzo VFS's existing
// journal package (github.com/hanzoai/vfs/journal), with zero new external
// dependencies.
//
// # Hybrid Dual-Path Architecture
//
// Incoming WriteAt(ctx, p, off) operations are routed dynamically based on
// alignment and size:
//
//  1. Scrap-Buffer Absorption Path (small or unaligned writes):
//     Writes with len(p) < BypassThreshold, off%PageSize != 0, or
//     len(p)%PageSize != 0 are absorbed into fixed-size aligned scrap buffer
//     slabs tracked by a per-page extent index. Repeated updates to the same
//     logical page or overlapping sub-page ranges coalesce in-place before
//     flushing, eliminating redundant WAL records, partial-sector padding
//     bloat, and scattered syscalls. On flush, dirty extents are sorted by
//     offset, emitted as a single CRC32C-protected WAL batch frame, and
//     applied to the backing file with contiguous-run coalescing.
//
//  2. Zero-Copy Direct Bypass Path (large aligned writes):
//     Writes with off%PageSize == 0, len(p)%PageSize == 0, and
//     len(p) >= BypassThreshold bypass the scrap buffer completely with zero
//     extra memcpy. Any live unflushed scrap extents covered by [off, off+len)
//     are invalidated immediately (saving dead write I/O), and if older
//     uncheckpointed WAL frames exist, a compact 56-byte zero-payload
//     FrameDirectBarrier record is appended to the WAL so crash recovery never
//     replays stale scrap bytes over a newer direct write.
//
// # Correctness and Isolation
//
//   - Linearizable ReadAt merges committed backing pages with live scrap
//     overlays in exact sequence order.
//   - Crash recovery validates frame headers and payloads with CRC32C
//     (Castagnoli), discards torn tails cleanly, and respects direct-bypass
//     barriers.
//   - Bounded memory caps (MaxScrapBytes per store, GlobalMaxScrapBytes in
//     Manager) trigger deterministic backpressure flushes and isolate volumes
//     by VolumeID.
package wsbuffer
