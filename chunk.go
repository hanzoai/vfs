package vfs

import (
	"encoding/binary"
	"github.com/hanzoai/vfs/internal/blake3"
)

// Chunk sizes. A file's bytes are cut into content-defined chunks between
// minChunk and maxChunk, averaging near avgChunk.
//
// The cut is FastCDC (Xia et al., USENIX ATC 2016): a gear rolling hash decides,
// byte by byte, whether the hash of the last 64 bytes matches a mask. Two masks
// give "normalized chunking": a stricter one up to avgChunk, a looser one after,
// which pulls the size distribution toward the average and keeps most chunks
// large. Why this one: a cut depends only on the bytes since the previous cut, so
// after an edit the cuts re-synchronize within a few chunks and everything past
// them keeps its ids. An edit therefore rewrites only the chunks it touched,
// where fixed-size blocks would rewrite every block after an insertion. FastCDC
// skips the first minChunk bytes unhashed, so it is also several times cheaper
// than a plain rolling hash.
//
// The gear derivation below and the masks are part of the on-disk layout:
// changing either changes every cut and so every chunk id.
const (
	minChunk = 64 << 10
	avgChunk = 512 << 10
	maxChunk = 4 << 20

	// avgBits is log2(avgChunk). The strict mask has two more bits, the loose
	// mask two fewer. Masks use the top bits of the hash, which depend on all 64
	// bytes of the window.
	avgBits = 19
	maskS   = (1<<(avgBits+2) - 1) << (64 - (avgBits + 2))
	maskL   = (1<<(avgBits-2) - 1) << (64 - (avgBits - 2))
)

// gear maps a byte to 64 random bits. Each Volume has its own table, drawn from
// blake3's extendable output over its seed (stored in the Head, so it is as secret
// as the Volume), which makes every cut point of a Volume unpredictable from
// outside: the lengths of a file's chunks cannot be matched against the chunks of
// a known file in another Volume. The derivation is part of the on-disk layout.
type gear [256]uint64

func newGear(seed [32]byte) *gear {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs chunker gear table")
	_, _ = h.Write(seed[:])
	var raw [256 * 8]byte
	h.Expand(raw[:])
	var g gear
	for i := range g {
		g[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	return &g
}

// cut returns the length of the first chunk of b. b is the next min(maxChunk,
// remaining) bytes of the file, so a result shorter than len(b) is a content
// cut and a result equal to len(b) is the window's end (the file's end, or
// maxChunk). The result depends only on b[:result].
func cut(g *gear, b []byte) int {
	n := len(b)
	if n <= minChunk {
		return n
	}
	mid := min(avgChunk, n)
	var fp uint64
	i := minChunk
	for ; i < mid; i++ {
		fp = fp<<1 + g[b[i]]
		if fp&maskS == 0 {
			return i + 1
		}
	}
	for ; i < n; i++ {
		fp = fp<<1 + g[b[i]]
		if fp&maskL == 0 {
			return i + 1
		}
	}
	return n
}

// chunkID names a chunk by the blake3 digest of its plaintext. Ids appear only
// inside sealed shards, so the backend never sees them; within one Volume equal
// ids mean equal bytes, which is what dedupe uses. Volumes do not share chunks.
type chunkID [32]byte

func sumChunk(p []byte) chunkID { return blake3.Sum(p) }
