// Package vfs is the top-level virtual filesystem implementation.
//
// Block layer: 4 KiB pages, content-addressable via blake3 (256-bit).
// Block IDs are the lowercase hex of the blake3 digest of the
// post-encryption ciphertext, so identical plaintext blocks dedupe IFF
// they share the same recipient set + ephemeral key. (For practical
// dedup across a fleet, derive the encryption key deterministically
// from a domain salt — see BlockKeyMode.)
package vfs

import (
	"crypto/subtle"
	"encoding/hex"
	"fmt"

	"github.com/zeebo/blake3"
)

// BlockSize is the canonical page size. Bumping requires a new on-disk
// format magic byte; do not change without coordination.
const BlockSize = 4096

// BlockID is the content hash of an encrypted block (blake3-256, hex).
type BlockID string

// HashBlock returns the BlockID for the given ciphertext bytes.
func HashBlock(ciphertext []byte) BlockID {
	sum := blake3.Sum256(ciphertext)
	return BlockID(hex.EncodeToString(sum[:]))
}

// Verify checks that the given ciphertext hashes to the expected ID.
// Returns nil on match, an error on mismatch. Constant-time compare
// guards against timing leaks on the hash itself (defense-in-depth;
// blake3 is already collision-resistant).
func (id BlockID) Verify(ciphertext []byte) error {
	got := HashBlock(ciphertext)
	if subtle.ConstantTimeCompare([]byte(id), []byte(got)) != 1 {
		return fmt.Errorf("block: hash mismatch: have %s, computed %s", id, got)
	}
	return nil
}

// Path returns the canonical backend key for a block ID. Pattern:
//
//	blocks/<first-2-hex>/<full-hash>.zap.age
//
// The 2-hex shard prefix (256 fan-out) keeps single-directory listings
// tractable for object stores that paginate.
func (id BlockID) Path() string {
	if len(id) < 2 {
		return "blocks/" + string(id) + ".zap.age"
	}
	return "blocks/" + string(id[:2]) + "/" + string(id) + ".zap.age"
}

// Pad zero-pads or truncates a payload to BlockSize. Plaintext blocks
// are always exactly BlockSize bytes before encryption so that block
// boundaries don't leak file lengths to the backend (each block in
// isolation looks identical in size after age framing — a few hundred
// bytes of header + tag + BlockSize body).
func Pad(plaintext []byte) []byte {
	if len(plaintext) == BlockSize {
		return plaintext
	}
	out := make([]byte, BlockSize)
	if len(plaintext) < BlockSize {
		copy(out, plaintext)
		return out
	}
	copy(out, plaintext[:BlockSize])
	return out
}
