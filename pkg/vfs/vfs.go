package vfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/hanzoai/vfs/pkg/backend"
)

// VFS is the top-level virtual filesystem handle. It wraps a Backend
// (object store) and a Crypto (per-block age encryption), with an LRU
// cache of decrypted blocks in front.
//
// Concurrency: all methods are safe for concurrent use.
type VFS struct {
	be     backend.Backend
	crypto *Crypto
	cache  *Cache
}

// Config holds construction parameters.
type Config struct {
	Backend  backend.Backend
	Crypto   *Crypto
	CacheMax int64 // bytes; <=0 disables eviction (unbounded)
}

// New constructs a VFS. Backend + Crypto are required.
func New(cfg Config) (*VFS, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("vfs: backend required")
	}
	if cfg.Crypto == nil {
		return nil, fmt.Errorf("vfs: crypto required")
	}
	return &VFS{
		be:     cfg.Backend,
		crypto: cfg.Crypto,
		cache:  NewCache(cfg.CacheMax),
	}, nil
}

// PutBlock encrypts a plaintext block and writes it to the backend.
// Returns the BlockID for later GetBlock.
//
// The plaintext is zero-padded to BlockSize before encryption (see
// vfs/block.go::Pad) so that block boundaries don't leak file lengths.
func (v *VFS) PutBlock(ctx context.Context, plaintext []byte) (BlockID, error) {
	if !v.crypto.HasRecipients() {
		return "", fmt.Errorf("vfs.PutBlock: crypto has no recipients (read-only mode)")
	}
	padded := Pad(plaintext)
	ct, err := v.crypto.Encrypt(padded)
	if err != nil {
		return "", fmt.Errorf("vfs.PutBlock: encrypt: %w", err)
	}
	id := HashBlock(ct)
	if err := v.be.Put(ctx, id.Path(), ct); err != nil {
		return "", fmt.Errorf("vfs.PutBlock: backend.Put: %w", err)
	}
	v.cache.Put(id, padded)
	return id, nil
}

// GetBlock fetches a block by ID, decrypts, and returns the plaintext
// (still zero-padded to BlockSize — the caller is responsible for
// remembering the logical size).
func (v *VFS) GetBlock(ctx context.Context, id BlockID) ([]byte, error) {
	if cached, ok := v.cache.Get(id); ok {
		return cached, nil
	}
	if !v.crypto.HasIdentities() {
		return nil, fmt.Errorf("vfs.GetBlock: crypto has no identities (write-only mode)")
	}
	ct, err := v.be.Get(ctx, id.Path())
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("vfs.GetBlock: backend.Get: %w", err)
	}
	if err := id.Verify(ct); err != nil {
		return nil, fmt.Errorf("vfs.GetBlock: integrity: %w", err)
	}
	pt, err := v.crypto.Decrypt(ct)
	if err != nil {
		return nil, fmt.Errorf("vfs.GetBlock: decrypt: %w", err)
	}
	v.cache.Put(id, pt)
	return pt, nil
}

// Delete removes a block from the backend (and the cache).
func (v *VFS) Delete(ctx context.Context, id BlockID) error {
	v.cache.Delete(id)
	return v.be.Delete(ctx, id.Path())
}

// Stats returns cache + backend metadata.
type Stats struct {
	Backend     string
	CacheBlocks int
	CacheBytes  int64
	CacheMax    int64
}

// Stats returns aggregate counters for observability.
func (v *VFS) Stats() Stats {
	entries, bytesIn, maxBytes := v.cache.Stats()
	return Stats{
		Backend:     v.be.String(),
		CacheBlocks: entries,
		CacheBytes:  bytesIn,
		CacheMax:    maxBytes,
	}
}

// Close releases the backend.
func (v *VFS) Close() error { return v.be.Close() }
