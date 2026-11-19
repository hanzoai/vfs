package vfs

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// Distributor handles fetching blocks from multiple independent mirrors.
type Distributor struct {
	Backends []FS // List of VFS backends (S3, File, etc.)
	Cache    FS   // Local hot tier/cache
}

// NewDistributor initializes a distributor with multiple providers.
func NewDistributor(cache FS, mirrors ...FS) *Distributor {
	return &Distributor{
		Backends: mirrors,
		Cache:    cache,
	}
}

// FetchBlock attempts to retrieve a block from the cache or any available mirror.
func (d *Distributor) FetchBlock(ctx context.Context, hash string) ([]byte, error) {
	// 1. Check Cache (Resume logic)
	if data, err := d.Cache.GetBlock(ctx, hash); err == nil {
		return data, nil
	}

	// 2. Try mirrors in parallel or sequence
	for _, backend := range d.Backends {
		data, err := backend.GetBlock(ctx, hash)
		if err != nil {
			continue
		}

		// Verify hash immediately (Reject modified chunks)
		if HashBlock(data) != hash {
			continue // Try next mirror if this one is corrupted
		}

		// Save to cache for future resumption
		_ = d.Cache.PutBlock(ctx, data)
		return data, nil
	}

	return nil, fmt.Errorf("block %s not found in any provider", hash)
}

// ReconstructFile pulls all blocks for a file entry and writes to a destination.
func (d *Distributor) ReconstructFile(ctx context.Context, entry FileEntry, dest io.Writer) error {
	for i, hash := range entry.Hashes {
		data, err := d.FetchBlock(ctx, hash)
		if err != nil {
			return fmt.Errorf("failed at block %d (%s): %w", i, hash, err)
		}
		if _, err := dest.Write(data); err != nil {
			return err
		}
	}
	return nil
}
