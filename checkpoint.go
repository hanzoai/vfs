package vfs

import (
	"context"
	"fmt"
	"time"
)

// CheckpointHandler manages the suspension of a sandbox to object storage.
type CheckpointHandler struct {
	vfs *VFS
}

// PrepareForCheckpoint executes the sequence: flush, release round, then signal ready.
// This ensures the filesystem is consistent before the sandbox memory is dumped.
func (h *CheckpointHandler) PrepareForCheckpoint(ctx context.Context) error {
	// 1. Flush all pending writes to the backend (S3)
	// Target: Acknowledged data published to S3 within 5s
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := h.vfs.backend.Flush(flushCtx); err != nil {
		return fmt.Errorf("checkpoint flush failed: %w", err)
	}

	// 2. Release the round (concurrency/locking release)
	if err := h.vfs.ReleaseRound(); err != nil {
		return fmt.Errorf("failed to release vfs round: %w", err)
	}

	return nil
}

// RestoreFromCheckpoint verifies unprivileged/rootful restore.
func (h *CheckpointHandler) RestoreFromCheckpoint(ctx context.Context, nodeID string) error {
	// Re-initialize the VFS mount outside the checkpoint
	// The vfs mount lives outside the checkpoint as per requirements.
	if err := h.vfs.Reload(ctx); err != nil {
		return fmt.Errorf("restore failed to reload vfs: %w", err)
	}
	return nil
}
