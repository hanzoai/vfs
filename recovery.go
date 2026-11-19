package vfs

import (
	"context"
	"fmt"
	"os"
)

// RecoverSession attempts to re-attach to an existing FUSE mount after a daemon crash.
// It verifies the current Round and ensures the session is resumed without tearing down the guest.
func (v *Volume) RecoverSession(ctx context.Context) error {
	// 1. Verify existing mount status
	if _, err := os.Stat(v.MountPoint); os.IsNotExist(err) {
		return fmt.Errorf("mount point does not exist: %w", err)
	}

	// 2. Re-initialize the FUSE connection using the existing Round
	// This follows the Ananke pattern of resuming the session state
	err := v.attach(ctx, v.Round)
	if err != nil {
		return fmt.Errorf("failed to recover session: %w", err)
	}

	return nil
}
