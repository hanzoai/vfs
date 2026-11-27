//go:build !fuse || !linux

package mount

import (
	"context"
	"fmt"

	"github.com/hanzoai/vfs"
)

// MountWriteback requires Linux built with `-tags fuse`.
func MountWriteback(_ context.Context, _ *vfs.Volume, _ string, _ WritebackOptions) error {
	return fmt.Errorf("mount: MountWriteback requires Linux built with `-tags fuse`")
}
