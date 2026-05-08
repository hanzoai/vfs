//go:build fuse && !windows

package mount

import (
	"context"
	"fmt"

	"github.com/hanzoai/vfs/pkg/vfs"
)

// Mount mounts a VFS at the given mountpoint via FUSE. Requires the
// `fuse` build tag and the bazil.org/fuse runtime (Linux/macOS).
//
// TODO(0.2.0): full FUSE filesystem implementation. This stub
// compiles under the `fuse` tag but returns a not-yet-implemented
// error. Wiring requires:
//   1. bazil.org/fuse + bazil.org/fuse/fs imports (added to go.mod
//      under the fuse tag — keep the default build dependency-light)
//   2. an FS root + Node implementations that map POSIX ops to
//      vfs.PutBlock/GetBlock with a per-file inode + extent index
//      stored in the same VFS as a special "metadata/" key prefix
//   3. proper handling of unmount on Ctrl-C + SIGTERM
//   4. tests against a tmpfs-backed file:// VFS on Linux CI
func Mount(ctx context.Context, v *vfs.VFS, mountpoint string) error {
	_ = ctx
	_ = v
	_ = mountpoint
	return fmt.Errorf("mount: FUSE implementation pending (0.2.0 milestone). " +
		"PutBlock/GetBlock work today via the CLI; mount-as-filesystem ships next")
}
