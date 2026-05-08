//go:build !fuse

// Package mount is the FUSE/WASI mount layer. Without the `fuse` build
// tag this is a stub — `vfs mount` returns a clear error pointing at
// `make build-fuse`.
package mount

import (
	"context"
	"fmt"

	"github.com/hanzoai/vfs/pkg/vfs"
)

// Mount returns a build-tag error. Build with `-tags fuse` to enable.
func Mount(ctx context.Context, v *vfs.VFS, mountpoint string) error {
	_ = ctx
	_ = v
	_ = mountpoint
	return fmt.Errorf("mount: this binary was built without FUSE support — use `make build-fuse` or `go build -tags fuse`")
}
