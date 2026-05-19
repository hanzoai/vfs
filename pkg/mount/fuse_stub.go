//go:build !fuse

// Package mount is the FUSE/WASI mount layer. Without the `fuse` build
// tag this is a stub — `vfs mount` returns a clear error pointing at
// `make build-fuse`.
package mount

import (
	"context"
	"fmt"

	"github.com/hanzoai/vfs"
)

// Mount returns a build-tag error. Build with `-tags fuse` to enable.
func Mount(_ context.Context, _ *vfs.FS, _ string) error {
	return fmt.Errorf("mount: this binary was built without FUSE support — use `make build-fuse` or `go build -tags fuse`")
}

// MountWithVFSAdapter is the same stub for the CLI helper.
func MountWithVFSAdapter(_ context.Context, _ *vfs.VFS, _ string) error {
	return Mount(nil, nil, "")
}
