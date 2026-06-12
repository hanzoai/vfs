// Instance lifecycle + version for the VFS package. Cloud-free: the
// HIP-0106 mount adapter (which wires these into a zip.App) lives in
// cloud/mounts/vfs, but the singleton instance and its accessors are a
// vfs-library concern and stay here so the standalone daemon, tests,
// and any embedder can attach/read a backend without importing cloud.

package vfs

import (
	"context"
	"sync"
)

// Version is overridden at build time via -ldflags
// "-X github.com/hanzoai/vfs.Version=...".
var Version = "dev"

// mountedInstance is the VFS handle the HTTP/data-plane routes operate
// against. nil until SetInstance() is called. Guarded by mu.
var (
	mu              sync.RWMutex
	mountedInstance *VFS
)

// SetInstance attaches a VFS handle for the mounted routes. Calling this
// with nil disables the data-plane routes. Idempotent.
func SetInstance(v *VFS) {
	mu.Lock()
	defer mu.Unlock()
	mountedInstance = v
}

// Instance returns the currently attached VFS handle (or nil).
func Instance() *VFS {
	mu.RLock()
	defer mu.RUnlock()
	return mountedInstance
}

// Shutdown drains the attached VFS instance (closing its backend).
// Idempotent. Safe to call when no instance was attached.
func Shutdown(_ context.Context) error {
	mu.Lock()
	defer mu.Unlock()
	if mountedInstance == nil {
		return nil
	}
	err := mountedInstance.Close()
	mountedInstance = nil
	return err
}
