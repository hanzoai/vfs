//go:build fuse && linux

package mount

import (
	"context"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/writeback"
)

// MountWriteback mounts vol at mountpoint with FUSE writeback caching and
// 1 MiB max_write (FUSE_MAX_PAGES=256) governed by opts, enforcing the
// mandatory syncfs -> journal sync -> final publish barrier on Detach, Suspend,
// or End. Existing Mount(ctx, vol, mountpoint) remains unchanged.
func MountWriteback(ctx context.Context, vol *vfs.Volume, mountpoint string, opts WritebackOptions) error {
	cfg := opts.Config
	if cfg == (writeback.WritebackConfig{}) {
		cfg = writeback.DefaultConfig()
	}
	return writeback.Mount(ctx, vol, mountpoint, writeback.MountOptions{
		Config:         cfg,
		Engine:         writeback.EngineJacobsa,
		Stats:          opts.Stats,
		LifecycleEvent: opts.LifecycleEvent,
		OnReady:        opts.OnReady,
		OnBarrier:      opts.OnBarrier,
		EnableGuard:    opts.EnableDirtyGuard,
	})
}
