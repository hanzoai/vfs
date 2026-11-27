package mount

import (
	"context"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/writeback"
)

// WritebackOptions configures a FUSE mount with HIP-1342 #13 writeback caching,
// 1 MiB max_write negotiation, dirty page-cache bounds, and lifecycle flush
// barriers.
type WritebackOptions struct {
	Config           writeback.WritebackConfig
	EnableDirtyGuard bool
	LifecycleEvent   writeback.LifecycleEvent
	Stats            *writeback.FUSEStats
	OnReady          func()
	OnBarrier        func(writeback.BarrierReport)
}

// FlushForLifecycle enforces the mandatory HIP-1342 #13 lifecycle barrier
// (kernel syncfs -> daemon journal sync -> final object-store publication)
// prior to Detach, Suspend, or End.
func FlushForLifecycle(ctx context.Context, vol *vfs.Volume, mountpoint string, event writeback.LifecycleEvent) (writeback.BarrierReport, error) {
	barrier, err := writeback.NewVolumeBarrier(writeback.DefaultConfig(), vol)
	if err != nil {
		return writeback.BarrierReport{}, err
	}
	return barrier.Execute(ctx, event, mountpoint)
}
