//go:build !fuse || !linux

package writeback

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/hanzoai/vfs"
)

// FUSEEngine selects the Linux FUSE engine when built with -tags fuse.
type FUSEEngine string

const (
	EngineJacobsa FUSEEngine = "jacobsa"
	EngineBazil   FUSEEngine = "bazil"
)

// FUSEStats exposes live atomic counters observed by the FUSE daemon process.
type FUSEStats struct {
	WriteRPCs         atomic.Int64
	WriteBytes        atomic.Int64
	MaxWriteRPCSize   atomic.Int64
	ReadRPCs          atomic.Int64
	ReadBytes         atomic.Int64
	FsyncRPCs         atomic.Int64
	FlushRPCs         atomic.Int64
	SyncS3PutsInFlush atomic.Int64
}

// FUSEStatsSnapshot is a JSON-serializable snapshot of FUSEStats.
type FUSEStatsSnapshot struct {
	WriteRPCs         int64 `json:"write_rpcs"`
	WriteBytes        int64 `json:"write_bytes"`
	MaxWriteRPCSize   int64 `json:"max_write_rpc_size"`
	ReadRPCs          int64 `json:"read_rpcs"`
	ReadBytes         int64 `json:"read_bytes"`
	FsyncRPCs         int64 `json:"fsync_rpcs"`
	FlushRPCs         int64 `json:"flush_rpcs"`
	SyncS3PutsInFlush int64 `json:"sync_s3_puts_in_flush"`
}

// Snapshot returns a point-in-time copy of FUSEStats.
func (s *FUSEStats) Snapshot() FUSEStatsSnapshot {
	if s == nil {
		return FUSEStatsSnapshot{}
	}
	return FUSEStatsSnapshot{
		WriteRPCs:         s.WriteRPCs.Load(),
		WriteBytes:        s.WriteBytes.Load(),
		MaxWriteRPCSize:   s.MaxWriteRPCSize.Load(),
		ReadRPCs:          s.ReadRPCs.Load(),
		ReadBytes:         s.ReadBytes.Load(),
		FsyncRPCs:         s.FsyncRPCs.Load(),
		FlushRPCs:         s.FlushRPCs.Load(),
		SyncS3PutsInFlush: s.SyncS3PutsInFlush.Load(),
	}
}

// MountOptions configures a Linux FUSE mount with explicit writeback policy.
type MountOptions struct {
	Config         WritebackConfig
	Engine         FUSEEngine
	Stats          *FUSEStats
	BackendCounter *CountingBackend
	LifecycleEvent LifecycleEvent
	OnReady        func()
	OnBarrier      func(BarrierReport)
	EnableGuard    bool
}

// Mount returns a build-tag error when built without -tags fuse on Linux.
func Mount(_ context.Context, _ *vfs.Volume, _ string, _ MountOptions) error {
	return fmt.Errorf("writeback: FUSE mount requires Linux with `-tags fuse`")
}
