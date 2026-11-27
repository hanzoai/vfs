//go:build !linux

package writeback

import (
	"errors"
	"syscall"
	"time"
)

// SyncFS falls back to a global syscall.Sync on platforms without per-mount
// syncfs(2) (e.g. macOS).
func SyncFS(mountpoint string) error {
	if mountpoint == "" {
		return ErrInvalidConfig
	}
	syscall.Sync()
	return nil
}

// KernelDirtySettings summarizes global kernel dirty page-cache thresholds.
type KernelDirtySettings struct {
	DirtyRatio             int64         `json:"dirty_ratio"`
	DirtyBackgroundRatio   int64         `json:"dirty_background_ratio"`
	DirtyBytes             int64         `json:"dirty_bytes"`
	DirtyBackgroundBytes   int64         `json:"dirty_background_bytes"`
	DirtyExpireDuration    time.Duration `json:"dirty_expire_duration"`
	DirtyWritebackInterval time.Duration `json:"dirty_writeback_interval"`
}

// InspectKernelDirtySettings returns ErrUnsupported on non-Linux platforms.
func InspectKernelDirtySettings() (KernelDirtySettings, error) {
	return KernelDirtySettings{}, errors.ErrUnsupported
}

// BDISettings summarizes per-mount backing-device-info limits.
type BDISettings struct {
	Device      string `json:"device"`
	MaxRatio    int64  `json:"max_ratio"`
	MinRatio    int64  `json:"min_ratio"`
	StrictLimit int64  `json:"strict_limit"`
	MaxBytes    int64  `json:"max_bytes,omitempty"`
}

// InspectMountBDI returns ErrUnsupported on non-Linux platforms.
func InspectMountBDI(_ string) (BDISettings, error) {
	return BDISettings{}, errors.ErrUnsupported
}
