//go:build linux

package writeback

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// SyncFS forces the Linux kernel to flush every dirty page-cache page belonging
// to the filesystem mounted at mountpoint down to the FUSE daemon via FUSE_WRITE
// RPCs before returning.
//
// The directory is opened directly with syscall.Open rather than os.Open so the
// FUSE daemon process never registers a descriptor of its own mount with Go's
// epoll netpoller (which would otherwise deadlock on readiness polling).
func SyncFS(mountpoint string) error {
	if mountpoint == "" {
		return fmt.Errorf("%w: empty mountpoint for syncfs", ErrInvalidConfig)
	}
	fd, err := syscall.Open(mountpoint, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("syncfs open %q: %w", mountpoint, err)
	}
	defer syscall.Close(fd)
	if err := unix.Syncfs(fd); err != nil {
		return fmt.Errorf("syncfs(%q): %w", mountpoint, err)
	}
	return nil
}

// KernelDirtySettings summarizes Linux's global VM dirty page-cache thresholds
// read from /proc/sys/vm/dirty_*. Under FUSE_WRITEBACK_CACHE, these defaults
// (typically dirty_ratio=20%, dirty_expire=30s) determine how much data can
// linger in volatile kernel memory if not bounded by BDI limits or syncfs.
type KernelDirtySettings struct {
	DirtyRatio             int64         `json:"dirty_ratio"`
	DirtyBackgroundRatio   int64         `json:"dirty_background_ratio"`
	DirtyBytes             int64         `json:"dirty_bytes"`
	DirtyBackgroundBytes   int64         `json:"dirty_background_bytes"`
	DirtyExpireDuration    time.Duration `json:"dirty_expire_duration"`
	DirtyWritebackInterval time.Duration `json:"dirty_writeback_interval"`
}

// InspectKernelDirtySettings reads Linux /proc/sys/vm/dirty_* sysctls.
func InspectKernelDirtySettings() (KernelDirtySettings, error) {
	readInt := func(name string) (int64, error) {
		raw, err := os.ReadFile(filepath.Join("/proc/sys/vm", name))
		if err != nil {
			return 0, err
		}
		return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	}
	var s KernelDirtySettings
	var err error
	if s.DirtyRatio, err = readInt("dirty_ratio"); err != nil {
		return s, err
	}
	if s.DirtyBackgroundRatio, err = readInt("dirty_background_ratio"); err != nil {
		return s, err
	}
	if s.DirtyBytes, err = readInt("dirty_bytes"); err != nil {
		return s, err
	}
	if s.DirtyBackgroundBytes, err = readInt("dirty_background_bytes"); err != nil {
		return s, err
	}
	expCS, err := readInt("dirty_expire_centisecs")
	if err != nil {
		return s, err
	}
	wbCS, err := readInt("dirty_writeback_centisecs")
	if err != nil {
		return s, err
	}
	s.DirtyExpireDuration = time.Duration(expCS) * 10 * time.Millisecond
	s.DirtyWritebackInterval = time.Duration(wbCS) * 10 * time.Millisecond
	return s, nil
}

// BDISettings summarizes per-mount Linux backing-device-info (BDI) limits from
// /sys/class/bdi/<major>:<minor>/ for a mounted FUSE filesystem.
type BDISettings struct {
	Device      string `json:"device"`
	MaxRatio    int64  `json:"max_ratio"`
	MinRatio    int64  `json:"min_ratio"`
	StrictLimit int64  `json:"strict_limit"`
	MaxBytes    int64  `json:"max_bytes,omitempty"`
}

// InspectMountBDI reads Linux BDI dirty throttling parameters for mountpoint.
// Linux sets strict_limit=1 on FUSE BDIs by default so per-mount max_ratio or
// max_bytes can bound writeback exposure independently of global vm.dirty_ratio.
func InspectMountBDI(mountpoint string) (BDISettings, error) {
	var st unix.Stat_t
	if err := unix.Stat(mountpoint, &st); err != nil {
		return BDISettings{}, fmt.Errorf("stat %q: %w", mountpoint, err)
	}
	dev := fmt.Sprintf("%d:%d", unix.Major(uint64(st.Dev)), unix.Minor(uint64(st.Dev)))
	base := filepath.Join("/sys/class/bdi", dev)
	readOpt := func(name string) (int64, error) {
		raw, err := os.ReadFile(filepath.Join(base, name))
		if err != nil {
			return 0, err
		}
		return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	}
	out := BDISettings{Device: dev}
	var err error
	if out.MaxRatio, err = readOpt("max_ratio"); err != nil {
		return out, err
	}
	if out.MinRatio, err = readOpt("min_ratio"); err != nil {
		return out, err
	}
	if out.StrictLimit, err = readOpt("strict_limit"); err != nil {
		return out, err
	}
	if v, err := readOpt("max_bytes"); err == nil {
		out.MaxBytes = v
	}
	return out, nil
}
