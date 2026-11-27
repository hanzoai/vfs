// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build !fuse || !linux

package gitvfs

import (
	"context"
	"errors"
	"time"
)

// ErrFUSEUnavailable is returned when FUSE mounting is requested on a binary
// built without `-tags fuse` on Linux.
var ErrFUSEUnavailable = errors.New("gitvfs: FUSE mount requires Linux with -tags fuse")

// FUSEMountAvailable reports false when built without `-tags fuse` on Linux.
func FUSEMountAvailable() bool { return false }

// MountMetricsSnapshot mirrors the FUSE mount server metrics payload.
type MountMetricsSnapshot struct {
	Backend       BackendMetrics `json:"backend"`
	Journal       JournalMetrics `json:"journal"`
	VolumeBacklog int64          `json:"volume_backlog"`
	VolumeDirty   int64          `json:"volume_dirty"`
	VolumePending int            `json:"volume_pending"`
	VolumeSeq     uint64         `json:"volume_seq"`
	VolumeDegrade bool           `json:"volume_degraded"`
}

// ServeInstrumentedMount returns ErrFUSEUnavailable without -tags fuse.
func ServeInstrumentedMount(context.Context, string, string, string, string, uint64, time.Duration, string) error {
	return ErrFUSEUnavailable
}

// MountedVolumeClient stub for non-FUSE builds.
type MountedVolumeClient struct {
	Mountpoint string
}

// StartMountedVolumeChild returns ErrFUSEUnavailable without -tags fuse.
func StartMountedVolumeChild(string, []string, string, string) (*MountedVolumeClient, error) {
	return nil, ErrFUSEUnavailable
}

func (*MountedVolumeClient) Metrics() (MountMetricsSnapshot, error) {
	return MountMetricsSnapshot{}, ErrFUSEUnavailable
}
func (*MountedVolumeClient) Flush() error { return ErrFUSEUnavailable }
func (*MountedVolumeClient) Stop() error  { return ErrFUSEUnavailable }
