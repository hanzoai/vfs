// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
)

// WireStats holds real, non-synthetic request and payload counters observed
// directly at the backend.Backend boundary.
type WireStats struct {
	Puts            int64
	PutIfAbsents    int64
	PutIfVersions   int64
	Gets            int64
	GetRanges       int64
	Stats           int64
	Deletes         int64
	Lists           int64
	Conflicts412    int64
	ReusedObjects   int64
	ReusedBytes     int64
	BytesUploaded   int64
	BytesDownloaded int64
}

// TotalRequests returns the sum of all object-store operations issued to the backend.
func (s WireStats) TotalRequests() int64 {
	return s.Puts + s.PutIfAbsents + s.PutIfVersions + s.Gets + s.GetRanges + s.Stats + s.Deletes + s.Lists
}

// TotalWriteRequests returns the sum of mutating object-store PUT operations.
func (s WireStats) TotalWriteRequests() int64 {
	return s.Puts + s.PutIfAbsents + s.PutIfVersions
}

// TotalReadRequests returns the sum of Get, GetRange, and Stat requests.
func (s WireStats) TotalReadRequests() int64 {
	return s.Gets + s.GetRanges + s.Stats
}

// CountingBackend wraps any backend.Backend and measures actual requests,
// conditional write conflicts, immutable reuses, and wire bytes.
type CountingBackend struct {
	be     backend.Backend
	simRTT time.Duration

	puts            atomic.Int64
	putIfAbsents    atomic.Int64
	putIfVersions   atomic.Int64
	gets            atomic.Int64
	getRanges       atomic.Int64
	stats           atomic.Int64
	deletes         atomic.Int64
	lists           atomic.Int64
	conflicts412    atomic.Int64
	reusedObjects   atomic.Int64
	reusedBytes     atomic.Int64
	bytesUploaded   atomic.Int64
	bytesDownloaded atomic.Int64
}

// NewCountingBackend wraps be with optional simulated per-request RTT (0 for native timing).
func NewCountingBackend(be backend.Backend, simRTT time.Duration) *CountingBackend {
	return &CountingBackend{be: be, simRTT: simRTT}
}

// Reset zeroes all counters while leaving underlying objects untouched.
func (c *CountingBackend) Reset() {
	c.puts.Store(0)
	c.putIfAbsents.Store(0)
	c.putIfVersions.Store(0)
	c.gets.Store(0)
	c.getRanges.Store(0)
	c.stats.Store(0)
	c.deletes.Store(0)
	c.lists.Store(0)
	c.conflicts412.Store(0)
	c.reusedObjects.Store(0)
	c.reusedBytes.Store(0)
	c.bytesUploaded.Store(0)
	c.bytesDownloaded.Store(0)
}

// Snapshot returns the accumulated wire counters.
func (c *CountingBackend) Snapshot() WireStats {
	return WireStats{
		Puts:            c.puts.Load(),
		PutIfAbsents:    c.putIfAbsents.Load(),
		PutIfVersions:   c.putIfVersions.Load(),
		Gets:            c.gets.Load(),
		GetRanges:       c.getRanges.Load(),
		Stats:           c.stats.Load(),
		Deletes:         c.deletes.Load(),
		Lists:           c.lists.Load(),
		Conflicts412:    c.conflicts412.Load(),
		ReusedObjects:   c.reusedObjects.Load(),
		ReusedBytes:     c.reusedBytes.Load(),
		BytesUploaded:   c.bytesUploaded.Load(),
		BytesDownloaded: c.bytesDownloaded.Load(),
	}
}

// RecordReuse records an immutable object verified as already resident on the store.
func (c *CountingBackend) RecordReuse(n int64) {
	c.reusedObjects.Add(1)
	c.reusedBytes.Add(n)
}

func (c *CountingBackend) waitRTT(ctx context.Context) error {
	if c.simRTT <= 0 {
		return nil
	}
	select {
	case <-time.After(c.simRTT):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *CountingBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	c.gets.Add(1)
	if err := c.waitRTT(ctx); err != nil {
		return nil, "", err
	}
	data, ver, err := c.be.Get(ctx, key, max)
	if err == nil {
		c.bytesDownloaded.Add(int64(len(data)))
	}
	return data, ver, err
}

func (c *CountingBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	c.getRanges.Add(1)
	if err := c.waitRTT(ctx); err != nil {
		return nil, err
	}
	data, err := c.be.GetRange(ctx, key, off, n)
	if err == nil {
		c.bytesDownloaded.Add(int64(len(data)))
	}
	return data, err
}

func (c *CountingBackend) Put(ctx context.Context, key string, data []byte) error {
	c.puts.Add(1)
	if err := c.waitRTT(ctx); err != nil {
		return err
	}
	if err := c.be.Put(ctx, key, data); err != nil {
		if errors.Is(err, backend.ErrConflict) {
			c.conflicts412.Add(1)
		}
		return err
	}
	c.bytesUploaded.Add(int64(len(data)))
	return nil
}

func (c *CountingBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	c.putIfAbsents.Add(1)
	if err := c.waitRTT(ctx); err != nil {
		return "", err
	}
	ver, err := c.be.PutIfAbsent(ctx, key, data)
	if err != nil {
		if errors.Is(err, backend.ErrConflict) {
			c.conflicts412.Add(1)
		}
		return "", err
	}
	c.bytesUploaded.Add(int64(len(data)))
	return ver, nil
}

func (c *CountingBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	c.putIfVersions.Add(1)
	if err := c.waitRTT(ctx); err != nil {
		return "", err
	}
	ver, err := c.be.PutIfVersion(ctx, key, data, v)
	if err != nil {
		if errors.Is(err, backend.ErrConflict) {
			c.conflicts412.Add(1)
		}
		return "", err
	}
	c.bytesUploaded.Add(int64(len(data)))
	return ver, nil
}

func (c *CountingBackend) Delete(ctx context.Context, key string) error {
	c.deletes.Add(1)
	if err := c.waitRTT(ctx); err != nil {
		return err
	}
	return c.be.Delete(ctx, key)
}

func (c *CountingBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	c.lists.Add(1)
	return c.be.List(ctx, prefix)
}

func (c *CountingBackend) Stat(ctx context.Context, key string) (int64, error) {
	c.stats.Add(1)
	if err := c.waitRTT(ctx); err != nil {
		return 0, err
	}
	return c.be.Stat(ctx, key)
}

func (c *CountingBackend) Close() error {
	return c.be.Close()
}

func (c *CountingBackend) String() string {
	return c.be.String()
}

var _ backend.Backend = (*CountingBackend)(nil)
