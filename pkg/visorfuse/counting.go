// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package visorfuse

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/hanzoai/vfs/pkg/backend"
)

// BackendStats captures exact object-store request and byte counters.
type BackendStats struct {
	Gets          int64 `json:"gets"`
	GetRanges     int64 `json:"get_ranges"`
	Puts          int64 `json:"puts"`
	PutIfAbsents  int64 `json:"put_if_absents"`
	PutIfVersions int64 `json:"put_if_versions"`
	Deletes       int64 `json:"deletes"`
	Lists         int64 `json:"lists"`
	Stats         int64 `json:"stats"`
	BytesRead     int64 `json:"bytes_read"`
	BytesWritten  int64 `json:"bytes_written"`
}

// TotalRequests returns the sum of all object-store RPC requests.
func (s BackendStats) TotalRequests() int64 {
	return s.Gets + s.GetRanges + s.Puts + s.PutIfAbsents + s.PutIfVersions + s.Deletes + s.Lists + s.Stats
}

// Delta returns s - prev.
func (s BackendStats) Delta(prev BackendStats) BackendStats {
	return BackendStats{
		Gets:          s.Gets - prev.Gets,
		GetRanges:     s.GetRanges - prev.GetRanges,
		Puts:          s.Puts - prev.Puts,
		PutIfAbsents:  s.PutIfAbsents - prev.PutIfAbsents,
		PutIfVersions: s.PutIfVersions - prev.PutIfVersions,
		Deletes:       s.Deletes - prev.Deletes,
		Lists:         s.Lists - prev.Lists,
		Stats:         s.Stats - prev.Stats,
		BytesRead:     s.BytesRead - prev.BytesRead,
		BytesWritten:  s.BytesWritten - prev.BytesWritten,
	}
}

// CountingBackend wraps any backend.Backend to count exact S3/object-store
// requests and bytes, and optionally inject post-write lost-ACK faults in tests.
type CountingBackend struct {
	inner backend.Backend

	gets          atomic.Int64
	getRanges     atomic.Int64
	puts          atomic.Int64
	putIfAbsents  atomic.Int64
	putIfVersions atomic.Int64
	deletes       atomic.Int64
	lists         atomic.Int64
	stats         atomic.Int64
	bytesRead     atomic.Int64
	bytesWritten  atomic.Int64

	faultMu        sync.Mutex
	onPutIfAbsent  func(key string, wrote bool) error
	suppressCloses bool
}

// NewCountingBackend wraps inner with atomic request/byte telemetry.
func NewCountingBackend(inner backend.Backend) *CountingBackend {
	return &CountingBackend{inner: inner}
}

// SetSuppressClose prevents Close() from closing the underlying backend when
// the same backend instance is intentionally reused across multi-node tests.
func (c *CountingBackend) SetSuppressClose(suppress bool) {
	c.faultMu.Lock()
	c.suppressCloses = suppress
	c.faultMu.Unlock()
}

// SetPutIfAbsentHook installs an optional post-write fault hook (used to test
// lost-ACK reconciliation where the write succeeded on S3 but the reply failed).
func (c *CountingBackend) SetPutIfAbsentHook(fn func(key string, wrote bool) error) {
	c.faultMu.Lock()
	c.onPutIfAbsent = fn
	c.faultMu.Unlock()
}

// Snapshot returns a point-in-time copy of all counters.
func (c *CountingBackend) Snapshot() BackendStats {
	return BackendStats{
		Gets:          c.gets.Load(),
		GetRanges:     c.getRanges.Load(),
		Puts:          c.puts.Load(),
		PutIfAbsents:  c.putIfAbsents.Load(),
		PutIfVersions: c.putIfVersions.Load(),
		Deletes:       c.deletes.Load(),
		Lists:         c.lists.Load(),
		Stats:         c.stats.Load(),
		BytesRead:     c.bytesRead.Load(),
		BytesWritten:  c.bytesWritten.Load(),
	}
}

func (c *CountingBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	c.gets.Add(1)
	b, v, err := c.inner.Get(ctx, key, max)
	if err == nil {
		c.bytesRead.Add(int64(len(b)))
	}
	return b, v, err
}

func (c *CountingBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	c.getRanges.Add(1)
	b, err := c.inner.GetRange(ctx, key, off, n)
	if err == nil {
		c.bytesRead.Add(int64(len(b)))
	}
	return b, err
}

func (c *CountingBackend) Put(ctx context.Context, key string, data []byte) error {
	c.puts.Add(1)
	err := c.inner.Put(ctx, key, data)
	if err == nil {
		c.bytesWritten.Add(int64(len(data)))
	}
	return err
}

func (c *CountingBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	c.putIfAbsents.Add(1)
	v, err := c.inner.PutIfAbsent(ctx, key, data)
	if err == nil {
		c.bytesWritten.Add(int64(len(data)))
	}
	c.faultMu.Lock()
	hook := c.onPutIfAbsent
	c.faultMu.Unlock()
	if hook != nil && err == nil {
		if ferr := hook(key, true); ferr != nil {
			return "", ferr
		}
	}
	return v, err
}

func (c *CountingBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	c.putIfVersions.Add(1)
	nv, err := c.inner.PutIfVersion(ctx, key, data, v)
	if err == nil {
		c.bytesWritten.Add(int64(len(data)))
	}
	return nv, err
}

func (c *CountingBackend) Delete(ctx context.Context, key string) error {
	c.deletes.Add(1)
	return c.inner.Delete(ctx, key)
}

func (c *CountingBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	c.lists.Add(1)
	return c.inner.List(ctx, prefix)
}

func (c *CountingBackend) Stat(ctx context.Context, key string) (int64, error) {
	c.stats.Add(1)
	return c.inner.Stat(ctx, key)
}

func (c *CountingBackend) Close() error {
	c.faultMu.Lock()
	suppress := c.suppressCloses
	c.faultMu.Unlock()
	if suppress {
		return nil
	}
	return c.inner.Close()
}

func (c *CountingBackend) String() string { return c.inner.String() }
