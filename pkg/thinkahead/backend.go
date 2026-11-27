// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package thinkahead

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
)

type ctxPrefetchKey struct{}

// WithPrefetchContext marks ctx as belonging to a background prefetch request
// so CountingBackend attributes S3 GET counts and bytes separately from
// foreground demand reads.
func WithPrefetchContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxPrefetchKey{}, true)
}

// IsPrefetchContext reports whether ctx was tagged by WithPrefetchContext.
func IsPrefetchContext(ctx context.Context) bool {
	v, _ := ctx.Value(ctxPrefetchKey{}).(bool)
	return v
}

// BackendCounts holds atomic request and byte counters captured by CountingBackend.
type BackendCounts struct {
	Gets          int64 `json:"gets"`
	GetRanges     int64 `json:"get_ranges"`
	DemandGets    int64 `json:"demand_gets"`
	PrefetchGets  int64 `json:"prefetch_gets"`
	Puts          int64 `json:"puts"`
	PutIfAbsents  int64 `json:"put_if_absents"`
	PutIfVersions int64 `json:"put_if_versions"`
	Deletes       int64 `json:"deletes"`
	Lists         int64 `json:"lists"`
	Stats         int64 `json:"stats"`
	BytesRead     int64 `json:"bytes_read"`
	DemandBytes   int64 `json:"demand_bytes"`
	PrefetchBytes int64 `json:"prefetch_bytes"`
	BytesWritten  int64 `json:"bytes_written"`
}

// TotalGets returns Gets + GetRanges.
func (c BackendCounts) TotalGets() int64 {
	return c.Gets + c.GetRanges
}

// CountingBackend wraps any canonical backend.Backend with thread-safe request
// accounting (split by demand vs prefetch) and optional S3-like RTT and shared
// link bandwidth pacing for reproducible cold-start benchmarks.
type CountingBackend struct {
	backend.Backend

	simRTT          time.Duration
	linkBytesPerSec int64

	linkMu   sync.Mutex
	linkFree time.Time

	gets          atomic.Int64
	getRanges     atomic.Int64
	demandGets    atomic.Int64
	prefetchGets  atomic.Int64
	puts          atomic.Int64
	putIfAbsents  atomic.Int64
	putIfVersions atomic.Int64
	deletes       atomic.Int64
	lists         atomic.Int64
	stats         atomic.Int64
	bytesRead     atomic.Int64
	demandBytes   atomic.Int64
	prefetchBytes atomic.Int64
	bytesWritten  atomic.Int64
}

// CountingOptions configures optional latency and link pacing on CountingBackend.
type CountingOptions struct {
	SimulatedRTT    time.Duration
	LinkBytesPerSec int64
}

// NewCountingBackend wraps be with request/byte instrumentation.
func NewCountingBackend(be backend.Backend, opts CountingOptions) *CountingBackend {
	return &CountingBackend{
		Backend:         be,
		simRTT:          opts.SimulatedRTT,
		linkBytesPerSec: opts.LinkBytesPerSec,
	}
}

// Reset zeroes all counters and updates the simulated latency/link parameters.
func (c *CountingBackend) Reset(opts CountingOptions) {
	c.linkMu.Lock()
	c.simRTT = opts.SimulatedRTT
	c.linkBytesPerSec = opts.LinkBytesPerSec
	c.linkFree = time.Time{}
	c.linkMu.Unlock()

	c.gets.Store(0)
	c.getRanges.Store(0)
	c.demandGets.Store(0)
	c.prefetchGets.Store(0)
	c.puts.Store(0)
	c.putIfAbsents.Store(0)
	c.putIfVersions.Store(0)
	c.deletes.Store(0)
	c.lists.Store(0)
	c.stats.Store(0)
	c.bytesRead.Store(0)
	c.demandBytes.Store(0)
	c.prefetchBytes.Store(0)
	c.bytesWritten.Store(0)
}

// Counts returns a consistent snapshot of all counters.
func (c *CountingBackend) Counts() BackendCounts {
	return BackendCounts{
		Gets:          c.gets.Load(),
		GetRanges:     c.getRanges.Load(),
		DemandGets:    c.demandGets.Load(),
		PrefetchGets:  c.prefetchGets.Load(),
		Puts:          c.puts.Load(),
		PutIfAbsents:  c.putIfAbsents.Load(),
		PutIfVersions: c.putIfVersions.Load(),
		Deletes:       c.deletes.Load(),
		Lists:         c.lists.Load(),
		Stats:         c.stats.Load(),
		BytesRead:     c.bytesRead.Load(),
		DemandBytes:   c.demandBytes.Load(),
		PrefetchBytes: c.prefetchBytes.Load(),
		BytesWritten:  c.bytesWritten.Load(),
	}
}

func (c *CountingBackend) pace(ctx context.Context, nBytes int64) error {
	c.linkMu.Lock()
	rtt := c.simRTT
	rate := c.linkBytesPerSec
	var wait time.Duration
	if rate > 0 && nBytes > 0 {
		now := time.Now()
		start := now
		if c.linkFree.After(now) {
			start = c.linkFree
		}
		xfer := time.Duration(float64(nBytes) / float64(rate) * float64(time.Second))
		end := start.Add(xfer)
		c.linkFree = end
		wait = end.Sub(now)
	}
	c.linkMu.Unlock()

	total := rtt + wait
	if total <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(total)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *CountingBackend) recordRead(ctx context.Context, isRange bool, n int64) {
	if isRange {
		c.getRanges.Add(1)
	} else {
		c.gets.Add(1)
	}
	c.bytesRead.Add(n)
	if IsPrefetchContext(ctx) {
		c.prefetchGets.Add(1)
		c.prefetchBytes.Add(n)
	} else {
		c.demandGets.Add(1)
		c.demandBytes.Add(n)
	}
}

func (c *CountingBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if err := c.pace(ctx, 0); err != nil {
		return nil, "", err
	}
	data, ver, err := c.Backend.Get(ctx, key, max)
	if err == nil {
		if c.linkBytesPerSec > 0 {
			if perr := c.paceBytesOnly(ctx, int64(len(data))); perr != nil {
				return nil, "", perr
			}
		}
		c.recordRead(ctx, false, int64(len(data)))
	}
	return data, ver, err
}

func (c *CountingBackend) paceBytesOnly(ctx context.Context, nBytes int64) error {
	if nBytes <= 0 {
		return nil
	}
	c.linkMu.Lock()
	rate := c.linkBytesPerSec
	if rate <= 0 {
		c.linkMu.Unlock()
		return nil
	}
	now := time.Now()
	start := now
	if c.linkFree.After(now) {
		start = c.linkFree
	}
	xfer := time.Duration(float64(nBytes) / float64(rate) * float64(time.Second))
	end := start.Add(xfer)
	c.linkFree = end
	wait := end.Sub(now)
	c.linkMu.Unlock()
	if wait <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *CountingBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := c.pace(ctx, n); err != nil {
		return nil, err
	}
	data, err := c.Backend.GetRange(ctx, key, off, n)
	if err == nil {
		c.recordRead(ctx, true, int64(len(data)))
	}
	return data, err
}

func (c *CountingBackend) Put(ctx context.Context, key string, data []byte) error {
	c.puts.Add(1)
	c.bytesWritten.Add(int64(len(data)))
	return c.Backend.Put(ctx, key, data)
}

func (c *CountingBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	c.putIfAbsents.Add(1)
	c.bytesWritten.Add(int64(len(data)))
	return c.Backend.PutIfAbsent(ctx, key, data)
}

func (c *CountingBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	c.putIfVersions.Add(1)
	c.bytesWritten.Add(int64(len(data)))
	return c.Backend.PutIfVersion(ctx, key, data, v)
}

func (c *CountingBackend) Delete(ctx context.Context, key string) error {
	c.deletes.Add(1)
	return c.Backend.Delete(ctx, key)
}

func (c *CountingBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	c.lists.Add(1)
	return c.Backend.List(ctx, prefix)
}

func (c *CountingBackend) Stat(ctx context.Context, key string) (int64, error) {
	c.stats.Add(1)
	return c.Backend.Stat(ctx, key)
}

// ChunkLocation records where a content-addressed chunk lives in a pack object.
type ChunkLocation struct {
	PackKey string
	Offset  int64
	Length  int64
}

// ChunkFetcher fetches content-addressed chunks and optionally checks
// reachability from the volume's published root (docs/prefetch.md S2.1).
type ChunkFetcher interface {
	FetchChunk(ctx context.Context, id ChunkID) ([]byte, error)
	Reachable(id ChunkID) bool
	Location(id ChunkID) (ChunkLocation, bool)
	FetchMergedRange(ctx context.Context, packKey string, off, n int64) ([]byte, error)
}

var (
	// ErrChunkNotFound is returned when a requested chunk is absent.
	ErrChunkNotFound = errors.New("thinkahead: chunk not found")

	// ErrChunkCorrupt is returned when a fetched chunk fails BLAKE3 verification.
	ErrChunkCorrupt = errors.New("thinkahead: chunk hash mismatch")
)

// ChunkObjectStore stores content-addressed chunks packed into vfs-style
// `pack/<2hex>/<id>` objects on any backend.Backend, supporting single-chunk
// GetRange and S8 merged adjacent pack-range reads.
type ChunkObjectStore struct {
	be backend.Backend

	mu       sync.RWMutex
	index    map[ChunkID]ChunkLocation
	nextPack uint64
}

// NewChunkObjectStore creates a pack-backed chunk object store over be.
func NewChunkObjectStore(be backend.Backend) *ChunkObjectStore {
	return &ChunkObjectStore{
		be:    be,
		index: make(map[ChunkID]ChunkLocation, 1024),
	}
}

// PutPack writes a batch of chunks contiguously into one or more pack objects
// (up to 16 MiB per pack, matching vfs packTarget) and records their offsets.
func (s *ChunkObjectStore) PutPack(ctx context.Context, order []ChunkID, payloads map[ChunkID][]byte) error {
	const packTarget = 16 << 20
	var buf []byte
	type pendingEntry struct {
		id  ChunkID
		off int64
		n   int64
	}
	var entries []pendingEntry

	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		s.mu.Lock()
		s.nextPack++
		pid := s.nextPack
		s.mu.Unlock()

		var raw [16]byte
		raw[0] = byte(pid >> 56)
		raw[1] = byte(pid >> 48)
		raw[2] = byte(pid >> 40)
		raw[3] = byte(pid >> 32)
		raw[4] = byte(pid >> 24)
		raw[5] = byte(pid >> 16)
		raw[6] = byte(pid >> 8)
		raw[7] = byte(pid)
		h := hex.EncodeToString(raw[:])
		pkey := "pack/" + h[:2] + "/" + h

		if err := s.be.Put(ctx, pkey, buf); err != nil {
			return err
		}
		s.mu.Lock()
		for _, e := range entries {
			s.index[e.id] = ChunkLocation{PackKey: pkey, Offset: e.off, Length: e.n}
		}
		s.mu.Unlock()
		buf = buf[:0]
		entries = entries[:0]
		return nil
	}

	seen := make(map[ChunkID]struct{}, len(order))
	for _, id := range order {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		s.mu.RLock()
		_, exists := s.index[id]
		s.mu.RUnlock()
		if exists {
			continue
		}
		data, ok := payloads[id]
		if !ok {
			continue
		}
		if len(buf)+len(data) > packTarget && len(entries) > 0 {
			if err := flush(); err != nil {
				return err
			}
		}
		off := int64(len(buf))
		buf = append(buf, data...)
		entries = append(entries, pendingEntry{id: id, off: off, n: int64(len(data))})
	}
	return flush()
}

// PutChunk writes a single chunk into its own pack object (or skips if present).
func (s *ChunkObjectStore) PutChunk(ctx context.Context, id ChunkID, data []byte) error {
	return s.PutPack(ctx, []ChunkID{id}, map[ChunkID][]byte{id: data})
}

// Reachable reports whether id is indexed in the published chunk table.
func (s *ChunkObjectStore) Reachable(id ChunkID) bool {
	s.mu.RLock()
	_, ok := s.index[id]
	s.mu.RUnlock()
	return ok
}

// Location returns the pack location of id.
func (s *ChunkObjectStore) Location(id ChunkID) (ChunkLocation, bool) {
	s.mu.RLock()
	loc, ok := s.index[id]
	s.mu.RUnlock()
	return loc, ok
}

// FetchChunk issues one ranged read to the backend for chunk id.
func (s *ChunkObjectStore) FetchChunk(ctx context.Context, id ChunkID) ([]byte, error) {
	loc, ok := s.Location(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrChunkNotFound, id)
	}
	return s.be.GetRange(ctx, loc.PackKey, loc.Offset, loc.Length)
}

// FetchMergedRange fetches a contiguous byte range covering multiple adjacent
// chunks inside one pack object (docs/prefetch.md S8).
func (s *ChunkObjectStore) FetchMergedRange(ctx context.Context, packKey string, off, n int64) ([]byte, error) {
	return s.be.GetRange(ctx, packKey, off, n)
}
