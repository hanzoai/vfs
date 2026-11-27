// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package gitvfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/backend"
)

// BackendMetrics records object-store operation counts and byte volumes broken
// down by vfs object class (head, meta/, pack/, ext/).
type BackendMetrics struct {
	Puts          int64 `json:"puts"`
	PutIfAbsents  int64 `json:"put_if_absents"`
	PutIfVersions int64 `json:"put_if_versions"`
	TotalPuts     int64 `json:"total_puts"`
	PutBytes      int64 `json:"put_bytes"`

	Gets          int64 `json:"gets"`
	GetBytes      int64 `json:"get_bytes"`
	GetRanges     int64 `json:"get_ranges"`
	GetRangeBytes int64 `json:"get_range_bytes"`
	Deletes       int64 `json:"deletes"`
	Lists         int64 `json:"lists"`
	Stats         int64 `json:"stats"`

	HeadPuts     int64 `json:"head_puts"`
	HeadPutBytes int64 `json:"head_put_bytes"`
	MetaPuts     int64 `json:"meta_puts"`
	MetaPutBytes int64 `json:"meta_put_bytes"`
	PackPuts     int64 `json:"pack_puts"`
	PackPutBytes int64 `json:"pack_put_bytes"`
	ExtPuts      int64 `json:"ext_puts"`
	ExtPutBytes  int64 `json:"ext_put_bytes"`
}

// Delta returns the difference (m - prev).
func (m BackendMetrics) Delta(prev BackendMetrics) BackendMetrics {
	return BackendMetrics{
		Puts:          m.Puts - prev.Puts,
		PutIfAbsents:  m.PutIfAbsents - prev.PutIfAbsents,
		PutIfVersions: m.PutIfVersions - prev.PutIfVersions,
		TotalPuts:     m.TotalPuts - prev.TotalPuts,
		PutBytes:      m.PutBytes - prev.PutBytes,
		Gets:          m.Gets - prev.Gets,
		GetBytes:      m.GetBytes - prev.GetBytes,
		GetRanges:     m.GetRanges - prev.GetRanges,
		GetRangeBytes: m.GetRangeBytes - prev.GetRangeBytes,
		Deletes:       m.Deletes - prev.Deletes,
		Lists:         m.Lists - prev.Lists,
		Stats:         m.Stats - prev.Stats,
		HeadPuts:      m.HeadPuts - prev.HeadPuts,
		HeadPutBytes:  m.HeadPutBytes - prev.HeadPutBytes,
		MetaPuts:      m.MetaPuts - prev.MetaPuts,
		MetaPutBytes:  m.MetaPutBytes - prev.MetaPutBytes,
		PackPuts:      m.PackPuts - prev.PackPuts,
		PackPutBytes:  m.PackPutBytes - prev.PackPutBytes,
		ExtPuts:       m.ExtPuts - prev.ExtPuts,
		ExtPutBytes:   m.ExtPutBytes - prev.ExtPutBytes,
	}
}

// CountingBackend wraps any canonical backend.Backend to measure exact S3 /
// object-store PUT/GET counts and bytes per key class without altering wire
// layout or sealing semantics.
type CountingBackend struct {
	Inner    backend.Backend
	PutDelay time.Duration
	GetDelay time.Duration

	puts          atomic.Int64
	putIfAbsents  atomic.Int64
	putIfVersions atomic.Int64
	putBytes      atomic.Int64
	gets          atomic.Int64
	getBytes      atomic.Int64
	getRanges     atomic.Int64
	getRangeBytes atomic.Int64
	deletes       atomic.Int64
	lists         atomic.Int64
	stats         atomic.Int64

	headPuts     atomic.Int64
	headPutBytes atomic.Int64
	metaPuts     atomic.Int64
	metaPutBytes atomic.Int64
	packPuts     atomic.Int64
	packPutBytes atomic.Int64
	extPuts      atomic.Int64
	extPutBytes  atomic.Int64
}

// NewCountingBackend wraps inner with atomic request/byte counters.
func NewCountingBackend(inner backend.Backend) *CountingBackend {
	return &CountingBackend{Inner: inner}
}

func (c *CountingBackend) recordPut(key string, n int64) {
	c.putBytes.Add(n)
	switch {
	case key == "head":
		c.headPuts.Add(1)
		c.headPutBytes.Add(n)
	case strings.HasPrefix(key, "meta/"):
		c.metaPuts.Add(1)
		c.metaPutBytes.Add(n)
	case strings.HasPrefix(key, "pack/"):
		c.packPuts.Add(1)
		c.packPutBytes.Add(n)
	case strings.HasPrefix(key, "ext/"):
		c.extPuts.Add(1)
		c.extPutBytes.Add(n)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Snapshot returns a point-in-time copy of all backend counters.
func (c *CountingBackend) Snapshot() BackendMetrics {
	puts := c.puts.Load()
	pia := c.putIfAbsents.Load()
	piv := c.putIfVersions.Load()
	return BackendMetrics{
		Puts:          puts,
		PutIfAbsents:  pia,
		PutIfVersions: piv,
		TotalPuts:     puts + pia + piv,
		PutBytes:      c.putBytes.Load(),
		Gets:          c.gets.Load(),
		GetBytes:      c.getBytes.Load(),
		GetRanges:     c.getRanges.Load(),
		GetRangeBytes: c.getRangeBytes.Load(),
		Deletes:       c.deletes.Load(),
		Lists:         c.lists.Load(),
		Stats:         c.stats.Load(),
		HeadPuts:      c.headPuts.Load(),
		HeadPutBytes:  c.headPutBytes.Load(),
		MetaPuts:      c.metaPuts.Load(),
		MetaPutBytes:  c.metaPutBytes.Load(),
		PackPuts:      c.packPuts.Load(),
		PackPutBytes:  c.packPutBytes.Load(),
		ExtPuts:       c.extPuts.Load(),
		ExtPutBytes:   c.extPutBytes.Load(),
	}
}

func (c *CountingBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if err := sleepCtx(ctx, c.GetDelay); err != nil {
		return nil, "", err
	}
	c.gets.Add(1)
	b, v, err := c.Inner.Get(ctx, key, max)
	if err == nil {
		c.getBytes.Add(int64(len(b)))
	}
	return b, v, err
}

func (c *CountingBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := sleepCtx(ctx, c.GetDelay); err != nil {
		return nil, err
	}
	c.getRanges.Add(1)
	b, err := c.Inner.GetRange(ctx, key, off, n)
	if err == nil {
		c.getRangeBytes.Add(int64(len(b)))
	}
	return b, err
}

func (c *CountingBackend) Put(ctx context.Context, key string, data []byte) error {
	if err := sleepCtx(ctx, c.PutDelay); err != nil {
		return err
	}
	c.puts.Add(1)
	c.recordPut(key, int64(len(data)))
	return c.Inner.Put(ctx, key, data)
}

func (c *CountingBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if err := sleepCtx(ctx, c.PutDelay); err != nil {
		return "", err
	}
	c.putIfAbsents.Add(1)
	c.recordPut(key, int64(len(data)))
	return c.Inner.PutIfAbsent(ctx, key, data)
}

func (c *CountingBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if err := sleepCtx(ctx, c.PutDelay); err != nil {
		return "", err
	}
	c.putIfVersions.Add(1)
	c.recordPut(key, int64(len(data)))
	return c.Inner.PutIfVersion(ctx, key, data, v)
}

func (c *CountingBackend) Delete(ctx context.Context, key string) error {
	c.deletes.Add(1)
	return c.Inner.Delete(ctx, key)
}

func (c *CountingBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	c.lists.Add(1)
	return c.Inner.List(ctx, prefix)
}

func (c *CountingBackend) Stat(ctx context.Context, key string) (int64, error) {
	c.stats.Add(1)
	return c.Inner.Stat(ctx, key)
}

func (c *CountingBackend) Close() error   { return c.Inner.Close() }
func (c *CountingBackend) String() string { return c.Inner.String() }

// JournalMetrics records exact journal segment disk operations and fsync
// latency percentiles.
type JournalMetrics struct {
	Syncs      int64   `json:"syncs"`
	SyncNanos  int64   `json:"sync_nanos"`
	SyncP50Us  float64 `json:"sync_p50_us"`
	SyncP95Us  float64 `json:"sync_p95_us"`
	SyncMeanUs float64 `json:"sync_mean_us"`
	WriteAts   int64   `json:"write_ats"`
	WriteBytes int64   `json:"write_bytes"`
	Writebacks int64   `json:"writebacks"`
	Creates    int64   `json:"creates"`
}

// JournalTracker instruments vfs.Config.Disk to capture every journal Sync and
// WriteAt across one or more segment files.
type JournalTracker struct {
	syncs      atomic.Int64
	syncNanos  atomic.Int64
	writeAts   atomic.Int64
	writeBytes atomic.Int64
	writebacks atomic.Int64
	creates    atomic.Int64

	mu      sync.Mutex
	samples []int64 // sync durations in nanoseconds
}

// NewJournalTracker returns a fresh JournalTracker.
func NewJournalTracker() *JournalTracker { return &JournalTracker{} }

// DiskFactory returns a vfs.Config.Disk callback that wraps journal.Files(dir).
func (t *JournalTracker) DiskFactory() func(dir string) (journal.Disk, error) {
	return func(dir string) (journal.Disk, error) {
		d, err := journal.Files(dir)
		if err != nil {
			return nil, err
		}
		return &countingDisk{Disk: d, tr: t}, nil
	}
}

func (t *JournalTracker) recordSync(d time.Duration) {
	ns := d.Nanoseconds()
	t.syncs.Add(1)
	t.syncNanos.Add(ns)
	t.mu.Lock()
	t.samples = append(t.samples, ns)
	t.mu.Unlock()
}

// Snapshot returns current cumulative journal metrics.
func (t *JournalTracker) Snapshot() JournalMetrics {
	t.mu.Lock()
	cp := make([]int64, len(t.samples))
	copy(cp, t.samples)
	t.mu.Unlock()

	syncs := t.syncs.Load()
	syncNanos := t.syncNanos.Load()
	m := JournalMetrics{
		Syncs:      syncs,
		SyncNanos:  syncNanos,
		WriteAts:   t.writeAts.Load(),
		WriteBytes: t.writeBytes.Load(),
		Writebacks: t.writebacks.Load(),
		Creates:    t.creates.Load(),
	}
	if len(cp) > 0 {
		sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
		m.SyncP50Us = float64(percentileInt64(cp, 0.50)) / 1000.0
		m.SyncP95Us = float64(percentileInt64(cp, 0.95)) / 1000.0
		m.SyncMeanUs = float64(syncNanos) / float64(len(cp)) / 1000.0
	}
	return m
}

// ResetSamples clears per-call latency samples while preserving cumulative counts,
// useful for measuring a specific phase's fsync percentiles.
func (t *JournalTracker) ResetSamples() {
	t.mu.Lock()
	t.samples = t.samples[:0]
	t.mu.Unlock()
}

type countingDisk struct {
	journal.Disk
	tr *JournalTracker
}

func (d *countingDisk) Create(name string, size int64) (journal.File, error) {
	d.tr.creates.Add(1)
	f, err := d.Disk.Create(name, size)
	if err != nil {
		return nil, err
	}
	return &countingFile{File: f, tr: d.tr}, nil
}

func (d *countingDisk) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	if err != nil {
		return nil, err
	}
	return &countingFile{File: f, tr: d.tr}, nil
}

type countingFile struct {
	journal.File
	tr *JournalTracker
}

func (f *countingFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.tr.writeAts.Add(1)
	if n > 0 {
		f.tr.writeBytes.Add(int64(n))
	}
	return n, err
}

func (f *countingFile) Writeback(off, n int64) error {
	f.tr.writebacks.Add(1)
	return f.File.Writeback(off, n)
}

func (f *countingFile) Sync() error {
	start := time.Now()
	err := f.File.Sync()
	f.tr.recordSync(time.Since(start))
	return err
}

func percentileInt64(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// NVMeProbeResult records baseline local filesystem primitive latencies so
// every vfs fsync and metadata measurement is compared against plain NVMe on
// the exact same node.
type NVMeProbeResult struct {
	Dir         string  `json:"dir"`
	Iterations  int     `json:"iterations"`
	FsyncP50Us  float64 `json:"fsync_p50_us"`
	FsyncP95Us  float64 `json:"fsync_p95_us"`
	FsyncMeanUs float64 `json:"fsync_mean_us"`
	CreateP50Us float64 `json:"create_p50_us"`
	StatP50Us   float64 `json:"stat_p50_us"`
	RenameP50Us float64 `json:"rename_p50_us"`
	UnlinkP50Us float64 `json:"unlink_p50_us"`
}

// ProbeNVMe measures plain local disk fsync, create, stat, rename, and unlink
// latency inside dir.
func ProbeNVMe(dir string, iters int) (NVMeProbeResult, error) {
	if iters <= 0 {
		iters = 40
	}
	probeDir, err := os.MkdirTemp(dir, "nvme-probe-*")
	if err != nil {
		return NVMeProbeResult{}, err
	}
	defer os.RemoveAll(probeDir)

	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i)
	}
	fsyncNs := make([]int64, 0, iters)
	createNs := make([]int64, 0, iters)
	statNs := make([]int64, 0, iters)
	renameNs := make([]int64, 0, iters)
	unlinkNs := make([]int64, 0, iters)
	var fsyncSum int64

	for i := range iters {
		p1 := filepath.Join(probeDir, fmt.Sprintf("f-%04d", i))
		p2 := filepath.Join(probeDir, fmt.Sprintf("r-%04d", i))

		t0 := time.Now()
		f, err := os.OpenFile(p1, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			return NVMeProbeResult{}, err
		}
		if _, err := f.Write(payload); err != nil {
			f.Close()
			return NVMeProbeResult{}, err
		}
		createNs = append(createNs, time.Since(t0).Nanoseconds())

		t1 := time.Now()
		if err := f.Sync(); err != nil {
			f.Close()
			return NVMeProbeResult{}, err
		}
		d := time.Since(t1).Nanoseconds()
		fsyncNs = append(fsyncNs, d)
		fsyncSum += d
		if err := f.Close(); err != nil {
			return NVMeProbeResult{}, err
		}

		t2 := time.Now()
		if _, err := os.Stat(p1); err != nil {
			return NVMeProbeResult{}, err
		}
		statNs = append(statNs, time.Since(t2).Nanoseconds())

		t3 := time.Now()
		if err := os.Rename(p1, p2); err != nil {
			return NVMeProbeResult{}, err
		}
		renameNs = append(renameNs, time.Since(t3).Nanoseconds())

		t4 := time.Now()
		if err := os.Remove(p2); err != nil {
			return NVMeProbeResult{}, err
		}
		unlinkNs = append(unlinkNs, time.Since(t4).Nanoseconds())
	}

	sort.Slice(fsyncNs, func(i, j int) bool { return fsyncNs[i] < fsyncNs[j] })
	sort.Slice(createNs, func(i, j int) bool { return createNs[i] < createNs[j] })
	sort.Slice(statNs, func(i, j int) bool { return statNs[i] < statNs[j] })
	sort.Slice(renameNs, func(i, j int) bool { return renameNs[i] < renameNs[j] })
	sort.Slice(unlinkNs, func(i, j int) bool { return unlinkNs[i] < unlinkNs[j] })

	p50OrMeanUs := func(sorted []int64) float64 {
		p := percentileInt64(sorted, 0.50)
		if p > 0 {
			return float64(p) / 1000.0
		}
		var sum int64
		for _, v := range sorted {
			sum += v
		}
		return float64(sum) / float64(max(len(sorted), 1)) / 1000.0
	}

	return NVMeProbeResult{
		Dir:         dir,
		Iterations:  iters,
		FsyncP50Us:  p50OrMeanUs(fsyncNs),
		FsyncP95Us:  float64(percentileInt64(fsyncNs, 0.95)) / 1000.0,
		FsyncMeanUs: float64(fsyncSum) / float64(len(fsyncNs)) / 1000.0,
		CreateP50Us: p50OrMeanUs(createNs),
		StatP50Us:   p50OrMeanUs(statNs),
		RenameP50Us: p50OrMeanUs(renameNs),
		UnlinkP50Us: p50OrMeanUs(unlinkNs),
	}, nil
}

// NewNonVolatileJournalDir creates a non-volatile directory usable by
// journal.Files (falling back from preferredBase to the current working
// directory if preferredBase resides on tmpfs/ramfs).
func NewNonVolatileJournalDir(preferredBase string) (dir string, cleanup func(), err error) {
	tryBase := func(base string) (string, error) {
		d, err := os.MkdirTemp(base, "vfs-git-journal-*")
		if err != nil {
			return "", err
		}
		probePath := filepath.Join(d, "probe")
		disk, err := journal.Files(probePath)
		if err != nil {
			_ = os.RemoveAll(d)
			return "", err
		}
		_ = disk.Close()
		_ = os.RemoveAll(probePath)
		return d, nil
	}
	if d, err := tryBase(preferredBase); err == nil {
		return d, func() { _ = os.RemoveAll(d) }, nil
	} else if !errors.Is(err, journal.ErrVolatile) {
		return "", nil, err
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", nil, err
	}
	d, err := tryBase(wd)
	if err != nil {
		return "", nil, err
	}
	return d, func() { _ = os.RemoveAll(d) }, nil
}
