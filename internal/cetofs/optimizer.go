// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cetofs

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

var (
	// ErrInvalidRange is returned when a RangeRequest has negative offsets,
	// negative lengths, or would overflow signed int64 arithmetic.
	ErrInvalidRange = errors.New("cetofs: invalid range request")

	// ErrIntegrityMismatch is returned when an immutable content-addressed
	// meta/ shard returned by the backend does not match its BLAKE3 key digest.
	ErrIntegrityMismatch = errors.New("cetofs: content-addressed shard digest mismatch")
)

// OptimizerConfig controls every opt-in, guarantee-preserving optimization in
// CachedGoferBridge and OptimizedBackend. Every feature can be toggled
// independently.
type OptimizerConfig struct {
	// Boundary 1 (FUSE/Gofer -> VFS) metadata caching.
	EntryTimeout          time.Duration // 0 disables Lookup cache
	AttrTimeout           time.Duration // 0 disables GetAttr cache
	DirCacheTTL           time.Duration // 0 disables ReadDir cache
	MaxMetaEntries        int           // bounded entry cap per metadata cache
	SkipRedundantOpenStat bool          // skip vol.Stat before vol.Open when cached attr proves regular file

	// Boundary 2 (VFS -> S3/Backend) optimizations.
	EnableHeadVersionCache    bool          // track Head ETag/version to avoid pre-PUT GET (CAS stays server-side)
	HeadCacheTTL              time.Duration // 0 = strict linearizable Head reads; >0 = opt-in bounded read-through
	EnableShardCache          bool          // BLAKE3-verified LRU cache for immutable meta/ shards and immutable Stat
	MaxShardCacheBytes        int64         // byte budget for ShardCache
	ListCacheTTL              time.Duration // 0 disables prefix List cache
	EnableRangeCoalescing     bool          // coalesce adjacent GetRange calls on immutable pack/ objects
	EnableSequentialReadAhead bool          // adaptive pack-window coalescing on detected adjacent access
	AlwaysReadAhead           bool          // force read-ahead even on non-adjacent access (negative-regime ablation)
	MaxCoalesceGap            int64         // max byte gap bridged when coalescing ranges
	MaxCoalesceWindow         int64         // max total byte span of a coalesced range read
	ReadAheadWindow           int64         // window size for sequential pack read-ahead
	MaxPackCacheBytes         int64         // byte budget for cached coalesced pack spans

	// Clock overrides time.Now for deterministic TTL tests. Nil uses time.Now.
	Clock func() time.Time
}

// DefaultOptimizerConfig returns the recommended production-safe configuration
// that preserves CAS authority, fenced-Head monotonicity, and content integrity.
func DefaultOptimizerConfig() OptimizerConfig {
	return OptimizerConfig{
		EntryTimeout:              5 * time.Second,
		AttrTimeout:               5 * time.Second,
		DirCacheTTL:               5 * time.Second,
		MaxMetaEntries:            16384,
		SkipRedundantOpenStat:     true,
		EnableHeadVersionCache:    true,
		HeadCacheTTL:              0, // strict linearizable Head reads by default
		EnableShardCache:          true,
		MaxShardCacheBytes:        64 << 20,
		ListCacheTTL:              2 * time.Second,
		EnableRangeCoalescing:     true,
		EnableSequentialReadAhead: true,
		AlwaysReadAhead:           false,
		MaxCoalesceGap:            256 << 10,
		MaxCoalesceWindow:         4 << 20,
		ReadAheadWindow:           2 << 20,
		MaxPackCacheBytes:         64 << 20,
	}
}

// UnoptimizedConfig returns a configuration with all CETOFS optimizations disabled.
func UnoptimizedConfig() OptimizerConfig {
	return OptimizerConfig{}
}

func (c OptimizerConfig) now() time.Time {
	if c.Clock != nil {
		return c.Clock()
	}
	return time.Now()
}

func (c OptimizerConfig) normalize() OptimizerConfig {
	if c.MaxMetaEntries <= 0 {
		c.MaxMetaEntries = 16384
	}
	if c.MaxShardCacheBytes <= 0 {
		c.MaxShardCacheBytes = 64 << 20
	}
	if c.MaxCoalesceGap < 0 {
		c.MaxCoalesceGap = 0
	}
	if c.MaxCoalesceWindow <= 0 {
		c.MaxCoalesceWindow = 4 << 20
	}
	if c.ReadAheadWindow <= 0 {
		c.ReadAheadWindow = 2 << 20
	}
	if c.MaxPackCacheBytes <= 0 {
		c.MaxPackCacheBytes = 64 << 20
	}
	return c
}

// OptimizerStats reports internal hit, miss, invalidation, and CAS conflict
// counters from CachedGoferBridge and OptimizedBackend.
type OptimizerStats struct {
	AttrHits            int64 `json:"attr_hits"`
	AttrMisses          int64 `json:"attr_misses"`
	LookupHits          int64 `json:"lookup_hits"`
	LookupMisses        int64 `json:"lookup_misses"`
	DirHits             int64 `json:"dir_hits"`
	DirMisses           int64 `json:"dir_misses"`
	OpenStatSkipped     int64 `json:"open_stat_skipped"`
	MetaInvalidations   int64 `json:"meta_invalidations"`
	HeadVersionHits     int64 `json:"head_version_hits"`
	HeadReadHits        int64 `json:"head_read_hits"`
	HeadCASConflicts    int64 `json:"head_cas_conflicts"`
	ShardCacheHits      int64 `json:"shard_cache_hits"`
	ShardCacheMisses    int64 `json:"shard_cache_misses"`
	ImmutableStatHits   int64 `json:"immutable_stat_hits"`
	ListCacheHits       int64 `json:"list_cache_hits"`
	ListCacheMisses     int64 `json:"list_cache_misses"`
	RangeCacheHits      int64 `json:"range_cache_hits"`
	RangeCoalescedSpans int64 `json:"range_coalesced_spans"`
	RangeReadAheadBytes int64 `json:"range_read_ahead_bytes"`
}

// --- Bounded LRU helpers (pure stdlib container/list) ---

type entryLRUItem[K comparable, V any] struct {
	key K
	val V
}

type entryLRU[K comparable, V any] struct {
	max   int
	ll    *list.List
	items map[K]*list.Element
}

func newEntryLRU[K comparable, V any](max int) *entryLRU[K, V] {
	if max <= 0 {
		max = 1
	}
	return &entryLRU[K, V]{
		max:   max,
		ll:    list.New(),
		items: make(map[K]*list.Element),
	}
}

func (c *entryLRU[K, V]) get(k K) (V, bool) {
	if el, ok := c.items[k]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(entryLRUItem[K, V]).val, true
	}
	var zero V
	return zero, false
}

func (c *entryLRU[K, V]) put(k K, v V) {
	if el, ok := c.items[k]; ok {
		el.Value = entryLRUItem[K, V]{key: k, val: v}
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(entryLRUItem[K, V]{key: k, val: v})
	c.items[k] = el
	for c.ll.Len() > c.max {
		back := c.ll.Back()
		if back == nil {
			break
		}
		item := back.Value.(entryLRUItem[K, V])
		delete(c.items, item.key)
		c.ll.Remove(back)
	}
}

func (c *entryLRU[K, V]) delete(k K) {
	if el, ok := c.items[k]; ok {
		delete(c.items, k)
		c.ll.Remove(el)
	}
}

func (c *entryLRU[K, V]) clear() {
	c.ll.Init()
	clear(c.items)
}

type byteLRUItem[K comparable, V any] struct {
	key  K
	val  V
	size int64
}

type byteLRU[K comparable, V any] struct {
	maxBytes int64
	curBytes int64
	ll       *list.List
	items    map[K]*list.Element
}

func newByteLRU[K comparable, V any](maxBytes int64) *byteLRU[K, V] {
	if maxBytes <= 0 {
		maxBytes = 1
	}
	return &byteLRU[K, V]{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[K]*list.Element),
	}
}

func (c *byteLRU[K, V]) get(k K) (V, bool) {
	if el, ok := c.items[k]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(byteLRUItem[K, V]).val, true
	}
	var zero V
	return zero, false
}

func (c *byteLRU[K, V]) put(k K, v V, size int64) {
	if size < 0 {
		size = 0
	}
	if size > c.maxBytes {
		return
	}
	if el, ok := c.items[k]; ok {
		old := el.Value.(byteLRUItem[K, V])
		c.curBytes -= old.size
		el.Value = byteLRUItem[K, V]{key: k, val: v, size: size}
		c.curBytes += size
		c.ll.MoveToFront(el)
	} else {
		el := c.ll.PushFront(byteLRUItem[K, V]{key: k, val: v, size: size})
		c.items[k] = el
		c.curBytes += size
	}
	for c.curBytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}
		old := back.Value.(byteLRUItem[K, V])
		delete(c.items, old.key)
		c.ll.Remove(back)
		c.curBytes -= old.size
	}
}

func (c *byteLRU[K, V]) clear() {
	c.ll.Init()
	clear(c.items)
	c.curBytes = 0
}

// --- Boundary 1 Optimizer: CachedGoferBridge ---

type cachedAttr struct {
	in      vfs.Inode
	expires time.Time
	gen     uint64
}

type cachedEntry struct {
	ino     vfs.InodeID
	expires time.Time
	gen     uint64
}

type cachedDir struct {
	entries []vfs.DirEntry
	parent  vfs.InodeID
	expires time.Time
	gen     uint64
}

// CachedGoferBridge wraps GoferBridge with bounded-TTL AttrTimeout,
// EntryTimeout, and DirCache caches plus SkipRedundantOpenStat. All caches are
// synchronously invalidated on local mutation or fenced-Head generation bumps.
type CachedGoferBridge struct {
	bridge *GoferBridge
	cfg    OptimizerConfig

	mu      sync.Mutex
	gen     uint64
	attrs   *entryLRU[vfs.InodeID, cachedAttr]
	entries *entryLRU[lookupPair, cachedEntry]
	dirs    *entryLRU[vfs.InodeID, cachedDir]

	attrHits     atomic.Int64
	attrMisses   atomic.Int64
	lookupHits   atomic.Int64
	lookupMisses atomic.Int64
	dirHits      atomic.Int64
	dirMisses    atomic.Int64
	openSkipped  atomic.Int64
	invalidates  atomic.Int64
}

var _ GoferFS = (*CachedGoferBridge)(nil)

// NewCachedGoferBridge builds a guarantee-preserving FUSE/Gofer metadata cache
// over bridge.
func NewCachedGoferBridge(bridge *GoferBridge, cfg OptimizerConfig) *CachedGoferBridge {
	cfg = cfg.normalize()
	return &CachedGoferBridge{
		bridge:  bridge,
		cfg:     cfg,
		attrs:   newEntryLRU[vfs.InodeID, cachedAttr](cfg.MaxMetaEntries),
		entries: newEntryLRU[lookupPair, cachedEntry](cfg.MaxMetaEntries),
		dirs:    newEntryLRU[vfs.InodeID, cachedDir](cfg.MaxMetaEntries),
	}
}

// InvalidateAll bumps the metadata generation and clears all cached attributes,
// directory entries, and listings. Safe to call from OptimizedBackend's
// fenced-Head conflict/advance callback.
func (c *CachedGoferBridge) InvalidateAll() {
	c.mu.Lock()
	c.gen++
	c.attrs.clear()
	c.entries.clear()
	c.dirs.clear()
	c.mu.Unlock()
	c.invalidates.Add(1)
}

// Generation returns the current metadata invalidation generation.
func (c *CachedGoferBridge) Generation() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// Stats returns cache hit/miss/invalidation statistics for this bridge.
func (c *CachedGoferBridge) Stats() OptimizerStats {
	return OptimizerStats{
		AttrHits:          c.attrHits.Load(),
		AttrMisses:        c.attrMisses.Load(),
		LookupHits:        c.lookupHits.Load(),
		LookupMisses:      c.lookupMisses.Load(),
		DirHits:           c.dirHits.Load(),
		DirMisses:         c.dirMisses.Load(),
		OpenStatSkipped:   c.openSkipped.Load(),
		MetaInvalidations: c.invalidates.Load(),
	}
}

func (c *CachedGoferBridge) GetAttr(ctx context.Context, ino vfs.InodeID) (vfs.Inode, error) {
	if c.cfg.AttrTimeout > 0 {
		now := c.cfg.now()
		c.mu.Lock()
		if e, ok := c.attrs.get(ino); ok && e.gen == c.gen && now.Before(e.expires) {
			c.mu.Unlock()
			c.attrHits.Add(1)
			return e.in, nil
		}
		startGen := c.gen
		c.mu.Unlock()
		c.attrMisses.Add(1)

		in, err := c.bridge.GetAttr(ctx, ino)
		if err == nil {
			c.mu.Lock()
			if c.gen == startGen {
				c.attrs.put(ino, cachedAttr{
					in:      in,
					expires: c.cfg.now().Add(c.cfg.AttrTimeout),
					gen:     startGen,
				})
			}
			c.mu.Unlock()
		}
		return in, err
	}
	return c.bridge.GetAttr(ctx, ino)
}

func (c *CachedGoferBridge) Lookup(ctx context.Context, dir vfs.InodeID, name string) (vfs.Inode, error) {
	if c.cfg.EntryTimeout > 0 {
		key := lookupPair{dir: dir, name: name}
		now := c.cfg.now()
		c.mu.Lock()
		if e, ok := c.entries.get(key); ok && e.gen == c.gen && now.Before(e.expires) {
			// Verify fresh inode attributes from attrCache so Write/SetAttr
			// invalidations are never shadowed by a stale dentry snapshot.
			if c.cfg.AttrTimeout > 0 {
				if a, aok := c.attrs.get(e.ino); aok && a.gen == c.gen && now.Before(a.expires) {
					c.mu.Unlock()
					c.lookupHits.Add(1)
					return a.in, nil
				}
			}
			childIno := e.ino
			c.mu.Unlock()
			c.lookupHits.Add(1)
			return c.GetAttr(ctx, childIno)
		}
		startGen := c.gen
		c.mu.Unlock()
		c.lookupMisses.Add(1)

		in, err := c.bridge.Lookup(ctx, dir, name)
		if err == nil {
			nowAfter := c.cfg.now()
			c.mu.Lock()
			if c.gen == startGen {
				c.entries.put(key, cachedEntry{
					ino:     in.ID,
					expires: nowAfter.Add(c.cfg.EntryTimeout),
					gen:     startGen,
				})
				if c.cfg.AttrTimeout > 0 {
					c.attrs.put(in.ID, cachedAttr{
						in:      in,
						expires: nowAfter.Add(c.cfg.AttrTimeout),
						gen:     startGen,
					})
				}
			}
			c.mu.Unlock()
		}
		return in, err
	}
	return c.bridge.Lookup(ctx, dir, name)
}

func (c *CachedGoferBridge) ReadDir(ctx context.Context, dir vfs.InodeID) ([]vfs.DirEntry, vfs.InodeID, error) {
	if c.cfg.DirCacheTTL > 0 {
		now := c.cfg.now()
		c.mu.Lock()
		if d, ok := c.dirs.get(dir); ok && d.gen == c.gen && now.Before(d.expires) {
			cp := make([]vfs.DirEntry, len(d.entries))
			copy(cp, d.entries)
			parent := d.parent
			c.mu.Unlock()
			c.dirHits.Add(1)
			return cp, parent, nil
		}
		startGen := c.gen
		c.mu.Unlock()
		c.dirMisses.Add(1)

		ents, parent, err := c.bridge.ReadDir(ctx, dir)
		if err == nil {
			cp := make([]vfs.DirEntry, len(ents))
			copy(cp, ents)
			c.mu.Lock()
			if c.gen == startGen {
				c.dirs.put(dir, cachedDir{
					entries: cp,
					parent:  parent,
					expires: c.cfg.now().Add(c.cfg.DirCacheTTL),
					gen:     startGen,
				})
			}
			c.mu.Unlock()
		}
		return ents, parent, err
	}
	return c.bridge.ReadDir(ctx, dir)
}

func (c *CachedGoferBridge) Open(ctx context.Context, ino vfs.InodeID) (*vfs.File, error) {
	if c.cfg.SkipRedundantOpenStat && c.cfg.AttrTimeout > 0 {
		now := c.cfg.now()
		c.mu.Lock()
		e, ok := c.attrs.get(ino)
		valid := ok && e.gen == c.gen && now.Before(e.expires)
		c.mu.Unlock()
		if valid {
			if e.in.IsDir() {
				return nil, vfs.ErrIsDir
			}
			c.openSkipped.Add(1)
			return c.bridge.openDirect(ctx, ino)
		}
	}
	return c.bridge.Open(ctx, ino)
}

func (c *CachedGoferBridge) Create(ctx context.Context, dir vfs.InodeID, name string, mode fs.FileMode) (vfs.Inode, *vfs.File, error) {
	in, f, err := c.bridge.Create(ctx, dir, name, mode)
	c.invalidateDirMutation(dir, name, in.ID)
	if err == nil {
		c.seedCreated(dir, name, in)
	}
	return in, f, err
}

func (c *CachedGoferBridge) Mkdir(ctx context.Context, dir vfs.InodeID, name string, mode fs.FileMode) (vfs.Inode, error) {
	in, err := c.bridge.Mkdir(ctx, dir, name, mode)
	c.invalidateDirMutation(dir, name, in.ID)
	if err == nil {
		c.seedCreated(dir, name, in)
	}
	return in, err
}

func (c *CachedGoferBridge) SetAttr(ctx context.Context, ino vfs.InodeID, ch vfs.Change) (vfs.Inode, error) {
	in, err := c.bridge.SetAttr(ctx, ino, ch)
	c.mu.Lock()
	c.attrs.delete(ino)
	if err == nil && c.cfg.AttrTimeout > 0 {
		c.attrs.put(ino, cachedAttr{
			in:      in,
			expires: c.cfg.now().Add(c.cfg.AttrTimeout),
			gen:     c.gen,
		})
	}
	c.mu.Unlock()
	c.invalidates.Add(1)
	return in, err
}

func (c *CachedGoferBridge) Remove(ctx context.Context, dir vfs.InodeID, name string) error {
	c.mu.Lock()
	var childID vfs.InodeID
	if e, ok := c.entries.get(lookupPair{dir: dir, name: name}); ok {
		childID = e.ino
	}
	c.mu.Unlock()

	err := c.bridge.Remove(ctx, dir, name)
	c.invalidateDirMutation(dir, name, childID)
	return err
}

func (c *CachedGoferBridge) Read(ctx context.Context, f *vfs.File, ino vfs.InodeID, buf []byte, off int64) (int, error) {
	return c.bridge.Read(ctx, f, ino, buf, off)
}

func (c *CachedGoferBridge) Write(ctx context.Context, f *vfs.File, ino vfs.InodeID, data []byte, off int64) (int, error) {
	n, err := c.bridge.Write(ctx, f, ino, data, off)
	// Synchronously evict inode attr so subsequent GetAttr/Lookup sees updated size/mtime.
	c.mu.Lock()
	c.attrs.delete(ino)
	c.mu.Unlock()
	c.invalidates.Add(1)
	return n, err
}

func (c *CachedGoferBridge) Flush(ctx context.Context) error {
	return c.bridge.Flush(ctx)
}

func (c *CachedGoferBridge) Resolve(ctx context.Context, p string) (vfs.Inode, error) {
	return resolveVia(ctx, c, p)
}

func (c *CachedGoferBridge) invalidateDirMutation(dir vfs.InodeID, name string, childIno vfs.InodeID) {
	c.mu.Lock()
	c.dirs.delete(dir)
	c.attrs.delete(dir)
	c.entries.delete(lookupPair{dir: dir, name: name})
	if childIno != 0 {
		c.attrs.delete(childIno)
	}
	c.mu.Unlock()
	c.invalidates.Add(1)
}

func (c *CachedGoferBridge) seedCreated(dir vfs.InodeID, name string, in vfs.Inode) {
	now := c.cfg.now()
	c.mu.Lock()
	if c.cfg.EntryTimeout > 0 {
		c.entries.put(lookupPair{dir: dir, name: name}, cachedEntry{
			ino:     in.ID,
			expires: now.Add(c.cfg.EntryTimeout),
			gen:     c.gen,
		})
	}
	if c.cfg.AttrTimeout > 0 {
		c.attrs.put(in.ID, cachedAttr{
			in:      in,
			expires: now.Add(c.cfg.AttrTimeout),
			gen:     c.gen,
		})
	}
	c.mu.Unlock()
}

// --- Range Coalescing (pure, overflow-safe) ---

// RangeRequest describes a single byte range [Off, Off+Len) to read from an object.
type RangeRequest struct {
	Off int64
	Len int64
}

// CoalescedMember maps one original RangeRequest inside a CoalescedSpan.
type CoalescedMember struct {
	OrigIndex int   // index in the caller's []RangeRequest
	SpanOff   int64 // offset within the coalesced span's bytes
	Len       int64 // length in bytes
}

// CoalescedSpan is one merged backend GetRange covering one or more RangeRequests.
type CoalescedSpan struct {
	Off     int64
	Len     int64
	Members []CoalescedMember
}

// CoalesceRanges merges overlapping, contiguous, or near-contiguous ranges
// separated by at most maxGap bytes into spans of at most maxWindow bytes
// (unless a single request itself exceeds maxWindow, in which case it becomes
// its own span). All arithmetic is overflow-checked against signed int64 bounds.
func CoalesceRanges(reqs []RangeRequest, maxGap, maxWindow int64) ([]CoalescedSpan, error) {
	if maxGap < 0 {
		return nil, fmt.Errorf("%w: negative maxGap %d", ErrInvalidRange, maxGap)
	}
	if maxWindow <= 0 {
		return nil, fmt.Errorf("%w: non-positive maxWindow %d", ErrInvalidRange, maxWindow)
	}
	if len(reqs) == 0 {
		return nil, nil
	}
	type indexedReq struct {
		idx int
		off int64
		len int64
		end int64
	}
	sorted := make([]indexedReq, len(reqs))
	for i, r := range reqs {
		if r.Off < 0 || r.Len < 0 || r.Off > math.MaxInt64-r.Len {
			return nil, fmt.Errorf("%w: request[%d] off=%d len=%d", ErrInvalidRange, i, r.Off, r.Len)
		}
		sorted[i] = indexedReq{idx: i, off: r.Off, len: r.Len, end: r.Off + r.Len}
	}
	slices.SortFunc(sorted, func(a, b indexedReq) int {
		if a.off != b.off {
			if a.off < b.off {
				return -1
			}
			return 1
		}
		if a.len != b.len {
			if a.len < b.len {
				return -1
			}
			return 1
		}
		if a.idx < b.idx {
			return -1
		}
		if a.idx > b.idx {
			return 1
		}
		return 0
	})

	var spans []CoalescedSpan
	curStart := sorted[0].off
	curEnd := sorted[0].end
	curMembers := []CoalescedMember{{
		OrigIndex: sorted[0].idx,
		SpanOff:   0,
		Len:       sorted[0].len,
	}}

	for _, r := range sorted[1:] {
		canBridge := false
		if r.off <= curEnd {
			canBridge = true
		} else if r.off-curEnd <= maxGap {
			canBridge = true
		}
		newEnd := curEnd
		if r.end > newEnd {
			newEnd = r.end
		}
		// newEnd >= curStart >= 0, so newEnd - curStart cannot overflow int64.
		if canBridge && (newEnd-curStart) <= maxWindow {
			curEnd = newEnd
			curMembers = append(curMembers, CoalescedMember{
				OrigIndex: r.idx,
				SpanOff:   r.off - curStart,
				Len:       r.len,
			})
			continue
		}
		spans = append(spans, CoalescedSpan{
			Off:     curStart,
			Len:     curEnd - curStart,
			Members: curMembers,
		})
		curStart = r.off
		curEnd = r.end
		curMembers = []CoalescedMember{{
			OrigIndex: r.idx,
			SpanOff:   0,
			Len:       r.len,
		}}
	}
	spans = append(spans, CoalescedSpan{
		Off:     curStart,
		Len:     curEnd - curStart,
		Members: curMembers,
	})
	return spans, nil
}

// --- Boundary 2 Optimizer: OptimizedBackend ---

type cachedShard struct {
	data []byte
	ver  backend.Version
}

type cachedList struct {
	objs    []backend.Object
	expires time.Time
}

type packSpanKey struct {
	key string
	off int64
}

type cachedPackSpan struct {
	key  string
	off  int64
	data []byte
}

// OptimizedBackend wraps any canonical backend.Backend with guarantee-preserving
// optimizations:
//  1. Fenced-Head version/ETag tracker (CAS remains 100% server-side).
//  2. BLAKE3-verified read-only LRU cache for immutable meta/ shards and Stat.
//  3. Bounded-TTL prefix List cache invalidated immediately on local mutation.
//  4. Overflow-safe adjacent RangeGET coalescer for immutable pack/ objects.
type OptimizedBackend struct {
	inner backend.Backend
	cfg   OptimizerConfig

	// Fenced-Head version/ETag tracking (strictly preserves CAS authority).
	headMu       sync.Mutex
	headValid    bool
	headVer      backend.Version
	headPayload  []byte
	headAt       time.Time
	generation   atomic.Uint64
	onInvalidate func()

	// Immutable content-addressed shard cache and immutable object sizes.
	shardMu        sync.Mutex
	shards         *byteLRU[string, cachedShard]
	immutableSizes map[string]int64

	// Bounded-TTL List cache.
	listMu sync.Mutex
	lists  map[string]cachedList

	// Pack range coalescer & bounded LRU span cache.
	packMu      sync.Mutex
	packSpans   *byteLRU[packSpanKey, cachedPackSpan]
	packIndex   map[string][]packSpanKey // spans indexed per pack key
	lastPackEnd map[string]rangeWindow

	// Counters.
	headVerHits    atomic.Int64
	headReadHits   atomic.Int64
	casConflicts   atomic.Int64
	shardHits      atomic.Int64
	shardMisses    atomic.Int64
	statHits       atomic.Int64
	listHits       atomic.Int64
	listMisses     atomic.Int64
	rangeHits      atomic.Int64
	coalescedSpans atomic.Int64
	readAheadBytes atomic.Int64
}

var _ backend.Backend = (*OptimizedBackend)(nil)

// NewOptimizedBackend wraps inner with the configured guarantee-preserving optimizations.
func NewOptimizedBackend(inner backend.Backend, cfg OptimizerConfig) *OptimizedBackend {
	cfg = cfg.normalize()
	return &OptimizedBackend{
		inner:          inner,
		cfg:            cfg,
		shards:         newByteLRU[string, cachedShard](cfg.MaxShardCacheBytes),
		immutableSizes: make(map[string]int64),
		lists:          make(map[string]cachedList),
		packSpans:      newByteLRU[packSpanKey, cachedPackSpan](cfg.MaxPackCacheBytes),
		packIndex:      make(map[string][]packSpanKey),
		lastPackEnd:    make(map[string]rangeWindow),
	}
}

// OnHeadInvalidation registers a callback invoked whenever a Head CAS conflict
// or external Head version advance is observed (e.g. CachedGoferBridge.InvalidateAll).
func (o *OptimizedBackend) OnHeadInvalidation(fn func()) {
	o.headMu.Lock()
	o.onInvalidate = fn
	o.headMu.Unlock()
}

// Generation returns the Head invalidation generation counter.
func (o *OptimizedBackend) Generation() uint64 { return o.generation.Load() }

// CachedHeadVersion returns the last observed backend.Version for "head" when
// EnableHeadVersionCache is active.
func (o *OptimizedBackend) CachedHeadVersion() (backend.Version, bool) {
	if !o.cfg.EnableHeadVersionCache {
		return "", false
	}
	o.headMu.Lock()
	defer o.headMu.Unlock()
	if !o.headValid || o.headVer == "" {
		return "", false
	}
	return o.headVer, true
}

// Stats returns optimization hit/miss/conflict counters for this backend wrapper.
func (o *OptimizedBackend) Stats() OptimizerStats {
	return OptimizerStats{
		HeadVersionHits:     o.headVerHits.Load(),
		HeadReadHits:        o.headReadHits.Load(),
		HeadCASConflicts:    o.casConflicts.Load(),
		ShardCacheHits:      o.shardHits.Load(),
		ShardCacheMisses:    o.shardMisses.Load(),
		ImmutableStatHits:   o.statHits.Load(),
		ListCacheHits:       o.listHits.Load(),
		ListCacheMisses:     o.listMisses.Load(),
		RangeCacheHits:      o.rangeHits.Load(),
		RangeCoalescedSpans: o.coalescedSpans.Load(),
		RangeReadAheadBytes: o.readAheadBytes.Load(),
	}
}

func (o *OptimizedBackend) notifyInvalidation() {
	o.generation.Add(1)
	o.headMu.Lock()
	fn := o.onInvalidate
	o.headMu.Unlock()
	if fn != nil {
		fn()
	}
}

// ConditionalPutHead performs a CAS-preserving conditional write of "head".
// When EnableHeadVersionCache is enabled and a valid ETag/version is cached,
// it skips the redundant pre-PUT GET and executes server-side PutIfVersion
// directly; on backend.ErrConflict it immediately invalidates the cache, reads
// the authoritative Head from the store, runs validate(currentBytes) (which can
// enforce round fencing / ErrStaleRound), and retries PutIfVersion.
//
// When EnableHeadVersionCache is disabled, it always performs an uncached
// Get -> validate -> PutIfVersion cycle.
func (o *OptimizedBackend) ConditionalPutHead(
	ctx context.Context,
	maxBytes int64,
	payload []byte,
	validate func(current []byte) error,
) (backend.Version, error) {
	const maxAttempts = 8
	if v, ok := o.CachedHeadVersion(); ok {
		o.headVerHits.Add(1)
		nv, err := o.PutIfVersion(ctx, "head", payload, v)
		if err == nil {
			return nv, nil
		}
		if !errors.Is(err, backend.ErrConflict) {
			return "", err
		}
		// ErrConflict already invalidated cached head inside PutIfVersion; fall
		// through to authoritative Get + validate + retry loop.
	}
	var lastErr error
	for range maxAttempts {
		cur, ver, err := o.inner.Get(ctx, "head", maxBytes)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				if validate != nil {
					if verr := validate(nil); verr != nil {
						return "", verr
					}
				}
				nv, perr := o.PutIfAbsent(ctx, "head", payload)
				if errors.Is(perr, backend.ErrConflict) {
					lastErr = perr
					continue
				}
				return nv, perr
			}
			return "", err
		}
		o.recordObservedHead(cur, ver)
		if validate != nil {
			if verr := validate(cur); verr != nil {
				return "", verr
			}
		}
		nv, err := o.PutIfVersion(ctx, "head", payload, ver)
		if err == nil {
			return nv, nil
		}
		if errors.Is(err, backend.ErrConflict) {
			lastErr = err
			continue
		}
		return "", err
	}
	return "", fmt.Errorf("cetofs: head CAS exhausted %d attempts: %w", maxAttempts, lastErr)
}

func (o *OptimizedBackend) recordObservedHead(data []byte, ver backend.Version) {
	if !o.cfg.EnableHeadVersionCache && o.cfg.HeadCacheTTL <= 0 {
		return
	}
	changed := false
	o.headMu.Lock()
	if o.headValid && ver != "" && ver != o.headVer {
		changed = true
	}
	o.headValid = ver != ""
	o.headVer = ver
	o.headPayload = append([]byte(nil), data...)
	o.headAt = o.cfg.now()
	o.headMu.Unlock()
	if changed {
		o.notifyInvalidation()
	}
}

func (o *OptimizedBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	// 1. Head read-through (only when HeadCacheTTL > 0; default 0 stays strictly linearizable).
	if key == "head" {
		if o.cfg.HeadCacheTTL > 0 {
			now := o.cfg.now()
			o.headMu.Lock()
			if o.headValid && now.Sub(o.headAt) < o.cfg.HeadCacheTTL {
				if int64(len(o.headPayload)) > max {
					o.headMu.Unlock()
					return nil, "", fmt.Errorf("%w: head exceeds max %d", backend.ErrTooLarge, max)
				}
				cp := append([]byte(nil), o.headPayload...)
				ver := o.headVer
				o.headMu.Unlock()
				o.headReadHits.Add(1)
				return cp, ver, nil
			}
			o.headMu.Unlock()
		}
		data, ver, err := o.inner.Get(ctx, key, max)
		if err == nil {
			o.recordObservedHead(data, ver)
		} else if errors.Is(err, backend.ErrNotFound) {
			o.headMu.Lock()
			o.headValid = false
			o.headVer = ""
			o.headPayload = nil
			o.headMu.Unlock()
		}
		return data, ver, err
	}

	// 2. Digest-verified read-only cache for immutable meta/<2hex>/<64hex> shards.
	if o.cfg.EnableShardCache {
		if wantDigest, ok := parseShardDigest(key); ok {
			o.shardMu.Lock()
			if entry, hit := o.shards.get(key); hit {
				o.shardMu.Unlock()
				if int64(len(entry.data)) > max {
					return nil, "", fmt.Errorf("%w: shard %s exceeds max %d", backend.ErrTooLarge, key, max)
				}
				o.shardHits.Add(1)
				return append([]byte(nil), entry.data...), entry.ver, nil
			}
			o.shardMu.Unlock()
			o.shardMisses.Add(1)

			data, ver, err := o.inner.Get(ctx, key, max)
			if err != nil {
				return nil, "", err
			}
			if blake3.Sum(data) != wantDigest {
				// Fail closed and never cache tampered or corrupted shard bytes.
				return nil, "", fmt.Errorf("%w: %s", ErrIntegrityMismatch, key)
			}
			cp := append([]byte(nil), data...)
			o.shardMu.Lock()
			o.shards.put(key, cachedShard{data: cp, ver: ver}, int64(len(cp)))
			o.immutableSizes[key] = int64(len(cp))
			o.shardMu.Unlock()
			return data, ver, nil
		}
	}

	return o.inner.Get(ctx, key, max)
}

// GetRanges reads multiple byte ranges from key, coalescing adjacent/near-adjacent
// ranges within MaxCoalesceGap and MaxCoalesceWindow into minimal backend GetRange calls.
func (o *OptimizedBackend) GetRanges(ctx context.Context, key string, reqs []RangeRequest) ([][]byte, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if !o.cfg.EnableRangeCoalescing {
		out := make([][]byte, len(reqs))
		for i, r := range reqs {
			if r.Off < 0 || r.Len < 0 || r.Off > math.MaxInt64-r.Len {
				return nil, fmt.Errorf("%w: request[%d] off=%d len=%d", ErrInvalidRange, i, r.Off, r.Len)
			}
			b, err := o.inner.GetRange(ctx, key, r.Off, r.Len)
			if err != nil {
				return nil, err
			}
			out[i] = b
		}
		return out, nil
	}

	spans, err := CoalesceRanges(reqs, o.cfg.MaxCoalesceGap, o.cfg.MaxCoalesceWindow)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(reqs))
	for _, sp := range spans {
		var buf []byte
		if sp.Len == 0 {
			buf, err = o.inner.GetRange(ctx, key, sp.Off, 0)
			if err != nil {
				return nil, err
			}
		} else {
			if cached, ok := o.lookupPackSpan(key, sp.Off, sp.Len); ok {
				buf = cached
				o.rangeHits.Add(1)
			} else {
				buf, err = o.inner.GetRange(ctx, key, sp.Off, sp.Len)
				if err != nil {
					return nil, err
				}
				if strings.HasPrefix(key, "pack/") {
					o.storePackSpan(key, sp.Off, buf)
				}
			}
			if len(sp.Members) > 1 {
				o.coalescedSpans.Add(int64(len(sp.Members) - 1))
			}
		}
		for _, m := range sp.Members {
			piece := make([]byte, m.Len)
			copy(piece, buf[m.SpanOff:m.SpanOff+m.Len])
			out[m.OrigIndex] = piece
		}
	}
	return out, nil
}

func (o *OptimizedBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 || off > math.MaxInt64-n {
		return nil, fmt.Errorf("%w: key=%s off=%d len=%d", ErrInvalidRange, key, off, n)
	}
	if !o.cfg.EnableRangeCoalescing || !strings.HasPrefix(key, "pack/") || n == 0 {
		return o.inner.GetRange(ctx, key, off, n)
	}

	// 1. Check bounded LRU pack span cache.
	if cached, ok := o.lookupPackSpan(key, off, n); ok {
		o.rangeHits.Add(1)
		o.recordPackAccess(key, off, off+n)
		return cached, nil
	}

	// 2. Decide whether to issue a coalesced sequential read-ahead window.
	end := off + n
	adjacent := o.isAdjacentPackAccess(key, off, end)
	shouldReadAhead := (o.cfg.AlwaysReadAhead || (o.cfg.EnableSequentialReadAhead && adjacent)) && o.cfg.ReadAheadWindow > n

	if shouldReadAhead {
		packSize, hasSize := o.getOrFetchPackSize(ctx, key)
		if hasSize && off < packSize && packSize-off >= n {
			avail := packSize - off
			win := o.cfg.ReadAheadWindow
			if win > o.cfg.MaxPackCacheBytes {
				win = o.cfg.MaxPackCacheBytes
			}
			if win < n {
				win = n
			}
			if win > avail {
				win = avail
			}
			if win > n {
				buf, err := o.inner.GetRange(ctx, key, off, win)
				if err != nil {
					return nil, err
				}
				o.storePackSpan(key, off, buf)
				o.recordPackAccess(key, off, end)
				o.coalescedSpans.Add(1)
				o.readAheadBytes.Add(win - n)
				out := make([]byte, n)
				copy(out, buf[:n])
				return out, nil
			}
		}
	}

	// 3. Exact single-range read.
	buf, err := o.inner.GetRange(ctx, key, off, n)
	if err != nil {
		return nil, err
	}
	o.storePackSpan(key, off, buf)
	o.recordPackAccess(key, off, end)
	return buf, nil
}

func (o *OptimizedBackend) lookupPackSpan(key string, off, n int64) ([]byte, bool) {
	end := off + n
	o.packMu.Lock()
	defer o.packMu.Unlock()
	keys := o.packIndex[key]
	alive := keys[:0]
	var found []byte
	for _, k := range keys {
		sp, ok := o.packSpans.get(k)
		if !ok {
			continue
		}
		alive = append(alive, k)
		spEnd := sp.off + int64(len(sp.data))
		if found == nil && off >= sp.off && end <= spEnd {
			rel := off - sp.off
			found = make([]byte, n)
			copy(found, sp.data[rel:rel+n])
		}
	}
	if len(alive) == 0 {
		delete(o.packIndex, key)
	} else {
		o.packIndex[key] = alive
	}
	return found, found != nil
}

func (o *OptimizedBackend) storePackSpan(key string, off int64, data []byte) {
	if int64(len(data)) == 0 || int64(len(data)) > o.cfg.MaxPackCacheBytes {
		return
	}
	cp := append([]byte(nil), data...)
	sk := packSpanKey{key: key, off: off}
	o.packMu.Lock()
	defer o.packMu.Unlock()
	o.packSpans.put(sk, cachedPackSpan{key: key, off: off, data: cp}, int64(len(cp)))
	// Avoid duplicate index entries for the same (key, off).
	exists := false
	for _, k := range o.packIndex[key] {
		if k == sk {
			exists = true
			break
		}
	}
	if !exists {
		o.packIndex[key] = append(o.packIndex[key], sk)
	}
}

func (o *OptimizedBackend) isAdjacentPackAccess(key string, off, end int64) bool {
	o.packMu.Lock()
	defer o.packMu.Unlock()
	prev, ok := o.lastPackEnd[key]
	if !ok {
		return false
	}
	return isAdjacentOrOverlapping(prev.off, prev.end, off, end, o.cfg.MaxCoalesceGap)
}

func (o *OptimizedBackend) recordPackAccess(key string, off, end int64) {
	o.packMu.Lock()
	o.lastPackEnd[key] = rangeWindow{off: off, end: end}
	o.packMu.Unlock()
}

func (o *OptimizedBackend) getOrFetchPackSize(ctx context.Context, key string) (int64, bool) {
	o.shardMu.Lock()
	if sz, ok := o.immutableSizes[key]; ok {
		o.shardMu.Unlock()
		return sz, true
	}
	o.shardMu.Unlock()

	sz, err := o.inner.Stat(ctx, key)
	if err != nil {
		return 0, false
	}
	o.shardMu.Lock()
	o.immutableSizes[key] = sz
	o.shardMu.Unlock()
	return sz, true
}

func (o *OptimizedBackend) Put(ctx context.Context, key string, data []byte) error {
	err := o.inner.Put(ctx, key, data)
	if err == nil {
		o.invalidateListPrefix(key)
		if strings.HasPrefix(key, "pack/") || isImmutableShardKey(key) {
			o.shardMu.Lock()
			o.immutableSizes[key] = int64(len(data))
			o.shardMu.Unlock()
		}
		if key == "head" {
			o.headMu.Lock()
			o.headValid = false
			o.headVer = ""
			o.headPayload = nil
			o.headMu.Unlock()
		}
	}
	return err
}

func (o *OptimizedBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	ver, err := o.inner.PutIfAbsent(ctx, key, data)
	if err == nil {
		o.invalidateListPrefix(key)
		if key == "head" && (o.cfg.EnableHeadVersionCache || o.cfg.HeadCacheTTL > 0) {
			o.headMu.Lock()
			o.headValid = ver != ""
			o.headVer = ver
			o.headPayload = append([]byte(nil), data...)
			o.headAt = o.cfg.now()
			o.headMu.Unlock()
		}
		return ver, nil
	}
	if key == "head" {
		o.headMu.Lock()
		o.headValid = false
		o.headVer = ""
		o.headPayload = nil
		o.headMu.Unlock()
		if errors.Is(err, backend.ErrConflict) {
			o.casConflicts.Add(1)
			o.notifyInvalidation()
		}
	}
	return "", err
}

func (o *OptimizedBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	// Server-side CAS authority is NEVER bypassed: every conditional write goes
	// straight to inner.PutIfVersion.
	ver, err := o.inner.PutIfVersion(ctx, key, data, v)
	if err == nil {
		o.invalidateListPrefix(key)
		if key == "head" && (o.cfg.EnableHeadVersionCache || o.cfg.HeadCacheTTL > 0) {
			o.headMu.Lock()
			o.headValid = ver != ""
			o.headVer = ver
			o.headPayload = append([]byte(nil), data...)
			o.headAt = o.cfg.now()
			o.headMu.Unlock()
		}
		return ver, nil
	}
	if key == "head" {
		// Immediately invalidate cached head on any error or CAS conflict.
		o.headMu.Lock()
		o.headValid = false
		o.headVer = ""
		o.headPayload = nil
		o.headMu.Unlock()
		if errors.Is(err, backend.ErrConflict) {
			o.casConflicts.Add(1)
			o.notifyInvalidation()
		}
	}
	return "", err
}

func (o *OptimizedBackend) Delete(ctx context.Context, key string) error {
	err := o.inner.Delete(ctx, key)
	if err == nil {
		o.invalidateListPrefix(key)
		if key == "head" {
			o.headMu.Lock()
			o.headValid = false
			o.headVer = ""
			o.headPayload = nil
			o.headMu.Unlock()
			o.notifyInvalidation()
		}
	}
	return err
}

func (o *OptimizedBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	if o.cfg.ListCacheTTL > 0 {
		now := o.cfg.now()
		o.listMu.Lock()
		if entry, ok := o.lists[prefix]; ok && now.Before(entry.expires) {
			objs := make([]backend.Object, len(entry.objs))
			copy(objs, entry.objs)
			o.listMu.Unlock()
			o.listHits.Add(1)
			ch := make(chan backend.Object, len(objs))
			errCh := make(chan error, 1)
			for _, obj := range objs {
				ch <- obj
			}
			close(ch)
			close(errCh)
			return ch, errCh
		}
		o.listMu.Unlock()
		o.listMisses.Add(1)

		inObj, inErr := o.inner.List(ctx, prefix)
		outObj := make(chan backend.Object)
		outErr := make(chan error, 1)
		go func() {
			defer close(outObj)
			defer close(outErr)
			var collected []backend.Object
			for obj := range inObj {
				collected = append(collected, obj)
				select {
				case outObj <- obj:
				case <-ctx.Done():
					outErr <- ctx.Err()
					return
				}
			}
			err := <-inErr
			if err != nil {
				outErr <- err
				return
			}
			o.listMu.Lock()
			o.lists[prefix] = cachedList{
				objs:    collected,
				expires: o.cfg.now().Add(o.cfg.ListCacheTTL),
			}
			o.listMu.Unlock()
			if o.cfg.EnableShardCache || o.cfg.EnableRangeCoalescing {
				o.shardMu.Lock()
				for _, obj := range collected {
					if strings.HasPrefix(obj.Key, "pack/") || isImmutableShardKey(obj.Key) {
						o.immutableSizes[obj.Key] = obj.Size
					}
				}
				o.shardMu.Unlock()
			}
		}()
		return outObj, outErr
	}
	return o.inner.List(ctx, prefix)
}

func (o *OptimizedBackend) Stat(ctx context.Context, key string) (int64, error) {
	if o.cfg.EnableShardCache && (isImmutableShardKey(key) || strings.HasPrefix(key, "pack/")) {
		o.shardMu.Lock()
		if sz, ok := o.immutableSizes[key]; ok {
			o.shardMu.Unlock()
			o.statHits.Add(1)
			return sz, nil
		}
		o.shardMu.Unlock()
	}
	sz, err := o.inner.Stat(ctx, key)
	if err == nil && o.cfg.EnableShardCache && (isImmutableShardKey(key) || strings.HasPrefix(key, "pack/")) {
		o.shardMu.Lock()
		o.immutableSizes[key] = sz
		o.shardMu.Unlock()
	}
	return sz, err
}

func (o *OptimizedBackend) invalidateListPrefix(key string) {
	o.listMu.Lock()
	for prefix := range o.lists {
		if strings.HasPrefix(key, prefix) {
			delete(o.lists, prefix)
		}
	}
	o.listMu.Unlock()
}

func (o *OptimizedBackend) Close() error   { return o.inner.Close() }
func (o *OptimizedBackend) String() string { return o.inner.String() }
