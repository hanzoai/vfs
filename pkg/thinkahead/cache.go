// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package thinkahead

import (
	"container/list"
	"sync"
)

// ScopedChunkKey pairs a derived ScopeKey hash with a ChunkID so cache lines
// are strictly isolated per (tenant_id, repo_hash, workflow_name). Even if two
// tenants hold identical bytes, Tenant B cannot observe a cache hit or timing
// side channel from Tenant A's prefetch or demand read.
type ScopedChunkKey struct {
	ScopeHash [32]byte
	ID        ChunkID
}

// CacheStats reports segmented node-cache usage and eviction counters.
type CacheStats struct {
	MaxBytes              int64 `json:"max_bytes"`
	UsedBytes             int64 `json:"used_bytes"`
	ProbationaryBytes     int64 `json:"probationary_bytes"`
	ProtectedBytes        int64 `json:"protected_bytes"`
	UnreadBytes           int64 `json:"unread_bytes"`
	Entries               int   `json:"entries"`
	PrefetchAdmitted      int64 `json:"prefetch_admitted"`
	PrefetchAdmittedBytes int64 `json:"prefetch_admitted_bytes"`
	PrefetchRejected      int64 `json:"prefetch_rejected"`
	PrefetchConsumed      int64 `json:"prefetch_consumed"`
	PrefetchConsumedBytes int64 `json:"prefetch_consumed_bytes"`
	EvictedUnusedCount    int64 `json:"evicted_unused_count"`
	EvictedUnusedBytes    int64 `json:"evicted_unused_bytes"`
	EvictedProtectedCount int64 `json:"evicted_protected_count"`
	EvictedProtectedBytes int64 `json:"evicted_protected_bytes"`
}

type cacheSegment uint8

const (
	segProbationary cacheSegment = iota
	segProtected
)

type cacheEntry struct {
	key        ScopedChunkKey
	data       []byte
	size       int64
	seg        cacheSegment
	prefetched bool
	consumed   bool
	pinned     int
	el         *list.Element
}

// SegmentedCache is a byte-bounded 2Q/SLRU node cache implementing
// docs/prefetch.md S5:
//   - Prefetched chunks enter the probationary segment and count toward UnreadBytes.
//   - First demand read promotes a probationary chunk to the protected segment
//     and releases its UnreadBytes reservation.
//   - Eviction ALWAYS evicts unpinned probationary entries before protected entries.
//   - Prefetch admission NEVER evicts protected entries (S5.1 acceptance).
//   - Pinned (dirty/unpublished) entries are never evicted (S5.2).
type SegmentedCache struct {
	mu          sync.Mutex
	maxBytes    int64
	probBytes   int64
	protBytes   int64
	unreadBytes int64
	prob        *list.List // front = MRU, back = LRU
	prot        *list.List // front = MRU, back = LRU
	entries     map[ScopedChunkKey]*cacheEntry
	onProbEvict func(key ScopedChunkKey, size int64)

	prefetchAdmitted      int64
	prefetchAdmittedBytes int64
	prefetchRejected      int64
	prefetchConsumed      int64
	prefetchConsumedBytes int64
	evictedUnusedCount    int64
	evictedUnusedBytes    int64
	evictedProtectedCount int64
	evictedProtectedBytes int64
}

// NewSegmentedCache allocates a byte-bounded segmented cache.
func NewSegmentedCache(maxBytes int64) *SegmentedCache {
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	return &SegmentedCache{
		maxBytes: maxBytes,
		prob:     list.New(),
		prot:     list.New(),
		entries:  make(map[ScopedChunkKey]*cacheEntry, 512),
	}
}

// SetProbationaryEvictCallback registers a callback invoked (outside internal
// mutations) whenever an unread probationary chunk is evicted.
func (c *SegmentedCache) SetProbationaryEvictCallback(fn func(key ScopedChunkKey, size int64)) {
	c.mu.Lock()
	c.onProbEvict = fn
	c.mu.Unlock()
}

// Contains reports whether key is currently cached without changing LRU order.
func (c *SegmentedCache) Contains(key ScopedChunkKey) bool {
	c.mu.Lock()
	_, ok := c.entries[key]
	c.mu.Unlock()
	return ok
}

// UnreadBytes returns bytes currently held in the cache from prefetch that
// have not yet been consumed by a demand read.
func (c *SegmentedCache) UnreadBytes() int64 {
	c.mu.Lock()
	u := c.unreadBytes
	c.mu.Unlock()
	return u
}

// Get serves a foreground demand read. If the entry is in the probationary
// segment, it is promoted to protected and marked consumed.
func (c *SegmentedCache) Get(key ScopedChunkKey) (data []byte, ok bool, fromPrefetch bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found {
		return nil, false, false
	}
	if e.seg == segProbationary {
		c.prob.Remove(e.el)
		c.probBytes -= e.size
		e.seg = segProtected
		e.el = c.prot.PushFront(e)
		c.protBytes += e.size
		if e.prefetched && !e.consumed {
			e.consumed = true
			c.unreadBytes -= e.size
			c.prefetchConsumed++
			c.prefetchConsumedBytes += e.size
			fromPrefetch = true
		}
	} else {
		c.prot.MoveToFront(e.el)
	}
	return e.data, true, fromPrefetch
}

// AddPrefetch inserts a prefetched chunk into the probationary segment.
// Per S5.1, it may evict older unpinned probationary entries to make room, but
// NEVER evicts any protected entry. If protected entries leave insufficient
// room, the prefetched chunk is cleanly rejected and false is returned.
func (c *SegmentedCache) AddPrefetch(key ScopedChunkKey, data []byte) bool {
	size := int64(len(data))
	if size == 0 {
		return false
	}
	var evicted []cacheEntry
	var cb func(ScopedChunkKey, int64)

	c.mu.Lock()
	if _, exists := c.entries[key]; exists {
		c.mu.Unlock()
		return false
	}
	if size > c.maxBytes || c.protBytes+size > c.maxBytes {
		c.prefetchRejected++
		c.mu.Unlock()
		return false
	}
	// Evict only unpinned probationary entries until the new entry fits.
	for c.probBytes+c.protBytes+size > c.maxBytes {
		vic := c.findEvictableTail(c.prob)
		if vic == nil {
			// Only pinned probationary entries remain; refuse admission.
			c.prefetchRejected++
			c.mu.Unlock()
			return false
		}
		c.removeLocked(vic)
		if vic.prefetched && !vic.consumed {
			c.evictedUnusedCount++
			c.evictedUnusedBytes += vic.size
			evicted = append(evicted, *vic)
		}
	}
	e := &cacheEntry{
		key:        key,
		data:       data,
		size:       size,
		seg:        segProbationary,
		prefetched: true,
	}
	e.el = c.prob.PushFront(e)
	c.entries[key] = e
	c.probBytes += size
	c.unreadBytes += size
	c.prefetchAdmitted++
	c.prefetchAdmittedBytes += size
	cb = c.onProbEvict
	c.mu.Unlock()

	if cb != nil {
		for _, vic := range evicted {
			cb(vic.key, vic.size)
		}
	}
	return true
}

// AddDemand inserts a demand-fetched chunk directly into the protected segment,
// evicting unpinned probationary entries first, then unpinned protected entries.
func (c *SegmentedCache) AddDemand(key ScopedChunkKey, data []byte) {
	size := int64(len(data))
	if size == 0 || size > c.maxBytes {
		return
	}
	var evicted []cacheEntry
	var cb func(ScopedChunkKey, int64)

	c.mu.Lock()
	if existing, ok := c.entries[key]; ok {
		if existing.seg == segProbationary {
			c.prob.Remove(existing.el)
			c.probBytes -= existing.size
			if existing.prefetched && !existing.consumed {
				existing.consumed = true
				c.unreadBytes -= existing.size
				c.prefetchConsumed++
				c.prefetchConsumedBytes += existing.size
			}
			existing.seg = segProtected
			existing.data = data
			existing.size = size
			existing.el = c.prot.PushFront(existing)
			c.protBytes += size
		} else {
			c.protBytes += size - existing.size
			existing.data = data
			existing.size = size
			c.prot.MoveToFront(existing.el)
		}
		c.mu.Unlock()
		return
	}

	for c.probBytes+c.protBytes+size > c.maxBytes {
		if vic := c.findEvictableTail(c.prob); vic != nil {
			c.removeLocked(vic)
			if vic.prefetched && !vic.consumed {
				c.evictedUnusedCount++
				c.evictedUnusedBytes += vic.size
				evicted = append(evicted, *vic)
			}
			continue
		}
		if vic := c.findEvictableTail(c.prot); vic != nil {
			c.removeLocked(vic)
			c.evictedProtectedCount++
			c.evictedProtectedBytes += vic.size
			continue
		}
		// All cached entries are pinned; do not exceed maxBytes by dropping demand cache.
		c.mu.Unlock()
		return
	}

	e := &cacheEntry{
		key:      key,
		data:     data,
		size:     size,
		seg:      segProtected,
		consumed: true,
	}
	e.el = c.prot.PushFront(e)
	c.entries[key] = e
	c.protBytes += size
	cb = c.onProbEvict
	c.mu.Unlock()

	if cb != nil {
		for _, vic := range evicted {
			cb(vic.key, vic.size)
		}
	}
}

// Pin increments the pin count on a cached entry (dirty/unpublished protection, S5.2).
func (c *SegmentedCache) Pin(key ScopedChunkKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return false
	}
	e.pinned++
	return true
}

// Unpin decrements the pin count on a cached entry.
func (c *SegmentedCache) Unpin(key ScopedChunkKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok && e.pinned > 0 {
		e.pinned--
	}
}

// EvictScope removes all entries belonging to a specific scopeHash.
func (c *SegmentedCache) EvictScope(scopeHash [32]byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int
	for k, e := range c.entries {
		if k.ScopeHash == scopeHash && e.pinned == 0 {
			c.removeLocked(e)
			n++
		}
	}
	return n
}

func (c *SegmentedCache) findEvictableTail(ll *list.List) *cacheEntry {
	for el := ll.Back(); el != nil; el = el.Prev() {
		e := el.Value.(*cacheEntry)
		if e.pinned == 0 {
			return e
		}
	}
	return nil
}

func (c *SegmentedCache) removeLocked(e *cacheEntry) {
	if e.seg == segProbationary {
		c.prob.Remove(e.el)
		c.probBytes -= e.size
		if e.prefetched && !e.consumed {
			c.unreadBytes -= e.size
		}
	} else {
		c.prot.Remove(e.el)
		c.protBytes -= e.size
	}
	delete(c.entries, e.key)
}

// Stats returns a snapshot of cache utilization and eviction metrics.
func (c *SegmentedCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		MaxBytes:              c.maxBytes,
		UsedBytes:             c.probBytes + c.protBytes,
		ProbationaryBytes:     c.probBytes,
		ProtectedBytes:        c.protBytes,
		UnreadBytes:           c.unreadBytes,
		Entries:               len(c.entries),
		PrefetchAdmitted:      c.prefetchAdmitted,
		PrefetchAdmittedBytes: c.prefetchAdmittedBytes,
		PrefetchRejected:      c.prefetchRejected,
		PrefetchConsumed:      c.prefetchConsumed,
		PrefetchConsumedBytes: c.prefetchConsumedBytes,
		EvictedUnusedCount:    c.evictedUnusedCount,
		EvictedUnusedBytes:    c.evictedUnusedBytes,
		EvictedProtectedCount: c.evictedProtectedCount,
		EvictedProtectedBytes: c.evictedProtectedBytes,
	}
}
