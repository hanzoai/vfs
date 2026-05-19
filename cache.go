package vfs

import (
	"container/list"
	"sync"
)

// Cache is an in-memory LRU bound by total bytes. Used as the hot tier
// in front of the backend. The on-disk cache layer (NVMe write-back)
// is a future addition (0.2.0); v0.1.0 keeps everything resident in RAM
// up to MaxBytes.
type Cache struct {
	mu       sync.Mutex
	items    map[BlockID]*list.Element
	order    *list.List
	bytesIn  int64
	maxBytes int64
}

type cacheEntry struct {
	id  BlockID
	val []byte
}

// NewCache creates an LRU cache holding up to maxBytes worth of blocks.
// maxBytes <= 0 disables eviction (unlimited; for tests).
func NewCache(maxBytes int64) *Cache {
	return &Cache{
		items:    make(map[BlockID]*list.Element),
		order:    list.New(),
		maxBytes: maxBytes,
	}
}

// Get returns the cached block (or nil, false on miss).
func (c *Cache) Get(id BlockID) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[id]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*cacheEntry).val, true
}

// Put inserts or updates a block. Older blocks are evicted to keep the
// total under maxBytes.
func (c *Cache) Put(id BlockID, val []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[id]; ok {
		entry := el.Value.(*cacheEntry)
		c.bytesIn += int64(len(val) - len(entry.val))
		entry.val = val
		c.order.MoveToFront(el)
		return
	}
	el := c.order.PushFront(&cacheEntry{id: id, val: val})
	c.items[id] = el
	c.bytesIn += int64(len(val))
	c.evictIfOver()
}

// Delete removes a single entry.
func (c *Cache) Delete(id BlockID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[id]; ok {
		c.bytesIn -= int64(len(el.Value.(*cacheEntry).val))
		c.order.Remove(el)
		delete(c.items, id)
	}
}

// Stats returns (entries, bytes-in-use, max-bytes).
func (c *Cache) Stats() (entries int, bytesInUse int64, maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items), c.bytesIn, c.maxBytes
}

func (c *Cache) evictIfOver() {
	if c.maxBytes <= 0 {
		return
	}
	for c.bytesIn > c.maxBytes {
		back := c.order.Back()
		if back == nil {
			return
		}
		entry := back.Value.(*cacheEntry)
		c.order.Remove(back)
		delete(c.items, entry.id)
		c.bytesIn -= int64(len(entry.val))
	}
}
