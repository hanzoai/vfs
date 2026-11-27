package vfs

import (
	"container/list"
	"sync"
)

// lru is a byte-bounded least-recently-used cache. Values are immutable once
// added, so a hit may be shared between goroutines.
type lru[K comparable, V any] struct {
	mu   sync.Mutex
	max  int64
	used int64
	ll   *list.List
	m    map[K]*list.Element
}

type lruItem[K comparable, V any] struct {
	k    K
	v    V
	cost int64
}

func newLRU[K comparable, V any](max int64) *lru[K, V] {
	return &lru[K, V]{max: max, ll: list.New(), m: map[K]*list.Element{}}
}

func (c *lru[K, V]) get(k K) (v V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.m[k]
	if !ok {
		return v, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*lruItem[K, V]).v, true
}

func (c *lru[K, V]) add(k K, v V, cost int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[k]; ok {
		it := el.Value.(*lruItem[K, V])
		c.used += cost - it.cost
		it.v, it.cost = v, cost
		c.ll.MoveToFront(el)
	} else {
		c.m[k] = c.ll.PushFront(&lruItem[K, V]{k: k, v: v, cost: cost})
		c.used += cost
	}
	for c.used > c.max && c.ll.Len() > 1 {
		el := c.ll.Back()
		it := el.Value.(*lruItem[K, V])
		c.ll.Remove(el)
		delete(c.m, it.k)
		c.used -= it.cost
	}
}

// clear empties the cache safely while held by concurrent callers.
func (c *lru[K, V]) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used = 0
	c.ll.Init()
	clear(c.m)
}

// stats returns the bytes held and the bound.
func (c *lru[K, V]) stats() (used, max int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.max
}
