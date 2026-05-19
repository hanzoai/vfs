package vfs

import (
	"strings"
	"testing"
)

func TestCacheBasic(t *testing.T) {
	c := NewCache(0) // unbounded
	id1 := BlockID(strings.Repeat("a", 64))
	c.Put(id1, []byte("hello"))
	v, ok := c.Get(id1)
	if !ok || string(v) != "hello" {
		t.Fatalf("get hit: ok=%v val=%q", ok, v)
	}
	c.Delete(id1)
	if _, ok := c.Get(id1); ok {
		t.Fatal("delete should remove")
	}
}

func TestCacheLRUEviction(t *testing.T) {
	// Cap at 100 bytes; 4 entries × 50 bytes each → first two evict.
	c := NewCache(100)
	for i := 0; i < 4; i++ {
		id := BlockID(strings.Repeat(string(rune('a'+i)), 64))
		c.Put(id, make([]byte, 50))
	}
	entries, bytesIn, _ := c.Stats()
	if entries > 2 {
		t.Fatalf("expected ≤2 entries after eviction, got %d", entries)
	}
	if bytesIn > 100 {
		t.Fatalf("expected ≤100 bytes after eviction, got %d", bytesIn)
	}
	// Oldest should be gone
	if _, ok := c.Get(BlockID(strings.Repeat("a", 64))); ok {
		t.Fatal("oldest entry should have been evicted")
	}
}

func TestCacheTouchPromotes(t *testing.T) {
	c := NewCache(100)
	idA := BlockID(strings.Repeat("a", 64))
	idB := BlockID(strings.Repeat("b", 64))
	idC := BlockID(strings.Repeat("c", 64))
	c.Put(idA, make([]byte, 50))
	c.Put(idB, make([]byte, 50))
	// touch A so it's most-recently-used
	if _, ok := c.Get(idA); !ok {
		t.Fatal("A missing pre-promote")
	}
	c.Put(idC, make([]byte, 50)) // should evict B, not A
	if _, ok := c.Get(idA); !ok {
		t.Fatal("A should survive (was touched)")
	}
	if _, ok := c.Get(idB); ok {
		t.Fatal("B should have been evicted")
	}
}
