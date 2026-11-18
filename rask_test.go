package vfs

import (
	"fmt"
	"runtime"
	"testing"
)

func TestRASK_Sparse(t *testing.T) {
	idx := NewRASKIndex()

	// Simulate a 1 PiB sparse file with a write at the very end
	onePiB := uint64(1 << 50)
	idx.Insert(0, 4096, 1)
	idx.Insert(onePiB-4096, 4096, 99)

	if idx.Len() != 2 {
		t.Errorf("Expected 2 entries, got %d", idx.Len())
	}

	// Test lookup at start
	e, found := idx.Lookup(1024)
	if !found || e.ID != 1 {
		t.Error("Failed to find start range")
	}

	// Test lookup in the "hole"
	_, found = idx.Lookup(onePiB / 2)
	if found {
		t.Error("Found data in a sparse hole")
	}

	// Test lookup at end
	e, found = idx.Lookup(onePiB - 100)
	if !found || e.ID != 99 {
		t.Error("Failed to find end range")
	}
}

func BenchmarkRASK_Lookup_1M(b *testing.B) {
	idx := NewRASKIndex()
	numEntries := 1000000
	for i := 0; i < numEntries; i++ {
		idx.Insert(uint64(i*8192), 4096, uint32(i))
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	fmt.Printf("Memory for 1M extents: %v MiB\n", m1.Alloc/1024/1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Lookup(uint64((i % numEntries) * 8192))
	}
}

func TestRASK_MergeContiguous(t *testing.T) {
	idx := NewRASKIndex()
	// Insert three contiguous blocks from same ID
	idx.Insert(0, 1024, 10)
	idx.Insert(1024, 1024, 10)
	idx.Insert(2048, 1024, 10)

	if idx.Len() != 1 {
		t.Errorf("Expected merged entry, got %d entries", idx.Len())
	}

	e, _ := idx.Lookup(1500)
	if e.Length != 3072 {
		t.Errorf("Expected merged length 3072, got %d", e.Length)
	}
}
