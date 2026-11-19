package vfs

import (
	"sort"
	"sync"
)

// RangeEntry represents a contiguous span of data in a sparse file.
// Optimized for memory: 24 bytes total.
type RangeEntry struct {
	Offset uint64 // Logical offset in the file
	Length uint32 // Length of the span
	ID     uint32 // Reference to the journal/chunk ID
}

// RASKIndex implements a compact range index inspired by RASK (FAST '26).
// It uses a sorted segment array to minimize pointer overhead and 
// maximize cache locality for range queries in sparse files.
type RASKIndex struct {
	mu      sync.RWMutex
	entries []RangeEntry
}

// NewRASKIndex initializes a new compact index.
func NewRASKIndex() *RASKIndex {
	return &RASKIndex{
		entries: make([]RangeEntry, 0, 1024),
	}
}

// Insert adds a new range to the index. If it overlaps or extends an existing 
// range with the same ID, it merges them (Compactness).
func (idx *RASKIndex) Insert(offset uint64, length uint32, id uint32) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Find insertion point
	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Offset >= offset
	})

	// Simple merge logic for contiguous spans from same ID
	if i > 0 && idx.entries[i-1].ID == id && idx.entries[i-1].Offset+uint64(idx.entries[i-1].Length) == offset {
		idx.entries[i-1].Length += length
		return
	}

	// Insert new entry
	idx.entries = append(idx.entries, RangeEntry{})
	copy(idx.entries[i+1:], idx.entries[i:])
	idx.entries[i] = RangeEntry{Offset: offset, Length: length, ID: id}
}

// Lookup finds the entry containing the given offset.
// Returns entry and true if found, empty and false otherwise.
func (idx *RASKIndex) Lookup(offset uint64) (RangeEntry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	n := len(idx.entries)
	if n == 0 {
		return RangeEntry{}, false
	}

	i := sort.Search(n, func(i int) bool {
		return idx.entries[i].Offset > offset
	})

	if i > 0 {
		entry := idx.entries[i-1]
		if offset >= entry.Offset && offset < entry.Offset+uint64(entry.Length) {
			return entry, true
		}
	}

	return RangeEntry{}, false
}

// Len returns the number of active extents (not the file size).
func (idx *RASKIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// MemoryUsage returns estimated bytes used by the index.
func (idx *RASKIndex) MemoryUsage() uintptr {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return uintptr(len(idx.entries)) * 16 // Approximate struct size + overhead
}
