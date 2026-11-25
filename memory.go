package vfs

import (
	"sync"
	"time"

	"github.com/hanzoai/vfs/internal/bufpool"
)

// Budget bounds the memory a process holds for changes the store does not have yet: the
// writes of every Volume that was given it, and the packs on their way to the store. A node
// runs many Volumes, and a limit on each multiplies into a limit on none; with one Budget in
// Config the sum is bounded. A change that finds the Budget full waits for a publication of any
// Volume to free memory, and is refused (ErrFull) when its own Volume's publication is failing.
// Memory is counted when a change is applied and when a pack is made. What the Budget admits stops
// at it, plus one change of each Volume that held nothing (such a Volume is always let in); a pack
// is made from changes already counted, lives beside them until the store has it, and is made
// only when the Budget is not spent or the Volume has no other on its way, so the memory held is at
// most twice the Budget and one change per Volume.
type Budget struct {
	limit int64

	mu   sync.Mutex
	used int64
	wake chan struct{}
}

// NewBudget returns a Budget of limit bytes.
func NewBudget(limit int64) *Budget {
	return &Budget{limit: max(limit, 1), wake: make(chan struct{})}
}

// Used is the bytes held now.
func (b *Budget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Limit is the bound.
func (b *Budget) Limit() int64 { return b.limit }

func (b *Budget) add(n int64) {
	if b == nil || n == 0 {
		return
	}
	b.mu.Lock()
	b.used += n
	if n < 0 {
		close(b.wake)
		b.wake = make(chan struct{})
	}
	b.mu.Unlock()
}

// full reports whether the Budget is spent, and returns the channel that closes when memory
// is freed.
func (b *Budget) full() (bool, <-chan struct{}) {
	if b == nil {
		return false, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used >= b.limit, b.wake
}

// slabs is where the large buffers of the write path (a pack, a sealed record, a window of a
// file that is cut) come from and go back to. A buffer is given back only after its last reader
// is done with it: a pack when the store has it, a window when the chunks cut from it were sealed.
// A generation holds the pack until the store has it, so an upload that is tried again reads the
// same bytes.
type slabs interface {
	Get(n int) []byte
	Put(b []byte)
}

// packSlabs is the pool of the process: the idle memory it keeps is bounded, and decays.
var packSlabs slabs = bufpool.NewBudget(256<<20, time.Minute).Pool(false)
