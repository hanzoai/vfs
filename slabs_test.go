package vfs

import (
	"fmt"
	"math/bits"
	"sync"
	"unsafe"
)

// Every test of this package runs on a pool that poisons what it takes back: the bytes a buffer
// held are overwritten with a pattern the moment it is returned, and when the pool hands the
// buffer out again it checks that nothing wrote to it in between. A reader that still holds a
// returned buffer reads the pattern and fails the test that its data belongs to (the model, the
// crash and the kill tests, uploads that are tried again, a replay beside the publisher); a
// writer that still holds one is caught here and panics; a buffer put back twice, or a view of
// one, is caught as well. The pool keeps what it takes back for ever, so an address never means
// two buffers.
type poisoner struct {
	mu   sync.Mutex
	idle map[int][][]byte    // by the capacity class
	back map[uintptr]poisonT // the buffers that are idle now
}

type poisonT struct{ used, class int }

const poisoned = 0xDD

func init() {
	packSlabs = &poisoner{idle: map[int][][]byte{}, back: map[uintptr]poisonT{}}
}

func classOf(n int) int { return 1 << bits.Len(uint(max(n, 1)-1)) }

func (p *poisoner) Get(n int) []byte {
	if n <= 0 {
		return nil
	}
	class := classOf(n)
	p.mu.Lock()
	var b []byte
	if l := p.idle[class]; len(l) > 0 {
		b, p.idle[class] = l[len(l)-1], l[:len(l)-1]
	}
	var held poisonT
	if b != nil {
		key := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
		held = p.back[key]
		delete(p.back, key)
	}
	p.mu.Unlock()
	if b == nil {
		return make([]byte, n, class)
	}
	for i, c := range b[:held.used] {
		if c != poisoned {
			panic(fmt.Sprintf("vfs: a buffer was written after it was given back (byte %d of %d)", i, held.used))
		}
	}
	return b[:n]
}

func (p *poisoner) Put(b []byte) {
	if cap(b) == 0 {
		return
	}
	class := cap(b)
	if class != classOf(class) {
		return // not one of ours: a buffer that outgrew its slice
	}
	key := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	p.mu.Lock()
	if _, twice := p.back[key]; twice {
		p.mu.Unlock()
		panic("vfs: a buffer was given back twice")
	}
	p.back[key] = poisonT{used: len(b), class: class}
	p.mu.Unlock()
	for i := range b {
		b[i] = poisoned
	}
	p.mu.Lock()
	p.idle[class] = append(p.idle[class], b[:class])
	p.mu.Unlock()
}
