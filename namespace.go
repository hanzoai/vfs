package vfs

import (
	"context"
	"fmt"
	"sync"
)

// The Head's fields for what the namespace needs to say about itself (SPECIFICATION 5.1):
// tag 2 is the `links` field, which bit 2 of Needs announces, and tag 64 is `usage`, a
// field that no bit announces because nothing depends on it.
const (
	tagLinks = 2
	tagUsage = 64

	needsLinks uint64 = 1 << tagLinks
)

// counters is what a Volume keeps beside its tree and writes into every Head it commits,
// so that statfs costs nothing and a link count is announced. mu guards them.
type counters struct {
	files  uint64 // inodes in the tree
	bytes  uint64 // bytes stored for files and symbolic links: inline records and extents
	linked uint64 // inodes with more than one name
	links  bool   // the Volume has had a hard link, so its Head carries the links field
}

// load reads the counters out of a Head. A Head without the usage field describes a
// Volume of the root directory and nothing else.
func (c *counters) load(h Head) error {
	*c = counters{files: 1}
	if val, ok := tailGet(h.Tail, tagUsage); ok {
		r := reader{b: val}
		c.files, c.bytes = r.uvarint(), r.uvarint()
		if err := r.done(); err != nil {
			return fmt.Errorf("%w: the Head's usage field", err)
		}
	}
	if h.Needs&needsLinks != 0 {
		val, _ := tailGet(h.Tail, tagLinks)
		r := reader{b: val}
		c.linked, c.links = r.uvarint(), true
		if err := r.done(); err != nil {
			return fmt.Errorf("%w: the Head's links field", err)
		}
	}
	return nil
}

// stamp writes the counters into a Head about to be committed.
func (c *counters) stamp(h *Head) {
	var w writer
	w.uvarint(c.files)
	w.uvarint(c.bytes)
	h.Tail = tailSet(h.Tail, tagUsage, w.b)
	if c.links {
		w = writer{}
		w.uvarint(c.linked)
		h.Needs |= needsLinks
		h.Tail = tailSet(h.Tail, tagLinks, w.b)
	}
}

// tailGet returns the value of the field with the given tag.
func tailGet(tail string, tag uint64) ([]byte, bool) {
	r := reader{b: []byte(tail)}
	for len(r.b) > 0 {
		t, val := r.uvarint(), r.bytes()
		if r.err != nil {
			return nil, false
		}
		if t == tag {
			return val, true
		}
	}
	return nil, false
}

// tailSet returns tail with the field of the given tag set to val, in its place among
// the others: tags stay strictly increasing and every other field is kept, byte for byte.
func tailSet(tail string, tag uint64, val []byte) string {
	var w writer
	r := reader{b: []byte(tail)}
	put := func() { w.uvarint(tag); w.bytes(val) }
	done := false
	for len(r.b) > 0 {
		start := r.b
		t := r.uvarint()
		r.bytes()
		if r.err != nil {
			return tail // not a Tail: left for the Head's own check to refuse
		}
		field := start[:len(start)-len(r.b)]
		switch {
		case t == tag:
			put()
			done = true
		case t > tag && !done:
			put()
			done = true
			w.raw(field)
		default:
			w.raw(field)
		}
	}
	if !done {
		put()
	}
	return string(w.b)
}

// dirCache holds the link counts of directories that were asked for: 2 and the number of
// subdirectories. A directory's count is derived from its entries, never stored, so a
// Volume that has directories needs no feature bit; the cache is what stops a stat of a
// directory from reading its entries every time. An operation that adds or removes a
// subdirectory of a directory moves its entry, so that a directory that gains a file
// every millisecond is not read again each time it is stat'd.
type dirCache struct {
	mu sync.Mutex
	n  map[InodeID]uint32
}

const dirCacheMax = 1 << 16

func (c *dirCache) get(dir InodeID) (uint32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.n[dir]
	return n, ok
}

func (c *dirCache) set(dir InodeID, n uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil || len(c.n) >= dirCacheMax {
		c.n = make(map[InodeID]uint32)
	}
	c.n[dir] = n
}

// bump moves the count of dir by delta if it is cached; one that is not is counted from the
// entries when it is asked for.
func (c *dirCache) bump(dir InodeID, delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.n[dir]; ok {
		c.n[dir] = uint32(int(n) + delta)
	}
}

// each calls fn for every directory whose count is cached.
func (c *dirCache) each(fn func(dir InodeID, n uint32)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for dir, n := range c.n {
		fn(dir, n)
	}
}

// dirLinks returns 2 and the number of subdirectories of dir.
func (v *Volume) dirLinks(ctx context.Context, dir InodeID) (uint32, error) {
	if n, ok := v.dirn.get(dir); ok {
		return n, nil
	}
	recs, err := v.scan(ctx, key(tEntry, uint64(dir)))
	if err != nil {
		return 0, err
	}
	n := uint32(2)
	for _, r := range recs {
		_, mode, err := decodeEntry(r.val)
		if err != nil {
			return 0, err
		}
		if mode.IsDir() {
			n++
		}
	}
	v.dirn.set(dir, n)
	return n, nil
}
