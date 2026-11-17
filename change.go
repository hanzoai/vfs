package vfs

import (
	"bytes"
	"fmt"
	"time"
)

// A change is one mutation of a Volume, as the journal records it. The value a caller's
// operation builds is the value that is sealed, appended to the journal and applied, and
// the value a mount decodes from the journal and applies again after a crash: one
// definition of what each change does, so what a replay produces cannot differ from
// what the caller saw.
//
// There are three kinds. A write puts bytes at an offset of a file. A size sets a
// file's length (truncate: shorter discards, longer adds zeros). A tree change puts and
// deletes records of the metadata tree, forgets the files it removes, and says how many
// inode numbers have been handed out: create, mkdir, remove and every operation that is
// only a change to records (rename, link, attributes) are this one kind, so a new
// operation adds nothing to the journal's format.
type change struct {
	kind changeKind

	ino  InodeID   // write, size
	off  uint64    // write: where the bytes go
	data []byte    // write
	size uint64    // size
	when time.Time // write, size: the file's new modification time

	muts []mut     // tree: records put and deleted
	gone []InodeID // tree: files removed; their data records are deleted by the generation that publishes the change
	next InodeID   // tree: the first inode number not yet handed out

	file   *fileState // write, size: the file's state, which the builder holds; a replay finds it
	sealed []byte     // the record as it was sealed, when the caller sealed it before taking the lock
}

type changeKind byte

const (
	changeWrite changeKind = 1 + iota
	changeSize
	changeTree
)

// maxChanges bounds the number of records of one tree change that a decoder accepts: it
// is a bound on what a damaged journal can make a mount allocate, not on what an
// operation builds.
const maxChanges = 1 << 20

func (c *change) encode() []byte { return c.encodeTo(make([]byte, 0, 64+len(c.data))) }

// encodeTo appends the change to b.
func (c *change) encodeTo(b []byte) []byte {
	w := writer{b: b}
	w.b = append(w.b, byte(c.kind))
	switch c.kind {
	case changeWrite:
		w.uvarint(uint64(c.ino))
		w.uvarint(c.off)
		w.varint(c.when.UnixNano())
		w.bytes(c.data)
	case changeSize:
		w.uvarint(uint64(c.ino))
		w.uvarint(c.size)
		w.varint(c.when.UnixNano())
	case changeTree:
		w.uvarint(uint64(c.next))
		w.uvarint(uint64(len(c.muts)))
		for _, m := range c.muts {
			if m.del {
				w.b = append(w.b, 1)
				w.bytes(m.key)
				continue
			}
			w.b = append(w.b, 0)
			w.bytes(m.key)
			w.bytes(m.val)
		}
		w.uvarint(uint64(len(c.gone)))
		for _, g := range c.gone {
			w.uvarint(uint64(g))
		}
	}
	return w.b
}

// decodeChange reads a change. What it returns holds no reference to b.
func decodeChange(b []byte) (change, error) {
	var c change
	if len(b) == 0 {
		return c, fmt.Errorf("%w: an empty change", ErrCorrupt)
	}
	r := reader{b: b[1:]}
	c.kind = changeKind(b[0])
	switch c.kind {
	case changeWrite:
		c.ino = InodeID(r.uvarint())
		c.off = r.uvarint()
		c.when = time.Unix(0, r.varint())
		c.data = bytes.Clone(r.bytes())
	case changeSize:
		c.ino = InodeID(r.uvarint())
		c.size = r.uvarint()
		c.when = time.Unix(0, r.varint())
	case changeTree:
		c.next = InodeID(r.uvarint())
		n := r.uvarint()
		if n > maxChanges {
			return c, fmt.Errorf("%w: a change of %d records", ErrCorrupt, n)
		}
		for range n {
			if len(r.b) == 0 {
				r.fail()
				break
			}
			del := r.b[0] == 1
			if r.b[0] > 1 {
				r.fail()
				break
			}
			r.b = r.b[1:]
			m := mut{key: bytes.Clone(r.bytes()), del: del}
			if !del {
				m.val = bytes.Clone(r.bytes())
			}
			c.muts = append(c.muts, m)
		}
		g := r.uvarint()
		if g > maxChanges {
			return c, fmt.Errorf("%w: a change forgetting %d files", ErrCorrupt, g)
		}
		for range g {
			c.gone = append(c.gone, InodeID(r.uvarint()))
		}
	default:
		return c, fmt.Errorf("%w: a change of kind %d", ErrCorrupt, b[0])
	}
	if err := r.done(); err != nil {
		return c, err
	}
	return c, nil
}
