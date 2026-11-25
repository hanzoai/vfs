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
// There are four kinds. A write puts bytes at an offset of a file. A size sets a file's
// length (truncate: shorter discards, longer adds zeros), optionally together with an
// updated inode record (SetAttr with Size). A punch deallocates a byte range into a hole.
// A tree change puts and deletes records of the metadata tree, updates the namespace
// counters, forgets removed files, and says how many inode numbers have been handed out:
// create, mkdir, mknod, symlink, link, remove, rename and attribute changes are this kind.
type change struct {
	kind changeKind

	ino   InodeID   // write, size, punch
	off   uint64    // write, punch: where the bytes or hole start
	data  []byte    // write
	size  uint64    // size: new length; punch: hole length
	when  time.Time // write, size, punch: new mtime (and ctime unless ctime is set)
	ctime time.Time // size: explicit ctime when different from when

	muts     []mut     // tree, size (SetAttr with Size): records put and deleted
	gone     []InodeID // tree: non-directory inodes whose last link was dropped
	next     InodeID   // tree: the first inode number not yet handed out
	dFiles   int64     // tree: delta to counters.files
	dBytes   int64     // tree: delta to counters.bytes
	dLinked  int64     // tree: delta to counters.linked
	setLinks bool      // tree: set counters.links = true

	// In-memory only (not encoded in the journal): a replay has no open handles and starts
	// with an empty directory link cache.
	dirBumps []dirBump
	orphans  map[InodeID]Inode

	file   *fileState // write, size, punch: the file's state, which the builder holds; a replay finds it
	sealed []byte     // the record as it was sealed, when the caller sealed it before taking the lock
}

type dirBump struct {
	dir   InodeID
	delta int8
}

type changeKind byte

const (
	changeWrite changeKind = 1 + iota
	changeSize
	changeTree
	changePunch
)

// maxChanges bounds the number of records of one tree change that a decoder accepts: it
// is a bound on what a damaged journal can make a mount allocate, not on what an
// operation builds.
const maxChanges = 1 << 20

func (c *change) encode() []byte { return c.encodeTo(make([]byte, 0, 64+len(c.data))) }

func encodeMuts(w *writer, muts []mut) {
	w.uvarint(uint64(len(muts)))
	for _, m := range muts {
		if m.del {
			w.b = append(w.b, 1)
			w.bytes(m.key)
			continue
		}
		w.b = append(w.b, 0)
		w.bytes(m.key)
		w.bytes(m.val)
	}
}

func decodeMuts(r *reader) ([]mut, error) {
	n := r.uvarint()
	if n > maxChanges {
		return nil, fmt.Errorf("%w: a change of %d records", ErrCorrupt, n)
	}
	var out []mut
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
		out = append(out, m)
	}
	return out, nil
}

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
		ct := c.ctime
		if ct.IsZero() {
			ct = c.when
		}
		w.uvarint(uint64(c.ino))
		w.uvarint(c.size)
		w.varint(c.when.UnixNano())
		w.varint(ct.UnixNano())
		encodeMuts(&w, c.muts)
	case changePunch:
		w.uvarint(uint64(c.ino))
		w.uvarint(c.off)
		w.uvarint(c.size)
		w.varint(c.when.UnixNano())
	case changeTree:
		w.uvarint(uint64(c.next))
		w.varint(c.dFiles)
		w.varint(c.dBytes)
		w.varint(c.dLinked)
		var flags byte
		if c.setLinks {
			flags |= 1
		}
		w.b = append(w.b, flags)
		encodeMuts(&w, c.muts)
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
		c.ctime = c.when
		c.data = bytes.Clone(r.bytes())
	case changeSize:
		c.ino = InodeID(r.uvarint())
		c.size = r.uvarint()
		c.when = time.Unix(0, r.varint())
		c.ctime = time.Unix(0, r.varint())
		var err error
		if c.muts, err = decodeMuts(&r); err != nil {
			return c, err
		}
	case changePunch:
		c.ino = InodeID(r.uvarint())
		c.off = r.uvarint()
		c.size = r.uvarint()
		c.when = time.Unix(0, r.varint())
		c.ctime = c.when
	case changeTree:
		c.next = InodeID(r.uvarint())
		c.dFiles = r.varint()
		c.dBytes = r.varint()
		c.dLinked = r.varint()
		if len(r.b) == 0 || r.b[0] > 1 {
			r.fail()
		} else {
			c.setLinks = r.b[0]&1 != 0
			r.b = r.b[1:]
		}
		var err error
		if c.muts, err = decodeMuts(&r); err != nil {
			return c, err
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
