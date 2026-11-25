package vfs

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"time"
)

// All metadata is records in one ordered tree, keyed by bytes:
//
//	[tInode][ino]                  inode attributes
//	[tEntry][dir ino][name]        directory entry: child ino and type
//	[tData][ino]                   a small file's bytes, inline
//	[tData][ino][offset]           a large file's chunk reference, by file offset
//	[tChunk][chunk id]             where a chunk lives: pack, offset, length
//
// Numbers in keys are big-endian so byte order is numeric order. Records of one
// directory, or of one file, are contiguous, and the inodes of consecutive
// numbers are dense in one table, so a listing or a stat sweep reads few shards.
const (
	tInode byte = 1
	tEntry byte = 2
	tData  byte = 3
	tChunk byte = 4

	// prefixLen is the length of the [table][ino] prefix every scan is by.
	prefixLen = 9
)

// InodeID names a file or directory in a Volume. IDs are assigned in increasing
// order and never reused within a Volume.
type InodeID uint64

// RootInode is the root directory of every Volume.
const RootInode InodeID = 1

// Inode is the attributes of a file or directory.
type Inode struct {
	ID    InodeID
	Mode  fs.FileMode // type, permission, setuid, setgid and sticky bits
	Size  uint64      // bytes, exact
	Mtime time.Time
	Atime time.Time
	Ctime time.Time

	// UID and GID own the inode. An inode with no owner record belongs to whoever runs
	// the mount, which a Volume does not know: both read as zero then, and Owned says so.
	UID, GID uint32
	Owned    bool

	// Nlink is the number of names the inode has: for a directory 2 and one more for each
	// subdirectory, for anything else the directory entries that name it. It is 0 for an
	// inode that was unlinked while a handle still held it. A directory's is computed
	// when it is asked for (Stat, Lookup); it is not stored.
	Nlink uint32

	// Parent is the directory that names a directory, 0 for the root and for anything
	// that is not a directory.
	Parent InodeID

	// later holds the fields of the stored record this library does not know
	// (a newer library wrote them), so a rewrite carries them on.
	later []byte
}

// IsDir reports whether the inode is a directory.
func (i Inode) IsDir() bool { return i.Mode.IsDir() }

// DirEntry is one name in a directory. Mode carries the type bits only; Stat
// the child for permissions and size.
type DirEntry struct {
	Name string
	ID   InodeID
	Mode fs.FileMode
}

func key(table byte, ino uint64, suffix ...byte) []byte {
	k := make([]byte, prefixLen, prefixLen+len(suffix))
	k[0] = table
	binary.BigEndian.PutUint64(k[1:], ino)
	return append(k, suffix...)
}

func keyInode(ino InodeID) []byte              { return key(tInode, uint64(ino)) }
func keyEntry(dir InodeID, name string) []byte { return key(tEntry, uint64(dir), []byte(name)...) }
func keyInline(ino InodeID) []byte             { return key(tData, uint64(ino)) }

func keyExtent(ino InodeID, off uint64) []byte {
	return key(tData, uint64(ino), binary.BigEndian.AppendUint64(nil, off)...)
}

func keyChunk(id chunkID) []byte { return append([]byte{tChunk}, id[:]...) }

// inoOf reads the inode number out of the key of a record of the inode, entry or data table.
func inoOf(k []byte) InodeID { return InodeID(binary.BigEndian.Uint64(k[1:])) }

// offsetOf reads the file offset out of an extent key.
func offsetOf(k []byte) uint64 { return binary.BigEndian.Uint64(k[prefixLen:]) }

// The inode record is a sequence of fields, each [tag][length][value], tags
// strictly ascending. A reader checks every field it knows and carries the ones it
// does not, so a later library adds an attribute (owner, link count, device,
// symlink target) as a new tag, and an older library that
// rewrites the inode does not lose it. The first five are always present.
const (
	fieldMode  = 1
	fieldSize  = 2
	fieldMtime = 3
	fieldAtime = 4
	fieldCtime = 5

	// The optional fields: a record carries one only when it differs from what its
	// absence says (SPECIFICATION 6.1).
	fieldUID    = 6 // owner, uvarint, 32 bits
	fieldGID    = 7 // group, uvarint, 32 bits; a record has both or neither
	fieldNlink  = 8 // names of a non-directory, uvarint, at least 2; critical (the links bit)
	fieldParent = 9 // the directory that names a directory, uvarint
)

func encodeInode(in Inode) []byte {
	var w writer
	field := func(tag uint64, value func(*writer)) {
		var v writer
		value(&v)
		w.uvarint(tag)
		w.bytes(v.b)
	}
	field(fieldMode, func(v *writer) { v.uvarint(uint64(in.Mode)) })
	field(fieldSize, func(v *writer) { v.uvarint(in.Size) })
	field(fieldMtime, func(v *writer) { v.varint(in.Mtime.UnixNano()) })
	field(fieldAtime, func(v *writer) { v.varint(in.Atime.UnixNano()) })
	field(fieldCtime, func(v *writer) { v.varint(in.Ctime.UnixNano()) })
	if in.Owned {
		field(fieldUID, func(v *writer) { v.uvarint(uint64(in.UID)) })
		field(fieldGID, func(v *writer) { v.uvarint(uint64(in.GID)) })
	}
	if in.Nlink > 1 && !in.IsDir() {
		field(fieldNlink, func(v *writer) { v.uvarint(uint64(in.Nlink)) })
	}
	if in.IsDir() && in.Parent != 0 {
		field(fieldParent, func(v *writer) { v.uvarint(uint64(in.Parent)) })
	}
	w.raw(in.later)
	return w.b
}

func decodeInode(id InodeID, b []byte) (Inode, error) {
	in := Inode{ID: id}
	r := reader{b: b}
	var seen, last uint64
	for len(r.b) > 0 && r.err == nil {
		start := r.b
		tag, value := r.uvarint(), r.bytes()
		if r.err != nil {
			break
		}
		if tag <= last {
			return in, fmt.Errorf("%w: inode field %d after %d", ErrCorrupt, tag, last)
		}
		last = tag
		v := reader{b: value}
		switch tag {
		case fieldMode:
			in.Mode = fs.FileMode(v.uvarint32())
		case fieldSize:
			in.Size = v.uvarint()
		case fieldMtime:
			in.Mtime = time.Unix(0, v.varint())
		case fieldAtime:
			in.Atime = time.Unix(0, v.varint())
		case fieldCtime:
			in.Ctime = time.Unix(0, v.varint())
		case fieldUID:
			in.UID = v.uvarint32()
		case fieldGID:
			in.GID = v.uvarint32()
		case fieldNlink:
			n := v.uvarint32()
			if n < 2 { // one link is the absence of the field: it has one spelling
				return in, fmt.Errorf("%w: inode link count %d is written as a field", ErrCorrupt, n)
			}
			in.Nlink = n
		case fieldParent:
			in.Parent = InodeID(v.uvarint())
		default:
			in.later = append(in.later, start[:len(start)-len(r.b)]...)
			continue
		}
		if err := v.done(); err != nil {
			return in, fmt.Errorf("inode field %d: %w", tag, err)
		}
		seen |= 1 << tag
	}
	if err := r.done(); err != nil {
		return in, err
	}
	const required = 1<<fieldMode | 1<<fieldSize | 1<<fieldMtime | 1<<fieldAtime | 1<<fieldCtime
	if seen&required != required {
		return in, fmt.Errorf("%w: inode record lacks a required field", ErrCorrupt)
	}
	owner := seen >> fieldUID & 3
	switch {
	case owner == 3:
		in.Owned = true
	case owner != 0:
		return in, fmt.Errorf("%w: inode record has an owner without a group, or the reverse", ErrCorrupt)
	}
	switch {
	case seen&(1<<fieldNlink) != 0 && in.IsDir():
		return in, fmt.Errorf("%w: a directory record holds a link count", ErrCorrupt)
	case seen&(1<<fieldParent) != 0 && (!in.IsDir() || in.Parent == 0 || in.Parent == id):
		return in, fmt.Errorf("%w: inode record holds a parent it cannot have", ErrCorrupt)
	}
	if !in.IsDir() && in.Nlink == 0 {
		in.Nlink = 1
	}
	return in, nil
}

func encodeEntry(child InodeID, mode fs.FileMode) []byte {
	var w writer
	w.uvarint(uint64(child))
	w.uvarint(uint64(mode & fs.ModeType))
	return w.b
}

func decodeEntry(b []byte) (InodeID, fs.FileMode, error) {
	r := reader{b: b}
	id, mode := InodeID(r.uvarint()), fs.FileMode(r.uvarint32())
	return id, mode, r.done()
}

// extent is a chunk of a large file: n bytes at file offset off.
type extent struct {
	off uint64
	n   uint32
	id  chunkID
}

func (e extent) end() uint64 { return e.off + uint64(e.n) }

func encodeExtent(e extent) []byte {
	var w writer
	w.raw(e.id[:])
	w.uvarint(uint64(e.n))
	return w.b
}

func decodeExtent(off uint64, b []byte) (extent, error) {
	r := reader{b: b}
	e := extent{off: off}
	copy(e.id[:], r.raw(32))
	e.n = r.uvarint32()
	return e, r.done()
}

// where is a chunk's place: a frame of length n at offset off in a pack.
type where struct {
	pack packID
	off  uint64
	n    uint64
}

func encodeWhere(p where) []byte {
	var w writer
	w.raw(p.pack[:])
	w.uvarint(p.off)
	w.uvarint(p.n)
	return w.b
}

func decodeWhere(b []byte) (where, error) {
	r := reader{b: b}
	var p where
	copy(p.pack[:], r.raw(uint64(len(p.pack))))
	p.off, p.n = r.uvarint(), r.uvarint()
	return p, r.done()
}
