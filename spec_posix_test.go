package vfs

// 5.1 and 6.1 of docs/SPECIFICATION.md for the POSIX operations: the inode's owner, link
// count and parent fields, and the Head's links and usage fields, rebuilt from the text.

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"
)

func specField(tag uint64, v []byte) []byte { return append(specUvarint(tag), specFrame(v)...) }

func specZig(n int64) []byte { return specUvarint(uint64(n<<1) ^ uint64(n>>63)) }

// specInode is 6.1 written out: fields 1 to 5 always, 6 and 7 together when the inode has an
// owner, 8 for a non-directory with two names or more, 9 for a directory that is not the root.
func specInode(mode fs.FileMode, size uint64, m, a, c time.Time, owner *[2]uint32, nlink uint32, parent uint64, later []byte) []byte {
	var b []byte
	b = append(b, specField(1, specUvarint(uint64(mode)))...)
	b = append(b, specField(2, specUvarint(size))...)
	b = append(b, specField(3, specZig(m.UnixNano()))...)
	b = append(b, specField(4, specZig(a.UnixNano()))...)
	b = append(b, specField(5, specZig(c.UnixNano()))...)
	if owner != nil {
		b = append(b, specField(6, specUvarint(uint64(owner[0])))...)
		b = append(b, specField(7, specUvarint(uint64(owner[1])))...)
	}
	if nlink >= 2 {
		b = append(b, specField(8, specUvarint(uint64(nlink)))...)
	}
	if parent != 0 {
		b = append(b, specField(9, specUvarint(parent))...)
	}
	return append(b, later...)
}

func TestSpecInodeAttributes(t *testing.T) {
	m, a, c := time.Unix(1, 5), time.Unix(2, 6), time.Unix(-3, 7)
	for name, tc := range map[string]struct {
		in   Inode
		want []byte
	}{
		"owned file with three names": {
			Inode{ID: 9, Mode: 0o644, Size: 3, Mtime: m, Atime: a, Ctime: c, Owned: true, UID: 1000, GID: 100, Nlink: 3},
			specInode(0o644, 3, m, a, c, &[2]uint32{1000, 100}, 3, 0, nil),
		},
		"owner zero is an owner": {
			Inode{ID: 9, Mode: 0o600, Mtime: m, Atime: a, Ctime: c, Owned: true},
			specInode(0o600, 0, m, a, c, &[2]uint32{0, 0}, 0, 0, nil),
		},
		"a file with one name has no link count": {
			Inode{ID: 9, Mode: 0o600, Mtime: m, Atime: a, Ctime: c, Nlink: 1},
			specInode(0o600, 0, m, a, c, nil, 1, 0, nil),
		},
		"directory with a parent": {
			Inode{ID: 9, Mode: fs.ModeDir | fs.ModeSetgid | 0o755, Mtime: m, Atime: a, Ctime: c, Parent: 4, Nlink: 5},
			specInode(fs.ModeDir|fs.ModeSetgid|0o755, 0, m, a, c, nil, 0, 4, nil),
		},
		"setuid, sticky and a symbolic link": {
			Inode{ID: 9, Mode: fs.ModeSymlink | fs.ModeSetuid | fs.ModeSticky | 0o777, Size: 4, Mtime: m, Atime: a, Ctime: c},
			specInode(fs.ModeSymlink|fs.ModeSetuid|fs.ModeSticky|0o777, 4, m, a, c, nil, 0, 0, nil),
		},
	} {
		if got := encodeInode(tc.in); !bytes.Equal(got, tc.want) {
			t.Errorf("%s:\n got %x\nwant %x", name, got, tc.want)
		}
		back, err := decodeInode(9, tc.want)
		if err != nil || back.Owned != tc.in.Owned || back.UID != tc.in.UID || back.GID != tc.in.GID || back.Parent != tc.in.Parent || back.Mode != tc.in.Mode {
			t.Errorf("%s: read back %+v %v", name, back, err)
		}
		if !tc.in.IsDir() && back.Nlink != max(tc.in.Nlink, 1) {
			t.Errorf("%s: link count %d", name, back.Nlink)
		}
	}
	// the mode bits the table of 6.1 fixes, in a record: the numbers are the table's
	if !bytes.Contains(specInode(0, 0, m, a, c, nil, 0, 0, nil), specField(1, specUvarint(0))) {
		t.Fatal("test")
	}
	for name, mode := range map[string]uint32{"setuid": 1 << 23, "setgid": 1 << 22, "sticky": 1 << 20, "symlink": 1 << 27} {
		got := encodeInode(Inode{ID: 9, Mode: fs.FileMode(mode), Mtime: m, Atime: a, Ctime: c})
		if !bytes.HasPrefix(got, specField(1, specUvarint(uint64(mode)))) {
			t.Errorf("mode bit %s is not where 6.1 puts it", name)
		}
	}
	// what a reader refuses, which is what keeps one record to one spelling
	base := func(extra ...[]byte) []byte {
		b := specInode(0o644, 0, m, a, c, nil, 0, 0, nil)
		for _, e := range extra {
			b = append(b, e...)
		}
		return b
	}
	dirBase := func(extra ...[]byte) []byte {
		b := specInode(fs.ModeDir|0o755, 0, m, a, c, nil, 0, 0, nil)
		for _, e := range extra {
			b = append(b, e...)
		}
		return b
	}
	for name, b := range map[string][]byte{
		"an owner without a group":           base(specField(6, specUvarint(1))),
		"a group without an owner":           base(specField(7, specUvarint(1))),
		"a link count of one":                base(specField(8, specUvarint(1))),
		"a link count of zero":               base(specField(8, specUvarint(0))),
		"a link count wider than 32":         base(specField(8, specUvarint(1<<32))),
		"a user wider than 32 bits":          base(specField(6, specUvarint(1<<32)), specField(7, specUvarint(0))),
		"a link count on a directory":        dirBase(specField(8, specUvarint(2))),
		"a parent on a file":                 base(specField(9, specUvarint(3))),
		"a parent of zero":                   dirBase(specField(9, specUvarint(0))),
		"a directory that is its own parent": dirBase(specField(9, specUvarint(9))),
		"fields out of order":                base(specField(9, specUvarint(3)), specField(8, specUvarint(2))),
		"a link count in two bytes":          base(specField(8, []byte{0x82, 0x00})),
	} {
		if _, err := decodeInode(9, b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v, want ErrCorrupt", name, err)
		}
	}
	// a field of a later build rides after them, and is carried
	later := specField(40, []byte("later"))
	got, err := decodeInode(9, specInode(0o644, 0, m, a, c, &[2]uint32{1, 2}, 2, 0, later))
	if err != nil || !bytes.Equal(encodeInode(got), specInode(0o644, 0, m, a, c, &[2]uint32{1, 2}, 2, 0, later)) {
		t.Errorf("a later field was not carried: %v", err)
	}
}

// 5.1: the links field is the uvarint count of inodes with more than one name, under bit 2,
// and the usage field (tag 64, no bit) is the uvarint count of inodes and the uvarint count of
// bytes stored. A commit writes both, and nothing but them is added to the Tail.
func TestSpecLinksAndUsageFields(t *testing.T) {
	ctx := context.Background()
	be, c, conf := headVolume(t)
	conf.Round = 1
	v, err := Attach(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := v.Mkdir(ctx, RootInode, "d", 0o755)
	f, _ := v.Create(ctx, d.ID, "f", 0o644)
	g, _ := v.Create(ctx, d.ID, "g", 0o644)
	h, _ := v.Open(ctx, f.ID)
	if _, err := h.WriteAt(ctx, bytes.Repeat([]byte("x"), 100), 0); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if _, err := v.Symlink(ctx, RootInode, "s", "abc"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"f2", "f3"} { // one inode with three names, one more with two
		if _, err := v.Link(ctx, f.ID, RootInode, name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := v.Link(ctx, g.ID, RootInode, "g2"); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	plain := headPlain(t, be, c)
	hd, err := unmarshalHead(plain)
	if err != nil {
		t.Fatal(err)
	}
	// the root, d, f, g and s: five inodes; 100 bytes of f and 3 of s
	want := append(specField(2, specUvarint(2)), specField(64, append(specUvarint(5), specUvarint(103)...))...)
	if hd.Needs != 1<<2 || hd.Tail != string(want) {
		t.Fatalf("needs %#x, tail %x\nwant bit 2 and %x", hd.Needs, hd.Tail, want)
	}
}
