package vfs

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"
)

// TestCheckFindsWhatIsWrong: Check is what the tests of the namespace lean on, so each way a
// namespace can be wrong is made, by writing the records a faulty writer would, and Check must
// say so. A namespace that is right passes first.
func TestCheckFindsWhatIsWrong(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(100, 0)
	build := func(t *testing.T) (*Volume, Inode, Inode, Inode) {
		t.Helper()
		_, _, conf := headVolume(t)
		conf.Round = 1
		v, err := Attach(ctx, conf)
		if err != nil {
			t.Fatal(err)
		}
		d, _ := v.Mkdir(ctx, RootInode, "d", 0o755)
		f, _ := v.Create(ctx, d.ID, "f", 0o644)
		g, _ := v.Create(ctx, RootInode, "g", 0o644)
		if _, err := v.Link(ctx, g.ID, d.ID, "g2"); err != nil {
			t.Fatal(err)
		}
		if err := v.Check(ctx); err != nil {
			t.Fatalf("a namespace that is right: %v", err)
		}
		return v, d, f, g
	}
	for name, break_ := range map[string]func(v *Volume, d, f, g Inode){
		"an entry that names no inode": func(v *Volume, d, f, g Inode) {
			v.over.put(keyEntry(RootInode, "ghost"), encodeEntry(999, 0))
		},
		"an entry of the wrong type": func(v *Volume, d, f, g Inode) {
			v.over.put(keyEntry(RootInode, "g"), encodeEntry(g.ID, fs.ModeDir))
		},
		"an entry in an inode that is not a directory": func(v *Volume, d, f, g Inode) {
			v.over.put(keyEntry(f.ID, "x"), encodeEntry(g.ID, 0))
		},
		"a link count that is too high": func(v *Volume, d, f, g Inode) {
			g.Nlink = 5
			v.put(g)
		},
		"a link count that is too low": func(v *Volume, d, f, g Inode) {
			g.Nlink = 0
			v.put(g)
		},
		"a second name and no link count": func(v *Volume, d, f, g Inode) {
			v.over.put(keyEntry(RootInode, "f2"), encodeEntry(f.ID, 0))
		},
		"a directory with two names": func(v *Volume, d, f, g Inode) {
			v.over.put(keyEntry(RootInode, "d2"), encodeEntry(d.ID, fs.ModeDir))
		},
		"a directory with the wrong parent": func(v *Volume, d, f, g Inode) {
			d.Parent = d.ID + 5
			v.put(d)
		},
		"an inode with no name": func(v *Volume, d, f, g Inode) {
			v.put(Inode{ID: 99, Mode: 0o644, Mtime: now, Atime: now, Ctime: now})
		},
		"data of an inode that is not there": func(v *Volume, d, f, g Inode) {
			v.over.put(keyInline(999), []byte("x"))
		},
		"a root that has a name": func(v *Volume, d, f, g Inode) {
			v.over.put(keyEntry(d.ID, "up"), encodeEntry(RootInode, fs.ModeDir))
		},
		"an inode count that is wrong": func(v *Volume, d, f, g Inode) { v.ns.files++ },
		"a byte count that is wrong":   func(v *Volume, d, f, g Inode) { v.ns.bytes++ },
		"a linked count that is wrong": func(v *Volume, d, f, g Inode) { v.ns.linked++ },
		"links not announced":          func(v *Volume, d, f, g Inode) { v.ns.links = false },
		"an unlinked inode that has a name's count": func(v *Volume, d, f, g Inode) {
			fh, _ := v.Open(ctx, f.ID)
			_ = v.Remove(ctx, d.ID, "f")
			v.files[f.ID].attr.Nlink = 1
			_ = fh
		},
	} {
		v, d, f, g := build(t)
		break_(v, d, f, g)
		if err := v.Check(ctx); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: Check says %v", name, err)
		}
	}
}
