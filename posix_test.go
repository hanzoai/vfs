package vfs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/backend"
)

// tick is a clock that moves one millisecond each time it is read, so every change has its
// own time and a test can order them. It implements journal.Clock.
type tick struct{ ns atomic.Int64 }

func (c *tick) now() time.Time                                    { return time.Unix(1_700_000_000, c.ns.Add(int64(time.Millisecond))) }
func (c *tick) Now() time.Time                                    { return c.now() }
func (c *tick) AfterFunc(d time.Duration, f func()) journal.Timer { return time.AfterFunc(d, f) }

// newClocked is newMem with a clock the test reads.
func newClocked(t testing.TB) (*storetest.Mem, vfs.Cipher, *vfs.Volume, *tick) {
	t.Helper()
	be := storetest.NewMem()
	c := vfs.Hybrid(newKey(t))
	clk := &tick{}
	conf := cfg(be, c, 0)
	conf.Clock = clk
	if err := vfs.Create(ctx, conf); err != nil {
		t.Fatal(err)
	}
	conf.Round = 1
	v, err := vfs.Attach(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	return be, c, v, clk
}

func check(t testing.TB, v *vfs.Volume) {
	t.Helper()
	if err := v.Check(ctx); err != nil {
		t.Fatalf("the namespace is not one: %v", err)
	}
}

func stat(t testing.TB, v *vfs.Volume, ino vfs.InodeID) vfs.Inode {
	t.Helper()
	in, err := v.Stat(ctx, ino)
	if err != nil {
		t.Fatalf("stat %d: %v", ino, err)
	}
	return in
}

func lookup(t testing.TB, v *vfs.Volume, dir vfs.InodeID, name string) vfs.Inode {
	t.Helper()
	in, err := v.Lookup(ctx, dir, name)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	return in
}

func absent(t testing.TB, v *vfs.Volume, dir vfs.InodeID, name string) {
	t.Helper()
	if _, err := v.Lookup(ctx, dir, name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lookup %s: %v, want fs.ErrNotExist", name, err)
	}
}

func rename(v *vfs.Volume, od vfs.InodeID, on string, nd vfs.InodeID, nn string) error {
	return v.Rename(ctx, od, on, nd, nn, 0)
}

func TestRenameMovesAFile(t *testing.T) {
	_, _, v, _ := newClocked(t)
	a, b := mkdir(t, v, vfs.RootInode, "a"), mkdir(t, v, vfs.RootInode, "b")
	f := write(t, v, a, "f", []byte("hello"))
	if err := rename(v, a, "f", a, "g"); err != nil {
		t.Fatal(err)
	}
	absent(t, v, a, "f")
	if got := lookup(t, v, a, "g"); got.ID != f.ID {
		t.Fatalf("g is inode %d, want %d", got.ID, f.ID)
	}
	if err := rename(v, a, "g", b, "h"); err != nil {
		t.Fatal(err)
	}
	absent(t, v, a, "g")
	got := lookup(t, v, b, "h")
	if got.ID != f.ID || got.Nlink != 1 || string(readAll(t, v, got.ID)) != "hello" {
		t.Fatalf("moved file: %+v", got)
	}
	check(t, v)
}

func TestRenameTimes(t *testing.T) {
	_, _, v, _ := newClocked(t)
	a, b := mkdir(t, v, vfs.RootInode, "a"), mkdir(t, v, vfs.RootInode, "b")
	f := write(t, v, a, "f", []byte("x"))
	flush(t, v)
	before := map[string]vfs.Inode{"a": stat(t, v, a), "b": stat(t, v, b), "f": stat(t, v, f.ID)}
	if err := rename(v, a, "f", b, "f"); err != nil {
		t.Fatal(err)
	}
	for name, ino := range map[string]vfs.InodeID{"a": a, "b": b, "f": f.ID} {
		after := stat(t, v, ino)
		if !after.Ctime.After(before[name].Ctime) {
			t.Errorf("%s: change time did not move: %v then %v", name, before[name].Ctime, after.Ctime)
		}
		wantM := name != "f"
		if moved := after.Mtime.After(before[name].Mtime); moved != wantM {
			t.Errorf("%s: modification time moved=%v, want %v", name, moved, wantM)
		}
	}
}

func TestRenameReplaces(t *testing.T) {
	_, _, v, _ := newClocked(t)
	d := mkdir(t, v, vfs.RootInode, "d")
	src := write(t, v, d, "src", []byte("new"))
	old := write(t, v, d, "dst", []byte("old"))
	if err := rename(v, d, "src", d, "dst"); err != nil {
		t.Fatal(err)
	}
	if got := lookup(t, v, d, "dst"); got.ID != src.ID || string(readAll(t, v, got.ID)) != "new" {
		t.Fatalf("dst after replace: %+v", got)
	}
	if _, err := v.Stat(ctx, old.ID); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the replaced inode: %v", err)
	}
	absent(t, v, d, "src")
	check(t, v)

	// an empty directory is replaced by a directory; a full one is not
	e1, e2, full := mkdir(t, v, d, "e1"), mkdir(t, v, d, "e2"), mkdir(t, v, d, "full")
	_ = e2
	write(t, v, full, "x", nil)
	if err := rename(v, d, "e1", d, "e2"); err != nil {
		t.Fatal(err)
	}
	if got := lookup(t, v, d, "e2"); got.ID != e1 {
		t.Fatalf("e2 is %d, want %d", got.ID, e1)
	}
	if err := rename(v, d, "e2", d, "full"); !errors.Is(err, vfs.ErrNotEmpty) {
		t.Fatalf("over a full directory: %v", err)
	}
	// the types must agree
	if err := rename(v, d, "dst", d, "full"); !errors.Is(err, vfs.ErrIsDir) {
		t.Fatalf("a file over a directory: %v", err)
	}
	if err := rename(v, d, "e2", d, "dst"); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("a directory over a file: %v", err)
	}
	check(t, v)
}

func TestRenameOfTwoNamesOfOneInodeIsNothing(t *testing.T) {
	_, _, v, _ := newClocked(t)
	f := write(t, v, vfs.RootInode, "a", []byte("x"))
	if _, err := v.Link(ctx, f.ID, vfs.RootInode, "b"); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]string{{"a", "b"}, {"a", "a"}} {
		if err := rename(v, vfs.RootInode, c[0], vfs.RootInode, c[1]); err != nil {
			t.Fatalf("rename %v: %v", c, err)
		}
	}
	lookup(t, v, vfs.RootInode, "a")
	lookup(t, v, vfs.RootInode, "b")
	if n := stat(t, v, f.ID).Nlink; n != 2 {
		t.Fatalf("link count %d", n)
	}
	check(t, v)
}

func TestRenameFlags(t *testing.T) {
	_, _, v, _ := newClocked(t)
	a := write(t, v, vfs.RootInode, "a", []byte("A"))
	b := write(t, v, vfs.RootInode, "b", []byte("B"))
	if err := v.Rename(ctx, vfs.RootInode, "a", vfs.RootInode, "b", vfs.RenameNoReplace); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("no-replace over an existing name: %v", err)
	}
	if err := v.Rename(ctx, vfs.RootInode, "a", vfs.RootInode, "c", vfs.RenameNoReplace); err != nil {
		t.Fatalf("no-replace to a new name: %v", err)
	}
	if err := v.Rename(ctx, vfs.RootInode, "c", vfs.RootInode, "b", vfs.RenameExchange); err != nil {
		t.Fatal(err)
	}
	if got := lookup(t, v, vfs.RootInode, "c"); got.ID != b.ID {
		t.Fatalf("c is %d after the exchange, want %d", got.ID, b.ID)
	}
	if got := lookup(t, v, vfs.RootInode, "b"); got.ID != a.ID {
		t.Fatalf("b is %d after the exchange, want %d", got.ID, a.ID)
	}
	if err := v.Rename(ctx, vfs.RootInode, "c", vfs.RootInode, "nope", vfs.RenameExchange); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("exchange with a name that is not there: %v", err)
	}
	if err := v.Rename(ctx, vfs.RootInode, "c", vfs.RootInode, "b", vfs.RenameExchange|vfs.RenameNoReplace); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("both flags: %v", err)
	}
	if err := v.Rename(ctx, vfs.RootInode, "c", vfs.RootInode, "b", 1<<7); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("an unknown flag: %v", err)
	}
	// exchanging a directory and a file moves each parent pointer that needs it
	d1, d2 := mkdir(t, v, vfs.RootInode, "d1"), mkdir(t, v, vfs.RootInode, "d2")
	sub := mkdir(t, v, d1, "sub")
	if err := v.Rename(ctx, d1, "sub", d2, "other", 0); err != nil {
		t.Fatal(err)
	}
	if p, err := v.Parent(ctx, sub); err != nil || p != d2 {
		t.Fatalf("parent of the moved directory: %d %v, want %d", p, err, d2)
	}
	if err := v.Rename(ctx, d2, "other", vfs.RootInode, "d1", vfs.RenameExchange); err != nil {
		t.Fatal(err)
	}
	if p, _ := v.Parent(ctx, sub); p != vfs.RootInode {
		t.Fatalf("parent after the exchange: %d", p)
	}
	if p, _ := v.Parent(ctx, d1); p != d2 {
		t.Fatalf("parent of the directory that went down: %d, want %d", p, d2)
	}
	check(t, v)
}

func TestRenameDirectoryNeverBecomesItsOwnAncestor(t *testing.T) {
	_, _, v, _ := newClocked(t)
	a := mkdir(t, v, vfs.RootInode, "a")
	b := mkdir(t, v, a, "b")
	c := mkdir(t, v, b, "c")
	for _, tc := range []struct {
		od         vfs.InodeID
		on         string
		nd         vfs.InodeID
		nn         string
		flags      vfs.RenameFlags
		whatToMove string
	}{
		{vfs.RootInode, "a", a, "x", 0, "into itself"},
		{vfs.RootInode, "a", b, "x", 0, "into its child"},
		{vfs.RootInode, "a", c, "x", 0, "into its grandchild"},
		{a, "b", c, "x", 0, "a child into its own child"},
		{vfs.RootInode, "a", b, "c", vfs.RenameExchange, "exchange with a descendant"},
	} {
		if err := v.Rename(ctx, tc.od, tc.on, tc.nd, tc.nn, tc.flags); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("%s: %v, want fs.ErrInvalid", tc.whatToMove, err)
		}
	}
	// a file that is below a directory cannot trade places with it: the directory would end up below itself
	write(t, v, c, "file", nil)
	if err := v.Rename(ctx, c, "file", vfs.RootInode, "a", vfs.RenameExchange); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("a file below a directory, exchanged with it: %v, want fs.ErrInvalid", err)
	}
	if err := v.Rename(ctx, c, "file", b, "c", vfs.RenameExchange); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("a file below a directory, exchanged with its parent's parent: %v, want fs.ErrInvalid", err)
	}
	// and a sibling can move into any directory it is not above
	if err := rename(v, c, "none", vfs.RootInode, "z"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("%v", err)
	}
	if err := rename(v, b, "c", vfs.RootInode, "c"); err != nil {
		t.Fatal(err)
	}
	if p, _ := v.Parent(ctx, c); p != vfs.RootInode {
		t.Fatalf("parent of c: %d", p)
	}
	check(t, v)
}

func TestDirectoryLinkCounts(t *testing.T) {
	_, _, v, _ := newClocked(t)
	if n := stat(t, v, vfs.RootInode).Nlink; n != 2 {
		t.Fatalf("an empty root: %d", n)
	}
	a := mkdir(t, v, vfs.RootInode, "a")
	b := mkdir(t, v, vfs.RootInode, "b")
	mkdir(t, v, a, "x")
	mkdir(t, v, a, "y")
	write(t, v, a, "file", nil)
	want := map[vfs.InodeID]uint32{vfs.RootInode: 4, a: 4, b: 2}
	for ino, n := range want {
		if got := stat(t, v, ino).Nlink; got != n {
			t.Fatalf("inode %d: %d, want %d", ino, got, n)
		}
	}
	if err := rename(v, a, "x", b, "x"); err != nil {
		t.Fatal(err)
	}
	if err := v.Remove(ctx, a, "y"); err != nil {
		t.Fatal(err)
	}
	for ino, n := range map[vfs.InodeID]uint32{a: 2, b: 3} {
		if got := stat(t, v, ino).Nlink; got != n {
			t.Fatalf("after moves, inode %d: %d, want %d", ino, got, n)
		}
	}
	if got := lookup(t, v, vfs.RootInode, "b").Nlink; got != 3 {
		t.Fatalf("lookup says %d", got)
	}
	check(t, v)
}

func TestHardLinks(t *testing.T) {
	be, c, v, _ := newClocked(t)
	d := mkdir(t, v, vfs.RootInode, "d")
	f := write(t, v, vfs.RootInode, "f", []byte("shared"))
	if n := stat(t, v, f.ID).Nlink; n != 1 {
		t.Fatalf("a new file has %d links", n)
	}
	l, err := v.Link(ctx, f.ID, d, "l")
	if err != nil || l.ID != f.ID || l.Nlink != 2 {
		t.Fatalf("link: %+v %v", l, err)
	}
	if _, err := v.Link(ctx, f.ID, d, "l"); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("link over a name: %v", err)
	}
	if _, err := v.Link(ctx, d, vfs.RootInode, "dd"); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("link of a directory: %v", err)
	}
	if _, err := v.Link(ctx, f.ID, f.ID, "x"); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("link into a file: %v", err)
	}
	// a write through one name is seen through the other
	h, _ := v.Open(ctx, lookup(t, v, d, "l").ID)
	if _, err := h.WriteAt(ctx, []byte("SH"), 0); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if got := string(readAll(t, v, lookup(t, v, vfs.RootInode, "f").ID)); got != "SHared" {
		t.Fatalf("through the other name: %q", got)
	}
	check(t, v)
	flush(t, v)
	cold := reopen(t, be, c)
	if n := stat(t, cold, f.ID).Nlink; n != 2 {
		t.Fatalf("after a reopen: %d links", n)
	}
	check(t, cold)

	if err := v.Remove(ctx, vfs.RootInode, "f"); err != nil {
		t.Fatal(err)
	}
	if n := stat(t, v, f.ID).Nlink; n != 1 {
		t.Fatalf("after removing one name: %d links", n)
	}
	if got := string(readAll(t, v, f.ID)); got != "SHared" {
		t.Fatalf("the other name lost the bytes: %q", got)
	}
	check(t, v)
	if err := v.Remove(ctx, d, "l"); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Stat(ctx, f.ID); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the last name is gone and the inode is not: %v", err)
	}
	check(t, v)
	flush(t, v)
	check(t, reopen(t, be, c))
}

func TestRenameOverAHardLinkKeepsTheOtherName(t *testing.T) {
	_, _, v, _ := newClocked(t)
	f := write(t, v, vfs.RootInode, "f", []byte("F"))
	if _, err := v.Link(ctx, f.ID, vfs.RootInode, "g"); err != nil {
		t.Fatal(err)
	}
	h := write(t, v, vfs.RootInode, "h", []byte("H"))
	if err := rename(v, vfs.RootInode, "h", vfs.RootInode, "f"); err != nil {
		t.Fatal(err)
	}
	if n := stat(t, v, f.ID).Nlink; n != 1 {
		t.Fatalf("links of the inode that lost a name: %d", n)
	}
	if got := lookup(t, v, vfs.RootInode, "f"); got.ID != h.ID {
		t.Fatalf("f is %d", got.ID)
	}
	check(t, v)
}

func TestSymlinks(t *testing.T) {
	be, c, v, _ := newClocked(t)
	l, err := v.Symlink(ctx, vfs.RootInode, "l", "../target/x", vfs.Owner(5, 6))
	if err != nil {
		t.Fatal(err)
	}
	if l.Mode&fs.ModeType != fs.ModeSymlink || l.Size != uint64(len("../target/x")) || l.Nlink != 1 || l.UID != 5 || l.GID != 6 {
		t.Fatalf("symlink inode: %+v", l)
	}
	if got, err := v.Readlink(ctx, l.ID); err != nil || got != "../target/x" {
		t.Fatalf("readlink: %q %v", got, err)
	}
	if got := lookup(t, v, vfs.RootInode, "l"); got.Mode&fs.ModeType != fs.ModeSymlink {
		t.Fatalf("lookup: %v", got.Mode)
	}
	ents, _ := v.ReadDir(ctx, vfs.RootInode)
	if len(ents) != 1 || ents[0].Mode&fs.ModeSymlink == 0 {
		t.Fatalf("readdir: %+v", ents)
	}
	if _, err := v.Open(ctx, l.ID); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("open of a symbolic link: %v", err)
	}
	f := write(t, v, vfs.RootInode, "f", []byte("x"))
	if _, err := v.Readlink(ctx, f.ID); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("readlink of a file: %v", err)
	}
	for name, target := range map[string]string{"empty": "", "nul": "a\x00b"} {
		if _, err := v.Symlink(ctx, vfs.RootInode, name, target); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("%s target: %v", name, err)
		}
	}
	long := make([]byte, 4096)
	for i := range long {
		long[i] = 'a'
	}
	if _, err := v.Symlink(ctx, vfs.RootInode, "long", string(long)); !errors.Is(err, vfs.ErrNameTooLong) {
		t.Fatalf("a 4096 byte target: %v", err)
	}
	if _, err := v.Symlink(ctx, vfs.RootInode, "max", string(long[:4095])); err != nil {
		t.Fatalf("a 4095 byte target: %v", err)
	}
	check(t, v)
	flush(t, v)
	cold := reopen(t, be, c)
	if got, err := cold.Readlink(ctx, l.ID); err != nil || got != "../target/x" {
		t.Fatalf("after a reopen: %q %v", got, err)
	}
	if err := v.Remove(ctx, vfs.RootInode, "l"); err != nil {
		t.Fatal(err)
	}
	check(t, v)
	// a link to a symbolic link names the link itself
	if _, err := v.Link(ctx, lookup(t, v, vfs.RootInode, "max").ID, vfs.RootInode, "max2"); err != nil {
		t.Fatal(err)
	}
	check(t, v)
}

func TestMknod(t *testing.T) {
	_, _, v, _ := newClocked(t)
	p, err := v.Mknod(ctx, vfs.RootInode, "p", fs.ModeNamedPipe|0o644)
	if err != nil || p.Mode&fs.ModeType != fs.ModeNamedPipe || p.Mode.Perm() != 0o644 {
		t.Fatalf("pipe: %+v %v", p, err)
	}
	s, err := v.Mknod(ctx, vfs.RootInode, "s", fs.ModeSocket|0o600)
	if err != nil || s.Mode&fs.ModeType != fs.ModeSocket {
		t.Fatalf("socket: %+v %v", s, err)
	}
	if _, err := v.Mknod(ctx, vfs.RootInode, "d", fs.ModeDevice|fs.ModeCharDevice|0o600); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("device: %v", err)
	}
	if _, err := v.Open(ctx, p.ID); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("open of a pipe: %v", err)
	}
	if err := v.Truncate(ctx, p.ID, 0); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("truncate of a pipe: %v", err)
	}
	// a pipe can be linked, renamed and removed like a file
	if _, err := v.Link(ctx, p.ID, vfs.RootInode, "p2"); err != nil {
		t.Fatal(err)
	}
	if err := rename(v, vfs.RootInode, "p2", vfs.RootInode, "p3"); err != nil {
		t.Fatal(err)
	}
	if err := v.Remove(ctx, vfs.RootInode, "p"); err != nil {
		t.Fatal(err)
	}
	check(t, v)
}

func TestSetAttr(t *testing.T) {
	be, c, v, _ := newClocked(t)
	f := write(t, v, vfs.RootInode, "f", []byte("0123456789"))
	flush(t, v)
	mode := fs.FileMode(0o4755) | fs.ModeSticky
	if got := mode & fs.ModeSetuid; got != 0 {
		t.Fatal("test mode")
	}
	mode = 0o755 | fs.ModeSetuid | fs.ModeSticky
	at, mt := time.Unix(1000, 123456789), time.Unix(2000, 987654321)
	uid, gid := uint32(1001), uint32(1002)
	before := stat(t, v, f.ID)
	got, err := v.SetAttr(ctx, f.ID, vfs.Change{Mode: &mode, UID: &uid, GID: &gid, Atime: &at, Mtime: &mt})
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != mode || got.UID != 1001 || got.GID != 1002 || !got.Owned || !got.Atime.Equal(at) || !got.Mtime.Equal(mt) {
		t.Fatalf("after setattr: %+v", got)
	}
	if !got.Ctime.After(before.Ctime) || got.Size != 10 {
		t.Fatalf("change time %v after %v, size %d", got.Ctime, before.Ctime, got.Size)
	}
	// the nanoseconds survive a commit and a reopen
	flush(t, v)
	cold := stat(t, reopen(t, be, c), f.ID)
	if cold.Mode != mode || cold.UID != 1001 || cold.GID != 1002 || !cold.Atime.Equal(at) || !cold.Mtime.Equal(mt) || !cold.Ctime.Equal(got.Ctime) {
		t.Fatalf("after a reopen: %+v want %+v", cold, got)
	}
	// the type does not change, and a mode is its own bits
	dm := fs.FileMode(0o700) | fs.ModeDir
	if got, err = v.SetAttr(ctx, f.ID, vfs.Change{Mode: &dm}); err != nil || got.IsDir() || got.Mode.Perm() != 0o700 {
		t.Fatalf("a mode with the directory bit: %+v %v", got, err)
	}
	// a time that 64 bits of nanoseconds cannot hold is refused, not wrapped into another
	far := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := v.SetAttr(ctx, f.ID, vfs.Change{Mtime: &far}); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("a time in the year 3000: %v", err)
	}
	if got := stat(t, v, f.ID); !got.Mtime.Equal(mt) {
		t.Fatalf("a refused time changed the modification time to %v", got.Mtime)
	}
	// half an owner needs the other half to exist
	one := uint32(7)
	if got, err = v.SetAttr(ctx, f.ID, vfs.Change{UID: &one}); err != nil || got.UID != 7 || got.GID != 1002 {
		t.Fatalf("one half of an owner that has both: %+v %v", got, err)
	}
	g := write(t, v, vfs.RootInode, "g", nil)
	if _, err := v.SetAttr(ctx, g.ID, vfs.Change{UID: &one}); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("one half of an owner that has none: %v", err)
	}
	// size
	ten := uint64(4)
	if got, err = v.SetAttr(ctx, f.ID, vfs.Change{Size: &ten}); err != nil || got.Size != 4 || string(readAll(t, v, f.ID)) != "0123" {
		t.Fatalf("truncate by setattr: %+v %v", got, err)
	}
	d := mkdir(t, v, vfs.RootInode, "d")
	if _, err := v.SetAttr(ctx, d, vfs.Change{Size: &ten}); !errors.Is(err, vfs.ErrIsDir) {
		t.Fatalf("size of a directory: %v", err)
	}
	check(t, v)
}

func TestOwnerAndGroupInheritance(t *testing.T) {
	_, _, v, _ := newClocked(t)
	d, err := v.Mkdir(ctx, vfs.RootInode, "d", fs.ModeSetgid|0o775, vfs.Owner(10, 20))
	if err != nil || d.Mode&fs.ModeSetgid == 0 || d.UID != 10 || d.GID != 20 {
		t.Fatalf("directory: %+v %v", d, err)
	}
	f, err := v.Create(ctx, d.ID, "f", 0o644, vfs.Owner(11, 99))
	if err != nil || f.UID != 11 || f.GID != 20 {
		t.Fatalf("a file in a setgid directory takes the group: %+v %v", f, err)
	}
	sub, err := v.Mkdir(ctx, d.ID, "sub", 0o755, vfs.Owner(11, 99))
	if err != nil || sub.GID != 20 || sub.Mode&fs.ModeSetgid == 0 {
		t.Fatalf("a directory in a setgid directory takes the group and the bit: %+v %v", sub, err)
	}
	plain, err := v.Create(ctx, vfs.RootInode, "p", 0o644, vfs.Owner(1, 2))
	if err != nil || plain.GID != 2 || plain.Mode&fs.ModeSetgid != 0 {
		t.Fatalf("a file elsewhere: %+v %v", plain, err)
	}
	none, _ := v.Create(ctx, vfs.RootInode, "n", 0o644)
	if none.Owned {
		t.Fatalf("an inode made with no owner has one: %+v", none)
	}
	check(t, v)
}

func TestDirectoryTimesFollowTheirEntries(t *testing.T) {
	_, _, v, _ := newClocked(t)
	d := mkdir(t, v, vfs.RootInode, "d")
	last := stat(t, v, d)
	step := func(what string, do func()) {
		t.Helper()
		do()
		now := stat(t, v, d)
		if !now.Mtime.After(last.Mtime) || !now.Ctime.After(last.Ctime) {
			t.Fatalf("%s did not move the directory's times: %v %v after %v %v", what, now.Mtime, now.Ctime, last.Mtime, last.Ctime)
		}
		last = now
	}
	var f vfs.Inode
	step("create", func() { f = write(t, v, d, "f", []byte("x")) })
	step("mkdir", func() { mkdir(t, v, d, "sub") })
	step("symlink", func() {
		if _, err := v.Symlink(ctx, d, "s", "f"); err != nil {
			t.Fatal(err)
		}
	})
	step("link", func() {
		if _, err := v.Link(ctx, f.ID, d, "l"); err != nil {
			t.Fatal(err)
		}
	})
	step("rename within", func() {
		if err := rename(v, d, "l", d, "l2"); err != nil {
			t.Fatal(err)
		}
	})
	step("unlink", func() {
		if err := v.Remove(ctx, d, "l2"); err != nil {
			t.Fatal(err)
		}
	})
	step("rmdir", func() {
		if err := v.Remove(ctx, d, "sub"); err != nil {
			t.Fatal(err)
		}
	})
	// writing to a file inside does not touch the directory
	h, _ := v.Open(ctx, f.ID)
	if _, err := h.WriteAt(ctx, []byte("y"), 0); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if now := stat(t, v, d); !now.Mtime.Equal(last.Mtime) {
		t.Fatalf("a write moved the directory's modification time")
	}
}

func TestUnlinkedFileIsWholeForItsHandle(t *testing.T) {
	be, c, v, _ := newClocked(t)
	big := content(5, 3<<20)
	f := write(t, v, vfs.RootInode, "f", big)
	flush(t, v)
	h, err := v.Open(ctx, f.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Remove(ctx, vfs.RootInode, "f"); err != nil {
		t.Fatal(err)
	}
	// the settled bytes are in packs the Volume still reaches; the handle reads them
	got := make([]byte, len(big))
	if n, err := h.ReadAt(ctx, got, 0); n != len(big) || err != nil && err != io.EOF {
		t.Fatalf("read: %d %v", n, err)
	}
	mustEqual(t, "unlinked file", got, big)
	// and writes: a large file's chunks are settled into memory, not the tree
	if _, err := h.WriteAt(ctx, []byte("tail"), int64(len(big))); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(ctx, uint64(len(big))+2); err != nil {
		t.Fatal(err)
	}
	if st, err := h.Stat(ctx); err != nil || st.Size != uint64(len(big))+2 || st.Nlink != 0 {
		t.Fatalf("stat: %+v %v", st, err)
	}
	flush(t, v)
	check(t, v)
	if u := v.Usage(); u.Files != 1 || u.Bytes != 0 {
		t.Fatalf("usage with the only file unlinked: %+v", u)
	}
	buf := make([]byte, 6)
	if _, err := h.ReadAt(ctx, buf, int64(len(big))-4); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(buf[4:]) != "ta" {
		t.Fatalf("the write after the commit: %q", buf)
	}
	// bytes written and not settled go with the last close, and are not charged to the Volume
	if _, err := h.WriteAt(ctx, []byte("late"), 0); err != nil {
		t.Fatal(err)
	}
	if st := v.Stats(); st.Dirty == 0 {
		t.Fatal("a write to an unlinked file is not counted while it is held")
	}
	h.Close()
	if st := v.Stats(); st.Dirty != 0 {
		t.Fatalf("the closed file left %d dirty bytes", st.Dirty)
	}
	flush(t, v)
	check(t, v)
	check(t, reopen(t, be, c))
}

func TestUnlinkedFileWithAnotherNameStaysNamed(t *testing.T) {
	_, _, v, _ := newClocked(t)
	f := write(t, v, vfs.RootInode, "f", []byte("x"))
	if _, err := v.Link(ctx, f.ID, vfs.RootInode, "g"); err != nil {
		t.Fatal(err)
	}
	h, _ := v.Open(ctx, f.ID)
	defer h.Close()
	if err := v.Remove(ctx, vfs.RootInode, "f"); err != nil {
		t.Fatal(err)
	}
	if st, _ := h.Stat(ctx); st.Nlink != 1 {
		t.Fatalf("links %d", st.Nlink)
	}
	if err := v.Remove(ctx, vfs.RootInode, "g"); err != nil {
		t.Fatal(err)
	}
	if st, _ := h.Stat(ctx); st.Nlink != 0 {
		t.Fatalf("links %d after the last name", st.Nlink)
	}
	if _, err := v.Link(ctx, f.ID, vfs.RootInode, "back"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("link of an unlinked inode: %v", err)
	}
	check(t, v)
}

func TestRenameOverAnOpenFile(t *testing.T) {
	_, _, v, _ := newClocked(t)
	old := write(t, v, vfs.RootInode, "dst", []byte("old"))
	write(t, v, vfs.RootInode, "src", []byte("new"))
	h, _ := v.Open(ctx, old.ID)
	defer h.Close()
	if err := rename(v, vfs.RootInode, "src", vfs.RootInode, "dst"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if _, err := h.ReadAt(ctx, buf, 0); err != nil && err != io.EOF || string(buf) != "old" {
		t.Fatalf("the replaced file through its handle: %q %v", buf, err)
	}
	check(t, v)
}

func TestUsage(t *testing.T) {
	be, c, v, _ := newClocked(t)
	if u := v.Usage(); u.Files != 1 || u.Bytes != 0 {
		t.Fatalf("a new Volume: %+v", u)
	}
	d := mkdir(t, v, vfs.RootInode, "d")
	a := write(t, v, d, "a", content(1, 100))                // inline
	b := write(t, v, d, "b", content(2, 300<<10))            // chunks
	write(t, v, d, "z", nil)                                 // empty
	if _, err := v.Symlink(ctx, d, "s", "abc"); err != nil { // 3 bytes
		t.Fatal(err)
	}
	if u := v.Usage(); u.Files != 6 || u.Pending == 0 {
		t.Fatalf("before settling: %+v", u)
	}
	flush(t, v)
	if u := v.Usage(); u.Files != 6 || u.Bytes != 100+300<<10+3 || u.Pending != 0 {
		t.Fatalf("after a Flush: %+v", u)
	}
	check(t, v)
	// an overwrite changes nothing; a truncate to zero frees the chunks; a hole costs nothing
	h, _ := v.Open(ctx, b.ID)
	if err := h.Truncate(ctx, 1<<30); err != nil {
		t.Fatal(err)
	}
	if err := h.Punch(ctx, 0, 200<<10); err != nil {
		t.Fatal(err)
	}
	flush(t, v)
	if u := v.Usage(); u.Bytes >= 100+300<<10+3 {
		t.Fatalf("after punching 200 KiB: %+v", u)
	}
	check(t, v)
	if err := h.Truncate(ctx, 0); err != nil {
		t.Fatal(err)
	}
	h.Close()
	flush(t, v)
	if u := v.Usage(); u.Bytes != 103 {
		t.Fatalf("after a truncate to zero: %+v", u)
	}
	if err := v.Remove(ctx, d, "a"); err != nil {
		t.Fatal(err)
	}
	_ = a
	flush(t, v)
	if u := v.Usage(); u.Files != 5 || u.Bytes != 3 {
		t.Fatalf("after a remove: %+v", u)
	}
	check(t, v)
	cold := reopen(t, be, c)
	if u := cold.Usage(); u.Files != 5 || u.Bytes != 3 {
		t.Fatalf("after a reopen: %+v", u)
	}
	check(t, cold)
}

func TestCapacity(t *testing.T) {
	be := storetest.NewMem()
	c := vfs.Hybrid(newKey(t))
	conf := cfg(be, c, 0)
	conf.Capacity = 1 << 20
	if err := vfs.Create(ctx, conf); err != nil {
		t.Fatal(err)
	}
	conf.Round = 1
	v, err := vfs.Attach(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := v.Create(ctx, vfs.RootInode, "f", 0o644)
	h, _ := v.Open(ctx, f.ID)
	defer h.Close()
	if _, err := h.WriteAt(ctx, make([]byte, 600<<10), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt(ctx, make([]byte, 600<<10), 600<<10); !errors.Is(err, vfs.ErrNoSpace) {
		t.Fatalf("a write past the capacity: %v", err)
	}
	// overwriting what is there needs no room
	if _, err := h.WriteAt(ctx, make([]byte, 600<<10), 0); err != nil {
		t.Fatalf("an overwrite at capacity: %v", err)
	}
	if u := v.Usage(); u.Capacity != 1<<20 {
		t.Fatalf("usage: %+v", u)
	}
	flush(t, v)
	if _, err := h.WriteAt(ctx, make([]byte, 600<<10), 600<<10); !errors.Is(err, vfs.ErrNoSpace) {
		t.Fatalf("after a Flush: %v", err)
	}
	// freeing makes room
	if err := h.Truncate(ctx, 0); err != nil {
		t.Fatal(err)
	}
	flush(t, v)
	if _, err := h.WriteAt(ctx, make([]byte, 600<<10), 0); err != nil {
		t.Fatalf("after freeing: %v", err)
	}
}

func TestNameLimits(t *testing.T) {
	_, _, v, _ := newClocked(t)
	name := make([]byte, 256)
	for i := range name {
		name[i] = 'n'
	}
	if _, err := v.Create(ctx, vfs.RootInode, string(name[:255]), 0o644); err != nil {
		t.Fatalf("255 bytes: %v", err)
	}
	if _, err := v.Create(ctx, vfs.RootInode, string(name), 0o644); !errors.Is(err, vfs.ErrNameTooLong) || !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("256 bytes: %v", err)
	}
	if _, err := v.Lookup(ctx, vfs.RootInode, string(name)); !errors.Is(err, vfs.ErrNameTooLong) {
		t.Fatalf("lookup of 256 bytes: %v", err)
	}
	if err := rename(v, vfs.RootInode, string(name[:255]), vfs.RootInode, string(name)); !errors.Is(err, vfs.ErrNameTooLong) {
		t.Fatalf("rename to 256 bytes: %v", err)
	}
}

func TestReadOnlyVolumeRefusesEveryNewOperation(t *testing.T) {
	be, c, v, _ := newClocked(t)
	f := write(t, v, vfs.RootInode, "f", []byte("x"))
	flush(t, v)
	ro := reopen(t, be, c)
	mode := fs.FileMode(0o600)
	for name, err := range map[string]error{
		"rename":  rename(ro, vfs.RootInode, "f", vfs.RootInode, "g"),
		"setattr": func() error { _, err := ro.SetAttr(ctx, f.ID, vfs.Change{Mode: &mode}); return err }(),
		"link":    func() error { _, err := ro.Link(ctx, f.ID, vfs.RootInode, "l"); return err }(),
		"symlink": func() error { _, err := ro.Symlink(ctx, vfs.RootInode, "s", "x"); return err }(),
		"mknod":   func() error { _, err := ro.Mknod(ctx, vfs.RootInode, "p", fs.ModeNamedPipe|0o600); return err }(),
	} {
		if !errors.Is(err, vfs.ErrReadOnly) {
			t.Errorf("%s on a read-only Volume: %v", name, err)
		}
	}
}

// failingReads fails the nth read of the store once armed.
type failingReads struct {
	backend.Backend
	armed atomic.Bool
	left  atomic.Int64
}

var errRead = errors.New("injected read failure")

func (f *failingReads) fail() error {
	if f.armed.Load() && f.left.Add(-1) == 0 {
		return errRead
	}
	return nil
}

func (f *failingReads) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if err := f.fail(); err != nil {
		return nil, "", err
	}
	return f.Backend.Get(ctx, key, max)
}

func (f *failingReads) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := f.fail(); err != nil {
		return nil, err
	}
	return f.Backend.GetRange(ctx, key, off, n)
}

// TestAnOperationThatFailsChangesNothing: each operation reads what it needs before it stages
// anything, so a read that fails at any point leaves the Volume as it was, to the byte, and a
// commit afterwards holds no half of it. The store's nth read fails, for each n, in a Volume
// whose caches are cold, and what the operation made is either all there or not at all.
func TestAnOperationThatFailsChangesNothing(t *testing.T) {
	ops := map[string]func(v *vfs.Volume) error{
		"rename a directory over an empty one": func(v *vfs.Volume) error {
			return v.Rename(ctx, vfs.RootInode, "a", vfs.RootInode, "empty", 0)
		},
		"rename a file over a linked file": func(v *vfs.Volume) error {
			return v.Rename(ctx, vfs.RootInode, "other", vfs.RootInode, "f", 0)
		},
		"rename a directory across": func(v *vfs.Volume) error {
			return v.Rename(ctx, vfs.RootInode, "a", vfs.RootInode, "d", vfs.RenameExchange)
		},
		"rename down a level": func(v *vfs.Volume) error {
			d, err := v.Lookup(ctx, vfs.RootInode, "d")
			if err != nil {
				return err
			}
			return v.Rename(ctx, vfs.RootInode, "a", d.ID, "a", 0)
		},
		"link": func(v *vfs.Volume) error {
			f, err := v.Lookup(ctx, vfs.RootInode, "other")
			if err != nil {
				return err
			}
			_, err = v.Link(ctx, f.ID, vfs.RootInode, "other2")
			return err
		},
		"remove the last name of a file": func(v *vfs.Volume) error { return v.Remove(ctx, vfs.RootInode, "other") },
		"remove a name of a linked file": func(v *vfs.Volume) error { return v.Remove(ctx, vfs.RootInode, "f") },
		"remove a directory":             func(v *vfs.Volume) error { return v.Remove(ctx, vfs.RootInode, "empty") },
		"make a directory": func(v *vfs.Volume) error {
			_, err := v.Mkdir(ctx, vfs.RootInode, "new", 0o755)
			return err
		},
		"make a symbolic link": func(v *vfs.Volume) error {
			_, err := v.Symlink(ctx, vfs.RootInode, "new", "f")
			return err
		},
		"set attributes": func(v *vfs.Volume) error {
			f, err := v.Lookup(ctx, vfs.RootInode, "f")
			if err != nil {
				return err
			}
			mode, size := fs.FileMode(0o600), uint64(1)
			_, err = v.SetAttr(ctx, f.ID, vfs.Change{Mode: &mode, Size: &size})
			return err
		},
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			be := storetest.NewMem()
			c := vfs.Hybrid(newKey(t))
			if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
				t.Fatal(err)
			}
			setup, err := vfs.Attach(ctx, cfg(be, c, 1))
			if err != nil {
				t.Fatal(err)
			}
			mkdir(t, setup, vfs.RootInode, "empty")
			mkdir(t, setup, vfs.RootInode, "d")
			a := mkdir(t, setup, vfs.RootInode, "a")
			for i := range 30 {
				write(t, setup, a, fmt.Sprint("x", i), content(uint64(i), 50))
			}
			bulk := mkdir(t, setup, vfs.RootInode, "bulk") // enough records that the tree is many shards, and the ones below lie in different ones
			for i := range 4000 {
				write(t, setup, bulk, fmt.Sprintf("b%05d", i), content(uint64(i), 40))
			}
			f := write(t, setup, vfs.RootInode, "f", []byte("file"))
			if _, err := setup.Link(ctx, f.ID, a, "alias"); err != nil {
				t.Fatal(err)
			}
			write(t, setup, vfs.RootInode, "other", []byte("other"))
			flush(t, setup)
			before := snapshot(t, reopen(t, be, c))

			// what the operation does when no read fails
			ref, err := vfs.Attach(ctx, cfg(be, c, 2))
			if err != nil {
				t.Fatal(err)
			}
			if err := op(ref); err != nil {
				t.Fatal(err)
			}
			after := snapshot(t, ref)

			round := uint64(3)
			for n := int64(1); ; n++ {
				fr := &failingReads{Backend: be}
				v, err := vfs.Attach(ctx, cfg(fr, c, round))
				round++
				if err != nil {
					t.Fatal(err)
				}
				fr.left.Store(n)
				fr.armed.Store(true)
				opErr := op(v)
				fr.armed.Store(false)
				switch {
				case errors.Is(opErr, errRead):
					sameSnapshot(t, fmt.Sprintf("read %d failed, then", n), snapshot(t, v), before)
					if err := v.Check(ctx); err != nil {
						t.Fatalf("read %d failed: %v", n, err)
					}
					flush(t, v)
					sameSnapshot(t, fmt.Sprintf("read %d failed, and a commit", n), snapshot(t, reopen(t, be, c)), before)
				case opErr == nil:
					sameSnapshot(t, fmt.Sprintf("the operation read %d times and", n), snapshot(t, v), after)
					if n < 3 {
						t.Fatalf("the operation made %d reads: the sweep proves nothing", n)
					}
					return
				default:
					t.Fatalf("read %d: %v", n, opErr)
				}
			}
		})
	}
}

// TestPunchedRangeCostsNothing: a range that is punched out reads as zero, is not stored
// as zero chunks, and frees what was stored for it.
func TestPunchedRangeCostsNothing(t *testing.T) {
	be, c, v, _ := newClocked(t)
	data := content(11, 8<<20)
	f := write(t, v, vfs.RootInode, "f", data)
	flush(t, v)
	packed := packBytes(be)
	bytes0 := v.Usage().Bytes
	h, _ := v.Open(ctx, f.ID)
	defer h.Close()
	if err := h.Punch(ctx, 1<<20, 6<<20); err != nil {
		t.Fatal(err)
	}
	clear(data[1<<20 : 7<<20])
	buf := make([]byte, len(data))
	if _, err := h.ReadAt(ctx, buf, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	mustEqual(t, "before settling", buf, data)
	flush(t, v)
	if st := stat(t, v, f.ID); st.Size != uint64(len(data)) {
		t.Fatalf("a punch changed the size to %d", st.Size)
	}
	if grown := packBytes(be) - packed; grown > 2<<20 {
		t.Fatalf("punching 6 MiB wrote %d bytes of packs: the range was stored", grown)
	}
	if u := v.Usage().Bytes; u > bytes0-(5<<20) {
		t.Fatalf("usage %d after punching 6 of %d MiB", u, bytes0>>20)
	}
	cold := reopen(t, be, c)
	mustEqual(t, "after a reopen", readAll(t, cold, f.ID), data)
	check(t, cold)
	// writing into the hole stores that write alone
	if _, err := h.WriteAt(ctx, []byte("hole"), 3<<20); err != nil {
		t.Fatal(err)
	}
	copy(data[3<<20:], "hole")
	flush(t, v)
	mustEqual(t, "after a write into the hole", readAll(t, reopen(t, be, c), f.ID), data)
	// a range past the end is clipped, and a range that is wholly past it does nothing at all
	before := stat(t, v, f.ID)
	if err := h.Punch(ctx, uint64(len(data))+5, 5); err != nil {
		t.Fatal(err)
	}
	if after := stat(t, v, f.ID); !after.Mtime.Equal(before.Mtime) || !after.Ctime.Equal(before.Ctime) {
		t.Fatalf("a punch past the end moved the times: %v %v, were %v %v", after.Mtime, after.Ctime, before.Mtime, before.Ctime)
	}
	if err := h.Punch(ctx, uint64(len(data))-10, 1<<40); err != nil {
		t.Fatal(err)
	}
	clear(data[len(data)-10:])
	if err := h.Punch(ctx, uint64(len(data))+5, 5); err != nil {
		t.Fatal(err)
	}
	if err := h.Punch(ctx, 2<<20, ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	clear(data[2<<20:])
	flush(t, v)
	mustEqual(t, "after punching to the end", readAll(t, reopen(t, be, c), f.ID), data)
	check(t, v)
}

// TestWriteMovesMtimeAndCtime: a write, a truncation and a punch set both times of the file,
// and a time that was set by hand is not put back by a write that settles later.
func TestWriteMovesMtimeAndCtime(t *testing.T) {
	_, _, v, _ := newClocked(t)
	f := write(t, v, vfs.RootInode, "f", []byte("0123456789"))
	h, _ := v.Open(ctx, f.ID)
	defer h.Close()
	last := stat(t, v, f.ID)
	step := func(what string, do func() error) {
		t.Helper()
		if err := do(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		now := stat(t, v, f.ID)
		if !now.Mtime.After(last.Mtime) || !now.Ctime.After(last.Ctime) {
			t.Fatalf("%s did not move mtime and ctime: %v %v after %v %v", what, now.Mtime, now.Ctime, last.Mtime, last.Ctime)
		}
		last = now
	}
	step("a write", func() error { _, err := h.WriteAt(ctx, []byte("x"), 3); return err })
	step("a truncation", func() error { return h.Truncate(ctx, 20) })
	step("a punch", func() error { return h.Punch(ctx, 1, 2) })
	flush(t, v)
	if got := stat(t, v, f.ID); !got.Mtime.Equal(last.Mtime) || !got.Ctime.Equal(last.Ctime) {
		t.Fatalf("settling changed the times: %v %v, were %v %v", got.Mtime, got.Ctime, last.Mtime, last.Ctime)
	}
	// a time set by hand, over a write that has not settled, is the time that is kept
	if _, err := h.WriteAt(ctx, []byte("y"), 0); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(5, 6)
	if _, err := v.SetAttr(ctx, f.ID, vfs.Change{Mtime: &mt}); err != nil {
		t.Fatal(err)
	}
	flush(t, v)
	if got := stat(t, v, f.ID); !got.Mtime.Equal(mt) || !got.Ctime.After(mt) {
		t.Fatalf("the modification time that was set is %v after the write settled (the change time is %v)", got.Mtime, got.Ctime)
	}
	// a change of mode after a write that has not settled is not lost when it does
	if _, err := h.WriteAt(ctx, []byte("z"), 1); err != nil {
		t.Fatal(err)
	}
	mode := fs.FileMode(0o600)
	if _, err := v.SetAttr(ctx, f.ID, vfs.Change{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	flush(t, v)
	if got := stat(t, v, f.ID); got.Mode.Perm() != 0o600 || got.Size != 20 {
		t.Fatalf("after a write and a chmod that settled together: %+v", got)
	}
}

// TestLinkLimit: an inode has at most 65,000 names.
func TestLinkLimit(t *testing.T) {
	_, _, v, _ := newClocked(t)
	f := write(t, v, vfs.RootInode, "f", nil)
	d := mkdir(t, v, vfs.RootInode, "d")
	for i := 1; i < 65000; i++ {
		if _, err := v.Link(ctx, f.ID, d, fmt.Sprint("l", i)); err != nil {
			t.Fatalf("link %d: %v", i, err)
		}
	}
	if n := stat(t, v, f.ID).Nlink; n != 65000 {
		t.Fatalf("%d names", n)
	}
	if _, err := v.Link(ctx, f.ID, d, "one-too-many"); !errors.Is(err, vfs.ErrTooManyLinks) {
		t.Fatalf("the 65,001st name: %v", err)
	}
	if err := v.Remove(ctx, d, "l1"); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Link(ctx, f.ID, d, "one-more"); err != nil {
		t.Fatalf("a name after one was removed: %v", err)
	}
	flush(t, v)
	check(t, v)
}

// TestNamespaceUnderConcurrency: goroutines create, link, rename, remove and read in
// directories they share, and the namespace is whole at the end: the race detector sees no
// race, no call hangs, and Check finds names, link counts, parents and counters in agreement.
func TestNamespaceUnderConcurrency(t *testing.T) {
	_, _, v, _ := newClocked(t)
	const workers, rounds = 8, 120
	dirs := make([]vfs.InodeID, 4)
	for i := range dirs {
		dirs[i] = mkdir(t, v, vfs.RootInode, fmt.Sprint("d", i))
	}
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for i := range rounds {
				d1, d2 := dirs[r.IntN(len(dirs))], dirs[r.IntN(len(dirs))]
				a, b := fmt.Sprint("n", r.IntN(12)), fmt.Sprint("n", r.IntN(12))
				switch r.IntN(7) {
				case 0:
					v.Create(ctx, d1, a, 0o644)
				case 1:
					if in, err := v.Lookup(ctx, d1, a); err == nil && !in.IsDir() {
						v.Link(ctx, in.ID, d2, b)
					}
				case 2:
					v.Rename(ctx, d1, a, d2, b, []vfs.RenameFlags{0, 0, vfs.RenameNoReplace, vfs.RenameExchange}[r.IntN(4)])
				case 3:
					v.Remove(ctx, d1, a)
				case 4:
					v.Symlink(ctx, d1, a, "target")
				case 5:
					if in, err := v.Lookup(ctx, d1, a); err == nil && in.Mode.IsRegular() {
						if h, err := v.Open(ctx, in.ID); err == nil {
							h.WriteAt(ctx, []byte(fmt.Sprint(w, i)), int64(r.IntN(100)))
							h.Close()
						}
					}
				default:
					v.ReadDir(ctx, d1)
					v.Stat(ctx, d1)
				}
				if i%40 == 39 {
					v.Flush(ctx)
				}
			}
		}()
	}
	wg.Wait()
	flush(t, v)
	check(t, v)
}

// TestObjectsPerNamespaceOperation: what each operation costs in objects written, in a
// tree of 2,000 files in 20 directories: committed one at a time (the cost of a Flush after
// each, which is what a mount that commits at every close pays) and committed 100 at a time
// (what a journal's group commit will pay). A namespace operation changes a few records, so
// it writes the few shards that hold them and the Head, and a batch shares them.
func TestObjectsPerNamespaceOperation(t *testing.T) {
	be, _, v, _ := newClocked(t)
	mem := be
	var dirs []vfs.InodeID
	for d := range 20 {
		dir := mkdir(t, v, vfs.RootInode, fmt.Sprint("d", d))
		dirs = append(dirs, dir)
		for i := range 100 {
			write(t, v, dir, fmt.Sprintf("f%03d", i), content(uint64(d*100+i), 200))
		}
	}
	// each operation has files of its own to act on, so that none finds another's gone
	for _, kind := range []string{"link", "ren", "mov", "chm", "utm", "unl"} {
		for n := range 120 {
			write(t, v, dirs[n%20], fmt.Sprint(kind, n), content(uint64(n), 100))
		}
	}
	flush(t, v)
	file := func(kind string, n int) vfs.Inode { return lookup(t, v, dirs[n%20], fmt.Sprint(kind, n)) }
	type op struct {
		name string
		do   func(n int)
	}
	ops := []op{
		{"create", func(n int) { v.Create(ctx, dirs[n%20], fmt.Sprint("new", n), 0o644) }},
		{"mkdir", func(n int) { v.Mkdir(ctx, dirs[n%20], fmt.Sprint("sub", n), 0o755) }},
		{"symlink", func(n int) { v.Symlink(ctx, dirs[n%20], fmt.Sprint("sym", n), "target") }},
		{"link", func(n int) { v.Link(ctx, file("link", n).ID, dirs[(n+1)%20], fmt.Sprint("hard", n)) }},
		{"rename in a directory", func(n int) { v.Rename(ctx, dirs[n%20], fmt.Sprint("ren", n), dirs[n%20], fmt.Sprint("renamed", n), 0) }},
		{"rename across", func(n int) {
			v.Rename(ctx, dirs[n%20], fmt.Sprint("mov", n), dirs[(n+7)%20], fmt.Sprint("moved", n), 0)
		}},
		{"chmod", func(n int) {
			m := fs.FileMode(0o600 + n%8)
			v.SetAttr(ctx, file("chm", n).ID, vfs.Change{Mode: &m})
		}},
		{"utimens", func(n int) {
			at := time.Unix(int64(n), 0)
			v.SetAttr(ctx, file("utm", n).ID, vfs.Change{Atime: &at, Mtime: &at})
		}},
		{"unlink", func(n int) { v.Remove(ctx, dirs[n%20], fmt.Sprint("unl", n)) }},
	}
	for _, o := range ops {
		mem.Reset()
		for n := range 20 {
			o.do(n)
			flush(t, v)
		}
		single := float64(mem.Total()) / 20
		mem.Reset()
		for n := 20; n < 120; n++ {
			o.do(n)
		}
		flush(t, v)
		batch := float64(mem.Total()) / 100
		t.Logf("%-22s %5.2f objects per operation committed one by one, %5.2f in a batch of 100", o.name, single, batch)
		if single > 16 || batch > 1 {
			t.Errorf("%s: %.2f objects one by one and %.2f in a batch: an operation writes more than the few shards that hold its records", o.name, single, batch)
		}
	}
	check(t, v)
}
