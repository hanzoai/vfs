package vfs_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

var ctx = context.Background()

func newKey(t testing.TB) *age.HybridIdentity {
	t.Helper()
	k, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// content is n pseudo-random bytes, the same for the same seed.
func content(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed*7919+1))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// cfg is the configuration of a Volume of a test: a journal of 16 MiB (segments of 1 MiB, which a
// test pays to make) in the directory the tests share, on files that are not synced to the disk.
func cfg(be backend.Backend, c vfs.Cipher, round uint64) vfs.Config {
	return vfs.Config{Backend: be, Cipher: c, Round: round, Journal: storetest.Journals(), JournalCapacity: 64 << 20, Disk: storetest.Disk}
}

// newMem creates a Volume on an in-memory store and returns the store, the
// cipher, and the Volume attached at round 1.
func newMem(t testing.TB) (*storetest.Mem, vfs.Cipher, *vfs.Volume) {
	t.Helper()
	be := storetest.NewMem()
	c := vfs.Hybrid(newKey(t))
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(be, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	return be, c, v
}

// write creates dir/name holding data and returns its inode.
func write(t testing.TB, v *vfs.Volume, dir vfs.InodeID, name string, data []byte) vfs.Inode {
	t.Helper()
	in, err := v.Create(ctx, dir, name, 0o644)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return in
}

// readAll returns the whole file.
func readAll(t testing.TB, v *vfs.Volume, ino vfs.InodeID) []byte {
	t.Helper()
	in, err := v.Stat(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := bytes.Repeat([]byte{0xAA}, int(in.Size)) // a byte the reader must overwrite, zeros included
	if n, err := f.ReadAt(ctx, buf, 0); err != nil && err != io.EOF || uint64(n) != in.Size {
		t.Fatalf("read inode %d: %d of %d bytes: %v", ino, n, in.Size, err)
	}
	return buf
}

func mustEqual(t testing.TB, what string, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		i := 0
		for i < len(got) && i < len(want) && got[i] == want[i] {
			i++
		}
		t.Fatalf("%s: %d bytes, want %d; first difference at %d", what, len(got), len(want), i)
	}
}

func flush(t testing.TB, v *vfs.Volume) {
	t.Helper()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// reopen opens a read-only view of the store as committed.
func reopen(t testing.TB, be backend.Backend, c vfs.Cipher) *vfs.Volume {
	t.Helper()
	v, err := vfs.Open(ctx, cfg(be, c, 0))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func newMemStore() *storetest.Mem { return storetest.NewMem() }

func vfsAge(t testing.TB) vfs.Cipher { return vfs.Hybrid(newKey(t)) }

// packBytes is the total size of every pack stored.
func packBytes(be *storetest.Mem) int {
	n := 0
	for k, v := range be.Dump() {
		if strings.HasPrefix(k, "pack/") {
			n += len(v)
		}
	}
	return n
}

// snapshot maps every path of the Volume to what is there: for a directory its mode and link
// count; for a file its inode number, mode, owner, link count and the blake3 of its bytes;
// for a symbolic link its target instead of the bytes.
func snapshot(t testing.TB, v *vfs.Volume) map[string]string {
	t.Helper()
	out := map[string]string{}
	var walk func(dir vfs.InodeID, prefix string)
	walk = func(dir vfs.InodeID, prefix string) {
		ents, err := v.ReadDir(ctx, dir)
		if err != nil {
			t.Fatalf("readdir %s: %v", prefix, err)
		}
		for _, e := range ents {
			p := prefix + "/" + e.Name
			in, err := v.Stat(ctx, e.ID)
			if err != nil {
				t.Fatalf("stat %s: %v", p, err)
			}
			attrs := fmt.Sprintf("i%d %v n%d %d:%d", in.ID, in.Mode, in.Nlink, in.UID, in.GID)
			switch {
			case e.Mode.IsDir():
				out[p] = "dir " + attrs
				walk(e.ID, p)
			case e.Mode&fs.ModeSymlink != 0:
				target, err := v.Readlink(ctx, e.ID)
				if err != nil {
					t.Fatalf("readlink %s: %v", p, err)
				}
				out[p] = "-> " + target + " " + attrs
			case e.Mode&fs.ModeType != 0:
				out[p] = "special " + attrs
			default:
				sum := blake3.Sum256(readAll(t, v, e.ID))
				out[p] = hex.EncodeToString(sum[:8]) + " " + attrs
			}
		}
	}
	walk(vfs.RootInode, "")
	return out
}

func sameSnapshot(t testing.TB, what string, got, want map[string]string) {
	t.Helper()
	for p, w := range want {
		if g, ok := got[p]; !ok {
			t.Errorf("%s: %s is missing", what, p)
		} else if g != w {
			t.Errorf("%s: %s differs", what, p)
		}
	}
	for p := range got {
		if _, ok := want[p]; !ok {
			t.Errorf("%s: unexpected %s", what, p)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

func mkdir(t testing.TB, v *vfs.Volume, dir vfs.InodeID, name string) vfs.InodeID {
	t.Helper()
	in, err := v.Mkdir(ctx, dir, name, 0o755)
	if err != nil {
		t.Fatalf("mkdir %s: %v", name, err)
	}
	return in.ID
}

// stateOne builds a Volume with small files, a medium file and a 5 MiB file.
func stateOne(t testing.TB, v *vfs.Volume) {
	// Enough small files that the metadata is dozens of shards, so a change touches
	// a few of them and a crash has many places to land.
	for d := range 6 {
		dir := mkdir(t, v, vfs.RootInode, fmt.Sprint("d", d))
		for i := range 150 {
			write(t, v, dir, fmt.Sprintf("f%03d", i), content(uint64(d*1000+i), 300+i*11))
		}
	}
	src := mkdir(t, v, vfs.RootInode, "src")
	for i := range 40 {
		write(t, v, src, fmt.Sprintf("f%02d", i), content(uint64(i), 200+i*37))
	}
	data := mkdir(t, v, vfs.RootInode, "data")
	write(t, v, data, "big", content(1000, 5<<20))
	write(t, v, data, "keep", content(1001, 30_000))

	// what the POSIX operations add: hard links, symbolic links, a pipe, owners and modes,
	// directories inside directories
	links := mkdir(t, v, vfs.RootInode, "links")
	orig := write(t, v, links, "orig", content(7000, 20_000))
	for dir, name := range map[vfs.InodeID]string{links: "hard", src: "also"} {
		if _, err := v.Link(ctx, orig.ID, dir, name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := v.Symlink(ctx, links, "sym", "orig"); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Symlink(ctx, links, "dangling", "../nowhere"); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Mknod(ctx, links, "pipe", fs.ModeNamedPipe|0o600); err != nil {
		t.Fatal(err)
	}
	mode, uid, gid := fs.FileMode(0o750)|fs.ModeSetuid, uint32(1000), uint32(100)
	if _, err := v.SetAttr(ctx, orig.ID, vfs.Change{Mode: &mode, UID: &uid, GID: &gid}); err != nil {
		t.Fatal(err)
	}
	deeper := mkdir(t, v, mkdir(t, v, links, "deep"), "deeper")
	write(t, v, deeper, "leaf", content(7001, 100))
}

// stateTwo changes it: edits the big file, grows keep, adds a directory, removes a file.
func stateTwo(t testing.TB, v *vfs.Volume) {
	data, err := v.Resolve(ctx, "/data")
	if err != nil {
		t.Fatal(err)
	}
	big, _ := v.Lookup(ctx, data.ID, "big")
	f, _ := v.Open(ctx, big.ID)
	if _, err := f.WriteAt(ctx, content(2000, 1000), 2<<20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	keep, _ := v.Lookup(ctx, data.ID, "keep")
	f, _ = v.Open(ctx, keep.ID)
	if _, err := f.WriteAt(ctx, content(2001, 40_000), 30_000); err != nil {
		t.Fatal(err)
	}
	f.Close()
	nw := mkdir(t, v, vfs.RootInode, "new")
	for i := range 10 {
		write(t, v, nw, fmt.Sprintf("n%d", i), content(uint64(3000+i), 5000+i))
	}
	src, _ := v.Resolve(ctx, "/src")
	if err := v.Remove(ctx, src.ID, "f03"); err != nil {
		t.Fatal(err)
	}
	// Moves, replacements and links that change names and counts: a directory of 150 entries
	// goes under another and one with children comes up; a file replaces a link; two files
	// trade names; a name is added and one is taken away.
	links, _ := v.Resolve(ctx, "/links")
	data, _ = v.Resolve(ctx, "/data")
	rename := func(od vfs.InodeID, on string, nd vfs.InodeID, nn string, fl vfs.RenameFlags) {
		if err := v.Rename(ctx, od, on, nd, nn, fl); err != nil {
			t.Fatalf("rename %s: %v", on, err)
		}
	}
	d4, _ := v.Resolve(ctx, "/d4")
	rename(vfs.RootInode, "d4", links.ID, "d4", 0)
	rename(links.ID, "deep", vfs.RootInode, "deep", 0)
	rename(links.ID, "sym", links.ID, "dangling", 0)
	keep, _ = v.Lookup(ctx, data.ID, "keep")
	if _, err := v.Link(ctx, keep.ID, links.ID, "keep"); err != nil {
		t.Fatal(err)
	}
	if err := v.Remove(ctx, src.ID, "also"); err != nil {
		t.Fatal(err)
	}
	d5, _ := v.Resolve(ctx, "/d5")
	rename(vfs.RootInode, "d5", links.ID, "d5", 0)
	rename(d5.ID, "f001", d5.ID, "f002", vfs.RenameExchange)
	rename(links.ID, "hard", d4.ID, "hard", 0)
	mode := fs.FileMode(0o600)
	if _, err := v.SetAttr(ctx, keep.ID, vfs.Change{Mode: &mode}); err != nil {
		t.Fatal(err)
	}

	// A change in the middle of the many-file directories.
	d3, _ := v.Resolve(ctx, "/d3")
	for _, i := range []int{7, 70, 140} {
		in, _ := v.Lookup(ctx, d3.ID, fmt.Sprintf("f%03d", i))
		f, _ := v.Open(ctx, in.ID)
		if _, err := f.WriteAt(ctx, []byte("changed!"), 4); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
}

func storeBytes(be *storetest.Mem) int {
	n := 0
	for _, v := range be.Dump() {
		n += len(v)
	}
	return n
}

func reopenBackendTB(tb testing.TB, dir string) backend.Backend {
	be, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		tb.Fatal(err)
	}
	return be
}

// sized returns the full size of a test's data, or the smaller one under -short,
// which is what the race detector run uses.
func sized(short, full int) int {
	if testing.Short() {
		return short
	}
	return full
}
