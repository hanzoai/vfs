package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

func TestNamespace(t *testing.T) {
	_, _, v := newMem(t)
	a, err := v.Mkdir(ctx, vfs.RootInode, "a", 0o755)
	if err != nil || !a.IsDir() {
		t.Fatalf("mkdir: %+v %v", a, err)
	}
	b := write(t, v, a.ID, "b.txt", []byte("hi"))
	if _, err := v.Create(ctx, a.ID, "b.txt", 0o644); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("create over existing: %v, want ErrExist", err)
	}
	if _, err := v.Mkdir(ctx, b.ID, "x", 0o755); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("mkdir under a file: %v, want ErrNotDir", err)
	}
	if _, err := v.Lookup(ctx, a.ID, "nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lookup of absent: %v, want ErrNotExist", err)
	}
	for _, bad := range []string{"", ".", "..", "a/b", "x\x00y", strings.Repeat("n", 256)} {
		if _, err := v.Create(ctx, a.ID, bad, 0o644); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("create %q: %v, want ErrInvalid", bad, err)
		}
	}
	got, err := v.Resolve(ctx, "/a/b.txt")
	if err != nil || got.ID != b.ID {
		t.Fatalf("resolve: %+v %v", got, err)
	}
	if got, _ := v.Resolve(ctx, ""); got.ID != vfs.RootInode {
		t.Fatalf("resolve of empty path = inode %d, want the root", got.ID)
	}
	for _, n := range []string{"z", "m", "c"} {
		write(t, v, a.ID, n, nil)
	}
	ents, err := v.ReadDir(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name)
	}
	if strings.Join(names, ",") != "b.txt,c,m,z" {
		t.Fatalf("readdir = %v, want name order", names)
	}
	if _, err := v.ReadDir(ctx, b.ID); !errors.Is(err, vfs.ErrNotDir) {
		t.Fatalf("readdir of a file: %v", err)
	}
	if err := v.Remove(ctx, vfs.RootInode, "a"); !errors.Is(err, vfs.ErrNotEmpty) {
		t.Fatalf("remove of non-empty directory: %v, want ErrNotEmpty", err)
	}
	for _, n := range names {
		if err := v.Remove(ctx, a.ID, n); err != nil {
			t.Fatalf("remove %s: %v", n, err)
		}
	}
	if err := v.Remove(ctx, vfs.RootInode, "a"); err != nil {
		t.Fatalf("remove of empty directory: %v", err)
	}
	if _, err := v.Resolve(ctx, "/a"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removed directory still resolves: %v", err)
	}
	if _, err := v.Stat(ctx, b.ID); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removed file's inode still stats: %v", err)
	}
}

// TestFilesOfEverySize round-trips sizes either side of every boundary: empty,
// inline, the inline limit, one chunk, the chunk minimum and maximum, several
// chunks; before Flush, from the same Volume after it, and from a fresh read-only
// Open.
func TestFilesOfEverySize(t *testing.T) {
	be, c, v := newMem(t)
	sizes := []int{0, 1, 4095, 4096, 16 << 10, 16<<10 + 1, 64<<10 - 1, 64 << 10, 1 << 20, 4<<20 - 1, 4 << 20, 4<<20 + 1}
	if !testing.Short() {
		sizes = append(sizes, 9<<20+12345)
	}
	inodes := map[int]vfs.InodeID{}
	for i, n := range sizes {
		inodes[n] = write(t, v, vfs.RootInode, fmt.Sprintf("f%d", n), content(uint64(i), n)).ID
	}
	check := func(when string, v *vfs.Volume) {
		for i, n := range sizes {
			mustEqual(t, fmt.Sprintf("%s: size %d", when, n), readAll(t, v, inodes[n]), content(uint64(i), n))
		}
	}
	check("before flush", v)
	flush(t, v)
	check("after flush", v)
	check("fresh open", reopen(t, be, c))
}

func TestPartialReadsAndWrites(t *testing.T) {
	be, c, v := newMem(t)
	data := content(10, 6<<20)
	ino := write(t, v, vfs.RootInode, "big", data).ID
	flush(t, v)

	f, err := v.Open(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Overwrite across a chunk boundary, inside one, and extend past the end.
	patches := []struct {
		off int
		n   int
	}{{70_000, 5000}, {1 << 20, 3 << 20}, {6<<20 - 10, 100}, {9 << 20, 10}}
	for i, p := range patches {
		b := content(uint64(100+i), p.n)
		if _, err := f.WriteAt(ctx, b, int64(p.off)); err != nil {
			t.Fatal(err)
		}
		if end := p.off + p.n; end > len(data) {
			data = append(data, make([]byte, end-len(data))...)
		}
		copy(data[p.off:], b)
		// Visible at once, to a read that crosses every layer.
		buf := make([]byte, p.n+200)
		off := max(0, p.off-100)
		n, err := f.ReadAt(ctx, buf, int64(off))
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		mustEqual(t, fmt.Sprintf("patch %d read-back", i), buf[:n], data[off:min(len(data), off+len(buf))])
	}
	mustEqual(t, "before flush", readAll(t, v, ino), data)
	flush(t, v)
	mustEqual(t, "after flush", readAll(t, v, ino), data)
	mustEqual(t, "fresh open", readAll(t, reopen(t, be, c), ino), data)
}

func TestTruncate(t *testing.T) {
	be, c, v := newMem(t)
	for _, size := range []int{10 << 10, 3 << 20} {
		data := content(20, size)
		ino := write(t, v, vfs.RootInode, fmt.Sprint("t", size), data).ID
		flush(t, v)

		// Shrink, then grow: the bytes between must read as zeros, never the old data.
		keep := size / 3
		if err := v.Truncate(ctx, ino, uint64(keep)); err != nil {
			t.Fatal(err)
		}
		if err := v.Truncate(ctx, ino, uint64(size)); err != nil {
			t.Fatal(err)
		}
		want := append(append([]byte(nil), data[:keep]...), make([]byte, size-keep)...)
		mustEqual(t, "shrink+grow before flush", readAll(t, v, ino), want)
		flush(t, v)
		mustEqual(t, "shrink+grow after flush", readAll(t, v, ino), want)
		mustEqual(t, "shrink+grow reopened", readAll(t, reopen(t, be, c), ino), want)

		// Shrink to zero and to a size of the other form.
		if err := v.Truncate(ctx, ino, 100); err != nil {
			t.Fatal(err)
		}
		flush(t, v)
		mustEqual(t, "truncate to 100", readAll(t, reopen(t, be, c), ino), want[:100])
		if err := v.Truncate(ctx, ino, 0); err != nil {
			t.Fatal(err)
		}
		flush(t, v)
		if got := readAll(t, reopen(t, be, c), ino); len(got) != 0 {
			t.Fatalf("truncate to 0 left %d bytes", len(got))
		}
	}
}

// TestHandlesShareWrites: every handle to a file sees every write at once.
func TestHandlesShareWrites(t *testing.T) {
	_, _, v := newMem(t)
	in := write(t, v, vfs.RootInode, "shared", []byte("0123456789"))
	a, _ := v.Open(ctx, in.ID)
	b, _ := v.Open(ctx, in.ID)
	defer a.Close()
	defer b.Close()
	if _, err := a.WriteAt(ctx, []byte("AB"), 3); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 10)
	if _, err := b.ReadAt(ctx, buf, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(buf) != "012AB56789" {
		t.Fatalf("second handle read %q, want 012AB56789", buf)
	}
	st, _ := b.Stat(ctx)
	if st.Size != 10 {
		t.Fatalf("size %d", st.Size)
	}
}

// TestRemovedFileStaysOpenForItsHandle: a file unlinked while open is gone for everyone
// else and whole for its handle, which reads and writes it until it closes; nothing of it
// reaches the store.
func TestRemovedFileStaysOpenForItsHandle(t *testing.T) {
	be, c, v := newMem(t)
	in := write(t, v, vfs.RootInode, "gone", []byte("data"))
	f, _ := v.Open(ctx, in.ID)
	if err := v.Remove(ctx, vfs.RootInode, "gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Lookup(ctx, vfs.RootInode, "gone"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lookup of the removed name: %v", err)
	}
	buf := make([]byte, 4)
	if n, err := f.ReadAt(ctx, buf, 0); err != nil && err != io.EOF || string(buf[:n]) != "data" {
		t.Fatalf("read of an unlinked file: %q %v", buf[:n], err)
	}
	if _, err := f.WriteAt(ctx, []byte("more"), 4); err != nil {
		t.Fatalf("write to an unlinked file: %v", err)
	}
	if st, err := f.Stat(ctx); err != nil || st.Size != 8 || st.Nlink != 0 {
		t.Fatalf("stat of an unlinked file: %+v %v", st, err)
	}
	if _, err := v.Open(ctx, in.ID); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("opening an unlinked inode by number: %v", err)
	}
	if err := v.Check(ctx); err != nil {
		t.Fatal(err)
	}
	flush(t, v) // the file reaches the store as nothing
	if err := v.Check(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if st := v.Stats(); st.Dirty != 0 {
		t.Fatalf("the closed orphan left %d dirty bytes", st.Dirty)
	}
	cold := reopen(t, be, c)
	if _, err := cold.Stat(ctx, in.ID); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the unlinked file is in the store: %v", err)
	}
	if err := cold.Check(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOpenIsReadOnly(t *testing.T) {
	be, c, v := newMem(t)
	in := write(t, v, vfs.RootInode, "f", []byte("x"))
	flush(t, v)
	ro := reopen(t, be, c)
	if _, err := ro.Create(ctx, vfs.RootInode, "g", 0o644); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("Create on a read-only volume: %v", err)
	}
	if err := ro.Remove(ctx, vfs.RootInode, "f"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("Remove on a read-only volume: %v", err)
	}
	f, _ := ro.Open(ctx, in.ID)
	if _, err := f.WriteAt(ctx, []byte("y"), 0); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteAt on a read-only volume: %v", err)
	}
	if err := ro.Truncate(ctx, in.ID, 0); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("Truncate on a read-only volume: %v", err)
	}
	if err := ro.Flush(ctx); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("Flush on a read-only volume: %v", err)
	}
}

func TestCreateRefusesExistingVolume(t *testing.T) {
	be, c, _ := newMem(t)
	if err := vfs.Create(ctx, cfg(be, c, 0)); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("second Create: %v, want ErrExist", err)
	}
	if _, err := vfs.Open(ctx, cfg(newMemStore(), c, 0)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open of nothing: %v, want ErrNotExist", err)
	}
	if _, err := vfs.Attach(ctx, cfg(newMemStore(), c, 1)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Attach to nothing: %v, want ErrNotExist", err)
	}
	if _, err := vfs.Attach(ctx, cfg(be, c, 0)); err == nil {
		t.Fatal("Attach at round 0 succeeded")
	}
}

func TestFlushWithNothingToDoWritesNothing(t *testing.T) {
	be, _, v := newMem(t)
	write(t, v, vfs.RootInode, "f", []byte("x"))
	flush(t, v)
	be.Reset()
	flush(t, v)
	if n := be.Total(); n != 0 {
		t.Fatalf("an idle Flush wrote %d objects", n)
	}
}

// TestDedupeWithinVolume: equal chunks are stored once, whichever file they are in.
func TestDedupeWithinVolume(t *testing.T) {
	be, _, v := newMem(t)
	data := content(30, 6<<20)
	write(t, v, vfs.RootInode, "one", data)
	flush(t, v)
	packs := packBytes(be)
	write(t, v, vfs.RootInode, "two", data)
	write(t, v, vfs.RootInode, "three", append([]byte("prefix"), data...))
	flush(t, v)
	grew := packBytes(be) - packs
	t.Logf("6 MiB stored; two more copies (one shifted by 6 bytes) added %d KiB of packs", grew>>10)
	if grew > 2<<20 {
		t.Fatalf("copies added %d bytes of packs, want under 2 MiB: chunks were not shared", grew)
	}
}

// TestSmallEditRewritesOneChunk: editing a few bytes of a large file uploads
// about one chunk, not the file.
func TestSmallEditRewritesOneChunk(t *testing.T) {
	be, c, v := newMem(t)
	data := content(40, sized(8<<20, 16<<20))
	ino := write(t, v, vfs.RootInode, "f", data).ID
	flush(t, v)
	before := packBytes(be)

	f, _ := v.Open(ctx, ino)
	defer f.Close()
	if _, err := f.WriteAt(ctx, []byte("EDITED"), int64(len(data)/2)); err != nil {
		t.Fatal(err)
	}
	copy(data[len(data)/2:], "EDITED")
	flush(t, v)
	grew := packBytes(be) - before
	t.Logf("6 bytes edited in a file: %d KiB of new packs", grew>>10)
	if grew > 3<<20 {
		t.Fatalf("a 6 byte edit uploaded %d bytes", grew)
	}
	mustEqual(t, "edited file", readAll(t, reopen(t, be, c), ino), data)

	// Appending adds about the new bytes plus the last chunk.
	before = packBytes(be)
	tail := content(41, 100_000)
	if _, err := f.WriteAt(ctx, tail, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	data = append(data, tail...)
	flush(t, v)
	if grew = packBytes(be) - before; grew > 5<<20 {
		t.Fatalf("appending 100 KB uploaded %d bytes", grew)
	}
	mustEqual(t, "appended file", readAll(t, reopen(t, be, c), ino), data)
}

func TestSettleBoundsMemory(t *testing.T) {
	be := newMemStore()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	conf := cfg(be, c, 1)
	conf.Dirty = 4 << 20
	v, err := vfs.Attach(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	data := content(50, sized(12<<20, 40<<20))
	in, _ := v.Create(ctx, vfs.RootInode, "stream", 0o644)
	f, _ := v.Open(ctx, in.ID)
	defer f.Close()
	for off := 0; off < len(data); off += 1 << 20 {
		if _, err := f.WriteAt(ctx, data[off:off+1<<20], int64(off)); err != nil {
			t.Fatal(err)
		}
		if st := v.Stats(); st.Dirty > 5<<20 {
			t.Fatalf("after %d MiB written %d MiB are unsettled, limit 4", off>>20, st.Dirty>>20)
		}
	}
	flush(t, v)
	mustEqual(t, "streamed file", readAll(t, reopen(t, be, c), in.ID), data)
}

// TestInodeNumbersAreNotReused: the next inode number is part of the Head, so a
// new owner never hands out a number a committed file already holds.
func TestInodeNumbersAreNotReused(t *testing.T) {
	be, c, v := newMem(t)
	a := write(t, v, vfs.RootInode, "a", []byte("first"))
	flush(t, v)
	v2, err := vfs.Attach(ctx, cfg(be, c, 2))
	if err != nil {
		t.Fatal(err)
	}
	b := write(t, v2, vfs.RootInode, "b", []byte("second"))
	flush(t, v2)
	if a.ID == b.ID {
		t.Fatalf("inode %d handed out twice", a.ID)
	}
	ro := reopen(t, be, c)
	mustEqual(t, "a", readAll(t, ro, a.ID), []byte("first"))
	mustEqual(t, "b", readAll(t, ro, b.ID), []byte("second"))
}

// TestReadAfterSettleBeforeFlush: bytes the Volume has chunked and sealed but not
// committed are served from the open pack, or from the packs already uploaded.
func TestReadAfterSettleBeforeFlush(t *testing.T) {
	be := newMemStore()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	conf := cfg(be, c, 1)
	conf.Dirty = 1 << 20 // settle after every megabyte
	conf.Cache = 1 << 16 // and hold almost no chunk: reads must come from the packs
	v, err := vfs.Attach(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	data := content(55, sized(6<<20, 20<<20)) // in full runs more than a pack: some are uploaded, one is open
	in, _ := v.Create(ctx, vfs.RootInode, "f", 0o644)
	f, _ := v.Open(ctx, in.ID)
	defer f.Close()
	for off := 0; off < len(data); off += 1 << 20 {
		if _, err := f.WriteAt(ctx, data[off:off+1<<20], int64(off)); err != nil {
			t.Fatal(err)
		}
	}
	if !testing.Short() && be.Puts()["pack"] == 0 {
		t.Fatal("no pack was uploaded before the Flush; the test proves little")
	}
	if st := v.Stats(); st.Dirty > 2<<20 {
		t.Fatalf("%d bytes unsettled", st.Dirty)
	}
	mustEqual(t, "settled, not flushed", readAll(t, v, in.ID), data)
}

// putBig stores an object of the given size at key, whatever it was.
func putBig(t *testing.T, be *storetest.Mem, key string, n int) {
	t.Helper()
	if err := be.Put(ctx, key, bytes.Repeat([]byte("x"), n)); err != nil {
		t.Fatal(err)
	}
}

// TestHeadBound: a store that answers a read of the Head with a huge body is not
// believed, and not buffered.
func TestHeadBound(t *testing.T) {
	be, c, _ := newMem(t)
	putBig(t, be, "head", 8<<20)
	if _, err := vfs.Open(ctx, cfg(be, c, 0)); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("Open with an 8 MiB Head = %v, want ErrTooLarge", err)
	}
	if _, err := vfs.Attach(ctx, cfg(be, c, 9)); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("Attach with an 8 MiB Head = %v, want ErrTooLarge", err)
	}
}

// TestShardBound: the same for a metadata shard.
func TestShardBound(t *testing.T) {
	be, c, v := newMem(t)
	write(t, v, vfs.RootInode, "f", []byte("x"))
	flush(t, v)
	for k := range be.Dump() {
		if strings.HasPrefix(k, "meta/") {
			putBig(t, be, k, 8<<20)
		}
	}
	ro, err := vfs.Open(ctx, cfg(be, c, 0))
	if err == nil {
		_, err = ro.ReadDir(ctx, vfs.RootInode)
	}
	if !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("reading through an 8 MiB shard = %v, want ErrTooLarge", err)
	}
}

// getCount counts the reads of whole objects.
type getCount struct {
	backend.Backend
	gets int
}

func (g *getCount) Get(c context.Context, k string, max int64) ([]byte, backend.Version, error) {
	g.gets++
	return g.Backend.Get(c, k, max)
}

// TestRemoveChecksEmptinessWithoutListing: rmdir learns that a directory has an
// entry from the first one it finds. It does not list the directory, however large.
func TestRemoveChecksEmptinessWithoutListing(t *testing.T) {
	mem, c, v := newMem(t)
	dir := mkdir(t, v, vfs.RootInode, "big")
	for i := range sized(30_000, 100_000) {
		if _, err := v.Create(ctx, dir, fmt.Sprintf("entry-%08d", i), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, v)
	gc := &getCount{Backend: mem}
	cold, err := vfs.Attach(ctx, cfg(gc, c, 2))
	if err != nil {
		t.Fatal(err)
	}
	gc.gets = 0
	if err := cold.Remove(ctx, vfs.RootInode, "big"); !errors.Is(err, vfs.ErrNotEmpty) {
		t.Fatalf("rmdir of a non-empty directory = %v", err)
	}
	if gc.gets > 6 {
		t.Fatalf("rmdir read %d shards to learn one entry exists", gc.gets)
	}
}

// TestCacheIsBounded: whatever is read and written, the decoded shards and chunks
// held in memory stay near Config.Cache, however much data passes through.
func TestCacheIsBounded(t *testing.T) {
	be := newMemStore()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
		t.Fatal(err)
	}
	conf := cfg(be, c, 1)
	conf.Cache = 4 << 20
	v, err := vfs.Attach(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	total := sized(12, 60)
	var inodes []vfs.InodeID
	for i := range total {
		inodes = append(inodes, write(t, v, vfs.RootInode, fmt.Sprint("f", i), content(uint64(500+i), 1<<20)).ID)
		flush(t, v)
	}
	for _, ino := range inodes {
		readAll(t, v, ino)
	}
	st := v.Stats()
	if st.CacheBytes > st.CacheMax+(5<<20) {
		t.Fatalf("%d MiB passed through; the caches hold %d bytes against a bound of %d", total, st.CacheBytes, st.CacheMax)
	}
}

// TestListingSeesAChangeOnceAndARemovalNever: a name that was committed, then removed
// and made again, or rewritten, and not yet committed again, lists once, with its
// new inode, and a name that was removed does not list. The listing merges the
// committed tree with the pending changes, and the two must not both be shown.
func TestListingSeesAChangeOnceAndARemovalNever(t *testing.T) {
	_, _, v := newMem(t)
	dir := mkdir(t, v, vfs.RootInode, "d")
	var ids = map[string]vfs.InodeID{}
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		ids[n] = write(t, v, dir, n, []byte("old "+n)).ID
	}
	flush(t, v)

	if err := v.Remove(ctx, dir, "b"); err != nil { // removed
		t.Fatal(err)
	}
	if err := v.Remove(ctx, dir, "c"); err != nil { // removed and made again
		t.Fatal(err)
	}
	again := write(t, v, dir, "c", []byte("new c")).ID
	if again == ids["c"] {
		t.Fatal("a name made again reused its inode")
	}
	if err := v.Remove(ctx, dir, "e"); err != nil { // removed, made again, removed
		t.Fatal(err)
	}
	write(t, v, dir, "e", []byte("new e"))
	if err := v.Remove(ctx, dir, "e"); err != nil {
		t.Fatal(err)
	}
	write(t, v, dir, "f", []byte("new f")) // added

	check := func(when string) {
		ents, err := v.ReadDir(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, e := range ents {
			got = append(got, e.Name)
			if e.Name == "c" && e.ID != again {
				t.Fatalf("%s: c lists the inode it had before it was made again", when)
			}
		}
		if want := "a c d f"; strings.Join(got, " ") != want {
			t.Fatalf("%s: listing = %v, want %s", when, got, want)
		}
	}
	check("pending")
	flush(t, v)
	check("committed")
}

// TestIdenticalFilesBeforeAFlushShareTheirChunks: two files with the same large content
// written before any Flush are one chunk's bytes in the store. The check for a chunk
// that already exists must see chunks that are only pending, not just committed ones.
func TestIdenticalFilesBeforeAFlushShareTheirChunks(t *testing.T) {
	be, _, v := newMem(t)
	data := content(77, 2<<20)
	be.Reset()
	write(t, v, vfs.RootInode, "a", data)
	write(t, v, vfs.RootInode, "b", data)
	write(t, v, vfs.RootInode, "c", data)
	flush(t, v)
	if packs := packBytes(be); packs > len(data)+len(data)/4 {
		t.Fatalf("three identical %d byte files stored %d bytes of packs: the pending chunk was not seen", len(data), packs)
	}
}
