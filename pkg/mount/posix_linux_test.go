//go:build fuse && linux

package mount_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/luxfi/age"
	"golang.org/x/sys/unix"

	"github.com/hanzoai/vfs"
)

var bg = context.Background()

func sys(t *testing.T, fi os.FileInfo) *syscall.Stat_t {
	t.Helper()
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("no stat_t")
	}
	return st
}

func put(t *testing.T, p string, data []byte) {
	t.Helper()
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestFUSERename drives rename(2) through the kernel: a file, a directory with contents,
// over an existing file, over an empty directory, across directories, and every refusal.
func TestFUSERename(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	j := func(n ...string) string { return filepath.Join(append([]string{mp}, n...)...) }

	put(t, j("a"), []byte("alpha"))
	if err := os.Rename(j("a"), j("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(j("a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the old name: %v", err)
	}
	if string(get(t, j("b"))) != "alpha" {
		t.Fatal("renamed file lost its bytes")
	}
	// over an existing file: the new bytes, and the old file is gone
	put(t, j("c"), []byte("gamma"))
	if err := os.Rename(j("c"), j("b")); err != nil {
		t.Fatal(err)
	}
	if string(get(t, j("b"))) != "gamma" {
		t.Fatal("rename over a file kept the old bytes")
	}
	// a directory with a tree under it
	if err := os.MkdirAll(j("d", "e", "f"), 0o755); err != nil {
		t.Fatal(err)
	}
	put(t, j("d", "e", "f", "x"), []byte("deep"))
	if err := os.Mkdir(j("target"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(j("d"), j("target", "moved")); err != nil {
		t.Fatal(err)
	}
	if string(get(t, j("target", "moved", "e", "f", "x"))) != "deep" {
		t.Fatal("a moved directory lost its contents")
	}
	// the moved directory's ".." is where it went
	up, err := os.Stat(j("target", "moved", ".."))
	tgt, _ := os.Stat(j("target"))
	if err != nil || !os.SameFile(up, tgt) {
		t.Fatalf("'..' of the moved directory: %v %v", up, err)
	}
	// over an empty directory, and refusals
	os.Mkdir(j("empty"), 0o755)
	os.Mkdir(j("full"), 0o755)
	put(t, j("full", "x"), nil)
	// os.Rename refuses to name a directory that exists, so these go to the kernel as they are
	if err := syscall.Rename(j("target", "moved"), j("empty")); err != nil {
		t.Fatalf("a directory over an empty one: %v", err)
	}
	if err := syscall.Rename(j("empty"), j("full")); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("over a full directory: %v", err)
	}
	if err := syscall.Rename(j("empty"), j("b")); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("a directory over a file: %v", err)
	}
	if err := syscall.Rename(j("b"), j("empty")); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("a file over a directory: %v", err)
	}
	if err := syscall.Rename(j("empty"), j("empty", "e", "inside")); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("a directory into itself: %v", err)
	}
	if err := os.Rename(j("none"), j("x")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a name that is not there: %v", err)
	}
	// the same file under two names stays
	if err := os.Link(j("b"), j("b2")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(j("b"), j("b2")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(j("b")); err != nil {
		t.Fatalf("rename between two names of one file removed one: %v", err)
	}
	// the flags of renameat2 are not delivered by this mount's FUSE library
	if err := syscall.Rename(j("b"), j("b3")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Renameat2(unix.AT_FDCWD, j("b3"), unix.AT_FDCWD, j("b4"), unix.RENAME_NOREPLACE); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("renameat2 with RENAME_NOREPLACE: %v, want EINVAL (documented)", err)
	}
	m.Stop(t)

	ro := storeView(t, store, key)
	if got, err := storeBytes(t, ro, "/empty/e/f/x"); err != nil || string(got) != "deep" {
		t.Fatalf("the committed store: %q %v", got, err)
	}
	if err := ro.Check(bg); err != nil {
		t.Fatal(err)
	}
}

// TestFUSELinks: symbolic links and hard links, with their counts, through the kernel.
func TestFUSELinks(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	j := func(n ...string) string { return filepath.Join(append([]string{mp}, n...)...) }

	put(t, j("file"), []byte("payload"))
	if err := os.Symlink("file", j("sym")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/nowhere/at/all", j("dangling")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.Readlink(j("sym")); err != nil || got != "file" {
		t.Fatalf("readlink: %q %v", got, err)
	}
	if fi, err := os.Lstat(j("sym")); err != nil || fi.Mode()&os.ModeSymlink == 0 || fi.Size() != 4 {
		t.Fatalf("lstat of a link: %v %v", fi, err)
	}
	if string(get(t, j("sym"))) != "payload" {
		t.Fatal("reading through a symbolic link")
	}
	if _, err := os.Stat(j("dangling")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat of a dangling link: %v", err)
	}
	if err := os.Mkdir(j("dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../file", j("dir", "up")); err != nil {
		t.Fatal(err)
	}
	if string(get(t, j("dir", "up"))) != "payload" {
		t.Fatal("a relative link through a directory")
	}
	// hard links
	if err := os.Link(j("file"), j("hard")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(j("file"), j("dir", "hard2")); err != nil {
		t.Fatal(err)
	}
	a, _ := os.Stat(j("file"))
	b, _ := os.Stat(j("dir", "hard2"))
	if !os.SameFile(a, b) || sys(t, a).Nlink != 3 {
		t.Fatalf("hard links: same=%v nlink=%d", os.SameFile(a, b), sys(t, a).Nlink)
	}
	// a write through one name is read through another, and size and times are shared
	f, _ := os.OpenFile(j("hard"), os.O_WRONLY|os.O_APPEND, 0)
	f.WriteString("+more")
	f.Close()
	if string(get(t, j("dir", "hard2"))) != "payload+more" {
		t.Fatal("a write through one link is not seen through another")
	}
	if err := os.Remove(j("file")); err != nil {
		t.Fatal(err)
	}
	if st := sys(t, mustStat(t, j("hard"))); st.Nlink != 2 {
		t.Fatalf("after one unlink: nlink=%d", st.Nlink)
	}
	if err := os.Link(j("dir"), j("dirlink")); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("a hard link to a directory: %v", err)
	}
	if err := os.Link(j("hard"), j("hard")); !errors.Is(err, os.ErrExist) {
		t.Fatalf("a link over its own name: %v", err)
	}
	// a hard link to a symbolic link names the link itself
	if err := os.Link(j("sym"), j("symhard")); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Lstat(j("symhard")); fi.Mode()&os.ModeSymlink == 0 {
		t.Fatal("a link to a symbolic link followed it")
	}
	// a name of the longest allowed length, and one byte more
	long := string(bytes.Repeat([]byte("n"), 255))
	if err := os.Symlink("x", j(long)); err != nil {
		t.Fatalf("a 255 byte name: %v", err)
	}
	if err := os.Symlink("x", j(long+"n")); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("a 256 byte name: %v", err)
	}
	if err := os.Symlink(string(bytes.Repeat([]byte("t"), 4096)), j("longtarget")); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("a 4096 byte target: %v", err)
	}
	m.Stop(t)

	// committed: a read-only view and a second mount see the same
	ro := storeView(t, store, key)
	if err := ro.Check(bg); err != nil {
		t.Fatal(err)
	}
	in, err := ro.Resolve(bg, "/hard")
	if err != nil || in.Nlink != 2 {
		t.Fatalf("committed: %+v %v", in, err)
	}
	m2 := mountAt(t, store, mp, key, 2)
	defer watchdog(t, m2, 90*time.Second)()
	if got, err := os.Readlink(j("dir", "up")); err != nil || got != "../file" {
		t.Fatalf("after a remount: %q %v", got, err)
	}
	if string(get(t, j("dir", "hard2"))) != "payload+more" {
		t.Fatal("after a remount")
	}
	m2.Stop(t)
}

func mustStat(t *testing.T, p string) os.FileInfo {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi
}

// TestFUSEAttributes: chmod with the special bits, chown to oneself, utimens to the
// nanosecond (and to now), and the directory times that git's untracked cache relies on.
func TestFUSEAttributes(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	j := func(n ...string) string { return filepath.Join(append([]string{mp}, n...)...) }

	put(t, j("f"), []byte("x"))
	st := sys(t, mustStat(t, j("f")))
	if int(st.Uid) != os.Getuid() || int(st.Gid) != os.Getgid() {
		t.Fatalf("a file I made is owned by %d:%d, I am %d:%d", st.Uid, st.Gid, os.Getuid(), os.Getgid())
	}
	// the sticky bit is stored but never reported: the FUSE library of this mount leaves it out of
	// the attributes it answers with (SPECIFICATION 11.2)
	for _, mode := range []os.FileMode{0o600, 0o755, 0o755 | os.ModeSetuid, 0o755 | os.ModeSetgid, 0o644 | os.ModeSetuid | os.ModeSetgid} {
		want := mode
		if err := os.Chmod(j("f"), mode); err != nil {
			t.Fatalf("chmod %v: %v", mode, err)
		}
		if got := mustStat(t, j("f")).Mode(); got&(os.ModePerm|os.ModeSetuid|os.ModeSetgid) != want&(os.ModePerm|os.ModeSetuid|os.ModeSetgid) {
			t.Fatalf("chmod %v read back as %v", want, got)
		}
	}
	// chown to oneself is allowed to a user; to someone else it is not, and the kernel says so
	if err := os.Chown(j("f"), os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("chown to myself: %v", err)
	}
	if os.Getuid() != 0 {
		if err := os.Chown(j("f"), os.Getuid()+1, -1); !errors.Is(err, syscall.EPERM) {
			t.Fatalf("chown to another user: %v", err)
		}
	}
	// utimens: nanoseconds survive
	at, mt := time.Unix(1_000_000_000, 123_456_789), time.Unix(1_500_000_000, 987_654_321)
	if err := os.Chtimes(j("f"), at, mt); err != nil {
		t.Fatal(err)
	}
	fi := mustStat(t, j("f"))
	if !fi.ModTime().Equal(mt) {
		t.Fatalf("mtime %v, want %v", fi.ModTime(), mt)
	}
	if a := time.Unix(sys(t, fi).Atim.Unix()); !a.Equal(time.Unix(at.Unix(), at.UnixNano()%1e9)) {
		t.Fatalf("atime %v, want %v", a, at)
	}
	// a write moves mtime and ctime
	before := mustStat(t, j("f"))
	time.Sleep(20 * time.Millisecond)
	f, _ := os.OpenFile(j("f"), os.O_WRONLY, 0)
	f.WriteAt([]byte("y"), 0)
	f.Close()
	after := mustStat(t, j("f"))
	if !after.ModTime().After(before.ModTime()) || !ctime(sys(t, after)).After(ctime(sys(t, before))) {
		t.Fatalf("a write did not move mtime and ctime: %v %v -> %v %v", before.ModTime(), ctime(sys(t, before)), after.ModTime(), ctime(sys(t, after)))
	}
	// a chmod moves ctime and not mtime
	time.Sleep(20 * time.Millisecond)
	os.Chmod(j("f"), 0o640)
	c := mustStat(t, j("f"))
	if !c.ModTime().Equal(after.ModTime()) || !ctime(sys(t, c)).After(ctime(sys(t, after))) {
		t.Fatalf("chmod: mtime %v -> %v, ctime %v -> %v", after.ModTime(), c.ModTime(), ctime(sys(t, after)), ctime(sys(t, c)))
	}
	// a directory's times follow its entries: create, unlink, rename, link, symlink, mkdir
	os.Mkdir(j("d"), 0o755)
	os.Mkdir(j("d2"), 0o755)
	last := mustStat(t, j("d")).ModTime()
	step := func(what string, do func() error) {
		t.Helper()
		time.Sleep(10 * time.Millisecond)
		if err := do(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		now := mustStat(t, j("d")).ModTime()
		if !now.After(last) {
			t.Fatalf("%s did not move the directory's mtime (%v, was %v)", what, now, last)
		}
		last = now
	}
	step("create", func() error { return os.WriteFile(j("d", "a"), nil, 0o644) })
	step("mkdir", func() error { return os.Mkdir(j("d", "sub"), 0o755) })
	step("symlink", func() error { return os.Symlink("a", j("d", "s")) })
	step("link", func() error { return os.Link(j("d", "a"), j("d", "l")) })
	step("rename within", func() error { return os.Rename(j("d", "l"), j("d", "l2")) })
	step("rename out", func() error { return os.Rename(j("d", "l2"), j("d2", "l2")) })
	step("rename in", func() error { return os.Rename(j("d2", "l2"), j("d", "l3")) })
	step("unlink", func() error { return os.Remove(j("d", "a")) })
	step("rmdir", func() error { return os.Remove(j("d", "sub")) })
	m.Stop(t)

	ro := storeView(t, store, key)
	if err := ro.Check(bg); err != nil {
		t.Fatal(err)
	}
}

func ctime(st *syscall.Stat_t) time.Time { return time.Unix(st.Ctim.Unix()) }

// TestFUSEUnlinkWhileOpen: a file that is unlinked while it is open is read, written,
// truncated and stat'd through its descriptor until the last close, and then it is gone.
func TestFUSEUnlinkWhileOpen(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	j := func(n ...string) string { return filepath.Join(append([]string{mp}, n...)...) }

	f, err := os.OpenFile(j("victim"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	big := rnd(9, 1<<20)
	if _, err := f.Write(big); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(j("victim")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(j("victim")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the name after unlink: %v", err)
	}
	buf := make([]byte, len(big))
	if _, err := f.ReadAt(buf, 0); err != nil && err != io.EOF || !bytes.Equal(buf, big) {
		t.Fatalf("read after unlink: %v", err)
	}
	if _, err := f.WriteAt([]byte("after"), int64(len(big))); err != nil {
		t.Fatalf("write after unlink: %v", err)
	}
	if err := f.Truncate(10); err != nil {
		t.Fatalf("truncate after unlink: %v", err)
	}
	fi, err := f.Stat()
	if err != nil || fi.Size() != 10 || sys(t, fi).Nlink != 0 {
		t.Fatalf("fstat after unlink: %v %v nlink %d", fi, err, sys(t, fi).Nlink)
	}
	// another name for it cannot be made
	if err := os.Link(j("victim"), j("again")); err == nil {
		t.Fatal("linked an unlinked file")
	}
	// the mount goes on: commit, create, and the file is not in the store
	if err := os.WriteFile(j("other"), []byte("o"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	ro := storeView(t, store, key)
	if _, err := ro.Lookup(bg, vfs.RootInode, "victim"); err == nil {
		t.Fatal("an unlinked file is in the store")
	}
	if err := ro.Check(bg); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// a rename over an open file: the old file lives for its descriptor
	put(t, j("old"), []byte("old bytes"))
	put(t, j("new"), []byte("new"))
	g, _ := os.Open(j("old"))
	if err := os.Rename(j("new"), j("old")); err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(g)
	g.Close()
	if string(b) != "old bytes" || string(get(t, j("old"))) != "new" {
		t.Fatalf("rename over an open file: %q then %q", b, get(t, j("old")))
	}
	m.Stop(t)
}

// TestFUSEFallocate: fallocate(2) in each mode the kernel hands a FUSE file system.
func TestFUSEFallocate(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	j := func(n ...string) string { return filepath.Join(append([]string{mp}, n...)...) }

	data := rnd(3, 4<<20)
	put(t, j("f"), data)
	f, err := os.OpenFile(j("f"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	const (
		keep  = unix.FALLOC_FL_KEEP_SIZE
		punch = unix.FALLOC_FL_PUNCH_HOLE
		zero  = unix.FALLOC_FL_ZERO_RANGE
	)
	fd := int(f.Fd())
	// punch a hole: the size stays, the bytes read as zero
	if err := unix.Fallocate(fd, keep|punch, 1<<20, 2<<20); err != nil {
		t.Fatalf("punch: %v", err)
	}
	clear(data[1<<20 : 3<<20])
	if got := get(t, j("f")); !bytes.Equal(got, data) {
		t.Fatalf("after a punch the file differs (%d bytes)", len(got))
	}
	// extend with mode 0; keep size leaves it
	if err := unix.Fallocate(fd, 0, 0, 5<<20); err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if fi := mustStat(t, j("f")); fi.Size() != 5<<20 {
		t.Fatalf("size after allocating past the end: %d", fi.Size())
	}
	data = append(data, make([]byte, 1<<20)...)
	if err := unix.Fallocate(fd, keep, 0, 8<<20); err != nil {
		t.Fatalf("keep size: %v", err)
	}
	if fi := mustStat(t, j("f")); fi.Size() != 5<<20 {
		t.Fatalf("keep size changed the size to %d", fi.Size())
	}
	// zero range, without keep size, extends
	if err := unix.Fallocate(fd, zero, 4<<20, 2<<20); err != nil {
		t.Fatalf("zero range: %v", err)
	}
	data = append(data, make([]byte, 1<<20)...)
	clear(data[4<<20:])
	if got := get(t, j("f")); !bytes.Equal(got, data) || len(got) != 6<<20 {
		t.Fatalf("after zeroing a range: %d bytes", len(got))
	}
	// the modes that this mount does not do
	if err := unix.Fallocate(fd, unix.FALLOC_FL_COLLAPSE_RANGE, 0, 4096); !errors.Is(err, syscall.EOPNOTSUPP) { // collapse range
		t.Fatalf("collapse range: %v", err)
	}
	f.Close()
	m.Stop(t)
	ro := storeView(t, store, key)
	if got, err := storeBytes(t, ro, "/f"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("committed: %d bytes %v", len(got), err)
	}
}

// TestFUSEStatfs: statfs reports what the Volume holds.
func TestFUSEStatfs(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	var s0, s1 syscall.Statfs_t
	if err := syscall.Statfs(mp, &s0); err != nil {
		t.Fatal(err)
	}
	if s0.Blocks == 0 || s0.Bfree == 0 || s0.Bavail > s0.Bfree || s0.Bfree > s0.Blocks || s0.Namelen != 255 || s0.Bsize != 4096 {
		t.Fatalf("statfs: %+v", s0)
	}
	put(t, filepath.Join(mp, "big"), rnd(5, 20<<20))
	if err := syscall.Statfs(mp, &s1); err != nil {
		t.Fatal(err)
	}
	if used := (s1.Blocks - s1.Bfree) - (s0.Blocks - s0.Bfree); used < 20<<20/4096 || used > 21<<20/4096 {
		t.Fatalf("20 MiB written, statfs says %d blocks more are used (%+v then %+v)", used, s0, s1)
	}
	if s1.Files-s1.Ffree != s0.Files-s0.Ffree+1 {
		t.Fatalf("inodes in use: %d then %d", s0.Files-s0.Ffree, s1.Files-s1.Ffree)
	}
	m.Stop(t)
}

// TestFUSELocks: flock and fcntl locks are advisory and the kernel's: a second descriptor
// conflicts with the first, and nothing hangs.
func TestFUSELocks(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	p := filepath.Join(mp, "lock")
	put(t, p, make([]byte, 4096))
	a, _ := os.OpenFile(p, os.O_RDWR, 0)
	b, _ := os.OpenFile(p, os.O_RDWR, 0)
	if err := syscall.Flock(int(a.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock: %v", err)
	}
	if err := syscall.Flock(int(b.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("a second exclusive flock: %v", err)
	}
	if err := syscall.Flock(int(b.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("a shared flock under an exclusive one: %v", err)
	}
	if err := syscall.Flock(int(a.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(b.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock after the release: %v", err)
	}
	// open file description locks conflict between two descriptors of one process
	lock := func(f *os.File, typ int16) error {
		return unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &unix.Flock_t{Type: typ, Whence: 0, Start: 100, Len: 50})
	}
	if err := lock(a, unix.F_WRLCK); err != nil {
		t.Fatalf("a byte range lock: %v", err)
	}
	if err := lock(b, unix.F_WRLCK); !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EACCES) {
		t.Fatalf("a conflicting byte range lock: %v", err)
	}
	if err := lock(a, unix.F_UNLCK); err != nil {
		t.Fatal(err)
	}
	if err := lock(b, unix.F_WRLCK); err != nil {
		t.Fatalf("byte range lock after the release: %v", err)
	}
	a.Close()
	b.Close()
	m.Stop(t)
}

// TestFUSEMmap: a shared writable mapping writes through to the file.
func TestFUSEMmap(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	p := filepath.Join(mp, "mapped")
	data := rnd(8, 1<<20)
	put(t, p, data)
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	mem, err := syscall.Mmap(int(f.Fd()), 0, len(data), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mem, data) {
		t.Fatal("a mapping reads other bytes than the file has")
	}
	copy(mem[100:], "written through a mapping")
	copy(mem[len(mem)-10:], "tail-write")
	copy(data[100:], "written through a mapping")
	copy(data[len(data)-10:], "tail-write")
	if _, _, e := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&mem[0])), uintptr(len(mem)), syscall.MS_SYNC); e != 0 {
		t.Fatalf("msync: %v", e)
	}
	if err := syscall.Munmap(mem); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if got := get(t, p); !bytes.Equal(got, data) {
		t.Fatal("writes through a mapping did not reach the file")
	}
	m.Stop(t)
	ro := storeView(t, store, key)
	if got, err := storeBytes(t, ro, "/mapped"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("committed: %d bytes %v", len(got), err)
	}
}

// TestFUSESpecialFiles: named pipes and sockets are made, found and removed; a device is
// refused; extended attributes and O_TMPFILE are honestly not supported.
func TestFUSESpecialFiles(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	j := func(n ...string) string { return filepath.Join(append([]string{mp}, n...)...) }

	if err := syscall.Mkfifo(j("fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Lstat(j("fifo")); err != nil || fi.Mode()&os.ModeNamedPipe == 0 || fi.Mode().Perm() != 0o600 {
		t.Fatalf("fifo: %v %v", fi, err)
	}
	// the kernel carries the bytes of a pipe: a reader and a writer on it
	r, err := os.OpenFile(j("fifo"), os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	w, err := os.OpenFile(j("fifo"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.WriteString("through a pipe")
	buf := make([]byte, 64)
	n, _ := r.Read(buf)
	r.Close()
	w.Close()
	if string(buf[:n]) != "through a pipe" {
		t.Fatalf("pipe: %q", buf[:n])
	}
	// a unix socket bound on the mount
	// the path of a socket is at most 107 bytes, which a test directory's is not: bind through a descriptor
	dirfd, err := os.Open(mp)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("unix", fmt.Sprintf("/proc/self/fd/%d/sock", dirfd.Fd()))
	if err != nil {
		t.Fatalf("bind of a unix socket on the mount: %v", err)
	}
	if fi, err := os.Lstat(j("sock")); err != nil || fi.Mode()&os.ModeSocket == 0 {
		t.Fatalf("socket: %v %v", fi, err)
	}
	l.Close()
	dirfd.Close()
	os.Remove(j("sock"))
	// directory listing shows the types
	ents, err := os.ReadDir(mp)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if e.Name() == "fifo" && e.Type()&os.ModeNamedPipe == 0 {
			t.Fatalf("readdir says %v for the pipe", e.Type())
		}
	}
	if err := unix.Mknod(j("dev"), unix.S_IFCHR|0o600, int(unix.Mkdev(1, 3))); err == nil || !errors.Is(err, syscall.EPERM) {
		t.Fatalf("a device node: %v", err)
	}
	put(t, j("x"), nil)
	if err := unix.Setxattr(j("x"), "user.k", []byte("v"), 0); !errors.Is(err, syscall.ENOTSUP) {
		t.Fatalf("setxattr: %v, want ENOTSUP", err)
	}
	if _, err := unix.Getxattr(j("x"), "user.k", make([]byte, 8)); !errors.Is(err, syscall.ENOTSUP) {
		t.Fatalf("getxattr: %v, want ENOTSUP", err)
	}
	fd, err := unix.Open(mp, unix.O_TMPFILE|unix.O_RDWR, 0o600)
	if err == nil {
		unix.Close(fd)
		t.Fatal("O_TMPFILE worked: the mount's FUSE library has no request for it")
	}
	if !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("O_TMPFILE: %v, want EOPNOTSUPP", err)
	}
	m.Stop(t)
}

// TestFUSEReaddirDots: "." and ".." of a directory are the directory and its parent, by inode.
func TestFUSEReaddirDots(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()
	os.MkdirAll(filepath.Join(mp, "a", "b"), 0o755)
	b := filepath.Join(mp, "a", "b")
	f, err := os.Open(b)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, err := syscall.ReadDirent(int(f.Fd()), buf)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{".": sys(t, mustStat(t, b)).Ino, "..": sys(t, mustStat(t, filepath.Join(mp, "a"))).Ino}
	got := map[string]uint64{}
	for p := buf[:n]; len(p) > 19; {
		reclen := int(p[16]) | int(p[17])<<8
		name := string(bytes.TrimRight(p[19:reclen], "\x00"))
		got[name] = *(*uint64)(unsafe.Pointer(&p[0]))
		p = p[reclen:]
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%q is inode %d in the listing, %d by stat (%v)", k, got[k], v, got)
		}
	}
	// the link count of a directory is 2 and a subdirectory each
	if n := sys(t, mustStat(t, filepath.Join(mp, "a"))).Nlink; n != 3 {
		t.Fatalf("nlink of a directory with one subdirectory: %d", n)
	}
	m.Stop(t)
}
