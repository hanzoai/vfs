//go:build fuse && (linux || darwin)

package mount_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

// watchdog kills the server and detaches the mount after d, so a hung request
// cannot hold the test process in the kernel.
func watchdog(t *testing.T, m *mounted, d time.Duration) (stop func()) {
	ch := make(chan struct{})
	go func() {
		select {
		case <-ch:
		case <-time.After(d):
			_ = m.Cmd.Process.Kill()
			_ = exec.Command("fusermount3", "-u", "-z", m.Dir).Run()
		}
	}()
	return func() { close(ch) }
}

func rnd(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed+1))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// storeView opens a cold read-only view of the store.
func storeView(t *testing.T, store string, key *age.HybridIdentity) *vfs.Volume {
	be, err := backend.Open(context.Background(), "file://"+store)
	if err != nil {
		t.Fatal(err)
	}
	ro, err := vfs.Open(context.Background(), vfs.Config{Backend: be, Cipher: vfs.Hybrid(key)})
	if err != nil {
		t.Fatal(err)
	}
	return ro
}

func storeBytes(t *testing.T, ro *vfs.Volume, p string) ([]byte, error) {
	in, err := ro.Resolve(context.Background(), p)
	if err != nil {
		return nil, err
	}
	f, err := ro.Open(context.Background(), in.ID)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, in.Size)
	n, err := f.ReadAt(context.Background(), buf, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

// TestFUSESemantics exercises the operations the mount has at the sizes
// where the layout changes shape, through the kernel, and checks the committed
// store agrees with what the kernel returned.
func TestFUSESemantics(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 150*time.Second)()

	// Sizes around the inline threshold (16 KiB), the minimum chunk (64 KiB) and
	// the maximum chunk (4 MiB).
	sizes := []int{0, 1, 16<<10 - 1, 16 << 10, 16<<10 + 1, 64<<10 - 1, 64 << 10, 64<<10 + 1, 512 << 10, 4<<20 - 1, 4 << 20, 4<<20 + 1, 9<<20 + 7}
	for i, n := range sizes {
		p := filepath.Join(mp, fmt.Sprintf("size-%d", n))
		want := rnd(uint64(i+1), n)
		if err := os.WriteFile(p, want, 0o644); err != nil {
			t.Fatalf("write %d: %v", n, err)
		}
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("size %d: read back %d bytes, %v", n, len(got), err)
		}
	}

	// Grow past the inline threshold, then shrink under it, same file.
	p := filepath.Join(mp, "morph")
	f, _ := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	a := rnd(100, 10<<10)
	f.WriteAt(a, 0)
	f.Close()
	f, _ = os.OpenFile(p, os.O_RDWR, 0o644)
	b := rnd(101, 200<<10)
	f.WriteAt(b, 8<<10) // overlaps the tail of a, extends past 16 KiB
	f.Close()
	model := append(append([]byte{}, a[:8<<10]...), b...)
	got, _ := os.ReadFile(p)
	if !bytes.Equal(got, model) {
		t.Fatalf("grow past inline: content differs (%d vs %d)", len(got), len(model))
	}
	if err := os.Truncate(p, 5000); err != nil {
		t.Fatal(err)
	}
	model = model[:5000]
	got, _ = os.ReadFile(p)
	if !bytes.Equal(got, model) {
		t.Fatalf("shrink under inline: content differs (%d vs %d)", len(got), len(model))
	}
	// Extend with a hole: truncate up, bytes between must read as zeros.
	if err := os.Truncate(p, 300<<10); err != nil {
		t.Fatal(err)
	}
	model = append(model, make([]byte, 300<<10-5000)...)
	got, _ = os.ReadFile(p)
	if !bytes.Equal(got, model) {
		t.Fatalf("extend after shrink: stale bytes reappear (len %d vs %d)", len(got), len(model))
	}

	// O_APPEND from two descriptors.
	ap := filepath.Join(mp, "append")
	f1, _ := os.OpenFile(ap, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	f2, _ := os.OpenFile(ap, os.O_WRONLY|os.O_APPEND, 0o644)
	var want []byte
	for i := range 50 {
		line := []byte(fmt.Sprintf("line %03d from %d\n", i, i%2))
		w := f1
		if i%2 == 1 {
			w = f2
		}
		if _, err := w.Write(line); err != nil {
			t.Fatal(err)
		}
		want = append(want, line...)
	}
	f1.Close()
	f2.Close()
	if got, _ := os.ReadFile(ap); !bytes.Equal(got, want) {
		t.Fatalf("O_APPEND lost or reordered writes: %d bytes, want %d", len(got), len(want))
	}

	// Sparse write far past EOF.
	sp := filepath.Join(mp, "sparse")
	f, _ = os.OpenFile(sp, os.O_CREATE|os.O_RDWR, 0o644)
	f.WriteAt([]byte("tail"), 6<<20)
	f.Close()
	got, _ = os.ReadFile(sp)
	if len(got) != 6<<20+4 || !bytes.Equal(got[:6<<20], make([]byte, 6<<20)) || string(got[6<<20:]) != "tail" {
		t.Fatalf("sparse file wrong: %d bytes", len(got))
	}

	m.Stop(t)

	// The committed store agrees.
	ro := storeView(t, store, key)
	for i, n := range sizes {
		got, err := storeBytes(t, ro, fmt.Sprintf("/size-%d", n))
		if err != nil || !bytes.Equal(got, rnd(uint64(i+1), n)) {
			t.Fatalf("committed size-%d differs: %d bytes, %v", n, len(got), err)
		}
	}
}

// TestFUSEHostileNames: names the kernel will pass through.
func TestFUSEHostileNames(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 90*time.Second)()

	names := []string{
		strings.Repeat("a", 255),
		"café",  // NFC
		"café", // NFD: a different name on Linux
		"CaSe", "case", "CASE",
		"trailing ", " leading", "tab\there", "new\nline", "\xff\xfe-not-utf8", "-rf", "~", "*", "?", "a:b", "a\\b", "..a", "a..", "...",
	}
	for i, n := range names {
		fd, err := syscall.Open(filepath.Join(mp, n), syscall.O_CREAT|syscall.O_RDWR, 0o644)
		if err != nil {
			t.Errorf("create %q: %v", n, err)
			continue
		}
		syscall.Write(fd, []byte(fmt.Sprint("content-", i)))
		syscall.Close(fd)
	}
	ents, err := os.ReadDir(mp)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != len(names) {
		t.Errorf("created %d distinct names, directory lists %d", len(names), len(ents))
	}
	for i, n := range names {
		b, err := os.ReadFile(filepath.Join(mp, n))
		if err != nil || string(b) != fmt.Sprint("content-", i) {
			t.Errorf("read %q: %q %v", n, b, err)
		}
	}
	// Path traversal through the mount can only reach the kernel's own '..'.
	os.Mkdir(filepath.Join(mp, "d"), 0o755)
	if _, err := os.Stat(filepath.Join(mp, "d", "..", "..", "..", "..", "etc")); err == nil {
		t.Logf("note: ../ out of the mount point reaches the host (kernel path walk, not the filesystem)")
	}
	// ls -a: are "." and ".." in the listing?
	out, _ := exec.Command("ls", "-a", filepath.Join(mp, "d")).CombinedOutput()
	t.Logf("ls -a of an empty directory: %q", strings.TrimSpace(string(out)))
	m.Stop(t)
	ro := storeView(t, store, key)
	for i, n := range names {
		b, err := storeBytes(t, ro, "/"+n)
		if err != nil || string(b) != fmt.Sprint("content-", i) {
			t.Errorf("committed %q: %q %v", n, b, err)
		}
	}
}

// TestFUSEConcurrentSameFile: several goroutines write disjoint ranges of one
// file and read it back while others read.
func TestFUSEConcurrentSameFile(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 120*time.Second)()
	p := filepath.Join(mp, "shared")
	if err := os.WriteFile(p, make([]byte, 8<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	want := make([]byte, 8<<20)
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := os.OpenFile(p, os.O_RDWR, 0)
			if err != nil {
				t.Error(err)
				return
			}
			defer f.Close()
			d := rnd(uint64(500+w), 1<<20)
			if _, err := f.WriteAt(d, int64(w)<<20); err != nil {
				t.Error(err)
			}
			copy(want[w<<20:], d)
			buf := make([]byte, 1<<20)
			for range 4 {
				if _, err := f.ReadAt(buf, int64((w+1)%8)<<20); err != nil && err != io.EOF {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	got, _ := os.ReadFile(p)
	if !bytes.Equal(got, want) {
		t.Fatal("concurrent disjoint writes: content differs")
	}
	m.Stop(t)
	ro := storeView(t, store, key)
	if b, err := storeBytes(t, ro, "/shared"); err != nil || !bytes.Equal(b, want) {
		t.Fatalf("committed content differs: %v", err)
	}
}

// storeObjects returns how many objects and bytes the store directory holds.
func storeObjects(store string) (objects int, bytes int64) {
	filepath.WalkDir(store, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.HasPrefix(d.Name(), ".") {
			objects++
			if fi, err := d.Info(); err == nil {
				bytes += fi.Size()
			}
		}
		return nil
	})
	return
}

// TestFUSECommitPerClose creates 400 files of 20 KiB with the plain cp -r pattern
// (create, write, close; every close is a commit) and reports how long it took and
// how many objects and bytes the store holds.
func TestFUSECommitPerClose(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 120*time.Second)()
	os.Mkdir(filepath.Join(mp, "t"), 0o755)
	before, _ := storeObjects(store)
	start := time.Now()
	const files = 400
	for i := range files {
		if err := os.WriteFile(filepath.Join(mp, "t", fmt.Sprintf("f%04d", i)), rnd(uint64(i), 20<<10), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	took := time.Since(start)
	after, bytes := storeObjects(store)
	t.Logf("%d files of 20 KiB through the mount: %v (%.0f files/s); store objects %d -> %d (%.1f per file); %.1f MiB stored for %.1f MiB of data (%.2fx)",
		files, took.Round(time.Millisecond), files/took.Seconds(), before, after, float64(after-before)/files, float64(bytes)/(1<<20), float64(files*20<<10)/(1<<20), float64(bytes)/float64(files*20<<10))
	m.Stop(t)
}

// checkoutSize is the size of file i of a source-tree-like checkout: mostly small
// files, a tail of large ones; about 57 KiB on average.
func checkoutSize(i int) int {
	r := rand.New(rand.NewPCG(uint64(i), 1234))
	switch p := r.IntN(100); {
	case p < 55:
		return 100 + r.IntN(4<<10)
	case p < 85:
		return 4<<10 + r.IntN(28<<10)
	case p < 97:
		return 32<<10 + r.IntN(224<<10)
	}
	return 256<<10 + r.IntN(1792<<10)
}

// TestFUSECheckout writes a 5,000-file checkout through the mount the way tar x or
// git checkout does (create, write, close, one commit per close), then reads every
// file back through the mount and from a cold view of the store, and reports the cost.
func TestFUSECheckout(t *testing.T) {
	if testing.Short() {
		t.Skip("5,000 commits")
	}
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 10*time.Minute)()
	const files = 5000
	var data int64
	for d := range files / 50 {
		os.Mkdir(filepath.Join(mp, fmt.Sprintf("d%03d", d)), 0o755)
	}
	start := time.Now()
	for i := range files {
		n := checkoutSize(i)
		data += int64(n)
		if err := os.WriteFile(filepath.Join(mp, fmt.Sprintf("d%03d", i%(files/50)), fmt.Sprintf("file%05d", i)), rnd(uint64(i), n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	took := time.Since(start)
	objects, bytes := storeObjects(store)
	t.Logf("checkout of %d files, %.1f MiB, through the mount: %v (%.0f files/s); the store holds %d objects (%.2f per file) and %.1f MiB (%.2fx the data)",
		files, float64(data)/(1<<20), took.Round(time.Millisecond), files/took.Seconds(), objects, float64(objects)/files, float64(bytes)/(1<<20), float64(bytes)/float64(data))
	for i := 0; i < files; i += 97 {
		got, err := os.ReadFile(filepath.Join(mp, fmt.Sprintf("d%03d", i%(files/50)), fmt.Sprintf("file%05d", i)))
		if err != nil || !bytes2equal(got, rnd(uint64(i), checkoutSize(i))) {
			t.Fatalf("file %d read back wrong: %d bytes, %v", i, len(got), err)
		}
	}
	m.Stop(t)
	ro := storeView(t, store, key)
	for i := 0; i < files; i += 331 {
		got, err := storeBytes(t, ro, fmt.Sprintf("/d%03d/file%05d", i%(files/50), i))
		if err != nil || !bytes2equal(got, rnd(uint64(i), checkoutSize(i))) {
			t.Fatalf("committed file %d wrong: %d bytes, %v", i, len(got), err)
		}
	}
}

func bytes2equal(a, b []byte) bool { return string(a) == string(b) }

// writeSynced writes a file, fsyncs it and closes it: what a caller that needs the bytes to
// survive a crash does.
func writeSynced(name string, data []byte) error {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// TestFUSEKillDuringWrite SIGKILLs the server in the middle of a large write
// and mounts again: everything fsynced before is intact, nothing torn.
func TestFUSEKillDuringWrite(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	stopdog := watchdog(t, m, 90*time.Second)
	good := rnd(1, 3<<20)
	if err := writeSynced(filepath.Join(mp, "good"), good); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(mp, "partial"), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(rnd(2, 3<<20+12345)) // not closed, not synced
	_ = m.Cmd.Process.Signal(syscall.SIGKILL)
	<-m.Done
	stopdog()
	f.Close() // fails: transport endpoint is not connected
	_ = exec.Command("fusermount3", "-u", "-z", mp).Run()
	time.Sleep(200 * time.Millisecond)

	m2 := mountAt(t, store, mp, key, 2)
	defer watchdog(t, m2, 60*time.Second)()
	got, err := os.ReadFile(filepath.Join(mp, "good"))
	if err != nil || !bytes.Equal(got, good) {
		t.Fatalf("closed file damaged by the kill: %d bytes, %v", len(got), err)
	}
	if st, err := os.Stat(filepath.Join(mp, "partial")); err == nil {
		if b, _ := os.ReadFile(filepath.Join(mp, "partial")); len(b) != int(st.Size()) {
			t.Fatalf("partial file torn: stat %d read %d", st.Size(), len(b))
		}
		t.Logf("partial file present with %d bytes (committed by another close)", st.Size())
	}
	m2.Stop(t)
}

// TestFUSEUnlinkVersusRmdir: unlink(2) of a directory must fail (EISDIR or
// EPERM) and rmdir(2) of a regular file must fail (ENOTDIR). The mount ignores
// RemoveRequest.Dir, so each succeeds against the wrong type.
func TestFUSEUnlinkVersusRmdir(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 60*time.Second)()
	os.Mkdir(filepath.Join(mp, "dir"), 0o755)
	os.WriteFile(filepath.Join(mp, "file"), []byte("x"), 0o644)
	errU := syscall.Unlink(filepath.Join(mp, "dir"))
	errR := syscall.Rmdir(filepath.Join(mp, "file"))
	t.Logf("unlink(dir) = %v ; rmdir(file) = %v", errU, errR)
	if errU == nil {
		t.Errorf("unlink(2) of a directory succeeded")
	}
	if errR == nil {
		t.Errorf("rmdir(2) of a regular file succeeded")
	}
	// getdents of an empty directory: "." and ".." are required by POSIX.
	os.Mkdir(filepath.Join(mp, "empty"), 0o755)
	fd, err := syscall.Open(filepath.Join(mp, "empty"), syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, err := syscall.ReadDirent(fd, buf)
	syscall.Close(fd)
	names, _ := parseDirents(buf[:n])
	t.Logf("getdents(empty dir) = %v err=%v", names, err)
	if len(names) < 2 {
		t.Errorf("readdir of an empty directory returned %v: no '.' and '..'", names)
	}
	m.Stop(t)
}

func parseDirents(b []byte) ([]string, int) {
	var out []string
	for len(b) > 19 {
		reclen := int(b[16]) | int(b[17])<<8
		if reclen == 0 || reclen > len(b) {
			break
		}
		name := b[19:reclen]
		if i := bytes.IndexByte(name, 0); i >= 0 {
			name = name[:i]
		}
		out = append(out, string(name))
		b = b[reclen:]
	}
	return out, len(out)
}

// vmrss returns the resident set of a process, in bytes.
func vmrss(t *testing.T, pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Skip("no /proc")
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "VmRSS:" {
			kb, _ := strconv.ParseInt(f[1], 10, 64)
			return kb << 10
		}
	}
	return 0
}

// TestFUSEPetabyteTruncate sends a 1 PiB truncate(2) through the kernel to the
// engine in a server process, writes a few bytes at the start, the middle and near
// the end, and fsyncs. The commit must take seconds, the server must stay small, and
// the store must hold a handful of objects: the cost follows the three runs of
// bytes, not the petabyte. A cold view of the store then reads the bytes back.
func TestFUSEPetabyteTruncate(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 120*time.Second)()
	rss0 := vmrss(t, m.Cmd.Process.Pid)

	const pib = int64(1) << 50
	f, err := os.OpenFile(filepath.Join(mp, "hole"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := f.Truncate(pib); err != nil {
		t.Fatalf("truncate to 1 PiB: %v", err)
	}
	spots := map[int64]string{0: "the start", pib / 2: "the middle", pib - 20: "the end"}
	for off, s := range spots {
		if _, err := f.WriteAt([]byte(s), off); err != nil {
			t.Fatalf("write at %d: %v", off, err)
		}
	}
	if err := f.Sync(); err != nil { // a full commit
		t.Fatal(err)
	}
	took := time.Since(start)
	st, err := f.Stat()
	if err != nil || st.Size() != pib {
		t.Fatalf("fstat = %v, %v; want 1 PiB", st, err)
	}
	f.Close()
	grew := vmrss(t, m.Cmd.Process.Pid) - rss0
	objects := 0
	filepath.WalkDir(store, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.HasPrefix(d.Name(), ".") {
			objects++
		}
		return nil
	})
	t.Logf("1 PiB truncate, 3 writes, fsync through the mount: %v; server RSS +%d MiB; %d objects in the store", took.Round(time.Millisecond), grew>>20, objects)
	if took > 10*time.Second {
		t.Errorf("truncate, writes and fsync of a 1 PiB file took %v", took)
	}
	if grew > 512<<20 {
		t.Errorf("the server grew %d MiB for a 1 PiB file", grew>>20)
	}
	if objects > 12 {
		t.Errorf("%d objects for a 1 PiB file with three runs of bytes", objects)
	}
	m.Stop(t)

	ro := storeView(t, store, key)
	in, err := ro.Resolve(context.Background(), "/hole")
	if err != nil || in.Size != uint64(pib) {
		t.Fatalf("committed size %d, %v", in.Size, err)
	}
	h, _ := ro.Open(context.Background(), in.ID)
	for off, s := range spots {
		buf := make([]byte, len(s))
		if _, err := h.ReadAt(context.Background(), buf, off); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if string(buf) != s {
			t.Errorf("bytes at %d are %q, want %q", off, buf, s)
		}
	}
}

// TestFUSEFileTooBig: a size past what a file can be is EFBIG at the system call, not
// an I/O error and not an acceptance.
func TestFUSEFileTooBig(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 60*time.Second)()
	f, err := os.OpenFile(filepath.Join(mp, "f"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() // an open file keeps the mount busy, and a failed test must not leave it
	if err := f.Truncate(1<<62 + 1); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("truncate past the maximum = %v, want EFBIG", err)
	}
	if err := f.Truncate(1 << 62); err != nil {
		t.Fatalf("truncate to the maximum: %v", err)
	}
	f.Close()
	m.Stop(t)
}

// TestFUSEFsyncSurvivesKill: a file whose fsync(2) returned is in the Volume when the
// server is killed with SIGKILL straight after, whether or not it was ever closed: the
// next mount of the Volume on the node replays the journal and publishes it, so the store
// has it by the time the mount is up.
func TestFUSEFsyncSurvivesKill(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	stopdog := watchdog(t, m, 90*time.Second)
	const files = 20
	var open []*os.File
	for i := range files {
		f, err := os.OpenFile(filepath.Join(mp, fmt.Sprintf("a%02d", i)), os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		open = append(open, f)
		if _, err := f.Write(rnd(uint64(i), 20_000+i*37_000)); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil { // acknowledged: durable in the journal
			t.Fatal(err)
		}
	}
	_ = m.Cmd.Process.Signal(syscall.SIGKILL)
	<-m.Done
	stopdog()
	for _, f := range open {
		f.Close() // fails: transport endpoint is not connected
	}
	_ = exec.Command("fusermount3", "-u", "-z", mp).Run()
	time.Sleep(200 * time.Millisecond)

	m2 := mountAt(t, store, mp, key, 2)
	defer watchdog(t, m2, 60*time.Second)()
	ro := storeView(t, store, key)
	for i := range files {
		got, err := storeBytes(t, ro, fmt.Sprintf("/a%02d", i))
		if err != nil || !bytes.Equal(got, rnd(uint64(i), 20_000+i*37_000)) {
			t.Fatalf("file a%02d, acknowledged by fsync, is not in the store after SIGKILL and a mount: %d bytes, %v", i, len(got), err)
		}
	}
	m2.Stop(t)
}

// TestFUSETermStopsAMountInUse: SIGTERM commits what the mount holds, asks for the unmount,
// and when a process still has a file open on the mount (the unmount is refused as busy)
// detaches it after a few seconds and ends, instead of staying mounted and alive for ever. A
// write that was never synced or closed is in the store, and the directory is free.
func TestFUSETermStopsAMountInUse(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	f, err := os.OpenFile(filepath.Join(mp, "held"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	want := rnd(7, 123_457)
	if _, err := f.Write(want); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_ = m.Cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-m.Done:
		if err != nil {
			t.Errorf("the server exited with %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("SIGTERM did not end a mount that a file is open on")
	}
	t.Logf("a mount with an open file ended %v after SIGTERM", time.Since(start).Round(100*time.Millisecond))
	if out, _ := os.ReadFile("/proc/self/mountinfo"); bytes.Contains(out, []byte(" "+mp+" ")) {
		t.Error("the directory is still a mount point")
	}
	got, err := storeBytes(t, storeView(t, store, key), "/held")
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("a file written and never synced is not in the store after SIGTERM: %d bytes of %d, %v", len(got), len(want), err)
	}
}

// TestFUSEStopPublishes: a file that was written and never synced or closed is in the store
// when the mount ends with SIGTERM, which has the kernel hand over what it holds and then
// publishes everything. Until then it was in the journal and the Volume's memory only.
func TestFUSEStopPublishes(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	f, err := os.OpenFile(filepath.Join(mp, "held"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	want := rnd(77, 900_000)
	if _, err := f.Write(want); err != nil {
		t.Fatal(err)
	}
	if _, err := storeBytes(t, storeView(t, store, key), "/held"); err == nil {
		t.Fatal("the store has a file that nothing asked it to publish")
	}
	_ = f.Close()
	m.Stop(t)
	got, err := storeBytes(t, storeView(t, store, key), "/held")
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("after the mount ended the store has %d bytes, %v; want %d", len(got), err, len(want))
	}
}
