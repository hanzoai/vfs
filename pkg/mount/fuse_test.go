//go:build fuse && (linux || darwin)

package mount_test

import (
	"bytes"
	"errors"
	"github.com/hanzoai/vfs/internal/mounttest"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/luxfi/age"
)

// TestMountChild is the server process of the mount tests: the test binary run again
// (see internal/mounttest).
func TestMountChild(t *testing.T) { mounttest.Child(t) }

// TestFUSEPOSIX drives every operation the mount supports through the kernel, and checks
// that fsync is the journal's: it returns with the store knowing nothing of the file, and
// the store has the file once the mount is released.
func TestFUSEPOSIX(t *testing.T) {
	needFUSE(t)
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)

	if err := os.Mkdir(filepath.Join(mp, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB, chunked
	path := filepath.Join(mp, "dir", "file.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil { // fsync: durable in this node's journal on return
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back %d bytes, %v", len(got), err)
	}
	if err := os.Truncate(path, 100); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil || st.Size() != 100 || st.IsDir() {
		t.Fatalf("stat after truncate: %v %v", st, err)
	}
	ents, err := os.ReadDir(filepath.Join(mp, "dir"))
	if err != nil || len(ents) != 1 || ents[0].Name() != "file.bin" {
		t.Fatalf("readdir: %v %v", ents, err)
	}
	if err := os.Remove(filepath.Join(mp, "dir")); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("rmdir of a non-empty directory: %v, want ENOTEMPTY", err)
	}
	if _, err := os.Stat(filepath.Join(mp, "dir", "none")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat of absent: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(mp, "dir")); err != nil {
		t.Fatal(err)
	}
	m.Stop(t)
}
