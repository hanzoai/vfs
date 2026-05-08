package vfs_test

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/vfs"
)

func newFS(t *testing.T) *vfs.FS {
	t.Helper()
	dir := t.TempDir()
	be, err := backend.Open(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("backend.Open: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity: %v", err)
	}
	c, err := vfs.NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
	if err != nil {
		t.Fatalf("NewCrypto: %v", err)
	}
	v, err := vfs.New(vfs.Config{Backend: be, Crypto: c})
	if err != nil {
		t.Fatalf("vfs.New: %v", err)
	}
	fs, err := vfs.NewFS(context.Background(), v)
	if err != nil {
		t.Fatalf("NewFS: %v", err)
	}
	return fs
}

func TestFSCreateAndStat(t *testing.T) {
	fs := newFS(t)
	in, err := fs.Create("/hello.txt", 0o644)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if in.Size != 0 {
		t.Fatalf("new file size = %d, want 0", in.Size)
	}
	got, err := fs.Lookup("/hello.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got.ID != in.ID {
		t.Fatalf("Lookup id mismatch: got %d want %d", got.ID, in.ID)
	}
}

func TestFSDirOps(t *testing.T) {
	fs := newFS(t)
	if _, err := fs.Mkdir("/sub", 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if _, err := fs.Create("/sub/a.txt", 0o644); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := fs.Create("/sub/b.txt", 0o644); err != nil {
		t.Fatalf("Create: %v", err)
	}
	entries, err := fs.ReadDir("/sub")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("ReadDir count: got %d want 2", len(entries))
	}
	if err := fs.Remove("/sub/a.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := fs.Lookup("/sub/a.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected ErrNotExist after remove, got %v", err)
	}
}

func TestFileWriteRead(t *testing.T) {
	fs := newFS(t)
	ctx := context.Background()
	if _, err := fs.Create("/data.bin", 0o644); err != nil {
		t.Fatalf("Create: %v", err)
	}
	f, err := fs.Open(ctx, "/data.bin")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	payload := []byte(strings.Repeat("abcdefgh", 100)) // 800 bytes
	n, err := f.WriteAt(payload, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("WriteAt n=%d, want %d", n, len(payload))
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Read back
	buf := make([]byte, len(payload))
	got, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
	if got != len(payload) {
		t.Fatalf("ReadAt n=%d, want %d", got, len(payload))
	}
	if string(buf) != string(payload) {
		t.Fatalf("readback mismatch")
	}
}

func TestFileMultiBlock(t *testing.T) {
	fs := newFS(t)
	ctx := context.Background()
	if _, err := fs.Create("/big.bin", 0o644); err != nil {
		t.Fatalf("Create: %v", err)
	}
	f, err := fs.Open(ctx, "/big.bin")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// 16 KiB = 4 full blocks
	payload := make([]byte, 4*vfs.BlockSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := f.WriteAt(payload, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	stat, _ := f.Stat()
	if stat.Size != uint64(len(payload)) {
		t.Fatalf("size=%d want %d", stat.Size, len(payload))
	}
	if len(stat.Blocks) != 4 {
		t.Fatalf("blocks=%d want 4", len(stat.Blocks))
	}

	// Read back full
	got := make([]byte, len(payload))
	if _, err := f.ReadAt(got, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestFilePartialBlockRMW(t *testing.T) {
	fs := newFS(t)
	ctx := context.Background()
	if _, err := fs.Create("/rmw.bin", 0o644); err != nil {
		t.Fatalf("Create: %v", err)
	}
	f, err := fs.Open(ctx, "/rmw.bin")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Write a full block of 'A'
	full := make([]byte, vfs.BlockSize)
	for i := range full {
		full[i] = 'A'
	}
	if _, err := f.WriteAt(full, 0); err != nil {
		t.Fatalf("WriteAt full: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync 1: %v", err)
	}

	// Partial overwrite at offset 100, 50 bytes of 'B'
	bs := []byte(strings.Repeat("B", 50))
	if _, err := f.WriteAt(bs, 100); err != nil {
		t.Fatalf("WriteAt partial: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync 2: %v", err)
	}

	// Read back full block
	got := make([]byte, vfs.BlockSize)
	if _, err := f.ReadAt(got, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
	for i := 0; i < 100; i++ {
		if got[i] != 'A' {
			t.Fatalf("byte %d: want A, got %c", i, got[i])
		}
	}
	for i := 100; i < 150; i++ {
		if got[i] != 'B' {
			t.Fatalf("byte %d: want B, got %c", i, got[i])
		}
	}
	for i := 150; i < vfs.BlockSize; i++ {
		if got[i] != 'A' {
			t.Fatalf("byte %d: want A, got %c", i, got[i])
		}
	}
}

func TestFSPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age: %v", err)
	}

	open := func() *vfs.FS {
		be, err := backend.Open(context.Background(), "file://"+dir)
		if err != nil {
			t.Fatalf("backend.Open: %v", err)
		}
		c, err := vfs.NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
		if err != nil {
			t.Fatalf("NewCrypto: %v", err)
		}
		v, err := vfs.New(vfs.Config{Backend: be, Crypto: c})
		if err != nil {
			t.Fatalf("vfs.New: %v", err)
		}
		fs, err := vfs.NewFS(context.Background(), v)
		if err != nil {
			t.Fatalf("NewFS: %v", err)
		}
		return fs
	}

	// First mount: create a file with content
	{
		fs := open()
		if _, err := fs.Create("/persist.txt", 0o644); err != nil {
			t.Fatalf("Create: %v", err)
		}
		f, _ := fs.Open(context.Background(), "/persist.txt")
		if _, err := f.WriteAt([]byte("hello after remount"), 0); err != nil {
			t.Fatalf("WriteAt: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Second mount: file should still be there
	{
		fs := open()
		f, err := fs.Open(context.Background(), "/persist.txt")
		if err != nil {
			t.Fatalf("Open after remount: %v", err)
		}
		buf := make([]byte, 19)
		n, err := f.ReadAt(buf, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("ReadAt: %v", err)
		}
		if string(buf[:n]) != "hello after remount" {
			t.Fatalf("got %q want %q", buf[:n], "hello after remount")
		}
	}
}
