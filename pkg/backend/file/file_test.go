package file

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

func newFileBE(t *testing.T) (backend.Backend, string) {
	t.Helper()
	dir := t.TempDir()
	be, err := backend.Open(context.Background(), "file://"+filepath.ToSlash(dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return be, dir
}

func TestFilePutGetDelete(t *testing.T) {
	be, _ := newFileBE(t)
	ctx := context.Background()

	if err := be.Put(ctx, "blocks/aa/abc.zap.age", []byte("hello")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := be.Get(ctx, "blocks/aa/abc.zap.age")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q want hello", got)
	}

	sz, err := be.Stat(ctx, "blocks/aa/abc.zap.age")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if sz != 5 {
		t.Fatalf("stat size %d want 5", sz)
	}

	if err := be.Delete(ctx, "blocks/aa/abc.zap.age"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := be.Get(ctx, "blocks/aa/abc.zap.age"); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Delete-of-absent must not error
	if err := be.Delete(ctx, "blocks/zz/missing.zap.age"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestFileList(t *testing.T) {
	be, _ := newFileBE(t)
	ctx := context.Background()

	keys := []string{
		"blocks/aa/1.zap.age",
		"blocks/aa/2.zap.age",
		"blocks/bb/3.zap.age",
	}
	for _, k := range keys {
		if err := be.Put(ctx, k, []byte("x")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	ch, errCh := be.List(ctx, "blocks/aa")
	got := map[string]bool{}
	for k := range ch {
		got[k] = true
	}
	if err := <-errCh; err != nil {
		t.Fatalf("list err: %v", err)
	}
	if !got["blocks/aa/1.zap.age"] || !got["blocks/aa/2.zap.age"] {
		t.Fatalf("missing aa keys: %v", got)
	}
	if got["blocks/bb/3.zap.age"] {
		t.Fatalf("bb key leaked into aa prefix list: %v", got)
	}
}

func TestFileTraversalBlocked(t *testing.T) {
	be, _ := newFileBE(t)
	ctx := context.Background()
	if err := be.Put(ctx, "../../../etc/evil", []byte("x")); err == nil {
		t.Fatal("path traversal should be blocked")
	}
}
