package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs/internal/storetest"
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

	if err := be.Put(ctx, "blocks/aa/abc", []byte("hello")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, _, err := be.Get(ctx, "blocks/aa/abc", backend.Unbounded)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q want hello", got)
	}

	sz, err := be.Stat(ctx, "blocks/aa/abc")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if sz != 5 {
		t.Fatalf("stat size %d want 5", sz)
	}

	if err := be.Delete(ctx, "blocks/aa/abc"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := be.Get(ctx, "blocks/aa/abc", backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Delete-of-absent must not error
	if err := be.Delete(ctx, "blocks/zz/missing"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestFileList(t *testing.T) {
	be, _ := newFileBE(t)
	ctx := context.Background()

	keys := []string{
		"blocks/aa/1",
		"blocks/aa/2",
		"blocks/bb/3",
	}
	for _, k := range keys {
		if err := be.Put(ctx, k, []byte("x")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	ch, errCh := be.List(ctx, "blocks/aa")
	got := map[string]bool{}
	for o := range ch {
		got[o.Key] = true
	}
	if err := <-errCh; err != nil {
		t.Fatalf("list err: %v", err)
	}
	if !got["blocks/aa/1"] || !got["blocks/aa/2"] {
		t.Fatalf("missing aa keys: %v", got)
	}
	if got["blocks/bb/3"] {
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

func TestFileConforms(t *testing.T) {
	storetest.Run(t, func(t *testing.T) backend.Backend {
		be, _ := newFileBE(t)
		return be
	})
}

// TestFileListHidesOnlyTheStoresOwn: the lock and the directory of in-flight temp files are
// the store's own and are not objects; nothing else is hidden, whatever its name.
func TestFileListHidesOnlyTheStoresOwn(t *testing.T) {
	be, dir := newFileBE(t)
	ctx := context.Background()
	if err := be.Put(ctx, "a/b", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a", ".vfs-b-123"), []byte("stray"), 0o644); err != nil {
		t.Fatal(err)
	}
	ch, errCh := be.List(ctx, "")
	var got []string
	for o := range ch {
		got = append(got, o.Key)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "a/.vfs-b-123" && got[1] != "a/.vfs-b-123" {
		t.Fatalf("List = %v, want [a/b a/.vfs-b-123]: a file the store did not make is listed", got)
	}
	if left, _ := os.ReadDir(filepath.Join(dir, ".tmp")); len(left) != 0 {
		t.Fatalf("a finished write left %d temp files", len(left))
	}
}

// TestFileOpenCreatesNothing: naming a directory, to read from it or by mistake, makes none.
func TestFileOpenCreatesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "typo", "deeper")
	be, err := backend.Open(context.Background(), "file://"+filepath.ToSlash(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := be.Get(context.Background(), "head", backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Get in a directory that is not there: %v", err)
	}
	if ch, errCh := be.List(context.Background(), ""); true {
		for range ch {
			t.Fatal("a listing of nothing returned an object")
		}
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Dir(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("opening and reading made %s: %v", filepath.Dir(dir), err)
	}
	if err := be.Put(context.Background(), "head", []byte("x")); err != nil {
		t.Fatalf("the first write makes the directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "head")); err != nil {
		t.Fatal(err)
	}
}

// TestFileOfADirectoryNotYetThere: a place that does not exist has the name it will have, the same
// from every spelling, symbolic links resolved where they exist.
func TestFileOfADirectoryNotYetThere(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(real)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	a, err := backend.Open(context.Background(), "file://"+filepath.ToSlash(filepath.Join(link, "x", "y")))
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.Open(context.Background(), "file://"+filepath.ToSlash(filepath.Join(real, "x", ".", "y/")))
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != b.String() || a.String() != "file://"+filepath.ToSlash(filepath.Join(resolved, "x", "y")) {
		t.Fatalf("places %q and %q", a.String(), b.String())
	}
}

// TestFileKeys: a key that would leave the directory, or names the store's own files, is
// refused and touches nothing; one that the grammar of a Volume's keys does not cover (a dot,
// a capital) is kept if the file system keeps it, as the replica package needs.
func TestFileKeys(t *testing.T) {
	be, dir := newFileBE(t)
	ctx := context.Background()
	for _, bad := range []string{"", "/a", "a/", "a//b", ".", "..", "a/../b", "../x", "a/./b", ".lock", ".tmp/x", "a\x00b"} {
		if err := be.Put(ctx, bad, []byte("x")); !errors.Is(err, backend.ErrKey) {
			t.Errorf("Put %q = %v, want ErrKey", bad, err)
		}
		if _, _, err := be.Get(ctx, bad, backend.Unbounded); !errors.Is(err, backend.ErrKey) {
			t.Errorf("Get %q = %v, want ErrKey", bad, err)
		}
	}
	for _, prefix := range []string{"/a", "a//b", "..", "../x", "a/../b", ".lock"} {
		ch, errCh := be.List(ctx, prefix)
		for range ch {
			t.Errorf("List(%q) returned an object", prefix)
		}
		if err := <-errCh; !errors.Is(err, backend.ErrKey) {
			t.Errorf("List(%q) = %v, want ErrKey", prefix, err)
		}
	}
	for _, ok := range []string{"orgs/acme/kv.db", "orgs/acme/kv.db.ver", "Upper", "a..b", ".hidden", "a b", "a%2e"} {
		if err := be.Put(ctx, ok, []byte("v:"+ok)); err != nil {
			t.Errorf("Put %q: %v", ok, err)
			continue
		}
		if got, _, err := be.Get(ctx, ok, backend.Unbounded); err != nil || string(got) != "v:"+ok {
			t.Errorf("Get %q: %q %v", ok, got, err)
		}
	}
	// a key that is a directory of another cannot be kept beside it, and says so
	if err := be.Put(ctx, "orgs/acme", []byte("x")); !errors.Is(err, backend.ErrKey) {
		t.Errorf("Put of a directory's name = %v, want ErrKey", err)
	}
	if err := be.Put(ctx, "Upper/under", []byte("x")); !errors.Is(err, backend.ErrKey) {
		t.Errorf("Put under a file = %v, want ErrKey", err)
	}
	if _, _, err := be.Get(ctx, "orgs/acme", backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
		t.Errorf("Get of a directory of keys = %v, want ErrNotFound", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "x")); err == nil {
		t.Error("a key left the directory")
	}
}
