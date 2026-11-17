package vfs_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// stall blocks a mutating call on key until ctx is done or release is closed, as a store
// that accepts the request and does not answer.
type stall struct {
	backend.Backend
	on      func(key string) bool
	entered chan struct{}
	release chan struct{}
}

func (s *stall) block(ctx context.Context, key string) error {
	if s.on(key) {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.release:
		}
	}
	return nil
}
func (s *stall) Put(ctx context.Context, k string, d []byte) error {
	if err := s.block(ctx, k); err != nil {
		return err
	}
	return s.Backend.Put(ctx, k, d)
}
func (s *stall) PutIfVersion(ctx context.Context, k string, d []byte, v backend.Version) (backend.Version, error) {
	if err := s.block(ctx, k); err != nil {
		return "", err
	}
	return s.Backend.PutIfVersion(ctx, k, d, v)
}

// TestStalledCommitStallsNobody: a publication blocked on the store holds no lock any
// other caller needs. A stat, a lookup, a create, a write and an fsync complete while it is
// stalled; the Flush that asked for it returns when its own context ends; and once the
// store answers, the next Flush commits everything.
func TestStalledCommitStallsNobody(t *testing.T) {
	mem := storetest.NewMem()
	st := &stall{Backend: mem, on: func(string) bool { return false }, entered: make(chan struct{}, 1), release: make(chan struct{})}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(st, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	in := write(t, v, vfs.RootInode, "f", []byte("hello"))
	st.on = func(k string) bool { return k == "head" }
	commitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- v.Flush(commitCtx) }()
	<-st.entered

	within := func(what string, fn func() error) {
		t.Helper()
		res := make(chan error, 1)
		go func() { res <- fn() }()
		select {
		case err := <-res:
			if err != nil {
				t.Fatalf("%s behind a stalled commit: %v", what, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s waits behind the stalled commit", what)
		}
	}
	within("stat", func() error { _, err := v.Stat(ctx, in.ID); return err })
	within("lookup", func() error { _, err := v.Lookup(ctx, vfs.RootInode, "f"); return err })
	within("readdir", func() error { _, err := v.ReadDir(ctx, vfs.RootInode); return err })
	within("create and write", func() error {
		g, err := v.Create(ctx, vfs.RootInode, "g", 0o644)
		if err != nil {
			return err
		}
		f, err := v.Open(ctx, g.ID)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := f.WriteAt(ctx, []byte("world"), 0); err != nil {
			return err
		}
		return f.Sync(ctx)
	})
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the stalled Flush = %v, want its context's deadline", err)
	}
	st.on = func(string) bool { return false }
	close(st.release)
	flush(t, v)
	got := reopen(t, mem, c)
	mustEqual(t, "after the stall", readAll(t, got, in.ID), []byte("hello"))
	g, err := got.Resolve(ctx, "/g")
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, "written during the stall", readAll(t, got, g.ID), []byte("world"))
}

// TestNoGoroutineLeak: opening, writing, flushing, failing and closing Volumes
// leaves no goroutine behind.
func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := range 20 {
		mem := storetest.NewMem()
		c := vfsAge(t)
		if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
			t.Fatal(err)
		}
		v, _ := vfs.Attach(ctx, cfg(mem, c, 1))
		write(t, v, vfs.RootInode, "f", content(uint64(i), 300<<10))
		flush(t, v)
		ro := reopen(t, mem, c)
		_, _ = ro.ReadDir(ctx, vfs.RootInode)
		_ = ro.Close()
		_ = v.Close()
	}
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	if after := runtime.NumGoroutine(); after > before+2 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutines %d -> %d\n%s", before, after, buf[:n])
	}
}
