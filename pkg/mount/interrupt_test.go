//go:build fuse && linux

package mount

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// TestInterruptIsEINTR: a cancelled context, however deep the Volume wrapped it, is
// EINTR and not EIO. (Only the end of the mount cancels one: no request does, see
// TestEveryHandlerOutlivesItsRequest.)
func TestInterruptIsEINTR(t *testing.T) {
	for name, err := range map[string]error{
		"bare":    context.Canceled,
		"wrapped": fmt.Errorf("vfs: write shard 1: %w", context.Canceled),
		"twice":   fmt.Errorf("vfs: commit: %w", fmt.Errorf("vfs: read chunk 2: %w", context.Canceled)),
	} {
		if got := errno(err); got != syscall.EINTR {
			t.Errorf("%s: errno = %v, want EINTR", name, got)
		}
	}
	if got := errno(fmt.Errorf("x: %w", context.DeadlineExceeded)); got != syscall.ETIMEDOUT {
		t.Errorf("deadline: errno = %v, want ETIMEDOUT", got)
	}
	if got := errno(errors.New("the store is down")); got != syscall.EIO {
		t.Errorf("a store fault: errno = %v, want EIO", got)
	}
}

// gate holds the calls it is armed for until released, and counts them. After the
// release a call whose context has ended fails, as an HTTP request to a store does.
type gate struct {
	backend.Backend
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
	landed  chan struct{} // closed when a Head write has returned success
	passed  chan struct{} // closed when the first held call is released
	heldErr error         // what that call's context said then; read after passed
	once    sync.Once
	second  sync.Once
	freed   sync.Once
	ranges  atomic.Int64
}

func newGate(be backend.Backend) *gate {
	return &gate{Backend: be, reached: make(chan struct{}), release: make(chan struct{}), landed: make(chan struct{}), passed: make(chan struct{})}
}

// free lets every held call go; a test that fails with calls still held defers it so
// that closing its files does not wait on them.
func (g *gate) free() { g.freed.Do(func() { close(g.release) }) }

func (g *gate) hold(ctx context.Context) error {
	if g.armed.Load() {
		g.once.Do(func() { close(g.reached) })
		<-g.release
		g.second.Do(func() { g.heldErr = ctx.Err(); close(g.passed) })
	}
	return ctx.Err()
}

func (g *gate) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if key == "head" {
		if err := g.hold(ctx); err != nil {
			return "", err
		}
	}
	nv, err := g.Backend.PutIfVersion(ctx, key, data, v)
	if err == nil && key == "head" && g.armed.Load() {
		close(g.landed)
	}
	return nv, err
}

func (g *gate) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	g.ranges.Add(1)
	if err := g.hold(ctx); err != nil {
		return nil, err
	}
	return g.Backend.GetRange(ctx, key, off, n)
}

func testVolume(t *testing.T) (*gate, *vfs.Volume, vfs.Cipher, *storetest.Mem) {
	t.Helper()
	k, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	mem := storetest.NewMem()
	g := newGate(storetest.Strict{Backend: mem})
	c := vfs.Hybrid(k)
	bg := context.Background()
	if err := vfs.Create(bg, vfs.Config{Backend: g, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(bg, vfs.Config{Backend: g, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return g, v, c, mem
}

// TestFsyncWaitsForTheJournalAndNothingElse: fsync(2) answered EINTR is an error to SQLite and
// to C callers, which do not restart it, and the kernel does not restart a FUSE request that
// was answered EINTR. So an interrupt does not end the wait. What it waits for is the journal
// on this node's disk: with the store stalled on the Head, fsync returns at once, whether or
// not it was interrupted, and the data reaches the store when the store answers.
func TestFsyncWaitsForTheJournalAndNothingElse(t *testing.T) {
	g, v, c, mem := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer g.free()
	g.armed.Store(true) // the store stops answering on the first Head write
	if _, err := f.WriteAt(bg, make([]byte, 200_000), 0); err != nil {
		t.Fatal(err)
	}
	n := newSession(bg, v).node(in.ID, vfs.RootInode)
	req, interrupt := context.WithCancel(bg)
	interrupt() // the kernel's FUSE_INTERRUPT arrived before the daemon looked
	result := make(chan error, 1)
	go func() { result <- n.Fsync(req, &fuse.FsyncRequest{}) }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("fsync after an interrupt: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("fsync waits for the store")
	}
	if st := v.State(); st.Durable < st.Visible {
		t.Fatalf("fsync returned and the journal is not durable: %+v", st)
	}
	if _, err := vfs.Open(bg, vfs.Config{Backend: mem, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	g.free() // the store answers
	if err := v.Flush(bg); err != nil {
		t.Fatal(err)
	}
	ro, err := vfs.Open(bg, vfs.Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ro.Resolve(bg, "/f")
	if err != nil || got.Size != 200_000 {
		t.Fatalf("the write did not reach the store whole: %+v %v", got, err)
	}
}

// TestCloseAsksForNothing: close(2) reaches the daemon as a flush of the descriptor. It makes
// no request of the store and does not wait for one, whatever the descriptor wrote and
// whatever the store is doing; the writes are published by the Volume within its bound.
func TestCloseAsksForNothing(t *testing.T) {
	g, v, _, _ := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bg, make([]byte, 100_000), 0); err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer g.free()
	h := newSession(bg, v).handle(f)
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- h.Flush(bg, &fuse.FlushRequest{}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("close waits for the store")
	}
	select {
	case <-g.reached:
		t.Fatal("close made the store a request")
	default:
	}
}

// TestReadWaitsForItsData: a read answered EINTR reaches the caller as EIO when the
// kernel's page cache was filling pages, so an interrupt does not end a read: it waits
// for its chunk and returns the bytes. The fetch runs on the mount's own context, not
// on the request that ended.
func TestReadWaitsForItsData(t *testing.T) {
	g, v, c, _ := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 300_000)
	for i := range data {
		data[i] = byte(i * 7)
	}
	if _, err := f.WriteAt(bg, data, 0); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(bg); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// A cold view of the store, so the read has to fetch the chunk.
	cold, err := vfs.Open(bg, vfs.Config{Backend: g, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	file, err := cold.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	defer g.free()
	h := newSession(bg, cold).handle(file)

	g.armed.Store(true)
	req, interrupt := context.WithCancel(bg)
	resp := &fuse.ReadResponse{}
	result := make(chan error, 1)
	go func() { result <- h.Read(req, &fuse.ReadRequest{Size: 50_000, Offset: 0}, resp) }()
	select {
	case <-g.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the read never reached the store")
	}
	interrupt()
	select {
	case err := <-result:
		t.Fatalf("a read was answered (%v) while its chunk was still being fetched", err)
	case <-time.After(300 * time.Millisecond):
	}
	g.free()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("the read after an interrupt: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the read never returned")
	}
	if g.heldErr != nil {
		t.Fatalf("the fetch was cancelled with the request that asked for it: %v", g.heldErr)
	}
	if string(resp.Data) != string(data[:50_000]) {
		t.Fatalf("the read returned %d wrong bytes", len(resp.Data))
	}
}

// TestFsyncCallersDoNotTouchTheStore: thousands of callers of fsync against a store that has
// stopped answering are all answered by the journal, together, and the store sees nothing of
// them; the writes are in the store when it answers again.
func TestFsyncCallersDoNotTouchTheStore(t *testing.T) {
	g, v, c, mem := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer g.free()
	if _, err := f.WriteAt(bg, make([]byte, 100_000), 0); err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	n := newSession(bg, v).node(in.ID, vfs.RootInode)
	const callers = 3000
	results := make(chan error, callers)
	for range callers {
		go func() { results <- n.Fsync(bg, &fuse.FsyncRequest{}) }()
	}
	for range callers {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("a caller of fsync: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("a caller never returned")
		}
	}
	g.free()
	if err := v.Flush(bg); err != nil {
		t.Fatal(err)
	}
	ro, err := vfs.Open(bg, vfs.Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ro.Resolve(bg, "/f"); err != nil || got.Size != 100_000 {
		t.Fatalf("the write is not in the store: %+v %v", got, err)
	}
}

// TestSessionRefusesANilContext: a session without the mount's context would hand
// every node a nil one, and the first read of a chunk against a real store would
// panic. It cannot be built.
func TestSessionRefusesANilContext(t *testing.T) {
	_, v, _, _ := testVolume(t)
	defer func() {
		if recover() == nil {
			t.Fatal("a session was built without a context")
		}
	}()
	newSession(nil, v)
}

// TestLookedUpFileReadsOnTheMountContext: the kernel finds every file that is not new
// to the mount session with Lookup and reads it through the node Lookup returns. The
// store here panics on a nil context, as every real store's client does; the
// directory backend would never have noticed.
func TestLookedUpFileReadsOnTheMountContext(t *testing.T) {
	_, v, c, mem := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 300_000)
	for i := range want {
		want[i] = byte(i * 7)
	}
	if _, err := f.WriteAt(bg, want, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := v.Flush(bg); err != nil {
		t.Fatal(err)
	}
	// A fresh server over the same store: nothing cached.
	v2, err := vfs.Attach(bg, vfs.Config{Backend: storetest.Strict{Backend: mem}, Cipher: c, Round: 2, Journal: storetest.Journals(), Capacity: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	root := newSession(bg, v2).node(vfs.RootInode, vfs.RootInode)
	// What the kernel does for open("/f"): LOOKUP, OPEN, READ.
	found, err := root.Lookup(bg, "f")
	if err != nil {
		t.Fatal(err)
	}
	h, err := found.(*node).Open(bg, &fuse.OpenRequest{}, &fuse.OpenResponse{})
	if err != nil {
		t.Fatal(err)
	}
	resp := &fuse.ReadResponse{}
	if err := h.(*handle).Read(bg, &fuse.ReadRequest{Size: 50_000, Offset: 0}, resp); err != nil {
		t.Fatalf("read of a file found by Lookup: %v", err)
	}
	if string(resp.Data) != string(want[:50_000]) {
		t.Fatalf("read of a file found by Lookup returned %d wrong bytes", len(resp.Data))
	}
	dir, err := root.Mkdir(bg, &fuse.MkdirRequest{Name: "d", Mode: 0o755})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dir.(*node).Lookup(bg, "nothing"); err == nil {
		t.Fatal("a lookup in a new directory found a file that is not there")
	}
}
