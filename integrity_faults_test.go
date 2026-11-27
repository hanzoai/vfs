package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// errStoreDown is what the wrapping backends below return where a real store would return
// a 503 or a socket error: a failure to read, which is no statement about the bytes.
var errStoreDown = errors.New("store unavailable")

// memWithTarget builds a Volume whose packs are flushed at target bytes instead of the
// default 16 MiB, so a test can hold several packs without paying 16 MiB for each.
func memWithTarget(t testing.TB, target int) (*storetest.Mem, vfs.Cipher, *vfs.Volume) {
	t.Helper()
	be := storetest.NewMem()
	c := vfs.Hybrid(newKey(t))
	create := cfg(be, c, 0)
	create.PackTargetBytes = target
	if err := vfs.Create(ctx, create); err != nil {
		t.Fatal(err)
	}
	attach := cfg(be, c, 1)
	attach.PackTargetBytes = target
	v, err := vfs.Attach(ctx, attach)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	return be, c, v
}

// cancelOnPack cancels the caller at the (after+1)-th pack read, which is the only moment a
// test can stop a sequential reader without reaching into it.
type cancelOnPack struct {
	backend.Backend
	after  int
	n      int
	cancel context.CancelFunc
}

func (w *cancelOnPack) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if strings.HasPrefix(key, "pack/") {
		w.n++
		if w.n > w.after {
			w.cancel()
		}
	}
	return w.Backend.Get(ctx, key, max)
}

// countedMem counts Get per key, so a test can say that an object was read once rather than
// that some number of bytes was read.
type countedMem struct {
	backend.Backend
	mu   sync.Mutex
	gets map[string]int
}

func (m *countedMem) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	m.mu.Lock()
	m.gets[key]++
	m.mu.Unlock()
	return m.Backend.Get(ctx, key, max)
}

func (m *countedMem) count() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.gets))
	for k, v := range m.gets {
		out[k] = v
	}
	return out
}

func (m *countedMem) reset() {
	m.mu.Lock()
	m.gets = map[string]int{}
	m.mu.Unlock()
}

// failingShard fails the first read of a class once it is armed. Arming is separate from
// building the Volume because opening one reads the same objects.
type failingShard struct {
	backend.Backend
	prefix string
	armed  bool
	fired  bool
}

func (f *failingShard) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if f.armed && !f.fired && strings.HasPrefix(key, f.prefix) {
		f.fired = true
		return nil, "", errStoreDown
	}
	return f.Backend.Get(ctx, key, max)
}

// TestVerifyStopsWhenTheCallerStops: a caller that has already given up gets its own error
// back and no verdict. Before this was fixed, walk read the whole store and the report said
// the Head was intact, because the cancelled context was never looked at.
func TestVerifyStopsWhenTheCallerStops(t *testing.T) {
	be, c, v := newMem(t)
	write(t, v, vfs.RootInode, "small", content(1, 8<<10))
	write(t, v, vfs.RootInode, "big", content(2, 300<<10))
	flush(t, v)
	v.Close()

	ro := reopen(t, be, c)
	defer ro.Close()

	stopped, cancel := context.WithCancel(ctx)
	cancel()
	r, err := ro.Verify(stopped, vfs.VerifyOptions{All: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("verify under a cancelled caller: err = %v, want context.Canceled", err)
	}
	if r.Chunks != 0 || len(r.Damaged) != 0 {
		t.Fatalf("verify under a cancelled caller read %d chunks and reported %d faults, want none of either",
			r.Chunks, len(r.Damaged))
	}
}

// TestVerifyStopsBetweenPacks: a caller that cancels while Verify is reading stops at the
// next pack boundary instead of reading and unsealing every pack that is left. The pack is
// the unit that resides, so the boundary is where the read can be abandoned without
// leaving a frame half checked.
func TestVerifyStopsBetweenPacks(t *testing.T) {
	be, c, v := memWithTarget(t, 64<<10)
	for i := range 12 {
		write(t, v, vfs.RootInode, fmt.Sprintf("f%02d", i), content(uint64(i), 200<<10))
	}
	flush(t, v)
	v.Close()

	packs := len(keysWith(be, "pack/"))
	if packs < 3 {
		t.Fatalf("the test needs at least 3 packs to stop between two of them; the store holds %d", packs)
	}

	stopped, cancel := context.WithCancel(ctx)
	defer cancel()
	w := &cancelOnPack{Backend: be, after: 1, cancel: cancel}
	ro := reopen(t, w, c)
	defer ro.Close()

	r, err := ro.Verify(stopped, vfs.VerifyOptions{All: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("verify cancelled after the first pack: err = %v, want context.Canceled", err)
	}
	if r.Packs >= packs {
		t.Fatalf("verify read all %d packs after the caller cancelled at pack 2", r.Packs)
	}
	if r.Packs < 1 {
		t.Fatalf("verify read %d packs, want the first one before it stopped", r.Packs)
	}
}

// TestVerifyReportsAMalformedFrame: a pack whose bytes were substituted behind its name is
// reported with the object, the place and the length, not merely as "damaged". The pack is
// rewritten at the same length, which is how a lost write arrives.
func TestVerifyReportsAMalformedFrame(t *testing.T) {
	be, c, v := newMem(t)
	write(t, v, vfs.RootInode, "big", content(3, 400<<10))
	flush(t, v)
	v.Close()

	keys := keysWith(be, "pack/")
	if len(keys) == 0 {
		t.Fatal("no pack to damage")
	}
	key := keys[0]
	b := be.Dump()[key]
	copy(b[len(b)/3:], bytes.Repeat([]byte{0xA5}, 32))
	if err := be.Put(ctx, key, b); err != nil {
		t.Fatal(err)
	}

	ro := reopen(t, be, c)
	defer ro.Close()
	r, err := ro.Verify(ctx, vfs.VerifyOptions{All: true})
	// All asks for every fault, not for a verdict: the error still names the first, so a
	// caller that only wants a gate keeps its boolean.
	if !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("verify a damaged pack: err = %v, want ErrCorrupt", err)
	}
	if len(r.Damaged) == 0 {
		t.Fatal("a frame with substituted bytes was reported intact")
	}
	d := r.Damaged[0]
	if d.Kind != vfs.ChunkSeal && d.Kind != vfs.ChunkHash {
		t.Fatalf("fault kind %v, want %v or %v", d.Kind, vfs.ChunkSeal, vfs.ChunkHash)
	}
	if d.Key != key {
		t.Fatalf("fault names %q, the damaged pack is %q", d.Key, key)
	}
	if d.N == 0 {
		t.Fatalf("fault carries no length: %v", d)
	}
	if !strings.Contains(d.Error(), key) {
		t.Fatalf("fault %q does not name the object it is about", d.Error())
	}
}

// TestVerifyWritesNothing: the fault path is where a repair pass would write, so the
// counters are read after a store that is damaged, not after a clean one. Rewriting a byte
// with the same value counts here; comparing the dump would not see it.
func TestVerifyWritesNothing(t *testing.T) {
	be, c, v := newMem(t)
	for i := range 4 {
		write(t, v, vfs.RootInode, fmt.Sprintf("f%d", i), content(uint64(10+i), 50<<10+i))
	}
	flush(t, v)
	v.Close()

	flipEvery(t, be, "pack/")
	be.Reset()

	ro := reopen(t, be, c)
	defer ro.Close()
	r, err := ro.Verify(ctx, vfs.VerifyOptions{All: true})
	if !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("verify a damaged pack: err = %v, want ErrCorrupt", err)
	}
	if len(r.Damaged) == 0 {
		t.Fatal("every pack was flipped and verify reported nothing: the fault path was not taken")
	}
	if n := be.Total(); n != 0 {
		t.Fatalf("verify wrote %d objects while reporting faults", n)
	}
}

// TestVerifyReadsCiphertextNotPlaintext: the bytes Verify hashes and unseals are the store's,
// not the file's. If a known plaintext appears in the store the reader is reading a
// plaintext backend and the receipt proves nothing about what a real S3 would return.
func TestVerifyReadsCiphertextNotPlaintext(t *testing.T) {
	be, c, v := newMem(t)
	probe := bytes.Repeat([]byte("CIPHERTEXT-PROBE-"), 4096)
	write(t, v, vfs.RootInode, "probe", probe)
	flush(t, v)
	v.Close()

	for k, b := range be.Dump() {
		if bytes.Contains(b, probe[:64]) {
			t.Fatalf("%s holds the file's plaintext: the store is not encrypted, so verify reads nothing",
				k)
		}
	}

	ro := reopen(t, be, c)
	defer ro.Close()
	r, err := ro.Verify(ctx, vfs.VerifyOptions{All: true})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !r.OK() {
		t.Fatalf("verify reported %d faults on a whole Volume: %v", len(r.Damaged), r.Damaged[0])
	}
	if r.Bytes == 0 || r.Chunks == 0 {
		t.Fatalf("verify read %d bytes in %d chunks, want the ciphertext of the file", r.Bytes, r.Chunks)
	}
}

// TestVerifyCountsEachObjectOnce: one read per object, whatever the number of chunks that
// name it. A reader that fetched a pack per chunk would pay as many requests as the Volume
// has chunks, which on S3 is the whole cost of the pass.
func TestVerifyCountsEachObjectOnce(t *testing.T) {
	be, c, v := newMem(t)
	for i := range 6 {
		write(t, v, vfs.RootInode, fmt.Sprintf("f%d", i), content(uint64(20+i), 120<<10+7*i))
	}
	flush(t, v)
	v.Close()

	cm := &countedMem{Backend: be, gets: map[string]int{}}
	ro := reopen(t, cm, c)
	defer ro.Close()
	cm.reset() // opening the Volume reads the Head and its shards; the count starts at Verify

	r, err := ro.Verify(ctx, vfs.VerifyOptions{All: true})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	gets := cm.count()
	for k, n := range gets {
		if n != 1 {
			t.Fatalf("%s was fetched %d times; verify reads an object once", k, n)
		}
	}
	if want := r.Shards + r.Packs; len(gets) != want {
		t.Fatalf("verify fetched %d distinct objects, the report counts %d shards and %d packs",
			len(gets), r.Shards, r.Packs)
	}
}

// TestVerifyReportsAStoreFailure: a store that failed is not a damaged object and not an
// intact Head. Verify returns the store's error and keeps the report empty. Before this was
// fixed the error was dropped and the report said the Head was intact: a false green in the
// one direction where a false green is expensive.
func TestVerifyReportsAStoreFailure(t *testing.T) {
	be, c, v := newMem(t)
	write(t, v, vfs.RootInode, "small", content(5, 8<<10))
	write(t, v, vfs.RootInode, "big", content(6, 300<<10))
	flush(t, v)
	v.Close()

	f := &failingShard{Backend: be, prefix: "meta/"}
	ro, err := vfs.Open(ctx, cfg(f, c, 0))
	if err != nil {
		t.Fatalf("open with a store that has not failed yet: %v", err)
	}
	defer ro.Close()
	f.armed = true

	r, err := ro.Verify(ctx, vfs.VerifyOptions{All: true})
	if err == nil {
		t.Fatal("a store that failed to read returned a verdict: the Head was reported intact")
	}
	if !errors.Is(err, errStoreDown) {
		t.Fatalf("verify returned %v, want the store's own error", err)
	}
	if len(r.Damaged) != 0 {
		t.Fatalf("a store failure was counted as %d damaged objects", len(r.Damaged))
	}
}
