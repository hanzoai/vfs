package vfs_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/backend"
)

// counted counts every request that reaches the store, by kind.
type counted struct {
	backend.Backend
	reads, writes, heads atomic.Int64
}

func (c *counted) Get(ctx context.Context, k string, max int64) ([]byte, backend.Version, error) {
	c.reads.Add(1)
	return c.Backend.Get(ctx, k, max)
}

func (c *counted) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	c.reads.Add(1)
	return c.Backend.GetRange(ctx, k, off, n)
}

func (c *counted) Put(ctx context.Context, k string, d []byte) error {
	c.writes.Add(1)
	return c.Backend.Put(ctx, k, d)
}

func (c *counted) PutIfVersion(ctx context.Context, k string, d []byte, v backend.Version) (backend.Version, error) {
	c.writes.Add(1)
	if k == "head" {
		c.heads.Add(1)
	}
	return c.Backend.PutIfVersion(ctx, k, d, v)
}

func (c *counted) requests() int64 { return c.reads.Load() + c.writes.Load() }

// clock is a time source a test moves by hand. Timers run in their own goroutine, in order of
// their deadlines, when the clock passes them.
type clock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*timer
}

type timer struct {
	c    *clock
	at   time.Time
	f    func()
	done bool
}

func newClock() *clock { return &clock{now: time.Unix(1_700_000_000, 0)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) AfterFunc(d time.Duration, f func()) journal.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &timer{c: c, at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

func (t *timer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := !t.done
	t.done = true
	return was
}

// Advance moves the clock by d and runs the timers it passes.
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*timer
	for _, t := range c.timers {
		if !t.done && !t.at.After(c.now) {
			t.done = true
			due = append(due, t)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].at.Before(due[j].at) })
	c.mu.Unlock()
	for _, t := range due {
		go t.f()
	}
}

// pair returns a store, a cipher and a Volume attached at round 1 over a store of the
// test's making.
func pair(t testing.TB, wrap func(backend.Backend) backend.Backend, edit func(*vfs.Config)) (*storetest.Mem, vfs.Cipher, *vfs.Volume, backend.Backend) {
	t.Helper()
	mem := storetest.NewMem()
	var be backend.Backend = mem
	if wrap != nil {
		be = wrap(mem)
	}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	conf := cfg(be, c, 1)
	if edit != nil {
		edit(&conf)
	}
	v, err := vfs.Attach(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	return mem, c, v, be
}

// Fsync is the journal's: it asks the store for nothing, and a handle's close, a read, and
// a Flush with nothing to publish ask for nothing either. Flush is the store's: after it a
// fresh view of the store has everything.
func TestFsyncAsksTheStoreForNothing(t *testing.T) {
	var cnt *counted
	mem, c, v, _ := pair(t, func(b backend.Backend) backend.Backend { cnt = &counted{Backend: b}; return cnt }, func(c *vfs.Config) { c.Bound = time.Hour })
	if _, err := v.Stat(ctx, vfs.RootInode); err != nil { // the root shard, which a cold Volume reads once
		t.Fatal(err)
	}
	before := cnt.requests()
	in, err := v.Create(ctx, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	data := content(1, 3<<20)
	for off := 0; off < len(data); off += 1 << 20 {
		if _, err := f.WriteAt(ctx, data[off:off+1<<20], int64(off)); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if st := v.State(); st.Durable < st.Visible {
		t.Fatalf("after the handle's fsync the journal is durable to %d of %d", st.Durable, st.Visible)
	}
	buf := make([]byte, len(data))
	if _, err := f.ReadAt(ctx, buf, 0); err != nil && err.Error() != "EOF" {
		t.Fatal(err)
	}
	mustEqual(t, "read back before publication", buf, data)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := v.Open(ctx, in.ID) // a handle that only reads
	if err != nil {
		t.Fatal(err)
	}
	ro.ReadAt(ctx, buf[:100], 0)
	ro.Close()
	if err := v.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n := cnt.requests() - before; n != 0 {
		t.Fatalf("writes, fsyncs, a read and two closes made %d requests of the store, want none", n)
	}
	if got := v.State(); got.Durable < got.Visible || got.Published != 0 {
		t.Fatalf("state after fsync: %+v", got)
	}
	flush(t, v)
	mustEqual(t, "after Flush", readAll(t, reopen(t, mem, c), in.ID), data)
	before = cnt.requests()
	flush(t, v)
	if n := cnt.requests() - before; n != 0 {
		t.Fatalf("a Flush with nothing to publish made %d requests", n)
	}
	if got := v.State(); got.Published != got.Visible {
		t.Fatalf("state after Flush: %+v", got)
	}
}

// What a process leaves in its journal is published by the next owner on the node: after the
// process ends without a Flush, an Attach replays the journal, publishes it under its own
// Round, and takes nothing from the file that was fsynced.
func TestProcessCrashReplays(t *testing.T) {
	mem, c, v, _ := pair(t, nil, func(c *vfs.Config) { c.Bound = time.Hour })
	want := map[string][]byte{}
	for i := range 20 {
		name := fmt.Sprintf("f%02d", i)
		want[name] = content(uint64(i), 1000+i*40_000)
		write(t, v, vfs.RootInode, name, want[name])
	}
	dir := mkdir(t, v, vfs.RootInode, "d")
	write(t, v, dir, "inner", content(77, 300<<10))
	rm, _ := v.Lookup(ctx, vfs.RootInode, "f03")
	if err := v.Remove(ctx, vfs.RootInode, "f03"); err != nil {
		t.Fatal(err)
	}
	_ = rm
	delete(want, "f03")
	if err := v.Truncate(ctx, mustLookup(t, v, "f05").ID, 500); err != nil {
		t.Fatal(err)
	}
	want["f05"] = want["f05"][:500]
	if err := v.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil { // the process ends: nothing was published
		t.Fatal(err)
	}
	if ro := reopen(t, mem, c); len(snapshot(t, ro)) != 0 {
		t.Fatalf("the store has %d paths before anyone published", len(snapshot(t, ro)))
	}

	w, err := vfs.Attach(ctx, cfg(mem, c, 2))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	rec := w.Recovery()
	if rec.Lost || rec.Replayed == 0 {
		t.Fatalf("recovery: %+v", rec)
	}
	check := func(what string, r *vfs.Volume) {
		t.Helper()
		for name, data := range want {
			in, err := r.Lookup(ctx, vfs.RootInode, name)
			if err != nil {
				t.Fatalf("%s: %s: %v", what, name, err)
			}
			mustEqual(t, what+" "+name, readAll(t, r, in.ID), data)
		}
		if _, err := r.Lookup(ctx, vfs.RootInode, "f03"); err == nil {
			t.Fatalf("%s: a removed file is back", what)
		}
		in, err := r.Resolve(ctx, "/d/inner")
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		mustEqual(t, what+" inner", readAll(t, r, in.ID), content(77, 300<<10))
	}
	check("the replayed Volume", w)
	check("the store after the replay", reopen(t, mem, c)) // Attach publishes what it replays
}

func mustLookup(t testing.TB, v *vfs.Volume, name string) vfs.Inode {
	t.Helper()
	in, err := v.Lookup(ctx, vfs.RootInode, name)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

// A replay that does not finish (the store refuses its Head) leaves the journal as it was, and
// the next Attach replays the same changes again: once, each.
func TestReplayTwice(t *testing.T) {
	var refuse atomic.Bool
	mem, c, v, _ := pair(t, func(b backend.Backend) backend.Backend {
		return &storetest.Hook{Backend: b, Before: func(op, key string) error {
			if refuse.Load() && key == "head" {
				return errors.New("the store is down")
			}
			return nil
		}}
	}, func(c *vfs.Config) { c.Bound = time.Hour })
	data := content(5, 700<<10)
	write(t, v, vfs.RootInode, "a", data)
	write(t, v, vfs.RootInode, "b", []byte("small"))
	v.Sync(ctx)
	v.Close()

	refuse.Store(true)
	hook := &storetest.Hook{Backend: mem, Before: func(op, key string) error {
		if refuse.Load() && key == "head" && op == "putver" {
			return errors.New("the store is down")
		}
		return nil
	}}
	if _, err := vfs.Attach(ctx, cfg(hook, c, 2)); err == nil {
		t.Fatal("an Attach whose replay could not be published succeeded")
	}
	refuse.Store(false)
	w, err := vfs.Attach(ctx, cfg(hook, c, 3))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if rec := w.Recovery(); rec.Lost && rec.Replayed == 0 {
		t.Fatalf("the second Attach found nothing to replay: %+v", rec)
	}
	got := snapshot(t, reopen(t, mem, c))
	if len(got) != 2 {
		t.Fatalf("the store holds %v, want a and b once each", got)
	}
	mustEqual(t, "a", readAll(t, w, mustLookup(t, w, "a").ID), data)
	mustEqual(t, "b", readAll(t, w, mustLookup(t, w, "b").ID), []byte("small"))
}

// A takeover on another node cannot see the old owner's journal: the new owner is told that
// an unpublished tail may be missing, has the last Head, and the old owner is fenced.
func TestTakeoverOnAnotherNodeSaysWhatItCannotKnow(t *testing.T) {
	mem, c, a, _ := pair(t, nil, nil)
	write(t, a, vfs.RootInode, "published", []byte("p"))
	flush(t, a)
	write(t, a, vfs.RootInode, "acknowledged", []byte("only in a's journal"))
	if err := a.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	other := cfg(mem, c, 2)
	other.Journal = filepath.Join(t.TempDir(), "elsewhere")
	if _, err := os.Stat(other.Journal); err == nil || !os.IsNotExist(err) {
		other.Journal = t.TempDir() + "/elsewhere"
	}
	b, err := vfs.Attach(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if rec := b.Recovery(); !rec.Lost || rec.Why == "" {
		t.Fatalf("a takeover that cannot see the old journal reports %+v", rec)
	}
	if _, err := b.Resolve(ctx, "/published"); err != nil {
		t.Fatalf("the last Head is not the new owner's: %v", err)
	}
	if _, err := b.Resolve(ctx, "/acknowledged"); err == nil {
		t.Fatal("the new owner has what only the old node's journal holds")
	}
	if err := a.Flush(ctx); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("the old owner's Flush = %v, want ErrStaleRound", err)
	}
	if _, err := reopen(t, mem, c).Resolve(ctx, "/acknowledged"); err == nil {
		t.Fatal("the fenced owner's change reached the store")
	}
}

// A takeover that happens while the old owner publishes: the old owner's Head write is held,
// the new owner claims, and the write is refused. The new owner has the last Head the store
// acknowledged, and the generation that was in flight is nowhere.
func TestTakeoverDuringPublication(t *testing.T) {
	st := &stall{on: func(string) bool { return false }, entered: make(chan struct{}, 1), release: make(chan struct{})}
	mem, c, a, _ := pair(t, func(b backend.Backend) backend.Backend { st.Backend = b; return st }, nil)
	write(t, a, vfs.RootInode, "before", []byte("b"))
	flush(t, a)
	write(t, a, vfs.RootInode, "inflight", []byte("i"))
	a.Sync(ctx)
	st.on = func(k string) bool { return k == "head" }
	done := make(chan error, 1)
	go func() { done <- a.Flush(ctx) }()
	<-st.entered
	other := cfg(mem, c, 2)
	other.Journal = t.TempDir() + "/elsewhere"
	b, err := vfs.Attach(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	st.on = func(string) bool { return false }
	close(st.release)
	if err := <-done; !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("the old owner's publication = %v, want ErrStaleRound", err)
	}
	if _, err := b.Resolve(ctx, "/before"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Resolve(ctx, "/inflight"); err == nil {
		t.Fatal("the new owner has a generation that was never acknowledged")
	}
	if _, err := reopen(t, mem, c).Resolve(ctx, "/inflight"); err == nil {
		t.Fatal("the store has a generation that the fenced owner never committed")
	}
	if err := a.Sync(ctx); err != nil {
		t.Fatalf("a fenced owner's journal still takes fsync: %v", err)
	}
}

// The bound is kept from each change's own acknowledgement: with a clock the test moves, a
// writer that never stops is published in time, every time, and no change waits longer than
// the bound however many follow it.
func TestBoundIsKeptFromEachAcknowledgement(t *testing.T) {
	clk := newClock()
	var heads atomic.Int64
	published := make(chan struct{}, 1000)
	mem, c, v, _ := pair(t, func(b backend.Backend) backend.Backend {
		return &storetest.Hook{Backend: b, After: func(op, key string) error {
			if key == "head" && op == "putver" {
				heads.Add(1)
				select {
				case published <- struct{}{}:
				default:
				}
			}
			return nil
		}}
	}, func(c *vfs.Config) { c.Clock, c.Bound = clk, 4*time.Second })
	started := time.Time{}
	acked := map[string]time.Time{}
	var order []string
	for step := range 120 {
		name := fmt.Sprintf("w%03d", step)
		write(t, v, vfs.RootInode, name, []byte(name))
		now := clk.Now()
		if started.IsZero() {
			started = now
		}
		acked[name] = now
		order = append(order, name)
		clk.Advance(250 * time.Millisecond)
		// What the bound promises: a change acknowledged more than the bound ago is in the store.
		// The publisher runs on its own goroutine; give it the moment it needs to finish what the
		// deadline asked of it, and no more.
		deadline := time.Now().Add(5 * time.Second)
		for {
			seen := snapshot(t, reopen(t, mem, c))
			late := ""
			for _, n := range order {
				if _, ok := seen["/"+n]; !ok && clk.Now().Sub(acked[n]) > 4*time.Second {
					late = n
					break
				}
			}
			if late == "" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("step %d: %s was acknowledged %v ago on a bound of 4s and the store does not have it (%d Head writes)", step, late, clk.Now().Sub(acked[late]), heads.Load())
			}
			select {
			case <-published:
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	if n := heads.Load(); n < 5 || n > 120 {
		t.Fatalf("%d Head writes for 30 s of changes on a 4 s bound", n)
	}
	t.Logf("%d Head writes for 120 changes over 30 s of the test's clock", heads.Load())
}

// Many writers, many Flushes and Syncs, in random order: a Flush that returns means every change
// that completed before it was called is in a view of the store; no view sees a change that has
// not been made; and the views only grow. The generation a Flush waits for is never one before
// the changes it covers.
func TestGenerationOrdering(t *testing.T) {
	mem, c, v, _ := pair(t, nil, nil)
	const workers, rounds = 8, 40
	var (
		mu    sync.Mutex
		done  = map[string]bool{} // changes that returned
		views []map[string]string
	)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 99))
			for i := range rounds {
				name := fmt.Sprintf("w%d-%03d", w, i)
				data := content(uint64(w*1000+i), 200+rng.IntN(300_000))
				in, err := v.Create(ctx, vfs.RootInode, name, 0o644)
				if err != nil {
					t.Error(err)
					return
				}
				f, err := v.Open(ctx, in.ID)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := f.WriteAt(ctx, data, 0); err != nil {
					t.Error(err)
					return
				}
				f.Close()
				mu.Lock()
				done[name] = true
				mu.Unlock()
				switch rng.IntN(4) {
				case 0:
					mu.Lock()
					before := make([]string, 0, len(done))
					for n := range done {
						before = append(before, n)
					}
					mu.Unlock()
					if err := v.Flush(ctx); err != nil {
						t.Error(err)
						return
					}
					seen := snapshot(t, reopen(t, mem, c))
					for _, n := range before {
						if _, ok := seen["/"+n]; !ok {
							t.Errorf("a Flush returned and %s, completed before it began, is not in the store", n)
							return
						}
					}
					mu.Lock()
					views = append(views, seen)
					mu.Unlock()
				case 1:
					if err := v.Sync(ctx); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	flush(t, v)
	final := snapshot(t, reopen(t, mem, c))
	if len(final) != workers*rounds {
		t.Fatalf("the store has %d files, want %d", len(final), workers*rounds)
	}
	// A file is created and then written: a view between the two shows it empty, and no view shows
	// it any other way than empty or whole.
	empty := blake3.Sum256(nil)
	for i, view := range views {
		for p, h := range view {
			if final[p] != h && !strings.HasPrefix(h, hex.EncodeToString(empty[:8])+" ") {
				t.Fatalf("view %d had %s as %s, the end has %s", i, p, h, final[p])
			}
		}
	}
}

// When the store does not answer, the journal takes changes up to its capacity, says so
// (State), and then refuses them with ErrFull instead of waiting or dropping them; when the
// store answers again, everything accepted drains in order and nothing is lost.
func TestBackpressureAndRecovery(t *testing.T) {
	var down atomic.Bool
	mem, c, v, _ := pair(t, func(b backend.Backend) backend.Backend {
		return &storetest.Hook{Backend: b, Before: func(op, key string) error {
			if down.Load() && (op == "put" || op == "putver") {
				return errors.New("the store is down")
			}
			return nil
		}}
	}, func(c *vfs.Config) { c.JournalCapacity = 8 << 20 })
	in, err := v.Create(ctx, vfs.RootInode, "log", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	down.Store(true)
	var model []byte
	var failure error
	block := content(3, 128<<10)
	for i := 0; i < 400; i++ {
		if _, failure = f.WriteAt(ctx, block, int64(len(model))); failure != nil {
			break
		}
		model = append(model, block...)
	}
	if !errors.Is(failure, vfs.ErrFull) {
		t.Fatalf("after %d KiB with the store down a write = %v, want ErrFull", len(model)>>10, failure)
	}
	if len(model) == 0 {
		t.Fatal("not one write was taken")
	}
	if st := v.State(); !st.Degraded || st.Backlog == 0 {
		t.Fatalf("State with the store down and the journal full: %+v", st)
	}
	if err := f.Sync(ctx); err != nil {
		t.Fatalf("fsync of what was accepted: %v", err)
	}
	down.Store(false)
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush once the store answers: %v", err)
	}
	mustEqual(t, "what was accepted", readAll(t, reopen(t, mem, c), in.ID), model)
	if _, err := f.WriteAt(ctx, block, int64(len(model))); err != nil {
		t.Fatalf("a write after the backlog drained: %v", err)
	}
	if st := v.State(); st.Err != nil {
		t.Fatalf("the journal failed: %+v", st)
	}
}

// A writer that outruns publication waits for it when memory is full, and is refused when
// the store is not taking the writes.
func TestMemoryBoundWaitsThenRefuses(t *testing.T) {
	var down atomic.Bool
	_, _, v, _ := pair(t, func(b backend.Backend) backend.Backend {
		return &storetest.Hook{Backend: b, Before: func(op, key string) error {
			if down.Load() && (op == "put" || op == "putver") {
				return errors.New("the store is down")
			}
			return nil
		}}
	}, func(c *vfs.Config) { c.JournalCapacity, c.Dirty = 256<<20, 1<<20 })
	in, err := v.Create(ctx, vfs.RootInode, "big", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := v.Open(ctx, in.ID)
	defer f.Close()
	block := content(9, 256<<10)
	for i := range 40 { // 10 MiB through a 1 MiB bound, the store answering: it waits, and gets there
		if _, err := f.WriteAt(ctx, block, int64(i*len(block))); err != nil {
			t.Fatalf("write %d with a healthy store: %v", i, err)
		}
	}
	down.Store(true)
	var failure error
	for i := 40; i < 200 && failure == nil; i++ {
		_, failure = f.WriteAt(ctx, block, int64(i*len(block)))
	}
	if !errors.Is(failure, vfs.ErrFull) {
		t.Fatalf("a write with memory full and the store down = %v, want ErrFull", failure)
	}
	down.Store(false)
}

// While a generation is being published, what it holds is read, listed and measured as before,
// and writes made since are seen over it: the three levels of a file's content (settled, frozen
// for the generation, written since) read as one.
func TestReadsSeeAGenerationInFlight(t *testing.T) {
	st := &stall{on: func(string) bool { return false }, entered: make(chan struct{}, 1), release: make(chan struct{})}
	mem, c, v, _ := pair(t, func(b backend.Backend) backend.Backend { st.Backend = b; return st }, nil)
	base := content(1, 300_000)
	in := write(t, v, vfs.RootInode, "f", base)
	flush(t, v) // settled
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	model := append([]byte(nil), base...)
	put := func(p []byte, off int) {
		t.Helper()
		if _, err := f.WriteAt(ctx, p, int64(off)); err != nil {
			t.Fatal(err)
		}
		if off+len(p) > len(model) {
			model = append(model, make([]byte, off+len(p)-len(model))...)
		}
		copy(model[off:], p)
	}
	put(content(2, 50_000), 100_000) // frozen when the generation is cut
	put([]byte("tail"), 400_000)     // extends the file, past a hole
	st.on = func(k string) bool { return k == "head" }
	done := make(chan error, 1)
	go func() { done <- v.Flush(ctx) }()
	<-st.entered // the generation is cut and held at the Head
	check := func(what string) {
		t.Helper()
		got, err := v.Stat(ctx, in.ID)
		if err != nil || got.Size != uint64(len(model)) {
			t.Fatalf("%s: size %d, %v; want %d", what, got.Size, err, len(model))
		}
		mustEqual(t, what, readAll(t, v, in.ID), model)
	}
	check("frozen over settled")
	put(content(3, 1000), 120_000) // over the frozen layer, in the live one
	put([]byte("x"), 10)
	check("live over frozen")
	if err := f.Truncate(ctx, 130_000); err != nil {
		t.Fatal(err)
	}
	model = model[:130_000]
	check("truncated below the frozen layer's end")
	put([]byte("end"), 135_000) // extended again: the gap is zeros, not the frozen bytes
	check("extended over the truncation")
	st.on = func(string) bool { return false }
	close(st.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	check("after the generation was published")
	flush(t, v)
	mustEqual(t, "in the store", readAll(t, reopen(t, mem, c), in.ID), model)
}

// The journal is told the store has a sequence only after the Head carrying it committed.
func TestJournalIsToldAfterTheHead(t *testing.T) {
	var refuse atomic.Bool
	_, _, v, _ := pair(t, func(b backend.Backend) backend.Backend {
		return &storetest.Hook{Backend: b, Before: func(op, key string) error {
			if refuse.Load() && key == "head" {
				return errors.New("the store refuses the Head")
			}
			return nil
		}}
	}, nil)
	write(t, v, vfs.RootInode, "a", []byte("a"))
	refuse.Store(true)
	if err := v.Flush(ctx); err == nil {
		t.Fatal("Flush succeeded with the Head refused")
	}
	if st := v.State(); st.Published != 0 || st.Publishing == nil || !st.Degraded {
		t.Fatalf("state after a refused Head: %+v", st)
	}
	refuse.Store(false)
	flush(t, v)
	if st := v.State(); st.Published != st.Visible || st.Publishing != nil {
		t.Fatalf("state after the Head: %+v", st)
	}
}

// A journal much smaller than what is written does not fill while the store answers: the
// Volume publishes when a quarter of the journal is taken, and the writer is not refused.
func TestJournalFillPublishesEarly(t *testing.T) {
	mem, c, v, _ := pair(t, nil, func(c *vfs.Config) { c.JournalCapacity = 8 << 20 })
	in, err := v.Create(ctx, vfs.RootInode, "big", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	block := content(4, 256<<10)
	const n = 160 // 40 MiB through an 8 MiB journal
	for i := range n {
		if _, err := f.WriteAt(ctx, block, int64(i*len(block))); err != nil {
			t.Fatalf("write %d: %v (state %+v, stats %+v)", i, err, v.State(), v.Stats())
		}
	}
	flush(t, v)
	if got := readAll(t, reopen(t, mem, c), in.ID); len(got) != n*len(block) {
		t.Fatalf("the store has %d bytes, want %d", len(got), n*len(block))
	}
}

// One Budget bounds the memory of every Volume given it: sixty-four Volumes whose store has
// stopped answering take writes until the Budget is spent, and then wait; the memory they hold is
// the Budget and one change of each writer, not sixty-four times a Volume's share; reads of what is
// held still run; and when the store answers every writer finishes and the memory is given back.
func TestOneBudgetBoundsManyVolumes(t *testing.T) {
	const volumes, block, limit = 64, 64 << 10, 8 << 20
	budget := vfs.NewBudget(limit)
	release := make(chan struct{})
	var armed atomic.Bool // the store answers while the Volumes are made, and then stops
	var ents []*stall
	var vols []*vfs.Volume
	var files []*vfs.File
	for range volumes {
		st := &stall{on: func(string) bool { return armed.Load() }, entered: make(chan struct{}, 1), release: release}
		_, _, v, _ := pair(t, func(b backend.Backend) backend.Backend { st.Backend = b; return st }, func(c *vfs.Config) {
			c.Budget, c.JournalCapacity = budget, 16<<20
		})
		// Created before the store stops answering: the stall holds writes, and a create is not one.
		in, err := v.Create(ctx, vfs.RootInode, "f", 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f, err := v.Open(ctx, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		ents, vols, files = append(ents, st), append(vols, v), append(files, f)
	}
	armed.Store(true)
	var wg sync.WaitGroup
	var written atomic.Int64
	for i, f := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for off := range 32 { // 2 MiB each: 128 MiB in all against a Budget of 8 MiB
				if _, err := f.WriteAt(ctx, content(uint64(i), block), int64(off*block)); err != nil {
					t.Errorf("volume %d write %d: %v", i, off, err)
					return
				}
				written.Add(block)
			}
		}()
	}
	// The writers stop, one after another, at the Budget. Wait for the count to stand still.
	last, still := int64(-1), 0
	for deadline := time.Now().Add(20 * time.Second); still < 20 && time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if w := written.Load(); w == last {
			still++
		} else {
			last, still = w, 0
		}
	}
	if written.Load() >= volumes*32*block {
		t.Fatalf("the writers finished (%d MiB) with the store stopped: nothing bounded them", written.Load()>>20)
	}
	// What the Budget admits is bounded by it, and one change of each writer that was admitted at the
	// same moment (a Volume that holds nothing is always let in); the packs made from it live beside it
	// until the store has them, and a Volume that has one on its way makes no other while the Budget is
	// spent, so the sum is under twice the two.
	if used := budget.Used(); used > 2*(limit+volumes*block) {
		t.Fatalf("the Volumes hold %d MiB against a Budget of %d MiB", used>>20, limit>>20)
	}
	t.Logf("store stopped: %d MiB written, %d MiB held, Budget %d MiB", written.Load()>>20, budget.Used()>>20, limit>>20)

	// Reads and stats of what is held do not wait for the store or for the Budget.
	for i := range 4 {
		done := make(chan error, 1)
		go func() {
			if _, err := vols[i].Stat(ctx, vfs.RootInode); err != nil {
				done <- err
				return
			}
			buf := make([]byte, block)
			_, err := files[i].ReadAt(ctx, buf, 0)
			if err == nil && !bytes.Equal(buf, content(uint64(i), block)) {
				err = errors.New("the read returned other bytes")
			}
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("read of volume %d: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("a read of volume %d waits behind a stalled store and a spent Budget", i)
		}
	}

	close(release)
	wg.Wait()
	for i, v := range vols {
		if err := v.Flush(ctx); err != nil {
			t.Fatalf("volume %d: %v", i, err)
		}
	}
	if used := budget.Used(); used != 0 {
		t.Fatalf("the Volumes hold %d bytes of the Budget after everything was published", used)
	}
	_ = ents
}

// holdSync is a journal file layer whose syncs wait while it is held.
type holdSync struct {
	journal.Disk
	held atomic.Bool
	gate chan struct{}
}

func (d *holdSync) Create(name string, size int64) (journal.File, error) {
	f, err := d.Disk.Create(name, size)
	return &heldFile{File: f, d: d}, err
}

func (d *holdSync) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	return &heldFile{File: f, d: d}, err
}

type heldFile struct {
	journal.File
	d *holdSync
}

func (f *heldFile) Sync() error {
	if f.d.held.Load() {
		<-f.d.gate
	}
	return f.File.Sync()
}

// The Head never names a journal sequence that is not durable on this node: with the journal's disk
// held in its sync, a publication gets as far as the Head and no further, and goes on when it is let go.
func TestHeadWaitsForTheJournalToBeDurable(t *testing.T) {
	var heads atomic.Int64
	disk := &holdSync{gate: make(chan struct{})}
	_, _, v, _ := pair(t, func(b backend.Backend) backend.Backend {
		return &storetest.Hook{Backend: b, Before: func(op, key string) error {
			if key == "head" && op == "putver" {
				heads.Add(1)
			}
			return nil
		}}
	}, func(c *vfs.Config) {
		c.Disk = func(dir string) (journal.Disk, error) {
			d, err := journal.Files(dir)
			disk.Disk = d
			return disk, err
		}
	})
	write(t, v, vfs.RootInode, "f", []byte("data"))
	before := heads.Load()
	disk.held.Store(true)
	done := make(chan error, 1)
	go func() { done <- v.Flush(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("Flush returned (%v) while the journal's sync was held", err)
	case <-time.After(500 * time.Millisecond):
	}
	if n := heads.Load() - before; n != 0 {
		t.Fatalf("%d Head writes while the journal's disk was held: the Head names a sequence the node does not have", n)
	}
	disk.held.Store(false)
	close(disk.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := heads.Load() - before; n != 1 {
		t.Fatalf("%d Head writes after the journal was let go", n)
	}
}

// A publication that the store refuses because the Volume was fenced is refused, not taken for a
// commit that landed: the Head the successor carried forward is not this Volume's tree.
func TestAFencedPublicationIsNotRecognisedAsLanded(t *testing.T) {
	mem, c, a, _ := pair(t, nil, nil)
	write(t, a, vfs.RootInode, "before", []byte("b"))
	flush(t, a)
	other := cfg(mem, c, 2)
	other.Journal = t.TempDir() + "/elsewhere"
	b, err := vfs.Attach(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	write(t, a, vfs.RootInode, "after", []byte("a"))
	if err := a.Flush(ctx); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("the fenced Volume's first publication = %v, want ErrStaleRound", err)
	}
	if _, err := reopen(t, mem, c).Resolve(ctx, "/after"); err == nil {
		t.Fatal("the fenced Volume's change is in the store")
	}
}
