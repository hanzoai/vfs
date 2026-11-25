package vfs_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// TestSameRoundRace: many claimants at the SAME round on every store. The
// fence is strict, so exactly one may own the round.
func TestSameRoundRace(t *testing.T) {
	for _, st := range stores(t) {
		t.Run(st.name, func(t *testing.T) {
			c := vfsAge(t)
			if err := vfs.Create(ctx, cfg(st.client(t), c, 0)); err != nil {
				t.Fatal(err)
			}
			for round := uint64(3); round < 40; round += 3 {
				var won atomic.Int32
				var wg sync.WaitGroup
				start := make(chan struct{})
				for range 16 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						_, err := vfs.Attach(ctx, cfg(st.client(t), c, round))
						switch {
						case err == nil:
							won.Add(1)
						case !errors.Is(err, vfs.ErrStaleRound):
							t.Errorf("round %d: %v", round, err)
						}
					}()
				}
				close(start)
				wg.Wait()
				if won.Load() != 1 {
					t.Fatalf("round %d: %d claimants own it, want 1", round, won.Load())
				}
			}
		})
	}
}

// TestLostAckThenTakeoverIsNotReportedLost: the Head write lands, its reply is
// lost, a successor attaches and carries the landed Head forward. The first
// writer's retry must not report a loss that did not happen: its changes are in
// the successor's tree, the store's Head is exactly its tree, so Flush says so (nil)
// and the Volume is fenced from then on. Had the successor committed more first,
// the retry could not know, and ErrStaleRound would be all it could say (the
// package documentation states both).
func TestLostAckThenTakeoverIsNotReportedLost(t *testing.T) {
	mem := storetest.NewMem()
	var lose atomic.Bool
	hook := &storetest.Hook{Backend: mem, After: func(op, key string) error {
		if lose.Load() && key == "head" && op == "putver" {
			return errors.New("reply lost")
		}
		return nil
	}}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
		t.Fatal(err)
	}
	a, err := vfs.Attach(ctx, cfg(hook, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	write(t, a, vfs.RootInode, "acked-by-nobody", []byte("landed"))
	lose.Store(true)
	// The publisher reads the Head, finds its own generation in it, and says so.
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush of a commit that landed and lost its reply = %v", err)
	}
	lose.Store(false)
	b, err := vfs.Attach(ctx, cfg(mem, c, 2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Resolve(ctx, "/acked-by-nobody"); err != nil {
		t.Fatalf("the landed commit is not in the successor's tree: %v", err)
	}
	// A's retry: its data IS committed. It must not be told it never will be.
	if err := a.Flush(ctx); err != nil {
		t.Fatalf("Flush = %v: the writer is not told its changes landed, but the successor carries them", err)
	}
	// It is fenced all the same. Nothing polls, so it learns at its next publication, and
	// what it accepts until then is in its own journal and goes nowhere.
	if _, err := a.Create(ctx, vfs.RootInode, "more", 0o644); err != nil {
		t.Fatalf("a writer that has not yet been told it is fenced refused a change: %v", err)
	}
	if err := a.Flush(ctx); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("the fenced writer's next publication = %v, want ErrStaleRound", err)
	}
	if _, err := a.Create(ctx, vfs.RootInode, "again", 0o644); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("a fenced writer that was told accepted a change: %v", err)
	}
	write(t, b, vfs.RootInode, "later", []byte("b"))
	flush(t, b)
	if _, err := b.Resolve(ctx, "/more"); err == nil {
		t.Fatal("a change of the fenced writer is in the successor's tree")
	}
}

// TestDeposedWriterIsToldAtItsNextCommit: after a takeover the old writer learns it
// was fenced only when it commits (nothing polls). Until then every write it accepts
// keeps chunking, sealing and uploading packs that no Head will ever name. This
// measures that and holds the line that matters: the orphans change nothing in the
// committed tree, and the commit refuses. Reclaiming them is a collector's job, which
// is not built yet. The writer is told by its next publication, whichever asks for it: the
// Volume's own (the bound, or the journal filling) or a Flush.
func TestDeposedWriterIsToldAtItsNextCommit(t *testing.T) {
	mem := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	a, err := vfs.Attach(ctx, cfg(mem, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	write(t, a, vfs.RootInode, "before", []byte("kept"))
	flush(t, a)
	b, err := vfs.Attach(ctx, cfg(mem, c, 2))
	if err != nil {
		t.Fatal(err)
	}
	in, err := a.Create(ctx, vfs.RootInode, "zombie", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := a.Open(ctx, in.ID)
	mem.Reset()
	var wrote int64
	for i := 0; i < sized(24, 96); i++ { // 96 MiB
		chunk := content(uint64(1000+i), 1<<20)
		if _, err := f.WriteAt(ctx, chunk, wrote); err != nil {
			// Its own publication, which the journal's fill or the bound starts, told it.
			if errors.Is(err, vfs.ErrStaleRound) {
				break
			}
			t.Fatal(err)
		}
		wrote += int64(len(chunk))
	}
	t.Logf("deposed writer wrote %d MiB; %d MiB of packs reached the store before it was told", wrote>>20, packBytes(mem)>>20)
	if err := a.Flush(ctx); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("the deposed writer's commit = %v, want ErrStaleRound", err)
	}
	got := snapshot(t, reopen(t, mem, c))
	if _, ok := got["/zombie"]; ok || len(got) != 1 {
		t.Fatalf("the committed tree is %v after a deposed writer's orphans", got)
	}
	_ = b
}

// TestRoundMaxIsRefused: Attach at the largest round would succeed and then no
// Attach could ever succeed again, so it is refused up front, and the Volume is
// untouched by the attempt.
func TestRoundMaxIsRefused(t *testing.T) {
	mem := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := vfs.Attach(ctx, cfg(mem, c, ^uint64(0))); err == nil {
		t.Fatal("Attach at round 2^64-1 was accepted: every later Attach is refused for good")
	}
	if _, err := vfs.Attach(ctx, cfg(mem, c, 5)); err != nil {
		t.Fatalf("a refused Attach damaged the Volume: %v", err)
	}
	if _, err := vfs.Attach(ctx, cfg(mem, c, ^uint64(0)-1)); err != nil {
		t.Fatalf("the largest usable round: %v", err)
	}
}

// TestHeadSeqNeverRepeats: Seq numbers commit attempts, so it names a state.
// After a lost acknowledgement the retry takes the next number rather than reusing
// the one that landed, and a caller can pin a state by it (it can skip, never
// repeat).
func TestHeadSeqNeverRepeats(t *testing.T) {
	mem := storetest.NewMem()
	var lose atomic.Bool
	hook := &storetest.Hook{Backend: mem, After: func(op, key string) error {
		if lose.Load() && key == "head" && op == "putver" {
			return errors.New("reply lost")
		}
		return nil
	}}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, _ := vfs.Attach(ctx, cfg(hook, c, 1))
	write(t, v, vfs.RootInode, "one", []byte("1"))
	lose.Store(true)
	_ = v.Flush(ctx) // lands as Seq 1, reply lost
	lose.Store(false)
	seqLanded := reopen(t, mem, c).Stats().Seq
	write(t, v, vfs.RootInode, "two", []byte("2"))
	flush(t, v)
	seqNext := reopen(t, mem, c).Stats().Seq
	if seqNext <= seqLanded {
		t.Fatalf("two different committed states share Seq %d", seqNext)
	}
}

// hostileSetup returns a Volume with files a and b committed, and a newer file c
// committed after, on a store of its own.
func hostileSetup(t *testing.T) (mem *storetest.Mem, c vfs.Cipher, a vfs.InodeID, oldHead []byte) {
	mem = storetest.NewMem()
	c = vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, _ := vfs.Attach(ctx, cfg(mem, c, 1))
	a = write(t, v, vfs.RootInode, "a", content(1, 700<<10)).ID
	write(t, v, vfs.RootInode, "b", content(2, 700<<10))
	flush(t, v)
	oldHead, _, _ = mem.Get(ctx, "head", backend.Unbounded)
	write(t, v, vfs.RootInode, "c", []byte("newer"))
	flush(t, v)
	return mem, c, a, oldHead
}

// TestHostileBackend feeds a Volume the objects a hostile or buggy store could
// return and checks every one fails closed, never returns other data.
func TestHostileBackend(t *testing.T) {
	t.Run("truncated pack", func(t *testing.T) {
		mem, c, a, _ := hostileSetup(t)
		for k, b := range mem.Dump() {
			if len(k) > 5 && k[:5] == "pack/" {
				_ = mem.Put(ctx, k, b[:len(b)/8])
			}
		}
		ro := reopen(t, mem, c)
		f, _ := ro.Open(ctx, a)
		if _, err := f.ReadAt(ctx, make([]byte, 700<<10), 0); err == nil {
			t.Fatal("read from a truncated pack succeeded")
		}
	})

	t.Run("old head replayed", func(t *testing.T) {
		// A store that returns an older Head: the Volume cannot know, and says so in
		// its documentation. This asserts what it does do: the old Head opens as the old
		// state, never a mixture, and a caller that pins the Seq it last saw can tell.
		mem, c, _, h1 := hostileSetup(t)
		newSeq := reopen(t, mem, c).Stats().Seq
		_ = mem.Put(ctx, "head", h1)
		ro := reopen(t, mem, c)
		if _, err := ro.Resolve(ctx, "/c"); err == nil {
			t.Fatal("rolled-back volume still shows the newer file")
		}
		if ro.Stats().Seq >= newSeq {
			t.Fatalf("the replayed Head has Seq %d, not below the %d a caller last saw", ro.Stats().Seq, newSeq)
		}
	})

	t.Run("another volume's head", func(t *testing.T) {
		// Another Volume sealed under the same Cipher, its objects copied over this
		// one's: its Head is sealed to the place it lives, so it does not open here.
		mem, c, _, _ := hostileSetup(t)
		other := storetest.NewMem()
		if err := vfs.Create(ctx, cfg(other, c, 0)); err != nil {
			t.Fatal(err)
		}
		ov, _ := vfs.Attach(ctx, cfg(other, c, 1))
		write(t, ov, vfs.RootInode, "other-volumes-file", []byte("secret of volume B"))
		flush(t, ov)
		for k, b := range other.Dump() {
			_ = mem.Put(ctx, k, b)
		}
		if ro, err := vfs.Open(ctx, cfg(mem, c, 0)); err == nil {
			if _, e := ro.Resolve(ctx, "/other-volumes-file"); e == nil {
				t.Fatal("this prefix shows another volume's file: its Head was believed")
			}
		}
	})

	t.Run("another volume's shards and chunks", func(t *testing.T) {
		// This Volume's Head is intact, but every shard and pack in the store is
		// another Volume's, renamed to the names this tree refers to by overwriting
		// them. A hash check would catch a renamed shard; this makes the bytes
		// self-consistent by replacing the Head and tree wholesale with another
		// Volume's at this Volume's place: the sealed Head still names the other's
		// place, which is refused (above), and the shards and chunks are sealed to the
		// other's id, which this Volume's id does not match.
		mem, c, a, _ := hostileSetup(t)
		other := storetest.NewMem()
		if err := vfs.Create(ctx, cfg(other, c, 0)); err != nil {
			t.Fatal(err)
		}
		ov, _ := vfs.Attach(ctx, cfg(other, c, 1))
		oa := write(t, ov, vfs.RootInode, "oa", content(1, 700<<10)).ID
		flush(t, ov)
		// Same plaintext as "a", different Volume: the chunk ids are equal by content.
		// Put the other Volume's packs where this one's are: every chunk id matches, and
		// only the Volume binding says no.
		var mine, theirs []string
		for k := range mem.Dump() {
			if len(k) > 5 && k[:5] == "pack/" {
				mine = append(mine, k)
			}
		}
		for k := range other.Dump() {
			if len(k) > 5 && k[:5] == "pack/" {
				theirs = append(theirs, k)
			}
		}
		if len(mine) == 0 || len(theirs) == 0 {
			t.Fatal("no packs")
		}
		b, _, _ := other.Get(ctx, theirs[0], backend.Unbounded)
		_ = mem.Put(ctx, mine[0], b)
		ro := reopen(t, mem, c)
		f, _ := ro.Open(ctx, a)
		if _, err := f.ReadAt(ctx, make([]byte, 700<<10), 0); err == nil {
			t.Fatal("a chunk sealed to another volume was believed")
		}
		_ = oa
	})
}

// TestLostAckNoLostUpdateNoDuplicate: the Head write lands, its reply is lost, and the
// publisher recognises it; the writer then makes MORE changes and flushes again, and that
// Head's reply is lost too. The tree must hold exactly the first batch and the second,
// each once.
func TestLostAckNoLostUpdateNoDuplicate(t *testing.T) {
	mem := storetest.NewMem()
	var lose atomic.Int32
	hook := &storetest.Hook{Backend: mem, After: func(op, key string) error {
		if key == "head" && op == "putver" && lose.Add(-1) >= 0 {
			return errors.New("reply lost")
		}
		return nil
	}}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, _ := vfs.Attach(ctx, cfg(hook, c, 1))
	write(t, v, vfs.RootInode, "a", []byte("first"))
	lose.Store(2) // the next two commits land and lose their replies
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush of a commit that landed and lost its reply = %v", err)
	}
	write(t, v, vfs.RootInode, "b", []byte("second"))
	big := write(t, v, vfs.RootInode, "big", content(5, 3<<20)).ID
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got := snapshot(t, reopen(t, mem, c))
	if len(got) != 3 {
		t.Fatalf("tree holds %v, want exactly a, b, big", got)
	}
	ro := reopen(t, mem, c)
	mustEqual(t, "big", readAll(t, ro, big), content(5, 3<<20))
	if n := ro.Stats().Inodes; n != 4 { // root, a, b, big
		t.Fatalf("%d inodes, want 4 (a duplicate create would show here)", n)
	}
}

// TestAttachLostAckIsNotStale: the claim's conditional write lands, a retry meets
// its own landed write as a lost precondition (412), and the claimant must not be
// told ErrStaleRound although it owns the round: it finds its own record, byte for
// byte, in the store.
func TestAttachLostAckIsNotStale(t *testing.T) {
	mem := storetest.NewMem()
	var once atomic.Bool
	hook := &storetest.Hook{Backend: mem, After: func(op, key string) error {
		if key == "head" && op == "putver" && once.CompareAndSwap(false, true) {
			return backend.ErrConflict // what an SDK retry of a landed If-Match PUT returns
		}
		return nil
	}}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(hook, c, 1))
	if err != nil {
		t.Fatalf("Attach whose claim landed (reply lost as a 412) = %v: the claimant owns the round and is told it does not", err)
	}
	write(t, v, vfs.RootInode, "mine", []byte("x"))
	flush(t, v)
	// Another claimant at the same round did not own it: the record found after a
	// lost race is not its own, however alike.
	if _, err := vfs.Attach(ctx, cfg(mem, c, 1)); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("a second claim at the round the first owns = %v, want ErrStaleRound", err)
	}
}

// TestInsertAtFrontUploadsOneChunk: an editor that writes a new copy of a
// 24 MiB file with 100 bytes inserted at the front rewrites every byte; the
// Volume must upload about one chunk, not the file.
func TestInsertAtFrontUploadsOneChunk(t *testing.T) {
	mem := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, _ := vfs.Attach(ctx, cfg(mem, c, 1))
	base := content(31, 24<<20)
	in := write(t, v, vfs.RootInode, "doc", base)
	flush(t, v)
	before := packBytes(mem)
	edited := append(content(32, 100), base...)
	f, _ := v.Open(ctx, in.ID)
	if err := v.Truncate(ctx, in.ID, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(ctx, edited, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	flush(t, v)
	grew := packBytes(mem) - before
	t.Logf("insert of 100 bytes at the front of 24 MiB, rewritten whole: %d KiB of new packs", grew>>10)
	if grew > 6<<20 {
		t.Fatalf("%d KiB of new packs for a 100 byte insertion", grew>>10)
	}
	mustEqual(t, "content", readAll(t, reopen(t, mem, c), in.ID), edited)
}
