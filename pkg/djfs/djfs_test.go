// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package djfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
	"github.com/hanzoai/vfs/pkg/djfs"
)

type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func newStepClock() *stepClock {
	return &stepClock{now: time.Unix(1_760_000_000, 0).UTC()}
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(time.Millisecond)
	return c.now
}

func (c *stepClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func sized(shortN, fullN int) int {
	if testing.Short() {
		return shortN
	}
	return fullN
}

func openCrashJournal(t testing.TB, disk journal.Disk, published uint64) *journal.Journal {
	t.Helper()
	j, err := journal.Open("", journal.Options{
		Capacity:  2 << 20,
		Segment:   256 << 10,
		Published: published,
		Lineage:   []byte("djfs-test-vol-round-1"),
		Disk:      disk,
	})
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	return j
}

func openScratchJournal(t testing.TB) *journal.Journal {
	t.Helper()
	dir := t.TempDir()
	opt := journal.Options{
		Capacity: 4 << 20,
		Segment:  1 << 20,
		Lineage:  []byte("djfs-bench-lineage"),
	}
	j, err := journal.Open(dir, opt)
	if err == nil {
		t.Cleanup(func() { _ = j.Close() })
		return j
	}
	if !errors.Is(err, journal.ErrVolatile) {
		t.Fatalf("journal.Open(%s): %v", dir, err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	alt, err := os.MkdirTemp(wd, ".djfs-jbench-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(alt) })
	j, err = journal.Open(alt, opt)
	if err != nil {
		t.Fatalf("journal.Open(%s): %v", alt, err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j
}

func assertTreesIdentical(t testing.TB, label string, got, want *djfs.MemTree) {
	t.Helper()
	if err := got.CheckInvariants(); err != nil {
		t.Fatalf("%s: got invariant failure: %v", label, err)
	}
	if err := want.CheckInvariants(); err != nil {
		t.Fatalf("%s: want invariant failure: %v", label, err)
	}

	gotDirs, gotFiles, _, _ := got.Counts()
	wantDirs, wantFiles, _, _ := want.Counts()
	if gotDirs != wantDirs || gotFiles != wantFiles {
		t.Fatalf("%s: counts (dirs=%d, files=%d), want (dirs=%d, files=%d)", label, gotDirs, gotFiles, wantDirs, wantFiles)
	}

	var walk func(dir djfs.InodeID, path string)
	walk = func(dir djfs.InodeID, path string) {
		ga, err := got.Stat(dir)
		if err != nil {
			t.Fatalf("%s: stat got dir %s (%d): %v", label, path, dir, err)
		}
		wa, err := want.Stat(dir)
		if err != nil {
			t.Fatalf("%s: stat want dir %s (%d): %v", label, path, dir, err)
		}
		if ga.Mode != wa.Mode || ga.Nlink != wa.Nlink || !ga.Mtime.Equal(wa.Mtime) {
			t.Fatalf("%s: dir %s attr got %+v, want %+v", label, path, ga, wa)
		}

		ge, err := got.ReadDir(dir)
		if err != nil {
			t.Fatalf("%s: readdir got %s: %v", label, path, err)
		}
		we, err := want.ReadDir(dir)
		if err != nil {
			t.Fatalf("%s: readdir want %s: %v", label, path, err)
		}
		if len(ge) != len(we) {
			t.Fatalf("%s: dir %s len=%d, want %d", label, path, len(ge), len(we))
		}
		for i := range ge {
			if ge[i] != we[i] {
				t.Fatalf("%s: dir %s entry[%d] got %+v, want %+v", label, path, i, ge[i], we[i])
			}
			childPath := path + "/" + ge[i].Name
			if ge[i].Mode.IsDir() {
				walk(ge[i].ID, childPath)
				continue
			}
			gfa, err := got.Stat(ge[i].ID)
			if err != nil {
				t.Fatalf("%s: stat got file %s: %v", label, childPath, err)
			}
			wfa, err := want.Stat(we[i].ID)
			if err != nil {
				t.Fatalf("%s: stat want file %s: %v", label, childPath, err)
			}
			if gfa.Mode != wfa.Mode || gfa.Size != wfa.Size || gfa.Nlink != wfa.Nlink || !gfa.Mtime.Equal(wfa.Mtime) {
				t.Fatalf("%s: file %s attr got %+v, want %+v", label, childPath, gfa, wfa)
			}
			gb, err := got.ReadFile(ge[i].ID)
			if err != nil {
				t.Fatalf("%s: read got %s: %v", label, childPath, err)
			}
			wb, err := want.ReadFile(we[i].ID)
			if err != nil {
				t.Fatalf("%s: read want %s: %v", label, childPath, err)
			}
			if !bytes.Equal(gb, wb) {
				t.Fatalf("%s: file %s bytes differ", label, childPath)
			}
		}
	}
	walk(djfs.RootInode, "")
}

func TestScratchCreateWriteUnlinkCancels(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	target := djfs.NewMemTree(clk.Now())
	disk := crash.New()
	j := openCrashJournal(t, disk, 0)
	defer j.Close()

	eng, err := djfs.New(djfs.Config{
		Journal: j,
		Target:  target,
		Clock:   clk,
	})
	if err != nil {
		t.Fatal(err)
	}

	buildDir, err := eng.Mkdir(ctx, djfs.RootInode, "scratch", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	beforeStats := eng.Stats()
	payload := bytes.Repeat([]byte("z"), 4096)
	const n = 500
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("tmp_%04d.o", i)
		if _, err := eng.Create(ctx, buildDir.ID, name, 0o600, payload); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Write(ctx, buildDir.ID, name, payload); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Chmod(ctx, buildDir.ID, name, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := eng.Unlink(ctx, buildDir.ID, name); err != nil {
			t.Fatal(err)
		}
	}

	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	afterStats := eng.Stats()

	rawDelta := afterStats.RawOps - beforeStats.RawOps
	pubDelta := afterStats.PublishedOps - beforeStats.PublishedOps
	if rawDelta != 4*n {
		t.Fatalf("raw ops = %d, want %d", rawDelta, 4*n)
	}
	// All 500 created+written+chmodded+unlinked files cancel; only the parent
	// directory's updated timestamp is emitted.
	if pubDelta != 1 {
		t.Fatalf("published ops = %d, want 1 coalesced directory timestamp update", pubDelta)
	}
	ents, err := target.ReadDir(buildDir.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("expected empty scratch dir, got %d entries", len(ents))
	}
	if err := target.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestNpmStagingExtractCoalescesToFinalCreate(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	target := djfs.NewMemTree(clk.Now())
	eng, err := djfs.New(djfs.Config{
		Target: target,
		Clock:  clk,
	})
	if err != nil {
		t.Fatal(err)
	}

	staging, err := eng.Mkdir(ctx, djfs.RootInode, ".staging", 0o700)
	if err != nil {
		t.Fatal(err)
	}
	pkgDir, err := eng.Mkdir(ctx, djfs.RootInode, "pkg-alpha", 0o755)
	if err != nil {
		t.Fatal(err)
	}

	const files = 100
	for i := 0; i < files; i++ {
		tmpName := fmt.Sprintf("extract_%03d.tmp", i)
		dstName := fmt.Sprintf("mod_%03d.js", i)
		body := []byte(fmt.Sprintf("export const v = %d;\n", i))

		in, err := eng.Create(ctx, staging.ID, tmpName, 0o600, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Write(ctx, staging.ID, tmpName, body); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Chmod(ctx, staging.ID, tmpName, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := eng.CloseFile(in.ID); err != nil {
			t.Fatal(err)
		}
		if err := eng.Rename(ctx, staging.ID, tmpName, pkgDir.ID, dstName); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Rmdir(ctx, djfs.RootInode, ".staging"); err != nil {
		t.Fatal(err)
	}

	// Clean closes must not publish anything synchronously before Publish.
	_, targetFilesBefore, batchesBefore, _ := target.Counts()
	if targetFilesBefore != 0 || batchesBefore != 0 {
		t.Fatalf("clean close mutated target prematurely: files=%d batches=%d", targetFilesBefore, batchesBefore)
	}

	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := target.Lookup(djfs.RootInode, ".staging"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected .staging to be absent, got err=%v", err)
	}
	ents, err := target.ReadDir(pkgDir.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != files {
		t.Fatalf("pkgDir has %d files, want %d", len(ents), files)
	}
	st := eng.Stats()
	// 100 files * 4 ops + 2 mkdir + 1 rmdir = 403 raw ops -> 1 mkdir(pkg-alpha) +
	// 100 creates + 2 dir setattr = 103 published ops (zero .staging ops!).
	if st.PublishedOps > 105 {
		t.Fatalf("published ops = %d, expected <= 105 for 100 surviving files", st.PublishedOps)
	}
	if err := target.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicWriteTmpRenameOverwriteStorm(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	target := djfs.NewMemTree(clk.Now())
	eng, err := djfs.New(djfs.Config{
		Target: target,
		Clock:  clk,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := eng.Create(ctx, djfs.RootInode, "package-lock.json", 0o644, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	before := eng.Stats()
	const rounds = 200
	var lastPayload []byte
	for i := 0; i < rounds; i++ {
		tmp := fmt.Sprintf(".package-lock.json.tmp.%d", i)
		lastPayload = []byte(fmt.Sprintf("{\"lockfileVersion\":3,\"seq\":%d}", i))
		if _, err := eng.Create(ctx, djfs.RootInode, tmp, 0o600, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Write(ctx, djfs.RootInode, tmp, lastPayload); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Chmod(ctx, djfs.RootInode, tmp, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := eng.Rename(ctx, djfs.RootInode, tmp, djfs.RootInode, "package-lock.json"); err != nil {
			t.Fatal(err)
		}
	}

	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	after := eng.Stats()

	// 800 raw ops collapse into 3 ops: unlink old inode, create final inode,
	// stamp root directory mtime/ctime.
	emitted := after.PublishedOps - before.PublishedOps
	if emitted != 3 {
		t.Fatalf("published ops = %d, want 3 (800 raw ops collapsed)", emitted)
	}
	in, err := target.Lookup(djfs.RootInode, "package-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	got, err := target.ReadFile(in.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, lastPayload) {
		t.Fatalf("got %q, want %q", got, lastPayload)
	}
	if err := target.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestSwapAndThreeWayCycleAndAncestorInversion(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	target := djfs.NewMemTree(clk.Now())
	eng, err := djfs.New(djfs.Config{
		Target: target,
		Clock:  clk,
	})
	if err != nil {
		t.Fatal(err)
	}

	fa, _ := eng.Create(ctx, djfs.RootInode, "a", 0o644, []byte("A"))
	fb, _ := eng.Create(ctx, djfs.RootInode, "b", 0o644, []byte("B"))
	fc, _ := eng.Create(ctx, djfs.RootInode, "c", 0o644, []byte("C"))

	dirA, _ := eng.Mkdir(ctx, djfs.RootInode, "dirA", 0o755)
	dirB, _ := eng.Mkdir(ctx, dirA.ID, "dirB", 0o755)
	dirC, _ := eng.Mkdir(ctx, dirB.ID, "dirC", 0o755)
	if _, err := eng.Create(ctx, dirC.ID, "leaf.txt", 0o644, []byte("leaf")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	// Moving dirA into its own descendant dirC must fail with fs.ErrInvalid.
	if err := eng.Rename(ctx, djfs.RootInode, "dirA", dirC.ID, "illegal"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("expected ErrInvalid on directory cycle, got %v", err)
	}

	// 3-way file cycle: a -> b, b -> c, c -> a via scratch name.
	if err := eng.Rename(ctx, djfs.RootInode, "a", djfs.RootInode, "swap"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Rename(ctx, djfs.RootInode, "c", djfs.RootInode, "a"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Rename(ctx, djfs.RootInode, "b", djfs.RootInode, "c"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Rename(ctx, djfs.RootInode, "swap", djfs.RootInode, "b"); err != nil {
		t.Fatal(err)
	}

	// 3-level ancestor inversion: unnest dirB (carrying dirC) to /dirB, then move
	// dirA inside /dirB/dirC/dirA!
	if err := eng.Rename(ctx, dirA.ID, "dirB", djfs.RootInode, "dirB"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Rename(ctx, djfs.RootInode, "dirA", dirC.ID, "dirA"); err != nil {
		t.Fatal(err)
	}

	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	assertTreesIdentical(t, "cycle+inversion", target, eng.Snapshot())

	gotA, _ := target.Lookup(djfs.RootInode, "a")
	gotB, _ := target.Lookup(djfs.RootInode, "b")
	gotC, _ := target.Lookup(djfs.RootInode, "c")
	if gotA.ID != fc.ID || gotB.ID != fa.ID || gotC.ID != fb.ID {
		t.Fatalf("3-way cycle IDs got (%d,%d,%d), want (%d,%d,%d)", gotA.ID, gotB.ID, gotC.ID, fc.ID, fa.ID, fb.ID)
	}
}

func TestCrossDirectoryRenameBarrierAndConcurrentFsync(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	disk := crash.New()
	j := openCrashJournal(t, disk, 0)
	defer j.Close()

	base := djfs.NewMemTree(clk.Now())
	eng, err := djfs.New(djfs.Config{
		Journal: j,
		Target:  base,
		Clock:   clk,
	})
	if err != nil {
		t.Fatal(err)
	}

	d1, _ := eng.Mkdir(ctx, djfs.RootInode, "src", 0o755)
	d2, _ := eng.Mkdir(ctx, djfs.RootInode, "dst", 0o755)
	f, _ := eng.Create(ctx, d1.ID, "item.bin", 0o644, []byte("atomic-payload"))
	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	// Cross-dir move from d1 to d2, then Fsync ONLY d1 (the vacated source dir):
	// entanglement must pull d2 into the same atomic journal batch so recovery
	// never loses item.bin or duplicates its link!
	if err := eng.Rename(ctx, d1.ID, "item.bin", d2.ID, "moved.bin"); err != nil {
		t.Fatal(err)
	}
	if err := eng.FsyncDir(ctx, d1.ID); err != nil {
		t.Fatal(err)
	}

	recEng, err := djfs.RecoverFromTree(ctx, djfs.Config{Clock: clk}, base, j)
	if err != nil {
		t.Fatal(err)
	}
	recSnap := recEng.Snapshot()
	if err := recSnap.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
	if _, err := recSnap.Lookup(d1.ID, "item.bin"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("source entry still present after cross-dir fsync: %v", err)
	}
	movedAttr, err := recSnap.Lookup(d2.ID, "moved.bin")
	if err != nil || movedAttr.ID != f.ID {
		t.Fatalf("destination entry missing or wrong ID: %+v, %v", movedAttr, err)
	}

	// Now verify concurrent Fsync barrier linearizability (CC1): while Fsync #1
	// is parked inside disk sync, Fsync #2 on the same directory (now clean wrt
	// jBase) MUST still wait for that disk sync to finish before returning.
	syncEntered := make(chan struct{}, 8)
	releaseSync := make(chan struct{})
	var holdSync atomic.Bool
	holdSync.Store(true)
	disk.Intercept(func(ev crash.Event) error {
		if ev.Kind == "sync" && holdSync.Load() {
			syncEntered <- struct{}{}
			<-releaseSync
		}
		return nil
	})

	if _, err := eng.Create(ctx, d2.ID, "barrier.txt", 0o644, []byte("sync-me")); err != nil {
		t.Fatal(err)
	}

	fsync1Done := make(chan error, 1)
	go func() {
		fsync1Done <- eng.FsyncDir(ctx, d2.ID)
	}()

	<-syncEntered

	fsync2Done := make(chan error, 1)
	go func() {
		fsync2Done <- eng.FsyncDir(ctx, d2.ID)
	}()

	select {
	case err := <-fsync2Done:
		t.Fatalf("second FsyncDir returned before in-flight disk sync completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	holdSync.Store(false)
	close(releaseSync)
	if err := <-fsync1Done; err != nil {
		t.Fatalf("fsync1: %v", err)
	}
	if err := <-fsync2Done; err != nil {
		t.Fatalf("fsync2: %v", err)
	}
	disk.Intercept(nil)
}

func TestWatermarksAndPublishWindow(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	disk := crash.New()
	j := openCrashJournal(t, disk, 0)
	defer j.Close()

	eng, err := djfs.New(djfs.Config{
		Journal:          j,
		Clock:            clk,
		MaxDirtyOps:      20,
		MaxDirtyBytes:    8 << 10,
		MaxPublishWindow: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 25; i++ {
		name := fmt.Sprintf("f%02d", i)
		if _, err := eng.Create(ctx, djfs.RootInode, name, 0o644, []byte("hello")); err != nil {
			t.Fatal(err)
		}
	}

	st := eng.Stats()
	if st.JournalBatches == 0 {
		t.Fatalf("expected automatic watermark journal flush after MaxDirtyOps=20")
	}
	if st.PublishOverdue {
		t.Fatalf("unexpected PublishOverdue before 5s window")
	}

	clk.Advance(6 * time.Second)
	st = eng.Stats()
	if !st.PublishOverdue {
		t.Fatalf("expected PublishOverdue after advancing clock by 6s (age=%v)", st.UnpublishedAge)
	}

	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	st = eng.Stats()
	if st.PublishOverdue || st.UnpublishedAge != 0 {
		t.Fatalf("expected clean watermark after Publish, got overdue=%v age=%v", st.PublishOverdue, st.UnpublishedAge)
	}
}

func TestCodecRoundTripAndTamperRejection(t *testing.T) {
	now := time.Unix(1_760_000_123, 456_000).UTC()
	orig := djfs.Batch{
		Epoch:  7,
		MaxSeq: 991,
		Ops: []djfs.DeltaOp{
			{
				Kind: djfs.DeltaMkdir,
				Dir:  djfs.RootInode,
				Name: "pkg",
				ID:   2,
				Attr: djfs.Attr{ID: 2, Mode: fs.ModeDir | 0o755, Nlink: 2, Mtime: now, Atime: now, Ctime: now},
			},
			{
				Kind: djfs.DeltaCreateFile,
				Dir:  2,
				Name: "index.js",
				ID:   3,
				Attr: djfs.Attr{ID: 3, Mode: 0o644, Size: 12, Nlink: 1, Mtime: now, Atime: now, Ctime: now},
				Data: []byte("hello world\n"),
			},
		},
	}

	raw := djfs.EncodeBatch(orig)
	got, err := djfs.DecodeBatch(raw)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if got.Epoch != orig.Epoch || got.MaxSeq != orig.MaxSeq || len(got.Ops) != len(orig.Ops) {
		t.Fatalf("header mismatch: got %+v, want %+v", got, orig)
	}
	if !bytes.Equal(got.Ops[1].Data, orig.Ops[1].Data) || got.Ops[1].Name != "index.js" {
		t.Fatalf("payload mismatch: %+v", got.Ops[1])
	}

	// Single bit-flip anywhere after magic must fail CRC-32C with ErrCorrupt.
	tampered := append([]byte(nil), raw...)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := djfs.DecodeBatch(tampered); !errors.Is(err, djfs.ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on bit-flip, got %v", err)
	}

	// Truncated frame must fail with ErrCorrupt.
	if _, err := djfs.DecodeBatch(raw[:10]); !errors.Is(err, djfs.ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on short header, got %v", err)
	}
	if _, err := djfs.DecodeBatch(raw[:len(raw)-2]); !errors.Is(err, djfs.ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on truncated body, got %v", err)
	}

	// Bad magic must fail with ErrCorrupt.
	badMagic := append([]byte(nil), raw...)
	badMagic[0] = 'X'
	if _, err := djfs.DecodeBatch(badMagic); !errors.Is(err, djfs.ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on bad magic, got %v", err)
	}
}

func cloneMemTree(t testing.TB, src *djfs.MemTree) *djfs.MemTree {
	t.Helper()
	eng, err := djfs.NewFromTree(djfs.Config{}, src)
	if err != nil {
		t.Fatal(err)
	}
	return eng.Snapshot()
}

func treesEqualExact(got, want *djfs.MemTree) error {
	if err := got.CheckInvariants(); err != nil {
		return fmt.Errorf("got invariant failure: %w", err)
	}
	if err := want.CheckInvariants(); err != nil {
		return fmt.Errorf("want invariant failure: %w", err)
	}
	gotDirs, gotFiles, _, _ := got.Counts()
	wantDirs, wantFiles, _, _ := want.Counts()
	if gotDirs != wantDirs || gotFiles != wantFiles {
		return fmt.Errorf("counts (dirs=%d, files=%d), want (dirs=%d, files=%d)", gotDirs, gotFiles, wantDirs, wantFiles)
	}
	var walk func(dir djfs.InodeID, path string) error
	walk = func(dir djfs.InodeID, path string) error {
		ga, err := got.Stat(dir)
		if err != nil {
			return fmt.Errorf("stat got dir %s (%d): %w", path, dir, err)
		}
		wa, err := want.Stat(dir)
		if err != nil {
			return fmt.Errorf("stat want dir %s (%d): %w", path, dir, err)
		}
		if ga.Mode != wa.Mode || ga.Nlink != wa.Nlink {
			return fmt.Errorf("dir %s attr got %+v, want %+v", path, ga, wa)
		}
		ge, err := got.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("readdir got %s: %w", path, err)
		}
		we, err := want.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("readdir want %s: %w", path, err)
		}
		if len(ge) != len(we) {
			return fmt.Errorf("dir %s len=%d, want %d", path, len(ge), len(we))
		}
		for i := range ge {
			if ge[i] != we[i] {
				return fmt.Errorf("dir %s entry[%d] got %+v, want %+v", path, i, ge[i], we[i])
			}
			childPath := path + "/" + ge[i].Name
			if ge[i].Mode.IsDir() {
				if err := walk(ge[i].ID, childPath); err != nil {
					return err
				}
				continue
			}
			gfa, err := got.Stat(ge[i].ID)
			if err != nil {
				return fmt.Errorf("stat got file %s: %w", childPath, err)
			}
			wfa, err := want.Stat(we[i].ID)
			if err != nil {
				return fmt.Errorf("stat want file %s: %w", childPath, err)
			}
			if gfa.Mode != wfa.Mode || gfa.Size != wfa.Size || gfa.Nlink != wfa.Nlink {
				return fmt.Errorf("file %s attr got %+v, want %+v", childPath, gfa, wfa)
			}
			gb, err := got.ReadFile(ge[i].ID)
			if err != nil {
				return fmt.Errorf("read got %s: %w", childPath, err)
			}
			wb, err := want.ReadFile(we[i].ID)
			if err != nil {
				return fmt.Errorf("read want %s: %w", childPath, err)
			}
			if !bytes.Equal(gb, wb) {
				return fmt.Errorf("file %s bytes %q != %q", childPath, gb, wb)
			}
		}
		return nil
	}
	return walk(djfs.RootInode, "")
}

func TestCrashConsistencyExploreOnJournalDisk(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	t0 := clk.Now()
	disk := crash.New()
	j := openCrashJournal(t, disk, 0)

	oracle := djfs.NewMemTree(t0)
	eng, err := djfs.NewFromTree(djfs.Config{
		Journal: j,
		Clock:   clk,
	}, oracle)
	if err != nil {
		t.Fatal(err)
	}

	var checkpoints []*djfs.MemTree
	checkpoints = append(checkpoints, cloneMemTree(t, oracle)) // 0: empty

	dA, err := eng.Mkdir(ctx, djfs.RootInode, "dirA", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaMkdir, Dir: djfs.RootInode, Name: "dirA", ID: dA.ID, Attr: dA},
	}}); err != nil {
		t.Fatal(err)
	}
	dB, err := eng.Mkdir(ctx, djfs.RootInode, "dirB", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaMkdir, Dir: djfs.RootInode, Name: "dirB", ID: dB.ID, Attr: dB},
	}}); err != nil {
		t.Fatal(err)
	}
	// Permanent synced file that is never renamed — must never vanish after mark 1.
	fKeep, err := eng.Create(ctx, dA.ID, "keep.txt", 0o644, []byte("keep-durable"))
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaCreateFile, Dir: dA.ID, Name: "keep.txt", ID: fKeep.ID, Attr: fKeep, Data: []byte("keep-durable")},
	}}); err != nil {
		t.Fatal(err)
	}
	f1, err := eng.Create(ctx, dA.ID, "alpha.txt", 0o644, []byte("v1-durable"))
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaCreateFile, Dir: dA.ID, Name: "alpha.txt", ID: f1.ID, Attr: f1, Data: []byte("v1-durable")},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Fsync(ctx, f1.ID); err != nil {
		t.Fatal(err)
	}
	checkpoints = append(checkpoints, cloneMemTree(t, oracle)) // 1: keep.txt + alpha.txt synced
	disk.Mark(1)

	// Transient scratch file that cancels before next sync.
	if _, err := eng.Create(ctx, dB.ID, "scratch.tmp", 0o600, []byte("ephemeral")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Unlink(ctx, dB.ID, "scratch.tmp"); err != nil {
		t.Fatal(err)
	}

	// Cross-directory atomic move + payload update, then FsyncDir.
	w1, err := eng.Write(ctx, dA.ID, "alpha.txt", []byte("v2-moved-durable"))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Rename(ctx, dA.ID, "alpha.txt", dB.ID, "beta.txt"); err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaWrite, Dir: dA.ID, Name: "alpha.txt", ID: f1.ID, Attr: w1, Data: []byte("v2-moved-durable")},
		{Kind: djfs.DeltaRename, Dir: dA.ID, Name: "alpha.txt", DstDir: dB.ID, DstName: "beta.txt", ID: f1.ID},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.FsyncDir(ctx, dB.ID); err != nil {
		t.Fatal(err)
	}
	checkpoints = append(checkpoints, cloneMemTree(t, oracle)) // 2: keep.txt + beta.txt synced, alpha.txt gone
	disk.Mark(2)

	// Appended but unsynced tail batch to exercise torn/prefix/subset volatile states.
	fTail, err := eng.Create(ctx, dB.ID, "unsynced.tail", 0o644, []byte("volatile-tail"))
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaCreateFile, Dir: dB.ID, Name: "unsynced.tail", ID: fTail.ID, Attr: fTail, Data: []byte("volatile-tail")},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.FlushJournal(ctx); err != nil {
		t.Fatal(err)
	}
	checkpoints = append(checkpoints, cloneMemTree(t, oracle)) // 3: unsynced tail landed
	_ = j.Close()

	var explored int
	seenCheckpoints := map[int]bool{}
	err = disk.Explore(2, 20261118, func(img *crash.Disk, point int, v crash.Variant) error {
		explored++
		rj, openErr := journal.Open("", journal.Options{
			Capacity: 2 << 20,
			Segment:  256 << 10,
			Lineage:  []byte("djfs-test-vol-round-1"),
			Disk:     img,
		})
		if openErr != nil {
			return fmt.Errorf("journal.Open: %w", openErr)
		}
		defer rj.Close()

		recEng, recErr := djfs.Recover(ctx, djfs.Config{Clock: clk}, rj)
		if recErr != nil {
			return fmt.Errorf("djfs.Recover: %w", recErr)
		}
		snap := recEng.Snapshot()
		if invErr := snap.CheckInvariants(); invErr != nil {
			return fmt.Errorf("invariant violation (%s): %w", v, invErr)
		}

		// Verify alpha/beta never appear simultaneously (atomic cross-dir rename).
		_, hasOld := snap.Lookup(dA.ID, "alpha.txt")
		_, hasNew := snap.Lookup(dB.ID, "beta.txt")
		if hasOld == nil && hasNew == nil {
			return fmt.Errorf("torn cross-dir rename: both dirA/alpha.txt and dirB/beta.txt exist")
		}

		// Every recovered crash state must match an exact atomic uncoalesced oracle
		// checkpoint in [completedMark .. completedMark+1], never losing a synced file.
		marks := disk.Marks(point)
		minIdx := 0
		if len(marks) > 0 {
			minIdx = marks[len(marks)-1].(int)
		}
		if minIdx >= 1 {
			inKeep, kErr := snap.Lookup(dA.ID, "keep.txt")
			if kErr != nil {
				return fmt.Errorf("synced permanent file dirA/keep.txt lost at mark %d (%s): %w", minIdx, v, kErr)
			}
			gotKeep, rErr := snap.ReadFile(inKeep.ID)
			if rErr != nil || !bytes.Equal(gotKeep, []byte("keep-durable")) {
				return fmt.Errorf("synced keep.txt corrupt at mark %d (%s): got %q, err=%v", minIdx, v, gotKeep, rErr)
			}
		}
		maxIdx := min(minIdx+1, len(checkpoints)-1)
		matched := -1
		var lastDiff error
		for idx := minIdx; idx <= maxIdx; idx++ {
			if diffErr := treesEqualExact(snap, checkpoints[idx]); diffErr == nil {
				matched = idx
				break
			} else {
				lastDiff = diffErr
			}
		}
		if matched < 0 {
			return fmt.Errorf("crash point %d (%s, mark=%d) matched neither oracle[%d] nor oracle[%d]: %v",
				point, v, minIdx, minIdx, maxIdx, lastDiff)
		}
		seenCheckpoints[matched] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if explored < 10 {
		t.Fatalf("expected crash explorer to check >= 10 distinct disk states, got %d", explored)
	}
	if !seenCheckpoints[1] || !seenCheckpoints[2] {
		t.Fatalf("expected crash explorer to observe both pre-rename (1) and post-rename (2) checkpoints, saw %v", seenCheckpoints)
	}
}

// TestRegressionSeed0CrossDirRenameParentExpansion is the shrunk minimal
// regression extracted from randomized seed-0 step 136 covering both cases:
//  1. Unentangled sibling subtree FsyncDir(sub) under an already-committed
//     parent d10 does not split the unsynced cross-dir rename {d11, Root}.
//  2. FsyncDir(sub2) under a newly created dir d12 (whose parent is Root)
//     expands d12 -> Root and MUST transitively pull in Root's entangled
//     partner d11 so the cross-dir rename is flushed atomically (never torn
//     into a duplicate DeltaCreateFile at Root/d while d11/d remains on disk).
func TestRegressionSeed0CrossDirRenameParentExpansion(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	t0 := clk.Now()

	oracle := djfs.NewMemTree(t0)
	published := djfs.NewMemTree(t0)
	disk := crash.New()
	j := openCrashJournal(t, disk, 0)
	defer j.Close()

	eng, err := djfs.NewFromTree(djfs.Config{
		Journal: j,
		Target:  published,
		Clock:   clk,
	}, published)
	if err != nil {
		t.Fatal(err)
	}

	d10, err := eng.Mkdir(ctx, djfs.RootInode, "a", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	d11, err := eng.Mkdir(ctx, djfs.RootInode, "index.js", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	f16, err := eng.Create(ctx, d11.ID, "d", 0o644, []byte("seed0-payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaMkdir, Dir: djfs.RootInode, Name: "a", ID: d10.ID, Attr: d10},
		{Kind: djfs.DeltaMkdir, Dir: djfs.RootInode, Name: "index.js", ID: d11.ID, Attr: d11},
		{Kind: djfs.DeltaCreateFile, Dir: d11.ID, Name: "d", ID: f16.ID, Attr: f16, Data: []byte("seed0-payload")},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	// Cross-dir rename entangling {d11, Root}.
	if err := eng.Rename(ctx, d11.ID, "d", djfs.RootInode, "d"); err != nil {
		t.Fatal(err)
	}
	// Case 1: subdirectory under already-committed d10 (entangles {d10, sub}).
	sub, err := eng.Mkdir(ctx, d10.ID, "sub", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	oracleSubOnly := cloneMemTree(t, oracle)
	if err := oracleSubOnly.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaMkdir, Dir: d10.ID, Name: "sub", ID: sub.ID, Attr: sub},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.FsyncDir(ctx, sub.ID); err != nil {
		t.Fatal(err)
	}
	rec1, err := djfs.RecoverFromTree(ctx, djfs.Config{Clock: clk}, published, j)
	if err != nil {
		t.Fatalf("RecoverFromTree after Case 1: %v", err)
	}
	if err := treesEqualExact(rec1.Snapshot(), oracleSubOnly); err != nil {
		t.Fatalf("Case 1 recovered tree diverged from uncoalesced oracle: %v", err)
	}

	// Case 2: newly born d12 under Root + child sub2 under d12 -> FsyncDir(sub2)
	// expands sub2 -> d12 -> Root and transitively pulls in entangled d11.
	d12, err := eng.Mkdir(ctx, djfs.RootInode, "d12", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := eng.Mkdir(ctx, d12.ID, "sub2", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaRename, Dir: d11.ID, Name: "d", DstDir: djfs.RootInode, DstName: "d", ID: f16.ID},
		{Kind: djfs.DeltaMkdir, Dir: d10.ID, Name: "sub", ID: sub.ID, Attr: sub},
		{Kind: djfs.DeltaMkdir, Dir: djfs.RootInode, Name: "d12", ID: d12.ID, Attr: d12},
		{Kind: djfs.DeltaMkdir, Dir: d12.ID, Name: "sub2", ID: sub2.ID, Attr: sub2},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.FsyncDir(ctx, sub2.ID); err != nil {
		t.Fatal(err)
	}
	rec2, err := djfs.RecoverFromTree(ctx, djfs.Config{Clock: clk}, published, j)
	if err != nil {
		t.Fatalf("RecoverFromTree after Case 2: %v", err)
	}
	if err := treesEqualExact(rec2.Snapshot(), oracle); err != nil {
		t.Fatalf("Case 2 recovered tree diverged from uncoalesced oracle: %v", err)
	}
}

// TestCrossDirRenameAndFsyncCrashCutsExactOracle enumerates crash cuts across
// cross-directory file overwrite rename, cross-directory subtree move, and
// chained 3-directory renames with interleaved fsyncs, asserting exact
// recovered whole-tree equivalence against the uncoalesced oracle at every cut.
func TestCrossDirRenameAndFsyncCrashCutsExactOracle(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	t0 := clk.Now()
	disk := crash.New()
	j := openCrashJournal(t, disk, 0)

	oracle := djfs.NewMemTree(t0)
	eng, err := djfs.NewFromTree(djfs.Config{Journal: j, Clock: clk}, oracle)
	if err != nil {
		t.Fatal(err)
	}

	checkpoints := []*djfs.MemTree{cloneMemTree(t, oracle)}

	// Checkpoint 1: create dirA, dirB, dirC, anchor, src, victim dst, and dirA/sub/deep.
	dA, err := eng.Mkdir(ctx, djfs.RootInode, "dirA", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	dB, err := eng.Mkdir(ctx, djfs.RootInode, "dirB", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	dC, err := eng.Mkdir(ctx, djfs.RootInode, "dirC", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := eng.Mkdir(ctx, dA.ID, "sub", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	fAnchor, err := eng.Create(ctx, dA.ID, "anchor.txt", 0o644, []byte("anchor-v1"))
	if err != nil {
		t.Fatal(err)
	}
	fSrc, err := eng.Create(ctx, dA.ID, "src.txt", 0o644, []byte("new-winner"))
	if err != nil {
		t.Fatal(err)
	}
	fVictim, err := eng.Create(ctx, dB.ID, "dst.txt", 0o644, []byte("old-victim"))
	if err != nil {
		t.Fatal(err)
	}
	fDeep, err := eng.Create(ctx, sub.ID, "deep.txt", 0o644, []byte("nested-data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaMkdir, Dir: djfs.RootInode, Name: "dirA", ID: dA.ID, Attr: dA},
		{Kind: djfs.DeltaMkdir, Dir: djfs.RootInode, Name: "dirB", ID: dB.ID, Attr: dB},
		{Kind: djfs.DeltaMkdir, Dir: djfs.RootInode, Name: "dirC", ID: dC.ID, Attr: dC},
		{Kind: djfs.DeltaMkdir, Dir: dA.ID, Name: "sub", ID: sub.ID, Attr: sub},
		{Kind: djfs.DeltaCreateFile, Dir: dA.ID, Name: "anchor.txt", ID: fAnchor.ID, Attr: fAnchor, Data: []byte("anchor-v1")},
		{Kind: djfs.DeltaCreateFile, Dir: dA.ID, Name: "src.txt", ID: fSrc.ID, Attr: fSrc, Data: []byte("new-winner")},
		{Kind: djfs.DeltaCreateFile, Dir: dB.ID, Name: "dst.txt", ID: fVictim.ID, Attr: fVictim, Data: []byte("old-victim")},
		{Kind: djfs.DeltaCreateFile, Dir: sub.ID, Name: "deep.txt", ID: fDeep.ID, Attr: fDeep, Data: []byte("nested-data")},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.FsyncDir(ctx, djfs.RootInode); err != nil {
		t.Fatal(err)
	}
	checkpoints = append(checkpoints, cloneMemTree(t, oracle))
	disk.Mark(1)

	// Checkpoint 2: cross-dir overwrite Rename(dirA/src.txt -> dirB/dst.txt) + FsyncDir(dirB).
	if err := eng.Rename(ctx, dA.ID, "src.txt", dB.ID, "dst.txt"); err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaRename, Dir: dA.ID, Name: "src.txt", DstDir: dB.ID, DstName: "dst.txt", ID: fSrc.ID},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.FsyncDir(ctx, dB.ID); err != nil {
		t.Fatal(err)
	}
	checkpoints = append(checkpoints, cloneMemTree(t, oracle))
	disk.Mark(2)

	// Checkpoint 3: cross-dir directory Rename(dirA/sub -> dirC/sub_moved) + FsyncDir(dirC).
	if err := eng.Rename(ctx, dA.ID, "sub", dC.ID, "sub_moved"); err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaRename, Dir: dA.ID, Name: "sub", DstDir: dC.ID, DstName: "sub_moved", ID: sub.ID},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.FsyncDir(ctx, dC.ID); err != nil {
		t.Fatal(err)
	}
	checkpoints = append(checkpoints, cloneMemTree(t, oracle))
	disk.Mark(3)

	// Checkpoint 4: chained 3-dir file move (dirB/dst.txt -> dirA/mid.txt -> dirC/final.txt) + FsyncDir(dirC).
	if err := eng.Rename(ctx, dB.ID, "dst.txt", dA.ID, "mid.txt"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Rename(ctx, dA.ID, "mid.txt", dC.ID, "final.txt"); err != nil {
		t.Fatal(err)
	}
	if err := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
		{Kind: djfs.DeltaRename, Dir: dB.ID, Name: "dst.txt", DstDir: dA.ID, DstName: "mid.txt", ID: fSrc.ID},
		{Kind: djfs.DeltaRename, Dir: dA.ID, Name: "mid.txt", DstDir: dC.ID, DstName: "final.txt", ID: fSrc.ID},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.FsyncDir(ctx, dC.ID); err != nil {
		t.Fatal(err)
	}
	checkpoints = append(checkpoints, cloneMemTree(t, oracle))
	disk.Mark(4)
	_ = j.Close()

	var explored int
	err = disk.Explore(2, 13450001, func(img *crash.Disk, point int, v crash.Variant) error {
		explored++
		rj, openErr := journal.Open("", journal.Options{
			Capacity: 2 << 20,
			Segment:  256 << 10,
			Lineage:  []byte("djfs-test-vol-round-1"),
			Disk:     img,
		})
		if openErr != nil {
			return fmt.Errorf("journal.Open: %w", openErr)
		}
		defer rj.Close()

		recEng, recErr := djfs.Recover(ctx, djfs.Config{Clock: clk}, rj)
		if recErr != nil {
			return fmt.Errorf("djfs.Recover: %w", recErr)
		}
		snap := recEng.Snapshot()
		marks := disk.Marks(point)
		minIdx := 0
		if len(marks) > 0 {
			minIdx = marks[len(marks)-1].(int)
		}
		maxIdx := min(minIdx+1, len(checkpoints)-1)
		var lastErr error
		for idx := minIdx; idx <= maxIdx; idx++ {
			if diffErr := treesEqualExact(snap, checkpoints[idx]); diffErr == nil {
				return nil
			} else {
				lastErr = diffErr
			}
		}
		return fmt.Errorf("point %d (%s, mark=%d) matched no exact oracle checkpoint [%d..%d]: %v",
			point, v, minIdx, minIdx, maxIdx, lastErr)
	})
	if err != nil {
		t.Fatal(err)
	}
	if explored < 20 {
		t.Fatalf("expected >= 20 crash states explored, got %d", explored)
	}
}

func TestRandomizedEquivalenceAgainstUncoalescedOracle(t *testing.T) {
	ctx := context.Background()
	seeds := sized(8, 24)
	stepsPerSeed := sized(250, 600)
	names := []string{"a", "b", "c", "d", "tmp.0", "tmp.1", "index.js", "lock"}

	for seed := 0; seed < seeds; seed++ {
		rng := rand.New(rand.NewPCG(uint64(seed+1), uint64(seed*97+13)))
		clk := newStepClock()
		t0 := clk.Now()

		oracle := djfs.NewMemTree(t0)
		published := djfs.NewMemTree(t0)
		disk := crash.New()
		j := openCrashJournal(t, disk, 0)

		eng, err := djfs.NewFromTree(djfs.Config{
			Journal: j,
			Target:  published,
			Clock:   clk,
		}, published)
		if err != nil {
			j.Close()
			t.Fatal(err)
		}

		liveDirs := []djfs.InodeID{djfs.RootInode}
		for step := 0; step < stepsPerSeed; step++ {
			dir := liveDirs[rng.IntN(len(liveDirs))]
			name := names[rng.IntN(len(names))]

			switch rng.IntN(10) {
			case 0, 1:
				payload := []byte(fmt.Sprintf("s%d-%d", seed, step))
				attr, err := eng.Create(ctx, dir, name, 0o644, payload)
				if err == nil {
					if oErr := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
						{Kind: djfs.DeltaCreateFile, Dir: dir, Name: name, ID: attr.ID, Attr: attr, Data: payload},
						{Kind: djfs.DeltaSetAttr, ID: dir, Attr: mustStatEngine(t, eng, dir)},
					}}); oErr != nil {
						t.Fatalf("seed %d step %d oracle create: %v", seed, step, oErr)
					}
				}
			case 2:
				if len(liveDirs) < 18 {
					attr, err := eng.Mkdir(ctx, dir, name, 0o755)
					if err == nil {
						liveDirs = append(liveDirs, attr.ID)
						if oErr := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
							{Kind: djfs.DeltaMkdir, Dir: dir, Name: name, ID: attr.ID, Attr: attr},
							{Kind: djfs.DeltaSetAttr, ID: dir, Attr: mustStatEngine(t, eng, dir)},
						}}); oErr != nil {
							t.Fatalf("seed %d step %d oracle mkdir: %v", seed, step, oErr)
						}
					}
				}
			case 3:
				payload := []byte(fmt.Sprintf("write-%d-%d", seed, step))
				attr, err := eng.Write(ctx, dir, name, payload)
				if err == nil {
					if oErr := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
						{Kind: djfs.DeltaWrite, Dir: dir, Name: name, ID: attr.ID, Attr: attr, Data: payload},
					}}); oErr != nil {
						t.Fatalf("seed %d step %d oracle write: %v", seed, step, oErr)
					}
				}
			case 4:
				perm := fs.FileMode(0o600)
				if step%2 == 0 {
					perm = 0o755
				}
				attr, err := eng.Chmod(ctx, dir, name, perm)
				if err == nil {
					if oErr := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
						{Kind: djfs.DeltaSetAttr, Dir: dir, Name: name, ID: attr.ID, Attr: attr},
					}}); oErr != nil {
						t.Fatalf("seed %d step %d oracle chmod: %v", seed, step, oErr)
					}
				}
			case 5:
				err := eng.Unlink(ctx, dir, name)
				if err == nil {
					if oErr := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
						{Kind: djfs.DeltaUnlink, Dir: dir, Name: name},
						{Kind: djfs.DeltaSetAttr, ID: dir, Attr: mustStatEngine(t, eng, dir)},
					}}); oErr != nil {
						t.Fatalf("seed %d step %d oracle unlink: %v", seed, step, oErr)
					}
				}
			case 6:
				preAttr, preErr := eng.Lookup(dir, name)
				err := eng.Rmdir(ctx, dir, name)
				if err == nil && preErr == nil {
					kept := liveDirs[:0]
					for _, id := range liveDirs {
						if id != preAttr.ID {
							kept = append(kept, id)
						}
					}
					liveDirs = kept
					if oErr := oracle.ApplyBatch(ctx, djfs.Batch{Ops: []djfs.DeltaOp{
						{Kind: djfs.DeltaRmdir, Dir: dir, Name: name, ID: preAttr.ID},
						{Kind: djfs.DeltaSetAttr, ID: dir, Attr: mustStatEngine(t, eng, dir)},
					}}); oErr != nil {
						t.Fatalf("seed %d step %d oracle rmdir: %v", seed, step, oErr)
					}
				}
			case 7, 8:
				dstDir := liveDirs[rng.IntN(len(liveDirs))]
				dstName := names[rng.IntN(len(names))]
				preSrc, preSrcErr := eng.Lookup(dir, name)
				preDst, preDstErr := eng.Lookup(dstDir, dstName)
				err := eng.Rename(ctx, dir, name, dstDir, dstName)
				if err == nil {
					if preSrcErr == nil && preDstErr == nil && preDst.IsDir() && preDst.ID != preSrc.ID {
						kept := liveDirs[:0]
						for _, id := range liveDirs {
							if id != preDst.ID {
								kept = append(kept, id)
							}
						}
						liveDirs = kept
					}
					ops := []djfs.DeltaOp{
						{Kind: djfs.DeltaRename, Dir: dir, Name: name, DstDir: dstDir, DstName: dstName},
						{Kind: djfs.DeltaSetAttr, ID: dir, Attr: mustStatEngine(t, eng, dir)},
					}
					if dstDir != dir {
						ops = append(ops, djfs.DeltaOp{Kind: djfs.DeltaSetAttr, ID: dstDir, Attr: mustStatEngine(t, eng, dstDir)})
					}
					if oErr := oracle.ApplyBatch(ctx, djfs.Batch{Ops: ops}); oErr != nil {
						t.Fatalf("seed %d step %d oracle rename (%d/%s -> %d/%s): %v", seed, step, dir, name, dstDir, dstName, oErr)
					}
				}
			case 9:
				if rng.IntN(2) == 0 {
					if err := eng.FsyncDir(ctx, dir); err != nil {
						t.Fatalf("seed %d step %d FsyncDir: %v", seed, step, err)
					}
					rec, err := djfs.RecoverFromTree(ctx, djfs.Config{Clock: clk}, published, j)
					if err != nil {
						t.Fatalf("seed %d step %d RecoverFromTree: %v", seed, step, err)
					}
					if invErr := rec.Snapshot().CheckInvariants(); invErr != nil {
						t.Fatalf("seed %d step %d recovered invariant: %v", seed, step, invErr)
					}
				} else {
					if err := eng.Publish(ctx); err != nil {
						t.Fatalf("seed %d step %d Publish: %v", seed, step, err)
					}
					assertTreesIdentical(t, fmt.Sprintf("seed=%d step=%d publish", seed, step), published, oracle)
				}
			}
		}

		if err := eng.Publish(ctx); err != nil {
			j.Close()
			t.Fatalf("seed %d final Publish: %v", seed, err)
		}
		assertTreesIdentical(t, fmt.Sprintf("seed=%d final", seed), published, oracle)
		assertTreesIdentical(t, fmt.Sprintf("seed=%d live", seed), eng.Snapshot(), oracle)
		_ = j.Close()
	}
}

func mustStatEngine(t testing.TB, eng *djfs.Engine, id djfs.InodeID) djfs.Attr {
	t.Helper()
	a, err := eng.Stat(id)
	if err != nil {
		t.Fatalf("Stat(%d): %v", id, err)
	}
	return a
}

func TestConcurrentMultiGoroutineStormIsRaceFree(t *testing.T) {
	ctx := context.Background()
	disk := crash.New()
	j := openCrashJournal(t, disk, 0)
	defer j.Close()

	target := djfs.NewMemTree(time.Now().UTC())
	eng, err := djfs.New(djfs.Config{
		Journal: j,
		Target:  target,
	})
	if err != nil {
		t.Fatal(err)
	}

	const workers = 12
	opsPerWorker := sized(80, 250)
	dirs := make([]djfs.InodeID, workers)
	for w := 0; w < workers; w++ {
		d, err := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("w%02d", w), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		dirs[w] = d.ID
	}
	sharedDir, err := eng.Mkdir(ctx, djfs.RootInode, "shared", 0o755)
	if err != nil {
		t.Fatal(err)
	}

	var stopReaders atomic.Bool
	var readerWG sync.WaitGroup
	for r := 0; r < 3; r++ {
		readerWG.Add(1)
		go func(rid int) {
			defer readerWG.Done()
			for !stopReaders.Load() {
				snap := eng.Snapshot()
				if err := snap.CheckInvariants(); err != nil {
					t.Errorf("reader %d snapshot invariant failed: %v", rid, err)
					return
				}
				_, _ = eng.ReadDir(sharedDir.ID)
				if rid == 0 {
					_ = eng.Publish(ctx)
				}
			}
		}(r)
	}

	var writerWG sync.WaitGroup
	for w := 0; w < workers; w++ {
		writerWG.Add(1)
		go func(wid int) {
			defer writerWG.Done()
			myDir := dirs[wid]
			payload := []byte(fmt.Sprintf("worker-%d-payload", wid))
			for i := 0; i < opsPerWorker; i++ {
				tmp := fmt.Sprintf("tmp_%d_%d", wid, i)
				fin := fmt.Sprintf("fin_%d_%d", wid, i%16)
				if _, err := eng.Create(ctx, myDir, tmp, 0o600, payload); err != nil {
					t.Errorf("w%d create: %v", wid, err)
					return
				}
				_, _ = eng.Chmod(ctx, myDir, tmp, 0o644)
				if i%3 == 0 {
					_ = eng.Unlink(ctx, myDir, tmp)
				} else if i%3 == 1 {
					_ = eng.Rename(ctx, myDir, tmp, myDir, fin)
				} else {
					shName := fmt.Sprintf("sh_%d_%d", wid, i%8)
					_ = eng.Rename(ctx, myDir, tmp, sharedDir.ID, shName)
				}
				if i%25 == 0 {
					_ = eng.FsyncDir(ctx, myDir)
				}
			}
		}(w)
	}

	writerWG.Wait()
	stopReaders.Store(true)
	readerWG.Wait()

	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	assertTreesIdentical(t, "concurrent-storm-final", target, eng.Snapshot())
}

func BenchmarkNpmInstallCreateChmodRename(b *testing.B) {
	ctx := context.Background()
	eng, err := djfs.New(djfs.Config{MaxDirtyOps: 1 << 24, MaxDirtyBytes: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	stg, _ := eng.Mkdir(ctx, djfs.RootInode, ".staging", 0o700)
	pkgDirs := make([]djfs.InodeID, 64)
	for i := range pkgDirs {
		d, _ := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("pkg-%02d", i), 0o755)
		pkgDirs[i] = d.ID
	}
	payload := bytes.Repeat([]byte("j"), 1024)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tmp := fmt.Sprintf("t%d.tmp", i)
		dst := fmt.Sprintf("f%d.js", i&255)
		pdir := pkgDirs[i&63]
		if _, err := eng.Create(ctx, stg.ID, tmp, 0o600, payload); err != nil {
			b.Fatal(err)
		}
		if _, err := eng.Chmod(ctx, stg.ID, tmp, 0o644); err != nil {
			b.Fatal(err)
		}
		if err := eng.Rename(ctx, stg.ID, tmp, pdir, dst); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := eng.FlushJournal(ctx); err != nil {
		b.Fatal(err)
	}
	st := eng.Stats()
	if st.JournalOps > 0 {
		b.ReportMetric(float64(st.RawOps)/float64(st.JournalOps), "coalesce_x")
	}
}

func BenchmarkScratchCreateUnlink(b *testing.B) {
	ctx := context.Background()
	eng, err := djfs.New(djfs.Config{MaxDirtyOps: 1 << 24, MaxDirtyBytes: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	dirs := make([]djfs.InodeID, 64)
	for i := range dirs {
		d, _ := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("d%02d", i), 0o755)
		dirs[i] = d.ID
	}
	payload := bytes.Repeat([]byte("s"), 4096)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := dirs[i&63]
		name := fmt.Sprintf("scr_%d.tmp", i)
		if _, err := eng.Create(ctx, d, name, 0o600, payload); err != nil {
			b.Fatal(err)
		}
		if err := eng.Unlink(ctx, d, name); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := eng.FlushJournal(ctx); err != nil {
		b.Fatal(err)
	}
	st := eng.Stats()
	if st.JournalOps > 0 {
		b.ReportMetric(float64(st.RawOps)/float64(st.JournalOps), "coalesce_x")
	}
}

func BenchmarkAtomicWriteTmpRename(b *testing.B) {
	ctx := context.Background()
	eng, err := djfs.New(djfs.Config{MaxDirtyOps: 1 << 24, MaxDirtyBytes: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	dirs := make([]djfs.InodeID, 64)
	for i := range dirs {
		d, _ := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("c%02d", i), 0o755)
		dirs[i] = d.ID
	}
	payload := bytes.Repeat([]byte("c"), 2048)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := dirs[i&63]
		tmp := fmt.Sprintf(".tmp.%d", i)
		dst := fmt.Sprintf("cfg_%d.json", (i>>6)&3)
		if _, err := eng.Create(ctx, d, tmp, 0o600, payload); err != nil {
			b.Fatal(err)
		}
		if err := eng.Rename(ctx, d, tmp, d, dst); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := eng.FlushJournal(ctx); err != nil {
		b.Fatal(err)
	}
	st := eng.Stats()
	if st.JournalOps > 0 {
		b.ReportMetric(float64(st.RawOps)/float64(st.JournalOps), "coalesce_x")
	}
}

func BenchmarkMDTestCreateStatDelete(b *testing.B) {
	ctx := context.Background()
	eng, err := djfs.New(djfs.Config{MaxDirtyOps: 1 << 24, MaxDirtyBytes: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	dirs := make([]djfs.InodeID, 64)
	for i := range dirs {
		d, _ := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("m%02d", i), 0o755)
		dirs[i] = d.ID
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := dirs[i&63]
		name := fmt.Sprintf("md_%d", i)
		if _, err := eng.Create(ctx, d, name, 0o644, nil); err != nil {
			b.Fatal(err)
		}
		if _, err := eng.Lookup(d, name); err != nil {
			b.Fatal(err)
		}
		if err := eng.Unlink(ctx, d, name); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVarmailConcurrentFsync(b *testing.B) {
	ctx := context.Background()
	j := openScratchJournal(b)
	eng, err := djfs.New(djfs.Config{Journal: j})
	if err != nil {
		b.Fatal(err)
	}
	dirs := make([]djfs.InodeID, 64)
	for i := range dirs {
		d, _ := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("u%02d", i), 0o755)
		dirs[i] = d.ID
	}
	if _, err := eng.FlushJournal(ctx); err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("m"), 4096)
	var seq atomic.Uint64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1)
			d := dirs[n&63]
			tmp := fmt.Sprintf("tmp_%d", n)
			cur := fmt.Sprintf("cur_%d", n)
			if _, err := eng.Create(ctx, d, tmp, 0o600, payload); err != nil {
				b.Fatal(err)
			}
			if err := eng.Rename(ctx, d, tmp, d, cur); err != nil {
				b.Fatal(err)
			}
			if err := eng.FsyncDir(ctx, d); err != nil {
				b.Fatal(err)
			}
			if err := eng.Unlink(ctx, d, cur); err != nil {
				b.Fatal(err)
			}
		}
	})
}
