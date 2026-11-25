package vfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// TestQualificationPosixCrashAndTornJournalRecovery verifies that every POSIX operation
// (Create, Mkdir, WriteAt, Link, Symlink, Mknod, Rename, RenameExchange, SetAttr, Punch,
// Remove, and open-unlinked orphans) survives a crash and replays cleanly even when a
// torn/corrupted write sits at the journal tail after the last synced record, and that
// corruption inside a durable record is never silently accepted.
func TestQualificationPosixCrashAndTornJournalRecovery(t *testing.T) {
	t.Run("torn-tail-recovers-all-synced-posix", func(t *testing.T) {
		jroot := t.TempDir()
		mem := storetest.NewMem()
		c := vfsAge(t)
		conf0 := cfg(mem, c, 0)
		conf0.Journal = jroot
		conf0.Disk = nil // real OS files so segment files exist on disk
		if err := vfs.Create(ctx, conf0); err != nil {
			t.Fatal(err)
		}
		conf1 := cfg(mem, c, 1)
		conf1.Journal = jroot
		conf1.Disk = nil
		conf1.Bound = time.Hour // keep everything unpublished in the journal until replay
		v, err := vfs.Attach(ctx, conf1)
		if err != nil {
			t.Fatal(err)
		}

		// Perform a rich mix of all POSIX operations.
		d1 := mkdir(t, v, vfs.RootInode, "dir1")
		d2 := mkdir(t, v, vfs.RootInode, "dir2")
		f1 := write(t, v, d1, "alpha", content(101, 64<<10))
		f2 := write(t, v, d1, "beta", content(102, 40<<10))
		write(t, v, d1, "gamma", content(103, 8<<10))

		// Hard link, symlink, FIFO mknod.
		if _, err := v.Link(ctx, f1.ID, d2, "alpha-link"); err != nil {
			t.Fatal(err)
		}
		if _, err := v.Symlink(ctx, d2, "sym", "../dir1/alpha"); err != nil {
			t.Fatal(err)
		}
		if _, err := v.Mknod(ctx, d2, "fifo", fs.ModeNamedPipe|0o640); err != nil {
			t.Fatal(err)
		}

		// Rename & RenameExchange.
		if err := v.Rename(ctx, d1, "beta", d2, "beta-moved", 0); err != nil {
			t.Fatal(err)
		}
		if err := v.Rename(ctx, d1, "alpha", d1, "gamma", vfs.RenameExchange); err != nil {
			t.Fatal(err)
		}

		// SetAttr (mode + uid/gid + timestamps).
		mode := fs.FileMode(0o750) | fs.ModeSetgid
		uid, gid := uint32(1001), uint32(2002)
		if _, err := v.SetAttr(ctx, f2.ID, vfs.Change{Mode: &mode, UID: &uid, GID: &gid}); err != nil {
			t.Fatal(err)
		}

		// Punch a hole in a chunked file.
		fh, err := v.Open(ctx, f1.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := fh.Punch(ctx, 8<<10, 16<<10); err != nil {
			t.Fatal(err)
		}
		fh.Close()

		// Open-then-unlink orphan write (must not leak into replayed namespace).
		orphIn := write(t, v, d1, "orphan", []byte("transient"))
		orphFH, err := v.Open(ctx, orphIn.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := v.Remove(ctx, d1, "orphan"); err != nil {
			t.Fatal(err)
		}
		if _, err := orphFH.WriteAt(ctx, []byte("still-open"), 0); err != nil {
			t.Fatal(err)
		}

		// Remove a regular file.
		write(t, v, d1, "temp", []byte("bye"))
		if err := v.Remove(ctx, d1, "temp"); err != nil {
			t.Fatal(err)
		}

		// Make all POSIX changes durable in the local journal (NOT published to S3).
		if err := v.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		check(t, v)
		wantSnap := snapshot(t, v)
		wantUsage := v.Usage()

		// Capture the synced journal segment bytes directly on disk.
		segs := findJournalSegments(t, jroot)
		if len(segs) == 0 {
			t.Fatal("expected at least one journal segment file on disk")
		}
		synced := map[string][]byte{}
		for _, p := range segs {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			synced[p] = b
		}

		// Close orphan and simulate crash (Close without Flush).
		orphFH.Close()
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
		// Ensure store still has nothing published yet.
		if got := snapshot(t, reopen(t, mem, c)); len(got) != 0 {
			t.Fatalf("store had %d paths before publication", len(got))
		}

		// Inject a torn/corrupted tail right after the last non-zero byte of the active segment.
		injected := false
		for _, p := range segs {
			b := bytes.Clone(synced[p])
			end := lastNonZero(b)
			if end > 0 && end+32 < len(b) {
				// Write a torn partial record header + garbage past the synced boundary.
				for i := end; i < end+32; i++ {
					b[i] = 0xDE
				}
				if err := os.WriteFile(p, b, 0o600); err != nil {
					t.Fatal(err)
				}
				injected = true
				break
			}
		}
		if !injected {
			t.Fatal("could not locate spare space after synced journal tail to inject torn write")
		}

		// Attach round 2 on the same node: must replay all synced POSIX ops, ignore torn tail, publish cleanly.
		conf2 := cfg(mem, c, 2)
		conf2.Journal = jroot
		conf2.Disk = nil
		w, err := vfs.Attach(ctx, conf2)
		if err != nil {
			t.Fatalf("Attach after torn journal tail failed: %v", err)
		}
		defer w.Close()

		rec := w.Recovery()
		if rec.Lost || rec.Replayed == 0 {
			t.Fatalf("unexpected recovery after torn tail: %+v", rec)
		}
		check(t, w)
		sameSnapshot(t, "replayed live volume", snapshot(t, w), wantSnap)
		ro := reopen(t, mem, c)
		check(t, ro)
		sameSnapshot(t, "published store view", snapshot(t, ro), wantSnap)
		if gotU := ro.Usage(); gotU.Files != wantUsage.Files {
			t.Fatalf("replayed usage files=%d want=%d", gotU.Files, wantUsage.Files)
		}
		// Verify punched range reads back as zeros and non-punched bytes survived.
		gotF1 := readAll(t, ro, f1.ID)
		if !bytes.Equal(gotF1[8<<10:24<<10], make([]byte, 16<<10)) {
			t.Fatal("punched range did not survive crash replay as zeros")
		}
		if !bytes.Equal(gotF1[:8<<10], content(101, 64<<10)[:8<<10]) {
			t.Fatal("bytes before punched range corrupted after crash replay")
		}
	})

	t.Run("mid-log-corruption-never-serves-corrupt-data", func(t *testing.T) {
		jroot := t.TempDir()
		mem := storetest.NewMem()
		c := vfsAge(t)
		conf0 := cfg(mem, c, 0)
		conf0.Journal = jroot
		conf0.Disk = nil
		if err := vfs.Create(ctx, conf0); err != nil {
			t.Fatal(err)
		}
		conf1 := cfg(mem, c, 1)
		conf1.Journal = jroot
		conf1.Disk = nil
		conf1.Bound = time.Hour
		v, err := vfs.Attach(ctx, conf1)
		if err != nil {
			t.Fatal(err)
		}
		write(t, v, vfs.RootInode, "committed", []byte("safe-before-crash"))
		flush(t, v)
		committedSnap := snapshot(t, reopen(t, mem, c))

		// Write two synced unpublished records, then corrupt bytes inside the active segment's payload.
		write(t, v, vfs.RootInode, "u1", content(501, 16<<10))
		write(t, v, vfs.RootInode, "u2", content(502, 16<<10))
		if err := v.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}

		segs := findJournalSegments(t, jroot)
		if len(segs) == 0 {
			t.Fatal("expected journal segment on disk")
		}
		// Flip a byte right inside the middle of the written area of the active segment.
		corrupted := false
		for _, p := range segs {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			end := lastNonZero(b)
			if end > 4096 {
				b[end/2] ^= 0xFF
				if err := os.WriteFile(p, b, 0o600); err != nil {
					t.Fatal(err)
				}
				corrupted = true
				break
			}
		}
		if !corrupted {
			t.Fatal("failed to corrupt active journal segment")
		}

		conf2 := cfg(mem, c, 2)
		conf2.Journal = jroot
		conf2.Disk = nil
		w, err := vfs.Attach(ctx, conf2)
		if err != nil {
			t.Fatalf("Attach on corrupted journal should fall back honestly rather than crash: %v", err)
		}
		defer w.Close()

		// Must explicitly report data loss (never silently claim full recovery).
		if rec := w.Recovery(); !rec.Lost || rec.Why == "" {
			t.Fatalf("corrupted journal must report Recovery.Lost=true with reason, got %+v", rec)
		}
		// Must preserve previously committed state cleanly and pass Check().
		check(t, w)
		got, err := w.Lookup(ctx, vfs.RootInode, "committed")
		if err != nil {
			t.Fatalf("previously committed file lost: %v", err)
		}
		mustEqual(t, "committed content", readAll(t, w, got.ID), []byte("safe-before-crash"))
		if _, ok := committedSnap["/committed"]; !ok {
			t.Fatal("committed snapshot missing file")
		}
	})
}

// TestQualificationHighLatencyAndOutageDurabilityContract verifies the end-to-end
// durability contract:
//  1. Local journal fsync stays fast under high S3 latency and NEVER claims remote durability.
//  2. S3 outage marks State.Degraded, applies backpressure (ErrFull), and preserves unpublished journal.
//  3. Recovery drains the backlog in order, clears Degraded, and Flush acts as a suspend/migration fence.
func TestQualificationHighLatencyAndOutageDurabilityContract(t *testing.T) {
	var (
		latencyNs atomic.Int64
		down      atomic.Bool
	)
	mem, c, v, _ := pair(t, func(b backend.Backend) backend.Backend {
		return &storetest.Hook{
			Backend: b,
			Before: func(op, key string) error {
				if op == "put" || op == "putver" {
					if d := time.Duration(latencyNs.Load()); d > 0 {
						time.Sleep(d)
					}
					if down.Load() {
						return errors.New("s3 outage")
					}
				}
				return nil
			},
		}
	}, func(conf *vfs.Config) {
		conf.JournalCapacity = 8 << 20
		conf.Dirty = 1 << 20
		conf.Bound = 2 * time.Second
	})

	// 1. High S3 latency (150ms per put): local WriteAt + Sync must NOT wait for S3,
	//    and State must honestly distinguish Durable (local) from Published (remote).
	latencyNs.Store(int64(150 * time.Millisecond))
	in, err := v.Create(ctx, vfs.RootInode, "fast-local", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	t0 := time.Now()
	if _, err := f.WriteAt(ctx, content(7, 32<<10), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	syncElapsed := time.Since(t0)
	if syncElapsed >= 100*time.Millisecond {
		t.Fatalf("local WriteAt+Sync took %v under 150ms S3 latency; must stay local-only", syncElapsed)
	}
	st := v.State()
	if st.Durable < st.Visible {
		t.Fatalf("expected Durable >= Visible after Sync, got %+v", st)
	}
	if st.Published >= st.Durable {
		t.Fatalf("must not claim remote durability immediately on local fsync under 150ms S3 latency: %+v", st)
	}

	// 2. S3 outage: write until backpressure returns ErrFull; verify Degraded + Publishing error.
	latencyNs.Store(0)
	down.Store(true)
	block := content(11, 256<<10)
	var model []byte
	model = append(model, content(7, 32<<10)...)
	var fullErr error
	for i := 0; i < 64; i++ {
		off := int64(len(model))
		if _, err := f.WriteAt(ctx, block, off); err != nil {
			fullErr = err
			break
		}
		model = append(model, block...)
	}
	if !errors.Is(fullErr, vfs.ErrFull) {
		t.Fatalf("expected ErrFull backpressure during S3 outage, got %v", fullErr)
	}
	st = v.State()
	if !st.Degraded || st.Publishing == nil || st.Backlog == 0 {
		t.Fatalf("expected Degraded=true with Publishing error and non-zero Backlog during outage, got %+v", st)
	}
	// Remote store must NOT have unpublished outage writes yet.
	if st.Published >= st.Visible {
		t.Fatalf("unpublished writes cannot be marked published during S3 outage: %+v", st)
	}

	// 3. Recover S3 and confirm Flush drain fence (suspend/migration contract).
	down.Store(false)
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush after S3 recovery failed: %v", err)
	}
	st = v.State()
	if st.Degraded || st.Publishing != nil || st.Published != st.Visible {
		t.Fatalf("expected healthy fully-published state after Flush, got %+v", st)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate migration to a brand-new host (fresh empty journal directory, higher fencing round).
	migratedCfg := cfg(mem, c, 2)
	migratedCfg.Journal = t.TempDir()
	v2, err := vfs.Attach(ctx, migratedCfg)
	if err != nil {
		t.Fatalf("Attach on destination host after Flush fence failed: %v", err)
	}
	defer v2.Close()
	check(t, v2)
	gotIn, err := v2.Lookup(ctx, vfs.RootInode, "fast-local")
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, "migrated file bytes", readAll(t, v2, gotIn.ID), model)
}

// TestQualificationConsistentSnapshotsUnderConcurrentPosix runs concurrent POSIX mutations
// (multi-piece writes, renames, exchanges, hard links, symlinks, setattr, punch, remove,
// sync, flush) while an auditor opens fresh read-only store snapshots and verifies that
// EVERY committed generation boundary passes Check() with zero torn writes and zero missing chunks.
func TestQualificationConsistentSnapshotsUnderConcurrentPosix(t *testing.T) {
	mem, c, v, _ := pair(t, nil, func(conf *vfs.Config) {
		conf.JournalCapacity = 16 << 20 // force multiple background generations and multi-piece writes
	})
	dA := mkdir(t, v, vfs.RootInode, "a")
	dB := mkdir(t, v, vfs.RootInode, "b")
	flush(t, v)

	workers := sized(4, 6)
	iters := sized(6, 20)
	stopAudit := make(chan struct{})
	auditDone := make(chan error, 1)
	var snapshotsChecked atomic.Int64

	// Auditor continuously opens fresh snapshots from the store and checks full invariants.
	go func() {
		defer close(auditDone)
		for {
			select {
			case <-stopAudit:
				return
			default:
			}
			ro, err := vfs.Open(ctx, cfg(mem, c, 0))
			if err != nil {
				auditDone <- fmt.Errorf("open snapshot: %w", err)
				return
			}
			if err := ro.Check(ctx); err != nil {
				auditDone <- fmt.Errorf("snapshot Check failed: %w", err)
				return
			}
			// Verify every multi-piece data file in dA is either empty or byte-exact whole.
			ents, err := ro.ReadDir(ctx, dA)
			if err != nil {
				auditDone <- fmt.Errorf("readdir dA: %w", err)
				return
			}
			for _, e := range ents {
				if !e.Mode.IsRegular() || !strings.HasPrefix(e.Name, "mp-") {
					continue
				}
				in, err := ro.Stat(ctx, e.ID)
				if err != nil {
					auditDone <- fmt.Errorf("stat %s: %w", e.Name, err)
					return
				}
				if in.Size == 0 {
					continue
				}
				// Every multi-piece file is written with a single WriteAt of 600 KiB of one repeated marker byte.
				const wantSize = 600 << 10
				if in.Size != wantSize {
					auditDone <- fmt.Errorf("torn multi-piece file %s: size=%d want=0 or %d", e.Name, in.Size, wantSize)
					return
				}
				fh, err := ro.Open(ctx, e.ID)
				if err != nil {
					auditDone <- fmt.Errorf("open %s: %w", e.Name, err)
					return
				}
				buf := make([]byte, wantSize)
				n, rerr := fh.ReadAt(ctx, buf, 0)
				fh.Close()
				if rerr != nil || n != wantSize {
					auditDone <- fmt.Errorf("read %s: n=%d err=%v", e.Name, n, rerr)
					return
				}
				mark := buf[0]
				if mark == 0 || buf[wantSize-1] != mark || buf[wantSize/2] != mark {
					auditDone <- fmt.Errorf("torn content in %s: first=%x mid=%x last=%x", e.Name, buf[0], buf[wantSize/2], buf[wantSize-1])
					return
				}
			}
			snapshotsChecked.Add(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w+1), 4242))
			for i := range iters {
				mpName := fmt.Sprintf("mp-%d-%d", w, i)
				in, err := v.Create(ctx, dA, mpName, 0o644)
				if err != nil {
					t.Errorf("create %s: %v", mpName, err)
					return
				}
				fh, err := v.Open(ctx, in.ID)
				if err != nil {
					t.Errorf("open %s: %v", mpName, err)
					return
				}
				// 600 KiB exceeds 16 MiB journal's 512 KiB record size -> exercises atomic multi-piece WriteAt!
				buf := bytes.Repeat([]byte{byte(1 + (w*31+i)%250)}, 600<<10)
				if _, err := fh.WriteAt(ctx, buf, 0); err != nil {
					fh.Close()
					t.Errorf("write %s: %v", mpName, err)
					return
				}
				fh.Close()

				// Hard link, symlink, rename, setattr, punch, remove on worker-owned names in dB.
				p1 := fmt.Sprintf("w%d-f%d", w, i)
				p2 := fmt.Sprintf("w%d-g%d", w, i)
				fIn := write(t, v, dB, p1, content(uint64(w*100+i), 24<<10))
				if _, err := v.Link(ctx, fIn.ID, dB, p2); err != nil {
					t.Errorf("link: %v", err)
					return
				}
				if _, err := v.Symlink(ctx, dB, fmt.Sprintf("w%d-s%d", w, i), p2); err != nil {
					t.Errorf("symlink: %v", err)
					return
				}
				f2, err := v.Open(ctx, fIn.ID)
				if err != nil {
					t.Errorf("open fIn: %v", err)
					return
				}
				if err := f2.Punch(ctx, 4096, 8192); err != nil {
					f2.Close()
					t.Errorf("punch: %v", err)
					return
				}
				f2.Close()
				if err := v.Rename(ctx, dB, p1, dB, fmt.Sprintf("w%d-r%d", w, i), 0); err != nil {
					t.Errorf("rename: %v", err)
					return
				}
				m := fs.FileMode(0o640)
				if _, err := v.SetAttr(ctx, fIn.ID, vfs.Change{Mode: &m}); err != nil {
					t.Errorf("setattr: %v", err)
					return
				}
				if i > 0 && rng.IntN(2) == 0 {
					_ = v.Remove(ctx, dB, fmt.Sprintf("w%d-g%d", w, i-1))
				}
				if rng.IntN(3) == 0 {
					if err := v.Flush(ctx); err != nil {
						t.Errorf("flush: %v", err)
						return
					}
				} else {
					if err := v.Sync(ctx); err != nil {
						t.Errorf("sync: %v", err)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(stopAudit)
	if err := <-auditDone; err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}
	flush(t, v)
	check(t, v)
	ro := reopen(t, mem, c)
	check(t, ro)
	t.Logf("verified %d live store snapshots under concurrent POSIX load", snapshotsChecked.Load())
}

// TestQualificationTenantIsolation verifies that two tenants sharing the same journal root
// and backend infrastructure cannot decrypt, substitute, or corrupt each other's heads,
// shards, packs, extension objects, or local journals.
func TestQualificationTenantIsolation(t *testing.T) {
	jroot := t.TempDir()
	memA, memB := storetest.NewMem(), storetest.NewMem()
	keyA, keyB := vfsAge(t), vfsAge(t)

	mkVol := func(mem *storetest.Mem, c vfs.Cipher) *vfs.Volume {
		c0 := cfg(mem, c, 0)
		c0.Journal = jroot
		if err := vfs.Create(ctx, c0); err != nil {
			t.Fatal(err)
		}
		c1 := cfg(mem, c, 1)
		c1.Journal = jroot
		v, err := vfs.Attach(ctx, c1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { v.Close() })
		return v
	}

	volA := mkVol(memA, keyA)
	volB := mkVol(memB, keyB)

	write(t, volA, vfs.RootInode, "secret-a", content(9001, 128<<10))
	if err := volA.PutExt(ctx, "tenant-meta", []byte("k1"), []byte("a-secret")); err != nil {
		t.Fatal(err)
	}
	flush(t, volA)

	write(t, volB, vfs.RootInode, "secret-b", content(9002, 128<<10))
	if err := volB.PutExt(ctx, "tenant-meta", []byte("k1"), []byte("b-secret")); err != nil {
		t.Fatal(err)
	}
	flush(t, volB)

	// 1. Separate journal directories under shared root (one directory per unique volumeID).
	entries, err := os.ReadDir(jroot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected distinct per-volume journal directories, found %d", len(entries))
	}

	// 2. Tenant B's cipher cannot open Tenant A's store (and vice versa).
	if _, err := vfs.Open(ctx, cfg(memA, keyB, 0)); !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("opening tenant A with tenant B key = %v, want ErrUnknownKey", err)
	}
	if _, err := vfs.Open(ctx, cfg(memB, keyA, 0)); !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("opening tenant B with tenant A key = %v, want ErrUnknownKey", err)
	}

	// 3. Even with the SAME cipher key (e.g., two volumes under one org KMS key),
	//    volume-bound AAD rejects cross-volume Head, shard, pack, and extension substitution.
	memC := storetest.NewMem()
	volC := mkVol(memC, keyA) // same keyA as volA, different volumeID!
	write(t, volC, vfs.RootInode, "secret-c", content(9003, 128<<10))
	if err := volC.PutExt(ctx, "tenant-meta", []byte("k1"), []byte("c-secret")); err != nil {
		t.Fatal(err)
	}
	flush(t, volC)

	// Extension keys for identical (name, key) MUST differ across volumes (seeded per-volume).
	extKeyA, err := volA.ExtKey("tenant-meta", []byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	extKeyC, err := volC.ExtKey("tenant-meta", []byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if extKeyA == extKeyC {
		t.Fatalf("extension keys collided across two volumes with same cipher: %s", extKeyA)
	}

	// Copying volC's objects to a different store/prefix MUST be refused by place-bound Head AAD.
	dumpA := memA.Dump()
	otherPlace := storetest.NewMem()
	for k, val := range memC.Dump() {
		if err := otherPlace.Put(ctx, k, val); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := vfs.Open(ctx, cfg(otherPlace, keyA, 0)); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("opening copied volume at another backend place = %v, want ErrCorrupt", err)
	}

	// Substituting volA's extension object under volC's extension key in volC's own store
	// MUST fail unseal (volume-bound AAD even with identical cipher key).
	if err := memC.Put(ctx, extKeyC, dumpA[extKeyA]); err != nil {
		t.Fatal(err)
	}
	roC, err := vfs.Open(ctx, cfg(memC, keyA, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roC.GetExt(ctx, "tenant-meta", []byte("k1")); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("cross-volume extension substitution = %v, want ErrCorrupt", err)
	}
}

func findJournalSegments(t *testing.T, root string) []string {
	t.Helper()
	var segs []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		// Journal segments are numeric/hex segment files inside r<round>/ (not .key, .end, or lock).
		if strings.HasSuffix(name, ".key") || strings.HasSuffix(name, ".end") || name == "lock" || strings.HasPrefix(name, ".") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Size() >= 4096 {
			segs = append(segs, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return segs
}

func lastNonZero(b []byte) int {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0 {
			return i + 1
		}
	}
	return 0
}
