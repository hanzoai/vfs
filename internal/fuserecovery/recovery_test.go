package fuserecovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

func setupSupervisor(t *testing.T, mode CheckpointMode) (*Supervisor, *OSBackingStore, string) {
	t.Helper()
	base := t.TempDir()
	backingDir := filepath.Join(base, "backing")
	stateDir := filepath.Join(base, "state")

	backing, err := NewOSBackingStore(backingDir)
	if err != nil {
		t.Fatalf("NewOSBackingStore: %v", err)
	}
	sup, err := NewSupervisor(SupervisorConfig{
		SessionID:      "sess-test-1",
		MountPath:      "/mnt/hanzo-test",
		StateDir:       stateDir,
		Backing:        backing,
		CheckpointMode: mode,
	})
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	t.Cleanup(func() {
		_ = sup.Close()
	})
	return sup, backing, stateDir
}

func TestStateStoreCRC32CAndCorruptionDetection(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	store, err := NewStateStore(dir)
	if err != nil {
		t.Fatalf("NewStateStore: %v", err)
	}

	st := NewSessionState("sess-crc", "/mnt/vfs")
	st.Inodes[2] = &InodeRecord{Inode: 2, Path: "a.txt", Generation: 1, NLookup: 3, OwnerTenant: "t1", Size: 42}
	if err := store.Save(st, true); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := store.Load()
	if err != nil {
		t.Fatalf("Load valid: %v", err)
	}
	if got.SessionID != "sess-crc" || got.Inodes[2].NLookup != 3 {
		t.Fatalf("loaded state mismatch: %+v", got)
	}

	statePath, err := store.StatePath()
	if err != nil {
		t.Fatalf("StatePath: %v", err)
	}
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// 1. Single-bit flip in payload must fail CRC32C.
	flipped := append([]byte(nil), raw...)
	flipped[len(flipped)-3] ^= 0x01
	if err := os.WriteFile(statePath, flipped, 0o600); err != nil {
		t.Fatalf("write flipped: %v", err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("expected ErrSessionCorrupt on payload bit flip, got %v", err)
	}

	// 2. Single-bit flip in header length must fail.
	badHdr := append([]byte(nil), raw...)
	badHdr[19] ^= 0x04
	if err := os.WriteFile(statePath, badHdr, 0o600); err != nil {
		t.Fatalf("write badHdr: %v", err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("expected ErrSessionCorrupt on header corruption, got %v", err)
	}

	// 3. Truncated file must fail.
	if err := os.WriteFile(statePath, raw[:10], 0o600); err != nil {
		t.Fatalf("write truncated: %v", err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("expected ErrSessionCorrupt on truncation, got %v", err)
	}
}

func TestSymlinkConfinementRejectsEscape(t *testing.T) {
	base := t.TempDir()
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "confined")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}

	// Symlink directory inside confined root pointing outside.
	linkDir := filepath.Join(root, "escape")
	if err := os.Symlink(outside, linkDir); err != nil {
		t.Fatal(err)
	}
	if _, err := ConfinePath(root, "escape/secret.bin"); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("expected ErrPathEscape for symlink dir, got %v", err)
	}

	// Parent traversal must fail.
	if _, err := ConfinePath(root, "../outside/secret.bin"); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("expected ErrPathEscape for .. traversal, got %v", err)
	}

	// Absolute path must fail.
	if _, err := ConfinePath(root, "/etc/passwd"); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("expected ErrPathEscape for abs path, got %v", err)
	}

	// Symlinked state directory itself must fail.
	symState := filepath.Join(base, "symstate")
	if err := os.Symlink(outside, symState); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStateStore(symState); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("expected ErrPathEscape for symlinked state dir, got %v", err)
	}

	// Symlinked state file inside valid state dir must fail Save and Load.
	validState := filepath.Join(base, "realstate")
	store, err := NewStateStore(validState)
	if err != nil {
		t.Fatal(err)
	}
	evilTarget := filepath.Join(outside, "pwned")
	if err := os.Symlink(evilTarget, filepath.Join(validState, stateFileName)); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(NewSessionState("s", "/m"), true); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("expected ErrPathEscape saving over symlinked state file, got %v", err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("expected ErrPathEscape loading symlinked state file, got %v", err)
	}
}

func TestTransparentRecoveryRegularReadWriteAndUnflushedDirtyWrites(t *testing.T) {
	ctx := context.Background()
	sup, _, _ := setupSupervisor(t, CheckpointSync)

	d1, err := sup.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	ino, h, err := d1.Create(1, "tenant-a", "work/data.bin", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	syncedPayload := []byte("durable-acknowledged-payload-v1")
	if _, _, err := d1.Write(2, "tenant-a", h.Handle, 0, syncedPayload); err != nil {
		t.Fatalf("Write synced: %v", err)
	}
	barrier, err := d1.Fsync(3, "tenant-a", h.Handle)
	if err != nil {
		t.Fatalf("Fsync: %v", err)
	}
	if barrier.DurableSize != uint64(len(syncedPayload)) {
		t.Fatalf("barrier size = %d, want %d", barrier.DurableSize, len(syncedPayload))
	}

	// Stage an unacknowledged dirty write (NOT fsynced) — read-your-own-writes works before crash.
	dirtyPayload := []byte("-UNSYNCED-TAIL")
	if _, _, err := d1.Write(4, "tenant-a", h.Handle, int64(len(syncedPayload)), dirtyPayload); err != nil {
		t.Fatalf("Write dirty: %v", err)
	}
	beforeCrash, err := d1.Read("tenant-a", h.Handle, 0, 128)
	if err != nil {
		t.Fatalf("Read before crash: %v", err)
	}
	if !bytes.Equal(beforeCrash, append(append([]byte(nil), syncedPayload...), dirtyPayload...)) {
		t.Fatalf("read-your-own-writes mismatch before crash: %q", beforeCrash)
	}

	// Inject abrupt SIGKILL process crash.
	sup.InjectProcessCrash()
	if d1.OpenFDCount() != 0 {
		t.Fatalf("expected 0 open FDs in dead daemon, got %d", d1.OpenFDCount())
	}
	if !sup.Channel().MountAlive() {
		t.Fatal("mount must remain alive when supervisor holds /dev/fuse fd")
	}
	if sup.Channel().UnmountCount() != 0 {
		t.Fatalf("expected 0 unmounts, got %d", sup.Channel().UnmountCount())
	}

	// Enqueue a FUSE request while daemon is crashed (simulating kernel /dev/fuse queue).
	postCrashAppend := []byte("-QUEUED")
	if err := sup.Channel().EnqueueBlocked(PendingRequest{
		ReqID:  5,
		Op:     "write",
		Tenant: "tenant-a",
		Handle: h.Handle,
		Offset: int64(len(syncedPayload)),
		Data:   postCrashAppend,
	}); err != nil {
		t.Fatalf("EnqueueBlocked: %v", err)
	}

	// Transparent Ananke recovery.
	d2, rep, err := sup.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rep.Generation != 2 || rep.HandlesRestored != 1 || rep.BarriersVerified != 1 || rep.ReplayedBlocked != 1 {
		t.Fatalf("unexpected recovery report: %+v", rep)
	}
	if d2.SessionID() != d1.SessionID() {
		t.Fatalf("session ID changed across transparent recovery: %q != %q", d2.SessionID(), d1.SessionID())
	}

	// Same open handle fh works seamlessly! Unsynced dirty write was dropped,
	// acknowledged fsync bytes are 100% preserved, and queued post-crash write applied!
	got, err := d2.Read("tenant-a", h.Handle, 0, 128)
	if err != nil {
		t.Fatalf("Read after recovery on same handle: %v", err)
	}
	want := append(append([]byte(nil), syncedPayload...), postCrashAppend...)
	if !bytes.Equal(got, want) {
		t.Fatalf("post-recovery content = %q, want %q", got, want)
	}

	// Stat on same inode & generation succeeds.
	st, err := d2.Stat("tenant-a", ino.Inode, ino.Generation)
	if err != nil {
		t.Fatalf("Stat after recovery: %v", err)
	}
	if st.Size != uint64(len(want)) {
		t.Fatalf("stat size = %d, want %d", st.Size, len(want))
	}

	// Release handle -> zero leaked FDs.
	if err := d2.Release("tenant-a", h.Handle); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if d2.OpenFDCount() != 0 {
		t.Fatalf("expected 0 open FDs after release, got %d", d2.OpenFDCount())
	}
}

func TestUnlinkedOpenOrphanSurvivesCrashAndCleansUpOnRelease(t *testing.T) {
	ctx := context.Background()
	sup, backing, _ := setupSupervisor(t, CheckpointSync)

	d1, err := sup.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ino, h, err := d1.Create(10, "tenant-a", "tmp/ephemeral.log", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		t.Fatal(err)
	}
	payload1 := []byte("orphan-part-1;")
	if _, _, err := d1.Write(11, "tenant-a", h.Handle, 0, payload1); err != nil {
		t.Fatal(err)
	}
	if _, err := d1.Fsync(12, "tenant-a", h.Handle); err != nil {
		t.Fatal(err)
	}

	// Unlink while handle is still open!
	if err := d1.Unlink(13, "tenant-a", "tmp/ephemeral.log"); err != nil {
		t.Fatalf("Unlink open file: %v", err)
	}
	// Lookup by path must now return ENOENT.
	if _, err := d1.Lookup("tenant-a", "tmp/ephemeral.log"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unlinked file, got %v", err)
	}

	// Abrupt crash & transparent recovery.
	sup.InjectProcessCrash()
	d2, rep, err := sup.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover with orphan: %v", err)
	}
	if rep.OrphansRestored != 1 || rep.HandlesRestored != 1 {
		t.Fatalf("expected 1 orphan & 1 handle restored, got %+v", rep)
	}

	// Existing handle continues to read, write, and fsync the orphan file!
	got1, err := d2.Read("tenant-a", h.Handle, 0, 64)
	if err != nil || !bytes.Equal(got1, payload1) {
		t.Fatalf("orphan read after crash = %q, err=%v", got1, err)
	}
	payload2 := []byte("orphan-part-2!")
	if _, _, err := d2.Write(14, "tenant-a", h.Handle, int64(len(payload1)), payload2); err != nil {
		t.Fatalf("orphan write after crash: %v", err)
	}
	if _, err := d2.Fsync(15, "tenant-a", h.Handle); err != nil {
		t.Fatalf("orphan fsync after crash: %v", err)
	}

	// Second crash! Orphan must still survive until Release().
	sup.InjectProcessCrash()
	d3, rep3, err := sup.Recover(ctx)
	if err != nil {
		t.Fatalf("second recover: %v", err)
	}
	if rep3.OrphansRestored != 1 {
		t.Fatalf("expected orphan preserved across 2nd crash, got %+v", rep3)
	}
	got2, err := d3.Read("tenant-a", h.Handle, 0, 64)
	want := append(append([]byte(nil), payload1...), payload2...)
	if err != nil || !bytes.Equal(got2, want) {
		t.Fatalf("orphan read after 2nd crash = %q, want %q, err=%v", got2, want, err)
	}

	// Release final handle -> orphan file must be deleted from .ananke-orphans/
	// and subsequent Stat on old inode returns ESTALE!
	if err := d3.Release("tenant-a", h.Handle); err != nil {
		t.Fatalf("Release orphan: %v", err)
	}
	if _, err := d3.Stat("tenant-a", ino.Inode, ino.Generation); !errors.Is(err, ErrStaleInode) || Errno(err) != syscall.ESTALE {
		t.Fatalf("expected ErrStaleInode/ESTALE after orphan release, got %v", err)
	}
	orphanEntries, err := os.ReadDir(filepath.Join(backing.Root(), orphanDirName))
	if err != nil {
		t.Fatal(err)
	}
	if len(orphanEntries) != 0 {
		t.Fatalf("expected .ananke-orphans empty after release, found %d entries", len(orphanEntries))
	}
}

func TestStaleInodeReturnsESTALE(t *testing.T) {
	ctx := context.Background()
	sup, backing, _ := setupSupervisor(t, CheckpointSync)

	d1, err := sup.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ino, h, err := d1.Create(1, "tenant-a", "stale.txt", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d1.Write(2, "tenant-a", h.Handle, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := d1.Fsync(3, "tenant-a", h.Handle); err != nil {
		t.Fatal(err)
	}

	// Wrong generation must immediately return ESTALE.
	if _, err := d1.Stat("tenant-a", ino.Inode, ino.Generation+99); !errors.Is(err, ErrStaleInode) || Errno(err) != syscall.ESTALE {
		t.Fatalf("expected ESTALE on generation mismatch, got %v", err)
	}

	// Simulate external backing file deletion while daemon is crashed.
	sup.InjectProcessCrash()
	if err := backing.UnlinkFile("stale.txt"); err != nil {
		t.Fatal(err)
	}

	d2, rep, err := sup.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover with externally removed file: %v", err)
	}
	if rep.StaleHandles != 1 {
		t.Fatalf("expected 1 stale handle detected, got %+v", rep)
	}
	if _, err := d2.Read("tenant-a", h.Handle, 0, 16); !errors.Is(err, ErrStaleInode) || Errno(err) != syscall.ESTALE {
		t.Fatalf("expected ESTALE reading stale handle, got %v", err)
	}
	if _, _, err := d2.Write(4, "tenant-a", h.Handle, 0, []byte("x")); !errors.Is(err, ErrStaleInode) {
		t.Fatalf("expected ESTALE writing stale handle, got %v", err)
	}
}

func TestIdempotentAppendAndCreateRetryDeduplicationAcrossCrash(t *testing.T) {
	ctx := context.Background()
	sup, _, _ := setupSupervisor(t, CheckpointSync)

	d1, err := sup.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// O_EXCL create with reqID=100.
	ino, h, err := d1.Create(100, "tenant-a", "append.log", 0o644, uint32(os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND|syscall.O_SYNC))
	if err != nil {
		t.Fatal(err)
	}
	// Append with O_SYNC + reqID=101 (simulates commit succeeded just before SIGKILL prior to reply).
	if n, dedup, err := d1.Write(101, "tenant-a", h.Handle, 0, []byte("LINE1\n")); err != nil || n != 6 || dedup {
		t.Fatalf("first append: n=%d dedup=%v err=%v", n, dedup, err)
	}

	sup.InjectProcessCrash()
	d2, _, err := sup.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Retried O_EXCL create with same reqID=100 must NOT fail with EEXIST!
	inoRetry, hRetry, err := d2.Create(100, "tenant-a", "append.log", 0o644, uint32(os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND|syscall.O_SYNC))
	if err != nil {
		t.Fatalf("retried create reqID=100 failed: %v", err)
	}
	if inoRetry.Inode != ino.Inode || hRetry.Handle != h.Handle {
		t.Fatalf("retried create returned different ids: (%d,%d) vs (%d,%d)", inoRetry.Inode, hRetry.Handle, ino.Inode, h.Handle)
	}

	// Retried O_APPEND write with same reqID=101 must be deduplicated (no double append!).
	n, dedup, err := d2.Write(101, "tenant-a", h.Handle, 0, []byte("LINE1\n"))
	if err != nil || !dedup || n != 6 {
		t.Fatalf("expected deduplicated append retry, got n=%d dedup=%v err=%v", n, dedup, err)
	}
	// New reqID=102 appends normally.
	if _, dedup2, err := d2.Write(102, "tenant-a", h.Handle, 0, []byte("LINE2\n")); err != nil || dedup2 {
		t.Fatalf("second distinct append failed: dedup=%v err=%v", dedup2, err)
	}

	got, err := d2.Read("tenant-a", h.Handle, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "LINE1\nLINE2\n" {
		t.Fatalf("append log = %q, want %q", string(got), "LINE1\nLINE2\n")
	}
}

func TestZeroLossAcknowledgedFsyncAcross120Crashes(t *testing.T) {
	ctx := context.Background()
	sup, _, _ := setupSupervisor(t, CheckpointBuffered)

	d, err := sup.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, h, err := d.Create(1, "tenant-durability", "ledger.bin", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		t.Fatal(err)
	}

	const crashes = 120
	var expected bytes.Buffer
	var reqID uint64 = 10

	for i := 0; i < crashes; i++ {
		rec := []byte(fmt.Sprintf("record-%04d;", i))
		off := int64(expected.Len())
		reqID++
		if _, _, err := d.Write(reqID, "tenant-durability", h.Handle, off, rec); err != nil {
			t.Fatalf("crash %d write: %v", i, err)
		}
		reqID++
		barrier, err := d.Fsync(reqID, "tenant-durability", h.Handle)
		if err != nil {
			t.Fatalf("crash %d fsync: %v", i, err)
		}
		expected.Write(rec)
		if barrier.DurableSize != uint64(expected.Len()) {
			t.Fatalf("crash %d barrier size=%d want=%d", i, barrier.DurableSize, expected.Len())
		}

		// Also stage an un-fsynced dirty write right before SIGKILL to verify
		// it does NOT corrupt the previously acknowledged fsync prefix.
		reqID++
		_, _, _ = d.Write(reqID, "tenant-durability", h.Handle, int64(expected.Len()), []byte("DROPME"))

		sup.InjectProcessCrash()
		d, _, err = sup.Recover(ctx)
		if err != nil {
			t.Fatalf("crash %d recover: %v", i, err)
		}

		got, err := d.Read("tenant-durability", h.Handle, 0, expected.Len()+64)
		if err != nil {
			t.Fatalf("crash %d read: %v", i, err)
		}
		if !bytes.Equal(got, expected.Bytes()) {
			t.Fatalf("crash %d data loss or corruption: got len=%d want len=%d", i, len(got), expected.Len())
		}
	}

	if sup.Channel().UnmountCount() != 0 {
		t.Fatalf("expected 0 unmounts across %d crashes, got %d", crashes, sup.Channel().UnmountCount())
	}
	if err := d.Release("tenant-durability", h.Handle); err != nil {
		t.Fatal(err)
	}
	if d.OpenFDCount() != 0 {
		t.Fatalf("FD leak detected: %d open FDs remain", d.OpenFDCount())
	}
}

func TestConcurrentMultiTenantIsolationAcrossRestarts(t *testing.T) {
	ctx := context.Background()
	sup, _, _ := setupSupervisor(t, CheckpointBuffered)

	d, err := sup.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	inoA, hA, err := d.Create(1, "tenant-alpha", "alpha/secret.txt", 0o600, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		t.Fatal(err)
	}
	inoB, hB, err := d.Create(2, "tenant-beta", "beta/secret.txt", 0o600, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		t.Fatal(err)
	}

	// Cross-tenant access must be rejected BEFORE crash.
	if _, err := d.Read("tenant-beta", hA.Handle, 0, 16); !errors.Is(err, ErrTenantMismatch) || Errno(err) != syscall.EACCES {
		t.Fatalf("expected EACCES cross-tenant read before crash, got %v", err)
	}
	if _, err := d.Lookup("tenant-alpha", "beta/secret.txt"); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch cross-tenant lookup, got %v", err)
	}

	var (
		mu         sync.RWMutex
		active     = d
		reqSeq     atomic.Uint64
		ackA, ackB atomic.Int64
	)
	reqSeq.Store(100)

	// Concurrent workers for tenant A and tenant B while supervisor injects crashes.
	var wg sync.WaitGroup
	worker := func(tenant string, fh uint64, ackCounter *atomic.Int64, otherFH uint64, otherIno uint64) {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			mu.RLock()
			cur := active
			// Verify cross-tenant isolation continuously.
			if _, err := cur.Read(tenant, otherFH, 0, 8); err != nil && !errors.Is(err, ErrDaemonCrashed) && !errors.Is(err, ErrTenantMismatch) {
				t.Errorf("tenant %s unexpected cross-tenant read err: %v", tenant, err)
			}
			if _, err := cur.Stat(tenant, otherIno, 0); err != nil && !errors.Is(err, ErrDaemonCrashed) && !errors.Is(err, ErrTenantMismatch) {
				t.Errorf("tenant %s unexpected cross-tenant stat err: %v", tenant, err)
			}
			// Append and fsync own file.
			off := ackCounter.Load()
			payload := []byte(fmt.Sprintf("%s-%03d;", tenant, i))
			_, _, werr := cur.Write(reqSeq.Add(1), tenant, fh, off, payload)
			if werr == nil {
				if _, ferr := cur.Fsync(reqSeq.Add(1), tenant, fh); ferr == nil {
					ackCounter.Store(off + int64(len(payload)))
				}
			}
			mu.RUnlock()
		}
	}

	wg.Add(2)
	go worker("tenant-alpha", hA.Handle, &ackA, hB.Handle, inoB.Inode)
	go worker("tenant-beta", hB.Handle, &ackB, hA.Handle, inoA.Inode)

	// Concurrently inject 5 crashes and recover.
	for c := 0; c < 5; c++ {
		mu.Lock()
		sup.InjectProcessCrash()
		nd, _, rerr := sup.Recover(ctx)
		if rerr != nil {
			mu.Unlock()
			t.Fatalf("concurrent recover %d: %v", c, rerr)
		}
		active = nd
		mu.Unlock()
	}
	wg.Wait()

	// Cross-tenant access MUST still be rejected AFTER all restarts.
	if _, err := active.Read("tenant-beta", hA.Handle, 0, 16); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch cross-tenant read after restarts, got %v", err)
	}
	if err := active.Unlink(reqSeq.Add(1), "tenant-alpha", "beta/secret.txt"); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch cross-tenant unlink after restarts, got %v", err)
	}

	// Every acknowledged fsync byte for both tenants must be readable.
	gotA, err := active.Read("tenant-alpha", hA.Handle, 0, int(ackA.Load())+16)
	if err != nil || int64(len(gotA)) < ackA.Load() {
		t.Fatalf("tenant-alpha lost acknowledged bytes: got=%d ack=%d err=%v", len(gotA), ackA.Load(), err)
	}
	gotB, err := active.Read("tenant-beta", hB.Handle, 0, int(ackB.Load())+16)
	if err != nil || int64(len(gotB)) < ackB.Load() {
		t.Fatalf("tenant-beta lost acknowledged bytes: got=%d ack=%d err=%v", len(gotB), ackB.Load(), err)
	}
}

func TestNegativeRegimesKernelCrashAndHandoffUnavailable(t *testing.T) {
	ctx := context.Background()

	t.Run("kernel_crash_requires_cold_remount", func(t *testing.T) {
		sup, _, _ := setupSupervisor(t, CheckpointSync)
		d1, err := sup.Start(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, h, err := d1.Create(1, "tenant-a", "file.bin", 0o644, uint32(os.O_RDWR|os.O_CREATE))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := d1.Write(2, "tenant-a", h.Handle, 0, []byte("persisted")); err != nil {
			t.Fatal(err)
		}
		if _, err := d1.Fsync(3, "tenant-a", h.Handle); err != nil {
			t.Fatal(err)
		}

		// Simulate full kernel/host crash (not process-only crash).
		sup.InjectKernelCrash()
		if _, _, err := sup.Recover(ctx); !errors.Is(err, ErrHandoffUnavailable) {
			t.Fatalf("expected ErrHandoffUnavailable after kernel crash, got %v", err)
		}
		tel := sup.Metrics()
		if tel.DegradedFallbacks != 1 || tel.ColdRemountsRequired != 1 {
			t.Fatalf("expected degraded fallback telemetry, got %+v", tel)
		}

		// Explicit degraded fallback to cold remount recovers durable files by path,
		// while old open handle fh is invalidated (EBADF).
		d2, reindexed, err := sup.ColdRemountRecover(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("ColdRemountRecover: %v", err)
		}
		if reindexed != 1 {
			t.Fatalf("expected 1 file reindexed on cold remount, got %d", reindexed)
		}
		if _, err := d2.Read("tenant-a", h.Handle, 0, 16); !errors.Is(err, ErrInvalidHandle) {
			t.Fatalf("expected old handle invalid after cold remount, got %v", err)
		}
		ino2, err := d2.Lookup("tenant-a", "file.bin")
		if err != nil {
			t.Fatalf("Lookup after cold remount: %v", err)
		}
		h2, err := d2.Open("tenant-a", ino2.Inode, ino2.Generation, uint32(os.O_RDONLY))
		if err != nil {
			t.Fatalf("Open after cold remount: %v", err)
		}
		data, err := d2.Read("tenant-a", h2.Handle, 0, 32)
		if err != nil || string(data) != "persisted" {
			t.Fatalf("cold remount data = %q, err=%v", data, err)
		}
	})

	t.Run("fd_handoff_disabled_degrades_gracefully", func(t *testing.T) {
		sup, _, _ := setupSupervisor(t, CheckpointSync)
		if _, err := sup.Start(ctx); err != nil {
			t.Fatal(err)
		}
		sup.DisableHandoff()
		sup.InjectProcessCrash()

		if sup.Channel().MountAlive() {
			t.Fatal("expected mount to drop when handoff is disabled")
		}
		if _, _, err := sup.Recover(ctx); !errors.Is(err, ErrHandoffUnavailable) {
			t.Fatalf("expected ErrHandoffUnavailable when handoff disabled, got %v", err)
		}
		tel := sup.Metrics()
		if tel.DegradedFallbacks != 1 || tel.ColdRemountsRequired != 1 || tel.LastError == "" {
			t.Fatalf("expected explicit error telemetry on handoff disabled, got %+v", tel)
		}
	})
}
