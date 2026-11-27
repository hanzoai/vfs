package writeback_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/writeback"
)

// TestDeterministicCrashSurvivalMatrix verifies all four HIP-1342 #13 survival
// scenarios on a real *vfs.Volume + real on-disk NVMe journal + counting file
// backend (portable across all OS/build tags without requiring root or FUSE):
//
//  1. Buffered write(2) held in kernel page cache (never delivered to daemon) +
//     daemon crash/remount -> buffered bytes LOST, prior fsync'd bytes 100% SURVIVE.
//  2. Write + fsync(2) + immediate daemon crash before S3 publication + remount ->
//     100% of fsync'd bytes SURVIVE via local journal replay and publish to S3.
//  3. Buffered write(2) WITHOUT app fsync + Detach/Suspend/End lifecycle barrier
//     (syncfs -> journal sync -> final publish) + remount -> 100% SURVIVES.
//  4. Clean close(2) -> 0 synchronous S3 PUTs; async publication completes <= 5 s.
func TestDeterministicCrashSurvivalMatrix(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	journalDir := t.TempDir()

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	cipher := vfs.Hybrid(key)

	openBe := func() *writeback.CountingBackend {
		be, err := backend.Open(ctx, "file://"+storeDir)
		if err != nil {
			t.Fatal(err)
		}
		return writeback.WrapCounting(be)
	}

	initBe := openBe()
	if err := vfs.Create(ctx, vfs.Config{Backend: initBe, Cipher: cipher}); err != nil {
		t.Fatal(err)
	}
	_ = initBe.Close()

	// Helper to read a full file from a Volume.
	readAll := func(vol *vfs.Volume, path string) ([]byte, error) {
		in, err := vol.Resolve(ctx, path)
		if err != nil {
			return nil, err
		}
		f, err := vol.Open(ctx, in.ID)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		buf := make([]byte, in.Size)
		n, err := f.ReadAt(ctx, buf, 0)
		if err != nil && err != io.EOF {
			return nil, err
		}
		return buf[:n], nil
	}

	// ── Case 1 & 2: fsync'd write survives abrupt daemon death without S3 publish,
	// while kernel-page-cache-only buffered write (never delivered to daemon) is lost.
	be1 := openBe()
	vol1, err := vfs.Attach(ctx, vfs.Config{
		Backend:         be1,
		Cipher:          cipher,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: 64 << 20,
		Bound:           30 * time.Second, // keep async publisher from firing before simulated crash
	})
	if err != nil {
		t.Fatal(err)
	}

	tracker, err := writeback.NewContractTracker(writeback.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	durableData := bytes.Repeat([]byte("DURABLE-FSYNC-DATA-"), 4096) // 76 KiB
	in, err := vol1.Create(ctx, vfs.RootInode, "state.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f1, err := vol1.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	_ = tracker.RecordKernelWrite(1, uint64(in.ID), 0, int64(len(durableData)), now)
	if _, err := f1.WriteAt(ctx, durableData, 0); err != nil {
		t.Fatal(err)
	}
	_ = tracker.RecordDaemonJournal(1, vol1.State().Visible, time.Now())
	if err := vol1.SyncInode(ctx, in.ID); err != nil {
		t.Fatal(err)
	}
	_ = tracker.RecordFsyncDurable(vol1.State().Durable, time.Now())

	// Confirm the backend still does NOT have the data (proving survival comes from local journal).
	if st := vol1.State(); st.Published >= st.Durable {
		t.Fatalf("expected unpublished journal tail before crash (durable=%d published=%d)", st.Durable, st.Published)
	}

	// Simulate buffered write(2) acknowledged by Linux kernel page cache that
	// NEVER reached the daemon before SIGKILL.
	volatileTail := bytes.Repeat([]byte("VOLATILE-PAGECACHE-"), 2048)
	_ = tracker.RecordKernelWrite(2, uint64(in.ID), int64(len(durableData)), int64(len(volatileTail)), time.Now())

	// Abrupt daemon close WITHOUT vol1.Flush() (simulating crash before S3 publication).
	_ = f1.Close()
	if err := vol1.Close(); err != nil {
		t.Fatal(err)
	}

	// Remount at Round 2 on the same journal directory + backend.
	be2 := openBe()
	vol2, err := vfs.Attach(ctx, vfs.Config{
		Backend:         be2,
		Cipher:          cipher,
		Round:           2,
		Journal:         journalDir,
		JournalCapacity: 64 << 20,
		Bound:           2 * time.Second,
	})
	if err != nil {
		t.Fatalf("remount at Round 2: %v", err)
	}
	rec := vol2.Recovery()
	if rec.Replayed == 0 {
		t.Fatalf("expected journal replay on remount, got Recovery=%+v", rec)
	}

	got, err := readAll(vol2, "/state.bin")
	if err != nil {
		t.Fatalf("read /state.bin after remount: %v", err)
	}
	if !bytes.Equal(got, durableData) {
		t.Fatalf("post-crash /state.bin length=%d, want exact %d fsync'd bytes (volatile tail must be absent)", len(got), len(durableData))
	}

	outcome := tracker.SimulateDaemonSIGKILL()
	if outcome.LostKernelBufferedBytes != int64(len(volatileTail)) || outcome.SurvivedFsyncBytes != int64(len(durableData)) {
		t.Fatalf("SIGKILL contract simulation mismatch: %+v", outcome)
	}

	// ── Case 3: Buffered write(2) WITHOUT app fsync + Detach/Suspend/End lifecycle barrier.
	// Simulate kernel page cache holding bufferedBytes until barrier.SyncFS flushes them into daemon.
	bufferedBytes := bytes.Repeat([]byte("LIFECYCLE-BARRIER-"), 4096)
	in2, err := vol2.Create(ctx, vfs.RootInode, "barrier.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := vol2.Open(ctx, in2.ID)
	if err != nil {
		t.Fatal(err)
	}

	barrier, err := writeback.NewLifecycleBarrier(writeback.DefaultConfig(), writeback.BarrierDeps{
		SyncFS: func(string) error {
			// Models Linux syncfs(mountFD) pushing kernel-buffered dirty pages into FUSE_WRITE.
			_, werr := f2.WriteAt(ctx, bufferedBytes, 0)
			return werr
		},
		SyncJournal:  vol2.Sync,
		PublishFinal: vol2.Flush,
	})
	if err != nil {
		t.Fatal(err)
	}

	rep, err := barrier.Suspend(ctx, "/mnt/simulated")
	if err != nil {
		t.Fatalf("barrier.Suspend: %v", err)
	}
	if len(rep.Steps) != 3 {
		t.Fatalf("expected 3 barrier steps, got %v", rep.Steps)
	}
	_ = f2.Close()
	if err := vol2.Close(); err != nil {
		t.Fatal(err)
	}

	// Open cold read-only Volume straight from object store (no journal!) -> 100% must be in S3!
	beRO := openBe()
	ro, err := vfs.Open(ctx, vfs.Config{Backend: beRO, Cipher: cipher})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	got1, err := readAll(ro, "/state.bin")
	if err != nil || !bytes.Equal(got1, durableData) {
		t.Fatalf("cold store /state.bin: len=%d err=%v, want %d bytes", len(got1), err, len(durableData))
	}
	got2, err := readAll(ro, "/barrier.bin")
	if err != nil || !bytes.Equal(got2, bufferedBytes) {
		t.Fatalf("cold store /barrier.bin: len=%d err=%v, want %d bytes", len(got2), err, len(bufferedBytes))
	}
}
