package writeback_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/writeback"
)

func TestLifecycleBarrierStrictOrderOnDetachSuspendEnd(t *testing.T) {
	for _, ev := range []writeback.LifecycleEvent{
		writeback.EventDetach,
		writeback.EventSuspend,
		writeback.EventEnd,
	} {
		t.Run(string(ev), func(t *testing.T) {
			var order []string
			b, err := writeback.NewLifecycleBarrier(writeback.DefaultConfig(), writeback.BarrierDeps{
				SyncFS: func(mp string) error {
					order = append(order, "syncfs:"+mp)
					return nil
				},
				SyncJournal: func(context.Context) error {
					order = append(order, "journal_sync")
					return nil
				},
				PublishFinal: func(context.Context) error {
					order = append(order, "final_publish")
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			rep, err := b.Execute(context.Background(), ev, "/mnt/vfs")
			if err != nil {
				t.Fatalf("Execute(%s): %v", ev, err)
			}
			want := []string{"syncfs:/mnt/vfs", "journal_sync", "final_publish"}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("barrier call order = %v, want %v", order, want)
			}
			wantSteps := []string{"syncfs", "journal_sync", "final_publish"}
			if !reflect.DeepEqual(rep.Steps, wantSteps) {
				t.Fatalf("report steps = %v, want %v", rep.Steps, wantSteps)
			}
			if rep.Event != ev {
				t.Fatalf("report event = %s, want %s", rep.Event, ev)
			}
		})
	}
}

func TestLifecycleBarrierReportsErrorsWithoutSilencing(t *testing.T) {
	errSync := errors.New("syncfs EIO")
	errPub := errors.New("s3 timeout")
	b, err := writeback.NewLifecycleBarrier(writeback.DefaultConfig(), writeback.BarrierDeps{
		SyncFS:       func(string) error { return errSync },
		SyncJournal:  func(context.Context) error { return nil },
		PublishFinal: func(context.Context) error { return errPub },
	})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := b.Detach(context.Background(), "/mnt/vfs")
	if !errors.Is(err, errSync) || !errors.Is(err, errPub) {
		t.Fatalf("expected joined syncfs + publish errors, got: %v", err)
	}
	if rep.Err == "" {
		t.Fatal("expected non-empty BarrierReport.Err")
	}
}

func TestDirtyGuardBoundsExposureByAgeAndBytes(t *testing.T) {
	cfg := writeback.DefaultConfig()
	cfg.DirtyWindowBytes = 4 << 20
	cfg.DirtyExpireDuration = 80 * time.Millisecond

	var dirty atomic.Int64
	var flushes atomic.Int64
	dirty.Store(8 << 20) // exceeds DirtyWindowBytes immediately

	g, err := writeback.StartDirtyGuard(cfg, "/mnt/test", func(string) error {
		flushes.Add(1)
		dirty.Store(0)
		return nil
	}, dirty.Load)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for flushes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(15 * time.Millisecond)
	}
	if flushes.Load() < 2 {
		t.Fatalf("expected >= 2 dirty-guard flushes (1 byte-triggered + >=1 age-triggered), got %d", flushes.Load())
	}
}

func TestVolumeCleanCloseZeroSyncS3AndPublishWithin5s(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	journalDir := t.TempDir()

	rawBe, err := backend.Open(ctx, "file://"+storeDir)
	if err != nil {
		t.Fatal(err)
	}
	counter := writeback.WrapCounting(rawBe)
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	cipher := vfs.Hybrid(key)
	if err := vfs.Create(ctx, vfs.Config{Backend: counter, Cipher: cipher}); err != nil {
		t.Fatal(err)
	}

	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend:         counter,
		Cipher:          cipher,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: 64 << 20,
		Bound:           2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	// Create, write 256 KiB, fsync (local NVMe journal), and clean close.
	in, err := vol.Create(ctx, vfs.RootInode, "clean-close.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := vol.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 256<<10)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	if _, err := f.WriteAt(ctx, payload, 0); err != nil {
		t.Fatal(err)
	}
	tJournal := time.Now()
	if err := vol.SyncInode(ctx, in.ID); err != nil {
		t.Fatal(err)
	}

	putsBeforeClose := counter.TotalPuts()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	putsAfterClose := counter.TotalPuts()
	if delta := putsAfterClose - putsBeforeClose; delta != 0 {
		t.Fatalf("clean close issued %d synchronous S3 PUT(s), want 0", delta)
	}

	// Wait for background publisher to publish within <= 5 s bound.
	wantSeq := vol.State().Durable
	if wantSeq == 0 {
		t.Fatal("expected non-zero durable journal sequence after SyncInode")
	}
	deadline := time.Now().Add(writeback.DefaultPublishBound)
	for vol.State().Published < wantSeq && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	pubLatency := time.Since(tJournal)
	if vol.State().Published < wantSeq {
		t.Fatalf("background publication did not reach seq %d within %s (published=%d)", wantSeq, writeback.DefaultPublishBound, vol.State().Published)
	}
	if pubLatency > writeback.DefaultPublishBound {
		t.Fatalf("publication latency %s exceeded 5 s bound", pubLatency)
	}
	t.Logf("clean close sync S3 PUTs = 0; journal-to-S3 publication latency = %s (bound <= 5s)", pubLatency.Round(time.Millisecond))
}
