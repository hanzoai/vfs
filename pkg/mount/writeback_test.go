package mount_test

import (
	"context"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/mount"
	"github.com/hanzoai/vfs/pkg/writeback"
)

func TestFlushForLifecyclePublishesPendingVolumeWrites(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	journalDir := t.TempDir()

	be, err := backend.Open(ctx, "file://"+storeDir)
	if err != nil {
		t.Fatal(err)
	}
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	cipher := vfs.Hybrid(key)
	if err := vfs.Create(ctx, vfs.Config{Backend: be, Cipher: cipher}); err != nil {
		t.Fatal(err)
	}
	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend:         be,
		Cipher:          cipher,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: 32 << 20,
		Bound:           30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	in, err := vol.Create(ctx, vfs.RootInode, "hook.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := vol.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(ctx, []byte("lifecycle hook payload"), 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// Passing mountpoint="" skips OS syncfs on a non-mounted test directory and
	// runs journal_sync -> final_publish directly on the Volume.
	rep, err := mount.FlushForLifecycle(ctx, vol, "", writeback.EventDetach)
	if err != nil {
		t.Fatalf("FlushForLifecycle: %v", err)
	}
	if rep.Event != writeback.EventDetach {
		t.Fatalf("event = %s, want detach", rep.Event)
	}
	if rep.JournalPublishedSeq == 0 || rep.JournalPublishedSeq < rep.JournalVisibleSeq {
		t.Fatalf("expected all writes published after FlushForLifecycle: %+v", rep)
	}
}
