//go:build fuse && linux

package mount

import (
	"context"
	"errors"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// TestFinishCommitsWhatTheMountWrote: the end of a mount is one flush. A kernel mount
// commits at every close, so this is the net under what a close did not reach, and it is
// tested here because no kernel mount can leave anything for it.
func TestFinishCommitsWhatTheMountWrote(t *testing.T) {
	_, v, c, mem := testVolume(t)
	bg := context.Background()
	in, err := v.Create(bg, vfs.RootInode, "unflushed", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(bg, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(bg, []byte("written, never flushed"), 0); err != nil {
		t.Fatal(err)
	}
	if err := finish(v, nil); err != nil {
		t.Fatal(err)
	}
	ro, err := vfs.Open(bg, vfs.Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ro.Resolve(bg, "/unflushed")
	if err != nil || got.Size != uint64(len("written, never flushed")) {
		t.Fatalf("after the end of the mount the file is %+v, %v", got, err)
	}
}

// TestFinishReportsWhatWentWrong: a mount that served fine and could not commit at its end
// says so, and one that failed to serve says that first.
func TestFinishReportsWhatWentWrong(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := vfs.Hybrid(k)
	h := &storetest.Hook{Backend: storetest.NewMem()}
	bg := context.Background()
	if err := vfs.Create(bg, vfs.Config{Backend: h, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(bg, vfs.Config{Backend: h, Cipher: c, Round: 1, Journal: storetest.Journals(), JournalCapacity: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Create(bg, vfs.RootInode, "x", 0o644); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("the store is down")
	h.Before = func(op, key string) error { return boom }
	if err := finish(v, nil); !errors.Is(err, boom) {
		t.Fatalf("finish with a failing store = %v, want the store's error", err)
	}
	served := errors.New("serve failed")
	if err := finish(v, served); !errors.Is(err, served) || !errors.Is(err, boom) {
		t.Fatalf("finish after a serve error = %v, want that error and the store's", err)
	}
}

// TestFinishOfAReadOnlyVolumeCommitsNothing: a Volume that was only opened has no Round and
// nothing to commit.
func TestFinishOfAReadOnlyVolumeCommitsNothing(t *testing.T) {
	_, _, c, mem := testVolume(t)
	ro, err := vfs.Open(context.Background(), vfs.Config{Backend: storetest.Strict{Backend: mem}, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	if err := finish(ro, nil); err != nil {
		t.Fatalf("finish of a read-only Volume = %v", err)
	}
}
