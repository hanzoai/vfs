//go:build fuse && linux

package mount

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// stalledVolume is a Volume whose publication has started and whose store has stopped answering,
// as stop() leaves it when it gives up. release lets the store answer again.
func stalledVolume(t *testing.T) (v *vfs.Volume, release func()) {
	t.Helper()
	k, _ := age.GenerateHybridIdentity()
	c := vfs.Hybrid(k)
	gate := make(chan struct{})
	h := &storetest.Hook{Backend: storetest.NewMem()}
	bg := context.Background()
	if err := vfs.Create(bg, vfs.Config{Backend: h, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(bg, vfs.Config{Backend: h, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Create(bg, vfs.RootInode, "x", 0o644); err != nil {
		t.Fatal(err)
	}
	h.Before = func(op, key string) error { <-gate; return nil }
	go func() { _ = v.Flush(bg) }()
	time.Sleep(300 * time.Millisecond)
	return v, func() { close(gate) }
}

// SPEC 11.2: SIGTERM ends a mount with a stalled store. A flush, or a Stats, would queue behind the
// Volume's lock that the stalled commit holds until the store answers. finish therefore ends after
// finishGrace, 3 s (not 4.9 and not 0.2), and says that the last commit did not finish, so the
// process exits non-zero and does not wait for the store.
func TestFinishDoesNotWaitForAStalledCommitForever(t *testing.T) {
	v, release := stalledVolume(t)
	defer release()
	t0 := time.Now()
	err := finish(v, nil)
	d := time.Since(t0)
	if err == nil {
		t.Fatal("finish said nothing of a commit that never finished: the process would exit 0 with writes lost")
	}
	if d < 2900*time.Millisecond || d > 3500*time.Millisecond {
		t.Errorf("finish gave up after %v, the text says 3 s", d)
	}
	if finishGrace != 3*time.Second {
		t.Errorf("finishGrace = %v, the text says 3 s", finishGrace)
	}
}

// The commit is not known to have failed: its request may be at the store already, so the message
// says that what the mount held may be lost, not that it is.
func TestFinishDoesNotClaimALostCommitIsLost(t *testing.T) {
	t.Parallel() // each waits out its own 3 s; what they check is not a time
	v, release := stalledVolume(t)
	defer release()
	err := finish(v, nil)
	if err == nil {
		t.Fatal("no error")
	}
	if !strings.Contains(err.Error(), "did not finish") || !strings.Contains(err.Error(), "journal") {
		t.Errorf("the message is %q: it must say that the last publication did not finish and that what it held is in the journal", err)
	}
}

// When Serve failed as well, finish returns both: the report of the commit is not dropped.
func TestFinishReportsTheLostCommitEvenWhenServeFailed(t *testing.T) {
	t.Parallel() // each waits out its own 3 s; what they check is not a time
	v, release := stalledVolume(t)
	defer release()
	served := errors.New("fuse.Serve: connection reset")
	err := finish(v, served)
	if !errors.Is(err, served) {
		t.Errorf("finish dropped the serve error: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "did not finish") {
		t.Errorf("finish with a serve error and a stalled last commit says %q: the report of what was not committed is gone", err)
	}
}

// A store that is slow and then answers is not reported as lost: this commit takes 1.5 s, inside
// the deadline.
func TestFinishWaitsForASlowCommit(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := vfs.Hybrid(k)
	h := &storetest.Hook{Backend: storetest.NewMem()}
	bg := context.Background()
	if err := vfs.Create(bg, vfs.Config{Backend: h, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(bg, vfs.Config{Backend: h, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Create(bg, vfs.RootInode, "x", 0o644); err != nil {
		t.Fatal(err)
	}
	var once bool
	h.Before = func(op, key string) error {
		if !once {
			once = true
			time.Sleep(1500 * time.Millisecond)
		}
		return nil
	}
	if err := finish(v, nil); err != nil {
		t.Fatalf("a commit that took 1.5 s was reported as %v", err)
	}
	ro, err := vfs.Open(bg, vfs.Config{Backend: h.Backend, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ro.Resolve(bg, "/x"); err != nil {
		t.Fatalf("the file of the slow commit is not in the store: %v", err)
	}
}
