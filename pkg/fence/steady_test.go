package fence

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

// counting counts the requests of each kind that reach the store.
var ctx = context.Background()

type counting struct {
	*fakeCAS
	gets, writes atomic.Int64
	fail         func() error // when it returns an error, the next write fails with it after landing
}

func (c *counting) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	c.gets.Add(1)
	return c.fakeCAS.Get(ctx, key, max)
}

func (c *counting) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	c.writes.Add(1)
	ver, err := c.fakeCAS.PutIfVersion(ctx, key, data, v)
	if err == nil && c.fail != nil {
		if ferr := c.fail(); ferr != nil {
			return "", ferr
		}
	}
	return ver, err
}

// A writer that holds the key commits with one conditional write each time: no read
// first. The read is what made a steady-state commit two round trips.
func TestSteadyStatePutIsOneRequest(t *testing.T) {
	be := &counting{fakeCAS: newFakeCAS()}
	s := New(be, 1<<16)
	if err := s.Claim(ctx, "head", 1, nil); err != nil {
		t.Fatal(err)
	}
	gets, writes := be.gets.Load(), be.writes.Load()
	for i := range 10 {
		if err := s.Put(ctx, "head", []byte{byte(i)}, 1); err != nil {
			t.Fatal(err)
		}
	}
	if g, w := be.gets.Load()-gets, be.writes.Load()-writes; g != 0 || w != 10 {
		t.Fatalf("10 puts made %d reads and %d writes, want 0 and 10", g, w)
	}
	got, round, err := s.Get(ctx, "head")
	if err != nil || round != 1 || string(got) != "\x09" {
		t.Fatalf("head after the puts: %q round %d: %v", got, round, err)
	}
}

// A successor's claim changes the version, so the old owner's next put loses the
// condition, reads, and is told it was fenced. It is never admitted on the strength of
// a version it kept.
func TestSteadyStatePutIsFencedByAClaim(t *testing.T) {
	be := &counting{fakeCAS: newFakeCAS()}
	old := New(be, 1<<16)
	if err := old.Claim(ctx, "head", 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := old.Put(ctx, "head", []byte("a"), 1); err != nil {
		t.Fatal(err)
	}
	if err := New(be, 1<<16).Claim(ctx, "head", 2, nil); err != nil {
		t.Fatal(err)
	}
	if err := old.Put(ctx, "head", []byte("b"), 1); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("put by the fenced owner: %v, want ErrStaleRound", err)
	}
	if got, _, _ := old.Get(ctx, "head"); string(got) != "a" {
		t.Fatalf("the fenced owner's write landed: %q", got)
	}
}

// A write whose answer is lost may have landed. The next put does not trust the version
// it kept: it reads, and if its own bytes are there says so rather than writing again.
func TestSteadyStatePutAfterALostAnswerReads(t *testing.T) {
	be := &counting{fakeCAS: newFakeCAS()}
	s := New(be, 1<<16)
	if err := s.Claim(ctx, "head", 1, nil); err != nil {
		t.Fatal(err)
	}
	lost := errors.New("connection reset")
	be.fail = func() error { return lost }
	if err := s.Put(ctx, "head", []byte("x"), 1); !errors.Is(err, lost) {
		t.Fatalf("put: %v, want the lost answer", err)
	}
	be.fail = nil
	gets, writes := be.gets.Load(), be.writes.Load()
	if err := s.Put(ctx, "head", []byte("x"), 1); err != nil {
		t.Fatal(err)
	}
	if g, w := be.gets.Load()-gets, be.writes.Load()-writes; g != 1 || w != 0 {
		t.Fatalf("the retry of a landed write made %d reads and %d writes, want 1 and 0", g, w)
	}
}
