package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
)

// Run is the conformance suite: what every Backend must do, in particular the
// two conditional writes the fence and the Head commit rest on. open returns a
// fresh, empty backend.
func Run(t *testing.T, open func(t *testing.T) backend.Backend) {
	ctx := context.Background()

	// The caller owns the buffer of a write: once the call returns, the backend does not read
	// it again, so the caller may reuse it or clear it, however the store sent it (in one
	// request, in parts, or again after a failure). The writer returns its buffers to a pool
	// the moment a write returns, so a backend that kept a reference would store another
	// object's bytes.
	t.Run("the caller owns its buffer when a write returns", func(t *testing.T) {
		be := open(t)
		clobber := func(b []byte) {
			for i := range b {
				b[i] = 0xEE
			}
		}
		put := func(name string, write func(key string, data []byte) error) {
			t.Helper()
			for _, n := range []int{1, 4 << 10, 1 << 20, 5 << 20} {
				data := make([]byte, n)
				for i := range data {
					data[i] = byte(i*7 + n)
				}
				want := append([]byte(nil), data...)
				key := fmt.Sprintf("own/%s/%d", name, n)
				if err := write(key, data); err != nil {
					t.Fatalf("%s %d bytes: %v", name, n, err)
				}
				clobber(data)
				got, _, err := be.Get(ctx, key, backend.Unbounded)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("%s of %d bytes: after the call returned and the caller cleared its buffer the store holds %d bytes that differ (err %v)", name, n, len(got), err)
				}
			}
		}
		put("put", func(key string, data []byte) error { return be.Put(ctx, key, data) })
		put("putabsent", func(key string, data []byte) error { _, err := be.PutIfAbsent(ctx, key, data); return err })
		put("putversion", func(key string, data []byte) error {
			if err := be.Put(ctx, key, []byte("first")); err != nil {
				return err
			}
			_, v, err := be.Get(ctx, key, backend.Unbounded)
			if err != nil {
				return err
			}
			_, err = be.PutIfVersion(ctx, key, data, v)
			return err
		})
	})

	t.Run("get put range delete", func(t *testing.T) {
		be := open(t)
		if _, _, err := be.Get(ctx, "a/b", backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("Get absent: %v, want ErrNotFound", err)
		}
		if err := be.Put(ctx, "a/b", []byte("0123456789")); err != nil {
			t.Fatal(err)
		}
		got, v, err := be.Get(ctx, "a/b", backend.Unbounded)
		if err != nil || string(got) != "0123456789" || v == "" {
			t.Fatalf("Get: %q %q %v", got, v, err)
		}
		if _, v2, _ := be.Get(ctx, "a/b", backend.Unbounded); v2 != v {
			t.Fatalf("version of unchanged key moved: %q then %q", v, v2)
		}
		r, err := be.GetRange(ctx, "a/b", 3, 4)
		if err != nil || string(r) != "3456" {
			t.Fatalf("GetRange: %q %v", r, err)
		}
		if _, err := be.GetRange(ctx, "a/b", 8, 5); err == nil {
			t.Fatal("GetRange past the end returned no error")
		}
		if _, err := be.GetRange(ctx, "a/none", 0, 1); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("GetRange absent: %v, want ErrNotFound", err)
		}
		if n, err := be.Stat(ctx, "a/b"); err != nil || n != 10 {
			t.Fatalf("Stat: %d %v", n, err)
		}
		if err := be.Delete(ctx, "a/b"); err != nil {
			t.Fatal(err)
		}
		if err := be.Delete(ctx, "a/b"); err != nil {
			t.Fatalf("Delete absent: %v", err)
		}
		if _, _, err := be.Get(ctx, "a/b", backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("Get after Delete: %v", err)
		}
	})

	t.Run("put if absent", func(t *testing.T) {
		be := open(t)
		v1, err := be.PutIfAbsent(ctx, "head", []byte("one"))
		if err != nil || v1 == "" {
			t.Fatalf("first PutIfAbsent: %q %v", v1, err)
		}
		if _, err := be.PutIfAbsent(ctx, "head", []byte("two")); !errors.Is(err, backend.ErrConflict) {
			t.Fatalf("second PutIfAbsent: %v, want ErrConflict", err)
		}
		got, v, _ := be.Get(ctx, "head", backend.Unbounded)
		if string(got) != "one" || v != v1 {
			t.Fatalf("a refused PutIfAbsent changed the object: %q %q (want one %q)", got, v, v1)
		}
	})

	t.Run("put if version", func(t *testing.T) {
		be := open(t)
		if _, err := be.PutIfVersion(ctx, "head", []byte("x"), "nothing"); !errors.Is(err, backend.ErrConflict) {
			t.Fatalf("PutIfVersion on absent: %v, want ErrConflict", err)
		}
		if _, _, err := be.Get(ctx, "head", backend.Unbounded); err == nil {
			t.Fatal("PutIfVersion on an absent key created it")
		}
		v1, _ := be.PutIfAbsent(ctx, "head", []byte("one"))
		v2, err := be.PutIfVersion(ctx, "head", []byte("two"), v1)
		if err != nil || v2 == v1 {
			t.Fatalf("PutIfVersion at the current version: %q %v", v2, err)
		}
		if _, err := be.PutIfVersion(ctx, "head", []byte("three"), v1); !errors.Is(err, backend.ErrConflict) {
			t.Fatalf("PutIfVersion at a stale version: %v, want ErrConflict", err)
		}
		got, v, _ := be.Get(ctx, "head", backend.Unbounded)
		if string(got) != "two" || v != v2 {
			t.Fatalf("object after a refused write: %q %q, want two %q", got, v, v2)
		}
		if _, err := be.PutIfVersion(ctx, "head", []byte("three"), v2); err != nil {
			t.Fatalf("PutIfVersion at the returned version: %v", err)
		}
	})

	t.Run("exactly one winner", func(t *testing.T) {
		be := open(t)
		v0, _ := be.PutIfAbsent(ctx, "head", []byte("base"))
		for round := range 20 {
			_, v, _ := be.Get(ctx, "head", backend.Unbounded)
			const racers = 8
			var wins atomic.Int32
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range racers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, err := be.PutIfVersion(ctx, "head", []byte(fmt.Sprintf("r%d-w%d", round, i)), v)
					switch {
					case err == nil:
						wins.Add(1)
					case !errors.Is(err, backend.ErrConflict):
						t.Errorf("racer %d: %v", i, err)
					}
				}()
			}
			close(start)
			wg.Wait()
			if wins.Load() != 1 {
				t.Fatalf("round %d: %d of %d racers on version %q won, want exactly 1", round, wins.Load(), racers, v)
			}
		}
		_ = v0

		// The same race for creation: exactly one PutIfAbsent wins.
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := be.PutIfAbsent(ctx, "fresh", []byte{byte(i)}); err == nil {
					wins.Add(1)
				} else if !errors.Is(err, backend.ErrConflict) {
					t.Errorf("creator %d: %v", i, err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("%d creators won, want exactly 1", wins.Load())
		}
	})

	t.Run("list", func(t *testing.T) {
		be := open(t)
		for _, k := range []string{"pack/aa/1", "pack/aa/2", "pack/bb/3", "head"} {
			if err := be.Put(ctx, k, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		ch, errCh := be.List(ctx, "pack/aa")
		var got []string
		for o := range ch {
			got = append(got, o.Key)
		}
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("List(pack/aa) = %v, want 2 keys", got)
		}
	})

	t.Run("keys", func(t *testing.T) {
		be := open(t)
		long255 := strings.Repeat("a", 255)
		deep := strings.Repeat("b", 250) + "/" + strings.Repeat("c", 250)    // 501 bytes
		longest := strings.Repeat("d", 255) + "/" + strings.Repeat("e", 255) // 511 bytes
		good := []string{
			"head", "ext/x/a", "ext/x/ab", "ext/x/a-b-0", "k0/" + long255, deep, longest,
			"ext/trace/" + strings.Repeat("0123456789abcdef", 4),
		}
		for _, k := range good {
			if err := be.Put(ctx, k, []byte("v:"+k)); err != nil {
				t.Fatalf("Put %.40q (%d bytes): %v", k, len(k), err)
			}
		}
		for _, k := range good {
			if got, _, err := be.Get(ctx, k, backend.Unbounded); err != nil || string(got) != "v:"+k {
				t.Fatalf("Get %.40q: %q %v", k, got, err)
			}
			if n, err := be.Stat(ctx, k); err != nil || n != int64(2+len(k)) {
				t.Fatalf("Stat %.40q: %d %v", k, n, err)
			}
		}
		if got := listKeys(t, be, ""); len(got) != len(good) {
			t.Fatalf("the store holds %d keys, want %d", len(got), len(good))
		}
	})

	t.Run("list is by string prefix", func(t *testing.T) {
		be := open(t)
		for _, k := range []string{"ext/y/ab", "ext/y/a/c", "ext/y/b", "ext/yy/a", "head"} {
			if err := be.Put(ctx, k, []byte("12345")); err != nil {
				t.Fatal(err)
			}
		}
		for prefix, want := range map[string][]string{
			"ext/y/a":  {"ext/y/a/c", "ext/y/ab"},
			"ext/y/a/": {"ext/y/a/c"},
			"ext/y":    {"ext/y/a/c", "ext/y/ab", "ext/y/b", "ext/yy/a"},
			"ext/y/":   {"ext/y/a/c", "ext/y/ab", "ext/y/b"},
			"h":        {"head"},
			"nothing":  nil,
			"":         {"ext/y/a/c", "ext/y/ab", "ext/y/b", "ext/yy/a", "head"},
		} {
			if got := listKeys(t, be, prefix); !slices.Equal(got, want) {
				t.Errorf("List(%q) = %v, want %v", prefix, got, want)
			}
		}
	})

	t.Run("list tells size and time", func(t *testing.T) {
		be := open(t)
		before := time.Now()
		if err := be.Put(ctx, "pack/aa/one", []byte("12345")); err != nil {
			t.Fatal(err)
		}
		if err := be.Put(ctx, "pack/aa/two", make([]byte, 70000)); err != nil {
			t.Fatal(err)
		}
		ch, errCh := be.List(ctx, "pack/aa/")
		got := map[string]backend.Object{}
		for o := range ch {
			got[o.Key] = o
		}
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
		if got["pack/aa/one"].Size != 5 || got["pack/aa/two"].Size != 70000 {
			t.Fatalf("sizes %+v", got)
		}
		// The time is the store's own, and every store the suite runs on is on this machine
		// (a directory, a server in a test process, a server started for the tests): it is the
		// time of the write to within the second that an S3 store truncates to, and so within
		// two. A collector's grace must be longer than that second (SPECIFICATION 11.1).
		for k, o := range got {
			if o.Modified.IsZero() || o.Modified.Before(before.Add(-2*time.Second)) || o.Modified.After(time.Now().Add(2*time.Second)) {
				t.Errorf("%s: modified at %v, written at %v", k, o.Modified, before)
			}
		}
	})

	t.Run("get is bounded", func(t *testing.T) {
		be := open(t)
		if err := be.Put(ctx, "k", []byte("0123456789")); err != nil {
			t.Fatal(err)
		}
		if b, _, err := be.Get(ctx, "k", 10); err != nil || len(b) != 10 {
			t.Fatalf("Get at exactly max: %d bytes, %v", len(b), err)
		}
		if b, _, err := be.Get(ctx, "k", 9); !errors.Is(err, backend.ErrTooLarge) || len(b) != 0 {
			t.Fatalf("Get over max = %d bytes, %v; want ErrTooLarge and nothing", len(b), err)
		}
	})

	t.Run("large object", func(t *testing.T) {
		be := open(t)
		big := bytes.Repeat([]byte("0123456789abcdef"), 1<<18) // 4 MiB
		if err := be.Put(ctx, "pack/aa/big", big); err != nil {
			t.Fatal(err)
		}
		got, _, err := be.Get(ctx, "pack/aa/big", backend.Unbounded)
		if err != nil || !bytes.Equal(got, big) {
			t.Fatalf("large object round trip failed: %v", err)
		}
		r, err := be.GetRange(ctx, "pack/aa/big", 1<<20+5, 1000)
		if err != nil || !bytes.Equal(r, big[1<<20+5:1<<20+5+1000]) {
			t.Fatalf("large object range failed: %v", err)
		}
	})
}

// listKeys returns the keys List gives for a prefix, sorted.
func listKeys(t *testing.T, be backend.Backend, prefix string) []string {
	t.Helper()
	ch, errCh := be.List(context.Background(), prefix)
	var out []string
	for o := range ch {
		out = append(out, o.Key)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	slices.Sort(out)
	return out
}
