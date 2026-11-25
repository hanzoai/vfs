package vfs_test

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

// expected returns the snapshots of the two states a crash test moves between.
func expected(t *testing.T) (one, two map[string]string) {
	_, _, v := newMem(t)
	stateOne(t, v)
	flush(t, v)
	one = snapshot(t, v)
	stateTwo(t, v)
	flush(t, v)
	return one, snapshot(t, v)
}

// TestFailureAtEveryWrite: a Flush that fails at any one of its writes leaves the
// store at the previous Head: a fresh Open sees exactly state one, whatever chunks,
// packs and shards the failed Flush managed to upload. A retry on the same Volume
// then commits state two.
//
// Every Volume draws its own identity and chunking seed, so the number of shards a
// commit writes differs from one Volume to the next. The writes are therefore named
// by kind and by their number within the kind (the second shard, the pack, the
// Head), and for each kind the sweep goes on until it reaches a number the commit
// does not make, which is then a commit that simply succeeds.
func TestFailureAtEveryWrite(t *testing.T) {
	one, two := expected(t)

	kind := func(key string) string {
		switch {
		case strings.HasPrefix(key, "pack/"):
			return "pack"
		case strings.HasPrefix(key, "meta/"):
			return "shard"
		}
		return "head"
	}
	// run commits state two on a fresh Volume, failing the nth write of a kind (n 0
	// fails none). It reports how many writes of the kind the commit made.
	run := func(want string, nth int) (made int, mem *storetest.Mem, c vfs.Cipher, v *vfs.Volume, err error, boom error) {
		mem = storetest.NewMem()
		hook := &storetest.Hook{Backend: mem}
		c = vfsAge(t)
		if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
			t.Fatal(err)
		}
		v, aerr := vfs.Attach(ctx, cfg(hook, c, 1))
		if aerr != nil {
			t.Fatal(aerr)
		}
		stateOne(t, v)
		flush(t, v)
		stateTwo(t, v)
		boom = errors.New("injected")
		hook.Before = func(op, key string) error {
			if kind(key) != want {
				return nil
			}
			made++
			if made == nth {
				return boom
			}
			return nil
		}
		err = v.Flush(ctx)
		return made, mem, c, v, err, boom
	}

	total := 0
	for _, want := range []string{"pack", "shard", "head"} {
		for nth := 1; ; nth++ {
			made, mem, c, v, err, boom := run(want, nth)
			if made < nth { // the commit has fewer writes of this kind: it was not failed
				if err != nil {
					t.Fatalf("%s write %d never came, yet Flush = %v", want, nth, err)
				}
				sameSnapshot(t, fmt.Sprintf("a commit with %d %s writes", made, want), snapshot(t, reopen(t, mem, c)), two)
				if nth == 1 {
					t.Fatalf("a commit that makes no %s write: the sweep proves nothing about it", want)
				}
				break
			}
			total++
			if !errors.Is(err, boom) {
				t.Fatalf("%s write %d: Flush = %v, want the injected error", want, nth, err)
			}
			sameSnapshot(t, fmt.Sprintf("failure at %s write %d", want, nth), snapshot(t, reopen(t, mem, c)), one)
			check(t, reopen(t, mem, c)) // names, link counts, parents and usage agree: a link count never lies

			// The Volume kept its changes: a retry commits them.
			flush(t, v)
			sameSnapshot(t, fmt.Sprintf("retry after %s write %d", want, nth), snapshot(t, reopen(t, mem, c)), two)
			check(t, reopen(t, mem, c))
		}
	}
	if total < 4 {
		t.Fatalf("only %d writes were failed; the sweep would prove little", total)
	}
	t.Logf("%d writes failed in turn, one at a time (pack, shards, Head)", total)
}

// TestLostAcknowledgementOfHead: the Head write lands but its reply is lost. The
// publisher reads the Head, finds its own generation in it, and counts it as published:
// the caller's Flush returns nil, the store holds the new state whole, and nothing is
// written twice, now or on a later Flush.
func TestLostAcknowledgementOfHead(t *testing.T) {
	_, two := expected(t)
	mem := storetest.NewMem()
	var lose atomic.Bool
	var heads atomic.Int64
	hook := &storetest.Hook{Backend: mem, After: func(op, key string) error {
		if key == "head" {
			heads.Add(1)
		}
		if lose.Load() && key == "head" {
			return errors.New("reply lost")
		}
		return nil
	}}
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(hook, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(hook, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	stateOne(t, v)
	flush(t, v)
	stateTwo(t, v)
	lose.Store(true)
	before := heads.Load()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush after a Head whose reply was lost, and which landed: %v", err)
	}
	if n := heads.Load() - before; n != 1 {
		t.Fatalf("%d Head writes for one generation", n)
	}
	sameSnapshot(t, "head landed", snapshot(t, reopen(t, mem, c)), two)
	lose.Store(false)
	flush(t, v) // nothing is left to publish
	if n := heads.Load() - before; n != 1 {
		t.Fatalf("a Flush with nothing to publish wrote the Head again (%d writes)", n)
	}
	sameSnapshot(t, "after the next Flush", snapshot(t, reopen(t, mem, c)), two)
}

// crashChild is the process the crash test kills. It is the test binary run
// again with the arguments "crashchild <dir> <stage>"; it takes the Volume key
// from standard input, as the vfs command does, and never from the environment.
func TestCrashChild(t *testing.T) {
	if flag.Arg(0) != "crashchild" {
		t.Skip("runs only as the child of TestSigkill")
	}
	dir, stage := flag.Arg(1), flag.Arg(2)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	key, err := age.ParseHybridIdentity(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	be, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		t.Fatal(err)
	}
	// The stop is armed once Attach has returned, so the claim's own write of the
	// Head is not mistaken for the commit.
	var armed, stopped atomic.Bool
	stop := func(op, key string) {
		if armed.Load() && stopped.CompareAndSwap(false, true) {
			fmt.Println("READY")
			select {} // wait for the parent's SIGKILL
		}
	}
	hook := &storetest.Hook{Backend: be}
	switch stage {
	case "pack":
		hook.Before = func(op, key string) error {
			if strings.HasPrefix(key, "pack/") {
				stop(op, key)
			}
			return nil
		}
	case "packed":
		hook.After = func(op, key string) error {
			if strings.HasPrefix(key, "pack/") {
				stop(op, key)
			}
			return nil
		}
	case "shard":
		hook.Before = func(op, key string) error {
			if strings.HasPrefix(key, "meta/") {
				stop(op, key)
			}
			return nil
		}
	case "sharded":
		hook.After = func(op, key string) error {
			if strings.HasPrefix(key, "meta/") {
				stop(op, key)
			}
			return nil
		}
	case "head":
		hook.Before = func(op, key string) error {
			if key == "head" {
				stop(op, key)
			}
			return nil
		}
	case "landed":
		hook.After = func(op, key string) error {
			if key == "head" {
				stop(op, key)
			}
			return nil
		}
	default:
		t.Fatalf("unknown stage %q", stage)
	}
	v, err := vfs.Attach(ctx, cfg(hook, vfs.Hybrid(key), 2))
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	stateTwo(t, v)
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	t.Fatal("the flush finished without reaching the stop")
}

// TestSigkill kills a real process between uploading chunks, uploading shards and
// committing the Head, and after the commit, and reopens the store from another
// process: the tree is exactly the last committed state, every time.
func TestSigkill(t *testing.T) {
	one, two := expected(t)
	for _, tc := range []struct {
		stage string
		want  map[string]string
	}{
		{"pack", one}, {"packed", one}, {"shard", one}, {"sharded", one}, {"head", one}, {"landed", two},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			dir := t.TempDir()
			key := newKey(t)
			c := vfs.Hybrid(key)
			be, err := backend.Open(ctx, "file://"+dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
				t.Fatal(err)
			}
			v, err := vfs.Attach(ctx, cfg(be, c, 1))
			if err != nil {
				t.Fatal(err)
			}
			stateOne(t, v)
			flush(t, v)

			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1", "crashchild", dir, tc.stage)
			cmd.Stdin = strings.NewReader(key.String() + "\n")
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			ready := make(chan bool, 1)
			go func() {
				sc := bufio.NewScanner(out)
				for sc.Scan() {
					if sc.Text() == "READY" {
						ready <- true
						return
					}
				}
				ready <- false
			}()
			select {
			case ok := <-ready:
				if !ok {
					_ = cmd.Wait()
					t.Fatal("the child exited before reaching the stop")
				}
			case <-time.After(2 * time.Minute):
				_ = cmd.Process.Kill()
				t.Fatal("the child never reached the stop")
			}
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()

			// Read-only first, then a takeover: both see the committed state.
			ro, err := vfs.Open(ctx, cfg(reopenBackend(t, dir), c, 0))
			if err != nil {
				t.Fatal(err)
			}
			sameSnapshot(t, "after SIGKILL at "+tc.stage, snapshot(t, ro), tc.want)
			next, err := vfs.Attach(ctx, cfg(reopenBackend(t, dir), c, 3))
			if err != nil {
				t.Fatalf("takeover after the kill: %v", err)
			}
			sameSnapshot(t, "takeover after SIGKILL at "+tc.stage, snapshot(t, next), tc.want)
			// And the survivor can carry on: it commits state two over what is there.
			if tc.stage != "landed" {
				stateTwo(t, next)
				flush(t, next)
				sameSnapshot(t, "carried on after "+tc.stage, snapshot(t, reopen(t, reopenBackend(t, dir), c)), two)
			}
		})
	}
}

func reopenBackend(t *testing.T, dir string) backend.Backend {
	be, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		t.Fatal(err)
	}
	return be
}
