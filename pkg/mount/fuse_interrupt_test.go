//go:build fuse && linux

package mount_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/luxfi/age"
)

// signaler sends SIGURG, the signal Go's runtime preempts with, to the threads of the
// workers it is given, as fast as it can. A thread that is waiting in a request to
// the mount is interrupted, and the kernel sends the server FUSE_INTERRUPT.
type signaler struct {
	pid  int
	tids [8]atomic.Int64
	stop atomic.Bool
	sent atomic.Int64
	wg   sync.WaitGroup
}

func newSignaler() *signaler {
	s := &signaler{pid: os.Getpid()}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for !s.stop.Load() {
			for i := range s.tids {
				if tid := s.tids[i].Load(); tid != 0 {
					syscall.Syscall(syscall.SYS_TGKILL, uintptr(s.pid), uintptr(tid), uintptr(syscall.SIGURG))
					s.sent.Add(1)
				}
			}
			time.Sleep(40 * time.Microsecond)
		}
	}()
	return s
}

// work runs fn on a thread of its own that the signaler targets.
func (s *signaler) work(slot int, fn func()) {
	runtime.LockOSThread()
	s.tids[slot].Store(int64(syscall.Gettid()))
	defer func() {
		s.tids[slot].Store(0)
		runtime.UnlockOSThread()
	}()
	fn()
}

func (s *signaler) close() { s.stop.Store(true); s.wg.Wait() }

// restart runs a system call again while it answers EINTR, as every C library and
// the Go runtime do for every call but close; it counts how many times the caller
// really saw EINTR, which a mount that answers no request EINTR never shows.
func restart(eintr *atomic.Int64, call func() error) error {
	for {
		err := call()
		if err == syscall.EINTR {
			eintr.Add(1)
			continue
		}
		return err
	}
}

// How much work the signal test does, and how long it may take. The work is bounded by
// count, and by the clock once it has done the least that says anything (hundreds of
// interrupted calls of each kind): a host that is loaded runs it longer and does not
// fail it for being slow. The test fails for a call that failed, and for nothing that
// takes only time; the hard cap is there so that a mount that hangs is a failure and not a
// wait.
const (
	signalPhase     = 45 * time.Second // after this, a phase ends as soon as it has done its least
	signalHard      = 6 * time.Minute  // the most a phase may take
	signalMinFsyncs = 300
	signalMinBytes  = 512 << 20
)

// TestFUSEInterruptsAreNotErrors: signals land on callers while they are inside
// fsync(2), close(2) and read(2) of the mount. Go's runtime sends one every few
// milliseconds on its own; here they come tens of thousands of times a second, at the
// threads that are waiting. Not one call may fail: the kernel restarts the call or the
// caller sees EINTR, and never EIO. The files written and synced are in the store.
func TestFUSEInterruptsAreNotErrors(t *testing.T) {
	needFUSE(t)
	fsyncs, passes := 2000, 32 // 8 GiB of reads of a 256 MiB file
	if testing.Short() {
		fsyncs, passes = 300, 4
	}
	store, mp := newMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	m := mountAt(t, store, mp, key, 1)
	defer watchdog(t, m, 20*time.Minute)()

	sig := newSignaler()
	defer sig.close()
	var eintr atomic.Int64

	// fsync and close of small files, two writers at once.
	var bad, synced, closeEINTR atomic.Int64
	var last [2]atomic.Int64 // per writer, the number of files it finished
	var wg sync.WaitGroup
	start := time.Now()
	for w := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sig.work(w, func() {
				for i := range fsyncs / 2 {
					if took := time.Since(start); took > signalHard || took > signalPhase && synced.Load() >= signalMinFsyncs {
						return
					}
					path := filepath.Join(mp, fmt.Sprintf("s%d-%05d", w, i))
					var fd int
					err := restart(&eintr, func() (e error) { fd, e = syscall.Open(path, syscall.O_CREAT|syscall.O_WRONLY, 0o644); return })
					if err != nil {
						bad.Add(1)
						t.Errorf("open: %v", err)
						return
					}
					buf := rnd(uint64(w*100000+i), 4096)
					if err := restart(&eintr, func() error { _, e := syscall.Write(fd, buf); return e }); err != nil {
						bad.Add(1)
						t.Errorf("write: %v", err)
					}
					if err := restart(&eintr, func() error { return syscall.Fsync(fd) }); err != nil {
						bad.Add(1)
						t.Errorf("fsync %d: %v", i, err)
					}
					// close is not restarted, and a Linux kernel waits for the mount's answer to
					// it however many signals arrive. A kernel that ends the wait itself and
					// returns EINTR from close leaves the commit going on and the file fsynced, so
					// the caller loses nothing and it is not a fault of the mount. Any other error
					// is.
					if err := syscall.Close(fd); err != nil && err != syscall.EINTR {
						bad.Add(1)
						t.Errorf("close %d: %v", i, err)
					} else if err != nil {
						closeEINTR.Add(1)
					}
					synced.Add(1)
					last[w].Store(int64(i) + 1)
				}
			})
		}()
	}
	wg.Wait()
	t.Logf("%d fsync+close under %d signals in %v: %d failed, the caller saw EINTR %d times (%d from close)", synced.Load(), sig.sent.Load(), time.Since(start).Round(time.Millisecond), bad.Load(), eintr.Load(), closeEINTR.Load())
	if bad.Load() != 0 {
		t.FailNow()
	}
	if synced.Load() < signalMinFsyncs {
		t.Fatalf("only %d fsyncs in %v: the mount hangs, or is too slow to say that signals do no harm", synced.Load(), signalHard)
	}

	// Sequential reads of a 256 MiB file, 1 MiB at a time, over and over.
	const block, blocks = 1 << 20, 256
	path := filepath.Join(mp, "big")
	func() {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		buf := make([]byte, block)
		for b := range blocks {
			copy(buf, rnd(uint64(b), 64))
			binary.LittleEndian.PutUint64(buf[64:], uint64(b))
			if _, err := f.Write(buf); err != nil {
				t.Fatal(err)
			}
		}
	}()
	start = time.Now()
	var read atomic.Int64
	for w := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sig.work(2+w, func() {
				var fd int
				err := restart(&eintr, func() (e error) { fd, e = syscall.Open(path, syscall.O_RDONLY, 0); return })
				if err != nil {
					bad.Add(1)
					t.Errorf("open: %v", err)
					return
				}
				defer syscall.Close(fd)
				buf := make([]byte, block)
				for p := range passes / 2 {
					for b := range blocks {
						if took := time.Since(start); took > signalHard || took > signalPhase && read.Load() >= signalMinBytes {
							return
						}
						off := int64(b) * block
						n, err := 0, error(nil)
						for n < block && err == nil {
							var k int
							err = restart(&eintr, func() (e error) { k, e = syscall.Pread(fd, buf[n:], off+int64(n)); return })
							if k == 0 && err == nil {
								err = fmt.Errorf("short read at %d", off+int64(n))
							}
							n += k
						}
						if err != nil {
							bad.Add(1)
							t.Errorf("pass %d block %d: %v", p, b, err)
							return
						}
						if binary.LittleEndian.Uint64(buf[64:]) != uint64(b) || string(buf[:64]) != string(rnd(uint64(b), 64)) {
							bad.Add(1)
							t.Errorf("pass %d block %d: wrong bytes", p, b)
							return
						}
						read.Add(block)
					}
				}
			})
		}()
	}
	wg.Wait()
	took := time.Since(start)
	t.Logf("%d GiB read under %d signals in %v (%.0f MB/s): %d failed, the caller saw EINTR %d times", read.Load()>>30, sig.sent.Load(), took.Round(time.Millisecond), float64(read.Load())/1e6/took.Seconds(), bad.Load(), eintr.Load())
	if bad.Load() != 0 {
		t.FailNow()
	}
	if read.Load() < signalMinBytes {
		t.Fatalf("only %d MiB read in %v: the mount hangs, or is too slow to say that signals do no harm", read.Load()>>20, signalHard)
	}
	sig.close()
	m.Stop(t)

	// What was acknowledged is in the store, read from outside the mount.
	ro := storeView(t, store, key)
	for w := range 2 {
		for _, i := range []int{0, int(last[w].Load()) - 1} {
			got, err := storeBytes(t, ro, fmt.Sprintf("/s%d-%05d", w, i))
			if err != nil || string(got) != string(rnd(uint64(w*100000+i), 4096)) {
				t.Fatalf("file s%d-%05d: %d bytes, %v", w, i, len(got), err)
			}
		}
	}
}
