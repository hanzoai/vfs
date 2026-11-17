package vfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// TestSparseTruncateIsBoundedWork: one truncate call must not cost work or memory
// proportional to the size it asks for. A guest can issue truncate(2) to any size
// the kernel allows in one syscall; a hole has no chunks, so the Flush that
// follows has nothing to cut. This measures 64 GiB and fails if the Flush takes
// over a second.
func TestSparseTruncateIsBoundedWork(t *testing.T) {
	_, _, v := newMem(t)
	in, err := v.Create(ctx, vfs.RootInode, "hole", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	const size = 64 << 30
	start := time.Now()
	if err := v.Truncate(ctx, in.ID, size); err != nil {
		t.Fatal(err)
	}
	t.Logf("Truncate to 64 GiB returned in %v", time.Since(start))
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	start = time.Now()
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	runtime.ReadMemStats(&m1)
	t.Logf("Flush of a 64 GiB hole took %v, allocated %d MiB", took, (m1.TotalAlloc-m0.TotalAlloc)>>20)
	if took > time.Second {
		t.Fatalf("Flush of a 64 GiB hole took %v holding the Volume lock: a truncate costs work proportional to the size it names", took)
	}
}

// TestSmallWritesAmplifyMemory: Config.Dirty bounds the bytes of unsettled
// writes, and each separate write span costs far more than its bytes, so each is
// charged a fixed overhead too. A million one-byte writes at a stride must be
// accounted for about what they cost.
func TestSmallWritesAmplifyMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement: the scale step runs it without the race detector")
	}
	// Nothing is published while the writes are made: the bound is an hour and the journal and memory
	// are large, so that the heap is the spans' and what a publication would cache is not.
	_, _, v, _ := pair(t, nil, func(c *vfs.Config) { c.Bound, c.Dirty, c.Capacity = time.Hour, 8<<30, 2<<30 })
	in, err := v.Create(ctx, vfs.RootInode, "sieve", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := v.Open(ctx, in.ID)
	defer f.Close()
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	const n = 2_000_000
	one := []byte{7}
	start := time.Now()
	for i := range n {
		if _, err := f.WriteAt(ctx, one, int64(i)*2); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m1)
	st := v.Stats()
	held := int64(m1.HeapInuse) - int64(m0.HeapInuse)
	t.Logf("%d one-byte writes in %v: Stats.Dirty=%d bytes, heap grew %d MiB (%.1fx the accounted bytes)", n, time.Since(start), st.Dirty, held>>20, float64(held)/float64(st.Dirty))
	if held > 8*st.Dirty+(8<<20) {
		t.Fatalf("heap grew %d bytes for %d accounted: Dirty bounds payload, not the memory the spans cost", held, st.Dirty)
	}
}

// hole is a plain model of a file: its bytes, with which of them were ever written.
// A byte that was not is a hole, and reads as zero.
type hole struct {
	data []byte
}

func (m *hole) grow(n int) {
	if n > len(m.data) {
		m.data = append(m.data, make([]byte, n-len(m.data))...)
	}
}

func (m *hole) write(off int, b []byte) {
	m.grow(off + len(b))
	copy(m.data[off:], b)
}

func (m *hole) truncate(n int) {
	if n < len(m.data) {
		m.data = m.data[:n]
	} else {
		m.grow(n)
	}
}

// sparseRun holds a Volume on a store, the model it must match, and the means to
// look at it the three ways that matter.
type sparseRun struct {
	t     *testing.T
	be    *storetest.Mem
	c     vfs.Cipher
	v     *vfs.Volume
	f     *vfs.File
	ino   vfs.InodeID
	m     hole
	round uint64
}

func newSparseRun(t *testing.T) *sparseRun {
	be, c, v := newMem(t)
	in, err := v.Create(ctx, vfs.RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	return &sparseRun{t: t, be: be, c: c, v: v, f: f, ino: in.ID, round: 1}
}

func (r *sparseRun) write(off int, b []byte) {
	r.t.Helper()
	if _, err := r.f.WriteAt(ctx, b, int64(off)); err != nil {
		r.t.Fatal(err)
	}
	r.m.write(off, b)
}

func (r *sparseRun) truncate(n int) {
	r.t.Helper()
	if err := r.v.Truncate(ctx, r.ino, uint64(n)); err != nil {
		r.t.Fatal(err)
	}
	r.m.truncate(n)
}

// check compares the live Volume with the model, byte for byte, including reads
// that start and end inside holes.
func (r *sparseRun) check(what string) {
	r.t.Helper()
	checkFile(r.t, what+" (live)", r.v, r.ino, r.m.data)
}

// settle flushes, then compares a cold read-only view, the same Volume, and a
// takeover that carries the commit forward.
func (r *sparseRun) settle(what string) {
	r.t.Helper()
	flush(r.t, r.v)
	checkFile(r.t, what+" (after flush)", r.v, r.ino, r.m.data)
	checkFile(r.t, what+" (cold view)", reopen(r.t, r.be, r.c), r.ino, r.m.data)
	r.round++
	next, err := vfs.Attach(ctx, cfg(r.be, r.c, r.round))
	if err != nil {
		r.t.Fatal(err)
	}
	checkFile(r.t, what+" (takeover)", next, r.ino, r.m.data)
	r.v = next
	r.f, err = next.Open(ctx, r.ino)
	if err != nil {
		r.t.Fatal(err)
	}
}

func checkFile(t *testing.T, what string, v *vfs.Volume, ino vfs.InodeID, want []byte) {
	t.Helper()
	in, err := v.Stat(ctx, ino)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if in.Size != uint64(len(want)) {
		t.Fatalf("%s: size %d, model %d", what, in.Size, len(want))
	}
	f, err := v.Open(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := bytes.Repeat([]byte{0xA5}, len(want)+16)
	n, err := f.ReadAt(ctx, got, 0)
	if err != nil && err.Error() != "EOF" || n != len(want) {
		t.Fatalf("%s: read %d of %d bytes: %v", what, n, len(want), err)
	}
	mustEqual(t, what, got[:n], want)
	// Reads that begin and end inside holes and across boundaries.
	if len(want) > 4096 {
		rng := rand.New(rand.NewPCG(uint64(len(want)), 3))
		for range 40 {
			off := rng.IntN(len(want) - 1)
			buf := bytes.Repeat([]byte{0xA5}, 1+rng.IntN(min(len(want)-off, 3<<20)))
			n, err := f.ReadAt(ctx, buf, int64(off))
			if err != nil && err.Error() != "EOF" {
				t.Fatal(err)
			}
			mustEqual(t, fmt.Sprintf("%s: read at %d", what, off), buf[:n], want[off:off+n])
		}
	}
}

// TestSparseExact keeps exact filesystem semantics, not just bounded cost: every
// step is compared byte for byte with a plain model, live, after Flush, from a cold
// view, and through a takeover.
func TestSparseExact(t *testing.T) {
	const MiB = 1 << 20

	t.Run("zeros from a hole", func(t *testing.T) {
		r := newSparseRun(t)
		r.truncate(10 * MiB)
		r.check("a hole of 10 MiB")
		r.settle("a hole of 10 MiB")
		if n := r.be.Puts()["pack"]; n != 0 {
			t.Fatalf("a hole wrote %d packs", n)
		}
	})

	t.Run("write into the middle of a hole", func(t *testing.T) {
		r := newSparseRun(t)
		r.truncate(10 * MiB)
		r.write(5*MiB+777, content(1, 100_000))
		r.check("data in a hole")
		r.settle("data in a hole")
		r.write(5*MiB+777+100_000, content(2, 50)) // grows the run
		r.settle("a run grown")
	})

	t.Run("writes that straddle hole, data and chunk boundaries", func(t *testing.T) {
		r := newSparseRun(t)
		r.write(1*MiB, content(3, 2*MiB)) // data [1,3) MiB: several chunks
		r.truncate(6 * MiB)               // a hole [3,6)
		r.write(6*MiB, content(4, 1*MiB)) // data again [6,7)
		r.settle("data, hole, data")
		r.write(900<<10, content(5, 600<<10))       // from a hole into data
		r.write(3*MiB-100_000, content(6, 400_000)) // from data into a hole
		r.write(2*MiB, content(7, 5*MiB))           // across data, hole and data
		r.check("straddles")
		r.settle("straddles")
		r.write(7*MiB-10, content(8, 40)) // across the end of the data and past EOF
		r.settle("past the end")
	})

	t.Run("truncate smaller than a hole, then extend", func(t *testing.T) {
		r := newSparseRun(t)
		r.write(0, content(9, 300_000))
		r.truncate(20 * MiB) // a hole [300000, 20 MiB)
		r.write(15*MiB, content(10, 70_000))
		r.settle("hole with data")
		r.truncate(10 * MiB) // inside the hole: drops the data at 15 MiB
		r.settle("shrunk inside the hole")
		r.truncate(25 * MiB) // the re-extended region reads zeros, not the old data
		r.check("re-extended")
		r.settle("re-extended")
		for _, off := range []int{15 * MiB, 15*MiB + 50_000, 10 * MiB, 24 * MiB} {
			buf := make([]byte, 20_000)
			f, _ := r.v.Open(ctx, r.ino)
			if _, err := f.ReadAt(ctx, buf, int64(off)); err != nil && err.Error() != "EOF" {
				t.Fatal(err)
			}
			f.Close()
			if !bytes.Equal(buf, make([]byte, len(buf))) {
				t.Fatalf("bytes at %d came back after a shrink and an extension", off)
			}
		}
	})

	t.Run("overwrite then truncate", func(t *testing.T) {
		r := newSparseRun(t)
		r.write(0, content(11, 4*MiB))
		r.write(1*MiB, make([]byte, 2*MiB)) // written zeros: data that happens to be zero
		r.check("zeroed")
		r.truncate(1*MiB + 500)
		r.settle("truncated mid-zeros")
		r.truncate(5 * MiB)
		r.write(4*MiB, content(12, 20))
		r.settle("extended and written")
	})

	t.Run("hole at EOF", func(t *testing.T) {
		r := newSparseRun(t)
		r.write(0, content(13, 700_000))
		r.settle("data")
		r.truncate(700_000 + 9*MiB) // hole at the end after settled data
		r.settle("hole at EOF")
		r.write(700_000+9*MiB, content(14, 3)) // append after the hole
		r.settle("appended after the hole")
	})

	t.Run("many holes", func(t *testing.T) {
		r := newSparseRun(t)
		for i := range 50 {
			r.write(i*MiB, content(uint64(100+i), 4096))
		}
		r.settle("fifty islands")
		for i := 0; i < 50; i += 3 {
			r.write(i*MiB+2048, content(uint64(200+i), 6000))
		}
		r.write(0, make([]byte, 0))
		r.truncate(49*MiB + 100)
		r.settle("islands edited and cut")
		if n := r.be.Puts()["pack"]; n > 12 {
			t.Fatalf("fifty 4 KiB islands wrote %d packs", n)
		}
	})

	t.Run("small files and the inline boundary", func(t *testing.T) {
		r := newSparseRun(t)
		r.truncate(10_000) // a hole in a small file
		r.write(9000, content(15, 500))
		r.settle("small with a hole")
		r.truncate(200_000) // grows past the inline limit: it becomes a chunked file with a hole
		r.settle("small grown into a hole")
		r.write(150_000, content(16, 100))
		r.truncate(8_000) // back under the limit: inline again, zeros kept
		r.settle("back to small")
		r.truncate(100_000)
		r.settle("and out again")
	})
}

// TestSparseRandom is the model fuzz aimed at holes: writes and truncations in a
// large logical space, with tiny bounds so settle and spill happen mid-stream, and
// every few steps a Flush, a cold read and a takeover.
func TestSparseRandom(t *testing.T) {
	seeds := sized(2, 40)
	for seed := range seeds {
		t.Run(strconv.Itoa(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 77))
			r := newSparseRun(t)
			for step := 0; step < 60; step++ {
				switch op := rng.IntN(10); {
				case op < 5:
					off := rng.IntN(24 << 20)
					n := 1 + rng.IntN(300_000)
					if rng.IntN(4) == 0 {
						n = 1 + rng.IntN(100)
					}
					r.write(off, content(uint64(seed*1000+step), n))
				case op < 7:
					n := rng.IntN(len(r.m.data) + 1)
					r.truncate(n)
				case op < 8:
					r.truncate(len(r.m.data) + rng.IntN(12<<20))
				case op < 9:
					r.check(fmt.Sprintf("seed %d step %d", seed, step))
				default:
					r.settle(fmt.Sprintf("seed %d step %d", seed, step))
				}
			}
			r.settle(fmt.Sprintf("seed %d end", seed))
		})
	}
}

// rss returns the process's resident set in bytes.
func rss(t *testing.T) uint64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Skip("no /proc")
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "VmRSS:" {
			kb, _ := strconv.ParseUint(f[1], 10, 64)
			return kb << 10
		}
	}
	t.Skip("no VmRSS")
	return 0
}

// TestSparsePetabyte is the adversary's file: truncate to 1 PiB, write a few bytes
// at the start, the middle and near the end, commit, read them back from a cold view
// and through a takeover. What it costs must follow the number of extents, not the
// logical size, and the ceilings are asserted: seconds, objects, bytes of metadata
// and memory.
func TestSparsePetabyte(t *testing.T) {
	const pib = int64(1) << 50
	be, c, v := newMem(t)
	be.Reset()
	in, err := v.Create(ctx, vfs.RootInode, "pib", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	rss0 := rss(t)
	start := time.Now()
	if err := f.Truncate(ctx, uint64(pib)); err != nil {
		t.Fatal(err)
	}
	spots := []struct {
		off  int64
		data []byte
	}{
		{0, []byte("start of a petabyte")},
		{pib / 2, content(1, 70_000)}, // a chunked run
		{pib/2 + 1<<30, []byte("a gibibyte on")},
		{pib - 31, []byte("near the very end!")},
	}
	for _, s := range spots {
		if _, err := f.WriteAt(ctx, s.data, s.off); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, v)
	took := time.Since(start)
	runtime.ReadMemStats(&m1)
	puts := be.Puts()
	var shardBytes int
	for k, b := range be.Dump() {
		if strings.HasPrefix(k, "meta/") {
			shardBytes += len(b)
		}
	}
	t.Logf("1 PiB file, %d written runs: truncate + writes + Flush in %v; %d shards (%d KiB), %d packs, %d head; heap +%d MiB, RSS +%d MiB",
		len(spots), took.Round(time.Millisecond), puts["meta"], shardBytes>>10, puts["pack"], puts["head"],
		(int64(m1.HeapInuse)-int64(m0.HeapInuse))>>20, (int64(rss(t))-int64(rss0))>>20)
	if took > 5*time.Second {
		t.Fatalf("truncate + 4 writes + Flush of a 1 PiB file took %v", took)
	}
	// One generation, or two when the bound or the journal started one before the Flush.
	if puts["meta"] > 12 || puts["pack"] > 2 || puts["head"] < 1 || puts["head"] > 2 {
		t.Fatalf("a 1 PiB file with %d extents wrote %d shards, %d packs, %d heads", len(spots), puts["meta"], puts["pack"], puts["head"])
	}
	if shardBytes > 64<<10 {
		t.Fatalf("a 1 PiB file's metadata is %d bytes of shards", shardBytes)
	}
	if grown := int64(m1.HeapInuse) - int64(m0.HeapInuse); grown > 64<<20 {
		t.Fatalf("heap grew %d MiB for a 1 PiB file", grown>>20)
	}
	if grown := int64(rss(t)) - int64(rss0); grown > 256<<20 {
		t.Fatalf("resident memory grew %d MiB for a 1 PiB file", grown>>20)
	}

	verify := func(what string, vol *vfs.Volume) {
		t.Helper()
		st, err := vol.Stat(ctx, in.ID)
		if err != nil || st.Size != uint64(pib) {
			t.Fatalf("%s: size %d, %v; want 1 PiB", what, st.Size, err)
		}
		h, err := vol.Open(ctx, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		for _, s := range spots {
			buf := make([]byte, len(s.data))
			if n, err := h.ReadAt(ctx, buf, s.off); (err != nil && err.Error() != "EOF") || n != len(buf) {
				t.Fatalf("%s: read at %d: %d bytes, %v", what, s.off, n, err)
			}
			mustEqual(t, fmt.Sprintf("%s: bytes at %d", what, s.off), buf, s.data)
		}
		for _, off := range []int64{1 << 20, pib / 4, pib/2 - 5000, pib/2 + 70_000, pib - 1<<20} {
			buf := bytes.Repeat([]byte{0xA5}, 4096)
			if _, err := h.ReadAt(ctx, buf, off); err != nil && err.Error() != "EOF" {
				t.Fatal(err)
			}
			if !bytes.Equal(buf, make([]byte, 4096)) {
				t.Fatalf("%s: the hole at %d is not zeros", what, off)
			}
		}
	}
	verify("live", v)
	verify("cold view", reopen(t, be, c))
	next, err := vfs.Attach(ctx, cfg(be, c, 2))
	if err != nil {
		t.Fatal(err)
	}
	verify("takeover", next)

	// Shrinking from a petabyte and extending again is as cheap, and exact.
	if err := next.Truncate(ctx, in.ID, uint64(pib/2+10)); err != nil {
		t.Fatal(err)
	}
	if err := next.Truncate(ctx, in.ID, uint64(pib)); err != nil {
		t.Fatal(err)
	}
	flush(t, next)
	h, _ := next.Open(ctx, in.ID)
	buf := bytes.Repeat([]byte{0xA5}, 64)
	if _, err := h.ReadAt(ctx, buf, pib/2+1<<30); err != nil && err.Error() != "EOF" {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, make([]byte, 64)) {
		t.Fatal("data past a shrink came back after the file was extended again")
	}
}

// TestFileSizeLimit: a file larger than MaxSize is refused, by a truncate and by a
// write, and nothing is changed.
func TestFileSizeLimit(t *testing.T) {
	_, _, v := newMem(t)
	in, _ := v.Create(ctx, vfs.RootInode, "f", 0o644)
	f, _ := v.Open(ctx, in.ID)
	defer f.Close()
	if err := f.Truncate(ctx, vfs.MaxSize+1); !errors.Is(err, vfs.ErrTooBig) {
		t.Fatalf("truncate past MaxSize = %v, want ErrTooBig", err)
	}
	if err := v.Truncate(ctx, in.ID, ^uint64(0)); !errors.Is(err, vfs.ErrTooBig) {
		t.Fatalf("truncate to 2^64-1 = %v, want ErrTooBig", err)
	}
	if _, err := f.WriteAt(ctx, []byte("xy"), vfs.MaxSize-1); !errors.Is(err, vfs.ErrTooBig) {
		t.Fatalf("write across MaxSize = %v, want ErrTooBig", err)
	}
	if st, _ := f.Stat(ctx); st.Size != 0 {
		t.Fatalf("a refused change moved the size to %d", st.Size)
	}
	if err := f.Truncate(ctx, vfs.MaxSize); err != nil {
		t.Fatalf("truncate to MaxSize: %v", err)
	}
	flush(t, v)
}
