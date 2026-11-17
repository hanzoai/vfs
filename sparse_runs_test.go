package vfs

// Sparse extents, byte-exact against a plain model across Flush, reopen and
// takeover, plus the structural invariants of the stored records and the claim
// that the chunk list is the cut of the written runs. In package vfs to read the
// extent records directly.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
)

func newHybridKey(t testing.TB) *age.HybridIdentity {
	t.Helper()
	k, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

type runModel struct {
	data    []byte
	written []bool
}

func (m *runModel) grow(n int) {
	if n > len(m.data) {
		m.data = append(m.data, make([]byte, n-len(m.data))...)
		m.written = append(m.written, make([]bool, n-len(m.written))...)
	}
}
func (m *runModel) write(off int, b []byte) {
	m.grow(off + len(b))
	copy(m.data[off:], b)
	for i := range b {
		m.written[off+i] = true
	}
}
func (m *runModel) truncate(n int) {
	if n < len(m.data) {
		m.data, m.written = m.data[:n], m.written[:n]
		return
	}
	m.grow(n)
}

// normalize applies what a settle of a file at most `small` bytes does: the whole
// file is stored inline, zeros included, so every byte of it counts as written.
func (m *runModel) normalize() {
	if len(m.data) <= small {
		for i := range m.written {
			m.written[i] = true
		}
	}
}

func runsExpected(g *gear, m *runModel) []extent {
	var out []extent
	size := len(m.data)
	for i := 0; i < size; {
		if !m.written[i] {
			i++
			continue
		}
		j := i
		for j < size && m.written[j] {
			j++
		}
		for pos := i; pos < j; {
			w := m.data[pos:min(pos+maxChunk, j)]
			n := cut(g, w)
			out = append(out, extent{off: uint64(pos), n: uint32(n), id: sumChunk(m.data[pos : pos+n])})
			pos += n
		}
		i = j
	}
	return out
}

func runsExtents(t *testing.T, v *Volume, ino InodeID) ([]extent, []byte, bool) {
	t.Helper()
	v.mu.RLock()
	defer v.mu.RUnlock()
	recs, err := v.scan(context.Background(), key(tData, uint64(ino)))
	if err != nil {
		t.Fatal(err)
	}
	var ext []extent
	var inline []byte
	hasInline := false
	for _, r := range recs {
		if len(r.key) == prefixLen {
			inline, hasInline = r.val, true
			continue
		}
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			t.Fatal(err)
		}
		ext = append(ext, e)
	}
	return ext, inline, hasInline
}

func runsCompare(t *testing.T, what string, v *Volume, ino InodeID, m *runModel, rng *rand.Rand) {
	t.Helper()
	ctx := context.Background()
	in, err := v.Stat(ctx, ino)
	if err != nil {
		t.Fatalf("%s: stat: %v", what, err)
	}
	if in.Size != uint64(len(m.data)) {
		t.Fatalf("%s: size %d, model %d", what, in.Size, len(m.data))
	}
	f, err := v.Open(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := bytes.Repeat([]byte{0xA5}, len(m.data)+7)
	n, err := f.ReadAt(ctx, buf, 0)
	if err != nil && err != io.EOF || n != len(m.data) {
		t.Fatalf("%s: read %d of %d: %v", what, n, len(m.data), err)
	}
	if !bytes.Equal(buf[:n], m.data) {
		i := 0
		for buf[i] == m.data[i] {
			i++
		}
		t.Fatalf("%s: differs at %d of %d: got %x want %x", what, i, n, buf[i], m.data[i])
	}
	for range 30 {
		if len(m.data) < 2 {
			break
		}
		off := rng.IntN(len(m.data))
		l := 1 + rng.IntN(min(len(m.data)-off+40, 1<<20))
		b := bytes.Repeat([]byte{0xA5}, l)
		n, err := f.ReadAt(ctx, b, int64(off))
		want := m.data[off:min(len(m.data), off+l)]
		if n != len(want) || !bytes.Equal(b[:n], want) {
			t.Fatalf("%s: window read at %d len %d: n=%d err=%v", what, off, l, n, err)
		}
		if n < l && err != io.EOF {
			t.Fatalf("%s: short read without EOF at %d", what, off)
		}
	}
}

// runsStructure checks the invariants of the stored records of one file.
func runsStructure(t *testing.T, what string, v *Volume, ino InodeID, size int) {
	t.Helper()
	ext, inline, hasInline := runsExtents(t, v, ino)
	if size <= small {
		if len(ext) != 0 {
			t.Fatalf("%s: size %d <= small but %d extent records", what, size, len(ext))
		}
		if len(inline) != size {
			t.Fatalf("%s: inline record %d bytes, size %d", what, len(inline), size)
		}
		return
	}
	if hasInline {
		t.Fatalf("%s: size %d > small but an inline record exists (%d bytes)", what, size, len(inline))
	}
	var prevEnd uint64
	for i, e := range ext {
		if e.n == 0 || e.n > maxChunk {
			t.Fatalf("%s: extent %d has length %d", what, i, e.n)
		}
		if i > 0 && e.off < prevEnd {
			t.Fatalf("%s: extents overlap/unsorted at %d (prev end %d, off %d)", what, i, prevEnd, e.off)
		}
		if e.end() > uint64(size) {
			t.Fatalf("%s: extent %d ends at %d past size %d", what, i, e.end(), size)
		}
		prevEnd = e.end()
		p, err := v.getChunk(context.Background(), v.root, e.id)
		if err != nil {
			t.Fatalf("%s: extent %d chunk: %v", what, i, err)
		}
		if len(p) != int(e.n) {
			t.Fatalf("%s: extent %d says %d bytes, chunk has %d", what, i, e.n, len(p))
		}
	}
}

func runsPick(rng *rand.Rand, m *runModel) int {
	size := len(m.data)
	marks := []int{0, 1, small - 1, small, small + 1, minChunk - 1, minChunk, minChunk + 1, avgChunk, maxChunk - 1, maxChunk, maxChunk + 1, 2 * maxChunk, size, size + 1, size - 1}
	// run edges
	for i := 1; i < size; i += 1 + size/64 {
		if m.written[i] != m.written[i-1] {
			marks = append(marks, i-1, i, i+1)
		}
	}
	switch rng.IntN(5) {
	case 0:
		return max(0, marks[rng.IntN(len(marks))]+rng.IntN(5)-2)
	case 1:
		return rng.IntN(runScale(22 << 20))
	case 2:
		if size > 0 {
			return rng.IntN(size + 1)
		}
	case 3:
		return size + rng.IntN(1<<20)
	}
	return max(0, marks[rng.IntN(len(marks))])
}

func runsRun(t *testing.T, seed uint64, canonical bool) {
	rng := rand.New(rand.NewPCG(seed, 0xBEEF))
	ctx := context.Background()
	be := storetest.NewMem()
	c := Hybrid(newHybridKey(t))
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	mk := func(round uint64) *Volume {
		cf := Config{Backend: be, Cipher: c, Round: round, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}
		if !canonical {
			cf.Dirty, cf.Overlay, cf.Cache = 150<<10, 5, 1<<20
		}
		v, err := Attach(ctx, cf)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	round := uint64(1)
	v := mk(round)
	in, err := v.Create(ctx, RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := v.Open(ctx, in.ID)
	m := &runModel{}
	check := func(what string) {
		t.Helper()
		runsCompare(t, what+" live", v, in.ID, m, rng)
	}
	settle := func(what string) {
		t.Helper()
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		m.normalize()
		runsCompare(t, what+" flushed", v, in.ID, m, rng)
		runsStructure(t, what+" flushed", v, in.ID, len(m.data))
		if canonical {
			ext, _, _ := runsExtents(t, v, in.ID)
			if len(m.data) > small {
				want := runsExpected(v.gear, m)
				if fmt.Sprint(ext) != fmt.Sprint(want) {
					t.Fatalf("%s: stored chunk list (%d extents) is not the cut of the written runs (%d extents)\nstored: %v\nwant:   %v", what, len(ext), len(want), briefRuns(ext), briefRuns(want))
				}
			}
		}
		cold, err := Open(ctx, Config{Backend: be, Cipher: c})
		if err != nil {
			t.Fatal(err)
		}
		runsCompare(t, what+" cold", cold, in.ID, m, rng)
		round++
		v = mk(round)
		runsCompare(t, what+" takeover", v, in.ID, m, rng)
		f, err = v.Open(ctx, in.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	for step := 0; step < 70; step++ {
		what := fmt.Sprintf("seed %d step %d", seed, step)
		switch op := rng.IntN(20); {
		case op < 9:
			off := runsPick(rng, m)
			n := 1 + rng.IntN(100)
			switch rng.IntN(4) {
			case 0:
				n = 1 + rng.IntN(300_000)
			case 1:
				n = 1 + rng.IntN(runScale(5<<20))
			}
			var b []byte
			if rng.IntN(5) == 0 {
				b = make([]byte, n) // written zeros: data, not a hole
			} else {
				b = make([]byte, n)
				for i := range b {
					b[i] = byte(rng.Uint32()) | 1
				}
			}
			if _, err := f.WriteAt(ctx, b, int64(off)); err != nil {
				t.Fatal(err)
			}
			m.write(off, b)
		case op < 12:
			n := runsPick(rng, m)
			if n > len(m.data) {
				n = len(m.data) + rng.IntN(1<<20)
			}
			if err := f.Truncate(ctx, uint64(n)); err != nil {
				t.Fatal(err)
			}
			m.truncate(n)
		case op < 14:
			n := len(m.data) + rng.IntN(runScale(8<<20))
			if err := v.Truncate(ctx, in.ID, uint64(n)); err != nil {
				t.Fatal(err)
			}
			m.truncate(n)
		case op < 17:
			check(what)
		default:
			settle(what)
		}
	}
	settle(fmt.Sprintf("seed %d end", seed))
}

func briefRuns(es []extent) string {
	s := ""
	for i, e := range es {
		if i > 12 {
			s += fmt.Sprintf("...(+%d)", len(es)-i)
			break
		}
		s += fmt.Sprintf("[%d+%d]", e.off, e.n)
	}
	return s
}

func TestSparseCanonicalRuns(t *testing.T) {
	n := 30
	if testing.Short() {
		n = 3
	}
	for seed := range n {
		t.Run(fmt.Sprint("canon", seed), func(t *testing.T) { t.Parallel(); runsRun(t, uint64(seed), true) })
	}
}

func TestSparseTinyBounds(t *testing.T) {
	n := 40
	if testing.Short() {
		n = 3
	}
	for seed := range n {
		t.Run(fmt.Sprint("tiny", seed), func(t *testing.T) { t.Parallel(); runsRun(t, uint64(1000+seed), false) })
	}
}

// runScale shrinks the sizes a run draws under -short, where the same operations on
// files a quarter as large still cross every boundary that matters (the inline limit,
// the minimum and maximum chunk) and cost a fraction as much under the race detector.
func runScale(n int) int {
	if testing.Short() {
		return max(n/4, 2*maxChunk+1)
	}
	return n
}
