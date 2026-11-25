package vfs_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// slow adds a fixed latency to every mutating call and every Get, the shape of
// a store on a LAN (a few ms) or the internet (tens of ms).
type slow struct {
	backend.Backend
	d time.Duration
}

func (s slow) Get(c context.Context, k string, max int64) ([]byte, backend.Version, error) {
	time.Sleep(s.d)
	return s.Backend.Get(c, k, max)
}
func (s slow) GetRange(c context.Context, k string, o, n int64) ([]byte, error) {
	time.Sleep(s.d)
	return s.Backend.GetRange(c, k, o, n)
}
func (s slow) Put(c context.Context, k string, d []byte) error {
	time.Sleep(s.d)
	return s.Backend.Put(c, k, d)
}
func (s slow) PutIfAbsent(c context.Context, k string, d []byte) (backend.Version, error) {
	time.Sleep(s.d)
	return s.Backend.PutIfAbsent(c, k, d)
}
func (s slow) PutIfVersion(c context.Context, k string, d []byte, v backend.Version) (backend.Version, error) {
	time.Sleep(s.d)
	return s.Backend.PutIfVersion(c, k, d, v)
}

// TestCommitPerClose measures what a FUSE close() costs: the mount calls
// Flush on every close (HandleFlusher), and Flush is a full commit. It creates
// 500 files of 20 KiB the way `git checkout` or `tar x` does (create, write,
// close) with a Flush after each close, and counts the objects written. The
// numbers are in docs/PRODUCTION-READINESS.md; a cheaper close is the node journal.
func TestCommitPerClose(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement: the scale step runs it without the race detector")
	}
	files := sized(100, 500)
	for _, lat := range []time.Duration{0, 5 * time.Millisecond} {
		mem := storetest.NewMem()
		be := slow{mem, lat}
		c := vfsAge(t)
		if err := vfs.Create(ctx, cfg(be, c, 0)); err != nil {
			t.Fatal(err)
		}
		v, err := vfs.Attach(ctx, cfg(be, c, 1))
		if err != nil {
			t.Fatal(err)
		}
		dir := mkdir(t, v, vfs.RootInode, "tree")
		flush(t, v)
		mem.Reset()
		start := time.Now()
		for i := range files {
			write(t, v, dir, fmt.Sprintf("f%04d", i), content(uint64(i), 20<<10))
			flush(t, v) // what handle.Flush does on close(2)
		}
		took := time.Since(start)
		p := mem.Puts()
		t.Logf("latency %v: %d files, Flush per close: %v total, %.1f files/s; objects written: %d heads, %d shards, %d packs = %d (%.1f per file); batch would be ~%d",
			lat, files, took.Round(time.Millisecond), float64(files)/took.Seconds(), p["head"], p["meta"], p["pack"], mem.Total(), float64(mem.Total())/float64(files), 10)
	}
}

// TestAppendWithFsyncAmplification: a log file that grows by 4 KiB and is
// fsynced each time. Each Flush re-cuts the file's last chunk, so it re-seals
// and re-uploads the whole tail chunk, and leaves the old one as garbage. That
// amplification is measured and bounded here by what one chunk can cost; a journal
// that makes fsync cheaper is a later change.
func TestAppendWithFsyncAmplification(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement: the scale step runs it without the race detector")
	}
	mem := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, cfg(mem, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	in, _ := v.Create(ctx, vfs.RootInode, "wal", 0o644)
	f, _ := v.Open(ctx, in.ID)
	// A 64 MiB base so the file has a settled chunk list.
	base := content(5, 64<<20)
	if _, err := f.WriteAt(ctx, base, 0); err != nil {
		t.Fatal(err)
	}
	flush(t, v)
	mem.Reset()
	before := packBytes(mem)
	const n = 200
	off := int64(len(base))
	start := time.Now()
	for i := range n {
		if _, err := f.WriteAt(ctx, content(uint64(100+i), 4096), off); err != nil {
			t.Fatal(err)
		}
		off += 4096
		flush(t, v)
	}
	took := time.Since(start)
	grew := packBytes(mem) - before
	p := mem.Puts()
	t.Logf("%d appends of 4 KiB with a Flush each: %v; wrote %d packs, %d shards, %d heads; pack bytes written %d KiB for %d KiB of data = %.0fx",
		n, took.Round(time.Millisecond), p["pack"], p["meta"], p["head"], grew>>10, n*4, float64(grew)/float64(n*4096))
	// Each Flush uploads the tail chunk, at most maxChunk plus sealing, for 4 KiB.
	if limit := float64(4<<20+8<<10) / 4096; float64(grew)/float64(n*4096) > limit {
		t.Fatalf("write amplification %.0fx on an fsync'd append exceeds one chunk per Flush (%.0fx)", float64(grew)/float64(n*4096), limit)
	}
}

// TestColdReadAmplification: a 4 KiB read of a cold file fetches the whole
// sealed chunk.
func TestColdReadAmplification(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement: the scale step runs it without the race detector")
	}
	mem := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, _ := vfs.Attach(ctx, cfg(mem, c, 1))
	in := write(t, v, vfs.RootInode, "big", content(9, 32<<20))
	flush(t, v)
	cr := &rangeCount{Backend: mem}
	ro, err := vfs.Open(ctx, cfg(cr, c, 0))
	if err != nil {
		t.Fatal(err)
	}
	f, _ := ro.Open(ctx, in.ID)
	buf := make([]byte, 4096)
	for _, off := range []int64{1 << 20, 9 << 20, 17<<20 + 5, 30 << 20} {
		before := cr.bytes
		if _, err := f.ReadAt(ctx, buf, off); err != nil {
			t.Fatal(err)
		}
		t.Logf("4 KiB read at %d MiB fetched %d KiB (%.0fx)", off>>20, (cr.bytes-before)>>10, float64(cr.bytes-before)/4096)
	}
	// A read fetches the chunk that holds it, and no more than one chunk.
	if cr.bytes/4 > 4<<20+8<<10 {
		t.Fatalf("a 4 KiB cold read fetched %d KiB on average, more than one chunk", cr.bytes/4>>10)
	}
}

type rangeCount struct {
	backend.Backend
	bytes int64
}

func (r *rangeCount) GetRange(c context.Context, k string, o, n int64) ([]byte, error) {
	r.bytes += n
	return r.Backend.GetRange(c, k, o, n)
}

// TestChunkedFilesCommitCost: the shard-cost claims (ten changed files among
// 100,000 = 21 shards) are measured on inline files. Files just over the inline
// threshold are chunked; each new chunk adds a record to the chunk table, keyed
// by a hash, so the records of ten changes land in ten unrelated leaves.
func TestChunkedFilesCommitCost(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement: the scale step runs it without the race detector")
	}
	if testing.Short() {
		t.Skip("scale")
	}
	mem := storetest.NewMem()
	c := vfsAge(t)
	if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
		t.Fatal(err)
	}
	v, _ := vfs.Attach(ctx, cfg(mem, c, 1))
	const dirs, per = 30, 1000
	var inodes [][]vfs.InodeID
	for d := range dirs {
		dir := mkdir(t, v, vfs.RootInode, fmt.Sprint("d", d))
		var row []vfs.InodeID
		for i := range per {
			row = append(row, write(t, v, dir, fmt.Sprintf("f%04d", i), content(uint64(d*per+i), 17<<10)).ID)
		}
		row = append(row, 0)
		inodes = append(inodes, row[:per])
		if d%5 == 4 {
			flush(t, v)
		}
	}
	flush(t, v)
	t.Logf("%d files of 17 KiB (chunked): %d objects stored", dirs*per, mem.Count())
	mem.Reset()
	for k := range 10 {
		d := k * 3
		f, _ := v.Open(ctx, inodes[d][(k*97)%per])
		if _, err := f.WriteAt(ctx, content(uint64(900000+k), 17<<10), 0); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	flush(t, v)
	p := mem.Puts()
	t.Logf("ten rewritten chunked files among %d: the commit wrote %d shards, %d packs, %d head (inline files: 14 to 21 shards)", dirs*per, p["meta"], p["pack"], p["head"])
	// Each changed chunked file dirties its inode, its chunk reference and a chunk
	// table record keyed by a hash, three leaves, besides the path above them.
	if p["meta"] > 40 {
		t.Fatalf("ten changed chunked files wrote %d shards; the documented cost is about 30 (at most 40)", p["meta"])
	}
}

// checkoutSize is the size of file i of a source-tree-like checkout: mostly small
// files, a tail of large ones; about 57 KiB on average.
func checkoutSize(i int) int {
	r := rand.New(rand.NewPCG(uint64(i), 1234))
	switch p := r.IntN(100); {
	case p < 55:
		return 100 + r.IntN(4<<10)
	case p < 85:
		return 4<<10 + r.IntN(28<<10)
	case p < 97:
		return 32<<10 + r.IntN(224<<10)
	}
	return 256<<10 + r.IntN(1792<<10)
}

func storedBytes(be *storetest.Mem) int {
	n := 0
	for _, b := range be.Dump() {
		n += len(b)
	}
	return n
}

// TestCheckoutCost writes a 5,000-file checkout twice: with a Flush after every
// file, which is what the mount does on each close(2), and with one Flush at the end.
// It reports the objects and bytes the store ends up holding.
func TestCheckoutCost(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement: the scale step runs it without the race detector")
	}
	files := sized(1000, 5000)
	for _, perClose := range []bool{true, false} {
		mem := storetest.NewMem()
		c := vfsAge(t)
		if err := vfs.Create(ctx, cfg(mem, c, 0)); err != nil {
			t.Fatal(err)
		}
		conf := cfg(mem, c, 1)
		conf.JournalCapacity = 512 << 20 // match production pressure (128 MiB) for a 285 MiB checkout
		v, err := vfs.Attach(ctx, conf)
		if err != nil {
			t.Fatal(err)
		}
		defer v.Close()
		var data int
		start := time.Now()
		var dirs []vfs.InodeID
		for d := range files / 50 {
			dirs = append(dirs, mkdir(t, v, vfs.RootInode, fmt.Sprintf("d%03d", d)))
		}
		mem.Reset()
		for i := range files {
			n := checkoutSize(i)
			data += n
			write(t, v, dirs[i%len(dirs)], fmt.Sprintf("file%05d", i), content(uint64(i), n))
			if perClose {
				flush(t, v)
			}
		}
		flush(t, v)
		took := time.Since(start)
		p := mem.Puts()
		stored := storedBytes(mem)
		what := "one Flush"
		if perClose {
			what = "Flush per close"
		}
		t.Logf("%s: %d files, %.1f MiB of data in %v: %d heads, %d shards, %d packs written = %d objects (%.2f per file); the store holds %d objects and %.1f MiB (%.2fx the data)",
			what, files, float64(data)/(1<<20), took.Round(time.Millisecond), p["head"], p["meta"], p["pack"], mem.Total(), float64(mem.Total())/float64(files),
			mem.Count(), float64(stored)/(1<<20), float64(stored)/float64(data))
		if !perClose && mem.Total() > files/10 {
			t.Fatalf("one Flush of %d files wrote %d objects", files, mem.Total())
		}
		// A commit per file rewrites the metadata leaves it touches, and the old ones
		// stay until a collector exists: the store holds several times the data. One
		// Flush leaves little behind. These are ceilings on what is measured, not goals.
		limit := 1.5
		if perClose {
			limit = 8
		}
		if amp := float64(stored) / float64(data); amp > limit {
			t.Fatalf("%s: the store holds %.2fx the data, over %.1fx", what, amp, limit)
		}
	}
}
