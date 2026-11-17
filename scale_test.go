package vfs_test

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
)

// populate creates dirs directories of perDir small files each and returns their
// inodes by [dir][i].
func populate(t testing.TB, v *vfs.Volume, dirs, perDir int) [][]vfs.InodeID {
	all := make([][]vfs.InodeID, dirs)
	for d := range dirs {
		dir := mkdir(t, v, vfs.RootInode, fmt.Sprintf("dir%04d", d))
		for i := range perDir {
			all[d] = append(all[d], write(t, v, dir, fmt.Sprintf("file%05d.txt", i), []byte(fmt.Sprintf("file %d/%d: a few bytes of content\n", d, i))).ID)
		}
	}
	return all
}

// TestHundredThousandFiles creates and stats 100,000 files, then changes ten of
// them and checks that the Sync wrote the shards on the ten paths and not the
// rest of the tree.
func TestHundredThousandFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	be, c, v := newMem(t)
	start := time.Now()
	inodes := populate(t, v, 100, 1000)
	created := time.Since(start)

	be.Reset()
	start = time.Now()
	flush(t, v)
	first := be.Puts()
	t.Logf("create 100,000 files: %v; first Flush %v wrote %d shards, %d packs, %d head", created.Round(time.Millisecond), time.Since(start).Round(time.Millisecond), first["meta"], first["pack"], first["head"])
	// The Volume publishes as it goes, so the first Flush wrote what was left: the tree is what the
	// store holds of it (every generation's shards are there, an older version of a node among them).
	shardsTotal := 0
	for k := range be.Dump() {
		if strings.HasPrefix(k, "meta/") {
			shardsTotal++
		}
	}
	if n := packBytes(be); n != 0 {
		t.Fatalf("small files made %d bytes of packs; they are inline", n)
	}

	// A cold volume: stat every file by path.
	ro := reopen(t, be, c)
	start = time.Now()
	n := 0
	for d := range 100 {
		dir, err := ro.Lookup(ctx, vfs.RootInode, fmt.Sprintf("dir%04d", d))
		if err != nil {
			t.Fatal(err)
		}
		for i := range 1000 {
			in, err := ro.Lookup(ctx, dir.ID, fmt.Sprintf("file%05d.txt", i))
			if err != nil || in.ID != inodes[d][i] || in.IsDir() || in.Size == 0 {
				t.Fatalf("lookup dir%04d/file%05d: %+v %v", d, i, in, err)
			}
			n++
		}
	}
	t.Logf("cold lookup+stat of %d files: %v (%.1f µs each)", n, time.Since(start).Round(time.Millisecond), float64(time.Since(start).Microseconds())/float64(n))

	// Change ten files, spread across ten directories.
	rng := rand.New(rand.NewPCG(7, 7))
	for k := range 10 {
		d, i := k*10+rng.IntN(10), rng.IntN(1000)
		f, err := v.Open(ctx, inodes[d][i])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(ctx, []byte("CHANGED"), 5); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	be.Reset()
	start = time.Now()
	flush(t, v)
	changed := be.Puts()
	t.Logf("Flush after changing 10 of 100,000 files wrote %d shards, %d packs, %d head of a tree of %d shards (%v)", changed["meta"], changed["pack"], changed["head"], shardsTotal, time.Since(start).Round(time.Millisecond))
	if changed["head"] != 1 || changed["pack"] != 0 {
		t.Fatalf("the Flush wrote %d heads and %d packs, want 1 and 0", changed["head"], changed["pack"])
	}
	// Each changed file dirties the shard holding its inode and the shard holding
	// its bytes; the levels above them are shared. A whole-tree rewrite would be all
	// of the shards.
	// Where a node ends is drawn from the Volume's seed, so the count varies by a few
	// from one run to the next.
	if limit := 10*2 + 10; changed["meta"] > limit {
		t.Fatalf("ten changed files wrote %d shards, want at most %d", changed["meta"], limit)
	}
	if shardsTotal < 150 {
		t.Fatalf("the tree has only %d shards; the comparison proves little", shardsTotal)
	}
}

// TestMillionInodes measures memory and time at a million inodes: how long they
// take to create and commit, what the Volume holds in memory afterwards (the heap
// less the bytes of the in-memory store it sits on), what a cold reader holds, and
// what ten changes cost in a tree this size. It fails only if the Volume cannot
// hold them or the cost grows.
func TestMillionInodes(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	be, c, v := newMem(t)
	var ms runtime.MemStats
	heap := func() uint64 {
		runtime.GC()
		runtime.ReadMemStats(&ms)
		return ms.HeapInuse
	}
	base := heap()
	var peak int64
	var sample []vfs.InodeID // one file in a thousand, for the change below
	start := time.Now()
	const dirs, perDir = 1000, 1000
	for d := range dirs {
		dir := mkdir(t, v, vfs.RootInode, fmt.Sprintf("d%04d", d))
		for i := range perDir {
			in, err := v.Create(ctx, dir, fmt.Sprintf("f%05d", i), 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if i == 500 {
				sample = append(sample, in.ID)
			}
		}
		if d%100 == 99 {
			flush(t, v)
			runtime.ReadMemStats(&ms)
			peak = max(peak, int64(ms.HeapInuse)-int64(base)-int64(storeBytes(be))) // signed: the heap can shrink under the store
		}
	}
	elapsed := time.Since(start)
	st := v.Stats()
	stored := uint64(storeBytes(be))
	held := int64(heap()) - int64(base) - int64(stored)
	t.Logf("%d inodes created and committed in %v; the Volume holds %d MiB (peak %d MiB at a commit; the cache bound is %d MiB); the store holds %d objects, %d MiB",
		st.Inodes, elapsed.Round(time.Millisecond), held>>20, peak>>20, st.CacheMax>>20, be.Count(), stored>>20)

	// Ten changes, spread across the tree.
	for k := range 10 {
		f, err := v.Open(ctx, sample[k*100+17])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(ctx, []byte("changed"), 0); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	be.Reset()
	flush(t, v)
	t.Logf("ten changed files in 1,000,000: the commit wrote %d shards", be.Puts()["meta"])
	if be.Puts()["meta"] > 25 {
		t.Fatalf("ten changes wrote %d shards at a million inodes", be.Puts()["meta"])
	}

	before := heap()
	ro := reopen(t, be, c)
	start = time.Now()
	for _, d := range []int{0, 500, 999} {
		dir, err := ro.Lookup(ctx, vfs.RootInode, fmt.Sprintf("d%04d", d))
		if err != nil {
			t.Fatal(err)
		}
		ents, err := ro.ReadDir(ctx, dir.ID)
		if err != nil || len(ents) != perDir {
			t.Fatalf("readdir: %d entries, %v", len(ents), err)
		}
	}
	t.Logf("cold open and three listings of 1,000 entries: %v; the reader holds %d KiB", time.Since(start).Round(time.Millisecond), (heap()-before)>>10)
	runtime.KeepAlive(ro)
	runtime.KeepAlive(v)
}

// changeTen changes ten files spread over the Volume and returns the shards the
// Flush wrote.
func changeTen(t *testing.T, be interface {
	Puts() map[string]int
	Reset()
}, v *vfs.Volume, inodes [][]vfs.InodeID) int {
	rng := rand.New(rand.NewPCG(9, 9))
	for k := range 10 {
		d := (k * len(inodes) / 10) + rng.IntN(len(inodes)/10)
		f, err := v.Open(ctx, inodes[d][rng.IntN(len(inodes[d]))])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(ctx, []byte("CHANGED"), 5); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	be.Reset()
	flush(t, v)
	return be.Puts()["meta"]
}

// TestSyncCostDoesNotGrowWithTheTree: the same ten changes cost about the same
// number of shards in a Volume of 40,000 files and one three times larger. (In a
// tree smaller than the change, ten changes share shards and cost less; the cost
// saturates at about two shards per change plus the levels above them.)
func TestSyncCostDoesNotGrowWithTheTree(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	cost := map[int]int{}
	for _, files := range []int{40000, 120000} {
		be, _, v := newMem(t)
		inodes := populate(t, v, 40, files/40)
		flush(t, v)
		cost[files] = changeTen(t, be, v, inodes)
		t.Logf("%d files in %d shards: ten changes wrote %d shards", files, be.Count()-1, cost[files])
	}
	if cost[120000] > cost[40000]+10 {
		t.Fatalf("ten changes cost %d shards at 120,000 files against %d at 40,000: the cost grows with the tree", cost[120000], cost[40000])
	}
}
