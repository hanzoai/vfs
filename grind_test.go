package vfs

import (
	"context"
	"fmt"
	"io/fs"
	"testing"

	"github.com/hanzoai/vfs/internal/storetest"
)

// countShards writes the given names into a directory of a fresh Volume and reports
// how many shards and bytes of shards the commit wrote.
func countShards(t *testing.T, names []string) (shards, shardBytes int) {
	t.Helper()
	ctx := context.Background()
	be := storetest.NewMem()
	c := Hybrid(newHybridKey(t))
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := v.Mkdir(ctx, RootInode, "d", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	be.Reset()
	for _, n := range names {
		if _, err := v.Create(ctx, dir.ID, n, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for k, b := range be.Dump() {
		if len(k) > 5 && k[:5] == "meta/" {
			shardBytes += len(b)
		}
	}
	return be.Puts()["meta"], shardBytes
}

// TestGroundNamesDoNotBloatTheTree: where a node of the tree ends is decided by a hash
// of the entry's key. Without a secret in it, a guest who chooses file names (a tarball)
// can choose names that each end their own node, and multiply the metadata a commit
// writes. The hash is keyed from the Volume's seed, so names ground against any other
// key (here an attacker's: all zeros) land where they land.
func TestGroundNamesDoNotBloatTheTree(t *testing.T) {
	n := grindCount(300, 2000)
	attacker := newTree(nil, [32]byte{})
	var ground, plain []string
	ino := InodeID(3)
	for i := 0; len(ground) < n; i++ {
		name := fmt.Sprintf("g%x", i)
		if attacker.ends(0, entry{keyEntry(2, name), encodeEntry(ino+InodeID(len(ground)), fs.FileMode(0))}) {
			ground = append(ground, name)
		}
	}
	for i := range n {
		plain = append(plain, fmt.Sprintf("p%x", i))
	}
	pp, pb := countShards(t, plain)
	gp, gb := countShards(t, ground)
	t.Logf("%d ordinary names: %d shards (%d KiB); %d names ground against another key: %d shards (%d KiB)", n, pp, pb>>10, n, gp, gb>>10)
	// Unkeyed, every ground name ends a node: about n shards. Keyed, a few nodes end
	// where they happen to, and a draw of the seed that ends two or three more is not a
	// guest's doing: the bound is a tenth of the names, far from both.
	if gp > max(3*pp, n/10) {
		t.Errorf("names chosen by a guest multiply the shards written by %.1fx (%d vs %d): the node boundary is not keyed", float64(gp)/float64(pp), gp, pp)
	}
}

// TestBoundariesDependOnTheSeed: the same records cut at different places under
// different seeds, and always at the same places under one.
func TestBoundariesDependOnTheSeed(t *testing.T) {
	ctx := context.Background()
	build := func(seed [32]byte) ref {
		be := storetest.NewMem()
		sh := &shards{be: be, c: plainCipher{}, aad: bind("test"), cache: newLRU[ref, *node](1 << 30)}
		tr := newTree(sh, seed)
		var es []entry
		for i := range 20000 {
			es = append(es, entry{keyEntry(2, fmt.Sprintf("n%06d", i)), encodeEntry(InodeID(i+3), 0)})
		}
		r, err := tr.build(ctx, 0, es)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	a, b := [32]byte{1}, [32]byte{2}
	if build(a) != build(a) {
		t.Fatal("one seed built two trees from the same records")
	}
	if build(a) == build(b) {
		t.Fatal("two seeds built the same tree: the boundaries do not depend on the seed")
	}
}

// grindCount is the number of names to grind: a few hundred under -short, which is plenty
// to show the effect (an unkeyed tree writes a shard for each), and 2,000 otherwise.
func grindCount(short, full int) int {
	if testing.Short() {
		return short
	}
	return full
}
