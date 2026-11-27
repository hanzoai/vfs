package vfs_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

func keysWith(be *storetest.Mem, prefix string) []string {
	var out []string
	for _, k := range be.Keys() {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

// flipEvery turns one bit of every object under prefix, as a store that lost a write
// would hold it: the bytes are the wrong bytes and the key still names them.
func flipEvery(t *testing.T, be *storetest.Mem, prefix string) []string {
	t.Helper()
	keys := keysWith(be, prefix)
	if len(keys) == 0 {
		t.Fatalf("no object under %s", prefix)
	}
	for _, k := range keys {
		b := be.Dump()[k]
		b[len(b)/2] ^= 1
		if err := be.Put(ctx, k, b); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func TestVerifyClean(t *testing.T) {
	be, c, v := newMem(t)
	write(t, v, vfs.RootInode, "small", content(1, 8<<10)) // inline in a shard
	write(t, v, vfs.RootInode, "big", content(2, 300<<10)) // a chunk in a pack
	flush(t, v)
	v.Close()

	ro := reopen(t, be, c)
	defer ro.Close()
	before := be.Dump()
	r, err := ro.Verify(ctx, vfs.VerifyOptions{All: true})
	if err != nil {
		t.Fatalf("verify a whole Volume: %v", err)
	}
	// Verify is a reader. A maintenance pass that repairs what it meets is a pass that can
	// destroy what it cannot read, so the store must be the same object after it as before.
	after := be.Dump()
	if len(after) != len(before) {
		t.Fatalf("verify changed the store: %d objects before, %d after", len(before), len(after))
	}
	for k, b := range before {
		if !bytes.Equal(after[k], b) {
			t.Fatalf("verify rewrote %s", k)
		}
	}
	if !r.OK() {
		t.Fatalf("verify a whole Volume reported %d faults, first %v", len(r.Damaged), r.Damaged[0])
	}
	if r.Shards < 1 || r.Chunks == 0 || r.Inline != 1 || r.Packs == 0 {
		t.Fatalf("read %d shards, %d packs, %d chunks, %d inline files: want every class", r.Shards, r.Packs, r.Chunks, r.Inline)
	}
	if r.InlineBytes != 8<<10 {
		t.Fatalf("inline bytes %d, want %d", r.InlineBytes, 8<<10)
	}
	if r.Shards > len(keysWith(be, "meta/")) || r.Packs > len(keysWith(be, "pack/")) {
		t.Fatalf("read %d shards and %d packs, the store holds %d and %d",
			r.Shards, r.Packs, len(keysWith(be, "meta/")), len(keysWith(be, "pack/")))
	}
	if r.Bytes == 0 {
		t.Fatal("read no bytes")
	}
}

// TestVerifyReadsWhatTheCheapChecksDoNot is the reason Verify exists: a pack whose bytes
// changed under a chunk no file has read since passes Check (the namespace is intact) and
// passes Reseal with Verify (that reads every shard and Stats every pack, and a flipped bit
// changes neither), and only a read of a chunk or a Verify sees it.
func TestVerifyReadsWhatTheCheapChecksDoNot(t *testing.T) {
	be, c, v := newMem(t)
	big := write(t, v, vfs.RootInode, "big", content(3, 400<<10))
	small := write(t, v, vfs.RootInode, "small", content(4, 8<<10))
	flush(t, v)
	v.Close()

	packs := keysWith(be, "pack/")
	if len(packs) == 0 {
		t.Fatal("the write made no pack")
	}
	b := be.Dump()[packs[0]]
	b[len(b)/2] ^= 1
	if err := be.Put(ctx, packs[0], b); err != nil {
		t.Fatal(err)
	}

	ro := reopen(t, be, c)
	defer ro.Close()
	if err := ro.Check(ctx); err != nil {
		t.Fatalf("Check refused a Volume whose only damage is inside a pack: %v", err)
	}
	// The full check a Reseal makes: every shard read, every pack Stat'ed. It passes.
	full := cfg(be, c, 2)
	full.Verify = true
	if err := vfs.Reseal(ctx, full, ""); err != nil {
		t.Fatalf("Reseal with Verify refused a Volume whose only damage is inside a pack: %v", err)
	}

	r, err := ro.Verify(ctx, vfs.VerifyOptions{})
	if err == nil || !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("Verify of a corrupted pack: %v, want ErrCorrupt", err)
	}
	if len(r.Damaged) != 1 {
		t.Fatalf("Verify reported %d faults, want 1: %v", len(r.Damaged), r.Damaged)
	}
	if f := r.Damaged[0]; f.Kind != vfs.ChunkSeal || f.Key != packs[0] || f.N == 0 || f.Chunk == ([32]byte{}) {
		t.Fatalf("fault %+v, want a chunk that does not open inside %s", f, packs[0])
	}

	// The damage is in a pack, so the file held in a shard is untouched and the chunked one is not.
	if got := readAll(t, ro, small.ID); !bytes.Equal(got, content(4, 8<<10)) {
		t.Fatal("the file held in a shard changed with a pack")
	}
	f, err := ro.Open(ctx, big.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.ReadAt(ctx, make([]byte, 4096), 0); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("reading a chunk out of a corrupted pack: %v, want an error", err)
	}
}

func TestVerifyCatchesShardDamage(t *testing.T) {
	t.Run("bit flip", func(t *testing.T) {
		be, c, v := newMem(t)
		write(t, v, vfs.RootInode, "a", content(5, 200<<10))
		flush(t, v)
		v.Close()
		flipEvery(t, be, "meta/")

		ro := reopen(t, be, c)
		defer ro.Close()
		r, err := ro.Verify(ctx, vfs.VerifyOptions{})
		if !errors.Is(err, vfs.ErrCorrupt) || len(r.Damaged) != 1 || r.Damaged[0].Kind != vfs.ShardName {
			t.Fatalf("Verify: %v %v, want one shard that does not hash to its name", err, r.Damaged)
		}
		// This is the class the full check a Reseal makes does cover.
		full := cfg(be, c, 2)
		full.Verify = true
		if err := vfs.Reseal(ctx, full, ""); !errors.Is(err, vfs.ErrCorrupt) {
			t.Fatalf("Reseal with Verify of a corrupted shard: %v, want ErrCorrupt", err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		be, c, v := newMem(t)
		write(t, v, vfs.RootInode, "a", content(6, 200<<10))
		flush(t, v)
		v.Close()
		for _, k := range keysWith(be, "meta/") {
			if err := be.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
		ro := reopen(t, be, c)
		defer ro.Close()
		r, err := ro.Verify(ctx, vfs.VerifyOptions{})
		if !errors.Is(err, vfs.ErrCorrupt) || len(r.Damaged) != 1 || r.Damaged[0].Kind != vfs.ShardMissing {
			t.Fatalf("Verify: %v %v, want one missing shard", err, r.Damaged)
		}
	})
}

func TestVerifyCatchesPackDamage(t *testing.T) {
	t.Run("truncated", func(t *testing.T) {
		be, c, v := newMem(t)
		write(t, v, vfs.RootInode, "a", content(7, 300<<10))
		flush(t, v)
		v.Close()
		packs := keysWith(be, "pack/")
		if len(packs) != 1 {
			t.Fatalf("%d packs, want 1", len(packs))
		}
		if err := be.Put(ctx, packs[0], be.Dump()[packs[0]][:16]); err != nil {
			t.Fatal(err)
		}
		ro := reopen(t, be, c)
		defer ro.Close()
		r, err := ro.Verify(ctx, vfs.VerifyOptions{})
		if !errors.Is(err, vfs.ErrCorrupt) || len(r.Damaged) != 1 || r.Damaged[0].Kind != vfs.PackShort {
			t.Fatalf("Verify: %v %v, want one short pack", err, r.Damaged)
		}
		// The namespace is intact, so Check says nothing about it.
		if err := ro.Check(ctx); err != nil {
			t.Fatalf("Check: %v", err)
		}
	})
	t.Run("missing, every pack counted", func(t *testing.T) {
		be, c, v := newMem(t)
		// Two packs: the packer sends when its buffer reaches 16 MiB.
		write(t, v, vfs.RootInode, "a", content(8, 10<<20))
		write(t, v, vfs.RootInode, "b", content(9, 10<<20))
		flush(t, v)
		v.Close()
		packs := keysWith(be, "pack/")
		if len(packs) < 2 {
			t.Fatalf("%d packs, want at least 2", len(packs))
		}
		for _, k := range packs {
			if err := be.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
		ro := reopen(t, be, c)
		defer ro.Close()
		r, err := ro.Verify(ctx, vfs.VerifyOptions{All: true})
		if !errors.Is(err, vfs.ErrCorrupt) {
			t.Fatalf("Verify: %v, want ErrCorrupt", err)
		}
		if len(r.Damaged) != len(packs) {
			t.Fatalf("Verify reported %d faults for %d packs, want one each", len(r.Damaged), len(packs))
		}
		for _, f := range r.Damaged {
			if f.Kind != vfs.PackMissing {
				t.Fatalf("fault %v, want every pack missing", f)
			}
		}
		if r.Chunks != 0 {
			t.Fatalf("%d chunks verified with every pack gone", r.Chunks)
		}
	})
}
