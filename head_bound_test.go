package vfs

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

// SPEC 5.1 and 9: the `head` object is at most 65,536 bytes, and a writer MUST NOT write a longer one.
// A Head that a later build filled to within a few bytes of the bound grows with Seq and Next at
// every commit; the commit that would pass the bound is refused with ErrTooLarge and changes nothing,
// so no commit that the writer was told succeeded leaves a Volume that nobody can open.
func TestWriterNeverWritesAHeadItsReadersRefuse(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Backend: be, Cipher: c, Round: 1, Journal: storetest.Journals(), JournalCapacity: 16 << 20, Disk: storetest.Disk}
	v, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// measure the size of the record for a Tail of n bytes of value, and find the n at which the record is exactly maxHead.
	size := func(n int) int {
		h := v.head
		h.Tail = tlv(70, string(make([]byte, n)))
		sealed, err := seal(c, headAAD(be.String()), h.marshal())
		if err != nil {
			t.Fatal(err)
		}
		return 6 + 8 + 16 + len(sealed)
	}
	lo, hi := 0, 70000
	for lo < hi {
		m := (lo + hi + 1) / 2
		if size(m) <= maxHead {
			lo = m
		} else {
			hi = m - 1
		}
	}
	t.Logf("the largest tail value that fits the bound is %d bytes (record %d, bound %d)", lo, size(lo), maxHead)
	// A conformant reader accepts a Head at exactly the bound. Plant it directly (a later build wrote it).
	h := v.head
	h.Tail = tlv(70, string(make([]byte, lo)))
	sealed, _ := seal(c, headAAD(be.String()), h.marshal())
	if err := fence.New(be, maxHead).Put(ctx, headKey, sealed, 1); err != nil {
		t.Fatal(err)
	}
	cfg.Round = 2
	w, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatalf("a Head at exactly the bound is refused: %v", err)
	}
	// An ordinary writer commits once. Seq passes from <128 to >=128 if it is near the edge, so grow it: take the record
	// to maxHead-0 and make Seq cross a varint width boundary by planting Seq 127.
	_ = w
	h2 := w.head
	h2.Seq = 127
	h2.Tail = tlv(70, string(make([]byte, lo)))
	sealed2, _ := seal(c, headAAD(be.String()), h2.marshal())
	if err := fence.New(be, maxHead).Put(ctx, headKey, sealed2, 2); err != nil {
		t.Fatal(err)
	}
	cfg.Round = 3
	x, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatalf("attach at the edge: %v", err)
	}
	if _, err := x.Create(ctx, RootInode, "f", 0o644); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(be.Dump()["head"])
	flushErr := x.Flush(ctx) // seq 128: the Head grows by one byte
	if !errors.Is(flushErr, backend.ErrTooLarge) {
		t.Errorf("the commit that would pass the bound = %v, want ErrTooLarge", flushErr)
	}
	if !bytes.Equal(before, be.Dump()["head"]) {
		t.Error("a refused commit changed the Head")
	}
	// Another commit is refused the same way while the Head stays at the bound.
	if err := x.Flush(ctx); !errors.Is(err, backend.ErrTooLarge) {
		t.Errorf("a second commit = %v, want the same refusal", err)
	}
	if _, err := Open(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Errorf("Open after the refused commits: %v", err)
	}
	cfg.Round = 4
	if _, err := Attach(ctx, cfg); err != nil {
		t.Errorf("Attach after the refused commits: %v", err)
	}
}

// SPEC 5.1: a reader MUST refuse a Head whose Needs has a bit it does not know, and change nothing
// (ErrNeeds). Reseal has a branch for a Head that is already sealed to this place, which leaves it as it is
// and claims the Round: it reads the Head like any other first, so a build without the feature, asked
// to reseal from some other place, does not fence the Volume of a feature it cannot read.
func TestResealOfAlreadySealedHeadChangesNothingWhenItNeedsMore(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	raw := be.Dump()["head"]
	plain, err := unseal(c, headAAD(be.String()), raw[30:])
	if err != nil {
		t.Fatal(err)
	}
	h, _ := unmarshalHead(plain)
	h.Needs, h.Tail = 1<<3, tlv(3, "a layer")
	sealed, _ := seal(c, headAAD(be.String()), h.marshal())
	if err := fence.New(be, maxHead).Put(ctx, headKey, sealed, 4); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(be.Dump()["head"])
	err = Reseal(ctx, Config{Backend: be, Cipher: c, Round: 9, Journal: storetest.Journals(), JournalCapacity: 16 << 20, Disk: storetest.Disk}, "s3://some/other/place")
	after := be.Dump()["head"]
	if !errors.Is(err, ErrNeeds) || !bytes.Equal(before, after) {
		t.Errorf("a build that does not know bit 3 changed the Volume (round claimed) instead of refusing: err=%v changed=%v", err, !bytes.Equal(before, after))
	}
}
