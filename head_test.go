package vfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/fence"
)

// docs/journal.md has the publisher commit "the Head carrying g", the highest journal
// sequence the store holds, and the mount read "the Head's journal sequence". The Head has
// the field, and a field a later build appends after it is read, not refused.
func TestTheHeadHasRoomForTheJournalSequence(t *testing.T) {
	h := Head{Seq: 3, Next: 9, Journal: 70_000}
	got, err := unmarshalHead(h.marshal())
	if err != nil || got.Journal != 70_000 {
		t.Fatalf("a Head with a journal sequence reads back as %+v, %v", got, err)
	}
	field := tlv(1000, "a field of a later build")
	later, err := unmarshalHead(append(h.marshal(), field...))
	if err != nil || later.Journal != 70_000 || later.Tail != field {
		t.Fatalf("a Head with a field after Needs reads as %+v, %v", later, err)
	}
}

// tlv is one field of a Head's tail: [tag uvarint][length uvarint][value].
func tlv(tag uint64, value string) string {
	return string(binary.AppendUvarint(binary.AppendUvarint(nil, tag), uint64(len(value)))) + value
}

// The bits and the tail that a Head carried are the ones the next commit writes: a build
// that does not change them does not lose them. The journal sequence is the journal's: the
// first change of the first generation is number 1.
func TestHeadFieldsSurviveACommit(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk})
	if err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	v.head.Tail = tlv(40, "a field") + tlv(41, "of a later") + tlv(1<<40, "build")
	v.mu.Unlock()
	if _, err := v.Create(ctx, RootInode, "f", 0o644); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r, err := Open(ctx, Config{Backend: be, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	if r.head.Journal != 1 || r.head.Tail != tlv(40, "a field")+tlv(41, "of a later")+tlv(1<<40, "build") {
		t.Fatalf("after a commit the Head has Journal %d, Tail %q", r.head.Journal, r.head.Tail)
	}
}

// Open, Attach and Reseal read the Head, and each refuses one that needs what this build
// lacks, without changing the store.
func TestHeadOfALaterBuildIsRefusedByEveryDoor(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	sealed, err := seal(c, headAAD(be.String()), Head{Seq: 5, Next: 2, Needs: 1}.marshal())
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.New(be, maxHead).Put(ctx, headKey, sealed, 0); err != nil {
		t.Fatal(err)
	}
	before := be.Dump()["head"]
	cfg := func(round uint64) Config {
		return Config{Backend: be, Cipher: c, Round: round, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}
	}
	if _, err := Open(ctx, cfg(0)); !errors.Is(err, ErrNeeds) {
		t.Errorf("Open = %v", err)
	}
	if _, err := Attach(ctx, cfg(9)); !errors.Is(err, ErrNeeds) {
		t.Errorf("Attach = %v", err)
	}
	if err := Reseal(ctx, cfg(9), be.String()); !errors.Is(err, ErrNeeds) {
		t.Errorf("Reseal = %v", err)
	}
	if err := Reseal(ctx, cfg(9), ""); !errors.Is(err, ErrNeeds) {
		t.Errorf("Reseal without a place = %v", err)
	}
	if !bytes.Equal(be.Dump()["head"], before) {
		t.Error("a refused door changed the Head")
	}
}
