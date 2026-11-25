package vfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
)

// Reseal without a place opens the Head "with the key alone, ignoring the context" (SPEC 5.4,
// way 3): unsealAny strips the 32 context bytes without looking at which kind of object they
// name. A file's chunk is bytes a guest chooses (a run after a hole is its own chunk, whatever its
// length), so a store writer can copy such a chunk's frame to the Head, and `vfs reseal` with
// no --from adopts it as the Head. The root-shard check cannot stop it for an empty root.
func TestResealWithoutAPlaceRefusesAnotherKindOfObject(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	var h Head
	h.Seq, h.Next = 99, 2
	var cid chunkID
	var vid volumeID // any: the attacker does not need the real id for an empty root
	frame, err := seal(c, chunkAAD(vid, cid), h.marshal())
	if err != nil {
		t.Fatal(err)
	}
	rec := append([]byte("fence\x00"), binary.BigEndian.AppendUint64(nil, 0)...)
	rec = append(rec, make([]byte, 16)...)
	rec = append(rec, frame...)
	if err := be.Put(ctx, "head", rec); err != nil {
		t.Fatal(err)
	}
	before := be.Dump()["head"]
	if err := Reseal(ctx, Config{Backend: be, Cipher: c, Round: 5, Journal: storetest.Journals(), JournalCapacity: 16 << 20, Disk: storetest.Disk}, ""); err == nil {
		t.Fatal("a chunk-kind object was adopted as the Head by a key-only Reseal: the Volume now reads as the attacker's empty tree")
	}
	if !bytes.Equal(be.Dump()["head"], before) {
		t.Fatal("a refused Reseal changed the Head")
	}
}
