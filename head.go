package vfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/hanzoai/vfs/internal/blake3"
)

// headKey is the one mutable object of a Volume. Everything else is written
// once under a name that is a hash or a random id, and never changes.
const headKey = "head"

// Head names the committed state of a Volume: the root of its metadata tree, the
// next inode number, and the Volume's identity. It is small and is replaced by
// one conditional write, the commit: the tree it names is exactly what the Volume
// was at that instant, so a crash before the write leaves the previous Head, and
// so the previous tree, intact. The Round that owns the Volume rides in the fence
// header around the sealed Head, where the store's conditional write can enforce it.
type Head struct {
	// Seq numbers commit attempts, strictly increasing. It names a state: no two
	// Heads a Volume writes share one. It can skip, since an attempt whose reply
	// was lost may or may not have landed, and the next attempt takes the next
	// number either way.
	Seq  uint64
	Root ref    // root of the metadata tree
	Next uint64 // next inode number to assign

	// ID names the Volume. Every shard and chunk is sealed to it, so an object
	// copied in from another Volume does not open. Seed draws the Volume's
	// chunking table, which is why chunk lengths are not comparable across Volumes.
	ID   volumeID
	Seed [32]byte

	// Journal is the highest sequence of the node journal (docs/journal.md) whose
	// effects this Head includes: what the store holds of it. 0 when there is none.
	Journal uint64

	// Needs is a set of feature bits a reader must know to read the Volume at all. A
	// build refuses a Head with a bit it does not know (ErrNeeds), so a change that
	// every writer must honour, or every collector, is announced by a bit and never
	// reaches a build that would break it. Bit 2 (links) is the one defined.
	Needs uint64

	// Tail is what follows Needs: fields that later builds append, each [tag][length]
	// [value] with tags strictly increasing (SPECIFICATION 5.1). This build understands
	// none, skips every one and writes them back unchanged.
	Tail string
}

// ErrNeeds is returned for a Volume whose Head announces a feature this build does
// not have. Nothing was read or changed.
var ErrNeeds = errors.New("vfs: the Volume needs a feature this build does not have")

// knownNeeds is the set of Needs bits this build understands. Bit N announces the field
// of the Tail with tag N; links is the one defined.
const knownNeeds uint64 = needsLinks

// maxTailFields bounds the number of fields of a Tail.
const maxTailFields = 256

type volumeID [16]byte

func (h Head) marshal() []byte {
	var w writer
	w.uvarint(h.Seq)
	w.raw(h.Root[:])
	w.uvarint(h.Next)
	w.raw(h.ID[:])
	w.raw(h.Seed[:])
	w.uvarint(h.Journal)
	w.uvarint(h.Needs)
	w.raw([]byte(h.Tail))
	return w.b
}

// unmarshalHead reads a Head as this build does: it knows the bits of knownNeeds.
func unmarshalHead(b []byte) (Head, error) { return parseHead(b, knownNeeds) }

// parseHead reads a Head as a build that knows the Needs bits of known. In this order: a
// fixed field that is short or not in its shortest form is ErrCorrupt; a Needs bit that
// is not in known is ErrNeeds, whatever the Tail holds; then the Tail must be whole and
// in order, and hold the field of every bit of Needs, or it is ErrCorrupt. A table of
// known bits, where the build holds a constant, is how the day a bit is defined is
// exercised before it comes.
func parseHead(b []byte, known uint64) (Head, error) {
	r := reader{b: b}
	h := Head{Seq: r.uvarint()}
	copy(h.Root[:], r.raw(32))
	h.Next = r.uvarint()
	copy(h.ID[:], r.raw(16))
	copy(h.Seed[:], r.raw(32))
	h.Journal = r.uvarint()
	h.Needs = r.uvarint()
	if r.err != nil {
		return Head{}, r.err
	}
	if unknown := h.Needs &^ known; unknown != 0 {
		return Head{}, fmt.Errorf("%w: bits %#x", ErrNeeds, unknown)
	}
	if err := checkTail(r.b, h.Needs); err != nil {
		return Head{}, err
	}
	h.Tail = string(r.b)
	return h, nil
}

// checkTail checks that b is a Tail: fields [tag uvarint][length uvarint][value], each
// whole, with tags strictly increasing, at most maxTailFields of them, and a field for
// every bit of needs. It looks inside no value: what a field holds is its build's.
func checkTail(b []byte, needs uint64) error {
	r := reader{b: b}
	var last, seen uint64
	for n := 0; len(r.b) > 0; n++ {
		tag := r.uvarint()
		r.bytes()
		switch {
		case r.err != nil:
			return fmt.Errorf("%w: a field of the Head's tail is not whole", ErrCorrupt)
		case n > 0 && tag <= last:
			return fmt.Errorf("%w: Head field %d after %d", ErrCorrupt, tag, last)
		case n >= maxTailFields:
			return fmt.Errorf("%w: a Head of more than %d fields", ErrCorrupt, maxTailFields)
		}
		last = tag
		if tag < 64 {
			seen |= 1 << tag
		}
	}
	if missing := needs &^ seen; missing != 0 {
		return fmt.Errorf("%w: the Head needs fields %#x that it does not carry", ErrCorrupt, missing)
	}
	return nil
}

// bindLen is the length of what every object is sealed to.
const bindLen = 32

// bind returns what an object is sealed to: the BLAKE3 digest of its kind and of the
// fields that name it, each framed by its length. It is a fixed length and
// injective, so no context is a prefix of another and two different contexts never
// share a digest; a reader compares it for equality.
func bind(kind string, fields ...[]byte) []byte {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs seal context")
	frame := func(p []byte) {
		_, _ = h.Write(binary.AppendUvarint(nil, uint64(len(p))))
		_, _ = h.Write(p)
	}
	frame([]byte(kind))
	for _, f := range fields {
		frame(f)
	}
	return h.Sum(nil)
}

// What each kind of sealed object is sealed to. A Head is sealed to the place it
// lives (the backend's name), so a Volume copied to another place does not open
// there until Reseal; shards and chunks are sealed to the Volume, so they move with
// it, and a chunk is sealed to its own id as well.
func headAAD(place string) []byte { return bind("head", []byte(place)) }

func shardAAD(id volumeID) []byte { return bind("shard", id[:]) }

func chunkAAD(id volumeID, c chunkID) []byte { return bind("chunk", id[:], c[:]) }

// readHead opens a fenced Head payload.
func (v *Volume) readHead(payload []byte) (Head, error) {
	plain, err := unseal(v.c, headAAD(v.be.String()), payload)
	if errors.Is(err, errContext) {
		return Head{}, fmt.Errorf("vfs: open head at %s: %w (a Head is sealed to the place it was written at: if the Volume was moved, `vfs reseal` it)", v.be, err)
	}
	if err != nil {
		return Head{}, fmt.Errorf("vfs: open head: %w", err)
	}
	return unmarshalHead(plain)
}

// writeHead seals h and replaces the Head at the Volume's round.
func (v *Volume) writeHead(ctx context.Context, h Head) error {
	sealed, err := seal(v.c, headAAD(v.be.String()), h.marshal())
	if err != nil {
		return err
	}
	return v.fence.Put(ctx, headKey, sealed, v.round)
}
