package vfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/fence"
)

func headVolume(t *testing.T) (*storetest.Mem, Cipher, Config) {
	t.Helper()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(context.Background(), Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	return be, c, Config{Backend: be, Cipher: c, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}
}

// putRaw replaces the Head's plaintext with exactly plain (sealed to the place), at round 0.
func headPutRaw(t *testing.T, be *storetest.Mem, c Cipher, plain []byte) {
	t.Helper()
	sealed, err := seal(c, headAAD(be.String()), plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.New(be, maxHead).Put(context.Background(), headKey, sealed, 0); err != nil {
		t.Fatal(err)
	}
}

func headPlain(t *testing.T, be *storetest.Mem, c Cipher) []byte {
	t.Helper()
	raw := be.Dump()["head"]
	plain, err := unseal(c, headAAD(be.String()), raw[30:])
	if err != nil {
		t.Fatal(err)
	}
	return plain
}

// SPEC 5.1: every combination of (tail absent/present) x (Needs clear / each unknown bit / all bits),
// through every door: Open, Attach, Reseal with and without the place.
func TestHeadMatrix(t *testing.T) {
	ctx := context.Background()
	for _, tail := range []string{"", tlv(7, "later"), tlv(60, string(bytes.Repeat([]byte{0xff}, 40))) + tlv(61, "x")} {
		for _, needs := range []uint64{0, 1, 2, 4, 1 << 3, 1 << 63, ^uint64(0)} {
			be, c, cfg := headVolume(t)
			base := headPlain(t, be, c)
			h, err := unmarshalHead(base)
			if err != nil {
				t.Fatal(err)
			}
			h.Journal, h.Needs, h.Tail = 55, needs, tail
			headPutRaw(t, be, c, h.marshal())
			before := be.Dump()["head"]
			c1 := cfg
			c1.Round = 9
			name := fmt.Sprintf("tail=%q needs=%#x", tail, needs)
			_, e1 := Open(ctx, cfg)
			_, e2 := Attach(ctx, c1)
			e3 := Reseal(ctx, c1, be.String())
			e4 := Reseal(ctx, c1, "")
			for i, e := range []error{e1, e2, e3, e4} {
				if needs != 0 {
					if !errors.Is(e, ErrNeeds) {
						t.Errorf("%s door %d: %v, want ErrNeeds", name, i, e)
					}
				}
			}
			if needs != 0 && !bytes.Equal(be.Dump()["head"], before) {
				t.Errorf("%s: a refused door wrote", name)
			}
			if needs == 0 {
				if e1 != nil {
					t.Errorf("%s: Open %v", name, e1)
				}
				// e2 landed (claimed round 9): state of store changed; e3/e4 need a higher round
				if e2 != nil {
					t.Errorf("%s: Attach %v", name, e2)
				}
			}
		}
	}
}

// SPEC 5.1: a Head read from the STORE with a Journal, and a Tail the build does not understand, is
// carried through Attach and an ordinary commit, byte for byte. The reader builds the next Head from
// the one it read, so nothing a later build appended is dropped.
func TestTailSurvivesAnOrdinaryWriter(t *testing.T) {
	ctx := context.Background()
	be, c, cfg := headVolume(t)
	h, _ := unmarshalHead(headPlain(t, be, c))
	value := make([]byte, 300)
	rfill(rand.New(rand.NewPCG(5, 6)), value)
	tail := tlv(30, "small") + tlv(31, string(value)) + tlv(1<<20, "")
	h.Seq, h.Journal, h.Tail = 9, 123456789, tail
	headPutRaw(t, be, c, h.marshal())

	cfg.Round = 4
	v, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := v.Create(ctx, RootInode, fmt.Sprintf("f%d", i), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		got, err := unmarshalHead(headPlain(t, be, c))
		if err != nil {
			t.Fatal(err)
		}
		if got.Tail != tail || got.Journal != 123456789+uint64(i)+1 || got.Needs != 0 {
			t.Fatalf("commit %d dropped or changed a field: journal=%d tail ok=%v needs=%d", i, got.Journal, got.Tail == tail, got.Needs)
		}
		if got.Seq <= 9 {
			t.Fatalf("seq %d", got.Seq)
		}
	}
	// A takeover and Reseal carry the plaintext verbatim.
	cfg.Round = 7
	if _, err := Attach(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	before := headPlain(t, be, c)
	cfg.Round = 8
	if err := Reseal(ctx, cfg, be.String()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, headPlain(t, be, c)) {
		t.Fatal("reseal changed the plaintext of the Head")
	}
}

// SPEC 5.1 and 1: fuzz the parser. Every truncation, bit flip, and overlong varint: no panic, and anything accepted
// re-marshals to bytes that parse to the same Head.
func TestHeadParserFuzz(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 12))
	var seen, accepted, noncanon int
	for range 300000 {
		var h Head
		h.Seq, h.Next, h.Journal = r.Uint64()>>uint(r.IntN(64)), r.Uint64()>>uint(r.IntN(64)), r.Uint64()>>uint(r.IntN(64))
		rfill(r, h.Root[:])
		rfill(r, h.ID[:])
		rfill(r, h.Seed[:])
		h.Tail = tlv(uint64(r.IntN(500)), string(bytes.Repeat([]byte{byte(r.Uint32())}, r.IntN(5))))
		b := h.marshal()
		switch r.IntN(5) {
		case 0:
			b = b[:r.IntN(len(b)+1)]
		case 1:
			b[r.IntN(len(b))] ^= 1 << r.IntN(8)
		case 2:
			// overlong: replace the first byte (Seq) by a long continuation run
			b = append(bytes.Repeat([]byte{0xff}, r.IntN(12)), b...)
		case 3:
			i := r.IntN(len(b))
			b = append(b[:i:i], append([]byte{0x80}, b[i:]...)...)
		}
		seen++
		got, err := unmarshalHead(b)
		if err != nil {
			continue
		}
		accepted++
		if !bytes.Equal(got.marshal(), b) {
			noncanon++
		}
		again, err := unmarshalHead(got.marshal())
		if err != nil || again != got {
			t.Fatalf("accepted %x but its canonical form %x does not read back: %v", b, got.marshal(), err)
		}
		if got.Needs != 0 {
			t.Fatalf("accepted a Head with Needs %#x", got.Needs)
		}
	}
	t.Logf("seen %d accepted %d accepted-but-not-canonical %d", seen, accepted, noncanon)
	if noncanon != 0 {
		t.Errorf("%d Heads were accepted that are not in their shortest form: two byte strings are one Head", noncanon)
	}
	// 2^64-1 and 2^64
	for _, n := range []uint64{1<<64 - 1, 1 << 63} {
		_, err := unmarshalHead(Head{Needs: n}.marshal())
		if !errors.Is(err, ErrNeeds) {
			t.Errorf("Needs %#x: %v", n, err)
		}
	}
	over := binary.AppendUvarint(nil, 1<<64-1)
	over[len(over)-1] = 0x02 // 2^64 + ... overflows
	b := Head{}.marshal()
	b = append(b[:len(b)-1], over...)
	if _, err := unmarshalHead(b); err == nil {
		t.Errorf("an overflowing Needs was accepted")
	}
}

func rfill(r *rand.Rand, b []byte) {
	for i := range b {
		b[i] = byte(r.Uint32())
	}
}

// SPEC 5.1: what a commit must carry from the Head it read: ID, Seed and Needs as well as Journal and Tail.
func TestCommitCarriesIdentity(t *testing.T) {
	ctx := context.Background()
	be, c, cfg := headVolume(t)
	before, _ := unmarshalHead(headPlain(t, be, c))
	cfg.Round = 2
	v, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Create(ctx, RootInode, "f", 0o644); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := unmarshalHead(headPlain(t, be, c))
	if err != nil {
		t.Fatal(err)
	}
	if after.ID != before.ID || after.Seed != before.Seed || after.Seed == ([32]byte{}) || after.Needs != before.Needs {
		t.Fatalf("a commit changed the identity of the Volume: id %x->%x seed %x->%x", before.ID, after.ID, before.Seed[:4], after.Seed[:4])
	}
	if after.Root == before.Root || after.Next <= before.Next {
		t.Fatalf("root/next did not advance")
	}
}

// SPEC 5.4: the full check of a copy takes the END of the furthest chunk of a pack, whatever order the chunk
// table lists them in: a pack cut one byte short of its last chunk is refused.
func TestVerifyChecksTheFurthestChunk(t *testing.T) {
	ctx := context.Background()
	refused := 0
	for seed := range 12 {
		be, c, cfg := headVolume(t)
		cfg.Round = 2
		v, err := Attach(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 6 {
			in, _ := v.Create(ctx, RootInode, fmt.Sprintf("f%d", i), 0o644)
			f, _ := v.Open(ctx, in.ID)
			b := make([]byte, 80<<10)
			rfill(rand.New(rand.NewPCG(uint64(seed), uint64(i))), b)
			if _, err := f.WriteAt(ctx, b, 0); err != nil {
				t.Fatal(err)
			}
			f.Close()
		}
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		var key string
		for k := range be.Dump() {
			if len(k) > 5 && k[:5] == "pack/" {
				key = k
			}
		}
		raw := be.Dump()[key]
		if err := be.Put(ctx, key, raw[:len(raw)-1]); err != nil {
			t.Fatal(err)
		}
		cfg.Round = 3
		cfg.Verify = true
		if err := Reseal(ctx, cfg, be.String()); err == nil {
			t.Errorf("seed %d: a pack one byte short of its last chunk passed the full check", seed)
		} else {
			refused++
		}
		_ = c
	}
	t.Logf("refused %d of 12", refused)
}
