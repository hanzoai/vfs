package vfs

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/fence"
)

// forgeWith seals plain||tag(key) to the public recipient only, the way a party that holds
// nothing secret can. The frame is [len id][id][age file] as 4.2 says.
func forgeWith(t *testing.T, k *age.HybridIdentity, plain []byte, key []byte, withTag bool) []byte {
	t.Helper()
	r, err := age.ParseHybridRecipient(k.Recipient().String())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, r)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(plain)
	if withTag {
		h, _ := blake3.NewKeyed(key)
		h.Write(plain)
		w.Write(h.Sum(nil))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	id := ageKeyID(k)
	return append(append([]byte{byte(len(id))}, id...), buf.Bytes()...)
}

// Every key a party holding only public data could use for the tag: none of them verifies.
func TestNoPublicTagKeyVerifies(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	aad := headAAD("file:///x")
	plain := append(append([]byte{}, aad...), Head{Seq: 9}.marshal()...)
	derive := func(ctx string, material string) []byte {
		var out [32]byte
		blake3.DeriveKey(ctx, []byte(material), out[:])
		return out[:]
	}
	sum := func(s string) []byte { x := blake3.Sum256([]byte(s)); return x[:] }
	for name, key := range map[string][]byte{
		"zero":                      make([]byte, 32),
		"derive(recipient)":         derive("hanzoai/vfs seal tag", k.Recipient().String()),
		"derive(key id)":            derive("hanzoai/vfs seal tag", ageKeyID(k)),
		"derive(constant)":          derive("hanzoai/vfs seal tag", "vfs"),
		"derive(empty)":             derive("hanzoai/vfs seal tag", ""),
		"hash(recipient)":           sum(k.Recipient().String()),
		"hash(key id)":              sum(ageKeyID(k)),
		"hash(seal tag)":            sum("hanzoai/vfs seal tag"),
		"derive(other ctx, recip.)": derive("hanzoai/vfs", k.Recipient().String()),
	} {
		frame := forgeWith(t, k, plain, key, true)
		if _, err := unseal(c, aad, frame); err == nil {
			t.Errorf("an object tagged with %s was accepted: the tag is not a secret", name)
		}
	}
}

// A keyless writer can make an age file of any plaintext, including one shorter than a tag.
// Opening it must be an error and never a panic that takes the process down.
func TestShortForgedPlaintextIsRefusedNotPanicked(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	for n := 0; n <= 40; n++ {
		frame := forgeWith(t, k, make([]byte, n), make([]byte, 32), false)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%d byte plaintext: Open panicked: %v", n, r)
				}
			}()
			if _, err := unseal(c, headAAD("x"), frame); err == nil {
				t.Errorf("%d byte plaintext accepted", n)
			}
		}()
	}
}

// 4.2: the context is compared whole. A context that differs in one bit anywhere is refused.
func TestContextIsComparedWhole(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	aad := headAAD("s3://b/p")
	frame, err := seal(c, aad, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range aad {
		for bit := 0; bit < 8; bit += 7 {
			other := append([]byte{}, aad...)
			other[i] ^= 1 << bit
			if _, err := unseal(c, other, frame); err == nil {
				t.Errorf("a context differing at byte %d bit %d was accepted", i, bit)
			}
		}
	}
}

// 4.1: every Seal is checked. A Cipher that adds a header and then returns its input is not sealing.
type headerCipher struct{ down bool }

func (h *headerCipher) Seal(p []byte) ([]byte, string, error) {
	if h.down {
		return append([]byte("HDR"), p...), "k", nil
	}
	return append([]byte("HDR"), bytes.Repeat([]byte{0xAA}, len(p))...), "k", nil
}
func (h *headerCipher) Open(s []byte, _ string) ([]byte, error) { return nil, fmt.Errorf("no") }

func TestSealCatchesAHeaderedFallback(t *testing.T) {
	h := &headerCipher{down: true}
	if _, err := seal(h, headAAD("x"), []byte("secret")); err == nil {
		t.Error("a Cipher that prefixes a header to its input was accepted")
	}
	// and the same bytes anywhere inside the output
	mid := &midCipher{}
	if _, err := seal(mid, headAAD("x"), []byte("secret")); err == nil {
		t.Error("a Cipher that wraps its input in a trailer was accepted")
	}
}

type midCipher struct{}

func (midCipher) Seal(p []byte) ([]byte, string, error) {
	return append(append(bytes.Repeat([]byte{1}, 40), p...), bytes.Repeat([]byte{2}, 40)...), "k", nil
}
func (midCipher) Open(s []byte, _ string) ([]byte, error) { return nil, fmt.Errorf("no") }

// Create draws an identity and a seed of its own for every Volume.
func TestCreateDrawsIdentity(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	var ids [][16]byte
	var seeds [][32]byte
	for range 4 {
		be := storetest.NewMem()
		if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
			t.Fatal(err)
		}
		v, err := Open(ctx, Config{Backend: be, Cipher: c})
		if err != nil {
			t.Fatal(err)
		}
		ids, seeds = append(ids, v.head.ID), append(seeds, v.head.Seed)
	}
	for i := range ids {
		if ids[i] == ([16]byte{}) || seeds[i] == ([32]byte{}) {
			t.Fatalf("volume %d has a zero id or seed", i)
		}
		for j := range i {
			if ids[i] == ids[j] || seeds[i] == seeds[j] {
				t.Fatalf("volumes %d and %d share an id or a seed", i, j)
			}
		}
	}
}

// Reseal: the last round bricks the Volume (no round can follow); a Head that does not decode is
// not sealed for a new place.
func TestResealRefusesTheLastRound(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	be := storetest.NewMem()
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	before := be.Dump()["head"]
	if err := Reseal(ctx, Config{Backend: be, Cipher: c, Round: math.MaxUint64, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}, be.String()); err == nil {
		t.Error("Reseal at 2^64-1 succeeded: no writer can attach to that Volume again")
	}
	if !bytes.Equal(be.Dump()["head"], before) {
		t.Error("a refused Reseal changed the Head")
	}
}

func TestResealRefusesAGarbageHead(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	be := storetest.NewMem()
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	// a key holder's bug writes a Head record that does not decode
	sealed, err := seal(c, headAAD(be.String()), []byte{0xff, 0xff, 0xff})
	if err != nil {
		t.Fatal(err)
	}
	st := fence.New(be, maxHead)
	if err := st.Put(ctx, headKey, sealed, 0); err != nil {
		t.Fatal(err)
	}
	before := be.Dump()["head"]
	if err := Reseal(ctx, Config{Backend: be, Cipher: c, Round: 5, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}, be.String()); err == nil {
		t.Error("Reseal sealed a Head that does not decode")
	}
	if !bytes.Equal(be.Dump()["head"], before) {
		t.Error("a refused Reseal changed the Head")
	}
}

// forgeTagged is an age file of plaintext followed by exactly the tag given, which a holder of
// the key (here, the test) can make.
func forgeTagged(t *testing.T, k *age.HybridIdentity, plain, tag []byte) []byte {
	t.Helper()
	r, err := age.ParseHybridRecipient(k.Recipient().String())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, r)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(plain)
	w.Write(tag)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	id := ageKeyID(k)
	return append(append([]byte{byte(len(id))}, id...), buf.Bytes()...)
}

// 4.1: the tag is 32 bytes and every one of them is checked: a tag that is right in its first
// half and wrong anywhere in its second does not open. The tag of the key and of the plaintext is
// recomputed from the text (BLAKE3 keyed, key = DeriveKey of the identity string).
func TestEveryByteOfTheTagIsChecked(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	aad := headAAD("s3://b/p")
	plain := append(append([]byte{}, aad...), Head{Seq: 4}.marshal()...)
	var key [32]byte
	blake3.DeriveKey("hanzoai/vfs seal tag", []byte(k.String()), key[:])
	h, _ := blake3.NewKeyed(key[:])
	h.Write(plain)
	good := h.Sum(nil)
	if len(good) != tagLen || tagLen != 32 {
		t.Fatalf("the tag is %d bytes, the text says 32", len(good))
	}
	if _, err := unseal(c, aad, forgeTagged(t, k, plain, good)); err != nil {
		t.Fatalf("the right tag does not open: %v", err)
	}
	for i := range good {
		for _, bit := range []byte{1, 0x80} {
			bad := append([]byte{}, good...)
			bad[i] ^= bit
			if _, err := unseal(c, aad, forgeTagged(t, k, plain, bad)); err == nil {
				t.Errorf("a tag that differs in byte %d (bit %#x) opened", i, bit)
			}
		}
	}
	// a short tag, and a long one, are not the tag
	for _, n := range []int{0, 16, 31, 33} {
		tag := append(append([]byte{}, good...), make([]byte, 40)...)[:n]
		if _, err := unseal(c, aad, forgeTagged(t, k, plain, tag)); err == nil {
			t.Errorf("a tag of %d bytes opened", n)
		}
	}
}
