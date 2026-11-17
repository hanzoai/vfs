package vfs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
)

// SPEC 5.1: the Tail is fields [tag][length][value] with strictly increasing tags. A reader
// refuses one that is not, and a Head that needs a field and lacks it.
func TestHeadTailRules(t *testing.T) {
	good := map[string]string{
		"none":                    "",
		"one":                     tlv(9, "x"),
		"tag 0":                   tlv(0, "x"),
		"empty value":             tlv(9, ""),
		"increasing":              tlv(1, "a") + tlv(2, "") + tlv(63, "c") + tlv(64, "d") + tlv(1<<63, "e"),
		"the largest tag":         tlv(1<<64-1, "x"),
		"a value of 60,000 bytes": tlv(300, strings.Repeat("v", 60_000)),
		"256 fields": func() string {
			var s string
			for i := range 256 {
				s += tlv(uint64(i), "")
			}
			return s
		}(),
	}
	for name, tail := range good {
		h := Head{Seq: 1, Next: 2, Tail: tail}
		got, err := unmarshalHead(h.marshal())
		if err != nil || got != h {
			t.Errorf("%s: %v", name, err)
		}
	}
	bad := map[string]string{
		"a duplicate tag":        tlv(5, "a") + tlv(5, "b"),
		"a tag that goes down":   tlv(6, "a") + tlv(5, "b"),
		"a tag that goes down 0": tlv(1, "") + tlv(0, ""),
		"a length past the end":  string([]byte{9, 5, 'a', 'b'}),
		"a tag without length":   string([]byte{9}),
		"a stray byte":           tlv(9, "x") + "\x01",
		"a stray byte alone":     "\x00",
		"a tag not shortest":     string([]byte{0x89, 0x00, 0x00}),
		"a length not shortest":  string([]byte{9, 0x80, 0x00}),
		"257 fields": func() string {
			var s string
			for i := range 257 {
				s += tlv(uint64(i), "")
			}
			return s
		}(),
	}
	for name, tail := range bad {
		h := Head{Seq: 1, Next: 2, Tail: tail}
		if _, err := unmarshalHead(h.marshal()); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v, want ErrCorrupt", name, err)
		}
	}
	// A Head that needs what this build lacks is refused for that, whatever its tail is.
	for _, tail := range []string{"", tlv(5, "a") + tlv(5, "b"), "\x01"} {
		h := Head{Seq: 1, Next: 2, Needs: 1 << 3, Tail: tail}
		if _, err := unmarshalHead(h.marshal()); !errors.Is(err, ErrNeeds) {
			t.Errorf("a Head that needs bit 3 with tail %q: %v, want ErrNeeds", tail, err)
		}
	}
}

// SPEC 5.1: bit N of Needs means that the field with tag N is there. No bit is known to this
// build, so the rule is held on the check itself.
func TestNeededFieldsMustBePresent(t *testing.T) {
	for _, c := range []struct {
		needs uint64
		tail  string
		ok    bool
	}{
		{0, "", true},
		{1 << 5, tlv(5, "x"), true},
		{1 << 5, tlv(4, "x") + tlv(5, "") + tlv(6, "x"), true},
		{1 << 5, "", false},
		{1 << 5, tlv(4, "x") + tlv(6, "x"), false},
		{1<<5 | 1<<7, tlv(5, "x"), false},
		{1<<5 | 1<<7, tlv(5, "x") + tlv(7, "y"), true},
		{1 << 63, tlv(63, "x"), true},
		{1 << 63, tlv(64, "x"), false},
		{1 << 63, tlv(1<<63, "x"), false},
		{1 << 5, tlv(69, "x"), false}, // tag 69 is not tag 5, whatever its low bits are
		{1 << 0, tlv(64, "x"), false},
		{1 << 63, tlv(127, "x"), false},
		{1 << 5, tlv(5+64, "x") + tlv(5+128, "y"), false},
	} {
		err := checkTail([]byte(c.tail), c.needs)
		if c.ok != (err == nil) {
			t.Errorf("needs %#x, tail %q: %v", c.needs, c.tail, err)
		}
		if err != nil && !errors.Is(err, ErrCorrupt) {
			t.Errorf("needs %#x: %v is not ErrCorrupt", c.needs, err)
		}
	}
}

// The full check of a copy finds a file whose chunk is in no table: an extent that refers to a
// chunk the chunk table does not know, which no read of the shards alone shows.
func TestVerifyFindsAnExtentWithoutItsChunk(t *testing.T) {
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
	in, _ := v.Create(ctx, RootInode, "big", 0o644)
	f, _ := v.Open(ctx, in.ID)
	data := make([]byte, 200_000)
	for i := range data {
		data[i] = byte(i*7 + i>>8)
	}
	if _, err := f.WriteAt(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	verify := func(round uint64) error {
		return Reseal(ctx, Config{Backend: be, Cipher: c, Round: round, Journal: storetest.Journals(), Capacity: 64 << 20, Verify: true}, be.String())
	}
	if err := verify(5); err != nil {
		t.Fatalf("a whole Volume does not pass the full check: %v", err)
	}
	// the check wrote the Head at its own round, which fences the writer: the next one is above
	if v, err = Attach(ctx, Config{Backend: be, Cipher: c, Round: 6, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}); err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	var id chunkID
	if err := v.t.scan(ctx, v.root, []byte{tChunk}, func(k, _ []byte) bool { copy(id[:], k[1:]); return false }); err != nil {
		t.Fatal(err)
	}
	root, err := v.t.apply(ctx, v.root, []mut{{key: keyChunk(id), del: true}})
	h := v.head
	v.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	h.Seq, h.Root = h.Seq+1, root
	if err := v.writeHead(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := verify(7); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("the full check of a file whose chunk is in no table = %v, want ErrCorrupt", err)
	}
	if err := Reseal(ctx, Config{Backend: be, Cipher: c, Round: 8, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}, be.String()); err != nil {
		t.Fatalf("the short check reads no chunk table and passes: %v", err)
	}
}
