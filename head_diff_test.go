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

// ---- a reference decoder written from SPEC 1 and 5.1 alone ----------------------------------

// refUv reads an unsigned LEB128: at most 10 bytes, shortest form, fits 64 bits.
func refUv(b []byte) (v uint64, n int, ok bool) {
	for i := 0; i < 10; i++ {
		if i >= len(b) {
			return 0, 0, false // truncated
		}
		c := b[i]
		if i == 9 && c > 1 {
			return 0, 0, false // does not fit 64 bits (or goes on)
		}
		v |= uint64(c&0x7f) << (7 * uint(i))
		if c < 0x80 {
			if i > 0 && c == 0 {
				return 0, 0, false // not shortest
			}
			return v, i + 1, true
		}
	}
	return 0, 0, false
}

const (
	refOK = iota
	refNeeds
	refCorrupt
)

// refTail is SPEC 5.1 for a Tail: whole fields, strictly increasing tags, at most 256, a field for every bit.
func refTail(b []byte, needs uint64) int {
	var last uint64
	var have [64]bool
	count := 0
	for len(b) > 0 {
		tag, n, ok := refUv(b)
		if !ok {
			return refCorrupt
		}
		b = b[n:]
		l, n, ok := refUv(b)
		if !ok {
			return refCorrupt
		}
		b = b[n:]
		if l > uint64(len(b)) {
			return refCorrupt
		}
		b = b[l:]
		if count > 0 && tag <= last {
			return refCorrupt
		}
		count++
		if count > 256 {
			return refCorrupt
		}
		last = tag
		if tag < 64 {
			have[tag] = true
		}
	}
	for i := 0; i < 64; i++ {
		if needs>>uint(i)&1 == 1 && !have[i] {
			return refCorrupt
		}
	}
	return refOK
}

// refHead decodes a Head plaintext as a build that knows the bits of known.
func refHead(b []byte, known uint64) int {
	var n int
	var ok bool
	step := func() uint64 {
		var v uint64
		v, n, ok = refUv(b)
		if ok {
			b = b[n:]
		}
		return v
	}
	take := func(k int) bool {
		if len(b) < k {
			return false
		}
		b = b[k:]
		return true
	}
	step()
	if !ok || !take(32) {
		return refCorrupt
	}
	step()
	if !ok || !take(16) || !take(32) {
		return refCorrupt
	}
	step() // journal
	if !ok {
		return refCorrupt
	}
	needs := step()
	if !ok {
		return refCorrupt
	}
	if needs&^known != 0 {
		return refNeeds
	}
	return refTail(b, needs)
}

func classify(err error) int {
	switch {
	case err == nil:
		return refOK
	case errors.Is(err, ErrNeeds):
		return refNeeds
	case errors.Is(err, ErrCorrupt):
		return refCorrupt
	}
	return -1
}

func uv(v uint64) []byte { return binary.AppendUvarint(nil, v) }

// pad rewrites the varint of v as a longer spelling: n extra continuation groups.
func padUv(v uint64, extra int) []byte {
	b := uv(v)
	for i := 0; i < extra; i++ {
		b[len(b)-1] |= 0x80
		b = append(b, 0)
	}
	return b
}

var interestingTags = []uint64{0, 1, 2, 3, 4, 5, 62, 63, 64, 65, 127, 128, 255, 256, 1 << 14, 1 << 20, 1 << 32, 1 << 62, 1 << 63, 1<<64 - 1}

func randTail(r *rand.Rand) []byte {
	var tags []uint64
	for range r.IntN(7) {
		if r.IntN(3) == 0 {
			tags = append(tags, uint64(r.IntN(70)))
		} else {
			tags = append(tags, interestingTags[r.IntN(len(interestingTags))])
		}
	}
	// strictly increasing, as a writer writes it
	for i := range tags {
		for j := i + 1; j < len(tags); j++ {
			if tags[j] < tags[i] {
				tags[i], tags[j] = tags[j], tags[i]
			}
		}
	}
	var out []byte
	var last uint64
	for i, tg := range tags {
		if i > 0 && tg <= last {
			continue
		}
		last = tg
		l := r.IntN(12)
		if r.IntN(20) == 0 {
			l = r.IntN(300)
		}
		v := make([]byte, l)
		rfillF(r, v)
		out = append(out, uv(tg)...)
		out = append(out, uv(uint64(l))...)
		out = append(out, v...)
	}
	return out
}

func rfillF(r *rand.Rand, b []byte) {
	for i := range b {
		b[i] = byte(r.Uint32())
	}
}

func randUv(r *rand.Rand) uint64 { return r.Uint64() >> uint(r.IntN(64)) }

// randHead makes a valid Head plaintext, optionally with Needs set to bits whose tags are present.
func randHeadBytes(r *rand.Rand, needs uint64, tail []byte) []byte {
	var b []byte
	b = append(b, uv(randUv(r))...)
	root := make([]byte, 32)
	rfillF(r, root)
	b = append(b, root...)
	b = append(b, uv(randUv(r))...)
	id := make([]byte, 48)
	rfillF(r, id)
	b = append(b, id...)
	b = append(b, uv(randUv(r))...)
	b = append(b, uv(needs)...)
	return append(b, tail...)
}

// mutate applies one structural or byte-level change.
func mutate(r *rand.Rand, b []byte) []byte {
	b = bytes.Clone(b)
	switch r.IntN(14) {
	case 0:
		if len(b) > 0 {
			b[r.IntN(len(b))] ^= 1 << uint(r.IntN(8))
		}
	case 1:
		b = b[:r.IntN(len(b)+1)]
	case 2:
		x := make([]byte, 1+r.IntN(5))
		rfillF(r, x)
		b = append(b, x...)
	case 3:
		i := r.IntN(len(b) + 1)
		b = append(b[:i:i], append([]byte{byte(r.Uint32())}, b[i:]...)...)
	case 4:
		if len(b) > 0 {
			i := r.IntN(len(b))
			b = append(b[:i:i], b[i+1:]...)
		}
	case 5:
		// a non-minimal spelling of whatever byte is a varint end: add 0x80 before a following 0x00
		if len(b) > 0 {
			i := r.IntN(len(b))
			b[i] |= 0x80
			b = append(b[:i+1:i+1], append([]byte{0}, b[i+1:]...)...)
		}
	case 6:
		// a long run of continuation bytes anywhere
		i := r.IntN(len(b) + 1)
		run := bytes.Repeat([]byte{0xff}, 1+r.IntN(11))
		b = append(b[:i:i], append(run, b[i:]...)...)
	case 7:
		// 10th byte overflow: ff*9 + 0x02
		i := r.IntN(len(b) + 1)
		run := append(bytes.Repeat([]byte{0xff}, 9), byte(1+r.IntN(3)))
		b = append(b[:i:i], append(run, b[i:]...)...)
	case 8:
		// set the tail length of the last field huge
		b = append(b, uv(randUv(r))...)
		b = append(b, uv(1<<63+uint64(r.IntN(5)))...)
	case 9:
		// trailing duplicate of the last 2..6 bytes
		if len(b) > 6 {
			k := 2 + r.IntN(5)
			b = append(b, b[len(b)-k:]...)
		}
	case 10:
		// swap two adjacent bytes
		if len(b) > 1 {
			i := r.IntN(len(b) - 1)
			b[i], b[i+1] = b[i+1], b[i]
		}
	case 11:
		// byte to 0, 0x80, 0xff, 0x7f
		if len(b) > 0 {
			b[r.IntN(len(b))] = []byte{0, 0x80, 0xff, 0x7f, 1}[r.IntN(5)]
		}
	case 12:
		// append an arbitrary whole field
		b = append(b, uv(interestingTags[r.IntN(len(interestingTags))])...)
		b = append(b, 0)
	case 13:
		// append the same field twice (equal tags)
		t := interestingTags[r.IntN(len(interestingTags))]
		f := append(uv(t), 0)
		b = append(b, f...)
		b = append(b, f...)
	}
	return b
}

// at least 3,000,000 inputs from the corpus of valid Heads. unmarshalHead must agree with
// the reference decoder on accept / ErrNeeds / ErrCorrupt, and everything it accepts re-encodes to the
// very same bytes (so no two encodings of one Head exist), and the checkTail with a Needs matrix agrees too.
func TestHeadDifferentialFuzz(t *testing.T) {
	n := 3_000_000
	if testing.Short() {
		n = 100_000
	}
	r := rand.New(rand.NewPCG(0x8f, 0x1234))
	var acc, needs, corrupt, ntail int
	for i := 0; i < n; i++ {
		tail := randTail(r)
		b := randHeadBytes(r, 0, tail)
		for k := r.IntN(4); k > 0; k-- {
			b = mutate(r, b)
		}
		want := refHead(b, 0)
		h, err := unmarshalHead(b)
		got := classify(err)
		if got != want {
			t.Fatalf("input %x: build says %d (%v), reference says %d", b, got, err, want)
		}
		switch want {
		case refOK:
			acc++
			if m := h.marshal(); !bytes.Equal(m, b) {
				t.Fatalf("accepted %x but re-encodes as %x", b, m)
			}
		case refNeeds:
			needs++
		default:
			corrupt++
		}
		// the Needs matrix on the same tail, with a build that knows every bit: checkTail is that build's check.
		if i%3 == 0 {
			var nd uint64
			switch r.IntN(4) {
			case 0:
				nd = 1 << uint(r.IntN(64))
			case 1:
				nd = r.Uint64()
			case 2:
				nd = 1<<uint(r.IntN(64)) | 1<<uint(r.IntN(64))
			}
			tb := b
			if len(tb) > 0 && r.IntN(2) == 0 {
				tb = tb[r.IntN(len(tb)):]
			}
			ntail++
			if w, g := refTail(tb, nd), classify(checkTail(tb, nd)); w != g {
				t.Fatalf("checkTail(%x, %#x): build %d reference %d", tb, nd, g, w)
			}
		}
	}
	t.Logf("%d inputs: %d accepted, %d needs, %d corrupt, %d tail/needs checks", n, acc, needs, corrupt, ntail)
	if acc < n/50 {
		t.Fatalf("the corpus is mostly refused (%d of %d accepted): the fuzz is not reaching the accept path", acc, n)
	}
}

// the structured product of the dimensions of the task: known/unknown tag present or absent x Needs bit
// 0..63 set or clear x tags ordered/equal/decreasing at 63/64/2^64-1 x length 0/huge/truncated x non-minimal.
func TestHeadProduct(t *testing.T) {
	tags := []uint64{0, 1, 2, 3, 7, 62, 63, 64, 65, 1 << 63, 1<<64 - 1}
	type lenCase struct {
		name string
		enc  func(l int) []byte // the length encoding for a value of l bytes
		fix  func(v []byte) []byte
	}
	lens := []lenCase{
		{"exact", func(l int) []byte { return uv(uint64(l)) }, func(v []byte) []byte { return v }},
		{"padded", func(l int) []byte { return padUv(uint64(l), 1) }, func(v []byte) []byte { return v }},
		{"padded3", func(l int) []byte { return padUv(uint64(l), 3) }, func(v []byte) []byte { return v }},
		{"huge", func(l int) []byte { return uv(1 << 63) }, func(v []byte) []byte { return v }},
		{"max", func(l int) []byte { return uv(1<<64 - 1) }, func(v []byte) []byte { return v }},
		{"over64", func(l int) []byte { return append(bytes.Repeat([]byte{0xff}, 9), 0x02) }, func(v []byte) []byte { return v }},
		{"one too long", func(l int) []byte { return uv(uint64(l) + 1) }, func(v []byte) []byte { return v }},
		{"truncated", func(l int) []byte { return uv(uint64(l)) }, func(v []byte) []byte {
			if len(v) > 0 {
				return v[:len(v)-1]
			}
			return []byte{}
		}},
	}
	tagEnc := []func(v uint64) []byte{
		func(v uint64) []byte { return uv(v) },
		func(v uint64) []byte { return padUv(v, 1) },
		func(v uint64) []byte { return padUv(v, 2) },
	}
	field := func(te func(uint64) []byte, tag uint64, lc lenCase, vlen int) []byte {
		v := bytes.Repeat([]byte{0xAB}, vlen)
		out := te(tag)
		out = append(out, lc.enc(vlen)...)
		return append(out, lc.fix(v)...)
	}
	count := 0
	check := func(tail []byte, needs uint64, what string) {
		count++
		if w, g := refTail(tail, needs), classify(checkTail(tail, needs)); w != g {
			t.Fatalf("%s: tail %x needs %#x: build %d reference %d", what, tail, needs, g, w)
		}
		// whole Head, with needs unknown to this build
		plain := Head{Seq: 5, Next: 6, Needs: needs}.marshal()
		plain = append(plain[:len(plain):len(plain)], tail...)
		w, g := refHead(plain, 0), classify(func() error { _, e := unmarshalHead(plain); return e }())
		if w != g {
			t.Fatalf("%s: head %x: build %d reference %d", what, plain, g, w)
		}
	}
	for _, a := range tags {
		for _, b := range tags {
			for _, te := range tagEnc {
				for _, lc := range lens {
					for _, vlen := range []int{0, 1, 5} {
						for _, nbit := range []int{-1, 0, 1, 2, 3, 62, 63} {
							var needs uint64
							if nbit >= 0 {
								needs = 1 << uint(nbit)
							}
							one := field(te, a, lc, vlen)
							two := append(append([]byte{}, one...), field(te, b, lc, vlen)...)
							check(one, needs, fmt.Sprintf("one tag %d len %s", a, lc.name))
							check(two, needs, fmt.Sprintf("two tags %d,%d len %s", a, b, lc.name))
							check(append(one[:len(one):len(one)], 0x00), needs, "trailing zero")
						}
					}
				}
			}
		}
	}
	// every bit 0..63, with its tag present, absent, present under a different spelling, and present after a skipped one
	for bit := 0; bit < 64; bit++ {
		needs := uint64(1) << uint(bit)
		check(nil, needs, "bit without tag")
		check(append(uv(uint64(bit)), 0), needs, "bit with tag")
		check(append(append(uv(uint64(bit)), 0), append(uv(64), 0)...), needs, "bit with tag and an unknown after")
		check(append(append(uv(uint64(bit)+1), 0), append(uv(uint64(bit)), 0)...), needs, "bit with tag but decreasing")
		check(append(padUv(uint64(bit), 1), 0), needs, "bit with a padded tag")
		if bit < 63 {
			check(append(uv(uint64(bit)+1), 0), needs, "next tag only")
		}
	}
	t.Logf("%d structured cases", count)
}

// every Head that is accepted re-encodes byte for byte, including after it was the Head of a store:
// the sealed record's plaintext carries no second spelling. Two spellings of one Head would be two plaintexts.
func TestOneSpellingPerHead(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	seen := map[string][]byte{}
	for range 400_000 {
		tail := randTail(r)
		b := randHeadBytes(r, 0, tail)
		b = mutate(r, b)
		h, err := unmarshalHead(b)
		if err != nil {
			continue
		}
		// structural identity of the Head: every field, as values
		key := fmt.Sprintf("%d|%x|%d|%x|%x|%d|%d|%x", h.Seq, h.Root, h.Next, h.ID, h.Seed, h.Journal, h.Needs, h.Tail)
		if prev, ok := seen[key]; ok && !bytes.Equal(prev, b) {
			t.Fatalf("one Head, two encodings: %x and %x", prev, b)
		}
		seen[key] = b
	}
}

// a writer may carry a Tail it does not understand through every door and the bytes do not change; this one
// goes through Attach, a commit, a takeover and Reseal with and without the place, on a Tail with tags 1..63 that
// are NOT backed by a Needs bit, and tags above 64.
func TestTailThroughEveryDoor(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Backend: be, Cipher: c, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}
	read := func() []byte {
		raw := be.Dump()["head"]
		plain, err := unseal(c, headAAD(be.String()), raw[30:])
		if err != nil {
			t.Fatal(err)
		}
		return plain
	}
	h, _ := unmarshalHead(read())
	tail := tlv(1, "a") + tlv(2, "") + tlv(3, string(bytes.Repeat([]byte{7}, 200))) + tlv(63, "z") + tlv(64, "y") + tlv(1<<63, "x") + tlv(1<<64-1, "w")
	h.Seq, h.Journal, h.Tail = 3, 99, tail
	sealed, err := seal(c, headAAD(be.String()), h.marshal())
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.New(be, maxHead).Put(ctx, headKey, sealed, 0); err != nil {
		t.Fatal(err)
	}
	cfg.Round = 2
	v, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		if _, err := v.Create(ctx, RootInode, fmt.Sprintf("f%d", i), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		got, err := unmarshalHead(read())
		if err != nil || got.Tail != tail || got.Journal != 99+uint64(i)+1 || got.Needs != 0 || got.ID != h.ID || got.Seed != h.Seed {
			t.Fatalf("commit %d: %v tail kept=%v", i, err, got.Tail == tail)
		}
	}
	cfg.Round = 5
	v2, err := Attach(ctx, cfg) // takeover
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := unmarshalHead(read()); got.Tail != tail {
		t.Fatal("takeover changed the tail")
	}
	// the fenced writer's next commit must not rewrite the tail either way
	if _, err := v.Create(ctx, RootInode, "late", 0o644); err == nil {
		_ = v.Flush(ctx)
	}
	before := read()
	cfg.Round = 6
	if err := Reseal(ctx, cfg, ""); err != nil {
		t.Fatal(err)
	}
	cfg.Round = 7
	if err := Reseal(ctx, cfg, be.String()); err != nil {
		t.Fatal(err)
	}
	// plaintexts compare equal except Seq may differ because of the fenced writer; compare tail+ids
	a, _ := unmarshalHead(before)
	b, _ := unmarshalHead(read())
	if a != b {
		t.Fatalf("reseal changed the Head: %+v vs %+v", a, b)
	}
	if b.Tail != tail {
		t.Fatal("reseal lost the tail")
	}
	_ = v2
}

// native coverage-guided fuzzing from a corpus of valid Heads. Accepts only what the reference accepts,
// and re-encodes accepted input byte for byte.
func FuzzHead(f *testing.F) {
	r := rand.New(rand.NewPCG(3, 4))
	for range 200 {
		f.Add(randHeadBytes(r, 0, randTail(r)))
	}
	f.Add(Head{Seq: 1, Next: 2}.marshal())
	f.Fuzz(func(t *testing.T, b []byte) {
		want := refHead(b, 0)
		h, err := unmarshalHead(b)
		if got := classify(err); got != want {
			t.Fatalf("input %x: build %d (%v) reference %d", b, got, err, want)
		}
		if want == refOK && !bytes.Equal(h.marshal(), b) {
			t.Fatalf("accepted %x re-encodes as %x", b, h.marshal())
		}
		// the same bytes as a Tail with every needs mask the build could be given
		for _, nd := range []uint64{0, 1, 2, 1 << 3, 1 << 63, ^uint64(0)} {
			if w, g := refTail(b, nd), classify(checkTail(b, nd)); w != g {
				t.Fatalf("tail %x needs %#x: build %d reference %d", b, nd, g, w)
			}
		}
	})
}

// Needs and the Tail live inside the sealed Head: flipping any bit of any byte of the sealed payload
// (a store that cannot open it) makes Open refuse, so a store cannot clear a Needs bit or drop a field. The 30-byte fence
// header (magic, Round, token) is outside the seal by design and is not asserted.
func TestNeedsAndTailAreSealed(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	plain, err := unseal(c, headAAD(be.String()), be.Dump()["head"][30:])
	if err != nil {
		t.Fatal(err)
	}
	h, _ := unmarshalHead(plain)
	h.Tail = tlv(70, "hint") + tlv(71, "another")
	sealed, _ := seal(c, headAAD(be.String()), h.marshal())
	if err := fence.New(be, maxHead).Put(ctx, headKey, sealed, 0); err != nil {
		t.Fatal(err)
	}
	rec := bytes.Clone(be.Dump()["head"])
	bad := 0
	for i := 30; i < len(rec); i++ {
		for _, bit := range []byte{1, 0x80} {
			m := bytes.Clone(rec)
			m[i] ^= bit
			// write the damaged record in place of the Head
			if _, ver, err := be.Get(ctx, headKey, 1<<20); err == nil {
				if _, err := be.PutIfVersion(ctx, headKey, m, ver); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Open(ctx, Config{Backend: be, Cipher: c}); err == nil {
				bad++
				t.Errorf("a flipped bit %#x at byte %d of the sealed Head was accepted", bit, i)
				if bad > 5 {
					return
				}
			}
		}
	}
	t.Logf("%d sealed bytes x 2 flips: every one refused", len(rec)-30)
}

// a commit carries Needs and the Tail of the Head it read, byte for byte, and writes the journal's
// sequence. This build cannot open a Volume whose Needs is not 0, so the Head is planted in the
// Volume's memory and the plaintext it commits is decoded by hand: the first build that knows a bit
// relies on exactly this.
func TestCommitCarriesNeedsAndTail(t *testing.T) {
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
	tail := tlv(3, "layer") + tlv(70, "hint")
	v.mu.Lock()
	v.head.Needs, v.head.Tail = 1<<3, tail
	v.mu.Unlock()
	if _, err := v.Create(ctx, RootInode, "f", 0o644); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	plain, err := unseal(c, headAAD(be.String()), be.Dump()["head"][30:])
	if err != nil {
		t.Fatal(err)
	}
	b := plain
	next := func() uint64 {
		x, n, ok := refUv(b)
		if !ok {
			t.Fatal("bad uvarint")
		}
		b = b[n:]
		return x
	}
	next()
	b = b[32:]
	next()
	b = b[48:]
	journal := next()
	needs := next()
	if journal != 1 || needs != 1<<3 || string(b) != tail {
		t.Fatalf("a commit wrote journal=%d needs=%#x tail=%q; it read 1, 8 and %q", journal, needs, b, tail)
	}
}

// order is against the PREVIOUS field, not the first or the largest so far.
func TestTailOrderIsAgainstThePreviousField(t *testing.T) {
	for name, tail := range map[string]string{
		"1,5,3":    tlv(1, "") + tlv(5, "") + tlv(3, ""),
		"1,5,5":    tlv(1, "") + tlv(5, "") + tlv(5, ""),
		"2,3,4,3":  tlv(2, "") + tlv(3, "") + tlv(4, "") + tlv(3, ""),
		"70,80,71": tlv(70, "") + tlv(80, "") + tlv(71, ""),
		"0,1,2,1":  tlv(0, "") + tlv(1, "") + tlv(2, "") + tlv(1, ""),
		"1,2^63,2": tlv(1, "") + tlv(1<<63, "") + tlv(2, ""),
	} {
		if err := checkTail([]byte(tail), 0); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v, want ErrCorrupt", name, err)
		}
	}
}

// knownTables are Needs bits that a later build could know, as a table: parseHead takes the known
// bits where the build holds a constant, so the day a bit is defined its checks (a field for the bit,
// the order of the Tail, the refusal of a bit beyond it) are run by tests that ran before it came.
var knownTables = []uint64{0, 1 << 3, 1<<1 | 1<<2 | 1<<3, 1 << 63, ^uint64(0)}

// SPEC 5.1 for a build that knows bits: the Head it reads agrees with the reference decoder on accept,
// ErrNeeds and ErrCorrupt, for every table of known bits, a Needs that is within the table or reaches
// past it, and a Tail that holds the fields of the bits, lacks one, or is mutated; what it accepts
// re-encodes to the same bytes.
func TestHeadDifferentialWithKnownBits(t *testing.T) {
	n := 600_000
	if testing.Short() {
		n = 100_000
	}
	r := rand.New(rand.NewPCG(0x8f, 0x5678))
	var counts [3]int
	for i := 0; i < n; i++ {
		known := knownTables[r.IntN(len(knownTables))]
		var needs uint64
		switch r.IntN(4) {
		case 0:
			needs = known & r.Uint64()
		case 1:
			needs = known & (1 << uint(r.IntN(64)))
		case 2:
			needs = 1 << uint(r.IntN(64)) // may be beyond known
		}
		tail := randTail(r)
		if r.IntN(2) == 0 { // a Tail that holds a field for every bit of needs, in order
			tail = tailWithFields(r, needs)
		}
		b := randHeadBytes(r, needs, tail)
		for k := r.IntN(3); k > 0 && r.IntN(2) == 0; k-- {
			b = mutate(r, b)
		}
		want := refHead(b, known)
		h, err := parseHead(b, known)
		if got := classify(err); got != want {
			t.Fatalf("known %#x input %x: build says %d (%v), reference says %d", known, b, got, err, want)
		}
		counts[want]++
		if want == refOK && !bytes.Equal(h.marshal(), b) {
			t.Fatalf("known %#x accepted %x but re-encodes as %x", known, b, h.marshal())
		}
	}
	t.Logf("%d inputs: %d accepted, %d needs, %d corrupt", n, counts[refOK], counts[refNeeds], counts[refCorrupt])
	if counts[refOK] < n/20 || counts[refNeeds] < n/20 || counts[refCorrupt] < n/20 {
		t.Fatalf("the corpus does not reach every class: %v", counts)
	}
}

// tailWithFields is a Tail in order that holds a field for each bit of needs and some of no bit.
func tailWithFields(r *rand.Rand, needs uint64) []byte {
	var tags []uint64
	for b := uint64(0); b < 64; b++ {
		if needs>>b&1 == 1 || r.IntN(40) == 0 {
			tags = append(tags, b)
		}
	}
	if r.IntN(3) == 0 {
		tags = append(tags, 64+uint64(r.IntN(1000)))
	}
	var out []byte
	for _, tg := range tags {
		v := make([]byte, r.IntN(8))
		rfillF(r, v)
		out = append(out, uv(tg)...)
		out = append(out, uv(uint64(len(v)))...)
		out = append(out, v...)
	}
	return out
}

// A build that knows bit 3 holds a Head that sets it to the field of tag 3, and to the form of the Tail,
// and refuses a bit it does not know before it looks at the Tail.
func TestAKnownBitIsHeldToItsField(t *testing.T) {
	head := func(needs uint64, tail string) []byte {
		return Head{Seq: 1, Next: 2, Needs: needs, Tail: tail}.marshal()
	}
	for _, c := range []struct {
		name  string
		known uint64
		b     []byte
		want  int
	}{
		{"known bit with its field", 1 << 3, head(1<<3, tlv(3, "layer")), refOK},
		{"known bit with its field among others", 1 << 3, head(1<<3, tlv(1, "")+tlv(3, "layer")+tlv(70, "x")), refOK},
		{"known bit without a Tail", 1 << 3, head(1<<3, ""), refCorrupt},
		{"known bit with another field", 1 << 3, head(1<<3, tlv(2, "x")+tlv(4, "y")), refCorrupt},
		{"known bit with a field past 63 that has its low bits", 1 << 3, head(1<<3, tlv(3+64, "x")), refCorrupt},
		{"known bit with a field out of order", 1 << 3, head(1<<3, tlv(5, "")+tlv(3, "x")), refCorrupt},
		{"two known bits, one field", 1<<2 | 1<<3, head(1<<2|1<<3, tlv(3, "x")), refCorrupt},
		{"two known bits, both fields", 1<<2 | 1<<3, head(1<<2|1<<3, tlv(2, "a")+tlv(3, "x")), refOK},
		{"a bit that is not known, with a good Tail", 1 << 3, head(1<<4, tlv(4, "x")), refNeeds},
		{"a bit that is not known, with a bad Tail", 1 << 3, head(1<<4, "\x07"), refNeeds},
		{"a known and an unknown bit", 1 << 3, head(1<<3|1<<4, tlv(3, "x")+tlv(4, "y")), refNeeds},
		{"no bit, a field of a tag that is known", 1 << 3, head(0, tlv(3, "x")), refOK},
		{"bit 63 known, its field", 1 << 63, head(1<<63, tlv(63, "x")), refOK},
		{"bit 63 known, field 64", 1 << 63, head(1<<63, tlv(64, "x")), refCorrupt},
	} {
		_, err := parseHead(c.b, c.known)
		if got := classify(err); got != c.want {
			t.Errorf("%s: %v (class %d), want class %d", c.name, err, got, c.want)
		}
		if want := refHead(c.b, c.known); want != c.want {
			t.Errorf("%s: the reference says %d, the table says %d", c.name, want, c.want)
		}
	}
	// and this build, which knows none, is the table with known = 0
	if _, err := unmarshalHead(head(1<<3, tlv(3, "x"))); !errors.Is(err, ErrNeeds) {
		t.Errorf("this build: %v, want ErrNeeds", err)
	}
}
