package vfs

import (
	"context"
	"errors"
	"testing"
)

// headFields reads the committed Head: its Needs and its Tail by tag.
func headFields(t *testing.T, be interface{ Dump() map[string][]byte }, c Cipher, place string) (uint64, map[uint64][]byte) {
	t.Helper()
	plain, err := unseal(c, headAAD(place), be.Dump()["head"][30:])
	if err != nil {
		t.Fatal(err)
	}
	h, err := parseHead(plain, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	fields := map[uint64][]byte{}
	r := reader{b: []byte(h.Tail)}
	for len(r.b) > 0 {
		tag := r.uvarint()
		fields[tag] = r.bytes()
	}
	return h.Needs, fields
}

// SPECIFICATION 5.1, the links bit: a Volume that has never had a second name for an inode
// writes a Head that a build ignorant of the bit reads; the commit that holds the first
// one sets the bit and writes the field, with the count of inodes that have more than one
// name; and the bit is never cleared.
func TestHeadAnnouncesLinksOnlyAfterTheFirstLink(t *testing.T) {
	ctx := context.Background()
	be, c, conf := headVolume(t)
	conf.Round = 1
	v, err := Attach(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := v.Create(ctx, RootInode, "f", 0o644)
	if _, err := v.Mkdir(ctx, RootInode, "d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := v.Rename(ctx, RootInode, "f", RootInode, "g", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Symlink(ctx, RootInode, "s", "g"); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	needs, fields := headFields(t, be, c, be.String())
	if needs != 0 || fields[tagLinks] != nil {
		t.Fatalf("a Volume that never linked announces links: needs %#x, field %x", needs, fields[tagLinks])
	}
	// a build that knows no bit reads it, and the usage field it does not understand rides along
	plain, _ := unseal(c, headAAD(be.String()), be.Dump()["head"][30:])
	if _, err := parseHead(plain, 0); err != nil {
		t.Fatalf("a build ignorant of links reads a Volume without any: %v", err)
	}

	if _, err := v.Link(ctx, f.ID, RootInode, "h"); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	needs, fields = headFields(t, be, c, be.String())
	if needs != needsLinks || string(fields[tagLinks]) != "\x01" {
		t.Fatalf("after the first link: needs %#x, links field %x, want bit 2 and 1", needs, fields[tagLinks])
	}
	plain, _ = unseal(c, headAAD(be.String()), be.Dump()["head"][30:])
	if _, err := parseHead(plain, 0); !errors.Is(err, ErrNeeds) {
		t.Fatalf("a build ignorant of links reads a Volume with one: %v", err)
	}
	if _, err := parseHead(plain, needsLinks); err != nil {
		t.Fatalf("a build that knows the bit: %v", err)
	}

	// the count follows the inodes with more than one name, and the bit stays
	if err := v.Remove(ctx, RootInode, "h"); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	needs, fields = headFields(t, be, c, be.String())
	if needs != needsLinks || string(fields[tagLinks]) != "\x00" {
		t.Fatalf("after the link is gone: needs %#x, links field %x", needs, fields[tagLinks])
	}
	if err := v.Check(ctx); err != nil {
		t.Fatal(err)
	}
	cold, err := Open(ctx, Config{Backend: be, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	if !cold.ns.links || cold.ns.linked != 0 {
		t.Fatalf("a reopened Volume: %+v", cold.ns)
	}
	// a new link is counted again, and a reopen reads the count back
	if _, err := v.Link(ctx, f.ID, RootInode, "again"); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, fields = headFields(t, be, c, be.String()); string(fields[tagLinks]) != "\x01" {
		t.Fatalf("after a link again: links field %x", fields[tagLinks])
	}
	if cold, err = Open(ctx, Config{Backend: be, Cipher: c}); err != nil || cold.ns.linked != 1 {
		t.Fatalf("a reopened Volume: %+v %v", cold.ns, err)
	}
}

// A Head that sets the bit without the field is not a Head of this feature, and neither is
// a field that is not a count.
func TestLinksFieldIsHeldToItsBit(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		needs uint64
		tail  string
		want  error
	}{
		"bit without field":      {needsLinks, "", ErrCorrupt},
		"field of another tag":   {needsLinks, tlv(3, "\x00"), ErrCorrupt},
		"field without bit":      {0, tlv(tagLinks, "\x05"), nil}, // ignorable: carried, never read
		"count":                  {needsLinks, tlv(tagLinks, "\x05"), nil},
		"count in two bytes":     {needsLinks, tlv(tagLinks, "\x85\x00"), ErrCorrupt},
		"count that is not one":  {needsLinks, tlv(tagLinks, "\x05\x05"), ErrCorrupt},
		"count that is not even": {needsLinks, tlv(tagLinks, ""), ErrCorrupt},
	} {
		for door, open := range map[string]func(Config) error{
			"open":   func(c Config) error { _, err := Open(ctx, c); return err },
			"attach": func(c Config) error { c.Round = 9; _, err := Attach(ctx, c); return err },
		} {
			be, c, conf := headVolume(t)
			h, _ := unmarshalHead(headPlain(t, be, c))
			h.Needs, h.Tail = tc.needs, tc.tail
			headPutRaw(t, be, c, h.marshal())
			if err := open(conf); tc.want == nil && err != nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("%s through %s: %v, want %v", name, door, err, tc.want)
			}
		}
	}
}

func TestUsageFieldIsCheckedWhenRead(t *testing.T) {
	ctx := context.Background()
	be, c, conf := headVolume(t)
	h, _ := unmarshalHead(headPlain(t, be, c))
	for name, val := range map[string]string{"empty": "", "one number": "\x03", "three": "\x01\x02\x03", "wide": "\x80\x00\x01"} {
		h.Tail = tlv(tagUsage, val)
		headPutRaw(t, be, c, h.marshal())
		if _, err := Open(ctx, conf); !errors.Is(err, ErrCorrupt) {
			t.Errorf("usage %s: %v, want ErrCorrupt", name, err)
		}
	}
	h.Tail = tlv(tagUsage, "\x07\xc8\x01")
	headPutRaw(t, be, c, h.marshal())
	v, err := Open(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	if u := v.Usage(); u.Files != 7 || u.Bytes != 200 {
		t.Fatalf("usage: %+v", u)
	}
}

func TestTailSetKeepsEveryOtherFieldInOrder(t *testing.T) {
	tail := tlv(3, "c") + tlv(70, "x") + tlv(100, "yy")
	for _, tc := range []struct {
		tag  uint64
		want string
	}{
		{1, tlv(1, "N") + tail},
		{3, tlv(3, "N") + tlv(70, "x") + tlv(100, "yy")},
		{64, tlv(3, "c") + tlv(64, "N") + tlv(70, "x") + tlv(100, "yy")},
		{70, tlv(3, "c") + tlv(70, "N") + tlv(100, "yy")},
		{101, tail + tlv(101, "N")},
	} {
		if got := tailSet(tail, tc.tag, []byte("N")); got != tc.want {
			t.Errorf("set %d: %q, want %q", tc.tag, got, tc.want)
		}
		if err := checkTail([]byte(tailSet(tail, tc.tag, []byte("N"))), 0); err != nil {
			t.Errorf("set %d: not a Tail: %v", tc.tag, err)
		}
	}
	if got := tailSet("", 5, nil); got != tlv(5, "") {
		t.Errorf("empty tail: %q", got)
	}
	if v, ok := tailGet(tail, 70); !ok || string(v) != "x" {
		t.Errorf("get: %q %v", v, ok)
	}
	if _, ok := tailGet(tail, 4); ok {
		t.Error("get of a tag that is not there")
	}
}
