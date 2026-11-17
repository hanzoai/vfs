package vfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/fence"
)

// TestHeadContextIsExact: a Head sealed for one place opens at that place only. The
// context is a fixed-length digest compared for equality, so no place is a prefix of
// another: a Head sealed for .../vol10 must not open at .../vol1, nor at the bucket.
func TestHeadContextIsExact(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	payload := Head{Seq: 5, Next: 9}.marshal()
	places := []string{
		"s3://bucket/vol10", "s3://bucket/vol1", "s3://bucket/vol100", "s3://bucket/vol1/", "s3://bucket/vol1/x",
		"s3://bucket/", "s3://bucket", "s3://bucke", "file:///a/b", "file:///a", "file:///a/b/", "", "x",
	}
	for _, from := range places {
		sealed, err := seal(c, headAAD(from), payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, at := range places {
			got, err := unseal(c, headAAD(at), sealed)
			if at == from {
				if err != nil || string(got) != string(payload) {
					t.Fatalf("a Head sealed for %q does not open there: %v", from, err)
				}
				continue
			}
			if err == nil {
				t.Errorf("a Head sealed for %q OPENED at %q", from, at)
			}
		}
	}
}

// TestContextsDoNotCollide: the contexts of every kind of object, built from every
// pair of fields, are distinct and all the same length.
func TestContextsDoNotCollide(t *testing.T) {
	var v1, v2 volumeID
	v2[0] = 1
	var c1, c2 chunkID
	c2[31] = 1
	seen := map[string]string{}
	add := func(name string, b []byte) {
		if len(b) != bindLen {
			t.Fatalf("%s: context of %d bytes, want %d", name, len(b), bindLen)
		}
		if prev, ok := seen[string(b)]; ok {
			t.Fatalf("%s and %s share a context", name, prev)
		}
		seen[string(b)] = name
	}
	add("head a", headAAD("a"))
	add("head ab", headAAD("ab"))
	add("head empty", headAAD(""))
	add("shard 1", shardAAD(v1))
	add("shard 2", shardAAD(v2))
	add("chunk 1/1", chunkAAD(v1, c1))
	add("chunk 1/2", chunkAAD(v1, c2))
	add("chunk 2/1", chunkAAD(v2, c1))
	// A field boundary cannot move: ("ab","c") and ("a","bc") are different.
	add("fields 1", bind("k", []byte("ab"), []byte("c")))
	add("fields 2", bind("k", []byte("a"), []byte("bc")))
}

// TestSwappedHeadIsRefused: copy the Head of one Volume over the Head of another under
// one shared Cipher, for places that are prefixes of one another, spelled with and
// without a trailing slash, and at the root, and Open and Attach. The wrong Volume is
// never adopted.
func TestSwappedHeadIsRefused(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	names := []string{"vol1", "vol10", "vol100", "vol1/sub", "v", ""}
	trials := 4
	if testing.Short() {
		trials = 1
	}
	for range trials {
		dir := t.TempDir()
		open := func(name string) backend.Backend {
			p := filepath.Join(dir, name)
			be, err := backend.Open(ctx, "file://"+p)
			if err != nil {
				t.Fatal(err)
			}
			return be
		}
		for _, name := range names {
			if err := Create(ctx, Config{Backend: open(name), Cipher: c}); err != nil {
				// "vol1" holds "vol1/sub" as a subdirectory, which is a different prefix.
				t.Fatalf("%q: %v", name, err)
			}
		}
		for _, from := range names {
			head, err := os.ReadFile(filepath.Join(dir, from, "head"))
			if err != nil {
				t.Fatal(err)
			}
			for _, to := range names {
				if to == from {
					continue
				}
				orig, err := os.ReadFile(filepath.Join(dir, to, "head"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, to, "head"), head, 0o644); err != nil {
					t.Fatal(err)
				}
				if _, err := Open(ctx, Config{Backend: open(to), Cipher: c}); err == nil {
					t.Errorf("the Head of %q was adopted as the Head of %q", from, to)
				}
				if _, err := Attach(ctx, Config{Backend: open(to), Cipher: c, Round: 7, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}); err == nil {
					t.Errorf("a writer attached to %q on the Head of %q", to, from)
				}
				if err := os.WriteFile(filepath.Join(dir, to, "head"), orig, 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

// TestPlaceSpellings: every spelling of one directory is one place, so a Volume opens
// through a trailing slash, a dot segment, a double slash and a symlink.
func TestPlaceSpellings(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	dir := t.TempDir()
	root := filepath.Join(dir, "store")
	be, err := backend.Open(ctx, "file://"+root)
	if err != nil {
		t.Fatal(err)
	}
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	for name, url := range map[string]string{
		"trailing slash": "file://" + root + "/",
		"dot segment":    "file://" + dir + "/./store",
		"double slash":   "file://" + dir + "//store",
		"parent segment": "file://" + dir + "/store/../store",
		"symlink":        "file://" + link,
	} {
		b2, err := backend.Open(ctx, url)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if b2.String() != be.String() {
			t.Errorf("%s: the place is %q, want %q", name, b2.String(), be.String())
		}
		if _, err := Open(ctx, Config{Backend: b2, Cipher: c}); err != nil {
			t.Errorf("%s: the same Volume does not open: %v", name, err)
		}
	}
}

// TestResealMovesAVolume: a Volume copied to another place does not open there until
// its Head is sealed for it; after Reseal it opens, holds everything, and a writer can
// attach. Only the Head is bound to the place.
func TestResealMovesAVolume(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	dir := t.TempDir()
	from, err := backend.Open(ctx, "file://"+filepath.Join(dir, "from"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Create(ctx, Config{Backend: from, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := Attach(ctx, Config{Backend: from, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk})
	if err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 700_000)
	for i := range big {
		big[i] = byte(i * 31)
	}
	for _, f := range []struct {
		name string
		data []byte
	}{{"small", []byte("hello")}, {"big", big}} {
		in, err := v.Create(ctx, RootInode, f.name, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		h, _ := v.Open(ctx, in.ID)
		if _, err := h.WriteAt(ctx, f.data, 0); err != nil {
			t.Fatal(err)
		}
		h.Close()
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Move: rename the directory. Every object moves with it.
	to := filepath.Join(dir, "to")
	if err := os.Rename(filepath.Join(dir, "from"), to); err != nil {
		t.Fatal(err)
	}
	be2, err := backend.Open(ctx, "file://"+to)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, Config{Backend: be2, Cipher: c}); err == nil || !strings.Contains(err.Error(), "Reseal") {
		t.Fatalf("a moved Volume opened, or its error does not say what to do: %v", err)
	}
	if err := Reseal(ctx, Config{Backend: be2, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}, from.String()); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("Reseal at a Round that does not exceed the recorded one = %v, want ErrStaleRound", err)
	}
	if err := Reseal(ctx, Config{Backend: be2, Cipher: c, Round: 2, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}, "file:///nowhere"); err == nil {
		t.Fatal("Reseal from a place the Head is not sealed for succeeded")
	}
	if err := Reseal(ctx, Config{Backend: be2, Cipher: c, Round: 2, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}, from.String()); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(ctx, Config{Backend: be2, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ro.Resolve(ctx, "/big")
	if err != nil || got.Size != uint64(len(big)) {
		t.Fatalf("after Reseal: %+v %v", got, err)
	}
	h, _ := ro.Open(ctx, got.ID)
	buf := make([]byte, len(big))
	if _, err := h.ReadAt(ctx, buf, 0); err != nil && err.Error() != "EOF" {
		t.Fatal(err)
	}
	if string(buf) != string(big) {
		t.Fatal("a file differs after the move")
	}
	w, err := Attach(ctx, Config{Backend: be2, Cipher: c, Round: 3, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk})
	if err != nil {
		t.Fatalf("a writer cannot attach after Reseal: %v", err)
	}
	if _, err := w.Create(ctx, RootInode, "after", 0o644); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// The writer at the old place was fenced only there: nothing here concerns it.
}

// TestRecipientAloneCannotForge: whoever holds the public recipient can write an age
// file the identity opens. The default Cipher also tags what it seals with a key
// derived from the identity's secret, so a Head sealed with the recipient alone is
// refused.
func TestRecipientAloneCannotForge(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	dir := t.TempDir()
	be, _ := backend.Open(ctx, "file://"+filepath.Join(dir, "v"))
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	r, err := age.ParseHybridRecipient(k.Recipient().String())
	if err != nil {
		t.Fatal(err)
	}
	forge := &ageCipher{current: r, curID: ageKeyID(k)}
	sealed, err := seal(forge, headAAD(be.String()), Head{Seq: 1 << 40, Next: 77}.marshal())
	if err != nil {
		t.Fatal(err)
	}
	st := fence.New(be, maxHead)
	round, _ := st.Round(ctx, headKey)
	if err := st.Put(ctx, headKey, sealed, round); err != nil {
		t.Fatal(err)
	}
	if v, err := Open(ctx, Config{Backend: be, Cipher: c}); err == nil {
		t.Fatalf("a Head forged with only the public recipient was accepted (Seq=%d)", v.head.Seq)
	}
	// The same holds for a shard: one object forged in is refused, not read.
	s, _, err := forge.Seal(make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(s, ageKeyID(k)); err == nil {
		t.Fatal("an object sealed with the recipient alone opened under the identity")
	}
}
