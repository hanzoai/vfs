package vfs

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/internal/storetest"
)

func extVolume(t *testing.T) (*storetest.Mem, Cipher, *Volume) {
	t.Helper()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk})
	if err != nil {
		t.Fatal(err)
	}
	return be, c, v
}

// the text is implementable from itself: 60 (seed, name, key) triples whose names were computed by a
// different BLAKE3 library (lukechampine.com/blake3 v1.4.1: DeriveKey, keyed hasher) from SPEC 3.1 alone.
func TestExtNamesMatchAnIndependentBLAKE3(t *testing.T) {
	raw, err := os.ReadFile("testdata/extnames.json")
	if err != nil {
		t.Fatal(err)
	}
	var vecs []struct{ Seed, Name, Key, Rest string }
	if err := json.Unmarshal(raw, &vecs); err != nil {
		t.Fatal(err)
	}
	_, _, v := extVolume(t)
	for i, x := range vecs {
		seed, _ := hex.DecodeString(x.Seed)
		key, _ := hex.DecodeString(x.Key)
		v.mu.Lock()
		copy(v.head.Seed[:], seed)
		v.mu.Unlock()
		got, err := v.ExtKey(x.Name, key)
		if err != nil {
			t.Fatal(err)
		}
		if want := "ext/" + x.Name + "/" + x.Rest; got != want {
			t.Fatalf("vector %d: %s, an independent BLAKE3 says %s", i, got, want)
		}
	}
	t.Logf("%d vectors agree", len(vecs))
}

// domain separation. The Seed enters three derivations; each has its own context and mode, so none of the
// three keys is another's and the extension-name hash of a string is not the boundary hash of it.
func TestSeedDerivationsAreSeparate(t *testing.T) {
	var seed [32]byte
	for i := range seed {
		seed[i] = byte(i * 7)
	}
	var ext, tree [32]byte
	blake3.DeriveKey("hanzoai/vfs extension names", seed[:], ext[:])
	blake3.DeriveKey("hanzoai/vfs tree boundaries", seed[:], tree[:])
	g := newGear(seed)
	if ext == tree {
		t.Fatal("one key")
	}
	// the gear table is H(ctx||seed) read as XOF: its first 32 bytes must not equal either key.
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs chunker gear table")
	_, _ = h.Write(seed[:])
	var first [32]byte
	_, _ = h.Digest().Read(first[:])
	if first == ext || first == tree {
		t.Fatal("gear output equals a derived key")
	}
	_ = g
	// the name hash of any (name, key) is not the same as the keyed hash under the tree key
	for _, key := range [][]byte{nil, []byte("a"), []byte("workflow-1")} {
		a := extRest(seed, "trace", key)
		th, _ := blake3.NewKeyed(tree[:])
		_, _ = th.Write(append([]byte{5}, append([]byte("trace"), key...)...))
		if a == hex.EncodeToString(th.Sum(nil)) {
			t.Fatal("ext name hash equals the tree boundary hash of the same bytes")
		}
	}
}

// names are stable over a commit, a takeover, a Reseal with and without the place and a read-only Open; a second
// node finds a trace by opening the Volume; the Seed is in no object the store holds.
func TestExtNamesFollowTheVolume(t *testing.T) {
	ctx := context.Background()
	be, c, v := extVolume(t)
	if err := v.PutExt(ctx, "trace", []byte("wf"), []byte("trace-bytes")); err != nil {
		t.Fatal(err)
	}
	name, _ := v.ExtKey("trace", []byte("wf"))
	if _, ok := be.Dump()[name]; !ok {
		t.Fatalf("PutExt did not write at ExtKey %s", name)
	}
	if _, err := v.Create(ctx, RootInode, "f", 0o644); err != nil {
		t.Fatal(err)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n2, _ := v.ExtKey("trace", []byte("wf")); n2 != name {
		t.Fatal("a commit moved the name")
	}
	w, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 3, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk})
	if err != nil {
		t.Fatal(err)
	}
	if n2, _ := w.ExtKey("trace", []byte("wf")); n2 != name {
		t.Fatal("a takeover moved the name")
	}
	if err := Reseal(ctx, Config{Backend: be, Cipher: c, Round: 4, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}, ""); err != nil {
		t.Fatal(err)
	}
	if err := Reseal(ctx, Config{Backend: be, Cipher: c, Round: 5, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk}, be.String()); err != nil {
		t.Fatal(err)
	}
	r, err := Open(ctx, Config{Backend: be, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	if n2, _ := r.ExtKey("trace", []byte("wf")); n2 != name {
		t.Fatal("a reseal moved the name")
	}
	got, err := r.GetExt(ctx, "trace", []byte("wf"))
	if err != nil || string(got) != "trace-bytes" {
		t.Fatalf("second node: %q %v", got, err)
	}
	// a read-only Volume cannot write
	if err := r.PutExt(ctx, "trace", []byte("wf"), []byte("x")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only PutExt: %v", err)
	}
	// the Seed and its derived keys appear in no object of the store
	seed := r.head.Seed
	var ek, tk [32]byte
	blake3.DeriveKey("hanzoai/vfs extension names", seed[:], ek[:])
	blake3.DeriveKey("hanzoai/vfs tree boundaries", seed[:], tk[:])
	for k, b := range be.Dump() {
		for _, s := range [][]byte{seed[:], ek[:], tk[:], seed[:16], []byte(hex.EncodeToString(seed[:]))} {
			if bytes.Contains(b, s) || strings.Contains(k, string(s)) {
				t.Errorf("the Seed or a key of it is in the object %s", k)
			}
		}
	}
	// another Volume's name for the same key is different, and the wrong Cipher finds nothing
	_, _, v2 := extVolume(t)
	n3, _ := v2.ExtKey("trace", []byte("wf"))
	if n3 == name {
		t.Fatal("two Volumes, one name")
	}
	k2, _ := age.GenerateHybridIdentity()
	if _, err := Open(ctx, Config{Backend: be, Cipher: Hybrid(k2)}); err == nil {
		t.Fatal("opened with the wrong key")
	}
	// the empty key, a 1 MiB key, a key with every byte, and the longest name
	for _, key := range [][]byte{nil, {}, bytes.Repeat([]byte{0}, 1<<20), func() []byte {
		b := make([]byte, 256)
		for i := range b {
			b[i] = byte(i)
		}
		return b
	}()} {
		if err := w.PutExt(ctx, strings.Repeat("n", 32), key, []byte("v")); err != nil {
			t.Fatalf("key of %d bytes: %v", len(key), err)
		}
		if got, err := w.GetExt(ctx, strings.Repeat("n", 32), key); err != nil || string(got) != "v" {
			t.Fatalf("key of %d bytes: %q %v", len(key), got, err)
		}
	}
	if _, err := w.GetExt(ctx, "trace", []byte("absent")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("absent: %v", err)
	}
	for _, n := range []string{"snapshots", "links", "base"} {
		if _, err := w.ExtKey(n, nil); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("ExtKey(%s): %v", n, err)
		}
	}
}

// ExtKey names the object that PutExt writes and GetExt reads, so it refuses what they refuse: a
// name outside the grammar and a reserved name. Without it a caller would be given the key of an object
// no call can make.
func TestExtKeyRefusesWhatPutExtRefuses(t *testing.T) {
	ctx := context.Background()
	_, _, v := extVolume(t)
	for _, name := range []string{"", "A", "a/b", "a b", "a.b", "a_b", "é", strings.Repeat("n", 33), "snapshots", "links", "base", "../x"} {
		if _, err := v.ExtKey(name, []byte("k")); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("ExtKey(%q) = %v, want ErrInvalid", name, err)
		}
		if err := v.PutExt(ctx, name, []byte("k"), []byte("x")); err == nil {
			t.Errorf("PutExt(%q) was accepted", name)
		}
	}
	for _, name := range []string{"a", "trace", "0", "a-b", strings.Repeat("n", 32), "snapshot", "link", "bases", "ext", "head", "meta", "pack"} {
		at, err := v.ExtKey(name, []byte("k"))
		if err != nil || !strings.HasPrefix(at, "ext/"+name+"/") || len(at) != len("ext/"+name+"/")+64 {
			t.Errorf("ExtKey(%q) = %q, %v", name, at, err)
		}
	}
}
