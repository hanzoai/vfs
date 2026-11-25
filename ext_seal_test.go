package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

// An extension object is sealed to its Volume, its extension and its key: copied to another
// Volume, another extension or another key it does not open.
func TestExtensionObjectIsSealedToItsVolumeNameAndKey(t *testing.T) {
	ctx := context.Background()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	vol := func() (*storetest.Mem, *Volume) {
		be := storetest.NewMem()
		if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
			t.Fatal(err)
		}
		v, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 1, Journal: storetest.Journals(), JournalCapacity: 16 << 20, Disk: storetest.Disk})
		if err != nil {
			t.Fatal(err)
		}
		return be, v
	}
	beA, a := vol()
	beB, b := vol()
	if err := a.PutExt(ctx, "one", []byte("k1"), []byte("secret")); err != nil {
		t.Fatal(err)
	}
	src, err := a.ExtKey("one", []byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	sealed := beA.Dump()[src]
	for name, dst := range map[string]struct {
		be   *storetest.Mem
		v    *Volume
		ext  string
		key  string
		desc string
	}{
		"another Volume":    {beB, b, "one", "k1", "same name and key"},
		"another extension": {beA, a, "two", "k1", "same Volume and key"},
		"another key":       {beA, a, "one", "k2", "same Volume and name"},
	} {
		at, err := dst.v.ExtKey(dst.ext, []byte(dst.key))
		if err != nil {
			t.Fatal(err)
		}
		if err := dst.be.Put(ctx, at, sealed); err != nil {
			t.Fatal(err)
		}
		if _, err := dst.v.GetExt(ctx, dst.ext, []byte(dst.key)); !errors.Is(err, ErrCorrupt) {
			t.Errorf("an object copied to %s (%s) opened: %v", name, dst.desc, err)
		}
	}
}

// A replacement that loses the race for the object (another writer replaced it between the
// read of its version and the write) reads the version again and tries again, up to extReplace
// times, and then says that it lost.
func TestExtensionReplaceRetriesAndGivesUp(t *testing.T) {
	ctx := context.Background()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	h := &storetest.Hook{Backend: storetest.NewMem()}
	if err := Create(ctx, Config{Backend: h, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := Attach(ctx, Config{Backend: h, Cipher: c, Round: 1, Journal: storetest.Journals(), JournalCapacity: 16 << 20, Disk: storetest.Disk})
	if err != nil {
		t.Fatal(err)
	}
	if err := v.PutExt(ctx, "retry", []byte("k"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	for lose := range extReplace { // the number of replacements lost before one lands: 0 to 7
		var lost, tried int
		h.Before = func(op, key string) error {
			if op == "putver" && strings.HasPrefix(key, "ext/retry/") {
				tried++
				if lost < lose {
					lost++
					return backend.ErrConflict
				}
			}
			return nil
		}
		data := []byte(fmt.Sprintf("after %d lost", lose))
		if err := v.PutExt(ctx, "retry", []byte("k"), data); err != nil {
			t.Fatalf("after %d lost replacements: %v", lose, err)
		}
		if got, err := v.GetExt(ctx, "retry", []byte("k")); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("after %d lost replacements the object is %q, %v", lose, got, err)
		}
		if tried != lose+1 {
			t.Fatalf("%d replacements lost: tried %d times, want %d", lose, tried, lose+1)
		}
	}
	// lost every time: the loss is reported, after extReplace tries and no more
	tried := 0
	h.Before = func(op, key string) error {
		if op == "putver" && strings.HasPrefix(key, "ext/retry/") {
			tried++
			return backend.ErrConflict
		}
		return nil
	}
	err = v.PutExt(ctx, "retry", []byte("k"), []byte("never"))
	if !errors.Is(err, backend.ErrConflict) || tried != extReplace {
		t.Fatalf("a replacement lost every time: %v after %d tries, want ErrConflict after %d", err, tried, extReplace)
	}
	h.Before = nil
	if got, _ := v.GetExt(ctx, "retry", []byte("k")); !bytes.Equal(got, []byte("after 7 lost")) {
		t.Fatalf("a replacement that never landed changed the object to %q", got)
	}
}
