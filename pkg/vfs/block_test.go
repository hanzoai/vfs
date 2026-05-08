package vfs

import (
	"strings"
	"testing"
)

func TestHashBlockDeterministic(t *testing.T) {
	a := HashBlock([]byte("hello"))
	b := HashBlock([]byte("hello"))
	if a != b {
		t.Fatalf("hash not deterministic: %s != %s", a, b)
	}
	c := HashBlock([]byte("hello!"))
	if a == c {
		t.Fatalf("hash should differ for different inputs: a=%s c=%s", a, c)
	}
}

func TestBlockIDPath(t *testing.T) {
	id := HashBlock([]byte("test"))
	p := id.Path()
	if !strings.HasPrefix(p, "blocks/") {
		t.Fatalf("expected blocks/ prefix, got %q", p)
	}
	if !strings.HasSuffix(p, ".zap.age") {
		t.Fatalf("expected .zap.age suffix, got %q", p)
	}
	if !strings.Contains(p, string(id[:2])+"/") {
		t.Fatalf("expected 2-hex shard prefix containing %q, got %q", string(id[:2]), p)
	}
}

func TestVerifyMatch(t *testing.T) {
	ct := []byte("ciphertext bytes")
	id := HashBlock(ct)
	if err := id.Verify(ct); err != nil {
		t.Fatalf("verify same bytes: %v", err)
	}
	if err := id.Verify([]byte("different")); err == nil {
		t.Fatal("verify should fail on tampered bytes")
	}
}

func TestPad(t *testing.T) {
	short := Pad([]byte("abc"))
	if len(short) != BlockSize {
		t.Fatalf("Pad short: got %d want %d", len(short), BlockSize)
	}
	for i := 3; i < BlockSize; i++ {
		if short[i] != 0 {
			t.Fatalf("Pad short: byte %d should be zero, got 0x%x", i, short[i])
		}
	}

	exact := make([]byte, BlockSize)
	for i := range exact {
		exact[i] = byte(i)
	}
	out := Pad(exact)
	if len(out) != BlockSize {
		t.Fatalf("Pad exact: got %d want %d", len(out), BlockSize)
	}

	long := make([]byte, BlockSize+100)
	out = Pad(long)
	if len(out) != BlockSize {
		t.Fatalf("Pad long: got %d want %d", len(out), BlockSize)
	}
}
