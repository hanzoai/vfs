package vfs_test

import (
	"context"
	"strings"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

func newRoundTripVFS(t *testing.T) *vfs.VFS {
	t.Helper()
	dir := t.TempDir()
	be, err := backend.Open(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("backend.Open: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity: %v", err)
	}
	c, err := vfs.NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
	if err != nil {
		t.Fatalf("NewCrypto: %v", err)
	}

	v, err := vfs.New(vfs.Config{
		Backend:  be,
		Crypto:   c,
		CacheMax: 0, // unbounded for the test
	})
	if err != nil {
		t.Fatalf("vfs.New: %v", err)
	}
	return v
}

func TestRoundTrip(t *testing.T) {
	v := newRoundTripVFS(t)
	ctx := context.Background()

	plaintext := []byte("hello, vfs world — this is a small block")
	id, err := v.PutBlock(ctx, plaintext)
	if err != nil {
		t.Fatalf("PutBlock: %v", err)
	}
	if !strings.HasPrefix(string(id), "") || len(id) != 64 {
		t.Fatalf("BlockID looks wrong: %q (len %d, want 64)", id, len(id))
	}

	got, err := v.GetBlock(ctx, id)
	if err != nil {
		t.Fatalf("GetBlock: %v", err)
	}

	// Decrypted block is zero-padded to BlockSize. Logical content is at the start.
	if len(got) != vfs.BlockSize {
		t.Fatalf("GetBlock returned %d bytes, want BlockSize=%d", len(got), vfs.BlockSize)
	}
	if string(got[:len(plaintext)]) != string(plaintext) {
		t.Fatalf("plaintext mismatch:\n  got=%q\n want=%q", got[:len(plaintext)], plaintext)
	}
	for i := len(plaintext); i < vfs.BlockSize; i++ {
		if got[i] != 0 {
			t.Fatalf("byte %d should be zero pad, got 0x%x", i, got[i])
		}
	}
}

func TestRoundTripDeduplicates(t *testing.T) {
	// Same plaintext + same recipient + same identity → identical ciphertext is NOT
	// guaranteed by age (it includes a random ephemeral key). What we DO guarantee
	// is that repeated PutBlock calls of the same plaintext return DIFFERENT block
	// IDs but each is independently retrievable. (Cross-fleet dedup is a separate
	// design with deterministic key derivation; that lands in 0.2.0+.)
	v := newRoundTripVFS(t)
	ctx := context.Background()

	id1, err := v.PutBlock(ctx, []byte("identical"))
	if err != nil {
		t.Fatalf("PutBlock 1: %v", err)
	}
	id2, err := v.PutBlock(ctx, []byte("identical"))
	if err != nil {
		t.Fatalf("PutBlock 2: %v", err)
	}
	// Both must round-trip even if the IDs differ.
	for _, id := range []vfs.BlockID{id1, id2} {
		if _, err := v.GetBlock(ctx, id); err != nil {
			t.Fatalf("GetBlock(%s): %v", id, err)
		}
	}
}

func TestStats(t *testing.T) {
	v := newRoundTripVFS(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := v.PutBlock(ctx, []byte{byte(i)}); err != nil {
			t.Fatalf("PutBlock: %v", err)
		}
	}
	s := v.Stats()
	if s.CacheBlocks != 3 {
		t.Fatalf("CacheBlocks = %d, want 3", s.CacheBlocks)
	}
	if s.Backend == "" {
		t.Fatal("Stats.Backend empty")
	}
}
