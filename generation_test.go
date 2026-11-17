package vfs

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// TestRecutHonoursItsContext: cutting a file's bytes into chunks stops when the publisher's
// context ends (the Volume is closed), and nothing is lost: the generation is made again whole
// by a publication with a live context, and the file is what was written.
func TestRecutHonoursItsContext(t *testing.T) {
	be, v := internalVolume(t, Config{})
	in, err := v.Create(context.Background(), RootInode, "f", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(context.Background(), in.ID)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("0123456789abcdef"), 6<<16) // 6 MiB
	if _, err := f.WriteAt(context.Background(), data, 0); err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	g := v.cut()
	v.mu.Unlock()
	if g == nil {
		t.Fatal("nothing to publish")
	}
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.settle(gone, newUploader(gone, v.be, 2)); !errors.Is(err, context.Canceled) {
		t.Fatalf("settle with a cancelled context = %v, want context.Canceled", err)
	}
	if err := g.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.complete(); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(context.Background(), Config{Backend: be, Cipher: plainCipher{}})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	g2, err := ro.Open(context.Background(), in.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := g2.ReadAt(context.Background(), got, 0); n != len(data) || (err != nil && err.Error() != "EOF") {
		t.Fatalf("read %d of %d: %v", n, len(data), err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the file is not what was written")
	}
}
