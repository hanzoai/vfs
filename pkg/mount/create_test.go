//go:build fuse && linux

package mount

import (
	"context"
	"os"
	"syscall"
	"testing"

	"bazil.org/fuse"
)

// TestCreateOfAnExistingFileOpensIt: open(O_CREAT) of a file that exists opens it, and
// only O_EXCL makes that an error. A kernel that does not look the name up first, or one
// whose lookup was interrupted and not repeated, asks for the creation anyway.
func TestCreateOfAnExistingFileOpensIt(t *testing.T) {
	_, v, _, _ := testVolume(t)
	bg := context.Background()
	root := newSession(bg, v).node(1)
	create := func(name string, flags int) (*handle, error) {
		_, h, err := root.Create(bg, &fuse.CreateRequest{Name: name, Mode: 0o644, Flags: fuse.OpenFlags(flags)}, &fuse.CreateResponse{})
		if err != nil {
			return nil, err
		}
		return h.(*handle), nil
	}
	first, err := create("f", os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Write(bg, &fuse.WriteRequest{Data: []byte("kept"), Offset: 0}, &fuse.WriteResponse{}); err != nil {
		t.Fatal(err)
	}
	if _, err := create("f", os.O_CREATE|os.O_WRONLY|os.O_EXCL); err != syscall.EEXIST {
		t.Fatalf("create with O_EXCL of an existing file = %v, want EEXIST", err)
	}
	again, err := create("f", os.O_CREATE|os.O_WRONLY)
	if err != nil {
		t.Fatalf("create without O_EXCL of an existing file = %v, want it opened", err)
	}
	resp := &fuse.ReadResponse{}
	if err := again.Read(bg, &fuse.ReadRequest{Size: 10, Offset: 0}, resp); err != nil || string(resp.Data) != "kept" {
		t.Fatalf("the file opened by the second create holds %q, %v", resp.Data, err)
	}
	if _, err := create("f", os.O_CREATE|os.O_WRONLY|os.O_TRUNC); err != nil {
		t.Fatal(err)
	}
	resp = &fuse.ReadResponse{}
	if err := again.Read(bg, &fuse.ReadRequest{Size: 10, Offset: 0}, resp); err != nil || len(resp.Data) != 0 {
		t.Fatalf("after O_TRUNC the file holds %q, %v", resp.Data, err)
	}
	if _, err := root.Mkdir(bg, &fuse.MkdirRequest{Name: "d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if _, err := create("d", os.O_CREATE|os.O_WRONLY); err != syscall.EISDIR {
		t.Fatalf("create of a directory's name = %v, want EISDIR", err)
	}
}

// TestCreateOfANameBeingRemoved: a name that exists when the creation is refused and is
// gone when it is looked up is created again, not answered ENOENT.
func TestCreateOfANameBeingRemoved(t *testing.T) {
	_, v, _, _ := testVolume(t)
	bg := context.Background()
	root := newSession(bg, v).node(1)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = v.Create(bg, 1, "x", 0o644)
			_ = v.Remove(bg, 1, "x")
		}
	}()
	defer func() { close(stop); <-done }()
	for i := range 3000 {
		_, _, err := root.Create(bg, &fuse.CreateRequest{Name: "x", Mode: 0o644, Flags: fuse.OpenFlags(os.O_CREATE | os.O_WRONLY)}, &fuse.CreateResponse{})
		if err != nil {
			t.Fatalf("create %d of a name that is being removed = %v", i, err)
		}
	}
}
