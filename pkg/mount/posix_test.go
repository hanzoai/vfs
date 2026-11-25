//go:build fuse && linux

package mount

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	bzfs "bazil.org/fuse/fs"

	"github.com/hanzoai/vfs"
)

// The handlers of the POSIX operations, called as the FUSE library calls them, with no kernel:
// what each hands the Volume and what it answers.

func mkfile(t *testing.T, n *node, name string) *node {
	t.Helper()
	nd, h, err := n.Create(context.Background(), &fuse.CreateRequest{Header: fuse.Header{Uid: 7, Gid: 8}, Name: name, Mode: 0o644}, &fuse.CreateResponse{})
	if err != nil {
		t.Fatal(err)
	}
	h.(*handle).file.Close()
	return nd.(*node)
}

func TestRenameLinkSymlinkReadlink(t *testing.T) {
	_, v, _, _ := testVolume(t)
	bg := context.Background()
	root := newSession(bg, v).node(vfs.RootInode)
	d, err := root.Mkdir(bg, &fuse.MkdirRequest{Name: "d", Mode: 0o755})
	if err != nil {
		t.Fatal(err)
	}
	f := mkfile(t, root, "f")
	if err := root.Rename(bg, &fuse.RenameRequest{OldName: "f", NewName: "g"}, d); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Lookup(bg, "f"); err != syscall.ENOENT {
		t.Fatalf("the old name: %v", err)
	}
	g, err := d.(*node).Lookup(bg, "g")
	if err != nil || g.(*node).ino != f.ino {
		t.Fatalf("the new name: %v %v", g, err)
	}
	// the kernel hands a node that is not ours as the new directory only if something is wrong
	if err := root.Rename(bg, &fuse.RenameRequest{OldName: "d", NewName: "x"}, bzfs.Node(nil)); err != syscall.EXDEV {
		t.Fatalf("a rename into a node of another file system: %v", err)
	}
	if err := root.Rename(bg, &fuse.RenameRequest{OldName: "nope", NewName: "x"}, root); err != syscall.ENOENT {
		t.Fatalf("a name that is not there: %v", err)
	}
	// a hard link is the same node: one inode to the kernel
	l, err := root.Link(bg, &fuse.LinkRequest{NewName: "hard"}, g)
	if err != nil || l.(*node) != g.(*node) {
		t.Fatalf("a link answers %v %v, want the node it names", l, err)
	}
	var a fuse.Attr
	if err := l.(*node).Attr(bg, &a); err != nil || a.Nlink != 2 {
		t.Fatalf("attr of a link: %+v %v", a, err)
	}
	if _, err := root.Link(bg, &fuse.LinkRequest{NewName: "dd"}, d); err != syscall.EPERM {
		t.Fatalf("a link to a directory: %v", err)
	}
	s, err := root.Symlink(bg, &fuse.SymlinkRequest{Header: fuse.Header{Uid: 7, Gid: 8}, NewName: "s", Target: "d/g"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.(*node).Readlink(bg, &fuse.ReadlinkRequest{}); err != nil || got != "d/g" {
		t.Fatalf("readlink: %q %v", got, err)
	}
	if err := s.(*node).Attr(bg, &a); err != nil || a.Mode&os.ModeSymlink == 0 || a.Size != 3 || a.Uid != 7 || a.Gid != 8 {
		t.Fatalf("attr of a symlink: %+v %v", a, err)
	}
	if _, err := root.Symlink(bg, &fuse.SymlinkRequest{NewName: "e", Target: ""}); err != syscall.EINVAL {
		t.Fatalf("an empty target: %v", err)
	}
	if _, err := root.Mknod(bg, &fuse.MknodRequest{Name: "dev", Mode: os.ModeDevice | os.ModeCharDevice | 0o600}); err != syscall.EPERM {
		t.Fatalf("a device: %v", err)
	}
	p, err := root.Mknod(bg, &fuse.MknodRequest{Header: fuse.Header{Uid: 7, Gid: 8}, Name: "fifo", Mode: os.ModeNamedPipe | 0o600})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.(*node).Attr(bg, &a); err != nil || a.Mode&os.ModeNamedPipe == 0 || a.Mode.Perm() != 0o600 {
		t.Fatalf("attr of a pipe: %+v %v", a, err)
	}
	ents, err := root.ReadDirAll(bg)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]fuse.DirentType{}
	for _, e := range ents {
		types[e.Name] = e.Type
	}
	if types["s"] != fuse.DT_Link || types["fifo"] != fuse.DT_FIFO || types["d"] != fuse.DT_Dir || types["hard"] != fuse.DT_File || types["."] != fuse.DT_Dir || types[".."] != fuse.DT_Dir {
		t.Fatalf("entry types: %v", types)
	}
}

func TestSetattrIsTheVolumes(t *testing.T) {
	_, v, _, _ := testVolume(t)
	bg := context.Background()
	root := newSession(bg, v).node(vfs.RootInode)
	f := mkfile(t, root, "f")
	at, mt := time.Unix(10, 11), time.Unix(20, 21)
	var resp fuse.SetattrResponse
	req := &fuse.SetattrRequest{
		Valid: fuse.SetattrMode | fuse.SetattrUid | fuse.SetattrAtime | fuse.SetattrMtime,
		Mode:  0o750 | os.ModeSetgid, Uid: 42, Atime: at, Mtime: mt,
	}
	if err := f.Setattr(bg, req, &resp); err != nil {
		t.Fatal(err)
	}
	a := resp.Attr
	if a.Mode.Perm() != 0o750 || a.Mode&os.ModeSetgid == 0 || a.Uid != 42 || a.Gid != 8 || !a.Atime.Equal(at) || !a.Mtime.Equal(mt) {
		t.Fatalf("attr after setattr: %+v (the group the request left out is the file's own: 8)", a)
	}
	// UTIME_NOW is the Volume's clock, and each time on its own
	before := time.Now()
	if err := f.Setattr(bg, &fuse.SetattrRequest{Valid: fuse.SetattrAtimeNow}, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Attr.Atime.Before(before) || !resp.Attr.Mtime.Equal(mt) {
		t.Fatalf("attr after setting the access time to now: %+v", resp.Attr)
	}
	if err := f.Setattr(bg, &fuse.SetattrRequest{Valid: fuse.SetattrMtimeNow}, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Attr.Mtime.Before(before) {
		t.Fatalf("attr after setting the modification time to now: %+v", resp.Attr)
	}
	// a size is a truncation
	if err := f.Setattr(bg, &fuse.SetattrRequest{Valid: fuse.SetattrSize, Size: 5}, &resp); err != nil || resp.Attr.Size != 5 {
		t.Fatalf("attr after setting the size: %+v %v", resp.Attr, err)
	}
	// an inode that has no owner takes the mount's own for the half a request leaves out
	g, _, err := root.Create(bg, &fuse.CreateRequest{Name: "g", Mode: 0o600}, &fuse.CreateResponse{})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.(*node).Setattr(bg, &fuse.SetattrRequest{Valid: fuse.SetattrGid, Gid: 9}, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Attr.Gid != 9 {
		t.Fatalf("gid %d", resp.Attr.Gid)
	}
	if err := g.(*node).Setattr(bg, &fuse.SetattrRequest{Valid: fuse.SetattrSize, Size: vfs.MaxSize + 1}, &resp); err != syscall.EFBIG {
		t.Fatalf("a size past the limit: %v", err)
	}
}

func TestOneInodeIsOneNode(t *testing.T) {
	_, v, _, _ := testVolume(t)
	s := newSession(context.Background(), v)
	a, b := s.node(5), s.node(5)
	if a != b {
		t.Fatal("two nodes for one inode: the kernel would hold two inodes, with two caches")
	}
	a.Forget()
	if c := s.node(5); c == a {
		t.Fatal("a node the kernel forgot is still handed out")
	}
	// a Forget of a node that was already replaced does not remove its successor
	c := s.node(5)
	a.Forget()
	if s.node(5) != c {
		t.Fatal("a stale Forget removed the node that replaced it")
	}
}

func TestStatfsOfAUsage(t *testing.T) {
	r := statfs(vfs.Usage{Files: 3, Bytes: 4096*10 + 1, Pending: 4096 * 3})
	if r.namelen != 255 {
		t.Fatalf("%+v", r)
	}
	if used := r.blocks - r.free; used != 14 {
		t.Fatalf("14 blocks are used (the bytes stored and the bytes written), statfs says %d (%+v)", used, r)
	}
	if r.files-r.ffree != 3 {
		t.Fatalf("3 inodes are used, statfs says %d", r.files-r.ffree)
	}
	if r.blocks != uint64(vfs.MaxSize)/4096 {
		t.Fatalf("without a capacity a Volume is as large as its largest file: %d blocks", r.blocks)
	}
	r = statfs(vfs.Usage{Bytes: 1 << 30, Capacity: 2 << 30})
	if r.blocks != 2<<30/4096 || r.free != 1<<30/4096 {
		t.Fatalf("with a capacity: %+v", r)
	}
	r = statfs(vfs.Usage{Bytes: 3 << 30, Capacity: 2 << 30})
	if r.free != 0 {
		t.Fatalf("over the capacity nothing is free: %+v", r)
	}
}

func TestAttrOfAnInode(t *testing.T) {
	var a fuse.Attr
	attr(&a, vfs.Inode{ID: 9, Mode: fs.ModeDir | 0o755, Nlink: 4, Owned: true, UID: 3, GID: 4, Size: 0})
	if a.Inode != 9 || a.Nlink != 4 || a.Uid != 3 || a.Gid != 4 || !a.Mode.IsDir() {
		t.Fatalf("%+v", a)
	}
	attr(&a, vfs.Inode{ID: 10, Mode: 0o600, Nlink: 1, Size: 1000})
	if int(a.Uid) != os.Getuid() || int(a.Gid) != os.Getgid() || a.Blocks != 2 {
		t.Fatalf("an inode without an owner belongs to whoever runs the mount: %+v", a)
	}
}

// TestFallocateModes: what the handler does with each mode the kernel hands a FUSE file
// system, with no kernel: allocate and zero extend, keep-size does not, punch needs keep-size,
// and what the Volume cannot do (collapse, insert, anything else) is not supported.
func TestFallocateModes(t *testing.T) {
	_, v, _, _ := testVolume(t)
	bg := context.Background()
	root := newSession(bg, v).node(vfs.RootInode)
	_, h, err := root.Create(bg, &fuse.CreateRequest{Name: "g", Mode: 0o644}, &fuse.CreateResponse{})
	if err != nil {
		t.Fatal(err)
	}
	hd := h.(*handle)
	if _, err := hd.file.WriteAt(bg, bytes.Repeat([]byte("x"), 100), 0); err != nil {
		t.Fatal(err)
	}
	const keep, punch, zero = fuse.FAllocateKeepSize, fuse.FAllocatePunchHole, fuse.FAllocateFlags(0x10)
	size := func() uint64 {
		in, err := hd.file.Stat(bg)
		if err != nil {
			t.Fatal(err)
		}
		return in.Size
	}
	read := func() []byte {
		b := make([]byte, 300)
		n, _ := hd.file.ReadAt(bg, b, 0)
		return b[:n]
	}
	do := func(mode fuse.FAllocateFlags, off, n uint64) error {
		return hd.FAllocate(bg, &fuse.FAllocateRequest{Mode: mode, Offset: off, Length: n})
	}
	if err := do(0, 0, 200); err != nil || size() != 200 {
		t.Fatalf("allocate past the end: size %d %v", size(), err)
	}
	if err := do(keep, 0, 500); err != nil || size() != 200 {
		t.Fatalf("keep size: size %d %v", size(), err)
	}
	if err := do(punch|keep, 10, 20); err != nil || size() != 200 || !bytes.Equal(read()[10:30], make([]byte, 20)) || read()[9] != 'x' || read()[30] != 'x' {
		t.Fatalf("punch: size %d %v", size(), err)
	}
	if err := do(zero|keep, 40, 10); err != nil || size() != 200 || !bytes.Equal(read()[40:50], make([]byte, 10)) {
		t.Fatalf("zero range with keep size: size %d %v", size(), err)
	}
	if err := do(zero, 190, 30); err != nil || size() != 220 {
		t.Fatalf("zero range past the end: size %d %v", size(), err)
	}
	for name, mode := range map[string]fuse.FAllocateFlags{
		"punch without keep size": punch, "punch and zero": punch | zero | keep,
		"collapse": 8, "insert": 32, "an unknown flag": 1 << 9,
	} {
		if err := do(mode, 0, 10); err != syscall.EOPNOTSUPP {
			t.Errorf("%s: %v, want EOPNOTSUPP", name, err)
		}
	}
	if err := do(0, vfs.MaxSize, 10); err != syscall.EFBIG {
		t.Fatalf("an allocation past the largest file: %v", err)
	}
	if err := do(0, ^uint64(0), 10); err != syscall.EFBIG {
		t.Fatalf("an allocation that wraps: %v", err)
	}
}
