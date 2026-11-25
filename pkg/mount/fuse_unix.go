//go:build fuse && linux

// NOTE: bazil.org/fuse dropped macOS support (commit 65cc252+); macOS uses
// jacobsa/fuse in fuse_darwin.go.

// Package mount serves a *vfs.Volume to the kernel over FUSE, so any POSIX
// program (SQLite, Postgres, ripgrep) reads and writes the sealed Volume as a
// mounted directory.
//
// The mount is made with default_permissions: the kernel checks every access against the
// mode, owner and group the Volume reports, so a Volume enforces no permission itself. What
// the file system does not implement is answered as the kernel answers it: extended
// attributes are not supported, and the flags of renameat2 and O_TMPFILE are refused (the
// library that speaks the FUSE protocol has neither request). Advisory locks, flock and
// fcntl, are the kernel's, per mount.
//
// A write on the mount is applied to the Volume and appended to its journal on this
// node's disk before it is answered. fsync returns when the journal has the change on the
// disk (group sync: concurrent callers share one), and asks the store for nothing; close
// asks for nothing either. The store has the change within the Volume's bound, and when
// the mount ends everything is published first. What the kernel itself holds back (a
// write that was acknowledged to its caller and has not reached the daemon) is the
// caller's to fsync, as for any file system; the end of a mount asks the kernel for it
// (syncfs) before it publishes.
//
// Build-tag gated: `go build -tags fuse ./...`.
package mount

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"bazil.org/fuse"
	bzfs "bazil.org/fuse/fs"

	"github.com/hanzoai/vfs"
)

// Mount mounts vol at mountpoint and serves until ctx is cancelled or the kernel
// unmounts it, then flushes the Volume. The caller owns vol and closes it. When ctx is
// cancelled the mount commits, is unmounted, and if something holds it open is detached
// after a few seconds, so that Mount returns within a bounded time (stop.go).
func Mount(ctx context.Context, vol *vfs.Volume, mountpoint string) error {
	if vol == nil {
		return errors.New("mount: Volume required")
	}
	if mountpoint == "" {
		return errors.New("mount: mountpoint required")
	}
	conn, err := fuse.Mount(mountpoint,
		fuse.FSName("vfs"),
		fuse.Subtype("hanzovfs"),
		fuse.AllowNonEmptyMount(),
		fuse.DefaultPermissions(),
	)
	if err != nil {
		return fmt.Errorf("fuse.Mount %q: %w", mountpoint, err)
	}

	// The mount's own context is not ctx: a read of a file still held open runs on it, and
	// must go on while the end of the mount waits for its holders.
	base, end := context.WithCancel(context.WithoutCancel(ctx))
	defer end()
	sess := newSession(base, vol)
	served := make(chan error, 1)
	go func() { served <- bzfs.New(conn, nil).Serve(&root{s: sess}) }()
	var serveErr error
	select {
	case serveErr = <-served:
		_ = conn.Close()
	case <-ctx.Done():
		var ended bool
		serveErr, ended = stop(vol, mountpoint, fuse.Unmount, detach, served)
		// A holder that outlived the grace keeps Serve reading, and closing the connection
		// waits for that read: it is left to end with the process.
		if ended {
			_ = conn.Close()
		}
	}
	if serveErr != nil {
		serveErr = fmt.Errorf("fuse.Serve: %w", serveErr)
	}
	return finish(vol, serveErr)
}

// detach takes the mount off the directory now and lets what holds it finish: fusermount
// -u -z. Nothing new can reach it, and the kernel drops the connection when the last
// holder lets go.
func detach(mountpoint string) error {
	if out, err := exec.Command("fusermount3", "-u", "-z", mountpoint).CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, bytes.TrimSpace(out))
	}
	return nil
}

// session is what every node and handle of one mount shares: the Volume and the mount's
// own context. It is built only by newSession, which refuses a nil context: the work of a
// read runs on base, and a store client panics on a nil parent the first time it is used.
type session struct {
	vol  *vfs.Volume
	base context.Context

	mu    sync.Mutex
	nodes map[vfs.InodeID]*node // the nodes the kernel holds, by inode
}

func newSession(base context.Context, vol *vfs.Volume) *session {
	if base == nil || vol == nil {
		panic("mount: a session needs a context and a Volume")
	}
	return &session{vol: vol, base: base}
}

// root is the file system.
type root struct{ s *session }

var (
	_ bzfs.FS         = (*root)(nil)
	_ bzfs.FSStatfser = (*root)(nil)
)

func (r *root) Root() (bzfs.Node, error) {
	return r.s.node(vfs.RootInode), nil
}

// Statfs reports the Volume's own numbers: the bytes it stores, against its capacity or,
// when it has none, against the largest file it can hold.
func (r *root) Statfs(ctx context.Context, _ *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	u := statfs(r.s.vol.Usage())
	*resp = fuse.StatfsResponse{
		Blocks: u.blocks, Bfree: u.free, Bavail: u.free, Files: u.files, Ffree: u.ffree,
		Bsize: blockSize, Frsize: blockSize, Namelen: u.namelen,
	}
	return nil
}

// node is a file or directory, named by its inode.
//
// A node is built only by session.node, so it always carries the session: the
// Volume, the mount's context and the committer. A struct literal would leave the
// context nil, and a store client that uses its context would panic on the first read.
type node struct {
	s   *session
	ino vfs.InodeID
}

// node returns the node of an inode of this mount: the same one for the same inode, so
// that every name of a file, every hard link, is one inode to the kernel and shares its
// cached size, times and pages. The FUSE library numbers a node by its identity, so a node
// built afresh for each lookup would be a new inode each time. The kernel's Forget removes it.
func (s *session) node(ino vfs.InodeID) *node {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.nodes[ino]; n != nil {
		return n
	}
	if s.nodes == nil {
		s.nodes = make(map[vfs.InodeID]*node)
	}
	n := &node{s: s, ino: ino}
	s.nodes[ino] = n
	return n
}

// Forget is the kernel letting go of an inode: the next lookup of it is a new node.
func (n *node) Forget() {
	n.s.mu.Lock()
	defer n.s.mu.Unlock()
	if n.s.nodes[n.ino] == n {
		delete(n.s.nodes, n.ino)
	}
}

// handle returns the handle of a file opened on this mount.
func (s *session) handle(f *vfs.File) *handle { return &handle{s: s, file: f} }

var (
	_ bzfs.Node               = (*node)(nil)
	_ bzfs.NodeStringLookuper = (*node)(nil)
	_ bzfs.HandleReadDirAller = (*node)(nil)
	_ bzfs.NodeMkdirer        = (*node)(nil)
	_ bzfs.NodeCreater        = (*node)(nil)
	_ bzfs.NodeRemover        = (*node)(nil)
	_ bzfs.NodeSetattrer      = (*node)(nil)
	_ bzfs.NodeFsyncer        = (*node)(nil)
	_ bzfs.NodeOpener         = (*node)(nil)
	_ bzfs.NodeRenamer        = (*node)(nil)
	_ bzfs.NodeLinker         = (*node)(nil)
	_ bzfs.NodeSymlinker      = (*node)(nil)
	_ bzfs.NodeReadlinker     = (*node)(nil)
	_ bzfs.NodeMknoder        = (*node)(nil)
	_ bzfs.NodeForgetter      = (*node)(nil)
)

// attr fills the kernel's view of an inode. An inode with no owner of its own belongs to
// whoever runs the mount.
func attr(a *fuse.Attr, in vfs.Inode) {
	a.Inode = uint64(in.ID)
	a.Mode = os.FileMode(in.Mode)
	a.Size = in.Size
	a.Blocks = (in.Size + 511) / 512
	a.Mtime, a.Atime, a.Ctime = in.Mtime, in.Atime, in.Ctime
	a.Nlink = in.Nlink
	a.Uid, a.Gid = owner(in)
	a.BlockSize = blockSize
}

// owner is the user and group an inode appears to have.
func owner(in vfs.Inode) (uid, gid uint32) {
	if in.Owned {
		return in.UID, in.GID
	}
	return uint32(os.Getuid()), uint32(os.Getgid())
}

// creator is who made the request, as the owner of what it creates.
func creator(h fuse.Header) vfs.Option { return vfs.Owner(h.Uid, h.Gid) }

// kind is the type of a directory entry as readdir says it.
func kind(m os.FileMode) fuse.DirentType {
	switch {
	case m.IsDir():
		return fuse.DT_Dir
	case m&os.ModeSymlink != 0:
		return fuse.DT_Link
	case m&os.ModeNamedPipe != 0:
		return fuse.DT_FIFO
	case m&os.ModeSocket != 0:
		return fuse.DT_Socket
	}
	return fuse.DT_File
}

func (n *node) Attr(ctx context.Context, a *fuse.Attr) error {
	ctx, done := patient(ctx)
	defer done()
	in, err := n.s.vol.Stat(ctx, n.ino)
	if err != nil {
		return errno(err)
	}
	attr(a, in)
	return nil
}

func (n *node) Lookup(ctx context.Context, name string) (bzfs.Node, error) {
	ctx, done := patient(ctx)
	defer done()
	in, err := n.s.vol.Lookup(ctx, n.ino, name)
	if err != nil {
		return nil, errno(err)
	}
	return n.s.node(in.ID), nil
}

func (n *node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	ctx, done := patient(ctx)
	defer done()
	ents, err := n.s.vol.ReadDir(ctx, n.ino)
	if err != nil {
		return nil, errno(err)
	}
	up, err := n.s.vol.Parent(ctx, n.ino)
	if err != nil {
		return nil, errno(err)
	}
	// A listing starts with "." and "..": POSIX requires them, and the kernel does
	// not make them up for a file system.
	out := make([]fuse.Dirent, 0, len(ents)+2)
	out = append(out,
		fuse.Dirent{Inode: uint64(n.ino), Name: ".", Type: fuse.DT_Dir},
		fuse.Dirent{Inode: uint64(up), Name: "..", Type: fuse.DT_Dir})
	for _, e := range ents {
		out = append(out, fuse.Dirent{Inode: uint64(e.ID), Name: e.Name, Type: kind(os.FileMode(e.Mode))})
	}
	return out, nil
}

func (n *node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (bzfs.Node, error) {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := n.s.vol.Mkdir(ctx, n.ino, req.Name, req.Mode, creator(req.Header))
	if err != nil {
		return nil, errno(err)
	}
	return n.s.node(in.ID), nil
}

func (n *node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (bzfs.Node, bzfs.Handle, error) {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	// A name that another caller removes while this one is looked up or opened is
	// created again; a name that is removed every time is not an error of this call.
	for range 8 {
		in, f, err := n.create(ctx, req)
		if errors.Is(err, errRemoved) {
			continue
		}
		if err != nil {
			return nil, nil, errno(err)
		}
		attr(&resp.Attr, in)
		return n.s.node(in.ID), n.s.handle(f), nil
	}
	return nil, nil, syscall.EAGAIN
}

// errRemoved says that the name was removed between two steps of a creation.
var errRemoved = errors.New("mount: the name was removed while it was being created")

// create makes the file, or opens the one that exists: open(O_CREAT) of a file that
// exists opens it, and only O_EXCL makes that an error. A kernel that does not look the
// name up first, or one whose lookup was interrupted and not repeated, asks for the
// creation anyway.
func (n *node) create(ctx context.Context, req *fuse.CreateRequest) (vfs.Inode, *vfs.File, error) {
	in, err := n.s.vol.Create(ctx, n.ino, req.Name, req.Mode, creator(req.Header))
	if errors.Is(err, fs.ErrExist) && req.Flags&fuse.OpenFlags(os.O_EXCL) == 0 {
		if in, err = n.s.vol.Lookup(ctx, n.ino, req.Name); errors.Is(err, fs.ErrNotExist) {
			return in, nil, errRemoved
		}
		switch {
		case err != nil:
		case in.IsDir():
			err = vfs.ErrIsDir
		case req.Flags&fuse.OpenFlags(os.O_TRUNC) != 0:
			err = n.s.vol.Truncate(ctx, in.ID, 0)
		}
	}
	if err != nil {
		return in, nil, err
	}
	f, err := n.s.vol.Open(ctx, in.ID)
	if errors.Is(err, fs.ErrNotExist) {
		return in, nil, errRemoved
	}
	return in, f, err
}

func (n *node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	return errno(n.s.vol.Remove(ctx, n.ino, req.Name))
}

// Rename moves an entry, atomically. The kernel has made the checks that need the whole
// path (a directory into itself, a name that is a mount point), and the Volume makes the
// rest. Flags are not delivered: the FUSE library of this mount has no renameat2.
func (n *node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir bzfs.Node) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	nd, ok := newDir.(*node)
	if !ok {
		return syscall.EXDEV
	}
	return errno(n.s.vol.Rename(ctx, n.ino, req.OldName, nd.ino, req.NewName, 0))
}

// Link names an inode a second time.
func (n *node) Link(ctx context.Context, req *fuse.LinkRequest, old bzfs.Node) (bzfs.Node, error) {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	o, ok := old.(*node)
	if !ok {
		return nil, syscall.EXDEV
	}
	in, err := n.s.vol.Link(ctx, o.ino, n.ino, req.NewName)
	if err != nil {
		return nil, errno(err)
	}
	return n.s.node(in.ID), nil
}

func (n *node) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (bzfs.Node, error) {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := n.s.vol.Symlink(ctx, n.ino, req.NewName, req.Target, creator(req.Header))
	if err != nil {
		return nil, errno(err)
	}
	return n.s.node(in.ID), nil
}

func (n *node) Readlink(ctx context.Context, _ *fuse.ReadlinkRequest) (string, error) {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	target, err := n.s.vol.Readlink(ctx, n.ino)
	return target, errno(err)
}

// Mknod makes a regular file, a named pipe or a socket. A device is refused.
func (n *node) Mknod(ctx context.Context, req *fuse.MknodRequest) (bzfs.Node, error) {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := n.s.vol.Mknod(ctx, n.ino, req.Name, req.Mode, creator(req.Header))
	if err != nil {
		return nil, errno(err)
	}
	return n.s.node(in.ID), nil
}

// Setattr changes the mode, the owner, the times and the size. UTIME_NOW is the Volume's
// clock; UTIME_OMIT leaves a time as it is (the kernel does not send it).
func (n *node) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	var c vfs.Change
	if req.Valid.Mode() {
		m := fs.FileMode(req.Mode)
		c.Mode = &m
	}
	if req.Valid.Uid() || req.Valid.Gid() {
		// An owner is a pair; one half of it that the request leaves out is the inode's.
		cur, err := n.s.vol.Stat(ctx, n.ino)
		if err != nil {
			return errno(err)
		}
		uid, gid := owner(cur)
		if req.Valid.Uid() {
			uid = req.Uid
		}
		if req.Valid.Gid() {
			gid = req.Gid
		}
		c.UID, c.GID = &uid, &gid
	}
	if req.Valid.Size() {
		c.Size = &req.Size
	}
	now := n.s.vol.Now()
	switch {
	case req.Valid.AtimeNow():
		c.Atime = &now
	case req.Valid.Atime():
		c.Atime = &req.Atime
	}
	switch {
	case req.Valid.MtimeNow():
		c.Mtime = &now
	case req.Valid.Mtime():
		c.Mtime = &req.Mtime
	}
	in, err := n.s.vol.SetAttr(ctx, n.ino, c)
	if err != nil {
		return errno(err)
	}
	attr(&resp.Attr, in)
	return nil
}

func (n *node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (bzfs.Handle, error) {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := n.s.vol.Stat(ctx, n.ino)
	if err != nil {
		return nil, errno(err)
	}
	if in.IsDir() {
		return n, nil // served by ReadDirAll
	}
	if req.Flags&fuse.OpenFlags(os.O_TRUNC) != 0 {
		if err := n.s.vol.Truncate(ctx, n.ino, 0); err != nil {
			return nil, errno(err)
		}
	}
	f, err := n.s.vol.Open(ctx, n.ino)
	if err != nil {
		return nil, errno(err)
	}
	return n.s.handle(f), nil
}

// Fsync makes what the file depends on durable in the journal, and waits for nothing else:
// not for another file's writes, and not for the store. It is not interrupted: C callers
// do not restart it (errno.go).
func (n *node) Fsync(ctx context.Context, _ *fuse.FsyncRequest) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	return errno(n.s.vol.SyncInode(ctx, n.ino))
}

// handle is an open file.
type handle struct {
	s    *session
	file *vfs.File
}

var (
	_ bzfs.Handle           = (*handle)(nil)
	_ bzfs.HandleReader     = (*handle)(nil)
	_ bzfs.HandleWriter     = (*handle)(nil)
	_ bzfs.HandleReleaser   = (*handle)(nil)
	_ bzfs.HandleFlusher    = (*handle)(nil)
	_ bzfs.HandleFAllocater = (*handle)(nil)
)

func (h *handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// The work of a read is the mount's, not the request's (errno.go).
	buf := make([]byte, req.Size)
	n, err := h.file.ReadAt(h.s.base, buf, req.Offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return errno(err)
	}
	resp.Data = buf[:n]
	return nil
}

func (h *handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	n, err := h.file.WriteAt(ctx, req.Data, req.Offset)
	if err != nil {
		return errno(err)
	}
	resp.Size = n
	return nil
}

// The flags of fallocate(2) that the kernel hands a FUSE file system.
const fallocZeroRange = 0x10

// FAllocate reserves, deallocates or zeroes a range. A Volume reserves nothing: a range is
// bytes that cost nothing until they are written, so an allocation extends the file as
// fallocate does and keeps no room back. Punching a hole, and zeroing a range, deallocate it.
func (h *handle) FAllocate(ctx context.Context, req *fuse.FAllocateRequest) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	keep := req.Mode&fuse.FAllocateKeepSize != 0
	punch := req.Mode&fuse.FAllocatePunchHole != 0
	zero := req.Mode&fallocZeroRange != 0
	if req.Mode&^(fuse.FAllocateKeepSize|fuse.FAllocatePunchHole|fallocZeroRange) != 0 || punch && zero || punch && !keep {
		return syscall.EOPNOTSUPP
	}
	if req.Offset+req.Length < req.Offset || req.Offset+req.Length > vfs.MaxSize {
		return syscall.EFBIG
	}
	if punch || zero {
		if err := h.file.Punch(ctx, req.Offset, req.Length); err != nil {
			return errno(err)
		}
	}
	if end := req.Offset + req.Length; !keep && !punch {
		in, err := h.file.Stat(ctx)
		if err != nil {
			return errno(err)
		}
		if end > in.Size {
			return errno(h.file.Truncate(ctx, end))
		}
	}
	return nil
}

// Flush is the kernel's close(2) of one descriptor. It makes no request and changes
// nothing: close is not an fsync, and a handle that wrote nothing has nothing to say.
func (h *handle) Flush(context.Context, *fuse.FlushRequest) error { return nil }

func (h *handle) Release(context.Context, *fuse.ReleaseRequest) error {
	return errno(h.file.Close())
}
