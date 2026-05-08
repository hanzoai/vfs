//go:build fuse && linux

// NOTE: bazil.org/fuse dropped macOS support (commit 65cc252+). On
// macOS use the default no-fuse build for in-process testing, or
// switch to github.com/jacobsa/fuse if you need a real FUSE mount
// (a 0.3.1 follow-up adds a parallel mount_darwin.go using jacobsa).

// Package mount wires the in-process *vfs.FS and *vfs.File to the
// kernel via bazil.org/fuse so any POSIX consumer (SQLite, Postgres,
// Redis, ripgrep, anything) can read/write the encrypted block VFS as
// if it were a normal mounted filesystem.
//
// Build-tag gated: `go build -tags fuse ./...`. Default builds stay
// dependency-light (no FUSE link).
package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	bzfs "bazil.org/fuse/fs"

	"github.com/hanzoai/vfs/pkg/vfs"
)

// Mount mounts the given *vfs.FS at mountpoint and serves until the
// context is cancelled or the kernel unmounts the filesystem.
//
// The caller is responsible for unmount-on-shutdown — typically:
//
//	go func() { <-ctx.Done(); _ = fuse.Unmount(mountpoint) }()
//
// On clean exit Mount returns nil; on kernel-side error it returns
// the underlying fuse error.
func Mount(ctx context.Context, fs *vfs.FS, mountpoint string) error {
	if fs == nil {
		return errors.New("mount: vfs.FS required")
	}
	if mountpoint == "" {
		return errors.New("mount: mountpoint required")
	}

	conn, err := fuse.Mount(mountpoint,
		fuse.FSName("vfs"),
		fuse.Subtype("hanzovfs"),
		fuse.AllowNonEmptyMount(),
	)
	if err != nil {
		return fmt.Errorf("fuse.Mount %q: %w", mountpoint, err)
	}
	defer conn.Close()

	// Ensure unmount on context cancel.
	go func() {
		<-ctx.Done()
		_ = fuse.Unmount(mountpoint)
	}()

	srv := bzfs.New(conn, nil)
	if err := srv.Serve(&fsRoot{fs: fs, ctx: ctx}); err != nil {
		return fmt.Errorf("fuse.Serve: %w", err)
	}
	return nil
}

// MountWithVFSAdapter is the alias used by the CLI when it constructs
// a *vfs.VFS first and only wraps it as a *vfs.FS at mount time.
//
// It exists so the public Mount(*vfs.FS, mp) signature stays canonical
// while the cmd path can pass the cheaper *vfs.VFS form.
func MountWithVFSAdapter(ctx context.Context, v *vfs.VFS, mountpoint string) error {
	fs, err := vfs.NewFS(ctx, v)
	if err != nil {
		return fmt.Errorf("mount: NewFS: %w", err)
	}
	return Mount(ctx, fs, mountpoint)
}

// ============================================================================
// fs.FS — root + node lookup
// ============================================================================

type fsRoot struct {
	fs  *vfs.FS
	ctx context.Context
}

var _ bzfs.FS = (*fsRoot)(nil)

func (r *fsRoot) Root() (bzfs.Node, error) {
	in, err := r.fs.Lookup("/")
	if err != nil {
		return nil, err
	}
	return &node{root: r, inode: in, path: "/"}, nil
}

// ============================================================================
// fs.Node — dir + file
// ============================================================================

type node struct {
	root  *fsRoot
	inode *vfs.Inode
	path  string
}

var (
	_ bzfs.Node               = (*node)(nil)
	_ bzfs.NodeStringLookuper = (*node)(nil)
	_ bzfs.HandleReadDirAller = (*node)(nil)
	_ bzfs.NodeMkdirer        = (*node)(nil)
	_ bzfs.NodeCreater        = (*node)(nil)
	_ bzfs.NodeRemover        = (*node)(nil)
	_ bzfs.NodeSetattrer      = (*node)(nil)
	_ bzfs.NodeFsyncer        = (*node)(nil)
)

func (n *node) Attr(_ context.Context, a *fuse.Attr) error {
	// Re-fetch in case another writer mutated the inode.
	fresh, err := n.root.fs.Lookup(n.path)
	if err != nil {
		return err
	}
	n.inode = fresh
	a.Inode = uint64(fresh.ID)
	a.Mode = os.FileMode(fresh.Mode)
	a.Size = fresh.Size
	a.Blocks = (fresh.Size + 511) / 512
	a.Mtime = fresh.Mtime
	a.Atime = fresh.Atime
	a.Ctime = fresh.Ctime
	a.Nlink = 1
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	a.BlockSize = vfs.BlockSize
	return nil
}

func (n *node) Lookup(_ context.Context, name string) (bzfs.Node, error) {
	child := joinPath(n.path, name)
	in, err := n.root.fs.Lookup(child)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, syscall.ENOENT
		}
		return nil, err
	}
	return &node{root: n.root, inode: in, path: child}, nil
}

func (n *node) ReadDirAll(_ context.Context) ([]fuse.Dirent, error) {
	if !n.inode.IsDir() {
		return nil, syscall.ENOTDIR
	}
	children, err := n.root.fs.ReadDir(n.path)
	if err != nil {
		return nil, err
	}
	out := make([]fuse.Dirent, 0, len(children))
	for _, c := range children {
		typ := fuse.DT_File
		if c.IsDir() {
			typ = fuse.DT_Dir
		}
		out = append(out, fuse.Dirent{
			Inode: uint64(c.ID),
			Name:  c.Name,
			Type:  typ,
		})
	}
	return out, nil
}

func (n *node) Mkdir(_ context.Context, req *fuse.MkdirRequest) (bzfs.Node, error) {
	child := joinPath(n.path, req.Name)
	in, err := n.root.fs.Mkdir(child, uint32(req.Mode.Perm()))
	if err != nil {
		return nil, err
	}
	return &node{root: n.root, inode: in, path: child}, nil
}

func (n *node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (bzfs.Node, bzfs.Handle, error) {
	child := joinPath(n.path, req.Name)
	in, err := n.root.fs.Create(child, uint32(req.Mode.Perm()))
	if err != nil {
		return nil, nil, err
	}
	cn := &node{root: n.root, inode: in, path: child}
	f, err := n.root.fs.Open(ctx, child)
	if err != nil {
		return nil, nil, err
	}
	resp.Attr.Inode = uint64(in.ID)
	resp.Attr.Mode = os.FileMode(in.Mode)
	resp.Attr.Size = in.Size
	resp.Attr.Mtime = in.Mtime
	resp.Attr.Atime = in.Atime
	resp.Attr.Ctime = in.Ctime
	resp.Attr.Nlink = 1
	resp.Attr.Uid = uint32(os.Getuid())
	resp.Attr.Gid = uint32(os.Getgid())
	resp.Attr.BlockSize = vfs.BlockSize
	return cn, &handle{node: cn, file: f}, nil
}

func (n *node) Remove(_ context.Context, req *fuse.RemoveRequest) error {
	child := joinPath(n.path, req.Name)
	if err := n.root.fs.Remove(child); err != nil {
		return err
	}
	return nil
}

func (n *node) Setattr(_ context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		f, err := n.root.fs.Open(n.root.ctx, n.path)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := f.Truncate(req.Size); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
	}
	// Refresh attr for response.
	fresh, err := n.root.fs.Lookup(n.path)
	if err != nil {
		return err
	}
	n.inode = fresh
	resp.Attr.Inode = uint64(fresh.ID)
	resp.Attr.Mode = os.FileMode(fresh.Mode)
	resp.Attr.Size = fresh.Size
	resp.Attr.Mtime = fresh.Mtime
	resp.Attr.Atime = fresh.Atime
	resp.Attr.Ctime = fresh.Ctime
	resp.Attr.Nlink = 1
	resp.Attr.Uid = uint32(os.Getuid())
	resp.Attr.Gid = uint32(os.Getgid())
	resp.Attr.BlockSize = vfs.BlockSize
	return nil
}

func (n *node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (bzfs.Handle, error) {
	if n.inode.IsDir() {
		return n, nil // fs.HandleReadDirAller
	}
	// O_TRUNC: caller wants size==0
	if req.Flags&fuse.OpenFlags(os.O_TRUNC) != 0 {
		f, err := n.root.fs.Open(ctx, n.path)
		if err != nil {
			return nil, err
		}
		if err := f.Truncate(0); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
		return &handle{node: n, file: f}, nil
	}
	f, err := n.root.fs.Open(ctx, n.path)
	if err != nil {
		return nil, err
	}
	return &handle{node: n, file: f}, nil
}

func (n *node) Fsync(_ context.Context, _ *fuse.FsyncRequest) error {
	// Directory fsync: persist FS metadata.
	return n.root.fs.Sync(n.root.ctx)
}

// ============================================================================
// fs.Handle — open file
// ============================================================================

type handle struct {
	node *node
	file *vfs.File
}

var (
	_ bzfs.Handle         = (*handle)(nil)
	_ bzfs.HandleReader   = (*handle)(nil)
	_ bzfs.HandleWriter   = (*handle)(nil)
	_ bzfs.HandleReleaser = (*handle)(nil)
	_ bzfs.HandleFlusher  = (*handle)(nil)
)

func (h *handle) Read(_ context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := h.file.ReadAt(buf, req.Offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	resp.Data = buf[:n]
	return nil
}

func (h *handle) Write(_ context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n, err := h.file.WriteAt(req.Data, req.Offset)
	if err != nil {
		return err
	}
	resp.Size = n
	return nil
}

func (h *handle) Flush(_ context.Context, _ *fuse.FlushRequest) error {
	return h.file.Sync()
}

func (h *handle) Release(_ context.Context, _ *fuse.ReleaseRequest) error {
	return h.file.Close()
}

// ============================================================================
// helpers
// ============================================================================

func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// keep imports referenced if a build mode strips one
var _ = time.Now
