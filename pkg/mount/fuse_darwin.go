//go:build fuse && darwin

// macOS FUSE driver. bazil.org/fuse dropped macOS support upstream
// (commit 65cc252+); we use github.com/jacobsa/fuse instead, which
// still supports both macFUSE and fuse-t.
//
// Build with: `go build -tags fuse ./...`
// Requires either macFUSE (https://macfuse.io) or fuse-t
// (https://www.fuse-t.org). fuse-t is preferred — no kernel extension,
// runs as a userspace NFSv4 server, works under SIP.
package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/hanzoai/vfs"
)

// Mount mounts the given *vfs.FS at mountpoint and serves until the
// context is cancelled or the kernel unmounts the filesystem.
func Mount(ctx context.Context, fs *vfs.FS, mountpoint string) error {
	if fs == nil {
		return errors.New("mount: vfs.FS required")
	}
	if mountpoint == "" {
		return errors.New("mount: mountpoint required")
	}
	srv := newServer(ctx, fs)
	mfs, err := fuse.Mount(mountpoint, fuseutil.NewFileSystemServer(srv), &fuse.MountConfig{
		FSName:                    "vfs",
		Subtype:                   "hanzovfs",
		ReadOnly:                  false,
		EnableNoOpenSupport:       true,
		EnableNoOpendirSupport:    true,
		DisableDefaultPermissions: false,
	})
	if err != nil {
		return fmt.Errorf("fuse.Mount %q: %w", mountpoint, err)
	}

	go func() {
		<-ctx.Done()
		_ = fuse.Unmount(mountpoint)
	}()

	return mfs.Join(context.Background())
}

// MountWithVFSAdapter mirrors the linux helper.
func MountWithVFSAdapter(ctx context.Context, v *vfs.VFS, mountpoint string) error {
	fs, err := vfs.NewFS(ctx, v)
	if err != nil {
		return fmt.Errorf("mount: NewFS: %w", err)
	}
	return Mount(ctx, fs, mountpoint)
}

// ============================================================================
// FileSystem implementation
// ============================================================================

type server struct {
	fuseutil.NotImplementedFileSystem
	ctx context.Context
	fs  *vfs.FS

	// FUSE inode IDs are u64 starting at 1 (fuseops.RootInodeID).
	// We map directly: VFS InodeID == FUSE InodeID.
	//
	// Open file handles are u64; we allocate sequential IDs.
	nextHandle fuseops.HandleID
	handles    map[fuseops.HandleID]*vfs.File
}

func newServer(ctx context.Context, fs *vfs.FS) *server {
	return &server{
		ctx:        ctx,
		fs:         fs,
		nextHandle: 1,
		handles:    map[fuseops.HandleID]*vfs.File{},
	}
}

func (s *server) inodeAttrs(in *vfs.Inode) fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Size:   in.Size,
		Nlink:  1,
		Mode:   os.FileMode(in.Mode),
		Atime:  in.Atime,
		Mtime:  in.Mtime,
		Ctime:  in.Ctime,
		Crtime: in.Ctime,
		Uid:    uint32(os.Getuid()),
		Gid:    uint32(os.Getgid()),
	}
}

// pathOfInode walks the FS metadata tree to recover the absolute path
// for a given inode ID. Used by every FUSE op the kernel sends as
// (parent_id, name) tuples.
func (s *server) pathOfInode(id fuseops.InodeID) (string, error) {
	if id == fuseops.RootInodeID {
		return "/", nil
	}
	// vfs.FS doesn't currently expose a reverse map (id → path) — walk
	// from the root by joining names up the parent chain. The tree is
	// in memory; this is O(depth).
	return s.fs.PathOfInode(vfs.InodeID(id))
}

// LookUpInode (parent + name) → child attrs.
func (s *server) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) error {
	parentPath, err := s.pathOfInode(op.Parent)
	if err != nil {
		return err
	}
	child := joinPath(parentPath, op.Name)
	in, err := s.fs.Lookup(child)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fuse.ENOENT
		}
		return err
	}
	op.Entry = fuseops.ChildInodeEntry{
		Child:                fuseops.InodeID(in.ID),
		Attributes:           s.inodeAttrs(in),
		AttributesExpiration: time.Now().Add(time.Second),
		EntryExpiration:      time.Now().Add(time.Second),
	}
	return nil
}

func (s *server) GetInodeAttributes(_ context.Context, op *fuseops.GetInodeAttributesOp) error {
	p, err := s.pathOfInode(op.Inode)
	if err != nil {
		return err
	}
	in, err := s.fs.Lookup(p)
	if err != nil {
		return err
	}
	op.Attributes = s.inodeAttrs(in)
	op.AttributesExpiration = time.Now().Add(time.Second)
	return nil
}

func (s *server) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	p, err := s.pathOfInode(op.Inode)
	if err != nil {
		return err
	}
	if op.Size != nil {
		f, err := s.fs.Open(s.ctx, p)
		if err != nil {
			return err
		}
		if err := f.Truncate(*op.Size); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	in, err := s.fs.Lookup(p)
	if err != nil {
		return err
	}
	op.Attributes = s.inodeAttrs(in)
	op.AttributesExpiration = time.Now().Add(time.Second)
	return nil
}

func (s *server) MkDir(_ context.Context, op *fuseops.MkDirOp) error {
	parentPath, err := s.pathOfInode(op.Parent)
	if err != nil {
		return err
	}
	in, err := s.fs.Mkdir(joinPath(parentPath, op.Name), uint32(op.Mode.Perm()))
	if err != nil {
		return err
	}
	op.Entry = fuseops.ChildInodeEntry{
		Child:                fuseops.InodeID(in.ID),
		Attributes:           s.inodeAttrs(in),
		AttributesExpiration: time.Now().Add(time.Second),
		EntryExpiration:      time.Now().Add(time.Second),
	}
	return nil
}

func (s *server) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	parentPath, err := s.pathOfInode(op.Parent)
	if err != nil {
		return err
	}
	child := joinPath(parentPath, op.Name)
	in, err := s.fs.Create(child, uint32(op.Mode.Perm()))
	if err != nil {
		return err
	}
	f, err := s.fs.Open(s.ctx, child)
	if err != nil {
		return err
	}
	op.Entry = fuseops.ChildInodeEntry{
		Child:                fuseops.InodeID(in.ID),
		Attributes:           s.inodeAttrs(in),
		AttributesExpiration: time.Now().Add(time.Second),
		EntryExpiration:      time.Now().Add(time.Second),
	}
	op.Handle = s.allocHandle(f)
	return nil
}

func (s *server) Unlink(_ context.Context, op *fuseops.UnlinkOp) error {
	parentPath, err := s.pathOfInode(op.Parent)
	if err != nil {
		return err
	}
	return s.fs.Remove(joinPath(parentPath, op.Name))
}

func (s *server) RmDir(_ context.Context, op *fuseops.RmDirOp) error {
	parentPath, err := s.pathOfInode(op.Parent)
	if err != nil {
		return err
	}
	return s.fs.Remove(joinPath(parentPath, op.Name))
}

// ─── directory ops ──────────────────────────────────────────────────────────

func (s *server) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	op.Handle = s.allocHandle(nil) // dirs use handle ID but no *vfs.File
	return nil
}

func (s *server) ReadDir(_ context.Context, op *fuseops.ReadDirOp) error {
	p, err := s.pathOfInode(op.Inode)
	if err != nil {
		return err
	}
	children, err := s.fs.ReadDir(p)
	if err != nil {
		return err
	}
	off := int(op.Offset)
	for i, c := range children {
		if i < off {
			continue
		}
		typ := fuseutil.DT_File
		if c.IsDir() {
			typ = fuseutil.DT_Directory
		}
		entry := fuseutil.Dirent{
			Offset: fuseops.DirOffset(i + 1),
			Inode:  fuseops.InodeID(c.ID),
			Name:   c.Name,
			Type:   typ,
		}
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], entry)
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
	return nil
}

func (s *server) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) error {
	delete(s.handles, op.Handle)
	return nil
}

// ─── file ops ───────────────────────────────────────────────────────────────

func (s *server) OpenFile(_ context.Context, op *fuseops.OpenFileOp) error {
	p, err := s.pathOfInode(op.Inode)
	if err != nil {
		return err
	}
	f, err := s.fs.Open(s.ctx, p)
	if err != nil {
		return err
	}
	op.Handle = s.allocHandle(f)
	op.KeepPageCache = false
	return nil
}

func (s *server) ReadFile(_ context.Context, op *fuseops.ReadFileOp) error {
	f := s.handles[op.Handle]
	if f == nil {
		return syscall.EBADF
	}
	n, err := f.ReadAt(op.Dst, op.Offset)
	op.BytesRead = n
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (s *server) WriteFile(_ context.Context, op *fuseops.WriteFileOp) error {
	f := s.handles[op.Handle]
	if f == nil {
		return syscall.EBADF
	}
	_, err := f.WriteAt(op.Data, op.Offset)
	return err
}

func (s *server) SyncFile(_ context.Context, op *fuseops.SyncFileOp) error {
	f := s.handles[op.Handle]
	if f == nil {
		return nil
	}
	return f.Sync()
}

func (s *server) FlushFile(_ context.Context, op *fuseops.FlushFileOp) error {
	f := s.handles[op.Handle]
	if f == nil {
		return nil
	}
	return f.Sync()
}

func (s *server) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error {
	if f, ok := s.handles[op.Handle]; ok && f != nil {
		_ = f.Close()
	}
	delete(s.handles, op.Handle)
	return nil
}

func (s *server) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	op.BlockSize = vfs.BlockSize
	// Pretend to have ~1 PiB available. Backend is "unlimited".
	op.Blocks = 1 << 38
	op.BlocksFree = 1 << 38
	op.BlocksAvailable = 1 << 38
	op.IoSize = vfs.BlockSize
	return nil
}

// ─── handle table ───────────────────────────────────────────────────────────

func (s *server) allocHandle(f *vfs.File) fuseops.HandleID {
	id := s.nextHandle
	s.nextHandle++
	s.handles[id] = f
	return id
}

// joinPath is shared with fuse_unix.go via the package; declared here
// for the build-tag-isolated darwin file.
func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}
