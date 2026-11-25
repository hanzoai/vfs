//go:build fuse && darwin

// macOS FUSE driver. bazil.org/fuse dropped macOS support upstream (commit
// 65cc252+); we use github.com/jacobsa/fuse instead, which still supports both
// macFUSE and fuse-t.
//
// Build with: `go build -tags fuse ./...`
// Requires either macFUSE (https://macfuse.io) or fuse-t (https://www.fuse-t.org).
// fuse-t is preferred: no kernel extension, runs as a userspace NFSv4 server,
// works under SIP.
//
// A write is applied to the Volume and appended to its journal on this node's disk before
// it is answered; SyncFile returns when the journal has it on the disk, and FlushFile (close)
// asks for nothing. The store has the change within the Volume's bound, and when the mount
// ends everything is published first.
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
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/hanzoai/vfs"
)

// Mount mounts vol at mountpoint and serves until ctx is cancelled or the kernel
// unmounts it, then flushes the Volume. The caller owns vol and closes it.
func Mount(ctx context.Context, vol *vfs.Volume, mountpoint string) error {
	if vol == nil {
		return errors.New("mount: Volume required")
	}
	if mountpoint == "" {
		return errors.New("mount: mountpoint required")
	}
	srv := newServer(vol)
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
	joined := make(chan error, 1)
	go func() { joined <- mfs.Join(context.Background()) }()
	var serveErr error
	select {
	case serveErr = <-joined:
	case <-ctx.Done():
		serveErr, _ = stop(vol, mountpoint, fuse.Unmount, detach, joined)
	}
	return finish(vol, serveErr)
}

// detach takes the mount off the directory by force, so that what holds it open sees it go.
func detach(mountpoint string) error {
	if out, err := exec.Command("umount", "-f", mountpoint).CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, bytes.TrimSpace(out))
	}
	return nil
}

// server maps FUSE inode numbers straight onto Volume inodes; both start the
// root at 1.
type server struct {
	fuseutil.NotImplementedFileSystem
	vol *vfs.Volume

	mu      sync.Mutex
	next    fuseops.HandleID
	handles map[fuseops.HandleID]*vfs.File
}

func newServer(vol *vfs.Volume) *server {
	return &server{vol: vol, next: 1, handles: map[fuseops.HandleID]*vfs.File{}}
}

// attrs is the kernel's view of an inode. An inode with no owner of its own belongs to
// whoever runs the mount.
func attrs(in vfs.Inode) fuseops.InodeAttributes {
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	if in.Owned {
		uid, gid = in.UID, in.GID
	}
	return fuseops.InodeAttributes{
		Size: in.Size, Nlink: in.Nlink, Mode: os.FileMode(in.Mode),
		Atime: in.Atime, Mtime: in.Mtime, Ctime: in.Ctime, Crtime: in.Ctime,
		Uid: uid, Gid: gid,
	}
}

// creator is who made the request, as the owner of what it creates.
func creator(c fuseops.OpContext) vfs.Option { return vfs.Owner(c.Uid, uint32(os.Getgid())) }

func entry(in vfs.Inode) fuseops.ChildInodeEntry {
	exp := time.Now().Add(time.Second)
	return fuseops.ChildInodeEntry{
		Child: fuseops.InodeID(in.ID), Attributes: attrs(in),
		AttributesExpiration: exp, EntryExpiration: exp,
	}
}

func (s *server) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	ctx, done := patient(ctx)
	defer done()
	in, err := s.vol.Lookup(ctx, vfs.InodeID(op.Parent), op.Name)
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	return nil
}

func (s *server) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	ctx, done := patient(ctx)
	defer done()
	in, err := s.vol.Stat(ctx, vfs.InodeID(op.Inode))
	if err != nil {
		return errno(err)
	}
	op.Attributes = attrs(in)
	op.AttributesExpiration = time.Now().Add(time.Second)
	return nil
}

// SetInodeAttributes changes the mode, the owner, the times and the size.
func (s *server) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	c := vfs.Change{Size: op.Size, Atime: op.Atime, Mtime: op.Mtime}
	if op.Mode != nil {
		m := fs.FileMode(*op.Mode)
		c.Mode = &m
	}
	if op.Uid != nil || op.Gid != nil {
		// An owner is a pair; one half of it that the request leaves out is the inode's.
		cur, err := s.vol.Stat(ctx, vfs.InodeID(op.Inode))
		if err != nil {
			return errno(err)
		}
		uid, gid := cur.UID, cur.GID
		if !cur.Owned {
			uid, gid = uint32(os.Getuid()), uint32(os.Getgid())
		}
		if op.Uid != nil {
			uid = *op.Uid
		}
		if op.Gid != nil {
			gid = *op.Gid
		}
		c.UID, c.GID = &uid, &gid
	}
	in, err := s.vol.SetAttr(ctx, vfs.InodeID(op.Inode), c)
	if err != nil {
		return errno(err)
	}
	op.Attributes = attrs(in)
	op.AttributesExpiration = time.Now().Add(time.Second)
	return nil
}

func (s *server) MkDir(ctx context.Context, op *fuseops.MkDirOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Mkdir(ctx, vfs.InodeID(op.Parent), op.Name, op.Mode, creator(op.OpContext))
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	return nil
}

func (s *server) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Create(ctx, vfs.InodeID(op.Parent), op.Name, op.Mode, creator(op.OpContext))
	if err != nil {
		return errno(err)
	}
	f, err := s.vol.Open(ctx, in.ID)
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	op.Handle = s.add(f)
	return nil
}

// Rename moves an entry, atomically. The flags of renameat2 are not delivered.
func (s *server) Rename(ctx context.Context, op *fuseops.RenameOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	return errno(s.vol.Rename(ctx, vfs.InodeID(op.OldParent), op.OldName, vfs.InodeID(op.NewParent), op.NewName, 0))
}

// CreateLink names an inode a second time.
func (s *server) CreateLink(ctx context.Context, op *fuseops.CreateLinkOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Link(ctx, vfs.InodeID(op.Target), vfs.InodeID(op.Parent), op.Name)
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	return nil
}

func (s *server) CreateSymlink(ctx context.Context, op *fuseops.CreateSymlinkOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Symlink(ctx, vfs.InodeID(op.Parent), op.Name, op.Target, creator(op.OpContext))
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	return nil
}

func (s *server) ReadSymlink(ctx context.Context, op *fuseops.ReadSymlinkOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	target, err := s.vol.Readlink(ctx, vfs.InodeID(op.Inode))
	op.Target = target
	return errno(err)
}

// MkNode makes a regular file, a named pipe or a socket. A device is refused.
func (s *server) MkNode(ctx context.Context, op *fuseops.MkNodeOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Mknod(ctx, vfs.InodeID(op.Parent), op.Name, op.Mode, creator(op.OpContext))
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	return nil
}

func (s *server) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	if err := kind(ctx, s.vol, vfs.InodeID(op.Parent), op.Name, false); err != nil {
		return errno(err)
	}
	return errno(s.vol.Remove(ctx, vfs.InodeID(op.Parent), op.Name))
}

func (s *server) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	if err := kind(ctx, s.vol, vfs.InodeID(op.Parent), op.Name, true); err != nil {
		return errno(err)
	}
	return errno(s.vol.Remove(ctx, vfs.InodeID(op.Parent), op.Name))
}

// kind checks that name in dir is the kind of thing the caller is removing:
// unlink removes a file and refuses a directory, rmdir the reverse. (The Linux
// kernel makes this check itself before it asks the file system; an NFS-backed
// mount does not.)
func kind(ctx context.Context, vol *vfs.Volume, dir vfs.InodeID, name string, wantDir bool) error {
	in, err := vol.Lookup(ctx, dir, name)
	if err != nil {
		return err
	}
	switch {
	case wantDir && !in.IsDir():
		return vfs.ErrNotDir
	case !wantDir && in.IsDir():
		return vfs.ErrIsDir
	}
	return nil
}

// ─── directories ────────────────────────────────────────────────────────────

func (s *server) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	op.Handle = s.add(nil) // a directory needs a handle id, not a file
	return nil
}

func (s *server) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) error {
	ctx, done := patient(ctx)
	defer done()
	ents, err := s.vol.ReadDir(ctx, vfs.InodeID(op.Inode))
	if err != nil {
		return errno(err)
	}
	for i, e := range ents {
		if i < int(op.Offset) {
			continue
		}
		typ := fuseutil.DT_File
		switch {
		case e.Mode.IsDir():
			typ = fuseutil.DT_Directory
		case e.Mode&fs.ModeSymlink != 0:
			typ = fuseutil.DT_Link
		case e.Mode&fs.ModeNamedPipe != 0:
			typ = fuseutil.DT_FIFO
		case e.Mode&fs.ModeSocket != 0:
			typ = fuseutil.DT_Socket
		}
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], fuseutil.Dirent{
			Offset: fuseops.DirOffset(i + 1), Inode: fuseops.InodeID(e.ID), Name: e.Name, Type: typ,
		})
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
	return nil
}

func (s *server) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) error {
	s.drop(op.Handle)
	return nil
}

// ─── files ──────────────────────────────────────────────────────────────────

func (s *server) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	f, err := s.vol.Open(ctx, vfs.InodeID(op.Inode))
	if err != nil {
		return errno(err)
	}
	op.Handle = s.add(f)
	op.KeepPageCache = false
	return nil
}

func (s *server) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	f := s.file(op.Handle)
	if f == nil {
		return syscall.EBADF
	}
	n, err := f.ReadAt(ctx, op.Dst, op.Offset)
	op.BytesRead = n
	if err != nil && !errors.Is(err, io.EOF) {
		return errno(err)
	}
	return nil
}

func (s *server) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	f := s.file(op.Handle)
	if f == nil {
		return syscall.EBADF
	}
	_, err := f.WriteAt(ctx, op.Data, op.Offset)
	return errno(err)
}

// A sync is not abandoned halfway because the kernel interrupted the call: it runs to its end.
func (s *server) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error {
	return errno(s.vol.SyncInode(context.WithoutCancel(ctx), vfs.InodeID(op.Inode)))
}

// FlushFile is the kernel's close(2): it asks for nothing.
func (s *server) FlushFile(context.Context, *fuseops.FlushFileOp) error { return nil }

func (s *server) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error {
	if f := s.file(op.Handle); f != nil {
		_ = f.Close()
	}
	s.drop(op.Handle)
	return nil
}

func (s *server) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	u := statfs(s.vol.Usage())
	op.BlockSize, op.IoSize = blockSize, blockSize
	op.Blocks, op.BlocksFree, op.BlocksAvailable = u.blocks, u.free, u.free
	op.Inodes, op.InodesFree = u.files, u.ffree
	return nil
}

// ─── handle table ───────────────────────────────────────────────────────────

func (s *server) add(f *vfs.File) fuseops.HandleID {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.next
	s.next++
	s.handles[id] = f
	return id
}

func (s *server) file(id fuseops.HandleID) *vfs.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handles[id]
}

func (s *server) drop(id fuseops.HandleID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, id)
}
