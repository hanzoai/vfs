//go:build fuse && linux

package writeback

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
	"sync/atomic"
	"syscall"
	"time"

	bzfuse "bazil.org/fuse"
	bzfs "bazil.org/fuse/fs"
	jfuse "github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/hanzoai/vfs"
)

// FUSEEngine selects which already-vendored FUSE protocol implementation serves
// the Linux mount:
//   - EngineJacobsa: github.com/jacobsa/fuse — negotiates 1 MiB max_write with
//     FUSE_MAX_PAGES=256 and supports FUSE_WRITEBACK_CACHE (HIP-1342 #13 target).
//   - EngineBazil: bazil.org/fuse — legacy 128 KiB max_write cap with optional
//     FUSE_WRITEBACK_CACHE (baseline comparison).
type FUSEEngine string

const (
	EngineJacobsa FUSEEngine = "jacobsa"
	EngineBazil   FUSEEngine = "bazil"
)

// FUSEStats exposes live atomic counters observed by the FUSE daemon process.
type FUSEStats struct {
	WriteRPCs         atomic.Int64
	WriteBytes        atomic.Int64
	MaxWriteRPCSize   atomic.Int64
	ReadRPCs          atomic.Int64
	ReadBytes         atomic.Int64
	FsyncRPCs         atomic.Int64
	FlushRPCs         atomic.Int64
	SyncS3PutsInFlush atomic.Int64
}

// FUSEStatsSnapshot is a JSON-serializable snapshot of FUSEStats.
type FUSEStatsSnapshot struct {
	WriteRPCs         int64 `json:"write_rpcs"`
	WriteBytes        int64 `json:"write_bytes"`
	MaxWriteRPCSize   int64 `json:"max_write_rpc_size"`
	ReadRPCs          int64 `json:"read_rpcs"`
	ReadBytes         int64 `json:"read_bytes"`
	FsyncRPCs         int64 `json:"fsync_rpcs"`
	FlushRPCs         int64 `json:"flush_rpcs"`
	SyncS3PutsInFlush int64 `json:"sync_s3_puts_in_flush"`
}

// Snapshot returns a point-in-time copy of the daemon's FUSE RPC counters.
func (s *FUSEStats) Snapshot() FUSEStatsSnapshot {
	if s == nil {
		return FUSEStatsSnapshot{}
	}
	return FUSEStatsSnapshot{
		WriteRPCs:         s.WriteRPCs.Load(),
		WriteBytes:        s.WriteBytes.Load(),
		MaxWriteRPCSize:   s.MaxWriteRPCSize.Load(),
		ReadRPCs:          s.ReadRPCs.Load(),
		ReadBytes:         s.ReadBytes.Load(),
		FsyncRPCs:         s.FsyncRPCs.Load(),
		FlushRPCs:         s.FlushRPCs.Load(),
		SyncS3PutsInFlush: s.SyncS3PutsInFlush.Load(),
	}
}

func (s *FUSEStats) observeWrite(n int64) {
	if s == nil {
		return
	}
	s.WriteRPCs.Add(1)
	s.WriteBytes.Add(n)
	for {
		cur := s.MaxWriteRPCSize.Load()
		if n <= cur {
			return
		}
		if s.MaxWriteRPCSize.CompareAndSwap(cur, n) {
			return
		}
	}
}

// MountOptions configures a Linux FUSE mount with explicit writeback policy,
// engine selection, lifecycle barrier reporting, and optional observability.
type MountOptions struct {
	Config         WritebackConfig
	Engine         FUSEEngine // default: EngineJacobsa when MaxWriteBytes > 128 KiB, else EngineJacobsa
	Stats          *FUSEStats
	BackendCounter *CountingBackend
	LifecycleEvent LifecycleEvent // default: EventEnd
	OnReady        func()
	OnBarrier      func(BarrierReport)
	EnableGuard    bool // run periodic DirtyGuard syncfs while mounted
}

// Mount serves vol over Linux FUSE at mountpoint using opts until ctx is
// cancelled or unmounted externally. On cancellation, Mount enforces the
// mandatory LifecycleBarrier (syncfs -> journal sync -> final publish) BEFORE
// unmounting so kernel-buffered dirty pages are never silently dropped on
// Detach, Suspend, or End.
func Mount(ctx context.Context, vol *vfs.Volume, mountpoint string, opts MountOptions) error {
	if vol == nil {
		return fmt.Errorf("%w: Volume required", ErrInvalidConfig)
	}
	if mountpoint == "" {
		return fmt.Errorf("%w: mountpoint required", ErrInvalidConfig)
	}
	cfg, err := opts.Config.Validate()
	if err != nil {
		return err
	}
	engine := opts.Engine
	if engine == "" {
		if cfg.MaxWriteBytes <= LegacyMaxWriteBytes && !cfg.WritebackCache {
			engine = EngineBazil
		} else {
			engine = EngineJacobsa
		}
	}
	if opts.Stats == nil {
		opts.Stats = &FUSEStats{}
	}
	if opts.LifecycleEvent == "" {
		opts.LifecycleEvent = EventEnd
	}
	barrier, err := NewVolumeBarrier(cfg, vol)
	if err != nil {
		return err
	}

	switch engine {
	case EngineJacobsa:
		return mountJacobsa(ctx, vol, mountpoint, cfg, opts, barrier)
	case EngineBazil:
		return mountBazil(ctx, vol, mountpoint, cfg, opts, barrier)
	default:
		return fmt.Errorf("%w: unknown FUSE engine %q", ErrInvalidConfig, engine)
	}
}

func mountJacobsa(ctx context.Context, vol *vfs.Volume, mountpoint string, cfg WritebackConfig, opts MountOptions, barrier *LifecycleBarrier) error {
	srv := &jServer{
		vol:     vol,
		cfg:     cfg,
		stats:   opts.Stats,
		counter: opts.BackendCounter,
		next:    1,
		handles: make(map[fuseops.HandleID]*vfs.File),
	}
	mfs, err := jfuse.Mount(mountpoint, fuseutil.NewFileSystemServer(srv), &jfuse.MountConfig{
		FSName:                    "vfs",
		Subtype:                   "hanzovfs",
		ReadOnly:                  false,
		DisableWritebackCaching:   !cfg.WritebackCache,
		DisableDefaultPermissions: false,
		EnableAsyncReads:          true,
		EnableParallelDirOps:      true,
	})
	if err != nil {
		return fmt.Errorf("jacobsa fuse.Mount %q: %w", mountpoint, err)
	}

	var guard *DirtyGuard
	if opts.EnableGuard && cfg.WritebackCache {
		guard, _ = StartDirtyGuard(cfg, mountpoint, SyncFS, nil)
	}

	if opts.OnReady != nil {
		opts.OnReady()
	}

	joined := make(chan error, 1)
	go func() { joined <- mfs.Join(context.Background()) }()

	var serveErr error
	var barrierErr error
	select {
	case serveErr = <-joined:
		if guard != nil {
			guard.Stop()
		}
	case <-ctx.Done():
		if guard != nil {
			guard.Stop()
		}
		rep, bErr := barrier.Execute(context.Background(), opts.LifecycleEvent, mountpoint)
		barrierErr = bErr
		if opts.OnBarrier != nil {
			opts.OnBarrier(rep)
		}
		serveErr, _ = unmountAndWait(mountpoint, jfuse.Unmount, joined)
	}
	return errors.Join(serveErr, barrierErr)
}

func mountBazil(ctx context.Context, vol *vfs.Volume, mountpoint string, cfg WritebackConfig, opts MountOptions, barrier *LifecycleBarrier) error {
	mopts := []bzfuse.MountOption{
		bzfuse.FSName("vfs"),
		bzfuse.Subtype("hanzovfs"),
		bzfuse.AllowNonEmptyMount(),
		bzfuse.DefaultPermissions(),
		bzfuse.AsyncRead(),
	}
	if cfg.WritebackCache {
		mopts = append(mopts, bzfuse.WritebackCache())
	}
	conn, err := bzfuse.Mount(mountpoint, mopts...)
	if err != nil {
		return fmt.Errorf("bazil fuse.Mount %q: %w", mountpoint, err)
	}

	var guard *DirtyGuard
	if opts.EnableGuard && cfg.WritebackCache {
		guard, _ = StartDirtyGuard(cfg, mountpoint, SyncFS, nil)
	}

	base, cancelBase := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelBase()
	sess := &bSession{
		vol:     vol,
		base:    base,
		cfg:     cfg,
		stats:   opts.Stats,
		counter: opts.BackendCounter,
		nodes:   make(map[vfs.InodeID]*bNode),
	}

	if opts.OnReady != nil {
		opts.OnReady()
	}

	served := make(chan error, 1)
	go func() { served <- bzfs.New(conn, nil).Serve(&bRoot{s: sess}) }()

	var serveErr error
	var barrierErr error
	select {
	case serveErr = <-served:
		if guard != nil {
			guard.Stop()
		}
		_ = conn.Close()
	case <-ctx.Done():
		if guard != nil {
			guard.Stop()
		}
		rep, bErr := barrier.Execute(context.Background(), opts.LifecycleEvent, mountpoint)
		barrierErr = bErr
		if opts.OnBarrier != nil {
			opts.OnBarrier(rep)
		}
		var ended bool
		serveErr, ended = unmountAndWait(mountpoint, bzfuse.Unmount, served)
		// In bazil.org/fuse, conn.Close() blocks waiting for an active Serve read
		// if an external holder still has an open fd after lazy detach. Only close
		// when Serve has actually exited; otherwise process exit releases the fd.
		if ended {
			_ = conn.Close()
		}
	}
	return errors.Join(serveErr, barrierErr)
}

func unmountAndWait(mountpoint string, unmount func(string) error, done <-chan error) (error, bool) {
	deadline := time.Now().Add(1500 * time.Millisecond)
	var busy error
	for {
		if busy = unmount(mountpoint); busy == nil {
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		select {
		case err := <-done:
			return err, true
		case <-time.After(100 * time.Millisecond):
		}
	}
	if busy != nil {
		_ = detachLazy(mountpoint)
	}
	select {
	case err := <-done:
		return err, true
	case <-time.After(1500 * time.Millisecond):
		return nil, false
	}
}

func detachLazy(mountpoint string) error {
	if out, err := exec.Command("fusermount3", "-u", "-z", mountpoint).CombinedOutput(); err != nil {
		if out2, err2 := exec.Command("fusermount", "-u", "-z", mountpoint).CombinedOutput(); err2 != nil {
			return fmt.Errorf("lazy unmount %s: %v (%s) / %v (%s)", mountpoint, err, bytes.TrimSpace(out), err2, bytes.TrimSpace(out2))
		}
	}
	return nil
}

// ─── jacobsa/fuse server (1 MiB max_write + FUSE_MAX_PAGES + writeback) ─────

const fuseBlockSize = 4096

type jServer struct {
	fuseutil.NotImplementedFileSystem
	vol     *vfs.Volume
	cfg     WritebackConfig
	stats   *FUSEStats
	counter *CountingBackend

	mu      sync.Mutex
	next    fuseops.HandleID
	handles map[fuseops.HandleID]*vfs.File
}

func (s *jServer) add(f *vfs.File) fuseops.HandleID {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.next
	s.next++
	s.handles[id] = f
	return id
}

func (s *jServer) file(id fuseops.HandleID) *vfs.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handles[id]
}

func (s *jServer) drop(id fuseops.HandleID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, id)
}

func jAttrs(in vfs.Inode) fuseops.InodeAttributes {
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	if in.Owned {
		uid, gid = in.UID, in.GID
	}
	return fuseops.InodeAttributes{
		Size:   in.Size,
		Nlink:  in.Nlink,
		Mode:   os.FileMode(in.Mode),
		Atime:  in.Atime,
		Mtime:  in.Mtime,
		Ctime:  in.Ctime,
		Crtime: in.Ctime,
		Uid:    uid,
		Gid:    gid,
	}
}

func jEntry(in vfs.Inode) fuseops.ChildInodeEntry {
	exp := time.Now().Add(time.Second)
	return fuseops.ChildInodeEntry{
		Child:                fuseops.InodeID(in.ID),
		Attributes:           jAttrs(in),
		AttributesExpiration: exp,
		EntryExpiration:      exp,
	}
}

func jCreator(c fuseops.OpContext) vfs.Option { return vfs.Owner(c.Uid, uint32(os.Getgid())) }

func (s *jServer) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	u := s.vol.Usage()
	capBytes := uint64(vfs.MaxSize)
	if u.Capacity > 0 {
		capBytes = uint64(u.Capacity)
	}
	blocks := capBytes / fuseBlockSize
	used := min(blocks, (u.Bytes+u.Pending+fuseBlockSize-1)/fuseBlockSize)
	const inodes = 1 << 40
	op.BlockSize = fuseBlockSize
	op.IoSize = s.cfg.MaxWriteBytes
	op.Blocks = blocks
	op.BlocksFree = blocks - used
	op.BlocksAvailable = blocks - used
	op.Inodes = inodes
	op.InodesFree = inodes - min(inodes, u.Files)
	return nil
}

func (s *jServer) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	in, err := s.vol.Lookup(context.WithoutCancel(ctx), vfs.InodeID(op.Parent), op.Name)
	if err != nil {
		return mapErrno(err)
	}
	op.Entry = jEntry(in)
	return nil
}

func (s *jServer) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	in, err := s.vol.Stat(context.WithoutCancel(ctx), vfs.InodeID(op.Inode))
	if err != nil {
		return mapErrno(err)
	}
	op.Attributes = jAttrs(in)
	op.AttributesExpiration = time.Now().Add(time.Second)
	return nil
}

func (s *jServer) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	ctx = context.WithoutCancel(ctx)
	c := vfs.Change{Size: op.Size, Atime: op.Atime, Mtime: op.Mtime}
	if op.Mode != nil {
		m := fs.FileMode(*op.Mode)
		c.Mode = &m
	}
	if op.Uid != nil || op.Gid != nil {
		cur, err := s.vol.Stat(ctx, vfs.InodeID(op.Inode))
		if err != nil {
			return mapErrno(err)
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
		// Under FUSE_WRITEBACK_CACHE, Linux's syncfs(2) invokes fuse_flush_times
		// (mtime/atime-only FUSE_SETATTR) for recently unlinked inodes that have
		// not yet received FUSE_FORGET. Returning ENOENT poisons syncfs(2).
		if s.cfg.WritebackCache && errors.Is(err, fs.ErrNotExist) &&
			c.Mode == nil && c.UID == nil && c.GID == nil && c.Size == nil {
			return nil
		}
		return mapErrno(err)
	}
	op.Attributes = jAttrs(in)
	op.AttributesExpiration = time.Now().Add(time.Second)
	return nil
}

func (s *jServer) MkDir(ctx context.Context, op *fuseops.MkDirOp) error {
	in, err := s.vol.Mkdir(context.WithoutCancel(ctx), vfs.InodeID(op.Parent), op.Name, op.Mode, jCreator(op.OpContext))
	if err != nil {
		return mapErrno(err)
	}
	op.Entry = jEntry(in)
	return nil
}

func (s *jServer) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	ctx = context.WithoutCancel(ctx)
	in, err := s.vol.Create(ctx, vfs.InodeID(op.Parent), op.Name, op.Mode, jCreator(op.OpContext))
	if err != nil {
		return mapErrno(err)
	}
	f, err := s.vol.Open(ctx, in.ID)
	if err != nil {
		return mapErrno(err)
	}
	op.Entry = jEntry(in)
	op.Handle = s.add(f)
	return nil
}

func (s *jServer) Rename(ctx context.Context, op *fuseops.RenameOp) error {
	return mapErrno(s.vol.Rename(context.WithoutCancel(ctx), vfs.InodeID(op.OldParent), op.OldName, vfs.InodeID(op.NewParent), op.NewName, 0))
}

func (s *jServer) CreateLink(ctx context.Context, op *fuseops.CreateLinkOp) error {
	in, err := s.vol.Link(context.WithoutCancel(ctx), vfs.InodeID(op.Target), vfs.InodeID(op.Parent), op.Name)
	if err != nil {
		return mapErrno(err)
	}
	op.Entry = jEntry(in)
	return nil
}

func (s *jServer) CreateSymlink(ctx context.Context, op *fuseops.CreateSymlinkOp) error {
	in, err := s.vol.Symlink(context.WithoutCancel(ctx), vfs.InodeID(op.Parent), op.Name, op.Target, jCreator(op.OpContext))
	if err != nil {
		return mapErrno(err)
	}
	op.Entry = jEntry(in)
	return nil
}

func (s *jServer) ReadSymlink(ctx context.Context, op *fuseops.ReadSymlinkOp) error {
	target, err := s.vol.Readlink(context.WithoutCancel(ctx), vfs.InodeID(op.Inode))
	op.Target = target
	return mapErrno(err)
}

func (s *jServer) MkNode(ctx context.Context, op *fuseops.MkNodeOp) error {
	in, err := s.vol.Mknod(context.WithoutCancel(ctx), vfs.InodeID(op.Parent), op.Name, op.Mode, jCreator(op.OpContext))
	if err != nil {
		return mapErrno(err)
	}
	op.Entry = jEntry(in)
	return nil
}

func (s *jServer) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	return mapErrno(s.vol.Remove(context.WithoutCancel(ctx), vfs.InodeID(op.Parent), op.Name))
}

func (s *jServer) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	return mapErrno(s.vol.Remove(context.WithoutCancel(ctx), vfs.InodeID(op.Parent), op.Name))
}

func (s *jServer) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	op.Handle = s.add(nil)
	return nil
}

func (s *jServer) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) error {
	ents, err := s.vol.ReadDir(context.WithoutCancel(ctx), vfs.InodeID(op.Inode))
	if err != nil {
		return mapErrno(err)
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
			Offset: fuseops.DirOffset(i + 1),
			Inode:  fuseops.InodeID(e.ID),
			Name:   e.Name,
			Type:   typ,
		})
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
	return nil
}

func (s *jServer) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) error {
	s.drop(op.Handle)
	return nil
}

func (s *jServer) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	f, err := s.vol.Open(context.WithoutCancel(ctx), vfs.InodeID(op.Inode))
	if err != nil {
		return mapErrno(err)
	}
	op.Handle = s.add(f)
	op.KeepPageCache = s.cfg.WritebackCache
	return nil
}

func (s *jServer) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	f := s.file(op.Handle)
	if f == nil {
		return syscall.EBADF
	}
	n, err := f.ReadAt(context.WithoutCancel(ctx), op.Dst, op.Offset)
	op.BytesRead = n
	if s.stats != nil {
		s.stats.ReadRPCs.Add(1)
		s.stats.ReadBytes.Add(int64(n))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return mapErrno(err)
	}
	return nil
}

func (s *jServer) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	f := s.file(op.Handle)
	if f == nil {
		return syscall.EBADF
	}
	n, err := f.WriteAt(context.WithoutCancel(ctx), op.Data, op.Offset)
	if s.stats != nil {
		s.stats.observeWrite(int64(n))
	}
	return mapErrno(err)
}

func (s *jServer) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error {
	if s.stats != nil {
		s.stats.FsyncRPCs.Add(1)
	}
	return mapErrno(s.vol.SyncInode(context.WithoutCancel(ctx), vfs.InodeID(op.Inode)))
}

// FlushFile handles close(2) with ZERO synchronous object-store requests.
func (s *jServer) FlushFile(_ context.Context, _ *fuseops.FlushFileOp) error {
	if s.stats != nil {
		s.stats.FlushRPCs.Add(1)
	}
	// By contract, close(2) never issues synchronous S3 requests; publication
	// runs asynchronously from the local NVMe journal within PublishBound.
	return nil
}

func (s *jServer) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error {
	if f := s.file(op.Handle); f != nil {
		_ = f.Close()
	}
	s.drop(op.Handle)
	return nil
}

// ─── bazil/fuse server (128 KiB max_write baseline) ──────────────────────────

type bSession struct {
	vol     *vfs.Volume
	base    context.Context
	cfg     WritebackConfig
	stats   *FUSEStats
	counter *CountingBackend

	mu    sync.Mutex
	nodes map[vfs.InodeID]*bNode
}

func (s *bSession) node(ino vfs.InodeID) *bNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.nodes[ino]; n != nil {
		return n
	}
	n := &bNode{s: s, ino: ino}
	s.nodes[ino] = n
	return n
}

type bRoot struct{ s *bSession }

func (r *bRoot) Root() (bzfs.Node, error) { return r.s.node(vfs.RootInode), nil }

func (r *bRoot) Statfs(_ context.Context, _ *bzfuse.StatfsRequest, resp *bzfuse.StatfsResponse) error {
	u := r.s.vol.Usage()
	capBytes := uint64(vfs.MaxSize)
	if u.Capacity > 0 {
		capBytes = uint64(u.Capacity)
	}
	blocks := capBytes / fuseBlockSize
	used := min(blocks, (u.Bytes+u.Pending+fuseBlockSize-1)/fuseBlockSize)
	const inodes = 1 << 40
	*resp = bzfuse.StatfsResponse{
		Blocks:  blocks,
		Bfree:   blocks - used,
		Bavail:  blocks - used,
		Files:   inodes,
		Ffree:   inodes - min(inodes, u.Files),
		Bsize:   fuseBlockSize,
		Frsize:  fuseBlockSize,
		Namelen: 255,
	}
	return nil
}

type bNode struct {
	s                 *bSession
	ino               vfs.InodeID
	unlinkedWriteback atomic.Bool
}

func (n *bNode) Forget() {
	n.s.mu.Lock()
	defer n.s.mu.Unlock()
	if n.s.nodes[n.ino] == n {
		delete(n.s.nodes, n.ino)
	}
}

func bAttr(a *bzfuse.Attr, in vfs.Inode) {
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	if in.Owned {
		uid, gid = in.UID, in.GID
	}
	a.Inode = uint64(in.ID)
	a.Mode = os.FileMode(in.Mode)
	a.Size = in.Size
	a.Blocks = (in.Size + 511) / 512
	a.Mtime, a.Atime, a.Ctime = in.Mtime, in.Atime, in.Ctime
	a.Nlink = in.Nlink
	a.Uid, a.Gid = uid, gid
	a.BlockSize = fuseBlockSize
}

func (n *bNode) Attr(ctx context.Context, a *bzfuse.Attr) error {
	in, err := n.s.vol.Stat(context.WithoutCancel(ctx), n.ino)
	if err != nil {
		if n.s.cfg.WritebackCache && errors.Is(err, fs.ErrNotExist) && n.unlinkedWriteback.Load() {
			a.Inode = uint64(n.ino)
			a.Mode = 0o644
			return nil
		}
		return mapErrno(err)
	}
	bAttr(a, in)
	return nil
}

func (n *bNode) Lookup(ctx context.Context, name string) (bzfs.Node, error) {
	in, err := n.s.vol.Lookup(context.WithoutCancel(ctx), n.ino, name)
	if err != nil {
		return nil, mapErrno(err)
	}
	return n.s.node(in.ID), nil
}

func (n *bNode) ReadDirAll(ctx context.Context) ([]bzfuse.Dirent, error) {
	ents, err := n.s.vol.ReadDir(context.WithoutCancel(ctx), n.ino)
	if err != nil {
		return nil, mapErrno(err)
	}
	up, err := n.s.vol.Parent(context.WithoutCancel(ctx), n.ino)
	if err != nil {
		return nil, mapErrno(err)
	}
	out := make([]bzfuse.Dirent, 0, len(ents)+2)
	out = append(out,
		bzfuse.Dirent{Inode: uint64(n.ino), Name: ".", Type: bzfuse.DT_Dir},
		bzfuse.Dirent{Inode: uint64(up), Name: "..", Type: bzfuse.DT_Dir})
	for _, e := range ents {
		typ := bzfuse.DT_File
		if e.Mode.IsDir() {
			typ = bzfuse.DT_Dir
		} else if e.Mode&os.ModeSymlink != 0 {
			typ = bzfuse.DT_Link
		}
		out = append(out, bzfuse.Dirent{Inode: uint64(e.ID), Name: e.Name, Type: typ})
	}
	return out, nil
}

func (n *bNode) Mkdir(ctx context.Context, req *bzfuse.MkdirRequest) (bzfs.Node, error) {
	in, err := n.s.vol.Mkdir(context.WithoutCancel(ctx), n.ino, req.Name, req.Mode, vfs.Owner(req.Header.Uid, req.Header.Gid))
	if err != nil {
		return nil, mapErrno(err)
	}
	return n.s.node(in.ID), nil
}

func (n *bNode) Create(ctx context.Context, req *bzfuse.CreateRequest, resp *bzfuse.CreateResponse) (bzfs.Node, bzfs.Handle, error) {
	ctx = context.WithoutCancel(ctx)
	in, err := n.s.vol.Create(ctx, n.ino, req.Name, req.Mode, vfs.Owner(req.Header.Uid, req.Header.Gid))
	if errors.Is(err, fs.ErrExist) && req.Flags&bzfuse.OpenFlags(os.O_EXCL) == 0 {
		in, err = n.s.vol.Lookup(ctx, n.ino, req.Name)
		if err == nil && req.Flags&bzfuse.OpenFlags(os.O_TRUNC) != 0 {
			err = n.s.vol.Truncate(ctx, in.ID, 0)
		}
	}
	if err != nil {
		return nil, nil, mapErrno(err)
	}
	f, err := n.s.vol.Open(ctx, in.ID)
	if err != nil {
		return nil, nil, mapErrno(err)
	}
	bAttr(&resp.Attr, in)
	return n.s.node(in.ID), &bHandle{s: n.s, file: f}, nil
}

func (n *bNode) Open(ctx context.Context, req *bzfuse.OpenRequest, _ *bzfuse.OpenResponse) (bzfs.Handle, error) {
	ctx = context.WithoutCancel(ctx)
	in, err := n.s.vol.Stat(ctx, n.ino)
	if err != nil {
		return nil, mapErrno(err)
	}
	if in.IsDir() {
		return n, nil
	}
	if req.Flags&bzfuse.OpenFlags(os.O_TRUNC) != 0 {
		if err := n.s.vol.Truncate(ctx, n.ino, 0); err != nil {
			return nil, mapErrno(err)
		}
	}
	f, err := n.s.vol.Open(ctx, n.ino)
	if err != nil {
		return nil, mapErrno(err)
	}
	return &bHandle{s: n.s, file: f}, nil
}

func (n *bNode) Remove(ctx context.Context, req *bzfuse.RemoveRequest) error {
	return mapErrno(n.s.vol.Remove(context.WithoutCancel(ctx), n.ino, req.Name))
}

func (n *bNode) Rename(ctx context.Context, req *bzfuse.RenameRequest, newDir bzfs.Node) error {
	nd, ok := newDir.(*bNode)
	if !ok {
		return syscall.EXDEV
	}
	return mapErrno(n.s.vol.Rename(context.WithoutCancel(ctx), n.ino, req.OldName, nd.ino, req.NewName, 0))
}

func (n *bNode) Setattr(ctx context.Context, req *bzfuse.SetattrRequest, resp *bzfuse.SetattrResponse) error {
	ctx = context.WithoutCancel(ctx)
	var c vfs.Change
	if req.Valid.Mode() {
		m := fs.FileMode(req.Mode)
		c.Mode = &m
	}
	if req.Valid.Size() {
		c.Size = &req.Size
	}
	now := n.s.vol.Now()
	if req.Valid.AtimeNow() {
		c.Atime = &now
	} else if req.Valid.Atime() {
		c.Atime = &req.Atime
	}
	if req.Valid.MtimeNow() {
		c.Mtime = &now
	} else if req.Valid.Mtime() {
		c.Mtime = &req.Mtime
	}
	in, err := n.s.vol.SetAttr(ctx, n.ino, c)
	if err != nil {
		// Under FUSE_WRITEBACK_CACHE, Linux's syncfs(2) invokes fuse_flush_times
		// (mtime/atime-only FUSE_SETATTR) for recently unlinked inodes that have
		// not yet received FUSE_FORGET. Returning ENOENT poisons syncfs(2).
		if n.s.cfg.WritebackCache && errors.Is(err, fs.ErrNotExist) &&
			c.Mode == nil && c.UID == nil && c.GID == nil {
			n.unlinkedWriteback.Store(true)
			resp.Attr.Inode = uint64(n.ino)
			return nil
		}
		return mapErrno(err)
	}
	bAttr(&resp.Attr, in)
	return nil
}

func (n *bNode) Fsync(ctx context.Context, _ *bzfuse.FsyncRequest) error {
	if n.s.stats != nil {
		n.s.stats.FsyncRPCs.Add(1)
	}
	return mapErrno(n.s.vol.SyncInode(context.WithoutCancel(ctx), n.ino))
}

type bHandle struct {
	s    *bSession
	file *vfs.File
}

func (h *bHandle) Read(_ context.Context, req *bzfuse.ReadRequest, resp *bzfuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := h.file.ReadAt(h.s.base, buf, req.Offset)
	if h.s.stats != nil {
		h.s.stats.ReadRPCs.Add(1)
		h.s.stats.ReadBytes.Add(int64(n))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return mapErrno(err)
	}
	resp.Data = buf[:n]
	return nil
}

func (h *bHandle) Write(ctx context.Context, req *bzfuse.WriteRequest, resp *bzfuse.WriteResponse) error {
	n, err := h.file.WriteAt(context.WithoutCancel(ctx), req.Data, req.Offset)
	if h.s.stats != nil {
		h.s.stats.observeWrite(int64(n))
	}
	if err != nil {
		return mapErrno(err)
	}
	resp.Size = n
	return nil
}

func (h *bHandle) Flush(context.Context, *bzfuse.FlushRequest) error {
	if h.s.stats != nil {
		h.s.stats.FlushRPCs.Add(1)
	}
	return nil
}

func (h *bHandle) Release(context.Context, *bzfuse.ReleaseRequest) error {
	return mapErrno(h.file.Close())
}

func mapErrno(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, fs.ErrExist):
		return syscall.EEXIST
	case errors.Is(err, vfs.ErrNotDir):
		return syscall.ENOTDIR
	case errors.Is(err, vfs.ErrIsDir):
		return syscall.EISDIR
	case errors.Is(err, vfs.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, vfs.ErrNameTooLong):
		return syscall.ENAMETOOLONG
	case errors.Is(err, fs.ErrInvalid):
		return syscall.EINVAL
	case errors.Is(err, fs.ErrPermission):
		return syscall.EPERM
	case errors.Is(err, vfs.ErrTooBig):
		return syscall.EFBIG
	case errors.Is(err, vfs.ErrNoSpace), errors.Is(err, vfs.ErrFull):
		return syscall.ENOSPC
	case errors.Is(err, vfs.ErrTooManyLinks):
		return syscall.EMLINK
	case errors.Is(err, vfs.ErrReadOnly), errors.Is(err, vfs.ErrStaleRound), errors.Is(err, vfs.ErrJournal), errors.Is(err, vfs.ErrClosed):
		return syscall.EROFS
	default:
		return syscall.EIO
	}
}
