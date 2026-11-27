package fuserecovery

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
)

type dirtyWrite struct {
	offset int64
	data   []byte
}

// DaemonConfig configures a FUSE daemon instance.
type DaemonConfig struct {
	SessionID      string
	MountPath      string
	StateStore     *StateStore
	Backing        BackingStore
	Channel        FuseChannel
	CheckpointMode CheckpointMode
}

// RecoveryReport summarizes what a respawned daemon restored on attach.
type RecoveryReport struct {
	SessionID        string
	Generation       uint64
	InodesRestored   int
	HandlesRestored  int
	OrphansRestored  int
	StaleHandles     int
	BarriersVerified int
	ReplayedBlocked  int
	UniqueReqID      uint64
}

// Daemon implements a host-side FUSE filesystem daemon with Ananke-style
// checkpointed session/handle state and crash-consistent fsync barriers.
type Daemon struct {
	mu              sync.Mutex
	store           *StateStore
	backing         BackingStore
	channel         FuseChannel
	mode            CheckpointMode
	state           *SessionState
	pathToInode     map[string]uint64
	openFiles       map[uint64]BackingFile
	pendingWrites   map[uint64][]dirtyWrite
	pendingReceipts map[uint64][]OpReceipt
	crashed         bool
	closed          bool
}

// NewDaemon initializes a fresh Generation-1 FUSE daemon and attaches it to
// cfg.Channel.
func NewDaemon(cfg DaemonConfig) (*Daemon, error) {
	if cfg.Backing == nil {
		return nil, fmt.Errorf("fuserecovery: backing store required")
	}
	if cfg.Channel == nil {
		return nil, fmt.Errorf("fuserecovery: fuse channel required")
	}
	if cfg.CheckpointMode != CheckpointDisabled && cfg.StateStore == nil {
		return nil, fmt.Errorf("fuserecovery: state store required when checkpointing is enabled")
	}
	sessID := cfg.SessionID
	if sessID == "" {
		sessID = cfg.Channel.SessionID()
	}
	mountPath := cfg.MountPath
	if mountPath == "" {
		mountPath = cfg.Channel.MountPath()
	}

	st := NewSessionState(sessID, mountPath)
	if err := cfg.Channel.AttachDaemon(st.Generation); err != nil {
		return nil, err
	}

	d := &Daemon{
		store:           cfg.StateStore,
		backing:         cfg.Backing,
		channel:         cfg.Channel,
		mode:            cfg.CheckpointMode,
		state:           st,
		pathToInode:     make(map[string]uint64),
		openFiles:       make(map[uint64]BackingFile),
		pendingWrites:   make(map[uint64][]dirtyWrite),
		pendingReceipts: make(map[uint64][]OpReceipt),
	}
	if err := d.checkpointLocked(false); err != nil {
		return nil, err
	}
	return d, nil
}

// RecoverDaemon loads the CRC32C-verified SessionState from cfg.StateStore,
// re-attaches to the live supervisor-held FuseChannel without unmounting,
// re-validates active/orphan inode generations and fsync barriers in O(open handles),
// re-binds open handles (including unlinked-but-open orphan files), and replays
// any kernel-queued FUSE requests.
func RecoverDaemon(cfg DaemonConfig) (*Daemon, RecoveryReport, error) {
	var rep RecoveryReport
	if cfg.Backing == nil {
		return nil, rep, fmt.Errorf("fuserecovery: backing store required")
	}
	if cfg.Channel == nil {
		return nil, rep, fmt.Errorf("fuserecovery: fuse channel required")
	}
	if cfg.StateStore == nil {
		return nil, rep, fmt.Errorf("fuserecovery: state store required for recovery")
	}
	if !cfg.Channel.HandoffAvailable() {
		return nil, rep, fmt.Errorf("%w: channel for %q cannot hand off session", ErrHandoffUnavailable, cfg.Channel.MountPath())
	}

	st, err := cfg.StateStore.Load()
	if err != nil {
		return nil, rep, err
	}
	if st.SessionID != cfg.Channel.SessionID() {
		return nil, rep, fmt.Errorf("%w: state session %q does not match live channel %q",
			ErrSessionCorrupt, st.SessionID, cfg.Channel.SessionID())
	}

	st.Generation++
	if err := cfg.Channel.AttachDaemon(st.Generation); err != nil {
		return nil, rep, err
	}

	d := &Daemon{
		store:           cfg.StateStore,
		backing:         cfg.Backing,
		channel:         cfg.Channel,
		mode:            cfg.CheckpointMode,
		state:           st,
		pathToInode:     make(map[string]uint64, len(st.Inodes)),
		openFiles:       make(map[uint64]BackingFile, len(st.Handles)),
		pendingWrites:   make(map[uint64][]dirtyWrite),
		pendingReceipts: make(map[uint64][]OpReceipt),
	}

	// Identify inodes referenced by open handles so recovery work scales with
	// O(open handles + orphans) rather than scanning every closed file on disk.
	activeInodes := make(map[uint64]struct{}, len(st.Handles))
	for _, h := range st.Handles {
		activeInodes[h.Inode] = struct{}{}
	}

	// Rebuild path index in memory and eagerly re-validate active & orphan inodes.
	for _, ino := range st.Inodes {
		if ino.Deleted {
			continue
		}
		_, isActive := activeInodes[ino.Inode]
		if !isActive && !ino.Unlinked {
			if ino.Path != "" {
				d.pathToInode[ino.Path] = ino.Inode
				rep.InodesRestored++
			}
			continue
		}
		targetRel := ino.Path
		if ino.Unlinked {
			targetRel = ino.OrphanRelPath
		}
		if targetRel == "" {
			ino.Deleted = true
			continue
		}
		sz, exists, statErr := cfg.Backing.StatSize(targetRel)
		if statErr != nil || !exists {
			// Backing object disappeared outside daemon control -> ESTALE.
			ino.Deleted = true
			continue
		}
		ino.Size = sz
		if !ino.Unlinked {
			d.pathToInode[ino.Path] = ino.Inode
		} else {
			rep.OrphansRestored++
		}
		rep.InodesRestored++
	}

	// Eagerly re-verify acknowledged fsync barriers for all active/open inodes.
	for inoID := range activeInodes {
		barrier, hasBarrier := st.FsyncBarriers[inoID]
		if !hasBarrier {
			continue
		}
		ino, ok := st.Inodes[inoID]
		if !ok || ino.Deleted || ino.Generation != barrier.InodeGen {
			continue
		}
		targetRel := ino.Path
		if ino.Unlinked {
			targetRel = ino.OrphanRelPath
		}
		raw, rerr := cfg.Backing.ReadAll(targetRel)
		if rerr != nil {
			d.closeAllFilesLocked()
			return nil, rep, fmt.Errorf("verify fsync barrier inode %d: %w", inoID, rerr)
		}
		if uint64(len(raw)) < barrier.DurableSize {
			d.closeAllFilesLocked()
			return nil, rep, fmt.Errorf("%w: inode %d durable size %d < acknowledged fsync size %d",
				ErrSessionCorrupt, inoID, len(raw), barrier.DurableSize)
		}
		if uint64(len(raw)) == barrier.DurableSize && ContentCRC32C(raw) != barrier.ContentCRC32C {
			d.closeAllFilesLocked()
			return nil, rep, fmt.Errorf("%w: inode %d crc mismatch on acknowledged fsync barrier",
				ErrSessionCorrupt, inoID)
		}
		rep.BarriersVerified++
	}

	// Re-bind open file handles to their backing files.
	for fh, h := range st.Handles {
		h.DirtyUnflushed = false // volatile dirty writes from dead daemon are gone
		ino, ok := st.Inodes[h.Inode]
		if !ok || ino.Deleted || ino.Generation != h.InodeGen {
			// Retain handle entry without an open FD so subsequent ops fail
			// deterministically with ErrStaleInode (ESTALE).
			rep.StaleHandles++
			continue
		}
		targetRel := ino.Path
		if ino.Unlinked {
			targetRel = ino.OrphanRelPath
		}
		rw := h.Flags&uint32(os.O_WRONLY|os.O_RDWR) != 0
		bf, oerr := cfg.Backing.OpenFile(targetRel, rw)
		if oerr != nil {
			ino.Deleted = true
			rep.StaleHandles++
			continue
		}
		d.openFiles[fh] = bf
		rep.HandlesRestored++
	}

	// Persist bumped generation checkpoint before serving requests.
	if err := d.checkpointLocked(false); err != nil {
		d.closeAllFilesLocked()
		return nil, rep, err
	}

	// Drain and replay any FUSE requests queued in /dev/fuse while daemon was down.
	blocked := cfg.Channel.DrainBlocked()
	for _, req := range blocked {
		if err := d.replayPending(req); err == nil {
			rep.ReplayedBlocked++
		}
	}

	rep.SessionID = st.SessionID
	rep.Generation = st.Generation
	rep.UniqueReqID = st.UniqueReqID
	return d, rep, nil
}

func (d *Daemon) replayPending(req PendingRequest) error {
	switch req.Op {
	case "create":
		_, _, err := d.Create(req.ReqID, req.Tenant, req.Path, req.Mode, req.Flags)
		return err
	case "write":
		_, _, err := d.Write(req.ReqID, req.Tenant, req.Handle, req.Offset, req.Data)
		return err
	case "fsync":
		_, err := d.Fsync(req.ReqID, req.Tenant, req.Handle)
		return err
	case "unlink":
		return d.Unlink(req.ReqID, req.Tenant, req.Path)
	default:
		return nil
	}
}

// SessionID returns the daemon's mount session ID.
func (d *Daemon) SessionID() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state.SessionID
}

// Generation returns the daemon's restart generation counter.
func (d *Daemon) Generation() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state.Generation
}

// OpenFDCount returns the number of currently open OS backing file descriptors.
func (d *Daemon) OpenFDCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.openFiles)
}

// StateSnapshot returns a deep copy of the current SessionState for inspection.
func (d *Daemon) StateSnapshot() *SessionState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state.Clone()
}

// CrashAbrupt simulates an ungraceful SIGKILL termination of the FUSE daemon:
//   - Discards all un-fsynced volatile write buffers without flushing to disk.
//   - Releases process-local backing file descriptors (kernel reclaims process FDs).
//   - Detaches from the FuseChannel (keeping the mount alive if supervisor holds /dev/fuse).
//   - Leaves the last durable checkpoint untouched on disk.
func (d *Daemon) CrashAbrupt() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.crashed || d.closed {
		return
	}
	d.crashed = true
	d.pendingWrites = nil
	d.pendingReceipts = nil
	d.closeAllFilesLocked()
	d.channel.DetachOnCrash()
}

// Close cleanly shuts down the daemon and closes all open backing file descriptors.
func (d *Daemon) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.closeAllFilesLocked()
	return nil
}

func (d *Daemon) closeAllFilesLocked() {
	for fh, bf := range d.openFiles {
		_ = bf.Close()
		delete(d.openFiles, fh)
	}
}

func (d *Daemon) checkAliveLocked() error {
	if d.crashed || d.closed {
		return ErrDaemonCrashed
	}
	if !d.channel.MountAlive() {
		return ErrHandoffUnavailable
	}
	return nil
}

func normalizeRelPath(p string) (string, error) {
	if p == "" || strings.ContainsRune(p, '\x00') {
		return "", fmt.Errorf("%w: invalid path %q", ErrPathEscape, p)
	}
	if strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("%w: absolute path %q", ErrPathEscape, p)
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return "", fmt.Errorf("%w: parent traversal in %q", ErrPathEscape, p)
		}
	}
	clean := path.Clean(p)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, orphanDirName) {
		return "", fmt.Errorf("%w: disallowed path %q", ErrPathEscape, p)
	}
	return clean, nil
}

func (d *Daemon) checkpointLocked(forceDurable bool) error {
	if d.mode == CheckpointDisabled || d.store == nil {
		return nil
	}
	durable := forceDurable || d.mode == CheckpointSync
	return d.store.Save(d.state, durable)
}

func (d *Daemon) effectiveSizeLocked(ino *InodeRecord) uint64 {
	sz := ino.Size
	for _, w := range d.pendingWrites[ino.Inode] {
		end := uint64(w.offset) + uint64(len(w.data))
		if end > sz {
			sz = end
		}
	}
	return sz
}

// Lookup resolves relPath to an InodeRecord and increments its FUSE nlookup refcount.
func (d *Daemon) Lookup(tenant, relPath string) (InodeRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return InodeRecord{}, err
	}
	clean, err := normalizeRelPath(relPath)
	if err != nil {
		return InodeRecord{}, err
	}
	inoID, ok := d.pathToInode[clean]
	if !ok {
		return InodeRecord{}, ErrNotFound
	}
	ino := d.state.Inodes[inoID]
	if ino == nil || ino.Deleted || ino.Unlinked {
		return InodeRecord{}, ErrNotFound
	}
	if ino.OwnerTenant != tenant {
		return InodeRecord{}, ErrTenantMismatch
	}
	ino.NLookup++
	if err := d.checkpointLocked(false); err != nil {
		return InodeRecord{}, err
	}
	out := *ino
	out.Size = d.effectiveSizeLocked(ino)
	return out, nil
}

// Forget decrements the FUSE kernel nlookup refcount for an inode.
func (d *Daemon) Forget(inode, nlookup uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return err
	}
	ino, ok := d.state.Inodes[inode]
	if !ok {
		return nil
	}
	if nlookup >= ino.NLookup {
		ino.NLookup = 0
	} else {
		ino.NLookup -= nlookup
	}
	return d.checkpointLocked(false)
}

// Stat returns metadata for an inode, verifying generation match and tenant ownership.
func (d *Daemon) Stat(tenant string, inode, expectedGen uint64) (InodeRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return InodeRecord{}, err
	}
	ino, ok := d.state.Inodes[inode]
	if !ok || ino.Deleted || (expectedGen != 0 && ino.Generation != expectedGen) {
		return InodeRecord{}, ErrStaleInode
	}
	if ino.OwnerTenant != tenant {
		return InodeRecord{}, ErrTenantMismatch
	}
	out := *ino
	out.Size = d.effectiveSizeLocked(ino)
	return out, nil
}

// Create creates a new file (or opens existing unless O_EXCL) and returns both
// its InodeRecord and HandleRecord. Non-idempotent creations are deduplicated
// across daemon crashes via reqID.
func (d *Daemon) Create(reqID uint64, tenant, relPath string, mode, flags uint32) (InodeRecord, HandleRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return InodeRecord{}, HandleRecord{}, err
	}

	// Idempotent deduplication across crash/retry.
	if reqID != 0 {
		if rec, ok := d.state.CompletedOps[reqID]; ok && rec.OpType == "create" {
			ino := d.state.Inodes[rec.Inode]
			h := d.state.Handles[rec.Handle]
			if ino != nil && h != nil && ino.OwnerTenant == tenant {
				return *ino, *h, nil
			}
		}
	}

	clean, err := normalizeRelPath(relPath)
	if err != nil {
		return InodeRecord{}, HandleRecord{}, err
	}
	if flags == 0 {
		flags = uint32(os.O_RDWR | os.O_CREATE)
	}

	// Handle existing file according to O_EXCL semantics.
	if existingID, exists := d.pathToInode[clean]; exists {
		ino := d.state.Inodes[existingID]
		if ino != nil && !ino.Deleted && !ino.Unlinked {
			if ino.OwnerTenant != tenant {
				return InodeRecord{}, HandleRecord{}, ErrTenantMismatch
			}
			if flags&uint32(os.O_EXCL) != 0 {
				return InodeRecord{}, HandleRecord{}, ErrAlreadyExists
			}
			h, err := d.openInodeLocked(tenant, ino, flags)
			if err != nil {
				return InodeRecord{}, HandleRecord{}, err
			}
			d.state.RecordOp(OpReceipt{
				ReqID:      reqID,
				OpType:     "create",
				Inode:      ino.Inode,
				InodeGen:   ino.Generation,
				Handle:     h.Handle,
				Generation: d.state.Generation,
			})
			if err := d.checkpointLocked(false); err != nil {
				return InodeRecord{}, HandleRecord{}, err
			}
			return *ino, h, nil
		}
	}

	if err := d.backing.CreateFile(clean, mode); err != nil {
		return InodeRecord{}, HandleRecord{}, err
	}

	inoID := d.state.NextInode
	d.state.NextInode++
	ino := &InodeRecord{
		Inode:       inoID,
		Path:        clean,
		Generation:  d.state.Generation,
		NLookup:     1,
		OwnerTenant: tenant,
		Mode:        mode,
		Size:        0,
	}
	d.state.Inodes[inoID] = ino
	d.pathToInode[clean] = inoID

	h, err := d.openInodeLocked(tenant, ino, flags)
	if err != nil {
		return InodeRecord{}, HandleRecord{}, err
	}

	d.state.RecordOp(OpReceipt{
		ReqID:      reqID,
		OpType:     "create",
		Inode:      ino.Inode,
		InodeGen:   ino.Generation,
		Handle:     h.Handle,
		Generation: d.state.Generation,
	})
	if err := d.checkpointLocked(false); err != nil {
		return InodeRecord{}, HandleRecord{}, err
	}
	return *ino, h, nil
}

// Open opens an existing inode by ID and returns a new HandleRecord.
func (d *Daemon) Open(tenant string, inode, expectedGen uint64, flags uint32) (HandleRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return HandleRecord{}, err
	}
	ino, ok := d.state.Inodes[inode]
	if !ok || ino.Deleted || (expectedGen != 0 && ino.Generation != expectedGen) {
		return HandleRecord{}, ErrStaleInode
	}
	if ino.OwnerTenant != tenant {
		return HandleRecord{}, ErrTenantMismatch
	}
	h, err := d.openInodeLocked(tenant, ino, flags)
	if err != nil {
		return HandleRecord{}, err
	}
	if err := d.checkpointLocked(false); err != nil {
		return HandleRecord{}, err
	}
	return h, nil
}

func (d *Daemon) openInodeLocked(tenant string, ino *InodeRecord, flags uint32) (HandleRecord, error) {
	targetRel := ino.Path
	if ino.Unlinked {
		targetRel = ino.OrphanRelPath
	}
	rw := flags&uint32(os.O_WRONLY|os.O_RDWR) != 0
	bf, err := d.backing.OpenFile(targetRel, rw)
	if err != nil {
		return HandleRecord{}, err
	}
	if rw && flags&uint32(os.O_TRUNC) != 0 {
		if err := bf.Truncate(0); err != nil {
			_ = bf.Close()
			return HandleRecord{}, err
		}
		if err := bf.Sync(); err != nil {
			_ = bf.Close()
			return HandleRecord{}, err
		}
		ino.Size = 0
		delete(d.pendingWrites, ino.Inode)
		delete(d.pendingReceipts, ino.Inode)
	}

	fh := d.state.NextHandle
	d.state.NextHandle++
	h := &HandleRecord{
		Handle:      fh,
		Inode:       ino.Inode,
		InodeGen:    ino.Generation,
		Flags:       flags,
		Offset:      0,
		Generation:  d.state.Generation,
		OwnerTenant: tenant,
	}
	d.state.Handles[fh] = h
	d.openFiles[fh] = bf
	return *h, nil
}

// Write stages bytes into the daemon's write buffer for fh. If the handle was
// opened with O_SYNC, bytes are immediately flushed and checkpointed durably;
// otherwise durability is established on Fsync(). Returns (bytesWritten, deduplicated, err).
func (d *Daemon) Write(reqID uint64, tenant string, fh uint64, offset int64, data []byte) (int, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return 0, false, err
	}

	// Check both durable CompletedOps and in-memory pendingReceipts for idempotent retry.
	if reqID != 0 {
		if rec, ok := d.state.CompletedOps[reqID]; ok && rec.OpType == "write" {
			return rec.BytesWritten, true, nil
		}
		for _, recs := range d.pendingReceipts {
			for _, rec := range recs {
				if rec.ReqID == reqID && rec.OpType == "write" {
					return rec.BytesWritten, true, nil
				}
			}
		}
	}

	h, ok := d.state.Handles[fh]
	if !ok {
		return 0, false, ErrInvalidHandle
	}
	if h.OwnerTenant != tenant {
		return 0, false, ErrTenantMismatch
	}
	if h.Flags&uint32(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, false, ErrReadOnlyHandle
	}
	ino, ok := d.state.Inodes[h.Inode]
	if !ok || ino.Deleted || ino.Generation != h.InodeGen {
		return 0, false, ErrStaleInode
	}
	if _, hasFD := d.openFiles[fh]; !hasFD {
		return 0, false, ErrStaleInode
	}

	writeOff := offset
	if h.Flags&uint32(os.O_APPEND) != 0 {
		writeOff = int64(d.effectiveSizeLocked(ino))
	}
	if writeOff < 0 {
		return 0, false, syscall.EINVAL
	}

	cp := append([]byte(nil), data...)
	d.pendingWrites[ino.Inode] = append(d.pendingWrites[ino.Inode], dirtyWrite{
		offset: writeOff,
		data:   cp,
	})
	h.Offset = writeOff + int64(len(cp))
	h.DirtyUnflushed = true

	receipt := OpReceipt{
		ReqID:        reqID,
		OpType:       "write",
		Inode:        ino.Inode,
		InodeGen:     ino.Generation,
		Handle:       fh,
		BytesWritten: len(cp),
		WriteOffset:  writeOff,
		Generation:   d.state.Generation,
	}
	if reqID != 0 {
		d.pendingReceipts[ino.Inode] = append(d.pendingReceipts[ino.Inode], receipt)
	}

	// If opened with O_SYNC, immediately flush + fsync + durable checkpoint so
	// the write and its idempotency receipt are atomically durable.
	if h.Flags&uint32(syscall.O_SYNC) != 0 {
		if _, err := d.fsyncLocked(0, h, ino); err != nil {
			return 0, false, err
		}
	}

	return len(cp), false, nil
}

// Read returns up to size bytes starting at offset for handle fh, overlaying
// any un-fsynced pending writes for read-your-own-writes consistency.
func (d *Daemon) Read(tenant string, fh uint64, offset int64, size int) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return nil, err
	}
	if offset < 0 || size < 0 {
		return nil, syscall.EINVAL
	}
	h, ok := d.state.Handles[fh]
	if !ok {
		return nil, ErrInvalidHandle
	}
	if h.OwnerTenant != tenant {
		return nil, ErrTenantMismatch
	}
	ino, ok := d.state.Inodes[h.Inode]
	if !ok || ino.Deleted || ino.Generation != h.InodeGen {
		return nil, ErrStaleInode
	}
	bf, hasFD := d.openFiles[fh]
	if !hasFD {
		return nil, ErrStaleInode
	}

	effSize := d.effectiveSizeLocked(ino)
	if uint64(offset) >= effSize || size == 0 {
		return []byte{}, nil
	}
	want := uint64(size)
	if uint64(offset)+want > effSize {
		want = effSize - uint64(offset)
	}
	buf := make([]byte, want)

	// Read durable base bytes from backing file.
	if uint64(offset) < ino.Size {
		n, err := bf.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return nil, err
		}
		_ = n
	}

	// Overlay uncommitted pending writes in chronological order.
	readEnd := offset + int64(want)
	for _, w := range d.pendingWrites[ino.Inode] {
		wStart := w.offset
		wEnd := w.offset + int64(len(w.data))
		if wEnd <= offset || wStart >= readEnd {
			continue
		}
		lo := maxInt64(offset, wStart)
		hi := minInt64(readEnd, wEnd)
		copy(buf[lo-offset:hi-offset], w.data[lo-wStart:hi-wStart])
	}

	h.Offset = offset + int64(len(buf))
	return buf, nil
}

// Fsync flushes all pending writes for fh's inode to the real OS backing file,
// calls BackingFile.Sync(), computes the durable content CRC32C, records an
// FsyncBarrier in SessionState, and durably checkpoints the state file
// (O_SYNC + fsync + atomic rename + dir fsync) BEFORE acknowledging to caller.
func (d *Daemon) Fsync(reqID uint64, tenant string, fh uint64) (FsyncBarrier, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return FsyncBarrier{}, err
	}

	if reqID != 0 {
		if rec, ok := d.state.CompletedOps[reqID]; ok && rec.OpType == "fsync" {
			if b, ok := d.state.FsyncBarriers[rec.Inode]; ok && b.Seq >= rec.FsyncSeq {
				return b, nil
			}
		}
	}

	h, ok := d.state.Handles[fh]
	if !ok {
		return FsyncBarrier{}, ErrInvalidHandle
	}
	if h.OwnerTenant != tenant {
		return FsyncBarrier{}, ErrTenantMismatch
	}
	ino, ok := d.state.Inodes[h.Inode]
	if !ok || ino.Deleted || ino.Generation != h.InodeGen {
		return FsyncBarrier{}, ErrStaleInode
	}
	return d.fsyncLocked(reqID, h, ino)
}

func (d *Daemon) fsyncLocked(reqID uint64, h *HandleRecord, ino *InodeRecord) (FsyncBarrier, error) {
	bf, hasFD := d.openFiles[h.Handle]
	if !hasFD {
		return FsyncBarrier{}, ErrStaleInode
	}

	// If this handle was opened O_RDONLY, open a temporary RW handle only if
	// there are pending writes from another handle on the same inode.
	pending := d.pendingWrites[ino.Inode]
	writer := bf
	var tempWriter BackingFile
	if len(pending) > 0 && h.Flags&uint32(os.O_WRONLY|os.O_RDWR) == 0 {
		targetRel := ino.Path
		if ino.Unlinked {
			targetRel = ino.OrphanRelPath
		}
		var err error
		tempWriter, err = d.backing.OpenFile(targetRel, true)
		if err != nil {
			return FsyncBarrier{}, err
		}
		defer tempWriter.Close()
		writer = tempWriter
	}

	// 1. Flush all pending dirty writes to the real backing file.
	for _, w := range pending {
		if _, err := writer.WriteAt(w.data, w.offset); err != nil {
			return FsyncBarrier{}, fmt.Errorf("flush dirty write inode %d: %w", ino.Inode, err)
		}
	}

	// 2. Durable OS fsync on the backing file.
	if err := writer.Sync(); err != nil {
		return FsyncBarrier{}, fmt.Errorf("fsync backing file inode %d: %w", ino.Inode, err)
	}

	// 3. Compute durable size and Castagnoli CRC32C directly from backing bytes.
	targetRel := ino.Path
	if ino.Unlinked {
		targetRel = ino.OrphanRelPath
	}
	raw, err := d.backing.ReadAll(targetRel)
	if err != nil {
		return FsyncBarrier{}, fmt.Errorf("read back synced inode %d: %w", ino.Inode, err)
	}

	d.state.LastFsyncSeq++
	seq := d.state.LastFsyncSeq
	barrier := FsyncBarrier{
		Seq:           seq,
		Inode:         ino.Inode,
		InodeGen:      ino.Generation,
		DurableSize:   uint64(len(raw)),
		ContentCRC32C: ContentCRC32C(raw),
		AckedAtNano:   nowUnixNano(),
	}
	ino.Size = barrier.DurableSize
	d.state.FsyncBarriers[ino.Inode] = barrier

	// Promote all flushed write receipts into durable CompletedOps.
	for _, rec := range d.pendingReceipts[ino.Inode] {
		d.state.RecordOp(rec)
	}
	if reqID != 0 {
		d.state.RecordOp(OpReceipt{
			ReqID:      reqID,
			OpType:     "fsync",
			Inode:      ino.Inode,
			InodeGen:   ino.Generation,
			Handle:     h.Handle,
			FsyncSeq:   seq,
			Generation: d.state.Generation,
		})
	}

	// Update all handles pointing to this inode.
	for _, other := range d.state.Handles {
		if other.Inode == ino.Inode && other.InodeGen == ino.Generation {
			other.DirtyUnflushed = false
			other.LastFsyncSeq = seq
		}
	}

	// 4. Checkpoint SessionState BEFORE acknowledging fsync to caller.
	// File data is already durable on disk via writer.Sync(); CheckpointSync
	// additionally fsyncs the state file and parent dir, while CheckpointBuffered
	// relies on atomic POSIX rename in host OS page cache (survives SIGKILL).
	if err := d.checkpointLocked(false); err != nil {
		return FsyncBarrier{}, fmt.Errorf("checkpoint fsync barrier: %w", err)
	}

	// Clear volatile buffers only after checkpoint succeeded.
	delete(d.pendingWrites, ino.Inode)
	delete(d.pendingReceipts, ino.Inode)
	return barrier, nil
}

// Unlink removes relPath from the namespace. If open file handles still
// reference the inode, the backing file is atomically stashed in
// .ananke-orphans/ so existing handles continue to function across daemon
// restarts until the final Release() cleans up the orphan.
func (d *Daemon) Unlink(reqID uint64, tenant, relPath string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return err
	}

	if reqID != 0 {
		if rec, ok := d.state.CompletedOps[reqID]; ok && rec.OpType == "unlink" {
			return nil
		}
	}

	clean, err := normalizeRelPath(relPath)
	if err != nil {
		return err
	}
	inoID, ok := d.pathToInode[clean]
	if !ok {
		return ErrNotFound
	}
	ino := d.state.Inodes[inoID]
	if ino == nil || ino.Deleted || ino.Unlinked {
		return ErrNotFound
	}
	if ino.OwnerTenant != tenant {
		return ErrTenantMismatch
	}

	openCount := d.openHandleCountLocked(inoID, ino.Generation)
	if openCount > 0 {
		orphanRel, err := d.backing.StashOrphan(ino.Path, ino.Inode, ino.Generation)
		if err != nil {
			return err
		}
		delete(d.pathToInode, ino.Path)
		ino.Path = ""
		ino.OrphanRelPath = orphanRel
		ino.Unlinked = true
	} else {
		if err := d.backing.UnlinkFile(ino.Path); err != nil {
			return err
		}
		delete(d.pathToInode, ino.Path)
		ino.Path = ""
		ino.Deleted = true
		delete(d.pendingWrites, ino.Inode)
		delete(d.pendingReceipts, ino.Inode)
	}

	d.state.RecordOp(OpReceipt{
		ReqID:      reqID,
		OpType:     "unlink",
		Inode:      ino.Inode,
		InodeGen:   ino.Generation,
		Generation: d.state.Generation,
	})
	return d.checkpointLocked(false)
}

// Release closes an open handle fh. If fh was the last open handle to an
// unlinked (orphan) inode, the stashed orphan backing file is removed and the
// inode is marked deleted.
func (d *Daemon) Release(tenant string, fh uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkAliveLocked(); err != nil {
		return err
	}
	h, ok := d.state.Handles[fh]
	if !ok {
		return ErrInvalidHandle
	}
	if h.OwnerTenant != tenant {
		return ErrTenantMismatch
	}

	if bf, exists := d.openFiles[fh]; exists {
		_ = bf.Close()
		delete(d.openFiles, fh)
	}
	delete(d.state.Handles, fh)

	if ino, exists := d.state.Inodes[h.Inode]; exists && ino.Generation == h.InodeGen {
		if ino.Unlinked && !ino.Deleted && d.openHandleCountLocked(ino.Inode, ino.Generation) == 0 {
			if ino.OrphanRelPath != "" {
				_ = d.backing.CleanupOrphan(ino.OrphanRelPath)
				ino.OrphanRelPath = ""
			}
			ino.Deleted = true
			delete(d.pendingWrites, ino.Inode)
			delete(d.pendingReceipts, ino.Inode)
		}
	}

	return d.checkpointLocked(false)
}

func (d *Daemon) openHandleCountLocked(inode, gen uint64) int {
	n := 0
	for _, h := range d.state.Handles {
		if h.Inode == inode && h.InodeGen == gen {
			n++
		}
	}
	return n
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
