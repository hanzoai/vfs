// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package visorfuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/zeebo/blake3"
)

// SavedHandle captures a live FUSE wire handle ID and its bound VFS inode ID
// at ReleaseRound time so RestoreToNode can reconstruct the exact handle table
// and allocator on the destination node before resuming the guest.
type SavedHandle struct {
	Handle uint64 `json:"handle"`
	Inode  uint64 `json:"inode"`
}

// QuiesceReceipt records the state of the external VFS volume immediately after
// FlushAndQuiesce completes and publishes all dirty data to object storage.
type QuiesceReceipt struct {
	TenantID        string        `json:"tenant_id"`
	VolumeID        string        `json:"volume_id"`
	Round           uint64        `json:"round"`
	HeadSeq         uint64        `json:"head_seq"`
	PublishedSeq    uint64        `json:"published_journal_seq"`
	PublishDuration time.Duration `json:"publish_duration_ns"`
	QuiescedAtUnix  int64         `json:"quiesced_at_unix_ns"`
}

// VolumeReleaseToken is the signed-over proof emitted by ReleaseRound showing
// that the external VFS volume was flushed, quiesced, and cleanly closed on the
// source node with zero unpublished dirty data and zero synchronous S3 requests
// during the clean close step.
type VolumeReleaseToken struct {
	TenantID             string        `json:"tenant_id"`
	VolumeID             string        `json:"volume_id"`
	BackendDesc          string        `json:"backend_desc"`
	ReleasedRound        uint64        `json:"released_round"`
	NextRound            uint64        `json:"next_round"`
	PublishedHeadSeq     uint64        `json:"published_head_seq"`
	PublishedJournalSeq  uint64        `json:"published_journal_seq"`
	PublishDuration      time.Duration `json:"publish_duration_ns"`
	CleanCloseS3Requests int64         `json:"clean_close_s3_requests"`
	CleanQuiesce         bool          `json:"clean_quiesce"`
	TokenDigest          string        `json:"token_digest"`
}

func (t *VolumeReleaseToken) computeDigest() string {
	cp := *t
	cp.TokenDigest = ""
	b, _ := json.Marshal(cp)
	sum := blake3.Sum256(b)
	return fmt.Sprintf("%x", sum)
}

// Verify checks the self-consistency and digest of a VolumeReleaseToken.
func (t VolumeReleaseToken) Verify() error {
	if !t.CleanQuiesce {
		return ErrUnquiescedVolume
	}
	if t.ReleasedRound == 0 || t.NextRound <= t.ReleasedRound {
		return ErrVolumeNotReleased
	}
	want := t.computeDigest()
	if t.TokenDigest == "" || t.TokenDigest != want {
		return fmt.Errorf("%w: volume release token digest mismatch", ErrCheckpointTampered)
	}
	return nil
}

// ServerMetrics snapshots live FUSE operation and lifecycle telemetry.
type ServerMetrics struct {
	InitCalls          int64
	LookupCalls        int64
	GetattrCalls       int64
	CreateCalls        int64
	MkdirCalls         int64
	UnlinkCalls        int64
	OpenCalls          int64
	ReadCalls          int64
	WriteCalls         int64
	FsyncCalls         int64
	FlushCalls         int64
	ReleaseCalls       int64
	StatfsCalls        int64
	BytesRead          int64
	BytesWritten       int64
	OpenHandles        int
	Quiesced           bool
	RoundReleased      bool
	DirtyWritesPending int64
}

type openHandle struct {
	id   uint64
	ino  vfs.InodeID
	file *vfs.File
}

// ExternalFUSEServer serves a *vfs.Volume directly over a host-authenticated
// socket FD with zero host-side gofer hop and zero guest privileges.
type ExternalFUSEServer struct {
	spec    MountSpec
	vol     *vfs.Volume
	counter *CountingBackend // optional; measures exact S3 requests on close/fsync
	conn    *FrameConn

	ioMu sync.RWMutex // RLock for FUSE ops; Lock for FlushAndQuiesce / ReleaseRound

	stateMu         sync.Mutex
	quiesced        bool
	released        bool
	closed          bool
	dirtySinceQ     int64
	quiesceRec      QuiesceReceipt
	releaseToken    VolumeReleaseToken
	nextHandle      uint64
	handles         map[uint64]*openHandle
	savedHandles    []SavedHandle
	savedNextHandle uint64
	workerSem       chan struct{}
	wg              sync.WaitGroup
	initCompleted   atomic.Bool

	// Operation counters.
	initCalls    atomic.Int64
	lookupCalls  atomic.Int64
	getattrCalls atomic.Int64
	createCalls  atomic.Int64
	mkdirCalls   atomic.Int64
	unlinkCalls  atomic.Int64
	openCalls    atomic.Int64
	readCalls    atomic.Int64
	writeCalls   atomic.Int64
	fsyncCalls   atomic.Int64
	flushCalls   atomic.Int64
	releaseCalls atomic.Int64
	statfsCalls  atomic.Int64
	bytesRead    atomic.Int64
	bytesWritten atomic.Int64
}

// ServerOption configures optional telemetry or concurrency bounds on ExternalFUSEServer.
type ServerOption func(*ExternalFUSEServer)

// WithCountingBackend attaches a CountingBackend so ReleaseRound records exact
// CleanCloseS3Requests without guessing.
func WithCountingBackend(cb *CountingBackend) ServerOption {
	return func(s *ExternalFUSEServer) {
		s.counter = cb
	}
}

// NewExternalFUSEServer validates spec fail-closed and binds vol directly to
// hostConn (the host end of the pre-authenticated socket FD pair).
func NewExternalFUSEServer(spec MountSpec, vol *vfs.Volume, hostConn io.ReadWriteCloser, opts ...ServerOption) (*ExternalFUSEServer, error) {
	if err := ValidateMountSpec(spec); err != nil {
		return nil, err
	}
	if vol == nil {
		return nil, fmt.Errorf("%w: volume is required", ErrInvalidMountConfig)
	}
	if hostConn == nil {
		return nil, fmt.Errorf("%w: host socket connection is required", ErrInvalidMountConfig)
	}
	s := &ExternalFUSEServer{
		spec:       spec,
		vol:        vol,
		conn:       NewFrameConn(hostConn, spec.SocketKind),
		nextHandle: 1,
		handles:    make(map[uint64]*openHandle),
		workerSem:  make(chan struct{}, 32),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s, nil
}

// Spec returns the validated OCI MountSpec bound to this server.
func (s *ExternalFUSEServer) Spec() MountSpec { return s.spec }

// Volume returns the underlying *vfs.Volume.
func (s *ExternalFUSEServer) Volume() *vfs.Volume { return s.vol }

// IsQuiesced reports whether FlushAndQuiesce has completed with zero subsequent
// dirty writes.
func (s *ExternalFUSEServer) IsQuiesced() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.quiesced && s.dirtySinceQ == 0
}

// HasReleasedRound reports whether ReleaseRound has cleanly released the VFS
// publication round.
func (s *ExternalFUSEServer) HasReleasedRound() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.released
}

// ReleaseToken returns the emitted VolumeReleaseToken if ReleaseRound has run.
func (s *ExternalFUSEServer) ReleaseToken() (VolumeReleaseToken, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.released {
		return VolumeReleaseToken{}, false
	}
	return s.releaseToken, true
}

// SavedHandles returns the open FUSE wire handle bindings and next-handle
// allocator watermark captured at ReleaseRound (or currently live if not yet
// released), sorted deterministically by Handle ID.
func (s *ExternalFUSEServer) SavedHandles() ([]SavedHandle, uint64) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.released {
		out := append([]SavedHandle(nil), s.savedHandles...)
		return out, s.savedNextHandle
	}
	out := make([]SavedHandle, 0, len(s.handles))
	for id, h := range s.handles {
		out = append(out, SavedHandle{Handle: id, Inode: uint64(h.ino)})
	}
	slices.SortFunc(out, func(a, b SavedHandle) int {
		if a.Handle < b.Handle {
			return -1
		}
		if a.Handle > b.Handle {
			return 1
		}
		return 0
	})
	return out, s.nextHandle
}

// RestoreHandles rebinds saved FUSE wire handle IDs to newly opened *vfs.File
// objects on the restored volume and advances the handle allocator strictly
// past both nextHandle and every restored wire handle ID so resumed guests can
// continue READ/WRITE/FSYNC/RELEASE on original handle IDs without collisions.
func (s *ExternalFUSEServer) RestoreHandles(ctx context.Context, saved []SavedHandle, nextHandle uint64) error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	opened := make([]*openHandle, 0, len(saved))
	seen := make(map[uint64]struct{}, len(saved))
	maxID := uint64(0)

	cleanup := func() {
		for _, h := range opened {
			_ = h.file.Close()
		}
	}

	for _, sh := range saved {
		if sh.Handle == 0 || sh.Inode == 0 {
			cleanup()
			return fmt.Errorf("%w: invalid saved handle (%d, inode %d)", ErrCheckpointTampered, sh.Handle, sh.Inode)
		}
		if _, dup := seen[sh.Handle]; dup {
			cleanup()
			return fmt.Errorf("%w: duplicate saved handle %d", ErrCheckpointTampered, sh.Handle)
		}
		seen[sh.Handle] = struct{}{}
		f, err := s.vol.Open(ctx, vfs.InodeID(sh.Inode))
		if err != nil {
			cleanup()
			return fmt.Errorf("visorfuse: restore FUSE handle %d (inode %d): %w", sh.Handle, sh.Inode, err)
		}
		opened = append(opened, &openHandle{id: sh.Handle, ino: vfs.InodeID(sh.Inode), file: f})
		if sh.Handle > maxID {
			maxID = sh.Handle
		}
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	for _, h := range opened {
		if _, exists := s.handles[h.id]; exists {
			for _, oh := range opened {
				if cur := s.handles[oh.id]; cur != oh {
					_ = oh.file.Close()
				}
			}
			return fmt.Errorf("%w: saved handle %d collides with existing handle", ErrCheckpointTampered, h.id)
		}
		s.handles[h.id] = h
	}
	if maxID+1 > s.nextHandle {
		s.nextHandle = maxID + 1
	}
	if nextHandle > s.nextHandle {
		s.nextHandle = nextHandle
	}
	return nil
}

// Metrics returns a consistent snapshot of FUSE operation and lifecycle stats.
func (s *ExternalFUSEServer) Metrics() ServerMetrics {
	s.stateMu.Lock()
	openN := len(s.handles)
	q := s.quiesced
	r := s.released
	dirty := s.dirtySinceQ
	s.stateMu.Unlock()
	return ServerMetrics{
		InitCalls:          s.initCalls.Load(),
		LookupCalls:        s.lookupCalls.Load(),
		GetattrCalls:       s.getattrCalls.Load(),
		CreateCalls:        s.createCalls.Load(),
		MkdirCalls:         s.mkdirCalls.Load(),
		UnlinkCalls:        s.unlinkCalls.Load(),
		OpenCalls:          s.openCalls.Load(),
		ReadCalls:          s.readCalls.Load(),
		WriteCalls:         s.writeCalls.Load(),
		FsyncCalls:         s.fsyncCalls.Load(),
		FlushCalls:         s.flushCalls.Load(),
		ReleaseCalls:       s.releaseCalls.Load(),
		StatfsCalls:        s.statfsCalls.Load(),
		BytesRead:          s.bytesRead.Load(),
		BytesWritten:       s.bytesWritten.Load(),
		OpenHandles:        openN,
		Quiesced:           q,
		RoundReleased:      r,
		DirtyWritesPending: dirty,
	}
}

// Serve processes binary FUSE frames from the guest over the host socket FD
// until ctx is cancelled, the socket closes, or Close() is called.
func (s *ExternalFUSEServer) Serve(ctx context.Context) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = s.conn.Close()
		case <-done:
		}
	}()

	for {
		req, err := s.conn.ReadFrame()
		if err != nil {
			s.wg.Wait()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || ctx.Err() != nil {
				return nil
			}
			s.stateMu.Lock()
			closed := s.closed
			s.stateMu.Unlock()
			if closed {
				return nil
			}
			return err
		}

		select {
		case s.workerSem <- struct{}{}:
		case <-ctx.Done():
			s.wg.Wait()
			return nil
		}
		s.wg.Add(1)
		go func(f *Frame) {
			defer s.wg.Done()
			defer func() { <-s.workerSem }()
			resp := s.handleFrame(ctx, f)
			_ = s.conn.WriteFrame(resp)
		}(req)
	}
}

// Close closes the host socket transport and drains all in-flight FUSE workers.
// It does NOT make any synchronous S3 requests.
func (s *ExternalFUSEServer) Close() error {
	s.stateMu.Lock()
	if s.closed {
		s.stateMu.Unlock()
		return nil
	}
	s.closed = true
	s.stateMu.Unlock()

	err := s.conn.Close()
	s.wg.Wait()

	s.stateMu.Lock()
	for id, h := range s.handles {
		_ = h.file.Close()
		delete(s.handles, id)
	}
	s.stateMu.Unlock()
	return err
}

// FlushAndQuiesce implements Step 1 of the VFS-outside-checkpoint protocol:
//   - drains in-flight FUSE mutations and blocks new writes,
//   - calls vol.Sync(ctx) and vol.Flush(ctx) so every dirty page and journal
//     entry is durably published to object storage,
//   - verifies zero pending bytes / unpublished backlog remain.
func (s *ExternalFUSEServer) FlushAndQuiesce(ctx context.Context) (QuiesceReceipt, error) {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	s.stateMu.Lock()
	if s.released {
		rec := s.quiesceRec
		s.stateMu.Unlock()
		return rec, nil
	}
	s.quiesced = true
	s.stateMu.Unlock()

	t0 := time.Now()
	if err := s.vol.Sync(ctx); err != nil {
		s.unquiesceOnFailure()
		return QuiesceReceipt{}, fmt.Errorf("visorfuse: quiesce sync: %w", err)
	}
	if err := s.vol.Flush(ctx); err != nil {
		s.unquiesceOnFailure()
		return QuiesceReceipt{}, fmt.Errorf("visorfuse: quiesce flush: %w", err)
	}
	pubDur := time.Since(t0)

	st := s.vol.State()
	usage := s.vol.Usage()
	stats := s.vol.Stats()
	if st.Visible != st.Published || st.Backlog != 0 || usage.Pending != 0 {
		s.unquiesceOnFailure()
		return QuiesceReceipt{}, fmt.Errorf("%w: visible=%d published=%d backlog=%d pending=%d",
			ErrUnquiescedVolume, st.Visible, st.Published, st.Backlog, usage.Pending)
	}

	rec := QuiesceReceipt{
		TenantID:        s.spec.TenantID,
		VolumeID:        s.spec.VolumeID,
		Round:           stats.Round,
		HeadSeq:         stats.Seq,
		PublishedSeq:    st.Published,
		PublishDuration: pubDur,
		QuiescedAtUnix:  time.Now().UTC().UnixNano(),
	}
	s.stateMu.Lock()
	s.dirtySinceQ = 0
	s.quiesceRec = rec
	s.stateMu.Unlock()
	return rec, nil
}

func (s *ExternalFUSEServer) unquiesceOnFailure() {
	s.stateMu.Lock()
	if !s.released {
		s.quiesced = false
	}
	s.stateMu.Unlock()
}

// ReleaseRound implements Step 2 of the VFS-outside-checkpoint protocol:
//   - verifies the volume is cleanly quiesced and flushed (fails closed with
//     ErrUnquiescedVolume otherwise),
//   - snapshots all live open FUSE handle IDs and closes them locally (zero
//     synchronous S3 requests),
//   - cleanly closes *vfs.Volume so its journal retires locally with zero
//     synchronous S3 requests, releasing ownership so another node may Attach at
//     NextRound = ReleasedRound + 1.
func (s *ExternalFUSEServer) ReleaseRound(ctx context.Context) (VolumeReleaseToken, error) {
	if err := ctx.Err(); err != nil {
		return VolumeReleaseToken{}, err
	}
	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	s.stateMu.Lock()
	if s.released {
		tok := s.releaseToken
		s.stateMu.Unlock()
		return tok, nil
	}
	if !s.quiesced || s.dirtySinceQ > 0 {
		s.stateMu.Unlock()
		return VolumeReleaseToken{}, ErrUnquiescedVolume
	}
	rec := s.quiesceRec
	s.stateMu.Unlock()

	st := s.vol.State()
	usage := s.vol.Usage()
	stats := s.vol.Stats()
	if st.Visible != st.Published || st.Backlog != 0 || usage.Pending != 0 {
		return VolumeReleaseToken{}, fmt.Errorf("%w: dirty state detected before release", ErrUnquiescedVolume)
	}

	var beforeS3 int64
	if s.counter != nil {
		beforeS3 = s.counter.Snapshot().TotalRequests()
	}

	// Snapshot open wire handle IDs deterministically before closing handles
	// locally (zero S3 requests) so RestoreToNode can rebind them identically.
	s.stateMu.Lock()
	saved := make([]SavedHandle, 0, len(s.handles))
	for id, h := range s.handles {
		saved = append(saved, SavedHandle{Handle: id, Inode: uint64(h.ino)})
		_ = h.file.Close()
		delete(s.handles, id)
	}
	slices.SortFunc(saved, func(a, b SavedHandle) int {
		if a.Handle < b.Handle {
			return -1
		}
		if a.Handle > b.Handle {
			return 1
		}
		return 0
	})
	savedNext := s.nextHandle
	s.stateMu.Unlock()

	// Cleanly close the Volume: stops publisher, closes and retires empty
	// journal locally without issuing any synchronous S3 request.
	if err := s.vol.Close(); err != nil {
		return VolumeReleaseToken{}, fmt.Errorf("visorfuse: release round close volume: %w", err)
	}

	var closeS3 int64
	if s.counter != nil {
		closeS3 = s.counter.Snapshot().TotalRequests() - beforeS3
	}

	tok := VolumeReleaseToken{
		TenantID:             s.spec.TenantID,
		VolumeID:             s.spec.VolumeID,
		BackendDesc:          stats.Backend,
		ReleasedRound:        rec.Round,
		NextRound:            rec.Round + 1,
		PublishedHeadSeq:     rec.HeadSeq,
		PublishedJournalSeq:  rec.PublishedSeq,
		PublishDuration:      rec.PublishDuration,
		CleanCloseS3Requests: closeS3,
		CleanQuiesce:         true,
	}
	tok.TokenDigest = tok.computeDigest()

	s.stateMu.Lock()
	s.released = true
	s.releaseToken = tok
	s.savedHandles = saved
	s.savedNextHandle = savedNext
	s.stateMu.Unlock()
	return tok, nil
}

func (s *ExternalFUSEServer) handleFrame(ctx context.Context, req *Frame) *Frame {
	resp := &Frame{
		Opcode: req.Opcode,
		Unique: req.Unique,
		NodeID: req.NodeID,
		Handle: req.Handle,
	}

	s.ioMu.RLock()
	defer s.ioMu.RUnlock()

	switch req.Opcode {
	case OpInit:
		s.initCalls.Add(1)
		in, err := decodeInitReq(req.Payload)
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		if in.TenantID != s.spec.TenantID {
			resp.Errno = mapErrno(ErrTenantMismatch)
			return resp
		}
		if in.VolumeID != s.spec.VolumeID {
			resp.Errno = mapErrno(ErrVolumeMismatch)
			return resp
		}
		if in.Flags&InitFlagNoGuestPriv == 0 || in.Flags&InitFlagNoGoferHop == 0 {
			resp.Errno = mapErrno(ErrGuestPrivilegeForbidden)
			return resp
		}
		st := s.vol.Stats()
		s.initCompleted.Store(true)
		resp.Payload = encodeInitResp(InitResponse{
			Major:        FUSEProtocolMajor,
			Minor:        FUSEProtocolMinor,
			MaxReadAhead: min(in.MaxReadAhead, s.spec.effectiveMaxReadAhead()),
			MaxWrite:     s.spec.effectiveMaxWrite(),
			Flags:        InitFlagNoGuestPriv | InitFlagNoGoferHop | InitFlagAsyncRead | InitFlagBigWrites,
			Round:        st.Round,
			HeadSeq:      st.Seq,
		})
		return resp

	case OpLookup:
		s.lookupCalls.Add(1)
		name := string(req.Payload)
		in, err := s.vol.Lookup(ctx, vfs.InodeID(req.NodeID), name)
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		resp.NodeID = uint64(in.ID)
		resp.Payload = encodeAttr(attrFromInode(in))
		return resp

	case OpGetattr:
		s.getattrCalls.Add(1)
		in, err := s.vol.Stat(ctx, vfs.InodeID(req.NodeID))
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		resp.Payload = encodeAttr(attrFromInode(in))
		return resp

	case OpCreate:
		s.createCalls.Add(1)
		if err := s.requireMutable(); err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		name := string(req.Payload)
		perm := fs.FileMode(req.Aux) & fs.ModePerm
		if perm == 0 {
			perm = 0o644
		}
		in, err := s.vol.Create(ctx, vfs.InodeID(req.NodeID), name, perm)
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		f, err := s.vol.Open(ctx, in.ID)
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		hid := s.registerHandle(in.ID, f)
		s.markDirtyWrite()
		resp.NodeID = uint64(in.ID)
		resp.Handle = hid
		resp.Payload = encodeAttr(attrFromInode(in))
		return resp

	case OpMkdir:
		s.mkdirCalls.Add(1)
		if err := s.requireMutable(); err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		name := string(req.Payload)
		perm := fs.FileMode(req.Aux) & fs.ModePerm
		if perm == 0 {
			perm = 0o755
		}
		in, err := s.vol.Mkdir(ctx, vfs.InodeID(req.NodeID), name, perm)
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		s.markDirtyWrite()
		resp.NodeID = uint64(in.ID)
		resp.Payload = encodeAttr(attrFromInode(in))
		return resp

	case OpUnlink:
		s.unlinkCalls.Add(1)
		if err := s.requireMutable(); err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		if err := s.vol.Remove(ctx, vfs.InodeID(req.NodeID), string(req.Payload)); err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		s.markDirtyWrite()
		return resp

	case OpOpen:
		s.openCalls.Add(1)
		f, err := s.vol.Open(ctx, vfs.InodeID(req.NodeID))
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		resp.Handle = s.registerHandle(vfs.InodeID(req.NodeID), f)
		return resp

	case OpRead:
		s.readCalls.Add(1)
		h := s.getHandle(req.Handle)
		if h == nil {
			resp.Errno = mapErrno(fs.ErrInvalid)
			return resp
		}
		off, size, err := decodeReadPayload(req.Payload)
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		if size > MaxAllowedWrite {
			size = MaxAllowedWrite
		}
		buf := make([]byte, size)
		n, err := h.file.ReadAt(ctx, buf, int64(off))
		if err != nil && !errors.Is(err, io.EOF) {
			resp.Errno = mapErrno(err)
			return resp
		}
		s.bytesRead.Add(int64(n))
		resp.Payload = buf[:n]
		resp.Aux = uint64(n)
		return resp

	case OpWrite:
		s.writeCalls.Add(1)
		if err := s.requireMutable(); err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		h := s.getHandle(req.Handle)
		if h == nil {
			resp.Errno = mapErrno(fs.ErrInvalid)
			return resp
		}
		if uint32(len(req.Payload)) > s.spec.effectiveMaxWrite() {
			resp.Errno = mapErrno(fs.ErrInvalid)
			return resp
		}
		n, err := h.file.WriteAt(ctx, req.Payload, int64(req.Aux))
		if err != nil {
			resp.Errno = mapErrno(err)
			return resp
		}
		s.markDirtyWrite()
		s.bytesWritten.Add(int64(n))
		resp.Aux = uint64(n)
		return resp

	case OpFsync:
		s.fsyncCalls.Add(1)
		if h := s.getHandle(req.Handle); h != nil {
			if err := h.file.Sync(ctx); err != nil {
				resp.Errno = mapErrno(err)
			}
			return resp
		}
		if req.NodeID != 0 {
			if err := s.vol.SyncInode(ctx, vfs.InodeID(req.NodeID)); err != nil {
				resp.Errno = mapErrno(err)
			}
			return resp
		}
		if err := s.vol.Sync(ctx); err != nil {
			resp.Errno = mapErrno(err)
		}
		return resp

	case OpFlush:
		// Clean FUSE_FLUSH validates handle state locally without issuing any
		// synchronous S3 request (as required by hanzoai/vfs#31: zero
		// synchronous S3 requests on a clean close).
		s.flushCalls.Add(1)
		if req.Handle != 0 && s.getHandle(req.Handle) == nil {
			resp.Errno = mapErrno(fs.ErrInvalid)
		}
		return resp

	case OpRelease:
		// Clean FUSE_RELEASE closes only the in-memory *vfs.File handle (zero
		// synchronous S3 requests).
		s.releaseCalls.Add(1)
		h := s.removeHandle(req.Handle)
		if h == nil {
			resp.Errno = mapErrno(fs.ErrInvalid)
			return resp
		}
		if err := h.file.Close(); err != nil {
			resp.Errno = mapErrno(err)
		}
		return resp

	case OpStatfs:
		s.statfsCalls.Add(1)
		u := s.vol.Usage()
		const bsize = 4096
		totalBytes := uint64(u.Capacity)
		if u.Capacity <= 0 {
			totalBytes = max(u.Bytes, 1<<30)
		}
		usedBlocks := (u.Bytes + bsize - 1) / bsize
		totalBlocks := (totalBytes + bsize - 1) / bsize
		var freeBlocks uint64
		if totalBlocks > usedBlocks {
			freeBlocks = totalBlocks - usedBlocks
		}
		resp.Payload = encodeStatfs(StatfsInfo{
			Blocks:  totalBlocks,
			Bfree:   freeBlocks,
			Bavail:  freeBlocks,
			Files:   u.Files,
			Ffree:   1 << 20,
			Bsize:   bsize,
			Namelen: 255,
		})
		return resp

	default:
		resp.Errno = mapErrno(ErrUnsupportedOpcode)
		return resp
	}
}

func (s *ExternalFUSEServer) requireMutable() error {
	if s.spec.ReadOnly {
		return vfs.ErrReadOnly
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.quiesced || s.released {
		return ErrUnquiescedVolume
	}
	return nil
}

func (s *ExternalFUSEServer) markDirtyWrite() {
	s.stateMu.Lock()
	s.dirtySinceQ++
	s.stateMu.Unlock()
}

func (s *ExternalFUSEServer) registerHandle(ino vfs.InodeID, f *vfs.File) uint64 {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	id := s.nextHandle
	s.nextHandle++
	s.handles[id] = &openHandle{id: id, ino: ino, file: f}
	return id
}

func (s *ExternalFUSEServer) getHandle(id uint64) *openHandle {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.handles[id]
}

func (s *ExternalFUSEServer) removeHandle(id uint64) *openHandle {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	h := s.handles[id]
	delete(s.handles, id)
	return h
}

// GoferHopProxy models the traditional Visor/gVisor host-side gofer architecture
// as an honest A/B baseline: every guest FUSE frame traverses an extra host
// Unix socket hop (sentry <-> gofer <-> VFS daemon) with extra marshaling,
// buffer copy, and optional per-op synchronous dispatch overhead.
type GoferHopProxy struct {
	guestSide  *FrameConn
	serverSide *FrameConn
	syncFlush  bool // models synchronous gofer journal/flush serialization when enabled
	hopCount   atomic.Int64
	bytesHop   atomic.Int64
	mu         sync.Mutex
	closed     bool
}

// NewGoferHopProxy connects guestEnd (from the sandbox) to serverEnd (to the
// VFS server) through an explicit intermediate host gofer relay hop.
func NewGoferHopProxy(guestEnd, serverEnd io.ReadWriteCloser, kind SocketKind, syncFlush bool) *GoferHopProxy {
	return &GoferHopProxy{
		guestSide:  NewFrameConn(guestEnd, kind),
		serverSide: NewFrameConn(serverEnd, kind),
		syncFlush:  syncFlush,
	}
}

// Hops returns the total number of frames relayed through the gofer hop.
func (g *GoferHopProxy) Hops() int64 { return g.hopCount.Load() }

// BytesRelayed returns total payload bytes copied through the gofer hop.
func (g *GoferHopProxy) BytesRelayed() int64 { return g.bytesHop.Load() }

// Close terminates both sockets of the gofer relay.
func (g *GoferHopProxy) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.mu.Unlock()
	return errors.Join(g.guestSide.Close(), g.serverSide.Close())
}

// Serve runs the bidirectional gofer relay loop until ctx cancels or a socket closes.
func (g *GoferHopProxy) Serve(ctx context.Context) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = g.Close()
		case <-done:
		}
	}()

	errCh := make(chan error, 2)
	var writeIDs sync.Map // map[uint64]uint64 (unique -> handle)
	var syncUnique atomic.Uint64
	syncUnique.Store(1 << 60)

	// Guest -> Gofer -> Server
	go func() {
		for {
			f, err := g.guestSide.ReadFrame()
			if err != nil {
				errCh <- err
				return
			}
			// Simulate gofer-side copy + LISAFS/9P RPC re-framing buffer copy.
			if len(f.Payload) > 0 {
				dup := make([]byte, len(f.Payload))
				copy(dup, f.Payload)
				f.Payload = dup
				g.bytesHop.Add(int64(len(dup)))
			}
			if g.syncFlush && f.Opcode == OpWrite {
				writeIDs.Store(f.Unique, f.Handle)
			}
			g.hopCount.Add(1)
			if err := g.serverSide.WriteFrame(f); err != nil {
				errCh <- err
				return
			}
		}
	}()
	// Server -> Gofer -> Guest
	go func() {
		for {
			f, err := g.serverSide.ReadFrame()
			if err != nil {
				errCh <- err
				return
			}
			// Swallow internal gofer-injected FSYNC responses.
			if f.Unique >= (1 << 60) {
				continue
			}
			if g.syncFlush && f.Opcode == OpWrite && f.Errno == 0 {
				if hVal, ok := writeIDs.LoadAndDelete(f.Unique); ok {
					syncID := syncUnique.Add(1)
					g.hopCount.Add(1)
					if err := g.serverSide.WriteFrame(&Frame{
						Opcode: OpFsync,
						Unique: syncID,
						NodeID: f.NodeID,
						Handle: hVal.(uint64),
					}); err != nil {
						errCh <- err
						return
					}
					// Wait synchronously for the injected FSYNC reply before replying to guest.
					for {
						sf, err := g.serverSide.ReadFrame()
						if err != nil {
							errCh <- err
							return
						}
						g.hopCount.Add(1)
						if sf.Unique == syncID {
							if sf.Errno != 0 {
								f.Errno = sf.Errno
							}
							break
						}
						// Forward any interleaved concurrent response to the guest.
						if err := g.guestSide.WriteFrame(sf); err != nil {
							errCh <- err
							return
						}
					}
				}
			}
			if len(f.Payload) > 0 {
				dup := make([]byte, len(f.Payload))
				copy(dup, f.Payload)
				f.Payload = dup
				g.bytesHop.Add(int64(len(dup)))
			}
			g.hopCount.Add(1)
			if err := g.guestSide.WriteFrame(f); err != nil {
				errCh <- err
				return
			}
		}
	}()

	err := <-errCh
	_ = g.Close()
	<-errCh
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || ctx.Err() != nil {
		return nil
	}
	return err
}
