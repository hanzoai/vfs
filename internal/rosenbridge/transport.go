package rosenbridge

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

const (
	goferWireMagic   uint32 = 0x474F4652 // "GOFR"
	goferHeaderBytes        = 48
	maxGoferPayload         = 16 << 20 // 16 MiB frame guard
)

// GoferTransport implements a realistic canonical host<->guest gofer IPC
// channel over a real OS AF_UNIX SOCK_STREAM socketpair (modeled after
// LISAFS / 9P sandbox gofers). Every operation pays real socketpair write/read
// syscalls, wire framing/marshaling, kernel socket buffer copies, and host
// backing store execution.
type GoferTransport struct {
	store    BackingStore
	profiler *GoferProfiler

	guest *ipcEndpoint
	host  *ipcEndpoint

	guestMu sync.Mutex
	reqSeq  atomic.Uint64
	closed  atomic.Bool

	doneCh chan struct{}

	// Instrumented cumulative metrics.
	ops            atomic.Int64
	bytes          atomic.Int64
	socketSyscalls atomic.Int64
	backSyscalls   atomic.Int64
	copies         atomic.Int64
	allocBytes     atomic.Int64
}

// GoferMetrics summarizes canonical gofer transport counters.
type GoferMetrics struct {
	Ops             int64 `json:"ops"`
	Bytes           int64 `json:"bytes"`
	SocketSyscalls  int64 `json:"socket_syscalls"`
	BackingSyscalls int64 `json:"backing_syscalls"`
	TotalSyscalls   int64 `json:"total_syscalls"`
	Copies          int64 `json:"copies"`
	AllocBytes      int64 `json:"alloc_bytes"`
}

// NewGoferTransport spawns the canonical gofer host worker connected to the
// guest side over a real Unix socketpair.
func NewGoferTransport(store BackingStore, profiler *GoferProfiler) (*GoferTransport, error) {
	if store == nil {
		return nil, errors.New("rosenbridge: BackingStore required")
	}
	guest, host, err := newGoferIPC()
	if err != nil {
		return nil, err
	}
	g := &GoferTransport{
		store:    store,
		profiler: profiler,
		guest:    guest,
		host:     host,
		doneCh:   make(chan struct{}),
	}
	go g.serveHost()
	return g, nil
}

// Metrics returns atomic gofer transport counters.
func (g *GoferTransport) Metrics() GoferMetrics {
	sock := g.socketSyscalls.Load()
	back := g.backSyscalls.Load()
	return GoferMetrics{
		Ops:             g.ops.Load(),
		Bytes:           g.bytes.Load(),
		SocketSyscalls:  sock,
		BackingSyscalls: back,
		TotalSyscalls:   sock + back,
		Copies:          g.copies.Load(),
		AllocBytes:      g.allocBytes.Load(),
	}
}

// Close tears down both socketpair endpoints and waits for the host loop to exit.
func (g *GoferTransport) Close() error {
	if g.closed.Swap(true) {
		return nil
	}
	err1 := g.guest.Close()
	err2 := g.host.Close()
	<-g.doneCh
	if err1 != nil {
		return err1
	}
	return err2
}

// Wire header layout (48 bytes):
// [0:4]   magic
// [4]     op
// [5]     flags
// [6:8]   status (0=ok, else errno)
// [8:16]  reqID
// [16:24] fileOffset (int64)
// [24:28] payloadLen (uint32)
// [28:30] tenantLen (uint16)
// [30:32] pathLen (uint16)
// [32:36] bytesDone (int32)
// [36:40] backSyscalls (uint32)
// [40:48] backingNs (int64)

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func (g *GoferTransport) serveHost() {
	defer close(g.doneCh)
	var hdr [goferHeaderBytes]byte
	ctx := context.Background()

	for {
		if _, err := io.ReadFull(g.host, hdr[:]); err != nil {
			return
		}
		magic := binary.LittleEndian.Uint32(hdr[0:4])
		if magic != goferWireMagic {
			return
		}
		op := OpCode(hdr[4])
		flags := hdr[5]
		reqID := binary.LittleEndian.Uint64(hdr[8:16])
		fileOff := int64(binary.LittleEndian.Uint64(hdr[16:24]))
		payloadLen := binary.LittleEndian.Uint32(hdr[24:28])
		tenantLen := binary.LittleEndian.Uint16(hdr[28:30])
		pathLen := binary.LittleEndian.Uint16(hdr[30:32])

		if payloadLen > maxGoferPayload || tenantLen == 0 || pathLen == 0 {
			return
		}

		metaBuf := make([]byte, int(tenantLen)+int(pathLen))
		if _, err := io.ReadFull(g.host, metaBuf); err != nil {
			return
		}
		tenantID := string(metaBuf[:tenantLen])
		relPath := string(metaBuf[tenantLen:])

		var respPayload []byte
		var bytesDone int64
		var backSc int
		var opErr error
		var backingDur time.Duration

		switch op {
		case OpWrite:
			reqPayload := make([]byte, payloadLen)
			if payloadLen > 0 {
				if _, err := io.ReadFull(g.host, reqPayload); err != nil {
					return
				}
			}
			t0 := time.Now()
			var n int
			n, backSc, opErr = g.store.WriteAt(ctx, tenantID, relPath, reqPayload, fileOff, flags&byte(FlagSync) != 0)
			backingDur = time.Since(t0)
			bytesDone = int64(n)

		case OpRead:
			respPayload = make([]byte, payloadLen)
			t0 := time.Now()
			var n int
			n, backSc, opErr = g.store.ReadAt(ctx, tenantID, relPath, respPayload, fileOff)
			backingDur = time.Since(t0)
			bytesDone = int64(n)
			if opErr != nil {
				respPayload = nil
			} else {
				respPayload = respPayload[:n]
			}

		case OpStat:
			t0 := time.Now()
			bytesDone, backSc, opErr = g.store.Stat(ctx, tenantID, relPath)
			backingDur = time.Since(t0)

		default:
			opErr = ErrUnsupportedOp
		}

		var respHdr [goferHeaderBytes]byte
		binary.LittleEndian.PutUint32(respHdr[0:4], goferWireMagic)
		respHdr[4] = byte(op)
		respHdr[5] = flags
		var status uint16
		if opErr != nil {
			status = encodeWireErr(opErr)
		}
		binary.LittleEndian.PutUint16(respHdr[6:8], status)
		binary.LittleEndian.PutUint64(respHdr[8:16], reqID)
		binary.LittleEndian.PutUint64(respHdr[16:24], uint64(bytesDone))
		binary.LittleEndian.PutUint32(respHdr[24:28], uint32(len(respPayload)))
		binary.LittleEndian.PutUint32(respHdr[36:40], uint32(backSc))
		binary.LittleEndian.PutUint64(respHdr[40:48], uint64(backingDur.Nanoseconds()))

		// Frame response header + payload in one contiguous write (standard RPC marshaling).
		respFrame := make([]byte, goferHeaderBytes+len(respPayload))
		copy(respFrame[:goferHeaderBytes], respHdr[:])
		if len(respPayload) > 0 {
			copy(respFrame[goferHeaderBytes:], respPayload)
		}
		if err := writeFull(g.host, respFrame); err != nil {
			return
		}
	}
}

const (
	wireErrPathEscape  uint16 = 1001
	wireErrTenant      uint16 = 1002
	wireErrNotExist    uint16 = 1003
	wireErrOutOfBounds uint16 = 1004
	wireErrUnsupported uint16 = 1005
	wireErrGenericIO   uint16 = 1099
)

func encodeWireErr(err error) uint16 {
	switch {
	case errors.Is(err, ErrPathEscape):
		return wireErrPathEscape
	case errors.Is(err, ErrTenantMismatch), errors.Is(err, ErrInvalidTenant):
		return wireErrTenant
	case errors.Is(err, os.ErrNotExist):
		return wireErrNotExist
	case errors.Is(err, ErrDescriptorOutOfBounds):
		return wireErrOutOfBounds
	case errors.Is(err, ErrUnsupportedOp):
		return wireErrUnsupported
	default:
		if se := Errno(err); se != 0 && se < 1000 {
			return uint16(se)
		}
		return wireErrGenericIO
	}
}

func decodeWireErr(code uint16) error {
	switch code {
	case 0:
		return nil
	case wireErrPathEscape:
		return ErrPathEscape
	case wireErrTenant:
		return ErrTenantMismatch
	case wireErrNotExist:
		return os.ErrNotExist
	case wireErrOutOfBounds:
		return ErrDescriptorOutOfBounds
	case wireErrUnsupported:
		return ErrUnsupportedOp
	case wireErrGenericIO:
		return syscall.EIO
	default:
		return syscall.Errno(code)
	}
}

func (g *GoferTransport) roundTrip(ctx context.Context, op OpCode, flags byte, tenantID, relPath string, off int64, writeBuf []byte, readBuf []byte) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if g.closed.Load() {
		return 0, ErrRingClosed
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return 0, err
	}
	if len(relPath) > 4096 {
		return 0, fmt.Errorf("%w: path length %d exceeds 4096", ErrPathEscape, len(relPath))
	}
	if _, err := NormalizeVirtualPath(relPath); err != nil {
		return 0, err
	}

	reqLen := uint32(len(readBuf))
	if op == OpWrite {
		reqLen = uint32(len(writeBuf))
	}
	if reqLen > maxGoferPayload {
		return 0, fmt.Errorf("%w: payload %d exceeds max %d", ErrDescriptorOutOfBounds, reqLen, maxGoferPayload)
	}

	t0 := time.Now()
	reqID := g.reqSeq.Add(1)

	// Marshal request frame: header + tenantID + relPath + optional write payload.
	frameSize := goferHeaderBytes + len(tenantID) + len(relPath)
	if op == OpWrite {
		frameSize += len(writeBuf)
	}
	frame := make([]byte, frameSize)
	binary.LittleEndian.PutUint32(frame[0:4], goferWireMagic)
	frame[4] = byte(op)
	frame[5] = flags
	binary.LittleEndian.PutUint64(frame[8:16], reqID)
	binary.LittleEndian.PutUint64(frame[16:24], uint64(off))
	binary.LittleEndian.PutUint32(frame[24:28], reqLen)
	binary.LittleEndian.PutUint16(frame[28:30], uint16(len(tenantID)))
	binary.LittleEndian.PutUint16(frame[30:32], uint16(len(relPath)))
	copy(frame[goferHeaderBytes:], tenantID)
	copy(frame[goferHeaderBytes+len(tenantID):], relPath)
	if op == OpWrite && len(writeBuf) > 0 {
		copy(frame[goferHeaderBytes+len(tenantID)+len(relPath):], writeBuf)
	}

	g.guestMu.Lock()
	if g.closed.Load() {
		g.guestMu.Unlock()
		return 0, ErrRingClosed
	}
	if err := writeFull(g.guest, frame); err != nil {
		g.guestMu.Unlock()
		return 0, err
	}

	var respHdr [goferHeaderBytes]byte
	if _, err := io.ReadFull(g.guest, respHdr[:]); err != nil {
		g.guestMu.Unlock()
		return 0, err
	}
	status := binary.LittleEndian.Uint16(respHdr[6:8])
	bytesDone := int64(binary.LittleEndian.Uint64(respHdr[16:24]))
	respPayloadLen := binary.LittleEndian.Uint32(respHdr[24:28])
	backSc := int(binary.LittleEndian.Uint32(respHdr[36:40]))
	backingNs := int64(binary.LittleEndian.Uint64(respHdr[40:48]))

	if respPayloadLen > 0 {
		if int(respPayloadLen) > len(readBuf) {
			g.guestMu.Unlock()
			return 0, ErrDescriptorOutOfBounds
		}
		if _, err := io.ReadFull(g.guest, readBuf[:respPayloadLen]); err != nil {
			g.guestMu.Unlock()
			return 0, err
		}
	}
	g.guestMu.Unlock()

	totalDur := time.Since(t0)
	backingDur := time.Duration(backingNs)
	boundaryDur := totalDur - backingDur
	if boundaryDur < 0 {
		boundaryDur = 0
	}

	sockSc := 4
	copies := 2
	allocs := frameSize + goferHeaderBytes
	payloadBytes := 0
	switch op {
	case OpRead:
		sockSc = 5
		if respPayloadLen > 0 {
			sockSc = 6
		}
		// Copies: (1) backing pread->host buf, (2) host buf->resp frame, (3) host write->kernel skb, (4) kernel skb->guest readBuf
		copies = 4
		allocs += int(respPayloadLen)*2 + len(tenantID) + len(relPath)
		payloadBytes = int(bytesDone)
	case OpWrite:
		sockSc = 6
		// Copies: (1) caller writeBuf->req frame, (2) guest write->kernel skb, (3) kernel skb->host reqPayload, (4) host reqPayload->backing pwrite
		copies = 4
		allocs += len(writeBuf) + len(tenantID) + len(relPath)
		payloadBytes = int(bytesDone)
	case OpStat:
		sockSc = 5
		copies = 2
		allocs += len(tenantID) + len(relPath)
	}

	g.ops.Add(1)
	g.bytes.Add(int64(payloadBytes))
	g.socketSyscalls.Add(int64(sockSc))
	g.backSyscalls.Add(int64(backSc))
	g.copies.Add(int64(copies))
	g.allocBytes.Add(int64(allocs))

	if g.profiler != nil && (op == OpRead || op == OpWrite) && status == 0 {
		g.profiler.Record(GoferSample{
			Op:           op,
			Bytes:        payloadBytes,
			TotalTime:    totalDur,
			BoundaryTime: boundaryDur,
			BackingTime:  backingDur,
			Syscalls:     sockSc + backSc,
			Copies:       copies,
			AllocBytes:   allocs,
		})
	}

	if status != 0 {
		return 0, decodeWireErr(status)
	}
	return bytesDone, nil
}

// ReadAt executes a read over the canonical gofer socketpair RPC path.
func (g *GoferTransport) ReadAt(ctx context.Context, tenantID, relPath string, dst []byte, off int64) (int, error) {
	n, err := g.roundTrip(ctx, OpRead, 0, tenantID, relPath, off, nil, dst)
	return int(n), err
}

// WriteAt executes a write over the canonical gofer socketpair RPC path.
func (g *GoferTransport) WriteAt(ctx context.Context, tenantID, relPath string, src []byte, off int64, doSync bool) (int, error) {
	var flags byte
	if doSync {
		flags = byte(FlagSync)
	}
	n, err := g.roundTrip(ctx, OpWrite, flags, tenantID, relPath, off, src, nil)
	return int(n), err
}

// Stat executes a metadata stat over the canonical gofer socketpair RPC path.
func (g *GoferTransport) Stat(ctx context.Context, tenantID, relPath string) (int64, error) {
	return g.roundTrip(ctx, OpStat, 0, tenantID, relPath, 0, nil, nil)
}

// RosenBridgeChannel is a strictly per-tenant lower-copy shared-memory fast
// path pairing one page-aligned mmap SlabPool with one atomic Ring and a host
// dispatch worker. Zero socketpair syscalls and zero wire-marshaling copies
// occur on admitted transfers.
type RosenBridgeChannel struct {
	tenantID    string
	tenantToken uint64
	slab        *SlabPool
	ring        *Ring
	store       BackingStore

	doneCh chan struct{}
	closed atomic.Bool

	// Instrumented counters.
	ops      atomic.Int64
	bytes    atomic.Int64
	syscalls atomic.Int64
	copies   atomic.Int64
}

// ChannelMetrics summarizes fast-path channel execution counters.
type ChannelMetrics struct {
	Ops      int64 `json:"ops"`
	Bytes    int64 `json:"bytes"`
	Syscalls int64 `json:"syscalls"`
	Copies   int64 `json:"copies"`
}

func generateTenantToken() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	tok := binary.LittleEndian.Uint64(b[:])
	if tok == 0 {
		tok = 1
	}
	return tok, nil
}

// NewRosenBridgeChannel creates an isolated per-tenant shared-memory channel.
func NewRosenBridgeChannel(tenantID string, store BackingStore, ringSlots int, slabSlotSize uint64) (*RosenBridgeChannel, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("rosenbridge: BackingStore required")
	}
	tok, err := generateTenantToken()
	if err != nil {
		return nil, fmt.Errorf("rosenbridge: generate tenant token: %w", err)
	}
	ring, err := NewRing(tenantID, ringSlots)
	if err != nil {
		return nil, err
	}
	slab, err := NewSlabPool(tenantID, tok, ring.Capacity(), slabSlotSize)
	if err != nil {
		ring.Close()
		return nil, err
	}
	ch := &RosenBridgeChannel{
		tenantID:    tenantID,
		tenantToken: tok,
		slab:        slab,
		ring:        ring,
		store:       store,
		doneCh:      make(chan struct{}),
	}
	go ch.hostWorker()
	return ch, nil
}

func (c *RosenBridgeChannel) TenantID() string { return c.tenantID }
func (c *RosenBridgeChannel) Token() uint64    { return c.tenantToken }
func (c *RosenBridgeChannel) Slab() *SlabPool  { return c.slab }
func (c *RosenBridgeChannel) Ring() *Ring      { return c.ring }

func (c *RosenBridgeChannel) Metrics() ChannelMetrics {
	return ChannelMetrics{
		Ops:      c.ops.Load(),
		Bytes:    c.bytes.Load(),
		Syscalls: c.syscalls.Load(),
		Copies:   c.copies.Load(),
	}
}

func (c *RosenBridgeChannel) hostWorker() {
	defer close(c.doneCh)
	var batch [64]ringEntry
	ctx := context.Background()

	processBatch := func(n int) {
		for i := 0; i < n; i++ {
			c.executeEntry(ctx, batch[i])
			batch[i] = ringEntry{}
		}
	}

	for {
		if n := c.ring.PopBatch(batch[:]); n > 0 {
			processBatch(n)
			continue
		}
		select {
		case <-c.ring.Doorbell():
			for {
				n := c.ring.PopBatch(batch[:])
				if n == 0 {
					break
				}
				processBatch(n)
			}
		case <-c.ring.StopCh():
			// Drain any remaining submitted entries with ErrRingClosed.
			for {
				n := c.ring.PopBatch(batch[:])
				if n == 0 {
					return
				}
				for i := 0; i < n; i++ {
					batch[i].done <- Completion{Seq: batch[i].desc.Seq, Err: ErrRingClosed}
					batch[i] = ringEntry{}
				}
			}
		}
	}
}

func (c *RosenBridgeChannel) executeEntry(ctx context.Context, e ringEntry) {
	d := e.desc
	if err := ValidateDescriptor(d, c.tenantID, c.tenantToken, c.slab.Size(), c.slab.SlotSize(), MaxFileSize); err != nil {
		e.done <- Completion{Seq: d.Seq, Err: err}
		return
	}

	slabSlice, err := c.slab.ReserveRange(d.TenantID, d.TenantToken, d.SlabOffset, d.Length)
	if err != nil {
		e.done <- Completion{Seq: d.Seq, Err: err}
		return
	}
	defer c.slab.ReleaseRange(d.SlabOffset, d.Length)

	var n, sc int
	var crc uint32
	switch d.Op {
	case OpRead:
		n, sc, err = c.store.ReadAt(ctx, c.tenantID, d.Path, slabSlice, int64(d.FileOffset))
		if err == nil && d.Flags&FlagVerifyCRC != 0 {
			crc = crc32.Checksum(slabSlice[:n], crc32cTable)
		}
	case OpWrite:
		if d.Flags&FlagVerifyCRC != 0 {
			got := crc32.Checksum(slabSlice, crc32cTable)
			if got != d.CRC32C {
				e.done <- Completion{Seq: d.Seq, Err: fmt.Errorf("%w: got %#x want %#x", ErrCorruptData, got, d.CRC32C)}
				return
			}
			crc = got
		}
		n, sc, err = c.store.WriteAt(ctx, c.tenantID, d.Path, slabSlice, int64(d.FileOffset), d.Flags&FlagSync != 0)
	default:
		err = ErrUnsupportedOp
	}

	if err == nil {
		c.ops.Add(1)
		c.bytes.Add(int64(n))
		c.syscalls.Add(int64(sc))
	}
	e.done <- Completion{
		Seq:       d.Seq,
		BytesDone: n,
		Syscalls:  sc,
		CRC32C:    crc,
		Err:       err,
	}
}

// SubmitRawDescriptor submits a caller-constructed Descriptor directly onto
// the ring and waits for completion. Host-side validation strictly rejects
// forged tenant tokens, out-of-bounds offsets, overlapping ranges, truncated
// lengths, and path/symlink escapes.
func (c *RosenBridgeChannel) SubmitRawDescriptor(ctx context.Context, d Descriptor) (Completion, error) {
	if err := ctx.Err(); err != nil {
		return Completion{}, err
	}
	if c.closed.Load() {
		return Completion{}, ErrRingClosed
	}
	doneCh, _, err := c.ring.Submit(d)
	if err != nil {
		return Completion{}, err
	}
	select {
	case comp := <-doneCh:
		c.ring.ReleaseCompletionChan(doneCh)
		return comp, comp.Err
	case <-ctx.Done():
		// Drain asynchronously so host worker never blocks if it completes later.
		go func() {
			<-doneCh
			c.ring.ReleaseCompletionChan(doneCh)
		}()
		return Completion{}, ctx.Err()
	}
}

// ReadSlab performs a 1-copy shared-slab read: the host writes directly from
// the backing store into the tenant's page-aligned mmap slab and returns the
// slice along with a release function. Zero intermediate marshal/IPC copies
// and zero socketpair syscalls are performed.
func (c *RosenBridgeChannel) ReadSlab(ctx context.Context, relPath string, off int64, length int, verifyCRC bool) ([]byte, func(), error) {
	if length <= 0 {
		return nil, nil, fmt.Errorf("%w: non-positive length %d", ErrDescriptorTruncated, length)
	}
	if off < 0 || off > math.MaxInt64-int64(length) {
		return nil, nil, fmt.Errorf("%w: invalid offset %d + length %d", ErrDescriptorOutOfBounds, off, length)
	}
	slotIdx, slabOff, slabSlice, err := c.slab.AcquireSlot(c.tenantID, c.tenantToken, uint64(length))
	if err != nil {
		return nil, nil, err
	}
	var flags uint32
	if verifyCRC {
		flags |= FlagVerifyCRC
	}
	comp, err := c.SubmitRawDescriptor(ctx, Descriptor{
		Op:          OpRead,
		Flags:       flags,
		TenantID:    c.tenantID,
		TenantToken: c.tenantToken,
		Path:        relPath,
		FileOffset:  uint64(off),
		SlabOffset:  slabOff,
		Length:      uint64(length),
		DeclaredLen: uint64(length),
	})
	if err != nil {
		c.slab.ReleaseSlot(slotIdx)
		return nil, nil, err
	}
	if verifyCRC {
		got := crc32.Checksum(slabSlice[:comp.BytesDone], crc32cTable)
		if got != comp.CRC32C {
			c.slab.ReleaseSlot(slotIdx)
			return nil, nil, fmt.Errorf("%w: read slab crc mismatch", ErrCorruptData)
		}
	}
	// 1 data copy total: backing store -> mmap slab.
	c.copies.Add(1)
	released := atomic.Bool{}
	release := func() {
		if released.CompareAndSwap(false, true) {
			c.slab.ReleaseSlot(slotIdx)
		}
	}
	return slabSlice[:comp.BytesDone], release, nil
}

// WriteSlab performs a 1-copy shared-slab write: fillFn writes directly into
// the tenant's page-aligned mmap slab slice, and the host executes pwrite/VFS
// WriteAt directly from that slab slice with zero intermediate copies or socket
// syscalls.
func (c *RosenBridgeChannel) WriteSlab(ctx context.Context, relPath string, off int64, length int, doSync, verifyCRC bool, fillFn func([]byte) int) (int, error) {
	if length <= 0 {
		return 0, fmt.Errorf("%w: non-positive length %d", ErrDescriptorTruncated, length)
	}
	if off < 0 || off > math.MaxInt64-int64(length) {
		return 0, fmt.Errorf("%w: invalid offset %d + length %d", ErrDescriptorOutOfBounds, off, length)
	}
	slotIdx, slabOff, slabSlice, err := c.slab.AcquireSlot(c.tenantID, c.tenantToken, uint64(length))
	if err != nil {
		return 0, err
	}
	defer c.slab.ReleaseSlot(slotIdx)

	written := length
	if fillFn != nil {
		written = fillFn(slabSlice)
		if written <= 0 || written > length {
			return 0, fmt.Errorf("%w: invalid fill length %d", ErrDescriptorTruncated, written)
		}
	}
	var flags uint32
	var crc uint32
	if doSync {
		flags |= FlagSync
	}
	if verifyCRC {
		flags |= FlagVerifyCRC
		crc = crc32.Checksum(slabSlice[:written], crc32cTable)
	}
	comp, err := c.SubmitRawDescriptor(ctx, Descriptor{
		Op:          OpWrite,
		Flags:       flags,
		TenantID:    c.tenantID,
		TenantToken: c.tenantToken,
		Path:        relPath,
		FileOffset:  uint64(off),
		SlabOffset:  slabOff,
		Length:      uint64(written),
		DeclaredLen: uint64(written),
		CRC32C:      crc,
	})
	if err != nil {
		return 0, err
	}
	// 1 data copy total: mmap slab -> backing store.
	c.copies.Add(1)
	return comp.BytesDone, nil
}

// ReadAt performs a POSIX-compatible slice read over the tenant's mmap slab
// (2 data copies: backing -> mmap slab -> dst; 0 socket syscalls).
func (c *RosenBridgeChannel) ReadAt(ctx context.Context, relPath string, dst []byte, off int64, verifyCRC bool) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if off < 0 || off > math.MaxInt64-int64(len(dst)) {
		return 0, fmt.Errorf("%w: invalid offset %d + length %d", ErrDescriptorOutOfBounds, off, len(dst))
	}
	total := 0
	maxChunk := int(c.slab.SlotSize())
	for total < len(dst) {
		chunk := min(len(dst)-total, maxChunk)
		slabData, release, err := c.ReadSlab(ctx, relPath, off+int64(total), chunk, verifyCRC)
		if err != nil {
			if total > 0 && errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}
		n := copy(dst[total:], slabData)
		release()
		// Account for the second copy (mmap slab -> caller dst).
		c.copies.Add(1)
		total += n
		if n < chunk {
			break
		}
	}
	return total, nil
}

// WriteAt performs a POSIX-compatible slice write over the tenant's mmap slab
// (2 data copies: src -> mmap slab -> backing; 0 socket syscalls).
func (c *RosenBridgeChannel) WriteAt(ctx context.Context, relPath string, src []byte, off int64, doSync, verifyCRC bool) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}
	if off < 0 || off > math.MaxInt64-int64(len(src)) {
		return 0, fmt.Errorf("%w: invalid offset %d + length %d", ErrDescriptorOutOfBounds, off, len(src))
	}
	total := 0
	maxChunk := int(c.slab.SlotSize())
	for total < len(src) {
		chunk := min(len(src)-total, maxChunk)
		isLast := total+chunk == len(src)
		sub := src[total : total+chunk]
		n, err := c.WriteSlab(ctx, relPath, off+int64(total), chunk, doSync && isLast, verifyCRC, func(dst []byte) int {
			return copy(dst, sub)
		})
		if err != nil {
			return total, err
		}
		// Account for the caller src -> mmap slab copy.
		c.copies.Add(1)
		total += n
	}
	return total, nil
}

// Close shuts down the host worker, closes the ring, zeroes and unmaps the
// tenant's mmap slab, and closes the backing file descriptor.
func (c *RosenBridgeChannel) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.ring.Close()
	<-c.doneCh
	return c.slab.Close()
}

// Router orchestrates the canonical Gofer baseline alongside conditional,
// strictly per-tenant RosenBridge fast paths governed by AdmissionGate.
type Router struct {
	store BackingStore
	gofer *GoferTransport
	gate  *AdmissionGate

	mu       sync.RWMutex
	channels map[string]*RosenBridgeChannel
	closed   bool

	verifyCRC bool
}

// RouterStats captures end-to-end telemetry across both paths and the gate.
type RouterStats struct {
	Gofer    GoferMetrics   `json:"gofer"`
	FastPath ChannelMetrics `json:"fast_path"`
	Gate     GateMetrics    `json:"gate"`
	Profile  ProfileSummary `json:"profile"`
}

// NewRouter creates a conditional router backed by store with a canonical
// GoferTransport always active and an AdmissionGate controlling RosenBridge.
func NewRouter(store BackingStore, cfg GateConfig) (*Router, error) {
	gate := NewAdmissionGate(cfg)
	gofer, err := NewGoferTransport(store, gate.Profiler())
	if err != nil {
		return nil, err
	}
	return &Router{
		store:    store,
		gofer:    gofer,
		gate:     gate,
		channels: make(map[string]*RosenBridgeChannel),
	}, nil
}

// SetVerifyCRC toggles Castagnoli CRC32C payload verification on fast-path IOs.
func (r *Router) SetVerifyCRC(v bool) { r.verifyCRC = v }

// Gate returns the router's conditional AdmissionGate.
func (r *Router) Gate() *AdmissionGate { return r.gate }

// Gofer returns the canonical GoferTransport baseline.
func (r *Router) Gofer() *GoferTransport { return r.gofer }

// RegisterTenant provisions a strictly isolated per-tenant RosenBridgeChannel.
func (r *Router) RegisterTenant(tenantID string, ringSlots int, slabSlotSize uint64) (*RosenBridgeChannel, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrRingClosed
	}
	if ch, exists := r.channels[tenantID]; exists {
		return ch, nil
	}
	ch, err := NewRosenBridgeChannel(tenantID, r.store, ringSlots, slabSlotSize)
	if err != nil {
		return nil, err
	}
	r.channels[tenantID] = ch
	return ch, nil
}

// TenantChannel returns the isolated RosenBridgeChannel for tenantID, if registered.
func (r *Router) TenantChannel(tenantID string) (*RosenBridgeChannel, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ch, ok := r.channels[tenantID]
	return ch, ok
}

// ReadAt routes a read conditionally through RosenBridge when admitted, or
// transparently through the canonical gofer baseline otherwise.
func (r *Router) ReadAt(ctx context.Context, tenantID, relPath string, dst []byte, off int64) (int, FallbackReason, error) {
	admit, reason := r.gate.Evaluate(AdmissionRequest{
		TenantID: tenantID,
		Op:       OpRead,
		Path:     relPath,
		Offset:   off,
		Length:   len(dst),
	})
	if !admit {
		n, err := r.gofer.ReadAt(ctx, tenantID, relPath, dst, off)
		return n, reason, err
	}
	defer r.gate.ReleaseInFlight(tenantID, len(dst))

	ch, ok := r.TenantChannel(tenantID)
	if !ok {
		r.gate.RecordFallback(FallbackNoTenantChannel)
		n, err := r.gofer.ReadAt(ctx, tenantID, relPath, dst, off)
		return n, FallbackNoTenantChannel, err
	}

	n, err := ch.ReadAt(ctx, relPath, dst, off, r.verifyCRC)
	if err != nil {
		if IsSecurityViolation(err) {
			return 0, ReasonAdmitted, err
		}
		if errors.Is(err, ErrRingFull) || errors.Is(err, ErrFastPathDisabled) {
			r.gate.RecordFallback(FallbackRingFull)
			gn, gerr := r.gofer.ReadAt(ctx, tenantID, relPath, dst, off)
			return gn, FallbackRingFull, gerr
		}
		return n, ReasonAdmitted, err
	}
	return n, ReasonAdmitted, nil
}

// WriteAt routes a write conditionally through RosenBridge when admitted, or
// transparently through the canonical gofer baseline otherwise.
func (r *Router) WriteAt(ctx context.Context, tenantID, relPath string, src []byte, off int64, doSync bool) (int, FallbackReason, error) {
	admit, reason := r.gate.Evaluate(AdmissionRequest{
		TenantID: tenantID,
		Op:       OpWrite,
		Path:     relPath,
		Offset:   off,
		Length:   len(src),
	})
	if !admit {
		n, err := r.gofer.WriteAt(ctx, tenantID, relPath, src, off, doSync)
		return n, reason, err
	}
	defer r.gate.ReleaseInFlight(tenantID, len(src))

	ch, ok := r.TenantChannel(tenantID)
	if !ok {
		r.gate.RecordFallback(FallbackNoTenantChannel)
		n, err := r.gofer.WriteAt(ctx, tenantID, relPath, src, off, doSync)
		return n, FallbackNoTenantChannel, err
	}

	n, err := ch.WriteAt(ctx, relPath, src, off, doSync, r.verifyCRC)
	if err != nil {
		if IsSecurityViolation(err) {
			return 0, ReasonAdmitted, err
		}
		if errors.Is(err, ErrRingFull) || errors.Is(err, ErrFastPathDisabled) {
			r.gate.RecordFallback(FallbackRingFull)
			gn, gerr := r.gofer.WriteAt(ctx, tenantID, relPath, src, off, doSync)
			return gn, FallbackRingFull, gerr
		}
		return n, ReasonAdmitted, err
	}
	return n, ReasonAdmitted, nil
}

// Stat ALWAYS routes through the canonical gofer path (metadata operations are
// never moved onto the data-plane fast path).
func (r *Router) Stat(ctx context.Context, tenantID, relPath string) (int64, FallbackReason, error) {
	_, reason := r.gate.Evaluate(AdmissionRequest{
		TenantID: tenantID,
		Op:       OpStat,
		Path:     relPath,
	})
	sz, err := r.gofer.Stat(ctx, tenantID, relPath)
	return sz, reason, err
}

// ReadSlab executes a 1-copy shared-slab read when admitted by the gate, or
// falls back to canonical gofer into an allocated buffer if unadmitted/full.
func (r *Router) ReadSlab(ctx context.Context, tenantID, relPath string, off int64, length int) ([]byte, func(), FallbackReason, error) {
	admit, reason := r.gate.Evaluate(AdmissionRequest{
		TenantID: tenantID,
		Op:       OpRead,
		Path:     relPath,
		Offset:   off,
		Length:   length,
	})
	if !admit {
		buf := make([]byte, length)
		n, err := r.gofer.ReadAt(ctx, tenantID, relPath, buf, off)
		return buf[:n], func() {}, reason, err
	}

	ch, ok := r.TenantChannel(tenantID)
	if !ok {
		r.gate.ReleaseInFlight(tenantID, length)
		r.gate.RecordFallback(FallbackNoTenantChannel)
		buf := make([]byte, length)
		n, err := r.gofer.ReadAt(ctx, tenantID, relPath, buf, off)
		return buf[:n], func() {}, FallbackNoTenantChannel, err
	}

	data, release, err := ch.ReadSlab(ctx, relPath, off, length, r.verifyCRC)
	if err != nil {
		r.gate.ReleaseInFlight(tenantID, length)
		if IsSecurityViolation(err) {
			return nil, nil, ReasonAdmitted, err
		}
		if errors.Is(err, ErrRingFull) {
			r.gate.RecordFallback(FallbackRingFull)
			buf := make([]byte, length)
			n, gerr := r.gofer.ReadAt(ctx, tenantID, relPath, buf, off)
			return buf[:n], func() {}, FallbackRingFull, gerr
		}
		return nil, nil, ReasonAdmitted, err
	}
	var once sync.Once
	wrappedRelease := func() {
		once.Do(func() {
			release()
			r.gate.ReleaseInFlight(tenantID, length)
		})
	}
	return data, wrappedRelease, ReasonAdmitted, nil
}

// WriteSlab executes a 1-copy shared-slab write when admitted by the gate, or
// falls back to canonical gofer if unadmitted/full.
func (r *Router) WriteSlab(ctx context.Context, tenantID, relPath string, off int64, length int, doSync bool, fillFn func([]byte) int) (int, FallbackReason, error) {
	admit, reason := r.gate.Evaluate(AdmissionRequest{
		TenantID: tenantID,
		Op:       OpWrite,
		Path:     relPath,
		Offset:   off,
		Length:   length,
	})
	if !admit {
		buf := make([]byte, length)
		nFill := length
		if fillFn != nil {
			nFill = fillFn(buf)
		}
		n, err := r.gofer.WriteAt(ctx, tenantID, relPath, buf[:nFill], off, doSync)
		return n, reason, err
	}
	defer r.gate.ReleaseInFlight(tenantID, length)

	ch, ok := r.TenantChannel(tenantID)
	if !ok {
		r.gate.RecordFallback(FallbackNoTenantChannel)
		buf := make([]byte, length)
		nFill := length
		if fillFn != nil {
			nFill = fillFn(buf)
		}
		n, err := r.gofer.WriteAt(ctx, tenantID, relPath, buf[:nFill], off, doSync)
		return n, FallbackNoTenantChannel, err
	}

	n, err := ch.WriteSlab(ctx, relPath, off, length, doSync, r.verifyCRC, fillFn)
	if err != nil {
		if IsSecurityViolation(err) {
			return 0, ReasonAdmitted, err
		}
		if errors.Is(err, ErrRingFull) {
			r.gate.RecordFallback(FallbackRingFull)
			buf := make([]byte, length)
			nFill := length
			if fillFn != nil {
				nFill = fillFn(buf)
			}
			gn, gerr := r.gofer.WriteAt(ctx, tenantID, relPath, buf[:nFill], off, doSync)
			return gn, FallbackRingFull, gerr
		}
		return n, ReasonAdmitted, err
	}
	return n, ReasonAdmitted, nil
}

// Stats returns aggregated metrics across gofer, all tenant fast-path channels,
// the admission gate, and the gofer bottleneck profiler.
func (r *Router) Stats() RouterStats {
	var fp ChannelMetrics
	r.mu.RLock()
	for _, ch := range r.channels {
		m := ch.Metrics()
		fp.Ops += m.Ops
		fp.Bytes += m.Bytes
		fp.Syscalls += m.Syscalls
		fp.Copies += m.Copies
	}
	r.mu.RUnlock()
	return RouterStats{
		Gofer:    r.gofer.Metrics(),
		FastPath: fp,
		Gate:     r.gate.Metrics(),
		Profile:  r.gate.Profiler().Summary(),
	}
}

// Close shuts down all tenant channels and the canonical gofer transport.
func (r *Router) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	channels := r.channels
	r.channels = nil
	r.mu.Unlock()

	var firstErr error
	for _, ch := range channels {
		if err := ch.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := r.gofer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
