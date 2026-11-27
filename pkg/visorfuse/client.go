// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package visorfuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
)

// GuestFUSEClient speaks the binary FUSE wire protocol from inside an
// unprivileged Visor sandbox over the pre-authenticated host socket FD.
// It supports concurrent multiplexed requests keyed by Frame.Unique.
type GuestFUSEClient struct {
	conn *FrameConn

	nextUnique atomic.Uint64

	writeSem  chan struct{}
	closeOnce sync.Once
	closeErr  error

	mu      sync.Mutex
	pending map[uint64]chan *Frame
	closed  bool
	readErr error
	done    chan struct{}
}

// NewGuestFUSEClient wraps guestConn (the sandbox end of the host-provided
// socket FD) and starts the background response demultiplexer.
func NewGuestFUSEClient(guestConn io.ReadWriteCloser, kind SocketKind) *GuestFUSEClient {
	c := &GuestFUSEClient{
		conn:     NewFrameConn(guestConn, kind),
		writeSem: make(chan struct{}, 1),
		pending:  make(map[uint64]chan *Frame),
		done:     make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *GuestFUSEClient) closeConn() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

func (c *GuestFUSEClient) failPending(err error) {
	c.mu.Lock()
	c.closed = true
	if c.readErr == nil {
		c.readErr = err
	}
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	c.mu.Unlock()
}

func (c *GuestFUSEClient) readLoop() {
	defer close(c.done)
	defer func() { _ = c.closeConn() }()
	for {
		resp, err := c.conn.ReadFrame()
		if err != nil {
			c.failPending(err)
			return
		}
		c.mu.Lock()
		ch, ok := c.pending[resp.Unique]
		if ok {
			delete(c.pending, resp.Unique)
		}
		c.mu.Unlock()
		if ok {
			ch <- resp
		}
	}
}

// Close closes the client socket exactly once and waits for the demuxer
// goroutine to exit, releasing the underlying transport even if readLoop has
// already exited on EOF or a malformed frame.
func (c *GuestFUSEClient) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	err := c.closeConn()
	<-c.done
	return err
}

func (c *GuestFUSEClient) call(ctx context.Context, req *Frame) (*Frame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := c.nextUnique.Add(1)
	req.Unique = id
	ch := make(chan *Frame, 1)

	// Acquire the single-writer turn in a context-aware manner so concurrent
	// callers unblock immediately on cancellation when a peer stalls reads.
	select {
	case c.writeSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrSessionClosed
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.writeSem
		return nil, ErrSessionClosed
	}
	c.pending[id] = ch
	c.mu.Unlock()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- c.conn.WriteFrame(req)
	}()

	var writeErr error
	select {
	case writeErr = <-writeDone:
		<-c.writeSem
	case <-ctx.Done():
		// Interrupting an in-flight write may leave a partial frame on a
		// stream/pipe transport. Fail-close the session and transport so the
		// blocked writer unblocks immediately and no subsequent caller can
		// interleave corrupted framing onto the stream.
		c.failPending(ctx.Err())
		_ = c.closeConn()
		<-writeDone
		<-c.writeSem
		return nil, ctx.Err()
	case <-c.done:
		_ = c.closeConn()
		<-writeDone
		<-c.writeSem
		return nil, ErrSessionClosed
	}

	if writeErr != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, writeErr
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, ErrSessionClosed
		}
		if resp.Errno != 0 {
			return resp, errnoToError(resp.Errno)
		}
		return resp, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Init performs the FUSE_INIT handshake verifying tenant/volume identity and
// enforcing no_guest_priv + no_gofer_hop flags.
func (c *GuestFUSEClient) Init(ctx context.Context, req InitRequest) (InitResponse, error) {
	if req.Major == 0 {
		req.Major = FUSEProtocolMajor
		req.Minor = FUSEProtocolMinor
	}
	if req.MaxReadAhead == 0 {
		req.MaxReadAhead = DefaultMaxReadAhead
	}
	resp, err := c.call(ctx, &Frame{
		Opcode:  OpInit,
		Payload: encodeInitReq(req),
	})
	if err != nil {
		return InitResponse{}, err
	}
	return decodeInitResp(resp.Payload)
}

// Lookup resolves name under parent inode via FUSE_LOOKUP.
func (c *GuestFUSEClient) Lookup(ctx context.Context, parent uint64, name string) (Attr, error) {
	resp, err := c.call(ctx, &Frame{
		Opcode:  OpLookup,
		NodeID:  parent,
		Payload: []byte(name),
	})
	if err != nil {
		return Attr{}, err
	}
	return decodeAttr(resp.Payload)
}

// Getattr retrieves inode attributes via FUSE_GETATTR.
func (c *GuestFUSEClient) Getattr(ctx context.Context, nodeID uint64) (Attr, error) {
	resp, err := c.call(ctx, &Frame{
		Opcode: OpGetattr,
		NodeID: nodeID,
	})
	if err != nil {
		return Attr{}, err
	}
	return decodeAttr(resp.Payload)
}

// Create creates and opens a regular file under parent via FUSE_CREATE.
func (c *GuestFUSEClient) Create(ctx context.Context, parent uint64, name string, perm fs.FileMode) (Attr, uint64, error) {
	resp, err := c.call(ctx, &Frame{
		Opcode:  OpCreate,
		NodeID:  parent,
		Aux:     uint64(perm),
		Payload: []byte(name),
	})
	if err != nil {
		return Attr{}, 0, err
	}
	a, err := decodeAttr(resp.Payload)
	if err != nil {
		return Attr{}, 0, err
	}
	return a, resp.Handle, nil
}

// Mkdir creates a subdirectory under parent via FUSE_MKDIR.
func (c *GuestFUSEClient) Mkdir(ctx context.Context, parent uint64, name string, perm fs.FileMode) (Attr, error) {
	resp, err := c.call(ctx, &Frame{
		Opcode:  OpMkdir,
		NodeID:  parent,
		Aux:     uint64(perm),
		Payload: []byte(name),
	})
	if err != nil {
		return Attr{}, err
	}
	return decodeAttr(resp.Payload)
}

// Unlink removes name from parent via FUSE_UNLINK.
func (c *GuestFUSEClient) Unlink(ctx context.Context, parent uint64, name string) error {
	_, err := c.call(ctx, &Frame{
		Opcode:  OpUnlink,
		NodeID:  parent,
		Payload: []byte(name),
	})
	return err
}

// Open opens an existing regular file inode via FUSE_OPEN.
func (c *GuestFUSEClient) Open(ctx context.Context, nodeID uint64) (uint64, error) {
	resp, err := c.call(ctx, &Frame{
		Opcode: OpOpen,
		NodeID: nodeID,
	})
	if err != nil {
		return 0, err
	}
	return resp.Handle, nil
}

// Read reads up to size bytes at off via FUSE_READ.
func (c *GuestFUSEClient) Read(ctx context.Context, handle uint64, off uint64, size uint32) ([]byte, error) {
	resp, err := c.call(ctx, &Frame{
		Opcode:  OpRead,
		Handle:  handle,
		Payload: encodeReadPayload(off, size),
	})
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// Write writes data at off via FUSE_WRITE.
func (c *GuestFUSEClient) Write(ctx context.Context, handle uint64, off uint64, data []byte) (int, error) {
	resp, err := c.call(ctx, &Frame{
		Opcode:  OpWrite,
		Handle:  handle,
		Aux:     off,
		Payload: data,
	})
	if err != nil {
		return 0, err
	}
	return int(resp.Aux), nil
}

// Fsync synchronizes the file handle (or inode/volume) to the node's local
// journal via FUSE_FSYNC (zero synchronous S3 requests).
func (c *GuestFUSEClient) Fsync(ctx context.Context, nodeID, handle uint64) error {
	_, err := c.call(ctx, &Frame{
		Opcode: OpFsync,
		NodeID: nodeID,
		Handle: handle,
	})
	return err
}

// Flush executes FUSE_FLUSH before closing a descriptor (zero synchronous S3 requests).
func (c *GuestFUSEClient) Flush(ctx context.Context, handle uint64) error {
	_, err := c.call(ctx, &Frame{
		Opcode: OpFlush,
		Handle: handle,
	})
	return err
}

// Release closes the handle on the server via FUSE_RELEASE (zero synchronous S3 requests).
func (c *GuestFUSEClient) Release(ctx context.Context, handle uint64) error {
	_, err := c.call(ctx, &Frame{
		Opcode: OpRelease,
		Handle: handle,
	})
	return err
}

// Statfs queries volume space and inode counters via FUSE_STATFS.
func (c *GuestFUSEClient) Statfs(ctx context.Context) (StatfsInfo, error) {
	resp, err := c.call(ctx, &Frame{
		Opcode: OpStatfs,
	})
	if err != nil {
		return StatfsInfo{}, err
	}
	return decodeStatfs(resp.Payload)
}

// WriteAll writes a large buffer across multiple FUSE_WRITE calls bounded by maxWrite.
func (c *GuestFUSEClient) WriteAll(ctx context.Context, handle uint64, off uint64, data []byte, maxWrite int) (int, error) {
	if maxWrite <= 0 {
		maxWrite = int(DefaultMaxWrite)
	}
	written := 0
	for written < len(data) {
		chunk := data[written:min(written+maxWrite, len(data))]
		n, err := c.Write(ctx, handle, off+uint64(written), chunk)
		if err != nil {
			return written, err
		}
		if n != len(chunk) {
			return written + n, fmt.Errorf("%w: short write %d != %d", io.ErrShortWrite, n, len(chunk))
		}
		written += n
	}
	return written, nil
}

// ReadAll reads exactLen bytes across multiple FUSE_READ calls bounded by chunkSize.
func (c *GuestFUSEClient) ReadAll(ctx context.Context, handle uint64, off uint64, exactLen int, chunkSize int) ([]byte, error) {
	if chunkSize <= 0 {
		chunkSize = int(DefaultMaxWrite)
	}
	out := make([]byte, 0, exactLen)
	for len(out) < exactLen {
		want := uint32(min(chunkSize, exactLen-len(out)))
		b, err := c.Read(ctx, handle, off+uint64(len(out)), want)
		if err != nil {
			return out, err
		}
		if len(b) == 0 {
			return out, errors.Join(io.ErrUnexpectedEOF)
		}
		out = append(out, b...)
	}
	return out, nil
}
