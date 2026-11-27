// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package visorfuse

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanzoai/vfs"
)

const (
	// WireMagic identifies a Visor external FUSE frame ("VFUS").
	WireMagic uint32 = 0x56465553

	// WireHeaderSize is the fixed binary header size in bytes (56 bytes).
	WireHeaderSize = 56

	// MaxFrameBytes bounds a single wire frame to prevent unbounded allocations.
	MaxFrameBytes = 8 << 20

	// seqpacketMaxPacket bounds each individual SOCK_SEQPACKET datagram so that
	// tests and sandboxes succeed even on kernels with default 208 KiB wmem_max.
	seqpacketMaxPacket = 128 << 10

	// FUSEProtocolMajor and FUSEProtocolMinor align with modern Linux FUSE ABI.
	FUSEProtocolMajor uint32 = 7
	FUSEProtocolMinor uint32 = 38
)

// Opcode identifies a FUSE wire operation on the host socket FD.
// Values align with Linux kernel <linux/fuse.h> opcodes.
type Opcode uint32

const (
	OpLookup  Opcode = 1  // FUSE_LOOKUP
	OpGetattr Opcode = 3  // FUSE_GETATTR
	OpMkdir   Opcode = 9  // FUSE_MKDIR
	OpUnlink  Opcode = 10 // FUSE_UNLINK
	OpOpen    Opcode = 14 // FUSE_OPEN
	OpRead    Opcode = 15 // FUSE_READ
	OpWrite   Opcode = 16 // FUSE_WRITE
	OpStatfs  Opcode = 17 // FUSE_STATFS
	OpRelease Opcode = 18 // FUSE_RELEASE
	OpFsync   Opcode = 20 // FUSE_FSYNC
	OpFlush   Opcode = 25 // FUSE_FLUSH
	OpInit    Opcode = 26 // FUSE_INIT
	OpCreate  Opcode = 35 // FUSE_CREATE
)

func (op Opcode) String() string {
	switch op {
	case OpLookup:
		return "FUSE_LOOKUP"
	case OpGetattr:
		return "FUSE_GETATTR"
	case OpMkdir:
		return "FUSE_MKDIR"
	case OpUnlink:
		return "FUSE_UNLINK"
	case OpOpen:
		return "FUSE_OPEN"
	case OpRead:
		return "FUSE_READ"
	case OpWrite:
		return "FUSE_WRITE"
	case OpStatfs:
		return "FUSE_STATFS"
	case OpRelease:
		return "FUSE_RELEASE"
	case OpFsync:
		return "FUSE_FSYNC"
	case OpFlush:
		return "FUSE_FLUSH"
	case OpInit:
		return "FUSE_INIT"
	case OpCreate:
		return "FUSE_CREATE"
	default:
		return fmt.Sprintf("FUSE_OP(%d)", uint32(op))
	}
}

// Frame is a single binary request or response transmitted over the host
// socket FD. Using a compact fixed header eliminates JSON marshaling overhead
// on the FUSE data path.
//
// Wire layout (56 bytes, little-endian):
//
//	[0:4]   Magic (0x56465553)
//	[4:8]   FrameLen (WireHeaderSize + len(Payload))
//	[8:12]  Opcode
//	[12:16] Flags
//	[16:24] Unique
//	[24:32] NodeID
//	[32:40] Handle
//	[40:44] Errno (int32)
//	[44:48] Reserved (0)
//	[48:56] Aux (offset / size / mode / bytes written)
//	[56:]   Payload
type Frame struct {
	Opcode  Opcode
	Flags   uint32
	Unique  uint64
	NodeID  uint64
	Handle  uint64
	Errno   int32
	Aux     uint64
	Payload []byte
}

func encodeFrame(f *Frame) ([]byte, error) {
	total := WireHeaderSize + len(f.Payload)
	if total > MaxFrameBytes {
		return nil, fmt.Errorf("%w: frame size %d exceeds %d", ErrProtocolViolation, total, MaxFrameBytes)
	}
	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf[0:4], WireMagic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(total))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(f.Opcode))
	binary.LittleEndian.PutUint32(buf[12:16], f.Flags)
	binary.LittleEndian.PutUint64(buf[16:24], f.Unique)
	binary.LittleEndian.PutUint64(buf[24:32], f.NodeID)
	binary.LittleEndian.PutUint64(buf[32:40], f.Handle)
	binary.LittleEndian.PutUint32(buf[40:44], uint32(f.Errno))
	binary.LittleEndian.PutUint32(buf[44:48], 0)
	binary.LittleEndian.PutUint64(buf[48:56], f.Aux)
	copy(buf[WireHeaderSize:], f.Payload)
	return buf, nil
}

func decodeHeader(hdr []byte) (Frame, int, error) {
	if len(hdr) < WireHeaderSize {
		return Frame{}, 0, fmt.Errorf("%w: short header (%d bytes)", ErrProtocolViolation, len(hdr))
	}
	magic := binary.LittleEndian.Uint32(hdr[0:4])
	if magic != WireMagic {
		return Frame{}, 0, fmt.Errorf("%w: invalid magic %#x", ErrProtocolViolation, magic)
	}
	total := int(binary.LittleEndian.Uint32(hdr[4:8]))
	if total < WireHeaderSize || total > MaxFrameBytes {
		return Frame{}, 0, fmt.Errorf("%w: invalid frame length %d", ErrProtocolViolation, total)
	}
	f := Frame{
		Opcode: Opcode(binary.LittleEndian.Uint32(hdr[8:12])),
		Flags:  binary.LittleEndian.Uint32(hdr[12:16]),
		Unique: binary.LittleEndian.Uint64(hdr[16:24]),
		NodeID: binary.LittleEndian.Uint64(hdr[24:32]),
		Handle: binary.LittleEndian.Uint64(hdr[32:40]),
		Errno:  int32(binary.LittleEndian.Uint32(hdr[40:44])),
		Aux:    binary.LittleEndian.Uint64(hdr[48:56]),
	}
	return f, total - WireHeaderSize, nil
}

// FrameConn wraps an io.ReadWriteCloser with framing semantics appropriate for
// either SOCK_SEQPACKET (packet-preserving) or SOCK_STREAM/pipe transports.
type FrameConn struct {
	rw   io.ReadWriteCloser
	kind SocketKind

	wmu  sync.Mutex
	rmu  sync.Mutex
	rbuf []byte // scratch buffer for SOCK_SEQPACKET reads
}

// NewFrameConn constructs a framed transport wrapper over rw.
func NewFrameConn(rw io.ReadWriteCloser, kind SocketKind) *FrameConn {
	fc := &FrameConn{rw: rw, kind: kind}
	if kind == SocketKindSeqpacket {
		fc.rbuf = make([]byte, seqpacketMaxPacket)
	}
	return fc
}

// Close closes the underlying connection.
func (fc *FrameConn) Close() error { return fc.rw.Close() }

// WriteFrame encodes and transmits f atomically with respect to concurrent writers.
func (fc *FrameConn) WriteFrame(f *Frame) error {
	raw, err := encodeFrame(f)
	if err != nil {
		return err
	}
	fc.wmu.Lock()
	defer fc.wmu.Unlock()
	if fc.kind == SocketKindSeqpacket {
		for off := 0; off < len(raw); {
			end := min(off+seqpacketMaxPacket, len(raw))
			n, err := fc.rw.Write(raw[off:end])
			if err != nil {
				return err
			}
			if n != end-off {
				return io.ErrShortWrite
			}
			off = end
		}
		return nil
	}
	return writeAll(fc.rw, raw)
}

// ReadFrame reads the next complete binary FUSE frame from the connection.
func (fc *FrameConn) ReadFrame() (*Frame, error) {
	fc.rmu.Lock()
	defer fc.rmu.Unlock()

	if fc.kind == SocketKindSeqpacket {
		n, err := fc.rw.Read(fc.rbuf)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.EOF
		}
		f, payLen, err := decodeHeader(fc.rbuf[:n])
		if err != nil {
			return nil, err
		}
		if payLen == 0 {
			return &f, nil
		}
		payload := make([]byte, payLen)
		copied := copy(payload, fc.rbuf[WireHeaderSize:n])
		for copied < payLen {
			m, err := fc.rw.Read(fc.rbuf)
			if err != nil {
				return nil, err
			}
			if m == 0 {
				return nil, io.ErrUnexpectedEOF
			}
			need := payLen - copied
			if m > need {
				return nil, fmt.Errorf("%w: trailing bytes in seqpacket frame", ErrProtocolViolation)
			}
			copy(payload[copied:copied+m], fc.rbuf[:m])
			copied += m
		}
		f.Payload = payload
		return &f, nil
	}

	var hdr [WireHeaderSize]byte
	if _, err := io.ReadFull(fc.rw, hdr[:]); err != nil {
		return nil, err
	}
	f, payLen, err := decodeHeader(hdr[:])
	if err != nil {
		return nil, err
	}
	if payLen > 0 {
		f.Payload = make([]byte, payLen)
		if _, err := io.ReadFull(fc.rw, f.Payload); err != nil {
			return nil, err
		}
	}
	return &f, nil
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

// InitFlags advertised during FUSE_INIT negotiation.
const (
	InitFlagNoGuestPriv uint32 = 1 << 0
	InitFlagNoGoferHop  uint32 = 1 << 1
	InitFlagAsyncRead   uint32 = 1 << 2
	InitFlagBigWrites   uint32 = 1 << 3
	InitFlagWriteback   uint32 = 1 << 4
)

// InitRequest is sent by the guest client on FUSE_INIT.
type InitRequest struct {
	Major        uint32
	Minor        uint32
	MaxReadAhead uint32
	Flags        uint32
	TenantID     string
	VolumeID     string
}

// InitResponse is returned by the host server on FUSE_INIT.
type InitResponse struct {
	Major        uint32
	Minor        uint32
	MaxReadAhead uint32
	MaxWrite     uint32
	Flags        uint32
	Round        uint64
	HeadSeq      uint64
}

func encodeInitReq(r InitRequest) []byte {
	tb := []byte(r.TenantID)
	vb := []byte(r.VolumeID)
	out := make([]byte, 20+len(tb)+len(vb))
	binary.LittleEndian.PutUint32(out[0:4], r.Major)
	binary.LittleEndian.PutUint32(out[4:8], r.Minor)
	binary.LittleEndian.PutUint32(out[8:12], r.MaxReadAhead)
	binary.LittleEndian.PutUint32(out[12:16], r.Flags)
	binary.LittleEndian.PutUint16(out[16:18], uint16(len(tb)))
	binary.LittleEndian.PutUint16(out[18:20], uint16(len(vb)))
	copy(out[20:], tb)
	copy(out[20+len(tb):], vb)
	return out
}

func decodeInitReq(b []byte) (InitRequest, error) {
	if len(b) < 20 {
		return InitRequest{}, fmt.Errorf("%w: short FUSE_INIT request", ErrProtocolViolation)
	}
	tlen := int(binary.LittleEndian.Uint16(b[16:18]))
	vlen := int(binary.LittleEndian.Uint16(b[18:20]))
	if len(b) != 20+tlen+vlen {
		return InitRequest{}, fmt.Errorf("%w: malformed FUSE_INIT length", ErrProtocolViolation)
	}
	return InitRequest{
		Major:        binary.LittleEndian.Uint32(b[0:4]),
		Minor:        binary.LittleEndian.Uint32(b[4:8]),
		MaxReadAhead: binary.LittleEndian.Uint32(b[8:12]),
		Flags:        binary.LittleEndian.Uint32(b[12:16]),
		TenantID:     string(b[20 : 20+tlen]),
		VolumeID:     string(b[20+tlen : 20+tlen+vlen]),
	}, nil
}

func encodeInitResp(r InitResponse) []byte {
	out := make([]byte, 36)
	binary.LittleEndian.PutUint32(out[0:4], r.Major)
	binary.LittleEndian.PutUint32(out[4:8], r.Minor)
	binary.LittleEndian.PutUint32(out[8:12], r.MaxReadAhead)
	binary.LittleEndian.PutUint32(out[12:16], r.MaxWrite)
	binary.LittleEndian.PutUint32(out[16:20], r.Flags)
	binary.LittleEndian.PutUint64(out[20:28], r.Round)
	binary.LittleEndian.PutUint64(out[28:36], r.HeadSeq)
	return out
}

func decodeInitResp(b []byte) (InitResponse, error) {
	if len(b) != 36 {
		return InitResponse{}, fmt.Errorf("%w: malformed FUSE_INIT response", ErrProtocolViolation)
	}
	return InitResponse{
		Major:        binary.LittleEndian.Uint32(b[0:4]),
		Minor:        binary.LittleEndian.Uint32(b[4:8]),
		MaxReadAhead: binary.LittleEndian.Uint32(b[8:12]),
		MaxWrite:     binary.LittleEndian.Uint32(b[12:16]),
		Flags:        binary.LittleEndian.Uint32(b[16:20]),
		Round:        binary.LittleEndian.Uint64(b[20:28]),
		HeadSeq:      binary.LittleEndian.Uint64(b[28:36]),
	}, nil
}

// Attr is the compact FUSE attribute response for LOOKUP/GETATTR/CREATE/MKDIR.
type Attr struct {
	Inode   uint64
	Size    uint64
	Mode    fs.FileMode
	Nlink   uint32
	UID     uint32
	GID     uint32
	MtimeNS int64
}

func attrFromInode(in vfs.Inode) Attr {
	return Attr{
		Inode:   uint64(in.ID),
		Size:    in.Size,
		Mode:    in.Mode,
		Nlink:   in.Nlink,
		UID:     in.UID,
		GID:     in.GID,
		MtimeNS: in.Mtime.UnixNano(),
	}
}

func encodeAttr(a Attr) []byte {
	out := make([]byte, 40)
	binary.LittleEndian.PutUint64(out[0:8], a.Inode)
	binary.LittleEndian.PutUint64(out[8:16], a.Size)
	binary.LittleEndian.PutUint32(out[16:20], uint32(a.Mode))
	binary.LittleEndian.PutUint32(out[20:24], a.Nlink)
	binary.LittleEndian.PutUint32(out[24:28], a.UID)
	binary.LittleEndian.PutUint32(out[28:32], a.GID)
	binary.LittleEndian.PutUint64(out[32:40], uint64(a.MtimeNS))
	return out
}

func decodeAttr(b []byte) (Attr, error) {
	if len(b) != 40 {
		return Attr{}, fmt.Errorf("%w: malformed attr (%d bytes)", ErrProtocolViolation, len(b))
	}
	return Attr{
		Inode:   binary.LittleEndian.Uint64(b[0:8]),
		Size:    binary.LittleEndian.Uint64(b[8:16]),
		Mode:    fs.FileMode(binary.LittleEndian.Uint32(b[16:20])),
		Nlink:   binary.LittleEndian.Uint32(b[20:24]),
		UID:     binary.LittleEndian.Uint32(b[24:28]),
		GID:     binary.LittleEndian.Uint32(b[28:32]),
		MtimeNS: int64(binary.LittleEndian.Uint64(b[32:40])),
	}, nil
}

// StatfsInfo is returned by FUSE_STATFS.
type StatfsInfo struct {
	Blocks  uint64
	Bfree   uint64
	Bavail  uint64
	Files   uint64
	Ffree   uint64
	Bsize   uint32
	Namelen uint32
}

func encodeStatfs(s StatfsInfo) []byte {
	out := make([]byte, 48)
	binary.LittleEndian.PutUint64(out[0:8], s.Blocks)
	binary.LittleEndian.PutUint64(out[8:16], s.Bfree)
	binary.LittleEndian.PutUint64(out[16:24], s.Bavail)
	binary.LittleEndian.PutUint64(out[24:32], s.Files)
	binary.LittleEndian.PutUint64(out[32:40], s.Ffree)
	binary.LittleEndian.PutUint32(out[40:44], s.Bsize)
	binary.LittleEndian.PutUint32(out[44:48], s.Namelen)
	return out
}

func decodeStatfs(b []byte) (StatfsInfo, error) {
	if len(b) != 48 {
		return StatfsInfo{}, fmt.Errorf("%w: malformed statfs (%d bytes)", ErrProtocolViolation, len(b))
	}
	return StatfsInfo{
		Blocks:  binary.LittleEndian.Uint64(b[0:8]),
		Bfree:   binary.LittleEndian.Uint64(b[8:16]),
		Bavail:  binary.LittleEndian.Uint64(b[16:24]),
		Files:   binary.LittleEndian.Uint64(b[24:32]),
		Ffree:   binary.LittleEndian.Uint64(b[32:40]),
		Bsize:   binary.LittleEndian.Uint32(b[40:44]),
		Namelen: binary.LittleEndian.Uint32(b[44:48]),
	}, nil
}

// encodeReadPayload packs (offset uint64, size uint32) for FUSE_READ.
func encodeReadPayload(off uint64, size uint32) []byte {
	out := make([]byte, 12)
	binary.LittleEndian.PutUint64(out[0:8], off)
	binary.LittleEndian.PutUint32(out[8:12], size)
	return out
}

func decodeReadPayload(b []byte) (off uint64, size uint32, err error) {
	if len(b) != 12 {
		return 0, 0, fmt.Errorf("%w: malformed FUSE_READ payload", ErrProtocolViolation)
	}
	return binary.LittleEndian.Uint64(b[0:8]), binary.LittleEndian.Uint32(b[8:12]), nil
}

// mapErrno translates VFS/OS errors to negative POSIX errno values on the wire.
func mapErrno(err error) int32 {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, fs.ErrNotExist), errors.Is(err, os.ErrNotExist):
		return -int32(syscall.ENOENT)
	case errors.Is(err, fs.ErrExist), errors.Is(err, os.ErrExist):
		return -int32(syscall.EEXIST)
	case errors.Is(err, vfs.ErrIsDir):
		return -int32(syscall.EISDIR)
	case errors.Is(err, vfs.ErrNotDir):
		return -int32(syscall.ENOTDIR)
	case errors.Is(err, vfs.ErrNotEmpty):
		return -int32(syscall.ENOTEMPTY)
	case errors.Is(err, vfs.ErrReadOnly), errors.Is(err, vfs.ErrJournal):
		return -int32(syscall.EROFS)
	case errors.Is(err, vfs.ErrFull), errors.Is(err, vfs.ErrNoSpace):
		return -int32(syscall.ENOSPC)
	case errors.Is(err, fs.ErrInvalid):
		return -int32(syscall.EINVAL)
	case errors.Is(err, ErrTenantMismatch), errors.Is(err, ErrVolumeMismatch), errors.Is(err, ErrGuestPrivilegeForbidden):
		return -int32(syscall.EACCES)
	case errors.Is(err, ErrUnquiescedVolume):
		return -int32(syscall.EBUSY)
	default:
		return -int32(syscall.EIO)
	}
}

func errnoToError(e int32) error {
	if e == 0 {
		return nil
	}
	code := syscall.Errno(-e)
	switch code {
	case syscall.ENOENT:
		return fmt.Errorf("%w: %v", fs.ErrNotExist, code)
	case syscall.EEXIST:
		return fmt.Errorf("%w: %v", fs.ErrExist, code)
	case syscall.EINVAL:
		return fmt.Errorf("%w: %v", fs.ErrInvalid, code)
	case syscall.EACCES:
		return fmt.Errorf("%w: %v", fs.ErrPermission, code)
	default:
		return code
	}
}

// ModTime converts Attr.MtimeNS into a UTC time.Time.
func (a Attr) ModTime() time.Time { return time.Unix(0, a.MtimeNS).UTC() }
