// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package visorfuse

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
)

// SocketPair wraps the two endpoints of a host-created socket or pipe pair
// connecting a Visor guest FUSE client directly to the host ExternalFUSEServer.
type SocketPair struct {
	GuestConn    io.ReadWriteCloser
	HostConn     io.ReadWriteCloser
	GuestFD      int
	HostFD       int
	Kind         SocketKind
	guestRawFile *os.File
	hostRawFile  *os.File
}

// Close closes both ends of the pair.
func (p *SocketPair) Close() error {
	var errs []error
	if p.GuestConn != nil {
		errs = append(errs, p.GuestConn.Close())
	}
	if p.HostConn != nil {
		errs = append(errs, p.HostConn.Close())
	}
	if p.guestRawFile != nil {
		errs = append(errs, p.guestRawFile.Close())
	}
	if p.hostRawFile != nil {
		errs = append(errs, p.hostRawFile.Close())
	}
	return errors.Join(errs...)
}

// NewUnixSocketPair allocates a real kernel AF_UNIX socket pair (SOCK_SEQPACKET
// on Linux or SOCK_STREAM on Linux/Darwin), attaches both endpoints to Go's
// netpoller via net.FileConn, and returns the live host and guest descriptors.
func NewUnixSocketPair(kind SocketKind) (*SocketPair, error) {
	fds, err := unixSocketPairFDs(kind)
	if err != nil {
		return nil, fmt.Errorf("visorfuse: socketpair(%s): %w", kind, err)
	}
	// Keep the original FDs valid long enough to hand to net.FileConn, which dups
	// them onto non-blocking netpoller-managed UnixConns.
	guestFile := os.NewFile(uintptr(fds[0]), "visorfuse-guest-sock")
	hostFile := os.NewFile(uintptr(fds[1]), "visorfuse-host-sock")

	guestConn, err := net.FileConn(guestFile)
	if err != nil {
		_ = guestFile.Close()
		_ = hostFile.Close()
		return nil, fmt.Errorf("visorfuse: guest FileConn: %w", err)
	}
	hostConn, err := net.FileConn(hostFile)
	if err != nil {
		_ = guestConn.Close()
		_ = guestFile.Close()
		_ = hostFile.Close()
		return nil, fmt.Errorf("visorfuse: host FileConn: %w", err)
	}
	guestFD := fds[0]
	hostFD := fds[1]
	if uc, ok := guestConn.(*net.UnixConn); ok {
		if raw, err := uc.SyscallConn(); err == nil {
			_ = raw.Control(func(fd uintptr) { guestFD = int(fd) })
		}
	}
	if uc, ok := hostConn.(*net.UnixConn); ok {
		if raw, err := uc.SyscallConn(); err == nil {
			_ = raw.Control(func(fd uintptr) { hostFD = int(fd) })
		}
	}
	// Close the temporary wrappers now that net.FileConn holds the dup'd FDs.
	_ = guestFile.Close()
	_ = hostFile.Close()

	// Tune socket buffers when possible for high-throughput burst I/O.
	if uc, ok := guestConn.(*net.UnixConn); ok {
		_ = uc.SetReadBuffer(4 << 20)
		_ = uc.SetWriteBuffer(4 << 20)
	}
	if uc, ok := hostConn.(*net.UnixConn); ok {
		_ = uc.SetReadBuffer(4 << 20)
		_ = uc.SetWriteBuffer(4 << 20)
	}

	return &SocketPair{
		GuestConn: guestConn,
		HostConn:  hostConn,
		GuestFD:   guestFD,
		HostFD:    hostFD,
		Kind:      kind,
	}, nil
}

// duplexPipe combines a read pipe and a write pipe into an io.ReadWriteCloser.
type duplexPipe struct {
	r *os.File
	w *os.File
}

func (d *duplexPipe) Read(p []byte) (int, error)  { return d.r.Read(p) }
func (d *duplexPipe) Write(p []byte) (int, error) { return d.w.Write(p) }
func (d *duplexPipe) Close() error                { return errors.Join(d.w.Close(), d.r.Close()) }

// NewPipePair creates a full-duplex pair of real OS pipes (os.Pipe) suitable
// for testing or sandboxes passing pipe FDs instead of AF_UNIX sockets.
func NewPipePair() (*SocketPair, error) {
	g2hR, g2hW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("visorfuse: os.Pipe g2h: %w", err)
	}
	h2gR, h2gW, err := os.Pipe()
	if err != nil {
		_ = g2hR.Close()
		_ = g2hW.Close()
		return nil, fmt.Errorf("visorfuse: os.Pipe h2g: %w", err)
	}
	syscall.CloseOnExec(int(g2hR.Fd()))
	syscall.CloseOnExec(int(g2hW.Fd()))
	syscall.CloseOnExec(int(h2gR.Fd()))
	syscall.CloseOnExec(int(h2gW.Fd()))
	return &SocketPair{
		GuestConn: &duplexPipe{r: h2gR, w: g2hW},
		HostConn:  &duplexPipe{r: g2hR, w: h2gW},
		GuestFD:   int(g2hW.Fd()),
		HostFD:    int(g2hR.Fd()),
		Kind:      SocketKindStream,
	}, nil
}
