// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && unix

package visorfuse

import (
	"fmt"
	"syscall"
)

// PreferredSocketKind returns SOCK_STREAM on non-Linux Unix systems (e.g.
// macOS/Darwin) where AF_UNIX SOCK_SEQPACKET is unavailable.
func PreferredSocketKind() SocketKind {
	return SocketKindStream
}

func unixSocketPairFDs(kind SocketKind) ([2]int, error) {
	switch kind {
	case SocketKindStream:
		fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
		if err != nil {
			return [2]int{-1, -1}, err
		}
		syscall.CloseOnExec(fds[0])
		syscall.CloseOnExec(fds[1])
		return fds, nil
	case SocketKindSeqpacket:
		return [2]int{-1, -1}, fmt.Errorf("%w: SOCK_SEQPACKET is unsupported on this OS; use SocketKindStream", ErrInvalidMountConfig)
	default:
		return [2]int{-1, -1}, ErrInvalidMountConfig
	}
}
