// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package visorfuse

import "syscall"

// PreferredSocketKind returns SOCK_SEQPACKET on Linux where AF_UNIX
// SOCK_SEQPACKET is natively supported by the kernel.
func PreferredSocketKind() SocketKind {
	return SocketKindSeqpacket
}

func unixSocketPairFDs(kind SocketKind) ([2]int, error) {
	switch kind {
	case SocketKindSeqpacket:
		return syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	case SocketKindStream:
		return syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	default:
		return [2]int{-1, -1}, ErrInvalidMountConfig
	}
}
