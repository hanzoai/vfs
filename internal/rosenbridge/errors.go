// Package rosenbridge implements a clean-room, conditional lower-copy
// host-to-guest I/O fast path across the virtualization boundary, inspired by
// RosenBridge (FAST '26) for HIP-1345 / hanzoai/vfs#54 & #66.
//
// RosenBridge is strictly a conditional fast path layered beside—not replacing—
// the canonical gofer I/O architecture. It activates only after runtime
// profiling confirms the gofer IPC boundary is a primary bottleneck and only
// for admitted bulk/sequential/hot-file reads and writes, automatically falling
// back to the canonical gofer path for metadata, tiny I/O, cold random I/O,
// ring saturation, or tenant fairness throttling.
package rosenbridge

import (
	"errors"
	"syscall"
)

var (
	// ErrInvalidTenant indicates an empty or malformed tenant identifier.
	ErrInvalidTenant = errors.New("rosenbridge: invalid tenant id")

	// ErrTenantMismatch indicates a cross-tenant access attempt or forged tenant token.
	ErrTenantMismatch = errors.New("rosenbridge: tenant isolation violation")

	// ErrPathEscape indicates a path traversal (".."), NUL byte, or symlink escape outside tenant root.
	ErrPathEscape = errors.New("rosenbridge: path escapes tenant root")

	// ErrDescriptorOutOfBounds indicates a descriptor offset/length out of bounds or integer overflow.
	ErrDescriptorOutOfBounds = errors.New("rosenbridge: descriptor out of bounds")

	// ErrDescriptorOverlap indicates a descriptor slab range overlaps an active in-flight descriptor.
	ErrDescriptorOverlap = errors.New("rosenbridge: overlapping descriptor slab range")

	// ErrDescriptorTruncated indicates a zero-length, truncated, or length-mismatched descriptor.
	ErrDescriptorTruncated = errors.New("rosenbridge: truncated or malformed descriptor")

	// ErrRingFull indicates the tenant's shared submission ring has no free slots.
	// Callers treat this as operational backpressure and fall back to canonical gofer.
	ErrRingFull = errors.New("rosenbridge: submission ring full")

	// ErrRingClosed indicates the tenant's shared ring or transport has been closed.
	ErrRingClosed = errors.New("rosenbridge: channel closed")

	// ErrFastPathDisabled indicates the conditional fast path is disabled or unconfirmed.
	ErrFastPathDisabled = errors.New("rosenbridge: fast path disabled")

	// ErrUnsupportedOp indicates an operation not eligible for the data-plane fast path (e.g. Stat).
	ErrUnsupportedOp = errors.New("rosenbridge: operation unsupported on fast path")

	// ErrQuotaExceeded indicates a tenant exceeded its in-flight collaborative fairness byte quota.
	ErrQuotaExceeded = errors.New("rosenbridge: tenant fairness quota exceeded")

	// ErrCorruptData indicates a CRC32C checksum mismatch on a verified fast-path transfer.
	ErrCorruptData = errors.New("rosenbridge: payload checksum mismatch")
)

// IsSecurityViolation reports whether err represents a security or descriptor
// integrity violation that MUST fail closed and never silently fall back.
func IsSecurityViolation(err error) bool {
	return errors.Is(err, ErrInvalidTenant) ||
		errors.Is(err, ErrTenantMismatch) ||
		errors.Is(err, ErrPathEscape) ||
		errors.Is(err, ErrDescriptorOutOfBounds) ||
		errors.Is(err, ErrDescriptorOverlap) ||
		errors.Is(err, ErrDescriptorTruncated) ||
		errors.Is(err, ErrCorruptData)
}

// Errno maps rosenbridge sentinel errors to POSIX syscall.Errno values.
func Errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, ErrTenantMismatch), errors.Is(err, ErrPathEscape):
		return syscall.EACCES
	case errors.Is(err, ErrInvalidTenant), errors.Is(err, ErrDescriptorTruncated):
		return syscall.EINVAL
	case errors.Is(err, ErrDescriptorOutOfBounds), errors.Is(err, ErrDescriptorOverlap):
		return syscall.EOVERFLOW
	case errors.Is(err, ErrRingFull), errors.Is(err, ErrQuotaExceeded):
		return syscall.EAGAIN
	case errors.Is(err, ErrRingClosed):
		return syscall.EPIPE
	case errors.Is(err, ErrUnsupportedOp), errors.Is(err, ErrFastPathDisabled):
		return syscall.EOPNOTSUPP
	case errors.Is(err, ErrCorruptData):
		return syscall.EIO
	default:
		var se syscall.Errno
		if errors.As(err, &se) {
			return se
		}
		return syscall.EIO
	}
}
