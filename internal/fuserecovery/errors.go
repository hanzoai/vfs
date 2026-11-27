// Package fuserecovery implements clean-room, Ananke-inspired transparent crash
// recovery for a host-side FUSE filesystem daemon.
//
// Unlike storage-journal replay (which reconstructs volume state after a full
// remount), this package recovers the live filesystem service itself: the
// active mount session, inode lookup table (nlookup refcounts & generations),
// open file handles (including unlinked-but-open orphan files), idempotent
// FUSE request deduplication watermarks, and locally acknowledged fsync
// barriers across process-only daemon crashes (SIGKILL equivalent).
package fuserecovery

import (
	"errors"
	"syscall"
)

var (
	// ErrStaleInode indicates an inode was deleted or its generation changed
	// (POSIX ESTALE).
	ErrStaleInode = errors.New("fuserecovery: stale inode")

	// ErrInvalidHandle indicates a file handle is unknown or already closed
	// (POSIX EBADF).
	ErrInvalidHandle = errors.New("fuserecovery: invalid file handle")

	// ErrReadOnlyHandle indicates a write/fsync mutation was attempted on a
	// handle opened read-only (POSIX EBADF).
	ErrReadOnlyHandle = errors.New("fuserecovery: handle is read-only")

	// ErrTenantMismatch indicates a caller attempted to access an inode or open
	// handle owned by a different tenant (POSIX EACCES).
	ErrTenantMismatch = errors.New("fuserecovery: tenant isolation violation")

	// ErrSessionCorrupt indicates the persisted daemon state snapshot failed
	// magic, version, length, or CRC32C integrity validation.
	ErrSessionCorrupt = errors.New("fuserecovery: corrupt session state")

	// ErrPathEscape indicates a relative path or symlink attempted to escape
	// the confined state or backing root directory.
	ErrPathEscape = errors.New("fuserecovery: path escapes confined root")

	// ErrHandoffUnavailable indicates the supervisor cannot hand off a live
	// /dev/fuse session descriptor (e.g. kernel/host crash or handoff disabled),
	// requiring a degraded cold remount.
	ErrHandoffUnavailable = errors.New("fuserecovery: fuse fd handoff unavailable")

	// ErrDaemonCrashed indicates the active FUSE daemon process terminated
	// abruptly and has not yet been respawned by the supervisor.
	ErrDaemonCrashed = errors.New("fuserecovery: daemon process crashed")

	// ErrNotFound indicates a directory entry or file does not exist (POSIX ENOENT).
	ErrNotFound = errors.New("fuserecovery: not found")

	// ErrAlreadyExists indicates an exclusive create collided with an existing
	// path (POSIX EEXIST).
	ErrAlreadyExists = errors.New("fuserecovery: already exists")
)

// Errno maps fuserecovery sentinel errors to their canonical POSIX syscall.Errno.
func Errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, ErrStaleInode):
		return syscall.ESTALE
	case errors.Is(err, ErrInvalidHandle), errors.Is(err, ErrReadOnlyHandle):
		return syscall.EBADF
	case errors.Is(err, ErrTenantMismatch), errors.Is(err, ErrPathEscape):
		return syscall.EACCES
	case errors.Is(err, ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, ErrAlreadyExists):
		return syscall.EEXIST
	case errors.Is(err, ErrDaemonCrashed), errors.Is(err, ErrHandoffUnavailable):
		return syscall.ENOTCONN
	default:
		var errno syscall.Errno
		if errors.As(err, &errno) {
			return errno
		}
		return syscall.EIO
	}
}
