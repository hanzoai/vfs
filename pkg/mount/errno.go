//go:build fuse && (linux || darwin)

package mount

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/hanzoai/vfs"
)

// blockSize is the st_blksize the mount reports. It is only a hint to callers
// about a good I/O size: the Volume has no block size.
const blockSize = 4096

// space is the numbers statfs(2) returns, with no FUSE library in them.
type space struct {
	blocks, free, files, ffree uint64
	namelen                    uint32
}

// statfs turns a Volume's usage into those numbers: blocks of blockSize, against the
// capacity or, where there is none, against the largest file a Volume holds.
func statfs(u vfs.Usage) space {
	capacity := uint64(vfs.MaxSize)
	if u.Capacity > 0 {
		capacity = uint64(u.Capacity)
	}
	blocks := capacity / blockSize
	used := min(blocks, (u.Bytes+u.Pending+blockSize-1)/blockSize)
	const inodes = 1 << 40
	return space{blocks: blocks, free: blocks - used, files: inodes, ffree: inodes - min(inodes, u.Files), namelen: 255}
}

// errno maps a Volume error onto the errno the kernel hands the caller.
func errno(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, fs.ErrExist):
		return syscall.EEXIST
	case errors.Is(err, vfs.ErrNotDir):
		return syscall.ENOTDIR
	case errors.Is(err, vfs.ErrIsDir):
		return syscall.EISDIR
	case errors.Is(err, vfs.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, vfs.ErrNameTooLong):
		return syscall.ENAMETOOLONG
	case errors.Is(err, fs.ErrInvalid):
		return syscall.EINVAL
	case errors.Is(err, fs.ErrPermission):
		return syscall.EPERM
	case errors.Is(err, vfs.ErrTooBig):
		return syscall.EFBIG
	case errors.Is(err, vfs.ErrNoSpace):
		return syscall.ENOSPC
	case errors.Is(err, vfs.ErrTooManyLinks):
		return syscall.EMLINK
	case errors.Is(err, vfs.ErrReadOnly), errors.Is(err, vfs.ErrStaleRound), errors.Is(err, vfs.ErrJournal), errors.Is(err, vfs.ErrClosed):
		return syscall.EROFS
	case errors.Is(err, vfs.ErrFull):
		return syscall.ENOSPC
	case errors.Is(err, context.Canceled):
		// Only the end of the mount cancels a context here (see below): nobody is
		// waiting for the answer.
		return syscall.EINTR
	case errors.Is(err, context.DeadlineExceeded):
		return syscall.ETIMEDOUT
	}
	// The kernel is told only "I/O error"; say why, where an operator will look.
	fmt.Fprintf(os.Stderr, "vfs mount: %v\n", err)
	return syscall.EIO
}

// What an interrupt does: nothing. When a signal reaches a caller that is waiting on the
// mount (Go's runtime sends one every few milliseconds to preempt), the kernel sends the
// server FUSE_INTERRUPT and the request's context ends. No request is answered EINTR,
// because what a caller does with that answer was measured, and it is not what C
// libraries are said to do:
//
//   - fsync(2) answered EINTR is an error to SQLite (its Go port and its C library both
//     report it as an I/O error): a C caller under SIGALRM saw 246,912 EINTRs over 5,000
//     fsyncs, and `go test` of a SQLite database on the mount failed 7 times of 7 on an
//     amd64 kernel. Neither glibc nor the kernel restarts it.
//   - stat(2) and readdir answered EINTR reached a C caller as an error 1,586 times of
//     60,000 without SA_RESTART and 2,884 times with it: the kernel does not restart a
//     FUSE request that was answered EINTR.
//   - read(2) answered EINTR reaches the caller as EIO when the kernel's page cache was
//     filling the pages (20 GiB of Go reads under signals failed at the first block).
//   - close(2) is not restarted at all (Linux has dropped the descriptor; Go's
//     os.File.Close returns the error as it is).
//   - a request that changes the Volume, answered EINTR after it took effect, makes a
//     restarted create(2) with O_EXCL fail with EEXIST on its own file.
//
// So every handler runs on a context its request cannot end (context.WithoutCancel),
// and the work of a read runs on the mount's own context. A caller is always answered
// with the result of its call. The exception is three calls that change nothing, a
// lookup, a getattr and a readdir: an interrupt ends them after a grace (patient below).
//
// The cost is that a call waits for the store. There are no deadlines on the store yet,
// so a store that stops answering stops every call that needs it. The kernel keeps a
// request that reached the daemon until the daemon answers it, and a killed caller is
// freed only by an answer. Measured with the store stopped: a cold lookup (a store fetch,
// no commit running) is freed 3 s after the kill, by the grace; anything behind a stalled
// commit (stat, a lookup of an absent name, readdir, mkdir, unlink, truncate by path, fsync
// of a read-only descriptor) is not, because the Volume's lock is held across the commit
// and a lock does not honour a request's context; read, create, truncate, write and fsync
// on a descriptor are not either, and nor are close and exit behind a stalled commit (the
// exit closes the descriptor, which flushes, which waits for the commit). Killing the
// daemon frees every caller in 20 ms. The daemon holds about 134 KiB of the FUSE library's
// message buffer for each abandoned request until its handler returns, whatever the
// handler does (1,600 reads, 1,600 more through the page cache and 3,000 lookups took it
// from 16.8 MB to 831 MB). Only answering them frees them, which is what a deadline on the
// store does, and the lock needs a context for that: the next change
// (docs/SPECIFICATION.md, section 11.2).

// interruptGrace is how long a request that changes nothing keeps waiting for the store
// after its caller was interrupted, before it is answered EINTR.
const interruptGrace = 3 * time.Second

// patient returns the context a lookup, a getattr or a readdir runs on. Those three change
// nothing, so answering one EINTR cannot leave a half-done change, and a caller that does
// not restart it (a C caller of stat) loses only a call that waited longer than the grace
// for the store: an interrupt ends the context only after interruptGrace. A caller that
// was killed is then freed by the answer, where it would stay in the kernel until the
// store answered. Anything that reads, writes or commits does not use this: its answer
// is its own result (the comment above).
func patient(req context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(req))
	go func() {
		select {
		case <-req.Done():
			t := time.NewTimer(interruptGrace)
			defer t.Stop()
			select {
			case <-t.C:
				cancel()
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// finishGrace is how long the end of a mount waits for its last commit.
const finishGrace = 3 * time.Second

// finish publishes what the mount wrote once the kernel has released it. A Volume that was
// only opened has nothing to publish. The kernel's own writeback has been flushed to the
// daemon by then (stop asks for it before it unmounts), so what is left is what a holder
// wrote after, and it is small. The journal holds all of it on this node either way: a
// publication that the store does not answer in finishGrace is given up with an error that
// says what the store may not have, and the next mount of the Volume on this node
// publishes it. When Serve failed as well, both are returned.
func finish(vol *vfs.Volume, serveErr error) error {
	done := make(chan error, 1)
	go func() {
		if vol.Stats().Round == 0 {
			done <- nil
			return
		}
		done <- vol.Flush(context.Background())
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(finishGrace):
		err = fmt.Errorf("the last publication did not finish in %s: what the store lacks is in this node's journal, and the next mount of the Volume here publishes it", finishGrace)
	}
	return errors.Join(serveErr, err)
}

// unsupported is what the mount answers for an operation it does not implement:
// an honest "not supported", never a success that changed nothing.
var unsupported error = syscall.ENOSYS
