//go:build fuse && linux

package mount

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"syscall"
	"testing"

	"github.com/hanzoai/vfs"
)

// TestErrnoMapping: what a Volume says is what the kernel is told, wrapped or not. A Volume
// that was fenced out answers EROFS, as one that was only opened does: the mount is read-only
// from then on, and not "I/O error" with a message on stderr.
func TestErrnoMapping(t *testing.T) {
	for _, c := range []struct {
		err  error
		want error
	}{
		{nil, nil},
		{fs.ErrNotExist, syscall.ENOENT},
		{fs.ErrExist, syscall.EEXIST},
		{vfs.ErrNotDir, syscall.ENOTDIR},
		{vfs.ErrIsDir, syscall.EISDIR},
		{vfs.ErrNotEmpty, syscall.ENOTEMPTY},
		{fs.ErrInvalid, syscall.EINVAL},
		{vfs.ErrTooBig, syscall.EFBIG},
		{fs.ErrPermission, syscall.EPERM},
		{vfs.ErrNameTooLong, syscall.ENAMETOOLONG},
		{fmt.Errorf("%w: 300 bytes: %w", vfs.ErrNameTooLong, fs.ErrInvalid), syscall.ENAMETOOLONG},
		{vfs.ErrNoSpace, syscall.ENOSPC},
		{vfs.ErrTooManyLinks, syscall.EMLINK},
		{vfs.ErrReadOnly, syscall.EROFS},
		{vfs.ErrStaleRound, syscall.EROFS},
		{vfs.ErrFull, syscall.ENOSPC},
		{fmt.Errorf("%w: memory holds 1 bytes", vfs.ErrFull), syscall.ENOSPC},
		{fmt.Errorf("%w: disk", vfs.ErrJournal), syscall.EROFS},
		{vfs.ErrClosed, syscall.EROFS},
		{fmt.Errorf("vfs: commit: %w", vfs.ErrStaleRound), syscall.EROFS},
		{context.Canceled, syscall.EINTR},
		{fmt.Errorf("read: %w", context.Canceled), syscall.EINTR},
		{context.DeadlineExceeded, syscall.ETIMEDOUT},
		{errors.New("the store is down"), syscall.EIO},
		{vfs.ErrCorrupt, syscall.EIO},
	} {
		if got := errno(c.err); got != c.want {
			t.Errorf("errno(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
