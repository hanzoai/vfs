package vfs

import (
	"context"
	"fmt"
	"io/fs"
	"time"
)

// Change is a set of attribute changes: a nil field is left as it is.
type Change struct {
	Mode  *fs.FileMode // the permission, setuid, setgid and sticky bits; the type does not change
	UID   *uint32
	GID   *uint32
	Size  *uint64 // a regular file only
	Atime *time.Time
	Mtime *time.Time
}

// SetAttr changes the attributes of an inode and returns it as it is after. The change
// time becomes now whatever changed, as POSIX has it. An owner is a pair: an inode that
// has none takes both a user and a group, which a caller that knows only one fills in with
// the one the inode appears to have.
//
// A Volume checks no permission: who may change what is the mount's to decide, and the
// kernel decides it for a mount made with default_permissions.
func (v *Volume) SetAttr(ctx context.Context, ino InodeID, c Change) (Inode, error) {
	for _, t := range []*time.Time{c.Atime, c.Mtime} {
		if t != nil && !time.Unix(0, t.UnixNano()).Equal(*t) { // a time is nanoseconds from 1970 in 64 bits: 1678 to 2262
			return Inode{}, fmt.Errorf("%w: the time %v is out of range", fs.ErrInvalid, *t)
		}
	}
	var in Inode
	_, err := v.mutate(ctx, func(ctx context.Context) (*change, error) {
		var err error
		in, err = v.stat(ctx, ino)
		if err != nil {
			return nil, err
		}
		if (c.UID != nil) != (c.GID != nil) && !in.Owned {
			return nil, fmt.Errorf("%w: inode %d has no owner to keep the other half of", fs.ErrInvalid, ino)
		}
		var f *fileState
		if c.Size != nil {
			switch {
			case in.IsDir():
				return nil, fmt.Errorf("%w: inode %d", ErrIsDir, ino)
			case in.Mode&fs.ModeType != 0:
				return nil, fmt.Errorf("%w: inode %d is not a regular file", fs.ErrInvalid, ino)
			case *c.Size > MaxSize:
				return nil, fmt.Errorf("%w: %d bytes", ErrTooBig, *c.Size)
			}
			if f, err = v.state(ctx, ino); err != nil {
				return nil, err
			}
		}
		now := v.Now()
		if c.Size != nil {
			in.Size = *c.Size
			in.Mtime = now
		}
		if c.Mode != nil {
			in.Mode = in.Mode&fs.ModeType | bits(*c.Mode)
		}
		if c.UID != nil || c.GID != nil {
			in.Owned = true
			if c.UID != nil {
				in.UID = *c.UID
			}
			if c.GID != nil {
				in.GID = *c.GID
			}
		}
		if c.Atime != nil {
			in.Atime = *c.Atime
		}
		if c.Mtime != nil {
			in.Mtime = *c.Mtime
		}
		in.Ctime = now
		if in.IsDir() { // read before anything is staged
			if in.Nlink, err = v.dirLinks(ctx, ino); err != nil {
				return nil, err
			}
		}
		if of := v.files[ino]; of != nil && of.orphan && of.refs > 0 {
			// An unlinked open file lives in memory alone.
			if c.Size != nil {
				v.truncate(of, *c.Size, in.Mtime, in.Ctime)
			}
			of.attr = in
			of.mtime, of.ctime = in.Mtime, in.Ctime
			return nil, nil
		}
		if c.Size != nil {
			return &change{
				kind:  changeSize,
				ino:   ino,
				file:  f,
				size:  *c.Size,
				when:  in.Mtime,
				ctime: in.Ctime,
				muts:  []mut{{key: keyInode(ino), val: encodeInode(in)}},
			}, nil
		}
		return &change{
			kind: changeTree,
			next: v.next,
			muts: []mut{{key: keyInode(ino), val: encodeInode(in)}},
		}, nil
	})
	return in, err
}
