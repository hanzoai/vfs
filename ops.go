package vfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"time"
)

var (
	// ErrNameTooLong is returned for a name of more than 255 bytes and for a symbolic
	// link target of more than 4095. It also satisfies errors.Is(err, fs.ErrInvalid).
	ErrNameTooLong = errors.New("vfs: name too long")

	// ErrNoSpace is returned by a write that would take the Volume past Config.Capacity.
	ErrNoSpace = errors.New("vfs: no space left on the Volume")

	// ErrTooManyLinks is returned by Link for an inode that has maxLinks names.
	ErrTooManyLinks = errors.New("vfs: too many links")
)

const (
	maxName   = 255
	maxTarget = 4095
	maxLinks  = 65000
)

func validName(name string) error {
	switch {
	case len(name) > maxName:
		return fmt.Errorf("%w: %d bytes: %w", ErrNameTooLong, len(name), fs.ErrInvalid)
	case name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00"):
		return fmt.Errorf("%w: name %q", fs.ErrInvalid, name)
	}
	return nil
}

// Now is the time every change is stamped with: Config.Clock, or the time of day.
func (v *Volume) Now() time.Time {
	if v.cfg.Clock != nil {
		return v.cfg.Clock.Now()
	}
	return time.Now()
}

// Option adjusts the creation of an inode.
type Option func(*creation)

type creation struct {
	owned    bool
	uid, gid uint32
}

// Owner makes uid and gid the owner of a new inode. Without it the inode has no owner
// record and belongs to whoever runs the mount.
func Owner(uid, gid uint32) Option {
	return func(c *creation) { c.owned, c.uid, c.gid = true, uid, gid }
}

// special is the mode bits above the permissions that a caller may set.
const special = fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

// bits returns the permission, setuid, setgid and sticky bits of m: what a caller may set.
func bits(m fs.FileMode) fs.FileMode { return m & (fs.ModePerm | special) }

// Stat returns the attributes of an inode.
func (v *Volume) Stat(ctx context.Context, ino InodeID) (Inode, error) {
	var in Inode
	err := v.view(ctx, func(ctx context.Context) (err error) {
		in, err = v.attrs(ctx, ino)
		return err
	})
	return in, err
}

// attrs is stat with a directory's link count. mu is held.
func (v *Volume) attrs(ctx context.Context, ino InodeID) (Inode, error) {
	in, err := v.stat(ctx, ino)
	if err != nil || !in.IsDir() {
		return in, err
	}
	in.Nlink, err = v.dirLinks(ctx, ino)
	return in, err
}

// stat reads an inode, with the size and time of any writes not yet settled. An
// inode that was unlinked while a handle holds it is read from the handle's state. mu is held.
func (v *Volume) stat(ctx context.Context, ino InodeID) (Inode, error) {
	if f := v.files[ino]; f != nil && f.orphan {
		if f.refs == 0 {
			return Inode{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, ino)
		}
		in := f.attr
		in.Size, in.Mtime, in.Ctime = f.size, f.mtime, f.ctime
		return in, nil
	}
	b, ok, err := v.get(ctx, keyInode(ino))
	if err != nil {
		return Inode{}, err
	}
	if !ok {
		return Inode{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, ino)
	}
	in, err := decodeInode(ino, b)
	if err != nil {
		return Inode{}, err
	}
	if f := v.files[ino]; f != nil && f.unsettled() {
		in.Size, in.Mtime, in.Ctime = f.size, f.mtime, f.ctime
	}
	return in, nil
}

// put stages an inode record directly into the live overlay (used by tests of Check).
func (v *Volume) put(in Inode) { v.over.put(keyInode(in.ID), encodeInode(in)) }

// Lookup returns the child of dir called name.
func (v *Volume) Lookup(ctx context.Context, dir InodeID, name string) (Inode, error) {
	if err := validName(name); err != nil {
		return Inode{}, err
	}
	var in Inode
	err := v.view(ctx, func(ctx context.Context) error {
		id, _, err := v.entry(ctx, dir, name)
		if err != nil {
			return err
		}
		in, err = v.attrs(ctx, id)
		return err
	})
	return in, err
}

// entry reads the directory entry of dir called name. mu is held.
func (v *Volume) entry(ctx context.Context, dir InodeID, name string) (InodeID, fs.FileMode, error) {
	b, ok, err := v.get(ctx, keyEntry(dir, name))
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("%w: %q in inode %d", fs.ErrNotExist, name, dir)
	}
	return decodeEntry(b)
}

// ReadDir lists a directory in name order.
func (v *Volume) ReadDir(ctx context.Context, dir InodeID) ([]DirEntry, error) {
	var out []DirEntry
	err := v.view(ctx, func(ctx context.Context) error {
		in, err := v.stat(ctx, dir)
		if err != nil {
			return err
		}
		if !in.IsDir() {
			return fmt.Errorf("%w: inode %d", ErrNotDir, dir)
		}
		recs, err := v.scan(ctx, key(tEntry, uint64(dir)))
		if err != nil {
			return err
		}
		out = make([]DirEntry, len(recs))
		for i, r := range recs {
			id, mode, err := decodeEntry(r.val)
			if err != nil {
				return err
			}
			out[i] = DirEntry{Name: string(r.key[prefixLen:]), ID: id, Mode: mode}
		}
		return nil
	})
	return out, err
}

// Parent returns the directory that names the directory dir; the root is its own parent.
func (v *Volume) Parent(ctx context.Context, dir InodeID) (InodeID, error) {
	var parent InodeID
	err := v.view(ctx, func(ctx context.Context) error {
		in, err := v.stat(ctx, dir)
		switch {
		case err != nil:
			return err
		case !in.IsDir():
			return fmt.Errorf("%w: inode %d", ErrNotDir, dir)
		case dir == RootInode:
			parent = dir
			return nil
		case in.Parent == 0:
			return fmt.Errorf("%w: directory %d has no parent", ErrCorrupt, dir)
		}
		parent = in.Parent
		return nil
	})
	return parent, err
}

// Mkdir creates a directory in dir.
func (v *Volume) Mkdir(ctx context.Context, dir InodeID, name string, perm fs.FileMode, opts ...Option) (Inode, error) {
	return v.make(ctx, dir, name, fs.ModeDir|bits(perm), nil, opts)
}

// Create creates an empty regular file in dir.
func (v *Volume) Create(ctx context.Context, dir InodeID, name string, perm fs.FileMode, opts ...Option) (Inode, error) {
	return v.make(ctx, dir, name, bits(perm), nil, opts)
}

// Mknod creates a regular file, a named pipe or a socket in dir: mode names the type
// (none, fs.ModeNamedPipe or fs.ModeSocket) and the permissions. A device cannot be made
// on a Volume (fs.ErrPermission).
func (v *Volume) Mknod(ctx context.Context, dir InodeID, name string, mode fs.FileMode, opts ...Option) (Inode, error) {
	switch mode & fs.ModeType &^ fs.ModeIrregular {
	case 0, fs.ModeNamedPipe, fs.ModeSocket:
		return v.make(ctx, dir, name, mode&fs.ModeType|bits(mode), nil, opts)
	}
	return Inode{}, fmt.Errorf("%w: a Volume holds no device nodes", fs.ErrPermission)
}

// Symlink creates a symbolic link in dir that holds target. The target is bytes that the
// Volume never interprets.
func (v *Volume) Symlink(ctx context.Context, dir InodeID, name, target string, opts ...Option) (Inode, error) {
	switch {
	case target == "":
		return Inode{}, fmt.Errorf("%w: an empty symbolic link target", fs.ErrInvalid)
	case len(target) > maxTarget:
		return Inode{}, fmt.Errorf("%w: a target of %d bytes: %w", ErrNameTooLong, len(target), fs.ErrInvalid)
	case strings.ContainsRune(target, 0):
		return Inode{}, fmt.Errorf("%w: a NUL in a symbolic link target", fs.ErrInvalid)
	}
	return v.make(ctx, dir, name, fs.ModeSymlink|0o777, []byte(target), opts)
}

// Readlink returns the target of a symbolic link.
func (v *Volume) Readlink(ctx context.Context, ino InodeID) (string, error) {
	var target string
	err := v.view(ctx, func(ctx context.Context) error {
		in, err := v.stat(ctx, ino)
		if err != nil {
			return err
		}
		if in.Mode&fs.ModeSymlink == 0 {
			return fmt.Errorf("%w: inode %d is not a symbolic link", fs.ErrInvalid, ino)
		}
		b, ok, err := v.get(ctx, keyInline(ino))
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: symbolic link %d has no target", ErrCorrupt, ino)
		}
		target = string(b)
		return nil
	})
	return target, err
}

// make creates an inode of the given mode (its type and permissions), with a target's
// bytes for a symbolic link, and names it name in dir.
func (v *Volume) make(ctx context.Context, dir InodeID, name string, mode fs.FileMode, target []byte, opts []Option) (Inode, error) {
	if err := validName(name); err != nil {
		return Inode{}, err
	}
	var cr creation
	for _, o := range opts {
		o(&cr)
	}
	var in Inode
	_, err := v.mutate(ctx, func(ctx context.Context) (*change, error) {
		parent, err := v.stat(ctx, dir)
		if err != nil {
			return nil, err
		}
		if !parent.IsDir() {
			return nil, fmt.Errorf("%w: inode %d", ErrNotDir, dir)
		}
		if _, ok, err := v.get(ctx, keyEntry(dir, name)); err != nil || ok {
			if err == nil {
				err = fmt.Errorf("%w: %q in inode %d", fs.ErrExist, name, dir)
			}
			return nil, err
		}
		if v.cfg.Capacity > 0 && v.used()+int64(len(target)) > v.cfg.Capacity {
			return nil, fmt.Errorf("%w: capacity %d", ErrNoSpace, v.cfg.Capacity)
		}
		m := mode
		c := cr
		// A directory with the setgid bit gives its group, and for a directory the bit, to
		// what is made in it, as BSD and Linux file systems do.
		if parent.Mode&fs.ModeSetgid != 0 {
			if m.IsDir() {
				m |= fs.ModeSetgid
			}
			if c.owned && parent.Owned {
				c.gid = parent.GID
			}
		}
		now := v.Now()
		in = Inode{ID: v.next, Mode: m, Mtime: now, Atime: now, Ctime: now, Owned: c.owned, UID: c.uid, GID: c.gid}
		if in.IsDir() {
			in.Parent = dir
		}
		muts := make([]mut, 0, 4)
		if target != nil {
			in.Size = uint64(len(target))
			muts = append(muts, mut{key: keyInline(in.ID), val: target})
		}
		parent.Mtime, parent.Ctime = now, now
		muts = append(muts,
			mut{key: keyInode(in.ID), val: encodeInode(in)},
			mut{key: keyEntry(dir, name), val: encodeEntry(in.ID, m)},
			mut{key: keyInode(dir), val: encodeInode(parent)},
		)
		ch := &change{
			kind:   changeTree,
			next:   in.ID + 1,
			dFiles: 1,
			dBytes: int64(len(target)),
			muts:   muts,
		}
		if in.IsDir() {
			ch.dirBumps = []dirBump{{dir: dir, delta: +1}}
			in.Nlink = 2
		} else {
			in.Nlink = 1
		}
		v.recordMakeHint(dir, name, in.ID)
		return ch, nil
	})
	return in, err
}

// Link names the inode ino a second time, as name in dir. A directory cannot be linked
// (fs.ErrPermission). The first hard link a Volume has makes the commit that holds it
// announce link counts in the Head, and every commit after (SPECIFICATION 5.1).
func (v *Volume) Link(ctx context.Context, ino, dir InodeID, name string) (Inode, error) {
	if err := validName(name); err != nil {
		return Inode{}, err
	}
	var in Inode
	_, err := v.mutate(ctx, func(ctx context.Context) (*change, error) {
		var err error
		in, err = v.stat(ctx, ino)
		if err != nil {
			return nil, err
		}
		if f := v.files[ino]; f != nil && f.orphan {
			return nil, fmt.Errorf("%w: inode %d was unlinked", fs.ErrNotExist, ino)
		}
		parent, err := v.stat(ctx, dir)
		if err != nil {
			return nil, err
		}
		switch {
		case in.IsDir():
			return nil, fmt.Errorf("%w: a directory has one name", fs.ErrPermission)
		case !parent.IsDir():
			return nil, fmt.Errorf("%w: inode %d", ErrNotDir, dir)
		case in.Nlink >= maxLinks:
			return nil, fmt.Errorf("%w: inode %d has %d names", ErrTooManyLinks, ino, in.Nlink)
		}
		if _, ok, err := v.get(ctx, keyEntry(dir, name)); err != nil || ok {
			if err == nil {
				err = fmt.Errorf("%w: %q in inode %d", fs.ErrExist, name, dir)
			}
			return nil, err
		}
		now := v.Now()
		in.Nlink++
		in.Ctime = now
		parent.Mtime, parent.Ctime = now, now
		var dLinked int64
		if in.Nlink == 2 {
			dLinked = 1
		}
		return &change{
			kind:     changeTree,
			next:     v.next,
			dLinked:  dLinked,
			setLinks: true,
			muts: []mut{
				{key: keyInode(ino), val: encodeInode(in)},
				{key: keyEntry(dir, name), val: encodeEntry(ino, in.Mode)},
				{key: keyInode(dir), val: encodeInode(parent)},
			},
		}, nil
	})
	return in, err
}

// Remove deletes a file or an empty directory from dir. A file with other names keeps
// them. A file that a handle still holds open stays readable and writable through
// that handle, and is gone for everyone else; its bytes go when the last handle closes.
func (v *Volume) Remove(ctx context.Context, dir InodeID, name string) error {
	if err := validName(name); err != nil {
		return err
	}
	_, err := v.mutate(ctx, func(ctx context.Context) (*change, error) {
		id, _, err := v.entry(ctx, dir, name)
		if err != nil {
			return nil, err
		}
		in, err := v.stat(ctx, id)
		if err != nil {
			return nil, err
		}
		parent, err := v.stat(ctx, dir)
		if err != nil {
			return nil, err
		}
		now := v.Now()
		ch := &change{kind: changeTree, next: v.next}
		if err := v.planDrop(ctx, in, now, ch); err != nil {
			return nil, err
		}
		parent.Mtime, parent.Ctime = now, now
		ch.muts = append(ch.muts,
			mut{key: keyEntry(dir, name), del: true},
			mut{key: keyInode(dir), val: encodeInode(parent)},
		)
		if in.IsDir() {
			ch.dirBumps = append(ch.dirBumps, dirBump{dir: dir, delta: -1})
		}
		return ch, nil
	})
	return err
}

// planDrop reads everything needed to take one name away from in and records the mutations
// into ch without mutating v. A directory must be empty. An inode with other names counts
// one fewer; one with none is deleted, or, if a handle holds it, becomes an orphan on apply.
// mu is held.
func (v *Volume) planDrop(ctx context.Context, in Inode, now time.Time, ch *change) error {
	if in.IsDir() {
		kids, err := v.has(ctx, key(tEntry, uint64(in.ID)))
		if err != nil {
			return err
		}
		if kids {
			return fmt.Errorf("%w: inode %d", ErrNotEmpty, in.ID)
		}
		ch.muts = append(ch.muts, mut{key: keyInode(in.ID), del: true})
		ch.dFiles--
		return nil
	}
	if in.Nlink > 1 {
		in.Nlink--
		in.Ctime = now
		ch.muts = append(ch.muts, mut{key: keyInode(in.ID), val: encodeInode(in)})
		if in.Nlink == 1 {
			ch.dLinked--
		}
		return nil
	}
	data, err := v.scan(ctx, key(tData, uint64(in.ID)))
	if err != nil {
		return err
	}
	var held uint64
	for _, r := range data {
		if len(r.key) == prefixLen {
			held += uint64(len(r.val))
			continue
		}
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			return err
		}
		held += uint64(e.n)
	}
	for _, r := range data {
		ch.muts = append(ch.muts, mut{key: r.key, del: true})
	}
	ch.muts = append(ch.muts, mut{key: keyInode(in.ID), del: true})
	ch.dFiles--
	ch.dBytes -= int64(held)
	ch.gone = append(ch.gone, in.ID)
	if f := v.files[in.ID]; f != nil && f.refs > 0 {
		in.Nlink, in.Ctime = 0, now
		if ch.orphans == nil {
			ch.orphans = map[InodeID]Inode{}
		}
		ch.orphans[in.ID] = in
	}
	return nil
}

// Resolve walks a /-separated path from the root.
func (v *Volume) Resolve(ctx context.Context, p string) (Inode, error) {
	cur, err := v.Stat(ctx, RootInode)
	if err != nil {
		return Inode{}, err
	}
	for _, name := range strings.Split(strings.Trim(path.Clean("/"+p), "/"), "/") {
		if name == "" {
			continue
		}
		if cur, err = v.Lookup(ctx, cur.ID, name); err != nil {
			return Inode{}, err
		}
	}
	return cur, nil
}
