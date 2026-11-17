package vfs

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"time"
)

func validName(name string) error {
	if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\x00") {
		return fmt.Errorf("%w: name %q", fs.ErrInvalid, name)
	}
	return nil
}

// Stat returns the attributes of an inode.
func (v *Volume) Stat(ctx context.Context, ino InodeID) (Inode, error) {
	var in Inode
	err := v.view(ctx, func(ctx context.Context) (err error) {
		in, err = v.stat(ctx, ino)
		return err
	})
	return in, err
}

// stat reads an inode, with the size and time of any writes not yet settled. mu is held.
func (v *Volume) stat(ctx context.Context, ino InodeID) (Inode, error) {
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
		in.Size, in.Mtime, in.Ctime = f.size, f.mtime, f.mtime
	}
	return in, nil
}

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
		in, err = v.stat(ctx, id)
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

// Mkdir creates a directory in dir.
func (v *Volume) Mkdir(ctx context.Context, dir InodeID, name string, perm fs.FileMode) (Inode, error) {
	return v.make(ctx, dir, name, fs.ModeDir|perm.Perm())
}

// Create creates an empty regular file in dir.
func (v *Volume) Create(ctx context.Context, dir InodeID, name string, perm fs.FileMode) (Inode, error) {
	return v.make(ctx, dir, name, perm.Perm())
}

func (v *Volume) make(ctx context.Context, dir InodeID, name string, mode fs.FileMode) (Inode, error) {
	if err := validName(name); err != nil {
		return Inode{}, err
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
		now := time.Now()
		in = Inode{ID: v.next, Mode: mode, Mtime: now, Atime: now, Ctime: now}
		parent.Mtime, parent.Ctime = now, now
		return &change{kind: changeTree, next: in.ID + 1, muts: []mut{
			{key: keyInode(in.ID), val: encodeInode(in)},
			{key: keyEntry(dir, name), val: encodeEntry(in.ID, mode)},
			{key: keyInode(dir), val: encodeInode(parent)},
		}}, nil
	})
	return in, err
}

// Remove deletes a file or an empty directory from dir. An open handle to a
// removed file fails afterwards; nothing of the file survives for it.
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
		var gone []InodeID
		if in.IsDir() {
			kids, err := v.has(ctx, key(tEntry, uint64(id)))
			if err != nil {
				return nil, err
			}
			if kids {
				return nil, fmt.Errorf("%w: %q", ErrNotEmpty, name)
			}
		} else {
			gone = []InodeID{id}
		}
		parent, err := v.stat(ctx, dir)
		if err != nil {
			return nil, err
		}
		now := time.Now()
		parent.Mtime, parent.Ctime = now, now
		return &change{kind: changeTree, next: v.next, gone: gone, muts: []mut{
			{key: keyEntry(dir, name), del: true},
			{key: keyInode(id), del: true},
			{key: keyInode(dir), val: encodeInode(parent)},
		}}, nil
	})
	return err
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
