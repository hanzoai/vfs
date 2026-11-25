package vfs

import (
	"context"
	"fmt"
	"io/fs"
)

// Check reads the whole namespace of the Volume as it is now, changes not yet committed
// included, and returns the first way in which it is not a namespace: a directory entry that
// names no inode, or one of another type; an inode that no entry names, or that a directory
// names twice; a link count that is not the number of names; a directory whose parent is not
// the directory that names it; data that belongs to no inode; or counters, in the Head's
// usage and links fields, that are not what the tree holds. It is for tests and for a
// tool run on a Volume that nothing writes: it reads every record.
func (v *Volume) Check(ctx context.Context) error {
	return v.view(ctx, func(ctx context.Context) error {
		type seen struct {
			in    Inode
			names uint32
			dirs  uint32 // subdirectories, for a directory
			dad   InodeID
		}
		inodes := map[InodeID]*seen{}
		recs, err := v.scan(ctx, []byte{tInode})
		if err != nil {
			return err
		}
		var files, linked uint64
		for _, r := range recs {
			id := inoOf(r.key)
			in, err := decodeInode(id, r.val)
			if err != nil {
				return err
			}
			inodes[id] = &seen{in: in}
			files++
			if in.Nlink > 1 {
				linked++
			}
		}
		if inodes[RootInode] == nil || !inodes[RootInode].in.IsDir() {
			return fmt.Errorf("%w: no root directory", ErrCorrupt)
		}
		ents, err := v.scan(ctx, []byte{tEntry})
		if err != nil {
			return err
		}
		for _, r := range ents {
			dir := inoOf(r.key)
			name := r.key[prefixLen:]
			child, mode, err := decodeEntry(r.val)
			if err != nil {
				return err
			}
			d, c := inodes[dir], inodes[child]
			switch {
			case d == nil || !d.in.IsDir():
				return fmt.Errorf("%w: entry %q is in inode %d, which is not a directory", ErrCorrupt, name, dir)
			case c == nil:
				return fmt.Errorf("%w: entry %q of directory %d names inode %d, which does not exist", ErrCorrupt, name, dir, child)
			case c.in.Mode&fs.ModeType != mode&fs.ModeType:
				return fmt.Errorf("%w: entry %q of directory %d says type %v, inode %d is %v", ErrCorrupt, name, dir, mode&fs.ModeType, child, c.in.Mode&fs.ModeType)
			case c.in.IsDir() && c.names > 0:
				return fmt.Errorf("%w: directory %d has two names", ErrCorrupt, child)
			}
			c.names++
			if c.in.IsDir() {
				c.dad = dir
				d.dirs++
			}
		}
		var cachedWrong error
		v.dirn.each(func(dir InodeID, n uint32) { // a directory that is gone may leave its count: no inode number is used twice
			if d := inodes[dir]; cachedWrong == nil && d != nil && n != 2+d.dirs {
				cachedWrong = fmt.Errorf("%w: the cached link count of directory %d is %d", ErrCorrupt, dir, n)
			}
		})
		if cachedWrong != nil {
			return cachedWrong
		}
		var bytesHeld uint64
		for id, s := range inodes {
			switch {
			case id == RootInode:
				if s.names != 0 || s.in.Parent != 0 {
					return fmt.Errorf("%w: the root directory is named or has a parent", ErrCorrupt)
				}
			case s.in.IsDir():
				if s.names != 1 || s.in.Parent != s.dad {
					return fmt.Errorf("%w: directory %d has parent %d and is named by %d (%d names)", ErrCorrupt, id, s.in.Parent, s.dad, s.names)
				}
			case s.names != s.in.Nlink:
				return fmt.Errorf("%w: inode %d has %d names and a link count of %d", ErrCorrupt, id, s.names, s.in.Nlink)
			}
		}
		data, err := v.scan(ctx, []byte{tData})
		if err != nil {
			return err
		}
		for _, r := range data {
			id := inoOf(r.key)
			s := inodes[id]
			if s == nil {
				return fmt.Errorf("%w: data of inode %d, which does not exist", ErrCorrupt, id)
			}
			if len(r.key) == prefixLen {
				bytesHeld += uint64(len(r.val))
				continue
			}
			e, err := decodeExtent(offsetOf(r.key), r.val)
			if err != nil {
				return err
			}
			bytesHeld += uint64(e.n)
		}
		switch {
		case v.ns.files != files:
			return fmt.Errorf("%w: usage says %d inodes, the tree holds %d", ErrCorrupt, v.ns.files, files)
		case v.ns.bytes != bytesHeld:
			return fmt.Errorf("%w: usage says %d bytes, the tree holds %d", ErrCorrupt, v.ns.bytes, bytesHeld)
		case v.ns.linked != linked:
			return fmt.Errorf("%w: links says %d inodes with more than one name, the tree holds %d", ErrCorrupt, v.ns.linked, linked)
		case linked > 0 && !v.ns.links:
			return fmt.Errorf("%w: an inode has more than one name and the Head does not announce links", ErrCorrupt)
		}
		// Orphans are not in the tree: a handle holds each, and it is named by nothing.
		for ino, f := range v.files {
			if f.orphan && f.refs > 0 && f.attr.Nlink != 0 {
				return fmt.Errorf("%w: unlinked inode %d has a link count of %d", ErrCorrupt, ino, f.attr.Nlink)
			}
		}
		return nil
	})
}
