package vfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
)

// RenameFlags are the flags of renameat2(2) that a Volume honours.
type RenameFlags uint32

const (
	// RenameNoReplace refuses a rename whose new name exists (fs.ErrExist).
	RenameNoReplace RenameFlags = 1 << 0
	// RenameExchange swaps two names that both exist; neither is removed.
	RenameExchange RenameFlags = 1 << 1
)

// Rename gives the inode named oldName in oldDir the name newName in newDir, in one atomic
// step: a crash leaves the old state or the new one, never both names, never neither.
//
// A new name that exists is replaced when both are directories (the new one must be empty)
// or neither is. Two names of the same inode are left as they are and Rename succeeds.
// A directory cannot move into itself or below itself (fs.ErrInvalid). The directories
// that gain and lose an entry take the time as their modification and change times, and so
// does the inode that moved.
func (v *Volume) Rename(ctx context.Context, oldDir InodeID, oldName string, newDir InodeID, newName string, flags RenameFlags) error {
	if err := validName(oldName); err != nil {
		return err
	}
	if err := validName(newName); err != nil {
		return err
	}
	if flags&^(RenameNoReplace|RenameExchange) != 0 || flags == RenameNoReplace|RenameExchange {
		return fmt.Errorf("%w: rename flags %#x", fs.ErrInvalid, uint32(flags))
	}
	_, err := v.mutate(ctx, func(ctx context.Context) (*change, error) {
		srcID, _, err := v.entry(ctx, oldDir, oldName)
		if err != nil {
			return nil, err
		}
		src, err := v.stat(ctx, srcID)
		if err != nil {
			return nil, err
		}
		od, err := v.stat(ctx, oldDir)
		if err != nil {
			return nil, err
		}
		nd := od
		if newDir != oldDir {
			if nd, err = v.stat(ctx, newDir); err != nil {
				return nil, err
			}
		}
		if !od.IsDir() || !nd.IsDir() {
			return nil, fmt.Errorf("%w: a rename between inodes %d and %d", ErrNotDir, oldDir, newDir)
		}
		dstID, _, err := v.entry(ctx, newDir, newName)
		switch {
		case err == nil:
		case flags&RenameExchange != 0:
			return nil, err
		case errors.Is(err, fs.ErrNotExist):
			dstID = 0
		default:
			return nil, err
		}
		if dstID != 0 && flags&RenameNoReplace != 0 {
			return nil, fmt.Errorf("%w: %q in inode %d", fs.ErrExist, newName, newDir)
		}
		if dstID == srcID {
			return nil, nil // two names of one inode: nothing to do
		}
		var dst Inode
		if dstID != 0 {
			if dst, err = v.stat(ctx, dstID); err != nil {
				return nil, err
			}
		}
		// A directory must not become its own ancestor.
		if src.IsDir() {
			if in, err := v.within(ctx, newDir, srcID); err != nil || in {
				if err == nil {
					err = fmt.Errorf("%w: directory %d cannot move into itself", fs.ErrInvalid, srcID)
				}
				return nil, err
			}
		}
		if flags&RenameExchange != 0 && dst.IsDir() {
			if in, err := v.within(ctx, oldDir, dstID); err != nil || in {
				if err == nil {
					err = fmt.Errorf("%w: directory %d cannot move into itself", fs.ErrInvalid, dstID)
				}
				return nil, err
			}
		}
		now := v.Now()
		ch := &change{kind: changeTree, next: v.next}
		switch {
		case dstID == 0:
		case flags&RenameExchange != 0:
		case src.IsDir() && !dst.IsDir():
			return nil, fmt.Errorf("%w: %q is not a directory", ErrNotDir, newName)
		case !src.IsDir() && dst.IsDir():
			return nil, fmt.Errorf("%w: %q is a directory", ErrIsDir, newName)
		default:
			if err := v.planDrop(ctx, dst, now, ch); err != nil { // fails, changing nothing, for a directory that is not empty
				return nil, err
			}
		}

		// Everything is read; build the single atomic tree change.
		ch.muts = append(ch.muts, mut{key: keyEntry(newDir, newName), val: encodeEntry(srcID, src.Mode)})
		if flags&RenameExchange != 0 {
			ch.muts = append(ch.muts, mut{key: keyEntry(oldDir, oldName), val: encodeEntry(dstID, dst.Mode)})
			dst.Ctime = now
			if dst.IsDir() {
				dst.Parent = oldDir
			}
			ch.muts = append(ch.muts, mut{key: keyInode(dst.ID), val: encodeInode(dst)})
		} else {
			ch.muts = append(ch.muts, mut{key: keyEntry(oldDir, oldName), del: true})
		}
		src.Ctime = now
		if src.IsDir() {
			src.Parent = newDir
		}
		ch.muts = append(ch.muts, mut{key: keyInode(src.ID), val: encodeInode(src)})
		od.Mtime, od.Ctime = now, now
		ch.muts = append(ch.muts, mut{key: keyInode(od.ID), val: encodeInode(od)})
		if newDir != oldDir {
			nd.Mtime, nd.Ctime = now, now
			ch.muts = append(ch.muts, mut{key: keyInode(nd.ID), val: encodeInode(nd)})
		}
		// each directory's subdirectories: the new name gains the inode that moved, and loses
		// what it replaced; the old name loses the one and, in an exchange, gains the other
		if src.IsDir() {
			ch.dirBumps = append(ch.dirBumps, dirBump{dir: oldDir, delta: -1}, dirBump{dir: newDir, delta: +1})
		}
		if dst.IsDir() {
			ch.dirBumps = append(ch.dirBumps, dirBump{dir: newDir, delta: -1})
			if flags&RenameExchange != 0 {
				ch.dirBumps = append(ch.dirBumps, dirBump{dir: oldDir, delta: +1})
			}
		}
		return ch, nil
	})
	return err
}

// within reports whether dir is the directory anc or lies below it, following the parent of
// each directory up to the root.
func (v *Volume) within(ctx context.Context, dir, anc InodeID) (bool, error) {
	for range 1 << 16 {
		if dir == anc {
			return true, nil
		}
		if dir == RootInode {
			return false, nil
		}
		in, err := v.stat(ctx, dir)
		if err != nil {
			return false, err
		}
		if in.Parent == 0 {
			return false, fmt.Errorf("%w: directory %d has no parent", ErrCorrupt, dir)
		}
		dir = in.Parent
	}
	return false, fmt.Errorf("%w: directories nest past %d levels, or in a loop", ErrCorrupt, 1<<16)
}
