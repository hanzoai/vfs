package vfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/hanzoai/vfs/pkg/backend"
)

// checkTree checks that the tree the Head names can be read from this Volume's store. Always
// it reads the root shard, which proves the store holds objects sealed to this Volume and
// that the Head is not another kind of object dressed as one, and finds the root directory
// in it: every Volume has one, so a tree without it is not a Volume's. With full it reads
// every shard the Head reaches (each is checked against its name and opened) and checks that
// every pack a chunk lives in is there and long enough to hold it, and that every chunk
// a file refers to is in the chunk table. It reads no chunk: a chunk is checked against its
// hash when a file is read, so a pack that changed under a chunk no file has read since is
// whole as far as this walk can tell. Verify (integrity.go) reads the chunks and says so.
func (v *Volume) checkTree(ctx context.Context, full bool) error {
	_, ok, err := v.t.get(ctx, v.root, keyInode(RootInode))
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: the tree has no root directory", ErrCorrupt)
	}
	if !full {
		return nil
	}
	need := map[packID]uint64{}
	var bad error
	if err := v.t.scan(ctx, v.root, nil, func(k, val []byte) bool {
		if len(k) == 0 || k[0] != tChunk {
			return true
		}
		w, err := decodeWhere(val)
		if err != nil {
			bad = err
			return false
		}
		need[w.pack] = max(need[w.pack], w.off+w.n)
		return true
	}); err != nil {
		return err
	}
	if bad != nil {
		return bad
	}
	// every chunk that a file refers to is in the chunk table, which says where it is
	if err := v.t.scan(ctx, v.root, []byte{tData}, func(k, val []byte) bool {
		if len(k) != prefixLen+8 {
			return true // the bytes of a small file, inline
		}
		e, err := decodeExtent(offsetOf(k), val)
		if err == nil {
			var ok bool
			if _, ok, err = v.t.get(ctx, v.root, keyChunk(e.id)); err == nil && !ok {
				err = fmt.Errorf("%w: chunk %x of a file is in no table", ErrCorrupt, e.id[:6])
			}
		}
		bad = err
		return err == nil
	}); err != nil {
		return err
	}
	if bad != nil {
		return bad
	}
	for id, end := range need {
		size, err := v.be.Stat(ctx, packKey(id))
		if errors.Is(err, backend.ErrNotFound) {
			return fmt.Errorf("%w: pack %x is missing", ErrCorrupt, id[:6])
		}
		if err != nil {
			return err
		}
		if uint64(size) < end {
			return fmt.Errorf("%w: pack %x has %d bytes and a chunk ends at %d", ErrCorrupt, id[:6], size, end)
		}
	}
	return nil
}
