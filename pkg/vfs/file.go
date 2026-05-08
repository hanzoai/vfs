package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
)

// File is a multi-block file handle. Reads/writes are byte-addressed;
// the implementation transparently fetches, modifies, and re-encrypts
// 4 KiB blocks. Dirty blocks are buffered in a per-handle write set
// until Sync flushes them to the backend (and updates the inode's
// block list in the FS metadata tree).
//
// Concurrency: a File is owned by a single goroutine. Concurrent FS
// access across multiple Files is supported by FS's RWMutex.
type File struct {
	fs      *FS
	ctx     context.Context
	inodeID InodeID

	// dirty maps block index → fresh plaintext (BlockSize-padded). Reads
	// hit dirty first, then the inode's block list. Sync materialises
	// dirty entries via vfs.PutBlock and atomically swaps inode.Blocks.
	dirty map[int][]byte
}

// Stat returns a snapshot of the file's inode metadata.
func (f *File) Stat() (*Inode, error) {
	f.fs.mu.RLock()
	defer f.fs.mu.RUnlock()
	in := f.fs.inodeByID(f.inodeID)
	if in == nil {
		return nil, fmt.Errorf("vfs.File.Stat: inode %d gone", f.inodeID)
	}
	return cloneInode(in), nil
}

// ReadAt reads len(p) bytes at the given offset. Returns io.EOF when
// reading past the end of the file (matching os.File semantics: when
// fewer bytes than requested are available, returns those bytes plus
// io.EOF).
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("vfs.File.ReadAt: negative offset %d", off)
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if uint64(off) >= stat.Size {
		return 0, io.EOF
	}
	want := len(p)
	end := uint64(off) + uint64(want)
	if end > stat.Size {
		end = stat.Size
		want = int(end - uint64(off))
	}

	written := 0
	cur := uint64(off)
	for cur < end {
		blockIdx := int(cur / BlockSize)
		blockOff := int(cur % BlockSize)
		blockBytes, err := f.readBlock(blockIdx, stat)
		if err != nil {
			return written, err
		}
		copyEnd := BlockSize
		remainingInFile := int(end - cur)
		if remainingInFile < BlockSize-blockOff {
			copyEnd = blockOff + remainingInFile
		}
		n := copy(p[written:want], blockBytes[blockOff:copyEnd])
		written += n
		cur += uint64(n)
	}
	if uint64(off)+uint64(written) >= stat.Size {
		return written, io.EOF
	}
	return written, nil
}

// WriteAt writes len(p) bytes at the given offset, extending the file
// as needed. Partial-block writes do read-modify-write to avoid
// clobbering adjacent bytes.
//
// Sync MUST be called to flush dirty blocks to the backend; without
// Sync the writes live only in this File's in-memory dirty set and
// will be lost on close-without-sync.
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("vfs.File.WriteAt: negative offset %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}

	if f.dirty == nil {
		f.dirty = map[int][]byte{}
	}

	written := 0
	cur := uint64(off)
	end := cur + uint64(len(p))
	for cur < end {
		blockIdx := int(cur / BlockSize)
		blockOff := int(cur % BlockSize)
		var existing []byte
		// Existing block content for read-modify-write (if any)
		if buf, ok := f.dirty[blockIdx]; ok {
			existing = append([]byte(nil), buf...)
		} else if blockIdx < len(stat.Blocks) {
			b, err := f.readBlock(blockIdx, stat)
			if err != nil {
				return written, err
			}
			existing = append([]byte(nil), b...)
		} else {
			existing = make([]byte, BlockSize)
		}

		copyLen := BlockSize - blockOff
		remaining := len(p) - written
		if copyLen > remaining {
			copyLen = remaining
		}
		copy(existing[blockOff:blockOff+copyLen], p[written:written+copyLen])
		f.dirty[blockIdx] = existing
		written += copyLen
		cur += uint64(copyLen)
	}

	// Extend size if we wrote past the old end. Inode update is staged
	// here in memory; the canonical Size + Blocks land at Sync().
	f.fs.mu.Lock()
	in := f.fs.inodeByID(f.inodeID)
	if end > in.Size {
		in.Size = end
	}
	in.Mtime = time.Now()
	f.fs.dirty = true
	f.fs.mu.Unlock()

	return written, nil
}

// Truncate sets the file size, dropping any blocks past the new end.
// Extending past the current size zero-pads (creates dirty zero blocks
// up to the new end).
func (f *File) Truncate(size uint64) error {
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if size == stat.Size {
		return nil
	}
	if f.dirty == nil {
		f.dirty = map[int][]byte{}
	}

	if size < stat.Size {
		// Shrink: drop dirty blocks past size, no read needed for backend.
		newBlockCount := int((size + BlockSize - 1) / BlockSize)
		for idx := range f.dirty {
			if idx >= newBlockCount {
				delete(f.dirty, idx)
			}
		}
	} else {
		// Grow: load (or zero-init) the last block, zero-pad in place.
		// Subsequent blocks remain implicitly zero — they materialise
		// on first read as a zero block.
		oldEnd := stat.Size
		newEnd := size
		if oldEnd%BlockSize != 0 || oldEnd == 0 {
			lastIdx := int(oldEnd / BlockSize)
			var buf []byte
			if cached, ok := f.dirty[lastIdx]; ok {
				buf = append([]byte(nil), cached...)
			} else if lastIdx < len(stat.Blocks) {
				existing, err := f.readBlock(lastIdx, stat)
				if err != nil {
					return err
				}
				buf = append([]byte(nil), existing...)
			} else {
				buf = make([]byte, BlockSize)
			}
			startZero := int(oldEnd % BlockSize)
			zeroEnd := BlockSize
			if newEnd-uint64(lastIdx*BlockSize) < uint64(BlockSize) {
				zeroEnd = int(newEnd - uint64(lastIdx*BlockSize))
			}
			for i := startZero; i < zeroEnd; i++ {
				buf[i] = 0
			}
			f.dirty[lastIdx] = buf
		}
	}

	f.fs.mu.Lock()
	in := f.fs.inodeByID(f.inodeID)
	in.Size = size
	in.Mtime = time.Now()
	f.fs.dirty = true
	f.fs.mu.Unlock()
	return nil
}

// Sync flushes all dirty blocks to the backend, updates the inode's
// block list, and persists the FS metadata tree. After a successful
// Sync the file's bytes are durable.
func (f *File) Sync() error {
	stat, err := f.Stat()
	if err != nil {
		return err
	}

	// Re-encrypt + upload each dirty block.
	newBlocks := make(map[int]BlockID, len(f.dirty))
	for idx, buf := range f.dirty {
		id, err := f.fs.v.PutBlock(f.ctx, buf)
		if err != nil {
			return fmt.Errorf("vfs.File.Sync: PutBlock idx=%d: %w", idx, err)
		}
		newBlocks[idx] = id
	}

	// Splice new blocks into the inode's block list.
	f.fs.mu.Lock()
	in := f.fs.inodeByID(f.inodeID)
	totalBlocks := int((stat.Size + BlockSize - 1) / BlockSize)
	if len(in.Blocks) < totalBlocks {
		// Extend
		grown := make([]BlockID, totalBlocks)
		copy(grown, in.Blocks)
		in.Blocks = grown
	} else if len(in.Blocks) > totalBlocks {
		in.Blocks = in.Blocks[:totalBlocks]
	}
	for idx, id := range newBlocks {
		if idx < len(in.Blocks) {
			in.Blocks[idx] = id
		}
	}
	f.fs.dirty = true
	f.fs.mu.Unlock()

	// Clear local dirty cache once committed.
	f.dirty = nil

	// Persist the metadata tree.
	return f.fs.Sync(f.ctx)
}

// Close flushes pending writes and releases the handle. Mirrors
// os.File.Close — must be called even on read-only handles to allow
// the FS to release any resources.
func (f *File) Close() error {
	if len(f.dirty) > 0 {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	f.dirty = nil
	return nil
}

// readBlock returns the plaintext bytes for the given block index.
// Blocks past the inode's block list (but inside the file size, e.g.
// after a Truncate-grow that hasn't been Synced) read as zero.
func (f *File) readBlock(idx int, stat *Inode) ([]byte, error) {
	if buf, ok := f.dirty[idx]; ok {
		return buf, nil
	}
	if idx >= len(stat.Blocks) {
		return make([]byte, BlockSize), nil
	}
	id := stat.Blocks[idx]
	if id == "" {
		return make([]byte, BlockSize), nil
	}
	b, err := f.fs.v.GetBlock(f.ctx, id)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("vfs.File.readBlock: dangling block %s at idx %d", id, idx)
		}
		return nil, err
	}
	return b, nil
}

// Compile-time interface checks.
var (
	_ io.ReaderAt = (*File)(nil)
	_ io.WriterAt = (*File)(nil)
)
