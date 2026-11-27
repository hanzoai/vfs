package wsbuffer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/hanzoai/vfs/journal"
)

// BackingFile abstracts the backing data file with physical I/O accounting.
type BackingFile interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Sync() error
	Size() (int64, error)
	Truncate(size int64) error
	Close() error
	Stats() (writes, bytes, syncs uint64)
}

type osBackingFile struct {
	f      *os.File
	writes atomic.Uint64
	bytes  atomic.Uint64
	syncs  atomic.Uint64
}

func openOSBackingFile(path string, prealloc int64) (*osBackingFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if prealloc > 0 && info.Size() < prealloc {
		zeros := make([]byte, 1<<20)
		for off := info.Size(); off < prealloc; {
			n := prealloc - off
			if n > int64(len(zeros)) {
				n = int64(len(zeros))
			}
			if _, err := f.WriteAt(zeros[:n], off); err != nil {
				f.Close()
				return nil, err
			}
			off += n
		}
		if err := fdatasyncFile(f); err != nil {
			f.Close()
			return nil, err
		}
	}
	return &osBackingFile{f: f}, nil
}

func (b *osBackingFile) ReadAt(p []byte, off int64) (int, error) {
	return b.f.ReadAt(p, off)
}

func (b *osBackingFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := b.f.WriteAt(p, off)
	if n > 0 {
		b.writes.Add(1)
		b.bytes.Add(uint64(n))
	}
	return n, err
}

func (b *osBackingFile) Sync() error {
	b.syncs.Add(1)
	return fdatasyncFile(b.f)
}

func (b *osBackingFile) Size() (int64, error) {
	info, err := b.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (b *osBackingFile) Truncate(size int64) error {
	return b.f.Truncate(size)
}

func (b *osBackingFile) Close() error {
	return b.f.Close()
}

func (b *osBackingFile) Stats() (writes, bytes, syncs uint64) {
	return b.writes.Load(), b.bytes.Load(), b.syncs.Load()
}

func fdatasyncFile(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// CountingDisk wraps a journal.Disk and tracks exact physical WriteAt and Sync
// operations issued by github.com/hanzoai/vfs/journal.
type CountingDisk struct {
	journal.Disk
	Writes atomic.Uint64
	Bytes  atomic.Uint64
	Syncs  atomic.Uint64
}

type countingFile struct {
	journal.File
	d *CountingDisk
}

// NewCountingDisk wraps base with exact physical byte, write, and sync counters.
func NewCountingDisk(base journal.Disk) *CountingDisk {
	return &CountingDisk{Disk: base}
}

func (c *CountingDisk) Create(name string, size int64) (journal.File, error) {
	f, err := c.Disk.Create(name, size)
	if err != nil {
		return nil, err
	}
	return countingFile{File: f, d: c}, nil
}

func (c *CountingDisk) Open(name string) (journal.File, error) {
	f, err := c.Disk.Open(name)
	if err != nil {
		return nil, err
	}
	return countingFile{File: f, d: c}, nil
}

func (f countingFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	if n > 0 {
		f.d.Writes.Add(1)
		f.d.Bytes.Add(uint64(n))
	}
	return n, err
}

func (f countingFile) Sync() error {
	f.d.Syncs.Add(1)
	return f.File.Sync()
}

// WAL abstracts the crash-recovery log backend (JournalWAL or FileWAL).
type WAL interface {
	AppendFrame(ctx context.Context, frame []byte) (uint64, error)
	Sync(ctx context.Context, seq uint64) error
	Replay(strict bool, fn func(frame []byte) error) (truncatedBytes int64, err error)
	Checkpoint(seq uint64, forceReclaim bool) error
	Stats() (writes, bytes, syncs uint64)
	Close() error
}

// ---- JournalWAL (backed by github.com/hanzoai/vfs/journal) ----

var ckptMagic = [4]byte{'W', 'C', 'K', '1'}

const ckptFileSize = 16

type journalWAL struct {
	dir          string
	ckptPath     string
	j            *journal.Journal
	disk         *CountingDisk
	segment      int64
	published    uint64
	reclaimBytes int64
	sinceReclaim int64
	mu           sync.Mutex
}

func openJournalWAL(dir string, cfg Config) (*journalWAL, error) {
	jdir := filepath.Join(dir, "journal")
	if err := os.MkdirAll(jdir, 0o700); err != nil {
		return nil, err
	}
	ckptPath := filepath.Join(dir, "journal.ckpt")
	published, err := loadCheckpointMeta(ckptPath)
	if err != nil {
		return nil, err
	}

	var baseDisk journal.Disk
	if cfg.JournalDisk != nil {
		baseDisk, err = cfg.JournalDisk(jdir)
	} else {
		baseDisk, err = journal.Files(jdir)
	}
	if err != nil {
		return nil, err
	}
	cd := NewCountingDisk(baseDisk)

	lineage := cfg.JournalLineage
	if len(lineage) == 0 {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], cfg.VolumeID)
		lineage = append([]byte("wsbuffer-vol-"), buf[:]...)
	}

	j, err := journal.Open(jdir, journal.Options{
		Capacity:  cfg.JournalCapacity,
		Segment:   cfg.JournalSegment,
		Published: published,
		Lineage:   lineage,
		Disk:      cd,
	})
	if err != nil {
		return nil, err
	}
	return &journalWAL{
		dir:          jdir,
		ckptPath:     ckptPath,
		j:            j,
		disk:         cd,
		segment:      cfg.JournalSegment,
		published:    published,
		reclaimBytes: cfg.JournalSegment / 2,
	}, nil
}

func (w *journalWAL) AppendFrame(ctx context.Context, frame []byte) (uint64, error) {
	seq, err := w.j.Append(ctx, frame)
	if errors.Is(err, journal.ErrFull) {
		// Reclaim already-published segments and retry once.
		if rerr := w.reclaimLocked(true); rerr == nil {
			seq, err = w.j.Append(ctx, frame)
		}
	}
	if err == nil {
		w.mu.Lock()
		w.sinceReclaim += int64(len(frame)) + 28
		w.mu.Unlock()
	}
	return seq, err
}

func (w *journalWAL) Sync(ctx context.Context, seq uint64) error {
	if seq == 0 {
		return nil
	}
	return w.j.Sync(ctx, seq)
}

func (w *journalWAL) Replay(_ bool, fn func(frame []byte) error) (int64, error) {
	err := w.j.Replay(func(r journal.Record) error {
		return fn(r.Data)
	})
	return 0, err
}

func (w *journalWAL) Checkpoint(seq uint64, forceReclaim bool) error {
	if seq == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq <= w.published {
		return nil
	}
	if err := w.j.Publish(seq); err != nil {
		return err
	}
	w.published = seq
	if forceReclaim || w.sinceReclaim >= w.reclaimBytes {
		return w.reclaimAlreadyLocked()
	}
	return nil
}

func (w *journalWAL) reclaimLocked(force bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !force && w.sinceReclaim < w.reclaimBytes {
		return nil
	}
	return w.reclaimAlreadyLocked()
}

func (w *journalWAL) reclaimAlreadyLocked() error {
	// Persist the published watermark BEFORE Reclaim() writes a floor mark
	// into the oldest surviving segment, so Open() always passes Published >= floor.
	if err := saveCheckpointMeta(w.ckptPath, w.published); err != nil {
		return err
	}
	if err := w.j.Reclaim(); err != nil {
		return err
	}
	w.sinceReclaim = 0
	return nil
}

func (w *journalWAL) Stats() (writes, bytes, syncs uint64) {
	return w.disk.Writes.Load(), w.disk.Bytes.Load(), w.disk.Syncs.Load()
}

func (w *journalWAL) Close() error {
	w.mu.Lock()
	if w.published > 0 {
		_ = saveCheckpointMeta(w.ckptPath, w.published)
	}
	w.mu.Unlock()
	return w.j.Close()
}

func loadCheckpointMeta(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(data) != ckptFileSize || string(data[0:4]) != string(ckptMagic[:]) {
		return 0, fmt.Errorf("%w: corrupt checkpoint meta %s", ErrCorrupt, path)
	}
	want := binary.LittleEndian.Uint32(data[12:16])
	got := crc32.Checksum(data[0:12], castagnoli)
	if want != got {
		return 0, fmt.Errorf("%w: checkpoint meta crc mismatch", ErrChecksumMismatch)
	}
	return binary.LittleEndian.Uint64(data[4:12]), nil
}

func saveCheckpointMeta(path string, published uint64) error {
	var buf [ckptFileSize]byte
	copy(buf[0:4], ckptMagic[:])
	binary.LittleEndian.PutUint64(buf[4:12], published)
	binary.LittleEndian.PutUint32(buf[12:16], crc32.Checksum(buf[0:12], castagnoli))

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := fdatasyncFile(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// ---- FileWAL (standalone crash-safe WAL file) ----

type fileWAL struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	off    int64
	seq    uint64
	writes atomic.Uint64
	bytes  atomic.Uint64
	syncs  atomic.Uint64
}

func openFileWAL(dir string) (*fileWAL, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "wsbuffer.wal")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &fileWAL{
		f:    f,
		path: path,
		off:  info.Size(),
	}, nil
}

func (w *fileWAL) AppendFrame(_ context.Context, frame []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.f.WriteAt(frame, w.off)
	if n > 0 {
		w.writes.Add(1)
		w.bytes.Add(uint64(n))
	}
	if err != nil {
		return 0, err
	}
	if n != len(frame) {
		return 0, io.ErrShortWrite
	}
	w.off += int64(n)
	w.seq++
	return w.seq, nil
}

func (w *fileWAL) Sync(_ context.Context, _ uint64) error {
	w.syncs.Add(1)
	return fdatasyncFile(w.f)
}

func (w *fileWAL) Replay(strict bool, fn func(frame []byte) error) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := os.ReadFile(w.path)
	if err != nil {
		return 0, err
	}
	var pos int
	for pos < len(data) {
		_, consumed, derr := DecodeFrame(data[pos:])
		if derr != nil {
			if strict {
				return 0, derr
			}
			// Torn tail or corrupt trailing bytes: truncate cleanly at the last valid frame.
			truncated := int64(len(data) - pos)
			if err := w.f.Truncate(int64(pos)); err != nil {
				return 0, err
			}
			if err := fdatasyncFile(w.f); err != nil {
				return 0, err
			}
			w.off = int64(pos)
			return truncated, nil
		}
		if err := fn(data[pos : pos+consumed]); err != nil {
			return 0, err
		}
		pos += consumed
		w.seq++
	}
	w.off = int64(pos)
	return 0, nil
}

func (w *fileWAL) Checkpoint(_ uint64, _ bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.off == 0 {
		return nil
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	w.off = 0
	return nil
}

func (w *fileWAL) Stats() (writes, bytes, syncs uint64) {
	return w.writes.Load(), w.bytes.Load(), w.syncs.Load()
}

func (w *fileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
