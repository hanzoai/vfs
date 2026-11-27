package wsbuffer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

// Store is a hybrid dual-path journaled block/file writer implementing
// WSBuffer partial-write absorption with aligned scrap buffers and zero-copy
// direct bypass for large aligned writes.
//
// Store is safe for concurrent use by multiple goroutines.
type Store struct {
	dir     string
	cfg     Config
	backing BackingFile
	wal     WAL
	report  RecoveryReport

	// Optional hook invoked by Manager to enforce a shared global scrap budget.
	onScrapDelta func(delta int64)

	mu           sync.Mutex
	closed       bool
	scrap        *scrapBuffer
	writeSeq     uint64
	frameSeq     uint64
	lastWALSeq   uint64
	walDirty     bool
	backingDirty bool
	logicalSize  int64
	stats        Stats

	// Reusable scratch buffers (guarded by mu) to avoid per-flush heap churn.
	frameBuf    []byte
	coalesceBuf []byte
	barrierBuf  [64]byte
}

// Open opens or creates a WSBuffer Store in dir and replays any uncheckpointed
// WAL frames onto the backing file before returning.
func Open(dir string, cfg Config) (*Store, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	backingPath := cfg.BackingFile
	if backingPath == "" {
		backingPath = filepath.Join(dir, "data.bin")
	}
	backing, err := openOSBackingFile(backingPath, cfg.PreallocBytes)
	if err != nil {
		return nil, err
	}
	backingSize, err := backing.Size()
	if err != nil {
		backing.Close()
		return nil, err
	}

	var wal WAL
	switch cfg.WALMode {
	case WALModeJournal:
		wal, err = openJournalWAL(dir, cfg)
	case WALModeFile:
		wal, err = openFileWAL(dir)
	case WALModeNone:
		wal = nil
	default:
		err = fmt.Errorf("%w: unknown WALMode %d", ErrInvalidConfig, cfg.WALMode)
	}
	if err != nil {
		backing.Close()
		return nil, err
	}

	s := &Store{
		dir:         dir,
		cfg:         cfg,
		backing:     backing,
		wal:         wal,
		scrap:       newScrapBuffer(cfg.PageSize, cfg.SlabSize),
		logicalSize: backingSize,
	}

	if err := s.recover(); err != nil {
		if wal != nil {
			wal.Close()
		}
		backing.Close()
		return nil, err
	}
	return s, nil
}

// recover replays durable uncheckpointed frames from WAL onto backing,
// respecting FrameDirectBarrier ordering and VolumeID isolation.
func (s *Store) recover() error {
	if s.wal == nil {
		return nil
	}
	var frames []Frame
	var maxCheckpoint uint64
	var maxFrameSeq uint64
	var maxWriteSeq uint64

	truncated, err := s.wal.Replay(s.cfg.StrictRecovery, func(raw []byte) error {
		f, consumed, derr := DecodeFrame(raw)
		if derr != nil {
			return derr
		}
		if consumed != len(raw) {
			return fmt.Errorf("%w: frame trailing bytes (%d != %d)", ErrCorrupt, consumed, len(raw))
		}
		if f.VolumeID != s.cfg.VolumeID {
			return fmt.Errorf("%w: frame volume %d != store volume %d", ErrVolumeMismatch, f.VolumeID, s.cfg.VolumeID)
		}
		if f.FrameSeq > maxFrameSeq {
			maxFrameSeq = f.FrameSeq
		}
		switch f.Type {
		case FrameCheckpoint:
			s.report.CheckpointsSeen++
			if f.CheckpointSeq > maxCheckpoint {
				maxCheckpoint = f.CheckpointSeq
			}
		case FrameDirectBarrier:
			if f.Barrier.WriteSeq > maxWriteSeq {
				maxWriteSeq = f.Barrier.WriteSeq
			}
			frames = append(frames, f)
		case FrameScrapBatch:
			for _, e := range f.Extents {
				if e.WriteSeq > maxWriteSeq {
					maxWriteSeq = e.WriteSeq
				}
			}
			frames = append(frames, f)
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.report.TruncatedTailBytes = truncated

	// Collect active direct barriers strictly newer than the last checkpoint.
	var barriers []DirectBarrier
	for _, f := range frames {
		if f.Type == FrameDirectBarrier && f.Barrier.WriteSeq > maxCheckpoint {
			barriers = append(barriers, f.Barrier)
			s.report.BarriersApplied++
		}
	}

	appliedAny := false
	for _, f := range frames {
		if f.Type != FrameScrapBatch {
			continue
		}
		s.report.FramesReplayed++
		for _, ext := range f.Extents {
			if ext.WriteSeq <= maxCheckpoint {
				continue
			}
			surviving := subtractBarriers(ext, barriers)
			if len(surviving) == 0 {
				s.report.SupersededExtentsSkipped++
				continue
			}
			for _, sub := range surviving {
				if len(sub.Data) == 0 {
					continue
				}
				if _, err := s.backing.WriteAt(sub.Data, sub.Offset); err != nil {
					return err
				}
				end := sub.Offset + int64(len(sub.Data))
				if end > s.logicalSize {
					s.logicalSize = end
				}
				s.report.ExtentsApplied++
				s.report.BytesApplied += uint64(len(sub.Data))
				appliedAny = true
			}
		}
	}

	if appliedAny {
		if err := s.backing.Sync(); err != nil {
			return err
		}
	}
	if maxCheckpoint > maxWriteSeq {
		maxWriteSeq = maxCheckpoint
	}
	s.writeSeq = maxWriteSeq
	s.frameSeq = maxFrameSeq
	s.report.HighestSeq = maxWriteSeq
	return nil
}

// subtractBarriers removes any sub-ranges of ext covered by a DirectBarrier
// with strictly higher WriteSeq.
func subtractBarriers(ext Extent, barriers []DirectBarrier) []Extent {
	if len(ext.Data) == 0 {
		return nil
	}
	cur := []Extent{ext}
	for _, b := range barriers {
		if b.WriteSeq <= ext.WriteSeq || b.Length == 0 {
			continue
		}
		bStart := b.Offset
		bEnd := b.Offset + int64(b.Length)
		var next []Extent
		for _, seg := range cur {
			sStart := seg.Offset
			sEnd := seg.Offset + int64(len(seg.Data))
			if bEnd <= sStart || bStart >= sEnd {
				next = append(next, seg)
				continue
			}
			if bStart > sStart {
				leftLen := bStart - sStart
				next = append(next, Extent{
					Offset:   sStart,
					WriteSeq: seg.WriteSeq,
					Data:     seg.Data[:leftLen],
				})
			}
			if bEnd < sEnd {
				rightOff := bEnd - sStart
				next = append(next, Extent{
					Offset:   bEnd,
					WriteSeq: seg.WriteSeq,
					Data:     seg.Data[rightOff:],
				})
			}
		}
		cur = next
		if len(cur) == 0 {
			return nil
		}
	}
	return cur
}

func (s *Store) isDirectBypass(off int64, length int) bool {
	ps := int64(s.cfg.PageSize)
	return length >= s.cfg.BypassThreshold && off%ps == 0 && int64(length)%ps == 0
}

// WriteAt writes len(p) bytes at logical offset off.
// Small or unaligned writes are absorbed into aligned scrap slabs; large
// page-aligned writes bypass the scrap buffer directly with zero extra copy.
func (s *Store) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off < 0 || int64(len(p)) > math.MaxInt64-off {
		return 0, ErrInvalidRange
	}
	if len(p) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrClosed
	}

	s.writeSeq++
	seq := s.writeSeq
	s.stats.LogicalWrites++
	s.stats.LogicalBytes += uint64(len(p))

	var scrapDelta int64

	if s.isDirectBypass(off, len(p)) {
		// Path A: Zero-Copy Direct Bypass.
		s.stats.BypassWrites++
		s.stats.BypassBytes += uint64(len(p))

		ps := int64(s.cfg.PageSize)
		firstPage := off / ps
		lastPage := (off + int64(len(p))) / ps

		prevDirty := s.scrap.dirtyBytes
		superseded := s.scrap.invalidateRange(firstPage, lastPage)
		if superseded > 0 {
			s.stats.BypassSupersededBytes += uint64(superseded)
			scrapDelta = s.scrap.dirtyBytes - prevDirty
		}

		// Write directly from caller's slice p with zero intermediate copy.
		n, err := s.backing.WriteAt(p, off)
		if err != nil {
			s.mu.Unlock()
			s.notifyScrapDelta(scrapDelta)
			return n, err
		}
		if n != len(p) {
			s.mu.Unlock()
			s.notifyScrapDelta(scrapDelta)
			return n, io.ErrShortWrite
		}
		s.backingDirty = true
		if end := off + int64(len(p)); end > s.logicalSize {
			s.logicalSize = end
		}

		// If uncheckpointed scrap frames exist in WAL, emit a compact 56-byte
		// zero-payload barrier so crash recovery never replays older scrap data
		// over this newer direct write.
		if s.wal != nil && s.walDirty {
			s.frameSeq++
			bframe := EncodeDirectBarrierFrame(s.barrierBuf[:0], s.cfg.VolumeID, s.frameSeq, DirectBarrier{
				Offset:   off,
				Length:   uint32(len(p)),
				WriteSeq: seq,
			})
			wseq, werr := s.wal.AppendFrame(ctx, bframe)
			if werr != nil {
				s.mu.Unlock()
				s.notifyScrapDelta(scrapDelta)
				return len(p), werr
			}
			s.lastWALSeq = wseq
			s.stats.DirectBarriers++
		}

		s.mu.Unlock()
		s.notifyScrapDelta(scrapDelta)
		return len(p), nil
	}

	// Path B: Scrap-Buffer Absorption.
	s.stats.ScrapWrites++
	s.stats.ScrapBytes += uint64(len(p))

	prevDirty := s.scrap.dirtyBytes
	coalesced := s.scrap.put(off, p, seq)
	if coalesced > 0 {
		s.stats.CoalescedWrites++
		s.stats.CoalescedBytes += uint64(coalesced)
	}
	if end := off + int64(len(p)); end > s.logicalSize {
		s.logicalSize = end
	}

	// Enforce bounded scrap memory cap with deterministic backpressure flush.
	slabCapLimit := max(s.cfg.MaxScrapBytes*2, int64(s.cfg.SlabSize)*2)
	var flushErr error
	if s.scrap.dirtyBytes >= s.cfg.MaxScrapBytes || s.scrap.slabBytes >= slabCapLimit {
		flushErr = s.flushScrapLocked(ctx, true)
	}
	scrapDelta = s.scrap.dirtyBytes - prevDirty
	s.mu.Unlock()

	s.notifyScrapDelta(scrapDelta)
	if flushErr != nil {
		return 0, flushErr
	}
	return len(p), nil
}

func (s *Store) notifyScrapDelta(delta int64) {
	if delta != 0 && s.onScrapDelta != nil {
		s.onScrapDelta(delta)
	}
}

// ReadAt performs a linearizable read of len(p) bytes at offset off, seamlessly
// merging committed backing-file bytes with live uncommitted scrap overlays.
func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || int64(len(p)) > math.MaxInt64-off {
		return 0, ErrInvalidRange
	}
	if len(p) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	s.stats.ReadCalls++

	if off >= s.logicalSize {
		return 0, io.EOF
	}
	want := len(p)
	eof := false
	if off+int64(want) > s.logicalSize {
		want = int(s.logicalSize - off)
		eof = true
	}

	// Read committed base bytes from backing file; zero-fill any hole beyond physical EOF.
	dst := p[:want]
	n, err := s.backing.ReadAt(dst, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, err
	}
	if n < want {
		clear(dst[n:])
	}

	// Overlay live uncommitted scrap extents on top of committed base bytes.
	if s.scrap.overlayRead(off, dst) {
		s.stats.ReadScrapHits++
	}
	if eof {
		return want, io.EOF
	}
	return want, nil
}

// Flush drains all live scrap extents to the WAL and backing file without
// forcing an fsync.
func (s *Store) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	prev := s.scrap.dirtyBytes
	err := s.flushScrapLocked(ctx, false)
	delta := s.scrap.dirtyBytes - prev
	s.mu.Unlock()
	s.notifyScrapDelta(delta)
	return err
}

// Sync flushes all buffered scrap extents, makes the WAL and backing file
// durable via fdatasync, and checkpoints/reclaims the WAL when enabled.
func (s *Store) Sync(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.stats.SyncCalls++

	prev := s.scrap.dirtyBytes
	if err := s.flushScrapLocked(ctx, false); err != nil {
		delta := s.scrap.dirtyBytes - prev
		s.mu.Unlock()
		s.notifyScrapDelta(delta)
		return err
	}
	delta := s.scrap.dirtyBytes - prev

	// Step 1: Make WAL durable before/alongside backing file sync.
	if s.wal != nil && s.walDirty {
		if err := s.wal.Sync(ctx, s.lastWALSeq); err != nil {
			s.mu.Unlock()
			s.notifyScrapDelta(delta)
			return err
		}
	}

	// Step 2: Make backing file durable.
	if s.backingDirty {
		if err := s.backing.Sync(); err != nil {
			s.mu.Unlock()
			s.notifyScrapDelta(delta)
			return err
		}
		s.backingDirty = false
	}

	// Step 3: Advance WAL checkpoint & reclaim segments once backing is durable.
	if s.wal != nil && s.walDirty && !s.cfg.DisableAutoCheckpoint {
		if err := s.wal.Checkpoint(s.lastWALSeq, false); err != nil {
			s.mu.Unlock()
			s.notifyScrapDelta(delta)
			return err
		}
		s.walDirty = false
	}

	s.mu.Unlock()
	s.notifyScrapDelta(delta)
	return nil
}

// flushScrapLocked drains dirty scrap extents to WAL (batched CRC32C frames)
// and applies them to the backing file using contiguous-run coalescing.
// Caller must hold s.mu.
func (s *Store) flushScrapLocked(ctx context.Context, backpressure bool) error {
	if s.scrap.dirtyBytes == 0 {
		return nil
	}
	extents, release := s.scrap.drainSorted()
	defer release()

	s.stats.ScrapFlushes++
	if backpressure {
		s.stats.BackpressureFlushes++
	}

	// 1. Append coalesced scrap batch frame(s) to WAL.
	if s.wal != nil {
		start := 0
		for start < len(extents) {
			end := start
			bytesInFrame := 4
			for end < len(extents) {
				extCost := extentDescSize + len(extents[end].Data)
				if end > start && bytesInFrame+extCost > s.cfg.MaxFrameBytes {
					break
				}
				bytesInFrame += extCost
				end++
			}
			s.frameSeq++
			s.frameBuf = EncodeScrapBatchFrame(s.frameBuf[:0], s.cfg.VolumeID, s.frameSeq, extents[start:end])
			wseq, err := s.wal.AppendFrame(ctx, s.frameBuf)
			if err != nil {
				return err
			}
			s.lastWALSeq = wseq
			s.walDirty = true
			start = end
		}
	}

	// 2. Apply sorted extents to backing file, merging contiguous runs into single WriteAt syscalls.
	i := 0
	for i < len(extents) {
		j := i + 1
		runBytes := len(extents[i].Data)
		runEnd := extents[i].Offset + int64(runBytes)
		for j < len(extents) && extents[j].Offset == runEnd && runBytes+len(extents[j].Data) <= s.cfg.MaxFrameBytes {
			runBytes += len(extents[j].Data)
			runEnd += int64(len(extents[j].Data))
			j++
		}
		if j == i+1 {
			// Single extent: write directly from slab slice (zero extra copy).
			n, err := s.backing.WriteAt(extents[i].Data, extents[i].Offset)
			if err != nil {
				return err
			}
			if n != len(extents[i].Data) {
				return io.ErrShortWrite
			}
		} else {
			// Adjacent extents: coalesce into one contiguous aligned WriteAt syscall.
			if cap(s.coalesceBuf) < runBytes {
				s.coalesceBuf = make([]byte, runBytes)
			}
			buf := s.coalesceBuf[:runBytes]
			pos := 0
			for k := i; k < j; k++ {
				copy(buf[pos:pos+len(extents[k].Data)], extents[k].Data)
				pos += len(extents[k].Data)
			}
			n, err := s.backing.WriteAt(buf, extents[i].Offset)
			if err != nil {
				return err
			}
			if n != len(buf) {
				return io.ErrShortWrite
			}
		}
		s.backingDirty = true
		i = j
	}
	return nil
}

// Close flushes and syncs all remaining data, checkpoints the WAL, and closes
// underlying files.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	prev := s.scrap.dirtyBytes
	var firstErr error
	if err := s.flushScrapLocked(context.Background(), false); err != nil {
		firstErr = err
	}
	delta := s.scrap.dirtyBytes - prev

	if firstErr == nil && s.wal != nil && s.walDirty {
		if err := s.wal.Sync(context.Background(), s.lastWALSeq); err != nil {
			firstErr = err
		}
	}
	if firstErr == nil && s.backingDirty {
		if err := s.backing.Sync(); err != nil {
			firstErr = err
		}
		s.backingDirty = false
	}
	if firstErr == nil && s.wal != nil && s.walDirty && !s.cfg.DisableAutoCheckpoint {
		if err := s.wal.Checkpoint(s.lastWALSeq, true); err != nil {
			firstErr = err
		}
		s.walDirty = false
	}

	s.closed = true
	if s.wal != nil {
		if err := s.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := s.backing.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	s.mu.Unlock()

	s.notifyScrapDelta(delta)
	return firstErr
}

// AbortCrash simulates an ungraceful process crash by closing file handles
// WITHOUT flushing live scrap memory or checkpointing the WAL. Intended for
// crash-recovery tests.
func (s *Store) AbortCrash() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	prev := s.scrap.dirtyBytes
	s.scrap.releaseSlabs()
	s.scrap.dirtyPages = nil
	s.scrap.dirtyBytes = 0
	s.closed = true

	var firstErr error
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			firstErr = err
		}
	}
	if err := s.backing.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	s.mu.Unlock()

	s.notifyScrapDelta(-prev)
	return firstErr
}

// Stats returns a point-in-time snapshot of logical and physical I/O counters.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	st := s.stats
	if s.scrap != nil {
		st.CurrentScrapBytes = s.scrap.dirtyBytes
	}
	s.mu.Unlock()

	if s.wal != nil {
		st.WALPhysicalWrites, st.WALPhysicalBytes, st.WALSyncs = s.wal.Stats()
	}
	st.BackingPhysicalWrites, st.BackingPhysicalBytes, st.BackingSyncs = s.backing.Stats()
	return st
}

// Recovery returns the RecoveryReport produced when the Store was opened.
func (s *Store) Recovery() RecoveryReport {
	return s.report
}

// Size returns the current logical size of the backing store in bytes.
func (s *Store) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logicalSize
}
