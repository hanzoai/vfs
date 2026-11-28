// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package wsbuffer measures, and reduces, the page-level write amplification of
// the journal's foreground writes (HIP-1344, Bounty #1).
//
// The journal hands the file layer one WriteAt per batch, and a batch is written
// wherever the previous one ended. Under concurrency a sync round carries
// several short batches, each at an offset the writer did not choose, so a media
// that writes whole pages must read and rewrite a page at both ends of every one
// of them: the bytes written are the logical bytes, the bytes the media writes
// are rounded out to the page at each end. Handing the media one run instead of
// several pieces removes the boundaries between them, which is what a scrap
// buffer does in WSBuffer.
//
// Scrap is a journal.Disk between the journal and its files. It counts what the
// journal asks for and what a page-granular media would write for it and, when
// its Options ask for it, holds the bytes of a small write in memory until the
// next write continues them, overwrites them, or a sync, a read or a full window
// ends the grace period. What reaches the file is the same byte stream in the
// same order: the layer changes how many calls the media sees, never what the
// files hold.
//
// The design follows WSBuffer (Zhan et al., "Rearchitecting Buffered I/O in the
// Era of High-Bandwidth SSDs", FAST '26, pp. 579-595): buffer the small and
// unaligned parts of a write, send the large and aligned parts straight through,
// flush the buffered dirt opportunistically. It is a clean-room implementation
// from the paper's description; no code from it was read or copied. What is not
// implemented is named in docs/wsbuffer.md.
//
// The counters are the measurement. Pages is what a page-granular media writes
// for a workload and RefPages is what it would write without the layer, both
// computed from the same call sequence, so a comparison needs no second
// execution.
package wsbuffer

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/hanzoai/vfs/journal"
)

// PageSize is the page the counters round writes out to. 4 KiB is the unit every
// SSD in reach reports for a write that cannot be merged into the one before it.
const PageSize = 4096

// DefaultRun is the window Scrap coalesces by when Options.Run asks for the
// default: one mebibyte, which is also the span the journal lets pass before it
// asks the system to start writing it out (hintBytes), so the journal's own hint
// ends a window and it never grows past it.
const DefaultRun = 1 << 20

// Options configure a Disk.
type Options struct {
	// Page is the page the media writes, in bytes. Zero is PageSize.
	Page int
	// Run is the most contiguous bytes held before they are written. Zero is
	// DefaultRun; a negative value writes everything straight through, which is
	// the baseline half of a measurement.
	Run int
}

func (o Options) page() int64 {
	if o.Page <= 0 {
		return PageSize
	}
	return int64(o.Page)
}

func (o Options) window() int {
	switch {
	case o.Run == 0:
		return DefaultRun
	case o.Run < 0:
		return 0
	}
	return o.Run
}

// Stats count what one Disk did. The Ref counters are the same workload with the
// layer out of the way: Pages/RefPages is the amplification the layer removed.
type Stats struct {
	Calls   uint64 // WriteAt calls that reached the files
	Bytes   uint64 // bytes those calls wrote
	Pages   uint64 // pages a media of Options.Page bytes writes for them
	Partial uint64 // those calls that do not cover whole pages, so the media reads first
	// Ref is the same for the calls the journal made, before coalescing.
	RefCalls   uint64
	RefBytes   uint64
	RefPages   uint64
	RefPartial uint64
	// Absorbed counts writes merged into a window instead of reaching the file,
	// and Flushed the windows that did reach it.
	Absorbed uint64
	Flushed  uint64
}

// Ratio is the write amplification of the coalesced calls: pages the media
// writes per page of logical bytes. Zero when nothing was written.
func (s Stats) Ratio() float64 {
	if s.Bytes == 0 {
		return 0
	}
	return float64(s.Pages*PageSize) / float64(s.Bytes)
}

// RefRatio is the same for the calls the journal made.
func (s Stats) RefRatio() float64 {
	if s.RefBytes == 0 {
		return 0
	}
	return float64(s.RefPages*PageSize) / float64(s.RefBytes)
}

// Disk is a journal.Disk that counts the page writes of the layer below it and,
// when its Options ask for it, coalesces the small ones.
type Disk struct {
	inner journal.Disk
	opt   Options

	calls, bytes, pages, partial             atomic.Uint64
	refCalls, refBytes, refPages, refPartial atomic.Uint64
	absorbed, flushed                        atomic.Uint64

	live  sync.Mutex
	files []*file // every file handed out, so Close flushes the ones left open
}

// New returns the counting Disk over inner.
func New(inner journal.Disk, opt Options) *Disk {
	return &Disk{inner: inner, opt: opt}
}

// Stats returns the counters.
func (d *Disk) Stats() Stats {
	return Stats{
		Calls: d.calls.Load(), Bytes: d.bytes.Load(), Pages: d.pages.Load(), Partial: d.partial.Load(),
		RefCalls: d.refCalls.Load(), RefBytes: d.refBytes.Load(), RefPages: d.refPages.Load(), RefPartial: d.refPartial.Load(),
		Absorbed: d.absorbed.Load(), Flushed: d.flushed.Load(),
	}
}

// Reset zeroes the counters, so one Disk can measure two phases of a workload.
// It touches neither the files nor what a file holds.
func (d *Disk) Reset() {
	for _, c := range []*atomic.Uint64{
		&d.calls, &d.bytes, &d.pages, &d.partial,
		&d.refCalls, &d.refBytes, &d.refPages, &d.refPartial,
		&d.absorbed, &d.flushed,
	} {
		c.Store(0)
	}
}

// List is the layer's.
func (d *Disk) List() ([]string, error) { return d.inner.List() }

// Remove is the layer's.
func (d *Disk) Remove(name string) error { return d.inner.Remove(name) }

// Close flushes every file this Disk handed out, then closes the layer. The
// journal closes a file before it closes the Disk and a file's own Close flushes
// its window, so this finds nothing but a file the journal opened and left.
func (d *Disk) Close() error {
	d.live.Lock()
	files := append([]*file(nil), d.files...)
	d.live.Unlock()
	for _, f := range files {
		f.mu.Lock()
		err := f.flushLocked()
		f.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return d.inner.Close()
}

// Create is the layer's, with the file it returns counted.
func (d *Disk) Create(name string, size int64) (journal.File, error) {
	f, err := d.inner.Create(name, size)
	if err != nil {
		return nil, err
	}
	return d.track(f), nil
}

// Open is the layer's, with the file it returns counted.
func (d *Disk) Open(name string) (journal.File, error) {
	f, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return d.track(f), nil
}

func (d *Disk) track(f journal.File) *file {
	w := &file{File: f, d: d, off: -1}
	d.live.Lock()
	d.files = append(d.files, w)
	d.live.Unlock()
	return w
}

// count records one call that reached the file: what the media sees.
func (d *Disk) count(off, n int64) {
	d.calls.Add(1)
	d.bytes.Add(uint64(n))
	d.pages.Add(pages(off, n, d.opt.page()))
	if off%d.opt.page() != 0 || n%d.opt.page() != 0 {
		d.partial.Add(1)
	}
}

// ref records one call the journal made, before any coalescing.
func (d *Disk) ref(off, n int64) {
	d.refCalls.Add(1)
	d.refBytes.Add(uint64(n))
	d.refPages.Add(pages(off, n, d.opt.page()))
	if off%d.opt.page() != 0 || n%d.opt.page() != 0 {
		d.refPartial.Add(1)
	}
}

// pages is how many pages of size p a media writes for n bytes at off: the two
// ends are rounded out to the page, and a page both ends share is written once.
func pages(off, n, p int64) uint64 {
	if n <= 0 {
		return 0
	}
	return uint64((off+n+p-1)/p - off/p)
}

// file is one segment file. Its window holds the bytes of the last small write
// until something needs them on the file.
type file struct {
	journal.File
	d *Disk

	mu  sync.Mutex
	run []byte // buffered bytes; len(run) == 0 when nothing is held
	off int64  // offset of run[0]; -1 when nothing is held
}

// WriteAt writes all of p, or returns an error.
func (f *file) WriteAt(p []byte, off int64) (int, error) {
	n := int64(len(p))
	f.d.ref(off, n)

	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.d.opt.window()
	if w <= 0 || len(p) > w {
		if err := f.flushLocked(); err != nil {
			return 0, err
		}
		return f.emit(off, p)
	}
	switch {
	case len(f.run) > 0 && off == f.off+int64(len(f.run)):
		// The write continues the window: one run reaches the media where the
		// journal asked for two.
		f.run = append(f.run, p...)
		f.d.absorbed.Add(1)
		if len(f.run) >= w {
			if err := f.flushLocked(); err != nil {
				return 0, err
			}
		}
		return len(p), nil
	case len(f.run) > 0 && off >= f.off && off+n <= f.off+int64(len(f.run)):
		// A rewrite inside the window, which is how a mark reaches a segment.
		// The last write wins, exactly as it would on the file.
		copy(f.run[off-f.off:], p)
		f.d.absorbed.Add(1)
		return len(p), nil
	}
	if err := f.flushLocked(); err != nil {
		return 0, err
	}
	f.run = append(f.run[:0], p...)
	f.off = off
	return len(p), nil
}

// emit is one write to the file, counted.
func (f *file) emit(off int64, p []byte) (int, error) {
	f.d.count(off, int64(len(p)))
	return f.File.WriteAt(p, off)
}

// flushLocked writes the window, if it holds anything. Caller holds mu.
func (f *file) flushLocked() error {
	if len(f.run) == 0 {
		f.off = -1
		return nil
	}
	n, err := f.emit(f.off, f.run)
	if err == nil && n != len(f.run) {
		err = io.ErrShortWrite
	}
	f.run, f.off = f.run[:0], -1
	f.d.flushed.Add(1)
	return err
}

// ReadAt reads the file. Anything the window still holds is written first: a
// reader of a segment, which is how Replay and Reclaim see one, must see every
// record the journal says it wrote.
func (f *file) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushLocked(); err != nil {
		return 0, err
	}
	return f.File.ReadAt(p, off)
}

// Writeback asks the system to start writing a range out, after the bytes that
// range covers have reached the file.
func (f *file) Writeback(off, n int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushLocked(); err != nil {
		return err
	}
	return f.File.Writeback(off, n)
}

// Sync makes what the journal wrote durable, which includes the window.
func (f *file) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushLocked(); err != nil {
		return err
	}
	return f.File.Sync()
}

// Size is the file's length, which the window never changes.
func (f *file) Size() int64 { return f.File.Size() }

// Close writes the window, then closes the file.
func (f *file) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.flushLocked()
	if cerr := f.File.Close(); err == nil {
		err = cerr
	}
	return err
}
