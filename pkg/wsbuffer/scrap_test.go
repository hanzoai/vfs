// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package wsbuffer

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/journal"
)

// open returns the layer over the operating system's files in a fresh directory.
func open(t testing.TB, opt Options) (*Disk, string) {
	t.Helper()
	dir := t.TempDir()
	d, err := journal.Files(dir)
	if err != nil {
		t.Fatalf("files: %v", err)
	}
	return New(d, opt), dir
}

// script is the write sequence both halves of every test apply: a contiguous
// run split by the record size, a rewrite inside it, a gap, and a last piece at
// the end of the file.
var script = []struct {
	off int64
	n   int
}{
	{0, 100}, {100, 4024}, {0, 20}, {4124, 4876}, {20000, 10}, {65536 - 5, 5},
}

func write(t testing.TB, d *Disk, dir string) journal.File {
	t.Helper()
	f, err := d.Create("000000.seg", 64<<10)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	buf := make([]byte, 8192)
	for i := range buf {
		buf[i] = byte(i*7 + 3)
	}
	for i, w := range script {
		// Each write carries a different byte, so an absorbed rewrite is visible
		// in the file and not just in the counters.
		for j := range w.n {
			buf[j] = byte(i*11 + j)
		}
		if _, err := f.WriteAt(buf[:w.n], w.off); err != nil {
			t.Fatalf("write %d at %d: %v", w.n, w.off, err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	return f
}

// TestScrapWritesTheSameBytes is the correctness oracle of the layer: a window,
// a rewrite inside it and an unaligned tail leave the file holding exactly what
// the journal wrote, byte for byte.
func TestScrapWritesTheSameBytes(t *testing.T) {
	raw, rawDir := open(t, Options{Run: -1})
	scrap, scrapDir := open(t, Options{Run: 1 << 20})

	rf := write(t, raw, rawDir)
	sf := write(t, scrap, scrapDir)

	rf.Close()
	sf.Close()
	raw.Close()
	scrap.Close()

	want, err := os.ReadFile(filepath.Join(rawDir, "000000.seg"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(scrapDir, "000000.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("size %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("byte %d is %d, want %d", i, got[i], want[i])
		}
	}
}

// TestScrapCountsPages checks the counters against the arithmetic of the script:
// six calls that a 4 KiB media rounds out to eight pages, and a window that
// hands it five, holding the twenty bytes the rewrite replaced.
func TestScrapCountsPages(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opt      Options
		pages    uint64
		calls    uint64
		bytes    uint64
		absorbed uint64
	}{
		{"through", Options{Run: -1}, 8, 6, 9035, 0},
		{"window", Options{Run: 1 << 20}, 5, 3, 9015, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, dir := open(t, tc.opt)
			f := write(t, d, dir)
			f.Close()
			d.Close()

			s := d.Stats()
			if s.RefPages != 8 || s.RefCalls != 6 || s.RefBytes != 9035 {
				t.Fatalf("journal asked for %d calls over %d pages and %d bytes, want 6 over 8 and 9035",
					s.RefCalls, s.RefPages, s.RefBytes)
			}
			if s.Pages != tc.pages || s.Calls != tc.calls || s.Bytes != tc.bytes {
				t.Fatalf("media saw %d calls over %d pages and %d bytes, want %d over %d and %d",
					s.Calls, s.Pages, s.Bytes, tc.calls, tc.pages, tc.bytes)
			}
			if s.Absorbed != tc.absorbed {
				t.Fatalf("absorbed %d calls, want %d", s.Absorbed, tc.absorbed)
			}
		})
	}
}

// TestScrapFlushesOnRead proves a reader of a segment sees every byte the
// journal wrote, even when the window still holds the last of them: Replay
// reads a segment through this layer.
func TestScrapFlushesOnRead(t *testing.T) {
	d, dir := open(t, Options{Run: 1 << 20})
	f, err := d.Create("000000.seg", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close(); d.Close() })

	if _, err := f.WriteAt([]byte("hello"), 4096); err != nil {
		t.Fatal(err)
	}
	if s := d.Stats(); s.Bytes != 0 {
		t.Fatalf("%d bytes reached the file before a read: the window held nothing", s.Bytes)
	}
	got := make([]byte, 5)
	if _, err := f.ReadAt(got, 4096); err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("read %q, want hello", got)
	}
	if s := d.Stats(); s.Bytes != 5 {
		t.Fatalf("%d bytes reached the file, want 5: a read must flush the window", s.Bytes)
	}
	if _, err := os.Stat(filepath.Join(dir, "000000.seg")); err != nil {
		t.Fatal(err)
	}
}

// TestScrapFullWindowWrites: a window that fills is written without waiting for
// a sync, so a writer that never syncs cannot grow the buffer past Options.Run.
func TestScrapFullWindowWrites(t *testing.T) {
	opts := Options{Run: 4096}
	d, _ := open(t, opts)
	f, err := d.Create("000000.seg", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close(); d.Close() })

	p := make([]byte, 1000)
	for i := range 4 {
		if _, err := f.WriteAt(p, int64(i*1000)); err != nil {
			t.Fatal(err)
		}
	}
	if s := d.Stats(); s.Pages != 0 {
		t.Fatalf("%d pages written for 4000 bytes below a window of %d", s.Pages, opts.window())
	}
	if _, err := f.WriteAt(p, 4000); err != nil {
		t.Fatal(err)
	}
	if s := d.Stats(); s.Pages != 2 || s.Bytes != 5000 {
		t.Fatalf("%d pages over %d bytes after the window filled, want 2 over 5000", s.Pages, s.Bytes)
	}
	if _, err := f.WriteAt(p, 5000); err != nil {
		t.Fatal(err)
	}
	if s := d.Stats(); s.Pages != 2 {
		t.Fatalf("%d pages written for a write after the window closed, want 2: it opens a new one", s.Pages)
	}
}

// TestScrapConcurrent writers share one file at disjoint offsets, which is what
// the journal's writer does to a segment. Run with -race.
func TestScrapConcurrent(t *testing.T) {
	d, dir := open(t, Options{Run: 64 << 10})
	f, err := d.Create("000000.seg", 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	const workers, each = 8, 64
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := make([]byte, 512)
			for i := range each {
				off := int64(w*each+i) * 512
				for j := range p {
					p[j] = byte(w)
				}
				if _, err := f.WriteAt(p, off); err != nil {
					t.Errorf("write at %d: %v", off, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	for w := range workers {
		buf := make([]byte, 512)
		for i := range each {
			off := int64(w*each+i) * 512
			if _, err := f.ReadAt(buf, off); err != nil {
				t.Fatalf("read at %d: %v", off, err)
			}
			for j := range buf {
				if buf[j] != byte(w) {
					t.Fatalf("byte %d at %d is %d, want %d", j, off, buf[j], w)
				}
			}
		}
	}
	f.Close()
	d.Close()
	_ = dir
}

func TestPages(t *testing.T) {
	for _, tc := range []struct{ off, n, want int64 }{
		{0, 4096, 1},
		{0, 4097, 2},
		{1, 4096, 2},
		{4096, 4096, 1},
		{4124, 4876, 2},
		{0, 0, 0},
	} {
		if got := pages(tc.off, tc.n, PageSize); got != uint64(tc.want) {
			t.Errorf("pages(%d, %d) = %d, want %d", tc.off, tc.n, got, tc.want)
		}
	}
}

// media is a journal.Disk that does nothing but record what the layer sends it: what it does not
// see is what the layer is holding. The files it hands out swallow every write, so a test can
// count the layer's memory from the outside, without reading a field.
type media struct {
	ev   []emit // one per write that reached the media, in order
	seen int64  // bytes the media has been given
}

// emit is one write that reached the media. before is how many bytes it had been given already,
// so accepted-before is everything the journal had handed the layer at that instant.
type emit struct {
	before int64
	n      int64
}

func (m *media) List() ([]string, error)                    { return nil, nil }
func (m *media) Remove(string) error                        { return nil }
func (m *media) Close() error                               { return nil }
func (m *media) Create(string, int64) (journal.File, error) { return &mediaFile{m: m}, nil }
func (m *media) Open(string) (journal.File, error)          { return &mediaFile{m: m}, nil }

type mediaFile struct{ m *media }

func (f *mediaFile) ReadAt(p []byte, off int64) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func (f *mediaFile) WriteAt(p []byte, off int64) (int, error) {
	f.m.ev = append(f.m.ev, emit{before: f.m.seen, n: int64(len(p))})
	f.m.seen += int64(len(p))
	return len(p), nil
}

func (f *mediaFile) Writeback(int64, int64) error { return nil }
func (f *mediaFile) Sync() error                  { return nil }
func (f *mediaFile) Size() int64                  { return 0 }
func (f *mediaFile) Close() error                 { return nil }

// TestScrapHoldsAtMostTwoWindows measures, from outside the layer, how many bytes it can be
// holding at one instant. Every byte the journal hands over either reached the media or is still
// in the layer, so accepted minus the media's own count is what the layer is holding; at the
// instant a write reaches the media, everything accepted so far is still on the layer's side.
//
// The bound is two windows, not one: a window may fill to exactly Run without flushing (a write
// of exactly Run bytes is captured, not sent through), and the write that continues it is added
// before the flush is decided, so the layer can hold Run in the window and Run in flight. The
// witness sequence attains 2*Run, which is what makes the bound the measured value rather than a
// statement looser than the code.
func TestScrapHoldsAtMostTwoWindows(t *testing.T) {
	type step struct {
		n   int
		off int64
	}
	for _, tc := range []struct {
		name     string
		run      int
		seq      []step
		wantPeak int64
	}{
		{
			// One byte short of a window, then a full window continuing it: the flush carries
			// 2*run-1. Then a window of exactly run, continued: the flush carries 2*run.
			name:     "a window and the write that ends it",
			run:      64,
			seq:      []step{{63, 0}, {64, 63}, {64, 127}, {64, 191}},
			wantPeak: 128,
		},
		{
			// The baseline half of the measurement: no window, so nothing accumulates and the
			// only bytes the media has not seen are the ones in the call that is emitting.
			name:     "no window",
			run:      -1,
			seq:      []step{{63, 0}, {64, 63}, {64, 127}, {64, 191}},
			wantPeak: 64,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &media{}
			d := New(m, Options{Run: tc.run})
			f, err := d.Create("000000.seg", 1<<20)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			var accepted, peak int64
			seen := 0
			for _, s := range tc.seq {
				if _, err := f.WriteAt(make([]byte, s.n), s.off); err != nil {
					t.Fatalf("write %d at %d: %v", s.n, s.off, err)
				}
				accepted += int64(s.n)
				for _, e := range m.ev[seen:] {
					if held := accepted - e.before; held > peak {
						peak = held
					}
				}
				seen = len(m.ev)
				held := accepted - m.seen
				if tc.run > 0 && held > int64(tc.run) {
					t.Errorf("after writing %d at %d the layer holds %d bytes, over the %d-byte window",
						s.n, s.off, held, tc.run)
				}
				if tc.run <= 0 && held != 0 {
					t.Errorf("with no window the layer holds %d bytes after a write of %d", held, s.n)
				}
			}
			if peak != tc.wantPeak {
				t.Errorf("the layer held at most %d bytes, want %d", peak, tc.wantPeak)
			}
			if tc.run > 0 && peak > int64(2*tc.run) {
				t.Errorf("the layer held %d bytes, over the two windows (%d) that bound it", peak, 2*tc.run)
			}
			if d.Stats().Bytes != uint64(m.seen) {
				t.Errorf("the layer says it wrote %d bytes, the media saw %d", d.Stats().Bytes, m.seen)
			}
			t.Logf("peak held %d bytes over a %d-byte window; %d bytes written, %d on the media, %d still resident",
				peak, tc.run, accepted, m.seen, accepted-int64(m.seen))
		})
	}
}
