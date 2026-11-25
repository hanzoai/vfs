package vfs

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"sort"
	"time"
)

// small is the largest file stored whole in a metadata record. Larger files are
// chunked. A file's form is a function of its size alone, so a given content has
// exactly one representation.
const small = 16 << 10

// MaxSize is the largest a file can be: 4 EiB, far past anything a store holds,
// chosen so offsets and lengths never overflow a signed 64-bit integer. A file
// may be declared much larger than the bytes written to it. What it costs follows
// the runs of bytes written, not its size: a hole has no chunks, no records and no
// memory.
const MaxSize = 1 << 62

// spanCost is what each separate unsettled write is charged beyond its bytes: the
// slice header, the list slot and the allocation it occupies. Without it a million
// one-byte writes would be accounted as a megabyte and cost a hundred.
const spanCost = 64

// A file's content is read through up to three levels. The settled content is what the
// tree holds: inline bytes, or chunks. Over it lies the frozen layer, the writes that the
// generation in flight is turning into chunks, which nothing changes any more; and over
// that the live layer, the writes since. A layer holds the writes that came after what is
// below it (spans, sorted, disjoint, never touching), a limit (truncating below the limit
// voids what the levels below hold past it, so a later extension reads zeros there, not
// the old bytes), and punched voids (ranges punched out of the levels below it). Bytes
// that no level holds are a hole and read as zero.
//
// When the generation that froze a layer is published, the settled content becomes what
// the layer and the settled content below it made together, the frozen layer goes, and
// the live layer, whose limit and voids were always about that combined content, stays as
// it is.

// span is a run of bytes written at an offset.
type span struct {
	off  uint64
	data []byte
}

func (s span) end() uint64 { return s.off + uint64(len(s.data)) }

type layer struct {
	dirty  []span // sorted, disjoint, never touching
	dirtyN int64  // bytes plus spanCost per span
	limit  uint64 // how much of the content below still counts
	void   []run  // ranges punched out of the content below; sorted, disjoint, never touching
}

// fileState is one file's content as the Volume holds it. All handles to a file share
// one fileState, so every handle sees every write at once.
type fileState struct {
	ino  InodeID
	refs int  // open handles
	gone bool // removed with no open handles

	// orphan is a file that was unlinked while a handle held it: no name reaches it, the
	// tree holds nothing of it, and it lives in memory (attr is its inode) until the last
	// handle closes.
	orphan bool
	attr   Inode

	// dep is the journal sequence of the last change the file depends on: its fsync waits
	// for it, and for nothing after it.
	dep uint64

	// The settled content: inline bytes, or chunks sorted by offset. Chunks need not
	// touch: where none covers a byte, the byte is a hole and reads as zero. A published
	// generation replaces these and never changes them.
	inline []byte
	ext    []extent

	size    uint64 // the file's length now
	mtime   time.Time
	ctime   time.Time
	fz      *layer // the writes of the generation in flight, if the file has any in it
	lv      layer  // the writes since
	changed bool   // the live layer holds a change, of the bytes or of the length
}

// unsettled reports whether the file holds anything the tree does not: its length and
// time are the file's own, not the inode record's.
func (f *fileState) unsettled() bool { return f.changed || f.fz != nil }

// File is an open file. Its methods are safe for concurrent use.
type File struct {
	v *Volume
	f *fileState
}

// Open opens a regular file for reading and writing. Writes are visible to every
// handle at once; they are durable in this node's journal after Sync, and in the store
// when the Volume's generation that holds them is published (Flush).
func (v *Volume) Open(ctx context.Context, ino InodeID) (*File, error) {
	var f *fileState
	err := v.edit(ctx, func(ctx context.Context) error {
		var err error
		if f, err = v.state(ctx, ino); err != nil {
			return err
		}
		if f.orphan {
			return fmt.Errorf("%w: inode %d was unlinked", fs.ErrNotExist, ino)
		}
		f.refs++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &File{v: v, f: f}, nil
}

// state returns the file's state, loading it from the tree on first use. mu is held.
func (v *Volume) state(ctx context.Context, ino InodeID) (*fileState, error) {
	if f := v.files[ino]; f != nil {
		if f.orphan && f.refs == 0 {
			return nil, fmt.Errorf("%w: inode %d", fs.ErrNotExist, ino)
		}
		return f, nil
	}
	in, err := v.stat(ctx, ino)
	if err != nil {
		return nil, err
	}
	switch {
	case in.IsDir():
		return nil, fmt.Errorf("%w: inode %d", ErrIsDir, ino)
	case in.Mode&fs.ModeType != 0:
		return nil, fmt.Errorf("%w: inode %d is not a regular file", fs.ErrInvalid, ino)
	}
	recs, err := v.scan(ctx, key(tData, uint64(ino)))
	if err != nil {
		return nil, err
	}
	f := &fileState{ino: ino, size: in.Size, mtime: in.Mtime, ctime: in.Ctime, lv: layer{limit: in.Size}, dep: v.applied}
	for _, r := range recs {
		if len(r.key) == prefixLen {
			f.inline = r.val
			continue
		}
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			return nil, err
		}
		f.ext = append(f.ext, e)
	}
	v.files[ino] = f
	return f, nil
}

// Stat returns the file's attributes, including writes not yet flushed.
func (h *File) Stat(ctx context.Context) (Inode, error) {
	var in Inode
	err := h.v.view(ctx, func(ctx context.Context) error {
		if h.f.gone {
			return fs.ErrNotExist
		}
		var err error
		in, err = h.v.stat(ctx, h.f.ino)
		return err
	})
	return in, err
}

// ReadAt reads up to len(p) bytes at off. Like io.ReaderAt it returns io.EOF
// with the bytes it has when the file ends inside the range.
func (h *File) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%w: negative offset %d", fs.ErrInvalid, off)
	}
	v := h.v
	n, root, pieces, err := h.planRead(p, uint64(off))
	if err != nil {
		return 0, err
	}
	if err := v.fetch(ctx, root, pieces, p); err != nil {
		return 0, err
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (h *File) planRead(p []byte, off uint64) (int, ref, []piece, error) {
	v := h.v
	v.mu.RLock()
	defer v.mu.RUnlock()
	if h.f.gone {
		return 0, none, nil, fs.ErrNotExist
	}
	if off >= h.f.size {
		return 0, none, nil, io.EOF
	}
	b := h.f.body()
	root := v.root
	n, pieces := b.plan(p, off)
	return n, root, pieces, nil
}

// body is a file's content as of one moment: the settled content and the layers over it,
// oldest first. It holds the layers, not copies of them: it is read under the Volume's
// lock, or built from layers that are frozen.
type body struct {
	inline []byte
	ext    []extent
	size   uint64
	layers []*layer
}

func (f *fileState) body() body {
	b := body{inline: f.inline, ext: f.ext, size: f.size, layers: []*layer{&f.lv}}
	if f.fz != nil {
		b.layers = []*layer{f.fz, &f.lv}
	}
	return b
}

// piece is a part of a read that the settled chunks answer: n bytes of chunk id from at,
// for p[to:].
type piece struct {
	id chunkID
	at uint64
	to int
	n  int
}

// plan fills p with what the layers and the inline bytes say of [off, off+len(p)) and
// returns how many bytes the file has there, and the pieces that chunks must fill in. It
// makes no request and reads nothing it does not hold, so it can run under the Volume's
// lock; what it leaves to the chunks is fetched after the lock is let go, into the bytes
// that no write or punched hole covers.
func (b *body) plan(p []byte, off uint64) (int, []piece) {
	if off >= b.size {
		return 0, nil
	}
	n := int(min(uint64(len(p)), b.size-off))
	end := off + uint64(n)
	clear(p[:n])

	// The settled content counts below every limit.
	lim := end
	for _, l := range b.layers {
		lim = min(lim, l.limit)
	}
	if b.ext == nil && off < lim && off < uint64(len(b.inline)) {
		copy(p[:lim-off], b.inline[off:min(uint64(len(b.inline)), lim)])
	}

	// Each layer's voids zero what lies below it, and its writes show where the layers
	// above it do not void them.
	var covered []run
	for i, l := range b.layers {
		cut := end
		for _, above := range b.layers[i+1:] {
			cut = min(cut, above.limit)
		}
		if cut <= off {
			continue
		}
		for j := sort.Search(len(l.void), func(j int) bool { return l.void[j].b > off }); j < len(l.void) && l.void[j].a < cut; j++ {
			r := l.void[j]
			lo, hi := max(r.a, off), min(r.b, cut)
			if lo < hi {
				clear(p[lo-off : hi-off])
				covered = append(covered, run{lo, hi})
			}
		}
		for j := sort.Search(len(l.dirty), func(j int) bool { return l.dirty[j].end() > off }); j < len(l.dirty) && l.dirty[j].off < cut; j++ {
			s := l.dirty[j]
			lo, hi := max(s.off, off), min(s.end(), cut)
			if lo < hi {
				copy(p[lo-off:hi-off], s.data[lo-s.off:hi-s.off])
				covered = append(covered, run{lo, hi})
			}
		}
	}
	if b.ext == nil || off >= lim {
		return n, nil
	}
	covered = join(covered)
	var pieces []piece
	for i := sort.Search(len(b.ext), func(i int) bool { return b.ext[i].end() > off }); i < len(b.ext) && b.ext[i].off < lim; i++ {
		e := b.ext[i]
		lo, hi := max(e.off, off), min(e.end(), lim)
		for lo < hi {
			// the first covered run that reaches lo
			c := sort.Search(len(covered), func(c int) bool { return covered[c].b > lo })
			if c < len(covered) && covered[c].a <= lo {
				lo = covered[c].b
				continue
			}
			stop := hi
			if c < len(covered) {
				stop = min(hi, covered[c].a)
			}
			pieces = append(pieces, piece{id: e.id, at: lo - e.off, to: int(lo - off), n: int(stop - lo)})
			lo = stop
		}
	}
	return n, pieces
}

// join sorts runs and merges the ones that overlap or touch.
func join(rs []run) []run {
	slices.SortFunc(rs, func(x, y run) int { return cmp.Compare(x.a, y.a) })
	merged := rs[:0]
	for _, r := range rs {
		if n := len(merged); n > 0 && r.a <= merged[n-1].b {
			merged[n-1].b = max(merged[n-1].b, r.b)
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

// fetch fills p with the bytes of pieces, reading the chunks (from the cache, or the
// store) at the tree whose root is given. No lock is held.
func (v *Volume) fetch(ctx context.Context, root ref, pieces []piece, p []byte) error {
	for _, pc := range pieces {
		plain, err := v.getChunk(ctx, root, pc.id)
		if err != nil {
			return err
		}
		copy(p[pc.to:pc.to+pc.n], plain[pc.at:])
	}
	return nil
}

// read fills p from the body as ReadAt does, for a body that no lock guards.
func (b *body) read(ctx context.Context, v *Volume, root ref, p []byte, off uint64) (int, error) {
	n, pieces := b.plan(p, off)
	if err := v.fetch(ctx, root, pieces, p); err != nil {
		return 0, err
	}
	return n, nil
}

// WriteAt writes p at off, extending the file if needed. The change is appended to the
// journal, and applied: it is visible at once, durable on this node after Sync, and in the
// store when the generation that holds it is published.
func (h *File) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%w: negative offset %d", fs.ErrInvalid, off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(off)+uint64(len(p)) > MaxSize {
		return 0, fmt.Errorf("%w: a write to %d", ErrTooBig, uint64(off)+uint64(len(p)))
	}
	return h.v.mutateWrite(ctx, h.f, uint64(off), p)
}

// write adds p at off to the live layer's writes, merging with the writes it touches so
// the list stays sorted, disjoint and gap-separated. mu is held.
func (v *Volume) write(f *fileState, p []byte, off uint64) {
	end := off + uint64(len(p))
	d := f.lv.dirty
	i := sort.Search(len(d), func(i int) bool { return d[i].end() >= off })
	j := i
	for j < len(d) && d[j].off <= end {
		j++
	}
	var merged span
	switch {
	case i == j:
		merged = span{off, append([]byte(nil), p...)}
	case j == i+1 && off >= d[i].off && end <= d[i].end():
		copy(d[i].data[off-d[i].off:], p) // inside one write: overwrite in place
		return
	case j == i+1 && off == d[i].end():
		merged = span{d[i].off, append(d[i].data, p...)} // append, amortized
	default:
		lo, hi := min(off, d[i].off), max(end, d[j-1].end())
		buf := make([]byte, hi-lo)
		for _, s := range d[i:j] {
			copy(buf[s.off-lo:], s.data)
		}
		copy(buf[off-lo:], p)
		merged = span{lo, buf}
	}
	grown := int64(len(merged.data)) + spanCost
	for _, s := range d[i:j] {
		grown -= int64(len(s.data)) + spanCost
	}
	f.lv.dirty = slices.Replace(d, i, j, merged)
	f.lv.dirtyN += grown
	v.account(grown)
}

// Truncate sets the file's length. Shrinking discards the bytes past it for
// good; growing adds zeros.
func (h *File) Truncate(ctx context.Context, size uint64) error {
	if size > MaxSize {
		return fmt.Errorf("%w: %d bytes", ErrTooBig, size)
	}
	_, err := h.v.mutate(ctx, func(context.Context) (*change, error) {
		if h.f.gone {
			return nil, fs.ErrNotExist
		}
		now := h.v.Now()
		if h.f.orphan {
			h.v.truncate(h.f, size, now, now)
			return nil, nil
		}
		return &change{kind: changeSize, ino: h.f.ino, file: h.f, size: size, when: now, ctime: now}, nil
	})
	return err
}

// Truncate sets the length of the file with inode ino, as File.Truncate does.
func (v *Volume) Truncate(ctx context.Context, ino InodeID, size uint64) error {
	if size > MaxSize {
		return fmt.Errorf("%w: %d bytes", ErrTooBig, size)
	}
	_, err := v.mutate(ctx, func(ctx context.Context) (*change, error) {
		f, err := v.state(ctx, ino)
		if err != nil {
			return nil, err
		}
		now := v.Now()
		if f.orphan {
			v.truncate(f, size, now, now)
			return nil, nil
		}
		return &change{kind: changeSize, ino: ino, file: f, size: size, when: now, ctime: now}, nil
	})
	return err
}

// truncate sets the file's length in the live layer. mu is held.
func (v *Volume) truncate(f *fileState, size uint64, when, ctime time.Time) {
	if size < f.size {
		f.lv.limit = min(f.lv.limit, size)
		var kept []span
		var n int64
		for _, s := range f.lv.dirty {
			if s.off >= size {
				break
			}
			if s.end() > size {
				s.data = s.data[:size-s.off]
			}
			kept = append(kept, s)
			n += int64(len(s.data)) + spanCost
		}
		v.account(n - f.lv.dirtyN)
		f.lv.dirty, f.lv.dirtyN = kept, n
		for len(f.lv.void) > 0 && f.lv.void[len(f.lv.void)-1].b > size {
			last := &f.lv.void[len(f.lv.void)-1]
			if last.a >= size {
				f.lv.void = f.lv.void[:len(f.lv.void)-1]
			} else {
				last.b = size
			}
		}
	}
	f.size, f.mtime, f.ctime = size, when, ctime
}

// addVoid punches [a, b) out of what lies beneath l.
func (l *layer) addVoid(a, b uint64) {
	out := make([]run, 0, len(l.void)+1)
	placed := false
	for _, r := range l.void {
		switch {
		case r.b < a:
			out = append(out, r)
		case r.a > b:
			if !placed {
				out, placed = append(out, run{a, b}), true
			}
			out = append(out, r)
		default:
			a, b = min(a, r.a), max(b, r.b)
		}
	}
	if !placed {
		out = append(out, run{a, b})
	}
	l.void = out
}

// Punch deallocates [off, off+n) of the file: those bytes read as zero from now on, the
// size is unchanged, and once settled the range costs no chunk, record or object. A range
// past the end of the file is clipped to it.
func (h *File) Punch(ctx context.Context, off, n uint64) error {
	_, err := h.v.mutate(ctx, func(context.Context) (*change, error) {
		if h.f.gone {
			return nil, fs.ErrNotExist
		}
		a, b := min(off, h.f.size), min(off+n, h.f.size)
		if off+n < off { // the range wraps: it reaches the end of the file
			b = h.f.size
		}
		if a >= b {
			return nil, nil
		}
		now := h.v.Now()
		if h.f.orphan {
			h.v.punch(h.f, a, b-a, now)
			return nil, nil
		}
		return &change{kind: changePunch, ino: h.f.ino, file: h.f, off: a, size: b - a, when: now, ctime: now}, nil
	})
	return err
}

// punch removes [off, off+n) from the live layer and records it as voided. mu is held.
func (v *Volume) punch(f *fileState, off, n uint64, when time.Time) {
	a, b := min(off, f.size), min(off+n, f.size)
	if off+n < off {
		b = f.size
	}
	if a >= b {
		return
	}
	var kept []span
	var bytes int64
	for _, s := range f.lv.dirty {
		switch {
		case s.end() <= a || s.off >= b:
			kept = append(kept, s)
		default:
			if s.off < a {
				kept = append(kept, span{s.off, s.data[: a-s.off : a-s.off]})
			}
			if s.end() > b {
				kept = append(kept, span{b, s.data[b-s.off:]})
			}
		}
	}
	for _, s := range kept {
		bytes += int64(len(s.data)) + spanCost
	}
	v.account(bytes - f.lv.dirtyN)
	f.lv.dirty, f.lv.dirtyN = kept, bytes
	f.lv.addVoid(a, b)
	f.mtime, f.ctime = when, when
}

// Sync makes every change the file depends on durable in this node's journal. It does not
// ask the store for anything: the store has the change within the Volume's bound, or when
// Flush returns.
func (h *File) Sync(ctx context.Context) error {
	h.v.mu.RLock()
	orphan, dep := h.f.orphan, h.f.dep
	h.v.mu.RUnlock()
	if orphan {
		return nil
	}
	return h.v.syncTo(ctx, dep)
}

// Close releases the handle. It makes no request and changes nothing but the count of
// holders: what the handle wrote is in the Volume's memory and journal whether it is
// closed or not. The last handle of a file that was unlinked takes the file with it.
func (h *File) Close() error {
	h.v.mu.Lock()
	defer h.v.mu.Unlock()
	h.f.refs--
	if h.f.refs == 0 {
		if h.f.orphan {
			h.v.account(-h.f.lv.dirtyN)
			h.f.lv = layer{}
			h.f.changed = false
			if h.f.fz == nil {
				delete(h.v.files, h.f.ino)
			}
		} else if !h.f.unsettled() {
			delete(h.v.files, h.f.ino)
		}
	}
	return nil
}

// run is a stretch of a file that holds written bytes: [a, b).
type run struct{ a, b uint64 }
