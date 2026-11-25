package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// A generation is what the Volume held, up to one journal sequence, cut out of it at an
// instant and made immutable: the metadata changes (an overlay, no longer written to) and,
// for each file that was written, its writes (a layer, no longer written to) and the
// content they were made on. Changes made after the cut belong to the next generation:
// they are applied to a new overlay and new layers, and a read sees the generation's
// beneath them. The cut is the only thing that needs the Volume's lock; what follows runs
// without it, and what it makes (chunks, packs, shards) is written to the store in
// parallel and named by its content or by a random id, so nothing it does is seen by
// anyone until the Head names it, and a generation that fails is made again whole.
//
// One generation is in flight at a time. Its Head carries the journal's sequence at the
// cut, and the order is fixed: the Head commits, then the journal is told the store has
// the sequence (Publish), then its space is reclaimed. A Head never carries a sequence
// that is not durable on this node, and the journal never says published what the store
// does not have.
type generation struct {
	v     *Volume
	seq   uint64   // the journal sequence the cut included
	over  *overlay // the metadata changes of the cut
	snaps []*snap
	gone  []InodeID // files removed before the cut: their data records go
	next  InodeID   // inode numbers handed out at the cut
	root  ref       // the tree the Head named at the cut
	bytes int64     // memory of the writes the generation holds
	ns    counters  // namespace counters at the cut

	mu    sync.Mutex // guards what the workers add
	known map[chunkID]struct{}
	recs  []mut
	packs []*packed

	settled bool
	muts    []mut
	outNS   counters // counters after settling this generation's files
	built   bool
	newRoot ref
	head    Head
}

// snap is one file's share of a generation: the content it had at the cut, and the writes
// made on it since the generation before.
type snap struct {
	f      *fileState
	ino    InodeID
	inline []byte
	ext    []extent
	size   uint64
	mtime  time.Time
	ctime  time.Time
	fz     *layer

	muts      []mut    // the records that make the file what it is now
	outInline []byte   // the settled content once the generation is published
	outExt    []extent //
	oldBytes  uint64   // settled bytes held before this generation
	newBytes  uint64   // settled bytes held after this generation
}

// cut makes the generation: everything applied so far, or nil when a generation is already
// in flight or when the Head already has everything. mu is held for writing.
func (v *Volume) cut() *generation {
	if v.gen != nil || v.applied == v.head.Journal {
		return nil
	}
	g := &generation{v: v, seq: v.applied, over: v.over, gone: v.gone, next: v.next, root: v.root, ns: v.ns}
	v.over, v.gone, v.goneSet, v.logged = newOverlay(), nil, map[InodeID]struct{}{}, 0
	for _, f := range v.dirtyFiles {
		fz := &layer{dirty: f.lv.dirty, dirtyN: f.lv.dirtyN, limit: f.lv.limit, void: f.lv.void}
		g.snaps = append(g.snaps, &snap{f: f, ino: f.ino, inline: f.inline, ext: f.ext, size: f.size, mtime: f.mtime, ctime: f.ctime, fz: fz})
		g.bytes += fz.dirtyN
		f.fz = fz
		f.lv = layer{limit: f.size}
		f.changed = false
	}
	v.dirtyFiles = map[InodeID]*fileState{}
	v.gen = g
	return g
}

// get reads a record as the cut had it: the generation's changes, then the tree.
func (g *generation) get(ctx context.Context, k []byte) ([]byte, bool, error) {
	if n, ok := g.over.get(k); ok {
		return n.val, !n.del, nil
	}
	return g.v.t.get(ctx, g.root, k)
}

// run takes the generation as far as it goes and returns when it is published or when
// something failed. A generation that failed is run again: what an attempt completed is
// kept, and what it did not is done over.
func (g *generation) run(ctx context.Context) error {
	v := g.v
	up := newUploader(ctx, v.be, v.cfg.Streams)
	if !g.settled {
		sort.Slice(g.snaps, func(i, j int) bool { return g.snaps[i].ino < g.snaps[j].ino })
		g.reset()
		if err := g.settle(ctx, up); err != nil {
			up.wait()
			return err
		}
		g.settled = true
	} else {
		for _, p := range g.packs {
			if !p.done.Load() {
				up.put(packKey(p.id), p.buf, p.finish)
			}
		}
	}
	if err := up.wait(); err != nil {
		return fmt.Errorf("vfs: write packs: %w", err)
	}
	if !g.built {
		if err := g.collect(ctx); err != nil {
			return err
		}
		g.built = true
	}
	shards := newUploader(ctx, v.be, v.cfg.Streams)
	root, err := v.t.using(shards).apply(ctx, g.root, g.muts)
	if werr := shards.wait(); err == nil && werr != nil {
		err = werr
	}
	if err != nil {
		return err
	}
	g.newRoot = root
	if err := v.syncTo(ctx, g.seq); err != nil {
		return err
	}
	return g.commit(ctx)
}

func (g *generation) reset() {
	g.known = map[chunkID]struct{}{}
	g.recs, g.packs = nil, nil
}

// settle turns every file's writes into chunks and records. Files are settled in
// parallel, and the chunks of each across the Volume's workers; the sealed chunks are laid
// into packs and the packs go to the store as they fill.
func (g *generation) settle(ctx context.Context, up *uploader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pk := &packer{g: g, up: up}
	files := make(chan struct{}, 2*g.v.cfg.Workers)
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if first == nil {
			first = err
		}
	}
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return first != nil
	}
	for _, s := range g.snaps {
		if failed() {
			break
		}
		files <- struct{}{}
		wg.Add(1)
		go func(s *snap) {
			defer wg.Done()
			defer func() { <-files }()
			if err := g.settleFile(ctx, s, pk); err != nil {
				fail(err)
			}
		}(s)
	}
	wg.Wait()
	if first != nil {
		return first
	}
	pk.flush()
	return nil
}

// settleFile settles one file: its content as the cut had it becomes inline bytes or a
// list of chunks, and the records that differ from the tree's are staged.
func (g *generation) settleFile(ctx context.Context, s *snap, pk *packer) error {
	v := g.v
	raw, ok, err := g.get(ctx, keyInode(s.ino))
	if err != nil {
		return err
	}
	if !ok {
		return nil // removed: the change that removed it deletes what there is
	}
	in, err := decodeInode(s.ino, raw)
	if err != nil {
		return err
	}
	b := body{inline: s.inline, ext: s.ext, size: s.size, layers: []*layer{s.fz}}
	var inline []byte
	var out []extent
	if s.size <= small {
		inline = make([]byte, s.size)
		if s.size > 0 {
			if _, err := b.read(ctx, v, g.root, inline, 0); err != nil {
				return err
			}
		}
	} else if out, err = g.recut(ctx, s, &b, pk); err != nil {
		return err
	}

	oldBytes := uint64(len(s.inline))
	for _, e := range s.ext {
		oldBytes += uint64(e.n)
	}
	newBytes := uint64(len(inline))
	for _, e := range out {
		newBytes += uint64(e.n)
	}

	// Stage only what differs from the settled records.
	var ms []mut
	if s.size <= small {
		if s.size > 0 {
			ms = append(ms, mut{key: keyInline(s.ino), val: inline})
		} else if len(s.inline) > 0 {
			ms = append(ms, mut{key: keyInline(s.ino), del: true})
		}
		for _, e := range s.ext {
			ms = append(ms, mut{key: keyExtent(s.ino, e.off), del: true})
		}
	} else {
		if len(s.inline) > 0 {
			ms = append(ms, mut{key: keyInline(s.ino), del: true})
		}
		old := make(map[uint64]extent, len(s.ext))
		for _, e := range s.ext {
			old[e.off] = e
		}
		for _, e := range out {
			if o, ok := old[e.off]; !ok || o != e {
				ms = append(ms, mut{key: keyExtent(s.ino, e.off), val: encodeExtent(e)})
			}
			delete(old, e.off)
		}
		for off := range old {
			ms = append(ms, mut{key: keyExtent(s.ino, off), del: true})
		}
	}
	ct := s.ctime
	if ct.IsZero() {
		ct = s.mtime
	}
	in.Size, in.Mtime, in.Ctime = s.size, s.mtime, ct
	ms = append(ms, mut{key: keyInode(s.ino), val: encodeInode(in)})
	s.muts, s.outInline, s.outExt = ms, inline, out
	s.oldBytes, s.newBytes = oldBytes, newBytes
	return nil
}

// runs returns, in order, the stretches of the file that hold written bytes at the cut:
// the settled chunks and inline bytes that still count (less punched voids), and the writes.
// Everything between them is a hole.
func (s *snap) runs() []run {
	var rs []run
	lim := min(s.size, s.fz.limit)
	add := func(a, b uint64) {
		if b = min(b, s.size); a < b {
			rs = append(rs, run{a, b})
		}
	}
	// settled adds a stretch of the settled content: what was punched out is not in it.
	settled := func(a, b uint64) {
		for _, r := range s.fz.void {
			if r.b <= a {
				continue
			}
			if r.a >= b {
				break
			}
			if r.a > a {
				add(a, r.a)
			}
			if a = max(a, r.b); a >= b {
				return
			}
		}
		add(a, b)
	}
	for _, e := range s.ext {
		if e.off >= lim {
			break
		}
		settled(e.off, min(e.end(), lim))
	}
	if s.ext == nil {
		settled(0, min(uint64(len(s.inline)), lim))
	}
	for _, d := range s.fz.dirty {
		add(d.off, d.end())
	}
	return join(rs)
}

// touched reports whether a write overlaps [a, b).
func (s *snap) touched(a, b uint64) bool {
	d := s.fz.dirty
	i := sort.Search(len(d), func(i int) bool { return d[i].end() > a })
	return i < len(d) && d[i].off < b
}

// voided reports whether a punched range overlaps [a, b).
func (s *snap) voided(a, b uint64) bool {
	v := s.fz.void
	i := sort.Search(len(v), func(i int) bool { return v[i].b > a })
	return i < len(v) && v[i].a < b
}

// direct returns the n bytes at pos if one write holds them all: a window of the file
// that needs no copy, since a frozen write never changes.
func (s *snap) direct(pos, n uint64) ([]byte, bool) {
	d := s.fz.dirty
	i := sort.Search(len(d), func(i int) bool { return d[i].end() > pos })
	if i < len(d) && d[i].off <= pos && pos+n <= d[i].end() {
		return d[i].data[pos-d[i].off : pos-d[i].off+n], true
	}
	return nil, false
}

// recut returns the chunk list of the file's content at the cut, sealing and packing the
// new chunks. It cuts each run of written bytes on its own, from the run's first byte, so
// chunks never span a hole and a hole costs nothing however long it is. Only the part of
// the file that the writes touch is cut again: cutting is local (a cut depends on the
// bytes since the previous cut), so a settled chunk that is unchanged and begins where
// the new cutting stands is exactly a chunk the new cutting would produce again, and it
// is kept. The cutting is one pass over the bytes; the hashing and sealing of what it cuts
// run on the Volume's workers.
func (g *generation) recut(ctx context.Context, s *snap, b *body, pk *packer) ([]extent, error) {
	v := g.v
	runs := s.runs()
	var widest uint64
	for _, r := range runs {
		widest = max(widest, r.b-r.a)
	}
	window := make([]byte, min(widest, maxChunk))
	old := s.ext
	lim := min(s.size, s.fz.limit)
	jobs := v.work.group(ctx)
	var out []*extent
	for _, r := range runs {
		// A settled chunk is kept if its bytes are unchanged and the cut that ended
		// it would still end it. A content cut does not depend on what follows. A
		// chunk that ended a run was cut by the run's end, so it is kept only if the
		// run still ends there.
		clean := func() bool {
			e := old[0]
			if e.end() > lim || s.touched(e.off, e.end()) || s.voided(e.off, e.end()) {
				return false
			}
			endsRun := len(old) == 1 || old[1].off != e.end()
			return !endsRun || e.end() == r.b
		}
		for pos := r.a; pos < r.b; {
			if err := jobs.failed(); err != nil {
				jobs.wait()
				return nil, err
			}
			if err := ctx.Err(); err != nil {
				jobs.wait()
				return nil, err
			}
			for len(old) > 0 && old[0].off < pos {
				old = old[1:]
			}
			if len(old) > 0 && old[0].off == pos && clean() {
				e := old[0]
				out = append(out, &e)
				pos = e.end()
				old = old[1:]
				continue
			}
			want := min(uint64(len(window)), r.b-pos)
			w, direct := s.direct(pos, want)
			if !direct {
				w = window[:want]
				if _, err := b.read(ctx, v, g.root, w, pos); err != nil && err != io.EOF {
					jobs.wait()
					return nil, err
				}
			}
			piece := w[:cut(v.gear, w)]
			if !direct {
				piece = bytes.Clone(piece)
			}
			e := &extent{off: pos, n: uint32(len(piece))}
			out = append(out, e)
			jobs.do(func(ctx context.Context) error { return g.putChunk(ctx, e, piece, pk) })
			pos += uint64(len(piece))
		}
	}
	if err := jobs.wait(); err != nil {
		return nil, err
	}
	list := make([]extent, len(out))
	for i, e := range out {
		list[i] = *e
	}
	return list, nil
}

// putChunk names a chunk, and seals it into a pack unless the Volume has it already: that
// is dedupe. The record of where it lies is staged for the tree.
func (g *generation) putChunk(ctx context.Context, e *extent, piece []byte, pk *packer) error {
	v := g.v
	id := sumChunk(piece)
	e.id = id
	fresh, err := g.reserve(ctx, id)
	if err != nil || !fresh {
		return err
	}
	frame, err := seal(v.c, chunkAAD(v.id, id), piece)
	if err != nil {
		return err
	}
	w, err := pk.add(frame)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.recs = append(g.recs, mut{key: keyChunk(id), val: encodeWhere(w)})
	g.mu.Unlock()
	v.chunks.add(id, bytes.Clone(piece), int64(len(piece)))
	return nil
}

// reserve claims a chunk id for this generation and reports whether it is new to the
// Volume. The chunk table is the tree's at the cut: an earlier generation has published
// whatever it wrote.
func (g *generation) reserve(ctx context.Context, id chunkID) (bool, error) {
	g.mu.Lock()
	if _, ok := g.known[id]; ok {
		g.mu.Unlock()
		return false, nil
	}
	g.known[id] = struct{}{}
	g.mu.Unlock()
	_, ok, err := g.v.t.get(ctx, g.root, keyChunk(id))
	return !ok, err
}

// collect makes the generation's changes to the tree: one list, sorted by key, with the
// last change to a key the one that counts. The metadata changes come first, then the
// deletion of what removed files held, then what settling the files made: a file's inode
// record is rewritten with the size and times its writes gave it.
func (g *generation) collect(ctx context.Context) error {
	ms := g.over.drain()
	for _, id := range g.gone {
		err := g.v.t.scan(ctx, g.root, key(tData, uint64(id)), func(k, _ []byte) bool {
			ms = append(ms, mut{key: bytes.Clone(k), del: true})
			return true
		})
		if err != nil {
			return err
		}
	}
	ns := g.ns
	for _, s := range g.snaps {
		if len(s.muts) > 0 {
			ms = append(ms, s.muts...)
			ns.bytes = ns.bytes - min(ns.bytes, s.oldBytes) + s.newBytes
		}
	}
	ms = append(ms, g.recs...)
	sort.SliceStable(ms, func(i, j int) bool { return bytes.Compare(ms[i].key, ms[j].key) < 0 })
	out := ms[:0]
	for _, m := range ms {
		if n := len(out); n > 0 && bytes.Equal(out[n-1].key, m.key) {
			out[n-1] = m
		} else {
			out = append(out, m)
		}
	}
	g.muts, g.outNS = out, ns
	return nil
}

// commit replaces the Head with one that names the new tree and carries the journal's
// sequence. If the write fails its outcome is not known: the Head is read, and if it is
// this generation's the commit landed and its answer was lost. A Round that no longer
// holds the Volume says so and nothing more can be published.
func (g *generation) commit(ctx context.Context) error {
	v := g.v
	v.seq++
	h := v.head
	h.Seq, h.Root, h.Next, h.Journal = v.seq, g.newRoot, uint64(g.next), g.seq
	g.outNS.stamp(&h)
	err := v.writeHead(ctx, h)
	if err != nil {
		if errors.Is(err, ErrStaleRound) {
			v.mu.Lock()
			v.fenced = true
			v.mu.Unlock()
		}
		cur, ok := v.landed(ctx, h)
		if !ok {
			return fmt.Errorf("vfs: commit: %w", err)
		}
		h = cur
	}
	g.head = h
	return nil
}

// landed reports the Head in the store if it is the one h says: the same Volume, tree and
// journal sequence.
func (v *Volume) landed(ctx context.Context, h Head) (Head, bool) {
	payload, _, err := v.fence.Get(ctx, headKey)
	if err != nil {
		return Head{}, false
	}
	cur, err := v.readHead(payload)
	return cur, err == nil && cur.ID == h.ID && cur.Root == h.Root && cur.Journal == h.Journal
}

// complete makes the published generation the Volume's: the Head is the new one, each
// file's settled content is what its writes made, and the frozen layers go. The journal
// is told the store has the sequence, and its space is reclaimed.
func (g *generation) complete() error {
	v := g.v
	v.mu.Lock()
	v.head, v.root = g.head, g.head.Root
	for _, s := range g.snaps {
		f := s.f
		f.fz = nil
		_, droppedAfterCut := v.goneSet[s.ino]
		if f.gone || droppedAfterCut {
			// The file was dropped after g was cut. Its new tData records are now in v.root;
			// stage deletions in v.over so v.scan(tData) stays consistent with v.ns.bytes.
			for _, m := range s.muts {
				if !m.del && len(m.key) > 0 && m.key[0] == tData {
					v.over.del(m.key)
				}
			}
			if f.orphan {
				f.inline, f.ext = s.outInline, s.outExt
				if f.refs == 0 {
					delete(v.files, f.ino)
				}
			}
			continue
		}
		f.inline, f.ext = s.outInline, s.outExt
		if len(s.muts) > 0 {
			v.ns.bytes = v.ns.bytes - min(v.ns.bytes, s.oldBytes) + s.newBytes
		}
		if f.refs == 0 && !f.changed {
			delete(v.files, f.ino)
		}
	}
	v.account(-g.bytes)
	v.gen, v.reclaiming = nil, true
	v.pubErr = nil
	v.release()
	v.mu.Unlock()
	defer func() {
		v.mu.Lock()
		v.reclaiming = false
		v.release() // a change that waited for the journal's space has it now
		v.mu.Unlock()
	}()
	j := v.led.j
	if err := j.Publish(g.seq); err != nil {
		return err
	}
	// A journal that cannot reclaim its space has failed, and the Volume takes no more
	// changes; what was published is published.
	if err := j.Reclaim(); err != nil {
		if st := j.State(); st.Err != nil {
			v.mu.Lock()
			v.failed = st.Err
			v.mu.Unlock()
		}
	}
	return nil
}

// request is a call of Flush: it wants the Head to carry the journal sequence want.
type request struct {
	want uint64
	done chan error
}

// publishThrough asks the publisher for a Head that carries want, and waits for its answer.
func (v *Volume) publishThrough(ctx context.Context, want uint64) error {
	r := &request{want: want, done: make(chan error, 1)}
	v.reqmu.Lock()
	v.reqs = append(v.reqs, r)
	v.reqmu.Unlock()
	v.wake()
	select {
	case err := <-r.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-v.done:
		v.mu.RLock()
		defer v.mu.RUnlock()
		if err := v.writable(); err != nil {
			return err
		}
		return ErrClosed
	}
}

// release wakes the callers that wait for memory: it has been freed, or will not be. mu is held.
func (v *Volume) release() {
	close(v.freed)
	v.freed = make(chan struct{})
}

// wake tells the publisher there is something to do.
func (v *Volume) wake() {
	select {
	case v.kick <- struct{}{}:
	default:
	}
}

func (v *Volume) takeRequests() []*request {
	v.reqmu.Lock()
	defer v.reqmu.Unlock()
	reqs := v.reqs
	v.reqs = nil
	return reqs
}

// answer tells the callers of Flush how an attempt went. One whose sequence the Head has
// is told so; one that an attempt did not reach is told the attempt failed, or is kept
// for the attempt that follows.
func (v *Volume) answer(reqs []*request, err error) {
	v.mu.RLock()
	published := v.head.Journal
	v.mu.RUnlock()
	var kept []*request
	for _, r := range reqs {
		switch {
		case r.want <= published:
			r.done <- nil
		case err != nil:
			r.done <- err
		default:
			kept = append(kept, r)
		}
	}
	if len(kept) > 0 {
		v.reqmu.Lock()
		v.reqs = append(kept, v.reqs...)
		v.reqmu.Unlock()
	}
}

// publishOnce makes one attempt: the generation in flight, or a new one if there is none.
func (v *Volume) publishOnce(ctx context.Context) error {
	v.mu.Lock()
	if v.fenced {
		v.mu.Unlock()
		return ErrStaleRound
	}
	g := v.gen
	if g == nil {
		g = v.cut()
	}
	v.mu.Unlock()
	if g == nil {
		return nil
	}
	if err := g.run(ctx); err != nil {
		return err
	}
	return g.complete()
}

// publishNow publishes everything applied, in as many generations as it takes, and fails
// at the first that fails.
func (v *Volume) publishNow(ctx context.Context) error {
	for {
		v.mu.RLock()
		done := v.applied == v.head.Journal
		v.mu.RUnlock()
		if done {
			return nil
		}
		if err := v.publishOnce(ctx); err != nil {
			return err
		}
	}
}

// publisher is the goroutine that publishes: when the oldest change the store does not
// have is half the bound old (the other half is for the publication), when memory or the
// overlay holds enough to make it worth doing early, when Flush asks, and again after a
// failure. The deadline is the journal's: one timer, set for the exact moment, and moved
// to the next change when one is published. Nothing polls.
func (v *Volume) publisher() {
	defer close(v.done)
	idle := false
	for {
		v.mu.RLock()
		stopped := v.fenced || v.failed != nil || v.closed
		v.mu.RUnlock()
		if stopped {
			v.mu.RLock()
			err := v.writable()
			v.mu.RUnlock()
			if err == nil {
				err = ErrClosed
			}
			v.answer(v.takeRequests(), err)
			return
		}
		v.mu.RLock()
		// After a failure the journal's deadline spaces the retries; a cycle that had nothing to publish
		// does not run again for pressure alone, so a count that is off cannot make the loop spin.
		pressing := v.pressure() && v.pubErr == nil && !idle
		work := v.applied != v.head.Journal || v.gen != nil
		v.mu.RUnlock()
		if v.reqCount() == 0 && !pressing {
			ctx, cancel := context.WithCancel(v.base)
			select {
			case <-v.led.j.Due(ctx, v.cfg.Bound-v.cfg.Bound/2):
			case <-v.kick:
			case <-v.quit:
				cancel()
				return
			}
			cancel()
		}
		reqs := v.takeRequests()
		idle = !work && len(reqs) == 0
		err := v.publishOnce(v.base)
		v.mu.Lock()
		v.pubErr = err
		if err != nil {
			v.release() // a writer waiting for memory learns that none is coming
		}
		v.mu.Unlock()
		v.answer(reqs, err)
	}
}

func (v *Volume) reqCount() int {
	v.reqmu.Lock()
	defer v.reqmu.Unlock()
	return len(v.reqs)
}
