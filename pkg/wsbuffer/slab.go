package wsbuffer

import (
	"sort"
	"sync"
)

// subRange represents one contiguous dirty byte interval [start, end) inside a
// single logical page. Ranges inside a pageEntry are maintained sorted and
// strictly non-overlapping.
type subRange struct {
	start uint32
	end   uint32
	seq   uint64
	data  []byte // slice into a slab buffer
}

// pageEntry tracks live dirty bytes for one logical page (pageIdx = off / PageSize).
type pageEntry struct {
	full     bool
	fullSeq  uint64
	fullData []byte     // length == pageSize when full == true
	ranges   []subRange // sorted, non-overlapping when full == false
}

// slab is a fixed-size aligned memory chunk backing absorbed scrap writes.
type slab struct {
	buf  []byte
	used int
}

// scrapBuffer absorbs small and unaligned writes into pooled aligned slabs,
// coalescing repeated writes to the same logical pages and sub-page ranges
// before flushing.
type scrapBuffer struct {
	pageSize int
	slabSize int
	pool     *sync.Pool

	slabs      []*slab
	dirtyPages map[int64]*pageEntry
	dirtyBytes int64
	// slabBytes tracks total slab capacity allocated to live dirty data;
	// bounded alongside dirtyBytes so heavy sub-page fragmentation also flushes.
	slabBytes int64

	// Reusable per-flush arenas to avoid heap allocations on every Flush/Sync.
	entryArena []pageEntry
	pagesBuf   []int64
	extentsBuf []Extent
}

func newScrapBuffer(pageSize, slabSize int) *scrapBuffer {
	sz := slabSize
	return &scrapBuffer{
		pageSize:   pageSize,
		slabSize:   slabSize,
		pool:       &sync.Pool{New: func() any { return make([]byte, sz) }},
		dirtyPages: make(map[int64]*pageEntry, 256),
		entryArena: make([]pageEntry, 0, 256),
		pagesBuf:   make([]int64, 0, 256),
		extentsBuf: make([]Extent, 0, 256),
	}
}

func (sb *scrapBuffer) newEntry(e pageEntry) *pageEntry {
	if len(sb.entryArena) == cap(sb.entryArena) {
		// Allocate a fresh entry directly if arena capacity is exceeded;
		// arena grows lazily up to a sensible steady-state watermark on reset.
		pe := e
		return &pe
	}
	idx := len(sb.entryArena)
	sb.entryArena = append(sb.entryArena, e)
	return &sb.entryArena[idx]
}

func (sb *scrapBuffer) alloc(n int) []byte {
	if n > sb.slabSize {
		// Defensive fallback if an unaligned write exceeds slabSize.
		sb.slabBytes += int64(n)
		return make([]byte, n)
	}
	if len(sb.slabs) == 0 || sb.slabSize-sb.slabs[len(sb.slabs)-1].used < n {
		buf := sb.pool.Get().([]byte)
		if cap(buf) < sb.slabSize {
			buf = make([]byte, sb.slabSize)
		} else {
			buf = buf[:sb.slabSize]
		}
		sb.slabs = append(sb.slabs, &slab{buf: buf, used: 0})
		sb.slabBytes += int64(sb.slabSize)
	}
	cur := sb.slabs[len(sb.slabs)-1]
	s := cur.buf[cur.used : cur.used+n]
	cur.used += n
	return s
}

// put absorbs write [off, off+len(p)) at sequence seq.
// It returns coalescedBytes: how many bytes overwrote already-dirty bytes
// in the scrap buffer (avoiding extra physical writes).
func (sb *scrapBuffer) put(off int64, p []byte, seq uint64) (coalescedBytes int64) {
	ps := int64(sb.pageSize)

	// Fast path: single page-aligned full-page write (e.g., 4 KiB random writes).
	if len(p) == sb.pageSize && off%ps == 0 {
		pg := off / ps
		entry, exists := sb.dirtyPages[pg]
		if exists && entry.full {
			copy(entry.fullData, p)
			entry.fullSeq = seq
			return ps
		}
		var prevBytes int64
		if exists {
			for _, r := range entry.ranges {
				prevBytes += int64(r.end - r.start)
			}
		}
		dst := sb.alloc(sb.pageSize)
		copy(dst, p)
		if exists {
			entry.full = true
			entry.fullSeq = seq
			entry.fullData = dst
			entry.ranges = nil
		} else {
			sb.dirtyPages[pg] = sb.newEntry(pageEntry{
				full:     true,
				fullSeq:  seq,
				fullData: dst,
			})
		}
		sb.dirtyBytes += ps - prevBytes
		return prevBytes
	}

	// General path: split across page boundaries and merge per page.
	remaining := p
	curOff := off
	for len(remaining) > 0 {
		pg := curOff / ps
		pStart := uint32(curOff % ps)
		avail := uint32(sb.pageSize) - pStart
		take := uint32(len(remaining))
		if take > avail {
			take = avail
		}
		pEnd := pStart + take
		seg := remaining[:take]

		coalescedBytes += sb.putPageSegment(pg, pStart, pEnd, seg, seq)

		remaining = remaining[take:]
		curOff += int64(take)
	}
	return coalescedBytes
}

func (sb *scrapBuffer) putPageSegment(pg int64, start, end uint32, seg []byte, seq uint64) int64 {
	entry, exists := sb.dirtyPages[pg]

	// Fast path: page is already full in scrap buffer -> overwrite in place!
	if exists && entry.full {
		copy(entry.fullData[start:end], seg)
		entry.fullSeq = seq
		return int64(end - start)
	}

	// Full page write arriving via multi-page small write.
	if start == 0 && end == uint32(sb.pageSize) {
		var prev int64
		if exists {
			for _, r := range entry.ranges {
				prev += int64(r.end - r.start)
			}
		}
		dst := sb.alloc(sb.pageSize)
		copy(dst, seg)
		if exists {
			entry.full = true
			entry.fullSeq = seq
			entry.fullData = dst
			entry.ranges = nil
		} else {
			sb.dirtyPages[pg] = sb.newEntry(pageEntry{
				full:     true,
				fullSeq:  seq,
				fullData: dst,
			})
		}
		sb.dirtyBytes += int64(sb.pageSize) - prev
		return prev
	}

	if !exists {
		dst := sb.alloc(len(seg))
		copy(dst, seg)
		sb.dirtyPages[pg] = sb.newEntry(pageEntry{
			ranges: []subRange{{start: start, end: end, seq: seq, data: dst}},
		})
		sb.dirtyBytes += int64(end - start)
		return 0
	}

	// Merge [start, end) into existing sorted non-overlapping ranges.
	// Any existing ranges that overlap or touch [start, end) are combined into
	// one contiguous merged interval with the new bytes taking precedence.
	var overlapBytes int64
	mergedStart := start
	mergedEnd := end

	firstTouch := len(entry.ranges)
	lastTouch := -1
	for i, r := range entry.ranges {
		if r.end < start {
			continue
		}
		if r.start > end {
			break
		}
		if firstTouch == len(entry.ranges) {
			firstTouch = i
		}
		lastTouch = i
		if r.start < mergedStart {
			mergedStart = r.start
		}
		if r.end > mergedEnd {
			mergedEnd = r.end
		}
		ovStart := max(r.start, start)
		ovEnd := min(r.end, end)
		if ovEnd > ovStart {
			overlapBytes += int64(ovEnd - ovStart)
		}
	}

	// Fast sub-case: exact in-place overwrite inside a single existing range!
	if firstTouch == lastTouch && firstTouch < len(entry.ranges) {
		r := &entry.ranges[firstTouch]
		if start >= r.start && end <= r.end {
			copy(r.data[start-r.start:end-r.start], seg)
			r.seq = seq
			return int64(end - start)
		}
	}

	mergedLen := int(mergedEnd - mergedStart)
	dst := sb.alloc(mergedLen)
	if lastTouch >= firstTouch {
		for i := firstTouch; i <= lastTouch; i++ {
			r := entry.ranges[i]
			copy(dst[r.start-mergedStart:r.end-mergedStart], r.data)
		}
	}
	copy(dst[start-mergedStart:end-mergedStart], seg)

	// If merged interval now covers the entire page, promote to full page!
	if mergedStart == 0 && mergedEnd == uint32(sb.pageSize) {
		entry.full = true
		entry.fullSeq = seq
		entry.fullData = dst
		entry.ranges = nil
		sb.dirtyBytes += int64(end-start) - overlapBytes
		return overlapBytes
	}

	merged := subRange{
		start: mergedStart,
		end:   mergedEnd,
		seq:   seq,
		data:  dst,
	}

	if lastTouch < firstTouch {
		ins := 0
		for ins < len(entry.ranges) && entry.ranges[ins].end < start {
			ins++
		}
		entry.ranges = append(entry.ranges, subRange{})
		copy(entry.ranges[ins+1:], entry.ranges[ins:])
		entry.ranges[ins] = merged
	} else {
		entry.ranges[firstTouch] = merged
		tailStart := lastTouch + 1
		if tailStart > firstTouch+1 {
			copy(entry.ranges[firstTouch+1:], entry.ranges[tailStart:])
			entry.ranges = entry.ranges[:len(entry.ranges)-(lastTouch-firstTouch)]
		}
	}

	sb.dirtyBytes += int64(end-start) - overlapBytes
	return overlapBytes
}

// overlayRead overlays any live dirty scrap bytes onto dst for [off, off+len(dst)).
// It returns true if at least one byte was served from the scrap buffer.
func (sb *scrapBuffer) overlayRead(off int64, dst []byte) bool {
	if len(sb.dirtyPages) == 0 || len(dst) == 0 {
		return false
	}
	ps := int64(sb.pageSize)
	firstPage := off / ps
	lastPage := (off + int64(len(dst)) - 1) / ps
	hit := false

	for pg := firstPage; pg <= lastPage; pg++ {
		entry, ok := sb.dirtyPages[pg]
		if !ok {
			continue
		}
		pgBase := pg * ps
		rStart := max(off, pgBase)
		rEnd := min(off+int64(len(dst)), pgBase+ps)
		if entry.full {
			pOff := rStart - pgBase
			dOff := rStart - off
			copy(dst[dOff:dOff+(rEnd-rStart)], entry.fullData[pOff:pOff+(rEnd-rStart)])
			hit = true
			continue
		}
		pReadStart := uint32(rStart - pgBase)
		pReadEnd := uint32(rEnd - pgBase)
		for _, r := range entry.ranges {
			if r.end <= pReadStart {
				continue
			}
			if r.start >= pReadEnd {
				break
			}
			ovStart := max(r.start, pReadStart)
			ovEnd := min(r.end, pReadEnd)
			dOff := (pgBase + int64(ovStart)) - off
			copy(dst[dOff:dOff+int64(ovEnd-ovStart)], r.data[ovStart-r.start:ovEnd-r.start])
			hit = true
		}
	}
	return hit
}

// invalidateRange drops all live dirty pages in [firstPage, lastPage),
// called by a page-aligned zero-copy direct bypass write that completely
// supersedes those pages. Returns total dirty bytes superseded.
func (sb *scrapBuffer) invalidateRange(firstPage, lastPage int64) int64 {
	if len(sb.dirtyPages) == 0 || firstPage >= lastPage {
		return 0
	}
	var superseded int64
	if (lastPage - firstPage) <= int64(len(sb.dirtyPages)) {
		for pg := firstPage; pg < lastPage; pg++ {
			if entry, ok := sb.dirtyPages[pg]; ok {
				superseded += sb.pageBytes(entry)
				delete(sb.dirtyPages, pg)
			}
		}
	} else {
		for pg, entry := range sb.dirtyPages {
			if pg >= firstPage && pg < lastPage {
				superseded += sb.pageBytes(entry)
				delete(sb.dirtyPages, pg)
			}
		}
	}
	sb.dirtyBytes -= superseded
	if len(sb.dirtyPages) == 0 {
		sb.releaseSlabs()
		clear(sb.entryArena)
		sb.entryArena = sb.entryArena[:0]
	}
	return superseded
}

func (sb *scrapBuffer) pageBytes(e *pageEntry) int64 {
	if e.full {
		return int64(sb.pageSize)
	}
	var n int64
	for _, r := range e.ranges {
		n += int64(r.end - r.start)
	}
	return n
}

// drainSorted extracts all dirty extents sorted by ascending offset and returns
// a cleanup function that returns the backing slabs to the pool once the caller
// finishes writing the extents out. The returned slice aliases internal reusable
// storage and is valid only until release() is called.
func (sb *scrapBuffer) drainSorted() (extents []Extent, release func()) {
	if len(sb.dirtyPages) == 0 {
		return nil, func() {}
	}
	sb.pagesBuf = sb.pagesBuf[:0]
	for pg := range sb.dirtyPages {
		sb.pagesBuf = append(sb.pagesBuf, pg)
	}
	sort.Slice(sb.pagesBuf, func(i, j int) bool { return sb.pagesBuf[i] < sb.pagesBuf[j] })

	ps := int64(sb.pageSize)
	sb.extentsBuf = sb.extentsBuf[:0]
	for _, pg := range sb.pagesBuf {
		e := sb.dirtyPages[pg]
		pgBase := pg * ps
		if e.full {
			sb.extentsBuf = append(sb.extentsBuf, Extent{
				Offset:   pgBase,
				WriteSeq: e.fullSeq,
				Data:     e.fullData,
			})
		} else {
			for _, r := range e.ranges {
				sb.extentsBuf = append(sb.extentsBuf, Extent{
					Offset:   pgBase + int64(r.start),
					WriteSeq: r.seq,
					Data:     r.data,
				})
			}
		}
	}

	oldSlabs := sb.slabs
	sb.slabs = nil
	clear(sb.dirtyPages)
	clear(sb.entryArena)
	sb.entryArena = sb.entryArena[:0]
	sb.dirtyBytes = 0
	sb.slabBytes = 0

	release = func() {
		clear(sb.extentsBuf)
		sb.extentsBuf = sb.extentsBuf[:0]
		for _, s := range oldSlabs {
			if cap(s.buf) == sb.slabSize {
				sb.pool.Put(s.buf[:sb.slabSize])
			}
		}
	}
	return sb.extentsBuf, release
}

func (sb *scrapBuffer) releaseSlabs() {
	for _, s := range sb.slabs {
		if cap(s.buf) == sb.slabSize {
			sb.pool.Put(s.buf[:sb.slabSize])
		}
	}
	sb.slabs = nil
	sb.slabBytes = 0
}
