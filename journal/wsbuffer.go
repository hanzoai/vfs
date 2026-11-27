package journal

import (
	"errors"
	"io"
	"slices"
	"sync"
)

// WSBuffer implements Write Scrap Buffer partial-write absorption for journal
// segment files, following the scrap-buffer principle of Zhan et al. (USENIX
// FAST '26, https://www.usenix.org/conference/fast26/presentation/zhan):
//
//   - Small or unaligned partial writes are absorbed into a bounded in-memory
//     scrap buffer indexed by physical page (4 KiB default). Adjacent and
//     overlapping partial writes coalesce into complete page-aligned runs before
//     reaching the underlying File.WriteAt, cutting physical page write
//     amplification and write syscall overhead.
//   - Large page-aligned writes (length >= BypassThreshold and aligned to
//     PageSize) and bulk writes >= Capacity bypass the scrap buffer completely
//     (zero-copy direct pass-through to the underlying File.WriteAt), avoiding
//     unnecessary page copies, syscall splitting, and cache pollution.
//   - ReadAt overlays dirty scrap pages over the underlying file so Replay and
//     concurrent readers always observe bit-exact latest bytes.
//   - Flush drains all dirty scrap pages in sorted byte-contiguous runs; Sync
//     flushes dirty pages before invoking the underlying fdatasync (releasing
//     the scrap mutex during fdatasync to preserve write/sync pipelining).

const (
	// DefaultWSPageSize is the default physical page alignment unit (4 KiB).
	DefaultWSPageSize = 4096
	// DefaultWSBufferCapacity is the default bounded scrap buffer capacity per file (16 KiB).
	DefaultWSBufferCapacity = 16 << 10
	// DefaultWSBypassThreshold is the default minimum aligned write length that
	// bypasses the scrap buffer zero-copy (4 KiB).
	DefaultWSBypassThreshold = 4096
)

// WSBufferConfig configures Write Scrap Buffer behavior.
type WSBufferConfig struct {
	// PageSize is the physical page alignment unit in bytes. Must be a power of
	// two >= 512; zero defaults to DefaultWSPageSize (4096).
	PageSize int
	// Capacity is the maximum dirty bytes held in the bounded scrap buffer per
	// open segment file. Zero defaults to DefaultWSBufferCapacity (16 KiB).
	// Rounded up to a multiple of PageSize.
	Capacity int
	// BypassThreshold is the minimum page-aligned write length eligible for
	// zero-copy direct pass-through. Zero defaults to PageSize (4096).
	BypassThreshold int
	// AlwaysCopy disables zero-copy direct bypass so every write copies through
	// the buffer. Intended for matched benchmark controls comparing an
	// always-copy buffered writer against adaptive WSBuffer bypass.
	AlwaysCopy bool
}

// DefaultWSBufferConfig returns the recommended production WSBuffer configuration
// (4 KiB pages, 16 KiB bounded scrap buffer, 4 KiB zero-copy bypass threshold).
func DefaultWSBufferConfig() WSBufferConfig {
	return WSBufferConfig{
		PageSize:        DefaultWSPageSize,
		Capacity:        DefaultWSBufferCapacity,
		BypassThreshold: DefaultWSBypassThreshold,
	}
}

func (c WSBufferConfig) normalized() WSBufferConfig {
	ps := c.PageSize
	if ps <= 0 {
		ps = DefaultWSPageSize
	}
	if ps < 512 {
		ps = 512
	}
	// Round up to next power of two if not already a power of two.
	if ps&(ps-1) != 0 {
		p := 512
		for p < ps {
			p <<= 1
		}
		ps = p
	}
	c.PageSize = ps

	capBytes := c.Capacity
	if capBytes <= 0 {
		capBytes = DefaultWSBufferCapacity
	}
	if capBytes < ps {
		capBytes = ps
	}
	if rem := capBytes % ps; rem != 0 {
		capBytes += ps - rem
	}
	c.Capacity = capBytes

	bt := c.BypassThreshold
	if bt <= 0 {
		bt = ps
	}
	if bt < ps {
		bt = ps
	}
	if rem := bt % ps; rem != 0 {
		bt += ps - rem
	}
	c.BypassThreshold = bt
	return c
}

// WSBufferStats records exact logical, physical, scrap-buffer, and bypass
// telemetry for observability, testing, and benchmarking.
type WSBufferStats struct {
	LogicalWriteOps    uint64 `json:"logical_write_ops"`
	LogicalWriteBytes  uint64 `json:"logical_write_bytes"`
	PhysicalWriteOps   uint64 `json:"physical_write_ops"`
	PhysicalWriteBytes uint64 `json:"physical_write_bytes"`
	PhysicalPages      uint64 `json:"physical_pages"`
	ScrapAbsorbedOps   uint64 `json:"scrap_absorbed_ops"`
	ScrapAbsorbedBytes uint64 `json:"scrap_absorbed_bytes"`
	ScrapCopyBytes     uint64 `json:"scrap_copy_bytes"`
	DirectBypassOps    uint64 `json:"direct_bypass_ops"`
	DirectBypassBytes  uint64 `json:"direct_bypass_bytes"`
	CoalescedHits      uint64 `json:"coalesced_hits"`
	FlushOps           uint64 `json:"flush_ops"`
	FlushPages         uint64 `json:"flush_pages"`
	SyncOps            uint64 `json:"sync_ops"`
	ReadHits           uint64 `json:"read_hits"`
	MaxDirtyBytes      uint64 `json:"max_dirty_bytes"`
}

// PhysicalWriteAmp returns the page-level physical write amplification factor:
// (PhysicalPages * pageSize) / LogicalWriteBytes. Returns 0 if no logical bytes
// have been written.
func (s WSBufferStats) PhysicalWriteAmp(pageSize int) float64 {
	if s.LogicalWriteBytes == 0 {
		return 0
	}
	if pageSize <= 0 {
		pageSize = DefaultWSPageSize
	}
	return (float64(s.PhysicalPages) * float64(pageSize)) / float64(s.LogicalWriteBytes)
}

// Add folds other into s.
func (s *WSBufferStats) Add(other WSBufferStats) {
	s.LogicalWriteOps += other.LogicalWriteOps
	s.LogicalWriteBytes += other.LogicalWriteBytes
	s.PhysicalWriteOps += other.PhysicalWriteOps
	s.PhysicalWriteBytes += other.PhysicalWriteBytes
	s.PhysicalPages += other.PhysicalPages
	s.ScrapAbsorbedOps += other.ScrapAbsorbedOps
	s.ScrapAbsorbedBytes += other.ScrapAbsorbedBytes
	s.ScrapCopyBytes += other.ScrapCopyBytes
	s.DirectBypassOps += other.DirectBypassOps
	s.DirectBypassBytes += other.DirectBypassBytes
	s.CoalescedHits += other.CoalescedHits
	s.FlushOps += other.FlushOps
	s.FlushPages += other.FlushPages
	s.SyncOps += other.SyncOps
	s.ReadHits += other.ReadHits
	if other.MaxDirtyBytes > s.MaxDirtyBytes {
		s.MaxDirtyBytes = other.MaxDirtyBytes
	}
}

type scrapPage struct {
	data    []byte
	lo      int // start offset of valid dirty bytes within page [0, pageSize)
	hi      int // end offset (exclusive) of valid dirty bytes within page (lo, pageSize]
	lastUse uint64
}

// WSBufferFile wraps a journal.File with a bounded Write Scrap Buffer and
// zero-copy large-aligned bypass. Safe for concurrent use.
type WSBufferFile struct {
	underlying File
	cfg        WSBufferConfig
	maxPages   int
	parent     *WSBufferDisk

	mu         sync.Mutex
	pages      map[int64]*scrapPage
	freeBufs   [][]byte
	flushBuf   []byte
	keysBuf    []int64
	dirtyBytes int64
	clock      uint64
	closed     bool
	stats      WSBufferStats
}

// NewWSBufferFile wraps f with a WSBuffer configured by cfg.
func NewWSBufferFile(f File, cfg WSBufferConfig) *WSBufferFile {
	norm := cfg.normalized()
	maxPages := norm.Capacity / norm.PageSize
	return &WSBufferFile{
		underlying: f,
		cfg:        norm,
		maxPages:   maxPages,
		pages:      make(map[int64]*scrapPage, maxPages),
		freeBufs:   make([][]byte, 0, maxPages),
		keysBuf:    make([]int64, 0, maxPages),
	}
}

// Config returns the normalized WSBufferConfig in effect.
func (f *WSBufferFile) Config() WSBufferConfig { return f.cfg }

// Size returns the underlying file's size.
func (f *WSBufferFile) Size() int64 { return f.underlying.Size() }

// Writeback forwards the advisory writeback hint to the underlying file without
// forcing premature partial-page eviction.
func (f *WSBufferFile) Writeback(off, n int64) error {
	return f.underlying.Writeback(off, n)
}

// DirtyBytes returns the number of dirty bytes currently buffered in the scrap buffer.
func (f *WSBufferFile) DirtyBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dirtyBytes
}

// Stats returns a point-in-time snapshot of the file's WSBuffer telemetry.
func (f *WSBufferFile) Stats() WSBufferStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

// WriteAt writes len(p) bytes at file offset off. Small or unaligned writes are
// absorbed into the bounded scrap buffer; aligned runs of at least
// BypassThreshold bytes bypass the scrap buffer zero-copy.
func (f *WSBufferFile) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, errors.New("journal: wsbuffer negative write offset")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, ErrClosed
	}

	ps := int64(f.cfg.PageSize)
	total := len(p)
	end := off + int64(total)

	f.stats.LogicalWriteOps++
	f.stats.LogicalWriteBytes += uint64(total)

	alignStart := (off + ps - 1) &^ (ps - 1)
	alignEnd := end &^ (ps - 1)
	alignedLen := alignEnd - alignStart
	canBypass := !f.cfg.AlwaysCopy && alignedLen >= int64(f.cfg.BypassThreshold)

	// Fast path 1: pure page-aligned large write -> zero-copy direct pass-through.
	if canBypass && alignStart == off && alignEnd == end {
		f.invalidateRangeLocked(off/ps, end/ps)
		n, err := f.underlying.WriteAt(p, off)
		if n > 0 {
			f.recordPhysicalWriteLocked(off, n)
			f.stats.DirectBypassOps++
			f.stats.DirectBypassBytes += uint64(n)
		}
		if err != nil {
			return n, err
		}
		if n != total {
			return n, io.ErrShortWrite
		}
		return n, nil
	}

	// Fast path 2: bulk write >= Capacity (e.g., an already-batched multi-page
	// journal flush). Flush any partially overlapped edge pages, invalidate
	// completely overwritten interior pages, and pass through zero-copy in one
	// WriteAt rather than splitting into three separate writes.
	if !f.cfg.AlwaysCopy && total >= f.cfg.Capacity {
		firstPg := off / ps
		lastPg := (end - 1) / ps
		var edgeKeys []int64
		if off%ps != 0 {
			if _, ok := f.pages[firstPg]; ok {
				edgeKeys = append(edgeKeys, firstPg)
			}
		}
		if end%ps != 0 && lastPg != firstPg {
			if _, ok := f.pages[lastPg]; ok {
				edgeKeys = append(edgeKeys, lastPg)
			}
		}
		if len(edgeKeys) > 0 {
			if err := f.flushKeysLocked(edgeKeys); err != nil {
				return 0, err
			}
		}
		if alignStart < alignEnd {
			f.invalidateRangeLocked(alignStart/ps, alignEnd/ps)
		}
		n, err := f.underlying.WriteAt(p, off)
		if n > 0 {
			f.recordPhysicalWriteLocked(off, n)
			f.stats.DirectBypassOps++
			f.stats.DirectBypassBytes += uint64(n)
		}
		if err != nil {
			return n, err
		}
		if n != total {
			return n, io.ErrShortWrite
		}
		return n, nil
	}

	// Scrap absorption path: small or unaligned writes < Capacity (or AlwaysCopy control).
	if err := f.absorbLocked(p, off); err != nil {
		return 0, err
	}
	return total, nil
}

// ReadAt reads len(p) bytes starting at off, overlaying any dirty scrap-buffered
// bytes so readers always observe the latest written state.
func (f *WSBufferFile) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err := f.underlying.ReadAt(p, off)
	if len(f.pages) == 0 || n == 0 {
		return n, err
	}

	ps := int64(f.cfg.PageSize)
	readEnd := off + int64(n)
	firstPg := off / ps
	lastPg := (readEnd - 1) / ps
	hit := false

	for pg := firstPg; pg <= lastPg; pg++ {
		sp, ok := f.pages[pg]
		if !ok {
			continue
		}
		baseOff := pg * ps
		dirtyStart := baseOff + int64(sp.lo)
		dirtyEnd := baseOff + int64(sp.hi)
		ovStart := max(off, dirtyStart)
		ovEnd := min(readEnd, dirtyEnd)
		if ovStart < ovEnd {
			copy(p[ovStart-off:ovEnd-off], sp.data[ovStart-baseOff:ovEnd-baseOff])
			hit = true
		}
	}
	if hit {
		f.stats.ReadHits++
	}
	return n, err
}

// Flush writes all dirty scrap-buffered pages to the underlying file in sorted
// byte-contiguous runs without calling Sync.
func (f *WSBufferFile) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	return f.flushAllLocked()
}

// Sync flushes all dirty scrap-buffered pages to the underlying file and then
// calls underlying.Sync(). The scrap mutex is released before underlying.Sync()
// so subsequent writes can continue absorbing into the scrap buffer while
// fdatasync is in flight.
func (f *WSBufferFile) Sync() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return ErrClosed
	}
	err := f.flushAllLocked()
	f.stats.SyncOps++
	f.mu.Unlock()
	if err != nil {
		return err
	}
	return f.underlying.Sync()
}

// Close flushes any remaining dirty scrap pages and closes the underlying file.
func (f *WSBufferFile) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	err := f.flushAllLocked()
	cerr := f.underlying.Close()
	if err == nil {
		err = cerr
	}
	f.closed = true
	finalStats := f.stats
	parent := f.parent
	f.mu.Unlock()

	if parent != nil {
		parent.retire(f, finalStats)
	}
	return err
}

// absorbLocked absorbs p at off into page-indexed scrap slots. Caller holds f.mu.
func (f *WSBufferFile) absorbLocked(p []byte, off int64) error {
	ps := int64(f.cfg.PageSize)
	f.stats.ScrapAbsorbedOps++
	f.stats.ScrapAbsorbedBytes += uint64(len(p))

	pos := 0
	curOff := off
	for pos < len(p) {
		pg := curOff / ps
		baseOff := pg * ps
		wLo := int(curOff - baseOff)
		take := min(len(p)-pos, f.cfg.PageSize-wLo)
		wHi := wLo + take
		chunk := p[pos : pos+take]

		sp, exists := f.pages[pg]
		if !exists {
			if len(f.pages) >= f.maxPages {
				if err := f.evictLocked(); err != nil {
					return err
				}
			}
			buf := f.allocPageLocked()
			copy(buf[wLo:wHi], chunk)
			f.clock++
			f.pages[pg] = &scrapPage{
				data:    buf,
				lo:      wLo,
				hi:      wHi,
				lastUse: f.clock,
			}
			f.dirtyBytes += int64(take)
			f.stats.ScrapCopyBytes += uint64(take)
		} else {
			oldSpan := sp.hi - sp.lo
			if wHi < sp.lo {
				// Disjoint write before existing dirty span: bridge gap from underlying file.
				clear(sp.data[wHi:sp.lo])
				if _, err := f.underlying.ReadAt(sp.data[wHi:sp.lo], baseOff+int64(wHi)); err != nil && !errors.Is(err, io.EOF) {
					return err
				}
				copy(sp.data[wLo:wHi], chunk)
				sp.lo = wLo
			} else if wLo > sp.hi {
				// Disjoint write after existing dirty span: bridge gap from underlying file.
				clear(sp.data[sp.hi:wLo])
				if _, err := f.underlying.ReadAt(sp.data[sp.hi:wLo], baseOff+int64(sp.hi)); err != nil && !errors.Is(err, io.EOF) {
					return err
				}
				copy(sp.data[wLo:wHi], chunk)
				sp.hi = wHi
			} else {
				// Adjacent or overlapping: merge in-place with zero disk read.
				copy(sp.data[wLo:wHi], chunk)
				if wLo < sp.lo {
					sp.lo = wLo
				}
				if wHi > sp.hi {
					sp.hi = wHi
				}
			}
			newSpan := sp.hi - sp.lo
			f.dirtyBytes += int64(newSpan - oldSpan)
			f.clock++
			sp.lastUse = f.clock
			f.stats.ScrapCopyBytes += uint64(take)
			f.stats.CoalescedHits++
		}

		if uint64(f.dirtyBytes) > f.stats.MaxDirtyBytes {
			f.stats.MaxDirtyBytes = uint64(f.dirtyBytes)
		}

		pos += take
		curOff += int64(take)
	}
	return nil
}

// invalidateRangeLocked removes any dirty scrap pages in [firstPg, endPg)
// without flushing them because a direct bypass write is overwriting every byte
// of those pages. Caller holds f.mu.
func (f *WSBufferFile) invalidateRangeLocked(firstPg, endPg int64) {
	if len(f.pages) == 0 {
		return
	}
	if endPg-firstPg <= int64(len(f.pages)) {
		for pg := firstPg; pg < endPg; pg++ {
			if sp, ok := f.pages[pg]; ok {
				f.dirtyBytes -= int64(sp.hi - sp.lo)
				f.releasePageLocked(sp.data)
				delete(f.pages, pg)
			}
		}
		return
	}
	for pg, sp := range f.pages {
		if pg >= firstPg && pg < endPg {
			f.dirtyBytes -= int64(sp.hi - sp.lo)
			f.releasePageLocked(sp.data)
			delete(f.pages, pg)
		}
	}
}

// evictLocked frees room in the scrap buffer when at capacity. Full pages
// (which have already achieved 1.00x page alignment) are flushed together in
// contiguous runs first; if no full pages exist, the least-recently-used page
// is flushed. Caller holds f.mu.
func (f *WSBufferFile) evictLocked() error {
	f.keysBuf = f.keysBuf[:0]
	for pg, sp := range f.pages {
		if sp.lo == 0 && sp.hi == f.cfg.PageSize {
			f.keysBuf = append(f.keysBuf, pg)
		}
	}
	if len(f.keysBuf) > 0 {
		slices.Sort(f.keysBuf)
		return f.flushKeysLocked(f.keysBuf)
	}

	// No full pages available: evict the least-recently-used dirty page.
	var victimPg int64
	var victimUse uint64
	first := true
	for pg, sp := range f.pages {
		if first || sp.lastUse < victimUse {
			victimPg = pg
			victimUse = sp.lastUse
			first = false
		}
	}
	if first {
		return nil
	}
	f.keysBuf = append(f.keysBuf[:0], victimPg)
	return f.flushKeysLocked(f.keysBuf)
}

func (f *WSBufferFile) flushAllLocked() error {
	if len(f.pages) == 0 {
		return nil
	}
	f.keysBuf = f.keysBuf[:0]
	for pg := range f.pages {
		f.keysBuf = append(f.keysBuf, pg)
	}
	slices.Sort(f.keysBuf)
	return f.flushKeysLocked(f.keysBuf)
}

// flushKeysLocked writes the sorted page indices in keys to the underlying file,
// coalescing byte-contiguous adjacent pages into single WriteAt calls.
// Caller holds f.mu.
func (f *WSBufferFile) flushKeysLocked(keys []int64) error {
	if len(keys) == 0 {
		return nil
	}
	ps := int64(f.cfg.PageSize)
	i := 0
	for i < len(keys) {
		j := i + 1
		for j < len(keys) {
			prevPg := keys[j-1]
			curPg := keys[j]
			if curPg != prevPg+1 {
				break
			}
			prevSp := f.pages[prevPg]
			curSp := f.pages[curPg]
			if prevSp.hi != f.cfg.PageSize || curSp.lo != 0 {
				break
			}
			j++
		}

		firstPg := keys[i]
		firstSp := f.pages[firstPg]
		runOff := firstPg*ps + int64(firstSp.lo)
		runCount := j - i

		if runCount == 1 {
			slice := firstSp.data[firstSp.lo:firstSp.hi]
			n, err := f.underlying.WriteAt(slice, runOff)
			if n > 0 {
				f.recordPhysicalWriteLocked(runOff, n)
			}
			if err != nil {
				return err
			}
			if n != len(slice) {
				return io.ErrShortWrite
			}
			f.dirtyBytes -= int64(firstSp.hi - firstSp.lo)
			f.releasePageLocked(firstSp.data)
			delete(f.pages, firstPg)
			f.stats.FlushPages++
		} else {
			totalLen := 0
			for k := i; k < j; k++ {
				sp := f.pages[keys[k]]
				totalLen += sp.hi - sp.lo
			}
			if cap(f.flushBuf) < totalLen {
				f.flushBuf = make([]byte, totalLen)
			}
			buf := f.flushBuf[:0]
			for k := i; k < j; k++ {
				sp := f.pages[keys[k]]
				buf = append(buf, sp.data[sp.lo:sp.hi]...)
			}
			n, err := f.underlying.WriteAt(buf, runOff)
			if n > 0 {
				f.recordPhysicalWriteLocked(runOff, n)
			}
			if err != nil {
				return err
			}
			if n != len(buf) {
				return io.ErrShortWrite
			}
			for k := i; k < j; k++ {
				pg := keys[k]
				sp := f.pages[pg]
				f.dirtyBytes -= int64(sp.hi - sp.lo)
				f.releasePageLocked(sp.data)
				delete(f.pages, pg)
			}
			f.stats.FlushPages += uint64(runCount)
		}
		i = j
	}
	f.stats.FlushOps++
	return nil
}

func (f *WSBufferFile) recordPhysicalWriteLocked(off int64, n int) {
	if n <= 0 {
		return
	}
	ps := int64(f.cfg.PageSize)
	firstPg := off / ps
	lastPg := (off + int64(n) - 1) / ps
	f.stats.PhysicalWriteOps++
	f.stats.PhysicalWriteBytes += uint64(n)
	f.stats.PhysicalPages += uint64(lastPg - firstPg + 1)
}

func (f *WSBufferFile) allocPageLocked() []byte {
	if n := len(f.freeBufs); n > 0 {
		buf := f.freeBufs[n-1]
		f.freeBufs = f.freeBufs[:n-1]
		return buf
	}
	return make([]byte, f.cfg.PageSize)
}

func (f *WSBufferFile) releasePageLocked(buf []byte) {
	if len(f.freeBufs) < f.maxPages {
		f.freeBufs = append(f.freeBufs, buf)
	}
}

// WSBufferDisk wraps a journal.Disk so every segment file created or opened
// routes through a WSBufferFile with the configured parameters.
type WSBufferDisk struct {
	underlying Disk
	cfg        WSBufferConfig

	mu      sync.Mutex
	active  map[*WSBufferFile]struct{}
	retired WSBufferStats
}

// WrapWSBuffer wraps d with Write Scrap Buffer partial-write absorption.
func WrapWSBuffer(d Disk, cfg WSBufferConfig) *WSBufferDisk {
	return &WSBufferDisk{
		underlying: d,
		cfg:        cfg.normalized(),
		active:     make(map[*WSBufferFile]struct{}),
	}
}

// Config returns the normalized WSBufferConfig.
func (d *WSBufferDisk) Config() WSBufferConfig { return d.cfg }

// List delegates to the underlying Disk.
func (d *WSBufferDisk) List() ([]string, error) { return d.underlying.List() }

// Remove delegates to the underlying Disk.
func (d *WSBufferDisk) Remove(name string) error { return d.underlying.Remove(name) }

// Close delegates to the underlying Disk.
func (d *WSBufferDisk) Close() error { return d.underlying.Close() }

// Warnings forwards filesystem risk warnings from the underlying Disk when supported.
func (d *WSBufferDisk) Warnings() []string {
	if w, ok := d.underlying.(interface{ Warnings() []string }); ok {
		return w.Warnings()
	}
	return nil
}

// Create creates a segment file on the underlying Disk and wraps it in a WSBufferFile.
func (d *WSBufferDisk) Create(name string, size int64) (File, error) {
	f, err := d.underlying.Create(name, size)
	if err != nil {
		return nil, err
	}
	return d.wrap(f), nil
}

// Open opens a segment file on the underlying Disk and wraps it in a WSBufferFile.
func (d *WSBufferDisk) Open(name string) (File, error) {
	f, err := d.underlying.Open(name)
	if err != nil {
		return nil, err
	}
	return d.wrap(f), nil
}

func (d *WSBufferDisk) wrap(f File) *WSBufferFile {
	wf := NewWSBufferFile(f, d.cfg)
	wf.parent = d
	d.mu.Lock()
	d.active[wf] = struct{}{}
	d.mu.Unlock()
	return wf
}

func (d *WSBufferDisk) retire(f *WSBufferFile, final WSBufferStats) {
	d.mu.Lock()
	if _, ok := d.active[f]; ok {
		delete(d.active, f)
		d.retired.Add(final)
	}
	d.mu.Unlock()
}

// Stats returns cumulative WSBufferStats across all active and closed files
// created or opened through this WSBufferDisk.
func (d *WSBufferDisk) Stats() WSBufferStats {
	d.mu.Lock()
	out := d.retired
	active := make([]*WSBufferFile, 0, len(d.active))
	for wf := range d.active {
		active = append(active, wf)
	}
	d.mu.Unlock()

	for _, wf := range active {
		out.Add(wf.Stats())
	}
	return out
}
