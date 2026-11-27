// Package journal is an append-only log on local disk that makes a write
// acknowledgement and an fsync run at the speed of the local NVMe, while a
// slower publisher (the object store) catches up behind it.
//
// Every record has a sequence number and moves through three watermarks:
//
//	visible    the record was appended; later reads see it. Append returns here.
//	durable    the record is on local disk and survives a process crash and a
//	           power loss. Sync waits for it.
//	published  the record is in the object store. The publisher reports it
//	           with Publish, and the journal then reclaims the space.
//
// Published <= Durable <= Visible. The publisher may only publish what is
// durable: Publish refuses a sequence a local sync has not covered, so the store
// never holds a record the journal could still lose.
//
// The design follows a commit pipeline in which sequence assignment is separate
// from the write and from a sync queue that merges concurrent waiters into one
// fsync; a durable-sequence watermark that callers wait on; bounded unflushed
// bytes, with admission control when the remote falls behind; and systematic
// crash testing. Segments are written once, in full, and recycled, because an
// fsync into blocks that are already written is cheaper than one into freshly
// allocated blocks. It is written independently of any of them.
//
// Its contents are transient: every record is published and then reclaimed. The
// layout therefore has no version and no upgrade path. A different layout means
// publishing everything, closing the journal and deleting its directory; Open
// refuses a directory with a segment it cannot read, and touches nothing in it.
//
// See docs/journal.md for the contract and the integration interface.
package journal

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"
	"sync"
	"time"
)

var (
	// ErrFull is returned by Append when the journal's capacity is used up by
	// records the publisher has not yet published, or when no segment file can
	// be made. Nothing was appended.
	ErrFull = errors.New("journal: full")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("journal: closed")
	// ErrTooLarge is returned by Append for a payload that cannot fit in one segment.
	ErrTooLarge = errors.New("journal: payload larger than a segment")
	// ErrAhead is returned for a sequence that is ahead of what the call needs:
	// Sync of a record not yet appended, Publish of a record not yet durable.
	ErrAhead = errors.New("journal: sequence ahead of the journal")
	// ErrCorrupt is returned by Open and Replay when a record that was made
	// durable is not on the disk any more or no longer validates.
	ErrCorrupt = errors.New("journal: corrupt record")
	// ErrMismatch is returned by Open when the sequence the caller says the
	// store holds does not fit the journal: the store is behind what the journal
	// has already reclaimed (a stale or wrong Head), behind the first record the
	// journal holds, or ahead of the last one. Open wrote nothing. Discard is the
	// explicit way to give up the records.
	ErrMismatch = errors.New("journal: sequence does not match the store")
	// ErrLineage is returned by Open when the directory holds a journal of
	// another identity (another volume, or an older fencing round). Open wrote
	// nothing. Discard is the explicit way to give up the records.
	ErrLineage = errors.New("journal: directory belongs to another lineage")
	// ErrLocked is returned by Open when another journal has the directory.
	ErrLocked = errors.New("journal: directory is in use by another journal")
	// ErrVolatile is returned by Open for a directory on memory (tmpfs, ramfs),
	// where a sync makes nothing durable.
	ErrVolatile = errors.New("journal: directory is on volatile memory")
	// ErrFormat is returned by Open and Discard when the directory holds a
	// segment file that is not in this layout (another build's, or not a segment
	// at all). Nothing was touched: such a file is neither read, nor reused, nor
	// deleted.
	ErrFormat = errors.New("journal: directory holds a file that is not a segment of this layout")
	// ErrRange is returned by Discard when the journal does not hold exactly the
	// records the caller named.
	ErrRange = errors.New("journal: records are not the range named")
)

// Options configure Open.
type Options struct {
	// Capacity is the most disk the journal may use, in bytes. It is required:
	// set it from the node's NVMe budget. At least two segments.
	Capacity int64
	// Segment is the size of one segment file, at most 4 GiB. Default 64 MiB.
	Segment int64
	// Bound is the longest an acknowledged record should stay unpublished.
	// State reports Degraded when the oldest one is older. Default 5 s.
	Bound time.Duration
	// Retry is the least time between two firings of Due. A publisher whose
	// publication fails sees the same overdue record each time; Retry keeps its
	// loop from spinning. Default 100 ms.
	Retry time.Duration
	// Published is the highest sequence the store holds (the Head's journal
	// sequence; 0 for a store with none). It is the truth about what is
	// published, and it must be the store's: Open returns ErrMismatch rather than
	// guess when it does not fit the journal. Records at or below it are not
	// replayed, and new sequences continue after it.
	Published uint64
	// Lineage is the identity of the history this journal belongs to, such as a
	// volume and its fencing round. It is written into every segment and mixed
	// into every checksum. Open refuses a directory of another lineage, so an
	// old owner's records cannot be replayed over a newer Head.
	Lineage []byte
	// Disk is the file layer. Nil means the operating system's files in dir.
	// The journal owns it after Open and closes it with the journal.
	Disk Disk
	// Clock supplies time. Nil means the wall clock.
	Clock Clock
	// WSBuffer, when non-nil, wraps the journal's Disk with a Write Scrap Buffer
	// that absorbs small unaligned partial writes into aligned pages while
	// bypassing large page-aligned writes zero-copy. Nil disables it (default).
	WSBuffer *WSBufferConfig
}

// Marks are the three watermarks. Published <= Durable <= Visible.
type Marks struct {
	Visible   uint64
	Durable   uint64
	Published uint64
}

// State is what a caller or an agent reads to learn whether the publication
// bound can be trusted.
type State struct {
	// Degraded is true when the bound cannot be guaranteed: the oldest
	// unpublished record is older than Options.Bound, the journal is slowing
	// appends because it is nearly full, or it has failed.
	Degraded bool
	// Backlog is the bytes of acknowledged records not yet published.
	Backlog int64
	// Age is how long the oldest unpublished record has been acknowledged. It
	// is never reset by later writes, and a restart never lengthens it. Zero
	// when nothing is unpublished.
	Age time.Duration
	// Err is the error that stopped the journal, if any.
	Err error
}

// Range is a run of sequence numbers.
type Range struct{ First, Last uint64 }

// Report is what Open found and what it gave up.
type Report struct {
	// First and Last are the sequences Open kept after Published. Last < First
	// when there are none.
	First, Last uint64
	// Proven is the highest sequence the log proves had been made durable by a
	// sync before it stopped. Records after it, up to Last, were recovered
	// but nothing shows their sync had finished; Open synced them.
	Proven uint64
	// Dropped lists the valid records Open gave up: the intact segments it wiped
	// because they follow a gap in the log (what follows a gap is not a prefix,
	// and nothing there was proven durable), and the records behind a damaged
	// payload in the last segment, from the damaged record on. Damage to a
	// record's header ends the log where it stands and is not looked past: the
	// bytes after Last in the last segment are never read again, and what they
	// held, if anything, is not listed.
	Dropped []Range
	// Warnings name risks of the directory's file system (a layered or remote
	// one, where a sync may not reach the media), and every segment whose header
	// is damaged and whose records reach a sequence above Published: they could
	// not be read, and if they were synced they are lost. A header torn on its way
	// in or out raises none, since what is behind it is new, or was all published.
	Warnings []string
}

// Record is one journal record.
type Record struct {
	Seq  uint64
	Time time.Time // when Append acknowledged it
	Data []byte    // valid only during the Replay callback
}

// Stats count what the journal did, for metrics and benchmarks.
type Stats struct {
	Records uint64 // records appended
	Bytes   uint64 // bytes appended, record headers included
	Waited  uint64 // Sync calls that had to wait
	Rounds  uint64 // local syncs (one per group)
	// Groups[i] counts rounds that released n waiters with bits.Len(n) == i,
	// the last bucket taking every larger group.
	Groups [12]uint64
}

const (
	batchCap   = 4 << 20          // bytes one write holds before the next record starts another
	batchSmall = 256 << 10        // the same, for a batch of small records
	maxPending = 16 << 20         // bytes queued for the file before Append waits for the writer
	syncBytes  = 8 << 20          // unsynced bytes that start a sync nobody asked for
	exposed    = 16 << 20         // bytes written but not synced before the writer waits for a sync
	hintBytes  = 1 << 20          // bytes written to a segment before the system is asked to start writing them out
	slowFrom   = 0.75             // fill at which Append starts to slow
	slowRate   = 64 << 20         // bytes per second Append is held to at a full journal
	markBytes  = 1 << 20          // bytes synced since the last mark that call for another
	quantum    = time.Millisecond // acknowledgement times closer than this share a span
)

// markRounds is how many sync rounds pass between two marks. A mark is a second
// block to flush (the one that holds the preamble), which on a disk that pays
// for each block doubles the cost of a single writer's fsync; spaced out, it
// costs a few percent, and the proof it gives lags by that many rounds.
var markRounds = 16

// slot is one segment file and, while it is in the log, the segment it holds.
type slot struct {
	name  string
	id    uint64
	size  int64
	ready chan struct{} // closed once file and err are set
	file  File
	err   error

	damagedSeq uint64 // its header fails its checksum: the last sequence its records name, 0 if none (see inventory)

	first uint64 // sequence of the first record; 0 until one is placed
	last  uint64 // sequence of the last record placed
	end   int64  // offset past the last byte placed
	life  uint64
	seed  uint32
	dirty bool  // written since it was last synced
	hint  int64 // offset up to which writeback has been requested

	// The mark last written, and how many; guarded by Journal.markmu.
	through, floor uint64
	marks          uint64
}

type batch struct {
	slot *slot
	off  int64
	data []byte
	last uint64 // sequence of the last record in data
}

type waiter struct {
	seq  uint64
	done chan struct{}
	err  error
}

type span struct {
	first uint64    // first sequence in the span
	ack   time.Time // acknowledgement time of that record
	cum   uint64    // bytes appended before it
}

// Journal is safe for concurrent use.
type Journal struct {
	disk    Disk
	ws      *WSBufferDisk
	clock   Clock
	segment int64
	limit   int // most segment files
	bound   time.Duration
	retry   time.Duration
	every   int // sync rounds between marks
	lineage lineage
	report  Report

	rw     sync.RWMutex // Replay holds it shared, Reclaim exclusive: a segment is not recycled while it is read
	mu     sync.Mutex
	markmu sync.Mutex // serializes the writes of marks, and guards the mark fields of a slot that holds records

	next      uint64 // next sequence to assign
	written   uint64 // highest sequence written to a file
	synced    uint64 // highest sequence a completed sync covered
	published uint64
	ack       time.Time // last acknowledgement time given

	queue   []*batch // batches not yet written, in order
	cur     *batch   // last batch of queue, still open to Append
	pending int64    // bytes in queue
	room    chan struct{}

	all    []*slot // every segment file the journal opened or made
	segs   []*slot // log segments, oldest first; the last is the tail
	tail   *slot
	last   *slot // segment of the last batch written
	spare  *slot // next segment, ready or being created
	free   []*slot
	nextID uint64
	nslots int
	held   int64 // bytes in segs, published or not

	dirty    []*slot
	rounds   int   // sync rounds since the last mark
	volume   int64 // bytes synced since the last mark
	unsynced int64 // bytes written since the last sync began
	behind   int64 // bytes written and not covered by a completed sync
	waiters  []*waiter
	wdone    bool // the writer has exited

	spans []span
	head  int // spans[head:] are live
	total uint64
	debt  time.Duration

	dues  []*due
	fired time.Time // when Due last fired

	err    error
	closed bool
	done   chan struct{} // closed on Close or failure
	kickw  chan struct{}
	kicks  chan struct{}
	wg     sync.WaitGroup
	stats  Stats
}

func newJournal(opt Options) (*Journal, error) {
	if opt.Segment == 0 {
		opt.Segment = 64 << 20
	}
	if opt.Bound == 0 {
		opt.Bound = 5 * time.Second
	}
	if opt.Retry == 0 {
		opt.Retry = 100 * time.Millisecond
	}
	if opt.Segment <= dataStart+recordHeader {
		return nil, fmt.Errorf("journal: segment of %d bytes is too small", opt.Segment)
	}
	if opt.Segment > math.MaxUint32 {
		return nil, fmt.Errorf("journal: segment of %d bytes is larger than 4 GiB", opt.Segment)
	}
	limit := opt.Capacity / opt.Segment
	if limit < 2 {
		return nil, fmt.Errorf("journal: capacity %d holds fewer than two segments of %d", opt.Capacity, opt.Segment)
	}
	if opt.Clock == nil {
		opt.Clock = wallClock{}
	}
	return &Journal{
		disk:    opt.Disk,
		clock:   opt.Clock,
		segment: opt.Segment,
		limit:   int(limit),
		bound:   opt.Bound,
		retry:   opt.Retry,
		every:   markRounds,
		lineage: lineageOf(opt.Lineage),
		room:    make(chan struct{}),
		done:    make(chan struct{}),
		kickw:   make(chan struct{}, 1),
		kicks:   make(chan struct{}, 1),
	}, nil
}

// Open opens the journal in dir, recovering what is on disk: it keeps the
// longest prefix of valid records and makes what it kept durable before it
// returns. New records go to a fresh segment, never behind a torn tail.
//
// Open returns an error and writes nothing when the directory is in use
// (ErrLocked), is on memory (ErrVolatile), belongs to another lineage
// (ErrLineage), does not fit Options.Published (ErrMismatch), or has lost
// records that were made durable (ErrCorrupt). It wipes only segments that
// follow a gap in a log with no proof of a finished sync past the gap, a torn
// tail, and Recovery says which. Discard is the explicit way to give up records.
func Open(dir string, opt Options) (*Journal, error) {
	j, err := newJournal(opt)
	if err != nil {
		if opt.Disk != nil {
			opt.Disk.Close() // the journal owns the Disk from the call, even one it refused
		}
		return nil, err
	}
	disk := opt.Disk
	if disk == nil {
		if disk, err = Files(dir); err != nil {
			return nil, err
		}
	}
	if opt.WSBuffer != nil {
		ws := WrapWSBuffer(disk, *opt.WSBuffer)
		j.ws = ws
		disk = ws
	}
	j.disk = disk
	if err := j.recover(opt.Published); err != nil {
		j.wg.Wait()
		j.release()
		return nil, err
	}
	j.wg.Add(2)
	go j.writer()
	go j.syncer()
	return j, nil
}

// Recovery reports what Open kept and what it gave up.
func (j *Journal) Recovery() Report { return j.report }

func (j *Journal) kick(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// fault reports why the journal cannot take work. Caller holds mu.
func (j *Journal) fault() error {
	if j.err != nil {
		return j.err
	}
	if j.closed {
		return ErrClosed
	}
	return nil
}

// failLocked stops the journal on an I/O error. After a failed write or sync
// the state of the file is unknown, so nothing is retried. Everything waiting
// on the journal learns of it: waiters get the error and Due fires.
func (j *Journal) failLocked(err error) {
	if j.err != nil {
		return
	}
	j.err = fmt.Errorf("journal: %w", err)
	j.finish()
	// Due first: a caller whose Sync fails must find Due already closed.
	for _, d := range append([]*due(nil), j.dues...) {
		j.fire(d)
	}
	for _, w := range j.waiters {
		w.err = j.err
		close(w.done)
	}
	j.waiters = nil
	j.kick(j.kickw)
	j.kick(j.kicks)
	j.broadcast()
}

func (j *Journal) finish() {
	select {
	case <-j.done:
	default:
		close(j.done)
	}
}

// broadcast wakes everything waiting on progress of the writer. Caller holds mu.
func (j *Journal) broadcast() {
	close(j.room)
	j.room = make(chan struct{})
}

// Append adds payload to the journal and returns its sequence number. The
// record is visible when Append returns; it is durable only after Sync. Near
// capacity Append slows down; at capacity it returns ErrFull.
func (j *Journal) Append(ctx context.Context, payload []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n := int64(recordHeader) + int64(len(payload))
	if n > j.segment-dataStart {
		return 0, ErrTooLarge
	}
	sum := crc32.Checksum(payload, castagnoli)

	j.mu.Lock()
	if err := j.fault(); err != nil {
		j.mu.Unlock()
		return 0, err
	}
	delay := j.slow(n)
	j.mu.Unlock()
	if delay > 0 {
		if err := j.sleep(ctx, delay); err != nil {
			return 0, err
		}
	}

	for {
		j.mu.Lock()
		if err := j.fault(); err != nil {
			j.mu.Unlock()
			return 0, err
		}
		var wait <-chan struct{} = j.room
		if j.pending < maxPending {
			var seq uint64
			var err error
			if seq, wait, err = j.place(payload, sum, n); wait == nil {
				j.mu.Unlock()
				return seq, err
			}
		}
		j.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-j.done:
		}
	}
}

// slow returns how long Append must wait before it may place n bytes. The wait
// grows with how full the journal is, so the writers' rate falls toward
// slowRate as the journal fills and settles where it matches the drain. The
// debt carries fractions of a millisecond between calls so small records are
// held to the same rate as large ones. Caller holds mu.
func (j *Journal) slow(n int64) time.Duration {
	fill := float64(j.held) / float64(int64(j.limit)*j.segment)
	if fill <= slowFrom {
		j.debt = 0
		return 0
	}
	t := min((fill-slowFrom)/(1-slowFrom), 1)
	j.debt += time.Duration(float64(n) / slowRate * t * float64(time.Second))
	if j.debt < time.Millisecond {
		return 0
	}
	d := j.debt
	j.debt = 0
	return d
}

func (j *Journal) sleep(ctx context.Context, d time.Duration) error {
	fired := make(chan struct{})
	t := j.clock.AfterFunc(d, func() { close(fired) })
	select {
	case <-fired:
		return nil
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	case <-j.done:
		t.Stop()
		return nil
	}
}

// place assigns the next sequence to payload and queues its record. When the
// record needs a segment that is still being made it queues nothing and returns
// a channel to wait on. Caller holds mu.
func (j *Journal) place(payload []byte, sum uint32, n int64) (uint64, <-chan struct{}, error) {
	if j.tail == nil || j.tail.end+n > j.tail.size {
		if wait, err := j.rotate(); err != nil || wait != nil {
			return 0, wait, err
		}
	}
	t := j.tail
	seq := j.next
	now := j.clock.Now()
	if now.Before(j.ack) {
		now = j.ack
	}
	j.ack = now

	header := t.first == 0
	if header {
		t.first = seq
	}
	b := j.cur
	if b == nil || b.slot != t || int64(cap(b.data)-len(b.data)) < n {
		b = j.open(t, header, n)
	}
	b.data = appendRecord(b.data, t.seed, seq, now.UnixNano(), payload, sum)
	b.last = seq
	t.end += n
	t.last = seq
	j.held += n
	j.pending += n
	j.next++
	j.stats.Records++
	j.stats.Bytes += uint64(n)

	fresh := j.head == len(j.spans) // nothing was unpublished
	if fresh || now.Sub(j.spans[len(j.spans)-1].ack) >= quantum {
		j.spans = append(j.spans, span{seq, now, j.total})
	}
	j.total += uint64(n)
	if fresh {
		j.rearm()
	}
	j.kick(j.kickw)
	return seq, nil, nil
}

// open starts a batch for slot t. The first batch of a segment starts at its
// header. Caller holds mu.
func (j *Journal) open(t *slot, header bool, n int64) *batch {
	b := &batch{slot: t, off: t.end}
	size := int64(batchSmall)
	if n > batchSmall/4 {
		size = batchCap
	}
	if header {
		b.off = 0
		size += dataStart
	}
	if n > size {
		size = n + dataStart
	}
	b.data = getBuffer(int(size))
	if header {
		// No mark is being written to t: it is the new tail, and no record has
		// reached a file yet to give a sync a reason to write one. So mu is
		// enough, and a placement never waits behind the write of a mark.
		b.data = appendPreamble(b.data, t.first, t.life, j.lineage, t.through, t.floor)
		j.pending += dataStart
	}
	j.queue = append(j.queue, b)
	j.cur = b
	return b
}

// Batches of small records get a small buffer: a journal that is opened, used
// a little and closed, as a crash exploration does thousands of times, must not
// allocate and clear four megabytes for each. Records over a quarter of it, and
// anything bigger, get the large one.
var (
	smallBuffers = sync.Pool{New: func() any { return make([]byte, 0, batchSmall+dataStart) }}
	buffers      = sync.Pool{New: func() any { return make([]byte, 0, batchCap+dataStart) }}
)

func getBuffer(size int) []byte {
	switch {
	case size <= batchSmall+dataStart:
		return smallBuffers.Get().([]byte)[:0]
	case size <= batchCap+dataStart:
		return buffers.Get().([]byte)[:0]
	}
	return make([]byte, 0, size)
}

func putBuffer(b []byte) {
	switch cap(b) {
	case batchSmall + dataStart:
		smallBuffers.Put(b[:0]) //nolint:staticcheck // a slice header per pooled buffer is cheaper than a boxed pointer here
	case batchCap + dataStart:
		buffers.Put(b[:0]) //nolint:staticcheck // as above
	}
}

// rotate makes the spare segment the tail. If the spare is still being made it
// changes nothing and returns a channel to wait on; if making it failed, which
// is a full disk more often than a broken one, the segment is given up and the
// caller gets ErrFull, not a dead journal. Caller holds mu.
func (j *Journal) rotate() (<-chan struct{}, error) {
	if j.spare == nil {
		j.refill()
	}
	s := j.spare
	if s == nil {
		return nil, ErrFull
	}
	select {
	case <-s.ready:
	default:
		return s.ready, nil
	}
	if s.err != nil {
		j.spare = nil
		j.nslots--
		return nil, fmt.Errorf("%w: making a segment: %v", ErrFull, s.err)
	}
	j.spare = nil
	var life [8]byte
	if _, err := rand.Read(life[:]); err != nil {
		return nil, err
	}
	s.life = binary.LittleEndian.Uint64(life[:])
	s.seed = seedOf(s.life, j.lineage)
	s.first, s.last, s.end = 0, 0, dataStart
	s.through, s.floor, s.marks = j.synced, j.published, 0 // see open: no mark is being written to s
	j.tail = s
	j.segs = append(j.segs, s)
	j.held += dataStart
	j.refill()
	return nil, nil
}

// refill makes sure a spare segment exists or is being created, if the
// capacity allows another file. Caller holds mu.
func (j *Journal) refill() {
	if j.spare != nil || j.err != nil || j.closed {
		return
	}
	if n := len(j.free); n > 0 {
		j.spare = j.free[n-1]
		j.free = j.free[:n-1]
		return
	}
	if j.nslots < j.limit {
		j.spare = j.create()
	}
}

// create starts a new segment file in the background. Append never waits for
// it unless it needs the segment, and then only for as long as it takes.
func (j *Journal) create() *slot {
	s := &slot{id: j.nextID, size: j.segment, ready: make(chan struct{})}
	s.name = slotName(s.id)
	j.all = append(j.all, s)
	j.nextID++
	j.nslots++
	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		s.file, s.err = j.disk.Create(s.name, s.size)
		close(s.ready)
	}()
	return s
}

// writeMark records in s what the journal knew: through is durable, floor is
// in the store. It writes nothing when s already says as much, and reports
// whether it wrote. What s says in memory changes only after the write has
// succeeded, so a write that failed is made again by the next call, into the
// same slot: the slot that holds the older mark, the other one standing.
func (j *Journal) writeMark(s *slot, through, floor uint64) (bool, error) {
	j.markmu.Lock()
	defer j.markmu.Unlock()
	through, floor = max(through, s.through), max(floor, s.floor)
	if through == s.through && floor == s.floor {
		return false, nil
	}
	m := appendMark(make([]byte, 0, markSize), s.seed, through, floor)
	if _, err := s.file.WriteAt(m, int64(headerSize)+int64((s.marks+1)&1)*markSize); err != nil {
		return false, err
	}
	s.through, s.floor = through, floor
	s.marks++
	return true, nil
}

// Sync returns once the record with sequence seq, and every record before it,
// is durable. Concurrent calls share one fsync. A call is released only by an
// fsync that began after the record was written, never by an earlier one.
func (j *Journal) Sync(ctx context.Context, seq uint64) error {
	j.mu.Lock()
	if seq <= j.synced {
		j.mu.Unlock()
		return nil
	}
	if err := j.fault(); err != nil {
		j.mu.Unlock()
		return err
	}
	if seq >= j.next {
		j.mu.Unlock()
		return ErrAhead
	}
	w := &waiter{seq: seq, done: make(chan struct{})}
	j.waiters = append(j.waiters, w)
	j.stats.Waited++
	j.mu.Unlock()
	j.kick(j.kickw)
	j.kick(j.kicks)

	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		j.mu.Lock()
		for i, x := range j.waiters {
			if x == w {
				j.waiters = append(j.waiters[:i], j.waiters[i+1:]...)
				break
			}
		}
		j.mu.Unlock()
		select {
		case <-w.done:
			return w.err
		default:
			return ctx.Err()
		}
	}
}

// Marks returns the watermarks.
func (j *Journal) Marks() Marks {
	j.mu.Lock()
	defer j.mu.Unlock()
	return Marks{Visible: j.next - 1, Durable: j.synced, Published: j.published}
}

// Publish records that the store holds every record up to seq. The call is
// monotonic: a lower seq than before is ignored. seq must be durable: the
// store never gets ahead of the journal's disk, which is what lets Open treat
// the store's sequence as the truth. It moves the Due deadline to the next
// record and does no I/O; the space is recycled by Reclaim.
func (j *Journal) Publish(seq uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if seq > j.synced {
		return ErrAhead
	}
	if seq <= j.published {
		return nil
	}
	j.published = seq
	j.trim()
	j.rearm()
	return nil
}

// trim drops spans of published records. Caller holds mu.
func (j *Journal) trim() {
	if j.published+1 >= j.next {
		j.head = len(j.spans)
	} else {
		for j.head+1 < len(j.spans) && j.spans[j.head+1].first <= j.published+1 {
			j.head++
		}
	}
	if j.head > 1024 && j.head > len(j.spans)/2 {
		j.spans = append(j.spans[:0], j.spans[j.head:]...)
		j.head = 0
	}
}

// oldest is the acknowledgement time of the oldest unpublished record, rounded
// down to the span it started. Caller holds mu.
func (j *Journal) oldest() (time.Time, bool) {
	if j.head == len(j.spans) {
		return time.Time{}, false
	}
	return j.spans[j.head].ack, true
}

// releaseLocked frees every waiter the durable watermark now covers and
// returns how many it freed. Caller holds mu.
func (j *Journal) releaseLocked() int {
	kept := j.waiters[:0]
	freed := 0
	for _, w := range j.waiters {
		if w.seq <= j.synced {
			close(w.done)
			freed++
		} else {
			kept = append(kept, w)
		}
	}
	clear(j.waiters[len(kept):])
	j.waiters = kept
	return freed
}

// State reports the backlog and whether the publication bound holds.
func (j *Journal) State() State {
	j.mu.Lock()
	defer j.mu.Unlock()
	s := State{Err: j.err}
	if ack, ok := j.oldest(); ok {
		s.Age = max(0, j.clock.Now().Sub(ack))
		s.Backlog = int64(j.total - j.spans[j.head].cum)
	}
	fill := float64(j.held) / float64(int64(j.limit)*j.segment)
	s.Degraded = j.err != nil || s.Age > j.bound || fill > slowFrom
	return s
}

// Stats returns the counters.
func (j *Journal) Stats() Stats {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.stats
}

// WSBufferStats returns cumulative Write Scrap Buffer telemetry across all
// segment files opened by this journal, or a zero value if WSBuffer is disabled.
func (j *Journal) WSBufferStats() WSBufferStats {
	if j.ws == nil {
		return WSBufferStats{}
	}
	return j.ws.Stats()
}

// Close makes everything appended durable, writes the final mark, stops the
// journal, closes its files and releases the directory. Records stay on disk
// for the next Open. Close must not run while Reclaim or Replay is running.
func (j *Journal) Close() error {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return nil
	}
	j.closed = true
	j.kick(j.kickw)
	j.kick(j.kicks)
	j.mu.Unlock()
	j.wg.Wait()

	j.mu.Lock()
	err := j.err
	last, through, floor := j.last, j.synced, j.published
	j.mu.Unlock()
	if err == nil && last != nil {
		// The mark that proves everything appended is durable: without it the
		// last group's records are indistinguishable from a torn tail.
		if wrote, werr := j.writeMark(last, through, floor); werr != nil {
			err = werr
		} else if wrote {
			err = last.file.Sync()
		}
	}

	j.mu.Lock()
	j.finish()
	for _, d := range append([]*due(nil), j.dues...) {
		j.fire(d)
	}
	for _, w := range j.waiters {
		w.err = ErrClosed
		close(w.done)
	}
	j.waiters = nil
	j.broadcast()
	j.mu.Unlock()
	if rerr := j.release(); err == nil {
		err = rerr
	}
	return err
}

// release closes every file and the disk. Called once the goroutines have
// stopped.
func (j *Journal) release() error {
	var first error
	for _, s := range j.all {
		if s.file != nil {
			if err := s.file.Close(); err != nil && first == nil {
				first = err
			}
			s.file = nil
		}
	}
	if j.disk != nil {
		if err := j.disk.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// bucket is the histogram index for a group of n waiters.
func bucket(n int) int { return min(bits.Len(uint(n)), 11) }
