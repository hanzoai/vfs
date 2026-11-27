package vfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

var (
	// ErrStaleRound is returned when the Round does not hold the Volume: a commit by
	// a Round below the recorded one (this writer was fenced, and changes it had not
	// committed will never land), or an Attach at a Round that does not exceed the
	// recorded one. A commit whose acknowledgement was lost may nevertheless be in
	// the successor's tree; see the package documentation.
	ErrStaleRound = fence.ErrStaleRound

	// ErrTooBig is returned for a file larger than MaxSize.
	ErrTooBig = errors.New("vfs: file too large")

	// ErrReadOnly is returned by a write on a Volume that was Opened, not Attached.
	ErrReadOnly = errors.New("vfs: volume is read-only")

	// ErrFull is returned by a change that cannot be accepted now: the node's journal is
	// full of changes the store does not have yet, or memory is, and the store is not taking
	// them. Nothing was changed. The mount answers it ENOSPC. The journal's own error is the
	// same value, so a caller tests for one.
	ErrFull = journal.ErrFull

	// ErrJournal is returned by every change after the node's journal has failed: no later
	// change can be made durable, so the Volume writes no more (the mount answers EROFS).
	// What was acknowledged before is on the disk, and in the store once published.
	ErrJournal = errors.New("vfs: the journal failed: the volume takes no more changes")

	// ErrClosed is returned by a change or a Flush of a Volume that was closed.
	ErrClosed = errors.New("vfs: volume is closed")

	ErrNotDir   = errors.New("vfs: not a directory")
	ErrIsDir    = errors.New("vfs: is a directory")
	ErrNotEmpty = errors.New("vfs: directory not empty")
)

// Config says where a Volume is, how it is sealed, and how much memory and disk it may use.
type Config struct {
	Backend backend.Backend // the Volume's prefix; the Volume owns it and Close closes it
	Cipher  Cipher          // seals and opens every object
	Round   uint64          // Attach, Reseal: the Round to claim; must exceed the recorded one, and be above 0
	Verify  bool            // Reseal: read every shard the Head reaches and check every pack it needs is there, not only the root

	Capacity int64 // bytes the Volume may hold: a write past it is refused with ErrNoSpace; 0 for no limit

	Cache   int64 // bytes of shards and chunks kept decoded; default 256 MiB
	Dirty   int64 // bytes of changes held in memory that the store does not have yet; past it a change waits for a publication; default 1 GiB
	Overlay int   // metadata changes held in memory that the store does not have yet, with the same effect; default 262144

	// Journal is a directory on this node's disk where Attach keeps the Volume's journal:
	// every change is appended to it before it is applied, so a call that returns from
	// Sync survives the process and the machine, and the store has the change within Bound.
	// It must not be on memory (the journal refuses it), and is required to Attach.
	Journal string
	// JournalCapacity is the most disk the journal uses, in bytes; default 4 GiB. When the
	// changes the store has not taken fill it, a change is refused with ErrFull; from three
	// quarters full a change is slowed.
	JournalCapacity int64
	// Bound is how long a change may wait to be published while the store answers;
	// default 5 s. The clock starts at the change's own acknowledgement and no later
	// change moves it. When the store does not answer it cannot be kept, and State says so.
	Bound time.Duration
	// Clock supplies the times of changes and the timers of the bound: nil is the wall clock.
	Clock journal.Clock
	// Disk makes the journal's file layer for a directory: nil is the operating system's files. A
	// caller wraps journal.Files to observe, count or hold what the journal does to its files.
	Disk func(dir string) (journal.Disk, error)

	// Budget bounds, across every Volume that is given it, the memory held for changes the store
	// does not have yet. Nil: each Volume is bounded by Dirty alone.
	Budget *Budget

	Workers int // chunks, hashes and seals in parallel; default the number of CPUs, at most 16
	Streams int // objects written to the store in parallel; default 8

	// PackClassifier optionally separates newly sealed chunks into lifetime-aware
	// open pack streams (0..2) during generation settlement (HIP-1344, DOGI).
	// Nil selects standard single-stream FIFO packing. Both modes write identical
	// format-v2 packs, shards and Heads.
	PackClassifier PackLifetimeClassifier
	// PackTargetBytes optionally overrides the open pack flush threshold (default
	// packTarget = 16 MiB) for pack-sizing sensitivity benchmarks and tests.
	PackTargetBytes int
}

func (c Config) valid() (Config, error) {
	if c.Backend == nil || c.Cipher == nil {
		return c, errors.New("vfs: config needs a Backend and a Cipher")
	}
	if c.Cache <= 0 {
		c.Cache = 256 << 20
	}
	if c.Dirty <= 0 {
		c.Dirty = 1 << 30
	}
	if c.Overlay <= 0 {
		c.Overlay = 1 << 18
	}
	if c.JournalCapacity <= 0 {
		c.JournalCapacity = 4 << 30
	}
	if c.Bound <= 0 {
		c.Bound = 5 * time.Second
	}
	if c.Workers <= 0 {
		c.Workers = min(runtime.GOMAXPROCS(0), 16)
	}
	if c.Streams <= 0 {
		c.Streams = 8
	}
	if err := check(c.Cipher); err != nil {
		return c, err
	}
	return c, nil
}

// maxHead bounds what a read accepts for the Head: it is a few hundred bytes
// sealed under a post-quantum key.
const maxHead = 64 << 10

// Volume is a mounted filesystem. All methods are safe for concurrent use.
// A Volume that was Attached may write; one that was Opened may only read.
//
// What a Volume does, in order, with a change: it is built from what the Volume holds
// in memory, sealed, appended to the node's journal, and applied; the call returns. The
// journal's sync, which a caller asks for with Sync, makes it durable on this node. A
// publisher, a goroutine of the Volume, takes what has been applied up to a point (a
// generation), turns it into chunks, packs and shards, writes them to the store in
// parallel, and commits the Head that names them and carries the journal's sequence; and
// it does so within Bound of the oldest change it holds. No call waits for it, except
// Flush, which asks for a publication and returns when the Head has it; and no call
// holds the Volume's lock across a request to the store or a call to the journal.
type Volume struct {
	be     backend.Backend
	c      Cipher
	fence  *fence.Store
	cfg    Config
	round  uint64 // 0 when read-only
	t      *tree
	record int // the most bytes of a write one journal record holds
	mem    *Budget
	packs  atomic.Int64 // bytes of packs made and not yet in the store

	chunks *lru[chunkID, []byte]
	fills  flights[chunkID, []byte] // one fetch for every caller that misses the same chunk
	work   *workers
	id     volumeID
	gear   *gear

	// wmu puts changes in order: the change that holds it builds, seals, appends and
	// applies, and no other does between. It may be held for as long as the journal makes
	// a writer wait (it is how a full journal slows every writer), and nothing that only
	// reads takes it.
	wmu  sync.Mutex
	tail uint64 // the journal sequence of the last change appended; wmu

	// mu guards everything below. It is held for moments, never across a request to the
	// store or a call to the journal.
	mu         sync.RWMutex
	head       Head   // the committed Head
	seq        uint64 // the last Seq this Volume wrote or read, landed or not
	root       ref    // the tree head names
	over       *overlay
	gen        *generation // the generation in flight: frozen, being published
	next       InodeID
	files      map[InodeID]*fileState
	dirtyFiles map[InodeID]*fileState // the files whose live layer holds a change
	gone       []InodeID              // files removed since the last generation was cut
	goneSet    map[InodeID]struct{}   // set of files removed since the last generation was cut
	dirty      int64
	logged     int64  // bytes of journal the changes since the last generation was cut took
	applied    uint64 // the journal sequence of the last change applied
	freed      chan struct{}
	pubErr     error // why the last attempt to publish failed, until one succeeds
	fenced     bool
	failed     error
	closed     bool
	reclaiming bool // a publication has finished and its journal space is being given back

	ns   counters // usage and links, written into every Head
	dirn dirCache // link counts of directories

	// mutating is true while mutate() is between build() and apply() (mu unlocked).
	// If a generation completes during that window, complete() records any newly settled
	// file data keys and byte deltas here so apply() reconciles drops atomically.
	mutating    bool
	settledDrop map[InodeID]settledInfo

	led      *ledger
	recovery Recovery

	reqmu sync.Mutex
	reqs  []*request
	kick  chan struct{}
	quit  chan struct{}
	done  chan struct{} // closed when the publisher has stopped
	base  context.Context
	stop  context.CancelFunc

	hintMu      sync.RWMutex
	pathHints   map[InodeID]string
	overwrites  map[InodeID]uint32
	rootMu      sync.Mutex
	rootCond    *sync.Cond
	activeRoots map[ref]int
}

func newVolume(cfg Config) *Volume {
	sh := &shards{
		be: cfg.Backend, c: cfg.Cipher,
		cache: newLRU[ref, *node](cfg.Cache / 2),
		read:  &flights[ref, *node]{},
	}
	v := &Volume{
		be:         cfg.Backend,
		c:          cfg.Cipher,
		fence:      fence.New(cfg.Backend, maxHead),
		cfg:        cfg,
		t:          newTree(sh, [32]byte{}),
		chunks:     newLRU[chunkID, []byte](cfg.Cache / 2),
		work:       newWorkers(cfg.Workers),
		over:       newOverlay(),
		files:      map[InodeID]*fileState{},
		dirtyFiles: map[InodeID]*fileState{},
		goneSet:    map[InodeID]struct{}{},
		freed:      make(chan struct{}),
		kick:       make(chan struct{}, 1),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	v.record = int(min(4<<20, v.segment()/2))
	v.mem = cfg.Budget
	v.base, v.stop = context.WithCancel(context.Background())
	return v
}

// adopt makes h the state of the Volume: what the Head names, and the identity
// everything else is sealed to.
func (v *Volume) adopt(h Head) {
	v.head, v.seq = h, h.Seq
	v.root, v.next = h.Root, InodeID(h.Next)
	v.applied, v.tail = h.Journal, h.Journal
	v.id, v.gear = h.ID, newGear(h.Seed)
	v.t = newTree(v.t.sh, h.Seed)
	v.t.sh.aad = shardAAD(h.ID)
}

// Create makes an empty Volume at the Backend's prefix: a tree holding the root
// directory, and a Head naming it, written only if no Head exists. It returns an
// error wrapping fs.ErrExist if the prefix already holds a Volume.
func Create(ctx context.Context, cfg Config) error {
	cfg, err := cfg.valid()
	if err != nil {
		return err
	}
	v := newVolume(cfg)
	h := Head{Next: uint64(RootInode) + 1}
	for _, b := range [][]byte{h.ID[:], h.Seed[:]} {
		if _, err := rand.Read(b); err != nil {
			return err
		}
	}
	v.adopt(h)
	now := v.Now()
	root := Inode{ID: RootInode, Mode: fs.ModeDir | 0o755, Mtime: now, Atime: now, Ctime: now}
	if h.Root, err = v.t.apply(ctx, none, []mut{{key: keyInode(RootInode), val: encodeInode(root)}}); err != nil {
		return err
	}
	sealed, err := seal(v.c, headAAD(cfg.Backend.String()), h.marshal())
	if err != nil {
		return err
	}
	if err := v.fence.Create(ctx, headKey, sealed); err != nil {
		if errors.Is(err, backend.ErrConflict) {
			return fmt.Errorf("vfs: %s already holds a volume: %w", cfg.Backend, fs.ErrExist)
		}
		return err
	}
	return nil
}

// Open returns a read-only Volume at the committed Head. It claims nothing and
// fences no one.
func Open(ctx context.Context, cfg Config) (*Volume, error) {
	cfg, err := cfg.valid()
	if err != nil {
		return nil, err
	}
	v := newVolume(cfg)
	payload, _, err := v.fence.Get(ctx, headKey)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("vfs: no volume at %s: %w", cfg.Backend, fs.ErrNotExist)
		}
		return nil, err
	}
	h, err := v.readHead(payload)
	if err != nil {
		return nil, err
	}
	v.adopt(h)
	if err := v.ns.load(h); err != nil {
		return nil, err
	}
	return v, nil
}

// Attach claims the Volume at cfg.Round and returns it ready to write. The claim
// is refused with ErrStaleRound unless cfg.Round exceeds the Round recorded; it
// seals the predecessor's last committed Head at the new Round, atomically, and
// opens the Volume at that Head, so nothing the predecessor committed is lost and
// nothing it tries to commit afterwards lands.
//
// What the predecessor acknowledged and did not commit is another matter. If it ran on
// this node and left its journal here, Attach replays the journal, publishes what it
// held under the new Round, and goes on. If it did not (another node took over, or the
// disk was lost), that tail is not here and nothing can say how much there was: Recovery
// reports Lost, and the Volume is what the last Head says. The same is reported when the
// journal is here and cannot be used (it belongs to another history, or is damaged), and
// the files are left as they are for an operator.
//
// After any error from Attach other than a refusal for a Round that is too low,
// the claim may or may not have landed (a reply can be lost). A caller that retries
// MUST use a higher Round: a retry at the same Round is told ErrStaleRound, since
// the Round is by then recorded, and only a claim whose own retry finds its own
// bytes within one call is recognised as landed.
func Attach(ctx context.Context, cfg Config) (*Volume, error) {
	cfg, err := cfg.valid()
	if err != nil {
		return nil, err
	}
	if cfg.Round == 0 || cfg.Round == math.MaxUint64 {
		return nil, fmt.Errorf("%w: Attach needs a Round above 0 and below 2^64-1, since no Round can follow the last", fs.ErrInvalid)
	}
	if cfg.Journal == "" {
		return nil, fmt.Errorf("%w: Attach needs Config.Journal, a directory on this node's disk for the journal", fs.ErrInvalid)
	}
	v := newVolume(cfg)
	var h Head
	var recorded uint64
	err = v.fence.Claim(ctx, headKey, cfg.Round, func(payload []byte, rec uint64) error {
		if payload == nil {
			return fmt.Errorf("vfs: no volume at %s: %w", cfg.Backend, fs.ErrNotExist)
		}
		if cfg.Round <= rec {
			return fmt.Errorf("%w: round %d does not exceed the recorded %d", ErrStaleRound, cfg.Round, rec)
		}
		var err error
		h, err = v.readHead(payload)
		recorded = rec
		return err
	})
	if err != nil {
		return nil, err
	}
	v.adopt(h)
	if err := v.ns.load(h); err != nil {
		return nil, err
	}
	v.round = cfg.Round
	if err := v.mount(ctx, recorded); err != nil {
		v.stop()
		return nil, err
	}
	return v, nil
}

// Stats is a snapshot of a Volume for observability.
type Stats struct {
	Backend    string
	Round      uint64 // 0 when read-only
	Seq        uint64 // the Seq of the committed Head: it counts commit attempts and can skip, so it is not a count of commits
	Inodes     uint64 // inode numbers handed out
	CacheBytes int64
	CacheMax   int64
	Pending    int // metadata changes the store does not have yet
	Dirty      int64
}

func (v *Volume) Stats() Stats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	cs, cm := v.chunks.stats()
	ss, sm := v.t.sh.cache.stats()
	pending := v.over.n
	if v.gen != nil {
		pending += v.gen.over.n
	}
	return Stats{
		Backend: v.be.String(), Round: v.round, Seq: v.head.Seq, Inodes: uint64(v.next) - 1,
		CacheBytes: cs + ss, CacheMax: cm + sm, Pending: pending, Dirty: v.dirty,
	}
}

// Close stops the publisher and closes the journal and the backend. It does not publish:
// what was changed and not flushed stays in the journal on this node, and the next
// Attach of the Volume on it publishes that (a Volume that must hand over what it has
// calls Flush first). A journal with nothing left in it is removed.
func (v *Volume) Close() error {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil
	}
	v.closed = true
	v.mu.Unlock()
	v.stop()     // a change that waits for memory or for the journal's space is let go
	v.wmu.Lock() // the change in progress finishes
	v.wmu.Unlock()
	defer func() {
		v.mu.Lock()
		v.account(-v.dirty - v.packs.Swap(0))
		v.mu.Unlock()
	}()
	var err error
	if v.led != nil {
		close(v.quit)
		<-v.done
		err = v.led.j.Close()
		v.mu.RLock()
		clean := v.head.Journal == v.applied
		v.mu.RUnlock()
		if err == nil && clean {
			err = v.led.retire()
		}
	}
	return errors.Join(err, v.be.Close())
}

// writable reports why v cannot write, if it cannot. mu must be held.
func (v *Volume) writable() error {
	switch {
	case v.round == 0:
		return ErrReadOnly
	case v.closed:
		return ErrClosed
	case v.fenced:
		return ErrStaleRound
	case v.failed != nil:
		return fmt.Errorf("%w: %v", ErrJournal, v.failed)
	}
	return nil
}

// View runs fn with the Volume's state held for reading, on a context that has no
// store requests in it: a read of a shard that is not in memory fails with *missing, the
// lock is let go, the shard is fetched, and fn runs again. A caller that only looks
// never keeps another from changing the Volume while the store answers.
func (v *Volume) view(ctx context.Context, fn func(context.Context) error) error {
	for i := 0; ; i++ {
		v.mu.RLock()
		err := fn(v.patience(ctx, i))
		v.mu.RUnlock()
		r, ok := absent(err)
		if !ok {
			return err
		}
		if _, err := v.t.sh.load(ctx, r); err != nil {
			return err
		}
	}
}

// edit is view for a caller that changes what the Volume holds in memory (a file's state
// is loaded when it is first opened).
func (v *Volume) edit(ctx context.Context, fn func(context.Context) error) error {
	for i := 0; ; i++ {
		v.mu.Lock()
		err := fn(v.patience(ctx, i))
		v.mu.Unlock()
		r, ok := absent(err)
		if !ok {
			return err
		}
		if _, err := v.t.sh.load(ctx, r); err != nil {
			return err
		}
	}
}

// patience is how many times a call is repeated after loading what it lacked before it
// reads under the lock whatever it must: a cache so small that a path through the tree
// does not fit would otherwise never end.
const patience = 64

func (v *Volume) patience(ctx context.Context, i int) context.Context {
	if i >= patience {
		return ctx
	}
	return local(ctx)
}

type settledInfo struct {
	keys  [][]byte // tData keys added to v.root by a generation completed mid-mutate
	delta int64    // net change (newBytes - oldBytes) applied to v.ns.bytes
}

// mutate makes one change. build reads the Volume (from memory: it is given a local
// context) and returns the change; the change is sealed, appended to the journal, and
// applied, with no other change in between, and the journal's sequence of it is returned.
// If build returns (nil, nil), nothing is appended to the journal (a no-op, or an
// in-memory-only orphan mutation). If the journal refuses the change nothing has changed.
// A change that needs a shard the cache lacks is built again after the shard is loaded,
// with no lock held while it is.
func (v *Volume) mutate(ctx context.Context, build func(context.Context) (*change, error)) (uint64, error) {
	for i := 0; ; i++ {
		if err := v.admit(ctx); err != nil {
			return 0, err
		}
		v.wmu.Lock()
		v.mu.Lock()
		var c *change
		err := v.writable()
		if err == nil {
			c, err = build(v.patience(ctx, i))
		}
		if err == nil && c != nil {
			v.mutating = true
			v.settledDrop = nil
		}
		v.mu.Unlock()
		if r, ok := absent(err); ok {
			v.wmu.Unlock()
			if _, err := v.t.sh.load(ctx, r); err != nil {
				return 0, err
			}
			continue
		}
		if err != nil || c == nil {
			v.wmu.Unlock()
			return 0, err
		}
		seq, logged, err := v.appendWithBackpressure(ctx, c)
		if err != nil {
			v.mu.Lock()
			v.mutating = false
			v.settledDrop = nil
			v.mu.Unlock()
			v.wmu.Unlock()
			return 0, err
		}
		v.mu.Lock()
		v.apply(c, seq)
		v.mutating = false
		v.settledDrop = nil
		v.logged += logged
		early := v.gen == nil && v.pressure()
		v.mu.Unlock()
		v.wmu.Unlock()
		if early {
			v.wake()
		}
		return seq, nil
	}
}

// appendWithBackpressure appends c to the journal, waiting for a progressing publication
// if the journal is temporarily full, and releases c.sealed back to the pool. wmu is held.
func (v *Volume) appendWithBackpressure(ctx context.Context, c *change) (uint64, int64, error) {
	seq, err := v.append(ctx, c)
	for errors.Is(err, journal.ErrFull) && v.draining() {
		// The journal is full of changes a publication is taking: the writer is slowed to
		// the speed of the drain, not refused, until the drain stops making progress.
		wait := v.waitDrained()
		select {
		case <-wait:
		case <-ctx.Done():
			err = ctx.Err()
		case <-v.base.Done():
			err = ErrClosed
		}
		if err == nil || errors.Is(err, journal.ErrFull) {
			seq, err = v.append(ctx, c)
		} else {
			break
		}
	}
	logged := int64(len(c.sealed))
	if c.sealed != nil {
		giveBuffer(c.sealed) // the journal has copied it, or does not take the change
		c.sealed = nil
	}
	return seq, logged, err
}

// mutateWrite performs a File.WriteAt as an atomic batch with respect to concurrent
// readers and generation cuts: all journal pieces (each <= v.record) are sealed before
// taking wmu, appended to the journal under wmu, and applied together under one mu.Lock()
// so no generation cut or reader ever sees a half-written WriteAt. Only if a single
// WriteAt exceeds the entire empty journal capacity does it flush its prefix to avoid
// deadlock.
func (v *Volume) mutateWrite(ctx context.Context, f *fileState, off uint64, p []byte) (int, error) {
	when := v.Now()
	// Fast path for orphans: memory-only, no journal sealing or append.
	v.mu.RLock()
	orphan := f.orphan
	v.mu.RUnlock()
	if orphan {
		v.mu.Lock()
		defer v.mu.Unlock()
		if err := v.writable(); err != nil {
			return 0, err
		}
		if f.gone {
			return 0, fs.ErrNotExist
		}
		if end := off + uint64(len(p)); v.cfg.Capacity > 0 && end > f.size {
			if grow := end - max(off, f.size); v.used()+int64(grow) > v.cfg.Capacity {
				return 0, fmt.Errorf("%w: capacity %d", ErrNoSpace, v.cfg.Capacity)
			}
		}
		v.write(f, p, off)
		f.size = max(f.size, off+uint64(len(p)))
		f.mtime, f.ctime = when, when
		return len(p), nil
	}

	// Build and seal all pieces before acquiring wmu so sealing does not serialize writers.
	nPieces := (len(p) + v.record - 1) / v.record
	cs := make([]*change, 0, nPieces)
	defer func() {
		for _, c := range cs {
			if c.sealed != nil {
				giveBuffer(c.sealed)
				c.sealed = nil
			}
		}
	}()
	curOff := off
	for rem := p; len(rem) > 0; {
		piece := rem[:min(len(rem), v.record)]
		c := &change{kind: changeWrite, ino: f.ino, file: f, off: curOff, data: piece, when: when, ctime: when}
		sealed, err := v.sealChange(c)
		if err != nil {
			return 0, err
		}
		c.sealed = sealed
		cs = append(cs, c)
		rem = rem[len(piece):]
		curOff += uint64(len(piece))
	}

	if err := v.admit(ctx); err != nil {
		return 0, err
	}
	v.wmu.Lock()
	defer v.wmu.Unlock()

	// Check writability, capacity, and orphan status under mu.
	v.mu.Lock()
	if err := v.writable(); err != nil {
		v.mu.Unlock()
		return 0, err
	}
	if f.gone {
		v.mu.Unlock()
		return 0, fs.ErrNotExist
	}
	if end := off + uint64(len(p)); v.cfg.Capacity > 0 && end > f.size {
		if grow := end - max(off, f.size); v.used()+int64(grow) > v.cfg.Capacity {
			v.mu.Unlock()
			return 0, fmt.Errorf("%w: capacity %d", ErrNoSpace, v.cfg.Capacity)
		}
	}
	if f.orphan {
		v.write(f, p, off)
		f.size = max(f.size, off+uint64(len(p)))
		f.mtime, f.ctime = when, when
		v.mu.Unlock()
		return len(p), nil
	}
	v.mu.Unlock()

	seqs := make([]uint64, len(cs))
	logs := make([]int64, len(cs))
	appliedFrom := 0
	doneBytes := 0

	applyRange := func(lo, hi int) bool {
		if lo >= hi {
			return false
		}
		v.mu.Lock()
		for k := lo; k < hi; k++ {
			v.apply(cs[k], seqs[k])
			v.logged += logs[k]
			doneBytes += len(cs[k].data)
		}
		early := v.gen == nil && v.pressure()
		v.mu.Unlock()
		return early
	}

	for i, c := range cs {
		seq, err := v.append(ctx, c)
		for errors.Is(err, journal.ErrFull) {
			// If our own unapplied pieces filled an otherwise empty journal (a single write
			// larger than the journal), apply them now so the publisher can drain them.
			if !v.draining() && i > appliedFrom {
				v.mu.RLock()
				healthy := v.pubErr == nil
				v.mu.RUnlock()
				if healthy {
					applyRange(appliedFrom, i)
					appliedFrom = i
				}
			}
			if !v.draining() {
				break
			}
			wait := v.waitDrained()
			select {
			case <-wait:
			case <-ctx.Done():
				err = ctx.Err()
			case <-v.base.Done():
				err = ErrClosed
			}
			if err == nil || errors.Is(err, journal.ErrFull) {
				seq, err = v.append(ctx, c)
			} else {
				break
			}
		}
		logged := int64(len(c.sealed))
		if c.sealed != nil {
			giveBuffer(c.sealed)
			c.sealed = nil
		}
		if err != nil {
			// Apply any pieces already appended to the journal so memory matches the log.
			if applyRange(appliedFrom, i) {
				v.wake()
			}
			return doneBytes, err
		}
		seqs[i], logs[i] = seq, logged
	}

	if applyRange(appliedFrom, len(cs)) {
		v.wake()
	}
	return doneBytes, nil
}

// sealChange seals a change for the journal.
func (v *Volume) sealChange(c *change) ([]byte, error) {
	if v.led == nil {
		return nil, ErrReadOnly
	}
	return v.led.sealChange(c)
}

// append puts the change in the journal, sealed. wmu is held, which is what makes the
// journal's sequence the order in which changes are applied.
func (v *Volume) append(ctx context.Context, c *change) (uint64, error) {
	sealed := c.sealed
	if sealed == nil {
		var err error
		if sealed, err = v.sealChange(c); err != nil {
			return 0, err
		}
		c.sealed = sealed
	}
	seq, err := v.led.j.Append(ctx, sealed)
	if err != nil {
		if st := v.led.j.State(); st.Err != nil {
			v.mu.Lock()
			v.failed = st.Err
			v.mu.Unlock()
			return 0, fmt.Errorf("%w: %v", ErrJournal, st.Err)
		}
		return 0, err
	}
	if seq != v.tail+1 {
		return 0, fmt.Errorf("%w: the journal numbered a change %d after %d", ErrCorrupt, seq, v.tail)
	}
	v.tail = seq
	return seq, nil
}

// apply makes the change what the Volume is. Callers make a change by building it,
// appending it to the journal and applying it, and a mount that replays the journal
// applies what it reads: this is the one definition of what a change does. mu is held.
func (v *Volume) apply(c *change, seq uint64) {
	switch c.kind {
	case changeWrite:
		f := c.file
		if cur := v.files[c.ino]; cur != nil {
			f = cur
		} else if f != nil {
			v.files[c.ino] = f
		}
		v.write(f, c.data, c.off)
		f.size = max(f.size, c.off+uint64(len(c.data)))
		f.mtime, f.ctime = c.when, c.when
		v.touch(f, seq)
	case changeSize:
		f := c.file
		if cur := v.files[c.ino]; cur != nil {
			f = cur
		} else if f != nil {
			v.files[c.ino] = f
		}
		ct := c.ctime
		if ct.IsZero() {
			ct = c.when
		}
		v.truncate(f, c.size, c.when, ct)
		for _, m := range c.muts {
			if m.del {
				v.over.del(m.key)
			} else {
				v.over.put(m.key, m.val)
				if len(m.key) == prefixLen && m.key[0] == tInode {
					if in, err := decodeInode(inoOf(m.key), m.val); err == nil {
						if fs := v.files[in.ID]; fs != nil {
							fs.mtime, fs.ctime = in.Mtime, in.Ctime
						}
					}
				}
			}
		}
		v.touch(f, seq)
	case changePunch:
		f := c.file
		if cur := v.files[c.ino]; cur != nil {
			f = cur
		} else if f != nil {
			v.files[c.ino] = f
		}
		v.punch(f, c.off, c.size, c.when)
		v.touch(f, seq)
	case changeTree:
		for _, m := range c.muts {
			if m.del {
				v.over.del(m.key)
			} else {
				v.over.put(m.key, m.val)
				if len(m.key) == prefixLen && m.key[0] == tInode {
					if in, err := decodeInode(inoOf(m.key), m.val); err == nil {
						if fs := v.files[in.ID]; fs != nil {
							fs.mtime, fs.ctime = in.Mtime, in.Ctime
						}
					}
				}
			}
		}
		dBytes := c.dBytes
		for _, id := range c.gone {
			if info, ok := v.settledDrop[id]; ok {
				for _, k := range info.keys {
					v.over.del(k)
				}
				dBytes -= info.delta
				delete(v.settledDrop, id)
			}
		}
		if c.dFiles < 0 {
			v.ns.files -= min(v.ns.files, uint64(-c.dFiles))
		} else {
			v.ns.files += uint64(c.dFiles)
		}
		if dBytes < 0 {
			v.ns.bytes -= min(v.ns.bytes, uint64(-dBytes))
		} else {
			v.ns.bytes += uint64(dBytes)
		}
		if c.dLinked < 0 {
			v.ns.linked -= min(v.ns.linked, uint64(-c.dLinked))
		} else {
			v.ns.linked += uint64(c.dLinked)
		}
		if c.setLinks {
			v.ns.links = true
		}
		for _, b := range c.dirBumps {
			v.dirn.bump(b.dir, int(b.delta))
		}
		for _, id := range c.gone {
			if f := v.files[id]; f != nil && f.refs > 0 {
				// Live unlinked file held by open handles: becomes an in-memory orphan.
				f.orphan = true
				if in, ok := c.orphans[id]; ok {
					f.attr = in
					f.ctime = in.Ctime
				}
				delete(v.dirtyFiles, id)
				v.gone = append(v.gone, id)
				v.goneSet[id] = struct{}{}
				continue
			}
			v.forget(id)
		}
		v.next = max(v.next, c.next)
	}
	v.applied = seq
}

// touch records that a file's live layer holds a change as of seq.
func (v *Volume) touch(f *fileState, seq uint64) {
	v.files[f.ino] = f
	f.dep, f.changed = seq, true
	v.dirtyFiles[f.ino] = f
	v.recordOverwriteHint(f)
}

// forget drops a removed file's state: nothing of it is held for a handle, and its data
// records go with the next generation.
func (v *Volume) forget(id InodeID) {
	if f := v.files[id]; f != nil {
		v.account(-f.lv.dirtyN)
		f.gone = true
		delete(v.files, id)
	}
	delete(v.dirtyFiles, id)
	v.gone = append(v.gone, id)
	v.goneSet[id] = struct{}{}
}

// admit holds a change back while the Volume holds more that the store does not have
// than its bound, until a publication frees memory. If the publication is failing there
// is nothing to wait for: the change is refused with ErrFull, as one that the journal
// has no room for is.
func (v *Volume) admit(ctx context.Context) error {
	for {
		v.mu.RLock()
		pending := v.over.n
		if v.gen != nil {
			pending += v.gen.over.n
		}
		full := v.dirty >= v.cfg.Dirty || pending >= v.cfg.Overlay
		spent, shared := v.mem.full()
		full = full || (spent && v.dirty > 0) // a Volume holding nothing is always let in
		freed, why := v.freed, v.pubErr
		v.mu.RUnlock()
		if !full {
			return nil
		}
		if why != nil {
			return fmt.Errorf("%w: memory holds %d bytes the store does not have, and publishing them fails: %v", ErrFull, v.cfg.Dirty, why)
		}
		v.wake()
		select {
		case <-freed:
		case <-shared:
		case <-ctx.Done():
			return ctx.Err()
		case <-v.base.Done():
			return ErrClosed
		}
	}
}

// account changes what the Volume holds that the store does not have, here and in its Budget.
// mu is held.
func (v *Volume) account(n int64) {
	v.dirty += n
	v.mem.add(n)
}

// draining reports whether a publication that is not failing is, or soon will be, freeing
// the journal: a change that finds the journal full waits for it. A journal full of changes
// that nothing is publishing, or whose publication fails, refuses the change.
func (v *Volume) draining() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.pubErr == nil && (v.gen != nil || v.reclaiming || v.applied > v.head.Journal)
}

// waitDrained wakes the publisher and returns the channel closed by the next publication
// that frees journal space, or fails.
func (v *Volume) waitDrained() <-chan struct{} {
	v.mu.RLock()
	ch := v.freed
	v.mu.RUnlock()
	v.wake()
	return ch
}

// pressure reports whether enough is waiting for the store to be worth publishing before
// the bound says it must be: an eighth of what memory may hold, or a quarter of the journal.
// mu is held.
func (v *Volume) pressure() bool {
	return v.dirty >= v.cfg.Dirty/8 || v.over.n >= v.cfg.Overlay/8 || v.logged >= v.cfg.JournalCapacity/4
}

// syncTo makes the journal's records up to seq durable on this node.
func (v *Volume) syncTo(ctx context.Context, seq uint64) error {
	if seq == 0 || v.led == nil {
		return nil
	}
	if err := v.led.j.Sync(ctx, seq); err != nil {
		if st := v.led.j.State(); st.Err != nil {
			return fmt.Errorf("%w: %v", ErrJournal, st.Err)
		}
		return err
	}
	return nil
}

// Sync makes every change made before the call durable in this node's journal: it
// survives a crash of the process and of the machine. The store has them within Bound,
// or when Flush returns; a node lost for good loses what the store has not taken.
func (v *Volume) Sync(ctx context.Context) error {
	v.mu.RLock()
	seq := v.applied
	v.mu.RUnlock()
	return v.syncTo(ctx, seq)
}

// SyncInode makes every change the file with inode ino depends on durable in this node's
// journal, as File.Sync does for a handle: for a directory, or a file nobody holds open,
// everything changed before the call.
func (v *Volume) SyncInode(ctx context.Context, ino InodeID) error {
	v.mu.RLock()
	seq := v.applied
	if f := v.files[ino]; f != nil {
		if f.orphan {
			v.mu.RUnlock()
			return nil
		}
		seq = f.dep
	}
	v.mu.RUnlock()
	return v.syncTo(ctx, seq)
}

// Flush makes everything changed before the call durable in the store: it returns after
// a Head that carries it has been committed. If it returns an error the outcome of the
// attempt it asked for is unknown to the caller (a reply can be lost after the write
// landed), the changes are still held and still in the journal, and the next publication,
// the Volume's own or a Flush, makes the next attempt. ErrStaleRound means this Volume
// was fenced and can never commit again; see the package documentation for what that
// does and does not say about earlier attempts.
func (v *Volume) Flush(ctx context.Context) error {
	v.mu.RLock()
	err := v.writable()
	want, published := v.applied, v.head.Journal
	v.mu.RUnlock()
	if err != nil {
		return err
	}
	if want == published {
		return nil // the Head already names everything
	}
	return v.publishThrough(ctx, want)
}

// State is what an operator or an agent reads to learn whether the Volume's promise, that
// the store has every change within Bound, can be trusted. It is the journal's state.
type State struct {
	// Degraded is true when the promise cannot be trusted: the oldest change the store
	// does not have is older than the bound, the journal is slowing changes because it is
	// nearly full, or it has failed.
	Degraded bool
	// Age is how long the oldest change the store does not have has been acknowledged.
	Age time.Duration
	// Backlog is the bytes of journal that the store does not have.
	Backlog int64
	// Err is why the journal stopped, if it did.
	Err error
	// Publishing is why the publisher's last attempt failed, until one succeeds.
	Publishing error
	// Visible, Durable and Published are the journal's three watermarks: the last change
	// applied, the last made durable on this node, and the last the store holds.
	Visible, Durable, Published uint64
}

// State returns the state of the Volume's journal. A Volume that was only opened has none.
func (v *Volume) State() State {
	if v.led == nil {
		return State{}
	}
	st, m := v.led.j.State(), v.led.j.Marks()
	v.mu.RLock()
	failing := v.pubErr
	v.mu.RUnlock()
	return State{Degraded: st.Degraded || failing != nil, Age: st.Age, Backlog: st.Backlog, Err: st.Err, Publishing: failing, Visible: m.Visible, Durable: m.Durable, Published: m.Published}
}

// Recovery is what Attach found of the journal of the Round that owned the Volume before.
type Recovery struct {
	// Replayed is how many changes were read back from this node's journal, applied and
	// published.
	Replayed uint64
	// Lost is true when changes the previous owner acknowledged and did not publish may be
	// missing: there is no journal of that Round on this node, or there is and it could not
	// be used. The Volume is what the last Head says; Why says which.
	Lost bool
	Why  string
	// Report is what the journal's own recovery kept and gave up. Dropped lists records
	// that were intact and had to be given up; Warnings name risks of the disk.
	Report journal.Report
}

// Recovery reports what Attach found.
func (v *Volume) Recovery() Recovery { return v.recovery }

// get returns the current value of a record: a pending change, else the tree's. mu is held.
func (v *Volume) get(ctx context.Context, k []byte) ([]byte, bool, error) {
	if n, ok := v.over.get(k); ok {
		return n.val, !n.del, nil
	}
	if g := v.gen; g != nil {
		if n, ok := g.over.get(k); ok {
			return n.val, !n.del, nil
		}
	}
	return v.t.get(ctx, v.root, k)
}

// scan returns the current records whose key starts with prefix, in key order.
func (v *Volume) scan(ctx context.Context, prefix []byte) ([]entry, error) {
	var base []entry
	if err := v.t.scan(ctx, v.root, prefix, func(k, val []byte) bool {
		base = append(base, entry{k, val})
		return true
	}); err != nil {
		return nil, err
	}
	if g := v.gen; g != nil {
		base = laid(base, g.over, prefix)
	}
	return laid(base, v.over, prefix), nil
}

// laid returns base with the pending changes of o to the records under prefix applied.
func laid(base []entry, o *overlay, prefix []byte) []entry {
	out := make([]entry, 0, len(base))
	i := 0
	for n := o.seek(prefix); n != nil && bytes.HasPrefix(n.key, prefix); n = n.next[0] {
		for i < len(base) && bytes.Compare(base[i].key, n.key) < 0 {
			out = append(out, base[i])
			i++
		}
		if i < len(base) && bytes.Equal(base[i].key, n.key) {
			i++
		}
		if !n.del {
			out = append(out, entry{n.key, n.val})
		}
	}
	return append(out, base[i:]...)
}

// has reports whether any record whose key starts with prefix exists, reading no
// more than it takes to find one: a directory is checked for emptiness without
// listing it.
func (v *Volume) has(ctx context.Context, prefix []byte) (bool, error) {
	overlays := []*overlay{v.over}
	if v.gen != nil {
		overlays = append(overlays, v.gen.over)
	}
	for i, o := range overlays {
		for n := o.seek(prefix); n != nil && bytes.HasPrefix(n.key, prefix); n = n.next[0] {
			if n.del {
				continue
			}
			if i > 0 {
				if m, ok := v.over.get(n.key); ok && m.del {
					continue // removed since
				}
			}
			return true, nil
		}
	}
	found := false
	err := v.t.scan(ctx, v.root, prefix, func(k, _ []byte) bool {
		for _, o := range overlays {
			if n, ok := o.get(k); ok && n.del {
				return true // removed by a pending change
			}
		}
		found = true
		return false
	})
	return found, err
}

// mount finishes an Attach: what the previous owner left on this node is replayed and
// published, and this Round's journal starts.
func (v *Volume) mount(ctx context.Context, recorded uint64) error {
	if recorded > 0 {
		if err := v.replayRound(ctx, recorded); err != nil {
			return err
		}
	}
	l, err := v.startLedger(v.round, v.head.Journal)
	if err != nil {
		return err
	}
	v.led = l
	go v.publisher()
	return nil
}

// replayRound reads back the journal of the Round that owned the Volume before this one,
// if this node has it, and publishes it under this Round.
func (v *Volume) replayRound(ctx context.Context, recorded uint64) error {
	at := journalPlaces(v.cfg.Journal, v.id, recorded)
	if _, err := os.Stat(at.end); err == nil {
		return removeAll(at) // it ended with everything published
	}
	if _, err := os.Stat(at.dir); err != nil {
		v.recovery.Lost = true
		v.recovery.Why = fmt.Sprintf("this node has no journal of round %d, which owned the volume last: changes it acknowledged and had not published are not here, and are lost if its node is gone", recorded)
		return nil
	}
	l, err := v.openLedger(recorded, v.head.Journal)
	switch {
	case errors.Is(err, journal.ErrLocked):
		// The owner before this one is still running on this node, and its journal is its
		// own: this claim has fenced it, so nothing it holds will ever be published.
		v.recovery.Lost = true
		v.recovery.Why = fmt.Sprintf("the owner of round %d is still running on this node and holds its journal: what it acknowledged and had not published is lost to it, now fenced", recorded)
		return nil
	case err != nil:
		v.recovery.Lost = true
		v.recovery.Why = fmt.Sprintf("the journal of round %d cannot be used, and is left as it is: %v", recorded, err)
		return nil
	}
	v.recovery.Report = l.j.Recovery()
	if len(v.recovery.Report.Dropped) > 0 {
		v.recovery.Lost = true
		v.recovery.Why = fmt.Sprintf("the journal of round %d lost records it could not keep: %v", recorded, v.recovery.Report.Dropped)
	}
	err = l.j.Replay(func(r journal.Record) error {
		plain, err := l.open(r.Data)
		if err != nil {
			return err
		}
		c, err := decodeChange(plain)
		if err != nil {
			return err
		}
		if err := v.replay(ctx, &c, r.Seq); err != nil {
			return err
		}
		v.recovery.Replayed++
		return nil
	})
	if err != nil {
		l.j.Close()
		return fmt.Errorf("vfs: replay the journal of round %d: %w", recorded, err)
	}
	if v.applied > v.head.Journal {
		v.led = l
		err = v.publishNow(ctx)
		v.led = nil
		if err != nil {
			l.j.Close()
			return fmt.Errorf("vfs: publish what the journal of round %d held: %w", recorded, err)
		}
	}
	if err := l.j.Close(); err != nil {
		return err
	}
	return removeAll(at)
}

func removeAll(at places) error {
	for _, name := range []string{at.dir, at.key, at.end} {
		if err := os.RemoveAll(name); err != nil {
			return err
		}
	}
	return nil
}

// replay applies a change read back from the journal. The state it needs is loaded
// from the tree first: there is no one to wait for the store.
func (v *Volume) replay(ctx context.Context, c *change, seq uint64) error {
	if seq != v.applied+1 {
		return fmt.Errorf("%w: the journal gave change %d after %d", ErrCorrupt, seq, v.applied)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	switch c.kind {
	case changeWrite, changeSize, changePunch:
		f, err := v.state(ctx, c.ino)
		if err != nil {
			return fmt.Errorf("change %d is for inode %d: %w", seq, c.ino, err)
		}
		c.file = f
	case changeTree:
		// If a generation settled a file right before a crash, the file's data records
		// in the committed Head may differ from those staged when the drop was logged.
		// Recompute the actual data deletions and bytes held from the current tree so
		// usage and tData stay exact.
		if len(c.gone) > 0 {
			var actualHeld uint64
			for _, id := range c.gone {
				data, err := v.scan(ctx, key(tData, uint64(id)))
				if err != nil {
					return err
				}
				for _, r := range data {
					if len(r.key) == prefixLen {
						actualHeld += uint64(len(r.val))
					} else {
						e, err := decodeExtent(offsetOf(r.key), r.val)
						if err != nil {
							return err
						}
						actualHeld += uint64(e.n)
					}
					c.muts = append(c.muts, mut{key: bytes.Clone(r.key), del: true})
				}
			}
			c.dBytes = -int64(actualHeld)
		}
	}
	v.apply(c, seq)
	v.tail = seq
	return nil
}

// Reseal rewrites the Head of the Volume at cfg.Backend so that it is sealed to
// that place, and claims the Volume there at cfg.Round, which must exceed the
// recorded Round and be above 0 and below 2^64-1. A Head is bound to its place, and
// every other object to the Volume, so moving a Volume to another bucket or prefix
// is: copy every object, then Reseal. It fences the copy only; a writer still
// attached at the old place is not told, so stop it first (a writer that commits
// without pause can also make Reseal lose the race for the Head, which is then
// retried at a higher Round).
//
// from is the place the Head is sealed to now: the String of the Backend it was last
// written through. If the Head is already sealed to cfg.Backend, a Reseal whose
// reply was lost for instance, it is left as it is and only the Round is claimed, so
// a Reseal can always be repeated at a higher Round.
//
// from may be empty when the old place is not remembered. The Head is then opened
// with the key alone, whatever place it was sealed to. That trusts the key (only a
// holder can have written a Head that opens) and that the Head at cfg.Backend is the
// newest Head of the Volume the caller means: it cannot tell a Head from before the
// newest commit from the newest, as no one can without the place.
//
// However the Head was opened, the copy is checked before anything is written: the root
// shard the Head names must open at cfg.Backend sealed to the Volume the Head names and
// hold the root directory, which refuses a copy that lacks its objects, a Head of another
// Volume, and an object of another kind that a store writer put where the Head goes.
// cfg.Verify reads every shard the Head reaches and checks that every pack a chunk is in
// is there and long enough; chunks are checked when a file is read, and a pack whose bytes
// changed under a chunk no file has read since passes this check. Verify (integrity.go)
// reads every chunk and reports what it finds, before a Reseal is asked to bless a copy.
func Reseal(ctx context.Context, cfg Config, from string) error {
	cfg, err := cfg.valid()
	if err != nil {
		return err
	}
	if cfg.Round == 0 || cfg.Round == math.MaxUint64 {
		return fmt.Errorf("%w: Reseal needs a Round above 0 and below 2^64-1", fs.ErrInvalid)
	}
	v := newVolume(cfg)
	here := cfg.Backend.String()
	w := newVolume(cfg) // reads the copy: one cache for every attempt, which find mostly the same shards
	return v.fence.Rewrite(ctx, headKey, cfg.Round, func(payload []byte, recorded uint64) ([]byte, error) {
		if payload == nil {
			return nil, fmt.Errorf("vfs: no volume at %s: %w", cfg.Backend, fs.ErrNotExist)
		}
		if cfg.Round <= recorded {
			return nil, fmt.Errorf("%w: round %d does not exceed the recorded %d", ErrStaleRound, cfg.Round, recorded)
		}
		var plain []byte
		var err error
		var sealedHere bool // the Head is already sealed to this place
		if from == "" {
			plain, err = unsealAny(cfg.Cipher, payload)
		} else if plain, err = unseal(cfg.Cipher, headAAD(from), payload); errors.Is(err, errContext) {
			if plain, err = unseal(cfg.Cipher, headAAD(here), payload); err == nil {
				sealedHere = true
			} else {
				err = fmt.Errorf("%w (from %s)", errContext, from)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("vfs: open head: %w", err)
		}
		// Whichever way the Head was opened it is read like any other, so a Head that needs
		// a feature this build lacks is refused and nothing is claimed.
		h, err := unmarshalHead(plain)
		if err != nil {
			return nil, fmt.Errorf("vfs: head: %w", err)
		}
		// The copy is checked whichever way the Head was opened: a Head sealed to the old
		// place says nothing about whether the objects it names came along.
		w.adopt(h)
		if err := w.checkTree(ctx, cfg.Verify); err != nil {
			return nil, fmt.Errorf("vfs: the copy at %s is not whole: %w", here, err)
		}
		if sealedHere {
			return payload, nil // left as it is: only the Round is claimed
		}
		return seal(cfg.Cipher, headAAD(here), plain)
	})
}
