// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package djfs implements directory-granularity metadata mutation coalescing
// (DJFS) with a single global commit order, cross-directory dependency
// barriers, and two-stage batching into hanzoai/vfs/journal and Volume targets.
package djfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/journal"
)

// RootInode is the root directory identifier of every Engine and MemTree.
const RootInode InodeID = 1

const numStripes = 64

const defaultMaxDirtyOps = 1 << 16

const defaultMaxDirtyBytes = 64 << 20

const defaultMaxPublishWindow = 5 * time.Second

var (
	// ErrClosed is returned by mutating calls after Close.
	ErrClosed = errors.New("djfs: engine is closed")

	// ErrNotDir is returned when a directory operation names a non-directory.
	ErrNotDir = errors.New("djfs: not a directory")

	// ErrIsDir is returned when a file-only operation names a directory.
	ErrIsDir = errors.New("djfs: is a directory")

	// ErrNotEmpty is returned when Rmdir or Rename targets a non-empty directory.
	ErrNotEmpty = errors.New("djfs: directory not empty")

	// ErrCorrupt is returned when a serialized DJFS batch fails validation.
	ErrCorrupt = errors.New("djfs: corrupt batch frame")
)

// InodeID uniquely identifies a directory or regular file within an Engine.
type InodeID uint64

// Attr holds POSIX-shaped inode attributes tracked by the coalescer.
type Attr struct {
	ID    InodeID
	Mode  fs.FileMode
	Size  uint64
	Nlink uint32
	Mtime time.Time
	Atime time.Time
	Ctime time.Time
}

// IsDir reports whether the attribute describes a directory.
func (a Attr) IsDir() bool { return a.Mode.IsDir() }

// DirEntry describes one named child entry inside a directory.
type DirEntry struct {
	Name string
	ID   InodeID
	Mode fs.FileMode
}

// DeltaKind identifies an algebraic net-state operation in a coalesced Batch.
type DeltaKind uint8

const (
	DeltaMkdir DeltaKind = iota + 1
	DeltaCreateFile
	DeltaWrite
	DeltaRename
	DeltaUnlink
	DeltaRmdir
	DeltaSetAttr
)

// String returns a human-readable name for k.
func (k DeltaKind) String() string {
	switch k {
	case DeltaMkdir:
		return "mkdir"
	case DeltaCreateFile:
		return "create"
	case DeltaWrite:
		return "write"
	case DeltaRename:
		return "rename"
	case DeltaUnlink:
		return "unlink"
	case DeltaRmdir:
		return "rmdir"
	case DeltaSetAttr:
		return "setattr"
	default:
		return "unknown"
	}
}

// DeltaOp is one canonical mutation inside a coalesced Batch.
type DeltaOp struct {
	Kind    DeltaKind
	Dir     InodeID
	Name    string
	DstDir  InodeID
	DstName string
	ID      InodeID
	Attr    Attr
	Data    []byte
}

// Batch is an atomic, globally ordered commit unit emitted by the coalescer.
type Batch struct {
	Epoch  uint64
	MaxSeq uint64
	Ops    []DeltaOp
}

// Target receives coalesced batches during Merkle-tree publication.
type Target interface {
	ApplyBatch(ctx context.Context, batch Batch) error
}

// Journal is the append-only local durability log contract satisfied directly
// by *journal.Journal.
type Journal interface {
	Append(ctx context.Context, payload []byte) (uint64, error)
	Sync(ctx context.Context, seq uint64) error
	Publish(seq uint64) error
	Reclaim() error
	Marks() journal.Marks
}

// Replayer reads unpublished records in sequence order during crash recovery.
// *journal.Journal satisfies Replayer directly.
type Replayer interface {
	Replay(fn func(journal.Record) error) error
}

// Clock supplies timestamps for inode metadata and watermark age tracking.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// Config configures a DJFS Engine.
type Config struct {
	// Journal stores coalesced batches for local NVMe durability on Fsync.
	Journal Journal

	// Target receives Layer-2 coalesced batches at Publish time.
	Target Target

	// Clock supplies timestamps; nil defaults to UTC wall clock.
	Clock Clock

	// MaxDirtyOps bounds unjournaled raw mutations before automatic journal
	// relief runs. Zero or negative selects 65536.
	MaxDirtyOps int

	// MaxDirtyBytes bounds unjournaled payload bytes before automatic journal
	// relief runs. Zero or negative selects 64 MiB.
	MaxDirtyBytes int64

	// MaxPublishWindow is the target S3 publication bound (default 5 s).
	MaxPublishWindow time.Duration
}

func (c Config) normalized() Config {
	if c.Clock == nil {
		c.Clock = systemClock{}
	}
	if c.MaxDirtyOps <= 0 {
		c.MaxDirtyOps = defaultMaxDirtyOps
	}
	if c.MaxDirtyBytes <= 0 {
		c.MaxDirtyBytes = defaultMaxDirtyBytes
	}
	if c.MaxPublishWindow <= 0 {
		c.MaxPublishWindow = defaultMaxPublishWindow
	}
	return c
}

// Stats reports operational, amplification, and watermark counters for Engine.
type Stats struct {
	GlobalSeq       uint64
	Epoch           uint64
	Inodes          uint64
	RawOps          uint64
	RawBytes        uint64
	JournalBatches  uint64
	JournalOps      uint64
	JournalBytes    uint64
	PublishedEpochs uint64
	PublishedOps    uint64
	DirtyOps        int64
	DirtyBytes      int64
	LastJournalSeq  uint64
	PublishedJSeq   uint64
	UnpublishedAge  time.Duration
	PublishOverdue  bool
}

type fileNode struct {
	id   InodeID
	dir  InodeID
	name string
	attr Attr
	data []byte
}

type dirState struct {
	id              InodeID
	parent          InodeID
	name            string
	attr            Attr
	entries         map[string]DirEntry
	files           map[InodeID]*fileNode
	lastJSeq        uint64
	lastModSeq      uint64
	unjournaledOps  int64
	unjournaledByte int64
}

func snapFromDirState(d *dirState) *dirSnap {
	if d == nil {
		return nil
	}
	entries := make(map[string]DirEntry, len(d.entries))
	for k, v := range d.entries {
		entries[k] = v
	}
	files := make(map[InodeID]fileSnap, len(d.files))
	for fid, fn := range d.files {
		files[fid] = fileSnap{
			id:   fn.id,
			dir:  fn.dir,
			name: fn.name,
			attr: fn.attr,
			data: cloneBytes(fn.data),
		}
	}
	return &dirSnap{
		id:      d.id,
		parent:  d.parent,
		name:    d.name,
		attr:    d.attr,
		entries: entries,
		files:   files,
	}
}

type stripe struct {
	mu    sync.RWMutex
	dirs  map[InodeID]*dirState
	jBase map[InodeID]*dirSnap
	pBase map[InodeID]*dirSnap
}

// Engine coordinates lock-striped per-directory mutation coalescing while
// maintaining one monotonic global commit order and crash-consistent cross-dir
// barriers.
//
// Lock ordering hierarchy (strictly acyclic):
//  1. pubMu (serializes Publish calls off the writer path)
//  2. epochMu (RLock for single-dir and cross-dir file mutations; Lock for
//     directory hierarchy moves, entangled multi-dir Fsync, FlushJournal, and
//     Publish snapshot cuts)
//  3. stripes[i].mu in strictly ascending stripe index order (0 .. numStripes-1)
//  4. metaMu leaf lock (protects inoHome, dirty sets, and entanglement graph)
type Engine struct {
	cfg     Config
	pubMu   sync.Mutex
	epochMu sync.RWMutex
	stripes [numStripes]stripe

	metaMu        sync.Mutex
	closed        bool
	inoHome       map[InodeID]InodeID
	dirtyJ        map[InodeID]bool
	dirtyP        map[InodeID]bool
	entAdj        map[InodeID]map[InodeID]bool
	oldestUnpubAt time.Time

	nextIno         atomic.Uint64
	gseq            atomic.Uint64
	epoch           atomic.Uint64
	rawOps          atomic.Uint64
	rawBytes        atomic.Uint64
	journalBatches  atomic.Uint64
	journalOps      atomic.Uint64
	journalBytes    atomic.Uint64
	publishedEpochs atomic.Uint64
	publishedOps    atomic.Uint64
	dirtyOps        atomic.Int64
	dirtyBytes      atomic.Int64
	lastJSeq        atomic.Uint64
	publishedJSeq   atomic.Uint64
	relieving       atomic.Bool
}

// New constructs an Engine initialized with an empty RootInode directory.
func New(cfg Config) (*Engine, error) {
	cfg = cfg.normalized()
	return NewFromTree(cfg, NewMemTree(cfg.Clock.Now()))
}

// NewFromTree constructs an Engine seeded from an already-published MemTree.
func NewFromTree(cfg Config, base *MemTree) (*Engine, error) {
	cfg = cfg.normalized()
	if base == nil {
		base = NewMemTree(cfg.Clock.Now())
	}

	e := &Engine{
		cfg:     cfg,
		inoHome: make(map[InodeID]InodeID),
		dirtyJ:  make(map[InodeID]bool),
		dirtyP:  make(map[InodeID]bool),
		entAdj:  make(map[InodeID]map[InodeID]bool),
	}
	for i := range e.stripes {
		e.stripes[i].dirs = make(map[InodeID]*dirState)
		e.stripes[i].jBase = make(map[InodeID]*dirSnap)
		e.stripes[i].pBase = make(map[InodeID]*dirSnap)
	}
	e.epoch.Store(1)

	base.mu.RLock()
	maxID := RootInode
	for did, snap := range base.dirs {
		if did > maxID {
			maxID = did
		}
		clonedJ := cloneDirSnap(snap)
		clonedP := cloneDirSnap(snap)
		ds := &dirState{
			id:      snap.id,
			parent:  snap.parent,
			name:    snap.name,
			attr:    snap.attr,
			entries: make(map[string]DirEntry, len(snap.entries)),
			files:   make(map[InodeID]*fileNode, len(snap.files)),
		}
		for k, ent := range snap.entries {
			ds.entries[k] = ent
		}
		for fid, fsn := range snap.files {
			if fid > maxID {
				maxID = fid
			}
			ds.files[fid] = &fileNode{
				id:   fsn.id,
				dir:  fsn.dir,
				name: fsn.name,
				attr: fsn.attr,
				data: cloneBytes(fsn.data),
			}
			e.inoHome[fid] = did
		}
		idx := stripeIndex(did)
		e.stripes[idx].dirs[did] = ds
		e.stripes[idx].jBase[did] = clonedJ
		e.stripes[idx].pBase[did] = clonedP
		e.inoHome[did] = did
	}
	base.mu.RUnlock()

	e.nextIno.Store(uint64(maxID))
	if cfg.Journal != nil {
		marks := cfg.Journal.Marks()
		e.lastJSeq.Store(marks.Durable)
		e.publishedJSeq.Store(marks.Published)
	}
	return e, nil
}

// Recover reconstructs an Engine from an empty published base by replaying all
// unpublished journal records in global commit order.
func Recover(ctx context.Context, cfg Config, rep Replayer) (*Engine, error) {
	cfg = cfg.normalized()
	return RecoverFromTree(ctx, cfg, NewMemTree(cfg.Clock.Now()), rep)
}

// RecoverFromTree reconstructs an Engine from a committed base MemTree and
// replays every unpublished record from rep in sequence order. Replayed batches
// advance working state and jBase while preserving pBase so a subsequent
// Publish commits the recovered suffix to Target.
func RecoverFromTree(ctx context.Context, cfg Config, publishedBase *MemTree, rep Replayer) (*Engine, error) {
	cfg = cfg.normalized()
	if publishedBase == nil {
		publishedBase = NewMemTree(cfg.Clock.Now())
	}

	working := NewMemTree(cfg.Clock.Now())
	publishedBase.mu.RLock()
	working.dirs = make(map[InodeID]*dirSnap, len(publishedBase.dirs))
	working.files = make(map[InodeID]fileSnap, len(publishedBase.files))
	for did, ds := range publishedBase.dirs {
		working.dirs[did] = cloneDirSnap(ds)
	}
	for fid, fsn := range publishedBase.files {
		fsn.data = cloneBytes(fsn.data)
		working.files[fid] = fsn
	}
	publishedBase.mu.RUnlock()

	var maxEpoch uint64 = 1
	var maxSeq uint64
	var maxJSeq uint64
	var earliest time.Time

	if rep != nil {
		err := rep.Replay(func(rec journal.Record) error {
			batch, decErr := DecodeBatch(rec.Data)
			if decErr != nil {
				return decErr
			}
			if appErr := working.ApplyBatch(ctx, batch); appErr != nil {
				return appErr
			}
			if batch.Epoch >= maxEpoch {
				maxEpoch = batch.Epoch + 1
			}
			if batch.MaxSeq > maxSeq {
				maxSeq = batch.MaxSeq
			}
			if rec.Seq > maxJSeq {
				maxJSeq = rec.Seq
			}
			if !rec.Time.IsZero() && (earliest.IsZero() || rec.Time.Before(earliest)) {
				earliest = rec.Time.UTC()
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	e, err := NewFromTree(cfg, working)
	if err != nil {
		return nil, err
	}
	e.epoch.Store(maxEpoch)
	e.gseq.Store(maxSeq)
	if maxJSeq > e.lastJSeq.Load() {
		e.lastJSeq.Store(maxJSeq)
	}

	for i := range e.stripes {
		e.stripes[i].pBase = make(map[InodeID]*dirSnap)
	}
	publishedBase.mu.RLock()
	for did, snap := range publishedBase.dirs {
		idx := stripeIndex(did)
		e.stripes[idx].pBase[did] = cloneDirSnap(snap)
		if _, alive := e.stripes[idx].dirs[did]; !alive {
			e.dirtyP[did] = true
		}
	}
	publishedBase.mu.RUnlock()

	for i := range e.stripes {
		for did, ds := range e.stripes[i].dirs {
			if maxJSeq > 0 {
				ds.lastJSeq = maxJSeq
				ds.lastModSeq = maxSeq
				e.dirtyP[did] = true
			}
		}
	}
	if maxJSeq > 0 && len(e.dirtyP) > 0 {
		if earliest.IsZero() {
			earliest = cfg.Clock.Now().UTC()
		}
		e.oldestUnpubAt = earliest
	}
	return e, nil
}

func stripeIndex(id InodeID) int {
	x := uint64(id)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return int(x & (numStripes - 1))
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\x00") {
		return fmt.Errorf("%w: name %q", fs.ErrInvalid, name)
	}
	if strings.HasPrefix(name, ".djfs.stage.") {
		return fmt.Errorf("%w: reserved staging prefix %q", fs.ErrInvalid, name)
	}
	return nil
}

func (e *Engine) lockTwoStripes(a, b int) {
	if a == b {
		e.stripes[a].mu.Lock()
		return
	}
	if a < b {
		e.stripes[a].mu.Lock()
		e.stripes[b].mu.Lock()
		return
	}
	e.stripes[b].mu.Lock()
	e.stripes[a].mu.Lock()
}

func (e *Engine) unlockTwoStripes(a, b int) {
	if a == b {
		e.stripes[a].mu.Unlock()
		return
	}
	if a < b {
		e.stripes[b].mu.Unlock()
		e.stripes[a].mu.Unlock()
		return
	}
	e.stripes[a].mu.Unlock()
	e.stripes[b].mu.Unlock()
}

func (e *Engine) nowUTC() time.Time {
	return e.cfg.Clock.Now().UTC()
}

func (e *Engine) markDirtyLocked(dir InodeID, now time.Time) {
	e.dirtyJ[dir] = true
	e.dirtyP[dir] = true
	if e.oldestUnpubAt.IsZero() {
		e.oldestUnpubAt = now
	}
}

func (e *Engine) entangleLocked(a, b InodeID) {
	if a == b {
		return
	}
	ma := e.entAdj[a]
	if ma == nil {
		ma = make(map[InodeID]bool)
		e.entAdj[a] = ma
	}
	ma[b] = true

	mb := e.entAdj[b]
	if mb == nil {
		mb = make(map[InodeID]bool)
		e.entAdj[b] = mb
	}
	mb[a] = true
}

func (e *Engine) isEntangledLocked(dir InodeID) bool {
	return len(e.entAdj[dir]) > 0
}

func (e *Engine) componentLocked(seed InodeID) []InodeID {
	visited := map[InodeID]bool{seed: true}
	queue := []InodeID{seed}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for nb := range e.entAdj[cur] {
			if !visited[nb] {
				visited[nb] = true
				queue = append(queue, nb)
			}
		}
	}
	out := make([]InodeID, 0, len(visited))
	for id := range visited {
		out = append(out, id)
	}
	sortInodeIDs(out)
	return out
}

func (e *Engine) clearComponentLocked(comp []InodeID) {
	inComp := make(map[InodeID]bool, len(comp))
	for _, id := range comp {
		inComp[id] = true
	}
	for _, id := range comp {
		delete(e.dirtyJ, id)
		delete(e.entAdj, id)
	}
	for other, adj := range e.entAdj {
		for id := range inComp {
			delete(adj, id)
		}
		if len(adj) == 0 {
			delete(e.entAdj, other)
		}
	}
}

func (e *Engine) recordRaw(payloadBytes int64, touched ...*dirState) {
	seq := e.gseq.Add(1)
	e.rawOps.Add(1)
	if payloadBytes > 0 {
		e.rawBytes.Add(uint64(payloadBytes))
	}
	e.dirtyOps.Add(1)
	if payloadBytes > 0 {
		e.dirtyBytes.Add(payloadBytes)
	}
	for i, d := range touched {
		if d == nil {
			continue
		}
		d.lastModSeq = seq
		if i == 0 {
			d.unjournaledOps++
			if payloadBytes > 0 {
				d.unjournaledByte += payloadBytes
			}
		}
	}
}

func (e *Engine) maybeRelieve(ctx context.Context) error {
	opsOver := e.dirtyOps.Load() >= int64(e.cfg.MaxDirtyOps)
	bytesOver := e.dirtyBytes.Load() >= e.cfg.MaxDirtyBytes
	if !opsOver && !bytesOver {
		return nil
	}
	if !e.relieving.CompareAndSwap(false, true) {
		return nil
	}
	defer e.relieving.Store(false)
	_, err := e.FlushJournal(ctx)
	return err
}

func (e *Engine) updateMaxJSeq(seq uint64) {
	for {
		cur := e.lastJSeq.Load()
		if seq <= cur {
			return
		}
		if e.lastJSeq.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// Create creates a new regular file in dir with optional initial inline/small
// payload bytes.
func (e *Engine) Create(ctx context.Context, dir InodeID, name string, perm fs.FileMode, data []byte) (Attr, error) {
	if err := validName(name); err != nil {
		return Attr{}, err
	}

	e.epochMu.RLock()
	idx := stripeIndex(dir)
	s := &e.stripes[idx]
	s.mu.Lock()

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, ErrClosed
	}
	e.metaMu.Unlock()

	d := s.dirs[dir]
	if d == nil {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	if _, exists := d.entries[name]; exists {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: %q in dir %d", fs.ErrExist, name, dir)
	}

	now := e.nowUTC()
	id := InodeID(e.nextIno.Add(1))
	copied := cloneBytes(data)
	attr := Attr{
		ID:    id,
		Mode:  perm.Perm(),
		Size:  uint64(len(copied)),
		Nlink: 1,
		Mtime: now,
		Atime: now,
		Ctime: now,
	}
	d.entries[name] = DirEntry{Name: name, ID: id, Mode: 0}
	d.files[id] = &fileNode{
		id:   id,
		dir:  dir,
		name: name,
		attr: attr,
		data: copied,
	}
	d.attr.Mtime = now
	d.attr.Ctime = now
	e.recordRaw(int64(len(copied)), d)

	e.metaMu.Lock()
	e.inoHome[id] = dir
	e.markDirtyLocked(dir, now)
	e.metaMu.Unlock()

	s.mu.Unlock()
	e.epochMu.RUnlock()

	if err := e.maybeRelieve(ctx); err != nil {
		return Attr{}, err
	}
	return attr, nil
}

// Mkdir creates a new directory named name inside parentDir.
func (e *Engine) Mkdir(ctx context.Context, parentDir InodeID, name string, perm fs.FileMode) (Attr, error) {
	if err := validName(name); err != nil {
		return Attr{}, err
	}

	childID := InodeID(e.nextIno.Add(1))
	pIdx := stripeIndex(parentDir)
	cIdx := stripeIndex(childID)

	e.epochMu.RLock()
	e.lockTwoStripes(pIdx, cIdx)

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		e.unlockTwoStripes(pIdx, cIdx)
		e.epochMu.RUnlock()
		return Attr{}, ErrClosed
	}
	e.metaMu.Unlock()

	parent := e.stripes[pIdx].dirs[parentDir]
	if parent == nil {
		e.unlockTwoStripes(pIdx, cIdx)
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, parentDir)
	}
	if _, exists := parent.entries[name]; exists {
		e.unlockTwoStripes(pIdx, cIdx)
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: %q in dir %d", fs.ErrExist, name, parentDir)
	}

	now := e.nowUTC()
	attr := Attr{
		ID:    childID,
		Mode:  fs.ModeDir | perm.Perm(),
		Nlink: 2,
		Mtime: now,
		Atime: now,
		Ctime: now,
	}
	child := &dirState{
		id:      childID,
		parent:  parentDir,
		name:    name,
		attr:    attr,
		entries: make(map[string]DirEntry),
		files:   make(map[InodeID]*fileNode),
	}
	parent.entries[name] = DirEntry{Name: name, ID: childID, Mode: fs.ModeDir}
	parent.attr.Nlink++
	parent.attr.Mtime = now
	parent.attr.Ctime = now
	e.stripes[cIdx].dirs[childID] = child
	e.recordRaw(0, parent, child)

	e.metaMu.Lock()
	e.inoHome[childID] = childID
	e.entangleLocked(parentDir, childID)
	e.markDirtyLocked(parentDir, now)
	e.markDirtyLocked(childID, now)
	e.metaMu.Unlock()

	e.unlockTwoStripes(pIdx, cIdx)
	e.epochMu.RUnlock()

	if err := e.maybeRelieve(ctx); err != nil {
		return Attr{}, err
	}
	return attr, nil
}

// Write replaces the payload bytes of regular file name in dir, coalescing with
// prior uncommitted writes or creates in the same epoch.
func (e *Engine) Write(ctx context.Context, dir InodeID, name string, data []byte) (Attr, error) {
	if err := validName(name); err != nil {
		return Attr{}, err
	}

	e.epochMu.RLock()
	idx := stripeIndex(dir)
	s := &e.stripes[idx]
	s.mu.Lock()

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, ErrClosed
	}
	e.metaMu.Unlock()

	d := s.dirs[dir]
	if d == nil {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	ent, ok := d.entries[name]
	if !ok {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, name, dir)
	}
	if ent.Mode.IsDir() {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: %q", ErrIsDir, name)
	}

	fn := d.files[ent.ID]
	now := e.nowUTC()
	copied := cloneBytes(data)
	fn.data = copied
	fn.attr.Size = uint64(len(copied))
	fn.attr.Mtime = now
	fn.attr.Ctime = now
	attr := fn.attr
	e.recordRaw(int64(len(copied)), d)

	e.metaMu.Lock()
	e.markDirtyLocked(dir, now)
	e.metaMu.Unlock()

	s.mu.Unlock()
	e.epochMu.RUnlock()

	if err := e.maybeRelieve(ctx); err != nil {
		return Attr{}, err
	}
	return attr, nil
}

// Chmod updates the permission bits of dir/name in place.
func (e *Engine) Chmod(ctx context.Context, dir InodeID, name string, perm fs.FileMode) (Attr, error) {
	if err := validName(name); err != nil {
		return Attr{}, err
	}

	e.epochMu.RLock()
	idx := stripeIndex(dir)
	s := &e.stripes[idx]
	s.mu.Lock()

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, ErrClosed
	}
	e.metaMu.Unlock()

	d := s.dirs[dir]
	if d == nil {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	ent, ok := d.entries[name]
	if !ok {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, name, dir)
	}
	if !ent.Mode.IsDir() {
		fn := d.files[ent.ID]
		now := e.nowUTC()
		fn.attr.Mode = perm.Perm()
		fn.attr.Ctime = now
		attr := fn.attr
		e.recordRaw(0, d)
		e.metaMu.Lock()
		e.markDirtyLocked(dir, now)
		e.metaMu.Unlock()
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return attr, e.maybeRelieve(ctx)
	}

	subID := ent.ID
	s.mu.Unlock()
	subIdx := stripeIndex(subID)
	subS := &e.stripes[subIdx]
	subS.mu.Lock()
	sub := subS.dirs[subID]
	if sub == nil {
		subS.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, subID)
	}
	now := e.nowUTC()
	sub.attr.Mode = fs.ModeDir | perm.Perm()
	sub.attr.Ctime = now
	attr := sub.attr
	e.recordRaw(0, sub)
	e.metaMu.Lock()
	e.markDirtyLocked(subID, now)
	e.metaMu.Unlock()
	subS.mu.Unlock()
	e.epochMu.RUnlock()
	return attr, e.maybeRelieve(ctx)
}

// Touch updates mtime and atime of dir/name in place.
func (e *Engine) Touch(ctx context.Context, dir InodeID, name string, mtime, atime time.Time) (Attr, error) {
	if err := validName(name); err != nil {
		return Attr{}, err
	}

	e.epochMu.RLock()
	idx := stripeIndex(dir)
	s := &e.stripes[idx]
	s.mu.Lock()

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, ErrClosed
	}
	e.metaMu.Unlock()

	d := s.dirs[dir]
	if d == nil {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	ent, ok := d.entries[name]
	if !ok {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, name, dir)
	}
	now := e.nowUTC()
	if mtime.IsZero() {
		mtime = now
	}
	if atime.IsZero() {
		atime = now
	}
	mtime = mtime.UTC()
	atime = atime.UTC()

	if !ent.Mode.IsDir() {
		fn := d.files[ent.ID]
		fn.attr.Mtime = mtime
		fn.attr.Atime = atime
		fn.attr.Ctime = now
		attr := fn.attr
		e.recordRaw(0, d)
		e.metaMu.Lock()
		e.markDirtyLocked(dir, now)
		e.metaMu.Unlock()
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return attr, e.maybeRelieve(ctx)
	}

	subID := ent.ID
	s.mu.Unlock()
	subIdx := stripeIndex(subID)
	subS := &e.stripes[subIdx]
	subS.mu.Lock()
	sub := subS.dirs[subID]
	if sub == nil {
		subS.mu.Unlock()
		e.epochMu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, subID)
	}
	sub.attr.Mtime = mtime
	sub.attr.Atime = atime
	sub.attr.Ctime = now
	attr := sub.attr
	e.recordRaw(0, sub)
	e.metaMu.Lock()
	e.markDirtyLocked(subID, now)
	e.metaMu.Unlock()
	subS.mu.Unlock()
	e.epochMu.RUnlock()
	return attr, e.maybeRelieve(ctx)
}

// Unlink removes a regular-file entry from dir. If the file was created in the
// current epoch, its creation, writes, and attributes cancel completely on the
// next flush.
func (e *Engine) Unlink(ctx context.Context, dir InodeID, name string) error {
	if err := validName(name); err != nil {
		return err
	}

	e.epochMu.RLock()
	idx := stripeIndex(dir)
	s := &e.stripes[idx]
	s.mu.Lock()

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return ErrClosed
	}
	e.metaMu.Unlock()

	d := s.dirs[dir]
	if d == nil {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	ent, ok := d.entries[name]
	if !ok {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, name, dir)
	}
	if ent.Mode.IsDir() {
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: %q", ErrIsDir, name)
	}

	now := e.nowUTC()
	delete(d.entries, name)
	delete(d.files, ent.ID)
	d.attr.Mtime = now
	d.attr.Ctime = now
	e.recordRaw(0, d)

	e.metaMu.Lock()
	delete(e.inoHome, ent.ID)
	e.markDirtyLocked(dir, now)
	e.metaMu.Unlock()

	s.mu.Unlock()
	e.epochMu.RUnlock()
	return e.maybeRelieve(ctx)
}

// Rmdir removes an empty subdirectory named name from parentDir.
func (e *Engine) Rmdir(ctx context.Context, parentDir InodeID, name string) error {
	if err := validName(name); err != nil {
		return err
	}

	e.epochMu.RLock()
	pIdx := stripeIndex(parentDir)
	e.stripes[pIdx].mu.RLock()
	parentPeek := e.stripes[pIdx].dirs[parentDir]
	if parentPeek == nil {
		e.stripes[pIdx].mu.RUnlock()
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: dir %d", fs.ErrNotExist, parentDir)
	}
	entPeek, ok := parentPeek.entries[name]
	e.stripes[pIdx].mu.RUnlock()
	if !ok {
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, name, parentDir)
	}
	if !entPeek.Mode.IsDir() {
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: %q", ErrNotDir, name)
	}

	childID := entPeek.ID
	cIdx := stripeIndex(childID)
	e.lockTwoStripes(pIdx, cIdx)

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		e.unlockTwoStripes(pIdx, cIdx)
		e.epochMu.RUnlock()
		return ErrClosed
	}
	e.metaMu.Unlock()

	parent := e.stripes[pIdx].dirs[parentDir]
	if parent == nil {
		e.unlockTwoStripes(pIdx, cIdx)
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: dir %d", fs.ErrNotExist, parentDir)
	}
	ent, ok := parent.entries[name]
	if !ok || ent.ID != childID || !ent.Mode.IsDir() {
		e.unlockTwoStripes(pIdx, cIdx)
		e.epochMu.RUnlock()
		if !ok {
			return fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, name, parentDir)
		}
		return fmt.Errorf("%w: %q", ErrNotDir, name)
	}
	child := e.stripes[cIdx].dirs[childID]
	if child != nil && len(child.entries) > 0 {
		e.unlockTwoStripes(pIdx, cIdx)
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: %q", ErrNotEmpty, name)
	}

	now := e.nowUTC()
	delete(parent.entries, name)
	parent.attr.Nlink--
	parent.attr.Mtime = now
	parent.attr.Ctime = now
	if child != nil {
		e.dirtyOps.Add(-child.unjournaledOps)
		e.dirtyBytes.Add(-child.unjournaledByte)
	}
	delete(e.stripes[cIdx].dirs, childID)
	e.recordRaw(0, parent)

	e.metaMu.Lock()
	delete(e.inoHome, childID)
	e.entangleLocked(parentDir, childID)
	e.markDirtyLocked(parentDir, now)
	e.markDirtyLocked(childID, now)
	e.metaMu.Unlock()

	e.unlockTwoStripes(pIdx, cIdx)
	e.epochMu.RUnlock()
	return e.maybeRelieve(ctx)
}

// Rename atomically moves srcDir/srcName to dstDir/dstName. Regular-file
// renames execute concurrently under per-directory stripe locks; cross-dir
// renames entangle srcDir and dstDir so any subsequent Fsync commits both
// sides atomically without torn link counts or orphaned entries.
func (e *Engine) Rename(ctx context.Context, srcDir InodeID, srcName string, dstDir InodeID, dstName string) error {
	if err := validName(srcName); err != nil {
		return err
	}
	if err := validName(dstName); err != nil {
		return err
	}

	sIdx := stripeIndex(srcDir)
	dIdx := stripeIndex(dstDir)

	e.epochMu.RLock()
	e.lockTwoStripes(sIdx, dIdx)

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		e.unlockTwoStripes(sIdx, dIdx)
		e.epochMu.RUnlock()
		return ErrClosed
	}
	e.metaMu.Unlock()

	sd := e.stripes[sIdx].dirs[srcDir]
	dd := e.stripes[dIdx].dirs[dstDir]
	if sd == nil || dd == nil {
		e.unlockTwoStripes(sIdx, dIdx)
		e.epochMu.RUnlock()
		if sd == nil {
			return fmt.Errorf("%w: dir %d", fs.ErrNotExist, srcDir)
		}
		return fmt.Errorf("%w: dir %d", fs.ErrNotExist, dstDir)
	}
	srcEnt, ok := sd.entries[srcName]
	if !ok {
		e.unlockTwoStripes(sIdx, dIdx)
		e.epochMu.RUnlock()
		return fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, srcName, srcDir)
	}
	dstEnt, hasDst := dd.entries[dstName]

	if !srcEnt.Mode.IsDir() && (!hasDst || !dstEnt.Mode.IsDir()) {
		if srcDir == dstDir && srcName == dstName {
			e.unlockTwoStripes(sIdx, dIdx)
			e.epochMu.RUnlock()
			return nil
		}
		now := e.nowUTC()
		var victimID InodeID
		if hasDst {
			victimID = dstEnt.ID
			if victimID == srcEnt.ID {
				e.unlockTwoStripes(sIdx, dIdx)
				e.epochMu.RUnlock()
				return nil
			}
			delete(dd.files, victimID)
		}

		fn := sd.files[srcEnt.ID]
		delete(sd.entries, srcName)
		if srcDir != dstDir {
			delete(sd.files, srcEnt.ID)
		}
		fn.dir = dstDir
		fn.name = dstName
		fn.attr.Ctime = now
		dd.files[srcEnt.ID] = fn
		dd.entries[dstName] = DirEntry{Name: dstName, ID: srcEnt.ID, Mode: 0}
		sd.attr.Mtime = now
		sd.attr.Ctime = now
		dd.attr.Mtime = now
		dd.attr.Ctime = now
		e.recordRaw(0, dd, sd)

		e.metaMu.Lock()
		if victimID != 0 {
			delete(e.inoHome, victimID)
		}
		e.inoHome[srcEnt.ID] = dstDir
		if srcDir != dstDir {
			e.entangleLocked(srcDir, dstDir)
			e.markDirtyLocked(srcDir, now)
		}
		e.markDirtyLocked(dstDir, now)
		e.metaMu.Unlock()

		e.unlockTwoStripes(sIdx, dIdx)
		e.epochMu.RUnlock()
		return e.maybeRelieve(ctx)
	}

	e.unlockTwoStripes(sIdx, dIdx)
	e.epochMu.RUnlock()

	e.epochMu.Lock()
	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		e.epochMu.Unlock()
		return ErrClosed
	}
	e.metaMu.Unlock()

	err := e.renameExclusiveLocked(srcDir, srcName, dstDir, dstName)
	e.epochMu.Unlock()
	if err != nil {
		return err
	}
	return e.maybeRelieve(ctx)
}

func (e *Engine) getDirExclusive(id InodeID) *dirState {
	return e.stripes[stripeIndex(id)].dirs[id]
}

func (e *Engine) isDescendantExclusive(cand, target InodeID) bool {
	cur := cand
	for steps := 0; steps < 1<<16 && cur != RootInode; steps++ {
		if cur == target {
			return true
		}
		d := e.getDirExclusive(cur)
		if d == nil || d.parent == cur {
			break
		}
		cur = d.parent
	}
	return cur == target
}

func (e *Engine) renameExclusiveLocked(srcDir InodeID, srcName string, dstDir InodeID, dstName string) error {
	sd := e.getDirExclusive(srcDir)
	dd := e.getDirExclusive(dstDir)
	if sd == nil {
		return fmt.Errorf("%w: dir %d", fs.ErrNotExist, srcDir)
	}
	if dd == nil {
		return fmt.Errorf("%w: dir %d", fs.ErrNotExist, dstDir)
	}
	srcEnt, ok := sd.entries[srcName]
	if !ok {
		return fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, srcName, srcDir)
	}
	if srcDir == dstDir && srcName == dstName {
		return nil
	}

	dstEnt, hasDst := dd.entries[dstName]
	if hasDst {
		if srcEnt.Mode.IsDir() && !dstEnt.Mode.IsDir() {
			return fmt.Errorf("%w: %q", ErrNotDir, dstName)
		}
		if !srcEnt.Mode.IsDir() && dstEnt.Mode.IsDir() {
			return fmt.Errorf("%w: %q", ErrIsDir, dstName)
		}
	}

	if srcEnt.Mode.IsDir() {
		if srcEnt.ID == RootInode || e.isDescendantExclusive(dstDir, srcEnt.ID) {
			return fmt.Errorf("%w: cannot move directory %d into itself (%d)", fs.ErrInvalid, srcEnt.ID, dstDir)
		}
		var victimDirID InodeID
		if hasDst {
			if dstEnt.ID == srcEnt.ID {
				return nil
			}
			vd := e.getDirExclusive(dstEnt.ID)
			if vd != nil && len(vd.entries) > 0 {
				return fmt.Errorf("%w: %q", ErrNotEmpty, dstName)
			}
			victimDirID = dstEnt.ID
			if vd != nil {
				e.dirtyOps.Add(-vd.unjournaledOps)
				e.dirtyBytes.Add(-vd.unjournaledByte)
			}
			delete(e.stripes[stripeIndex(victimDirID)].dirs, victimDirID)
			dd.attr.Nlink--
		}

		sub := e.getDirExclusive(srcEnt.ID)
		if sub == nil {
			return fmt.Errorf("%w: dir %d", fs.ErrNotExist, srcEnt.ID)
		}
		now := e.nowUTC()
		delete(sd.entries, srcName)
		sd.attr.Nlink--
		sd.attr.Mtime = now
		sd.attr.Ctime = now

		dd.entries[dstName] = DirEntry{Name: dstName, ID: srcEnt.ID, Mode: fs.ModeDir}
		dd.attr.Nlink++
		dd.attr.Mtime = now
		dd.attr.Ctime = now

		sub.parent = dstDir
		sub.name = dstName
		sub.attr.Ctime = now
		e.recordRaw(0, dd, sd, sub)

		e.metaMu.Lock()
		if victimDirID != 0 {
			delete(e.inoHome, victimDirID)
			e.entangleLocked(dstDir, victimDirID)
			e.markDirtyLocked(victimDirID, now)
		}
		e.entangleLocked(srcDir, srcEnt.ID)
		e.entangleLocked(dstDir, srcEnt.ID)
		e.entangleLocked(srcDir, dstDir)
		e.markDirtyLocked(srcDir, now)
		e.markDirtyLocked(dstDir, now)
		e.markDirtyLocked(srcEnt.ID, now)
		e.metaMu.Unlock()
		return nil
	}

	now := e.nowUTC()
	var victimID InodeID
	if hasDst {
		victimID = dstEnt.ID
		delete(dd.files, victimID)
	}
	fn := sd.files[srcEnt.ID]
	delete(sd.entries, srcName)
	if srcDir != dstDir {
		delete(sd.files, srcEnt.ID)
	}
	fn.dir = dstDir
	fn.name = dstName
	fn.attr.Ctime = now
	dd.files[srcEnt.ID] = fn
	dd.entries[dstName] = DirEntry{Name: dstName, ID: srcEnt.ID, Mode: 0}
	sd.attr.Mtime = now
	sd.attr.Ctime = now
	dd.attr.Mtime = now
	dd.attr.Ctime = now
	e.recordRaw(0, dd, sd)

	e.metaMu.Lock()
	if victimID != 0 {
		delete(e.inoHome, victimID)
	}
	e.inoHome[srcEnt.ID] = dstDir
	if srcDir != dstDir {
		e.entangleLocked(srcDir, dstDir)
		e.markDirtyLocked(srcDir, now)
	}
	e.markDirtyLocked(dstDir, now)
	e.metaMu.Unlock()
	return nil
}

// Lookup resolves name in dir against the live working state.
func (e *Engine) Lookup(dir InodeID, name string) (Attr, error) {
	if err := validName(name); err != nil {
		return Attr{}, err
	}
	e.epochMu.RLock()
	defer e.epochMu.RUnlock()

	idx := stripeIndex(dir)
	s := &e.stripes[idx]
	s.mu.RLock()
	d := s.dirs[dir]
	if d == nil {
		s.mu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	ent, ok := d.entries[name]
	if !ok {
		s.mu.RUnlock()
		return Attr{}, fmt.Errorf("%w: %q in dir %d", fs.ErrNotExist, name, dir)
	}
	if !ent.Mode.IsDir() {
		fn := d.files[ent.ID]
		attr := fn.attr
		s.mu.RUnlock()
		return attr, nil
	}
	subID := ent.ID
	s.mu.RUnlock()

	subS := &e.stripes[stripeIndex(subID)]
	subS.mu.RLock()
	sub := subS.dirs[subID]
	if sub == nil {
		subS.mu.RUnlock()
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, subID)
	}
	attr := sub.attr
	subS.mu.RUnlock()
	return attr, nil
}

// Stat returns the live attributes of inode id.
func (e *Engine) Stat(id InodeID) (Attr, error) {
	e.epochMu.RLock()
	defer e.epochMu.RUnlock()

	e.metaMu.Lock()
	home, ok := e.inoHome[id]
	e.metaMu.Unlock()
	if !ok {
		return Attr{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, id)
	}

	s := &e.stripes[stripeIndex(home)]
	s.mu.RLock()
	defer s.mu.RUnlock()

	d := s.dirs[home]
	if d == nil {
		return Attr{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, id)
	}
	if home == id {
		return d.attr, nil
	}
	fn := d.files[id]
	if fn == nil {
		return Attr{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, id)
	}
	return fn.attr, nil
}

// ReadDir returns the live sorted entries of directory dir.
func (e *Engine) ReadDir(dir InodeID) ([]DirEntry, error) {
	e.epochMu.RLock()
	defer e.epochMu.RUnlock()

	s := &e.stripes[stripeIndex(dir)]
	s.mu.RLock()
	d := s.dirs[dir]
	if d == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	out := make([]DirEntry, 0, len(d.entries))
	for _, ent := range d.entries {
		out = append(out, ent)
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ReadFile returns a copy of regular file id's bytes.
func (e *Engine) ReadFile(id InodeID) ([]byte, error) {
	e.epochMu.RLock()
	defer e.epochMu.RUnlock()

	e.metaMu.Lock()
	home, ok := e.inoHome[id]
	e.metaMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: inode %d", fs.ErrNotExist, id)
	}
	if home == id {
		return nil, fmt.Errorf("%w: inode %d", ErrIsDir, id)
	}

	s := &e.stripes[stripeIndex(home)]
	s.mu.RLock()
	defer s.mu.RUnlock()

	d := s.dirs[home]
	if d == nil {
		return nil, fmt.Errorf("%w: inode %d", fs.ErrNotExist, id)
	}
	fn := d.files[id]
	if fn == nil {
		return nil, fmt.Errorf("%w: inode %d", fs.ErrNotExist, id)
	}
	return cloneBytes(fn.data), nil
}

// CloseFile models a clean POSIX close(2) without fsync(2): it performs zero
// synchronous S3 requests and zero synchronous local disk flushes, leaving
// staged mutations to coalesce until Fsync, watermark relief, or the 5 s
// Publish window.
func (e *Engine) CloseFile(id InodeID) error {
	_, err := e.Stat(id)
	return err
}

// Fsync makes inode id and all causally entangled directory mutations durable
// in the local Journal.
func (e *Engine) Fsync(ctx context.Context, id InodeID) error {
	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		return ErrClosed
	}
	home, ok := e.inoHome[id]
	e.metaMu.Unlock()
	if !ok {
		return fmt.Errorf("%w: inode %d", fs.ErrNotExist, id)
	}
	return e.FsyncDir(ctx, home)
}

// FsyncDir coalesces and flushes directory dir (and any directories entangled
// with dir via cross-directory Rename, Mkdir, or Rmdir) to Journal, releasing
// all Engine locks before waiting on Journal.Sync.
func (e *Engine) FsyncDir(ctx context.Context, dir InodeID) error {
	e.epochMu.RLock()
	idx := stripeIndex(dir)
	s := &e.stripes[idx]
	s.mu.Lock()

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		s.mu.Unlock()
		e.epochMu.RUnlock()
		return ErrClosed
	}
	entangled := e.isEntangledLocked(dir)
	if !entangled {
		d := s.dirs[dir]
		if d == nil {
			e.metaMu.Unlock()
			s.mu.Unlock()
			e.epochMu.RUnlock()
			return fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
		}
		wasDirty := e.dirtyJ[dir]
		delete(e.dirtyJ, dir)
		e.metaMu.Unlock()

		targetSeq := d.lastJSeq
		if wasDirty {
			currSnap := snapFromDirState(d)
			baseMap := map[InodeID]*dirSnap{dir: s.jBase[dir]}
			currMap := map[InodeID]*dirSnap{dir: currSnap}
			ops := computeDelta(baseMap, currMap)
			if len(ops) > 0 {
				batch := Batch{
					Epoch:  e.epoch.Load(),
					MaxSeq: e.gseq.Load(),
					Ops:    ops,
				}
				if e.cfg.Journal != nil {
					raw := EncodeBatch(batch)
					seq, err := e.cfg.Journal.Append(ctx, raw)
					if err != nil {
						e.metaMu.Lock()
						e.dirtyJ[dir] = true
						e.metaMu.Unlock()
						s.mu.Unlock()
						e.epochMu.RUnlock()
						return err
					}
					targetSeq = seq
					d.lastJSeq = seq
					e.updateMaxJSeq(seq)
					e.journalBatches.Add(1)
					e.journalOps.Add(uint64(len(ops)))
					e.journalBytes.Add(uint64(len(raw)))
				} else {
					e.journalBatches.Add(1)
					e.journalOps.Add(uint64(len(ops)))
				}
				s.jBase[dir] = currSnap
			}
			e.dirtyOps.Add(-d.unjournaledOps)
			e.dirtyBytes.Add(-d.unjournaledByte)
			d.unjournaledOps = 0
			d.unjournaledByte = 0
		}

		s.mu.Unlock()
		e.epochMu.RUnlock()

		if e.cfg.Journal != nil && targetSeq > 0 {
			return e.cfg.Journal.Sync(ctx, targetSeq)
		}
		return nil
	}

	e.metaMu.Unlock()
	s.mu.Unlock()
	e.epochMu.RUnlock()

	e.epochMu.Lock()
	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		e.epochMu.Unlock()
		return ErrClosed
	}
	if e.getDirExclusive(dir) == nil {
		e.metaMu.Unlock()
		e.epochMu.Unlock()
		return fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	comp := e.componentLocked(dir)
	e.metaMu.Unlock()

	targetSeq, err := e.flushComponentExclusiveLocked(ctx, comp)
	e.epochMu.Unlock()
	if err != nil {
		return err
	}
	if e.cfg.Journal != nil && targetSeq > 0 {
		return e.cfg.Journal.Sync(ctx, targetSeq)
	}
	return nil
}

func (e *Engine) flushComponentExclusiveLocked(ctx context.Context, comp []InodeID) (uint64, error) {
	baseMap := make(map[InodeID]*dirSnap, len(comp)+4)
	currMap := make(map[InodeID]*dirSnap, len(comp)+4)
	var maxExistingSeq uint64

	expanded := make(map[InodeID]bool, len(comp))
	for _, did := range comp {
		expanded[did] = true
		idx := stripeIndex(did)
		if bd := e.stripes[idx].jBase[did]; bd != nil && bd.parent != 0 {
			expanded[bd.parent] = true
		}
		if cd := e.stripes[idx].dirs[did]; cd != nil && cd.parent != 0 {
			expanded[cd.parent] = true
		}
	}
	allIDs := make([]InodeID, 0, len(expanded))
	for did := range expanded {
		allIDs = append(allIDs, did)
	}
	sortInodeIDs(allIDs)

	for _, did := range allIDs {
		idx := stripeIndex(did)
		if bd := e.stripes[idx].jBase[did]; bd != nil {
			baseMap[did] = bd
		}
		if cd := e.stripes[idx].dirs[did]; cd != nil {
			currMap[did] = snapFromDirState(cd)
			if cd.lastJSeq > maxExistingSeq {
				maxExistingSeq = cd.lastJSeq
			}
		}
	}

	ops := computeDelta(baseMap, currMap)
	targetSeq := maxExistingSeq
	if len(ops) > 0 {
		batch := Batch{
			Epoch:  e.epoch.Load(),
			MaxSeq: e.gseq.Load(),
			Ops:    ops,
		}
		if e.cfg.Journal != nil {
			raw := EncodeBatch(batch)
			seq, err := e.cfg.Journal.Append(ctx, raw)
			if err != nil {
				return 0, err
			}
			targetSeq = seq
			e.updateMaxJSeq(seq)
			e.journalBatches.Add(1)
			e.journalOps.Add(uint64(len(ops)))
			e.journalBytes.Add(uint64(len(raw)))
		} else {
			e.journalBatches.Add(1)
			e.journalOps.Add(uint64(len(ops)))
		}
	}

	for _, did := range allIDs {
		idx := stripeIndex(did)
		if snap, alive := currMap[did]; alive && snap != nil {
			e.stripes[idx].jBase[did] = snap
		} else {
			delete(e.stripes[idx].jBase, did)
		}
		if cd := e.stripes[idx].dirs[did]; cd != nil {
			if targetSeq > cd.lastJSeq {
				cd.lastJSeq = targetSeq
			}
			e.dirtyOps.Add(-cd.unjournaledOps)
			e.dirtyBytes.Add(-cd.unjournaledByte)
			d := cd
			d.unjournaledOps = 0
			d.unjournaledByte = 0
		}
	}

	e.metaMu.Lock()
	e.clearComponentLocked(allIDs)
	e.metaMu.Unlock()
	return targetSeq, nil
}

// FlushJournal coalesces all currently unjournaled directory partitions into
// one globally ordered journal batch without forcing S3 Target publication.
func (e *Engine) FlushJournal(ctx context.Context) (uint64, error) {
	e.epochMu.Lock()
	defer e.epochMu.Unlock()

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		return 0, ErrClosed
	}
	if len(e.dirtyJ) == 0 {
		e.metaMu.Unlock()
		return e.lastJSeq.Load(), nil
	}
	comp := make([]InodeID, 0, len(e.dirtyJ))
	for did := range e.dirtyJ {
		comp = append(comp, did)
	}
	e.metaMu.Unlock()
	sortInodeIDs(comp)

	seq, err := e.flushComponentExclusiveLocked(ctx, comp)
	if err != nil {
		return 0, err
	}
	e.dirtyOps.Store(0)
	e.dirtyBytes.Store(0)
	return seq, nil
}

// Publish executes two-stage coalescing and commits the resulting minimal
// epoch Batch to Target:
//  1. Any unjournaled mutations are coalesced and synced to Journal so local
//     durability precedes S3 publication (RG1).
//  2. All mutations since the last published epoch are coalesced directly from
//     pBase -> current state before calling Target.ApplyBatch, absorbing even
//     intermediate synced scratch/rename churn across the 5 s window.
//  3. Only after Target.ApplyBatch succeeds does Publish advance Journal.Publish
//     and Journal.Reclaim (RG2).
func (e *Engine) Publish(ctx context.Context) error {
	e.pubMu.Lock()
	defer e.pubMu.Unlock()

	e.epochMu.Lock()

	e.metaMu.Lock()
	if e.closed {
		e.metaMu.Unlock()
		e.epochMu.Unlock()
		return ErrClosed
	}
	hasDirtyJ := len(e.dirtyJ) > 0
	var compJ []InodeID
	if hasDirtyJ {
		compJ = make([]InodeID, 0, len(e.dirtyJ))
		for did := range e.dirtyJ {
			compJ = append(compJ, did)
		}
		sortInodeIDs(compJ)
	}
	e.metaMu.Unlock()

	if hasDirtyJ {
		if _, err := e.flushComponentExclusiveLocked(ctx, compJ); err != nil {
			e.epochMu.Unlock()
			return err
		}
		e.dirtyOps.Store(0)
		e.dirtyBytes.Store(0)
	}

	targetJSeq := e.lastJSeq.Load()
	e.metaMu.Lock()
	if len(e.dirtyP) == 0 {
		e.oldestUnpubAt = time.Time{}
		e.metaMu.Unlock()
		e.epochMu.Unlock()
		return nil
	}
	expanded := make(map[InodeID]bool, len(e.dirtyP)+4)
	for did := range e.dirtyP {
		expanded[did] = true
		idx := stripeIndex(did)
		if pb := e.stripes[idx].pBase[did]; pb != nil && pb.parent != 0 {
			expanded[pb.parent] = true
		}
		if cd := e.stripes[idx].dirs[did]; cd != nil && cd.parent != 0 {
			expanded[cd.parent] = true
		}
	}
	e.metaMu.Unlock()

	pubIDs := make([]InodeID, 0, len(expanded))
	for did := range expanded {
		pubIDs = append(pubIDs, did)
	}
	sortInodeIDs(pubIDs)

	baseMap := make(map[InodeID]*dirSnap, len(pubIDs))
	currMap := make(map[InodeID]*dirSnap, len(pubIDs))
	for _, did := range pubIDs {
		idx := stripeIndex(did)
		if pb := e.stripes[idx].pBase[did]; pb != nil {
			baseMap[did] = pb
		}
		if cd := e.stripes[idx].dirs[did]; cd != nil {
			currMap[did] = snapFromDirState(cd)
		}
	}

	pubEpoch := e.epoch.Add(1) - 1
	pubMaxSeq := e.gseq.Load()
	ops := computeDelta(baseMap, currMap)
	e.epochMu.Unlock()

	if e.cfg.Journal != nil && targetJSeq > 0 {
		if err := e.cfg.Journal.Sync(ctx, targetJSeq); err != nil {
			return err
		}
	}

	if len(ops) > 0 && e.cfg.Target != nil {
		if err := e.cfg.Target.ApplyBatch(ctx, Batch{
			Epoch:  pubEpoch,
			MaxSeq: pubMaxSeq,
			Ops:    ops,
		}); err != nil {
			return err
		}
	}

	if e.cfg.Journal != nil && targetJSeq > 0 {
		if err := e.cfg.Journal.Publish(targetJSeq); err != nil {
			return err
		}
		if err := e.cfg.Journal.Reclaim(); err != nil {
			return err
		}
		e.publishedJSeq.Store(targetJSeq)
	}

	e.epochMu.Lock()
	for _, did := range pubIDs {
		idx := stripeIndex(did)
		if snap, alive := currMap[did]; alive && snap != nil {
			e.stripes[idx].pBase[did] = snap
		} else {
			delete(e.stripes[idx].pBase, did)
		}
		cd := e.stripes[idx].dirs[did]
		if cd == nil || cd.lastModSeq <= pubMaxSeq {
			e.metaMu.Lock()
			delete(e.dirtyP, did)
			e.metaMu.Unlock()
		}
	}
	e.metaMu.Lock()
	if len(e.dirtyP) == 0 {
		e.oldestUnpubAt = time.Time{}
	} else {
		e.oldestUnpubAt = e.nowUTC()
	}
	e.metaMu.Unlock()
	e.epochMu.Unlock()

	e.publishedEpochs.Add(1)
	e.publishedOps.Add(uint64(len(ops)))
	return nil
}

// Snapshot captures a globally consistent reader view across all 64 stripes
// without blocking concurrent readers.
func (e *Engine) Snapshot() *MemTree {
	e.epochMu.RLock()
	for i := range e.stripes {
		e.stripes[i].mu.RLock()
	}

	mt := &MemTree{
		dirs:  make(map[InodeID]*dirSnap),
		files: make(map[InodeID]fileSnap),
	}
	for i := range e.stripes {
		for did, d := range e.stripes[i].dirs {
			snap := snapFromDirState(d)
			mt.dirs[did] = snap
			for fid, fsn := range snap.files {
				mt.files[fid] = fsn
			}
		}
	}

	for i := numStripes - 1; i >= 0; i-- {
		e.stripes[i].mu.RUnlock()
	}
	e.epochMu.RUnlock()
	return mt
}

// Stats returns a point-in-time snapshot of Engine metrics and watermarks.
func (e *Engine) Stats() Stats {
	now := e.nowUTC()
	e.metaMu.Lock()
	oldest := e.oldestUnpubAt
	e.metaMu.Unlock()

	var age time.Duration
	if !oldest.IsZero() && now.After(oldest) {
		age = now.Sub(oldest)
	}

	inodes := e.nextIno.Load()
	if inodes > 0 {
		inodes--
	}

	return Stats{
		GlobalSeq:       e.gseq.Load(),
		Epoch:           e.epoch.Load(),
		Inodes:          inodes,
		RawOps:          e.rawOps.Load(),
		RawBytes:        e.rawBytes.Load(),
		JournalBatches:  e.journalBatches.Load(),
		JournalOps:      e.journalOps.Load(),
		JournalBytes:    e.journalBytes.Load(),
		PublishedEpochs: e.publishedEpochs.Load(),
		PublishedOps:    e.publishedOps.Load(),
		DirtyOps:        max(int64(0), e.dirtyOps.Load()),
		DirtyBytes:      max(int64(0), e.dirtyBytes.Load()),
		LastJournalSeq:  e.lastJSeq.Load(),
		PublishedJSeq:   e.publishedJSeq.Load(),
		UnpublishedAge:  age,
		PublishOverdue:  age >= e.cfg.MaxPublishWindow,
	}
}

// Close marks the Engine closed so subsequent mutations fail with ErrClosed.
func (e *Engine) Close() error {
	e.epochMu.Lock()
	defer e.epochMu.Unlock()

	e.metaMu.Lock()
	e.closed = true
	e.metaMu.Unlock()
	return nil
}
