// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package aiturbo implements grouped checkpoint IO planning (AITURBO, FAST '26)
// over the canonical Hanzo VFS backend and fence layers. It shares a single
// bounded-stream scheduler across both ungrouped parallel baselines and grouped
// pack/range execution while preserving immutable object reuse, strict Head
// conditional-PUT fencing via pkg/fence, and crash/abort safety.
package aiturbo

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

var (
	// ErrEmptyBatch is returned when planning zero items.
	ErrEmptyBatch = errors.New("aiturbo: empty batch")

	// ErrInvalidGroupSize is returned when GroupSize is non-positive.
	ErrInvalidGroupSize = errors.New("aiturbo: group size must be > 0")

	// ErrHeadCoalesceForbidden is returned if any data item attempts to address
	// the fenced "head" object. Head updates MUST remain strictly serialized via
	// pkg/fence conditional writes (If-Match).
	ErrHeadCoalesceForbidden = errors.New("aiturbo: Head conditional PUT cannot be coalesced into data batches")

	// ErrInvalidRound is returned when a fenced commit uses round 0.
	ErrInvalidRound = errors.New("aiturbo: commit requires non-zero round")

	// ErrCorruptObject is returned when a fetched shard, pack slice, or
	// reused immutable object fails digest or size verification.
	ErrCorruptObject = errors.New("aiturbo: object integrity verification failed")
)

const (
	// DefaultGroupSize is the default number of small/medium items packed per
	// grouped pack object.
	DefaultGroupSize = 8

	// DefaultTargetGroupBytes mirrors canonical vfs packTarget (16 MiB) while staying
	// strictly under S3 single-request limits.
	DefaultTargetGroupBytes = 16 << 20

	// DefaultLargeBypassBytes routes items at or above this threshold directly to
	// their own immutable pack object without copying into a coalescing buffer.
	DefaultLargeBypassBytes = 4 << 20

	// maxHeadSize matches canonical vfs maxHead (64 KiB) for fenced checkpoint heads.
	maxHeadSize = 64 << 10

	// maxManifestBytes bounds the size of an immutable checkpoint manifest shard.
	maxManifestBytes = 8 << 20

	manifestMagic = "aiturbo-manifest-v1\n"
	headKey       = "head"
)

// Item represents one logical object entry belonging to a checkpoint.
type Item struct {
	Key  string
	Data []byte
	Done func()
}

// EntryRef records where an item lives inside canonical pack/<2hex>/<id> objects
// and its exact BLAKE3 content digest.
type EntryRef struct {
	ItemKey string
	PackKey string
	Offset  uint64
	Length  uint64
	Digest  [blake3.Size]byte
}

// PackObject is one immutable packed bundle uploaded via PutIfAbsent.
type PackObject struct {
	Key          string
	Payload      []byte
	DirectBypass bool
	Entries      []EntryRef
	Callbacks    []func()
}

// Manifest is the immutable checkpoint metadata shard written under meta/<2hex>/<hash>
// describing every logical Item and its exact pack placement.
type Manifest struct {
	Seq     uint64
	Entries []EntryRef
}

// Encode serializes m deterministically so identical content produces identical hashes.
func (m Manifest) Encode() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(manifestMagic)
	var u64 [8]byte
	binary.BigEndian.PutUint64(u64[:], m.Seq)
	buf.Write(u64[:])
	binary.BigEndian.PutUint64(u64[:], uint64(len(m.Entries)))
	buf.Write(u64[:])
	for _, e := range m.Entries {
		if len(e.ItemKey) > backend.MaxKey || len(e.PackKey) > backend.MaxKey {
			return nil, fmt.Errorf("%w: manifest key length exceeds %d", backend.ErrKey, backend.MaxKey)
		}
		var u16 [2]byte
		binary.BigEndian.PutUint16(u16[:], uint16(len(e.ItemKey)))
		buf.Write(u16[:])
		buf.WriteString(e.ItemKey)
		binary.BigEndian.PutUint16(u16[:], uint16(len(e.PackKey)))
		buf.Write(u16[:])
		buf.WriteString(e.PackKey)
		binary.BigEndian.PutUint64(u64[:], e.Offset)
		buf.Write(u64[:])
		binary.BigEndian.PutUint64(u64[:], e.Length)
		buf.Write(u64[:])
		buf.Write(e.Digest[:])
	}
	if buf.Len() > maxManifestBytes {
		return nil, fmt.Errorf("%w: checkpoint manifest %d bytes exceeds limit %d", backend.ErrTooLarge, buf.Len(), maxManifestBytes)
	}
	return buf.Bytes(), nil
}

// DecodeManifest deserializes a sealed manifest shard.
func DecodeManifest(raw []byte) (Manifest, error) {
	if !bytes.HasPrefix(raw, []byte(manifestMagic)) {
		return Manifest{}, fmt.Errorf("%w: bad manifest magic", ErrCorruptObject)
	}
	r := bytes.NewReader(raw[len(manifestMagic):])
	var m Manifest
	var u64 [8]byte
	if _, err := io.ReadFull(r, u64[:]); err != nil {
		return Manifest{}, fmt.Errorf("%w: truncated seq: %v", ErrCorruptObject, err)
	}
	m.Seq = binary.BigEndian.Uint64(u64[:])
	if _, err := io.ReadFull(r, u64[:]); err != nil {
		return Manifest{}, fmt.Errorf("%w: truncated count: %v", ErrCorruptObject, err)
	}
	cnt := binary.BigEndian.Uint64(u64[:])
	if cnt > uint64(len(raw)) {
		return Manifest{}, fmt.Errorf("%w: absurd manifest entry count %d", ErrCorruptObject, cnt)
	}
	m.Entries = make([]EntryRef, cnt)
	for i := range m.Entries {
		var u16 [2]byte
		if _, err := io.ReadFull(r, u16[:]); err != nil {
			return Manifest{}, fmt.Errorf("%w: truncated item key len: %v", ErrCorruptObject, err)
		}
		ik := make([]byte, binary.BigEndian.Uint16(u16[:]))
		if _, err := io.ReadFull(r, ik); err != nil {
			return Manifest{}, fmt.Errorf("%w: truncated item key: %v", ErrCorruptObject, err)
		}
		if _, err := io.ReadFull(r, u16[:]); err != nil {
			return Manifest{}, fmt.Errorf("%w: truncated pack key len: %v", ErrCorruptObject, err)
		}
		pk := make([]byte, binary.BigEndian.Uint16(u16[:]))
		if _, err := io.ReadFull(r, pk); err != nil {
			return Manifest{}, fmt.Errorf("%w: truncated pack key: %v", ErrCorruptObject, err)
		}
		var e EntryRef
		e.ItemKey = string(ik)
		e.PackKey = string(pk)
		if _, err := io.ReadFull(r, u64[:]); err != nil {
			return Manifest{}, fmt.Errorf("%w: truncated offset: %v", ErrCorruptObject, err)
		}
		e.Offset = binary.BigEndian.Uint64(u64[:])
		if _, err := io.ReadFull(r, u64[:]); err != nil {
			return Manifest{}, fmt.Errorf("%w: truncated length: %v", ErrCorruptObject, err)
		}
		e.Length = binary.BigEndian.Uint64(u64[:])
		if _, err := io.ReadFull(r, e.Digest[:]); err != nil {
			return Manifest{}, fmt.Errorf("%w: truncated digest: %v", ErrCorruptObject, err)
		}
		m.Entries[i] = e
	}
	if r.Len() != 0 {
		return Manifest{}, fmt.Errorf("%w: trailing manifest bytes", ErrCorruptObject)
	}
	return m, nil
}

// Plan holds the planned immutable packs and metadata manifest for one checkpoint.
type Plan struct {
	TotalItems       int
	GroupSize        int
	TargetGroupBytes int64
	Packs            []PackObject
	Manifest         Manifest
	ManifestKey      string
	ManifestBytes    []byte
	TotalItemBytes   int64
}

// Options configures grouped planning and bounded parallel IO scheduling.
type Options struct {
	GroupSize          int           // Items per grouped pack (1 = ungrouped matched baseline; default 8)
	TargetGroupBytes   int64         // Target packed object size in bytes (default 16 MiB)
	LargeBypassBytes   int64         // Items >= this route direct without coalescing buffer (default 4 MiB)
	Streams            int           // Concurrent object-store streams (default 16)
	CoalesceGapBytes   int64         // Max byte gap coalesced during grouped range download (default 64 KiB)
	MaxCoalescedRange  int64         // Max byte span per single GetRange call (default 16 MiB)
	SimRTT             time.Duration // Optional explicit delay only for RTT sensitivity experiments
	StatBeforePutReuse bool          // Issue Stat check before PutIfAbsent for already-resident immutable objects
}

func (o Options) withDefaults() Options {
	if o.GroupSize <= 0 {
		o.GroupSize = DefaultGroupSize
	}
	if o.TargetGroupBytes <= 0 {
		o.TargetGroupBytes = DefaultTargetGroupBytes
	}
	if o.LargeBypassBytes <= 0 {
		o.LargeBypassBytes = DefaultLargeBypassBytes
	}
	if o.Streams <= 0 {
		o.Streams = 16
	}
	if o.CoalesceGapBytes <= 0 {
		o.CoalesceGapBytes = 64 << 10
	}
	if o.MaxCoalescedRange <= 0 {
		o.MaxCoalescedRange = DefaultTargetGroupBytes
	}
	return o
}

func packKeyForDigest(sum [blake3.Size]byte) string {
	h := hex.EncodeToString(sum[:16])
	return "pack/" + h[:2] + "/" + h
}

func metaKeyForDigest(sum [blake3.Size]byte) string {
	h := hex.EncodeToString(sum[:])
	return "meta/" + h[:2] + "/" + h
}

// Planner builds deterministic, content-addressed grouped checkpoint IO plans.
type Planner struct {
	opt Options
}

// NewPlanner constructs a grouped checkpoint IO planner.
func NewPlanner(opt Options) (*Planner, error) {
	return &Planner{opt: opt.withDefaults()}, nil
}

// PlanBatches partitions sorted items into immutable PackObjects plus a content-addressed
// manifest shard. "head" keys are strictly refused (`ErrHeadCoalesceForbidden`).
// Note: when GroupSize == 1, every item lands in its own independent pack object while
// passing through the exact same execution path (matched parallel baseline).
func (p *Planner) PlanBatches(items []Item) (*Plan, error) {
	return p.PlanCheckpoint(1, items)
}

// PlanCheckpoint constructs a deterministic grouped plan tagged with checkpoint sequence seq.
func (p *Planner) PlanCheckpoint(seq uint64, items []Item) (*Plan, error) {
	if len(items) == 0 {
		return nil, ErrEmptyBatch
	}
	sorted := make([]Item, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	seen := make(map[string]struct{}, len(sorted))
	plan := &Plan{
		TotalItems:       len(sorted),
		GroupSize:        p.opt.GroupSize,
		TargetGroupBytes: p.opt.TargetGroupBytes,
	}

	var entries []EntryRef
	type pending struct {
		item   Item
		digest [blake3.Size]byte
	}
	var cur []pending
	var curBytes int64

	flushCurrent := func() {
		if len(cur) == 0 {
			return
		}
		buf := make([]byte, 0, curBytes)
		refs := make([]EntryRef, 0, len(cur))
		cbs := make([]func(), 0, len(cur))
		for _, c := range cur {
			off := uint64(len(buf))
			buf = append(buf, c.item.Data...)
			refs = append(refs, EntryRef{
				ItemKey: c.item.Key,
				Offset:  off,
				Length:  uint64(len(c.item.Data)),
				Digest:  c.digest,
			})
			if c.item.Done != nil {
				cbs = append(cbs, c.item.Done)
			}
		}
		packSum := blake3.Sum(buf)
		pk := packKeyForDigest(packSum)
		for i := range refs {
			refs[i].PackKey = pk
		}
		plan.Packs = append(plan.Packs, PackObject{
			Key:          pk,
			Payload:      buf,
			DirectBypass: false,
			Entries:      refs,
			Callbacks:    cbs,
		})
		entries = append(entries, refs...)
		cur = nil
		curBytes = 0
	}

	for _, it := range sorted {
		if it.Key == headKey || strings.HasPrefix(it.Key, headKey+"/") {
			return nil, ErrHeadCoalesceForbidden
		}
		if err := backend.ValidKey(it.Key); err != nil {
			return nil, err
		}
		if _, dup := seen[it.Key]; dup {
			return nil, fmt.Errorf("%w: duplicate item key %q", backend.ErrKey, it.Key)
		}
		seen[it.Key] = struct{}{}

		sum := blake3.Sum(it.Data)
		n := int64(len(it.Data))
		plan.TotalItemBytes += n

		// Large objects or baseline GroupSize==1 bypass copy-coalescing directly.
		if p.opt.GroupSize == 1 || n >= p.opt.LargeBypassBytes || n > p.opt.TargetGroupBytes {
			flushCurrent()
			pk := packKeyForDigest(sum)
			ref := EntryRef{
				ItemKey: it.Key,
				PackKey: pk,
				Offset:  0,
				Length:  uint64(n),
				Digest:  sum,
			}
			var cbs []func()
			if it.Done != nil {
				cbs = []func(){it.Done}
			}
			plan.Packs = append(plan.Packs, PackObject{
				Key:          pk,
				Payload:      it.Data,
				DirectBypass: true,
				Entries:      []EntryRef{ref},
				Callbacks:    cbs,
			})
			entries = append(entries, ref)
			continue
		}

		if len(cur) > 0 && (len(cur) >= p.opt.GroupSize || curBytes+n > p.opt.TargetGroupBytes) {
			flushCurrent()
		}
		cur = append(cur, pending{item: it, digest: sum})
		curBytes += n
	}
	flushCurrent()

	manifest := Manifest{Seq: seq, Entries: entries}
	rawManifest, err := manifest.Encode()
	if err != nil {
		return nil, err
	}
	manSum := blake3.Sum(rawManifest)
	plan.Manifest = manifest
	plan.ManifestKey = metaKeyForDigest(manSum)
	plan.ManifestBytes = rawManifest
	return plan, nil
}

// Metrics summarizes an executed checkpoint upload or download strictly from
// measured wall-clock time and real backend WireStats.
type Metrics struct {
	TotalItemBytes int64
	TotalObjects   int
	PlannedPacks   int
	Duration       time.Duration
	BytesPerSec    float64
	Wire           WireStats
}

// GroupedUploader executes checkpoint Plans against a canonical backend and
// commits the fenced "head" strictly last using pkg/fence CAS.
type GroupedUploader struct {
	be      backend.Backend
	counter *CountingBackend
	ownCnt  bool
	fence   *fence.Store
	opt     Options
	planner *Planner
}

// NewGroupedUploader creates an uploader wrapping be cleanly. If be is already
// a *CountingBackend it records directly into it; otherwise it creates one.
func NewGroupedUploader(be backend.Backend, opt Options) (*GroupedUploader, error) {
	if be == nil {
		return nil, errors.New("aiturbo: nil backend")
	}
	opt = opt.withDefaults()
	p, err := NewPlanner(opt)
	if err != nil {
		return nil, err
	}
	cnt, ok := be.(*CountingBackend)
	own := false
	if !ok {
		cnt = NewCountingBackend(be, opt.SimRTT)
		own = true
	}
	return &GroupedUploader{
		be:      cnt,
		counter: cnt,
		ownCnt:  own,
		fence:   fence.New(cnt, maxHeadSize),
		opt:     opt,
		planner: p,
	}, nil
}

// Planner returns the planner configured on u.
func (u *GroupedUploader) Planner() *Planner {
	return u.planner
}

// Counter returns the real wire counter wrapping the backend.
func (u *GroupedUploader) Counter() *CountingBackend {
	return u.counter
}

// UploadAndCommit publishes plan's immutable packs and manifest concurrently,
// reuses any already-present immutable objects on PutIfAbsent conflict (without
// deleting shared objects on failure), and ONLY then commits head at `round`
// via fence.Store CAS.
func (u *GroupedUploader) UploadAndCommit(ctx context.Context, plan *Plan, round uint64) (*Metrics, error) {
	if plan == nil {
		return nil, ErrEmptyBatch
	}
	if round == 0 {
		return nil, ErrInvalidRound
	}
	if u.ownCnt {
		u.counter.Reset()
	}
	start := time.Now()
	if err := u.uploadObjects(ctx, plan); err != nil {
		return nil, err
	}

	// Head is NEVER coalesced and NEVER uploaded until all packs and manifest shards succeed.
	if err := u.fence.Put(ctx, headKey, []byte(plan.ManifestKey), round); err != nil {
		return nil, fmt.Errorf("aiturbo: commit head at round %d: %w", round, err)
	}

	dur := time.Since(start)
	if dur <= 0 {
		dur = time.Nanosecond
	}
	ws := u.counter.Snapshot()
	return &Metrics{
		TotalItemBytes: plan.TotalItemBytes,
		TotalObjects:   plan.TotalItems,
		PlannedPacks:   len(plan.Packs),
		Duration:       dur,
		BytesPerSec:    float64(plan.TotalItemBytes) / dur.Seconds(),
		Wire:           ws,
	}, nil
}

// UploadPlan uploads the pack and manifest objects cleanly without touching "head"
// (useful when the caller drives Head fencing externally via vfs.Volume).
func (u *GroupedUploader) UploadPlan(ctx context.Context, plan *Plan) (*Metrics, error) {
	if plan == nil {
		return nil, ErrEmptyBatch
	}
	if u.ownCnt {
		u.counter.Reset()
	}
	start := time.Now()
	if err := u.uploadObjects(ctx, plan); err != nil {
		return nil, err
	}
	dur := time.Since(start)
	if dur <= 0 {
		dur = time.Nanosecond
	}
	ws := u.counter.Snapshot()
	return &Metrics{
		TotalItemBytes: plan.TotalItemBytes,
		TotalObjects:   plan.TotalItems,
		PlannedPacks:   len(plan.Packs),
		Duration:       dur,
		BytesPerSec:    float64(plan.TotalItemBytes) / dur.Seconds(),
		Wire:           ws,
	}, nil
}

func (u *GroupedUploader) uploadObjects(ctx context.Context, plan *Plan) error {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, u.opt.Streams)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	// Deduplicate identical pack keys within one plan cleanly before scheduling.
	uniquePacks := make([]PackObject, 0, len(plan.Packs))
	seenPack := make(map[string]struct{}, len(plan.Packs))
	for _, pk := range plan.Packs {
		if _, ok := seenPack[pk.Key]; ok {
			for _, cb := range pk.Callbacks {
				if cb != nil {
					cb()
				}
			}
			continue
		}
		seenPack[pk.Key] = struct{}{}
		uniquePacks = append(uniquePacks, pk)
	}

	for _, pk := range uniquePacks {
		select {
		case sem <- struct{}{}:
		case <-workCtx.Done():
			setErr(workCtx.Err())
			goto waitWorkers
		}

		wg.Add(1)
		go func(packObj PackObject) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := u.putImmutable(workCtx, packObj.Key, packObj.Payload); err != nil {
				setErr(err)
				return
			}
			for _, cb := range packObj.Callbacks {
				if cb != nil {
					cb()
				}
			}
		}(pk)
	}

waitWorkers:
	wg.Wait()
	if firstErr != nil {
		// Safety invariant: never delete uploaded content-addressed objects on abort so
		// concurrent publishers referencing shared packs never suffer use-after-delete.
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Upload immutable manifest shard strictly after packs land.
	if err := u.putImmutable(ctx, plan.ManifestKey, plan.ManifestBytes); err != nil {
		return fmt.Errorf("aiturbo: upload manifest %q: %w", plan.ManifestKey, err)
	}
	return nil
}

// putImmutable writes a content-addressed object via PutIfAbsent. If it already exists
// (`ErrConflict`), verify size equality via Stat before treating it as safe immutable reuse.
func (u *GroupedUploader) putImmutable(ctx context.Context, key string, data []byte) error {
	if u.opt.StatBeforePutReuse {
		if sz, err := u.be.Stat(ctx, key); err == nil {
			if sz != int64(len(data)) {
				return fmt.Errorf("%w: existing immutable object %q size=%d want=%d", ErrCorruptObject, key, sz, len(data))
			}
			u.counter.RecordReuse(sz)
			return nil
		}
	}
	_, err := u.be.PutIfAbsent(ctx, key, data)
	if err == nil {
		return nil
	}
	if errors.Is(err, backend.ErrConflict) {
		sz, serr := u.be.Stat(ctx, key)
		if serr != nil {
			return fmt.Errorf("aiturbo: verify reused object %q: %w", key, serr)
		}
		if sz != int64(len(data)) {
			return fmt.Errorf("%w: conflicting immutable object %q size=%d want=%d", ErrCorruptObject, key, sz, len(data))
		}
		u.counter.RecordReuse(sz)
		return nil
	}
	return fmt.Errorf("aiturbo: put %q: %w", key, err)
}

// HeadStore exposes a fence.Store bound to the uploader's backend so callers can
// explicitly invoke Claim / Round / Get during failover tests.
func (u *GroupedUploader) HeadStore() *fence.Store {
	return u.fence
}
