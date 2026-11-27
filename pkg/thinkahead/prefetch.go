// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package thinkahead

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// PolicyMode selects which prefetching regime an Engine executes.
type PolicyMode string

const (
	// PolicyNoPrefetch disables all speculative prefetching (cold demand-paged LRU).
	PolicyNoPrefetch PolicyMode = "vfs_cold_no_prefetch"

	// PolicySequentialReadahead enables fixed sequential block read-ahead (S7)
	// without trace guidance.
	PolicySequentialReadahead PolicyMode = "vfs_naive_sequential_readahead"

	// PolicyThinkAhead enables learned workflow chunk-sequence prefetching with
	// bounded unread bytes, segmented cache admission, and adaptive confidence gating.
	PolicyThinkAhead PolicyMode = "vfs_thinkahead_trace_prefetch"

	// PolicyThinkAheadNoThrottle disables the adaptive confidence gate so
	// negative-regime benchmarks can quantify naive unconditional replay waste.
	PolicyThinkAheadNoThrottle PolicyMode = "vfs_thinkahead_unthrottled"
)

// ReadOutcome classifies how a foreground demand read was served.
type ReadOutcome uint8

const (
	// OutcomeHit means the chunk was already cached (warm or prefetched in time).
	OutcomeHit ReadOutcome = iota
	// OutcomeLate means the chunk was already being fetched in flight (coalesced).
	OutcomeLate
	// OutcomeMiss means a cold demand GET had to be issued.
	OutcomeMiss
)

// String returns the canonical outcome label from docs/prefetch.md S1.
func (o ReadOutcome) String() string {
	switch o {
	case OutcomeHit:
		return "hit"
	case OutcomeLate:
		return "late"
	default:
		return "miss"
	}
}

// ReadObservation is the non-blocking S1 read event delivered to observers.
// Per S1.3 it carries no file contents, path names, or user identity.
type ReadObservation struct {
	SinceAttach time.Duration
	FileID      uint64
	ChunkIdx    int64
	ChunkID     ChunkID
	Outcome     ReadOutcome
	Wait        time.Duration
}

// PrefetchPlan is an ordered sequence of chunk IDs proposed by the Learner
// for one scoped workflow run.
type PrefetchPlan struct {
	Scope       ScopeKey
	ScopeHash   [32]byte
	Chunks      []ChunkID
	Fingerprint [32]byte
}

// TransitionEdge records a learned Markov-1 transition weight from one chunk
// to its successor in a workflow.
type TransitionEdge struct {
	To     ChunkID
	Weight float64
}

type scopeModel struct {
	runs        int
	decay       float64
	lastOrder   []ChunkID
	chunkWeight map[ChunkID]float64
	transitions map[ChunkID]map[ChunkID]float64
}

// Learner learns ordered chunk sequences and Markov-1 transition chains from
// prior completed workflow runs. All learned state is strictly partitioned by
// ScopeKey.DeriveKey() so cross-tenant leakage is impossible.
type Learner struct {
	mu     sync.RWMutex
	decay  float64
	models map[[32]byte]*scopeModel
}

// NewLearner creates a tenant-isolated sequence learner. decay in (0, 1]
// controls exponential decay of older runs (default 0.7).
func NewLearner(decay float64) *Learner {
	if decay <= 0 || decay > 1 {
		decay = 0.7
	}
	return &Learner{
		decay:  decay,
		models: make(map[[32]byte]*scopeModel),
	}
}

// RecordTrace ingests a completed workflow run into the scope's model.
func (l *Learner) RecordTrace(tr *WorkflowTrace) {
	if tr == nil || len(tr.UniqueOrder) == 0 {
		return
	}
	l.RecordSequence(tr.Scope, tr.UniqueOrder)
}

// RecordSequence ingests a first-touch ordered chunk sequence for scope.
func (l *Learner) RecordSequence(scope ScopeKey, order []ChunkID) {
	if len(order) == 0 {
		return
	}
	sk := scope.DeriveKey()
	l.mu.Lock()
	defer l.mu.Unlock()

	m := l.models[sk]
	if m == nil {
		m = &scopeModel{
			decay:       l.decay,
			chunkWeight: make(map[ChunkID]float64, len(order)),
			transitions: make(map[ChunkID]map[ChunkID]float64, len(order)),
		}
		l.models[sk] = m
	} else {
		// Exponentially decay prior run weights so recent code edits dominate.
		for k, w := range m.chunkWeight {
			nw := w * m.decay
			if nw < 1e-3 {
				delete(m.chunkWeight, k)
			} else {
				m.chunkWeight[k] = nw
			}
		}
		for src, dsts := range m.transitions {
			for dst, w := range dsts {
				nw := w * m.decay
				if nw < 1e-3 {
					delete(dsts, dst)
				} else {
					dsts[dst] = nw
				}
			}
			if len(dsts) == 0 {
				delete(m.transitions, src)
			}
		}
	}

	m.runs++
	m.lastOrder = make([]ChunkID, len(order))
	copy(m.lastOrder, order)

	for i, id := range order {
		m.chunkWeight[id] += 1.0
		if i+1 < len(order) {
			next := order[i+1]
			dsts := m.transitions[id]
			if dsts == nil {
				dsts = make(map[ChunkID]float64, 2)
				m.transitions[id] = dsts
			}
			dsts[next] += 1.0
		}
	}
}

// HasPlan reports whether scope has at least one learned trace.
func (l *Learner) HasPlan(scope ScopeKey) bool {
	sk := scope.DeriveKey()
	l.mu.RLock()
	m := l.models[sk]
	ok := m != nil && len(m.lastOrder) > 0
	l.mu.RUnlock()
	return ok
}

// Plan builds a prioritized PrefetchPlan for scope. The plan preserves the
// latest run's first-touch order (which achieves 96-99% hit rates even under
// parallel build reordering) while capping to maxEntries if > 0.
func (l *Learner) Plan(scope ScopeKey, maxEntries int) *PrefetchPlan {
	sk := scope.DeriveKey()
	l.mu.RLock()
	defer l.mu.RUnlock()

	m := l.models[sk]
	if m == nil || len(m.lastOrder) == 0 {
		return nil
	}
	n := len(m.lastOrder)
	if maxEntries > 0 && n > maxEntries {
		n = maxEntries
	}
	chunks := make([]ChunkID, n)
	copy(chunks, m.lastOrder[:n])
	return &PrefetchPlan{
		Scope:       scope,
		ScopeHash:   sk,
		Chunks:      chunks,
		Fingerprint: sk,
	}
}

// SuggestNext returns up to k highest-weight Markov-1 successor chunks for
// current within scope.
func (l *Learner) SuggestNext(scope ScopeKey, current ChunkID, k int) []ChunkID {
	if k <= 0 {
		return nil
	}
	sk := scope.DeriveKey()
	l.mu.RLock()
	defer l.mu.RUnlock()

	m := l.models[sk]
	if m == nil {
		return nil
	}
	dsts := m.transitions[current]
	if len(dsts) == 0 {
		return nil
	}
	edges := make([]TransitionEdge, 0, len(dsts))
	for to, w := range dsts {
		edges = append(edges, TransitionEdge{To: to, Weight: w})
	}
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].Weight > edges[j].Weight
	})
	if len(edges) > k {
		edges = edges[:k]
	}
	out := make([]ChunkID, len(edges))
	for i, e := range edges {
		out[i] = e.To
	}
	return out
}

// Config configures the bounded node-cache prefetch Engine.
type Config struct {
	Policy              PolicyMode
	MaxCacheBytes       int64
	MaxUnreadBytes      int64
	MaxPrefetchInflight int
	MaxMergeBytes       int64
	MinConfidence       float64
	ConfidenceWindow    int
	ReadaheadMax        int
	VerifyContentHash   bool
	ObserveQueueSize    int
	// FileChunkResolver optionally resolves (fileID, chunkIdx) -> ChunkID for
	// PolicySequentialReadahead (S7).
	FileChunkResolver func(fileID uint64, chunkIdx int64) (ChunkID, bool)
}

func (c Config) withDefaults() Config {
	if c.Policy == "" {
		c.Policy = PolicyThinkAhead
	}
	if c.MaxCacheBytes <= 0 {
		c.MaxCacheBytes = 64 << 20
	}
	if c.MaxUnreadBytes <= 0 {
		c.MaxUnreadBytes = min(256<<20, c.MaxCacheBytes/2)
		if c.MaxUnreadBytes < 256<<10 {
			c.MaxUnreadBytes = c.MaxCacheBytes
		}
	}
	if c.MaxPrefetchInflight <= 0 {
		c.MaxPrefetchInflight = 16
	}
	if c.MaxMergeBytes < 0 {
		c.MaxMergeBytes = 0
	}
	if c.MinConfidence <= 0 {
		c.MinConfidence = 0.25
	}
	if c.ConfidenceWindow <= 0 {
		c.ConfidenceWindow = 32
	}
	if c.ReadaheadMax <= 0 {
		c.ReadaheadMax = 8
	}
	if c.ObserveQueueSize <= 0 {
		c.ObserveQueueSize = 4096
	}
	return c
}

// EngineStats captures end-to-end read latency, cache hit rate, prefetch waste,
// and adaptive throttle telemetry for a workflow run.
type EngineStats struct {
	Policy              PolicyMode `json:"policy"`
	Reads               int64      `json:"reads"`
	Hits                int64      `json:"hits"`
	Lates               int64      `json:"lates"`
	Misses              int64      `json:"misses"`
	HitRate             float64    `json:"hit_rate"`
	EffectiveHitRate    float64    `json:"effective_hit_rate"` // (hits + lates) / reads
	WaitP50Us           float64    `json:"wait_p50_us"`
	WaitP95Us           float64    `json:"wait_p95_us"`
	WaitP99Us           float64    `json:"wait_p99_us"`
	WaitMeanUs          float64    `json:"wait_mean_us"`
	TotalWaitMs         float64    `json:"total_wait_ms"`
	PrefetchIssued      int64      `json:"prefetch_issued"`
	PrefetchBytes       int64      `json:"prefetch_bytes"`
	PrefetchIgnored     int64      `json:"prefetch_ignored"` // unreachable from root (S2.1)
	PrefetchMergedCalls int64      `json:"prefetch_merged_calls"`
	PrefetchConsumed    int64      `json:"prefetch_consumed"`
	WastedBytes         int64      `json:"wasted_bytes"`
	WastedRatio         float64    `json:"wasted_ratio"`
	HashMismatchCount   int64      `json:"hash_mismatch_count"`
	ThrottledEvents     int64      `json:"throttled_events"`
	ThrottledPaused     bool       `json:"throttled_paused"`
	DroppedObservations int64      `json:"dropped_observations"`
	Cache               CacheStats `json:"cache"`
}

type flight struct {
	done       chan struct{}
	data       []byte
	err        error
	prefetched bool
}

type seqTracker struct {
	lastIdx int64
	run     int
	window  int
}

type activePlan struct {
	scopeHash [32]byte
	cancel    context.CancelFunc
	planSet   map[ChunkID]struct{}
	readSet   map[ChunkID]struct{}
	mu        sync.Mutex
}

func (p *activePlan) markRead(id ChunkID) bool {
	p.mu.Lock()
	p.readSet[id] = struct{}{}
	_, inPlan := p.planSet[id]
	p.mu.Unlock()
	return inPlan
}

func (p *activePlan) isRead(id ChunkID) bool {
	p.mu.Lock()
	_, ok := p.readSet[id]
	p.mu.Unlock()
	return ok
}

// Engine executes demand chunk reads and bounded asynchronous prefetching over
// a ChunkFetcher and SegmentedCache.
type Engine struct {
	cfg     Config
	fetcher ChunkFetcher
	cache   *SegmentedCache
	learner *Learner

	attachedAt time.Time

	inflightMu sync.Mutex
	inflight   map[ScopedChunkKey]*flight

	sem chan struct{} // bounds background prefetch in-flight GETs ONLY

	plansMu      sync.Mutex
	plans        map[[32]byte]*activePlan
	pubLagPaused atomic.Bool
	detached     atomic.Bool
	wg           sync.WaitGroup

	// Adaptive confidence tracking (rolling ring buffer).
	confMu      sync.Mutex
	confRing    []bool
	confPos     int
	confCount   int
	confMatches int
	throttled   atomic.Bool

	// Sequential read-ahead state per (scopeHash, fileID).
	raMu sync.Mutex
	ra   map[raKey]*seqTracker

	// Non-blocking S1 observation queue.
	obsCh      chan ReadObservation
	obsDropped atomic.Int64

	// Telemetry counters.
	reads               atomic.Int64
	hits                atomic.Int64
	lates               atomic.Int64
	misses              atomic.Int64
	prefetchIssued      atomic.Int64
	prefetchBytes       atomic.Int64
	prefetchIgnored     atomic.Int64
	prefetchMergedCalls atomic.Int64
	hashMismatches      atomic.Int64
	throttledEvents     atomic.Int64

	waitMu sync.Mutex
	waits  []time.Duration
}

type raKey struct {
	scopeHash [32]byte
	fileID    uint64
}

// NewEngine constructs a bounded ThinkAhead prefetch engine over fetcher.
func NewEngine(fetcher ChunkFetcher, learner *Learner, cfg Config) *Engine {
	cfg = cfg.withDefaults()
	e := &Engine{
		cfg:        cfg,
		fetcher:    fetcher,
		cache:      NewSegmentedCache(cfg.MaxCacheBytes),
		learner:    learner,
		attachedAt: time.Now(),
		inflight:   make(map[ScopedChunkKey]*flight, 128),
		sem:        make(chan struct{}, cfg.MaxPrefetchInflight),
		plans:      make(map[[32]byte]*activePlan),
		confRing:   make([]bool, cfg.ConfidenceWindow),
		ra:         make(map[raKey]*seqTracker),
		obsCh:      make(chan ReadObservation, cfg.ObserveQueueSize),
	}
	return e
}

// Cache exposes the underlying SegmentedCache for inspection and S5 tests.
func (e *Engine) Cache() *SegmentedCache {
	return e.cache
}

// Observations returns the read-only S1 observation channel.
func (e *Engine) Observations() <-chan ReadObservation {
	return e.obsCh
}

func (e *Engine) emitObservation(obs ReadObservation) {
	select {
	case e.obsCh <- obs:
	default:
		// S1.2: never block a foreground read; drop newest and count it.
		e.obsDropped.Add(1)
	}
}

// NotifyUnpublishedAge implements S4.2: when the oldest unpublished write
// exceeds bound/2, all active prefetch plans are cancelled and new prefetches
// pause until the backlog drains.
func (e *Engine) NotifyUnpublishedAge(age, bound time.Duration) {
	if bound <= 0 {
		return
	}
	if age > bound/2 {
		if !e.pubLagPaused.Swap(true) {
			e.cancelAllPlans()
		}
	} else {
		e.pubLagPaused.Store(false)
	}
}

func (e *Engine) cancelAllPlans() {
	e.plansMu.Lock()
	for _, p := range e.plans {
		p.cancel()
	}
	e.plans = make(map[[32]byte]*activePlan)
	e.plansMu.Unlock()
}

// Detach cancels all active prefetch plans and waits for every in-flight
// background prefetch goroutine to exit (S4.3: zero post-detach store requests).
func (e *Engine) Detach() {
	e.detached.Store(true)
	e.cancelAllPlans()
	e.wg.Wait()
}

// StartPlan launches bounded asynchronous trace-guided prefetching for plan
// under scope. Returns a cancel function for this specific plan (S4.1).
func (e *Engine) StartPlan(ctx context.Context, scope ScopeKey, plan *PrefetchPlan) func() {
	if plan == nil || len(plan.Chunks) == 0 {
		return func() {}
	}
	if e.cfg.Policy != PolicyThinkAhead && e.cfg.Policy != PolicyThinkAheadNoThrottle {
		return func() {}
	}
	if e.detached.Load() || e.pubLagPaused.Load() {
		return func() {}
	}
	sh := scope.DeriveKey()
	// Enforce strict scope binding: a plan cannot run under a different tenant/scope.
	if plan.ScopeHash != sh {
		return func() {}
	}

	planCtx, cancel := context.WithCancel(ctx)
	planSet := make(map[ChunkID]struct{}, len(plan.Chunks))
	for _, id := range plan.Chunks {
		planSet[id] = struct{}{}
	}
	ap := &activePlan{
		scopeHash: sh,
		cancel:    cancel,
		planSet:   planSet,
		readSet:   make(map[ChunkID]struct{}, 64),
	}

	e.plansMu.Lock()
	if prev, ok := e.plans[sh]; ok {
		prev.cancel()
	}
	e.plans[sh] = ap
	e.plansMu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.runPlanLoop(planCtx, sh, ap, plan.Chunks)
	}()

	return func() {
		cancel()
		e.plansMu.Lock()
		if cur, ok := e.plans[sh]; ok && cur == ap {
			delete(e.plans, sh)
		}
		e.plansMu.Unlock()
	}
}

func (e *Engine) runPlanLoop(ctx context.Context, sh [32]byte, ap *activePlan, chunks []ChunkID) {
	i := 0
	for i < len(chunks) {
		if ctx.Err() != nil || e.detached.Load() || e.pubLagPaused.Load() {
			return
		}
		if e.cfg.Policy == PolicyThinkAhead && e.throttled.Load() {
			// Adaptive confidence gate triggered: back off cleanly instead of
			// wasting S3 GET bandwidth on a stale/mismatched trace.
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
				continue
			}
		}

		// S3.2: stop starting prefetches while unread bytes >= MaxUnreadBytes.
		for e.cache.UnreadBytes() >= e.cfg.MaxUnreadBytes {
			if ctx.Err() != nil || e.detached.Load() || e.pubLagPaused.Load() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Microsecond):
			}
		}

		id := chunks[i]
		i++

		// S3.3: skip chunks already read by this run or already cached.
		if ap.isRead(id) {
			continue
		}
		ck := ScopedChunkKey{ScopeHash: sh, ID: id}
		if e.cache.Contains(ck) {
			continue
		}
		// S2.1: ignore chunks unreachable from the volume's published root.
		if !e.fetcher.Reachable(id) {
			e.prefetchIgnored.Add(1)
			continue
		}

		// S8: optionally merge adjacent trace entries that are contiguous in
		// the same pack object up to cfg.MaxMergeBytes.
		batch := []ChunkID{id}
		if e.cfg.MaxMergeBytes > 0 {
			if loc0, ok := e.fetcher.Location(id); ok {
				endOff := loc0.Offset + loc0.Length
				totalLen := loc0.Length
				for i < len(chunks) {
					nextID := chunks[i]
					if ap.isRead(nextID) || e.cache.Contains(ScopedChunkKey{ScopeHash: sh, ID: nextID}) {
						break
					}
					locN, okN := e.fetcher.Location(nextID)
					if !okN || locN.PackKey != loc0.PackKey || locN.Offset != endOff || totalLen+locN.Length > e.cfg.MaxMergeBytes {
						break
					}
					batch = append(batch, nextID)
					endOff += locN.Length
					totalLen += locN.Length
					i++
				}
			}
		}

		// Acquire bounded background prefetch slot.
		select {
		case e.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		if len(batch) > 1 {
			e.wg.Add(1)
			go func(ids []ChunkID) {
				defer e.wg.Done()
				defer func() { <-e.sem }()
				e.fetchPrefetchMerged(ctx, sh, ids)
			}(batch)
		} else {
			e.wg.Add(1)
			go func(cid ChunkID) {
				defer e.wg.Done()
				defer func() { <-e.sem }()
				e.fetchPrefetchSingle(ctx, sh, cid)
			}(id)
		}
	}
}

func (e *Engine) fetchPrefetchSingle(ctx context.Context, sh [32]byte, id ChunkID) {
	if ctx.Err() != nil {
		return
	}
	ck := ScopedChunkKey{ScopeHash: sh, ID: id}
	if e.cache.Contains(ck) {
		return
	}

	e.inflightMu.Lock()
	if _, dup := e.inflight[ck]; dup {
		e.inflightMu.Unlock()
		return
	}
	fl := &flight{done: make(chan struct{}), prefetched: true}
	e.inflight[ck] = fl
	e.inflightMu.Unlock()

	pctx := WithPrefetchContext(ctx)
	data, err := e.fetcher.FetchChunk(pctx, id)
	if err == nil && e.cfg.VerifyContentHash {
		if SumChunk(data) != id {
			e.hashMismatches.Add(1)
			err = ErrChunkCorrupt
			data = nil
		}
	}

	if err == nil {
		e.prefetchIssued.Add(1)
		e.prefetchBytes.Add(int64(len(data)))
		e.cache.AddPrefetch(ck, data)
	}

	fl.data = data
	fl.err = err
	close(fl.done)

	e.inflightMu.Lock()
	delete(e.inflight, ck)
	e.inflightMu.Unlock()
}

func (e *Engine) fetchPrefetchMerged(ctx context.Context, sh [32]byte, ids []ChunkID) {
	if ctx.Err() != nil || len(ids) == 0 {
		return
	}
	loc0, ok := e.fetcher.Location(ids[0])
	if !ok {
		return
	}

	// Register flights for all chunks in the batch that are not already in flight.
	flights := make([]*flight, len(ids))
	locs := make([]ChunkLocation, len(ids))
	var totalLen int64
	e.inflightMu.Lock()
	for i, id := range ids {
		loc, ok := e.fetcher.Location(id)
		if !ok {
			e.inflightMu.Unlock()
			return
		}
		locs[i] = loc
		totalLen += loc.Length
		ck := ScopedChunkKey{ScopeHash: sh, ID: id}
		if _, dup := e.inflight[ck]; !dup {
			fl := &flight{done: make(chan struct{}), prefetched: true}
			e.inflight[ck] = fl
			flights[i] = fl
		}
	}
	e.inflightMu.Unlock()

	pctx := WithPrefetchContext(ctx)
	raw, err := e.fetcher.FetchMergedRange(pctx, loc0.PackKey, loc0.Offset, totalLen)
	if err == nil {
		e.prefetchMergedCalls.Add(1)
	}

	var cur int64
	for i, id := range ids {
		fl := flights[i]
		n := locs[i].Length
		var chunkData []byte
		chunkErr := err
		if chunkErr == nil {
			if cur+n <= int64(len(raw)) {
				chunkData = make([]byte, n)
				copy(chunkData, raw[cur:cur+n])
				if e.cfg.VerifyContentHash && SumChunk(chunkData) != id {
					e.hashMismatches.Add(1)
					chunkErr = ErrChunkCorrupt
					chunkData = nil
				}
			} else {
				chunkErr = ErrChunkCorrupt
			}
		}
		cur += n

		ck := ScopedChunkKey{ScopeHash: sh, ID: id}
		if chunkErr == nil {
			e.prefetchIssued.Add(1)
			e.prefetchBytes.Add(int64(len(chunkData)))
			e.cache.AddPrefetch(ck, chunkData)
		}
		if fl != nil {
			fl.data = chunkData
			fl.err = chunkErr
			close(fl.done)
			e.inflightMu.Lock()
			delete(e.inflight, ck)
			e.inflightMu.Unlock()
		}
	}
}

func (e *Engine) recordConfidenceSample(matched bool) {
	if e.cfg.Policy != PolicyThinkAhead {
		return
	}
	e.confMu.Lock()
	if e.confCount == len(e.confRing) {
		if e.confRing[e.confPos] {
			e.confMatches--
		}
	} else {
		e.confCount++
	}
	e.confRing[e.confPos] = matched
	if matched {
		e.confMatches++
	}
	e.confPos = (e.confPos + 1) % len(e.confRing)

	minSamples := min(len(e.confRing)/2, 16)
	if minSamples < 4 {
		minSamples = 4
	}
	if e.confCount >= minSamples {
		conf := float64(e.confMatches) / float64(e.confCount)
		if conf < e.cfg.MinConfidence {
			if !e.throttled.Swap(true) {
				e.throttledEvents.Add(1)
			}
		} else if conf >= e.cfg.MinConfidence*1.5 {
			e.throttled.Store(false)
		}
	}
	e.confMu.Unlock()
}

// ReadChunk serves a foreground demand read for (scope, fileID, chunkIdx, id).
// Foreground demand reads NEVER wait on the prefetch semaphore and immediately
// coalesce with any in-flight prefetch via singleflight (S2.2, S2.3).
func (e *Engine) ReadChunk(ctx context.Context, scope ScopeKey, fileID uint64, chunkIdx int64, id ChunkID) ([]byte, ReadOutcome, time.Duration, error) {
	start := time.Now()
	sh := scope.DeriveKey()
	ck := ScopedChunkKey{ScopeHash: sh, ID: id}

	// Mark read in active plan (S3.3) and feed adaptive confidence monitor.
	e.plansMu.Lock()
	ap := e.plans[sh]
	e.plansMu.Unlock()
	if ap != nil {
		inPlan := ap.markRead(id)
		e.recordConfidenceSample(inPlan)
	}

	// Trigger sequential read-ahead if configured (S7).
	if e.cfg.Policy == PolicySequentialReadahead {
		e.maybeReadahead(ctx, sh, fileID, chunkIdx)
	}

	// Fast path: cache hit.
	if data, ok, _ := e.cache.Get(ck); ok {
		wait := time.Since(start)
		e.recordOutcome(OutcomeHit, wait, fileID, chunkIdx, id)
		return data, OutcomeHit, wait, nil
	}

	// Check singleflight (late hit if prefetch or concurrent demand is in flight).
	e.inflightMu.Lock()
	if fl, inFlight := e.inflight[ck]; inFlight {
		e.inflightMu.Unlock()
		select {
		case <-fl.done:
		case <-ctx.Done():
			return nil, OutcomeLate, time.Since(start), ctx.Err()
		}
		wait := time.Since(start)
		if fl.err != nil {
			e.recordOutcome(OutcomeLate, wait, fileID, chunkIdx, id)
			return nil, OutcomeLate, wait, fl.err
		}
		// Promote entry to protected if it landed in probationary.
		e.cache.Get(ck)
		e.recordOutcome(OutcomeLate, wait, fileID, chunkIdx, id)
		return fl.data, OutcomeLate, wait, nil
	}

	// Cold demand miss: register foreground singleflight and fetch directly
	// without acquiring the background prefetch semaphore.
	fl := &flight{done: make(chan struct{}), prefetched: false}
	e.inflight[ck] = fl
	e.inflightMu.Unlock()

	data, err := e.fetcher.FetchChunk(ctx, id)
	if err == nil && e.cfg.VerifyContentHash {
		if SumChunk(data) != id {
			e.hashMismatches.Add(1)
			err = ErrChunkCorrupt
			data = nil
		}
	}
	if err == nil {
		e.cache.AddDemand(ck, data)
	}

	fl.data = data
	fl.err = err
	close(fl.done)

	e.inflightMu.Lock()
	delete(e.inflight, ck)
	e.inflightMu.Unlock()

	wait := time.Since(start)
	e.recordOutcome(OutcomeMiss, wait, fileID, chunkIdx, id)
	return data, OutcomeMiss, wait, err
}

func (e *Engine) maybeReadahead(ctx context.Context, sh [32]byte, fileID uint64, chunkIdx int64) {
	if e.cfg.FileChunkResolver == nil || e.detached.Load() || e.pubLagPaused.Load() {
		return
	}
	rk := raKey{scopeHash: sh, fileID: fileID}
	e.raMu.Lock()
	st := e.ra[rk]
	if st == nil {
		st = &seqTracker{lastIdx: chunkIdx, run: 1, window: 0}
		e.ra[rk] = st
		e.raMu.Unlock()
		return
	}
	var launch []int64
	if chunkIdx == st.lastIdx+1 {
		st.run++
		// S7.1: start after two consecutive chunks, double up to ReadaheadMax (8).
		if st.run >= 2 {
			if st.window == 0 {
				st.window = 2
			} else if st.window < e.cfg.ReadaheadMax {
				st.window = min(st.window*2, e.cfg.ReadaheadMax)
			}
			for step := 1; step <= st.window; step++ {
				launch = append(launch, chunkIdx+int64(step))
			}
		}
	} else if chunkIdx != st.lastIdx {
		// S7.1: reset window on non-consecutive read.
		st.run = 1
		st.window = 0
	}
	st.lastIdx = chunkIdx
	e.raMu.Unlock()

	for _, nextIdx := range launch {
		if e.cache.UnreadBytes() >= e.cfg.MaxUnreadBytes {
			return
		}
		nextID, ok := e.cfg.FileChunkResolver(fileID, nextIdx)
		if !ok {
			break
		}
		ck := ScopedChunkKey{ScopeHash: sh, ID: nextID}
		if e.cache.Contains(ck) {
			continue
		}
		select {
		case e.sem <- struct{}{}:
			e.wg.Add(1)
			go func(cid ChunkID) {
				defer e.wg.Done()
				defer func() { <-e.sem }()
				e.fetchPrefetchSingle(ctx, sh, cid)
			}(nextID)
		default:
			// Background slots saturated; never block demand read.
			return
		}
	}
}

func (e *Engine) recordOutcome(outcome ReadOutcome, wait time.Duration, fileID uint64, chunkIdx int64, id ChunkID) {
	e.reads.Add(1)
	switch outcome {
	case OutcomeHit:
		e.hits.Add(1)
	case OutcomeLate:
		e.lates.Add(1)
	default:
		e.misses.Add(1)
	}
	e.waitMu.Lock()
	e.waits = append(e.waits, wait)
	e.waitMu.Unlock()

	e.emitObservation(ReadObservation{
		SinceAttach: time.Since(e.attachedAt),
		FileID:      fileID,
		ChunkIdx:    chunkIdx,
		ChunkID:     id,
		Outcome:     outcome,
		Wait:        wait,
	})
}

// Stats computes a complete telemetry snapshot including p50/p95/p99 wait
// latency, hit rate, and wasted prefetch byte ratio.
func (e *Engine) Stats() EngineStats {
	reads := e.reads.Load()
	hits := e.hits.Load()
	lates := e.lates.Load()
	misses := e.misses.Load()
	cs := e.cache.Stats()
	prefBytes := e.prefetchBytes.Load()

	// Total wasted bytes = unread bytes still sitting unused + unread bytes evicted unused.
	wasted := cs.UnreadBytes + cs.EvictedUnusedBytes
	var wastedRatio float64
	if prefBytes > 0 {
		wastedRatio = float64(wasted) / float64(prefBytes)
	}

	e.waitMu.Lock()
	waitsCopy := make([]time.Duration, len(e.waits))
	copy(waitsCopy, e.waits)
	e.waitMu.Unlock()

	p50, p95, p99, mean, totalMs := summarizeDurations(waitsCopy)

	var hitRate, effHitRate float64
	if reads > 0 {
		hitRate = float64(hits) / float64(reads)
		effHitRate = float64(hits+lates) / float64(reads)
	}

	return EngineStats{
		Policy:              e.cfg.Policy,
		Reads:               reads,
		Hits:                hits,
		Lates:               lates,
		Misses:              misses,
		HitRate:             hitRate,
		EffectiveHitRate:    effHitRate,
		WaitP50Us:           p50,
		WaitP95Us:           p95,
		WaitP99Us:           p99,
		WaitMeanUs:          mean,
		TotalWaitMs:         totalMs,
		PrefetchIssued:      e.prefetchIssued.Load(),
		PrefetchBytes:       prefBytes,
		PrefetchIgnored:     e.prefetchIgnored.Load(),
		PrefetchMergedCalls: e.prefetchMergedCalls.Load(),
		PrefetchConsumed:    cs.PrefetchConsumed,
		WastedBytes:         wasted,
		WastedRatio:         wastedRatio,
		HashMismatchCount:   e.hashMismatches.Load(),
		ThrottledEvents:     e.throttledEvents.Load(),
		ThrottledPaused:     e.throttled.Load(),
		DroppedObservations: e.obsDropped.Load(),
		Cache:               cs,
	}
}

func summarizeDurations(ds []time.Duration) (p50Us, p95Us, p99Us, meanUs, totalMs float64) {
	if len(ds) == 0 {
		return 0, 0, 0, 0, 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	var sum int64
	for _, d := range ds {
		sum += d.Nanoseconds()
	}
	pct := func(q float64) float64 {
		idx := int(math.Ceil(q*float64(len(ds)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(ds) {
			idx = len(ds) - 1
		}
		return float64(ds[idx].Nanoseconds()) / 1e3
	}
	p50Us = pct(0.50)
	p95Us = pct(0.95)
	p99Us = pct(0.99)
	meanUs = (float64(sum) / float64(len(ds))) / 1e3
	totalMs = float64(sum) / 1e6
	return
}
