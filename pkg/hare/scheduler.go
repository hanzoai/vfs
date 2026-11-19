// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package hare implements cache-centric multi-resource per-tenant IO scheduling
// across 32-128 co-located sandboxes on a node for hanzoai/vfs (HIP-1342,
// Refs #23), inspired by HARE (USENIX FAST 2026).
//
// A visor host running dozens of sandboxes shares four coupled node budgets:
// local NVMe journal capacity, decoded shard/chunk cache memory, S3 network
// bandwidth, and upload/chunking worker slots. Unlike static 1/N partitioning
// (which strands idle cache and journal space) or unpartitioned FIFO/LRU
// sharing (where scan or bulk-ingest noisy neighbours evict interactive
// working sets and trigger global journal slowdowns), HARE jointly allocates
// cache by online marginal miss-bandwidth reduction (Miss-Ratio Curve) and
// schedules worker slots, journal bytes, and S3 network tokens via start-time
// fair queuing (SFQ) with publication-deadline boost (<= 5 s bound), using
// caller/injected clocks and zero background polling goroutines.
package hare

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	ghostRingSize   = 256
	mrcBuckets      = 16
	minTenantWeight = 1
)

var (
	// ErrUnknownTenant is returned for operations referencing an unregistered sandbox.
	ErrUnknownTenant = errors.New("hare: unknown tenant")

	// ErrJournalQuota is returned when a tenant exhausts its dynamic journal ceiling
	// and cannot borrow from the shared pool without violating node safety.
	ErrJournalQuota = errors.New("hare: tenant journal quota exceeded")

	// ErrInvalidConfig is returned when NodeConfig carries non-positive capacities.
	ErrInvalidConfig = errors.New("hare: invalid node configuration")
)

// Clock supplies wall time without background polling goroutines (LLM.md Rule 4).
type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// NodeConfig specifies the aggregate node capacities shared across sandboxes.
type NodeConfig struct {
	// JournalCapacity is total local NVMe journal space in bytes across all tenants.
	JournalCapacity int64

	// CacheCapacity is total decoded chunk/shard cache space in bytes across all tenants.
	CacheCapacity int64

	// NetBandwidthBps is sustained aggregate S3 network throughput in bytes/sec.
	NetBandwidthBps int64

	// NetBurstBytes is maximum instantaneous burst credit before rate pacing engages.
	NetBurstBytes int64

	// WorkerSlots is maximum concurrent IO/upload worker slots across the node.
	WorkerSlots int

	// PublishBound is the target S3 publication SLA (default 5 s per HIP-1342).
	PublishBound time.Duration

	// Clock overrides time.Now in deterministic simulations and unit tests.
	Clock Clock
}

func (c NodeConfig) valid() (NodeConfig, error) {
	if c.JournalCapacity <= 0 || c.CacheCapacity <= 0 || c.NetBandwidthBps <= 0 || c.WorkerSlots <= 0 {
		return c, ErrInvalidConfig
	}
	if c.NetBurstBytes <= 0 {
		c.NetBurstBytes = max(64<<20, c.NetBandwidthBps/10)
	}
	if c.PublishBound <= 0 {
		c.PublishBound = 5 * time.Second
	}
	if c.Clock == nil {
		c.Clock = wallClock{}
	}
	return c, nil
}

// OpClass categorizes a tenant IO request for scheduling and accounting.
type OpClass uint8

const (
	OpMetaRead OpClass = iota + 1
	OpChunkRead
	OpJournalAppend
	OpPublishPut
)

// TenantAllocation is a snapshot of a sandbox's current four-resource share
// and observed telemetry.
type TenantAllocation struct {
	ID              string
	Weight          int
	CacheQuota      int64
	CacheUsed       int64
	JournalQuota    int64
	JournalUsed     int64
	NetShareBps     int64
	WorkerGuarantee int
	ActiveWorkers   int
	Hits            uint64
	Misses          uint64
	BytesMoved      uint64
	ThrottleMicros  uint64
	MarginalUtility float64
}

type ghostEntry struct {
	blockKey uint64
	stamp    uint64
}

// mrcTracker estimates a tenant's reuse distance distribution in O(1) per
// access using a fixed-size ghost ring buffer over 64 KiB logical blocks.
type mrcTracker struct {
	accessSeq uint64
	ring      [ghostRingSize]ghostEntry
	ringPos   int
	index     map[uint64]uint64
	distHist  [mrcBuckets]uint64
	hits      uint64
	misses    uint64
}

func newMRCTracker() mrcTracker {
	return mrcTracker{index: make(map[uint64]uint64, ghostRingSize)}
}

func (m *mrcTracker) observe(blockKey uint64, cacheBlocks int64) (hit bool) {
	m.accessSeq++
	prev, ok := m.index[blockKey]
	if ok {
		dist := m.accessSeq - prev
		bucket := min(int(dist/4), mrcBuckets-1)
		m.distHist[bucket]++
		if int64(dist) <= max(1, cacheBlocks) {
			hit = true
			m.hits++
		} else {
			m.misses++
		}
	} else {
		m.misses++
	}

	evict := m.ring[m.ringPos]
	if evict.stamp != 0 && m.index[evict.blockKey] == evict.stamp {
		delete(m.index, evict.blockKey)
	}
	m.ring[m.ringPos] = ghostEntry{blockKey: blockKey, stamp: m.accessSeq}
	m.ringPos = (m.ringPos + 1) % ghostRingSize
	m.index[blockKey] = m.accessSeq
	return hit
}

// marginalGain estimates miss-rate reduction if this tenant receives extra
// cache blocks beyond currentBlocks. Pure streaming/scan workloads have near-zero
// reuse inside ghostRingSize and therefore return ~0, preventing cache pollution.
func (m *mrcTracker) marginalGain(currentBlocks int64) float64 {
	total := m.hits + m.misses
	if total < 8 {
		return 1.0
	}
	curBucket := min(max(int(currentBlocks/4), 0), mrcBuckets-1)
	nextBucket := min(curBucket+3, mrcBuckets)
	var capturable uint64
	for b := curBucket; b < nextBucket; b++ {
		capturable += m.distHist[b]
	}
	// Smooth Laplace prior keeps lightly active interactive sandboxes responsive.
	return float64(capturable) / float64(total+16)
}

type tenantState struct {
	id     string
	weight int

	cacheQuota   int64
	cacheUsed    int64
	journalQuota int64
	journalUsed  int64
	oldestAck    time.Time

	netShareBps   int64
	lastFinishTag float64
	activeWorkers int

	mrc            mrcTracker
	bytesMoved     uint64
	opsCompleted   uint64
	throttleMicros uint64
	utility        float64
}

type sfqWaiter struct {
	tenantID string
	startTag float64
	urgent   bool
	ready    chan struct{}
}

// Ticket represents an admitted worker + bandwidth slot that MUST be released
// via Done when the IO operation completes.
type Ticket struct {
	sched    *Scheduler
	tenantID string
	PaceWait time.Duration
	released bool
}

// Done releases the worker slot held by Ticket.
func (tk *Ticket) Done() {
	if tk == nil || tk.released {
		return
	}
	tk.released = true
	tk.sched.releaseWorker(tk.tenantID)
}

// Scheduler coordinates cache, journal, network bandwidth, and worker slots
// across all registered sandboxes on one host node.
type Scheduler struct {
	cfg NodeConfig

	mu          sync.Mutex
	tenants     map[string]*tenantState
	totalWeight int
	virtualTime float64
	inFlight    int
	waitQueue   []*sfqWaiter

	// Aggregate node token bucket for S3 network shaping (updated lazily on Now()).
	nodeTokens float64
	lastTokenT time.Time
	totalJUsed int64
}

// NewScheduler constructs a multi-resource HARE Scheduler for a host node.
func NewScheduler(cfg NodeConfig) (*Scheduler, error) {
	cfg, err := cfg.valid()
	if err != nil {
		return nil, err
	}
	now := cfg.Clock.Now()
	return &Scheduler{
		cfg:        cfg,
		tenants:    make(map[string]*tenantState),
		nodeTokens: float64(cfg.NetBurstBytes),
		lastTokenT: now,
	}, nil
}

// RegisterTenant adds or updates a sandbox tenant with relative weight >= 1
// and triggers an O(N) cache-centric multi-resource rebalance.
func (s *Scheduler) RegisterTenant(id string, weight int) {
	if weight < minTenantWeight {
		weight = minTenantWeight
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if t, ok := s.tenants[id]; ok {
		s.totalWeight += weight - t.weight
		t.weight = weight
	} else {
		s.tenants[id] = &tenantState{
			id:     id,
			weight: weight,
			mrc:    newMRCTracker(),
		}
		s.totalWeight += weight
	}
	s.rebalanceLocked()
}

// UnregisterTenant removes a stopped sandbox and redistributes its quotas.
func (s *Scheduler) UnregisterTenant(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tenants[id]
	if !ok {
		return
	}
	s.totalWeight -= t.weight
	s.totalJUsed -= t.journalUsed
	delete(s.tenants, id)
	s.rebalanceLocked()
}

// ObserveBlockAccess records a 64 KiB logical block reference for tenant id,
// updating its online Miss-Ratio Curve and returning whether the access hits
// within the tenant's currently assigned cache quota.
func (s *Scheduler) ObserveBlockAccess(id string, blockKey uint64, blockBytes int64) (hit bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tenants[id]
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrUnknownTenant, id)
	}
	const minBlock int64 = 64 << 10
	unit := max(minBlock, blockBytes)
	quotaBlocks := max(1, t.cacheQuota/unit)
	hit = t.mrc.observe(blockKey, quotaBlocks)
	if !hit {
		t.cacheUsed = min(t.cacheQuota, t.cacheUsed+unit)
	}
	if (t.mrc.hits+t.mrc.misses)%64 == 0 {
		s.rebalanceLocked()
	}
	return hit, nil
}

// Rebalance recomputes per-tenant cache, journal, and S3 network shares using
// HARE's cache-centric marginal utility allocation + dominant-resource fairness.
func (s *Scheduler) Rebalance() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebalanceLocked()
}

func (s *Scheduler) rebalanceLocked() {
	n := len(s.tenants)
	if n == 0 || s.totalWeight <= 0 {
		return
	}

	// 1. Cache allocation: every tenant receives a scan-resistant floor (25% of
	// weighted proportional share) so no tenant is starved; the remaining 75%
	// elastic pool is distributed proportionally to weight * (epsilon + marginalGain),
	// shifting cache away from zero-reuse scan noisy neighbours to high-reuse
	// interactive toolchains.
	const floorFrac = 0.25
	elasticPool := float64(s.cfg.CacheCapacity) * (1.0 - floorFrac)

	var sumScore float64
	ids := make([]string, 0, n)
	for id := range s.tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	const blockUnit int64 = 64 << 10
	scores := make([]float64, n)
	for i, id := range ids {
		t := s.tenants[id]
		curBlocks := max(1, t.cacheQuota/blockUnit)
		u := t.mrc.marginalGain(curBlocks)
		t.utility = u
		sc := float64(t.weight) * (0.02 + u)
		scores[i] = sc
		sumScore += sc
	}

	var assignedCache int64
	for i, id := range ids {
		t := s.tenants[id]
		wFrac := float64(t.weight) / float64(s.totalWeight)
		floorBytes := int64(float64(s.cfg.CacheCapacity) * floorFrac * wFrac)
		var bonus int64
		if sumScore > 0 {
			bonus = int64(elasticPool * (scores[i] / sumScore))
		}
		q := max(64<<10, floorBytes+bonus)
		t.cacheQuota = q
		if t.cacheUsed > q {
			t.cacheUsed = q
		}
		assignedCache += q
	}
	// Trim or hand out rounding remainder on the first tenant so sum <= CacheCapacity.
	if diff := s.cfg.CacheCapacity - assignedCache; diff != 0 && len(ids) > 0 {
		first := s.tenants[ids[0]]
		if first.cacheQuota+diff >= 64<<10 {
			first.cacheQuota += diff
		}
	}

	// 2. Journal & S3 Network shares: coupled with cache miss pressure and
	// publication-age urgency so tenants approaching the 5 s S3 publication SLA
	// get priority drain bandwidth while noisy writers are capped before pushing
	// the node past journal slowFrom (75%).
	now := s.cfg.Clock.Now()
	var sumNetWeight float64
	netWeights := make([]float64, n)
	for i, id := range ids {
		t := s.tenants[id]
		wFrac := float64(t.weight) / float64(s.totalWeight)
		// Dynamic journal ceiling allows an active tenant to burst up to 4x its
		// static share while total node usage is low, tightening toward 1.25x as
		// total node journal fill rises toward 75%.
		nodeFill := float64(s.totalJUsed) / float64(s.cfg.JournalCapacity)
		burstMult := max(1.25, 4.0-3.0*nodeFill)
		jCeil := int64(math.Min(float64(s.cfg.JournalCapacity)*0.50, float64(s.cfg.JournalCapacity)*wFrac*burstMult))
		t.journalQuota = max(256<<10, jCeil)

		nw := float64(t.weight)
		if !t.oldestAck.IsZero() && now.Sub(t.oldestAck) > s.cfg.PublishBound/2 {
			// Boost S3 publication drain share for tenants past 50% of the 5 s bound.
			nw *= 2.5
		}
		netWeights[i] = nw
		sumNetWeight += nw
	}

	for i, id := range ids {
		t := s.tenants[id]
		t.netShareBps = max(64<<10, int64(float64(s.cfg.NetBandwidthBps)*(netWeights[i]/sumNetWeight)))
	}
}

// ReserveJournal admits bytes into tenant id's local journal quota and computes
// proportional per-tenant backpressure delay when a noisy tenant exceeds 70% of
// its own journal ceiling (isolating well-behaved tenants from global slowdown).
func (s *Scheduler) ReserveJournal(id string, bytes int64) (delay time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tenants[id]
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrUnknownTenant, id)
	}
	if bytes <= 0 {
		return 0, nil
	}
	if t.journalUsed+bytes > t.journalQuota || s.totalJUsed+bytes > s.cfg.JournalCapacity {
		return 0, fmt.Errorf("%w: tenant %s used %d + %d > quota %d", ErrJournalQuota, id, t.journalUsed, bytes, t.journalQuota)
	}
	if t.journalUsed == 0 {
		t.oldestAck = s.cfg.Clock.Now()
	}
	t.journalUsed += bytes
	s.totalJUsed += bytes

	fill := float64(t.journalUsed) / float64(t.journalQuota)
	if fill > 0.70 {
		severity := min((fill-0.70)/0.30, 1.0)
		sec := (float64(bytes) / float64(max(1, t.netShareBps))) * severity
		delay = time.Duration(sec * float64(time.Second))
		t.throttleMicros += uint64(delay.Microseconds())
	}
	return delay, nil
}

// ReleaseJournal marks publishedBytes as flushed to S3 for tenant id, freeing
// its journal quota and advancing/clearing its publication-age anchor.
func (s *Scheduler) ReleaseJournal(id string, publishedBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tenants[id]
	if !ok {
		return
	}
	rel := min(publishedBytes, t.journalUsed)
	t.journalUsed -= rel
	s.totalJUsed = max(0, s.totalJUsed-rel)
	if t.journalUsed == 0 {
		t.oldestAck = time.Time{}
	} else {
		t.oldestAck = s.cfg.Clock.Now()
	}
}

func (s *Scheduler) refillTokensLocked(now time.Time) {
	dt := now.Sub(s.lastTokenT).Seconds()
	if dt > 0 {
		s.nodeTokens = math.Min(float64(s.cfg.NetBurstBytes), s.nodeTokens+dt*float64(s.cfg.NetBandwidthBps))
		s.lastTokenT = now
	}
}

// Acquire schedules one IO operation of size bytes for tenant id across the
// Start-Time Fair Queuing (SFQ) worker pool and S3 bandwidth shaper.
//
// Callers receive a Ticket containing any required PaceWait (which real callers
// sleep on via context-aware select, or simulated clocks advance directly) and
// MUST call Ticket.Done() when the operation finishes.
func (s *Scheduler) Acquire(ctx context.Context, id string, op OpClass, bytes int64) (*Ticket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	t, ok := s.tenants[id]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrUnknownTenant, id)
	}

	cost := math.Max(1.0, float64(max(bytes, 4096))/float64(max(1, t.weight)))
	now := s.cfg.Clock.Now()
	urgent := op == OpPublishPut && !t.oldestAck.IsZero() && now.Sub(t.oldestAck) >= (s.cfg.PublishBound*3)/5
	if urgent {
		cost *= 0.25
	}

	startTag := math.Max(s.virtualTime, t.lastFinishTag)
	t.lastFinishTag = startTag + cost

	if s.inFlight < s.cfg.WorkerSlots {
		s.inFlight++
		t.activeWorkers++
		if startTag > s.virtualTime {
			s.virtualTime = startTag
		}
		pace := s.chargeNetworkLocked(t, op, bytes, now)
		s.mu.Unlock()
		return &Ticket{sched: s, tenantID: id, PaceWait: pace}, nil
	}

	w := &sfqWaiter{
		tenantID: id,
		startTag: startTag,
		urgent:   urgent,
		ready:    make(chan struct{}),
	}
	s.enqueueWaiterLocked(w)
	s.mu.Unlock()

	select {
	case <-w.ready:
		s.mu.Lock()
		t2 := s.tenants[id]
		var pace time.Duration
		if t2 != nil {
			pace = s.chargeNetworkLocked(t2, op, bytes, s.cfg.Clock.Now())
		}
		s.mu.Unlock()
		return &Ticket{sched: s, tenantID: id, PaceWait: pace}, nil
	case <-ctx.Done():
		s.mu.Lock()
		s.removeWaiterLocked(w)
		s.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (s *Scheduler) enqueueWaiterLocked(w *sfqWaiter) {
	s.waitQueue = append(s.waitQueue, w)
	sort.SliceStable(s.waitQueue, func(i, j int) bool {
		if s.waitQueue[i].urgent != s.waitQueue[j].urgent {
			return s.waitQueue[i].urgent
		}
		return s.waitQueue[i].startTag < s.waitQueue[j].startTag
	})
}

func (s *Scheduler) removeWaiterLocked(target *sfqWaiter) {
	for i, w := range s.waitQueue {
		if w == target {
			s.waitQueue = append(s.waitQueue[:i], s.waitQueue[i+1:]...)
			return
		}
	}
	// If already popped and granted right before cancellation, release the slot.
	select {
	case <-target.ready:
		s.releaseWorkerLocked(target.tenantID)
	default:
	}
}

func (s *Scheduler) chargeNetworkLocked(t *tenantState, op OpClass, bytes int64, now time.Time) time.Duration {
	t.opsCompleted++
	if bytes <= 0 || op == OpJournalAppend {
		return 0
	}
	t.bytesMoved += uint64(bytes)
	s.refillTokensLocked(now)

	need := float64(bytes)
	if s.nodeTokens >= need {
		s.nodeTokens -= need
		return 0
	}
	deficit := need - s.nodeTokens
	s.nodeTokens = 0

	// Charge deficit against this tenant's fair S3 bandwidth share when contended,
	// or full node bandwidth when only 1 tenant is active.
	rate := float64(t.netShareBps)
	if len(s.tenants) <= 1 {
		rate = float64(s.cfg.NetBandwidthBps)
	}
	wait := time.Duration((deficit / math.Max(1, rate)) * float64(time.Second))
	t.throttleMicros += uint64(wait.Microseconds())
	return wait
}

func (s *Scheduler) releaseWorker(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseWorkerLocked(id)
}

func (s *Scheduler) releaseWorkerLocked(id string) {
	if t, ok := s.tenants[id]; ok && t.activeWorkers > 0 {
		t.activeWorkers--
	}
	if len(s.waitQueue) > 0 {
		next := s.waitQueue[0]
		s.waitQueue = s.waitQueue[1:]
		if next.startTag > s.virtualTime {
			s.virtualTime = next.startTag
		}
		if nt, ok := s.tenants[next.tenantID]; ok {
			nt.activeWorkers++
		}
		close(next.ready)
		return
	}
	if s.inFlight > 0 {
		s.inFlight--
	}
}

// Snapshot returns current allocations and counters for all tenants in ID order.
func (s *Scheduler) Snapshot() []TenantAllocation {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(s.tenants))
	for id := range s.tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]TenantAllocation, 0, len(ids))
	for _, id := range ids {
		t := s.tenants[id]
		wGuar := max(1, (s.cfg.WorkerSlots*t.weight)/max(1, s.totalWeight))
		out = append(out, TenantAllocation{
			ID:              t.id,
			Weight:          t.weight,
			CacheQuota:      t.cacheQuota,
			CacheUsed:       t.cacheUsed,
			JournalQuota:    t.journalQuota,
			JournalUsed:     t.journalUsed,
			NetShareBps:     t.netShareBps,
			WorkerGuarantee: wGuar,
			ActiveWorkers:   t.activeWorkers,
			Hits:            t.mrc.hits,
			Misses:          t.mrc.misses,
			BytesMoved:      t.bytesMoved,
			ThrottleMicros:  t.throttleMicros,
			MarginalUtility: t.utility,
		})
	}
	return out
}

// JainsFairness computes Jain's fairness index (in [1/N, 1.0]) over normalized
// tenant values (e.g. throughput divided by tenant weight).
func JainsFairness(values []float64) float64 {
	if len(values) == 0 {
		return 1.0
	}
	var sum, sumSq float64
	for _, v := range values {
		sum += v
		sumSq += v * v
	}
	if sumSq == 0 {
		return 1.0
	}
	return (sum * sum) / (float64(len(values)) * sumSq)
}
