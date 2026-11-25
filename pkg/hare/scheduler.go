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
// fair queuing (SFQ) with bounded publication-deadline boost (<= 5 s bound),
// using caller/injected clocks and zero background polling goroutines.
//
// HARE complements — and never replaces — the canonical VFS v2 journal and
// publication bound: callers feed real Volume.State() backlog and age into
// ObserveJournalState while TenantBackend schedules S3 operations through
// Scheduler.Acquire.
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
	mrcBuckets      = 64
	mrcWindowDecay  = 512
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
	Evictions       uint64
	BytesMoved      uint64
	ThrottleMicros  uint64
	MarginalUtility float64
}

// mrcTracker estimates a tenant's exact Mattson stack reuse-distance
// distribution over a fixed 256-block ghost LRU stack (zero heap allocations
// per access, periodic sliding-window halving so idle-to-active and phase
// shifts adapt rapidly).
type mrcTracker struct {
	lru          [ghostRingSize]uint64
	lruLen       int
	distHist     [mrcBuckets]uint64
	windowHits   uint64
	windowMisses uint64
	hits         uint64
	misses       uint64
	evictions    uint64
}

func newMRCTracker() mrcTracker {
	return mrcTracker{}
}

func (m *mrcTracker) observe(blockKey uint64, cacheBlocks int64) (hit bool) {
	capBlocks := max(int64(1), cacheBlocks)
	pos := -1
	for i := 0; i < m.lruLen; i++ {
		if m.lru[i] == blockKey {
			pos = i
			break
		}
	}
	if pos >= 0 {
		// Exact distinct-block reuse distance (Mattson stack distance, 1-based).
		dist := pos + 1
		if pos > 0 {
			copy(m.lru[1:pos+1], m.lru[0:pos])
			m.lru[0] = blockKey
		}
		bucket := min((dist-1)/4, mrcBuckets-1)
		m.distHist[bucket]++
		if int64(dist) <= capBlocks {
			hit = true
			m.hits++
			m.windowHits++
		} else {
			m.misses++
			m.windowMisses++
			m.evictions++
		}
	} else {
		m.misses++
		m.windowMisses++
		if int64(m.lruLen) >= capBlocks {
			m.evictions++
		}
		if m.lruLen < ghostRingSize {
			copy(m.lru[1:m.lruLen+1], m.lru[0:m.lruLen])
			m.lru[0] = blockKey
			m.lruLen++
		} else {
			copy(m.lru[1:], m.lru[:ghostRingSize-1])
			m.lru[0] = blockKey
		}
	}

	if m.windowHits+m.windowMisses >= mrcWindowDecay {
		for i := range m.distHist {
			m.distHist[i] >>= 1
		}
		m.windowHits >>= 1
		m.windowMisses >>= 1
	}
	return hit
}

// cacheUtility estimates the cache benefit a tenant gains from holding elastic
// cache above floorBlocks.
//   - Completely idle tenants (0 window accesses) return 0 so they do not hoard
//     elastic cache away from active tenants.
//   - Newly waking tenants (< 8 accesses, no reuse observed yet) receive a
//     small warm-up probe utility so idle-to-active bursts ramp quickly.
//   - Confirmed zero-reuse scans/streams return 0.
//   - Interactive tenants with reuse inside or above floorBlocks return utility
//     weighted strongly toward hits above floorBlocks that would be evicted if
//     shrunk to the scan-resistant floor.
func (m *mrcTracker) cacheUtility(floorBlocks int64) float64 {
	total := m.windowHits + m.windowMisses
	if total == 0 {
		return 0.0
	}
	floorBucket := min(max(int(floorBlocks/4), 0), mrcBuckets-1)
	var aboveFloor, totalReuse uint64
	for b := 0; b < mrcBuckets; b++ {
		totalReuse += m.distHist[b]
		if b >= floorBucket {
			aboveFloor += m.distHist[b]
		}
	}
	if totalReuse == 0 {
		if total < 8 {
			return 0.15
		}
		return 0.0
	}
	aboveRate := float64(aboveFloor) / float64(total+8)
	reuseRate := float64(totalReuse) / float64(total+8)
	return 0.75*aboveRate + 0.25*reuseRate
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
	removed  bool
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

	mu           sync.Mutex
	tenants      map[string]*tenantState
	totalWeight  int
	virtualTime  float64
	inFlight     int
	urgentStreak int
	waitQueue    []*sfqWaiter

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

// UnregisterTenant removes a stopped sandbox, wakes any queued waiters for it
// with ErrUnknownTenant, and redistributes its quotas cleanly across remaining
// tenants while preserving strict quota conservation.
func (s *Scheduler) UnregisterTenant(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tenants[id]
	if !ok {
		return
	}
	s.totalWeight -= t.weight
	s.totalJUsed = max(0, s.totalJUsed-t.journalUsed)
	delete(s.tenants, id)

	if len(s.waitQueue) > 0 {
		kept := s.waitQueue[:0]
		for _, w := range s.waitQueue {
			if w.tenantID == id {
				w.removed = true
				close(w.ready)
				continue
			}
			kept = append(kept, w)
		}
		s.waitQueue = kept
	}

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
	ops := t.mrc.hits + t.mrc.misses
	if ops == 1 || ops == 16 || ops%64 == 0 {
		s.rebalanceLocked()
	}
	return hit, nil
}

// ObserveJournalState synchronizes real VFS v2 Volume.State() backlog and
// oldest-unpublished age into HARE without replacing the canonical v2 journal
// or its publication bound.
func (s *Scheduler) ObserveJournalState(id string, backlog int64, oldestAge time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tenants[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownTenant, id)
	}
	if backlog < 0 {
		backlog = 0
	}
	s.totalJUsed = max(0, s.totalJUsed-t.journalUsed) + backlog
	t.journalUsed = backlog
	if backlog == 0 || oldestAge <= 0 {
		t.oldestAck = time.Time{}
	} else {
		t.oldestAck = s.cfg.Clock.Now().Add(-oldestAge)
	}
	return nil
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
	// elastic pool is distributed proportionally to weight * (epsilon + cacheUtility(floorBlocks)),
	// shifting cache away from zero-reuse scans and idle sandboxes to high-reuse
	// interactive toolchains while conserving exact sum(CacheQuota) == CacheCapacity.
	const floorFrac = 0.25
	const blockUnit int64 = 64 << 10

	ids := make([]string, 0, n)
	for id := range s.tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	minUnit := min(blockUnit, s.cfg.CacheCapacity/int64(n))
	floors := make([]int64, n)
	scores := make([]float64, n)
	var sumFloor int64
	var sumScore float64

	for i, id := range ids {
		t := s.tenants[id]
		wFrac := float64(t.weight) / float64(s.totalWeight)
		fb := max(minUnit, int64(float64(s.cfg.CacheCapacity)*floorFrac*wFrac))
		floors[i] = fb
		sumFloor += fb
	}
	// If minUnit clamping pushed sumFloor past CacheCapacity, cap floors at minUnit.
	if sumFloor > s.cfg.CacheCapacity {
		sumFloor = 0
		for i := range ids {
			floors[i] = minUnit
			sumFloor += minUnit
		}
	}
	elasticPool := max(int64(0), s.cfg.CacheCapacity-sumFloor)

	for i, id := range ids {
		t := s.tenants[id]
		floorBlocks := max(int64(1), floors[i]/blockUnit)
		u := t.mrc.cacheUtility(floorBlocks)
		t.utility = u
		sc := float64(t.weight) * (0.02 + u)
		scores[i] = sc
		sumScore += sc
	}

	quotas := make([]int64, n)
	var assignedCache int64
	for i := range ids {
		var bonus int64
		if sumScore > 0 && elasticPool > 0 {
			bonus = int64(float64(elasticPool) * (scores[i] / sumScore))
		}
		q := max(minUnit, floors[i]+bonus)
		quotas[i] = q
		assignedCache += q
	}

	// Enforce exact quota conservation: sum(CacheQuota) == s.cfg.CacheCapacity.
	diff := s.cfg.CacheCapacity - assignedCache
	if diff > 0 {
		// Distribute rounding remainder proportionally, then +1 byte in score/ID order.
		order := make([]int, n)
		for i := range n {
			order[i] = i
		}
		sort.SliceStable(order, func(a, b int) bool {
			return scores[order[a]] > scores[order[b]]
		})
		for k := 0; diff > 0; k = (k + 1) % n {
			quotas[order[k]]++
			diff--
		}
	} else if diff < 0 {
		excess := -diff
		var trimmable int64
		for i := range n {
			if quotas[i] > minUnit {
				trimmable += quotas[i] - minUnit
			}
		}
		if trimmable > 0 {
			for i := range n {
				if quotas[i] <= minUnit || excess == 0 {
					continue
				}
				avail := quotas[i] - minUnit
				cut := min(avail, (excess*avail)/trimmable)
				quotas[i] -= cut
				excess -= cut
			}
		}
		for excess > 0 {
			progressed := false
			for i := n - 1; i >= 0 && excess > 0; i-- {
				if quotas[i] > minUnit {
					quotas[i]--
					excess--
					progressed = true
				}
			}
			if !progressed {
				break
			}
		}
	}

	for i, id := range ids {
		t := s.tenants[id]
		t.cacheQuota = quotas[i]
		if t.cacheUsed > t.cacheQuota {
			evictedBytes := t.cacheUsed - t.cacheQuota
			t.mrc.evictions += uint64((evictedBytes + blockUnit - 1) / blockUnit)
			t.cacheUsed = t.cacheQuota
		}
	}

	// 2. Journal & S3 Network shares: coupled with publication-age urgency so
	// tenants approaching the 5 s S3 publication SLA get priority drain bandwidth
	// while noisy writers are capped before pushing the node past journal
	// slowFrom (75%).
	now := s.cfg.Clock.Now()
	var sumNetWeight float64
	netWeights := make([]float64, n)
	for i, id := range ids {
		t := s.tenants[id]
		wFrac := float64(t.weight) / float64(s.totalWeight)
		nodeFill := min(1.0, float64(s.totalJUsed)/float64(s.cfg.JournalCapacity))
		burstMult := max(1.25, 4.0-3.0*nodeFill)
		jCeil := int64(math.Min(float64(s.cfg.JournalCapacity)*0.50, float64(s.cfg.JournalCapacity)*wFrac*burstMult))
		t.journalQuota = max(min(256<<10, s.cfg.JournalCapacity), jCeil)

		nw := float64(t.weight)
		if !t.oldestAck.IsZero() && now.Sub(t.oldestAck) > s.cfg.PublishBound/2 {
			// Boost S3 publication drain share for tenants past 50% of the 5 s bound.
			nw *= 2.5
		}
		netWeights[i] = nw
		sumNetWeight += nw
	}

	minNet := min(int64(64<<10), s.cfg.NetBandwidthBps/int64(n))
	netShares := make([]int64, n)
	var assignedNet int64
	for i := range ids {
		share := max(minNet, int64(float64(s.cfg.NetBandwidthBps)*(netWeights[i]/sumNetWeight)))
		netShares[i] = share
		assignedNet += share
	}
	if excess := assignedNet - s.cfg.NetBandwidthBps; excess > 0 {
		var trimmable int64
		for i := range n {
			if netShares[i] > minNet {
				trimmable += netShares[i] - minNet
			}
		}
		if trimmable > 0 {
			for i := range n {
				if netShares[i] <= minNet || excess == 0 {
					continue
				}
				avail := netShares[i] - minNet
				cut := min(avail, (excess*avail)/trimmable)
				netShares[i] -= cut
				excess -= cut
			}
		}
		for excess > 0 {
			progressed := false
			for i := n - 1; i >= 0 && excess > 0; i-- {
				if netShares[i] > minNet {
					netShares[i]--
					excess--
					progressed = true
				}
			}
			if !progressed {
				break
			}
		}
	}
	for i, id := range ids {
		s.tenants[id].netShareBps = netShares[i]
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
	if publishedBytes >= t.journalUsed {
		s.totalJUsed = max(0, s.totalJUsed-t.journalUsed)
		t.journalUsed = 0
		t.oldestAck = time.Time{}
	} else if publishedBytes > 0 {
		t.journalUsed -= publishedBytes
		s.totalJUsed = max(0, s.totalJUsed-publishedBytes)
	}
}

func (s *Scheduler) refillTokensLocked(now time.Time) {
	if now.After(s.lastTokenT) {
		dt := now.Sub(s.lastTokenT).Seconds()
		s.nodeTokens = math.Min(float64(s.cfg.NetBurstBytes), s.nodeTokens+dt*float64(s.cfg.NetBandwidthBps))
		s.lastTokenT = now
	}
}

// Acquire admits an IO operation for tenant id using Start-Time Fair Queuing
// across node worker slots and lazy token-bucket pacing over S3 bandwidth.
// Operations belonging to a tenant approaching PublishBound receive bounded
// priority without starving non-urgent readers. Callers MUST invoke Ticket.Done
// when the operation finishes.
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
		if w.removed {
			s.mu.Unlock()
			return nil, fmt.Errorf("%w: %s", ErrUnknownTenant, id)
		}
		t2 := s.tenants[id]
		if t2 == nil {
			s.releaseWorkerLocked(id)
			s.mu.Unlock()
			return nil, fmt.Errorf("%w: %s", ErrUnknownTenant, id)
		}
		pace := s.chargeNetworkLocked(t2, op, bytes, s.cfg.Clock.Now())
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
	// If already popped and granted right before cancellation (and not dropped by UnregisterTenant), release the slot.
	if target.removed {
		return
	}
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
		pickIdx := 0
		maxUrgentStreak := max(2, s.cfg.WorkerSlots/2)
		if s.waitQueue[0].urgent {
			nonUrgentIdx := -1
			for i, w := range s.waitQueue {
				if !w.urgent {
					nonUrgentIdx = i
					break
				}
			}
			if nonUrgentIdx >= 0 && s.urgentStreak >= maxUrgentStreak {
				pickIdx = nonUrgentIdx
				s.urgentStreak = 0
			} else if nonUrgentIdx >= 0 {
				s.urgentStreak++
			} else {
				s.urgentStreak = 0
			}
		} else {
			s.urgentStreak = 0
		}

		next := s.waitQueue[pickIdx]
		s.waitQueue = append(s.waitQueue[:pickIdx], s.waitQueue[pickIdx+1:]...)
		if next.startTag > s.virtualTime {
			s.virtualTime = next.startTag
		}
		if nt, ok := s.tenants[next.tenantID]; ok {
			nt.activeWorkers++
		}
		close(next.ready)
		return
	}
	s.urgentStreak = 0
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
			Evictions:       t.mrc.evictions,
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
