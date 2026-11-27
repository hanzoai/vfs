// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package hare_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/hare"
)

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (m *manualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *manualClock) Advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	m.mu.Unlock()
}

func TestCacheCentricAllocationAcross128Sandboxes(t *testing.T) {
	clk := &manualClock{now: time.Unix(1_700_000_000, 0)}
	const totalCache = int64(512 << 20)
	s, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 8 << 30,
		CacheCapacity:   totalCache,
		NetBandwidthBps: 2000 << 20,
		WorkerSlots:     16,
		PublishBound:    5 * time.Second,
		Clock:           clk,
	})
	if err != nil {
		t.Fatal(err)
	}

	const nSandboxes = 128
	const nInteractive = 96
	for i := range nSandboxes {
		s.RegisterTenant(fmt.Sprintf("sbx-%03d", i), 1)
	}

	// Interactive sandboxes reuse a hot working set inside the MRC ghost window;
	// noisy sandboxes stream unique block IDs with zero reuse.
	for round := range 80 {
		for i := range nSandboxes {
			id := fmt.Sprintf("sbx-%03d", i)
			var blk uint64
			if i < nInteractive {
				blk = uint64(i*1000 + (round % 24))
			} else {
				blk = uint64(9_000_000 + i*100000 + round)
			}
			if _, err := s.ObserveBlockAccess(id, blk, 64<<10); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.Rebalance()

	snap := s.Snapshot()
	if len(snap) != nSandboxes {
		t.Fatalf("got %d tenants, want %d", len(snap), nSandboxes)
	}

	var sumQuota int64
	var interactiveSum, noisySum int64
	for i, a := range snap {
		sumQuota += a.CacheQuota
		if i < nInteractive {
			interactiveSum += a.CacheQuota
		} else {
			noisySum += a.CacheQuota
		}
	}
	if sumQuota != totalCache {
		t.Fatalf("sum(CacheQuota) = %d, want exact %d", sumQuota, totalCache)
	}
	meanInteractive := interactiveSum / nInteractive
	meanNoisy := noisySum / (nSandboxes - nInteractive)
	if meanInteractive <= meanNoisy*2 {
		t.Fatalf("expected interactive sandboxes to receive >2x cache of zero-reuse scan sandboxes: interactive=%d, noisy=%d",
			meanInteractive, meanNoisy)
	}
}

func TestJournalIsolationAndDeadlinePriorityBoost(t *testing.T) {
	clk := &manualClock{now: time.Unix(1_700_000_000, 0)}
	s, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 64 << 20,
		CacheCapacity:   128 << 20,
		NetBandwidthBps: 500 << 20,
		WorkerSlots:     4,
		PublishBound:    5 * time.Second,
		Clock:           clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		s.RegisterTenant(fmt.Sprintf("t-%d", i), 1)
	}

	// Noisy tenant t-7 tries to monopolize the entire 64 MiB node journal:
	// HARE caps it at its dynamic per-tenant ceiling so t-0 can still append without delay.
	var quotaErr error
	for range 64 {
		_, err := s.ReserveJournal("t-7", 1<<20)
		if err != nil {
			quotaErr = err
			break
		}
	}
	if !errors.Is(quotaErr, hare.ErrJournalQuota) {
		t.Fatalf("expected ErrJournalQuota on noisy writer, got %v", quotaErr)
	}

	// Innocent tenant t-0 still reserves 512 KiB with zero throttle delay!
	d0, err := s.ReserveJournal("t-0", 512<<10)
	if err != nil {
		t.Fatalf("innocent tenant t-0 rejected despite isolation: %v", err)
	}
	if d0 != 0 {
		t.Fatalf("innocent tenant t-0 incurred unexpected delay %v", d0)
	}

	// Advance clock by 3.5 s (> 50% of 5 s PublishBound): t-0's unpublished journal
	// age triggers the deadline S3 bandwidth boost on Rebalance.
	clk.Advance(3500 * time.Millisecond)
	s.ReleaseJournal("t-7", 64<<20)
	s.Rebalance()

	snap := s.Snapshot()
	var t0Net, t1Net int64
	for _, a := range snap {
		if a.ID == "t-0" {
			t0Net = a.NetShareBps
		}
		if a.ID == "t-1" {
			t1Net = a.NetShareBps
		}
	}
	if t0Net <= t1Net*2 {
		t.Fatalf("expected deadline-boosted S3 bandwidth for overdue t-0 (%d) > 2x idle t-1 (%d)", t0Net, t1Net)
	}
}

func TestSFQConcurrentContentionAndCancellationRaceFree(t *testing.T) {
	ctx := context.Background()
	s, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 1 << 30,
		CacheCapacity:   256 << 20,
		NetBandwidthBps: 1000 << 20,
		WorkerSlots:     4,
		PublishBound:    5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	const tenants = 32
	for i := range tenants {
		s.RegisterTenant(fmt.Sprintf("w-%02d", i), 1)
	}

	var wg sync.WaitGroup
	for i := range tenants {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("w-%02d", i)
			for j := range 40 {
				if j%13 == 0 {
					cctx, cancel := context.WithCancel(ctx)
					cancel()
					_, _ = s.Acquire(cctx, id, hare.OpChunkRead, 64<<10)
					continue
				}
				tk, err := s.Acquire(ctx, id, hare.OpChunkRead, 64<<10)
				if err != nil {
					t.Errorf("Acquire(%s): %v", id, err)
					return
				}
				_, _ = s.ObserveBlockAccess(id, uint64(i*100+(j%8)), 64<<10)
				tk.Done()
			}
		}(i)
	}
	wg.Wait()

	snap := s.Snapshot()
	opsBytes := make([]float64, tenants)
	for i, a := range snap {
		if a.ActiveWorkers != 0 {
			t.Fatalf("tenant %s leaked %d active workers", a.ID, a.ActiveWorkers)
		}
		opsBytes[i] = float64(a.BytesMoved)
	}
	if jf := hare.JainsFairness(opsBytes); jf < 0.99 {
		t.Fatalf("expected Jain's fairness >= 0.99 across equal tenants, got %.4f", jf)
	}
}

// TestAdversarialIdleToActiveTenants verifies that idle tenants do not hoard
// elastic cache away from active tenants, and when idle tenants wake up with
// reusable working sets HARE rapidly shifts cache quota to them while
// conserving total CacheCapacity identically.
func TestAdversarialIdleToActiveTenants(t *testing.T) {
	const totalCache = int64(256 << 20)
	s, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 4 << 30,
		CacheCapacity:   totalCache,
		NetBandwidthBps: 1000 << 20,
		WorkerSlots:     8,
		PublishBound:    5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 64
	const half = 32
	for i := range n {
		s.RegisterTenant(fmt.Sprintf("t-%02d", i), 1)
	}

	// Phase 1: first 32 tenants are active with hot 32-block working sets;
	// last 32 tenants are completely idle (0 accesses).
	for round := range 96 {
		for i := range half {
			_, err := s.ObserveBlockAccess(fmt.Sprintf("t-%02d", i), uint64(i*1000+(round%32)), 64<<10)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	s.Rebalance()

	snap1 := s.Snapshot()
	var sum1, activeSum1, idleSum1 int64
	for i, a := range snap1 {
		sum1 += a.CacheQuota
		if i < half {
			activeSum1 += a.CacheQuota
		} else {
			idleSum1 += a.CacheQuota
		}
	}
	if sum1 != totalCache {
		t.Fatalf("phase 1 sum(CacheQuota) = %d, want %d", sum1, totalCache)
	}
	if activeSum1/half <= (idleSum1/half)*2 {
		t.Fatalf("idle tenants hoarded cache: active=%d idle=%d", activeSum1/half, idleSum1/half)
	}

	// Phase 2: idle tenants abruptly wake up with hot 32-block working sets.
	for round := range 96 {
		for i := half; i < n; i++ {
			_, err := s.ObserveBlockAccess(fmt.Sprintf("t-%02d", i), uint64(i*1000+(round%32)), 64<<10)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	s.Rebalance()

	snap2 := s.Snapshot()
	var sum2, wokenSum int64
	quotas := make([]float64, n)
	for i, a := range snap2 {
		sum2 += a.CacheQuota
		quotas[i] = float64(a.CacheQuota)
		if i >= half {
			wokenSum += a.CacheQuota
		}
	}
	if sum2 != totalCache {
		t.Fatalf("phase 2 sum(CacheQuota) = %d, want %d", sum2, totalCache)
	}
	if wokenSum/half <= (idleSum1/half)*2 {
		t.Fatalf("woken tenants did not reclaim cache: before=%d after=%d", idleSum1/half, wokenSum/half)
	}
	if jf := hare.JainsFairness(quotas); jf < 0.99 {
		t.Fatalf("expected balanced quota after idle->active convergence, Jain=%.4f", jf)
	}
}

// TestAdversarialTenantRemovalDuringContention verifies that unregistering
// tenants while they have queued waiters and in-flight workers unblocks waiters
// with ErrUnknownTenant, leaks zero worker slots, and conserves total cache
// across remaining tenants cleanly.
func TestAdversarialTenantRemovalDuringContention(t *testing.T) {
	ctx := context.Background()
	const totalCache = int64(128 << 20)
	s, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 512 << 20,
		CacheCapacity:   totalCache,
		NetBandwidthBps: 500 << 20,
		WorkerSlots:     2,
		PublishBound:    5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := range 8 {
		s.RegisterTenant(fmt.Sprintf("dyn-%d", i), 1)
	}
	// Saturate both worker slots using dyn-0 and dyn-1.
	tk0, err := s.Acquire(ctx, "dyn-0", hare.OpChunkRead, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	tk1, err := s.Acquire(ctx, "dyn-1", hare.OpChunkRead, 64<<10)
	if err != nil {
		t.Fatal(err)
	}

	// Queue waiters for dyn-2 (which will be removed while queued) and dyn-3 (survivor).
	removedDone := make(chan error, 1)
	go func() {
		tk, err := s.Acquire(ctx, "dyn-2", hare.OpChunkRead, 64<<10)
		if err == nil {
			tk.Done()
		}
		removedDone <- err
	}()
	survivorDone := make(chan error, 1)
	go func() {
		tk, err := s.Acquire(ctx, "dyn-3", hare.OpChunkRead, 64<<10)
		if err == nil {
			tk.Done()
		}
		survivorDone <- err
	}()

	// Give goroutines a moment to enter waitQueue, then unregister dyn-2 and in-flight dyn-1.
	time.Sleep(15 * time.Millisecond)
	_, _ = s.ReserveJournal("dyn-2", 4<<20)
	s.UnregisterTenant("dyn-2")
	s.UnregisterTenant("dyn-1")

	// dyn-2's queued waiter must wake promptly with ErrUnknownTenant (never hang).
	select {
	case err := <-removedDone:
		if !errors.Is(err, hare.ErrUnknownTenant) {
			t.Fatalf("removed queued waiter got %v, want ErrUnknownTenant", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued waiter for unregistered tenant hung")
	}

	// Releasing both tickets must unblock survivor dyn-3 cleanly.
	tk1.Done()
	tk0.Done()
	select {
	case err := <-survivorDone:
		if err != nil {
			t.Fatalf("surviving waiter got %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("surviving waiter hung after peer removal")
	}

	// Verify exact cache quota conservation and zero leaked worker slots across survivors.
	snap := s.Snapshot()
	if len(snap) != 6 {
		t.Fatalf("remaining tenants = %d, want 6", len(snap))
	}
	var sumCache int64
	for _, a := range snap {
		sumCache += a.CacheQuota
		if a.ActiveWorkers != 0 {
			t.Fatalf("tenant %s leaked %d active workers", a.ID, a.ActiveWorkers)
		}
	}
	if sumCache != totalCache {
		t.Fatalf("sum(CacheQuota) after removals = %d, want exact %d", sumCache, totalCache)
	}

	// Unregistering every remaining tenant leaves clean empty state without panic.
	for _, a := range snap {
		s.UnregisterTenant(a.ID)
	}
	if len(s.Snapshot()) != 0 {
		t.Fatalf("expected 0 tenants after removing all, got %d", len(s.Snapshot()))
	}
}

// TestAdversarialStrictQuotaConservation checks exact byte-for-byte quota
// conservation (sum(CacheQuota) == CacheCapacity, CacheUsed <= CacheQuota,
// sum(NetShareBps) <= NetBandwidthBps) across adversarial tenant counts,
// skewed weights, prime/tiny capacities, and register/unregister churn.
func TestAdversarialStrictQuotaConservation(t *testing.T) {
	cases := []struct {
		name      string
		cacheCap  int64
		netBps    int64
		nTenants  int
		skewEvery int
	}{
		{name: "prime-capacity-13", cacheCap: (16 << 20) + 13, netBps: 100_000_007, nTenants: 13, skewEvery: 3},
		{name: "tiny-capacity-many-tenants", cacheCap: 1 << 20, netBps: 1 << 20, nTenants: 40, skewEvery: 5},
		{name: "257-tenants-extreme-weights", cacheCap: (512 << 20) + 7, netBps: 2_000_000_003, nTenants: 257, skewEvery: 17},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := hare.NewScheduler(hare.NodeConfig{
				JournalCapacity: 2 << 30,
				CacheCapacity:   tc.cacheCap,
				NetBandwidthBps: tc.netBps,
				WorkerSlots:     8,
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := range tc.nTenants {
				w := 1
				if i%tc.skewEvery == 0 {
					w = 50
				}
				s.RegisterTenant(fmt.Sprintf("c-%03d", i), w)
			}
			for r := range 32 {
				for i := range tc.nTenants {
					id := fmt.Sprintf("c-%03d", i)
					blk := uint64(i*10000 + r)
					if i%2 == 0 {
						blk = uint64(i*10000 + (r % 12))
					}
					_, _ = s.ObserveBlockAccess(id, blk, 64<<10)
				}
			}
			s.Rebalance()

			check := func(phase string) {
				t.Helper()
				snap := s.Snapshot()
				var sumCache, sumNet int64
				for _, a := range snap {
					if a.CacheQuota < 0 || a.CacheUsed > a.CacheQuota {
						t.Fatalf("%s: tenant %s invalid cache quota=%d used=%d", phase, a.ID, a.CacheQuota, a.CacheUsed)
					}
					sumCache += a.CacheQuota
					sumNet += a.NetShareBps
				}
				if len(snap) > 0 && sumCache != tc.cacheCap {
					t.Fatalf("%s: sum(CacheQuota)=%d != CacheCapacity=%d", phase, sumCache, tc.cacheCap)
				}
				if sumNet > tc.netBps {
					t.Fatalf("%s: sum(NetShareBps)=%d > NetBandwidthBps=%d", phase, sumNet, tc.netBps)
				}
			}
			check("after-rebalance")

			for i := 0; i < tc.nTenants; i += 3 {
				s.UnregisterTenant(fmt.Sprintf("c-%03d", i))
			}
			check("after-partial-removal")
		})
	}
}

// TestAdversarialStarvationPreventionUnderUrgentFlood verifies that a noisy
// tenant holding an overdue journal and continuously flooding urgent
// OpPublishPut operations cannot starve innocent interactive readers.
func TestAdversarialStarvationPreventionUnderUrgentFlood(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clk := &manualClock{now: time.Unix(1_700_000_000, 0)}
	s, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 256 << 20,
		CacheCapacity:   128 << 20,
		NetBandwidthBps: 1000 << 20,
		WorkerSlots:     2,
		PublishBound:    5 * time.Second,
		Clock:           clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.RegisterTenant("flood-urgent", 1)
	s.RegisterTenant("interactive-reader", 1)

	// Make flood-urgent perpetually overdue (>60% of 5 s PublishBound).
	if err := s.ObserveJournalState("flood-urgent", 16<<20, 4*time.Second); err != nil {
		t.Fatal(err)
	}

	var stopFlood atomic.Bool
	var floodOps, readerOps atomic.Int64
	var wg sync.WaitGroup
	// 4 goroutines continuously hammering urgent OpPublishPut on 2 worker slots.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stopFlood.Load() {
				tk, err := s.Acquire(ctx, "flood-urgent", hare.OpPublishPut, 64<<10)
				if err != nil {
					return
				}
				floodOps.Add(1)
				time.Sleep(200 * time.Microsecond)
				tk.Done()
			}
		}()
	}

	// Interactive reader must make guaranteed progress and complete 30 reads promptly.
	const wantReads = 30
	for range wantReads {
		tk, err := s.Acquire(ctx, "interactive-reader", hare.OpChunkRead, 64<<10)
		if err != nil {
			stopFlood.Store(true)
			wg.Wait()
			t.Fatalf("interactive reader starved after %d/%d ops: %v (floodOps=%d)",
				readerOps.Load(), wantReads, err, floodOps.Load())
		}
		readerOps.Add(1)
		tk.Done()
	}
	stopFlood.Store(true)
	wg.Wait()

	if readerOps.Load() != wantReads {
		t.Fatalf("readerOps = %d, want %d", readerOps.Load(), wantReads)
	}
	for _, a := range s.Snapshot() {
		if a.ActiveWorkers != 0 {
			t.Fatalf("tenant %s leaked %d active workers", a.ID, a.ActiveWorkers)
		}
	}
}

// TestAdversarialTenantBackendS3Outage verifies that backend errors and context
// timeouts during an S3 outage never leak HARE worker slots and leave healthy
// tenants unaffected.
func TestAdversarialTenantBackendS3Outage(t *testing.T) {
	ctx := context.Background()
	s, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 256 << 20,
		CacheCapacity:   128 << 20,
		NetBandwidthBps: 500 << 20,
		WorkerSlots:     2,
		PublishBound:    5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.RegisterTenant("failing", 1)
	s.RegisterTenant("healthy", 1)

	var down atomic.Bool
	down.Store(true)
	memFail := storetest.NewMem()
	failHook := &storetest.Hook{
		Backend: memFail,
		Before: func(op, key string) error {
			if down.Load() {
				return errors.New("injected s3 outage")
			}
			return nil
		},
	}
	tbFail := hare.NewTenantBackend(failHook, s, "failing", false)
	tbGood := hare.NewTenantBackend(storetest.NewMem(), s, "healthy", false)

	// Failing tenant hammers S3 during outage; every call errors without leaking slots.
	for i := range 20 {
		if err := tbFail.Put(ctx, fmt.Sprintf("pack/%02d", i), []byte("data")); err == nil {
			t.Fatal("expected S3 outage error on failing tenant")
		}
		if _, _, err := tbFail.Get(ctx, "head", 64<<10); err == nil {
			t.Fatal("expected S3 outage error on failing Get")
		}
	}

	// Healthy tenant must still complete Put + Get cleanly.
	if err := tbGood.Put(ctx, "pack/ok", []byte("good-payload")); err != nil {
		t.Fatalf("healthy tenant Put failed during peer outage: %v", err)
	}
	got, _, err := tbGood.Get(ctx, "pack/ok", 64<<10)
	if err != nil || string(got) != "good-payload" {
		t.Fatalf("healthy tenant Get = %q, %v", got, err)
	}

	// Recovery on failing tenant succeeds and zero worker slots leaked.
	down.Store(false)
	if err := tbFail.Put(ctx, "pack/recovered", []byte("recovered")); err != nil {
		t.Fatalf("recovered tenant Put failed: %v", err)
	}
	for _, a := range s.Snapshot() {
		if a.ActiveWorkers != 0 {
			t.Fatalf("tenant %s leaked %d active workers after outage", a.ID, a.ActiveWorkers)
		}
	}
}

func BenchmarkHAREAcquireDone(b *testing.B) {
	ctx := context.Background()
	s, _ := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 8 << 30,
		CacheCapacity:   512 << 20,
		NetBandwidthBps: 4000 << 20,
		WorkerSlots:     32,
	})
	for i := range 64 {
		s.RegisterTenant(fmt.Sprintf("t-%02d", i), 1)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk, err := s.Acquire(ctx, fmt.Sprintf("t-%02d", i&63), hare.OpMetaRead, 4096)
		if err == nil {
			tk.Done()
		}
	}
}
