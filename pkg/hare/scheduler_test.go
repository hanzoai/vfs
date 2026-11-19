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
	"testing"
	"time"

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
