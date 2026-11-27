// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/hare"
)

// TestHARE32LiveSandboxesFairnessAndIsolation attaches real VFS v2 journaled
// post-quantum vfs.Volume sandboxes (75% interactive coding sandboxes + 25%
// bulk-write/scan noisy neighbours) over one shared vfs.Budget and one shared
// hare.Scheduler with bounded worker slots, verifying end-to-end v2 admission,
// data integrity, zero worker leaks, zero Budget leaks, and Jain's fairness
// across interactive sandboxes.
func TestHARE32LiveSandboxesFairnessAndIsolation(t *testing.T) {
	nSandboxes := sized(16, 32)
	nNoisy := nSandboxes / 4
	nInteractive := nSandboxes - nNoisy

	sched, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 4 << 30,
		CacheCapacity:   256 << 20,
		NetBandwidthBps: 2000 << 20,
		NetBurstBytes:   128 << 20,
		WorkerSlots:     6,
		PublishBound:    5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	sharedBudget := vfs.NewBudget(64 << 20)
	ciph := vfs.Hybrid(newKey(t))
	vols := make([]*vfs.Volume, nSandboxes)
	for i := range nSandboxes {
		id := fmt.Sprintf("sandbox-%03d", i)
		sched.RegisterTenant(id, 1)
		tb := hare.NewTenantBackend(storetest.NewMem(), sched, id, false)
		if err := vfs.Create(ctx, cfg(tb, ciph, 0)); err != nil {
			t.Fatal(err)
		}
		conf := cfg(tb, ciph, 1)
		conf.Budget = sharedBudget
		v, err := vfs.Attach(ctx, conf)
		if err != nil {
			t.Fatal(err)
		}
		vols[i] = v
	}
	defer func() {
		for _, v := range vols {
			_ = v.Close()
		}
	}()

	var wg sync.WaitGroup
	interactiveElapseds := make([]float64, nInteractive)

	for i := range nSandboxes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := vols[i]
			if i < nInteractive {
				t0 := time.Now()
				src, err := v.Mkdir(ctx, vfs.RootInode, "src", 0o755)
				if err != nil {
					t.Errorf("sandbox %d mkdir: %v", i, err)
					return
				}
				for f := range 8 {
					nm := fmt.Sprintf("mod-%02d.go", f)
					in, err := v.Create(ctx, src.ID, nm, 0o644)
					if err != nil {
						t.Errorf("sandbox %d create: %v", i, err)
						return
					}
					fh, err := v.Open(ctx, in.ID)
					if err != nil {
						t.Errorf("sandbox %d open: %v", i, err)
						return
					}
					_, _ = fh.WriteAt(ctx, content(uint64(i*100+f), 2048), 0)
					_ = fh.Close()
				}
				if err := v.Flush(ctx); err != nil {
					t.Errorf("sandbox %d flush: %v", i, err)
					return
				}
				for round := range 3 {
					_ = round
					for f := range 8 {
						if _, err := v.Resolve(ctx, fmt.Sprintf("/src/mod-%02d.go", f)); err != nil {
							t.Errorf("sandbox %d resolve: %v", i, err)
							return
						}
					}
				}
				interactiveElapseds[i] = max(1.0, float64(time.Since(t0).Microseconds()))
			} else {
				// Noisy neighbour: large chunked ingest + unique cold reads.
				bulk, err := v.Mkdir(ctx, vfs.RootInode, "bulk", 0o755)
				if err != nil {
					t.Errorf("noisy %d mkdir: %v", i, err)
					return
				}
				for f := range 4 {
					in, err := v.Create(ctx, bulk.ID, fmt.Sprintf("blob-%02d.bin", f), 0o644)
					if err != nil {
						t.Errorf("noisy %d create: %v", i, err)
						return
					}
					fh, err := v.Open(ctx, in.ID)
					if err != nil {
						t.Errorf("noisy %d open: %v", i, err)
						return
					}
					_, _ = fh.WriteAt(ctx, content(uint64(9000+i*10+f), 96<<10), 0)
					_ = fh.Close()
				}
				if err := v.Flush(ctx); err != nil {
					t.Errorf("noisy %d flush: %v", i, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	if used := sharedBudget.Used(); used != 0 {
		t.Fatalf("shared vfs.Budget leaked %d bytes after all Volumes flushed", used)
	}

	sched.Rebalance()
	snap := sched.Snapshot()
	for _, a := range snap {
		if a.ActiveWorkers != 0 {
			t.Fatalf("tenant %s leaked %d active workers", a.ID, a.ActiveWorkers)
		}
	}

	rates := make([]float64, nInteractive)
	for i, us := range interactiveElapseds {
		rates[i] = 1e6 / us
	}
	if jf := hare.JainsFairness(rates); jf < 0.70 {
		t.Fatalf("interactive sandbox Jain's fairness under noisy neighbours = %.4f, want >= 0.70", jf)
	}
}

// TestHAREV2AdmissionPathS3OutageAndBackpressure qualifies HARE directly against
// the canonical VFS v2 admission path (real per-Volume journal, shared
// vfs.Budget, Dirty/JournalCapacity backpressure, and canonical Config.Bound)
// during an S3 outage and recovery. HARE never replaces the v2 journal or its
// publication bound; instead Volume.State() feeds real backlog and age into
// HARE's urgency accounting while TenantBackend schedules S3 operations.
func TestHAREV2AdmissionPathS3OutageAndBackpressure(t *testing.T) {
	clk := newClock()
	sched, err := hare.NewScheduler(hare.NodeConfig{
		JournalCapacity: 32 << 20,
		CacheCapacity:   64 << 20,
		NetBandwidthBps: 500 << 20,
		WorkerSlots:     4,
		PublishBound:    4 * time.Second,
		Clock:           clk,
	})
	if err != nil {
		t.Fatal(err)
	}

	budget := vfs.NewBudget(4 << 20)
	ciph := vfs.Hybrid(newKey(t))

	var outageDown atomic.Bool
	memOutage := storetest.NewMem()
	outageHook := &storetest.Hook{
		Backend: memOutage,
		Before: func(op, key string) error {
			if outageDown.Load() && (op == "put" || op == "putver") {
				return errors.New("s3 outage on noisy tenant")
			}
			return nil
		},
	}
	memHealthy := storetest.NewMem()

	sched.RegisterTenant("outage-tenant", 1)
	sched.RegisterTenant("healthy-tenant", 1)

	tbOutage := hare.NewTenantBackend(outageHook, sched, "outage-tenant", false)
	tbHealthy := hare.NewTenantBackend(memHealthy, sched, "healthy-tenant", false)

	if err := vfs.Create(ctx, cfg(memOutage, ciph, 0)); err != nil {
		t.Fatal(err)
	}
	if err := vfs.Create(ctx, cfg(memHealthy, ciph, 0)); err != nil {
		t.Fatal(err)
	}

	cfgOutage := cfg(tbOutage, ciph, 1)
	cfgOutage.JournalCapacity = 8 << 20
	cfgOutage.Dirty = 1 << 20
	cfgOutage.Budget = budget
	cfgOutage.Bound = 4 * time.Second
	cfgOutage.Clock = clk

	cfgHealthy := cfg(tbHealthy, ciph, 1)
	cfgHealthy.JournalCapacity = 8 << 20
	cfgHealthy.Dirty = 1 << 20
	cfgHealthy.Budget = budget
	cfgHealthy.Bound = 4 * time.Second
	cfgHealthy.Clock = clk

	vOutage, err := vfs.Attach(ctx, cfgOutage)
	if err != nil {
		t.Fatal(err)
	}
	defer vOutage.Close()

	vHealthy, err := vfs.Attach(ctx, cfgHealthy)
	if err != nil {
		t.Fatal(err)
	}
	defer vHealthy.Close()

	inOut, err := vOutage.Create(ctx, vfs.RootInode, "stream.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fOut, err := vOutage.Open(ctx, inOut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer fOut.Close()

	// Trigger S3 outage on outage-tenant and drive it past real v2 Dirty/JournalCapacity -> ErrFull.
	outageDown.Store(true)
	block := content(42, 128<<10)
	var accepted []byte
	var writeErr error
	for i := 0; i < 200; i++ {
		if _, writeErr = fOut.WriteAt(ctx, block, int64(len(accepted))); writeErr != nil {
			break
		}
		accepted = append(accepted, block...)
	}
	if !errors.Is(writeErr, vfs.ErrFull) {
		t.Fatalf("outage-tenant write error = %v, want vfs.ErrFull (accepted=%d KiB)", writeErr, len(accepted)>>10)
	}
	if len(accepted) == 0 {
		t.Fatal("expected outage-tenant to accept writes into local v2 journal before ErrFull")
	}
	if err := fOut.Sync(ctx); err != nil {
		t.Fatalf("outage-tenant local v2 Sync failed: %v", err)
	}
	stOut := vOutage.State()
	if !stOut.Degraded || stOut.Backlog == 0 || stOut.Durable < stOut.Visible {
		t.Fatalf("unexpected outage-tenant v2 State: %+v", stOut)
	}

	// While outage-tenant is degraded and holding Budget memory, healthy-tenant
	// (holding nothing) must STILL be admitted by v2's Budget, write, and publish
	// via the canonical v2 Bound clock!
	healthyPayload := content(99, 64<<10)
	inGood := write(t, vHealthy, vfs.RootInode, "interactive.go", healthyPayload)
	clk.Advance(5 * time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if st := vHealthy.State(); st.Published >= st.Visible && st.Visible > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("healthy-tenant canonical v2 Bound publication stalled during peer S3 outage: %+v", vHealthy.State())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := readAll(t, reopen(t, memHealthy, ciph), inGood.ID); !bytes.Equal(got, healthyPayload) {
		t.Fatal("healthy-tenant data mismatch in store")
	}

	// Feed real v2 Volume.State() backlog & age into HARE and verify urgency boost.
	stAged := vOutage.State()
	if err := sched.ObserveJournalState("outage-tenant", stAged.Backlog, stAged.Age); err != nil {
		t.Fatal(err)
	}
	if err := sched.ObserveJournalState("healthy-tenant", 0, 0); err != nil {
		t.Fatal(err)
	}
	sched.Rebalance()
	var outNet, goodNet int64
	for _, a := range sched.Snapshot() {
		if a.ID == "outage-tenant" {
			outNet = a.NetShareBps
		}
		if a.ID == "healthy-tenant" {
			goodNet = a.NetShareBps
		}
	}
	if outNet <= goodNet*2 {
		t.Fatalf("expected overdue v2 journal tenant to receive >2x S3 drain share: outage=%d healthy=%d (age=%v)",
			outNet, goodNet, stAged.Age)
	}

	// End S3 outage: outage-tenant drains cleanly through v2 Flush, freeing shared Budget and all workers.
	outageDown.Store(false)
	if err := vOutage.Flush(ctx); err != nil {
		t.Fatalf("outage-tenant recovery Flush: %v", err)
	}
	if st := vOutage.State(); st.Degraded || st.Published != st.Visible || st.Publishing != nil {
		t.Fatalf("outage-tenant post-recovery State = %+v", st)
	}
	if got := readAll(t, reopen(t, memOutage, ciph), inOut.ID); !bytes.Equal(got, accepted) {
		t.Fatalf("outage-tenant recovered bytes mismatch: got %d want %d", len(got), len(accepted))
	}
	if used := budget.Used(); used != 0 {
		t.Fatalf("shared vfs.Budget leaked %d bytes after recovery", used)
	}
	for _, a := range sched.Snapshot() {
		if a.ActiveWorkers != 0 {
			t.Fatalf("tenant %s leaked %d active workers", a.ID, a.ActiveWorkers)
		}
	}
}
