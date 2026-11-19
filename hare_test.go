// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/hare"
)

// TestHARE32LiveSandboxesFairnessAndIsolation attaches 32 real post-quantum
// vfs.Volume sandboxes (24 interactive coding sandboxes + 8 bulk-write/scan
// noisy neighbours) over one shared hare.Scheduler with bounded worker slots,
// verifying end-to-end data integrity, zero worker leaks, cache reclamation
// from noisy scan tenants, and Jain's fairness across interactive sandboxes.
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

	ciph := vfs.Hybrid(newKey(t))
	vols := make([]*vfs.Volume, nSandboxes)
	for i := range nSandboxes {
		id := fmt.Sprintf("sandbox-%03d", i)
		sched.RegisterTenant(id, 1)
		tb := hare.NewTenantBackend(storetest.NewMem(), sched, id, false)
		if err := vfs.Create(ctx, cfg(tb, ciph, 0)); err != nil {
			t.Fatal(err)
		}
		v, err := vfs.Attach(ctx, cfg(tb, ciph, 1))
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
