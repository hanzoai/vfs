// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command hare runs the reproducible HARE per-tenant multi-resource IO
// scheduling benchmark suite across 32 and 128 sandboxes (HIP-1342, Refs #23),
// reporting Plain NVMe beside unscheduled FCFS/LRU, static 1/N partitioning,
// and HARE cache-centric scheduling.
//
//	go run ./tools/hare
//	go run ./tools/hare -json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/hanzoai/vfs/pkg/hare"
)

func main() {
	dir := flag.String("dir", "", "scratch directory for plain NVMe host baseline")
	asJSON := flag.Bool("json", false, "emit machine-readable JSON")
	flag.Parse()

	rep, err := hare.RunSuite(context.Background(), *dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hare: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "hare: encode: %v\n", err)
			os.Exit(1)
		}
		return
	}

	nv := rep.HostNVMe
	fmt.Printf("HARE Per-Tenant Sandbox IO Scheduling Report (HIP-1342, Refs #23)\n")
	fmt.Printf("=================================================================\n")
	fmt.Printf("Plain NVMe Baseline : burst %.1f MiB/s | 4K read p50/p99 %.2f / %.2f us | fsync p50/p99 %.2f / %.2f us\n\n",
		nv.BurstWriteMBps, nv.RandReadP50Us, nv.RandReadP99Us, nv.FsyncP50Us, nv.FsyncP99Us)

	printMatrix := func(title string, rows []hare.ContentionRow) {
		fmt.Printf("%s\n", title)
		fmt.Printf("| Policy | Hit Rate | Jain Fairness | p50 (us) | p99 (us) | Interactive ops/s | Max Pub (s) | <= 5s |\n")
		fmt.Printf("|---|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, r := range rows {
			fmt.Printf("| %-18s | %6.2f%% | %.4f | %8.1f | %8.1f | %17.0f | %11.2f | %v |\n",
				r.Policy, r.InteractiveHitRatePct, r.JainsFairnessIndex,
				r.InteractiveP50Micros, r.InteractiveP99Micros,
				r.InteractiveOpsPerSec, r.MaxJournalPublishSec, r.PublishWithin5sAll)
		}
		fmt.Println()
	}

	printMatrix("1. 32 Sandboxes (24 interactive toolchains + 8 scan/ingest noisy neighbours)", rep.Matrix32)
	printMatrix("2. 128 Sandboxes (96 interactive toolchains + 32 scan/ingest noisy neighbours)", rep.Matrix128)

	nc := rep.NegativeControl
	fmt.Printf("3. Boundary & Negative Controls\n")
	fmt.Printf("- Uncontended single-tenant burst : %.1f MiB/s (admission overhead %.1f ns/op)\n",
		nc.SingleTenantBurstMBps, nc.AdmissionOverheadNs)
	fmt.Printf("- Homogeneous 32 identical tenants: static hit=%.1f%% (J=%.4f) vs HARE hit=%.1f%% (J=%.4f)\n",
		nc.HomogeneousStaticHitPct, nc.HomogeneousStaticFairness,
		nc.HomogeneousHAREHitPct, nc.HomogeneousHAREFairness)
}
