// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command mostbench runs the reproducible MOST selective hot-data mirroring
// vs ordinary tier-migration benchmark suite (HIP-1344, #62), measuring real
// disk-backed NVMe caching, backend GET counts, demotion write amplification,
// P50/P95/P99 nanosecond read latencies, MemStats allocations, and a 100%-cold
// zero-reuse boundary control.
//
//	go run ./tools/mostbench
//	go run ./tools/mostbench -full -csv docs/most.csv
//	go run ./tools/mostbench -json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/hanzoai/vfs/pkg/most"
)

func main() {
	dir := flag.String("dir", "", "scratch directory for NVMe and file store (defaults to temp dir)")
	full := flag.Bool("full", false, "run full-scale benchmark")
	asJSON := flag.Bool("json", false, "emit JSON report")
	csvPath := flag.String("csv", "", "optional path to write CSV rows")
	flag.Parse()

	rep, err := most.RunBenchmarkSuite(context.Background(), *dir, *full)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mostbench: %v\n", err)
		os.Exit(1)
	}

	if *csvPath != "" {
		f, err := os.Create(*csvPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mostbench: create csv: %v\n", err)
			os.Exit(1)
		}
		if err := most.WriteCSV(f, rep.Rows); err != nil {
			_ = f.Close()
			fmt.Fprintf(os.Stderr, "mostbench: write csv: %v\n", err)
			os.Exit(1)
		}
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "mostbench: close csv: %v\n", err)
			os.Exit(1)
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "mostbench: encode json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("MOST Selective Hot-Data Mirroring Report (HIP-1344, #62)\n")
	fmt.Printf("=========================================================\n")
	fmt.Printf("Scale: %s | Platform: %s/%s (%d CPUs)\n\n", rep.Scale, rep.GOOS, rep.GOARCH, rep.NumCPU)
	fmt.Printf("| Workload | System | Delay (ms) | Ops | Wall (ms) | P50 (us) | P95 (us) | P99 (us) | Backend GETs | RAM Hits | NVMe Hits | Promotions | Demotion Writes | Res RAM (KiB) | Res NVMe (KiB) |\n")
	fmt.Printf("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rep.Rows {
		fmt.Printf("| %s | %s | %.1f | %d | %.2f | %.1f | %.1f | %.1f | %d | %d | %d | %d | %d | %.1f | %.1f |\n",
			r.Workload, r.System, r.InjectedDelayMs, r.Ops, r.TotalWallMs,
			r.P50Us, r.P95Us, r.P99Us,
			r.BackendGets, r.RAMHits, r.NVMeHits,
			r.MirrorPromotions, r.TierMigrationWrites,
			float64(r.ResidentRAMBytes)/1024.0, float64(r.ResidentNVMeBytes)/1024.0)
	}
}
