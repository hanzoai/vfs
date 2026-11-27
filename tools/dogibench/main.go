// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command dogibench runs the reproducible DOGI lifetime-aware object packing
// benchmark suite (HIP-1344, Refs #63), comparing single-stream FIFO co-packing
// against held-out learned 3-stream DOGI and Oracle (NoDaP) upper-bound modes,
// plus adversarial misprediction sweeps, PackTarget sensitivity, and boundary
// negative controls.
//
//	go run ./tools/dogibench
//	go run ./tools/dogibench -full
//	go run ./tools/dogibench -csv
//	go run ./tools/dogibench -json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hanzoai/vfs/pkg/dogi"
)

func main() {
	dir := flag.String("dir", "", "scratch directory (defaults to temporary directory)")
	full := flag.Bool("full", false, "run full-scale benchmark workload")
	simRTT := flag.Duration("rtt", 0, "optional simulated per-request S3 RTT delay (labeled separately from unthrottled runs)")
	asJSON := flag.Bool("json", false, "emit machine-readable JSON")
	asCSV := flag.Bool("csv", false, "emit CSV rows matching docs/dogi.csv")
	csvOut := flag.String("csv-out", "", "optional path to write CSV alongside stdout report")
	flag.Parse()

	rep, err := dogi.RunSuite(context.Background(), dogi.SuiteConfig{
		ScratchDir:   *dir,
		FullScale:    *full,
		SimulatedRTT: *simRTT,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dogibench: %v\n", err)
		os.Exit(1)
	}

	if *csvOut != "" {
		if err := os.WriteFile(*csvOut, []byte(dogi.FormatCSV(rep)), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "dogibench: write csv %s: %v\n", *csvOut, err)
			os.Exit(1)
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "dogibench: encode: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *asCSV {
		fmt.Print(dogi.FormatCSV(rep))
		return
	}

	fmt.Printf("DOGI Lifetime-Aware Object Packing Report (HIP-1344, Refs #63)\n")
	fmt.Printf("==============================================================\n")
	fmt.Printf("Scale              : %s (train=%d, held-out=%d)\n", rep.Scale, rep.TrainSamples, rep.HeldOutSamples)
	fmt.Printf("Held-out classifier: %.2f%% accuracy (%d/%d correct, %d Ephemeral<->Long severe swaps)\n\n",
		rep.HeldOutEval.Accuracy*100.0, rep.HeldOutEval.Correct, rep.HeldOutEval.Total, rep.HeldOutEval.SevereMispredictions)

	printTable := func(title string, rows []dogi.ModeMetrics) {
		fmt.Printf("%s\n", title)
		fmt.Printf("| Name | Streams | Target | Acc | Copied (B) | Written (B) | WAF | InitPacks | GCPacks | TotalPacks | Del | Pack OH | Reqs | Wall (ms) |\n")
		fmt.Printf("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, r := range rows {
			accStr := "n/a"
			if r.NumStreams > 1 {
				accStr = fmt.Sprintf("%.1f%%", r.ClassifierAccuracy*100.0)
			}
			fmt.Printf("| %s | %d | %d KiB | %s | %d | %d | %.4f | %d | %d | %d | %d | %+.1f%% | %d | %.2f |\n",
				r.Name, r.NumStreams, r.PackTargetBytes>>10, accStr,
				r.LiveBytesCopied, r.PhysicalBytesWritten, r.WriteAmplification,
				r.InitialPacksCreated, r.CompactionPacksCreated, r.PacksCreated,
				r.PacksDeleted, r.PackCountOverheadPct,
				r.TotalBackendRequests, r.WallMs)
		}
		fmt.Println()
	}

	printTable("1. Primary Held-Out Evaluation (Unthrottled file:// + vfs.Hybrid)", rep.PrimaryRuns)
	printTable("2. Adversarial Misprediction Sweep (Injected Label Flip Rate)", rep.AdversarialSweep)
	printTable("3. PackTarget Sensitivity Sweep", rep.PackTargetSweep)
	printTable("4. Boundary Negative Control (Mixed-Class Archive, Zero Expirations)", rep.BoundaryControls)
	if len(rep.SimulatedRTTChecks) > 0 {
		printTable(fmt.Sprintf("5. Labeled Simulated RTT (%v per backend request)", (*simRTT).Round(time.Microsecond)), rep.SimulatedRTTChecks)
	}
}
