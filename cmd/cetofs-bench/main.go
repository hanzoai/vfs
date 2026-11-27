// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command cetofs-bench runs the reproducible CETOFS redundant-request audit
// benchmark suite for hanzoai/vfs#44 (HIP-1345) across four canonical
// workloads (npm_ci, checkout_5000, git_status, go_build) plus the negative
// multi-writer / random-read regime, comparing Plain NVMe, Unoptimized
// FUSE/Gofer -> *vfs.Volume -> S3/Backend, and CETOFS-Optimized stacks.
//
//	go run ./cmd/cetofs-bench
//	go run ./cmd/cetofs-bench -out docs/research/hip-1345-cetofs/results.json
//	go run ./cmd/cetofs-bench -rtt 2ms -files 5000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hanzoai/vfs/internal/cetofs"
)

func main() {
	dir := flag.String("dir", "", "scratch directory on local disk (defaults to temporary directory)")
	rtt := flag.Duration("rtt", 2*time.Millisecond, "simulated intra-region S3 network RTT per backend RPC")
	files := flag.Int("files", 5000, "file count for checkout_5000 and git_status")
	npmPkgs := flag.Int("npm-pkgs", 60, "package count for npm_ci")
	goPkgs := flag.Int("go-pkgs", 25, "package count for go_build")
	outPath := flag.String("out", "docs/research/hip-1345-cetofs/results.json", "path to write JSON receipts (empty to skip)")
	asJSON := flag.Bool("json", false, "print JSON report to stdout instead of formatted tables")
	flag.Parse()

	cfg := cetofs.SuiteConfig{
		ScratchDir:    *dir,
		SimulatedRTT:  *rtt,
		CheckoutFiles: *files,
		NpmPackages:   *npmPkgs,
		GoPackages:    *goPkgs,
	}

	rep, err := cetofs.RunSuite(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cetofs-bench: %v\n", err)
		os.Exit(1)
	}

	rawJSON, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cetofs-bench: marshal JSON: %v\n", err)
		os.Exit(1)
	}
	rawJSON = append(rawJSON, '\n')

	if *outPath != "" {
		if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "cetofs-bench: mkdir %s: %v\n", filepath.Dir(*outPath), err)
			os.Exit(1)
		}
		if err := os.WriteFile(*outPath, rawJSON, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "cetofs-bench: write %s: %v\n", *outPath, err)
			os.Exit(1)
		}
	}

	if *asJSON {
		_, _ = os.Stdout.Write(rawJSON)
		return
	}

	fmt.Printf("CETOFS Redundant-Request Audit Benchmark (HIP-1345, hanzoai/vfs#44)\n")
	fmt.Printf("===================================================================\n")
	fmt.Printf("Host: %s/%s (%d CPUs, %s) | Simulated S3 RTT: %.2f ms | Repo files: %d\n\n",
		rep.GOOS, rep.GOARCH, rep.NumCPU, rep.GoVersion, rep.SimulatedRTTMs, rep.CheckoutFiles)

	fmt.Printf("| Workload | Stack | App Ops | Wall (ms) | App p50/p95/p99 (us) | FUSE Reqs (per op) | S3/BE Reqs (per op) | Total Reduction |\n")
	fmt.Printf("|---|---|---:|---:|---:|---:|---:|---:|\n")
	for _, w := range rep.Workloads {
		printRow := func(label string, m cetofs.StackMetrics, note string) {
			fmt.Printf("| %s | %s | %d | %.2f | %.1f / %.1f / %.1f | %d (%.2f) | %d (%.2f) | %s |\n",
				w.Name, label, m.AppOps, m.WallTimeMs,
				m.AppOpLatency.P50Us, m.AppOpLatency.P95Us, m.AppOpLatency.P99Us,
				m.Boundary.FUSEBoundaryRequests, m.FUSEReqsPerAppOp,
				m.Boundary.BackendBoundaryRequests, m.BackendReqsPerAppOp,
				note)
		}
		fmtPct := func(reduction float64) string {
			if reduction >= 0 {
				return fmt.Sprintf("-%.1f%%", reduction)
			}
			return fmt.Sprintf("+%.1f%%", -reduction)
		}
		printRow("Plain NVMe", w.PlainNVMe, "local reference")
		printRow("Unoptimized VFS", w.Unoptimized, "baseline")
		printRow("CETOFS-Optimized", w.Optimized,
			fmt.Sprintf("%s reqs (FUSE %s, BE %s, wall %s)",
				fmtPct(w.TotalReqReductionPct), fmtPct(w.FUSEReqReductionPct),
				fmtPct(w.BackendReqReductionPct), fmtPct(w.WallTimeSpeedupPct)))
	}

	neg := rep.NegativeRegime
	fmt.Printf("\nNegative Regime (Multi-Writer CAS Contention, External TTL Staleness, Random Pack Reads)\n")
	fmt.Printf("----------------------------------------------------------------------------------------\n")
	fmt.Printf("1. Multi-writer Head CAS (%d contended commits): uncached=%d reqs (%.2f ms) vs cached=%d reqs (%.2f ms, %d conflicts, %+.1f%% reqs) | CAS authority preserved: %v\n",
		neg.CASIterations, neg.UncachedCASBackendReqs, neg.UncachedCASWallMs,
		neg.CachedContendedBackendReqs, neg.CachedContendedWallMs, neg.CachedContendedConflicts,
		neg.CASRequestOverheadPct, neg.CASAuthorityPreserved)
	fmt.Printf("2. External mutation under AttrTimeout: stale reads before invalidation=%d | immediate fresh read after Head-generation invalidation=%v\n",
		neg.StaleReadsBeforeInvalidation, neg.FreshAfterInvalidation)
	fmt.Printf("3. Random non-adjacent pack reads (%d ops, Plain NVMe=%.3f ms): unoptimized=%d B (1.00x) | aggressive AlwaysReadAhead=%d B (%.1fx read amp) | adaptive default=%d B (%.2fx)\n",
		neg.RandomReadOps, neg.PlainNVMeRandomReadMs,
		neg.UnoptimizedRandomBytesRead, neg.AggressiveCoalesceBytesRead, neg.AggressiveReadAmplificationX,
		neg.AdaptiveCoalesceBytesRead, neg.AdaptiveReadAmplificationX)

	if *outPath != "" {
		fmt.Printf("\nWrote JSON receipts to %s\n", *outPath)
	}
}
