// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command cofsbench runs the reproducible CoFS read-only minimal-perfect-hash
// lookup index benchmark suite (HIP-1342, Refs #25), reporting Plain NVMe
// beside unindexed vfs.Open and CoFS-indexed clones across cold-start clone
// bursts, warm deep-path Resolve, single-component Lookup, ENOENT storms, and
// a 100%-mutable-workspace boundary control.
//
//	go run ./tools/cofsbench
//	go run ./tools/cofsbench -full
//	go run ./tools/cofsbench -json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/hanzoai/vfs/pkg/cofs"
)

func main() {
	dir := flag.String("dir", "", "scratch directory (defaults to temporary directory on local disk)")
	full := flag.Bool("full", false, "run full-scale base image benchmark")
	asJSON := flag.Bool("json", false, "emit machine-readable JSON")
	flag.Parse()

	rep, err := cofs.RunSuite(context.Background(), *dir, *full)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cofsbench: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "cofsbench: encode: %v\n", err)
			os.Exit(1)
		}
		return
	}

	cc := rep.ColdClone
	fmt.Printf("CoFS Read-Only Lookup Index Report (HIP-1342, Refs #25)\n")
	fmt.Printf("=======================================================\n")
	fmt.Printf("Scale              : %s (%d base files, depth 6)\n", rep.Scale, cc.BaseFiles)
	fmt.Printf("Index size         : %d bytes (%.1f bytes/file), built in %.2f ms\n\n",
		cc.IndexBytes, cc.BytesPerFile, cc.BuildMs)

	fmt.Printf("1. Cloned Base Image Cold-Start (%d positive + %d ENOENT paths, %.0f ms S3 GET RTT)\n",
		cc.StartupPositivePaths, cc.StartupENOENTPaths, cc.SimulatedS3RTTMs)
	fmt.Printf("| System | Cold-Start Time (ms) | Total Store GETs | meta/ Shard GETs | vs Unindexed VFS |\n")
	fmt.Printf("|---|---:|---:|---:|---:|\n")
	fmt.Printf("| Plain NVMe | %.2f | n/a | n/a | baseline |\n", cc.PlainNVMeColdMs)
	fmt.Printf("| Unindexed vfs.Open (cold prolly tree) | %.2f | %d | %d | 1.00x |\n",
		cc.UnindexedColdMs, cc.UnindexedTotalGets, cc.UnindexedMetaGets)
	fmt.Printf("| CoFS Indexed Clone (head + 1 ext GET) | %.2f | %d | %d | -%.2f%% latency |\n\n",
		cc.CoFSColdMs, cc.CoFSTotalGets, cc.CoFSMetaGets, cc.ColdStartSpeedupPct)

	wl := rep.WarmLookups
	fmt.Printf("2. Warm Lookup Latency (%d iterations, depth-6 paths)\n", wl.Iterations)
	fmt.Printf("| Operation | Plain NVMe (ns/op) | Unindexed VFS (ns/op) | CoFS Index (ns/op) | Faster vs VFS |\n")
	fmt.Printf("|---|---:|---:|---:|---:|\n")
	fmt.Printf("| Whole-path Resolve (hit) | %.1f | %.1f | %.1f | -%.2f%% |\n",
		wl.PlainNVMeResolveNs, wl.UnindexedResolveNs, wl.CoFSResolveNs, wl.ResolveFasterPctVsVFS)
	fmt.Printf("| Whole-path Resolve (ENOENT) | %.1f | %.1f | %.1f | -%.2f%% |\n",
		wl.PlainNVMeENOENTNs, wl.UnindexedENOENTNs, wl.CoFSENOENTNs, wl.ENOENTFasterPctVsVFS)
	fmt.Printf("| Single-step Lookup(dir,name) | %.1f | %.1f | %.1f | -%.2f%% |\n\n",
		wl.PlainNVMeComponentNs, wl.UnindexedComponentNs, wl.CoFSComponentNs, wl.ComponentFasterPctVFS)

	wb := rep.WorkspaceBoundary
	fmt.Printf("3. Boundary Control: 100%% Mutable Workspace Lookups (%d dirty workspace files)\n", wb.MutatedFiles)
	fmt.Printf("| Path | Latency (ns/op) | Delta |\n")
	fmt.Printf("|---|---:|---:|\n")
	fmt.Printf("| Direct mutable vfs.Volume | %.1f | baseline |\n", wb.DirectWorkspaceNs)
	fmt.Printf("| LayeredResolver (workspace + CoFS base) | %.1f | %+.1f ns |\n", wb.LayeredWorkspaceNs, wb.OverheadNs)
}
