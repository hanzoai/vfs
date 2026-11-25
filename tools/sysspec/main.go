// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command sysspec evaluates the structured storage specifications of vfs
// (HIP-1349, Refs #56), runs the generated regression suite, audits
// tools/mutate/mutations.json, and prints the bidirectional comparison table
// showing which fault classes SYSSPEC catches that existing mutation/fault
// tests miss and the reverse.
//
//	go run ./tools/sysspec
//	go run ./tools/sysspec -json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/hanzoai/vfs/pkg/sysspec"
)

func main() {
	mutPath := flag.String("mutations", "tools/mutate/mutations.json", "path to mutations.json")
	asJSON := flag.Bool("json", false, "emit machine-readable JSON summary")
	flag.Parse()

	var raw []byte
	if *mutPath != "" {
		b, err := os.ReadFile(*mutPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sysspec: read %s: %v\n", *mutPath, err)
			os.Exit(1)
		}
		raw = b
	}

	rep, err := sysspec.BuildComparativeMatrix(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sysspec: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "sysspec: encode: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("SYSSPEC Storage Specification Report (HIP-1349)\n")
	fmt.Printf("===============================================\n")
	fmt.Printf("Specification rules : %d total (%d invariants, %d transitions, %d Rely/Guarantee, %d concurrency)\n",
		rep.RuleCountTotal, rep.RuleCountInvariants, rep.RuleCountTransitions,
		rep.RuleCountRelyGuarantees, rep.RuleCountConcurrency)
	fmt.Printf("Generated cases     : %d (%.1f%% exact verdict match)\n",
		rep.GeneratedRegressions, rep.GeneratedPassRatePct)
	fmt.Printf("Upstream mutations  : %d total (journal=%d, fence=%d, corruption/codec=%d, publication=%d, tree/chunk/pack=%d, backend/mount/ext=%d)\n",
		rep.UpstreamMutations.Total,
		rep.UpstreamMutations.ByDomain["journal"],
		rep.UpstreamMutations.ByDomain["fence"],
		rep.UpstreamMutations.ByDomain["corruption_sealing_codec"],
		rep.UpstreamMutations.ByDomain["publication_volume"],
		rep.UpstreamMutations.ByDomain["tree_chunk_pack_ops"],
		rep.UpstreamMutations.ByDomain["backend_mount_ext"])
	fmt.Printf("Comparative matrix  : %d cases -> %d caught by both, %d SYSSPEC-only, %d existing-only (reverse)\n",
		len(rep.Cases), rep.BothCatchCount, rep.SpecOnlyCount, rep.ExistingOnlyCount)
	fmt.Printf("Evaluation latency  : %d us\n\n", rep.EvalDurationMicros)

	fmt.Printf("| ID  | Class         | Domain      | Spec | Existing | Rule / Reference |\n")
	fmt.Printf("|-----|---------------|-------------|------|----------|------------------|\n")
	for _, c := range rep.Cases {
		ref := c.SpecRule
		if ref == "" {
			ref = c.ExistingRef
		}
		fmt.Printf("| %-3s | %-13s | %-11s | %-4v | %-8v | %s |\n",
			c.ID, c.Class, c.Domain, c.SpecCaught, c.ExistingCaught, ref)
	}
}
