// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/rubikfs"
)

func TestBenchmarkCorpusDeterminism(t *testing.T) {
	f1, b1, s1, err := buildVisorBaseImage("../..", 1344)
	if err != nil {
		t.Fatalf("buildVisorBaseImage #1: %v", err)
	}
	f2, b2, s2, err := buildVisorBaseImage("../..", 1344)
	if err != nil {
		t.Fatalf("buildVisorBaseImage #2: %v", err)
	}
	if s1.RepoFiles == 0 {
		t.Fatal("expected real repository files to be ingested")
	}
	if s1.TotalFiles != s2.TotalFiles || s1.TotalBytes != s2.TotalBytes || len(b1) != len(b2) {
		t.Fatalf("non-deterministic corpus: %+v vs %+v", s1, s2)
	}
	if len(f1) != len(f2) || len(b1) == 0 {
		t.Fatalf("unexpected corpus counts: files=%d boot=%d", len(f1), len(b1))
	}
}

func TestBenchmarkHarnessEndToEndContract(t *testing.T) {
	ctx := context.Background()
	files, bootPaths, summary, err := buildVisorBaseImage("../..", 1344)
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[string][]byte, len(files))
	for _, f := range files {
		expected[f.Path] = f.Data
	}

	scope := rubikfs.Scope{TenantID: "test-tenant", VolumeID: "test-vol"}

	pathRes, err := runRegime(ctx, summary.Name, files, expected, bootPaths, rubikfs.Config{
		Scope:           scope,
		ImageID:         "t-path",
		Policy:          rubikfs.PolicyPathOrder,
		Codec:           rubikfs.CodecLongLZFlate,
		ChunkSize:       16 << 10,
		TargetPackBytes: 1 << 20,
		Seed:            1344,
	})
	if err != nil {
		t.Fatalf("path regime: %v", err)
	}

	pureRes, err := runRegime(ctx, summary.Name, files, expected, bootPaths, rubikfs.Config{
		Scope:           scope,
		ImageID:         "t-pure",
		Policy:          rubikfs.PolicyPureSimilarity,
		Codec:           rubikfs.CodecLongLZFlate,
		ChunkSize:       16 << 10,
		TargetPackBytes: 1 << 20,
		Seed:            1344,
	})
	if err != nil {
		t.Fatalf("pure regime: %v", err)
	}

	hybRes, err := runRegime(ctx, summary.Name, files, expected, bootPaths, rubikfs.Config{
		Scope:           scope,
		ImageID:         "t-hyb",
		Policy:          rubikfs.PolicyLocalityHybrid,
		Codec:           rubikfs.CodecLongLZFlate,
		ChunkSize:       16 << 10,
		TargetPackBytes: 1 << 20,
		BootPackBytes:   256 << 10,
		Seed:            1344,
	})
	if err != nil {
		t.Fatalf("hybrid regime: %v", err)
	}

	if !pathRes.BitExactVerified || !pureRes.BitExactVerified || !hybRes.BitExactVerified {
		t.Fatal("expected bit-exact verification to pass for all regimes")
	}
	if pureRes.CompressionRatio <= pathRes.CompressionRatio {
		t.Fatalf("expected pure similarity (%.3fx) > path baseline (%.3fx)", pureRes.CompressionRatio, pathRes.CompressionRatio)
	}
	if hybRes.CompressionRatio <= pathRes.CompressionRatio {
		t.Fatalf("expected locality hybrid (%.3fx) > path baseline (%.3fx)", hybRes.CompressionRatio, pathRes.CompressionRatio)
	}
	if hybRes.ColdPacksFetched >= pureRes.ColdPacksFetched {
		t.Fatalf("expected hybrid cold packs (%d) < pure similarity (%d)", hybRes.ColdPacksFetched, pureRes.ColdPacksFetched)
	}
	if hybRes.ColdS3ModeledMs >= pureRes.ColdS3ModeledMs {
		t.Fatalf("expected hybrid cold S3 ms (%.2f) < pure similarity (%.2f)", hybRes.ColdS3ModeledMs, pureRes.ColdS3ModeledMs)
	}

	// Verify isolation audit contract.
	iso, err := runIsolationAudit(ctx, files)
	if err != nil {
		t.Fatalf("isolation audit: %v", err)
	}
	if iso.SharedBackendKeys != 0 || iso.IdenticalCiphertexts != 0 || iso.CrossVolumeDedupBytes != 0 {
		t.Fatalf("isolation audit violation: %+v", iso)
	}
	if !iso.CrossVolumeOpenRejected || !iso.TransplantRejected || !iso.SingleBitFlipRejected {
		t.Fatalf("expected all tamper/isolation checks rejected: %+v", iso)
	}

	// Verify markdown rendering covers required contract sections.
	rep := &BenchmarkReport{
		PrimaryCorpus:     summary,
		PrimaryComparison: []RegimeResult{pathRes, pathRes, pureRes, hybRes},
		IsolationAudit:    iso,
	}
	md := renderMarkdownReport(rep)
	for _, want := range []string{"RubikFS", "Tenant Isolation", "Negative Regime #1", "Negative Regime #2", "Negative Regime #3"} {
		if !strings.Contains(md, want) {
			t.Fatalf("rendered markdown missing section %q", want)
		}
	}
}
