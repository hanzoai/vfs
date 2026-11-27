// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBenchSuiteEndToEndAndReportOutputs(t *testing.T) {
	cfg := BenchConfig{
		Seed:     1342,
		OutDir:   t.TempDir(),
		BurstMB:  4,
		FsyncOps: 8,
		MemoryMB: 1,
		StormOps: 8,
	}
	res, err := RunSuite(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunSuite: %v", err)
	}
	if len(res.Positive) != 4 {
		t.Fatalf("positive regimes = %d, want 4", len(res.Positive))
	}
	if len(res.Negative) != 3 {
		t.Fatalf("negative regimes = %d, want 3", len(res.Negative))
	}
	for _, r := range res.Positive {
		if !r.BitExactVerified {
			t.Fatalf("positive regime %s failed bit-exact verification", r.Name)
		}
		if r.CleanCloseS3Requests != 0 {
			t.Fatalf("positive regime %s clean close S3 requests = %d, want 0", r.Name, r.CleanCloseS3Requests)
		}
		if r.PublishLatencyMS >= 5000 {
			t.Fatalf("positive regime %s publish latency %.2f ms >= 5000 ms", r.Name, r.PublishLatencyMS)
		}
	}
	if !res.Negative[0].FailClosedVerified {
		t.Fatalf("negative regime 1 must verify fail-closed rejection")
	}
	if err := WriteOutputs(cfg.OutDir, res); err != nil {
		t.Fatalf("WriteOutputs: %v", err)
	}

	rawJSON, err := os.ReadFile(filepath.Join(cfg.OutDir, "results.json"))
	if err != nil {
		t.Fatalf("read results.json: %v", err)
	}
	var decoded StudyResults
	if err := json.Unmarshal(rawJSON, &decoded); err != nil {
		t.Fatalf("unmarshal results.json: %v", err)
	}
	rawMD, err := os.ReadFile(filepath.Join(cfg.OutDir, "REPORT.md"))
	if err != nil {
		t.Fatalf("read REPORT.md: %v", err)
	}
	md := string(rawMD)
	for _, mustContain := range []string{
		"plain_nvme_baseline",
		"gofer_hop_baseline",
		"ext_fuse_socket_fd",
		"ext_fuse_s3_checkpoint_restore",
		"neg_unquiesced_dirty_checkpoint",
		"neg_tiny_sync_4k_metadata_storm",
		"neg_uncompressed_high_entropy_memory_checkpoint",
	} {
		if !strings.Contains(md, mustContain) {
			t.Fatalf("REPORT.md missing required regime %q", mustContain)
		}
	}
}
