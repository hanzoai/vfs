// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/gitvfs"
)

func TestBenchCLIGeneratesJSONAndMarkdown(t *testing.T) {
	ctx := context.Background()
	r, err := gitvfs.NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	// Build a small real git repo as the CLI input corpus.
	repo := filepath.Join(t.TempDir(), "mini-vfs")
	for _, sub := range []string{"pkg/gitvfs", "corpus/core", "docs"} {
		if err := os.MkdirAll(filepath.Join(repo, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Run(ctx, repo, "init", "--quiet"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "pkg/gitvfs/a.go"), []byte("package gitvfs\n// Volume test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "corpus/core/b.go"), []byte("package core\n// Volume core\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "docs/readme.md"), []byte("# Mini\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(ctx, repo, "add", "-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(ctx, repo, "commit", "--quiet", "-m", "init"); err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(t.TempDir(), "out")
	var stdout, stderr bytes.Buffer
	args := []string{
		"--repo", repo,
		"--extra-files", "24",
		"--bulk-stage", "40",
		"--reps", "1",
		"--cow-sandboxes", "2",
		"--out-dir", outDir,
		"--json",
	}
	if err := run(ctx, args, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatalf("cli run failed: %v\nstderr: %s", err, stderr.String())
	}

	var report gitvfs.BenchReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if report.Issue != "hanzoai/vfs#29" || report.HIP != "HIP-1342" {
		t.Errorf("unexpected issue/hip: %s / %s", report.Issue, report.HIP)
	}
	if !report.ClosePublishCheck.ZeroSyncPutOnClose || !report.ClosePublishCheck.PublishedWithinBound {
		t.Errorf("close/publish invariant failed: %+v", report.ClosePublishCheck)
	}
	if len(report.Modes) < 6 {
		t.Errorf("expected >= 6 benchmark modes, got %d", len(report.Modes))
	}
	if len(report.NegativeRegimes) < 3 {
		t.Errorf("expected >= 3 negative regimes, got %d", len(report.NegativeRegimes))
	}
	if report.COWClone.ClonePackPutsEach != 0 || report.COWClone.BaseWritesDuringClone != 0 {
		t.Errorf("unexpected COW clone writes: %+v", report.COWClone)
	}

	mdBytes, err := os.ReadFile(filepath.Join(outDir, "REPORT.md"))
	if err != nil {
		t.Fatalf("missing REPORT.md: %v", err)
	}
	if !strings.Contains(string(mdBytes), "Honest Negative Regimes") {
		t.Error("REPORT.md missing Honest Negative Regimes section")
	}
	if _, err := os.Stat(filepath.Join(outDir, "results.json")); err != nil {
		t.Fatalf("missing results.json: %v", err)
	}
}
