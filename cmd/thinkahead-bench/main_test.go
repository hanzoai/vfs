// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBenchQuickEndToEndAndOutputs(t *testing.T) {
	ctx := context.Background()
	rep, err := RunAll(ctx, RunOptions{
		SimulatedS3RTT: 1 * time.Millisecond,
		Quick:          true,
		SkipLive:       true,
	})
	if err != nil {
		t.Fatalf("RunAll(quick): %v", err)
	}
	if len(rep.Workflows) != 6 {
		t.Fatalf("expected 6 workflows, got %d", len(rep.Workflows))
	}
	if !rep.LiveVolume.ZeroSyncS3OnClose {
		t.Fatalf("expected zero sync S3 requests on clean Close, got %d", rep.LiveVolume.SyncS3RequestsOnClose)
	}
	if !rep.LiveVolume.PublicationWithin5s {
		t.Fatalf("expected publication within 5s, got %.1f ms", rep.LiveVolume.PublicationLatencyMs)
	}
	if !rep.NegativeRegimes.StaleMismatchedTrace.AdaptiveThrottledPrefetch.ThrottledPaused {
		t.Fatalf("expected Neg-2 adaptive throttle to pause on stale trace")
	}

	outDir := t.TempDir()
	if err := WriteOutputs(outDir, rep); err != nil {
		t.Fatalf("WriteOutputs: %v", err)
	}
	for _, fn := range []string{"REPORT.md", "results.json", "trace_dataset_summary.json"} {
		data, err := os.ReadFile(filepath.Join(outDir, fn))
		if err != nil {
			t.Fatalf("missing output %s: %v", fn, err)
		}
		if len(data) == 0 {
			t.Fatalf("empty output %s", fn)
		}
	}
	md := RenderMarkdown(rep)
	for _, want := range []string{"Plain Local Disk", "Negative Regime 1", "Negative Regime 2", "Negative Regime 3", "vfs_thinkahead_trace_prefetch"} {
		if !strings.Contains(md, want) {
			t.Fatalf("REPORT.md missing section %q", want)
		}
	}
}
