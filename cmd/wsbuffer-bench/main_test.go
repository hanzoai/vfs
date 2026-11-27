package main

import (
	"testing"
)

func TestBenchmarkSuiteQuickAndInvariants(t *testing.T) {
	bundle, err := RunSuite(t.TempDir(), 1, true)
	if err != nil {
		t.Fatalf("RunSuite quick failed: %v", err)
	}
	if len(bundle.Results) == 0 {
		t.Fatalf("expected non-empty benchmark results")
	}

	byKey := make(map[string]BenchResult)
	for _, r := range bundle.Results {
		if !r.Verified {
			t.Fatalf("result %s/%s failed verification", r.Workload, r.Engine)
		}
		if r.ThroughputMiBs <= 0 || r.IOPS <= 0 || r.WAF <= 0 {
			t.Fatalf("invalid metrics for %s/%s: %+v", r.Workload, r.Engine, r)
		}
		byKey[r.Workload+"/"+r.Engine] = r
	}

	// Verify 4 KiB random-write burst achieves lower WAF than journal-main.
	j4k := byKey["4k-rand-burst/journal-main"]
	w4k := byKey["4k-rand-burst/wsbuffer-journal"]
	if w4k.WAF >= j4k.WAF {
		t.Fatalf("expected wsbuffer 4k-rand-burst WAF (%.3f) < journal-main WAF (%.3f)", w4k.WAF, j4k.WAF)
	}

	// Verify 512 B unaligned partial writes reduce WAF significantly vs journal-main.
	j512 := byKey["partial-512b-unaligned/journal-main"]
	w512 := byKey["partial-512b-unaligned/wsbuffer-journal"]
	if w512.WAF >= j512.WAF {
		t.Fatalf("expected wsbuffer 512b WAF (%.3f) < journal-main WAF (%.3f)", w512.WAF, j512.WAF)
	}

	// Verify 256 KiB aligned sequential direct bypass achieves ~1.0x WAF vs ~2.0x for journal-main and no-bypass.
	j256 := byKey["large-seq-256k-aligned/journal-main"]
	nb256 := byKey["large-seq-256k-aligned/wsbuffer-no-bypass"]
	w256 := byKey["large-seq-256k-aligned/wsbuffer-journal"]
	if w256.WAF > 1.05 {
		t.Fatalf("expected direct bypass 256k WAF <= 1.05, got %.3f", w256.WAF)
	}
	if w256.WAF >= j256.WAF || w256.WAF >= nb256.WAF {
		t.Fatalf("expected direct bypass WAF (%.3f) < journal-main (%.3f) and no-bypass (%.3f)",
			w256.WAF, j256.WAF, nb256.WAF)
	}

	// Verify Negative Regime #1 (sync after every single 4 KiB write) honestly shows WAF >= journal-main or > 2.0.
	negSync := byKey["neg-sync-every-4k/wsbuffer-journal"]
	if negSync.WAF < 2.0 {
		t.Fatalf("expected neg-sync-every-4k WAF >= 2.0 (no coalescing), got %.3f", negSync.WAF)
	}

	// Verify Negative Regime #3 (undersized scrap buffer) triggers backpressure flushes and higher WAF than 4 MiB cap.
	u64 := byKey["neg-undersized-scrap-scatter/wsbuffer-cap-64k"]
	u4m := byKey["neg-undersized-scrap-scatter/wsbuffer-cap-4m"]
	if u64.BackpressureFlushes == 0 {
		t.Fatalf("expected undersized 64 KiB cap to trigger backpressure flushes")
	}
	if u64.WAF <= u4m.WAF {
		t.Fatalf("expected undersized cap WAF (%.3f) > 4 MiB cap WAF (%.3f)", u64.WAF, u4m.WAF)
	}
}
