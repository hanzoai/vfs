package main

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestHeapPeakSamplerCapturesDuringTransferPeak verifies that heapPeakSampler
// records transient heap spikes during execution rather than only reporting
// post-GC HeapInuse after buffers have been released.
func TestHeapPeakSamplerCapturesDuringTransferPeak(t *testing.T) {
	runtime.GC()
	sampler := startHeapPeakSampler(5 * time.Millisecond)

	const spikeBytes = 24 << 20 // 24 MiB transient allocation
	spike := make([]byte, spikeBytes)
	for i := 0; i < len(spike); i += 4096 {
		spike[i] = byte(i)
	}
	// Hold long enough for multiple sampler ticks while spike is live.
	time.Sleep(45 * time.Millisecond)
	runtime.KeepAlive(spike)
	spike = nil

	// Force GC before stopping the sampler so post-transfer HeapInuse drops.
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	peakBytes, samples := sampler.Stop()
	if samples < 3 {
		t.Fatalf("expected >= 3 periodic samples during window, got %d", samples)
	}
	if peakBytes < after.HeapInuse+(16<<20) {
		t.Fatalf("sampler failed to capture transient heap peak: peak=%.2f MiB postGC=%.2f MiB",
			float64(peakBytes)/(1024*1024), float64(after.HeapInuse)/(1024*1024))
	}
}

// TestRunMultiGiBThreeMirrorsReportsPeakHeap verifies runMultiGiBThreeMirrors
// runs end-to-end with the continuous peak heap sampler and reports PeakHeapMiB.
func TestRunMultiGiBThreeMirrorsReportsPeakHeap(t *testing.T) {
	ctx := context.Background()
	// 16 MiB (0.015625 GiB) with 1 MiB chunks for a fast deterministic test.
	res, err := runMultiGiBThreeMirrors(ctx, 16.0/1024.0, 1, 2)
	if err != nil {
		t.Fatalf("runMultiGiBThreeMirrors: %v", err)
	}
	if !res.Passed {
		t.Fatalf("expected scenario to pass, got %+v", res)
	}
	if res.OriginHitsAfterOff != 0 {
		t.Fatalf("expected 0 origin hits after shutdown, got %d", res.OriginHitsAfterOff)
	}
	if res.PeakHeapMiB <= 0 {
		t.Fatalf("expected positive sampled PeakHeapMiB, got %.4f", res.PeakHeapMiB)
	}
}
