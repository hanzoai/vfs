package main

import (
	"strings"
	"testing"
	"time"
)

func TestSummarizeLatenciesAndReportRender(t *testing.T) {
	samples := []time.Duration{
		500 * time.Microsecond,
		100 * time.Microsecond,
		300 * time.Microsecond,
		200 * time.Microsecond,
		400 * time.Microsecond,
	}
	st := summarizeLatencies(samples)
	if st.Ops != 5 || st.P50US != 300 || st.MaxUS != 500 {
		t.Fatalf("unexpected latency summary: %+v", st)
	}

	res := &BenchResults{
		SchemaVersion: "hip-1342-writeback-v1",
		GeneratedAt:   "2026-11-26T00:00:00Z",
		Environment: EnvInfo{
			OS:       "linux",
			Arch:     "amd64",
			BurstMiB: 64,
			FsyncOps: 100,
		},
		PlainDisk: PlainDiskMetrics{
			BurstBufferedMBs: 1500,
			BurstBufferedGBs: 1.5,
			BurstDurableMBs:  800,
			BurstDurableGBs:  0.8,
			FsyncLatency:     st,
		},
	}
	md := renderMarkdownReport(res)
	for _, want := range []string{
		"BufferedInKernelPageCache",
		"FsyncDurable",
		"PublishedToObjectStore",
		"FUSE Passthrough",
		"FUSE-over-`io_uring`",
		"Honest Negative Regimes",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("REPORT.md missing required section %q", want)
		}
	}
}
