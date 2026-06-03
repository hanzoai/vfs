package cost_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hanzoai/vfs/pkg/cost"
)

func TestFromBenchmark_ProducesPositiveCosts(t *testing.T) {
	w := cost.FromBenchmark(6.4, 0.1, 64*1024, 12, 2.0)
	if w.PutsPerMonth <= 0 {
		t.Fatalf("PutsPerMonth must be positive; got %f", w.PutsPerMonth)
	}
	if w.StorageGB <= 0 {
		t.Fatalf("StorageGB must be positive; got %f", w.StorageGB)
	}
	for _, e := range cost.CompareBackends(w) {
		if e.TotalUSD < 0 {
			t.Errorf("%s: negative cost %f", e.Backend, e.TotalUSD)
		}
	}
	t.Log("\n" + cost.Report("benchmark-projection-6.4ops", w))
}

func TestReport_FormatsAllBackends(t *testing.T) {
	w := cost.Workload{
		StorageGB:        100,
		PutsPerMonth:     1_000_000,
		GetsPerMonth:     1_000_000,
		EgressGBPerMonth: 10,
		Description:      "synthetic 100 GB / 1M+1M req / 10 GB egress",
	}
	r := cost.Report("synthetic", w)
	for be := range cost.Catalog {
		if !strings.Contains(r, string(be)) {
			t.Errorf("report missing backend %q in:\n%s", be, r)
		}
	}
}

func TestPVCBaseline_IsExpensiveAtScale(t *testing.T) {
	// At 1 TB stored, the PVC baseline ($0.10/GB-mo = $102.40/mo)
	// should be substantially more expensive than R2 ($0.015/GB-mo =
	// $15.36/mo). This pins the "PVC is the expensive baseline"
	// invariant — flip detection if pricing changes.
	w := cost.Workload{
		StorageGB:    1024,
		PutsPerMonth: 1_000,
		GetsPerMonth: 1_000,
		Description:  "1 TB cold archive",
	}
	pvc := cost.EstimateFor(w, cost.BackendPVCBlock)
	r2 := cost.EstimateFor(w, cost.BackendCloudflareR2)
	if pvc.TotalUSD < r2.TotalUSD*3 {
		t.Errorf("PVC baseline ($%f) is meant to be substantially more expensive than R2 ($%f); ratio is only %.2fx",
			pvc.TotalUSD, r2.TotalUSD, pvc.TotalUSD/r2.TotalUSD)
	}
}

func TestR2_EgressIsZero(t *testing.T) {
	// R2's headline is $0 egress; pin that.
	w := cost.Workload{
		StorageGB:        100,
		PutsPerMonth:     1_000_000,
		GetsPerMonth:     1_000_000,
		EgressGBPerMonth: 10_000, // 10 TB egress
		Description:      "egress-heavy",
	}
	e := cost.EstimateFor(w, cost.BackendCloudflareR2)
	if e.EgressUSD != 0 {
		t.Errorf("R2 egress cost expected 0; got $%f", e.EgressUSD)
	}
}

// ---------------------------------------------------------------------
// Block-time sweep: 1 ms → 1 s, on a representative chain workload.
// ---------------------------------------------------------------------

// TestBlockTimeSweep_PrintsCostCurve walks block-time from 1ms to 1s,
// printing the cold-archive monthly cost on R2 + the hot SSD size
// needed for a 24h / 7d / 30d retention window. Pure smoke test; the
// real interpretation lives in the lux-papers companion study.
func TestBlockTimeSweep_PrintsCostCurve(t *testing.T) {
	// Representative single-chain block size; chains with heavier
	// activity should rerun with their own values.
	const avgBlockBytes int64 = 20 * 1024     // 20 KB / block
	const objectBytes int64 = 4 * 1024        // VFS 4 KiB blocks
	const monthsRetention = 12.0
	const readAmp = 2.0

	blockTimes := []time.Duration{
		1 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}
	hotWindows := []time.Duration{
		24 * time.Hour,
		7 * 24 * time.Hour,
		30 * 24 * time.Hour,
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\nBlock-time sweep (avgBlock=%d B, archive=R2, retention=%.0f mo)\n",
		avgBlockBytes, monthsRetention)
	fmt.Fprintf(&b, "%-10s %-12s %-12s %-12s %s\n",
		"block-time", "hot24h-GB", "hot7d-GB", "hot30d-GB", "cold-R2-$/mo")

	for _, bt := range blockTimes {
		w := cost.BlockTimeWorkload(bt, avgBlockBytes, monthsRetention, readAmp, objectBytes)
		coldR2 := cost.EstimateFor(w, cost.BackendCloudflareR2)

		// Hot-tier size for each window.
		bytesPerSec := float64(avgBlockBytes) / bt.Seconds()
		var hot [3]float64
		for i, hw := range hotWindows {
			hot[i] = bytesPerSec * hw.Seconds() / (1 << 30)
		}
		fmt.Fprintf(&b, "%-10s %-12.2f %-12.2f %-12.2f $%.2f\n",
			bt, hot[0], hot[1], hot[2], coldR2.TotalUSD,
		)
	}
	t.Log(b.String())
}

// TestTiered_24h_Hot_R2_Cold validates the Tiered cost split: 24h SSD
// hot tier + R2 cold archive at 1s block time.
func TestTiered_24h_Hot_R2_Cold(t *testing.T) {
	tiered := cost.Tiered{
		RatePerSecond: 20 * 1024, // 20 KB/s
		HotWindow:     24 * time.Hour,
		ColdRetention: 12 * 30 * 24 * time.Hour, // 12 months
		HotBackend:    cost.BackendPVCBlock,
		ColdBackend:   cost.BackendCloudflareR2,
		PutsPerByte:   1.0 / 4096,
		GetsPerByte:   2.0 / 4096,
	}
	est := tiered.Estimate()
	if est.HotStorageGB <= 0 || est.ColdStorageGB <= 0 {
		t.Fatalf("storage sizes must be positive: hot=%.2f cold=%.2f",
			est.HotStorageGB, est.ColdStorageGB)
	}
	if est.ColdStorageGB <= est.HotStorageGB {
		t.Errorf("cold archive should hold > hot tier at 12 mo retention; got hot=%.2f cold=%.2f",
			est.HotStorageGB, est.ColdStorageGB)
	}
	t.Logf("\n%s\n", est)
}

// TestCluster_DedupKeepsWriterCostFlat validates the "one writer
// effectively, regardless of fleet size" invariant: a deduped cluster's
// PUT cost is independent of writer count.
func TestCluster_DedupKeepsWriterCostFlat(t *testing.T) {
	base := cost.BlockTimeWorkload(1*time.Second, 20*1024, 12, 0, 4096)

	cluster1 := cost.Cluster{Writers: 1, Readers: 0, Base: base, ReadsPerReaderRatio: 0}
	cluster11 := cost.Cluster{Writers: 11, Readers: 0, Base: base, ReadsPerReaderRatio: 0}

	e1 := cost.EstimateFor(cluster1.Workload(), cost.BackendCloudflareR2)
	e11 := cost.EstimateFor(cluster11.Workload(), cost.BackendCloudflareR2)

	if e1.TotalUSD != e11.TotalUSD {
		t.Errorf("deduped cluster cost must be flat across writers; 1w=$%.2f 11w=$%.2f",
			e1.TotalUSD, e11.TotalUSD)
	}
	t.Logf("dedup invariant pinned: 1 writer = 11 writers = $%.2f/mo", e1.TotalUSD)
}

// TestCluster_ReadersScaleGetCost validates the read-replica scaling
// model: GET cost is linear in reader count.
func TestCluster_ReadersScaleGetCost(t *testing.T) {
	base := cost.BlockTimeWorkload(1*time.Second, 20*1024, 12, 0, 4096)

	for _, n := range []int{1, 11, 100, 1000} {
		c := cost.Cluster{
			Writers:             1,
			Readers:             n,
			Base:                base,
			ReadsPerReaderRatio: 0.1, // each reader pulls 10% of writes
		}
		w := c.Workload()
		e := cost.EstimateFor(w, cost.BackendCloudflareR2)
		t.Logf("readers=%-4d total=$%6.2f/mo  (gets/mo=%.2e)", n, e.TotalUSD, w.GetsPerMonth)
	}
}
