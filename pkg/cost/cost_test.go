package cost_test

import (
	"strings"
	"testing"

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
