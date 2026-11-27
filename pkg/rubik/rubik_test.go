package rubik

import (
	"slices"
	"testing"
)

// small is a workload the whole file shares: two tenants, a stacked lineage and
// near-copies, small enough that four plans over it cost a fraction of a second.
func small() *Workload {
	return Layered{Images: 40, Layers: 4, LayerChunks: 32, Deltas: 6, ChunkBytes: 4096}.Workload()
}

func allPlanners() []Planner {
	return []Planner{Arrival{}, BySize{}, ByContent{}, ByCoAccess{}}
}

// TestOrderIsAPermutation is the precondition of every other test: a planner that
// repeats or drops a chunk is not an order, and the packer would either panic or
// silently lose the chunk.
func TestOrderIsAPermutation(t *testing.T) {
	w := small()
	for _, p := range allPlanners() {
		order := p.Order(w)
		if len(order) != len(w.Chunks) {
			t.Fatalf("%s: order has %d of %d chunks", p.Name(), len(order), len(w.Chunks))
		}
		got := slices.Clone(order)
		slices.Sort(got)
		for i, c := range got {
			if c != i {
				t.Fatalf("%s: chunk %d appears at position %d", p.Name(), i, c)
			}
		}
	}
}

// TestPacksNeverMixTenants: strict isolation is a constraint of the store, not a
// preference of the planner, so no order may be able to defeat it.
func TestPacksNeverMixTenants(t *testing.T) {
	w := small()
	packer := Packer{Bytes: 64 << 10}
	for _, p := range allPlanners() {
		plan := packer.Plan(w, p.Order(w))
		if bad := plan.Isolation(w); bad != 0 {
			t.Fatalf("%s: %d packs mix tenants", p.Name(), bad)
		}
	}
}

// TestPlanHoldsEveryChunkOnce: the bytes a plan stores are the bytes of the
// workload, so a plan cannot buy its numbers by dropping or duplicating a chunk.
func TestPlanHoldsEveryChunkOnce(t *testing.T) {
	w := small()
	want := 0
	for _, c := range w.Chunks {
		want += len(c.Body) + FrameOverhead
	}
	packer := Packer{Bytes: 64 << 10}
	for _, p := range allPlanners() {
		if got := packer.Plan(w, p.Order(w)).Stored(w); got != want {
			t.Fatalf("%s: plan stores %d bytes, workload has %d", p.Name(), got, want)
		}
	}
}

// TestColdStartFetchesWholePacks: the pack is the unit of a GET, so a boot pays
// for every chunk travelling with the one it needs.
func TestColdStartFetchesWholePacks(t *testing.T) {
	w := &Workload{
		Chunks: []Chunk{
			{Tenant: "t", Body: make([]byte, 100)},
			{Tenant: "t", Body: make([]byte, 100)},
			{Tenant: "t", Body: make([]byte, 100)},
		},
		Images: []Image{{Name: "one", Tenant: "t", Chunks: []int{0}}},
	}
	frame := 100 + FrameOverhead

	alone := Packer{Bytes: frame}.Plan(w, []int{0, 1, 2})
	if got, need := alone.ColdStart(w, 0); got != frame || need != 100 {
		t.Fatalf("one chunk per pack: fetched %d needed %d, want %d and 100", got, need, frame)
	}
	together := Packer{Bytes: 3 * frame}.Plan(w, []int{0, 1, 2})
	if got, _ := together.ColdStart(w, 0); got != 3*frame {
		t.Fatalf("all chunks in one pack: fetched %d, want %d", got, 3*frame)
	}
}

// TestCoAccessOrderIsAClustering: the order it returns keeps every reader set in
// one piece, which is the property the plan is named after.
func TestCoAccessOrderIsAClustering(t *testing.T) {
	w := small()
	runs, groups := CoAccessRuns(w, ByCoAccess{}.Order(w))
	if runs != groups {
		t.Fatalf("co-access order has %d runs for %d reader sets", runs, groups)
	}
}

// TestContentIsNotTheCheapestPack is the counterfactual held to its word: the
// compressed bytes a content-clustered pack would hold are fewer than the bytes
// the store holds today, because a similarity order is a compression order.
func TestContentIsNotTheCheapestPack(t *testing.T) {
	w := small()
	packer := Packer{Bytes: 64 << 10}
	arrival := packer.Plan(w, Arrival{}.Order(w))
	content := packer.Plan(w, ByContent{}.Order(w))
	if a, c := arrival.Sealed(w), content.Sealed(w); c >= a {
		t.Fatalf("content sealed %d, arrival sealed %d: the order did not help compression", c, a)
	}
	if got, want := content.Stored(w), arrival.Stored(w); got != want {
		t.Fatalf("stored %d != %d: sealing does not compress, so no order may change it", got, want)
	}
}

// TestArrivalTiesCoAccessOnlyIfLayersArrivedInOrder is the finding in one test.
// A layered workload writes its layers in depth order, so the order chunks arrived
// in already groups them by reader set and a co-access planner has nothing left to
// win. Scatter the arrival of the layers and the same planner recovers what the
// history lost — the equivalence is a property of the store's history, not of the
// algorithm.
func TestArrivalTiesCoAccessOnlyIfLayersArrivedInOrder(t *testing.T) {
	packer := Packer{Bytes: 64 << 10}
	amp := func(cfg Layered) (arrival, coaccess float64) {
		w := cfg.Workload()
		pa := packer.Plan(w, Arrival{}.Order(w))
		pc := packer.Plan(w, ByCoAccess{}.Order(w))
		return pa.ColdStarts(w, "").SumAmp, pc.ColdStarts(w, "").SumAmp
	}
	base := Layered{Images: 40, Layers: 4, LayerChunks: 32, Deltas: 6, ChunkBytes: 4096}

	ordered := base
	a1, c1 := amp(ordered)
	if a1 > c1*1.01 {
		t.Fatalf("layers in depth order: arrival %.3f, co-access %.3f — arrival should tie", a1, c1)
	}

	scattered := base
	scattered.Scatter = true
	a2, c2 := amp(scattered)
	if a2 < c2*1.05 {
		t.Fatalf("scattered layers: arrival %.3f, co-access %.3f — co-access should win by 5%% or more", a2, c2)
	}
}

// TestPlansAreDeterministic: a bench that reshuffles its input is not a bench. No
// planner may read a random source or a map's iteration order.
func TestPlansAreDeterministic(t *testing.T) {
	w := small()
	for _, p := range allPlanners() {
		first, second := p.Order(w), p.Order(w)
		if !slices.Equal(first, second) {
			t.Fatalf("%s: two orders of one workload differ", p.Name())
		}
	}
}
