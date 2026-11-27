package rubik

import (
	"encoding/binary"
	"sort"
)

// ByCoAccess groups chunks by the exact set of images that read them and emits the
// groups from the most shared to the least, so a pack holds chunks with the same
// readers and a boot touches as few packs as its chunk count allows.
//
// It is the exact co-access clustering, not a greedy approximation: two chunks
// travel together when their reader sets are equal, the finest partition that
// leaves a pack's readers identical.
type ByCoAccess struct{}

func (ByCoAccess) Name() string { return "coaccess" }

func (ByCoAccess) Order(w *Workload) []int {
	inv := w.Inverted()
	type group struct {
		readers int
		first   int
		chunks  []int
	}
	byKey := make(map[string]*group, len(w.Chunks))
	groups := make([]*group, 0, len(w.Chunks))
	for c := range w.Chunks {
		key := w.Chunks[c].Tenant + "\x00" + readerKey(inv[c])
		g, ok := byKey[key]
		if !ok {
			g = &group{readers: len(inv[c]), first: c}
			byKey[key] = g
			groups = append(groups, g)
		}
		g.chunks = append(g.chunks, c)
	}
	sort.SliceStable(groups, func(a, b int) bool {
		ga, gb := groups[a], groups[b]
		if ta, tb := w.Chunks[ga.first].Tenant, w.Chunks[gb.first].Tenant; ta != tb {
			return ta < tb
		}
		if ga.readers != gb.readers {
			return ga.readers > gb.readers
		}
		if len(ga.chunks) != len(gb.chunks) {
			return len(ga.chunks) > len(gb.chunks)
		}
		return ga.first < gb.first
	})
	order := make([]int, 0, len(w.Chunks))
	for _, g := range groups {
		order = append(order, g.chunks...)
	}
	return order
}

func readerKey(readers []int) string {
	if len(readers) == 0 {
		return ""
	}
	b := make([]byte, 4*len(readers))
	for i, r := range readers {
		binary.LittleEndian.PutUint32(b[4*i:], uint32(r))
	}
	return string(b)
}

// CoAccessRuns reports how many maximal stretches of order share one reader set,
// and how many distinct reader sets the workload has. Equal counts mean the order
// is a co-access clustering — every group sits in one piece — which is a
// structural fact about the order, not a reading of any metric.
func CoAccessRuns(w *Workload, order []int) (runs, groups int) {
	inv := w.Inverted()
	seen := make(map[string]bool, len(w.Chunks))
	prev := "\x00"
	for _, c := range order {
		k := w.Chunks[c].Tenant + "\x00" + readerKey(inv[c])
		if !seen[k] {
			seen[k] = true
			groups++
		}
		if k != prev {
			runs++
			prev = k
		}
	}
	return runs, groups
}
