package rubik

import (
	"slices"
	"sort"
)

// ByContent orders chunks so that chunks whose plaintext is alike end up
// adjacent: a bottom-k shingle sketch of each body, minhashed into a signature,
// banded into buckets, and single-linkage over the buckets two chunks share.
//
// It is the relation a compressed pack would pay for, and only a compressed pack:
// on the store as it stands this order changes nothing but the layout.
type ByContent struct {
	K     int // shingle length in bytes
	Keep  int // shingles kept per body, the bottom-k sketch
	Rows  int // signature length
	Bands int // buckets the signature is cut into
}

func (p ByContent) Name() string { return "content" }

func (p ByContent) defaults() ByContent {
	if p.K <= 0 {
		p.K = 16
	}
	if p.Keep <= 0 {
		p.Keep = 256
	}
	if p.Rows <= 0 {
		p.Rows = 32
	}
	if p.Bands <= 0 {
		p.Bands = 8
	}
	if p.Rows%p.Bands != 0 {
		p.Rows -= p.Rows % p.Bands
		if p.Rows == 0 {
			p.Rows, p.Bands = 32, 8
		}
	}
	return p
}

func (p ByContent) Order(w *Workload) []int {
	p = p.defaults()
	sig := make([][]uint64, len(w.Chunks))
	for i := range w.Chunks {
		sig[i] = p.signature(w.Chunks[i].Body)
	}
	uf := newUnion(len(w.Chunks))
	per := p.Rows / p.Bands
	seen := make(map[uint64]int, len(w.Chunks)*p.Bands)
	for i := range sig {
		for b := 0; b < p.Bands; b++ {
			h := bucketKey(sig[i][b*per : (b+1)*per])
			if prev, ok := seen[h]; ok {
				uf.join(i, prev)
			} else {
				seen[h] = i
			}
		}
	}
	root := make([]int, len(w.Chunks))
	for i := range root {
		root[i] = uf.root(i)
	}
	order := make([]int, len(w.Chunks))
	for i := range order {
		order[i] = i
	}
	// A pack may not mix tenants, so the order keeps each tenant contiguous: a
	// planner that interleaves them measures the packer's barrier, not its own
	// relation, and every switch would spend a partially filled pack.
	sort.SliceStable(order, func(a, b int) bool {
		ca, cb := w.Chunks[order[a]], w.Chunks[order[b]]
		if ca.Tenant != cb.Tenant {
			return ca.Tenant < cb.Tenant
		}
		ra, rb := root[order[a]], root[order[b]]
		if ra != rb {
			return ra < rb
		}
		return order[a] < order[b]
	})
	return order
}

// signature is the minhash of the bottom-k shingle sketch: the global minimum of
// every hash is in the sketch, so the minimum over the sketch is the minimum over
// the body for all but a negligible fraction of permutations.
func (p ByContent) signature(body []byte) []uint64 {
	sketch := bottomK(shingles(body, p.K), p.Keep)
	out := make([]uint64, p.Rows)
	for r := range out {
		m := ^uint64(0)
		for _, x := range sketch {
			if y := mix(x ^ seedAt(r)); y < m {
				m = y
			}
		}
		out[r] = m
	}
	return out
}

// shingles is the rolling hash of every K-byte window of body. A body shorter
// than K is one shingle, so two short bodies are similar only if they are equal.
func shingles(body []byte, k int) []uint64 {
	if len(body) <= k {
		return []uint64{fnv64(body)}
	}
	out := make([]uint64, 0, len(body)-k+1)
	var h uint64
	for i := 0; i < k; i++ {
		h = (h ^ uint64(body[i])) * fnvPrime
	}
	out = append(out, h)
	pow := uint64(1)
	for i := 0; i < k-1; i++ {
		pow *= fnvPrime
	}
	for i := k; i < len(body); i++ {
		h -= uint64(body[i-k]) * pow
		h = (h ^ uint64(body[i])) * fnvPrime
		out = append(out, h)
	}
	return out
}

func bottomK(xs []uint64, k int) []uint64 {
	if len(xs) <= k {
		out := slices.Clone(xs)
		slices.Sort(out)
		return out
	}
	work := slices.Clone(xs)
	slices.Sort(work)
	return work[:k]
}

func bucketKey(rows []uint64) uint64 {
	h := uint64(fnvOffset)
	for _, r := range rows {
		h = (h ^ mix(r)) * fnvPrime
	}
	return h
}

const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

func fnv64(b []byte) uint64 {
	h := uint64(fnvOffset)
	for _, c := range b {
		h = (h ^ uint64(c)) * fnvPrime
	}
	return h
}

// seedAt is the fixed permutation seed of row r: the same run must produce the
// same signature on every machine, so no seed comes from a random source.
func seedAt(r int) uint64 { return mix(uint64(r)*0x9e3779b97f4a7c15 + 0x2545f4914f6cdd1d) }

// mix is the splitmix64 finalizer.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

type union struct{ parent []int }

func newUnion(n int) *union {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return &union{parent: p}
}

func (u *union) root(i int) int {
	for u.parent[i] != i {
		u.parent[i] = u.parent[u.parent[i]]
		i = u.parent[i]
	}
	return i
}

func (u *union) join(a, b int) {
	ra, rb := u.root(a), u.root(b)
	if ra == rb {
		return
	}
	if ra < rb {
		u.parent[rb] = ra
	} else {
		u.parent[ra] = rb
	}
}
