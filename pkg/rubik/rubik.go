// Package rubik plans the layout of an immutable base image's chunks into packs
// before they are sealed.
//
// A pack is "sealed chunks laid end to end" (pack.go:16) and the chunk table
// remembers where each one lies, so the order the chunks are laid out in is free
// to be chosen. Two readings of "similar" compete for that choice, and they are
// not the same relation:
//
//   - content similarity: chunks whose plaintext is alike compress together, if
//     the pack is compressed before it is sealed;
//   - co-access similarity: chunks the same image reads land in the same pack,
//     so a cold start fetches fewer objects and fewer bytes.
//
// Nothing in vfs compresses a pack today — a sealed frame is AEAD ciphertext and
// does not compress — so the first relation only exists in a counterfactual the
// store cannot currently reach. Plan.Sealed measures that counterfactual and
// Plan.Stored measures what the store holds now; keeping them apart is the point
// of having two numbers.
package rubik

import (
	"bytes"
	"compress/flate"
	"fmt"
	"sort"
	"sync"
)

// FrameOverhead is the per-frame cost the layout model charges for a sealed
// chunk: the AEAD tag (ledger.go adds aead.Overhead() to every frame). The record
// that locates the frame lives in the chunk table, not in the pack, so it is not
// charged here.
const FrameOverhead = 16

// DefaultPackBytes matches the packTarget of vfs: 16 MiB, one S3 PUT.
const DefaultPackBytes = 16 << 20

// A Chunk is one immutable unit. ID is the address a chunk is stored under (its
// plaintext hash), Tenant is the tenant that owns the sealed object, and Body is
// the plaintext, kept only so the study can weigh and compress it.
type Chunk struct {
	ID     [32]byte
	Tenant string
	Body   []byte
}

// An Image is a base image as the set of chunks a boot touches.
type Image struct {
	Name   string
	Tenant string
	Chunks []int // indices into Workload.Chunks, without repeats
}

// A Workload is the chunk population of one store and the images that read it.
type Workload struct {
	Chunks []Chunk
	Images []Image

	once   sync.Once
	invert [][]int // chunk index -> image indices
}

// NewWorkload indexes a chunk population.
func NewWorkload(chunks []Chunk, images []Image) *Workload {
	return &Workload{Chunks: chunks, Images: images}
}

// Inverted returns, for each chunk, the images that read it. A Workload must not
// be copied after this has been called.
func (w *Workload) Inverted() [][]int {
	w.once.Do(func() {
		if len(w.Chunks) > 0 && len(w.Images) > 0 {
			w.valid()
		}
		w.invert = make([][]int, len(w.Chunks))
		for i, im := range w.Images {
			for _, c := range im.Chunks {
				w.invert[c] = append(w.invert[c], i)
			}
		}
	})
	return w.invert
}

func (w *Workload) valid() {
	for _, im := range w.Images {
		for _, c := range im.Chunks {
			if c < 0 || c >= len(w.Chunks) {
				panic(fmt.Sprintf("rubik: image %q names chunk %d of %d", im.Name, c, len(w.Chunks)))
			}
		}
	}
}

// A Pack is the set of chunks laid out together, and the tenant they belong to.
type Pack struct {
	Tenant string
	Chunks []int
}

// A Plan is a whole layout: every chunk of the workload exactly once.
type Plan struct {
	Packs  []Pack
	packOf []int // chunk index -> pack index, built on first use
}

// A Packer turns an order into a plan. It alone decides a pack's boundary, so two
// orders are compared on equal terms: the same size limit and the same tenant
// barrier.
type Packer struct {
	// Bytes is the size at which a pack is closed. A chunk larger than this goes
	// in a pack of its own rather than being refused: vfs keeps every chunk below
	// packTarget, so the case is a guard, not a regime.
	Bytes int
}

// Plan lays the chunks out in the order given.
func (p Packer) Plan(w *Workload, order []int) Plan {
	limit := p.Bytes
	if limit <= 0 {
		limit = DefaultPackBytes
	}
	if len(order) != len(w.Chunks) {
		panic(fmt.Sprintf("rubik: order has %d chunks, workload has %d", len(order), len(w.Chunks)))
	}
	seen := make([]bool, len(w.Chunks))
	out := Plan{}
	var cur Pack
	var n int
	flush := func() {
		if len(cur.Chunks) > 0 {
			out.Packs = append(out.Packs, cur)
			cur = Pack{}
			n = 0
		}
	}
	for _, c := range order {
		if c < 0 || c >= len(w.Chunks) {
			panic(fmt.Sprintf("rubik: order names chunk %d of %d", c, len(w.Chunks)))
		}
		if seen[c] {
			panic(fmt.Sprintf("rubik: order repeats chunk %d", c))
		}
		seen[c] = true
		ch := w.Chunks[c]
		if len(cur.Chunks) > 0 && (cur.Tenant != ch.Tenant || n+frameBytes(ch) > limit) {
			flush()
		}
		cur.Tenant = ch.Tenant
		cur.Chunks = append(cur.Chunks, c)
		n += frameBytes(ch)
	}
	flush()
	return out
}

func frameBytes(c Chunk) int { return len(c.Body) + FrameOverhead }

// Count is the number of packs the plan lays out.
func (p Plan) Count() int { return len(p.Packs) }

// Stored is what a store holds after the plan is sealed as vfs writes it today:
// every frame once, uncompressed.
func (p Plan) Stored(w *Workload) int {
	n := 0
	for _, pk := range p.Packs {
		for _, c := range pk.Chunks {
			n += frameBytes(w.Chunks[c])
		}
	}
	return n
}

// Sealed is what the store would hold if each pack were compressed before it was
// sealed: the compressed plaintext of the pack plus the tag of every frame. It is
// a counterfactual — vfs seals frames, it does not compress packs — and is
// labelled as one everywhere it is reported.
func (p Plan) Sealed(w *Workload) int {
	n := 0
	var buf bytes.Buffer
	for _, pk := range p.Packs {
		buf.Reset()
		for _, c := range pk.Chunks {
			buf.Write(w.Chunks[c].Body)
		}
		n += deflateLen(buf.Bytes()) + FrameOverhead*len(pk.Chunks)
	}
	return n
}

func deflateLen(b []byte) int {
	var out bytes.Buffer
	zw, err := flate.NewWriter(&out, flate.DefaultCompression)
	if err != nil {
		panic("rubik: flate: " + err.Error())
	}
	if _, err := zw.Write(b); err != nil {
		panic("rubik: flate: " + err.Error())
	}
	if err := zw.Close(); err != nil {
		panic("rubik: flate: " + err.Error())
	}
	return out.Len()
}

func (p *Plan) index(n int) {
	if p.packOf != nil {
		return
	}
	p.packOf = make([]int, n)
	for i := range p.packOf {
		p.packOf[i] = -1
	}
	for i, pk := range p.Packs {
		for _, c := range pk.Chunks {
			p.packOf[c] = i
		}
	}
}

// ColdStart returns the bytes a boot of image fetches and the bytes it reads. A
// pack is the unit of a fetch, so every pack holding one of the image's chunks is
// a whole GET.
func (p *Plan) ColdStart(w *Workload, image int) (fetched, needed int) {
	p.index(len(w.Chunks))
	im := w.Images[image]
	hit := make(map[int]bool, len(im.Chunks))
	for _, c := range im.Chunks {
		needed += len(w.Chunks[c].Body)
		if pk := p.packOf[c]; pk >= 0 {
			hit[pk] = true
		}
	}
	for pk := range hit {
		for _, c := range p.Packs[pk].Chunks {
			fetched += frameBytes(w.Chunks[c])
		}
	}
	return fetched, needed
}

// Isolation is the number of packs that mix tenants. Strict tenant isolation
// admits none, so a non-zero count is a defect of the plan, not a tuning knob.
func (p Plan) Isolation(w *Workload) int {
	bad := 0
	for _, pk := range p.Packs {
		for _, c := range pk.Chunks {
			if w.Chunks[c].Tenant != pk.Tenant {
				bad++
				break
			}
		}
	}
	return bad
}

// A Planner is an order over a workload's chunks — the only degree of freedom the
// packer is given.
type Planner interface {
	Name() string
	Order(w *Workload) []int
}

// Arrival is the order vfs writes in: the order the chunks were sealed, which for
// a layered base image is layer order.
type Arrival struct{}

func (Arrival) Name() string { return "arrival" }

func (Arrival) Order(w *Workload) []int {
	out := make([]int, len(w.Chunks))
	for i := range out {
		out[i] = i
	}
	return out
}

// BySize orders chunks by ascending plaintext length. It is the null hypothesis
// of packing: that only the fit matters, not the content.
type BySize struct{}

func (BySize) Name() string { return "size" }

func (BySize) Order(w *Workload) []int {
	out := make([]int, len(w.Chunks))
	for i := range out {
		out[i] = i
	}
	// Tenant first, like every other order: a pack may not mix tenants, so an
	// order that interleaves them is not a packing, it is a run of one-chunk packs.
	sort.SliceStable(out, func(a, b int) bool {
		ca, cb := w.Chunks[out[a]], w.Chunks[out[b]]
		if ca.Tenant != cb.Tenant {
			return ca.Tenant < cb.Tenant
		}
		if len(ca.Body) != len(cb.Body) {
			return len(ca.Body) < len(cb.Body)
		}
		return out[a] < out[b]
	})
	return out
}

// ColdStartStats is the distribution of a plan's cold starts, in bytes.
type ColdStartStats struct {
	Images        int
	MedianFetched int
	P90Fetched    int
	MedianNeeded  int
	MedianAmp     float64
	SumFetched    int
	SumNeeded     int
	SumAmp        float64 // every split shows here; the median hides small ones
}

// ColdStarts summarises the cold starts of the images of tenant. An image of
// another tenant is not measured: a plan that made one tenant's boot cheap by
// reading another tenant's packs would be a defect, not a result.
func (p *Plan) ColdStarts(w *Workload, tenant string) ColdStartStats {
	type pair struct{ fetched, needed int }
	var pairs []pair
	for i, im := range w.Images {
		if tenant != "" && im.Tenant != tenant {
			continue
		}
		f, n := p.ColdStart(w, i)
		pairs = append(pairs, pair{f, n})
	}
	if len(pairs) == 0 {
		return ColdStartStats{}
	}
	fetched := make([]int, len(pairs))
	needed := make([]int, len(pairs))
	amps := make([]float64, len(pairs))
	for i, pr := range pairs {
		fetched[i], needed[i] = pr.fetched, pr.needed
		if pr.needed > 0 {
			amps[i] = float64(pr.fetched) / float64(pr.needed)
		}
	}
	sort.Ints(fetched)
	sort.Ints(needed)
	sort.Float64s(amps)
	var sumF, sumN int
	for _, pr := range pairs {
		sumF += pr.fetched
		sumN += pr.needed
	}
	out := ColdStartStats{
		Images:        len(pairs),
		MedianFetched: fetched[len(fetched)/2],
		P90Fetched:    fetched[(len(fetched)*9)/10],
		MedianNeeded:  needed[len(needed)/2],
		MedianAmp:     amps[len(amps)/2],
		SumFetched:    sumF,
		SumNeeded:     sumN,
	}
	if sumN > 0 {
		out.SumAmp = float64(sumF) / float64(sumN)
	}
	return out
}
