package rubik

import "fmt"

// Layered describes a synthetic population of base images: a store of immutable
// lineages, each a stack of the layers below it plus a handful of near-copies of
// layer chunks — the recompiled binary, the patched script — that carry almost the
// same words and hash to a different address.
//
// Images are generated tenant-major and chunks in order of first appearance, so
// chunk order is arrival order: what vfs writes without a planner.
type Layered struct {
	Tenants     int
	Images      int // per tenant
	Layers      int
	LayerChunks int
	Deltas      int // near-copies per image
	ChunkBytes  int
	Scatter     bool // layers reach the store out of depth order
	Seed        uint64
}

func (cfg Layered) defaults() Layered {
	if cfg.Tenants <= 0 {
		cfg.Tenants = 2
	}
	if cfg.Images <= 0 {
		cfg.Images = 12
	}
	if cfg.Layers <= 0 {
		cfg.Layers = 6
	}
	if cfg.LayerChunks <= 0 {
		cfg.LayerChunks = 64
	}
	if cfg.Deltas <= 0 {
		cfg.Deltas = 12
	}
	if cfg.ChunkBytes <= 0 {
		cfg.ChunkBytes = 4096
	}
	return cfg
}

// Workload builds the population. It is a function of the config alone: no clock,
// no map iteration order, no random source.
func (cfg Layered) Workload() *Workload {
	cfg = cfg.defaults()
	w := &Workload{}
	addr := make(map[[32]byte]int)
	add := func(tenant string, body []byte) int {
		id := contentID(body)
		if i, ok := addr[id]; ok {
			return i
		}
		i := len(w.Chunks)
		w.Chunks = append(w.Chunks, Chunk{ID: id, Tenant: tenant, Body: body})
		addr[id] = i
		return i
	}
	for t := 0; t < cfg.Tenants; t++ {
		tenant := fmt.Sprintf("tenant-%02d", t)
		rng := &stream{state: mix(cfg.Seed ^ (uint64(t+1) * 0x9e3779b97f4a7c15))}
		vocab := newVocab(rng)
		bodies := make([][][]byte, cfg.Layers)
		for l := range bodies {
			bodies[l] = make([][]byte, cfg.LayerChunks)
			for c := range bodies[l] {
				bodies[l][c] = vocab.text(rng, cfg.bodyBytes(rng))
			}
		}
		// Layers reach the store in depth order unless the config scatters them —
		// a base image rebuilt long after the images that stack on it. The bodies
		// are the same either way, so the two modes differ only in arrival order.
		emit := make([]int, cfg.Layers)
		for l := range emit {
			emit[l] = l
		}
		if cfg.Scatter {
			shuf := &stream{state: mix(cfg.Seed ^ 0x5ca77e5 ^ uint64(t+1))}
			for i := len(emit) - 1; i > 0; i-- {
				j := shuf.n(i + 1)
				emit[i], emit[j] = emit[j], emit[i]
			}
		}
		layers := make([][]int, cfg.Layers)
		for _, l := range emit {
			for _, b := range bodies[l] {
				layers[l] = append(layers[l], add(tenant, b))
			}
		}
		for i := 0; i < cfg.Images; i++ {
			depth := 1 + i%cfg.Layers
			im := Image{Name: fmt.Sprintf("%s/image-%03d", tenant, i), Tenant: tenant}
			for l := 0; l < depth; l++ {
				im.Chunks = append(im.Chunks, layers[l]...)
			}
			for d := 0; d < cfg.Deltas; d++ {
				src := w.Chunks[layers[i%cfg.Layers][rng.n(cfg.LayerChunks)]]
				im.Chunks = append(im.Chunks, add(tenant, vocab.perturb(rng, src.Body)))
			}
			w.Images = append(w.Images, im)
		}
	}
	return w
}

// bodyBytes is the plaintext length of one chunk: uniform over [ChunkBytes/2,
// 3*ChunkBytes/2), because a base image is not made of equal files and a packer
// that only fits equal pieces would be measuring its own arithmetic.
func (cfg Layered) bodyBytes(s *stream) int {
	return cfg.ChunkBytes/2 + s.n(cfg.ChunkBytes)
}

// TenantNames lists the tenants of a workload, in order of first appearance.
func (w *Workload) TenantNames() []string {
	var out []string
	seen := make(map[string]bool)
	for _, c := range w.Chunks {
		if !seen[c.Tenant] {
			seen[c.Tenant] = true
			out = append(out, c.Tenant)
		}
	}
	return out
}

// stream is splitmix64: a stream with no state but its own, so a workload is
// reproducible from its seed alone.
type stream struct{ state uint64 }

func (s *stream) next() uint64 {
	s.state += 0x9e3779b97f4a7c15
	return mix(s.state)
}

func (s *stream) n(mod int) int {
	if mod <= 0 {
		return 0
	}
	return int(s.next() % uint64(mod))
}

// vocab is a small word pool. Text drawn from it compresses the way source code
// and userland binaries do — by repeating tokens — which is the redundancy a pack
// compressor would find and a sealed frame does not.
type vocab []string

func newVocab(s *stream) vocab {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	v := make(vocab, 192)
	for i := range v {
		n := 3 + int(s.next()%6)
		b := make([]byte, n)
		for k := range b {
			b[k] = letters[s.next()%uint64(len(letters))]
		}
		v[i] = string(b)
	}
	return v
}

func (v vocab) text(s *stream, size int) []byte {
	b := make([]byte, 0, size+8)
	for len(b) < size {
		if len(b) > 0 {
			b = append(b, ' ')
		}
		b = append(b, v[s.n(len(v))]...)
	}
	return b[:size]
}

// perturb returns a body of the same length as src with two words replaced: the
// near-copy a rebuild produces. It shares almost every shingle with src and none
// of its address.
func (v vocab) perturb(s *stream, src []byte) []byte {
	out := make([]byte, len(src))
	copy(out, src)
	for k := 0; k < 2 && len(out) > 8; k++ {
		at := 1 + s.n(len(out)-2)
		end := at
		for end < len(out) && out[end] != ' ' {
			end++
		}
		word := []byte(v[s.n(len(v))])
		for i := 0; i < end-at; i++ {
			out[at+i] = word[i%len(word)]
		}
	}
	return out
}
