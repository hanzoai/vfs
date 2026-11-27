package modeldist

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

// ProviderStats records atomic operational counters for a chunk provider.
type ProviderStats struct {
	GetRequests      int64 `json:"get_requests"`
	ManifestRequests int64 `json:"manifest_requests"`
	BytesServed      int64 `json:"bytes_served"`
	Errors           int64 `json:"errors"`
	NotFound         int64 `json:"not_found"`
}

// ChunkProvider serves content-addressed chunks and manifests.
// Implementations must be safe for concurrent use.
type ChunkProvider interface {
	ID() string
	IsOrigin() bool
	HasChunk(ctx context.Context, blake3Hex string) (bool, error)
	GetChunk(ctx context.Context, blake3Hex string, maxSize int64) ([]byte, error)
	GetManifest(ctx context.Context, cidHex string) ([]byte, error)
	Stats() ProviderStats
}

// WritableProvider extends ChunkProvider with content-addressed writes.
type WritableProvider interface {
	ChunkProvider
	PutChunk(ctx context.Context, blake3Hex string, data []byte) error
	PutManifest(ctx context.Context, cidHex string, raw []byte) error
}

// VFSStoreProvider adapts any hanzoai/vfs pkg/backend.Backend (file://, s3://,
// or storetest.Mem) into a WritableProvider with zero second disk format.
type VFSStoreProvider struct {
	id      string
	origin  bool
	backend backend.Backend

	offline atomic.Bool

	getReqs      atomic.Int64
	manifestReqs atomic.Int64
	bytesServed  atomic.Int64
	errCount     atomic.Int64
	notFound     atomic.Int64

	mu      sync.RWMutex
	tampers map[string]func([]byte) []byte
}

// NewVFSStoreProvider wraps b as a content-addressed model chunk/manifest provider.
func NewVFSStoreProvider(id string, isOrigin bool, b backend.Backend) *VFSStoreProvider {
	return &VFSStoreProvider{
		id:      id,
		origin:  isOrigin,
		backend: b,
		tampers: make(map[string]func([]byte) []byte),
	}
}

func (p *VFSStoreProvider) ID() string     { return p.id }
func (p *VFSStoreProvider) IsOrigin() bool { return p.origin }

// Backend exposes the underlying hanzoai/vfs backend.Backend.
func (p *VFSStoreProvider) Backend() backend.Backend { return p.backend }

// Disable simulates removing or shutting down this provider (e.g., origin removal).
func (p *VFSStoreProvider) Disable() { p.offline.Store(true) }

// Enable restores an offline provider.
func (p *VFSStoreProvider) Enable() { p.offline.Store(false) }

// IsOffline reports whether Disable has been called.
func (p *VFSStoreProvider) IsOffline() bool { return p.offline.Load() }

// InjectTamper registers a deterministic mutation callback invoked when serving
// blake3Hex (use "*" to tamper with every served chunk). Used in adversarial tests.
func (p *VFSStoreProvider) InjectTamper(blake3Hex string, fn func([]byte) []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fn == nil {
		delete(p.tampers, strings.ToLower(blake3Hex))
		return
	}
	p.tampers[strings.ToLower(blake3Hex)] = fn
}

func (p *VFSStoreProvider) applyTamper(blake3Hex string, data []byte) []byte {
	p.mu.RLock()
	fn := p.tampers[strings.ToLower(blake3Hex)]
	if fn == nil {
		fn = p.tampers["*"]
	}
	p.mu.RUnlock()
	if fn == nil {
		return data
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return fn(cp)
}

// PutChunk stores data under model/chunk/<2hex>/<blake3Hex> after checking its BLAKE3 digest.
func (p *VFSStoreProvider) PutChunk(ctx context.Context, blake3Hex string, data []byte) error {
	if p.offline.Load() {
		return fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	key, err := ChunkKey(blake3Hex)
	if err != nil {
		return err
	}
	sum := blake3.Sum(data)
	got := hex.EncodeToString(sum[:])
	if got != strings.ToLower(blake3Hex) {
		return fmt.Errorf("%w: put chunk %s != computed %s", ErrChunkDigestMismatch, blake3Hex, got)
	}
	return p.backend.Put(ctx, key, data)
}

// PutManifest stores raw manifest JSON under model/manifest/<2hex>/<cidHex>.
func (p *VFSStoreProvider) PutManifest(ctx context.Context, cidHex string, raw []byte) error {
	if p.offline.Load() {
		return fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	key, err := ManifestKey(cidHex)
	if err != nil {
		return err
	}
	return p.backend.Put(ctx, key, raw)
}

// PutRef stores a logical model/version pointer containing manifestCID.
func (p *VFSStoreProvider) PutRef(ctx context.Context, modelID, version, cidHex string) error {
	if p.offline.Load() {
		return fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	if !isHexDigest64(strings.ToLower(cidHex)) {
		return fmt.Errorf("%w: invalid cid %q", ErrInvalidManifest, cidHex)
	}
	key, err := RefKey(modelID, version)
	if err != nil {
		return err
	}
	return p.backend.Put(ctx, key, []byte(strings.ToLower(cidHex)))
}

// HasChunk checks chunk existence via backend.Stat without transferring the payload.
func (p *VFSStoreProvider) HasChunk(ctx context.Context, blake3Hex string) (bool, error) {
	if p.offline.Load() {
		p.errCount.Add(1)
		return false, fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	key, err := ChunkKey(blake3Hex)
	if err != nil {
		return false, err
	}
	_, err = p.backend.Stat(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return false, nil
		}
		p.errCount.Add(1)
		return false, err
	}
	return true, nil
}

// GetChunk fetches chunk bytes from the underlying VFS backend.
func (p *VFSStoreProvider) GetChunk(ctx context.Context, blake3Hex string, maxSize int64) ([]byte, error) {
	p.getReqs.Add(1)
	if p.offline.Load() {
		p.errCount.Add(1)
		return nil, fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	key, err := ChunkKey(blake3Hex)
	if err != nil {
		p.errCount.Add(1)
		return nil, err
	}
	if maxSize <= 0 {
		maxSize = backend.Unbounded
	}
	data, _, err := p.backend.Get(ctx, key, maxSize)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			p.notFound.Add(1)
			return nil, fmt.Errorf("%w: %s on %s", ErrChunkNotFound, blake3Hex, p.id)
		}
		p.errCount.Add(1)
		return nil, err
	}
	out := p.applyTamper(blake3Hex, data)
	p.bytesServed.Add(int64(len(out)))
	return out, nil
}

// GetManifest fetches a serialized manifest by CID from the underlying VFS backend.
func (p *VFSStoreProvider) GetManifest(ctx context.Context, cidHex string) ([]byte, error) {
	p.manifestReqs.Add(1)
	if p.offline.Load() {
		p.errCount.Add(1)
		return nil, fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	key, err := ManifestKey(cidHex)
	if err != nil {
		p.errCount.Add(1)
		return nil, err
	}
	data, _, err := p.backend.Get(ctx, key, MaxManifestBytes)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			p.notFound.Add(1)
			return nil, fmt.Errorf("%w: %s on %s", ErrManifestNotFound, cidHex, p.id)
		}
		p.errCount.Add(1)
		return nil, err
	}
	p.bytesServed.Add(int64(len(data)))
	return data, nil
}

// Stats returns a snapshot of atomic request/byte/error counters.
func (p *VFSStoreProvider) Stats() ProviderStats {
	return ProviderStats{
		GetRequests:      p.getReqs.Load(),
		ManifestRequests: p.manifestReqs.Load(),
		BytesServed:      p.bytesServed.Load(),
		Errors:           p.errCount.Load(),
		NotFound:         p.notFound.Load(),
	}
}

// HTTPMirrorServer exposes any ChunkProvider over plain content-addressed HTTP
// ("GET/HEAD /v1/chunks/<blake3>" and "GET /v1/manifests/<cid>") with zero
// vendor-specific protocol dependencies.
type HTTPMirrorServer struct {
	id      string
	origin  bool
	backing ChunkProvider
	ln      net.Listener
	srv     *http.Server
	baseURL string

	closed atomic.Bool

	mu      sync.RWMutex
	tampers map[string]func([]byte) []byte
	delay   time.Duration

	getReqs      atomic.Int64
	manifestReqs atomic.Int64
	bytesServed  atomic.Int64
	errCount     atomic.Int64
}

// StartHTTPMirror launches an independent loopback HTTP mirror backed by backing.
func StartHTTPMirror(id string, isOrigin bool, backing ChunkProvider) (*HTTPMirrorServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("modeldist: listen mirror %s: %w", id, err)
	}
	m := &HTTPMirrorServer{
		id:      id,
		origin:  isOrigin,
		backing: backing,
		ln:      ln,
		baseURL: "http://" + ln.Addr().String(),
		tampers: make(map[string]func([]byte) []byte),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chunks/", m.handleChunk)
	mux.HandleFunc("/v1/manifests/", m.handleManifest)
	m.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		_ = m.srv.Serve(ln)
	}()
	return m, nil
}

// BaseURL returns the mirror's HTTP base URL.
func (m *HTTPMirrorServer) BaseURL() string { return m.baseURL }

// ID returns the mirror identifier.
func (m *HTTPMirrorServer) ID() string { return m.id }

// InjectTamper mutates responses for blake3Hex ("*" mutates every chunk).
func (m *HTTPMirrorServer) InjectTamper(blake3Hex string, fn func([]byte) []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fn == nil {
		delete(m.tampers, strings.ToLower(blake3Hex))
		return
	}
	m.tampers[strings.ToLower(blake3Hex)] = fn
}

// SetDelay injects artificial network latency per request (for tail-latency testing).
func (m *HTTPMirrorServer) SetDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay = d
}

// Close immediately closes the listener and shuts down the HTTP mirror server.
func (m *HTTPMirrorServer) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = m.srv.Shutdown(ctx)
	return m.srv.Close()
}

// ServerStats returns server-side atomic request counters.
func (m *HTTPMirrorServer) ServerStats() ProviderStats {
	return ProviderStats{
		GetRequests:      m.getReqs.Load(),
		ManifestRequests: m.manifestReqs.Load(),
		BytesServed:      m.bytesServed.Load(),
		Errors:           m.errCount.Load(),
	}
}

// Client returns an HTTPMirrorProvider configured to pull from this server.
func (m *HTTPMirrorServer) Client() *HTTPMirrorProvider {
	return NewHTTPMirrorProvider(m.id, m.origin, m.baseURL, nil)
}

func (m *HTTPMirrorServer) handleChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	hash := strings.TrimPrefix(r.URL.Path, "/v1/chunks/")
	hash = strings.ToLower(strings.TrimSpace(hash))
	if !isHexDigest64(hash) {
		http.Error(w, "invalid chunk digest", http.StatusBadRequest)
		return
	}

	m.mu.RLock()
	delay := m.delay
	tamper := m.tampers[hash]
	if tamper == nil {
		tamper = m.tampers["*"]
	}
	m.mu.RUnlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}

	if r.Method == http.MethodHead {
		ok, err := m.backing.HasChunk(r.Context(), hash)
		if err != nil {
			m.errCount.Add(1)
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.WriteHeader(http.StatusOK)
		return
	}

	m.getReqs.Add(1)
	data, err := m.backing.GetChunk(r.Context(), hash, backend.Unbounded)
	if err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			http.NotFound(w, r)
			return
		}
		m.errCount.Add(1)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if tamper != nil {
		cp := make([]byte, len(data))
		copy(cp, data)
		data = tamper(cp)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("X-Content-BLAKE3", hash)
	w.WriteHeader(http.StatusOK)
	n, _ := w.Write(data)
	m.bytesServed.Add(int64(n))
}

func (m *HTTPMirrorServer) handleManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cid := strings.TrimPrefix(r.URL.Path, "/v1/manifests/")
	cid = strings.ToLower(strings.TrimSpace(cid))
	if !isHexDigest64(cid) {
		http.Error(w, "invalid manifest cid", http.StatusBadRequest)
		return
	}
	m.manifestReqs.Add(1)
	raw, err := m.backing.GetManifest(r.Context(), cid)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			http.NotFound(w, r)
			return
		}
		m.errCount.Add(1)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.WriteHeader(http.StatusOK)
	n, _ := w.Write(raw)
	m.bytesServed.Add(int64(n))
}

// HTTPMirrorProvider fetches content-addressed chunks and manifests over plain HTTP.
type HTTPMirrorProvider struct {
	id      string
	origin  bool
	baseURL string
	client  *http.Client

	getReqs      atomic.Int64
	manifestReqs atomic.Int64
	bytesServed  atomic.Int64
	errCount     atomic.Int64
	notFound     atomic.Int64
}

// NewHTTPMirrorProvider creates a ChunkProvider targeting baseURL.
func NewHTTPMirrorProvider(id string, isOrigin bool, baseURL string, hc *http.Client) *HTTPMirrorProvider {
	if hc == nil {
		tr := &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     30 * time.Second,
		}
		hc = &http.Client{Transport: tr, Timeout: 30 * time.Second}
	}
	return &HTTPMirrorProvider{
		id:      id,
		origin:  isOrigin,
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  hc,
	}
}

func (p *HTTPMirrorProvider) ID() string     { return p.id }
func (p *HTTPMirrorProvider) IsOrigin() bool { return p.origin }

// HasChunk sends an HTTP HEAD request for blake3Hex.
func (p *HTTPMirrorProvider) HasChunk(ctx context.Context, blake3Hex string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, p.baseURL+"/v1/chunks/"+strings.ToLower(blake3Hex), nil)
	if err != nil {
		return false, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		p.errCount.Add(1)
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		p.errCount.Add(1)
		return false, fmt.Errorf("modeldist: mirror %s HEAD status %d", p.id, resp.StatusCode)
	}
	return true, nil
}

// GetChunk downloads up to maxSize bytes for blake3Hex over HTTP.
func (p *HTTPMirrorProvider) GetChunk(ctx context.Context, blake3Hex string, maxSize int64) ([]byte, error) {
	p.getReqs.Add(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/v1/chunks/"+strings.ToLower(blake3Hex), nil)
	if err != nil {
		p.errCount.Add(1)
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		p.errCount.Add(1)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		p.notFound.Add(1)
		return nil, fmt.Errorf("%w: %s on %s", ErrChunkNotFound, blake3Hex, p.id)
	}
	if resp.StatusCode != http.StatusOK {
		p.errCount.Add(1)
		return nil, fmt.Errorf("modeldist: mirror %s GET chunk status %d", p.id, resp.StatusCode)
	}
	var r io.Reader = resp.Body
	if maxSize > 0 && maxSize < backend.Unbounded {
		r = io.LimitReader(resp.Body, backend.Past(maxSize))
	}
	data, err := io.ReadAll(r)
	if err != nil {
		p.errCount.Add(1)
		return nil, err
	}
	if maxSize > 0 && maxSize < backend.Unbounded && int64(len(data)) > maxSize {
		p.errCount.Add(1)
		return nil, fmt.Errorf("%w: chunk exceeds max %d", ErrChunkSizeMismatch, maxSize)
	}
	p.bytesServed.Add(int64(len(data)))
	return data, nil
}

// GetManifest downloads a serialized ManifestV1 by CID over HTTP.
func (p *HTTPMirrorProvider) GetManifest(ctx context.Context, cidHex string) ([]byte, error) {
	p.manifestReqs.Add(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/v1/manifests/"+strings.ToLower(cidHex), nil)
	if err != nil {
		p.errCount.Add(1)
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		p.errCount.Add(1)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		p.notFound.Add(1)
		return nil, fmt.Errorf("%w: %s on %s", ErrManifestNotFound, cidHex, p.id)
	}
	if resp.StatusCode != http.StatusOK {
		p.errCount.Add(1)
		return nil, fmt.Errorf("modeldist: mirror %s GET manifest status %d", p.id, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxManifestBytes+1))
	if err != nil {
		p.errCount.Add(1)
		return nil, err
	}
	if int64(len(data)) > MaxManifestBytes {
		p.errCount.Add(1)
		return nil, fmt.Errorf("%w: manifest exceeds max %d", ErrInvalidManifest, MaxManifestBytes)
	}
	p.bytesServed.Add(int64(len(data)))
	return data, nil
}

// Stats returns client-side atomic provider statistics.
func (p *HTTPMirrorProvider) Stats() ProviderStats {
	return ProviderStats{
		GetRequests:      p.getReqs.Load(),
		ManifestRequests: p.manifestReqs.Load(),
		BytesServed:      p.bytesServed.Load(),
		Errors:           p.errCount.Load(),
		NotFound:         p.notFound.Load(),
	}
}

// DeterministicChunkCatalog generates deterministic multi-GiB model chunks on
// demand in O(chunkSize) memory while computing real per-chunk and full-stream
// BLAKE3 and SHA-256 digests. This allows real multi-GiB HTTP/mirror transfer
// and cryptographic verification without requiring multi-GiB RAM in CI.
type DeterministicChunkCatalog struct {
	seed      uint64
	chunkSize int64
	totalSize int64
	byHash    map[string]int
	chunks    []ChunkRef
	manifest  []byte
	cid       string
}

// BuildDeterministicMultiGiBArtifact constructs an Artifact of totalSize bytes
// split into chunkSize pieces, streaming every byte through BLAKE3 and SHA-256
// to produce exact chunk and root digests, and returns a catalog that can back
// multiple independent HTTP mirrors with bounded memory.
func BuildDeterministicMultiGiBArtifact(path string, kind AssetKind, variant string, seed uint64, totalSize, chunkSize int64) (Artifact, *DeterministicChunkCatalog, error) {
	if totalSize <= 0 || chunkSize <= 0 {
		return Artifact{}, nil, fmt.Errorf("%w: totalSize and chunkSize must be > 0", ErrInvalidManifest)
	}
	nChunks := int((totalSize + chunkSize - 1) / chunkSize)
	chunks := make([]ChunkRef, 0, nChunks)
	byHash := make(map[string]int, nChunks)

	rootB3 := blake3.New()
	rootS2 := sha256.New()
	buf := make([]byte, chunkSize)

	var off int64
	for idx := 0; idx < nChunks; idx++ {
		sz := chunkSize
		if rem := totalSize - off; rem < sz {
			sz = rem
		}
		chunk := buf[:sz]
		fillDeterministicChunk(chunk, seed, idx)

		b3, s2 := DigestChunk(chunk)
		_, _ = rootB3.Write(chunk)
		_, _ = rootS2.Write(chunk)

		chunks = append(chunks, ChunkRef{
			Index:  idx,
			Offset: off,
			Size:   sz,
			BLAKE3: b3,
			SHA256: s2,
		})
		byHash[b3] = idx
		off += sz
	}

	art := Artifact{
		Path:       path,
		Kind:       kind,
		Variant:    variant,
		Size:       totalSize,
		ChunkSize:  chunkSize,
		RootBLAKE3: hex.EncodeToString(rootB3.Sum(nil)),
		RootSHA256: hex.EncodeToString(rootS2.Sum(nil)),
		Chunks:     chunks,
	}
	cat := &DeterministicChunkCatalog{
		seed:      seed,
		chunkSize: chunkSize,
		totalSize: totalSize,
		byHash:    byHash,
		chunks:    chunks,
	}
	return art, cat, nil
}

// AttachManifest binds the signed manifest bytes to the catalog so mirrors can serve it.
func (c *DeterministicChunkCatalog) AttachManifest(cid string, raw []byte) {
	c.cid = strings.ToLower(cid)
	c.manifest = append([]byte(nil), raw...)
}

// fillDeterministicChunk fills dst with fast deterministic pseudo-random bytes
// keyed by (seed, chunkIndex) via internal/blake3's SIMD XOF (Expand). Every
// chunk has cryptographically unique content (zero accidental dedup collapse)
// and generates at multi-GB/s even under the Go race detector.
func fillDeterministicChunk(dst []byte, seed uint64, chunkIndex int) {
	var key [16]byte
	binary.LittleEndian.PutUint64(key[0:8], seed)
	binary.LittleEndian.PutUint64(key[8:16], uint64(chunkIndex))
	h := blake3.New()
	_, _ = h.Write(key[:])
	h.Expand(dst)
}

// CatalogBackingProvider serves chunks from a DeterministicChunkCatalog on demand
// with O(chunkSize) memory per concurrent request.
type CatalogBackingProvider struct {
	id      string
	origin  bool
	cat     *DeterministicChunkCatalog
	offline atomic.Bool

	getReqs      atomic.Int64
	manifestReqs atomic.Int64
	bytesServed  atomic.Int64
	errCount     atomic.Int64
	notFound     atomic.Int64
}

// NewCatalogBackingProvider creates an independent provider over cat.
func NewCatalogBackingProvider(id string, isOrigin bool, cat *DeterministicChunkCatalog) *CatalogBackingProvider {
	return &CatalogBackingProvider{
		id:     id,
		origin: isOrigin,
		cat:    cat,
	}
}

func (p *CatalogBackingProvider) ID() string     { return p.id }
func (p *CatalogBackingProvider) IsOrigin() bool { return p.origin }
func (p *CatalogBackingProvider) Disable()       { p.offline.Store(true) }

func (p *CatalogBackingProvider) HasChunk(_ context.Context, blake3Hex string) (bool, error) {
	if p.offline.Load() {
		p.errCount.Add(1)
		return false, fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	_, ok := p.cat.byHash[strings.ToLower(blake3Hex)]
	return ok, nil
}

func (p *CatalogBackingProvider) GetChunk(_ context.Context, blake3Hex string, maxSize int64) ([]byte, error) {
	p.getReqs.Add(1)
	if p.offline.Load() {
		p.errCount.Add(1)
		return nil, fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	idx, ok := p.cat.byHash[strings.ToLower(blake3Hex)]
	if !ok {
		p.notFound.Add(1)
		return nil, fmt.Errorf("%w: %s on %s", ErrChunkNotFound, blake3Hex, p.id)
	}
	ref := p.cat.chunks[idx]
	if maxSize > 0 && ref.Size > maxSize {
		p.errCount.Add(1)
		return nil, fmt.Errorf("%w: chunk size %d > max %d", ErrChunkSizeMismatch, ref.Size, maxSize)
	}
	buf := make([]byte, ref.Size)
	fillDeterministicChunk(buf, p.cat.seed, idx)
	p.bytesServed.Add(ref.Size)
	return buf, nil
}

func (p *CatalogBackingProvider) GetManifest(_ context.Context, cidHex string) ([]byte, error) {
	p.manifestReqs.Add(1)
	if p.offline.Load() {
		p.errCount.Add(1)
		return nil, fmt.Errorf("%w: %s", ErrProviderOffline, p.id)
	}
	if strings.ToLower(cidHex) != p.cat.cid || len(p.cat.manifest) == 0 {
		p.notFound.Add(1)
		return nil, fmt.Errorf("%w: %s on %s", ErrManifestNotFound, cidHex, p.id)
	}
	cp := append([]byte(nil), p.cat.manifest...)
	p.bytesServed.Add(int64(len(cp)))
	return cp, nil
}

func (p *CatalogBackingProvider) Stats() ProviderStats {
	return ProviderStats{
		GetRequests:      p.getReqs.Load(),
		ManifestRequests: p.manifestReqs.Load(),
		BytesServed:      p.bytesServed.Load(),
		Errors:           p.errCount.Load(),
		NotFound:         p.notFound.Load(),
	}
}
