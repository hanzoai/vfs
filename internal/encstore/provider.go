// Copyright 2026 Hanzo AI Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encstore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

const (
	// MaxManifestSize bounds serialized SignedManifest size consistently across
	// both PutArtifact (write) and GetVerifiedManifest (read).
	MaxManifestSize int64 = 64 << 20

	// MaxRevocationListSize bounds serialized SignedRevocationList size across
	// both publication and retrieval.
	MaxRevocationListSize int64 = 4 << 20

	// MaxTombstoneSize bounds serialized DeletionTombstone size across both
	// publication and retrieval.
	MaxTombstoneSize int64 = 1 << 20

	// MaxHeadPointerSize bounds serialized HeadPointer size when recovering
	// persisted artifact heads across storage nodes.
	MaxHeadPointerSize int64 = 1 << 20

	// MaxBundleIndexSize bounds serialized AuthorizedBundleIndex size across
	// both export and restore.
	MaxBundleIndexSize int64 = 4 << 20

	// maxHTTPBodySize caps a single HTTPNodeServer PUT request body.
	maxHTTPBodySize int64 = 128 << 20
)

// FaultMode injects deterministic storage-provider faults for adversarial
// integrity and resilience testing.
type FaultMode int

const (
	FaultNone FaultMode = iota
	FaultOffline
	FaultCorruptChunk
	FaultTruncateChunk
	FaultCorruptManifest
)

// NodeStats reports request counters for an independent HTTPNodeServer.
type NodeStats struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	Gets      int64  `json:"gets"`
	Puts      int64  `json:"puts"`
	Deletes   int64  `json:"deletes"`
	Lists     int64  `json:"lists"`
	BytesSent int64  `json:"bytes_sent"`
	BytesRecv int64  `json:"bytes_recv"`
}

// HTTPNodeServer is an independent, zero-Hanzo-account HTTP storage provider
// backed directly by any hanzoai/vfs pkg/backend.Backend (e.g. file://).
// It exposes canonical VFS keys over standard HTTP:
//
//	GET    /v1/obj/<key>
//	HEAD   /v1/obj/<key>
//	PUT    /v1/obj/<key>
//	DELETE /v1/obj/<key>
//	GET    /v1/list?prefix=<prefix>
type HTTPNodeServer struct {
	name     string
	store    backend.Backend
	listener net.Listener
	server   *http.Server
	baseURL  string

	faultMu   sync.RWMutex
	faultMode FaultMode

	gets      atomic.Int64
	puts      atomic.Int64
	deletes   atomic.Int64
	lists     atomic.Int64
	bytesSent atomic.Int64
	bytesRecv atomic.Int64
	closed    atomic.Bool
}

// NewHTTPNodeServer starts an independent HTTP storage provider listening on
// addr (use "127.0.0.1:0" for an ephemeral loopback port) backed by store.
func NewHTTPNodeServer(name, addr string, store backend.Backend) (*HTTPNodeServer, error) {
	if store == nil {
		return nil, errors.New("encstore: nil backend for HTTPNodeServer")
	}
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("encstore: listen %s: %w", addr, err)
	}
	n := &HTTPNodeServer{
		name:     name,
		store:    store,
		listener: ln,
		baseURL:  "http://" + ln.Addr().String(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/obj/", n.handleObj)
	mux.HandleFunc("/v1/list", n.handleList)
	n.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		_ = n.server.Serve(ln)
	}()
	return n, nil
}

// Name returns the operator-assigned node name.
func (n *HTTPNodeServer) Name() string { return n.name }

// URL returns the base HTTP URL of this independent storage node.
func (n *HTTPNodeServer) URL() string { return n.baseURL }

// Backend returns the underlying pkg/backend.Backend for inspection in tests.
func (n *HTTPNodeServer) Backend() backend.Backend { return n.store }

// SetFaultMode dynamically configures fault injection on this node.
func (n *HTTPNodeServer) SetFaultMode(m FaultMode) {
	n.faultMu.Lock()
	n.faultMode = m
	n.faultMu.Unlock()
}

func (n *HTTPNodeServer) currentFault() FaultMode {
	n.faultMu.RLock()
	m := n.faultMode
	n.faultMu.RUnlock()
	return m
}

// Stats returns a snapshot of the node's request and byte counters.
func (n *HTTPNodeServer) Stats() NodeStats {
	return NodeStats{
		Name:      n.name,
		URL:       n.baseURL,
		Gets:      n.gets.Load(),
		Puts:      n.puts.Load(),
		Deletes:   n.deletes.Load(),
		Lists:     n.lists.Load(),
		BytesSent: n.bytesSent.Load(),
		BytesRecv: n.bytesRecv.Load(),
	}
}

// Close shuts down the HTTP server (without closing the caller-owned backend).
func (n *HTTPNodeServer) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return n.server.Shutdown(ctx)
}

func (n *HTTPNodeServer) handleObj(w http.ResponseWriter, r *http.Request) {
	if n.currentFault() == FaultOffline {
		http.Error(w, "node offline", http.StatusServiceUnavailable)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/v1/obj/")
	if err := backend.ValidKey(key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	switch r.Method {
	case http.MethodGet:
		n.gets.Add(1)
		data, ver, err := n.store.Get(ctx, key, backend.Unbounded)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := append([]byte(nil), data...)
		fault := n.currentFault()
		if strings.HasPrefix(key, "enc/chunk/") {
			switch fault {
			case FaultCorruptChunk:
				if len(out) > chunkFrameHeaderSize {
					out[len(out)-1] ^= 0xff
				}
			case FaultTruncateChunk:
				if len(out) > 8 {
					out = out[:len(out)/2]
				}
			}
		} else if strings.HasPrefix(key, "enc/manifest/") && fault == FaultCorruptManifest {
			if len(out) > 2 {
				out[len(out)-2] ^= 0x01
			}
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", string(ver))
		w.Header().Set("Content-Length", strconv.Itoa(len(out)))
		w.WriteHeader(http.StatusOK)
		written, _ := w.Write(out)
		n.bytesSent.Add(int64(written))

	case http.MethodHead:
		size, err := n.store.Stat(ctx, key)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)

	case http.MethodPut:
		n.puts.Add(1)
		body, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPBodySize+1))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if int64(len(body)) > maxHTTPBodySize {
			http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
			return
		}
		n.bytesRecv.Add(int64(len(body)))
		if r.Header.Get("If-None-Match") == "*" {
			ver, err := n.store.PutIfAbsent(ctx, key, body)
			if err != nil {
				if errors.Is(err, backend.ErrConflict) {
					http.Error(w, "conflict", http.StatusPreconditionFailed)
					return
				}
				if errors.Is(err, backend.ErrTooLarge) {
					http.Error(w, "too large", http.StatusRequestEntityTooLarge)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("ETag", string(ver))
			w.WriteHeader(http.StatusCreated)
			return
		}
		if vals, hasIfMatch := r.Header["If-Match"]; hasIfMatch {
			expected := ""
			if len(vals) > 0 {
				expected = vals[0]
			}
			ver, err := n.store.PutIfVersion(ctx, key, body, backend.Version(expected))
			if err != nil {
				if errors.Is(err, backend.ErrConflict) {
					http.Error(w, "conflict", http.StatusPreconditionFailed)
					return
				}
				if errors.Is(err, backend.ErrTooLarge) {
					http.Error(w, "too large", http.StatusRequestEntityTooLarge)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("ETag", string(ver))
			w.WriteHeader(http.StatusOK)
			return
		}
		if err := n.store.Put(ctx, key, body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		n.deletes.Add(1)
		if err := n.store.Delete(ctx, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *HTTPNodeServer) handleList(w http.ResponseWriter, r *http.Request) {
	if n.currentFault() == FaultOffline {
		http.Error(w, "node offline", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	n.lists.Add(1)
	prefix := r.URL.Query().Get("prefix")
	if err := backend.ValidKeyPrefix(prefix); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	objCh, errCh := n.store.List(r.Context(), prefix)
	var objs []backend.Object
	for obj := range objCh {
		objs = append(objs, obj)
	}
	if err := <-errCh; err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(objs)
}

// HTTPBackend implements hanzoai/vfs's pkg/backend.Backend over standard HTTP
// against any independent HTTPNodeServer (zero Hanzo auth or control plane).
type HTTPBackend struct {
	baseURL string
	client  *http.Client
}

// NewHTTPBackend returns a pkg/backend.Backend connected to baseURL.
func NewHTTPBackend(baseURL string) *HTTPBackend {
	return &HTTPBackend{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

func (b *HTTPBackend) objURL(key string) string {
	return b.baseURL + "/v1/obj/" + key
}

func (b *HTTPBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if err := backend.ValidKey(key); err != nil {
		return nil, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.objURL(key), nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, "", backend.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("http backend get %s: status %d", key, resp.StatusCode)
	}
	limit := backend.Past(max)
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, "", err
	}
	if max != backend.Unbounded && int64(len(data)) > max {
		return nil, "", backend.ErrTooLarge
	}
	ver := backend.Version(resp.Header.Get("ETag"))
	if ver == "" {
		sum := blake3.Sum(data)
		ver = backend.Version(hex.EncodeToString(sum[:]))
	}
	return data, ver, nil
}

func (b *HTTPBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	data, _, err := b.Get(ctx, key, backend.Unbounded)
	if err != nil {
		return nil, err
	}
	if off < 0 || n < 0 || off+n > int64(len(data)) {
		return nil, fmt.Errorf("http backend range [%d:%d] out of bounds (%d)", off, off+n, len(data))
	}
	return append([]byte(nil), data[off:off+n]...), nil
}

func (b *HTTPBackend) Put(ctx context.Context, key string, data []byte) error {
	if err := backend.ValidKey(key); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, b.objURL(key), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("http backend put %s: status %d", key, resp.StatusCode)
	}
	return nil
}

func (b *HTTPBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, b.objURL(key), bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("If-None-Match", "*")
	req.ContentLength = int64(len(data))
	resp, err := b.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusPreconditionFailed {
		return "", backend.ErrConflict
	}
	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		return "", backend.ErrTooLarge
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("http backend put-if-absent %s: status %d", key, resp.StatusCode)
	}
	return backend.Version(resp.Header.Get("ETag")), nil
}

func (b *HTTPBackend) PutIfVersion(ctx context.Context, key string, data []byte, expected backend.Version) (backend.Version, error) {
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, b.objURL(key), bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("If-Match", string(expected))
	req.ContentLength = int64(len(data))
	resp, err := b.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusPreconditionFailed {
		return "", backend.ErrConflict
	}
	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		return "", backend.ErrTooLarge
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("http backend put-if-version %s: status %d", key, resp.StatusCode)
	}
	ver := backend.Version(resp.Header.Get("ETag"))
	if ver == "" {
		sum := blake3.Sum(data)
		ver = backend.Version(hex.EncodeToString(sum[:]))
	}
	return ver, nil
}

func (b *HTTPBackend) Delete(ctx context.Context, key string) error {
	if err := backend.ValidKey(key); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, b.objURL(key), nil)
	if err != nil {
		return err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("http backend delete %s: status %d", key, resp.StatusCode)
	}
	return nil
}

func (b *HTTPBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	objCh := make(chan backend.Object, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(objCh)
		defer close(errCh)
		if err := backend.ValidKeyPrefix(prefix); err != nil {
			errCh <- err
			return
		}
		u := b.baseURL + "/v1/list?prefix=" + url.QueryEscape(prefix)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			errCh <- err
			return
		}
		resp, err := b.client.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			errCh <- fmt.Errorf("http backend list %q: status %d", prefix, resp.StatusCode)
			return
		}
		var objs []backend.Object
		if err := json.NewDecoder(resp.Body).Decode(&objs); err != nil {
			errCh <- err
			return
		}
		for _, o := range objs {
			select {
			case objCh <- o:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()
	return objCh, errCh
}

func (b *HTTPBackend) Stat(ctx context.Context, key string) (int64, error) {
	if err := backend.ValidKey(key); err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, b.objURL(key), nil)
	if err != nil {
		return 0, err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, backend.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http backend stat %s: status %d", key, resp.StatusCode)
	}
	sz, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return 0, err
	}
	return sz, nil
}

func (b *HTTPBackend) Close() error {
	b.client.CloseIdleConnections()
	return nil
}

func (b *HTTPBackend) String() string {
	return b.baseURL
}

// NodeEndpoint pairs a human-readable operator name with a pkg/backend.Backend.
type NodeEndpoint struct {
	Name    string
	Backend backend.Backend
}

// ReplicationStats summarizes read/write/failover/quarantine behavior across
// independent storage providers.
type ReplicationStats struct {
	NodeNames          []string         `json:"node_names"`
	QuarantinedNodes   []string         `json:"quarantined_nodes"`
	ChunksVerified     int64            `json:"chunks_verified"`
	ChunksRejected     int64            `json:"chunks_rejected"`
	FailoverRecoveries int64            `json:"failover_recoveries"`
	PerNodeServed      map[string]int64 `json:"per_node_served"`
}

// ReplicatedStore replicates encrypted VFS objects across N independent
// pkg/backend.Backend nodes with configurable write quorum and fail-closed
// BLAKE3-verified read failover + bad-replica quarantine.
type ReplicatedStore struct {
	nodes       []NodeEndpoint
	writeQuorum int

	mu          sync.RWMutex
	quarantined map[string]string // node name -> reason
	served      map[string]int64
	rrCursor    atomic.Uint64

	chunksVerified     atomic.Int64
	chunksRejected     atomic.Int64
	failoverRecoveries atomic.Int64
}

// NewReplicatedStore constructs a multi-node store over independent backends.
// If writeQuorum <= 0, it defaults to len(nodes) (all replicas).
func NewReplicatedStore(nodes []NodeEndpoint, writeQuorum int) (*ReplicatedStore, error) {
	if len(nodes) == 0 {
		return nil, errors.New("encstore: at least one storage node is required")
	}
	if writeQuorum <= 0 || writeQuorum > len(nodes) {
		writeQuorum = len(nodes)
	}
	copied := make([]NodeEndpoint, len(nodes))
	copy(copied, nodes)
	served := make(map[string]int64, len(nodes))
	for _, n := range copied {
		served[n.Name] = 0
	}
	return &ReplicatedStore{
		nodes:       copied,
		writeQuorum: writeQuorum,
		quarantined: make(map[string]string),
		served:      served,
	}, nil
}

// Nodes returns a copy of the configured independent endpoints.
func (s *ReplicatedStore) Nodes() []NodeEndpoint {
	out := make([]NodeEndpoint, len(s.nodes))
	copy(out, s.nodes)
	return out
}

// ResetQuarantine clears any quarantined nodes (useful between test phases).
func (s *ReplicatedStore) ResetQuarantine() {
	s.mu.Lock()
	clear(s.quarantined)
	s.mu.Unlock()
}

func (s *ReplicatedStore) quarantine(name, reason string) {
	s.mu.Lock()
	s.quarantined[name] = reason
	s.mu.Unlock()
}

func (s *ReplicatedStore) isQuarantined(name string) bool {
	s.mu.RLock()
	_, ok := s.quarantined[name]
	s.mu.RUnlock()
	return ok
}

func (s *ReplicatedStore) recordServed(name string) {
	s.mu.Lock()
	s.served[name]++
	s.mu.Unlock()
}

// Stats returns a snapshot of replication, quarantine, and verification metrics.
func (s *ReplicatedStore) Stats() ReplicationStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, len(s.nodes))
	for i, n := range s.nodes {
		names[i] = n.Name
	}
	q := make([]string, 0, len(s.quarantined))
	for name := range s.quarantined {
		q = append(q, name)
	}
	served := make(map[string]int64, len(s.served))
	for k, v := range s.served {
		served[k] = v
	}
	return ReplicationStats{
		NodeNames:          names,
		QuarantinedNodes:   q,
		ChunksVerified:     s.chunksVerified.Load(),
		ChunksRejected:     s.chunksRejected.Load(),
		FailoverRecoveries: s.failoverRecoveries.Load(),
		PerNodeServed:      served,
	}
}

// PutQuorum writes key concurrently to all independent nodes and requires at
// least s.writeQuorum nodes to succeed.
func (s *ReplicatedStore) PutQuorum(ctx context.Context, key string, data []byte) (int, error) {
	if err := backend.ValidKey(key); err != nil {
		return 0, err
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		okCount int
		errs    []error
	)
	for _, node := range s.nodes {
		wg.Add(1)
		go func(ep NodeEndpoint) {
			defer wg.Done()
			if err := ep.Backend.Put(ctx, key, data); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", ep.Name, err))
				mu.Unlock()
				return
			}
			mu.Lock()
			okCount++
			mu.Unlock()
		}(node)
	}
	wg.Wait()
	if okCount < s.writeQuorum {
		return okCount, fmt.Errorf("%w: wrote %d/%d (need %d): %v", ErrQuorumFailed, okCount, len(s.nodes), s.writeQuorum, errors.Join(errs...))
	}
	return okCount, nil
}

// DeleteBestEffort deletes key from every reachable independent node, returning
// the number of nodes where deletion succeeded.
func (s *ReplicatedStore) DeleteBestEffort(ctx context.Context, key string) int {
	var (
		wg      sync.WaitGroup
		okCount atomic.Int64
	)
	for _, node := range s.nodes {
		wg.Add(1)
		go func(ep NodeEndpoint) {
			defer wg.Done()
			if err := ep.Backend.Delete(ctx, key); err == nil {
				okCount.Add(1)
			}
		}(node)
	}
	wg.Wait()
	return int(okCount.Load())
}

// orderedNodes returns non-quarantined nodes in round-robin rotated order so
// verified chunk fetches balance across independent replicas.
func (s *ReplicatedStore) orderedNodes() []NodeEndpoint {
	n := len(s.nodes)
	start := int(s.rrCursor.Add(1)-1) % n
	out := make([]NodeEndpoint, 0, n)
	for i := 0; i < n; i++ {
		ep := s.nodes[(start+i)%n]
		if !s.isQuarantined(ep.Name) {
			out = append(out, ep)
		}
	}
	return out
}

// GetVerifiedChunk fetches an encrypted chunk by ciphertextBLAKE3 from healthy
// replicas, verifies exact ciphertext BLAKE3 digest and length BEFORE returning,
// quarantines any replica returning corrupted bytes, and fails over cleanly.
func (s *ReplicatedStore) GetVerifiedChunk(ctx context.Context, ciphertextBLAKE3 string, expectedSize int64) ([]byte, string, error) {
	key, err := ChunkKey(ciphertextBLAKE3)
	if err != nil {
		return nil, "", err
	}
	candidates := s.orderedNodes()
	if len(candidates) == 0 {
		return nil, "", fmt.Errorf("%w: all nodes quarantined for chunk %s", ErrAllReplicasFailed, ciphertextBLAKE3[:12])
	}
	var (
		hadFailure bool
		lastErr    error
	)
	for _, ep := range candidates {
		data, _, err := ep.Backend.Get(ctx, key, expectedSize+1024)
		if err != nil {
			hadFailure = true
			lastErr = fmt.Errorf("%s: %w", ep.Name, err)
			continue
		}
		if expectedSize > 0 && int64(len(data)) != expectedSize {
			s.chunksRejected.Add(1)
			s.quarantine(ep.Name, fmt.Sprintf("truncated chunk %s (%d != %d)", ciphertextBLAKE3[:12], len(data), expectedSize))
			hadFailure = true
			lastErr = fmt.Errorf("%w: %s returned %d bytes, want %d", ErrChunkCorrupted, ep.Name, len(data), expectedSize)
			continue
		}
		sum := blake3.Sum(data)
		gotCID := hex.EncodeToString(sum[:])
		if gotCID != ciphertextBLAKE3 {
			s.chunksRejected.Add(1)
			s.quarantine(ep.Name, fmt.Sprintf("ciphertext BLAKE3 mismatch on %s", ciphertextBLAKE3[:12]))
			hadFailure = true
			lastErr = fmt.Errorf("%w: %s ciphertext BLAKE3 mismatch (got %s, want %s)", ErrChunkCorrupted, ep.Name, gotCID[:12], ciphertextBLAKE3[:12])
			continue
		}
		s.chunksVerified.Add(1)
		s.recordServed(ep.Name)
		if hadFailure {
			s.failoverRecoveries.Add(1)
		}
		return data, ep.Name, nil
	}
	return nil, "", fmt.Errorf("%w: chunk %s: %v", ErrAllReplicasFailed, ciphertextBLAKE3[:12], lastErr)
}

// GetVerifiedManifest fetches a SignedManifest by manifestCID, verifies both
// its content-addressed BLAKE3 ManifestCID and its Ed25519 signature, failing
// over across replicas if any node serves a corrupted manifest.
func (s *ReplicatedStore) GetVerifiedManifest(ctx context.Context, manifestCID string, trustedOwnerEdPubs []ed25519.PublicKey) (*SignedManifest, []byte, error) {
	key, err := ManifestKey(manifestCID)
	if err != nil {
		return nil, nil, err
	}
	var lastErr error
	for _, ep := range s.nodes {
		raw, _, err := ep.Backend.Get(ctx, key, MaxManifestSize)
		if err != nil {
			lastErr = err
			continue
		}
		var m SignedManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			s.quarantine(ep.Name, "invalid manifest JSON")
			lastErr = fmt.Errorf("%w: %v", ErrInvalidManifest, err)
			continue
		}
		computedCID, err := m.ComputeCID()
		if err != nil || computedCID != manifestCID {
			s.quarantine(ep.Name, "manifest CID mismatch")
			lastErr = fmt.Errorf("%w: manifest CID mismatch from %s", ErrInvalidManifest, ep.Name)
			continue
		}
		if m.ManifestCID != "" && m.ManifestCID != manifestCID {
			s.quarantine(ep.Name, "manifest self-CID mismatch")
			lastErr = fmt.Errorf("%w: manifest self-CID mismatch from %s", ErrInvalidManifest, ep.Name)
			continue
		}
		if err := m.Verify(trustedOwnerEdPubs); err != nil {
			// A manifest whose CID matched the requested CID has a cryptographically
			// bound bad signature or untrusted owner — fail closed immediately.
			return nil, nil, err
		}
		if m.ManifestCID == "" {
			m.ManifestCID = manifestCID
		}
		return &m, raw, nil
	}
	if lastErr == nil {
		lastErr = backend.ErrNotFound
	}
	return nil, nil, fmt.Errorf("%w: manifest %s: %w", ErrAllReplicasFailed, manifestCID[:12], lastErr)
}

func revocationNewer(a, b *SignedRevocationList) bool {
	return a.Seq > b.Seq || (a.Seq == b.Seq && a.CurrentEpoch > b.CurrentEpoch)
}

// PutRevocationQuorum publishes revList monotonically across all replicas using
// conditional compare-and-swap (PutIfAbsent / PutIfVersion) so a delayed or
// stale publication can never overwrite a higher-sequence or conflicting
// equal-sequence valid revocation list.
func (s *ReplicatedStore) PutRevocationQuorum(ctx context.Context, ownerKeyID string, ownerEdPub ed25519.PublicKey, revList *SignedRevocationList) (int, error) {
	key, err := RevocationKey(ownerKeyID)
	if err != nil {
		return 0, err
	}
	if err := revList.Verify(ownerEdPub); err != nil {
		return 0, err
	}
	if revList.Owner.KeyID != ownerKeyID {
		return 0, fmt.Errorf("%w: revocation list owner %q != %q", ErrUntrustedOwner, revList.Owner.KeyID, ownerKeyID)
	}
	raw, err := json.Marshal(revList)
	if err != nil {
		return 0, err
	}
	if int64(len(raw)) > MaxRevocationListSize {
		return 0, fmt.Errorf("%w: revocation list size %d exceeds limit %d", backend.ErrTooLarge, len(raw), MaxRevocationListSize)
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		okCount   int
		errs      []error
		staleErrs []error
	)
	for _, node := range s.nodes {
		wg.Add(1)
		go func(ep NodeEndpoint) {
			defer wg.Done()
			if err := s.putRevocationOnNode(ctx, ep, key, ownerKeyID, ownerEdPub, revList, raw); err != nil {
				mu.Lock()
				if errors.Is(err, ErrStaleRevocation) {
					staleErrs = append(staleErrs, err)
				} else {
					errs = append(errs, fmt.Errorf("%s: %w", ep.Name, err))
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			okCount++
			mu.Unlock()
		}(node)
	}
	wg.Wait()
	if len(staleErrs) > 0 {
		return okCount, fmt.Errorf("%w: %v", ErrStaleRevocation, errors.Join(staleErrs...))
	}
	if okCount < s.writeQuorum {
		return okCount, fmt.Errorf("%w: wrote revocation list to %d/%d (need %d): %v", ErrQuorumFailed, okCount, len(s.nodes), s.writeQuorum, errors.Join(errs...))
	}
	return okCount, nil
}

func (s *ReplicatedStore) putRevocationOnNode(ctx context.Context, ep NodeEndpoint, key, ownerKeyID string, ownerEdPub ed25519.PublicKey, revList *SignedRevocationList, raw []byte) error {
	const maxCASAttempts = 8
	for attempt := 0; attempt < maxCASAttempts; attempt++ {
		curRaw, curVer, err := ep.Backend.Get(ctx, key, MaxRevocationListSize)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				_, putErr := ep.Backend.PutIfAbsent(ctx, key, raw)
				if putErr == nil {
					return nil
				}
				if errors.Is(putErr, backend.ErrConflict) {
					continue
				}
				return putErr
			}
			return err
		}
		var cur SignedRevocationList
		if json.Unmarshal(curRaw, &cur) == nil && cur.Verify(ownerEdPub) == nil && cur.Owner.KeyID == ownerKeyID {
			if cur.Seq > revList.Seq || cur.CurrentEpoch > revList.CurrentEpoch {
				return fmt.Errorf("%w: %s holds seq=%d epoch=%d, refusing seq=%d epoch=%d",
					ErrStaleRevocation, ep.Name, cur.Seq, cur.CurrentEpoch, revList.Seq, revList.CurrentEpoch)
			}
			if cur.Seq == revList.Seq {
				if bytes.Equal(curRaw, raw) {
					return nil
				}
				return fmt.Errorf("%w: %s holds conflicting revocation list at seq=%d (cur epoch=%d, candidate epoch=%d)",
					ErrStaleRevocation, ep.Name, cur.Seq, cur.CurrentEpoch, revList.CurrentEpoch)
			}
		}
		_, putErr := ep.Backend.PutIfVersion(ctx, key, raw, curVer)
		if putErr == nil {
			return nil
		}
		if errors.Is(putErr, backend.ErrConflict) {
			continue
		}
		return putErr
	}
	return fmt.Errorf("%w: %s exceeded CAS retry limit", ErrStaleRevocation, ep.Name)
}

// GetAllValidRevocationLists fetches every valid owner-signed revocation list
// observed across reachable replicas. Returns (nil, nil) only when every
// replica reports genuine absence (backend.ErrNotFound); fails closed if no
// valid record could be obtained after retrieval or verification failures.
func (s *ReplicatedStore) GetAllValidRevocationLists(ctx context.Context, ownerKeyID string, ownerEdPub ed25519.PublicKey) ([]*SignedRevocationList, error) {
	key, err := RevocationKey(ownerKeyID)
	if err != nil {
		return nil, err
	}
	var (
		valid      []*SignedRevocationList
		hadFailure bool
		lastErr    error
	)
	for _, ep := range s.nodes {
		raw, _, err := ep.Backend.Get(ctx, key, MaxRevocationListSize)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				continue
			}
			hadFailure = true
			lastErr = fmt.Errorf("%s: %w", ep.Name, err)
			continue
		}
		var r SignedRevocationList
		if err := json.Unmarshal(raw, &r); err != nil {
			hadFailure = true
			lastErr = fmt.Errorf("%w: %s: %v", ErrInvalidManifest, ep.Name, err)
			continue
		}
		if err := r.Verify(ownerEdPub); err != nil {
			hadFailure = true
			lastErr = fmt.Errorf("%s: %w", ep.Name, err)
			continue
		}
		if r.Owner.KeyID != ownerKeyID {
			hadFailure = true
			lastErr = fmt.Errorf("%w: %s revocation list owner %q != %q", ErrUntrustedOwner, ep.Name, r.Owner.KeyID, ownerKeyID)
			continue
		}
		copyR := r
		valid = append(valid, &copyR)
	}
	if len(valid) == 0 && hadFailure {
		return nil, fmt.Errorf("%w: revocation list for %s: %w", ErrAllReplicasFailed, ownerKeyID, lastErr)
	}
	return valid, nil
}

// GetLatestRevocationList fetches the highest-Seq valid SignedRevocationList
// signed by ownerEdPub across all reachable nodes. Returns (nil, nil) only when
// every replica reports genuine absence (backend.ErrNotFound); fails closed
// with an error if no valid record could be obtained after retrieval or
// verification failures.
func (s *ReplicatedStore) GetLatestRevocationList(ctx context.Context, ownerKeyID string, ownerEdPub ed25519.PublicKey) (*SignedRevocationList, error) {
	valid, err := s.GetAllValidRevocationLists(ctx, ownerKeyID, ownerEdPub)
	if err != nil || len(valid) == 0 {
		return nil, err
	}
	best := valid[0]
	for _, r := range valid[1:] {
		if revocationNewer(r, best) {
			best = r
		}
	}
	return best, nil
}

// GetTombstone fetches and verifies any owner-signed DeletionTombstone for
// artifactID across all reachable nodes. Returns (nil, nil) only when every
// replica reports genuine absence (backend.ErrNotFound); fails closed with an
// error if no valid tombstone could be obtained after retrieval or verification
// failures.
func (s *ReplicatedStore) GetTombstone(ctx context.Context, artifactID string, ownerEdPub ed25519.PublicKey) (*DeletionTombstone, error) {
	key, err := TombstoneKey(artifactID)
	if err != nil {
		return nil, err
	}
	var (
		hadFailure bool
		lastErr    error
	)
	for _, ep := range s.nodes {
		raw, _, err := ep.Backend.Get(ctx, key, MaxTombstoneSize)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				continue
			}
			hadFailure = true
			lastErr = fmt.Errorf("%s: %w", ep.Name, err)
			continue
		}
		var t DeletionTombstone
		if err := json.Unmarshal(raw, &t); err != nil {
			hadFailure = true
			lastErr = fmt.Errorf("%w: %s: %v", ErrInvalidManifest, ep.Name, err)
			continue
		}
		if err := t.Verify(ownerEdPub); err != nil {
			hadFailure = true
			lastErr = fmt.Errorf("%s: %w", ep.Name, err)
			continue
		}
		if t.ArtifactID != artifactID {
			hadFailure = true
			lastErr = fmt.Errorf("%w: %s tombstone artifact_id %q != %q", ErrInvalidManifest, ep.Name, t.ArtifactID, artifactID)
			continue
		}
		return &t, nil
	}
	if hadFailure {
		return nil, fmt.Errorf("%w: tombstone for %s: %w", ErrAllReplicasFailed, artifactID, lastErr)
	}
	return nil, nil
}
