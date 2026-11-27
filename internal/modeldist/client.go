package modeldist

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

// ClientConfig configures a verifying, fail-closed multi-provider model client.
type ClientConfig struct {
	Policy                   VerificationPolicy
	Concurrency              int             // parallel chunk workers (default 4)
	QuarantineAfterBadChunks int             // bad chunks before quarantining a mirror (default 1)
	CheckpointDir            string          // optional directory for atomic resume checkpoints
	ChunkStagingStore        backend.Backend // optional VFS/S3 backend to mirror verified chunks
	PerChunkTimeout          time.Duration   // per-provider chunk fetch timeout (default 15s)
	CheckpointFlushInterval  int             // persist checkpoint every N verified chunks (default 1)
}

// PullOptions tunes a single PullToDir or PullArtifactStream invocation.
type PullOptions struct {
	// InterruptAfterChunks, when > 0, deterministically cancels the transfer
	// after that many newly downloaded chunks have been verified and persisted,
	// returning ErrTransferInterrupted so resume behavior can be tested cleanly.
	InterruptAfterChunks int
}

// ErrTransferInterrupted is returned when PullOptions.InterruptAfterChunks triggers.
var ErrTransferInterrupted = errors.New("modeldist: transfer interrupted")

// PullReport captures verified transfer metrics, provider breakdown, and root digests.
type PullReport struct {
	ManifestCID          string            `json:"manifest_cid"`
	ModelID              string            `json:"model_id"`
	Version              string            `json:"version"`
	TotalBytes           int64             `json:"total_bytes"`
	VerifiedBytes        int64             `json:"verified_bytes"`
	TotalChunks          int               `json:"total_chunks"`
	DownloadedChunks     int               `json:"downloaded_chunks"`
	ResumedChunks        int               `json:"resumed_chunks"`
	RejectedChunks       int               `json:"rejected_chunks"`
	QuarantinedProviders []string          `json:"quarantined_providers"`
	ProviderChunkCounts  map[string]int    `json:"provider_chunk_counts"`
	OriginChunksServed   int               `json:"origin_chunks_served"`
	MirrorChunksServed   int               `json:"mirror_chunks_served"`
	TTFTMs               float64           `json:"ttft_ms"`
	ElapsedMs            float64           `json:"elapsed_ms"`
	ThroughputMBps       float64           `json:"throughput_mbps"`
	ChunkLatencyP50Ms    float64           `json:"chunk_latency_p50_ms"`
	ChunkLatencyP99Ms    float64           `json:"chunk_latency_p99_ms"`
	ArtifactRootBLAKE3   map[string]string `json:"artifact_root_blake3"`
	ArtifactRootSHA256   map[string]string `json:"artifact_root_sha256"`
}

// CheckpointState records verified chunk hashes for resumable transfers.
type CheckpointState struct {
	ManifestCID    string         `json:"manifest_cid"`
	ArtifactPath   string         `json:"artifact_path"`
	TotalChunks    int            `json:"total_chunks"`
	VerifiedChunks map[int]string `json:"verified_chunks"`
	BytesVerified  int64          `json:"bytes_verified"`
	UpdatedAt      string         `json:"updated_at"`
}

// Client downloads, cryptographically verifies, and reconstructs models from
// independent chunk providers. Verification is unconditionally fail-closed.
type Client struct {
	cfg ClientConfig
}

// NewClient constructs a fail-closed verifying Client.
func NewClient(cfg ClientConfig) *Client {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.QuarantineAfterBadChunks <= 0 {
		cfg.QuarantineAfterBadChunks = 1
	}
	if cfg.PerChunkTimeout <= 0 {
		cfg.PerChunkTimeout = 15 * time.Second
	}
	if cfg.CheckpointFlushInterval <= 0 {
		cfg.CheckpointFlushInterval = 1
	}
	return &Client{cfg: cfg}
}

type providerPool struct {
	providers []ChunkProvider
	maxBad    int

	mu          sync.Mutex
	badCounts   map[string]int
	quarantined map[string]bool
	served      map[string]int
	rejected    int
	originHits  int
	mirrorHits  int
}

func newProviderPool(providers []ChunkProvider, maxBad int) *providerPool {
	return &providerPool{
		providers:   providers,
		maxBad:      maxBad,
		badCounts:   make(map[string]int, len(providers)),
		quarantined: make(map[string]bool, len(providers)),
		served:      make(map[string]int, len(providers)),
	}
}

func (pp *providerPool) isQuarantined(id string) bool {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return pp.quarantined[id]
}

func (pp *providerPool) recordGood(p ChunkProvider) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.served[p.ID()]++
	if p.IsOrigin() {
		pp.originHits++
	} else {
		pp.mirrorHits++
	}
}

func (pp *providerPool) recordBadDigest(id string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.rejected++
	pp.badCounts[id]++
	if pp.badCounts[id] >= pp.maxBad {
		pp.quarantined[id] = true
	}
}

func (pp *providerPool) snapshot() (rejected, originHits, mirrorHits int, served map[string]int, quarantined []string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	served = make(map[string]int, len(pp.served))
	for k, v := range pp.served {
		served[k] = v
	}
	for k, q := range pp.quarantined {
		if q {
			quarantined = append(quarantined, k)
		}
	}
	sort.Strings(quarantined)
	return pp.rejected, pp.originHits, pp.mirrorHits, served, quarantined
}

// FetchVerifiedManifest retrieves a manifest by CID from the first healthy
// provider whose payload parses, matches cidHex, and passes VerificationPolicy.
func (c *Client) FetchVerifiedManifest(ctx context.Context, cidHex string, providers []ChunkProvider) (*ManifestV1, ChunkProvider, error) {
	cidHex = strings.ToLower(strings.TrimSpace(cidHex))
	if !isHexDigest64(cidHex) {
		return nil, nil, fmt.Errorf("%w: invalid cid %q", ErrInvalidManifest, cidHex)
	}
	if len(providers) == 0 {
		return nil, nil, fmt.Errorf("%w: no providers configured", ErrAllProvidersFailed)
	}
	var (
		lastErr      error
		integrityErr error
	)
	for _, p := range providers {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		raw, err := p.GetManifest(ctx, cidHex)
		if err != nil {
			lastErr = err
			continue
		}
		m, err := ParseManifest(raw)
		if err != nil {
			integrityErr = err
			lastErr = err
			continue
		}
		if strings.ToLower(m.ManifestCID) != cidHex {
			integrityErr = fmt.Errorf("%w: provider %s returned CID %s, want %s", ErrManifestCIDMismatch, p.ID(), m.ManifestCID, cidHex)
			lastErr = integrityErr
			continue
		}
		computedCID, err := m.ComputeCID()
		if err != nil {
			integrityErr = err
			lastErr = err
			continue
		}
		if computedCID != cidHex {
			integrityErr = fmt.Errorf("%w: provider %s payload computed CID %s, want %s", ErrManifestCIDMismatch, p.ID(), computedCID, cidHex)
			lastErr = integrityErr
			continue
		}
		if err := m.Verify(c.cfg.Policy); err != nil {
			// License and version checks depend strictly on the unsigned body,
			// which is now cryptographically bound to cidHex, so they are fatal.
			// Signature, structural, or provenance failures can be mirror-specific
			// (e.g. stripped/tampered signatures outside the unsigned CID body),
			// so record them and continue failing over to remaining mirrors.
			if errors.Is(err, ErrLicenseRejected) || errors.Is(err, ErrVersionRejected) {
				return nil, nil, err
			}
			integrityErr = err
			lastErr = err
			continue
		}
		return m, p, nil
	}
	if integrityErr != nil {
		return nil, nil, fmt.Errorf("%w: fetch manifest %s: %w", ErrAllProvidersFailed, cidHex, integrityErr)
	}
	if lastErr == nil {
		lastErr = ErrManifestNotFound
	}
	return nil, nil, fmt.Errorf("%w: fetch manifest %s: %w", ErrAllProvidersFailed, cidHex, lastErr)
}

// fetchVerifiedChunk attempts to fetch and cryptographically verify ref across
// all non-quarantined providers starting from startIdx.
func (c *Client) fetchVerifiedChunk(ctx context.Context, pool *providerPool, ref ChunkRef, startIdx int) ([]byte, ChunkProvider, time.Duration, error) {
	n := len(pool.providers)
	if n == 0 {
		return nil, nil, 0, fmt.Errorf("%w: no providers", ErrAllProvidersFailed)
	}
	var (
		lastErr       error
		digestFailure error
	)
	for attempt := 0; attempt < n; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, 0, err
		}
		p := pool.providers[(startIdx+attempt)%n]
		if pool.isQuarantined(p.ID()) {
			continue
		}
		t0 := time.Now()
		reqCtx, cancel := context.WithTimeout(ctx, c.cfg.PerChunkTimeout)
		data, err := p.GetChunk(reqCtx, ref.BLAKE3, ref.Size)
		cancel()
		if err != nil {
			lastErr = err
			if errors.Is(err, ErrChunkSizeMismatch) {
				pool.recordBadDigest(p.ID())
				digestFailure = err
			}
			continue
		}
		if err := VerifyChunkBytes(ref, data); err != nil {
			pool.recordBadDigest(p.ID())
			digestFailure = err
			lastErr = err
			continue
		}
		pool.recordGood(p)
		return data, p, time.Since(t0), nil
	}
	if digestFailure != nil {
		return nil, nil, 0, fmt.Errorf("%w: chunk %d (%s): %w", ErrAllProvidersFailed, ref.Index, ref.BLAKE3, digestFailure)
	}
	if lastErr != nil {
		return nil, nil, 0, fmt.Errorf("%w: chunk %d (%s): %w", ErrAllProvidersFailed, ref.Index, ref.BLAKE3, lastErr)
	}
	return nil, nil, 0, fmt.Errorf("%w: chunk %d (%s): no healthy providers remain", ErrAllProvidersFailed, ref.Index, ref.BLAKE3)
}

// PullToDir verifies m against ClientConfig.Policy and downloads every artifact
// into outDir using resumable checkpoints and multi-provider failover.
func (c *Client) PullToDir(ctx context.Context, m *ManifestV1, providers []ChunkProvider, outDir string, opts PullOptions) (*PullReport, error) {
	if err := m.Verify(c.cfg.Policy); err != nil {
		return nil, err
	}
	if len(providers) == 0 {
		return nil, fmt.Errorf("%w: no providers supplied", ErrAllProvidersFailed)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	outRoot, err := os.OpenRoot(outDir)
	if err != nil {
		return nil, err
	}
	defer outRoot.Close()
	if c.cfg.CheckpointDir != "" {
		if err := os.MkdirAll(c.cfg.CheckpointDir, 0o755); err != nil {
			return nil, err
		}
	}

	start := time.Now()
	pool := newProviderPool(providers, c.cfg.QuarantineAfterBadChunks)
	report := &PullReport{
		ManifestCID:        m.ManifestCID,
		ModelID:            m.ModelID,
		Version:            m.Version,
		TotalBytes:         m.TotalBytes(),
		TotalChunks:        m.TotalChunks(),
		ArtifactRootBLAKE3: make(map[string]string, len(m.Artifacts)),
		ArtifactRootSHA256: make(map[string]string, len(m.Artifacts)),
	}

	var (
		latMu          sync.Mutex
		chunkLatencies []time.Duration
		ttftSet        atomic.Bool
		ttftNs         atomic.Int64
		downloaded     atomic.Int64
		resumed        atomic.Int64
		verifiedBytes  atomic.Int64
	)

	recordLatency := func(d time.Duration) {
		if ttftSet.CompareAndSwap(false, true) {
			ttftNs.Store(time.Since(start).Nanoseconds())
		}
		latMu.Lock()
		chunkLatencies = append(chunkLatencies, d)
		latMu.Unlock()
	}

	seenInfos := make([]os.FileInfo, 0, len(m.Artifacts))
	for artIdx, art := range m.Artifacts {
		f, fi, err := openConfinedArtifactFile(outRoot, art.Path, seenInfos)
		if err != nil {
			return nil, err
		}
		seenInfos = append(seenInfos, fi)

		ckptPath := c.checkpointPath(m.ManifestCID, artIdx)
		ckpt := loadCheckpoint(ckptPath, m.ManifestCID, art.Path, len(art.Chunks))

		// Re-verify any previously checkpointed chunks against actual on-disk bytes.
		pending := make([]ChunkRef, 0, len(art.Chunks))
		diskBuf := make([]byte, art.ChunkSize)
		for _, ref := range art.Chunks {
			if savedHash, ok := ckpt.VerifiedChunks[ref.Index]; ok && strings.EqualFold(savedHash, ref.BLAKE3) {
				buf := diskBuf[:ref.Size]
				n, readErr := f.ReadAt(buf, ref.Offset)
				if readErr == nil && int64(n) == ref.Size && VerifyChunkBytes(ref, buf) == nil {
					resumed.Add(1)
					verifiedBytes.Add(ref.Size)
					continue
				}
				delete(ckpt.VerifiedChunks, ref.Index)
			}
			pending = append(pending, ref)
		}

		if len(pending) > 0 {
			if err := c.downloadArtifactChunks(ctx, pool, f, ckpt, ckptPath, pending, opts, &downloaded, &verifiedBytes, recordLatency); err != nil {
				_ = f.Sync()
				_ = f.Close()
				c.finalizeReport(report, start, pool, &ttftNs, &downloaded, &resumed, &verifiedBytes, chunkLatencies)
				return report, err
			}
		}

		if err := f.Truncate(art.Size); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}

		// Verify full-artifact streaming BLAKE3 and SHA-256 roots from disk.
		gotB3, gotS2, err := hashFileDual(f)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		if gotB3 != strings.ToLower(art.RootBLAKE3) || gotS2 != strings.ToLower(art.RootSHA256) {
			return nil, fmt.Errorf("%w: artifact %q root mismatch (b3=%s want=%s, s2=%s want=%s)",
				ErrRootDigestMismatch, art.Path, gotB3, art.RootBLAKE3, gotS2, art.RootSHA256)
		}
		report.ArtifactRootBLAKE3[art.Path] = gotB3
		report.ArtifactRootSHA256[art.Path] = gotS2
		if ckptPath != "" {
			_ = os.Remove(ckptPath)
		}
	}

	c.finalizeReport(report, start, pool, &ttftNs, &downloaded, &resumed, &verifiedBytes, chunkLatencies)
	return report, nil
}

func (c *Client) downloadArtifactChunks(
	ctx context.Context,
	pool *providerPool,
	f *os.File,
	ckpt *CheckpointState,
	ckptPath string,
	pending []ChunkRef,
	opts PullOptions,
	downloaded *atomic.Int64,
	verifiedBytes *atomic.Int64,
	recordLatency func(time.Duration),
) error {
	workers := c.cfg.Concurrency
	if opts.InterruptAfterChunks > 0 {
		// Deterministic single-worker ordering when testing exact interrupt cutoff.
		workers = 1
	}
	if workers > len(pending) {
		workers = len(pending)
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan ChunkRef, len(pending))
	for _, ref := range pending {
		jobs <- ref
	}
	close(jobs)

	var (
		wg          sync.WaitGroup
		ckptMu      sync.Mutex
		errMu       sync.Mutex
		firstErr    error
		sinceFlush  int
		interrupted atomic.Bool
	)

	setErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ref := range jobs {
				if workCtx.Err() != nil {
					return
				}
				data, _, dur, err := c.fetchVerifiedChunk(workCtx, pool, ref, ref.Index)
				if err != nil {
					setErr(err)
					return
				}
				if _, err := f.WriteAt(data, ref.Offset); err != nil {
					setErr(fmt.Errorf("modeldist: write chunk %d: %w", ref.Index, err))
					return
				}
				if c.cfg.ChunkStagingStore != nil {
					if key, kerr := ChunkKey(ref.BLAKE3); kerr == nil {
						if perr := c.cfg.ChunkStagingStore.Put(workCtx, key, data); perr != nil {
							setErr(fmt.Errorf("modeldist: stage chunk %d to VFS backend: %w", ref.Index, perr))
							return
						}
					}
				}
				recordLatency(dur)
				dl := downloaded.Add(1)
				verifiedBytes.Add(ref.Size)

				ckptMu.Lock()
				ckpt.VerifiedChunks[ref.Index] = ref.BLAKE3
				ckpt.BytesVerified += ref.Size
				sinceFlush++
				needFlush := ckptPath != "" && (sinceFlush >= c.cfg.CheckpointFlushInterval || (opts.InterruptAfterChunks > 0 && int(dl) >= opts.InterruptAfterChunks))
				if needFlush {
					ckpt.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
					_ = saveCheckpoint(ckptPath, ckpt)
					sinceFlush = 0
				}
				ckptMu.Unlock()

				if opts.InterruptAfterChunks > 0 && int(dl) >= opts.InterruptAfterChunks {
					interrupted.Store(true)
					setErr(ErrTransferInterrupted)
					return
				}
			}
		}()
	}

	wg.Wait()
	if ckptPath != "" {
		ckptMu.Lock()
		ckpt.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		_ = saveCheckpoint(ckptPath, ckpt)
		ckptMu.Unlock()
	}
	if interrupted.Load() {
		return ErrTransferInterrupted
	}
	return firstErr
}

// PullArtifactStream streams one artifact from providers in parallel while
// keeping bounded memory (O(concurrency * chunkSize)), verifying every chunk
// and the full in-order streaming BLAKE3 + SHA-256 roots, and writing verified
// bytes sequentially to sink (which may be io.Discard for memory-bounded tests).
func (c *Client) PullArtifactStream(ctx context.Context, m *ManifestV1, artifactIdx int, providers []ChunkProvider, sink io.Writer) (*PullReport, error) {
	if err := m.Verify(c.cfg.Policy); err != nil {
		return nil, err
	}
	if artifactIdx < 0 || artifactIdx >= len(m.Artifacts) {
		return nil, fmt.Errorf("%w: artifact index %d out of range", ErrInvalidManifest, artifactIdx)
	}
	if len(providers) == 0 {
		return nil, fmt.Errorf("%w: no providers supplied", ErrAllProvidersFailed)
	}
	if sink == nil {
		sink = io.Discard
	}

	art := m.Artifacts[artifactIdx]
	start := time.Now()
	pool := newProviderPool(providers, c.cfg.QuarantineAfterBadChunks)
	report := &PullReport{
		ManifestCID:        m.ManifestCID,
		ModelID:            m.ModelID,
		Version:            m.Version,
		TotalBytes:         art.Size,
		TotalChunks:        len(art.Chunks),
		ArtifactRootBLAKE3: make(map[string]string, 1),
		ArtifactRootSHA256: make(map[string]string, 1),
	}

	workers := c.cfg.Concurrency
	if workers > len(art.Chunks) && len(art.Chunks) > 0 {
		workers = len(art.Chunks)
	}
	if workers <= 0 {
		workers = 1
	}

	type fetchedChunk struct {
		index int
		data  []byte
		dur   time.Duration
		err   error
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Credit-bounded dispatch window ties new chunk fetches strictly to in-order
	// consumption so peak RAM stays O(workers * chunkSize) even if an early
	// chunk stalls.
	window := workers * 2
	credits := make(chan struct{}, window)
	for i := 0; i < window; i++ {
		credits <- struct{}{}
	}
	jobs := make(chan ChunkRef, window)
	results := make(chan fetchedChunk, window)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ref := range jobs {
				if workCtx.Err() != nil {
					return
				}
				data, _, dur, err := c.fetchVerifiedChunk(workCtx, pool, ref, ref.Index)
				select {
				case results <- fetchedChunk{index: ref.Index, data: data, dur: dur, err: err}:
				case <-workCtx.Done():
					return
				}
				if err != nil {
					cancel()
					return
				}
			}
		}()
	}

	// Feeder goroutine acquires one credit per dispatched chunk; credits are
	// replenished exclusively when the coordinator consumes chunks in order.
	go func() {
		defer close(jobs)
		for _, ref := range art.Chunks {
			select {
			case <-credits:
			case <-workCtx.Done():
				return
			}
			select {
			case jobs <- ref:
			case <-workCtx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	rootB3 := blake3.New()
	rootS2 := sha256.New()
	reorder := make(map[int][]byte, window)
	nextIdx := 0

	var (
		ttftSet        atomic.Bool
		ttftNs         atomic.Int64
		downloaded     atomic.Int64
		resumed        atomic.Int64
		verifiedBytes  atomic.Int64
		chunkLatencies = make([]time.Duration, 0, len(art.Chunks))
		streamErr      error
	)

	for res := range results {
		if res.err != nil {
			if streamErr == nil {
				streamErr = res.err
				cancel()
			}
			continue
		}
		if streamErr != nil {
			continue
		}
		chunkLatencies = append(chunkLatencies, res.dur)
		downloaded.Add(1)
		verifiedBytes.Add(int64(len(res.data)))
		reorder[res.index] = res.data

		for {
			buf, ok := reorder[nextIdx]
			if !ok {
				break
			}
			delete(reorder, nextIdx)
			_, _ = rootB3.Write(buf)
			_, _ = rootS2.Write(buf)
			if _, err := sink.Write(buf); err != nil && streamErr == nil {
				streamErr = err
				cancel()
				clear(reorder)
				break
			}
			if ttftSet.CompareAndSwap(false, true) {
				ttftNs.Store(time.Since(start).Nanoseconds())
			}
			nextIdx++
			select {
			case credits <- struct{}{}:
			case <-workCtx.Done():
			}
		}
	}

	c.finalizeReport(report, start, pool, &ttftNs, &downloaded, &resumed, &verifiedBytes, chunkLatencies)
	if streamErr != nil {
		return report, streamErr
	}
	if nextIdx != len(art.Chunks) {
		return report, fmt.Errorf("%w: streamed %d/%d chunks", ErrAllProvidersFailed, nextIdx, len(art.Chunks))
	}

	gotB3 := hex.EncodeToString(rootB3.Sum(nil))
	gotS2 := hex.EncodeToString(rootS2.Sum(nil))
	if gotB3 != strings.ToLower(art.RootBLAKE3) || gotS2 != strings.ToLower(art.RootSHA256) {
		return report, fmt.Errorf("%w: stream artifact %q root mismatch (b3=%s want=%s)", ErrRootDigestMismatch, art.Path, gotB3, art.RootBLAKE3)
	}
	report.ArtifactRootBLAKE3[art.Path] = gotB3
	report.ArtifactRootSHA256[art.Path] = gotS2
	return report, nil
}

func sameFileAny(fi os.FileInfo, seen []os.FileInfo) bool {
	for _, prev := range seen {
		if os.SameFile(fi, prev) {
			return true
		}
	}
	return false
}

// openConfinedArtifactFile opens or creates relSlashPath strictly within root,
// rejecting any symlink components along parent directories or the target file
// and detaching any hard-linked or previously-opened aliased target so each
// artifact writes to an independent regular inode.
func openConfinedArtifactFile(root *os.Root, relSlashPath string, seenInfos []os.FileInfo) (*os.File, os.FileInfo, error) {
	if err := validateRelativePath(relSlashPath); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	needDetach := false
	segments := strings.Split(relSlashPath, "/")
	for i := range segments {
		prefix := filepath.Join(segments[:i+1]...)
		fi, err := root.Lstat(prefix)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			return nil, nil, fmt.Errorf("modeldist: inspect artifact path %q: %w", relSlashPath, err)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return nil, nil, fmt.Errorf("%w: refusing to traverse symlink %q in output directory", ErrInvalidManifest, prefix)
		}
		isLast := i == len(segments)-1
		if !isLast && !fi.IsDir() {
			return nil, nil, fmt.Errorf("%w: parent path %q is not a directory", ErrInvalidManifest, prefix)
		}
		if isLast && !fi.Mode().IsRegular() {
			return nil, nil, fmt.Errorf("%w: target path %q is not a regular file", ErrInvalidManifest, prefix)
		}
		if isLast && (hasMultipleHardLinks(fi) || sameFileAny(fi, seenInfos)) {
			needDetach = true
		}
	}

	relPath := filepath.FromSlash(relSlashPath)
	if relDir := filepath.Dir(relPath); relDir != "." {
		if err := root.MkdirAll(relDir, 0o755); err != nil {
			return nil, nil, fmt.Errorf("modeldist: create artifact dir %q: %w", relDir, err)
		}
	}

	openExclusive := func() (*os.File, os.FileInfo, error) {
		if err := root.Remove(relPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("modeldist: detach aliased artifact %q: %w", relSlashPath, err)
		}
		f, err := root.OpenFile(relPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("modeldist: create independent artifact %q: %w", relSlashPath, err)
		}
		fi, err := verifyOpenedArtifactFile(root, relPath, relSlashPath, f)
		if err != nil {
			_ = f.Close()
			return nil, nil, err
		}
		if hasMultipleHardLinks(fi) || sameFileAny(fi, seenInfos) {
			_ = f.Close()
			return nil, nil, fmt.Errorf("%w: artifact %q shares an inode with another link", ErrInvalidManifest, relSlashPath)
		}
		return f, fi, nil
	}

	if needDetach {
		return openExclusive()
	}

	f, err := root.OpenFile(relPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("modeldist: open artifact %q: %w", relSlashPath, err)
	}
	fi, err := verifyOpenedArtifactFile(root, relPath, relSlashPath, f)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if hasMultipleHardLinks(fi) || sameFileAny(fi, seenInfos) {
		_ = f.Close()
		return openExclusive()
	}
	return f, fi, nil
}

func verifyOpenedArtifactFile(root *os.Root, relPath, relSlashPath string, f *os.File) (os.FileInfo, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: opened artifact %q is not a regular file", ErrInvalidManifest, relSlashPath)
	}
	lfi, err := root.Lstat(relPath)
	if err != nil {
		return nil, fmt.Errorf("modeldist: verify artifact %q: %w", relSlashPath, err)
	}
	if lfi.Mode()&os.ModeSymlink != 0 || !os.SameFile(fi, lfi) {
		return nil, fmt.Errorf("%w: artifact %q resolved through symlink", ErrInvalidManifest, relSlashPath)
	}
	return fi, nil
}

func (c *Client) finalizeReport(
	report *PullReport,
	start time.Time,
	pool *providerPool,
	ttftNs, downloaded, resumed, verifiedBytes *atomic.Int64,
	chunkLatencies []time.Duration,
) {
	elapsed := time.Since(start)
	rejected, originHits, mirrorHits, served, quarantined := pool.snapshot()
	report.DownloadedChunks = int(downloaded.Load())
	report.ResumedChunks = int(resumed.Load())
	report.VerifiedBytes = verifiedBytes.Load()
	report.RejectedChunks = rejected
	report.QuarantinedProviders = quarantined
	report.ProviderChunkCounts = served
	report.OriginChunksServed = originHits
	report.MirrorChunksServed = mirrorHits
	report.TTFTMs = float64(ttftNs.Load()) / float64(time.Millisecond)
	report.ElapsedMs = float64(elapsed.Nanoseconds()) / float64(time.Millisecond)
	if elapsed > 0 {
		report.ThroughputMBps = (float64(report.VerifiedBytes) / (1024 * 1024)) / elapsed.Seconds()
	}
	report.ChunkLatencyP50Ms, report.ChunkLatencyP99Ms = latencyPercentilesMs(chunkLatencies)
}

func (c *Client) checkpointPath(manifestCID string, artifactIdx int) string {
	if c.cfg.CheckpointDir == "" {
		return ""
	}
	return filepath.Join(c.cfg.CheckpointDir, fmt.Sprintf("%s_art%d.ckpt.json", manifestCID, artifactIdx))
}

func loadCheckpoint(path, manifestCID, artPath string, totalChunks int) *CheckpointState {
	empty := &CheckpointState{
		ManifestCID:    manifestCID,
		ArtifactPath:   artPath,
		TotalChunks:    totalChunks,
		VerifiedChunks: make(map[int]string),
	}
	if path == "" {
		return empty
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return empty
	}
	var st CheckpointState
	if err := json.Unmarshal(raw, &st); err != nil {
		return empty
	}
	if st.ManifestCID != manifestCID || st.ArtifactPath != artPath || st.TotalChunks != totalChunks {
		return empty
	}
	if st.VerifiedChunks == nil {
		st.VerifiedChunks = make(map[int]string)
	}
	return &st
}

func saveCheckpoint(path string, st *CheckpointState) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return atomicWriteFile(path, raw)
}

func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".modeldist-tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

func hashFileDual(f *os.File) (blake3Hex, sha256Hex string, err error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", "", err
	}
	b3 := blake3.New()
	s2 := sha256.New()
	mw := io.MultiWriter(b3, s2)
	if _, err := io.Copy(mw, f); err != nil {
		return "", "", err
	}
	return hex.EncodeToString(b3.Sum(nil)), hex.EncodeToString(s2.Sum(nil)), nil
}

func latencyPercentilesMs(durs []time.Duration) (p50, p99 float64) {
	if len(durs) == 0 {
		return 0, 0
	}
	cp := append([]time.Duration(nil), durs...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	p50Idx := (len(cp) - 1) * 50 / 100
	p99Idx := (len(cp) - 1) * 99 / 100
	return float64(cp[p50Idx].Nanoseconds()) / float64(time.Millisecond),
		float64(cp[p99Idx].Nanoseconds()) / float64(time.Millisecond)
}

// UnsafeUnverifiedMirrorPullForNegativeControl deliberately omits chunk and root
// cryptographic verification to measure the negative baseline (silent weight
// corruption rate when pulling from an untrusted mirror without hash checks).
// Never use this in production; always use Client.
func UnsafeUnverifiedMirrorPullForNegativeControl(ctx context.Context, m *ManifestV1, provider ChunkProvider) (corruptedChunks int, totalChunks int, rootMatches bool, err error) {
	if len(m.Artifacts) == 0 {
		return 0, 0, false, fmt.Errorf("%w: empty manifest", ErrInvalidManifest)
	}
	art := m.Artifacts[0]
	rootB3 := blake3.New()
	for _, ref := range art.Chunks {
		data, err := provider.GetChunk(ctx, ref.BLAKE3, ref.Size)
		if err != nil {
			return corruptedChunks, totalChunks, false, err
		}
		totalChunks++
		// Intentionally accept bytes without verifying, then inspect post-hoc
		// to measure how many silently corrupted chunks would be written.
		if VerifyChunkBytes(ref, data) != nil {
			corruptedChunks++
		}
		_, _ = rootB3.Write(data)
	}
	gotRoot := hex.EncodeToString(rootB3.Sum(nil))
	return corruptedChunks, totalChunks, gotRoot == strings.ToLower(art.RootBLAKE3), nil
}
