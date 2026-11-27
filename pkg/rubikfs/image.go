// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rubikfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

const manifestVersion uint32 = 1

// manifestEnvelope wraps the JSON manifest with a BLAKE3 integrity checksum
// prior to AEAD sealing.
type manifestEnvelope struct {
	Magic    string   `json:"magic"`
	Digest   [32]byte `json:"digest"`
	Manifest Manifest `json:"manifest"`
}

// BuildImage chunks files strictly within cfg.Scope, rearranges chunks according
// to cfg.Policy, compresses each pack with cfg.Codec, seals every pack and the
// image manifest with c under cfg.Scope, and writes all objects to be.
//
// BuildImage guarantees:
//   - Zero cross-volume / cross-tenant plaintext deduplication or clustering.
//   - Every pack is authenticated with per-volume AEAD and BLAKE3 digests.
//   - Bit-exact reconstruction of every input file (including empty files).
func BuildImage(ctx context.Context, be backend.Backend, c vfs.Cipher, cfg Config, files []FileInput) (*Manifest, BuildStats, error) {
	totalStart := time.Now()
	if err := cfg.Validate(); err != nil {
		return nil, BuildStats{}, err
	}
	if be == nil || c == nil {
		return nil, BuildStats{}, fmt.Errorf("%w: backend and cipher are required", ErrInvalidConfig)
	}

	// Validate file inputs and enforce strict single-scope isolation before touching any bytes.
	seenPaths := make(map[string]struct{}, len(files))
	sortedFiles := make([]FileInput, len(files))
	copy(sortedFiles, files)
	sort.Slice(sortedFiles, func(i, j int) bool {
		return sortedFiles[i].Path < sortedFiles[j].Path
	})
	for i, f := range sortedFiles {
		if f.Path == "" {
			return nil, BuildStats{}, fmt.Errorf("%w: file %d has empty path", ErrInvalidConfig, i)
		}
		if _, dup := seenPaths[f.Path]; dup {
			return nil, BuildStats{}, fmt.Errorf("%w: duplicate file path %q", ErrInvalidConfig, f.Path)
		}
		seenPaths[f.Path] = struct{}{}
		if f.Scope != (Scope{}) && f.Scope != cfg.Scope {
			return nil, BuildStats{}, fmt.Errorf("%w: file %q belongs to (%s,%s), want (%s,%s)",
				ErrTenantIsolation, f.Path, f.Scope.TenantID, f.Scope.VolumeID,
				cfg.Scope.TenantID, cfg.Scope.VolumeID)
		}
	}

	// 1. Fixed-size chunking + per-chunk BLAKE3 digest + similarity sketching.
	sketchStart := time.Now()
	var chunks []ChunkInput
	type fileChunkRef struct {
		chunkIdx int
		chunkID  ChunkID
	}
	fileRefs := make([][]fileChunkRef, len(sortedFiles))
	fileDigests := make([][32]byte, len(sortedFiles))
	var uncompressedTotal int64
	intraDedupIndex := make(map[ChunkID]int)

	for fi, f := range sortedFiles {
		fileDigests[fi] = blake3.Sum(f.Data)
		uncompressedTotal += int64(len(f.Data))
		if len(f.Data) == 0 {
			continue
		}
		for off := 0; off < len(f.Data); off += cfg.ChunkSize {
			end := off + cfg.ChunkSize
			if end > len(f.Data) {
				end = len(f.Data)
			}
			slice := f.Data[off:end]
			cid := ChunkID(blake3.Sum(slice))

			if cfg.IntraVolumeDedup {
				if existingIdx, ok := intraDedupIndex[cid]; ok {
					// If any referencing file is boot-hot, promote the intra-volume chunk.
					if f.BootHot {
						chunks[existingIdx].BootHot = true
					}
					fileRefs[fi] = append(fileRefs[fi], fileChunkRef{chunkIdx: existingIdx, chunkID: cid})
					continue
				}
			}

			idx := len(chunks)
			var sk ChunkSketch
			if cfg.Policy == PolicyPureSimilarity || cfg.Policy == PolicyLocalityHybrid {
				sk = ComputeSketch(slice)
			}
			chunks = append(chunks, ChunkInput{
				Index:   idx,
				Scope:   cfg.Scope,
				Path:    f.Path,
				FileOff: int64(off),
				Data:    slice,
				ID:      cid,
				BootHot: f.BootHot,
				Sketch:  sk,
			})
			if cfg.IntraVolumeDedup {
				intraDedupIndex[cid] = idx
			}
			fileRefs[fi] = append(fileRefs[fi], fileChunkRef{chunkIdx: idx, chunkID: cid})
		}
	}
	sketchDur := time.Since(sketchStart)

	// 2. Plan packs according to policy.
	clusterStart := time.Now()
	planned, meanAdjSim, err := PlanPacks(cfg, chunks)
	if err != nil {
		return nil, BuildStats{}, err
	}
	clusterDur := time.Since(clusterStart)

	// 3. Compress, frame, seal, and upload each pack.
	chunkLocs := make([]ChunkLocation, len(chunks))
	packsMeta := make([]PackMeta, len(planned))

	var compressDur, sealPutDur time.Duration
	var totalCompressedBytes, totalSealedPackBytes int64
	var bootPackCount int

	for pi, plan := range planned {
		if plan.Role == PackRoleBoot {
			bootPackCount++
		}
		var pid PackID
		if _, err := rand.Read(pid[:]); err != nil {
			return nil, BuildStats{}, fmt.Errorf("rubikfs: generate pack id: %w", err)
		}

		// Assemble contiguous pack payload in the planned similarity order.
		rawBuf := make([]byte, 0, plan.RawBytes)
		entries := make([]packChunkEntry, len(plan.ChunkIndices))
		var entSum float64
		for j, ci := range plan.ChunkIndices {
			ch := chunks[ci]
			off := uint32(len(rawBuf))
			n := uint32(len(ch.Data))
			rawBuf = append(rawBuf, ch.Data...)
			entries[j] = packChunkEntry{ID: ch.ID, Off: off, Len: n}
			chunkLocs[ci] = ChunkLocation{
				ChunkID:   ch.ID,
				PackIndex: pi,
				PackOff:   off,
				Length:    n,
			}
			entSum += ch.Sketch.Entropy
		}
		meanEnt := 0.0
		if len(plan.ChunkIndices) > 0 {
			meanEnt = entSum / float64(len(plan.ChunkIndices))
		}

		compStart := time.Now()
		compressed, err := Compress(cfg.Codec, rawBuf)
		if err != nil {
			return nil, BuildStats{}, fmt.Errorf("rubikfs: compress pack %d: %w", pi, err)
		}
		compressDur += time.Since(compStart)

		plainPack := encodePackPlaintext(cfg.Codec, plan.Role, entries, uint32(len(rawBuf)), compressed)
		plainDigest := blake3.Sum(plainPack)

		sealStart := time.Now()
		aad := packAAD(cfg.Scope, cfg.ImageID, pid)
		sealed, err := sealFramed(c, aad, plainPack)
		if err != nil {
			return nil, BuildStats{}, fmt.Errorf("rubikfs: seal pack %d: %w", pi, err)
		}
		bkey := packBackendKey(cfg.Scope, pid)
		if err := be.Put(ctx, bkey, sealed); err != nil {
			return nil, BuildStats{}, fmt.Errorf("rubikfs: put pack %d: %w", pi, err)
		}
		sealPutDur += time.Since(sealStart)

		totalCompressedBytes += int64(len(compressed))
		totalSealedPackBytes += int64(len(sealed))
		packsMeta[pi] = PackMeta{
			Index:           pi,
			ID:              pid,
			BackendKey:      bkey,
			Role:            plan.Role,
			Codec:           cfg.Codec,
			ChunkCount:      len(entries),
			RawBytes:        int64(len(rawBuf)),
			CompressedBytes: int64(len(compressed)),
			SealedBytes:     int64(len(sealed)),
			PlainDigest:     plainDigest,
			MeanEntropy:     meanEnt,
		}
	}

	// 4. Construct, checksum, seal, and upload Manifest.
	fileManifests := make([]FileManifest, len(sortedFiles))
	totalLogicalChunks := 0
	for fi, f := range sortedFiles {
		locs := make([]ChunkLocation, len(fileRefs[fi]))
		for j, ref := range fileRefs[fi] {
			locs[j] = chunkLocs[ref.chunkIdx]
		}
		totalLogicalChunks += len(locs)
		fm := FileManifest{
			Size:    int64(len(f.Data)),
			Digest:  fileDigests[fi],
			BootHot: f.BootHot,
			Chunks:  locs,
		}
		if utf8.ValidString(f.Path) {
			fm.Path = f.Path
		} else {
			fm.RawPath = []byte(f.Path)
		}
		fileManifests[fi] = fm
	}

	manifest := &Manifest{
		Version:           manifestVersion,
		Scope:             cfg.Scope,
		ImageID:           cfg.ImageID,
		Policy:            cfg.Policy,
		Codec:             cfg.Codec,
		ChunkSize:         cfg.ChunkSize,
		TargetPackBytes:   cfg.TargetPackBytes,
		BootPackBytes:     cfg.BootPackBytes,
		TotalFiles:        len(fileManifests),
		TotalChunks:       totalLogicalChunks,
		UniqueChunks:      len(chunks),
		UncompressedBytes: uncompressedTotal,
		PackedSealedBytes: totalSealedPackBytes,
		Packs:             packsMeta,
		Files:             fileManifests,
	}

	sealStart := time.Now()
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return nil, BuildStats{}, fmt.Errorf("rubikfs: marshal manifest: %w", err)
	}
	env := manifestEnvelope{
		Magic:    manifestMagic,
		Digest:   blake3.Sum(manifestJSON),
		Manifest: *manifest,
	}
	envBytes, err := json.Marshal(&env)
	if err != nil {
		return nil, BuildStats{}, fmt.Errorf("rubikfs: marshal manifest envelope: %w", err)
	}
	sealedManifest, err := sealFramed(c, manifestAAD(cfg.Scope, cfg.ImageID), envBytes)
	if err != nil {
		return nil, BuildStats{}, fmt.Errorf("rubikfs: seal manifest: %w", err)
	}
	if int64(len(sealedManifest)) > MaxManifestBytes {
		return nil, BuildStats{}, fmt.Errorf("rubikfs: sealed manifest size %d exceeds max %d: %w",
			len(sealedManifest), MaxManifestBytes, backend.ErrTooLarge)
	}
	mkey := manifestBackendKey(cfg.Scope, cfg.ImageID)
	if err := be.Put(ctx, mkey, sealedManifest); err != nil {
		return nil, BuildStats{}, fmt.Errorf("rubikfs: put manifest: %w", err)
	}
	sealPutDur += time.Since(sealStart)

	for i := range manifest.Files {
		if len(manifest.Files[i].RawPath) > 0 {
			manifest.Files[i].Path = string(manifest.Files[i].RawPath)
		}
	}
	manifest.ManifestBytes = int64(len(sealedManifest))
	totalStored := totalSealedPackBytes + int64(len(sealedManifest))

	compRatio := 1.0
	if totalCompressedBytes > 0 {
		compRatio = float64(uncompressedTotal) / float64(totalCompressedBytes)
	}
	storedRatio := 1.0
	if totalStored > 0 {
		storedRatio = float64(uncompressedTotal) / float64(totalStored)
	}

	totalBuildDur := time.Since(totalStart)
	if meanAdjSim == 0 && len(chunks) > 1 {
		meanAdjSim = computeMeanAdjacentSimilarityFull(planned, chunks)
	}

	stats := BuildStats{
		Policy:            cfg.Policy,
		Codec:             cfg.Codec,
		ChunkSize:         cfg.ChunkSize,
		TargetPackBytes:   cfg.TargetPackBytes,
		TotalFiles:        len(fileManifests),
		TotalChunks:       totalLogicalChunks,
		PackCount:         len(packsMeta),
		BootPackCount:     bootPackCount,
		UncompressedBytes: uncompressedTotal,
		CompressedBytes:   totalCompressedBytes,
		SealedPackBytes:   totalSealedPackBytes,
		ManifestBytes:     int64(len(sealedManifest)),
		TotalStoredBytes:  totalStored,
		CompressionRatio:  compRatio,
		StoredRatio:       storedRatio,
		SketchDuration:    sketchDur,
		ClusterDuration:   clusterDur,
		CompressDuration:  compressDur,
		SealPutDuration:   sealPutDur,
		TotalBuildTime:    totalBuildDur,
		MeanAdjacentSim:   meanAdjSim,
	}
	return manifest, stats, nil
}

// ManifestKey returns the opaque backend object key for (scope, imageID).
func ManifestKey(scope Scope, imageID string) string {
	return manifestBackendKey(scope, imageID)
}

type packFetchCall struct {
	done chan struct{}
	data []byte
	err  error
}

// Reader provides concurrency-safe, integrity-verified reads over a sealed
// RubikFS immutable image within a single (tenant_id, volume_id) scope.
type Reader struct {
	be       backend.Backend
	c        vfs.Cipher
	scope    Scope
	manifest *Manifest
	byPath   map[string]*FileManifest

	mu        sync.Mutex
	packCache map[int][]byte
	inflight  map[int]*packFetchCall
	stats     ReadStats
}

// OpenImage fetches, unseals, and verifies the manifest for (scope, imageID).
func OpenImage(ctx context.Context, be backend.Backend, c vfs.Cipher, scope Scope, imageID string) (*Reader, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if imageID == "" || be == nil || c == nil {
		return nil, fmt.Errorf("%w: backend, cipher, and image_id are required", ErrInvalidConfig)
	}

	mkey := manifestBackendKey(scope, imageID)
	sealed, _, err := be.Get(ctx, mkey, MaxManifestBytes)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("%w: manifest %q", ErrNotFound, imageID)
		}
		return nil, fmt.Errorf("rubikfs: get manifest: %w", err)
	}

	plain, err := unsealFramed(c, manifestAAD(scope, imageID), sealed)
	if err != nil {
		return nil, err
	}

	var env manifestEnvelope
	if err := json.Unmarshal(plain, &env); err != nil {
		return nil, fmt.Errorf("%w: decode manifest: %v", ErrCorruptPack, err)
	}
	if env.Magic != manifestMagic {
		return nil, fmt.Errorf("%w: bad manifest magic", ErrCorruptPack)
	}
	if env.Manifest.Scope != scope || env.Manifest.ImageID != imageID {
		return nil, fmt.Errorf("%w: manifest scope/image mismatch", ErrTenantIsolation)
	}
	// Verify BLAKE3 digest of the manifest payload before restoring RawPath into Path.
	recheck, err := json.Marshal(&env.Manifest)
	if err != nil {
		return nil, fmt.Errorf("%w: re-encode manifest: %v", ErrCorruptPack, err)
	}
	if blake3.Sum(recheck) != env.Digest {
		return nil, fmt.Errorf("%w: manifest digest mismatch", ErrCorruptPack)
	}

	m := &env.Manifest
	m.ManifestBytes = int64(len(sealed))
	byPath := make(map[string]*FileManifest, len(m.Files))
	for i := range m.Files {
		if len(m.Files[i].RawPath) > 0 {
			m.Files[i].Path = string(m.Files[i].RawPath)
		}
		byPath[m.Files[i].Path] = &m.Files[i]
	}
	return &Reader{
		be:        be,
		c:         c,
		scope:     scope,
		manifest:  m,
		byPath:    byPath,
		packCache: make(map[int][]byte),
		inflight:  make(map[int]*packFetchCall),
	}, nil
}

// Manifest returns the verified image manifest.
func (r *Reader) Manifest() *Manifest {
	return r.manifest
}

// ClearCache drops all cached decompressed packs so subsequent reads measure cold-start I/O.
func (r *Reader) ClearCache() {
	r.mu.Lock()
	clear(r.packCache)
	r.mu.Unlock()
}

// ResetStats zeroes all accumulated read metrics.
func (r *Reader) ResetStats() {
	r.mu.Lock()
	r.stats = ReadStats{}
	r.mu.Unlock()
}

// Stats returns a snapshot of accumulated read metrics, including computed read amplification.
func (r *Reader) Stats() ReadStats {
	r.mu.Lock()
	s := r.stats
	r.mu.Unlock()
	if s.UsefulBytes > 0 {
		s.ReadAmplification = float64(s.BackendBytesRead) / float64(s.UsefulBytes)
		s.RawReadAmp = float64(s.DecompressedBytes) / float64(s.UsefulBytes)
	}
	return s
}

// ReadFile reconstructs the complete plaintext of path and verifies every chunk
// and the whole-file BLAKE3 digest.
func (r *Reader) ReadFile(ctx context.Context, path string) ([]byte, error) {
	start := time.Now()
	fm, ok := r.byPath[path]
	if !ok {
		return nil, fmt.Errorf("%w: file %q", ErrNotFound, path)
	}
	if fm.Size == 0 {
		r.mu.Lock()
		r.stats.FilesRead++
		r.stats.Elapsed += time.Since(start)
		r.mu.Unlock()
		return []byte{}, nil
	}

	out := make([]byte, 0, fm.Size)
	for _, loc := range fm.Chunks {
		chunk, err := r.ReadChunk(ctx, loc)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
	}
	if int64(len(out)) != fm.Size {
		return nil, fmt.Errorf("%w: file %q length %d != expected %d", ErrCorruptPack, path, len(out), fm.Size)
	}
	if blake3.Sum(out) != fm.Digest {
		return nil, fmt.Errorf("%w: file %q digest mismatch", ErrCorruptPack, path)
	}

	r.mu.Lock()
	r.stats.FilesRead++
	r.stats.Elapsed += time.Since(start)
	r.mu.Unlock()
	return out, nil
}

// ReadChunk returns the BLAKE3-verified plaintext bytes of a single chunk.
func (r *Reader) ReadChunk(ctx context.Context, loc ChunkLocation) ([]byte, error) {
	if loc.PackIndex < 0 || loc.PackIndex >= len(r.manifest.Packs) {
		return nil, fmt.Errorf("%w: pack index %d out of range", ErrCorruptPack, loc.PackIndex)
	}
	packData, err := r.getPack(ctx, loc.PackIndex)
	if err != nil {
		return nil, err
	}
	end := uint64(loc.PackOff) + uint64(loc.Length)
	if end > uint64(len(packData)) {
		return nil, fmt.Errorf("%w: chunk offset %d+%d exceeds pack size %d", ErrCorruptPack, loc.PackOff, loc.Length, len(packData))
	}
	chunk := make([]byte, loc.Length)
	copy(chunk, packData[loc.PackOff:end])
	if ChunkID(blake3.Sum(chunk)) != loc.ChunkID {
		return nil, fmt.Errorf("%w: chunk %x BLAKE3 integrity failure", ErrCorruptPack, loc.ChunkID[:6])
	}

	r.mu.Lock()
	r.stats.ChunksRead++
	r.stats.UsefulBytes += int64(len(chunk))
	r.mu.Unlock()
	return chunk, nil
}

func (r *Reader) getPack(ctx context.Context, packIdx int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if data, ok := r.packCache[packIdx]; ok {
		r.stats.PackCacheHits++
		r.mu.Unlock()
		return data, nil
	}
	call, ok := r.inflight[packIdx]
	if !ok {
		call = &packFetchCall{done: make(chan struct{})}
		r.inflight[packIdx] = call
		// Detach shared single-flight fetch from any individual caller's context
		// so one caller canceling or timing out cannot abort live waiters.
		go func(c *packFetchCall) {
			fetchCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			data, fetchDur, unsealDur, sealedBytes, err := r.fetchAndDecodePack(fetchCtx, packIdx)

			r.mu.Lock()
			delete(r.inflight, packIdx)
			if err == nil {
				r.packCache[packIdx] = data
				r.stats.BackendGets++
				r.stats.PacksFetched++
				r.stats.BackendBytesRead += sealedBytes
				r.stats.DecompressedBytes += int64(len(data))
				r.stats.BackendFetchTime += fetchDur
				r.stats.UnsealDecompTime += unsealDur
			}
			c.data = data
			c.err = err
			close(c.done)
			r.mu.Unlock()
		}(call)
	}
	r.mu.Unlock()

	select {
	case <-call.done:
		return call.data, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Reader) fetchAndDecodePack(ctx context.Context, packIdx int) ([]byte, time.Duration, time.Duration, int64, error) {
	meta := r.manifest.Packs[packIdx]
	if meta.SealedBytes <= 0 || meta.SealedBytes > MaxSealedPackBytes {
		return nil, 0, 0, 0, fmt.Errorf("%w: pack %d sealed size %d out of bounds [1, %d]",
			ErrCorruptPack, packIdx, meta.SealedBytes, MaxSealedPackBytes)
	}
	fetchStart := time.Now()
	sealed, _, err := r.be.Get(ctx, meta.BackendKey, meta.SealedBytes)
	fetchDur := time.Since(fetchStart)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fetchDur, 0, 0, fmt.Errorf("%w: pack %d missing", ErrCorruptPack, packIdx)
		}
		return nil, fetchDur, 0, 0, fmt.Errorf("rubikfs: get pack %d: %w", packIdx, err)
	}
	if int64(len(sealed)) != meta.SealedBytes {
		return nil, fetchDur, 0, int64(len(sealed)), fmt.Errorf("%w: pack %d sealed length %d != manifest %d",
			ErrCorruptPack, packIdx, len(sealed), meta.SealedBytes)
	}

	unsealStart := time.Now()
	plainPack, err := unsealFramed(r.c, packAAD(r.scope, r.manifest.ImageID, meta.ID), sealed)
	if err != nil {
		return nil, fetchDur, time.Since(unsealStart), int64(len(sealed)), err
	}
	if blake3.Sum(plainPack) != meta.PlainDigest {
		return nil, fetchDur, time.Since(unsealStart), int64(len(sealed)), fmt.Errorf("%w: pack %d plaintext BLAKE3 mismatch", ErrCorruptPack, packIdx)
	}
	dp, err := decodePackPlaintext(plainPack)
	if err != nil {
		return nil, fetchDur, time.Since(unsealStart), int64(len(sealed)), err
	}
	if int64(dp.RawBytes) != meta.RawBytes || dp.Codec != meta.Codec {
		return nil, fetchDur, time.Since(unsealStart), int64(len(sealed)), fmt.Errorf("%w: pack %d metadata mismatch", ErrCorruptPack, packIdx)
	}
	raw, err := Decompress(dp.Codec, int(dp.RawBytes), dp.Compressed)
	if err != nil {
		return nil, fetchDur, time.Since(unsealStart), int64(len(sealed)), err
	}
	// Verify every chunk entry's BLAKE3 digest in the pack header matches the decompressed bytes.
	for i, e := range dp.Entries {
		if ChunkID(blake3.Sum(raw[e.Off:e.Off+e.Len])) != e.ID {
			return nil, fetchDur, time.Since(unsealStart), int64(len(sealed)), fmt.Errorf("%w: pack %d chunk %d digest mismatch", ErrCorruptPack, packIdx, i)
		}
	}
	unsealDur := time.Since(unsealStart)
	return raw, fetchDur, unsealDur, int64(len(sealed)), nil
}

// ReplayColdStart clears the pack cache, resets stats, reads every path in bootPaths,
// and returns the exact measured cold-start ReadStats.
func (r *Reader) ReplayColdStart(ctx context.Context, bootPaths []string) (ReadStats, error) {
	r.ClearCache()
	r.ResetStats()
	start := time.Now()
	for _, p := range bootPaths {
		if _, err := r.ReadFile(ctx, p); err != nil {
			return ReadStats{}, err
		}
	}
	s := r.Stats()
	s.Elapsed = time.Since(start)
	return s, nil
}

// VerifyAll reads and verifies every file in the manifest against expected (if provided).
func (r *Reader) VerifyAll(ctx context.Context, expected map[string][]byte) error {
	for _, fm := range r.manifest.Files {
		got, err := r.ReadFile(ctx, fm.Path)
		if err != nil {
			return fmt.Errorf("verify %q: %w", fm.Path, err)
		}
		if expected != nil {
			want, ok := expected[fm.Path]
			if !ok {
				return fmt.Errorf("verify %q: unexpected file in manifest", fm.Path)
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("verify %q: byte content mismatch", fm.Path)
			}
		}
	}
	if expected != nil && len(r.manifest.Files) != len(expected) {
		return fmt.Errorf("verify: manifest has %d files, want %d", len(r.manifest.Files), len(expected))
	}
	return nil
}
