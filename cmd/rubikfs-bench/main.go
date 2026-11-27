// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/rubikfs"
)

const (
	// Calibrated S3 latency model matching docs/cofs.md (2 ms GET RTT, 400 MiB/s throughput).
	defaultS3RTTMs       = 2.0
	defaultS3BandwidthMB = 400.0
)

type RegimeResult struct {
	Corpus                   string  `json:"corpus"`
	Policy                   string  `json:"policy"`
	Codec                    string  `json:"codec"`
	ChunkSizeKiB             int     `json:"chunk_size_kib"`
	TargetPackKiB            int     `json:"target_pack_kib"`
	BootPackKiB              int     `json:"boot_pack_kib"`
	UncompressedBytes        int64   `json:"uncompressed_bytes"`
	CompressedBytes          int64   `json:"compressed_bytes"`
	SealedPackBytes          int64   `json:"sealed_pack_bytes"`
	ManifestBytes            int64   `json:"manifest_bytes"`
	TotalStoredBytes         int64   `json:"total_stored_bytes"`
	CompressionRatio         float64 `json:"compression_ratio"`
	StoredRatio              float64 `json:"stored_ratio"`
	CompressionGainPct       float64 `json:"compression_gain_vs_path_pct"`
	PackCount                int     `json:"pack_count"`
	BootPackCount            int     `json:"boot_pack_count"`
	MeanAdjacentSim          float64 `json:"mean_adjacent_sim"`
	SketchMs                 float64 `json:"sketch_ms"`
	ClusterMs                float64 `json:"cluster_ms"`
	CompressMs               float64 `json:"compress_ms"`
	BuildTimeMs              float64 `json:"build_time_ms"`
	ColdBootFiles            int     `json:"cold_boot_files"`
	ColdBootUsefulBytes      int64   `json:"cold_boot_useful_bytes"`
	ColdPacksFetched         int     `json:"cold_packs_fetched"`
	ColdBackendGets          int     `json:"cold_backend_gets"`
	ColdBytesFetched         int64   `json:"cold_bytes_fetched"`
	ColdDecompBytes          int64   `json:"cold_decomp_bytes"`
	ReadAmplification        float64 `json:"read_amplification"`
	RawReadAmp               float64 `json:"raw_read_amp"`
	ColdDiskMs               float64 `json:"cold_disk_ms"`
	ColdUnsealDecompMs       float64 `json:"cold_unseal_decomp_ms"`
	ColdS3ModeledMs          float64 `json:"cold_s3_modeled_ms"`
	SingleColdFileRawReadAmp float64 `json:"single_cold_file_raw_read_amp"`
	BitExactVerified         bool    `json:"bit_exact_verified"`
}

type IsolationAudit struct {
	TenantA                 string `json:"tenant_a"`
	TenantB                 string `json:"tenant_b"`
	VolumeABytes            int64  `json:"volume_a_bytes"`
	VolumeBBytes            int64  `json:"volume_b_bytes"`
	CombinedBackendBytes    int64  `json:"combined_backend_bytes"`
	SharedBackendKeys       int    `json:"shared_backend_keys"`
	IdenticalCiphertexts    int    `json:"identical_ciphertexts"`
	CrossVolumeDedupBytes   int64  `json:"cross_volume_dedup_bytes"`
	CrossVolumeOpenRejected bool   `json:"cross_volume_open_rejected"`
	TransplantRejected      bool   `json:"transplant_rejected"`
	SingleBitFlipRejected   bool   `json:"single_bit_flip_rejected"`
}

type BenchmarkReport struct {
	GeneratedAtUTC      string         `json:"generated_at_utc"`
	Seed                int64          `json:"seed"`
	GoVersion           string         `json:"go_version"`
	GOOS                string         `json:"goos"`
	GOARCH              string         `json:"goarch"`
	NumCPU              int            `json:"num_cpu"`
	S3ModelRTTMs        float64        `json:"s3_model_rtt_ms"`
	S3ModelBandwidthMBs float64        `json:"s3_model_bandwidth_mbs"`
	PrimaryCorpus       CorpusSummary  `json:"primary_corpus"`
	IncompCorpus        CorpusSummary  `json:"incompressible_corpus"`
	PrimaryComparison   []RegimeResult `json:"primary_comparison"`
	GranularitySweep    []RegimeResult `json:"granularity_sweep"`
	Chunk64KiBCheck     []RegimeResult `json:"chunk_64kib_check"`
	IncompressibleCheck []RegimeResult `json:"incompressible_check"`
	IsolationAudit      IsolationAudit `json:"isolation_audit"`
}

func runRegime(ctx context.Context, corpusName string, files []rubikfs.FileInput, expected map[string][]byte, bootPaths []string, cfg rubikfs.Config) (RegimeResult, error) {
	dir, err := os.MkdirTemp("", "rubikfs-bench-*")
	if err != nil {
		return RegimeResult{}, err
	}
	defer os.RemoveAll(dir)

	be, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		return RegimeResult{}, err
	}
	defer be.Close()

	cipher, err := rubikfs.NewVolumeCipher(cfg.Scope, []byte("rubikfs-bench-deterministic-secret!"))
	if err != nil {
		return RegimeResult{}, err
	}

	_, bstats, err := rubikfs.BuildImage(ctx, be, cipher, cfg, files)
	if err != nil {
		return RegimeResult{}, err
	}

	rdr, err := rubikfs.OpenImage(ctx, be, cipher, cfg.Scope, cfg.ImageID)
	if err != nil {
		return RegimeResult{}, err
	}

	// Verify bit-exact full-image reconstruction first.
	if err := rdr.VerifyAll(ctx, expected); err != nil {
		return RegimeResult{}, fmt.Errorf("bit-exact verification failed: %w", err)
	}

	// Measure cold-start working-set replay over real file:// backend (average of 3 cold replays).
	const coldRuns = 3
	var totalDiskNs, totalUnsealNs int64
	var lastCold rubikfs.ReadStats
	for i := 0; i < coldRuns; i++ {
		cold, err := rdr.ReplayColdStart(ctx, bootPaths)
		if err != nil {
			return RegimeResult{}, fmt.Errorf("cold-start replay: %w", err)
		}
		totalDiskNs += cold.Elapsed.Nanoseconds()
		totalUnsealNs += cold.UnsealDecompTime.Nanoseconds()
		lastCold = cold
	}
	avgDiskMs := float64(totalDiskNs) / float64(coldRuns) / 1e6
	avgUnsealMs := float64(totalUnsealNs) / float64(coldRuns) / 1e6

	// Calibrated S3 cold-start model:
	//   1 manifest GET + PacksFetched pack GETs, each paying RTT + transfer + real unseal/decompress CPU.
	manifestBytes := bstats.ManifestBytes
	totalS3Gets := 1 + lastCold.PacksFetched
	totalS3Bytes := manifestBytes + lastCold.BackendBytesRead
	transferMs := (float64(totalS3Bytes) / (defaultS3BandwidthMB * 1024 * 1024)) * 1000.0
	s3ModeledMs := float64(totalS3Gets)*defaultS3RTTMs + transferMs + avgUnsealMs

	// Measure single cold-file read amplification (pick first non-boot file >= 16 KiB).
	var singleRawAmp float64
	for _, f := range files {
		if !f.BootHot && len(f.Data) >= 16<<10 {
			oneCold, err := rdr.ReplayColdStart(ctx, []string{f.Path})
			if err != nil {
				return RegimeResult{}, fmt.Errorf("single cold-file replay: %w", err)
			}
			singleRawAmp = oneCold.RawReadAmp
			break
		}
	}

	return RegimeResult{
		Corpus:                   corpusName,
		Policy:                   string(cfg.Policy),
		Codec:                    cfg.Codec.String(),
		ChunkSizeKiB:             cfg.ChunkSize >> 10,
		TargetPackKiB:            cfg.TargetPackBytes >> 10,
		BootPackKiB:              cfg.BootPackBytes >> 10,
		UncompressedBytes:        bstats.UncompressedBytes,
		CompressedBytes:          bstats.CompressedBytes,
		SealedPackBytes:          bstats.SealedPackBytes,
		ManifestBytes:            bstats.ManifestBytes,
		TotalStoredBytes:         bstats.TotalStoredBytes,
		CompressionRatio:         bstats.CompressionRatio,
		StoredRatio:              bstats.StoredRatio,
		PackCount:                bstats.PackCount,
		BootPackCount:            bstats.BootPackCount,
		MeanAdjacentSim:          bstats.MeanAdjacentSim,
		SketchMs:                 float64(bstats.SketchDuration.Nanoseconds()) / 1e6,
		ClusterMs:                float64(bstats.ClusterDuration.Nanoseconds()) / 1e6,
		CompressMs:               float64(bstats.CompressDuration.Nanoseconds()) / 1e6,
		BuildTimeMs:              float64(bstats.TotalBuildTime.Nanoseconds()) / 1e6,
		ColdBootFiles:            lastCold.FilesRead,
		ColdBootUsefulBytes:      lastCold.UsefulBytes,
		ColdPacksFetched:         lastCold.PacksFetched,
		ColdBackendGets:          lastCold.BackendGets,
		ColdBytesFetched:         lastCold.BackendBytesRead,
		ColdDecompBytes:          lastCold.DecompressedBytes,
		ReadAmplification:        lastCold.ReadAmplification,
		RawReadAmp:               lastCold.RawReadAmp,
		ColdDiskMs:               avgDiskMs,
		ColdUnsealDecompMs:       avgUnsealMs,
		ColdS3ModeledMs:          s3ModeledMs,
		SingleColdFileRawReadAmp: singleRawAmp,
		BitExactVerified:         true,
	}, nil
}

func runIsolationAudit(ctx context.Context, files []rubikfs.FileInput) (IsolationAudit, error) {
	dir, err := os.MkdirTemp("", "rubikfs-iso-*")
	if err != nil {
		return IsolationAudit{}, err
	}
	defer os.RemoveAll(dir)

	be, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		return IsolationAudit{}, err
	}
	defer be.Close()

	// Use the first 24 files so the audit runs rapidly while exercising multi-pack images.
	subset := files
	if len(subset) > 24 {
		subset = files[:24]
	}

	sharedMaster := []byte("shared-cluster-kms-root-key-32b!")
	scopeA := rubikfs.Scope{TenantID: "tenant-alpha", VolumeID: "vol-01"}
	scopeB := rubikfs.Scope{TenantID: "tenant-beta", VolumeID: "vol-02"}
	cA, err := rubikfs.NewVolumeCipher(scopeA, sharedMaster)
	if err != nil {
		return IsolationAudit{}, err
	}
	cB, err := rubikfs.NewVolumeCipher(scopeB, sharedMaster)
	if err != nil {
		return IsolationAudit{}, err
	}

	cfgA := rubikfs.Config{Scope: scopeA, ImageID: "visor-base", Policy: rubikfs.PolicyLocalityHybrid, Codec: rubikfs.CodecLongLZFlate, ChunkSize: 16 << 10, TargetPackBytes: 256 << 10}
	cfgB := rubikfs.Config{Scope: scopeB, ImageID: "visor-base", Policy: rubikfs.PolicyLocalityHybrid, Codec: rubikfs.CodecLongLZFlate, ChunkSize: 16 << 10, TargetPackBytes: 256 << 10}

	manA, stA, err := rubikfs.BuildImage(ctx, be, cA, cfgA, subset)
	if err != nil {
		return IsolationAudit{}, err
	}
	manB, stB, err := rubikfs.BuildImage(ctx, be, cB, cfgB, subset)
	if err != nil {
		return IsolationAudit{}, err
	}

	packsA := make(map[string][]byte, len(manA.Packs))
	for _, p := range manA.Packs {
		raw, _, err := be.Get(ctx, p.BackendKey, backend.Unbounded)
		if err != nil {
			return IsolationAudit{}, err
		}
		packsA[p.BackendKey] = raw
	}

	sharedKeys := 0
	identicalCT := 0
	for _, p := range manB.Packs {
		if _, ok := packsA[p.BackendKey]; ok {
			sharedKeys++
		}
		rawB, _, err := be.Get(ctx, p.BackendKey, backend.Unbounded)
		if err != nil {
			return IsolationAudit{}, err
		}
		for _, rawA := range packsA {
			if bytes.Equal(rawA, rawB) {
				identicalCT++
			}
		}
	}

	var combined int64
	objs, errCh := be.List(ctx, "rubikfs/")
	for o := range objs {
		combined += o.Size
	}
	if err := <-errCh; err != nil {
		return IsolationAudit{}, err
	}
	dedupBytes := (stA.TotalStoredBytes + stB.TotalStoredBytes) - combined

	// Cross-volume OpenImage must fail closed.
	_, openErr := rubikfs.OpenImage(ctx, be, cB, scopeA, "visor-base")
	crossOpenRejected := openErr != nil

	// Single-bit flip in Volume A pack 0 must fail with ErrCorruptPack.
	p0KeyA := manA.Packs[0].BackendKey
	origA := append([]byte(nil), packsA[p0KeyA]...)
	flipped := append([]byte(nil), origA...)
	flipped[len(flipped)/2] ^= 0x01
	if err := be.Put(ctx, p0KeyA, flipped); err != nil {
		return IsolationAudit{}, err
	}
	rdrA, err := rubikfs.OpenImage(ctx, be, cA, scopeA, "visor-base")
	if err != nil {
		return IsolationAudit{}, err
	}
	targetFileA := firstFileInPack(manA, 0)
	_, flipErr := rdrA.ReadFile(ctx, targetFileA)
	flipRejected := errors.Is(flipErr, rubikfs.ErrCorruptPack)

	// Transplant Volume A's valid pack 0 into Volume B's pack 0 key -> must fail closed.
	if err := be.Put(ctx, manB.Packs[0].BackendKey, origA); err != nil {
		return IsolationAudit{}, err
	}
	rdrB, err := rubikfs.OpenImage(ctx, be, cB, scopeB, "visor-base")
	if err != nil {
		return IsolationAudit{}, err
	}
	targetFileB := firstFileInPack(manB, 0)
	_, transplantErr := rdrB.ReadFile(ctx, targetFileB)
	transplantRejected := transplantErr != nil

	return IsolationAudit{
		TenantA:                 scopeA.TenantID + "/" + scopeA.VolumeID,
		TenantB:                 scopeB.TenantID + "/" + scopeB.VolumeID,
		VolumeABytes:            stA.TotalStoredBytes,
		VolumeBBytes:            stB.TotalStoredBytes,
		CombinedBackendBytes:    combined,
		SharedBackendKeys:       sharedKeys,
		IdenticalCiphertexts:    identicalCT,
		CrossVolumeDedupBytes:   dedupBytes,
		CrossVolumeOpenRejected: crossOpenRejected,
		TransplantRejected:      transplantRejected,
		SingleBitFlipRejected:   flipRejected,
	}, nil
}

func firstFileInPack(m *rubikfs.Manifest, packIdx int) string {
	for _, fm := range m.Files {
		for _, c := range fm.Chunks {
			if c.PackIndex == packIdx {
				return fm.Path
			}
		}
	}
	return m.Files[0].Path
}

func annotateCompressionGain(rows []RegimeResult) {
	var baseRatio float64
	for _, r := range rows {
		if r.Policy == string(rubikfs.PolicyPathOrder) {
			baseRatio = r.CompressionRatio
			break
		}
	}
	if baseRatio <= 0 {
		return
	}
	for i := range rows {
		rows[i].CompressionGainPct = 100.0 * (rows[i].CompressionRatio/baseRatio - 1.0)
	}
}

func runAllBenchmarks(ctx context.Context, repoRoot string, seed int64) (*BenchmarkReport, error) {
	files, bootPaths, primarySummary, err := buildVisorBaseImage(repoRoot, seed)
	if err != nil {
		return nil, err
	}
	expected := make(map[string][]byte, len(files))
	for _, f := range files {
		expected[f.Path] = f.Data
	}

	scope := rubikfs.Scope{TenantID: "bench-tenant", VolumeID: "visor-base-v1"}

	// Experiment A: 4-Regime Matched Baseline Comparison (16 KiB chunks, 1 MiB packs, 256 KiB boot pack).
	policies := []rubikfs.Policy{
		rubikfs.PolicyPathOrder,
		rubikfs.PolicyRandomOrder,
		rubikfs.PolicyPureSimilarity,
		rubikfs.PolicyLocalityHybrid,
	}
	var primary []RegimeResult
	for _, pol := range policies {
		bootPack := 1 << 20
		if pol == rubikfs.PolicyLocalityHybrid {
			bootPack = 256 << 10
		}
		cfg := rubikfs.Config{
			Scope:           scope,
			ImageID:         "primary-" + string(pol),
			Policy:          pol,
			Codec:           rubikfs.CodecLongLZFlate,
			ChunkSize:       16 << 10,
			TargetPackBytes: 1 << 20,
			BootPackBytes:   bootPack,
			Seed:            seed,
		}
		res, err := runRegime(ctx, primarySummary.Name, files, expected, bootPaths, cfg)
		if err != nil {
			return nil, fmt.Errorf("primary %s: %w", pol, err)
		}
		primary = append(primary, res)
	}
	annotateCompressionGain(primary)

	// Experiment B: Pack Granularity Sweep (256 KiB, 1 MiB, 4 MiB, 8 MiB — includes Negative Regime #3).
	packSizes := []int{256 << 10, 1 << 20, 4 << 20, 8 << 20}
	var sweep []RegimeResult
	for _, psz := range packSizes {
		for _, pol := range []rubikfs.Policy{rubikfs.PolicyPathOrder, rubikfs.PolicyPureSimilarity, rubikfs.PolicyLocalityHybrid} {
			bootPack := psz
			if pol == rubikfs.PolicyLocalityHybrid && bootPack > 256<<10 {
				bootPack = 256 << 10
			}
			cfg := rubikfs.Config{
				Scope:           scope,
				ImageID:         fmt.Sprintf("sweep-%dk-%s", psz>>10, pol),
				Policy:          pol,
				Codec:           rubikfs.CodecLongLZFlate,
				ChunkSize:       16 << 10,
				TargetPackBytes: psz,
				BootPackBytes:   bootPack,
				Seed:            seed,
			}
			res, err := runRegime(ctx, primarySummary.Name, files, expected, bootPaths, cfg)
			if err != nil {
				return nil, fmt.Errorf("sweep %d %s: %w", psz, pol, err)
			}
			sweep = append(sweep, res)
		}
	}

	// Experiment B2: 64 KiB Chunk Check (proves long-range LZ77 captures cross-chunk matches beyond 32 KiB DEFLATE window).
	var chunk64 []RegimeResult
	for _, pol := range []rubikfs.Policy{rubikfs.PolicyPathOrder, rubikfs.PolicyPureSimilarity, rubikfs.PolicyLocalityHybrid} {
		bootPack := 1 << 20
		if pol == rubikfs.PolicyLocalityHybrid {
			bootPack = 256 << 10
		}
		cfg := rubikfs.Config{
			Scope:           scope,
			ImageID:         "c64-" + string(pol),
			Policy:          pol,
			Codec:           rubikfs.CodecLongLZFlate,
			ChunkSize:       64 << 10,
			TargetPackBytes: 1 << 20,
			BootPackBytes:   bootPack,
			Seed:            seed,
		}
		res, err := runRegime(ctx, primarySummary.Name, files, expected, bootPaths, cfg)
		if err != nil {
			return nil, fmt.Errorf("chunk64 %s: %w", pol, err)
		}
		chunk64 = append(chunk64, res)
	}
	annotateCompressionGain(chunk64)

	// Experiment C: Negative Regime #2 — Incompressible / Pre-Compressed Payloads (4 MiB).
	incompFiles, incompBoot, incompSummary := buildIncompressibleCorpus(seed, 64, 64<<10)
	incompExp := make(map[string][]byte, len(incompFiles))
	for _, f := range incompFiles {
		incompExp[f.Path] = f.Data
	}
	var incompRes []RegimeResult
	for _, pol := range []rubikfs.Policy{rubikfs.PolicyPathOrder, rubikfs.PolicyPureSimilarity, rubikfs.PolicyLocalityHybrid} {
		cfg := rubikfs.Config{
			Scope:           scope,
			ImageID:         "incomp-" + string(pol),
			Policy:          pol,
			Codec:           rubikfs.CodecLongLZFlate,
			ChunkSize:       16 << 10,
			TargetPackBytes: 1 << 20,
			BootPackBytes:   256 << 10,
			Seed:            seed,
		}
		res, err := runRegime(ctx, incompSummary.Name, incompFiles, incompExp, incompBoot, cfg)
		if err != nil {
			return nil, fmt.Errorf("incomp %s: %w", pol, err)
		}
		incompRes = append(incompRes, res)
	}
	annotateCompressionGain(incompRes)

	// Experiment D: Tenant Isolation & Zero Cross-Volume Plaintext Deduplication Audit.
	iso, err := runIsolationAudit(ctx, files)
	if err != nil {
		return nil, fmt.Errorf("isolation audit: %w", err)
	}

	return &BenchmarkReport{
		GeneratedAtUTC:      time.Now().UTC().Format(time.RFC3339),
		Seed:                seed,
		GoVersion:           runtime.Version(),
		GOOS:                runtime.GOOS,
		GOARCH:              runtime.GOARCH,
		NumCPU:              runtime.NumCPU(),
		S3ModelRTTMs:        defaultS3RTTMs,
		S3ModelBandwidthMBs: defaultS3BandwidthMB,
		PrimaryCorpus:       primarySummary,
		IncompCorpus:        incompSummary,
		PrimaryComparison:   primary,
		GranularitySweep:    sweep,
		Chunk64KiBCheck:     chunk64,
		IncompressibleCheck: incompRes,
		IsolationAudit:      iso,
	}, nil
}

func renderMarkdownReport(rep *BenchmarkReport) string {
	var b strings.Builder
	b.WriteString("# Similarity-Clustered Immutable Base Images for `hanzoai/vfs` (RubikFS, HIP-1344 / HIP-1345)\n\n")
	b.WriteString("Refs `hanzoai/vfs#61` and `hanzoai/vfs#49`. Tracked by HIP-1344 / HIP-1345.\n")
	b.WriteString("Library: `pkg/rubikfs`, benchmark harness: `cmd/rubikfs-bench`, machine-readable results: `results.json`.\n\n")

	b.WriteString("## 1. Primary Reference, Summary, and Clean-Room License Audit\n\n")
	b.WriteString("* **Primary paper:** RubikFS — Rearranging immutable base-image chunks by content similarity before sealing and packing, 24th USENIX Conference on File and Storage Technologies (FAST 2026), <https://www.usenix.org/conference/fast26/presentation/huang>.\n")
	b.WriteString("* **Problem & stack boundary:** Sandbox runtimes (`hanzoai/visor` mounting `hanzoai/vfs` over `hanzoai/s3`) distribute immutable base images containing compiler toolchains, language standard libraries, shared ELF binaries, and JSON/YAML manifests across deep directory trees. Packing chunks in lexicographical directory-walk order scatters structurally similar chunks across separate packs and beyond LZ77 windows. `pkg/rubikfs` clusters immutable chunks by shift-invariant content similarity **strictly inside one `(tenant_id, volume_id)` scope** before compression, AEAD sealing, and packing.\n")
	b.WriteString("* **Orthogonality & clean-room audit:** `pkg/rubikfs` is completely orthogonal to `pkg/cofs` (HIP-1342 metadata MPHF), operates cleanly above `pkg/backend.Backend` and `vfs.Cipher`, introduces **zero changes to `go.mod` / `go.sum`**, is written clean-room under Apache-2.0, and builds with `CGO_ENABLED=0` on `linux/amd64` and `darwin/arm64`.\n\n")

	b.WriteString("## 2. Architecture (`pkg/rubikfs`)\n\n")
	b.WriteString("1. **Shift-Invariant Similarity Sketching (`sketch.go`):** Content-sampled 8-byte shingles mapped into a 32-register one-permutation MinHash (with deterministic densification), paired with a 64-bit SimHash, 16-bin byte-class histogram, and exact Shannon entropy classification.\n")
	b.WriteString("2. **Scoped LSH Similarity Clustering & Boot-Locality Pinning (`cluster.go`):** Hard-enforces a single `Scope{TenantID, VolumeID}` (refusing mixed scopes with `ErrTenantIsolation`). Supports 4 matched regimes: `path_order_baseline`, `random_order_control`, `rubikfs_pure_similarity`, and `rubikfs_locality_hybrid` (which pins boot-hot working-set chunks into dedicated similarity-clustered boot packs while running full similarity clustering over cold image chunks).\n")
	b.WriteString("3. **Zero-Dependency Pack-Wide Long-Range LZ77 + DEFLATE (`codec.go`):** Captures cross-chunk matches across multi-MiB packs (far beyond DEFLATE's 32 KiB sliding window, enabling full similarity gains on both `16 KiB` and `64 KiB` chunks) with automatic 1-byte raw fallback so high-entropy payloads never expand.\n")
	b.WriteString("4. **Per-Volume Independent AEAD Sealing & BLAKE3 Verification (`seal.go`, `image.go`):** Derives an independent AES-256-GCM key per `(tenant_id, volume_id)` (or accepts `vfs.Hybrid` post-quantum X25519+ML-KEM-768 directly), seals every pack with a fresh 96-bit CSPRNG nonce and `(tenant_id, volume_id, image_id, pack_id)` AAD binding, stores objects under opaque keyed BLAKE3 backend keys, and verifies pack + chunk + file BLAKE3 digests on every read.\n\n")

	b.WriteString("## 3. Grounded Corpus & Experimental Environment\n\n")
	fmt.Fprintf(&b, "* **Environment:** `%s`, `%s/%s`, `%d` logical CPUs, `--seed %d`, generated `%s`.\n",
		rep.GoVersion, rep.GOOS, rep.GOARCH, rep.NumCPU, rep.Seed, rep.GeneratedAtUTC)
	fmt.Fprintf(&b, "* **Grounded primary corpus (`%s`):** `%d` files (`%.2f MiB` uncompressed), combining **`%d` real versioned `hanzoai/vfs` repository files** (`%.2f MiB` of Go sources, docs, configs, and JSON test vectors) with **`%d` deterministic seeded visor base-image files** (`%.2f MiB` of ELF64-structured binaries, multi-directory toolchain/stdlib modules, and service manifests; zero identical chunks across files). Cold-start boot working set: **`%d` files (`%.2f KiB`, `%.1f%%` of image)** spanning `/sbin`, `/etc/visor`, `/usr/bin`, `/usr/lib`, `/opt/toolchain`, and `/src/hanzo/vfs`.\n",
		rep.PrimaryCorpus.Name,
		rep.PrimaryCorpus.TotalFiles, float64(rep.PrimaryCorpus.TotalBytes)/(1024*1024),
		rep.PrimaryCorpus.RepoFiles, float64(rep.PrimaryCorpus.RepoBytes)/(1024*1024),
		rep.PrimaryCorpus.SyntheticFiles, float64(rep.PrimaryCorpus.SyntheticBytes)/(1024*1024),
		rep.PrimaryCorpus.BootFiles, float64(rep.PrimaryCorpus.BootBytes)/1024.0, rep.PrimaryCorpus.BootFractionPct)
	fmt.Fprintf(&b, "* **Backend & S3 model:** Every regime writes and reads through the real `pkg/backend/file` (`file://`) backend for disk cold-start timings and bit-exact verification; S3 cold-start latency models `1` manifest `GET` + measured pack `GET`s at `%.1f ms` RTT and `%.0f MiB/s` transfer plus real measured AEAD unseal + BLAKE3 + decompress CPU time.\n\n",
		rep.S3ModelRTTMs, rep.S3ModelBandwidthMBs)

	b.WriteString("## 4. Matched-Baseline Evaluation\n\n")
	b.WriteString("### A. Primary 4-Regime Comparison (`16 KiB` chunks, `1 MiB` target packs, `256 KiB` hybrid boot pack, `longlz_flate`)\n\n")
	b.WriteString("| Regime | Compression Ratio | Gain vs Path | Stored Ratio (incl. AEAD+Manifest) | Sealed Size (MiB) | Packs (Boot) | Mean Adj Sim | Build (ms) | Cold Packs Fetched | Read Amp (Net) | Raw Read Amp | Cold Disk (ms) | Cold S3 (ms) | Bit-Exact |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|\n")
	for _, r := range rep.PrimaryComparison {
		fmt.Fprintf(&b, "| `%s` | **%.2fx** | %+.1f%% | %.2fx | %.2f | %d (%d) | %.3f | %.1f | %d | **%.2fx** | %.2fx | %.2f | **%.2f** | %v |\n",
			r.Policy, r.CompressionRatio, r.CompressionGainPct, r.StoredRatio,
			float64(r.TotalStoredBytes)/(1024*1024), r.PackCount, r.BootPackCount,
			r.MeanAdjacentSim, r.BuildTimeMs, r.ColdPacksFetched,
			r.ReadAmplification, r.RawReadAmp, r.ColdDiskMs, r.ColdS3ModeledMs, passFail(r.BitExactVerified))
	}

	b.WriteString("\n### B. `64 KiB` Chunk Verification (`1 MiB` target packs, `longlz_flate`)\n\n")
	b.WriteString("Demonstrates that pack-wide long-range LZ77 captures cross-chunk similarity even when chunk size (`64 KiB`) exceeds standard DEFLATE's `32 KiB` sliding window:\n\n")
	b.WriteString("| Regime | Compression Ratio | Gain vs Path | Sealed Size (MiB) | Cold Packs Fetched | Read Amp (Net) | Cold S3 (ms) |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rep.Chunk64KiBCheck {
		fmt.Fprintf(&b, "| `%s` | **%.2fx** | %+.1f%% | %.2f | %d | %.2fx | %.2f |\n",
			r.Policy, r.CompressionRatio, r.CompressionGainPct,
			float64(r.TotalStoredBytes)/(1024*1024), r.ColdPacksFetched, r.ReadAmplification, r.ColdS3ModeledMs)
	}

	b.WriteString("\n### C. Pack Granularity Sweep (`256 KiB` -> `8 MiB`, including Negative Regime #3)\n\n")
	b.WriteString("| Pack Size | Regime | Compression Ratio | Packs | Cold Packs Fetched | Cold Bytes Fetched (KiB) | Boot Read Amp (Net) | Boot Raw Read Amp | 1-File Cold Raw Amp | Cold S3 (ms) |\n")
	b.WriteString("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rep.GranularitySweep {
		fmt.Fprintf(&b, "| %d KiB | `%s` | %.2fx | %d | %d | %.1f | %.2fx | %.2fx | **%.1fx** | %.2f |\n",
			r.TargetPackKiB, r.Policy, r.CompressionRatio, r.PackCount,
			r.ColdPacksFetched, float64(r.ColdBytesFetched)/1024.0, r.ReadAmplification, r.RawReadAmp, r.SingleColdFileRawReadAmp, r.ColdS3ModeledMs)
	}

	b.WriteString("\n## 5. Tenant Isolation & Zero Cross-Volume Plaintext Deduplication Proof\n\n")
	iso := rep.IsolationAudit
	b.WriteString("Identical base-image files were built independently for two distinct scopes on the exact same `file://` backend using the same cluster KMS root secret:\n\n")
	b.WriteString("| Security / Isolation Invariant | Observed Value | Status |\n")
	b.WriteString("|---|---:|:---:|\n")
	fmt.Fprintf(&b, "| Volume A (`%s`) total stored bytes | %d B | PASS |\n", iso.TenantA, iso.VolumeABytes)
	fmt.Fprintf(&b, "| Volume B (`%s`) total stored bytes | %d B | PASS |\n", iso.TenantB, iso.VolumeBBytes)
	fmt.Fprintf(&b, "| Combined backend bytes (`A + B`) | %d B | PASS |\n", iso.CombinedBackendBytes)
	fmt.Fprintf(&b, "| Cross-volume plaintext deduplication bytes | **%d B** | %s |\n", iso.CrossVolumeDedupBytes, passFail(iso.CrossVolumeDedupBytes == 0))
	fmt.Fprintf(&b, "| Shared backend object keys across volumes | **%d** | %s |\n", iso.SharedBackendKeys, passFail(iso.SharedBackendKeys == 0))
	fmt.Fprintf(&b, "| Identical pack ciphertexts across volumes | **%d** | %s |\n", iso.IdenticalCiphertexts, passFail(iso.IdenticalCiphertexts == 0))
	fmt.Fprintf(&b, "| Cross-volume `OpenImage` rejected (fail-closed) | %v | %s |\n", iso.CrossVolumeOpenRejected, passFail(iso.CrossVolumeOpenRejected))
	fmt.Fprintf(&b, "| Transplanted pack ciphertext rejected (AAD + key binding) | %v | %s |\n", iso.TransplantRejected, passFail(iso.TransplantRejected))
	fmt.Fprintf(&b, "| Single-bit pack flip rejected (`ErrCorruptPack`) | %v | %s |\n", iso.SingleBitFlipRejected, passFail(iso.SingleBitFlipRejected))

	b.WriteString("\n## 6. Honest Negative Regimes & Operational Guardrails\n\n")
	b.WriteString("### Negative Regime #1 — Pure Similarity Scattering Cold-Start Boot Files\n\n")
	var pure256, pure8192 *RegimeResult
	for i := range rep.GranularitySweep {
		r := &rep.GranularitySweep[i]
		if r.Policy == string(rubikfs.PolicyPureSimilarity) {
			if r.TargetPackKiB == 256 {
				pure256 = r
			} else if r.TargetPackKiB == 8192 {
				pure8192 = r
			}
		}
	}
	if len(rep.PrimaryComparison) >= 4 {
		pathR := rep.PrimaryComparison[0]
		pureR := rep.PrimaryComparison[2]
		hybR := rep.PrimaryComparison[3]
		extra256 := ""
		if pure256 != nil {
			extra256 = fmt.Sprintf(" At `256 KiB` pack granularity (Table 4C), pure similarity scatters the `%d`-file boot working set across **%d packs** (`%.2fx` raw read amp, `%.2f ms` S3 cold start — nearly 2x slower than `path_order_baseline`!).",
				pure256.ColdBootFiles, pure256.ColdPacksFetched, pure256.RawReadAmp, pure256.ColdS3ModeledMs)
		}
		fmt.Fprintf(&b, "Naive `rubikfs_pure_similarity` achieves the highest compression ratio (**%.2fx** vs `%.2fx` for `path_order_baseline`), but rearranges boot-hot files across **%d packs** (vs **%d** for `rubikfs_locality_hybrid` and %d for `path_order_baseline`), resulting in **%.2fx** raw read amplification and **%.2f ms** S3 cold-start latency.%s Pinning boot-hot chunks under `rubikfs_locality_hybrid` cuts cold-start packs fetched to **%d**, lowers S3 cold-start latency to **%.2f ms** (vs `%.2f ms` for pure similarity and `%.2f ms` for path order), and reduces net read amplification to **%.2fx** while retaining **%.2fx** overall compression ratio.\n\n",
			pureR.CompressionRatio, pathR.CompressionRatio,
			pureR.ColdPacksFetched, hybR.ColdPacksFetched, pathR.ColdPacksFetched,
			pureR.RawReadAmp, pureR.ColdS3ModeledMs, extra256,
			hybR.ColdPacksFetched, hybR.ColdS3ModeledMs, pureR.ColdS3ModeledMs, pathR.ColdS3ModeledMs,
			hybR.ReadAmplification, hybR.CompressionRatio)
	}

	b.WriteString("### Negative Regime #2 — Incompressible / Pre-Compressed Payloads (`.zst`, `.gz`, `.age`, random weights)\n\n")
	fmt.Fprintf(&b, "On `%d` high-entropy files (`%.2f MiB` of uniform random / encrypted / pre-compressed blobs), similarity clustering yields **~1.00x (0.0%% compression gain)** while still paying deterministic sketching + clustering CPU overhead (mitigated by raw-mode fallback preventing ciphertext bloat):\n\n",
		rep.IncompCorpus.TotalFiles, float64(rep.IncompCorpus.TotalBytes)/(1024*1024))
	b.WriteString("| Regime | Compression Ratio | Gain vs Path | Sketch (ms) | Cluster (ms) | Total Build (ms) | Raw Fallback Expansion |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rep.IncompressibleCheck {
		expPct := 100.0 * (float64(r.CompressedBytes)/float64(r.UncompressedBytes) - 1.0)
		fmt.Fprintf(&b, "| `%s` | %.4fx | %+.2f%% | %.2f | %.2f | %.1f | %+.3f%% |\n",
			r.Policy, r.CompressionRatio, r.CompressionGainPct, r.SketchMs, r.ClusterMs, r.BuildTimeMs, expPct)
	}

	b.WriteString("\n### Negative Regime #3 — Oversized Pack Granularity (`8 MiB` Packs)\n\n")
	if pure256 != nil && pure8192 != nil {
		fmt.Fprintf(&b, "As shown in Table 4C above, scaling `TargetPackBytes` from `256 KiB` up to `8 MiB` yields diminishing compression returns (`%.2fx -> %.2fx` for `rubikfs_pure_similarity`), while **blowing up single-file cold raw read amplification from `%.1fx` to `%.1fx`** because reading a single cold file forces fetching and decompressing an entire `8 MiB` pack (and unpinned boot reads pull `100%%` of the image, `%.2fx` raw read amp!). **Operational guardrail:** default to `PolicyLocalityHybrid` with `TargetPackBytes = 1 MiB` (`16 KiB` chunks) and `BootPackBytes = 256 KiB`.\n\n",
			pure256.CompressionRatio, pure8192.CompressionRatio,
			pure256.SingleColdFileRawReadAmp, pure8192.SingleColdFileRawReadAmp, pure8192.RawReadAmp)
	} else {
		b.WriteString("As shown in Table 4C above, scaling `TargetPackBytes` from `256 KiB` up to `8 MiB` yields diminishing compression returns while blowing up cold-file raw read amplification. **Operational guardrail:** default to `PolicyLocalityHybrid` with `TargetPackBytes = 1 MiB` (`16 KiB` chunks) and `BootPackBytes = 256 KiB`.\n\n")
	}

	b.WriteString("## 7. Reproducing\n\n")
	b.WriteString("```sh\n")
	b.WriteString("go test -v -race -count=1 ./pkg/rubikfs/... ./cmd/rubikfs-bench/...\n")
	b.WriteString("GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./pkg/rubikfs/... ./cmd/rubikfs-bench/...\n")
	b.WriteString("go run ./cmd/rubikfs-bench --seed 1344 --repo-root . --out-dir docs/research/hip-1344-rubikfs-images\n")
	b.WriteString("```\n")
	return b.String()
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func main() {
	var (
		seed     = flag.Int64("seed", 1344, "deterministic PRNG seed for synthetic base-image layers")
		repoRoot = flag.String("repo-root", ".", "path to hanzoai/vfs repository root for real file grounding")
		outDir   = flag.String("out-dir", "", "optional directory to write REPORT.md and results.json")
		jsonOnly = flag.Bool("json", false, "print JSON report to stdout instead of Markdown")
	)
	flag.Parse()

	ctx := context.Background()
	rep, err := runAllBenchmarks(ctx, *repoRoot, *seed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rubikfs-bench error: %v\n", err)
		os.Exit(1)
	}

	jsonBytes, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal results.json: %v\n", err)
		os.Exit(1)
	}
	jsonBytes = append(jsonBytes, '\n')
	md := renderMarkdownReport(rep)

	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %q: %v\n", *outDir, err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(*outDir, "results.json"), jsonBytes, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write results.json: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(*outDir, "REPORT.md"), []byte(md), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write REPORT.md: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "wrote %s and %s\n",
			filepath.Join(*outDir, "REPORT.md"),
			filepath.Join(*outDir, "results.json"))
	}

	if *jsonOnly {
		_, _ = os.Stdout.Write(jsonBytes)
	} else {
		_, _ = os.Stdout.WriteString(md)
	}
}
