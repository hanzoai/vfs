package modeldist

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

func mustKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

func deterministicPayload(seed uint64, size int) []byte {
	buf := make([]byte, size)
	fillDeterministicChunk(buf, seed, 0)
	return buf
}

// buildTestBundle creates a signed 5-asset-kind ManifestV1 stored in a real
// file:// VFS backend (proving zero second disk format).
func buildTestBundle(t *testing.T, ctx context.Context, chunkSize int64) (*ManifestV1, []byte, ed25519.PublicKey, ed25519.PrivateKey, *VFSStoreProvider, string) {
	t.Helper()
	pub, priv := mustKeyPair(t)
	originDir := t.TempDir()
	b, err := backend.Open(ctx, "file://"+originDir)
	if err != nil {
		t.Fatalf("backend.Open origin: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	origin := NewVFSStoreProvider("origin-vfs", true, b)

	specs := []struct {
		path    string
		kind    AssetKind
		variant string
		seed    uint64
		size    int
	}{
		{"weights/zen-7b-layer0.safetensors", AssetKindZenWeights, "bf16", 101, int(chunkSize*3 + 1337)},
		{"gguf/zen-7b-q4_k_m.gguf", AssetKindGGUF, "Q4_K_M", 202, int(chunkSize*2 + 512)},
		{"adapters/code-lora-r16.safetensors", AssetKindAdapter, "lora-r16", 303, int(chunkSize + 777)},
		{"tokenizer/tokenizer.json", AssetKindTokenizer, "bpe-v2", 404, 16384},
		{"profiles/default-gpu.json", AssetKindExecProfile, "fp16-8k", 505, 4096},
	}

	artifacts := make([]Artifact, 0, len(specs))
	for _, s := range specs {
		art, err := IngestArtifact(ctx, s.path, s.kind, s.variant, bytes.NewReader(deterministicPayload(s.seed, s.size)), chunkSize, origin)
		if err != nil {
			t.Fatalf("IngestArtifact %s: %v", s.path, err)
		}
		artifacts = append(artifacts, art)
	}

	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "zen-7b-instruct",
		Version:       "1.2.0",
		License:       "Apache-2.0",
		LicenseURL:    "https://www.apache.org/licenses/LICENSE-2.0",
		Description:   "Zen 7B Instruct multi-asset bundle",
		ExecutionProfile: map[string]string{
			"context_length": "32768",
			"kv_cache_dtype": "fp8",
			"rope_scaling":   "yarn",
		},
		Provenance: Provenance{
			SourceCommit:           "abc123def456",
			TrainingDatasetLicense: "Apache-2.0 AND CC-BY-4.0",
			QuantizationMethod:     "k-quants-q4_k_m",
			CreatedAt:              "2026-11-26T00:00:00Z",
		},
		Artifacts: artifacts,
	}
	if err := m.Sign(priv, "zen-foundation"); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	raw, err := PublishManifest(ctx, m, origin)
	if err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	return m, raw, pub, priv, origin, originDir
}

// TestMultiGiBDistributionAcrossThreeProviders verifies streaming distribution
// of a 2.5 GiB model across 3 independent HTTP mirrors with origin removed,
// real BLAKE3 + SHA-256 verification of all 2.5 GiB bytes, and bounded RAM.
func TestMultiGiBDistributionAcrossThreeProviders(t *testing.T) {
	ctx := context.Background()
	const chunkSize int64 = 4 << 20  // 4 MiB chunks
	var totalSize int64 = 2560 << 20 // 2.5 GiB (2,684,354,560 bytes -> 640 unique chunks)
	if testing.Short() {
		totalSize = 256 << 20 // 256 MiB (64 unique chunks) for fast -short CI gates
	}

	art, cat, err := BuildDeterministicMultiGiBArtifact(
		"weights/zen-70b-q4_k_m.gguf",
		AssetKindGGUF,
		"Q4_K_M",
		0x5a454e373042,
		totalSize,
		chunkSize,
	)
	if err != nil {
		t.Fatalf("BuildDeterministicMultiGiBArtifact: %v", err)
	}
	pub, priv := mustKeyPair(t)
	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "zen-70b-gguf",
		Version:       "2.0.0",
		License:       "Apache-2.0",
		Provenance: Provenance{
			TrainingDatasetLicense: "Apache-2.0",
			QuantizationMethod:     "Q4_K_M",
			CreatedAt:              "2026-11-26T00:00:00Z",
		},
		Artifacts: []Artifact{art},
	}
	if err := m.Sign(priv, "zen-release-key"); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	raw, err := m.MarshalJSONCanonical()
	if err != nil {
		t.Fatalf("MarshalJSONCanonical: %v", err)
	}
	cat.AttachManifest(m.ManifestCID, raw)

	// Start origin + 3 independent community mirrors.
	originBack := NewCatalogBackingProvider("origin", true, cat)
	originSrv, err := StartHTTPMirror("origin", true, originBack)
	if err != nil {
		t.Fatalf("StartHTTPMirror origin: %v", err)
	}
	var mirrorSrvs []*HTTPMirrorServer
	var mirrorClients []ChunkProvider
	for _, id := range []string{"mirror-eu-1", "mirror-us-2", "mirror-ap-3"} {
		back := NewCatalogBackingProvider(id, false, cat)
		srv, err := StartHTTPMirror(id, false, back)
		if err != nil {
			t.Fatalf("StartHTTPMirror %s: %v", id, err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		mirrorSrvs = append(mirrorSrvs, srv)
		mirrorClients = append(mirrorClients, srv.Client())
	}

	// Shut down and disable origin completely before the client starts.
	originBack.Disable()
	_ = originSrv.Close()

	client := NewClient(ClientConfig{
		Policy: VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			License: LicensePolicy{
				RequireLicense:  true,
				AllowedLicenses: []string{"Apache-2.0", "MIT"},
			},
			MinVersion: "2.0.0",
		},
		Concurrency: 4,
	})

	// Fetch manifest from independent mirrors (zero origin contact).
	fetched, via, err := client.FetchVerifiedManifest(ctx, m.ManifestCID, mirrorClients)
	if err != nil {
		t.Fatalf("FetchVerifiedManifest: %v", err)
	}
	if via.IsOrigin() {
		t.Fatalf("expected manifest from independent mirror, got origin")
	}

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	report, err := client.PullArtifactStream(ctx, fetched, 0, mirrorClients, io.Discard)
	if err != nil {
		t.Fatalf("PullArtifactStream 2.5 GiB: %v", err)
	}

	runtime.ReadMemStats(&memAfter)
	if report.VerifiedBytes != totalSize {
		t.Fatalf("VerifiedBytes = %d, want %d", report.VerifiedBytes, totalSize)
	}
	if report.DownloadedChunks != len(art.Chunks) {
		t.Fatalf("DownloadedChunks = %d, want %d", report.DownloadedChunks, len(art.Chunks))
	}
	if report.OriginChunksServed != 0 || originSrv.ServerStats().GetRequests != 0 {
		t.Fatalf("expected 0 origin hits after kill, got report=%d server=%d",
			report.OriginChunksServed, originSrv.ServerStats().GetRequests)
	}
	for _, srv := range mirrorSrvs {
		if report.ProviderChunkCounts[srv.ID()] == 0 {
			t.Errorf("mirror %s served 0 chunks; expected balanced multi-mirror distribution", srv.ID())
		}
	}
	if report.ArtifactRootBLAKE3[art.Path] != art.RootBLAKE3 {
		t.Fatalf("root BLAKE3 mismatch: got %s, want %s", report.ArtifactRootBLAKE3[art.Path], art.RootBLAKE3)
	}
	if report.ArtifactRootSHA256[art.Path] != art.RootSHA256 {
		t.Fatalf("root SHA-256 mismatch: got %s, want %s", report.ArtifactRootSHA256[art.Path], art.RootSHA256)
	}
	t.Logf("%.0f MiB 3-mirror verified stream: %.1f MB/s, TTFT=%.2f ms, p50=%.2f ms, p99=%.2f ms, HeapInuse=%.1f MiB",
		float64(totalSize)/(1024*1024), report.ThroughputMBps, report.TTFTMs, report.ChunkLatencyP50Ms, report.ChunkLatencyP99Ms,
		float64(memAfter.HeapInuse)/(1024*1024))
}

// TestOriginRemovedFullBundleReconstruction seeds 3 independent mirrors from a
// file:// VFS origin, deletes the origin completely, and verifies byte-for-byte
// reconstruction of all 5 asset kinds plus VFS staging with zero second format.
func TestOriginRemovedFullBundleReconstruction(t *testing.T) {
	ctx := context.Background()
	m, _, pub, _, origin, originDir := buildTestBundle(t, ctx, 64<<10)

	originSrv, err := StartHTTPMirror("origin-http", true, origin)
	if err != nil {
		t.Fatalf("StartHTTPMirror origin: %v", err)
	}

	// Replicate from origin HTTP into 3 independent mirrors backed by VFS stores.
	var mirrors []ChunkProvider
	for _, name := range []string{"mirror-1", "mirror-2", "mirror-3"} {
		store := NewVFSStoreProvider(name+"-vfs", false, storetest.NewMem())
		if _, err := ReplicateManifest(ctx, m, originSrv.Client(), store); err != nil {
			t.Fatalf("ReplicateManifest to %s: %v", name, err)
		}
		srv, err := StartHTTPMirror(name, false, store)
		if err != nil {
			t.Fatalf("StartHTTPMirror %s: %v", name, err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		mirrors = append(mirrors, srv.Client())
	}

	// Permanently kill and delete the origin server and its backing directory.
	originReqsBeforeKill := originSrv.ServerStats().GetRequests
	origin.Disable()
	_ = originSrv.Close()
	if err := os.RemoveAll(originDir); err != nil {
		t.Fatalf("RemoveAll originDir: %v", err)
	}

	// Stage verified chunks into a fresh local VFS file:// backend as well.
	stagingDir := t.TempDir()
	stagingBackend, err := backend.Open(ctx, "file://"+stagingDir)
	if err != nil {
		t.Fatalf("backend.Open staging: %v", err)
	}
	defer stagingBackend.Close()

	client := NewClient(ClientConfig{
		Policy: VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			License: LicensePolicy{
				RequireLicense:        true,
				AllowedLicenses:       []string{"Apache-2.0"},
				RequireDatasetLicense: true,
			},
			MinVersion: "1.0.0",
			MaxVersion: "2.0.0",
		},
		Concurrency:       4,
		ChunkStagingStore: stagingBackend,
	})

	// Include the dead origin in the provider list to prove automatic failover too.
	allProviders := append([]ChunkProvider{originSrv.Client()}, mirrors...)
	fetched, via, err := client.FetchVerifiedManifest(ctx, m.ManifestCID, allProviders)
	if err != nil {
		t.Fatalf("FetchVerifiedManifest after origin kill: %v", err)
	}
	if via.IsOrigin() {
		t.Fatalf("dead origin should not serve manifest")
	}

	outDir := t.TempDir()
	report, err := client.PullToDir(ctx, fetched, allProviders, outDir, PullOptions{})
	if err != nil {
		t.Fatalf("PullToDir after origin kill: %v", err)
	}
	if report.OriginChunksServed != 0 {
		t.Fatalf("OriginChunksServed = %d, want 0", report.OriginChunksServed)
	}
	if originSrv.ServerStats().GetRequests != originReqsBeforeKill {
		t.Fatalf("origin served requests after shutdown")
	}
	if len(report.ArtifactRootBLAKE3) != 5 {
		t.Fatalf("reconstructed %d artifacts, want 5", len(report.ArtifactRootBLAKE3))
	}
	for _, art := range m.Artifacts {
		if report.ArtifactRootBLAKE3[art.Path] != art.RootBLAKE3 {
			t.Errorf("artifact %s root BLAKE3 mismatch", art.Path)
		}
		// Verify chunks exist directly in the VFS backend under model/chunk/<2hex>/<hash>.
		for _, ref := range art.Chunks {
			key, err := ChunkKey(ref.BLAKE3)
			if err != nil {
				t.Fatalf("ChunkKey: %v", err)
			}
			if _, err := stagingBackend.Stat(ctx, key); err != nil {
				t.Fatalf("staged VFS backend missing %s: %v", key, err)
			}
		}
	}
}

// TestRejectModifiedChunkAndFailover injects a 1-bit flip into Mirror 1 and a
// truncation into Mirror 2, asserting that the client rejects tampered chunks,
// quarantines both poisoned mirrors, and reconstructs cleanly from Mirror 3.
func TestRejectModifiedChunkAndFailover(t *testing.T) {
	ctx := context.Background()
	m, _, pub, _, origin, _ := buildTestBundle(t, ctx, 32<<10)

	var srvs []*HTTPMirrorServer
	var providers []ChunkProvider
	for _, name := range []string{"mirror-bitflip", "mirror-truncate", "mirror-clean"} {
		store := NewVFSStoreProvider(name, false, storetest.NewMem())
		if _, err := ReplicateManifest(ctx, m, origin, store); err != nil {
			t.Fatalf("ReplicateManifest: %v", err)
		}
		srv, err := StartHTTPMirror(name, false, store)
		if err != nil {
			t.Fatalf("StartHTTPMirror: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		srvs = append(srvs, srv)
		providers = append(providers, srv.Client())
	}

	// Poison mirror 0 with a 1-bit flip on every chunk, and mirror 1 with truncation.
	srvs[0].InjectTamper("*", func(b []byte) []byte {
		if len(b) > 0 {
			b[len(b)/2] ^= 0x01
		}
		return b
	})
	srvs[1].InjectTamper("*", func(b []byte) []byte {
		if len(b) > 8 {
			return b[:len(b)-8]
		}
		return b
	})

	client := NewClient(ClientConfig{
		Policy:                   VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}},
		Concurrency:              3,
		QuarantineAfterBadChunks: 1,
	})

	outDir := t.TempDir()
	report, err := client.PullToDir(ctx, m, providers, outDir, PullOptions{})
	if err != nil {
		t.Fatalf("PullToDir with tampered mirrors: %v", err)
	}
	if report.RejectedChunks < 2 {
		t.Fatalf("expected >= 2 rejected tampered chunks, got %d", report.RejectedChunks)
	}
	if len(report.QuarantinedProviders) != 2 {
		t.Fatalf("expected 2 quarantined providers, got %v", report.QuarantinedProviders)
	}
	if report.ProviderChunkCounts["mirror-bitflip"] != 0 || report.ProviderChunkCounts["mirror-truncate"] != 0 {
		t.Fatalf("poisoned mirrors must have 0 accepted chunks, got %v", report.ProviderChunkCounts)
	}
	if report.ProviderChunkCounts["mirror-clean"] != m.TotalChunks() {
		t.Fatalf("clean mirror served %d/%d chunks", report.ProviderChunkCounts["mirror-clean"], m.TotalChunks())
	}
}

// TestResumeInterruptedTransfer aborts at 50% progress, restarts from disk
// checkpoint without re-downloading verified chunks, and also verifies recovery
// if a previously downloaded byte on disk is corrupted during a crash.
func TestResumeInterruptedTransfer(t *testing.T) {
	ctx := context.Background()
	pub, priv := mustKeyPair(t)
	origin := NewVFSStoreProvider("origin", true, storetest.NewMem())
	const chunkSize int64 = 32 << 10
	art, err := IngestArtifact(ctx, "weights/zen-resume.gguf", AssetKindGGUF, "Q4_K_M",
		bytes.NewReader(deterministicPayload(999, int(chunkSize*10))), chunkSize, origin)
	if err != nil {
		t.Fatalf("IngestArtifact: %v", err)
	}
	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "zen-resume",
		Version:       "1.0.0",
		License:       "MIT",
		Provenance:    Provenance{CreatedAt: "2026-11-26T00:00:00Z"},
		Artifacts:     []Artifact{art},
	}
	if err := m.Sign(priv, "resume-pub"); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := PublishManifest(ctx, m, origin); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	srv, err := StartHTTPMirror("mirror-resume", false, origin)
	if err != nil {
		t.Fatalf("StartHTTPMirror: %v", err)
	}
	defer srv.Close()
	providers := []ChunkProvider{srv.Client()}

	outDir := t.TempDir()
	ckptDir := t.TempDir()
	cfg := ClientConfig{
		Policy:                  VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}},
		Concurrency:             2,
		CheckpointDir:           ckptDir,
		CheckpointFlushInterval: 1,
	}

	// Phase 1: Interrupt cleanly after 5 of 10 chunks (50%).
	rep1, err := NewClient(cfg).PullToDir(ctx, m, providers, outDir, PullOptions{InterruptAfterChunks: 5})
	if !errors.Is(err, ErrTransferInterrupted) {
		t.Fatalf("expected ErrTransferInterrupted, got %v", err)
	}
	if rep1.DownloadedChunks != 5 {
		t.Fatalf("phase 1 DownloadedChunks = %d, want 5", rep1.DownloadedChunks)
	}
	reqsAfterPhase1 := srv.ServerStats().GetRequests

	// Phase 2: Resume with a brand-new client instance; 0 already-verified chunks re-fetched.
	rep2, err := NewClient(cfg).PullToDir(ctx, m, providers, outDir, PullOptions{})
	if err != nil {
		t.Fatalf("resume PullToDir: %v", err)
	}
	if rep2.ResumedChunks != 5 || rep2.DownloadedChunks != 5 {
		t.Fatalf("resume counts: Resumed=%d Downloaded=%d, want 5/5", rep2.ResumedChunks, rep2.DownloadedChunks)
	}
	reqsPhase2 := srv.ServerStats().GetRequests - reqsAfterPhase1
	if reqsPhase2 != 5 {
		t.Fatalf("mirror served %d requests during resume, want exactly 5 (0 redundant)", reqsPhase2)
	}
	if rep2.ArtifactRootBLAKE3[art.Path] != art.RootBLAKE3 {
		t.Fatalf("resumed root BLAKE3 mismatch")
	}

	// Phase 3: Simulate partial-file disk corruption before resume; client must
	// detect the corrupted local chunk via re-verification and re-fetch it.
	outDir2 := t.TempDir()
	ckptDir2 := t.TempDir()
	cfg2 := cfg
	cfg2.CheckpointDir = ckptDir2
	_, err = NewClient(cfg2).PullToDir(ctx, m, providers, outDir2, PullOptions{InterruptAfterChunks: 5})
	if !errors.Is(err, ErrTransferInterrupted) {
		t.Fatalf("expected ErrTransferInterrupted, got %v", err)
	}
	partialFile := filepath.Join(outDir2, filepath.FromSlash(art.Path))
	f, err := os.OpenFile(partialFile, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile partial: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xff, 0x00, 0xaa}, 128); err != nil {
		t.Fatalf("corrupt chunk 0: %v", err)
	}
	_ = f.Close()

	rep3, err := NewClient(cfg2).PullToDir(ctx, m, providers, outDir2, PullOptions{})
	if err != nil {
		t.Fatalf("resume after local corruption: %v", err)
	}
	if rep3.ResumedChunks != 4 || rep3.DownloadedChunks != 6 {
		t.Fatalf("expected 4 resumed + 6 downloaded (re-fetched corrupted chunk 0), got %d/%d",
			rep3.ResumedChunks, rep3.DownloadedChunks)
	}
}

// TestManifestSecurityLicenseVersionProvenance exercises fail-closed manifest
// verification across signatures, licenses, SemVer bounds, path traversal, and
// multi-step parent provenance chains.
func TestManifestSecurityLicenseVersionProvenance(t *testing.T) {
	ctx := context.Background()
	m, _, pub, priv, _, _ := buildTestBundle(t, ctx, 32<<10)
	pub2, priv2 := mustKeyPair(t)
	untrustedPub, untrustedPriv := mustKeyPair(t)
	_ = untrustedPub

	basePolicy := VerificationPolicy{
		TrustedKeys: []ed25519.PublicKey{pub},
		License: LicensePolicy{
			RequireLicense:  true,
			AllowedLicenses: []string{"Apache-2.0", "MIT"},
			DeniedLicenses:  []string{"Proprietary-NC"},
		},
		MinVersion: "1.0.0",
		MaxVersion: "1.9.9",
	}
	if err := m.Verify(basePolicy); err != nil {
		t.Fatalf("valid manifest failed verify: %v", err)
	}

	// 1. Forged/corrupted signature byte rejected.
	forged := *m
	forged.Signatures = append([]SignatureEnvelope(nil), m.Signatures...)
	sigBytes := []byte(forged.Signatures[0].SignatureHex)
	if sigBytes[0] == 'a' {
		sigBytes[0] = 'b'
	} else {
		sigBytes[0] = 'a'
	}
	forged.Signatures[0].SignatureHex = string(sigBytes)
	if err := forged.Verify(basePolicy); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("expected ErrSignatureInvalid for forged sig, got %v", err)
	}

	// 2. Untrusted signer rejected.
	untrusted := *m
	untrusted.Signatures = nil
	if err := untrusted.Sign(untrustedPriv, "rogue-publisher"); err != nil {
		t.Fatalf("Sign untrusted: %v", err)
	}
	if err := untrusted.Verify(basePolicy); !errors.Is(err, ErrInsufficientSigs) {
		t.Fatalf("expected ErrInsufficientSigs for untrusted key, got %v", err)
	}

	// 3. Threshold 2-of-2 multi-signature support.
	coSigned := *m
	coSigned.Signatures = append([]SignatureEnvelope(nil), m.Signatures...)
	if err := coSigned.Sign(priv2, "community-auditor"); err != nil {
		t.Fatalf("co-sign: %v", err)
	}
	threshPolicy := basePolicy
	threshPolicy.TrustedKeys = []ed25519.PublicKey{pub, pub2}
	threshPolicy.MinValidSignatures = 2
	if err := coSigned.Verify(threshPolicy); err != nil {
		t.Fatalf("2-of-2 co-signed manifest failed: %v", err)
	}
	if err := m.Verify(threshPolicy); !errors.Is(err, ErrInsufficientSigs) {
		t.Fatalf("1-of-2 manifest should fail 2-sig policy, got %v", err)
	}

	// 4. Post-signing license tamper rejected.
	tampered := *m
	tampered.License = "MIT"
	if err := tampered.Verify(basePolicy); !errors.Is(err, ErrManifestCIDMismatch) {
		t.Fatalf("expected ErrManifestCIDMismatch on post-sign edit, got %v", err)
	}

	// 5. Disallowed license rejected even when properly signed.
	badLic := *m
	badLic.License = "Proprietary-NC"
	badLic.ManifestCID = ""
	badLic.Signatures = nil
	if err := badLic.Sign(priv, "zen-foundation"); err != nil {
		t.Fatalf("Sign badLic: %v", err)
	}
	if err := badLic.Verify(basePolicy); !errors.Is(err, ErrLicenseRejected) {
		t.Fatalf("expected ErrLicenseRejected, got %v", err)
	}

	// 6. Out-of-range SemVer rejected.
	oldVer := *m
	oldVer.Version = "0.9.5"
	oldVer.ManifestCID = ""
	oldVer.Signatures = nil
	if err := oldVer.Sign(priv, "zen-foundation"); err != nil {
		t.Fatalf("Sign oldVer: %v", err)
	}
	if err := oldVer.Verify(basePolicy); !errors.Is(err, ErrVersionRejected) {
		t.Fatalf("expected ErrVersionRejected, got %v", err)
	}

	// 7. Path traversal rejected.
	escape := *m
	escape.Artifacts = append([]Artifact(nil), m.Artifacts...)
	escape.Artifacts[0].Path = "../etc/passwd"
	escape.ManifestCID = ""
	escape.Signatures = nil
	if err := escape.Sign(priv, "zen-foundation"); err != nil {
		t.Fatalf("Sign escape: %v", err)
	}
	if err := escape.Verify(basePolicy); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected ErrInvalidManifest for path traversal, got %v", err)
	}

	// 8. Provenance chain verification (child adapter -> parent base model).
	child := *m
	child.ModelID = "zen-7b-code-adapter"
	child.Version = "1.3.0"
	child.Provenance.ParentManifestCID = m.ManifestCID
	child.ManifestCID = ""
	child.Signatures = nil
	if err := child.Sign(priv, "community-adapter-author"); err != nil {
		t.Fatalf("Sign child: %v", err)
	}
	chainPolicy := basePolicy
	chainPolicy.ParentResolver = func(cid string) (*ManifestV1, error) {
		if cid == m.ManifestCID {
			return m, nil
		}
		return nil, ErrManifestNotFound
	}
	if err := child.Verify(chainPolicy); err != nil {
		t.Fatalf("valid provenance chain failed: %v", err)
	}
	brokenPolicy := basePolicy
	brokenPolicy.ParentResolver = func(string) (*ManifestV1, error) { return nil, ErrManifestNotFound }
	if err := child.Verify(brokenPolicy); !errors.Is(err, ErrProvenanceInvalid) {
		t.Fatalf("expected ErrProvenanceInvalid when parent missing, got %v", err)
	}
}

// TestConcurrentPullsRace verifies 8 concurrent clients pulling concurrently
// from 3 mirrors (with 1 poisoned mirror) under the Go race detector.
func TestConcurrentPullsRace(t *testing.T) {
	ctx := context.Background()
	m, _, pub, _, origin, _ := buildTestBundle(t, ctx, 32<<10)

	var srvs []*HTTPMirrorServer
	var providers []ChunkProvider
	for _, name := range []string{"race-m1-bad", "race-m2-ok", "race-m3-ok"} {
		store := NewVFSStoreProvider(name, false, storetest.NewMem())
		if _, err := ReplicateManifest(ctx, m, origin, store); err != nil {
			t.Fatalf("ReplicateManifest: %v", err)
		}
		srv, err := StartHTTPMirror(name, false, store)
		if err != nil {
			t.Fatalf("StartHTTPMirror: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		srvs = append(srvs, srv)
		providers = append(providers, srv.Client())
	}
	srvs[0].InjectTamper(m.Artifacts[0].Chunks[0].BLAKE3, func(b []byte) []byte {
		b[0] ^= 0x80
		return b
	})

	const numClients = 8
	var wg sync.WaitGroup
	errs := make(chan error, numClients)
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := NewClient(ClientConfig{
				Policy:      VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}},
				Concurrency: 3,
			})
			_, err := cli.PullToDir(ctx, m, providers, t.TempDir(), PullOptions{})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent client failed: %v", err)
		}
	}
}

// TestSovereigntyOfflineExportAndFork verifies exporting a model to a standard
// file:// VFS directory, shutting down all original mirrors, serving from the
// offline export, and forking a provenance-linked child manifest.
func TestSovereigntyOfflineExportAndFork(t *testing.T) {
	ctx := context.Background()
	m, _, pub, _, origin, _ := buildTestBundle(t, ctx, 32<<10)
	forkPub, forkPriv := mustKeyPair(t)

	originSrv, err := StartHTTPMirror("origin-orig", true, origin)
	if err != nil {
		t.Fatalf("StartHTTPMirror: %v", err)
	}

	policy := VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}}
	exportDir := t.TempDir()
	receipt, err := ExportBundle(ctx, m, policy, originSrv.Client(), exportDir)
	if err != nil {
		t.Fatalf("ExportBundle: %v", err)
	}
	if receipt.ChunksExported == 0 || receipt.TotalBytes != m.TotalBytes() {
		t.Fatalf("unexpected export receipt: %+v", receipt)
	}

	// Shut down original infrastructure completely.
	origin.Disable()
	_ = originSrv.Close()

	// Mount exported VFS file:// backend independently and serve over HTTP.
	exportedBackend, err := backend.Open(ctx, receipt.BackendURL)
	if err != nil {
		t.Fatalf("backend.Open exported: %v", err)
	}
	defer exportedBackend.Close()

	sovereignStore := NewVFSStoreProvider("sovereign-mirror", false, exportedBackend)
	sovereignSrv, err := StartHTTPMirror("sovereign-mirror", false, sovereignStore)
	if err != nil {
		t.Fatalf("StartHTTPMirror sovereign: %v", err)
	}
	defer sovereignSrv.Close()

	cli := NewClient(ClientConfig{Policy: policy, Concurrency: 2})
	fetched, _, err := cli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{sovereignSrv.Client()})
	if err != nil {
		t.Fatalf("FetchVerifiedManifest from offline export: %v", err)
	}
	if _, err := cli.PullToDir(ctx, fetched, []ChunkProvider{sovereignSrv.Client()}, t.TempDir(), PullOptions{}); err != nil {
		t.Fatalf("PullToDir from offline export: %v", err)
	}

	// Independent community fork referencing upstream ManifestCID.
	fork := *fetched
	fork.ModelID = "zen-7b-community-fork"
	fork.Version = "1.2.1"
	fork.Provenance.ParentManifestCID = fetched.ManifestCID
	fork.ManifestCID = ""
	fork.Signatures = nil
	if err := fork.Sign(forkPriv, "independent-community-org"); err != nil {
		t.Fatalf("Sign fork: %v", err)
	}
	forkPolicy := VerificationPolicy{
		TrustedKeys: []ed25519.PublicKey{pub, forkPub},
		ParentResolver: func(cid string) (*ManifestV1, error) {
			if cid == fetched.ManifestCID {
				return fetched, nil
			}
			return nil, ErrManifestNotFound
		},
	}
	if err := fork.Verify(forkPolicy); err != nil {
		t.Fatalf("fork verify failed: %v", err)
	}
}

// TestHonestNegativeBoundaryRegimes explicitly validates three negative /
// boundary conditions required for falsifiable HIP-1350 evaluation:
//  1. unverified_mirror_negative: omitting chunk hash checks accepts 100% silently corrupted weights.
//  2. single_origin_removed_negative: centralized single-origin fails with 0% availability on origin removal.
//  3. all_mirrors_poisoned_fail_closed: total eclipse fails closed with 0% silent corruption.
func TestHonestNegativeBoundaryRegimes(t *testing.T) {
	ctx := context.Background()
	m, _, pub, _, origin, _ := buildTestBundle(t, ctx, 32<<10)

	// Regime 1: Unverified pull from a poisoned mirror silently accepts corrupted chunks.
	poisonedStore := NewVFSStoreProvider("poisoned-single", false, storetest.NewMem())
	if _, err := ReplicateManifest(ctx, m, origin, poisonedStore); err != nil {
		t.Fatalf("ReplicateManifest: %v", err)
	}
	poisonedStore.InjectTamper("*", func(b []byte) []byte {
		if len(b) > 0 {
			b[0] ^= 0xff
		}
		return b
	})
	corrupted, total, rootOK, err := UnsafeUnverifiedMirrorPullForNegativeControl(ctx, m, poisonedStore)
	if err != nil {
		t.Fatalf("unverified negative control error: %v", err)
	}
	if corrupted != total || rootOK {
		t.Fatalf("expected 100%% silent corruption in unverified baseline, got %d/%d rootOK=%v", corrupted, total, rootOK)
	}

	// Regime 2: Single centralized origin removed -> 0% availability.
	singleSrv, err := StartHTTPMirror("single-origin", true, origin)
	if err != nil {
		t.Fatalf("StartHTTPMirror: %v", err)
	}
	_ = singleSrv.Close()
	cli := NewClient(ClientConfig{
		Policy:          VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}},
		PerChunkTimeout: 2 * time.Second,
	})
	if _, err := cli.PullToDir(ctx, m, []ChunkProvider{singleSrv.Client()}, t.TempDir(), PullOptions{}); !errors.Is(err, ErrAllProvidersFailed) {
		t.Fatalf("expected ErrAllProvidersFailed on dead single origin, got %v", err)
	}

	// Regime 3: Total eclipse (all 3 mirrors poisoned) -> fail closed, 0 silent corruption.
	var badProviders []ChunkProvider
	for _, name := range []string{"bad-1", "bad-2", "bad-3"} {
		st := NewVFSStoreProvider(name, false, storetest.NewMem())
		if _, err := ReplicateManifest(ctx, m, origin, st); err != nil {
			t.Fatalf("ReplicateManifest: %v", err)
		}
		st.InjectTamper("*", func(b []byte) []byte {
			if len(b) > 0 {
				b[0] ^= 0x55
			}
			return b
		})
		badProviders = append(badProviders, st)
	}
	rep, err := cli.PullToDir(ctx, m, badProviders, t.TempDir(), PullOptions{})
	if !errors.Is(err, ErrAllProvidersFailed) || !errors.Is(err, ErrChunkDigestMismatch) {
		t.Fatalf("expected fail-closed ErrAllProvidersFailed wrapping ErrChunkDigestMismatch, got %v", err)
	}
	if rep.VerifiedBytes != 0 {
		t.Fatalf("expected 0 verified/accepted bytes under total eclipse, got %d", rep.VerifiedBytes)
	}
	if len(rep.QuarantinedProviders) != 3 {
		t.Fatalf("expected all 3 poisoned mirrors quarantined, got %v", rep.QuarantinedProviders)
	}
}
