package modeldist

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

type stallingChunkProvider struct {
	ChunkProvider
	stallHash     string
	release       <-chan struct{}
	stalledNotify chan struct{}
	stallOnce     sync.Once
	getCount      atomic.Int64
}

func (p *stallingChunkProvider) GetChunk(ctx context.Context, blake3Hex string, maxSize int64) ([]byte, error) {
	p.getCount.Add(1)
	if blake3Hex == p.stallHash {
		p.stallOnce.Do(func() { close(p.stalledNotify) })
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.ChunkProvider.GetChunk(ctx, blake3Hex, maxSize)
}

// TestPullArtifactStreamBoundsDispatchWhenEarlyChunkStalls verifies that when
// chunk 0 stalls, PullArtifactStream caps total dispatched chunks at the
// in-order consumption window (2 * concurrency) rather than buffering the rest
// of the artifact in memory.
func TestPullArtifactStreamBoundsDispatchWhenEarlyChunkStalls(t *testing.T) {
	ctx := context.Background()
	pub, priv := mustKeyPair(t)
	store := NewVFSStoreProvider("stall-mirror", false, storetest.NewMem())

	const chunkSize int64 = 16 << 10
	const totalChunks = 32
	art, err := IngestArtifact(
		ctx,
		"weights/bounded-stream.gguf",
		AssetKindGGUF,
		"Q4_K_M",
		bytes.NewReader(deterministicPayload(777, int(chunkSize)*totalChunks)),
		chunkSize,
		store,
	)
	if err != nil {
		t.Fatalf("IngestArtifact: %v", err)
	}
	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "bounded-stream-model",
		Version:       "1.0.0",
		License:       "Apache-2.0",
		Provenance:    Provenance{CreatedAt: "2026-11-26T00:00:00Z"},
		Artifacts:     []Artifact{art},
	}
	if err := m.Sign(priv, "test-pub"); err != nil {
		t.Fatalf("Sign: %v", err)
	}

	release := make(chan struct{})
	stalled := make(chan struct{})
	provider := &stallingChunkProvider{
		ChunkProvider: store,
		stallHash:     art.Chunks[0].BLAKE3,
		release:       release,
		stalledNotify: stalled,
	}

	const concurrency = 2
	const maxWindow = concurrency * 2
	cli := NewClient(ClientConfig{
		Policy:      VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}},
		Concurrency: concurrency,
	})

	type result struct {
		rep *PullReport
		err error
	}
	done := make(chan result, 1)
	go func() {
		rep, err := cli.PullArtifactStream(ctx, m, 0, []ChunkProvider{provider}, io.Discard)
		done <- result{rep: rep, err: err}
	}()

	select {
	case <-stalled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for chunk 0 to stall")
	}

	// Give other workers plenty of time to drain all available credits.
	deadline := time.Now().Add(2 * time.Second)
	for provider.getCount.Load() < maxWindow && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// Hold slightly longer to prove no additional chunks are dispatched while chunk 0 is stalled.
	time.Sleep(80 * time.Millisecond)

	dispatchedWhileStalled := provider.getCount.Load()
	if dispatchedWhileStalled > maxWindow {
		close(release)
		t.Fatalf("dispatched %d chunks while chunk 0 stalled; must be bounded to <= %d (2 * concurrency)",
			dispatchedWhileStalled, maxWindow)
	}

	// Release chunk 0 and confirm the entire stream finishes cleanly.
	close(release)
	res := <-done
	if res.err != nil {
		t.Fatalf("PullArtifactStream failed after releasing chunk 0: %v", res.err)
	}
	if res.rep.DownloadedChunks != totalChunks || res.rep.VerifiedBytes != art.Size {
		t.Fatalf("unexpected report: downloaded=%d/%d bytes=%d/%d",
			res.rep.DownloadedChunks, totalChunks, res.rep.VerifiedBytes, art.Size)
	}
	if res.rep.ArtifactRootBLAKE3[art.Path] != art.RootBLAKE3 {
		t.Fatalf("root BLAKE3 mismatch after stalled stream")
	}
}

// TestPullToDirRejectsSymlinkEscape verifies PullToDir confines opens to outDir
// and refuses to traverse or overwrite symlinks pointing outside or inside outDir.
func TestPullToDirRejectsSymlinkEscape(t *testing.T) {
	ctx := context.Background()
	pub, priv := mustKeyPair(t)
	store := NewVFSStoreProvider("symlink-mirror", false, storetest.NewMem())
	art, err := IngestArtifact(
		ctx,
		"weights/model.gguf",
		AssetKindGGUF,
		"Q4_K_M",
		bytes.NewReader(deterministicPayload(123, 8192)),
		4096,
		store,
	)
	if err != nil {
		t.Fatalf("IngestArtifact: %v", err)
	}
	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "symlink-test",
		Version:       "1.0.0",
		License:       "Apache-2.0",
		Provenance:    Provenance{CreatedAt: "2026-11-26T00:00:00Z"},
		Artifacts:     []Artifact{art},
	}
	if err := m.Sign(priv, "test-pub"); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	cli := NewClient(ClientConfig{
		Policy:      VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}},
		Concurrency: 2,
	})
	sentinel := []byte("DO-NOT-OVERWRITE-OUTSIDE")

	t.Run("dir_symlink_absolute_outside", func(t *testing.T) {
		outsideDir := t.TempDir()
		outsideFile := filepath.Join(outsideDir, "model.gguf")
		if err := os.WriteFile(outsideFile, sentinel, 0o644); err != nil {
			t.Fatalf("WriteFile sentinel: %v", err)
		}
		outDir := t.TempDir()
		if err := os.Symlink(outsideDir, filepath.Join(outDir, "weights")); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject symlinked parent directory escaping outDir")
		}
		got, err := os.ReadFile(outsideFile)
		if err != nil || !bytes.Equal(got, sentinel) {
			t.Fatalf("outside file was modified: err=%v content=%q", err, string(got))
		}
	})

	t.Run("dir_symlink_relative_outside", func(t *testing.T) {
		base := t.TempDir()
		outsideDir := filepath.Join(base, "outside")
		outDir := filepath.Join(base, "out")
		if err := os.MkdirAll(outsideDir, 0o755); err != nil {
			t.Fatalf("MkdirAll outside: %v", err)
		}
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatalf("MkdirAll out: %v", err)
		}
		outsideFile := filepath.Join(outsideDir, "model.gguf")
		if err := os.WriteFile(outsideFile, sentinel, 0o644); err != nil {
			t.Fatalf("WriteFile sentinel: %v", err)
		}
		if err := os.Symlink("../outside", filepath.Join(outDir, "weights")); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject relative symlink escaping outDir")
		}
		got, err := os.ReadFile(outsideFile)
		if err != nil || !bytes.Equal(got, sentinel) {
			t.Fatalf("outside file was modified: err=%v content=%q", err, string(got))
		}
	})

	t.Run("file_symlink_outside_existing_and_dangling", func(t *testing.T) {
		outsideDir := t.TempDir()
		outsideExisting := filepath.Join(outsideDir, "secret.bin")
		if err := os.WriteFile(outsideExisting, sentinel, 0o644); err != nil {
			t.Fatalf("WriteFile sentinel: %v", err)
		}
		outDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(outDir, "weights"), 0o755); err != nil {
			t.Fatalf("MkdirAll weights: %v", err)
		}
		if err := os.Symlink(outsideExisting, filepath.Join(outDir, "weights", "model.gguf")); err != nil {
			t.Fatalf("Symlink existing: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject symlinked artifact file pointing outside outDir")
		}
		got, err := os.ReadFile(outsideExisting)
		if err != nil || !bytes.Equal(got, sentinel) {
			t.Fatalf("outside existing file was modified: err=%v content=%q", err, string(got))
		}

		// Dangling symlink pointing to a non-existent file outside outDir.
		outDir2 := t.TempDir()
		if err := os.MkdirAll(filepath.Join(outDir2, "weights"), 0o755); err != nil {
			t.Fatalf("MkdirAll weights2: %v", err)
		}
		outsideNew := filepath.Join(outsideDir, "created-outside.bin")
		if err := os.Symlink(outsideNew, filepath.Join(outDir2, "weights", "model.gguf")); err != nil {
			t.Fatalf("Symlink dangling: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir2, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject dangling symlink pointing outside outDir")
		}
		if _, err := os.Stat(outsideNew); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dangling symlink target outside outDir must not be created, stat err=%v", err)
		}
	})

	t.Run("in_dir_symlink_rejected", func(t *testing.T) {
		outDir := t.TempDir()
		otherFile := filepath.Join(outDir, "other.txt")
		if err := os.WriteFile(otherFile, sentinel, 0o644); err != nil {
			t.Fatalf("WriteFile other: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(outDir, "weights"), 0o755); err != nil {
			t.Fatalf("MkdirAll weights: %v", err)
		}
		if err := os.Symlink("../other.txt", filepath.Join(outDir, "weights", "model.gguf")); err != nil {
			t.Fatalf("Symlink in-dir: %v", err)
		}
		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err == nil {
			t.Fatal("expected PullToDir to reject in-dir symlink")
		}
		got, err := os.ReadFile(otherFile)
		if err != nil || !bytes.Equal(got, sentinel) {
			t.Fatalf("in-dir symlink target was modified: err=%v content=%q", err, string(got))
		}
	})
}

type staticManifestProvider struct {
	id  string
	raw []byte
}

func (p *staticManifestProvider) ID() string     { return p.id }
func (p *staticManifestProvider) IsOrigin() bool { return false }
func (p *staticManifestProvider) HasChunk(context.Context, string) (bool, error) {
	return false, nil
}
func (p *staticManifestProvider) GetChunk(context.Context, string, int64) ([]byte, error) {
	return nil, ErrChunkNotFound
}
func (p *staticManifestProvider) GetManifest(context.Context, string) ([]byte, error) {
	return append([]byte(nil), p.raw...), nil
}
func (p *staticManifestProvider) Stats() ProviderStats { return ProviderStats{} }

// TestFetchVerifiedManifestFailoverAfterIntegrityFailure verifies that a
// corrupted or malicious mirror returning a matching ManifestCID JSON field
// with modified unsigned content or stripped/forged signatures does not abort
// failover before healthy mirrors are tried.
func TestFetchVerifiedManifestFailoverAfterIntegrityFailure(t *testing.T) {
	ctx := context.Background()
	m, honestRaw, pub, _, _, _ := buildTestBundle(t, ctx, 32<<10)

	cli := NewClient(ClientConfig{
		Policy: VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			License:     LicensePolicy{RequireLicense: true, AllowedLicenses: []string{"Apache-2.0"}},
		},
	})

	// Case 1: Mirror 1 modifies an unsigned field while keeping ManifestCID string equal.
	tamperedBody := *m
	tamperedBody.Description = "evil mirror modified unsigned field"
	tamperedBodyRaw, err := tamperedBody.MarshalJSONCanonical()
	if err != nil {
		t.Fatalf("MarshalJSONCanonical tamperedBody: %v", err)
	}
	badBodyMirror := &staticManifestProvider{id: "evil-body-mirror", raw: tamperedBodyRaw}
	honestMirror := &staticManifestProvider{id: "honest-mirror", raw: honestRaw}

	fetched, via, err := cli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{badBodyMirror, honestMirror})
	if err != nil {
		t.Fatalf("expected failover to honest mirror after unsigned-field tamper, got error: %v", err)
	}
	if via.ID() != "honest-mirror" || fetched.Description != m.Description {
		t.Fatalf("unexpected manifest/provider: via=%s desc=%q", via.ID(), fetched.Description)
	}

	// Case 2: Mirror 1 keeps exact unsigned body (matching computed CID) but strips signatures.
	strippedSigs := *m
	strippedSigs.Signatures = nil
	strippedSigsRaw, err := strippedSigs.MarshalJSONCanonical()
	if err != nil {
		t.Fatalf("MarshalJSONCanonical strippedSigs: %v", err)
	}
	badSigMirror := &staticManifestProvider{id: "evil-sig-mirror", raw: strippedSigsRaw}

	fetched, via, err = cli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{badSigMirror, honestMirror})
	if err != nil {
		t.Fatalf("expected failover to honest mirror after stripped signatures, got error: %v", err)
	}
	if via.ID() != "honest-mirror" || len(fetched.Signatures) == 0 {
		t.Fatalf("unexpected manifest/provider after sig failover: via=%s sigs=%d", via.ID(), len(fetched.Signatures))
	}

	// Case 3: All mirrors return tampered manifests -> still fail closed.
	if _, _, err := cli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{badBodyMirror}); !errors.Is(err, ErrAllProvidersFailed) || !errors.Is(err, ErrManifestCIDMismatch) {
		t.Fatalf("expected ErrAllProvidersFailed+ErrManifestCIDMismatch when all mirrors tampered, got %v", err)
	}

	// Case 4: Verified CID violating local license policy still fails closed with ErrLicenseRejected.
	strictCli := NewClient(ClientConfig{
		Policy: VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			License:     LicensePolicy{RequireLicense: true, AllowedLicenses: []string{"MIT"}},
		},
	})
	if _, _, err := strictCli.FetchVerifiedManifest(ctx, m.ManifestCID, []ChunkProvider{honestMirror}); !errors.Is(err, ErrLicenseRejected) {
		t.Fatalf("expected ErrLicenseRejected for policy-violating verified CID, got %v", err)
	}
}

// TestVerifyChecksParentCIDBeforeRecursion ensures an untrusted ParentResolver
// returning the signed child (or a spoofed/nil manifest) is rejected before
// recursing instead of overflowing the stack.
func TestVerifyChecksParentCIDBeforeRecursion(t *testing.T) {
	ctx := context.Background()
	parent, _, pub, priv, _, _ := buildTestBundle(t, ctx, 32<<10)

	child := *parent
	child.ModelID = "zen-7b-child"
	child.Version = "1.3.0"
	child.Provenance.ParentManifestCID = parent.ManifestCID
	child.ManifestCID = ""
	child.Signatures = nil
	if err := child.Sign(priv, "child-pub"); err != nil {
		t.Fatalf("Sign child: %v", err)
	}

	t.Run("resolver_returns_signed_child", func(t *testing.T) {
		calls := 0
		policy := VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			ParentResolver: func(cid string) (*ManifestV1, error) {
				calls++
				if calls > 2 {
					t.Fatalf("infinite recursion detected in parent verification (calls=%d)", calls)
				}
				return &child, nil
			},
		}
		err := child.Verify(policy)
		if !errors.Is(err, ErrProvenanceInvalid) {
			t.Fatalf("expected ErrProvenanceInvalid, got %v", err)
		}
		if calls != 1 {
			t.Fatalf("expected resolver to be called exactly once before rejecting mismatched parent CID, got %d", calls)
		}
	})

	t.Run("resolver_returns_spoofed_parent_cid", func(t *testing.T) {
		spoofed := child
		spoofed.ManifestCID = parent.ManifestCID
		calls := 0
		policy := VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			ParentResolver: func(cid string) (*ManifestV1, error) {
				calls++
				return &spoofed, nil
			},
		}
		err := child.Verify(policy)
		if !errors.Is(err, ErrProvenanceInvalid) {
			t.Fatalf("expected ErrProvenanceInvalid for spoofed parent CID, got %v", err)
		}
		if calls != 1 {
			t.Fatalf("expected exactly 1 resolver call for spoofed parent, got %d", calls)
		}
	})

	t.Run("resolver_returns_nil", func(t *testing.T) {
		policy := VerificationPolicy{
			TrustedKeys: []ed25519.PublicKey{pub},
			ParentResolver: func(cid string) (*ManifestV1, error) {
				return nil, nil
			},
		}
		if err := child.Verify(policy); !errors.Is(err, ErrProvenanceInvalid) {
			t.Fatalf("expected ErrProvenanceInvalid for nil parent, got %v", err)
		}
	})
}

// TestRefKeyPreservesDistinctReleases verifies distinct (modelID, version)
// tuples such as 1.0.0-rc.1 and 1.0.0-rc-1 never collide on backend keys.
func TestRefKeyPreservesDistinctReleases(t *testing.T) {
	ctx := context.Background()
	pairs := [][2]string{
		{"zen-7b", "1.0.0-rc.1"},
		{"zen-7b", "1.0.0-rc-1"},
		{"zen/7b", "1.0.0-rc.1"},
		{"Zen-7B", "1.0.0-rc.1"},
		{"zen-7b", "1.0.0-rc.10"},
	}
	seen := make(map[string][2]string, len(pairs))
	for _, p := range pairs {
		k, err := RefKey(p[0], p[1])
		if err != nil {
			t.Fatalf("RefKey(%q, %q) unexpected error: %v", p[0], p[1], err)
		}
		if err := backend.ValidKey(k); err != nil {
			t.Fatalf("RefKey(%q, %q) = %q failed backend.ValidKey: %v", p[0], p[1], k, err)
		}
		if prev, dup := seen[k]; dup {
			t.Fatalf("RefKey collision: (%q, %q) and (%q, %q) both produced %q",
				prev[0], prev[1], p[0], p[1], k)
		}
		seen[k] = p
	}

	// End-to-end: publishing 1.0.0-rc.1 and 1.0.0-rc-1 keeps both refs intact.
	pub, priv := mustKeyPair(t)
	_ = pub
	mem := storetest.NewMem()
	store := NewVFSStoreProvider("ref-store", true, mem)
	art, err := IngestArtifact(ctx, "weights/w.gguf", AssetKindGGUF, "Q4_K_M", bytes.NewReader(deterministicPayload(55, 4096)), 4096, store)
	if err != nil {
		t.Fatalf("IngestArtifact: %v", err)
	}
	makeRelease := func(ver string) *ManifestV1 {
		m := &ManifestV1{
			SchemaVersion: SchemaVersionV1,
			ModelID:       "zen-7b",
			Version:       ver,
			License:       "Apache-2.0",
			Provenance:    Provenance{CreatedAt: "2026-11-26T00:00:00Z"},
			Artifacts:     []Artifact{art},
		}
		if err := m.Sign(priv, "zen-release"); err != nil {
			t.Fatalf("Sign %s: %v", ver, err)
		}
		if _, err := PublishManifest(ctx, m, store); err != nil {
			t.Fatalf("PublishManifest %s: %v", ver, err)
		}
		return m
	}

	mDot := makeRelease("1.0.0-rc.1")
	mDash := makeRelease("1.0.0-rc-1")

	kDot, _ := RefKey("zen-7b", "1.0.0-rc.1")
	kDash, _ := RefKey("zen-7b", "1.0.0-rc-1")

	gotDot, _, err := mem.Get(ctx, kDot, backend.Unbounded)
	if err != nil {
		t.Fatalf("Get rc.1 ref: %v", err)
	}
	gotDash, _, err := mem.Get(ctx, kDash, backend.Unbounded)
	if err != nil {
		t.Fatalf("Get rc-1 ref: %v", err)
	}
	if string(gotDot) != mDot.ManifestCID {
		t.Fatalf("rc.1 ref was overwritten: got %s, want %s", string(gotDot), mDot.ManifestCID)
	}
	if string(gotDash) != mDash.ManifestCID {
		t.Fatalf("rc-1 ref mismatch: got %s, want %s", string(gotDash), mDash.ManifestCID)
	}
}

// TestSemVerPrereleaseNumericPrecedence verifies SemVer 2.0.0 prerelease
// comparison (numeric, non-numeric, and prefix rules) and policy bounds.
func TestSemVerPrereleaseNumericPrecedence(t *testing.T) {
	ordered := []string{
		"1.0.0-1",
		"1.0.0-2",
		"1.0.0-10",
		"1.0.0-alpha",
		"1.0.0-alpha.1",
		"1.0.0-alpha.2",
		"1.0.0-alpha.10",
		"1.0.0-alpha.beta",
		"1.0.0-beta",
		"1.0.0-beta.2",
		"1.0.0-beta.11",
		"1.0.0-rc.1",
		"1.0.0-rc.2",
		"1.0.0-rc.10",
		"1.0.0-rc.10.0",
		"1.0.0-rc.99999999999999999999999",
		"1.0.0",
	}
	parsed := make([]SemVer, len(ordered))
	for i, s := range ordered {
		v, err := ParseSemVer(s)
		if err != nil {
			t.Fatalf("ParseSemVer(%q): %v", s, err)
		}
		parsed[i] = v
	}
	for i := 0; i < len(parsed); i++ {
		if c := parsed[i].Compare(parsed[i]); c != 0 {
			t.Fatalf("%s == %s gave %d", ordered[i], ordered[i], c)
		}
		for j := i + 1; j < len(parsed); j++ {
			if c := parsed[i].Compare(parsed[j]); c != -1 {
				t.Fatalf("expected %s < %s (-1), got %d", ordered[i], ordered[j], c)
			}
			if c := parsed[j].Compare(parsed[i]); c != 1 {
				t.Fatalf("expected %s > %s (+1), got %d", ordered[j], ordered[i], c)
			}
		}
	}

	for _, bad := range []string{"1.0.0-", "1.0.0-rc..1", "1.0.0-.rc", "1.0.0-rc."} {
		if _, err := ParseSemVer(bad); err == nil {
			t.Fatalf("expected ParseSemVer(%q) to fail", bad)
		}
	}

	// Policy enforcement: rc.10 must be rejected when MaxVersion is rc.2.
	ctx := context.Background()
	base, _, pub, priv, _, _ := buildTestBundle(t, ctx, 32<<10)
	rc10 := *base
	rc10.Version = "1.0.0-rc.10"
	rc10.ManifestCID = ""
	rc10.Signatures = nil
	if err := rc10.Sign(priv, "zen-release"); err != nil {
		t.Fatalf("Sign rc10: %v", err)
	}
	maxPolicy := VerificationPolicy{
		TrustedKeys: []ed25519.PublicKey{pub},
		MaxVersion:  "1.0.0-rc.2",
	}
	if err := rc10.Verify(maxPolicy); !errors.Is(err, ErrVersionRejected) {
		t.Fatalf("expected rc.10 to be rejected by MaxVersion=1.0.0-rc.2, got %v", err)
	}
	minPolicy := VerificationPolicy{
		TrustedKeys: []ed25519.PublicKey{pub},
		MinVersion:  "1.0.0-rc.2",
		MaxVersion:  "1.0.0-rc.10",
	}
	if err := rc10.Verify(minPolicy); err != nil {
		t.Fatalf("expected rc.10 to pass [1.0.0-rc.2, 1.0.0-rc.10], got %v", err)
	}
}

// TestPullToDirDetachesHardLinkedArtifacts verifies that pre-existing hard links
// between two artifacts in outDir (or linking an artifact to a file outside
// outDir) are detached onto independent inodes so writing artifact B never
// overwrites artifact A or any external file.
func TestPullToDirDetachesHardLinkedArtifacts(t *testing.T) {
	ctx := context.Background()
	pub, priv := mustKeyPair(t)
	store := NewVFSStoreProvider("hardlink-origin", true, storetest.NewMem())

	payloadA := deterministicPayload(101, 48<<10)
	payloadB := deterministicPayload(202, 48<<10)
	artA, err := IngestArtifact(ctx, "weights/a.bin", AssetKindGGUF, "Q4_K_M", bytes.NewReader(payloadA), 16<<10, store)
	if err != nil {
		t.Fatalf("IngestArtifact A: %v", err)
	}
	artB, err := IngestArtifact(ctx, "weights/b.bin", AssetKindGGUF, "Q4_K_M", bytes.NewReader(payloadB), 16<<10, store)
	if err != nil {
		t.Fatalf("IngestArtifact B: %v", err)
	}
	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "zen-hardlink-test",
		Version:       "1.0.0",
		License:       "Apache-2.0",
		Provenance:    Provenance{CreatedAt: time.Now().UTC().Format(time.RFC3339)},
		Artifacts:     []Artifact{artA, artB},
	}
	if err := m.Sign(priv, "zen-release"); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	cfg := ClientConfig{}
	cfg.Policy = VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}}
	cli := NewClient(cfg)

	t.Run("two_artifacts_hardlinked_together", func(t *testing.T) {
		outDir := t.TempDir()
		weightsDir := filepath.Join(outDir, "weights")
		if err := os.MkdirAll(weightsDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		pathA := filepath.Join(weightsDir, "a.bin")
		pathB := filepath.Join(weightsDir, "b.bin")
		if err := os.WriteFile(pathA, []byte("preexisting-shared-inode"), 0o644); err != nil {
			t.Fatalf("WriteFile A: %v", err)
		}
		if err := os.Link(pathA, pathB); err != nil {
			t.Fatalf("Link A->B: %v", err)
		}

		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err != nil {
			t.Fatalf("PullToDir: %v", err)
		}

		fiA, err := os.Stat(pathA)
		if err != nil {
			t.Fatalf("Stat A: %v", err)
		}
		fiB, err := os.Stat(pathB)
		if err != nil {
			t.Fatalf("Stat B: %v", err)
		}
		if os.SameFile(fiA, fiB) {
			t.Fatal("expected artifact A and B to occupy independent inodes after PullToDir")
		}
		gotA, err := os.ReadFile(pathA)
		if err != nil {
			t.Fatalf("ReadFile A: %v", err)
		}
		gotB, err := os.ReadFile(pathB)
		if err != nil {
			t.Fatalf("ReadFile B: %v", err)
		}
		if !bytes.Equal(gotA, payloadA) {
			t.Fatalf("artifact A bytes corrupted (len=%d, want=%d)", len(gotA), len(payloadA))
		}
		if !bytes.Equal(gotB, payloadB) {
			t.Fatalf("artifact B bytes mismatch (len=%d, want=%d)", len(gotB), len(payloadB))
		}
	})

	t.Run("external_hardlink_untouched", func(t *testing.T) {
		base := t.TempDir()
		outsideFile := filepath.Join(base, "victim.txt")
		victimBytes := []byte("do-not-overwrite-outside")
		if err := os.WriteFile(outsideFile, victimBytes, 0o644); err != nil {
			t.Fatalf("WriteFile victim: %v", err)
		}
		outDir := filepath.Join(base, "out")
		weightsDir := filepath.Join(outDir, "weights")
		if err := os.MkdirAll(weightsDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.Link(outsideFile, filepath.Join(weightsDir, "a.bin")); err != nil {
			t.Fatalf("Link outside->a.bin: %v", err)
		}

		if _, err := cli.PullToDir(ctx, m, []ChunkProvider{store}, outDir, PullOptions{}); err != nil {
			t.Fatalf("PullToDir: %v", err)
		}
		gotVictim, err := os.ReadFile(outsideFile)
		if err != nil {
			t.Fatalf("ReadFile victim: %v", err)
		}
		if !bytes.Equal(gotVictim, victimBytes) {
			t.Fatalf("external hard-linked file was modified: got %q, want %q", gotVictim, victimBytes)
		}
		gotA, err := os.ReadFile(filepath.Join(weightsDir, "a.bin"))
		if err != nil {
			t.Fatalf("ReadFile a.bin: %v", err)
		}
		if !bytes.Equal(gotA, payloadA) {
			t.Fatalf("artifact A bytes mismatch")
		}
	})
}

// TestSemVerBuildMetadataDoesNotAffectPrecedence verifies SemVer 2.0.0 build
// metadata separation/validation and confirms +metadata never changes
// prerelease or policy version precedence.
func TestSemVerBuildMetadataDoesNotAffectPrecedence(t *testing.T) {
	vRC2Linux, err := ParseSemVer("1.0.0-rc.2+linux")
	if err != nil {
		t.Fatalf("ParseSemVer rc.2+linux: %v", err)
	}
	if vRC2Linux.Prerelease != "rc.2" || vRC2Linux.Build != "linux" {
		t.Fatalf("unexpected parsed fields: %+v", vRC2Linux)
	}
	vRC2Darwin, err := ParseSemVer("1.0.0-rc.2+darwin.arm64-2026")
	if err != nil {
		t.Fatalf("ParseSemVer rc.2+darwin: %v", err)
	}
	if vRC2Linux.Compare(vRC2Darwin) != 0 {
		t.Fatalf("build metadata must not affect precedence: got %d", vRC2Linux.Compare(vRC2Darwin))
	}
	vRC10, err := ParseSemVer("1.0.0-rc.10")
	if err != nil {
		t.Fatalf("ParseSemVer rc.10: %v", err)
	}
	if c := vRC2Linux.Compare(vRC10); c != -1 {
		t.Fatalf("expected 1.0.0-rc.2+linux < 1.0.0-rc.10 (-1), got %d", c)
	}

	// Build metadata containing hyphens on a release version must not be parsed as prerelease.
	vRelBuild, err := ParseSemVer("1.0.0+build-1")
	if err != nil {
		t.Fatalf("ParseSemVer 1.0.0+build-1: %v", err)
	}
	if vRelBuild.Prerelease != "" || vRelBuild.Build != "build-1" {
		t.Fatalf("unexpected release+build parse: %+v", vRelBuild)
	}
	vRel, _ := ParseSemVer("1.0.0")
	if vRelBuild.Compare(vRel) != 0 {
		t.Fatalf("expected 1.0.0+build-1 == 1.0.0, got %d", vRelBuild.Compare(vRel))
	}

	for _, bad := range []string{
		"1.0.0+",
		"1.0.0+a..b",
		"1.0.0+a+b",
		"1.0.0+a/b",
		"1.0.0-rc.2+",
		"1.0.0-rc.01",
		"01.0.0",
	} {
		if _, err := ParseSemVer(bad); err == nil {
			t.Fatalf("expected ParseSemVer(%q) to fail", bad)
		}
	}

	// Policy probe: signed 1.0.0-rc.2+linux must be rejected when MinVersion is 1.0.0-rc.10.
	ctx := context.Background()
	base, _, pub, priv, _, _ := buildTestBundle(t, ctx, 32<<10)
	older := *base
	older.Version = "1.0.0-rc.2+linux"
	older.ManifestCID = ""
	older.Signatures = nil
	if err := older.Sign(priv, "zen-release"); err != nil {
		t.Fatalf("Sign older: %v", err)
	}
	if err := older.Verify(VerificationPolicy{
		TrustedKeys: []ed25519.PublicKey{pub},
		MinVersion:  "1.0.0-rc.10",
	}); !errors.Is(err, ErrVersionRejected) {
		t.Fatalf("expected 1.0.0-rc.2+linux to be rejected by MinVersion=1.0.0-rc.10, got %v", err)
	}
	if err := older.Verify(VerificationPolicy{
		TrustedKeys: []ed25519.PublicKey{pub},
		MinVersion:  "1.0.0-rc.2",
		MaxVersion:  "1.0.0-rc.2",
	}); err != nil {
		t.Fatalf("expected 1.0.0-rc.2+linux to satisfy [1.0.0-rc.2, 1.0.0-rc.2], got %v", err)
	}
}

// TestHTTPMirrorRelayUnboundedRead verifies that HTTPMirrorProvider.GetChunk
// does not overflow io.LimitReader when called with backend.Unbounded (such as
// when one HTTPMirrorServer relays requests from an upstream HTTPMirrorProvider).
func TestHTTPMirrorRelayUnboundedRead(t *testing.T) {
	ctx := context.Background()
	m, _, pub, _, store, _ := buildTestBundle(t, ctx, 64<<10)

	upstream, err := StartHTTPMirror("upstream-http", true, store)
	if err != nil {
		t.Fatalf("StartHTTPMirror upstream: %v", err)
	}
	defer upstream.Close()

	// Direct unbounded read against HTTPMirrorProvider.
	firstChunk := m.Artifacts[0].Chunks[0]
	gotChunk, err := upstream.Client().GetChunk(ctx, firstChunk.BLAKE3, backend.Unbounded)
	if err != nil {
		t.Fatalf("GetChunk(Unbounded): %v", err)
	}
	if int64(len(gotChunk)) != firstChunk.Size {
		t.Fatalf("GetChunk(Unbounded) len=%d, want %d", len(gotChunk), firstChunk.Size)
	}

	// Relay HTTPMirrorServer backed by upstream HTTPMirrorProvider.
	relay, err := StartHTTPMirror("relay-http", false, upstream.Client())
	if err != nil {
		t.Fatalf("StartHTTPMirror relay: %v", err)
	}
	defer relay.Close()

	cfg := ClientConfig{}
	cfg.Policy = VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}}
	cli := NewClient(cfg)

	var out bytes.Buffer
	rep, err := cli.PullArtifactStream(ctx, m, 0, []ChunkProvider{relay.Client()}, &out)
	if err != nil {
		t.Fatalf("PullArtifactStream via relay: %v", err)
	}
	if int64(out.Len()) != m.Artifacts[0].Size {
		t.Fatalf("relayed payload size mismatch: got %d bytes, want %d", out.Len(), m.Artifacts[0].Size)
	}
	if rep.ArtifactRootBLAKE3[m.Artifacts[0].Path] != m.Artifacts[0].RootBLAKE3 {
		t.Fatalf("relayed root BLAKE3 mismatch: got %s, want %s",
			rep.ArtifactRootBLAKE3[m.Artifacts[0].Path], m.Artifacts[0].RootBLAKE3)
	}
}

// TestExportBundleEscapesSpecialCharsInDir verifies that ExportBundle properly
// percent-encodes filesystem characters like #, ?, %, and spaces when opening
// its file:// backend so chunks are written inside dstDir/vfs-store rather
// than a truncated sibling path.
func TestExportBundleEscapesSpecialCharsInDir(t *testing.T) {
	ctx := context.Background()
	m, _, pub, _, store, _ := buildTestBundle(t, ctx, 48<<10)
	policy := VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}}

	base := t.TempDir()
	dstDir := filepath.Join(base, "bundle#one?two%20three")
	receipt, err := ExportBundle(ctx, m, policy, store, dstDir)
	if err != nil {
		t.Fatalf("ExportBundle: %v", err)
	}
	if !strings.HasPrefix(receipt.BackendURL, "file:///") {
		t.Fatalf("unexpected BackendURL %q", receipt.BackendURL)
	}
	// Ensure no truncated sibling directory "/.../bundle" was created.
	if _, err := os.Stat(filepath.Join(base, "bundle")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected truncated sibling path %q not to exist, stat err=%v", filepath.Join(base, "bundle"), err)
	}
	entries, err := os.ReadDir(filepath.Join(dstDir, "vfs-store"))
	if err != nil {
		t.Fatalf("ReadDir vfs-store: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected exported vfs-store inside dstDir to be non-empty")
	}

	// Re-open the exported backend using receipt.BackendURL and pull offline.
	b, err := backend.Open(ctx, receipt.BackendURL)
	if err != nil {
		t.Fatalf("backend.Open(%q): %v", receipt.BackendURL, err)
	}
	defer b.Close()
	offline := NewVFSStoreProvider("offline-special-dir", false, b)

	cfg := ClientConfig{}
	cfg.Policy = policy
	var out bytes.Buffer
	rep, err := NewClient(cfg).PullArtifactStream(ctx, m, 0, []ChunkProvider{offline}, &out)
	if err != nil {
		t.Fatalf("PullArtifactStream from escaped export: %v", err)
	}
	if int64(out.Len()) != m.Artifacts[0].Size {
		t.Fatalf("offline exported payload size mismatch: got %d bytes, want %d", out.Len(), m.Artifacts[0].Size)
	}
	if rep.ArtifactRootBLAKE3[m.Artifacts[0].Path] != m.Artifacts[0].RootBLAKE3 {
		t.Fatalf("offline exported root BLAKE3 mismatch")
	}
}

type delayedHashProvider struct {
	ChunkProvider
	delayHash string
	delay     time.Duration
}

func (p *delayedHashProvider) GetChunk(ctx context.Context, blake3Hex string, maxSize int64) ([]byte, error) {
	if blake3Hex == p.delayHash {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.ChunkProvider.GetChunk(ctx, blake3Hex, maxSize)
}

type firstWriteRecordingSink struct {
	start         time.Time
	firstWriteSet atomic.Bool
	firstWriteNs  atomic.Int64
}

func (s *firstWriteRecordingSink) Write(p []byte) (int, error) {
	if s.firstWriteSet.CompareAndSwap(false, true) {
		s.firstWriteNs.Store(time.Since(s.start).Nanoseconds())
	}
	return len(p), nil
}

// TestPullArtifactStreamTTFTWaitsForFirstInOrderWrite verifies that TTFT
// reflects when the first in-order verified bytes become available at the sink,
// not when an out-of-order later chunk arrives while chunk 0 is still delayed.
func TestPullArtifactStreamTTFTWaitsForFirstInOrderWrite(t *testing.T) {
	ctx := context.Background()
	pub, priv := mustKeyPair(t)
	store := NewVFSStoreProvider("ttft-origin", true, storetest.NewMem())

	const chunkSize int64 = 16 << 10
	art, err := IngestArtifact(
		ctx,
		"weights/ttft.gguf",
		AssetKindGGUF,
		"Q4_K_M",
		bytes.NewReader(deterministicPayload(404, int(chunkSize)*8)),
		chunkSize,
		store,
	)
	if err != nil {
		t.Fatalf("IngestArtifact: %v", err)
	}
	m := &ManifestV1{
		SchemaVersion: SchemaVersionV1,
		ModelID:       "zen-ttft-test",
		Version:       "1.0.0",
		License:       "Apache-2.0",
		Provenance:    Provenance{CreatedAt: time.Now().UTC().Format(time.RFC3339)},
		Artifacts:     []Artifact{art},
	}
	if err := m.Sign(priv, "zen-release"); err != nil {
		t.Fatalf("Sign: %v", err)
	}

	const delay = 120 * time.Millisecond
	delayed := &delayedHashProvider{
		ChunkProvider: store,
		delayHash:     art.Chunks[0].BLAKE3,
		delay:         delay,
	}

	cfg := ClientConfig{}
	cfg.Concurrency = 4
	cfg.Policy = VerificationPolicy{TrustedKeys: []ed25519.PublicKey{pub}}
	cli := NewClient(cfg)

	sink := &firstWriteRecordingSink{start: time.Now()}
	report, err := cli.PullArtifactStream(ctx, m, 0, []ChunkProvider{delayed}, sink)
	if err != nil {
		t.Fatalf("PullArtifactStream: %v", err)
	}
	firstWriteMs := float64(sink.firstWriteNs.Load()) / float64(time.Millisecond)
	if report.TTFTMs < 100 {
		t.Fatalf("TTFT=%.2fms is far below chunk-0 delay (%.0fms); out-of-order arrival leaked into TTFT",
			report.TTFTMs, float64(delay/time.Millisecond))
	}
	if report.TTFTMs+5 < firstWriteMs {
		t.Fatalf("TTFT=%.2fms preceded first sink write at %.2fms", report.TTFTMs, firstWriteMs)
	}
}

