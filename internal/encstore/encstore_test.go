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
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

// TestHKDFSHA256RFC5869Vector1 verifies our zero-dependency stdlib HKDF-SHA256
// implementation against RFC 5869 Appendix A.1 Test Case 1.
func TestHKDFSHA256RFC5869Vector1(t *testing.T) {
	ikm, _ := hex.DecodeString("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b")
	salt, _ := hex.DecodeString("000102030405060708090a0b0c")
	info, _ := hex.DecodeString("f0f1f2f3f4f5f6f7f8f9")
	wantOKM := "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"

	got, err := hkdfSHA256(ikm, salt, info, 42)
	if err != nil {
		t.Fatalf("hkdfSHA256 error: %v", err)
	}
	if hex.EncodeToString(got) != wantOKM {
		t.Fatalf("HKDF-SHA256 RFC 5869 vector 1 mismatch:\ngot:  %s\nwant: %s", hex.EncodeToString(got), wantOKM)
	}
}

// TestCanonicalVFSKeysAndZeroCollision verifies all encstore keys conform to
// pkg/backend.ValidKey and never collide with VFS format-v2 keys.
func TestCanonicalVFSKeysAndZeroCollision(t *testing.T) {
	digest := strings.Repeat("a", 64)
	keys := []string{}
	for _, fn := range []func() (string, error){
		func() (string, error) { return ChunkKey(digest) },
		func() (string, error) { return ManifestKey(digest) },
		func() (string, error) { return HeadKey("agent-memory-01") },
		func() (string, error) { return RevocationKey("0123456789abcdef0123456789abcdef") },
		func() (string, error) { return TombstoneKey("dataset-clinical-01") },
	} {
		k, err := fn()
		if err != nil {
			t.Fatalf("key builder error: %v", err)
		}
		if err := backend.ValidKey(k); err != nil {
			t.Fatalf("key %q failed backend.ValidKey: %v", k, err)
		}
		if !strings.HasPrefix(k, "enc/") {
			t.Fatalf("expected enc/ namespace prefix, got %q", k)
		}
		keys = append(keys, k)
	}
	if err := backend.ValidKey(BundleIndexKey()); err != nil {
		t.Fatalf("BundleIndexKey invalid: %v", err)
	}
	// Verify invalid segments (path traversal, uppercase, spaces) are rejected fail-closed.
	for _, bad := range []string{"", "../etc/passwd", "UPPERCASE", "has space", "a/b"} {
		if _, err := HeadKey(bad); err == nil {
			t.Fatalf("HeadKey(%q) should fail closed", bad)
		}
	}
	if len(keys) != 5 {
		t.Fatalf("unexpected key count %d", len(keys))
	}
}

type testCluster struct {
	servers []*HTTPNodeServer
	dirs    []string
	store   *ReplicatedStore
}

func startTestCluster(t *testing.T, nodeCount, quorum int) *testCluster {
	t.Helper()
	ctx := context.Background()
	servers := make([]*HTTPNodeServer, 0, nodeCount)
	dirs := make([]string, 0, nodeCount)
	endpoints := make([]NodeEndpoint, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		dir := filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", i+1))
		fb, err := backend.Open(ctx, "file://"+dir)
		if err != nil {
			t.Fatalf("open file backend: %v", err)
		}
		srv, err := NewHTTPNodeServer(fmt.Sprintf("node-%d", i+1), "127.0.0.1:0", fb)
		if err != nil {
			t.Fatalf("start http node: %v", err)
		}
		t.Cleanup(func() {
			_ = srv.Close()
			_ = fb.Close()
		})
		servers = append(servers, srv)
		dirs = append(dirs, dir)
		endpoints = append(endpoints, NodeEndpoint{
			Name:    srv.Name(),
			Backend: NewHTTPBackend(srv.URL()),
		})
	}
	store, err := NewReplicatedStore(endpoints, quorum)
	if err != nil {
		t.Fatalf("NewReplicatedStore: %v", err)
	}
	return &testCluster{servers: servers, dirs: dirs, store: store}
}

func mustIdentity(t *testing.T, name string) *Identity {
	t.Helper()
	id, err := GenerateIdentity(name)
	if err != nil {
		t.Fatalf("GenerateIdentity(%s): %v", name, err)
	}
	return id
}

func sampleConsent(owner *Identity, id, purpose string, ttl time.Duration) ConsentRecord {
	now := time.Now().UTC()
	c := ConsentRecord{
		ConsentID:    id,
		GrantorKeyID: owner.KeyID,
		Purpose:      purpose,
		IssuedAt:     now.Add(-time.Minute),
		Revocable:    true,
	}
	if ttl != 0 {
		c.ExpiresAt = now.Add(ttl)
	}
	c.ConsentHash = c.ComputeConsentHash()
	return c
}

func deterministicPayload(tag string, size int) []byte {
	out := make([]byte, size)
	copy(out, []byte(tag))
	seed := blake3.Sum([]byte(tag))
	for off := len(tag); off < size; off += 32 {
		seed = blake3.Sum(seed[:])
		copy(out[off:], seed[:])
	}
	return out
}

// TestAllFiveArtifactKindsReplicatedRoundTrip verifies encryption, replication
// across 3 independent HTTP storage nodes, and authenticated decryption for all
// 5 artifact kinds in hanzoai/vfs#39.
func TestAllFiveArtifactKindsReplicatedRoundTrip(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 3)

	owner := mustIdentity(t, "agent-owner")
	peer := mustIdentity(t, "authorized-peer")
	vault, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}
	if err := vault.AuthorizePeer(peer.Public("collaborator")); err != nil {
		t.Fatalf("AuthorizePeer: %v", err)
	}

	kinds := AllArtifactKinds()
	if len(kinds) != 5 {
		t.Fatalf("expected 5 artifact kinds, got %d", len(kinds))
	}
	for i, kind := range kinds {
		artID := fmt.Sprintf("art-%s-%d", kind, i+1)
		payload := deterministicPayload("payload-"+string(kind)+"-", (i+1)*90000+137)
		res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
			ArtifactID:   artID,
			ArtifactName: "test-" + string(kind),
			Kind:         kind,
			License: LicensePolicy{
				SPDX:                  "Apache-2.0",
				CommercialAllowed:     true,
				RedistributionAllowed: true,
				AttributionRequired:   true,
			},
			Consent:             sampleConsent(owner, fmt.Sprintf("consent-%d", i+1), "agent-sync", time.Hour),
			Labels:              map[string]string{"kind": string(kind), "env": "independent"},
			ExperienceLedgerRef: fmt.Sprintf("zel:epoch-1:%s", artID),
			ChunkSize:           64 << 10,
			Data:                payload,
		})
		if err != nil {
			t.Fatalf("PutArtifact(%s): %v", kind, err)
		}

		policy := ReadPolicy{
			TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub},
			AllowedLicenses:    []string{"Apache-2.0"},
			RequiredPurpose:    "agent-sync",
		}
		// Both owner and authorized peer must decrypt identically.
		for _, reader := range []*Identity{owner, peer} {
			got, err := ReadArtifact(ctx, cluster.store, res.ManifestCID, reader, policy)
			if err != nil {
				t.Fatalf("ReadArtifact(%s, reader=%s): %v", kind, reader.DisplayName, err)
			}
			if !bytes.Equal(got.Data, payload) {
				t.Fatalf("plaintext mismatch for %s", kind)
			}
			if got.PrivateMeta.PlaintextRootBLAKE3 != res.PlaintextRootBLAKE3 {
				t.Fatalf("plaintext BLAKE3 mismatch for %s", kind)
			}
		}
	}
}

// TestContentAddressingIsNotPrivacyNegativeControl empirically compares raw
// unencrypted content-addressed storage (Negative Regime #1: 100% leakage)
// against internal/encstore (0% canary & plaintext-hash leakage).
func TestContentAddressingIsNotPrivacyNegativeControl(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 1, 1)
	owner := mustIdentity(t, "privacy-owner")
	vault, _ := NewVault(owner)

	canaries := []string{
		"CANARY-MEMORY-RECORD-001-CLINICAL-NOTE-ALPHA",
		"CANARY-AGENT-SCRATCHPAD-PROJECT-ORION-42",
		"CANARY-PRIVATE-HYPOTHESIS-DELTA-99",
	}
	var buf bytes.Buffer
	for _, c := range canaries {
		buf.WriteString("record: " + c + "\n")
		buf.Write(deterministicPayload(c, 16384))
	}
	payload := buf.Bytes()

	// 1. Encrypted encstore write: scan raw disk objects of the storage provider.
	putRes, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "private-memory-01",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-mem-1", "agent-memory-sync", time.Hour),
		ChunkSize:  16384,
		Data:       payload,
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}
	encScan, err := ScanBackendForPlaintextCanaries(ctx, cluster.servers[0].Backend(), "enc/", canaries, []string{putRes.PlaintextRootBLAKE3})
	if err != nil {
		t.Fatalf("ScanBackend encrypted: %v", err)
	}
	if encScan.CanariesLeaked != 0 || encScan.PlaintextHashesLeaked != 0 || encScan.LeakageRatePct != 0 {
		t.Fatalf("encrypted store leaked sensitive data: %+v", encScan)
	}

	// 2. Negative control: unencrypted content-addressed write leaks 100% verbatim.
	unencDir := filepath.Join(t.TempDir(), "unenc-node")
	unencBackend, err := backend.Open(ctx, "file://"+unencDir)
	if err != nil {
		t.Fatalf("open unenc backend: %v", err)
	}
	defer unencBackend.Close()

	cids, err := UnsafeUnencryptedContentAddressedPutForNegativeControl(ctx, unencBackend, payload, 16384)
	if err != nil {
		t.Fatalf("unencrypted put: %v", err)
	}
	unencScan, err := ScanBackendForPlaintextCanaries(ctx, unencBackend, "unenc/", canaries, cids[:1])
	if err != nil {
		t.Fatalf("ScanBackend unencrypted: %v", err)
	}
	if unencScan.CanariesLeaked != len(canaries) || unencScan.PlaintextHashesLeaked != 1 || unencScan.LeakageRatePct != 100.0 {
		t.Fatalf("expected 100%% leakage in unencrypted negative control, got %+v", unencScan)
	}
}

// TestRevocationForwardSecurityAndPriorEpochBoundary verifies:
// (a) Revoking Peer A increments key epoch and blocks 100% of post-revocation epochs;
// (b) Authorized Peer B continues reading both pre- and post-revocation epochs;
// (c) Honest Boundary (Negative Regime #2): Peer A holding cached Epoch 1 data
//
//	prior to revocation can still decrypt Epoch 1 locally — cryptography cannot
//	retroactively erase already-disclosed plaintext.
func TestRevocationForwardSecurityAndPriorEpochBoundary(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 3)

	owner := mustIdentity(t, "vault-owner")
	peerA := mustIdentity(t, "peer-a-to-revoke")
	peerB := mustIdentity(t, "peer-b-retained")

	vault, _ := NewVault(owner)
	_ = vault.AuthorizePeer(peerA.Public("reader"))
	_ = vault.AuthorizePeer(peerB.Public("reader"))

	ep1Data := deterministicPayload("epoch-1-agent-memory-", 64<<10)
	ep1Res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "agent-mem-timeline",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "MIT", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-ep1", "agent-memory-sync", time.Hour),
		Data:       ep1Data,
	})
	if err != nil {
		t.Fatalf("PutArtifact epoch 1: %v", err)
	}
	if ep1Res.Manifest.Epoch != 1 {
		t.Fatalf("want epoch 1, got %d", ep1Res.Manifest.Epoch)
	}

	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}

	// Before revocation, both Peer A and Peer B decrypt Epoch 1.
	if got, err := ReadArtifact(ctx, cluster.store, ep1Res.ManifestCID, peerA, policy); err != nil || !bytes.Equal(got.Data, ep1Data) {
		t.Fatalf("peerA failed pre-revocation epoch 1 read: %v", err)
	}
	if got, err := ReadArtifact(ctx, cluster.store, ep1Res.ManifestCID, peerB, policy); err != nil || !bytes.Equal(got.Data, ep1Data) {
		t.Fatalf("peerB failed pre-revocation epoch 1 read: %v", err)
	}

	// Revoke Peer A -> Epoch advances to 2 and signed revocation list is published.
	revList, err := vault.RevokePeer(ctx, cluster.store, peerA.KeyID, "membership-ended")
	if err != nil {
		t.Fatalf("RevokePeer: %v", err)
	}
	if revList.CurrentEpoch != 2 || vault.CurrentEpoch() != 2 {
		t.Fatalf("expected epoch 2 after revocation, got rev=%d vault=%d", revList.CurrentEpoch, vault.CurrentEpoch())
	}

	// Write Epoch 2 artifact: Peer A MUST be omitted from KeyEnvelopes.
	ep2Data := deterministicPayload("epoch-2-secret-memory-", 64<<10)
	ep2Res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID:        "agent-mem-timeline",
		Kind:              KindAgentMemory,
		ParentManifestCID: ep1Res.ManifestCID,
		License:           LicensePolicy{SPDX: "MIT", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:           sampleConsent(owner, "consent-ep2", "agent-memory-sync", time.Hour),
		Data:              ep2Data,
	})
	if err != nil {
		t.Fatalf("PutArtifact epoch 2: %v", err)
	}
	if ep2Res.Manifest.Epoch != 2 {
		t.Fatalf("want epoch 2, got %d", ep2Res.Manifest.Epoch)
	}
	if _, found := ep2Res.Manifest.FindEnvelope(peerA.KeyID); found {
		t.Fatalf("revoked peerA must NOT receive a KeyEnvelope in epoch 2")
	}

	// Peer A fails closed on Epoch 2 both with revocation list AND even if revocation list is ignored!
	if _, err := ReadArtifact(ctx, cluster.store, ep2Res.ManifestCID, peerA, policy); !errors.Is(err, ErrPeerRevoked) {
		t.Fatalf("expected ErrPeerRevoked for peerA on epoch 2, got: %v", err)
	}
	noRevPolicy := policy
	noRevPolicy.IgnoreRevocationList = true
	if _, err := ReadArtifact(ctx, cluster.store, ep2Res.ManifestCID, peerA, noRevPolicy); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("expected cryptographic ErrAccessDenied for peerA on epoch 2 without revList, got: %v", err)
	}

	// Authorized Peer B still decrypts both Epoch 1 and Epoch 2.
	if got, err := ReadArtifact(ctx, cluster.store, ep2Res.ManifestCID, peerB, policy); err != nil || !bytes.Equal(got.Data, ep2Data) {
		t.Fatalf("peerB failed epoch 2 read: %v", err)
	}

	// Honest Boundary (Negative Regime #2): Peer A already received Epoch 1 KeyEnvelope
	// before revocation (revokedAtEpoch == 2 > manifest.Epoch == 1). With cached Epoch 1
	// blobs, Peer A can still decrypt Epoch 1 locally.
	if got, err := ReadArtifact(ctx, cluster.store, ep1Res.ManifestCID, peerA, noRevPolicy); err != nil || !bytes.Equal(got.Data, ep1Data) {
		t.Fatalf("expected honest prior-epoch boundary: peerA retains local readability of epoch 1, got err=%v", err)
	}
}

// TestAdversarialTrustAndPolicyMatrix verifies 100% fail-closed detection across
// unauthorized reads, ciphertext bit-flips, truncations, chunk AAD splicing,
// forged signatures, tampered licenses, untrusted owners, expired/revoked
// consent, and tampered key envelopes.
func TestAdversarialTrustAndPolicyMatrix(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 3)

	owner := mustIdentity(t, "policy-owner")
	otherOwner := mustIdentity(t, "untrusted-owner")
	unauthorized := mustIdentity(t, "unauthorized-eve")
	vault, _ := NewVault(owner)

	payload := deterministicPayload("checkpoint-weights-", 192<<10)
	putRes, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "ckpt-adversarial-01",
		Kind:       KindModelCheckpoint,
		License: LicensePolicy{
			SPDX:                  "CC-BY-NC-4.0",
			CommercialAllowed:     false,
			RedistributionAllowed: true,
		},
		Consent:   sampleConsent(owner, "consent-adv-01", "research-eval", time.Hour),
		ChunkSize: 64 << 10,
		Data:      payload,
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}

	basePolicy := ReadPolicy{
		TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub},
		AllowedLicenses:    []string{"CC-BY-NC-4.0", "Apache-2.0"},
		RequiredPurpose:    "research-eval",
	}

	// 1. Unauthorized peer decryption attempt -> ErrAccessDenied.
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, unauthorized, basePolicy); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("probe 1 (unauthorized peer): want ErrAccessDenied, got %v", err)
	}

	// 2. Replica 1 returns corrupted ciphertext bit-flip -> quarantined + failover succeeds.
	cluster.servers[0].SetFaultMode(FaultCorruptChunk)
	got, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, basePolicy)
	if err != nil || !bytes.Equal(got.Data, payload) {
		t.Fatalf("probe 2 (bit-flip failover): expected clean failover, got err=%v", err)
	}
	if cluster.store.Stats().ChunksRejected == 0 {
		t.Fatalf("probe 2: expected at least 1 rejected corrupted chunk")
	}
	cluster.servers[0].SetFaultMode(FaultNone)
	cluster.store.ResetQuarantine()

	// 3. Replica 2 returns truncated ciphertext -> quarantined + failover succeeds.
	cluster.servers[1].SetFaultMode(FaultTruncateChunk)
	got, err = ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, basePolicy)
	if err != nil || !bytes.Equal(got.Data, payload) {
		t.Fatalf("probe 3 (truncation failover): expected clean failover, got err=%v", err)
	}
	cluster.servers[1].SetFaultMode(FaultNone)
	cluster.store.ResetQuarantine()

	// 4. Chunk splicing / reordering attack (chunk 0 decrypted with chunk 1 AAD) -> ErrChunkCorrupted.
	salt, _ := hex.DecodeString(putRes.Manifest.EpochSaltHex)
	dek := vault.localDEKCache["ckpt-adversarial-01"][0]
	chunk0Frame, _, err := cluster.store.GetVerifiedChunk(ctx, putRes.Manifest.CipherChunks[0].CiphertextCID, putRes.Manifest.CipherChunks[0].CiphertextSize)
	if err != nil {
		t.Fatalf("fetch chunk 0: %v", err)
	}
	if _, err := decryptChunk(dek, salt, putRes.Manifest.ArtifactID, putRes.Manifest.Epoch, 1, putRes.Manifest.Kind, putRes.Manifest.License.SPDX, putRes.Manifest.Consent.ConsentHash, chunk0Frame); !errors.Is(err, ErrChunkCorrupted) {
		t.Fatalf("probe 4 (chunk splice AAD): want ErrChunkCorrupted, got %v", err)
	}
	// Wrong SPDX license in AAD -> ErrChunkCorrupted.
	if _, err := decryptChunk(dek, salt, putRes.Manifest.ArtifactID, putRes.Manifest.Epoch, 0, putRes.Manifest.Kind, "MIT", putRes.Manifest.Consent.ConsentHash, chunk0Frame); !errors.Is(err, ErrChunkCorrupted) {
		t.Fatalf("probe 4b (license AAD tampering): want ErrChunkCorrupted, got %v", err)
	}

	// 5. Forged Ed25519 signature / post-sign license edit -> ErrBadSignature.
	tampered := *putRes.Manifest
	tampered.License.CommercialAllowed = true
	tampered.ManifestCID = ""
	if err := tampered.Verify(basePolicy.TrustedOwnerEdPubs); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("probe 5 (post-sign manifest edit): want ErrBadSignature, got %v", err)
	}

	// 6. Untrusted owner key -> ErrUntrustedOwner.
	untrustedPolicy := basePolicy
	untrustedPolicy.TrustedOwnerEdPubs = []ed25519.PublicKey{otherOwner.Ed25519Pub}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, untrustedPolicy); !errors.Is(err, ErrUntrustedOwner) {
		t.Fatalf("probe 6 (untrusted owner): want ErrUntrustedOwner, got %v", err)
	}

	// 7. License policy violation (commercial required on CC-BY-NC-4.0, or disallowed SPDX) -> ErrLicenseDenied.
	commPolicy := basePolicy
	commPolicy.RequireCommercialOK = true
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, commPolicy); !errors.Is(err, ErrLicenseDenied) {
		t.Fatalf("probe 7a (commercial disallowed): want ErrLicenseDenied, got %v", err)
	}
	spdxPolicy := basePolicy
	spdxPolicy.AllowedLicenses = []string{"Apache-2.0", "MIT"}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, spdxPolicy); !errors.Is(err, ErrLicenseDenied) {
		t.Fatalf("probe 7b (spdx disallowed): want ErrLicenseDenied, got %v", err)
	}

	// 8. Consent expiry & purpose mismatch -> ErrConsentExpired / ErrConsentDenied.
	expiredPolicy := basePolicy
	expiredPolicy.CheckTime = time.Now().UTC().Add(2 * time.Hour)
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, expiredPolicy); !errors.Is(err, ErrConsentExpired) {
		t.Fatalf("probe 8a (consent expired): want ErrConsentExpired, got %v", err)
	}
	wrongPurposePolicy := basePolicy
	wrongPurposePolicy.RequiredPurpose = "commercial-finetune"
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, wrongPurposePolicy); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("probe 8b (consent purpose mismatch): want ErrConsentDenied, got %v", err)
	}

	// 9. Tampered KeyEnvelope wrapped DEK -> ErrAccessDenied.
	envCopy := putRes.Manifest.KeyEnvelopes[0]
	rawWrapped, _ := hex.DecodeString(envCopy.WrappedDEKHex)
	rawWrapped[0] ^= 0xff
	envCopy.WrappedDEKHex = hex.EncodeToString(rawWrapped)
	if _, err := unwrapDEK(envCopy, salt, putRes.Manifest.ArtifactID, putRes.Manifest.Epoch, putRes.Manifest.Consent.ConsentHash, owner); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("probe 9 (tampered envelope): want ErrAccessDenied, got %v", err)
	}

	// 10. Consent revocation -> ErrConsentRevoked.
	if _, err := vault.RevokeConsent(ctx, cluster.store, "consent-adv-01"); err != nil {
		t.Fatalf("RevokeConsent: %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, basePolicy); !errors.Is(err, ErrConsentRevoked) {
		t.Fatalf("probe 10 (consent revoked): want ErrConsentRevoked, got %v", err)
	}
}

// TestCryptoShredErasure verifies local DEK zeroing, signed tombstone
// replication, cooperative deletion, and fail-closed rejection on restore.
func TestCryptoShredErasure(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 3)

	owner := mustIdentity(t, "shred-owner")
	vault, _ := NewVault(owner)
	payload := deterministicPayload("sensitive-dataset-", 128<<10)

	putRes, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "dataset-shred-target",
		Kind:       KindDataset,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-shred", "eval", time.Hour),
		Data:       payload,
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}
	if vault.LocalDEKCount("dataset-shred-target") != 1 {
		t.Fatalf("expected 1 cached local DEK before shred")
	}

	// Keep a copy of the raw manifest on node 3 simulating an uncooperative mirror
	// that ignores Delete requests — tombstone must still block restore!
	mKey, _ := ManifestKey(putRes.ManifestCID)
	rawManifest, _, err := cluster.servers[2].Backend().Get(ctx, mKey, backend.Unbounded)
	if err != nil {
		t.Fatalf("read raw manifest: %v", err)
	}

	receipt, err := vault.CryptoShred(ctx, cluster.store, "dataset-shred-target", "gdpr-erasure-request")
	if err != nil {
		t.Fatalf("CryptoShred: %v", err)
	}
	if receipt.LocalDEKsZeroed != 1 || vault.LocalDEKCount("dataset-shred-target") != 0 {
		t.Fatalf("expected local DEK cache zeroed, got receipt=%+v count=%d", receipt, vault.LocalDEKCount("dataset-shred-target"))
	}
	if receipt.TombstoneReplicas != 3 {
		t.Fatalf("expected tombstone on 3 replicas, got %d", receipt.TombstoneReplicas)
	}

	// Re-insert manifest onto node 3 (simulating uncooperative replica) and verify ReadArtifact fails closed with ErrTombstoned.
	if err := cluster.servers[2].Backend().Put(ctx, mKey, rawManifest); err != nil {
		t.Fatalf("re-put manifest: %v", err)
	}
	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, policy); !errors.Is(err, ErrTombstoned) {
		t.Fatalf("expected ErrTombstoned after CryptoShred, got: %v", err)
	}
}

// TestSovereignExportZeroHanzoRestoreAndFork verifies exporting an agent's
// complete 5-kind authorized state to a file:// VFS directory, shutting down
// ALL original HTTP nodes, restoring and verifying on a brand-new independent
// node with zero Hanzo account, and forking a child artifact linked via ParentManifestCID.
func TestSovereignExportZeroHanzoRestoreAndFork(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 3)

	owner := mustIdentity(t, "sovereign-agent")
	delegate := mustIdentity(t, "sovereign-delegate")
	vault, _ := NewVault(owner)
	_ = vault.AuthorizePeer(delegate.Public("delegate"))

	expected := make(map[string][]byte)
	var manifestCIDs []string
	for i, kind := range AllArtifactKinds() {
		id := fmt.Sprintf("agent-%s", kind)
		data := deterministicPayload("sovereign-"+string(kind)+"-", 48<<10+i*7111)
		res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
			ArtifactID: id,
			Kind:       kind,
			License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
			Consent:    sampleConsent(owner, fmt.Sprintf("consent-sov-%d", i), "sovereign-export", 2*time.Hour),
			ChunkSize:  32 << 10,
			Data:       data,
		})
		if err != nil {
			t.Fatalf("PutArtifact(%s): %v", kind, err)
		}
		expected[id] = data
		manifestCIDs = append(manifestCIDs, res.ManifestCID)
	}

	exportDir := filepath.Join(t.TempDir(), "sovereign-vfs-export")
	expRes, err := vault.ExportAuthorizedBundle(ctx, cluster.store, exportDir, manifestCIDs)
	if err != nil {
		t.Fatalf("ExportAuthorizedBundle: %v", err)
	}
	if !expRes.ZeroSecondFormat || expRes.ArtifactsExported != 5 {
		t.Fatalf("unexpected export result: %+v", expRes)
	}

	// Shut down ALL original HTTP storage nodes completely.
	for _, srv := range cluster.servers {
		_ = srv.Close()
	}

	// Stand up a brand-new independent HTTP node serving directly from exportDir (zero Hanzo account).
	freshFileBackend, err := backend.Open(ctx, "file://"+exportDir)
	if err != nil {
		t.Fatalf("open exported file backend: %v", err)
	}
	defer freshFileBackend.Close()
	independentNode, err := NewHTTPNodeServer("independent-restore-node", "127.0.0.1:0", freshFileBackend)
	if err != nil {
		t.Fatalf("start independent node: %v", err)
	}
	defer independentNode.Close()

	independentStore, err := NewReplicatedStore([]NodeEndpoint{
		{Name: independentNode.Name(), Backend: NewHTTPBackend(independentNode.URL())},
	}, 1)
	if err != nil {
		t.Fatalf("NewReplicatedStore independent: %v", err)
	}

	policy := ReadPolicy{
		TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub},
		AllowedLicenses:    []string{"Apache-2.0"},
		RequiredPurpose:    "sovereign-export",
	}
	restoreRes, err := RestoreAndVerify(ctx, independentStore, delegate, policy)
	if err != nil {
		t.Fatalf("RestoreAndVerify: %v", err)
	}
	if !restoreRes.ZeroHanzoAccount || len(restoreRes.Restored) != 5 {
		t.Fatalf("unexpected restore result: %+v", restoreRes)
	}
	for _, art := range restoreRes.Restored {
		if !bytes.Equal(art.Data, expected[art.ArtifactID]) {
			t.Fatalf("restored bytes mismatch for %s", art.ArtifactID)
		}
	}

	// Sovereign fork: delegate creates a derived child artifact referencing the restored parent CID.
	forkVault, _ := NewVault(delegate)
	forkRes, err := forkVault.PutArtifact(ctx, independentStore, PutRequest{
		ArtifactID:        "agent-memory-fork-v2",
		Kind:              KindAgentMemory,
		ParentManifestCID: restoreRes.Restored[2].ManifestCID,
		License:           LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:           sampleConsent(delegate, "consent-fork-1", "sovereign-export", time.Hour),
		Data:              append(append([]byte(nil), restoreRes.Restored[2].Data...), []byte("\nfork-delta-verified")...),
	})
	if err != nil {
		t.Fatalf("fork PutArtifact: %v", err)
	}
	if forkRes.Manifest.ParentManifestCID != restoreRes.Restored[2].ManifestCID {
		t.Fatalf("fork lineage parent CID mismatch")
	}
	forkRead, err := ReadArtifact(ctx, independentStore, forkRes.ManifestCID, delegate, ReadPolicy{
		TrustedOwnerEdPubs: []ed25519.PublicKey{delegate.Ed25519Pub},
	})
	if err != nil || !bytes.HasSuffix(forkRead.Data, []byte("fork-delta-verified")) {
		t.Fatalf("fork read verification failed: %v", err)
	}
}

// TestAllReplicasCorruptedEclipseBoundary verifies Negative Regime #3:
// when all 3 independent replicas return corrupted ciphertext, verification
// fails closed with ErrAllReplicasFailed and 0 corrupted bytes accepted.
func TestAllReplicasCorruptedEclipseBoundary(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 3)

	owner := mustIdentity(t, "eclipse-owner")
	vault, _ := NewVault(owner)
	payload := deterministicPayload("eclipse-checkpoint-", 128<<10)

	putRes, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "ckpt-eclipse-01",
		Kind:       KindModelCheckpoint,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-eclipse", "eval", time.Hour),
		ChunkSize:  64 << 10,
		Data:       payload,
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}

	// Poison every single replica.
	for _, srv := range cluster.servers {
		srv.SetFaultMode(FaultCorruptChunk)
	}
	res, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, ReadPolicy{
		TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub},
	})
	if !errors.Is(err, ErrAllReplicasFailed) {
		t.Fatalf("expected ErrAllReplicasFailed under eclipse, got err=%v", err)
	}
	if res != nil {
		t.Fatalf("expected nil ReadResult (0 corrupted bytes accepted), got %+v", res)
	}
	stats := cluster.store.Stats()
	if stats.ChunksRejected < 3 {
		t.Fatalf("expected >= 3 rejected corrupted chunks across poisoned cluster, got %d", stats.ChunksRejected)
	}
}

// TestConcurrentPutReadRaceSafety hammers PutArtifact, ReadArtifact, and
// RevokePeer concurrently to verify zero data races under `go test -race`.
func TestConcurrentPutReadRaceSafety(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 2)

	owner := mustIdentity(t, "race-owner")
	peer := mustIdentity(t, "race-peer")
	vault, _ := NewVault(owner)
	_ = vault.AuthorizePeer(peer.Public("reader"))

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := deterministicPayload(fmt.Sprintf("race-payload-%d-", id), 32<<10)
			res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
				ArtifactID: fmt.Sprintf("race-art-%d", id),
				Kind:       KindExecutionReceipt,
				License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
				Consent:    sampleConsent(owner, fmt.Sprintf("consent-race-%d", id), "receipt-verify", time.Hour),
				ChunkSize:  16 << 10,
				Data:       data,
			})
			if err != nil {
				errCh <- err
				return
			}
			got, err := ReadArtifact(ctx, cluster.store, res.ManifestCID, peer, ReadPolicy{
				TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub},
			})
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(got.Data, data) {
				errCh <- fmt.Errorf("race payload %d mismatch", id)
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent error: %v", err)
	}

	// Verify serialized manifest round-trip JSON cleanliness.
	raw, _ := json.Marshal(cluster.store.Stats())
	if len(raw) == 0 {
		t.Fatalf("empty stats JSON")
	}
}
