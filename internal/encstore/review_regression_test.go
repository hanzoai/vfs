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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

// memBackend is a lightweight, thread-safe in-memory backend.Backend with
// atomic PutIfAbsent / PutIfVersion semantics used by regression tests.
type memBackend struct {
	mu   sync.RWMutex
	objs map[string][]byte
	mods map[string]time.Time
}

func newMemBackend() *memBackend {
	return &memBackend{
		objs: make(map[string][]byte),
		mods: make(map[string]time.Time),
	}
}

func memVersion(data []byte) backend.Version {
	sum := blake3.Sum(data)
	return backend.Version(hex.EncodeToString(sum[:]))
}

func (m *memBackend) Get(_ context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if err := backend.ValidKey(key); err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	raw, ok := m.objs[key]
	if !ok {
		return nil, "", backend.ErrNotFound
	}
	if max != backend.Unbounded && int64(len(raw)) > max {
		return nil, "", backend.ErrTooLarge
	}
	cp := append([]byte(nil), raw...)
	return cp, memVersion(cp), nil
}

func (m *memBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	raw, _, err := m.Get(ctx, key, backend.Unbounded)
	if err != nil {
		return nil, err
	}
	if off < 0 || n < 0 || off+n > int64(len(raw)) {
		return nil, fmt.Errorf("mem backend range [%d:%d] out of bounds (%d)", off, off+n, len(raw))
	}
	return append([]byte(nil), raw[off:off+n]...), nil
}

func (m *memBackend) Put(_ context.Context, key string, data []byte) error {
	if err := backend.ValidKey(key); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = append([]byte(nil), data...)
	m.mods[key] = time.Now().UTC()
	return nil
}

func (m *memBackend) PutIfAbsent(_ context.Context, key string, data []byte) (backend.Version, error) {
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.objs[key]; exists {
		return "", backend.ErrConflict
	}
	cp := append([]byte(nil), data...)
	m.objs[key] = cp
	m.mods[key] = time.Now().UTC()
	return memVersion(cp), nil
}

func (m *memBackend) PutIfVersion(_ context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.objs[key]
	if !exists {
		return "", backend.ErrConflict
	}
	if memVersion(cur) != v {
		return "", backend.ErrConflict
	}
	cp := append([]byte(nil), data...)
	m.objs[key] = cp
	m.mods[key] = time.Now().UTC()
	return memVersion(cp), nil
}

func (m *memBackend) Delete(_ context.Context, key string) error {
	if err := backend.ValidKey(key); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	delete(m.mods, key)
	return nil
}

func (m *memBackend) List(_ context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	objCh := make(chan backend.Object, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(objCh)
		defer close(errCh)
		if err := backend.ValidKeyPrefix(prefix); err != nil {
			errCh <- err
			return
		}
		m.mu.RLock()
		keys := make([]string, 0, len(m.objs))
		for k := range m.objs {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		objs := make([]backend.Object, 0, len(keys))
		for _, k := range keys {
			objs = append(objs, backend.Object{
				Key:      k,
				Size:     int64(len(m.objs[k])),
				Modified: m.mods[k],
			})
		}
		m.mu.RUnlock()
		for _, o := range objs {
			objCh <- o
		}
	}()
	return objCh, errCh
}

func (m *memBackend) Stat(_ context.Context, key string) (int64, error) {
	if err := backend.ValidKey(key); err != nil {
		return 0, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	raw, ok := m.objs[key]
	if !ok {
		return 0, backend.ErrNotFound
	}
	return int64(len(raw)), nil
}

func (m *memBackend) Close() error   { return nil }
func (m *memBackend) String() string { return "mem://test" }

// blockingPutBackend wraps a backend.Backend and blocks the first conditional
// write on targetPrefix until release is closed, signaling started when blocked.
type blockingPutBackend struct {
	backend.Backend
	targetPrefix string
	started      chan struct{}
	release      chan struct{}
	once         sync.Once
}

func (b *blockingPutBackend) maybeBlock(key string) {
	if strings.HasPrefix(key, b.targetPrefix) {
		b.once.Do(func() {
			close(b.started)
			<-b.release
		})
	}
}

func (b *blockingPutBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	b.maybeBlock(key)
	return b.Backend.PutIfAbsent(ctx, key, data)
}

func (b *blockingPutBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	b.maybeBlock(key)
	return b.Backend.PutIfVersion(ctx, key, data, v)
}

// Regression Test #1 (provider.go:818):
// Fail closed when all copies of an existing revocation list or tombstone are
// malformed, invalidly signed, or unreadable.
func TestPolicyRecordVerificationFailsClosed(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 2, 2)

	owner := mustIdentity(t, "policy-owner")
	otherOwner := mustIdentity(t, "forging-owner")
	vault, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}

	payload := deterministicPayload("policy-fail-closed-", 32<<10)
	putRes, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-policy-fail-closed",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-rev-check", "agent-sync", time.Hour),
		Data:       payload,
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}
	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}

	// Revoke consent so the artifact is denied.
	revList, err := vault.RevokeConsent(ctx, cluster.store, "consent-rev-check")
	if err != nil {
		t.Fatalf("RevokeConsent: %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, policy); !errors.Is(err, ErrConsentRevoked) {
		t.Fatalf("want ErrConsentRevoked, got %v", err)
	}

	revKey, _ := RevocationKey(owner.KeyID)
	validRevBytes, _ := json.Marshal(revList)

	// Subcase 1a: Malformed JSON on all replicas must fail closed (never return nil, nil).
	for _, srv := range cluster.servers {
		if err := srv.Backend().Put(ctx, revKey, []byte("{malformed-json")); err != nil {
			t.Fatalf("corrupt rev list: %v", err)
		}
	}
	if _, err := cluster.store.GetLatestRevocationList(ctx, owner.KeyID, owner.Ed25519Pub); !errors.Is(err, ErrAllReplicasFailed) {
		t.Fatalf("malformed revocation list: want ErrAllReplicasFailed, got %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, policy); err == nil {
		t.Fatalf("ReadArtifact must fail closed when all revocation lists are malformed")
	}

	// Subcase 1b: Invalidly signed revocation list on all replicas must fail closed.
	forgedRev := *revList
	forgedRev.SignatureHex = strings.Repeat("00", ed25519.SignatureSize)
	forgedRevBytes, _ := json.Marshal(&forgedRev)
	for _, srv := range cluster.servers {
		_ = srv.Backend().Put(ctx, revKey, forgedRevBytes)
	}
	if _, err := cluster.store.GetLatestRevocationList(ctx, owner.KeyID, owner.Ed25519Pub); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("forged revocation list: want ErrBadSignature, got %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, policy); err == nil {
		t.Fatalf("ReadArtifact must fail closed when all revocation lists have invalid signatures")
	}

	// Subcase 1c: Failover succeeds when replica 0 is malformed and replica 1 is valid.
	_ = cluster.servers[1].Backend().Put(ctx, revKey, validRevBytes)
	gotRev, err := cluster.store.GetLatestRevocationList(ctx, owner.KeyID, owner.Ed25519Pub)
	if err != nil || gotRev == nil || !gotRev.IsConsentRevoked("consent-rev-check") {
		t.Fatalf("expected valid failover revocation list, got rev=%+v err=%v", gotRev, err)
	}

	// Subcase 1d: Unreadable backend (all nodes offline) must fail closed on revocation lookup.
	for _, srv := range cluster.servers {
		srv.SetFaultMode(FaultOffline)
	}
	if _, err := cluster.store.GetLatestRevocationList(ctx, owner.KeyID, owner.Ed25519Pub); !errors.Is(err, ErrAllReplicasFailed) {
		t.Fatalf("offline revocation list: want ErrAllReplicasFailed, got %v", err)
	}
	for _, srv := range cluster.servers {
		srv.SetFaultMode(FaultNone)
	}

	// Subcase 1e: Corrupted / invalidly signed tombstones must also fail closed!
	// Clear revocation list so only tombstone guards the artifact.
	for _, srv := range cluster.servers {
		_ = srv.Backend().Delete(ctx, revKey)
	}
	tombKey, _ := TombstoneKey("art-policy-fail-closed")

	// Malformed tombstone on all replicas -> fail closed.
	for _, srv := range cluster.servers {
		_ = srv.Backend().Put(ctx, tombKey, []byte("{corrupt-tombstone"))
	}
	if _, err := cluster.store.GetTombstone(ctx, "art-policy-fail-closed", owner.Ed25519Pub); !errors.Is(err, ErrAllReplicasFailed) {
		t.Fatalf("malformed tombstone: want ErrAllReplicasFailed, got %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, policy); err == nil {
		t.Fatalf("ReadArtifact must fail closed when all tombstones are malformed")
	}

	// Tombstone signed by wrong owner -> fail closed.
	wrongTomb := &DeletionTombstone{
		Version:        FormatVersion,
		ArtifactID:     "art-policy-fail-closed",
		Owner:          otherOwner.Public("owner"),
		DeletedAtEpoch: 1,
		DeletedAt:      time.Now().UTC(),
		Reason:         "forged",
	}
	_ = wrongTomb.Sign(otherOwner)
	wrongTombBytes, _ := json.Marshal(wrongTomb)
	for _, srv := range cluster.servers {
		_ = srv.Backend().Put(ctx, tombKey, wrongTombBytes)
	}
	if _, err := cluster.store.GetTombstone(ctx, "art-policy-fail-closed", owner.Ed25519Pub); !errors.Is(err, ErrUntrustedOwner) {
		t.Fatalf("wrong-owner tombstone: want ErrUntrustedOwner, got %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, policy); err == nil {
		t.Fatalf("ReadArtifact must fail closed when all tombstones fail verification")
	}

	// Genuine absence (deleted from all replicas) -> returns (nil, nil) and ReadArtifact succeeds.
	for _, srv := range cluster.servers {
		_ = srv.Backend().Delete(ctx, tombKey)
	}
	if got, err := ReadArtifact(ctx, cluster.store, putRes.ManifestCID, owner, policy); err != nil || !bytes.Equal(got.Data, payload) {
		t.Fatalf("expected clean read on genuine policy absence, got err=%v", err)
	}
}

// Regression Test #2 (vault.go:71):
// Reopening an owner's vault with NewVault(owner) must load and preserve
// persisted revocations and epoch before allowing mutations or writes.
func TestReopenedVaultPreservesPersistedRevocations(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 3)

	owner := mustIdentity(t, "reopen-owner")
	peerA := mustIdentity(t, "reopen-peer-a")

	vault1, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault 1: %v", err)
	}
	_ = vault1.AuthorizePeer(peerA.Public("reader"))

	artA, err := vault1.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-consent-a",
		Kind:       KindDataset,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-a", "train", time.Hour),
		Data:       deterministicPayload("consent-a-", 16<<10),
	})
	if err != nil {
		t.Fatalf("PutArtifact A: %v", err)
	}
	artB, err := vault1.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-consent-b",
		Kind:       KindDataset,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-b", "train", time.Hour),
		Data:       deterministicPayload("consent-b-", 16<<10),
	})
	if err != nil {
		t.Fatalf("PutArtifact B: %v", err)
	}

	// Revoke consent A and peer A on vault1.
	if _, err := vault1.RevokeConsent(ctx, cluster.store, "consent-a"); err != nil {
		t.Fatalf("RevokeConsent A: %v", err)
	}
	if _, err := vault1.RevokePeer(ctx, cluster.store, peerA.KeyID, "revoked-in-v1"); err != nil {
		t.Fatalf("RevokePeer A: %v", err)
	}

	// Reopen a fresh Vault for the same owner and revoke consent B.
	vault2, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault 2: %v", err)
	}
	revList, err := vault2.RevokeConsent(ctx, cluster.store, "consent-b")
	if err != nil {
		t.Fatalf("vault2.RevokeConsent B: %v", err)
	}
	if revList.Seq != 3 {
		t.Fatalf("expected monotonic Seq=3 after reopening vault, got %d", revList.Seq)
	}
	if revList.CurrentEpoch != 2 || vault2.CurrentEpoch() != 2 {
		t.Fatalf("expected preserved epoch=2 after reopening vault, got rev=%d vault=%d", revList.CurrentEpoch, vault2.CurrentEpoch())
	}
	if !revList.IsConsentRevoked("consent-a") || !revList.IsConsentRevoked("consent-b") {
		t.Fatalf("expected both consent-a and consent-b revoked, got %v", revList.RevokedConsents)
	}
	if !revList.IsPeerRevoked(peerA.KeyID, 2) {
		t.Fatalf("expected peerA revocation preserved after reopening vault")
	}

	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}
	if _, err := ReadArtifact(ctx, cluster.store, artA.ManifestCID, owner, policy); !errors.Is(err, ErrConsentRevoked) {
		t.Fatalf("artifact A must remain ErrConsentRevoked after vault reopen, got %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, artB.ManifestCID, owner, policy); !errors.Is(err, ErrConsentRevoked) {
		t.Fatalf("artifact B must be ErrConsentRevoked, got %v", err)
	}

	// A reopened vault writing a new artifact must use epoch >= 2 (not reset to 1).
	vault3, _ := NewVault(owner)
	artC, err := vault3.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-reopen-epoch",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-c", "train", time.Hour),
		Data:       deterministicPayload("consent-c-", 8<<10),
	})
	if err != nil {
		t.Fatalf("vault3.PutArtifact: %v", err)
	}
	if artC.Manifest.Epoch < 2 {
		t.Fatalf("reopened vault wrote epoch %d, want >= 2", artC.Manifest.Epoch)
	}
}

// Regression Test #3 (vault.go:563):
// Prevent stale or delayed revocation publications from overwriting newer
// sequences (both overlapping calls and direct stale snapshots).
func TestStaleRevocationCannotOverwriteNewer(t *testing.T) {
	ctx := context.Background()
	owner := mustIdentity(t, "monotonic-owner")

	// Part A: Overlapping revocation calls from two Vault instances with the
	// first write delayed mid-flight while the second completes first.
	sharedMem := newMemBackend()
	started := make(chan struct{})
	release := make(chan struct{})
	delayedNode := &blockingPutBackend{
		Backend:      sharedMem,
		targetPrefix: "enc/revoke/",
		started:      started,
		release:      release,
	}

	storeDelayed, err := NewReplicatedStore([]NodeEndpoint{{Name: "delayed", Backend: delayedNode}}, 1)
	if err != nil {
		t.Fatalf("NewReplicatedStore delayed: %v", err)
	}
	storeFast, err := NewReplicatedStore([]NodeEndpoint{{Name: "fast", Backend: sharedMem}}, 1)
	if err != nil {
		t.Fatalf("NewReplicatedStore fast: %v", err)
	}

	v1, _ := NewVault(owner)
	v2, _ := NewVault(owner)

	errCh := make(chan error, 1)
	go func() {
		_, err := v1.RevokeConsent(ctx, storeDelayed, "consent-one")
		errCh <- err
	}()

	// Wait until v1 is blocked inside its first conditional PUT (Seq=1).
	<-started

	// While v1 is paused, v2 revokes consent-two and publishes Seq=1 first!
	if _, err := v2.RevokeConsent(ctx, storeFast, "consent-two"); err != nil {
		t.Fatalf("v2.RevokeConsent: %v", err)
	}

	// Release v1: its stale PutIfAbsent must lose CAS, reload consent-two, merge,
	// and publish Seq=2 containing BOTH consent-one and consent-two!
	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("v1.RevokeConsent after CAS retry: %v", err)
	}

	latest, err := storeFast.GetLatestRevocationList(ctx, owner.KeyID, owner.Ed25519Pub)
	if err != nil {
		t.Fatalf("GetLatestRevocationList: %v", err)
	}
	if latest == nil || latest.Seq != 2 {
		t.Fatalf("expected final Seq=2, got %+v", latest)
	}
	if !latest.IsConsentRevoked("consent-one") || !latest.IsConsentRevoked("consent-two") {
		t.Fatalf("expected both consents revoked after overlapping updates, got %v", latest.RevokedConsents)
	}

	// Part B: Direct stale publication (Seq=1) after Seq=2 is persisted MUST be
	// rejected with ErrStaleRevocation and leave Seq=2 untouched.
	stale := &SignedRevocationList{
		Version:         FormatVersion,
		Owner:           owner.Public("owner"),
		CurrentEpoch:    1,
		Seq:             1,
		RevokedConsents: []string{"consent-one"},
		UpdatedAt:       time.Now().UTC(),
	}
	if err := stale.Sign(owner); err != nil {
		t.Fatalf("sign stale: %v", err)
	}
	if _, err := storeFast.PutRevocationQuorum(ctx, owner.KeyID, owner.Ed25519Pub, stale); !errors.Is(err, ErrStaleRevocation) {
		t.Fatalf("expected ErrStaleRevocation for stale seq=1 write, got %v", err)
	}
	after, err := storeFast.GetLatestRevocationList(ctx, owner.KeyID, owner.Ed25519Pub)
	if err != nil || after.Seq != 2 || !after.IsConsentRevoked("consent-two") {
		t.Fatalf("stale write corrupted stored revocation list: %+v err=%v", after, err)
	}

	// Part C: Concurrent overlapping RevokeConsent calls on the SAME vault over HTTP.
	cluster := startTestCluster(t, 3, 3)
	sameVault, _ := NewVault(owner)
	var wg sync.WaitGroup
	const nConsents = 8
	errs := make(chan error, nConsents)
	for i := 0; i < nConsents; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := sameVault.RevokeConsent(ctx, cluster.store, fmt.Sprintf("consent-conc-%d", idx))
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent RevokeConsent error: %v", err)
	}
	finalRev, err := cluster.store.GetLatestRevocationList(ctx, owner.KeyID, owner.Ed25519Pub)
	if err != nil {
		t.Fatalf("GetLatestRevocationList concurrent: %v", err)
	}
	if finalRev == nil || len(finalRev.RevokedConsents) != nConsents || finalRev.Seq != nConsents {
		t.Fatalf("expected %d consents and seq=%d, got %+v", nConsents, nConsents, finalRev)
	}
}

// Regression Test #4 (vault.go:908):
// ExportAuthorizedBundle must reject tombstoned artifacts even when an
// uncooperative provider retained their encrypted manifest and chunks.
func TestExportRejectsTombstonedArtifacts(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 2, 2)

	owner := mustIdentity(t, "export-tomb-owner")
	vault, _ := NewVault(owner)

	liveRes, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-live-keep",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-live", "export", time.Hour),
		Data:       deterministicPayload("live-data-", 16<<10),
	})
	if err != nil {
		t.Fatalf("PutArtifact live: %v", err)
	}
	shredRes, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-shred-me",
		Kind:       KindDataset,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-shred-exp", "export", time.Hour),
		Data:       deterministicPayload("erased-data-", 16<<10),
	})
	if err != nil {
		t.Fatalf("PutArtifact shred: %v", err)
	}

	// Simulate an uncooperative provider retaining the encrypted manifest and
	// all ciphertext chunks across CryptoShred.
	mKey, _ := ManifestKey(shredRes.ManifestCID)
	retainedManifest, _, err := cluster.servers[0].Backend().Get(ctx, mKey, backend.Unbounded)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	retainedChunks := make(map[string][]byte, len(shredRes.Manifest.CipherChunks))
	for _, ref := range shredRes.Manifest.CipherChunks {
		cKey, _ := ChunkKey(ref.CiphertextCID)
		raw, _, err := cluster.servers[0].Backend().Get(ctx, cKey, backend.Unbounded)
		if err != nil {
			t.Fatalf("read chunk: %v", err)
		}
		retainedChunks[cKey] = raw
	}

	if _, err := vault.CryptoShred(ctx, cluster.store, "art-shred-me", "right-to-erasure"); err != nil {
		t.Fatalf("CryptoShred: %v", err)
	}

	// Restore retained ciphertext copy on node 0 while valid tombstone stays present.
	if err := cluster.servers[0].Backend().Put(ctx, mKey, retainedManifest); err != nil {
		t.Fatalf("re-put manifest: %v", err)
	}
	for cKey, raw := range retainedChunks {
		if err := cluster.servers[0].Backend().Put(ctx, cKey, raw); err != nil {
			t.Fatalf("re-put chunk: %v", err)
		}
	}

	// Attempt to export the erased artifact (alongside a live artifact).
	exportDir := filepath.Join(t.TempDir(), "blocked-export")
	if _, err := vault.ExportAuthorizedBundle(ctx, cluster.store, exportDir, []string{liveRes.ManifestCID, shredRes.ManifestCID}); !errors.Is(err, ErrTombstoned) {
		t.Fatalf("expected ErrTombstoned from ExportAuthorizedBundle, got %v", err)
	}
	// Verify no bundle index was written to exportDir.
	if _, err := os.Stat(filepath.Join(exportDir, BundleIndexKey())); !os.IsNotExist(err) {
		t.Fatalf("expected no bundle index written for rejected tombstoned export, stat err=%v", err)
	}
}

// Regression Test #5 (provider.go:770):
// Keep write and read manifest size limits consistent:
// (a) A 4 MiB payload with 64-byte chunks (65,536 chunks, ~23.9 MiB manifest)
//
//	must both write AND read cleanly under MaxManifestSize;
//
// (b) Oversized manifests (> MaxManifestSize) must be rejected BEFORE
//
//	publishing chunks, manifest, or head pointer.
func TestManifestSizeLimitWriteReadConsistency(t *testing.T) {
	ctx := context.Background()
	mem := newMemBackend()
	store, err := NewReplicatedStore([]NodeEndpoint{{Name: "mem", Backend: mem}}, 1)
	if err != nil {
		t.Fatalf("NewReplicatedStore: %v", err)
	}

	owner := mustIdentity(t, "manifest-limit-owner")
	vault, _ := NewVault(owner)

	// Part A: Reviewer's exact repro — 4 MiB payload with 64-byte chunks (65,536 chunks, ~23.9 MiB manifest).
	payload := deterministicPayload("large-manifest-repro-", 4<<20)
	putRes, err := vault.PutArtifact(ctx, store, PutRequest{
		ArtifactID: "art-65536-chunks",
		Kind:       KindModelCheckpoint,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-big-manifest", "eval", time.Hour),
		ChunkSize:  64,
		Data:       payload,
	})
	if err != nil {
		t.Fatalf("PutArtifact (65,536 chunks): %v", err)
	}
	if putRes.ChunkCount != 65536 {
		t.Fatalf("want 65536 chunks, got %d", putRes.ChunkCount)
	}
	got, err := ReadArtifact(ctx, store, putRes.ManifestCID, owner, ReadPolicy{
		TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub},
	})
	if err != nil {
		t.Fatalf("ReadArtifact (65,536 chunks): %v", err)
	}
	if !bytes.Equal(got.Data, payload) {
		t.Fatalf("decrypted payload mismatch for 65,536-chunk artifact")
	}

	// Part B1: Early chunk-count rejection (> MaxManifestSize/minManifestBytesPerChunk chunks).
	tooManyChunksData := make([]byte, int(MaxManifestSize/minManifestBytesPerChunk)+1)
	tooManyChunksData[0] = 0x42
	if _, err := vault.PutArtifact(ctx, store, PutRequest{
		ArtifactID: "art-too-many-chunks",
		Kind:       KindDataset,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-too-many", "eval", time.Hour),
		ChunkSize:  1,
		Data:       tooManyChunksData,
	}); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("expected backend.ErrTooLarge for oversized chunk count, got %v", err)
	}
	headKey1, _ := HeadKey("art-too-many-chunks")
	if _, _, err := mem.Get(ctx, headKey1, backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("head pointer must not be published for rejected oversized manifest, got err=%v", err)
	}

	// Part B2: Post-marshal manifest size rejection (small chunk count, huge ProvenanceNotes
	// pushing hex-encoded EncryptedPrivatePayloadHex past MaxManifestSize).
	hugeNotes := strings.Repeat("x", int(MaxManifestSize/2)+1024)
	if _, err := vault.PutArtifact(ctx, store, PutRequest{
		ArtifactID:      "art-oversized-meta",
		Kind:            KindResearchArtifact,
		License:         LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:         sampleConsent(owner, "consent-huge-meta", "eval", time.Hour),
		ProvenanceNotes: hugeNotes,
		Data:            []byte("small-payload"),
	}); !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("expected backend.ErrTooLarge for oversized manifest metadata, got %v", err)
	}
	headKey2, _ := HeadKey("art-oversized-meta")
	if _, _, err := mem.Get(ctx, headKey2, backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("head pointer must not be published when post-marshal manifest exceeds MaxManifestSize, got err=%v", err)
	}
}

// Regression Test #6 (provider.go:423):
// HTTPBackend.PutIfVersion must enforce If-Match preconditions atomically on
// HTTPNodeServer, rejecting absent keys, stale versions, and concurrent races.
func TestHTTPBackendPutIfVersionAtomicCAS(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 1, 1)
	hb := cluster.store.Nodes()[0].Backend

	const key = "enc/head/cas-test"

	// 1. PutIfVersion on absent key must fail with ErrConflict and write nothing.
	if _, err := hb.PutIfVersion(ctx, key, []byte("absent-write"), "nonexistent-version"); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("PutIfVersion on absent key: want ErrConflict, got %v", err)
	}
	if _, _, err := hb.Get(ctx, key, backend.Unbounded); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("absent key should remain absent after failed PutIfVersion, got err=%v", err)
	}

	// 2. Create with PutIfAbsent, then verify wrong/empty versions fail with ErrConflict.
	v1, err := hb.PutIfAbsent(ctx, key, []byte("v1-data"))
	if err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	if v1 == "" {
		t.Fatalf("expected non-empty Version from PutIfAbsent")
	}
	if _, err := hb.PutIfVersion(ctx, key, []byte("bad-ver"), "stale-wrong-version"); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("PutIfVersion with wrong version: want ErrConflict, got %v", err)
	}
	if _, err := hb.PutIfVersion(ctx, key, []byte("empty-ver"), ""); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("PutIfVersion with empty version: want ErrConflict, got %v", err)
	}

	// 3. Matching version succeeds and returns new version; reusing old version fails.
	v2, err := hb.PutIfVersion(ctx, key, []byte("v2-data"), v1)
	if err != nil {
		t.Fatalf("PutIfVersion(v1): %v", err)
	}
	if v2 == "" || v2 == v1 {
		t.Fatalf("expected new distinct version, got v1=%q v2=%q", v1, v2)
	}
	if _, err := hb.PutIfVersion(ctx, key, []byte("stale-overwrite"), v1); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("PutIfVersion with stale v1: want ErrConflict, got %v", err)
	}
	gotData, gotVer, err := hb.Get(ctx, key, backend.Unbounded)
	if err != nil || string(gotData) != "v2-data" || gotVer != v2 {
		t.Fatalf("unexpected stored value after stale write: data=%q ver=%q err=%v", gotData, gotVer, err)
	}

	// 4. Concurrent CAS race: 16 writers naming v2 -> exactly ONE wins!
	const writers = 16
	var (
		wg        sync.WaitGroup
		wins      atomic.Int64
		conflicts atomic.Int64
		otherErr  atomic.Int64
		winBody   string
		winVer    backend.Version
		winMu     sync.Mutex
	)
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			body := fmt.Sprintf("winner-%02d", id)
			ver, err := hb.PutIfVersion(ctx, key, []byte(body), v2)
			if err == nil {
				wins.Add(1)
				winMu.Lock()
				winBody = body
				winVer = ver
				winMu.Unlock()
			} else if errors.Is(err, backend.ErrConflict) {
				conflicts.Add(1)
			} else {
				otherErr.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if wins.Load() != 1 || conflicts.Load() != writers-1 || otherErr.Load() != 0 {
		t.Fatalf("concurrent PutIfVersion race: wins=%d conflicts=%d otherErr=%d (want 1/%d/0)", wins.Load(), conflicts.Load(), otherErr.Load(), writers-1)
	}
	finalData, finalVer, err := hb.Get(ctx, key, backend.Unbounded)
	if err != nil || string(finalData) != winBody || finalVer != winVer {
		t.Fatalf("final stored value (%q, %q) != winner (%q, %q), err=%v", finalData, finalVer, winBody, winVer, err)
	}
}

// Regression Test #7 (vault.go:98):
// Reauthorizing a previously revoked peer (via AuthorizePeer + PutArtifact, via
// ReauthorizePeer, or across a reopened vault) must advance the epoch and
// publish the updated authorization state so the peer can decrypt new epochs
// while remaining revoked for earlier revoked epochs.
func TestReauthorizePeerPublishesAuthorizationState(t *testing.T) {
	ctx := context.Background()
	cluster := startTestCluster(t, 3, 3)

	owner := mustIdentity(t, "reauth-owner")
	peerA := mustIdentity(t, "reauth-peer-a")

	vault, _ := NewVault(owner)
	if err := vault.AuthorizePeer(peerA.Public("reader")); err != nil {
		t.Fatalf("AuthorizePeer: %v", err)
	}

	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}

	// Epoch 1: peerA authorized.
	ep1Data := deterministicPayload("reauth-ep1-", 16<<10)
	ep1Res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-reauth-timeline",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-reauth-1", "sync", time.Hour),
		Data:       ep1Data,
	})
	if err != nil {
		t.Fatalf("PutArtifact ep1: %v", err)
	}

	// Revoke peerA -> Epoch 2.
	if _, err := vault.RevokePeer(ctx, cluster.store, peerA.KeyID, "suspended"); err != nil {
		t.Fatalf("RevokePeer: %v", err)
	}
	ep2Data := deterministicPayload("reauth-ep2-", 16<<10)
	ep2Res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-reauth-timeline",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-reauth-2", "sync", time.Hour),
		Data:       ep2Data,
	})
	if err != nil {
		t.Fatalf("PutArtifact ep2: %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, ep2Res.ManifestCID, peerA, policy); !errors.Is(err, ErrPeerRevoked) {
		t.Fatalf("peerA on ep2: want ErrPeerRevoked, got %v", err)
	}

	// Reauthorize via AuthorizePeer (existing signature!) -> Epoch 3, published on PutArtifact!
	if err := vault.AuthorizePeer(peerA.Public("reader")); err != nil {
		t.Fatalf("AuthorizePeer reauth: %v", err)
	}
	ep3Data := deterministicPayload("reauth-ep3-", 16<<10)
	ep3Res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-reauth-timeline",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-reauth-3", "sync", time.Hour),
		Data:       ep3Data,
	})
	if err != nil {
		t.Fatalf("PutArtifact ep3: %v", err)
	}
	if ep3Res.Manifest.Epoch != 3 {
		t.Fatalf("expected epoch 3 after reauthorization, got %d", ep3Res.Manifest.Epoch)
	}
	// peerA MUST now decrypt Epoch 3 cleanly!
	gotEp3, err := ReadArtifact(ctx, cluster.store, ep3Res.ManifestCID, peerA, policy)
	if err != nil || !bytes.Equal(gotEp3.Data, ep3Data) {
		t.Fatalf("reauthorized peerA failed to read ep3: err=%v", err)
	}
	// Epoch 2 (revoked interval) MUST still reject peerA with ErrPeerRevoked!
	if _, err := ReadArtifact(ctx, cluster.store, ep2Res.ManifestCID, peerA, policy); !errors.Is(err, ErrPeerRevoked) {
		t.Fatalf("peerA on revoked ep2 after ep3 reauth: want ErrPeerRevoked, got %v", err)
	}
	// Epoch 1 (pre-revocation) remains readable with valid envelope.
	if gotEp1, err := ReadArtifact(ctx, cluster.store, ep1Res.ManifestCID, peerA, policy); err != nil || !bytes.Equal(gotEp1.Data, ep1Data) {
		t.Fatalf("peerA on ep1: err=%v", err)
	}

	// Revoke peerA AGAIN -> Epoch 4; then reopen a fresh Vault and reauthorize -> Epoch 5!
	if _, err := vault.RevokePeer(ctx, cluster.store, peerA.KeyID, "revoked-second-time"); err != nil {
		t.Fatalf("second RevokePeer: %v", err)
	}
	ep4Res, err := vault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-reauth-timeline",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-reauth-4", "sync", time.Hour),
		Data:       deterministicPayload("reauth-ep4-", 16<<10),
	})
	if err != nil {
		t.Fatalf("PutArtifact ep4: %v", err)
	}
	if _, err := ReadArtifact(ctx, cluster.store, ep4Res.ManifestCID, peerA, policy); !errors.Is(err, ErrPeerRevoked) {
		t.Fatalf("peerA on ep4: want ErrPeerRevoked, got %v", err)
	}

	// Reopened vault calls AuthorizePeer + PutArtifact -> syncs persisted epoch 4,
	// advances to epoch 5, publishes updated revocation list, and peerA reads ep5!
	reopenedVault, _ := NewVault(owner)
	if err := reopenedVault.AuthorizePeer(peerA.Public("reader")); err != nil {
		t.Fatalf("reopenedVault.AuthorizePeer: %v", err)
	}
	ep5Data := deterministicPayload("reauth-ep5-", 16<<10)
	ep5Res, err := reopenedVault.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "art-reauth-timeline",
		Kind:       KindAgentMemory,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-reauth-5", "sync", time.Hour),
		Data:       ep5Data,
	})
	if err != nil {
		t.Fatalf("reopenedVault.PutArtifact ep5: %v", err)
	}
	if ep5Res.Manifest.Epoch != 5 {
		t.Fatalf("expected epoch 5 from reopened vault reauth, got %d", ep5Res.Manifest.Epoch)
	}
	if gotEp5, err := ReadArtifact(ctx, cluster.store, ep5Res.ManifestCID, peerA, policy); err != nil || !bytes.Equal(gotEp5.Data, ep5Data) {
		t.Fatalf("peerA failed to read ep5 after reopened vault reauth: err=%v", err)
	}

	// Explicit ReauthorizePeer immediately publishes to store without waiting for PutArtifact.
	if _, err := reopenedVault.RevokePeer(ctx, cluster.store, peerA.KeyID, "third-revoke"); err != nil {
		t.Fatalf("third RevokePeer: %v", err)
	}
	revAfterReauth, err := reopenedVault.ReauthorizePeer(ctx, cluster.store, peerA.Public("reader"))
	if err != nil {
		t.Fatalf("ReauthorizePeer: %v", err)
	}
	if revAfterReauth == nil || revAfterReauth.CurrentEpoch != 7 || revAfterReauth.IsPeerRevoked(peerA.KeyID, 7) {
		t.Fatalf("unexpected revList after ReauthorizePeer: %+v", revAfterReauth)
	}
	if !revAfterReauth.IsPeerRevoked(peerA.KeyID, 6) {
		t.Fatalf("epoch 6 must remain revoked after epoch 7 ReauthorizePeer")
	}
}

// Regression Test #8 (vault.go:1023):
// RestoreAndVerify must decode and cryptographically verify each replica's
// bundle index inside the replica loop, failing over cleanly past corrupted,
// forged, or untrusted replicas.
func TestRestoreAndVerifyFailsOverPastCorruptedBundleIndex(t *testing.T) {
	ctx := context.Background()
	srcCluster := startTestCluster(t, 2, 2)

	owner := mustIdentity(t, "bundle-owner")
	otherOwner := mustIdentity(t, "untrusted-bundle-owner")
	vault, _ := NewVault(owner)

	payload := deterministicPayload("bundle-failover-", 32<<10)
	putRes, err := vault.PutArtifact(ctx, srcCluster.store, PutRequest{
		ArtifactID: "art-bundle-failover",
		Kind:       KindExecutionReceipt,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-bundle-failover", "restore", time.Hour),
		Data:       payload,
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}

	exportDir := filepath.Join(t.TempDir(), "bundle-export")
	if _, err := vault.ExportAuthorizedBundle(ctx, srcCluster.store, exportDir, []string{putRes.ManifestCID}); err != nil {
		t.Fatalf("ExportAuthorizedBundle: %v", err)
	}

	// Copy the exported bundle into two independent in-memory replicas.
	srcFileBackend, err := backend.Open(ctx, "file://"+exportDir)
	if err != nil {
		t.Fatalf("open export dir: %v", err)
	}
	defer srcFileBackend.Close()

	replica1 := newMemBackend()
	replica2 := newMemBackend()
	objCh, errCh := srcFileBackend.List(ctx, "")
	for obj := range objCh {
		data, _, err := srcFileBackend.Get(ctx, obj.Key, backend.Unbounded)
		if err != nil {
			t.Fatalf("get exported %s: %v", obj.Key, err)
		}
		_ = replica1.Put(ctx, obj.Key, data)
		_ = replica2.Put(ctx, obj.Key, data)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("list export dir: %v", err)
	}

	validIdxBytes, _, err := replica2.Get(ctx, BundleIndexKey(), backend.Unbounded)
	if err != nil {
		t.Fatalf("get valid bundle index: %v", err)
	}

	indepStore, err := NewReplicatedStore([]NodeEndpoint{
		{Name: "replica-1-poisoned", Backend: replica1},
		{Name: "replica-2-healthy", Backend: replica2},
	}, 2)
	if err != nil {
		t.Fatalf("NewReplicatedStore indep: %v", err)
	}

	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}

	// Subcase 8a: replica-1 returns malformed JSON bundle index -> failover to replica-2 succeeds.
	_ = replica1.Put(ctx, BundleIndexKey(), []byte("{malformed-bundle-index"))
	res, err := RestoreAndVerify(ctx, indepStore, owner, policy)
	if err != nil || len(res.Restored) != 1 || !bytes.Equal(res.Restored[0].Data, payload) {
		t.Fatalf("RestoreAndVerify failover (malformed JSON): err=%v", err)
	}
	indepStore.ResetQuarantine()

	// Subcase 8b: replica-1 returns bundle index with tampered signature -> failover to replica-2 succeeds.
	var badSigIdx AuthorizedBundleIndex
	_ = json.Unmarshal(validIdxBytes, &badSigIdx)
	badSigIdx.SignatureHex = strings.Repeat("aa", ed25519.SignatureSize)
	badSigBytes, _ := json.Marshal(&badSigIdx)
	_ = replica1.Put(ctx, BundleIndexKey(), badSigBytes)
	res, err = RestoreAndVerify(ctx, indepStore, owner, policy)
	if err != nil || len(res.Restored) != 1 || !bytes.Equal(res.Restored[0].Data, payload) {
		t.Fatalf("RestoreAndVerify failover (bad signature): err=%v", err)
	}
	indepStore.ResetQuarantine()

	// Subcase 8c: replica-1 returns validly signed index from an untrusted owner -> failover to replica-2 succeeds.
	untrustedIdx := AuthorizedBundleIndex{
		Version:    FormatVersion,
		Owner:      otherOwner.Public("owner"),
		ExportedAt: time.Now().UTC(),
		Artifacts:  badSigIdx.Artifacts,
	}
	_ = untrustedIdx.Sign(otherOwner)
	untrustedBytes, _ := json.Marshal(&untrustedIdx)
	_ = replica1.Put(ctx, BundleIndexKey(), untrustedBytes)
	res, err = RestoreAndVerify(ctx, indepStore, owner, policy)
	if err != nil || len(res.Restored) != 1 || !bytes.Equal(res.Restored[0].Data, payload) {
		t.Fatalf("RestoreAndVerify failover (untrusted owner): err=%v", err)
	}
	indepStore.ResetQuarantine()

	// Subcase 8d: When ALL replicas have corrupted bundle indexes, fail closed with ErrAllReplicasFailed.
	_ = replica2.Put(ctx, BundleIndexKey(), []byte("{also-corrupted"))
	if _, err := RestoreAndVerify(ctx, indepStore, owner, policy); !errors.Is(err, ErrAllReplicasFailed) {
		t.Fatalf("expected ErrAllReplicasFailed when all bundle indexes corrupted, got %v", err)
	}
}

// gatingRevocationBackend pauses the first PutIfAbsent for a targeted
// revocation payload (matching seq & epoch) so a second Vault can concurrently
// publish a conflicting equal-sequence record at a different epoch.
type gatingRevocationBackend struct {
	backend.Backend
	targetSeq   uint64
	targetEpoch uint64
	paused      chan struct{}
	release     chan struct{}
	once        sync.Once
}

func (g *gatingRevocationBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if strings.HasPrefix(key, "enc/revoke/") {
		var r SignedRevocationList
		if json.Unmarshal(data, &r) == nil && r.Seq == g.targetSeq && r.CurrentEpoch == g.targetEpoch {
			g.once.Do(func() {
				close(g.paused)
				<-g.release
			})
		}
	}
	return g.Backend.PutIfAbsent(ctx, key, data)
}

// TestRevocationCASRejectsEqualSeqConflictAcrossEpochs verifies that a paused
// RevokePeer publication at (Seq=1, epoch=2) cannot overwrite an accepted
// RevokeConsent publication at (Seq=1, epoch=1); CAS rejects the equal-sequence
// conflict regardless of epoch, forcing reload, merge, and sequence advancement
// so both revocations survive.
func TestRevocationCASRejectsEqualSeqConflictAcrossEpochs(t *testing.T) {
	ctx := context.Background()
	owner, err := GenerateIdentity("owner-cas-cross-epoch")
	if err != nil {
		t.Fatalf("GenerateIdentity owner: %v", err)
	}
	peer, err := GenerateIdentity("peer-cas-cross-epoch")
	if err != nil {
		t.Fatalf("GenerateIdentity peer: %v", err)
	}

	rawMem := newMemBackend()
	gated := &gatingRevocationBackend{
		Backend:     rawMem,
		targetSeq:   1,
		targetEpoch: 2,
		paused:      make(chan struct{}),
		release:     make(chan struct{}),
	}
	store, err := NewReplicatedStore([]NodeEndpoint{
		{Name: "node-gated", Backend: gated},
	}, 1)
	if err != nil {
		t.Fatalf("NewReplicatedStore: %v", err)
	}

	vaultPeer, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault peer: %v", err)
	}
	_ = vaultPeer.AuthorizePeer(peer.Public("peer"))
	vaultConsent, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault consent: %v", err)
	}
	_ = vaultConsent.AuthorizePeer(peer.Public("peer"))

	// Write an artifact bound to consent-target before the race.
	putRes, err := vaultPeer.PutArtifact(ctx, store, PutRequest{
		ArtifactID: "art-cas-consent",
		Kind:       KindAgentMemory,
		Data:       []byte("confidential-consent-bound-payload"),
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-target", "memory:read", time.Hour),
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}

	// Start Vault 1's RevokePeer (Seq=1, epoch=2); it pauses before PutIfAbsent.
	type revResult struct {
		list *SignedRevocationList
		err  error
	}
	peerDone := make(chan revResult, 1)
	go func() {
		rl, rerr := vaultPeer.RevokePeer(ctx, store, peer.KeyID, "compromised-peer")
		peerDone <- revResult{list: rl, err: rerr}
	}()

	select {
	case <-gated.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RevokePeer (Seq=1, epoch=2) to pause")
	}

	// While Vault 1 is paused, Vault 2 publishes RevokeConsent at (Seq=1, epoch=1).
	consentList, err := vaultConsent.RevokeConsent(ctx, store, "consent-target")
	if err != nil {
		close(gated.release)
		t.Fatalf("RevokeConsent: %v", err)
	}
	if consentList.Seq != 1 || consentList.CurrentEpoch != 1 {
		close(gated.release)
		t.Fatalf("expected consent revocation at (Seq=1, epoch=1), got (Seq=%d, epoch=%d)", consentList.Seq, consentList.CurrentEpoch)
	}

	// Also confirm direct PutRevocationQuorum rejects a conflicting (Seq=1, epoch=2) record.
	directConflict := &SignedRevocationList{
		Version:      FormatVersion,
		Owner:        owner.Public("owner"),
		CurrentEpoch: 2,
		Seq:          1,
		RevokedPeers: []RevocationEntry{{PeerKeyID: peer.KeyID, RevokedAtEpoch: 2, RevokedAt: time.Now().UTC(), Reason: "direct-conflict"}},
		UpdatedAt:    time.Now().UTC(),
	}
	if err := directConflict.Sign(owner); err != nil {
		close(gated.release)
		t.Fatalf("Sign directConflict: %v", err)
	}
	if _, err := store.PutRevocationQuorum(ctx, owner.KeyID, owner.Ed25519Pub, directConflict); !errors.Is(err, ErrStaleRevocation) {
		close(gated.release)
		t.Fatalf("expected ErrStaleRevocation for conflicting equal-seq cross-epoch write, got %v", err)
	}

	// Resume Vault 1's paused RevokePeer: it must reject the stale Seq=1 write,
	// reload (Seq=1, epoch=1), merge consent-target, advance to Seq=2, and publish.
	close(gated.release)
	res := <-peerDone
	if res.err != nil {
		t.Fatalf("RevokePeer after CAS retry: %v", res.err)
	}
	if res.list.Seq < 2 {
		t.Fatalf("expected merged revocation list at Seq>=2, got Seq=%d", res.list.Seq)
	}

	latest, err := store.GetLatestRevocationList(ctx, owner.KeyID, owner.Ed25519Pub)
	if err != nil || latest == nil {
		t.Fatalf("GetLatestRevocationList: latest=%v err=%v", latest, err)
	}
	if !latest.IsConsentRevoked("consent-target") {
		t.Fatalf("consent revocation was overwritten! latest=%+v", latest)
	}
	if !latest.IsPeerRevoked(peer.KeyID, 2) {
		t.Fatalf("peer revocation missing from merged list! latest=%+v", latest)
	}

	// Artifact bound to consent-target must STILL fail with ErrConsentRevoked.
	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}
	if _, err := ReadArtifact(ctx, store, putRes.ManifestCID, owner, policy); !errors.Is(err, ErrConsentRevoked) {
		t.Fatalf("expected ErrConsentRevoked after concurrent peer+consent revocation, got %v", err)
	}
}

// TestRestoreAndVerifyEnforcesRequiredBundleRevocationLog verifies that
// RestoreAndVerify enforces the signed AuthorizedBundleIndex's revocation-log
// requirements before restoring any artifacts, rejecting bundles whose
// enc/revoke/<owner> record was deleted or rolled back to an older sequence.
func TestRestoreAndVerifyEnforcesRequiredBundleRevocationLog(t *testing.T) {
	ctx := context.Background()
	owner, err := GenerateIdentity("owner-bundle-rev-req")
	if err != nil {
		t.Fatalf("GenerateIdentity owner: %v", err)
	}
	peer, err := GenerateIdentity("peer-bundle-rev-req")
	if err != nil {
		t.Fatalf("GenerateIdentity peer: %v", err)
	}

	srcCluster := startTestCluster(t, 2, 2)
	vault, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}
	_ = vault.AuthorizePeer(peer.Public("peer"))

	putRes, err := vault.PutArtifact(ctx, srcCluster.store, PutRequest{
		ArtifactID: "art-revoked-consent",
		Kind:       KindAgentMemory,
		Data:       []byte("secret-under-revoked-consent"),
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-rev-bundle", "memory:read", time.Hour),
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}

	// Publish Seq=1 (unrelated consent) so we can later simulate a rollback attack.
	olderRev, err := vault.RevokeConsent(ctx, srcCluster.store, "consent-unrelated")
	if err != nil {
		t.Fatalf("RevokeConsent unrelated: %v", err)
	}
	olderRevBytes, err := json.Marshal(olderRev)
	if err != nil {
		t.Fatalf("Marshal olderRev: %v", err)
	}

	// Publish Seq=2 revoking the artifact's actual consent.
	if _, err := vault.RevokeConsent(ctx, srcCluster.store, "consent-rev-bundle"); err != nil {
		t.Fatalf("RevokeConsent target: %v", err)
	}

	exportDir := filepath.Join(t.TempDir(), "bundle-with-rev")
	expRes, err := vault.ExportAuthorizedBundle(ctx, srcCluster.store, exportDir, []string{putRes.ManifestCID})
	if err != nil {
		t.Fatalf("ExportAuthorizedBundle: %v", err)
	}
	if !expRes.Index.HasRevocationLog || len(expRes.Index.RevocationLogs) == 0 {
		t.Fatalf("expected ExportAuthorizedBundle to sign HasRevocationLog=true and RevocationLogs, got %+v", expRes.Index)
	}

	loadBundleMem := func(t *testing.T) *memBackend {
		t.Helper()
		fb, err := backend.Open(ctx, "file://"+exportDir)
		if err != nil {
			t.Fatalf("backend.Open: %v", err)
		}
		defer fb.Close()
		mb := newMemBackend()
		objCh, errCh := fb.List(ctx, "")
		for obj := range objCh {
			raw, _, err := fb.Get(ctx, obj.Key, backend.Unbounded)
			if err != nil {
				t.Fatalf("Get %s: %v", obj.Key, err)
			}
			_ = mb.Put(ctx, obj.Key, raw)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("List: %v", err)
		}
		return mb
	}

	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}
	revKey, err := RevocationKey(owner.KeyID)
	if err != nil {
		t.Fatalf("RevocationKey: %v", err)
	}

	// Baseline: intact bundle enforces ErrConsentRevoked.
	intactStore, _ := NewReplicatedStore([]NodeEndpoint{{Name: "intact", Backend: loadBundleMem(t)}}, 1)
	if _, err := RestoreAndVerify(ctx, intactStore, owner, policy); !errors.Is(err, ErrConsentRevoked) {
		t.Fatalf("expected ErrConsentRevoked on intact bundle, got %v", err)
	}

	// Subcase A: deleting enc/revoke/<owner_key_id> must fail closed, never return plaintext.
	strippedMem := loadBundleMem(t)
	if err := strippedMem.Delete(ctx, revKey); err != nil {
		t.Fatalf("Delete revKey: %v", err)
	}
	strippedStore, _ := NewReplicatedStore([]NodeEndpoint{{Name: "stripped", Backend: strippedMem}}, 1)
	if res, err := RestoreAndVerify(ctx, strippedStore, owner, policy); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected ErrInvalidManifest when required revocation log is deleted, got res=%v err=%v", res, err)
	}

	// Subcase B: rolling back enc/revoke/<owner_key_id> to older valid Seq=1 must fail closed.
	rollbackMem := loadBundleMem(t)
	if err := rollbackMem.Put(ctx, revKey, olderRevBytes); err != nil {
		t.Fatalf("Put olderRev: %v", err)
	}
	rollbackStore, _ := NewReplicatedStore([]NodeEndpoint{{Name: "rollback", Backend: rollbackMem}}, 1)
	if res, err := RestoreAndVerify(ctx, rollbackStore, owner, policy); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected ErrInvalidManifest when bundle revocation log is rolled back, got res=%v err=%v", res, err)
	}

	// Subcase C: legacy index with only HasRevocationLog=true (no RevocationLogs slice)
	// must also fail closed when enc/revoke/<owner_key_id> is deleted.
	legacyMem := loadBundleMem(t)
	legacyIdx := *expRes.Index
	legacyIdx.RevocationLogs = nil
	legacyIdx.HasRevocationLog = true
	if err := legacyIdx.Sign(owner); err != nil {
		t.Fatalf("Sign legacyIdx: %v", err)
	}
	legacyIdxBytes, _ := json.Marshal(&legacyIdx)
	_ = legacyMem.Put(ctx, BundleIndexKey(), legacyIdxBytes)
	_ = legacyMem.Delete(ctx, revKey)
	legacyStore, _ := NewReplicatedStore([]NodeEndpoint{{Name: "legacy-stripped", Backend: legacyMem}}, 1)
	if res, err := RestoreAndVerify(ctx, legacyStore, owner, policy); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected ErrInvalidManifest on legacy HasRevocationLog=true bundle missing log, got res=%v err=%v", res, err)
	}
}

// TestCryptoShredRecoversPersistedManifestsAfterRestart verifies that a freshly
// recreated Vault (with empty in-memory artifactHistory) recovers all persisted
// manifests and chunks for artifactID from store before deleting the head,
// erasing wrapped DEKs and ciphertext chunks across all reachable replicas.
func TestCryptoShredRecoversPersistedManifestsAfterRestart(t *testing.T) {
	ctx := context.Background()
	owner, err := GenerateIdentity("owner-shred-restart")
	if err != nil {
		t.Fatalf("GenerateIdentity owner: %v", err)
	}
	peer, err := GenerateIdentity("peer-shred-restart")
	if err != nil {
		t.Fatalf("GenerateIdentity peer: %v", err)
	}

	cluster := startTestCluster(t, 3, 2)
	vaultBeforeRestart, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault before restart: %v", err)
	}
	_ = vaultBeforeRestart.AuthorizePeer(peer.Public("peer"))

	// Write v1 and v2 (without ParentManifestCID link to also test manifest scan)
	// plus an unrelated artifact that must survive untouched.
	v1Res, err := vaultBeforeRestart.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "shred-after-restart",
		Kind:       KindAgentMemory,
		Data:       bytes.Repeat([]byte("version-1-secret-"), 256),
		ChunkSize:  1024,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-shred", "memory:read", time.Hour),
	})
	if err != nil {
		t.Fatalf("PutArtifact v1: %v", err)
	}
	v2Res, err := vaultBeforeRestart.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "shred-after-restart",
		Kind:       KindAgentMemory,
		Data:       bytes.Repeat([]byte("version-2-secret-"), 256),
		ChunkSize:  1024,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-shred", "memory:read", time.Hour),
	})
	if err != nil {
		t.Fatalf("PutArtifact v2: %v", err)
	}
	keepRes, err := vaultBeforeRestart.PutArtifact(ctx, cluster.store, PutRequest{
		ArtifactID: "unrelated-keep-me",
		Kind:       KindAgentMemory,
		Data:       []byte("unrelated-artifact-must-survive"),
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-keep", "memory:read", time.Hour),
	})
	if err != nil {
		t.Fatalf("PutArtifact keep: %v", err)
	}

	// Simulate process restart: brand-new Vault instance with empty artifactHistory.
	restartedVault, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault restarted: %v", err)
	}
	receipt, err := restartedVault.CryptoShred(ctx, cluster.store, "shred-after-restart", "gdpr-restart-erasure")
	if err != nil {
		t.Fatalf("CryptoShred after restart: %v", err)
	}
	if receipt.ManifestsDeleted != 2 {
		t.Fatalf("ManifestsDeleted=%d, want 2 (v1 and v2 recovered from store)", receipt.ManifestsDeleted)
	}
	if receipt.ChunksDeleteOps == 0 {
		t.Fatal("expected ChunksDeleteOps > 0 after recovering persisted manifests")
	}

	// Verify every manifest and every ciphertext chunk of v1 and v2 is gone on ALL replicas.
	for _, m := range []*SignedManifest{v1Res.Manifest, v2Res.Manifest} {
		mk, _ := ManifestKey(m.ManifestCID)
		for _, ep := range cluster.store.Nodes() {
			if _, _, err := ep.Backend.Get(ctx, mk, MaxManifestSize); !errors.Is(err, backend.ErrNotFound) {
				t.Fatalf("node %s still retains shredded manifest %s: err=%v", ep.Name, m.ManifestCID[:12], err)
			}
			for _, ref := range m.CipherChunks {
				ck, _ := ChunkKey(ref.CiphertextCID)
				if _, _, err := ep.Backend.Get(ctx, ck, ref.CiphertextSize+1024); !errors.Is(err, backend.ErrNotFound) {
					t.Fatalf("node %s still retains shredded chunk %s: err=%v", ep.Name, ref.CiphertextCID[:12], err)
				}
			}
		}
	}

	// Verify unrelated artifact was NOT touched and remains readable by the peer.
	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}
	keepRead, err := ReadArtifact(ctx, cluster.store, keepRes.ManifestCID, peer, policy)
	if err != nil {
		t.Fatalf("ReadArtifact unrelated keep-me: %v", err)
	}
	if string(keepRead.Data) != "unrelated-artifact-must-survive" {
		t.Fatalf("unrelated artifact payload mismatch: %q", string(keepRead.Data))
	}
}

// TestGetVerifiedManifestFailsOverOnCorruptedSelfCID verifies that when the
// first replica alters only the unsigned JSON manifest_cid field (leaving
// ComputeCID equal to the requested CID), GetVerifiedManifest treats it as
// replica corruption, quarantines that replica, and fails over cleanly to a
// healthy replica for reads and exports.
func TestGetVerifiedManifestFailsOverOnCorruptedSelfCID(t *testing.T) {
	ctx := context.Background()
	owner, err := GenerateIdentity("owner-selfcid-failover")
	if err != nil {
		t.Fatalf("GenerateIdentity owner: %v", err)
	}

	node0 := newMemBackend()
	node1 := newMemBackend()
	store, err := NewReplicatedStore([]NodeEndpoint{
		{Name: "replica-0-bad-selfcid", Backend: node0},
		{Name: "replica-1-healthy", Backend: node1},
	}, 2)
	if err != nil {
		t.Fatalf("NewReplicatedStore: %v", err)
	}

	vault, err := NewVault(owner)
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}
	payload := []byte("manifest-self-cid-failover-payload")
	putRes, err := vault.PutArtifact(ctx, store, PutRequest{
		ArtifactID: "art-selfcid-failover",
		Kind:       KindAgentMemory,
		Data:       payload,
		License:    LicensePolicy{SPDX: "Apache-2.0", CommercialAllowed: true, RedistributionAllowed: true},
		Consent:    sampleConsent(owner, "consent-selfcid", "memory:read", time.Hour),
	})
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}

	// Corrupt ONLY the unsigned manifest_cid JSON field on replica-0.
	mk, _ := ManifestKey(putRes.ManifestCID)
	raw0, _, err := node0.Get(ctx, mk, MaxManifestSize)
	if err != nil {
		t.Fatalf("node0 Get manifest: %v", err)
	}
	var poisoned SignedManifest
	if err := json.Unmarshal(raw0, &poisoned); err != nil {
		t.Fatalf("Unmarshal raw0: %v", err)
	}
	poisoned.ManifestCID = strings.Repeat("ff", 32)
	poisonedRaw, err := json.Marshal(&poisoned)
	if err != nil {
		t.Fatalf("Marshal poisoned: %v", err)
	}
	if err := node0.Put(ctx, mk, poisonedRaw); err != nil {
		t.Fatalf("node0 Put poisoned: %v", err)
	}

	// 1. ReadArtifact must fail over past replica-0 to replica-1 and succeed.
	policy := ReadPolicy{TrustedOwnerEdPubs: []ed25519.PublicKey{owner.Ed25519Pub}}
	readRes, err := ReadArtifact(ctx, store, putRes.ManifestCID, owner, policy)
	if err != nil {
		t.Fatalf("ReadArtifact failed instead of failing over past corrupted self-CID: %v", err)
	}
	if !bytes.Equal(readRes.Data, payload) {
		t.Fatalf("ReadArtifact payload mismatch: got %q, want %q", readRes.Data, payload)
	}

	// 2. ExportAuthorizedBundle must also succeed via failover to replica-1.
	exportDir := filepath.Join(t.TempDir(), "export-selfcid")
	if _, err := vault.ExportAuthorizedBundle(ctx, store, exportDir, []string{putRes.ManifestCID}); err != nil {
		t.Fatalf("ExportAuthorizedBundle failed instead of failing over past corrupted self-CID: %v", err)
	}

	// 3. When ALL replicas have corrupted self-CID, fail closed with ErrAllReplicasFailed + ErrInvalidManifest.
	if err := node1.Put(ctx, mk, poisonedRaw); err != nil {
		t.Fatalf("node1 Put poisoned: %v", err)
	}
	if _, _, err := store.GetVerifiedManifest(ctx, putRes.ManifestCID, []ed25519.PublicKey{owner.Ed25519Pub}); !errors.Is(err, ErrAllReplicasFailed) || !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected ErrAllReplicasFailed wrapping ErrInvalidManifest when all replicas alter manifest_cid, got %v", err)
	}
}

