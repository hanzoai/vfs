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
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

// Sentinel errors returned by internal/encstore.
var (
	ErrInvalidKind       = errors.New("encstore: invalid artifact kind")
	ErrInvalidID         = errors.New("encstore: invalid identifier")
	ErrInvalidManifest   = errors.New("encstore: invalid manifest")
	ErrBadSignature      = errors.New("encstore: signature verification failed")
	ErrUntrustedOwner    = errors.New("encstore: untrusted owner identity")
	ErrAccessDenied      = errors.New("encstore: access denied (unauthorized peer or invalid key envelope)")
	ErrPeerRevoked       = errors.New("encstore: peer access revoked for this epoch")
	ErrLicenseDenied     = errors.New("encstore: artifact license disallowed by policy")
	ErrConsentInvalid    = errors.New("encstore: invalid consent record")
	ErrConsentExpired    = errors.New("encstore: consent expired")
	ErrConsentDenied     = errors.New("encstore: consent purpose mismatch")
	ErrConsentRevoked    = errors.New("encstore: consent revoked")
	ErrTombstoned        = errors.New("encstore: artifact cryptographically erased (tombstoned)")
	ErrChunkCorrupted    = errors.New("encstore: ciphertext chunk corrupted or authenticated tag invalid")
	ErrPlaintextMismatch = errors.New("encstore: decrypted plaintext digest mismatch")
	ErrAllReplicasFailed = errors.New("encstore: all storage replicas failed or returned corrupted data")
	ErrQuorumFailed      = errors.New("encstore: replication write quorum not met")
	ErrStaleRevocation   = errors.New("encstore: stale revocation list cannot replace newer sequence")
)

// ArtifactKind enumerates the five portable decentralized artifact kinds
// specified in hanzoai/vfs#39.
type ArtifactKind string

const (
	KindDataset          ArtifactKind = "dataset"
	KindModelCheckpoint  ArtifactKind = "model-checkpoint"
	KindAgentMemory      ArtifactKind = "agent-memory"
	KindResearchArtifact ArtifactKind = "research-artifact"
	KindExecutionReceipt ArtifactKind = "execution-receipt"
)

// AllArtifactKinds returns all five canonical artifact kinds.
func AllArtifactKinds() []ArtifactKind {
	return []ArtifactKind{
		KindDataset,
		KindModelCheckpoint,
		KindAgentMemory,
		KindResearchArtifact,
		KindExecutionReceipt,
	}
}

// Valid reports whether k is one of the five supported artifact kinds.
func (k ArtifactKind) Valid() bool {
	switch k {
	case KindDataset, KindModelCheckpoint, KindAgentMemory, KindResearchArtifact, KindExecutionReceipt:
		return true
	default:
		return false
	}
}

// ValidateSegment checks that id is a safe lowercase backend key segment
// (`a-z`, `0-9`, `-`, length 1..128) so all derived VFS keys satisfy backend.ValidKey.
func ValidateSegment(id string) error {
	if len(id) == 0 || len(id) > 128 {
		return fmt.Errorf("%w: segment length %d outside 1..128", ErrInvalidID, len(id))
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return fmt.Errorf("%w: segment %q contains invalid byte %#x", ErrInvalidID, id, c)
		}
	}
	return nil
}

// ValidateHexDigest checks that h is a 64-character lowercase hex BLAKE3/SHA-256 digest.
func ValidateHexDigest(h string) error {
	if len(h) != 64 {
		return fmt.Errorf("%w: digest length %d, want 64 hex chars", ErrInvalidID, len(h))
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return fmt.Errorf("%w: digest %q has non-lowercase-hex byte %#x", ErrInvalidID, h, c)
		}
	}
	return nil
}

// Canonical VFS key builders (strictly conforming to pkg/backend.ValidKey and
// non-overlapping with Volume format-v2 keys head, meta/*, pack/*, ext/*).

// ChunkKey returns the canonical VFS backend key for an encrypted ciphertext chunk:
// `enc/chunk/<2hex>/<64hex_ciphertext_blake3>`. Plaintext hashes are never used here.
func ChunkKey(ciphertextBLAKE3 string) (string, error) {
	if err := ValidateHexDigest(ciphertextBLAKE3); err != nil {
		return "", err
	}
	key := "enc/chunk/" + ciphertextBLAKE3[:2] + "/" + ciphertextBLAKE3
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// ManifestKey returns the canonical VFS backend key for a signed manifest:
// `enc/manifest/<2hex>/<64hex_manifest_cid>`.
func ManifestKey(manifestCID string) (string, error) {
	if err := ValidateHexDigest(manifestCID); err != nil {
		return "", err
	}
	key := "enc/manifest/" + manifestCID[:2] + "/" + manifestCID
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// HeadKey returns the mutable latest-manifest pointer key for artifactID:
// `enc/head/<artifact_id>`.
func HeadKey(artifactID string) (string, error) {
	if err := ValidateSegment(artifactID); err != nil {
		return "", err
	}
	key := "enc/head/" + artifactID
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// RevocationKey returns the canonical VFS key for an owner's signed revocation list:
// `enc/revoke/<owner_key_id>`.
func RevocationKey(ownerKeyID string) (string, error) {
	if err := ValidateSegment(ownerKeyID); err != nil {
		return "", err
	}
	key := "enc/revoke/" + ownerKeyID
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// TombstoneKey returns the canonical VFS key for an artifact's signed deletion tombstone:
// `enc/tombstone/<artifact_id>`.
func TombstoneKey(artifactID string) (string, error) {
	if err := ValidateSegment(artifactID); err != nil {
		return "", err
	}
	key := "enc/tombstone/" + artifactID
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// BundleIndexKey returns the canonical VFS key for a sovereign offline export index:
// `enc/bundle/index`.
func BundleIndexKey() string {
	return "enc/bundle/index"
}

// LicensePolicy records mandatory dataset/model/artifact licensing terms.
// SPDX is bound into every chunk's AES-GCM AAD and covered by the manifest signature.
type LicensePolicy struct {
	SPDX                  string `json:"spdx"`
	CommercialAllowed     bool   `json:"commercial_allowed"`
	RedistributionAllowed bool   `json:"redistribution_allowed"`
	AttributionRequired   bool   `json:"attribution_required"`
	CustomTermsHash       string `json:"custom_terms_hash,omitempty"`
}

// Validate checks that LicensePolicy carries a non-empty SPDX identifier.
func (l LicensePolicy) Validate() error {
	if strings.TrimSpace(l.SPDX) == "" || len(l.SPDX) > 128 {
		return fmt.Errorf("%w: spdx license identifier is required", ErrLicenseDenied)
	}
	return nil
}

// ConsentRecord captures verifiable, revocable, purpose-scoped data consent.
// ConsentHash is bound into every chunk's and key envelope's AES-GCM AAD.
type ConsentRecord struct {
	ConsentID    string    `json:"consent_id"`
	GrantorKeyID string    `json:"grantor_key_id"`
	Purpose      string    `json:"purpose"`
	IssuedAt     time.Time `json:"issued_at"`
	ExpiresAt    time.Time `json:"expires_at,omitempty"`
	Revocable    bool      `json:"revocable"`
	ConsentHash  string    `json:"consent_hash"`
}

// ComputeConsentHash returns the canonical BLAKE3 hex digest of the consent record's
// substantive fields (excluding ConsentHash itself).
func (c ConsentRecord) ComputeConsentHash() string {
	canon := struct {
		ConsentID    string `json:"consent_id"`
		GrantorKeyID string `json:"grantor_key_id"`
		Purpose      string `json:"purpose"`
		IssuedAt     string `json:"issued_at"`
		ExpiresAt    string `json:"expires_at,omitempty"`
		Revocable    bool   `json:"revocable"`
	}{
		ConsentID:    c.ConsentID,
		GrantorKeyID: c.GrantorKeyID,
		Purpose:      c.Purpose,
		IssuedAt:     c.IssuedAt.UTC().Format(time.RFC3339Nano),
		Revocable:    c.Revocable,
	}
	if !c.ExpiresAt.IsZero() {
		canon.ExpiresAt = c.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	b, _ := json.Marshal(canon)
	sum := blake3.Sum(b)
	return hex.EncodeToString(sum[:])
}

// Validate checks consent completeness and ConsentHash integrity.
func (c ConsentRecord) Validate() error {
	if err := ValidateSegment(c.ConsentID); err != nil {
		return fmt.Errorf("%w: %v", ErrConsentInvalid, err)
	}
	if err := ValidateSegment(c.GrantorKeyID); err != nil {
		return fmt.Errorf("%w: invalid grantor_key_id: %v", ErrConsentInvalid, err)
	}
	if strings.TrimSpace(c.Purpose) == "" || len(c.Purpose) > 128 {
		return fmt.Errorf("%w: purpose is required", ErrConsentInvalid)
	}
	if c.IssuedAt.IsZero() {
		return fmt.Errorf("%w: issued_at is required", ErrConsentInvalid)
	}
	if !c.ExpiresAt.IsZero() && c.ExpiresAt.Before(c.IssuedAt) {
		return fmt.Errorf("%w: expires_at precedes issued_at", ErrConsentInvalid)
	}
	expected := c.ComputeConsentHash()
	if c.ConsentHash != expected {
		return fmt.Errorf("%w: consent_hash mismatch (got %q, want %q)", ErrConsentInvalid, c.ConsentHash, expected)
	}
	return nil
}

// CipherChunkRef is the public, privacy-preserving reference to an encrypted
// chunk stored at enc/chunk/<2hex>/<CiphertextCID>.
type CipherChunkRef struct {
	Index          uint64 `json:"index"`
	CiphertextCID  string `json:"ciphertext_cid"`
	CiphertextSize int64  `json:"ciphertext_size"`
}

// PlainChunkMeta is stored ONLY inside the AES-256-GCM encrypted
// PrivateManifestPayload — never exposed in plaintext on storage nodes.
type PlainChunkMeta struct {
	Index           uint64 `json:"index"`
	PlaintextBLAKE3 string `json:"plaintext_blake3"`
	PlaintextSize   int64  `json:"plaintext_size"`
}

// PrivateManifestPayload holds sensitive plaintext digests and semantic
// metadata, encrypted inside SignedManifest.EncryptedPrivatePayloadHex.
type PrivateManifestPayload struct {
	ArtifactName        string            `json:"artifact_name"`
	PlaintextTotalSize  int64             `json:"plaintext_total_size"`
	PlaintextRootBLAKE3 string            `json:"plaintext_root_blake3"`
	PlaintextRootSHA256 string            `json:"plaintext_root_sha256"`
	Chunks              []PlainChunkMeta  `json:"chunks"`
	Labels              map[string]string `json:"labels,omitempty"`
	ProvenanceNotes     string            `json:"provenance_notes,omitempty"`
	ExperienceLedgerRef string            `json:"experience_ledger_ref,omitempty"`
}

// KeyEnvelope wraps the per-epoch 32-byte DEK for one authorized recipient
// using ephemeral X25519 ECDH + HKDF-SHA256 + AES-256-GCM.
type KeyEnvelope struct {
	RecipientKeyID     string `json:"recipient_key_id"`
	RecipientX25519Hex string `json:"recipient_x25519_hex"`
	EphemeralPubHex    string `json:"ephemeral_pub_hex"`
	NonceHex           string `json:"nonce_hex"`
	WrappedDEKHex      string `json:"wrapped_dek_hex"`
}

// SignedManifest is the canonical, verifiable, encrypted manifest stored at
// `enc/manifest/<2hex>/<ManifestCID>`. It exposes zero plaintext digests or
// plaintext metadata to storage providers or unauthorized peers.
type SignedManifest struct {
	Version                    string           `json:"version"`
	ArtifactID                 string           `json:"artifact_id"`
	Kind                       ArtifactKind     `json:"kind"`
	Epoch                      uint64           `json:"epoch"`
	ParentManifestCID          string           `json:"parent_manifest_cid,omitempty"`
	Owner                      PublicIdentity   `json:"owner"`
	License                    LicensePolicy    `json:"license"`
	Consent                    ConsentRecord    `json:"consent"`
	EpochSaltHex               string           `json:"epoch_salt_hex"`
	CipherChunks               []CipherChunkRef `json:"cipher_chunks"`
	PrivatePayloadNonceHex     string           `json:"private_payload_nonce_hex"`
	EncryptedPrivatePayloadHex string           `json:"encrypted_private_payload_hex"`
	KeyEnvelopes               []KeyEnvelope    `json:"key_envelopes"`
	CreatedAt                  time.Time        `json:"created_at"`
	SignatureHex               string           `json:"signature_hex"`
	ManifestCID                string           `json:"manifest_cid,omitempty"`
}

func (m *SignedManifest) signableBytes() ([]byte, error) {
	clone := *m
	clone.SignatureHex = ""
	clone.ManifestCID = ""
	return json.Marshal(clone)
}

// Sign signs m with owner's Ed25519 key and populates SignatureHex and ManifestCID.
func (m *SignedManifest) Sign(owner *Identity) error {
	if owner == nil || len(owner.Ed25519Priv) != ed25519.PrivateKeySize {
		return fmt.Errorf("%w: missing owner signing key", ErrBadSignature)
	}
	if m.Owner.KeyID != owner.KeyID {
		return fmt.Errorf("%w: owner key_id mismatch", ErrBadSignature)
	}
	payload, err := m.signableBytes()
	if err != nil {
		return err
	}
	sig := owner.SignDomain(domainManifestSign, payload)
	m.SignatureHex = hex.EncodeToString(sig)
	cid, err := m.ComputeCID()
	if err != nil {
		return err
	}
	m.ManifestCID = cid
	return nil
}

// ComputeCID computes the canonical BLAKE3 content identifier of the signed
// manifest (covering SignatureHex, excluding ManifestCID itself).
func (m *SignedManifest) ComputeCID() (string, error) {
	clone := *m
	clone.ManifestCID = ""
	b, err := json.Marshal(clone)
	if err != nil {
		return "", err
	}
	sum := blake3.Sum(b)
	return hex.EncodeToString(sum[:]), nil
}

// Verify structurally validates m, verifies the Ed25519 signature over the
// canonical payload, and verifies ManifestCID if populated.
func (m *SignedManifest) Verify(trustedOwnerEdPubs []ed25519.PublicKey) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	if m.Version != FormatVersion {
		return fmt.Errorf("%w: unsupported version %q", ErrInvalidManifest, m.Version)
	}
	if err := ValidateSegment(m.ArtifactID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if !m.Kind.Valid() {
		return fmt.Errorf("%w: %q", ErrInvalidKind, m.Kind)
	}
	if m.Epoch == 0 {
		return fmt.Errorf("%w: epoch must be >= 1", ErrInvalidManifest)
	}
	if m.ParentManifestCID != "" {
		if err := ValidateHexDigest(m.ParentManifestCID); err != nil {
			return fmt.Errorf("%w: invalid parent_manifest_cid: %v", ErrInvalidManifest, err)
		}
	}
	ownerEdPub, _, err := m.Owner.Validate()
	if err != nil {
		return err
	}
	if len(trustedOwnerEdPubs) > 0 {
		trusted := false
		for _, k := range trustedOwnerEdPubs {
			if slices.Equal(k, ownerEdPub) {
				trusted = true
				break
			}
		}
		if !trusted {
			return fmt.Errorf("%w: owner %s not in trusted key set", ErrUntrustedOwner, m.Owner.KeyID)
		}
	}
	if err := m.License.Validate(); err != nil {
		return err
	}
	if err := m.Consent.Validate(); err != nil {
		return err
	}
	salt, err := hex.DecodeString(m.EpochSaltHex)
	if err != nil || len(salt) != SaltSize {
		return fmt.Errorf("%w: invalid epoch_salt_hex", ErrInvalidManifest)
	}
	if len(m.CipherChunks) == 0 {
		return fmt.Errorf("%w: cipher_chunks must be non-empty", ErrInvalidManifest)
	}
	for i, ref := range m.CipherChunks {
		if ref.Index != uint64(i) {
			return fmt.Errorf("%w: non-contiguous chunk index %d at position %d", ErrInvalidManifest, ref.Index, i)
		}
		if err := ValidateHexDigest(ref.CiphertextCID); err != nil {
			return fmt.Errorf("%w: chunk %d ciphertext_cid: %v", ErrInvalidManifest, i, err)
		}
		if ref.CiphertextSize <= chunkFrameHeaderSize+16 {
			return fmt.Errorf("%w: chunk %d ciphertext_size too small (%d)", ErrInvalidManifest, i, ref.CiphertextSize)
		}
	}

	sig, err := hex.DecodeString(m.SignatureHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("%w: malformed signature_hex", ErrBadSignature)
	}
	payload, err := m.signableBytes()
	if err != nil {
		return err
	}
	if !VerifyDomain(ownerEdPub, domainManifestSign, payload, sig) {
		return fmt.Errorf("%w: manifest Ed25519 signature invalid", ErrBadSignature)
	}
	computedCID, err := m.ComputeCID()
	if err != nil {
		return err
	}
	if m.ManifestCID != "" && m.ManifestCID != computedCID {
		return fmt.Errorf("%w: manifest_cid mismatch (got %s, want %s)", ErrInvalidManifest, m.ManifestCID, computedCID)
	}
	return nil
}

// FindEnvelope returns the KeyEnvelope matching recipientKeyID, if present.
func (m *SignedManifest) FindEnvelope(recipientKeyID string) (KeyEnvelope, bool) {
	for _, env := range m.KeyEnvelopes {
		if env.RecipientKeyID == recipientKeyID {
			return env, true
		}
	}
	return KeyEnvelope{}, false
}

// RevocationEntry records when a peer was revoked and the first epoch in which
// the peer MUST NOT receive key envelopes. If the peer is later reauthorized at
// a strictly newer epoch, ReauthorizedAtEpoch records the first epoch in which
// access resumes while preserving revocation for earlier revoked epochs.
type RevocationEntry struct {
	PeerKeyID           string    `json:"peer_key_id"`
	RevokedAtEpoch      uint64    `json:"revoked_at_epoch"`
	ReauthorizedAtEpoch uint64    `json:"reauthorized_at_epoch,omitempty"`
	RevokedAt           time.Time `json:"revoked_at"`
	Reason              string    `json:"reason,omitempty"`
}

// SignedRevocationList is the owner-signed list of revoked peers (by epoch)
// and revoked consent IDs stored at `enc/revoke/<owner_key_id>`.
type SignedRevocationList struct {
	Version         string            `json:"version"`
	Owner           PublicIdentity    `json:"owner"`
	CurrentEpoch    uint64            `json:"current_epoch"`
	Seq             uint64            `json:"seq"`
	RevokedPeers    []RevocationEntry `json:"revoked_peers"`
	RevokedConsents []string          `json:"revoked_consents,omitempty"`
	UpdatedAt       time.Time         `json:"updated_at"`
	SignatureHex    string            `json:"signature_hex"`
}

func (r *SignedRevocationList) signableBytes() ([]byte, error) {
	clone := *r
	clone.SignatureHex = ""
	return json.Marshal(clone)
}

// Sign signs the revocation list with owner's Ed25519 key.
func (r *SignedRevocationList) Sign(owner *Identity) error {
	if owner == nil || r.Owner.KeyID != owner.KeyID {
		return fmt.Errorf("%w: revocation list owner mismatch", ErrBadSignature)
	}
	payload, err := r.signableBytes()
	if err != nil {
		return err
	}
	r.SignatureHex = hex.EncodeToString(owner.SignDomain(domainRevocationSign, payload))
	return nil
}

// Verify checks the owner identity and Ed25519 signature of the revocation list.
func (r *SignedRevocationList) Verify(expectedOwnerEdPub ed25519.PublicKey) error {
	if r == nil {
		return fmt.Errorf("%w: nil revocation list", ErrInvalidManifest)
	}
	if r.Version != FormatVersion {
		return fmt.Errorf("%w: unsupported revocation list version %q", ErrInvalidManifest, r.Version)
	}
	ownerEdPub, _, err := r.Owner.Validate()
	if err != nil {
		return err
	}
	if len(expectedOwnerEdPub) > 0 && !slices.Equal(ownerEdPub, expectedOwnerEdPub) {
		return fmt.Errorf("%w: revocation list signed by unexpected owner", ErrUntrustedOwner)
	}
	for _, entry := range r.RevokedPeers {
		if entry.ReauthorizedAtEpoch != 0 && entry.ReauthorizedAtEpoch <= entry.RevokedAtEpoch {
			return fmt.Errorf("%w: invalid revocation epoch interval [%d, %d) for peer %s", ErrInvalidManifest, entry.RevokedAtEpoch, entry.ReauthorizedAtEpoch, entry.PeerKeyID)
		}
	}
	sig, err := hex.DecodeString(r.SignatureHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("%w: invalid revocation list signature hex", ErrBadSignature)
	}
	payload, err := r.signableBytes()
	if err != nil {
		return err
	}
	if !VerifyDomain(ownerEdPub, domainRevocationSign, payload, sig) {
		return fmt.Errorf("%w: revocation list Ed25519 signature invalid", ErrBadSignature)
	}
	return nil
}

// IsPeerRevoked reports whether peerKeyID is revoked for epoch (i.e. revoked at
// or before epoch and not subsequently reauthorized at or before epoch).
func (r *SignedRevocationList) IsPeerRevoked(peerKeyID string, epoch uint64) bool {
	if r == nil {
		return false
	}
	for _, entry := range r.RevokedPeers {
		if entry.PeerKeyID == peerKeyID && epoch >= entry.RevokedAtEpoch &&
			(entry.ReauthorizedAtEpoch == 0 || epoch < entry.ReauthorizedAtEpoch) {
			return true
		}
	}
	return false
}

// IsConsentRevoked reports whether consentID has been revoked by the owner.
func (r *SignedRevocationList) IsConsentRevoked(consentID string) bool {
	if r == nil {
		return false
	}
	return slices.Contains(r.RevokedConsents, consentID)
}

// DeletionTombstone is a signed cryptographic erasure record published at
// `enc/tombstone/<artifact_id>` when an owner calls CryptoShred.
type DeletionTombstone struct {
	Version        string         `json:"version"`
	ArtifactID     string         `json:"artifact_id"`
	Owner          PublicIdentity `json:"owner"`
	DeletedAtEpoch uint64         `json:"deleted_at_epoch"`
	DeletedAt      time.Time      `json:"deleted_at"`
	Reason         string         `json:"reason,omitempty"`
	SignatureHex   string         `json:"signature_hex"`
}

func (t *DeletionTombstone) signableBytes() ([]byte, error) {
	clone := *t
	clone.SignatureHex = ""
	return json.Marshal(clone)
}

// Sign signs the tombstone with owner's Ed25519 key.
func (t *DeletionTombstone) Sign(owner *Identity) error {
	if owner == nil || t.Owner.KeyID != owner.KeyID {
		return fmt.Errorf("%w: tombstone owner mismatch", ErrBadSignature)
	}
	payload, err := t.signableBytes()
	if err != nil {
		return err
	}
	t.SignatureHex = hex.EncodeToString(owner.SignDomain(domainTombstoneSign, payload))
	return nil
}

// Verify checks the tombstone signature and owner match.
func (t *DeletionTombstone) Verify(expectedOwnerEdPub ed25519.PublicKey) error {
	if t == nil {
		return fmt.Errorf("%w: nil tombstone", ErrInvalidManifest)
	}
	if t.Version != FormatVersion {
		return fmt.Errorf("%w: unsupported tombstone version %q", ErrInvalidManifest, t.Version)
	}
	if err := ValidateSegment(t.ArtifactID); err != nil {
		return fmt.Errorf("%w: tombstone artifact_id: %v", ErrInvalidManifest, err)
	}
	ownerEdPub, _, err := t.Owner.Validate()
	if err != nil {
		return err
	}
	if len(expectedOwnerEdPub) > 0 && !slices.Equal(ownerEdPub, expectedOwnerEdPub) {
		return fmt.Errorf("%w: tombstone signed by unexpected owner", ErrUntrustedOwner)
	}
	sig, err := hex.DecodeString(t.SignatureHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("%w: invalid tombstone signature hex", ErrBadSignature)
	}
	payload, err := t.signableBytes()
	if err != nil {
		return err
	}
	if !VerifyDomain(ownerEdPub, domainTombstoneSign, payload, sig) {
		return fmt.Errorf("%w: tombstone Ed25519 signature invalid", ErrBadSignature)
	}
	return nil
}

// HeadPointer is stored at `enc/head/<artifact_id>` to locate the latest epoch manifest.
type HeadPointer struct {
	ArtifactID  string    `json:"artifact_id"`
	Epoch       uint64    `json:"epoch"`
	ManifestCID string    `json:"manifest_cid"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// BundleArtifactEntry describes one exported artifact inside an AuthorizedBundleIndex.
type BundleArtifactEntry struct {
	ArtifactID  string       `json:"artifact_id"`
	Kind        ArtifactKind `json:"kind"`
	Epoch       uint64       `json:"epoch"`
	ManifestCID string       `json:"manifest_cid"`
}

// BundleRevocationRef pins an owner-signed revocation list exported into an
// offline bundle so deletion or rollback of enc/revoke/<owner_key_id> is
// rejected fail-closed during RestoreAndVerify.
type BundleRevocationRef struct {
	Owner        PublicIdentity `json:"owner"`
	Seq          uint64         `json:"seq"`
	CurrentEpoch uint64         `json:"current_epoch"`
}

// AuthorizedBundleIndex is the Ed25519-signed index written into a portable
// `file://` VFS directory during ExportAuthorizedBundle.
type AuthorizedBundleIndex struct {
	Version          string                `json:"version"`
	Owner            PublicIdentity        `json:"owner"`
	ExportedAt       time.Time             `json:"exported_at"`
	Artifacts        []BundleArtifactEntry `json:"artifacts"`
	HasRevocationLog bool                  `json:"has_revocation_log"`
	RevocationLogs   []BundleRevocationRef `json:"revocation_logs,omitempty"`
	SignatureHex     string                `json:"signature_hex"`
}

func (b *AuthorizedBundleIndex) signableBytes() ([]byte, error) {
	clone := *b
	clone.SignatureHex = ""
	return json.Marshal(clone)
}

// Sign signs the bundle index with owner's Ed25519 key.
func (b *AuthorizedBundleIndex) Sign(owner *Identity) error {
	if owner == nil || b.Owner.KeyID != owner.KeyID {
		return fmt.Errorf("%w: bundle index owner mismatch", ErrBadSignature)
	}
	payload, err := b.signableBytes()
	if err != nil {
		return err
	}
	b.SignatureHex = hex.EncodeToString(owner.SignDomain(domainBundleIndexSign, payload))
	return nil
}

// Verify checks the bundle index signature and optional owner trust set.
func (b *AuthorizedBundleIndex) Verify(trustedOwnerEdPubs []ed25519.PublicKey) error {
	if b == nil {
		return fmt.Errorf("%w: nil bundle index", ErrInvalidManifest)
	}
	if b.Version != FormatVersion {
		return fmt.Errorf("%w: unsupported bundle index version %q", ErrInvalidManifest, b.Version)
	}
	ownerEdPub, _, err := b.Owner.Validate()
	if err != nil {
		return err
	}
	if len(trustedOwnerEdPubs) > 0 {
		trusted := false
		for _, k := range trustedOwnerEdPubs {
			if slices.Equal(k, ownerEdPub) {
				trusted = true
				break
			}
		}
		if !trusted {
			return fmt.Errorf("%w: bundle owner %s not trusted", ErrUntrustedOwner, b.Owner.KeyID)
		}
	}
	for _, ref := range b.RevocationLogs {
		if _, _, err := ref.Owner.Validate(); err != nil {
			return fmt.Errorf("%w: bundle revocation ref owner invalid: %v", ErrInvalidManifest, err)
		}
		if ref.Seq == 0 || ref.CurrentEpoch == 0 {
			return fmt.Errorf("%w: bundle revocation ref for %s requires seq>=1 and epoch>=1", ErrInvalidManifest, ref.Owner.KeyID)
		}
	}
	sig, err := hex.DecodeString(b.SignatureHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("%w: invalid bundle signature hex", ErrBadSignature)
	}
	payload, err := b.signableBytes()
	if err != nil {
		return err
	}
	if !VerifyDomain(ownerEdPub, domainBundleIndexSign, payload, sig) {
		return fmt.Errorf("%w: bundle index Ed25519 signature invalid", ErrBadSignature)
	}
	return nil
}

// ReadPolicy configures fail-closed authorization, licensing, consent, and
// revocation enforcement during ReadArtifact and RestoreAndVerify.
type ReadPolicy struct {
	// TrustedOwnerEdPubs restricts reads to manifests signed by one of these keys.
	// Empty means any structurally valid owner key is accepted.
	TrustedOwnerEdPubs []ed25519.PublicKey

	// AllowedLicenses restricts reads to SPDX identifiers in this slice (if non-empty).
	AllowedLicenses []string

	// RequireCommercialOK fails closed if License.CommercialAllowed is false.
	RequireCommercialOK bool

	// RequireRedistributionOK fails closed if License.RedistributionAllowed is false.
	RequireRedistributionOK bool

	// RequiredPurpose fails closed if Consent.Purpose != RequiredPurpose (if non-empty).
	RequiredPurpose string

	// CheckTime is the reference time for consent expiry checks. Defaults to time.Now().UTC().
	CheckTime time.Time

	// IgnoreRevocationList disables lookup of the remote SignedRevocationList
	// (used ONLY in negative-regime tests demonstrating cached prior-epoch decryption).
	IgnoreRevocationList bool
}

// Enforce checks manifest license and consent compliance against p.
func (p ReadPolicy) Enforce(m *SignedManifest, revList *SignedRevocationList, readerKeyID string) error {
	if len(p.AllowedLicenses) > 0 && !slices.Contains(p.AllowedLicenses, m.License.SPDX) {
		return fmt.Errorf("%w: spdx %q not in allowed set %v", ErrLicenseDenied, m.License.SPDX, p.AllowedLicenses)
	}
	if p.RequireCommercialOK && !m.License.CommercialAllowed {
		return fmt.Errorf("%w: commercial use disallowed for %s", ErrLicenseDenied, m.ArtifactID)
	}
	if p.RequireRedistributionOK && !m.License.RedistributionAllowed {
		return fmt.Errorf("%w: redistribution disallowed for %s", ErrLicenseDenied, m.ArtifactID)
	}
	now := p.CheckTime
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if !m.Consent.ExpiresAt.IsZero() && now.After(m.Consent.ExpiresAt) {
		return fmt.Errorf("%w: consent %s expired at %s", ErrConsentExpired, m.Consent.ConsentID, m.Consent.ExpiresAt.UTC().Format(time.RFC3339))
	}
	if p.RequiredPurpose != "" && m.Consent.Purpose != p.RequiredPurpose {
		return fmt.Errorf("%w: consent purpose %q != required %q", ErrConsentDenied, m.Consent.Purpose, p.RequiredPurpose)
	}
	if revList != nil {
		if revList.IsConsentRevoked(m.Consent.ConsentID) {
			return fmt.Errorf("%w: consent %s revoked by owner", ErrConsentRevoked, m.Consent.ConsentID)
		}
		if readerKeyID != "" && revList.IsPeerRevoked(readerKeyID, m.Epoch) {
			return fmt.Errorf("%w: peer %s revoked at epoch <= %d", ErrPeerRevoked, readerKeyID, m.Epoch)
		}
	}
	return nil
}
