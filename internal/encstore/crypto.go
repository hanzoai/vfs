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

// Package encstore implements a portable, encrypted, content-addressed storage
// format for datasets, model checkpoints, agent memories, research artifacts,
// and execution receipts on top of hanzoai/vfs's canonical pkg/backend.Backend
// key space (zero second disk format, zero external dependencies).
//
// Threat-model boundary: this package provides cryptographic confidentiality
// (AES-256-GCM AEAD with per-chunk HKDF-SHA256 derived keys and AAD binding),
// cryptographic integrity and authenticity (BLAKE3 + SHA-256 digests and
// Ed25519 manifest/revocation/tombstone signatures), and forward-secure key
// distribution on peer revocation (X25519 ECDH + HKDF-SHA256 + AES-256-GCM key
// envelopes across monotonic key epochs). It does not claim retroactive
// revocation of data already decrypted by a peer prior to revocation, nor does
// it claim hardware TEE attestation or economic slashing.
package encstore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/hanzoai/vfs/internal/blake3"
)

const (
	// FormatVersion identifies the canonical encrypted manifest schema version.
	FormatVersion = "encstore/v1"

	// KeySize is the AES-256 key size in bytes.
	KeySize = 32

	// SaltSize is the per-epoch HKDF salt size in bytes.
	SaltSize = 32

	// NonceSize is the AES-256-GCM standard 96-bit nonce size in bytes.
	NonceSize = 12

	// chunkFrameHeaderSize is 4B magic + 1B version + 12B nonce.
	chunkFrameHeaderSize = 4 + 1 + NonceSize

	chunkFrameVersion byte = 1

	domainManifestSign    = "hanzo.vfs.encstore.v1.manifest\x00"
	domainRevocationSign  = "hanzo.vfs.encstore.v1.revocation-list\x00"
	domainTombstoneSign   = "hanzo.vfs.encstore.v1.tombstone\x00"
	domainBundleIndexSign = "hanzo.vfs.encstore.v1.bundle-index\x00"
	domainChunkKeyPrefix  = "hanzo.vfs.encstore.v1.chunk-key|"
	domainPrivatePayload  = "hanzo.vfs.encstore.v1.private-manifest|"
	domainKeyEnvelope     = "hanzo.vfs.encstore.v1.key-envelope|"
	domainChunkAADPrefix  = "hanzo.vfs.encstore.v1.chunk-aad|"
	domainPrivAADPrefix   = "hanzo.vfs.encstore.v1.private-aad|"
	domainEnvAADPrefix    = "hanzo.vfs.encstore.v1.envelope-aad|"
)

var chunkFrameMagic = [4]byte{'H', 'V', 'E', 'C'}

// Identity holds an entity's Ed25519 signing identity and X25519 key-agreement
// identity. Both public keys are bound into the deterministic KeyID.
type Identity struct {
	DisplayName string
	KeyID       string
	Ed25519Priv ed25519.PrivateKey
	Ed25519Pub  ed25519.PublicKey
	X25519Priv  *ecdh.PrivateKey
	X25519Pub   *ecdh.PublicKey
}

// PublicIdentity is the shareable, non-secret half of an Identity.
type PublicIdentity struct {
	DisplayName   string `json:"display_name,omitempty"`
	KeyID         string `json:"key_id"`
	Ed25519PubHex string `json:"ed25519_pub_hex"`
	X25519PubHex  string `json:"x25519_pub_hex"`
	Role          string `json:"role,omitempty"`
}

// GenerateIdentity creates a fresh random Identity using crypto/rand.
func GenerateIdentity(displayName string) (*Identity, error) {
	edPub, edPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("encstore: generate ed25519 key: %w", err)
	}
	xPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("encstore: generate x25519 key: %w", err)
	}
	xPub := xPriv.PublicKey()
	keyID := DeriveKeyID(edPub, xPub.Bytes())
	return &Identity{
		DisplayName: displayName,
		KeyID:       keyID,
		Ed25519Priv: edPriv,
		Ed25519Pub:  edPub,
		X25519Priv:  xPriv,
		X25519Pub:   xPub,
	}, nil
}

// DeriveKeyID deterministically derives a 32-character lowercase hex KeyID
// (128 bits of BLAKE3) from the concatenation of the Ed25519 and X25519 public
// keys. The resulting KeyID strictly conforms to backend.ValidKey.
func DeriveKeyID(edPub, xPub []byte) string {
	buf := make([]byte, 0, len("hanzo.vfs.encstore.v1.keyid|")+len(edPub)+len(xPub))
	buf = append(buf, "hanzo.vfs.encstore.v1.keyid|"...)
	buf = append(buf, edPub...)
	buf = append(buf, xPub...)
	sum := blake3.Sum(buf)
	return hex.EncodeToString(sum[:16])
}

// Public returns the non-secret PublicIdentity for id.
func (id *Identity) Public(role string) PublicIdentity {
	return PublicIdentity{
		DisplayName:   id.DisplayName,
		KeyID:         id.KeyID,
		Ed25519PubHex: hex.EncodeToString(id.Ed25519Pub),
		X25519PubHex:  hex.EncodeToString(id.X25519Pub.Bytes()),
		Role:          role,
	}
}

// Validate checks that PublicIdentity has well-formed keys matching KeyID.
func (p PublicIdentity) Validate() (ed25519.PublicKey, *ecdh.PublicKey, error) {
	edBytes, err := hex.DecodeString(p.Ed25519PubHex)
	if err != nil || len(edBytes) != ed25519.PublicKeySize {
		return nil, nil, fmt.Errorf("%w: invalid ed25519 public key", ErrInvalidManifest)
	}
	xBytes, err := hex.DecodeString(p.X25519PubHex)
	if err != nil || len(xBytes) != 32 {
		return nil, nil, fmt.Errorf("%w: invalid x25519 public key", ErrInvalidManifest)
	}
	xPub, err := ecdh.X25519().NewPublicKey(xBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse x25519 public key: %v", ErrInvalidManifest, err)
	}
	expectedID := DeriveKeyID(edBytes, xBytes)
	if p.KeyID != expectedID {
		return nil, nil, fmt.Errorf("%w: key_id %q does not match public keys (want %q)", ErrInvalidManifest, p.KeyID, expectedID)
	}
	return ed25519.PublicKey(edBytes), xPub, nil
}

// SignDomain signs payload with domain separation using id.Ed25519Priv.
func (id *Identity) SignDomain(domain string, payload []byte) []byte {
	msg := make([]byte, 0, len(domain)+len(payload))
	msg = append(msg, domain...)
	msg = append(msg, payload...)
	return ed25519.Sign(id.Ed25519Priv, msg)
}

// VerifyDomain verifies an Ed25519 signature over domain || payload.
func VerifyDomain(pub ed25519.PublicKey, domain string, payload, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	msg := make([]byte, 0, len(domain)+len(payload))
	msg = append(msg, domain...)
	msg = append(msg, payload...)
	return ed25519.Verify(pub, msg, sig)
}

// hkdfExtract implements RFC 5869 HKDF-Extract with HMAC-SHA256.
func hkdfExtract(salt, ikm []byte) []byte {
	if len(salt) == 0 {
		salt = make([]byte, sha256.Size)
	}
	mac := hmac.New(sha256.New, salt)
	mac.Write(ikm)
	return mac.Sum(nil)
}

// hkdfExpand implements RFC 5869 HKDF-Expand with HMAC-SHA256.
func hkdfExpand(prk, info []byte, length int) ([]byte, error) {
	if length <= 0 || length > 255*sha256.Size {
		return nil, errors.New("encstore: invalid hkdf output length")
	}
	n := (length + sha256.Size - 1) / sha256.Size
	okm := make([]byte, 0, n*sha256.Size)
	var prev []byte
	for i := 1; i <= n; i++ {
		mac := hmac.New(sha256.New, prk)
		mac.Write(prev)
		mac.Write(info)
		mac.Write([]byte{byte(i)})
		prev = mac.Sum(nil)
		okm = append(okm, prev...)
	}
	return okm[:length], nil
}

// hkdfSHA256 derives length bytes of key material using RFC 5869 HKDF-SHA256.
func hkdfSHA256(ikm, salt, info []byte, length int) ([]byte, error) {
	prk := hkdfExtract(salt, ikm)
	defer zeroBytes(prk)
	return hkdfExpand(prk, info, length)
}

// randomBytes returns n cryptographically random bytes from crypto/rand.
func randomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return nil, fmt.Errorf("encstore: crypto/rand read: %w", err)
	}
	return b, nil
}

// zeroBytes overwrites b in place to minimize key residency in memory.
func zeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// buildChunkAAD constructs canonical Associated Authenticated Data binding each
// ciphertext chunk to its artifact, epoch, chunk index, kind, SPDX license, and
// consent hash. Any modification or chunk splicing fails AES-GCM verification.
func buildChunkAAD(artifactID string, epoch, chunkIndex uint64, kind ArtifactKind, spdxLicense, consentHash string) []byte {
	s := domainChunkAADPrefix +
		artifactID + "|" +
		strconv.FormatUint(epoch, 10) + "|" +
		strconv.FormatUint(chunkIndex, 10) + "|" +
		string(kind) + "|" +
		spdxLicense + "|" +
		consentHash
	return []byte(s)
}

func deriveChunkKey(dek, salt []byte, artifactID string, epoch, chunkIndex uint64) ([]byte, error) {
	info := domainChunkKeyPrefix + artifactID + "|epoch=" + strconv.FormatUint(epoch, 10) + "|idx=" + strconv.FormatUint(chunkIndex, 10)
	return hkdfSHA256(dek, salt, []byte(info), KeySize)
}

// encryptChunk encrypts a single plaintext chunk using AES-256-GCM with a
// per-chunk HKDF-SHA256 derived key, fresh CSPRNG 96-bit nonce, and canonical
// AAD binding. The returned frame format is:
//
//	[4B magic "HVEC"][1B version=1][12B nonce][ciphertext || 16B GCM tag]
func encryptChunk(dek, salt []byte, artifactID string, epoch, chunkIndex uint64, kind ArtifactKind, spdxLicense, consentHash string, plaintext []byte) ([]byte, error) {
	chunkKey, err := deriveChunkKey(dek, salt, artifactID, epoch, chunkIndex)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(chunkKey)

	block, err := aes.NewCipher(chunkKey)
	if err != nil {
		return nil, fmt.Errorf("encstore: aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encstore: gcm: %w", err)
	}
	nonce, err := randomBytes(NonceSize)
	if err != nil {
		return nil, err
	}
	aad := buildChunkAAD(artifactID, epoch, chunkIndex, kind, spdxLicense, consentHash)

	frame := make([]byte, chunkFrameHeaderSize, chunkFrameHeaderSize+len(plaintext)+gcm.Overhead())
	copy(frame[0:4], chunkFrameMagic[:])
	frame[4] = chunkFrameVersion
	copy(frame[5:chunkFrameHeaderSize], nonce)
	frame = gcm.Seal(frame, nonce, plaintext, aad)
	return frame, nil
}

// decryptChunk authenticates and decrypts a framed ciphertext chunk.
func decryptChunk(dek, salt []byte, artifactID string, epoch, chunkIndex uint64, kind ArtifactKind, spdxLicense, consentHash string, frame []byte) ([]byte, error) {
	if len(frame) < chunkFrameHeaderSize+16 {
		return nil, fmt.Errorf("%w: chunk %d frame truncated (%d bytes)", ErrChunkCorrupted, chunkIndex, len(frame))
	}
	if frame[0] != chunkFrameMagic[0] || frame[1] != chunkFrameMagic[1] || frame[2] != chunkFrameMagic[2] || frame[3] != chunkFrameMagic[3] {
		return nil, fmt.Errorf("%w: chunk %d bad frame magic", ErrChunkCorrupted, chunkIndex)
	}
	if frame[4] != chunkFrameVersion {
		return nil, fmt.Errorf("%w: chunk %d unsupported frame version %d", ErrChunkCorrupted, chunkIndex, frame[4])
	}
	nonce := frame[5:chunkFrameHeaderSize]
	ct := frame[chunkFrameHeaderSize:]

	chunkKey, err := deriveChunkKey(dek, salt, artifactID, epoch, chunkIndex)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(chunkKey)

	block, err := aes.NewCipher(chunkKey)
	if err != nil {
		return nil, fmt.Errorf("encstore: aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encstore: gcm: %w", err)
	}
	aad := buildChunkAAD(artifactID, epoch, chunkIndex, kind, spdxLicense, consentHash)
	pt, err := gcm.Open(nil, nonce, ct, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: chunk %d AEAD authentication failed: %v", ErrChunkCorrupted, chunkIndex, err)
	}
	return pt, nil
}

func buildPrivatePayloadAAD(artifactID string, epoch uint64, kind ArtifactKind, ownerKeyID, spdxLicense, consentHash string) []byte {
	s := domainPrivAADPrefix +
		artifactID + "|" +
		strconv.FormatUint(epoch, 10) + "|" +
		string(kind) + "|" +
		ownerKeyID + "|" +
		spdxLicense + "|" +
		consentHash
	return []byte(s)
}

func encryptPrivatePayload(dek, salt []byte, artifactID string, epoch uint64, kind ArtifactKind, ownerKeyID, spdxLicense, consentHash string, plaintextJSON []byte) (nonceHex, ctHex string, err error) {
	info := domainPrivatePayload + artifactID + "|epoch=" + strconv.FormatUint(epoch, 10)
	mKey, err := hkdfSHA256(dek, salt, []byte(info), KeySize)
	if err != nil {
		return "", "", err
	}
	defer zeroBytes(mKey)

	block, err := aes.NewCipher(mKey)
	if err != nil {
		return "", "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", "", err
	}
	nonce, err := randomBytes(NonceSize)
	if err != nil {
		return "", "", err
	}
	aad := buildPrivatePayloadAAD(artifactID, epoch, kind, ownerKeyID, spdxLicense, consentHash)
	ct := gcm.Seal(nil, nonce, plaintextJSON, aad)
	return hex.EncodeToString(nonce), hex.EncodeToString(ct), nil
}

func decryptPrivatePayload(dek, salt []byte, artifactID string, epoch uint64, kind ArtifactKind, ownerKeyID, spdxLicense, consentHash, nonceHex, ctHex string) ([]byte, error) {
	nonce, err := hex.DecodeString(nonceHex)
	if err != nil || len(nonce) != NonceSize {
		return nil, fmt.Errorf("%w: invalid private payload nonce", ErrInvalidManifest)
	}
	ct, err := hex.DecodeString(ctHex)
	if err != nil || len(ct) < 16 {
		return nil, fmt.Errorf("%w: invalid private payload ciphertext", ErrInvalidManifest)
	}

	info := domainPrivatePayload + artifactID + "|epoch=" + strconv.FormatUint(epoch, 10)
	mKey, err := hkdfSHA256(dek, salt, []byte(info), KeySize)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(mKey)

	block, err := aes.NewCipher(mKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	aad := buildPrivatePayloadAAD(artifactID, epoch, kind, ownerKeyID, spdxLicense, consentHash)
	pt, err := gcm.Open(nil, nonce, ct, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: private manifest decryption failed: %v", ErrAccessDenied, err)
	}
	return pt, nil
}

func buildEnvelopeAAD(artifactID string, epoch uint64, recipientKeyID, consentHash string) []byte {
	s := domainEnvAADPrefix +
		artifactID + "|" +
		strconv.FormatUint(epoch, 10) + "|" +
		recipientKeyID + "|" +
		consentHash
	return []byte(s)
}

// wrapDEKForRecipient creates an X25519 ECDH + HKDF-SHA256 + AES-256-GCM
// KeyEnvelope wrapping dek for recipient.
func wrapDEKForRecipient(dek, salt []byte, artifactID string, epoch uint64, consentHash string, recipient PublicIdentity) (KeyEnvelope, error) {
	_, recipientXPub, err := recipient.Validate()
	if err != nil {
		return KeyEnvelope{}, err
	}
	ephPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return KeyEnvelope{}, fmt.Errorf("encstore: ephemeral x25519: %w", err)
	}
	ephPub := ephPriv.PublicKey()
	shared, err := ephPriv.ECDH(recipientXPub)
	if err != nil {
		return KeyEnvelope{}, fmt.Errorf("encstore: x25519 ecdh: %w", err)
	}
	defer zeroBytes(shared)

	ephHex := hex.EncodeToString(ephPub.Bytes())
	recHex := hex.EncodeToString(recipientXPub.Bytes())
	info := domainKeyEnvelope + artifactID + "|epoch=" + strconv.FormatUint(epoch, 10) + "|" + recipient.KeyID + "|" + ephHex + "|" + recHex
	kek, err := hkdfSHA256(shared, salt, []byte(info), KeySize)
	if err != nil {
		return KeyEnvelope{}, err
	}
	defer zeroBytes(kek)

	block, err := aes.NewCipher(kek)
	if err != nil {
		return KeyEnvelope{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return KeyEnvelope{}, err
	}
	nonce, err := randomBytes(NonceSize)
	if err != nil {
		return KeyEnvelope{}, err
	}
	aad := buildEnvelopeAAD(artifactID, epoch, recipient.KeyID, consentHash)
	wrapped := gcm.Seal(nil, nonce, dek, aad)

	return KeyEnvelope{
		RecipientKeyID:     recipient.KeyID,
		RecipientX25519Hex: recHex,
		EphemeralPubHex:    ephHex,
		NonceHex:           hex.EncodeToString(nonce),
		WrappedDEKHex:      hex.EncodeToString(wrapped),
	}, nil
}

// unwrapDEK recovers the 32-byte DEK from env using recipient.X25519Priv.
func unwrapDEK(env KeyEnvelope, salt []byte, artifactID string, epoch uint64, consentHash string, recipient *Identity) ([]byte, error) {
	if recipient == nil || recipient.X25519Priv == nil {
		return nil, fmt.Errorf("%w: missing recipient private key", ErrAccessDenied)
	}
	if env.RecipientKeyID != recipient.KeyID {
		return nil, fmt.Errorf("%w: envelope recipient mismatch", ErrAccessDenied)
	}
	ephBytes, err := hex.DecodeString(env.EphemeralPubHex)
	if err != nil || len(ephBytes) != 32 {
		return nil, fmt.Errorf("%w: invalid ephemeral public key", ErrAccessDenied)
	}
	ephPub, err := ecdh.X25519().NewPublicKey(ephBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse ephemeral x25519 key: %v", ErrAccessDenied, err)
	}
	nonce, err := hex.DecodeString(env.NonceHex)
	if err != nil || len(nonce) != NonceSize {
		return nil, fmt.Errorf("%w: invalid envelope nonce", ErrAccessDenied)
	}
	wrapped, err := hex.DecodeString(env.WrappedDEKHex)
	if err != nil || len(wrapped) < KeySize+16 {
		return nil, fmt.Errorf("%w: invalid wrapped dek", ErrAccessDenied)
	}

	shared, err := recipient.X25519Priv.ECDH(ephPub)
	if err != nil {
		return nil, fmt.Errorf("%w: x25519 ecdh failed: %v", ErrAccessDenied, err)
	}
	defer zeroBytes(shared)

	recHex := hex.EncodeToString(recipient.X25519Pub.Bytes())
	info := domainKeyEnvelope + artifactID + "|epoch=" + strconv.FormatUint(epoch, 10) + "|" + recipient.KeyID + "|" + env.EphemeralPubHex + "|" + recHex
	kek, err := hkdfSHA256(shared, salt, []byte(info), KeySize)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(kek)

	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	aad := buildEnvelopeAAD(artifactID, epoch, recipient.KeyID, consentHash)
	dek, err := gcm.Open(nil, nonce, wrapped, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: key envelope decryption failed: %v", ErrAccessDenied, err)
	}
	if len(dek) != KeySize {
		zeroBytes(dek)
		return nil, fmt.Errorf("%w: unwrapped dek has unexpected length %d", ErrAccessDenied, len(dek))
	}
	return dek, nil
}
