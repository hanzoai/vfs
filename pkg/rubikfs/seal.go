// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rubikfs

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/blake3"
)

const (
	packMagic     = "RBKP\x01"
	manifestMagic = "RBKM\x01"
	aadLen        = 32
)

// VolumeCipher implements vfs.Cipher using AES-256-GCM with a key derived
// uniquely per (tenant_id, volume_id) via BLAKE3 key derivation. Every Seal
// generates a fresh 96-bit CSPRNG nonce, ensuring identical plaintexts across
// volumes (or even within the same volume) produce distinct ciphertexts.
type VolumeCipher struct {
	scope Scope
	keyID string
	aead  cipher.AEAD
}

func writeLenPrefixed(h *blake3.Hasher, s string) {
	var lb [8]byte
	binary.BigEndian.PutUint64(lb[:], uint64(len(s)))
	_, _ = h.Write(lb[:])
	_, _ = h.WriteString(s)
}

// NewVolumeCipher derives an isolated per-volume AES-256-GCM cipher from masterSecret.
// Two volumes with different (tenant_id, volume_id) always derive independent keys
// even if given the same masterSecret.
func NewVolumeCipher(scope Scope, masterSecret []byte) (*VolumeCipher, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if len(masterSecret) < 16 {
		return nil, fmt.Errorf("%w: master secret must be at least 16 bytes", ErrInvalidConfig)
	}

	var key [32]byte
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs rubikfs volume key v2\x00")
	writeLenPrefixed(h, scope.TenantID)
	writeLenPrefixed(h, scope.VolumeID)
	var mlen [8]byte
	binary.BigEndian.PutUint64(mlen[:], uint64(len(masterSecret)))
	_, _ = h.Write(mlen[:])
	_, _ = h.Write(masterSecret)
	h.Expand(key[:])

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	var idSum [16]byte
	ih := blake3.New()
	_, _ = ih.WriteString("hanzoai/vfs rubikfs key id v2\x00")
	writeLenPrefixed(ih, scope.TenantID)
	writeLenPrefixed(ih, scope.VolumeID)
	_, _ = ih.Write(key[:8])
	ih.Expand(idSum[:])

	return &VolumeCipher{
		scope: scope,
		keyID: hex.EncodeToString(idSum[:8]),
		aead:  aead,
	}, nil
}

// Seal encrypts and authenticates plaintext with a fresh random 96-bit nonce.
func (vc *VolumeCipher) Seal(plaintext []byte) ([]byte, string, error) {
	nonce := make([]byte, vc.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, "", err
	}
	out := make([]byte, len(nonce), len(nonce)+len(plaintext)+vc.aead.Overhead())
	copy(out, nonce)
	out = vc.aead.Seal(out, nonce, plaintext, []byte(vc.keyID))
	return out, vc.keyID, nil
}

// Open verifies and decrypts sealed bytes under keyID.
func (vc *VolumeCipher) Open(sealed []byte, keyID string) ([]byte, error) {
	if subtle.ConstantTimeCompare([]byte(keyID), []byte(vc.keyID)) != 1 {
		return nil, fmt.Errorf("%w: key id %q does not match volume key", ErrTenantIsolation, keyID)
	}
	ns := vc.aead.NonceSize()
	if len(sealed) < ns+vc.aead.Overhead() {
		return nil, fmt.Errorf("%w: truncated AEAD ciphertext", ErrCorruptPack)
	}
	plain, err := vc.aead.Open(nil, sealed[:ns], sealed[ns:], []byte(vc.keyID))
	if err != nil {
		return nil, fmt.Errorf("%w: AEAD authentication failed: %v", ErrCorruptPack, err)
	}
	return plain, nil
}

// Ensure VolumeCipher satisfies vfs.Cipher at compile time.
var _ vfs.Cipher = (*VolumeCipher)(nil)

// scopeNamespace derives an opaque, backend.ValidKey-compliant prefix for a
// (tenant_id, volume_id) scope so no plaintext tenant or volume names appear in S3 keys.
func scopeNamespace(scope Scope) string {
	th := blake3.New()
	_, _ = th.WriteString("hanzoai/vfs rubikfs tenant ns v2\x00")
	writeLenPrefixed(th, scope.TenantID)
	var tsum [8]byte
	th.Expand(tsum[:])

	vh := blake3.New()
	_, _ = vh.WriteString("hanzoai/vfs rubikfs volume ns v2\x00")
	writeLenPrefixed(vh, scope.TenantID)
	writeLenPrefixed(vh, scope.VolumeID)
	var vsum [8]byte
	vh.Expand(vsum[:])

	return "rubikfs/" + hex.EncodeToString(tsum[:]) + "/" + hex.EncodeToString(vsum[:])
}

// packBackendKey returns the backend object key for a pack within scope.
func packBackendKey(scope Scope, id PackID) string {
	h := hex.EncodeToString(id[:])
	return scopeNamespace(scope) + "/pack/" + h[:2] + "/" + h
}

// manifestBackendKey returns the backend object key for an image manifest within scope.
func manifestBackendKey(scope Scope, imageID string) string {
	ih := blake3.New()
	_, _ = ih.WriteString("hanzoai/vfs rubikfs manifest v2\x00")
	writeLenPrefixed(ih, scope.TenantID)
	writeLenPrefixed(ih, scope.VolumeID)
	writeLenPrefixed(ih, imageID)
	var sum [16]byte
	ih.Expand(sum[:])
	return scopeNamespace(scope) + "/manifest/" + hex.EncodeToString(sum[:])
}

// packAAD binds a sealed pack cryptographically to its exact (tenant, volume, image, pack).
func packAAD(scope Scope, imageID string, id PackID) [aadLen]byte {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs rubikfs pack aad v2\x00")
	writeLenPrefixed(h, scope.TenantID)
	writeLenPrefixed(h, scope.VolumeID)
	writeLenPrefixed(h, imageID)
	_, _ = h.Write(id[:])
	var out [aadLen]byte
	h.Expand(out[:])
	return out
}

// manifestAAD binds a sealed manifest cryptographically to its exact (tenant, volume, image).
func manifestAAD(scope Scope, imageID string) [aadLen]byte {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs rubikfs manifest aad v2\x00")
	writeLenPrefixed(h, scope.TenantID)
	writeLenPrefixed(h, scope.VolumeID)
	writeLenPrefixed(h, imageID)
	var out [aadLen]byte
	h.Expand(out[:])
	return out
}

// sealFramed seals (aad || plain) using c and frames [keyIDLen][keyID][ciphertext],
// matching hanzoai/vfs's self-describing seal contract in cipher.go.
func sealFramed(c vfs.Cipher, aad [aadLen]byte, plain []byte) ([]byte, error) {
	buf := make([]byte, aadLen+len(plain))
	copy(buf[:aadLen], aad[:])
	copy(buf[aadLen:], plain)

	sealed, keyID, err := c.Seal(buf)
	if err != nil {
		return nil, fmt.Errorf("rubikfs: seal: %w", err)
	}
	if len(keyID) == 0 || len(keyID) > 255 {
		return nil, fmt.Errorf("rubikfs: cipher returned invalid key id length %d", len(keyID))
	}
	if bytes.Contains(sealed, aad[:]) {
		return nil, fmt.Errorf("%w: cipher output contains plaintext aad", vfs.ErrBadCipher)
	}

	out := make([]byte, 1+len(keyID)+len(sealed))
	out[0] = byte(len(keyID))
	copy(out[1:], keyID)
	copy(out[1+len(keyID):], sealed)
	return out, nil
}

// unsealFramed reverses sealFramed and requires exact constant-time AAD match.
func unsealFramed(c vfs.Cipher, expectedAAD [aadLen]byte, frame []byte) ([]byte, error) {
	if len(frame) < 2 || len(frame) < 1+int(frame[0]) {
		return nil, fmt.Errorf("%w: truncated sealed frame", ErrCorruptPack)
	}
	n := 1 + int(frame[0])
	keyID := string(frame[1:n])
	plain, err := c.Open(frame[n:], keyID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptPack, err)
	}
	if len(plain) < aadLen {
		return nil, fmt.Errorf("%w: opened frame shorter than aad", ErrCorruptPack)
	}
	if subtle.ConstantTimeCompare(plain[:aadLen], expectedAAD[:]) != 1 {
		return nil, fmt.Errorf("%w: pack sealed for another tenant, volume, or image", ErrTenantIsolation)
	}
	return plain[aadLen:], nil
}

// packChunkEntry records one chunk's position inside the decompressed pack payload.
type packChunkEntry struct {
	ID  ChunkID
	Off uint32
	Len uint32
}

// encodePackPlaintext serializes a pack header + compressed payload prior to AEAD sealing.
// Format:
//
//	[magic 5B "RBKP\x01"][codec 1B][role 1B][chunkCount 4B][rawBytes 4B]
//	chunkCount * ([chunkID 32B][off 4B][len 4B])
//	[compressedBytesLen 4B][compressedBytes]
func encodePackPlaintext(codec CodecKind, role PackRole, entries []packChunkEntry, rawBytes uint32, compressed []byte) []byte {
	hdrLen := len(packMagic) + 1 + 1 + 4 + 4 + len(entries)*40 + 4
	out := make([]byte, hdrLen+len(compressed))
	p := 0
	copy(out[p:], packMagic)
	p += len(packMagic)
	out[p] = byte(codec)
	p++
	out[p] = byte(role)
	p++
	binary.LittleEndian.PutUint32(out[p:], uint32(len(entries)))
	p += 4
	binary.LittleEndian.PutUint32(out[p:], rawBytes)
	p += 4
	for _, e := range entries {
		copy(out[p:], e.ID[:])
		p += 32
		binary.LittleEndian.PutUint32(out[p:], e.Off)
		p += 4
		binary.LittleEndian.PutUint32(out[p:], e.Len)
		p += 4
	}
	binary.LittleEndian.PutUint32(out[p:], uint32(len(compressed)))
	p += 4
	copy(out[p:], compressed)
	return out
}

type decodedPack struct {
	Codec      CodecKind
	Role       PackRole
	RawBytes   uint32
	Entries    []packChunkEntry
	Compressed []byte
}

func decodePackPlaintext(raw []byte) (*decodedPack, error) {
	minHdr := len(packMagic) + 1 + 1 + 4 + 4 + 4
	if len(raw) < minHdr {
		return nil, fmt.Errorf("%w: truncated pack header", ErrCorruptPack)
	}
	if string(raw[:len(packMagic)]) != packMagic {
		return nil, fmt.Errorf("%w: bad pack magic", ErrCorruptPack)
	}
	p := len(packMagic)
	codec := CodecKind(raw[p])
	p++
	role := PackRole(raw[p])
	p++
	chunkCount := binary.LittleEndian.Uint32(raw[p:])
	p += 4
	rawBytes := binary.LittleEndian.Uint32(raw[p:])
	p += 4
	if rawBytes > MaxPackPlaintextBytes {
		return nil, fmt.Errorf("%w: pack rawBytes %d exceeds limit %d", ErrCorruptPack, rawBytes, MaxPackPlaintextBytes)
	}
	tableBytes := uint64(chunkCount) * 40
	if tableBytes+4 > uint64(len(raw)-p) {
		return nil, fmt.Errorf("%w: truncated pack chunk table", ErrCorruptPack)
	}
	entries := make([]packChunkEntry, chunkCount)
	for i := range entries {
		copy(entries[i].ID[:], raw[p:p+32])
		p += 32
		entries[i].Off = binary.LittleEndian.Uint32(raw[p:])
		p += 4
		entries[i].Len = binary.LittleEndian.Uint32(raw[p:])
		p += 4
		if uint64(entries[i].Off)+uint64(entries[i].Len) > uint64(rawBytes) {
			return nil, fmt.Errorf("%w: chunk %d slice out of pack bounds", ErrCorruptPack, i)
		}
	}
	compLen := binary.LittleEndian.Uint32(raw[p:])
	p += 4
	if int(compLen) != len(raw)-p {
		return nil, fmt.Errorf("%w: compressed payload length mismatch (%d != %d)", ErrCorruptPack, compLen, len(raw)-p)
	}
	return &decodedPack{
		Codec:      codec,
		Role:       role,
		RawBytes:   rawBytes,
		Entries:    entries,
		Compressed: raw[p:],
	}, nil
}
