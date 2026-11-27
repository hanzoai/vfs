// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package visorfuse

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/zeebo/blake3"
)

const (
	// DefaultCheckpointPageSize is the chunk size for guest memory pages (64 KiB).
	DefaultCheckpointPageSize = 64 << 10

	// DefaultPublishSLA is the hanzoai/vfs#31 S3 publication SLA (< 5 s).
	DefaultPublishSLA = 5 * time.Second

	// CheckpointFormatVersion is the wire schema version for CheckpointManifest.
	CheckpointFormatVersion = 1

	ckptMagicPage     uint32 = 0x56435047 // "VCPG"
	ckptMagicManifest uint32 = 0x56434D46 // "VCMF"

	ckptFlagCompressed byte = 1 << 0
	ckptFlagEncrypted  byte = 1 << 1

	maxManifestBytes int64 = 32 << 20
	maxPageBytes     int64 = 16 << 20
)

// OpenDescriptor captures a sandbox's open file descriptor state pointing into
// the external VFS mount so it can be cleanly re-bound on the target node.
type OpenDescriptor struct {
	FD     int         `json:"fd"`
	Handle uint64      `json:"handle,omitempty"`
	Path   string      `json:"path"`
	Inode  uint64      `json:"inode"`
	Offset int64       `json:"offset"`
	Flags  uint32      `json:"flags"`
	Mode   fs.FileMode `json:"mode"`
	Size   uint64      `json:"size"`
}

// SandboxSnapshot describes the guest sandbox state to checkpoint to S3 while
// the external VFS volume lives outside the checkpoint.
type SandboxSnapshot struct {
	CheckpointID  string            `json:"checkpoint_id"`
	SandboxID     string            `json:"sandbox_id"`
	SourceNodeID  string            `json:"source_node_id"`
	TenantID      string            `json:"tenant_id"`
	VolumeID      string            `json:"volume_id"`
	PrivilegeMode PrivilegeMode     `json:"privilege_mode"`
	UID           uint32            `json:"uid"`
	GID           uint32            `json:"gid"`
	Memory        []byte            `json:"-"`
	Descriptors   []OpenDescriptor  `json:"descriptors,omitempty"`
	Registers     []byte            `json:"registers,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
}

// CheckpointConfig configures the S3-backed checkpoint store.
type CheckpointConfig struct {
	// Store is the object-store backend (pkg/backend/s3 or pkg/backend/file).
	Store backend.Backend

	// Cipher optionally seals every memory page and checkpoint manifest using
	// vfs.Hybrid (X25519 + ML-KEM-768 post-quantum encryption).
	Cipher vfs.Cipher

	// HMACKey signs the canonical checkpoint manifest using keyed BLAKE3-256.
	// Required to prevent unsigned or forged manifests.
	HMACKey []byte

	// PageSize sets the memory chunking granularity (default 64 KiB).
	PageSize int

	// Compress enables fast flate compression of non-zero memory pages.
	// Only pages that compress strictly smaller than raw bytes are stored compressed.
	Compress bool

	// MaxRetries controls retry attempts for transient lost-ACK errors on S3 PUT.
	// Default 3.
	MaxRetries int

	// PublishSLA optionally overrides the default 5 s S3 publication SLA check.
	PublishSLA time.Duration
}

func (c CheckpointConfig) normalized() (CheckpointConfig, error) {
	if c.Store == nil {
		return c, fmt.Errorf("%w: checkpoint store backend is required", ErrInvalidMountConfig)
	}
	if len(c.HMACKey) < 16 {
		return c, fmt.Errorf("%w: HMACKey must be at least 16 bytes", ErrInvalidMountConfig)
	}
	if c.PageSize <= 0 {
		c.PageSize = DefaultCheckpointPageSize
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	if c.PublishSLA <= 0 {
		c.PublishSLA = DefaultPublishSLA
	}
	return c, nil
}

// PageRef records one chunked guest memory page in CheckpointManifest.
type PageRef struct {
	Index        int    `json:"index"`
	Zero         bool   `json:"zero,omitempty"`
	PlainDigest  string `json:"plain_digest,omitempty"`
	StoredDigest string `json:"stored_digest,omitempty"`
	ObjectKey    string `json:"object_key,omitempty"`
	RawLen       int    `json:"raw_len"`
	StoredLen    int    `json:"stored_len,omitempty"`
	Compressed   bool   `json:"compressed,omitempty"`
	Encrypted    bool   `json:"encrypted,omitempty"`
	KeyID        string `json:"key_id,omitempty"`
}

// CheckpointStats captures exact storage, dedup, compression, and runtime
// upload telemetry. Deterministic content statistics are signed into the S3
// manifest; post-upload runtime telemetry fields (StoredManifestSize, S3PutOps,
// LostACKRecovered, CheckpointDuration) are tagged json:"-" so populating them
// after manifest upload never invalidates ManifestDigest or Signature.
type CheckpointStats struct {
	RawMemoryBytes     uint64        `json:"raw_memory_bytes"`
	StoredPageBytes    uint64        `json:"stored_page_bytes"`
	StoredManifestSize uint64        `json:"-"`
	TotalPages         int           `json:"total_pages"`
	ZeroPages          int           `json:"zero_pages"`
	DedupedPages       int           `json:"deduped_pages"`
	UploadedPages      int           `json:"uploaded_pages"`
	CompressedPages    int           `json:"compressed_pages"`
	S3PutOps           int           `json:"-"`
	LostACKRecovered   int           `json:"-"`
	Encrypted          bool          `json:"encrypted"`
	CompressionRatio   float64       `json:"compression_ratio"`
	CheckpointDuration time.Duration `json:"-"`
}

// CheckpointManifest is the signed, deterministic root descriptor stored on S3
// for a suspended sandbox. The external VFS filesystem lives outside the
// checkpoint and is referenced strictly via VFSRelease.
type CheckpointManifest struct {
	FormatVersion   int                `json:"format_version"`
	CheckpointID    string             `json:"checkpoint_id"`
	SandboxID       string             `json:"sandbox_id"`
	SourceNodeID    string             `json:"source_node_id"`
	TenantID        string             `json:"tenant_id"`
	VolumeID        string             `json:"volume_id"`
	PrivilegeMode   PrivilegeMode      `json:"privilege_mode"`
	UID             uint32             `json:"uid"`
	GID             uint32             `json:"gid"`
	VFSRelease      VolumeReleaseToken `json:"vfs_release"`
	MountSpecDigest string             `json:"mount_spec_digest"`
	Descriptors     []OpenDescriptor   `json:"descriptors,omitempty"`
	SavedHandles    []SavedHandle      `json:"saved_handles,omitempty"`
	NextHandle      uint64             `json:"next_handle,omitempty"`
	Registers       []byte             `json:"registers,omitempty"`
	Env             map[string]string  `json:"env,omitempty"`
	PageSize        int                `json:"page_size"`
	MemoryBytes     uint64             `json:"memory_bytes"`
	MemoryDigest    string             `json:"memory_digest"`
	Pages           []PageRef          `json:"pages"`
	Stats           CheckpointStats    `json:"stats"`
	ManifestDigest  string             `json:"manifest_digest"`
	Signature       string             `json:"signature"`
}

// PageObjectKey returns the backend.ValidKey-compliant S3 object key for a
// plaintext content-addressed memory page scoped strictly to (tenantID, volumeID).
func PageObjectKey(tenantID, volumeID, plainDigest string) string {
	return PageObjectKeyForPolicy(tenantID, volumeID, plainDigest, false)
}

// PageObjectKeyForPolicy returns the backend.ValidKey-compliant S3 object key
// scoped to (tenantID, volumeID) and separated by encryption policy so an
// encrypted checkpoint never collides with or reuses an earlier plaintext page.
func PageObjectKeyForPolicy(tenantID, volumeID, plainDigest string, encrypted bool) string {
	shard := "00"
	if len(plainDigest) >= 2 {
		shard = plainDigest[:2]
	}
	if encrypted {
		return "ckpt/" + tenantID + "/" + volumeID + "/p/e/" + shard + "/" + plainDigest
	}
	return "ckpt/" + tenantID + "/" + volumeID + "/p/" + shard + "/" + plainDigest
}

// ManifestObjectKey returns the backend.ValidKey-compliant S3 object key for a
// checkpoint manifest scoped strictly to (tenantID, volumeID).
func ManifestObjectKey(tenantID, volumeID, checkpointID string) string {
	return "ckpt/" + tenantID + "/" + volumeID + "/m/" + checkpointID
}

func deriveMACKey(secret []byte) [32]byte {
	var key [32]byte
	blake3.DeriveKey("hanzoai/vfs/visorfuse checkpoint mac v1", secret, key[:])
	return key
}

func signManifestDigest(hmacKey []byte, tenantID, volumeID, checkpointID, manifestDigest string) string {
	key := deriveMACKey(hmacKey)
	h, _ := blake3.NewKeyed(key[:])
	_, _ = h.Write([]byte(tenantID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(volumeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(checkpointID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(manifestDigest))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func computeManifestDigest(m *CheckpointManifest) (string, error) {
	cp := *m
	cp.ManifestDigest = ""
	cp.Signature = ""
	b, err := json.Marshal(cp)
	if err != nil {
		return "", err
	}
	sum := blake3.Sum256(b)
	return fmt.Sprintf("%x", sum), nil
}

func pageAAD(tenantID, volumeID, plainDigest string) []byte {
	h := blake3.New()
	_, _ = h.Write([]byte("visorfuse-page-v1:"))
	_, _ = h.Write([]byte(tenantID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(volumeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(plainDigest))
	return h.Sum(nil) // 32 bytes
}

func manifestAAD(tenantID, volumeID, checkpointID string) []byte {
	h := blake3.New()
	_, _ = h.Write([]byte("visorfuse-manifest-v1:"))
	_, _ = h.Write([]byte(tenantID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(volumeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(checkpointID))
	return h.Sum(nil) // 32 bytes
}

func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func compressFlate(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(raw); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressFlate(comp []byte, maxOut int) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(comp))
	defer r.Close()
	out, err := io.ReadAll(io.LimitReader(r, int64(maxOut)+1))
	if err != nil {
		return nil, err
	}
	if len(out) > maxOut {
		return nil, fmt.Errorf("%w: decompressed page exceeds %d bytes", ErrCheckpointTampered, maxOut)
	}
	return out, nil
}

// Envelope binary layout:
//
//	[0:4]          Magic (ckptMagicPage or ckptMagicManifest)
//	[4]            Flags (ckptFlagCompressed | ckptFlagEncrypted)
//	[5]            len(KeyID)
//	[6:6+klen]     KeyID
//	[6+klen:]      Body (if encrypted: Cipher.Seal(aad(32) || innerBody))
func packEnvelope(magic uint32, flags byte, keyID string, body []byte) ([]byte, error) {
	if len(keyID) > 255 {
		return nil, fmt.Errorf("%w: keyID too long (%d)", ErrInvalidMountConfig, len(keyID))
	}
	out := make([]byte, 6+len(keyID)+len(body))
	binary.LittleEndian.PutUint32(out[0:4], magic)
	out[4] = flags
	out[5] = byte(len(keyID))
	copy(out[6:6+len(keyID)], keyID)
	copy(out[6+len(keyID):], body)
	return out, nil
}

func unpackEnvelope(raw []byte, wantMagic uint32) (flags byte, keyID string, body []byte, err error) {
	if len(raw) < 6 {
		return 0, "", nil, fmt.Errorf("%w: truncated envelope", ErrCheckpointTampered)
	}
	magic := binary.LittleEndian.Uint32(raw[0:4])
	if magic != wantMagic {
		return 0, "", nil, fmt.Errorf("%w: invalid envelope magic %#x", ErrCheckpointTampered, magic)
	}
	flags = raw[4]
	klen := int(raw[5])
	if len(raw) < 6+klen {
		return 0, "", nil, fmt.Errorf("%w: truncated keyID in envelope", ErrCheckpointTampered)
	}
	keyID = string(raw[6 : 6+klen])
	body = raw[6+klen:]
	return flags, keyID, body, nil
}

func sealWithAAD(c vfs.Cipher, aad, plain []byte) (sealed []byte, keyID string, err error) {
	buf := make([]byte, len(aad)+len(plain))
	copy(buf, aad)
	copy(buf[len(aad):], plain)
	return c.Seal(buf)
}

func openWithAAD(c vfs.Cipher, aad, sealed []byte, keyID string) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: encrypted object requires a Cipher", ErrCheckpointTampered)
	}
	plain, err := c.Open(sealed, keyID)
	if err != nil {
		return nil, fmt.Errorf("%w: cipher open: %v", ErrCheckpointTampered, err)
	}
	if len(plain) < len(aad) || subtle.ConstantTimeCompare(plain[:len(aad)], aad) != 1 {
		return nil, fmt.Errorf("%w: AAD binding mismatch", ErrCheckpointTampered)
	}
	return plain[len(aad):], nil
}

// SuspendSandbox executes the complete hanzoai/vfs#31 VFS-outside-checkpoint
// lifecycle atomically:
//  1. FlushAndQuiesce the external VFS volume (flushes dirty pages & journal),
//  2. ReleaseRound (releases VFS publication round with zero synchronous S3
//     requests on clean close; verifies publish latency < PublishSLA),
//  3. CheckpointSandbox (uploads chunked, BLAKE3-addressed, optionally compressed
//     and vfs.Hybrid PQ-encrypted memory pages + signed manifest to S3).
func SuspendSandbox(ctx context.Context, server *ExternalFUSEServer, snap SandboxSnapshot, cfg CheckpointConfig) (*CheckpointManifest, error) {
	if server == nil {
		return nil, ErrUnquiescedVolume
	}
	normCfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	rec, err := server.FlushAndQuiesce(ctx)
	if err != nil {
		return nil, err
	}
	if rec.PublishDuration > normCfg.PublishSLA {
		return nil, fmt.Errorf("visorfuse: S3 publication took %v, exceeding SLA %v", rec.PublishDuration, normCfg.PublishSLA)
	}
	if _, err := server.ReleaseRound(ctx); err != nil {
		return nil, err
	}
	return CheckpointSandbox(ctx, server, snap, normCfg)
}

// CheckpointSandbox captures a sandbox's memory and descriptor state into S3.
// It FAILS CLOSED with ErrUnquiescedVolume unless server has already completed
// FlushAndQuiesce and ReleaseRound (enforcing the VFS-outside-checkpoint
// invariant).
func CheckpointSandbox(ctx context.Context, server *ExternalFUSEServer, snap SandboxSnapshot, cfg CheckpointConfig) (*CheckpointManifest, error) {
	t0 := time.Now()
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	if server == nil || !server.IsQuiesced() {
		return nil, ErrUnquiescedVolume
	}
	token, ok := server.ReleaseToken()
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrUnquiescedVolume, ErrVolumeNotReleased)
	}
	if err := token.Verify(); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier("checkpoint_id", snap.CheckpointID); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier("tenant_id", snap.TenantID); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier("volume_id", snap.VolumeID); err != nil {
		return nil, err
	}
	if err := ValidatePrivilegeMode(snap.PrivilegeMode); err != nil {
		return nil, err
	}
	spec := server.Spec()
	if snap.TenantID != spec.TenantID || token.TenantID != snap.TenantID {
		return nil, fmt.Errorf("%w: snapshot tenant %q != mount tenant %q", ErrTenantMismatch, snap.TenantID, spec.TenantID)
	}
	if snap.VolumeID != spec.VolumeID || token.VolumeID != snap.VolumeID {
		return nil, fmt.Errorf("%w: snapshot volume %q != mount volume %q", ErrVolumeMismatch, snap.VolumeID, spec.VolumeID)
	}

	memSum := blake3.Sum256(snap.Memory)
	memDigest := fmt.Sprintf("%x", memSum)

	pageSize := cfg.PageSize
	totalPages := (len(snap.Memory) + pageSize - 1) / pageSize
	pages := make([]PageRef, 0, totalPages)
	seenInCkpt := make(map[string]PageRef, totalPages)
	var stats CheckpointStats
	stats.RawMemoryBytes = uint64(len(snap.Memory))
	stats.TotalPages = totalPages
	stats.Encrypted = cfg.Cipher != nil

	for idx := 0; idx < totalPages; idx++ {
		start := idx * pageSize
		end := min(start+pageSize, len(snap.Memory))
		rawPage := snap.Memory[start:end]

		if isAllZero(rawPage) {
			stats.ZeroPages++
			pages = append(pages, PageRef{
				Index:  idx,
				Zero:   true,
				RawLen: len(rawPage),
			})
			continue
		}

		pSum := blake3.Sum256(rawPage)
		plainDigest := fmt.Sprintf("%x", pSum)

		if prev, exists := seenInCkpt[plainDigest]; exists {
			stats.DedupedPages++
			dup := prev
			dup.Index = idx
			pages = append(pages, dup)
			continue
		}

		objKey := PageObjectKeyForPolicy(snap.TenantID, snap.VolumeID, plainDigest, cfg.Cipher != nil)
		if err := backend.ValidKey(objKey); err != nil {
			return nil, fmt.Errorf("%w: invalid page key %q: %v", ErrInvalidMountConfig, objKey, err)
		}

		payload := rawPage
		var flags byte
		if cfg.Compress {
			comp, err := compressFlate(rawPage)
			if err != nil {
				return nil, fmt.Errorf("visorfuse: compress page %d: %w", idx, err)
			}
			if len(comp) < len(rawPage) {
				payload = comp
				flags |= ckptFlagCompressed
			}
		}

		var keyID string
		if cfg.Cipher != nil {
			aad := pageAAD(snap.TenantID, snap.VolumeID, plainDigest)
			sealed, kid, err := sealWithAAD(cfg.Cipher, aad, payload)
			if err != nil {
				return nil, fmt.Errorf("visorfuse: seal page %d: %w", idx, err)
			}
			payload = sealed
			keyID = kid
			flags |= ckptFlagEncrypted
		}

		envBytes, err := packEnvelope(ckptMagicPage, flags, keyID, payload)
		if err != nil {
			return nil, err
		}
		storedDigest := fmt.Sprintf("%x", blake3.Sum256(envBytes))

		ref, uploaded, recovered, puts, err := putPageIdempotent(
			ctx, cfg, objKey, envBytes, snap.TenantID, snap.VolumeID, plainDigest, storedDigest,
			len(rawPage), flags, keyID,
		)
		if err != nil {
			return nil, err
		}
		ref.Index = idx
		stats.S3PutOps += puts
		stats.LostACKRecovered += recovered
		if uploaded {
			stats.UploadedPages++
			stats.StoredPageBytes += uint64(ref.StoredLen)
			if ref.Compressed {
				stats.CompressedPages++
			}
		} else {
			stats.DedupedPages++
		}
		seenInCkpt[plainDigest] = ref
		pages = append(pages, ref)
	}

	savedHandles, nextHandle := server.SavedHandles()
	savedByHandle := make(map[uint64]uint64, len(savedHandles))
	for _, sh := range savedHandles {
		savedByHandle[sh.Handle] = sh.Inode
	}
	descs := append([]OpenDescriptor(nil), snap.Descriptors...)
	for i := range descs {
		if descs[i].Handle != 0 {
			if _, exists := savedByHandle[descs[i].Handle]; !exists && descs[i].Inode != 0 {
				savedHandles = append(savedHandles, SavedHandle{Handle: descs[i].Handle, Inode: descs[i].Inode})
				savedByHandle[descs[i].Handle] = descs[i].Inode
			}
			if descs[i].Handle >= nextHandle {
				nextHandle = descs[i].Handle + 1
			}
		}
	}

	manifest := &CheckpointManifest{
		FormatVersion:   CheckpointFormatVersion,
		CheckpointID:    snap.CheckpointID,
		SandboxID:       snap.SandboxID,
		SourceNodeID:    snap.SourceNodeID,
		TenantID:        snap.TenantID,
		VolumeID:        snap.VolumeID,
		PrivilegeMode:   snap.PrivilegeMode,
		UID:             snap.UID,
		GID:             snap.GID,
		VFSRelease:      token,
		MountSpecDigest: spec.CanonicalDigest(),
		Descriptors:     descs,
		SavedHandles:    savedHandles,
		NextHandle:      nextHandle,
		Registers:       append([]byte(nil), snap.Registers...),
		Env:             snap.Env,
		PageSize:        pageSize,
		MemoryBytes:     uint64(len(snap.Memory)),
		MemoryDigest:    memDigest,
		Pages:           pages,
	}

	// Compute deterministic content stats cleanly before signing the manifest.
	// Runtime upload telemetry fields (S3PutOps, LostACKRecovered,
	// StoredManifestSize, CheckpointDuration) are tagged json:"-" so updating
	// them after upload never mutates the signed canonical manifest bytes.
	if stats.StoredPageBytes > 0 {
		stats.CompressionRatio = float64(stats.RawMemoryBytes) / float64(stats.StoredPageBytes)
	} else if stats.RawMemoryBytes > 0 {
		stats.CompressionRatio = float64(stats.RawMemoryBytes)
	} else {
		stats.CompressionRatio = 1.0
	}
	manifest.Stats = stats

	mdigest, err := computeManifestDigest(manifest)
	if err != nil {
		return nil, err
	}
	manifest.ManifestDigest = mdigest
	manifest.Signature = signManifestDigest(cfg.HMACKey, manifest.TenantID, manifest.VolumeID, manifest.CheckpointID, mdigest)

	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	mKey := ManifestObjectKey(snap.TenantID, snap.VolumeID, snap.CheckpointID)
	if err := backend.ValidKey(mKey); err != nil {
		return nil, fmt.Errorf("%w: invalid manifest key %q: %v", ErrInvalidMountConfig, mKey, err)
	}

	mPayload := manifestJSON
	var mFlags byte
	var mKeyID string
	if cfg.Cipher != nil {
		aad := manifestAAD(snap.TenantID, snap.VolumeID, snap.CheckpointID)
		sealed, kid, err := sealWithAAD(cfg.Cipher, aad, manifestJSON)
		if err != nil {
			return nil, fmt.Errorf("visorfuse: seal manifest: %w", err)
		}
		mPayload = sealed
		mKeyID = kid
		mFlags |= ckptFlagEncrypted
	}
	mEnv, err := packEnvelope(ckptMagicManifest, mFlags, mKeyID, mPayload)
	if err != nil {
		return nil, err
	}

	mPuts, mRecovered, err := putManifestIdempotent(ctx, cfg, mKey, mEnv, manifestJSON, snap.TenantID, snap.VolumeID, snap.CheckpointID)
	if err != nil {
		return nil, err
	}
	manifest.Stats.S3PutOps += mPuts
	manifest.Stats.LostACKRecovered += mRecovered
	manifest.Stats.StoredManifestSize = uint64(len(mEnv))
	manifest.Stats.CheckpointDuration = time.Since(t0)
	return manifest, nil
}

// Verify checks the cryptographic digest, keyed BLAKE3 signature, and release
// token of a CheckpointManifest.
func (m *CheckpointManifest) Verify(hmacKey []byte) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrCheckpointTampered)
	}
	if m.FormatVersion != CheckpointFormatVersion {
		return fmt.Errorf("%w: unsupported format version %d", ErrCheckpointTampered, m.FormatVersion)
	}
	wantDigest, err := computeManifestDigest(m)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(m.ManifestDigest), []byte(wantDigest)) != 1 {
		return fmt.Errorf("%w: manifest digest mismatch", ErrCheckpointTampered)
	}
	wantSig := signManifestDigest(hmacKey, m.TenantID, m.VolumeID, m.CheckpointID, m.ManifestDigest)
	if subtle.ConstantTimeCompare([]byte(m.Signature), []byte(wantSig)) != 1 {
		return fmt.Errorf("%w: manifest signature mismatch", ErrCheckpointTampered)
	}
	return m.VFSRelease.Verify()
}

// putPageIdempotent uploads a content-addressed page via PutIfAbsent, safely
// reconciling both cross-checkpoint dedup and transient lost-ACK errors by
// cryptographically verifying any existing object on ErrConflict.
func putPageIdempotent(
	ctx context.Context,
	cfg CheckpointConfig,
	objKey string,
	envBytes []byte,
	tenantID, volumeID, plainDigest, storedDigest string,
	rawLen int,
	flags byte,
	keyID string,
) (ref PageRef, uploaded bool, recovered int, puts int, err error) {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		puts++
		_, err = cfg.Store.PutIfAbsent(ctx, objKey, envBytes)
		if err == nil {
			return PageRef{
				PlainDigest:  plainDigest,
				StoredDigest: storedDigest,
				ObjectKey:    objKey,
				RawLen:       rawLen,
				StoredLen:    len(envBytes),
				Compressed:   flags&ckptFlagCompressed != 0,
				Encrypted:    flags&ckptFlagEncrypted != 0,
				KeyID:        keyID,
			}, true, recovered, puts, nil
		}
		if errors.Is(err, backend.ErrConflict) {
			// Verify the existing object on S3 matches plainDigest bit-for-bit
			// AND obeys the exact encryption policy requested by this checkpoint.
			existing, _, gerr := cfg.Store.Get(ctx, objKey, maxPageBytes)
			if gerr != nil {
				return PageRef{}, false, recovered, puts, fmt.Errorf("visorfuse: verify existing page %s: %w", objKey, gerr)
			}
			eflags, ekid, plain, verr := decodeAndVerifyPage(existing, cfg.Cipher, tenantID, volumeID, plainDigest, rawLen)
			if verr != nil {
				return PageRef{}, false, recovered, puts, verr
			}
			wantEncrypted := flags&ckptFlagEncrypted != 0
			gotEncrypted := eflags&ckptFlagEncrypted != 0
			if wantEncrypted != gotEncrypted {
				return PageRef{}, false, recovered, puts, fmt.Errorf("%w: existing page %s encryption policy mismatch (got encrypted=%v, want %v)", ErrCheckpointTampered, objKey, gotEncrypted, wantEncrypted)
			}
			if len(plain) != rawLen {
				return PageRef{}, false, recovered, puts, fmt.Errorf("%w: existing page length mismatch", ErrCheckpointTampered)
			}
			exDigest := fmt.Sprintf("%x", blake3.Sum256(existing))
			if attempt > 0 {
				recovered++
			}
			return PageRef{
				PlainDigest:  plainDigest,
				StoredDigest: exDigest,
				ObjectKey:    objKey,
				RawLen:       rawLen,
				StoredLen:    len(existing),
				Compressed:   eflags&ckptFlagCompressed != 0,
				Encrypted:    gotEncrypted,
				KeyID:        ekid,
			}, attempt > 0, recovered, puts, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return PageRef{}, false, recovered, puts, ctx.Err()
		}
	}
	return PageRef{}, false, recovered, puts, fmt.Errorf("visorfuse: upload page %s failed after %d attempts: %w", objKey, cfg.MaxRetries, lastErr)
}

func putManifestIdempotent(
	ctx context.Context,
	cfg CheckpointConfig,
	mKey string,
	mEnv []byte,
	wantJSON []byte,
	tenantID, volumeID, checkpointID string,
) (puts int, recovered int, err error) {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		puts++
		_, err = cfg.Store.PutIfAbsent(ctx, mKey, mEnv)
		if err == nil {
			return puts, recovered, nil
		}
		if errors.Is(err, backend.ErrConflict) {
			existing, _, gerr := cfg.Store.Get(ctx, mKey, maxManifestBytes)
			if gerr != nil {
				return puts, recovered, fmt.Errorf("visorfuse: read conflicting manifest %s: %w", mKey, gerr)
			}
			gotJSON, uerr := unwrapManifest(existing, cfg.Cipher, tenantID, volumeID, checkpointID)
			if uerr == nil && bytes.Equal(gotJSON, wantJSON) {
				recovered++
				return puts, recovered, nil
			}
			return puts, recovered, fmt.Errorf("%w: %s", ErrCheckpointExists, mKey)
		}
		lastErr = err
		if ctx.Err() != nil {
			return puts, recovered, ctx.Err()
		}
	}
	return puts, recovered, fmt.Errorf("visorfuse: commit manifest %s failed after %d attempts: %w", mKey, cfg.MaxRetries, lastErr)
}

func decodeAndVerifyPage(raw []byte, cipher vfs.Cipher, tenantID, volumeID, wantPlainDigest string, wantRawLen int) (flags byte, keyID string, plain []byte, err error) {
	flags, keyID, body, err := unpackEnvelope(raw, ckptMagicPage)
	if err != nil {
		return 0, "", nil, err
	}
	if flags&ckptFlagEncrypted != 0 {
		aad := pageAAD(tenantID, volumeID, wantPlainDigest)
		body, err = openWithAAD(cipher, aad, body, keyID)
		if err != nil {
			return 0, "", nil, err
		}
	}
	if flags&ckptFlagCompressed != 0 {
		body, err = decompressFlate(body, wantRawLen)
		if err != nil {
			return 0, "", nil, err
		}
	}
	sum := fmt.Sprintf("%x", blake3.Sum256(body))
	if subtle.ConstantTimeCompare([]byte(sum), []byte(wantPlainDigest)) != 1 {
		return 0, "", nil, fmt.Errorf("%w: page BLAKE3 digest mismatch", ErrCheckpointTampered)
	}
	return flags, keyID, body, nil
}

func unwrapManifest(raw []byte, cipher vfs.Cipher, tenantID, volumeID, checkpointID string) ([]byte, error) {
	flags, keyID, body, err := unpackEnvelope(raw, ckptMagicManifest)
	if err != nil {
		return nil, err
	}
	if flags&ckptFlagEncrypted != 0 {
		aad := manifestAAD(tenantID, volumeID, checkpointID)
		return openWithAAD(cipher, aad, body, keyID)
	}
	return body, nil
}

// RestoreRequest configures cross-node restoration of a suspended sandbox from S3.
type RestoreRequest struct {
	TargetNodeID             string
	TargetTenantID           string
	TargetVolumeID           string
	CheckpointID             string
	CheckpointStore          backend.Backend
	CheckpointCipher         vfs.Cipher
	HMACKey                  []byte
	VFSBackend               backend.Backend
	VFSCipher                vfs.Cipher
	TargetJournalDir         string
	TargetJournalDisk        func(dir string) (journal.Disk, error)
	TargetMountSpec          MountSpec
	TargetHostConn           io.ReadWriteCloser // optional: if set, binds a live ExternalFUSEServer on target node
	TargetCountingBackend    *CountingBackend   // optional: attaches to target ExternalFUSEServer
	TargetPrivilegeMode      PrivilegeMode
	AllowPrivilegeEscalation bool
	OverrideNextRound        uint64
}

// RestoreMetrics records exact timing and S3 fetch counters during cross-node restore.
type RestoreMetrics struct {
	RestoreWallTime   time.Duration `json:"restore_wall_time_ns"`
	ManifestFetchTime time.Duration `json:"manifest_fetch_time_ns"`
	MemoryRestoreTime time.Duration `json:"memory_restore_time_ns"`
	VFSAttachTime     time.Duration `json:"vfs_attach_time_ns"`
	S3GetOps          int           `json:"s3_get_ops"`
	UniquePagesRead   int           `json:"unique_pages_read"`
	ZeroPagesSkipped  int           `json:"zero_pages_skipped"`
	BytesFetched      uint64        `json:"bytes_fetched"`
	MemoryBytes       uint64        `json:"memory_bytes"`
	VerifiedBitExact  bool          `json:"verified_bit_exact"`
	AttachedRound     uint64        `json:"attached_round"`
	AttachedHeadSeq   uint64        `json:"attached_head_seq"`
}

// RestoredSandbox is the verified live sandbox state restored on the target node.
type RestoredSandbox struct {
	Manifest            *CheckpointManifest
	Memory              []byte
	Volume              *vfs.Volume
	Server              *ExternalFUSEServer
	RestoredDescriptors []OpenDescriptor
	PrivilegeMode       PrivilegeMode
	Metrics             RestoreMetrics
}

// RestoreToNode performs fail-closed cross-node restoration:
//  1. Validates TargetMountSpec (zero guest priv, zero gofer hop, allowlisted host FD),
//  2. Fetches, unseals, and verifies manifest BLAKE3 digest, keyed MAC, tenant/volume
//     isolation, signed mount policy digest, and VFSRelease token,
//  3. Enforces unprivileged/rootful restore policy (rejects unauthorized escalation),
//  4. Fetches and cryptographically verifies every memory page + full memory BLAKE3 digest,
//  5. Re-attaches the external VFS volume at ReleasedRound+1 on the target node and
//     requires an exact match with VFSRelease.PublishedHeadSeq,
//  6. Verifies all saved open descriptors and reconstructs the live FUSE handle
//     table and allocator before returning the restored server.
func RestoreToNode(ctx context.Context, req RestoreRequest) (*RestoredSandbox, error) {
	tTotal := time.Now()
	if err := ValidateMountSpec(req.TargetMountSpec); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier("tenant_id", req.TargetTenantID); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier("volume_id", req.TargetVolumeID); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier("checkpoint_id", req.CheckpointID); err != nil {
		return nil, err
	}
	if err := ValidatePrivilegeMode(req.TargetPrivilegeMode); err != nil {
		return nil, err
	}
	if req.TargetMountSpec.TenantID != req.TargetTenantID {
		return nil, fmt.Errorf("%w: target mount tenant %q != request tenant %q", ErrTenantMismatch, req.TargetMountSpec.TenantID, req.TargetTenantID)
	}
	if req.TargetMountSpec.VolumeID != req.TargetVolumeID {
		return nil, fmt.Errorf("%w: target mount volume %q != request volume %q", ErrVolumeMismatch, req.TargetMountSpec.VolumeID, req.TargetVolumeID)
	}
	if req.CheckpointStore == nil || req.VFSBackend == nil || req.VFSCipher == nil || req.TargetJournalDir == "" {
		return nil, fmt.Errorf("%w: checkpoint store, VFS backend/cipher, and target journal dir are required", ErrInvalidMountConfig)
	}
	if len(req.HMACKey) < 16 {
		return nil, fmt.Errorf("%w: HMACKey is required for manifest verification", ErrInvalidMountConfig)
	}

	var metrics RestoreMetrics
	tManifest := time.Now()
	mKey := ManifestObjectKey(req.TargetTenantID, req.TargetVolumeID, req.CheckpointID)
	rawManifest, _, err := req.CheckpointStore.Get(ctx, mKey, maxManifestBytes)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrCheckpointNotFound, mKey)
		}
		return nil, fmt.Errorf("visorfuse: fetch manifest %s: %w", mKey, err)
	}
	metrics.S3GetOps++
	metrics.BytesFetched += uint64(len(rawManifest))

	manifestJSON, err := unwrapManifest(rawManifest, req.CheckpointCipher, req.TargetTenantID, req.TargetVolumeID, req.CheckpointID)
	if err != nil {
		return nil, err
	}
	var manifest CheckpointManifest
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		return nil, fmt.Errorf("%w: decode manifest: %v", ErrCheckpointTampered, err)
	}
	if manifest.FormatVersion != CheckpointFormatVersion {
		return nil, fmt.Errorf("%w: unsupported format version %d", ErrCheckpointTampered, manifest.FormatVersion)
	}
	if manifest.TenantID != req.TargetTenantID {
		return nil, fmt.Errorf("%w: manifest tenant %q != target %q", ErrTenantMismatch, manifest.TenantID, req.TargetTenantID)
	}
	if manifest.VolumeID != req.TargetVolumeID {
		return nil, fmt.Errorf("%w: manifest volume %q != target %q", ErrVolumeMismatch, manifest.VolumeID, req.TargetVolumeID)
	}
	if manifest.CheckpointID != req.CheckpointID {
		return nil, fmt.Errorf("%w: manifest checkpoint_id mismatch", ErrCheckpointTampered)
	}

	// Verify manifest BLAKE3 digest, constant-time keyed signature, and VFS release token.
	if err := manifest.Verify(req.HMACKey); err != nil {
		return nil, err
	}
	if manifest.VFSRelease.TenantID != req.TargetTenantID {
		return nil, ErrTenantMismatch
	}
	if manifest.VFSRelease.VolumeID != req.TargetVolumeID {
		return nil, ErrVolumeMismatch
	}

	// Enforce the signed OCI mount policy (ReadOnly, Destination, MaxWrite,
	// SocketKind, NoGuestPriv, NoGoferHop, tenant/volume) before attaching or
	// exposing the volume on the target node.
	targetMountDigest := req.TargetMountSpec.CanonicalDigest()
	if subtle.ConstantTimeCompare([]byte(targetMountDigest), []byte(manifest.MountSpecDigest)) != 1 {
		return nil, fmt.Errorf("%w: target mount policy digest %s != signed checkpoint mount_spec_digest %s", ErrInvalidMountConfig, targetMountDigest, manifest.MountSpecDigest)
	}

	// Verify unprivileged / rootful restore policy.
	if manifest.PrivilegeMode == PrivilegeUnprivileged && req.TargetPrivilegeMode == PrivilegeRootful && !req.AllowPrivilegeEscalation {
		return nil, fmt.Errorf("%w: cannot escalate unprivileged checkpoint to rootful without AllowPrivilegeEscalation", ErrRestorePrivilegeDenied)
	}
	metrics.ManifestFetchTime = time.Since(tManifest)

	// Reconstruct and verify sandbox memory image page-by-page.
	tMem := time.Now()
	mem := make([]byte, manifest.MemoryBytes)
	pageCache := make(map[string][]byte, len(manifest.Pages))
	expectedPrefix := "ckpt/" + req.TargetTenantID + "/" + req.TargetVolumeID + "/p/"

	for _, pref := range manifest.Pages {
		start := uint64(pref.Index) * uint64(manifest.PageSize)
		end := start + uint64(pref.RawLen)
		if end > manifest.MemoryBytes {
			return nil, fmt.Errorf("%w: page %d bounds [%d:%d] exceed memory size %d", ErrCheckpointTampered, pref.Index, start, end, manifest.MemoryBytes)
		}
		if pref.Zero {
			metrics.ZeroPagesSkipped++
			continue
		}
		if manifest.Stats.Encrypted && !pref.Encrypted {
			return nil, fmt.Errorf("%w: encrypted checkpoint references plaintext page %d", ErrCheckpointTampered, pref.Index)
		}
		// Enforce strict tenant+volume key prefix isolation on every page ref.
		if !strings.HasPrefix(pref.ObjectKey, expectedPrefix) {
			return nil, fmt.Errorf("%w: page key %q escapes tenant/volume prefix %q", ErrTenantMismatch, pref.ObjectKey, expectedPrefix)
		}
		plain, cached := pageCache[pref.ObjectKey]
		if !cached {
			rawPage, _, err := req.CheckpointStore.Get(ctx, pref.ObjectKey, maxPageBytes)
			if err != nil {
				if errors.Is(err, backend.ErrNotFound) {
					return nil, fmt.Errorf("%w: missing page %s", ErrCheckpointNotFound, pref.ObjectKey)
				}
				return nil, fmt.Errorf("visorfuse: fetch page %s: %w", pref.ObjectKey, err)
			}
			metrics.S3GetOps++
			metrics.UniquePagesRead++
			metrics.BytesFetched += uint64(len(rawPage))

			gotStoredDigest := fmt.Sprintf("%x", blake3.Sum256(rawPage))
			if subtle.ConstantTimeCompare([]byte(gotStoredDigest), []byte(pref.StoredDigest)) != 1 {
				return nil, fmt.Errorf("%w: stored digest mismatch for page %d", ErrCheckpointTampered, pref.Index)
			}
			eflags, _, dec, err := decodeAndVerifyPage(rawPage, req.CheckpointCipher, req.TargetTenantID, req.TargetVolumeID, pref.PlainDigest, pref.RawLen)
			if err != nil {
				return nil, err
			}
			gotEncrypted := eflags&ckptFlagEncrypted != 0
			if gotEncrypted != pref.Encrypted {
				return nil, fmt.Errorf("%w: page %d envelope encryption flag (%v) != manifest (%v)", ErrCheckpointTampered, pref.Index, gotEncrypted, pref.Encrypted)
			}
			if len(dec) != pref.RawLen {
				return nil, fmt.Errorf("%w: page %d length %d != %d", ErrCheckpointTampered, pref.Index, len(dec), pref.RawLen)
			}
			plain = dec
			pageCache[pref.ObjectKey] = plain
		}
		copy(mem[start:end], plain)
	}

	gotMemDigest := fmt.Sprintf("%x", blake3.Sum256(mem))
	if subtle.ConstantTimeCompare([]byte(gotMemDigest), []byte(manifest.MemoryDigest)) != 1 {
		return nil, fmt.Errorf("%w: full memory BLAKE3 digest mismatch", ErrCheckpointTampered)
	}
	metrics.MemoryRestoreTime = time.Since(tMem)
	metrics.MemoryBytes = manifest.MemoryBytes
	metrics.VerifiedBitExact = true

	// Re-attach the external VFS volume on the destination node at NextRound.
	tVFS := time.Now()
	nextRound := manifest.VFSRelease.NextRound
	if req.OverrideNextRound > nextRound {
		nextRound = req.OverrideNextRound
	}
	vol, err := vfs.Attach(ctx, vfs.Config{
		Backend: req.VFSBackend,
		Cipher:  req.VFSCipher,
		Round:   nextRound,
		Journal: req.TargetJournalDir,
		Disk:    req.TargetJournalDisk,
	})
	if err != nil {
		return nil, fmt.Errorf("visorfuse: re-attach external VFS at round %d: %w", nextRound, err)
	}
	vStats := vol.Stats()
	// Require exact filesystem Head generation match so restoring an earlier
	// checkpoint never pairs stale guest memory with newer diverged writes.
	if vStats.Seq != manifest.VFSRelease.PublishedHeadSeq {
		_ = vol.Close()
		return nil, fmt.Errorf("%w: target VFS head seq %d != checkpoint published seq %d", ErrCheckpointTampered, vStats.Seq, manifest.VFSRelease.PublishedHeadSeq)
	}

	// Verify all saved open descriptors exist on the restored external VFS volume.
	restoredDescs := make([]OpenDescriptor, 0, len(manifest.Descriptors))
	for _, d := range manifest.Descriptors {
		cleanPath := path.Clean("/" + strings.TrimPrefix(d.Path, "/"))
		in, err := vol.Resolve(ctx, cleanPath)
		if err != nil {
			_ = vol.Close()
			return nil, fmt.Errorf("visorfuse: restore descriptor %q (inode %d): %w", d.Path, d.Inode, err)
		}
		if d.Inode != 0 && uint64(in.ID) != d.Inode {
			_ = vol.Close()
			return nil, fmt.Errorf("%w: descriptor %q inode %d != expected %d", ErrCheckpointTampered, d.Path, in.ID, d.Inode)
		}
		rd := d
		rd.Inode = uint64(in.ID)
		rd.Size = in.Size
		restoredDescs = append(restoredDescs, rd)
	}

	// Collect and validate all saved FUSE wire handle bindings.
	handlesToRestore := append([]SavedHandle(nil), manifest.SavedHandles...)
	seenHandle := make(map[uint64]struct{}, len(handlesToRestore))
	for _, sh := range handlesToRestore {
		seenHandle[sh.Handle] = struct{}{}
	}
	for _, rd := range restoredDescs {
		if rd.Handle != 0 {
			if _, ok := seenHandle[rd.Handle]; !ok {
				handlesToRestore = append(handlesToRestore, SavedHandle{Handle: rd.Handle, Inode: rd.Inode})
				seenHandle[rd.Handle] = struct{}{}
			}
		}
	}
	for _, sh := range handlesToRestore {
		if _, err := vol.Stat(ctx, vfs.InodeID(sh.Inode)); err != nil {
			_ = vol.Close()
			return nil, fmt.Errorf("visorfuse: saved FUSE handle %d inode %d missing: %w", sh.Handle, sh.Inode, err)
		}
	}

	var srv *ExternalFUSEServer
	if req.TargetHostConn != nil {
		var opts []ServerOption
		if req.TargetCountingBackend != nil {
			opts = append(opts, WithCountingBackend(req.TargetCountingBackend))
		}
		srv, err = NewExternalFUSEServer(req.TargetMountSpec, vol, req.TargetHostConn, opts...)
		if err != nil {
			_ = vol.Close()
			return nil, err
		}
		if len(handlesToRestore) > 0 || manifest.NextHandle > 1 {
			if err := srv.RestoreHandles(ctx, handlesToRestore, manifest.NextHandle); err != nil {
				_ = srv.Close()
				_ = vol.Close()
				return nil, err
			}
		}
	}
	metrics.VFSAttachTime = time.Since(tVFS)
	metrics.AttachedRound = vStats.Round
	metrics.AttachedHeadSeq = vStats.Seq
	metrics.RestoreWallTime = time.Since(tTotal)

	return &RestoredSandbox{
		Manifest:            &manifest,
		Memory:              mem,
		Volume:              vol,
		Server:              srv,
		RestoredDescriptors: restoredDescs,
		PrivilegeMode:       req.TargetPrivilegeMode,
		Metrics:             metrics,
	}, nil
}
