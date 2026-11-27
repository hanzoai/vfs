// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rubikfs

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrInvalidConfig is returned when image build or read configuration is invalid.
	ErrInvalidConfig = errors.New("rubikfs: invalid configuration")

	// ErrTenantIsolation is returned when an operation attempts to mix chunks,
	// keys, or packs across different (tenant_id, volume_id) scopes.
	ErrTenantIsolation = errors.New("rubikfs: tenant/volume isolation violation")

	// ErrCorruptPack is returned when a sealed pack or manifest fails AEAD
	// authentication, BLAKE3 integrity verification, or structural bounds checks.
	ErrCorruptPack = errors.New("rubikfs: corrupt or tampered pack")

	// ErrNotFound is returned when a requested file, chunk, or manifest does not exist.
	ErrNotFound = errors.New("rubikfs: object not found")
)

const (
	// DefaultChunkSize is the default immutable base-image chunk size (16 KiB).
	DefaultChunkSize = 16 << 10

	// DefaultTargetPackBytes is the default uncompressed target pack size (1 MiB).
	DefaultTargetPackBytes = 1 << 20

	// MaxPackPlaintextBytes bounds any single decompressed pack to prevent
	// memory exhaustion on untrusted/corrupt pack headers (32 MiB).
	MaxPackPlaintextBytes = 32 << 20

	// MaxSealedPackBytes bounds any sealed pack object including worst-case
	// chunk index table (up to 32,768 entries at 1 KiB min chunk size),
	// pack header, and AEAD framing overhead (36 MiB).
	MaxSealedPackBytes = MaxPackPlaintextBytes + 4<<20

	// MaxManifestBytes bounds sealed image manifests consistently on both
	// BuildImage write and OpenImage read (32 MiB).
	MaxManifestBytes = MaxPackPlaintextBytes
)

// Scope uniquely identifies an isolated tenant and volume boundary.
// All similarity sketching, clustering, packing, backend key derivation, and
// AEAD sealing are strictly confined to a single Scope.
type Scope struct {
	TenantID string `json:"tenant_id"`
	VolumeID string `json:"volume_id"`
}

// Validate reports whether Scope has non-empty, NUL-free tenant and volume identifiers.
func (s Scope) Validate() error {
	if s.TenantID == "" || s.VolumeID == "" {
		return fmt.Errorf("%w: tenant_id and volume_id must be non-empty", ErrInvalidConfig)
	}
	if strings.IndexByte(s.TenantID, 0) >= 0 || strings.IndexByte(s.VolumeID, 0) >= 0 {
		return fmt.Errorf("%w: tenant_id and volume_id must not contain NUL bytes", ErrInvalidConfig)
	}
	return nil
}

// Policy selects the chunk-ordering and packing strategy.
type Policy string

const (
	// PolicyPathOrder packs chunks sequentially in lexicographical path + offset order.
	PolicyPathOrder Policy = "path_order_baseline"

	// PolicyRandomOrder packs chunks in a deterministic seeded random permutation.
	PolicyRandomOrder Policy = "random_order_control"

	// PolicyPureSimilarity clusters all chunks in the volume by content similarity
	// to maximize compression ratio without cold-start locality pinning.
	PolicyPureSimilarity Policy = "rubikfs_pure_similarity"

	// PolicyLocalityHybrid pins boot-hot chunks into dedicated boot packs (clustered
	// internally for similarity) and runs full similarity clustering across all
	// remaining cold image chunks.
	PolicyLocalityHybrid Policy = "rubikfs_locality_hybrid"
)

// CodecKind selects the pack-level compression codec.
type CodecKind uint8

const (
	// CodecDefault is the omitted zero value, normalized by Config.Validate to CodecLongLZFlate.
	CodecDefault CodecKind = 0

	// CodecFlate compresses each pack with standard library DEFLATE (32 KiB window).
	CodecFlate CodecKind = 1

	// CodecLongLZFlate runs a pack-wide multi-MiB window LZ77 match pass followed
	// by DEFLATE entropy coding, with automatic raw fallback on incompressible packs.
	CodecLongLZFlate CodecKind = 2

	// CodecNone stores pack chunk payloads explicitly uncompressed.
	CodecNone CodecKind = 3
)

// String returns the canonical name of CodecKind.
func (c CodecKind) String() string {
	switch c {
	case CodecDefault:
		return "default"
	case CodecNone:
		return "none"
	case CodecFlate:
		return "flate"
	case CodecLongLZFlate:
		return "longlz_flate"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(c))
	}
}

// PackRole classifies whether a pack holds boot-hot working-set chunks or general cold chunks.
type PackRole uint8

const (
	PackRoleGeneral PackRole = 0
	PackRoleBoot    PackRole = 1
	PackRoleCold    PackRole = 2
)

// String returns the canonical label for PackRole.
func (r PackRole) String() string {
	switch r {
	case PackRoleBoot:
		return "boot"
	case PackRoleCold:
		return "cold"
	default:
		return "general"
	}
}

// FileInput describes one immutable file belonging to a base image.
type FileInput struct {
	// Scope optionally overrides/asserts the chunk's owning tenant and volume.
	// If non-zero, it MUST match Config.Scope or Build fails with ErrTenantIsolation.
	Scope Scope

	// Path is the canonical image path (e.g., "/usr/lib/go/src/io/io.go").
	Path string

	// Data is the immutable plaintext content of the file.
	Data []byte

	// BootHot marks this file as part of the sandbox cold-start boot working set.
	// Used only by PolicyLocalityHybrid (ignored by other policies for fair comparison).
	BootHot bool
}

// Config controls image chunking, clustering, compression, and sealing.
type Config struct {
	// Scope binds the built image strictly to one (tenant_id, volume_id).
	Scope Scope

	// ImageID uniquely names this immutable base image within Scope.
	ImageID string

	// Policy selects the packing regime. Defaults to PolicyLocalityHybrid.
	Policy Policy

	// Codec selects the pack compression codec. Defaults to CodecLongLZFlate when omitted.
	Codec CodecKind

	// ChunkSize is the fixed base-image chunk size in bytes (e.g., 16 KiB or 64 KiB).
	// Defaults to DefaultChunkSize.
	ChunkSize int

	// TargetPackBytes is the target uncompressed payload size per pack.
	// Defaults to DefaultTargetPackBytes.
	TargetPackBytes int

	// BootPackBytes caps the target uncompressed size for PackRoleBoot packs under
	// PolicyLocalityHybrid. If 0, defaults to TargetPackBytes.
	BootPackBytes int

	// Seed makes PolicyRandomOrder and any randomized tie-breaking deterministic.
	Seed int64

	// IntraVolumeDedup enables exact-chunk deduplication strictly within this
	// single (tenant_id, volume_id) image. Defaults to false so benchmarks isolate
	// similarity-clustering compression gain. Cross-volume dedup is NEVER performed.
	IntraVolumeDedup bool
}

// Validate normalizes defaults and checks Config invariants.
func (c *Config) Validate() error {
	if err := c.Scope.Validate(); err != nil {
		return err
	}
	if c.ImageID == "" || strings.IndexByte(c.ImageID, 0) >= 0 {
		return fmt.Errorf("%w: image_id must be non-empty and NUL-free", ErrInvalidConfig)
	}
	if c.Policy == "" {
		c.Policy = PolicyLocalityHybrid
	}
	switch c.Policy {
	case PolicyPathOrder, PolicyRandomOrder, PolicyPureSimilarity, PolicyLocalityHybrid:
	default:
		return fmt.Errorf("%w: unknown policy %q", ErrInvalidConfig, c.Policy)
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = DefaultChunkSize
	}
	if c.ChunkSize < 1024 || c.ChunkSize > 4<<20 {
		return fmt.Errorf("%w: chunk_size %d out of [1 KiB, 4 MiB]", ErrInvalidConfig, c.ChunkSize)
	}
	if c.TargetPackBytes == 0 {
		c.TargetPackBytes = DefaultTargetPackBytes
	}
	if c.TargetPackBytes < c.ChunkSize || c.TargetPackBytes > MaxPackPlaintextBytes {
		return fmt.Errorf("%w: target_pack_bytes %d out of [%d, %d]", ErrInvalidConfig, c.TargetPackBytes, c.ChunkSize, MaxPackPlaintextBytes)
	}
	if c.BootPackBytes == 0 {
		c.BootPackBytes = c.TargetPackBytes
	}
	if c.BootPackBytes < c.ChunkSize || c.BootPackBytes > MaxPackPlaintextBytes {
		return fmt.Errorf("%w: boot_pack_bytes %d out of [%d, %d]", ErrInvalidConfig, c.BootPackBytes, c.ChunkSize, MaxPackPlaintextBytes)
	}
	if c.Codec == CodecDefault {
		c.Codec = CodecLongLZFlate
	}
	if c.Codec != CodecNone && c.Codec != CodecFlate && c.Codec != CodecLongLZFlate {
		return fmt.Errorf("%w: unknown codec %d", ErrInvalidConfig, c.Codec)
	}
	return nil
}

// ChunkID is the 32-byte BLAKE3 digest of a chunk's plaintext bytes.
type ChunkID [32]byte

// PackID is the 16-byte unique identifier of a sealed pack within a volume.
type PackID [16]byte

// ChunkInput is an internal chunk record before packing.
type ChunkInput struct {
	Index   int
	Scope   Scope
	Path    string
	FileOff int64
	Data    []byte
	ID      ChunkID
	BootHot bool
	Sketch  ChunkSketch
}

// ChunkLocation locates one chunk inside its sealed pack.
type ChunkLocation struct {
	ChunkID   ChunkID `json:"chunk_id"`
	PackIndex int     `json:"pack_index"`
	PackOff   uint32  `json:"pack_off"`
	Length    uint32  `json:"length"`
}

// FileManifest records the ordered chunks that reconstruct an immutable file.
type FileManifest struct {
	Path    string          `json:"path,omitempty"`
	RawPath []byte          `json:"raw_path,omitempty"`
	Size    int64           `json:"size"`
	Digest  [32]byte        `json:"digest"`
	BootHot bool            `json:"boot_hot"`
	Chunks  []ChunkLocation `json:"chunks"`
}

// PackMeta describes one sealed pack stored in the backend.
type PackMeta struct {
	Index           int       `json:"index"`
	ID              PackID    `json:"id"`
	BackendKey      string    `json:"backend_key"`
	Role            PackRole  `json:"role"`
	Codec           CodecKind `json:"codec"`
	ChunkCount      int       `json:"chunk_count"`
	RawBytes        int64     `json:"raw_bytes"`
	CompressedBytes int64     `json:"compressed_bytes"`
	SealedBytes     int64     `json:"sealed_bytes"`
	PlainDigest     [32]byte  `json:"plain_digest"`
	MeanEntropy     float64   `json:"mean_entropy"`
}

// Manifest is the sealed per-volume index of an immutable RubikFS image.
type Manifest struct {
	Version           uint32         `json:"version"`
	Scope             Scope          `json:"scope"`
	ImageID           string         `json:"image_id"`
	Policy            Policy         `json:"policy"`
	Codec             CodecKind      `json:"codec"`
	ChunkSize         int            `json:"chunk_size"`
	TargetPackBytes   int            `json:"target_pack_bytes"`
	BootPackBytes     int            `json:"boot_pack_bytes"`
	TotalFiles        int            `json:"total_files"`
	TotalChunks       int            `json:"total_chunks"`
	UniqueChunks      int            `json:"unique_chunks"`
	UncompressedBytes int64          `json:"uncompressed_bytes"`
	PackedSealedBytes int64          `json:"packed_sealed_bytes"`
	ManifestBytes     int64          `json:"manifest_bytes"`
	Packs             []PackMeta     `json:"packs"`
	Files             []FileManifest `json:"files"`
}

// BuildStats reports timing and compression breakdown for a Build call.
type BuildStats struct {
	Policy            Policy        `json:"policy"`
	Codec             CodecKind     `json:"codec"`
	ChunkSize         int           `json:"chunk_size"`
	TargetPackBytes   int           `json:"target_pack_bytes"`
	TotalFiles        int           `json:"total_files"`
	TotalChunks       int           `json:"total_chunks"`
	PackCount         int           `json:"pack_count"`
	BootPackCount     int           `json:"boot_pack_count"`
	UncompressedBytes int64         `json:"uncompressed_bytes"`
	CompressedBytes   int64         `json:"compressed_bytes"`
	SealedPackBytes   int64         `json:"sealed_pack_bytes"`
	ManifestBytes     int64         `json:"manifest_bytes"`
	TotalStoredBytes  int64         `json:"total_stored_bytes"`
	CompressionRatio  float64       `json:"compression_ratio"`
	StoredRatio       float64       `json:"stored_ratio"`
	SketchDuration    time.Duration `json:"sketch_duration_ns"`
	ClusterDuration   time.Duration `json:"cluster_duration_ns"`
	CompressDuration  time.Duration `json:"compress_duration_ns"`
	SealPutDuration   time.Duration `json:"seal_put_duration_ns"`
	TotalBuildTime    time.Duration `json:"total_build_time_ns"`
	MeanAdjacentSim   float64       `json:"mean_adjacent_sim"`
}

// ReadStats summarizes backend fetches, read amplification, and latency for a read workload.
type ReadStats struct {
	FilesRead         int           `json:"files_read"`
	ChunksRead        int           `json:"chunks_read"`
	UsefulBytes       int64         `json:"useful_bytes"`
	BackendGets       int           `json:"backend_gets"`
	PacksFetched      int           `json:"packs_fetched"`
	PackCacheHits     int           `json:"pack_cache_hits"`
	BackendBytesRead  int64         `json:"backend_bytes_read"`
	DecompressedBytes int64         `json:"decompressed_bytes"`
	ReadAmplification float64       `json:"read_amplification"`
	RawReadAmp        float64       `json:"raw_read_amp"`
	Elapsed           time.Duration `json:"elapsed_ns"`
	BackendFetchTime  time.Duration `json:"backend_fetch_time_ns"`
	UnsealDecompTime  time.Duration `json:"unseal_decomp_time_ns"`
}
