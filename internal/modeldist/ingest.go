package modeldist

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

// IngestArtifact streams r in chunkSize pieces, computes per-chunk and full-artifact
// BLAKE3 + SHA-256 digests, writes each chunk into sink (a VFSStoreProvider or other
// WritableProvider), and returns the resulting Artifact metadata.
func IngestArtifact(ctx context.Context, logicalPath string, kind AssetKind, variant string, r io.Reader, chunkSize int64, sink WritableProvider) (Artifact, error) {
	if err := validateRelativePath(logicalPath); err != nil {
		return Artifact{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if !kind.Valid() {
		return Artifact{}, fmt.Errorf("%w: invalid asset kind %q", ErrInvalidManifest, kind)
	}
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	rootB3 := blake3.New()
	rootS2 := sha256.New()
	buf := make([]byte, chunkSize)

	var (
		chunks []ChunkRef
		offset int64
		index  int
	)
	for {
		if err := ctx.Err(); err != nil {
			return Artifact{}, err
		}
		n, err := io.ReadFull(r, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return Artifact{}, fmt.Errorf("modeldist: read %s at offset %d: %w", logicalPath, offset, err)
		}
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			b3, s2 := DigestChunk(chunk)
			_, _ = rootB3.Write(chunk)
			_, _ = rootS2.Write(chunk)
			if sink != nil {
				if err := sink.PutChunk(ctx, b3, chunk); err != nil {
					return Artifact{}, fmt.Errorf("modeldist: store chunk %d (%s): %w", index, b3, err)
				}
			}
			chunks = append(chunks, ChunkRef{
				Index:  index,
				Offset: offset,
				Size:   int64(n),
				BLAKE3: b3,
				SHA256: s2,
			})
			offset += int64(n)
			index++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}

	return Artifact{
		Path:       logicalPath,
		Kind:       kind,
		Variant:    variant,
		Size:       offset,
		ChunkSize:  chunkSize,
		RootBLAKE3: hex.EncodeToString(rootB3.Sum(nil)),
		RootSHA256: hex.EncodeToString(rootS2.Sum(nil)),
		Chunks:     chunks,
	}, nil
}

// PublishManifest stores a signed manifest into sink (and optionally records a ref).
func PublishManifest(ctx context.Context, m *ManifestV1, sink WritableProvider) ([]byte, error) {
	if err := m.validateStructure(); err != nil {
		return nil, err
	}
	cid, err := m.ComputeCID()
	if err != nil {
		return nil, err
	}
	if m.ManifestCID != cid {
		return nil, fmt.Errorf("%w: manifest_cid %s != computed %s", ErrManifestCIDMismatch, m.ManifestCID, cid)
	}
	raw, err := m.MarshalJSONCanonical()
	if err != nil {
		return nil, err
	}
	if err := sink.PutManifest(ctx, m.ManifestCID, raw); err != nil {
		return nil, err
	}
	if vfsSink, ok := sink.(*VFSStoreProvider); ok {
		if err := vfsSink.PutRef(ctx, m.ModelID, m.Version, m.ManifestCID); err != nil {
			return nil, err
		}
	}
	return raw, nil
}

// ReplicateManifest copies a verified manifest and every referenced chunk from
// src to dst, verifying each chunk's BLAKE3 and SHA-256 digests in transit.
// Chunks already present on dst are skipped. Returns the number of chunks written.
func ReplicateManifest(ctx context.Context, m *ManifestV1, src ChunkProvider, dst WritableProvider) (int, error) {
	raw, err := m.MarshalJSONCanonical()
	if err != nil {
		return 0, err
	}
	written := 0
	seen := make(map[string]struct{}, m.TotalChunks())
	for _, art := range m.Artifacts {
		for _, ref := range art.Chunks {
			if _, ok := seen[ref.BLAKE3]; ok {
				continue
			}
			seen[ref.BLAKE3] = struct{}{}
			exists, err := dst.HasChunk(ctx, ref.BLAKE3)
			if err == nil && exists {
				continue
			}
			data, err := src.GetChunk(ctx, ref.BLAKE3, ref.Size)
			if err != nil {
				return written, fmt.Errorf("modeldist: replicate chunk %s from %s: %w", ref.BLAKE3, src.ID(), err)
			}
			if err := VerifyChunkBytes(ref, data); err != nil {
				return written, fmt.Errorf("modeldist: replicate verify chunk %s: %w", ref.BLAKE3, err)
			}
			if err := dst.PutChunk(ctx, ref.BLAKE3, data); err != nil {
				return written, fmt.Errorf("modeldist: replicate put chunk %s to %s: %w", ref.BLAKE3, dst.ID(), err)
			}
			written++
		}
	}
	if err := dst.PutManifest(ctx, m.ManifestCID, raw); err != nil {
		return written, fmt.Errorf("modeldist: replicate manifest to %s: %w", dst.ID(), err)
	}
	if vfsDst, ok := dst.(*VFSStoreProvider); ok {
		if err := vfsDst.PutRef(ctx, m.ModelID, m.Version, m.ManifestCID); err != nil {
			return written, err
		}
	}
	return written, nil
}

// ExportReceipt summarizes an offline, sovereign bundle export into a standard
// hanzoai/vfs file:// backend directory (zero second disk format).
type ExportReceipt struct {
	ExportDir      string `json:"export_dir"`
	BackendURL     string `json:"backend_url"`
	ManifestPath   string `json:"manifest_path"`
	ManifestCID    string `json:"manifest_cid"`
	ChunksExported int    `json:"chunks_exported"`
	TotalBytes     int64  `json:"total_bytes"`
	ExportedAt     string `json:"exported_at"`
}

// ExportBundle exports a verified manifest and all of its content-addressed
// chunks into dstDir using hanzoai/vfs's native file:// backend layout plus a
// top-level manifest.json. The exported directory can be mounted directly by
// any independent VFSStoreProvider or HTTPMirrorServer with zero origin contact.
func ExportBundle(ctx context.Context, m *ManifestV1, policy VerificationPolicy, src ChunkProvider, dstDir string) (*ExportReceipt, error) {
	if err := m.Verify(policy); err != nil {
		return nil, err
	}
	absDir, err := filepath.Abs(dstDir)
	if err != nil {
		return nil, err
	}
	storeDir := filepath.Join(absDir, "vfs-store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, err
	}
	backendURL := fileBackendURL(storeDir)
	b, err := backend.Open(ctx, backendURL)
	if err != nil {
		return nil, fmt.Errorf("modeldist: open export backend %s: %w", backendURL, err)
	}
	defer b.Close()

	dstProvider := NewVFSStoreProvider("export-"+m.ModelID, false, b)
	written, err := ReplicateManifest(ctx, m, src, dstProvider)
	if err != nil {
		return nil, err
	}
	raw, err := m.MarshalJSONCanonical()
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(absDir, "manifest.json")
	if err := atomicWriteFile(manifestPath, raw); err != nil {
		return nil, err
	}
	return &ExportReceipt{
		ExportDir:      absDir,
		BackendURL:     backendURL,
		ManifestPath:   manifestPath,
		ManifestCID:    m.ManifestCID,
		ChunksExported: written,
		TotalBytes:     m.TotalBytes(),
		ExportedAt:     time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func fileBackendURL(absDir string) string {
	p := filepath.ToSlash(absDir)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return (&url.URL{Scheme: "file", Path: p}).String()
}
