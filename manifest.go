package vfs

import (
	"encoding/json"
	"fmt"
	"io"
)

// ModelManifest defines the structure for decentralized model distribution.
type ModelManifest struct {
	Name        string      `json:"name"`
	Version     string      `json:"version"`
	License     string      `json:"license"`
	Files       []FileEntry `json:"files"`
	PublicKey   string      `json:"public_key,omitempty"`
	Signature   string      `json:"signature,omitempty"`
	Description string      `json:"description,omitempty"`
}

// FileEntry maps a logical file within a model to its VFS blocks.
type FileEntry struct {
	Path   string   `json:"path"`
	Size   int64    `json:"size"`
	Hashes []string `json:"hashes"` // List of BLAKE3-256 hashes of 4KiB blocks
}

// EncodeManifest serializes the manifest to JSON.
func EncodeManifest(m *ModelManifest) ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// DecodeManifest deserializes the manifest from JSON.
func DecodeManifest(data []byte) (*ModelManifest, error) {
	var m ModelManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// VerifyIntegrity checks if a block matches the hash declared in the manifest.
func (f *FileEntry) VerifyBlock(index int, data []byte) bool {
	if index >= len(f.Hashes) {
		return false
	}
	actualHash := HashBlock(data) // Uses existing blake3-256 logic
	return actualHash == f.Hashes[index]
}
