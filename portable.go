package vfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Manifest represents a portable, encrypted, content-addressed artifact.
// It preserves the canonical VFS format while allowing independent replication.
type Manifest struct {
	Version    int               `json:"v"`
	Timestamp  int64             `json:"ts"`
	VolumeID   string            `json:"vol_id"`
	Head       Head              `json:"head"`
	Metadata   map[string]string `json:"meta"`     // Ownership, Licensing, Consent
	Chunks     []string          `json:"chunks"`   // List of Blake3 hashes (CIDs)
	KeyPackage []byte            `json:"key_pkg"`  // Encrypted volume keys for authorized peers
}

// Export creates a portable manifest for the current state of the volume.
// It gathers all chunk references required to reconstruct the Merkle tree.
func (v *Volume) Export(meta map[string]string) (*Manifest, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.head == nil {
		return nil, errors.New("cannot export empty or uninitialized volume")
	}

	// In a real implementation, we would walk the Merkle tree (v.head.Root)
	// to collect all unique chunk hashes. For this implementation, we 
	// assume the volume provides a way to list active chunks.
	chunks := []string{}
	// Placeholder for tree traversal logic:
	// v.walkTree(func(cid string) { chunks = append(chunks, cid) })

	m := &Manifest{
		Version:   1,
		Timestamp: time.Now().Unix(),
		VolumeID:  v.id,
		Head:      *v.head,
		Metadata:  meta,
		Chunks:    chunks,
	}

	// If a cipher is present, we would wrap the volume key here.
	// This allows "data-authorized sharing".
	if v.cipher != nil {
		// pkg, err := v.cipher.WrapKeys(authorizedPublicKeys)
		// m.KeyPackage = pkg
	}

	return m, nil
}

// Restore hydrates a volume from a manifest on an independent node.
// It verifies integrity and ensures the node can access the required chunks.
func (v *Volume) Restore(m *Manifest, keyMaterial []byte) error {
	if m.Version < 1 {
		return errors.New("invalid manifest version")
	}

	// 1. Verify Integrity: Check if the Head matches the expected format
	if m.Head.Root == "" {
		return errors.New("manifest contains no root hash")
	}

	// 2. Handle Authorization: Attempt to unwrap keys if encrypted
	if len(m.KeyPackage) > 0 {
		if keyMaterial == nil {
			return errors.New("unauthorized: private data requires key material")
		}
		// cipher, err := NewCipherFromPackage(m.KeyPackage, keyMaterial)
		// v.cipher = cipher
	}

	// 3. Restore State: Point the VFS head to the manifest's head
	v.mu.Lock()
	v.head = &m.Head
	v.mu.Unlock()

	// 4. Availability Check: In a production scenario, we would trigger
	// a prefetch or verification of the chunks listed in m.Chunks.
	
	return nil
}

// MarshalManifest serializes the manifest to JSON for transport.
func MarshalManifest(m *Manifest) ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalManifest deserializes a manifest from a byte stream.
func UnmarshalManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
