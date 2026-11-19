package vfs

import (
	"testing"
)

func TestPortableManifest(t *testing.T) {
	// 1. Setup a mock volume (simulating an agent's state)
	meta := map[string]string{
		"owner":   "did:key:z6Mkg...",
		"license": "Apache-2.0",
		"type":    "agent-memory",
	}

	// Mocking a volume with a head
	v := &Volume{
		id: "test-agent-001",
		head: &Head{
			Root:  "b3-root-hash",
			Round: 42,
		},
	}

	// 2. Export the manifest
	manifest, err := v.Export(meta)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if manifest.Metadata["license"] != "Apache-2.0" {
		t.Errorf("Metadata mismatch")
	}

	// 3. Serialize (Simulate transport over IPFS/P2P)
	data, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// 4. Restore on an "Independent Node"
	newNode := &Volume{id: "independent-node"}
	restoredManifest, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	err = newNode.Restore(restoredManifest, nil) // nil keyMaterial for public data
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// 5. Verify Integrity
	if newNode.head.Root != "b3-root-hash" {
		t.Errorf("Integrity check failed: expected b3-root-hash, got %s", newNode.head.Root)
	}
	
	if newNode.head.Round != 42 {
		t.Errorf("State restoration failed: expected round 42, got %d", newNode.head.Round)
	}
}

func TestUnauthorizedAccess(t *testing.T) {
	// Simulate a manifest with a KeyPackage (encrypted data)
	m := &Manifest{
		Version:    1,
		KeyPackage: []byte("encrypted-key-blob"),
		Head:       Head{Root: "secret-root"},
	}

	v := &Volume{id: "attacker-node"}
	
	// Attempt to restore without keys
	err := v.Restore(m, nil)
	if err == nil {
		t.Error("Expected error for unauthorized access to encrypted manifest, got nil")
	}
}
