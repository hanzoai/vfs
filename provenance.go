package vfs

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
)

// SignManifest signs the manifest content (excluding the signature field itself).
func SignManifest(m *ModelManifest, privKeyHex string) error {
	privBytes, err := hex.DecodeString(privKeyHex)
	if err != nil || len(privBytes) != ed25519.PrivateKeySize {
		return errors.New("invalid private key")
	}
	priv := ed25519.PrivateKey(privBytes)
	
	// Clear signature for signing
	sigBackup := m.Signature
	m.Signature = ""
	data, _ := json.Marshal(m)
	
	sig := ed25519.Sign(priv, data)
	m.Signature = hex.EncodeToString(sig)
	m.PublicKey = hex.EncodeToString(priv.Public().(ed25519.PublicKey))
	
	if sigBackup != "" && sigBackup != m.Signature {
		// Log or handle re-signing if necessary
	}
	return nil
}

// VerifyManifestProvenance ensures the manifest was signed by the holder of the public key.
func VerifyManifestProvenance(m *ModelManifest) (bool, error) {
	if m.Signature == "" || m.PublicKey == "" {
		return false, errors.New("manifest is not signed")
	}
	
	sig, err := hex.DecodeString(m.Signature)
	if err != nil {
		return false, err
	}
	
	pub, err := hex.DecodeString(m.PublicKey)
	if err != nil {
		return false, err
	}
	
	// Copy manifest and clear signature to verify
	mCopy := *m
	mCopy.Signature = ""
	data, _ := json.Marshal(mCopy)
	
	return ed25519.Verify(pub, data, sig), nil
}
