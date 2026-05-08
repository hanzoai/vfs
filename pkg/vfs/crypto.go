package vfs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/luxfi/age"
)

// Crypto wraps a set of age recipients (write-side) and identities
// (read-side) for per-block encryption. luxfi/age supports classical
// X25519 + hybrid PQ ML-KEM-768 recipients in the same recipient list.
type Crypto struct {
	recipients []age.Recipient
	identities []age.Identity
}

// NewCrypto constructs a Crypto with the given recipients (encrypt
// targets) and identities (decrypt keys). A node that only writes can
// pass nil identities; a read-only node can pass nil recipients.
func NewCrypto(recipients []age.Recipient, identities []age.Identity) (*Crypto, error) {
	if len(recipients) == 0 && len(identities) == 0 {
		return nil, fmt.Errorf("crypto: at least one of recipients or identities required")
	}
	return &Crypto{recipients: recipients, identities: identities}, nil
}

// Encrypt produces an age-encrypted ciphertext for the given plaintext
// block using the configured recipients.
func (c *Crypto) Encrypt(plaintext []byte) ([]byte, error) {
	if len(c.recipients) == 0 {
		return nil, fmt.Errorf("crypto: encrypt called with no recipients")
	}
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, c.recipients...)
	if err != nil {
		return nil, fmt.Errorf("crypto: age.Encrypt: %w", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		return nil, fmt.Errorf("crypto: write plaintext: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("crypto: finalize: %w", err)
	}
	return buf.Bytes(), nil
}

// Decrypt reverses Encrypt using the configured identities.
func (c *Crypto) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(c.identities) == 0 {
		return nil, fmt.Errorf("crypto: decrypt called with no identities")
	}
	r, err := age.Decrypt(bytes.NewReader(ciphertext), c.identities...)
	if err != nil {
		return nil, fmt.Errorf("crypto: age.Decrypt: %w", err)
	}
	plaintext, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("crypto: read plaintext: %w", err)
	}
	return plaintext, nil
}

// HasIdentities reports whether read decryption is possible.
func (c *Crypto) HasIdentities() bool { return len(c.identities) > 0 }

// HasRecipients reports whether write encryption is possible.
func (c *Crypto) HasRecipients() bool { return len(c.recipients) > 0 }
