package vfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/hanzoai/vfs/internal/blake3"
	"io"

	"github.com/luxfi/age"
)

// Cipher seals and opens every object a Volume writes: chunks, metadata shards
// and the Head. It is the only door to key material: a Volume is handed a Cipher
// and never reads a key from a file or the environment, never stores one, and
// never sees the key itself.
//
// Seal must authenticate as well as encrypt (a tampered object must fail Open)
// and returns the id of the key it used. The Volume writes that id next to the
// sealed bytes, and hands it back to Open, so a Cipher that holds several keys
// can keep reading old objects while it seals new ones under the current key:
// that is rotation, with no bulk re-encryption.
//
// A Volume checks its Cipher when it is configured: a Cipher whose output
// contains its input, that does not open what it sealed, or that opens bytes
// that were altered, is refused.
//
// Implementations must be safe for concurrent use.
type Cipher interface {
	Seal(plaintext []byte) (sealed []byte, keyID string, err error)
	Open(sealed []byte, keyID string) (plaintext []byte, err error)
}

// Provider yields the Cipher for a Volume, from wherever the caller keeps keys: a
// key service, or the command line's standard input.
type Provider interface {
	Cipher(ctx context.Context) (Cipher, error)
}

var (
	// ErrUnknownKey is returned by Open when an object was sealed under a key the
	// Cipher does not hold: the wrong Volume key, or a retired key already dropped.
	ErrUnknownKey = errors.New("vfs: object is sealed under a key this cipher does not hold")

	// ErrBadCipher is returned when a Cipher fails the check made when a Volume is
	// configured.
	ErrBadCipher = errors.New("vfs: cipher does not seal")
)

// check seals a probe and requires that the sealed bytes do not contain it, that
// opening them returns it, and that opening them with one bit altered does not. It
// cannot prove a Cipher is strong; it refuses the one that is wired to nothing.
func check(c Cipher) error {
	probe := make([]byte, 64)
	if _, err := rand.Read(probe); err != nil {
		return err
	}
	sealed, id, err := c.Seal(append([]byte(nil), probe...))
	if err != nil {
		return fmt.Errorf("%w: seal: %v", ErrBadCipher, err)
	}
	if bytes.Contains(sealed, probe) || bytes.Contains(sealed, probe[:16]) {
		return fmt.Errorf("%w: its output contains its input", ErrBadCipher)
	}
	got, err := c.Open(append([]byte(nil), sealed...), id)
	if err != nil || !bytes.Equal(got, probe) {
		return fmt.Errorf("%w: it does not open what it sealed (%v)", ErrBadCipher, err)
	}
	altered := append([]byte(nil), sealed...)
	altered[len(altered)/2] ^= 1
	if _, err := c.Open(altered, id); err == nil {
		return fmt.Errorf("%w: it opens bytes that were altered", ErrBadCipher)
	}
	return nil
}

// seal frames a sealed object as [len(keyID) byte][keyID][sealed], which makes
// every object self-describing: no table anywhere records which key sealed what.
// What is sealed is aad followed by plain: the aad (bindLen bytes, from bind) names
// what the object is (its kind and what names it), and unseal refuses an object
// whose aad is not exactly the one the reader expects, so an object copied from
// another Volume or place fails closed even under a Cipher both share. A Cipher
// whose output contains the aad is not sealing, and the Seal fails: the check made
// when a Volume is configured cannot see a Cipher that stops sealing later.
func seal(c Cipher, aad, plain []byte) ([]byte, error) {
	if len(aad) != bindLen {
		return nil, fmt.Errorf("vfs: seal: context of %d bytes, want %d", len(aad), bindLen)
	}
	buf := make([]byte, 0, len(aad)+len(plain))
	buf = append(append(buf, aad...), plain...)
	sealed, id, err := c.Seal(buf)
	if err != nil {
		return nil, fmt.Errorf("vfs: seal: %w", err)
	}
	if bytes.Contains(sealed, aad) {
		return nil, fmt.Errorf("%w: its output contains its input", ErrBadCipher)
	}
	if len(id) == 0 || len(id) > 255 {
		return nil, fmt.Errorf("vfs: cipher returned a key id of %d bytes, want 1 to 255", len(id))
	}
	out := make([]byte, 0, 1+len(id)+len(sealed))
	out = append(out, byte(len(id)))
	out = append(out, id...)
	return append(out, sealed...), nil
}

// errContext is returned by unseal for an object sealed to another context.
var errContext = fmt.Errorf("%w: sealed for another volume, kind or place", ErrCorrupt)

// unseal reverses seal for the context the reader expects.
func unseal(c Cipher, aad, frame []byte) ([]byte, error) {
	if len(aad) != bindLen {
		return nil, fmt.Errorf("vfs: unseal: context of %d bytes, want %d", len(aad), bindLen)
	}
	plain, err := open(c, frame)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(plain[:bindLen], aad) != 1 {
		return nil, errContext
	}
	return plain[bindLen:], nil
}

// unsealAny opens an object without asking what it was sealed to. It proves only that
// the Cipher's key sealed it, not for which Volume, kind or place: the one caller is
// Reseal without a place, which checks the rest itself.
func unsealAny(c Cipher, frame []byte) ([]byte, error) {
	plain, err := open(c, frame)
	if err != nil {
		return nil, err
	}
	return plain[bindLen:], nil
}

// open reverses the Cipher and returns the context followed by the plaintext.
func open(c Cipher, frame []byte) ([]byte, error) {
	if len(frame) < 1 || len(frame) < 1+int(frame[0]) {
		return nil, fmt.Errorf("%w: truncated frame", ErrCorrupt)
	}
	n := 1 + int(frame[0])
	plain, err := c.Open(frame[n:], string(frame[1:n]))
	if err != nil {
		return nil, err
	}
	if len(plain) < bindLen {
		return nil, errContext
	}
	return plain, nil
}

// ageCipher is the default Cipher: every object is an age file sealed to a
// hybrid post-quantum recipient (X25519 with ML-KEM-768), so what is stored today
// stays confidential against a future quantum adversary who recorded it.
//
// Anyone who holds a recipient, which is public, can write an age file the identity
// opens. So the plaintext carries a tag keyed from the identity's secret, and Open
// refuses an object whose tag does not verify: a party that holds only the public
// recipient can encrypt to the Volume but cannot forge an object it accepts. The
// tag key is derived from the secret identity string, so everything that can open
// an object can also seal one, and nothing else can.
type ageCipher struct {
	current age.Recipient
	curID   string
	ids     map[string]age.Identity
	tags    map[string][32]byte // per key id: the key of the tag
}

const tagLen = 32

// errOlder is what a tag that is missing or wrong most often means: this layout has no
// version number, so a Volume sealed some other way fails here and nowhere sooner.
const errOlder = "the object was altered, or the Volume was written in a layout this build does not read"

// tagKey derives the tag key of an identity from its secret.
func tagKey(k *age.HybridIdentity) (key [32]byte) {
	blake3.DeriveKey("hanzoai/vfs seal tag", []byte(k.String()), key[:])
	return key
}

// tag is the keyed digest of what is sealed.
func tag(key [32]byte, plain []byte) []byte {
	h := blake3.Keyed(&key)
	_, _ = h.Write(plain)
	return h.Sum(nil)
}

// Hybrid returns the default Cipher. New objects are sealed to current; objects
// sealed to any of retired still open. Each key is an independent identity, so a
// Volume's key is per Volume by construction: nothing is shared across Volumes,
// and dedupe never crosses one.
func Hybrid(current *age.HybridIdentity, retired ...*age.HybridIdentity) Cipher {
	c := &ageCipher{current: current.Recipient(), curID: ageKeyID(current), ids: map[string]age.Identity{}, tags: map[string][32]byte{}}
	for _, k := range append([]*age.HybridIdentity{current}, retired...) {
		c.ids[ageKeyID(k)] = k
		c.tags[ageKeyID(k)] = tagKey(k)
	}
	return c
}

// ageKeyID is the first 8 bytes of blake3 over the public recipient string: it
// names a key without revealing anything secret.
func ageKeyID(k *age.HybridIdentity) string {
	sum := blake3.Sum([]byte(k.Recipient().String()))
	return hex.EncodeToString(sum[:8])
}

func (c *ageCipher) Seal(plain []byte) ([]byte, string, error) {
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, c.current)
	if err != nil {
		return nil, "", err
	}
	if _, err := w.Write(plain); err != nil {
		return nil, "", err
	}
	if _, err := w.Write(tag(c.tags[c.curID], plain)); err != nil {
		return nil, "", err
	}
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), c.curID, nil
}

func (c *ageCipher) Open(sealed []byte, keyID string) ([]byte, error) {
	id, ok := c.ids[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: key %s", ErrUnknownKey, keyID)
	}
	r, err := age.Decrypt(bytes.NewReader(sealed), id)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	plain, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if len(plain) < tagLen {
		return nil, fmt.Errorf("%w: no tag (%s)", ErrCorrupt, errOlder)
	}
	body, got := plain[:len(plain)-tagLen], plain[len(plain)-tagLen:]
	if subtle.ConstantTimeCompare(got, tag(c.tags[keyID], body)) != 1 {
		return nil, fmt.Errorf("%w: the tag does not verify (%s)", ErrCorrupt, errOlder)
	}
	return body, nil
}
