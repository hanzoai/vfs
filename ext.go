package vfs

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

// Extension objects (SPECIFICATION 3.1) are what a feature keeps beside the tree of a
// Volume: a hint, a trace, a retained root. They are sealed to the Volume and to their
// own key, they are replaced by conditional write, and the Head's Round does not cover
// them: a fenced-out writer can still write one, so whatever one holds must be safe to
// find stale.

const (
	// maxExt is the most an extension object holds, and what a read of one is bounded
	// by, with the room the seal takes.
	maxExt       = 32 << 20
	maxExtSealed = maxExt + 64<<10

	// extReplace is how many times a replacement races another writer's before it
	// reports the loss.
	extReplace = 8
)

// reservedExt are the names of the features that the bits of Head.Needs reserve (5.1): a
// feature keeps its objects under its own name, so no other use may take it.
var reservedExt = map[string]bool{"snapshots": true, "links": true, "base": true}

// validExtName reports whether name can name an extension: 1 to 32 bytes of "a" to "z",
// "0" to "9" and "-", and not one reserved for a feature of the Head.
func validExtName(name string) error {
	if reservedExt[name] {
		return fmt.Errorf("%w: the extension name %q is reserved for the feature of that name", fs.ErrInvalid, name)
	}
	if len(name) == 0 || len(name) > 32 {
		return fmt.Errorf("%w: an extension name has 1 to 32 bytes, not %d", fs.ErrInvalid, len(name))
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return fmt.Errorf("%w: %q is not a-z, 0-9 or - in an extension name", fs.ErrInvalid, name)
		}
	}
	return nil
}

// extRest is the last segment of an extension object's key: 64 lower case hex digits of
// the BLAKE3 hash, keyed from the Volume's Seed, of the extension's name, length-framed,
// and the caller's key. The Seed is secret to the Volume, so the store, which sees every
// name, cannot confirm a guess at a key by hashing it, and cannot tell that two Volumes
// use one key; every other name it sees is the hash of sealed bytes or a random id. Every
// key of an extension is one segment of one length, so none is a prefix of another and
// every store keeps it as written. The name is framed by its length, so no split of a
// string into name and key gives another's.
func extRest(seed [32]byte, name string, key []byte) string {
	var k [32]byte
	blake3.DeriveKey("hanzoai/vfs extension names", seed[:], k[:])
	h := blake3.Keyed(&k)
	_, _ = h.Write(binary.AppendUvarint(nil, uint64(len(name))))
	_, _ = h.WriteString(name)
	_, _ = h.Write(key)
	return hex.EncodeToString(h.Sum(nil))
}

// extObjectKey is the store key of an extension object.
func extObjectKey(name, rest string) string { return "ext/" + name + "/" + rest }

// extAAD is what an extension object is sealed to: the Volume, the extension and its key
// in the store, so an object of another Volume, another extension or another key does
// not open.
func extAAD(id volumeID, name, rest string) []byte {
	return bind("ext", id[:], []byte(name), []byte(rest))
}

// ExtKey is where the object that the extension name keeps under key is in the store, from
// the prefix of the Volume: ext/<name>/<64 hex digits>, the digits of a hash keyed from
// the Volume's Seed, so it is not the name of the same key in another Volume.
func (v *Volume) ExtKey(name string, key []byte) (string, error) {
	if err := validExtName(name); err != nil {
		return "", err
	}
	v.mu.RLock()
	seed := v.head.Seed
	v.mu.RUnlock()
	return extObjectKey(name, extRest(seed, name, key)), nil
}

// GetExt returns what the extension name stored under key. The error wraps fs.ErrNotExist
// when nothing is stored there, and ErrCorrupt when the object does not open as this
// Volume's.
func (v *Volume) GetExt(ctx context.Context, name string, key []byte) ([]byte, error) {
	if err := validExtName(name); err != nil {
		return nil, err
	}
	v.mu.RLock()
	id, seed := v.id, v.head.Seed
	v.mu.RUnlock()
	rest := extRest(seed, name, key)
	sealed, _, err := v.be.Get(ctx, extObjectKey(name, rest), maxExtSealed)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("vfs: extension %s has nothing at that key: %w", name, fs.ErrNotExist)
		}
		return nil, err
	}
	return unseal(v.c, extAAD(id, name, rest), sealed)
}

// PutExt stores data under key for the extension name: it creates the object, or
// replaces what is there, whole, so a reader finds the old object or the new. A Volume
// that was only Opened cannot; one that was fenced out can, since the Round orders the
// commits of the tree and not these. data is at most 32 MiB.
func (v *Volume) PutExt(ctx context.Context, name string, key, data []byte) error {
	if err := validExtName(name); err != nil {
		return err
	}
	if len(data) > maxExt {
		return fmt.Errorf("%w: %d bytes in an extension object, at most %d", backend.ErrTooLarge, len(data), maxExt)
	}
	v.mu.RLock()
	id, seed, round := v.id, v.head.Seed, v.round
	v.mu.RUnlock()
	if round == 0 {
		return ErrReadOnly
	}
	rest := extRest(seed, name, key)
	sealed, err := seal(v.c, extAAD(id, name, rest), data)
	if err != nil {
		return err
	}
	objKey := extObjectKey(name, rest)
	for range extReplace {
		_, err = v.be.PutIfAbsent(ctx, objKey, sealed)
		if err == nil || !errors.Is(err, backend.ErrConflict) {
			return err
		}
		// There is an object: replace the one seen, and only that one.
		_, ver, gerr := v.be.Get(ctx, objKey, maxExtSealed)
		if errors.Is(gerr, backend.ErrNotFound) {
			continue // removed since: create it
		}
		if gerr != nil {
			return gerr
		}
		if _, err = v.be.PutIfVersion(ctx, objKey, sealed, ver); err == nil || !errors.Is(err, backend.ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("vfs: extension %s: %w", name, err)
}
