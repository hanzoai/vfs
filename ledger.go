package vfs

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/hanzoai/vfs/journal"
)

// The journal is the node's own disk (docs/journal.md), and what it holds is what a
// Volume has acknowledged and the store does not yet have. It is sealed like everything
// the Volume writes: each journal gets a key of its own, 32 random bytes, which is sealed
// by the Volume's Cipher (the hybrid post-quantum one by default) and kept beside the
// journal, and every record is sealed under that key with AES-256-GCM. The Cipher, which
// costs about 200 microseconds and a kilobyte and a half for each use, is used once per
// journal; a record costs a microsecond and 28 bytes, which is what lets a create of a
// small file be a few hundred bytes of journal and not the hundred times that.

// ledger is a Volume's journal on this node.
type ledger struct {
	j     *journal.Journal
	aead  cipher.AEAD
	aad   []byte // the lineage: a record opens only in the journal it was sealed for
	at    places
	salt  [4]byte       // random 32-bit fixed field per journal key (NIST SP 800-38D §8.2.1)
	nonce atomic.Uint64 // monotonic 64-bit invocation counter: unique per key with zero syscalls
}

// places are the files of one journal on one node: the directory the journal keeps its
// segments in, the file holding its sealed key, and the marker that says the round ended
// with every record published. They are in one directory per Volume, and named by the
// round, so a Round that follows another never reuses what it left.
type places struct {
	dir string
	key string
	end string
}

func journalPlaces(root string, id volumeID, round uint64) places {
	base := filepath.Join(root, hex.EncodeToString(id[:]))
	dir := filepath.Join(base, fmt.Sprintf("r%d", round))
	return places{dir: dir, key: dir + ".key", end: dir + ".end"}
}

// lineage names a history: the Volume and the Round that owned it. Sequence numbers mean
// something only inside one, and the journal refuses a directory of another.
func lineage(id volumeID, round uint64) []byte {
	return binary.BigEndian.AppendUint64(append([]byte(nil), id[:]...), round)
}

// keyAAD is what a journal's key is sealed to.
func keyAAD(id volumeID, round uint64) []byte {
	return bind("journal key", id[:], binary.BigEndian.AppendUint64(nil, round))
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// nextNonce writes a unique 96-bit AES-GCM nonce into dst: a 4-byte random salt fixed
// per journal key followed by an 8-byte strictly increasing invocation counter. Because
// every journal round generates a fresh 256-bit key and never reopens it for writing,
// this satisfies NIST SP 800-38D §8.2.1 with zero per-record syscalls and no 2^32
// birthday collision bound.
func (l *ledger) nextNonce(dst []byte) {
	copy(dst[:4], l.salt[:])
	binary.BigEndian.PutUint64(dst[4:12], l.nonce.Add(1))
}

// seal returns plain sealed for this journal: a unique nonce, then the ciphertext and tag.
func (l *ledger) seal(plain []byte) ([]byte, error) {
	n := l.aead.NonceSize()
	out := make([]byte, n, n+len(plain)+l.aead.Overhead())
	l.nextNonce(out)
	return l.aead.Seal(out, out, plain, l.aad), nil
}

// sealChange seals a change into a buffer taken from the pool: the nonce, then the change
// encoded where it will be encrypted, in place, then the tag. The caller gives the buffer
// back (giveBuffer) once the journal has copied it.
func (l *ledger) sealChange(c *change) ([]byte, error) {
	n := l.aead.NonceSize()
	buf := takeBuffer(n + 96 + len(c.data) + l.aead.Overhead())
	buf = buf[:n]
	l.nextNonce(buf)
	nonce := buf[:n:n]
	buf = c.encodeTo(buf)
	plain := buf[n:]
	return l.aead.Seal(buf[:n], nonce, plain, l.aad), nil
}

// Sealed records are the size of a write and are made at the rate of writes: their buffers
// come from the pool and go back when the journal has copied them.
func takeBuffer(n int) []byte { return packSlabs.Get(n)[:0] }

func giveBuffer(b []byte) { packSlabs.Put(b) }

func (l *ledger) open(sealed []byte) ([]byte, error) {
	n := l.aead.NonceSize()
	if len(sealed) < n+l.aead.Overhead() {
		return nil, fmt.Errorf("%w: a journal record of %d bytes", ErrCorrupt, len(sealed))
	}
	plain, err := l.aead.Open(nil, sealed[:n], sealed[n:], l.aad)
	if err != nil {
		return nil, fmt.Errorf("%w: a journal record does not open: %v", ErrCorrupt, err)
	}
	return plain, nil
}

func (v *Volume) journalOptions(dir string, round, published uint64) (journal.Options, error) {
	opt := journal.Options{
		Capacity:  v.cfg.JournalCapacity,
		Segment:   v.segment(),
		Bound:     v.cfg.Bound,
		Published: published,
		Lineage:   lineage(v.id, round),
		Clock:     v.cfg.Clock,
	}
	if v.cfg.Disk != nil {
		d, err := v.cfg.Disk(dir)
		if err != nil {
			return opt, err
		}
		opt.Disk = d
	}
	return opt, nil
}

// segment is the size of the journal's segment files: a sixteenth of its capacity, from a
// megabyte to the 64 MiB the journal makes by default.
func (v *Volume) segment() int64 {
	return min(max(v.cfg.JournalCapacity/16, 1<<20), 64<<20)
}

// startLedger makes the journal of round: a key of its own, sealed and written before
// the journal exists, and the journal, whose numbering continues after published.
func (v *Volume) startLedger(round, published uint64) (*ledger, error) {
	at := journalPlaces(v.cfg.Journal, v.id, round)
	if err := os.MkdirAll(filepath.Dir(at.dir), 0o700); err != nil {
		return nil, err
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	sealed, err := seal(v.c, keyAAD(v.id, round), key)
	if err != nil {
		return nil, err
	}
	if err := writeDurably(at.key, sealed); err != nil {
		return nil, err
	}
	// The marker is gone from the first moment there is anything to lose.
	if err := os.Remove(at.end); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	opt, err := v.journalOptions(at.dir, round, published)
	if err != nil {
		return nil, err
	}
	j, err := journal.Open(at.dir, opt)
	if err != nil {
		return nil, err
	}
	return &ledger{j: j, aead: aead, aad: lineage(v.id, round), at: at, salt: salt}, nil
}

// openLedger opens the journal that round left on this node, to replay it. published is the
// sequence the store holds of it: the Head's, which is the truth about what is published.
func (v *Volume) openLedger(round, published uint64) (*ledger, error) {
	at := journalPlaces(v.cfg.Journal, v.id, round)
	sealed, err := os.ReadFile(at.key)
	if err != nil {
		return nil, fmt.Errorf("the key of the journal of round %d: %w", round, err)
	}
	key, err := unseal(v.c, keyAAD(v.id, round), sealed)
	if err != nil {
		return nil, fmt.Errorf("the key of the journal of round %d does not open: %w", round, err)
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	opt, err := v.journalOptions(at.dir, round, published)
	if err != nil {
		return nil, err
	}
	j, err := journal.Open(at.dir, opt)
	if err != nil {
		return nil, err
	}
	return &ledger{j: j, aead: aead, aad: lineage(v.id, round), at: at, salt: salt}, nil
}

// retire ends a journal whose every record is published: the marker is written, then the
// directory and the key go. A later Attach that finds the marker knows nothing was left
// behind, and one that finds neither the marker nor the directory knows the journal is
// somewhere else, or gone.
func (l *ledger) retire() error {
	if err := writeDurably(l.at.end, nil); err != nil {
		return err
	}
	if err := os.RemoveAll(l.at.dir); err != nil {
		return err
	}
	if err := os.Remove(l.at.key); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncParent(l.at.dir)
}

// writeDurably writes name whole: a temporary file, synced, renamed into place, and the
// directory synced.
func writeDurably(name string, data []byte) error {
	tmp := name + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, name); err != nil {
		return err
	}
	return syncParent(name)
}

func syncParent(name string) error {
	d, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
