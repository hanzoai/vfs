package vfs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

// ref names a shard: the blake3 digest of its sealed bytes. The shard is
// stored at meta/<2 hex>/<hex>, immutable, so a ref is both its address and its
// integrity check: a shard whose bytes do not hash to its ref is corrupt.
type ref [32]byte

// none is the ref of the empty tree.
var none ref

func shardKey(r ref) string {
	h := hex.EncodeToString(r[:])
	return "meta/" + h[:2] + "/" + h
}

// node is one decoded shard. A leaf (level 0) holds records; a higher node
// holds, per child, the child's first key and its ref. Nodes are immutable once
// decoded, so a cached node is shared freely.
type node struct {
	level int
	keys  [][]byte
	vals  [][]byte
	size  int64 // plaintext bytes, for cache accounting
}

func (n *node) encode() []byte {
	var w writer
	w.uvarint(uint64(n.level))
	w.uvarint(uint64(len(n.keys)))
	for i := range n.keys {
		w.bytes(n.keys[i])
		w.bytes(n.vals[i])
	}
	return w.b
}

func decodeNode(b []byte) (*node, error) {
	r := reader{b: b}
	level := r.uvarint()
	if level > 255 { // the level is one byte of the boundary hash's input (SPEC 6.3)
		return nil, fmt.Errorf("%w: node of level %d", ErrCorrupt, level)
	}
	n := &node{level: int(level), size: int64(len(b))}
	count := r.uvarint()
	if count == 0 || count > uint64(len(b)) {
		return nil, fmt.Errorf("%w: node of %d entries", ErrCorrupt, count)
	}
	n.keys, n.vals = make([][]byte, count), make([][]byte, count)
	for i := range n.keys {
		n.keys[i], n.vals[i] = r.bytes(), r.bytes()
		if n.level > 0 && len(n.vals[i]) != len(ref{}) {
			return nil, fmt.Errorf("%w: child ref of %d bytes", ErrCorrupt, len(n.vals[i]))
		}
	}
	return n, r.done()
}

// shards reads and writes sealed shards through the backend, with a bounded
// cache of decoded ones.
type shards struct {
	be    backend.Backend
	c     Cipher
	aad   []byte // what every shard is sealed to: the Volume
	cache *lru[ref, *node]
	read  *flights[ref, *node] // one fetch for every caller that misses the same shard
	up    *uploader            // when set, store hands the write to it and returns at once
}

// maxShard bounds what a read will accept for a shard: a node ends at nodeMax, by
// at most one record more.
const maxShard = 2 * nodeMax

// localKey marks a context whose reads may not go to the store.
type localKey struct{}

// local returns ctx marked so that a read of a shard the cache does not hold fails with
// *missing instead of asking the store. A Volume reads under its lock with such a
// context, and loads what it is told is missing after letting the lock go: no caller of
// the Volume waits for the store while holding what every other caller needs.
func local(ctx context.Context) context.Context {
	return context.WithValue(ctx, localKey{}, struct{}{})
}

// missing is the error of a read of a shard that is not in the cache, under a local
// context.
type missing struct{ r ref }

func (m *missing) Error() string { return fmt.Sprintf("vfs: shard %x is not in memory", m.r[:6]) }

// absent returns the shard a read needed and did not have, if that is why err is.
func absent(err error) (ref, bool) {
	var m *missing
	if errors.As(err, &m) {
		return m.r, true
	}
	return none, false
}

func (s *shards) load(ctx context.Context, r ref) (*node, error) {
	if n, ok := s.cache.get(r); ok {
		return n, nil
	}
	if ctx.Value(localKey{}) != nil {
		return nil, &missing{r}
	}
	if s.read == nil {
		return s.fetch(ctx, r)
	}
	return s.read.do(ctx, r, func() (*node, error) { return s.fetch(ctx, r) })
}

func (s *shards) fetch(ctx context.Context, r ref) (*node, error) {
	sealed, _, err := s.be.Get(ctx, shardKey(r), maxShard)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("%w: shard %x is missing", ErrCorrupt, r[:6])
		}
		return nil, fmt.Errorf("vfs: read shard %x: %w", r[:6], err)
	}
	if blake3.Sum(sealed) != r {
		return nil, fmt.Errorf("%w: shard %x does not hash to its name", ErrCorrupt, r[:6])
	}
	plain, err := unseal(s.c, s.aad, sealed)
	if err != nil {
		return nil, fmt.Errorf("vfs: open shard %x: %w", r[:6], err)
	}
	n, err := decodeNode(plain)
	if err != nil {
		return nil, fmt.Errorf("vfs: decode shard %x: %w", r[:6], err)
	}
	s.cache.add(r, n, n.size+int64(48*len(n.keys)))
	return n, nil
}

// store seals a node and returns its ref, and writes it: at once, or by the uploader
// when there is one (the generation that publishes it waits for every write before it
// commits the Head). The write is idempotent (the name is the hash of the bytes), so a
// retry after an error is safe.
func (s *shards) store(ctx context.Context, n *node) (ref, error) {
	plain := n.encode()
	n.size = int64(len(plain))
	sealed, err := seal(s.c, s.aad, plain)
	if err != nil {
		return none, err
	}
	r := ref(blake3.Sum(sealed))
	if s.up != nil {
		s.up.put(shardKey(r), sealed, nil)
		if err := s.up.failed(); err != nil {
			return none, fmt.Errorf("vfs: write shard %x: %w", r[:6], err)
		}
	} else if err := s.be.Put(ctx, shardKey(r), sealed); err != nil {
		return none, fmt.Errorf("vfs: write shard %x: %w", r[:6], err)
	}
	s.cache.add(r, n, n.size+int64(48*len(n.keys)))
	return r, nil
}

// using returns a tree over the same shards, written through up.
func (t *tree) using(up *uploader) *tree {
	sh := *t.sh
	sh.up = up
	u := newTree(&sh, [32]byte{})
	u.target, u.max, u.key = t.target, t.max, t.key
	u.hashes.New = func() any {
		return blake3.Keyed(&u.key)
	}
	return u
}
