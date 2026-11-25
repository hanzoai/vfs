package vfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/hanzoai/vfs/internal/blake3"
	"sort"
	"sync"
)

// The metadata tree is a Merkle search tree whose shape is a pure function of
// its records (a "prolly tree"). Leaves hold sorted records; each higher node
// holds, per child, the child's first key and ref; one ref, the root, names the
// whole tree. Every node is an immutable shard.
//
// Where a node ends is decided by the entries themselves, not by position: an
// entry ends its node when a keyed hash of its key falls under a threshold
// proportional to its size, so nodes average nodeTarget bytes whatever the record
// sizes are. The hash is keyed from the Volume's seed: without the key a guest who
// picks the names of files can pick names that each end their own node, and
// multiply the metadata a commit writes by hundreds. Because a boundary depends
// only on the entries up to it, a change
// disturbs only the nodes it touches: later boundaries fall where they did, the
// untouched nodes are reused by ref, and the new tree is identical to one built
// from scratch from the same records. A commit therefore writes the changed
// leaves, their ancestors and nothing else: its cost is proportional to what
// changed, however large the tree.
const (
	nodeTarget = 64 << 10  // average plaintext bytes of a node
	nodeMax    = 512 << 10 // a node this large ends regardless of the hash
)

// mut is one pending change to the tree: put key=val, or delete key.
type mut struct {
	key, val []byte
	del      bool
}

// entry is one key and value of a node.
type entry struct{ key, val []byte }

// tree reads and rebuilds the metadata tree held in sh. target and max are the
// node sizes above; a Volume always uses nodeTarget and nodeMax, and they are
// fields only so a test can cut small nodes and so build tall trees from few records.
type tree struct {
	sh     *shards
	target uint64
	max    int
	key    [32]byte // keys the boundary hash: derived from the Volume's seed
	hashes sync.Pool
}

// boundaryKey derives the key of the boundary hash from a Volume's seed.
func boundaryKey(seed [32]byte) (key [32]byte) {
	blake3.DeriveKey("hanzoai/vfs tree boundaries", seed[:], key[:])
	return key
}

func newTree(sh *shards, seed [32]byte) *tree {
	t := &tree{sh: sh, target: nodeTarget, max: nodeMax, key: boundaryKey(seed)}
	t.hashes.New = func() any {
		return blake3.Keyed(&t.key)
	}
	return t
}

// ends reports whether e ends its node at level: a function of the entry and the
// tree's key alone.
func (t *tree) ends(level int, e entry) bool {
	h := t.hashes.Get().(*blake3.Hasher)
	h.Reset()
	_, _ = h.Write([]byte{byte(level)})
	_, _ = h.Write(e.key)
	var sum [32]byte
	h.Sum(sum[:0])
	t.hashes.Put(h)
	size := uint64(len(e.key)+len(e.val)) + 4
	return uint64(binary.LittleEndian.Uint32(sum[:4]))*t.target < size<<32
}

// builder cuts a stream of entries into nodes at one level, storing each.
type builder struct {
	t     *tree
	level int
	n     node
	size  int
	out   []entry // (first key, ref) of each stored node, in order
}

func (b *builder) empty() bool { return len(b.n.keys) == 0 }

func (b *builder) add(ctx context.Context, e entry) error {
	b.n.keys = append(b.n.keys, e.key)
	b.n.vals = append(b.n.vals, e.val)
	b.size += len(e.key) + len(e.val) + 4
	if b.t.ends(b.level, e) || b.size >= b.t.max {
		return b.flush(ctx)
	}
	return nil
}

// flush stores the pending node, if any.
func (b *builder) flush(ctx context.Context) error {
	if b.empty() {
		return nil
	}
	n := &node{level: b.level, keys: b.n.keys, vals: b.n.vals}
	r, err := b.t.sh.store(ctx, n)
	if err != nil {
		return err
	}
	b.out = append(b.out, entry{key: n.keys[0], val: r[:]})
	b.n, b.size = node{}, 0
	return nil
}

// build makes the tree of the given sorted entries from level up and returns its
// root. Zero entries make the empty tree.
func (t *tree) build(ctx context.Context, level int, es []entry) (ref, error) {
	for len(es) > 0 {
		b := &builder{t: t, level: level}
		for _, e := range es {
			if err := b.add(ctx, e); err != nil {
				return none, err
			}
		}
		if err := b.flush(ctx); err != nil {
			return none, err
		}
		if len(b.out) == 1 {
			return ref(b.out[0].val), nil
		}
		es, level = b.out, level+1
	}
	return none, nil
}

// child returns the index of the entry of n that covers key: the last one whose
// key is at most key, or the first.
func child(n *node, key []byte) int {
	i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) > 0 })
	return max(i-1, 0)
}

// get returns the value of key.
func (t *tree) get(ctx context.Context, root ref, key []byte) ([]byte, bool, error) {
	if root == none {
		return nil, false, nil
	}
	n, err := t.sh.load(ctx, root)
	if err != nil {
		return nil, false, err
	}
	for n.level > 0 {
		if n, err = t.sh.load(ctx, ref(n.vals[child(n, key)])); err != nil {
			return nil, false, err
		}
	}
	i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) >= 0 })
	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		return n.vals[i], true, nil
	}
	return nil, false, nil
}

// scan calls fn, in key order, for every record whose key starts with prefix,
// until fn returns false.
func (t *tree) scan(ctx context.Context, root ref, prefix []byte, fn func(key, val []byte) bool) error {
	if root == none {
		return nil
	}
	n, err := t.sh.load(ctx, root)
	if err != nil {
		return err
	}
	_, err = t.scanNode(ctx, n, prefix, fn)
	return err
}

func (t *tree) scanNode(ctx context.Context, n *node, prefix []byte, fn func(key, val []byte) bool) (more bool, err error) {
	if n.level == 0 {
		i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], prefix) >= 0 })
		for ; i < len(n.keys); i++ {
			if !bytes.HasPrefix(n.keys[i], prefix) {
				return false, nil
			}
			if !fn(n.keys[i], n.vals[i]) {
				return false, nil
			}
		}
		return true, nil
	}
	first := child(n, prefix)
	for i := first; i < len(n.keys); i++ {
		if i > first && !bytes.HasPrefix(n.keys[i], prefix) {
			return false, nil // this child starts past every key with the prefix
		}
		c, err := t.sh.load(ctx, ref(n.vals[i]))
		if err != nil {
			return false, err
		}
		if more, err = t.scanNode(ctx, c, prefix, fn); !more || err != nil {
			return false, err
		}
	}
	return true, nil
}

// cursor is a position on one node of a level, with the path from the root.
type cursor struct {
	t     *tree
	level int
	path  []step
}

type step struct {
	n *node
	i int // index of the entry taken to go down
}

// seek positions a cursor on the node of level whose range covers key.
func (t *tree) seek(ctx context.Context, root ref, level int, key []byte) (*cursor, error) {
	n, err := t.sh.load(ctx, root)
	if err != nil {
		return nil, err
	}
	c := &cursor{t: t, level: level}
	for n.level > level {
		i := child(n, key)
		c.path = append(c.path, step{n, i})
		if n, err = t.sh.load(ctx, ref(n.vals[i])); err != nil {
			return nil, err
		}
	}
	c.path = append(c.path, step{n, 0})
	return c, nil
}

func (c *cursor) node() *node { return c.path[len(c.path)-1].n }

// bound is the first key of the next node on the level, or nil at the last.
func (c *cursor) bound() []byte {
	for j := len(c.path) - 2; j >= 0; j-- {
		if s := c.path[j]; s.i+1 < len(s.n.keys) {
			return s.n.keys[s.i+1]
		}
	}
	return nil
}

// next moves to the next node on the level. It reports false at the last.
func (c *cursor) next(ctx context.Context) (bool, error) {
	j := len(c.path) - 2
	for j >= 0 && c.path[j].i+1 >= len(c.path[j].n.keys) {
		j--
	}
	if j < 0 {
		return false, nil
	}
	c.path[j].i++
	c.path = c.path[:j+1]
	for {
		s := c.path[len(c.path)-1]
		n, err := c.t.sh.load(ctx, ref(s.n.vals[s.i]))
		if err != nil {
			return false, err
		}
		c.path = append(c.path, step{n, 0})
		if n.level == c.level {
			return true, nil
		}
	}
}

// merge feeds add the records of n with muts applied, in key order. muts are
// sorted and all fall inside n's range.
func merge(n *node, muts []mut, add func(entry) error) error {
	i := 0
	for _, m := range muts {
		for i < len(n.keys) && bytes.Compare(n.keys[i], m.key) < 0 {
			if err := add(entry{n.keys[i], n.vals[i]}); err != nil {
				return err
			}
			i++
		}
		if i < len(n.keys) && bytes.Equal(n.keys[i], m.key) {
			i++ // replaced or deleted
		}
		if !m.del {
			if err := add(entry{m.key, m.val}); err != nil {
				return err
			}
		}
	}
	for ; i < len(n.keys); i++ {
		if err := add(entry{n.keys[i], n.vals[i]}); err != nil {
			return err
		}
	}
	return nil
}

// pass applies muts (sorted, unique keys) to the nodes of one level of the old
// tree. It returns the first keys of the old nodes it replaced and the nodes that
// replace them, in order. Old nodes it did not touch are not read.
//
// A touched node is re-cut from its first entry. If its last entry still ends a
// node, the rest of the level is as it was, and the pass jumps to the next node a
// mutation touches. If it does not (the ending entry was deleted, or a forced
// cut moved), the pass absorbs the next old node and keeps cutting until a node
// ends exactly where one did; that is where the new cuts rejoin the old.
func (t *tree) pass(ctx context.Context, root ref, level int, muts []mut) (out []entry, replaced [][]byte, err error) {
	b := &builder{t: t, level: level}
	c, err := t.seek(ctx, root, level, muts[0].key)
	if err != nil {
		return nil, nil, err
	}
	j := 0
	for {
		n, bound := c.node(), c.bound()
		replaced = append(replaced, n.keys[0])
		k := j
		for k < len(muts) && (bound == nil || bytes.Compare(muts[k].key, bound) < 0) {
			k++
		}
		if err := merge(n, muts[j:k], func(e entry) error { return b.add(ctx, e) }); err != nil {
			return nil, nil, err
		}
		j = k
		switch {
		case b.empty() && j == len(muts):
			return b.out, replaced, nil
		case b.empty():
			if c, err = t.seek(ctx, root, level, muts[j].key); err != nil {
				return nil, nil, err
			}
		case bound == nil:
			return b.out, replaced, b.flush(ctx)
		default:
			if _, err = c.next(ctx); err != nil {
				return nil, nil, err
			}
		}
	}
}

// above turns one level's replacement into the next level's mutations: delete
// each replaced node's entry, put each new node's. Both lists are sorted; a key in
// both is a put.
func above(replaced [][]byte, out []entry) []mut {
	ms := make([]mut, 0, len(replaced)+len(out))
	i, j := 0, 0
	for i < len(replaced) || j < len(out) {
		switch {
		case j == len(out) || (i < len(replaced) && bytes.Compare(replaced[i], out[j].key) < 0):
			ms = append(ms, mut{key: replaced[i], del: true})
			i++
		case i == len(replaced) || bytes.Compare(out[j].key, replaced[i]) < 0:
			ms = append(ms, mut{key: out[j].key, val: out[j].val})
			j++
		default:
			ms = append(ms, mut{key: out[j].key, val: out[j].val})
			i, j = i+1, j+1
		}
	}
	return ms
}

// apply returns the root of the tree that is root with muts applied. muts must be
// sorted by key with no key twice. Every node it writes is new; nothing the old
// tree names is modified, so the old root stays valid until the caller drops it.
func (t *tree) apply(ctx context.Context, root ref, muts []mut) (ref, error) {
	if len(muts) == 0 {
		return root, nil
	}
	if root == none {
		var es []entry
		for _, m := range muts {
			if !m.del {
				es = append(es, entry{m.key, m.val})
			}
		}
		return t.build(ctx, 0, es)
	}
	top, err := t.sh.load(ctx, root)
	if err != nil {
		return none, err
	}
	var out []entry
	for level := 0; level <= top.level; level++ {
		var replaced [][]byte
		if out, replaced, err = t.pass(ctx, root, level, muts); err != nil {
			return none, err
		}
		muts = above(replaced, out)
	}
	// out is the new top level: it may have emptied, shrunk to one node, or grown
	// past one and need levels above it.
	var r ref
	switch len(out) {
	case 0:
		return none, nil
	case 1:
		r = ref(out[0].val)
	default:
		if r, err = t.build(ctx, top.level+1, out); err != nil {
			return none, err
		}
	}
	// A level of one node under a chain of single-entry nodes is the root.
	for {
		n, err := t.sh.load(ctx, r)
		if err != nil {
			return none, err
		}
		if n.level == 0 || len(n.keys) != 1 {
			return r, nil
		}
		r = ref(n.vals[0])
	}
}
