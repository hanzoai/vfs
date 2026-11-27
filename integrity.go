package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

// Verify answers one question about a committed Head: does every object it reaches still
// hold what it was sealed to? It reads the store directly, not the Volume's caches, so a
// shard or pack that changed underneath a running writer is still read as it is now.
//
// It is not Check (check.go), which asks whether the namespace hangs together, and it is
// more than checkTree(full): that one Stats every pack but reads no chunk, because a chunk
// is checked against its hash when a file is read. A pack whose bytes changed under a chunk
// no file has read since is invisible to both, and stops being invisible here.
//
// What it can prove is integrity, not freshness: a Head and a pack that are authentic but
// older than the state the caller expected open and verify, because the seal names the
// Volume and the place, never a moment. Only a reference outside the Volume -- the Round its
// store's conditional write enforces -- can say that.
func (v *Volume) Verify(ctx context.Context, opts VerifyOptions) (*VerifyReport, error) {
	r := &VerifyReport{}
	type job struct {
		id chunkID
		w  where
	}
	var jobs []job
	seen := map[ref]bool{}

	fail := func(f Fault) error {
		r.Damaged = append(r.Damaged, f)
		if !opts.All {
			return f
		}
		return nil
	}

	var walk func(rf ref) error
	walk = func(rf ref) error {
		// The caller's deadline is above every read: a walk that ignores it keeps asking the
		// store for shards the caller no longer wants.
		if err := ctx.Err(); err != nil {
			return err
		}
		if rf == none || seen[rf] {
			return nil
		}
		seen[rf] = true
		key := shardKey(rf)
		sealed, _, err := v.be.Get(ctx, key, maxShard)
		switch {
		case errors.Is(err, backend.ErrNotFound):
			return fail(Fault{Kind: ShardMissing, Key: key, Err: err})
		case err != nil:
			return err // the store failed; that is no verdict on the bytes
		}
		r.Bytes += int64(len(sealed))
		r.Shards++
		if blake3.Sum(sealed) != rf {
			return fail(Fault{Kind: ShardName, Key: key, Err: errors.New("bytes do not hash to the name")})
		}
		plain, err := unseal(v.c, shardAAD(v.id), sealed)
		if err != nil {
			return fail(Fault{Kind: ShardSeal, Key: key, Err: err})
		}
		n, err := decodeNode(plain)
		if err != nil {
			return fail(Fault{Kind: ShardShape, Key: key, Err: err})
		}
		if n.level > 0 {
			for _, val := range n.vals {
				var child ref
				copy(child[:], val)
				if err := walk(child); err != nil {
					return err
				}
			}
			return nil
		}
		for i, k := range n.keys {
			switch {
			case len(k) == prefixLen && k[0] == tData:
				// A small file's bytes live in this shard: the seal above is their only check.
				r.Inline++
				r.InlineBytes += int64(len(n.vals[i]))
			case len(k) == 1+len(chunkID{}) && k[0] == tChunk:
				var id chunkID
				copy(id[:], k[1:])
				w, err := decodeWhere(n.vals[i])
				if err != nil {
					return fail(Fault{Kind: ChunkTable, Key: key, Err: err})
				}
				jobs = append(jobs, job{id, w})
			}
		}
		return nil
	}
	if err := walk(v.root); err != nil {
		// A Fault is a verdict on the bytes and belongs in the report. Anything else -- a
		// store that failed, a cancelled caller -- is no verdict: r.Err() is nil without a
		// Fault, so answering it here would report an intact Head over a store that was
		// never read.
		var f Fault
		if errors.As(err, &f) {
			return r, r.Err()
		}
		return r, err
	}

	// One read per pack, whatever its length: a pack is what the writer made one object of,
	// and reading a frame at a time would ask the store for as many requests as there are
	// chunks. Jobs are sorted so that a pack is read once and its frames are checked in
	// place, in the order they were written.
	sort.Slice(jobs, func(i, j int) bool {
		a, b := jobs[i].w, jobs[j].w
		if c := bytes.Compare(a.pack[:], b.pack[:]); c != 0 {
			return c < 0
		}
		if a.off != b.off {
			return a.off < b.off
		}
		return bytes.Compare(jobs[i].id[:], jobs[j].id[:]) < 0
	})

	var (
		pack     []byte
		loaded   bool
		cur      packID
		err      error
		packSeen = map[packID]bool{}
		gone     = map[packID]bool{} // a missing pack is one fault, however many chunks it held
	)
	for _, j := range jobs {
		// A pack is the unit that resides: stopping here bounds the read, not the walk.
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if !loaded || cur != j.w.pack {
			cur, loaded = j.w.pack, false
			if !packSeen[cur] {
				packSeen[cur] = true
				r.Packs++
			}
			if !gone[cur] {
				key := packKey(cur)
				pack, _, err = v.be.Get(ctx, key, packTarget+packRoom)
				switch {
				case errors.Is(err, backend.ErrNotFound):
					gone[cur] = true
					pack = nil
					if err := fail(Fault{Kind: PackMissing, Key: key, Err: err}); err != nil {
						return r, r.Err()
					}
				case err != nil:
					return r, err
				default:
					loaded = true
					r.Bytes += int64(len(pack))
				}
			}
			if !loaded {
				continue
			}
		}
		key := packKey(cur)
		end := j.w.off + j.w.n
		if end > uint64(len(pack)) {
			if err := fail(Fault{Kind: PackShort, Key: key, Off: j.w.off, N: j.w.n,
				Err: fmt.Errorf("pack has %d bytes and chunk %x ends at %d", len(pack), j.id[:6], end)}); err != nil {
				return r, r.Err()
			}
			continue
		}
		plain, err := unseal(v.c, chunkAAD(v.id, j.id), pack[j.w.off:end])
		if err != nil {
			if err := fail(Fault{Kind: ChunkSeal, Key: key, Chunk: j.id, Off: j.w.off, N: j.w.n, Err: err}); err != nil {
				return r, r.Err()
			}
			continue
		}
		if sumChunk(plain) != j.id {
			if err := fail(Fault{Kind: ChunkHash, Key: key, Chunk: j.id, Off: j.w.off, N: j.w.n,
				Err: errors.New("bytes do not hash to the id")}); err != nil {
				return r, r.Err()
			}
			continue
		}
		r.Chunks++
	}
	return r, r.Err()
}

// VerifyOptions is how far Verify goes after it finds something wrong.
type VerifyOptions struct {
	// All keeps walking after a fault, so the report counts every damaged object instead of
	// the first. A repair decision needs the count; a gate does not.
	All bool
}

// VerifyReport is what Verify read and what it found. Damaged is empty when the Head is
// whole, which is the only state in which Chunks and Inline account for every byte the tree
// names.
type VerifyReport struct {
	Shards      int   // shards read and opened
	Packs       int   // packs read
	Chunks      int   // chunks whose seal and id both hold
	Inline      int   // small files whose bytes live in a shard
	InlineBytes int64 // bytes accounted for by those files
	Bytes       int64 // bytes read from the store: every shard once, every pack once
	Damaged     []Fault
}

// OK reports that nothing Verify read disagrees with what it was sealed to.
func (r *VerifyReport) OK() bool { return len(r.Damaged) == 0 }

// Err is nil when the report is whole, and otherwise names the first fault as corruption, so
// that a caller that only wants a gate can read errors.Is(err, ErrCorrupt) and ignore the rest.
func (r *VerifyReport) Err() error {
	if len(r.Damaged) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrCorrupt, r.Damaged[0])
}

// Damage names the way an object stopped being the object its name and its seal claim.
type Damage int

const (
	ShardMissing Damage = iota + 1 // the store does not hold the shard the tree names
	ShardName                      // the bytes do not hash to the shard's ref
	ShardSeal                      // the shard does not open under this Volume and kind
	ShardShape                     // it opens and is not a node
	ChunkTable                     // a chunk record does not decode
	PackMissing                    // the pack a chunk table names is gone
	PackShort                      // the pack is shorter than the chunk that ends past it
	ChunkSeal                      // the frame does not open under this Volume and chunk id
	ChunkHash                      // it opens and does not hash to the chunk id
)

func (d Damage) String() string {
	switch d {
	case ShardMissing:
		return "shard missing"
	case ShardName:
		return "shard does not hash to its name"
	case ShardSeal:
		return "shard does not open"
	case ShardShape:
		return "shard is not a node"
	case ChunkTable:
		return "chunk record does not decode"
	case PackMissing:
		return "pack missing"
	case PackShort:
		return "pack shorter than a chunk it holds"
	case ChunkSeal:
		return "chunk does not open"
	case ChunkHash:
		return "chunk does not hash to its id"
	}
	return fmt.Sprintf("damage %d", int(d))
}

// Fault is one object that is not what it was sealed to. It is an error so that a fail-fast
// walk can hand back the first one unchanged, and it unwraps to what the check itself
// returned, which may be backend.ErrNotFound or errContext.
type Fault struct {
	Kind  Damage
	Key   string   // the store key, e.g. meta/<2 hex>/<hex> or pack/<2 hex>/<hex>
	Chunk [32]byte // the chunk id, for chunk faults
	Off   uint64   // where the chunk was supposed to lie, for chunk faults
	N     uint64   // its length, for chunk faults
	Err   error
}

func (f Fault) Error() string {
	if f.Off != 0 || f.Kind == ChunkSeal || f.Kind == ChunkHash || f.Kind == PackShort {
		return fmt.Sprintf("%s at %s+%d/%d: %v", f.Kind, f.Key, f.Off, f.N, f.Err)
	}
	return fmt.Sprintf("%s at %s: %v", f.Kind, f.Key, f.Err)
}

func (f Fault) Unwrap() error { return f.Err }
