package vfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hanzoai/vfs/pkg/backend"
)

// packTarget is the size at which the open pack is uploaded. A pack is sealed
// chunks laid end to end; where each lies is in the chunk table, so a pack has no
// header and needs no index of its own. Packs are what keep the object count
// proportional to bytes divided by 16 MiB rather than to chunks. They stay under
// the single-request limit of the S3 backend so each is one PUT.
const packTarget = 16 << 20

// packID names a pack: 128 random bits, since a pack's records are written
// before its bytes are final. Integrity does not rest on the name: every chunk
// is checked against its own hash when read.
type packID [16]byte

func packKey(id packID) string {
	h := hex.EncodeToString(id[:])
	return "pack/" + h[:2] + "/" + h
}

// pack is the open pack of a generation: chunks sealed since the last upload.
type pack struct {
	id  packID
	buf []byte
}

// packRoom is what a pack's buffer holds beyond the size at which it is sent: a frame may
// end past it, by at most a chunk and its seal.
const packRoom = maxChunk + 64<<10

func (p *pack) empty() bool { return len(p.buf) == 0 }

// add appends a sealed frame and returns where it lies.
func (p *pack) add(frame []byte) (where, error) {
	if p.empty() {
		if _, err := rand.Read(p.id[:]); err != nil {
			return where{}, err
		}
		if p.buf == nil {
			p.buf = packSlabs.Get(packTarget + packRoom)[:0]
		}
	}
	w := where{pack: p.id, off: uint64(len(p.buf)), n: uint64(len(frame))}
	p.buf = append(p.buf, frame...)
	return w, nil
}

// packed is a pack that is full, and on its way to the store.
type packed struct {
	id   packID
	buf  []byte
	v    *Volume
	done atomic.Bool
}

// packer lays the sealed chunks of a generation into packs and hands each full one to the
// uploader, whichever worker sealed the chunk. Where a frame lies is decided when it is
// added, so a record that points at it can be written before the pack is on its way.
type packer struct {
	g  *generation
	up *uploader

	mu  sync.Mutex
	cur pack
}

func (pk *packer) add(frame []byte) (where, error) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	w, err := pk.cur.add(frame)
	if err == nil && len(pk.cur.buf) >= packTarget {
		pk.send()
	}
	return w, err
}

// flush sends the open pack, whatever its size.
func (pk *packer) flush() {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	if !pk.cur.empty() {
		pk.send()
	}
}

// send hands the open pack to the uploader. mu is held.
func (pk *packer) send() {
	// A pack is memory made from changes that are already counted, and lives beside them until the
	// store has it. A Volume with a pack on its way waits for the Budget before it makes another; one
	// with none makes one, so that a spent Budget never keeps a publication from starting.
	v := pk.g.v
	for v.packs.Load() > 0 {
		spent, freed := v.mem.full()
		if !spent {
			break
		}
		select {
		case <-freed:
			continue
		case <-pk.up.ctx.Done():
		}
		break
	}
	p := &packed{id: pk.cur.id, buf: pk.cur.buf, v: pk.g.v}
	pk.cur = pack{}
	pk.g.packs = append(pk.g.packs, p)
	p.v.packs.Add(int64(len(p.buf)))
	p.v.mem.add(int64(len(p.buf)))
	pk.up.put(packKey(p.id), p.buf, p.finish)
}

// finish is called when the store has the pack: its bytes are not needed again, and the buffer
// goes back to the pool. The pack was held until now so that a request that was tried again
// read the same bytes.
func (p *packed) finish() {
	n := int64(len(p.buf))
	buf := p.buf
	p.buf = nil
	p.done.Store(true)
	packSlabs.Put(buf)
	if p.v.packs.Add(-n) >= 0 {
		p.v.mem.add(-n)
	}
}

// getChunk returns the plaintext of a chunk, checked against its id, reading the chunk
// table of the tree whose root is given. No lock is held: callers that miss the same
// chunk share one fetch.
func (v *Volume) getChunk(ctx context.Context, root ref, id chunkID) ([]byte, error) {
	if p, ok := v.chunks.get(id); ok {
		return p, nil
	}
	return v.fills.do(ctx, id, func() ([]byte, error) { return v.fetchChunk(ctx, root, id) })
}

func (v *Volume) fetchChunk(ctx context.Context, root ref, id chunkID) ([]byte, error) {
	rec, ok, err := v.t.get(ctx, root, keyChunk(id))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: chunk %x is in no table", ErrCorrupt, id[:6])
	}
	w, err := decodeWhere(rec)
	if err != nil {
		return nil, err
	}
	frame, err := v.be.GetRange(ctx, packKey(w.pack), int64(w.off), int64(w.n))
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("%w: pack %x is missing", ErrCorrupt, w.pack[:6])
		}
		return nil, fmt.Errorf("vfs: read chunk %x: %w", id[:6], err)
	}
	plain, err := unseal(v.c, chunkAAD(v.id, id), frame)
	if err != nil {
		return nil, fmt.Errorf("vfs: open chunk %x: %w", id[:6], err)
	}
	if sumChunk(plain) != id {
		return nil, fmt.Errorf("%w: chunk %x does not hash to its id", ErrCorrupt, id[:6])
	}
	v.chunks.add(id, plain, int64(len(plain)))
	return plain, nil
}
