package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

func internalVolume(t *testing.T, conf Config) (*storetest.Mem, *Volume) {
	t.Helper()
	be := storetest.NewMem()
	conf.Backend, conf.Cipher = be, plainCipher{}
	conf.Journal, conf.Capacity, conf.Disk = storetest.Journals(), 16<<20, storetest.Disk
	if err := Create(context.Background(), conf); err != nil {
		t.Fatal(err)
	}
	conf.Round = 1
	v, err := Attach(context.Background(), conf)
	if err != nil {
		t.Fatal(err)
	}
	return be, v
}

// records counts the data records of a file: its inline bytes (0 or 1) and its
// chunk references.
func records(t *testing.T, v *Volume, ino InodeID) (inline, extents int) {
	t.Helper()
	v.mu.RLock()
	defer v.mu.RUnlock()
	recs, err := v.scan(context.Background(), key(tData, uint64(ino)))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if len(r.key) == prefixLen {
			inline++
		} else {
			extents++
		}
	}
	return inline, extents
}

func put(t *testing.T, v *Volume, name string, data []byte) InodeID {
	t.Helper()
	ctx := context.Background()
	in, err := v.Create(ctx, RootInode, name, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	return in.ID
}

// TestRecordsFollowTheFile: a file's data records are exactly its current form.
// Shrinking, truncating to nothing, growing back into chunks and removing the file
// leave no record of the form it had.
func TestRecordsFollowTheFile(t *testing.T) {
	ctx := context.Background()
	_, v := internalVolume(t, Config{})
	flush := func() {
		t.Helper()
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	ino := put(t, v, "f", randBytes(1, 3<<20))
	flush()
	if in, ex := records(t, v, ino); in != 0 || ex < 3 {
		t.Fatalf("a 3 MiB file has %d inline and %d chunk records", in, ex)
	}
	if err := v.Truncate(ctx, ino, 1000); err != nil { // chunked -> inline
		t.Fatal(err)
	}
	flush()
	if in, ex := records(t, v, ino); in != 1 || ex != 0 {
		t.Fatalf("a 1000 byte file has %d inline and %d chunk records, want 1 and 0", in, ex)
	}
	f, err := v.Open(ctx, ino) // inline -> chunked, by writing several chunks' worth
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(ctx, randBytes(2, 3<<20), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	flush()
	if in, ex := records(t, v, ino); in != 0 || ex < 3 {
		t.Fatalf("after writing 3 MiB the file has %d inline and %d chunk records", in, ex)
	}
	if err := v.Truncate(ctx, ino, 1<<20+7); err != nil { // chunked -> a shorter chunked
		t.Fatal(err)
	}
	flush()
	covered(t, v, ino, 1<<20+7)
	if err := v.Truncate(ctx, ino, 0); err != nil {
		t.Fatal(err)
	}
	flush()
	if in, ex := records(t, v, ino); in+ex != 0 {
		t.Fatalf("an empty file has %d inline and %d chunk records", in, ex)
	}
	f, err = v.Open(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(ctx, randBytes(3, 1<<20), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	flush()
	if in, ex := records(t, v, ino); in+ex == 0 {
		t.Fatal("the file has no data records to remove")
	}
	if err := v.Remove(ctx, RootInode, "f"); err != nil {
		t.Fatal(err)
	}
	flush()
	if in, ex := records(t, v, ino); in+ex != 0 {
		t.Fatalf("a removed file left %d inline and %d chunk records", in, ex)
	}
}

// TestSpillThenFlush: with room for only a few pending changes the Volume writes
// shards as it goes, and a Flush that finds nothing pending still commits them.
func TestSpillThenFlush(t *testing.T) {
	ctx := context.Background()
	be, v := internalVolume(t, Config{Overlay: 40})
	for i := range 300 {
		if _, err := v.Create(ctx, RootInode, fmt.Sprintf("f%03d", i), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if v.over.n >= 40+3 { // the bound is checked before a change, which adds up to three records
		t.Fatalf("%d changes pending, the limit is 40", v.over.n)
	}
	if got := be.Puts()["meta"]; got == 0 {
		t.Fatal("nothing was spilled to shards")
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(ctx, Config{Backend: be, Cipher: plainCipher{}})
	if err != nil {
		t.Fatal(err)
	}
	ents, err := ro.ReadDir(ctx, RootInode)
	if err != nil || len(ents) != 300 {
		t.Fatalf("a fresh Open sees %d of 300 files, %v", len(ents), err)
	}
	// A second Flush with nothing at all pending is the idle case, and writes nothing.
	be.Reset()
	if err := v.Flush(ctx); err != nil || be.Total() != 0 {
		t.Fatalf("idle Flush: %v, %d objects", err, be.Total())
	}
}

// TestPublishedChangesAreOneGeneration: a Volume that holds few changes in memory
// publishes early, and what a read-only view of the store sees is the state at a
// generation's end: the first k files of the hundred, never a part of one generation, and
// every one once a Flush has returned. The shards of a generation are not part of the
// Volume until the Head names them.
func TestPublishedChangesAreOneGeneration(t *testing.T) {
	ctx := context.Background()
	be, v := internalVolume(t, Config{Overlay: 10})
	for i := range 100 {
		if _, err := v.Create(ctx, RootInode, fmt.Sprintf("f%03d", i), 0o644); err != nil {
			t.Fatal(err)
		}
		ro, err := Open(ctx, Config{Backend: be, Cipher: plainCipher{}})
		if err != nil {
			t.Fatal(err)
		}
		ents, _ := ro.ReadDir(ctx, RootInode)
		for j, e := range ents {
			if e.Name != fmt.Sprintf("f%03d", j) {
				t.Fatalf("after %d creates a view of the store sees %q at place %d: not a prefix of them", i+1, e.Name, j)
			}
		}
		if len(ents) > i+1 {
			t.Fatalf("after %d creates a view of the store sees %d files", i+1, len(ents))
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(ctx, Config{Backend: be, Cipher: plainCipher{}})
	if err != nil {
		t.Fatal(err)
	}
	if ents, _ := ro.ReadDir(ctx, RootInode); len(ents) != 100 {
		t.Fatalf("after Flush a view of the store sees %d of 100 files", len(ents))
	}
}

// covered checks the file's chunk references are contiguous from 0 and end at size:
// nothing missing, nothing left over from a longer file.
func covered(t *testing.T, v *Volume, ino InodeID, size uint64) {
	t.Helper()
	v.mu.RLock()
	defer v.mu.RUnlock()
	recs, err := v.scan(context.Background(), key(tData, uint64(ino)))
	if err != nil {
		t.Fatal(err)
	}
	var end uint64
	for _, r := range recs {
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			t.Fatal(err)
		}
		if e.off != end {
			t.Fatalf("chunk at %d, want %d: the list has a gap or a stale chunk", e.off, end)
		}
		end = e.end()
	}
	if end != size {
		t.Fatalf("chunks end at %d, the file is %d bytes", end, size)
	}
}

// TestHeadRejectsGarbage: a Head that is truncated is an error, not a guess; one with
// bytes after Needs keeps them (5.1), and one that needs what this build does not have
// is refused, whatever follows.
func TestHeadRejectsGarbage(t *testing.T) {
	good := Head{Seq: 1, Next: 2}.marshal()
	for name, b := range map[string][]byte{
		"empty":                nil,
		"truncated":            good[:len(good)-1],
		"short":                good[:10],
		"without Needs":        good[:len(good)-1],
		"without Journal":      good[:len(good)-2],
		"a Needs of ten bytes": append(append([]byte(nil), good[:len(good)-1]...), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
	} {
		if _, err := unmarshalHead(b); err == nil {
			t.Fatalf("a Head that is %s was accepted", name)
		}
	}
	h := Head{Seq: 9, Next: 77, Journal: 12345, Tail: tlv(70, "later fields")}
	h.Root[3] = 5
	got, err := unmarshalHead(h.marshal())
	if err != nil || got != h {
		t.Fatalf("Head round trip: %+v, %v", got, err)
	}
}

// TestHeadNeeds: every Needs bit is unknown to this build, so every Head that sets one
// is refused with ErrNeeds, with or without trailing fields, and one that sets none is
// read whatever trails it.
func TestHeadNeeds(t *testing.T) {
	for bit := range 64 {
		h := Head{Seq: 1, Next: 2, Needs: 1 << bit}
		if _, err := unmarshalHead(h.marshal()); !errors.Is(err, ErrNeeds) {
			t.Fatalf("Needs bit %d: %v, want ErrNeeds", bit, err)
		}
		h.Tail = tlv(uint64(bit), "x")
		if _, err := unmarshalHead(h.marshal()); !errors.Is(err, ErrNeeds) {
			t.Fatalf("Needs bit %d with a tail: %v, want ErrNeeds", bit, err)
		}
	}
	h := Head{Seq: 1, Next: 2}
	for _, tail := range []string{tlv(0, ""), tlv(1, "x"), tlv(3, "") + tlv(200, "any bytes \xff\xfe"), tlv(5, "a") + tlv(6, "b") + tlv(1<<62, "c")} {
		h.Tail = tail
		if got, err := unmarshalHead(h.marshal()); err != nil || got.Tail != tail {
			t.Fatalf("tail %q: %+v %v", tail, got, err)
		}
	}
}

// stored returns a file's chunks as stored: offset and length of each.
func stored(t *testing.T, v *Volume, ino InodeID) [][2]int {
	t.Helper()
	v.mu.RLock()
	defer v.mu.RUnlock()
	recs, err := v.scan(context.Background(), key(tData, uint64(ino)))
	if err != nil {
		t.Fatal(err)
	}
	var out [][2]int
	for _, r := range recs {
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, [2]int{int(e.off), int(e.n)})
	}
	return out
}

// cutRuns is the reference: the chunks a cut of the file's content gives when each
// maximal run of written bytes is cut on its own from its first byte, and holes
// have none.
func cutRuns(g *gear, data []byte, written []bool) [][2]int {
	var out [][2]int
	for a := 0; a < len(data); {
		if !written[a] {
			a++
			continue
		}
		b := a
		for b < len(data) && written[b] {
			b++
		}
		for pos := a; pos < b; {
			w := data[pos:min(b, pos+maxChunk)]
			n := cut(g, w)
			out = append(out, [2]int{pos, n})
			pos += n
		}
		a = b
	}
	return out
}

// TestChunkListIsTheWholeFileCut: whatever sequence of writes, appends, holes and
// truncations built a file, its stored chunk list is exactly what cutting the
// file's final bytes from scratch gives. That is what makes a chunk list a function
// of content, so an edit's neighbours dedupe against the file's past, and what
// recutting only around the edit must preserve.
func TestChunkListIsTheWholeFileCut(t *testing.T) {
	ctx := context.Background()
	_, v := internalVolume(t, Config{})
	size := sized(3<<20, 5<<20)
	model := randBytes(7, size)
	written := make([]bool, size)
	for i := range written {
		written[i] = true
	}
	ino := put(t, v, "f", model)
	f, err := v.Open(ctx, ino)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	check := func(what string) {
		t.Helper()
		if err := v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		got, want := stored(t, v, ino), cutRuns(v.gear, model, written)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("after %s the stored chunks are %v, a cut of the file gives %v", what, got, want)
		}
	}
	check("the first write")

	grow := func(n int) {
		model = append(model, make([]byte, n-len(model))...)
		written = append(written, make([]bool, n-len(written))...)
	}
	write := func(off int, b []byte) {
		t.Helper()
		if _, err := f.WriteAt(ctx, b, int64(off)); err != nil {
			t.Fatal(err)
		}
		if off+len(b) > len(model) {
			grow(off + len(b))
		}
		copy(model[off:], b)
		for i := range b {
			written[off+i] = true
		}
	}
	truncate := func(n int) {
		t.Helper()
		if err := v.Truncate(ctx, ino, uint64(n)); err != nil {
			t.Fatal(err)
		}
		if n < len(model) {
			model, written = model[:n], written[:n]
		} else {
			grow(n)
		}
	}
	write(size/2, randBytes(8, 100))
	check("a 100 byte overwrite")
	write(len(model), randBytes(9, 300_000))
	check("an append")
	write(len(model), randBytes(10, 17))
	check("a tiny append")
	write(len(model)+500_000, randBytes(11, 1000)) // past the end: a hole of 500,000 bytes
	check("a write past the end")
	write(10, randBytes(12, 2<<20))
	check("a large overwrite near the start")
	truncate(len(model) - 123_456)
	check("a truncation")
	write(len(model)-5, randBytes(13, 900_000))
	check("a write over the end")
	truncate(len(model) + 700_000) // a hole at the end
	check("an extension")
	write(len(model)-300_000, randBytes(14, 1000)) // data in the middle of the hole
	check("a write into a hole")
	truncate(len(model) / 2)
	truncate(len(model) * 2) // shrink then extend: the voided bytes are a hole, not the old data
	check("a shrink then an extension")
	for i := range 6 { // several edits between commits
		write(i*400_000+1234, randBytes(uint64(20+i), 50_000))
	}
	check("six scattered writes")
}

// TestInodeRecordIsExtensible: a later library adds a field to the inode record
// without a new layout. This library, reading it, checks every field it knows,
// ignores the one it does not, and writes it back when it rewrites the inode; and it
// refuses a record that is out of order, repeats a field, has a known field that does
// not parse, or lacks one it needs.
func TestInodeRecordIsExtensible(t *testing.T) {
	now := time.Unix(1700000000, 12345)
	in := Inode{ID: 7, Mode: fs.ModeDir | 0o750, Size: 99, Mtime: now, Atime: now.Add(time.Second), Ctime: now.Add(2 * time.Second)}
	base := encodeInode(in)

	// A newer library's record: the same fields, then tags 9 and 12 this one lacks.
	var later writer
	later.uvarint(9)
	later.bytes([]byte("owner=1000"))
	later.uvarint(12)
	later.bytes([]byte{0xff, 0x01, 0x02})
	newer := append(append([]byte(nil), base...), later.b...)

	got, err := decodeInode(7, newer)
	if err != nil {
		t.Fatalf("a record with fields from a later library: %v", err)
	}
	if got.Mode != in.Mode || got.Size != in.Size || !got.Mtime.Equal(in.Mtime) {
		t.Fatalf("known fields read wrong: %+v", got)
	}
	// Rewriting the inode (a size or time change) keeps what it does not know.
	got.Size = 100
	again, err := decodeInode(7, encodeInode(got))
	if err != nil || again.Size != 100 || !bytes.Equal(again.later, later.b) {
		t.Fatalf("a rewrite lost the later fields: %v, %x vs %x", err, again.later, later.b)
	}
	if !bytes.Equal(encodeInode(again), encodeInode(got)) {
		t.Fatal("encoding is not canonical")
	}

	// Strict on what it knows.
	field := func(w *writer, tag uint64, value func(*writer)) {
		var v writer
		value(&v)
		w.uvarint(tag)
		w.bytes(v.b)
	}
	var short writer // every field but the change time
	field(&short, fieldMode, func(v *writer) { v.uvarint(uint64(in.Mode)) })
	field(&short, fieldSize, func(v *writer) { v.uvarint(in.Size) })
	field(&short, fieldMtime, func(v *writer) { v.varint(now.UnixNano()) })
	field(&short, fieldAtime, func(v *writer) { v.varint(now.UnixNano()) })
	bad := map[string][]byte{
		"truncated":    newer[:len(base)-3],
		"trailing":     append(append([]byte(nil), base...), 0x80),
		"out of order": append(append([]byte(nil), later.b...), base...),
		"missing time": short.b,
	}
	// A field repeated.
	var dup writer
	dup.uvarint(fieldMode)
	dup.bytes([]byte{1})
	dup.uvarint(fieldMode)
	dup.bytes([]byte{2})
	bad["repeated"] = dup.b
	// A known field whose value does not parse as the whole of its length.
	var junk writer
	junk.uvarint(fieldMode)
	junk.bytes([]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	bad["junk value"] = junk.b
	bad["empty"] = nil
	for name, b := range bad {
		if _, err := decodeInode(7, b); err == nil {
			t.Errorf("%s: a malformed inode record was accepted", name)
		}
	}
}

// TestSealedToItsContext: an object opens only for the Volume, kind and place it was
// sealed to, under a Cipher both share.
func TestSealedToItsContext(t *testing.T) {
	c := plainCipher{}
	frame, err := seal(c, headAAD("place a"), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := unseal(c, headAAD("place a"), frame); err != nil || string(got) != "payload" {
		t.Fatalf("the right context: %q, %v", got, err)
	}
	var v1, v2 volumeID
	v2[0] = 1
	var c1 chunkID
	c1[0] = 1
	for name, wrong := range map[string][]byte{
		"another place":  headAAD("place b"),
		"another volume": shardAAD(v2),
		"another kind":   shardAAD(v1),
		"a chunk":        chunkAAD(v1, c1),
	} {
		if _, err := unseal(c, wrong, frame); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: unsealed a Head sealed to place a: %v", name, err)
		}
	}
	shard, _ := seal(c, shardAAD(v1), []byte("shard bytes"))
	if _, err := unseal(c, shardAAD(v2), shard); !errors.Is(err, ErrCorrupt) {
		t.Errorf("a shard of one Volume opened for another: %v", err)
	}
	chunk, _ := seal(c, chunkAAD(v1, c1), []byte("chunk bytes"))
	c2 := c1
	c2[1] = 1
	if _, err := unseal(c, chunkAAD(v1, c2), chunk); !errors.Is(err, ErrCorrupt) {
		t.Errorf("a chunk opened under another chunk's id: %v", err)
	}
}

// TestChunkingIsPerVolume: the same file is cut into different chunks in two
// Volumes, so chunk lengths cannot identify a known file across them.
func TestChunkingIsPerVolume(t *testing.T) {
	data := randBytes(33, 6<<20)
	cuts := func() [][2]int {
		_, v := internalVolume(t, Config{})
		ino := put(t, v, "f", data)
		if err := v.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		return stored(t, v, ino)
	}
	a, b := cuts(), cuts()
	if len(a) < 4 {
		t.Fatalf("only %d chunks", len(a))
	}
	if fmt.Sprint(a) == fmt.Sprint(b) {
		t.Fatalf("two Volumes cut the same file at the same places: %v", a)
	}
}

// TestChunkHashIsChecked: a chunk sealed correctly, to the right Volume and the right
// id, but holding other bytes, passes the Cipher and the sealing context, and is
// refused only by the hash of what it opened to. (Anyone who holds the key can seal
// one; the check is what a stolen key or a buggy writer meets.)
func TestChunkHashIsChecked(t *testing.T) {
	ctx := context.Background()
	be, v := internalVolume(t, Config{})
	ino := put(t, v, "f", randBytes(41, 100_000))
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	exts := stored(t, v, ino)
	if len(exts) != 1 {
		t.Fatalf("%d chunks, want 1", len(exts))
	}
	// Find the chunk's id and place.
	recs, _ := v.scan(ctx, key(tData, uint64(ino)))
	e, _ := decodeExtent(offsetOf(recs[0].key), recs[0].val)
	rec, ok, err := v.get(ctx, keyChunk(e.id))
	if err != nil || !ok {
		t.Fatalf("chunk table: %v %v", ok, err)
	}
	w, _ := decodeWhere(rec)
	forged, err := seal(v.c, chunkAAD(v.id, e.id), randBytes(42, int(e.n)))
	if err != nil || uint64(len(forged)) != w.n {
		t.Fatalf("forged frame of %d bytes, want %d: %v", len(forged), w.n, err)
	}
	pack, _, _ := be.Get(ctx, packKey(w.pack), backend.Unbounded)
	copy(pack[w.off:], forged)
	if err := be.Put(ctx, packKey(w.pack), pack); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(ctx, Config{Backend: be, Cipher: plainCipher{}})
	if err != nil {
		t.Fatal(err)
	}
	h, _ := ro.Open(ctx, ino)
	if _, err := h.ReadAt(ctx, make([]byte, 100_000), 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reading a chunk that opens to other bytes = %v, want ErrCorrupt", err)
	}
}
