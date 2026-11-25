package vfs

// The layout is described by docs/SPECIFICATION.md. This file re-implements the bytes of that
// text, with every constant spelled out, and compares them with what the code writes. A change to
// the code that moves a byte of the layout, and that the code's own tests cannot see because they
// read back what they wrote, fails here.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"io/fs"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/internal/storetest"
)

func specUvarint(n uint64) []byte { return binary.AppendUvarint(nil, n) }
func specFrame(p []byte) []byte   { return append(specUvarint(uint64(len(p))), p...) }

// 4.2: ctx = BLAKE3("hanzoai/vfs seal context" || frame(kind) || frame(field)...)
func specContext(kind string, fields ...[]byte) []byte {
	in := []byte("hanzoai/vfs seal context")
	in = append(in, specFrame([]byte(kind))...)
	for _, f := range fields {
		in = append(in, specFrame(f)...)
	}
	s := blake3.Sum256(in)
	return s[:]
}

func TestSpecContexts(t *testing.T) {
	var id volumeID
	var cid chunkID
	for i := range id {
		id[i] = byte(i + 1)
	}
	for i := range cid {
		cid[i] = byte(0xa0 + i)
	}
	for _, place := range []string{"", "s3://b/p", "file:///a/b", "s3://b/"} {
		if !bytes.Equal(headAAD(place), specContext("head", []byte(place))) {
			t.Errorf("head context for %q differs from 4.2", place)
		}
	}
	if !bytes.Equal(shardAAD(id), specContext("shard", id[:])) {
		t.Error("shard context differs from 4.2")
	}
	if !bytes.Equal(chunkAAD(id, cid), specContext("chunk", id[:], cid[:])) {
		t.Error("chunk context differs from 4.2")
	}
}

// 4.1, 4.2: [len(keyID):1][keyID][sealed], sealed = Seal(ctx || plaintext), and the default
// Cipher's plaintext ends in BLAKE3-keyed(DeriveKey("hanzoai/vfs seal tag", identity), all before it).
func TestSpecFrameAndTag(t *testing.T) {
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := specContext("head", []byte("s3://b/p"))
	plain := []byte("payload bytes")
	frame, err := seal(c, ctx, plain)
	if err != nil {
		t.Fatal(err)
	}
	sum := blake3.Sum256([]byte(k.Recipient().String()))
	wantID := ""
	for _, b := range sum[:8] {
		wantID += string("0123456789abcdef"[b>>4]) + string("0123456789abcdef"[b&15])
	}
	if int(frame[0]) != len(wantID) || string(frame[1:1+len(wantID)]) != wantID {
		t.Errorf("key id %q, want first 8 bytes of BLAKE3(recipient) in hex %q", frame[1:1+int(frame[0])], wantID)
	}
	r, err := age.Decrypt(bytes.NewReader(frame[1+len(wantID):]), k)
	if err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	if _, err := raw.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	got := raw.Bytes()
	if len(got) != len(ctx)+len(plain)+32 || !bytes.Equal(got[:len(ctx)], ctx) || !bytes.Equal(got[len(ctx):len(ctx)+len(plain)], plain) {
		t.Fatalf("plaintext of an object is not ctx || plaintext || tag (%d bytes)", len(got))
	}
	var key [32]byte
	blake3.DeriveKey("hanzoai/vfs seal tag", []byte(k.String()), key[:])
	h, _ := blake3.NewKeyed(key[:])
	h.Write(got[:len(got)-32])
	if !bytes.Equal(h.Sum(nil), got[len(got)-32:]) {
		t.Error("the tag is not BLAKE3-keyed(DeriveKey(tag context, identity), everything before it)")
	}
}

// 5.1: Seq uvarint, Root 32, Next uvarint, ID 16, Seed 32.
func TestSpecHead(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		var h Head
		h.Seq, h.Next = r.Uint64()>>uint(r.IntN(60)), r.Uint64()>>uint(r.IntN(60))
		h.Journal = r.Uint64() >> uint(r.IntN(64))
		for _, b := range [][]byte{h.Root[:], h.ID[:], h.Seed[:]} {
			for i := range b {
				b[i] = byte(r.Uint32())
			}
		}
		// 5.1: the Tail is [tag uvarint][length uvarint][value], tags strictly increasing
		var tag uint64
		for range r.IntN(4) {
			tag += 1 + r.Uint64()>>uint(32+r.IntN(32))
			value := make([]byte, r.IntN(5))
			for i := range value {
				value[i] = byte(r.Uint32())
			}
			h.Tail += string(specUvarint(tag)) + string(specUvarint(uint64(len(value)))) + string(value)
		}
		var want []byte
		want = append(want, specUvarint(h.Seq)...)
		want = append(want, h.Root[:]...)
		want = append(want, specUvarint(h.Next)...)
		want = append(want, h.ID[:]...)
		want = append(want, h.Seed[:]...)
		want = append(want, specUvarint(h.Journal)...)
		want = append(want, specUvarint(h.Needs)...)
		want = append(want, h.Tail...)
		if !bytes.Equal(h.marshal(), want) {
			t.Fatalf("Head differs from 5.1")
		}
		if got, err := unmarshalHead(want); err != nil || got != h {
			t.Fatalf("5.1: the Head does not read back: %+v %v", got, err)
		}
	}
	// 5.1: the fixed bytes of a Head with nothing set (Journal 0, Needs 0) end in two zero bytes.
	var zero Head
	if got := zero.marshal(); len(got) != 1+32+1+16+32+1+1 || got[len(got)-2] != 0 || got[len(got)-1] != 0 {
		t.Fatalf("an empty Head is %x", got)
	}
}

// 5.2: fence || 0x00, round (8, big endian), token (16), payload.
func TestSpecFenceRecord(t *testing.T) {
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	ctx := context.Background()
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	raw := be.Dump()["head"]
	if string(raw[:6]) != "fence\x00" || binary.BigEndian.Uint64(raw[6:14]) != 0 {
		t.Fatalf("created head: magic %q round %d", raw[:6], binary.BigEndian.Uint64(raw[6:14]))
	}
	if _, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 0x0102030405060708, Journal: storetest.Journals(), JournalCapacity: 16 << 20, Disk: storetest.Disk}); err != nil {
		t.Fatal(err)
	}
	raw2 := be.Dump()["head"]
	if !bytes.Equal(raw2[6:14], []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("round field %x", raw2[6:14])
	}
	if bytes.Equal(raw[14:30], raw2[14:30]) {
		t.Error("the token of a claim equals the token of the creator")
	}
	// the payload is carried unchanged by a claim
	if !bytes.Equal(raw[30:], raw2[30:]) {
		t.Error("a claim did not carry the sealed Head unchanged")
	}
	if len(raw) != 6+8+16+len(raw[30:]) {
		t.Error("record length")
	}
}

// 6.1: keys and the inode, entry, extent and chunk-table records.
func TestSpecRecords(t *testing.T) {
	be8 := func(n uint64) []byte { return binary.BigEndian.AppendUint64(nil, n) }
	if !bytes.Equal(keyInode(77), append([]byte{1}, be8(77)...)) {
		t.Error("inode key")
	}
	if !bytes.Equal(keyEntry(5, "abc"), append(append([]byte{2}, be8(5)...), "abc"...)) {
		t.Error("entry key")
	}
	if !bytes.Equal(keyInline(9), append([]byte{3}, be8(9)...)) {
		t.Error("inline key")
	}
	if !bytes.Equal(keyExtent(9, 1<<40), append(append([]byte{3}, be8(9)...), be8(1<<40)...)) {
		t.Error("extent key")
	}
	var cid chunkID
	cid[0], cid[31] = 7, 9
	if !bytes.Equal(keyChunk(cid), append([]byte{4}, cid[:]...)) {
		t.Error("chunk key")
	}
	zig := func(n int64) []byte { return specUvarint(uint64(n<<1) ^ uint64(n>>63)) }
	in := Inode{ID: 4, Mode: fs.ModeDir | 0o755 | fs.ModeSticky, Size: 12345, Mtime: time.Unix(1, 5), Atime: time.Unix(2, 6), Ctime: time.Unix(-3, 7)}
	want := []byte{}
	field := func(tag uint64, v []byte) { want = append(append(want, specUvarint(tag)...), specFrame(v)...) }
	field(1, specUvarint(uint64(in.Mode)))
	field(2, specUvarint(in.Size))
	field(3, zig(in.Mtime.UnixNano()))
	field(4, zig(in.Atime.UnixNano()))
	field(5, zig(in.Ctime.UnixNano()))
	if !bytes.Equal(encodeInode(in), want) {
		t.Errorf("inode record differs from 6.1\n got %x\nwant %x", encodeInode(in), want)
	}
	// the mode bits of 6.1
	for name, tc := range map[string]struct {
		m   fs.FileMode
		bit uint
	}{"sticky": {fs.ModeSticky, 20}, "setgid": {fs.ModeSetgid, 22}, "setuid": {fs.ModeSetuid, 23}, "dir": {fs.ModeDir, 31}, "symlink": {fs.ModeSymlink, 27}, "device": {fs.ModeDevice, 26}, "pipe": {fs.ModeNamedPipe, 25}, "socket": {fs.ModeSocket, 24}, "chardev": {fs.ModeCharDevice, 21}, "irregular": {fs.ModeIrregular, 19}} {
		if uint32(tc.m) != 1<<tc.bit {
			t.Errorf("mode bit %s is %#x, SPEC 6.1 says bit %d", name, uint32(tc.m), tc.bit)
		}
	}
	if got := encodeEntry(300, fs.ModeDir|0o700); !bytes.Equal(got, append(specUvarint(300), specUvarint(1<<31)...)) {
		t.Errorf("entry value %x", got)
	}
	e := extent{off: 1, n: 70000, id: cid}
	if got := encodeExtent(e); !bytes.Equal(got, append(append([]byte{}, cid[:]...), specUvarint(70000)...)) {
		t.Errorf("extent value %x", got)
	}
	var pid packID
	pid[3] = 3
	w := where{pack: pid, off: 99999, n: 1234}
	if got := encodeWhere(w); !bytes.Equal(got, append(append(append([]byte{}, pid[:]...), specUvarint(99999)...), specUvarint(1234)...)) {
		t.Errorf("chunk table value %x", got)
	}
	if len(pid) != 16 {
		t.Error("pack id is not 16 bytes")
	}
}

// 6.2: [level uvarint][count uvarint] then (key, value) pairs of length-prefixed byte strings.
func TestSpecNodeAndBoundary(t *testing.T) {
	n := &node{level: 3, keys: [][]byte{{1, 2}, {3}}, vals: [][]byte{{9, 9, 9}, {}}}
	want := append(append(specUvarint(3), specUvarint(2)...), []byte{2, 1, 2, 3, 9, 9, 9, 1, 3, 0}...)
	if !bytes.Equal(n.encode(), want) {
		t.Errorf("node %x want %x", n.encode(), want)
	}
	// 6.3: size = len(key)+len(value)+4; u32le(H[0:4]) * 65536 < size * 2^32, H keyed BLAKE3 of level_byte||key
	for _, seed := range [][32]byte{{}, {1}, {0xff, 0xfe}} {
		var K [32]byte
		blake3.DeriveKey("hanzoai/vfs tree boundaries", seed[:], K[:])
		tr := newTree(nil, seed)
		r := rand.New(rand.NewPCG(5, 6))
		hits := 0
		for i := range 100000 {
			key := make([]byte, 1+r.IntN(40))
			for j := range key {
				key[j] = byte(r.Uint32())
			}
			val := make([]byte, r.IntN(400))
			level := r.IntN(4)
			h, _ := blake3.NewKeyed(K[:])
			h.Write(append([]byte{byte(level)}, key...))
			sum := h.Sum(nil)
			size := uint64(len(key) + len(val) + 4)
			want := uint64(binary.LittleEndian.Uint32(sum[:4]))*65536 < size<<32
			if got := tr.ends(level, entry{key: key, val: val}); got != want {
				t.Fatalf("seed %x entry %d: ends = %v, SPEC 6.3 says %v", seed[:2], i, got, want)
			}
			if want {
				hits++
			}
		}
		if hits == 0 {
			t.Error("no boundary at all in 100000 entries")
		}
	}
}

// 7.2: the gear table and the cut, from the text.
func TestSpecChunker(t *testing.T) {
	seed := [32]byte{9, 8, 7}
	h := blake3.New()
	h.Write([]byte("hanzoai/vfs chunker gear table"))
	h.Write(seed[:])
	var raw [2048]byte
	h.Digest().Read(raw[:])
	var g [256]uint64
	for i := range g {
		g[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	if *newGear(seed) != g {
		t.Fatal("gear table differs from 7.2")
	}
	maskS := uint64(1<<21-1) << 43
	maskL := uint64(1<<17-1) << 47
	spec := func(b []byte) int {
		if len(b) <= 64<<10 {
			return len(b)
		}
		var fp uint64
		i := 64 << 10
		for ; i < min(512<<10, len(b)); i++ {
			fp = fp<<1 + g[b[i]]
			if fp&maskS == 0 {
				return i + 1
			}
		}
		for ; i < len(b); i++ {
			fp = fp<<1 + g[b[i]]
			if fp&maskL == 0 {
				return i + 1
			}
		}
		return len(b)
	}
	r := rand.New(rand.NewPCG(7, 7))
	for range 40 {
		b := make([]byte, 4<<20)
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		b = b[:64<<10+r.IntN(4<<20-64<<10)]
		if got, want := cut(newGear(seed), b), spec(b); got != want {
			t.Fatalf("cut = %d, SPEC 7.2 says %d", got, want)
		}
	}
}

// specExtName is 3.1: the last segment of an extension object's key, in 64 lower case hex
// digits, of BLAKE3 keyed with DeriveKey("hanzoai/vfs extension names", Seed) over the length
// of the name as a uvarint, the name, and the key.
func specExtName(seed [32]byte, name string, key []byte) string {
	var k [32]byte
	blake3.DeriveKey("hanzoai/vfs extension names", seed[:], k[:])
	h, _ := blake3.NewKeyed(k[:])
	h.Write(specUvarint(uint64(len(name))))
	h.Write([]byte(name))
	h.Write(key)
	return hex.EncodeToString(h.Sum(nil))
}

// 3.1 and 4.2: an extension object is at ext/<name>/<64 hex digits of a hash keyed from the
// Seed> and is sealed to the context of kind "ext" with the Volume id, the name and that last
// segment. The same key in two Volumes is two names, and so is one key under two extensions.
func TestSpecExtension(t *testing.T) {
	ctx := context.Background()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	vol := func() (*storetest.Mem, *Volume) {
		be := storetest.NewMem()
		if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
			t.Fatal(err)
		}
		v, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 1, Journal: storetest.Journals(), JournalCapacity: 16 << 20, Disk: storetest.Disk})
		if err != nil {
			t.Fatal(err)
		}
		return be, v
	}
	be, v := vol()
	be2, v2 := vol()
	names := map[string]bool{}
	for _, key := range [][]byte{nil, []byte("workflow key"), bytes.Repeat([]byte{0xff}, 3000)} {
		rest := specExtName(v.head.Seed, "trace", key)
		if len(rest) != 64 {
			t.Fatal("the text says 64 hex digits")
		}
		data := []byte("the trace of " + rest[:8])
		if err := v.PutExt(ctx, "trace", key, data); err != nil {
			t.Fatal(err)
		}
		sealed, ok := be.Dump()["ext/trace/"+rest]
		if !ok {
			t.Fatalf("the object is not at ext/trace/%s", rest)
		}
		got, err := unseal(c, specContext("ext", v.id[:], []byte("trace"), []byte(rest)), sealed)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("the object does not open under the context of 4.2: %q %v", got, err)
		}
		if !bytes.Equal(extAAD(v.id, "trace", rest), specContext("ext", v.id[:], []byte("trace"), []byte(rest))) {
			t.Fatal("extAAD differs from 4.2")
		}
		// the same key in another Volume, and under another name, is another object
		if err := v2.PutExt(ctx, "trace", key, data); err != nil {
			t.Fatal(err)
		}
		other := specExtName(v2.head.Seed, "trace", key)
		if other == rest {
			t.Fatal("two Volumes name the object of one key alike")
		}
		if _, ok := be2.Dump()["ext/trace/"+other]; !ok {
			t.Fatalf("the object of the second Volume is not at ext/trace/%s", other)
		}
		for _, n := range []string{rest, other, specExtName(v.head.Seed, "trace2", key), specExtName(v.head.Seed, "trac", append([]byte("e"), key...))} {
			if names[n] {
				t.Fatal("two extension objects have one name")
			}
			names[n] = true
		}
	}
}
