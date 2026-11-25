# Specification notes

What follows is the behaviour of the on-disk layout and of the commit, fence and
sealing protocols, stated so that a third party can write a compatible reader or
writer. It describes what the reference implementation does today and what its tests
hold it to. "MUST", "SHOULD" and "MAY" are used as in RFC 2119. Sections marked
(informative) explain and bind no one. There is one layout: it carries no version
number, and nothing here describes any other. Every byte this text fixes is pinned by a
known-answer test that rebuilds it from the text alone, with each constant spelled out
(`spec_test.go` and `pin_test.go`, section 10), or by a fixture of what an outside library
produced (the Cipher): a change that moves a byte of the layout fails there, whatever the
other tests read back.

## 1. Conventions

* Integers inside a record are varints: an unsigned integer is an unsigned LEB128
  (7 bits per byte, least significant group first, at most 10 bytes); a signed integer
  is that encoding of its zigzag form (`(n << 1) ^ (n >> 63)`). A byte string is a
  varint length and then the bytes. A decoder MUST reject a truncated item and MUST
  reject bytes left over after the last item of a record.
* **An integer is in its shortest form**, and a value is as wide as its field and no
  wider. An encoding whose last byte is zero after a first byte that continues (`80 00`
  for 0) is another spelling of a number that has one, and a decoder MUST reject it; so
  it MUST reject one that does not fit 64 bits. A field of a fixed width (the mode and
  the length of an extent are 32 bits, a node's level is 8) is rejected, not truncated,
  when its value is wider. Without this one record has many byte strings, and a hash of
  the bytes (a shard's ref, a Head's seal) names one of them. A writer never writes
  another form, so a Volume that has one was not written by a conformant writer.
* Integers inside a key, and in the fence header, are big-endian and fixed width.
* A hash is BLAKE3 with a 32-byte output unless said otherwise.
* A Volume lives in one place: a prefix of a bucket of an object store, or a
  directory. The place has a canonical location string, the same for every process that
  opens it (`s3://<bucket>/<prefix>` for an object store, `file://<absolute directory>`
  for a directory). A reader and a writer MUST use the same string. The string is
  canonical: the prefix has no leading or trailing `/`; the directory is absolute,
  cleaned (no `.`, `..`, doubled or trailing `/`) and has its symbolic links resolved,
  so every spelling of one place gives one string. It never contains a credential, an
  endpoint or a query.

## 2. What the store must provide

A Volume needs a keyed store of byte strings with these operations. Keys below are
relative to the Volume's prefix, which is joined to them with one `/`.

| Operation | Contract |
|---|---|
| `Get(key, max)` | the bytes at `key` and a version token naming exactly those bytes; absent is `NotFound`; an object longer than `max` is `TooLarge` and is not returned |
| `GetRange(key, off, n)` | exactly `n` bytes from `off`; a range past the end is an error, never a short read |
| `Put(key, data)` | store `data`, replacing any object; used only for objects no one else writes (packs, shards) |
| `PutIfAbsent(key, data)` | store `data` only if `key` does not exist, atomically with the check; `Conflict` otherwise; returns the new version |
| `PutIfVersion(key, data, v)` | store `data` only if `key` exists and its version is `v`, atomically with the check; `Conflict` otherwise; of several calls naming one `v` exactly one succeeds; returns the new version |
| `Delete(key)` | remove; removing an absent key is not an error |
| `List(prefix)` | every object whose key begins with `prefix` as a string (`ext/y/a` lists `ext/y/ab` and `ext/y/a/c`), each with its key, its size and the time the store last modified it, by the store's own clock; none is hidden |
| `Stat(key)` | the size of the object at `key` without its body; absent is `NotFound` |

**Buffers.** The caller owns the buffer of a write. Once `Put`, `PutIfAbsent` or `PutIfVersion`
returns, the store client MUST NOT read it again, whatever it did to send it (one request, parts, a
retry after a failure, a request still running when the caller gave up): the caller may reuse it,
clear it or give it back to a pool at once, and a writer does. A call that fails may have been
read: the caller keeps the buffer until it returns, and reuses it only after. The conformance
suite (`internal/storetest`) clears the buffer the instant a write returns and reads the object
back, against the memory store, the directory store, the S3 client over a protocol server, and a
real server.

A Volume reads and writes with the first six. `List` and `Stat` are for tools: a
collector (11.1), and the check of a copied Volume (5.4).

Every key of this text is 1 to 511 bytes of `a-z`, `0-9` and `-`, in segments of 1 to 255
bytes separated by `/`. A store MUST keep every key of that grammar exactly as given, and
`List` it by its string prefix with none hidden; it MAY keep other keys (the SQLite
replica of this repository names objects `orgs/acme/kv.db`) and this text promises nothing
for them. The grammar is the alphabet that every store keeps as written: a case-insensitive
file system, one that normalises names, one that gives `.` and `..` a meaning, or one that
reads `%2e` as a dot would each keep a wider one differently, and a store that keeps keys
as files has a name for a segment only up to 255 bytes. The longest key leaves a 512-byte
prefix room in the 1,024 bytes that an object store keeps. All keys of one kind have the
same number of segments, so no key is a prefix of another at a `/`, which a store that
keeps keys as files could not hold: it refuses a key that is the directory of another
(`InvalidKey`), and a read of such a name answers absent or empty, whichever the store does.

A version token is opaque and compared only for equality: two reads of an unchanged
object return the same token, and any write by anyone changes it. A store that cannot
write an object atomically under a condition (for instance because the object is too
large to send in one request) MUST refuse the conditional write with `TooLarge`; it
MUST NOT perform it unconditionally. The reference object-store backend sends an object
in one request up to 64 MiB and refuses conditional writes above that. The store is
trusted to return the object it holds, and not trusted with confidentiality: it learns
object sizes, key names and timing, and no content.

A prefix is a sequence of `/`-separated segments; no segment is empty, `.` or `..`,
none contains a control character (below 0x20, or 0x7f), and the whole is at most 512
bytes. A prefix that fails this MUST be refused, not cleaned. A Volume's prefix MUST NOT
nest inside another's, in either direction (`vols/a` and `vols/a/b`): a listing of one
would return the other's objects, and a collector of the first would find them unknown,
or worse, take them for its own. Two Volumes share a bucket by prefixes that name different
segments (`vols/a`, `vols/b`).

## 3. Objects

A Volume is three kinds of object, and may hold others (3.1). Every object is a sealed
frame (section 4).

| Key | Object | Mutable | Name |
|---|---|---|---|
| `head` | the Head, inside a fence record (section 5) | yes, only by conditional write | fixed |
| `meta/<2 hex>/<64 hex>` | a metadata shard (section 6) | no | the hash of the shard's sealed bytes, hex; the 2 hex digits are its first byte |
| `pack/<2 hex>/<32 hex>` | a pack of chunks (section 7) | no | 128 random bits, hex; the 2 hex digits are its first byte |

Of these three, everything but `head` is written once, under a name that is a hash of
its bytes or a random identifier, and never changes. A writer MAY write an immutable
object again, or after its caller gave up; the second write puts the same bytes at the
same name. A reader MUST check a shard against its name (the hash of the sealed bytes)
and a chunk against its id (section 7), and MUST treat a mismatch as corruption, not
retry it.

### 3.1 Objects this text does not define

Something that is not the tree of the Volume, such as a hint that makes the next
attachment faster, can be stored beside it. Such an object is an extension object, and
the rules for one are these.

* **Unknown objects are left alone.** A reader MUST ignore every key it does not know
  and MUST NOT fail because one exists. A collector, or any tool that removes objects,
  MUST NOT delete an object it does not know: it deletes only an object under `meta/`
  or `pack/` whose key has exactly the shape in the table above and that no Head it
  must keep reaches. A tool that finds a key of another shape, under `meta/` or `pack/`
  or anywhere else, reports it and leaves it.
* **Naming.** An extension object lives at `ext/<name>/<rest>`. `<name>` is 1 to 32
  bytes of `a-z`, `0-9` and `-` and names the extension. `<rest>` is 64 lower case hex
  digits of a hash that is keyed from the Volume, whatever the caller's key is:
  `BLAKE3` keyed with `K = DeriveKey("hanzoai/vfs extension names", Head.Seed)` (32 bytes)
  over `len(<name>)` as a uvarint, then `<name>`, then the caller's key. The extension's
  own name for an object (a workflow, a snapshot) is bytes of its choosing and is never
  part of the key in the store. The store sees every name, and `Head.Seed` is secret to
  the Volume (5.1), so the store cannot confirm a guess at a key by hashing it, and the
  same key in two Volumes, or under two extensions, is two names: nothing in the name
  says that two Volumes share a workflow. The name is framed by its length, so no way of
  splitting a string into a name and a key gives another's. Every extension key is one
  segment of one length: none is a prefix of another, the alphabet is the grammar of
  section 2 and no store holds it differently from another, and the whole key is at most
  101 bytes. An extension that must enumerate its objects keeps an index object of its
  own, as it keeps any other state; `List` shows hashes. Nothing but an extension defined
  this way uses `ext/`. A Volume's `Seed` is not rewritten (5.1), so a name is stable for
  the life of the Volume and moves with it (5.4).
* **Reserved names.** `snapshots`, `links` and `base` are the names of the features of
  5.1, and an extension MUST NOT take them for any other use: a feature keeps its objects
  under its own name. The reference implementation refuses them.
* **Sealing.** An extension object is a sealed frame (4.2) whose context has the kind
  `ext` and the fields: the 16-byte Volume id, the extension's `<name>` as bytes, and
  `<rest>` as bytes. It is bound to the Volume (it moves with it, as shards do, and
  needs no reseal) and to its own key, so an object of one Volume, of another
  extension or of another key does not open.
* **Size.** An extension object holds at most 32 MiB, and a reader MUST bound its read of
  one at that and the room the seal takes (the reference implementation reads 64 KiB
  more).
* **Fencing.** The Head's Round orders the commits of the tree; it does not cover
  extension objects. A writer that has been fenced out can still write one. An
  extension MUST therefore be safe against that: whatever version of one a reader finds,
  an old one, a stale writer's or none, the contents of the Volume are what its Head
  says, and the extension is only a hint, or says what a stale write costs and why it
  is bearable. State the tree depends on is not an extension: it is a change to this
  text.
* **Replacement.** A mutable extension object is created with `PutIfAbsent` and
  replaced with `PutIfVersion`, so a reader finds the old object or the new one, whole,
  and two writers do not overwrite each other's replacement unseen. An immutable one
  is named by a hash of its sealed bytes or by a random identifier, as in the table (an
  extension whose objects are immutable keeps its own index of them).
* **Unreachable is not garbage.** The rule that an object no Head reaches is garbage
  (section 11.1) applies to the three kinds of the table and to no extension object.
  An extension that wants reclamation defines it, and a collector applies it only if it
  implements that extension.

The reference implementation has `GetExt` and `PutExt` for them. The first user is the
trace store of `docs/prefetch.md` (S6), one object per workflow key at
`ext/trace/<keyed hash of the workflow key>`.

## 4. Sealing

### 4.1 The Cipher

All object bytes pass through a Cipher, the only holder of key material. A Cipher
provides:

* `Seal(plaintext) -> (sealed, keyID)`: authenticated encryption. `keyID` names the key
  used, 1 to 255 bytes, and reveals nothing secret.
* `Open(sealed, keyID) -> plaintext`: fails if `sealed` was not produced by `Seal`
  under the key `keyID` names, or was altered, or if the Cipher does not hold that
  key (a distinct error, "unknown key").

Authenticated means authenticated against the holder of the key material that opens:
`Open` MUST refuse an object that a party without that material could have made. A
public-key construction does not give that by itself, since whoever holds the public
recipient can encrypt to the Volume; the Cipher MUST add something only the holders of
the secret can produce. A Cipher that does not is a Cipher under which anyone who
learns the recipient can forge a Head or a shard.

A Cipher MAY hold several keys, sealing under the current one and opening any it
holds; that is key rotation, and needs no re-encryption. A Cipher MUST be safe for
concurrent use. When a Volume is configured it MUST check its Cipher: it seals a
random 64-byte probe and refuses the Cipher unless the sealed bytes contain neither
the probe nor its first 16 bytes, `Open` returns the probe, and `Open` fails when one
bit in the middle of the sealed bytes is flipped. This refuses a Cipher wired to
nothing; it does not prove a Cipher strong. Because a Cipher can stop sealing after it
was checked (a fallback to the identity when its key service is gone), the Volume also
checks every `Seal`: the sealed bytes MUST NOT contain the context (4.2), and a Seal
whose output does is an error, not a write.

The reference implementation's default Cipher is a public-key hybrid of X25519 and
ML-KEM-768, producing one self-contained encrypted file per sealed object; its key id
is the first 8 bytes of the hash of the public recipient, in hex. Its integrity rests
on the secrecy of the identity, not of the recipient: the plaintext carries a final
32-byte tag, the BLAKE3 keyed hash of everything before it under a key derived from
the secret identity (`DeriveKey("hanzoai/vfs seal tag", identity string)`), and `Open`
refuses an object whose tag does not verify. All 32 bytes are compared: a tag that is
right in its first 16 bytes and wrong after them is refused. A party that holds only the public
recipient can encrypt to the Volume but cannot make an object the Volume accepts.
Everything that can open an object can also seal one. (informative)

### 4.2 The frame and its context

A sealed object is `[len(keyID): 1 byte][keyID][sealed]`, where `sealed` is
`Seal(ctx || plaintext)`. The context `ctx` names what the object is and is not
stored. It is always exactly 32 bytes: the BLAKE3 digest of

    "hanzoai/vfs seal context" || frame(kind) || frame(field 1) || frame(field 2) ...

where `frame(x)` is the length of `x` as an unsigned varint followed by `x`, and the
kind and fields are:

| Object | kind | fields |
|---|---|---|
| Head | `head` | the canonical location string of the place |
| shard | `shard` | the 16-byte Volume id |
| chunk | `chunk` | the 16-byte Volume id, the 32-byte chunk id |

A reader supplies the context it expects, and after `Open` MUST check that the first
32 bytes of the plaintext are equal to it, byte for byte, and refuse the object
otherwise; the plaintext proper is what follows. Because the context has a fixed
length and its preimage is length-framed, no context is a prefix of another and two
different (kind, fields) never share one, so an object of another Volume, of another
kind or of another place fails closed even under a Cipher both Volumes share. A
variable-length context compared as a prefix would not: a Head sealed for
`.../vol10` would open at `.../vol1` and at the bucket root.

A Head is bound to its place: a Volume copied to another place does not open there
until its Head is sealed again for that place. Shards and chunks are bound to the
Volume, so they move with it, and moving a Volume is copying its objects and then
resealing the Head (5.4).

## 5. The Head, the fence and the commit

### 5.1 The Head

The Head is the root of everything a reader may see: it names the metadata tree, so
the tree it names is exactly the Volume at the instant it was written. Its plaintext
is, in order: `Seq` (uvarint), `Root` (32 bytes), `Next` (uvarint), `ID` (16 bytes), `Seed` (32
bytes), `Journal` (uvarint), `Needs` (uvarint), and then the Tail: the fields of later
builds (none today), each `[tag: uvarint][length: uvarint][value]`, with tags strictly
increasing. It is sealed (4.2) and placed in a fence record.

* `Seq` numbers commit attempts. A writer takes the next number for every attempt,
  including one whose reply was lost, so no two Heads a Volume writes share a `Seq`; it
  can skip.
* `Root` is the ref (section 6) of the root shard, or 32 zero bytes for the empty tree.
* `Next` is the next inode number to assign.
* `ID` names the Volume. A new Volume draws it at random.
* `Seed` draws the Volume's chunking table (section 7.2) and keys the boundaries of
  the metadata tree (section 6.3). A new Volume draws it at random. It is as secret as
  the Volume: it is what a party who chooses file names does not know. It never changes: a
  commit and a reseal (5.4) write it back as it was. It cannot be rotated: it keys the
  chunking table, the tree boundaries and the names of extension objects (3.1), so a new
  Seed is a new Volume, written by copying the files into it.
* `Journal` is the highest sequence of the node journal (`docs/journal.md`) whose effects
  the Head includes: what the store holds of it, as the journal's `Published`. It is 0
  when there is no journal. A writer that rewrites the Head and does not publish carries it
  unchanged.
* `Needs` is a set of feature bits: 0 for a Volume that has never had a hard link, bit 2
  (`links`) for one that has. Bit N says that the field of the Tail with tag N
  is there and must be understood. A reader MUST refuse a Head whose `Needs` has a bit it
  does not know, and change nothing (`ErrNeeds`). A writer sets a bit only for a feature it
  implements, and writes the field of that tag with it. This is how a change that every
  writer, or every collector, must honour reaches the Volume without a flag day: it sets a
  bit, appends the field, and a build that lacks the feature refuses instead of damaging the
  Volume. A reader that knows every bit set MUST also refuse a Head that lacks the field of
  a bit it sets (`ErrCorrupt`): a bit that nothing backs is a Head that was not written by
  a writer of the feature. Tags 64 and above have no bit and are never needed. There is no
  class of feature that only writers must know: a bit refuses every build that lacks the
  feature, readers included, even where a reader could have read what a writer cannot
  write (a link count that only unlink maintains locks out an old reader too). A reader
  that is to be let in is a feature that sets no bit.
* The Tail is the fields of later builds, and its form is checked by every reader, whether
  it knows a field or not: every field whole (the tag, the length, and `length` bytes of
  value), tags strictly increasing (so no tag twice, and one order of the fields), at most
  256 fields, and nothing after the last. A reader MUST reject a Tail that is not that
  (`ErrCorrupt`). It does not look inside a value: what a field holds is its feature's. A
  reader MUST skip a field whose tag it does not know, and a writer that rewrites the Head
  MUST write every field back unchanged, byte for byte and in order, including the fields
  it skipped. A field whose bit is not set is ignorable (a hint, a counter nothing depends
  on): it is carried, and never forces a flag day.
* **Reserved tags and bits.** Tag and bit 1 are `snapshots`, 2 are `links` and 3 are `base`
  (the table below) and 63 is an escape; tag and bit 0 are not assigned. `links` is defined
  (below); the others are not. A reserved bit is unusable until the
  encoding of its field is written in this text: an implementation MUST NOT set a bit, and
  MUST NOT write a field of a tag, whose encoding is not in this text. A build that sets
  `Needs` bit 3 with a field of its own invention writes a Head that another build, which
  was given the encoding, cannot read, and was not a conformant writer.
* **Precedence of the refusals.** A reader checks a Head in this order, and the first that
  fails is the error. The fixed fields: one that is short, or not in its shortest form, is
  `ErrCorrupt`. `Needs`: a bit that the build does not know is `ErrNeeds`, whatever the Tail
  holds, even a Tail that is not whole (a build cannot judge the Head of a feature it does
  not have). Then the Tail: not whole, not in order, over 256 fields, or lacking the field
  of a bit it knows is set, is `ErrCorrupt`. So a Head is `ErrNeeds` only if its fixed
  fields are sound, and an `ErrCorrupt` Head is one that no build could read.

The bits below are decided and RESERVED, except `links`, whose encoding follows the table: the
others have names and no encoding, no build sets one, and each is named so that two changes do
not take the same bit or tag.

| Bit and tag | Name | Set when |
|---|---|---|
| 1 | `snapshots` | the Volume has retained roots, stored as extension objects (3.1), that a collector must keep: a collector that does not know the bit would delete what they reach, so a build that does not know it refuses the Volume |
| 2 | `links` | inodes carry link counts that every unlink must adjust (6.1): a writer that does not keep them would leave them wrong. Defined below |
| 3 | `base` | chunks are read through a base layer shared with other Volumes (11.1): the field names the layer, and a shared read key opens its chunks |
| 63 | (escape) | a Head that needs more than 62 features, or that needs one whose field is not a tag: this text will say how, and until it does the bit is unusable, as the others are |

**The `links` field** (bit and tag 2). Its value is one unsigned integer (uvarint), the number
of inodes whose link count is above one at the commit that wrote it. The commit that makes any
inode's link count exceed one sets bit 2 and writes the field, and so does every later commit of
that Volume: the bit is never cleared, even when the count falls to 0. A Volume that has never had
a hard link has neither, and a build that does not know the bit reads it. A writer MUST keep the
count equal to the inodes of the tree that have a link count field (6.1) at every Head it writes,
and the link count of every non-directory inode MUST be the number of directory entries that name
it. A reader that knows the bit MUST refuse (`ErrCorrupt`) a value that is not one integer in its
shortest form with nothing after it.

**The `usage` field** (tag 64, no bit). A field of tag 64 and above is never needed (above), and
this one is a hint that a commit keeps current: two unsigned integers (uvarints), the number of
inodes in the tree (the root, files, directories, symbolic links, pipes and sockets) and the number
of bytes stored for files and symbolic links, which is the sum of the lengths of the inline
records and of the extents (7.1): a hole costs nothing, a chunk that two extents share counts for
each. A Head without the field describes a Volume of the root directory and nothing else. A writer
that does not know the field carries it unchanged and so lets it go stale, which is why it is a
hint; a reader that knows it MUST refuse (`ErrCorrupt`) a value that is not those two integers in
their shortest form with nothing after them.

**The size of the Head.** The `head` object (the fence record of 5.2: 30 bytes of header
and the sealed Head) is at most 65,536 bytes. A reader MUST refuse a longer one, and a writer
MUST NOT write one: a commit that would make the object longer is refused with an error
(`ErrTooLarge` in the reference implementation) before any request is made, and changes
nothing, so no commit that returned success leaves a Volume that cannot be opened. A
Head of the default Cipher is about 1,850 bytes with the header, plus its Tail (the largest
single field value that fits is 63,678 bytes, measured). `Seq` and `Next` grow, so a Head that is
within a few bytes of the bound ends the Volume's writes: a feature keeps its fields small,
and keeps what is large in an extension object (3.1) that its field names.

### 5.2 The fence record

The Head object is `fence-magic || round || token || sealed-head`:

| Field | Size | |
|---|---|---|
| magic | 6 | the bytes `fence` and 0x00 |
| round | 8 | big-endian; the Round of the writer that wrote this object |
| token | 16 | random per writer process; identifies the writer, not the Volume |
| payload | rest | the sealed Head |

The Round is a number the caller supplies and that only grows. The store's conditional
write is what enforces it:

* **Create**: a new Volume writes `head` with `PutIfAbsent` at round 0.
* **Put at round r**: read `head`; if its bytes equal exactly the record this writer
  would write (same round, same token, same payload), the earlier attempt landed and
  the Put succeeds. Otherwise, if `r` is below the recorded round the Put fails with
  "stale round" and the object is untouched; otherwise write with `PutIfVersion` on
  the version just read. On `Conflict`, read again and repeat, at most 8 times.
* **Claim at round r** (taking the Volume over): read `head`; if `r` is below the
  recorded round fail "stale round"; call the hydrate step with the recorded payload;
  write the record with round `r`, the same payload and this writer's token, with
  `PutIfVersion`. On `Conflict`, re-read and repeat, at most 8 times, carrying forward
  whatever the winner wrote. A retry that finds exactly its own previous bytes has
  landed and succeeds.

The Volume is attached at a Round `r` that MUST be strictly greater than the round
recorded, and MUST NOT be 0 or 2^64-1 (no Round could follow the latter). A Volume
opened without a Round is read-only and claims nothing. After any error from an attach
other than a refusal for a Round that is too low, the claim may or may not have
landed (a reply can be lost), and a caller that tries again MUST use a higher Round: a
claim that landed with its reply lost is recognised as the claimant's own only by a
retry inside the same call, which finds its own bytes, and a later call at the same
Round is told "stale round". After a successor has claimed,
no commit by a lower Round can land: its conditional write finds a different version, or
its read finds a higher round.

A store that returns an older Head that it once held cannot be told from the current
one by this protocol. A caller that must notice a rollback keeps the `Seq` of the last
Head it saw and compares.

### 5.3 The commit

A commit publishes a generation: the changes a writer holds, up to one journal sequence, cut
out at an instant. In order:

1. Every file with unsettled writes in the generation is settled: its bytes are cut into
   chunks, the chunks are sealed and appended to a pack, and its records are staged. Packs go to
   the store with `Put` as they fill, in parallel.
2. The staged records are written into new shards, every changed node of the tree and no
   other, leaf to root. Each is written with `Put`, in parallel.
3. The journal is made durable up to the generation's sequence.
4. The Head is written, once, at the writer's Round, with `Seq` the next attempt number, `Root`
   the new root, `Next` the next inode number and `Journal` the generation's sequence. In the
   steady state the write is conditioned on the version the writer's last write left, with no
   read before it; if it loses, the Head is read and the Round that holds it is decided.

Only step 4 changes any object a reader can reach. A crash, or a failure, before it
leaves the previous Head and so the previous tree whole; the chunks, packs and shards
already written are unreferenced garbage, never corruption. Steps 1 and 2 are
idempotent by content address (a pack's name is random: a repeated pack is garbage), so
repeating them is harmless.

The outcome of a failed commit: success means committed. Any other error means the
outcome of that attempt is unknown to the caller (a reply can be lost after the write
landed): the writer reads the Head, and if it carries the generation's tree and sequence the
commit counts as landed. Otherwise the writer keeps its changes and tries again. "Stale round"
means the writer was fenced and can never commit again; if the store's Head then holds exactly
the writer's root and sequence (an earlier attempt landed and a successor carried it forward),
the commit counts as landed.

### 5.4 Moving a Volume

Only the Head is bound to the place (4.2). To move a Volume to another place, copy
every object under its prefix to the new place and then reseal the Head there: read
`head` through the fence, open its sealed payload (below), check that it decodes as a
Head, seal it again under the context of the new place, and write it with the Claim
procedure (5.2) at a Round strictly greater than the one recorded (neither 0 nor
2^64-1). The reseal fences the copy only: a writer still attached at the old place is
not told, so it must be stopped first. A writer that commits without pause can win the
race for the Head eight times, and the reseal then fails with a lost conditional
write. A failed reseal is retried at a higher Round.

Whichever way the payload was opened, the copy is checked before anything is written: the
root shard that the Head names MUST open at the new place under the Volume id the Head
carries and MUST hold the root directory (inode 1). That refuses a copy that lacks its
objects, a Head of another Volume, and an object of another kind that a writer of the store
put where the Head goes. A reseal MAY also be asked for the full check (`--verify`): every
shard the Head reaches opens, every pack that a chunk record names is present and long
enough to hold it, and every chunk that an extent names has a record in the chunk table. The
check reads no chunk and does not follow a chunk record into the pack's bytes: a chunk is
checked against its hash when a file is read.

The payload is opened in one of three ways, tried in this order:

1. under the context of the old place, given as its canonical location string (1);
2. if that fails because the context differs, under the context of the new place: the
   Head is already sealed there, a reseal whose reply was lost for instance. It is read
   like any other (5.1: a bit this build does not know is refused, and the Round is not
   claimed), and the copy is checked; then it is left as it is and only the Round is
   claimed. A reseal can therefore be repeated at
   a higher Round whatever it is told the old place was;
3. if the old place is not given, with the key alone, ignoring the context. This
   trusts the key (only a holder of it can have written a Head that opens, because of
   the tag in 4.1) and trusts that the Head found is the newest of the Volume the
   operator means. It checks one thing more: the root shard that the Head names MUST
   open at the new place under the Volume id the Head carries, which refuses a copy
   that lacks its objects and a Head of another Volume. It cannot tell a Head from
   before the newest commit from the newest, as no one can without the place.

## 6. The metadata tree

All metadata is records in one ordered tree of immutable shards: a Merkle search tree
(a prolly tree). A record is a key and a value, both byte strings; keys sort
bytewise.

### 6.1 Records

| Key | Value |
|---|---|
| `01` `ino` | inode: a sequence of fields, `[tag: uvarint][length: uvarint][value]`, tags strictly ascending |
| `02` `dir` `name` | directory entry: child inode (uvarint), type bits of the mode, as in an inode (uvarint) |
| `03` `ino` | the whole bytes of a small file, inline |
| `03` `ino` `offset` | extent of a large file: chunk id (32 bytes), length (uvarint) |
| `04` `chunk id` | chunk table entry: pack id (16 bytes), offset (uvarint), length (uvarint) |

`ino`, `dir` and `offset` are 8 bytes, big-endian. `name` is the bytes of the name. The
root directory is inode 1. Inode numbers are assigned in increasing order from
`Head.Next` and are never reused within a Volume. A name is 1 to 255 bytes, contains
no `/` and no NUL, and is not `.` or `..`.

An inode's fields: tag 1 mode (uvarint, a 32-bit value, below), 2 size in bytes
(uvarint), 3 mtime, 4 atime, 5 ctime (varint, nanoseconds since the Unix epoch). Tags 1 to 5 MUST be present.
The optional fields are these, and a record has one only when what it says differs from what
its absence says:

| Tag | Field | Value | Absent means |
|---|---|---|---|
| 6 | owner | user, uvarint, 32 bits | the inode belongs to whoever runs the mount (a Volume does not know who) |
| 7 | group | group, uvarint, 32 bits | the same: tags 6 and 7 are both there or both absent, and a record with one is `ErrCorrupt` |
| 8 | link count | uvarint, at least 2, 32 bits | the inode has one name. Not on a directory. Critical: the `links` bit announces it (5.1) |
| 9 | parent | uvarint, the inode that names this directory | the root has none; any other directory MUST have one, and it is never the inode itself and never 0. Only on a directory |

A directory's link count is not stored: it is 2 and one more for each subdirectory, which a reader
counts from the entries of the directory (an entry whose type bits say directory). A reader MUST
refuse (`ErrCorrupt`) a link count of 0 or 1 written as a field, one on a directory, and a parent
on anything but a directory or equal to 0 or to the inode: a record has one spelling. The length of an extent (the table above) and the type bits of a directory entry are 32-bit values too (section 1). A reader MUST check the fields it
knows, MUST reject tags that are not strictly ascending, MUST keep the bytes of fields
it does not know and write them back unchanged when it rewrites the inode, and MUST
NOT fail on them. A new attribute is a new tag. A new kind of record is a new first
byte of the key; a reader MUST ignore records of a table it does not know.

The mode is a 32-bit value whose bits are fixed here, so that a later record never has
to reinterpret one:

| Bits | Meaning |
|---|---|
| 0 to 8 | permissions `rwxrwxrwx` (octal 0777) |
| 20 | sticky |
| 22, 23 | setgid, setuid |
| 31 | directory |
| 27 | symbolic link |
| 26, 25, 24, 21 | device, named pipe, socket, character device |
| 19 | irregular file |
| 28, 29, 30 | reserved |

A regular file has none of the type bits (19, 21, 24 to 27, 31). The type bits of the
mode are the ones listed (19, 21, 24, 25, 26, 27, 31), and a directory entry (6.1)
records those only. A writer MUST NOT set a bit this table does not list; a reader
MUST keep bits it does not understand when it rewrites an inode and MUST NOT act on
them. The reference implementation sets the permissions, the setuid, setgid and sticky bits
(23, 22 and 20), and the type bits of a directory (31), a symbolic link (27), a named pipe (25)
and a socket (24); it makes no device and no irregular file. The setuid and setgid bits mean
what they mean to POSIX only for a mount that enforces them; a Volume stores them.

A symbolic link is an inode with bit 27 and the permissions 0777 whose size is the length of its
target and whose target is the bytes of its inline record (`03 ino`, 7.1), at most 4,095 and at
least 1 byte with no NUL; the Volume never interprets the bytes. A named pipe and a socket are
inodes with their type bit and no data record.

Fields are optional by default: a reader that does not know one keeps it and ignores
its meaning. A field that some writer must maintain for the Volume to stay consistent
is critical: a link count that every unlink must adjust is one (the reserved `links` bit),
an owner is not. A critical field MUST be announced by a bit in `Head.Needs` (5.1). A
reader that does not know the bit refuses the Volume, instead of rewriting inodes behind
the field's back.

### 6.2 Shards and nodes

A node is `[level: uvarint][count: uvarint]` (the level is at most 255, as 6.3 hashes it as one byte) and then `count` pairs of byte strings
`(key, value)`, with `count` at least 1. Level 0 is a leaf: its pairs are records. At a
higher level each pair is the first key of a child and the child's ref (32 bytes), and
the child is at level one below. A shard is a node, sealed (4.2); its ref is the hash of
its sealed bytes and its object is `meta/<2 hex>/<hex of ref>`. The root's ref is
`Head.Root`. A reader MUST verify the hash of every shard it reads and MUST bound the
read (the reference implementation accepts at most 1 MiB, since a node ends at
512 KiB of plaintext by at most one record more).

### 6.3 Where a node ends

A tree is canonical: its shape depends on its records and the Volume's seed alone, so the tree built
incrementally by a sequence of changes is identical, shard for shard, to the tree
built from scratch from the same records. A writer MUST produce the canonical shape.
Entries of one level are taken in key order and cut into nodes: an entry ends its node
if either

* `size = len(key) + len(value) + 4` and `u32le(H[0:4]) * 65536 < size * 2^32`, where
  `H` is the BLAKE3 keyed hash, under the key `K` below, of `level_byte || key`,
  `level_byte` is the level as one byte, and `u32le(H[0:4])` is the first four bytes of
  `H` as a little-endian unsigned integer (65536 is the average node size, 64 KiB); or
* the node's accumulated size (the sum of `size` over its entries) has reached
  512 KiB.

The key is `K = DeriveKey("hanzoai/vfs tree boundaries", Head.Seed)` (32 bytes). It is
secret to the Volume, and that is its purpose: with an unkeyed hash a party who chooses
file names (an archive being unpacked) can choose names that each end their own node,
and 2,000 such names cost 2,007 shards instead of 4. With the key the boundaries fall
where they fall, and the tree is a pure function of the records and the seed: two
writers that hold the Head build the same tree, and a writer that does not hold it
cannot predict where a node will end.

The last node of a level ends there whatever the entries. If a level yields one node,
that node is the root; otherwise its nodes become the entries of the next level up
(first key, ref). The empty tree has no shard and root ref of 32 zero bytes.

### 6.4 The namespace

The entries (`02 dir name`) and the inodes make a namespace, and these rules hold in every tree
a writer commits. A Volume checks no permission: who may do what to which inode is the
mount's, and a mount asks the kernel to decide it from the mode, the owner and the group.

* **Names.** Every directory but the root has exactly one name, in the directory its parent
  field names. A non-directory inode has one or more, and exactly as many as its link count
  (one if it has no link count field). An entry's type bits are those of the inode it names.
  No inode but the root is without a name, except one that a handle holds open after its last
  name was removed (below), which is in no tree. A name that is longer than 255 bytes
  is refused with `ErrNameTooLong`; an inode has at most 65,000 names (`ErrTooManyLinks`).
* **Times.** The operations below set the times of what they change to the Volume's clock at
  the moment of the change. An entry made, removed or renamed in a directory sets that
  directory's modification time and change time (this is what a tool that caches the
  contents of a directory by its modification time relies on); an inode whose link count or
  parent changes, or that is renamed, has its change time set; a write, a truncation and
  a punch set the modification time and the change time of the file; an attribute change sets
  the change time and, where it sets a time, that time exactly, to the nanosecond. Reading
  sets no time: the access time is the one the inode was made with or given.
* **Rename** names the inode of an old entry with a new name in one step: the Head that names
  the tree holds the old entry and not the new one, or the new and not the old, and the same for
  every other effect of the rename (the replaced inode, the link counts, the parents' times).
  A crash, or an error, at any point leaves the tree whole as it was. If the new name is the
  name of the same inode, nothing happens and the call succeeds, including when the two are
  two names of one inode. Otherwise an existing new name is replaced when the inodes are both
  directories and the new one is empty, or neither is a directory, and the replaced inode loses
  that name as `unlink` does; a directory over a file is `ErrNotDir`, a file over a directory
  `ErrIsDir`, a directory over one that has entries `ErrNotEmpty`. A directory moves with
  everything below it by changing its entry and its parent field alone. A directory cannot move
  into itself or below itself (`fs.ErrInvalid`), which a writer finds by following parent fields
  up from the new directory to the root. The precedence of these answers is: the flags, the old
  name is not there, the new name is not there under exchange, the new name exists under
  no-replace, the same inode (which succeeds), the directory loop, the types, the new
  directory's entries. Two flags are honoured, as renameat2(2) defines them:
  no-replace refuses an existing new name (`fs.ErrExist`), and exchange swaps two names that
  both exist (`fs.ErrNotExist` otherwise), each inode taking the other's place, with the same
  loop rule for a directory in either place. The two flags together, or any other, are
  `fs.ErrInvalid`.
* **Link** adds a name for a non-directory inode (`fs.ErrPermission` for a directory) in a
  directory where the name does not exist, raises the link count by one, sets the inode's
  change time, and, the first time any link count exceeds one, announces `links` in the Head
  of the commit (5.1). An inode that was removed while open cannot be linked (`fs.ErrNotExist`).
  A link to a symbolic link names the link itself.
* **Remove** takes one name of a non-directory away, or the name of an empty directory (`ErrNotEmpty`
  otherwise). A non-directory with other names keeps them and its link count falls by one. One
  with no other name is deleted with its data records, unless a handle holds it open.
* **Unlink while open.** A handle that holds an inode open when its last name is removed keeps
  the inode: reads, writes, truncation, punching, the change of attributes and fstat go on
  working through the handle, with a link count of 0, and none of it reaches the tree, which
  holds nothing of the inode from the commit after the removal on. The last close of the
  handle ends it. A process that dies with the handle open leaves nothing: the inode was never
  in a Head after its removal. The inode number is not reused (6.1).
* **Attributes.** A change of the permission, setuid, setgid and sticky bits, the owner and
  group (both at once), the access and modification times, or the size of a regular file
  (7.1) is one call and sets the change time. A new inode takes the group of its directory
  when the directory has the setgid bit, and a new directory also takes the bit.
* **An operation that fails changes nothing.** Every operation reads what it needs before it
  changes anything, so an error, a failed read of the store included, leaves the Volume as it
  was and a later commit holds nothing of the call.
* **Usage and capacity.** A writer keeps the `usage` field (5.1) at every commit, and
  `Config.Capacity` bounds it: a write that would take the bytes stored and the bytes written but
  not yet stored past the capacity is refused with `ErrNoSpace`; an overwrite of bytes that are
  there is not.

## 7. Files

### 7.1 Small files, extents and holes

A file's size is its inode's size and is at most 2^62 bytes. A file of at most 16 KiB
is stored inline: its bytes are the value of its `03 ino` record and nothing else. A
larger file is stored as extents: each record `03 ino offset` says that the file bytes
`[offset, offset+length)` are the chunk with the given id. Extents MUST NOT overlap, and
each `length` MUST equal the length of the chunk's plaintext and be at most 4 MiB. A
byte of the file that no extent covers is a hole and reads as zero; a hole costs no
record, no chunk and no object, however large. Bytes at or beyond the size do not
exist: a read there returns end of file, and a file made larger by truncation reads
zero in the new range. Truncating a file smaller discards extents past the new size
and shortens the one that crosses it; extending it afterwards MUST read zero, never the
discarded bytes. A range of a file can also be punched out (deallocated): its bytes read as
zero, the size does not change, and, once committed, no extent covers it, so it costs nothing
and the file's chunks are cut again around it as around any hole (7.2); a write into it is a
write. A range that reaches past the end of the file is clipped to it.

### 7.2 Cutting chunks

A writer cuts the written bytes of a file into content-defined chunks. Each maximal
run of consecutive written bytes (bytes not separated by a hole) is cut on its own,
starting from its first byte. The cut is FastCDC with normalized chunking (Xia et al.,
USENIX ATC 2016), with these parameters; a reader does not need them, but writers that
agree on them produce the same chunks for the same bytes in the same Volume:

* minimum 64 KiB, average 512 KiB, maximum 4 MiB; `avgBits = 19`.
* The gear table `g[0..255]` is drawn per Volume: the first 2048 bytes of the BLAKE3
  extendable output over the bytes `hanzoai/vfs chunker gear table` followed by
  `Head.Seed`, read as 256 little-endian 64-bit words.
* For a window `b` (the next `min(4 MiB, remaining)` bytes of the run): if `len(b)` is at
  most the minimum, the chunk is all of `b`. Otherwise start with `fp = 0` at index
  `i = 64 KiB`; for `i` below `min(512 KiB, len(b))` set `fp = (fp << 1) + g[b[i]]` and cut
  after byte `i` if `fp & maskS == 0`; then for the rest of the window the same with
  `maskL`. If no cut is found the chunk is the whole window. `maskS` is the top 21 bits
  set (`(2^21 - 1) << 43`) and `maskL` the top 17 bits (`(2^17 - 1) << 47`).
* The chunk id is the hash of the chunk's plaintext. Within a Volume equal ids mean
  equal bytes, which is the whole of dedupe. A Volume never shares chunks with another,
  and ids appear only inside sealed shards.

Because the gear table is per Volume, chunk lengths do not identify a known file
across Volumes.

### 7.3 Packs

A chunk is sealed on its own (4.2, chunk context) and the sealed frames are laid end to
end in a pack, with no header and no index: where each lies is the chunk table (record
`04`). A writer uploads the open pack when it reaches 16 MiB or when the generation it belongs to has no more chunks. A reader
fetches one chunk with a ranged read of its frame, opens it, and MUST check that the
hash of the plaintext is the chunk id.

## 8. What is durable when a call returns

"Journal" is the writer's node-local journal (the journal's own text is `docs/journal.md`): a
change is durable there when the journal's sync for it has returned. "Store" is the Head
advancing: the successful return of its conditional write (5.3). Three watermarks name the
changes of a Volume in order: the last applied (visible), the last durable in the journal,
and the last the Head carries (published, the Head's `Journal` field).

| Call | Durable when it returns |
|---|---|
| write, truncate, punch, create, make directory, remove, rename, link, symbolic link, make pipe or socket, set attributes | nothing: applied in memory and appended to the journal. Visible to every reader at once. Each operation is atomic in a generation: a Head holds all of it or none |
| fsync of a file | the journal, for every change the file depends on (its writes, truncates, punches, and the change that created it). Concurrent callers share one sync. The store is not asked |
| fsync of a directory, or of a Volume | the journal, for every change made before the call |
| close of a file handle | nothing, and no request of the store |
| flush on a mount (every `close(2)`) | nothing, and no request of the store; constant time |
| release on a mount | nothing |
| flush of a Volume | the store, for every change made before the call: it returns after a Head that carries them committed |
| publication by the writer, on its own | the store, within the bound (below) of each change's own acknowledgement while the store answers |
| end of a mount | the kernel's cached writes handed to the writer, then one flush; if the store does not answer in time, what it lacks stays in the journal |
| close of the Volume | the journal; it does not flush and asks the store for nothing |
| attach (takeover on the node that holds the previous owner's journal) | every change the previous owner had made durable in its journal is in the new owner's tree before the call returns |
| attach (takeover elsewhere, or the journal lost) | the last Head only. The changes the previous owner acknowledged and had not published are not available; the writer is told they may be missing |

**The bound.** Every change is published within `Bound` (default 5 s) of its own
acknowledgement, measured from the instant the journal appended it and moved by no later change.
The bound is a promise about a store that answers: when it does not, the writer cannot keep it
and says so (`State`). A node that is lost for good loses at most what the journal held and the
store did not, which the bound limits to the changes of the last `Bound`. A process that ends
loses none that a sync returned for.

**What the kernel holds.** A mount may let the kernel hold a write that was acknowledged to its
caller before it reaches the writer. The bound runs from the change's entry into the journal. As
for any file system, only fsync, and syncfs for the whole mount, make such a write durable, and
the end of a mount asks the kernel for them first.

**Ordering and the Head.** The Head's `Journal` is the highest journal sequence whose effects the
tree includes, and a Head never carries a sequence that is not durable in the journal. In order:
the generation's objects are written, the Head commits, the journal is told the sequence is
published, and its space is reclaimed. A change made after the instant a generation was cut
belongs to the next generation, and a flush returns only after the generation that holds every
change made before it. A takeover loses no change the previous owner saw published, and no
publication by an older Round lands afterwards.

**Backpressure.** The journal has a fixed capacity and a writer has a bound on the changes it
holds that the store lacks. A change that finds either nearly full is slowed; one that finds it
full is refused, changing nothing (`ErrFull`, which a mount answers `ENOSPC`); a journal that has
failed refuses every later change (`ErrJournal`, `EROFS`). The state of the bound is readable:
whether it can be trusted, the age of the oldest change the store lacks, and the bytes of
journal behind it.

On a mounted file system, a signal that reaches a waiting caller makes the kernel
interrupt the request, and no request is answered "interrupted": every call waits for
its own result, and no interrupt cancels a sync or a fetch. (informative) The reason
is what callers do with the answer, measured on Linux: a C caller does not retry
`fsync(2)`, so SQLite reported an I/O error from it (a database over a mount failed
7 runs of 7 on one kernel, and a C caller under a timer saw 246,912 interrupted
fsyncs in 5,000); `stat(2)` and directory reads answered "interrupted" reached C callers
as errors 1,586 times in 60,000 calls without `SA_RESTART` and 2,884 with it; `read(2)`
reached its caller as `EIO`; `close(2)` is never restarted; a restarted create with
`O_EXCL` meets its own file. The cost is in section 11.2.

### 8.1 Specification notes: the journal's place in a writer

A conforming writer MUST:

1. Make every change in this order: build it from what the writer holds in memory, seal it,
   append it to the journal, apply it, with no other change between. It MUST NOT hold any lock
   that a reader needs across a request of the store or a call that waits for the journal's
   disk.
2. Seal what it appends to the journal before it is appended. The key it seals under is of
   that journal's own, sealed by the Volume's Cipher and kept beside the journal.
3. Name the journal by the Volume and the Round (a lineage), and open it with the Head's
   `Journal` as the sequence the store holds, on every mount, including 0. When the journal does
   not fit the Head, or is damaged, it MUST fall back to the Head, MUST report that an
   unpublished tail may be missing, and MUST NOT retry with another sequence. A journal in use by
   another process is a writer still running: the claim has fenced it.
4. Cut a generation under a short lock: everything applied up to one sequence, immutable
   thereafter. It MUST write the generation's objects and commit the Head outside that lock, with
   the Head carrying the sequence of the cut; MUST make the sequence durable in the journal before
   the Head names it; and MUST tell the journal the store has it only after the Head committed.
5. After a Head write whose answer is not known, read the Head back and recognise a Head that
   carries its own tree and sequence as committed; it MUST NOT commit that generation twice, and
   MUST NOT discard changes made after the cut.
6. Refuse a change, changing nothing, when the journal or the bound on memory is full and the
   store is not taking publications; and stop taking changes when the journal has failed.
7. Make a close, and the flush of a descriptor on a mount, free of any request of the store, and
   the fsync of a file wait for the journal and for nothing else.

Acceptance criteria, each testable from outside a store and a clock the test moves:

* a file written and fsynced, the process killed, and the Volume attached again on the same node,
  has every byte, and the store has them when Attach returns;
* a write, fsync, read, close and a flush with nothing to publish make no request of the store;
* with a request of the store held for 2 s (the Head, a shard, a pack, or a read of a chunk),
  stat, lookup, readdir, create, write, fsync and read of other files complete in the time they
  take with nothing held;
* with a clock the test moves and a writer that never stops, no change is absent from the store
  once its age exceeds the bound;
* with the store refusing every write, changes are accepted until the journal or the memory bound
  is full, then refused with `ErrFull`; when the store answers, everything accepted is published in
  order and nothing is lost;
* concurrent writes, flushes and syncs never acknowledge the wrong generation: a flush that
  returns implies every change that completed before it began is in the store;
* a Head whose write landed and whose answer was lost is counted once, and later changes are kept;
* a takeover during a publication refuses the old owner's Head, and the new owner has the last
  Head the store acknowledged and no part of the generation in flight.

## 9. Limits

| What | Limit |
|---|---|
| file size | 2^62 bytes (4 EiB) |
| inline file | at most 16 KiB |
| chunk | 64 KiB to 4 MiB (a short final chunk of a run is allowed) |
| pack | uploaded at 16 MiB; a pack may exceed that by one frame |
| node (shard plaintext) | ends at 512 KiB; sealed shard read bound 1 MiB |
| `head` object (fence record: 30 bytes and the sealed Head) | 65,536 bytes, read and written |
| Head Tail | at most 256 fields |
| name | 1 to 255 bytes, no `/`, no NUL, not `.` or `..` |
| symbolic link target | 1 to 4,095 bytes, no NUL |
| names of one inode | 65,000 |
| directories nested | no bound but a rename that must follow parents up stops at 65,536 levels (`ErrCorrupt`) |
| prefix | at most 512 bytes, segments as in section 2 |
| round | 1 to 2^64-2 for a writer |
| fence retries | at most 8 compare-and-set attempts per Put or Claim |
| key id | 1 to 255 bytes |
| sealing context | exactly 32 bytes |
| default Cipher tag | 32 bytes at the end of the plaintext |

## 10. Conformance

A reader or writer is conformant when it passes the equivalent of these tests of the
reference implementation (all in the repository root unless a package is named):

| Property | Tests |
|---|---|
| every byte of the layout | `TestSpecContexts`, `TestSpecFrameAndTag`, `TestSpecHead`, `TestSpecFenceRecord`, `TestSpecRecords`, `TestSpecNodeAndBoundary`, `TestSpecChunker`, `TestSpecExtension` (each rebuilds the bytes from this text and compares), `TestObjectKeysAreTheSpecShapes`, `TestFenceTokenIsSixteenBytes`, `TestTheSizesOfTheText`, and `TestTheCipherOpensAGoldenObject`, which holds the default Cipher's format to an object that the library produced once |
| the Head: its fields, the journal sequence, the bits, the tagged Tail and what a later build appends | `TestHeadRejectsGarbage`, `TestHeadNeeds`, `TestHeadTailRules`, `TestNeededFieldsMustBePresent`, `TestTailSurvivesAnOrdinaryWriter`, `TestHeadMatrix`, `TestHeadParserFuzz`, `TestHeadDifferentialFuzz`, `TestHeadDifferentialWithKnownBits`, `TestAKnownBitIsHeldToItsField`, `TestHeadProduct`, `TestOneSpellingPerHead`, `TestTailThroughEveryDoor`, `TestNeedsAndTailAreSealed`, `TestCommitCarriesNeedsAndTail`, `TestTailOrderIsAgainstThePreviousField`, `FuzzHead`, `TestWriterNeverWritesAHeadItsReadersRefuse`, `TestStoreNeverWritesWhatItWouldNotRead` (`pkg/fence`), `TestTheHeadHasRoomForTheJournalSequence`, `TestHeadFieldsSurviveACommit`, `TestHeadOfALaterBuildIsRefusedByEveryDoor` |
| objects are written once; crash at any point leaves the last commit | `TestSigkill` (six kill points, every run), `TestFailureAtEveryWrite`, `TestLostAcknowledgementOfHead`, `TestKillOverRealServer` |
| fence: one winner, older Round refused, takeover loses nothing, lost acknowledgements | `pkg/fence` tests, `TestFenceRefusesOlderRound`, `TestAttachNeedsAHigherRound`, `TestTakeoverLosesNoAcknowledgedCommit`, `TestRacingClaims`, `TestSameRoundRace`, `TestAttachLostAckIsNotStale`, `TestAttachAfterALostClaimNeedsAHigherRound`, `TestRoundMaxIsRefused`, `TestHeadSeqNeverRepeats` |
| store contract, the key grammar, listing by string prefix with sizes and times | `internal/storetest` conformance suite, run on every backend (the memory store, the directory, an S3 protocol server and the real server); `TestRealServerCAS`, `TestRealServerRaceStress`, `TestValidKey`, `TestListTimesAndPages` |
| extension objects: one behaviour on every store, sealed to Volume, name and key, left alone | `TestExtensionObjectsOnEveryStore`, `TestExtensionObjectsAndTheFence`, `TestExtensionObjectsRaced`, `TestExtensionObjectIsSealedToItsVolumeNameAndKey`, `TestExtensionReplaceRetriesAndGivesUp`, `TestStrayKeysAreIgnoredAndKept`, `TestExtNameRevealsTheKey`, `TestExtNamesMatchAnIndependentBLAKE3` (vectors from another BLAKE3 library, `testdata/extnames.json`), `TestSeedDerivationsAreSeparate`, `TestExtNamesFollowTheVolume`, `TestExtKeyRefusesWhatPutExtRefuses`, `TestExtNamesAndCollisions`, `TestExtMasquerade`, `TestExtReadIsBounded`, `TestStrayKeys` |
| sealing context, exact and injective | `TestSealedToItsContext`, `TestHeadContextIsExact`, `TestContextsDoNotCollide`, `TestSwappedHeadIsRefused`, `TestChunkHashIsChecked` |
| the Cipher: checked at configuration and at every Seal; unforgeable with the recipient alone | `TestIdentityCipherIsRefused`, `TestCipherThatStopsSealingIsCaughtAtEverySeal`, `TestSealCatchesAHeaderedFallback`, `TestNoPublicTagKeyVerifies`, `TestShortForgedPlaintextIsRefusedNotPanicked`, `TestContextIsComparedWhole`, `TestRecipientAloneCannotForge`, `TestEveryByteOfTheTagIsChecked`, `TestWrongKeyFailsClosed`, `TestTamperedObjectsAreRefused`, `TestSwappedChunkIsRefused` |
| places and moving a Volume | `TestPlaceSpellings`, `TestPlaceStringsAreOneSpelling`, `TestPlacesAreRefusedNotCleaned`, `TestRedact`, `TestRedactionCases`, `TestRedactionLeaks`, `TestRedactionOfEverySpelling`, `TestErrorsDoNotEchoACredentialInAPrefix`, `TestResealMovesAVolume`, `TestResealWrongFromChangesNothing`, `TestResealCrashAtTheWrite`, `TestResealIsRepeatable`, `TestResealWithoutThePlace`, `TestResealOfALiveVolume`, `TestResealRacesAWriter`, `TestResealRefusesTheLastRound`, `TestResealRefusesAGarbageHead`, `TestResealChecksTheCopy`, `TestResealWrongKeyChangesNothing`, `TestResealWithoutAPlaceRefusesAnotherKindOfObject`, `TestPartialCopyFailsClosed`, `TestVerifyFindsAnExtentWithoutItsChunk`, `TestResealOfAlreadySealedHeadChangesNothingWhenItNeedsMore`, `TestVerifyChecksTheFurthestChunk`, `TestResealOfShortCopies`, `TestResealRepeatsAndForeigners`, `TestCLIReseal` (`cmd/vfs`), `TestFileOpenCreatesNothing` (`pkg/backend/file`) |
| shortest-form integers and fields no wider than their width | `TestHeadNonMinimalUvarint`, `TestIntegersAreInTheirShortestFormAndFitTheirField` |
| keyed tree boundaries | `TestGroundNamesDoNotBloatTheTree`, `TestBoundariesDependOnTheSeed` |
| rotation | `TestRotation` |
| records and the tagged inode | `TestInodeRecordIsExtensible`, `TestHeadRejectsGarbage` |
| canonical tree | `TestTreeMatchesModel`, `TestTreeForcedCuts` |
| chunking | `TestCutBounds`, `TestCutStableUnderEdit`, `TestGearTableIsFixed`, `TestGearDiffersPerVolume`, `TestChunkListIsTheWholeFileCut` |
| holes and truncation, byte for byte against a plain model | `TestSparseExact`, `TestSparseRandom`, `TestSparsePetabyte`, `TestModelFuzz` |
| bounded reads | `TestHeadBound`, `TestShardBound`, `TestBackendBodyIsBounded`, `TestReadIsBounded` |
| prefixes | `TestValidPrefix`, `TestPrefixConfinement` |
| durability and interrupts on a mount | `TestFUSEFsyncSurvivesKill`, `TestFUSEInterruptsAreNotErrors`, `TestFsyncWaitsForItsCommit`, `TestCloseWaitsForItsCommit`, `TestReadWaitsForItsData`, `TestLookupIsNotAnsweredEINTR`, `TestAttrIsNotAnsweredEINTR`, `TestReaddirIsNotAnsweredEINTR`, `TestMkdirIsNotAnsweredEINTR`, `TestRemoveIsNotAnsweredEINTR`, `TestCreateIsNotAnsweredEINTR`, `TestEveryHandlerOutlivesItsRequest`, `TestFsyncCallersShareTwoCommits`, `TestFsyncJoinsAFlightThatCoversIt`, `TestKilledColdLookupIsFreedAfterTheGrace`, `TestErrnoMapping`, `TestFinishCommitsWhatTheMountWrote` (the flush at the end of a mount), `TestFinishDoesNotWaitForAStalledCommitForever`, `TestFinishDoesNotClaimALostCommitIsLost`, `TestFinishReportsTheLostCommitEvenWhenServeFailed`, `TestFinishWaitsForASlowCommit`, `TestFUSETermStopsAMountInUse`, `TestFUSEReadsExistingFilesOverS3`, and `TestFUSESQLite` (`internal/sqlitecheck`) on a Linux kernel |
| the namespace: rename and its flags, loops, replacement, hard links, symbolic links, pipes and sockets, attributes, times, unlink while open | `TestRenameMovesAFile`, `TestRenameTimes`, `TestRenameReplaces`, `TestRenameOfTwoNamesOfOneInodeIsNothing`, `TestRenameFlags`, `TestRenameDirectoryNeverBecomesItsOwnAncestor`, `TestRenameOverAHardLinkKeepsTheOtherName`, `TestRenameOverAnOpenFile`, `TestDirectoryLinkCounts`, `TestHardLinks`, `TestSymlinks`, `TestMknod`, `TestSetAttr`, `TestOwnerAndGroupInheritance`, `TestDirectoryTimesFollowTheirEntries`, `TestUnlinkedFileIsWholeForItsHandle`, `TestUnlinkedFileWithAnotherNameStaysNamed`, `TestRemovedFileStaysOpenForItsHandle`, `TestNameLimits`, `TestReadOnlyVolumeRefusesEveryNewOperation` |
| an operation is all or nothing; a rename is atomic across a crash; a link count never lies | `TestAnOperationThatFailsChangesNothing` (the store's nth read fails, for every n), `TestFailureAtEveryWrite` and `TestSigkill` (the states they move between hold renames, links and symbolic links, and every state read back is checked by `Volume.Check`), `TestModelPosix` and `TestModelPosixMany` (random operations against a plain model of a tree: names, inode numbers, types, modes, owners, link counts, bytes and times, across Flush, reopen and takeover) |
| the `links` and `usage` fields, the inode's owner, link count and parent | `TestSpecInodeAttributes`, `TestSpecLinksAndUsageFields`, `TestHeadAnnouncesLinksOnlyAfterTheFirstLink`, `TestLinksFieldIsHeldToItsBit`, `TestUsageFieldIsCheckedWhenRead`, `TestTailSetKeepsEveryOtherFieldInOrder`, `TestUsage`, `TestCapacity`, `TestHeadNeeds` |
| punched holes | `TestPunchedRangeCostsNothing`, and the punch step of `TestModelPosix` |
| the mount on a kernel: rename, links, attributes, unlink while open, fallocate, statfs, locks, mmap, pipes and sockets, `.` and `..` | `TestFUSERename`, `TestFUSELinks`, `TestFUSEAttributes`, `TestFUSEUnlinkWhileOpen`, `TestFUSEFallocate`, `TestFUSEStatfs`, `TestFUSELocks`, `TestFUSEMmap`, `TestFUSESpecialFiles`, `TestFUSEReaddirDots` |
| full-size randomised runs | `TestSparseExact`, `TestSparseRandom`, `TestSparseCanonicalRuns`, `TestSparseTinyBounds` (the `sparse` gate, not `-short`) |

## 11. Not specified, and known limits

Extended attributes and device nodes have no records; a mount answers them "not supported".
There is no garbage collection of unreferenced objects (11.1 says how it will be done), no
quota beyond the capacity of 6.4, and no snapshot or clone. Retries and back-off are the
caller's. Sealed sizes depend on the Cipher.

### 11.1 Collection (decided, not built)

A collector runs online, against live writers, and is mark and sweep. It is built on these
rules, and a change to a rule is a change to this text.

* **Mark.** From the Head's `Root` and from every retained root. A Volume with the
  `snapshots` bit (5.1) has retained roots as extension objects (3.1), and a collector that
  does not know the bit refuses the Volume, which is why the bit exists. The mark follows
  every shard to its children and every chunk record to its pack.
* **Sweep.** It deletes an object only if it is under `meta/` or `pack/` with exactly the
  shape of section 3, it is not marked, and it is older than the grace below. It deletes
  nothing under a key it does not know, whatever it is (3.1).
* **Age is the store's.** An unmarked object may be one a writer has uploaded for a commit
  that has not landed, and pack names are random, so nothing but time tells it from garbage.
  Age is measured from the object's modification time as the store reports it in `List`
  (`Object.Modified`), against the store's own time, which a collector learns by writing an
  object of its own and listing it: never against a writer's clock or the collector's. The
  reference interface lists keys, sizes and modification times. A store's time has a
  granularity: S3 reports `LastModified` in whole seconds, truncated down, so an object
  written at 12.9 s is listed at 12 s and looks up to a second older than it is, and the
  grace below MUST be that long and one second more: an age that looks a second older than
  it is deletes an object of a commit that has not landed. The conformance suite checks that every store it runs on
  lists a time within 2 s of the write.
* **Grace.** Longer than the longest a commit can take, which is the longest time between
  the first object of a commit being written and the Head naming it. That is bounded only
  when every store request has a deadline (11.2), so no collector is built before them: a
  writer ends a commit within the deadlines of its requests, and the grace is more than
  twice their sum. A collector marks from the Head it reads after the listing, not before,
  so a commit that landed during the listing is marked.
* **Volumes never share chunks**, and stay so until a build that sets the `base` bit (5.1)
  exists: a base layer shared across Volumes needs the field of its tag (5.1) that names it and
  a read key that opens its chunks for every Volume that uses it, and a collector of one
  Volume never deletes what another can reach. Until then every object of a Volume is
  reachable from that Volume's roots alone, and a prefix does not nest in another (2).
* **`Open` pins nothing.** A reader holds the Head it read and nothing in the store says so,
  so the grace is the only thing that keeps its shards and packs: a reader that stays on
  one Head for longer than the grace, while the Volume goes on committing, finds that a shard
  it needs is gone, and sees `ErrCorrupt` (the object is absent and the Head says it must be
  there), not a "stale" error that a reader could tell from damage. A reader that lives
  long re-reads the Head before the grace ends; a collector's grace is therefore a limit on
  how long a Head may be held, and it is published with the collector.

### 11.2 Known limits

Known limits of the reference implementation. None changes the layout.

* **A stalled store does not stall the callers that do not need it, and a request has no
  deadline.** The writer holds no lock across a request of the store or a call that waits for the
  journal's disk, so with a request held (the Head, a shard, a pack, a read of a chunk) a stat,
  a lookup, a readdir, a create, a write, an fsync and a read of other files complete as usual.
  What needs the store itself still waits for it: a read of a chunk or a shard that is not held,
  and a flush. No request has a deadline, no call is answered "interrupted" (section 8), and a
  killed caller that waits on the store stays in the kernel until the store answers, except:
  * a cold lookup, stat and readdir of a name not yet read: freed in about 3 s, the grace after
    which an interrupted request that changes nothing is answered "interrupted";
  * killing the daemon frees every caller at once (the connection ends, 20 ms) and loses what
    the journal on that node does not hold.
  Memory: the FUSE library holds about 134 KiB for each abandoned request until its handler
  returns. Only answering the abandoned requests frees them, which a deadline on the store does.
  The publisher has no deadline either: a publication blocked on a request that is never answered
  stays blocked until the writer is closed, and the state says so.
* **Before a tenant is given a mount**, the supervisor must: watch the store's latency and
  kill the daemon when it does not answer (callers are freed in 20 ms; what the daemon had
  not committed is lost); set `pids.max` and a memory limit for the mount's callers and for
  the daemon (about 134 KiB for each stuck request); and unmount lazily at teardown
  (`fusermount3 -u -z`), because a mount in use refuses a plain unmount.
* **SIGTERM** has the kernel hand its cached writes to the daemon (syncfs) and publishes what the
  daemon holds, asks for the unmount, and if something holds the mount open says so, asks again
  for 5 s and then detaches it lazily and ends within 3 s more. What a holder writes after is in
  the journal and not in the store. The publication at the end has a deadline of 3 s: when the
  store has stopped answering, the daemon says that the last publication did not finish and that
  what it lacks is in this node's journal, and exits non-zero. With a stalled store the whole of
  it takes up to 21 s. A supervisor that kills at once loses nothing the journal holds: the next
  mount on the node publishes it; only a lost node loses it.
* **The unpublished tail lives on one disk.** What the store has not taken is on this node's
  journal and nowhere else. A journal is never restored from a backup or copied between nodes.
  A node lost for good loses the changes of the last bound, and a takeover elsewhere says so.
* **A burst larger than memory waits for the store.** A writer holds the changes the store lacks
  in memory (`Config.Dirty`, 1 GiB by default) as well as in the journal; past that a change waits
  for a publication, so a burst bigger than the memory bound runs at the speed the store takes it.
* **Closes no longer commit** (section 8): a close and a flush on a mount ask the store for
  nothing, and a generation holds every change of the last few seconds, so a tree of many
  small files costs a fraction of an object per file (measured 0.15 objects per file for 82,000
  files of 1 to 16 KiB).
* **What the mount cannot do, because of the FUSE library it is built on** (the library has
  neither request, and the engine is complete without them):
  * the flags of `renameat2(2)` are answered `EINVAL` by the kernel (the Volume honours them:
    6.4); a plain rename is whole;
  * `O_TMPFILE` is answered `EOPNOTSUPP`; a program that wants an unnamed file falls back to
    a named one that it removes, which an open handle survives (6.4);
  * the sticky bit is stored and returned by the Volume, but the library leaves it out of the
    attributes it answers the kernel with, so `stat` on a mount never shows it;
  * extended attributes (`user.*`, `security.*`, ACLs) are answered `ENOTSUP`, and a device node is
    refused with `EPERM`;
  * advisory locks, `flock` and `fcntl`, are the kernel's own, per mount and in memory: they
    exclude the processes of one mount and nothing else, and are gone with the mount.
* **Usage is a hint, capacity is a bound on a count.** The `usage` field (5.1) is as current as
  the writers that kept it; the capacity refuses a write on the bytes this Volume knows of. There
  is no collection, and a Volume only grows (11.1).
* **Tenants must not hold the store's write credentials.** The Round in the fence header
  is not authenticated: it is checked against the recorded Round by conditional writes, not
  by the Cipher, and whoever can write the store can set it, as they can delete the Head.
  The store is trusted with availability and with ordering, not with confidentiality.
* **Metadata grows with the number of chunks** of a file: a chunk table is a record of
  the tree, so the shards a file costs grow in proportion to its chunks.
* **A chunk is read whole to be authenticated.** Its tag covers all of it, so a read of
  a few bytes fetches the chunk; sub-chunk reads cannot be added without a different
  sealing.
* **A retired key stays trusted** for as long as it is in the Cipher: it opens, and
  its holder can seal. Dropping it makes what only it sealed unreadable (rotation).
* **Keyed boundaries stop a guest, not an observer.** The boundaries are keyed from the
  Volume's seed so that a guest who picks names cannot grind them to bloat the tree
  (6.3). They do not hide from an observer of the store how many shards each commit
  writes.
* **A reseal can lose the race** against a writer that commits without pause (5.4);
  stop the writer first.
* The `replica/` package keeps a SQLite replica current by a ticker; it is not part
  of this layout and is not specified.

## 12. Specification notes: the POSIX operations

What a third party needs to implement the operations of 6.4 and to test the implementation. Each
criterion is stated so that a program can check it; the test that does it for the reference
implementation is named in section 10. HIP-1342 (the draft HIP for this layout) is the home of
these notes.

**Layout.**

1. An inode record carries the owner (tag 6) and the group (tag 7) together or not at all, a link
   count (tag 8, at least 2, only on a non-directory) and a parent (tag 9, only on a directory
   other than the root, never 0 and never itself); the encodings are those of 6.1, in ascending
   tag order, each integer in its shortest form. A decoder rejects every other spelling.
2. The mode bits are the ones of 6.1: setuid 23, setgid 22, sticky 20, symbolic link 27, directory
   31, named pipe 25, socket 24. A directory entry records the type bits of its inode.
3. A symbolic link's target is the inline data record of its inode; its size is the target's
   length.
4. The `links` field (tag 2, bit 2) is one uvarint: the number of inodes whose link count is above
   one. It is set by the commit that first makes one, and never cleared. A Volume without a hard
   link has a Head that a build that does not know bit 2 reads; one with a hard link is refused by
   that build (`ErrNeeds`) and read by a build that knows the bit.
5. The `usage` field (tag 64) is two uvarints, inodes and bytes stored. After a commit of a
   Volume that only builds that keep the field have written, its values are the counts of the
   tree: the number of inode records, and the sum of the lengths of the inline records and the
   extents.

**Behaviour.** For each, "the answer" is the error named, and "nothing" means that the tree, the
counters and every inode are as they were.

6. Rename of a name to a name of the same inode answers nothing. Rename over a file removes the
   replaced inode's name; if that was its last name and no handle holds it, the inode and its
   data are gone from the next commit.
7. Rename of a directory below itself, or into itself, answers `ErrInvalid` and does nothing;
   so does an exchange that would put either directory below the other.
8. A rename is atomic: for every point at which a writer may be stopped or a request may fail
   (every write to the store, every read), what a reader opening the Volume then sees is the tree
   before the rename or the tree after it, entry for entry, inode for inode, link count for link
   count.
9. After any sequence of operations and a commit, the link count of every non-directory inode is
   the number of entries that name it; every directory has one entry, whose directory is the
   parent field; the count of a directory is 2 and its subdirectories; the usage and links fields
   are the counts of the tree.
10. An entry made, removed or renamed in a directory moves that directory's modification time and
    change time forward; a write to a file inside does not. A chmod moves the file's change time and
    not its modification time. A time that is set is read back to the nanosecond, across a commit
    and a reopen.
11. A handle on an inode whose last name is removed reads what it held and writes, truncates and
    stats (with a link count of 0) until its close; the next commit and a reopen show no such
    inode; Link of it answers `fs.ErrNotExist`.
12. Punching a range reads as zero over it and leaves the size; committing it stores no chunk for
    it and lowers the bytes stored by the extents it covered; a write into it is read back.
13. A change of owner needs both halves for an inode that has none (`ErrInvalid`); a new inode in a
    directory with the setgid bit has the directory's group, and a new directory the bit too.
14. A write past `Config.Capacity` answers `ErrNoSpace` and does nothing; an overwrite of existing
    bytes is allowed at the capacity; truncation to zero makes room.
15. A mount made with `default_permissions` shows the mode, owner, group and link count of each
    inode in `stat(2)`, and the kernel, not the Volume, refuses what the mode does not allow.

**References.** POSIX.1-2017 `rename`, `link`, `symlink`, `unlink`, `rmdir`, `utimensat`,
`fchmod`, `fchown`, `ftruncate`, `statvfs`; Linux `renameat2(2)`, `fallocate(2)`, `flock(2)`,
`fcntl(2)` open file description locks, `fuse(4)` with `default_permissions`.
