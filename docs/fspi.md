# End-to-end object integrity: read the pack, not just its name (FS-PI, bounty #10)

A Volume keeps three kinds of object in its store, and they are not equally protected. A **shard**
is named after the hash of its own sealed bytes, so any change to it is a name that no longer
matches, and every reader that walks the namespace pays that check for free (`shard.go:120-131`).
A **pack** is named by 128 random bits the writer chose before the pack was final, and its own
comment says what follows: *"Integrity does not rest on the name: every chunk is checked against
its own hash when read"* (`pack.go:22-24`). That check happens when a file is read. Nothing in the
package reads a pack to ask whether it is whole.

This change adds the reader that asks. `(*Volume).Verify` walks the Head, opens every shard,
decodes every record, and reads **every pack once**, unsealing each frame and comparing
`sumChunk(plaintext)` with the chunk id the extent names. It reports every fault it finds, with
the object, the offset and the chunk, and it writes nothing.

The thesis, in one line: **for a shard the name is the proof; for a pack only the bytes are**, so a
store can be intact by every cheap check and still hold a chunk no one can ever read again.

## 1. Primary reference, summary, and license audit

- Paper: **"Advancing Data Integrity in Linux"**, USENIX FAST '26 —
  <https://www.usenix.org/conference/fast26/presentation/gupta>
- Summary, as the authors state it (page fetched 2026-11-26; abstract quoted, not paraphrased):
  hardware-only or software-only methods both fall short, so end-to-end data protection carries
  per-block protection information (PI) through the whole I/O stack; Linux under-uses the PI that
  devices have supported for a decade. The paper closes two kernel gaps — an `io_uring` interface
  that exchanges integrity metadata with the data, and flexible PI placement in the block layer —
  and proposes **FS-PI**, where the filesystem generates and verifies the metadata itself. It is
  implemented in XFS (native data checksumming) and BTRFS (a lightweight path in place of the
  checksum tree).
- **The paper's own numbers are the authors'** (`26 %` BTRFS throughput, reduced host CPU). They
  are neither reproduced nor assumed here: on this machine the hardware half is `NO_MEDIDO`.
  What this document measures is the *software-only* half of the same question, over the object
  layer a Volume already has.
- **License audit.** The presentation page links the PDF and the call for artifacts; it publishes
  **no source artifact**, so there is nothing to copy and nothing to attribute beyond the citation
  above. This change is clean-room against `vfs`'s own formats (`cipher.go`, `shard.go`,
  `pack.go`, `chunk.go`) and adds no dependency: the BLAKE3 it uses is the repository's own
  `internal/blake3`, the same one that names a shard.

## 2. The baseline: three things a store can already be asked

| reader | what it does | what it costs on a clean store |
|---|---|---|
| `Check` (`check.go`) | walks the namespace, reads every shard it reaches, hashes each against its name | reads the shards, never a pack |
| `Reseal{Verify:true}` → `checkTree(full)` (`verify.go:20`) | the same walk, plus `Stat` of every pack a chunk points into, for size and presence | the shards, plus one `Stat` per pack |
| a full read of every file | unseals every chunk through the read path | every byte, through the caches |

The gap: `Check` and `checkTree(full)` both verify the objects whose name is their hash, and both
only *look at* the objects whose name is not. A disk that flips one bit inside a frame keeps the
pack's length and its name, and no file may ever read that chunk again — a cold file, an extent
whose file was deleted, a chunk of a snapshot nothing is walking. That is the window this closes.

## 3. Architecture (`integrity.go`, 272 lines)

One exported call, one report type, no new state:

```go
r, err := v.Verify(ctx, vfs.VerifyOptions{All: true}) // All: keep going after a fault
```

- **Walk.** From `h.root`, `shards.fetch`-equivalent reads, but through the store's `Get`
  directly rather than the Volume's cache, so a shard or pack that changed underneath a running
  writer is read as it is now and not as it was remembered.
- **Collect, then read packs once.** The walk gathers `(chunkID, where)` for every extent and
  every record of the tree, deduplicated by chunk id. Jobs are sorted by `pack`, and each pack is
  fetched once — one `Get` of at most `packTarget + packRoom` (16 MiB + 64 KiB) — and each frame
  inside it is unsealed with `chunkAAD(volume, chunk)` and hashed.
- **Report.** `VerifyReport` counts shards, packs, chunks, inline records and bytes, and keeps a
  `[]Fault` of `{Kind, Key, Chunk, Off, N, Err}`. `OK()` is `len(Damaged) == 0`; `Err()` wraps
  `ErrCorrupt` around the first fault so a caller can tell "corrupt" from "the store failed".
  Nine kinds: `ShardMissing`, `ShardName`, `ShardSeal`, `ShardShape`, `ChunkTable`, `PackMissing`,
  `PackShort`, `ChunkSeal`, `ChunkHash`.
- **It is a reader.** It never calls `Put`, and `TestVerifyClean` asserts it: the store's objects
  are byte-identical after a full pass.

## 4. Hypotheses, and what became of them

**H1 — coverage.** *A bit flipped inside a pack is invisible to `Check` and to
`Reseal{Verify:true}`, and a pass that reads the chunks sees it; a flipped shard is caught by
every reader, because the shard's name is the hash of its own bytes.*
Metric: objects caught / objects damaged, per class, per reader.
Falsified by: any reachable pack mutation caught by `check`, or missed by `verify-all`.
Result: **confirmed**, on two different store layouts (§5). Packs: `0/2` for the cheap readers,
`2/2` for the deep ones. Shards: `n/n` for all five readers.

**H2 — cost.** *The deep pass costs about one read of every byte the Volume reaches — near 100 %
of the store — while the cheap walk costs about 1 %.*
Metric: bytes read / store bytes, and store requests.
Falsified by: the deep pass reading < 90 % of the store, or the cheap one > 5 %.
Result: **confirmed**: `99.7 %` vs `1.1 %` (§5c). The cheap walk is cheap because a shard is
small and few; the deep pass is expensive because a pack is 16 MiB and there is one per
publication.

**H3 — freshness, a declared negative.** *A Head that is authentic but older than the state the
caller meant opens, walks and verifies; no reader that sees only the store can call it damage.*
Metric: caught for the `head-rollback` class.
Result: **`0/1` for every in-band reader, `1/1` only for the reader that is given the expected
file set from outside.** `Verify` proves integrity, not freshness; the seal names the Volume and
the place, never a moment. What the caller has instead is the `Round` its store's conditional
write already enforces (`fence`), which is the only clock in the system that is not in-band.

## 5. Measurements

### 5a. The store's shape is not reproducible, and every row says which store it saw

Two runs of the same command on the same commit produced different layouts: the journal decides
how many shards one publication writes, and that depends on when the publisher ran relative to
the writes.

| run | file | objects | shards | packs | store bytes | reachable | rows |
|---|---|---:|---:|---:|---:|---:|---:|
| A | `docs/fspi.csv` | 13 | 10 | 2 | 19 597 272 | 8 of 12 | 115 |
| B | `docs/fspi-layout-b.csv` | 12 | 9 | 2 | 19 695 354 | 7 of 11 | 105 |

Both are published, because *the pattern is the claim and the count is not*: the layout changes
and every ratio below holds. Each CSV row carries `store_objects`, `store_shards`, `store_packs`,
`reachable_shards`, `reachable_packs`, `store_bytes`, so a row can be read without the prose.
`reachable` is **measured, not assumed**: the counter records every key a reader asked the store
for on a clean Volume, and an object no reader ever asks for is superseded — damaging it is
correctly undetectable by anybody, so it is excluded from the denominators and reported instead
(4 of 12, 4 of 11).

### 5b. Detection — objects caught / objects damaged (run A; run B is identical in shape)

| damage | objects | `check` | `reseal-verify` | `verify-shallow` | `verify-all` | `read-all` |
|---|---:|---:|---:|---:|---:|---:|
| `shard-bitflip` | 6 | 6/6 | 6/6 | 6/6 | 6/6 | 6/6 |
| `shard-missing` | 6 | 6/6 | 6/6 | 6/6 | 6/6 | 6/6 |
| `pack-bitflip` | 2 | **0/2** | **0/2** | 2/2 | 2/2 | 2/2 |
| `pack-truncated` | 2 | **0/2** | 2/2 | 2/2 | 2/2 | 2/2 |
| `pack-missing` | 2 | **0/2** | 2/2 | 2/2 | 2/2 | 2/2 |
| `head-bitflip` | 1 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 |
| `head-rollback` | 1 | 0/1 | 0/1 | 0/1 | 0/1 | **1/1** |

Run B: `shard-*` 5/5, `pack-bitflip` 0/2 · 0/2 · 2/2 · 2/2 · 2/2, `pack-truncated` and
`pack-missing` 0/2 · 2/2 · 2/2 · 2/2 · 2/2, `head-bitflip` 1/1, `head-rollback` 0/1 … 1/1.
Same verdicts, different denominators: the reader's coverage is a property of the code path, not
of how many objects the journal happened to write.

- `head-bitflip` is caught by all five for the same trivial reason — no reader can say anything
  before it opens the Head — so it is a control, not evidence for any of them.
- The two that catch a truncation and a missing pack (`reseal-verify`) do it by **size and
  presence**, which is exactly the check that a flipped bit inside a frame defeats: same length,
  same name.
- `verify-shallow` (`All:false`) stops at the first fault, so its byte count in the damaged runs
  is smaller (§5c shows the clean-store cost of both modes, which is the same).

### 5c. Overhead — a store nothing has damaged, 19 597 272 bytes of objects (run A)

| reader | bytes read | of store | requests | stats | ms (median, 3 runs) |
|---|---:|---:|---:|---:|---:|
| `check` | 225 161 | 1.1 % | 7 | 0 | 6.24 [5.97–7.90] |
| `reseal-verify` | 225 161 | 1.1 % | 7 | 2 | 12.96 [7.62–17.85] |
| `verify-shallow` | 19 538 121 | 99.7 % | 9 | 0 | 285.12 [232.65–506.26] |
| `verify-all` | 19 538 121 | 99.7 % | 9 | 0 | 239.79 [224.21–417.46] |
| `read-all` | 19 538 121 | 99.7 % | 87 | 0 | 192.27 [175.57–200.89] |

Run B: `1.1 %` and `99.2 %` of 19 695 354 bytes; `check` 6 requests, `reseal-verify` 6 + 2 stats,
`verify-*` 8 requests, `read-all` 90.

- **Bytes and requests are exact** — the counter sits between the Volume and the store and tallies
  what was asked for and what came back.
- **Milliseconds are indicative only.** This is an in-memory store on a busy laptop: the timing
  spread in four columns of the table above is the machine, not the code. No throughput or latency
  claim is made here, and none should be quoted from this table.
- `read-all` is *not* cheaper than `verify-all` in wall time even though it does fewer unseals per
  byte: it makes 87 requests instead of 9 because it reads per file, and on a memory store the
  per-request bookkeeping is the cost. On a networked store that trade would invert, which is why
  the numbers come with the store they were measured on.

### 5d. Before and after, in the one place it matters

| | before | after |
|---|---|---|
| a pack with one flipped bit, on a cold chunk | `Check` ok, `Reseal{Verify:true}` ok, first read of that chunk fails | `Verify` names the pack, the offset and the chunk id |
| cost of saying so | 1.1 % of the store, 7 requests | 99.7 % of the store, 9 requests, one full pack read per pack |

## 6. Where the cost comes from, and the limits this leaves

- **The unit of transfer is the pack, not the byte.** To check one chunk, the whole enclosing pack
  must be read: up to 16 MiB + 64 KiB for a 4 KiB frame. The pass is therefore O(store), not O(damage),
  and a store with many small packs pays per pack.
- **`ChunkHash` is unreachable through object corruption, and that is not a bug.** To get past
  `chunkAAD` a frame must be sealed under `(volume, chunk id)`, and then its plaintext hashes to
  that id; the two checks are defence in depth, and the report keeps the kind for a cipher or a
  key that is wrong in a way the AAD does not catch. `ShardSeal` and `ShardShape` are equally
  unreachable without rewriting the parent that names the shard. Both are declared rather than
  exercised: an unreachable branch dressed as a measurement is a false green.
- **Freshness is out of scope** (H3). A rollback needs a reference that is not in the store.
- **Hardware PI is `NO_MEDIDO`.** No NVMe, no `io_uring`, no per-block metadata: the software path
  only, which is what the bounty's own text allows and what this repository has today.
- **Sequential, single pack at a time,** and the pack is held whole. Concurrency was not added:
  on an in-memory store it hides no latency, and a number measured where there is nothing to hide
  would be a claim about S3 dressed as a claim about the code.
- **Superseded objects are invisible to everybody, correctly** — 4 of 12 in run A. They are old
  metadata nothing reaches; no reader should report them and none does.

## 7. Correctness and safety evidence

Local, on this machine (`go 1.27.4 darwin/arm64`, 8 CPUs), because **CI does not run on pull
requests from a fork** in this repository:

| command | result |
|---|---|
| `go test -run TestVerify -v .` | 4 tests, 6 cases, all pass; includes the false-green pair |
| `go test -race -short -run TestVerify .` | pass |
| `go test -short .` | `ok 255.741s` |
| `go test -short ./...` | all packages ok except `tools/gitbench::TestReferenceCloneBorrowsTheStore` — pre-existing and environmental (git resolves the temp directory through `/private` on macOS) |
| `go vet ./...` · `gofmt -l .` | clean |

One caveat measured twice rather than assumed: the suite keeps its journals on the node's disk, and
this machine has about 5 GB free. Started with less, a cluster of namespace tests fails mid-run
(`TestModelFuzz`, the `TestRename*` family, `TestHardLinks`, `TestDirectoryLinkCounts`); each of
them passes on its own in under a second, and the whole package passes when it starts with room
(`ok 255.741s`). It is the machine, not the change — and it is worth knowing before a reviewer
sees a red run that is not about this PR.

The test that justifies the file is `TestVerifyReadsWhatTheCheapChecksDoNot`: it flips one bit in
the middle of a pack and asserts, in order, that `Check` **passes**, that `Reseal{Verify:true}`
**passes**, and that `Verify` fails with `ChunkSeal` naming that pack, that offset and that chunk —
while the file that lives in a shard still reads back byte for byte. A test that only asserted
"`Verify` catches it" would pass on a package where nothing else was blind.

Safety: `Verify` only ever *opens* frames with the Volume's own key; a frame that does not open
yields `ChunkSeal` and no plaintext is written anywhere; the walk is read-only (§3) and has no new
dependency. Failure to reach the store is returned as an error, never as a verdict on the bytes —
"the store did not answer" and "the object is corrupt" are different answers and the API keeps them
apart.

## 8. Reproduction

```sh
go run ./tools/fspiverify -files 96 -size 266240 -csv docs/fspi.csv
```

Prints the two tables (§5b, §5c) and writes one row per (damage, object, reader) with the bytes,
requests, stats, duration and detail. `-files` and `-size` move the workload; `-repeat` sets the
control repetitions behind the median. Two things to expect: the store's layout differs from the
published one (that is §5a, and the CSV now records it), and the millisecond columns move with the
machine.

## 9. Status against HIP-1344

- Bounty **#10 / FS-PI** (`#55`, `#67`): the software-only path, over `vfs`'s own object layer.
- **Complementary to #94**, which injects corruption into journal sectors (`journal/fspi.go`):
  that is the layer below the cipher, this is the layer above it. Neither duplicates the other,
  and this change touches no journal file.
- Bounded PR: `integrity.go`, `integrity_test.go`, `tools/fspiverify/main.go`, `docs/fspi.md`,
  `docs/fspi.csv`, `docs/fspi-layout-b.csv`, plus two doc comments (`verify.go`, `volume.go`) that
  now point at the reader that does read the chunks.
- What a maintainer would do with it, in the order it pays: use it as a **scrub** on a schedule or
  before a copy is trusted, not as part of `Open` (99.7 % of the store is not an open-time cost);
  feed `Fault` into whatever the caller already reports, since it names the object and the offset;
  and treat `Round` as the freshness half that no in-band reader can supply.
