# Machine-checkable storage specifications (SYSSPEC)

Refs #56. Tracked by HIP-1349. The library is `pkg/sysspec`, the CLI evaluator
is `tools/sysspec`, and the live engine bridge tests are in `sysspec_test.go`
and `pkg/sysspec/spec_test.go`.

## 1. Primary reference, summary, and license audit

* **Primary paper:** Qingyuan Liu, Mo Zou, Hengbin Zhang, Dong Du, Yubin Xia,
  and Haibo Chen (Shanghai Jiao Tong University), *"Sharpen the Spec, Cut the
  Code: A Case for Generative File System with SYSSPEC"*, 24th USENIX Conference
  on File and Storage Technologies (FAST 2026),
  <https://www.usenix.org/conference/fast26/presentation/liu-qingyuan>.
* **Summary:** SYSSPEC treats structured, machine-checkable specifications
  rather than ad-hoc prose or isolated unit tests as the source of truth for
  storage correctness across tightly coupled modules. Each module specification
  is split into three layers:
  1. **Functionality:** explicit state invariants and transition
     pre/post-conditions over observable storage state.
  2. **Modularity:** **Rely/Guarantee** boundary contracts along an acyclic
     module dependency DAG so localized protocol changes propagate cleanly to
     dependent modules without breaking cross-layer invariants.
  3. **Concurrency:** explicit synchronization, epoch fencing, and group-sync
     barrier rules decoupled from sequential state transitions.
* **License and architecture audit:** The upstream SJTU-IPADS `SysSpec`
  artifact is distributed under the MIT License, which targets code generation
  for a standalone C/Rust research file system (`SpecFS`). **Zero code, grammar
  files, prompts, or fixtures were imported.** Everything in `pkg/sysspec`,
  `tools/sysspec`, and `sysspec_test.go` is a clean-room Apache-2.0 Go
  implementation authored specifically for `hanzoai/vfs` using only the Go
  standard library and existing `vfs` packages (`go.mod` and `go.sum` are
  untouched; zero patent or third-party notice additions). Canonical
  architecture (`vfs -> Hanzo S3 -> Visor`) is unchanged.

## 2. Reproducible upstream baseline

All measurements below were collected on `hanzoai/vfs` `main` using `go1.27.4
linux/amd64` (`x86_64`) prior to adding `pkg/sysspec`:

```sh
# 1. Static build, vet, cross-compile (darwin/arm64 CGO_ENABLED=0), and gofmt gate
pkgs=$(go list ./... | grep -v -e /replica -e /internal/sqlitecheck)
go build $pkgs && go vet $pkgs
go vet -tags fuse ./pkg/mount ./internal/mounttest
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build . ./cmd/... ./pkg/... ./tools/...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go vet -tags fuse ./pkg/mount
test -z "$(gofmt -l .)"

# 2. Unit, model, internal race, and journal suites
go test -short -count=1 -skip '^Test(Sparse|Model|TreeForced)' . ./cmd/... ./pkg/... ./internal/storetest
go test -short -count=1 -run '^Test(Sparse|Model|TreeForced)' .
go test -race -count=1 -timeout 4m ./internal/blake3 ./internal/bufpool
go test -short -count=1 ./journal

# 3. Existing mutation corpus audit
go run ./tools/sysspec -mutations tools/mutate/mutations.json
```

Upstream mutation inventory (`tools/mutate/mutations.json`, 250 entries across
22 files):

| Subsystem category | Files | Mutations in `mutations.json` | Share |
|---|---|---:|---:|
| Corruption, sealing, codec, verify | `head.go`, `cipher.go`, `codec.go`, `verify.go` | 53 | 21.2% |
| Fencing (`pkg/fence`) | `pkg/fence/fence.go` | 15 | 6.0% |
| Publication & Volume lifecycle | `volume.go` | 29 | 11.6% |
| Journal (`journal/*`) | `journal/*.go` | 0 | 0.0% |
| Prolly tree, FastCDC, packs, records | `tree.go`, `chunk.go`, `pack.go`, `shard.go`, `record.go`, `file.go`, `ops.go`, `lru.go` | 61 | 24.4% |
| Backends, FUSE mount, extensions | `pkg/backend/*`, `pkg/mount/*`, `ext.go`, `internal/storetest/mem.go` | 92 | 36.8% |
| **Total** | **22 files** | **250** | **100.0%** |

Two structural facts stand out in the baseline:

1. `tools/mutate` has strong single-file coverage of `head.go`, `volume.go`,
   `cipher.go`, and `pkg/fence/fence.go`, but **0 of 250 mutations** target
   `journal/`.
2. `journal/` has its own standalone randomized model (`journal/model_test.go`)
   and crash explorer (`journal/crash`), while `vfs` root has `crash_test.go`
   and `fence_test.go`, but **zero tests or mutations exercise the cross-module
   contract between `Volume`, `pkg/fence`, and `journal`**.

## 3. Falsifiable hypothesis and metrics

* **Hypothesis:** Encoding the journal, fencing, corruption, and publication
  invariants of `vfs` as an executable three-layer SYSSPEC DAG (state
  invariants, transition rules, Rely/Guarantee module contracts, and
  concurrency barrier rules) and generating trace regressions from them will:
  1. Detect **100%** of injected cross-module `Volume <-> journal <-> fence`
     protocol violations and `journal/` watermark/barrier regressions that
     `tools/mutate` and isolated package tests miss (`ClassSpecOnly`).
  2. Exhibit a well-defined **negative boundary (`0%` detection on abstract
     state traces, `ClassExistingOnly`)** on byte-level algorithmic cuts
     (FastCDC gear seed derivation, prolly boundary keying), kernel syscall
     ordering (`ptrace` `fdatasync -> rename -> fsync(dir)`), FUSE signal
     cancellation (`EINTR`), and post-`SIGKILL` bitrot inside the 16-round
     `through`-mark lag window, where existing AST/byte mutations and syscall
     tests remain strictly necessary.
* **Metrics:**
  1. Rule count and generated regression verdict accuracy (`%` exact rule match
     with `0` false positives on randomized valid multi-step traces).
  2. Bidirectional differential counts across a 24-case fault corpus:
     `both_catch` (`Class A`), `sysspec_only` (`Class B`), and `existing_only`
     (`Class C`).
  3. End-to-end specification evaluation latency (`us`).

## 4. Clean-room implementation

`pkg/sysspec` organizes the four required domains into a verified topological
DAG (`corruption <- fence <- publication <- journal`) containing **38
machine-checkable rules**:

* **11 State Invariants (`LayerFunctionality`):**
  * `C1_InjectiveSealingContext`: length-framed BLAKE3 context domain
    separation across `head` (`place`), `shard` (`volumeID`), and `chunk`
    (`volumeID`, `chunkID`).
  * `C2_CanonicalShortestVarint`: non-minimal varints fail closed as
    `ErrCorrupt`.
  * `C3_NeedsCheckedBeforeTail`: unknown `Head.Needs` bits return `ErrNeeds`
    before `Tail` validation (`ErrCorrupt`).
  * `C4_HeadTailStrictAscendingBounded`: at most 256 whole TLV fields in
    strictly ascending tag order, carrying every set `Needs` bit.
  * `C5_HeadSizeWithinFenceCeiling`: fenced `Head` never exceeds 65,536 bytes;
    oversized writes return `ErrTooLarge` without mutating the store.
  * `C6_CipherRejectsPlaintextOrPartialTag`: cipher never outputs raw plaintext
    and verifies all 32 authentication tag bytes.
  * `C7_ExtentChunkAndPackBounds`: full copy verification rejects missing
    chunk-table entries and packs truncated short of their furthest chunk.
  * `F1_RoundBoundsValid`: attached writer rounds lie strictly in
    `(0, 2^64 - 1)`.
  * `P2_OrphansNeverReferencedBeforeHeadCAS`: every shard and pack reachable
    from committed `Head` is uploaded prior to Head CAS; failed Flushes leave
    only unreferenced orphans.
  * `J1_MonotoneWatermarks`, `J2_ContiguousSequenceNoGaps`: `Published <=
    Durable <= Visible`, `Proven <= Durable`, `UsedBytes <= CapacityBytes`, and
    contiguous unpublished suffix `(Published+1 .. Visible)`.
* **15 Transition Rules (`LayerFunctionality`):**
  * `C8_JournalProvenDamageFailsClosed`: damage at or below a surviving
    `through` mark (`Proven > Published`) fails `Open` as `ErrCorrupt`.
  * `F2_PutRequiresAtLeastRecordedRound`, `F3_ClaimRequiresStrictlyHigherRound`,
    `F4_TakeoverPreservesLatestPayload`, `F5_DeposedWriterPermanentlyFenced`,
    `F6_LostAckTokenIdempotence`.
  * `P1_WriteOrderPacksShardsBeforeHead`, `P3_HeadSeqStrictlyNonRepeating`,
    `P4_HeadIdentityAndUnknownFieldsCarried`, `P5_CleanFlushMakesZeroStoreWrites`.
  * `J3_PublishRequiresDurable`, `J4_ReclaimPreservesUnpublishedAndFloor`,
    `J5_OpenRefusesLineageFormatAndFloorMismatch`, `J6_AppendMonotoneSequence`,
    `J7_DeadlineAnchoredToOldestUnpublished`.
* **8 Cross-Module Rely/Guarantee Contracts (`LayerModularity`):**
  * `RG1_JournalToPublisherDurableGate`: publisher commits `Head.Journal = g`
    only when `journal.Marks().Durable >= g`.
  * `RG2_PublisherToJournalPostCommitAdvance`: publisher calls
    `journal.Publish(g)` only after `Head` carrying `Journal >= g` commits in
    the store.
  * `RG3_PublisherToFenceAtomicHeadCAS`: `Flush` commits `Head` solely through
    `fence.Put` at the claimed `Round`.
  * `RG4_TakeoverRotatesJournalLineage`: takeover binds local journal `Lineage`
    to `(Volume.ID, Round)`.
  * `RG5_OpenNeverRetriesMismatchWithZero`: `ErrMismatch`/`ErrLineage`/`ErrCorrupt`
    from `journal.Open` falls back to store `Head` and never retries
    `Published = 0`.
  * `RG6_BulkSpoolDurableBeforeOrderedLogRef`: side bulk spool (`> 64 KiB`) is
    synced before appending its reference record to the ordered journal.
  * `RG7_ResealVerifiesRootShardUnderTargetVolume`: `Reseal` opens the root
    shard under `Head.ID` at the target backend before rewriting `Head`.
  * `RG8_LostAckLandedChecksFullCommitWatermarks`: lost-ack recovery on
    `ErrStaleRound` checks `Next` and `Journal` watermarks in addition to `ID`
    and `Root`.
* **4 Concurrency Rules (`LayerConcurrency`):**
  * `CC1_SyncGroupBarrierEpoch` (`Sync(seq)` requires sync flight started after
    `seq` was written), `CC2_ReplayBlocksReclaim`, `CC3_FlushExclusiveHold`,
    and `CC4_SingleConditionalWriteNoAdmitWindow`.

`GenerateRegressionSuite(seed)` synthesizes **52 deterministic regression
traces** (14 valid multi-step/boundary histories + 38 targeted single-rule
violations covering all 38 rules across the four domains and three SYSSPEC
layers). `sysspec_test.go` bridges real
`vfs.Volume`, `parseHead`, `seal`/`unseal`, `pkg/fence.Store`, and
`journal.Journal` (`journal/crash.Disk`) executions directly into
`Registry.CheckTrace`.

## 5. Correctness, concurrency, security, and live engine findings

1. **Real cross-layer finding on `Volume.landed()` (`volume.go:264`, `B01` /
   `RG8`):**
   `Volume.landed()` currently decides whether a fenced `Flush` had already
   landed with a lost acknowledgement via:
   ```go
   return cur, err == nil && cur.ID == v.id && cur.Root == v.root
   ```
   Because prolly tree node boundaries and hashes are a pure function of the
   record set and `Head.Seed`, an ABA sequence (or a journal-backed commit that
   advances `Head.Journal` on an unchanged metadata root) deposed by a
   successor at an older `Head.Journal` observes `cur.ID == v.id && cur.Root ==
   v.root` while `cur.Journal < targetJournal` (or `cur.Next < workingNext`).
   `TestSysSpecRealJournalFenceVolumeCrossLayerContract` reproduces this on a
   live `vfs.Volume` + `journal.Journal`: `v1.landed(ctx)` returns `true` when
   the store holds `Head.Journal = 2` and `v1` attempted `Journal = 3`, which
   would trick a publisher into calling `journal.Publish(3)` + `Reclaim()` and
   destroying unpublished record `3`. When connecting `journal` to `Volume`
   (#57), `landed()` should require `cur.Next >= uint64(v.next) && cur.Journal
   >= targetJournal`, and the clean-Flush fast path (`volume.go:229`) should
   also check that `Head.Journal` needs no advance.
2. **Lineage rotation across takeover (`B04` / `RG4`):**
   Verified on live `journal.Open` that binding `Lineage` to `Volume.ID ||
   Round` causes a successor at `Round = 2` to reject a predecessor's local
   segment directory with `journal.ErrLineage`, whereas passing `Volume.ID`
   alone silently adopts stale segment files.
3. **Concurrency and security:**
   `pkg/sysspec` passes `go test -race` with 12 concurrent trace-checking
   workers (`TestConcurrentTraceVerificationIsRaceFree`), enforces fail-closed
   checking on `ErrNeeds`, `ErrCorrupt`, `ErrContext`, `ErrTooLarge`, and
   `ErrStaleRound`, and compiles cleanly with `CGO_ENABLED=0` on `linux/amd64`
   and `darwin/arm64`.

## 6. Bidirectional comparison and raw measurements (HIP-1349)

Summary metrics from `go run ./tools/sysspec` and `hanzo.yml` test gates:

| Metric | Upstream baseline | With `pkg/sysspec` + `tools/sysspec` |
|---|---:|---:|
| Structured machine-checkable rules | 0 (prose in `docs/`) | 38 (11 inv, 15 tr, 8 RG, 4 conc) |
| Generated specification regression traces | 0 | 52 (100.0% exact verdict match) |
| Cross-module `Volume + fence + journal` fault detection (`B01..B08`) | 0 / 8 | 8 / 8 (100.0%) |
| Shared core invariant fault detection (`A01..A10`) | 10 / 10 | 10 / 10 (100.0%) |
| Byte/algorithmic/syscall/FUSE implementation faults (`C01..C06`, reverse) | 6 / 6 (100.0%) | 0 / 6 on state traces (by design) |
| `tools/sysspec` full matrix + 52-trace runtime | n/a | 45.5 ms (45,516 us) |
| Upstream `unit` / `model` / `internal -race` gates | 154.3 s / 105.9 s / 174.3 s | 100% green (0 regressions) |

### Bidirectional case-by-case table

| ID | Class | Domain | SYSSPEC | Existing suite | What happens and why |
|---|---|---|:---:|:---:|---|
| A01 | `both_catch` | publication | yes | yes | `Head` written before `pack`/`shard` upload (`P1` and `mutations.json` #25/#26 + `TestFailureAtEveryWrite`). |
| A02 | `both_catch` | fence | yes | yes | Older round admitted by `fence.Put` (`F2` and `mutations.json` #60 + `TestFenceRefusesOlderRound`). |
| A03 | `both_catch` | fence | yes | yes | `Attach`/`Reseal` admits equal or `2^64-1` round (`F3` and `mutations.json` #27/#28/#115/#125). |
| A04 | `both_catch` | publication | yes | yes | `Flush` reuses an attempted `Head.Seq` (`P3` and `mutations.json` #32 + `TestHeadSeqNeverRepeats`). |
| A05 | `both_catch` | corruption | yes | yes | Cross-place `Head` or cross-volume shard/chunk AAD swap (`C1` and `mutations.json` #41/#42/#90/#91). |
| A06 | `both_catch` | corruption | yes | yes | Non-minimal varint accepted (`C2` and `mutations.json` #206/#207 + `TestHeadNonMinimalUvarint`). |
| A07 | `both_catch` | corruption | yes | yes | Malformed `Head.Tail` checked before unknown `Head.Needs` (`C3` and `mutations.json` #205). |
| A08 | `both_catch` | corruption | yes | yes | Oversized `Head` (`> 65,536` B) written to store (`C5` and `mutations.json` #226-#231). |
| A09 | `both_catch` | corruption | yes | yes | Pack truncated 1 byte short of furthest chunk passes `Verify` (`C7` and `mutations.json` #174). |
| A10 | `both_catch` | journal | yes | yes | `journal.Publish` admitted ahead of `Durable` (`J3` and `journal/published_test.go`). |
| B01 | `sysspec_only` | publication | **yes** | **no** | **Live finding:** `volume.go:264` `landed()` checks `ID` and `Root` only; ABA/metadata-neutral root match across takeover falsely acknowledges uncommitted `Head.Journal`/`Next` (`RG8`). |
| B02 | `sysspec_only` | publication | **yes** | **no** | Publisher writes `Head.Journal = g` before local `journal.Marks().Durable >= g` (`RG1`); single-package tests pass independently. |
| B03 | `sysspec_only` | journal | **yes** | **no** | Publisher calls `journal.Publish(g)` + `Reclaim()` before `fence.Put("head")` succeeds (`RG2`), losing unpublished records on CAS conflict. |
| B04 | `sysspec_only` | journal | **yes** | **no** | Takeover opens `journal` with `Lineage = Volume.ID` without `Round` (`RG4`), failing to fence deposed local segments. |
| B05 | `sysspec_only` | journal | **yes** | **no** | Caller retries `journal.Open(..., Published: 0)` after `ErrMismatch` instead of falling back to store `Head` (`RG5`). |
| B06 | `sysspec_only` | journal | **yes** | **no** | Ordered journal appends metadata naming `> 64 KiB` bulk spool entry before spool `Sync` (`RG6`). |
| B07 | `sysspec_only` | publication | **yes** | **no** | Store replays an older valid `Head.Seq` below caller's `SeenSeq` (`P3`); single-Head signature/AAD checks pass. |
| B08 | `sysspec_only` | journal | **yes** | **no** | Mutations in `journal/` (`Sync` barrier epoch `CC1`, `Floor` order `J4`, `Due` anchor `J7`) survive `go run ./tools/mutate` (`0/250` journal entries). |
| C01 | `existing_only` | corruption | **no** | **yes** | FastCDC gear ignores `Head.Seed` or skips `minChunk` (`mutations.json` #8-#10): state invariants hold, chunk-length privacy breaks. |
| C02 | `existing_only` | publication | **no** | **yes** | Unkeyed prolly boundary hash (`mutations.json` #92): Merkle tree stays functionally valid while adversarial filenames bloat 4 shards to 2,007. |
| C03 | `existing_only` | publication | **no** | **yes** | FUSE handlers run on `req.Context` instead of `context.WithoutCancel` (`mutations.json` #116-#120): kernel/C `EINTR` bug outside storage state model. |
| C04 | `existing_only` | journal | **no** | **yes** | Missing `fdatasync -> rename -> fsync(dir)` syscall order (`journal/trace_linux_test.go` A20 `ptrace`): hidden above atomic `Disk.Create` abstraction. |
| C05 | `existing_only` | corruption | **no** | **yes** | URL parse error leaks credentials (`mutations.json` #97/#108/#109 + `TestRedaction`): string-hygiene bug invisible to state transitions. |
| C06 | `existing_only` | journal | **no** | **yes** | Sector bitrot in last `<= 16` sync rounds before `SIGKILL` (`journal/cadence_test.go` A25): `Proven` lags `Durable` by design unless `markRounds = 1`. |

### Integration recommendation (HIP-1349)

1. **Keep SYSSPEC and `tools/mutate` as orthogonal gates:** SYSSPEC catches
   multi-step cross-module Rely/Guarantee violations (`B01..B08`) in under 1 ms
   without recompiling the repository 250 times, while `tools/mutate`,
   `spec_test.go`, and `ptrace` syscall tests catch low-level cryptographic,
   chunking, FUSE-context, and filesystem-durability bugs (`C01..C06`) that
   abstract specifications cannot see.
2. **Wire `Registry.CheckStep` into the `#57` Volume-journal integration:**
   tighten `Volume.landed()` (`volume.go:264`) and `Flush()` (`volume.go:229`)
   to enforce `RG8` (`cur.Journal >= targetJournal && cur.Next >= workingNext`),
   enforce `RG4` (`Lineage = Volume.ID || Round`), and add `journal/` entries to
   `tools/mutate/mutations.json` alongside `pkg/sysspec`.
