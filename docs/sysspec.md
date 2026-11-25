# Machine-checkable storage specifications (SYSSPEC)

Refs #56. Tracked by HIP-1349. The library is `pkg/sysspec`, the CLI evaluator
is `tools/sysspec`, and the live VFS v2 engine bridge tests are in
`sysspec_test.go` and `pkg/sysspec/spec_test.go`.

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
  architecture (`vfs -> Hanzo S3 -> Visor`) is unchanged; SYSSPEC is regression
  verification infrastructure over the current VFS v2 `Volume` lifecycle, not a
  new filesystem or separate runtime.

## 2. Reproducible baseline against current `main` (`v0.7.0` + `#89`, `c3335b13`)

All mutation inventory and kill measurements below were collected against
`hanzoai/vfs` `main` commit `c3335b13a6204b160898afe3235d7bea0f9b44e9`
(`v0.7.0` journaled `Volume` + `#89`):

```sh
# 1. Static build, vet, cross-compile (darwin/arm64 CGO_ENABLED=0), and gofmt gate
pkgs=$(go list ./... | grep -v -e /replica -e /internal/sqlitecheck)
go build $pkgs && go vet $pkgs
go vet -tags fuse ./pkg/mount ./internal/mounttest
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build . ./cmd/... ./pkg/... ./tools/...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go vet -tags fuse ./pkg/mount
test -z "$(gofmt -l .)"

# 2. SYSSPEC package and live v2 Volume lifecycle trace suites under -race
go test -race -count=1 ./pkg/sysspec/...
go test -race -count=1 -run '^TestSysSpec' .
go run ./tools/sysspec -mutations tools/mutate/mutations.json

# 3. Existing mutation corpus verification on current main (c3335b13)
for pat in 'generation:' 'ledger:' 'fence:' 'volume:' 'reseal:' 'budget:' 'head:'; do
  go run ./tools/mutate -j 6 -timeout 25s -only "$pat"
done
```

### Upstream mutation corpus inventory (`tools/mutate/mutations.json` on `c3335b13`, 398 entries across 29 files)

| Subsystem category | Files | Mutations in `mutations.json` | Share |
|---|---|---:|---:|
| Corruption, sealing, codec, verify, check | `head.go`, `cipher.go`, `codec.go`, `verify.go`, `check.go` | 62 | 15.6% |
| Fencing (`pkg/fence`) | `pkg/fence/fence.go` | 18 | 4.5% |
| Publication, Generation & Volume ledger | `volume.go` (30), `generation.go` (24), `ledger.go` (1) | 55 | 13.8% |
| Journal (`journal/*`) | `journal/*.go` | 0 | 0.0% |
| Prolly tree, FastCDC, packs, records, namespace | `tree.go`, `chunk.go`, `pack.go`, `shard.go`, `record.go`, `file.go`, `ops.go`, `lru.go`, `attr.go`, `namespace.go`, `rename.go`, `usage.go` | 143 | 35.9% |
| Backends, FUSE mount, extensions | `pkg/backend/*`, `pkg/mount/*`, `ext.go`, `internal/storetest/mem.go` | 120 | 30.2% |
| **Total** | **29 files** | **398** | **100.0%** |

### Mutation kills against current `main` (`c3335b13`)

Running the existing `tools/mutate` suite (preserved unmodified) across the 67
core storage-specification mutations (`generation:`, `ledger:`, `fence:`,
`volume:`, `reseal:`, `budget:`, `head:`) on `c3335b13` yields **57 killed, 7
survived, 3 build-broken**:

| Filter | Total | Killed | Survived | Build-broken | Notes on `c3335b13` |
|---|---:|---:|---:|---:|---|
| `fence:` | 15 | 15 | 0 | 0 | 100% killed |
| `head:` | 10 | 10 | 0 | 0 | 100% killed |
| `reseal:` | 8 | 8 | 0 | 0 | 100% killed |
| `generation:` | 13 | 10 | 2 | 1 | Survived: `any tree with the same root counts as landed`, `removed file keeps its data records`; Build: `a Flush is answered by an earlier generation` |
| `volume:` | 17 | 12 | 4 | 1 | Survived: `a full journal is refused though a publication drains it`, `removed file stays openable`, `a fenced Volume answers sync with an error`, `an owner that still holds its journal is not told it was fenced`; Build: `a journal with an unpublished tail is retired on close` |
| `budget:` | 3 | 2 | 1 | 0 | Survived: `a Volume that holds nothing waits for a spent Budget` |
| `ledger:` | 1 | 0 | 0 | 1 | Build: `a sealed record is the plaintext` |
| `journal/*` | 0 | 0 | 0 | 0 | `0 / 398` mutations in `mutations.json` target `journal/` |
| **Subtotal (core specs)** | **67** | **57** | **7** | **3** | Existing mutation tests preserved intact |

## 3. Clean-room implementation and live VFS v2 `Volume` lifecycle traces

`pkg/sysspec` organizes the four required domains into a verified topological
DAG (`corruption <- fence <- publication <- journal`) containing **38
machine-checkable rules**:

* **11 State Invariants (`LayerFunctionality`):** `C1`–`C7`, `F1`, `P2`, `J1`, `J2`.
* **15 Transition Rules (`LayerFunctionality`):** `C8`, `F2`–`F6`, `P1`, `P3`–`P5`, `J3`–`J7`.
* **8 Cross-Module Rely/Guarantee Contracts (`LayerModularity`):** `RG1`–`RG8`.
* **4 Concurrency Rules (`LayerConcurrency`):** `CC1`–`CC4`.

`GenerateRegressionSuite(seed)` synthesizes **52 deterministic regression
traces** (14 valid multi-step/boundary histories + 38 targeted single-rule
violations covering all 38 rules). `sysspec_test.go` verifies real VFS v2
`Volume` executions directly against `Registry.CheckTrace` / `CheckStep`:

1. **`TestSysSpecRealFlushWriteOrderAndOrphanSafety`:** clean no-op `Flush`
   (`P5`), chunked + small writes with injected Head failure, pack -> shard ->
   Head write order (`P1`), orphan safety (`P2`), and strict `Head.Seq`
   non-repetition (`P3`).
2. **`TestSysSpecRealCorruptionAndContextDomainSeparation`:** real `seal`/`unseal`,
   `unmarshalHead`, and `fence.Store` against `C1`, `C2`, `C3`, `C5`.
3. **`TestSysSpecRealJournalFenceVolumeCrossLayerContract`:** live v2 `Volume`
   journal watermarks, `RG4` lineage rejection (`lineage(id, round)`) on Round 1's
   on-disk journal opened under Round 2, plus the **historical pre-v2 B01
   negative fixture** (confirming both that v0.7.0 `Volume.landed(ctx, Head)`
   rejects ABA `Journal` mismatch and that `RG8` still catches injected pre-v2
   false positives).
4. **`TestSysSpecRealV2CrashReopenTrace`:** real v2 `Volume` local mutations +
   `Sync` (0 store writes), un-flushed process exit, refused Round 2 reopen
   leaving the local journal intact, and Round 3 same-node replay + publication
   verified against `Recovery()`, read-only `Open`, and `Registry.CheckTrace`.
5. **`TestSysSpecRealV2FencingTakeoverTrace`:** real v2 `Volume` Round 1 commit,
   equal-round `Attach` refusal (`F3`), unpublished local tail, cross-node Round 2
   takeover preserving `Head` payload (`F4`) and rotating lineage (`RG4`),
   permanent fencing of the deposed writer (`F2`/`F5`/`RG3`) without falsely
   advancing `State().Published` past `Head.Journal` (`RG2`), and successor commit.
6. **`TestSysSpecRealV2MonotonicPublicationTrace`:** multi-generation explicit
   `Flush` (with injected Head failure + retry) plus controlled-clock
   `Config.Bound` background publication, asserting strict monotonicity of
   `Head.Seq`, `Head.Journal`, `Head.Next`, and journal watermarks (`J1`/`J2`/`P3`/`P4`).
7. **`TestSysSpecRealV2BoundedBackpressureTrace`:** real v2 `Volume` under tight
   `JournalCapacity` (8 MiB) and `Dirty` (1 MiB): healthy early-publication drain,
   S3 outage hitting bounded `ErrFull` without mutating `Visible` (`J6`) while
   maintaining `Degraded=true`, `Backlog <= JournalCapacity` (`J1`), and working
   local `Sync`, followed by recovery drain and exact store verification.

## 4. Historical pre-v2 B01 fixture and current v2 findings

1. **Historical pre-v2 B01 negative fixture (`RG8`):**
   Pre-v2 `Volume.landed()` checked only `cur.ID == v.id && cur.Root == v.root`.
   In `v0.7.0` (`generation.go:532`), production `Volume.landed(ctx, h)` was
   fixed to require `cur.ID == h.ID && cur.Root == h.Root && cur.Journal == h.Journal`.
   Accordingly, B01 is retained **only as a historical negative fixture** in
   `pkg/sysspec` and `sysspec_test.go`. Notably, on `c3335b13`, relaxing
   `landed()` to `return cur, err == nil` (`mutations.json` entry `"generation:
   any tree with the same root counts as landed"`) still survives
   `TestAFencedPublicationIsNotRecognisedAsLanded` because the deposed writer's
   `Flush` returns `ErrStaleRound` on the subsequent publisher loop tick while
   silently calling `j.Publish(g.seq)` — whereas `TestSysSpecRealV2FencingTakeoverTrace`
   asserts `v1.State().Published == h1.Journal` and catches it directly.
2. **Convergence of B02 and B03 on `v0.7.0`:**
   `v0.7.0` landed both `generation.go` durability gating (`syncTo` before Head
   CAS, `TestHeadWaitsForTheJournalToBeDurable`) and post-commit journal
   advancement (`TestJournalIsToldAfterTheHead`), so B02 (`RG1`) and B03 (`RG2`)
   are reclassified to `both_catch` on `c3335b13`.
3. **Remaining `sysspec_only` gaps (`B04`–`B08`):**
   `ledger.go` has zero lineage-rotation mutations (`RG4`), caller-side
   `ErrMismatch` fallback (`RG5`), bulk spool ordering (`RG6`), caller-tracked
   `SeenSeq` rollback (`P3`), and `journal/` still has `0 / 398` entries in
   `tools/mutate/mutations.json` (`B08`).

## 5. Bidirectional comparison on current `main` (`c3335b13`)

| Metric | Current `main` (`c3335b13`) | With `pkg/sysspec` + `tools/sysspec` |
|---|---:|---:|
| Structured machine-checkable rules | 0 | 38 (11 inv, 15 tr, 8 RG, 4 conc) |
| Generated specification regression traces | 0 | 52 (100.0% exact verdict match) |
| Live v2 `Volume` lifecycle trace tests | 0 | 7 tests in `sysspec_test.go` (`-race` clean) |
| Upstream `tools/mutate/mutations.json` entries | 398 (`journal/*`: 0) | 398 (preserved untouched) |
| Comparative matrix (24 cases) | 12 `both_catch`, 6 `existing_only` | 12 `both_catch`, 6 `sysspec_only`, 6 `existing_only` |

### Bidirectional case-by-case table

| ID | Class | Domain | SYSSPEC | Existing suite | What happens and why |
|---|---|---|:---:|:---:|---|
| A01 | `both_catch` | publication | yes | yes | `Head` written before `pack`/`shard` upload (`P1` and `generation.go` mutations + `TestFailureAtEveryWrite`). |
| A02 | `both_catch` | fence | yes | yes | Older round admitted by `fence.Put` (`F2` and `pkg/fence/fence.go` mutation + `TestFenceRefusesOlderRound`). |
| A03 | `both_catch` | fence | yes | yes | `Attach`/`Reseal` admits equal or `2^64-1` round (`F3` and `volume.go` mutations + `TestAttachNeedsAHigherRound`). |
| A04 | `both_catch` | publication | yes | yes | `Flush` reuses an attempted `Head.Seq` (`P3` and `generation.go` mutation + `TestHeadSeqNeverRepeats`). |
| A05 | `both_catch` | corruption | yes | yes | Cross-place `Head` or cross-volume shard/chunk AAD swap (`C1` and `head.go`/`cipher.go` mutations). |
| A06 | `both_catch` | corruption | yes | yes | Non-minimal varint accepted (`C2` and `codec.go` mutations + `TestHeadNonMinimalUvarint`). |
| A07 | `both_catch` | corruption | yes | yes | Malformed `Head.Tail` checked before unknown `Head.Needs` (`C3` and `head.go` mutations). |
| A08 | `both_catch` | corruption | yes | yes | Oversized `Head` (`> 65,536` B) written to store (`C5` and `pkg/fence/fence.go` mutations). |
| A09 | `both_catch` | corruption | yes | yes | Pack truncated 1 byte short of furthest chunk passes `Verify` (`C7` and `verify.go` mutations). |
| A10 | `both_catch` | journal | yes | yes | `journal.Publish` admitted ahead of `Durable` (`J3` and `journal/published_test.go`). |
| B01 | `sysspec_only` | publication | **yes** | **no** | **Historical negative fixture (fixed in v0.7.0 `generation.go:532`):** pre-v2 `landed()` ABA match (`RG8`); relaxing `landed()` still survives `TestAFencedPublicationIsNotRecognisedAsLanded` on `c3335b13`. |
| B02 | `both_catch` | publication | **yes** | **yes** | Converged in v0.7.0: `RG1` and `"generation: the journal is not made durable before the Head"` (`TestHeadWaitsForTheJournalToBeDurable`). |
| B03 | `both_catch` | journal | **yes** | **yes** | Converged in v0.7.0: `RG2` and `"generation: the journal is told before the Head commits"` (`TestJournalIsToldAfterTheHead`). |
| B04 | `sysspec_only` | journal | **yes** | **no** | Takeover opens `journal` with `Volume.ID` without `Round` (`RG4`); `ledger.go` has 0 lineage mutations. |
| B05 | `sysspec_only` | journal | **yes** | **no** | Caller retries `journal.Open(..., Published: 0)` after `ErrMismatch` (`RG5`). |
| B06 | `sysspec_only` | journal | **yes** | **no** | Ordered journal appends metadata naming `> 64 KiB` bulk spool entry before spool `Sync` (`RG6`). |
| B07 | `sysspec_only` | publication | **yes** | **no** | Store replays an older valid `Head.Seq` below caller's `SeenSeq` (`P3`). |
| B08 | `sysspec_only` | journal | **yes** | **no** | `0 / 398` entries in `tools/mutate/mutations.json` target `journal/` (`CC1`/`J4`/`J7`). |
| C01 | `existing_only` | corruption | **no** | **yes** | FastCDC gear ignores `Head.Seed` or skips `minChunk` (`chunk.go` mutations). |
| C02 | `existing_only` | publication | **no** | **yes** | Unkeyed prolly boundary hash (`tree.go` mutation + `TestGroundNamesDoNotBloatTheTree`). |
| C03 | `existing_only` | publication | **no** | **yes** | FUSE handlers run on `req.Context` instead of `context.WithoutCancel` (`pkg/mount` mutations). |
| C04 | `existing_only` | journal | **no** | **yes** | Missing `fdatasync -> rename -> fsync(dir)` syscall order (`journal/trace_linux_test.go` `ptrace`). |
| C05 | `existing_only` | corruption | **no** | **yes** | URL parse error leaks credentials (`pkg/backend/backend.go` mutations + `TestRedaction`). |
| C06 | `existing_only` | journal | **no** | **yes** | Sector bitrot in last `<= 16` sync rounds before `SIGKILL` (`journal/cadence_test.go`). |
