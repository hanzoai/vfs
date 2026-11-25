# Production readiness of a Volume

Everything here was measured by the test named beside it; nothing is a claim without
one. The layout and protocols themselves are stated in `SPECIFICATION.md`. The previous engine measured 6.4 fsync/s on other hardware; that number is kept
only as history, since the code it described is gone.

Machine: GB10 arm64, 20 cores, Go 1.27.1, shared with other jobs (load average 8 to
16 during the runs, so throughput is a floor, not a peak). The file backend sat on
tmpfs, so its own fsync costs nothing: the figures are the engine's (chunking,
hashing, sealing, shard and Head writes), not a disk's. The real-server figures are a
Hanzo S3 server on the loopback interface, also on tmpfs.

## What is durable when a call returns

"Commit" is the Head advancing in the store: the conditional PUT of the Head has
returned success. This table is also in the package documentation (`doc.go`).

| Call | Durable when it returns |
|---|---|
| `File.WriteAt`, `Truncate`, `Create`, `Mkdir`, `Remove` | nothing. Held in memory and lost if the process dies. Past `Config.Dirty` bytes the data is chunked and uploaded, still unreferenced, still not durable |
| `File.Close` | nothing. It releases the handle; it is not a flush |
| `Volume.Flush` | everything written before the call, committed. It returns only after the Head advanced |
| Fsync on the mount (`FSYNC`) | the same as `Flush`: a full commit. There is no cheaper durable point. A signal that reaches the caller does not end the wait: the call returns the commit's own result |
| Close on the mount (`FLUSH`, every `close(2)`) | the same as `Flush`: a full commit. An interrupted close waits for the commit and returns its result |
| Release on the mount | nothing |
| End of a mount | one `Flush`, then the mount returns |
| `Volume.Close` | nothing. It releases the backend and does not flush: the detach sequence is `Flush`, then `Close` |
| `Attach` (takeover) | every commit the previous owner saw acknowledged is in the new owner's tree, and no commit by an older Round lands afterwards |

Interrupts on the mount. A signal that reaches a caller while it waits on the mount
makes the kernel interrupt the request (Go's runtime sends one every few milliseconds
to preempt), and no request is answered `EINTR`: every call waits for its own result.
What the answer costs a caller was measured, and C callers do not restart it. `fsync(2)`
answered `EINTR` is an I/O error to SQLite: a database on the mount failed
`TestFUSESQLite` 7 times of 7 on an amd64 kernel, and a C caller under a timer saw
246,912 `EINTR` over 5,000 fsyncs (neither glibc nor the kernel retries it). `stat(2)`
and directory reads answered `EINTR` reached a C caller as an error 1,586 times in
60,000 without `SA_RESTART` and 2,884 with it. `read(2)` answered `EINTR` reached the
caller as `EIO` (20 GiB of Go reads under signals failed at the first block).
`close(2)` is never restarted (Go's `os.File.Close` returns the error). A restarted
`create(2)` with `O_EXCL` would meet its own file. No caller's interrupt cancels a
commit or a fetch.

The cost is that a call waits for the store, and there are no deadlines on store
requests yet. The kernel keeps a request that reached the daemon until the daemon answers
it, so with the store stopped a caller that is killed stays in state D until the store
resumes (measured for read, stat, readdir, create, mkdir, unlink, truncate, write and
fsync). Measured again with the store stopped, so that the facts are not a guess:

* A cold lookup (the store is asked, no commit is running), a stat and a readdir are
  freed 3 s after the kill: an interrupt ends a request that changes nothing only after
  that grace. A mount that answered them at once freed the caller in 0.03 s, and could not
  be used by C callers.
* Anything behind a stalled commit is not freed: stat, a lookup of an absent name, readdir,
  mkdir, unlink, truncate by path, fsync of a read-only descriptor. The Volume's lock is
  held across the commit and a lock does not honour a request's context.
* read, create, truncate and write, and fsync on a descriptor, are not freed, before or
  now. A mount that answered fsync `EINTR` still waited: a goroutine dump shows the
  flush that the exit of the dying caller sends, which waits for the same commit. Close and
  exit are not freed behind a stalled commit either.
* Killing the daemon frees every caller in 20 ms (`ENOTCONN`) and loses what was not
  committed.
* The daemon holds about 134 KiB for each abandoned request, until its handler returns:
  1,600 reads at distinct offsets, 1,600 more through the page cache and 3,000 distinct
  lookups took it from 16.8 MB to 831 MB, with no panic and the data intact after the
  store resumed. Bounding what a read allocates changed nothing. Callers of fsync add
  nothing while they wait (12 writers blocked for 11 s: 20,344 kB before and after, two
  commits at most).

A supervisor therefore needs a store-latency watchdog that kills the daemon, limits on
processes and memory for the mount's callers and the daemon, and a lazy unmount at
teardown. Deadlines on store requests, and a lock that gives way to a context, are the
next change (section 11.2 of the specification).

What a failed `Flush` tells the caller. `nil`: committed. Any other error: the outcome
of that attempt is unknown (a reply can be lost after the write landed) and the
changes stay in memory for the next `Flush`. `ErrStaleRound`: this Volume was fenced
and can never commit again; a commit whose acknowledgement was lost may already be in
the successor's tree, and `Flush` reports `nil` when the store's Head is exactly this
Volume's tree (`TestLostAckThenTakeoverIsNotReportedLost`).

What a later change (a node journal for fast fsync, group commit) must keep. `Flush`
and the detach sequence return only after the Head advanced in the store. A takeover
loses no acknowledged commit. Objects are written once under names that make
repeating a write harmless (a hash of the content, or a random id), so a write that is
repeated or completes after its caller gave up changes nothing. The Head
compare-and-swap stays the only mutation. A journal may make a call cheaper only by
making it durable somewhere else first, and then `Flush` must still be a commit in the
store.

### The kill matrix

A process killed with SIGKILL, or a store call made to fail, at each point of a
commit; then the store is read from another process. In every row the fresh view is
the last committed tree, whole, and a retry commits the next.

| Point | Test |
|---|---|
| before the first pack upload | `TestSigkill/pack` |
| after the first pack upload | `TestSigkill/packed` |
| before the first shard upload | `TestSigkill/shard` |
| after the first shard upload | `TestSigkill/sharded` |
| before the Head write | `TestSigkill/head` |
| after the Head write | `TestSigkill/landed` |
| every write of a commit failing in turn, by kind: the pack, each shard, the Head | `TestFailureAtEveryWrite` |
| Head landed, reply lost, then retry | `TestLostAcknowledgementOfHead`, `TestLostAckNoLostUpdateNoDuplicate` |
| Head landed, reply lost, then takeover | `TestLostAckThenTakeoverIsNotReportedLost` |
| takeover during a stream of commits | `TestTakeoverLosesNoAcknowledgedCommit` |
| a writer on the mount killed mid-write | `TestFUSEKillDuringWrite` |
| twenty files each acknowledged by `fsync(2)` on the mount, then the server killed | `TestFUSEFsyncSurvivesKill`: all twenty in the store |
| killed at a random moment, 10 times over a real server | `TestKillOverRealServer`: every acknowledged file present and byte-exact after each kill |

## What is guaranteed

| Concern | Status | Evidence |
|---|---|---|
| Crash at any point leaves the last committed tree, whole | pass | the kill matrix above |
| A deposed writer cannot commit; takeover loses no acknowledged commit | pass | `TestFenceRefusesOlderRound`, `TestAttachNeedsAHigherRound`, `TestTakeoverLosesNoAcknowledgedCommit`, `TestRacingClaims`, `TestAttachLostAckIsNotStale`, `TestRoundMaxIsRefused`, on the in-memory store, the file backend, an S3 protocol server, and a real Hanzo S3 server |
| Exactly one of N writers on one Head version wins | pass | the conformance suite on every backend; against the real server `TestRealServerRaceStress` and `TestRealServerCAS` (128 racers, identical bodies of 1 B, 1 MiB and 20 MiB) |
| Conditional writes are never silently unconditional | pass | `TestRealServerMultipartIsConditional`, `TestLargeConditional`: above 64 MiB (a multipart upload) a conditional write is refused with `ErrTooLarge` |
| A read returns at most what its kind of object may weigh | pass | `TestHeadBound`, `TestShardBound`, `TestBackendBodyIsBounded`, `TestReadIsBounded`: a hostile store cannot make a read allocate its way out |
| Backend outage surfaces, loses nothing | pass | `TestFailureAtEveryWrite`: an error from the store is an error from `Flush`, the changes stay in memory, the store is untouched |
| Wrong key fails closed; tampered, swapped or foreign objects are refused | pass | `TestWrongKeyFailsClosed`, `TestTamperedObjectsAreRefused`, `TestSwappedChunkIsRefused`, `TestTreeDetectsSwappedShard`, `TestChunkHashIsChecked`, `TestSealedToItsContext`, `TestHeadContextIsExact`, `TestSwappedHeadIsRefused` (a shard, chunk or Head of another Volume or place does not open under a Cipher both share; the context is a fixed-length digest compared for equality, so a Head sealed for `.../vol10` does not open at `.../vol1`) |
| A Cipher that stops sealing is caught at the first Seal | pass | `TestCipherThatStopsSealingIsCaughtAtEverySeal` |
| The recipient alone cannot forge an object | pass | `TestRecipientAloneCannotForge`: every sealed object carries a tag keyed from the secret identity, so integrity rests on the identity, not on the (public) recipient |
| A guest who picks file names cannot multiply the metadata | pass | `TestGroundNamesDoNotBloatTheTree`: names ground against another key write as many shards as ordinary ones (6 and 6; 2,007 and 4 before the boundaries were keyed from the Volume's seed) |
| Every spelling of one place is one place; a moved Volume is resealed | pass | `TestPlaceSpellings`, `TestResealMovesAVolume` |
| Callers of fsync do not pile up commits | pass | `TestFsyncCallersShareTwoCommits`: 3,000 callers against a stalled store share two commits, one running and one waiting, and the store sees at most four Head writes |
| A mount hands the store a context on every path | pass | `TestLookedUpFileReadsOnTheMountContext` (over a store that panics on a nil context), `TestSessionRefusesANilContext`, `TestFUSEReadsExistingFilesOverS3` (a mount over the real server) |
| A Cipher that hides nothing is refused at configuration | pass | `TestIdentityCipherIsRefused` |
| Key rotation | pass | `TestRotation`: a Cipher with the new key and the old one reads everything and seals under the new; once the old is dropped, what only it sealed no longer opens |
| No key, plaintext or name on disk; the Cipher is the only door | pass | `TestNothingKeyShapedOnDisk`, `TestCipherIsTheOnlyDoor`, `TestEveryObjectIsHybridPQ`, `TestCLIRoundTrip` (the key comes from standard input; an environment variable is ignored) |
| A backend never prints a credential | pass | `TestURLCredentialRefused`, `TestURLStringNamesNoQuery` |
| A prefix cannot escape its place | pass | `TestValidPrefix`, `TestURLPrefixValidated`, `TestPrefixConfinement` (on the real server) |
| The on-disk records are extensible | pass | `TestInodeRecordIsExtensible`: a reader checks the fields it knows, carries the ones it does not, and writes them back |
| A hole costs nothing, and reads as zero | pass | `TestSparseExact` (hole, write into a hole, straddling hole/data/chunk/pack boundaries, truncate smaller than a hole then extend, overwrite then truncate, hole at EOF, many holes), `TestSparseRandom` (against a plain model across Flush, reopen and takeover), `TestChunkListIsTheWholeFileCut` |
| The tree is the same whatever order it was built in | pass | `TestTreeMatchesModel`, `TestTreeForcedCuts` (0 root mismatches in 1,200 batches at four node sizes) |
| A model of the whole API, many seeds | pass | `TestModelFuzz`, `TestModelFuzzMany` (300 seeds): random operations against a plain filesystem, with Flush, reopen and takeover; the memory bound is asserted |
| SQLite on the Volume | pass | `TestSQLiteRoundTrip`; over the mount `TestFUSESQLite` (100 rows survive release and remount, `integrity_check` ok) |
| The mount: Lookup, ReadDir (with `.` and `..`), Mkdir, Create, Remove, Setattr size, Read, Write, Flush, Fsync, Release | pass | `TestFUSEPOSIX`, `TestFUSESemantics`, `TestFUSEHostileNames`, `TestFUSEConcurrentSameFile`, `TestFUSEUnlinkVersusRmdir`; after `fsync(2)` a second read-only view of the store sees the file |
| The mount says what it cannot do | pass | `TestFUSESpecialFiles`: extended attributes answer ENOTSUP, `O_TMPFILE` EOPNOTSUPP, a device node EPERM; `TestFUSERename`: the flags of `renameat2` EINVAL; a file too large is EFBIG |
| Rename, hard links, symbolic links, attributes, times, unlink while open, fallocate, statfs, locks, mmap, pipes and sockets | pass | `TestFUSERename`, `TestFUSELinks`, `TestFUSEAttributes`, `TestFUSEUnlinkWhileOpen`, `TestFUSEFallocate`, `TestFUSEStatfs`, `TestFUSELocks`, `TestFUSEMmap`, `TestFUSESpecialFiles`, `TestFUSEReaddirDots`; the engine side is `TestModelPosix` against a plain model, and the crash sweeps over states that hold renames and links |
| A signal reaching a caller is never an `EIO` or an `EINTR`, and never cancels a commit or a fetch | pass | `TestInterruptIsEINTR`, `TestFsyncWaitsForItsCommit` (the caller is interrupted mid-PUT of the Head; it waits and gets the commit's result), `TestCloseWaitsForItsCommit`, `TestReadWaitsForItsData`, `TestLookupIsNotAnsweredEINTR`, `TestAttrIsNotAnsweredEINTR`, `TestReaddirIsNotAnsweredEINTR`, `TestMkdirIsNotAnsweredEINTR`, `TestRemoveIsNotAnsweredEINTR`, `TestCreateIsNotAnsweredEINTR`, `TestEveryHandlerOutlivesItsRequest`, and through a kernel mount `TestFUSEInterruptsAreNotErrors`: 2,000 fsync+close and 8 GiB of reads, with SIGURG sent to the waiting threads thousands of times a second, zero failures |

Mount tests run only with `-tags fuse`, from a child process that serves the mount
while the test acts as its client; a process that mounts a Volume and also opens a
file on that mount deadlocks. The real-server tests need an `s3` binary on `PATH`.

CI (`hanzo.yml`, `.github/workflows/cicd.yml`) runs sixteen gates as jobs, three at a
time, each under five minutes from a cold Go cache. Three at a time because the gates
share the machine's capacity, and more at once only made each slower. The gates are:
`check` (build, vet and gofmt of what does not need the SQLite engine, and a vet of
the mount for macOS); `unit`; `race`, the tests that have goroutines under the race
detector, with the handlers of the mount; `model`, the random tests against a plain
model at their short size, without the race detector (they have no goroutines, and the
detector multiplies their hashing tenfold); `sparse` and `sparse-tiny`, the same randomised tests of
holes at full size; `sqlite`, the only gate that builds the SQLite engine; `realserver`,
`realserver-cas`, `realserver-cas-large` and `realserver-kill`, against the real Hanzo
S3 server (the conditional-write race on a real server is split in two because its
objects are the slowest thing CI does), which fail if a test they exist for did not
pass; the measurements `scale-a`, `scale-b` and `scale-c`; `fuzz`; and `fuzz-head`, the
Head parser fuzzed for 20 s against a decoder written from the text.
The server is the published image of release v1.0.41, pinned by digest and read with
`crane`, which needs no docker: building it from its source costs five minutes of a
cold job, and every real-server test passes against the release as it did against a
build of the main commit cbf3d650. `TestSigkill` runs all six kill points in every
gate that runs it; the `-short` runs skip only the sizes of the randomised tests.

**The tests on a kernel mount are not CI gates, and why.** A gate runs on whatever
kernel it is given, and these tests assert what Linux's FUSE does: that after
`fsync(2)` the file is in the store, that truncate and chmod answer as Linux's do, that
a close is not interrupted by the kernel itself, and that an unmount ends the server.
They need `/dev/fuse` and a `fusermount3` that can mount for an unprivileged user (or
a user namespace, where `tools/fusemount` stands in for it: `tools/ci/mount.sh` runs
the tests that way). Where the kernel answers FUSE differently, a test of what Linux
does would test that kernel, so none is gated there and none skips there. They run on a
Linux host, and they are part of releasing:

> **Release checklist.** On an amd64 Linux kernel and on an arm64 one, from test binaries
> built with the toolchain of `go.mod` (`go test -tags fuse -c ./pkg/mount` and
> `./internal/sqlitecheck`, with `GOARCH=amd64 CGO_ENABLED=0` for a host without the
> toolchain), run each binary ten times, one at a time:
> `nice -n19 ./mount.test -test.count=1 -test.timeout 22m` and
> `./sqlitecheck.test -test.count=1`, with `s3` of `tools/ci/s3server.sh` on `PATH`. Every
> round passes. `TestFUSEInterruptsAreNotErrors` fails only for a call that failed, never
> for how long the host took.
>
> The tools that a coding agent runs, on a mount and on tmpfs, give the same results: with the
> Volume mounted at `$MNT` (`vfs mount`, key on standard input) and `$ORIGIN` a bare repository,
> `tools/ci/gitwork.sh $MNT/work $ORIGIN` and the same on a tmpfs directory print the same tree
> hashes, file counts, modes, sizes and content hash; and after an `npm ci` or a `pnpm install`
> of one lock file, `tools/ci/treehash.sh node_modules facts` gives one hash on both (pnpm's own
> `.modules.yaml` and `.pnpm-workspace-state-v1.json` name the store and the time, and differ).
> `go build` and `cargo check` run with their source, caches and outputs on the mount, and a
> second run finds everything built. `git update-index --test-untracked-cache` answers OK.
> `pjdfstest` (rename, link, symlink, chmod, chown, truncate, unlink, rmdir, mkdir, open,
> utimensat) fails on the mount only where it fails on tmpfs under the same user
> namespace: both lack the second user it switches to.
>
> `TMPDIR` is a directory the binary can create files in, made in the same session that runs the
> binary. `/dev/shm` is faster than a disk but is not safe to prepare ahead: on a host whose
> logind has `RemoveIPC=yes` and the account `Linger=no` (an amd64 host used for release runs), the
> account's `/dev/shm` is deleted when its last session ends, and a test started afterwards
> fails at once, in `t.TempDir()`, with nothing wrong with the Volume. `TestSQLiteRoundTrip`
> builds its reference database in one transaction; committed row by row it made 80,004 fsyncs,
> which took more than 250 s on a disk of about 3 ms an fsync.

The recorded Cipher object of `pin_test.go` pins what a later build of the library reads
(`TestTheCipherOpensAGoldenObject`) and the structure that a build of the past needs to read
what a later library writes: the length and id of the key, the version line and the type of
the recipient stanza (`TestTheCipherSealsTheStructureOfTheGoldenObject`). It does not run
the build of the past. Before a release that raises the version of `luxfi/age`, open a Volume
that the last release wrote with the new build, and a Volume that the new build wrote with the
last release.

What the mount does that needs no kernel is gated: the commits, the interrupts
(no request is answered `EINTR`, checked by a test of the source and by calling every
handler), the context every read runs on, and the creation of a file that exists, all
by calling the handlers, under the race detector.

## Measurements

| What | Result | Test |
|---|---|---|
| Sequential write, 4 GiB in 1 MiB writes, one handle, Flush every GiB | 143 MB/s; peak heap 361 MiB | `TestSequentialWriteMemory` |
| Sequential write, 50 GiB, same | 87 MB/s under load (10 min 15 s); peak heap 878 MiB against a 1 GiB bound; 5,254 objects | `TestSequentialWrite50GiB` |
| Flush after a 4 KiB overwrite in a 16 MiB file | p50 7 ms, p90 16 ms, p98 23 ms, max 46 ms (100 rounds) | `TestFlushLatency` |
| 64 KiB write then Flush, round-robin over 16 files, 10 s | 122 ops/s, mean 8 ms, p50 6 ms, p99 52 ms, max 69 ms (a box with a load average of 20) | `TestSustainedWriteWithFsync` |
| Objects written for 1,000 files of 200 bytes | 5 (4 shards, 1 Head) | `TestObjectsPerThousandSmallFiles` |
| Objects written for 1,000 files of 2.5 KiB | 50 (their bytes live in the shards, about one shard per 64 KiB) | same |
| Objects written for 1,000 files of 100 KiB | 15 (7 shards, 7 packs, 1 Head) | same |
| 100,000 files: create, first Flush, cold stat of every file | 0.9 s, 1.8 s (204 shards, 1 Head), 1.6 µs per Lookup + Stat | `TestHundredThousandFiles` |
| Ten changed files among 100,000 | a commit of 21 shards of 204, 67 ms | same |
| The same ten changes at 40,000 and at 120,000 files | 19 and 22 shards: the cost does not grow with the tree | `TestSyncCostDoesNotGrowWithTheTree` |
| 1,000,000 inodes: create and commit | 6.4 s; the store holds 1,150 objects, 72 MiB | `TestMillionInodes` |
| Memory at 1,000,000 inodes | the Volume holds 218 MiB after the commits (peak 358 MiB during one), against a cache bound of 256 MiB; a cold reader holds 2.8 MiB after opening and listing three directories | same |
| Ten changed files among 1,000,000 | a commit of 12 shards | same |
| Chunker, random data | mean chunk 578 KiB (170 chunks in 96 MiB), minimum 64 KiB, maximum 4 MiB | `TestCutBounds` |
| A 100 byte overwrite, insertion or deletion in the middle of 24 MiB (45 chunks) | 1 chunk changes | `TestCutStableUnderEdit` |
| A 6 byte edit in a file | 590 KiB of new packs | `TestSmallEditRewritesOneChunk` |
| Sealing overhead of the default Cipher | +1,756 bytes at 64 KiB, +1,996 at 1 MiB, +2,764 at 4 MiB (the context and the tag are 64 of them) | `TestAgeFrameOverhead` |

### Under signals, through the mount

Measured on an arm64 Linux 7.0 kernel (a kernel that is not Linux's cannot run this test; see above). The
work is bounded by count and by the clock, whichever comes first (2,000 fsyncs and 8 GiB
of reads, at most 45 s each), so it is fast on a busy disk.

| What | Result | Test |
|---|---|---|
| 2,000 small files written, fsynced and closed, two writers, SIGURG at their threads | 25,828 signals in 7.6 s; 0 failures; the callers saw `EINTR` from neither fsync nor close (a mount that answered fsync `EINTR` showed it 7,909 times in one run) | `TestFUSEInterruptsAreNotErrors` |
| 8 GiB read back in 1 MiB reads of a 256 MiB file, two readers, same signals | 49,526 signals in 6.5 s (1,325 MB/s); 0 failures | same |

### Holes

| What | Result | Test |
|---|---|---|
| Truncate a file to 64 GiB, then Flush | 7 µs, then 297 µs; no allocation | `TestSparseTruncateIsBoundedWork` |
| A 1 PiB file: truncate, write at the start, the middle and near the end, Flush, reopen, read back | 2 to 19 ms in all, by machine; 1 shard (4 KiB), 1 pack, 1 Head; heap +1 MiB; ceilings asserted on seconds, shard count and bytes, and memory | `TestSparsePetabyte` |
| The same through the mount, in a child process | truncate, three writes and fsync in 3 ms; the server's RSS +2 MiB; 4 objects in the store | `TestFUSEPetabyteTruncate` |
| 2,000,000 one-byte writes with no Flush | `Stats.Dirty` 62.9 MB (each write span is charged 64 bytes), the heap grew 297 MiB: about 5 times the accounted bytes, so in that pattern memory is bounded by five times `Config.Dirty` | `TestSmallWritesAmplifyMemory` |

### The cost of a small file, through the mount

Every `close(2)` on the mount is a commit (see Not yet built), so a tree written by
`cp -r` or `tar x` pays one Head, a few shards and one pack per file. The count varies with the Volume's seed, which decides where its nodes end: 20 KiB files cost 2.0, 2.8, 3.2 and 4.9 objects each in four runs on one machine, and a checkout of 5,000 files 4.9 to 6.0 objects each (143 to 168 files/s).

| What | Result | Test |
|---|---|---|
| 400 files of 20 KiB written through the mount | 1.1 s (374 files/s); the store went from 2 to 802 objects, 2.0 per file; 23.0 MiB held for 7.8 MiB of data (2.94 times). The count per file differs from run to run (2.0, 2.8, 3.2 and 4.9 in four runs on one machine): each Volume draws its own seed, and with it where its nodes end | `TestFUSECommitPerClose` |
| 500 files of 20 KiB, Flush per file, engine alone | 3.0 objects per file (1 Head, 1 shard, 1 pack) at no latency, 360 files/s; at 5 ms store latency 4.8 per file and 23.8 files/s | `TestCommitPerClose` |
| A 5,000-file checkout (284.6 MiB; 55% under 4 KiB, 30% to 32 KiB, 12% to 256 KiB, 3% to 2 MiB), a commit per file, through the mount | 35.1 s (143 files/s); the store holds 24,537 objects (4.91 per file) and 1,333.0 MiB (4.68 times the data) | `TestFUSECheckout` |
| The same 5,000 files, Flush per file, engine alone | 28,283 objects written (5.66 per file: 5,000 Heads, 21,667 shards, 1,616 packs); 23,285 held, 1,461.7 MiB (5.14 times) in 31.0 s | `TestCheckoutCost` |
| The same 5,000 files, one Flush at the end | 223 objects written (0.04 per file: 1 Head, 204 shards, 18 packs); 224 held, 288.8 MiB (1.01 times) in 6.6 s | same |
| Ten rewritten chunked files among 30,000 | a commit of 30 shards, 1 pack, 1 Head (ten inline files: 14 to 21 shards) | `TestChunkedFilesCommitCost` |

The 4 to 5 times is the shards a commit replaces: each commit rewrites the leaves it
touched and their ancestors, and the old ones stay until a collector exists. One
Flush at the end of the same tree leaves nothing behind.

### The amplification of a small change in a large file

| What | Result | Test |
|---|---|---|
| 200 appends of 4 KiB with a Flush each | 82,828 KiB of packs written for 800 KiB of data (104 times): each Flush re-chunks the tail chunk | `TestAppendWithFsyncAmplification` |
| A cold 4 KiB read | fetches the whole chunk holding it: 528 to 815 KiB (132 to 204 times) | `TestColdReadAmplification` |

### The object budget: 100,000 files, 150 GiB

`TestObjectBudget150GiB` builds every metadata record such a tree would have into a
real tree on a counting store, and models the data from the measured chunk mean and
sealing overhead (150 GiB is not written). The 4 KiB block store this layout replaced needed 39,321,600
objects for the same bytes.

| Shape | Chunks | Metadata shards | Packs | Objects |
|---|---|---|---|---|
| mixed (70,000 files of 4 KiB, 25,000 of 100 KiB, 4,900 of 2 MiB, 100 huge) | 285,700 | 5,056 (313 MiB) | 9,633 | 14,690 |
| the worst shape: 99,000 files of exactly 16 KiB (all inline) and 1,000 huge | 260,000 | 25,306 (1,586 MiB) | 9,630 | 34,937 |

Both are under 40,000. The second shows where the margin goes: inline bytes cost one
shard per 64 KiB, so a tree dominated by files just under 16 KiB approaches it.

### On a real server

| What | Result | Test |
|---|---|---|
| 300 small files, a commit per file, over HTTP to a Hanzo S3 server | 96 files/s; a commit p50 9.5 ms, p99 32 ms (measured against a build of the main commit cbf3d650; the gates now run the published release v1.0.41, which passes every test) | `TestCommitRateOnRealServer` |
| The same 300 files, one Flush | 218 ms (1,373 files/s) | same |
| Reads that return a version that is not the MD5 of their body, under writers | 0 in 14,660 reads and 8,611 writes | `TestGetPairsBodyWithVersion` |

## The cipher

Every number above was taken with `vfs.Hybrid`: X25519 and ML-KEM-768 together, one
age file per chunk and shard (`TestEveryObjectIsHybridPQ` checks that every object
on disk carries the hybrid stanza and none a classical-only one). It is the only
Cipher the library ships and the only one the command line builds; there is no plain
X25519 option. A caller who wants another Cipher implements the two-method
interface, and a Volume refuses one that does not seal (`ErrBadCipher`). The few
tests that use a cheap stand-in cipher (the tree and chunk-list tests) count nodes
and chunks, which do not depend on the Cipher, and none of them contributes a number
to this document.

Moving a Volume is a copy of its objects and `vfs.Reseal` of its Head: only the Head
is bound to the place (`TestResealMovesAVolume`).

## Upload defaults

A pack is cut at 16 MiB (`packTarget`, the top of the 4 to 16 MiB range that suits an object store) and
uploaded as one request; the S3 backend sends an object as a single request up to 64
MiB and uses a multipart upload only above that (`partSize`), which is also why a
conditional write above 64 MiB is refused rather than made unconditional. Packs are
uploaded one at a time, under the Volume's lock: a pipeline with several streams in
flight is not built yet (see below), so these are the settings it will start from.

## Mutation check

`tools/mutate` breaks the source one way at a time (a node cut by position instead of
content, a chunk-reuse rule, the commit order, the fence comparison, a missing
conditional header, an unchecked hash, a Cipher check removed, a hole that stores
chunks, a Get that reads past its bound, and so on), copies the repository, applies
the change, and runs the test that should catch it. `tools/mutate/mutations.json` is
the list; `go run ./tools/mutate -j 5` runs it. The list has 252 mutations and every one is killed by a test (a run takes about fifteen minutes on four workers).

## Not yet built

Plain statements of fact; each is its own change.

* Extended attributes, device nodes, `O_TMPFILE`, the flags of `renameat2` and the sticky
  bit's appearance in `stat` on a mount: the FUSE library has no request for the first four
  and drops the last (SPECIFICATION 11.2). Everything else of POSIX that a coding agent's
  tools use is there (SPECIFICATION 6.4).
* The Volume's lock is held across the store calls of a commit, so a slow or stalled
  store stalls every call on that Volume, `Stat` included (`TestStalledCommitHonoursItsDeadline`:
  a caller's deadline ends its own call, and a `Stat` waited 2.0 s behind the stalled
  commit). There are no deadlines of the library's own, no retry policy of its own,
  and no circuit breaker. The S3 backend's SDK retries a failing request about ten
  times, out of sight: an unreachable endpoint took about 31 s to fail. The mount flushes
  at its end with a deadline of 3 s (`TestFinishDoesNotWaitForAStalledCommitForever`): past
  it, the daemon says that the last commit did not finish and exits non-zero.
* A commit per `close(2)` on the mount: a file costs a Head, its shards and a pack
  (2.0 to 4.9 objects through the mount, above); group commit is not built. The node cache
  and journal are not built either: reads and writes go to the store, `Fsync` costs a
  commit, uploads are sequential, and there is no read-ahead.
* Concurrent cold readers of one chunk each fetch it: there is no shared in-flight
  fetch (readers hold the Volume's lock shared, and the cache is filled by whoever
  finishes first).
* Partial-chunk reads and writes: a cold 4 KiB read fetches a whole chunk, and each
  Flush after an append re-chunks the tail chunk (the 61 to 146 times and the 80 times
  above).
* Paged directory listing: `ReadDir` returns a whole directory (435 MiB allocated for
  1,000,000 entries, with the Volume locked). Removing a directory checks that it is
  empty without listing it.
* Garbage collection: chunks, packs and shards that a failed or superseded commit
  left, and the chunks of removed files, stay until a collector exists. A deposed
  writer can upload packs until its next commit is refused (`TestDeposedWriterIsToldAtItsNextCommit`:
  of 96 MiB written, 48 MiB of packs reached the store before it was told). Snapshot,
  Clone, Usage, import and export are not built either.
* Detecting a replayed old Head: a store that returns an older Head it once held
  cannot be told from the current one without a pin; a caller that must notice keeps
  the `Seq` of the last Head it saw. `Seq` counts commit attempts and never repeats,
  but it can skip.
* `vfs serve` and the agent.
* Compression.
* A tiny extent costs a whole sealed chunk (a few hundred bytes of header and tag on
  top of its bytes): 100,000 scattered 17-byte writes into a sparse file are 100,000
  chunks, about 171 MiB of packs for about 2 MiB of data, and a first Flush of 12.7 s
  (`TestTinyExtentsCost`). The cost follows the number of extents, not the size of the
  file. Packing tiny extents inline is not built.
* `statfs` reports the Volume's own usage; it is exact after a commit (SPECIFICATION 5.1).

## Running the tests

```bash
go test -short -race ./...             # the race detector over the lot (CI runs it on the tests with goroutines)
go test ./...                          # adds the scale tests and the full failure sweep
go test -tags fuse ./pkg/mount ./internal/sqlitecheck  # a kernel mount (Linux with /dev/fuse and fusermount3)
tools/ci/mount.sh ./pkg/mount run '^TestFUSEPOSIX$'    # the same as an unprivileged user, in a user namespace
PATH=$DIR:$PATH go test ./...          # $DIR holds the `s3` server (tools/ci/s3server.sh $DIR): adds the real-server tests
go test -run xxx -bench . .            # throughput
go run ./tools/mutate -j 5             # every mutation must be killed by a test
```
