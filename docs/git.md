# Git on a vfs mount

A coding run is mostly git: it clones one commit, switches to a branch, looks
(`status`, `diff`), edits, stages, commits and pushes. This document measures
that sequence, says which client settings are worth changing and which are not,
prices fsync for the three kinds of disk a run can land on, designs starting a
run without downloading the repository again, and lists what the forge should
change. The runner is `tools/gitbench`; every number below can be regenerated
with it (section 9), on a local disk, on tmpfs, or on a mounted volume.

The measurements are on a local NVMe disk and on tmpfs, and the benchmark
prints the system calls that a volume would be charged for, so the mount's
number can be computed before it is measured. The mount runs git since the
POSIX operations of issue #28 (rename, links, attributes, directory times):
`git clone --depth 1`, `add`, `commit`, `checkout`, `status`, `gc` and `fsck` give
the trees that tmpfs gives, and `tools/gitbench` runs on it.

Refs #29.

## 1. Result

The settings for a coding run's checkout are three, written at clone time, and
git's defaults otherwise:

```sh
git clone --quiet --depth 1 --single-branch \
    -c core.untrackedCache=true -c gc.auto=0 -c maintenance.auto=false \
    <remote> .
```

| change | measured | section |
|---|---|---|
| **No `--filter=blob:none`.** A run checks out every file, so the filter only splits the download in two. | Large tree (166,187 files): clone 21.7 s to 13.1 s (minimum of five), CPU 26.5 s to 17.1 s (-35%), fsyncs 6 to 3, 1.79 to 1.58 million calls. Medium tree: sequence CPU 4.9 s to 3.5 s (-28%, interleaved repetitions). Over HTTP: five exchanges to three, the second discovery (0.26 to 0.38 s here, 1.7 s and up on a big mirror) gone; the server's pack work 1.8x lower. | 4, 7 |
| **`core.untrackedCache=true`** | Second and later `git status`: 15,657 to 9,609 calls (-39%), directory reads 2,866 to 4, 31 to 9 ms (minimum). With 100 us per directory call: 714 to 53 ms (minimum), 2.1 s to 0.25 s (median). | 4 |
| **`gc.auto=0`, `maintenance.auto=false`** | Nothing in an ordinary run. The commit that stages 8,000 files and crosses git's gc threshold: 735 to 60 ms (NVMe), 1,674 to 386 ms (8.8 ms fsync). | 4 |
| **Never sync loose objects** (git's default does not; do not change it). | 8,000 staged files with `core.fsync=committed`: 71.9 s median on this disk (6 ms fsync), 128 s with 8.8 ms injected, against 1.8 s and 0.34 s. `core.fsyncMethod=batch` makes it one fsync. | 5 |

Rejected by measurement: `core.fsync=none` (saves at most 4% of a run at 8.8 ms
fsync), `checkout.workers` (tripled write calls and did not shorten a local clone; it halves a large clone only when each directory call costs 100 us, so it is a decision for the mount, section 10),
`feature.manyFiles`, `core.fsmonitor` (git 2.43 on Linux has no daemon and
ignores it), `GIT_OPTIONAL_LOCKS=0` (+24% calls), index version, split index,
thread counts, compression, commit graph. Every setting measured is in section 4.

Starting a run without downloading the repository (section 6): borrowing the
objects from a read-only per-node store takes the large clone from 13.1 s to
4.7 s and from 296 MB written under `.git` to 20 MB; a snapshot of the finished
checkout, cloned in constant time, takes the first command to a status call, and
needs the clone to keep modification times (otherwise the first `git status`
reads every file: 312 ms against 29 ms on the medium tree, 6.3 s against 0.54 s on the
large one, avoided by `core.checkStat=minimal`).

The forge (section 7): protocol-version-2 discovery answers in 0.1 to 0.3 s for
ordinary repositories and 1.7 s to 125 s (HTTP 524) for mirrored ones, for a
reply of 178 bytes that does not depend on the repository. Serving it from a
constant is the first change.

Git runs identically on any mount; `tools/gitbench` reproduces every number
(section 9). The mount cannot run it until the POSIX operations land (#28).

## 2. Method

**Repositories.** Two real repositories, copied (`git clone --bare
--no-hardlinks`) and never modified: a medium one (6,234 files, 21,441
commits, 124 MB tree, 593 MB of packs in 28 files plus 4,348 loose objects, as it
lies on a developer's disk) and a large one (166,187 files, 5 commits, 2.3 GB tree, 290 MB pack).
Each is the *origin* of a fixture built in the directory under test: a bare
repository that serves partial clones and receives pushes, standing in for the
forge, and a branch `bulk` that rewrites 80% of 2,500 files, deletes 10% and adds
10% (so a branch switch touches 2,500 files). Nothing is pushed anywhere else.

**Sequence** (what a run does, in the order it does it): `clone --depth 1
--filter=blob:none --single-branch` (the current runner's command), `fetch` of
the bulk branch, `switch` to it (2,500 files), `switch` back, `switch -c` the
run's branch, `status` three times (first after the switch, repeated, and with
edits present), edit 25 tracked files and add 10 new ones, `add -A`, `commit`,
`diff --stat`, `log`, `gc --auto`, `push` to the origin. A churn variant
stages 8,000 small untracked files (a vendored directory nobody ignored)
before the commit.

**Measured per operation:** wall time and CPU (user+sys) of a timed pass, and
the system calls of a second pass under `strace -c -f`: fsync, fdatasync,
sync_file_range, rename, openat, unlink, stat, getdents, write and the total.
Each configuration runs the whole sequence in a fresh directory; the timed pass
and the counted pass are separate runs, because tracing slows a process
several times. Git is 2.43.0. Every git command runs with no user or system
configuration (`GIT_CONFIG_GLOBAL` names a file that only trusts the
directory), as it does in a leased sandbox.

**Settings are given with `-c`** on every command, one change per
configuration, so a result is attributable; combinations are written `a+b`.

**The machine was shared.** One-minute load average was between 6 and 90 on a
20-core arm64 host while this ran; each row of the CSV carries the load at its
start. A wall-clock difference smaller than the spread between repetitions
means nothing, and this document does not claim one. What does not depend on
load: system-call counts (deterministic to a few percent), fsync counts
(exact), and the CPU-seconds of a run (stable to about 10%). Wall time is
reported as median and minimum of five repetitions; CPU and counts decide.

**Disks.** `probe` prices the calls git is made of, with no git in it.

| call | ext4 on NVMe (p50) | tmpfs (p50) |
|---|---|---|
| fsync of a new 4 KiB file | 6.0 to 7.7 ms (p99 44 to 83 ms) | 0.0002 ms |
| create, write 1 KiB, close | 0.012 to 0.017 ms | 0.002 to 0.003 ms |
| rename | 0.010 to 0.024 ms | 0.003 to 0.006 ms |
| stat | 0.002 ms | 0.001 to 0.005 ms |
| unlink | 0.007 to 0.021 ms | 0.005 to 0.010 ms |

So the local disk is in the 4 to 9 ms fsync class and tmpfs in the 20 us class
(or better); both ends of the range the volume can be in.


## 3. A coding run's git, measured

The current runner's sequence on the medium repository, local NVMe, default
git settings, five repetitions (CPU and calls are medians; the wall minimum is
the run least disturbed by the machine's other tenants):

| operation | wall min | wall median | CPU | fsync | rename | openat | stat | getdents | write | all calls |
|---|---|---|---|---|---|---|---|---|---|---|
| clone | 1956 ms | 3973 ms | 3521 ms | 6 | 19 | 10652 | 15974 | 70 | 14248 | 87518 |
| fetch | 107 ms | 201 ms | 87 ms | 4 | 2 | 1872 | 572 | 46 | 306 | 7621 |
| switch_bulk | 366 ms | 837 ms | 372 ms | 3 | 3 | 4869 | 17147 | 36 | 2321 | 33434 |
| switch_base | 251 ms | 450 ms | 243 ms | 0 | 2 | 3707 | 15071 | 4 | 2603 | 27775 |
| switch_new | 4 ms | 8 ms | 4 ms | 0 | 2 | 55 | 145 | 4 | 5 | 444 |
| status_cold | 313 ms | 411 ms | 199 ms | 0 | 1 | 2383 | 9804 | 2866 | 7 | 17791 |
| status_warm | 31 ms | 384 ms | 182 ms | 0 | 0 | 1797 | 9219 | 2866 | 0 | 15657 |
| status_dirty | 36 ms | 97 ms | 54 ms | 0 | 0 | 1799 | 9339 | 2867 | 1 | 15803 |
| add | 40 ms | 101 ms | 67 ms | 0 | 1 | 1966 | 9277 | 2868 | 35 | 16348 |
| commit | 18 ms | 106 ms | 41 ms | 0 | 2 | 228 | 7958 | 58 | 48 | 9502 |
| diff | 7 ms | 9 ms | 8 ms | 0 | 0 | 198 | 210 | 56 | 1 | 961 |
| log | 2 ms | 4 ms | 3 ms | 0 | 0 | 30 | 77 | 4 | 1 | 250 |
| gc_auto | 2 ms | 3 ms | 2 ms | 0 | 0 | 17 | 43 | 2 | 0 | 161 |
| push | 70 ms | 217 ms | 112 ms | 0 | 1 | 1787 | 1702 | 164 | 463 | 10661 |

- **The clone is the run.** It is 1.96 s of the 3.2 s sequence, and 21.7 s of
  24.2 s for the large repository (26.5 s of CPU). Everything else a run
  does to git is under 0.4 s each, and `commit`, `diff`, `log` and `gc --auto`
  are under 20 ms.
- **A clone is about 14 calls per file** (87,518 calls for 6,234 files; 1.79
  million for 166,187 files, 10.8 per file): open, write, close and stat on
  the file, plus its directory entry. On a mount that charges a request per
  namespace call these are the cost, and nothing in the settings reduces them
  except not materialising the files (sparse checkout, a snapshot).
- **Renames are rare** (19 in a clone, 1 to 3 in the other operations): git
  writes checked-out files in place and renames only its lock files and packs.
  The mount's rename (issue #28) is still on the path of every operation,
  because every operation ends with one.
- **fsync is rare.** 13 in the whole sequence on the medium repository: 6 in
  the clone, 4 in the `fetch`, 3 in the first `switch` (each fetch of objects
  writes a pack and its index and syncs both, and a partial clone makes a
  second fetch). Everything git does by default after that, including staging
  and committing, is fsync-free: git's default does not sync loose objects.
- **`status` reads every tracked file's metadata every time** (9,200 stats and
  2,866 `getdents` calls for 6,234 files) unless the untracked cache is on.

On tmpfs the clone takes 1.31 s and the sequence 1.96 s (sums of wall minima);
on the disk 1.96 s and 3.2 s. The difference is the disk: its 6 ms fsync and its
journal.

## 4. The settings, one change each

Medium repository, local NVMe, five repetitions per setting. "Calls" are
deterministic; CPU is the median of the sequence and has about 10% of noise;
wall time is not used to rank settings because of the machine's load.

| setting | what changes | measured effect | verdict |
|---|---|---|---|
| no `--filter=blob:none` (whole-tree clone) | the commit's files come with its trees in one fetch instead of two | clone 87,518 to 73,150 calls (-16%), fsync 6 to 3, CPU -22 to -43% (sequence 4.9 s to 2.8 s in the sweep, 4.9 s to 3.5 s interleaved), 5 HTTP requests to 3; large repository clone 21.7 s to 13.1 s, CPU 26.5 s to 17.1 s | **adopt** |
| `core.untrackedCache=true` | later `status` skip directories that did not change | second status 15,657 to 9,609 calls (-39%), `getdents` 2,866 to 4, `openat` 1,797 to 44, wall minimum 31 to 9 ms; the first status costs 6,100 calls more (+34%) to build the cache | **adopt** |
| `feature.manyFiles=true` | untracked cache and index version 4 | the same as `core.untrackedCache`; index v4 adds nothing (`index.version=4` alone: no change in calls) | no, use the one setting |
| `gc.auto=0`, `maintenance.auto=false` | no automatic gc in the run | nothing in an ordinary run (commit -2.6% calls); with 8,000 staged files: `commit` 735 to 60 ms (NVMe, 12x), 1,674 to 386 ms (tmpfs, 8.8 ms fsync, 4.3x) | **adopt** |
| `core.fsync=none` | no fsync at all | 13 to 0 in the sequence; wall effect within noise (13 x 6 ms = 78 ms of 3 s) | no (section 5) |
| `core.fsync=committed`, `all`, `core.fsyncObjectFiles=true` | fsync every loose object | fsyncs 13 to 81, 87, 75; `add` 0 to 26 and `commit` 0 to 37; 8,000 staged files: 8,000 fsyncs | **never** |
| `core.fsync=committed` + `core.fsyncMethod=batch` | one flush per batch of objects | `add` 26 to 1, `commit` 37 to 3, 8,000 staged files: 8,000 to 1 fsync; +500 calls per `add`; on this disk `churn_add` 0.5 s to 6.9 s (min) for the writeout | only if loose objects must be durable |
| `checkout.workers=0` (all cores), `=4` | parallel checkout | medium tree: `write` calls 14,248 to 62,247 (4.4x), CPU +3%, no shorter; large tree: 373,000 to 1.06 million writes, CPU 17.1 s to 20.2 s, no shorter on local disk (minimum 13.1 s without, 17.9 s with). With 100 us injected into every open, create, rename, unlink and directory read (`-calls`), the large clone takes 55.8 s without and 28.9 s with (minimum of four, interleaved); a second run, three repetitions not interleaved, gave 57.0 s and 53.3 s for the same pair and 68.9 s and 41.8 s with the filter kept; the medium clone 3.6 s and 2.0 s, but the medium `switch` of 2,500 files 0.9 s and 2.2 s | not set; decide on the mount (10) |
| `core.fsmonitor=true` | file system monitor | no effect: git 2.43 on Linux has no built-in daemon (`fsmonitor--daemon not supported on this platform`) and ignores the setting silently; the hook form needs an external watcher, none is installed | no |
| `core.preloadIndex` true/false | threaded index refresh | none (it is on by default) | no |
| `pack.threads`, `index.threads` 1 / 8 | threads for packing and index | none | no |
| `fetch.writeCommitGraph=true` | commit graph after fetch | none | no |
| `transfer.unpackLimit=1` | keep every fetched pack as a pack | clone writes 13,202 vs 14,248 calls; nothing else | no |
| `core.looseCompression=0`, `pack.compression=0` | no zlib | none | no |
| `core.splitIndex=true` | split index | `switch` -17% calls; nothing else | no |
| `index.version=4` | index version 4 | none | no |
| `GIT_OPTIONAL_LOCKS=0` | `status` does not refresh the index file | second status 15,657 to 19,444 calls (+24%): it re-reads what it would have remembered | **never** |
| sparse checkout, cone of 3% of the tree | check out part of the tree | medium: whole sequence 243,926 to 43,028 calls (-82%), wall minimum 3.2 s to 0.44 s on NVMe and 2.0 s to 0.29 s on tmpfs, the `switch` of 2,500 files touches the 3% of them in the cone. Large: the pack still has to be received and indexed, so the clone is not shorter (20.8 s to the first command against 13.4 s without a cone, then 886,000 calls for the whole sequence against 3.30 million without a cone) | a mode, not a default: when the task names its directories |

Not recommended, with the reason measured: nothing that changes how objects
are *stored* matters to a run, because a run stores little; what matters is how
many files git creates and how often it asks the directory what it holds.

## 5. What fsync costs: 20 us, 4 ms, 8.8 ms

A run lands on one of three disks. fsync of a new file costs about 20 us when
it is acknowledged from a local journal (the volume's design), 4 ms on a
typical SSD, and 8.8 ms when it waits for a conditional write to object
storage. The local NVMe measured here is in the second class (6.0 ms, section
2), tmpfs is below the first. Git's cost in each class is the number of fsyncs
it makes times that price, since a run is otherwise the same:

Cost added to one run (the sequence of section 3):

| setting | fsyncs | at 20 us | at 4 ms | at 8.8 ms |
|---|---|---|---|---|
| default | 13 | 0.3 ms | 52 ms | 114 ms |
| whole-tree clone | 7 | 0.1 ms | 28 ms | 62 ms |
| `core.fsync=none` | 0 | 0 | 0 | 0 |
| `core.fsync=committed` + `core.fsyncMethod=batch` | 22 | 0.4 ms | 88 ms | 194 ms |
| `core.fsync=committed` (loose objects too) | 81 | 1.6 ms | 324 ms | 713 ms |
| `core.fsync=all` | 87 | 1.7 ms | 348 ms | 766 ms |
| `core.fsyncObjectFiles=true` | 75 | 1.5 ms | 300 ms | 660 ms |

The same run when the agent also stages 8,000 untracked files:

| setting | fsyncs | at 20 us | at 4 ms | at 8.8 ms |
|---|---|---|---|---|
| default | 20 | 0.4 ms | 80 ms | 176 ms |
| `core.fsync=none` | 4 | 0.1 ms | 16 ms | 35 ms |
| `core.fsync=committed` + `batch` | 34 | 0.7 ms | 136 ms | 299 ms |
| `core.fsync=committed` | 8,093 | 162 ms | 32 s | 71 s |

The counts are `strace` counts (exact). The times are the counts times the
price, and two of them were measured as well:

- **Staging 8,000 files with a loose-object fsync, on this disk (6 ms):**
  `add -A` took a median 71.9 s (minimum 53.0 s) against 1.8 s (0.5 s) for the
  default; 8,000 fsyncs.
- **The same on tmpfs with 8.8 ms injected into every fsync**
  (`gitbench run -delay 8800us`): median 128 s, minimum 76.9 s, against 0.34 s
  for the default and 0.70 s for `core.fsyncMethod=batch` (one fsync).

What matters in each class:

- **20 us.** Nothing. The worst setting costs 1.7 ms per run and 160 ms for
  8,000 staged files. Do not tune for it.
- **4 ms and 8.8 ms.** One setting: never sync loose objects. Everything else
  is below the run's own noise: git's default 13 fsyncs cost 52 and 114 ms of
  a run that takes 3 seconds or more, and `core.fsync=none` would save them at
  the price of leaving a pack unsynced after a power loss, which in a run whose
  work is pushed buys at most 4%. With the loose-object setting on, a run that
  stages a vendored directory spends a minute waiting for its disk. If
  durability of loose objects is wanted, `core.fsyncMethod=batch` turns the
  8,000 fsyncs into 1 (with `sync_file_range` on each file, which a volume must
  make cheap) and costs about 500 extra calls per `add`.
- **A whole-tree clone** has 7 fsyncs against 13: the second fetch of a partial
  clone writes and syncs another pack and index.

fsync on the mount is the one number to re-measure before choosing between
`default` and `core.fsync=none`: `gitbench probe -dir <mount>` prints it in a
second; if it is above 4 ms and the work is delivered by `push`, `none` is a
legitimate choice for a disposable sandbox, worth at most 4% of the run.

## 6. Starting a run without downloading or writing the repository

### 6.1 Where a clone's time goes

The large repository clones in 21.7 s (minimum; 23.1 s median, 26.5 s of CPU).
Most of that is not the checkout. It is receiving the pack, inflating and
hashing every object in it (`index-pack`), then reading each object back to
write the file. The same clone, borrowing the objects from a store on the same
disk, takes 4.7 s (minimum; 7.3 s median, 13.4 s of CPU), with the same 166,187
files written:

| clone of 166,187 files (NVMe, five repetitions) | wall min | wall median | CPU | `.git` written | files written |
|---|---|---|---|---|---|
| current: `--depth 1 --filter=blob:none` | 21.7 s | 23.1 s | 26.5 s | 309 MB | 166,187 |
| whole tree: `--depth 1` | 13.1 s | 26.5 s | 17.1 s | 296 MB | 166,187 |
| whole tree, `--reference` (objects borrowed) | 4.7 s | 7.3 s | 13.4 s | 19.7 MB | 166,187 |
| whole tree, `--reference --dissociate` (borrowed, then copied) | 6.8 s | 9.8 s | 15.8 s | 296 MB | 166,187 |
| whole tree, sparse cone of 3% (clone, then cone) | 19.5 s + 0.8 s | 25.3 s + 1.2 s | 15.6 s + 0.9 s | 296 MB | 13, then the cone's |

(The reference rows ran with `checkout.workers=0` as the whole-tree row did;
that setting changes the wall time by less than the spread of repetitions.)
Time to the first command, clone plus the first `git status` (minimum of five):
22.0 s today, 13.4 s without the filter, 5.6 s borrowing (7.5 s dissociating),
20.8 s with a sparse cone, and a mount plus one `status` when the checkout is
a snapshot (6.3).

What a reference clone writes is the index (19.7 MB for 166,187 entries),
refs and configuration; its object count is 0. What it still writes is every
file of the working tree, 2.3 GB here. Only a snapshot of the checkout avoids
that.

### 6.2 Alternates: a shared read-only object store

Mechanism: one bare repository per repository, kept current by a single
updater (`git fetch` into it, on the forge's push event, never polling), mounted
read-only at the same absolute path in every sandbox. A run clones with
`--reference-if-able <store> --depth 1 --single-branch`; git records the store in
`objects/info/alternates` and fetches from the forge only what the store lacks
(its refs are offered to the server as objects the client already has).

What is tested (`tools/gitbench`, `shared_test.go`, each a failing test when its
behaviour is removed):

- **Zero objects written.** After a reference clone `git count-objects -v` is
  `count: 0, in-pack: 0`; a plain clone has objects. `fsck` and `status` pass.
- **gc is safe.** After committing in the clone, `git gc --prune=now` leaves the
  alternates file, copies none of the store's objects (the clone holds the 3
  objects of its own work) and `fsck` passes: `gc` repacks with `-l`.
- **Push works.** Branch created in the clone, pushed to the origin: the origin
  has the commit and passes `fsck`.
- **A read-only store works.** With every file of the store `0444` and every
  directory `0555`, switch, add, commit, gc, fsck and push all succeed.
- **What breaks: pruning the store.** If the store loses objects the clone
  borrowed (all its refs deleted, then `gc --prune=now`), the clone's `fsck`
  fails. This is the one hazard, and it is a rule (8.2.4): a store is replaced,
  never pruned in place while a borrower lives. A run lasts hours and a store is
  replaced rarely; a sandbox that outlives its store dissociates first.
- **The way out.** `git repack -a -d` in the clone (no `-l`) copies the
  borrowed objects in; after removing the alternates file the clone survives the
  store's deletion, as does one made with `--dissociate`.

Also: the alternates file holds an absolute path, so the store must be mounted
at the same path in every sandbox (and in a snapshot taken from one); a shallow
clone stays shallow; the clone's `git fsck` is clean without the store only
after dissociating.

### 6.3 A snapshot of the checkout, cloned in constant time

A saved environment is a volume snapshot: the checkout of a repository at a
commit, with its `.git`, taken once and cloned for every run. The volume layer
already addresses data by content within an organisation, so a clone shares
every chunk with its source and copies none; the clone is a new volume whose
head names the same tree.

What it needs from the volume (not built yet): `Snapshot` (record the head of a
finished checkout) and `Clone` (a new volume at that head; a new writer round;
no data copied), reference counting so that deleting the source does not free
what clones still use, and one property of the clone for git: **the files must
keep their size, mode and modification time to the nanosecond, and ideally
their inode numbers and change times.** Git decides whether a file changed by
comparing exactly those stat fields with what its index remembers; where they
differ it reads the whole file to be sure.

`gitbench snapshot` measures that cost with `cp -a`, a copy that keeps
contents and modification times and gives every file a new inode and change
time, exactly what a clone that does not preserve them gives:

| checkout | copy with `cp -a` (a clone with no copy-on-write) | first `status`, git's defaults | first `status`, `core.checkStat=minimal` `core.trustctime=false` | later `status` |
|---|---|---|---|---|
| medium: 6,234 files, 124 MB | 228 ms, 128,437 calls | 312 ms, 43,529 calls, 9,464 opens | 29 ms, 15,637 calls, 1,794 opens | 26 ms |
| large: 166,187 files, 2.3 GB | 7.3 s, 3.07 million calls | 6.29 s, 970,324 calls, 202,224 opens | 0.54 s, 266,729 calls, 20,448 opens | 0.44 s |

Where the clone cannot preserve inode numbers and change times, setting
`core.checkStat=minimal` and `core.trustctime=false` before the first status
makes git compare size and modification time only (8.3.3).

### 6.4 The design that fits: Disk, visor, forge

The stack is a vfs volume on the organisation's bucket (the Disk), a visor
sandbox that mounts it (the runtime), and the forge as `origin`. One run starts
as follows, each step falling to the next if it fails:

1. **Snapshot.** An updater keeps one snapshot per (organisation, repository,
   default branch): on the forge's push event it fetches into the saved
   checkout, runs `git status` once to refresh the index, flushes the volume
   and records the head. A run is a `Clone` of the latest head, mounted at the
   working directory; its first command is `git fetch --depth 1 origin <base>`
   and `git switch` to the base commit, which moves a few files. The
   repository's configuration in the snapshot names no credential: the run's
   remote and credential are set at lease time. Cost: one head object written,
   the mount, the fetch; independent of the repository's size.
2. **Borrowed store.** If there is no snapshot, the run clones with
   `--reference-if-able` against the node's store of that repository (the
   updater keeps one per node for the repositories it has seen), mounted
   read-only into the sandbox at a fixed path. Cost: the checkout's file
   writes, none of the pack's.
3. **Network clone.** Otherwise, the whole-tree shallow clone with the settings
   of section 1.

The three differ in what they save, in the large repository: the network clone
13.1 s and a pack's 296 MB written; the borrowed store 4.7 s and 20 MB; the
snapshot nothing but the head and the fetch. Keys and isolation are the
volume's: an organisation's snapshots are encrypted with its key and cloned only
inside the organisation, so no repository's contents are shared across
organisations and the store of a private repository is as private as the
repository.

What the design does not do: it does not make the first run of a repository
fast (someone has to clone it once), and it does not help a repository nobody
has run on; for that the whole-tree clone and a faster forge (section 7) are
the remedy.

## 7. The forge

What a clone asks of the forge, and what the forge does about it, from the
forge's source and from the client side (`GIT_TRACE_PACKET`, `GIT_TRACE_CURL`,
`GIT_TRACE2_PERF`). The forge is not changed here.

### 7.1 What the forge does today

Read from the forge's source: smart HTTP serves a clone with three requests,
each handled the same way: look up the owner and repository in the database,
check the caller's permission, and run `git upload-pack` as a child process in
the repository directory (`--advertise-refs` for the discovery request,
`--stateless-rpc` for the two POSTs). Nothing is cached between requests: the
discovery output is collected in memory before the first byte is sent. The
server's git advertises protocol version 2 with `shallow`, `filter`,
`wait-for-done` and `server-option`; it does not advertise `bundle-uri` or
`ref-in-want`. Every repository is configured with `uploadpack.allowFilter` and
`uploadpack.allowAnySHA1InWant`, `core.commitGraph`, `gc.writeCommitGraph` and
`fetch.writeCommitGraph`. The scheduled `git gc` of every repository ships
disabled (`git_gc_repos`, off, 72 h when on, no arguments), so a repository is
repacked only when git's own `gc --auto` runs after a push crosses its
thresholds (6,700 loose objects, 50 packs); no pack bitmap is written unless
that happens.

### 7.2 A clone, request by request

`gitbench net` on a clone of a small public repository (135 files) over HTTP/2,
five repetitions each, time from sending a request header to receiving the
first response header (median; minimum in brackets):

| request | partial clone (current) | whole tree |
|---|---|---|
| 1 discovery `GET info/refs` | 448 (298) ms | 281 (263) ms |
| 2 `POST` reference listing | 107 (96) ms | 114 (104) ms |
| 3 `POST` fetch of the commit and trees (or files) | 146 (121) ms | 147 (136) ms |
| 4 discovery again, on a second connection | 382 (255) ms | none |
| 5 `POST` fetch of the blobs | 199 (175) ms | none |
| whole clone | 2,123 (1,621) ms | 1,786 (1,476) ms |

A partial clone sends five requests and makes a second connection, because the
checkout needs every blob and git fetches them in a second, separate step. A
whole-tree clone is three requests. The saving is the second discovery plus the
blob fetch's first byte: about 0.5 s here.

On a large mirrored repository (2.5 GB of history, a checkout of 30,798 files),
three repetitions each, minimum in brackets:

| request | partial clone (current) | whole tree |
|---|---|---|
| 1 discovery | 1,606 (1,556) ms | 1,784 (1,650) ms |
| 2 reference listing | 107 (106) ms | 116 (105) ms |
| 3 fetch of the commit, trees (or files) | 259 (253) ms | 1,075 (1,013) ms |
| 4 discovery again | 1,755 (1,554) ms | none |
| 5 reference listing again | 105 (97) ms | none |
| 6 fetch of every blob | 2,908 (2,871) ms | none |
| whole clone | 14.4 (13.8) s | 10.2 (9.3) s |
| client CPU | 3.65 s | 3.2 s |

Both fetches of the partial clone together take 3.2 s to first byte against the
whole-tree fetch's 1.1 s, because the blob fetch asks the server to build a
pack from 30,000 explicit object ids; and the second discovery costs as much as
the first. The whole-tree clone is 29% shorter (32% by minimum). A partial
clone is meant to bound clones of large repositories; on this one it does the
opposite.

### 7.3 Discovery is where the time goes

The advertisement for protocol version 2 contains no references; it is the same
178 bytes for every repository. Its time to first byte (`curl`, six requests in a
row to each, `Git-Protocol: version=2`; connection setup, 0.04 to 0.09 s, is
included in every figure):

| repository | kind | size | first request | following requests |
|---|---|---|---|---|
| a static page | | | | 0.07 to 0.12 s |
| small | mirror | 0.6 MB | 0.39 s | 0.32 to 0.57 s |
| 1.4 GB | **not** a mirror | 1.4 GB | 0.34 s | 0.17 to 0.23 s |
| 1.4 GB | **mirror** | 1.4 GB | 21.3 s | 1.7 to 2.3 s |
| 0.8 GB | mirror | 0.8 GB | 28.1 s | 4.3 to 8.0 s |
| 1.3 GB | mirror | 1.3 GB | 8.8 s | 0.65 to 0.69 s |
| 0.9 GB | mirror | 0.9 GB | 2.8 s | 0.69 to 0.75 s |
| 2.5 GB | mirror | 2.5 GB | 10.0 s | 2.0 to 2.5 s |
| 8.7 GB | mirror | 8.7 GB | 125 s, HTTP 524 | 95.8 s |

The two 1.4 GB repositories (one a mirror, one not) differ in one thing, and
the answer takes 0.2 s for one and 1.7 s to 21 s for the other. The first
request after idle is the slowest. The output is 178 bytes either way: nothing
about this request needs the repository. A clone of the 8.7 GB mirror cannot
start reliably: the front proxy gave up on the first request (HTTP 524 after
125 s) and the next one answered after 96 s. A partial clone pays this twice.

The likely causes are in the request path, not in git: a lookup or lock that
mirrors take and plain repositories do not, or the repository being touched at
all by `git upload-pack --advertise-refs` on a cold disk. The measurement does
not distinguish them; the forge's own request timings split by phase (database,
permission, process start, copy) would.

### 7.4 What the server computes

Server compute separated from the client's by `GIT_TRACE2_PERF` (the
`pack-objects` process's own elapsed time), on a 21,441-commit, 6,234-file
repository served from a local bare copy in the state a repository is in after
many pushes (28 packs, 4,348 loose objects), five clones each, minimum shown;
CPU is the clone's user plus system time, client included:

| request | state | pack-objects wall | clone CPU |
|---|---|---|---|
| `--depth 1` | 28 packs | 0.97 s | 1.86 s |
| `--depth 1` | one pack with bitmap | 1.00 s | 1.93 s |
| `--depth 1 --filter=blob:none` (two fetches) | 28 packs | 1.75 s | 2.63 s |
| `--depth 1 --filter=blob:none` | one pack with bitmap | 1.63 s | 2.71 s |
| `--depth 50 --single-branch` | 28 packs | 0.98 s | 2.19 s |
| `--depth 50 --single-branch` | one pack with bitmap | 0.83 s | 1.96 s |
| full history | 28 packs | 9.98 s | 34.3 s |
| full history | one pack with bitmap | 7.37 s | 24.4 s |
| full history, blobless | 28 packs | 7.56 s | 9.2 s |
| full history, blobless | one pack with bitmap | 2.47 s | 5.0 s |

Three things follow. A shallow clone is not helped by repacking: git does not
use reachability bitmaps for a `deepen` request, so a forge that serves coding
runs gains nothing for them from `gc` and should not expect to. A partial clone
costs the server about 1.8 times as much as a whole-tree shallow clone, because
the blob fetch is a second pack built from explicit object ids. Full and
partial clones of whole histories, and fetches, gain a lot from one bitmapped
pack (a quarter to a third less CPU for a full clone, a half for a blobless
one), and the forge's own `gc` is disabled.

### 7.5 Changes, in order of value

1. **Answer protocol-version-2 discovery from a constant.** The capability
   advertisement depends on the server's git and configuration, not on the
   repository. Build it once at start, serve it for any repository the caller
   may read (the permission check stays), and spawn nothing. Gain: the 0.1 to
   0.3 s of a small repository, the 1.7 to 21 s of a mirror, and the clone of
   the largest mirrors that cannot reliably start today; once per whole-tree clone, twice
   per partial one. Test: section 8.4.1.
2. **Take mirror synchronisation out of the read path.** If the cause is a lock
   shared between a mirror's synchroniser and its readers, readers do not take
   it: fetches and clones read a consistent set of refs without waiting. Gain:
   the cold 9 to 28 s.
3. **Cache the repository and permission lookup for the length of a clone.**
   Each of the three requests repeats the same lookups before it starts git.
   The two POSTs take 107 to 146 ms against a round trip of 12 to 20 ms and
   30 to 50 ms of server time for a static page, so most of the rest is lookup
   and process start. One lookup per clone, keyed by repository and caller,
   expiring in seconds, would save part of that on each request (an estimate,
   up to about 170 ms of the clone; the forge's request timings would settle it).
4. **Enable the scheduled `gc` with bitmaps and the commit graph** for
   repositories that are cloned in full or partially (and for the fetches of
   every mirror). A quarter to a half of the CPU of those requests. It does
   nothing for `--depth 1`.
5. **Do not do for coding runs:** repacking for shallow clones (no effect,
   measured); `ref-in-want` or `bundle-uri` (they help fetches into an existing
   history, not the one-commit clone, which section 6 removes instead).

## 8. Specification notes

Normative behaviours that a third party can implement and test, with the
measurement that decides each. Words in capitals are used as in RFC 2119.
These notes extend the git repository hosting specification (HIP-1232) and the
volume specification that describes the disk a run mounts.

### 8.1 A run client

A *run client* is whatever puts a repository into a sandbox's working
directory: it acquires one commit of one branch, then the sandbox's tools
(an agent, a shell) use git on it.

1. **One commit, one branch.** The client MUST request a single commit of a
   single branch (`--depth 1 --single-branch`), and MUST NOT download history it
   was not asked to read.
2. **Files with the commit.** When the working tree will hold every file of the
   commit, the client MUST NOT request a partial-clone filter. The commit's
   blobs travel in the same fetch as its trees, so acquiring the repository is
   one reference discovery, one reference listing and one fetch (three HTTP
   exchanges over smart HTTP). A filter MAY be requested only when the client
   checks out a subset of the tree (a sparse cone) or never reads file contents.
   *Test:* clone a fixture over HTTP with `GIT_TRACE_CURL`; the number of
   request headers sent is 3, and the clone's repository configuration has no
   `remote.<name>.promisor`.
3. **Settings are written at clone time.** The client MUST set the repository's
   own configuration (`git clone -c key=value`) rather than the user's, so that
   every later command inherits it and no other repository is affected:
   `core.untrackedCache=true`, `gc.auto=0`, `maintenance.auto=false`.
   *Test:* after the clone, `git config --local --get` returns each value.
4. **Untracked cache.** With `core.untrackedCache=true`, a second `git status`
   on an unchanged tree MUST read no directory: its `getdents` count is at most
   1% of the first status's, and its total system calls are at most 65% of the
   first's. *Test:* the `status_cold` and `status_warm` rows of `gitbench run
   -configs untracked` (measured: 17,791 and 9,609 calls on a 6,234-file tree
   against 15,657 for the second status without the cache).
5. **No fsync per object.** The client MUST NOT enable `core.fsync` for loose
   objects (`loose-object`, `committed`, `all`) or `core.fsyncObjectFiles` unless
   the disk's fsync completes in under 100 us. With the default, staging N new
   files performs 0 fsyncs for every N. If durability of loose objects is
   required, `core.fsyncMethod=batch` MUST be set with it, and staging N files
   then performs at most 3 fsyncs for every N. *Test:* `gitbench run -churn
   8000`: `churn_add` fsync count is 0 for the default and at most 3 for batch.
6. **Parallel checkout.** A run client MAY set `checkout.workers=0` on a disk whose directory calls
   (open, create, rename, unlink, list) each take 50 us or more, and MUST NOT
   elsewhere. *Test:* `gitbench run -configs whole,whole+workers` on the disk; the
   setting is kept only if the clone is at least 20% shorter (minimum of five)
   and `switch` is not slower. Measured with 100 us injected: the large clone
   55.8 s to 28.9 s in one run and 57.0 s to 53.3 s in another, the medium
   `switch` 0.9 s to 2.2 s (so, mixed); on local disk, no gain.
7. **Delivery.** The run's work is delivered by pushing its branch to the
   forge; a run client MUST NOT rely on the sandbox disk for anything that is
   not pushed once the disk's last durable publication is older than the
   publication bound of the volume.

### 8.2 Clone by reference

1. **The store.** A shared object store is a bare repository holding the
   objects of one repository of one organisation. It is written by exactly one
   updater (a fetch into it) and mounted read-only into sandboxes at the same
   absolute path in every sandbox.
2. **Borrowing.** A borrowing clone has `objects/info/alternates` naming the
   store's `objects` directory by absolute path. Right after the clone, its own
   object count (`git count-objects -v`: `count` plus `in-pack`) MUST be 0 when
   the store holds the commit, and the clone's `fsck` MUST pass.
3. **Local maintenance only.** Maintenance in a borrowing clone MUST be
   local-only (`git gc`, which repacks with `-l`). It MUST NOT repack borrowed
   objects into the clone unless it is dissociating.
4. **Retention.** A store MUST NOT delete an object reachable from a ref of any
   borrower that is not dissociated. Replacing a store means: end or dissociate
   every borrower (`git repack -a -d`, then remove the alternates file), then
   remove the old store. A borrower of a store that lost its objects is broken
   (`fsck` reports missing objects); this is by design and is tested.
5. **Push.** A borrowing clone pushes like any other, and the receiving
   repository passes `fsck` after it.
6. **Read-only.** Every command of a run works when the store is read-only.
7. **Fallback order.** The client tries a volume snapshot, then a borrowed
   store, then a network clone; each failure falls to the next without failing
   the run.

### 8.3 Volume snapshot clone

1. A snapshot of a checkout preserves, for every file, its contents, size,
   mode and modification time to the nanosecond. It SHOULD preserve inode
   numbers and change times.
2. The clone is constant-time in the size of the checkout: its cost is a fixed
   number of requests, independent of the number of files, and it copies no
   data. *Test:* clone snapshots of a 6,000-file and a 165,000-file checkout; the
   request counts are equal.
3. Where inode numbers or change times are not preserved, the run client MUST
   set `core.checkStat=minimal` and `core.trustctime=false` before the first
   `git status`. Without them the first status reads every file
   (6.29 s against 0.54 s for a 166,187-file checkout, 312 ms against 29 ms for a 6,234-file one).
4. A snapshot MUST NOT contain a credential: the repository configuration it
   carries names no token and no key.

### 8.4 A conforming forge

1. **Constant discovery.** A request for the reference advertisement with
   protocol version 2 (`GET info/refs?service=git-upload-pack` with
   `Git-Protocol: version=2`) MUST be answered without reading the repository:
   the advertisement is a constant of the server's software and configuration.
   The server's own time for it (first byte minus connection setup) MUST be at
   most 50 ms at the 95th percentile for every repository, whatever its size
   and whether it is a mirror. *Test:* 30 requests per repository of each size
   class; today a repository of 600 KB takes 270 ms, a mirrored 1.4 GB repository
   1,700 to 21,000 ms, a mirrored 8.7 GB repository 96,000 ms and more (section 7).
2. **Capabilities.** The advertisement offers `shallow`, `filter` and
   `ls-refs` with `unborn`, and the server MUST honour `deepen 1` and
   `filter blob:none`.
3. **Per-request overhead.** Each of the two POST requests of a clone (reference
   listing, fetch) MUST reach its first byte within 60 ms of the request plus
   the server's own compute for that request. *Test:* a repository of one file;
   the time to first byte of each POST is at most 60 ms above the network round
   trip.
4. **Maintenance.** After a repository's pack count exceeds 50 or its loose
   objects exceed 6,700 (git's own `gc.autoPackLimit` and `gc.auto`), the server MUST repack it into
   one pack with a reachability bitmap and write the commit graph. *Test:* a
   full clone of the fixture costs at most 75% of its CPU before the repack
   (measured: 34.3 s to 24.4 s of CPU for a 21,441-commit history; blobless
   9.2 s to 5.0 s).
5. **Independence from synchronisation.** Reading a mirror MUST NOT wait on the
   mirror's synchronisation.
6. **Shallow cost.** A `deepen 1` fetch is not accelerated by bitmaps (measured
   1.86 s and 1.93 s of CPU with 28 packs and with one bitmapped pack); repacking is
   therefore not a remedy for shallow clones, and a service that must make them
   cheaper does it with section 8.2 or 8.3.

### 8.5 References

- git-clone(1), `--depth`, `--single-branch`, `--filter`, `--reference`,
  `--dissociate`; gitrepository-layout(5), `objects/info/alternates`.
- git-config(1): `core.untrackedCache`, `core.fsync`, `core.fsyncMethod`,
  `checkout.workers`, `core.checkStat`, `gc.auto`, `maintenance.auto`.
- gitprotocol-v2(5), `info/refs` discovery, `ls-refs`, `fetch` with `deepen`
  and `filter`.
- git-sparse-checkout(1), cone mode.

## 9. Reproduce

`tools/gitbench` is one Go program with no dependency beyond git (and `strace`
for the system-call columns and for the injected latencies). It prints CSV and
runs unchanged on any directory:

```sh
go build -o gitbench ./tools/gitbench

# what the directory's calls cost, with no git in it
gitbench probe -dir /mnt/disk

# the sequence, one row per operation per repetition; -src is only read
gitbench run -src ~/work/some/repo -dir /mnt/disk -reps 5 \
    -configs default,untracked,whole,whole+untracked+gcoff > run.csv

# the same with a price on fsync, and on namespace calls
gitbench run -src REPO -dir /dev/shm -delay 8800us -configs default,fsyncloose,batch
gitbench run -src REPO -dir /dev/shm -calls 100us -configs default,whole

# 8,000 staged files: gc, and an fsync per object if the config asks for it
gitbench run -src REPO -dir /mnt/disk -churn 8000 -configs default,fsyncloose,batch,gcoff

# a copy of a finished checkout: what git does when inode numbers change
gitbench snapshot -src REPO -dir /mnt/disk

# a clone over HTTP, request by request
gitbench net -url https://forge.example/org/repo -reps 5
gitbench net -url https://forge.example/org/repo -filter ""

# median and minimum per (filesystem, repository, config, delay, operation)
gitbench summary < run.csv
```

`docs/git.csv` holds the summary of every run behind this document, one row
per (run, filesystem, repository, config, operation), repositories relabelled
medium, large and mirror.

The `files` column counts regular non-executable files of the tree at the base
commit (6,142 and 165,230); a checkout holds 6,234 and 166,187.

Columns: `host,fs,git,repo,files,config,delay_us,calls_us,op,rep,wall_ms,cpu_ms,load1,exit`,
then the system calls of the counted pass (`fsync,fdatasync,syncrange,rename,openat,unlink,stat,getdents,write,syscalls`),
then `git_kb` and `tree_files` (storage a clone cost), `req,ttfb_ms,proto` (net)
and `note` (why an operation failed). The file system is read from the mount
table, so the same command on a mounted volume labels its rows with the mount's
type. A row with `exit` not 0 is kept in the CSV and ignored by `summary`.

`-configs` takes the names in `knob.go`; `a+b` combines them; `all` runs each
once. The runner repeats configs in turn (repetition outermost) so that a change
in the machine's load lands on all of them. Scratch lives under `-dir` and is
removed unless `-keep`. `-strace off` skips the counted pass; `-until OP` stops
the sequence after an operation.

Tests: `go test -race ./tools/gitbench` (about 40 s; they run real git against
repositories they build). They cover the strace parser, the knob catalogue
(every entry reaches git), the fixture, the sequence end to end including the
push, the failure paths, the latency injection command lines, the CSV, the
summary, and the shared object store behaviours of section 6 (borrow, gc,
prune, dissociate, push, read-only).

## 10. Not done

- **Nothing was measured on the mount.** It cannot run git until issue #28.
  Run `gitbench probe`, `run` and `snapshot` on it unchanged; the settings of
  section 1 are chosen from counts and CPU, which transfer, and the decision
  rules below are stated for the numbers that do not.
- **`checkout.workers`** did not shorten a clone on local disk and tripled its
  write calls (1.06 million against 373,000 for the large repository). With a
  price of 100 us on directory calls it shortened the large clone in four of
  six comparisons, by 6% to 48%, and made the 2,500-file `switch` slower. On a
  mount whose calls are requests to a daemon it might help; the rule: adopt it if
  `gitbench run -configs whole,whole+workers` on the mount shows a clone at
  least 20% shorter and no slower `switch`.
- **No cold-cache runs.** Dropping the page cache needs privileges the host
  did not give; every repetition read a warm origin.
- **The host was shared.** Wall times are indicative; calls and CPU are what
  the conclusions rest on. The CSVs carry the load of every row.
- **fsmonitor** has no built-in daemon in git 2.43 on Linux and no external
  watcher was available; a watcher-backed hook was not measured.
- **The forge** was measured from outside and read, not instrumented; the
  causes in section 7.3 are inferences from the pattern of timings.
- **`bundle-uri` and `packfile-uris`** (a pre-built tip pack served from object
  storage) were not tried; the reference clone and the snapshot remove the same
  cost without a server feature.
