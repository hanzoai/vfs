# Journal

`github.com/hanzoai/vfs/journal` is an append-only log on the node's local disk.
It lets a write acknowledgement and an fsync run at the speed of local NVMe while
the object store catches up behind it. It is a standalone package: it knows
nothing of volumes, chunks or heads. A later change connects it to the Volume
(see [Integration](#integration-with-the-volume)).

Refs hanzoai/vfs#9 (journal and group fsync), #12 (backpressure), #26 (crash
exploration), and the ordering contract of #10 (watermarks).

## The contract

Every record has a sequence number: 1, 2, 3, ... assigned in the order Append
admits them, with no gaps. A record moves through three watermarks.

| Watermark | Means | Reached by |
|---|---|---|
| `Visible` | the record exists; Replay in this run returns it | `Append` returning |
| `Durable` | the record is on the local disk's media | `Sync(seq)` returning |
| `Published` | the store holds the record | `Publish(seq)` |

`Published <= Durable <= Visible`, and each only moves forward. The publisher may
only publish what is durable: `Publish` refuses a sequence no local sync has
covered. The store therefore never holds a record the journal's disk could still
lose, and the sequence the store reports is a fact `Open` can rely on.

| Call | Guarantees |
|---|---|
| `Open(dir, Options)` | Takes an exclusive lock on the directory (`ErrLocked`), refuses a directory on memory (`ErrVolatile`), recovers the longest valid prefix of the log, makes it durable, and starts a fresh segment for new records. `Options.Published` is the highest sequence the store holds (the Head's journal sequence, 0 for a store with none): it is the truth, and Open **refuses**, and writes nothing, when it does not fit: `ErrMismatch` (the store is behind what the journal reclaimed, behind its first record, or ahead of its last), `ErrLineage` (the directory belongs to another identity), `ErrCorrupt` (records that were made durable are gone or damaged), `ErrFormat` (a segment file that is not in this layout). See [Recovery](#recovery). |
| `Recovery()` | What Open kept (`First`..`Last`), what a surviving mark proves was durable (`Proven`), the valid records it gave up (`Dropped`: the intact segments that follow a gap, and the records behind a damaged payload), and warnings about the file system. Damage to a record's header ends the log there without a trace: what follows it cannot be found, because its length is not to be believed. |
| `Discard(dir, Options, first, last)` | The only way to give records up. It removes the journal's records if and only if they are exactly `first` through `last` (with any copy of a segment inside that range), and returns `ErrRange`, changing nothing, if anything else is there, such as intact segments beyond a hole. It does not check the lineage: it exists to remove what `Open` refused. |
| `Append(ctx, p) (seq, err)` | The record is **visible** when it returns, in order, with `seq = previous + 1`. It is **not durable**. Near capacity it slows; at capacity, or when no segment file can be made, it returns `ErrFull` and changes nothing. A payload larger than a segment less 80 bytes gives `ErrTooLarge`. |
| `Sync(ctx, seq)` | Returns `nil` only when record `seq` and every record before it are durable. Concurrent calls share one fsync. A call is released only by an fsync that **began after** its record was written; an fsync already in flight never releases it. After a failed write or sync it returns the error for anything not yet durable, forever. |
| `Marks()` | The three watermarks. |
| `Publish(seq)` | Records that the store holds `seq` and everything before it. Monotonic (a lower value is ignored). `seq` must be durable: otherwise `ErrAhead` and nothing changes. Moves the `Due` deadline to the next unpublished record. Does no I/O. |
| `Replay(fn)` | Calls `fn` for every record after `Published`, in order, with no gaps, then returns. It changes nothing: replaying twice yields the same records. Records of the current run are included. It blocks `Reclaim` while it runs. |
| `Reclaim()` | Makes the space of fully published segments available again. Does file I/O, so the publisher calls it after `Publish`, off the path of `Append` and `Sync`. Never touches an unpublished record or the tail segment. A failed write of the floor is made again by the next call; a failed sync of it fails the journal, because a sync that is repeated after a failure can succeed without the data. |
| `State()` | `Backlog` (bytes acknowledged and unpublished), `Age` (how long the oldest unpublished record has been acknowledged; later writes never reset it; a restart counts a record as acknowledged no later than the restart, so against a clock found behind where it was the age starts again), `Degraded`, and the `Err` that stopped the journal, if any. |
| `Due(ctx, age)` | A channel closed when the oldest unpublished record has been acknowledged for `age`. One timer set for the exact deadline; no polling. Cancelling `ctx` releases it. Two firings are at least `Options.Retry` apart. It also closes when the journal closes or fails. See below. |
| `Close()` | Makes everything appended durable, writes the final mark, releases the directory, then stops. |

### Lineage

Sequence numbers mean something only inside one history of a volume. A node that
lost a takeover still holds acknowledged, unpublished records 9 and 10 of its
own; the new owner's Head says 8. `Options.Lineage` is an opaque identity the
caller supplies (a volume and its fencing round). It is written into every
segment header and mixed into every record and mark checksum. `Open` refuses a
directory whose segments carry another identity, so the old owner's records are
never replayed over a newer Head; the operator gives them up with `Discard`, or
keeps them for forensics.

### Generation ordering

The rule that a `Sync(N)` is never satisfied by a sync that started before record
`N` was written is what an fsync caller relies on. The syncer takes a snapshot of
"written so far" and the list of files holding it, syncs those files, and only
then releases the waiters at or below the snapshot. Waiters that arrive during a
sync form the next group, so the group size follows the load with no timer. The
pipeline has a writer goroutine and a syncer goroutine: the next batch is written
while the previous one is being synced.

### The 5 s bound is per record

`Due(ctx, age)` fires when the oldest unpublished record, measured from **its
own** acknowledgement, is `age` old. It is never reset by later writes. When
`Publish` retires the oldest record the deadline moves to the next one; with
nothing unpublished there is no timer, and the deadline starts with the next
`Append`. The publisher loop is

```go
for {
    c, cancel := context.WithCancel(ctx)
    select {
    case <-j.Due(c, bound-margin):   // the deadline
    case <-dirtyBytesOrFlush:        // an earlier trigger
    }
    cancel()                         // releases the timer if Due did not fire
    seq := <the durable generation the publisher is about to commit>
    commit the Head containing seq
    j.Publish(seq)
    j.Reclaim()
}
```

If the commit fails, the next `Due` for the same overdue record fires `Retry`
(default 100 ms) after the last firing, not at once, so the loop backs off
without a sleep of its own. `Due` closes when the journal fails, so the publisher
learns of it at once.

Acknowledgement times come from the injected `Clock`. In the process they are
read from the clock's monotonic reading, so a step of the wall clock does not
change an age. On disk they are wall-clock nanoseconds, because that is all that
survives a restart; a restarted journal clamps a record that appears to be from
the future (the clock is behind what it was) to the time of the restart. That
bounds the time a record has left by the whole bound, counted from the restart; it
does not keep the time it had. A record acknowledged 4.9 s before a restart on a 5 s
bound has 0.1 s left, and a restart that finds the clock an hour behind gives it 5 s
again; a clock ahead of where it was gives it less. Times are
kept to a millisecond: records acknowledged within a millisecond of each other
share a deadline, which can make `Due` fire up to a millisecond early, never late.

### Backpressure and the degraded state

The journal has a fixed `Capacity`, an explicit parameter set from the node's
NVMe budget. Disk use never exceeds it.

* Below 75 % full, `Append` is not slowed.
* From 75 % to 100 %, `Append` is held to a rate that falls linearly from
  unrestricted to 64 MiB/s, so the writers settle where their rate matches the
  drain. The delay is a timer wait; it ends early if the caller's context does.
* At 100 % `Append` returns `ErrFull`. Nothing was appended and nothing is
  dropped. The caller maps it to `ENOSPC`/`EAGAIN`. A segment file that cannot be
  made (the disk is full) is the same `ErrFull`, not a dead journal.
* Recovery drains in order: once the publisher publishes and reclaims, `Append`
  succeeds again and `Replay` returns the old records first.

`State().Degraded` is true when the publication bound cannot be trusted: the
oldest unpublished record is older than `Options.Bound` (default 5 s), the
journal is slowing appends, or it has failed. During an object-store outage the
journal keeps accepting writes within its capacity, `Degraded` is true, and
`Age` and `Backlog` say how far behind it is.

## Crash outcomes

| What happens | What `Replay` returns after the next `Open` |
|---|---|
| Process crash (SIGKILL), disk and OS intact | Every record the writer had written to the file, which includes every record whose `Sync` returned. Records still in memory, appended but not yet written, are lost; none of them was durable. |
| OS crash or power loss | A **prefix** of the appended records that contains every record whose `Sync` returned. Which unsynced records survive is up to the disk; they survive as a prefix, never with a gap. |
| Torn write (partial sector) | The valid prefix before the damaged record. The damaged record and everything after it are dropped, and `Recovery` says what was. |
| Damage to a record a mark proves durable | `ErrCorrupt`; nothing is dropped. The disk lost something that was acknowledged; the Volume falls back to the store, as for a lost disk. |
| Damage to a record that no surviving mark covers: any record `Sync` acknowledged in the last sixteen rounds, or the last megabyte, before a kill | Treated as a torn tail, and **dropped although its `Sync` had returned**: the marks are spaced out, so the proof lags. This is a documented residual, the price of not flushing a second block on every round; a clean `Close` leaves no such window for damage to a record. |
| Damage to the header of the *tail* segment (one flipped bit in its checksum, first, life or lineage), a journal closed cleanly included | The marks that prove the segment's records are in the same block, and cannot be read without the life the header holds. `Open` succeeds, the segment's records are lost although their `Sync` returned and `Close` covered them, and `Recovery().Warnings` names the segment and its size. **Accepted:** a media error in that block is a loss of the disk, and the unpublished tail is lost with a disk. A flipped bit in the magic is refused (`ErrFormat`). A header torn on its way in, or in the wipe that frees a segment, is quiet (see Recovery). |
| Damage to the header of a segment that is *not* the tail | The later segments hold marks that prove its records: `ErrCorrupt`, as for any damage to a record a mark proves. Were those marks lost with the tail, the segments after it are given up and listed. |
| A directory holding a segment that is not in this layout | `ErrFormat`; nothing is read, reused or deleted. |
| Permanent disk loss | Nothing. The unpublished tail is lost. The journal cannot know what it lost; the caller knows the last published sequence and must report that an unpublished tail may be missing. A takeover must not report those writes as durable. |
| A new disk for a volume with history | `Open` on an empty directory with `Published` set to the store's sequence numbers on from it. |

Replay is idempotent: it is a read, and a record is identified by its sequence.
A consumer that applies records must tolerate seeing a record again after a crash
that happened between applying it and publishing it.

### Recovery

`Open` reads the preamble of every segment file (header and marks) and decides
before it writes anything.

| Finding | `Open` |
|---|---|
| a segment of another lineage | `ErrLineage`; nothing written |
| the store holds less than the *floor* (a mark records that the store held it when segments were reclaimed) | `ErrMismatch`: a stale Head, or a Head of another volume |
| the store holds more than the log's last record, and the log is not empty | `ErrMismatch`: the disk lost synced records, or the Head is not this volume's |
| a surviving *through* mark proves records durable that the log no longer holds, and the store does not | `ErrCorrupt` |
| the log begins after the store, with a proof of a finished sync past it | `ErrCorrupt` |
| the log begins after the store, with no such proof | the start of the log was lost with the tail in a power cut; nothing in it was acknowledged. Open drops it and reports it |
| a hole in the log, with intact segments after it, with no proof past the hole | those segments follow a gap and nothing there was acknowledged. Open wipes them (`Recovery().Dropped`) |
| a segment file that is not in this layout (its first bytes are neither a header of ours, nor zeros, nor a header of ours that was torn) | `ErrFormat`; nothing touched |
| an empty directory | a fresh journal; numbering continues after `Published` |

A segment file is *live* if its magic and its header checksum are valid, *free* if
its magic field is torn (see below) or is ours with a checksum that fails, and
*foreign* otherwise: a directory with a foreign segment is refused, whatever its
size, so that a build that does not know a layout cannot reuse or delete a file
written in it. A write is torn at any byte, so the magic of a header that was
being written over zeros, or of zeros being written over a header, is zeros and the
magic's own bytes in their places and nothing else, whichever byte the tear fell
on: a magic of which every byte is zero or the magic's own at that place is torn,
and its segment is free. Any other byte makes it foreign. The price is that a
foreign file named like a segment whose four bytes at that place happen to be zeros
and parts of the magic is taken for a torn header, and so for a free segment: it is
**reused** (overwritten when a record is placed in it) when it is the size of a
segment, and **deleted** when its size is not. Nothing else of a foreign file is. Live segments are
ordered by the sequence of their first record, then by name, and kept while each
begins exactly where the one before ended and holds at least one valid record.
The valid records of a segment are the run, from its first, in which each
record's checksum (seeded by the segment's life and the lineage) is valid and its
sequence is the previous plus one. The first byte that fails ends the log. The length in a record header is read
only after the header's own checksum has held; a payload whose checksum fails ends
the log too, and the valid records behind it, whose positions the intact headers
give, are listed in `Recovery().Dropped`, past any further damaged payloads: in the
last segment of the log, and in every segment that is wiped, where a damaged first
record makes the whole segment unreachable and its intact records are among those
given up. `Discard` holds the same records to be the journal's, so that it removes
exactly the range named.

What is *proven* comes from the marks. A **through** mark says every record up to
a sequence had been made durable by a completed sync. It is written after the
sync it describes, at the start of a later sync round, so that round's `fdatasync`
carries it, and at `Close`; so wherever one survives, everything it claims is on
the media. A mark is a second block to flush, which doubled a lone writer's fsync on
a SATA SSD when it was written every round, so it is written every sixteenth round or
after a megabyte of synced records, whichever comes first. A **floor** says the store held every record up to a sequence;
`Reclaim` makes it durable in the oldest segment that stays before it wipes any
header, so a log that begins after the store always says why. The lag is real:
the records of the last sync rounds of a journal that was killed (up to sixteen
rounds, or a megabyte of them) are covered by no surviving mark, and damage to them
is indistinguishable from a torn tail. **They are dropped even though their `Sync`
returned**, and the report says what lies behind the damage where it can
(`Recovery().Proven < Recovery().Last`); a clean `Close` leaves no such window for
damage to a record. Damage to the *header* of a segment is another matter: its marks
are in the same block and need the header's life to be read, so the records of that
segment are lost, even after a clean `Close` (the tail segment's, too), and `Open`
succeeds; the warning in `Recovery()` names the segment and its size, and the loss is
accepted as that of a block of the disk. The warning is raised when the records
behind the damaged header, followed through their own headers (each one's length leads
to the next, whose sequence is the previous plus one), reach a sequence the store does
not hold; a header torn on its way into a segment (a new file, or a recycled one whose
records are an older life's, all published) and one torn in the wipe that frees a
segment (all its records published) raise none, for what is behind them is zeros or
sequences the store holds. A false alarm remains possible for a directory opened under a Head older
than the one it was reclaimed under, which `Open` refuses before it gets there. This is a residual of the design, not a bug to be fixed by a caller: a journal that must
not lose an acknowledged record to media damage in its last rounds has to write a
mark with every round, and pays a second block flush for it.

The segments after the end of the log are wiped (their header zeroed) and the wipe
is made durable **before** any new record is written. New records always go to a
fresh segment, never behind a torn tail: otherwise a record that reappeared from
the lost tail, carrying the sequence the log expects next, would be taken for a
new one. A segment file that is reused gets a new random *life*, which seeds the
checksum of every record in it, so records of its earlier uses never validate.

`Reclaim` wipes a segment's header durably, oldest first, one at a time: a crash
may lose the newest wipes but never an older one while keeping a newer, which would
leave a gap.

## How it writes

How should a write be made durable? `bench -what strategy` times the options on a
file of 1 GiB: `pwrite` then `fdatasync`, `O_DSYNC`, `O_DIRECT` with `fdatasync`,
`O_DIRECT|O_DSYNC`, each on a file whose every byte is written and on one that was
only `fallocate`d. Median of the p50 latency of five interleaved 1.2 s runs, ext4, on
shared machines (the one-minute load average is given with each table):

NVMe, load average 8.78 at the start of the run:

| Strategy | 4 KiB | 64 KiB | 1 MiB |
|---|---|---|---|
| buffered write + `fdatasync`, fully written file | 1.21 ms | 1.22 ms | 1.39 ms |
| buffered write + `fdatasync`, `fallocate`d file | 1.32 ms | 1.33 ms | 1.54 ms |
| `O_DSYNC`, fully written | 1.24 ms | 1.18 ms | 1.36 ms |
| `O_DIRECT` + `fdatasync`, fully written | 1.22 ms | 894 µs | 1.17 ms |
| `O_DIRECT` with `O_DSYNC`, fully written | 1.74 ms | 725 µs | 1.24 ms |

SATA SSD, load average 11.88 at the start of the run:

| Strategy | 4 KiB | 64 KiB | 1 MiB |
|---|---|---|---|
| buffered write + `fdatasync`, fully written file | 815 µs | 1.01 ms | 12.64 ms |
| buffered write + `fdatasync`, `fallocate`d file | 844 µs | 1.04 ms | 12.58 ms |
| `O_DSYNC`, fully written | 796 µs | 1.01 ms | 12.31 ms |
| `O_DIRECT` + `fdatasync`, fully written | 785 µs | 939 µs | 11.50 ms |
| `O_DIRECT` with `O_DSYNC`, fully written | 779 µs | 924 µs | 11.30 ms |

What decides the design:

1. **Overwriting blocks that are already written is a little cheaper than writing
   into `fallocate`d space**: 1.08 to 1.11 times as fast on the NVMe disk, 1.00 to
   1.04 times on the SATA SSD, which is no difference. An earlier run on a
   node at load 39 showed 1.3 to 1.9 times on the NVMe disk; it did not repeat at
   load 8.78, and is not the reason for the design. An `fdatasync` into unwritten extents
   must commit the extent conversion through the filesystem journal; an overwrite needs
   only the data and one device flush, and that is the whole of the gain. The other
   reasons to write the segments fully, once, ahead of need are that the disk-full error
   shows up when a segment is made, off the append path (`ErrFull`, not a dead
   journal), and that a segment file is then recycled for good instead of deleted and
   made again, so the write traffic is the data and one pass of zeros per file.
2. **`O_DIRECT` and `O_DSYNC` are not consistently better than buffered writes plus
   one `fdatasync`.** `O_DIRECT|O_DSYNC` wins at 64 KiB on the NVMe disk (725 µs against
   1.22 ms) and loses at 4 KiB (1.74 ms against 1.21 ms); on the SATA SSD
   all five are within a few percent at 4 KiB and 64 KiB. `O_DIRECT` with `fdatasync`
   is within 1 % of the buffered path at 4 KiB. They cost alignment and padding
   in the record format and cannot share a page cache. That is the choice made here,
   and it has a price that is stated: the records begin at byte 80, which is not a
   block boundary, and the marks share the first block of a segment with the first
   records, so direct I/O (1.7 times faster at 64 KiB on the NVMe disk above) is closed
   off, and so is a mark on every round without a second block flush. The journal uses buffered
   `pwrite` and `fdatasync`; the group-sync queue makes one `fdatasync` serve every
   waiter.

Two more rules keep the cost of a sync bounded:

* Data is handed to the system for writeback (`sync_file_range`, a hint) as soon as
  1 MiB has been written to a segment, so a later `fdatasync` finds it already on its
  way. Its effect is measured under [The writeback hint](#the-writeback-hint).
* An `fdatasync` is started every 8 MiB with nobody waiting, and the writer waits
  when 16 MiB are written and not yet synced. A sync is therefore never asked to
  flush more than about 16 MiB, and the next `Sync` call never waits behind
  gigabytes. On a disk that flushes 80 MB/s that is still 200 ms; see the SATA
  measurement under [A bulk writer beside fsync writers](#a-bulk-writer-beside-fsync-writers).

## Measurements

`bench` (in `journal/bench`) runs three programs on the same disk, one directory,
ext4. **Journal** is `Append` + `Sync` of a 4 KiB record by N goroutines. **Plain**
is what a writer does without a journal: each goroutine writes its own file and
calls `fdatasync` on it. **Group** is the loop one writes by hand to get the same
batching: the goroutines write to one shared file and wait for one shared
`fdatasync`, run by a dedicated goroutine that wakes exactly the waiters whose write
finished before the call began (`bench -what group`, about 60 lines). Each row is a
5 s run. Both machines are shared with other work, so a run starts only when the
one-minute load average is below 20, and the load average seen by each run is
recorded in `journal/bench`'s output; the range over each table is given with it.
The latency tables are single runs; the bulk and sequential tables give every
round. Runs on these machines vary by several percent.
The tables were measured before the header gained its four bytes of magic and
before a mark's in-memory record moved after its write; neither changes what a sync
round writes.

### Small synchronous writes

**NVMe, one-minute load average 9 to 24 over the table:**

| Writers | Journal ops/s | p50 | `fdatasync` per 1,000 syncs | Group ops/s | p50 | Plain ops/s | p50 | Journal / group | Journal / plain |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 790 | 1.20 ms | 1000 | 751 | 1.30 ms | 757 | 1.29 ms | 1.05 | 1.0 |
| 8 | 3,196 | 2.49 ms | 248 | 3,082 | 2.57 ms | 1,610 | 4.83 ms | 1.04 | 2.0 |
| 32 | 12,729 | 2.51 ms | 62 | 12,460 | 2.53 ms | 2,645 | 12.4 ms | 1.02 | 4.8 |
| 64 | 24,611 | 2.57 ms | 32 | 24,421 | 2.59 ms | 4,726 | 13.8 ms | 1.01 | 5.2 |
| 128 | 47,764 | 2.64 ms | 16 | 47,432 | 2.69 ms | 8,881 | 14.2 ms | 1.01 | 5.4 |

* With one writer the three programs are the same: one `fdatasync` per write.
* Against plain `fdatasync` per file the journal does 2.0 times the operations per second
  at 8 writers and 5.4 times at 128, because it makes one `fdatasync` serve many
  waiters (the `fdatasync` column: 248 per 1,000 syncs at 8 writers, 16 at 128).
* **Against a hand-written group commit the journal is not faster**: 1.01 to 1.05 times
  the operations per second, the same latency to within a few percent. It is the
  same algorithm. What the journal adds is listed under
  [What the journal adds](#what-the-journal-adds).

**SATA SSD, one-minute load average 11 to 23 over the table:**

| Writers | Journal ops/s | p50 | `fdatasync` per 1,000 syncs | Group ops/s | p50 | Plain ops/s | p50 | Journal / group | Journal / plain |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 923 | 819 µs | 1000 | 1,065 | 777 µs | 1,054 | 782 µs | 0.87 | 0.9 |
| 8 | 3,865 | 1.64 ms | 249 | 4,261 | 1.65 ms | 1,236 | 5.60 ms | 0.91 | 3.1 |
| 32 | 9,938 | 2.49 ms | 63 | 11,050 | 2.39 ms | 1,228 | 25.3 ms | 0.90 | 8.1 |
| 64 | 13,478 | 3.88 ms | 32 | 15,217 | 3.65 ms | 1,184 | 51.8 ms | 0.89 | 11.4 |
| 128 | 15,937 | 7.06 ms | 16 | 18,826 | 6.28 ms | 1,201 | 99.3 ms | 0.85 | 13.3 |

* One writer: the journal is 0.88 times plain (819 µs against 782 µs p50).
* 8 to 128 writers: 3.1 to 13.3 times plain; 0.85 to 0.91 times the group loop. Where one
  `fdatasync` costs a device flush of about a millisecond, batching matters more in
  absolute terms and still comes from the batching, not from the journal.

### A bulk writer beside fsync writers

Two goroutines stream 1 MiB writes without waiting for durability while N goroutines
do 4 KiB write + fsync. In the plain and group programs the bulk goes to separate
files; in the journal it is appended to the same log, because the journal is one
ordered log: a `Sync` covers everything before it, bulk included. Median of three
rounds (load noted per table).

NVMe, load 12 to 17:

| Fsync writers | Plain p50 | Group p50 | Journal p50 | Journal / plain (p50) | Journal ops/s | Plain ops/s |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 4.42 ms | 2.60 ms | 10.0 ms | 2.3 | 768 | 1,754 |
| 32 | 9.69 ms | 2.66 ms | 10.3 ms | 1.1 | 3,014 | 3,325 |

SATA SSD, load 13 to 16:

| Fsync writers | Plain p50 | Group p50 | Journal p50 | Journal / plain (p50) | Journal ops/s | Plain ops/s |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 5.64 ms | 1.63 ms | 285.8 ms | 50.7 | 28 | 201 |
| 32 | 25.9 ms | 2.37 ms | 276.9 ms | 10.7 | 113 | 1,150 |

This is the journal's known cost and the reason for the
[payload split](#integration-with-the-volume): the small fsyncs wait 2.3 times as long
as plain at 8 writers on the NVMe disk and 51 times as long on the SATA SSD, where a
megabyte takes about 12 ms to flush. The journal bounds what is written ahead of a
sync (a sync every 8 MiB, at most 16 MiB unsynced), which keeps the NVMe factor small and
stops the wait growing with the amount of bulk data. On a disk that flushes 80 MB/s
16 MiB is 200 ms, and no bound that lets bulk data into the same ordered log
makes that small.

### Sequential throughput

One writer, 1 MiB `Append`s back to back, then `Sync` of the last: burst is 1.5 GiB,
sustained is 8 GiB on the NVMe disk and 4 GiB on the SATA SSD. The plain loop
writes the same amount to one file and calls `fdatasync` every N bytes. Three rounds,
interleaved on the NVMe disk. On the SATA SSD the first three rounds were
disturbed (the one-minute load average of that shared node went above 60 in two of
them and one run fell to 8 MB/s) and were repeated: its table is five fresh rounds.

NVMe, load 9 to 15:

| Run | MB/s, round by round | Median |
|---|---|---:|
| journal, burst (1.5 GiB) | 2,782, 2,694, 2,563 | 2,694 |
| journal, sustained | 2,592, 2,634, 2,678 | 2,634 |
| plain `pwrite`, `fdatasync` every 1 MiB | 736, 748, 744 | 744 |
| plain `pwrite`, `fdatasync` every 8 MiB | 2,619, 2,596, 2,462 | 2,596 |
| plain `pwrite`, `fdatasync` every 32 MiB | 3,756, 3,889, 3,768 | 3,768 |

SATA SSD, load 12 to 15:

| Run | MB/s, round by round | Median |
|---|---|---:|
| journal, burst (1.5 GiB) | 49, 47, 71, 39, 71 | 49 |
| journal, sustained | 67, 72, 76, 60, 80 | 72 |
| plain `pwrite`, `fdatasync` every 1 MiB | 64, 76, 65, 60, 81 | 65 |
| plain `pwrite`, `fdatasync` every 8 MiB | 90, 87, 69, 67, 94 | 87 |
| plain `pwrite`, `fdatasync` every 32 MiB | 88, 82, 67, 69, 75 | 75 |

* The journal, which syncs every 8 MiB in the background and when `Sync` asks, runs at
  1.01 times the plain loop that syncs every 8 MiB on the NVMe disk (0.70 times the
  loop that syncs every 32 MiB, 3.5 times the loop that syncs every MiB).
  On the SATA SSD it is 0.83 times the 8 MiB loop (0.97 of the 32 MiB loop, 1.1 times
  the 1 MiB loop).
* So bulk data written through the journal is as fast as a loop with the same
  `fdatasync` spacing on the NVMe disk, 0.83 times as fast on the SATA SSD (where the
  journal also writes the zeros of each new segment once), and not faster than a loop
  that syncs less often. A burst into segments that do not exist yet pays for making
  them: 49 MB/s against 72 sustained on the SATA SSD. The
  earlier claim that the journal beat the plain loop on bulk data by 1.27 times
  compared against the wrong loop and is withdrawn.

### The writeback hint

The journal asks for writeback (`sync_file_range`) of each megabyte as it is written.
Measured by building the journal with the hint off (nothing else changed), three
interleaved rounds each:

NVMe, load 9 to 10:

| | With the hint | Without |
|---|---:|---:|
| 1 MiB sequential, sustained: MB/s | 2,614 (2,621, 2,614, 2,510) | 2,392 (2,450, 2,193, 2,392) |
| 1 MiB sequential, sustained: p95 of a write | 1.60 ms (1.58 ms, 1.62 ms, 1.60 ms) | 2.16 ms (2.16 ms, 2.25 ms, 2.13 ms) |
| 1 MiB sequential, sustained: p99 of a write | 3.05 ms (2.86 ms, 3.05 ms, 3.06 ms) | 4.21 ms (4.01 ms, 4.80 ms, 4.21 ms) |
| 8 fsync writers beside 2 bulk writers: p50 | 10.3 ms (10.3 ms, 10.1 ms, 10.7 ms) | 16.7 ms (11.1 ms, 16.7 ms, 17.1 ms) |
| 8 fsync writers beside 2 bulk writers: ops/s | 745 (745, 754, 719) | 479 (704, 479, 464) |

SATA SSD, load 11 to 14:

| | With the hint | Without |
|---|---:|---:|
| 1 MiB sequential, sustained: MB/s | 68 (64, 88, 68) | 78 (78, 101, 72) |
| 1 MiB sequential, sustained: p95 of a write | 50.9 ms (53.7 ms, 50.9 ms, 50.5 ms) | 88.4 ms (88.4 ms, 84.7 ms, 97.7 ms) |
| 1 MiB sequential, sustained: p99 of a write | 85.0 ms (85.0 ms, 70.2 ms, 104.5 ms) | 161.1 ms (161.1 ms, 143.5 ms, 178.5 ms) |
| 8 fsync writers beside 2 bulk writers: p50 | 313.9 ms (349.6 ms, 294.4 ms, 313.9 ms) | 271.3 ms (271.3 ms, 270.7 ms, 301.9 ms) |
| 8 fsync writers beside 2 bulk writers: ops/s | 27 (23, 27, 27) | 26 (27, 24, 26) |

* On the NVMe disk the hint raises sustained throughput by about 9 percent, cuts the p95 and p99 of
  a 1 MiB write by about a quarter, and makes small fsyncs beside bulk writers faster.
* On the SATA SSD it cuts the tail of a 1 MiB write (p95 1.7 times lower, p99 1.9 times)
  and does not make small fsyncs beside bulk writers faster; throughput and that latency
  differ by less than the spread between rounds. It is kept because it costs nothing
  measurable and the tails improve on both disks.



## File layout

A directory of segment files `NNNNNN.seg`, all of one size (`Options.Segment`,
default 64 MiB, at most 4 GiB). Integers are little endian; checksums are CRC-32C
(Castagnoli).

```
segment  header | mark | mark | record | record | ... | (unused)
header   checksum u32 | magic [4]byte | first u64 | life u64 | lineage [16]byte   40 bytes
mark     checksum u32 | through u64 | floor u64                       20 bytes, two slots
record   checksum u32 | length u32 | sequence u64 | acknowledged i64 | payload checksum u32 | payload
```

* The header checksum covers the `magic` (the four bytes `JRNL`, which are not a
  version and mean nothing else), `first` (the sequence of the segment's first
  record, at least 1), `life` (random, new each time the file is reused) and
  `lineage` (the first 16 bytes of the SHA-256 of `Options.Lineage`). A header whose
  magic is zero is a free segment. The records begin at byte 80.
* The seed of every record and mark checksum is the CRC-32C of `life` followed by
  `lineage`. The record checksum covers the record header after itself. The payload
  checksum is computed before the journal's lock is taken; the record checksum binds
  it to the sequence and the time.
* The two marks are written alternately, each in one 20-byte write; a torn write
  damages one and the other still stands. A recovery takes the highest `through`
  and the highest `floor` of the marks that validate. See [Recovery](#recovery).
* `acknowledged` is the wall-clock time of the `Append`, in nanoseconds since the
  Unix epoch. It survives a restart, so the age of the oldest unpublished record
  does too (clamped to the restart, never extended by it).
* Records do not span segments. Many records are written by one `pwrite` (group
  commit); the first write into a segment starts at its header and carries the
  first mark.
* **One layout, no versions.** The format has no version number, no upgrade path
  and no compatibility with any other. The magic exists so that this code can tell
  a file it wrote from a file it did not, and refuse the latter. The journal's
  contents are transient: every record is published and then reclaimed. A change of
  layout is therefore never an in-place upgrade; it is *publish everything, `Close`,
  delete the directory*, and start the new build on an empty one.
* The payload is stored as given. **The journal does not encrypt**: every write of
  the volume is in the clear on the node's disk. The directory is created `0700`
  and the files `0600`; a directory that already exists is used with the mode it
  has. The Volume seals what it appends.

## The file layer

The journal writes only through the `Disk` and `File` interfaces. `Files(dir)` is
the operating system's: it creates the directory (`0700`, and every directory above
it that did not exist) and makes the entry of each one in its parent durable; takes an exclusive `flock` on the directory, held until the
journal closes or the process dies (`ErrLocked` for a second journal on the
directory); and reads the file system type (`statfs`), refusing tmpfs and ramfs
(`ErrVolatile`: an fsync there makes nothing durable) and warning about overlay,
FUSE, 9p and network file systems (`Recovery().Warnings`: a sync reaches the media
only if the layer below passes it on; check before relying on a journal in a
sandbox). A `File`'s `WriteAt` writes all of its bytes or returns an error; a short
write with no error fails the journal. On macOS Go's `File.Sync` is `F_FULLFSYNC`,
which asks the drive to flush its own cache; it is slower than `fdatasync` on Linux
and is what durable means there. The directory syncs are `fsync` of a directory
descriptor; on macOS that call is not `F_FULLFSYNC` and this layer has not been run
there to see what reaches the media. Only Linux is a supported platform, and of its
architectures `amd64` and `arm64`: 32-bit ARM does not build (the writeback hint
uses a system call it does not have).

Two tests keep this layer honest. The journal's logic runs against `journal/crash`,
a `Disk` in memory that logs every write, sync, create and remove, and rebuilds the
disk a crash would leave after any prefix of that log: nothing persisted,
everything persisted, each prefix, every subset of a few writes, torn writes inside
and across sectors, random scatters. `crash.Disk.Explore` checks every such state, and a crash of the recovery itself is
explored the same way. A write at the start of a file, where a header goes, is also
torn after each of its first bytes in turn (`Tears`), since a write may be torn at any
byte. The workloads run at several mark cadences (a mark every
round, every third, every sixteenth, and none but the ones a segment starts with),
because the order of a mark and the sync that carries it, and the lag of the proof,
are only in the model when marks are as rare as they are in production. It models `Create` and `Remove` as durable when they
return, which is the contract; whether the real layer meets it is what the second
test checks: the test binary runs again as its own child, traced with `ptrace`
(a process may trace its child with no privileges, and nothing is installed), and
reads the system calls the real layer makes. A created file is `fdatasync`ed,
renamed into place, and the directory `fsync`ed, in that order, before the first
record is written to it; a removal is followed by a directory `fsync`; the parent of
the journal's directory is synced; the lock is taken before anything is created; and
an `EIO` the tracer puts into an `fdatasync` of a segment fails the `Sync` and every
later `Append`. The tracer reads the registers of `linux/amd64` and `linux/arm64`;
on another architecture these tests fail, they do not skip.

## Integration with the Volume

The Volume uses the journal as of the change that wired it in (`volume.go`, `change.go`,
`ledger.go`, `generation.go`; the text of what it guarantees is `docs/SPECIFICATION.md` section 8).
Items 1 to 7, 10 to 12, 14 and 15 below are done. Items 8, 9 and 13, the bulk payloads that bypass
the ordered log, are not: every payload goes through the one log, so a small fsync waits behind
the bulk written before it, and the journal should not carry a disk of the SATA class until they
are. The records are opaque bytes: the Volume chooses the encoding of a mutation (`change.go`: a
write, a size, a set of tree records), seals it with a key of the journal's own, and the journal
only orders, stores and numbers it. The surface the Volume calls:

```go
j, err := journal.Open(dir, journal.Options{Capacity: budget, Published: head, Lineage: volumeAndRound})
err = j.Replay(func(r journal.Record) error { ... })   // at mount, before serving
seq, err := j.Append(ctx, mutation)                    // before applying it
err = j.Sync(ctx, seq)                                 // fsync
<-j.Due(c, bound-margin)                               // the publisher's deadline (c: cancel when another trigger wins)
err = j.Publish(g); err = j.Reclaim()                  // after the Head carrying g, a durable g, is committed
st := j.State(); m := j.Marks(); r := j.Recovery()     // what an agent reads
err = j.Close()                                        // Detach, suspend, end
err = journal.Discard(dir, opt, first, last)           // only on an operator's decision
```

The integration change does the following.

1. **Mount.** Read the Head's journal sequence `h` (the last sequence committed
   into the Head) and its fencing round. `journal.Open(dir, Options{Capacity:
   <NVMe budget>, Published: h, Lineage: <volume id and round>})`. The Head is the
   truth: pass it every time, including 0 for a Disk that has never published. An
   `ErrMismatch`, `ErrLineage` or `ErrCorrupt` is not retried with another Head:
   it means the node's journal is not this volume's current history, and the
   Volume falls back to the Head (as after a lost disk) and reports that an
   unpublished tail may be missing; `Discard` is for an operator who has decided
   the records are not wanted. `Replay` and apply each record to the Volume's
   in-memory state, in order; then start the publisher.
2. **Mutation.** Every Volume mutation (write, truncate, rename, create, unlink,
   setattr) is encoded as one record and `Append`ed before it is applied. A write's
   journal sequence is the Volume's write sequence.
3. **fsync.** `Sync(ctx, seq)` with the sequence of the last record the file
   depends on. A local fsync is then durable in the node's journal, not in the
   store.
4. **Publisher.** One serialized publisher per Volume. It takes an immutable
   generation under a short lock (everything up to a durable sequence `g`), releases
   the lock, builds and uploads the objects, commits the Head carrying `g`, and
   calls `Publish(g)` then `Reclaim()`. It loops on `Due(c, bound - margin)` and also
   acts early on dirty bytes, an explicit Flush, Detach, suspend or end. A fsync
   waits for the journal, never for a generation.
5. **Bound.** The 5 s bound is `Due`'s argument. `State()` is what the caller or an
   agent reads to learn that it cannot be trusted. `ErrFull` is `ENOSPC`/`EAGAIN`.
6. **Detach, suspend, end.** Force the kernel to flush its dirty pages for the
   mount first (the journal sees only what reaches the daemon), then publish
   everything, then `Close`.
7. **Takeover.** The new owner opens its own journal, under its own round, with
   `Published` set to the Head's sequence. The old owner's unpublished tail is on
   its disk and is not visible to the new owner; if that disk is gone it is lost,
   and the takeover must say so rather than report those writes durable. If the old
   owner comes back, its journal's lineage no longer matches and it is refused.
8. **Large payloads do not go through the ordered log.** One log means a `Sync`
   covers all the bulk data before it. Payloads above about 64 KiB (a file's data,
   as against a metadata mutation) go to a second `Journal` instance, or to a spool
   of files, whose own `Sync` the fsync of that file waits for; the ordered log
   carries only the small record that names them. No format change is needed. The
   [measurements](#a-bulk-writer-beside-fsync-writers) show why: with the bulk in the
   same log, the p50 of small fsyncs behind two bulk writers was 2.3 times that
   of plain per-file fsyncs on NVMe and 51 times on a SATA SSD (8 writers). The Volume change must do this, or measure
   fsync-heavy phases (a source checkout, a package install, a database) beside bulk writes first.
9. **Do not enable the journal on SATA-class disks** without that split and that
   measurement.
10. **A directory per volume and fencing round**, whose lineage is the volume and
    the round. Never restore a journal directory from a backup, never copy one
    between nodes, and never reuse one for another history: a restored directory
    opens under its lineage and a Head at or below its last record and replays an
    older timeline with no sign that records are missing. If a node's disk is lost or doubtful, delete the
    directory and treat the unpublished tail as lost.
11. **The Head's sequence on every `Open`**, 0 included, and never retried with
    another value. `ErrMismatch`, `ErrCorrupt`, `ErrLineage` and `ErrFormat` mean
    fall back to the Head and report that an unpublished tail may be missing;
    `ErrLocked` is a fence (another journal has the directory) and is not retried
    either.
12. **Order of the publisher**: commit the Head, then `Publish(g)` with `g` no more
    than `Marks().Durable`, then `Reclaim()`. The publisher is stopped before
    `Close` and before a `Replay` it must not race with.
13. **Bulk payloads** (over about 64 KiB) bypass the ordered log, as in item 8: a
    second journal or a spool of files, durable first, and only then the record in
    the ordered log that names them, so that a record never names bulk data that a
    crash could lose. The two journals have distinct lineages.
14. **Errors to the mount**: `ErrFull` is `ENOSPC` or `EAGAIN`; a journal failure
    (`State().Err`) makes the mount read-only, since no later write can be made
    durable.
15. **Seal before `Append`.** The journal stores the payload as given.

The trap to test in that change: with a writeback page cache the kernel can
acknowledge a `write(2)` before the daemon sees it, so the 5 s bound is measured
from the record's entry into the journal, and `fsync` or `syncfs` are the only
durability promises for buffered writes.

## What the journal adds

A loop that does the fsync part by hand is short: writers `pwrite` to one shared
file, and one `fdatasync` serves every writer whose write finished before it began
(`bench -what group` is that loop, 60 lines). Against it, at the same concurrency,
the journal's group sync is no faster on the NVMe disk and 10 to 15 percent slower
on the SATA SSD (see [Measurements](#measurements)): it is the same algorithm, plus
record framing and one more block to flush every sixteenth round. What the journal adds is everything around it, and that is
what a Volume needs and a loop does not have:

* **Many writers and many files behind one sync queue**, with the barrier rule
  proven under a crash model, not argued.
* **Records**: length, checksum, sequence and acknowledgement time, many per
  write; a torn tail detected, never read as data.
* **Replay**: the unpublished records in order, idempotently, after any crash,
  checked by exploring every persistence point, including a crash of the recovery.
* **The three watermarks** and the rule between them, with a `Publish` that cannot
  get ahead of the disk.
* **The bounded deadline**: a timer on the oldest unpublished record's own
  acknowledgement, correct across restarts and clock steps.
* **Backpressure and a state an agent can read**: a fixed capacity, a slowdown,
  `ErrFull`, a degraded flag, the backlog and its age.
* **Recycling** of fully written segments, so an fdatasync never pays for new
  extents; **bounded exposure**, so one bulk writer cannot make the next fsync wait
  for gigabytes; **refusal** of a directory, Head or lineage that does not fit,
  instead of destroying what is there.

## Limits

* **One ordered log.** A `Sync` covers all the data before it. Bulk data written
  beside fsync callers delays them (measured below); the cure is to keep the bulk
  out of the log, in the Volume.
* **The proof lags.** A mark is written with a later sync round (every sixteenth, or
  every megabyte, and at `Close`). Damage to a record written in the last sixteen
  rounds, or the last megabyte, before a kill is treated as a torn tail and dropped
  although its `Sync` returned. Plainly: a journal that is killed and then found with
  a damaged sector in its last rounds loses those acknowledged records, and `Open`
  succeeds.
* **A restored directory is mostly invisible.** A journal directory restored from a
  backup opens under its lineage and a Head at or below its last record, and replays
  the older history; nothing can tell. Under a Head beyond its last record it is
  refused (`ErrMismatch`). Never restore one: delete the directory and treat the
  node's disk as lost.
* **Plaintext at rest.** The journal does not encrypt. The Volume seals what it
  appends.
* **Namespace durability is pinned, not simulated.** The crash harness treats
  `Create` and `Remove` as durable when they return, which is the contract; the
  real layer's order of file sync, rename and directory sync is pinned by the
  system-call tests. A model that loses a directory entry the layer failed to sync
  is not built.
* **A failed `Reclaim`** returns its error. A failed write of the floor leaves the
  journal as it was and the next call writes it again; a failed sync of it fails the
  journal, since a sync repeated after a failure may succeed without the data. The
  wipes of headers can be repeated.
* **One platform.** Linux on `amd64` and `arm64`. 32-bit ARM does not build, and
  macOS has not been run (see the file layer).
* **One process, one directory.** The lock is advisory and per directory; a platform
  without `flock` has none.

## Specification notes

This section is normative. The words MUST, MUST NOT, SHOULD and MAY are used as in
RFC 2119. A conforming implementation need not be this package; each criterion can
be tested from outside, with a file layer that can hold, fail and crash.

### Model

A *journal* is a sequence of *records*. A record has a *payload* of 0 to `S - 80 - 28`
bytes (`S` is the segment size), a *sequence* and an *acknowledgement time*. The
journal has a fixed *capacity* of at least two segments. It is used by one process
at a time. The caller supplies a clock, a file layer, a *lineage* (an opaque
identity) and the *store's sequence*, the highest sequence the object store
holds. The file layer has `create` (file of a given size, every byte written,
durable on return), `open`, `remove` (durable on return), `list`, `close`, and per
file `read`, `write` (all of the bytes, or an error), `sync` and `size`. A write is
volatile until a `sync` that began after it completes; a crash may leave any subset
of the volatile writes, each possibly torn at any byte.

### Requirements

1. **Sequences.** Sequences are positive integers. The journal MUST assign
   `previous + 1` to each appended record, in the order the appends are admitted,
   and MUST NOT return a sequence twice in one history of a store. After an `open`
   that is told the store holds sequence `h`, the next sequence MUST be greater
   than `h` and greater than every sequence the journal recovers.
2. **Watermarks.** The journal MUST maintain `published <= durable <= visible`,
   each non-decreasing. `visible` is the highest appended sequence. `durable` is
   the highest sequence covered by a completed sync. `published` is the highest
   sequence passed to `publish` (monotonic; a lower value is ignored), and
   `publish(s)` MUST fail, and change nothing, when `s` is greater than `durable`.
3. **Append.** On return the record MUST be visible: a replay in the same run
   includes it. A record is not durable on return. When the journal cannot accept
   the record because its capacity is used by unpublished records, or because a
   segment file cannot be made, `append` MUST fail with a distinguishable "full"
   error and MUST leave all state unchanged; a segment file that cannot be made
   MUST NOT fail the journal. A payload that cannot fit in a segment MUST be
   rejected with a distinguishable error.
4. **Sync.** `sync(s)` MUST return success only when record `s` and every record
   before it are durable, and MUST NOT be satisfied by a sync that began before
   record `s` was written. Concurrent `sync` calls MUST share syncs: calls that
   arrive while a sync is in flight are released by the next one. After a failed
   write or sync (or a write of fewer bytes than asked, with no error),
   `sync(s)` MUST fail for every `s` not already durable, and every later `append`
   MUST fail; the journal MUST NOT retry. `sync` of a sequence not yet appended
   MUST fail with a distinguishable error.
5. **Publish.** `publish` does no file I/O.
6. **Replay.** `replay` MUST yield, in order and without gaps, every record with a
   sequence greater than `published`, up to `visible`, each with the payload and
   acknowledgement time it was appended with. It MUST NOT change the journal: two
   replays with no operation between them yield the same records. It MUST fail
   rather than skip a record that should be present.
7. **Reclaim.** `reclaim` MUST NOT make any record with a sequence above `published`
   unavailable, and MUST NOT free the segment new records are being added to. It
   MUST make the store's sequence durable in the oldest segment that stays (the
   *floor*) before it wipes any header, and MUST NOT wipe a header until that
   segment's own header is on the disk. A mark MUST NOT be taken for written until
   its write has succeeded, so that a failed write is made again; a failed sync of
   the floor MUST fail the journal. It MUST wipe oldest first, each durable
   before the next. It MUST NOT run concurrently with a `replay` that is reading
   the segment.
8. **Deadline.** `due(a)` MUST complete when the oldest unpublished record has been
   acknowledged for `a`, measured from that record's own acknowledgement time. Later
   appends MUST NOT move the deadline. When `publish` retires the oldest record, the
   deadline MUST move to the next unpublished record. With nothing unpublished no
   timer MAY be pending; the deadline starts with the next append. Two firings MUST
   be at least a configured *retry* apart. `due` MUST complete when the journal
   closes or fails, and MUST release its timer and its registration when its context
   ends. Ages within the process MUST be measured on a monotonic clock; a record
   recovered from disk whose acknowledgement time is later than the time of the
   restart MUST be treated as acknowledged at the restart, so that after a restart
   no record has more than the whole bound left. (It can have more than it had
   before the restart when the clock is found behind where it was; a restart does
   not keep the time left.) The implementation MAY fire up to 1 ms
   early and MUST NOT fire late by more than the clock's resolution.
9. **State.** `state` MUST report the bytes of unpublished records, the age of the
   oldest, and the error that stopped the journal. It MUST report `degraded` when
   that age exceeds the configured bound, when the journal is slowing appends, or
   when it has failed.
10. **Backpressure.** Disk use MUST NOT exceed the capacity. Below 75 % of the
    capacity `append` MUST NOT be delayed. Between 75 % and 100 % it MUST be delayed
    so that the admitted rate falls toward 64 MiB/s as the fill approaches 100 %; a
    delayed append MUST return when its context ends. At 100 % it MUST fail with the
    "full" error. No record is ever dropped. When the publisher publishes and
    reclaims, appends MUST succeed again and replay MUST return the older records
    first.
11. **Format.** A segment is a file holding a 40-byte header, two 20-byte marks and
    records back to back. The header is `checksum(4) magic(4) first(8) life(8)
    lineage(16)`: the CRC-32C of the last 36 bytes, four fixed bytes (`JRNL`, which
    are not a version), the sequence of the segment's first record (at
    least 1), a random value fresh each time the file is used, and the first 16
    bytes of the SHA-256 of the caller's lineage. A mark is `checksum(4) through(8)
    floor(8)`, its checksum the CRC-32C of the last 16 bytes seeded with the seed
    below. A record is `checksum(4) length(4) sequence(8) acknowledged(8) payload
    checksum(4) payload`: the record checksum is the CRC-32C of the 24 bytes after
    it seeded with the seed below; the payload checksum is the CRC-32C of the
    payload; `acknowledged` is Unix nanoseconds. The seed is the CRC-32C of the
    segment's `life` (8 bytes) followed by its `lineage`. Integers are little endian.
    Records do not span segments. A header whose magic is zero marks a free segment,
    as does one whose magic is the fixed bytes and whose checksum fails, and one whose
    magic is torn: every byte of it zero or the fixed byte at that place. A header whose
    checksum holds and whose first sequence is zero is not a segment's either: it is
    free. There is one layout and no version: an implementation MUST NOT read, reuse
    or delete a segment file whose first bytes are none of these (see 13), except
    that a file whose magic only looks torn, being zeros and parts of the magic, is
    free and so is reused or, if its size is not a segment's, deleted, and a change of layout MUST be
    made by publishing everything, closing the journal and deleting its directory.
12. **Marks.** A mark's `through` MUST be a sequence that a completed sync had
    covered before the mark was written; its `floor` MUST be a sequence the store
    held. A mark MUST be written after the sync it describes (in a later sync round,
    so that round's sync carries it) and at close, and MUST NOT cost a sync of its
    own. Marks MAY be spaced out; the more rounds between them, the more of the end
    of a killed journal is covered by no surviving mark and is treated as a torn tail
    (the reference implementation writes one every sixteenth round or megabyte). The two
    mark slots MUST be written alternately: a mark goes to the slot that does not hold
    the newest mark, the one with the highest `through` and, among equal throughs,
    the highest `floor`, also in a segment found at `open`, so that a write torn in
    flight leaves the newest standing. A reader takes the highest `through` and
    `floor` of the slots that validate.
13. **Recovery.** `open` MUST decide before it writes. It MUST fail, and MUST NOT
    write (but for removing the empty temporary files of its own interrupted creations
    of segments, which are not a segment and which a refused `open` still removes),
    when: a segment file is not in this layout; a segment's lineage is not the caller's; the store's sequence is
    below the highest floor; the store's sequence is above the last record of a
    non-empty log; or a `through` is above both the last record and the store's
    sequence (records that were made durable are gone); or the log begins after the
    store's sequence with a `through` above it. Otherwise it MUST keep the longest
    prefix of records in which every record is valid under its segment's life and
    lineage and has the sequence one greater than its predecessor, across segments
    ordered by `first` then name and linked only where `first` equals the previous
    segment's last sequence plus one. It MAY drop what follows the first byte that
    fails, and a log that begins after the store's sequence when no `through`
    proves a sync past it; it MUST report what it dropped (the intact segments that follow a gap, and the
    valid records behind a record whose payload fails its checksum, where the
    header's length can be believed), MUST make the dropped
    segments free durably before any new record is written, MUST make the records it
    keeps durable before it returns, and MUST write new records to a segment that
    holds no kept record.
14. **Discard.** The only operation that gives records up is a separate one that
    names the first and last sequence; it MUST do nothing, and fail, unless the
    journal holds exactly that range: intact records beyond a hole, and intact records
    behind a record whose payload is damaged, are a different range, and a copy of a
    segment inside the range goes with it.
15. **Exclusion and placement.** The journal MUST hold an exclusive lock on its
    directory for as long as it is open and MUST refuse a second journal on the
    directory. It MUST refuse a directory on volatile memory. It SHOULD create the
    directory readable by its owner alone, the files likewise, and warn about
    layered and remote file systems. The segment size MUST NOT exceed 4 GiB.
16. **Bounded exposure.** The memory the journal queues for the file layer MUST be
    bounded: when the writer cannot keep up, `append` waits for it. The data written
    but not yet synced SHOULD be bounded too, and the journal SHOULD start syncs
    without a caller waiting and ask the system to write data out as it is written,
    so a `sync` never waits behind an unbounded amount of other callers' data. This
    implementation queues at most 16 MiB, starts a sync every 8 MiB, lets the writer
    run at most 16 MiB ahead of the last completed sync, and requests writeback for
    every 1 MiB.
17. **Crash outcomes.** After a crash of the process with the files intact, the
    recovered log MUST contain every record whose `sync` returned. After a crash of
    the machine, the recovered log MUST be a prefix of the appended records that
    contains every record whose `sync` returned and that is not published, and
    `open`, told the store's sequence, MUST NOT fail. After loss of the disk the
    journal is empty, the unpublished tail is lost, and the caller MUST NOT report
    those records durable.

### Acceptance criteria

Each criterion is a test that fails if the requirement is removed.

* **A1 order**: 16 goroutines append 150 records each; the returned sequences are
  exactly 1..2400, each once, and a replay returns them in order with their payloads.
* **A2 barrier**: hold the first sync in the file layer, append a record `b`, call
  `sync(b)`, release the sync: `sync(b)` is still blocked; it returns after a second
  sync that began after `b` was written.
* **A3 grouping**: hold a sync, start 32 `sync` callers, release: at most two further
  syncs are issued.
* **A4 crash exploration**: run a mixed workload (append, sync, publish, reclaim,
  several writers) on a file layer that logs writes and syncs. For every prefix of
  the log and for each of: no volatile write kept, all kept, each prefix kept, every
  subset of up to six volatile writes, the last volatile writes torn at 1, 100, 512
  and 612 bytes, at half the write rounded down to a sector, and one byte short,
  random per-sector subsets: recovery, told the store's sequence, succeeds; the
  replay is contiguous after it; its payloads equal those appended; it reaches every
  record whose `sync` returned unless the store holds it; a second replay and a
  second `open` return the same; and the same holds for every crash state of the
  recovery itself.
* **A5 no resurrection**: in every state of A4, append four new records with
  different payloads and sync them, keep only synced data, recover: the replay is
  the earlier records followed by exactly the four, never a record of the lost
  tail. A direct case: reuse a segment so that the new records end where a stale
  record with the next expected sequence begins; the stale record is not replayed.
* **A6 reclaim window**: append 12 records (9 synced), publish 6 and reclaim; at
  every crash point, with the wipes persisted in any order, recovery with the store
  at the sequence it held yields a log that is a prefix and loses no record above 6
  that was synced.
* **A7 reclaim safety**: while a replay is paused inside its callback, `reclaim` does
  not modify any segment; with the next segment's first record still queued behind
  the writer, `reclaim` wipes nothing.
* **A8 deadline**: with a controlled clock, append at `t`, `due(5 s)` closes at
  `t + 5 s` and not 1 ms before; ten appends in between do not move it; with records
  at `t` and `t + 3 s`, publishing the first moves the deadline to `t + 8 s`; with
  nothing unpublished no timer is pending, and after 1,000 appends one timer is;
  1,000 abandoned calls leave none; an overdue record fires again `retry` after the
  last firing, not before; the channel closes when the journal fails.
* **A9 bound**: a writer appending every 10 ms and a publisher that publishes when
  `due(5 s)` closes keep `state.age` at or below 5 s for a minute; stopping the
  publisher for 20 s makes `degraded` true with `age` above 20 s and a growing
  `backlog`; publishing everything clears both.
* **A10 backpressure**: with no publisher, appends are delayed before the first
  `full` error and no acknowledged record is lost; after publishing half and
  reclaiming, appends succeed, the next sequence continues, and replay starts after
  the published mark. A file layer that cannot make a segment gives `full`, not a
  failed journal.
* **A11 failure**: after an injected write or sync error, or a short write, `sync` of
  a non-durable sequence and every `append` fail with that error; `sync` of a durable
  sequence succeeds.
* **A12 damage**: after a clean close, flipping a byte inside record `k` makes `open`
  fail as corrupt, twice, changing nothing; with the marks zeroed (a crash that
  lost them) it leaves a replay of exactly `1..k-1`, later appends continue at `k`,
  and `recovery` lists the intact segments it dropped.
* **A13 the store**: with records 7 to 9 synced and 1 to 6 published and reclaimed,
  `open` with the store at 0, 2 or 5 fails as a mismatch and a mount with 6 still
  replays 7 to 9. With records 1 to 5 and the store at 9, `open` fails as a mismatch;
  on an empty directory with the store at 9 the marks are 9/9/9 and the next sequence
  is 10.
* **A14 lineage**: records written under one lineage are not opened under another or
  under none; `discard` of the exact range removes them and the new lineage starts
  after the store's sequence; `discard` of any other range fails and changes nothing.
* **A15 exclusion**: a second `open` on a directory with a live journal fails; it
  succeeds after the first closes.
* **A16 placement**: `open` on a tmpfs directory fails as volatile; the file system
  types tmpfs and ramfs are refused and overlay, FUSE, 9p and network ones warned
  about; directory and files are not accessible to group and others.
* **A17 capacity**: the files never occupy more than the capacity, on the real file
  layer, across restarts.
* **A18 exposure**: with the first sync held in the file layer, a stream of 1 MiB
  appends is written no further than 28 MiB; with the writer held in its first
  write, at most 20 MiB are admitted. Appending 6 MiB in 1 MiB records requests
  writeback for 5 to 7 MiB, and 3 KiB of small records requests none.
* **A19 clock**: reopening with a clock an hour behind makes a record count as just
  acknowledged and `due` fires a whole `age` after the restart, and not before; with
  the clock right the age carries across the restart.
* **A20 system calls** (under a system-call tracer, with no root): `create` issues
  `fdatasync` of the new file, then the rename, then `fsync` of the directory;
  `remove` is followed by `fsync` of the directory; the parent of the journal's
  directory, and of every directory made for it, is synced and the lock taken before
  the first segment; `sync` issues `fdatasync` of the segment; an
  injected `EIO` on that `fdatasync` fails the `sync` and the next `append`.
* **A21 format**: a directory holding a segment file of any other layout (the same
  size as a segment, another size, shorter than a preamble) is refused by `open`
  and by `discard`, and the file is neither read, nor reused, nor deleted; a
  header torn by a crash, at any byte (its checksum damaged, its magic zeroed or
  part of it written) is free and is reused. Files that end in the temporary suffix
  but are not a segment's are not removed, by a refused `open` or any other.
* **A21a header damage**: a flipped bit in the checksum, first, life or lineage of the
  tail segment of a journal closed cleanly leaves the log ending before that
  segment, and a warning that names the segment and its size; a flipped bit in the
  magic is refused; a header torn at any byte, over a new segment, over a recycled one,
  or in a wipe of a published one, raises no warning.
* **A22 floor**: with the write of the floor failing once, the next `reclaim`
  writes it again before wiping, and a power loss then leaves a floor that refuses a
  stale store; with the sync of the floor failing, the journal fails and no header
  is wiped.
* **A23 discard**: `discard` of the range that is the log, with intact segments
  beyond a hole, fails with the range error and wipes neither; a copy of a segment
  inside the range goes with it.
* **A24 report**: damage to the payload of record 6 of 10, the marks lost, leaves 1
  to 5 and lists 6 to 10; damage to payloads 5 and 9 of 12 lists 5 to 12; with
  records in three segments and the first record of the middle one damaged, the
  records of that segment and the next are listed; damage to a header lists nothing,
  and damage to the last record lists no empty range. `discard(1, 5)` of a journal
  that also holds 7 to 10 behind a damaged 6 fails and changes nothing. A journal
  that holds valid records behind a hole, or whose record 1 is damaged with the marks
  present, cannot be cleared by `discard` for any range: the recourse is to delete
  the directory and treat the disk as lost. The `first` in the error text of `open`
  (`the first record of the log is 4`) is not a range to give to `discard`.
* **A25 cadence**: the crash exploration of A4 and A6 is repeated with a mark every
  round, every third, every sixteenth and every thousandth round, and a journal with
  nothing set writes one every sixteenth; a mark is
  written once a megabyte has been synced however few the rounds; a segment starts
  with the floor and the proof of the journal at that time, and those alone refuse a
  stale store and call damage before them corruption; after `close` returns the final
  mark is on the media.
* **A26 scan**: a record header that fails its checksum, however long it says its
  payload is, is not followed: recovery reads no more than the header.
* **A27 order of segments**: with twenty segments, each copied under a higher name,
  recovery keeps the lower names.

## References

* J. Mohan, A. Martinez, S. Ponnapalli, P. Raju, V. Chidambaram. Finding
  Crash-Consistency Bugs with Bounded Black-Box Crash Testing. OSDI 2018.
* J. Bornholt, A. Kaufmann, J. Li, A. Krishnamurthy, E. Torlak, X. Wang. Specifying
  and Checking File System Crash-Consistency Models. ASPLOS 2016.
* Wang et al., FAST 2025,
  <https://www.usenix.org/conference/fast25/presentation/wang> (byte-granularity
  records, absorbing small synchronous writes, systematic crash testing).
