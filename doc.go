// Package vfs is a filesystem on an object store: a Volume.
//
// # On-disk layout
//
// A Volume is a prefix of a bucket holding three kinds of object, all sealed by
// the Volume's Cipher:
//
//	head                    the one mutable object: names the root shard
//	meta/<2 hex>/<hash>     metadata shards, immutable, named by the hash of their bytes
//	pack/<2 hex>/<id>       packs of file chunks, immutable, named by a random id
//
// File bytes are cut into content-defined chunks of 64 KiB to 4 MiB (chunk.go).
// Each chunk is sealed on its own, and sealed chunks are packed together into
// objects of about 16 MiB, so a tree of 150 GiB is a few thousand PUTs rather
// than one per chunk. Files of at most 16 KiB are not chunked at all: their bytes
// are a record in the metadata shard, beside their inode. A hole in a file is no
// record at all: a file's size is its inode's, and only the runs of bytes that
// were written have chunks, so the cost of a file follows what was written and
// not how large it was declared.
//
// All metadata (inodes, directory entries, chunk references, the table saying
// where each chunk lives) is records in one ordered Merkle tree (tree.go), cut
// into shards. The Head holds the tree's root ref, the next inode number, the
// Volume's identity and the seed of its chunking table.
//
// Every object is sealed together with what it is: shards and chunks to their
// Volume, a Head to the Volume's place (Backend.String). An object copied in from
// another Volume, or a Volume copied to another place, does not open.
//
// Objects are written once, under names that are a hash of their content or a
// random id: a write that is repeated, or that completes after its caller gave
// up, puts the same bytes at the same name. The Head is the only object that
// changes, and only by compare-and-swap.
//
// # Commit
//
// A Volume that is written keeps a journal on its node's disk (journal/, docs/journal.md).
// A change (a write, a truncate, a create, a remove) is built from what the Volume holds in
// memory, sealed, appended to the journal, and applied, in that order and with no other
// change between. Nothing in that path waits for the store.
//
// A publisher, a goroutine of the Volume, takes everything applied up to one journal sequence
// at an instant (a generation: immutable from then on; changes made after it belong to the
// next), settles the files' writes into chunks and seals them into packs, writes the packs and
// the shards the generation touched (and no others) to the store in parallel, and then replaces
// the Head with one conditional write that names the new tree and carries the journal's
// sequence. Only that write changes any object a reader can reach. A crash, or a failure, before
// it leaves the previous Head and so the previous tree whole; the chunks, packs and shards
// already written are unreferenced garbage, never corruption. After it the journal is told the
// store has the sequence and reclaims its space. A generation is published within the bound
// (Config.Bound, 5 s) of the oldest change it holds, by a timer set for that change's own
// acknowledgement, and earlier when memory or the journal holds enough to make it worth doing,
// when Flush asks, and when the Volume is closed down by a mount.
//
// # Durability
//
// What is durable, and where, when a call returns. "Journal" is this node's disk: the
// journal's sync has returned for the change. "Store" is the Head advancing in the store: the
// conditional PUT of the Head has returned success.
//
//	call                                   durable when it returns
//	File.WriteAt, Truncate, Create,        nothing. The change is in memory and in the journal's
//	  Mkdir, Remove                          buffer: it survives the process ending only if the
//	                                         journal had written it, and a machine ending only if the
//	                                         journal had synced it. Visible to every reader at once.
//	File.Sync, Volume.SyncInode,           the journal, for the changes the file depends on (all of
//	  Volume.Sync                            them, for a directory or the Volume): they survive a crash
//	                                         of the process and of the machine. Concurrent callers
//	                                         share one disk sync. The store is not asked.
//	File.Close                             nothing, and no request of the store: it releases the
//	                                         handle. A handle that only read touches nothing.
//	Volume.Flush                           the store, for everything changed before the call: it
//	                                         returns after a Head that carries it has committed.
//	FUSE flush (every close(2))            nothing, and no request: constant time.
//	FUSE fsync                             the journal.
//	publication by the Volume              the store, within Config.Bound of each change's own
//	                                         acknowledgement while the store answers.
//	end of a mount (SIGTERM, detach)       the kernel hands its cached writes to the daemon (syncfs),
//	                                         then one Flush; if the store does not answer in time what
//	                                         it lacks stays in the journal and the next mount of the
//	                                         Volume on this node publishes it.
//	Volume.Close                           the journal, whatever was changed and not published; no
//	                                         request. It does not flush: a caller that hands the Volume
//	                                         over calls Flush first. A journal with nothing left in it is
//	                                         removed.
//	Attach, the same node                  every change the previous owner acknowledged with Sync (and
//	                                         every one its journal had written) is replayed from the
//	                                         journal, published under the new Round, and in the new
//	                                         owner's tree before Attach returns.
//	Attach, another node or a lost disk    the last Head. What the previous owner acknowledged and had
//	                                         not published is not here: Recovery reports Lost, and a
//	                                         caller must not tell its users those changes are durable.
//
// A node that is lost for good loses at most the changes of the last Bound (5 s) that its
// journal held and the store did not; a process that crashes loses none that a Sync returned
// for. The change from the earlier rule, that a call is durable only when the Head advanced
// in the store, is deliberate and is for coding and development workloads; a policy that has
// fsync return after the Head commits (remote durability) needs a Sync that asks for a
// Flush, and no other change.
//
// Backpressure and the state. A change that finds the journal three quarters full is slowed;
// one that finds it full, or memory full while the store is not taking publications, is
// refused with ErrFull (the mount answers ENOSPC) and changes nothing. When the journal
// fails the Volume takes no more changes (ErrJournal, EROFS). State reports whether the
// bound can be trusted: Degraded, the age of the oldest change the store lacks, and the
// bytes of journal behind it. The writes the kernel itself holds back from the daemon are
// the caller's to fsync, as on any file system; the bound runs from a change's entry into the
// journal.
//
// Interrupts on the mount. A signal that reaches a caller while it waits on the mount
// (Go's runtime sends one every few milliseconds) makes the kernel interrupt the
// request, and no request is answered EINTR: every call waits and returns its own
// result. Callers do not restart it: SQLite reports an fsync answered EINTR as an I/O
// error, a stat answered EINTR is an error to a C caller whether or not it set
// SA_RESTART, a read answered EINTR reaches its caller as EIO, close is never
// restarted, and a restarted create(2) with O_EXCL would meet its own file. Nothing a
// caller's interrupt does cancels a journal sync or a fetch. The cost is that a call waits
// as long as the store takes when it needs it (a read of a chunk not held, a lookup of a
// shard not held): store requests have no deadline yet (docs/SPECIFICATION.md, section 11.2).
//
// What a failed Flush tells the caller. nil: published. Any other error: the outcome of that
// attempt is unknown to the caller (a reply can be lost after the write landed; the
// publisher reads the Head back and, if its own generation is in it, says nil), and the
// changes stay in memory and in the journal for the next attempt. ErrStaleRound: this
// Volume was fenced and can never publish again; changes it had not published will never
// land, but a commit whose acknowledgement was lost may already be in the successor's tree, and
// Flush reports nil when the store's Head is exactly this Volume's tree.
//
// The crash matrix the tests cover (a process killed by SIGKILL, or a call made
// to fail, at each point of a publication, then the store read from another process, and
// the Volume attached again on the same journal):
//
//	point                                  test
//	before the first pack upload           TestSigkill/pack
//	after the first pack upload            TestSigkill/packed
//	before the first shard upload          TestSigkill/shard
//	after the first shard upload           TestSigkill/sharded
//	before the Head write                  TestSigkill/head
//	after the Head write                   TestSigkill/landed
//	each write of a publication failing    TestFailureAtEveryWrite
//	Head landed, reply lost, then retry    TestLostAcknowledgementOfHead, TestLostAckNoLostUpdateNoDuplicate
//	Head landed, reply lost, takeover      TestLostAckThenTakeoverIsNotReportedLost
//	process ends with an unpublished tail  TestProcessCrashReplays, TestFUSEFsyncSurvivesKill
//	replay interrupted, replayed again     TestReplayTwice
//	takeover on another node               TestTakeoverOnAnotherNodeSaysWhatItCannotKnow
//	takeover during a publication          TestTakeoverDuringPublication
//	concurrent writes, Flushes, syncs      TestGenerationOrdering
//	the bound, with a clock the test moves TestBoundIsKeptFromEachAcknowledgement
//	the store down, journal full           TestBackpressureAndRecovery, TestMemoryBoundWaitsThenRefuses
//	a request held for 2 s                 TestStalledCommitStallsNobody
//	kill at a random moment, real server   TestKillOverRealServer
//
// In every row, a fresh view of the store is the last published generation, whole.
//
// # Fence
//
// Attach claims the Volume at a Round, a number that only grows. The claim is a
// conditional write of the Head that also carries forward the previous owner's
// last committed Head, so no acknowledged commit is lost, and from then on a
// commit by any older Round is refused with ErrStaleRound. A claim whose write
// landed but whose reply was lost is recognised as the claimant's own. Round
// 2^64-1 is refused, since no Round could follow it.
//
// The store is trusted to return the Head it holds. A store that returns an older
// Head it once held cannot be told from the current one by this library: a caller
// that must notice a rollback keeps the Seq of the last Head it saw and compares.
//
// # Keys
//
// Every object goes through a Cipher (cipher.go); the library never reads a key
// from a file or the environment and never stores one.
package vfs
