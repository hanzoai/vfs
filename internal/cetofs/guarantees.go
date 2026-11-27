// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package cetofs implements a clean-room redundant-request audit and
// guarantee-preserving optimization layer across the Hanzo VFS boundaries:
//
//	FUSE/Gofer handler -> *vfs.Volume -> backend.Backend (Hanzo S3)
//
// Inspired by the boundary-accounting methodology of CETOFS (FAST 2026), this
// package counts every request crossing each boundary per application
// operation, classifies redundant traffic, and provides opt-in optimizations
// that never add a second server-side filesystem and never alter compare-and-set
// (CAS) authority or the fenced Head.
package cetofs

// Boundary identifies which architectural interface a request crosses.
type Boundary string

const (
	// BoundaryFUSEGofer is the host FUSE / sandbox Gofer to *vfs.Volume boundary.
	BoundaryFUSEGofer Boundary = "fuse_gofer->vfs"
	// BoundaryVFSBackend is the *vfs.Volume / *fence.Store to backend.Backend (S3) boundary.
	BoundaryVFSBackend Boundary = "vfs->s3_backend"
)

// OpType identifies a tracked request operation at either boundary.
type OpType string

const (
	// FUSE / Gofer -> VFS operations.
	OpGetAttr OpType = "GetAttr"
	OpLookup  OpType = "Lookup"
	OpOpen    OpType = "Open"
	OpRead    OpType = "Read"
	OpWrite   OpType = "Write"
	OpReadDir OpType = "ReadDir"
	OpCreate  OpType = "Create"
	OpMkdir   OpType = "Mkdir"
	OpSetAttr OpType = "SetAttr"
	OpRemove  OpType = "Remove"
	OpFlush   OpType = "Flush"

	// VFS -> S3 / Backend operations.
	OpBackendGet            OpType = "BackendGet"
	OpBackendPut            OpType = "BackendPut"
	OpBackendConditionalPut OpType = "BackendConditionalPut"
	OpBackendHead           OpType = "BackendHead"
	OpBackendList           OpType = "BackendList"
	OpBackendRangeGet       OpType = "BackendRangeGet"
	OpBackendDelete         OpType = "BackendDelete"
)

// BoundaryOf returns the boundary to which op belongs.
func BoundaryOf(op OpType) Boundary {
	switch op {
	case OpBackendGet, OpBackendPut, OpBackendConditionalPut,
		OpBackendHead, OpBackendList, OpBackendRangeGet, OpBackendDelete:
		return BoundaryVFSBackend
	default:
		return BoundaryFUSEGofer
	}
}

// RedundancyClass names a specific category of redundant boundary traffic.
type RedundancyClass string

const (
	RedundancyRepeatedGetAttr     RedundancyClass = "repeated_getattr"
	RedundancyRepeatedLookup      RedundancyClass = "repeated_lookup"
	RedundancyRepeatedReadDir     RedundancyClass = "repeated_readdir"
	RedundancyRedundantOpenStat   RedundancyClass = "redundant_open_stat"
	RedundancyPrePutGet           RedundancyClass = "redundant_pre_put_get"
	RedundancyRepeatedHeadRead    RedundancyClass = "repeated_head_read"
	RedundancyRepeatedShardRead   RedundancyClass = "repeated_shard_read"
	RedundancyRepeatedList        RedundancyClass = "repeated_list"
	RedundancyUncoalescedRangeGet RedundancyClass = "uncoalesced_range_get"
)

// AllRedundancyClasses returns every tracked redundancy class in canonical order.
func AllRedundancyClasses() []RedundancyClass {
	return []RedundancyClass{
		RedundancyRepeatedGetAttr,
		RedundancyRepeatedLookup,
		RedundancyRepeatedReadDir,
		RedundancyRedundantOpenStat,
		RedundancyPrePutGet,
		RedundancyRepeatedHeadRead,
		RedundancyRepeatedShardRead,
		RedundancyRepeatedList,
		RedundancyUncoalescedRangeGet,
	}
}

// GuaranteeRecord documents the exact guarantee a removed request class had
// been protecting (or that it protected none), how the CETOFS optimization
// preserves or strictly bounds that guarantee, and the negative regime where
// the optimization degrades.
type GuaranteeRecord struct {
	Class              RedundancyClass `json:"class"`
	Boundary           Boundary        `json:"boundary"`
	Optimization       string          `json:"optimization"`
	PreviousGuarantee  string          `json:"previous_guarantee"`
	PreservedGuarantee string          `json:"preserved_guarantee"`
	NegativeRegime     string          `json:"negative_regime"`
	SafetyInvariant    string          `json:"safety_invariant"`
}

// GuaranteeCatalog returns the complete, canonical guarantee statement table
// for every removed request class in internal/cetofs.
func GuaranteeCatalog() []GuaranteeRecord {
	return []GuaranteeRecord{
		{
			Class:              RedundancyRepeatedGetAttr,
			Boundary:           BoundaryFUSEGofer,
			Optimization:       "Bounded-TTL AttrTimeout cache with immediate local-mutation and Head-generation invalidation",
			PreviousGuarantee:  "Cross-writer inode attribute freshness (size, mtime, mode, nlink) at single-syscall granularity.",
			PreservedGuarantee: "100% read-your-own-writes consistency via synchronous invalidation on Write/SetAttr/Create/Mkdir/Remove/Flush and fenced-Head generation bumps; cross-writer staleness strictly bounded to AttrTimeout.",
			NegativeRegime:     "External concurrent writer mutating attributes faster than AttrTimeout observes stale cached metadata until TTL expiry or Head generation bump.",
			SafetyInvariant:    "Local mutations synchronously evict affected inode attr before returning; expired entries are never served.",
		},
		{
			Class:              RedundancyRepeatedLookup,
			Boundary:           BoundaryFUSEGofer,
			Optimization:       "Bounded-TTL EntryTimeout cache for (parentIno, name) -> Inode with immediate local-mutation invalidation",
			PreviousGuarantee:  "Immediate visibility of external directory entry creation, unlink, or rename on every path component walk.",
			PreservedGuarantee: "100% local namespace coherence via synchronous invalidation on Create/Mkdir/Remove/Rename and fenced-Head generation bumps; cross-writer staleness strictly bounded to EntryTimeout.",
			NegativeRegime:     "External writer creating or unlinking names faster than EntryTimeout sees stale positive entries until TTL expiry or Head generation bump.",
			SafetyInvariant:    "Local Create/Mkdir/Remove synchronously invalidate (parent, name) and parent dir entries before returning.",
		},
		{
			Class:              RedundancyRepeatedReadDir,
			Boundary:           BoundaryFUSEGofer,
			Optimization:       "Bounded-TTL DirCache caching directory entries and parent inode together",
			PreviousGuarantee:  "Immediate visibility of external directory membership updates on every readdir + parent lookup.",
			PreservedGuarantee: "100% local directory listing coherence via immediate invalidation on any child mutation in dir and on fenced-Head generation bumps; cross-writer staleness strictly bounded to DirCacheTTL.",
			NegativeRegime:     "Externally churned directories within DirCacheTTL return stale child sets until TTL expiry or invalidation.",
			SafetyInvariant:    "Any local Create/Mkdir/Remove under dir synchronously evicts DirCache[dir].",
		},
		{
			Class:              RedundancyRedundantOpenStat,
			Boundary:           BoundaryFUSEGofer,
			Optimization:       "SkipRedundantOpenStat using cached immutable inode type (regular vs directory) prior to vol.Open",
			PreviousGuarantee:  "None beyond AttrTimeout: pkg/mount/fuse_unix.go calls vol.Stat solely to check in.IsDir(), but an allocated inode ID never changes file type in place.",
			PreservedGuarantee: "No guarantee weakened: directory inodes are still rejected/handled as directories, and cache misses or unlinked inodes fall back directly to vol.Stat and vol.Open.",
			NegativeRegime:     "None on valid POSIX volumes; cold cache falls back cleanly to vol.Stat.",
			SafetyInvariant:    "An inode ID's file type bit (ModeDir vs regular) is immutable for its lifetime; unlinked inodes still return fs.ErrNotExist from vol.Open.",
		},
		{
			Class:              RedundancyPrePutGet,
			Boundary:           BoundaryVFSBackend,
			Optimization:       "EnableHeadVersionCache tracking the last observed backend.Version (ETag) for fenced Head conditional writes",
			PreviousGuarantee:  "Precondition ETag acquisition for server-side compare-and-set (PutIfVersion / S3 If-Match) prior to committing fenced Head.",
			PreservedGuarantee: "100% CAS authority and fenced-Head monotonicity preserved: every conditional write still executes server-side PutIfVersion(key, data, cachedVersion); any backend.ErrConflict immediately invalidates the cache and retries against an authoritative Get + round check.",
			NegativeRegime:     "Continuous multi-writer CAS contention causes optimistic cached PutIfVersion to fail with ErrConflict, adding a failed conditional PUT round-trip before authoritative Get+retry.",
			SafetyInvariant:    "OptimizedBackend never converts PutIfVersion into unconditional Put and never bypasses fence.ErrStaleRound.",
		},
		{
			Class:              RedundancyRepeatedHeadRead,
			Boundary:           BoundaryVFSBackend,
			Optimization:       "Opt-in bounded-TTL HeadCacheTTL read-through (default 0 = strict linearizable reads)",
			PreviousGuarantee:  "Linearizable observation of the latest externally committed Head by independent readers (Volume.Open, fence.Store.Round).",
			PreservedGuarantee: "With HeadCacheTTL == 0 (default), linearizability is 100% unchanged. With HeadCacheTTL > 0, staleness is bounded by HeadCacheTTL and immediately invalidated on local writes or ErrConflict.",
			NegativeRegime:     "External Head commits or round takeovers within HeadCacheTTL are hidden from read-only callers until TTL expiry or conflict invalidation.",
			SafetyInvariant:    "Any local PutIfVersion, PutIfAbsent, Delete, or ErrConflict synchronously invalidates cached Head payload and bumps generation.",
		},
		{
			Class:              RedundancyRepeatedShardRead,
			Boundary:           BoundaryVFSBackend,
			Optimization:       "EnableShardCache bounded LRU for immutable content-addressed meta/<2hex>/<blake3hex> shards and immutable Stat",
			PreviousGuarantee:  "None: meta/ shards are content-addressed by the BLAKE3 digest of their sealed bytes (SPECIFICATION 3) and immutable once written.",
			PreservedGuarantee: "100% content integrity preserved: every cached meta/ shard is verified against blake3.Sum(data) == keyDigest before insertion; tampered backend responses fail closed and are never cached.",
			NegativeRegime:     "One-shot cold scans larger than MaxShardCacheBytes incur bounded LRU eviction overhead with 0% reuse (no correctness loss).",
			SafetyInvariant:    "Only immutable digest-verified meta/<2hex>/<64hex> objects enter ShardCache; returned slices are defensive copies.",
		},
		{
			Class:              RedundancyRepeatedList,
			Boundary:           BoundaryVFSBackend,
			Optimization:       "Bounded-TTL ListCacheTTL prefix listing cache with immediate local mutation invalidation",
			PreviousGuarantee:  "Point-in-time visibility of objects added or deleted externally under the listed prefix.",
			PreservedGuarantee: "100% local write visibility via immediate invalidation of any cached prefix matching local Put/PutIfAbsent/PutIfVersion/Delete; external staleness strictly bounded to ListCacheTTL.",
			NegativeRegime:     "External writers or garbage collectors modifying objects under the prefix within ListCacheTTL observe stale listings until expiry.",
			SafetyInvariant:    "Any mutating backend call synchronously evicts every cached prefix that is a prefix of the mutated key.",
		},
		{
			Class:              RedundancyUncoalescedRangeGet,
			Boundary:           BoundaryVFSBackend,
			Optimization:       "EnableRangeCoalescing merging contiguous/near-contiguous GetRange spans on immutable pack/ objects within MaxCoalesceGap and MaxCoalesceWindow",
			PreviousGuarantee:  "None for immutable pack/ objects: packs are write-once sealed chunk containers and every chunk is still individually authenticated against its chunkID and chunkAAD.",
			PreservedGuarantee: "100% chunk integrity and backend.Backend.GetRange EOF contract preserved: arithmetic is overflow-checked and sequential read-ahead is clamped to exact known pack size so reads never cross EOF.",
			NegativeRegime:     "Purely random sparse chunk reads suffer read amplification if non-adaptive read-ahead is forced; adaptive adjacency gating avoids unnecessary prefetch on random access.",
			SafetyInvariant:    "All range offsets/lengths are validated against negative values and int64 overflow; returned byte slices match exact uncoalesced GetRange output byte-for-byte.",
		},
	}
}

// GuaranteeFor looks up the GuaranteeRecord for a specific RedundancyClass.
func GuaranteeFor(class RedundancyClass) (GuaranteeRecord, bool) {
	for _, rec := range GuaranteeCatalog() {
		if rec.Class == class {
			return rec, true
		}
	}
	return GuaranteeRecord{}, false
}
