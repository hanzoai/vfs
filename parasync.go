// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

// ErrSourceCheckpointGCed is returned when a shard or pack required by the
// source checkpoint disappears from source during planning or transfer (for
// example, due to concurrent source checkpoint garbage collection). It wraps
// both ErrCorrupt and backend.ErrNotFound, and leaves destination untouched.
var ErrSourceCheckpointGCed = fmt.Errorf("vfs: source checkpoint object was garbage-collected during restore (%w: %w)", ErrCorrupt, backend.ErrNotFound)

const (
	defaultParaSyncStreams = 8
	// maxParaSyncStagedShardBytes bounds how many unpruned source shard ciphertexts
	// RestoreDelta keeps in memory between planning and destination upload so small
	// and medium restores avoid double-fetching shards without unbounded RAM growth.
	maxParaSyncStagedShardBytes = 128 << 20
	paraSyncExtName             = "parasync"
	paraSyncReceiptMagic        = "HNZPRCPT1"
)

// DeltaPlan describes the verified minimal object-level transfer required to
// restore source's committed checkpoint onto destination without modifying the
// canonical format-v2 layout or re-encrypting pack payloads.
type DeltaPlan struct {
	SrcHead           Head
	DstHead           Head
	HasDst            bool
	ShardsToCopy      []string
	ShardsPruned      int
	PacksToCopy       []string
	PacksPruned       int
	TotalSourceShards int
	TotalSourcePacks  int
	MetaBytes         int64
	PackBytes         int64
	FullMetaBytes     int64
	FullPackBytes     int64
	FullSnapshotBytes int64
}

// RestoreOptions configures a ParaSync restore execution.
type RestoreOptions struct {
	// Round claims the destination Head at this round (must exceed the
	// destination's recorded round, and be in (0, 2^64-1)).
	Round uint64
	// Streams bounds concurrent backend GET/Stat/PutIfAbsent operations (default 8).
	Streams int
	// SaveVerifyReceipt writes a Cipher-sealed integrity receipt under
	// ext/parasync/<keyed-hash> after end-to-end chunk verification succeeds,
	// binding (packID, dstVersion, packSize) under the Volume's Cipher.
	SaveVerifyReceipt bool
	// TrustSealedReceipt allows verifyRestoredTree to reuse a previously written
	// Cipher-sealed ext/parasync/<keyed-hash> receipt for unchanged destination
	// packs whose backend Version and size still match. When false (the default),
	// every referenced chunk is read from dst, unsealed with chunkAAD, and hashed
	// with BLAKE3 before Head CAS.
	TrustSealedReceipt bool
}

// RestoreStats reports measured counts, transferred bytes, verification traffic,
// resource usage, and timings from a RestoreDelta execution.
type RestoreStats struct {
	Plan                 DeltaPlan
	ShardsCopied         int
	PacksCopied          int
	BytesTransferred     int64
	FullSnapshotBytes    int64
	SourceGets           int64
	DestPuts             int64
	DestStats            int64
	VerifyBytesRead      int64
	VerifyGets           int64
	ChunksVerified       int
	ReceiptsVerified     int
	DestGCRecoveredPacks int
	LostHeadACKRecovered bool
	Duration             time.Duration
	CPUTime              time.Duration
	PeakGoroutines       int
}

// parasyncCountingBackend wraps a backend.Backend to measure real storage calls
// and transferred payload bytes while preserving String() for headAAD sealing.
type parasyncCountingBackend struct {
	backend.Backend
	gets          atomic.Int64
	getRanges     atomic.Int64
	stats         atomic.Int64
	puts          atomic.Int64
	putIfAbsents  atomic.Int64
	putIfVersions atomic.Int64
	deletes       atomic.Int64
	bytesRead     atomic.Int64
	bytesWritten  atomic.Int64
}

func newParaSyncCountingBackend(be backend.Backend) *parasyncCountingBackend {
	return &parasyncCountingBackend{Backend: be}
}

func (b *parasyncCountingBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	b.gets.Add(1)
	data, ver, err := b.Backend.Get(ctx, key, max)
	if err == nil {
		b.bytesRead.Add(int64(len(data)))
	}
	return data, ver, err
}

func (b *parasyncCountingBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b.getRanges.Add(1)
	data, err := b.Backend.GetRange(ctx, key, off, n)
	if err == nil {
		b.bytesRead.Add(int64(len(data)))
	}
	return data, err
}

func (b *parasyncCountingBackend) Put(ctx context.Context, key string, data []byte) error {
	b.puts.Add(1)
	err := b.Backend.Put(ctx, key, data)
	if err == nil {
		b.bytesWritten.Add(int64(len(data)))
	}
	return err
}

func (b *parasyncCountingBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	b.puts.Add(1)
	b.putIfAbsents.Add(1)
	ver, err := b.Backend.PutIfAbsent(ctx, key, data)
	if err == nil {
		b.bytesWritten.Add(int64(len(data)))
	}
	return ver, err
}

func (b *parasyncCountingBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	b.puts.Add(1)
	b.putIfVersions.Add(1)
	ver, err := b.Backend.PutIfVersion(ctx, key, data, v)
	if err == nil {
		b.bytesWritten.Add(int64(len(data)))
	}
	return ver, err
}

func (b *parasyncCountingBackend) Delete(ctx context.Context, key string) error {
	b.deletes.Add(1)
	return b.Backend.Delete(ctx, key)
}

func (b *parasyncCountingBackend) Stat(ctx context.Context, key string) (int64, error) {
	b.stats.Add(1)
	return b.Backend.Stat(ctx, key)
}

type dstNodeInfo struct {
	level            int
	sealedLen        int64
	children         []ref
	packs            map[packID]uint64
	subtreeShards    int
	subtreeMetaBytes int64
}

type planExtras struct {
	dstRecordedRound uint64
	shardsToCopyRefs map[string]ref
	stagedShards     map[string][]byte
	packSizes        map[string]int64
}

// PlanDelta inspects source and destination checkpoints, verifies every visited
// shard against its BLAKE3 content address and volume sealing context, prunes
// subtrees already reachable in destination in O(1), and determines the exact
// shards and packs required to restore source onto destination.
func PlanDelta(ctx context.Context, src, dst backend.Backend, c Cipher) (*DeltaPlan, error) {
	if ctx == nil {
		return nil, errors.New("vfs: parasync requires a non-nil context")
	}
	if src == nil || dst == nil || c == nil {
		return nil, errors.New("vfs: parasync requires source backend, destination backend, and cipher")
	}
	if err := check(c); err != nil {
		return nil, err
	}
	var peakG atomic.Int64
	plan, _, err := planDelta(ctx, src, dst, c, defaultParaSyncStreams, false, &peakG)
	return plan, err
}

// RestoreDelta restores source's committed checkpoint onto destination using a
// parallel delta transfer (or full parallel restore when destination has no
// head), verifies the full restored tree on destination before touching head,
// and atomically advances destination head with round fencing.
func RestoreDelta(ctx context.Context, src, dst backend.Backend, c Cipher, opts RestoreOptions) (*RestoreStats, error) {
	if ctx == nil {
		return nil, errors.New("vfs: parasync requires a non-nil context")
	}
	if src == nil || dst == nil || c == nil {
		return nil, errors.New("vfs: parasync requires source backend, destination backend, and cipher")
	}
	if opts.Round == 0 || opts.Round == math.MaxUint64 {
		return nil, fmt.Errorf("%w: RestoreDelta needs a Round above 0 and below 2^64-1", fs.ErrInvalid)
	}
	if err := check(c); err != nil {
		return nil, err
	}

	streams := opts.Streams
	if streams <= 0 {
		streams = defaultParaSyncStreams
	}

	startWall := time.Now()
	startCPU := parasyncProcessCPU()
	var peakG atomic.Int64
	recordPeakGoroutines(&peakG)

	srcCount := newParaSyncCountingBackend(src)
	dstCount := newParaSyncCountingBackend(dst)

	plan, ext, err := planDelta(ctx, srcCount, dstCount, c, streams, true, &peakG)
	if err != nil {
		return nil, err
	}
	if plan.HasDst && opts.Round <= ext.dstRecordedRound {
		return nil, fmt.Errorf("%w: round %d does not exceed destination recorded round %d", ErrStaleRound, opts.Round, ext.dstRecordedRound)
	}

	var (
		packsCopied      atomic.Int64
		shardsCopied     atomic.Int64
		bytesTransferred atomic.Int64
		gcRecoveredPacks atomic.Int64
	)

	// Canonical VFS write order (LLM.md Rule 3): packs first, then shards, then head.
	if err := runParallel(ctx, streams, len(plan.PacksToCopy), &peakG, func(wctx context.Context, idx int) error {
		pk := plan.PacksToCopy[idx]
		expectedSize := ext.packSizes[pk]
		data, _, err := srcCount.Get(wctx, pk, expectedSize)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				return fmt.Errorf("%w: source pack %s is missing", ErrSourceCheckpointGCed, pk)
			}
			return fmt.Errorf("vfs: read source pack %s: %w", pk, err)
		}
		if int64(len(data)) != expectedSize {
			return fmt.Errorf("%w: source pack %s length %d, expected %d", ErrCorrupt, pk, len(data), expectedSize)
		}
		_, err = dstCount.PutIfAbsent(wctx, pk, data)
		switch {
		case err == nil:
			packsCopied.Add(1)
			bytesTransferred.Add(int64(len(data)))
			return nil
		case errors.Is(err, backend.ErrConflict):
			sz, serr := dstCount.Stat(wctx, pk)
			if serr != nil {
				return fmt.Errorf("vfs: stat conflicting destination pack %s: %w", pk, serr)
			}
			if sz != int64(len(data)) {
				return fmt.Errorf("%w: destination pack %s conflict has size %d, expected %d", ErrCorrupt, pk, sz, len(data))
			}
			return nil
		default:
			return fmt.Errorf("vfs: write destination pack %s: %w", pk, err)
		}
	}); err != nil {
		return nil, err
	}

	if err := runParallel(ctx, streams, len(plan.ShardsToCopy), &peakG, func(wctx context.Context, idx int) error {
		sk := plan.ShardsToCopy[idx]
		r, ok := ext.shardsToCopyRefs[sk]
		if !ok {
			var perr error
			if r, perr = parseShardRef(sk); perr != nil {
				return perr
			}
		}
		sealed, cached := ext.stagedShards[sk]
		if !cached {
			var err error
			sealed, _, err = srcCount.Get(wctx, sk, maxShard)
			if err != nil {
				if errors.Is(err, backend.ErrNotFound) {
					return fmt.Errorf("%w: source shard %x is missing", ErrSourceCheckpointGCed, r[:6])
				}
				return fmt.Errorf("vfs: read source shard %x: %w", r[:6], err)
			}
		}
		if blake3.Sum(sealed) != r {
			return fmt.Errorf("%w: shard %x does not hash to its name", ErrCorrupt, r[:6])
		}
		if _, err := unseal(c, shardAAD(plan.SrcHead.ID), sealed); err != nil {
			return fmt.Errorf("vfs: open source shard %x: %w", r[:6], err)
		}
		_, err := dstCount.PutIfAbsent(wctx, sk, sealed)
		switch {
		case err == nil:
			shardsCopied.Add(1)
			bytesTransferred.Add(int64(len(sealed)))
			return nil
		case errors.Is(err, backend.ErrConflict):
			sz, serr := dstCount.Stat(wctx, sk)
			if serr != nil {
				return fmt.Errorf("vfs: stat conflicting destination shard %s: %w", sk, serr)
			}
			if sz != int64(len(sealed)) {
				return fmt.Errorf("%w: destination shard %s conflict has size %d, expected %d", ErrCorrupt, sk, sz, len(sealed))
			}
			return nil
		default:
			return fmt.Errorf("vfs: write destination shard %s: %w", sk, err)
		}
	}); err != nil {
		return nil, err
	}
	// Release staged shard ciphertexts before verifying destination.
	ext.stagedShards = nil

	dstVerifyCount := newParaSyncCountingBackend(dst)
	vOutcome, err := verifyRestoredTree(
		ctx,
		srcCount,
		dstCount,
		dstVerifyCount,
		c,
		plan.SrcHead,
		streams,
		opts,
		&packsCopied,
		&shardsCopied,
		&bytesTransferred,
		&gcRecoveredPacks,
		&peakG,
	)
	if err != nil {
		return nil, err
	}

	var lostHeadACKRecovered bool
	dstFence := fence.New(dstCount, maxHead)
	if err := dstFence.Rewrite(ctx, headKey, opts.Round, func(payload []byte, recorded uint64) ([]byte, error) {
		if payload != nil {
			if opts.Round <= recorded {
				return nil, fmt.Errorf("%w: round %d does not exceed destination recorded round %d", ErrStaleRound, opts.Round, recorded)
			}
			dstPlain, err := unseal(c, headAAD(dstCount.String()), payload)
			if err != nil {
				return nil, fmt.Errorf("vfs: open destination head: %w", err)
			}
			curHead, err := unmarshalHead(dstPlain)
			if err != nil {
				return nil, fmt.Errorf("vfs: destination head: %w", err)
			}
			if curHead.ID != plan.SrcHead.ID {
				return nil, fmt.Errorf("%w: destination volume id %x does not match source %x", ErrCorrupt, curHead.ID[:6], plan.SrcHead.ID[:6])
			}
			if curHead.Seed != plan.SrcHead.Seed {
				return nil, fmt.Errorf("%w: destination volume seed does not match source", ErrCorrupt)
			}
		}
		return seal(c, headAAD(dstCount.String()), plan.SrcHead.marshal())
	}); err != nil {
		if parasyncHeadLanded(ctx, dstFence, dstCount.String(), c, plan.SrcHead, opts.Round) {
			lostHeadACKRecovered = true
		} else {
			return nil, err
		}
	}

	recordPeakGoroutines(&peakG)
	return &RestoreStats{
		Plan:                 *plan,
		ShardsCopied:         int(shardsCopied.Load()),
		PacksCopied:          int(packsCopied.Load()),
		BytesTransferred:     bytesTransferred.Load(),
		FullSnapshotBytes:    plan.FullSnapshotBytes,
		SourceGets:           srcCount.gets.Load(),
		DestPuts:             dstCount.puts.Load(),
		DestStats:            dstCount.stats.Load(),
		VerifyBytesRead:      dstVerifyCount.bytesRead.Load(),
		VerifyGets:           dstVerifyCount.gets.Load() + dstVerifyCount.getRanges.Load(),
		ChunksVerified:       vOutcome.chunksVerified,
		ReceiptsVerified:     vOutcome.receiptsVerified,
		DestGCRecoveredPacks: int(gcRecoveredPacks.Load()),
		LostHeadACKRecovered: lostHeadACKRecovered,
		Duration:             time.Since(startWall),
		CPUTime:              parasyncProcessCPU() - startCPU,
		PeakGoroutines:       int(peakG.Load()),
	}, nil
}

func planDelta(ctx context.Context, src, dst backend.Backend, c Cipher, streams int, retainShards bool, peakG *atomic.Int64) (*DeltaPlan, *planExtras, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	srcFence := fence.New(src, maxHead)
	srcPayload, _, err := srcFence.Get(ctx, headKey)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, nil, fmt.Errorf("vfs: no volume at %s: %w", src, fs.ErrNotExist)
		}
		return nil, nil, fmt.Errorf("vfs: read source head: %w", err)
	}
	srcPlain, err := unseal(c, headAAD(src.String()), srcPayload)
	if err != nil {
		return nil, nil, fmt.Errorf("vfs: open source head: %w", err)
	}
	srcHead, err := unmarshalHead(srcPlain)
	if err != nil {
		return nil, nil, fmt.Errorf("vfs: source head: %w", err)
	}

	plan := &DeltaPlan{SrcHead: srcHead}
	ext := &planExtras{
		shardsToCopyRefs: map[string]ref{},
		stagedShards:     map[string][]byte{},
		packSizes:        map[string]int64{},
	}

	dstFence := fence.New(dst, maxHead)
	dstPayload, dstRecorded, err := dstFence.Get(ctx, headKey)
	switch {
	case err == nil:
		dstPlain, uerr := unseal(c, headAAD(dst.String()), dstPayload)
		if uerr != nil {
			return nil, nil, fmt.Errorf("vfs: open destination head: %w", uerr)
		}
		dstHead, herr := unmarshalHead(dstPlain)
		if herr != nil {
			return nil, nil, fmt.Errorf("vfs: destination head: %w", herr)
		}
		if dstHead.ID != srcHead.ID {
			return nil, nil, fmt.Errorf("%w: destination volume id %x does not match source %x", ErrCorrupt, dstHead.ID[:6], srcHead.ID[:6])
		}
		if dstHead.Seed != srcHead.Seed {
			return nil, nil, fmt.Errorf("%w: destination volume seed does not match source", ErrCorrupt)
		}
		plan.HasDst = true
		plan.DstHead = dstHead
		ext.dstRecordedRound = dstRecorded
	case errors.Is(err, backend.ErrNotFound):
		plan.HasDst = false
	default:
		return nil, nil, fmt.Errorf("vfs: read destination head: %w", err)
	}

	var dstNodes map[ref]*dstNodeInfo
	if plan.HasDst && plan.DstHead.Root != none {
		dstNodes, err = collectDstSubtrees(ctx, dst, c, plan.DstHead.ID, plan.DstHead.Root, streams, peakG)
		if err != nil {
			return nil, nil, err
		}
	}

	requiredPacks := map[packID]uint64{}
	if srcHead.Root != none {
		if err := collectSourceDelta(ctx, src, c, srcHead.ID, srcHead.Root, dstNodes, streams, retainShards, peakG, plan, ext, requiredPacks); err != nil {
			return nil, nil, err
		}
	}

	sort.Strings(plan.ShardsToCopy)
	plan.TotalSourceShards = len(plan.ShardsToCopy) + plan.ShardsPruned

	if len(requiredPacks) > 0 {
		type packCheck struct {
			id          packID
			key         string
			requiredEnd uint64
			pruned      bool
			size        int64
		}
		checks := make([]packCheck, 0, len(requiredPacks))
		for id, end := range requiredPacks {
			checks = append(checks, packCheck{
				id:          id,
				key:         packKey(id),
				requiredEnd: end,
			})
		}
		sort.Slice(checks, func(i, j int) bool { return checks[i].key < checks[j].key })

		if err := runParallel(ctx, streams, len(checks), peakG, func(wctx context.Context, idx int) error {
			ch := &checks[idx]
			dstSz, err := dst.Stat(wctx, ch.key)
			if err == nil && uint64(dstSz) >= ch.requiredEnd {
				ch.pruned = true
				ch.size = dstSz
				return nil
			}
			if err != nil && !errors.Is(err, backend.ErrNotFound) {
				return fmt.Errorf("vfs: stat destination pack %s: %w", ch.key, err)
			}
			srcSz, serr := src.Stat(wctx, ch.key)
			if serr != nil {
				if errors.Is(serr, backend.ErrNotFound) {
					return fmt.Errorf("%w: pack %x is missing from source", ErrSourceCheckpointGCed, ch.id[:6])
				}
				return fmt.Errorf("vfs: stat source pack %s: %w", ch.key, serr)
			}
			if uint64(srcSz) < ch.requiredEnd {
				return fmt.Errorf("%w: source pack %x has %d bytes and a chunk ends at %d", ErrCorrupt, ch.id[:6], srcSz, ch.requiredEnd)
			}
			ch.size = srcSz
			return nil
		}); err != nil {
			return nil, nil, err
		}

		for _, ch := range checks {
			plan.FullPackBytes += ch.size
			if ch.pruned {
				plan.PacksPruned++
			} else {
				plan.PacksToCopy = append(plan.PacksToCopy, ch.key)
				plan.PackBytes += ch.size
				ext.packSizes[ch.key] = ch.size
			}
		}
	}

	plan.TotalSourcePacks = len(plan.PacksToCopy) + plan.PacksPruned
	plan.FullSnapshotBytes = plan.FullMetaBytes + plan.FullPackBytes
	return plan, ext, nil
}

func collectDstSubtrees(ctx context.Context, dst backend.Backend, c Cipher, id volumeID, root ref, streams int, peakG *atomic.Int64) (map[ref]*dstNodeInfo, error) {
	aad := shardAAD(id)
	nodes := map[ref]*dstNodeInfo{}
	var levels [][]ref
	frontier := []ref{root}
	expectedLevel := -1

	for len(frontier) > 0 {
		type fetchRes struct {
			r    ref
			info *dstNodeInfo
		}
		res := make([]fetchRes, len(frontier))
		if err := runParallel(ctx, streams, len(frontier), peakG, func(wctx context.Context, idx int) error {
			r := frontier[idx]
			sealed, _, err := dst.Get(wctx, shardKey(r), maxShard)
			if err != nil {
				if errors.Is(err, backend.ErrNotFound) {
					return fmt.Errorf("%w: destination shard %x is missing", ErrCorrupt, r[:6])
				}
				return fmt.Errorf("vfs: read destination shard %x: %w", r[:6], err)
			}
			if blake3.Sum(sealed) != r {
				return fmt.Errorf("%w: destination shard %x does not hash to its name", ErrCorrupt, r[:6])
			}
			plain, err := unseal(c, aad, sealed)
			if err != nil {
				return fmt.Errorf("vfs: open destination shard %x: %w", r[:6], err)
			}
			n, err := decodeNode(plain)
			if err != nil {
				return fmt.Errorf("vfs: decode destination shard %x: %w", r[:6], err)
			}
			if expectedLevel >= 0 && n.level != expectedLevel {
				return fmt.Errorf("%w: destination shard %x has level %d, want %d", ErrCorrupt, r[:6], n.level, expectedLevel)
			}
			info := &dstNodeInfo{
				level:     n.level,
				sealedLen: int64(len(sealed)),
			}
			if n.level > 0 {
				info.children = make([]ref, len(n.vals))
				for i, v := range n.vals {
					info.children[i] = ref(v)
				}
			} else {
				packs, err := extractLeafPacks(n)
				if err != nil {
					return err
				}
				info.packs = packs
			}
			res[idx] = fetchRes{r: r, info: info}
			return nil
		}); err != nil {
			return nil, err
		}

		levelRefs := make([]ref, 0, len(res))
		var nextFrontier []ref
		levelVal := -1
		for _, item := range res {
			nodes[item.r] = item.info
			levelRefs = append(levelRefs, item.r)
			if levelVal < 0 {
				levelVal = item.info.level
			} else if item.info.level != levelVal {
				return nil, fmt.Errorf("%w: destination tree level mismatch (%d vs %d)", ErrCorrupt, item.info.level, levelVal)
			}
			for _, ch := range item.info.children {
				if _, seen := nodes[ch]; !seen {
					nodes[ch] = nil // reserve against duplicates in same tree
					nextFrontier = append(nextFrontier, ch)
				}
			}
		}
		levels = append(levels, levelRefs)
		if levelVal == 0 && len(nextFrontier) > 0 {
			return nil, fmt.Errorf("%w: leaf shard has children", ErrCorrupt)
		}
		expectedLevel = levelVal - 1
		frontier = dedupRefs(nextFrontier)
	}

	// Aggregate exact subtree shard counts and sealed byte totals bottom-up.
	for i := len(levels) - 1; i >= 0; i-- {
		for _, r := range levels[i] {
			info := nodes[r]
			shards := 1
			metaBytes := info.sealedLen
			for _, ch := range info.children {
				cinfo := nodes[ch]
				if cinfo == nil {
					return nil, fmt.Errorf("%w: destination subtree %x missing child %x", ErrCorrupt, r[:6], ch[:6])
				}
				shards += cinfo.subtreeShards
				metaBytes += cinfo.subtreeMetaBytes
			}
			info.subtreeShards = shards
			info.subtreeMetaBytes = metaBytes
		}
	}

	return nodes, nil
}

func collectSourceDelta(
	ctx context.Context,
	src backend.Backend,
	c Cipher,
	id volumeID,
	root ref,
	dstNodes map[ref]*dstNodeInfo,
	streams int,
	retainShards bool,
	peakG *atomic.Int64,
	plan *DeltaPlan,
	ext *planExtras,
	requiredPacks map[packID]uint64,
) error {
	aad := shardAAD(id)
	visited := map[ref]struct{}{}
	var stagedBytes atomic.Int64

	type srcItem struct {
		r             ref
		expectedLevel int // -1 for root
	}

	pruneSubtree := func(r ref, expectedLevel int) (bool, error) {
		if dstNodes == nil {
			return false, nil
		}
		info, ok := dstNodes[r]
		if !ok || info == nil {
			return false, nil
		}
		if expectedLevel >= 0 && info.level != expectedLevel {
			return false, fmt.Errorf("%w: source subtree %x level %d mismatches destination level %d", ErrCorrupt, r[:6], expectedLevel, info.level)
		}
		plan.ShardsPruned += info.subtreeShards
		plan.FullMetaBytes += info.subtreeMetaBytes
		collectPrunedPacks(dstNodes, r, requiredPacks)
		return true, nil
	}

	visited[root] = struct{}{}
	if pruned, err := pruneSubtree(root, -1); err != nil {
		return err
	} else if pruned {
		return nil
	}

	frontier := []srcItem{{r: root, expectedLevel: -1}}
	for len(frontier) > 0 {
		type shardRes struct {
			r         ref
			key       string
			level     int
			sealedLen int64
			sealed    []byte
			children  []ref
			packs     map[packID]uint64
		}
		results := make([]shardRes, len(frontier))
		if err := runParallel(ctx, streams, len(frontier), peakG, func(wctx context.Context, idx int) error {
			it := frontier[idx]
			sk := shardKey(it.r)
			sealed, _, err := src.Get(wctx, sk, maxShard)
			if err != nil {
				if errors.Is(err, backend.ErrNotFound) {
					return fmt.Errorf("%w: source shard %x is missing", ErrSourceCheckpointGCed, it.r[:6])
				}
				return fmt.Errorf("vfs: read source shard %x: %w", it.r[:6], err)
			}
			if blake3.Sum(sealed) != it.r {
				return fmt.Errorf("%w: shard %x does not hash to its name", ErrCorrupt, it.r[:6])
			}
			plain, err := unseal(c, aad, sealed)
			if err != nil {
				return fmt.Errorf("vfs: open shard %x: %w", it.r[:6], err)
			}
			n, err := decodeNode(plain)
			if err != nil {
				return fmt.Errorf("vfs: decode shard %x: %w", it.r[:6], err)
			}
			if it.expectedLevel >= 0 && n.level != it.expectedLevel {
				return fmt.Errorf("%w: source shard %x has level %d, want %d", ErrCorrupt, it.r[:6], n.level, it.expectedLevel)
			}

			out := shardRes{
				r:         it.r,
				key:       sk,
				level:     n.level,
				sealedLen: int64(len(sealed)),
			}
			if retainShards && stagedBytes.Add(int64(len(sealed))) <= maxParaSyncStagedShardBytes {
				out.sealed = sealed
			}
			if n.level > 0 {
				out.children = make([]ref, len(n.vals))
				for i, v := range n.vals {
					out.children[i] = ref(v)
				}
			} else {
				packs, err := extractLeafPacks(n)
				if err != nil {
					return err
				}
				out.packs = packs
			}
			results[idx] = out
			return nil
		}); err != nil {
			return err
		}

		var nextFrontier []srcItem
		for _, res := range results {
			plan.ShardsToCopy = append(plan.ShardsToCopy, res.key)
			plan.MetaBytes += res.sealedLen
			plan.FullMetaBytes += res.sealedLen
			ext.shardsToCopyRefs[res.key] = res.r
			if res.sealed != nil {
				ext.stagedShards[res.key] = res.sealed
			}
			for id, end := range res.packs {
				requiredPacks[id] = max(requiredPacks[id], end)
			}
			if res.level > 0 {
				childLevel := res.level - 1
				for _, ch := range res.children {
					if _, seen := visited[ch]; seen {
						continue
					}
					visited[ch] = struct{}{}
					pruned, err := pruneSubtree(ch, childLevel)
					if err != nil {
						return err
					}
					if !pruned {
						nextFrontier = append(nextFrontier, srcItem{r: ch, expectedLevel: childLevel})
					}
				}
			}
		}
		frontier = nextFrontier
	}
	return nil
}

func collectPrunedPacks(dstNodes map[ref]*dstNodeInfo, root ref, out map[packID]uint64) {
	stack := []ref{root}
	for len(stack) > 0 {
		r := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		info := dstNodes[r]
		if info == nil {
			continue
		}
		if info.level == 0 {
			for id, end := range info.packs {
				out[id] = max(out[id], end)
			}
			continue
		}
		stack = append(stack, info.children...)
	}
}

func extractLeafPacks(n *node) (map[packID]uint64, error) {
	var packs map[packID]uint64
	for i, k := range n.keys {
		if len(k) == 0 || k[0] != tChunk {
			continue
		}
		if len(k) != 1+len(chunkID{}) {
			return nil, fmt.Errorf("%w: chunk table key of %d bytes", ErrCorrupt, len(k))
		}
		w, err := decodeWhere(n.vals[i])
		if err != nil {
			return nil, err
		}
		if w.off+w.n < w.off {
			return nil, fmt.Errorf("%w: chunk extent overflow", ErrCorrupt)
		}
		if packs == nil {
			packs = map[packID]uint64{}
		}
		packs[w.pack] = max(packs[w.pack], w.off+w.n)
	}
	return packs, nil
}

type verifyOutcome struct {
	chunksVerified   int
	receiptsVerified int
}

type parasyncChunkEntry struct {
	id where
	ch chunkID
}

// verifyRestoredTree warms verifyVol's shard cache in parallel from dst, re-fetches
// any pruned shard or pack concurrently deleted by destination GC from src, runs
// checkTree(ctx, true) to enforce every canonical tree and metadata invariant, and
// cryptographically verifies every referenced chunk in every pack against
// unseal(c, chunkAAD(head.ID, id), frame) and sumChunk(plain) == id (or an opted-in
// Cipher-sealed ext/parasync/... integrity receipt) before updating destination head.
func verifyRestoredTree(
	ctx context.Context,
	srcCount *parasyncCountingBackend,
	dstTransferCount *parasyncCountingBackend,
	dstVerifyCount *parasyncCountingBackend,
	c Cipher,
	head Head,
	streams int,
	opts RestoreOptions,
	packsCopied *atomic.Int64,
	shardsCopied *atomic.Int64,
	bytesTransferred *atomic.Int64,
	gcRecoveredPacks *atomic.Int64,
	peakG *atomic.Int64,
) (verifyOutcome, error) {
	vCfg, err := Config{Backend: dstVerifyCount, Cipher: c, Streams: streams}.valid()
	if err != nil {
		return verifyOutcome{}, err
	}
	verifyVol := newVolume(vCfg)
	defer verifyVol.stop()
	verifyVol.adopt(head)

	if head.Root != none {
		visited := map[ref]struct{}{head.Root: {}}
		frontier := []ref{head.Root}
		expectedLevel := -1
		for len(frontier) > 0 {
			nodes := make([]*node, len(frontier))
			if err := runParallel(ctx, streams, len(frontier), peakG, func(wctx context.Context, idx int) error {
				r := frontier[idx]
				n, err := verifyVol.t.sh.load(wctx, r)
				if err != nil && errors.Is(err, backend.ErrNotFound) {
					// A pruned destination shard was concurrently removed by destination GC
					// between PlanDelta and verifyRestoredTree; recover it from source.
					sk := shardKey(r)
					sealed, _, serr := srcCount.Get(wctx, sk, maxShard)
					if serr != nil {
						if errors.Is(serr, backend.ErrNotFound) {
							return fmt.Errorf("%w: source shard %x is missing", ErrSourceCheckpointGCed, r[:6])
						}
						return fmt.Errorf("vfs: read source shard %x: %w", r[:6], serr)
					}
					if blake3.Sum(sealed) != r {
						return fmt.Errorf("%w: shard %x does not hash to its name", ErrCorrupt, r[:6])
					}
					if _, uerr := unseal(c, shardAAD(head.ID), sealed); uerr != nil {
						return fmt.Errorf("vfs: open source shard %x: %w", r[:6], uerr)
					}
					if _, perr := dstTransferCount.PutIfAbsent(wctx, sk, sealed); perr == nil {
						shardsCopied.Add(1)
						bytesTransferred.Add(int64(len(sealed)))
					} else if !errors.Is(perr, backend.ErrConflict) {
						return fmt.Errorf("vfs: recover destination shard %s: %w", sk, perr)
					}
					n, err = verifyVol.t.sh.load(wctx, r)
				}
				if err != nil {
					return err
				}
				if expectedLevel >= 0 && n.level != expectedLevel {
					return fmt.Errorf("%w: shard %x has level %d, want %d", ErrCorrupt, r[:6], n.level, expectedLevel)
				}
				nodes[idx] = n
				return nil
			}); err != nil {
				return verifyOutcome{}, fmt.Errorf("vfs: verify destination shards: %w", err)
			}
			var next []ref
			levelVal := -1
			for _, n := range nodes {
				if levelVal < 0 {
					levelVal = n.level
				} else if n.level != levelVal {
					return verifyOutcome{}, fmt.Errorf("%w: destination tree level mismatch (%d vs %d)", ErrCorrupt, n.level, levelVal)
				}
				if n.level > 0 {
					for _, v := range n.vals {
						ch := ref(v)
						if _, ok := visited[ch]; !ok {
							visited[ch] = struct{}{}
							next = append(next, ch)
						}
					}
				}
			}
			expectedLevel = levelVal - 1
			frontier = next
		}
	}
	if err := verifyVol.ns.load(head); err != nil {
		return verifyOutcome{}, fmt.Errorf("vfs: verify head metadata: %w", err)
	}

	// Collect all chunk table records [tChunk] and all file extent references [tData]
	// from the restored tree on destination.
	chunkTable := map[chunkID]where{}
	needPacks := map[packID]uint64{}
	if head.Root != none {
		var bad error
		if err := verifyVol.t.scan(ctx, verifyVol.root, []byte{tChunk}, func(k, val []byte) bool {
			if len(k) != 1+len(chunkID{}) {
				bad = fmt.Errorf("%w: chunk table key of %d bytes", ErrCorrupt, len(k))
				return false
			}
			w, err := decodeWhere(val)
			if err != nil {
				bad = err
				return false
			}
			if w.off+w.n < w.off || w.n == 0 {
				bad = fmt.Errorf("%w: invalid chunk extent off=%d n=%d", ErrCorrupt, w.off, w.n)
				return false
			}
			var cid chunkID
			copy(cid[:], k[1:])
			chunkTable[cid] = w
			needPacks[w.pack] = max(needPacks[w.pack], w.off+w.n)
			return true
		}); err != nil {
			return verifyOutcome{}, err
		}
		if bad != nil {
			return verifyOutcome{}, bad
		}

		if err := verifyVol.t.scan(ctx, verifyVol.root, []byte{tData}, func(k, val []byte) bool {
			if len(k) != prefixLen+8 {
				return true
			}
			e, err := decodeExtent(offsetOf(k), val)
			if err != nil {
				bad = err
				return false
			}
			if _, ok := chunkTable[e.id]; !ok {
				bad = fmt.Errorf("%w: chunk %x of a file is in no table", ErrCorrupt, e.id[:6])
				return false
			}
			return true
		}); err != nil {
			return verifyOutcome{}, err
		}
		if bad != nil {
			return verifyOutcome{}, bad
		}
	}

	// Recover any pruned destination pack that was concurrently deleted by destination GC
	// between PlanDelta and verifyRestoredTree BEFORE checkTree stats every pack.
	if len(needPacks) > 0 {
		packIDs := make([]packID, 0, len(needPacks))
		for pid := range needPacks {
			packIDs = append(packIDs, pid)
		}
		sort.Slice(packIDs, func(i, j int) bool { return packKey(packIDs[i]) < packKey(packIDs[j]) })

		if err := runParallel(ctx, streams, len(packIDs), peakG, func(wctx context.Context, idx int) error {
			pid := packIDs[idx]
			pk := packKey(pid)
			reqEnd := needPacks[pid]
			sz, err := dstVerifyCount.Stat(wctx, pk)
			if errors.Is(err, backend.ErrNotFound) {
				if rerr := recoverDestinationGCPack(wctx, srcCount, dstTransferCount, pk, pid, reqEnd, packsCopied, bytesTransferred, gcRecoveredPacks); rerr != nil {
					return rerr
				}
				sz, err = dstVerifyCount.Stat(wctx, pk)
			}
			if err != nil {
				if errors.Is(err, backend.ErrNotFound) {
					return fmt.Errorf("%w: pack %x is missing", ErrCorrupt, pid[:6])
				}
				return err
			}
			if uint64(sz) < reqEnd {
				return fmt.Errorf("%w: pack %x has %d bytes and a chunk ends at %d", ErrCorrupt, pid[:6], sz, reqEnd)
			}
			return nil
		}); err != nil {
			return verifyOutcome{}, err
		}
	}

	if err := verifyVol.checkTree(ctx, true); err != nil {
		return verifyOutcome{}, err
	}

	if len(chunkTable) == 0 {
		return verifyOutcome{}, nil
	}

	byPack := map[packID][]parasyncChunkEntry{}
	for cid, w := range chunkTable {
		byPack[w.pack] = append(byPack[w.pack], parasyncChunkEntry{id: w, ch: cid})
	}
	packList := make([]packID, 0, len(byPack))
	for pid, entries := range byPack {
		sort.Slice(entries, func(i, j int) bool { return entries[i].id.off < entries[j].id.off })
		byPack[pid] = entries
		packList = append(packList, pid)
	}
	sort.Slice(packList, func(i, j int) bool { return packKey(packList[i]) < packKey(packList[j]) })

	var (
		chunksVerified   atomic.Int64
		receiptsVerified atomic.Int64
	)

	if err := runParallel(ctx, streams, len(packList), peakG, func(wctx context.Context, idx int) error {
		pid := packList[idx]
		pk := packKey(pid)
		entries := byPack[pid]
		reqEnd := needPacks[pid]

		data, ver, err := dstVerifyCount.Get(wctx, pk, int64(packTarget+packRoom))
		if errors.Is(err, backend.ErrNotFound) {
			if rerr := recoverDestinationGCPack(wctx, srcCount, dstTransferCount, pk, pid, reqEnd, packsCopied, bytesTransferred, gcRecoveredPacks); rerr != nil {
				return rerr
			}
			data, ver, err = dstVerifyCount.Get(wctx, pk, int64(packTarget+packRoom))
		}
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				return fmt.Errorf("%w: pack %x is missing", ErrCorrupt, pid[:6])
			}
			return fmt.Errorf("vfs: read destination pack %x: %w", pid[:6], err)
		}
		if uint64(len(data)) < reqEnd {
			return fmt.Errorf("%w: pack %x has %d bytes and a chunk ends at %d", ErrCorrupt, pid[:6], len(data), reqEnd)
		}

		if opts.TrustSealedReceipt && ver != "" {
			if checkParaSyncPackReceipt(wctx, dstVerifyCount, c, head, pid, ver, uint64(len(data)), uint64(len(entries))) {
				receiptsVerified.Add(1)
				chunksVerified.Add(int64(len(entries)))
				return nil
			}
		}

		for _, ent := range entries {
			end := ent.id.off + ent.id.n
			if end < ent.id.off || end > uint64(len(data)) {
				return fmt.Errorf("%w: pack %x chunk %x extent [%d,%d) out of bounds %d", ErrCorrupt, pid[:6], ent.ch[:6], ent.id.off, end, len(data))
			}
			frame := data[ent.id.off:end]
			plain, uerr := unseal(c, chunkAAD(head.ID, ent.ch), frame)
			if uerr != nil {
				return fmt.Errorf("vfs: verify chunk %x in pack %x: %w", ent.ch[:6], pid[:6], uerr)
			}
			if sumChunk(plain) != ent.ch {
				return fmt.Errorf("%w: chunk %x in pack %x does not hash to its id", ErrCorrupt, ent.ch[:6], pid[:6])
			}
			chunksVerified.Add(1)
		}

		if opts.SaveVerifyReceipt && ver != "" {
			if err := saveParaSyncPackReceipt(wctx, dstTransferCount, c, head, pid, ver, uint64(len(data)), uint64(len(entries))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return verifyOutcome{}, err
	}

	return verifyOutcome{
		chunksVerified:   int(chunksVerified.Load()),
		receiptsVerified: int(receiptsVerified.Load()),
	}, nil
}

func recoverDestinationGCPack(
	ctx context.Context,
	srcCount *parasyncCountingBackend,
	dstTransferCount *parasyncCountingBackend,
	pk string,
	pid packID,
	reqEnd uint64,
	packsCopied *atomic.Int64,
	bytesTransferred *atomic.Int64,
	gcRecoveredPacks *atomic.Int64,
) error {
	data, _, err := srcCount.Get(ctx, pk, int64(packTarget+packRoom))
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return fmt.Errorf("%w: source pack %x is missing", ErrSourceCheckpointGCed, pid[:6])
		}
		return fmt.Errorf("vfs: read source pack %s: %w", pk, err)
	}
	if uint64(len(data)) < reqEnd {
		return fmt.Errorf("%w: source pack %x has %d bytes and a chunk ends at %d", ErrCorrupt, pid[:6], len(data), reqEnd)
	}
	_, err = dstTransferCount.PutIfAbsent(ctx, pk, data)
	switch {
	case err == nil:
		packsCopied.Add(1)
		bytesTransferred.Add(int64(len(data)))
		gcRecoveredPacks.Add(1)
		return nil
	case errors.Is(err, backend.ErrConflict):
		return nil
	default:
		return fmt.Errorf("vfs: recover destination pack %s: %w", pk, err)
	}
}

func encodeParaSyncReceipt(pid packID, ver backend.Version, packSize, chunkCount uint64) []byte {
	var b []byte
	b = append(b, paraSyncReceiptMagic...)
	b = append(b, pid[:]...)
	b = binary.AppendUvarint(b, packSize)
	b = binary.AppendUvarint(b, chunkCount)
	b = binary.AppendUvarint(b, uint64(len(ver)))
	b = append(b, ver...)
	return b
}

func checkParaSyncPackReceipt(
	ctx context.Context,
	dst backend.Backend,
	c Cipher,
	head Head,
	pid packID,
	ver backend.Version,
	packSize uint64,
	chunkCount uint64,
) bool {
	rest := extRest(head.Seed, paraSyncExtName, pid[:])
	objKey := extObjectKey(paraSyncExtName, rest)
	sealed, _, err := dst.Get(ctx, objKey, maxExtSealed)
	if err != nil {
		return false
	}
	plain, err := unseal(c, extAAD(head.ID, paraSyncExtName, rest), sealed)
	if err != nil {
		return false
	}
	return bytes.Equal(plain, encodeParaSyncReceipt(pid, ver, packSize, chunkCount))
}

func saveParaSyncPackReceipt(
	ctx context.Context,
	dst backend.Backend,
	c Cipher,
	head Head,
	pid packID,
	ver backend.Version,
	packSize uint64,
	chunkCount uint64,
) error {
	rest := extRest(head.Seed, paraSyncExtName, pid[:])
	objKey := extObjectKey(paraSyncExtName, rest)
	plain := encodeParaSyncReceipt(pid, ver, packSize, chunkCount)
	sealed, err := seal(c, extAAD(head.ID, paraSyncExtName, rest), plain)
	if err != nil {
		return err
	}
	for range extReplace {
		_, err = dst.PutIfAbsent(ctx, objKey, sealed)
		if err == nil || !errors.Is(err, backend.ErrConflict) {
			return err
		}
		_, curVer, gerr := dst.Get(ctx, objKey, maxExtSealed)
		if errors.Is(gerr, backend.ErrNotFound) {
			continue
		}
		if gerr != nil {
			return gerr
		}
		if _, err = dst.PutIfVersion(ctx, objKey, sealed, curVer); err == nil || !errors.Is(err, backend.ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("vfs: save parasync verify receipt: %w", err)
}

func parasyncHeadLanded(ctx context.Context, dstFence *fence.Store, place string, c Cipher, want Head, round uint64) bool {
	payload, recorded, err := dstFence.Get(ctx, headKey)
	if err != nil || recorded < round {
		return false
	}
	plain, err := unseal(c, headAAD(place), payload)
	if err != nil {
		return false
	}
	cur, err := unmarshalHead(plain)
	return err == nil &&
		cur.ID == want.ID &&
		cur.Root == want.Root &&
		cur.Seq == want.Seq &&
		cur.Journal == want.Journal &&
		cur.Next == want.Next &&
		cur.Needs == want.Needs &&
		cur.Tail == want.Tail
}

func runParallel(ctx context.Context, workers, n int, peakG *atomic.Int64, fn func(context.Context, int) error) error {
	if n <= 0 {
		recordPeakGoroutines(peakG)
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w := min(max(workers, 1), n)
	if w == 1 {
		recordPeakGoroutines(peakG)
		for i := range n {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(ctx, i); err != nil {
				return err
			}
		}
		return nil
	}

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		next     atomic.Int64
		errMu    sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	setErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	for range w {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recordPeakGoroutines(peakG)
			for {
				if wctx.Err() != nil {
					return
				}
				idx := int(next.Add(1) - 1)
				if idx >= n {
					return
				}
				if err := fn(wctx, idx); err != nil {
					setErr(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	recordPeakGoroutines(peakG)
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func recordPeakGoroutines(peak *atomic.Int64) {
	if peak == nil {
		return
	}
	cur := int64(runtime.NumGoroutine())
	for {
		prev := peak.Load()
		if cur <= prev || peak.CompareAndSwap(prev, cur) {
			return
		}
	}
}

func dedupRefs(in []ref) []ref {
	if len(in) <= 1 {
		return in
	}
	seen := make(map[ref]struct{}, len(in))
	out := in[:0]
	for _, r := range in {
		if _, ok := seen[r]; !ok {
			seen[r] = struct{}{}
			out = append(out, r)
		}
	}
	return out
}

func parseShardRef(key string) (ref, error) {
	const prefix = "meta/"
	if !strings.HasPrefix(key, prefix) {
		return none, fmt.Errorf("%w: invalid shard key %q", ErrCorrupt, key)
	}
	rest := key[len(prefix):]
	dir, hexRef, ok := strings.Cut(rest, "/")
	if !ok || len(dir) != 2 || len(hexRef) != 64 || hexRef[:2] != dir {
		return none, fmt.Errorf("%w: invalid shard key %q", ErrCorrupt, key)
	}
	b, err := hex.DecodeString(hexRef)
	if err != nil || len(b) != len(ref{}) {
		return none, fmt.Errorf("%w: invalid shard key %q", ErrCorrupt, key)
	}
	var r ref
	copy(r[:], b)
	return r, nil
}
