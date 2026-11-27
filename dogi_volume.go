// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/hanzoai/vfs/pkg/backend"
)

// PackLifetimeClassifier maps write-time observable file metadata to a target
// pack stream index in [0, 2] during generation settlement (HIP-1344, DOGI).
// When Config.PackClassifier is nil, the Volume uses standard single-stream
// FIFO packing. Both modes write identical format-v2 packs, shards, and Heads.
type PackLifetimeClassifier interface {
	ClassifyWrite(pathHint string, ino InodeID, size uint64, overwrites uint32) int
}

// PackLifetimeClassifierFunc adapts a function to PackLifetimeClassifier.
type PackLifetimeClassifierFunc func(pathHint string, ino InodeID, size uint64, overwrites uint32) int

// ClassifyWrite calls f(pathHint, ino, size, overwrites).
func (f PackLifetimeClassifierFunc) ClassifyWrite(pathHint string, ino InodeID, size uint64, overwrites uint32) int {
	return f(pathHint, ino, size, overwrites)
}

func (v *Volume) recordMakeHint(dir InodeID, name string, id InodeID) {
	v.hintMu.Lock()
	defer v.hintMu.Unlock()
	if v.pathHints == nil {
		v.pathHints = map[InodeID]string{RootInode: "/"}
	}
	base := v.pathHints[dir]
	if base == "" || base == "/" {
		v.pathHints[id] = "/" + name
	} else {
		v.pathHints[id] = base + "/" + name
	}
}

func (v *Volume) recordRenameHint(oldDir InodeID, oldName string, srcID InodeID, newDir InodeID, newName string, dstID InodeID, flags RenameFlags) {
	v.hintMu.Lock()
	defer v.hintMu.Unlock()
	if v.pathHints == nil {
		v.pathHints = map[InodeID]string{RootInode: "/"}
	}
	makePath := func(dir InodeID, name string) string {
		base := v.pathHints[dir]
		if base == "" || base == "/" {
			return "/" + name
		}
		return base + "/" + name
	}
	oldPath := v.pathHints[srcID]
	if oldPath == "" {
		oldPath = makePath(oldDir, oldName)
	}
	newPath := makePath(newDir, newName)
	v.pathHints[srcID] = newPath
	if flags&RenameExchange != 0 && dstID != 0 {
		v.pathHints[dstID] = oldPath
	}
}

func (v *Volume) recordOverwriteHint(f *fileState) {
	if f == nil {
		return
	}
	// Count distinct settle epochs or overwrites over existing settled content.
	if len(f.ext) > 0 || len(f.inline) > 0 || f.fz != nil || !f.changed {
		v.hintMu.Lock()
		if v.overwrites == nil {
			v.overwrites = map[InodeID]uint32{}
		}
		v.overwrites[f.ino]++
		v.hintMu.Unlock()
	}
}

// SetPathHint records or overrides the in-memory logical path hint and optional
// overwrite count for ino. Hints live strictly in memory and never alter the
// on-disk or on-store format-v2 wire layout.
func (v *Volume) SetPathHint(ino InodeID, p string, overwrites uint32) {
	v.hintMu.Lock()
	defer v.hintMu.Unlock()
	if v.pathHints == nil {
		v.pathHints = map[InodeID]string{RootInode: "/"}
	}
	if p != "" {
		v.pathHints[ino] = path.Clean("/" + strings.TrimSpace(p))
	}
	if overwrites > 0 {
		if v.overwrites == nil {
			v.overwrites = map[InodeID]uint32{}
		}
		v.overwrites[ino] = overwrites
	}
}

func (v *Volume) classifySnap(ctx context.Context, g *generation, s *snap) int {
	if v.cfg.PackClassifier == nil {
		return 0
	}
	v.hintMu.RLock()
	p := v.pathHints[s.ino]
	ow := v.overwrites[s.ino]
	v.hintMu.RUnlock()
	if p == "" {
		p = v.resolveInodePathHint(ctx, g, s.ino)
	}
	if ow == 0 && (len(s.ext) > 0 || len(s.inline) > 0) {
		ow = 2
	}
	cls := v.cfg.PackClassifier.ClassifyWrite(p, s.ino, s.size, ow)
	if cls < 0 || cls >= 3 {
		return 1
	}
	return cls
}

// resolveInodePathHint reconstructs an inode's path from the generation's
// directory entries if the Volume was reopened after Creation and lacked an
// in-memory hint for target.
func (v *Volume) resolveInodePathHint(ctx context.Context, g *generation, target InodeID) string {
	var base []entry
	_ = v.t.scan(ctx, g.root, []byte{tEntry}, func(k, val []byte) bool {
		base = append(base, entry{bytes.Clone(k), bytes.Clone(val)})
		return true
	})
	ents := laid(base, g.over, []byte{tEntry})
	type edge struct {
		parent InodeID
		name   string
	}
	parents := make(map[InodeID]edge, len(ents))
	for _, r := range ents {
		if len(r.key) <= prefixLen {
			continue
		}
		dir := inoOf(r.key)
		name := string(r.key[prefixLen:])
		child, _, err := decodeEntry(r.val)
		if err == nil {
			parents[child] = edge{parent: dir, name: name}
		}
	}
	var parts []string
	cur := target
	for range 64 {
		if cur == RootInode || cur == 0 {
			break
		}
		e, ok := parents[cur]
		if !ok {
			break
		}
		parts = append(parts, e.name)
		cur = e.parent
	}
	if len(parts) == 0 {
		return ""
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	resolved := "/" + strings.Join(parts, "/")
	v.hintMu.Lock()
	if v.pathHints == nil {
		v.pathHints = map[InodeID]string{RootInode: "/"}
	}
	v.pathHints[target] = resolved
	v.hintMu.Unlock()
	return resolved
}

// RootRef is a snapshot reference to a committed prolly-tree root shard
// (`meta/<2hex>/<blake3>`). Holding a RootRef and passing it to
// CompactFragmentedPacks pins every pack referenced by that snapshot root so
// concurrent or historical snapshot readers remain 100% live during relocation.
type RootRef = ref

// CommittedRoot returns the Volume's latest committed prolly-tree RootRef.
func (v *Volume) CommittedRoot() RootRef {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.root
}

// ClearCaches drops all cached decoded chunks and prolly-tree shards so
// subsequent reads must fetch and verify objects directly from the backend.
func (v *Volume) ClearCaches() {
	v.mu.RLock()
	chunks := v.chunks
	shCache := v.t.sh.cache
	v.mu.RUnlock()
	chunks.clear()
	shCache.clear()
}

func (v *Volume) acquireRoot(r ref) {
	if r == none {
		return
	}
	v.rootMu.Lock()
	if v.activeRoots == nil {
		v.activeRoots = map[ref]int{}
		v.rootCond = sync.NewCond(&v.rootMu)
	}
	v.activeRoots[r]++
	v.rootMu.Unlock()
}

func (v *Volume) releaseRoot(r ref) {
	if r == none {
		return
	}
	v.rootMu.Lock()
	if v.activeRoots != nil {
		v.activeRoots[r]--
		if v.activeRoots[r] <= 0 {
			delete(v.activeRoots, r)
			if v.rootCond != nil {
				v.rootCond.Broadcast()
			}
		}
	}
	v.rootMu.Unlock()
}

func (v *Volume) waitOldRootDrained(ctx context.Context, oldRoot ref) error {
	if oldRoot == none {
		return nil
	}
	stop := context.AfterFunc(ctx, func() {
		v.rootMu.Lock()
		if v.rootCond != nil {
			v.rootCond.Broadcast()
		}
		v.rootMu.Unlock()
	})
	defer stop()

	v.rootMu.Lock()
	defer v.rootMu.Unlock()
	for v.activeRoots != nil && v.activeRoots[oldRoot] > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		v.rootCond.Wait()
	}
	return nil
}

// PackUtilization describes the live vs physical byte utilization of one
// canonical format-v2 `pack/<2hex>/<128-bit-random-id>` object as seen from a
// committed prolly-tree root.
type PackUtilization struct {
	ID            [16]byte
	Key           string
	PhysicalBytes uint64
	LiveBytes     uint64
	DeadBytes     uint64
	TotalChunks   int
	LiveChunks    int
	DeadChunks    int
	LiveRatio     float64
	Pinned        bool
}

// PackUtilizationSummary aggregates pack utilization across all packs
// referenced by a Volume's committed prolly tree.
type PackUtilizationSummary struct {
	Packs              []PackUtilization
	TotalPacks         int
	TotalPhysicalBytes uint64
	TotalLiveBytes     uint64
	TotalDeadBytes     uint64
	FullyDeadPacks     int
	FragmentedPacks    int
}

// CompactionStats reports the deterministic work performed by
// CompactFragmentedPacks on a canonical vfs.Volume.
type CompactionStats struct {
	PacksInspected         int
	PacksCompacted         int
	PacksDeleted           int
	PacksPinnedSkipped     int
	NewPacksCreated        int
	ChunksRelocated        int
	DeadChunksPruned       int
	LiveBytesCopied        uint64
	PhysicalBytesWritten   uint64
	PhysicalBytesReclaimed uint64
}

type chunkLoc struct {
	id   chunkID
	w    where
	live bool
}

// MeasurePackUtilization inspects the canonical prolly tree of v (`[tData]`
// live extents vs `[tChunk]` `where{pack, off, n}` records) and `Backend.Stat`
// for each referenced pack, returning exact per-pack live/dead byte accounting.
func MeasurePackUtilization(ctx context.Context, v *Volume, pinnedRoots ...RootRef) (PackUtilizationSummary, error) {
	v.mu.RLock()
	root := v.root
	v.mu.RUnlock()
	return measureRootPackUtilization(ctx, v, root, pinnedRoots...)
}

func measureRootPackUtilization(ctx context.Context, v *Volume, root ref, pinnedRoots ...RootRef) (PackUtilizationSummary, error) {
	liveChunks, err := liveChunksAtRoot(ctx, v, root)
	if err != nil {
		return PackUtilizationSummary{}, err
	}
	pinnedPacks, err := collectPinnedPacks(ctx, v, pinnedRoots)
	if err != nil {
		return PackUtilizationSummary{}, err
	}

	type acc struct {
		id          packID
		maxEnd      uint64
		liveBytes   uint64
		totalChunks int
		liveChunks  int
	}
	byPack := map[packID]*acc{}
	var scanErr error
	err = v.t.scan(ctx, root, []byte{tChunk}, func(k, val []byte) bool {
		if len(k) != 1+len(chunkID{}) || k[0] != tChunk {
			return true
		}
		var cid chunkID
		copy(cid[:], k[1:])
		w, derr := decodeWhere(val)
		if derr != nil {
			scanErr = derr
			return false
		}
		a := byPack[w.pack]
		if a == nil {
			a = &acc{id: w.pack}
			byPack[w.pack] = a
		}
		a.totalChunks++
		a.maxEnd = max(a.maxEnd, w.off+w.n)
		if _, ok := liveChunks[cid]; ok {
			a.liveChunks++
			a.liveBytes += w.n
		}
		return true
	})
	if err != nil {
		return PackUtilizationSummary{}, err
	}
	if scanErr != nil {
		return PackUtilizationSummary{}, scanErr
	}

	ids := make([]packID, 0, len(byPack))
	for id := range byPack {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })

	var sum PackUtilizationSummary
	sum.Packs = make([]PackUtilization, 0, len(ids))
	for _, id := range ids {
		a := byPack[id]
		k := packKey(id)
		phys := a.maxEnd
		if sz, serr := v.be.Stat(ctx, k); serr == nil && sz > 0 {
			phys = max(phys, uint64(sz))
		} else if serr != nil && !errors.Is(serr, backend.ErrNotFound) {
			return PackUtilizationSummary{}, serr
		}
		live := min(a.liveBytes, phys)
		dead := phys - live
		ratio := 1.0
		if phys > 0 {
			ratio = float64(live) / float64(phys)
		}
		_, isPinned := pinnedPacks[id]
		pu := PackUtilization{
			ID:            id,
			Key:           k,
			PhysicalBytes: phys,
			LiveBytes:     live,
			DeadBytes:     dead,
			TotalChunks:   a.totalChunks,
			LiveChunks:    a.liveChunks,
			DeadChunks:    a.totalChunks - a.liveChunks,
			LiveRatio:     ratio,
			Pinned:        isPinned,
		}
		sum.Packs = append(sum.Packs, pu)
		sum.TotalPacks++
		sum.TotalPhysicalBytes += phys
		sum.TotalLiveBytes += live
		sum.TotalDeadBytes += dead
		if live == 0 && phys > 0 {
			sum.FullyDeadPacks++
		} else if dead > 0 {
			sum.FragmentedPacks++
		}
	}
	return sum, nil
}

// liveChunksAtRoot returns the set of all chunkIDs referenced by live `[tData]`
// extent records in the prolly tree rooted at root.
func liveChunksAtRoot(ctx context.Context, v *Volume, root ref) (map[chunkID]struct{}, error) {
	live := map[chunkID]struct{}{}
	var bad error
	err := v.t.scan(ctx, root, []byte{tData}, func(k, val []byte) bool {
		if len(k) != prefixLen+8 {
			return true // inline small file
		}
		e, derr := decodeExtent(offsetOf(k), val)
		if derr != nil {
			bad = derr
			return false
		}
		live[e.id] = struct{}{}
		return true
	})
	if err != nil {
		return nil, err
	}
	if bad != nil {
		return nil, bad
	}
	return live, nil
}

// collectPinnedPacks returns all packIDs referenced by `[tData]` extents (or
// `[tChunk]` entries) in any of pinnedRoots, plus any root currently in use by
// active in-memory readers on v.
func collectPinnedPacks(ctx context.Context, v *Volume, pinnedRoots []RootRef) (map[packID]struct{}, error) {
	pinned := map[packID]struct{}{}
	for _, r := range pinnedRoots {
		if r == none {
			continue
		}
		var bad error
		err := v.t.scan(ctx, r, []byte{tChunk}, func(k, val []byte) bool {
			if len(k) != 1+len(chunkID{}) || k[0] != tChunk {
				return true
			}
			w, derr := decodeWhere(val)
			if derr != nil {
				bad = derr
				return false
			}
			pinned[w.pack] = struct{}{}
			return true
		})
		if err != nil {
			return nil, err
		}
		if bad != nil {
			return nil, bad
		}
	}
	return pinned, nil
}

// CompactFragmentedPacks evaluates pack fragmentation on a canonical vfs.Volume
// and relocates surviving chunks from fragmented packs whose live byte ratio is
// at most maxLiveRatio (e.g. 0.60 means >= 40% dead bytes) into new canonical
// 128-bit random `pack/<2hex>/<id>` packs.
//
// It introduces no separate GC authority or custom metadata format:
//  1. It flushes any pending changes on v so v.root reflects all settled writes.
//  2. It scans `[tData]` live extents and `[tChunk]` `where` records in v.root.
//  3. Any pack referenced by `pinnedRoots` (such as an open read-only snapshot
//     Volume or pinned checkpoint) is strictly protected and never deleted.
//  4. For each eligible fragmented pack with dead bytes, surviving live chunks
//     are read, unsealed, and verified against their chunkID hash, packed into
//     new canonical 128-bit random `pack/<2hex>/<id>` objects, and uploaded to
//     the backend BEFORE any tree or Head update.
//  5. Updated `[tChunk]` `where` mutations (and deletion of dead `[tChunk]`
//     records from reclaimed packs) are applied to the canonical prolly tree
//     (`meta/<2hex>/<blake3>`) and committed via fenced `Head` CAS
//     (`v.writeHead` + `v.landed` lost-ACK recovery).
//  6. Only after the new Head is durably committed does it delete superseded
//     packs that are unreferenced by both the new `Head.Root` and `pinnedRoots`.
func CompactFragmentedPacks(ctx context.Context, v *Volume, maxLiveRatio float64, pinnedRoots ...RootRef) (CompactionStats, error) {
	if ctx == nil {
		return CompactionStats{}, errors.New("vfs: nil context")
	}
	if maxLiveRatio < 0 || maxLiveRatio > 1 {
		return CompactionStats{}, fmt.Errorf("vfs: invalid maxLiveRatio %v, want [0, 1]", maxLiveRatio)
	}
	if err := v.Flush(ctx); err != nil {
		return CompactionStats{}, err
	}

	// Hold wmu across relocation so no concurrent writer mutates v.root mid-compaction,
	// while concurrent File.ReadAt calls continue without taking wmu.
	v.wmu.Lock()
	defer v.wmu.Unlock()

	v.mu.RLock()
	if err := v.writable(); err != nil {
		v.mu.RUnlock()
		return CompactionStats{}, err
	}
	root := v.root
	v.mu.RUnlock()

	liveSet, err := liveChunksAtRoot(ctx, v, root)
	if err != nil {
		return CompactionStats{}, err
	}
	pinnedPacks, err := collectPinnedPacks(ctx, v, pinnedRoots)
	if err != nil {
		return CompactionStats{}, err
	}

	type packGroup struct {
		id        packID
		maxEnd    uint64
		liveBytes uint64
		chunks    []chunkLoc
	}
	groups := map[packID]*packGroup{}
	var scanErr error
	err = v.t.scan(ctx, root, []byte{tChunk}, func(k, val []byte) bool {
		if len(k) != 1+len(chunkID{}) || k[0] != tChunk {
			return true
		}
		var cid chunkID
		copy(cid[:], k[1:])
		w, derr := decodeWhere(val)
		if derr != nil {
			scanErr = derr
			return false
		}
		g := groups[w.pack]
		if g == nil {
			g = &packGroup{id: w.pack}
			groups[w.pack] = g
		}
		_, isLive := liveSet[cid]
		g.chunks = append(g.chunks, chunkLoc{id: cid, w: w, live: isLive})
		g.maxEnd = max(g.maxEnd, w.off+w.n)
		if isLive {
			g.liveBytes += w.n
		}
		return true
	})
	if err != nil {
		return CompactionStats{}, err
	}
	if scanErr != nil {
		return CompactionStats{}, scanErr
	}

	packIDs := make([]packID, 0, len(groups))
	for id := range groups {
		packIDs = append(packIDs, id)
	}
	sort.Slice(packIDs, func(i, j int) bool { return bytes.Compare(packIDs[i][:], packIDs[j][:]) < 0 })

	var stats CompactionStats
	stats.PacksInspected = len(packIDs)

	var candidates []*packGroup
	physSizes := make(map[packID]uint64, len(packIDs))
	for _, id := range packIDs {
		g := groups[id]
		phys := g.maxEnd
		if sz, serr := v.be.Stat(ctx, packKey(id)); serr == nil && sz > 0 {
			phys = max(phys, uint64(sz))
		} else if serr != nil && !errors.Is(serr, backend.ErrNotFound) {
			return stats, serr
		}
		physSizes[id] = phys
		live := min(g.liveBytes, phys)
		dead := phys - live
		if dead == 0 {
			continue
		}
		ratio := float64(live) / float64(phys)
		if ratio > maxLiveRatio+1e-12 {
			continue
		}
		if _, pinned := pinnedPacks[id]; pinned {
			stats.PacksPinnedSkipped++
			continue
		}
		sort.Slice(g.chunks, func(i, j int) bool { return g.chunks[i].w.off < g.chunks[j].w.off })
		candidates = append(candidates, g)
	}

	if len(candidates) == 0 {
		return stats, nil
	}

	target := packTarget
	if v.cfg.PackTargetBytes > 0 && v.cfg.PackTargetBytes < packTarget {
		target = v.cfg.PackTargetBytes
	}

	type relocatedChunk struct {
		id    chunkID
		frame []byte
	}
	var survivors []relocatedChunk
	var muts []mut

	for _, g := range candidates {
		srcKey := packKey(g.id)
		for _, ch := range g.chunks {
			if !ch.live {
				muts = append(muts, mut{key: keyChunk(ch.id), del: true})
				stats.DeadChunksPruned++
				continue
			}
			frame, rerr := v.be.GetRange(ctx, srcKey, int64(ch.w.off), int64(ch.w.n))
			if rerr != nil {
				return stats, fmt.Errorf("vfs: compact read chunk %x from %s: %w", ch.id[:6], srcKey, rerr)
			}
			plain, uerr := unseal(v.c, chunkAAD(v.id, ch.id), frame)
			if uerr != nil {
				return stats, fmt.Errorf("vfs: compact open chunk %x: %w", ch.id[:6], uerr)
			}
			if sumChunk(plain) != ch.id {
				return stats, fmt.Errorf("%w: chunk %x does not hash to its id", ErrCorrupt, ch.id[:6])
			}
			survivors = append(survivors, relocatedChunk{id: ch.id, frame: frame})
			stats.ChunksRelocated++
			stats.LiveBytesCopied += uint64(len(frame))
		}
	}

	// Build and upload new canonical 128-bit random packs FIRST before committing the new tree/Head.
	var curID packID
	var curBuf []byte
	flushRelocatedPack := func() error {
		if len(curBuf) == 0 {
			return nil
		}
		if err := v.be.Put(ctx, packKey(curID), curBuf); err != nil {
			return fmt.Errorf("vfs: write relocated pack %s: %w", hex.EncodeToString(curID[:6]), err)
		}
		stats.NewPacksCreated++
		stats.PhysicalBytesWritten += uint64(len(curBuf))
		curBuf = nil
		curID = packID{}
		return nil
	}

	for _, sc := range survivors {
		if len(curBuf) == 0 {
			if _, err := rand.Read(curID[:]); err != nil {
				return stats, err
			}
		}
		w := where{pack: curID, off: uint64(len(curBuf)), n: uint64(len(sc.frame))}
		curBuf = append(curBuf, sc.frame...)
		muts = append(muts, mut{key: keyChunk(sc.id), val: encodeWhere(w)})
		if len(curBuf) >= target {
			if err := flushRelocatedPack(); err != nil {
				return stats, err
			}
		}
	}
	if err := flushRelocatedPack(); err != nil {
		return stats, err
	}

	sort.SliceStable(muts, func(i, j int) bool { return bytes.Compare(muts[i].key, muts[j].key) < 0 })
	deduped := muts[:0]
	for _, m := range muts {
		if n := len(deduped); n > 0 && bytes.Equal(deduped[n-1].key, m.key) {
			deduped[n-1] = m
		} else {
			deduped = append(deduped, m)
		}
	}
	muts = deduped

	shards := newUploader(ctx, v.be, v.cfg.Streams)
	newRoot, err := v.t.using(shards).apply(ctx, root, muts)
	if werr := shards.wait(); err == nil && werr != nil {
		err = werr
	}
	if err != nil {
		return stats, err
	}

	// Commit updated Head via canonical fenced write + lost-ACK landed check.
	v.mu.Lock()
	if err := v.writable(); err != nil {
		v.mu.Unlock()
		return stats, err
	}
	v.seq++
	h := v.head
	h.Seq, h.Root = v.seq, newRoot
	v.ns.stamp(&h)
	v.mu.Unlock()

	if err := v.writeHead(ctx, h); err != nil {
		if errors.Is(err, ErrStaleRound) {
			v.mu.Lock()
			v.fenced = true
			v.mu.Unlock()
		}
		cur, ok := v.landed(ctx, h)
		if !ok {
			return stats, fmt.Errorf("vfs: commit relocated packs: %w", err)
		}
		h = cur
	}

	v.mu.Lock()
	v.head, v.root = h, h.Root
	v.mu.Unlock()

	// Wait for any in-flight File.ReadAt calls that planned against the previous
	// root before deleting retired candidate packs.
	if root != newRoot {
		if err := v.waitOldRootDrained(ctx, root); err != nil {
			return stats, err
		}
	}

	// Verify which packs remain referenced in newRoot before deleting retired candidate packs.
	stillRef, err := collectPinnedPacks(ctx, v, []RootRef{newRoot})
	if err != nil {
		return stats, err
	}

	stats.PacksCompacted = len(candidates)
	for _, g := range candidates {
		if _, inNew := stillRef[g.id]; inNew {
			continue
		}
		if _, inPin := pinnedPacks[g.id]; inPin {
			continue
		}
		if err := v.be.Delete(ctx, packKey(g.id)); err != nil && !errors.Is(err, backend.ErrNotFound) {
			return stats, fmt.Errorf("vfs: delete reclaimed pack %s: %w", packKey(g.id), err)
		}
		stats.PacksDeleted++
		stats.PhysicalBytesReclaimed += physSizes[g.id]
	}

	return stats, nil
}
