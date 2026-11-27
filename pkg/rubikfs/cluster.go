// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rubikfs

import (
	"fmt"
	"math/rand"
	"sort"
)

// PlannedPack describes a single pack's ordered chunk indices and role prior to sealing.
type PlannedPack struct {
	Role         PackRole
	ChunkIndices []int
	RawBytes     int64
}

// PlanPacks partitions and orders chunks according to cfg.Policy.
// Every chunk MUST belong to cfg.Scope; mixed scopes return ErrTenantIsolation.
func PlanPacks(cfg Config, chunks []ChunkInput) ([]PlannedPack, float64, error) {
	if err := cfg.Validate(); err != nil {
		return nil, 0, err
	}
	for i := range chunks {
		if chunks[i].Scope != cfg.Scope {
			return nil, 0, fmt.Errorf("%w: chunk %d belongs to (%s,%s), want (%s,%s)",
				ErrTenantIsolation, i, chunks[i].Scope.TenantID, chunks[i].Scope.VolumeID,
				cfg.Scope.TenantID, cfg.Scope.VolumeID)
		}
	}
	if len(chunks) == 0 {
		return nil, 0, nil
	}

	var plans []PlannedPack
	switch cfg.Policy {
	case PolicyPathOrder:
		order := make([]int, len(chunks))
		for i := range chunks {
			order[i] = i
		}
		sort.Slice(order, func(i, j int) bool {
			a, b := chunks[order[i]], chunks[order[j]]
			if a.Path != b.Path {
				return a.Path < b.Path
			}
			return a.FileOff < b.FileOff
		})
		plans = sliceIntoPacks(order, chunks, cfg.TargetPackBytes, PackRoleGeneral)

	case PolicyRandomOrder:
		order := make([]int, len(chunks))
		for i := range chunks {
			order[i] = i
		}
		rng := rand.New(rand.NewSource(cfg.Seed))
		rng.Shuffle(len(order), func(i, j int) {
			order[i], order[j] = order[j], order[i]
		})
		plans = sliceIntoPacks(order, chunks, cfg.TargetPackBytes, PackRoleGeneral)

	case PolicyPureSimilarity:
		all := make([]int, len(chunks))
		for i := range chunks {
			all[i] = i
		}
		plans = clusterSimilarity(all, chunks, cfg.TargetPackBytes, PackRoleGeneral)

	case PolicyLocalityHybrid:
		var bootIdx, coldIdx []int
		for i := range chunks {
			if chunks[i].BootHot {
				bootIdx = append(bootIdx, i)
			} else {
				coldIdx = append(coldIdx, i)
			}
		}
		if len(bootIdx) > 0 {
			bootPlans := clusterSimilarity(bootIdx, chunks, cfg.BootPackBytes, PackRoleBoot)
			plans = append(plans, bootPlans...)
		}
		if len(coldIdx) > 0 {
			coldPlans := clusterSimilarity(coldIdx, chunks, cfg.TargetPackBytes, PackRoleCold)
			plans = append(plans, coldPlans...)
		}
	}

	meanAdj := computeMeanAdjacentSimilarity(plans, chunks)
	return plans, meanAdj, nil
}

// sliceIntoPacks splits an already-ordered list of chunk indices into bounded packs.
func sliceIntoPacks(order []int, chunks []ChunkInput, targetBytes int, role PackRole) []PlannedPack {
	if len(order) == 0 {
		return nil
	}
	var plans []PlannedPack
	var cur PlannedPack
	cur.Role = role

	for _, idx := range order {
		sz := int64(len(chunks[idx].Data))
		if len(cur.ChunkIndices) > 0 && cur.RawBytes+sz > int64(targetBytes) {
			plans = append(plans, cur)
			cur = PlannedPack{Role: role}
		}
		cur.ChunkIndices = append(cur.ChunkIndices, idx)
		cur.RawBytes += sz
	}
	if len(cur.ChunkIndices) > 0 {
		plans = append(plans, cur)
	}
	return plans
}

// clusterSimilarity performs LSH-accelerated greedy similarity chaining strictly
// over subset within a single volume, placing highly similar chunks adjacent to
// each other inside targetBytes-bounded packs.
func clusterSimilarity(subset []int, chunks []ChunkInput, targetBytes int, role PackRole) []PlannedPack {
	if len(subset) == 0 {
		return nil
	}
	if len(subset) == 1 {
		idx := subset[0]
		return []PlannedPack{{
			Role:         role,
			ChunkIndices: []int{idx},
			RawBytes:     int64(len(chunks[idx].Data)),
		}}
	}

	// Separate high-entropy incompressible chunks from compressible chunks so
	// random/encrypted blobs never disrupt compressible similarity chains.
	var compIdx, hiEntIdx []int
	for _, idx := range subset {
		if chunks[idx].Sketch.HighEntropy {
			hiEntIdx = append(hiEntIdx, idx)
		} else {
			compIdx = append(compIdx, idx)
		}
	}

	var plans []PlannedPack
	if len(compIdx) > 0 {
		plans = append(plans, clusterChain(compIdx, chunks, targetBytes, role)...)
	}
	if len(hiEntIdx) > 0 {
		// Deterministic path order for incompressible chunks; clustering yields no gain.
		sort.Slice(hiEntIdx, func(i, j int) bool {
			a, b := chunks[hiEntIdx[i]], chunks[hiEntIdx[j]]
			if a.Path != b.Path {
				return a.Path < b.Path
			}
			return a.FileOff < b.FileOff
		})
		plans = append(plans, sliceIntoPacks(hiEntIdx, chunks, targetBytes, role)...)
	}
	return plans
}

const (
	maxBucketEntries   = 64
	maxCandidateChecks = 96
	minChainSimilarity = 0.08
)

type bandKey struct {
	band uint8
	val  uint64
}

func clusterChain(indices []int, chunks []ChunkInput, targetBytes int, role PackRole) []PlannedPack {
	n := len(indices)
	// Map local position [0, n) -> global chunk index.
	localToGlobal := make([]int, n)
	copy(localToGlobal, indices)

	// Sort deterministically by (coarse bucket, path, offset) so seed iteration
	// starts from structurally coherent groups and is 100% reproducible.
	sort.Slice(localToGlobal, func(i, j int) bool {
		a, b := chunks[localToGlobal[i]], chunks[localToGlobal[j]]
		ba, bb := a.Sketch.CoarseBucket(), b.Sketch.CoarseBucket()
		if ba != bb {
			return ba < bb
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.FileOff < b.FileOff
	})

	// Build LSH inverted index: (band, 64-bit value) -> []localPos.
	lsh := make(map[bandKey][]int, n*4)
	// Also build a 1-register coarse index on the first 4 MinHash registers for
	// extra recall on low-to-medium similarity pairs (10-25% overlap).
	single := make(map[bandKey][]int, n*2)
	// Coarse bucket index for smooth fallback within same file/data class.
	coarse := make(map[uint32][]int, 64)

	for loc, g := range localToGlobal {
		sk := chunks[g].Sketch
		for b := 0; b < sketchBands; b++ {
			k := bandKey{band: uint8(b), val: sk.Bands[b]}
			if len(lsh[k]) < maxBucketEntries {
				lsh[k] = append(lsh[k], loc)
			}
		}
		for r := 0; r < 4; r++ {
			k := bandKey{band: uint8(r), val: uint64(sk.Sig[r])}
			if len(single[k]) < maxBucketEntries {
				single[k] = append(single[k], loc)
			}
		}
		cb := sk.CoarseBucket()
		coarse[cb] = append(coarse[cb], loc)
	}

	visited := make([]bool, n)
	seenStamp := make([]uint32, n)
	var stamp uint32
	coarseCursor := make(map[uint32]int, len(coarse))

	nextUnvisitedSeed := 0
	pickSeed := func() int {
		for nextUnvisitedSeed < n && visited[nextUnvisitedSeed] {
			nextUnvisitedSeed++
		}
		if nextUnvisitedSeed >= n {
			return -1
		}
		return nextUnvisitedSeed
	}

	var plans []PlannedPack
	var cur PlannedPack
	cur.Role = role

	findBestNeighbor := func(anchorLoc int, remainingBytes int64) int {
		anchor := chunks[localToGlobal[anchorLoc]].Sketch
		stamp++
		if stamp == 0 {
			clear(seenStamp)
			stamp = 1
		}

		bestLoc := -1
		bestScore := minChainSimilarity
		checked := 0

		consider := func(cand int) bool {
			if visited[cand] || seenStamp[cand] == stamp {
				return false
			}
			seenStamp[cand] = stamp
			g := localToGlobal[cand]
			if int64(len(chunks[g].Data)) > remainingBytes {
				return false
			}
			checked++
			score := Similarity(anchor, chunks[g].Sketch)
			if score > bestScore || (score == bestScore && bestLoc >= 0 && cand < bestLoc) {
				bestScore = score
				bestLoc = cand
			}
			return checked >= maxCandidateChecks
		}

		// 1. Primary 2-register LSH bands (high precision).
		for b := 0; b < sketchBands; b++ {
			list := lsh[bandKey{band: uint8(b), val: anchor.Bands[b]}]
			for _, cand := range list {
				if consider(cand) {
					return bestLoc
				}
			}
		}

		// 2. Secondary 1-register LSH probes (higher recall if no strong hit yet).
		if bestScore < 0.35 {
			for r := 0; r < 4; r++ {
				list := single[bandKey{band: uint8(r), val: uint64(anchor.Sig[r])}]
				for _, cand := range list {
					if consider(cand) {
						return bestLoc
					}
				}
			}
		}

		if bestLoc >= 0 {
			return bestLoc
		}

		// 3. Fallback: pick next unvisited chunk in the same coarse byte-distribution bucket.
		cb := anchor.CoarseBucket()
		list := coarse[cb]
		pos := coarseCursor[cb]
		for pos < len(list) {
			cand := list[pos]
			pos++
			if visited[cand] {
				continue
			}
			g := localToGlobal[cand]
			if int64(len(chunks[g].Data)) <= remainingBytes {
				coarseCursor[cb] = pos
				return cand
			}
		}
		coarseCursor[cb] = pos
		return -1
	}

	remaining := n
	for remaining > 0 {
		seed := pickSeed()
		if seed < 0 {
			break
		}
		cur = PlannedPack{Role: role}
		curLoc := seed

		for {
			visited[curLoc] = true
			remaining--
			g := localToGlobal[curLoc]
			cur.ChunkIndices = append(cur.ChunkIndices, g)
			cur.RawBytes += int64(len(chunks[g].Data))

			room := int64(targetBytes) - cur.RawBytes
			if room <= 0 || remaining == 0 {
				break
			}
			nextLoc := findBestNeighbor(curLoc, room)
			if nextLoc < 0 {
				// No neighbor fits from LSH or coarse bucket; fill remaining pack room
				// with the next available seed if it fits so we don't emit tiny packs.
				nextSeed := pickSeed()
				if nextSeed < 0 || int64(len(chunks[localToGlobal[nextSeed]].Data)) > room {
					break
				}
				nextLoc = nextSeed
			}
			curLoc = nextLoc
		}
		plans = append(plans, cur)
	}

	return plans
}

func computeMeanAdjacentSimilarity(plans []PlannedPack, chunks []ChunkInput) float64 {
	var total float64
	var pairs int
	for _, p := range plans {
		for i := 1; i < len(p.ChunkIndices); i++ {
			a := chunks[p.ChunkIndices[i-1]].Sketch
			b := chunks[p.ChunkIndices[i]].Sketch
			if !a.Computed || !b.Computed {
				return 0
			}
			total += Similarity(a, b)
			pairs++
		}
	}
	if pairs == 0 {
		return 0
	}
	return total / float64(pairs)
}

// computeMeanAdjacentSimilarityFull computes exact adjacent chunk similarity across
// all packs, lazily sketching chunks that were not sketched during build (e.g., for
// path_order_baseline and random_order_control diagnostic comparison).
func computeMeanAdjacentSimilarityFull(plans []PlannedPack, chunks []ChunkInput) float64 {
	sketches := make([]ChunkSketch, len(chunks))
	for i := range chunks {
		if chunks[i].Sketch.Computed {
			sketches[i] = chunks[i].Sketch
		} else {
			sketches[i] = ComputeSketch(chunks[i].Data)
		}
	}
	var total float64
	var pairs int
	for _, p := range plans {
		for i := 1; i < len(p.ChunkIndices); i++ {
			total += Similarity(sketches[p.ChunkIndices[i-1]], sketches[p.ChunkIndices[i]])
			pairs++
		}
	}
	if pairs == 0 {
		return 0
	}
	return total / float64(pairs)
}
