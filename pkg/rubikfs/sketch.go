// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rubikfs

import (
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	// sketchSigLen is the number of one-permutation MinHash registers per chunk.
	sketchSigLen = 32

	// sketchBands partitions the 32 MinHash registers into 16 64-bit LSH bands
	// (2 registers per band), giving high recall for medium-similarity chunks
	// (e.g., 20-50% shingle overlap across distinct source files or binaries).
	sketchBands = 16

	// histBins is the number of coarse byte-distribution bins (top 4 bits of each byte).
	histBins = 16

	// incompressibleEntropyThreshold marks chunks whose Shannon entropy is too
	// high for LZ77 clustering to yield meaningful compression gains.
	incompressibleEntropyThreshold = 7.85
)

// ChunkSketch holds compact deterministic content-similarity features for one chunk.
type ChunkSketch struct {
	Sig          [sketchSigLen]uint32
	Bands        [sketchBands]uint64
	SimHash      uint64
	Hist         [histBins]uint16 // normalized to sum to 10000
	Entropy      float64
	EntropyClass uint8
	HighEntropy  bool
	Empty        bool
	Computed     bool
}

// ComputeSketch computes a deterministic, shift-invariant similarity sketch over data.
// It uses content-sampled 8-byte shingles (selected when the mixed shingle hash has
// low bits == 0) mapped into a 32-bin one-permutation MinHash with densification,
// alongside a 64-bit SimHash, 16-bin byte histogram, and Shannon entropy estimate.
func ComputeSketch(data []byte) ChunkSketch {
	var sk ChunkSketch
	sk.Computed = true
	n := len(data)
	if n == 0 {
		sk.Empty = true
		return sk
	}

	// 1. Exact byte counts for Shannon entropy and 16-bin coarse histogram.
	var byteCounts [256]uint32
	var histCounts [histBins]uint32
	for _, b := range data {
		byteCounts[b]++
		histCounts[b>>4]++
	}
	invN := 1.0 / float64(n)
	var entropy float64
	for _, c := range byteCounts {
		if c > 0 {
			p := float64(c) * invN
			entropy -= p * math.Log2(p)
		}
	}
	sk.Entropy = entropy
	switch {
	case entropy < 4.5:
		sk.EntropyClass = 0
	case entropy < 6.5:
		sk.EntropyClass = 1
	case entropy < incompressibleEntropyThreshold:
		sk.EntropyClass = 2
	default:
		sk.EntropyClass = 3
		sk.HighEntropy = true
	}
	for i := 0; i < histBins; i++ {
		sk.Hist[i] = uint16((uint64(histCounts[i]) * 10000) / uint64(n))
	}

	// 2. Initialize MinHash registers to sentinel max.
	for i := range sk.Sig {
		sk.Sig[i] = math.MaxUint32
	}

	// Content-defined shingle sampling mask:
	// - chunks >= 4 KiB: sample ~1/8 of 8-byte shingles (shift-invariant, fast)
	// - chunks >= 512 B: sample ~1/2 of 8-byte shingles
	// - smaller chunks: sample every 8-byte (or 4-byte) shingle
	var sampleMask uint64
	switch {
	case n >= 4096:
		sampleMask = 0x07
	case n >= 512:
		sampleMask = 0x01
	default:
		sampleMask = 0x00
	}

	var simAcc [64]int32
	sampled := 0

	if n >= 8 {
		limit := n - 8
		for i := 0; i <= limit; i++ {
			w := binary.LittleEndian.Uint64(data[i:])
			h := mix64(w)
			if (h & sampleMask) != 0 {
				continue
			}
			sampled++
			bin := h >> 59 // top 5 bits -> [0, 31]
			val := uint32(h >> 27)
			if val < sk.Sig[bin] {
				sk.Sig[bin] = val
			}
			// Update 64-bit SimHash accumulator (only on every 4th sampled shingle for speed).
			if (sampled & 3) == 1 {
				sh := mix64(h ^ 0xd6e8feb86659fd93)
				for b := 0; b < 64; b++ {
					if (sh>>uint(b))&1 == 1 {
						simAcc[b]++
					} else {
						simAcc[b]--
					}
				}
			}
		}
	}

	// Fallback if sampling produced too few shingles (e.g., constant block or < 8 bytes).
	if sampled == 0 {
		for i := 0; i < n; i++ {
			w := uint64(data[i])<<56 | uint64(i)*0x9e3779b185ebca87
			if i+4 <= n {
				w = uint64(binary.LittleEndian.Uint32(data[i:]))
			}
			h := mix64(w)
			bin := h >> 59
			val := uint32(h >> 27)
			if val < sk.Sig[bin] {
				sk.Sig[bin] = val
			}
		}
	}

	// 3. Densify any empty one-permutation MinHash bins deterministically.
	for i := 0; i < sketchSigLen; i++ {
		if sk.Sig[i] != math.MaxUint32 {
			continue
		}
		for step := 1; step < sketchSigLen; step++ {
			j := (i + step) & (sketchSigLen - 1)
			if sk.Sig[j] != math.MaxUint32 {
				sk.Sig[i] = uint32(mix64(uint64(sk.Sig[j]) ^ uint64(i)*0x9e3779b185ebca87))
				break
			}
		}
	}

	// 4. Finalize SimHash and 16 64-bit LSH band keys.
	var sim uint64
	for b := 0; b < 64; b++ {
		if simAcc[b] >= 0 {
			sim |= 1 << uint(b)
		}
	}
	sk.SimHash = sim

	for b := 0; b < sketchBands; b++ {
		sk.Bands[b] = (uint64(sk.Sig[2*b]) << 32) | uint64(sk.Sig[2*b+1])
	}

	return sk
}

// Similarity estimates content similarity in [0.0, 1.0] between two chunk sketches.
// It blends MinHash Jaccard similarity (dominant signal for shared LZ77 substrings)
// with SimHash Hamming similarity and coarse byte-histogram overlap.
func Similarity(a, b ChunkSketch) float64 {
	if a.Empty && b.Empty {
		return 1.0
	}
	if a.Empty || b.Empty {
		return 0.0
	}
	// Exact MinHash Jaccard estimate over 32 registers.
	var matches int
	for i := 0; i < sketchSigLen; i++ {
		if a.Sig[i] == b.Sig[i] {
			matches++
		}
	}
	jaccard := float64(matches) / float64(sketchSigLen)

	// SimHash Hamming similarity in [0, 1].
	hamming := bits.OnesCount64(a.SimHash ^ b.SimHash)
	simHashSim := 1.0 - float64(hamming)/64.0

	// L1 histogram intersection in [0, 1].
	var overlap int
	for i := 0; i < histBins; i++ {
		if a.Hist[i] < b.Hist[i] {
			overlap += int(a.Hist[i])
		} else {
			overlap += int(b.Hist[i])
		}
	}
	histSim := float64(overlap) / 10000.0
	if histSim > 1.0 {
		histSim = 1.0
	}

	// MinHash Jaccard is the primary predictor of cross-chunk LZ77 match density.
	return 0.75*jaccard + 0.15*simHashSim + 0.10*histSim
}

// CoarseBucket maps a sketch into a deterministic fallback bucket based on entropy
// class and dominant byte nibble so chunks without exact LSH collisions still
// land with structurally compatible neighbors (e.g., ASCII source vs ELF binaries).
func (sk ChunkSketch) CoarseBucket() uint32 {
	var top1, top2 uint32
	var max1, max2 uint16
	for i := 0; i < histBins; i++ {
		v := sk.Hist[i]
		if v >= max1 {
			max2, top2 = max1, top1
			max1, top1 = v, uint32(i)
		} else if v >= max2 {
			max2, top2 = v, uint32(i)
		}
	}
	return (uint32(sk.EntropyClass) << 8) | (top1 << 4) | top2
}

// mix64 is SplitMix64 finalizer: fast, bijective, deterministic 64-bit mixer.
func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
