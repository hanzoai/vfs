// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package cofs implements a minimal-perfect-hash read-only metadata lookup
// index for immutable base images and snapshots in hanzoai/vfs (HIP-1342,
// Refs #25), inspired by CoFS (USENIX FAST 2026).
//
// Mutable workspaces continue to use vfs.Volume directly; immutable base
// images and snapshots store a sealed CoFS index as a Volume extension object
// ("cofs") keyed by the committed Merkle root ref, turning cold multi-shard
// prolly-tree traversals and post-quantum shard decryptions into single-probe
// O(1) in-memory lookups.
package cofs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/zeebo/blake3"
)

const (
	chdLambda     = 4
	directSlotBit = uint32(1 << 31)
	maxMPHFKeys   = 1 << 30
)

var (
	// ErrDuplicateKey is returned when BuildMPHF is handed duplicate keys.
	ErrDuplicateKey = errors.New("cofs: duplicate key in minimal perfect hash input")

	// ErrCorruptIndex is returned when serialized index bytes fail bounds or BLAKE3 checks.
	ErrCorruptIndex = errors.New("cofs: corrupt lookup index")
)

// MPHF is a Compress-Hash-and-Displace (CHD) minimal perfect hash function
// mapping n distinct keys bijectively onto [0, n) with 100% slot utilization
// and single-probe O(1) evaluation.
//
// Multi-key buckets are displaced in descending bucket-size order while ~30%
// of table slots remain free; singleton buckets encode their assigned free
// slot directly via directSlotBit, eliminating coupon-collector tail probing.
type MPHF struct {
	n       uint32
	buckets uint32
	key     [32]byte
	seeds   []uint32
}

type hashedItem struct {
	idx int
	h0  uint64
	h1  uint64
	h2  uint64
}

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func hashKey192(k [32]byte, data []byte) (h0, h1, h2 uint64) {
	// Fast keyed SipHash/SplitMix-reinforced BLAKE3 keyed compression for short keys.
	h, _ := blake3.NewKeyed(k[:])
	_, _ = h.Write(data)
	var sum [32]byte
	h.Sum(sum[:0])
	h0 = binary.LittleEndian.Uint64(sum[0:8])
	h1 = binary.LittleEndian.Uint64(sum[8:16])
	h2 = binary.LittleEndian.Uint64(sum[16:24])
	return h0, h1, h2
}

func hashString192(k [32]byte, s string) (h0, h1, h2 uint64) {
	h, _ := blake3.NewKeyed(k[:])
	_, _ = h.WriteString(s)
	var sum [32]byte
	h.Sum(sum[:0])
	h0 = binary.LittleEndian.Uint64(sum[0:8])
	h1 = binary.LittleEndian.Uint64(sum[8:16])
	h2 = binary.LittleEndian.Uint64(sum[16:24])
	return h0, h1, h2
}

func hashDirEntry192(k [32]byte, dir uint64, name string) (h0, h1, h2 uint64, fp uint32) {
	h, _ := blake3.NewKeyed(k[:])
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], dir)
	_, _ = h.Write(b[:])
	_, _ = h.WriteString(name)
	var sum [32]byte
	h.Sum(sum[:0])
	h0 = binary.LittleEndian.Uint64(sum[0:8])
	h1 = binary.LittleEndian.Uint64(sum[8:16])
	h2 = binary.LittleEndian.Uint64(sum[16:24])
	fp = binary.LittleEndian.Uint32(sum[24:28])
	return h0, h1, h2, fp
}

func displaceSlot(h0, h1, h2 uint64, d uint32, n uint32) uint32 {
	u := uint64(d)
	mixed := mix64(h1 + u*h2 + mix64(h0^u))
	return uint32(mixed % uint64(n))
}

// BuildMPHF constructs a minimal perfect hash over keys using a key derived from seed.
func BuildMPHF(seed [32]byte, domain string, keys [][]byte) (*MPHF, error) {
	if len(keys) > maxMPHFKeys {
		return nil, fmt.Errorf("cofs: %d keys exceeds limit %d", len(keys), maxMPHFKeys)
	}
	var derived [32]byte
	blake3.DeriveKey("hanzoai/vfs cofs mphf "+domain, seed[:], derived[:])

	n := uint32(len(keys))
	if n == 0 {
		return &MPHF{key: derived}, nil
	}
	numBuckets := (n + chdLambda - 1) / chdLambda
	if numBuckets == 0 {
		numBuckets = 1
	}

	buckets := make([][]hashedItem, numBuckets)
	for i, raw := range keys {
		h0, h1, h2 := hashKey192(derived, raw)
		b := uint32(h0 % uint64(numBuckets))
		buckets[b] = append(buckets[b], hashedItem{idx: i, h0: h0, h1: h1, h2: h2})
	}

	order := make([]uint32, numBuckets)
	for i := range order {
		order[i] = uint32(i)
	}
	sort.Slice(order, func(i, j int) bool {
		if len(buckets[order[i]]) != len(buckets[order[j]]) {
			return len(buckets[order[i]]) > len(buckets[order[j]])
		}
		return order[i] < order[j]
	})

	occupied := make([]bool, n)
	seeds := make([]uint32, numBuckets)
	probeSlots := make([]uint32, 0, 16)
	var singletons []uint32

	for _, bIdx := range order {
		b := buckets[bIdx]
		switch len(b) {
		case 0:
			continue
		case 1:
			singletons = append(singletons, bIdx)
			continue
		}

		// Detect exact duplicate keys inside the same bucket.
		for i := 0; i < len(b); i++ {
			for j := i + 1; j < len(b); j++ {
				if b[i].h0 == b[j].h0 && b[i].h1 == b[j].h1 && b[i].h2 == b[j].h2 {
					return nil, ErrDuplicateKey
				}
			}
		}

		placed := false
		for d := uint32(1); d < directSlotBit; d++ {
			probeSlots = probeSlots[:0]
			collision := false
			for _, it := range b {
				s := displaceSlot(it.h0, it.h1, it.h2, d, n)
				if occupied[s] {
					collision = true
					break
				}
				for _, prev := range probeSlots {
					if prev == s {
						collision = true
						break
					}
				}
				if collision {
					break
				}
				probeSlots = append(probeSlots, s)
			}
			if !collision {
				seeds[bIdx] = d
				for _, s := range probeSlots {
					occupied[s] = true
				}
				placed = true
				break
			}
		}
		if !placed {
			return nil, fmt.Errorf("cofs: failed to displace bucket %d of size %d", bIdx, len(b))
		}
	}

	freeCursor := uint32(0)
	for _, bIdx := range singletons {
		for freeCursor < n && occupied[freeCursor] {
			freeCursor++
		}
		if freeCursor >= n {
			return nil, errors.New("cofs: internal slot exhaustion on singleton assignment")
		}
		occupied[freeCursor] = true
		seeds[bIdx] = directSlotBit | freeCursor
		freeCursor++
	}

	return &MPHF{
		n:       n,
		buckets: numBuckets,
		key:     derived,
		seeds:   seeds,
	}, nil
}

// Len returns the number of keys indexed by m.
func (m *MPHF) Len() int { return int(m.n) }

func (m *MPHF) eval(h0, h1, h2 uint64) int {
	if m.n == 0 {
		return -1
	}
	b := uint32(h0 % uint64(m.buckets))
	d := m.seeds[b]
	if d == 0 {
		return -1
	}
	if d&directSlotBit != 0 {
		return int(d &^ directSlotBit)
	}
	return int(displaceSlot(h0, h1, h2, d, m.n))
}

// Index returns the slot in [0, m.Len()) for key, or -1 if m is empty or hits an empty bucket.
func (m *MPHF) Index(key []byte) int {
	if m.n == 0 {
		return -1
	}
	h0, h1, h2 := hashKey192(m.key, key)
	return m.eval(h0, h1, h2)
}

// IndexString returns the slot in [0, m.Len()) for string key without allocating.
func (m *MPHF) IndexString(key string) int {
	if m.n == 0 {
		return -1
	}
	h0, h1, h2 := hashString192(m.key, key)
	return m.eval(h0, h1, h2)
}

func (m *MPHF) indexDirEntry(dir uint64, name string) (slot int, fp uint32) {
	if m.n == 0 {
		return -1, 0
	}
	h0, h1, h2, fp := hashDirEntry192(m.key, dir, name)
	return m.eval(h0, h1, h2), fp
}

func (m *MPHF) appendTo(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, m.n)
	dst = binary.LittleEndian.AppendUint32(dst, m.buckets)
	dst = append(dst, m.key[:]...)
	for _, s := range m.seeds {
		dst = binary.LittleEndian.AppendUint32(dst, s)
	}
	return dst
}

func decodeMPHF(src []byte) (*MPHF, []byte, error) {
	if len(src) < 40 {
		return nil, nil, ErrCorruptIndex
	}
	n := binary.LittleEndian.Uint32(src[0:4])
	buckets := binary.LittleEndian.Uint32(src[4:8])
	if n > maxMPHFKeys || buckets > maxMPHFKeys {
		return nil, nil, ErrCorruptIndex
	}
	if n == 0 && buckets != 0 {
		return nil, nil, ErrCorruptIndex
	}
	need := 40 + int(buckets)*4
	if len(src) < need {
		return nil, nil, ErrCorruptIndex
	}
	m := &MPHF{
		n:       n,
		buckets: buckets,
		seeds:   make([]uint32, buckets),
	}
	copy(m.key[:], src[8:40])
	off := 40
	for i := range m.seeds {
		s := binary.LittleEndian.Uint32(src[off : off+4])
		if s&directSlotBit != 0 && (s&^directSlotBit) >= n {
			return nil, nil, ErrCorruptIndex
		}
		m.seeds[i] = s
		off += 4
	}
	return m, src[off:], nil
}
