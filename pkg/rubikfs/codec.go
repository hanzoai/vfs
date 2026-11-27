// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rubikfs

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const (
	codecModeRaw         byte = 0x00
	codecModeFlate       byte = 0x01
	codecModeLongLZFlate byte = 0x02

	longLZHashBits = 17
	longLZHashSize = 1 << longLZHashBits
	longLZMinMatch = 16
)

type longLZTable [longLZHashSize][2]uint32

var longLZPool = sync.Pool{
	New: func() any {
		return new(longLZTable)
	},
}

// Compress encodes src using the requested CodecKind.
// For CodecFlate and CodecLongLZFlate, incompressible payloads automatically fall
// back to raw storage (1-byte mode prefix) so high-entropy packs never expand.
func Compress(codec CodecKind, src []byte) ([]byte, error) {
	switch codec {
	case CodecNone:
		out := make([]byte, len(src))
		copy(out, src)
		return out, nil

	case CodecFlate:
		comp, err := flateCompress(src)
		if err != nil {
			return nil, err
		}
		if len(comp)+1 >= len(src) {
			out := make([]byte, 1+len(src))
			out[0] = codecModeRaw
			copy(out[1:], src)
			return out, nil
		}
		out := make([]byte, 1+len(comp))
		out[0] = codecModeFlate
		copy(out[1:], comp)
		return out, nil

	case CodecLongLZFlate:
		return compressLongLZFlate(src)

	default:
		return nil, fmt.Errorf("%w: unsupported codec %d", ErrInvalidConfig, codec)
	}
}

// Decompress reverses Compress and verifies that the output length matches expectedLen.
func Decompress(codec CodecKind, expectedLen int, data []byte) ([]byte, error) {
	if expectedLen < 0 || expectedLen > MaxPackPlaintextBytes {
		return nil, fmt.Errorf("%w: invalid decompressed length %d", ErrCorruptPack, expectedLen)
	}
	switch codec {
	case CodecNone:
		if len(data) != expectedLen {
			return nil, fmt.Errorf("%w: uncompressed length %d != expected %d", ErrCorruptPack, len(data), expectedLen)
		}
		out := make([]byte, expectedLen)
		copy(out, data)
		return out, nil

	case CodecFlate:
		if len(data) < 1 {
			return nil, fmt.Errorf("%w: empty flate payload", ErrCorruptPack)
		}
		switch data[0] {
		case codecModeRaw:
			if len(data)-1 != expectedLen {
				return nil, fmt.Errorf("%w: raw fallback length %d != expected %d", ErrCorruptPack, len(data)-1, expectedLen)
			}
			out := make([]byte, expectedLen)
			copy(out, data[1:])
			return out, nil
		case codecModeFlate:
			out, err := flateDecompress(data[1:], expectedLen)
			if err != nil {
				return nil, err
			}
			if len(out) != expectedLen {
				return nil, fmt.Errorf("%w: flate length %d != expected %d", ErrCorruptPack, len(out), expectedLen)
			}
			return out, nil
		default:
			return nil, fmt.Errorf("%w: unknown flate mode %d", ErrCorruptPack, data[0])
		}

	case CodecLongLZFlate:
		return decompressLongLZFlate(data, expectedLen)

	default:
		return nil, fmt.Errorf("%w: unsupported codec %d", ErrCorruptPack, codec)
	}
}

func flateCompress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(src); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func flateDecompress(src []byte, maxBytes int) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	lim := io.LimitReader(r, int64(maxBytes)+1)
	out, err := io.ReadAll(lim)
	if err != nil {
		return nil, fmt.Errorf("%w: flate inflate: %v", ErrCorruptPack, err)
	}
	if len(out) > maxBytes {
		return nil, fmt.Errorf("%w: flate output exceeds bound %d", ErrCorruptPack, maxBytes)
	}
	return out, nil
}

// compressLongLZFlate performs a single-pass pack-wide LZ77 extraction (capturing
// cross-chunk matches across the entire multi-MiB pack, beyond DEFLATE's 32 KiB
// window), then entropy-codes the command and literal streams with DEFLATE.
func compressLongLZFlate(src []byte) ([]byte, error) {
	n := len(src)
	if n == 0 {
		return []byte{codecModeRaw}, nil
	}

	ht := longLZPool.Get().(*longLZTable)
	clear(ht[:])
	defer longLZPool.Put(ht)

	cmdBuf := make([]byte, 0, n/16+64)
	litBuf := make([]byte, 0, n/2+64)

	litStart := 0
	pos := 0
	limit := n - longLZMinMatch
	consecutiveMisses := 0
	var varTmp [binary.MaxVarintLen64]byte

	for pos <= limit {
		w := binary.LittleEndian.Uint64(src[pos:])
		h := (w * 0x9e3779b185ebca87) >> (64 - longLZHashBits)
		slot := &ht[h]
		c0, c1 := slot[0], slot[1]

		bestMatchPos := pos
		bestCandPos := 0
		bestLen := 0

		for _, entry := range [2]uint32{c0, c1} {
			if entry == 0 {
				continue
			}
			cand := int(entry - 1)
			if cand >= pos {
				continue
			}
			if binary.LittleEndian.Uint64(src[cand:]) != w {
				continue
			}
			// Extend forward in 8-byte strides, then bytewise.
			ml := 8
			for pos+ml+8 <= n && binary.LittleEndian.Uint64(src[cand+ml:]) == binary.LittleEndian.Uint64(src[pos+ml:]) {
				ml += 8
			}
			for pos+ml < n && src[cand+ml] == src[pos+ml] {
				ml++
			}
			// Extend backward into pending literals when possible.
			mp := pos
			cp := cand
			for mp > litStart && cp > 0 && src[cp-1] == src[mp-1] {
				mp--
				cp--
				ml++
			}
			if ml > bestLen {
				bestLen = ml
				bestMatchPos = mp
				bestCandPos = cp
			}
		}

		// Record current position in 2-way hash table.
		slot[1] = c0
		slot[0] = uint32(pos + 1)

		if bestLen >= longLZMinMatch {
			consecutiveMisses = 0
			litLen := bestMatchPos - litStart
			dist := bestMatchPos - bestCandPos
			extraLen := bestLen - longLZMinMatch

			litBuf = append(litBuf, src[litStart:bestMatchPos]...)
			k := binary.PutUvarint(varTmp[:], uint64(litLen))
			cmdBuf = append(cmdBuf, varTmp[:k]...)
			k = binary.PutUvarint(varTmp[:], uint64(dist))
			cmdBuf = append(cmdBuf, varTmp[:k]...)
			k = binary.PutUvarint(varTmp[:], uint64(extraLen))
			cmdBuf = append(cmdBuf, varTmp[:k]...)

			endPos := bestMatchPos + bestLen
			// Index sampled positions inside matched region so subsequent chunks can chain onto it.
			for p := bestMatchPos + 16; p < endPos && p+8 <= n; p += 16 {
				pw := binary.LittleEndian.Uint64(src[p:])
				ph := (pw * 0x9e3779b185ebca87) >> (64 - longLZHashBits)
				ht[ph][1] = ht[ph][0]
				ht[ph][0] = uint32(p + 1)
			}

			pos = endPos
			litStart = endPos
		} else {
			consecutiveMisses++
			step := 1
			if consecutiveMisses > 512 {
				step = 1 + (consecutiveMisses >> 9)
				if step > 4 {
					step = 4
				}
			}
			pos += step
		}
	}

	// If no long matches were found, fall back directly to single-pass DEFLATE or raw.
	if len(cmdBuf) == 0 {
		comp, err := flateCompress(src)
		if err != nil {
			return nil, err
		}
		if len(comp)+1 >= n {
			out := make([]byte, 1+n)
			out[0] = codecModeRaw
			copy(out[1:], src)
			return out, nil
		}
		out := make([]byte, 1+len(comp))
		out[0] = codecModeFlate
		copy(out[1:], comp)
		return out, nil
	}

	if litStart < n {
		litBuf = append(litBuf, src[litStart:]...)
	}

	cmdComp, err := flateCompress(cmdBuf)
	if err != nil {
		return nil, err
	}
	litComp, err := flateCompress(litBuf)
	if err != nil {
		return nil, err
	}

	var hdr [1 + 2*binary.MaxVarintLen64]byte
	hdr[0] = codecModeLongLZFlate
	off := 1
	off += binary.PutUvarint(hdr[off:], uint64(n))
	off += binary.PutUvarint(hdr[off:], uint64(len(cmdComp)))

	totalLen := off + len(cmdComp) + len(litComp)
	if totalLen >= 1+n {
		out := make([]byte, 1+n)
		out[0] = codecModeRaw
		copy(out[1:], src)
		return out, nil
	}

	out := make([]byte, totalLen)
	copy(out, hdr[:off])
	copy(out[off:], cmdComp)
	copy(out[off+len(cmdComp):], litComp)
	return out, nil
}

func decompressLongLZFlate(data []byte, expectedLen int) ([]byte, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("%w: empty longlz payload", ErrCorruptPack)
	}
	switch data[0] {
	case codecModeRaw:
		if len(data)-1 != expectedLen {
			return nil, fmt.Errorf("%w: raw length %d != expected %d", ErrCorruptPack, len(data)-1, expectedLen)
		}
		out := make([]byte, expectedLen)
		copy(out, data[1:])
		return out, nil

	case codecModeFlate:
		out, err := flateDecompress(data[1:], expectedLen)
		if err != nil {
			return nil, err
		}
		if len(out) != expectedLen {
			return nil, fmt.Errorf("%w: flate length %d != expected %d", ErrCorruptPack, len(out), expectedLen)
		}
		return out, nil

	case codecModeLongLZFlate:
		pos := 1
		rawLen, n1 := binary.Uvarint(data[pos:])
		if n1 <= 0 || rawLen != uint64(expectedLen) {
			return nil, fmt.Errorf("%w: invalid longlz rawLen %d (want %d)", ErrCorruptPack, rawLen, expectedLen)
		}
		pos += n1

		cmdCompLen, n2 := binary.Uvarint(data[pos:])
		if n2 <= 0 || cmdCompLen > uint64(len(data)-pos-n2) {
			return nil, fmt.Errorf("%w: invalid longlz cmd stream length", ErrCorruptPack)
		}
		pos += n2

		cmdComp := data[pos : pos+int(cmdCompLen)]
		litComp := data[pos+int(cmdCompLen):]

		// Bound inflated command stream to avoid zip-bomb allocations.
		cmdMax := expectedLen + 64<<10
		if cmdMax > MaxPackPlaintextBytes {
			cmdMax = MaxPackPlaintextBytes
		}
		cmdStream, err := flateDecompress(cmdComp, cmdMax)
		if err != nil {
			return nil, err
		}
		litStream, err := flateDecompress(litComp, expectedLen)
		if err != nil {
			return nil, err
		}

		dst := make([]byte, 0, expectedLen)
		cpos := 0
		lpos := 0

		for cpos < len(cmdStream) {
			litLen, k1 := binary.Uvarint(cmdStream[cpos:])
			if k1 <= 0 {
				return nil, fmt.Errorf("%w: truncated longlz litLen", ErrCorruptPack)
			}
			cpos += k1

			dist, k2 := binary.Uvarint(cmdStream[cpos:])
			if k2 <= 0 {
				return nil, fmt.Errorf("%w: truncated longlz dist", ErrCorruptPack)
			}
			cpos += k2

			extraLen, k3 := binary.Uvarint(cmdStream[cpos:])
			if k3 <= 0 {
				return nil, fmt.Errorf("%w: truncated longlz extraLen", ErrCorruptPack)
			}
			cpos += k3

			if litLen > uint64(len(litStream)-lpos) {
				return nil, fmt.Errorf("%w: longlz literal overrun", ErrCorruptPack)
			}
			remOut := uint64(expectedLen - len(dst))
			if litLen > remOut {
				return nil, fmt.Errorf("%w: longlz output literal overrun", ErrCorruptPack)
			}
			dst = append(dst, litStream[lpos:lpos+int(litLen)]...)
			lpos += int(litLen)

			remOut = uint64(expectedLen - len(dst))
			if extraLen > remOut || longLZMinMatch > remOut-extraLen {
				return nil, fmt.Errorf("%w: longlz match overrun", ErrCorruptPack)
			}
			matchLen := extraLen + longLZMinMatch
			if dist == 0 || dist > uint64(len(dst)) {
				return nil, fmt.Errorf("%w: longlz invalid match distance %d at offset %d", ErrCorruptPack, dist, len(dst))
			}

			srcOff := len(dst) - int(dist)
			dstOff := len(dst)
			dst = dst[:dstOff+int(matchLen)]
			if int(dist) >= int(matchLen) {
				copy(dst[dstOff:], dst[srcOff:srcOff+int(matchLen)])
			} else {
				// Overlapping RLE-style copy.
				copied := copy(dst[dstOff:], dst[srcOff:dstOff])
				for copied < int(matchLen) {
					nCopy := copy(dst[dstOff+copied:], dst[dstOff:dstOff+copied])
					copied += nCopy
				}
			}
		}

		// Append trailing literals.
		rem := len(litStream) - lpos
		if len(dst)+rem != expectedLen {
			return nil, fmt.Errorf("%w: longlz final length %d != expected %d", ErrCorruptPack, len(dst)+rem, expectedLen)
		}
		dst = append(dst, litStream[lpos:]...)
		return dst, nil

	default:
		return nil, fmt.Errorf("%w: unknown longlz mode %d", ErrCorruptPack, data[0])
	}
}
