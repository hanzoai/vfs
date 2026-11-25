// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package djfs

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/fs"
	"time"
)

var batchMagic = [4]byte{'D', 'J', 'F', 'S'}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const batchHeaderLen = 28

const maxBatchOps = 1 << 22

// EncodeBatch serializes a coalesced Batch into a deterministic, CRC-32C
// authenticated binary record suitable for journal.Journal.Append.
//
// Wire layout:
//
//	magic     [4]byte   "DJFS"
//	crc32c    uint32    Castagnoli checksum over bytes [8:]
//	epoch     uint64    little-endian epoch identifier
//	maxSeq    uint64    little-endian highest global mutation sequence covered
//	opCount   uint32    little-endian count of encoded DeltaOps
//	ops       ...       back-to-back canonical varint-framed DeltaOp records
func EncodeBatch(b Batch) []byte {
	est := batchHeaderLen + len(b.Ops)*48
	buf := make([]byte, batchHeaderLen, est)
	copy(buf[0:4], batchMagic[:])
	binary.LittleEndian.PutUint64(buf[8:16], b.Epoch)
	binary.LittleEndian.PutUint64(buf[16:24], b.MaxSeq)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(len(b.Ops)))

	for i := range b.Ops {
		op := &b.Ops[i]
		buf = append(buf, byte(op.Kind))
		buf = binary.AppendUvarint(buf, uint64(op.Dir))
		buf = appendBytes(buf, []byte(op.Name))
		buf = binary.AppendUvarint(buf, uint64(op.DstDir))
		buf = appendBytes(buf, []byte(op.DstName))
		buf = binary.AppendUvarint(buf, uint64(op.ID))
		buf = binary.AppendUvarint(buf, uint64(op.Attr.Mode))
		buf = binary.AppendUvarint(buf, op.Attr.Size)
		buf = binary.AppendUvarint(buf, uint64(op.Attr.Nlink))
		buf = binary.AppendVarint(buf, op.Attr.Mtime.UnixNano())
		buf = binary.AppendVarint(buf, op.Attr.Atime.UnixNano())
		buf = binary.AppendVarint(buf, op.Attr.Ctime.UnixNano())
		buf = appendBytes(buf, op.Data)
	}

	sum := crc32.Checksum(buf[8:], crcTable)
	binary.LittleEndian.PutUint32(buf[4:8], sum)
	return buf
}

// DecodeBatch parses and validates a binary Batch produced by EncodeBatch.
// Any header mismatch, checksum failure, non-canonical varint, or truncated
// slice fails closed with ErrCorrupt.
func DecodeBatch(raw []byte) (Batch, error) {
	if len(raw) < batchHeaderLen {
		return Batch{}, fmt.Errorf("%w: frame of %d bytes is shorter than header", ErrCorrupt, len(raw))
	}
	if raw[0] != batchMagic[0] || raw[1] != batchMagic[1] || raw[2] != batchMagic[2] || raw[3] != batchMagic[3] {
		return Batch{}, fmt.Errorf("%w: invalid batch magic", ErrCorrupt)
	}
	wantCRC := binary.LittleEndian.Uint32(raw[4:8])
	gotCRC := crc32.Checksum(raw[8:], crcTable)
	if wantCRC != gotCRC {
		return Batch{}, fmt.Errorf("%w: batch checksum mismatch", ErrCorrupt)
	}

	epoch := binary.LittleEndian.Uint64(raw[8:16])
	maxSeq := binary.LittleEndian.Uint64(raw[16:24])
	count := binary.LittleEndian.Uint32(raw[24:28])
	if count > maxBatchOps || int(count) > len(raw)-batchHeaderLen {
		return Batch{}, fmt.Errorf("%w: unreasonable op count %d", ErrCorrupt, count)
	}

	ops := make([]DeltaOp, count)
	cur := raw[batchHeaderLen:]
	for i := uint32(0); i < count; i++ {
		if len(cur) == 0 {
			return Batch{}, fmt.Errorf("%w: unexpected EOF at op %d", ErrCorrupt, i)
		}
		kind := DeltaKind(cur[0])
		if kind < DeltaMkdir || kind > DeltaSetAttr {
			return Batch{}, fmt.Errorf("%w: unknown delta kind %d", ErrCorrupt, kind)
		}
		cur = cur[1:]

		var err error
		var dir, dstDir, id, mode, size, nlink uint64
		var mtimeNS, atimeNS, ctimeNS int64
		var nameBytes, dstNameBytes, dataBytes []byte

		if dir, cur, err = readUvarint(cur); err != nil {
			return Batch{}, err
		}
		if nameBytes, cur, err = readBytes(cur); err != nil {
			return Batch{}, err
		}
		if dstDir, cur, err = readUvarint(cur); err != nil {
			return Batch{}, err
		}
		if dstNameBytes, cur, err = readBytes(cur); err != nil {
			return Batch{}, err
		}
		if id, cur, err = readUvarint(cur); err != nil {
			return Batch{}, err
		}
		if mode, cur, err = readUvarint(cur); err != nil {
			return Batch{}, err
		}
		if size, cur, err = readUvarint(cur); err != nil {
			return Batch{}, err
		}
		if nlink, cur, err = readUvarint(cur); err != nil {
			return Batch{}, err
		}
		if mtimeNS, cur, err = readVarint(cur); err != nil {
			return Batch{}, err
		}
		if atimeNS, cur, err = readVarint(cur); err != nil {
			return Batch{}, err
		}
		if ctimeNS, cur, err = readVarint(cur); err != nil {
			return Batch{}, err
		}
		if dataBytes, cur, err = readBytes(cur); err != nil {
			return Batch{}, err
		}
		if mode > uint64(^uint32(0)) || nlink > uint64(^uint32(0)) {
			return Batch{}, fmt.Errorf("%w: attribute field overflow", ErrCorrupt)
		}

		var copiedData []byte
		if len(dataBytes) > 0 {
			copiedData = make([]byte, len(dataBytes))
			copy(copiedData, dataBytes)
		}

		ops[i] = DeltaOp{
			Kind:    kind,
			Dir:     InodeID(dir),
			Name:    string(nameBytes),
			DstDir:  InodeID(dstDir),
			DstName: string(dstNameBytes),
			ID:      InodeID(id),
			Attr: Attr{
				ID:    InodeID(id),
				Mode:  fs.FileMode(uint32(mode)),
				Size:  size,
				Nlink: uint32(nlink),
				Mtime: time.Unix(0, mtimeNS).UTC(),
				Atime: time.Unix(0, atimeNS).UTC(),
				Ctime: time.Unix(0, ctimeNS).UTC(),
			},
			Data: copiedData,
		}
	}

	if len(cur) != 0 {
		return Batch{}, fmt.Errorf("%w: %d trailing bytes after batch", ErrCorrupt, len(cur))
	}
	return Batch{Epoch: epoch, MaxSeq: maxSeq, Ops: ops}, nil
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func readUvarint(b []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, fmt.Errorf("%w: bad uvarint", ErrCorrupt)
	}
	// Enforce canonical shortest-form encoding (matching SPECIFICATION 1).
	if n > 1 && b[n-1] == 0 {
		return 0, nil, fmt.Errorf("%w: non-minimal uvarint", ErrCorrupt)
	}
	return v, b[n:], nil
}

func readVarint(b []byte) (int64, []byte, error) {
	u, rest, err := readUvarint(b)
	if err != nil {
		return 0, nil, err
	}
	x := int64(u >> 1)
	if u&1 != 0 {
		x = ^x
	}
	return x, rest, nil
}

func readBytes(b []byte) ([]byte, []byte, error) {
	length, rest, err := readUvarint(b)
	if err != nil {
		return nil, nil, err
	}
	if length > uint64(len(rest)) {
		return nil, nil, fmt.Errorf("%w: slice length %d exceeds remaining %d", ErrCorrupt, length, len(rest))
	}
	n := int(length)
	return rest[:n], rest[n:], nil
}
