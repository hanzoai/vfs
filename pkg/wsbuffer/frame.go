package wsbuffer

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

var (
	frameMagic = [4]byte{'W', 'S', 'B', '1'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

const (
	frameHeaderSize  = 32
	frameTrailerSize = 4
	extentDescSize   = 20 // offset(i64) + length(u32) + writeSeq(u64)
	barrierBodySize  = 20 // offset(i64) + length(u32) + writeSeq(u64)
	checkpointSize   = 8  // checkpointSeq(u64)
)

// FrameType identifies the kind of WAL record stored by WSBuffer.
type FrameType uint8

const (
	// FrameScrapBatch carries one or more coalesced scrap-buffer extents.
	FrameScrapBatch FrameType = 1
	// FrameDirectBarrier records that a zero-copy direct bypass write covered
	// [Offset, Offset+Length) at WriteSeq, superseding any earlier uncheckpointed
	// WAL extents in that byte range without copying payload bytes into the WAL.
	FrameDirectBarrier FrameType = 2
	// FrameCheckpoint records that all writes up to CheckpointSeq are durable
	// on the backing file.
	FrameCheckpoint FrameType = 3
)

// Extent represents one contiguous byte range write with its monotonic sequence number.
type Extent struct {
	Offset   int64
	WriteSeq uint64
	Data     []byte
}

// DirectBarrier represents a zero-payload direct-write ordering barrier.
type DirectBarrier struct {
	Offset   int64
	Length   uint32
	WriteSeq uint64
}

// Frame is a decoded WAL frame.
type Frame struct {
	Type          FrameType
	VolumeID      uint64
	FrameSeq      uint64
	Extents       []Extent
	Barrier       DirectBarrier
	CheckpointSeq uint64
}

// EncodeScrapBatchFrame encodes a slice of extents into dst (reusing capacity if
// available) as a CRC32C-protected FrameScrapBatch.
func EncodeScrapBatchFrame(dst []byte, volumeID, frameSeq uint64, extents []Extent) []byte {
	payloadLen := 4 + len(extents)*extentDescSize
	for _, e := range extents {
		payloadLen += len(e.Data)
	}
	totalLen := frameHeaderSize + payloadLen + frameTrailerSize
	if cap(dst) < totalLen {
		dst = make([]byte, totalLen)
	} else {
		dst = dst[:totalLen]
	}

	writeFrameHeader(dst[:frameHeaderSize], FrameScrapBatch, volumeID, frameSeq, uint32(payloadLen))

	body := dst[frameHeaderSize : frameHeaderSize+payloadLen]
	binary.LittleEndian.PutUint32(body[0:4], uint32(len(extents)))
	pos := 4
	for _, e := range extents {
		binary.LittleEndian.PutUint64(body[pos:pos+8], uint64(e.Offset))
		binary.LittleEndian.PutUint32(body[pos+8:pos+12], uint32(len(e.Data)))
		binary.LittleEndian.PutUint64(body[pos+12:pos+20], e.WriteSeq)
		pos += extentDescSize
	}
	for _, e := range extents {
		copy(body[pos:pos+len(e.Data)], e.Data)
		pos += len(e.Data)
	}

	payloadCRC := crc32.Checksum(body, castagnoli)
	binary.LittleEndian.PutUint32(dst[frameHeaderSize+payloadLen:totalLen], payloadCRC)
	return dst
}

// EncodeDirectBarrierFrame encodes a compact 56-byte zero-payload barrier frame.
func EncodeDirectBarrierFrame(dst []byte, volumeID, frameSeq uint64, b DirectBarrier) []byte {
	totalLen := frameHeaderSize + barrierBodySize + frameTrailerSize
	if cap(dst) < totalLen {
		dst = make([]byte, totalLen)
	} else {
		dst = dst[:totalLen]
	}

	writeFrameHeader(dst[:frameHeaderSize], FrameDirectBarrier, volumeID, frameSeq, barrierBodySize)

	body := dst[frameHeaderSize : frameHeaderSize+barrierBodySize]
	binary.LittleEndian.PutUint64(body[0:8], uint64(b.Offset))
	binary.LittleEndian.PutUint32(body[8:12], b.Length)
	binary.LittleEndian.PutUint64(body[12:20], b.WriteSeq)

	payloadCRC := crc32.Checksum(body, castagnoli)
	binary.LittleEndian.PutUint32(dst[frameHeaderSize+barrierBodySize:totalLen], payloadCRC)
	return dst
}

// EncodeCheckpointFrame encodes a 44-byte checkpoint frame.
func EncodeCheckpointFrame(dst []byte, volumeID, frameSeq, checkpointSeq uint64) []byte {
	totalLen := frameHeaderSize + checkpointSize + frameTrailerSize
	if cap(dst) < totalLen {
		dst = make([]byte, totalLen)
	} else {
		dst = dst[:totalLen]
	}

	writeFrameHeader(dst[:frameHeaderSize], FrameCheckpoint, volumeID, frameSeq, checkpointSize)

	body := dst[frameHeaderSize : frameHeaderSize+checkpointSize]
	binary.LittleEndian.PutUint64(body[0:8], checkpointSeq)

	payloadCRC := crc32.Checksum(body, castagnoli)
	binary.LittleEndian.PutUint32(dst[frameHeaderSize+checkpointSize:totalLen], payloadCRC)
	return dst
}

func writeFrameHeader(h []byte, ft FrameType, volumeID, frameSeq uint64, payloadLen uint32) {
	copy(h[0:4], frameMagic[:])
	h[4] = byte(ft)
	h[5] = 0
	h[6] = 0
	h[7] = 0
	binary.LittleEndian.PutUint64(h[8:16], volumeID)
	binary.LittleEndian.PutUint64(h[16:24], frameSeq)
	binary.LittleEndian.PutUint32(h[24:28], payloadLen)
	binary.LittleEndian.PutUint32(h[28:32], crc32.Checksum(h[0:28], castagnoli))
}

// DecodeFrame decodes the next WAL frame at the start of buf and returns the
// decoded Frame, the number of bytes consumed, or an error:
//   - ErrTruncated when buf ends mid-header or mid-payload (torn tail)
//   - ErrChecksumMismatch when HeaderCRC or PayloadCRC fails
//   - ErrCorrupt when magic, frame type, or extent layout is invalid
func DecodeFrame(buf []byte) (Frame, int, error) {
	if len(buf) < frameHeaderSize {
		return Frame{}, 0, ErrTruncated
	}
	if buf[0] != frameMagic[0] || buf[1] != frameMagic[1] || buf[2] != frameMagic[2] || buf[3] != frameMagic[3] {
		return Frame{}, 0, fmt.Errorf("%w: bad magic %q", ErrCorrupt, buf[0:4])
	}
	wantHdrCRC := binary.LittleEndian.Uint32(buf[28:32])
	gotHdrCRC := crc32.Checksum(buf[0:28], castagnoli)
	if wantHdrCRC != gotHdrCRC {
		return Frame{}, 0, fmt.Errorf("%w: header crc want=%#x got=%#x", ErrChecksumMismatch, wantHdrCRC, gotHdrCRC)
	}

	ft := FrameType(buf[4])
	volumeID := binary.LittleEndian.Uint64(buf[8:16])
	frameSeq := binary.LittleEndian.Uint64(buf[16:24])
	payloadLen := int(binary.LittleEndian.Uint32(buf[24:28]))
	if payloadLen < 0 {
		return Frame{}, 0, fmt.Errorf("%w: negative payload len", ErrCorrupt)
	}
	totalLen := frameHeaderSize + payloadLen + frameTrailerSize
	if len(buf) < totalLen {
		return Frame{}, 0, ErrTruncated
	}

	body := buf[frameHeaderSize : frameHeaderSize+payloadLen]
	wantBodyCRC := binary.LittleEndian.Uint32(buf[frameHeaderSize+payloadLen : totalLen])
	gotBodyCRC := crc32.Checksum(body, castagnoli)
	if wantBodyCRC != gotBodyCRC {
		return Frame{}, 0, fmt.Errorf("%w: payload crc want=%#x got=%#x", ErrChecksumMismatch, wantBodyCRC, gotBodyCRC)
	}

	f := Frame{
		Type:     ft,
		VolumeID: volumeID,
		FrameSeq: frameSeq,
	}

	switch ft {
	case FrameScrapBatch:
		if len(body) < 4 {
			return Frame{}, 0, fmt.Errorf("%w: scrap batch too short", ErrCorrupt)
		}
		count := int(binary.LittleEndian.Uint32(body[0:4]))
		descEnd := 4 + count*extentDescSize
		if count < 0 || descEnd < 4 || descEnd > len(body) {
			return Frame{}, 0, fmt.Errorf("%w: invalid extent count %d", ErrCorrupt, count)
		}
		extents := make([]Extent, count)
		dataPos := descEnd
		for i := 0; i < count; i++ {
			offPos := 4 + i*extentDescSize
			off := int64(binary.LittleEndian.Uint64(body[offPos : offPos+8]))
			length := int(binary.LittleEndian.Uint32(body[offPos+8 : offPos+12]))
			wseq := binary.LittleEndian.Uint64(body[offPos+12 : offPos+20])
			if off < 0 || length < 0 || dataPos+length > len(body) {
				return Frame{}, 0, fmt.Errorf("%w: extent %d out of bounds", ErrCorrupt, i)
			}
			// Copy payload bytes out so decoded frames do not alias a caller's reused buffer.
			data := make([]byte, length)
			copy(data, body[dataPos:dataPos+length])
			extents[i] = Extent{
				Offset:   off,
				WriteSeq: wseq,
				Data:     data,
			}
			dataPos += length
		}
		if dataPos != len(body) {
			return Frame{}, 0, fmt.Errorf("%w: trailing bytes in scrap batch (%d != %d)", ErrCorrupt, dataPos, len(body))
		}
		f.Extents = extents
		return f, totalLen, nil

	case FrameDirectBarrier:
		if len(body) != barrierBodySize {
			return Frame{}, 0, fmt.Errorf("%w: bad barrier size %d", ErrCorrupt, len(body))
		}
		off := int64(binary.LittleEndian.Uint64(body[0:8]))
		length := binary.LittleEndian.Uint32(body[8:12])
		wseq := binary.LittleEndian.Uint64(body[12:20])
		if off < 0 {
			return Frame{}, 0, fmt.Errorf("%w: negative barrier offset %d", ErrCorrupt, off)
		}
		f.Barrier = DirectBarrier{
			Offset:   off,
			Length:   length,
			WriteSeq: wseq,
		}
		return f, totalLen, nil

	case FrameCheckpoint:
		if len(body) != checkpointSize {
			return Frame{}, 0, fmt.Errorf("%w: bad checkpoint size %d", ErrCorrupt, len(body))
		}
		f.CheckpointSeq = binary.LittleEndian.Uint64(body[0:8])
		return f, totalLen, nil

	default:
		return Frame{}, 0, fmt.Errorf("%w: unknown frame type %d", ErrCorrupt, ft)
	}
}
