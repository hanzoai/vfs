package journal

import (
	"encoding/binary"
	"fmt"
	"io"
)

// FSPIFaultClass enumerates the below-encryption storage corruption classes
// injected directly onto raw on-disk journal bytes beneath the encrypted
// object layer.
type FSPIFaultClass string

const (
	// FaultSingleBitFlip flips a single bit inside a committed 4 KiB ciphertext sector.
	FaultSingleBitFlip FSPIFaultClass = "single_bit_flip"
	// FaultMultiByteBurst corrupts a 64-byte contiguous burst inside a committed sector.
	FaultMultiByteBurst FSPIFaultClass = "multi_byte_burst_corruption"
	// FaultMisdirectedBlockSwap swaps two valid committed 4 KiB sectors at different LBAs.
	FaultMisdirectedBlockSwap FSPIFaultClass = "misdirected_write_block_swap"
	// FaultStaleLostWriteReplay replays an authentic older sector from generation g-1 at the same LBA.
	FaultStaleLostWriteReplay FSPIFaultClass = "stale_lost_write_replay"
	// FaultCrossTenantTransplant transplants a valid encrypted sector from Tenant B into Tenant A at the same LBA.
	FaultCrossTenantTransplant FSPIFaultClass = "cross_tenant_block_transplant"
	// FaultZeroedSectorPhantom silently zero-fills a committed 4 KiB sector on disk.
	FaultZeroedSectorPhantom FSPIFaultClass = "zeroed_sector_phantom_write"
	// FaultTruncatedTornTail simulates a partial 2 KiB torn write on the tail sector.
	FaultTruncatedTornTail FSPIFaultClass = "truncated_torn_tail_sector"
	// FaultActiveAdversaryCRCForgery models an active storage adversary who modifies
	// ciphertext bytes AND recomputes an unkeyed CRC32C PI tuple (Negative Regime #1).
	FaultActiveAdversaryCRCForgery FSPIFaultClass = "crc_only_active_adversary_forgery"
)

// AllFSPIFaultClasses lists the 7 canonical accidental/storage corruption classes
// evaluated for end-to-end detection coverage.
var AllFSPIFaultClasses = []FSPIFaultClass{
	FaultSingleBitFlip,
	FaultMultiByteBurst,
	FaultMisdirectedBlockSwap,
	FaultStaleLostWriteReplay,
	FaultCrossTenantTransplant,
	FaultZeroedSectorPhantom,
	FaultTruncatedTornTail,
}

// PhysicalLayout resolves the on-disk offset and stride for a logical sector
// on either an FSPIFile or a raw baseline File.
func PhysicalLayout(f File, lba uint32) (raw File, physOff int64, stride int) {
	if ff, ok := f.(*FSPIFile); ok {
		return ff.Underlying(), ff.PhysicalOffset(lba), int(ff.PhysicalSectorStride())
	}
	return f, int64(lba) * FSPISectorSize, FSPISectorSize
}

// SnapshotPhysicalSector captures the exact raw on-disk bytes of logical sector
// lba (4 KiB ciphertext plus its PI tuple when FS-PI is enabled) so a later
// InjectStaleLostWriteReplay can replay the authentic generation g-1 block.
func SnapshotPhysicalSector(f File, lba uint32) ([]byte, error) {
	raw, physOff, stride := PhysicalLayout(f, lba)
	snap := make([]byte, stride)
	n, err := raw.ReadAt(snap, physOff)
	if err != nil {
		return nil, fmt.Errorf("fspi/fault: snapshot lba=%d: %w", lba, err)
	}
	if n != stride {
		return nil, fmt.Errorf("fspi/fault: snapshot lba=%d: %w", lba, io.ErrUnexpectedEOF)
	}
	return snap, nil
}

// InjectSingleBitFlip flips bit (0..7) at byteOffset within the 4 KiB ciphertext
// payload of lba directly on the raw underlying file.
func InjectSingleBitFlip(f File, lba uint32, byteOffset int, bit uint8) error {
	if byteOffset < 0 || byteOffset >= FSPISectorSize {
		return fmt.Errorf("fspi/fault: single_bit_flip byteOffset=%d out of range", byteOffset)
	}
	raw, physOff, _ := PhysicalLayout(f, lba)
	var b [1]byte
	if _, err := raw.ReadAt(b[:], physOff+int64(byteOffset)); err != nil {
		return err
	}
	b[0] ^= 1 << (bit & 7)
	_, err := raw.WriteAt(b[:], physOff+int64(byteOffset))
	return err
}

// InjectMultiByteBurst corrupts burstLen contiguous bytes (e.g., 64 bytes)
// starting at byteOffset inside the 4 KiB ciphertext payload of lba.
func InjectMultiByteBurst(f File, lba uint32, byteOffset, burstLen int) error {
	if byteOffset < 0 || burstLen <= 0 || byteOffset+burstLen > FSPISectorSize {
		return fmt.Errorf("fspi/fault: multi_byte_burst range [%d:%d] out of sector bounds", byteOffset, byteOffset+burstLen)
	}
	raw, physOff, _ := PhysicalLayout(f, lba)
	buf := make([]byte, burstLen)
	if _, err := raw.ReadAt(buf, physOff+int64(byteOffset)); err != nil {
		return err
	}
	for i := range buf {
		buf[i] ^= byte(0xA5 ^ (i * 13))
	}
	_, err := raw.WriteAt(buf, physOff+int64(byteOffset))
	return err
}

// InjectMisdirectedBlockSwap swaps two complete committed physical sectors
// (both ciphertext and PI metadata) between lbaA and lbaB directly on raw disk.
func InjectMisdirectedBlockSwap(f File, lbaA, lbaB uint32) error {
	if lbaA == lbaB {
		return fmt.Errorf("fspi/fault: block swap requires distinct LBAs")
	}
	rawA, offA, strideA := PhysicalLayout(f, lbaA)
	rawB, offB, strideB := PhysicalLayout(f, lbaB)
	if strideA != strideB {
		return fmt.Errorf("fspi/fault: mismatched sector strides %d vs %d", strideA, strideB)
	}
	bufA := make([]byte, strideA)
	bufB := make([]byte, strideB)
	if _, err := rawA.ReadAt(bufA, offA); err != nil {
		return err
	}
	if _, err := rawB.ReadAt(bufB, offB); err != nil {
		return err
	}
	if _, err := rawA.WriteAt(bufB, offA); err != nil {
		return err
	}
	_, err := rawB.WriteAt(bufA, offB)
	return err
}

// InjectStaleLostWriteReplay overwrites lba on raw disk with a previously
// captured authentic snapshot from generation g-1 (both ciphertext and PI tuple).
func InjectStaleLostWriteReplay(f File, lba uint32, oldSnapshot []byte) error {
	raw, physOff, stride := PhysicalLayout(f, lba)
	if len(oldSnapshot) != stride {
		return fmt.Errorf("fspi/fault: stale snapshot len=%d want %d", len(oldSnapshot), stride)
	}
	_, err := raw.WriteAt(oldSnapshot, physOff)
	return err
}

// InjectCrossTenantTransplant copies a valid committed physical sector (ciphertext
// and PI tuple) from srcFile (Tenant B) at srcLBA into dstFile (Tenant A) at dstLBA.
func InjectCrossTenantTransplant(dstFile File, dstLBA uint32, srcFile File, srcLBA uint32) error {
	rawSrc, offSrc, strideSrc := PhysicalLayout(srcFile, srcLBA)
	rawDst, offDst, strideDst := PhysicalLayout(dstFile, dstLBA)
	if strideSrc != strideDst {
		return fmt.Errorf("fspi/fault: cross-tenant stride mismatch %d vs %d", strideSrc, strideDst)
	}
	buf := make([]byte, strideSrc)
	if _, err := rawSrc.ReadAt(buf, offSrc); err != nil {
		return err
	}
	_, err := rawDst.WriteAt(buf, offDst)
	return err
}

// InjectZeroedSectorPhantom silently zero-fills the entire committed physical
// sector (4 KiB payload + PI tuple) on raw disk.
func InjectZeroedSectorPhantom(f File, lba uint32) error {
	raw, physOff, stride := PhysicalLayout(f, lba)
	zeros := make([]byte, stride)
	_, err := raw.WriteAt(zeros, physOff)
	return err
}

// InjectTruncatedTornTail simulates a partial 2 KiB torn write by leaving the
// first validPrefix bytes (typically 2048) intact and zeroing the remaining
// tail of the physical sector including any trailing PI tuple.
func InjectTruncatedTornTail(f File, lba uint32, validPrefix int) error {
	raw, physOff, stride := PhysicalLayout(f, lba)
	if validPrefix <= 0 || validPrefix >= FSPISectorSize {
		validPrefix = FSPISectorSize / 2
	}
	tail := make([]byte, stride-validPrefix)
	_, err := raw.WriteAt(tail, physOff+int64(validPrefix))
	return err
}

// InjectActiveAdversaryCRCForgery simulates an active below-encryption storage
// adversary who modifies bytes inside the 4 KiB ciphertext payload of lba AND
// recomputes a valid unkeyed Castagnoli CRC32C GuardTag in the PI tuple.
//
// Honest Negative Regime #1:
//   - Under ModeSoftwareCRC32C, the forged CRC32C matches and passes PI-layer
//     validation, demonstrating that unkeyed CRC32C does NOT protect against an
//     active adversary modifying both sector bytes and unkeyed metadata.
//   - Under ModeKeyedBLAKE3PI, the adversary cannot forge the 128-bit keyed
//     BLAKE3 MAC without the secret key, so validation rejects the tampered
//     sector BEFORE decryption with ErrFSPIGuardMismatch.
func InjectActiveAdversaryCRCForgery(f File, lba uint32, byteOffset int) error {
	if byteOffset < 0 || byteOffset >= FSPISectorSize {
		byteOffset = 64
	}
	raw, physOff, stride := PhysicalLayout(f, lba)
	buf := make([]byte, stride)
	if _, err := raw.ReadAt(buf, physOff); err != nil {
		return err
	}

	// Adversary flips payload bytes inside the ciphertext sector.
	buf[byteOffset] ^= 0xFF
	if byteOffset+1 < FSPISectorSize {
		buf[byteOffset+1] ^= 0x55
	}

	// Adversary recomputes the unkeyed Castagnoli CRC32C over the tampered sector.
	if ff, ok := f.(*FSPIFile); ok {
		sector := buf[:FSPISectorSize]
		rawPI := buf[FSPISectorSize:]
		pi := ff.decodeSectorPI(rawPI)
		forgedCRC := computeCRC32CGuard(sector, pi.AppTag, pi.RefTag, pi.Generation, pi.Flags)
		// Overwrite the GuardTag field with the forged unkeyed CRC32C.
		binary.LittleEndian.PutUint32(rawPI[0:4], forgedCRC)
	}

	_, err := raw.WriteAt(buf, physOff)
	return err
}
