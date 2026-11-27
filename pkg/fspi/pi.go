package fspi

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/hanzoai/vfs/internal/blake3"
)

// Supported logical sector sizes matching NVMe / T10-DIF LBA formats.
const (
	SectorSize512  = 512
	SectorSize4096 = 4096
)

// Protection Information tuple sizes in bytes.
const (
	// Tuple64Size is the exact 8-byte (64-bit) NVMe / T10-DIF Type 1/2/3 PI
	// tuple: [GuardTag:2B][AppTag:2B][RefTag:4B].
	Tuple64Size = 8

	// Tuple128Size is the 16-byte (128-bit) software strong-guard PI tuple:
	// [GuardCRC32C:4B][GuardBLAKE3:4B][AppTag:2B][Epoch:2B][RefTag:4B].
	Tuple128Size = 16
)

// NVMe / T10-DIF reserved escape tags (T10 SBC-3 / NVMe NVM Command Set §5.21):
// AppTag 0xFFFF and RefTag 0xFFFFFFFF disable hardware PI checking on Type 1/2
// namespaces, so FS-PI never emits them as valid tags and fails closed if seen.
const (
	t10AppTagEscape uint16 = 0xFFFF
	t10RefTagEscape uint32 = 0xFFFFFFFF
)

// GuardMode selects the FS-PI Protection Information format.
type GuardMode uint8

const (
	// GuardNone disables FS-PI metadata framing (baseline mode).
	GuardNone GuardMode = iota

	// GuardT10DIF uses the 8-byte (64-bit) NVMe / T10-DIF hardware-compatible
	// PI tuple: 16-bit CRC16-T10-DIF GuardTag + 16-bit AppTag + 32-bit
	// epoch-bound RefTag.
	GuardT10DIF

	// GuardSoftwareStrong uses the 16-byte (128-bit) software strong-guard PI
	// tuple: 32-bit Castagnoli CRC32C + 32-bit domain-separated BLAKE3 guard
	// over (AppTag, Epoch, RefTag, payload) + 16-bit AppTag + 16-bit Epoch +
	// 32-bit RefTag.
	GuardSoftwareStrong
)

func (m GuardMode) String() string {
	switch m {
	case GuardNone:
		return "none"
	case GuardT10DIF:
		return "t10_dif_64"
	case GuardSoftwareStrong:
		return "software_strong_128"
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

// TupleSize returns the serialized PI tuple length in bytes for m.
func (m GuardMode) TupleSize() int {
	switch m {
	case GuardNone:
		return 0
	case GuardT10DIF:
		return Tuple64Size
	case GuardSoftwareStrong:
		return Tuple128Size
	default:
		return 0
	}
}

// ValidateSectorSize verifies that sectorSize is an NVMe/T10-DIF supported size.
func ValidateSectorSize(sectorSize int) error {
	if sectorSize != SectorSize512 && sectorSize != SectorSize4096 {
		return fmt.Errorf("%w: got %d", ErrUnsupportedSectorSize, sectorSize)
	}
	return nil
}

// crc16T10DIFPoly is the ANSI INCITS 394 / T10 SBC-3 / NVMe PI CRC-16 polynomial
// (x^16 + x^15 + x^11 + x^9 + x^8 + x^7 + x^5 + x^4 + x^2 + x + 1).
const crc16T10DIFPoly uint16 = 0x8BB7

var (
	crc16T10Table   [256]uint16
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
)

func init() {
	for i := 0; i < 256; i++ {
		crc := uint16(i) << 8
		for b := 0; b < 8; b++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ crc16T10DIFPoly
			} else {
				crc <<= 1
			}
		}
		crc16T10Table[i] = crc
	}
}

// CRC16T10DIF computes the standard T10-DIF / NVMe Protection Information
// 16-bit CRC (polynomial 0x8BB7, init 0x0000, non-reflected, xorout 0x0000).
// Standard check value: CRC16T10DIF([]byte("123456789")) == 0xD0DB.
func CRC16T10DIF(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc = (crc << 8) ^ crc16T10Table[byte(crc>>8)^b]
	}
	return crc
}

// DeriveAppTag deterministically derives a 16-bit volume/tenant/round
// namespace AppTag using BLAKE3 key derivation, ensuring it never collides
// with the reserved T10-DIF escape tag (0xFFFF) or zero (0x0000).
func DeriveAppTag(volumeID, tenantID []byte, round uint64) uint16 {
	material := make([]byte, 0, len(volumeID)+len(tenantID)+10)
	material = binary.BigEndian.AppendUint16(material, uint16(len(volumeID)))
	material = append(material, volumeID...)
	material = binary.BigEndian.AppendUint16(material, uint16(len(tenantID)))
	material = append(material, tenantID...)
	material = binary.BigEndian.AppendUint64(material, round)

	var out [4]byte
	blake3.DeriveKey("hanzoai/vfs fspi apptag v1", material, out[:])
	tag := binary.BigEndian.Uint16(out[:2])
	if tag == 0 {
		tag = 1
	} else if tag == t10AppTagEscape {
		tag = t10AppTagEscape - 1
	}
	return tag
}

// PackT10RefTag encodes a 24-bit LBA and 8-bit rolling epoch into the 32-bit
// NVMe / T10-DIF RefTag, avoiding the reserved 0xFFFFFFFF escape value.
// Bit layout: [epoch:8 bits][lba:24 bits].
func PackT10RefTag(lba uint64, epoch uint16) uint32 {
	ep8 := uint32(epoch & 0xFF)
	if ep8 == 0 && epoch != 0 {
		ep8 = 1 // avoid 0 after 8-bit wrap for written sectors
	}
	if ep8 == 0xFF {
		ep8 = 0xFE // prevent 0xFFFFFFFF escape tag when lba == 0xFFFFFF
	}
	return (ep8 << 24) | uint32(lba&0x00FFFFFF)
}

// UnpackT10RefTag splits a 32-bit T10-DIF RefTag into its 8-bit epoch and
// 24-bit logical block address components.
func UnpackT10RefTag(ref uint32) (lba uint32, epoch8 uint8) {
	return ref & 0x00FFFFFF, uint8(ref >> 24)
}

// blake3Guard32 computes the 32-bit domain-separated BLAKE3 guard binding
// (AppTag, Epoch, RefTag, payload) for GuardSoftwareStrong mode.
func blake3Guard32(appTag, epoch uint16, refTag uint32, payload []byte) uint32 {
	if len(payload) <= SectorSize4096 {
		var buf [16 + SectorSize4096]byte
		copy(buf[0:8], "FSPI-G32")
		binary.BigEndian.PutUint16(buf[8:10], appTag)
		binary.BigEndian.PutUint16(buf[10:12], epoch)
		binary.BigEndian.PutUint32(buf[12:16], refTag)
		copy(buf[16:16+len(payload)], payload)
		sum := blake3.Sum(buf[:16+len(payload)])
		return binary.BigEndian.Uint32(sum[:4])
	}
	h := blake3.New()
	var hdr [16]byte
	copy(hdr[0:8], "FSPI-G32")
	binary.BigEndian.PutUint16(hdr[8:10], appTag)
	binary.BigEndian.PutUint16(hdr[10:12], epoch)
	binary.BigEndian.PutUint32(hdr[12:16], refTag)
	_, _ = h.Write(hdr[:])
	_, _ = h.Write(payload)
	var sum [blake3.Size]byte
	h.Sum(sum[:0])
	return binary.BigEndian.Uint32(sum[:4])
}

// EncodePI writes the Protection Information tuple for payload into dst.
// dst must have length >= mode.TupleSize().
func EncodePI(dst []byte, mode GuardMode, appTag uint16, lba uint64, epoch uint16, payload []byte) error {
	switch mode {
	case GuardNone:
		return nil
	case GuardT10DIF:
		if len(dst) < Tuple64Size {
			return fmt.Errorf("%w: short PI buffer (%d < %d)", ErrMetadataTruncated, len(dst), Tuple64Size)
		}
		guard := CRC16T10DIF(payload)
		ref := PackT10RefTag(lba, epoch)
		binary.BigEndian.PutUint16(dst[0:2], guard)
		binary.BigEndian.PutUint16(dst[2:4], appTag)
		binary.BigEndian.PutUint32(dst[4:8], ref)
		return nil
	case GuardSoftwareStrong:
		if len(dst) < Tuple128Size {
			return fmt.Errorf("%w: short PI buffer (%d < %d)", ErrMetadataTruncated, len(dst), Tuple128Size)
		}
		ref := uint32(lba)
		gCRC := crc32.Checksum(payload, castagnoliTable)
		gB3 := blake3Guard32(appTag, epoch, ref, payload)
		binary.BigEndian.PutUint32(dst[0:4], gCRC)
		binary.BigEndian.PutUint32(dst[4:8], gB3)
		binary.BigEndian.PutUint16(dst[8:10], appTag)
		binary.BigEndian.PutUint16(dst[10:12], epoch)
		binary.BigEndian.PutUint32(dst[12:16], ref)
		return nil
	default:
		return fmt.Errorf("%w: unknown guard mode %d", ErrPICorrupt, mode)
	}
}

// VerifyPI validates payload against its serialized Protection Information
// tuple, returning nil on exact match or a typed *IntegrityError attributing
// the exact failure class.
func VerifyPI(seg string, mode GuardMode, expectedApp uint16, lba uint64, expectedEpoch uint16, payload, pi []byte) error {
	switch mode {
	case GuardNone:
		return nil
	case GuardT10DIF:
		return verifyT10DIF(seg, expectedApp, lba, expectedEpoch, payload, pi)
	case GuardSoftwareStrong:
		return verifySoftwareStrong(seg, expectedApp, lba, expectedEpoch, payload, pi)
	default:
		return &IntegrityError{Kind: ErrPICorrupt, Segment: seg, LBA: lba, Detail: fmt.Sprintf("unknown mode %d", mode)}
	}
}

func isAllZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func verifyT10DIF(seg string, expectedApp uint16, lba uint64, expectedEpoch uint16, payload, pi []byte) error {
	if len(pi) < Tuple64Size {
		return &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: seg, LBA: lba,
			Detail: fmt.Sprintf("PI tuple truncated: %d < %d", len(pi), Tuple64Size),
		}
	}
	if expectedEpoch > 0 && isAllZero(pi[:Tuple64Size]) {
		return &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, ExpectedEp: expectedEpoch,
			Detail: "PI tuple zero-filled for written sector",
		}
	}
	gotGuard := binary.BigEndian.Uint16(pi[0:2])
	gotApp := binary.BigEndian.Uint16(pi[2:4])
	gotRef := binary.BigEndian.Uint32(pi[4:8])

	// Fail closed on reserved T10-DIF escape values (0xFFFF / 0xFFFFFFFF).
	if gotApp == t10AppTagEscape || gotRef == t10RefTagEscape {
		return &IntegrityError{
			Kind: ErrPICorrupt, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: PackT10RefTag(lba, expectedEpoch), GotRef: gotRef,
			Detail: "reserved T10-DIF escape tag observed (fail-closed)",
		}
	}

	expRef := PackT10RefTag(lba, expectedEpoch)
	expLBA, expEp8 := UnpackT10RefTag(expRef)
	gotLBA, gotEp8 := UnpackT10RefTag(gotRef)
	calcGuard := CRC16T10DIF(payload)
	guardOK := calcGuard == gotGuard

	// Exact attribution order:
	// 1. Cross-volume swap: AppTag differs.
	if gotApp != expectedApp {
		kind := ErrAppTagCrossVolume
		detail := fmt.Sprintf("got app_tag=0x%04x want 0x%04x", gotApp, expectedApp)
		if !guardOK {
			// If both AppTag and guard fail, could be corrupted PI or cross-volume + corrupt.
			detail += fmt.Sprintf(" (guard also mismatched: got=0x%04x calc=0x%04x)", gotGuard, calcGuard)
		}
		return &IntegrityError{
			Kind: kind, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: expRef, GotRef: gotRef,
			ExpectedEp: expectedEpoch, GotEp: uint16(gotEp8),
			Detail: detail,
		}
	}
	// 2. Misdirected write: LBA portion of RefTag differs.
	if gotLBA != expLBA {
		return &IntegrityError{
			Kind: ErrRefTagMisdirected, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: expRef, GotRef: gotRef,
			ExpectedEp: expectedEpoch, GotEp: uint16(gotEp8),
			Detail: fmt.Sprintf("got ref_lba=%d want %d", gotLBA, expLBA),
		}
	}
	// 3. Epoch mismatch: distinguishes torn write (data half-updated, guard fails)
	//    from lost write / stale replay (entire stale sector intact, guard passes).
	if gotEp8 != expEp8 {
		if !guardOK {
			return &IntegrityError{
				Kind: ErrTornWrite, Segment: seg, LBA: lba,
				ExpectedApp: expectedApp, GotApp: gotApp,
				ExpectedRef: expRef, GotRef: gotRef,
				ExpectedEp: expectedEpoch, GotEp: uint16(gotEp8),
				Detail: fmt.Sprintf("stale PI epoch %d (want %d) with mismatched guard 0x%04x != 0x%04x", gotEp8, expEp8, gotGuard, calcGuard),
			}
		}
		return &IntegrityError{
			Kind: ErrLostWrite, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: expRef, GotRef: gotRef,
			ExpectedEp: expectedEpoch, GotEp: uint16(gotEp8),
			Detail: fmt.Sprintf("intact stale sector at epoch %d (want %d)", gotEp8, expEp8),
		}
	}
	// 4. Payload or guard corruption.
	if !guardOK {
		return &IntegrityError{
			Kind: ErrGuardMismatch, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: expRef, GotRef: gotRef,
			ExpectedEp: expectedEpoch, GotEp: uint16(gotEp8),
			Detail: fmt.Sprintf("CRC16-T10-DIF mismatch: got 0x%04x calc 0x%04x", gotGuard, calcGuard),
		}
	}
	return nil
}

func verifySoftwareStrong(seg string, expectedApp uint16, lba uint64, expectedEpoch uint16, payload, pi []byte) error {
	if len(pi) < Tuple128Size {
		return &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: seg, LBA: lba,
			Detail: fmt.Sprintf("PI tuple truncated: %d < %d", len(pi), Tuple128Size),
		}
	}
	if expectedEpoch > 0 && isAllZero(pi[:Tuple128Size]) {
		return &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, ExpectedEp: expectedEpoch,
			Detail: "PI tuple zero-filled for written sector",
		}
	}

	gotCRC := binary.BigEndian.Uint32(pi[0:4])
	gotB3 := binary.BigEndian.Uint32(pi[4:8])
	gotApp := binary.BigEndian.Uint16(pi[8:10])
	gotEp := binary.BigEndian.Uint16(pi[10:12])
	gotRef := binary.BigEndian.Uint32(pi[12:16])

	expRef := uint32(lba)
	calcCRC := crc32.Checksum(payload, castagnoliTable)
	calcB3Self := blake3Guard32(gotApp, gotEp, gotRef, payload)

	crcOK := calcCRC == gotCRC
	b3SelfOK := calcB3Self == gotB3

	// Orthogonal PI-tuple corruption detection:
	// If exactly one of (CRC32C, self-consistent BLAKE3) passes, the payload is
	// consistent with one independent guard while the PI tuple itself was altered!
	if crcOK != b3SelfOK {
		return &IntegrityError{
			Kind: ErrPICorrupt, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: expRef, GotRef: gotRef,
			ExpectedEp: expectedEpoch, GotEp: gotEp,
			Detail: fmt.Sprintf("PI tuple bit-corruption detected (crc_ok=%v blake3_self_ok=%v)", crcOK, b3SelfOK),
		}
	}

	// Both guards agree with (gotApp, gotEp, gotRef, payload): the sector is
	// self-consistent, so any metadata mismatch is a structural storage fault!
	if crcOK && b3SelfOK {
		if gotApp != expectedApp {
			return &IntegrityError{
				Kind: ErrAppTagCrossVolume, Segment: seg, LBA: lba,
				ExpectedApp: expectedApp, GotApp: gotApp,
				ExpectedRef: expRef, GotRef: gotRef,
				ExpectedEp: expectedEpoch, GotEp: gotEp,
				Detail: fmt.Sprintf("valid foreign sector from app_tag=0x%04x (want 0x%04x)", gotApp, expectedApp),
			}
		}
		if gotRef != expRef {
			return &IntegrityError{
				Kind: ErrRefTagMisdirected, Segment: seg, LBA: lba,
				ExpectedApp: expectedApp, GotApp: gotApp,
				ExpectedRef: expRef, GotRef: gotRef,
				ExpectedEp: expectedEpoch, GotEp: gotEp,
				Detail: fmt.Sprintf("valid sector misdirected from lba=%d to lba=%d", gotRef, expRef),
			}
		}
		if gotEp != expectedEpoch {
			return &IntegrityError{
				Kind: ErrLostWrite, Segment: seg, LBA: lba,
				ExpectedApp: expectedApp, GotApp: gotApp,
				ExpectedRef: expRef, GotRef: gotRef,
				ExpectedEp: expectedEpoch, GotEp: gotEp,
				Detail: fmt.Sprintf("intact stale sector at epoch=%d (want %d)", gotEp, expectedEpoch),
			}
		}
		return nil
	}

	// Both guards failed: payload was modified relative to its PI tuple.
	if gotEp != expectedEpoch {
		return &IntegrityError{
			Kind: ErrTornWrite, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: expRef, GotRef: gotRef,
			ExpectedEp: expectedEpoch, GotEp: gotEp,
			Detail: fmt.Sprintf("torn sector: partial payload with stale PI epoch=%d (want %d)", gotEp, expectedEpoch),
		}
	}
	if gotApp != expectedApp {
		return &IntegrityError{
			Kind: ErrAppTagCrossVolume, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: expRef, GotRef: gotRef,
			ExpectedEp: expectedEpoch, GotEp: gotEp,
			Detail: fmt.Sprintf("corrupt cross-volume sector app_tag=0x%04x (want 0x%04x)", gotApp, expectedApp),
		}
	}
	if gotRef != expRef {
		return &IntegrityError{
			Kind: ErrRefTagMisdirected, Segment: seg, LBA: lba,
			ExpectedApp: expectedApp, GotApp: gotApp,
			ExpectedRef: expRef, GotRef: gotRef,
			ExpectedEp: expectedEpoch, GotEp: gotEp,
			Detail: fmt.Sprintf("corrupt misdirected sector lba=%d (want %d)", gotRef, expRef),
		}
	}
	return &IntegrityError{
		Kind: ErrGuardMismatch, Segment: seg, LBA: lba,
		ExpectedApp: expectedApp, GotApp: gotApp,
		ExpectedRef: expRef, GotRef: gotRef,
		ExpectedEp: expectedEpoch, GotEp: gotEp,
		Detail: fmt.Sprintf("guard mismatch: crc32c got=0x%08x calc=0x%08x, blake3 got=0x%08x calc=0x%08x", gotCRC, calcCRC, gotB3, calcB3Self),
	}
}
