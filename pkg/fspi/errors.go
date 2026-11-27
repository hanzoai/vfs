// Package fspi implements clean-room FS-PI (FAST '26) end-to-end journal
// integrity protection for Hanzo VFS (HIP-1344, #67).
//
// FS-PI attaches per-sector Protection Information (GuardTag, AppTag, RefTag,
// and write Epoch) to local NVMe journal writes using either T10-DIF/DIX
// hardware-compatible 8-byte tuples or 16-byte software strong-guard tuples,
// and verifies integrity strictly before HIP-0302 / AES-256-GCM AEAD
// decryption and plaintext exposure.
package fspi

import (
	"errors"
	"fmt"
)

var (
	// ErrGuardMismatch indicates payload corruption (bit flip or multi-byte
	// burst) detected by the sector GuardTag (CRC16-T10-DIF or CRC32C+BLAKE3)
	// while AppTag, RefTag, and Epoch remained intact.
	ErrGuardMismatch = errors.New("fspi: sector guard tag mismatch")

	// ErrRefTagMisdirected indicates a valid sector was written to or read from
	// the wrong logical block address (LBA / offset) within the journal segment.
	ErrRefTagMisdirected = errors.New("fspi: sector ref tag mismatch (misdirected write)")

	// ErrAppTagCrossVolume indicates a sector belonging to another volume,
	// tenant, or fencing round was substituted into this journal.
	ErrAppTagCrossVolume = errors.New("fspi: sector app tag mismatch (cross-volume swap)")

	// ErrTornWrite indicates a partial/torn sector write where data and
	// protection information span different write epochs or a partial sector.
	ErrTornWrite = errors.New("fspi: torn sector write detected")

	// ErrLostWrite indicates a silent lost write or stale sector replay where
	// an entire intact sector reverted to an earlier write epoch.
	ErrLostWrite = errors.New("fspi: lost write / stale sector epoch detected")

	// ErrMetadataTruncated indicates segment PI superblock or sector framing
	// truncation, corruption, or unexpected zero-fill.
	ErrMetadataTruncated = errors.New("fspi: metadata or header truncated/zero-filled")

	// ErrPICorrupt indicates corruption directly within the Protection
	// Information tuple bytes themselves (fail-closed rejection).
	ErrPICorrupt = errors.New("fspi: protection information tuple corrupted")

	// ErrAEADAuthFailed indicates HIP-0302 / AES-256-GCM AEAD authentication
	// failure during envelope open (defense-in-depth against active forgery).
	ErrAEADAuthFailed = errors.New("fspi: AEAD envelope authentication failed")

	// ErrUnsupportedSectorSize indicates an unsupported sector size (must be
	// 512 or 4096 bytes).
	ErrUnsupportedSectorSize = errors.New("fspi: unsupported sector size (want 512 or 4096)")
)

// IntegrityError provides structured fault attribution when FS-PI or AEAD
// verification rejects a journal sector or record prior to plaintext exposure.
type IntegrityError struct {
	Kind        error
	Segment     string
	LBA         uint64
	ExpectedApp uint16
	GotApp      uint16
	ExpectedRef uint32
	GotRef      uint32
	ExpectedEp  uint16
	GotEp       uint16
	Detail      string
}

func (e *IntegrityError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s (seg=%s lba=%d): %s", e.Kind, e.Segment, e.LBA, e.Detail)
	}
	return fmt.Sprintf("%s (seg=%s lba=%d)", e.Kind, e.Segment, e.LBA)
}

func (e *IntegrityError) Unwrap() error { return e.Kind }
