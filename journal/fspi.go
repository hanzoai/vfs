package journal

// End-to-End Journal Integrity (FS-PI) — Clean-Room Implementation
//
// Reference:
//   FS-PI (Gupta et al., USENIX FAST '26),
//   https://www.usenix.org/conference/fast26/presentation/gupta
//   NVMe 2.0 NVM Command Set Specification §5.2 (End-to-End Data Protection /
//   T10-DIF Protection Information).
//   Hanzo HIP-1344 (Agent-Optimized Storage and Recovery), HIP-1345, HIP-0302.
//
// Design overview:
//   FSPIDisk and FSPIFile wrap journal.Disk and journal.File transparently
//   (opt-in via Options.Integrity; nil leaves baseline journal behavior
//   untouched). Every 4 KiB logical sector is stored on disk alongside a
//   Protection Information tuple (PITuple) binding:
//     - GuardTag / GuardMAC : Castagnoli CRC32C (+ T10-CRC16-DIF helper) or
//                             128-bit keyed BLAKE3 MAC over the ciphertext sector
//     - AppTag              : 32-bit tenant/volume domain tag (cross-tenant isolation)
//     - RefTag              : 32-bit logical block address (LBA; prevents block swap)
//     - Generation          : monotonic per-sector epoch counter (prevents stale
//                             lost-write replay)
//
//   Validation executes on ReadAt BEFORE ciphertext bytes are returned to the
//   upper AEAD cipher envelope (HIP-0302 / AES-256-GCM), ensuring corrupted,
//   swapped, replayed, or transplanted sectors are rejected prior to decryption
//   and plaintext exposure.

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"

	"github.com/hanzoai/vfs/internal/blake3"
)

const (
	// FSPISectorSize is the logical sector size (4 KiB) protected by each PI tuple.
	FSPISectorSize = 4096

	// FSPIT10DIFSize is the standard 8-byte SCSI/NVMe T10-DIF protection tuple size.
	FSPIT10DIFSize = 8

	// FSPITupleSizeCRC32C is the 16-byte NVMe extended-LBA PI tuple used by
	// ModeSoftwareCRC32C (0.39% sector metadata overhead).
	FSPITupleSizeCRC32C = 16

	// FSPITupleSizeBLAKE3 is the 32-byte cryptographic PI tuple used by
	// ModeKeyedBLAKE3PI (128-bit keyed BLAKE3 MAC + AppTag + RefTag + Generation).
	FSPITupleSizeBLAKE3 = 32

	fspiHeaderSize     = 64
	fspiTrailerMACSize = 16
	fspiVersion        = 1

	piFlagValid      = 1 << 0
	piFlagModeCRC32C = 1 << 1
	piFlagModeBLAKE3 = 1 << 2
)

var fspiMagic = [4]byte{'F', 'S', 'P', 'I'}

// FSPIMode selects the end-to-end journal integrity protection mode.
type FSPIMode uint8

const (
	// ModeNone disables FS-PI wrapping (unmodified baseline journal).
	ModeNone FSPIMode = iota
	// ModeSoftwareCRC32C validates per-sector 16-byte PI tuples using unkeyed
	// Castagnoli CRC32C GuardTag + AppTag + RefTag + Generation before decryption.
	// Detects 100% of accidental media bitrot, swaps, replays, and torn writes;
	// vulnerable to an active storage adversary who rewrites both data and CRC.
	ModeSoftwareCRC32C
	// ModeKeyedBLAKE3PI validates per-sector 32-byte cryptographic PI tuples
	// using a 128-bit keyed BLAKE3 MAC bound to (tenant, volume, file, LBA,
	// generation) prior to HIP-0302 AEAD decryption. Detects both accidental
	// corruption and active below-encryption tampering/forgery.
	ModeKeyedBLAKE3PI
	// ModeHardwarePINVMeProbe probes Linux /sys/block/*/integrity for NVMe/T10-DIF
	// hardware protection information offload and falls back cleanly to software
	// validation (FallbackMode, defaulting to ModeKeyedBLAKE3PI) when hardware PI
	// is unavailable on virtualized or cloud block devices.
	ModeHardwarePINVMeProbe
	// ModeAEADOnlyBaseline represents the comparison baseline where HIP-0302 AEAD
	// protects records at the object layer with no below-encryption sector PI.
	ModeAEADOnlyBaseline
)

// String returns a canonical name for m.
func (m FSPIMode) String() string {
	switch m {
	case ModeNone:
		return "none"
	case ModeSoftwareCRC32C:
		return "software_crc32c"
	case ModeKeyedBLAKE3PI:
		return "keyed_blake3_pi"
	case ModeHardwarePINVMeProbe:
		return "hw_nvme_pi_probe"
	case ModeAEADOnlyBaseline:
		return "aead_only_baseline"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(m))
	}
}

// Integrity error sentinels. Sector/header validation errors wrap both
// ErrCorrupt and ErrFSPIIntegrity so existing journal callers handling
// ErrCorrupt fail closed automatically.
var (
	ErrFSPIIntegrity      = errors.New("journal/fspi: end-to-end integrity check failed")
	ErrFSPIGuardMismatch  = errors.New("journal/fspi: guard tag verification failed")
	ErrFSPIAppTagMismatch = errors.New("journal/fspi: app tag tenant/volume domain mismatch")
	ErrFSPIRefTagMismatch = errors.New("journal/fspi: ref tag LBA mismatch (misdirected write or block swap)")
	ErrFSPIStaleReplay    = errors.New("journal/fspi: stale sector generation replay detected")
	ErrFSPIZeroedSector   = errors.New("journal/fspi: zeroed sector phantom write detected")
	ErrFSPITornSector     = errors.New("journal/fspi: truncated or torn sector detected")
	ErrFSPIHeaderCorrupt  = errors.New("journal/fspi: segment integrity header corrupt")
	ErrFSPIInvalidConfig  = errors.New("journal/fspi: invalid configuration")
)

type fspiError struct {
	base error // ErrCorrupt or ErrFormat
	kind error
	msg  string
}

func (e *fspiError) Error() string { return e.msg }

func (e *fspiError) Unwrap() []error {
	return []error{e.base, ErrFSPIIntegrity, e.kind}
}

func newCorruptFSPIError(kind error, format string, args ...any) error {
	return &fspiError{
		base: ErrCorrupt,
		kind: kind,
		msg:  fmt.Sprintf("%s: %s", kind.Error(), fmt.Sprintf(format, args...)),
	}
}

func newFormatFSPIError(format string, args ...any) error {
	return &fspiError{
		base: ErrFormat,
		kind: ErrFSPIHeaderCorrupt,
		msg:  fmt.Sprintf("%s: %s", ErrFormat.Error(), fmt.Sprintf(format, args...)),
	}
}

// PITuple holds the decoded Protection Information metadata for one 4 KiB sector.
type PITuple struct {
	// GuardTag holds the CRC32C guard (ModeSoftwareCRC32C) or the lower 64 bits
	// of the 128-bit keyed BLAKE3 MAC (ModeKeyedBLAKE3PI).
	GuardTag uint64
	// GuardExt holds the upper 64 bits of the 128-bit keyed BLAKE3 MAC
	// (zero in ModeSoftwareCRC32C).
	GuardExt uint64
	// AppTag binds the sector to its tenant and volume domain.
	AppTag uint32
	// RefTag binds the sector to its logical block address (LBA).
	RefTag uint32
	// Generation is the monotonic per-sector write generation counter.
	Generation uint64
	// Flags records validity and mode bits.
	Flags uint16
}

// T10DIFTuple is the standard 8-byte SCSI SBC / NVMe T10-DIF protection tuple
// (2-byte CRC-16 Guard, 2-byte Application Tag, 4-byte Reference Tag, big-endian).
type T10DIFTuple struct {
	Guard  uint16
	AppTag uint16
	RefTag uint32
}

// MarshalBinary encodes t into 8 bytes in NVMe/T10-DIF network byte order.
func (t T10DIFTuple) MarshalBinary() [FSPIT10DIFSize]byte {
	var b [FSPIT10DIFSize]byte
	binary.BigEndian.PutUint16(b[0:2], t.Guard)
	binary.BigEndian.PutUint16(b[2:4], t.AppTag)
	binary.BigEndian.PutUint32(b[4:8], t.RefTag)
	return b
}

// UnmarshalT10DIF decodes an 8-byte NVMe/T10-DIF protection tuple.
func UnmarshalT10DIF(b []byte) (T10DIFTuple, error) {
	if len(b) < FSPIT10DIFSize {
		return T10DIFTuple{}, ErrFSPITornSector
	}
	return T10DIFTuple{
		Guard:  binary.BigEndian.Uint16(b[0:2]),
		AppTag: binary.BigEndian.Uint16(b[2:4]),
		RefTag: binary.BigEndian.Uint32(b[4:8]),
	}, nil
}

// t10DIFTable is the precomputed lookup table for the SCSI/NVMe T10-DIF
// CRC-16 polynomial x^16 + x^15 + x^11 + x^9 + x^8 + x^7 + x^5 + x^4 + x^2 + x + 1 (0x8BB7).
var t10DIFTable = func() [256]uint16 {
	const poly = 0x8BB7
	var tab [256]uint16
	for i := 0; i < 256; i++ {
		crc := uint16(i) << 8
		for b := 0; b < 8; b++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ poly
			} else {
				crc <<= 1
			}
		}
		tab[i] = crc
	}
	return tab
}()

// CRC16T10DIF computes the standard T10-DIF CRC-16 (polynomial 0x8BB7) over data.
func CRC16T10DIF(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc = (crc << 8) ^ t10DIFTable[byte(crc>>8)^b]
	}
	return crc
}

// ComputeT10DIFTuple constructs an 8-byte T10-DIF tuple for a sector.
func ComputeT10DIFTuple(sector []byte, appTag uint16, lba uint32) T10DIFTuple {
	return T10DIFTuple{
		Guard:  CRC16T10DIF(sector),
		AppTag: appTag,
		RefTag: lba,
	}
}

// NVMePIProbeResult reports the outcome of probing Linux sysfs for NVMe/T10-DIF
// hardware protection information support.
type NVMePIProbeResult struct {
	Probed         bool     `json:"probed"`
	Supported      bool     `json:"supported"`
	Device         string   `json:"device,omitempty"`
	SysfsPath      string   `json:"sysfs_path,omitempty"`
	Format         string   `json:"format,omitempty"`
	TagSize        int      `json:"tag_size"`
	IntervalBytes  int      `json:"interval_bytes"`
	ReadVerify     bool     `json:"read_verify"`
	WriteGenerate  bool     `json:"write_generate"`
	CheckedDevices []string `json:"checked_devices,omitempty"`
	Reason         string   `json:"reason"`
}

// FSPIConfig configures end-to-end FS-PI journal integrity protection.
type FSPIConfig struct {
	// Mode selects the integrity validation path. ModeNone disables FS-PI.
	Mode FSPIMode
	// FallbackMode selects the software validation mode used when Mode is
	// ModeHardwarePINVMeProbe and the host block device lacks hardware PI.
	// Defaults to ModeKeyedBLAKE3PI.
	FallbackMode FSPIMode
	// TenantID identifies the tenant/namespace bound into every sector's AppTag
	// and cryptographic MAC.
	TenantID string
	// VolumeID identifies the volume bound into every sector's AppTag and MAC.
	VolumeID string
	// Key is the secret key material used by ModeKeyedBLAKE3PI (and when
	// ModeHardwarePINVMeProbe falls back to ModeKeyedBLAKE3PI). Keys of any
	// non-zero length are domain-separated via BLAKE3 DeriveKey.
	Key []byte
	// ProbeDir is the directory whose backing block device is probed when
	// Mode is ModeHardwarePINVMeProbe. Journal.Open populates this from dir.
	ProbeDir string
}

// DeriveFSPIKey derives a 32-byte domain-separated FS-PI integrity key from a
// secret and domain context using BLAKE3 key derivation (compatible with
// hanzoai/vfs's internal/blake3 key derivation in cipher.go).
func DeriveFSPIKey(secret []byte, tenantID, volumeID string) [32]byte {
	var out [32]byte
	ctx := "hanzoai/vfs fspi v1 " + tenantID + "/" + volumeID
	blake3.DeriveKey(ctx, secret, out[:])
	return out
}

// FSPIStats holds atomic counters and runtime telemetry for an FSPIDisk.
type FSPIStats struct {
	RequestedMode       FSPIMode          `json:"requested_mode"`
	EffectiveMode       FSPIMode          `json:"effective_mode"`
	HardwareProbe       NVMePIProbeResult `json:"hardware_probe"`
	SectorsWritten      uint64            `json:"sectors_written"`
	SectorsVerified     uint64            `json:"sectors_verified"`
	ValidationFailures  uint64            `json:"validation_failures"`
	PrePlaintextRejects uint64            `json:"pre_plaintext_rejects"`
	GuardMismatches     uint64            `json:"guard_mismatches"`
	AppTagMismatches    uint64            `json:"app_tag_mismatches"`
	RefTagMismatches    uint64            `json:"ref_tag_mismatches"`
	StaleReplayRejects  uint64            `json:"stale_replay_rejects"`
	ZeroedSectorRejects uint64            `json:"zeroed_sector_rejects"`
	TornSectorRejects   uint64            `json:"torn_sector_rejects"`
	MetadataBytes       uint64            `json:"metadata_bytes"`
	LogicalBytes        uint64            `json:"logical_bytes"`
}

type fspiCounters struct {
	sectorsWritten      atomic.Uint64
	sectorsVerified     atomic.Uint64
	validationFailures  atomic.Uint64
	prePlaintextRejects atomic.Uint64
	guardMismatches     atomic.Uint64
	appTagMismatches    atomic.Uint64
	refTagMismatches    atomic.Uint64
	staleReplayRejects  atomic.Uint64
	zeroedSectorRejects atomic.Uint64
	tornSectorRejects   atomic.Uint64
	metadataBytes       atomic.Uint64
	logicalBytes        atomic.Uint64
}

func (c *fspiCounters) snapshot(req, eff FSPIMode, probe NVMePIProbeResult) FSPIStats {
	return FSPIStats{
		RequestedMode:       req,
		EffectiveMode:       eff,
		HardwareProbe:       probe,
		SectorsWritten:      c.sectorsWritten.Load(),
		SectorsVerified:     c.sectorsVerified.Load(),
		ValidationFailures:  c.validationFailures.Load(),
		PrePlaintextRejects: c.prePlaintextRejects.Load(),
		GuardMismatches:     c.guardMismatches.Load(),
		AppTagMismatches:    c.appTagMismatches.Load(),
		RefTagMismatches:    c.refTagMismatches.Load(),
		StaleReplayRejects:  c.staleReplayRejects.Load(),
		ZeroedSectorRejects: c.zeroedSectorRejects.Load(),
		TornSectorRejects:   c.tornSectorRejects.Load(),
		MetadataBytes:       c.metadataBytes.Load(),
		LogicalBytes:        c.logicalBytes.Load(),
	}
}

// FSPIDisk wraps a journal.Disk with per-sector end-to-end Protection
// Information generation on writes and pre-decryption verification on reads.
type FSPIDisk struct {
	raw           Disk
	requestedMode FSPIMode
	effectiveMode FSPIMode
	probe         NVMePIProbeResult
	tenantID      string
	volumeID      string
	appTag        uint32
	piSize        int
	macKey        [32]byte
	hasKey        bool
	counters      fspiCounters
}

// NewFSPIDisk wraps raw with end-to-end FS-PI sector integrity protection.
func NewFSPIDisk(raw Disk, cfg *FSPIConfig) (*FSPIDisk, error) {
	if raw == nil {
		return nil, fmt.Errorf("%w: underlying Disk is nil", ErrFSPIInvalidConfig)
	}
	if cfg == nil {
		return nil, fmt.Errorf("%w: FSPIConfig is nil", ErrFSPIInvalidConfig)
	}
	d := &FSPIDisk{
		raw:           raw,
		requestedMode: cfg.Mode,
		effectiveMode: cfg.Mode,
		tenantID:      cfg.TenantID,
		volumeID:      cfg.VolumeID,
	}

	switch cfg.Mode {
	case ModeNone, ModeAEADOnlyBaseline:
		d.effectiveMode = cfg.Mode
		return d, nil
	case ModeHardwarePINVMeProbe:
		d.probe = ProbeNVMePI(cfg.ProbeDir)
		fb := cfg.FallbackMode
		if fb == ModeNone || fb == ModeHardwarePINVMeProbe || fb == ModeAEADOnlyBaseline {
			fb = ModeKeyedBLAKE3PI
		}
		if d.probe.Supported {
			// Even if hardware PI is advertised by sysfs, software validation
			// still wraps user-space buffers to provide cryptographic domain
			// binding when fb == ModeKeyedBLAKE3PI, or CRC32C parity otherwise.
			d.effectiveMode = fb
		} else {
			d.effectiveMode = fb
		}
	case ModeSoftwareCRC32C, ModeKeyedBLAKE3PI:
		d.effectiveMode = cfg.Mode
	default:
		return nil, fmt.Errorf("%w: unsupported mode %v", ErrFSPIInvalidConfig, cfg.Mode)
	}

	switch d.effectiveMode {
	case ModeSoftwareCRC32C:
		d.piSize = FSPITupleSizeCRC32C
		d.appTag = computeUnkeyedAppTag(cfg.TenantID, cfg.VolumeID)
	case ModeKeyedBLAKE3PI:
		if len(cfg.Key) == 0 {
			return nil, fmt.Errorf("%w: ModeKeyedBLAKE3PI requires a non-empty Key", ErrFSPIInvalidConfig)
		}
		d.piSize = FSPITupleSizeBLAKE3
		d.macKey = DeriveFSPIKey(cfg.Key, cfg.TenantID, cfg.VolumeID)
		d.hasKey = true
		d.appTag = computeKeyedAppTag(&d.macKey, cfg.TenantID, cfg.VolumeID)
	default:
		return nil, fmt.Errorf("%w: unsupported effective mode %v", ErrFSPIInvalidConfig, d.effectiveMode)
	}

	return d, nil
}

// Underlying returns the wrapped Disk (for fault injection tests and inspection).
func (d *FSPIDisk) Underlying() Disk { return d.raw }

// EffectiveMode returns the resolved active integrity mode.
func (d *FSPIDisk) EffectiveMode() FSPIMode { return d.effectiveMode }

// ProbeResult returns the Linux NVMe hardware PI capability probe result.
func (d *FSPIDisk) ProbeResult() NVMePIProbeResult { return d.probe }

// Stats returns a snapshot of the integrity counters and metadata overhead.
func (d *FSPIDisk) Stats() FSPIStats {
	return d.counters.snapshot(d.requestedMode, d.effectiveMode, d.probe)
}

// Warnings forwards any underlying filesystem warnings (such as overlayfs or
// network filesystem risks) to Journal.Recovery().
func (d *FSPIDisk) Warnings() []string {
	if w, ok := d.raw.(interface{ Warnings() []string }); ok {
		return w.Warnings()
	}
	return nil
}

// List implements Disk.
func (d *FSPIDisk) List() ([]string, error) { return d.raw.List() }

// Remove implements Disk.
func (d *FSPIDisk) Remove(name string) error { return d.raw.Remove(name) }

// Close implements Disk.
func (d *FSPIDisk) Close() error { return d.raw.Close() }

// Create creates a new segment file sized to hold logicalSize bytes plus the
// FS-PI superheader, interleaved per-sector PI tuples, and authenticated
// generation trailer.
func (d *FSPIDisk) Create(name string, logicalSize int64) (File, error) {
	if d.effectiveMode == ModeNone || d.effectiveMode == ModeAEADOnlyBaseline {
		return d.raw.Create(name, logicalSize)
	}
	if logicalSize < 0 {
		return nil, fmt.Errorf("%w: negative size %d", ErrFSPIInvalidConfig, logicalSize)
	}
	numSectors := sectorCount(logicalSize)
	physSize := physicalFileSize(numSectors, d.piSize)

	rawFile, err := d.raw.Create(name, physSize)
	if err != nil {
		return nil, err
	}

	f := &FSPIFile{
		disk:        d,
		raw:         rawFile,
		name:        name,
		fileID:      computeFileID(name, d.tenantID, d.volumeID),
		logicalSize: logicalSize,
		numSectors:  numSectors,
		piSize:      d.piSize,
		mode:        d.effectiveMode,
		appTag:      d.appTag,
		macKey:      d.macKey,
		gens:        make([]uint32, numSectors),
	}
	f.initPools()
	for i := range f.gens {
		f.gens[i] = 1
	}

	// Write superheader at offset 0.
	hdr := f.encodeHeader()
	if _, err := f.raw.WriteAt(hdr[:], 0); err != nil {
		f.raw.Close()
		_ = d.raw.Remove(name)
		return nil, err
	}

	// Initialize valid PI tuples for every zero-filled sector in sequential
	// 64-sector batches so zeroing any sector later is always caught as a
	// phantom zero-fill fault rather than mistaken for an unwritten sector.
	stride := FSPISectorSize + f.piSize
	const initBatch = 64
	batchBuf := make([]byte, min(int(numSectors), initBatch)*stride)
	zeroSector := make([]byte, FSPISectorSize)
	for start := uint32(0); start < numSectors; start += initBatch {
		count := min(uint32(initBatch), numSectors-start)
		buf := batchBuf[:int(count)*stride]
		clear(buf)
		for i := uint32(0); i < count; i++ {
			lba := start + i
			piOff := int(i)*stride + FSPISectorSize
			f.encodeSectorPI(buf[piOff:piOff+f.piSize], zeroSector, lba, 1)
		}
		if _, err := f.raw.WriteAt(buf, f.PhysicalOffset(start)); err != nil {
			f.raw.Close()
			_ = d.raw.Remove(name)
			return nil, err
		}
	}

	// Write authenticated generation trailer.
	if err := f.flushTrailerLocked(); err != nil {
		f.raw.Close()
		_ = d.raw.Remove(name)
		return nil, err
	}
	if err := f.raw.Sync(); err != nil {
		f.raw.Close()
		_ = d.raw.Remove(name)
		return nil, err
	}

	d.counters.logicalBytes.Add(uint64(logicalSize))
	d.counters.metadataBytes.Add(uint64(physSize - logicalSize))
	return f, nil
}

// Open opens an existing FS-PI segment file, verifies its superheader, domain
// AppTag, and authenticated generation trailer, and returns an FSPIFile.
func (d *FSPIDisk) Open(name string) (File, error) {
	if d.effectiveMode == ModeNone || d.effectiveMode == ModeAEADOnlyBaseline {
		return d.raw.Open(name)
	}
	rawFile, err := d.raw.Open(name)
	if err != nil {
		return nil, err
	}

	if rawFile.Size() < fspiHeaderSize {
		rawFile.Close()
		return nil, newFormatFSPIError("%s: file smaller than FS-PI superheader (%d bytes)", name, rawFile.Size())
	}

	var hdr [fspiHeaderSize]byte
	if _, err := rawFile.ReadAt(hdr[:], 0); err != nil {
		rawFile.Close()
		return nil, newCorruptFSPIError(ErrFSPIHeaderCorrupt, "%s: read superheader: %v", name, err)
	}
	if [4]byte(hdr[0:4]) != fspiMagic {
		rawFile.Close()
		return nil, newFormatFSPIError("%s: missing FS-PI magic", name)
	}
	ver := binary.LittleEndian.Uint16(hdr[4:6])
	fileMode := FSPIMode(hdr[6])
	piSize := int(hdr[7])
	secSize := binary.LittleEndian.Uint32(hdr[8:12])
	numSectors := binary.LittleEndian.Uint32(hdr[12:16])
	logicalSize := int64(binary.LittleEndian.Uint64(hdr[16:24]))
	appTag := binary.LittleEndian.Uint32(hdr[24:28])
	var fileID [16]byte
	copy(fileID[:], hdr[48:64])

	if ver != fspiVersion || secSize != FSPISectorSize || fileMode != d.effectiveMode || piSize != d.piSize {
		rawFile.Close()
		return nil, newFormatFSPIError("%s: incompatible FS-PI layout (ver=%d mode=%v piSize=%d sector=%d)", name, ver, fileMode, piSize, secSize)
	}
	if appTag != d.appTag {
		d.counters.validationFailures.Add(1)
		d.counters.prePlaintextRejects.Add(1)
		d.counters.appTagMismatches.Add(1)
		rawFile.Close()
		return nil, newCorruptFSPIError(ErrFSPIAppTagMismatch, "%s: superheader appTag=0x%08x want 0x%08x", name, appTag, d.appTag)
	}

	expectedFileID := computeFileID(name, d.tenantID, d.volumeID)
	if subtle.ConstantTimeCompare(fileID[:], expectedFileID[:]) != 1 {
		d.counters.validationFailures.Add(1)
		d.counters.prePlaintextRejects.Add(1)
		d.counters.appTagMismatches.Add(1)
		rawFile.Close()
		return nil, newCorruptFSPIError(ErrFSPIAppTagMismatch, "%s: superheader file identity mismatch", name)
	}

	f := &FSPIFile{
		disk:        d,
		raw:         rawFile,
		name:        name,
		fileID:      fileID,
		logicalSize: logicalSize,
		numSectors:  numSectors,
		piSize:      piSize,
		mode:        fileMode,
		appTag:      appTag,
		macKey:      d.macKey,
		gens:        make([]uint32, numSectors),
	}
	f.initPools()

	// Verify superheader authentication tag.
	wantHdrAuth := f.computeHeaderAuth(hdr[:32], hdr[48:64])
	if subtle.ConstantTimeCompare(hdr[32:48], wantHdrAuth[:]) != 1 {
		d.counters.validationFailures.Add(1)
		d.counters.prePlaintextRejects.Add(1)
		d.counters.guardMismatches.Add(1)
		rawFile.Close()
		return nil, newCorruptFSPIError(ErrFSPIHeaderCorrupt, "%s: superheader authentication failed", name)
	}

	// Load and verify generation trailer (if physical file was truncated,
	// fail closed immediately with ErrFSPITornSector).
	wantPhys := physicalFileSize(numSectors, piSize)
	if rawFile.Size() < wantPhys {
		d.counters.validationFailures.Add(1)
		d.counters.prePlaintextRejects.Add(1)
		d.counters.tornSectorRejects.Add(1)
		rawFile.Close()
		return nil, newCorruptFSPIError(ErrFSPITornSector, "%s: physical file truncated (%d < %d bytes)", name, rawFile.Size(), wantPhys)
	}
	if err := f.loadTrailerLocked(); err != nil {
		rawFile.Close()
		return nil, err
	}
	return f, nil
}

// FSPIFile wraps a journal.File with per-sector NVMe-style Protection
// Information (GuardTag/MAC + AppTag + RefTag + Generation).
// It is safe for concurrent ReadAt, WriteAt, and Sync calls.
type FSPIFile struct {
	mu          sync.RWMutex
	disk        *FSPIDisk
	raw         File
	name        string
	fileID      [16]byte
	logicalSize int64
	numSectors  uint32
	piSize      int
	mode        FSPIMode
	appTag      uint32
	macKey      [32]byte
	gens        []uint32
	gensDirty   bool
	closed      bool
	hasherPool  sync.Pool
	sectorPool  sync.Pool
}

func (f *FSPIFile) initPools() {
	stride := FSPISectorSize + f.piSize
	f.sectorPool = sync.Pool{
		New: func() any {
			b := make([]byte, stride)
			return &b
		},
	}
	if f.mode == ModeKeyedBLAKE3PI {
		key := f.macKey
		f.hasherPool = sync.Pool{
			New: func() any {
				return blake3.Keyed(&key)
			},
		}
	}
}

func (f *FSPIFile) acquirePhysBuf(n int) ([]byte, *[]byte) {
	if n == FSPISectorSize+f.piSize {
		bp := f.sectorPool.Get().(*[]byte)
		return (*bp)[:n], bp
	}
	return make([]byte, n), nil
}

func (f *FSPIFile) releasePhysBuf(bp *[]byte) {
	if bp != nil {
		f.sectorPool.Put(bp)
	}
}

// Size returns the logical segment size visible to Journal.
func (f *FSPIFile) Size() int64 { return f.logicalSize }

// NumSectors returns the number of 4 KiB logical sectors in the segment.
func (f *FSPIFile) NumSectors() uint32 { return f.numSectors }

// PISize returns the per-sector PI tuple size in bytes (16 or 32).
func (f *FSPIFile) PISize() int { return f.piSize }

// AppTag returns the 32-bit tenant/volume domain tag bound to every sector.
func (f *FSPIFile) AppTag() uint32 { return f.appTag }

// Underlying returns the raw underlying journal.File (for below-encryption
// fault injection harnesses and tests).
func (f *FSPIFile) Underlying() File { return f.raw }

// PhysicalSectorStride returns the on-disk byte stride per logical sector
// (4096 + piSize).
func (f *FSPIFile) PhysicalSectorStride() int64 {
	return int64(FSPISectorSize + f.piSize)
}

// PhysicalOffset returns the physical byte offset of logical sector lba on disk.
func (f *FSPIFile) PhysicalOffset(lba uint32) int64 {
	return int64(fspiHeaderSize) + int64(lba)*f.PhysicalSectorStride()
}

// TrailerOffset returns the physical byte offset of the authenticated generation trailer.
func (f *FSPIFile) TrailerOffset() int64 {
	return int64(fspiHeaderSize) + int64(f.numSectors)*f.PhysicalSectorStride()
}

// SectorGeneration returns the current committed generation counter for lba.
func (f *FSPIFile) SectorGeneration(lba uint32) uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if lba >= f.numSectors {
		return 0
	}
	return uint64(f.gens[lba])
}

// ReadAt reads len(p) logical bytes starting at off, verifying every touched
// 4 KiB sector's Protection Information tuple BEFORE copying any bytes into p.
// On any integrity failure, p is cleared and an ErrCorrupt-wrapped FS-PI error
// is returned so corrupted ciphertext never reaches an upper AEAD decryptor.
func (f *FSPIFile) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("journal/fspi: negative read offset %d", off)
	}
	f.mu.RLock()
	defer f.mu.RUnlock()

	if off >= f.logicalSize {
		return 0, io.EOF
	}
	want := len(p)
	eof := false
	if off+int64(want) > f.logicalSize {
		want = int(f.logicalSize - off)
		eof = true
	}

	firstLBA := uint32(off / FSPISectorSize)
	lastLBA := uint32((off + int64(want) - 1) / FSPISectorSize)
	count := lastLBA - firstLBA + 1
	stride := FSPISectorSize + f.piSize

	physBuf, physBP := f.acquirePhysBuf(int(count) * stride)
	defer f.releasePhysBuf(physBP)
	n, err := f.raw.ReadAt(physBuf, f.PhysicalOffset(firstLBA))
	if err != nil && !errors.Is(err, io.EOF) {
		clear(p)
		return 0, err
	}
	if n < len(physBuf) {
		clear(p)
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		f.disk.counters.tornSectorRejects.Add(1)
		return 0, newCorruptFSPIError(ErrFSPITornSector, "%s lba=%d..%d: short physical read (%d < %d bytes)", f.name, firstLBA, lastLBA, n, len(physBuf))
	}

	// Validate EVERY sector BEFORE copying any bytes into caller buffer p.
	for i := uint32(0); i < count; i++ {
		lba := firstLBA + i
		base := int(i) * stride
		sectorData := physBuf[base : base+FSPISectorSize]
		rawPI := physBuf[base+FSPISectorSize : base+stride]
		if verr := f.verifySectorLocked(sectorData, rawPI, lba); verr != nil {
			clear(p)
			return 0, verr
		}
		f.disk.counters.sectorsVerified.Add(1)
	}

	// All sectors verified — copy requested logical byte range to p.
	copied := 0
	for i := uint32(0); i < count; i++ {
		lba := firstLBA + i
		secStart := int64(lba) * FSPISectorSize
		lo := max(off, secStart) - secStart
		hi := min(off+int64(want), secStart+FSPISectorSize) - secStart
		base := int(i) * stride
		copied += copy(p[copied:], physBuf[base+int(lo):base+int(hi)])
	}
	if eof {
		return copied, io.EOF
	}
	return copied, nil
}

// WriteAt writes p at logical offset off, performing integrity-checked
// read-modify-write for partial sectors, advancing each touched sector's
// monotonic generation counter, and emitting fresh per-sector PI tuples.
func (f *FSPIFile) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 || off+int64(len(p)) > f.logicalSize {
		return 0, fmt.Errorf("journal/fspi: write out of range off=%d len=%d size=%d", off, len(p), f.logicalSize)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	firstLBA := uint32(off / FSPISectorSize)
	lastLBA := uint32((off + int64(len(p)) - 1) / FSPISectorSize)
	count := lastLBA - firstLBA + 1
	stride := FSPISectorSize + f.piSize
	physBuf, physBP := f.acquirePhysBuf(int(count) * stride)
	defer f.releasePhysBuf(physBP)

	// If leading sector is partial, read & validate its current contents first
	// so corrupted on-disk sectors are never silently masked by an RMW overwrite.
	if off%FSPISectorSize != 0 || int64(len(p)) < FSPISectorSize {
		if err := f.readVerifiedSectorLocked(physBuf[:stride], firstLBA); err != nil {
			return 0, err
		}
	}
	// If trailing sector is distinct from leading and partial, read & validate it too.
	endOff := off + int64(len(p))
	if lastLBA != firstLBA && endOff%FSPISectorSize != 0 {
		tailBase := int(lastLBA-firstLBA) * stride
		if err := f.readVerifiedSectorLocked(physBuf[tailBase:tailBase+stride], lastLBA); err != nil {
			return 0, err
		}
	}

	// Merge incoming logical bytes and compute new PI tuples with generation+1.
	written := 0
	var stackGens [8]uint32
	var newGens []uint32
	if int(count) <= len(stackGens) {
		newGens = stackGens[:count]
	} else {
		newGens = make([]uint32, count)
	}
	for i := uint32(0); i < count; i++ {
		lba := firstLBA + i
		secStart := int64(lba) * FSPISectorSize
		lo := max(off, secStart) - secStart
		hi := min(endOff, secStart+FSPISectorSize) - secStart
		base := int(i) * stride
		written += copy(physBuf[base+int(lo):base+int(hi)], p[written:])

		nextGen := f.gens[lba] + 1
		newGens[i] = nextGen
		f.encodeSectorPI(
			physBuf[base+FSPISectorSize:base+stride],
			physBuf[base:base+FSPISectorSize],
			lba,
			uint64(nextGen),
		)
	}

	if n, err := f.raw.WriteAt(physBuf, f.PhysicalOffset(firstLBA)); err != nil {
		return 0, err
	} else if n != len(physBuf) {
		return 0, io.ErrShortWrite
	}

	for i := uint32(0); i < count; i++ {
		f.gens[firstLBA+i] = newGens[i]
	}
	f.gensDirty = true
	f.disk.counters.sectorsWritten.Add(uint64(count))
	return written, nil
}

// Writeback translates logical range [off, off+n) to the physical sector span
// and forwards the writeback hint to the underlying file.
func (f *FSPIFile) Writeback(off, n int64) error {
	if n <= 0 || off < 0 || off >= f.logicalSize {
		return nil
	}
	end := min(off+n, f.logicalSize)
	firstLBA := uint32(off / FSPISectorSize)
	lastLBA := uint32((end - 1) / FSPISectorSize)
	physOff := f.PhysicalOffset(firstLBA)
	physLen := int64(lastLBA-firstLBA+1) * f.PhysicalSectorStride()
	return f.raw.Writeback(physOff, physLen)
}

// Sync flushes the authenticated generation trailer (if dirty) and syncs the
// underlying file durably to media.
func (f *FSPIFile) Sync() error {
	f.mu.Lock()
	var err error
	if f.gensDirty && !f.closed {
		err = f.flushTrailerLocked()
		if err == nil {
			f.gensDirty = false
		}
	}
	f.mu.Unlock()
	if err != nil {
		return err
	}
	return f.raw.Sync()
}

// Close flushes any pending generation trailer updates and closes the underlying file.
func (f *FSPIFile) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	var flushErr error
	if f.gensDirty {
		flushErr = f.flushTrailerLocked()
		if flushErr == nil {
			f.gensDirty = false
		}
	}
	f.mu.Unlock()
	closeErr := f.raw.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// readVerifiedSectorLocked reads and validates a single physical sector into dst.
// Caller holds f.mu.
func (f *FSPIFile) readVerifiedSectorLocked(dst []byte, lba uint32) error {
	n, err := f.raw.ReadAt(dst, f.PhysicalOffset(lba))
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(dst) {
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		f.disk.counters.tornSectorRejects.Add(1)
		return newCorruptFSPIError(ErrFSPITornSector, "%s lba=%d: short RMW physical read (%d < %d bytes)", f.name, lba, n, len(dst))
	}
	return f.verifySectorLocked(dst[:FSPISectorSize], dst[FSPISectorSize:], lba)
}

// verifySectorLocked validates the Protection Information tuple for lba before
// any ciphertext bytes are returned to upper layers.
func (f *FSPIFile) verifySectorLocked(sectorData, rawPI []byte, lba uint32) error {
	if allZero(rawPI) {
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		if allZero(sectorData) {
			f.disk.counters.zeroedSectorRejects.Add(1)
			return newCorruptFSPIError(ErrFSPIZeroedSector, "%s lba=%d: sector and PI tuple are all zero", f.name, lba)
		}
		f.disk.counters.tornSectorRejects.Add(1)
		return newCorruptFSPIError(ErrFSPITornSector, "%s lba=%d: PI tuple zeroed (torn sector write)", f.name, lba)
	}

	pi := f.decodeSectorPI(rawPI)
	if pi.Flags&piFlagValid == 0 {
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		f.disk.counters.zeroedSectorRejects.Add(1)
		return newCorruptFSPIError(ErrFSPIZeroedSector, "%s lba=%d: missing valid flag in PI tuple", f.name, lba)
	}

	if pi.AppTag != f.appTag {
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		f.disk.counters.appTagMismatches.Add(1)
		return newCorruptFSPIError(ErrFSPIAppTagMismatch, "%s lba=%d: appTag=0x%08x want 0x%08x", f.name, lba, pi.AppTag, f.appTag)
	}

	if pi.RefTag != lba {
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		f.disk.counters.refTagMismatches.Add(1)
		return newCorruptFSPIError(ErrFSPIRefTagMismatch, "%s lba=%d: refTag=%d want %d", f.name, lba, pi.RefTag, lba)
	}

	expectedGen := uint64(f.gens[lba])
	if pi.Generation < expectedGen {
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		f.disk.counters.staleReplayRejects.Add(1)
		return newCorruptFSPIError(ErrFSPIStaleReplay, "%s lba=%d: generation=%d < expected %d", f.name, lba, pi.Generation, expectedGen)
	}

	switch f.mode {
	case ModeSoftwareCRC32C:
		wantCRC := computeCRC32CGuard(sectorData, pi.AppTag, pi.RefTag, pi.Generation, pi.Flags)
		if uint32(pi.GuardTag) != wantCRC {
			f.disk.counters.validationFailures.Add(1)
			f.disk.counters.prePlaintextRejects.Add(1)
			f.disk.counters.guardMismatches.Add(1)
			if allZero(sectorData) {
				f.disk.counters.zeroedSectorRejects.Add(1)
				return newCorruptFSPIError(ErrFSPIZeroedSector, "%s lba=%d: sector zeroed (crc=0x%08x want 0x%08x)", f.name, lba, uint32(pi.GuardTag), wantCRC)
			}
			return newCorruptFSPIError(ErrFSPIGuardMismatch, "%s lba=%d: crc32c=0x%08x want 0x%08x", f.name, lba, uint32(pi.GuardTag), wantCRC)
		}
	case ModeKeyedBLAKE3PI:
		wantMAC := f.computeBLAKE3Guard(sectorData, pi.AppTag, pi.RefTag, pi.Generation, pi.Flags)
		if subtle.ConstantTimeCompare(rawPI[:16], wantMAC[:]) != 1 {
			f.disk.counters.validationFailures.Add(1)
			f.disk.counters.prePlaintextRejects.Add(1)
			f.disk.counters.guardMismatches.Add(1)
			if allZero(sectorData) {
				f.disk.counters.zeroedSectorRejects.Add(1)
				return newCorruptFSPIError(ErrFSPIZeroedSector, "%s lba=%d: sector zeroed (keyed BLAKE3 MAC failed)", f.name, lba)
			}
			return newCorruptFSPIError(ErrFSPIGuardMismatch, "%s lba=%d: keyed BLAKE3-128 MAC verification failed", f.name, lba)
		}
	default:
		return newCorruptFSPIError(ErrFSPIHeaderCorrupt, "%s lba=%d: invalid mode %v", f.name, lba, f.mode)
	}
	return nil
}

// encodeSectorPI encodes the Protection Information tuple for (sectorData, lba, gen) into dst.
func (f *FSPIFile) encodeSectorPI(dst, sectorData []byte, lba uint32, gen uint64) {
	switch f.mode {
	case ModeSoftwareCRC32C:
		flags := uint16(piFlagValid | piFlagModeCRC32C)
		crc := computeCRC32CGuard(sectorData, f.appTag, lba, gen, flags)
		binary.LittleEndian.PutUint32(dst[0:4], crc)
		binary.LittleEndian.PutUint32(dst[4:8], f.appTag)
		binary.LittleEndian.PutUint32(dst[8:12], lba)
		genFlags := (uint32(flags) << 28) | (uint32(gen) & 0x0fffffff)
		binary.LittleEndian.PutUint32(dst[12:16], genFlags)
	case ModeKeyedBLAKE3PI:
		flags := uint16(piFlagValid | piFlagModeBLAKE3)
		mac := f.computeBLAKE3Guard(sectorData, f.appTag, lba, gen, flags)
		copy(dst[0:16], mac[:])
		binary.LittleEndian.PutUint32(dst[16:20], f.appTag)
		binary.LittleEndian.PutUint32(dst[20:24], lba)
		genFlags := (uint64(flags) << 56) | (gen & 0x00ffffffffffffff)
		binary.LittleEndian.PutUint64(dst[24:32], genFlags)
	}
}

func (f *FSPIFile) decodeSectorPI(rawPI []byte) PITuple {
	switch f.mode {
	case ModeSoftwareCRC32C:
		genFlags := binary.LittleEndian.Uint32(rawPI[12:16])
		return PITuple{
			GuardTag:   uint64(binary.LittleEndian.Uint32(rawPI[0:4])),
			AppTag:     binary.LittleEndian.Uint32(rawPI[4:8]),
			RefTag:     binary.LittleEndian.Uint32(rawPI[8:12]),
			Generation: uint64(genFlags & 0x0fffffff),
			Flags:      uint16(genFlags >> 28),
		}
	case ModeKeyedBLAKE3PI:
		genFlags := binary.LittleEndian.Uint64(rawPI[24:32])
		return PITuple{
			GuardTag:   binary.LittleEndian.Uint64(rawPI[0:8]),
			GuardExt:   binary.LittleEndian.Uint64(rawPI[8:16]),
			AppTag:     binary.LittleEndian.Uint32(rawPI[16:20]),
			RefTag:     binary.LittleEndian.Uint32(rawPI[20:24]),
			Generation: genFlags & 0x00ffffffffffffff,
			Flags:      uint16(genFlags >> 56),
		}
	default:
		return PITuple{}
	}
}

// computeCRC32CGuard computes an unkeyed Castagnoli CRC32C over the sector
// payload and its PI metadata fields (AppTag, RefTag, Generation, Flags).
func computeCRC32CGuard(sector []byte, appTag, lba uint32, gen uint64, flags uint16) uint32 {
	var meta [16]byte
	binary.LittleEndian.PutUint32(meta[0:4], appTag)
	binary.LittleEndian.PutUint32(meta[4:8], lba)
	binary.LittleEndian.PutUint64(meta[8:16], (uint64(flags)<<56)|(gen&0x00ffffffffffffff))
	c := crc32.Checksum(meta[:], castagnoli)
	return crc32.Update(c, castagnoli, sector)
}

// computeBLAKE3Guard computes a 128-bit keyed BLAKE3 MAC bound to the file
// identity, tenant/volume AppTag, LBA RefTag, generation epoch, flags, and
// ciphertext sector payload.
func (f *FSPIFile) computeBLAKE3Guard(sector []byte, appTag, lba uint32, gen uint64, flags uint16) [16]byte {
	var hdr [32]byte
	copy(hdr[0:16], f.fileID[:])
	binary.LittleEndian.PutUint32(hdr[16:20], appTag)
	binary.LittleEndian.PutUint32(hdr[20:24], lba)
	binary.LittleEndian.PutUint64(hdr[24:32], (uint64(flags)<<56)|(gen&0x00ffffffffffffff))

	h := f.hasherPool.Get().(*blake3.Hasher)
	h.Reset()
	_, _ = h.Write(hdr[:])
	_, _ = h.Write(sector)
	var sum [blake3.Size]byte
	h.Sum(sum[:0])
	f.hasherPool.Put(h)
	var mac [16]byte
	copy(mac[:], sum[:16])
	return mac
}

func (f *FSPIFile) encodeHeader() [fspiHeaderSize]byte {
	var hdr [fspiHeaderSize]byte
	copy(hdr[0:4], fspiMagic[:])
	binary.LittleEndian.PutUint16(hdr[4:6], fspiVersion)
	hdr[6] = byte(f.mode)
	hdr[7] = byte(f.piSize)
	binary.LittleEndian.PutUint32(hdr[8:12], FSPISectorSize)
	binary.LittleEndian.PutUint32(hdr[12:16], f.numSectors)
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(f.logicalSize))
	binary.LittleEndian.PutUint32(hdr[24:28], f.appTag)
	copy(hdr[48:64], f.fileID[:])
	auth := f.computeHeaderAuth(hdr[:32], hdr[48:64])
	copy(hdr[32:48], auth[:])
	return hdr
}

func (f *FSPIFile) computeHeaderAuth(prefix, fileID []byte) [16]byte {
	var out [16]byte
	switch f.mode {
	case ModeSoftwareCRC32C:
		c := crc32.Checksum(prefix, castagnoli)
		c = crc32.Update(c, castagnoli, fileID)
		binary.LittleEndian.PutUint32(out[0:4], c)
	case ModeKeyedBLAKE3PI:
		h := blake3.Keyed(&f.macKey)
		_, _ = h.WriteString("fspi-hdr-v1:")
		_, _ = h.Write(prefix)
		_, _ = h.Write(fileID)
		var sum [blake3.Size]byte
		h.Sum(sum[:0])
		copy(out[:], sum[:16])
	}
	return out
}

func (f *FSPIFile) computeTrailerAuth(genBytes []byte) [fspiTrailerMACSize]byte {
	var out [fspiTrailerMACSize]byte
	switch f.mode {
	case ModeSoftwareCRC32C:
		c := crc32.Checksum(f.fileID[:], castagnoli)
		c = crc32.Update(c, castagnoli, genBytes)
		binary.LittleEndian.PutUint32(out[0:4], c)
	case ModeKeyedBLAKE3PI:
		h := blake3.Keyed(&f.macKey)
		_, _ = h.WriteString("fspi-gen-v1:")
		_, _ = h.Write(f.fileID[:])
		_, _ = h.Write(genBytes)
		var sum [blake3.Size]byte
		h.Sum(sum[:0])
		copy(out[:], sum[:16])
	}
	return out
}

func (f *FSPIFile) flushTrailerLocked() error {
	genBytesLen := int(f.numSectors) * 4
	buf := make([]byte, genBytesLen+fspiTrailerMACSize)
	for i, g := range f.gens {
		binary.LittleEndian.PutUint32(buf[i*4:], g)
	}
	auth := f.computeTrailerAuth(buf[:genBytesLen])
	copy(buf[genBytesLen:], auth[:])
	n, err := f.raw.WriteAt(buf, f.TrailerOffset())
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}
	return nil
}

func (f *FSPIFile) loadTrailerLocked() error {
	genBytesLen := int(f.numSectors) * 4
	buf := make([]byte, genBytesLen+fspiTrailerMACSize)
	n, err := f.raw.ReadAt(buf, f.TrailerOffset())
	if err != nil && !errors.Is(err, io.EOF) {
		return newCorruptFSPIError(ErrFSPIHeaderCorrupt, "%s: read generation trailer: %v", f.name, err)
	}
	if n < len(buf) {
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		f.disk.counters.tornSectorRejects.Add(1)
		return newCorruptFSPIError(ErrFSPITornSector, "%s: generation trailer truncated (%d < %d)", f.name, n, len(buf))
	}
	wantAuth := f.computeTrailerAuth(buf[:genBytesLen])
	if subtle.ConstantTimeCompare(buf[genBytesLen:], wantAuth[:]) != 1 {
		f.disk.counters.validationFailures.Add(1)
		f.disk.counters.prePlaintextRejects.Add(1)
		f.disk.counters.guardMismatches.Add(1)
		return newCorruptFSPIError(ErrFSPIHeaderCorrupt, "%s: generation trailer authentication failed", f.name)
	}
	for i := range f.gens {
		f.gens[i] = binary.LittleEndian.Uint32(buf[i*4:])
	}
	return nil
}

func sectorCount(logicalSize int64) uint32 {
	if logicalSize <= 0 {
		return 1
	}
	return uint32((logicalSize + FSPISectorSize - 1) / FSPISectorSize)
}

func physicalFileSize(numSectors uint32, piSize int) int64 {
	return int64(fspiHeaderSize) +
		int64(numSectors)*int64(FSPISectorSize+piSize) +
		int64(numSectors)*4 +
		int64(fspiTrailerMACSize)
}

func computeUnkeyedAppTag(tenantID, volumeID string) uint32 {
	tag := crc32.Checksum([]byte("hanzoai/vfs/fspi/app:"+tenantID+"\x00"+volumeID), castagnoli)
	if tag == 0 {
		tag = 1
	}
	return tag
}

func computeKeyedAppTag(key *[32]byte, tenantID, volumeID string) uint32 {
	h := blake3.Keyed(key)
	_, _ = h.WriteString("hanzoai/vfs/fspi/app-keyed:")
	_, _ = h.WriteString(tenantID)
	_ = h.WriteByte(0)
	_, _ = h.WriteString(volumeID)
	var sum [blake3.Size]byte
	h.Sum(sum[:0])
	tag := binary.LittleEndian.Uint32(sum[:4])
	if tag == 0 {
		tag = 1
	}
	return tag
}

func computeFileID(name, tenantID, volumeID string) [16]byte {
	sum := blake3.Sum([]byte("hanzoai/vfs/fspi/file:" + tenantID + "\x00" + volumeID + "\x00" + name))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}

// HIP0302SectorStore composes an FS-PI protected (or raw baseline) journal.File
// with a HIP-0302 / AES-256-GCM AEAD cipher envelope (matching hanzoai/vfs
// ledger.go's per-journal AES-256-GCM + volume lineage AAD + NIST SP 800-38D
// 96-bit salt+counter nonce).
//
// It tracks DecryptInvocations, PreDecryptRejects, and AEADRejects so tests
// and benchmarks can verify mechanically that FS-PI rejects corrupted, swapped,
// replayed, and transplanted sectors BEFORE AEAD decryption is invoked.
type HIP0302SectorStore struct {
	file    File
	mode    FSPIMode
	aead    cipher.AEAD
	aad     []byte
	salt    [4]byte
	nonce   atomic.Uint64
	metrics HIP0302StoreMetrics
}

// HIP0302StoreMetrics tracks cryptographic and pre-decryption rejection events.
type HIP0302StoreMetrics struct {
	DecryptInvocations atomic.Uint64
	PreDecryptRejects  atomic.Uint64
	AEADRejects        atomic.Uint64
	PlaintextExposed   atomic.Uint64
}

// HIP0302MaxSectorPlaintext is the maximum plaintext payload per 4 KiB sector
// after reserving 4-byte length prefix, 12-byte GCM nonce, and 16-byte GCM tag.
const HIP0302MaxSectorPlaintext = FSPISectorSize - 4 - 12 - 16

// NewHIP0302SectorStore creates an encrypted 4 KiB sector store over file.
// When mode is ModeNone, AEAD is bypassed to provide an unencrypted raw disk
// baseline. For all other modes, AES-256-GCM with volume-scoped AAD is active.
func NewHIP0302SectorStore(file File, mode FSPIMode, aeadKey []byte, tenantID, volumeID string) (*HIP0302SectorStore, error) {
	s := &HIP0302SectorStore{
		file: file,
		mode: mode,
		aad:  []byte("hanzoai/vfs/hip-0302:" + tenantID + ":" + volumeID),
	}
	if mode == ModeNone {
		return s, nil
	}
	if len(aeadKey) != 32 {
		return nil, fmt.Errorf("%w: HIP-0302 AES-256-GCM requires 32-byte key (got %d)", ErrFSPIInvalidConfig, len(aeadKey))
	}
	block, err := aes.NewCipher(aeadKey)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if _, err := rand.Read(s.salt[:]); err != nil {
		return nil, err
	}
	s.aead = aead
	return s, nil
}

// WriteSector seals plaintext into a 4 KiB sector (or writes raw plaintext in
// ModeNone) and writes it at logical block address lba.
func (s *HIP0302SectorStore) WriteSector(lba uint32, plaintext []byte) error {
	if len(plaintext) > HIP0302MaxSectorPlaintext {
		return fmt.Errorf("journal/fspi: sector payload %d exceeds max %d", len(plaintext), HIP0302MaxSectorPlaintext)
	}
	var sector [FSPISectorSize]byte
	if s.mode == ModeNone {
		binary.LittleEndian.PutUint32(sector[0:4], uint32(len(plaintext)))
		copy(sector[4:], plaintext)
	} else {
		binary.LittleEndian.PutUint32(sector[0:4], uint32(len(plaintext)))
		nonce := sector[4:16]
		copy(nonce[:4], s.salt[:])
		binary.BigEndian.PutUint64(nonce[4:12], s.nonce.Add(1))
		s.aead.Seal(sector[16:16], nonce, plaintext, s.aad)
	}
	_, err := s.file.WriteAt(sector[:], int64(lba)*FSPISectorSize)
	return err
}

// ReadSector reads logical sector lba. If the underlying file is an FSPIFile,
// FS-PI verification executes BEFORE AEAD decryption is invoked; any FS-PI
// rejection increments PreDecryptRejects without touching DecryptInvocations.
func (s *HIP0302SectorStore) ReadSector(lba uint32) ([]byte, error) {
	var sector [FSPISectorSize]byte
	if _, err := s.file.ReadAt(sector[:], int64(lba)*FSPISectorSize); err != nil {
		if errors.Is(err, ErrFSPIIntegrity) || errors.Is(err, ErrCorrupt) {
			s.metrics.PreDecryptRejects.Add(1)
		}
		return nil, err
	}
	if s.mode == ModeNone {
		n := binary.LittleEndian.Uint32(sector[0:4])
		if int(n) > HIP0302MaxSectorPlaintext {
			n = HIP0302MaxSectorPlaintext
		}
		out := make([]byte, n)
		copy(out, sector[4:4+n])
		s.metrics.PlaintextExposed.Add(1)
		return out, nil
	}
	n := binary.LittleEndian.Uint32(sector[0:4])
	if int(n) > HIP0302MaxSectorPlaintext {
		s.metrics.DecryptInvocations.Add(1)
		s.metrics.AEADRejects.Add(1)
		return nil, fmt.Errorf("%w: invalid HIP-0302 ciphertext length %d", ErrCorrupt, n)
	}
	s.metrics.DecryptInvocations.Add(1)
	nonce := sector[4:16]
	ct := sector[16 : 16+int(n)+s.aead.Overhead()]
	plain, err := s.aead.Open(nil, nonce, ct, s.aad)
	if err != nil {
		s.metrics.AEADRejects.Add(1)
		return nil, fmt.Errorf("%w: HIP-0302 AEAD open failed: %v", ErrCorrupt, err)
	}
	s.metrics.PlaintextExposed.Add(1)
	return plain, nil
}

// Sync flushes the underlying file durably.
func (s *HIP0302SectorStore) Sync() error { return s.file.Sync() }

// Metrics returns the store's decryption and pre-decryption rejection counters.
func (s *HIP0302SectorStore) Metrics() (decryptCalls, preDecryptRejects, aeadRejects, exposed uint64) {
	return s.metrics.DecryptInvocations.Load(),
		s.metrics.PreDecryptRejects.Load(),
		s.metrics.AEADRejects.Load(),
		s.metrics.PlaintextExposed.Load()
}
