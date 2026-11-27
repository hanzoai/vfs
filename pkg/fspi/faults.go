package fspi

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/hanzoai/vfs/journal"
)

// FaultKind enumerates the 8 below-encryption storage corruption classes
// injected directly into underlying journal segment files beneath AEAD.
type FaultKind string

const (
	FaultSingleBitFlip     FaultKind = "single_bit_flip"
	FaultMultiByteBurst    FaultKind = "multi_byte_burst"
	FaultMisdirectedWrite  FaultKind = "misdirected_write"
	FaultTornWrite         FaultKind = "torn_write"
	FaultLostWrite         FaultKind = "lost_write"
	FaultCrossVolumeSwap   FaultKind = "cross_volume_swap"
	FaultMetadataTruncated FaultKind = "metadata_truncated"
	FaultPICorrupt         FaultKind = "pi_tuple_corrupt"
)

// AllFaultKinds lists all 8 below-encryption corruption classes in canonical order.
var AllFaultKinds = []FaultKind{
	FaultSingleBitFlip,
	FaultMultiByteBurst,
	FaultMisdirectedWrite,
	FaultTornWrite,
	FaultLostWrite,
	FaultCrossVolumeSwap,
	FaultMetadataTruncated,
	FaultPICorrupt,
}

// RegimeSpec defines one of the 4 matched evaluation regimes.
type RegimeSpec struct {
	Name       string    `json:"name"`
	Mode       GuardMode `json:"-"`
	ModeName   string    `json:"mode"`
	EnableAEAD bool      `json:"enable_aead"`
	SectorSize int       `json:"sector_size"`
}

// FourRegimes returns the 4 matched evaluation regimes for a given sectorSize.
func FourRegimes(sectorSize int) []RegimeSpec {
	return []RegimeSpec{
		{
			Name:       "unprotected_journal_baseline",
			Mode:       GuardNone,
			ModeName:   GuardNone.String(),
			EnableAEAD: false,
			SectorSize: sectorSize,
		},
		{
			Name:       "aead_only_no_pi",
			Mode:       GuardNone,
			ModeName:   GuardNone.String(),
			EnableAEAD: true,
			SectorSize: sectorSize,
		},
		{
			Name:       "fspi_t10_dif_hw_compat",
			Mode:       GuardT10DIF,
			ModeName:   GuardT10DIF.String(),
			EnableAEAD: true,
			SectorSize: sectorSize,
		},
		{
			Name:       "fspi_software_strong_guard",
			Mode:       GuardSoftwareStrong,
			ModeName:   GuardSoftwareStrong.String(),
			EnableAEAD: true,
			SectorSize: sectorSize,
		},
	}
}

// FaultOutcome captures the exact detection, pre-plaintext rejection, and
// fault-attribution result for one (regime, fault) trial.
type FaultOutcome struct {
	Regime             string    `json:"regime"`
	Fault              FaultKind `json:"fault"`
	Detected           bool      `json:"detected"`
	PreDecryptRejected bool      `json:"pre_decrypt_rejected"`
	SectorLocalized    bool      `json:"sector_localized"`
	PlaintextExposed   bool      `json:"plaintext_exposed"`
	SilentCorruption   bool      `json:"silent_corruption"`
	AEADOpenAttempts   uint64    `json:"aead_open_attempts"`
	AttributedError    string    `json:"attributed_error"`
	Notes              string    `json:"notes,omitempty"`
}

// RegimeFaultSummary aggregates fault detection coverage for a single regime.
type RegimeFaultSummary struct {
	Regime                  string         `json:"regime"`
	Mode                    string         `json:"mode"`
	EnableAEAD              bool           `json:"enable_aead"`
	TotalFaults             int            `json:"total_faults"`
	DetectedCount           int            `json:"detected_count"`
	DetectionCoveragePct    float64        `json:"detection_coverage_pct"`
	PreDecryptRejectedCount int            `json:"pre_decrypt_rejected_count"`
	SectorLocalizedCount    int            `json:"sector_localized_count"`
	PlaintextExposedCount   int            `json:"plaintext_exposed_count"`
	SilentCorruptionCount   int            `json:"silent_corruption_count"`
	Outcomes                []FaultOutcome `json:"outcomes"`
}

// EvaluateFaultMatrix injects all 8 below-encryption faults into real on-disk
// segment files beneath the AEAD layer across all 4 matched regimes.
func EvaluateFaultMatrix(workDir string, sectorSize int) ([]RegimeFaultSummary, error) {
	if err := ValidateSectorSize(sectorSize); err != nil {
		return nil, err
	}
	regimes := FourRegimes(sectorSize)
	summaries := make([]RegimeFaultSummary, 0, len(regimes))

	for _, reg := range regimes {
		sum := RegimeFaultSummary{
			Regime:      reg.Name,
			Mode:        reg.ModeName,
			EnableAEAD:  reg.EnableAEAD,
			TotalFaults: len(AllFaultKinds),
		}
		for _, fk := range AllFaultKinds {
			out, err := runSingleFaultTrial(workDir, reg, fk)
			if err != nil {
				return nil, fmt.Errorf("regime=%s fault=%s: %w", reg.Name, fk, err)
			}
			sum.Outcomes = append(sum.Outcomes, out)
			if out.Detected {
				sum.DetectedCount++
			}
			if out.PreDecryptRejected {
				sum.PreDecryptRejectedCount++
			}
			if out.SectorLocalized {
				sum.SectorLocalizedCount++
			}
			if out.PlaintextExposed {
				sum.PlaintextExposedCount++
			}
			if out.SilentCorruption {
				sum.SilentCorruptionCount++
			}
		}
		sum.DetectionCoveragePct = 100.0 * float64(sum.DetectedCount) / float64(sum.TotalFaults)
		summaries = append(summaries, sum)
	}
	return summaries, nil
}

func runSingleFaultTrial(workDir string, reg RegimeSpec, fault FaultKind) (FaultOutcome, error) {
	trialDir := filepath.Join(workDir, fmt.Sprintf("fault_%s_%s", reg.Name, fault))
	_ = os.RemoveAll(trialDir)
	if err := os.MkdirAll(trialDir, 0o700); err != nil {
		return FaultOutcome{}, err
	}
	defer os.RemoveAll(trialDir)

	dirA := filepath.Join(trialDir, "volA")
	rawDiskA, err := journal.Files(dirA)
	if err != nil {
		return FaultOutcome{}, err
	}

	keyA := bytes.Repeat([]byte{0xA1}, 32)
	storeA, err := OpenSectorEnvelopeStore(
		rawDiskA, "000001.seg", 8, reg.SectorSize, reg.Mode, reg.EnableAEAD,
		[]byte("volume-alpha"), []byte("tenant-alpha"), 1, keyA,
	)
	if err != nil {
		_ = rawDiskA.Close()
		return FaultOutcome{}, err
	}
	defer storeA.Close()

	// Seed LBA 1 (epoch 1) and LBA 2 (epoch 1) with distinct deterministic plaintexts.
	plainLBA1Ep1 := makeDeterministicPayload(reg.SectorSize, 0x11, "volA-lba1-epoch1")
	plainLBA1Ep2 := makeDeterministicPayload(reg.SectorSize, 0x22, "volA-lba1-epoch2")
	plainLBA2Ep1 := makeDeterministicPayload(reg.SectorSize, 0x33, "volA-lba2-epoch1")

	if err := storeA.WriteSector(1, plainLBA1Ep1); err != nil {
		return FaultOutcome{}, err
	}
	if err := storeA.WriteSector(2, plainLBA2Ep1); err != nil {
		return FaultOutcome{}, err
	}
	if err := storeA.Sync(); err != nil {
		return FaultOutcome{}, err
	}

	// Capture exact raw on-disk snapshot of LBA 1 at Epoch 1 (before updating to Epoch 2).
	frameSize := reg.SectorSize + reg.Mode.TupleSize()
	lba1DataOff, lba1PIOff := storeA.File().PhysicalOffsets(1)
	lba2DataOff, _ := storeA.File().PhysicalOffsets(2)
	rawA := storeA.File().RawFile()

	snapLBA1Ep1 := make([]byte, frameSize)
	if _, err := rawA.ReadAt(snapLBA1Ep1, lba1DataOff); err != nil {
		return FaultOutcome{}, err
	}

	// Advance LBA 1 to Epoch 2.
	if err := storeA.WriteSector(1, plainLBA1Ep2); err != nil {
		return FaultOutcome{}, err
	}
	if err := storeA.Sync(); err != nil {
		return FaultOutcome{}, err
	}

	snapLBA1Ep2 := make([]byte, frameSize)
	if _, err := rawA.ReadAt(snapLBA1Ep2, lba1DataOff); err != nil {
		return FaultOutcome{}, err
	}
	snapLBA2Ep1 := make([]byte, frameSize)
	if _, err := rawA.ReadAt(snapLBA2Ep1, lba2DataOff); err != nil {
		return FaultOutcome{}, err
	}

	// Target LBA to read after fault injection (always LBA 1).
	targetLBA := uint64(1)
	expectedPlain := plainLBA1Ep2
	var notes string

	switch fault {
	case FaultSingleBitFlip:
		// Flip 1 bit inside the ciphertext payload (byte 64 of LBA 1).
		var b [1]byte
		if _, err := rawA.ReadAt(b[:], lba1DataOff+64); err != nil {
			return FaultOutcome{}, err
		}
		b[0] ^= 0x08
		if _, err := rawA.WriteAt(b[:], lba1DataOff+64); err != nil {
			return FaultOutcome{}, err
		}

	case FaultMultiByteBurst:
		// Corrupt 64 contiguous bytes in the middle of the ciphertext payload.
		burst := make([]byte, 64)
		if _, err := rawA.ReadAt(burst, lba1DataOff+128); err != nil {
			return FaultOutcome{}, err
		}
		for i := range burst {
			burst[i] ^= 0x5A
		}
		if _, err := rawA.WriteAt(burst, lba1DataOff+128); err != nil {
			return FaultOutcome{}, err
		}

	case FaultMisdirectedWrite:
		// Copy valid (sector + PI) from LBA 2 into LBA 1 within the same volume/round.
		if _, err := rawA.WriteAt(snapLBA2Ep1, lba1DataOff); err != nil {
			return FaultOutcome{}, err
		}
		notes = "valid LBA 2 sector+PI copied onto LBA 1 in same volume/round"

	case FaultTornWrite:
		// First half of sector data from Epoch 2, second half + trailing PI from Epoch 1.
		torn := append([]byte(nil), snapLBA1Ep1...)
		copy(torn[:reg.SectorSize/2], snapLBA1Ep2[:reg.SectorSize/2])
		if _, err := rawA.WriteAt(torn, lba1DataOff); err != nil {
			return FaultOutcome{}, err
		}
		notes = "first half Epoch 2 over second half + trailing PI from Epoch 1"

	case FaultLostWrite:
		// Silently revert entire LBA 1 frame (data + PI) to valid Epoch 1 snapshot.
		if _, err := rawA.WriteAt(snapLBA1Ep1, lba1DataOff); err != nil {
			return FaultOutcome{}, err
		}
		notes = "entire LBA 1 reverted to valid Epoch 1 snapshot (stale replay)"

	case FaultCrossVolumeSwap:
		// Create Volume B (different tenant/volume, same LBA 1 and Epoch 2) and
		// substitute Volume B's valid raw sector+PI frame into Volume A.
		dirB := filepath.Join(trialDir, "volB")
		rawDiskB, err := journal.Files(dirB)
		if err != nil {
			return FaultOutcome{}, err
		}
		keyB := bytes.Repeat([]byte{0xB2}, 32)
		storeB, err := OpenSectorEnvelopeStore(
			rawDiskB, "000001.seg", 8, reg.SectorSize, reg.Mode, reg.EnableAEAD,
			[]byte("volume-beta"), []byte("tenant-beta"), 1, keyB,
		)
		if err != nil {
			_ = rawDiskB.Close()
			return FaultOutcome{}, err
		}
		plainB1 := makeDeterministicPayload(reg.SectorSize, 0x77, "volB-lba1-ep1")
		plainB2 := makeDeterministicPayload(reg.SectorSize, 0x88, "volB-lba1-ep2")
		_ = storeB.WriteSector(1, plainB1)
		_ = storeB.WriteSector(1, plainB2)
		_ = storeB.Sync()
		lba1BOff, _ := storeB.File().PhysicalOffsets(1)
		snapB := make([]byte, frameSize)
		_, rerr := storeB.File().RawFile().ReadAt(snapB, lba1BOff)
		_ = storeB.Close()
		if rerr != nil {
			return FaultOutcome{}, rerr
		}
		if _, err := rawA.WriteAt(snapB, lba1DataOff); err != nil {
			return FaultOutcome{}, err
		}
		notes = "valid Volume B LBA 1 (Epoch 2) swapped into Volume A LBA 1"

	case FaultMetadataTruncated:
		// Zero-fill segment metadata header (FS-PI superblock in PI regimes;
		// first 64 bytes of target sector length/header in non-PI regimes).
		if reg.Mode != GuardNone {
			if _, err := rawA.WriteAt(make([]byte, SuperblockSize), 0); err != nil {
				return FaultOutcome{}, err
			}
			notes = "zero-filled 64-byte FS-PI segment superblock"
		} else {
			if _, err := rawA.WriteAt(make([]byte, 64), lba1DataOff); err != nil {
				return FaultOutcome{}, err
			}
			notes = "zero-filled 64-byte sector envelope header"
		}

	case FaultPICorrupt:
		if reg.Mode == GuardNone {
			// No PI metadata exists in non-PI regimes; sector remains unguarded.
			return FaultOutcome{
				Regime:             reg.Name,
				Fault:              fault,
				Detected:           false,
				PreDecryptRejected: false,
				SectorLocalized:    false,
				PlaintextExposed:   false,
				SilentCorruption:   false,
				AttributedError:    "none (no PI tuple present)",
				Notes:              "regime stores no PI metadata; PI-layer fault detection absent",
			}, nil
		}
		// Flip 1 bit in the PI tuple (byte 2 = AppTag high byte) while leaving payload untouched.
		var b [1]byte
		flipOff := lba1PIOff + 2
		if reg.Mode == GuardSoftwareStrong {
			flipOff = lba1PIOff + 8 // AppTag high byte in Tuple128
		}
		if _, err := rawA.ReadAt(b[:], flipOff); err != nil {
			return FaultOutcome{}, err
		}
		b[0] ^= 0x01
		if _, err := rawA.WriteAt(b[:], flipOff); err != nil {
			return FaultOutcome{}, err
		}
		notes = "single bit flipped inside PI tuple bytes while payload untouched"
	}

	if err := rawA.Sync(); err != nil {
		return FaultOutcome{}, err
	}

	gotPlain, stats, readErr := storeA.ReadSectorVerified(targetLBA)
	out := FaultOutcome{
		Regime:           reg.Name,
		Fault:            fault,
		AEADOpenAttempts: stats.AEADOpenAttempts,
		Notes:            notes,
	}

	if readErr != nil {
		out.Detected = true
		out.PlaintextExposed = stats.PlaintextExposedCount > 0
		out.PreDecryptRejected = stats.AEADOpenAttempts == 0 && stats.PIVerifyRejected > 0
		var ie *IntegrityError
		if errors.As(readErr, &ie) {
			out.AttributedError = ie.Kind.Error()
			// Sector localization is true when PI rejects a specific LBA prior to AEAD.
			out.SectorLocalized = out.PreDecryptRejected
		} else {
			out.AttributedError = readErr.Error()
		}
		return out, nil
	}

	// Read succeeded without error! Check whether returned plaintext is silently corrupted/stale.
	out.Detected = false
	out.PlaintextExposed = stats.PlaintextExposedCount > 0
	out.SilentCorruption = !bytes.Equal(gotPlain, expectedPlain)
	out.AttributedError = "none (accepted silently)"
	return out, nil
}

func makeDeterministicPayload(sectorSize int, fill byte, tag string) []byte {
	// Keep 64 bytes headroom for 4B length prefix + 12B nonce + 16B GCM tag.
	n := sectorSize - 64
	if n < 64 {
		n = 64
	}
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte((int(fill) + i*31) & 0xFF)
	}
	copy(buf, []byte(tag))
	return buf
}

// CRC16ForgeryResult documents Negative Regime #1: empirical proof that
// 16-bit CRC16-T10-DIF is non-cryptographic (trivially forgeable by an active
// attacker and subject to 1/65536 random collisions), and that composing it
// with HIP-0302 / AES-256-GCM AEAD stops 100% of active forgeries before
// plaintext exposure.
type CRC16ForgeryResult struct {
	SectorSize                 int     `json:"sector_size"`
	ActiveForgeryPIBypassed    bool    `json:"active_forgery_pi_bypassed"`
	ActiveForgeryAEADCaught    bool    `json:"active_forgery_aead_caught"`
	ActiveForgeryPlainExposed  bool    `json:"active_forgery_plaintext_exposed"`
	ActiveForgeryError         string  `json:"active_forgery_error"`
	CollisionTrials            int     `json:"collision_trials"`
	CRC16CollisionsObserved    int     `json:"crc16_collisions_observed"`
	CRC16EmpiricalRate         float64 `json:"crc16_empirical_rate"`
	CRC16TheoreticalRate       float64 `json:"crc16_theoretical_rate"`
	StrongGuardCollisionsFound int     `json:"strong_guard_collisions_found"`
}

// RunCRC16ForgeryControl executes Negative Regime #1:
//  1. Active attacker tampers with ciphertext AND recomputes a valid 16-bit
//     CRC16-T10-DIF guard -> proves GuardT10DIF alone is bypassed while the
//     composed AES-256-GCM / HIP-0302 AEAD catches it with zero plaintext exposed.
//  2. Empirical random collision sweep over trials mutations comparing
//     CRC16-T10-DIF (1/65536) vs GuardSoftwareStrong (CRC32C + BLAKE3-32).
func RunCRC16ForgeryControl(workDir string, sectorSize int, trials int, seed int64) (CRC16ForgeryResult, error) {
	if err := ValidateSectorSize(sectorSize); err != nil {
		return CRC16ForgeryResult{}, err
	}
	if trials <= 0 {
		trials = 65536
	}

	dir := filepath.Join(workDir, "neg1_crc16_forgery")
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return CRC16ForgeryResult{}, err
	}
	defer os.RemoveAll(dir)

	rawDisk, err := journal.Files(dir)
	if err != nil {
		return CRC16ForgeryResult{}, err
	}
	key := bytes.Repeat([]byte{0x42}, 32)
	store, err := OpenSectorEnvelopeStore(
		rawDisk, "000001.seg", 4, sectorSize, GuardT10DIF, true,
		[]byte("volume-neg1"), []byte("tenant-neg1"), 1, key,
	)
	if err != nil {
		_ = rawDisk.Close()
		return CRC16ForgeryResult{}, err
	}
	defer store.Close()

	plain := makeDeterministicPayload(sectorSize, 0x55, "hip-0302-secret-record")
	if err := store.WriteSector(1, plain); err != nil {
		return CRC16ForgeryResult{}, err
	}
	if err := store.Sync(); err != nil {
		return CRC16ForgeryResult{}, err
	}

	// Active attacker reads raw LBA 1 sector + T10-DIF PI, modifies ciphertext,
	// and recomputes the unkeyed 16-bit CRC16-T10-DIF GuardTag!
	dataOff, piOff := store.File().PhysicalOffsets(1)
	raw := store.File().RawFile()
	sectorData := make([]byte, sectorSize)
	if _, err := raw.ReadAt(sectorData, dataOff); err != nil {
		return CRC16ForgeryResult{}, err
	}
	piBuf := make([]byte, Tuple64Size)
	if _, err := raw.ReadAt(piBuf, piOff); err != nil {
		return CRC16ForgeryResult{}, err
	}

	// Tamper with ciphertext payload and forge matching CRC16-T10-DIF.
	sectorData[48] ^= 0xFF
	forgedGuard := CRC16T10DIF(sectorData)
	binary.BigEndian.PutUint16(piBuf[0:2], forgedGuard)

	if _, err := raw.WriteAt(sectorData, dataOff); err != nil {
		return CRC16ForgeryResult{}, err
	}
	if _, err := raw.WriteAt(piBuf, piOff); err != nil {
		return CRC16ForgeryResult{}, err
	}
	if err := raw.Sync(); err != nil {
		return CRC16ForgeryResult{}, err
	}

	_, stats, readErr := store.ReadSectorVerified(1)
	res := CRC16ForgeryResult{
		SectorSize:                sectorSize,
		ActiveForgeryPIBypassed:   stats.PIVerifyPassed > 0 && stats.PIVerifyRejected == 0,
		ActiveForgeryAEADCaught:   errors.Is(readErr, ErrAEADAuthFailed),
		ActiveForgeryPlainExposed: stats.PlaintextExposedCount > 0,
		CollisionTrials:           trials,
		CRC16TheoreticalRate:      1.0 / 65536.0,
	}
	if readErr != nil {
		res.ActiveForgeryError = readErr.Error()
	}

	// Empirical collision measurement over deterministic random mutations.
	rng := rand.New(rand.NewSource(seed))
	base := make([]byte, sectorSize)
	for i := range base {
		base[i] = byte(rng.Intn(256))
	}
	baseCRC16 := CRC16T10DIF(base)
	baseCRC32 := crc32.Checksum(base, castagnoliTable)
	baseB3 := blake3Guard32(0x1234, 1, 1, base)

	mut := make([]byte, sectorSize)
	copy(mut, base)
	for i := 0; i < trials; i++ {
		// Mutate 4 deterministic bytes per trial (non-trivial multi-byte corruption).
		off := rng.Intn(sectorSize - 4)
		var orig [4]byte
		copy(orig[:], mut[off:off+4])
		binary.BigEndian.PutUint32(mut[off:off+4], rng.Uint32()^binary.BigEndian.Uint32(orig[:])|1)

		if CRC16T10DIF(mut) == baseCRC16 {
			res.CRC16CollisionsObserved++
		}
		if crc32.Checksum(mut, castagnoliTable) == baseCRC32 {
			if blake3Guard32(0x1234, 1, 1, mut) == baseB3 {
				res.StrongGuardCollisionsFound++
			}
		}
		copy(mut[off:off+4], orig[:])
	}
	res.CRC16EmpiricalRate = float64(res.CRC16CollisionsObserved) / float64(trials)
	return res, nil
}
