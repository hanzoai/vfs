package fspi

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/journal"
)

// JournalConfig configures a ProtectedJournal that composes FS-PI
// (ProtectedDisk) beneath journal.Journal and HIP-0302 / AES-256-GCM AEAD
// cipher envelopes.
type JournalConfig struct {
	VolumeID    []byte
	TenantID    []byte
	Round       uint64
	Published   uint64
	Capacity    int64
	SegmentSize int64
	SectorSize  int
	Mode        GuardMode
	EnableAEAD  bool
	// Cipher optionally supplies the canonical vfs.Cipher (e.g. vfs.Hybrid
	// X25519 + ML-KEM-768 per HIP-0302) used to seal the per-journal 256-bit
	// AES-GCM data key, mirroring vfs/ledger.go.
	Cipher vfs.Cipher
	// RawDisk optionally overrides journal.Files(dir) for testing or custom I/O.
	RawDisk journal.Disk
	// Epochs optionally supplies a shared EpochLedger across close/reopen.
	Epochs *EpochLedger
}

// ReplayStats records exact pre-plaintext verification telemetry during
// ReplayVerified or SectorEnvelopeStore.ReadVerified.
type ReplayStats struct {
	RecordsVerified       uint64 `json:"records_verified"`
	PIVerifyPassed        uint64 `json:"pi_verify_passed"`
	PIVerifyRejected      uint64 `json:"pi_verify_rejected"`
	AEADOpenAttempts      uint64 `json:"aead_open_attempts"`
	AEADOpenFailures      uint64 `json:"aead_open_failures"`
	PlaintextExposedCount uint64 `json:"plaintext_exposed_count"`
}

// aeadEnvelope implements the canonical vfs/ledger.go AES-256-GCM per-record
// envelope: [4B random salt | 8B monotonic counter][ciphertext][16B GCM tag]
// bound to lineage AAD (NIST SP 800-38D §8.2.1).
type aeadEnvelope struct {
	aead  cipher.AEAD
	aad   []byte
	salt  [4]byte
	nonce atomic.Uint64
	key   []byte
}

func makeLineage(volumeID, tenantID []byte, round uint64) []byte {
	out := make([]byte, 0, len(volumeID)+len(tenantID)+12)
	out = binary.BigEndian.AppendUint16(out, uint16(len(volumeID)))
	out = append(out, volumeID...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(tenantID)))
	out = append(out, tenantID...)
	out = binary.BigEndian.AppendUint64(out, round)
	return out
}

func newAEADEnvelope(key, aad []byte) (*aeadEnvelope, error) {
	if len(key) != 32 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, err
		}
	}
	blk, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, err
	}
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	return &aeadEnvelope{
		aead: aead,
		aad:  append([]byte(nil), aad...),
		salt: salt,
		key:  append([]byte(nil), key...),
	}, nil
}

func (e *aeadEnvelope) Seal(plaintext []byte) []byte {
	ns := e.aead.NonceSize()
	out := make([]byte, ns, ns+len(plaintext)+e.aead.Overhead())
	copy(out[:4], e.salt[:])
	binary.BigEndian.PutUint64(out[4:12], e.nonce.Add(1))
	return e.aead.Seal(out, out[:ns], plaintext, e.aad)
}

func (e *aeadEnvelope) Open(sealed []byte) ([]byte, error) {
	ns := e.aead.NonceSize()
	if len(sealed) < ns+e.aead.Overhead() {
		return nil, &IntegrityError{
			Kind:   ErrAEADAuthFailed,
			Detail: fmt.Sprintf("truncated AEAD envelope (%d bytes)", len(sealed)),
		}
	}
	plain, err := e.aead.Open(nil, sealed[:ns], sealed[ns:], e.aad)
	if err != nil {
		return nil, &IntegrityError{
			Kind:   ErrAEADAuthFailed,
			Detail: fmt.Sprintf("AES-256-GCM tag verification failed: %v", err),
		}
	}
	return plain, nil
}

// ProtectedJournal composes FS-PI (ProtectedDisk) beneath journal.Journal and
// HIP-0302 / AES-256-GCM AEAD envelopes, enforcing strict pre-plaintext
// verification on every replay and read.
type ProtectedJournal struct {
	dir     string
	keyPath string
	cfg     JournalConfig
	disk    *ProtectedDisk
	j       *journal.Journal
	env     *aeadEnvelope
	lineage []byte
	appTag  uint16
}

// OpenProtectedJournal opens or recovers a journal backed by ProtectedDisk and
// optional HIP-0302 / AES-256-GCM AEAD encryption.
func OpenProtectedJournal(dir string, cfg JournalConfig) (*ProtectedJournal, error) {
	if len(cfg.VolumeID) == 0 {
		cfg.VolumeID = []byte("vfs-vol-default")
	}
	if len(cfg.TenantID) == 0 {
		cfg.TenantID = []byte("tenant-default")
	}
	if cfg.Round == 0 {
		cfg.Round = 1
	}
	if cfg.SectorSize == 0 {
		cfg.SectorSize = SectorSize4096
	}
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = 1 << 20 // 1 MiB
	}
	if cfg.Capacity == 0 {
		cfg.Capacity = 4 * cfg.SegmentSize
	}

	lin := makeLineage(cfg.VolumeID, cfg.TenantID, cfg.Round)
	appTag := DeriveAppTag(cfg.VolumeID, cfg.TenantID, cfg.Round)

	rawDisk := cfg.RawDisk
	if rawDisk == nil {
		var err error
		rawDisk, err = journal.Files(dir)
		if err != nil {
			return nil, err
		}
	}
	pDisk, err := WrapDisk(rawDisk, DiskConfig{
		Mode:       cfg.Mode,
		SectorSize: cfg.SectorSize,
		AppTag:     appTag,
		Epochs:     cfg.Epochs,
	})
	if err != nil {
		_ = rawDisk.Close()
		return nil, err
	}

	var env *aeadEnvelope
	keyPath := filepath.Join(filepath.Dir(filepath.Clean(dir)), filepath.Base(filepath.Clean(dir))+".fspi.key")
	if cfg.EnableAEAD {
		var dataKey []byte
		if cfg.Cipher != nil {
			dataKey, err = loadOrCreateWrappedKey(keyPath, cfg.Cipher)
			if err != nil {
				_ = pDisk.Close()
				return nil, err
			}
		}
		env, err = newAEADEnvelope(dataKey, lin)
		if err != nil {
			_ = pDisk.Close()
			return nil, err
		}
	}

	j, err := journal.Open(dir, journal.Options{
		Capacity:  cfg.Capacity,
		Segment:   cfg.SegmentSize,
		Published: cfg.Published,
		Lineage:   lin,
		Disk:      pDisk,
	})
	if err != nil {
		_ = pDisk.Close()
		return nil, err
	}

	return &ProtectedJournal{
		dir:     dir,
		keyPath: keyPath,
		cfg:     cfg,
		disk:    pDisk,
		j:       j,
		env:     env,
		lineage: lin,
		appTag:  appTag,
	}, nil
}

func loadOrCreateWrappedKey(keyPath string, c vfs.Cipher) ([]byte, error) {
	if raw, err := os.ReadFile(keyPath); err == nil && len(raw) > 2 {
		idLen := int(raw[0])
		if len(raw) < 1+idLen {
			return nil, fmt.Errorf("%w: truncated wrapped journal key", ErrMetadataTruncated)
		}
		keyID := string(raw[1 : 1+idLen])
		key, err := c.Open(raw[1+idLen:], keyID)
		if err != nil {
			return nil, fmt.Errorf("%w: unwrap journal key: %v", ErrAEADAuthFailed, err)
		}
		return key, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	sealed, keyID, err := c.Seal(key)
	if err != nil {
		return nil, err
	}
	if len(keyID) == 0 || len(keyID) > 255 {
		return nil, fmt.Errorf("fspi: invalid cipher keyID length %d", len(keyID))
	}
	frame := make([]byte, 0, 1+len(keyID)+len(sealed))
	frame = append(frame, byte(len(keyID)))
	frame = append(frame, keyID...)
	frame = append(frame, sealed...)
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, frame, 0o600); err != nil {
		return nil, err
	}
	return key, nil
}

// Disk returns the underlying FS-PI ProtectedDisk.
func (p *ProtectedJournal) Disk() *ProtectedDisk { return p.disk }

// Journal returns the underlying journal.Journal.
func (p *ProtectedJournal) Journal() *journal.Journal { return p.j }

// AppTag returns the volume/tenant AppTag bound to this journal.
func (p *ProtectedJournal) AppTag() uint16 { return p.appTag }

// Append seals plaintext with AEAD (when enabled) and appends the ciphertext
// into the FS-PI protected journal.
func (p *ProtectedJournal) Append(ctx context.Context, plaintext []byte) (uint64, error) {
	payload := plaintext
	if p.env != nil {
		payload = p.env.Seal(plaintext)
	}
	return p.j.Append(ctx, payload)
}

// Sync waits for seq to become durable on the local journal disk.
func (p *ProtectedJournal) Sync(ctx context.Context, seq uint64) error {
	return p.j.Sync(ctx, seq)
}

// ReplayVerified replays all unpublished records, enforcing strict
// pre-plaintext FS-PI verification BEFORE AEAD decryption.
func (p *ProtectedJournal) ReplayVerified(fn func(seq uint64, plaintext []byte) error) (ReplayStats, error) {
	beforePass := p.disk.verifyPassCount.Load()
	beforeFail := p.disk.verifyFailCount.Load()

	var stats ReplayStats
	err := p.j.Replay(func(r journal.Record) error {
		var plain []byte
		if p.env != nil {
			stats.AEADOpenAttempts++
			var oerr error
			plain, oerr = p.env.Open(r.Data)
			if oerr != nil {
				stats.AEADOpenFailures++
				return oerr
			}
		} else {
			plain = append([]byte(nil), r.Data...)
		}
		stats.RecordsVerified++
		stats.PlaintextExposedCount++
		if fn != nil {
			return fn(r.Seq, plain)
		}
		return nil
	})
	stats.PIVerifyPassed = p.disk.verifyPassCount.Load() - beforePass
	stats.PIVerifyRejected = p.disk.verifyFailCount.Load() - beforeFail
	return stats, err
}

// Close closes the underlying journal and disk.
func (p *ProtectedJournal) Close() error {
	return p.j.Close()
}

// SectorEnvelopeStore models sector-addressed encrypted journal storage where
// each logical sector carries a length-framed AEAD ciphertext (or raw payload
// in baseline mode) persisted through ProtectedFile. It enables deterministic
// multi-epoch overwrites and exact below-encryption fault injection across all
// 8 fault classes and all 4 evaluation regimes.
type SectorEnvelopeStore struct {
	disk       *ProtectedDisk
	file       *ProtectedFile
	env        *aeadEnvelope
	sectorSize int
	enableAEAD bool
	segName    string
}

// OpenSectorEnvelopeStore creates a sector-addressed store over rawDisk.
func OpenSectorEnvelopeStore(rawDisk journal.Disk, segName string, numSectors int, sectorSize int, mode GuardMode, enableAEAD bool, volumeID, tenantID []byte, round uint64, key []byte) (*SectorEnvelopeStore, error) {
	if err := ValidateSectorSize(sectorSize); err != nil {
		return nil, err
	}
	appTag := DeriveAppTag(volumeID, tenantID, round)
	pDisk, err := WrapDisk(rawDisk, DiskConfig{
		Mode:       mode,
		SectorSize: sectorSize,
		AppTag:     appTag,
	})
	if err != nil {
		return nil, err
	}
	logicalSize := int64(numSectors) * int64(sectorSize)
	jf, err := pDisk.Create(segName, logicalSize)
	if err != nil {
		_ = pDisk.Close()
		return nil, err
	}
	pf := jf.(*ProtectedFile)

	var env *aeadEnvelope
	if enableAEAD {
		lin := makeLineage(volumeID, tenantID, round)
		env, err = newAEADEnvelope(key, lin)
		if err != nil {
			_ = pf.Close()
			_ = pDisk.Close()
			return nil, err
		}
	}

	return &SectorEnvelopeStore{
		disk:       pDisk,
		file:       pf,
		env:        env,
		sectorSize: sectorSize,
		enableAEAD: enableAEAD,
		segName:    segName,
	}, nil
}

// Disk returns the underlying ProtectedDisk.
func (s *SectorEnvelopeStore) Disk() *ProtectedDisk { return s.disk }

// File returns the underlying ProtectedFile.
func (s *SectorEnvelopeStore) File() *ProtectedFile { return s.file }

// WriteSector encrypts plaintext (when AEAD is enabled), frames it into a
// logical sector, and writes it through ProtectedFile (which computes and
// interleaves the FS-PI tuple).
func (s *SectorEnvelopeStore) WriteSector(lba uint64, plaintext []byte) error {
	payload := plaintext
	if s.enableAEAD {
		payload = s.env.Seal(plaintext)
	}
	if 4+len(payload) > s.sectorSize {
		return fmt.Errorf("fspi: sealed payload (%d bytes) exceeds sector size %d", 4+len(payload), s.sectorSize)
	}
	sec := make([]byte, s.sectorSize)
	binary.BigEndian.PutUint32(sec[0:4], uint32(len(payload)))
	copy(sec[4:4+len(payload)], payload)
	_, err := s.file.WriteAt(sec, int64(lba)*int64(s.sectorSize))
	return err
}

// ReadSectorVerified reads logical sector lba through ProtectedFile (verifying
// FS-PI strictly BEFORE AEAD decryption) and returns plaintext plus exact
// pre-plaintext verification telemetry.
func (s *SectorEnvelopeStore) ReadSectorVerified(lba uint64) ([]byte, ReplayStats, error) {
	beforePass := s.disk.verifyPassCount.Load()
	beforeFail := s.disk.verifyFailCount.Load()

	var stats ReplayStats
	sec := make([]byte, s.sectorSize)
	// Always verify segment superblock integrity on read so below-encryption
	// superblock truncation/zero-fill fails closed regardless of target LBA.
	if s.disk.mode != GuardNone {
		if _, err := s.disk.readAndVerifySuperblock(s.file.raw, s.segName); err != nil {
			stats.PIVerifyPassed = s.disk.verifyPassCount.Load() - beforePass
			stats.PIVerifyRejected = s.disk.verifyFailCount.Load() - beforeFail
			return nil, stats, err
		}
	}
	_, err := s.file.ReadAt(sec, int64(lba)*int64(s.sectorSize))
	stats.PIVerifyPassed = s.disk.verifyPassCount.Load() - beforePass
	stats.PIVerifyRejected = s.disk.verifyFailCount.Load() - beforeFail
	if err != nil {
		// Strictly rejected BEFORE AEAD open and BEFORE plaintext exposure!
		return nil, stats, err
	}

	n := int(binary.BigEndian.Uint32(sec[0:4]))
	if n == 0 || 4+n > s.sectorSize {
		if s.enableAEAD {
			stats.AEADOpenAttempts++
			stats.AEADOpenFailures++
			return nil, stats, &IntegrityError{
				Kind:    ErrAEADAuthFailed,
				Segment: s.segName,
				LBA:     lba,
				Detail:  fmt.Sprintf("invalid framed length %d in sector", n),
			}
		}
		// Unprotected baseline silently returns whatever bytes were on disk.
		stats.PlaintextExposedCount++
		return sec[4:], stats, nil
	}

	rawPayload := sec[4 : 4+n]
	if !s.enableAEAD {
		stats.RecordsVerified = 1
		stats.PlaintextExposedCount = 1
		return append([]byte(nil), rawPayload...), stats, nil
	}

	stats.AEADOpenAttempts = 1
	plain, err := s.env.Open(rawPayload)
	if err != nil {
		stats.AEADOpenFailures = 1
		return nil, stats, err
	}
	stats.RecordsVerified = 1
	stats.PlaintextExposedCount = 1
	return plain, stats, nil
}

// Sync flushes the underlying segment file.
func (s *SectorEnvelopeStore) Sync() error { return s.file.Sync() }

// Close closes the underlying file and disk.
func (s *SectorEnvelopeStore) Close() error {
	err1 := s.file.Close()
	err2 := s.disk.Close()
	return errors.Join(err1, err2)
}
