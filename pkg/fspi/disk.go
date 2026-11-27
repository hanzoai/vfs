package fspi

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/journal"
)

const (
	// SuperblockSize is the fixed 64-byte FS-PI metadata header stored at the
	// start of every protected segment file.
	SuperblockSize = 64

	superblockVersion = 1
)

var superblockMagic = [4]byte{'F', 'S', 'P', 'I'}

// EpochLedger tracks the latest durable write epoch per (segment, LBA) and
// computes a deterministic BLAKE3 Merkle epoch root for lost-write / stale
// sector replay detection across reads and crash recovery.
type EpochLedger struct {
	mu     sync.RWMutex
	epochs map[string]map[uint64]uint16
}

// NewEpochLedger allocates an empty EpochLedger.
func NewEpochLedger() *EpochLedger {
	return &EpochLedger{
		epochs: make(map[string]map[uint64]uint16),
	}
}

// Get returns the expected epoch for (seg, lba), or 0 if never written.
func (e *EpochLedger) Get(seg string, lba uint64) uint16 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if m := e.epochs[seg]; m != nil {
		return m[lba]
	}
	return 0
}

// Set records the latest committed epoch for (seg, lba).
func (e *EpochLedger) Set(seg string, lba uint64, epoch uint16) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.epochs[seg]
	if m == nil {
		m = make(map[uint64]uint16)
		e.epochs[seg] = m
	}
	m[lba] = epoch
}

// DeleteSegment drops all epoch entries for a removed segment.
func (e *EpochLedger) DeleteSegment(seg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.epochs, seg)
}

// Clone returns a deep copy of the epoch table (useful for crash snapshots).
func (e *EpochLedger) Clone() *EpochLedger {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := NewEpochLedger()
	for seg, m := range e.epochs {
		cp := make(map[uint64]uint16, len(m))
		for k, v := range m {
			cp[k] = v
		}
		out.epochs[seg] = cp
	}
	return out
}

// MerkleRoot computes a canonical BLAKE3 digest over all tracked (seg, lba, epoch)
// entries so a volume head or control-plane witness can commit to the journal's
// sector epoch map against silent lost writes.
func (e *EpochLedger) MerkleRoot() [blake3.Size]byte {
	e.mu.RLock()
	defer e.mu.RUnlock()

	segs := make([]string, 0, len(e.epochs))
	for seg := range e.epochs {
		segs = append(segs, seg)
	}
	sort.Strings(segs)

	h := blake3.New()
	_, _ = h.Write([]byte("hanzoai/vfs fspi epoch-root v1"))
	var rec [10]byte
	for _, seg := range segs {
		m := e.epochs[seg]
		lbas := make([]uint64, 0, len(m))
		for lba := range m {
			lbas = append(lbas, lba)
		}
		sort.Slice(lbas, func(i, j int) bool { return lbas[i] < lbas[j] })
		_, _ = h.Write([]byte(seg))
		_, _ = h.Write([]byte{0})
		for _, lba := range lbas {
			binary.BigEndian.PutUint64(rec[0:8], lba)
			binary.BigEndian.PutUint16(rec[8:10], m[lba])
			_, _ = h.Write(rec[:])
		}
	}
	var root [blake3.Size]byte
	h.Sum(root[:0])
	return root
}

// DiskConfig configures a ProtectedDisk wrapping a journal.Disk.
type DiskConfig struct {
	Mode       GuardMode
	SectorSize int
	AppTag     uint16
	Epochs     *EpochLedger
	Probe      *ProbeReport
}

// DiskStats summarizes runtime I/O, space overhead, and verification counts
// collected by a ProtectedDisk.
type DiskStats struct {
	Mode                 string  `json:"mode"`
	SectorSize           int     `json:"sector_size"`
	TupleSize            int     `json:"tuple_size"`
	HWPISupported        bool    `json:"hw_pi_supported"`
	SectorsRead          uint64  `json:"sectors_read"`
	SectorsWritten       uint64  `json:"sectors_written"`
	PartialRMWCount      uint64  `json:"partial_rmw_count"`
	LogicalBytesWritten  uint64  `json:"logical_bytes_written"`
	PhysicalBytesWritten uint64  `json:"physical_bytes_written"`
	PIBytesWritten       uint64  `json:"pi_bytes_written"`
	VerifyPassCount      uint64  `json:"verify_pass_count"`
	VerifyFailCount      uint64  `json:"verify_fail_count"`
	PIGenerateNanos      uint64  `json:"pi_generate_nanos"`
	PIVerifyNanos        uint64  `json:"pi_verify_nanos"`
	WriteAmpRatio        float64 `json:"write_amp_ratio"`
	SpaceOverheadPct     float64 `json:"space_overhead_pct"`
}

// ProtectedDisk wraps any journal.Disk to attach and verify per-sector FS-PI
// Protection Information on every journal segment read and write.
type ProtectedDisk struct {
	raw        journal.Disk
	mode       GuardMode
	sectorSize int
	tupleSize  int
	appTag     uint16
	epochs     *EpochLedger
	probe      ProbeReport

	mu    sync.Mutex
	files map[string]*ProtectedFile

	sectorsRead          atomic.Uint64
	sectorsWritten       atomic.Uint64
	partialRMWCount      atomic.Uint64
	logicalBytesWritten  atomic.Uint64
	physicalBytesWritten atomic.Uint64
	piBytesWritten       atomic.Uint64
	verifyPassCount      atomic.Uint64
	verifyFailCount      atomic.Uint64
	piGenerateNanos      atomic.Uint64
	piVerifyNanos        atomic.Uint64
}

// WrapDisk creates an FS-PI protected journal.Disk over raw.
func WrapDisk(raw journal.Disk, cfg DiskConfig) (*ProtectedDisk, error) {
	if raw == nil {
		return nil, fmt.Errorf("%w: nil underlying journal.Disk", ErrMetadataTruncated)
	}
	if cfg.SectorSize == 0 {
		cfg.SectorSize = SectorSize4096
	}
	if err := ValidateSectorSize(cfg.SectorSize); err != nil {
		return nil, err
	}
	if cfg.Mode != GuardNone && (cfg.AppTag == 0 || cfg.AppTag == t10AppTagEscape) {
		cfg.AppTag = DeriveAppTag([]byte("default-volume"), []byte("default-tenant"), 1)
	}
	epochs := cfg.Epochs
	if epochs == nil {
		epochs = NewEpochLedger()
	}
	var probe ProbeReport
	if cfg.Probe != nil {
		probe = *cfg.Probe
	} else {
		probe = ProbeNVMePI()
	}
	return &ProtectedDisk{
		raw:        raw,
		mode:       cfg.Mode,
		sectorSize: cfg.SectorSize,
		tupleSize:  cfg.Mode.TupleSize(),
		appTag:     cfg.AppTag,
		epochs:     epochs,
		probe:      probe,
		files:      make(map[string]*ProtectedFile),
	}, nil
}

// DiskFactory returns a journal.Disk constructor suitable for wiring directly
// into vfs.Config.Disk, enabling FS-PI protection on live vfs.Volume journals
// without modifying core vfs or journal packages.
func DiskFactory(cfg DiskConfig) func(dir string) (journal.Disk, error) {
	return func(dir string) (journal.Disk, error) {
		raw, err := journal.Files(dir)
		if err != nil {
			return nil, err
		}
		d, err := WrapDisk(raw, cfg)
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		return d, nil
	}
}

// Mode returns the configured GuardMode.
func (d *ProtectedDisk) Mode() GuardMode { return d.mode }

// SectorSize returns the configured logical sector size in bytes.
func (d *ProtectedDisk) SectorSize() int { return d.sectorSize }

// AppTag returns the 16-bit volume/tenant namespace AppTag.
func (d *ProtectedDisk) AppTag() uint16 { return d.appTag }

// Epochs returns the EpochLedger tracking committed sector epochs.
func (d *ProtectedDisk) Epochs() *EpochLedger { return d.epochs }

// ProbeReport returns the NVMe PI hardware capability probe result.
func (d *ProtectedDisk) ProbeReport() ProbeReport { return d.probe }

// RawDisk returns the underlying journal.Disk (used by below-encryption fault
// injection tests to corrupt raw storage bytes beneath FS-PI and AEAD).
func (d *ProtectedDisk) RawDisk() journal.Disk { return d.raw }

// Stats returns a point-in-time snapshot of I/O, verification, and overhead metrics.
func (d *ProtectedDisk) Stats() DiskStats {
	logB := d.logicalBytesWritten.Load()
	physB := d.physicalBytesWritten.Load()
	wa := 1.0
	if logB > 0 {
		wa = float64(physB) / float64(logB)
	}
	spacePct := 0.0
	if d.sectorSize > 0 && d.tupleSize > 0 {
		spacePct = 100.0 * float64(d.tupleSize) / float64(d.sectorSize)
	}
	return DiskStats{
		Mode:                 d.mode.String(),
		SectorSize:           d.sectorSize,
		TupleSize:            d.tupleSize,
		HWPISupported:        d.probe.HWPISupported,
		SectorsRead:          d.sectorsRead.Load(),
		SectorsWritten:       d.sectorsWritten.Load(),
		PartialRMWCount:      d.partialRMWCount.Load(),
		LogicalBytesWritten:  logB,
		PhysicalBytesWritten: physB,
		PIBytesWritten:       d.piBytesWritten.Load(),
		VerifyPassCount:      d.verifyPassCount.Load(),
		VerifyFailCount:      d.verifyFailCount.Load(),
		PIGenerateNanos:      d.piGenerateNanos.Load(),
		PIVerifyNanos:        d.piVerifyNanos.Load(),
		WriteAmpRatio:        wa,
		SpaceOverheadPct:     spacePct,
	}
}

// PhysicalSize computes the physical segment file size required for logicalSize.
func (d *ProtectedDisk) PhysicalSize(logicalSize int64) int64 {
	if d.mode == GuardNone {
		return logicalSize
	}
	nSec := (logicalSize + int64(d.sectorSize) - 1) / int64(d.sectorSize)
	return int64(SuperblockSize) + nSec*int64(d.sectorSize+d.tupleSize)
}

// List implements journal.Disk.
func (d *ProtectedDisk) List() ([]string, error) { return d.raw.List() }

// Warnings forwards any filesystem warnings from the underlying journal.Disk.
func (d *ProtectedDisk) Warnings() []string {
	if w, ok := d.raw.(interface{ Warnings() []string }); ok {
		return w.Warnings()
	}
	return nil
}

// Create implements journal.Disk.
func (d *ProtectedDisk) Create(name string, logicalSize int64) (journal.File, error) {
	physSize := d.PhysicalSize(logicalSize)
	rf, err := d.raw.Create(name, physSize)
	if err != nil {
		return nil, err
	}
	if d.mode != GuardNone {
		sb := encodeSuperblock(d.mode, d.appTag, d.sectorSize, d.tupleSize, logicalSize, segmentIDOf(name))
		if _, err := rf.WriteAt(sb[:], 0); err != nil {
			_ = rf.Close()
			return nil, err
		}
		if err := rf.Sync(); err != nil {
			_ = rf.Close()
			return nil, err
		}
		d.epochs.DeleteSegment(name)
	}
	pf := &ProtectedFile{
		disk:        d,
		raw:         rf,
		name:        name,
		logicalSize: logicalSize,
		frameSize:   d.sectorSize + d.tupleSize,
	}
	d.mu.Lock()
	d.files[name] = pf
	d.mu.Unlock()
	return pf, nil
}

// Open implements journal.Disk.
func (d *ProtectedDisk) Open(name string) (journal.File, error) {
	rf, err := d.raw.Open(name)
	if err != nil {
		return nil, err
	}
	logicalSize := rf.Size()
	if d.mode != GuardNone {
		sb, err := d.readAndVerifySuperblock(rf, name)
		if err != nil {
			_ = rf.Close()
			return nil, err
		}
		logicalSize = sb.logicalSize
	}
	pf := &ProtectedFile{
		disk:        d,
		raw:         rf,
		name:        name,
		logicalSize: logicalSize,
		frameSize:   d.sectorSize + d.tupleSize,
	}
	d.mu.Lock()
	d.files[name] = pf
	d.mu.Unlock()
	return pf, nil
}

// Remove implements journal.Disk.
func (d *ProtectedDisk) Remove(name string) error {
	d.mu.Lock()
	delete(d.files, name)
	d.mu.Unlock()
	d.epochs.DeleteSegment(name)
	return d.raw.Remove(name)
}

// Close implements journal.Disk.
func (d *ProtectedDisk) Close() error { return d.raw.Close() }

// OpenFile returns the currently tracked ProtectedFile for name, if open.
func (d *ProtectedDisk) OpenFile(name string) *ProtectedFile {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.files[name]
}

type superblock struct {
	mode        GuardMode
	appTag      uint16
	sectorSize  int
	tupleSize   int
	logicalSize int64
	segmentID   uint64
}

func segmentIDOf(name string) uint64 {
	sum := blake3.Sum([]byte(name))
	return binary.BigEndian.Uint64(sum[:8])
}

func encodeSuperblock(mode GuardMode, appTag uint16, sectorSize, tupleSize int, logicalSize int64, segID uint64) [SuperblockSize]byte {
	var sb [SuperblockSize]byte
	copy(sb[0:4], superblockMagic[:])
	sb[4] = superblockVersion
	sb[5] = uint8(mode)
	binary.BigEndian.PutUint16(sb[6:8], appTag)
	binary.BigEndian.PutUint32(sb[8:12], uint32(sectorSize))
	binary.BigEndian.PutUint32(sb[12:16], uint32(tupleSize))
	binary.BigEndian.PutUint64(sb[16:24], uint64(logicalSize))
	binary.BigEndian.PutUint64(sb[24:32], segID)
	crc := crc32.Checksum(sb[0:60], castagnoliTable)
	binary.BigEndian.PutUint32(sb[60:64], crc)
	return sb
}

func (d *ProtectedDisk) readAndVerifySuperblock(rf journal.File, name string) (superblock, error) {
	if rf.Size() < SuperblockSize {
		d.verifyFailCount.Add(1)
		return superblock{}, &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: name, LBA: 0,
			Detail: fmt.Sprintf("segment smaller than FS-PI superblock (%d < %d)", rf.Size(), SuperblockSize),
		}
	}
	var rawSB [SuperblockSize]byte
	if _, err := rf.ReadAt(rawSB[:], 0); err != nil {
		d.verifyFailCount.Add(1)
		return superblock{}, &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: name, LBA: 0,
			Detail: fmt.Sprintf("short superblock read: %v", err),
		}
	}
	if isAllZero(rawSB[:]) || string(rawSB[0:4]) != string(superblockMagic[:]) {
		d.verifyFailCount.Add(1)
		return superblock{}, &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: name, LBA: 0,
			Detail: "FS-PI superblock magic missing or zero-filled",
		}
	}
	gotCRC := binary.BigEndian.Uint32(rawSB[60:64])
	calcCRC := crc32.Checksum(rawSB[0:60], castagnoliTable)
	if gotCRC != calcCRC {
		d.verifyFailCount.Add(1)
		return superblock{}, &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: name, LBA: 0,
			Detail: fmt.Sprintf("FS-PI superblock CRC32C mismatch: got 0x%08x calc 0x%08x", gotCRC, calcCRC),
		}
	}
	sb := superblock{
		mode:        GuardMode(rawSB[5]),
		appTag:      binary.BigEndian.Uint16(rawSB[6:8]),
		sectorSize:  int(binary.BigEndian.Uint32(rawSB[8:12])),
		tupleSize:   int(binary.BigEndian.Uint32(rawSB[12:16])),
		logicalSize: int64(binary.BigEndian.Uint64(rawSB[16:24])),
		segmentID:   binary.BigEndian.Uint64(rawSB[24:32]),
	}
	if sb.appTag != d.appTag {
		d.verifyFailCount.Add(1)
		return superblock{}, &IntegrityError{
			Kind: ErrAppTagCrossVolume, Segment: name, LBA: 0,
			ExpectedApp: d.appTag, GotApp: sb.appTag,
			Detail: fmt.Sprintf("segment superblock belongs to another volume/tenant (0x%04x != 0x%04x)", sb.appTag, d.appTag),
		}
	}
	if sb.mode != d.mode || sb.sectorSize != d.sectorSize || sb.tupleSize != d.tupleSize {
		d.verifyFailCount.Add(1)
		return superblock{}, &IntegrityError{
			Kind: ErrMetadataTruncated, Segment: name, LBA: 0,
			Detail: fmt.Sprintf("segment superblock layout mismatch (mode=%s sec=%d tuple=%d)", sb.mode, sb.sectorSize, sb.tupleSize),
		}
	}
	return sb, nil
}

// ProtectedFile implements journal.File with per-sector FS-PI framing and
// pre-delivery integrity verification.
type ProtectedFile struct {
	mu          sync.RWMutex
	disk        *ProtectedDisk
	raw         journal.File
	name        string
	logicalSize int64
	frameSize   int
}

// RawFile returns the underlying journal.File (for below-encryption fault injection).
func (f *ProtectedFile) RawFile() journal.File { return f.raw }

// Name returns the segment filename.
func (f *ProtectedFile) Name() string { return f.name }

// PhysicalOffsets returns the physical byte offsets of sector data and its
// interleaved PI tuple for logical block address lba.
func (f *ProtectedFile) PhysicalOffsets(lba uint64) (dataOff, piOff int64) {
	if f.disk.mode == GuardNone {
		off := int64(lba) * int64(f.disk.sectorSize)
		return off, off
	}
	dataOff = int64(SuperblockSize) + int64(lba)*int64(f.frameSize)
	piOff = dataOff + int64(f.disk.sectorSize)
	return dataOff, piOff
}

// Size implements journal.File, returning the logical segment size.
func (f *ProtectedFile) Size() int64 { return f.logicalSize }

// Close implements journal.File.
func (f *ProtectedFile) Close() error { return f.raw.Close() }

// Sync implements journal.File.
func (f *ProtectedFile) Sync() error { return f.raw.Sync() }

// Writeback implements journal.File.
func (f *ProtectedFile) Writeback(off, n int64) error {
	if f.disk.mode == GuardNone {
		return f.raw.Writeback(off, n)
	}
	if n <= 0 {
		return nil
	}
	sec := int64(f.disk.sectorSize)
	startLBA := uint64(off / sec)
	endLBA := uint64((off + n - 1) / sec)
	physStart, _ := f.PhysicalOffsets(startLBA)
	physEnd, _ := f.PhysicalOffsets(endLBA + 1)
	return f.raw.Writeback(physStart, physEnd-physStart)
}

func nextEpochForMode(cur uint16, mode GuardMode) uint16 {
	next := cur + 1
	if next == 0 {
		next = 1
	}
	if mode == GuardT10DIF {
		low := next & 0xFF
		if low == 0 || low == 0xFF {
			next++
			if next == 0 || (next&0xFF) == 0 {
				next++
			}
		}
	}
	return next
}

// WriteAt implements journal.File, computing and storing per-sector FS-PI
// tuples alongside every modified logical sector.
func (f *ProtectedFile) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 || off+int64(len(p)) > f.logicalSize {
		return 0, io.ErrShortWrite
	}

	if f.disk.mode == GuardNone {
		n, err := f.raw.WriteAt(p, off)
		if n > 0 {
			f.disk.logicalBytesWritten.Add(uint64(n))
			f.disk.physicalBytesWritten.Add(uint64(n))
		}
		return n, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	secSize := f.disk.sectorSize
	tupleSize := f.disk.tupleSize
	var stackFrame [SectorSize4096 + Tuple128Size]byte
	frame := stackFrame[:f.frameSize]

	written := 0
	for written < len(p) {
		curOff := off + int64(written)
		lba := uint64(curOff / int64(secSize))
		inSec := int(curOff % int64(secSize))
		chunk := min(secSize-inSec, len(p)-written)
		physOff, _ := f.PhysicalOffsets(lba)

		curEpoch := f.disk.epochs.Get(f.name, lba)
		if inSec > 0 || chunk < secSize {
			f.disk.partialRMWCount.Add(1)
			if _, err := f.raw.ReadAt(frame, physOff); err != nil {
				return written, err
			}
			if curEpoch > 0 {
				t0 := time.Now()
				err := VerifyPI(f.name, f.disk.mode, f.disk.appTag, lba, curEpoch, frame[:secSize], frame[secSize:])
				f.disk.piVerifyNanos.Add(uint64(time.Since(t0).Nanoseconds()))
				if err != nil {
					f.disk.verifyFailCount.Add(1)
					return written, err
				}
				f.disk.verifyPassCount.Add(1)
			}
		} else {
			clear(frame)
		}

		copy(frame[inSec:inSec+chunk], p[written:written+chunk])
		nextEpoch := nextEpochForMode(curEpoch, f.disk.mode)

		t0 := time.Now()
		if err := EncodePI(frame[secSize:], f.disk.mode, f.disk.appTag, lba, nextEpoch, frame[:secSize]); err != nil {
			return written, err
		}
		f.disk.piGenerateNanos.Add(uint64(time.Since(t0).Nanoseconds()))

		if _, err := f.raw.WriteAt(frame, physOff); err != nil {
			return written, err
		}
		f.disk.epochs.Set(f.name, lba, nextEpoch)
		f.disk.sectorsWritten.Add(1)
		f.disk.logicalBytesWritten.Add(uint64(chunk))
		f.disk.physicalBytesWritten.Add(uint64(len(frame)))
		f.disk.piBytesWritten.Add(uint64(tupleSize))
		written += chunk
	}
	return written, nil
}

// ReadAt implements journal.File, verifying the FS-PI superblock and every
// spanned sector's Protection Information BEFORE returning any bytes to the
// caller. On any verification failure, p is zeroed so unverified bytes are
// never exposed.
func (f *ProtectedFile) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 || off >= f.logicalSize {
		return 0, io.EOF
	}
	toRead := len(p)
	var eofErr error
	if off+int64(toRead) > f.logicalSize {
		toRead = int(f.logicalSize - off)
		eofErr = io.EOF
	}

	if f.disk.mode == GuardNone {
		n, err := f.raw.ReadAt(p[:toRead], off)
		if err != nil {
			return n, err
		}
		return n, eofErr
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Re-verify the 64-byte FS-PI superblock whenever reading the head of a
	// segment so post-open header truncation/zero-fill fails closed immediately.
	if off == 0 {
		if _, err := f.disk.readAndVerifySuperblock(f.raw, f.name); err != nil {
			clear(p)
			return 0, err
		}
	}

	secSize := f.disk.sectorSize
	var stackFrame [SectorSize4096 + Tuple128Size]byte
	frame := stackFrame[:f.frameSize]
	done := 0
	for done < toRead {
		curOff := off + int64(done)
		lba := uint64(curOff / int64(secSize))
		inSec := int(curOff % int64(secSize))
		chunk := min(secSize-inSec, toRead-done)
		physOff, _ := f.PhysicalOffsets(lba)

		if _, err := f.raw.ReadAt(frame, physOff); err != nil {
			clear(p)
			f.disk.verifyFailCount.Add(1)
			return 0, &IntegrityError{
				Kind: ErrMetadataTruncated, Segment: f.name, LBA: lba,
				Detail: fmt.Sprintf("short sector read at physical offset %d: %v", physOff, err),
			}
		}
		f.disk.sectorsRead.Add(1)

		expEpoch := f.disk.epochs.Get(f.name, lba)
		data := frame[:secSize]
		pi := frame[secSize:]

		if expEpoch == 0 {
			if isAllZero(pi) {
				if !isAllZero(data) {
					clear(p)
					f.disk.verifyFailCount.Add(1)
					return 0, &IntegrityError{
						Kind: ErrMetadataTruncated, Segment: f.name, LBA: lba,
						Detail: "non-zero sector payload with zero PI tuple",
					}
				}
				// Cleanly unwritten zero sector.
				copy(p[done:done+chunk], data[inSec:inSec+chunk])
				done += chunk
				continue
			}
			// Cold recovery without an in-memory epoch entry: infer epoch from PI
			// while still enforcing AppTag, RefTag, and GuardTag verification!
			expEpoch = extractEpochFromPI(f.disk.mode, pi)
			if expEpoch == 0 {
				expEpoch = 1
			}
		}

		t0 := time.Now()
		err := VerifyPI(f.name, f.disk.mode, f.disk.appTag, lba, expEpoch, data, pi)
		f.disk.piVerifyNanos.Add(uint64(time.Since(t0).Nanoseconds()))
		if err != nil {
			clear(p)
			f.disk.verifyFailCount.Add(1)
			return 0, err
		}
		f.disk.verifyPassCount.Add(1)
		copy(p[done:done+chunk], data[inSec:inSec+chunk])
		done += chunk
	}
	return done, eofErr
}

func extractEpochFromPI(mode GuardMode, pi []byte) uint16 {
	switch mode {
	case GuardT10DIF:
		if len(pi) < Tuple64Size {
			return 0
		}
		_, ep8 := UnpackT10RefTag(binary.BigEndian.Uint32(pi[4:8]))
		return uint16(ep8)
	case GuardSoftwareStrong:
		if len(pi) < Tuple128Size {
			return 0
		}
		return binary.BigEndian.Uint16(pi[10:12])
	default:
		return 0
	}
}
