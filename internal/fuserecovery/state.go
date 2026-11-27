package fuserecovery

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateFormatVersion uint32 = 1
	stateHeaderSize           = 24
	maxStatePayload           = 64 << 20 // 64 MiB safety ceiling
	stateFileName             = "session_state.ananke"
	maxCompletedOps           = 4096
)

var (
	stateMagic  = [8]byte{'A', 'N', 'A', 'N', 'K', 'E', 'S', '1'}
	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
)

// CheckpointMode controls how eagerly daemon metadata transitions are flushed
// to the crash-safe state store. User file data on Fsync() ALWAYS executes a
// real BackingFile.Sync() regardless of mode.
type CheckpointMode int

const (
	// CheckpointSync persists every metadata mutation (Create/Open/Unlink/Release/Fsync)
	// with fsync + atomic rename + parent directory fsync (disk-durable state).
	CheckpointSync CheckpointMode = iota

	// CheckpointBuffered (Ananke process-crash mode) persists CRC32C-verified
	// state snapshots via atomic temp-file write + rename in host OS page cache
	// without per-op state-dir fsync (survives 100% of SIGKILL process crashes
	// while keeping real BackingFile.Sync() on every Fsync() barrier).
	CheckpointBuffered

	// CheckpointDisabled disables state persistence entirely (matched baseline
	// for measuring checkpoint overhead and cold-remount behavior).
	CheckpointDisabled
)

func (m CheckpointMode) String() string {
	switch m {
	case CheckpointSync:
		return "sync"
	case CheckpointBuffered:
		return "buffered"
	case CheckpointDisabled:
		return "disabled"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// InodeRecord tracks kernel-visible FUSE inode state across daemon restarts.
type InodeRecord struct {
	Inode         uint64 `json:"inode"`
	Path          string `json:"path,omitempty"`
	OrphanRelPath string `json:"orphan_rel_path,omitempty"`
	Generation    uint64 `json:"generation"`
	NLookup       uint64 `json:"nlookup"`
	Unlinked      bool   `json:"unlinked"`
	Deleted       bool   `json:"deleted"`
	OwnerTenant   string `json:"owner_tenant"`
	Mode          uint32 `json:"mode"`
	Size          uint64 `json:"size"`
}

// HandleRecord tracks an open FUSE file handle (fh) across daemon restarts.
type HandleRecord struct {
	Handle         uint64 `json:"handle"`
	Inode          uint64 `json:"inode"`
	InodeGen       uint64 `json:"inode_gen"`
	Flags          uint32 `json:"flags"`
	Offset         int64  `json:"offset"`
	Generation     uint64 `json:"generation"`
	OwnerTenant    string `json:"owner_tenant"`
	DirtyUnflushed bool   `json:"dirty_unflushed"`
	LastFsyncSeq   uint64 `json:"last_fsync_seq"`
}

// FsyncBarrier records a locally acknowledged fsync() for an inode.
type FsyncBarrier struct {
	Seq           uint64 `json:"seq"`
	Inode         uint64 `json:"inode"`
	InodeGen      uint64 `json:"inode_gen"`
	DurableSize   uint64 `json:"durable_size"`
	ContentCRC32C uint32 `json:"content_crc32c"`
	AckedAtNano   int64  `json:"acked_at_nano"`
}

// OpReceipt records the outcome of a non-idempotent FUSE operation keyed by
// FUSE request unique ID so retried requests after crash recovery replay
// idempotently without double-applying mutations.
type OpReceipt struct {
	ReqID        uint64 `json:"req_id"`
	OpType       string `json:"op_type"`
	Inode        uint64 `json:"inode,omitempty"`
	InodeGen     uint64 `json:"inode_gen,omitempty"`
	Handle       uint64 `json:"handle,omitempty"`
	BytesWritten int    `json:"bytes_written,omitempty"`
	WriteOffset  int64  `json:"write_offset,omitempty"`
	FsyncSeq     uint64 `json:"fsync_seq,omitempty"`
	Generation   uint64 `json:"generation"`
}

// SessionState is the complete serializable FUSE daemon session snapshot
// persisted by StateStore.
type SessionState struct {
	FormatVersion uint32                   `json:"format_version"`
	SessionID     string                   `json:"session_id"`
	MountPath     string                   `json:"mount_path"`
	Generation    uint64                   `json:"generation"`
	UniqueReqID   uint64                   `json:"unique_req_id"`
	NextInode     uint64                   `json:"next_inode"`
	NextHandle    uint64                   `json:"next_handle"`
	LastFsyncSeq  uint64                   `json:"last_fsync_seq"`
	Inodes        map[uint64]*InodeRecord  `json:"inodes"`
	Handles       map[uint64]*HandleRecord `json:"handles"`
	FsyncBarriers map[uint64]FsyncBarrier  `json:"fsync_barriers"`
	CompletedOps  map[uint64]OpReceipt     `json:"completed_ops"`
}

// NewSessionState initializes an empty SessionState with RootInode = 1.
func NewSessionState(sessionID, mountPath string) *SessionState {
	return &SessionState{
		FormatVersion: stateFormatVersion,
		SessionID:     sessionID,
		MountPath:     mountPath,
		Generation:    1,
		NextInode:     2, // inode 1 is reserved for FUSE root
		NextHandle:    1,
		Inodes:        make(map[uint64]*InodeRecord),
		Handles:       make(map[uint64]*HandleRecord),
		FsyncBarriers: make(map[uint64]FsyncBarrier),
		CompletedOps:  make(map[uint64]OpReceipt),
	}
}

// Clone returns a deep copy safe for lock-free serialization.
func (s *SessionState) Clone() *SessionState {
	cp := &SessionState{
		FormatVersion: s.FormatVersion,
		SessionID:     s.SessionID,
		MountPath:     s.MountPath,
		Generation:    s.Generation,
		UniqueReqID:   s.UniqueReqID,
		NextInode:     s.NextInode,
		NextHandle:    s.NextHandle,
		LastFsyncSeq:  s.LastFsyncSeq,
		Inodes:        make(map[uint64]*InodeRecord, len(s.Inodes)),
		Handles:       make(map[uint64]*HandleRecord, len(s.Handles)),
		FsyncBarriers: make(map[uint64]FsyncBarrier, len(s.FsyncBarriers)),
		CompletedOps:  make(map[uint64]OpReceipt, len(s.CompletedOps)),
	}
	for k, v := range s.Inodes {
		rec := *v
		cp.Inodes[k] = &rec
	}
	for k, v := range s.Handles {
		rec := *v
		cp.Handles[k] = &rec
	}
	for k, v := range s.FsyncBarriers {
		cp.FsyncBarriers[k] = v
	}
	for k, v := range s.CompletedOps {
		cp.CompletedOps[k] = v
	}
	return cp
}

// RecordOp stores a completed non-idempotent op receipt and advances the
// UniqueReqID watermark while keeping CompletedOps bounded.
func (s *SessionState) RecordOp(r OpReceipt) {
	if r.ReqID == 0 {
		return
	}
	if r.ReqID > s.UniqueReqID {
		s.UniqueReqID = r.ReqID
	}
	s.CompletedOps[r.ReqID] = r
	if len(s.CompletedOps) > maxCompletedOps {
		keys := make([]uint64, 0, len(s.CompletedOps))
		for k := range s.CompletedOps {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		prune := len(keys) - maxCompletedOps
		for i := 0; i < prune; i++ {
			delete(s.CompletedOps, keys[i])
		}
	}
}

// StateStore persists and loads CRC32C-verified SessionState snapshots inside
// a symlink-confined state directory.
type StateStore struct {
	dir     string
	mu      sync.Mutex
	tmpSeq  atomic.Uint64
	saves   atomic.Uint64
	bytesWr atomic.Uint64
}

// NewStateStore validates dir confinement and returns a crash-safe StateStore.
func NewStateStore(dir string) (*StateStore, error) {
	if err := ValidateStateDir(dir); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("abs state dir %q: %w", dir, err)
	}
	return &StateStore{dir: abs}, nil
}

// Dir returns the confined state directory path.
func (st *StateStore) Dir() string { return st.dir }

// StatePath returns the confined path to the active state file.
func (st *StateStore) StatePath() (string, error) {
	return ConfinePath(st.dir, stateFileName)
}

// Stats returns total checkpoints saved and total bytes persisted.
func (st *StateStore) Stats() (saves uint64, bytesWritten uint64) {
	return st.saves.Load(), st.bytesWr.Load()
}

// Save atomically persists snap with a 24-byte header and Castagnoli CRC32C.
// When durable is true, the temp file is opened with O_SYNC, fsynced before
// close, atomically renamed over the active state file, and the parent state
// directory is fsynced.
func (st *StateStore) Save(snap *SessionState, durable bool) error {
	if snap == nil {
		return fmt.Errorf("%w: nil session state", ErrSessionCorrupt)
	}
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := ValidateStateDir(st.dir); err != nil {
		return err
	}
	targetPath, err := ConfinePath(st.dir, stateFileName)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal session state: %w", err)
	}
	if len(payload) > maxStatePayload {
		return fmt.Errorf("%w: payload size %d exceeds limit %d", ErrSessionCorrupt, len(payload), maxStatePayload)
	}

	frame := encodeStateFrame(payload)

	seq := st.tmpSeq.Add(1)
	tmpName := fmt.Sprintf(".state.%d.%d.tmp", os.Getpid(), seq)
	tmpPath, err := ConfinePath(st.dir, tmpName)
	if err != nil {
		return err
	}

	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY
	f, err := os.OpenFile(tmpPath, flags, 0o600)
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}

	cleanup := func() {
		_ = os.Remove(tmpPath)
	}

	if _, err := f.Write(frame); err != nil {
		_ = f.Close()
		cleanup()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if durable {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			cleanup()
			return fmt.Errorf("fsync temp state file: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close temp state file: %w", err)
	}

	// Re-verify target path has not been swapped for a symlink before rename.
	if _, err := ConfinePath(st.dir, stateFileName); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		cleanup()
		return fmt.Errorf("atomic rename state file: %w", err)
	}
	if durable {
		if err := SyncDir(st.dir); err != nil {
			return err
		}
	}

	st.saves.Add(1)
	st.bytesWr.Add(uint64(len(frame)))
	return nil
}

// Load reads and CRC32C-verifies the persisted SessionState from the confined
// state directory. Returns ErrNotFound if no checkpoint exists yet, or
// ErrSessionCorrupt if magic, version, length, CRC32C, or JSON decoding fails.
func (st *StateStore) Load() (*SessionState, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := ValidateStateDir(st.dir); err != nil {
		return nil, err
	}
	targetPath, err := ConfinePath(st.dir, stateFileName)
	if err != nil {
		return nil, err
	}

	raw, err := os.ReadFile(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("read state file: %w", err)
	}

	payload, err := decodeStateFrame(raw)
	if err != nil {
		return nil, err
	}

	var snap SessionState
	if err := json.Unmarshal(payload, &snap); err != nil {
		return nil, fmt.Errorf("%w: json decode: %v", ErrSessionCorrupt, err)
	}
	if snap.FormatVersion != stateFormatVersion {
		return nil, fmt.Errorf("%w: unsupported format version %d", ErrSessionCorrupt, snap.FormatVersion)
	}
	if snap.Inodes == nil {
		snap.Inodes = make(map[uint64]*InodeRecord)
	}
	if snap.Handles == nil {
		snap.Handles = make(map[uint64]*HandleRecord)
	}
	if snap.FsyncBarriers == nil {
		snap.FsyncBarriers = make(map[uint64]FsyncBarrier)
	}
	if snap.CompletedOps == nil {
		snap.CompletedOps = make(map[uint64]OpReceipt)
	}
	return &snap, nil
}

// encodeStateFrame builds [magic(8) | version(4) | flags(4) | len(4) | crc32c(4) | payload].
func encodeStateFrame(payload []byte) []byte {
	buf := make([]byte, stateHeaderSize+len(payload))
	copy(buf[0:8], stateMagic[:])
	binary.BigEndian.PutUint32(buf[8:12], stateFormatVersion)
	binary.BigEndian.PutUint32(buf[12:16], 0)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(payload)))
	copy(buf[stateHeaderSize:], payload)

	h := crc32.New(crc32cTable)
	_, _ = h.Write(buf[0:20])
	_, _ = h.Write(payload)
	binary.BigEndian.PutUint32(buf[20:24], h.Sum32())
	return buf
}

// decodeStateFrame validates the header and Castagnoli CRC32C checksum.
func decodeStateFrame(raw []byte) ([]byte, error) {
	if len(raw) < stateHeaderSize {
		return nil, fmt.Errorf("%w: truncated frame (%d bytes)", ErrSessionCorrupt, len(raw))
	}
	if !bytes.Equal(raw[0:8], stateMagic[:]) {
		return nil, fmt.Errorf("%w: bad magic %x", ErrSessionCorrupt, raw[0:8])
	}
	ver := binary.BigEndian.Uint32(raw[8:12])
	if ver != stateFormatVersion {
		return nil, fmt.Errorf("%w: unsupported frame version %d", ErrSessionCorrupt, ver)
	}
	payloadLen := binary.BigEndian.Uint32(raw[16:20])
	if payloadLen > maxStatePayload {
		return nil, fmt.Errorf("%w: declared length %d exceeds ceiling", ErrSessionCorrupt, payloadLen)
	}
	if int(payloadLen) != len(raw)-stateHeaderSize {
		return nil, fmt.Errorf("%w: length mismatch (header=%d actual=%d)", ErrSessionCorrupt, payloadLen, len(raw)-stateHeaderSize)
	}
	wantCRC := binary.BigEndian.Uint32(raw[20:24])
	payload := raw[stateHeaderSize:]

	h := crc32.New(crc32cTable)
	_, _ = h.Write(raw[0:20])
	_, _ = h.Write(payload)
	if gotCRC := h.Sum32(); gotCRC != wantCRC {
		return nil, fmt.Errorf("%w: crc32c mismatch (got=%#08x want=%#08x)", ErrSessionCorrupt, gotCRC, wantCRC)
	}
	return payload, nil
}

// ContentCRC32C computes the Castagnoli CRC32C of file contents for fsync
// barrier verification.
func ContentCRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// nowUnixNano is a package-level clock hook for deterministic testing if needed.
var nowUnixNano = func() int64 { return time.Now().UnixNano() }
