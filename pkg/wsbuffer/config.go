package wsbuffer

import (
	"errors"
	"fmt"

	"github.com/hanzoai/vfs/journal"
)

const (
	// DefaultPageSize is the standard 4 KiB filesystem and NVMe page size.
	DefaultPageSize = 4096
	// DefaultSlabSize is the size of each pooled aligned scrap slab (256 KiB).
	DefaultSlabSize = 256 << 10
	// DefaultBypassThreshold is the minimum size for aligned zero-copy direct bypass (64 KiB).
	DefaultBypassThreshold = 64 << 10
	// DefaultMaxScrapBytes is the default per-store bounded scrap memory cap (4 MiB).
	DefaultMaxScrapBytes = 4 << 20
	// DefaultJournalCapacity is the default journal capacity when WALModeJournal is used.
	DefaultJournalCapacity = 64 << 20
	// DefaultJournalSegment is the default journal segment size when WALModeJournal is used.
	DefaultJournalSegment = 8 << 20
	// DefaultMaxFrameBytes caps a single encoded WAL scrap-batch frame payload.
	DefaultMaxFrameBytes = 2 << 20
)

var (
	// ErrClosed is returned when operating on a closed Store or Manager.
	ErrClosed = errors.New("wsbuffer: closed")
	// ErrInvalidConfig is returned when Config fails validation.
	ErrInvalidConfig = errors.New("wsbuffer: invalid config")
	// ErrInvalidRange is returned when an offset or length is negative or overflows.
	ErrInvalidRange = errors.New("wsbuffer: invalid offset or length")
	// ErrCorrupt is returned when on-disk WAL or checkpoint data is malformed.
	ErrCorrupt = errors.New("wsbuffer: corrupt frame")
	// ErrChecksumMismatch is returned when a WAL frame header or payload CRC32C fails.
	ErrChecksumMismatch = errors.New("wsbuffer: checksum mismatch")
	// ErrVolumeMismatch is returned when a WAL frame belongs to a different VolumeID.
	ErrVolumeMismatch = errors.New("wsbuffer: volume ID mismatch")
	// ErrTruncated is returned internally when a WAL frame is cut short at EOF.
	ErrTruncated = errors.New("wsbuffer: truncated frame")
	// ErrQuotaExceeded is returned when a volume exceeds a strict quota that cannot be flushed.
	ErrQuotaExceeded = errors.New("wsbuffer: scrap memory quota exceeded")
)

// WALMode selects the crash-recovery log backend used by a Store.
type WALMode int

const (
	// WALModeJournal logs scrap frames and direct barriers through Hanzo VFS's
	// production append-only journal (github.com/hanzoai/vfs/journal).
	WALModeJournal WALMode = iota
	// WALModeFile logs frames to a standalone crash-safe WAL file in dir.
	WALModeFile
	// WALModeNone disables WAL logging (backing file only; useful for ablation benchmarks).
	WALModeNone
)

// String returns a human-readable name for WALMode.
func (m WALMode) String() string {
	switch m {
	case WALModeJournal:
		return "journal"
	case WALModeFile:
		return "file"
	case WALModeNone:
		return "none"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// Config configures a WSBuffer Store.
type Config struct {
	// PageSize is the alignment granularity in bytes (power of 2, >= 512).
	// Defaults to DefaultPageSize (4096).
	PageSize int
	// SlabSize is the size of each scrap buffer slab in bytes (multiple of PageSize).
	// Defaults to DefaultSlabSize (256 KiB).
	SlabSize int
	// BypassThreshold is the minimum byte length for aligned writes to bypass
	// the scrap buffer with zero copy (multiple of PageSize).
	// Defaults to DefaultBypassThreshold (64 KiB).
	BypassThreshold int
	// MaxScrapBytes bounds live dirty bytes buffered in scrap slabs before an
	// automatic backpressure flush is triggered. Defaults to DefaultMaxScrapBytes (4 MiB).
	MaxScrapBytes int64
	// MaxFrameBytes bounds the payload size of a single WAL scrap-batch frame.
	// Defaults to DefaultMaxFrameBytes (2 MiB).
	MaxFrameBytes int
	// VolumeID tags every WAL frame for multi-tenant isolation.
	VolumeID uint64
	// BackingFile overrides the path to the backing data file.
	// Defaults to <dir>/data.bin.
	BackingFile string
	// PreallocBytes pre-writes zeros up to this size on creation of a new
	// backing file so overwrites hit allocated blocks cleanly.
	PreallocBytes int64
	// WALMode selects WALModeJournal (default), WALModeFile, or WALModeNone.
	WALMode WALMode
	// StrictRecovery causes Open to return ErrCorrupt / ErrChecksumMismatch on
	// any corrupt trailing WAL frame instead of silently truncating a torn tail.
	// Default false (standard crash-recovery truncates torn tail frames).
	StrictRecovery bool
	// DisableAutoCheckpoint disables automatic WAL checkpointing/reclamation on Sync.
	// By default (false), Sync checkpoints the WAL after backing.Sync() succeeds.
	DisableAutoCheckpoint bool
	// JournalCapacity sets journal.Options.Capacity when WALMode == WALModeJournal.
	JournalCapacity int64
	// JournalSegment sets journal.Options.Segment when WALMode == WALModeJournal.
	JournalSegment int64
	// JournalLineage sets journal.Options.Lineage when WALMode == WALModeJournal.
	JournalLineage []byte
	// JournalDisk optionally wraps or replaces journal.Files(dir) for testing or
	// I/O instrumentation.
	JournalDisk func(dir string) (journal.Disk, error)
}

func (c Config) withDefaults() (Config, error) {
	if c.PageSize == 0 {
		c.PageSize = DefaultPageSize
	}
	if c.PageSize < 512 || (c.PageSize&(c.PageSize-1)) != 0 {
		return c, fmt.Errorf("%w: PageSize %d must be a power of two >= 512", ErrInvalidConfig, c.PageSize)
	}
	if c.MaxScrapBytes == 0 {
		c.MaxScrapBytes = DefaultMaxScrapBytes
	}
	if c.MaxScrapBytes < int64(c.PageSize) {
		return c, fmt.Errorf("%w: MaxScrapBytes %d must be >= PageSize %d", ErrInvalidConfig, c.MaxScrapBytes, c.PageSize)
	}
	if c.SlabSize == 0 {
		c.SlabSize = DefaultSlabSize
		if int64(c.SlabSize) > c.MaxScrapBytes {
			c.SlabSize = int((c.MaxScrapBytes / int64(c.PageSize)) * int64(c.PageSize))
		}
	}
	if c.SlabSize < c.PageSize || c.SlabSize%c.PageSize != 0 {
		return c, fmt.Errorf("%w: SlabSize %d must be a positive multiple of PageSize %d", ErrInvalidConfig, c.SlabSize, c.PageSize)
	}
	if c.BypassThreshold == 0 {
		c.BypassThreshold = DefaultBypassThreshold
	}
	if c.BypassThreshold < c.PageSize || c.BypassThreshold%c.PageSize != 0 {
		return c, fmt.Errorf("%w: BypassThreshold %d must be a positive multiple of PageSize %d", ErrInvalidConfig, c.BypassThreshold, c.PageSize)
	}
	if c.MaxFrameBytes == 0 {
		c.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if c.MaxFrameBytes < c.PageSize {
		return c, fmt.Errorf("%w: MaxFrameBytes %d must be >= PageSize %d", ErrInvalidConfig, c.MaxFrameBytes, c.PageSize)
	}
	if c.PreallocBytes < 0 {
		return c, fmt.Errorf("%w: PreallocBytes %d must be >= 0", ErrInvalidConfig, c.PreallocBytes)
	}
	if c.WALMode == WALModeJournal {
		if c.JournalSegment == 0 {
			c.JournalSegment = DefaultJournalSegment
		}
		if c.JournalCapacity == 0 {
			c.JournalCapacity = DefaultJournalCapacity
		}
		if c.JournalCapacity < 2*c.JournalSegment {
			c.JournalCapacity = 2 * c.JournalSegment
		}
		// Ensure a scrap frame always fits cleanly inside a single journal segment.
		maxAllowed := int(c.JournalSegment - 4096)
		if maxAllowed < c.PageSize {
			return c, fmt.Errorf("%w: JournalSegment %d is too small", ErrInvalidConfig, c.JournalSegment)
		}
		if c.MaxFrameBytes > maxAllowed {
			c.MaxFrameBytes = maxAllowed
		}
	}
	return c, nil
}

// Stats captures logical and physical I/O metrics for a Store.
type Stats struct {
	LogicalWrites         uint64 `json:"logical_writes"`
	LogicalBytes          uint64 `json:"logical_bytes"`
	ScrapWrites           uint64 `json:"scrap_writes"`
	ScrapBytes            uint64 `json:"scrap_bytes"`
	BypassWrites          uint64 `json:"bypass_writes"`
	BypassBytes           uint64 `json:"bypass_bytes"`
	CoalescedWrites       uint64 `json:"coalesced_writes"`
	CoalescedBytes        uint64 `json:"coalesced_bytes"`
	BypassSupersededBytes uint64 `json:"bypass_superseded_bytes"`
	ScrapFlushes          uint64 `json:"scrap_flushes"`
	BackpressureFlushes   uint64 `json:"backpressure_flushes"`
	DirectBarriers        uint64 `json:"direct_barriers"`
	SyncCalls             uint64 `json:"sync_calls"`
	ReadCalls             uint64 `json:"read_calls"`
	ReadScrapHits         uint64 `json:"read_scrap_hits"`
	CurrentScrapBytes     int64  `json:"current_scrap_bytes"`

	WALPhysicalWrites     uint64 `json:"wal_physical_writes"`
	WALPhysicalBytes      uint64 `json:"wal_physical_bytes"`
	WALSyncs              uint64 `json:"wal_syncs"`
	BackingPhysicalWrites uint64 `json:"backing_physical_writes"`
	BackingPhysicalBytes  uint64 `json:"backing_physical_bytes"`
	BackingSyncs          uint64 `json:"backing_syncs"`
}

// TotalPhysicalBytes returns total physical bytes written across WAL and backing storage.
func (s Stats) TotalPhysicalBytes() uint64 {
	return s.WALPhysicalBytes + s.BackingPhysicalBytes
}

// WAF returns the total write amplification factor (physical bytes / logical bytes).
func (s Stats) WAF() float64 {
	if s.LogicalBytes == 0 {
		return 0
	}
	return float64(s.TotalPhysicalBytes()) / float64(s.LogicalBytes)
}

// WALWAF returns the WAL-only write amplification factor.
func (s Stats) WALWAF() float64 {
	if s.LogicalBytes == 0 {
		return 0
	}
	return float64(s.WALPhysicalBytes) / float64(s.LogicalBytes)
}

// BackingWAF returns the backing-file-only write amplification factor.
func (s Stats) BackingWAF() float64 {
	if s.LogicalBytes == 0 {
		return 0
	}
	return float64(s.BackingPhysicalBytes) / float64(s.LogicalBytes)
}

// RecoveryReport summarizes what Open recovered from the WAL.
type RecoveryReport struct {
	FramesReplayed           uint64 `json:"frames_replayed"`
	ExtentsApplied           uint64 `json:"extents_applied"`
	BytesApplied             uint64 `json:"bytes_applied"`
	BarriersApplied          uint64 `json:"barriers_applied"`
	SupersededExtentsSkipped uint64 `json:"superseded_extents_skipped"`
	CheckpointsSeen          uint64 `json:"checkpoints_seen"`
	TruncatedTailBytes       int64  `json:"truncated_tail_bytes"`
	HighestSeq               uint64 `json:"highest_seq"`
}
