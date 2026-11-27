// Package writeback defines the FUSE writeback cache configuration, formal
// write-acknowledgement durability contract, dirty page-cache exposure bounds,
// and mandatory lifecycle flush barriers (Detach, Suspend, End) for hanzoai/vfs
// (HIP-1342, issue #13).
//
// Why this contract exists:
//
// When Linux FUSE writeback caching (FUSE_WRITEBACK_CACHE) is enabled, the
// kernel acknowledges buffered write(2) calls into its page cache BEFORE the
// FUSE daemon ever receives a FUSE_WRITE RPC. Consequently:
//
//  1. A buffered write(2) acknowledgement provides ZERO durability across a
//     daemon SIGKILL or host crash — those pages live only in volatile kernel
//     memory until flushed.
//  2. The 5-second object-store publication bound MUST NOT be measured from
//     the application's buffered write(2) return. It starts strictly at the
//     daemon journal entry / POSIX durability boundary (fsync(2), fdatasync(2),
//     or syncfs(2)), which forces the kernel to deliver dirty pages to the
//     daemon and wait for the local NVMe journal sync.
//  3. Because Linux defaults to up to 30 seconds of dirty page-cache exposure
//     (vm.dirty_expire_centisecs = 3000, vm.dirty_ratio = 20%), unbounded
//     writeback accumulation causes severe tail stalls during lifecycle
//     transitions and enlarges the crash-loss window. WritebackConfig bounds
//     dirty exposure and enforces a mandatory syncfs -> journal sync -> final
//     publish barrier on Detach, Suspend, and End.
//  4. Clean close(2) (FUSE_FLUSH) MUST issue zero synchronous object-store
//     requests: durability is local NVMe journal speed, and publication to S3
//     proceeds asynchronously within PublishBound (default 5 s).
package writeback

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultMaxWriteBytes is the target FUSE max_write payload size (1 MiB),
	// requiring Linux FUSE_MAX_PAGES (256 * 4 KiB pages) so the kernel coalesces
	// sequential page-cache writeback into 1 MiB FUSE_WRITE RPCs instead of the
	// legacy 128 KiB (32-page) cap.
	DefaultMaxWriteBytes uint32 = 1 << 20

	// LegacyMaxWriteBytes is the legacy FUSE max_write cap (128 KiB, 32 pages)
	// used by older FUSE clients that do not negotiate FUSE_MAX_PAGES.
	LegacyMaxWriteBytes uint32 = 128 << 10

	// MinMaxWriteBytes is the minimum legal FUSE max_write size (4 KiB, 1 page).
	MinMaxWriteBytes uint32 = 4 << 10

	// DefaultDirtyWindowBytes caps the un-flushed kernel page-cache exposure
	// before the dirty guard requests a background syncfs, bounding Detach and
	// Suspend drain latency.
	DefaultDirtyWindowBytes int64 = 64 << 20

	// DefaultDirtyExpireDuration caps how long buffered writes may sit in the
	// kernel page cache before a periodic syncfs flushes them into the daemon
	// journal (compared to Linux's 30 s default vm.dirty_expire_centisecs).
	DefaultDirtyExpireDuration = 1 * time.Second

	// DefaultPublishBound is the SLA for publishing acknowledged durable writes
	// from daemon journal entry / fsync to the object-store Head.
	DefaultPublishBound = 5 * time.Second
)

var (
	// ErrInvalidConfig is returned when WritebackConfig violates required bounds.
	ErrInvalidConfig = errors.New("writeback: invalid configuration")

	// ErrInvalidTransition is returned when a write record attempts to move
	// backwards or skip mandatory contract boundaries.
	ErrInvalidTransition = errors.New("writeback: invalid durability state transition")

	// ErrSyncS3OnCloseViolation is returned when a clean close(2) triggers one
	// or more synchronous object-store PUT requests.
	ErrSyncS3OnCloseViolation = errors.New("writeback: clean close issued synchronous object-store PUT(s)")

	// ErrPublishBoundExceeded is returned when an acknowledged durable write
	// exceeded PublishBound between journal entry / fsync and object-store Head
	// publication.
	ErrPublishBoundExceeded = errors.New("writeback: publication exceeded bound from journal entry")

	// ErrRecordNotFound is returned when referencing an unknown write record ID.
	ErrRecordNotFound = errors.New("writeback: write record not found")
)

// DurabilityState models the exact progression of a write under FUSE writeback
// caching. Only states >= FsyncDurable satisfy POSIX durability and survive a
// daemon SIGKILL + remount without data loss.
type DurabilityState uint8

const (
	// BufferedInKernelPageCache means write(2) returned to the application after
	// copying bytes into the Linux kernel page cache, BEFORE the daemon has seen
	// a FUSE_WRITE RPC. This state is VOLATILE: a daemon SIGKILL or crash drops
	// these bytes, and the 5 s publication clock MUST NOT start here.
	BufferedInKernelPageCache DurabilityState = iota

	// JournaledInDaemon means the kernel delivered FUSE_WRITE to the daemon and
	// the daemon applied the change and appended it to the local WAL (journal
	// watermark Visible). The publication SLA clock starts at this journal entry.
	JournaledInDaemon

	// FsyncDurable means fsync(2), fdatasync(2), or syncfs(2) flushed dirty
	// pages to the daemon and committed the journal record to local NVMe media
	// (journal watermark Durable). This is the POSIX durability boundary: 100%
	// of bytes at or below this watermark survive an immediate daemon SIGKILL
	// and remount.
	FsyncDurable

	// PublishedToObjectStore means the Volume publisher committed an object-store
	// Head carrying this write's journal sequence (journal watermark Published),
	// within PublishBound (<= 5 s) measured from JournaledInDaemon/FsyncDurable.
	PublishedToObjectStore
)

// String returns the canonical human-readable state name.
func (s DurabilityState) String() string {
	switch s {
	case BufferedInKernelPageCache:
		return "BufferedInKernelPageCache"
	case JournaledInDaemon:
		return "JournaledInDaemon"
	case FsyncDurable:
		return "FsyncDurable"
	case PublishedToObjectStore:
		return "PublishedToObjectStore"
	default:
		return fmt.Sprintf("DurabilityState(%d)", uint8(s))
	}
}

// SurvivesDaemonSIGKILL reports whether a write in state s is guaranteed by
// contract to survive an abrupt daemon SIGKILL followed by remount on the same
// node's journal.
func (s DurabilityState) SurvivesDaemonSIGKILL() bool {
	return s >= FsyncDurable
}

// StartsPublishClock reports whether the 5 s object-store publication clock is
// active in state s. Buffered kernel writes NEVER start the publication clock.
func (s DurabilityState) StartsPublishClock() bool {
	return s >= JournaledInDaemon
}

// WritebackConfig controls FUSE writeback caching, max_write negotiation,
// dirty page-cache exposure bounds, and lifecycle flush barriers.
type WritebackConfig struct {
	// WritebackCache enables Linux FUSE_WRITEBACK_CACHE so buffered writes are
	// absorbed in the kernel page cache and coalesced up to MaxWriteBytes.
	WritebackCache bool `json:"writeback_cache"`

	// MaxWriteBytes is the maximum FUSE_WRITE payload negotiated via FUSE_INIT
	// (default 1 MiB = 1<<20, requiring FUSE_MAX_PAGES = 256).
	MaxWriteBytes uint32 `json:"max_write_bytes"`

	// DirtyWindowBytes bounds un-flushed kernel dirty page-cache exposure before
	// proactive syncfs drain is triggered (default 64 MiB).
	DirtyWindowBytes int64 `json:"dirty_window_bytes"`

	// DirtyExpireDuration bounds the age of kernel-buffered dirty pages before
	// proactive syncfs drain (default 1 s, bounding Linux's 30 s default).
	DirtyExpireDuration time.Duration `json:"dirty_expire_duration"`

	// PublishBound is the maximum allowed latency from daemon journal entry /
	// fsync to object-store Head publication (default 5 s).
	PublishBound time.Duration `json:"publish_bound"`

	// SyncFSOnLifecycle requires forcing kernel syncfs(mountFD) before journal
	// sync and final publication on Detach, Suspend, and End (default true).
	SyncFSOnLifecycle bool `json:"syncfs_on_lifecycle"`

	// ZeroSyncS3OnClose enforces the invariant that clean close(2) (FUSE_FLUSH)
	// performs zero synchronous object-store requests (default true).
	ZeroSyncS3OnClose bool `json:"zero_sync_s3_on_close"`
}

// DefaultConfig returns the production HIP-1342 #13 writeback configuration:
// 1 MiB max_write, FUSE writeback cache enabled, 64 MiB / 1 s dirty page-cache
// exposure guard, 5 s publication bound from journal entry, mandatory syncfs on
// Detach/Suspend/End, and zero synchronous S3 PUTs on clean close.
func DefaultConfig() WritebackConfig {
	return WritebackConfig{
		WritebackCache:      true,
		MaxWriteBytes:       DefaultMaxWriteBytes,
		DirtyWindowBytes:    DefaultDirtyWindowBytes,
		DirtyExpireDuration: DefaultDirtyExpireDuration,
		PublishBound:        DefaultPublishBound,
		SyncFSOnLifecycle:   true,
		ZeroSyncS3OnClose:   true,
	}
}

// Validate normalizes zero-valued defaults where appropriate and verifies all
// configuration invariants.
func (c WritebackConfig) Validate() (WritebackConfig, error) {
	out := c
	if out.MaxWriteBytes == 0 {
		out.MaxWriteBytes = DefaultMaxWriteBytes
	}
	if out.MaxWriteBytes < MinMaxWriteBytes || out.MaxWriteBytes > DefaultMaxWriteBytes {
		return out, fmt.Errorf("%w: max_write_bytes %d must be in [%d, %d]", ErrInvalidConfig, out.MaxWriteBytes, MinMaxWriteBytes, DefaultMaxWriteBytes)
	}
	if out.MaxWriteBytes%MinMaxWriteBytes != 0 {
		return out, fmt.Errorf("%w: max_write_bytes %d must be a multiple of %d", ErrInvalidConfig, out.MaxWriteBytes, MinMaxWriteBytes)
	}
	if out.DirtyWindowBytes == 0 {
		out.DirtyWindowBytes = DefaultDirtyWindowBytes
	}
	if out.DirtyWindowBytes < int64(out.MaxWriteBytes) {
		return out, fmt.Errorf("%w: dirty_window_bytes %d must be >= max_write_bytes %d", ErrInvalidConfig, out.DirtyWindowBytes, out.MaxWriteBytes)
	}
	if out.DirtyExpireDuration == 0 {
		out.DirtyExpireDuration = DefaultDirtyExpireDuration
	}
	if out.DirtyExpireDuration < 0 {
		return out, fmt.Errorf("%w: dirty_expire_duration must be > 0", ErrInvalidConfig)
	}
	if out.PublishBound == 0 {
		out.PublishBound = DefaultPublishBound
	}
	if out.PublishBound < 0 {
		return out, fmt.Errorf("%w: publish_bound must be > 0", ErrInvalidConfig)
	}
	return out, nil
}

// MaxPages returns the FUSE_INIT max_pages count corresponding to MaxWriteBytes
// for a 4 KiB page size (256 for 1 MiB, 32 for 128 KiB).
func (c WritebackConfig) MaxPages() uint16 {
	mw := c.MaxWriteBytes
	if mw == 0 {
		mw = DefaultMaxWriteBytes
	}
	return uint16(mw / MinMaxWriteBytes)
}

// WriteRecord captures the lifecycle timestamps and durability state of a
// single tracked write operation.
type WriteRecord struct {
	ID          uint64          `json:"id"`
	Inode       uint64          `json:"inode"`
	Offset      int64           `json:"offset"`
	Length      int64           `json:"length"`
	State       DurabilityState `json:"state"`
	JournalSeq  uint64          `json:"journal_seq,omitempty"`
	AppAckAt    time.Time       `json:"app_ack_at"`
	JournalAt   time.Time       `json:"journal_at,omitempty"`
	FsyncAt     time.Time       `json:"fsync_at,omitempty"`
	PublishedAt time.Time       `json:"published_at,omitempty"`
}

// PublishLatencyFromJournal returns the elapsed time between daemon journal
// entry and object-store Head publication (the contract's measured 5 s bound).
func (r WriteRecord) PublishLatencyFromJournal() time.Duration {
	if r.State < PublishedToObjectStore || r.JournalAt.IsZero() || r.PublishedAt.IsZero() {
		return 0
	}
	return r.PublishedAt.Sub(r.JournalAt)
}

// PublishLatencyFromFsync returns the elapsed time between POSIX fsync
// durability and object-store Head publication.
func (r WriteRecord) PublishLatencyFromFsync() time.Duration {
	if r.State < PublishedToObjectStore || r.FsyncAt.IsZero() || r.PublishedAt.IsZero() {
		return 0
	}
	return r.PublishedAt.Sub(r.FsyncAt)
}

// KernelBufferLag returns how long the write lingered in the volatile kernel
// page cache before reaching the daemon journal.
func (r WriteRecord) KernelBufferLag() time.Duration {
	if r.State < JournaledInDaemon || r.AppAckAt.IsZero() || r.JournalAt.IsZero() {
		return 0
	}
	return r.JournalAt.Sub(r.AppAckAt)
}

// SIGKILLOutcome documents what survives and what is lost if the FUSE daemon
// receives SIGKILL at the instant of evaluation.
type SIGKILLOutcome struct {
	LostKernelBufferedCount int   `json:"lost_kernel_buffered_count"`
	LostKernelBufferedBytes int64 `json:"lost_kernel_buffered_bytes"`
	AtRiskJournaledCount    int   `json:"at_risk_journaled_count"`
	AtRiskJournaledBytes    int64 `json:"at_risk_journaled_bytes"`
	SurvivedFsyncCount      int   `json:"survived_fsync_count"`
	SurvivedFsyncBytes      int64 `json:"survived_fsync_bytes"`
	SurvivedPublishedCount  int   `json:"survived_published_count"`
	SurvivedPublishedBytes  int64 `json:"survived_published_bytes"`
}

// DirtyExposure describes current un-flushed kernel page-cache exposure.
type DirtyExposure struct {
	BufferedWrites int           `json:"buffered_writes"`
	BufferedBytes  int64         `json:"buffered_bytes"`
	OldestAge      time.Duration `json:"oldest_age"`
	ExceedsBytes   bool          `json:"exceeds_bytes"`
	ExceedsAge     bool          `json:"exceeds_age"`
}

// NeedsFlush reports whether kernel dirty page-cache exposure has reached
// either the byte window or age bound.
func (d DirtyExposure) NeedsFlush() bool {
	return d.ExceedsBytes || d.ExceedsAge
}

// ContractSnapshot is a point-in-time summary of the write-acknowledgement
// contract state and invariants.
type ContractSnapshot struct {
	CountsByState        map[string]int   `json:"counts_by_state"`
	BytesByState         map[string]int64 `json:"bytes_by_state"`
	VisibleSeq           uint64           `json:"visible_seq"`
	DurableSeq           uint64           `json:"durable_seq"`
	PublishedSeq         uint64           `json:"published_seq"`
	MaxJournalToPublish  time.Duration    `json:"max_journal_to_publish"`
	MaxFsyncToPublish    time.Duration    `json:"max_fsync_to_publish"`
	PublishSLAViolations int              `json:"publish_sla_violations"`
	CleanCloses          int64            `json:"clean_closes"`
	SyncS3PutsOnClose    int64            `json:"sync_s3_puts_on_close"`
}

// ContractTracker is a concurrency-safe state machine enforcing the HIP-1342
// write-acknowledgement contract across buffered writes, daemon journal
// entries, POSIX fsync/syncfs durability boundaries, and asynchronous S3
// publication.
type ContractTracker struct {
	cfg WritebackConfig

	mu                   sync.RWMutex
	records              map[uint64]*WriteRecord
	order                []uint64
	visibleSeq           uint64
	durableSeq           uint64
	publishedSeq         uint64
	maxJournalToPublish  time.Duration
	maxFsyncToPublish    time.Duration
	publishSLAViolations int
	cleanCloses          int64
	syncS3PutsOnClose    int64
}

// NewContractTracker constructs a ContractTracker validated against cfg.
func NewContractTracker(cfg WritebackConfig) (*ContractTracker, error) {
	valid, err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	return &ContractTracker{
		cfg:     valid,
		records: make(map[uint64]*WriteRecord),
	}, nil
}

// Config returns the validated WritebackConfig governing this tracker.
func (t *ContractTracker) Config() WritebackConfig { return t.cfg }

// RecordKernelWrite registers a write(2) acknowledged by the Linux kernel page
// cache under FUSE_WRITEBACK_CACHE before the daemon receives FUSE_WRITE.
// The publication SLA clock is NOT started here.
func (t *ContractTracker) RecordKernelWrite(id, inode uint64, offset, length int64, now time.Time) error {
	if length < 0 || offset < 0 {
		return fmt.Errorf("%w: negative offset=%d or length=%d", ErrInvalidConfig, offset, length)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.records[id]; exists {
		return fmt.Errorf("%w: duplicate record id %d", ErrInvalidTransition, id)
	}
	t.records[id] = &WriteRecord{
		ID:       id,
		Inode:    inode,
		Offset:   offset,
		Length:   length,
		State:    BufferedInKernelPageCache,
		AppAckAt: now,
	}
	t.order = append(t.order, id)
	return nil
}

// RecordDaemonJournal advances a write from BufferedInKernelPageCache (or
// registers a direct writethrough write) to JournaledInDaemon when the daemon
// appends it to the local WAL at journalSeq. This starts the publication clock.
func (t *ContractTracker) RecordDaemonJournal(id, journalSeq uint64, now time.Time) error {
	if journalSeq == 0 {
		return fmt.Errorf("%w: journal sequence must be > 0", ErrInvalidTransition)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.records[id]
	if !ok {
		return fmt.Errorf("%w: id=%d", ErrRecordNotFound, id)
	}
	if rec.State != BufferedInKernelPageCache {
		return fmt.Errorf("%w: record %d in state %s cannot move to %s", ErrInvalidTransition, id, rec.State, JournaledInDaemon)
	}
	if journalSeq < t.visibleSeq {
		return fmt.Errorf("%w: journal seq %d regressed below visible %d", ErrInvalidTransition, journalSeq, t.visibleSeq)
	}
	rec.State = JournaledInDaemon
	rec.JournalSeq = journalSeq
	rec.JournalAt = now
	if journalSeq > t.visibleSeq {
		t.visibleSeq = journalSeq
	}
	return nil
}

// RecordFsyncDurable advances all journaled records with JournalSeq <= upToSeq
// to FsyncDurable, representing completion of fsync(2)/fdatasync(2)/syncfs(2).
// Records still in BufferedInKernelPageCache are untouched because they have
// not yet reached the daemon journal.
func (t *ContractTracker) RecordFsyncDurable(upToSeq uint64, now time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if upToSeq > t.visibleSeq {
		return fmt.Errorf("%w: durable seq %d exceeds visible seq %d", ErrInvalidTransition, upToSeq, t.visibleSeq)
	}
	if upToSeq < t.durableSeq {
		return fmt.Errorf("%w: durable seq %d regressed below %d", ErrInvalidTransition, upToSeq, t.durableSeq)
	}
	t.durableSeq = upToSeq
	for _, id := range t.order {
		rec := t.records[id]
		if rec.State == JournaledInDaemon && rec.JournalSeq <= upToSeq {
			rec.State = FsyncDurable
			rec.FsyncAt = now
		}
	}
	return nil
}

// RecordPublished advances all FsyncDurable records with JournalSeq <= upToSeq
// to PublishedToObjectStore and checks the publication SLA strictly from
// JournalAt (never AppAckAt). Returns ErrPublishBoundExceeded if any newly
// published record exceeded PublishBound.
func (t *ContractTracker) RecordPublished(upToSeq uint64, now time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if upToSeq > t.durableSeq {
		return fmt.Errorf("%w: published seq %d exceeds durable seq %d (cannot publish undurable journal records)", ErrInvalidTransition, upToSeq, t.durableSeq)
	}
	if upToSeq < t.publishedSeq {
		return fmt.Errorf("%w: published seq %d regressed below %d", ErrInvalidTransition, upToSeq, t.publishedSeq)
	}
	t.publishedSeq = upToSeq
	var violated error
	for _, id := range t.order {
		rec := t.records[id]
		if rec.State == FsyncDurable && rec.JournalSeq <= upToSeq {
			rec.State = PublishedToObjectStore
			rec.PublishedAt = now
			jLat := rec.PublishLatencyFromJournal()
			if jLat > t.maxJournalToPublish {
				t.maxJournalToPublish = jLat
			}
			fLat := rec.PublishLatencyFromFsync()
			if fLat > t.maxFsyncToPublish {
				t.maxFsyncToPublish = fLat
			}
			if jLat > t.cfg.PublishBound {
				t.publishSLAViolations++
				violated = fmt.Errorf("%w: record %d seq %d took %s > bound %s", ErrPublishBoundExceeded, rec.ID, rec.JournalSeq, jLat, t.cfg.PublishBound)
			}
		}
	}
	return violated
}

// DirtyExposure computes the current volatile kernel page-cache exposure and
// whether it exceeds the configured byte window or age expiry.
func (t *ContractTracker) DirtyExposure(now time.Time) DirtyExposure {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var exp DirtyExposure
	for _, id := range t.order {
		rec := t.records[id]
		if rec.State != BufferedInKernelPageCache {
			continue
		}
		exp.BufferedWrites++
		exp.BufferedBytes += rec.Length
		if age := now.Sub(rec.AppAckAt); age > exp.OldestAge {
			exp.OldestAge = age
		}
	}
	exp.ExceedsBytes = exp.BufferedBytes >= t.cfg.DirtyWindowBytes
	exp.ExceedsAge = exp.BufferedWrites > 0 && exp.OldestAge >= t.cfg.DirtyExpireDuration
	return exp
}

// VerifyCleanClose records a clean close(2) (FUSE_FLUSH) and verifies the zero
// synchronous S3 PUT invariant.
func (t *ContractTracker) VerifyCleanClose(syncS3PutsDelta int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cleanCloses++
	t.syncS3PutsOnClose += syncS3PutsDelta
	if t.cfg.ZeroSyncS3OnClose && syncS3PutsDelta > 0 {
		return fmt.Errorf("%w: observed %d synchronous PUT(s) on close", ErrSyncS3OnCloseViolation, syncS3PutsDelta)
	}
	return nil
}

// SimulateDaemonSIGKILL evaluates the exact crash survival outcome if the FUSE
// daemon is killed with SIGKILL right now, without running lifecycle syncfs:
//   - BufferedInKernelPageCache -> LOST (never reached daemon)
//   - JournaledInDaemon         -> AT RISK (not yet fsynced to local media)
//   - FsyncDurable              -> SURVIVES (recovered from local NVMe journal)
//   - PublishedToObjectStore    -> SURVIVES (committed in object-store Head)
func (t *ContractTracker) SimulateDaemonSIGKILL() SIGKILLOutcome {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out SIGKILLOutcome
	for _, id := range t.order {
		rec := t.records[id]
		switch rec.State {
		case BufferedInKernelPageCache:
			out.LostKernelBufferedCount++
			out.LostKernelBufferedBytes += rec.Length
		case JournaledInDaemon:
			out.AtRiskJournaledCount++
			out.AtRiskJournaledBytes += rec.Length
		case FsyncDurable:
			out.SurvivedFsyncCount++
			out.SurvivedFsyncBytes += rec.Length
		case PublishedToObjectStore:
			out.SurvivedPublishedCount++
			out.SurvivedPublishedBytes += rec.Length
		}
	}
	return out
}

// Record returns a copy of the WriteRecord with the given id.
func (t *ContractTracker) Record(id uint64) (WriteRecord, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	rec, ok := t.records[id]
	if !ok {
		return WriteRecord{}, false
	}
	return *rec, true
}

// Snapshot returns a consistent summary of all tracked records and invariants.
func (t *ContractTracker) Snapshot() ContractSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	counts := map[string]int{
		BufferedInKernelPageCache.String(): 0,
		JournaledInDaemon.String():         0,
		FsyncDurable.String():              0,
		PublishedToObjectStore.String():    0,
	}
	bytes := map[string]int64{
		BufferedInKernelPageCache.String(): 0,
		JournaledInDaemon.String():         0,
		FsyncDurable.String():              0,
		PublishedToObjectStore.String():    0,
	}
	for _, id := range t.order {
		rec := t.records[id]
		k := rec.State.String()
		counts[k]++
		bytes[k] += rec.Length
	}
	return ContractSnapshot{
		CountsByState:        counts,
		BytesByState:         bytes,
		VisibleSeq:           t.visibleSeq,
		DurableSeq:           t.durableSeq,
		PublishedSeq:         t.publishedSeq,
		MaxJournalToPublish:  t.maxJournalToPublish,
		MaxFsyncToPublish:    t.maxFsyncToPublish,
		PublishSLAViolations: t.publishSLAViolations,
		CleanCloses:          t.cleanCloses,
		SyncS3PutsOnClose:    t.syncS3PutsOnClose,
	}
}
