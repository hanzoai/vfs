package writeback

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

// LifecycleEvent identifies a volume mount lifecycle transition that requires
// flushing kernel page-cache dirty pages before final object-store publication.
type LifecycleEvent string

const (
	// EventDetach occurs when a volume mount is detached from a sandbox or node.
	EventDetach LifecycleEvent = "detach"

	// EventSuspend occurs when a sandbox/container is suspended or snapshotted.
	EventSuspend LifecycleEvent = "suspend"

	// EventEnd occurs on graceful mount shutdown / process termination.
	EventEnd LifecycleEvent = "end"
)

// BarrierDeps supplies the three ordered stages of a lifecycle flush barrier.
// When constructed via NewVolumeBarrier, all three default to real Linux
// SyncFS + *vfs.Volume.Sync + *vfs.Volume.Flush. Tests may override any stage
// to verify strict ordering and error propagation deterministically.
type BarrierDeps struct {
	SyncFS       func(mountpoint string) error
	SyncJournal  func(ctx context.Context) error
	PublishFinal func(ctx context.Context) error
}

// BarrierReport captures the timing, executed step order, and outcome of a
// lifecycle flush barrier.
type BarrierReport struct {
	Event               LifecycleEvent `json:"event"`
	Mountpoint          string         `json:"mountpoint"`
	Steps               []string       `json:"steps"`
	SyncFSDuration      time.Duration  `json:"syncfs_duration"`
	JournalSyncDuration time.Duration  `json:"journal_sync_duration"`
	PublishDuration     time.Duration  `json:"publish_duration"`
	TotalDuration       time.Duration  `json:"total_duration"`
	JournalVisibleSeq   uint64         `json:"journal_visible_seq,omitempty"`
	JournalDurableSeq   uint64         `json:"journal_durable_seq,omitempty"`
	JournalPublishedSeq uint64         `json:"journal_published_seq,omitempty"`
	Err                 string         `json:"err,omitempty"`
}

// LifecycleBarrier enforces the HIP-1342 #13 shutdown/detach/suspend barrier:
//
//  1. syncfs(mountpoint): force Linux kernel writeback of all dirty pages held
//     for the FUSE superblock down to the daemon (FUSE_WRITE).
//  2. vol.Sync(ctx): flush the daemon's local NVMe WAL so every drained write
//     crosses the FsyncDurable boundary.
//  3. vol.Flush(ctx): publish the final generation and commit the object-store
//     Head so all changes are PublishedToObjectStore before Detach/Suspend/End
//     returns.
type LifecycleBarrier struct {
	cfg  WritebackConfig
	deps BarrierDeps
	vol  *vfs.Volume

	mu sync.Mutex // serializes overlapping lifecycle barriers
}

// NewLifecycleBarrier creates a barrier from custom dependencies.
func NewLifecycleBarrier(cfg WritebackConfig, deps BarrierDeps) (*LifecycleBarrier, error) {
	valid, err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	if deps.SyncFS == nil {
		deps.SyncFS = SyncFS
	}
	return &LifecycleBarrier{
		cfg:  valid,
		deps: deps,
	}, nil
}

// NewVolumeBarrier creates a production LifecycleBarrier bound to vol using
// real OS SyncFS, vol.Sync, and vol.Flush.
func NewVolumeBarrier(cfg WritebackConfig, vol *vfs.Volume) (*LifecycleBarrier, error) {
	if vol == nil {
		return nil, fmt.Errorf("%w: volume required for lifecycle barrier", ErrInvalidConfig)
	}
	b, err := NewLifecycleBarrier(cfg, BarrierDeps{
		SyncFS:       SyncFS,
		SyncJournal:  vol.Sync,
		PublishFinal: vol.Flush,
	})
	if err != nil {
		return nil, err
	}
	b.vol = vol
	return b, nil
}

// Execute runs the ordered barrier for event against mountpoint.
func (b *LifecycleBarrier) Execute(ctx context.Context, event LifecycleEvent, mountpoint string) (BarrierReport, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	start := time.Now()
	rep := BarrierReport{
		Event:      event,
		Mountpoint: mountpoint,
		Steps:      make([]string, 0, 3),
	}

	var errs []error

	// Step 1: Kernel syncfs(2) flushes dirty page-cache writes to the daemon.
	if b.cfg.SyncFSOnLifecycle && b.deps.SyncFS != nil && mountpoint != "" {
		t0 := time.Now()
		rep.Steps = append(rep.Steps, "syncfs")
		if err := b.deps.SyncFS(mountpoint); err != nil {
			errs = append(errs, fmt.Errorf("%s syncfs: %w", event, err))
		}
		rep.SyncFSDuration = max(time.Since(t0), 0)
	}

	// Step 2: Daemon journal sync makes all drained writes locally durable.
	if b.deps.SyncJournal != nil {
		t0 := time.Now()
		rep.Steps = append(rep.Steps, "journal_sync")
		if err := b.deps.SyncJournal(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s journal sync: %w", event, err))
		}
		rep.JournalSyncDuration = max(time.Since(t0), 0)
	}

	// Step 3: Final object-store publication commits the Head.
	if b.deps.PublishFinal != nil {
		t0 := time.Now()
		rep.Steps = append(rep.Steps, "final_publish")
		if err := b.deps.PublishFinal(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s final publish: %w", event, err))
		}
		rep.PublishDuration = max(time.Since(t0), 0)
	}

	if b.vol != nil {
		st := b.vol.State()
		rep.JournalVisibleSeq = st.Visible
		rep.JournalDurableSeq = st.Durable
		rep.JournalPublishedSeq = st.Published
	}
	rep.TotalDuration = max(time.Since(start), 0)

	err := errors.Join(errs...)
	if err != nil {
		rep.Err = err.Error()
	}
	return rep, err
}

// Detach runs the mandatory flush barrier prior to detaching the mount.
func (b *LifecycleBarrier) Detach(ctx context.Context, mountpoint string) (BarrierReport, error) {
	return b.Execute(ctx, EventDetach, mountpoint)
}

// Suspend runs the mandatory flush barrier prior to suspending or snapshotting.
func (b *LifecycleBarrier) Suspend(ctx context.Context, mountpoint string) (BarrierReport, error) {
	return b.Execute(ctx, EventSuspend, mountpoint)
}

// End runs the mandatory flush barrier on clean mount shutdown.
func (b *LifecycleBarrier) End(ctx context.Context, mountpoint string) (BarrierReport, error) {
	return b.Execute(ctx, EventEnd, mountpoint)
}

// DirtyGuard periodically invokes syncfs(mountpoint) while a writeback-cached
// mount is active so kernel dirty page-cache exposure remains bounded by
// DirtyExpireDuration and DirtyWindowBytes instead of Linux's 30 s / 20% RAM
// defaults.
type DirtyGuard struct {
	cfg        WritebackConfig
	mountpoint string
	syncFn     func(string) error
	dirtyBytes func() int64

	flushes   atomic.Int64
	lastErrMu sync.Mutex
	lastErr   error

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// StartDirtyGuard launches a background dirty page-cache exposure guard.
// dirtyBytes is optional; when non-nil it returns estimated buffered dirty
// bytes so the guard can trigger early before DirtyExpireDuration elapses.
func StartDirtyGuard(cfg WritebackConfig, mountpoint string, syncFn func(string) error, dirtyBytes func() int64) (*DirtyGuard, error) {
	valid, err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	if syncFn == nil {
		syncFn = SyncFS
	}
	g := &DirtyGuard{
		cfg:        valid,
		mountpoint: mountpoint,
		syncFn:     syncFn,
		dirtyBytes: dirtyBytes,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	go g.loop()
	return g, nil
}

func (g *DirtyGuard) loop() {
	defer close(g.doneCh)
	interval := g.cfg.DirtyExpireDuration
	if g.dirtyBytes != nil && interval > 50*time.Millisecond {
		// Poll more frequently when a byte-exposure estimator is wired in so
		// DirtyWindowBytes triggers promptly without busy-spinning.
		poll := interval / 4
		if poll < 10*time.Millisecond {
			poll = 10 * time.Millisecond
		}
		interval = poll
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastFlush := time.Now()
	for {
		select {
		case <-g.stopCh:
			return
		case now := <-ticker.C:
			ageExceeded := now.Sub(lastFlush) >= g.cfg.DirtyExpireDuration
			bytesExceeded := g.dirtyBytes != nil && g.dirtyBytes() >= g.cfg.DirtyWindowBytes
			if !ageExceeded && !bytesExceeded {
				continue
			}
			err := g.syncFn(g.mountpoint)
			g.flushes.Add(1)
			lastFlush = time.Now()
			if err != nil {
				g.lastErrMu.Lock()
				g.lastErr = err
				g.lastErrMu.Unlock()
			}
		}
	}
}

// Flushes returns the number of proactive dirty-guard syncfs calls executed.
func (g *DirtyGuard) Flushes() int64 { return g.flushes.Load() }

// LastErr returns the most recent error returned by syncFn, if any.
func (g *DirtyGuard) LastErr() error {
	g.lastErrMu.Lock()
	defer g.lastErrMu.Unlock()
	return g.lastErr
}

// Stop terminates the guard goroutine and waits for it to exit.
func (g *DirtyGuard) Stop() {
	g.stopOnce.Do(func() { close(g.stopCh) })
	<-g.doneCh
}

// CountingBackend wraps any backend.Backend and atomically records all object
// store PUT/GET/DELETE operations so tests and benchmarks can prove zero
// synchronous S3 PUTs on clean close(2) and measure exact publication traffic.
type CountingBackend struct {
	backend.Backend

	Puts        atomic.Int64
	PutBytes    atomic.Int64
	HeadCommits atomic.Int64
	Gets        atomic.Int64
	Deletes     atomic.Int64
}

// WrapCounting wraps inner in a CountingBackend.
func WrapCounting(inner backend.Backend) *CountingBackend {
	return &CountingBackend{Backend: inner}
}

// TotalPuts returns the total number of Put, PutIfAbsent, and PutIfVersion
// calls issued against the underlying object store.
func (c *CountingBackend) TotalPuts() int64 { return c.Puts.Load() }

func (c *CountingBackend) Put(ctx context.Context, key string, data []byte) error {
	c.Puts.Add(1)
	c.PutBytes.Add(int64(len(data)))
	return c.Backend.Put(ctx, key, data)
}

func (c *CountingBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	c.Puts.Add(1)
	c.PutBytes.Add(int64(len(data)))
	if key == "head" {
		c.HeadCommits.Add(1)
	}
	return c.Backend.PutIfAbsent(ctx, key, data)
}

func (c *CountingBackend) PutIfVersion(ctx context.Context, key string, data []byte, version backend.Version) (backend.Version, error) {
	c.Puts.Add(1)
	c.PutBytes.Add(int64(len(data)))
	if key == "head" {
		c.HeadCommits.Add(1)
	}
	return c.Backend.PutIfVersion(ctx, key, data, version)
}

func (c *CountingBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	c.Gets.Add(1)
	return c.Backend.Get(ctx, key, max)
}

func (c *CountingBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	c.Gets.Add(1)
	return c.Backend.GetRange(ctx, key, off, n)
}

func (c *CountingBackend) Delete(ctx context.Context, key string) error {
	c.Deletes.Add(1)
	return c.Backend.Delete(ctx, key)
}
