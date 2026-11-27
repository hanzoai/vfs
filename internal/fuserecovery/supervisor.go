package fuserecovery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Telemetry records recovery outcomes, MTTR durations, and degraded fallback
// counters for the FUSE daemon supervisor.
type Telemetry struct {
	CrashCount              uint64
	SuccessfulRecoveries    uint64
	DegradedFallbacks       uint64
	ColdRemountsRequired    uint64
	ColdRemountsExecuted    uint64
	HandlesRecovered        uint64
	OrphansRecovered        uint64
	StaleHandlesDetected    uint64
	BarriersVerified        uint64
	ReplayedBlocked         uint64
	LastRecoveryDuration    time.Duration
	LastColdRemountDuration time.Duration
	LastError               string
}

// SupervisorConfig configures a Supervisor instance.
type SupervisorConfig struct {
	SessionID      string
	MountPath      string
	StateDir       string
	Backing        BackingStore
	Channel        FuseChannel
	CheckpointMode CheckpointMode
}

// Supervisor owns the restart-resilient /dev/fuse channel and manages the
// lifecycle of host-side FUSE daemon processes across abrupt crashes.
type Supervisor struct {
	mu           sync.Mutex
	cfg          SupervisorConfig
	store        *StateStore
	channel      FuseChannel
	daemon       *Daemon
	remountCount uint64
	telemetry    Telemetry
}

// NewSupervisor creates a Supervisor and initializes its confined StateStore
// (unless CheckpointMode == CheckpointDisabled).
func NewSupervisor(cfg SupervisorConfig) (*Supervisor, error) {
	if cfg.Backing == nil {
		return nil, fmt.Errorf("fuserecovery: backing store required")
	}
	if cfg.SessionID == "" {
		cfg.SessionID = "ananke-session-1"
	}
	if cfg.MountPath == "" {
		cfg.MountPath = "/mnt/hanzovfs"
	}
	ch := cfg.Channel
	if ch == nil {
		ch = NewSupervisorChannel(cfg.SessionID, cfg.MountPath)
	}
	var st *StateStore
	if cfg.CheckpointMode != CheckpointDisabled {
		var err error
		st, err = NewStateStore(cfg.StateDir)
		if err != nil {
			return nil, err
		}
	}
	return &Supervisor{
		cfg:     cfg,
		store:   st,
		channel: ch,
	}, nil
}

// Start launches the initial Generation-1 FUSE daemon.
func (s *Supervisor) Start(ctx context.Context) (*Daemon, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.daemon != nil {
		return s.daemon, nil
	}
	d, err := NewDaemon(DaemonConfig{
		SessionID:      s.channel.SessionID(),
		MountPath:      s.channel.MountPath(),
		StateStore:     s.store,
		Backing:        s.cfg.Backing,
		Channel:        s.channel,
		CheckpointMode: s.cfg.CheckpointMode,
	})
	if err != nil {
		return nil, err
	}
	s.daemon = d
	return d, nil
}

// Daemon returns the currently active FUSE daemon (or nil if stopped).
func (s *Supervisor) Daemon() *Daemon {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.daemon
}

// Channel returns the supervisor-held FuseChannel.
func (s *Supervisor) Channel() FuseChannel {
	return s.channel
}

// StateStore returns the supervisor's StateStore (nil if checkpointing disabled).
func (s *Supervisor) StateStore() *StateStore {
	return s.store
}

// Metrics returns a snapshot of the supervisor's recovery telemetry.
func (s *Supervisor) Metrics() Telemetry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.telemetry
}

// InjectProcessCrash abruptly terminates the current daemon (SIGKILL semantics)
// while keeping the supervisor-held /dev/fuse channel alive (when handoff is enabled).
func (s *Supervisor) InjectProcessCrash() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.daemon != nil {
		s.daemon.CrashAbrupt()
	}
	s.telemetry.CrashCount++
}

// InjectKernelCrash simulates a full host/kernel crash that destroys both the
// daemon process and the supervisor-held /dev/fuse descriptor.
func (s *Supervisor) InjectKernelCrash() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.daemon != nil {
		s.daemon.CrashAbrupt()
	}
	s.channel.SimulateKernelCrash()
	s.telemetry.CrashCount++
}

// DisableHandoff disables /dev/fuse fd handoff to test degraded fallback behavior.
func (s *Supervisor) DisableHandoff() {
	s.channel.DisableHandoff()
}

// Recover performs Ananke transparent daemon recovery: loads the CRC32C-verified
// state snapshot, re-attaches the live /dev/fuse channel without unmounting,
// re-binds open handles and orphan files, and replays blocked requests.
//
// If /dev/fuse handoff is unavailable (kernel crash or handoff disabled),
// Recover increments degraded-fallback telemetry and returns ErrHandoffUnavailable
// so the caller can explicitly trigger ColdRemountRecover.
func (s *Supervisor) Recover(ctx context.Context) (*Daemon, RecoveryReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, RecoveryReport{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	start := time.Now()
	if !s.channel.HandoffAvailable() {
		s.telemetry.DegradedFallbacks++
		s.telemetry.ColdRemountsRequired++
		err := fmt.Errorf("%w: mount=%q alive=%t", ErrHandoffUnavailable, s.channel.MountPath(), s.channel.MountAlive())
		s.telemetry.LastError = err.Error()
		return nil, RecoveryReport{}, err
	}
	if s.store == nil || s.cfg.CheckpointMode == CheckpointDisabled {
		s.telemetry.DegradedFallbacks++
		s.telemetry.ColdRemountsRequired++
		err := fmt.Errorf("%w: checkpointing disabled", ErrHandoffUnavailable)
		s.telemetry.LastError = err.Error()
		return nil, RecoveryReport{}, err
	}

	d, rep, err := RecoverDaemon(DaemonConfig{
		SessionID:      s.channel.SessionID(),
		MountPath:      s.channel.MountPath(),
		StateStore:     s.store,
		Backing:        s.cfg.Backing,
		Channel:        s.channel,
		CheckpointMode: s.cfg.CheckpointMode,
	})
	elapsed := time.Since(start)
	if err != nil {
		if errors.Is(err, ErrHandoffUnavailable) {
			s.telemetry.DegradedFallbacks++
			s.telemetry.ColdRemountsRequired++
		}
		s.telemetry.LastError = err.Error()
		return nil, rep, err
	}

	s.daemon = d
	s.telemetry.SuccessfulRecoveries++
	s.telemetry.HandlesRecovered += uint64(rep.HandlesRestored)
	s.telemetry.OrphansRecovered += uint64(rep.OrphansRestored)
	s.telemetry.StaleHandlesDetected += uint64(rep.StaleHandles)
	s.telemetry.BarriersVerified += uint64(rep.BarriersVerified)
	s.telemetry.ReplayedBlocked += uint64(rep.ReplayedBlocked)
	s.telemetry.LastRecoveryDuration = elapsed
	s.telemetry.LastError = ""
	return d, rep, nil
}

// ColdRemountRecover executes the matched non-Ananke baseline recovery:
// tears down the mountpoint (invalidating all open file handles and orphans),
// establishes a fresh FUSE mount session, and walks/re-indexes the backing
// storage tree from scratch.
func (s *Supervisor) ColdRemountRecover(ctx context.Context, defaultTenant string) (*Daemon, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	start := time.Now()
	if s.daemon != nil {
		_ = s.daemon.Close()
		s.daemon = nil
	}

	s.remountCount++
	newSessID := fmt.Sprintf("%s-cold-%d", s.cfg.SessionID, s.remountCount)
	if err := s.channel.ColdRemount(newSessID); err != nil {
		return nil, 0, err
	}

	d, err := NewDaemon(DaemonConfig{
		SessionID:      newSessID,
		MountPath:      s.channel.MountPath(),
		StateStore:     s.store,
		Backing:        s.cfg.Backing,
		Channel:        s.channel,
		CheckpointMode: s.cfg.CheckpointMode,
	})
	if err != nil {
		return nil, 0, err
	}

	// Re-index existing non-orphan files from backing root (as a cold remount
	// daemon must rediscover the tree from storage without cached FUSE inodes).
	reindexed := 0
	if osStore, ok := s.cfg.Backing.(*OSBackingStore); ok {
		root := osStore.Root()
		_ = filepath.WalkDir(root, func(p string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() {
				if entry != nil && entry.IsDir() && entry.Name() == orphanDirName {
					return filepath.SkipDir
				}
				return nil
			}
			rel, rerr := filepath.Rel(root, p)
			if rerr != nil {
				return nil
			}
			rel = filepath.ToSlash(rel)
			info, ierr := entry.Info()
			if ierr != nil {
				return nil
			}
			d.mu.Lock()
			inoID := d.state.NextInode
			d.state.NextInode++
			d.state.Inodes[inoID] = &InodeRecord{
				Inode:       inoID,
				Path:        rel,
				Generation:  d.state.Generation,
				NLookup:     0,
				OwnerTenant: defaultTenant,
				Mode:        uint32(info.Mode().Perm()),
				Size:        uint64(info.Size()),
			}
			d.pathToInode[rel] = inoID
			d.mu.Unlock()
			reindexed++
			return nil
		})
	}

	elapsed := time.Since(start)
	s.daemon = d
	s.telemetry.ColdRemountsExecuted++
	s.telemetry.LastColdRemountDuration = elapsed
	return d, reindexed, nil
}

// Close shuts down the active daemon cleanly and closes all backing FDs.
func (s *Supervisor) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.daemon != nil {
		err := s.daemon.Close()
		s.daemon = nil
		return err
	}
	return nil
}
