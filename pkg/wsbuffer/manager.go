package wsbuffer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Manager coordinates multiple isolated WSBuffer volumes under a shared global
// scrap-memory budget, enforcing per-volume VolumeID isolation and deterministic
// backpressure eviction when the aggregate scrap footprint exceeds GlobalMaxScrapBytes.
type Manager struct {
	rootDir             string
	globalMaxScrapBytes int64

	mu         sync.Mutex
	closed     bool
	totalScrap int64
	volumes    map[uint64]*Store
}

// NewManager creates a multi-volume WSBuffer Manager rooted at rootDir with
// aggregate scrap buffer memory capped at globalMaxScrapBytes.
func NewManager(rootDir string, globalMaxScrapBytes int64) (*Manager, error) {
	if globalMaxScrapBytes < DefaultPageSize {
		return nil, fmt.Errorf("%w: globalMaxScrapBytes %d must be >= %d", ErrInvalidConfig, globalMaxScrapBytes, DefaultPageSize)
	}
	if err := os.MkdirAll(rootDir, 0o700); err != nil {
		return nil, err
	}
	return &Manager{
		rootDir:             rootDir,
		globalMaxScrapBytes: globalMaxScrapBytes,
		volumes:             make(map[uint64]*Store),
	}, nil
}

// OpenVolume opens or returns an isolated Store for volumeID.
func (m *Manager) OpenVolume(volumeID uint64, cfg Config) (*Store, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if existing, ok := m.volumes[volumeID]; ok {
		m.mu.Unlock()
		return existing, nil
	}
	m.mu.Unlock()

	cfg.VolumeID = volumeID
	if cfg.MaxScrapBytes == 0 || cfg.MaxScrapBytes > m.globalMaxScrapBytes {
		cfg.MaxScrapBytes = m.globalMaxScrapBytes
	}
	volDir := filepath.Join(m.rootDir, fmt.Sprintf("vol-%016x", volumeID))
	store, err := Open(volDir, cfg)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		store.Close()
		return nil, ErrClosed
	}
	if existing, ok := m.volumes[volumeID]; ok {
		m.mu.Unlock()
		store.Close()
		return existing, nil
	}
	store.onScrapDelta = m.handleScrapDelta
	m.volumes[volumeID] = store
	m.mu.Unlock()
	return store, nil
}

func (m *Manager) handleScrapDelta(delta int64) {
	m.mu.Lock()
	m.totalScrap += delta
	if m.totalScrap < 0 {
		m.totalScrap = 0
	}
	needEvict := !m.closed && m.totalScrap > m.globalMaxScrapBytes
	var victim *Store
	if needEvict {
		var maxBytes int64
		for _, v := range m.volumes {
			st := v.Stats()
			if st.CurrentScrapBytes > maxBytes {
				maxBytes = st.CurrentScrapBytes
				victim = v
			}
		}
	}
	m.mu.Unlock()

	if victim != nil {
		_ = victim.Flush(context.Background())
	}
}

// TotalScrapBytes returns the aggregate live scrap bytes across all open volumes.
func (m *Manager) TotalScrapBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalScrap
}

// Close closes every managed volume cleanly.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	vols := make([]*Store, 0, len(m.volumes))
	for _, v := range m.volumes {
		vols = append(vols, v)
	}
	m.volumes = nil
	m.mu.Unlock()

	var firstErr error
	for _, v := range vols {
		if err := v.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
