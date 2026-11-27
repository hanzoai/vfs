package rosenbridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/hanzoai/vfs"
)

// BackingStore is the host-side storage layer shared identically by both the
// canonical gofer baseline and the conditional RosenBridge fast path.
type BackingStore interface {
	ReadAt(ctx context.Context, tenantID, relPath string, dst []byte, off int64) (n int, syscalls int, err error)
	WriteAt(ctx context.Context, tenantID, relPath string, src []byte, off int64, doSync bool) (n int, syscalls int, err error)
	Stat(ctx context.Context, tenantID, relPath string) (size int64, syscalls int, err error)
	Close() error
}

// OSBackingStore stores each tenant's files under an isolated, symlink-confined
// host directory <baseDir>/<tenantID>/ using real OS file descriptors and real
// pread/pwrite/fsync/stat syscalls.
type OSBackingStore struct {
	baseDir string

	mu      sync.Mutex
	closed  bool
	handles map[string]*os.File // key: tenantID + "\x00" + relPath
	openFDs atomic.Int64
}

// NewOSBackingStore creates a confined OS-backed store rooted at baseDir.
func NewOSBackingStore(baseDir string) (*OSBackingStore, error) {
	if err := os.MkdirAll(baseDir, 0o750); err != nil {
		return nil, fmt.Errorf("rosenbridge: mkdir baseDir: %w", err)
	}
	return &OSBackingStore{
		baseDir: baseDir,
		handles: make(map[string]*os.File),
	}, nil
}

// TenantRoot returns the isolated root directory for tenantID (creating it if needed).
func (s *OSBackingStore) TenantRoot(tenantID string) (string, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return "", err
	}
	dir := filepath.Join(s.baseDir, tenantID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("rosenbridge: mkdir tenant root: %w", err)
	}
	return dir, nil
}

// OpenFDs returns the number of currently open OS file descriptors held by the store.
func (s *OSBackingStore) OpenFDs() int64 { return s.openFDs.Load() }

func (s *OSBackingStore) resolveAndOpen(tenantID, relPath string, create bool) (*os.File, int, error) {
	root, err := s.TenantRoot(tenantID)
	if err != nil {
		return nil, 0, err
	}
	full, err := ConfinePath(root, relPath)
	if err != nil {
		return nil, 0, err
	}
	cacheKey := tenantID + "\x00" + relPath

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, 0, ErrRingClosed
	}
	if f, ok := s.handles[cacheKey]; ok {
		// Re-verify confinement against post-open symlink swap before reuse.
		if _, err := ConfinePath(root, relPath); err != nil {
			delete(s.handles, cacheKey)
			_ = f.Close()
			s.openFDs.Add(-1)
			s.mu.Unlock()
			return nil, 0, err
		}
		s.mu.Unlock()
		return f, 0, nil
	}
	s.mu.Unlock()

	if create {
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			return nil, 0, err
		}
		// Re-check confinement after parent directory creation.
		if _, err := ConfinePath(root, relPath); err != nil {
			return nil, 0, err
		}
	}

	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(full, flags, 0o640)
	if err != nil {
		return nil, 1, err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = f.Close()
		return nil, 1, ErrRingClosed
	}
	if existing, ok := s.handles[cacheKey]; ok {
		s.mu.Unlock()
		_ = f.Close()
		return existing, 1, nil
	}
	// Evict if cache grows too large to bound open FD usage.
	if len(s.handles) >= 256 {
		for k, old := range s.handles {
			_ = old.Close()
			s.openFDs.Add(-1)
			delete(s.handles, k)
			if len(s.handles) < 192 {
				break
			}
		}
	}
	s.handles[cacheKey] = f
	s.openFDs.Add(1)
	s.mu.Unlock()
	return f, 1, nil
}

func (s *OSBackingStore) ReadAt(ctx context.Context, tenantID, relPath string, dst []byte, off int64) (int, int, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if off < 0 {
		return 0, 0, fmt.Errorf("%w: negative offset %d", ErrDescriptorOutOfBounds, off)
	}
	f, openSc, err := s.resolveAndOpen(tenantID, relPath, false)
	if err != nil {
		return 0, openSc, err
	}
	n, err := f.ReadAt(dst, off)
	if errors.Is(err, io.EOF) && n > 0 {
		err = nil
	}
	return n, openSc + 1, err
}

func (s *OSBackingStore) WriteAt(ctx context.Context, tenantID, relPath string, src []byte, off int64, doSync bool) (int, int, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if off < 0 {
		return 0, 0, fmt.Errorf("%w: negative offset %d", ErrDescriptorOutOfBounds, off)
	}
	f, openSc, err := s.resolveAndOpen(tenantID, relPath, true)
	if err != nil {
		return 0, openSc, err
	}
	n, err := f.WriteAt(src, off)
	syscalls := openSc + 1
	if err != nil {
		return n, syscalls, err
	}
	if doSync {
		syscalls++
		if err := f.Sync(); err != nil {
			return n, syscalls, err
		}
	}
	return n, syscalls, nil
}

func (s *OSBackingStore) Stat(ctx context.Context, tenantID, relPath string) (int64, int, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	root, err := s.TenantRoot(tenantID)
	if err != nil {
		return 0, 0, err
	}
	full, err := ConfinePath(root, relPath)
	if err != nil {
		return 0, 0, err
	}
	fi, err := os.Lstat(full)
	if err != nil {
		return 0, 1, err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return 0, 1, fmt.Errorf("%w: leaf symlink rejected", ErrPathEscape)
	}
	return fi.Size(), 1, nil
}

// Close closes every cached file descriptor and guarantees OpenFDs() == 0.
func (s *OSBackingStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	handles := s.handles
	s.handles = nil
	s.mu.Unlock()

	var firstErr error
	for _, f := range handles {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.openFDs.Add(-1)
	}
	return firstErr
}

// VFSBackingStore wires RosenBridge and the canonical Gofer baseline directly
// onto per-tenant canonical *vfs.Volume instances (each tenant sealed with its
// own Volume and cipher, never sharing Volumes across tenants).
type VFSBackingStore struct {
	mu      sync.RWMutex
	volumes map[string]*vfs.Volume
	closed  bool
}

// NewVFSBackingStore creates a multi-tenant BackingStore backed by canonical
// hanzoai/vfs Volumes.
func NewVFSBackingStore() *VFSBackingStore {
	return &VFSBackingStore{
		volumes: make(map[string]*vfs.Volume),
	}
}

// MountTenant registers an isolated *vfs.Volume exclusively for tenantID.
func (s *VFSBackingStore) MountTenant(tenantID string, vol *vfs.Volume) error {
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	if vol == nil {
		return errors.New("rosenbridge: nil vfs.Volume")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrRingClosed
	}
	s.volumes[tenantID] = vol
	return nil
}

func (s *VFSBackingStore) tenantVolume(tenantID string) (*vfs.Volume, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrRingClosed
	}
	vol, ok := s.volumes[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: no volume mounted for tenant %q", ErrTenantMismatch, tenantID)
	}
	return vol, nil
}

func (s *VFSBackingStore) resolveInode(ctx context.Context, vol *vfs.Volume, relPath string, create bool) (vfs.Inode, error) {
	segs, err := NormalizeVirtualPath(relPath)
	if err != nil {
		return vfs.Inode{}, err
	}
	dir := vfs.RootInode
	for i := 0; i < len(segs)-1; i++ {
		in, err := vol.Lookup(ctx, dir, segs[i])
		if err != nil {
			if create && errors.Is(err, fs.ErrNotExist) {
				in, err = vol.Mkdir(ctx, dir, segs[i], 0o750)
				if err != nil && !errors.Is(err, fs.ErrExist) {
					return vfs.Inode{}, err
				}
				if errors.Is(err, fs.ErrExist) {
					in, err = vol.Lookup(ctx, dir, segs[i])
				}
			}
			if err != nil {
				return vfs.Inode{}, err
			}
		}
		if in.Mode&fs.ModeSymlink != 0 {
			return vfs.Inode{}, fmt.Errorf("%w: vfs symlink component %q rejected", ErrPathEscape, segs[i])
		}
		if !in.IsDir() {
			return vfs.Inode{}, vfs.ErrNotDir
		}
		dir = in.ID
	}

	leaf := segs[len(segs)-1]
	in, err := vol.Lookup(ctx, dir, leaf)
	if err != nil {
		if create && errors.Is(err, fs.ErrNotExist) {
			in, err = vol.Create(ctx, dir, leaf, 0o640)
			if errors.Is(err, fs.ErrExist) {
				in, err = vol.Lookup(ctx, dir, leaf)
			}
		}
		if err != nil {
			return vfs.Inode{}, err
		}
	}
	if in.Mode&fs.ModeSymlink != 0 {
		return vfs.Inode{}, fmt.Errorf("%w: vfs leaf symlink %q rejected", ErrPathEscape, leaf)
	}
	return in, nil
}

func (s *VFSBackingStore) ReadAt(ctx context.Context, tenantID, relPath string, dst []byte, off int64) (int, int, error) {
	vol, err := s.tenantVolume(tenantID)
	if err != nil {
		return 0, 0, err
	}
	in, err := s.resolveInode(ctx, vol, relPath, false)
	if err != nil {
		return 0, 1, err
	}
	f, err := vol.Open(ctx, in.ID)
	if err != nil {
		return 0, 1, err
	}
	defer f.Close()
	n, err := f.ReadAt(ctx, dst, off)
	if errors.Is(err, io.EOF) && n > 0 {
		err = nil
	}
	return n, 1, err
}

func (s *VFSBackingStore) WriteAt(ctx context.Context, tenantID, relPath string, src []byte, off int64, doSync bool) (int, int, error) {
	vol, err := s.tenantVolume(tenantID)
	if err != nil {
		return 0, 0, err
	}
	in, err := s.resolveInode(ctx, vol, relPath, true)
	if err != nil {
		return 0, 1, err
	}
	f, err := vol.Open(ctx, in.ID)
	if err != nil {
		return 0, 1, err
	}
	defer f.Close()
	n, err := f.WriteAt(ctx, src, off)
	syscalls := 1
	if err != nil {
		return n, syscalls, err
	}
	if doSync {
		syscalls++
		if err := f.Sync(ctx); err != nil {
			return n, syscalls, err
		}
	}
	return n, syscalls, nil
}

func (s *VFSBackingStore) Stat(ctx context.Context, tenantID, relPath string) (int64, int, error) {
	vol, err := s.tenantVolume(tenantID)
	if err != nil {
		return 0, 0, err
	}
	in, err := s.resolveInode(ctx, vol, relPath, false)
	if err != nil {
		return 0, 1, err
	}
	return int64(in.Size), 1, nil
}

// Close detaches references to tenant Volumes (caller retains Volume ownership
// and closes Volumes according to its lifecycle).
func (s *VFSBackingStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.volumes = nil
	s.mu.Unlock()
	return nil
}
