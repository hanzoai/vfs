// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package gitvfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

func openFileBackend(ctx context.Context, dir string) (backend.Backend, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	return backend.Open(ctx, "file://"+abs)
}

// ExtName is the Volume extension namespace ("ext/gitvfs/<hash>") for sealed
// git snapshot metadata persisted alongside a Volume's tree (SPECIFICATION 3.1).
const ExtName = "gitvfs"

// ErrCredentialLeak is returned when a snapshot candidate carries embedded
// credentials or auth headers in `.git/config`.
var ErrCredentialLeak = errors.New("gitvfs: credential detected in repository config")

// COWBackend overlays a thin writable per-sandbox delta backend over a shared
// read-only base backend. Every write (Put, PutIfAbsent, PutIfVersion, Delete)
// lands exclusively on delta; base is never mutated and never closed by a
// single sandbox's Close().
type COWBackend struct {
	base  backend.Backend
	delta backend.Backend

	mu      sync.RWMutex
	deleted map[string]struct{}
}

// NewCOWBackend constructs a copy-on-write backend over a read-only base and
// a per-sandbox writable delta backend.
func NewCOWBackend(base, delta backend.Backend) (*COWBackend, error) {
	if base == nil || delta == nil {
		return nil, errors.New("gitvfs: COWBackend requires both base and delta backends")
	}
	return &COWBackend{
		base:    base,
		delta:   delta,
		deleted: make(map[string]struct{}),
	}, nil
}

func (c *COWBackend) isDeleted(key string) bool {
	c.mu.RLock()
	_, ok := c.deleted[key]
	c.mu.RUnlock()
	return ok
}

func (c *COWBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if c.isDeleted(key) {
		return nil, "", backend.ErrNotFound
	}
	data, ver, err := c.delta.Get(ctx, key, max)
	if err == nil || !errors.Is(err, backend.ErrNotFound) {
		return data, ver, err
	}
	return c.base.Get(ctx, key, max)
}

func (c *COWBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if c.isDeleted(key) {
		return nil, backend.ErrNotFound
	}
	data, err := c.delta.GetRange(ctx, key, off, n)
	if err == nil || !errors.Is(err, backend.ErrNotFound) {
		return data, err
	}
	return c.base.GetRange(ctx, key, off, n)
}

func (c *COWBackend) Stat(ctx context.Context, key string) (int64, error) {
	if c.isDeleted(key) {
		return 0, backend.ErrNotFound
	}
	sz, err := c.delta.Stat(ctx, key)
	if err == nil || !errors.Is(err, backend.ErrNotFound) {
		return sz, err
	}
	return c.base.Stat(ctx, key)
}

func (c *COWBackend) Put(ctx context.Context, key string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.delta.Put(ctx, key, data); err != nil {
		return err
	}
	delete(c.deleted, key)
	return nil
}

func (c *COWBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, del := c.deleted[key]; !del {
		if _, err := c.base.Stat(ctx, key); err == nil {
			return "", backend.ErrConflict
		} else if !errors.Is(err, backend.ErrNotFound) {
			return "", err
		}
	}
	ver, err := c.delta.PutIfAbsent(ctx, key, data)
	if err != nil {
		return "", err
	}
	delete(c.deleted, key)
	return ver, nil
}

func (c *COWBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, del := c.deleted[key]; del {
		return "", backend.ErrConflict
	}
	if _, statErr := c.delta.Stat(ctx, key); statErr == nil {
		return c.delta.PutIfVersion(ctx, key, data, v)
	} else if !errors.Is(statErr, backend.ErrNotFound) {
		return "", statErr
	}
	// Key has not yet been copied into delta: verify version against read-only base,
	// then atomically create it in delta without touching base.
	_, baseVer, err := c.base.Get(ctx, key, backend.Unbounded)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return "", backend.ErrConflict
		}
		return "", err
	}
	if baseVer != v {
		return "", backend.ErrConflict
	}
	return c.delta.PutIfAbsent(ctx, key, data)
}

func (c *COWBackend) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.delta.Delete(ctx, key); err != nil {
		return err
	}
	c.deleted[key] = struct{}{}
	return nil
}

func (c *COWBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	out := make(chan backend.Object, 64)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		seen := make(map[string]struct{})
		c.mu.RLock()
		deleted := make(map[string]struct{}, len(c.deleted))
		for k := range c.deleted {
			deleted[k] = struct{}{}
		}
		c.mu.RUnlock()

		// Emit delta objects first (shadowing base).
		dObjs, dErr := c.delta.List(ctx, prefix)
		for obj := range dObjs {
			if _, del := deleted[obj.Key]; del {
				continue
			}
			seen[obj.Key] = struct{}{}
			select {
			case out <- obj:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
		if err := <-dErr; err != nil {
			errCh <- err
			return
		}
		// Emit non-shadowed, non-deleted base objects.
		bObjs, bErr := c.base.List(ctx, prefix)
		for obj := range bObjs {
			if _, del := deleted[obj.Key]; del {
				continue
			}
			if _, dup := seen[obj.Key]; dup {
				continue
			}
			select {
			case out <- obj:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
		if err := <-bErr; err != nil {
			errCh <- err
		}
	}()
	return out, errCh
}

// Close closes only the per-sandbox delta backend; the shared base backend
// stays open for other sandboxes.
func (c *COWBackend) Close() error   { return c.delta.Close() }
func (c *COWBackend) String() string { return c.delta.String() }

// SnapshotMetadata is stored encrypted under ext/gitvfs/<hash> on the base
// Volume snapshot.
type SnapshotMetadata struct {
	TenantID           string    `json:"tenant_id"`
	RepoID             string    `json:"repo_id"`
	CommitSHA          string    `json:"commit_sha"`
	Branch             string    `json:"branch"`
	FileCount          int       `json:"file_count"`
	TotalBytes         int64     `json:"total_bytes"`
	CredentialScrubbed bool      `json:"credential_scrubbed"`
	CreatedAt          time.Time `json:"created_at"`
}

// VolumeSnapshot is a sealed, immutable base *vfs.Volume snapshot of a git
// checkout that can be cloned in O(1) time (1 head PUT, 0 pack/meta copies)
// across N sandboxes via COWBackend + vfs.Reseal.
type VolumeSnapshot struct {
	TenantID    string
	RepoID      string
	BaseDir     string
	BaseBackend *CountingBackend
	Cipher      vfs.Cipher
	Round       uint64
	Meta        SnapshotMetadata
	BuildTime   time.Duration
	BuildPuts   BackendMetrics
	BuildDisk   JournalMetrics
}

// ClonedVolume is one sandbox's read-write *vfs.Volume cloned by reference
// from a VolumeSnapshot.
type ClonedVolume struct {
	TenantID      string
	SandboxID     string
	Volume        *vfs.Volume
	DeltaBackend  *CountingBackend
	Journal       *JournalTracker
	CloneDuration time.Duration
	ClonePuts     BackendMetrics
}

// VerifyNoCredentials inspects repoDir/.git/config and rejects embedded
// credentials, tokens, or Authorization headers (HIP-1342 §8.3.4).
func VerifyNoCredentials(repoDir string) error {
	cfgPath := filepath.Join(repoDir, ".git", "config")
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	lower := strings.ToLower(string(raw))
	forbidden := []string{
		"authorization:",
		"extraheader",
		"ghp_",
		"github_pat_",
		"glpat-",
		"x-access-token:",
	}
	for _, needle := range forbidden {
		if strings.Contains(lower, needle) {
			return fmt.Errorf("%w: matched %q in %s", ErrCredentialLeak, needle, cfgPath)
		}
	}
	// Check for user:pass@ in URL lines.
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToLower(trimmed), "url") {
			continue
		}
		_, val, ok := strings.Cut(trimmed, "=")
		if !ok {
			continue
		}
		val = strings.TrimSpace(val)
		if schemeIdx := strings.Index(val, "://"); schemeIdx > 0 {
			rest := val[schemeIdx+3:]
			if slashIdx := strings.IndexByte(rest, '/'); slashIdx > 0 {
				rest = rest[:slashIdx]
			}
			if atIdx := strings.IndexByte(rest, '@'); atIdx > 0 && strings.Contains(rest[:atIdx], ":") {
				return fmt.Errorf("%w: embedded user:password URL in %s", ErrCredentialLeak, cfgPath)
			}
		}
	}
	return nil
}

// CreateVolumeSnapshot ingests a prepared git checkout directory (srcRepoDir)
// into a new sealed *vfs.Volume backed by baseStoreDir, persists encrypted
// SnapshotMetadata via PutExt, flushes all objects, verifies tree invariants,
// and closes the writer cleanly so the snapshot is ready for O(1) COW cloning.
func CreateVolumeSnapshot(ctx context.Context, cipher vfs.Cipher, tenantID, repoID, srcRepoDir, baseStoreDir, journalDir, commitSHA, branch string) (*VolumeSnapshot, error) {
	if err := VerifyNoCredentials(srcRepoDir); err != nil {
		return nil, err
	}
	rawBe, err := openFileBackend(ctx, baseStoreDir)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: open base backend: %w", err)
	}
	cntBe := NewCountingBackend(rawBe)
	jt := NewJournalTracker()
	cfg := vfs.Config{
		Backend:         cntBe,
		Cipher:          cipher,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: 256 << 20,
		Bound:           5 * time.Second,
		Disk:            jt.DiskFactory(),
	}
	start := time.Now()
	if err := vfs.Create(ctx, cfg); err != nil {
		_ = cntBe.Close()
		return nil, fmt.Errorf("gitvfs: create base volume: %w", err)
	}
	vol, err := vfs.Attach(ctx, cfg)
	if err != nil {
		_ = cntBe.Close()
		return nil, fmt.Errorf("gitvfs: attach base volume: %w", err)
	}

	files, totalBytes, err := IngestDirToVolume(ctx, vol, srcRepoDir)
	if err != nil {
		_ = vol.Close()
		return nil, fmt.Errorf("gitvfs: ingest checkout into volume: %w", err)
	}
	meta := SnapshotMetadata{
		TenantID:           tenantID,
		RepoID:             repoID,
		CommitSHA:          commitSHA,
		Branch:             branch,
		FileCount:          files,
		TotalBytes:         totalBytes,
		CredentialScrubbed: true,
		CreatedAt:          time.Now().UTC(),
	}
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		_ = vol.Close()
		return nil, err
	}
	if err := vol.PutExt(ctx, ExtName, []byte("snapshot-meta"), metaRaw); err != nil {
		_ = vol.Close()
		return nil, fmt.Errorf("gitvfs: write snapshot metadata extension: %w", err)
	}
	if err := vol.Flush(ctx); err != nil {
		_ = vol.Close()
		return nil, fmt.Errorf("gitvfs: flush base volume: %w", err)
	}
	if err := vol.Check(ctx); err != nil {
		_ = vol.Close()
		return nil, fmt.Errorf("gitvfs: check base volume: %w", err)
	}
	// Close the writer Volume cleanly, then re-open a read-only handle on baseStoreDir
	// for shared O(1) COW reads (since vol.Close closes its Backend).
	buildPuts := cntBe.Snapshot()
	buildDisk := jt.Snapshot()
	if err := vol.Close(); err != nil {
		return nil, fmt.Errorf("gitvfs: close base volume: %w", err)
	}

	roRaw, err := openFileBackend(ctx, baseStoreDir)
	if err != nil {
		return nil, err
	}
	roCnt := NewCountingBackend(roRaw)

	return &VolumeSnapshot{
		TenantID:    tenantID,
		RepoID:      repoID,
		BaseDir:     baseStoreDir,
		BaseBackend: roCnt,
		Cipher:      cipher,
		Round:       1,
		Meta:        meta,
		BuildTime:   time.Since(start),
		BuildPuts:   buildPuts,
		BuildDisk:   buildDisk,
	}, nil
}

// Close releases the shared read-only base backend handle after all clones have
// been closed.
func (s *VolumeSnapshot) Close() error {
	if s == nil || s.BaseBackend == nil {
		return nil
	}
	return s.BaseBackend.Close()
}

// CloneVolumeByReference creates a new read-write sandbox *vfs.Volume in O(1)
// time by overlaying a thin delta backend on snap.BaseBackend and resealing
// only the single Head object to the new sandbox's place. Zero pack/ or meta/
// objects are copied and snap.BaseBackend receives zero writes.
func CloneVolumeByReference(ctx context.Context, snap *VolumeSnapshot, tenantID, sandboxID, deltaStoreDir, journalDir string) (*ClonedVolume, error) {
	if snap == nil {
		return nil, errors.New("gitvfs: VolumeSnapshot is required")
	}
	if snap.TenantID != "" && tenantID != snap.TenantID {
		return nil, fmt.Errorf("%w: tenant %q cannot clone snapshot owned by %q", ErrTenantMismatch, tenantID, snap.TenantID)
	}
	rawDelta, err := openFileBackend(ctx, deltaStoreDir)
	if err != nil {
		return nil, fmt.Errorf("gitvfs: open sandbox delta backend: %w", err)
	}
	cntDelta := NewCountingBackend(rawDelta)
	cow, err := NewCOWBackend(snap.BaseBackend, cntDelta)
	if err != nil {
		_ = cntDelta.Close()
		return nil, err
	}

	start := time.Now()
	resealCfg := vfs.Config{
		Backend: cow,
		Cipher:  snap.Cipher,
		Round:   snap.Round + 1,
	}
	if err := vfs.Reseal(ctx, resealCfg, snap.BaseBackend.String()); err != nil {
		_ = cntDelta.Close()
		return nil, fmt.Errorf("gitvfs: reseal COW clone %s: %w", sandboxID, err)
	}

	jt := NewJournalTracker()
	attachCfg := vfs.Config{
		Backend:         cow,
		Cipher:          snap.Cipher,
		Round:           snap.Round + 2,
		Journal:         journalDir,
		JournalCapacity: 128 << 20,
		Bound:           5 * time.Second,
		Disk:            jt.DiskFactory(),
	}
	vol, err := vfs.Attach(ctx, attachCfg)
	if err != nil {
		_ = cntDelta.Close()
		return nil, fmt.Errorf("gitvfs: attach COW clone %s: %w", sandboxID, err)
	}
	return &ClonedVolume{
		TenantID:      tenantID,
		SandboxID:     sandboxID,
		Volume:        vol,
		DeltaBackend:  cntDelta,
		Journal:       jt,
		CloneDuration: time.Since(start),
		ClonePuts:     cntDelta.Snapshot(),
	}, nil
}

// ReadSnapshotMetadata reads and unmarshals the sealed SnapshotMetadata
// extension stored in vol.
func ReadSnapshotMetadata(ctx context.Context, vol *vfs.Volume) (SnapshotMetadata, error) {
	raw, err := vol.GetExt(ctx, ExtName, []byte("snapshot-meta"))
	if err != nil {
		return SnapshotMetadata{}, err
	}
	var m SnapshotMetadata
	if err := json.Unmarshal(raw, &m); err != nil {
		return SnapshotMetadata{}, err
	}
	return m, nil
}

// IngestDirToVolume copies a host directory tree (directories, regular files,
// symlinks, permissions, and nanosecond mtimes) into vol starting at RootInode.
func IngestDirToVolume(ctx context.Context, vol *vfs.Volume, srcDir string) (fileCount int, totalBytes int64, err error) {
	dirInodes := map[string]vfs.InodeID{".": vfs.RootInode}
	err = filepath.WalkDir(srcDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, p)
		if err != nil || rel == "." {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		parentRel := path.Dir(relSlash)
		name := path.Base(relSlash)
		parentIno, ok := dirInodes[parentRel]
		if !ok {
			return fmt.Errorf("gitvfs: missing parent dir inode for %s", relSlash)
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		mtime := info.ModTime()
		switch {
		case info.IsDir():
			in, err := vol.Mkdir(ctx, parentIno, name, info.Mode().Perm())
			if err != nil {
				return err
			}
			_, _ = vol.SetAttr(ctx, in.ID, vfs.Change{Mtime: &mtime})
			dirInodes[relSlash] = in.ID
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			in, err := vol.Symlink(ctx, parentIno, name, target)
			if err != nil {
				return err
			}
			_, _ = vol.SetAttr(ctx, in.ID, vfs.Change{Mtime: &mtime})
			fileCount++
		case info.Mode().IsRegular():
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			in, err := vol.Create(ctx, parentIno, name, info.Mode().Perm())
			if err != nil {
				return err
			}
			if len(data) > 0 {
				f, err := vol.Open(ctx, in.ID)
				if err != nil {
					return err
				}
				if _, err := f.WriteAt(ctx, data, 0); err != nil {
					_ = f.Close()
					return err
				}
				if err := f.Close(); err != nil {
					return err
				}
			}
			_, _ = vol.SetAttr(ctx, in.ID, vfs.Change{Mtime: &mtime})
			fileCount++
			totalBytes += int64(len(data))
		}
		return nil
	})
	return fileCount, totalBytes, err
}

// ExportVolumeToDir materializes a *vfs.Volume tree into dstDir preserving
// regular files, symlinks, modes, and nanosecond mtimes.
func ExportVolumeToDir(ctx context.Context, vol *vfs.Volume, dstDir string) (fileCount int, totalBytes int64, err error) {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return 0, 0, err
	}
	var walk func(ino vfs.InodeID, hostDir string) error
	walk = func(ino vfs.InodeID, hostDir string) error {
		entries, err := vol.ReadDir(ctx, ino)
		if err != nil {
			return err
		}
		for _, e := range entries {
			st, err := vol.Stat(ctx, e.ID)
			if err != nil {
				return err
			}
			dstPath := filepath.Join(hostDir, e.Name)
			switch {
			case st.IsDir():
				if err := os.MkdirAll(dstPath, st.Mode.Perm()|0o700); err != nil {
					return err
				}
				if err := walk(e.ID, dstPath); err != nil {
					return err
				}
				_ = os.Chtimes(dstPath, st.Mtime, st.Mtime)
			case st.Mode&fs.ModeSymlink != 0:
				target, err := vol.Readlink(ctx, e.ID)
				if err != nil {
					return err
				}
				if err := os.Symlink(target, dstPath); err != nil {
					return err
				}
				fileCount++
			case st.Mode.IsRegular():
				data := make([]byte, st.Size)
				if st.Size > 0 {
					f, err := vol.Open(ctx, e.ID)
					if err != nil {
						return err
					}
					_, rerr := f.ReadAt(ctx, data, 0)
					cerr := f.Close()
					if rerr != nil && !errors.Is(rerr, io.EOF) {
						return rerr
					}
					if cerr != nil {
						return cerr
					}
				}
				perm := st.Mode.Perm()
				if perm == 0 {
					perm = 0o644
				}
				if err := os.WriteFile(dstPath, data, perm); err != nil {
					return err
				}
				_ = os.Chtimes(dstPath, st.Mtime, st.Mtime)
				fileCount++
				totalBytes += int64(len(data))
			}
		}
		return nil
	}
	if err := walk(vfs.RootInode, dstDir); err != nil {
		return fileCount, totalBytes, err
	}
	return fileCount, totalBytes, nil
}

// WriteVolumeFile creates or overwrites relPath inside vol, optionally syncing
// the file to the journal before Close.
func WriteVolumeFile(ctx context.Context, vol *vfs.Volume, relPath string, data []byte, perm fs.FileMode, syncBeforeClose bool) (vfs.InodeID, error) {
	clean := path.Clean(filepath.ToSlash(relPath))
	clean = strings.TrimPrefix(clean, "/")
	parts := strings.Split(clean, "/")
	dir := vfs.RootInode
	for _, seg := range parts[:len(parts)-1] {
		if seg == "" || seg == "." {
			continue
		}
		in, err := vol.Lookup(ctx, dir, seg)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return 0, err
			}
			in, err = vol.Mkdir(ctx, dir, seg, 0o755)
			if err != nil {
				return 0, err
			}
		}
		dir = in.ID
	}
	name := parts[len(parts)-1]
	in, err := vol.Lookup(ctx, dir, name)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return 0, err
		}
		if perm == 0 {
			perm = 0o644
		}
		in, err = vol.Create(ctx, dir, name, perm)
		if err != nil {
			return 0, err
		}
	}
	f, err := vol.Open(ctx, in.ID)
	if err != nil {
		return 0, err
	}
	if err := f.Truncate(ctx, 0); err != nil {
		_ = f.Close()
		return 0, err
	}
	if len(data) > 0 {
		if _, err := f.WriteAt(ctx, data, 0); err != nil {
			_ = f.Close()
			return 0, err
		}
	}
	if syncBeforeClose {
		if err := f.Sync(ctx); err != nil {
			_ = f.Close()
			return 0, err
		}
	}
	return in.ID, f.Close()
}

// CloseAndPublishCheck verifies two core S3-backed disk invariants on a real
// *vfs.Volume:
//  1. Clean file Close() performs ZERO synchronous backend/S3 PUT requests.
//  2. Acknowledged data is automatically published to the backend within maxBound
//     (5 s target under HIP-1342).
type CloseAndPublishResult struct {
	PutsDuringWriteSyncClose int64         `json:"puts_during_write_sync_close"`
	PublishLatencyMs         float64       `json:"publish_latency_ms"`
	PublishPuts              int64         `json:"publish_puts"`
	PublishBytes             int64         `json:"publish_bytes"`
	ZeroSyncPutOnClose       bool          `json:"zero_sync_put_on_close"`
	PublishedWithinBound     bool          `json:"published_within_bound"`
	Bound                    time.Duration `json:"bound"`
}

// VerifyCloseAndPublishBound runs a real write + journal Sync + file Close on
// vol, asserts zero synchronous backend PUTs during Write/Sync/Close, and
// measures the automatic background publication latency to the object store.
func VerifyCloseAndPublishBound(ctx context.Context, vol *vfs.Volume, be *CountingBackend, relPath string, payload []byte, maxBound time.Duration) (CloseAndPublishResult, error) {
	if maxBound <= 0 {
		maxBound = 5 * time.Second
	}
	// Ensure no prior unpublished backlog interferes with the measurement.
	if err := vol.Flush(ctx); err != nil {
		return CloseAndPublishResult{}, err
	}
	before := be.Snapshot()
	if _, err := WriteVolumeFile(ctx, vol, relPath, payload, 0o644, true); err != nil {
		return CloseAndPublishResult{}, err
	}
	afterClose := be.Snapshot()
	closeDelta := afterClose.Delta(before)
	ackTime := time.Now()

	// Wait for the Volume's background publisher to clear the backlog without
	// calling Flush(), proving automatic publication within maxBound.
	deadline := time.NewTimer(maxBound + 500*time.Millisecond)
	defer deadline.Stop()
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	var pubDur time.Duration
	published := false
	for !published {
		st := vol.State()
		if st.Backlog == 0 && vol.Stats().Pending == 0 && vol.Stats().Dirty == 0 {
			pubDur = time.Since(ackTime)
			published = true
			break
		}
		select {
		case <-ctx.Done():
			return CloseAndPublishResult{}, ctx.Err()
		case <-deadline.C:
			pubDur = time.Since(ackTime)
			published = false
			goto done
		case <-tick.C:
		}
	}
done:
	afterPub := be.Snapshot()
	pubDelta := afterPub.Delta(afterClose)
	return CloseAndPublishResult{
		PutsDuringWriteSyncClose: closeDelta.TotalPuts,
		PublishLatencyMs:         float64(pubDur.Microseconds()) / 1000.0,
		PublishPuts:              pubDelta.TotalPuts,
		PublishBytes:             pubDelta.PutBytes,
		ZeroSyncPutOnClose:       closeDelta.TotalPuts == 0,
		PublishedWithinBound:     published && pubDur <= maxBound,
		Bound:                    maxBound,
	}, nil
}
