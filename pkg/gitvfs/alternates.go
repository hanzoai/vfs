// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package gitvfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// ErrTenantMismatch is returned when a sandbox attempts to borrow an
	// alternates store or volume snapshot belonging to a different tenant.
	ErrTenantMismatch = errors.New("gitvfs: tenant isolation violation")

	// ErrAlternatesMutated is returned when the read-only shared object store's
	// cryptographic manifest differs from its sealed state.
	ErrAlternatesMutated = errors.New("gitvfs: shared alternates object store was mutated")

	// ErrActiveBorrowers is returned when SafePrune is requested while active
	// non-dissociated sandboxes still reference the alternates store.
	ErrActiveBorrowers = errors.New("gitvfs: cannot prune alternates store with active borrowers")
)

// ObjectCounts captures `git count-objects -v` metrics for a repository.
type ObjectCounts struct {
	LooseCount int    `json:"loose_count"`
	LooseKB    int    `json:"loose_kb"`
	InPack     int    `json:"in_pack"`
	Packs      int    `json:"packs"`
	PackKB     int    `json:"pack_kb"`
	OwnObjects int    `json:"own_objects"`
	OwnKB      int    `json:"own_kb"`
	Alternate  string `json:"alternate,omitempty"`
}

// CountObjectsInDir parses `git count-objects -v` for repoDir.
func CountObjectsInDir(ctx context.Context, r *Runner, repoDir string) (ObjectCounts, error) {
	res, err := r.Run(ctx, repoDir, "count-objects", "-v")
	if err != nil {
		return ObjectCounts{}, err
	}
	var c ObjectCounts
	for _, line := range strings.Split(res.Stdout, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		switch strings.TrimSpace(k) {
		case "count":
			c.LooseCount, _ = strconv.Atoi(v)
		case "size":
			c.LooseKB, _ = strconv.Atoi(v)
		case "in-pack":
			c.InPack, _ = strconv.Atoi(v)
		case "packs":
			c.Packs, _ = strconv.Atoi(v)
		case "size-pack":
			c.PackKB, _ = strconv.Atoi(v)
		case "alternate":
			c.Alternate = v
		}
	}
	c.OwnObjects = c.LooseCount + c.InPack
	c.OwnKB = c.LooseKB + c.PackKB
	return c, nil
}

// FileDigest records the immutable cryptographic identity of one file inside a
// shared read-only alternates object store.
type FileDigest struct {
	RelPath string
	Size    int64
	SHA256  string
}

// SnapshotDirManifest hashes every regular file under root in deterministic order.
func SnapshotDirManifest(root string) ([]FileDigest, error) {
	var out []FileDigest
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		h := sha256.New()
		n, copyErr := io.Copy(h, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		out = append(out, FileDigest{
			RelPath: filepath.ToSlash(rel),
			Size:    n,
			SHA256:  hex.EncodeToString(h.Sum(nil)),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RelPath < out[j].RelPath })
	return out, nil
}

// AlternatesStore is a tenant-scoped, read-only bare object store shared across
// sandboxes via `.git/objects/info/alternates`.
type AlternatesStore struct {
	TenantID string
	RepoID   string
	Path     string
	runner   *Runner

	mu        sync.Mutex
	sealed    bool
	manifest  []FileDigest
	borrowers map[string]struct{}
}

// NewAlternatesStore creates a tenant-scoped bare mirror from forgeURL at
// storeDir, packs it cleanly with bitmaps + commit-graph + MIDX, and records
// its initial cryptographic manifest.
func NewAlternatesStore(ctx context.Context, r *Runner, tenantID, repoID, forgeURL, storeDir string) (*AlternatesStore, error) {
	if tenantID == "" || repoID == "" {
		return nil, errors.New("gitvfs: tenantID and repoID are required")
	}
	if err := os.MkdirAll(filepath.Dir(storeDir), 0o755); err != nil {
		return nil, err
	}
	if _, err := r.Run(ctx, filepath.Dir(storeDir), "clone", "--bare", "--no-hardlinks", "--quiet", forgeURL, storeDir); err != nil {
		return nil, fmt.Errorf("gitvfs: create alternates store: %w", err)
	}
	if _, err := r.Run(ctx, storeDir, "repack", "-ad", "--write-midx", "--write-bitmap-index", "--quiet"); err != nil {
		return nil, fmt.Errorf("gitvfs: repack alternates store: %w", err)
	}
	if _, err := r.Run(ctx, storeDir, "commit-graph", "write", "--reachable", "--changed-paths"); err != nil {
		return nil, fmt.Errorf("gitvfs: alternates commit-graph: %w", err)
	}
	manifest, err := SnapshotDirManifest(filepath.Join(storeDir, "objects"))
	if err != nil {
		return nil, err
	}
	return &AlternatesStore{
		TenantID:  tenantID,
		RepoID:    repoID,
		Path:      storeDir,
		runner:    r,
		manifest:  manifest,
		borrowers: make(map[string]struct{}),
	}, nil
}

// ObjectsDir returns the absolute path written into `.git/objects/info/alternates`.
func (s *AlternatesStore) ObjectsDir() string {
	return filepath.Join(s.Path, "objects")
}

// SealReadOnly locks all files under the alternates store to 0444 and all
// directories to 0555 so the kernel enforces read-only sharing across sandboxes.
func (s *AlternatesStore) SealReadOnly() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dirs []string
	err := filepath.WalkDir(s.Path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs = append(dirs, p)
			return nil
		}
		return os.Chmod(p, 0o444)
	})
	if err != nil {
		return err
	}
	// Chmod directories deepest-first so parent chmod does not block children.
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chmod(dirs[i], 0o555); err != nil {
			return err
		}
	}
	s.sealed = true
	return nil
}

// UnsealWritable restores owner write permissions (0755 dirs, 0644 files) so
// teardown or replacement cleanup can proceed cleanly.
func (s *AlternatesStore) UnsealWritable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var files []string
	err := filepath.WalkDir(s.Path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.Chmod(p, 0o755)
		}
		files = append(files, p)
		return nil
	})
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.Chmod(f, 0o644); err != nil {
			return err
		}
	}
	s.sealed = false
	return nil
}

// VerifyUnmodified re-hashes every file under objects/ and returns
// ErrAlternatesMutated if any file was added, removed, or modified.
func (s *AlternatesStore) VerifyUnmodified() error {
	s.mu.Lock()
	orig := append([]FileDigest(nil), s.manifest...)
	s.mu.Unlock()

	cur, err := SnapshotDirManifest(s.ObjectsDir())
	if err != nil {
		return err
	}
	if len(cur) != len(orig) {
		return fmt.Errorf("%w: file count changed %d -> %d", ErrAlternatesMutated, len(orig), len(cur))
	}
	for i := range orig {
		if cur[i] != orig[i] {
			return fmt.Errorf("%w: mismatch at %s (want %s, got %s)", ErrAlternatesMutated, orig[i].RelPath, orig[i].SHA256, cur[i].SHA256)
		}
	}
	return nil
}

// AddBorrower registers sandboxID as an active borrower of this store.
func (s *AlternatesStore) AddBorrower(sandboxID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.borrowers[sandboxID] = struct{}{}
}

// RemoveBorrower removes sandboxID from the active borrower set (e.g. after
// dissociation or sandbox termination).
func (s *AlternatesStore) RemoveBorrower(sandboxID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.borrowers, sandboxID)
}

// ActiveBorrowers returns a sorted list of active borrower sandbox IDs.
func (s *AlternatesStore) ActiveBorrowers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.borrowers))
	for id := range s.borrowers {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// SafePrune refuses to prune objects while any borrower remains attached,
// preventing the alternates GC-prune corruption hazard.
func (s *AlternatesStore) SafePrune(ctx context.Context) error {
	if borrowers := s.ActiveBorrowers(); len(borrowers) > 0 {
		return fmt.Errorf("%w: %v", ErrActiveBorrowers, borrowers)
	}
	if err := s.UnsealWritable(); err != nil {
		return err
	}
	if _, err := s.runner.Run(ctx, s.Path, "gc", "--prune=now", "--quiet"); err != nil {
		return err
	}
	manifest, err := SnapshotDirManifest(s.ObjectsDir())
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.manifest = manifest
	s.mu.Unlock()
	return s.SealReadOnly()
}

// ForcePruneAllRefsForHazardTest deletes every ref in the alternates store and
// runs `git gc --prune=now` to demonstrate the shared-alternates prune hazard
// in negative-regime tests and benchmarks.
func (s *AlternatesStore) ForcePruneAllRefsForHazardTest(ctx context.Context) error {
	if err := s.UnsealWritable(); err != nil {
		return err
	}
	res, err := s.runner.Run(ctx, s.Path, "for-each-ref", "--format=%(refname)")
	if err != nil {
		return err
	}
	for _, ref := range splitNonEmptyLines(res.Stdout) {
		_, _ = s.runner.Run(ctx, s.Path, "update-ref", "-d", ref)
	}
	// Point HEAD to an unborn ref so all commits become unreachable.
	_, _ = s.runner.Run(ctx, s.Path, "symbolic-ref", "HEAD", "refs/heads/__pruned__")
	if _, err := s.runner.Run(ctx, s.Path, "gc", "--prune=now", "--quiet"); err != nil {
		return err
	}
	return nil
}

// TenantRegistry enforces strict per-tenant isolation over shared alternates
// stores and sandbox ref namespaces.
type TenantRegistry struct {
	mu     sync.RWMutex
	stores map[string]*AlternatesStore // key: repoID -> store (single-tenant lookup checks TenantID)
	byKey  map[string]*AlternatesStore // key: tenantID + "/" + repoID
}

// NewTenantRegistry creates an empty TenantRegistry.
func NewTenantRegistry() *TenantRegistry {
	return &TenantRegistry{
		stores: make(map[string]*AlternatesStore),
		byKey:  make(map[string]*AlternatesStore),
	}
}

// Register adds store to the registry under its (TenantID, RepoID) pair.
func (reg *TenantRegistry) Register(store *AlternatesStore) error {
	if store == nil || store.TenantID == "" || store.RepoID == "" {
		return errors.New("gitvfs: invalid alternates store")
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	key := store.TenantID + "/" + store.RepoID
	reg.byKey[key] = store
	reg.stores[store.RepoID] = store
	return nil
}

// Resolve returns the AlternatesStore for (tenantID, repoID), returning
// ErrTenantMismatch if repoID belongs to a different tenant.
func (reg *TenantRegistry) Resolve(tenantID, repoID string) (*AlternatesStore, error) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	if st, ok := reg.byKey[tenantID+"/"+repoID]; ok {
		return st, nil
	}
	if other, exists := reg.stores[repoID]; exists && other.TenantID != tenantID {
		return nil, fmt.Errorf("%w: tenant %q cannot borrow repo %q owned by tenant %q", ErrTenantMismatch, tenantID, repoID, other.TenantID)
	}
	return nil, fmt.Errorf("gitvfs: no alternates store for %s/%s: %w", tenantID, repoID, fs.ErrNotExist)
}

// SandboxRefNamespace returns the tenant- and sandbox-scoped branch name to
// prevent cross-sandbox ref collisions or overwrites on a shared forge.
func SandboxRefNamespace(tenantID, sandboxID, branch string) string {
	clean := func(s string) string {
		s = strings.TrimSpace(s)
		s = strings.ReplaceAll(s, " ", "-")
		s = strings.ReplaceAll(s, "..", "-")
		return s
	}
	return fmt.Sprintf("sandbox/%s/%s/%s", clean(tenantID), clean(sandboxID), clean(branch))
}

// SandboxCloneOptions configures a sandbox git checkout.
type SandboxCloneOptions struct {
	TenantID   string
	SandboxID  string
	RepoID     string
	RemoteURL  string
	TargetDir  string
	Profile    Profile
	Alternates *AlternatesStore // optional direct reference
	Registry   *TenantRegistry  // optional tenant-checked lookup
	Dissociate bool
}

// SandboxClone represents a single isolated sandbox git working tree.
type SandboxClone struct {
	TenantID       string
	SandboxID      string
	RepoID         string
	Dir            string
	Profile        Profile
	Alternates     *AlternatesStore
	AlternatesPath string
	CloneDuration  time.Duration
	runner         *Runner
}

// PushResult records a verified sandbox commit + push round-trip to the forge.
type PushResult struct {
	Branch      string        `json:"branch"`
	CommitSHA   string        `json:"commit_sha"`
	CommitTime  time.Duration `json:"commit_time"`
	PushTime    time.Duration `json:"push_time"`
	ForgeSHA    string        `json:"forge_sha"`
	ForgeFsckOK bool          `json:"forge_fsck_ok"`
	SandboxFsck bool          `json:"sandbox_fsck"`
}

// CloneByReference creates an isolated sandbox checkout using a tenant-scoped
// read-only alternates store (when available) and applies the requested Profile.
func CloneByReference(ctx context.Context, r *Runner, opts SandboxCloneOptions) (*SandboxClone, error) {
	if r == nil {
		return nil, errors.New("gitvfs: runner is required")
	}
	if opts.RemoteURL == "" || opts.TargetDir == "" {
		return nil, errors.New("gitvfs: RemoteURL and TargetDir are required")
	}
	altStore := opts.Alternates
	if altStore != nil && opts.TenantID != "" && altStore.TenantID != opts.TenantID {
		return nil, fmt.Errorf("%w: sandbox tenant %q cannot use alternates owned by %q", ErrTenantMismatch, opts.TenantID, altStore.TenantID)
	}
	if altStore == nil && opts.Registry != nil && opts.RepoID != "" {
		st, err := opts.Registry.Resolve(opts.TenantID, opts.RepoID)
		if err != nil {
			if errors.Is(err, ErrTenantMismatch) {
				return nil, err
			}
			// Missing store falls back cleanly to direct forge clone (HIP-1342 §8.2.7).
		} else {
			altStore = st
		}
	}

	if err := os.MkdirAll(filepath.Dir(opts.TargetDir), 0o755); err != nil {
		return nil, err
	}
	fsmon := r.FSMonitorSupported(ctx)
	altPath := ""
	if altStore != nil {
		altPath = altStore.Path
	}
	args := opts.Profile.CloneArgs(opts.RemoteURL, opts.TargetDir, altPath, fsmon)
	if opts.Dissociate && altPath != "" {
		// Insert --dissociate right after --reference-if-able <store>.
		args = append(args[:len(args)-2], "--dissociate", args[len(args)-2], args[len(args)-1])
	}

	start := time.Now()
	if _, err := r.Run(ctx, filepath.Dir(opts.TargetDir), args...); err != nil {
		return nil, fmt.Errorf("gitvfs: clone sandbox %s: %w", opts.SandboxID, err)
	}
	if err := opts.Profile.OptimizeRepo(ctx, r, opts.TargetDir); err != nil {
		return nil, err
	}
	cloneDur := time.Since(start)

	sc := &SandboxClone{
		TenantID:      opts.TenantID,
		SandboxID:     opts.SandboxID,
		RepoID:        opts.RepoID,
		Dir:           opts.TargetDir,
		Profile:       opts.Profile,
		Alternates:    altStore,
		CloneDuration: cloneDur,
		runner:        r,
	}
	altFile := filepath.Join(opts.TargetDir, ".git", "objects", "info", "alternates")
	if b, err := os.ReadFile(altFile); err == nil {
		sc.AlternatesPath = strings.TrimSpace(string(b))
		if altStore != nil && opts.SandboxID != "" {
			altStore.AddBorrower(opts.SandboxID)
		}
	}
	return sc, nil
}

// CountOwnObjects reports objects stored locally in this sandbox (excluding
// objects borrowed from alternates).
func (sc *SandboxClone) CountOwnObjects(ctx context.Context) (ObjectCounts, error) {
	return CountObjectsInDir(ctx, sc.runner, sc.Dir)
}

// Dissociate materializes all borrowed objects into the sandbox's own pack and
// removes `.git/objects/info/alternates` so the sandbox survives store pruning.
func (sc *SandboxClone) Dissociate(ctx context.Context) error {
	if _, err := sc.runner.Run(ctx, sc.Dir, "repack", "-a", "-d", "--quiet"); err != nil {
		return fmt.Errorf("gitvfs: dissociate repack: %w", err)
	}
	altFile := filepath.Join(sc.Dir, ".git", "objects", "info", "alternates")
	if err := os.Remove(altFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	sc.AlternatesPath = ""
	if sc.Alternates != nil && sc.SandboxID != "" {
		sc.Alternates.RemoveBorrower(sc.SandboxID)
	}
	return nil
}

// CommitAndPush creates a file edit on an isolated sandbox branch, commits it,
// pushes back to forge, and verifies both forge and sandbox fsck integrity.
func (sc *SandboxClone) CommitAndPush(ctx context.Context, forge *Forge, relPath string, content []byte, msg, branchSuffix string) (PushResult, error) {
	branch := branchSuffix
	if sc.TenantID != "" && sc.SandboxID != "" {
		branch = SandboxRefNamespace(sc.TenantID, sc.SandboxID, branchSuffix)
	}
	if _, err := sc.runner.Run(ctx, sc.Dir, "checkout", "--quiet", "-B", branch); err != nil {
		return PushResult{}, fmt.Errorf("gitvfs: checkout sandbox branch %s: %w", branch, err)
	}
	fullPath := filepath.Join(sc.Dir, relPath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return PushResult{}, err
	}
	if err := os.WriteFile(fullPath, content, 0o644); err != nil {
		return PushResult{}, err
	}
	commitStart := time.Now()
	if _, err := sc.runner.Run(ctx, sc.Dir, "add", "--", relPath); err != nil {
		return PushResult{}, fmt.Errorf("gitvfs: git add: %w", err)
	}
	if _, err := sc.runner.Run(ctx, sc.Dir, "commit", "--quiet", "-m", msg); err != nil {
		return PushResult{}, fmt.Errorf("gitvfs: git commit: %w", err)
	}
	commitDur := time.Since(commitStart)
	headRes, err := sc.runner.Run(ctx, sc.Dir, "rev-parse", "HEAD")
	if err != nil {
		return PushResult{}, err
	}
	commitSHA := strings.TrimSpace(headRes.Stdout)

	pushDur, forgeSHA, err := MeasureForgePushRoundTrip(ctx, sc.runner, forge, sc.Dir, branch)
	if err != nil {
		return PushResult{}, fmt.Errorf("gitvfs: git push: %w", err)
	}
	if forgeSHA != commitSHA {
		return PushResult{}, fmt.Errorf("gitvfs: forge ref mismatch: pushed %s, forge has %s", commitSHA, forgeSHA)
	}
	_, sandboxFsckErr := sc.runner.Run(ctx, sc.Dir, "fsck", "--no-dangling")
	_, forgeFsckErr := sc.runner.Run(ctx, forge.Path, "fsck", "--no-dangling")
	return PushResult{
		Branch:      branch,
		CommitSHA:   commitSHA,
		CommitTime:  commitDur,
		PushTime:    pushDur,
		ForgeSHA:    forgeSHA,
		ForgeFsckOK: forgeFsckErr == nil,
		SandboxFsck: sandboxFsckErr == nil,
	}, errors.Join(sandboxFsckErr, forgeFsckErr)
}
