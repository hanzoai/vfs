// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package most

import (
	"bytes"
	"container/list"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

type cacheKey struct {
	scope  Scope  // canonicalized: ScopePrivate or ScopeSharedImmutable
	tenant string // empty for immutable shared; tenantID for ScopePrivate
	volID  string // empty for immutable shared; volID for ScopePrivate
	key    string // logical key or range key ("<key>#<off>+<n>")
}

func (k cacheKey) hotKey() string {
	if k.scope.isImmutableShared() {
		return "S:" + k.key
	}
	return "P:" + k.tenant + "\x00" + k.volID + "\x00" + k.key
}

func (k cacheKey) diskRelPath() string {
	h := hashHex("disk", k.scope.String(), k.tenant, k.volID, k.key)
	if k.scope.isImmutableShared() {
		return filepath.Join("shared", h[:2], h)
	}
	th := hashHex("tenantdir", k.tenant, k.volID)[:24]
	return filepath.Join("priv", th, h[:2], h)
}

type sharedKeyMeta struct {
	artifactID string
	scope      Scope
	digest     [blake3.Size]byte
	size       int64
}

type ramEntry struct {
	id         cacheKey
	artifactID string
	plain      []byte
	cost       int64
	ver        backend.Version
	digest     [blake3.Size]byte
	holders    map[string]*list.Element
}

type nvmeEntry struct {
	id         cacheKey
	artifactID string
	relPath    string
	cost       int64
	ver        backend.Version
	digest     [blake3.Size]byte
	holders    map[string]*list.Element
	isShared   bool
}

type tenantState struct {
	id            string
	volID         string
	tvHash        string
	cipher        Cipher
	quotaRAM      int64
	quotaNVMe     int64
	sharedAllowed bool
	sharedPrefix  []string

	lifecycleMu sync.RWMutex
	revoked     bool

	ramUsed  int64
	nvmeUsed int64
	ramLRU   *list.List // elements hold cacheKey
	nvmeLRU  *list.List // elements hold cacheKey

	reads          atomic.Int64
	backendGets    atomic.Int64
	ramHits        atomic.Int64
	nvmeHits       atomic.Int64
	mirrorPromotes atomic.Int64
	migrationWrite atomic.Int64
	nvmeWrites     atomic.Int64
	latencies      latencyRecorder
}

func (t *tenantState) allowsShared(key string) bool {
	if !t.sharedAllowed {
		return false
	}
	if len(t.sharedPrefix) == 0 {
		return true
	}
	for _, p := range t.sharedPrefix {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	done chan struct{}
	val  []byte
	ver  backend.Version
	err  error
}

func (f *flightGroup) do(ctx context.Context, k string, fn func() ([]byte, backend.Version, error)) ([]byte, backend.Version, error) {
	for {
		f.mu.Lock()
		if f.m == nil {
			f.m = make(map[string]*flightCall)
		}
		if c, ok := f.m[k]; ok {
			f.mu.Unlock()
			select {
			case <-c.done:
			case <-ctx.Done():
				return nil, "", ctx.Err()
			}
			if c.err != nil && (errors.Is(c.err, context.Canceled) || errors.Is(c.err, context.DeadlineExceeded)) && ctx.Err() == nil {
				continue
			}
			return append([]byte(nil), c.val...), c.ver, c.err
		}
		c := &flightCall{done: make(chan struct{})}
		f.m[k] = c
		f.mu.Unlock()

		c.val, c.ver, c.err = fn()

		f.mu.Lock()
		delete(f.m, k)
		f.mu.Unlock()
		close(c.done)
		return append([]byte(nil), c.val...), c.ver, c.err
	}
}

// Engine manages a tenant-isolated, byte-bounded host RAM pool and encrypted
// disk-backed NVMe cache tier in front of an underlying backend.Backend.
type Engine struct {
	be           backend.Backend
	cfg          Config
	hostCipher   Cipher
	ephemHost    *ephemeralHostCipher
	manifestCiph Cipher
	ephemManif   *ephemeralHostCipher
	ownNVMeDir   bool

	keyLocks [64]sync.Mutex
	flights  flightGroup

	mu          sync.RWMutex
	closed      bool
	tenants     map[string]*tenantState
	ramEntries  map[cacheKey]*ramEntry
	nvmeEntries map[cacheKey]*nvmeEntry
	globalRAM   int64
	globalNVMe  int64
	hot         *hotTracker
	sharedMeta  map[string]sharedKeyMeta
	manifests   map[string]ArtifactManifest

	latencies      latencyRecorder
	totalReads     atomic.Int64
	backendGets    atomic.Int64
	ramHits        atomic.Int64
	nvmeHits       atomic.Int64
	mirrorPromotes atomic.Int64
	migrationWrite atomic.Int64
	nvmeWrites     atomic.Int64
	ramEvictions   atomic.Int64
	nvmeEvictions  atomic.Int64
	integrityErrs  atomic.Int64
	unauthAttempts atomic.Int64
}

// NewEngine constructs a MOST Engine wrapping be with the given configuration.
// Every configured Cipher (HostCacheCipher, SharedManifestCipher) is verified
// via checkCipher so identity/plaintext ciphers fail closed immediately.
func NewEngine(be backend.Backend, cfg Config) (*Engine, error) {
	if be == nil {
		return nil, fmt.Errorf("%w: nil backend", ErrInvalidConfig)
	}
	norm, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if norm.SharedPrefix != "" {
		if err := backend.ValidPrefix(norm.SharedPrefix); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
	}

	var hostCipher Cipher
	var ephemHost *ephemeralHostCipher
	if norm.HostCacheCipher != nil {
		if err := checkCipher(norm.HostCacheCipher); err != nil {
			return nil, fmt.Errorf("%w: HostCacheCipher: %v", ErrInvalidConfig, err)
		}
		hostCipher = norm.HostCacheCipher
	} else {
		ephemHost, err = newEphemeralHostCipher()
		if err != nil {
			return nil, err
		}
		if err := checkCipher(ephemHost); err != nil {
			ephemHost.wipe()
			return nil, err
		}
		hostCipher = ephemHost
	}

	var manifestCipher Cipher
	var ephemManif *ephemeralHostCipher
	if norm.SharedManifestCipher != nil {
		if err := checkCipher(norm.SharedManifestCipher); err != nil {
			if ephemHost != nil {
				ephemHost.wipe()
			}
			return nil, fmt.Errorf("%w: SharedManifestCipher: %v", ErrInvalidConfig, err)
		}
		manifestCipher = norm.SharedManifestCipher
	} else {
		ephemManif, err = newEphemeralHostCipher()
		if err != nil {
			if ephemHost != nil {
				ephemHost.wipe()
			}
			return nil, err
		}
		manifestCipher = ephemManif
	}

	ownDir := false
	nvmeDir := norm.NVMeDir
	if nvmeDir == "" {
		d, err := os.MkdirTemp("", "most-nvme-*")
		if err != nil {
			if ephemHost != nil {
				ephemHost.wipe()
			}
			if ephemManif != nil {
				ephemManif.wipe()
			}
			return nil, fmt.Errorf("most: create nvme temp dir: %w", err)
		}
		nvmeDir = d
		ownDir = true
	} else {
		if err := os.MkdirAll(nvmeDir, 0o700); err != nil {
			if ephemHost != nil {
				ephemHost.wipe()
			}
			if ephemManif != nil {
				ephemManif.wipe()
			}
			return nil, fmt.Errorf("most: create nvme dir: %w", err)
		}
	}
	norm.NVMeDir = nvmeDir
	for _, sub := range []string{"shared", "priv", ".tmp"} {
		if err := os.MkdirAll(filepath.Join(nvmeDir, sub), 0o700); err != nil {
			if ownDir {
				_ = os.RemoveAll(nvmeDir)
			}
			if ephemHost != nil {
				ephemHost.wipe()
			}
			if ephemManif != nil {
				ephemManif.wipe()
			}
			return nil, fmt.Errorf("most: init nvme subdir %s: %w", sub, err)
		}
	}
	return &Engine{
		be:           be,
		cfg:          norm,
		hostCipher:   hostCipher,
		ephemHost:    ephemHost,
		manifestCiph: manifestCipher,
		ephemManif:   ephemManif,
		ownNVMeDir:   ownDir,
		tenants:      make(map[string]*tenantState),
		ramEntries:   make(map[cacheKey]*ramEntry),
		nvmeEntries:  make(map[cacheKey]*nvmeEntry),
		hot:          newHotTracker(norm.DecayHalfLife, norm.WindowOps, norm.MaxHotEntries),
		sharedMeta:   make(map[string]sharedKeyMeta),
		manifests:    make(map[string]ArtifactManifest),
	}, nil
}

// Close zeros all cached RAM buffers and ephemeral host keys, drops metadata,
// and cleans up the NVMe directory if it was auto-created by NewEngine.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	for _, re := range e.ramEntries {
		clear(re.plain)
		re.plain = nil
	}
	e.ramEntries = nil
	e.nvmeEntries = nil
	e.sharedMeta = nil
	e.manifests = nil
	e.globalRAM = 0
	e.globalNVMe = 0
	for _, t := range e.tenants {
		t.revoked = true
		t.cipher = nil
		t.ramUsed = 0
		t.nvmeUsed = 0
		t.ramLRU.Init()
		t.nvmeLRU.Init()
	}
	if e.ephemHost != nil {
		e.ephemHost.wipe()
	}
	if e.ephemManif != nil {
		e.ephemManif.wipe()
	}
	e.mu.Unlock()
	if e.ownNVMeDir && e.cfg.NVMeDir != "" {
		return os.RemoveAll(e.cfg.NVMeDir)
	}
	return nil
}

// RegisterTenant registers a tenant binding with explicit volume ID, verified
// per-tenant Cipher (when non-nil), and RAM/NVMe byte quotas (0 disables caching
// in that tier; UnlimitedQuota / < 0 inherits Engine global bounds).
func (e *Engine) RegisterTenant(tenantID, volID string, cipher Cipher, quotaRAM, quotaNVMe int64) error {
	if tenantID == "" || volID == "" {
		return fmt.Errorf("%w: tenantID and volID are required", ErrInvalidConfig)
	}
	if cipher != nil {
		if err := checkCipher(cipher); err != nil {
			return fmt.Errorf("%w: tenant %q cipher: %v", ErrInvalidConfig, tenantID, err)
		}
	}
	tvHash := hashHex("tv", tenantID, volID)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrTenantUnauthorized
	}
	if prev, ok := e.tenants[tenantID]; ok && !prev.revoked {
		return fmt.Errorf("%w: tenant %q already registered", ErrInvalidConfig, tenantID)
	}
	e.tenants[tenantID] = &tenantState{
		id:            tenantID,
		volID:         volID,
		tvHash:        tvHash,
		cipher:        cipher,
		quotaRAM:      quotaRAM,
		quotaNVMe:     quotaNVMe,
		sharedAllowed: true,
		ramLRU:        list.New(),
		nvmeLRU:       list.New(),
	}
	return nil
}

// SetSharedAccess updates the shared/public immutable toolchain ACL for a tenant.
// When allowed is false, all shared/public immutable access is immediately blocked
// and any resident shared RAM/NVMe references held by tenantID are detached.
// When allowed is true and allowedPrefixes is empty, all registered shared keys
// are allowed; otherwise only keys matching at least one prefix are permitted.
func (e *Engine) SetSharedAccess(tenantID string, allowed bool, allowedPrefixes ...string) error {
	e.mu.RLock()
	t := e.tenants[tenantID]
	e.mu.RUnlock()
	if t == nil {
		return ErrTenantUnauthorized
	}
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()

	var removeFiles []string
	e.mu.Lock()
	if t.revoked {
		e.mu.Unlock()
		return ErrTenantUnauthorized
	}
	t.sharedAllowed = allowed
	t.sharedPrefix = append([]string(nil), allowedPrefixes...)
	// Detach any shared RAM/NVMe holders that this tenant is no longer permitted to access.
	removeFiles = e.detachDisallowedSharedLocked(t)
	e.mu.Unlock()

	for _, p := range removeFiles {
		_ = os.Remove(p)
	}
	return nil
}

// RevokeTenant immediately revokes tenantID, zeros and purges all its RAM buffers,
// removes its private NVMe cache blocks from disk, and detaches it from shared
// entries without disturbing any healthy tenants.
func (e *Engine) RevokeTenant(tenantID string) error {
	e.mu.RLock()
	t := e.tenants[tenantID]
	e.mu.RUnlock()
	if t == nil {
		return ErrTenantUnauthorized
	}

	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()

	var privFiles []string
	var orphanSharedFiles []string
	var privDir string

	e.mu.Lock()
	if !t.revoked {
		t.revoked = true
		t.cipher = nil
		t.sharedAllowed = false
		privDir = filepath.Join(e.cfg.NVMeDir, "priv", hashHex("tenantdir", t.id, t.volID)[:24])

		for el := t.ramLRU.Front(); el != nil; el = el.Next() {
			k := el.Value.(cacheKey)
			e.hot.forget(k.hotKey())
			if re, ok := e.ramEntries[k]; ok {
				delete(re.holders, t.id)
				if len(re.holders) == 0 {
					clear(re.plain)
					re.plain = nil
					delete(e.ramEntries, k)
					e.globalRAM -= re.cost
				}
			}
		}
		t.ramLRU.Init()
		t.ramUsed = 0

		for el := t.nvmeLRU.Front(); el != nil; el = el.Next() {
			k := el.Value.(cacheKey)
			e.hot.forget(k.hotKey())
			if ne, ok := e.nvmeEntries[k]; ok {
				delete(ne.holders, t.id)
				if len(ne.holders) == 0 {
					delete(e.nvmeEntries, k)
					e.globalNVMe -= ne.cost
					p := filepath.Join(e.cfg.NVMeDir, ne.relPath)
					if ne.isShared {
						orphanSharedFiles = append(orphanSharedFiles, p)
					} else {
						privFiles = append(privFiles, p)
					}
				}
			}
		}
		t.nvmeLRU.Init()
		t.nvmeUsed = 0
	}
	e.mu.Unlock()

	for _, p := range privFiles {
		_ = os.Remove(p)
	}
	for _, p := range orphanSharedFiles {
		_ = os.Remove(p)
	}
	if privDir != "" {
		_ = os.RemoveAll(privDir)
	}
	return nil
}

func (e *Engine) detachDisallowedSharedLocked(t *tenantState) []string {
	var orphanFiles []string
	var next *list.Element
	for el := t.ramLRU.Front(); el != nil; el = next {
		next = el.Next()
		k := el.Value.(cacheKey)
		if !k.scope.isImmutableShared() {
			continue
		}
		baseKey, _, _ := splitRangeKey(k.key)
		if t.allowsShared(baseKey) {
			continue
		}
		t.ramLRU.Remove(el)
		if re, ok := e.ramEntries[k]; ok {
			delete(re.holders, t.id)
			t.ramUsed -= re.cost
			if len(re.holders) == 0 {
				clear(re.plain)
				re.plain = nil
				delete(e.ramEntries, k)
				e.globalRAM -= re.cost
			}
		}
	}
	for el := t.nvmeLRU.Front(); el != nil; el = next {
		next = el.Next()
		k := el.Value.(cacheKey)
		if !k.scope.isImmutableShared() {
			continue
		}
		baseKey, _, _ := splitRangeKey(k.key)
		if t.allowsShared(baseKey) {
			continue
		}
		t.nvmeLRU.Remove(el)
		if ne, ok := e.nvmeEntries[k]; ok {
			delete(ne.holders, t.id)
			t.nvmeUsed -= ne.cost
			if len(ne.holders) == 0 {
				delete(e.nvmeEntries, k)
				e.globalNVMe -= ne.cost
				orphanFiles = append(orphanFiles, filepath.Join(e.cfg.NVMeDir, ne.relPath))
			}
		}
	}
	return orphanFiles
}

func splitRangeKey(k string) (baseKey string, isRange bool, err error) {
	idx := strings.LastIndexByte(k, '#')
	if idx < 0 {
		return k, false, nil
	}
	return k[:idx], true, nil
}

func (e *Engine) resolveManifestCipher(c Cipher) (Cipher, error) {
	if c != nil {
		if err := checkCipher(c); err != nil {
			return nil, err
		}
		return c, nil
	}
	return e.manifestCiph, nil
}

func (e *Engine) manifestBackendKey(artifactID string) (string, error) {
	if artifactID == "" {
		return "", fmt.Errorf("%w: empty artifactID", ErrInvalidConfig)
	}
	if err := backend.ValidPrefix(artifactID); err != nil {
		return "", fmt.Errorf("%w: invalid artifactID %q: %v", ErrInvalidConfig, artifactID, err)
	}
	mh := hashHex("manifest-key", artifactID)
	return e.sharedBackendKey(manifestKeyDir + mh[:2] + "/" + mh), nil
}

func (e *Engine) putImmutableBackendObject(ctx context.Context, beKey string, payload []byte, wantDigest [blake3.Size]byte) error {
	_, err := e.be.PutIfAbsent(ctx, beKey, payload)
	if err == nil {
		return nil
	}
	if !errors.Is(err, backend.ErrConflict) {
		return err
	}
	// Conditional conflict: fetch existing object and verify byte-for-byte / BLAKE3 identity.
	existing, _, gerr := e.be.Get(ctx, beKey, backend.Unbounded)
	if gerr != nil {
		return fmt.Errorf("%w: conflict check failed for %s: %v", ErrImmutableConflict, beKey, gerr)
	}
	gotSum := blake3.Sum(existing)
	if int64(len(existing)) != int64(len(payload)) || subtle.ConstantTimeCompare(gotSum[:], wantDigest[:]) != 1 {
		e.integrityErrs.Add(1)
		return fmt.Errorf("%w: existing immutable object %s differs from published payload", ErrImmutableConflict, beKey)
	}
	return nil
}

func (e *Engine) registerManifestLocked(m ArtifactManifest) error {
	scope := m.Scope
	if scope == 0 {
		scope = ScopeSharedImmutable
	}
	if !scope.isImmutableShared() {
		return fmt.Errorf("%w: invalid manifest scope %v", ErrInvalidConfig, scope)
	}
	if prev, exists := e.manifests[m.ArtifactID]; exists {
		if !manifestsEqual(prev, m) {
			e.integrityErrs.Add(1)
			return fmt.Errorf("%w: manifest %q conflicts with already-registered manifest", ErrImmutableConflict, m.ArtifactID)
		}
		return nil
	}
	// First check all entries for conflicts before mutating Engine state atomically.
	for k, entry := range m.Entries {
		if prevMeta, ok := e.sharedMeta[k]; ok {
			if prevMeta.digest != entry.Digest || prevMeta.size != entry.Size {
				e.integrityErrs.Add(1)
				return fmt.Errorf("%w: shared key %q already registered with different digest/size", ErrImmutableConflict, k)
			}
		}
	}
	cpEntries := make(map[string]ManifestEntry, len(m.Entries))
	for k, entry := range m.Entries {
		cpEntries[k] = entry
		e.sharedMeta[k] = sharedKeyMeta{
			artifactID: m.ArtifactID,
			scope:      scope,
			digest:     entry.Digest,
			size:       entry.Size,
		}
	}
	e.manifests[m.ArtifactID] = ArtifactManifest{
		ArtifactID: m.ArtifactID,
		Scope:      scope,
		Version:    m.Version,
		Entries:    cpEntries,
	}
	return nil
}

func manifestsEqual(a, b ArtifactManifest) bool {
	sa, sb := a.Scope, b.Scope
	if sa == 0 {
		sa = ScopeSharedImmutable
	}
	if sb == 0 {
		sb = ScopeSharedImmutable
	}
	if a.ArtifactID != b.ArtifactID || sa != sb || a.Version != b.Version || len(a.Entries) != len(b.Entries) {
		return false
	}
	for k, ea := range a.Entries {
		eb, ok := b.Entries[k]
		if !ok || ea.Digest != eb.Digest || ea.Size != eb.Size {
			return false
		}
	}
	return true
}

// PublishSharedArtifact conditionally publishes an immutable set of shared
// toolchain objects via backend.PutIfAbsent, seals an ArtifactManifest with
// manifestCipher (domain-separated AAD ["most-manifest-v1" | artifactID]),
// conditionally publishes the manifest via PutIfAbsent, and atomically registers
// the verified entries in Engine.
//
// Concurrent or retry calls with identical payloads and manifest entries succeed
// idempotently; any attempt to replace an existing key or manifest with different
// content fails closed with ErrImmutableConflict.
func (e *Engine) PublishSharedArtifact(ctx context.Context, artifactID string, files map[string][]byte, manifestCipher Cipher) (*ArtifactManifest, error) {
	return e.PublishArtifactWithScope(ctx, artifactID, ScopeSharedImmutable, 1, files, manifestCipher)
}

// PublishPublicArtifact conditionally publishes an explicitly public toolchain
// artifact (ScopePublicImmutable) and its Cipher-authenticated ArtifactManifest.
func (e *Engine) PublishPublicArtifact(ctx context.Context, artifactID string, files map[string][]byte, manifestCipher Cipher) (*ArtifactManifest, error) {
	return e.PublishArtifactWithScope(ctx, artifactID, ScopePublicImmutable, 1, files, manifestCipher)
}

// PublishArtifactWithScope conditionally publishes immutable objects under
// ScopeSharedImmutable or ScopePublicImmutable and seals their ArtifactManifest.
func (e *Engine) PublishArtifactWithScope(ctx context.Context, artifactID string, scope Scope, version uint64, files map[string][]byte, manifestCipher Cipher) (*ArtifactManifest, error) {
	if !scope.isImmutableShared() {
		return nil, fmt.Errorf("%w: PublishArtifactWithScope requires immutable shared/public scope, got %v", ErrInvalidConfig, scope)
	}
	mc, err := e.resolveManifestCipher(manifestCipher)
	if err != nil {
		return nil, err
	}
	mKey, err := e.manifestBackendKey(artifactID)
	if err != nil {
		return nil, err
	}

	keys := slices.Sorted(maps.Keys(files))
	entries := make(map[string]ManifestEntry, len(keys))
	for _, k := range keys {
		if err := backend.ValidPrefix(k); err != nil {
			return nil, fmt.Errorf("%w: invalid shared key %q: %v", ErrInvalidConfig, k, err)
		}
		if strings.HasPrefix(k, manifestKeyDir) {
			return nil, fmt.Errorf("%w: reserved manifest prefix in key %q", ErrInvalidConfig, k)
		}
		data := files[k]
		sum := blake3.Sum(data)
		beKey := e.sharedBackendKey(k)
		if err := e.putImmutableBackendObject(ctx, beKey, data, sum); err != nil {
			return nil, err
		}
		entries[k] = ManifestEntry{
			Digest: sum,
			Size:   int64(len(data)),
		}
	}

	manifest := ArtifactManifest{
		ArtifactID: artifactID,
		Scope:      scope,
		Version:    version,
		Entries:    entries,
	}
	if err := e.publishAndRegisterManifest(ctx, mKey, manifest, mc); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// SnapshotPrefixManifest scans existing immutable objects in the Engine's
// shared prefix (e.g. a finalized read-only base Volume image), seals an
// authenticated ArtifactManifest via manifestCipher, conditionally writes it
// via PutIfAbsent, and registers all entries for fail-closed reads.
func (e *Engine) SnapshotPrefixManifest(ctx context.Context, artifactID string, scope Scope, manifestCipher Cipher) (*ArtifactManifest, error) {
	if !scope.isImmutableShared() {
		return nil, fmt.Errorf("%w: SnapshotPrefixManifest requires immutable shared/public scope", ErrInvalidConfig)
	}
	mc, err := e.resolveManifestCipher(manifestCipher)
	if err != nil {
		return nil, err
	}
	mKey, err := e.manifestBackendKey(artifactID)
	if err != nil {
		return nil, err
	}

	var basePrefix string
	if e.cfg.SharedPrefix != "" {
		basePrefix = e.cfg.SharedPrefix + "/"
	}
	objs, errCh := e.be.List(ctx, basePrefix)
	entries := make(map[string]ManifestEntry)
	for o := range objs {
		logical := strings.TrimPrefix(o.Key, basePrefix)
		if logical == "" || strings.HasPrefix(logical, manifestKeyDir) {
			continue
		}
		raw, _, err := e.be.Get(ctx, o.Key, backend.Unbounded)
		if err != nil {
			return nil, err
		}
		entries[logical] = ManifestEntry{
			Digest: blake3.Sum(raw),
			Size:   int64(len(raw)),
		}
	}
	if err := <-errCh; err != nil {
		return nil, err
	}

	manifest := ArtifactManifest{
		ArtifactID: artifactID,
		Scope:      scope,
		Version:    1,
		Entries:    entries,
	}
	if err := e.publishAndRegisterManifest(ctx, mKey, manifest, mc); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func (e *Engine) publishAndRegisterManifest(ctx context.Context, mKey string, manifest ArtifactManifest, mc Cipher) error {
	sealed, err := sealManifest(mc, manifest)
	if err != nil {
		return err
	}
	_, err = e.be.PutIfAbsent(ctx, mKey, sealed)
	if err != nil {
		if !errors.Is(err, backend.ErrConflict) {
			return err
		}
		// Conflict on manifest key: open & authenticate existing manifest and compare entries!
		existingRaw, _, gerr := e.be.Get(ctx, mKey, backend.Unbounded)
		if gerr != nil {
			return fmt.Errorf("%w: read conflicting manifest %q: %v", ErrImmutableConflict, manifest.ArtifactID, gerr)
		}
		existingMan, uerr := unsealManifest(mc, manifest.ArtifactID, existingRaw)
		if uerr != nil {
			e.integrityErrs.Add(1)
			return fmt.Errorf("%w: existing manifest %q failed authentication: %v", ErrImmutableConflict, manifest.ArtifactID, uerr)
		}
		if !manifestsEqual(existingMan, manifest) {
			e.integrityErrs.Add(1)
			return fmt.Errorf("%w: manifest %q already exists with different entries", ErrImmutableConflict, manifest.ArtifactID)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrTenantUnauthorized
	}
	return e.registerManifestLocked(manifest)
}

// LoadArtifactManifest fetches the sealed manifest for artifactID from the
// underlying backend, authenticates and decrypts it via manifestCipher, and
// atomically registers its (key -> digest, size) entries into Engine.
// Tampered or unauthenticated manifests fail closed with ErrIntegrityViolation.
func (e *Engine) LoadArtifactManifest(ctx context.Context, artifactID string, manifestCipher Cipher) (*ArtifactManifest, error) {
	mc, err := e.resolveManifestCipher(manifestCipher)
	if err != nil {
		e.integrityErrs.Add(1)
		return nil, err
	}
	mKey, err := e.manifestBackendKey(artifactID)
	if err != nil {
		return nil, err
	}
	raw, _, err := e.be.Get(ctx, mKey, backend.Unbounded)
	if err != nil {
		return nil, err
	}
	manifest, err := unsealManifest(mc, artifactID, raw)
	if err != nil {
		e.integrityErrs.Add(1)
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrTenantUnauthorized
	}
	if err := e.registerManifestLocked(manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// PublishSharedObject conditionally publishes a single immutable shared object
// under its own authenticated single-entry ArtifactManifest ("obj/"+key).
func (e *Engine) PublishSharedObject(ctx context.Context, key string, data []byte, manifestCipher Cipher) ([blake3.Size]byte, error) {
	if key == "" {
		return [blake3.Size]byte{}, fmt.Errorf("%w: empty shared key", ErrInvalidConfig)
	}
	if err := backend.ValidPrefix(key); err != nil {
		return [blake3.Size]byte{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	artID := "obj/" + key
	m, err := e.PublishSharedArtifact(ctx, artID, map[string][]byte{key: data}, manifestCipher)
	if err != nil {
		return [blake3.Size]byte{}, err
	}
	return m.Entries[key].Digest, nil
}

// PublishShared conditionally publishes a single immutable shared object via
// PutIfAbsent along with a Cipher-authenticated manifest sealed under the
// Engine's SharedManifestCipher, rejecting conflicting replacements with
// ErrImmutableConflict.
func (e *Engine) PublishShared(ctx context.Context, key string, data []byte) ([blake3.Size]byte, error) {
	return e.PublishSharedObject(ctx, key, data, nil)
}

func (e *Engine) sharedBackendKey(key string) string {
	if e.cfg.SharedPrefix == "" {
		return key
	}
	return e.cfg.SharedPrefix + "/" + key
}

func (e *Engine) privateEngineBackendKey(tvHash, key string) string {
	kh := hashHex("privkey", key)
	return "p/" + tvHash[:24] + "/" + kh[:2] + "/" + kh
}

func (e *Engine) privateVolumeBackendKey(tvHash, key string) string {
	return "v/" + tvHash[:24] + "/" + key
}

func (e *Engine) keyShard(k cacheKey) *sync.Mutex {
	h := blake3.Sum([]byte(k.hotKey()))
	return &e.keyLocks[h[0]&63]
}

func (e *Engine) getTenant(tenantID string) (*tenantState, error) {
	e.mu.RLock()
	closed := e.closed
	t := e.tenants[tenantID]
	e.mu.RUnlock()
	if closed || t == nil {
		e.unauthAttempts.Add(1)
		return nil, ErrTenantUnauthorized
	}
	return t, nil
}

func (e *Engine) authorizeLocked(t *tenantState, scope Scope, logicalKey string) error {
	if e.closed || t.revoked {
		return ErrTenantUnauthorized
	}
	if scope == ScopePrivate {
		if t.cipher == nil {
			return ErrTenantUnauthorized
		}
		return nil
	}
	if scope.isImmutableShared() {
		baseKey, _, _ := splitRangeKey(logicalKey)
		if !t.allowsShared(baseKey) {
			return ErrTenantUnauthorized
		}
		return nil
	}
	return ErrTenantUnauthorized
}

func (e *Engine) authorizeSharedKeyLocked(t *tenantState, reqScope Scope, baseKey string, meta sharedKeyMeta) error {
	if err := e.authorizeLocked(t, reqScope, baseKey); err != nil {
		return err
	}
	// ScopePublicImmutable callers can read ONLY artifacts explicitly published as ScopePublicImmutable!
	if reqScope == ScopePublicImmutable && meta.scope != ScopePublicImmutable {
		return ErrTenantUnauthorized
	}
	return nil
}

func (e *Engine) lookupSharedMeta(t *tenantState, reqScope Scope, baseKey string) (sharedKeyMeta, error) {
	e.mu.RLock()
	authErr := e.authorizeLocked(t, reqScope, baseKey)
	meta, ok := e.sharedMeta[baseKey]
	if authErr == nil && ok {
		authErr = e.authorizeSharedKeyLocked(t, reqScope, baseKey, meta)
	}
	e.mu.RUnlock()
	if authErr != nil {
		e.unauthAttempts.Add(1)
		return sharedKeyMeta{}, authErr
	}
	if !ok {
		e.integrityErrs.Add(1)
		e.unauthAttempts.Add(1)
		return sharedKeyMeta{}, fmt.Errorf("%w: %s", ErrUnregisteredArtifact, baseKey)
	}
	return meta, nil
}

func (e *Engine) effectiveQuotaRAM(t *tenantState) int64 {
	if t.quotaRAM < 0 {
		return e.cfg.TotalRAMBytes
	}
	return t.quotaRAM
}

func (e *Engine) effectiveQuotaNVMe(t *tenantState) int64 {
	if t.quotaNVMe < 0 {
		return e.cfg.TotalNVMeBytes
	}
	return t.quotaNVMe
}

// Get reads key within scope on behalf of tenantID, serving from RAM -> NVMe ->
// underlying backend according to the Engine's Policy and verifying tenant AAD
// or manifest-registered BLAKE3 integrity fail-closed.
func (e *Engine) Get(ctx context.Context, tenantID string, scope Scope, key string, max int64) ([]byte, error) {
	t, err := e.getTenant(tenantID)
	if err != nil {
		return nil, err
	}
	var beKey string
	if scope == ScopePrivate {
		beKey = e.privateEngineBackendKey(t.tvHash, key)
	} else if scope.isImmutableShared() {
		beKey = e.sharedBackendKey(key)
	} else {
		e.unauthAttempts.Add(1)
		return nil, ErrTenantUnauthorized
	}
	b, _, err := e.getWithKeys(ctx, t, scope, key, beKey, max)
	return b, err
}

func (e *Engine) getForBackend(ctx context.Context, tenantID string, scope Scope, logicalKey, beKey string, max int64) ([]byte, backend.Version, error) {
	t, err := e.getTenant(tenantID)
	if err != nil {
		return nil, "", err
	}
	if scope == ScopePrivate && logicalKey == "head" {
		t.lifecycleMu.RLock()
		defer t.lifecycleMu.RUnlock()
		e.mu.RLock()
		authErr := e.authorizeLocked(t, scope, logicalKey)
		e.mu.RUnlock()
		if authErr != nil {
			e.unauthAttempts.Add(1)
			return nil, "", authErr
		}
		start := time.Now()
		e.totalReads.Add(1)
		t.reads.Add(1)
		e.backendGets.Add(1)
		t.backendGets.Add(1)
		b, ver, err := e.be.Get(ctx, beKey, max)
		dur := time.Since(start)
		e.latencies.record(dur)
		t.latencies.record(dur)
		return b, ver, err
	}
	return e.getWithKeys(ctx, t, scope, logicalKey, beKey, max)
}

func (e *Engine) getWithKeys(ctx context.Context, t *tenantState, scope Scope, logicalKey, beKey string, max int64) ([]byte, backend.Version, error) {
	t.lifecycleMu.RLock()
	defer t.lifecycleMu.RUnlock()
	return e.getWithKeysRLocked(ctx, t, scope, logicalKey, beKey, max)
}

func (e *Engine) getWithKeysRLocked(ctx context.Context, t *tenantState, scope Scope, logicalKey, beKey string, max int64) ([]byte, backend.Version, error) {
	var sharedMeta sharedKeyMeta
	var ciph Cipher
	var volID string

	if scope == ScopePrivate {
		e.mu.RLock()
		authErr := e.authorizeLocked(t, scope, logicalKey)
		ciph = t.cipher
		volID = t.volID
		e.mu.RUnlock()
		if authErr != nil {
			e.unauthAttempts.Add(1)
			return nil, "", authErr
		}
	} else if scope.isImmutableShared() {
		var err error
		sharedMeta, err = e.lookupSharedMeta(t, scope, logicalKey)
		if err != nil {
			return nil, "", err
		}
	} else {
		e.unauthAttempts.Add(1)
		return nil, "", ErrTenantUnauthorized
	}

	start := time.Now()
	e.totalReads.Add(1)
	t.reads.Add(1)

	ck := cacheKey{scope: canonicalCacheScope(scope), key: logicalKey}
	if scope == ScopePrivate {
		ck.tenant = t.id
		ck.volID = volID
	}

	// Tier 1: RAM check.
	if b, ver, ok, err := e.tryRAMHit(t, scope, ck, sharedMeta, max); ok || err != nil {
		dur := time.Since(start)
		e.latencies.record(dur)
		t.latencies.record(dur)
		return b, ver, err
	}

	// Tier 2: NVMe check (under per-key shard lock so disk read & eviction don't race).
	b, ver, ok, err := e.tryNVMeRead(t, scope, ck, ciph, sharedMeta, max)
	if err != nil {
		dur := time.Since(start)
		e.latencies.record(dur)
		t.latencies.record(dur)
		return nil, "", err
	}
	if ok {
		dur := time.Since(start)
		e.latencies.record(dur)
		t.latencies.record(dur)
		return b, ver, nil
	}

	// Tier 3: Backend fetch (singleflight coalesced per isolated flight key).
	fk := ck.hotKey() + "\x00" + beKey
	plain, ver, err := e.flights.do(ctx, fk, func() ([]byte, backend.Version, error) {
		if rb, rver, rok, rerr := e.tryRAMHit(t, scope, ck, sharedMeta, backend.Unbounded); rok || rerr != nil {
			return rb, rver, rerr
		}
		if nb, nver, nok, nerr := e.tryNVMeRead(t, scope, ck, ciph, sharedMeta, backend.Unbounded); nok || nerr != nil {
			return nb, nver, nerr
		}

		e.backendGets.Add(1)
		t.backendGets.Add(1)
		fetchMax := max
		if scope == ScopePrivate && max != backend.Unbounded {
			fetchMax = max + 4096
		}
		raw, ver, err := e.be.Get(ctx, beKey, fetchMax)
		if err != nil {
			return nil, "", err
		}

		var plain []byte
		var digest [blake3.Size]byte
		var diskFrame []byte
		var artID string
		if scope == ScopePrivate {
			plain, err = unsealPrivate(ciph, t.id, volID, logicalKey, raw)
			if err != nil {
				e.integrityErrs.Add(1)
				return nil, "", err
			}
			diskFrame = raw
		} else {
			if int64(len(raw)) != sharedMeta.size {
				e.integrityErrs.Add(1)
				return nil, "", fmt.Errorf("%w: shared key %s size %d != manifest size %d", ErrIntegrityViolation, logicalKey, len(raw), sharedMeta.size)
			}
			digest = blake3.Sum(raw)
			if subtle.ConstantTimeCompare(digest[:], sharedMeta.digest[:]) != 1 {
				e.integrityErrs.Add(1)
				return nil, "", fmt.Errorf("%w: shared key %s digest mismatch against manifest %s", ErrIntegrityViolation, logicalKey, sharedMeta.artifactID)
			}
			artID = sharedMeta.artifactID
			plain = raw
			diskFrame, err = sealSharedFrame(e.hostCipher, artID, logicalKey, digest, plain)
			if err != nil {
				e.integrityErrs.Add(1)
				return nil, "", err
			}
		}

		if err := e.admitOnMiss(t, scope, ck, artID, plain, diskFrame, digest, ver); err != nil {
			return nil, "", err
		}
		return plain, ver, nil
	})
	if err != nil {
		dur := time.Since(start)
		e.latencies.record(dur)
		t.latencies.record(dur)
		return nil, "", err
	}
	if max != backend.Unbounded && int64(len(plain)) > max {
		dur := time.Since(start)
		e.latencies.record(dur)
		t.latencies.record(dur)
		return nil, "", fmt.Errorf("%w: %s is %d bytes (max %d)", backend.ErrTooLarge, logicalKey, len(plain), max)
	}
	dur := time.Since(start)
	e.latencies.record(dur)
	t.latencies.record(dur)
	return plain, ver, nil
}

// GetRange reads n bytes starting at off from key within scope on behalf of tenantID.
// For immutable shared/public keys, range reads verify the full parent object against
// its manifest-registered BLAKE3 digest (zero unauthenticated sub-slice TOFU).
func (e *Engine) GetRange(ctx context.Context, tenantID string, scope Scope, key string, off, n int64) ([]byte, error) {
	t, err := e.getTenant(tenantID)
	if err != nil {
		return nil, err
	}
	var beKey string
	if scope == ScopePrivate {
		beKey = e.privateEngineBackendKey(t.tvHash, key)
	} else if scope.isImmutableShared() {
		beKey = e.sharedBackendKey(key)
	} else {
		e.unauthAttempts.Add(1)
		return nil, ErrTenantUnauthorized
	}
	return e.getRangeForBackend(ctx, tenantID, scope, key, beKey, off, n)
}

func (e *Engine) getRangeForBackend(ctx context.Context, tenantID string, scope Scope, logicalKey, beKey string, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 {
		return nil, fmt.Errorf("most: negative range %d+%d", off, n)
	}
	t, err := e.getTenant(tenantID)
	if err != nil {
		return nil, err
	}
	if scope == ScopePrivate {
		full, _, err := e.getWithKeys(ctx, t, scope, logicalKey, beKey, backend.Unbounded)
		if err != nil {
			return nil, err
		}
		if off+n > int64(len(full)) {
			return nil, fmt.Errorf("most: range %d+%d outside %s (%d bytes)", off, n, logicalKey, len(full))
		}
		return append([]byte(nil), full[off:off+n]...), nil
	}
	if !scope.isImmutableShared() {
		e.unauthAttempts.Add(1)
		return nil, ErrTenantUnauthorized
	}

	// Authenticate the parent key against its registered ArtifactManifest first (fail closed if unregistered!).
	t.lifecycleMu.RLock()
	defer t.lifecycleMu.RUnlock()

	sharedMeta, err := e.lookupSharedMeta(t, scope, logicalKey)
	if err != nil {
		return nil, err
	}
	if off+n > sharedMeta.size {
		return nil, fmt.Errorf("most: range %d+%d outside %s (%d bytes)", off, n, logicalKey, sharedMeta.size)
	}

	// Verify full immutable object through manifest digest and serve slice cleanly from verified cache.
	full, _, err := e.getWithKeysRLocked(ctx, t, scope, logicalKey, beKey, backend.Unbounded)
	if err != nil {
		return nil, err
	}
	if off+n > int64(len(full)) {
		return nil, fmt.Errorf("most: range %d+%d outside %s (%d bytes)", off, n, logicalKey, len(full))
	}
	return append([]byte(nil), full[off:off+n]...), nil
}

func (e *Engine) tryRAMHit(t *tenantState, reqScope Scope, ck cacheKey, sharedMeta sharedKeyMeta, max int64) ([]byte, backend.Version, bool, error) {
	now := e.cfg.Clock()
	e.mu.Lock()
	if err := e.authorizeLocked(t, reqScope, ck.key); err != nil {
		e.mu.Unlock()
		e.unauthAttempts.Add(1)
		return nil, "", false, err
	}
	if reqScope.isImmutableShared() {
		if err := e.authorizeSharedKeyLocked(t, reqScope, ck.key, sharedMeta); err != nil {
			e.mu.Unlock()
			e.unauthAttempts.Add(1)
			return nil, "", false, err
		}
	}
	re, ok := e.ramEntries[ck]
	if !ok {
		e.mu.Unlock()
		return nil, "", false, nil
	}
	if reqScope.isImmutableShared() && subtle.ConstantTimeCompare(re.digest[:], sharedMeta.digest[:]) != 1 {
		e.mu.Unlock()
		e.integrityErrs.Add(1)
		return nil, "", false, fmt.Errorf("%w: cached RAM shared digest mismatch for %s", ErrIntegrityViolation, ck.key)
	}
	// Tenants with quotaRAM == 0 do not read through RAM tier.
	qRAM := e.effectiveQuotaRAM(t)
	if qRAM == 0 || re.cost > qRAM {
		e.mu.Unlock()
		return nil, "", false, nil
	}
	if max != backend.Unbounded && re.cost > max {
		e.mu.Unlock()
		return nil, "", false, fmt.Errorf("%w: %s is %d bytes (max %d)", backend.ErrTooLarge, ck.key, re.cost, max)
	}
	if el, held := re.holders[t.id]; held {
		t.ramLRU.MoveToFront(el)
		e.hot.touch(ck.hotKey(), now)
		cp := append([]byte(nil), re.plain...)
		ver := re.ver
		e.mu.Unlock()
		e.ramHits.Add(1)
		t.ramHits.Add(1)
		return cp, ver, true, nil
	} else if ck.scope.isImmutableShared() {
		// Attach authorized tenant as a holder under its own quota (deduped physical RAM).
		demote := e.evictRAMForTenantLocked(t, re.cost)
		if t.ramUsed+re.cost <= qRAM {
			el := t.ramLRU.PushFront(ck)
			re.holders[t.id] = el
			t.ramUsed += re.cost
		}
		e.hot.touch(ck.hotKey(), now)
		cp := append([]byte(nil), re.plain...)
		ver := re.ver
		e.mu.Unlock()
		if len(demote) > 0 {
			e.executeDemotions(demote)
		}
		e.ramHits.Add(1)
		t.ramHits.Add(1)
		return cp, ver, true, nil
	} else {
		e.mu.Unlock()
		return nil, "", false, nil
	}
}

func (e *Engine) tryNVMeRead(t *tenantState, reqScope Scope, ck cacheKey, ciph Cipher, sharedMeta sharedKeyMeta, max int64) ([]byte, backend.Version, bool, error) {
	kl := e.keyShard(ck)
	kl.Lock()

	e.mu.RLock()
	if err := e.authorizeLocked(t, reqScope, ck.key); err != nil {
		e.mu.RUnlock()
		kl.Unlock()
		e.unauthAttempts.Add(1)
		return nil, "", false, err
	}
	if reqScope.isImmutableShared() {
		if err := e.authorizeSharedKeyLocked(t, reqScope, ck.key, sharedMeta); err != nil {
			e.mu.RUnlock()
			kl.Unlock()
			e.unauthAttempts.Add(1)
			return nil, "", false, err
		}
	}
	ne, ok := e.nvmeEntries[ck]
	if !ok {
		e.mu.RUnlock()
		kl.Unlock()
		return nil, "", false, nil
	}
	diskPath := filepath.Join(e.cfg.NVMeDir, ne.relPath)
	artID := ne.artifactID
	ver := ne.ver
	volID := t.volID
	e.mu.RUnlock()

	frame, err := os.ReadFile(diskPath)
	if err != nil {
		kl.Unlock()
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}

	var plain []byte
	var expectedDigest [blake3.Size]byte
	if ck.scope == ScopePrivate {
		plain, err = unsealPrivate(ciph, t.id, volID, ck.key, frame)
		if err != nil {
			kl.Unlock()
			e.integrityErrs.Add(1)
			return nil, "", false, err
		}
	} else {
		expectedDigest = sharedMeta.digest
		if artID == "" {
			artID = sharedMeta.artifactID
		}
		plain, err = unsealSharedFrame(e.hostCipher, artID, ck.key, expectedDigest, frame)
		if err != nil {
			kl.Unlock()
			e.integrityErrs.Add(1)
			return nil, "", false, err
		}
		if int64(len(plain)) != sharedMeta.size {
			kl.Unlock()
			clear(plain)
			e.integrityErrs.Add(1)
			return nil, "", false, fmt.Errorf("%w: shared NVMe block %s size %d != manifest size %d", ErrIntegrityViolation, ck.key, len(plain), sharedMeta.size)
		}
	}

	if max != backend.Unbounded && int64(len(plain)) > max {
		kl.Unlock()
		clear(plain)
		return nil, "", false, fmt.Errorf("%w: %s is %d bytes (max %d)", backend.ErrTooLarge, ck.key, len(plain), max)
	}

	now := e.cfg.Clock()
	var demote []demoteJob
	var removeNVMePath string
	var evPaths []string

	e.mu.Lock()
	if err := e.authorizeLocked(t, reqScope, ck.key); err != nil {
		e.mu.Unlock()
		kl.Unlock()
		clear(plain)
		e.unauthAttempts.Add(1)
		return nil, "", false, err
	}
	if reqScope.isImmutableShared() {
		if err := e.authorizeSharedKeyLocked(t, reqScope, ck.key, sharedMeta); err != nil {
			e.mu.Unlock()
			kl.Unlock()
			clear(plain)
			e.unauthAttempts.Add(1)
			return nil, "", false, err
		}
	}
	e.nvmeHits.Add(1)
	t.nvmeHits.Add(1)
	score := e.hot.touch(ck.hotKey(), now)

	// Re-check presence in nvmeEntries before mutating LRU/policy state.
	ne, stillNVMe := e.nvmeEntries[ck]
	if stillNVMe {
		if el, held := ne.holders[t.id]; held {
			t.nvmeLRU.MoveToFront(el)
		} else if ck.scope.isImmutableShared() {
			qNVMe := e.effectiveQuotaNVMe(t)
			if qNVMe != 0 && ne.cost <= qNVMe {
				evPaths = e.evictNVMeForTenantLocked(t, ne.cost)
				if t.nvmeUsed+ne.cost <= qNVMe {
					ne.holders[t.id] = t.nvmeLRU.PushFront(ck)
					t.nvmeUsed += ne.cost
				}
			}
		}
	}

	switch e.cfg.Policy {
	case PolicySelectiveMirror:
		// Selective mirroring: promote to RAM ONLY if score >= HotThreshold,
		// leaving the NVMe copy completely intact (zero demotion writes).
		if score >= e.cfg.HotThreshold {
			if e.admitRAMLocked(t, ck, artID, plain, expectedDigest, ver) {
				e.mirrorPromotes.Add(1)
				t.mirrorPromotes.Add(1)
			}
		}
	case PolicyTierMigration:
		// Exclusive tier migration: migrate NVMe -> RAM and delete NVMe copy.
		if e.effectiveQuotaRAM(t) > 0 && int64(len(plain)) <= e.effectiveQuotaRAM(t) {
			if stillNVMe {
				removeNVMePath = e.removeNVMeEntryLocked(ck)
			}
			demote, _ = e.admitRAMWithDemoteLocked(t, ck, artID, plain, expectedDigest, ver)
		}
	}
	e.mu.Unlock()

	if removeNVMePath != "" {
		_ = os.Remove(removeNVMePath)
	}
	for _, p := range evPaths {
		_ = os.Remove(p)
	}
	// Release ck's shard lock BEFORE executing demotions so demoting a victim
	// that hashes to the same shard can never self-deadlock.
	kl.Unlock()

	if len(demote) > 0 {
		e.executeDemotions(demote)
	}
	return plain, ver, true, nil
}

func (e *Engine) admitOnMiss(t *tenantState, reqScope Scope, ck cacheKey, artifactID string, plain, diskFrame []byte, digest [blake3.Size]byte, ver backend.Version) error {
	now := e.cfg.Clock()
	cost := int64(len(plain))

	e.mu.Lock()
	if err := e.authorizeLocked(t, reqScope, ck.key); err != nil {
		e.mu.Unlock()
		e.unauthAttempts.Add(1)
		return err
	}
	score := e.hot.touch(ck.hotKey(), now)

	switch e.cfg.Policy {
	case PolicySelectiveMirror:
		// One-hit cold scans go ONLY to NVMe (never pollute RAM).
		// Only admit to RAM immediately if score already >= HotThreshold (e.g. threshold <= 1).
		mirrorRAM := score >= e.cfg.HotThreshold
		if mirrorRAM {
			if e.admitRAMLocked(t, ck, artifactID, plain, digest, ver) {
				e.mirrorPromotes.Add(1)
				t.mirrorPromotes.Add(1)
			}
		}
		e.mu.Unlock()
		return e.writeNVMe(t, ck, artifactID, diskFrame, cost, digest, ver)

	case PolicyTierMigration:
		// Baseline exclusive tier migration: cold misses enter RAM immediately,
		// demoting evicted RAM items down to NVMe (write amplification).
		if e.effectiveQuotaRAM(t) > 0 && cost <= e.effectiveQuotaRAM(t) && (e.cfg.TotalRAMBytes == 0 || cost <= e.cfg.TotalRAMBytes) {
			demote, _ := e.admitRAMWithDemoteLocked(t, ck, artifactID, plain, digest, ver)
			e.mu.Unlock()
			if len(demote) > 0 {
				e.executeDemotions(demote)
			}
			return nil
		}
		e.mu.Unlock()
		return e.writeNVMe(t, ck, artifactID, diskFrame, cost, digest, ver)
	}
	e.mu.Unlock()
	return nil
}

type demoteJob struct {
	ck         cacheKey
	artifactID string
	plain      []byte
	cost       int64
	digest     [blake3.Size]byte
	ver        backend.Version
	tenant     *tenantState
}

func (e *Engine) admitRAMLocked(t *tenantState, ck cacheKey, artifactID string, plain []byte, digest [blake3.Size]byte, ver backend.Version) bool {
	_, promoted := e.admitRAMInternalLocked(t, ck, artifactID, plain, digest, ver, false)
	return promoted
}

func (e *Engine) admitRAMWithDemoteLocked(t *tenantState, ck cacheKey, artifactID string, plain []byte, digest [blake3.Size]byte, ver backend.Version) ([]demoteJob, bool) {
	return e.admitRAMInternalLocked(t, ck, artifactID, plain, digest, ver, true)
}

func (e *Engine) admitRAMInternalLocked(t *tenantState, ck cacheKey, artifactID string, plain []byte, digest [blake3.Size]byte, ver backend.Version, collectDemote bool) ([]demoteJob, bool) {
	cost := int64(len(plain))
	qRAM := e.effectiveQuotaRAM(t)
	if qRAM == 0 || cost > qRAM || (e.cfg.TotalRAMBytes > 0 && cost > e.cfg.TotalRAMBytes) {
		return nil, false
	}

	var demotes []demoteJob
	if re, exists := e.ramEntries[ck]; exists {
		if el, held := re.holders[t.id]; held {
			t.ramLRU.MoveToFront(el)
			return nil, false
		}
		demotes = append(demotes, e.evictRAMForTenantInternalLocked(t, cost, collectDemote)...)
		if t.ramUsed+cost > qRAM {
			return demotes, false
		}
		re.holders[t.id] = t.ramLRU.PushFront(ck)
		t.ramUsed += cost
		return demotes, true
	}

	// Enforce per-tenant RAM quota strictly by evicting ONLY from t.
	demotes = append(demotes, e.evictRAMForTenantInternalLocked(t, cost, collectDemote)...)
	if t.ramUsed+cost > qRAM {
		return demotes, false
	}

	// Enforce host global RAM cap if quotas are overcommitted.
	if e.cfg.TotalRAMBytes > 0 {
		for e.globalRAM+cost > e.cfg.TotalRAMBytes {
			victimTenant := e.pickGlobalRAMVictimLocked(t)
			if victimTenant == nil {
				break
			}
			demotes = append(demotes, e.evictOneRAMLocked(victimTenant, collectDemote)...)
		}
		if e.globalRAM+cost > e.cfg.TotalRAMBytes {
			return demotes, false
		}
	}

	re := &ramEntry{
		id:         ck,
		artifactID: artifactID,
		plain:      append([]byte(nil), plain...),
		cost:       cost,
		ver:        ver,
		digest:     digest,
		holders:    map[string]*list.Element{},
	}
	re.holders[t.id] = t.ramLRU.PushFront(ck)
	e.ramEntries[ck] = re
	t.ramUsed += cost
	e.globalRAM += cost
	return demotes, true
}

func (e *Engine) evictRAMForTenantLocked(t *tenantState, needed int64) []demoteJob {
	return e.evictRAMForTenantInternalLocked(t, needed, e.cfg.Policy == PolicyTierMigration)
}

func (e *Engine) evictRAMForTenantInternalLocked(t *tenantState, needed int64, collectDemote bool) []demoteJob {
	qRAM := e.effectiveQuotaRAM(t)
	var demotes []demoteJob
	for t.ramUsed+needed > qRAM && t.ramLRU.Len() > 0 {
		demotes = append(demotes, e.evictOneRAMLocked(t, collectDemote)...)
	}
	return demotes
}

func (e *Engine) pickGlobalRAMVictimLocked(requestor *tenantState) *tenantState {
	// Prefer evicting from the requesting tenant first; only touch another
	// tenant if host TotalRAMBytes is overcommitted and the requestor holds 0 bytes.
	if requestor.ramLRU.Len() > 0 {
		return requestor
	}
	var sumQuotas int64
	for _, ts := range e.tenants {
		if !ts.revoked {
			q := e.effectiveQuotaRAM(ts)
			if q > 0 {
				sumQuotas += q
			}
		}
	}
	var best *tenantState
	maxExcess := float64(-1)
	for _, ts := range e.tenants {
		if ts.revoked || ts.ramLRU.Len() == 0 {
			continue
		}
		q := e.effectiveQuotaRAM(ts)
		fairShare := q
		if sumQuotas > e.cfg.TotalRAMBytes && sumQuotas > 0 {
			fairShare = int64(float64(q) * float64(e.cfg.TotalRAMBytes) / float64(sumQuotas))
		}
		excess := float64(ts.ramUsed - fairShare)
		if excess > maxExcess {
			maxExcess = excess
			best = ts
		}
	}
	if best == nil {
		for _, ts := range e.tenants {
			if !ts.revoked && ts.ramLRU.Len() > 0 && ts.ramUsed > 0 {
				return ts
			}
		}
	}
	return best
}

func (e *Engine) evictOneRAMLocked(t *tenantState, collectDemote bool) []demoteJob {
	back := t.ramLRU.Back()
	if back == nil {
		return nil
	}
	ck := back.Value.(cacheKey)
	t.ramLRU.Remove(back)
	re, ok := e.ramEntries[ck]
	if !ok {
		return nil
	}
	delete(re.holders, t.id)
	t.ramUsed -= re.cost
	e.ramEvictions.Add(1)
	if len(re.holders) > 0 {
		return nil
	}
	delete(e.ramEntries, ck)
	e.globalRAM -= re.cost
	if collectDemote {
		// Transfer ownership of re.plain to the demotion job; executeDemotions clears it.
		owner := t
		if ck.scope == ScopePrivate {
			if ot, exists := e.tenants[ck.tenant]; exists {
				owner = ot
			}
		}
		job := demoteJob{
			ck:         ck,
			artifactID: re.artifactID,
			plain:      re.plain,
			cost:       re.cost,
			digest:     re.digest,
			ver:        re.ver,
			tenant:     owner,
		}
		re.plain = nil
		return []demoteJob{job}
	}
	clear(re.plain)
	re.plain = nil
	return nil
}

func (e *Engine) executeDemotions(demotes []demoteJob) {
	for _, j := range demotes {
		if j.plain == nil {
			continue
		}
		var frame []byte
		var err error
		if j.ck.scope == ScopePrivate {
			if j.tenant != nil {
				e.mu.RLock()
				rev := j.tenant.revoked
				ciph := j.tenant.cipher
				e.mu.RUnlock()
				if !rev && ciph != nil {
					frame, err = sealPrivate(ciph, j.ck.tenant, j.ck.volID, j.ck.key, j.plain)
				}
			}
		} else {
			sum := blake3.Sum(j.plain)
			j.digest = sum
			frame, err = sealSharedFrame(e.hostCipher, j.artifactID, j.ck.key, sum, j.plain)
		}
		clear(j.plain)
		if err != nil || frame == nil || j.tenant == nil {
			continue
		}
		if werr := e.writeNVMe(j.tenant, j.ck, j.artifactID, frame, j.cost, j.digest, j.ver); werr == nil {
			e.migrationWrite.Add(1)
			j.tenant.migrationWrite.Add(1)
		}
	}
}

func (e *Engine) writeNVMe(t *tenantState, ck cacheKey, artifactID string, diskFrame []byte, logicalCost int64, digest [blake3.Size]byte, ver backend.Version) error {
	qNVMe := e.effectiveQuotaNVMe(t)
	if qNVMe == 0 || logicalCost > qNVMe || (e.cfg.TotalNVMeBytes > 0 && logicalCost > e.cfg.TotalNVMeBytes) {
		return nil
	}

	kl := e.keyShard(ck)
	kl.Lock()
	defer kl.Unlock()

	e.mu.RLock()
	if e.closed || t.revoked {
		e.mu.RUnlock()
		return ErrTenantUnauthorized
	}
	e.mu.RUnlock()

	rel := ck.diskRelPath()
	fullPath := filepath.Join(e.cfg.NVMeDir, rel)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Join(e.cfg.NVMeDir, ".tmp"), "n-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(diskFrame); err != nil {
		_ = tmp.Close()
		return err
	}
	if e.cfg.SyncNVMe {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, fullPath); err != nil {
		return err
	}
	cleanupTmp = false

	var toRemove []string
	e.mu.Lock()
	if e.closed || t.revoked {
		e.mu.Unlock()
		_ = os.Remove(fullPath)
		return ErrTenantUnauthorized
	}
	if existing, ok := e.nvmeEntries[ck]; ok {
		existing.artifactID = artifactID
		existing.digest = digest
		existing.ver = ver
		if el, held := existing.holders[t.id]; held {
			t.nvmeLRU.MoveToFront(el)
			if existing.cost != logicalCost {
				delta := logicalCost - existing.cost
				existing.cost = logicalCost
				t.nvmeUsed += delta
				e.globalNVMe += delta
			}
		} else {
			toRemove = append(toRemove, e.evictNVMeForTenantLocked(t, logicalCost)...)
			if t.nvmeUsed+logicalCost <= qNVMe {
				existing.holders[t.id] = t.nvmeLRU.PushFront(ck)
				t.nvmeUsed += logicalCost
			}
		}
		e.mu.Unlock()
		for _, p := range toRemove {
			_ = os.Remove(p)
		}
		e.nvmeWrites.Add(1)
		t.nvmeWrites.Add(1)
		return nil
	}

	toRemove = append(toRemove, e.evictNVMeForTenantLocked(t, logicalCost)...)
	if t.nvmeUsed+logicalCost > qNVMe {
		e.mu.Unlock()
		_ = os.Remove(fullPath)
		for _, p := range toRemove {
			_ = os.Remove(p)
		}
		return nil
	}
	if e.cfg.TotalNVMeBytes > 0 {
		for e.globalNVMe+logicalCost > e.cfg.TotalNVMeBytes {
			victim := e.pickGlobalNVMeVictimLocked(t)
			if victim == nil {
				break
			}
			if p := e.evictOneNVMeLocked(victim); p != "" {
				toRemove = append(toRemove, p)
			}
		}
		if e.globalNVMe+logicalCost > e.cfg.TotalNVMeBytes {
			e.mu.Unlock()
			_ = os.Remove(fullPath)
			for _, p := range toRemove {
				_ = os.Remove(p)
			}
			return nil
		}
	}

	ne := &nvmeEntry{
		id:         ck,
		artifactID: artifactID,
		relPath:    rel,
		cost:       logicalCost,
		ver:        ver,
		digest:     digest,
		holders:    map[string]*list.Element{},
		isShared:   ck.scope.isImmutableShared(),
	}
	ne.holders[t.id] = t.nvmeLRU.PushFront(ck)
	e.nvmeEntries[ck] = ne
	t.nvmeUsed += logicalCost
	e.globalNVMe += logicalCost
	e.mu.Unlock()

	for _, p := range toRemove {
		_ = os.Remove(p)
	}
	e.nvmeWrites.Add(1)
	t.nvmeWrites.Add(1)
	return nil
}

func (e *Engine) evictNVMeForTenantLocked(t *tenantState, needed int64) []string {
	qNVMe := e.effectiveQuotaNVMe(t)
	var remove []string
	for t.nvmeUsed+needed > qNVMe && t.nvmeLRU.Len() > 0 {
		if p := e.evictOneNVMeLocked(t); p != "" {
			remove = append(remove, p)
		}
	}
	return remove
}

func (e *Engine) pickGlobalNVMeVictimLocked(requestor *tenantState) *tenantState {
	if requestor.nvmeLRU.Len() > 0 {
		return requestor
	}
	for _, ts := range e.tenants {
		if !ts.revoked && ts.nvmeLRU.Len() > 0 {
			return ts
		}
	}
	return nil
}

func (e *Engine) evictOneNVMeLocked(t *tenantState) string {
	back := t.nvmeLRU.Back()
	if back == nil {
		return ""
	}
	ck := back.Value.(cacheKey)
	t.nvmeLRU.Remove(back)
	ne, ok := e.nvmeEntries[ck]
	if !ok {
		return ""
	}
	delete(ne.holders, t.id)
	t.nvmeUsed -= ne.cost
	e.nvmeEvictions.Add(1)
	if len(ne.holders) > 0 {
		return ""
	}
	delete(e.nvmeEntries, ck)
	e.globalNVMe -= ne.cost
	return filepath.Join(e.cfg.NVMeDir, ne.relPath)
}

func (e *Engine) removeNVMeEntryLocked(ck cacheKey) string {
	ne, ok := e.nvmeEntries[ck]
	if !ok {
		return ""
	}
	for tid, el := range ne.holders {
		if t, tok := e.tenants[tid]; tok {
			t.nvmeLRU.Remove(el)
			t.nvmeUsed -= ne.cost
		}
	}
	delete(e.nvmeEntries, ck)
	e.globalNVMe -= ne.cost
	return filepath.Join(e.cfg.NVMeDir, ne.relPath)
}

func (e *Engine) invalidateLocked(ck cacheKey) string {
	if re, ok := e.ramEntries[ck]; ok {
		for tid, el := range re.holders {
			if t, tok := e.tenants[tid]; tok {
				t.ramLRU.Remove(el)
				t.ramUsed -= re.cost
			}
		}
		clear(re.plain)
		re.plain = nil
		delete(e.ramEntries, ck)
		e.globalRAM -= re.cost
	}
	return e.removeNVMeEntryLocked(ck)
}

// Put seals and writes private data for tenantID. Writing to ScopeSharedImmutable
// or ScopePublicImmutable fails closed with ErrReadOnly.
func (e *Engine) Put(ctx context.Context, tenantID string, scope Scope, key string, data []byte) error {
	if scope.isImmutableShared() {
		e.unauthAttempts.Add(1)
		return ErrReadOnly
	}
	t, err := e.getTenant(tenantID)
	if err != nil {
		return err
	}
	beKey := e.privateEngineBackendKey(t.tvHash, key)
	return e.putWithKeys(ctx, t, scope, key, beKey, data)
}

func (e *Engine) putForBackend(ctx context.Context, tenantID string, scope Scope, logicalKey, beKey string, data []byte) error {
	if scope.isImmutableShared() {
		e.unauthAttempts.Add(1)
		return ErrReadOnly
	}
	t, err := e.getTenant(tenantID)
	if err != nil {
		return err
	}
	if logicalKey == "head" {
		t.lifecycleMu.RLock()
		defer t.lifecycleMu.RUnlock()
		e.mu.RLock()
		authErr := e.authorizeLocked(t, scope, logicalKey)
		e.mu.RUnlock()
		if authErr != nil {
			e.unauthAttempts.Add(1)
			return authErr
		}
		return e.be.Put(ctx, beKey, data)
	}
	return e.putWithKeys(ctx, t, scope, logicalKey, beKey, data)
}

func (e *Engine) putWithKeys(ctx context.Context, t *tenantState, scope Scope, logicalKey, beKey string, data []byte) error {
	t.lifecycleMu.RLock()
	defer t.lifecycleMu.RUnlock()

	e.mu.RLock()
	authErr := e.authorizeLocked(t, scope, logicalKey)
	ciph := t.cipher
	volID := t.volID
	e.mu.RUnlock()
	if authErr != nil {
		e.unauthAttempts.Add(1)
		return authErr
	}

	sealed, err := sealPrivate(ciph, t.id, volID, logicalKey, data)
	if err != nil {
		return err
	}

	ck := cacheKey{scope: ScopePrivate, tenant: t.id, volID: volID, key: logicalKey}
	kl := e.keyShard(ck)
	kl.Lock()
	defer kl.Unlock()

	if err := e.be.Put(ctx, beKey, sealed); err != nil {
		return err
	}
	e.mu.Lock()
	remPath := e.invalidateLocked(ck)
	e.mu.Unlock()
	if remPath != "" {
		_ = os.Remove(remPath)
	}
	return nil
}

func (e *Engine) putIfAbsentForBackend(ctx context.Context, tenantID string, scope Scope, logicalKey, beKey string, data []byte) (backend.Version, error) {
	return e.putConditionalForBackend(ctx, tenantID, scope, logicalKey, beKey, data, "", true)
}

func (e *Engine) putIfVersionForBackend(ctx context.Context, tenantID string, scope Scope, logicalKey, beKey string, data []byte, v backend.Version) (backend.Version, error) {
	return e.putConditionalForBackend(ctx, tenantID, scope, logicalKey, beKey, data, v, false)
}

func (e *Engine) putConditionalForBackend(ctx context.Context, tenantID string, scope Scope, logicalKey, beKey string, data []byte, v backend.Version, absent bool) (backend.Version, error) {
	if scope.isImmutableShared() {
		e.unauthAttempts.Add(1)
		return "", ErrReadOnly
	}
	t, err := e.getTenant(tenantID)
	if err != nil {
		return "", err
	}
	t.lifecycleMu.RLock()
	defer t.lifecycleMu.RUnlock()

	e.mu.RLock()
	authErr := e.authorizeLocked(t, scope, logicalKey)
	ciph := t.cipher
	volID := t.volID
	e.mu.RUnlock()
	if authErr != nil {
		e.unauthAttempts.Add(1)
		return "", authErr
	}
	if logicalKey == "head" {
		if absent {
			return e.be.PutIfAbsent(ctx, beKey, data)
		}
		return e.be.PutIfVersion(ctx, beKey, data, v)
	}

	sealed, err := sealPrivate(ciph, t.id, volID, logicalKey, data)
	if err != nil {
		return "", err
	}

	ck := cacheKey{scope: ScopePrivate, tenant: t.id, volID: volID, key: logicalKey}
	kl := e.keyShard(ck)
	kl.Lock()
	defer kl.Unlock()

	var newVer backend.Version
	if absent {
		newVer, err = e.be.PutIfAbsent(ctx, beKey, sealed)
	} else {
		newVer, err = e.be.PutIfVersion(ctx, beKey, sealed, v)
	}
	// Always invalidate cached RAM/NVMe under the shard lock so neither winning
	// nor losing CAS callers can ever observe or leave behind a stale version.
	e.mu.Lock()
	remPath := e.invalidateLocked(ck)
	e.mu.Unlock()
	if remPath != "" {
		_ = os.Remove(remPath)
	}
	if err != nil {
		return "", err
	}
	return newVer, nil
}

// Delete removes a private object from cache and underlying backend. Deleting
// from ScopeSharedImmutable or ScopePublicImmutable fails closed with ErrReadOnly.
func (e *Engine) Delete(ctx context.Context, tenantID string, scope Scope, key string) error {
	if scope.isImmutableShared() {
		e.unauthAttempts.Add(1)
		return ErrReadOnly
	}
	t, err := e.getTenant(tenantID)
	if err != nil {
		return err
	}
	beKey := e.privateEngineBackendKey(t.tvHash, key)
	return e.deleteWithKeys(ctx, t, scope, key, beKey)
}

func (e *Engine) deleteForBackend(ctx context.Context, tenantID string, scope Scope, logicalKey, beKey string) error {
	if scope.isImmutableShared() {
		e.unauthAttempts.Add(1)
		return ErrReadOnly
	}
	t, err := e.getTenant(tenantID)
	if err != nil {
		return err
	}
	return e.deleteWithKeys(ctx, t, scope, logicalKey, beKey)
}

func (e *Engine) deleteWithKeys(ctx context.Context, t *tenantState, scope Scope, logicalKey, beKey string) error {
	t.lifecycleMu.RLock()
	defer t.lifecycleMu.RUnlock()

	e.mu.RLock()
	authErr := e.authorizeLocked(t, scope, logicalKey)
	volID := t.volID
	e.mu.RUnlock()
	if authErr != nil {
		e.unauthAttempts.Add(1)
		return authErr
	}

	if logicalKey != "head" {
		ck := cacheKey{scope: ScopePrivate, tenant: t.id, volID: volID, key: logicalKey}
		kl := e.keyShard(ck)
		kl.Lock()
		defer kl.Unlock()
		e.mu.Lock()
		remPath := e.invalidateLocked(ck)
		e.hot.forget(ck.hotKey())
		e.mu.Unlock()
		if remPath != "" {
			_ = os.Remove(remPath)
		}
	}
	return e.be.Delete(ctx, beKey)
}

// Stat returns the object byte length visible to tenantID in scope.
func (e *Engine) Stat(ctx context.Context, tenantID string, scope Scope, key string) (int64, error) {
	t, err := e.getTenant(tenantID)
	if err != nil {
		return 0, err
	}
	var beKey string
	if scope == ScopePrivate {
		beKey = e.privateEngineBackendKey(t.tvHash, key)
	} else if scope.isImmutableShared() {
		beKey = e.sharedBackendKey(key)
	} else {
		e.unauthAttempts.Add(1)
		return 0, ErrTenantUnauthorized
	}
	return e.statForBackend(ctx, tenantID, scope, key, beKey)
}

func (e *Engine) statForBackend(ctx context.Context, tenantID string, scope Scope, logicalKey, beKey string) (int64, error) {
	t, err := e.getTenant(tenantID)
	if err != nil {
		return 0, err
	}
	t.lifecycleMu.RLock()
	defer t.lifecycleMu.RUnlock()

	if scope.isImmutableShared() {
		meta, err := e.lookupSharedMeta(t, scope, logicalKey)
		if err != nil {
			return 0, err
		}
		return meta.size, nil
	}

	e.mu.RLock()
	authErr := e.authorizeLocked(t, scope, logicalKey)
	volID := t.volID
	if authErr == nil && logicalKey != "head" {
		ck := cacheKey{scope: ScopePrivate, tenant: t.id, volID: volID, key: logicalKey}
		if re, ok := e.ramEntries[ck]; ok {
			cost := re.cost
			e.mu.RUnlock()
			return cost, nil
		}
		if ne, ok := e.nvmeEntries[ck]; ok {
			cost := ne.cost
			e.mu.RUnlock()
			return cost, nil
		}
	}
	e.mu.RUnlock()
	if authErr != nil {
		e.unauthAttempts.Add(1)
		return 0, authErr
	}
	if logicalKey == "head" {
		return e.be.Stat(ctx, beKey)
	}
	b, _, err := e.getWithKeysRLocked(ctx, t, scope, logicalKey, beKey, backend.Unbounded)
	if err != nil {
		return 0, err
	}
	return int64(len(b)), nil
}

// HotnessScore returns the current decayed hotness score for key in scope.
func (e *Engine) HotnessScore(tenantID string, scope Scope, key string) float64 {
	now := e.cfg.Clock()
	e.mu.RLock()
	defer e.mu.RUnlock()
	ck := cacheKey{scope: canonicalCacheScope(scope), key: key}
	if scope == ScopePrivate {
		t := e.tenants[tenantID]
		if t == nil {
			return 0
		}
		ck.tenant = t.id
		ck.volID = t.volID
	}
	return e.hot.score(ck.hotKey(), now)
}

// ResidentRAMBytes returns the resident RAM bytes accounted to tenantID.
func (e *Engine) ResidentRAMBytes(tenantID string) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	t := e.tenants[tenantID]
	if t == nil || t.revoked {
		return 0
	}
	return t.ramUsed
}

// ResidentNVMeBytes returns the resident NVMe bytes accounted to tenantID.
func (e *Engine) ResidentNVMeBytes(tenantID string) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	t := e.tenants[tenantID]
	if t == nil || t.revoked {
		return 0
	}
	return t.nvmeUsed
}

// Stats returns Engine-wide telemetry and exact nanosecond read-latency percentiles.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	ramBytes := e.globalRAM
	nvmeBytes := e.globalNVMe
	e.mu.RUnlock()

	p50, p95, p99 := e.latencies.percentiles()
	return Stats{
		Policy:               e.cfg.Policy,
		TotalReads:           e.totalReads.Load(),
		BackendGets:          e.backendGets.Load(),
		RAMHits:              e.ramHits.Load(),
		NVMeHits:             e.nvmeHits.Load(),
		MirrorPromotions:     e.mirrorPromotes.Load(),
		TierMigrationWrites:  e.migrationWrite.Load(),
		NVMeWrites:           e.nvmeWrites.Load(),
		RAMEvictions:         e.ramEvictions.Load(),
		NVMeEvictions:        e.nvmeEvictions.Load(),
		ResidentRAMBytes:     ramBytes,
		ResidentNVMeBytes:    nvmeBytes,
		IntegrityViolations:  e.integrityErrs.Load(),
		UnauthorizedAttempts: e.unauthAttempts.Load(),
		P50:                  p50,
		P95:                  p95,
		P99:                  p99,
	}
}

// TenantStats returns per-tenant residency, counters, and read-latency percentiles.
func (e *Engine) TenantStats(tenantID string) TenantStats {
	e.mu.RLock()
	t := e.tenants[tenantID]
	if t == nil {
		e.mu.RUnlock()
		return TenantStats{TenantID: tenantID}
	}
	st := TenantStats{
		TenantID:            t.id,
		VolID:               t.volID,
		Registered:          true,
		Revoked:             t.revoked,
		SharedAllowed:       t.sharedAllowed,
		QuotaRAMBytes:       e.effectiveQuotaRAM(t),
		QuotaNVMeBytes:      e.effectiveQuotaNVMe(t),
		ResidentRAMBytes:    t.ramUsed,
		ResidentNVMeBytes:   t.nvmeUsed,
		TotalReads:          t.reads.Load(),
		BackendGets:         t.backendGets.Load(),
		RAMHits:             t.ramHits.Load(),
		NVMeHits:            t.nvmeHits.Load(),
		MirrorPromotions:    t.mirrorPromotes.Load(),
		TierMigrationWrites: t.migrationWrite.Load(),
		NVMeWrites:          t.nvmeWrites.Load(),
	}
	e.mu.RUnlock()
	st.P50, st.P95, st.P99 = t.latencies.percentiles()
	return st
}

// ResetStats clears global and per-tenant counters and latency distributions
// without dropping resident cache entries.
func (e *Engine) ResetStats() {
	e.totalReads.Store(0)
	e.backendGets.Store(0)
	e.ramHits.Store(0)
	e.nvmeHits.Store(0)
	e.mirrorPromotes.Store(0)
	e.migrationWrite.Store(0)
	e.nvmeWrites.Store(0)
	e.ramEvictions.Store(0)
	e.nvmeEvictions.Store(0)
	e.integrityErrs.Store(0)
	e.unauthAttempts.Store(0)
	e.latencies.reset()

	e.mu.RLock()
	for _, t := range e.tenants {
		t.reads.Store(0)
		t.backendGets.Store(0)
		t.ramHits.Store(0)
		t.nvmeHits.Store(0)
		t.mirrorPromotes.Store(0)
		t.migrationWrite.Store(0)
		t.nvmeWrites.Store(0)
		t.latencies.reset()
	}
	e.mu.RUnlock()
}

// SnapshotLatencies returns a copy of all recorded Engine read durations.
func (e *Engine) SnapshotLatencies() []time.Duration {
	raw := e.latencies.snapshot()
	out := make([]time.Duration, len(raw))
	for i, ns := range raw {
		out[i] = time.Duration(ns) * time.Nanosecond
	}
	return out
}

// Unused import guard for bytes in case future builds trim helpers.
var _ = bytes.Equal
