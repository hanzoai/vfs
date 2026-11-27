// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package most implements Mirror-Optimized Storage Tiering (MOST, FAST '26)
// for hanzoai/vfs: selective hot-data mirroring between an encrypted disk-backed
// NVMe cache tier and a tenant-bounded host RAM pool over an authenticated object
// backend, alongside an ordinary exclusive cache-tier migration baseline.
package most

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/hanzoai/vfs/internal/blake3"
)

// Policy selects the caching and promotion behavior of an Engine.
type Policy uint8

const (
	// PolicySelectiveMirror implements MOST: shared immutable and cold blocks
	// stay in the NVMe tier by default, one-hit scans never pollute RAM, and
	// only keys whose hotness score reaches HotThreshold are mirrored into RAM
	// while keeping the NVMe copy intact (zero demotion write-back on eviction).
	PolicySelectiveMirror Policy = iota

	// PolicyTierMigration implements ordinary exclusive cache-tier migration:
	// cold misses enter RAM, RAM evictions demote/write back to NVMe, and NVMe
	// hits migrate to RAM by removing the NVMe copy.
	PolicyTierMigration
)

func (p Policy) String() string {
	switch p {
	case PolicySelectiveMirror:
		return "selective_mirror"
	case PolicyTierMigration:
		return "tier_migration"
	default:
		return fmt.Sprintf("policy(%d)", p)
	}
}

// Scope partitions objects between tenant-private encrypted volumes and
// manifest-authenticated read-only immutable toolchains.
type Scope uint8

const (
	// ScopePrivate designates tenant-owned objects sealed with the tenant's
	// Cipher and bound via AAD to [tenantID | volID | key]. Private plaintext
	// never crosses the canonical per-tenant Cipher boundary.
	ScopePrivate Scope = iota

	// ScopeSharedImmutable designates read-only immutable toolchain objects
	// verified against an independently authenticated ArtifactManifest (never TOFU),
	// encrypted at rest in the NVMe disk tier via HostCacheCipher, and shared
	// only among ACL-authorized tenants.
	ScopeSharedImmutable

	// ScopePublicImmutable designates explicitly public read-only toolchain
	// artifacts (e.g., standard compiler/runtime layers). Like ScopeSharedImmutable,
	// every key MUST be pre-registered via an independently authenticated
	// ArtifactManifest (never TOFU) and every NVMe disk frame is encrypted at
	// rest via HostCacheCipher with domain-separated AAD.
	ScopePublicImmutable
)

func (s Scope) String() string {
	switch s {
	case ScopePrivate:
		return "private"
	case ScopeSharedImmutable:
		return "shared_immutable"
	case ScopePublicImmutable:
		return "public_immutable"
	default:
		return fmt.Sprintf("scope(%d)", s)
	}
}

func (s Scope) isImmutableShared() bool {
	return s == ScopeSharedImmutable || s == ScopePublicImmutable
}

func canonicalCacheScope(s Scope) Scope {
	if s.isImmutableShared() {
		return ScopeSharedImmutable
	}
	return s
}

// Cipher matches vfs.Cipher without introducing a circular import on package vfs.
// Implementations must authenticate as well as encrypt and be concurrency-safe.
type Cipher interface {
	Seal(plaintext []byte) (sealed []byte, keyID string, err error)
	Open(sealed []byte, keyID string) (plaintext []byte, err error)
}

var (
	// ErrTenantUnauthorized is returned fail-closed when a caller is unknown,
	// revoked, lacks a registered Cipher for private operations, or fails a
	// shared-toolchain ACL check.
	ErrTenantUnauthorized = errors.New("most: tenant unauthorized")

	// ErrIntegrityViolation is returned fail-closed when authenticated AAD
	// verification, Cipher tag checking, or BLAKE3 content digest checking fails.
	ErrIntegrityViolation = errors.New("most: integrity violation")

	// ErrUnregisteredArtifact is returned fail-closed when reading a shared or
	// public immutable key whose (digest, size) was not previously registered
	// via an authenticated ArtifactManifest (zero Trust-On-First-Use).
	ErrUnregisteredArtifact = errors.New("most: unregistered shared artifact")

	// ErrImmutableConflict is returned fail-closed when attempting to overwrite
	// or replace an existing shared immutable object or manifest with a
	// conflicting payload or digest.
	ErrImmutableConflict = errors.New("most: immutable artifact conflict")

	// ErrBadCipher is returned when a Cipher is nil, leaves plaintext visible
	// in its output, fails round-trip Open, or accepts altered ciphertext.
	ErrBadCipher = errors.New("most: cipher does not authenticate and encrypt")

	// ErrReadOnly is returned when attempting to mutate an immutable shared tier.
	ErrReadOnly = errors.New("most: shared immutable tier is read-only")

	// ErrInvalidConfig is returned when Engine or tenant parameters are invalid.
	ErrInvalidConfig = errors.New("most: invalid configuration")
)

const (
	// UnlimitedQuota lets a tenant share up to the Engine's global tier limit.
	UnlimitedQuota int64 = -1

	defaultTotalRAMBytes  int64         = 64 << 20
	defaultTotalNVMeBytes int64         = 512 << 20
	defaultHotThreshold   float64       = 2.0
	defaultDecayHalfLife  time.Duration = 30 * time.Second
	defaultMaxHotEntries  int           = 262144

	privateFrameMagic  = "MOST\x01"
	sharedFrameMagic   = "MIMG\x02"
	manifestFrameMagic = "MMAN\x01"
	aadLen             = blake3.Size
)

// ManifestEntry records the authenticated BLAKE3 digest and exact byte length
// of one immutable object inside an ArtifactManifest.
type ManifestEntry struct {
	Digest [blake3.Size]byte
	Size   int64
}

// ArtifactManifest is an independently Cipher-authenticated immutable manifest
// binding an ArtifactID and Version to a closed map of (logicalKey -> Digest, Size).
type ArtifactManifest struct {
	ArtifactID string
	Scope      Scope
	Version    uint64
	Entries    map[string]ManifestEntry
}

type manifestWireEntry struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type manifestWire struct {
	ArtifactID string                       `json:"artifact_id"`
	Scope      string                       `json:"scope"`
	Version    uint64                       `json:"version"`
	Entries    map[string]manifestWireEntry `json:"entries"`
}

// Config configures a MOST Engine instance.
type Config struct {
	Policy               Policy
	NVMeDir              string
	TotalRAMBytes        int64
	TotalNVMeBytes       int64
	HotThreshold         float64
	DecayHalfLife        time.Duration
	WindowOps            uint64
	MaxHotEntries        int
	SharedPrefix         string
	SyncNVMe             bool
	Clock                func() time.Time
	HostCacheCipher      Cipher
	SharedManifestCipher Cipher
}

func (c Config) withDefaults() (Config, error) {
	if c.Policy != PolicySelectiveMirror && c.Policy != PolicyTierMigration {
		return c, fmt.Errorf("%w: unknown policy %d", ErrInvalidConfig, c.Policy)
	}
	if c.TotalRAMBytes == 0 {
		c.TotalRAMBytes = defaultTotalRAMBytes
	}
	if c.TotalRAMBytes < 0 {
		return c, fmt.Errorf("%w: TotalRAMBytes must be >= 0", ErrInvalidConfig)
	}
	if c.TotalNVMeBytes == 0 {
		c.TotalNVMeBytes = defaultTotalNVMeBytes
	}
	if c.TotalNVMeBytes < 0 {
		return c, fmt.Errorf("%w: TotalNVMeBytes must be >= 0", ErrInvalidConfig)
	}
	if c.HotThreshold <= 0 {
		c.HotThreshold = defaultHotThreshold
	}
	if c.DecayHalfLife == 0 {
		c.DecayHalfLife = defaultDecayHalfLife
	}
	if c.MaxHotEntries <= 0 {
		c.MaxHotEntries = defaultMaxHotEntries
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	return c, nil
}

// Stats captures Engine-wide cache, write-amplification, security, and
// exact nanosecond read-latency percentiles.
type Stats struct {
	Policy               Policy
	TotalReads           int64
	BackendGets          int64
	RAMHits              int64
	NVMeHits             int64
	MirrorPromotions     int64
	TierMigrationWrites  int64
	NVMeWrites           int64
	RAMEvictions         int64
	NVMeEvictions        int64
	ResidentRAMBytes     int64
	ResidentNVMeBytes    int64
	IntegrityViolations  int64
	UnauthorizedAttempts int64
	P50                  time.Duration
	P95                  time.Duration
	P99                  time.Duration
}

// TenantStats captures per-tenant residency, quota bounds, cache efficiency,
// and exact read-latency percentiles.
type TenantStats struct {
	TenantID            string
	VolID               string
	Registered          bool
	Revoked             bool
	SharedAllowed       bool
	QuotaRAMBytes       int64
	QuotaNVMeBytes      int64
	ResidentRAMBytes    int64
	ResidentNVMeBytes   int64
	TotalReads          int64
	BackendGets         int64
	RAMHits             int64
	NVMeHits            int64
	MirrorPromotions    int64
	TierMigrationWrites int64
	NVMeWrites          int64
	P50                 time.Duration
	P95                 time.Duration
	P99                 time.Duration
}

type latencyRecorder struct {
	mu      sync.Mutex
	samples []int64
}

func (l *latencyRecorder) record(d time.Duration) {
	ns := d.Nanoseconds()
	l.mu.Lock()
	l.samples = append(l.samples, ns)
	l.mu.Unlock()
}

func (l *latencyRecorder) reset() {
	l.mu.Lock()
	l.samples = l.samples[:0]
	l.mu.Unlock()
}

func (l *latencyRecorder) snapshot() []int64 {
	l.mu.Lock()
	cp := append([]int64(nil), l.samples...)
	l.mu.Unlock()
	return cp
}

func (l *latencyRecorder) percentiles() (p50, p95, p99 time.Duration) {
	return computePercentiles(l.snapshot())
}

func computePercentiles(samples []int64) (p50, p95, p99 time.Duration) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	s := slices.Clone(samples)
	slices.Sort(s)
	at := func(q float64) time.Duration {
		idx := min(len(s)-1, max(0, int(math.Ceil(q*float64(len(s))))-1))
		return time.Duration(s[idx]) * time.Nanosecond
	}
	return at(0.50), at(0.95), at(0.99)
}

type hotEntry struct {
	score    float64
	lastTime time.Time
	lastOp   uint64
	lastSeq  uint64
}

// hotTracker tracks sliding-window / exponentially decayed hit frequencies.
// Decay uses a short grace window (halfLife/64) so rapid consecutive hits
// accumulate cleanly (1st=1.0, 2nd=2.0 >= HotThreshold=2.0) while stale
// singletons decay smoothly over DecayHalfLife.
type hotTracker struct {
	halfLife  time.Duration
	grace     time.Duration
	windowOps uint64
	maxItems  int
	opSeq     uint64
	entries   map[string]hotEntry
}

func newHotTracker(halfLife time.Duration, windowOps uint64, maxItems int) *hotTracker {
	var grace time.Duration
	if halfLife > 0 {
		grace = halfLife / 64
	}
	return &hotTracker{
		halfLife:  halfLife,
		grace:     grace,
		windowOps: windowOps,
		maxItems:  maxItems,
		entries:   make(map[string]hotEntry),
	}
}

func (h *hotTracker) decayedScore(e hotEntry, now time.Time) float64 {
	score := e.score
	if score <= 0 {
		return 0
	}
	if h.halfLife > 0 {
		dt := now.Sub(e.lastTime)
		if dt > h.grace {
			score *= math.Pow(0.5, float64(dt)/float64(h.halfLife))
		}
	}
	if h.windowOps > 0 && h.opSeq > e.lastOp {
		dops := h.opSeq - e.lastOp
		if dops > h.windowOps/64 {
			score *= math.Pow(0.5, float64(dops)/float64(h.windowOps))
		}
	}
	return score
}

func (h *hotTracker) touch(key string, now time.Time) float64 {
	h.opSeq++
	cur, ok := h.entries[key]
	if !ok {
		if len(h.entries) >= h.maxItems {
			h.evictColdest(now)
		}
		h.entries[key] = hotEntry{score: 1.0, lastTime: now, lastOp: h.opSeq, lastSeq: h.opSeq}
		return 1.0
	}
	score := h.decayedScore(cur, now) + 1.0
	h.entries[key] = hotEntry{score: score, lastTime: now, lastOp: h.opSeq, lastSeq: h.opSeq}
	return score
}

func (h *hotTracker) score(key string, now time.Time) float64 {
	cur, ok := h.entries[key]
	if !ok {
		return 0
	}
	return h.decayedScore(cur, now)
}

func (h *hotTracker) forget(key string) {
	delete(h.entries, key)
}

func (h *hotTracker) evictColdest(now time.Time) {
	// Sample a small bounded batch rather than scanning the full map so O(1) cost holds.
	const sampleBudget = 32
	var victim string
	minScore := math.Inf(1)
	checked := 0
	for k, v := range h.entries {
		s := h.decayedScore(v, now)
		if s < minScore {
			minScore = s
			victim = k
		}
		checked++
		if checked >= sampleBudget {
			break
		}
	}
	if victim != "" {
		delete(h.entries, victim)
	}
}

// ephemeralHostCipher seals host NVMe disk frames with a per-Engine ephemeral
// XChaCha20-Poly1305 key generated from crypto/rand when Config.HostCacheCipher
// is omitted, ensuring zero plaintext ever touches NVMe disk.
type ephemeralHostCipher struct {
	keyID string
	key   [chacha20poly1305.KeySize]byte
}

func newEphemeralHostCipher() (*ephemeralHostCipher, error) {
	var key [chacha20poly1305.KeySize]byte
	if _, err := rand.Read(key[:]); err != nil {
		return nil, fmt.Errorf("most: generate ephemeral host cache key: %w", err)
	}
	var idBytes [8]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return nil, fmt.Errorf("most: generate ephemeral host key id: %w", err)
	}
	return &ephemeralHostCipher{
		keyID: "ephem-" + hex.EncodeToString(idBytes[:]),
		key:   key,
	}, nil
}

func (c *ephemeralHostCipher) wipe() {
	if c != nil {
		clear(c.key[:])
	}
}

func (c *ephemeralHostCipher) Seal(plain []byte) ([]byte, string, error) {
	aead, err := chacha20poly1305.NewX(c.key[:])
	if err != nil {
		return nil, "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, "", err
	}
	return aead.Seal(nonce, nonce, plain, []byte(c.keyID)), c.keyID, nil
}

func (c *ephemeralHostCipher) Open(sealed []byte, keyID string) ([]byte, error) {
	if keyID != c.keyID {
		return nil, fmt.Errorf("unknown host cache key %q", keyID)
	}
	aead, err := chacha20poly1305.NewX(c.key[:])
	if err != nil {
		return nil, err
	}
	ns := aead.NonceSize()
	if len(sealed) < ns {
		return nil, errors.New("short host cache ciphertext")
	}
	return aead.Open(nil, sealed[:ns], sealed[ns:], []byte(c.keyID))
}

// checkCipher mirrors vfs.check: seals a random 64-byte probe and refuses any
// Cipher that is nil, leaks plaintext in its output, fails to Open what it
// sealed, or opens altered ciphertext without error.
func checkCipher(c Cipher) error {
	if c == nil {
		return fmt.Errorf("%w: nil cipher", ErrBadCipher)
	}
	probe := make([]byte, 64)
	if _, err := rand.Read(probe); err != nil {
		return err
	}
	sealed, id, err := c.Seal(append([]byte(nil), probe...))
	if err != nil {
		return fmt.Errorf("%w: seal: %v", ErrBadCipher, err)
	}
	if len(id) == 0 || len(id) > 255 {
		return fmt.Errorf("%w: invalid keyID length %d", ErrBadCipher, len(id))
	}
	if bytes.Contains(sealed, probe) || bytes.Contains(sealed, probe[:16]) {
		return fmt.Errorf("%w: cipher output contains plaintext input", ErrBadCipher)
	}
	got, err := c.Open(append([]byte(nil), sealed...), id)
	if err != nil || !bytes.Equal(got, probe) {
		return fmt.Errorf("%w: cipher does not open what it sealed (%v)", ErrBadCipher, err)
	}
	altered := append([]byte(nil), sealed...)
	altered[len(altered)/2] ^= 1
	if _, err := c.Open(altered, id); err == nil {
		return fmt.Errorf("%w: cipher opens altered bytes", ErrBadCipher)
	}
	return nil
}

func bindContext(kind string, fields ...string) [aadLen]byte {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs/most context")
	writeField := func(s string) {
		var lenBuf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lenBuf[:], uint64(len(s)))
		_, _ = h.Write(lenBuf[:n])
		_, _ = h.WriteString(s)
	}
	writeField(kind)
	for _, f := range fields {
		writeField(f)
	}
	var out [aadLen]byte
	h.Sum(out[:0])
	return out
}

func privateAAD(tenantID, volID, key string) [aadLen]byte {
	return bindContext("private", tenantID, volID, key)
}

func sharedFrameAAD(artifactID, key string, expected [blake3.Size]byte) [aadLen]byte {
	return bindContext("most-shared-v2", artifactID, key, hex.EncodeToString(expected[:]))
}

func manifestAAD(artifactID string) [aadLen]byte {
	return bindContext("most-manifest-v1", artifactID)
}

func sealWithAAD(c Cipher, magic string, aad [aadLen]byte, plain []byte, nilErr error) ([]byte, error) {
	if c == nil {
		return nil, nilErr
	}
	buf := make([]byte, 0, aadLen+len(plain))
	buf = append(buf, aad[:]...)
	buf = append(buf, plain...)
	sealed, keyID, err := c.Seal(buf)
	clear(buf)
	if err != nil {
		return nil, fmt.Errorf("most: seal failed: %w", err)
	}
	if bytes.Contains(sealed, aad[:]) || (len(plain) >= 16 && bytes.Contains(sealed, plain[:16])) {
		return nil, fmt.Errorf("%w: cipher output leaks authenticated context or plaintext", ErrIntegrityViolation)
	}
	if len(keyID) == 0 || len(keyID) > 255 {
		return nil, fmt.Errorf("%w: invalid keyID length %d", ErrIntegrityViolation, len(keyID))
	}
	out := make([]byte, 0, len(magic)+1+len(keyID)+len(sealed))
	out = append(out, magic...)
	out = append(out, byte(len(keyID)))
	out = append(out, keyID...)
	out = append(out, sealed...)
	return out, nil
}

func unsealWithAAD(c Cipher, magic string, aad [aadLen]byte, frame []byte, nilErr error) ([]byte, error) {
	if c == nil {
		return nil, nilErr
	}
	hdr := len(magic) + 1
	if len(frame) < hdr || string(frame[:len(magic)]) != magic {
		return nil, fmt.Errorf("%w: invalid sealed frame header", ErrIntegrityViolation)
	}
	keyLen := int(frame[len(magic)])
	if keyLen == 0 || len(frame) < hdr+keyLen {
		return nil, fmt.Errorf("%w: truncated keyID in sealed frame", ErrIntegrityViolation)
	}
	keyID := string(frame[hdr : hdr+keyLen])
	ciphertext := frame[hdr+keyLen:]
	dec, err := c.Open(ciphertext, keyID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIntegrityViolation, err)
	}
	if len(dec) < aadLen {
		clear(dec)
		return nil, fmt.Errorf("%w: missing authenticated AAD header", ErrIntegrityViolation)
	}
	if subtle.ConstantTimeCompare(dec[:aadLen], aad[:]) != 1 {
		clear(dec)
		return nil, fmt.Errorf("%w: authenticated AAD context mismatch", ErrIntegrityViolation)
	}
	plain := append([]byte(nil), dec[aadLen:]...)
	clear(dec)
	return plain, nil
}

func sealPrivate(c Cipher, tenantID, volID, key string, plain []byte) ([]byte, error) {
	return sealWithAAD(c, privateFrameMagic, privateAAD(tenantID, volID, key), plain, ErrTenantUnauthorized)
}

func unsealPrivate(c Cipher, tenantID, volID, key string, frame []byte) ([]byte, error) {
	return unsealWithAAD(c, privateFrameMagic, privateAAD(tenantID, volID, key), frame, ErrTenantUnauthorized)
}

func sealSharedFrame(c Cipher, artifactID, key string, expected [blake3.Size]byte, plain []byte) ([]byte, error) {
	sum := blake3.Sum(plain)
	if expected == ([blake3.Size]byte{}) || subtle.ConstantTimeCompare(sum[:], expected[:]) != 1 {
		return nil, fmt.Errorf("%w: shared payload does not match expected manifest digest", ErrIntegrityViolation)
	}
	return sealWithAAD(c, sharedFrameMagic, sharedFrameAAD(artifactID, key, expected), plain, ErrBadCipher)
}

func unsealSharedFrame(c Cipher, artifactID, key string, expected [blake3.Size]byte, frame []byte) ([]byte, error) {
	if expected == ([blake3.Size]byte{}) {
		return nil, fmt.Errorf("%w: missing expected digest for shared key %s", ErrUnregisteredArtifact, key)
	}
	plain, err := unsealWithAAD(c, sharedFrameMagic, sharedFrameAAD(artifactID, key, expected), frame, ErrBadCipher)
	if err != nil {
		return nil, err
	}
	sum := blake3.Sum(plain)
	if subtle.ConstantTimeCompare(sum[:], expected[:]) != 1 {
		clear(plain)
		return nil, fmt.Errorf("%w: shared block digest mismatch after unseal", ErrIntegrityViolation)
	}
	return plain, nil
}

func sealManifest(c Cipher, m ArtifactManifest) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: nil manifest cipher", ErrBadCipher)
	}
	if m.ArtifactID == "" {
		return nil, fmt.Errorf("%w: empty artifactID in manifest", ErrInvalidConfig)
	}
	scope := m.Scope
	if scope == 0 {
		scope = ScopeSharedImmutable
	}
	if !scope.isImmutableShared() {
		return nil, fmt.Errorf("%w: invalid manifest scope %v", ErrInvalidConfig, scope)
	}
	w := manifestWire{
		ArtifactID: m.ArtifactID,
		Scope:      scope.String(),
		Version:    m.Version,
		Entries:    make(map[string]manifestWireEntry, len(m.Entries)),
	}
	for k, e := range m.Entries {
		if e.Size < 0 {
			return nil, fmt.Errorf("%w: negative size for manifest key %q", ErrInvalidConfig, k)
		}
		w.Entries[k] = manifestWireEntry{
			Digest: hex.EncodeToString(e.Digest[:]),
			Size:   e.Size,
		}
	}
	raw, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("most: marshal manifest: %w", err)
	}
	return sealWithAAD(c, manifestFrameMagic, manifestAAD(m.ArtifactID), raw, ErrBadCipher)
}

func unsealManifest(c Cipher, artifactID string, frame []byte) (ArtifactManifest, error) {
	if artifactID == "" {
		return ArtifactManifest{}, fmt.Errorf("%w: empty artifactID", ErrInvalidConfig)
	}
	raw, err := unsealWithAAD(c, manifestFrameMagic, manifestAAD(artifactID), frame, ErrBadCipher)
	if err != nil {
		return ArtifactManifest{}, err
	}
	var w manifestWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return ArtifactManifest{}, fmt.Errorf("%w: corrupt manifest payload: %v", ErrIntegrityViolation, err)
	}
	if w.ArtifactID != artifactID {
		return ArtifactManifest{}, fmt.Errorf("%w: manifest artifactID %q != %q", ErrIntegrityViolation, w.ArtifactID, artifactID)
	}
	var scope Scope
	switch w.Scope {
	case "", ScopeSharedImmutable.String():
		scope = ScopeSharedImmutable
	case ScopePublicImmutable.String():
		scope = ScopePublicImmutable
	default:
		return ArtifactManifest{}, fmt.Errorf("%w: unknown manifest scope %q", ErrIntegrityViolation, w.Scope)
	}
	out := ArtifactManifest{
		ArtifactID: w.ArtifactID,
		Scope:      scope,
		Version:    w.Version,
		Entries:    make(map[string]ManifestEntry, len(w.Entries)),
	}
	for k, we := range w.Entries {
		if we.Size < 0 {
			return ArtifactManifest{}, fmt.Errorf("%w: negative manifest entry size for %q", ErrIntegrityViolation, k)
		}
		db, err := hex.DecodeString(we.Digest)
		if err != nil || len(db) != blake3.Size {
			return ArtifactManifest{}, fmt.Errorf("%w: invalid manifest digest for %q", ErrIntegrityViolation, k)
		}
		var d [blake3.Size]byte
		copy(d[:], db)
		out.Entries[k] = ManifestEntry{Digest: d, Size: we.Size}
	}
	return out, nil
}

func hashHex(kind string, parts ...string) string {
	d := bindContext(kind, parts...)
	return hex.EncodeToString(d[:])
}
