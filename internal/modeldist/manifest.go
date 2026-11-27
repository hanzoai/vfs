// Package modeldist implements content-addressed model manifests, cryptographic
// provenance verification, multi-provider mirror distribution, and byte-verified
// resumable transfers on top of hanzoai/vfs backends.
//
// Design principles:
//   - No second disk format: chunks and manifests map directly onto standard
//     pkg/backend.Backend keys ("model/chunk/<2hex>/<blake3>" and
//     "model/manifest/<2hex>/<cid>"), so local file://, Hanzo S3, and any
//     S3-compatible object store work natively.
//   - No mandatory central origin: once replicated to independent HTTP or VFS
//     mirrors, clients reconstruct and verify models with zero origin calls.
//   - Fail-closed cryptographic verification: manifests are domain-separated
//     and signed with Ed25519; every chunk and full artifact is verified with
//     both BLAKE3 (via internal/blake3) and SHA-256 before acceptance.
package modeldist

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
)

const (
	// SchemaVersionV1 is the canonical schema identifier for ManifestV1.
	SchemaVersionV1 = "modeldist/v1"

	// SignatureAlgorithmV1 identifies domain-separated Ed25519 signatures over
	// canonical ManifestV1 unsigned bytes.
	SignatureAlgorithmV1 = "ed25519-blake3-v1"

	// DefaultChunkSize is the default content-addressed chunk size (4 MiB).
	DefaultChunkSize int64 = 4 << 20

	// MaxManifestBytes bounds untrusted manifest payloads read from mirrors.
	MaxManifestBytes int64 = 16 << 20

	cidDomainPrefix        = "hanzo-vfs-modeldist-cid-v1\x00"
	sigDomainPrefix        = "hanzo-vfs-modeldist-manifest-v1\x00"
	refSegmentDomainPrefix = "hanzo-vfs-modeldist-ref-seg-v1\x00"
)

// AssetKind classifies artifacts inside a model distribution manifest.
type AssetKind string

const (
	AssetKindZenWeights  AssetKind = "zen-weights"
	AssetKindGGUF        AssetKind = "gguf-variant"
	AssetKindAdapter     AssetKind = "adapter"
	AssetKindTokenizer   AssetKind = "tokenizer"
	AssetKindExecProfile AssetKind = "execution-profile"
)

// Valid reports whether k is one of the recognized model asset kinds.
func (k AssetKind) Valid() bool {
	switch k {
	case AssetKindZenWeights, AssetKindGGUF, AssetKindAdapter, AssetKindTokenizer, AssetKindExecProfile:
		return true
	default:
		return false
	}
}

// Sentinel errors returned by manifest verification and client transfers.
var (
	ErrInvalidManifest     = errors.New("modeldist: invalid manifest")
	ErrManifestCIDMismatch = errors.New("modeldist: manifest CID mismatch")
	ErrSignatureInvalid    = errors.New("modeldist: signature verification failed")
	ErrInsufficientSigs    = errors.New("modeldist: insufficient trusted signatures")
	ErrLicenseRejected     = errors.New("modeldist: license rejected by policy")
	ErrVersionRejected     = errors.New("modeldist: version rejected by policy")
	ErrProvenanceInvalid   = errors.New("modeldist: provenance chain invalid")
	ErrChunkDigestMismatch = errors.New("modeldist: chunk digest mismatch")
	ErrChunkSizeMismatch   = errors.New("modeldist: chunk size mismatch")
	ErrRootDigestMismatch  = errors.New("modeldist: artifact root digest mismatch")
	ErrChunkNotFound       = errors.New("modeldist: chunk not found")
	ErrManifestNotFound    = errors.New("modeldist: manifest not found")
	ErrAllProvidersFailed  = errors.New("modeldist: all providers failed")
	ErrProviderOffline     = errors.New("modeldist: provider offline")
)

// ChunkRef addresses one immutable byte range of an artifact by dual digests.
type ChunkRef struct {
	Index  int    `json:"index"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	BLAKE3 string `json:"blake3"`
	SHA256 string `json:"sha256"`
}

// Artifact describes one verifiable file within a model distribution bundle.
type Artifact struct {
	Path       string     `json:"path"`
	Kind       AssetKind  `json:"kind"`
	Variant    string     `json:"variant,omitempty"`
	Size       int64      `json:"size"`
	ChunkSize  int64      `json:"chunk_size"`
	RootBLAKE3 string     `json:"root_blake3"`
	RootSHA256 string     `json:"root_sha256"`
	Chunks     []ChunkRef `json:"chunks"`
}

// Provenance links a manifest to its lineage, recipe, and dataset license metadata.
type Provenance struct {
	ParentManifestCID      string `json:"parent_manifest_cid,omitempty"`
	SourceCommit           string `json:"source_commit,omitempty"`
	TrainingDatasetLicense string `json:"training_dataset_license,omitempty"`
	QuantizationMethod     string `json:"quantization_method,omitempty"`
	CreatedAt              string `json:"created_at"`
}

// SignatureEnvelope binds an Ed25519 signature over the canonical unsigned manifest.
type SignatureEnvelope struct {
	Algorithm    string `json:"algorithm"`
	PublisherID  string `json:"publisher_id"`
	PublicKeyHex string `json:"public_key_hex"`
	SignatureHex string `json:"signature_hex"`
}

// ManifestV1 is a versioned, content-addressed, signed model manifest covering
// Zen weights, GGUF variants, adapters, tokenizer assets, and execution profiles.
type ManifestV1 struct {
	SchemaVersion    string              `json:"schema_version"`
	ModelID          string              `json:"model_id"`
	Version          string              `json:"version"`
	License          string              `json:"license"`
	LicenseURL       string              `json:"license_url,omitempty"`
	Description      string              `json:"description,omitempty"`
	ExecutionProfile map[string]string   `json:"execution_profile,omitempty"`
	Provenance       Provenance          `json:"provenance"`
	Artifacts        []Artifact          `json:"artifacts"`
	ManifestCID      string              `json:"manifest_cid"`
	Signatures       []SignatureEnvelope `json:"signatures"`
}

type unsignedManifestV1 struct {
	SchemaVersion    string            `json:"schema_version"`
	ModelID          string            `json:"model_id"`
	Version          string            `json:"version"`
	License          string            `json:"license"`
	LicenseURL       string            `json:"license_url,omitempty"`
	Description      string            `json:"description,omitempty"`
	ExecutionProfile map[string]string `json:"execution_profile,omitempty"`
	Provenance       Provenance        `json:"provenance"`
	Artifacts        []Artifact        `json:"artifacts"`
}

// CanonicalUnsignedBytes serializes the unsigned body deterministically.
// Go's encoding/json emits struct fields in declaration order and sorts map keys
// lexicographically with no insignificant whitespace.
func (m *ManifestV1) CanonicalUnsignedBytes() ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	u := unsignedManifestV1{
		SchemaVersion:    m.SchemaVersion,
		ModelID:          m.ModelID,
		Version:          m.Version,
		License:          m.License,
		LicenseURL:       m.LicenseURL,
		Description:      m.Description,
		ExecutionProfile: m.ExecutionProfile,
		Provenance:       m.Provenance,
		Artifacts:        m.Artifacts,
	}
	return json.Marshal(u)
}

// ComputeCID computes the domain-separated BLAKE3 content identifier of the
// manifest's unsigned canonical payload.
func (m *ManifestV1) ComputeCID() (string, error) {
	body, err := m.CanonicalUnsignedBytes()
	if err != nil {
		return "", err
	}
	h := blake3.New()
	_, _ = h.WriteString(cidDomainPrefix)
	_, _ = h.Write(body)
	var sum [blake3.Size]byte
	copy(sum[:], h.Sum(nil))
	return hex.EncodeToString(sum[:]), nil
}

// Sign computes and sets ManifestCID (if empty, or verifies it matches if set)
// and appends an Ed25519 signature envelope from priv.
func (m *ManifestV1) Sign(priv ed25519.PrivateKey, publisherID string) error {
	if len(priv) != ed25519.PrivateKeySize {
		return fmt.Errorf("%w: invalid ed25519 private key length %d", ErrSignatureInvalid, len(priv))
	}
	if strings.TrimSpace(publisherID) == "" {
		return fmt.Errorf("%w: publisher_id is required", ErrInvalidManifest)
	}
	if m.SchemaVersion == "" {
		m.SchemaVersion = SchemaVersionV1
	}
	cid, err := m.ComputeCID()
	if err != nil {
		return err
	}
	if m.ManifestCID != "" && m.ManifestCID != cid {
		return fmt.Errorf("%w: stored %s != computed %s", ErrManifestCIDMismatch, m.ManifestCID, cid)
	}
	m.ManifestCID = cid

	body, err := m.CanonicalUnsignedBytes()
	if err != nil {
		return err
	}
	msg := make([]byte, 0, len(sigDomainPrefix)+len(body))
	msg = append(msg, sigDomainPrefix...)
	msg = append(msg, body...)

	sig := ed25519.Sign(priv, msg)
	pub := priv.Public().(ed25519.PublicKey)
	m.Signatures = append(m.Signatures, SignatureEnvelope{
		Algorithm:    SignatureAlgorithmV1,
		PublisherID:  publisherID,
		PublicKeyHex: hex.EncodeToString(pub),
		SignatureHex: hex.EncodeToString(sig),
	})
	return nil
}

// MarshalJSONCanonical encodes the full signed manifest deterministically.
func (m *ManifestV1) MarshalJSONCanonical() ([]byte, error) {
	return json.Marshal(m)
}

// ParseManifest decodes a serialized ManifestV1 payload.
func ParseManifest(raw []byte) (*ManifestV1, error) {
	if int64(len(raw)) > MaxManifestBytes {
		return nil, fmt.Errorf("%w: manifest size %d exceeds max %d", ErrInvalidManifest, len(raw), MaxManifestBytes)
	}
	var m ManifestV1
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	return &m, nil
}

// TotalBytes returns the sum of all artifact byte sizes in the manifest.
func (m *ManifestV1) TotalBytes() int64 {
	var total int64
	for _, a := range m.Artifacts {
		total += a.Size
	}
	return total
}

// TotalChunks returns the total chunk count across all artifacts.
func (m *ManifestV1) TotalChunks() int {
	total := 0
	for _, a := range m.Artifacts {
		total += len(a.Chunks)
	}
	return total
}

// LicensePolicy constrains acceptable model and dataset licenses.
type LicensePolicy struct {
	RequireLicense        bool
	AllowedLicenses       []string // case-insensitive SPDX identifiers; empty means any non-denied license
	DeniedLicenses        []string // case-insensitive SPDX identifiers always rejected
	RequireDatasetLicense bool
}

// VerificationPolicy configures fail-closed manifest verification.
type VerificationPolicy struct {
	TrustedKeys        []ed25519.PublicKey
	MinValidSignatures int // defaults to 1 if <= 0
	License            LicensePolicy
	MinVersion         string // optional inclusive SemVer lower bound
	MaxVersion         string // optional inclusive SemVer upper bound
	ExactVersion       string // optional exact SemVer match
	ParentResolver     func(cid string) (*ManifestV1, error)
}

// Verify checks structural integrity, content-addressed CID, Ed25519 signatures,
// license compliance, semantic version constraints, and optional parent provenance.
func (m *ManifestV1) Verify(policy VerificationPolicy) error {
	return m.verifyWithVisited(policy, make(map[string]struct{}))
}

func (m *ManifestV1) verifyWithVisited(policy VerificationPolicy, visited map[string]struct{}) error {
	if err := m.validateStructure(); err != nil {
		return err
	}
	cid, err := m.ComputeCID()
	if err != nil {
		return err
	}
	if m.ManifestCID != cid {
		return fmt.Errorf("%w: manifest_cid %s != computed %s", ErrManifestCIDMismatch, m.ManifestCID, cid)
	}
	if _, seen := visited[cid]; seen {
		return fmt.Errorf("%w: cyclic provenance at %s", ErrProvenanceInvalid, cid)
	}
	visited[cid] = struct{}{}

	if err := m.verifySignatures(policy); err != nil {
		return err
	}
	if err := m.verifyLicense(policy.License); err != nil {
		return err
	}
	if err := m.verifyVersion(policy); err != nil {
		return err
	}
	if m.Provenance.ParentManifestCID != "" {
		parentCID := m.Provenance.ParentManifestCID
		if !isHexDigest64(parentCID) {
			return fmt.Errorf("%w: invalid parent_manifest_cid %q", ErrProvenanceInvalid, parentCID)
		}
		if parentCID == m.ManifestCID {
			return fmt.Errorf("%w: self-referential parent_manifest_cid", ErrProvenanceInvalid)
		}
		if _, seen := visited[parentCID]; seen {
			return fmt.Errorf("%w: cyclic provenance referencing %s", ErrProvenanceInvalid, parentCID)
		}
		if policy.ParentResolver != nil {
			parent, err := policy.ParentResolver(parentCID)
			if err != nil {
				return fmt.Errorf("%w: resolve parent %s: %v", ErrProvenanceInvalid, parentCID, err)
			}
			if parent == nil {
				return fmt.Errorf("%w: resolved nil parent %s", ErrProvenanceInvalid, parentCID)
			}
			// Verify the returned parent matches the requested CID before
			// recursing so an untrusted resolver returning the child (or any
			// wrong manifest) cannot trigger unbounded recursion.
			if parent.ManifestCID != parentCID {
				return fmt.Errorf("%w: parent CID mismatch (got %s, want %s)", ErrProvenanceInvalid, parent.ManifestCID, parentCID)
			}
			computedParentCID, err := parent.ComputeCID()
			if err != nil {
				return fmt.Errorf("%w: parent %s compute CID: %v", ErrProvenanceInvalid, parentCID, err)
			}
			if computedParentCID != parentCID {
				return fmt.Errorf("%w: parent %s computed CID mismatch (got %s)", ErrProvenanceInvalid, parentCID, computedParentCID)
			}
			parentPolicy := policy
			// Parent models may be older versions than derived fine-tunes/adapters.
			parentPolicy.MinVersion = ""
			parentPolicy.MaxVersion = ""
			parentPolicy.ExactVersion = ""
			if err := parent.verifyWithVisited(parentPolicy, visited); err != nil {
				return fmt.Errorf("%w: parent %s failed verification: %v", ErrProvenanceInvalid, parentCID, err)
			}
		}
	}
	return nil
}

func (m *ManifestV1) validateStructure() error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	if m.SchemaVersion != SchemaVersionV1 {
		return fmt.Errorf("%w: unsupported schema_version %q", ErrInvalidManifest, m.SchemaVersion)
	}
	if strings.TrimSpace(m.ModelID) == "" {
		return fmt.Errorf("%w: empty model_id", ErrInvalidManifest)
	}
	if _, err := ParseSemVer(m.Version); err != nil {
		return fmt.Errorf("%w: invalid version %q: %v", ErrInvalidManifest, m.Version, err)
	}
	if !isHexDigest64(m.ManifestCID) {
		return fmt.Errorf("%w: invalid manifest_cid %q", ErrInvalidManifest, m.ManifestCID)
	}
	if len(m.Artifacts) == 0 {
		return fmt.Errorf("%w: manifest must list at least one artifact", ErrInvalidManifest)
	}
	seenPaths := make(map[string]struct{}, len(m.Artifacts))
	for i, a := range m.Artifacts {
		if err := validateRelativePath(a.Path); err != nil {
			return fmt.Errorf("%w: artifact[%d]: %v", ErrInvalidManifest, i, err)
		}
		if _, dup := seenPaths[a.Path]; dup {
			return fmt.Errorf("%w: duplicate artifact path %q", ErrInvalidManifest, a.Path)
		}
		seenPaths[a.Path] = struct{}{}
		if !a.Kind.Valid() {
			return fmt.Errorf("%w: artifact %q has unknown kind %q", ErrInvalidManifest, a.Path, a.Kind)
		}
		if a.Size < 0 || a.ChunkSize <= 0 {
			return fmt.Errorf("%w: artifact %q has invalid size=%d chunk_size=%d", ErrInvalidManifest, a.Path, a.Size, a.ChunkSize)
		}
		if !isHexDigest64(a.RootBLAKE3) || !isHexDigest64(a.RootSHA256) {
			return fmt.Errorf("%w: artifact %q has invalid root digest", ErrInvalidManifest, a.Path)
		}
		var off int64
		for j, c := range a.Chunks {
			if c.Index != j {
				return fmt.Errorf("%w: artifact %q chunk[%d] has index %d", ErrInvalidManifest, a.Path, j, c.Index)
			}
			if c.Offset != off {
				return fmt.Errorf("%w: artifact %q chunk[%d] offset %d != expected %d", ErrInvalidManifest, a.Path, j, c.Offset, off)
			}
			if c.Size <= 0 || c.Size > a.ChunkSize {
				return fmt.Errorf("%w: artifact %q chunk[%d] invalid size %d", ErrInvalidManifest, a.Path, j, c.Size)
			}
			if !isHexDigest64(c.BLAKE3) || !isHexDigest64(c.SHA256) {
				return fmt.Errorf("%w: artifact %q chunk[%d] invalid digest hex", ErrInvalidManifest, a.Path, j)
			}
			off += c.Size
		}
		if off != a.Size {
			return fmt.Errorf("%w: artifact %q chunks sum to %d != size %d", ErrInvalidManifest, a.Path, off, a.Size)
		}
	}
	return nil
}

func (m *ManifestV1) verifySignatures(policy VerificationPolicy) error {
	minSigs := policy.MinValidSignatures
	if minSigs <= 0 {
		minSigs = 1
	}
	if len(policy.TrustedKeys) == 0 {
		return fmt.Errorf("%w: no trusted public keys configured", ErrSignatureInvalid)
	}
	trusted := make(map[string]ed25519.PublicKey, len(policy.TrustedKeys))
	for _, k := range policy.TrustedKeys {
		if len(k) == ed25519.PublicKeySize {
			trusted[hex.EncodeToString(k)] = k
		}
	}
	if len(trusted) == 0 {
		return fmt.Errorf("%w: no valid ed25519 trusted keys", ErrSignatureInvalid)
	}

	body, err := m.CanonicalUnsignedBytes()
	if err != nil {
		return err
	}
	msg := make([]byte, 0, len(sigDomainPrefix)+len(body))
	msg = append(msg, sigDomainPrefix...)
	msg = append(msg, body...)

	validSigners := make(map[string]struct{})
	for i, env := range m.Signatures {
		if env.Algorithm != SignatureAlgorithmV1 {
			return fmt.Errorf("%w: signature[%d] unsupported algorithm %q", ErrSignatureInvalid, i, env.Algorithm)
		}
		pubBytes, err := hex.DecodeString(env.PublicKeyHex)
		if err != nil || len(pubBytes) != ed25519.PublicKeySize {
			return fmt.Errorf("%w: signature[%d] invalid public_key_hex", ErrSignatureInvalid, i)
		}
		sigBytes, err := hex.DecodeString(env.SignatureHex)
		if err != nil || len(sigBytes) != ed25519.SignatureSize {
			return fmt.Errorf("%w: signature[%d] invalid signature_hex", ErrSignatureInvalid, i)
		}
		if !ed25519.Verify(ed25519.PublicKey(pubBytes), msg, sigBytes) {
			return fmt.Errorf("%w: signature[%d] by %q failed ed25519 verify", ErrSignatureInvalid, i, env.PublisherID)
		}
		normKey := strings.ToLower(env.PublicKeyHex)
		if _, ok := trusted[normKey]; ok {
			validSigners[normKey] = struct{}{}
		}
	}
	if len(validSigners) < minSigs {
		return fmt.Errorf("%w: got %d trusted valid signatures, need %d", ErrInsufficientSigs, len(validSigners), minSigs)
	}
	return nil
}

func (m *ManifestV1) verifyLicense(policy LicensePolicy) error {
	lic := strings.TrimSpace(m.License)
	if policy.RequireLicense && lic == "" {
		return fmt.Errorf("%w: manifest license is empty", ErrLicenseRejected)
	}
	if policy.RequireDatasetLicense && strings.TrimSpace(m.Provenance.TrainingDatasetLicense) == "" {
		return fmt.Errorf("%w: training_dataset_license is required", ErrLicenseRejected)
	}
	for _, denied := range policy.DeniedLicenses {
		if strings.EqualFold(lic, strings.TrimSpace(denied)) {
			return fmt.Errorf("%w: license %q is explicitly denied", ErrLicenseRejected, lic)
		}
	}
	if len(policy.AllowedLicenses) > 0 {
		allowed := false
		for _, a := range policy.AllowedLicenses {
			if strings.EqualFold(lic, strings.TrimSpace(a)) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("%w: license %q not in allowlist %v", ErrLicenseRejected, lic, policy.AllowedLicenses)
		}
	}
	return nil
}

func (m *ManifestV1) verifyVersion(policy VerificationPolicy) error {
	cur, err := ParseSemVer(m.Version)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVersionRejected, err)
	}
	if policy.ExactVersion != "" {
		want, err := ParseSemVer(policy.ExactVersion)
		if err != nil {
			return fmt.Errorf("%w: invalid exact version constraint %q", ErrVersionRejected, policy.ExactVersion)
		}
		if cur.Compare(want) != 0 {
			return fmt.Errorf("%w: got %s, require exact %s", ErrVersionRejected, m.Version, policy.ExactVersion)
		}
	}
	if policy.MinVersion != "" {
		minV, err := ParseSemVer(policy.MinVersion)
		if err != nil {
			return fmt.Errorf("%w: invalid min version constraint %q", ErrVersionRejected, policy.MinVersion)
		}
		if cur.Compare(minV) < 0 {
			return fmt.Errorf("%w: version %s < min %s", ErrVersionRejected, m.Version, policy.MinVersion)
		}
	}
	if policy.MaxVersion != "" {
		maxV, err := ParseSemVer(policy.MaxVersion)
		if err != nil {
			return fmt.Errorf("%w: invalid max version constraint %q", ErrVersionRejected, policy.MaxVersion)
		}
		if cur.Compare(maxV) > 0 {
			return fmt.Errorf("%w: version %s > max %s", ErrVersionRejected, m.Version, policy.MaxVersion)
		}
	}
	return nil
}

// SemVer is a parsed semantic version MAJOR.MINOR.PATCH[-prerelease][+build].
type SemVer struct {
	Major      int
	Minor      int
	Patch      int
	Prerelease string
	Build      string
}

// ParseSemVer parses a strict MAJOR.MINOR.PATCH[-prerelease][+build] version string.
func ParseSemVer(s string) (SemVer, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")
	if s == "" {
		return SemVer{}, errors.New("empty version")
	}
	rest := s
	build := ""
	if idx := strings.IndexByte(rest, '+'); idx >= 0 {
		build = rest[idx+1:]
		rest = rest[:idx]
		if build == "" {
			return SemVer{}, errors.New("empty build metadata")
		}
		for _, id := range strings.Split(build, ".") {
			if !isValidSemVerIdent(id) {
				return SemVer{}, fmt.Errorf("invalid build metadata identifier %q in %q", id, s)
			}
		}
	}
	core := rest
	pre := ""
	if idx := strings.IndexByte(rest, '-'); idx >= 0 {
		core = rest[:idx]
		pre = rest[idx+1:]
		if pre == "" {
			return SemVer{}, errors.New("empty prerelease")
		}
		for _, id := range strings.Split(pre, ".") {
			if !isValidSemVerIdent(id) {
				return SemVer{}, fmt.Errorf("invalid prerelease identifier %q in %q", id, s)
			}
			if len(id) > 1 && id[0] == '0' && isASCIIOnlyDigits(id) {
				return SemVer{}, fmt.Errorf("numeric prerelease identifier %q must not have leading zero in %q", id, s)
			}
		}
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return SemVer{}, fmt.Errorf("expected MAJOR.MINOR.PATCH, got %q", s)
	}
	maj, err := parseSemVerCoreNum(parts[0], "major")
	if err != nil {
		return SemVer{}, err
	}
	min, err := parseSemVerCoreNum(parts[1], "minor")
	if err != nil {
		return SemVer{}, err
	}
	pat, err := parseSemVerCoreNum(parts[2], "patch")
	if err != nil {
		return SemVer{}, err
	}
	return SemVer{Major: maj, Minor: min, Patch: pat, Prerelease: pre, Build: build}, nil
}

func parseSemVerCoreNum(raw, label string) (int, error) {
	if !isASCIIOnlyDigits(raw) {
		return 0, fmt.Errorf("invalid %s version %q", label, raw)
	}
	if len(raw) > 1 && raw[0] == '0' {
		return 0, fmt.Errorf("invalid %s version %q: leading zero", label, raw)
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid %s version %q", label, raw)
	}
	return n, nil
}

func isValidSemVerIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isDigit := c >= '0' && c <= '9'
		isUpper := c >= 'A' && c <= 'Z'
		isLower := c >= 'a' && c <= 'z'
		if !isDigit && !isUpper && !isLower && c != '-' {
			return false
		}
	}
	return true
}

// Compare returns -1 if v < o, 0 if equal, +1 if v > o.
// Per SemVer 2.0.0 section 10, build metadata does not affect precedence.
func (v SemVer) Compare(o SemVer) int {
	if v.Major != o.Major {
		return cmpInt(v.Major, o.Major)
	}
	if v.Minor != o.Minor {
		return cmpInt(v.Minor, o.Minor)
	}
	if v.Patch != o.Patch {
		return cmpInt(v.Patch, o.Patch)
	}
	if v.Prerelease == o.Prerelease {
		return 0
	}
	if v.Prerelease == "" {
		return 1 // release > prerelease
	}
	if o.Prerelease == "" {
		return -1
	}
	return comparePrerelease(v.Prerelease, o.Prerelease)
}

// comparePrerelease implements SemVer 2.0.0 section 11.4 prerelease precedence:
// dot-separated identifiers are compared left-to-right, purely numeric
// identifiers are compared numerically, numeric identifiers sort below
// non-numeric identifiers, and a shorter prefix sorts below a longer one.
func comparePrerelease(a, b string) int {
	ap := strings.Split(a, ".")
	bp := strings.Split(b, ".")
	n := len(ap)
	if len(bp) < n {
		n = len(bp)
	}
	for i := 0; i < n; i++ {
		if ap[i] == bp[i] {
			continue
		}
		aNum := isASCIIOnlyDigits(ap[i])
		bNum := isASCIIOnlyDigits(bp[i])
		switch {
		case aNum && bNum:
			if c := cmpNumericIdent(ap[i], bp[i]); c != 0 {
				return c
			}
		case aNum && !bNum:
			return -1
		case !aNum && bNum:
			return 1
		default:
			return strings.Compare(ap[i], bp[i])
		}
	}
	return cmpInt(len(ap), len(bp))
}

func isASCIIOnlyDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func cmpNumericIdent(a, b string) int {
	a = strings.TrimLeft(a, "0")
	b = strings.TrimLeft(b, "0")
	if len(a) != len(b) {
		return cmpInt(len(a), len(b))
	}
	return strings.Compare(a, b)
}

func cmpInt(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// ChunkKey maps a 64-hex BLAKE3 digest to a valid pkg/backend.Backend key
// ("model/chunk/<2hex>/<64hex>"), reusing VFS/S3 storage with no second format.
func ChunkKey(blake3Hex string) (string, error) {
	h := strings.ToLower(strings.TrimSpace(blake3Hex))
	if !isHexDigest64(h) {
		return "", fmt.Errorf("%w: invalid chunk digest %q", ErrInvalidManifest, blake3Hex)
	}
	key := "model/chunk/" + h[:2] + "/" + h
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// ManifestKey maps a 64-hex manifest CID to a valid pkg/backend.Backend key
// ("model/manifest/<2hex>/<64hex>").
func ManifestKey(cidHex string) (string, error) {
	h := strings.ToLower(strings.TrimSpace(cidHex))
	if !isHexDigest64(h) {
		return "", fmt.Errorf("%w: invalid manifest cid %q", ErrInvalidManifest, cidHex)
	}
	key := "model/manifest/" + h[:2] + "/" + h
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// RefKey maps a logical (modelID, version) pair to a valid pkg/backend.Backend
// key while preserving distinct releases (e.g. "1.0.0-rc.1" vs "1.0.0-rc-1")
// via a domain-separated BLAKE3 digest of each raw identifier.
func RefKey(modelID, version string) (string, error) {
	if strings.TrimSpace(modelID) == "" || strings.TrimSpace(version) == "" {
		return "", fmt.Errorf("%w: invalid ref (%q, %q)", ErrInvalidManifest, modelID, version)
	}
	key := "model/ref/" + encodeRefSegment(modelID) + "/" + encodeRefSegment(version)
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	return key, nil
}

func encodeRefSegment(raw string) string {
	h := blake3.New()
	_, _ = h.WriteString(refSegmentDomainPrefix)
	_, _ = h.WriteString(raw)
	digest := hex.EncodeToString(h.Sum(nil))

	slug := sanitizeKeySegment(raw)
	if len(slug) > 64 {
		slug = strings.TrimRight(slug[:64], "-")
	}
	if slug == "" {
		return digest
	}
	return slug + "-" + digest
}

// DigestChunk computes both BLAKE3 (internal/blake3) and SHA-256 hex digests for data.
func DigestChunk(data []byte) (blake3Hex, sha256Hex string) {
	b3 := blake3.Sum(data)
	s2 := sha256.Sum256(data)
	return hex.EncodeToString(b3[:]), hex.EncodeToString(s2[:])
}

// VerifyChunkBytes verifies exact byte length, BLAKE3 digest, and SHA-256 digest
// for a downloaded or resumed chunk.
func VerifyChunkBytes(ref ChunkRef, data []byte) error {
	if int64(len(data)) != ref.Size {
		return fmt.Errorf("%w: chunk %d got %d bytes, want %d", ErrChunkSizeMismatch, ref.Index, len(data), ref.Size)
	}
	b3, s2 := DigestChunk(data)
	if b3 != strings.ToLower(ref.BLAKE3) {
		return fmt.Errorf("%w: chunk %d blake3 got %s, want %s", ErrChunkDigestMismatch, ref.Index, b3, ref.BLAKE3)
	}
	if s2 != strings.ToLower(ref.SHA256) {
		return fmt.Errorf("%w: chunk %d sha256 got %s, want %s", ErrChunkDigestMismatch, ref.Index, s2, ref.SHA256)
	}
	return nil
}

func validateRelativePath(p string) error {
	if p == "" {
		return errors.New("empty path")
	}
	if strings.HasPrefix(p, "/") || strings.Contains(p, "\\") || strings.ContainsRune(p, 0) {
		return fmt.Errorf("unsafe path %q", p)
	}
	cleaned := path.Clean(p)
	if cleaned != p || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return fmt.Errorf("non-canonical or escaping path %q", p)
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("invalid path segment in %q", p)
		}
	}
	return nil
}

func isHexDigest64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func sanitizeKeySegment(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-':
			b.WriteByte(c)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 128 {
		out = out[:128]
	}
	return out
}
