// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package visorfuse

import (
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/zeebo/blake3"
)

const (
	// MountTypeFUSE is the OCI runtime mount type for Visor's external FUSE
	// server connected over a pre-authenticated host socket FD.
	MountTypeFUSE = "fuse"

	// DefaultMaxWrite is the default maximum single FUSE_WRITE payload (1 MiB).
	DefaultMaxWrite uint32 = 1 << 20

	// MaxAllowedWrite caps a single FUSE_WRITE payload to prevent unbounded
	// memory allocation per socket frame (4 MiB).
	MaxAllowedWrite uint32 = 4 << 20

	// DefaultMaxReadAhead is the default guest read-ahead window (1 MiB).
	DefaultMaxReadAhead uint32 = 1 << 20
)

// SocketKind identifies the Unix-domain socket type used for the host-handed
// FUSE transport FD.
type SocketKind string

const (
	// SocketKindSeqpacket uses AF_UNIX SOCK_SEQPACKET (message-boundary preserving
	// on Linux).
	SocketKindSeqpacket SocketKind = "seqpacket"

	// SocketKindStream uses AF_UNIX SOCK_STREAM with explicit length-prefixed
	// framing (portable across Linux and macOS/Darwin).
	SocketKindStream SocketKind = "stream"
)

// PrivilegeMode records whether the sandbox runs unprivileged (rootless user
// namespace) or rootful. In both modes, the external FUSE mount itself still
// requires zero guest privileges (no_guest_priv=true) because the host opens
// the FUSE transport and passes a pre-authenticated socket FD.
type PrivilegeMode string

const (
	PrivilegeUnprivileged PrivilegeMode = "unprivileged"
	PrivilegeRootful      PrivilegeMode = "rootful"
)

// MountIdentity uniquely binds a mount or checkpoint to one tenant's volume.
type MountIdentity struct {
	TenantID string `json:"tenant_id"`
	VolumeID string `json:"volume_id"`
}

// Prefix returns the canonical backend.ValidKey-safe object prefix for this
// tenant/volume pair.
func (id MountIdentity) Prefix() string {
	return "t/" + id.TenantID + "/v/" + id.VolumeID
}

// MountSpec defines the OCI "fuse" mount configuration for connecting a Visor
// sandbox directly to an external host-side FUSE server over a host socket FD.
type MountSpec struct {
	Type                   string     `json:"type"`
	Source                 string     `json:"source"`
	Destination            string     `json:"destination"`
	HostSocketFD           int        `json:"host_socket_fd"`
	AllowedHostFDs         []int      `json:"allowed_host_fds"`
	SocketKind             SocketKind `json:"socket_kind"`
	TenantID               string     `json:"tenant_id"`
	VolumeID               string     `json:"volume_id"`
	NoGuestPriv            bool       `json:"no_guest_priv"`
	NoGoferHop             bool       `json:"no_gofer_hop"`
	AllowDeviceFuseInGuest bool       `json:"allow_device_fuse_in_guest"`
	GuestCapabilities      []string   `json:"guest_capabilities,omitempty"`
	ReadOnly               bool       `json:"read_only,omitempty"`
	MaxWrite               uint32     `json:"max_write,omitempty"`
	MaxReadAhead           uint32     `json:"max_read_ahead,omitempty"`
	Options                []string   `json:"options,omitempty"`
}

// Identity returns the strict (TenantID, VolumeID) binding of the mount spec.
func (m MountSpec) Identity() MountIdentity {
	return MountIdentity{TenantID: m.TenantID, VolumeID: m.VolumeID}
}

// CanonicalDigest returns the deterministic BLAKE3-256 hex digest of the
// security-relevant fields of the OCI mount spec (excluding ephemeral host FD
// numbers which change across nodes during restore).
func (m MountSpec) CanonicalDigest() string {
	type canonical struct {
		Type        string     `json:"type"`
		Source      string     `json:"source"`
		Destination string     `json:"destination"`
		SocketKind  SocketKind `json:"socket_kind"`
		TenantID    string     `json:"tenant_id"`
		VolumeID    string     `json:"volume_id"`
		NoGuestPriv bool       `json:"no_guest_priv"`
		NoGoferHop  bool       `json:"no_gofer_hop"`
		ReadOnly    bool       `json:"read_only"`
		MaxWrite    uint32     `json:"max_write"`
	}
	c := canonical{
		Type:        m.Type,
		Source:      m.Source,
		Destination: m.Destination,
		SocketKind:  m.SocketKind,
		TenantID:    m.TenantID,
		VolumeID:    m.VolumeID,
		NoGuestPriv: m.NoGuestPriv,
		NoGoferHop:  m.NoGoferHop,
		ReadOnly:    m.ReadOnly,
		MaxWrite:    m.effectiveMaxWrite(),
	}
	b, _ := json.Marshal(c)
	sum := blake3.Sum256(b)
	return fmt.Sprintf("%x", sum)
}

func (m MountSpec) effectiveMaxWrite() uint32 {
	if m.MaxWrite == 0 {
		return DefaultMaxWrite
	}
	return m.MaxWrite
}

func (m MountSpec) effectiveMaxReadAhead() uint32 {
	if m.MaxReadAhead == 0 {
		return DefaultMaxReadAhead
	}
	return m.MaxReadAhead
}

var forbiddenGuestCaps = map[string]bool{
	"CAP_SYS_ADMIN":    true,
	"CAP_SYS_RAWIO":    true,
	"CAP_SYS_MODULE":   true,
	"CAP_MKNOD":        true,
	"CAP_DAC_OVERRIDE": true,
	"CAP_SYS_PTRACE":   true,
	"CAP_NET_ADMIN":    true,
}

var forbiddenMountOptions = map[string]bool{
	"suid":                 true,
	"dev":                  true,
	"privileged":           true,
	"gofer":                true,
	"gofer_hop":            true,
	"no_guest_priv=false":  true,
	"no_gofer_hop=false":   true,
	"cap_sys_admin":        true,
	"allow_guest_dev_fuse": true,
}

// ValidateIdentifier ensures tenant, volume, and checkpoint IDs conform to
// backend.ValidKey's strict lowercase ASCII grammar ([a-z0-9-], 1..63 bytes,
// no leading/trailing hyphens or slashes).
func ValidateIdentifier(kind, s string) error {
	if len(s) == 0 || len(s) > 63 {
		return fmt.Errorf("%w: %s %q must be 1..63 bytes", ErrInvalidMountConfig, kind, s)
	}
	if s[0] == '-' || s[len(s)-1] == '-' {
		return fmt.Errorf("%w: %s %q cannot start or end with '-'", ErrInvalidMountConfig, kind, s)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return fmt.Errorf("%w: %s %q contains invalid byte %#x", ErrInvalidMountConfig, kind, s, c)
		}
	}
	return nil
}

// ValidateMountSpec verifies all security, isolation, and structural invariants
// of an OCI external FUSE mount specification fail-closed.
func ValidateMountSpec(spec MountSpec) error {
	if spec.Type != MountTypeFUSE {
		return fmt.Errorf("%w: type %q, want %q", ErrInvalidMountConfig, spec.Type, MountTypeFUSE)
	}
	if !spec.NoGuestPriv {
		return fmt.Errorf("%w: no_guest_priv must be true", ErrGuestPrivilegeForbidden)
	}
	if spec.AllowDeviceFuseInGuest {
		return fmt.Errorf("%w: guest /dev/fuse access must be disabled", ErrGuestPrivilegeForbidden)
	}
	if !spec.NoGoferHop {
		return fmt.Errorf("%w: no_gofer_hop must be true", ErrGoferHopForbidden)
	}
	for _, capName := range spec.GuestCapabilities {
		norm := strings.ToUpper(strings.TrimSpace(capName))
		if forbiddenGuestCaps[norm] {
			return fmt.Errorf("%w: guest capability %s is forbidden", ErrGuestPrivilegeForbidden, norm)
		}
	}
	if spec.HostSocketFD < 3 {
		return fmt.Errorf("%w: fd %d must be >= 3", ErrUntrustedSocketFD, spec.HostSocketFD)
	}
	if len(spec.AllowedHostFDs) == 0 || !slices.Contains(spec.AllowedHostFDs, spec.HostSocketFD) {
		return fmt.Errorf("%w: fd %d is not in host allowlist %v", ErrUntrustedSocketFD, spec.HostSocketFD, spec.AllowedHostFDs)
	}
	switch spec.SocketKind {
	case SocketKindSeqpacket, SocketKindStream:
	default:
		return fmt.Errorf("%w: unsupported socket kind %q", ErrInvalidMountConfig, spec.SocketKind)
	}
	if err := ValidateIdentifier("tenant_id", spec.TenantID); err != nil {
		return err
	}
	if err := ValidateIdentifier("volume_id", spec.VolumeID); err != nil {
		return err
	}
	if spec.Source == "" {
		return fmt.Errorf("%w: source must be non-empty", ErrInvalidMountConfig)
	}
	if err := validateMountDestination(spec.Destination); err != nil {
		return err
	}
	if spec.MaxWrite > MaxAllowedWrite {
		return fmt.Errorf("%w: max_write %d exceeds %d", ErrInvalidMountConfig, spec.MaxWrite, MaxAllowedWrite)
	}
	for _, opt := range spec.Options {
		norm := strings.ToLower(strings.TrimSpace(opt))
		if forbiddenMountOptions[norm] {
			if strings.Contains(norm, "gofer") {
				return fmt.Errorf("%w: option %q", ErrGoferHopForbidden, opt)
			}
			return fmt.Errorf("%w: option %q", ErrGuestPrivilegeForbidden, opt)
		}
	}
	return nil
}

func validateMountDestination(dest string) error {
	if dest == "" || !strings.HasPrefix(dest, "/") {
		return fmt.Errorf("%w: destination %q must be an absolute path", ErrInvalidMountConfig, dest)
	}
	if strings.ContainsRune(dest, '\x00') {
		return fmt.Errorf("%w: destination contains NUL", ErrInvalidMountConfig)
	}
	for _, seg := range strings.Split(dest, "/") {
		if seg == ".." || seg == "." {
			return fmt.Errorf("%w: destination %q must not contain traversal segments", ErrInvalidMountConfig, dest)
		}
	}
	cleaned := path.Clean(dest)
	if cleaned == "/" || cleaned != dest {
		return fmt.Errorf("%w: destination %q must be a clean non-root mountpoint", ErrInvalidMountConfig, dest)
	}
	return nil
}

// ValidatePrivilegeMode verifies that mode is a recognized sandbox privilege mode.
func ValidatePrivilegeMode(mode PrivilegeMode) error {
	switch mode {
	case PrivilegeUnprivileged, PrivilegeRootful:
		return nil
	default:
		return fmt.Errorf("%w: unknown privilege mode %q", ErrInvalidMountConfig, mode)
	}
}
