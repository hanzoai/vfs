// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package visorfuse implements Visor's external OCI "fuse" mount transport over
// pre-authenticated host socket file descriptors (zero gofer hop, zero guest
// privilege) and an S3-backed checkpoint/restore store following the
// VFS-outside-checkpoint protocol (flush -> release round -> checkpoint ->
// cross-node restore) specified in hanzoai/vfs#31 (HIP-1342).
package visorfuse

import "errors"

var (
	// ErrInvalidMountConfig is returned when an OCI fuse mount specification
	// violates structural or security invariants.
	ErrInvalidMountConfig = errors.New("visorfuse: invalid OCI fuse mount config")

	// ErrGuestPrivilegeForbidden is returned when a mount spec requests guest
	// privileges (CAP_SYS_ADMIN, /dev/fuse in guest, or no_guest_priv=false).
	ErrGuestPrivilegeForbidden = errors.New("visorfuse: guest privileges are forbidden for external FUSE mount")

	// ErrGoferHopForbidden is returned when a mount spec enables or permits a
	// host-side gofer hop instead of direct host socket FD transport.
	ErrGoferHopForbidden = errors.New("visorfuse: host-side gofer hop is forbidden")

	// ErrUntrustedSocketFD is returned when the host socket FD is negative,
	// reserved (stdin/stdout/stderr), or absent from the host FD allowlist.
	ErrUntrustedSocketFD = errors.New("visorfuse: host socket FD is not allowlisted")

	// ErrTenantMismatch is returned when an operation or restore attempts to
	// access a volume or checkpoint belonging to another tenant.
	ErrTenantMismatch = errors.New("visorfuse: tenant isolation violation")

	// ErrVolumeMismatch is returned when an operation or restore references a
	// different volume ID than the mounted or checkpointed volume.
	ErrVolumeMismatch = errors.New("visorfuse: volume isolation violation")

	// ErrUnquiescedVolume is returned fail-closed when a sandbox checkpoint is
	// attempted before the external VFS volume has been flushed, quiesced, and
	// had its publication round released.
	ErrUnquiescedVolume = errors.New("visorfuse: external VFS volume is not quiesced and flushed")

	// ErrVolumeNotReleased is returned when a checkpoint is attempted while the
	// source node still holds the VFS publication round.
	ErrVolumeNotReleased = errors.New("visorfuse: VFS publication round has not been released")

	// ErrCheckpointTampered is returned fail-closed when a checkpoint manifest,
	// signature, page digest, or AEAD ciphertext fails cryptographic verification.
	ErrCheckpointTampered = errors.New("visorfuse: checkpoint integrity or signature verification failed")

	// ErrCheckpointNotFound is returned when the requested checkpoint manifest or
	// referenced page object does not exist in object storage.
	ErrCheckpointNotFound = errors.New("visorfuse: checkpoint not found")

	// ErrCheckpointExists is returned when committing a checkpoint manifest
	// collides with a distinct existing checkpoint at the same key.
	ErrCheckpointExists = errors.New("visorfuse: checkpoint already exists with different content")

	// ErrProtocolViolation is returned on malformed, oversized, or unexpected
	// FUSE wire frames on the host socket FD.
	ErrProtocolViolation = errors.New("visorfuse: FUSE socket protocol violation")

	// ErrSessionClosed is returned when a client or server operation is invoked
	// after the FUSE session or socket has closed.
	ErrSessionClosed = errors.New("visorfuse: FUSE session closed")

	// ErrUnsupportedOpcode is returned when an unknown FUSE opcode is received.
	ErrUnsupportedOpcode = errors.New("visorfuse: unsupported FUSE opcode")

	// ErrRestorePrivilegeDenied is returned when a cross-node restore requests
	// unauthorized privilege escalation (e.g. unprivileged -> rootful).
	ErrRestorePrivilegeDenied = errors.New("visorfuse: unauthorized restore privilege transition")
)
