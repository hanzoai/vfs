// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package replica

// BackendStore adapts a vfs backend.Backend (the raw object store — file locally,
// s3/SeaweedFS in production) to the Replicator Store interface. This is the ready
// Store every service wires: `replica.NewReplicator(key, replica.NewBackendStore(be), db)`.
//
// A tiny content-version sidecar (`<key>.ver` holding the snapshot's SHA-256) lets
// a reader answer Version(key) cheaply — a small Get — instead of fetching the whole
// database just to decide whether it changed. Get itself recomputes the version from
// the fetched bytes (authoritative), so a missing/stale sidecar can never cause a
// wrong restore — at worst a redundant pull.

import (
	"context"
	"errors"

	"github.com/hanzoai/vfs/pkg/backend"
)

// BackendStore is a Store backed by a vfs backend.Backend.
type BackendStore struct {
	be backend.Backend
}

// NewBackendStore wraps be as a Replicator Store.
func NewBackendStore(be backend.Backend) *BackendStore { return &BackendStore{be: be} }

func verKey(key string) string { return key + ".ver" }

// Put writes the snapshot then its version sidecar. If the sidecar write fails the
// data is still durable; Version falls back to "" (unknown → the reader pulls), so
// a partial Put degrades to a redundant pull, never data loss.
func (s *BackendStore) Put(ctx context.Context, key string, data []byte) error {
	if err := s.be.Put(ctx, key, data); err != nil {
		return err
	}
	return s.be.Put(ctx, verKey(key), []byte(version(data)))
}

// Get returns the snapshot bytes and their authoritative content version (recomputed
// from the bytes, so it always matches what a writer stored).
func (s *BackendStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	data, err := s.be.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	return data, version(data), nil
}

// Version returns the cheap sidecar version, or "" when absent (a first-ever key or
// a legacy object with no sidecar — the reader then pulls once).
func (s *BackendStore) Version(ctx context.Context, key string) (string, error) {
	v, err := s.be.Get(ctx, verKey(key))
	if errors.Is(err, backend.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(v), nil
}
