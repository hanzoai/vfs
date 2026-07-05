// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package replica is the ONE shared HA-SQLite substrate every Hanzo service uses
// (HIP-0107): per-org SQLite on the local disk for speed, its durable copy in
// hanzoai/vfs (SeaweedFS-backed object store), a deterministic single-writer
// election (Rendezvous/HRW over IAM membership — no coordinator, no lock service,
// no Postgres, no Redis), and per-org at-rest encryption via KMS.
//
// It was proven inside hanzoai/cloud (internal/org) and is promoted here so EVERY
// service — visor, iam, commerce, base, and every Base adopter — imports the SAME
// code instead of re-inventing durability. The adoption contract at each service's
// per-org store seam:
//
//	hydrate-on-open : Pull() the latest snapshot before first use of an org DB.
//	single-writer   : IsOwner(org, self, members) gates writes; a non-owner serves
//	                  stale-tolerant reads (Pull-then-read) and forwards writes.
//	post-commit ship: the owner runs PushLoop (or Push after a write burst); the
//	                  durable copy in vfs means a lost pod loses no committed data.
package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"sync"
	"time"
)

// Store is the object-store surface the Replicator needs — satisfied natively by
// hanzoai/vfs (SeaweedFS-backed, in-process). NO minio, NO external S3 SDK.
// version is a content hash so readers skip redundant pulls.
type Store interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) (data []byte, version string, err error)
	Version(ctx context.Context, key string) (version string, err error)
}

// DB is the local per-org SQLite the owner writes and readers restore into.
// Snapshot returns a consistent (WAL-checkpointed) copy; Restore replaces the
// local bytes atomically. Satisfied by *SQLiteDB (sqlite.go).
type DB interface {
	Snapshot(ctx context.Context) ([]byte, error)
	Restore(ctx context.Context, data []byte) error
}

// Cipher seals/opens a snapshot with an org-bound key (envelope encryption via
// the KMS master). Optional — omit it for local dev (plaintext object).
type Cipher interface {
	Seal(org string, plaintext []byte) ([]byte, error)
	Open(org string, ciphertext []byte) ([]byte, error)
}

// DBPath is the canonical object-store location of one of an org's SQLite DBs
// (HIP-0302 layout). Every replica computes the same path for the same inputs.
//
//	org root: DBPath("acme", "",              "iam")  -> orgs/acme/iam.db
//	project:  DBPath("acme", "projects/site", "base") -> orgs/acme/projects/site/base.db
//	user:     DBPath("acme", "users/dave",    "kv")   -> orgs/acme/users/dave/kv.db
func DBPath(orgID, scope, service string) string {
	if scope == "" {
		return path.Join("orgs", orgID, service+".db")
	}
	return path.Join("orgs", orgID, scope, service+".db")
}

// Replicator binds ONE per-org SQLite (at a DBPath key) to its object-store slot.
//
//	owner replica  (IsOwner true):  Push() on a timer / after a write burst
//	reader replica (IsOwner false): Pull() before a stale-tolerant read
//
// It does NOT decide ownership — that is Owner()/IsOwner(). Push/Get/Put are
// whole-object (a WAL-checkpointed snapshot), the proven HIP-0107 mechanism.
type Replicator struct {
	key   string
	store Store
	db    DB

	cipher Cipher
	org    string // org id — the AAD / key-derivation input for the cipher

	mu      sync.Mutex
	lastVer string // last object version pushed or pulled — skip redundant pulls
}

// Option configures a Replicator.
type Option func(*Replicator)

// WithEncryption seals every pushed snapshot with the org's per-org key so the
// object is ciphertext; orgID is bound as AAD, so a blob cannot be replayed under
// another org. Omit it and the DB is stored in the clear (local dev only).
func WithEncryption(c Cipher, orgID string) Option {
	return func(r *Replicator) { r.cipher, r.org = c, orgID }
}

// NewReplicator binds the DB at key (from DBPath) to its store slot.
func NewReplicator(key string, store Store, db DB, opts ...Option) *Replicator {
	r := &Replicator{key: key, store: store, db: db}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Key is this replicator's object-store location.
func (r *Replicator) Key() string { return r.key }

// Push snapshots the local DB (WAL-checkpointed) and writes it to the object
// store. Called by the OWNER — the single writer. The durable copy lives in vfs,
// so a lost pod loses no committed data.
func (r *Replicator) Push(ctx context.Context) error {
	data, err := r.db.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("replica: snapshot %s: %w", r.key, err)
	}
	if r.cipher != nil {
		if data, err = r.cipher.Seal(r.org, data); err != nil {
			return fmt.Errorf("replica: seal %s: %w", r.key, err)
		}
	}
	if err := r.store.Put(ctx, r.key, data); err != nil {
		return fmt.Errorf("replica: put %s: %w", r.key, err)
	}
	r.mu.Lock()
	r.lastVer = version(data) // version over the STORED (sealed) bytes
	r.mu.Unlock()
	return nil
}

// Pull downloads the latest snapshot and restores it into the local DB — called
// by a READER before serving stale-tolerant reads, or by a NEW OWNER on handover
// to load the last committed state. A no-op when the remote is unchanged since
// our last push/pull.
func (r *Replicator) Pull(ctx context.Context) (changed bool, err error) {
	ver, err := r.store.Version(ctx, r.key)
	if err != nil {
		return false, fmt.Errorf("replica: version %s: %w", r.key, err)
	}
	r.mu.Lock()
	current := ver != "" && ver == r.lastVer
	r.mu.Unlock()
	if current {
		return false, nil
	}
	data, ver, err := r.store.Get(ctx, r.key)
	if err != nil {
		return false, fmt.Errorf("replica: get %s: %w", r.key, err)
	}
	if r.cipher != nil {
		if data, err = r.cipher.Open(r.org, data); err != nil {
			return false, fmt.Errorf("replica: open %s: %w", r.key, err)
		}
	}
	if err := r.db.Restore(ctx, data); err != nil {
		return false, fmt.Errorf("replica: restore %s: %w", r.key, err)
	}
	r.mu.Lock()
	r.lastVer = ver
	r.mu.Unlock()
	return true, nil
}

// PushLoop runs Push on an interval until ctx cancel — the owner's background
// WAL-shipper. On error it keeps the last good remote copy and retries next tick
// (fire-and-forget durability; never blocks writes).
func (r *Replicator) PushLoop(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = 10 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = r.Push(ctx)
		}
	}
}

// version is the content hash used to skip redundant pulls — SHA-256 of the
// snapshot bytes, matching hanzoai/vfs's content-addressable model.
func version(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
