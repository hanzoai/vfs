// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package replica

// SQLiteDB is the real per-org SQLite DB handle the Replicator snapshots and
// restores — the piece that makes HA SQLite work for EVERY Hanzo service (the
// cloud replicator only ever exercised an in-memory test double). CGO-free
// (modernc.org/sqlite), so it builds under CGO_ENABLED=0 like the rest of the
// stack.
//
// Snapshot is consistent: `VACUUM INTO` a temp file forces a WAL checkpoint and
// writes a single defragmented, transaction-consistent copy of the database at
// one point in time — no torn pages, no separate -wal/-shm to ship. Restore is
// atomic: the incoming bytes are written to a temp file next to the live DB, the
// live handle is closed, the temp file is renamed over it (atomic on the same
// filesystem), and the handle is reopened. A crash mid-restore leaves the old DB
// intact (the rename either fully happened or not at all).

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite" // CGO-free sqlite driver ("sqlite")
)

// sqlitePragmas is the durability profile every Hanzo per-org SQLite opens with
// (WAL, NORMAL sync, foreign keys, a generous busy timeout) — the same profile
// hanzoai/base + visor apply, so one on-disk convention everywhere.
const sqlitePragmas = "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)"

// SQLiteDB is a Replicator DB backed by an on-disk SQLite file. It owns the sole
// *sql.DB handle to that path; Restore swaps the file underneath it. Safe for
// concurrent Snapshot / query use; Restore is exclusive.
type SQLiteDB struct {
	path string
	mu   sync.RWMutex
	db   *sql.DB
}

// OpenSQLite opens (creating parent dirs + the file) the SQLite DB at path with
// the standard durability pragmas. The returned *SQLiteDB satisfies the
// Replicator DB interface and exposes DB() for the service's own queries.
func OpenSQLite(path string) (*SQLiteDB, error) {
	if path == "" {
		return nil, fmt.Errorf("replica: sqlite path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("replica: sqlite dir %s: %w", filepath.Dir(path), err)
	}
	db, err := sql.Open("sqlite", path+sqlitePragmas)
	if err != nil {
		return nil, fmt.Errorf("replica: sqlite open %s: %w", path, err)
	}
	// modernc opens lazily; force a connection so a bad path fails here, not later.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("replica: sqlite ping %s: %w", path, err)
	}
	return &SQLiteDB{path: path, db: db}, nil
}

// DB returns the live *sql.DB for the service's own reads/writes. It stays valid
// across Restore (Restore reopens under the same *SQLiteDB), so hold the
// *SQLiteDB, call DB() per query, and never cache the *sql.DB across a Restore.
func (s *SQLiteDB) DB() *sql.DB {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db
}

// Snapshot returns a consistent, WAL-checkpointed copy of the whole database as
// bytes, via `VACUUM INTO` a temp file. Concurrent with ongoing reads/writes.
func (s *SQLiteDB) Snapshot(ctx context.Context) ([]byte, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".snap-*.db")
	if err != nil {
		return nil, fmt.Errorf("replica: snapshot temp: %w", err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)
	// VACUUM INTO forces a checkpoint and writes one consistent copy. The temp
	// path is a literal (created by us), so this is not an injection surface.
	if _, err := db.ExecContext(ctx, "VACUUM INTO '"+tmpPath+"'"); err != nil {
		return nil, fmt.Errorf("replica: vacuum into: %w", err)
	}
	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("replica: read snapshot: %w", err)
	}
	return data, nil
}

// Restore atomically replaces the local database with data and reopens the
// handle. Exclusive: it closes the live handle, renames the new file over the
// old, and reopens. On any failure the previous DB is left intact and the handle
// is reopened on the original file, so the service never ends up with no DB.
func (s *SQLiteDB) Restore(ctx context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tmpPath := s.path + ".restore"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("replica: write restore temp: %w", err)
	}
	// Drop the WAL/SHM sidecars of the OLD db so a reopen can't replay stale WAL
	// over the freshly restored file.
	if err := s.db.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("replica: close for restore: %w", err)
	}
	for _, side := range []string{"-wal", "-shm"} {
		_ = os.Remove(s.path + side)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		// Rename failed — reopen the original so the service still has a DB.
		s.db, _ = sql.Open("sqlite", s.path+sqlitePragmas)
		return fmt.Errorf("replica: rename restore over %s: %w", s.path, err)
	}
	db, err := sql.Open("sqlite", s.path+sqlitePragmas)
	if err != nil {
		return fmt.Errorf("replica: reopen after restore: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("replica: ping after restore: %w", err)
	}
	s.db = db
	return nil
}

// Close closes the underlying handle.
func (s *SQLiteDB) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}
