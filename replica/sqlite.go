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

	_ "github.com/hanzoai/sqlite" // CGO-free sqlite driver ("sqlite")
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
// bytes. Concurrent with ongoing reads/writes.
func (s *SQLiteDB) Snapshot(ctx context.Context) ([]byte, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	return snapshotConn(ctx, db, filepath.Dir(s.path))
}

// SnapshotFile returns a consistent snapshot of the SQLite database at path
// WITHOUT holding a persistent handle — the adoption primitive for a service that
// manages the DB with its OWN library (xorm, GORM, …): open a transient read
// handle, VACUUM INTO a temp, return the bytes. In WAL mode the transient handle
// sees all committed data, so this is consistent even while the service writes.
func SnapshotFile(ctx context.Context, path string) ([]byte, error) {
	db, err := sql.Open("sqlite", path+sqlitePragmas)
	if err != nil {
		return nil, fmt.Errorf("replica: snapshot open %s: %w", path, err)
	}
	defer db.Close()
	return snapshotConn(ctx, db, filepath.Dir(path))
}

// snapshotConn is the shared VACUUM-INTO core (one and one way).
func snapshotConn(ctx context.Context, db *sql.DB, dir string) ([]byte, error) {
	tmp, err := os.CreateTemp(dir, ".snap-*.db")
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

// RestoreFile atomically writes snapshot bytes over the SQLite database at path —
// the handle-less adoption primitive. The caller MUST have no open handle to path
// (close the service's engine first; reopen after). Drops the -wal/-shm sidecars
// so a reopen can't replay stale WAL over the restored file. Used at hydrate-on-open,
// BEFORE the service opens its own engine.
func RestoreFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("replica: restore dir %s: %w", filepath.Dir(path), err)
	}
	tmpPath := path + ".restore"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("replica: write restore temp: %w", err)
	}
	for _, side := range []string{"-wal", "-shm"} {
		_ = os.Remove(path + side)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("replica: rename restore over %s: %w", path, err)
	}
	return nil
}

// Restore atomically replaces the local database with data and reopens the
// handle. Exclusive: it closes the live handle, renames the new file over the
// old, and reopens. On any failure the previous DB is left intact and the handle
// is reopened on the original file, so the service never ends up with no DB.
func (s *SQLiteDB) Restore(ctx context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close our handle, swap the file via the shared primitive, reopen.
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("replica: close for restore: %w", err)
	}
	if err := RestoreFile(s.path, data); err != nil {
		// Reopen the original so the service still has a DB.
		s.db, _ = sql.Open("sqlite", s.path+sqlitePragmas)
		return err
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
