// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package replica

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file" // register file:// opener
)

// TestBackendStoreEndToEnd is the full HA path a service runs: an OWNER writes a
// real SQLite DB and Pushes it through a vfs backend; a READER on a different disk
// Pulls it back and sees the committed rows. Proves the BackendStore adapter +
// SQLiteDB + Replicator compose over the real object-store backend.
func TestBackendStoreEndToEnd(t *testing.T) {
	ctx := context.Background()
	objDir := t.TempDir() // stands in for SeaweedFS
	be, err := backend.Open(ctx, "file://"+objDir)
	if err != nil {
		t.Fatalf("backend.Open: %v", err)
	}
	defer be.Close()
	store := NewBackendStore(be)
	key := DBPath("acme", "", "kv")

	// Owner: create + write + push.
	ownerDir := t.TempDir()
	owner, err := OpenSQLite(filepath.Join(ownerDir, "kv.db"))
	if err != nil {
		t.Fatalf("open owner: %v", err)
	}
	defer owner.Close()
	if _, err := owner.DB().ExecContext(ctx, `CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := owner.DB().ExecContext(ctx, `INSERT INTO kv VALUES('hello','world')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	or := NewReplicator(key, store, owner)
	if err := or.Push(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}

	// Version sidecar is queryable cheaply.
	if v, err := store.Version(ctx, key); err != nil || v == "" {
		t.Fatalf("version after push = %q, err=%v; want non-empty", v, err)
	}

	// Reader on a separate disk: pull + read.
	readerDir := t.TempDir()
	reader, err := OpenSQLite(filepath.Join(readerDir, "kv.db"))
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	rr := NewReplicator(key, store, reader)
	if changed, err := rr.Pull(ctx); err != nil || !changed {
		t.Fatalf("pull: changed=%v err=%v; want changed=true", changed, err)
	}
	var got string
	if err := reader.DB().QueryRowContext(ctx, `SELECT v FROM kv WHERE k='hello'`).Scan(&got); err != nil {
		t.Fatalf("read pulled: %v", err)
	}
	if got != "world" {
		t.Fatalf("pulled kv[hello] = %q, want world", got)
	}

	// Owner writes more + pushes; reader pulls the update.
	if _, err := owner.DB().ExecContext(ctx, `INSERT INTO kv VALUES('a','b')`); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	if err := or.Push(ctx); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	if changed, err := rr.Pull(ctx); err != nil || !changed {
		t.Fatalf("pull 2: changed=%v err=%v; want changed=true", changed, err)
	}
	var n int
	if err := reader.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM kv`).Scan(&n); err != nil {
		t.Fatalf("count after update: %v", err)
	}
	if n != 2 {
		t.Fatalf("reader row count = %d, want 2", n)
	}
}
