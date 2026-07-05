// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package replica

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// memStore is an in-memory Store (a vfs stand-in) for the round-trip tests.
type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
	ver  map[string]string
}

func newMemStore() *memStore {
	return &memStore{data: map[string][]byte{}, ver: map[string]string{}}
}
func (s *memStore) Put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := make([]byte, len(data))
	copy(b, data)
	s.data[key] = b
	s.ver[key] = version(data)
	return nil
}
func (s *memStore) Get(_ context.Context, key string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[key], s.ver[key], nil
}
func (s *memStore) Version(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ver[key], nil
}

// TestSQLiteSnapshotRestoreRoundTrip is the core proof the cloud replicator never
// had: a REAL on-disk SQLite DB snapshots to bytes and restores byte-consistently
// into a fresh DB, preserving committed rows.
func TestSQLiteSnapshotRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Owner DB: write rows, snapshot.
	src, err := OpenSQLite(filepath.Join(dir, "orgs", "acme", "kv.db"))
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer src.Close()
	if _, err := src.DB().ExecContext(ctx, `CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if _, err := src.DB().ExecContext(ctx, `INSERT INTO kv(k,v) VALUES(?,?)`, kv[0], kv[1]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	snap, err := src.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("empty snapshot")
	}

	// Reader DB (a different pod): restore the snapshot, see exactly the rows.
	dst, err := OpenSQLite(filepath.Join(dir, "orgs", "acme-reader", "kv.db"))
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()
	if err := dst.Restore(ctx, snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	var got string
	if err := dst.DB().QueryRowContext(ctx, `SELECT v FROM kv WHERE k='b'`).Scan(&got); err != nil {
		t.Fatalf("read restored: %v", err)
	}
	if got != "2" {
		t.Fatalf("restored kv[b] = %q, want 2", got)
	}
	var n int
	if err := dst.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM kv`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Fatalf("restored row count = %d, want 3", n)
	}

	// The DB stays usable after Restore (handle reopened cleanly).
	if _, err := dst.DB().ExecContext(ctx, `INSERT INTO kv(k,v) VALUES('d','4')`); err != nil {
		t.Fatalf("write after restore: %v", err)
	}
}

// TestReplicatorPushPull proves the full owner→store→reader cycle over a real
// SQLite DB and an in-memory vfs stand-in, including the redundant-pull skip.
func TestReplicatorPushPull(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newMemStore()
	key := DBPath("acme", "", "kv") // orgs/acme/kv.db

	owner, err := OpenSQLite(filepath.Join(dir, "owner.db"))
	if err != nil {
		t.Fatalf("open owner: %v", err)
	}
	defer owner.Close()
	if _, err := owner.DB().ExecContext(ctx, `CREATE TABLE t(x INT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := owner.DB().ExecContext(ctx, `INSERT INTO t VALUES(42)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	or := NewReplicator(key, store, owner)
	if err := or.Push(ctx); err != nil {
		t.Fatalf("push: %v", err)
	}

	reader, err := OpenSQLite(filepath.Join(dir, "reader.db"))
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	rr := NewReplicator(key, store, reader)
	changed, err := rr.Pull(ctx)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if !changed {
		t.Fatal("first pull must report changed")
	}
	var x int
	if err := reader.DB().QueryRowContext(ctx, `SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatalf("read pulled: %v", err)
	}
	if x != 42 {
		t.Fatalf("pulled x = %d, want 42", x)
	}

	// A second pull with no new push is a no-op (content-version skip).
	if changed, err := rr.Pull(ctx); err != nil || changed {
		t.Fatalf("second pull: changed=%v err=%v, want changed=false", changed, err)
	}
}

// TestOwnerDeterministicAndExclusive proves the HRW election picks exactly one
// owner, order-independently, and every member agrees.
func TestOwnerDeterministicAndExclusive(t *testing.T) {
	members := []Member{{ID: "r3"}, {ID: "r1"}, {ID: "r2"}}
	reordered := []Member{{ID: "r1"}, {ID: "r2"}, {ID: "r3"}}

	o1, ok1 := Owner("acme", members)
	o2, ok2 := Owner("acme", reordered)
	if !ok1 || !ok2 || o1.ID != o2.ID {
		t.Fatalf("owner not order-independent: %q vs %q", o1.ID, o2.ID)
	}

	// Exactly one member is the owner across the set.
	owners := 0
	for _, m := range members {
		if IsOwner("acme", m.ID, members) {
			owners++
		}
	}
	if owners != 1 {
		t.Fatalf("IsOwner true for %d members, want exactly 1", owners)
	}

	// Empty membership fails closed (no owner).
	if _, ok := Owner("acme", nil); ok {
		t.Fatal("empty membership must have no owner")
	}

	// Distinct orgs spread across replicas (not all pinned to one).
	seen := map[string]bool{}
	for _, org := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		o, _ := Owner(org, members)
		seen[o.ID] = true
	}
	if len(seen) < 2 {
		t.Fatalf("HRW pinned all orgs to %d replica(s); want spread", len(seen))
	}
}
