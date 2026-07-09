// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package replica

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

// seedDB creates a SQLite DB at path with n rows (~a real per-org store's size).
func seedDB(b *testing.B, path string, rows int) *SQLiteDB {
	b.Helper()
	db, err := OpenSQLite(path)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	if _, err := db.DB().ExecContext(ctx, `CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		b.Fatalf("create: %v", err)
	}
	tx, _ := db.DB().Begin()
	for i := 0; i < rows; i++ {
		if _, err := tx.Exec(`INSERT INTO kv(k,v) VALUES(?,?)`, i, fmt.Sprintf("row-%d-payload-data-padding", i)); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
	_ = tx.Commit()
	return db
}

// BenchmarkSnapshot measures the WAL-checkpointed VACUUM-INTO snapshot (the backup
// hot path — runs every PushLoop tick per org).
func BenchmarkSnapshot(b *testing.B) {
	for _, rows := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			db := seedDB(b, filepath.Join(b.TempDir(), "s.db"), rows)
			defer db.Close()
			ctx := context.Background()
			var bytes int
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := db.Snapshot(ctx)
				if err != nil {
					b.Fatalf("snapshot: %v", err)
				}
				bytes = len(data)
			}
			b.StopTimer()
			b.ReportMetric(float64(bytes)/1024, "KiB/snap")
		})
	}
}

// BenchmarkPushPull measures the full replication round trip (snapshot → seal-skipped
// → object-store Put → Version → Get → restore) over the real file backend.
func BenchmarkPushPull(b *testing.B) {
	for _, rows := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			ctx := context.Background()
			objDir := b.TempDir()
			be, err := backend.Open(ctx, "file://"+objDir)
			if err != nil {
				b.Fatalf("backend: %v", err)
			}
			defer be.Close()
			store := NewBackendStore(be)
			key := DBPath("acme", "", "kv")

			owner := seedDB(b, filepath.Join(b.TempDir(), "o.db"), rows)
			defer owner.Close()
			or := NewReplicator(key, store, owner)
			reader := seedDB(b, filepath.Join(b.TempDir(), "r.db"), 0)
			defer reader.Close()
			rr := NewReplicator(key, store, reader)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := or.Push(ctx); err != nil {
					b.Fatalf("push: %v", err)
				}
				// Force a real pull each iter (bump content so version differs).
				if _, err := owner.DB().ExecContext(ctx, `INSERT INTO kv(k,v) VALUES(?,?)`, rows+i, "x"); err != nil {
					b.Fatalf("bump: %v", err)
				}
				if err := or.Push(ctx); err != nil {
					b.Fatalf("push2: %v", err)
				}
				if _, err := rr.Pull(ctx); err != nil {
					b.Fatalf("pull: %v", err)
				}
			}
		})
	}
}
