// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cofs_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/cofs"
)

func TestMPHFBijectionAndEdgeSizes(t *testing.T) {
	seed := [32]byte{1, 2, 3, 4, 5}
	sizes := []int{0, 1, 2, 7, 64, 1000, 15000}
	if testing.Short() {
		sizes = []int{0, 1, 2, 7, 64, 2500}
	}

	for _, n := range sizes {
		keys := make([][]byte, n)
		for i := range n {
			keys[i] = fmt.Appendf(nil, "/usr/lib/pkg-%04d/file-%06d.so", i/32, i)
		}
		m, err := cofs.BuildMPHF(seed, "bijection-test", keys)
		if err != nil {
			t.Fatalf("n=%d: BuildMPHF failed: %v", n, err)
		}
		if m.Len() != n {
			t.Fatalf("n=%d: Len() = %d", n, m.Len())
		}
		seen := make([]bool, n)
		for i, k := range keys {
			s := m.Index(k)
			if s < 0 || s >= n {
				t.Fatalf("n=%d key %d: slot %d out of range [0, %d)", n, i, s, n)
			}
			if seen[s] {
				t.Fatalf("n=%d key %d: collision at slot %d", n, i, s)
			}
			seen[s] = true
			if sStr := m.IndexString(string(k)); sStr != s {
				t.Fatalf("n=%d: IndexString(%q)=%d != Index=%d", n, k, sStr, s)
			}
		}
	}

	// Duplicate key detection.
	dups := [][]byte{[]byte("same"), []byte("other"), []byte("same")}
	// Force into small bucket set where duplicates land in same bucket.
	if _, err := cofs.BuildMPHF(seed, "dup", [][]byte{[]byte("dup"), []byte("dup")}); !errors.Is(err, cofs.ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got %v", err)
	}
	_ = dups
}

func TestIndexExactnessZeroFalsePositivesAndLayeredWhiteouts(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ciph := vfs.Hybrid(k)
	if err := vfs.Create(ctx, vfs.Config{Backend: be, Cipher: ciph}); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, vfs.Config{Backend: be, Cipher: ciph, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	usr, _ := v.Mkdir(ctx, vfs.RootInode, "usr", 0o755)
	bin, _ := v.Mkdir(ctx, usr.ID, "bin", 0o755)
	lib, _ := v.Mkdir(ctx, usr.ID, "lib", 0o755)
	for i := range 200 {
		in, err := v.Create(ctx, bin.ID, fmt.Sprintf("tool-%03d", i), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		fh, _ := v.Open(ctx, in.ID)
		_, _ = fh.WriteAt(ctx, fmt.Appendf(nil, "binary-%d", i), 0)
		_ = fh.Close()
	}
	sub, _ := v.Mkdir(ctx, lib.ID, "deep", 0o755)
	leaf, _ := v.Create(ctx, sub.ID, "libcore.so", 0o644)
	_ = leaf
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	idx, err := cofs.Build(ctx, v, []byte("snap-1"))
	if err != nil {
		t.Fatal(err)
	}

	// Binary Marshal / Unmarshal + corruption guard.
	raw, err := idx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), raw...)
	bad[len(bad)/2] ^= 0x01
	if _, err := cofs.UnmarshalBinary(bad); !errors.Is(err, cofs.ErrCorruptIndex) {
		t.Fatalf("expected ErrCorruptIndex on bit flip, got %v", err)
	}
	idx2, err := cofs.UnmarshalBinary(raw)
	if err != nil {
		t.Fatal(err)
	}

	// Verify 100% positive parity + ReadDir order.
	for i := range 200 {
		p := fmt.Sprintf("/usr/bin/tool-%03d", i)
		got, err := idx2.Resolve(p)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", p, err)
		}
		want, err := v.Resolve(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want.ID || got.Size != want.Size || got.Mode != want.Mode {
			t.Fatalf("inode mismatch on %s: got %+v, want %+v", p, got, want)
		}
	}
	ents, err := idx2.ReadDir(bin.ID)
	if err != nil || len(ents) != 200 {
		t.Fatalf("ReadDir(bin): len=%d, err=%v", len(ents), err)
	}

	// Probe 20,000 non-existent paths and (dir, name) pairs -> 0 false positives.
	for i := range 20000 {
		missPath := fmt.Sprintf("/usr/bin/nonexistent-%06d", i)
		if _, err := idx2.Resolve(missPath); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("false positive on %s: err=%v", missPath, err)
		}
		// Partial/extended path prefix traps for verifyCleanPath:
		if _, err := idx2.Resolve("/extra/usr/lib/deep/libcore.so"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("false positive on extended prefix: %v", err)
		}
		if _, err := idx2.Resolve("/lib/deep/libcore.so"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("false positive on truncated prefix: %v", err)
		}
		if _, err := idx2.Lookup(bin.ID, fmt.Sprintf("missing-%06d", i)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("false positive Lookup: %v", err)
		}
	}

	// Concurrent race detector check across 16 readers + LayeredResolver whiteouts.
	wsStore := storetest.NewMem()
	_ = vfs.Create(ctx, vfs.Config{Backend: wsStore, Cipher: ciph})
	ws, err := vfs.Attach(ctx, vfs.Config{Backend: wsStore, Cipher: ciph, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	lr := cofs.NewLayeredResolver(idx2, ws)
	lr.MarkWorkspaceRemove("/usr/bin/tool-005")
	if _, _, err := lr.Resolve(ctx, "/usr/bin/tool-005"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected whiteout ErrNotExist, got %v", err)
	}

	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := range 200 {
				p := fmt.Sprintf("/usr/bin/tool-%03d", j)
				if j == 5 {
					_, _, _ = lr.Resolve(ctx, p)
					continue
				}
				in, src, err := lr.Resolve(ctx, p)
				if err != nil || src != cofs.SourceBase || in.ID == 0 {
					t.Errorf("goroutine %d failed on %s: %v", g, p, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkCoFSResolveDepth6(b *testing.B) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	ciph := vfs.Hybrid(k)
	_ = vfs.Create(ctx, vfs.Config{Backend: be, Cipher: ciph})
	v, _ := vfs.Attach(ctx, vfs.Config{Backend: be, Cipher: ciph, Round: 1, Journal: b.TempDir()})
	defer v.Close()

	d1, _ := v.Mkdir(ctx, vfs.RootInode, "usr", 0o755)
	d2, _ := v.Mkdir(ctx, d1.ID, "local", 0o755)
	d3, _ := v.Mkdir(ctx, d2.ID, "lib", 0o755)
	d4, _ := v.Mkdir(ctx, d3.ID, "node_modules", 0o755)
	d5, _ := v.Mkdir(ctx, d4.ID, "typescript", 0o755)
	paths := make([]string, 256)
	for i := range paths {
		nm := fmt.Sprintf("mod-%03d.js", i)
		_, _ = v.Create(ctx, d5.ID, nm, 0o644)
		paths[i] = "/usr/local/lib/node_modules/typescript/" + nm
	}
	_ = v.Flush(ctx)

	idx, err := cofs.Build(ctx, v, []byte("bench"))
	if err != nil {
		b.Fatal(err)
	}

	b.Run("VFS_WarmProllyResolve", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = v.Resolve(ctx, paths[i&255])
		}
	})
	b.Run("CoFS_MPHFResolve", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = idx.Resolve(paths[i&255])
		}
	})
}
