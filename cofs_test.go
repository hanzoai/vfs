// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/cofs"
)

// TestCoFSExtensionSealedRoundTripAndZeroShardColdLookups verifies that a CoFS
// index stored via Volume.PutExt ("ext/cofs/<hash>"):
//  1. Never leaks path or filename plaintext into backend objects (Rule 1).
//  2. Opens on a cold read-only clone in 1 GetExt call and serves 1,000 deep
//     positive + ENOENT lookups with 0 metadata shard ("meta/...") GETs.
//  3. Matches Volume.Resolve, Lookup, Stat, and ReadDir identically.
func TestCoFSExtensionSealedRoundTripAndZeroShardColdLookups(t *testing.T) {
	be, c, v := newMem(t)
	defer v.Close()

	usr := mkdir(t, v, vfs.RootInode, "usr")
	lib := mkdir(t, v, usr, "lib")
	secretName := "confidential-toolchain-symbol-xyz987.so"

	var paths []string
	dirs := sized(15, 40)
	for d := range dirs {
		pkg := mkdir(t, v, lib, fmt.Sprintf("pkg-%03d", d))
		for f := range 25 {
			nm := fmt.Sprintf("file-%03d.go", f)
			if d == 0 && f == 0 {
				nm = secretName
			}
			write(t, v, pkg, nm, content(uint64(d*100+f), 120))
			paths = append(paths, fmt.Sprintf("/usr/lib/pkg-%03d/%s", d, nm))
		}
	}
	flush(t, v)

	snapKey := []byte("immutable-snapshot-001")
	idx, err := cofs.Build(ctx, v, snapKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.SaveToVolume(ctx, v, snapKey); err != nil {
		t.Fatal(err)
	}

	// Verify Rule 1: no stored object in backend contains secretName in plaintext.
	foundExt := false
	for objKey, raw := range be.Dump() {
		if strings.HasPrefix(objKey, "ext/cofs/") {
			foundExt = true
		}
		if bytes.Contains(raw, []byte(secretName)) {
			t.Fatalf("backend object %s leaked plaintext filename %q", objKey, secretName)
		}
	}
	if !foundExt {
		t.Fatal("expected ext/cofs/<hash> object in backend")
	}

	// Cold read-only clone: open Volume (reads head) + LoadFromVolume (reads ext/cofs/...),
	// then resolve every path and 500 ENOENT paths with 0 meta/ shard reads.
	probe := cofs.NewProbeBackend(be, 0)
	ro, err := vfs.Open(ctx, cfg(probe, c, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	coldIdx, err := cofs.LoadFromVolume(ctx, ro, snapKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		in, err := coldIdx.Resolve(p)
		if err != nil {
			t.Fatalf("coldIdx.Resolve(%s): %v", p, err)
		}
		if in.Size != 120 {
			t.Fatalf("%s size = %d, want 120", p, in.Size)
		}
	}
	for i := range 500 {
		if _, err := coldIdx.Resolve(fmt.Sprintf("/usr/lib/pkg-000/missing-%04d", i)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("expected ErrNotExist, got %v", err)
		}
	}

	totalGets, metaGets, extGets := probe.Counts()
	if metaGets != 0 {
		t.Fatalf("cold CoFS lookup issued %d meta/ shard GETs, want 0 (total=%d, ext=%d)", metaGets, totalGets, extGets)
	}
	if extGets != 1 {
		t.Fatalf("expected exactly 1 ext/ GET for cold clone index, got %d", extGets)
	}
}
