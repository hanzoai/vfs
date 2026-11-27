// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/dogi"
)

func newS3Backend(t *testing.T, srv *storetest.S3, prefix string) backend.Backend {
	t.Helper()
	be, err := s3.New(s3.Config{
		Endpoint:  srv.Endpoint(),
		Insecure:  true,
		Region:    "us-east-1",
		Bucket:    srv.Bucket,
		Prefix:    prefix,
		AccessKey: "k",
		SecretKey: "s",
	})
	if err != nil {
		t.Fatal(err)
	}
	return storetest.Strict{Backend: be}
}

// assertSpecFormatV2 verifies that every object in mem conforms strictly to
// canonical format-v2 (SPEC §3):
//   - "head" (with 30-byte fence header and no private DGHD/DGMN magic)
//   - "meta/<2hex>/<64hex-blake3>"
//   - "pack/<2hex>/<32hex-128bit-random-packID>"
//
// and zero plaintext filename/content leaks (Rule 1).
func assertSpecFormatV2(t *testing.T, mem *storetest.Mem, forbiddenPlaintext ...string) (shards, packs int) {
	t.Helper()
	metaRe := regexp.MustCompile(`^meta/([0-9a-f]{2})/([0-9a-f]{64})$`)
	packRe := regexp.MustCompile(`^pack/([0-9a-f]{2})/([0-9a-f]{32})$`)
	for key, obj := range mem.Dump() {
		if err := backend.ValidKey(key); err != nil {
			t.Fatalf("invalid backend key %q: %v", key, err)
		}
		if bytes.Contains(obj, []byte("DGHD")) || bytes.Contains(obj, []byte("DGMN")) {
			t.Fatalf("object %q contains non-canonical magic", key)
		}
		for _, secret := range forbiddenPlaintext {
			if secret != "" && (strings.Contains(key, secret) || bytes.Contains(obj, []byte(secret))) {
				t.Fatalf("object %q leaked plaintext %q", key, secret)
			}
		}
		switch {
		case key == "head":
			if len(obj) < 30 || string(obj[:6]) != "fence\x00" {
				t.Fatalf("head lacks canonical fence header: len=%d", len(obj))
			}
		case metaRe.MatchString(key):
			m := metaRe.FindStringSubmatch(key)
			sum := blake3.Sum256(obj)
			if m[2] != hex.EncodeToString(sum[:]) || m[1] != m[2][:2] {
				t.Fatalf("%s: not meta/<first byte>/<BLAKE3 of sealed shard>", key)
			}
			shards++
		case packRe.MatchString(key):
			m := packRe.FindStringSubmatch(key)
			if m[1] != m[2][:2] {
				t.Fatalf("%s: pack fan-out is not first byte of 128-bit random packID", key)
			}
			packs++
		default:
			t.Fatalf("key %q violates canonical format-v2 SPEC 3", key)
		}
	}
	return shards, packs
}

// TestDOGIStandardVolumeOpensBothModesAndOldReaderWorks proves that a Volume
// written with 3-stream lifetime-separated packing (Config.PackClassifier != nil)
// and relocated via CompactFragmentedPacks writes 100% standard format-v2
// objects (`head`, `meta/<2hex>/<64hex>`, `pack/<2hex>/<32hex>`), and that an
// unmodified standard reader (`vfs.Open` and `vfs.Reseal(Verify: true)` with
// `PackClassifier: nil`) opens, verifies, and reads every file byte-for-byte
// identically to 1-stream FIFO mode.
func TestDOGIStandardVolumeOpensBothModesAndOldReaderWorks(t *testing.T) {
	ciph := vfs.Hybrid(newKey(t))

	trace := dogi.GenerateAgentTrace(0x1344, 6, 12, 22<<10)
	clf := dogi.NewClassifier()
	clf.Train(trace.TrainRecords)

	type modeResult struct {
		mem  *storetest.Mem
		snap map[string]string
	}

	runMode := func(name string, classifier vfs.PackLifetimeClassifier) modeResult {
		mem := storetest.NewMem()
		strict := storetest.Strict{Backend: mem}
		vcfg := cfg(strict, ciph, 1)
		vcfg.PackClassifier = classifier
		vcfg.PackTargetBytes = 64 << 10

		if err := vfs.Create(ctx, vcfg); err != nil {
			t.Fatalf("%s Create: %v", name, err)
		}
		v, err := vfs.Attach(ctx, vcfg)
		if err != nil {
			t.Fatalf("%s Attach: %v", name, err)
		}
		defer v.Close()

		// Create workspace hierarchy with long-lived source/weights, medium artifacts,
		// and ephemeral build temporaries (.o, .tmp, /target/).
		ws := mkdir(t, v, vfs.RootInode, "ws-prod")
		srcDir := mkdir(t, v, ws, "src")
		modelsDir := mkdir(t, v, ws, "models")
		distDir := mkdir(t, v, ws, "dist")
		targetDir := mkdir(t, v, ws, "target")
		tmpDir := mkdir(t, v, ws, "tmp")

		secretFile := "confidential_weights_v2.safetensors"
		secretContent := append([]byte("TOP-SECRET-WEIGHTS-HEADER-"), content(901, 48<<10)...)

		files := map[string][]byte{
			"src/engine.go":        content(101, 36<<10),
			"src/packer.rs":        content(102, 42<<10),
			"src/small.go":         []byte("package main\n"),
			"models/" + secretFile: secretContent,
			"dist/bundle.tar":      content(201, 34<<10),
		}

		write(t, v, srcDir, "engine.go", files["src/engine.go"])
		write(t, v, srcDir, "packer.rs", files["src/packer.rs"])
		write(t, v, srcDir, "small.go", files["src/small.go"])
		write(t, v, modelsDir, secretFile, files["models/"+secretFile])
		write(t, v, distDir, "bundle.tar", files["dist/bundle.tar"])

		// Interleave ephemeral build temporaries that will be overwritten and deleted.
		for i := range 6 {
			write(t, v, targetDir, fmt.Sprintf("crate_%02d.o", i), content(uint64(300+i), 28<<10))
			write(t, v, tmpDir, fmt.Sprintf("scratch_%02d.tmp", i), content(uint64(400+i), 26<<10))
		}
		flush(t, v)

		// Overwrite one source file and remove all ephemeral temporaries.
		updatedEngine := content(199, 40<<10)
		files["src/engine.go"] = updatedEngine
		inEng, err := v.Lookup(ctx, srcDir, "engine.go")
		if err != nil {
			t.Fatal(err)
		}
		fh, err := v.Open(ctx, inEng.ID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fh.WriteAt(ctx, updatedEngine, 0); err != nil {
			t.Fatal(err)
		}
		_ = fh.Close()

		for i := range 6 {
			if err := v.Remove(ctx, targetDir, fmt.Sprintf("crate_%02d.o", i)); err != nil {
				t.Fatal(err)
			}
			if err := v.Remove(ctx, tmpDir, fmt.Sprintf("scratch_%02d.tmp", i)); err != nil {
				t.Fatal(err)
			}
		}
		flush(t, v)

		// Verify all stored keys and bytes conform strictly to canonical format-v2.
		shards, packs := assertSpecFormatV2(t, mem, secretFile, "TOP-SECRET-WEIGHTS-HEADER-")
		if shards == 0 || packs == 0 {
			t.Fatalf("%s expected shards>0 and packs>0, got shards=%d packs=%d", name, shards, packs)
		}

		// Compact fragmented packs on the canonical prolly tree.
		st, err := vfs.CompactFragmentedPacks(ctx, v, 0.70)
		if err != nil {
			t.Fatalf("%s CompactFragmentedPacks: %v", name, err)
		}
		if st.PacksDeleted == 0 {
			t.Fatalf("%s expected CompactFragmentedPacks to reclaim dead packs, got %+v", name, st)
		}
		assertSpecFormatV2(t, mem, secretFile, "TOP-SECRET-WEIGHTS-HEADER-")

		// Prove an unmodified standard read-only Volume (with PackClassifier: nil!)
		// opens the Volume, passes Check(), and reads every file byte-for-byte.
		standardRO, err := vfs.Open(ctx, vfs.Config{Backend: strict, Cipher: ciph})
		if err != nil {
			t.Fatalf("%s standard vfs.Open(PackClassifier: nil): %v", name, err)
		}
		defer standardRO.Close()
		if err := standardRO.Check(ctx); err != nil {
			t.Fatalf("%s standardRO.Check: %v", name, err)
		}
		for rel, want := range files {
			in, err := standardRO.Resolve(ctx, "/ws-prod/"+rel)
			if err != nil {
				t.Fatalf("%s standardRO.Resolve(%s): %v", name, rel, err)
			}
			mustEqual(t, name+" "+rel, readAll(t, standardRO, in.ID), want)
		}

		// Prove canonical Reseal with Verify: true (full shard + pack verification) passes.
		if err := vfs.Reseal(ctx, vfs.Config{
			Backend: strict,
			Cipher:  ciph,
			Round:   2,
			Verify:  true,
		}, strict.String()); err != nil {
			t.Fatalf("%s vfs.Reseal(Verify: true): %v", name, err)
		}

		return modeResult{
			mem:  mem,
			snap: snapshot(t, standardRO),
		}
	}

	fifo := runMode("fifo-1stream", nil)
	dogiMode := runMode("dogi-3stream", clf)

	// Both modes produce identical logical namespace and file content snapshots.
	sameSnapshot(t, "fifo vs dogi canonical volume snapshot", dogiMode.snap, fifo.snap)
}

// TestDOGISnapshotAndConcurrentReaderLivenessDuringRelocation verifies that:
//  1. A read-only snapshot Volume opened at Checkpoint 1 pins its RootRef so
//     CompactFragmentedPacks on Checkpoint 2 NEVER deletes packs needed by the
//     Checkpoint 1 snapshot reader, even when those files were deleted in
//     Checkpoint 2.
//  2. Active concurrent File.ReadAt goroutines on the writer Volume experience
//     zero ErrNotFound / ErrCorrupt errors while CompactFragmentedPacks
//     relocates surviving chunks and deletes unpinned fragmented packs.
//  3. Once the Checkpoint 1 snapshot is unpinned, a subsequent
//     CompactFragmentedPacks call reclaims the formerly pinned dead packs.
func TestDOGISnapshotAndConcurrentReaderLivenessDuringRelocation(t *testing.T) {
	ciph := vfs.Hybrid(newKey(t))
	srv := storetest.NewS3(t, "dogi-liveness")
	be := newS3Backend(t, srv, "vol-liveness")

	// Use 1-stream FIFO co-packing with 48 KiB PackTarget so live files and
	// deleted temporaries share packs, forcing relocation during compaction.
	vcfg := cfg(be, ciph, 1)
	vcfg.PackTargetBytes = 48 << 10
	vcfg.Cache = 1 << 16 // small cache so concurrent readers continuously hit backend packs
	if err := vfs.Create(ctx, vcfg); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(ctx, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	// Checkpoint 1: write a snapshot-only file plus shared live files and temporaries.
	snapOnlyData := content(777, 40<<10)
	snapOnlyIn := write(t, v, vfs.RootInode, "snapshot_only_ckpt1.bin", snapOnlyData)

	const numLive = 10
	liveInodes := make([]vfs.InodeID, numLive)
	livePayloads := make([][]byte, numLive)
	for i := range numLive {
		livePayloads[i] = content(uint64(2000+i), 22<<10)
		liveInodes[i] = write(t, v, vfs.RootInode, fmt.Sprintf("live_%02d.go", i), livePayloads[i]).ID
		// Interleave ephemeral temporaries in the same packs.
		for j := range 2 {
			write(t, v, vfs.RootInode, fmt.Sprintf("ephem_%02d_%d.o", i, j), content(uint64(3000+i*10+j), 24<<10))
		}
	}
	flush(t, v)

	// Open a read-only Volume snapshot at Checkpoint 1 and record its committed RootRef.
	snapRO, err := vfs.Open(ctx, vfs.Config{Backend: be, Cipher: ciph, Cache: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	defer snapRO.Close()
	ckpt1Root := snapRO.CommittedRoot()

	// Checkpoint 2: delete snapshot_only_ckpt1.bin and all ephem_*.o files.
	if err := v.Remove(ctx, vfs.RootInode, "snapshot_only_ckpt1.bin"); err != nil {
		t.Fatal(err)
	}
	for i := range numLive {
		for j := range 2 {
			if err := v.Remove(ctx, vfs.RootInode, fmt.Sprintf("ephem_%02d_%d.o", i, j)); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Also write Checkpoint 2 new files + temporaries that are NOT in ckpt1Root.
	for i := range 6 {
		write(t, v, vfs.RootInode, fmt.Sprintf("ckpt2_keep_%02d.go", i), content(uint64(5000+i), 20<<10))
		write(t, v, vfs.RootInode, fmt.Sprintf("ckpt2_temp_%02d.o", i), content(uint64(6000+i), 28<<10))
	}
	flush(t, v)
	for i := range 6 {
		if err := v.Remove(ctx, vfs.RootInode, fmt.Sprintf("ckpt2_temp_%02d.o", i)); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, v)

	// Launch concurrent readers on both the Checkpoint 1 read-only snapshot AND
	// the live Checkpoint 2 Volume while compaction relocates packs.
	stopReaders := make(chan struct{})
	var readOps atomic.Int64
	var readErrs atomic.Int64
	var wg sync.WaitGroup

	for r := range sized(3, 6) {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for step := 0; ; step++ {
				select {
				case <-stopReaders:
					return
				default:
				}
				if step%3 == 0 {
					snapRO.ClearCaches()
					got := readAll(t, snapRO, snapOnlyIn.ID)
					if !bytes.Equal(got, snapOnlyData) {
						readErrs.Add(1)
						t.Errorf("snapshot reader %d mismatch on snapshot_only_ckpt1.bin", id)
						return
					}
				}
				idx := (id + step) % numLive
				got := readAll(t, v, liveInodes[idx])
				if !bytes.Equal(got, livePayloads[idx]) {
					readErrs.Add(1)
					t.Errorf("live reader %d mismatch on live_%02d.go", id, idx)
					return
				}
				readOps.Add(1)
			}
		}(r)
	}

	// Compact while pinning ckpt1Root: Checkpoint 2's unpinned fragmented packs are
	// compacted and deleted, while Checkpoint 1's pinned packs are preserved!
	stPinned, err := vfs.CompactFragmentedPacks(ctx, v, 0.70, ckpt1Root)
	if err != nil {
		close(stopReaders)
		wg.Wait()
		t.Fatalf("CompactFragmentedPacks(pinned ckpt1Root): %v", err)
	}
	if stPinned.PacksPinnedSkipped == 0 {
		close(stopReaders)
		wg.Wait()
		t.Fatalf("expected PacksPinnedSkipped > 0 when ckpt1Root is pinned, got %+v", stPinned)
	}
	if stPinned.PacksDeleted == 0 {
		close(stopReaders)
		wg.Wait()
		t.Fatalf("expected unpinned Checkpoint 2 packs to be compacted & deleted, got %+v", stPinned)
	}

	close(stopReaders)
	wg.Wait()

	if readErrs.Load() > 0 {
		t.Fatalf("concurrent readers observed %d errors", readErrs.Load())
	}
	if readOps.Load() < 15 {
		t.Fatalf("too few concurrent read operations (%d) during relocation", readOps.Load())
	}

	// Verify the Checkpoint 1 snapshot reader still reads every Checkpoint 1 file
	// from cold storage (after clearing its cache).
	snapRO.ClearCaches()
	mustEqual(t, "pinned snapshot_only_ckpt1.bin after compaction", readAll(t, snapRO, snapOnlyIn.ID), snapOnlyData)
	for i := range numLive {
		mustEqual(t, fmt.Sprintf("pinned live_%02d.go on snapshot", i), readAll(t, snapRO, liveInodes[i]), livePayloads[i])
	}

	// Now unpin ckpt1Root (close snapshot reader) and run CompactFragmentedPacks
	// without pinned roots: the formerly pinned fragmented packs are now reclaimed!
	stUnpinned, err := vfs.CompactFragmentedPacks(ctx, v, 0.70)
	if err != nil {
		t.Fatalf("CompactFragmentedPacks(unpinned): %v", err)
	}
	if stUnpinned.PacksDeleted == 0 {
		t.Fatalf("expected formerly pinned packs to be reclaimed after unpinning, got %+v", stUnpinned)
	}

	// Verify all surviving Checkpoint 2 files remain readable from a fresh cold Open.
	coldRO := reopen(t, be, ciph)
	defer coldRO.Close()
	if err := coldRO.Check(ctx); err != nil {
		t.Fatalf("coldRO.Check after unpinned compaction: %v", err)
	}
	for i := range numLive {
		mustEqual(t, fmt.Sprintf("cold live_%02d.go after full compaction", i), readAll(t, coldRO, liveInodes[i]), livePayloads[i])
	}
	t.Logf("Snapshot & concurrent reader liveness verified: read_ops=%d pinned_skipped=%d deleted_phase1=%d deleted_phase2=%d",
		readOps.Load(), stPinned.PacksPinnedSkipped, stPinned.PacksDeleted, stUnpinned.PacksDeleted)
}

// TestDOGILostHeadACKAnd412CASOnCanonicalVolume exercises storetest.Hook
// Before/After faults on canonical `head` commits and 412 CAS stale-round
// fencing on a DOGI-enabled canonical vfs.Volume over storetest.NewS3.
func TestDOGILostHeadACKAnd412CASOnCanonicalVolume(t *testing.T) {
	ciph := vfs.Hybrid(newKey(t))
	srv := storetest.NewS3(t, "dogi-cas")
	rawBE := newS3Backend(t, srv, "vol-cas")

	trace := dogi.GenerateAgentTrace(0x1344, 6, 10, 22<<10)
	clf := dogi.NewClassifier()
	clf.Train(trace.TrainRecords)

	var (
		beforeFn func(op, key string) error
		afterFn  func(op, key string) error
		hookMu   sync.Mutex
	)
	setHooks := func(b, a func(op, key string) error) {
		hookMu.Lock()
		beforeFn = b
		afterFn = a
		hookMu.Unlock()
	}
	hookBE := &storetest.Hook{
		Backend: rawBE,
		Before: func(op, key string) error {
			hookMu.Lock()
			fn := beforeFn
			hookMu.Unlock()
			if fn != nil {
				return fn(op, key)
			}
			return nil
		},
		After: func(op, key string) error {
			hookMu.Lock()
			fn := afterFn
			hookMu.Unlock()
			if fn != nil {
				return fn(op, key)
			}
			return nil
		},
	}

	vcfg := cfg(hookBE, ciph, 1)
	vcfg.PackClassifier = clf
	vcfg.PackTargetBytes = 48 << 10
	if err := vfs.Create(ctx, vcfg); err != nil {
		t.Fatal(err)
	}
	v1, err := vfs.Attach(ctx, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v1.Close()

	srcDir := mkdir(t, v1, vfs.RootInode, "src")
	targetDir := mkdir(t, v1, vfs.RootInode, "target")
	fileA := content(101, 28<<10)
	inA := write(t, v1, srcDir, "keep_a.go", fileA)
	write(t, v1, targetDir, "temp_a.o", content(102, 28<<10))
	flush(t, v1)

	// 1. Before(head) failure during Flush -> prior checkpoint remains intact, retry succeeds.
	fileB := content(103, 30<<10)
	inB := write(t, v1, srcDir, "keep_b.go", fileB)
	errInjectedHead := errors.New("injected before head putver")
	setHooks(func(op, key string) error {
		if key == "head" && op == "putver" {
			return errInjectedHead
		}
		return nil
	}, nil)
	if err := v1.Flush(ctx); !errors.Is(err, errInjectedHead) {
		t.Fatalf("Flush with Before(head) = %v, want %v", err, errInjectedHead)
	}
	setHooks(nil, nil)

	// Cold reader sees keep_a.go intact and not yet keep_b.go.
	ro1 := reopen(t, rawBE, ciph)
	mustEqual(t, "keep_a before retry", readAll(t, ro1, inA.ID), fileA)
	if _, err := ro1.Stat(ctx, inB.ID); err == nil {
		t.Fatal("uncommitted keep_b visible after Before(head) failure")
	}
	_ = ro1.Close()

	// Retry Flush succeeds cleanly.
	flush(t, v1)

	// 2. After(head) lost-ACK during Flush -> Volume.landed recognizes the landed Head!
	fileC := content(104, 30<<10)
	inC := write(t, v1, srcDir, "keep_c.go", fileC)
	var lostHeadOnce atomic.Bool
	lostHeadOnce.Store(true)
	setHooks(nil, func(op, key string) error {
		if key == "head" && op == "putver" && lostHeadOnce.Swap(false) {
			return errors.New("simulated lost ACK after head PutIfVersion")
		}
		return nil
	})
	if err := v1.Flush(ctx); err != nil {
		t.Fatalf("Flush with After(head) lost-ACK failed to recognize landed commit: %v", err)
	}
	setHooks(nil, nil)

	// 3. After(head) lost-ACK during CompactFragmentedPacks -> landed check recognizes commit!
	if err := v1.Remove(ctx, targetDir, "temp_a.o"); err != nil {
		t.Fatal(err)
	}
	flush(t, v1)
	lostHeadOnce.Store(true)
	setHooks(nil, func(op, key string) error {
		if key == "head" && op == "putver" && lostHeadOnce.Swap(false) {
			return errors.New("simulated lost ACK after compaction head PutIfVersion")
		}
		return nil
	})
	st, err := vfs.CompactFragmentedPacks(ctx, v1, 0.70)
	if err != nil {
		t.Fatalf("CompactFragmentedPacks with After(head) lost-ACK failed: %v", err)
	}
	setHooks(nil, nil)
	if st.PacksDeleted == 0 {
		t.Fatalf("expected CompactFragmentedPacks to delete dead pack after lost-ACK recovery, got %+v", st)
	}

	// 4. Stale-round 412 CAS fencing over S3 when writer 2 attaches at Round 2.
	vcfgEqual := cfg(rawBE, ciph, 1)
	vcfgEqual.PackClassifier = clf
	if _, err := vfs.Attach(ctx, vcfgEqual); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("Attach at equal Round 1 = %v, want ErrStaleRound", err)
	}

	vcfg2 := cfg(rawBE, ciph, 2)
	vcfg2.PackClassifier = clf
	v2, err := vfs.Attach(ctx, vcfg2)
	if err != nil {
		t.Fatalf("Attach at Round 2: %v", err)
	}
	defer v2.Close()

	// Deposed writer v1 (Round 1) must be fenced with ErrStaleRound on Flush.
	write(t, v1, srcDir, "fenced_stale.go", content(105, 24<<10))
	if err := v1.Flush(ctx); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("deposed v1.Flush = %v, want ErrStaleRound", err)
	}

	// New owner v2 has keep_a, keep_b, keep_c intact and does not see fenced_stale.go.
	mustEqual(t, "v2 keep_a", readAll(t, v2, inA.ID), fileA)
	mustEqual(t, "v2 keep_b", readAll(t, v2, inB.ID), fileB)
	mustEqual(t, "v2 keep_c", readAll(t, v2, inC.ID), fileC)
	if _, err := v2.Resolve(ctx, "/src/fenced_stale.go"); err == nil {
		t.Fatal("deposed writer file leaked into new owner")
	}
}

// TestDOGIHeldOutPredictorOnCanonicalProductionVolume runs the chronologically
// split 40% train / 60% held-out coding-agent trace directly through the
// canonical vfs.Volume production packer over storetest.NewS3, comparing:
//   - PackClassifier: nil (1-stream FIFO baseline)
//   - PackClassifier: trained dogi.Classifier (3-stream DOGI learned)
//   - PackClassifier: dogi.OraclePredictor (3-stream NoDaP upper-bound ref)
func TestDOGIHeldOutPredictorOnCanonicalProductionVolume(t *testing.T) {
	ciph := vfs.Hybrid(newKey(t))
	srv := storetest.NewS3(t, "dogi-prod")

	rounds := sized(7, 9)
	filesPerRound := sized(12, 18)
	avgChunk := 22 << 10
	packTarget := 64 << 10

	trace := dogi.GenerateAgentTrace(0x20261344, rounds, filesPerRound, avgChunk)
	clf := dogi.NewClassifier()
	clf.Train(trace.TrainRecords)

	eval := dogi.EvaluateClassifier(clf, trace.HeldOutRecords)
	if eval.Accuracy < 0.85 {
		t.Fatalf("held-out classifier accuracy = %.3f (< 0.85), confusion=%v", eval.Accuracy, eval.Confusion)
	}
	oracle := &dogi.OraclePredictor{Truth: trace.OracleTruth}

	type runSummary struct {
		name         string
		physWritten  int64
		copiedBytes  uint64
		waf          float64
		packsCreated int64
		packsDeleted int
		s3Reqs       int
	}

	executeMode := func(prefix, name string, packClf vfs.PackLifetimeClassifier) runSummary {
		s3BE := newS3Backend(t, srv, prefix)
		probe := dogi.NewProbeBackend(s3BE, 0)
		vcfg := cfg(probe, ciph, 1)
		vcfg.PackClassifier = packClf
		vcfg.PackTargetBytes = packTarget

		if err := vfs.Create(ctx, vcfg); err != nil {
			t.Fatalf("%s Create: %v", name, err)
		}
		v, err := vfs.Attach(ctx, vcfg)
		if err != nil {
			t.Fatalf("%s Attach: %v", name, err)
		}
		defer v.Close()

		reqBefore := srv.Requests()
		_, logicalPlain, compStats, liveFiles, err := dogi.ExecuteTraceOnVolume(ctx, v, trace.HeldOutOps, 0.60)
		if err != nil {
			t.Fatalf("%s ExecuteTraceOnVolume: %v", name, err)
		}
		if err := v.Check(ctx); err != nil {
			t.Fatalf("%s v.Check: %v", name, err)
		}

		// Reopen with a standard unmodified read-only Volume (PackClassifier: nil)
		// and verify surviving files byte-for-byte.
		ro, err := vfs.Open(ctx, vfs.Config{Backend: s3BE, Cipher: ciph})
		if err != nil {
			t.Fatalf("%s standard vfs.Open: %v", name, err)
		}
		defer ro.Close()
		if err := ro.Check(ctx); err != nil {
			t.Fatalf("%s standard ro.Check: %v", name, err)
		}
		checked := 0
		for p, want := range liveFiles {
			if checked%4 == 0 {
				in, err := ro.Resolve(ctx, p)
				if err != nil {
					t.Fatalf("%s ro.Resolve(%s): %v", name, p, err)
				}
				mustEqual(t, name+" "+p, readAll(t, ro, in.ID), want)
			}
			checked++
		}

		physWritten := probe.PackBytesWritten.Load()
		copied := compStats.LiveBytesCopied
		logicalSealed := physWritten - int64(copied)
		if logicalSealed <= 0 {
			logicalSealed = logicalPlain
		}
		waf := float64(physWritten) / float64(logicalSealed)
		return runSummary{
			name:         name,
			physWritten:  physWritten,
			copiedBytes:  copied,
			waf:          waf,
			packsCreated: probe.PackPuts.Load(),
			packsDeleted: compStats.PacksDeleted,
			s3Reqs:       srv.Requests() - reqBefore,
		}
	}

	base := executeMode("prod-fifo", "baseline-fifo-1stream", nil)
	learned := executeMode("prod-learned", "dogi-learned-3stream", clf)
	oracleRun := executeMode("prod-oracle", "dogi-oracle-3stream", oracle)

	if learned.copiedBytes >= base.copiedBytes {
		t.Fatalf("expected DOGI learned LiveBytesCopied (%d) < Baseline (%d)", learned.copiedBytes, base.copiedBytes)
	}
	if learned.waf >= base.waf {
		t.Fatalf("expected DOGI learned WAF (%.4f) < Baseline (%.4f)", learned.waf, base.waf)
	}
	if oracleRun.copiedBytes > learned.copiedBytes {
		t.Fatalf("expected Oracle LiveBytesCopied (%d) <= Learned (%d)", oracleRun.copiedBytes, learned.copiedBytes)
	}

	t.Logf("Held-out accuracy=%.2f%% (train=%d heldout=%d severe_swaps=%d)",
		eval.Accuracy*100, len(trace.TrainRecords), len(trace.HeldOutRecords), eval.SevereMispredictions)
	t.Logf("Baseline FIFO : copied=%dB waf=%.4f packs=%d deleted=%d s3_reqs=%d",
		base.copiedBytes, base.waf, base.packsCreated, base.packsDeleted, base.s3Reqs)
	t.Logf("DOGI Learned  : copied=%dB waf=%.4f packs=%d deleted=%d s3_reqs=%d",
		learned.copiedBytes, learned.waf, learned.packsCreated, learned.packsDeleted, learned.s3Reqs)
	t.Logf("DOGI Oracle   : copied=%dB waf=%.4f packs=%d deleted=%d s3_reqs=%d",
		oracleRun.copiedBytes, oracleRun.waf, oracleRun.packsCreated, oracleRun.packsDeleted, oracleRun.s3Reqs)
}
