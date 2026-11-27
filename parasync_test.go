// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/fence"
)

func TestParaSyncFullAndDeltaCorrectness(t *testing.T) {
	srcMem := storetest.NewMem()
	dstMem := storetest.NewMem()
	srcBE := storetest.Strict{Backend: srcMem}
	dstBE := storetest.Strict{Backend: dstMem}
	c := vfsAge(t)

	if err := vfs.Create(ctx, cfg(srcBE, c, 0)); err != nil {
		t.Fatal(err)
	}
	srcVol, err := vfs.Attach(ctx, cfg(srcBE, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer srcVol.Close()

	stateOne(t, srcVol)
	const secretName = "parasync-confidential-path-xyz.txt"
	const secretBody = "PARASYNC-PLAINTEXT-MARKER-987654"
	write(t, srcVol, vfs.RootInode, secretName, []byte(secretBody))
	flush(t, srcVol)
	snap1 := snapshot(t, reopen(t, srcBE, c))

	// Empty destination: PlanDelta cleanly degrades into a full restore plan.
	plan1, err := vfs.PlanDelta(ctx, srcBE, dstBE, c)
	if err != nil {
		t.Fatalf("PlanDelta full: %v", err)
	}
	if plan1.HasDst {
		t.Fatal("expected HasDst=false on empty destination")
	}
	if len(plan1.ShardsToCopy) == 0 || len(plan1.ShardsToCopy) != plan1.TotalSourceShards {
		t.Fatalf("unexpected full shard plan: copy=%d total=%d", len(plan1.ShardsToCopy), plan1.TotalSourceShards)
	}
	if len(plan1.PacksToCopy) == 0 || len(plan1.PacksToCopy) != plan1.TotalSourcePacks {
		t.Fatalf("unexpected full pack plan: copy=%d total=%d", len(plan1.PacksToCopy), plan1.TotalSourcePacks)
	}
	if plan1.ShardsPruned != 0 || plan1.PacksPruned != 0 {
		t.Fatalf("expected 0 pruned on empty dst, got shards=%d packs=%d", plan1.ShardsPruned, plan1.PacksPruned)
	}
	if plan1.FullSnapshotBytes != plan1.MetaBytes+plan1.PackBytes || plan1.FullSnapshotBytes <= 0 {
		t.Fatalf("unexpected full bytes: meta=%d pack=%d full=%d", plan1.MetaBytes, plan1.PackBytes, plan1.FullSnapshotBytes)
	}

	stats1, err := vfs.RestoreDelta(ctx, srcBE, dstBE, c, vfs.RestoreOptions{Round: 1, Streams: 4})
	if err != nil {
		t.Fatalf("RestoreDelta full: %v", err)
	}
	if stats1.ShardsCopied != len(plan1.ShardsToCopy) || stats1.PacksCopied != len(plan1.PacksToCopy) {
		t.Fatalf("copied mismatch: got shards=%d packs=%d, want %d/%d",
			stats1.ShardsCopied, stats1.PacksCopied, len(plan1.ShardsToCopy), len(plan1.PacksToCopy))
	}
	if stats1.BytesTransferred != stats1.FullSnapshotBytes {
		t.Fatalf("full restore bytes=%d, want %d", stats1.BytesTransferred, stats1.FullSnapshotBytes)
	}

	dstRO1 := reopen(t, dstBE, c)
	defer dstRO1.Close()
	sameSnapshot(t, "parasync full", snapshot(t, dstRO1), snap1)
	if err := dstRO1.Check(ctx); err != nil {
		t.Fatalf("dstRO1.Check: %v", err)
	}

	// Rule 1 check: no plaintext filename or content marker in destination objects.
	for objKey, raw := range dstMem.Dump() {
		if bytes.Contains(raw, []byte(secretName)) || bytes.Contains(raw, []byte(secretBody)) {
			t.Fatalf("destination object %s leaked plaintext", objKey)
		}
	}

	// Advance source to stateTwo and add a new large pack.
	stateTwo(t, srcVol)
	dataDir, err := srcVol.Resolve(ctx, "/data")
	if err != nil {
		t.Fatal(err)
	}
	write(t, srcVol, dataDir.ID, "delta-new", content(8888, 1200_000))
	flush(t, srcVol)
	snap2 := snapshot(t, reopen(t, srcBE, c))

	plan2, err := vfs.PlanDelta(ctx, srcBE, dstBE, c)
	if err != nil {
		t.Fatalf("PlanDelta delta: %v", err)
	}
	if !plan2.HasDst {
		t.Fatal("expected HasDst=true on populated destination")
	}
	if plan2.DstHead.ID != plan2.SrcHead.ID {
		t.Fatalf("volume id mismatch: dst=%x src=%x", plan2.DstHead.ID, plan2.SrcHead.ID)
	}
	if plan2.ShardsPruned == 0 || plan2.PacksPruned == 0 {
		t.Fatalf("expected pruned objects on delta plan: shardsPruned=%d packsPruned=%d", plan2.ShardsPruned, plan2.PacksPruned)
	}
	if plan2.TotalSourceShards != len(plan2.ShardsToCopy)+plan2.ShardsPruned {
		t.Fatalf("shard total invariant violated: %d != %d + %d", plan2.TotalSourceShards, len(plan2.ShardsToCopy), plan2.ShardsPruned)
	}
	if plan2.TotalSourcePacks != len(plan2.PacksToCopy)+plan2.PacksPruned {
		t.Fatalf("pack total invariant violated: %d != %d + %d", plan2.TotalSourcePacks, len(plan2.PacksToCopy), plan2.PacksPruned)
	}
	if plan2.MetaBytes+plan2.PackBytes >= plan2.FullSnapshotBytes {
		t.Fatalf("delta plan did not shrink transfer: delta=%d full=%d", plan2.MetaBytes+plan2.PackBytes, plan2.FullSnapshotBytes)
	}

	stats2, err := vfs.RestoreDelta(ctx, srcBE, dstBE, c, vfs.RestoreOptions{Round: 2, Streams: 4})
	if err != nil {
		t.Fatalf("RestoreDelta delta: %v", err)
	}
	if stats2.BytesTransferred >= stats2.FullSnapshotBytes {
		t.Fatalf("delta transferred %d >= full %d", stats2.BytesTransferred, stats2.FullSnapshotBytes)
	}
	if stats2.ShardsCopied != len(plan2.ShardsToCopy) || stats2.PacksCopied != len(plan2.PacksToCopy) {
		t.Fatalf("delta copied mismatch: got %d/%d, want %d/%d",
			stats2.ShardsCopied, stats2.PacksCopied, len(plan2.ShardsToCopy), len(plan2.PacksToCopy))
	}

	dstRO2 := reopen(t, dstBE, c)
	defer dstRO2.Close()
	sameSnapshot(t, "parasync delta", snapshot(t, dstRO2), snap2)
	if err := dstRO2.Check(ctx); err != nil {
		t.Fatalf("dstRO2.Check: %v", err)
	}

	// Idempotent no-op delta restore when dst already matches src.
	plan3, err := vfs.PlanDelta(ctx, srcBE, dstBE, c)
	if err != nil {
		t.Fatalf("PlanDelta noop: %v", err)
	}
	if len(plan3.ShardsToCopy) != 0 || len(plan3.PacksToCopy) != 0 {
		t.Fatalf("expected 0 objects to copy on identical checkpoint, got shards=%d packs=%d",
			len(plan3.ShardsToCopy), len(plan3.PacksToCopy))
	}
	stats3, err := vfs.RestoreDelta(ctx, srcBE, dstBE, c, vfs.RestoreOptions{Round: 3, Streams: 4})
	if err != nil {
		t.Fatalf("RestoreDelta noop: %v", err)
	}
	if stats3.BytesTransferred != 0 || stats3.SourceGets != 1 {
		t.Fatalf("noop restore expected 0 bytes / 1 source get (head), got bytes=%d gets=%d",
			stats3.BytesTransferred, stats3.SourceGets)
	}

	// Destination can be attached at a higher round and written cleanly after restore.
	dstRW, err := vfs.Attach(ctx, cfg(dstBE, c, 4))
	if err != nil {
		t.Fatalf("attach restored dst: %v", err)
	}
	defer dstRW.Close()
	write(t, dstRW, vfs.RootInode, "post-restore", []byte("ok"))
	flush(t, dstRW)
	if err := dstRW.Check(ctx); err != nil {
		t.Fatalf("dstRW.Check: %v", err)
	}
}

func TestParaSyncHeadCAS412AndConcurrentFencing(t *testing.T) {
	srv := storetest.NewS3(t, "disk")
	newS3 := func(prefix string) backend.Backend {
		be, err := s3.New(s3.Config{
			Endpoint:  srv.Endpoint(),
			Insecure:  true,
			Region:    "us-east-1",
			Bucket:    "disk",
			Prefix:    prefix,
			AccessKey: "k",
			SecretKey: "s",
		})
		if err != nil {
			t.Fatal(err)
		}
		return be
	}
	srcBE := newS3("src")
	dstBE := newS3("dst")
	c := vfsAge(t)

	if err := vfs.Create(ctx, cfg(srcBE, c, 0)); err != nil {
		t.Fatal(err)
	}
	srcVol, err := vfs.Attach(ctx, cfg(srcBE, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer srcVol.Close()

	stateOne(t, srcVol)
	flush(t, srcVol)
	snap1 := snapshot(t, reopen(t, srcBE, c))

	if _, err := vfs.RestoreDelta(ctx, srcBE, dstBE, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
		t.Fatalf("initial restore: %v", err)
	}

	stateTwo(t, srcVol)
	flush(t, srcVol)
	snap2 := snapshot(t, reopen(t, srcBE, c))

	// Invalid bounds on Round.
	for _, badRound := range []uint64{0, math.MaxUint64} {
		if _, err := vfs.RestoreDelta(ctx, srcBE, dstBE, c, vfs.RestoreOptions{Round: badRound, Streams: 4}); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("Round=%d: got %v, want fs.ErrInvalid", badRound, err)
		}
	}

	// Bump writer round on dst to 5 via Reseal; stale rounds <= 5 must be rejected.
	if err := vfs.Reseal(ctx, cfg(dstBE, c, 5), dstBE.String()); err != nil {
		t.Fatalf("bump dst round: %v", err)
	}
	for _, stale := range []uint64{3, 5} {
		if _, err := vfs.RestoreDelta(ctx, srcBE, dstBE, c, vfs.RestoreOptions{Round: stale, Streams: 4}); !errors.Is(err, vfs.ErrStaleRound) {
			t.Fatalf("stale Round=%d: got %v, want ErrStaleRound", stale, err)
		}
	}
	sameSnapshot(t, "dst untouched after stale round", snapshot(t, reopen(t, dstBE, c)), snap1)

	// Live concurrent fencing race right before head CAS on S3:
	// hook fires before putver("head") and advances dst to Round 10, causing
	// storetest.S3 to reject RestoreDelta's If-Match PUT with HTTP 412 -> ErrConflict,
	// after which fence.Rewrite sees recorded round 10 > 7 and returns ErrStaleRound.
	var raced atomic.Bool
	racedHook := &storetest.Hook{
		Backend: dstBE,
		Before: func(op, key string) error {
			if op == "putver" && key == "head" && raced.CompareAndSwap(false, true) {
				if err := vfs.Reseal(ctx, cfg(dstBE, c, 10), dstBE.String()); err != nil {
					return err
				}
			}
			return nil
		},
	}
	if _, err := vfs.RestoreDelta(ctx, srcBE, racedHook, c, vfs.RestoreOptions{Round: 7, Streams: 4}); !errors.Is(err, vfs.ErrStaleRound) {
		t.Fatalf("concurrent 412 race: got %v, want ErrStaleRound", err)
	}
	if !raced.Load() {
		t.Fatal("expected putver hook to trigger 412 race")
	}
	sameSnapshot(t, "dst untouched after concurrent 412", snapshot(t, reopen(t, dstBE, c)), snap1)

	// Valid higher Round 11 succeeds and completes the delta restore.
	if _, err := vfs.RestoreDelta(ctx, srcBE, dstBE, c, vfs.RestoreOptions{Round: 11, Streams: 4}); err != nil {
		t.Fatalf("RestoreDelta at round 11: %v", err)
	}
	sameSnapshot(t, "dst after valid round 11", snapshot(t, reopen(t, dstBE, c)), snap2)
}

func TestParaSyncFailureLeavesDestIntactAndCancelClean(t *testing.T) {
	srcMem := storetest.NewMem()
	dstMem := storetest.NewMem()
	c := vfsAge(t)

	if err := vfs.Create(ctx, cfg(srcMem, c, 0)); err != nil {
		t.Fatal(err)
	}
	srcVol, err := vfs.Attach(ctx, cfg(srcMem, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer srcVol.Close()

	stateOne(t, srcVol)
	flush(t, srcVol)
	snap1 := snapshot(t, reopen(t, srcMem, c))

	if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
		t.Fatal(err)
	}

	stateTwo(t, srcVol)
	flush(t, srcVol)
	snap2 := snapshot(t, reopen(t, srcMem, c))

	plan, err := vfs.PlanDelta(ctx, srcMem, dstMem, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.PacksToCopy) == 0 || len(plan.ShardsToCopy) == 0 {
		t.Fatalf("expected non-empty delta packs (%d) and shards (%d)", len(plan.PacksToCopy), len(plan.ShardsToCopy))
	}

	// 1. Mid-pack failure leaves dst at stateOne.
	boom := errors.New("injected io failure")
	packHook := &storetest.Hook{
		Backend: dstMem,
		Before: func(op, key string) error {
			if op == "putabsent" && strings.HasPrefix(key, "pack/") {
				return boom
			}
			return nil
		},
	}
	if _, err := vfs.RestoreDelta(ctx, srcMem, packHook, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !errors.Is(err, boom) {
		t.Fatalf("mid-pack failure = %v, want %v", err, boom)
	}
	sameSnapshot(t, "dst after mid-pack failure", snapshot(t, reopen(t, dstMem, c)), snap1)

	// 2. Mid-shard failure (after packs copied) leaves dst at stateOne.
	var shardWrites atomic.Int32
	shardHook := &storetest.Hook{
		Backend: dstMem,
		Before: func(op, key string) error {
			if op == "putabsent" && strings.HasPrefix(key, "meta/") && shardWrites.Add(1) >= 2 {
				return boom
			}
			return nil
		},
	}
	if _, err := vfs.RestoreDelta(ctx, srcMem, shardHook, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !errors.Is(err, boom) {
		t.Fatalf("mid-shard failure = %v, want %v", err, boom)
	}
	sameSnapshot(t, "dst after mid-shard failure", snapshot(t, reopen(t, dstMem, c)), snap1)

	// 3. Context cancellation mid-shard leaves dst at stateOne.
	cctx, cancel := context.WithCancel(ctx)
	cancelHook := &storetest.Hook{
		Backend: dstMem,
		Before: func(op, key string) error {
			if op == "putabsent" && strings.HasPrefix(key, "meta/") {
				cancel()
			}
			return nil
		},
	}
	if _, err := vfs.RestoreDelta(cctx, srcMem, cancelHook, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restore = %v, want context.Canceled", err)
	}
	sameSnapshot(t, "dst after cancel", snapshot(t, reopen(t, dstMem, c)), snap1)

	// 4. Conflicting pack with wrong size on dst must fail closed.
	corruptDst := storetest.NewMem()
	if _, err := vfs.RestoreDelta(ctx, dstMem, corruptDst, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
		t.Fatal(err)
	}
	if err := corruptDst.Put(ctx, plan.PacksToCopy[0], []byte("corrupt-short-pack")); err != nil {
		t.Fatal(err)
	}
	if _, err := vfs.RestoreDelta(ctx, srcMem, corruptDst, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("conflicting pack size = %v, want ErrCorrupt", err)
	}
	sameSnapshot(t, "corruptDst head untouched", snapshot(t, reopen(t, corruptDst, c)), snap1)

	// 5. Clean retry after partial writes reuses content-addressed objects (ErrConflict) and finishes cleanly.
	if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 2, Streams: 4}); err != nil {
		t.Fatalf("retry after partial failure: %v", err)
	}
	sameSnapshot(t, "dst after clean retry", snapshot(t, reopen(t, dstMem, c)), snap2)
}

func TestParaSyncCorruptObjectCrossVolumeAndKeyRotation(t *testing.T) {
	oldKey := newKey(t)
	newKey_ := newKey(t)
	oldCipher := vfs.Hybrid(oldKey)
	rotatedCipher := vfs.Hybrid(newKey_, oldKey)
	wrongCipher := vfsAge(t)

	srcMem := storetest.NewMem()
	dstMem := storetest.NewMem()
	if err := vfs.Create(ctx, cfg(srcMem, oldCipher, 0)); err != nil {
		t.Fatal(err)
	}
	srcVol, err := vfs.Attach(ctx, cfg(srcMem, oldCipher, 1))
	if err != nil {
		t.Fatal(err)
	}
	stateOne(t, srcVol)
	flush(t, srcVol)
	snap1 := snapshot(t, reopen(t, srcMem, oldCipher))

	// Wrong key fails closed on PlanDelta and RestoreDelta.
	if _, err := vfs.PlanDelta(ctx, srcMem, dstMem, wrongCipher); !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("PlanDelta wrong key = %v, want ErrUnknownKey", err)
	}
	if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, wrongCipher, vfs.RestoreOptions{Round: 1, Streams: 4}); !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("RestoreDelta wrong key = %v, want ErrUnknownKey", err)
	}

	// Restore stateOne with old key.
	if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, oldCipher, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
		t.Fatal(err)
	}

	// Corrupt a source shard copy and verify both PlanDelta and RestoreDelta fail closed.
	badSrc := storetest.NewMem()
	for k, v := range srcMem.Dump() {
		_ = badSrc.Put(ctx, k, v)
	}
	for k, v := range badSrc.Dump() {
		if strings.HasPrefix(k, "meta/") {
			b := bytes.Clone(v)
			b[len(b)/2] ^= 0xff
			_ = badSrc.Put(ctx, k, b)
			break
		}
	}
	if _, err := vfs.PlanDelta(ctx, badSrc, storetest.NewMem(), oldCipher); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("PlanDelta corrupt shard = %v, want ErrCorrupt", err)
	}
	if _, err := vfs.RestoreDelta(ctx, badSrc, dstMem, oldCipher, vfs.RestoreOptions{Round: 2, Streams: 4}); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("RestoreDelta corrupt shard = %v, want ErrCorrupt", err)
	}
	sameSnapshot(t, "dst untouched after corrupt src", snapshot(t, reopen(t, dstMem, oldCipher)), snap1)

	// Cross-volume rejection: another Volume under the SAME key must be rejected.
	otherVolMem := storetest.NewMem()
	if err := vfs.Create(ctx, cfg(otherVolMem, oldCipher, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := vfs.PlanDelta(ctx, srcMem, otherVolMem, oldCipher); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("PlanDelta cross-volume = %v, want ErrCorrupt", err)
	}
	if _, err := vfs.RestoreDelta(ctx, srcMem, otherVolMem, oldCipher, vfs.RestoreOptions{Round: 2, Streams: 4}); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("RestoreDelta cross-volume = %v, want ErrCorrupt", err)
	}

	// Key rotation: close old writer, attach with Hybrid(newKey, oldKey), mutate to stateTwo, delta restore.
	if err := srcVol.Close(); err != nil {
		t.Fatal(err)
	}
	srcRot, err := vfs.Attach(ctx, cfg(srcMem, rotatedCipher, 2))
	if err != nil {
		t.Fatal(err)
	}
	defer srcRot.Close()
	stateTwo(t, srcRot)
	flush(t, srcRot)
	snap2 := snapshot(t, reopen(t, srcMem, rotatedCipher))

	if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, rotatedCipher, vfs.RestoreOptions{Round: 2, Streams: 4}); err != nil {
		t.Fatalf("RestoreDelta under rotated cipher: %v", err)
	}
	sameSnapshot(t, "dst under rotated cipher", snapshot(t, reopen(t, dstMem, rotatedCipher)), snap2)

	// Old key alone can no longer open dst head because RestoreDelta re-sealed head under newKey.
	if _, err := vfs.Open(ctx, cfg(dstMem, oldCipher, 0)); !errors.Is(err, vfs.ErrUnknownKey) {
		t.Fatalf("open dst with retired key = %v, want ErrUnknownKey", err)
	}

	// Unknown Needs bit in source Head fails closed with ErrNeeds.
	needsSrc := storetest.NewMem()
	for k, v := range srcMem.Dump() {
		_ = needsSrc.Put(ctx, k, v)
	}
	// Craft a Head with unknown Needs bit 1 using Head fields from PlanDelta.
	pl, err := vfs.PlanDelta(ctx, srcMem, storetest.NewMem(), rotatedCipher)
	if err != nil {
		t.Fatal(err)
	}
	badHead := pl.SrcHead
	badHead.Needs |= 1 // reserved bit 1 ("snapshots") unknown to this build
	badHeadPlain := marshalTestHead(badHead)
	sealedBadHead, keyID, err := rotatedCipher.Seal(append(testHeadAAD(needsSrc.String()), badHeadPlain...))
	if err != nil {
		t.Fatal(err)
	}
	frameBadHead := append([]byte{byte(len(keyID))}, []byte(keyID)...)
	frameBadHead = append(frameBadHead, sealedBadHead...)
	if err := fence.New(needsSrc, 64<<10).Put(ctx, "head", frameBadHead, 9); err != nil {
		t.Fatal(err)
	}
	if _, err := vfs.PlanDelta(ctx, needsSrc, storetest.NewMem(), rotatedCipher); !errors.Is(err, vfs.ErrNeeds) {
		t.Fatalf("PlanDelta unknown Head.Needs = %v, want ErrNeeds", err)
	}
}

func marshalTestHead(h vfs.Head) []byte {
	var b []byte
	b = binary.AppendUvarint(b, h.Seq)
	b = append(b, h.Root[:]...)
	b = binary.AppendUvarint(b, h.Next)
	b = append(b, h.ID[:]...)
	b = append(b, h.Seed[:]...)
	b = binary.AppendUvarint(b, h.Journal)
	b = binary.AppendUvarint(b, h.Needs)
	b = append(b, h.Tail...)
	return b
}

func testHeadAAD(place string) []byte {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs seal context")
	frame := func(p []byte) {
		_, _ = h.Write(binary.AppendUvarint(nil, uint64(len(p))))
		_, _ = h.Write(p)
	}
	frame([]byte("head"))
	frame([]byte(place))
	return h.Sum(nil)
}

func TestParaSyncSameLengthCorruptResidentPackRejected(t *testing.T) {
	srcMem := storetest.NewMem()
	dstMem := storetest.NewMem()
	c := vfsAge(t)

	if err := vfs.Create(ctx, cfg(srcMem, c, 0)); err != nil {
		t.Fatal(err)
	}
	srcVol, err := vfs.Attach(ctx, cfg(srcMem, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer srcVol.Close()

	stateOne(t, srcVol)
	write(t, srcVol, vfs.RootInode, "pack-resident.bin", content(4242, 1200_000))
	flush(t, srcVol)
	snap1 := snapshot(t, reopen(t, srcMem, c))

	stats1, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 1, Streams: 4})
	if err != nil {
		t.Fatalf("initial restore: %v", err)
	}
	if stats1.ChunksVerified == 0 || stats1.VerifyBytesRead <= 0 || stats1.VerifyGets <= 0 {
		t.Fatalf("expected non-zero chunk verification metrics on initial restore, got chunks=%d bytes=%d gets=%d",
			stats1.ChunksVerified, stats1.VerifyBytesRead, stats1.VerifyGets)
	}

	// Find a resident pack/... object on dst and flip 1 byte while keeping the exact byte length.
	var residentPackKey string
	for k, v := range dstMem.Dump() {
		if strings.HasPrefix(k, "pack/") {
			residentPackKey = k
			corrupted := bytes.Clone(v)
			corrupted[len(corrupted)/2] ^= 0xff
			if len(corrupted) != len(v) {
				t.Fatal("byte length changed")
			}
			if err := dstMem.Put(ctx, k, corrupted); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if residentPackKey == "" {
		t.Fatal("expected at least one resident pack on destination")
	}

	// Mutate src to checkpoint 2 while keeping pack-resident.bin live so PlanDelta prunes residentPackKey.
	stateTwo(t, srcVol)
	write(t, srcVol, vfs.RootInode, "pack-delta2.bin", content(5353, 1200_000))
	flush(t, srcVol)

	plan2, err := vfs.PlanDelta(ctx, srcMem, dstMem, c)
	if err != nil {
		t.Fatalf("PlanDelta with same-length corrupt resident pack: %v", err)
	}
	if plan2.PacksPruned == 0 {
		t.Fatalf("expected resident pack %s to be pruned by Stat length in PlanDelta", residentPackKey)
	}

	// Record dst head before RestoreDelta: RestoreDelta MUST fail closed with ErrCorrupt
	// during chunk verification BEFORE advancing dst head.
	headBefore, verBefore, err := dstMem.Get(ctx, "head", backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("RestoreDelta with same-length bit-flipped resident pack = %v, want ErrCorrupt", err)
	}
	headAfter, verAfter, err := dstMem.Get(ctx, "head", backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	if verAfter != verBefore || !bytes.Equal(headAfter, headBefore) {
		t.Fatalf("destination head changed despite corrupt resident pack (ver %q -> %q)", verBefore, verAfter)
	}
	_ = snap1
}

func TestParaSyncSameLengthConflictingPackSubstitutionRejected(t *testing.T) {
	srcMem := storetest.NewMem()
	dstMem := storetest.NewMem()
	c := vfsAge(t)

	if err := vfs.Create(ctx, cfg(srcMem, c, 0)); err != nil {
		t.Fatal(err)
	}
	srcVol, err := vfs.Attach(ctx, cfg(srcMem, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer srcVol.Close()

	stateOne(t, srcVol)
	flush(t, srcVol)
	snap1 := snapshot(t, reopen(t, srcMem, c))

	if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
		t.Fatal(err)
	}

	// Advance src to checkpoint 2 with a new packed file.
	stateTwo(t, srcVol)
	write(t, srcVol, vfs.RootInode, "pack-new.bin", content(7777, 1200_000))
	flush(t, srcVol)

	plan, err := vfs.PlanDelta(ctx, srcMem, dstMem, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.PacksToCopy) == 0 {
		t.Fatal("expected at least one new pack in PacksToCopy")
	}
	targetPack := plan.PacksToCopy[0]
	rawPack, _, err := srcMem.Get(ctx, targetPack, backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}

	// Inject a same-length substituted/bit-flipped pack on dst right when RestoreDelta
	// attempts PutIfAbsent(targetPack), triggering ErrConflict with matching Stat length.
	bogusPack := bytes.Clone(rawPack)
	bogusPack[len(bogusPack)/3] ^= 0xa5
	hookDst := &storetest.Hook{
		Backend: dstMem,
		Before: func(op, key string) error {
			if op == "putabsent" && key == targetPack {
				return dstMem.Put(ctx, targetPack, bogusPack)
			}
			return nil
		},
	}

	if _, err := vfs.RestoreDelta(ctx, srcMem, hookDst, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !errors.Is(err, vfs.ErrCorrupt) {
		t.Fatalf("RestoreDelta with same-length conflicting pack substitution = %v, want ErrCorrupt", err)
	}
	// Destination head must remain untouched at checkpoint 1.
	sameSnapshot(t, "dst untouched after conflicting pack substitution", snapshot(t, reopen(t, dstMem, c)), snap1)
}

func TestParaSyncLostHeadACKAndGCRaces(t *testing.T) {
	c := vfsAge(t)

	// (a) Lost Head ACK on PutIfVersion/PutIfAbsent ("head"): write landed on dst,
	// After hook returns simulated network error, RestoreDelta inspects dst head,
	// recognizes our commit landed, sets LostHeadACKRecovered=true, and returns nil.
	t.Run("lost_head_ack_recovered", func(t *testing.T) {
		srcMem := storetest.NewMem()
		dstMem := storetest.NewMem()
		if err := vfs.Create(ctx, cfg(srcMem, c, 0)); err != nil {
			t.Fatal(err)
		}
		srcVol, err := vfs.Attach(ctx, cfg(srcMem, c, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer srcVol.Close()

		stateOne(t, srcVol)
		flush(t, srcVol)
		if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
			t.Fatal(err)
		}

		stateTwo(t, srcVol)
		write(t, srcVol, vfs.RootInode, "pack-ack.bin", content(31415, 1100_000))
		flush(t, srcVol)
		snap2 := snapshot(t, reopen(t, srcMem, c))

		lostACK := errors.New("simulated lost HTTP 200 ACK after head CAS")
		var injected atomic.Bool
		ackHook := &storetest.Hook{
			Backend: dstMem,
			After: func(op, key string) error {
				if (op == "putver" || op == "putabsent") && key == "head" && injected.CompareAndSwap(false, true) {
					return lostACK
				}
				return nil
			},
		}

		stats, err := vfs.RestoreDelta(ctx, srcMem, ackHook, c, vfs.RestoreOptions{Round: 2, Streams: 4})
		if err != nil {
			t.Fatalf("RestoreDelta should recover lost head ACK, got err: %v", err)
		}
		if !injected.Load() || !stats.LostHeadACKRecovered {
			t.Fatalf("expected LostHeadACKRecovered=true (injected=%v, stat=%v)", injected.Load(), stats.LostHeadACKRecovered)
		}
		sameSnapshot(t, "dst after lost head ACK recovery", snapshot(t, reopen(t, dstMem, c)), snap2)
	})

	// (b) Concurrent destination GC deleting a pruned pack (and pruned shard) after PlanDelta
	// Stat/prune decision: verifyRestoredTree re-fetches the missing object from src,
	// cryptographically verifies all chunks, and completes the restore cleanly.
	t.Run("concurrent_dest_gc_pruned_pack_recovered", func(t *testing.T) {
		srcMem := storetest.NewMem()
		dstMem := storetest.NewMem()
		if err := vfs.Create(ctx, cfg(srcMem, c, 0)); err != nil {
			t.Fatal(err)
		}
		srcVol, err := vfs.Attach(ctx, cfg(srcMem, c, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer srcVol.Close()

		stateOne(t, srcVol)
		write(t, srcVol, vfs.RootInode, "pruned-pack.bin", content(6161, 1200_000))
		flush(t, srcVol)

		if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
			t.Fatal(err)
		}

		var prunedPackKey string
		for k := range dstMem.Dump() {
			if strings.HasPrefix(k, "pack/") {
				prunedPackKey = k
				break
			}
		}
		if prunedPackKey == "" {
			t.Fatal("expected resident pack on dst")
		}

		stateTwo(t, srcVol)
		write(t, srcVol, vfs.RootInode, "new-pack.bin", content(6262, 1200_000))
		flush(t, srcVol)
		snap2 := snapshot(t, reopen(t, srcMem, c))

		// When RestoreDelta writes the new pack to dst (after PlanDelta has already pruned
		// prunedPackKey), simulate concurrent destination GC deleting prunedPackKey.
		var gcFired atomic.Bool
		gcHook := &storetest.Hook{
			Backend: dstMem,
			After: func(op, key string) error {
				if op == "putabsent" && strings.HasPrefix(key, "pack/") && key != prunedPackKey && gcFired.CompareAndSwap(false, true) {
					return dstMem.Delete(ctx, prunedPackKey)
				}
				return nil
			},
		}

		stats, err := vfs.RestoreDelta(ctx, srcMem, gcHook, c, vfs.RestoreOptions{Round: 2, Streams: 4})
		if err != nil {
			t.Fatalf("RestoreDelta after concurrent destination GC of pruned pack: %v", err)
		}
		if !gcFired.Load() || stats.DestGCRecoveredPacks != 1 {
			t.Fatalf("expected 1 recovered destination GC pack (gcFired=%v, recovered=%d)", gcFired.Load(), stats.DestGCRecoveredPacks)
		}
		sameSnapshot(t, "dst after destination GC recovery", snapshot(t, reopen(t, dstMem, c)), snap2)
	})

	// (c) Concurrent source GC deleting a planned pack on src before transfer:
	// returns ErrSourceCheckpointGCed (wrapping backend.ErrNotFound) and leaves dst at checkpoint 1.
	t.Run("concurrent_source_gc_returns_ErrSourceCheckpointGCed", func(t *testing.T) {
		srcMem := storetest.NewMem()
		dstMem := storetest.NewMem()
		if err := vfs.Create(ctx, cfg(srcMem, c, 0)); err != nil {
			t.Fatal(err)
		}
		srcVol, err := vfs.Attach(ctx, cfg(srcMem, c, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer srcVol.Close()

		stateOne(t, srcVol)
		flush(t, srcVol)
		snap1 := snapshot(t, reopen(t, srcMem, c))

		if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
			t.Fatal(err)
		}

		stateTwo(t, srcVol)
		write(t, srcVol, vfs.RootInode, "src-gc-pack.bin", content(9191, 1200_000))
		flush(t, srcVol)

		plan, err := vfs.PlanDelta(ctx, srcMem, dstMem, c)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.PacksToCopy) == 0 {
			t.Fatal("expected planned pack on source")
		}
		// Delete planned pack from source to simulate concurrent source checkpoint GC.
		if err := srcMem.Delete(ctx, plan.PacksToCopy[0]); err != nil {
			t.Fatal(err)
		}

		_, err = vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 2, Streams: 4})
		if !errors.Is(err, vfs.ErrSourceCheckpointGCed) || !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("RestoreDelta after source GC = %v, want ErrSourceCheckpointGCed & backend.ErrNotFound", err)
		}
		sameSnapshot(t, "dst untouched after source GC race", snapshot(t, reopen(t, dstMem, c)), snap1)
	})

	// (d) Optional Cipher-sealed integrity receipt (SaveVerifyReceipt / TrustSealedReceipt).
	t.Run("sealed_verify_receipt_opt_in", func(t *testing.T) {
		srcMem := storetest.NewMem()
		dstMem := storetest.NewMem()
		if err := vfs.Create(ctx, cfg(srcMem, c, 0)); err != nil {
			t.Fatal(err)
		}
		srcVol, err := vfs.Attach(ctx, cfg(srcMem, c, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer srcVol.Close()

		stateOne(t, srcVol)
		write(t, srcVol, vfs.RootInode, "receipt-pack.bin", content(1234, 1200_000))
		flush(t, srcVol)

		st1, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{
			Round:             1,
			Streams:           4,
			SaveVerifyReceipt: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if st1.ChunksVerified == 0 || st1.ReceiptsVerified != 0 {
			t.Fatalf("unexpected initial receipt stats: chunks=%d receipts=%d", st1.ChunksVerified, st1.ReceiptsVerified)
		}

		// Subsequent delta restore with TrustSealedReceipt=true reuses the sealed receipt
		// when pack version & size match, while default TrustSealedReceipt=false re-verifies chunks.
		stateTwo(t, srcVol)
		flush(t, srcVol)
		st2, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{
			Round:              2,
			Streams:            4,
			TrustSealedReceipt: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if st2.ReceiptsVerified == 0 {
			t.Fatalf("expected ReceiptsVerified > 0 with TrustSealedReceipt=true, got %d", st2.ReceiptsVerified)
		}
	})
}

func TestParaSyncMatchedBaselineAndRealS3(t *testing.T) {
	runCase := func(t *testing.T, name string, mkBE func(prefix string) backend.Backend, reqCount func() int) {
		t.Run(name, func(t *testing.T) {
			c := vfsAge(t)

			// 1. Inline-heavy workload: many <= 16 KiB files, localized 5% delta.
			t.Run("inline", func(t *testing.T) {
				srcBE := mkBE("inline-src")
				dstFullBE := mkBE("inline-full")
				dstDeltaBE := mkBE("inline-delta")

				if err := vfs.Create(ctx, cfg(srcBE, c, 0)); err != nil {
					t.Fatal(err)
				}
				srcVol, err := vfs.Attach(ctx, cfg(srcBE, c, 1))
				if err != nil {
					t.Fatal(err)
				}
				defer srcVol.Close()

				dirs := sized(8, 12)
				filesPerDir := sized(50, 80)
				var dirIDs []vfs.InodeID
				for d := range dirs {
					did := mkdir(t, srcVol, vfs.RootInode, fmt.Sprintf("d%02d", d))
					dirIDs = append(dirIDs, did)
					for f := range filesPerDir {
						write(t, srcVol, did, fmt.Sprintf("f%03d", f), content(uint64(d*1000+f), 4096))
					}
				}
				flush(t, srcVol)

				// Baseline checkpoint 1 onto dstDeltaBE.
				if _, err := vfs.RestoreDelta(ctx, srcBE, dstDeltaBE, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
					t.Fatal(err)
				}

				// Mutate ~5% of inline files in one directory (d00).
				editCount := max(filesPerDir/10, 4)
				for f := range editCount {
					in, err := srcVol.Lookup(ctx, dirIDs[0], fmt.Sprintf("f%03d", f))
					if err != nil {
						t.Fatal(err)
					}
					fh, err := srcVol.Open(ctx, in.ID)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := fh.WriteAt(ctx, content(uint64(9000+f), 4096), 0); err != nil {
						t.Fatal(err)
					}
					_ = fh.Close()
				}
				flush(t, srcVol)
				wantSnap := snapshot(t, reopen(t, srcBE, c))

				rBeforeFull := reqCount()
				fullStats, err := vfs.RestoreDelta(ctx, srcBE, dstFullBE, c, vfs.RestoreOptions{Round: 1, Streams: 4})
				if err != nil {
					t.Fatalf("full restore: %v", err)
				}
				fullReqs := reqCount() - rBeforeFull

				rBeforeDelta := reqCount()
				deltaStats, err := vfs.RestoreDelta(ctx, srcBE, dstDeltaBE, c, vfs.RestoreOptions{Round: 2, Streams: 4})
				if err != nil {
					t.Fatalf("delta restore: %v", err)
				}
				deltaReqs := reqCount() - rBeforeDelta

				sameSnapshot(t, "inline full", snapshot(t, reopen(t, dstFullBE, c)), wantSnap)
				sameSnapshot(t, "inline delta", snapshot(t, reopen(t, dstDeltaBE, c)), wantSnap)

				t.Logf("inline matched Streams=4: full(dur=%v cpu=%v bytes=%d verifyBytes=%d verifyGets=%d chunksVer=%d gets=%d reqs=%d) vs delta(dur=%v cpu=%v bytes=%d verifyBytes=%d verifyGets=%d chunksVer=%d gets=%d pruned=%d/%d reqs=%d)",
					fullStats.Duration, fullStats.CPUTime, fullStats.BytesTransferred, fullStats.VerifyBytesRead, fullStats.VerifyGets, fullStats.ChunksVerified, fullStats.SourceGets, fullReqs,
					deltaStats.Duration, deltaStats.CPUTime, deltaStats.BytesTransferred, deltaStats.VerifyBytesRead, deltaStats.VerifyGets, deltaStats.ChunksVerified, deltaStats.SourceGets,
					deltaStats.Plan.ShardsPruned, deltaStats.Plan.TotalSourceShards, deltaReqs)

				if deltaStats.BytesTransferred >= fullStats.BytesTransferred {
					t.Fatalf("expected inline delta bytes (%d) < full (%d)", deltaStats.BytesTransferred, fullStats.BytesTransferred)
				}
				if deltaStats.SourceGets >= fullStats.SourceGets {
					t.Fatalf("expected inline delta source GETs (%d) < full (%d)", deltaStats.SourceGets, fullStats.SourceGets)
				}
			})

			// 2. Packed-chunk workload: multiple flush generations producing separate packs.
			t.Run("packed", func(t *testing.T) {
				srcBE := mkBE("pack-src")
				dstFullBE := mkBE("pack-full")
				dstDeltaBE := mkBE("pack-delta")

				if err := vfs.Create(ctx, cfg(srcBE, c, 0)); err != nil {
					t.Fatal(err)
				}
				srcVol, err := vfs.Attach(ctx, cfg(srcBE, c, 1))
				if err != nil {
					t.Fatal(err)
				}
				defer srcVol.Close()

				nPacks := sized(3, 5)
				blobSize := sized(1<<20, 2<<20)
				for p := range nPacks {
					write(t, srcVol, vfs.RootInode, fmt.Sprintf("packfile-%02d.bin", p), content(uint64(500+p), blobSize))
					flush(t, srcVol)
				}

				if _, err := vfs.RestoreDelta(ctx, srcBE, dstDeltaBE, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
					t.Fatal(err)
				}

				// Add one new packed file in checkpoint 2.
				write(t, srcVol, vfs.RootInode, "packfile-delta.bin", content(9999, blobSize))
				flush(t, srcVol)
				wantSnap := snapshot(t, reopen(t, srcBE, c))

				rBeforeFull := reqCount()
				fullStats, err := vfs.RestoreDelta(ctx, srcBE, dstFullBE, c, vfs.RestoreOptions{Round: 1, Streams: 4})
				if err != nil {
					t.Fatalf("full restore: %v", err)
				}
				fullReqs := reqCount() - rBeforeFull

				rBeforeDelta := reqCount()
				deltaStats, err := vfs.RestoreDelta(ctx, srcBE, dstDeltaBE, c, vfs.RestoreOptions{Round: 2, Streams: 4})
				if err != nil {
					t.Fatalf("delta restore: %v", err)
				}
				deltaReqs := reqCount() - rBeforeDelta

				sameSnapshot(t, "packed full", snapshot(t, reopen(t, dstFullBE, c)), wantSnap)
				sameSnapshot(t, "packed delta", snapshot(t, reopen(t, dstDeltaBE, c)), wantSnap)

				t.Logf("packed matched Streams=4: full(dur=%v cpu=%v bytes=%d verifyBytes=%d verifyGets=%d chunksVer=%d packs=%d gets=%d reqs=%d) vs delta(dur=%v cpu=%v bytes=%d verifyBytes=%d verifyGets=%d chunksVer=%d packs=%d prunedPacks=%d/%d gets=%d reqs=%d)",
					fullStats.Duration, fullStats.CPUTime, fullStats.BytesTransferred, fullStats.VerifyBytesRead, fullStats.VerifyGets, fullStats.ChunksVerified, fullStats.PacksCopied, fullStats.SourceGets, fullReqs,
					deltaStats.Duration, deltaStats.CPUTime, deltaStats.BytesTransferred, deltaStats.VerifyBytesRead, deltaStats.VerifyGets, deltaStats.ChunksVerified, deltaStats.PacksCopied,
					deltaStats.Plan.PacksPruned, deltaStats.Plan.TotalSourcePacks, deltaStats.SourceGets, deltaReqs)

				if deltaStats.PacksCopied != 1 || deltaStats.Plan.PacksPruned != nPacks {
					t.Fatalf("expected 1 pack copied and %d pruned, got copied=%d pruned=%d",
						nPacks, deltaStats.PacksCopied, deltaStats.Plan.PacksPruned)
				}
				if deltaStats.BytesTransferred >= fullStats.BytesTransferred {
					t.Fatalf("expected packed delta bytes (%d) < full (%d)", deltaStats.BytesTransferred, fullStats.BytesTransferred)
				}
				if deltaStats.SourceGets >= fullStats.SourceGets {
					t.Fatalf("expected packed delta source GETs (%d) < full (%d)", deltaStats.SourceGets, fullStats.SourceGets)
				}
			})
		})
	}

	srv := storetest.NewS3(t, "disk")
	runCase(t, "storetest-s3", func(prefix string) backend.Backend {
		be, err := s3.New(s3.Config{
			Endpoint:  srv.Endpoint(),
			Insecure:  true,
			Region:    "us-east-1",
			Bucket:    "disk",
			Prefix:    prefix,
			AccessKey: "k",
			SecretKey: "s",
		})
		if err != nil {
			t.Fatal(err)
		}
		return be
	}, srv.Requests)

	if storetest.HaveReal() {
		realSrv := storetest.RealS3(t)
		runCase(t, "hanzo-s3", func(prefix string) backend.Backend {
			be, err := s3.New(s3.Config{
				Endpoint:  realSrv.Endpoint,
				Insecure:  true,
				Region:    "us-east-1",
				Bucket:    realSrv.Bucket,
				Prefix:    realSrv.Prefix(prefix),
				AccessKey: realSrv.AccessKey,
				SecretKey: realSrv.SecretKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			return be
		}, func() int { return 0 })
	}
}
