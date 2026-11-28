// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

type headGuardBackend struct {
	backend.Backend
	headWrites atomic.Int64
	afterPut   func(op, key string) error
}

func (b *headGuardBackend) Put(ctx context.Context, key string, data []byte) error {
	if key == "head" {
		b.headWrites.Add(1)
	}
	if err := b.Backend.Put(ctx, key, data); err != nil {
		return err
	}
	if b.afterPut != nil {
		return b.afterPut("put", key)
	}
	return nil
}

func (b *headGuardBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if key == "head" {
		b.headWrites.Add(1)
	}
	v, err := b.Backend.PutIfAbsent(ctx, key, data)
	if err != nil {
		return "", err
	}
	if b.afterPut != nil {
		return v, b.afterPut("putabsent", key)
	}
	return v, nil
}

func (b *headGuardBackend) PutIfVersion(ctx context.Context, key string, data []byte, expected backend.Version) (backend.Version, error) {
	if key == "head" {
		b.headWrites.Add(1)
	}
	v, err := b.Backend.PutIfVersion(ctx, key, data, expected)
	if err != nil {
		return "", err
	}
	if b.afterPut != nil {
		return v, b.afterPut("putver", key)
	}
	return v, nil
}

func (b *headGuardBackend) String() string {
	return b.Backend.String()
}

type namedMemBackend struct {
	*storetest.Mem
	place string
}

func (b *namedMemBackend) String() string {
	return b.place
}

type staleVersionGetBackend struct {
	backend.Backend
	pinnedKey string
	pinnedVer backend.Version
}

func (b *staleVersionGetBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	data, ver, err := b.Backend.Get(ctx, key, max)
	if err == nil && key == b.pinnedKey {
		ver = b.pinnedVer
	}
	return data, ver, err
}

func (b *staleVersionGetBackend) String() string {
	return b.Backend.String()
}

func isCryptoReject(err error) bool {
	return errors.Is(err, vfs.ErrCorrupt) || errors.Is(err, vfs.ErrUnknownKey)
}

func flipByteSameLength(t *testing.T, be *storetest.Mem, key string, offset int, mask byte) []byte {
	t.Helper()
	raw, _, err := be.Get(ctx, key, backend.Unbounded)
	if err != nil {
		t.Fatalf("get %s for fault injection: %v", key, err)
	}
	if offset < 0 || offset >= len(raw) {
		t.Fatalf("fault offset %d out of bounds for %s (%d bytes)", offset, key, len(raw))
	}
	corrupted := bytes.Clone(raw)
	corrupted[offset] ^= mask
	if len(corrupted) != len(raw) {
		t.Fatalf("corrupted length %d != original %d", len(corrupted), len(raw))
	}
	if err := be.Put(ctx, key, corrupted); err != nil {
		t.Fatalf("inject corrupt %s: %v", key, err)
	}
	return raw
}

func assertHeadUnchanged(t *testing.T, dst backend.Backend, headBefore []byte, verBefore backend.Version) {
	t.Helper()
	headAfter, verAfter, err := dst.Get(ctx, "head", backend.Unbounded)
	if err != nil {
		t.Fatalf("read destination head after rejected restore: %v", err)
	}
	if verAfter != verBefore || !bytes.Equal(headAfter, headBefore) {
		t.Fatalf("destination head mutated despite corrupt pack (ver %q -> %q)", verBefore, verAfter)
	}
}

func assertSnapshotUnchanged(t *testing.T, dst backend.Backend, c vfs.Cipher, snapBefore map[string]string) {
	t.Helper()
	ro := reopen(t, dst, c)
	defer ro.Close()
	sameSnapshot(t, "destination untouched after fault injection", snapshot(t, ro), snapBefore)
}

func sortedPackKeys(be *storetest.Mem) []string {
	var keys []string
	for k := range be.Dump() {
		if strings.HasPrefix(k, "pack/") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// TestParaSyncFaultInjectionInFlightCopiedPackSameLength flips a byte at the
// start, midpoint, and trailing AEAD tag of a newly uploaded destination pack
// while keeping its exact byte length and asserts RestoreDelta fails closed
// before writing or advancing Head.
func TestParaSyncFaultInjectionInFlightCopiedPackSameLength(t *testing.T) {
	positions := []struct {
		name      string
		offset    func(n int) int
		wantExact error
	}{
		// Flipping byte 0 alters the unauthenticated key-ID framing length prefix
		// before AEAD/age decryption, yielding ErrUnknownKey; middle/tail flips
		// hit age ciphertext/Poly1305 tag verification and yield ErrCorrupt.
		{name: "first_byte_framing", offset: func(_ int) int { return 0 }, wantExact: vfs.ErrUnknownKey},
		{name: "middle_byte_ciphertext", offset: func(n int) int { return n / 2 }, wantExact: vfs.ErrCorrupt},
		{name: "last_byte_tag", offset: func(n int) int { return n - 1 }, wantExact: vfs.ErrCorrupt},
	}

	for _, tc := range positions {
		t.Run(tc.name, func(t *testing.T) {
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

			write(t, srcVol, vfs.RootInode, "base.bin", content(9001, 256<<10))
			flush(t, srcVol)
			if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
				t.Fatalf("round 1 restore: %v", err)
			}
			snapBefore := snapshot(t, reopen(t, dstMem, c))
			headBefore, verBefore, err := dstMem.Get(ctx, "head", backend.Unbounded)
			if err != nil {
				t.Fatal(err)
			}

			// Commit a fresh packed file that must be copied on Round 2.
			write(t, srcVol, vfs.RootInode, "delta-inflight.bin", content(9002, 1200_000))
			flush(t, srcVol)

			plan, err := vfs.PlanDelta(ctx, srcMem, dstMem, c)
			if err != nil || len(plan.PacksToCopy) == 0 {
				t.Fatalf("PlanDelta packs=%v err=%v", plan, err)
			}
			targetPack := plan.PacksToCopy[len(plan.PacksToCopy)-1]

			var injected atomic.Bool
			faultDst := &headGuardBackend{
				Backend: dstMem,
				afterPut: func(op, key string) error {
					if op == "putabsent" && key == targetPack && injected.CompareAndSwap(false, true) {
						raw, _, gerr := dstMem.Get(ctx, key, backend.Unbounded)
						if gerr != nil {
							return gerr
						}
						flipByteSameLength(t, dstMem, key, tc.offset(len(raw)), 0x80)
					}
					return nil
				},
			}

			if _, err := vfs.RestoreDelta(ctx, srcMem, faultDst, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !isCryptoReject(err) || !errors.Is(err, tc.wantExact) {
				t.Fatalf("RestoreDelta fault at %s = %v, want %v", tc.name, err, tc.wantExact)
			}
			if !injected.Load() {
				t.Fatal("expected in-flight pack fault injection to fire")
			}
			if got := faultDst.headWrites.Load(); got != 0 {
				t.Fatalf("expected 0 writes to destination head on corrupt pack, got %d", got)
			}
			assertHeadUnchanged(t, dstMem, headBefore, verBefore)
			assertSnapshotUnchanged(t, dstMem, c, snapBefore)
		})
	}
}

// TestParaSyncFaultInjectionMultiChunkPackEveryFrame flips one byte across
// every span of a multi-chunk pack on destination while preserving exact file
// length and verifies PlanDelta Stat pruning cannot bypass cryptographic
// verification prior to Head publication.
func TestParaSyncFaultInjectionMultiChunkPackEveryFrame(t *testing.T) {
	srcMem := storetest.NewMem()
	baseDst := &namedMemBackend{Mem: storetest.NewMem(), place: "mem://parasync-multichunk-dst"}
	c := vfsAge(t)

	if err := vfs.Create(ctx, cfg(srcMem, c, 0)); err != nil {
		t.Fatal(err)
	}
	srcVol, err := vfs.Attach(ctx, cfg(srcMem, c, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer srcVol.Close()

	// Multiple incompressible files ensure several distinct FastCDC chunks in packs.
	for i := range 4 {
		write(t, srcVol, vfs.RootInode, fmt.Sprintf("chunked-%d.bin", i), content(uint64(9100+i), 400<<10))
	}
	flush(t, srcVol)

	if _, err := vfs.RestoreDelta(ctx, srcMem, baseDst, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
		t.Fatalf("round 1 restore: %v", err)
	}
	snapBefore := snapshot(t, reopen(t, baseDst, c))
	packKeys := sortedPackKeys(baseDst.Mem)
	if len(packKeys) == 0 {
		t.Fatal("expected packed objects in destination store")
	}
	targetPack := packKeys[0]
	targetData, _, err := baseDst.Get(ctx, targetPack, backend.Unbounded)
	if err != nil || len(targetData) < 8 {
		t.Fatalf("read baseline pack %s: len=%d err=%v", targetPack, len(targetData), err)
	}

	// Advance checkpoint on src while retaining all chunked-*.bin objects.
	write(t, srcVol, vfs.RootInode, "checkpoint-2.txt", []byte("trigger round 2"))
	flush(t, srcVol)

	// Sweep byte flips across 8 evenly spaced offsets spanning the entire pack.
	const probes = 8
	for i := range probes {
		off := (i * len(targetData)) / probes
		if i == probes-1 {
			off = len(targetData) - 1
		}
		t.Run(fmt.Sprintf("offset_%d_of_%d", off, len(targetData)), func(t *testing.T) {
			dstMem := storetest.NewMem()
			for k, v := range baseDst.Mem.Dump() {
				if err := dstMem.Put(ctx, k, v); err != nil {
					t.Fatal(err)
				}
			}
			dstBE := &namedMemBackend{Mem: dstMem, place: baseDst.place}
			headBefore, verBefore, err := dstBE.Get(ctx, "head", backend.Unbounded)
			if err != nil {
				t.Fatal(err)
			}

			cleanPack := flipByteSameLength(t, dstMem, targetPack, off, 0x01)
			guardDst := &headGuardBackend{Backend: dstBE}

			plan, err := vfs.PlanDelta(ctx, srcMem, guardDst, c)
			if err != nil {
				t.Fatalf("PlanDelta with same-length bit flip: %v", err)
			}
			if plan.PacksPruned == 0 {
				t.Fatal("expected unchanged-length pack to be pruned during planning")
			}

			if _, err := vfs.RestoreDelta(ctx, srcMem, guardDst, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !isCryptoReject(err) {
				t.Fatalf("RestoreDelta at offset %d = %v, want ErrCorrupt or ErrUnknownKey", off, err)
			}
			if got := guardDst.headWrites.Load(); got != 0 {
				t.Fatalf("expected 0 writes to destination head, got %d", got)
			}
			assertHeadUnchanged(t, dstBE, headBefore, verBefore)
			if err := dstMem.Put(ctx, targetPack, cleanPack); err != nil {
				t.Fatal(err)
			}
			assertSnapshotUnchanged(t, dstBE, c, snapBefore)
		})
	}
}

// TestParaSyncFaultInjectionGCRecoveredPackSameLength simulates concurrent
// destination GC deleting a pruned pack followed by same-length corruption in
// the GC recovery upload and verifies Head stays untouched.
func TestParaSyncFaultInjectionGCRecoveredPackSameLength(t *testing.T) {
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

	write(t, srcVol, vfs.RootInode, "pruned.bin", content(9201, 1200_000))
	flush(t, srcVol)
	if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{Round: 1, Streams: 4}); err != nil {
		t.Fatal(err)
	}
	packKeys := sortedPackKeys(dstMem)
	if len(packKeys) != 1 {
		t.Fatalf("expected 1 resident pack after round 1, got %v", packKeys)
	}
	prunedPackKey := packKeys[0]
	cleanPrunedPack, _, err := dstMem.Get(ctx, prunedPackKey, backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	headBefore, verBefore, err := dstMem.Get(ctx, "head", backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	snapBefore := snapshot(t, reopen(t, dstMem, c))

	write(t, srcVol, vfs.RootInode, "new.bin", content(9202, 1200_000))
	flush(t, srcVol)

	var deletedForGC atomic.Bool
	var corruptedRecovery atomic.Bool
	faultDst := &headGuardBackend{
		Backend: dstMem,
		afterPut: func(op, key string) error {
			if op != "putabsent" {
				return nil
			}
			if key != prunedPackKey && strings.HasPrefix(key, "pack/") && deletedForGC.CompareAndSwap(false, true) {
				return dstMem.Delete(ctx, prunedPackKey)
			}
			if key == prunedPackKey && deletedForGC.Load() && corruptedRecovery.CompareAndSwap(false, true) {
				raw, _, gerr := dstMem.Get(ctx, prunedPackKey, backend.Unbounded)
				if gerr != nil {
					return gerr
				}
				flipByteSameLength(t, dstMem, prunedPackKey, len(raw)/3, 0x5a)
			}
			return nil
		},
	}

	if _, err := vfs.RestoreDelta(ctx, srcMem, faultDst, c, vfs.RestoreOptions{Round: 2, Streams: 4}); !isCryptoReject(err) {
		t.Fatalf("RestoreDelta with corrupt GC-recovered pack = %v, want ErrCorrupt or ErrUnknownKey", err)
	}
	if !deletedForGC.Load() || !corruptedRecovery.Load() {
		t.Fatalf("expected GC deletion (%v) and recovery corruption (%v)", deletedForGC.Load(), corruptedRecovery.Load())
	}
	if got := faultDst.headWrites.Load(); got != 0 {
		t.Fatalf("expected 0 head writes, got %d", got)
	}
	assertHeadUnchanged(t, dstMem, headBefore, verBefore)
	if err := dstMem.Put(ctx, prunedPackKey, cleanPrunedPack); err != nil {
		t.Fatal(err)
	}
	assertSnapshotUnchanged(t, dstMem, c, snapBefore)
}

// TestParaSyncFaultInjectionSealedReceiptBitRotAndTampering verifies that
// sealed ext/parasync/... verification receipts bind the pack digest so silent
// same-length bit-rot (even with unchanged backend Version) and tampered receipt
// objects both fail closed with ErrCorrupt before advancing Head.
func TestParaSyncFaultInjectionSealedReceiptBitRotAndTampering(t *testing.T) {
	setupWithReceipt := func(t *testing.T) (*storetest.Mem, *storetest.Mem, vfs.Cipher, *vfs.Volume, string, backend.Version, []byte, backend.Version, map[string]string) {
		t.Helper()
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

		write(t, srcVol, vfs.RootInode, "receipt-pack.bin", content(9301, 1200_000))
		flush(t, srcVol)

		if _, err := vfs.RestoreDelta(ctx, srcMem, dstMem, c, vfs.RestoreOptions{
			Round:             1,
			Streams:           4,
			SaveVerifyReceipt: true,
		}); err != nil {
			srcVol.Close()
			t.Fatal(err)
		}
		packs := sortedPackKeys(dstMem)
		if len(packs) != 1 {
			srcVol.Close()
			t.Fatalf("expected 1 pack, got %v", packs)
		}
		_, packVer, err := dstMem.Get(ctx, packs[0], backend.Unbounded)
		if err != nil {
			srcVol.Close()
			t.Fatal(err)
		}
		headBefore, verBefore, err := dstMem.Get(ctx, "head", backend.Unbounded)
		if err != nil {
			srcVol.Close()
			t.Fatal(err)
		}
		snapBefore := snapshot(t, reopen(t, dstMem, c))

		write(t, srcVol, vfs.RootInode, "bump.txt", []byte("round 2"))
		flush(t, srcVol)
		return srcMem, dstMem, c, srcVol, packs[0], packVer, headBefore, verBefore, snapBefore
	}

	t.Run("silent_bit_rot_without_version_bump", func(t *testing.T) {
		srcMem, dstMem, c, srcVol, packKey, preRotVer, headBefore, verBefore, snapBefore := setupWithReceipt(t)
		defer srcVol.Close()

		raw, _, err := dstMem.Get(ctx, packKey, backend.Unbounded)
		if err != nil {
			t.Fatal(err)
		}
		cleanPack := flipByteSameLength(t, dstMem, packKey, len(raw)/2, 0x3c)

		staleVerDst := &headGuardBackend{
			Backend: &staleVersionGetBackend{
				Backend:   dstMem,
				pinnedKey: packKey,
				pinnedVer: preRotVer,
			},
		}

		if _, err := vfs.RestoreDelta(ctx, srcMem, staleVerDst, c, vfs.RestoreOptions{
			Round:              2,
			Streams:            4,
			TrustSealedReceipt: true,
		}); !errors.Is(err, vfs.ErrCorrupt) {
			t.Fatalf("RestoreDelta with silent same-length bit-rot and TrustSealedReceipt = %v, want ErrCorrupt", err)
		}
		if got := staleVerDst.headWrites.Load(); got != 0 {
			t.Fatalf("expected 0 head writes, got %d", got)
		}
		assertHeadUnchanged(t, dstMem, headBefore, verBefore)
		if err := dstMem.Put(ctx, packKey, cleanPack); err != nil {
			t.Fatal(err)
		}
		assertSnapshotUnchanged(t, dstMem, c, snapBefore)
	})

	t.Run("tampered_receipt_with_corrupt_pack", func(t *testing.T) {
		srcMem, dstMem, c, srcVol, packKey, _, headBefore, verBefore, snapBefore := setupWithReceipt(t)
		defer srcVol.Close()

		var receiptKey string
		for k := range dstMem.Dump() {
			if strings.HasPrefix(k, "ext/parasync/") {
				receiptKey = k
				break
			}
		}
		if receiptKey == "" {
			t.Fatal("expected sealed ext/parasync/ receipt object")
		}

		rawPack, _, err := dstMem.Get(ctx, packKey, backend.Unbounded)
		if err != nil {
			t.Fatal(err)
		}
		rawReceipt, _, err := dstMem.Get(ctx, receiptKey, backend.Unbounded)
		if err != nil {
			t.Fatal(err)
		}

		flipByteSameLength(t, dstMem, receiptKey, len(rawReceipt)/2, 0x7f)
		cleanPack := flipByteSameLength(t, dstMem, packKey, len(rawPack)/4, 0x19)

		guardDst := &headGuardBackend{Backend: dstMem}
		if _, err := vfs.RestoreDelta(ctx, srcMem, guardDst, c, vfs.RestoreOptions{
			Round:              2,
			Streams:            4,
			TrustSealedReceipt: true,
		}); !errors.Is(err, vfs.ErrCorrupt) {
			t.Fatalf("RestoreDelta with tampered receipt + corrupt pack = %v, want ErrCorrupt", err)
		}
		if got := guardDst.headWrites.Load(); got != 0 {
			t.Fatalf("expected 0 head writes, got %d", got)
		}
		assertHeadUnchanged(t, dstMem, headBefore, verBefore)
		if err := dstMem.Put(ctx, packKey, cleanPack); err != nil {
			t.Fatal(err)
		}
		assertSnapshotUnchanged(t, dstMem, c, snapBefore)
	})
}
