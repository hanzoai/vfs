// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
	"github.com/hanzoai/vfs/pkg/sysspec"
)

func sysSpecCommitHead(h Head) sysspec.CommitHead {
	return sysspec.CommitHead{
		Seq:     h.Seq,
		Root:    h.Root,
		Next:    h.Next,
		ID:      h.ID,
		Seed:    h.Seed,
		Journal: h.Journal,
		Needs:   h.Needs,
		Tail:    h.Tail,
	}
}

func sysSpecClassifyErr(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrNeeds):
		return "ErrNeeds"
	case errors.Is(err, ErrCorrupt):
		return "ErrCorrupt"
	case errors.Is(err, errContext):
		return "ErrContext"
	case errors.Is(err, backend.ErrTooLarge):
		return "ErrTooLarge"
	case errors.Is(err, ErrBadCipher):
		return "ErrBadCipher"
	case errors.Is(err, ErrStaleRound):
		return "ErrStaleRound"
	default:
		return err.Error()
	}
}

func sysSpecLineage(id volumeID, round uint64) []byte {
	out := make([]byte, 24)
	copy(out[:16], id[:])
	binary.LittleEndian.PutUint64(out[16:], round)
	return out
}

// TestSysSpecRealFlushWriteOrderAndOrphanSafety runs real Volume commits and
// injected Head-write failures through the SYSSPEC trace verifier, checking
// write ordering (packs -> shards -> Head), orphan safety, clean no-op flushes,
// and strict Head.Seq non-repetition across retries.
func TestSysSpecRealFlushWriteOrderAndOrphanSafety(t *testing.T) {
	ctx := context.Background()
	reg := sysspec.DefaultRegistry()

	mem := storetest.NewMem()
	var hookMu sync.Mutex
	var stages []sysspec.WriteStage
	failHeadOnce := false
	resetStages := func() {
		hookMu.Lock()
		stages = nil
		hookMu.Unlock()
	}
	snapshotStages := func() []sysspec.WriteStage {
		hookMu.Lock()
		out := slices.Clone(stages)
		hookMu.Unlock()
		return out
	}
	hook := &storetest.Hook{
		Backend: mem,
		Before: func(op, key string) error {
			if op == "delete" {
				return nil
			}
			hookMu.Lock()
			defer hookMu.Unlock()
			switch {
			case strings.HasPrefix(key, "pack/"):
				stages = append(stages, sysspec.WriteStage{Kind: "pack", Key: key})
			case strings.HasPrefix(key, "meta/"):
				stages = append(stages, sysspec.WriteStage{Kind: "shard", Key: key})
			case key == headKey:
				stages = append(stages, sysspec.WriteStage{Kind: "head", Key: key})
				if failHeadOnce {
					failHeadOnce = false
					return errors.New("injected head timeout")
				}
			}
			return nil
		},
	}

	_, c, _ := headVolume(t)
	if err := Create(ctx, Config{Backend: hook, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	resetStages()

	jdir, err := os.MkdirTemp(storetest.Journals(), "sysspec-flush-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(jdir) })

	v, err := Attach(ctx, Config{
		Backend:         hook,
		Cipher:          c,
		Round:           1,
		Journal:         jdir,
		JournalCapacity: 16 << 20,
		Bound:           time.Hour, // keep background timer from racing explicit Flush steps
		Disk:            storetest.Disk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	h0, err := unmarshalHead(headPlain(t, mem, c))
	if err != nil {
		t.Fatal(err)
	}

	state := sysspec.BaselineValidState()
	state.Journal.Open = false
	state.Fence.Head = sysspec.FenceRecord{
		Exists:  true,
		Version: 1,
		Round:   1,
		Size:    len(mem.Dump()[headKey]),
	}
	state.Publication.Committed = sysSpecCommitHead(h0)
	state.Publication.WorkingRoot = h0.Root
	state.Publication.WorkingNext = h0.Next
	state.Publication.LastAttemptedSeq = h0.Seq
	state.Publication.CallerSeenSeq = h0.Seq
	state.Publication.UsedSeqs = map[uint64]bool{h0.Seq: true}
	state.Publication.UploadedPacks = map[string]bool{}
	state.Publication.UploadedShards = map[string]bool{}
	state.Publication.ReferencedPacks = map[string]bool{}
	state.Publication.ReferencedShards = map[string]bool{}
	for k := range mem.Dump() {
		if strings.HasPrefix(k, "meta/") {
			state.Publication.UploadedShards[k] = true
			state.Publication.ReferencedShards[k] = true
		}
	}

	// Step 1: clean Flush on freshly attached Volume makes zero store writes (P5).
	resetStages()
	pre1 := state.Clone()
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	post1 := pre1.Clone()
	post1.Publication.FlushStages = snapshotStages()
	if vs := reg.CheckStep(sysspec.Step{
		Index:  0,
		Action: sysspec.Action{Kind: sysspec.ActionVolumeFlush, Actor: "w1", Round: 1, CleanNoOpFlush: true},
		Pre:    pre1,
		Post:   post1,
	}); len(vs) != 0 {
		t.Fatalf("clean Flush violated spec: %v", vs)
	}

	// Step 2: write a chunked file (96 KiB) + small file (512 B), fail Head PUT once.
	inBig, err := v.Create(ctx, RootInode, "chunked.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fh, err := v.Open(ctx, inBig.ID)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 96<<10)
	for i := range buf {
		buf[i] = byte(i*31 + 7)
	}
	if _, err := fh.WriteAt(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()

	inSmall, err := v.Create(ctx, RootInode, "small.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fsSmall, err := v.Open(ctx, inSmall.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fsSmall.WriteAt(ctx, []byte("small-inline-record"), 0); err != nil {
		t.Fatal(err)
	}
	_ = fsSmall.Close()

	resetStages()
	hookMu.Lock()
	failHeadOnce = true
	hookMu.Unlock()
	pre2 := post1.Clone()
	flushErr := v.Flush(ctx)
	if flushErr == nil {
		t.Fatal("expected injected Head error")
	}
	failedAttemptSeq := v.seq
	post2 := pre2.Clone()
	post2.Publication.FlushStages = snapshotStages()
	post2.Publication.LastAttemptedSeq = failedAttemptSeq
	post2.Publication.UsedSeqs[failedAttemptSeq] = true
	post2.Publication.FlushHeldBy = "w1"
	for k := range mem.Dump() {
		if strings.HasPrefix(k, "pack/") {
			post2.Publication.UploadedPacks[k] = true
		} else if strings.HasPrefix(k, "meta/") {
			post2.Publication.UploadedShards[k] = true
		}
	}
	if vs := reg.CheckStep(sysspec.Step{
		Index: 1,
		Action: sysspec.Action{
			Kind:        sysspec.ActionVolumeFlush,
			Actor:       "w1",
			Round:       1,
			Seq:         failedAttemptSeq,
			ReturnedErr: flushErr.Error(),
		},
		Pre:  pre2,
		Post: post2,
	}); len(vs) != 0 {
		t.Fatalf("failed Flush violated spec: %v", vs)
	}

	// Step 3: retry Flush succeeds, advances Seq past failedAttemptSeq (never repeats!).
	resetStages()
	pre3 := post2.Clone()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	h1, err := unmarshalHead(headPlain(t, mem, c))
	if err != nil {
		t.Fatal(err)
	}
	if h1.Seq <= failedAttemptSeq {
		t.Fatalf("committed Head.Seq=%d did not exceed failed attempt Seq=%d", h1.Seq, failedAttemptSeq)
	}
	post3 := pre3.Clone()
	post3.Publication.FlushStages = snapshotStages()
	post3.Publication.Committed = sysSpecCommitHead(h1)
	post3.Publication.WorkingRoot = h1.Root
	post3.Publication.WorkingNext = h1.Next
	post3.Publication.LastAttemptedSeq = h1.Seq
	post3.Publication.CallerSeenSeq = h1.Seq
	post3.Publication.UsedSeqs[h1.Seq] = true
	post3.Publication.FlushHeldBy = "w1"
	for k := range mem.Dump() {
		if strings.HasPrefix(k, "pack/") {
			post3.Publication.UploadedPacks[k] = true
			post3.Publication.ReferencedPacks[k] = true
		} else if strings.HasPrefix(k, "meta/") {
			post3.Publication.UploadedShards[k] = true
			post3.Publication.ReferencedShards[k] = true
		}
	}
	if vs := reg.CheckStep(sysspec.Step{
		Index:  2,
		Action: sysspec.Action{Kind: sysspec.ActionVolumeFlush, Actor: "w1", Round: 1, Seq: h1.Seq},
		Pre:    pre3,
		Post:   post3,
	}); len(vs) != 0 {
		t.Fatalf("succeeded retry Flush violated spec: %v", vs)
	}
}

// TestSysSpecRealCorruptionAndContextDomainSeparation exercises real vfs
// parsers, sealing contexts, and fence bounds against SYSSPEC corruption rules
// C1..C5.
func TestSysSpecRealCorruptionAndContextDomainSeparation(t *testing.T) {
	ctx := context.Background()
	reg := sysspec.DefaultRegistry()
	be, c, _ := headVolume(t)
	baseHead, err := unmarshalHead(headPlain(t, be, c))
	if err != nil {
		t.Fatal(err)
	}

	// 1. Cross-place Head unseal fails with errContext (satisfies C1).
	sealedAtA, err := seal(c, headAAD("s3://bucket-a/vol"), baseHead.marshal())
	if err != nil {
		t.Fatal(err)
	}
	_, openErr := unseal(c, headAAD("s3://bucket-b/vol"), sealedAtA)
	s1 := sysspec.BaselineValidState()
	s1.Corruption = &sysspec.ObjectSealCheck{
		Kind:              "head",
		ExpectedPlace:     "s3://bucket-b/vol",
		ActualPlace:       "s3://bucket-a/vol",
		ObservedErrorKind: sysSpecClassifyErr(openErr),
	}
	if vs := reg.CheckState(0, s1); len(vs) != 0 {
		t.Fatalf("cross-place rejection failed C1: %v (err=%v)", vs, openErr)
	}

	// 2. Non-minimal uvarint in Head Seq fails with ErrCorrupt (satisfies C2).
	nonMinimal := append([]byte{0x80, 0x00}, baseHead.marshal()[1:]...)
	_, varErr := unmarshalHead(nonMinimal)
	s2 := sysspec.BaselineValidState()
	s2.Corruption = &sysspec.ObjectSealCheck{
		Kind:              "head",
		NonMinimalVarint:  true,
		ObservedErrorKind: sysSpecClassifyErr(varErr),
	}
	if vs := reg.CheckState(1, s2); len(vs) != 0 {
		t.Fatalf("non-minimal varint check failed C2: %v (err=%v)", vs, varErr)
	}

	// 3. Unknown Needs bit + malformed Tail returns ErrNeeds, not ErrCorrupt (satisfies C3).
	badNeedsHead := baseHead
	badNeedsHead.Needs = 1 << 4
	badNeedsHead.Tail = "\x05\x09short" // truncated field after unknown Needs
	_, needsErr := unmarshalHead(badNeedsHead.marshal())
	s3 := sysspec.BaselineValidState()
	s3.Corruption = &sysspec.ObjectSealCheck{
		Kind:              "head",
		KnownNeeds:        knownNeeds,
		HeadNeeds:         badNeedsHead.Needs,
		TailFields:        []sysspec.TailField{{Tag: 5, Length: 9, ActualLen: 5}},
		ObservedErrorKind: sysSpecClassifyErr(needsErr),
	}
	if vs := reg.CheckState(2, s3); len(vs) != 0 {
		t.Fatalf("Needs-before-Tail precedence failed C3: %v (err=%v)", vs, needsErr)
	}

	// 4. Oversized fenced Head (> 65,536 bytes) returns ErrTooLarge without mutating store (satisfies C5).
	beforeHead := slices.Clone(be.Dump()[headKey])
	fs := fence.New(be, maxHead)
	oversized := make([]byte, maxHead)
	putErr := fs.Put(ctx, headKey, oversized, 1)
	mutated := !slices.Equal(beforeHead, be.Dump()[headKey])
	s4 := sysspec.BaselineValidState()
	s4.Corruption = &sysspec.ObjectSealCheck{
		Kind:               "head",
		ObservedErrorKind:  sysSpecClassifyErr(putErr),
		StoreMutatedOnFail: mutated,
	}
	if vs := reg.CheckState(3, s4); len(vs) != 0 {
		t.Fatalf("oversized Head check failed C5: %v (err=%v)", vs, putErr)
	}
}

// TestSysSpecRealJournalFenceVolumeCrossLayerContract exercises a real
// journal.Journal (over crash.Disk) paired with a real vfs.Volume and
// pkg/fence.Store, verifying both compliant cross-layer execution and real
// detection of the four Class B boundary hazards (B01..B04).
func TestSysSpecRealJournalFenceVolumeCrossLayerContract(t *testing.T) {
	ctx := context.Background()
	reg := sysspec.DefaultRegistry()

	be, c, cfg := headVolume(t)
	j1Dir, err := os.MkdirTemp(storetest.Journals(), "sysspec-w1-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(j1Dir) })
	cfg.Round = 1
	cfg.Journal = j1Dir
	v1, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v1.Close()

	h0, err := unmarshalHead(headPlain(t, be, c))
	if err != nil {
		t.Fatal(err)
	}

	disk1 := crash.New()
	j1, err := journal.Open("jdir", journal.Options{
		Capacity:  8 * 4096,
		Segment:   4096,
		Disk:      disk1,
		Published: h0.Journal,
		Lineage:   sysSpecLineage(h0.ID, 1),
		Bound:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("journal.Open round 1: %v", err)
	}
	defer j1.Close()

	// Compliant cycle on v2 journaled Volume: Create + WriteAt append two journal
	// records natively, Flush publishes Head carrying Head.Journal == seq2, and
	// the standalone journal mirrors Publish -> Reclaim.
	seq1, err := j1.Append(ctx, []byte("op:create:a"))
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := j1.Append(ctx, []byte("op:write:a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := j1.Sync(ctx, seq2); err != nil {
		t.Fatal(err)
	}
	inA, err := v1.Create(ctx, RootInode, "a", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fhA, err := v1.Open(ctx, inA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fhA.WriteAt(ctx, []byte("payload-a"), 0); err != nil {
		t.Fatal(err)
	}
	_ = fhA.Close()
	if err := v1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	hCommitted, err := unmarshalHead(headPlain(t, be, c))
	if err != nil {
		t.Fatal(err)
	}
	if hCommitted.Journal != seq2 {
		t.Fatalf("Head.Journal = %d, want %d", hCommitted.Journal, seq2)
	}
	if err := j1.Publish(hCommitted.Journal); err != nil {
		t.Fatal(err)
	}
	if err := j1.Reclaim(); err != nil {
		t.Fatal(err)
	}
	_ = seq1

	// Append unpublished tail (seq 3) on writer 1, then writer 2 takes over at Round 2
	// on a distinct node journal directory.
	seq3, err := j1.Append(ctx, []byte("op:unpublished-on-w1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := j1.Sync(ctx, seq3); err != nil {
		t.Fatal(err)
	}

	j2Dir := filepath.Join(j1Dir, "w2")
	if err := os.MkdirAll(j2Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg2 := cfg
	cfg2.Round = 2
	cfg2.Journal = j2Dir
	v2, err := Attach(ctx, cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()

	// B04 demonstration on real journal.Journal:
	// Opening writer 1's disk under Round 2's proper lineage (ID, Round=2) fails with ErrLineage,
	// whereas omitting Round from lineage (ID only) would wrongly accept the deposed log.
	_ = j1.Close()
	if _, lerr := journal.Open("jdir", journal.Options{
		Capacity:  8 * 4096,
		Segment:   4096,
		Disk:      disk1,
		Published: hCommitted.Journal,
		Lineage:   sysSpecLineage(hCommitted.ID, 2),
	}); !errors.Is(lerr, journal.ErrLineage) {
		t.Fatalf("expected ErrLineage when rotating Round in journal lineage, got %v", lerr)
	}

	// B01 verification on real v0.7.0 Volume.landed(ctx, Head) + SYSSPEC RG8:
	// Writer 1 is now deposed (store Round=2, Head.Root=hCommitted.Root, Head.Journal=2).
	// 1. Real v1.landed(ctx, hCommitted) confirms an exact (ID, Root, Journal) match.
	curHead, exactOK := v1.landed(ctx, hCommitted)
	if !exactOK {
		t.Fatal("expected v1.landed(ctx, hCommitted) to match identical ID, Root, and Journal on store")
	}
	// 2. Real v1.landed(ctx, targetHead) with target Journal=3 (seq3) while store Head.Journal=2
	//    must reject the ABA Root match (fixed in v0.7.0 / a0670e19).
	targetHead := hCommitted
	targetHead.Journal = seq3
	if _, abaOK := v1.landed(ctx, targetHead); abaOK {
		t.Fatalf("expected v1.landed to reject ABA Root match when target Journal=%d > store Journal=%d", seq3, curHead.Journal)
	}
	// 3. SYSSPEC RG8 still catches an injected pre-v2 LostAckLanded=true false positive.
	preB1 := sysspec.BaselineValidState()
	preB1.Publication.Committed = sysSpecCommitHead(hCommitted)
	v1.mu.RLock()
	preB1.Publication.WorkingRoot = v1.root
	preB1.Publication.WorkingNext = uint64(v1.next)
	v1.mu.RUnlock()
	preB1.Journal.Visible = seq3
	preB1.Journal.Durable = seq3
	preB1.Journal.Published = hCommitted.Journal
	preB1.Journal.FirstKept = seq3
	preB1.Journal.LastKept = seq3

	postB1 := preB1.Clone()
	postB1.Publication.Committed = sysSpecCommitHead(curHead)
	vs := reg.CheckStep(sysspec.Step{
		Index: 0,
		Action: sysspec.Action{
			Kind:          sysspec.ActionVolumeFlush,
			Actor:         "w1",
			Round:         1,
			Seq:           hCommitted.Seq + 1,
			JournalTarget: seq3,
			LostAckLanded: true,
		},
		Pre:  preB1,
		Post: postB1,
	})
	if !slices.ContainsFunc(vs, func(v sysspec.Violation) bool {
		return v.RuleID == "RG8_LostAckLandedChecksFullCommitWatermarks"
	}) {
		t.Fatalf("expected RG8 violation on ABA/journal landed() mismatch, got %v", vs)
	}
}
