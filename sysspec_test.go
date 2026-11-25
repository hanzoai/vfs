// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/journal"
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
	case errors.Is(err, ErrFull):
		return "ErrFull"
	default:
		return err.Error()
	}
}

func sysSpecTempJournalDir(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp(storetest.Journals(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func sysSpecReadFile(t *testing.T, v *Volume, ino InodeID) []byte {
	t.Helper()
	in, err := v.Stat(context.Background(), ino)
	if err != nil {
		t.Fatalf("Stat(%d): %v", ino, err)
	}
	f, err := v.Open(context.Background(), ino)
	if err != nil {
		t.Fatalf("Open(%d): %v", ino, err)
	}
	defer f.Close()
	buf := make([]byte, in.Size)
	if in.Size == 0 {
		return buf
	}
	n, err := f.ReadAt(context.Background(), buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt(%d): %v", ino, err)
	}
	return buf[:n]
}

func sysSpecWriteFile(t *testing.T, v *Volume, parent InodeID, name string, data []byte) Inode {
	t.Helper()
	ctx := context.Background()
	in, err := v.Create(ctx, parent, name, 0o644)
	if err != nil {
		t.Fatalf("Create(%s): %v", name, err)
	}
	fh, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatalf("Open(%s): %v", name, err)
	}
	if _, err := fh.WriteAt(ctx, data, 0); err != nil {
		_ = fh.Close()
		t.Fatalf("WriteAt(%s): %v", name, err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("Close(%s): %v", name, err)
	}
	return in
}

func sysSpecInitState(t *testing.T, mem *storetest.Mem, c Cipher, round uint64, jCap int64) sysspec.SystemState {
	t.Helper()
	h0, err := unmarshalHead(headPlain(t, mem, c))
	if err != nil {
		t.Fatal(err)
	}
	var tok [16]byte
	tok[0] = byte(round)
	actor := "w1"
	if round > 1 {
		actor = "w" + string(rune('0'+round))
	}
	st := sysspec.BaselineValidState()
	st.Journal = sysspec.JournalState{
		Open:             true,
		LineageVolumeID:  h0.ID,
		LineageRound:     round,
		Visible:          h0.Journal,
		Durable:          h0.Journal,
		Published:        h0.Journal,
		Proven:           h0.Journal,
		Floor:            h0.Journal,
		FirstKept:        0,
		LastKept:         h0.Journal,
		ActiveSyncEpoch:  1,
		CompletedSync:    1,
		RecordWriteEpoch: map[uint64]uint64{},
		CapacityBytes:    jCap,
		UsedBytes:        0,
		BulkDurableSeq:   1,
	}
	st.Fence = sysspec.FenceState{
		Head: sysspec.FenceRecord{
			Exists:  true,
			Version: 1,
			Round:   round,
			Token:   tok,
			Payload: slices.Clone(mem.Dump()[headKey]),
			Size:    len(mem.Dump()[headKey]),
		},
		MaxSize: maxHead,
		Writers: map[string]sysspec.FenceWriter{
			actor: {ID: actor, Round: round, Token: tok},
		},
	}
	st.Publication = sysspec.PublicationState{
		Committed:        sysSpecCommitHead(h0),
		WorkingRoot:      h0.Root,
		WorkingNext:      h0.Next,
		OpenPackEmpty:    true,
		LastAttemptedSeq: h0.Seq,
		CallerSeenSeq:    h0.Seq,
		UsedSeqs:         map[uint64]bool{h0.Seq: true},
		UploadedPacks:    map[string]bool{},
		UploadedShards:   map[string]bool{},
		ReferencedPacks:  map[string]bool{},
		ReferencedShards: map[string]bool{},
	}
	for k := range mem.Dump() {
		if strings.HasPrefix(k, "meta/") {
			st.Publication.UploadedShards[k] = true
			st.Publication.ReferencedShards[k] = true
		} else if strings.HasPrefix(k, "pack/") {
			st.Publication.UploadedPacks[k] = true
			st.Publication.ReferencedPacks[k] = true
		}
	}
	return st
}

// sysSpecClock is a deterministic journal.Clock for bound-driven publication traces.
type sysSpecClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*sysSpecTimer
}

type sysSpecTimer struct {
	c    *sysSpecClock
	at   time.Time
	f    func()
	done bool
}

func newSysSpecClock() *sysSpecClock {
	return &sysSpecClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *sysSpecClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *sysSpecClock) AfterFunc(d time.Duration, f func()) journal.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := &sysSpecTimer{c: c, at: c.now.Add(d), f: f}
	c.timers = append(c.timers, tm)
	return tm
}

func (tm *sysSpecTimer) Stop() bool {
	tm.c.mu.Lock()
	defer tm.c.mu.Unlock()
	was := !tm.done
	tm.done = true
	return was
}

func (c *sysSpecClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*sysSpecTimer
	for _, tm := range c.timers {
		if !tm.done && !tm.at.After(c.now) {
			tm.done = true
			due = append(due, tm)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].at.Before(due[j].at) })
	c.mu.Unlock()
	for _, tm := range due {
		go tm.f()
	}
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

	jdir := sysSpecTempJournalDir(t, "sysspec-flush-*")
	v, err := Attach(ctx, Config{
		Backend:         hook,
		Cipher:          c,
		Round:           1,
		Journal:         jdir,
		JournalCapacity: 16 << 20,
		Bound:           time.Hour,
		Disk:            storetest.Disk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	state := sysSpecInitState(t, mem, c, 1, 16<<20)
	state.Journal.Open = false

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
	buf := make([]byte, 96<<10)
	for i := range buf {
		buf[i] = byte(i*31 + 7)
	}
	sysSpecWriteFile(t, v, RootInode, "chunked.bin", buf)
	sysSpecWriteFile(t, v, RootInode, "small.txt", []byte("small-inline-record"))

	resetStages()
	hookMu.Lock()
	failHeadOnce = true
	hookMu.Unlock()
	pre2 := post1.Clone()
	flushErr := v.Flush(ctx)
	if flushErr == nil {
		t.Fatal("expected injected Head error")
	}
	v.mu.RLock()
	failedAttemptSeq := v.seq
	v.mu.RUnlock()
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
	badNeedsHead.Tail = "\x05\x09short"
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

// TestSysSpecRealJournalFenceVolumeCrossLayerContract exercises the real v2
// journaled Volume lifecycle (native ledger + journal.Journal + pkg/fence.Store),
// verifying lineage binding across rounds (RG4) and keeping old pre-v2 B01
// strictly as a historical negative fixture verifying both v0.7.0's landed()
// fix and RG8's detection of injected pre-v2 false positives.
func TestSysSpecRealJournalFenceVolumeCrossLayerContract(t *testing.T) {
	ctx := context.Background()
	reg := sysspec.DefaultRegistry()

	be, c, cfg := headVolume(t)
	j1Dir := sysSpecTempJournalDir(t, "sysspec-w1-*")
	cfg.Round = 1
	cfg.Journal = j1Dir
	cfg.Bound = time.Hour
	v1, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v1.Close()

	// Real v2 journaled Volume mutations: Create + WriteAt append to v1's native
	// journal, Sync makes them durable locally, and Flush commits Head.Journal
	// and advances v1.State().Published atomically after Head CAS.
	sysSpecWriteFile(t, v1, RootInode, "a", []byte("payload-a"))
	if err := v1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	stPreFlush := v1.State()
	if stPreFlush.Visible < 2 || stPreFlush.Durable != stPreFlush.Visible || stPreFlush.Published != 0 {
		t.Fatalf("unexpected pre-Flush v2 journal state: %+v", stPreFlush)
	}
	if err := v1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	hCommitted, err := unmarshalHead(headPlain(t, be, c))
	if err != nil {
		t.Fatal(err)
	}
	stPostFlush := v1.State()
	if hCommitted.Journal != stPreFlush.Visible || stPostFlush.Published != hCommitted.Journal {
		t.Fatalf("Head.Journal=%d State.Published=%d want %d", hCommitted.Journal, stPostFlush.Published, stPreFlush.Visible)
	}

	// Write an unpublished local tail on writer 1 and sync it locally.
	sysSpecWriteFile(t, v1, RootInode, "unpublished", []byte("only-on-w1"))
	if err := v1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	stTail := v1.State()
	if stTail.Visible <= hCommitted.Journal || stTail.Durable != stTail.Visible {
		t.Fatalf("expected unpublished durable tail on w1, got %+v (Head.Journal=%d)", stTail, hCommitted.Journal)
	}

	// Writer 2 takes over at Round 2 on a distinct node journal directory.
	j2Dir := sysSpecTempJournalDir(t, "sysspec-w2-*")
	cfg2 := cfg
	cfg2.Round = 2
	cfg2.Journal = j2Dir
	v2, err := Attach(ctx, cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()

	// Verify real RG4 lineage rotation on v1's actual on-disk journal directory:
	// opening Round 1's directory under Round 2's lineage fails closed with ErrLineage.
	r1Places := journalPlaces(j1Dir, hCommitted.ID, 1)
	_ = v1.Close()
	r1Disk, err := storetest.Disk(r1Places.dir)
	if err != nil {
		t.Fatalf("storetest.Disk(%s): %v", r1Places.dir, err)
	}
	if _, lerr := journal.Open(r1Places.dir, journal.Options{
		Capacity:  cfg.JournalCapacity,
		Segment:   1 << 20,
		Disk:      r1Disk,
		Published: hCommitted.Journal,
		Lineage:   lineage(hCommitted.ID, 2),
	}); !errors.Is(lerr, journal.ErrLineage) {
		t.Fatalf("expected ErrLineage opening Round 1 journal dir under Round 2 lineage, got %v", lerr)
	}

	// Historical pre-v2 B01 negative fixture:
	// 1. Current v2 Volume.landed(ctx, Head) matches exact (ID, Root, Journal).
	curHead, exactOK := v2.landed(ctx, hCommitted)
	if !exactOK {
		t.Fatal("expected v2.landed(ctx, hCommitted) to match identical ID, Root, and Journal on store")
	}
	// 2. Current v2 Volume.landed(ctx, targetHead) rejects ABA Root match when target Journal > store Journal.
	targetHead := hCommitted
	targetHead.Journal = stTail.Visible
	if _, abaOK := v2.landed(ctx, targetHead); abaOK {
		t.Fatalf("expected v2.landed to reject ABA Root match when target Journal=%d > store Journal=%d",
			stTail.Visible, curHead.Journal)
	}
	// 3. Historical negative fixture: an injected pre-v2 LostAckLanded=true step still triggers RG8.
	preB1 := sysspec.BaselineValidState()
	preB1.Publication.Committed = sysSpecCommitHead(hCommitted)
	preB1.Publication.WorkingRoot = hCommitted.Root
	preB1.Publication.WorkingNext = hCommitted.Next
	preB1.Journal.Visible = stTail.Visible
	preB1.Journal.Durable = stTail.Visible
	preB1.Journal.Published = hCommitted.Journal
	preB1.Journal.FirstKept = hCommitted.Journal + 1
	preB1.Journal.LastKept = stTail.Visible

	postB1 := preB1.Clone()
	postB1.Publication.Committed = sysSpecCommitHead(curHead)
	vs := reg.CheckStep(sysspec.Step{
		Index: 0,
		Action: sysspec.Action{
			Kind:          sysspec.ActionVolumeFlush,
			Actor:         "w1",
			Round:         1,
			Seq:           hCommitted.Seq + 1,
			JournalTarget: stTail.Visible,
			LostAckLanded: true,
		},
		Pre:  preB1,
		Post: postB1,
	})
	if !slices.ContainsFunc(vs, func(v sysspec.Violation) bool {
		return v.RuleID == "RG8_LostAckLandedChecksFullCommitWatermarks"
	}) {
		t.Fatalf("expected RG8 violation on historical B01 ABA/journal landed() fixture, got %v", vs)
	}
}

// TestSysSpecRealV2CrashReopenTrace drives a real VFS v2 Volume through
// local-journal writes + Sync (zero store requests), un-flushed process exit,
// a refused takeover attempt (leaving the journal intact), and a successful
// same-node reopen that replays and publishes the journal under the new Round,
// validating the full trace against SYSSPEC.
func TestSysSpecRealV2CrashReopenTrace(t *testing.T) {
	ctx := context.Background()
	reg := sysspec.DefaultRegistry()

	mem := storetest.NewMem()
	var storeWrites atomic.Int64
	var refuseHeadPut atomic.Bool
	hook := &storetest.Hook{
		Backend: mem,
		Before: func(op, key string) error {
			if op == "put" || op == "putver" || op == "putabs" {
				storeWrites.Add(1)
			}
			if refuseHeadPut.Load() && key == headKey && op == "putver" {
				return errors.New("injected head unavailable during reopen")
			}
			return nil
		},
	}

	_, c, _ := headVolume(t)
	if err := Create(ctx, Config{Backend: hook, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	jdir := sysSpecTempJournalDir(t, "sysspec-crash-*")
	cfg1 := Config{
		Backend:         hook,
		Cipher:          c,
		Round:           1,
		Journal:         jdir,
		JournalCapacity: 16 << 20,
		Bound:           time.Hour,
		Disk:            storetest.Disk,
	}
	v1, err := Attach(ctx, cfg1)
	if err != nil {
		t.Fatal(err)
	}

	cur := sysSpecInitState(t, mem, c, 1, cfg1.JournalCapacity)
	var steps []sysspec.Step
	addStep := func(act sysspec.Action, next sysspec.SystemState) {
		steps = append(steps, sysspec.Step{Index: len(steps), Action: act, Pre: cur.Clone(), Post: next.Clone()})
		cur = next
	}

	writesBefore := storeWrites.Load()
	payloadA := bytes.Repeat([]byte("crash-reopen-chunked-"), 4096) // ~86 KiB chunked file
	sysSpecWriteFile(t, v1, RootInode, "keep.bin", payloadA)
	sysSpecWriteFile(t, v1, RootInode, "drop.txt", []byte("transient"))
	if err := v1.Remove(ctx, RootInode, "drop.txt"); err != nil {
		t.Fatal(err)
	}
	dir, err := v1.Mkdir(ctx, RootInode, "sub", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	sysSpecWriteFile(t, v1, dir.ID, "nested.txt", []byte("survives-crash"))
	if err := v1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := storeWrites.Load() - writesBefore; got != 0 {
		t.Fatalf("local mutations + Sync issued %d store writes, want 0", got)
	}
	st1 := v1.State()
	if st1.Visible == 0 || st1.Durable != st1.Visible || st1.Published != 0 {
		t.Fatalf("pre-crash state = %+v, want Visible==Durable>0 Published==0", st1)
	}

	// Record local journal appends + sync in SYSSPEC trace.
	for seq := uint64(1); seq <= st1.Visible; seq++ {
		next := cur.Clone()
		next.Journal.Visible = seq
		next.Journal.LastKept = seq
		if cur.Journal.Visible == cur.Journal.Published {
			next.Journal.FirstKept = seq
			next.Journal.OldestAckUnixNs = 1_000_000
			next.Journal.DeadlineAnchorNs = 1_000_000 + int64(time.Hour)
		}
		next.Journal.RecordWriteEpoch[seq] = cur.Journal.ActiveSyncEpoch
		next.Journal.UsedBytes = min(st1.Backlog, next.Journal.CapacityBytes)
		addStep(sysspec.Action{
			Kind:       sysspec.ActionJournalAppend,
			Actor:      "w1",
			Round:      1,
			Seq:        seq,
			BulkRefSeq: 1,
		}, next)
	}
	{
		next := cur.Clone()
		flight := cur.Journal.ActiveSyncEpoch + 1
		next.Journal.ActiveSyncEpoch = flight
		next.Journal.CompletedSync = flight
		next.Journal.Durable = st1.Durable
		addStep(sysspec.Action{
			Kind:           sysspec.ActionJournalSync,
			Actor:          "w1",
			Round:          1,
			Seq:            st1.Durable,
			SyncFlightUsed: flight,
		}, next)
	}

	// Simulate process crash: Close without Flush leaves store at Head.Journal==0.
	if err := v1.Close(); err != nil {
		t.Fatal(err)
	}
	hPreCrash, err := unmarshalHead(headPlain(t, mem, c))
	if err != nil {
		t.Fatal(err)
	}
	if hPreCrash.Journal != 0 {
		t.Fatalf("Head.Journal before reopen = %d, want 0", hPreCrash.Journal)
	}

	// Failed reopen attempt at Round 2 (store refuses Head claim): must fail and keep Round 1 journal intact.
	refuseHeadPut.Store(true)
	cfg2 := cfg1
	cfg2.Round = 2
	if _, err := Attach(ctx, cfg2); err == nil {
		t.Fatal("expected Attach(Round=2) to fail while store refuses Head")
	}
	refuseHeadPut.Store(false)

	// Reopen on the same node at Round 3: replays Round 1's journal and publishes it under Round 3.
	cfg3 := cfg1
	cfg3.Round = 3
	v3, err := Attach(ctx, cfg3)
	if err != nil {
		t.Fatalf("Attach(Round=3) reopen: %v", err)
	}
	defer v3.Close()

	rec := v3.Recovery()
	if rec.Lost || rec.Replayed != st1.Visible {
		t.Fatalf("Recovery() = %+v, want Lost=false Replayed=%d", rec, st1.Visible)
	}
	hRecovered, err := unmarshalHead(headPlain(t, mem, c))
	if err != nil {
		t.Fatal(err)
	}
	if hRecovered.Journal != st1.Visible {
		t.Fatalf("recovered Head.Journal = %d, want %d", hRecovered.Journal, st1.Visible)
	}
	st3 := v3.State()
	if st3.Published != st1.Visible || st3.Visible != st1.Visible {
		t.Fatalf("post-reopen state = %+v, want Published==Visible==%d", st3, st1.Visible)
	}

	// Verify recovered data on both live v3 and a fresh read-only Open from the store.
	ro, err := Open(ctx, Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	for label, vol := range map[string]*Volume{"replayed-v3": v3, "store-ro": ro} {
		inKeep, err := vol.Resolve(ctx, "/keep.bin")
		if err != nil {
			t.Fatalf("%s Resolve(/keep.bin): %v", label, err)
		}
		if !bytes.Equal(sysSpecReadFile(t, vol, inKeep.ID), payloadA) {
			t.Fatalf("%s /keep.bin content mismatch", label)
		}
		if _, err := vol.Resolve(ctx, "/drop.txt"); err == nil {
			t.Fatalf("%s /drop.txt resurrected after crash reopen", label)
		}
		inNested, err := vol.Resolve(ctx, "/sub/nested.txt")
		if err != nil {
			t.Fatalf("%s Resolve(/sub/nested.txt): %v", label, err)
		}
		if got := string(sysSpecReadFile(t, vol, inNested.ID)); got != "survives-crash" {
			t.Fatalf("%s /sub/nested.txt = %q, want survives-crash", label, got)
		}
	}

	// Record crash-reopen takeover + replay publication + journal publish/reclaim in SYSSPEC trace.
	{
		next := cur.Clone()
		var tok3 [16]byte
		tok3[0] = 3
		oldW := next.Fence.Writers["w1"]
		oldW.Fenced = true
		next.Fence.Writers["w1"] = oldW
		next.Fence.Writers["w3"] = sysspec.FenceWriter{ID: "w3", Round: 3, Token: tok3}
		next.Fence.Head.Round = 3
		next.Fence.Head.Token = tok3
		next.Fence.Head.Version++
		next.Journal.LineageRound = 3
		addStep(sysspec.Action{Kind: sysspec.ActionVolumeTakeover, Actor: "w3", Round: 3}, next)
	}
	{
		next := cur.Clone()
		next.Publication.Committed = sysSpecCommitHead(hRecovered)
		next.Publication.WorkingRoot = hRecovered.Root
		next.Publication.WorkingNext = hRecovered.Next
		next.Publication.LastAttemptedSeq = hRecovered.Seq
		next.Publication.CallerSeenSeq = hRecovered.Seq
		next.Publication.UsedSeqs[hRecovered.Seq] = true
		next.Publication.FlushHeldBy = "w3"
		next.Publication.FlushStages = []sysspec.WriteStage{
			{Kind: "pack", Key: "pack/replayed"},
			{Kind: "shard", Key: "meta/replayed"},
			{Kind: "head", Key: headKey},
		}
		for k := range mem.Dump() {
			if strings.HasPrefix(k, "pack/") {
				next.Publication.UploadedPacks[k] = true
				next.Publication.ReferencedPacks[k] = true
			} else if strings.HasPrefix(k, "meta/") {
				next.Publication.UploadedShards[k] = true
				next.Publication.ReferencedShards[k] = true
			}
		}
		addStep(sysspec.Action{
			Kind:          sysspec.ActionVolumeFlush,
			Actor:         "w3",
			Round:         3,
			Seq:           hRecovered.Seq,
			JournalTarget: hRecovered.Journal,
		}, next)
	}
	{
		next := cur.Clone()
		next.Journal.Published = hRecovered.Journal
		next.Journal.CommittedFix()
		next.Journal.OldestAckUnixNs = 0
		next.Journal.DeadlineAnchorNs = 0
		addStep(sysspec.Action{Kind: sysspec.ActionJournalPublish, Actor: "w3", Round: 3, Seq: hRecovered.Journal}, next)
	}
	{
		next := cur.Clone()
		next.Journal.Floor = hRecovered.Journal
		next.Journal.UsedBytes = 0
		addStep(sysspec.Action{Kind: sysspec.ActionJournalReclaim, Actor: "w3", Round: 3}, next)
	}

	if vs := reg.CheckTrace(sysspec.Trace{Name: "v2-crash-reopen", Steps: steps}); len(vs) != 0 {
		t.Fatalf("v2 crash/reopen trace violated SYSSPEC: %v", vs)
	}
}

// TestSysSpecRealV2FencingTakeoverTrace drives a real VFS v2 Volume through
// equal-round rejection (F3), cross-node takeover preserving Head payload (F4)
// and rotating lineage (RG4), permanent fencing of the deposed writer (F2/F5/RG3)
// without falsely advancing its Published watermark (RG2), and successor commits.
func TestSysSpecRealV2FencingTakeoverTrace(t *testing.T) {
	ctx := context.Background()
	reg := sysspec.DefaultRegistry()

	be, c, cfg := headVolume(t)
	j1Dir := sysSpecTempJournalDir(t, "sysspec-fence-w1-*")
	cfg1 := cfg
	cfg1.Round = 1
	cfg1.Journal = j1Dir
	cfg1.Bound = time.Hour
	v1, err := Attach(ctx, cfg1)
	if err != nil {
		t.Fatal(err)
	}
	defer v1.Close()

	cur := sysSpecInitState(t, be, c, 1, cfg1.JournalCapacity)
	var steps []sysspec.Step
	addStep := func(act sysspec.Action, next sysspec.SystemState) {
		steps = append(steps, sysspec.Step{Index: len(steps), Action: act, Pre: cur.Clone(), Post: next.Clone()})
		cur = next
	}

	// Step 1: Round 1 writes + Flush commits h1.
	sysSpecWriteFile(t, v1, RootInode, "committed.txt", []byte("from-round-1"))
	if err := v1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	h1, err := unmarshalHead(headPlain(t, be, c))
	if err != nil {
		t.Fatal(err)
	}
	st1 := v1.State()
	if h1.Journal != st1.Published || st1.Published == 0 {
		t.Fatalf("round 1 post-Flush Head.Journal=%d Published=%d", h1.Journal, st1.Published)
	}
	for seq := uint64(1); seq <= h1.Journal; seq++ {
		next := cur.Clone()
		next.Journal.Visible = seq
		next.Journal.LastKept = seq
		if cur.Journal.Visible == cur.Journal.Published {
			next.Journal.FirstKept = seq
			next.Journal.OldestAckUnixNs = 1_000_000
			next.Journal.DeadlineAnchorNs = 1_000_000 + int64(time.Hour)
		}
		next.Journal.RecordWriteEpoch[seq] = cur.Journal.ActiveSyncEpoch
		addStep(sysspec.Action{Kind: sysspec.ActionJournalAppend, Actor: "w1", Round: 1, Seq: seq, BulkRefSeq: 1}, next)
	}
	{
		next := cur.Clone()
		flight := cur.Journal.ActiveSyncEpoch + 1
		next.Journal.ActiveSyncEpoch = flight
		next.Journal.CompletedSync = flight
		next.Journal.Durable = h1.Journal
		addStep(sysspec.Action{Kind: sysspec.ActionJournalSync, Actor: "w1", Round: 1, Seq: h1.Journal, SyncFlightUsed: flight}, next)
	}
	{
		next := cur.Clone()
		next.Publication.Committed = sysSpecCommitHead(h1)
		next.Publication.WorkingRoot = h1.Root
		next.Publication.WorkingNext = h1.Next
		next.Publication.LastAttemptedSeq = h1.Seq
		next.Publication.CallerSeenSeq = h1.Seq
		next.Publication.UsedSeqs[h1.Seq] = true
		next.Publication.FlushHeldBy = "w1"
		next.Publication.FlushStages = []sysspec.WriteStage{{Kind: "shard", Key: "meta/r1"}, {Kind: "head", Key: headKey}}
		next.Fence.Head.Payload = slices.Clone(be.Dump()[headKey])
		next.Fence.Head.Size = len(next.Fence.Head.Payload)
		next.Fence.Head.Version++
		for k := range be.Dump() {
			if strings.HasPrefix(k, "meta/") {
				next.Publication.UploadedShards[k] = true
				next.Publication.ReferencedShards[k] = true
			}
		}
		addStep(sysspec.Action{Kind: sysspec.ActionVolumeFlush, Actor: "w1", Round: 1, Seq: h1.Seq, JournalTarget: h1.Journal}, next)
	}
	{
		next := cur.Clone()
		next.Journal.Published = h1.Journal
		next.Journal.CommittedFix()
		next.Journal.OldestAckUnixNs = 0
		next.Journal.DeadlineAnchorNs = 0
		addStep(sysspec.Action{Kind: sysspec.ActionJournalPublish, Actor: "w1", Round: 1, Seq: h1.Journal}, next)
	}

	// Step 2: Equal-round Attach(Round=1) is refused with ErrStaleRound (F3).
	jDupDir := sysSpecTempJournalDir(t, "sysspec-fence-dup-*")
	cfgDup := cfg1
	cfgDup.Journal = jDupDir
	eqErr := func() error {
		_, err := Attach(ctx, cfgDup)
		return err
	}()
	if !errors.Is(eqErr, ErrStaleRound) {
		t.Fatalf("Attach(Round=1) = %v, want ErrStaleRound", eqErr)
	}
	addStep(sysspec.Action{
		Kind:        sysspec.ActionFenceClaim,
		Actor:       "wdup",
		Round:       1,
		ReturnedErr: sysSpecClassifyErr(eqErr),
	}, cur.Clone())

	// Step 3: Writer 1 writes an unpublished local tail, then writer 2 claims Round 2 on another node.
	sysSpecWriteFile(t, v1, RootInode, "unflushed-w1.txt", []byte("lost-to-takeover"))
	if err := v1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	stTail := v1.State()

	j2Dir := sysSpecTempJournalDir(t, "sysspec-fence-w2-*")
	cfg2 := cfg1
	cfg2.Round = 2
	cfg2.Journal = j2Dir
	v2, err := Attach(ctx, cfg2)
	if err != nil {
		t.Fatalf("Attach(Round=2): %v", err)
	}
	defer v2.Close()

	if rec := v2.Recovery(); !rec.Lost || rec.Why == "" {
		t.Fatalf("cross-node takeover Recovery() = %+v, want Lost=true", rec)
	}
	hAfterClaim, err := unmarshalHead(headPlain(t, be, c))
	if err != nil {
		t.Fatal(err)
	}
	if hAfterClaim.Root != h1.Root || hAfterClaim.Seq != h1.Seq || hAfterClaim.Journal != h1.Journal {
		t.Fatalf("takeover altered committed Head: got %+v want %+v", hAfterClaim, h1)
	}
	{
		next := cur.Clone()
		var tok2 [16]byte
		tok2[0] = 2
		w1 := next.Fence.Writers["w1"]
		w1.Fenced = true
		next.Fence.Writers["w1"] = w1
		next.Fence.Writers["w2"] = sysspec.FenceWriter{ID: "w2", Round: 2, Token: tok2}
		next.Fence.Head.Round = 2
		next.Fence.Head.Token = tok2
		next.Fence.Head.Version++
		next.Journal.LineageRound = 2
		next.Journal.Visible = h1.Journal
		next.Journal.Durable = h1.Journal
		next.Journal.Published = h1.Journal
		next.Journal.Proven = h1.Journal
		next.Journal.Floor = h1.Journal
		next.Journal.FirstKept = 0
		next.Journal.LastKept = h1.Journal
		addStep(sysspec.Action{Kind: sysspec.ActionVolumeTakeover, Actor: "w2", Round: 2}, next)
	}

	// Step 4: Deposed writer 1's Flush fails with ErrStaleRound, does NOT mutate Head,
	// and does NOT falsely advance v1.State().Published past h1.Journal!
	headBytesBeforeDeposed := slices.Clone(be.Dump()[headKey])
	depErr1 := v1.Flush(ctx)
	if !errors.Is(depErr1, ErrStaleRound) {
		t.Fatalf("deposed v1.Flush = %v, want ErrStaleRound", depErr1)
	}
	if !bytes.Equal(headBytesBeforeDeposed, be.Dump()[headKey]) {
		t.Fatal("deposed v1.Flush mutated Head in store")
	}
	if stDep := v1.State(); stDep.Published != h1.Journal {
		t.Fatalf("deposed v1 falsely advanced Published=%d past committed Head.Journal=%d (tail=%d)",
			stDep.Published, h1.Journal, stTail.Visible)
	}
	// Second Flush and mutation on deposed v1 stay permanently fenced (F5).
	depErr2 := v1.Flush(ctx)
	if !errors.Is(depErr2, ErrStaleRound) {
		t.Fatalf("second deposed v1.Flush = %v, want ErrStaleRound", depErr2)
	}
	if _, err := v1.Create(ctx, RootInode, "after-fence.txt", 0o644); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("deposed v1.Create = %v, want ErrStaleRound", err)
	}
	addStep(sysspec.Action{
		Kind:        sysspec.ActionFencePut,
		Actor:       "w1",
		Round:       1,
		ReturnedErr: sysSpecClassifyErr(depErr1),
	}, cur.Clone())

	// Step 5: Successor v2 sees Round 1's committed file (not w1's unflushed tail) and commits cleanly.
	if _, err := v2.Resolve(ctx, "/committed.txt"); err != nil {
		t.Fatalf("v2 missing Round 1 committed file: %v", err)
	}
	if _, err := v2.Resolve(ctx, "/unflushed-w1.txt"); err == nil {
		t.Fatal("v2 saw Round 1 unflushed file")
	}
	sysSpecWriteFile(t, v2, RootInode, "round2.txt", []byte("from-round-2"))
	if err := v2.Flush(ctx); err != nil {
		t.Fatalf("v2.Flush: %v", err)
	}
	h2, err := unmarshalHead(headPlain(t, be, c))
	if err != nil {
		t.Fatal(err)
	}
	if h2.Seq <= h1.Seq || h2.Journal <= h1.Journal {
		t.Fatalf("successor commit did not advance monotonically: h1=(Seq=%d,Journal=%d) h2=(Seq=%d,Journal=%d)",
			h1.Seq, h1.Journal, h2.Seq, h2.Journal)
	}
	for seq := cur.Journal.Visible + 1; seq <= h2.Journal; seq++ {
		next := cur.Clone()
		next.Journal.Visible = seq
		next.Journal.LastKept = seq
		if cur.Journal.Visible == cur.Journal.Published {
			next.Journal.FirstKept = seq
			next.Journal.OldestAckUnixNs = 2_000_000
			next.Journal.DeadlineAnchorNs = 2_000_000 + int64(time.Hour)
		}
		next.Journal.RecordWriteEpoch[seq] = cur.Journal.ActiveSyncEpoch
		addStep(sysspec.Action{Kind: sysspec.ActionJournalAppend, Actor: "w2", Round: 2, Seq: seq, BulkRefSeq: 1}, next)
	}
	{
		next := cur.Clone()
		flight := cur.Journal.ActiveSyncEpoch + 1
		next.Journal.ActiveSyncEpoch = flight
		next.Journal.CompletedSync = flight
		next.Journal.Durable = h2.Journal
		addStep(sysspec.Action{Kind: sysspec.ActionJournalSync, Actor: "w2", Round: 2, Seq: h2.Journal, SyncFlightUsed: flight}, next)
	}
	{
		next := cur.Clone()
		next.Publication.Committed = sysSpecCommitHead(h2)
		next.Publication.WorkingRoot = h2.Root
		next.Publication.WorkingNext = h2.Next
		next.Publication.LastAttemptedSeq = h2.Seq
		next.Publication.CallerSeenSeq = h2.Seq
		next.Publication.UsedSeqs[h2.Seq] = true
		next.Publication.FlushHeldBy = "w2"
		next.Publication.FlushStages = []sysspec.WriteStage{{Kind: "shard", Key: "meta/r2"}, {Kind: "head", Key: headKey}}
		for k := range be.Dump() {
			if strings.HasPrefix(k, "meta/") {
				next.Publication.UploadedShards[k] = true
				next.Publication.ReferencedShards[k] = true
			}
		}
		addStep(sysspec.Action{Kind: sysspec.ActionVolumeFlush, Actor: "w2", Round: 2, Seq: h2.Seq, JournalTarget: h2.Journal}, next)
	}
	{
		next := cur.Clone()
		next.Journal.Published = h2.Journal
		next.Journal.CommittedFix()
		next.Journal.OldestAckUnixNs = 0
		next.Journal.DeadlineAnchorNs = 0
		addStep(sysspec.Action{Kind: sysspec.ActionJournalPublish, Actor: "w2", Round: 2, Seq: h2.Journal}, next)
	}

	if vs := reg.CheckTrace(sysspec.Trace{Name: "v2-fencing-takeover", Steps: steps}); len(vs) != 0 {
		t.Fatalf("v2 fencing/takeover trace violated SYSSPEC: %v", vs)
	}
}

// TestSysSpecRealV2MonotonicPublicationTrace drives a real VFS v2 Volume across
// multiple explicit Flushes (including an injected Head write failure + retry)
// and a controlled-clock bound-driven background publication, asserting strict
// monotonicity of Head.Seq, Head.Journal, Head.Next, and journal watermarks,
// packs -> shards -> Head ordering, and zero SYSSPEC violations.
func TestSysSpecRealV2MonotonicPublicationTrace(t *testing.T) {
	ctx := context.Background()
	reg := sysspec.DefaultRegistry()

	mem := storetest.NewMem()
	clk := newSysSpecClock()
	var hookMu sync.Mutex
	var stages []sysspec.WriteStage
	failNextHead := false
	headCommittedCh := make(chan struct{}, 16)
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
			if op != "put" && op != "putver" && op != "putabs" {
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
				if failNextHead {
					failNextHead = false
					return errors.New("injected head publish failure")
				}
			}
			return nil
		},
		After: func(op, key string) error {
			if key == headKey && op == "putver" {
				select {
				case headCommittedCh <- struct{}{}:
				default:
				}
			}
			return nil
		},
	}

	_, c, _ := headVolume(t)
	if err := Create(ctx, Config{Backend: hook, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	jdir := sysSpecTempJournalDir(t, "sysspec-mono-*")
	cfg := Config{
		Backend:         hook,
		Cipher:          c,
		Round:           1,
		Journal:         jdir,
		JournalCapacity: 16 << 20,
		Bound:           4 * time.Second,
		Clock:           clk,
		Disk:            storetest.Disk,
	}
	v, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	resetStages()

	cur := sysSpecInitState(t, mem, c, 1, cfg.JournalCapacity)
	var steps []sysspec.Step
	addStep := func(act sysspec.Action, next sysspec.SystemState) {
		steps = append(steps, sysspec.Step{Index: len(steps), Action: act, Pre: cur.Clone(), Post: next.Clone()})
		cur = next
	}

	appendUpTo := func(targetVisible uint64) {
		for seq := cur.Journal.Visible + 1; seq <= targetVisible; seq++ {
			next := cur.Clone()
			next.Journal.Visible = seq
			next.Journal.LastKept = seq
			if cur.Journal.Visible == cur.Journal.Published {
				next.Journal.FirstKept = seq
				next.Journal.OldestAckUnixNs = clk.Now().UnixNano()
				next.Journal.DeadlineAnchorNs = clk.Now().Add(cfg.Bound).UnixNano()
			}
			next.Journal.RecordWriteEpoch[seq] = cur.Journal.ActiveSyncEpoch
			addStep(sysspec.Action{Kind: sysspec.ActionJournalAppend, Actor: "w1", Round: 1, Seq: seq, BulkRefSeq: 1}, next)
		}
	}

	// Generation 1: chunked write, fail Head once (verify Published stays 0 & Seq advances on retry).
	sysSpecWriteFile(t, v, RootInode, "gen1.bin", bytes.Repeat([]byte("g1"), 48<<10))
	stG1 := v.State()
	appendUpTo(stG1.Visible)

	resetStages()
	hookMu.Lock()
	failNextHead = true
	hookMu.Unlock()
	if err := v.Flush(ctx); err == nil {
		t.Fatal("expected injected Head failure on gen 1")
	}
	if stFail := v.State(); stFail.Published != 0 || !stFail.Degraded || stFail.Publishing == nil {
		t.Fatalf("after failed Head publish, State = %+v, want Published=0 Degraded=true", stFail)
	}
	v.mu.RLock()
	failedSeq := v.seq
	v.mu.RUnlock()
	{
		// syncTo ran before Head write inside g.run.
		next := cur.Clone()
		flight := cur.Journal.ActiveSyncEpoch + 1
		next.Journal.ActiveSyncEpoch = flight
		next.Journal.CompletedSync = flight
		next.Journal.Durable = stG1.Visible
		addStep(sysspec.Action{Kind: sysspec.ActionJournalSync, Actor: "w1", Round: 1, Seq: stG1.Visible, SyncFlightUsed: flight}, next)
	}
	{
		next := cur.Clone()
		next.Publication.FlushStages = snapshotStages()
		next.Publication.LastAttemptedSeq = failedSeq
		next.Publication.UsedSeqs[failedSeq] = true
		next.Publication.FlushHeldBy = "w1"
		for k := range mem.Dump() {
			if strings.HasPrefix(k, "pack/") {
				next.Publication.UploadedPacks[k] = true
			} else if strings.HasPrefix(k, "meta/") {
				next.Publication.UploadedShards[k] = true
			}
		}
		addStep(sysspec.Action{
			Kind:          sysspec.ActionVolumeFlush,
			Actor:         "w1",
			Round:         1,
			Seq:           failedSeq,
			JournalTarget: stG1.Visible,
			ReturnedErr:   "injected head publish failure",
		}, next)
	}

	// Retry gen 1 Flush: must succeed with Seq > failedSeq and publish through stG1.Visible.
	resetStages()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("gen 1 retry Flush: %v", err)
	}
	h1, err := unmarshalHead(headPlain(t, mem, c))
	if err != nil {
		t.Fatal(err)
	}
	if h1.Seq <= failedSeq || h1.Journal != stG1.Visible {
		t.Fatalf("gen 1 Head=(Seq=%d,Journal=%d), want Seq>%d Journal=%d", h1.Seq, h1.Journal, failedSeq, stG1.Visible)
	}
	{
		next := cur.Clone()
		next.Publication.FlushStages = snapshotStages()
		next.Publication.Committed = sysSpecCommitHead(h1)
		next.Publication.WorkingRoot = h1.Root
		next.Publication.WorkingNext = h1.Next
		next.Publication.LastAttemptedSeq = h1.Seq
		next.Publication.CallerSeenSeq = h1.Seq
		next.Publication.UsedSeqs[h1.Seq] = true
		next.Publication.FlushHeldBy = "w1"
		for k := range mem.Dump() {
			if strings.HasPrefix(k, "pack/") {
				next.Publication.UploadedPacks[k] = true
				next.Publication.ReferencedPacks[k] = true
			} else if strings.HasPrefix(k, "meta/") {
				next.Publication.UploadedShards[k] = true
				next.Publication.ReferencedShards[k] = true
			}
		}
		addStep(sysspec.Action{Kind: sysspec.ActionVolumeFlush, Actor: "w1", Round: 1, Seq: h1.Seq, JournalTarget: h1.Journal}, next)
	}
	{
		next := cur.Clone()
		next.Journal.Published = h1.Journal
		next.Journal.CommittedFix()
		next.Journal.OldestAckUnixNs = 0
		next.Journal.DeadlineAnchorNs = 0
		addStep(sysspec.Action{Kind: sysspec.ActionJournalPublish, Actor: "w1", Round: 1, Seq: h1.Journal}, next)
	}

	// Generation 2: controlled-clock bound-driven background publication (no explicit Flush call).
	resetStages()
	sysSpecWriteFile(t, v, RootInode, "gen2-bound.txt", []byte("published-by-bound"))
	stG2Pre := v.State()
	appendUpTo(stG2Pre.Visible)

	// Drain stale notifications then advance clock past Bound so publisher fires automatically.
	for len(headCommittedCh) > 0 {
		<-headCommittedCh
	}
	clk.Advance(cfg.Bound + time.Second)
	deadline := time.Now().Add(5 * time.Second)
	var h2 Head
	for {
		var uerr error
		h2, uerr = unmarshalHead(headPlain(t, mem, c))
		if uerr == nil && h2.Journal >= stG2Pre.Visible && v.State().Published >= stG2Pre.Visible {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bound-driven background publication did not land within 5s (Head.Journal=%d want >= %d, State=%+v)",
				h2.Journal, stG2Pre.Visible, v.State())
		}
		select {
		case <-headCommittedCh:
		case <-time.After(20 * time.Millisecond):
		}
	}
	if h2.Seq <= h1.Seq || h2.Journal <= h1.Journal || h2.Next <= h1.Next {
		t.Fatalf("non-monotonic bound publication: h1=(Seq=%d,Journal=%d,Next=%d) h2=(Seq=%d,Journal=%d,Next=%d)",
			h1.Seq, h1.Journal, h1.Next, h2.Seq, h2.Journal, h2.Next)
	}
	{
		next := cur.Clone()
		flight := cur.Journal.ActiveSyncEpoch + 1
		next.Journal.ActiveSyncEpoch = flight
		next.Journal.CompletedSync = flight
		next.Journal.Durable = h2.Journal
		addStep(sysspec.Action{Kind: sysspec.ActionJournalSync, Actor: "w1", Round: 1, Seq: h2.Journal, SyncFlightUsed: flight}, next)
	}
	{
		next := cur.Clone()
		next.Publication.FlushStages = snapshotStages()
		next.Publication.Committed = sysSpecCommitHead(h2)
		next.Publication.WorkingRoot = h2.Root
		next.Publication.WorkingNext = h2.Next
		next.Publication.LastAttemptedSeq = h2.Seq
		next.Publication.CallerSeenSeq = h2.Seq
		next.Publication.UsedSeqs[h2.Seq] = true
		next.Publication.FlushHeldBy = "w1"
		for k := range mem.Dump() {
			if strings.HasPrefix(k, "pack/") {
				next.Publication.UploadedPacks[k] = true
				next.Publication.ReferencedPacks[k] = true
			} else if strings.HasPrefix(k, "meta/") {
				next.Publication.UploadedShards[k] = true
				next.Publication.ReferencedShards[k] = true
			}
		}
		addStep(sysspec.Action{Kind: sysspec.ActionVolumeFlush, Actor: "w1", Round: 1, Seq: h2.Seq, JournalTarget: h2.Journal}, next)
	}
	{
		next := cur.Clone()
		next.Journal.Published = h2.Journal
		next.Journal.CommittedFix()
		next.Journal.OldestAckUnixNs = 0
		next.Journal.DeadlineAnchorNs = 0
		addStep(sysspec.Action{Kind: sysspec.ActionJournalPublish, Actor: "w1", Round: 1, Seq: h2.Journal}, next)
	}

	if vs := reg.CheckTrace(sysspec.Trace{Name: "v2-monotonic-publication", Steps: steps}); len(vs) != 0 {
		t.Fatalf("v2 monotonic publication trace violated SYSSPEC: %v", vs)
	}
}

// TestSysSpecRealV2BoundedBackpressureTrace drives a real VFS v2 Volume through
// healthy early-publication drain under a tight Dirty/JournalCapacity bound,
// an S3 outage that hits bounded ErrFull without mutating Visible or losing
// accepted local writes, local Sync during outage, and clean recovery drain,
// validating the full trace against SYSSPEC.
func TestSysSpecRealV2BoundedBackpressureTrace(t *testing.T) {
	ctx := context.Background()
	reg := sysspec.DefaultRegistry()

	mem := storetest.NewMem()
	var storeDown atomic.Bool
	hook := &storetest.Hook{
		Backend: mem,
		Before: func(op, key string) error {
			if storeDown.Load() && (op == "put" || op == "putver") {
				return errors.New("injected s3 outage")
			}
			return nil
		},
	}

	_, c, _ := headVolume(t)
	if err := Create(ctx, Config{Backend: hook, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	jdir := sysSpecTempJournalDir(t, "sysspec-bp-*")
	const jCap = int64(8 << 20)
	const dirtyCap = int64(1 << 20)
	cfg := Config{
		Backend:         hook,
		Cipher:          c,
		Round:           1,
		Journal:         jdir,
		JournalCapacity: jCap,
		Dirty:           dirtyCap,
		Bound:           time.Hour,
		Disk:            storetest.Disk,
	}
	v, err := Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	in, err := v.Create(ctx, RootInode, "stream.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Phase 1: healthy store drains > 3 MiB through 1 MiB Dirty / 8 MiB JournalCapacity without ErrFull.
	block := bytes.Repeat([]byte{0x5a, 0xa5, 0x3c, 0xc3}, 32<<10) // 128 KiB
	var model []byte
	for range 24 {
		if _, err := f.WriteAt(ctx, block, int64(len(model))); err != nil {
			t.Fatalf("healthy write failed at %d KiB: %v", len(model)>>10, err)
		}
		model = append(model, block...)
		if st := v.State(); st.Backlog > jCap {
			t.Fatalf("healthy Backlog=%d exceeded JournalCapacity=%d", st.Backlog, jCap)
		}
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("post-phase1 Flush: %v", err)
	}
	hBase, err := unmarshalHead(headPlain(t, mem, c))
	if err != nil {
		t.Fatal(err)
	}
	cur := sysSpecInitState(t, mem, c, 1, jCap)
	var steps []sysspec.Step
	addStep := func(act sysspec.Action, next sysspec.SystemState) {
		steps = append(steps, sysspec.Step{Index: len(steps), Action: act, Pre: cur.Clone(), Post: next.Clone()})
		cur = next
	}

	// Phase 2: store outage -> accept up to bound, then fail closed with ErrFull without mutating Visible.
	storeDown.Store(true)
	var outErr error
	acceptedDuringOutage := 0
	var preErrVisible uint64
	for range 400 {
		preErrVisible = v.State().Visible
		if _, outErr = f.WriteAt(ctx, block, int64(len(model))); outErr != nil {
			break
		}
		model = append(model, block...)
		acceptedDuringOutage++
	}
	if !errors.Is(outErr, ErrFull) {
		t.Fatalf("outage write error = %v, want ErrFull (accepted=%d)", outErr, acceptedDuringOutage)
	}
	if acceptedDuringOutage == 0 {
		t.Fatal("expected at least one write accepted into local journal before ErrFull")
	}
	stFull := v.State()
	if stFull.Visible != preErrVisible {
		t.Fatalf("rejected ErrFull write mutated Visible (%d -> %d)", preErrVisible, stFull.Visible)
	}
	if !stFull.Degraded || stFull.Backlog <= 0 || stFull.Backlog > jCap || stFull.Published >= stFull.Visible {
		t.Fatalf("unexpected outage State at ErrFull: %+v", stFull)
	}

	// Record accepted appends + rejected ErrFull append in SYSSPEC trace (J1, J6, J7).
	for seq := hBase.Journal + 1; seq <= stFull.Visible; seq++ {
		next := cur.Clone()
		next.Journal.Visible = seq
		next.Journal.LastKept = seq
		if cur.Journal.Visible == cur.Journal.Published {
			next.Journal.FirstKept = seq
			next.Journal.OldestAckUnixNs = 5_000_000
			next.Journal.DeadlineAnchorNs = 5_000_000 + int64(time.Hour)
		}
		next.Journal.RecordWriteEpoch[seq] = cur.Journal.ActiveSyncEpoch
		next.Journal.UsedBytes = min(stFull.Backlog, jCap)
		addStep(sysspec.Action{Kind: sysspec.ActionJournalAppend, Actor: "w1", Round: 1, Seq: seq, BulkRefSeq: 1}, next)
	}
	addStep(sysspec.Action{
		Kind:        sysspec.ActionJournalAppend,
		Actor:       "w1",
		Round:       1,
		Seq:         stFull.Visible + 1,
		BulkRefSeq:  1,
		ReturnedErr: sysSpecClassifyErr(outErr),
	}, cur.Clone())

	// Local Sync during outage must still succeed and advance Durable == Visible.
	if err := f.Sync(ctx); err != nil {
		t.Fatalf("local Sync during outage: %v", err)
	}
	stSynced := v.State()
	if stSynced.Durable < stSynced.Visible {
		t.Fatalf("after local Sync during outage, Durable=%d < Visible=%d", stSynced.Durable, stSynced.Visible)
	}
	{
		next := cur.Clone()
		flight := cur.Journal.ActiveSyncEpoch + 1
		next.Journal.ActiveSyncEpoch = flight
		next.Journal.CompletedSync = flight
		next.Journal.Durable = stSynced.Durable
		addStep(sysspec.Action{Kind: sysspec.ActionJournalSync, Actor: "w1", Round: 1, Seq: stSynced.Durable, SyncFlightUsed: flight}, next)
	}

	// Phase 3: store recovers -> Flush drains backlog in order, clears Degraded, and reclaims journal space.
	storeDown.Store(false)
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("recovery Flush: %v", err)
	}
	hRecovered, err := unmarshalHead(headPlain(t, mem, c))
	if err != nil {
		t.Fatal(err)
	}
	stDone := v.State()
	if stDone.Degraded || stDone.Publishing != nil || stDone.Published != stDone.Visible || hRecovered.Journal != stDone.Visible {
		t.Fatalf("post-recovery State=%+v Head.Journal=%d", stDone, hRecovered.Journal)
	}

	// Verify exact recovered bytes on a fresh read-only Open from the store.
	ro, err := Open(ctx, Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if got := sysSpecReadFile(t, ro, in.ID); !bytes.Equal(got, model) {
		t.Fatalf("recovered file length=%d, want %d (content mismatch)", len(got), len(model))
	}

	// Subsequent write after recovery must succeed cleanly.
	if _, err := f.WriteAt(ctx, block[:4096], int64(len(model))); err != nil {
		t.Fatalf("post-recovery write failed: %v", err)
	}

	{
		next := cur.Clone()
		next.Publication.Committed = sysSpecCommitHead(hRecovered)
		next.Publication.WorkingRoot = hRecovered.Root
		next.Publication.WorkingNext = hRecovered.Next
		next.Publication.LastAttemptedSeq = hRecovered.Seq
		next.Publication.CallerSeenSeq = hRecovered.Seq
		next.Publication.UsedSeqs[hRecovered.Seq] = true
		next.Publication.FlushHeldBy = "w1"
		next.Publication.FlushStages = []sysspec.WriteStage{
			{Kind: "pack", Key: "pack/recovery"},
			{Kind: "shard", Key: "meta/recovery"},
			{Kind: "head", Key: headKey},
		}
		for k := range mem.Dump() {
			if strings.HasPrefix(k, "pack/") {
				next.Publication.UploadedPacks[k] = true
				next.Publication.ReferencedPacks[k] = true
			} else if strings.HasPrefix(k, "meta/") {
				next.Publication.UploadedShards[k] = true
				next.Publication.ReferencedShards[k] = true
			}
		}
		addStep(sysspec.Action{Kind: sysspec.ActionVolumeFlush, Actor: "w1", Round: 1, Seq: hRecovered.Seq, JournalTarget: hRecovered.Journal}, next)
	}
	{
		next := cur.Clone()
		next.Journal.Published = hRecovered.Journal
		next.Journal.CommittedFix()
		next.Journal.OldestAckUnixNs = 0
		next.Journal.DeadlineAnchorNs = 0
		addStep(sysspec.Action{Kind: sysspec.ActionJournalPublish, Actor: "w1", Round: 1, Seq: hRecovered.Journal}, next)
	}
	{
		next := cur.Clone()
		next.Journal.Floor = hRecovered.Journal
		next.Journal.UsedBytes = 0
		addStep(sysspec.Action{Kind: sysspec.ActionJournalReclaim, Actor: "w1", Round: 1}, next)
	}

	if vs := reg.CheckTrace(sysspec.Trace{Name: "v2-bounded-backpressure", Steps: steps}); len(vs) != 0 {
		t.Fatalf("v2 bounded backpressure trace violated SYSSPEC: %v", vs)
	}
}
