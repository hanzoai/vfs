// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/fence"
)

func openS3HTTPBackend(t *testing.T, bucket, prefix string) (*storetest.S3, backend.Backend) {
	t.Helper()
	srv := storetest.NewS3(t, bucket)
	be, err := s3be.New(s3be.Config{
		Endpoint: srv.Endpoint(),
		Insecure: true,
		Bucket:   bucket,
		Prefix:   prefix,
	})
	if err != nil {
		t.Fatalf("open s3 backend: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return srv, be
}

func makeTestItems(n, size int, tag string) []Item {
	items := make([]Item, n)
	for i := range items {
		buf := make([]byte, size)
		copy(buf, fmt.Sprintf("%s-%04d-", tag, i))
		for j := 16; j < len(buf); j += 97 {
			buf[j] = byte((i + j) & 0xff)
		}
		items[i] = Item{
			Key:  fmt.Sprintf("meta/%02x/%s-%04d", i%16, tag, i),
			Data: buf,
		}
	}
	return items
}

func TestAITURBOHeadCoalesceForbiddenAndKeyValidation(t *testing.T) {
	p, err := NewPlanner(Options{GroupSize: 8})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := p.PlanCheckpoint(1, []Item{
		{Key: "pack/00/obj-a", Data: []byte("payload")},
		{Key: "head", Data: []byte("forged-head")},
	}); !errors.Is(err, ErrHeadCoalesceForbidden) {
		t.Fatalf("got %v, want ErrHeadCoalesceForbidden", err)
	}

	if _, err := p.PlanCheckpoint(1, []Item{
		{Key: "Invalid_UpperCase/key", Data: []byte("x")},
	}); !errors.Is(err, backend.ErrKey) {
		t.Fatalf("got %v, want backend.ErrKey", err)
	}
}

func TestAITURBOMatchedParallelBaselineOverS3HTTP(t *testing.T) {
	ctx := context.Background()
	const (
		numItems  = 64
		itemBytes = 16 << 10
		streams   = 8
	)
	items := makeTestItems(numItems, itemBytes, "ckpt1")

	// 1. Matched parallel ungrouped baseline (Streams=8, GroupSize=1) over S3 HTTP
	srvBase, s3Base := openS3HTTPBackend(t, "disk", "base")
	cntBase := NewCountingBackend(s3Base, 0)
	upBase, err := NewGroupedUploader(cntBase, Options{GroupSize: 1, Streams: streams})
	if err != nil {
		t.Fatal(err)
	}
	planBase, err := upBase.Planner().PlanCheckpoint(1, items)
	if err != nil {
		t.Fatal(err)
	}
	mBase, err := upBase.UploadAndCommit(ctx, planBase, 1)
	if err != nil {
		t.Fatalf("baseline UploadAndCommit: %v", err)
	}
	s3ReqsBase := srvBase.Requests()

	downBase, err := NewGroupedDownloader(cntBase, Options{GroupSize: 1, Streams: streams})
	if err != nil {
		t.Fatal(err)
	}
	cntBase.Reset()
	gotBase, dmBase, err := downBase.DownloadCheckpoint(ctx, nil)
	if err != nil {
		t.Fatalf("baseline DownloadCheckpoint: %v", err)
	}

	// 2. AITURBO grouped pack + coalesced range download (Streams=8, GroupSize=8) over S3 HTTP
	srvTurbo, s3Turbo := openS3HTTPBackend(t, "disk", "turbo")
	cntTurbo := NewCountingBackend(s3Turbo, 0)
	upTurbo, err := NewGroupedUploader(cntTurbo, Options{GroupSize: 8, Streams: streams})
	if err != nil {
		t.Fatal(err)
	}
	planTurbo, err := upTurbo.Planner().PlanCheckpoint(1, items)
	if err != nil {
		t.Fatal(err)
	}
	mTurbo, err := upTurbo.UploadAndCommit(ctx, planTurbo, 1)
	if err != nil {
		t.Fatalf("turbo UploadAndCommit: %v", err)
	}
	s3ReqsTurbo := srvTurbo.Requests()

	downTurbo, err := NewGroupedDownloader(cntTurbo, Options{GroupSize: 8, Streams: streams})
	if err != nil {
		t.Fatal(err)
	}
	cntTurbo.Reset()
	gotTurbo, dmTurbo, err := downTurbo.DownloadCheckpoint(ctx, nil)
	if err != nil {
		t.Fatalf("turbo DownloadCheckpoint: %v", err)
	}

	// Assert REAL HTTP S3 request counts dropped substantially on both upload and download paths.
	if mTurbo.Wire.PutIfAbsents >= mBase.Wire.PutIfAbsents {
		t.Fatalf("expected fewer PutIfAbsent calls: turbo=%d baseline=%d", mTurbo.Wire.PutIfAbsents, mBase.Wire.PutIfAbsents)
	}
	if s3ReqsTurbo >= s3ReqsBase {
		t.Fatalf("expected fewer real S3 HTTP requests on upload: turbo=%d baseline=%d", s3ReqsTurbo, s3ReqsBase)
	}
	if dmTurbo.Wire.GetRanges >= dmBase.Wire.GetRanges {
		t.Fatalf("expected fewer GetRange calls on download: turbo=%d baseline=%d", dmTurbo.Wire.GetRanges, dmBase.Wire.GetRanges)
	}
	if mTurbo.Wire.PutIfVersions == 0 {
		t.Logf("initial create used PutIfAbsent for Head; checking CAS PutIfVersion next round")
	}

	for _, it := range items {
		if !bytes.Equal(gotBase[it.Key], it.Data) {
			t.Fatalf("baseline byte mismatch on %s", it.Key)
		}
		if !bytes.Equal(gotTurbo[it.Key], it.Data) {
			t.Fatalf("turbo byte mismatch on %s", it.Key)
		}
	}

	t.Logf("S3 HTTP Upload: baseline(streams=%d,g=1)=%d reqs / %d HTTP calls, turbo(streams=%d,g=8)=%d reqs / %d HTTP calls",
		streams, mBase.Wire.PutIfAbsents, s3ReqsBase, streams, mTurbo.Wire.PutIfAbsents, s3ReqsTurbo)
	t.Logf("S3 HTTP Download GetRanges: baseline=%d, turbo=%d", dmBase.Wire.GetRanges, dmTurbo.Wire.GetRanges)
}

func TestAITURBOImmutableDataReuseAndConcurrentAbortSafety(t *testing.T) {
	ctx := context.Background()
	_, s3be := openS3HTTPBackend(t, "disk", "reuse")
	cnt := NewCountingBackend(s3be, 0)
	up, err := NewGroupedUploader(cnt, Options{GroupSize: 4, Streams: 4})
	if err != nil {
		t.Fatal(err)
	}

	// Checkpoint 1: commit 16 objects (4 packed groups) at round 1
	items1 := makeTestItems(16, 4096, "00shared")
	plan1, err := up.Planner().PlanCheckpoint(1, items1)
	if err != nil {
		t.Fatal(err)
	}
	m1, err := up.UploadAndCommit(ctx, plan1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Checkpoint 2: republish the exact first 8 items sorted into lower prefixes plus
	// 8 new items sorted after them ("99ckpt2"), preserving packed group identity so
	// PutIfAbsent encounters real immutable ErrConflict reuse without overwriting.
	cnt.Reset()
	items2 := append([]Item(nil), items1[:8]...)
	items2 = append(items2, makeTestItems(8, 4096, "99ckpt2")...)
	for i := 8; i < len(items2); i++ {
		items2[i].Key = fmt.Sprintf("meta/ff/ckpt2-%04d", i)
	}
	plan2, err := up.Planner().PlanCheckpoint(2, items2)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := up.UploadAndCommit(ctx, plan2, 1)
	if err != nil {
		t.Fatalf("ckpt2 commit: %v", err)
	}
	if m2.Wire.ReusedObjects == 0 || m2.Wire.ReusedBytes == 0 {
		t.Fatalf("expected immutable pack reuse on overlapping groups, got reusedObjects=%d reusedBytes=%d",
			m2.Wire.ReusedObjects, m2.Wire.ReusedBytes)
	}
	if m2.Wire.PutIfVersions == 0 {
		t.Fatalf("expected Head update on ckpt2 to use conditional PutIfVersion CAS")
	}

	// Concurrent publisher abort test: Publisher B injects failure after first PutIfAbsent
	// MUST NOT delete immutable packs referenced by committed Checkpoint 2!
	var puts atomic.Int32
	failingHook := &storetest.Hook{
		Backend: s3be,
		Before: func(op, key string) error {
			if op == "putabsent" && puts.Add(1) >= 2 {
				return errors.New("injected partial upload failure")
			}
			return nil
		},
	}
	upFail, err := NewGroupedUploader(failingHook, Options{GroupSize: 4, Streams: 2})
	if err != nil {
		t.Fatal(err)
	}
	items3 := append([]Item(nil), items2[:4]...)
	for i := range 8 {
		items3 = append(items3, Item{
			Key:  fmt.Sprintf("pack/ff/fail-%04d", i),
			Data: make([]byte, 4096),
		})
	}
	plan3, err := upFail.Planner().PlanCheckpoint(3, items3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := upFail.UploadAndCommit(ctx, plan3, 1); err == nil {
		t.Fatal("expected injected partial failure")
	}

	// Verify Checkpoint 2 restores 100% cleanly after aborted upload
	down, _ := NewGroupedDownloader(cnt, Options{GroupSize: 4, Streams: 4})
	got, _, err := down.DownloadCheckpoint(ctx, nil)
	if err != nil {
		t.Fatalf("DownloadCheckpoint after aborted upload failed: %v", err)
	}
	for _, it := range items2 {
		if !bytes.Equal(got[it.Key], it.Data) {
			t.Fatalf("checkpoint 2 data corrupted for %s after concurrent abort (m1 uploaded=%d)", it.Key, m1.Wire.BytesUploaded)
		}
	}
}

func TestAITURBO412CASFencingRealS3(t *testing.T) {
	ctx := context.Background()
	_, s3be := openS3HTTPBackend(t, "disk", "cas")
	cntA := NewCountingBackend(s3be, 0)
	cntB := NewCountingBackend(s3be, 0)

	upA, err := NewGroupedUploader(cntA, Options{GroupSize: 4, Streams: 4})
	if err != nil {
		t.Fatal(err)
	}
	itemsA := makeTestItems(8, 2048, "writera")
	planA, err := upA.Planner().PlanCheckpoint(1, itemsA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := upA.UploadAndCommit(ctx, planA, 1); err != nil {
		t.Fatalf("writerA initial commit: %v", err)
	}

	// Writer B takes over at Round 2 via fence.Claim (HTTP 412 fencing)
	upB, err := NewGroupedUploader(cntB, Options{GroupSize: 4, Streams: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := upB.HeadStore().Claim(ctx, headKey, 2, nil); err != nil {
		t.Fatalf("writerB claim round 2: %v", err)
	}

	// Writer A attempts another commit at stale Round 1 -> MUST receive 412 conflict -> ErrStaleRound
	itemsA2 := makeTestItems(8, 2048, "stalea")
	planA2, err := upA.Planner().PlanCheckpoint(2, itemsA2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := upA.UploadAndCommit(ctx, planA2, 1); !errors.Is(err, fence.ErrStaleRound) {
		t.Fatalf("stale writer got %v, want fence.ErrStaleRound", err)
	}
	if cntA.Snapshot().Conflicts412 == 0 {
		t.Fatalf("expected real HTTP 412 ErrConflict recorded on wire during stale CAS attempt")
	}

	// Verify committed Checkpoint A remains active and untouched under Round 2
	down, _ := NewGroupedDownloader(cntB, Options{GroupSize: 4, Streams: 4})
	got, _, err := down.DownloadCheckpoint(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range itemsA {
		if !bytes.Equal(got[it.Key], it.Data) {
			t.Fatalf("fenced writer corrupted %s", it.Key)
		}
	}
}

func TestAITURBOInjectedPartialFailureCancellationAndNoGoroutineLeak(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()

	// Commit clean baseline checkpoint 1 first
	up, _ := NewGroupedUploader(mem, Options{GroupSize: 4, Streams: 4})
	items1 := makeTestItems(8, 2048, "stable")
	plan1, _ := up.Planner().PlanCheckpoint(1, items1)
	if _, err := up.UploadAndCommit(ctx, plan1, 1); err != nil {
		t.Fatal(err)
	}

	// Inject error on Head CAS write (PutIfVersion) and verify prior Head stays intact
	headHook := &storetest.Hook{
		Backend: mem,
		Before: func(op, key string) error {
			if key == headKey && op == "putver" {
				return errors.New("injected head CAS failure")
			}
			return nil
		},
	}
	upHeadFail, _ := NewGroupedUploader(headHook, Options{GroupSize: 4, Streams: 4})
	plan2, _ := upHeadFail.Planner().PlanCheckpoint(2, makeTestItems(8, 2048, "headfail"))
	if _, err := upHeadFail.UploadAndCommit(ctx, plan2, 1); err == nil {
		t.Fatal("expected head commit failure")
	}

	// Cancellation mid-upload over slow hook + goroutine leak check
	baseGoroutines := runtime.NumGoroutine()
	cctx, cancel := context.WithCancel(ctx)
	var started sync.WaitGroup
	started.Add(1)
	var once sync.Once
	cancelHook := &storetest.Hook{
		Backend: mem,
		Before: func(op, key string) error {
			if op == "putabsent" {
				once.Do(started.Done)
				time.Sleep(15 * time.Millisecond)
			}
			return nil
		},
	}
	upCancel, _ := NewGroupedUploader(cancelHook, Options{GroupSize: 2, Streams: 4})
	plan3, _ := upCancel.Planner().PlanCheckpoint(3, makeTestItems(16, 1024, "cancel"))
	errCh := make(chan error, 1)
	go func() {
		_, e := upCancel.UploadAndCommit(cctx, plan3, 1)
		errCh <- e
	}()
	started.Wait()
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}

	// Allow worker teardown to settle and ensure zero goroutine leaks
	time.Sleep(30 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > baseGoroutines+2 {
		t.Fatalf("goroutine leak suspected: before=%d after=%d", baseGoroutines, after)
	}

	// Verify Checkpoint 1 still restores intact
	down, _ := NewGroupedDownloader(mem, Options{GroupSize: 4, Streams: 4})
	got, _, err := down.DownloadCheckpoint(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items1 {
		if !bytes.Equal(got[it.Key], it.Data) {
			t.Fatalf("stable checkpoint lost data for %s", it.Key)
		}
	}
}
