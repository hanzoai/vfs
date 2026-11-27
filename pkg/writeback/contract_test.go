package writeback_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hanzoai/vfs/pkg/writeback"
)

func TestWritebackConfigValidation(t *testing.T) {
	def := writeback.DefaultConfig()
	valid, err := def.Validate()
	if err != nil {
		t.Fatalf("DefaultConfig().Validate(): %v", err)
	}
	if valid.MaxWriteBytes != 1<<20 {
		t.Fatalf("MaxWriteBytes = %d, want %d (1 MiB)", valid.MaxWriteBytes, 1<<20)
	}
	if valid.MaxPages() != 256 {
		t.Fatalf("MaxPages() = %d, want 256", valid.MaxPages())
	}
	if valid.PublishBound != 5*time.Second {
		t.Fatalf("PublishBound = %s, want 5s", valid.PublishBound)
	}
	if !valid.WritebackCache || !valid.SyncFSOnLifecycle || !valid.ZeroSyncS3OnClose {
		t.Fatalf("unexpected default flags: %+v", valid)
	}

	cases := []struct {
		name string
		cfg  writeback.WritebackConfig
	}{
		{"too small max_write", writeback.WritebackConfig{MaxWriteBytes: 1024}},
		{"too large max_write", writeback.WritebackConfig{MaxWriteBytes: 2 << 20}},
		{"unaligned max_write", writeback.WritebackConfig{MaxWriteBytes: 6000}},
		{"dirty window smaller than max_write", writeback.WritebackConfig{MaxWriteBytes: 1 << 20, DirtyWindowBytes: 512 << 10}},
		{"negative expire", writeback.WritebackConfig{DirtyExpireDuration: -1 * time.Second}},
		{"negative publish bound", writeback.WritebackConfig{PublishBound: -1 * time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.cfg.Validate(); !errors.Is(err, writeback.ErrInvalidConfig) {
				t.Fatalf("Validate(%s) = %v, want ErrInvalidConfig", tc.name, err)
			}
		})
	}
}

func TestDurabilityContractClockStartsAtJournalNotWrite(t *testing.T) {
	tr, err := writeback.NewContractTracker(writeback.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	t0 := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)

	// Buffered write(2) acknowledged by kernel page cache at t0.
	if err := tr.RecordKernelWrite(1, 42, 0, 64<<10, t0); err != nil {
		t.Fatal(err)
	}
	rec, ok := tr.Record(1)
	if !ok {
		t.Fatal("missing record 1")
	}
	if rec.State != writeback.BufferedInKernelPageCache {
		t.Fatalf("state = %s, want BufferedInKernelPageCache", rec.State)
	}
	if rec.State.SurvivesDaemonSIGKILL() {
		t.Fatal("BufferedInKernelPageCache MUST NOT claim SIGKILL survival")
	}
	if rec.State.StartsPublishClock() {
		t.Fatal("BufferedInKernelPageCache MUST NOT start the 5 s publish clock")
	}

	// Suppose the write lingered 12 seconds in Linux kernel page cache before
	// the app called fsync(2) or writeback flushed it (exceeding 5 s from write(2)!).
	tJournal := t0.Add(12 * time.Second)
	if err := tr.RecordDaemonJournal(1, 1, tJournal); err != nil {
		t.Fatal(err)
	}
	tFsync := tJournal.Add(2 * time.Millisecond)
	if err := tr.RecordFsyncDurable(1, tFsync); err != nil {
		t.Fatal(err)
	}

	// Published 2.5 seconds after journal entry (14.5 s after write(2)).
	// Because the contract measures from journal entry / fsync (NOT buffered write(2)),
	// this MUST satisfy the 5 s PublishBound with zero SLA violations!
	tPub := tJournal.Add(2500 * time.Millisecond)
	if err := tr.RecordPublished(1, tPub); err != nil {
		t.Fatalf("RecordPublished should pass 5 s SLA from journal entry, got: %v", err)
	}

	rec, _ = tr.Record(1)
	if rec.State != writeback.PublishedToObjectStore {
		t.Fatalf("state = %s, want PublishedToObjectStore", rec.State)
	}
	if got := rec.KernelBufferLag(); got != 12*time.Second {
		t.Fatalf("KernelBufferLag = %s, want 12s", got)
	}
	if got := rec.PublishLatencyFromJournal(); got != 2500*time.Millisecond {
		t.Fatalf("PublishLatencyFromJournal = %s, want 2.5s", got)
	}
	if got := rec.PublishLatencyFromFsync(); got != 2498*time.Millisecond {
		t.Fatalf("PublishLatencyFromFsync = %s, want 2.498s", got)
	}

	// Now verify that exceeding 5 s AFTER journal entry DOES trigger ErrPublishBoundExceeded.
	if err := tr.RecordKernelWrite(2, 42, 64<<10, 4096, tPub); err != nil {
		t.Fatal(err)
	}
	if err := tr.RecordDaemonJournal(2, 2, tPub.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := tr.RecordFsyncDurable(2, tPub.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := tr.RecordPublished(2, tPub.Add(5200*time.Millisecond)); !errors.Is(err, writeback.ErrPublishBoundExceeded) {
		t.Fatalf("expected ErrPublishBoundExceeded, got %v", err)
	}

	snap := tr.Snapshot()
	if snap.PublishSLAViolations != 1 {
		t.Fatalf("PublishSLAViolations = %d, want 1", snap.PublishSLAViolations)
	}
}

func TestContractRejectsInvalidTransitionsAndZeroSyncS3OnClose(t *testing.T) {
	tr, err := writeback.NewContractTracker(writeback.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := tr.RecordKernelWrite(1, 10, 0, 4096, now); err != nil {
		t.Fatal(err)
	}
	if err := tr.RecordDaemonJournal(1, 1, now); err != nil {
		t.Fatal(err)
	}
	// Publishing before FsyncDurable MUST fail (cannot publish undurable journal records).
	if err := tr.RecordPublished(1, now); !errors.Is(err, writeback.ErrInvalidTransition) {
		t.Fatalf("publishing undurable seq returned %v, want ErrInvalidTransition", err)
	}
	// Regressing journal sequence MUST fail.
	if err := tr.RecordKernelWrite(2, 10, 4096, 4096, now); err != nil {
		t.Fatal(err)
	}
	if err := tr.RecordDaemonJournal(2, 5, now); err != nil {
		t.Fatal(err)
	}
	if err := tr.RecordKernelWrite(3, 10, 8192, 4096, now); err != nil {
		t.Fatal(err)
	}
	if err := tr.RecordDaemonJournal(3, 4, now); !errors.Is(err, writeback.ErrInvalidTransition) {
		t.Fatalf("regressed seq returned %v, want ErrInvalidTransition", err)
	}

	// Clean close with 0 synchronous S3 PUTs passes; >0 fails.
	if err := tr.VerifyCleanClose(0); err != nil {
		t.Fatalf("VerifyCleanClose(0) failed: %v", err)
	}
	if err := tr.VerifyCleanClose(1); !errors.Is(err, writeback.ErrSyncS3OnCloseViolation) {
		t.Fatalf("VerifyCleanClose(1) = %v, want ErrSyncS3OnCloseViolation", err)
	}
}

func TestDirtyExposureBoundsAndSIGKILLSimulation(t *testing.T) {
	cfg := writeback.DefaultConfig()
	cfg.DirtyWindowBytes = 16 << 20
	cfg.DirtyExpireDuration = 500 * time.Millisecond
	tr, err := writeback.NewContractTracker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)

	// Record 1: published to object store (seq 1 -> must survive SIGKILL).
	_ = tr.RecordKernelWrite(1, 1, 0, 2<<20, t0)
	_ = tr.RecordDaemonJournal(1, 1, t0.Add(time.Millisecond))
	_ = tr.RecordFsyncDurable(1, t0.Add(2*time.Millisecond))
	_ = tr.RecordPublished(1, t0.Add(50*time.Millisecond))

	// Record 2: fsync durable in local journal, not yet published (seq 2 -> must survive SIGKILL).
	_ = tr.RecordKernelWrite(2, 1, 2<<20, 1<<20, t0.Add(60*time.Millisecond))
	_ = tr.RecordDaemonJournal(2, 2, t0.Add(61*time.Millisecond))
	_ = tr.RecordFsyncDurable(2, t0.Add(62*time.Millisecond))

	// Record 3: still buffered in kernel page cache (16 MiB) -> exceeds byte window & LOST on SIGKILL.
	_ = tr.RecordKernelWrite(3, 2, 0, 16<<20, t0.Add(100*time.Millisecond))

	exp := tr.DirtyExposure(t0.Add(200 * time.Millisecond))
	if !exp.ExceedsBytes || exp.ExceedsAge || !exp.NeedsFlush() {
		t.Fatalf("unexpected DirtyExposure at +100ms: %+v", exp)
	}
	expAge := tr.DirtyExposure(t0.Add(700 * time.Millisecond))
	if !expAge.ExceedsAge {
		t.Fatalf("expected ExceedsAge at +600ms: %+v", expAge)
	}

	out := tr.SimulateDaemonSIGKILL()
	if out.LostKernelBufferedCount != 1 || out.LostKernelBufferedBytes != 16<<20 {
		t.Fatalf("unexpected lost buffered: %+v", out)
	}
	if out.SurvivedFsyncCount != 1 || out.SurvivedFsyncBytes != 1<<20 {
		t.Fatalf("unexpected survived fsync: %+v", out)
	}
	if out.SurvivedPublishedCount != 1 || out.SurvivedPublishedBytes != 2<<20 {
		t.Fatalf("unexpected survived published: %+v", out)
	}
}
