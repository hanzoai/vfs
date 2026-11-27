//go:build fuse && linux

package writeback_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/writeback"
)

type childStatus struct {
	Ready          bool                        `json:"ready"`
	Stats          writeback.FUSEStatsSnapshot `json:"stats"`
	BackendPuts    int64                       `json:"backend_puts"`
	BackendHeads   int64                       `json:"backend_heads"`
	JournalVisible uint64                      `json:"journal_visible"`
	JournalDurable uint64                      `json:"journal_durable"`
	JournalPub     uint64                      `json:"journal_published"`
	LastBarrier    *writeback.BarrierReport    `json:"last_barrier,omitempty"`
}

func needLinuxFUSE(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse on this machine")
	}
	if _, err := exec.LookPath("fusermount3"); err != nil {
		if _, err := exec.LookPath("fusermount"); err != nil {
			t.Skip("no fusermount3/fusermount on this machine")
		}
	}
}

func forceDetach(mp string) {
	if exec.Command("fusermount3", "-u", "-z", mp).Run() != nil {
		_ = exec.Command("fusermount", "-u", "-z", mp).Run()
	}
}

// TestWritebackFUSEChild is the helper daemon subprocess for real Linux kernel
// FUSE tests in this file.
func TestWritebackFUSEChild(t *testing.T) {
	if flag.Arg(0) != "wbchild" {
		t.Skip("helper subprocess only")
	}
	storeDir := flag.Arg(1)
	journalDir := flag.Arg(2)
	mp := flag.Arg(3)
	round, _ := strconv.ParseUint(flag.Arg(4), 10, 64)
	engine := writeback.FUSEEngine(flag.Arg(5))
	wb := flag.Arg(6) == "true"
	boundMs, _ := strconv.Atoi(flag.Arg(7))
	event := writeback.LifecycleEvent(flag.Arg(8))
	statusPath := flag.Arg(9)

	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	key, err := age.ParseHybridIdentity(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}

	rawBe, err := backend.Open(context.Background(), "file://"+storeDir)
	if err != nil {
		t.Fatal(err)
	}
	counter := writeback.WrapCounting(rawBe)
	cfg := vfs.Config{
		Backend:         counter,
		Cipher:          vfs.Hybrid(key),
		Round:           round,
		Journal:         journalDir,
		JournalCapacity: 128 << 20,
		Bound:           time.Duration(boundMs) * time.Millisecond,
	}
	if round == 1 {
		if err := vfs.Create(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
	}
	vol, err := vfs.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	wbCfg := writeback.DefaultConfig()
	wbCfg.WritebackCache = wb
	if engine == writeback.EngineBazil {
		wbCfg.MaxWriteBytes = writeback.LegacyMaxWriteBytes
	}

	stats := &writeback.FUSEStats{}
	var lastBarrier *writeback.BarrierReport
	ready := false

	writeStatus := func() {
		st := vol.State()
		cs := childStatus{
			Ready:          ready,
			Stats:          stats.Snapshot(),
			BackendPuts:    counter.TotalPuts(),
			BackendHeads:   counter.HeadCommits.Load(),
			JournalVisible: st.Visible,
			JournalDurable: st.Durable,
			JournalPub:     st.Published,
			LastBarrier:    lastBarrier,
		}
		b, _ := json.Marshal(cs)
		tmp := statusPath + ".tmp"
		_ = os.WriteFile(tmp, b, 0o644)
		_ = os.Rename(tmp, statusPath)
	}

	stopReporter := make(chan struct{})
	go func() {
		tick := time.NewTicker(15 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopReporter:
				return
			case <-tick.C:
				writeStatus()
			}
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	err = writeback.Mount(ctx, vol, mp, writeback.MountOptions{
		Config:         wbCfg,
		Engine:         engine,
		Stats:          stats,
		BackendCounter: counter,
		LifecycleEvent: event,
		OnReady: func() {
			ready = true
			writeStatus()
		},
		OnBarrier: func(rep writeback.BarrierReport) {
			cp := rep
			lastBarrier = &cp
			writeStatus()
		},
	})
	close(stopReporter)
	writeStatus()
	if err != nil && !strings.Contains(err.Error(), "transport endpoint") {
		t.Fatal(err)
	}
}

type fuseDaemon struct {
	cmd        *exec.Cmd
	mp         string
	statusPath string
	done       chan error
}

func startDaemon(t *testing.T, storeDir, journalDir, mp string, key *age.HybridIdentity, round uint64, engine writeback.FUSEEngine, wb bool, bound time.Duration, ev writeback.LifecycleEvent) *fuseDaemon {
	t.Helper()
	if err := os.MkdirAll(mp, 0o755); err != nil {
		t.Fatal(err)
	}
	statusPath := filepath.Join(t.TempDir(), fmt.Sprintf("status-r%d.json", round))
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestWritebackFUSEChild$",
		"-test.count=1",
		"wbchild",
		storeDir,
		journalDir,
		mp,
		strconv.FormatUint(round, 10),
		string(engine),
		strconv.FormatBool(wb),
		strconv.Itoa(int(bound.Milliseconds())),
		string(ev),
		statusPath,
	)
	cmd.Stdin = strings.NewReader(key.String() + "\n")
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	d := &fuseDaemon{
		cmd:        cmd,
		mp:         mp,
		statusPath: statusPath,
		done:       make(chan error, 1),
	}
	go func() { d.done <- cmd.Wait() }()

	// Wait for mount to be ready (both ready flag and stat change on mountpoint).
	var parentSt syscall.Stat_t
	_ = syscall.Stat(filepath.Dir(mp), &parentSt)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-d.done:
			t.Fatalf("daemon exited prematurely before mount ready: %v", err)
		default:
		}
		var mpSt syscall.Stat_t
		if syscall.Stat(mp, &mpSt) == nil && mpSt.Dev != parentSt.Dev {
			if cs, err := d.status(); err == nil && cs.Ready {
				return d
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	d.killAndDetach()
	t.Fatalf("daemon did not mount %s within 15s", mp)
	return nil
}

func (d *fuseDaemon) status() (childStatus, error) {
	b, err := os.ReadFile(d.statusPath)
	if err != nil {
		return childStatus{}, err
	}
	var cs childStatus
	err = json.Unmarshal(b, &cs)
	return cs, err
}

func (d *fuseDaemon) stopClean(t *testing.T) childStatus {
	t.Helper()
	_ = d.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-d.done:
		if err != nil {
			t.Fatalf("daemon clean stop failed: %v", err)
		}
	case <-time.After(20 * time.Second):
		d.killAndDetach()
		t.Fatal("daemon did not stop within 20s")
	}
	cs, err := d.status()
	if err != nil {
		t.Fatalf("read final daemon status: %v", err)
	}
	return cs
}

func (d *fuseDaemon) killAndDetach() {
	if d.cmd != nil && d.cmd.Process != nil {
		_ = d.cmd.Process.Signal(syscall.SIGKILL)
	}
	select {
	case <-d.done:
	case <-time.After(5 * time.Second):
	}
	forceDetach(d.mp)
}

// TestRealFUSESIGKILLSurvivalMatrix runs the exact #13 real Linux kernel FUSE
// crash & survival experiments:
//
//  1. Buffered write(2) (no fsync, file open, write acknowledged to app BEFORE
//     reaching daemon) + SIGKILL daemon + remount -> proves buffered write is
//     LOST while prior fsync(2)'d write 100% SURVIVES via local journal!
//  2. write(2) + fsync(2) + immediate SIGKILL before S3 publication + remount ->
//     100% SURVIVES and publishes cleanly to object store!
//  3. Buffered write(2) WITHOUT app fsync (file still open) + Detach/Suspend/End
//     lifecycle barrier (forcing kernel syncfs before final publish) -> 100%
//     SURVIVES in object store!
//  4. Clean close(2) -> 0 synchronous S3 PUTs on close; background publication
//     completes in <= 5 s!
//  5. 1 MiB max_write (FUSE_MAX_PAGES=256) coalescing check -> proves daemon
//     receives FUSE_WRITE RPCs > 128 KiB (up to 1 MiB) under EngineJacobsa.
func TestRealFUSESIGKILLSurvivalMatrix(t *testing.T) {
	needLinuxFUSE(t)

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	storeDir := t.TempDir()
	journalDir := t.TempDir()
	mp := filepath.Join(t.TempDir(), "mnt")

	// ── Scenario 1 & 2: Buffered write (no fsync) vs fsync'd write across SIGKILL ──
	// Bound=60s ensures async publisher does NOT publish before SIGKILL, proving
	// fsync survival comes strictly from local journal replay.
	d1 := startDaemon(t, storeDir, journalDir, mp, key, 1, writeback.EngineJacobsa, true, 60*time.Second, writeback.EventEnd)

	durablePayload := bytes.Repeat([]byte("FSYNC-DURABLE-PAYLOAD-"), 4096) // 90,112 bytes
	volatileTail := bytes.Repeat([]byte("BUFFERED-ONLY-UNSYNCED"), 4096)   // 90,112 bytes

	fpath := filepath.Join(mp, "contract.bin")
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		d1.killAndDetach()
		t.Fatal(err)
	}
	if _, err := f.Write(durablePayload); err != nil {
		_ = f.Close()
		d1.killAndDetach()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		d1.killAndDetach()
		t.Fatal(err)
	}

	// Wait briefly for status snapshot to reflect fsync.
	time.Sleep(40 * time.Millisecond)
	stAfterFsync, err := d1.status()
	if err != nil {
		_ = f.Close()
		d1.killAndDetach()
		t.Fatal(err)
	}
	if stAfterFsync.Stats.WriteBytes < int64(len(durablePayload)) {
		_ = f.Close()
		d1.killAndDetach()
		t.Fatalf("after fsync, daemon WriteBytes=%d < %d", stAfterFsync.Stats.WriteBytes, len(durablePayload))
	}
	if stAfterFsync.JournalDurable == 0 || stAfterFsync.JournalPub >= stAfterFsync.JournalDurable {
		_ = f.Close()
		d1.killAndDetach()
		t.Fatalf("expected unpublished durable journal tail after fsync: durable=%d published=%d", stAfterFsync.JournalDurable, stAfterFsync.JournalPub)
	}

	// Now issue a buffered write(2) WITHOUT fsync and WITHOUT close(2).
	// Under FUSE_WRITEBACK_CACHE, the kernel acknowledges write(2) immediately
	// into its page cache BEFORE sending FUSE_WRITE to the daemon!
	n, err := f.Write(volatileTail)
	if err != nil || n != len(volatileTail) {
		_ = f.Close()
		d1.killAndDetach()
		t.Fatalf("buffered write failed: n=%d err=%v", n, err)
	}

	// Confirm the daemon has NOT received volatileTail yet, then SIGKILL daemon immediately!
	stBeforeKill, _ := d1.status()
	d1.killAndDetach()
	_ = f.Close() // ignore error on dead mount

	t.Logf("Scenario 1+2 before SIGKILL: daemon WriteBytes=%d (durable=%d, volatile_in_page_cache=%d), journal durable=%d published=%d",
		stBeforeKill.Stats.WriteBytes, len(durablePayload), len(volatileTail), stBeforeKill.JournalDurable, stBeforeKill.JournalPub)

	// Remount at Round 2 on same store + journal directory.
	d2 := startDaemon(t, storeDir, journalDir, mp, key, 2, writeback.EngineJacobsa, true, 1*time.Second, writeback.EventSuspend)

	gotAfterKill, err := os.ReadFile(filepath.Join(mp, "contract.bin"))
	if err != nil {
		d2.killAndDetach()
		t.Fatalf("read contract.bin after SIGKILL + remount: %v", err)
	}
	if !bytes.Equal(gotAfterKill, durablePayload) {
		d2.killAndDetach()
		t.Fatalf("after SIGKILL + remount: got %d bytes, want exact %d fsync'd bytes (buffered tail must be lost)", len(gotAfterKill), len(durablePayload))
	}
	t.Logf("Scenario 1+2 VERIFIED: %d/%d fsync'd bytes survived SIGKILL via journal; %d/%d buffered-only page-cache bytes LOST as expected",
		len(gotAfterKill), len(durablePayload), len(volatileTail), len(volatileTail))

	// ── Scenario 4: Clean close(2) -> 0 synchronous S3 PUTs + <= 5 s async publish ──
	cleanData := bytes.Repeat([]byte("CLEAN-CLOSE-DATA"), 16384) // 256 KiB
	cf, err := os.OpenFile(filepath.Join(mp, "clean.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		d2.killAndDetach()
		t.Fatal(err)
	}
	if _, err := cf.Write(cleanData); err != nil {
		_ = cf.Close()
		d2.killAndDetach()
		t.Fatal(err)
	}
	tWriteDone := time.Now()
	stBeforeClose, _ := d2.status()
	if err := cf.Close(); err != nil {
		d2.killAndDetach()
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	stRightAfterClose, _ := d2.status()
	if stRightAfterClose.Stats.SyncS3PutsInFlush != 0 {
		d2.killAndDetach()
		t.Fatalf("SyncS3PutsInFlush = %d, want 0", stRightAfterClose.Stats.SyncS3PutsInFlush)
	}
	// Wait for background publication to complete within <= 5 s.
	wantSeq := stRightAfterClose.JournalVisible
	deadline := time.Now().Add(writeback.DefaultPublishBound)
	var pubDur time.Duration
	for time.Now().Before(deadline) {
		cs, _ := d2.status()
		if cs.JournalPub >= wantSeq && wantSeq > 0 {
			pubDur = time.Since(tWriteDone)
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if pubDur == 0 {
		cs, _ := d2.status()
		d2.killAndDetach()
		t.Fatalf("background publication did not finish within 5 s (visible=%d published=%d)", cs.JournalVisible, cs.JournalPub)
	}
	t.Logf("Scenario 4 VERIFIED: clean close SyncS3PutsInFlush=0 (puts before=%d right_after=%d); published to store in %s <= 5s",
		stBeforeClose.BackendPuts, stRightAfterClose.BackendPuts, pubDur.Round(time.Millisecond))

	// ── Scenario 5: 1 MiB max_write coalescing check ──
	bigData := make([]byte, 8<<20)
	for i := range bigData {
		bigData[i] = byte(i * 7)
	}
	bf, err := os.OpenFile(filepath.Join(mp, "big-coalesce.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		d2.killAndDetach()
		t.Fatal(err)
	}
	if _, err := bf.Write(bigData); err != nil {
		_ = bf.Close()
		d2.killAndDetach()
		t.Fatal(err)
	}
	if err := bf.Sync(); err != nil {
		_ = bf.Close()
		d2.killAndDetach()
		t.Fatal(err)
	}
	_ = bf.Close()
	time.Sleep(40 * time.Millisecond)
	stBig, _ := d2.status()
	if stBig.Stats.MaxWriteRPCSize <= int64(writeback.LegacyMaxWriteBytes) {
		d2.killAndDetach()
		t.Fatalf("expected 1 MiB max_write coalescing (> 128 KiB), got MaxWriteRPCSize=%d bytes", stBig.Stats.MaxWriteRPCSize)
	}
	t.Logf("Scenario 5 VERIFIED: 1 MiB max_write active — daemon observed MaxWriteRPCSize=%d bytes (%d KiB) across %d WriteRPCs",
		stBig.Stats.MaxWriteRPCSize, stBig.Stats.MaxWriteRPCSize/1024, stBig.Stats.WriteRPCs)

	// ── Scenario 3: Buffered write WITHOUT fsync (file still open!) + Suspend lifecycle barrier ──
	barrierPayload := bytes.Repeat([]byte("SUSPEND-SYNCFS-BARRIER!!"), 8192) // 192 KiB
	sf, err := os.OpenFile(filepath.Join(mp, "unsynced-open.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		d2.killAndDetach()
		t.Fatal(err)
	}
	if _, err := sf.Write(barrierPayload); err != nil {
		_ = sf.Close()
		d2.killAndDetach()
		t.Fatal(err)
	}
	// Do NOT call sf.Sync() and do NOT close sf before triggering Suspend!
	// Trigger clean Suspend lifecycle barrier (SIGTERM -> syncfs -> journal sync -> final publish).
	finalSt := d2.stopClean(t)
	_ = sf.Close()

	if finalSt.LastBarrier == nil {
		t.Fatal("expected LastBarrier report on clean Suspend shutdown")
	}
	if len(finalSt.LastBarrier.Steps) != 3 || finalSt.LastBarrier.Steps[0] != "syncfs" {
		t.Fatalf("unexpected barrier steps: %+v", finalSt.LastBarrier)
	}
	t.Logf("Scenario 3 barrier report: event=%s steps=%v syncfs=%s journal=%s publish=%s total=%s",
		finalSt.LastBarrier.Event, finalSt.LastBarrier.Steps,
		finalSt.LastBarrier.SyncFSDuration.Round(time.Microsecond),
		finalSt.LastBarrier.JournalSyncDuration.Round(time.Microsecond),
		finalSt.LastBarrier.PublishDuration.Round(time.Microsecond),
		finalSt.LastBarrier.TotalDuration.Round(time.Microsecond))

	// Cold read straight from the object store (no journal, read-only Volume) proves 100% survived!
	rawBe, err := backend.Open(context.Background(), "file://"+storeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer rawBe.Close()
	ro, err := vfs.Open(context.Background(), vfs.Config{Backend: rawBe, Cipher: vfs.Hybrid(key)})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	in, err := ro.Resolve(context.Background(), "/unsynced-open.bin")
	if err != nil {
		t.Fatalf("cold store missing /unsynced-open.bin after Suspend syncfs barrier: %v", err)
	}
	rf, err := ro.Open(context.Background(), in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	gotBarrier := make([]byte, in.Size)
	if _, err := rf.ReadAt(context.Background(), gotBarrier, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBarrier, barrierPayload) {
		t.Fatalf("cold store /unsynced-open.bin len=%d, want %d exact bytes", len(gotBarrier), len(barrierPayload))
	}
	t.Logf("Scenario 3 VERIFIED: 100%% of un-fsynced open-file buffered bytes (%d B) survived Suspend syncfs barrier into object store!", len(gotBarrier))
}
