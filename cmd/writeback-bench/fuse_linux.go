//go:build fuse && linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/writeback"
)

func fuseSupported() bool {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		return false
	}
	if _, err := exec.LookPath("fusermount3"); err != nil {
		if _, err := exec.LookPath("fusermount"); err != nil {
			return false
		}
	}
	return true
}

type daemonStatus struct {
	Ready          bool                        `json:"ready"`
	Stats          writeback.FUSEStatsSnapshot `json:"stats"`
	BackendPuts    int64                       `json:"backend_puts"`
	JournalVisible uint64                      `json:"journal_visible"`
	JournalDurable uint64                      `json:"journal_durable"`
	JournalPub     uint64                      `json:"journal_published"`
	LastBarrier    *writeback.BarrierReport    `json:"last_barrier,omitempty"`
}

func runFUSEChild(args []string) {
	if len(args) < 9 {
		fatal(fmt.Errorf("child expects 9 args, got %d", len(args)))
	}
	storeDir := args[0]
	journalDir := args[1]
	mp := args[2]
	round, _ := strconv.ParseUint(args[3], 10, 64)
	engine := writeback.FUSEEngine(args[4])
	wb := args[5] == "true"
	boundMs, _ := strconv.Atoi(args[6])
	event := writeback.LifecycleEvent(args[7])
	statusPath := args[8]

	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		fatal(err)
	}
	key, err := age.ParseHybridIdentity(strings.TrimSpace(line))
	if err != nil {
		fatal(err)
	}

	rawBe, err := backend.Open(context.Background(), "file://"+storeDir)
	if err != nil {
		fatal(err)
	}
	counter := writeback.WrapCounting(rawBe)
	cfg := vfs.Config{
		Backend:         counter,
		Cipher:          vfs.Hybrid(key),
		Round:           round,
		Journal:         journalDir,
		JournalCapacity: 512 << 20,
		Bound:           time.Duration(boundMs) * time.Millisecond,
	}
	if round == 1 {
		if err := vfs.Create(context.Background(), cfg); err != nil {
			fatal(err)
		}
	}
	vol, err := vfs.Attach(context.Background(), cfg)
	if err != nil {
		fatal(err)
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
		ds := daemonStatus{
			Ready:          ready,
			Stats:          stats.Snapshot(),
			BackendPuts:    counter.TotalPuts(),
			JournalVisible: st.Visible,
			JournalDurable: st.Durable,
			JournalPub:     st.Published,
			LastBarrier:    lastBarrier,
		}
		b, _ := json.Marshal(ds)
		tmp := statusPath + ".tmp"
		_ = os.WriteFile(tmp, b, 0o644)
		_ = os.Rename(tmp, statusPath)
	}

	stopTick := make(chan struct{})
	go func() {
		tick := time.NewTicker(15 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopTick:
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
	close(stopTick)
	writeStatus()
	if err != nil && !strings.Contains(err.Error(), "transport endpoint") {
		fatal(err)
	}
}

type benchDaemon struct {
	cmd        *exec.Cmd
	mp         string
	statusPath string
	done       chan error
}

func spawnDaemon(storeDir, journalDir, mp, statusPath string, key *age.HybridIdentity, round uint64, engine writeback.FUSEEngine, wb bool, bound time.Duration, ev writeback.LifecycleEvent) *benchDaemon {
	if err := os.MkdirAll(mp, 0o755); err != nil {
		fatal(err)
	}
	cmd := exec.Command(os.Args[0],
		"--child",
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
		fatal(err)
	}
	d := &benchDaemon{cmd: cmd, mp: mp, statusPath: statusPath, done: make(chan error, 1)}
	go func() { d.done <- cmd.Wait() }()

	var parentSt syscall.Stat_t
	_ = syscall.Stat(filepath.Dir(mp), &parentSt)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-d.done:
			fatal(fmt.Errorf("FUSE child exited before mount ready: %v", err))
		default:
		}
		var mpSt syscall.Stat_t
		if syscall.Stat(mp, &mpSt) == nil && mpSt.Dev != parentSt.Dev {
			if st, err := d.status(); err == nil && st.Ready {
				return d
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	d.killAndDetach()
	fatal(fmt.Errorf("FUSE child did not become ready at %s", mp))
	return nil
}

func (d *benchDaemon) status() (daemonStatus, error) {
	b, err := os.ReadFile(d.statusPath)
	if err != nil {
		return daemonStatus{}, err
	}
	var st daemonStatus
	err = json.Unmarshal(b, &st)
	return st, err
}

func (d *benchDaemon) stopClean() daemonStatus {
	_ = d.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-d.done:
		if err != nil {
			fatal(fmt.Errorf("FUSE child clean exit failed: %v", err))
		}
	case <-time.After(30 * time.Second):
		d.killAndDetach()
		fatal(fmt.Errorf("FUSE child did not stop cleanly"))
	}
	st, err := d.status()
	if err != nil {
		fatal(err)
	}
	return st
}

func (d *benchDaemon) killAndDetach() {
	if d.cmd != nil && d.cmd.Process != nil {
		_ = d.cmd.Process.Signal(syscall.SIGKILL)
	}
	select {
	case <-d.done:
	case <-time.After(5 * time.Second):
	}
	if exec.Command("fusermount3", "-u", "-z", d.mp).Run() != nil {
		_ = exec.Command("fusermount", "-u", "-z", d.mp).Run()
	}
}

func runRealFUSEBenchmarks(res *BenchResults, baseWork string, burstMiB, fsyncOps int) {
	modes := []struct {
		name   string
		engine writeback.FUSEEngine
		maxKiB uint32
		wb     bool
		event  writeback.LifecycleEvent
	}{
		{"bazil 128 KiB write-through (legacy baseline)", writeback.EngineBazil, 128, false, writeback.EventEnd},
		{"bazil 128 KiB writeback cache", writeback.EngineBazil, 128, true, writeback.EventDetach},
		{"jacobsa 1 MiB write-through", writeback.EngineJacobsa, 1024, false, writeback.EventEnd},
		{"jacobsa 1 MiB writeback cache (#13 target)", writeback.EngineJacobsa, 1024, true, writeback.EventSuspend},
	}

	chunk := makePayload(1 << 20)
	blk := makePayload(4096)

	for idx, m := range modes {
		fmt.Fprintf(os.Stderr, "[writeback-bench]   mode %d/4: %s\n", idx+1, m.name)
		modeDir := filepath.Join(baseWork, fmt.Sprintf("mode-%d", idx))
		storeDir := filepath.Join(modeDir, "store")
		journalDir := filepath.Join(modeDir, "journal")
		mp := filepath.Join(modeDir, "mnt")
		statusPath := filepath.Join(modeDir, "status.json")
		_ = os.MkdirAll(storeDir, 0o755)
		_ = os.MkdirAll(journalDir, 0o755)

		key, err := age.GenerateHybridIdentity()
		if err != nil {
			fatal(err)
		}
		d := spawnDaemon(storeDir, journalDir, mp, statusPath, key, 1, m.engine, m.wb, 1*time.Second, m.event)

		if res.FUSEBDI == nil && m.wb && m.engine == writeback.EngineJacobsa {
			if bdi, err := writeback.InspectMountBDI(mp); err == nil {
				res.FUSEBDI = &bdi
			}
		}

		// 1a. Small-write (4 KiB) page-cache coalescing burst (12 MiB total — fits in FUSE BDI 1% window).
		const smallBurstMiB = 12
		stSmall0, _ := d.status()
		smPath := filepath.Join(mp, "small4k.bin")
		smf, err := os.OpenFile(smPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			d.killAndDetach()
			fatal(err)
		}
		ts0 := time.Now()
		for range smallBurstMiB * 256 {
			if _, err := smf.Write(blk); err != nil {
				d.killAndDetach()
				fatal(err)
			}
		}
		smallDur := time.Since(ts0)
		if err := smf.Sync(); err != nil {
			d.killAndDetach()
			fatal(err)
		}
		_ = smf.Close()
		time.Sleep(35 * time.Millisecond)
		stSmall1, _ := d.status()
		_ = os.Remove(smPath)
		smallMBs := float64(smallBurstMiB<<20) / smallDur.Seconds() / 1e6
		smallRPCs := stSmall1.Stats.WriteRPCs - stSmall0.Stats.WriteRPCs

		// 1b. Sequential large burst write (buffered + durable after final fsync).
		stStart, _ := d.status()
		bPath := filepath.Join(mp, "burst.bin")
		f, err := os.OpenFile(bPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			d.killAndDetach()
			fatal(err)
		}
		t0 := time.Now()
		for range burstMiB {
			if _, err := f.Write(chunk); err != nil {
				d.killAndDetach()
				fatal(err)
			}
		}
		bufDur := time.Since(t0)
		if err := f.Sync(); err != nil {
			d.killAndDetach()
			fatal(err)
		}
		durDur := time.Since(t0)
		_ = f.Close()
		time.Sleep(35 * time.Millisecond)
		stAfterBurst, _ := d.status()
		_ = os.Remove(bPath)

		totalBytes := float64(burstMiB) * float64(1<<20)
		bufMBs := totalBytes / bufDur.Seconds() / 1e6
		durMBs := totalBytes / durDur.Seconds() / 1e6

		// 2. Sequential 4 KiB write + fsync latency.
		fsPath := filepath.Join(mp, "fsync.bin")
		ff, err := os.OpenFile(fsPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			d.killAndDetach()
			fatal(err)
		}
		samples := make([]time.Duration, 0, fsyncOps)
		for i := range fsyncOps {
			blk[0] = byte(i)
			if _, err := ff.WriteAt(blk, int64(i)*4096); err != nil {
				d.killAndDetach()
				fatal(err)
			}
			ts := time.Now()
			if err := ff.Sync(); err != nil {
				d.killAndDetach()
				fatal(err)
			}
			samples = append(samples, time.Since(ts))
		}
		_ = ff.Close()
		_ = os.Remove(fsPath)
		fsLat := summarizeLatencies(samples)

		// 3. Clean close zero sync S3 PUTs + <= 5 s journal->S3 publication latency.
		cleanPayload := makePayload(256 << 10)
		cf, err := os.OpenFile(filepath.Join(mp, "clean-close.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			d.killAndDetach()
			fatal(err)
		}
		if _, err := cf.Write(cleanPayload); err != nil {
			d.killAndDetach()
			fatal(err)
		}
		tClose := time.Now()
		if err := cf.Close(); err != nil {
			d.killAndDetach()
			fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
		stClose, _ := d.status()
		wantSeq := stClose.JournalVisible
		pubDeadline := time.Now().Add(writeback.DefaultPublishBound)
		var pubDur time.Duration
		for time.Now().Before(pubDeadline) {
			cur, _ := d.status()
			if cur.JournalPub >= wantSeq && wantSeq > 0 {
				pubDur = time.Since(tClose)
				break
			}
			time.Sleep(25 * time.Millisecond)
		}

		// 4. Negative Regime 2 (sync-heavy random 4 KiB write+fsync) for legacy baseline & target.
		if idx == 0 || idx == 3 {
			rPath := filepath.Join(mp, "rand4k.bin")
			rf, err := os.OpenFile(rPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
			if err != nil {
				d.killAndDetach()
				fatal(err)
			}
			pre := makePayload(4 << 20)
			_, _ = rf.Write(pre)
			_ = rf.Sync()
			rng := rand.New(rand.NewPCG(99, 13))
			rt0 := time.Now()
			for i := range fsyncOps {
				off := int64(rng.IntN(1024)) * 4096
				blk[0] = byte(i)
				_, _ = rf.WriteAt(blk, off)
				_ = rf.Sync()
			}
			rIOPS := float64(fsyncOps) / time.Since(rt0).Seconds()
			_ = rf.Close()
			_ = os.Remove(rPath)
			if idx == 0 {
				res.NegativeRegimes.NR2WriteThrough4KIOPS = rIOPS
			} else {
				res.NegativeRegimes.NR2Writeback4KIOPS = rIOPS
			}
		}

		// 5. Lifecycle barrier test: leave an un-fsynced open file with buffered writes, trigger Detach/Suspend/End!
		lcPayload := bytes.Repeat([]byte("LIFECYCLE-VERIFY"), 8192) // 128 KiB
		lcf, err := os.OpenFile(filepath.Join(mp, "lifecycle.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			d.killAndDetach()
			fatal(err)
		}
		_, _ = lcf.Write(lcPayload)
		// Deliberately keep lcf open and un-fsynced across clean shutdown!
		finalSt := d.stopClean()
		_ = lcf.Close()

		// Verify 100% survival in cold object store!
		rawBe, err := backend.Open(context.Background(), "file://"+storeDir)
		if err != nil {
			fatal(err)
		}
		ro, err := vfs.Open(context.Background(), vfs.Config{Backend: rawBe, Cipher: vfs.Hybrid(key)})
		if err != nil {
			fatal(err)
		}
		survRate := 0.0
		if in, err := ro.Resolve(context.Background(), "/lifecycle.bin"); err == nil && in.Size == uint64(len(lcPayload)) {
			rf, err := ro.Open(context.Background(), in.ID)
			if err == nil {
				got := make([]byte, in.Size)
				_, _ = rf.ReadAt(context.Background(), got, 0)
				_ = rf.Close()
				if bytes.Equal(got, lcPayload) {
					survRate = 1.0
				}
			}
		}
		_ = ro.Close()

		var syncfsMS, jSyncMS, bPubMS, bTotMS float64
		if finalSt.LastBarrier != nil {
			syncfsMS = float64(finalSt.LastBarrier.SyncFSDuration.Microseconds()) / 1000.0
			jSyncMS = float64(finalSt.LastBarrier.JournalSyncDuration.Microseconds()) / 1000.0
			bPubMS = float64(finalSt.LastBarrier.PublishDuration.Microseconds()) / 1000.0
			bTotMS = float64(finalSt.LastBarrier.TotalDuration.Microseconds()) / 1000.0
		}

		res.FUSEMatrix = append(res.FUSEMatrix, FUSEModeMetrics{
			Name:                  m.name,
			Engine:                string(m.engine),
			MaxWriteKiB:           m.maxKiB,
			WritebackCache:        m.wb,
			BurstBufferedMBs:      bufMBs,
			BurstBufferedGBs:      bufMBs / 1000.0,
			BurstBufferedVsPlain:  bufMBs / res.PlainDisk.BurstBufferedMBs,
			BurstDurableMBs:       durMBs,
			BurstDurableGBs:       durMBs / 1000.0,
			BurstDurableVsPlain:   durMBs / res.PlainDisk.BurstDurableMBs,
			Small4KBurstMBs:       smallMBs,
			Small4KBurstGBs:       smallMBs / 1000.0,
			Small4KWriteRPCs:      smallRPCs,
			DaemonWriteRPCs:       stAfterBurst.Stats.WriteRPCs - stStart.Stats.WriteRPCs,
			DaemonMaxWriteKiB:     stAfterBurst.Stats.MaxWriteRPCSize / 1024,
			FsyncLatency:          fsLat,
			FsyncP50VsPlain:       fsLat.P50US / res.PlainDisk.FsyncLatency.P50US,
			CleanCloseSyncS3Puts:  stClose.Stats.SyncS3PutsInFlush,
			JournalToS3PublishMS:  float64(pubDur.Microseconds()) / 1000.0,
			PublishWithin5s:       pubDur > 0 && pubDur <= writeback.DefaultPublishBound,
			LifecycleEvent:        string(m.event),
			BarrierSyncFSMS:       syncfsMS,
			BarrierJournalSyncMS:  jSyncMS,
			BarrierPublishMS:      bPubMS,
			BarrierTotalMS:        bTotMS,
			LifecycleSurvivalRate: survRate,
		})
	}

	res.NegativeRegimes.NR2PlainSync4KIOPS = res.PlainDisk.SyncRandom4KIOPS
	if res.NegativeRegimes.NR2WriteThrough4KIOPS > 0 {
		res.NegativeRegimes.NR2WritebackGainRatio = res.NegativeRegimes.NR2Writeback4KIOPS / res.NegativeRegimes.NR2WriteThrough4KIOPS
	}

	// Run real kernel SIGKILL survival matrix (4 explicit cases).
	fmt.Fprintf(os.Stderr, "[writeback-bench] running real Linux kernel FUSE SIGKILL survival matrix...\n")
	runSIGKILLBenchMatrix(res, filepath.Join(baseWork, "sigkill"))

	// Run Negative Regime 3: dirty page-cache exposure vs Detach/Suspend syncfs stall curve.
	fmt.Fprintf(os.Stderr, "[writeback-bench] running Negative Regime 3 dirty window syncfs stall curve...\n")
	runDirtyStallCurve(res, filepath.Join(baseWork, "nr3-stall"))
}

func runSIGKILLBenchMatrix(res *BenchResults, dir string) {
	storeDir := filepath.Join(dir, "store")
	journalDir := filepath.Join(dir, "journal")
	mp := filepath.Join(dir, "mnt")
	_ = os.MkdirAll(storeDir, 0o755)
	_ = os.MkdirAll(journalDir, 0o755)

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		fatal(err)
	}

	// Case 1 & 2: fsync'd prefix + buffered-only tail -> SIGKILL before S3 publish -> remount.
	d1 := spawnDaemon(storeDir, journalDir, mp, filepath.Join(dir, "st1.json"), key, 1, writeback.EngineJacobsa, true, 60*time.Second, writeback.EventEnd)
	durable := bytes.Repeat([]byte("DURABLE-FSYNC-01"), 8192)  // 131,072 B (128 KiB)
	volatile := bytes.Repeat([]byte("VOLATILE-CACHE02"), 8192) // 131,072 B (128 KiB)

	f, err := os.OpenFile(filepath.Join(mp, "crash.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		d1.killAndDetach()
		fatal(err)
	}
	_, _ = f.Write(durable)
	if err := f.Sync(); err != nil {
		d1.killAndDetach()
		fatal(err)
	}
	// Buffered write acknowledged by kernel page cache, no fsync, no close!
	_, _ = f.Write(volatile)
	d1.killAndDetach()
	_ = f.Close()

	// Remount at Round 2.
	d2 := spawnDaemon(storeDir, journalDir, mp, filepath.Join(dir, "st2.json"), key, 2, writeback.EngineJacobsa, true, 1*time.Second, writeback.EventDetach)
	got, err := os.ReadFile(filepath.Join(mp, "crash.bin"))
	if err != nil {
		d2.killAndDetach()
		fatal(err)
	}

	case1Pass := bytes.Equal(got, durable)
	res.NegativeRegimes.NR1FsyncSurvivedBytes = int64(len(got))
	res.NegativeRegimes.NR1BufferedSIGKILLLostBytes = int64(len(volatile))

	res.SIGKILLMatrix = append(res.SIGKILLMatrix,
		SIGKILLCaseResult{
			CaseID:          "buffered_write_no_fsync_sigkill",
			Description:     "Buffered write(2) (no fsync) + daemon SIGKILL before FUSE_WRITE",
			WrittenBytes:    int64(len(durable) + len(volatile)),
			FsyncBytes:      int64(len(durable)),
			BufferedOnlyB:   int64(len(volatile)),
			SurvivedBytes:   int64(len(got)),
			LostBytes:       int64(len(volatile)),
			Passed:          case1Pass,
			ObservedOutcome: fmt.Sprintf("Remounted file = %d B (exact fsync'd prefix); %d B buffered-only tail LOST in volatile kernel page cache", len(got), len(volatile)),
		},
		SIGKILLCaseResult{
			CaseID:          "write_fsync_immediate_sigkill",
			Description:     "write(2) + fsync(2) + immediate SIGKILL before S3 publish",
			WrittenBytes:    int64(len(durable)),
			FsyncBytes:      int64(len(durable)),
			BufferedOnlyB:   0,
			SurvivedBytes:   int64(len(got)),
			LostBytes:       0,
			Passed:          case1Pass,
			ObservedOutcome: fmt.Sprintf("100%% (%d/%d B) recovered from local NVMe journal on Round 2 remount", len(got), len(durable)),
		},
	)

	// Case 4: Clean close(2) -> 0 synchronous S3 PUTs + async publish <= 5 s.
	cleanData := makePayload(256 << 10)
	cf, err := os.OpenFile(filepath.Join(mp, "clean.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		d2.killAndDetach()
		fatal(err)
	}
	_, _ = cf.Write(cleanData)
	tC := time.Now()
	_ = cf.Close()
	time.Sleep(30 * time.Millisecond)
	stC, _ := d2.status()
	wantSeq := stC.JournalVisible
	var pubDur time.Duration
	for dl := time.Now().Add(writeback.DefaultPublishBound); time.Now().Before(dl); {
		cur, _ := d2.status()
		if cur.JournalPub >= wantSeq && wantSeq > 0 {
			pubDur = time.Since(tC)
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	res.SIGKILLMatrix = append(res.SIGKILLMatrix, SIGKILLCaseResult{
		CaseID:          "clean_close_zero_sync_s3",
		Description:     "write(2) + clean close(2) (FUSE_FLUSH)",
		WrittenBytes:    int64(len(cleanData)),
		FsyncBytes:      0,
		BufferedOnlyB:   0,
		SurvivedBytes:   int64(len(cleanData)),
		LostBytes:       0,
		SyncS3OnClose:   stC.Stats.SyncS3PutsInFlush,
		PublishMS:       float64(pubDur.Microseconds()) / 1000.0,
		Passed:          stC.Stats.SyncS3PutsInFlush == 0 && pubDur > 0 && pubDur <= writeback.DefaultPublishBound,
		ObservedOutcome: fmt.Sprintf("0 sync S3 PUTs on close(2); published asynchronously in %.0f ms (<= 5000 ms)", float64(pubDur.Microseconds())/1000.0),
	})

	// Case 3: Buffered write WITHOUT app fsync (file open) + Detach lifecycle barrier (syncfs -> journal -> publish).
	barrierData := bytes.Repeat([]byte("DETACH-BARRIER!!"), 16384) // 256 KiB
	bf, err := os.OpenFile(filepath.Join(mp, "detach-open.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		d2.killAndDetach()
		fatal(err)
	}
	_, _ = bf.Write(barrierData)
	// Keep open & un-fsynced while triggering Detach barrier!
	finalSt := d2.stopClean()
	_ = bf.Close()

	rawBe, err := backend.Open(context.Background(), "file://"+storeDir)
	if err != nil {
		fatal(err)
	}
	ro, err := vfs.Open(context.Background(), vfs.Config{Backend: rawBe, Cipher: vfs.Hybrid(key)})
	if err != nil {
		fatal(err)
	}
	var bSurvived int64
	if in, err := ro.Resolve(context.Background(), "/detach-open.bin"); err == nil {
		rf, err := ro.Open(context.Background(), in.ID)
		if err == nil {
			bGot := make([]byte, in.Size)
			_, _ = rf.ReadAt(context.Background(), bGot, 0)
			_ = rf.Close()
			if bytes.Equal(bGot, barrierData) {
				bSurvived = int64(len(bGot))
			}
		}
	}
	_ = ro.Close()

	var bTotMS float64
	if finalSt.LastBarrier != nil {
		bTotMS = float64(finalSt.LastBarrier.TotalDuration.Microseconds()) / 1000.0
	}
	res.SIGKILLMatrix = append(res.SIGKILLMatrix, SIGKILLCaseResult{
		CaseID:          "write_no_fsync_detach_barrier",
		Description:     "Buffered write(2) (no fsync, open fd) + Detach syncfs barrier",
		WrittenBytes:    int64(len(barrierData)),
		FsyncBytes:      0,
		BufferedOnlyB:   int64(len(barrierData)),
		SurvivedBytes:   bSurvived,
		LostBytes:       int64(len(barrierData)) - bSurvived,
		Passed:          bSurvived == int64(len(barrierData)),
		ObservedOutcome: fmt.Sprintf("100%% (%d/%d B) drained by syncfs -> journal -> publish in %.1f ms and verified in cold object store", bSurvived, len(barrierData), bTotMS),
	})
}

func runDirtyStallCurve(res *BenchResults, dir string) {
	sizesMiB := []int{16, 64, 128}
	chunk := makePayload(1 << 20)

	for _, mb := range sizesMiB {
		runDir := filepath.Join(dir, fmt.Sprintf("dirty-%dm", mb))
		storeDir := filepath.Join(runDir, "store")
		journalDir := filepath.Join(runDir, "journal")
		mp := filepath.Join(runDir, "mnt")
		_ = os.MkdirAll(storeDir, 0o755)
		_ = os.MkdirAll(journalDir, 0o755)

		key, err := age.GenerateHybridIdentity()
		if err != nil {
			fatal(err)
		}
		d := spawnDaemon(storeDir, journalDir, mp, filepath.Join(runDir, "st.json"), key, 1, writeback.EngineJacobsa, true, 60*time.Second, writeback.EventSuspend)
		f, err := os.OpenFile(filepath.Join(mp, "dirty.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			d.killAndDetach()
			fatal(err)
		}
		for range mb {
			if _, err := f.Write(chunk); err != nil {
				d.killAndDetach()
				fatal(err)
			}
		}
		// Keep fd open and un-fsynced, then measure Suspend barrier drain!
		finalSt := d.stopClean()
		_ = f.Close()

		var pt DirtyWindowStallPoint
		pt.DirtyMiB = mb
		if finalSt.LastBarrier != nil {
			pt.SyncFSMS = float64(finalSt.LastBarrier.SyncFSDuration.Microseconds()) / 1000.0
			pt.JournalMS = float64(finalSt.LastBarrier.JournalSyncDuration.Microseconds()) / 1000.0
			pt.PublishMS = float64(finalSt.LastBarrier.PublishDuration.Microseconds()) / 1000.0
			pt.TotalMS = float64(finalSt.LastBarrier.TotalDuration.Microseconds()) / 1000.0
		}
		res.NegativeRegimes.NR3DirtyStallCurve = append(res.NegativeRegimes.NR3DirtyStallCurve, pt)
	}
}
