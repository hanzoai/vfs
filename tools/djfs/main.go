// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

// Command djfs evaluates DJFS directory-granularity metadata mutation
// coalescing against current journaled VFS v2 under the same durability
// contract across npm-ci extraction, scratch cancellation, atomic
// WriteTmp+Rename, and matched-durability Varmail Sync latency (HIP-1345).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/djfs"
)

type cliVolumeTarget struct {
	mu       sync.Mutex
	v        *vfs.Volume
	syncOnly bool
	idMap    map[djfs.InodeID]vfs.InodeID
}

func newCLIVolumeTarget(v *vfs.Volume, syncOnly bool) *cliVolumeTarget {
	return &cliVolumeTarget{
		v:        v,
		syncOnly: syncOnly,
		idMap:    map[djfs.InodeID]vfs.InodeID{djfs.RootInode: vfs.RootInode},
	}
}

func (vt *cliVolumeTarget) ApplyBatch(ctx context.Context, batch djfs.Batch) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	for i := range batch.Ops {
		op := &batch.Ops[i]
		switch op.Kind {
		case djfs.DeltaMkdir:
			in, err := vt.v.Mkdir(ctx, vt.idMap[op.Dir], op.Name, op.Attr.Mode.Perm())
			if err != nil {
				return err
			}
			vt.idMap[op.ID] = in.ID

		case djfs.DeltaCreateFile:
			in, err := vt.v.Create(ctx, vt.idMap[op.Dir], op.Name, op.Attr.Mode.Perm())
			if err != nil {
				return err
			}
			vt.idMap[op.ID] = in.ID
			if len(op.Data) > 0 {
				f, err := vt.v.Open(ctx, in.ID)
				if err != nil {
					return err
				}
				_, wErr := f.WriteAt(ctx, op.Data, 0)
				cErr := f.Close()
				if wErr != nil {
					return wErr
				}
				if cErr != nil {
					return cErr
				}
			}

		case djfs.DeltaWrite:
			f, err := vt.v.Open(ctx, vt.idMap[op.ID])
			if err != nil {
				return err
			}
			if err := f.Truncate(ctx, 0); err != nil {
				_ = f.Close()
				return err
			}
			if len(op.Data) > 0 {
				if _, err := f.WriteAt(ctx, op.Data, 0); err != nil {
					_ = f.Close()
					return err
				}
			}
			if err := f.Close(); err != nil {
				return err
			}

		case djfs.DeltaRename:
			if err := vt.v.Rename(ctx, vt.idMap[op.Dir], op.Name, vt.idMap[op.DstDir], op.DstName, 0); err != nil {
				return err
			}

		case djfs.DeltaUnlink, djfs.DeltaRmdir:
			if err := vt.v.Remove(ctx, vt.idMap[op.Dir], op.Name); err != nil {
				return err
			}
			if op.ID != 0 {
				delete(vt.idMap, op.ID)
			}
		}
	}
	if vt.syncOnly {
		return vt.v.Sync(ctx)
	}
	return vt.v.Flush(ctx)
}

type report struct {
	Packages            int     `json:"packages"`
	FilesPerPackage     int     `json:"files_per_package"`
	NpmSurvivingFiles   int     `json:"npm_surviving_files"`
	V2DirectJournalRecs uint64  `json:"v2_direct_journal_recs"`
	V2DirectS3Puts      int     `json:"v2_direct_s3_puts"`
	V2DirectPutsPerFile float64 `json:"v2_direct_puts_per_file"`
	V2DirectDurationMs  float64 `json:"v2_direct_duration_ms"`
	DJFSJournalRecs     uint64  `json:"djfs_journal_recs"`
	DJFSRawOps          uint64  `json:"djfs_raw_ops"`
	DJFSPublishedOps    uint64  `json:"djfs_published_ops"`
	DJFSS3Puts          int     `json:"djfs_s3_puts"`
	DJFSPutsPerFile     float64 `json:"djfs_puts_per_file"`
	DJFSDurationMs      float64 `json:"djfs_duration_ms"`
	ScratchRawOps       uint64  `json:"scratch_raw_ops"`
	ScratchJournalOps   uint64  `json:"scratch_journal_ops"`
	AtomicRawOps        uint64  `json:"atomic_raw_ops"`
	AtomicJournalOps    uint64  `json:"atomic_journal_ops"`
	V2NativeSyncP50Us   float64 `json:"v2_native_sync_p50_us"`
	V2NativeSyncP99Us   float64 `json:"v2_native_sync_p99_us"`
	DJFSOverV2SyncP50Us float64 `json:"djfs_over_v2_sync_p50_us"`
	DJFSOverV2SyncP99Us float64 `json:"djfs_over_v2_sync_p99_us"`
}

func percentileUs(samples []time.Duration, q float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)) * q)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx].Nanoseconds()) / 1000.0
}

func attachCLI(ctx context.Context, mem *storetest.Mem, ciph vfs.Cipher, jdir string) (*vfs.Volume, error) {
	if err := vfs.Create(ctx, vfs.Config{Backend: mem, Cipher: ciph}); err != nil {
		return nil, err
	}
	return vfs.Attach(ctx, vfs.Config{
		Backend:         mem,
		Cipher:          ciph,
		Round:           1,
		Journal:         jdir,
		JournalCapacity: 128 << 20,
		Bound:           time.Hour,
		Disk:            storetest.Disk,
	})
}

func main() {
	pkgs := flag.Int("pkgs", 25, "number of npm-ci packages")
	perPkg := flag.Int("files-per-pkg", 40, "surviving files per package")
	asJSON := flag.Bool("json", false, "emit JSON report")
	flag.Parse()

	ctx := context.Background()
	jRoot, err := os.MkdirTemp(storetest.Journals(), "djfs-cli-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdirtemp: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(jRoot)

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		fmt.Fprintf(os.Stderr, "age key: %v\n", err)
		os.Exit(1)
	}
	ciph := vfs.Hybrid(key)

	var rep report
	rep.Packages = *pkgs
	rep.FilesPerPackage = *perPkg
	rep.NpmSurvivingFiles = *pkgs * *perPkg

	// 1. Matched-durability baseline: current VFS v2 Volume directly (disk journal + epoch Flush).
	{
		mem := storetest.NewMem()
		vol, err := attachCLI(ctx, mem, ciph, filepath.Join(jRoot, "v2-direct"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "v2 direct attach: %v\n", err)
			os.Exit(1)
		}
		mem.Reset()
		t0 := time.Now()
		stg, _ := vol.Mkdir(ctx, vfs.RootInode, ".staging", 0o700)
		for p := 0; p < *pkgs; p++ {
			pd, _ := vol.Mkdir(ctx, vfs.RootInode, fmt.Sprintf("pkg-%04d", p), 0o755)
			lockName := fmt.Sprintf(".lock-%04d", p)
			lk, _ := vol.Create(ctx, stg.ID, lockName, 0o600)
			lf, _ := vol.Open(ctx, lk.ID)
			_, _ = lf.WriteAt(ctx, []byte("lock"), 0)
			_ = lf.Close()
			_ = vol.Remove(ctx, stg.ID, lockName)

			for f := 0; f < *perPkg; f++ {
				tmp := fmt.Sprintf("t_%04d_%03d.tmp", p, f)
				dst := fmt.Sprintf("f_%03d.js", f)
				sz := 512 + ((p*29 + f*13) % 1536)
				if f == 0 && p%8 == 0 {
					sz = 20 << 10
				}
				payload := bytes.Repeat([]byte("j"), sz)
				in, _ := vol.Create(ctx, stg.ID, tmp, 0o644)
				fh, _ := vol.Open(ctx, in.ID)
				_, _ = fh.WriteAt(ctx, payload, 0)
				_ = fh.Close()
				_ = vol.Rename(ctx, stg.ID, tmp, pd.ID, dst, 0)
			}
		}
		_ = vol.Remove(ctx, vfs.RootInode, ".staging")
		if err := vol.Flush(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "v2 direct flush: %v\n", err)
			os.Exit(1)
		}
		rep.V2DirectDurationMs = float64(time.Since(t0).Microseconds()) / 1000.0
		rep.V2DirectJournalRecs = vol.State().Published
		rep.V2DirectS3Puts = mem.Total()
		rep.V2DirectPutsPerFile = float64(rep.V2DirectS3Puts) / float64(rep.NpmSurvivingFiles)
		_ = vol.Close()
	}

	// 2. Coalesced: DJFS reusing VFS v2 native journal + epoch Flush under identical durability contract.
	{
		mem := storetest.NewMem()
		vol, err := attachCLI(ctx, mem, ciph, filepath.Join(jRoot, "djfs-v2"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "djfs v2 attach: %v\n", err)
			os.Exit(1)
		}
		mem.Reset()
		eng, _ := djfs.New(djfs.Config{
			Target:        newCLIVolumeTarget(vol, false),
			MaxDirtyOps:   1 << 22,
			MaxDirtyBytes: 512 << 20,
		})
		t0 := time.Now()
		stg, _ := eng.Mkdir(ctx, djfs.RootInode, ".staging", 0o700)
		for p := 0; p < *pkgs; p++ {
			pd, _ := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("pkg-%04d", p), 0o755)
			lockName := fmt.Sprintf(".lock-%04d", p)
			_, _ = eng.Create(ctx, stg.ID, lockName, 0o600, []byte("lock"))
			_ = eng.Unlink(ctx, stg.ID, lockName)

			for f := 0; f < *perPkg; f++ {
				tmp := fmt.Sprintf("t_%04d_%03d.tmp", p, f)
				dst := fmt.Sprintf("f_%03d.js", f)
				sz := 512 + ((p*29 + f*13) % 1536)
				if f == 0 && p%8 == 0 {
					sz = 20 << 10
				}
				payload := bytes.Repeat([]byte("j"), sz)
				_, _ = eng.Create(ctx, stg.ID, tmp, 0o600, payload)
				_, _ = eng.Chmod(ctx, stg.ID, tmp, 0o644)
				_ = eng.Rename(ctx, stg.ID, tmp, pd.ID, dst)
			}
		}
		_ = eng.Rmdir(ctx, djfs.RootInode, ".staging")
		if err := eng.Publish(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "djfs publish: %v\n", err)
			os.Exit(1)
		}
		rep.DJFSDurationMs = float64(time.Since(t0).Microseconds()) / 1000.0
		st := eng.Stats()
		rep.DJFSJournalRecs = vol.State().Published
		rep.DJFSRawOps = st.RawOps
		rep.DJFSPublishedOps = st.PublishedOps
		rep.DJFSS3Puts = mem.Total()
		rep.DJFSPutsPerFile = float64(rep.DJFSS3Puts) / float64(rep.NpmSurvivingFiles)
		_ = vol.Close()
	}

	// 3. Scratch Create+Unlink & Atomic WriteTmp+Rename coalescing counts.
	{
		eng, _ := djfs.New(djfs.Config{MaxDirtyOps: 1 << 22, MaxDirtyBytes: 512 << 20})
		d, _ := eng.Mkdir(ctx, djfs.RootInode, "scratch", 0o755)
		_, _ = eng.FlushJournal(ctx)
		b0 := eng.Stats()
		payload := bytes.Repeat([]byte("s"), 4096)
		for i := 0; i < 5000; i++ {
			nm := fmt.Sprintf("tmp_%d", i)
			_, _ = eng.Create(ctx, d.ID, nm, 0o600, payload)
			_ = eng.Unlink(ctx, d.ID, nm)
		}
		_, _ = eng.FlushJournal(ctx)
		b1 := eng.Stats()
		rep.ScratchRawOps = b1.RawOps - b0.RawOps
		rep.ScratchJournalOps = b1.JournalOps - b0.JournalOps

		for i := 0; i < 5000; i++ {
			tmp := fmt.Sprintf(".cfg.%d.tmp", i)
			_, _ = eng.Create(ctx, d.ID, tmp, 0o600, payload[:1024])
			_, _ = eng.Chmod(ctx, d.ID, tmp, 0o644)
			_ = eng.Rename(ctx, d.ID, tmp, d.ID, "config.json")
		}
		_, _ = eng.FlushJournal(ctx)
		b2 := eng.Stats()
		rep.AtomicRawOps = b2.RawOps - b1.RawOps
		rep.AtomicJournalOps = b2.JournalOps - b1.JournalOps
	}

	// 4. Matched-durability local Sync latency: VFS v2 native Sync vs DJFS over VFS v2 native Sync.
	{
		payload := bytes.Repeat([]byte("m"), 4096)
		const rounds = 100

		mem1 := storetest.NewMem()
		v1, err := attachCLI(ctx, mem1, ciph, filepath.Join(jRoot, "sync-v2"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "sync-v2 attach: %v\n", err)
			os.Exit(1)
		}
		d1, _ := v1.Mkdir(ctx, vfs.RootInode, "mail", 0o755)
		_ = v1.Sync(ctx)
		v2Lat := make([]time.Duration, 0, rounds)
		for i := 0; i < rounds; i++ {
			tmp := fmt.Sprintf("tmp_%d", i)
			cur := fmt.Sprintf("cur_%d", i)
			t0 := time.Now()
			in, _ := v1.Create(ctx, d1.ID, tmp, 0o600)
			fh, _ := v1.Open(ctx, in.ID)
			_, _ = fh.WriteAt(ctx, payload, 0)
			_ = fh.Close()
			_ = v1.Rename(ctx, d1.ID, tmp, d1.ID, cur, 0)
			_ = v1.Sync(ctx)
			v2Lat = append(v2Lat, time.Since(t0))
		}
		rep.V2NativeSyncP50Us = percentileUs(v2Lat, 0.50)
		rep.V2NativeSyncP99Us = percentileUs(v2Lat, 0.99)
		_ = v1.Close()

		mem2 := storetest.NewMem()
		v2, err := attachCLI(ctx, mem2, ciph, filepath.Join(jRoot, "sync-djfs"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "sync-djfs attach: %v\n", err)
			os.Exit(1)
		}
		eng, _ := djfs.New(djfs.Config{Target: newCLIVolumeTarget(v2, true)})
		d2, _ := eng.Mkdir(ctx, djfs.RootInode, "mail", 0o755)
		_ = eng.Publish(ctx)
		djfsLat := make([]time.Duration, 0, rounds)
		for i := 0; i < rounds; i++ {
			tmp := fmt.Sprintf("tmp_%d", i)
			cur := fmt.Sprintf("cur_%d", i)
			t0 := time.Now()
			_, _ = eng.Create(ctx, d2.ID, tmp, 0o600, payload)
			_ = eng.Rename(ctx, d2.ID, tmp, d2.ID, cur)
			_ = eng.Publish(ctx)
			djfsLat = append(djfsLat, time.Since(t0))
		}
		rep.DJFSOverV2SyncP50Us = percentileUs(djfsLat, 0.50)
		rep.DJFSOverV2SyncP99Us = percentileUs(djfsLat, 0.99)
		_ = v2.Close()
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return
	}

	fmt.Printf("HIP-1345 DJFS vs Current VFS v2 Matched-Durability Summary\n")
	fmt.Printf("  npm-ci (%d files, both disk-journaled + epoch Flush):\n", rep.NpmSurvivingFiles)
	fmt.Printf("    VFS v2 direct : journal_recs=%d | S3 PUTs=%d (%.4f PUTs/file) | %.1f ms\n",
		rep.V2DirectJournalRecs, rep.V2DirectS3Puts, rep.V2DirectPutsPerFile, rep.V2DirectDurationMs)
	fmt.Printf("    DJFS + VFS v2 : journal_recs=%d | S3 PUTs=%d (%.4f PUTs/file) | %.1f ms (raw=%d -> pub=%d)\n",
		rep.DJFSJournalRecs, rep.DJFSS3Puts, rep.DJFSPutsPerFile, rep.DJFSDurationMs, rep.DJFSRawOps, rep.DJFSPublishedOps)
	fmt.Printf("  scratch cancellation: %d raw ops -> %d journal ops\n", rep.ScratchRawOps, rep.ScratchJournalOps)
	fmt.Printf("  atomic WriteTmp+Rename: %d raw ops -> %d journal ops\n", rep.AtomicRawOps, rep.AtomicJournalOps)
	fmt.Printf("  matched local Sync latency (us): VFS v2 native p50=%.1f p99=%.1f | DJFS+v2 p50=%.1f p99=%.1f\n",
		rep.V2NativeSyncP50Us, rep.V2NativeSyncP99Us, rep.DJFSOverV2SyncP50Us, rep.DJFSOverV2SyncP99Us)
}
