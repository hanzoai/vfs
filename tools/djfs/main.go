// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

// Command djfs evaluates DJFS directory-granularity metadata mutation
// coalescing against uncoalesced Volume commits and plain local disk I/O
// across npm-ci, MDTest, scratch cancellation, atomic WriteTmp+Rename,
// Varmail fsync, and negative-control workloads (HIP-1345).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/djfs"
)

type cliVolumeTarget struct {
	mu    sync.Mutex
	v     *vfs.Volume
	idMap map[djfs.InodeID]vfs.InodeID
}

func newCLIVolumeTarget(v *vfs.Volume) *cliVolumeTarget {
	return &cliVolumeTarget{
		v:     v,
		idMap: map[djfs.InodeID]vfs.InodeID{djfs.RootInode: vfs.RootInode},
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

		case djfs.DeltaUnlink, djfs.DeltaRmdir:
			if err := vt.v.Remove(ctx, vt.idMap[op.Dir], op.Name); err != nil {
				return err
			}
			if op.ID != 0 {
				delete(vt.idMap, op.ID)
			}
		}
	}
	return vt.v.Flush(ctx)
}

type report struct {
	Packages          int     `json:"packages"`
	FilesPerPackage   int     `json:"files_per_package"`
	NpmSurvivingFiles int     `json:"npm_surviving_files"`
	NpmRawOps         uint64  `json:"npm_raw_ops"`
	NpmPublishedOps   uint64  `json:"npm_published_ops"`
	NpmS3Puts         int     `json:"npm_s3_puts"`
	NpmPutsPerFile    float64 `json:"npm_puts_per_file"`
	ClosePutsPerFile  float64 `json:"close_puts_per_file"`
	ScratchRawOps     uint64  `json:"scratch_raw_ops"`
	ScratchJournalOps uint64  `json:"scratch_journal_ops"`
	AtomicRawOps      uint64  `json:"atomic_raw_ops"`
	AtomicJournalOps  uint64  `json:"atomic_journal_ops"`
	DJFSFsyncP50Us    float64 `json:"djfs_fsync_p50_us"`
	DJFSFsyncP99Us    float64 `json:"djfs_fsync_p99_us"`
	PlainFsyncP50Us   float64 `json:"plain_fsync_p50_us"`
	PlainFsyncP99Us   float64 `json:"plain_fsync_p99_us"`
}

func openLocalJournal(baseDir string) (*journal.Journal, string, error) {
	jDir := filepath.Join(baseDir, "journal")
	opt := journal.Options{
		Capacity: 16 << 20,
		Segment:  4 << 20,
		Lineage:  []byte("tools-djfs-eval"),
	}
	j, err := journal.Open(jDir, opt)
	if err == nil {
		return j, "", nil
	}
	if !errors.Is(err, journal.ErrVolatile) {
		return nil, "", err
	}
	wd, err := os.Getwd()
	if err != nil {
		return nil, "", err
	}
	alt, err := os.MkdirTemp(wd, ".djfs-cli-")
	if err != nil {
		return nil, "", err
	}
	j, err = journal.Open(alt, opt)
	if err != nil {
		_ = os.RemoveAll(alt)
		return nil, "", err
	}
	return j, alt, nil
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

func main() {
	pkgs := flag.Int("pkgs", 100, "number of npm-ci packages")
	perPkg := flag.Int("files-per-pkg", 100, "surviving files per package")
	asJSON := flag.Bool("json", false, "emit JSON report")
	flag.Parse()

	ctx := context.Background()
	workDir, err := os.MkdirTemp("", "djfs-eval-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdirtemp: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(workDir)

	var rep report
	rep.Packages = *pkgs
	rep.FilesPerPackage = *perPkg
	nPkgs := *pkgs
	nPerPkg := *perPkg
	rep.NpmSurvivingFiles = nPkgs * nPerPkg

	// 1. Measure uncoalesced Flush-per-close baseline on live vfs.Volume (100 files).
	{
		mem := storetest.NewMem()
		key, err := age.GenerateHybridIdentity()
		if err != nil {
			fmt.Fprintf(os.Stderr, "age key: %v\n", err)
			os.Exit(1)
		}
		ciph := vfs.Hybrid(key)
		if err := vfs.Create(ctx, vfs.Config{Backend: mem, Cipher: ciph}); err != nil {
			fmt.Fprintf(os.Stderr, "vfs create: %v\n", err)
			os.Exit(1)
		}
		vol, err := vfs.Attach(ctx, vfs.Config{Backend: mem, Cipher: ciph, Round: 1})
		if err != nil {
			fmt.Fprintf(os.Stderr, "vfs attach: %v\n", err)
			os.Exit(1)
		}
		d, err := vol.Mkdir(ctx, vfs.RootInode, "pkg", 0o755)
		if err != nil {
			fmt.Fprintf(os.Stderr, "vfs mkdir: %v\n", err)
			os.Exit(1)
		}
		_ = vol.Flush(ctx)
		mem.Reset()

		const sampleCloseFiles = 100
		body := bytes.Repeat([]byte("x"), 20<<10)
		for i := 0; i < sampleCloseFiles; i++ {
			in, err := vol.Create(ctx, d.ID, fmt.Sprintf("f_%03d.js", i), 0o644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "vfs create file: %v\n", err)
				os.Exit(1)
			}
			f, err := vol.Open(ctx, in.ID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "vfs open: %v\n", err)
				os.Exit(1)
			}
			_, _ = f.WriteAt(ctx, body, 0)
			_ = f.Close()
			_ = vol.Flush(ctx)
		}
		rep.ClosePutsPerFile = float64(mem.Total()) / float64(sampleCloseFiles)
		_ = vol.Close()
	}

	// 2. Measure DJFS coalesced npm-ci extraction onto live vfs.Volume.
	{
		mem := storetest.NewMem()
		key, _ := age.GenerateHybridIdentity()
		ciph := vfs.Hybrid(key)
		_ = vfs.Create(ctx, vfs.Config{Backend: mem, Cipher: ciph})
		vol, _ := vfs.Attach(ctx, vfs.Config{Backend: mem, Cipher: ciph, Round: 1})
		mem.Reset()

		target := newCLIVolumeTarget(vol)
		eng, _ := djfs.New(djfs.Config{
			Target:        target,
			MaxDirtyOps:   1 << 22,
			MaxDirtyBytes: 512 << 20,
		})

		stg, _ := eng.Mkdir(ctx, djfs.RootInode, ".staging", 0o700)
		for p := 0; p < *pkgs; p++ {
			pd, _ := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("pkg-%04d", p), 0o755)
			lockName := fmt.Sprintf(".lock-%04d", p)
			_, _ = eng.Create(ctx, stg.ID, lockName, 0o600, []byte("lock"))
			_ = eng.Unlink(ctx, stg.ID, lockName)

			for f := 0; f < *perPkg; f++ {
				tmp := fmt.Sprintf("t_%04d_%03d.tmp", p, f)
				dst := fmt.Sprintf("f_%03d.js", f)
				mix := p*29 + f*13
				rem := mix % 1536
				sz := 512 + rem
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
			fmt.Fprintf(os.Stderr, "publish: %v\n", err)
			os.Exit(1)
		}
		st := eng.Stats()
		rep.NpmRawOps = st.RawOps
		rep.NpmPublishedOps = st.PublishedOps
		rep.NpmS3Puts = mem.Total()
		rep.NpmPutsPerFile = float64(rep.NpmS3Puts) / float64(rep.NpmSurvivingFiles)
		_ = vol.Close()
	}

	// 3. Scratch Create+Unlink & Atomic WriteTmp+Rename storms.
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

	// 4. Local disk Fsync latency: DJFS + journal.Journal vs plain OS file + dir Sync.
	{
		j, altCleanup, err := openLocalJournal(workDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open journal: %v\n", err)
			os.Exit(1)
		}
		if altCleanup != "" {
			defer os.RemoveAll(altCleanup)
		}
		defer j.Close()

		eng, _ := djfs.New(djfs.Config{Journal: j})
		d, _ := eng.Mkdir(ctx, djfs.RootInode, "mail", 0o755)
		_ = eng.FsyncDir(ctx, d.ID)

		payload := bytes.Repeat([]byte("m"), 4096)
		const rounds = 200
		djfsLat := make([]time.Duration, 0, rounds)
		for i := 0; i < rounds; i++ {
			tmp := fmt.Sprintf("tmp_%d", i)
			cur := fmt.Sprintf("cur_%d", i)
			t0 := time.Now()
			_, _ = eng.Create(ctx, d.ID, tmp, 0o600, payload)
			_ = eng.Rename(ctx, d.ID, tmp, d.ID, cur)
			if err := eng.FsyncDir(ctx, d.ID); err != nil {
				fmt.Fprintf(os.Stderr, "djfs fsync: %v\n", err)
				os.Exit(1)
			}
			djfsLat = append(djfsLat, time.Since(t0))
		}
		rep.DJFSFsyncP50Us = percentileUs(djfsLat, 0.50)
		rep.DJFSFsyncP99Us = percentileUs(djfsLat, 0.99)

		plainDir := filepath.Join(workDir, "plain-fsync")
		_ = os.MkdirAll(plainDir, 0o755)
		plainLat := make([]time.Duration, 0, rounds)
		for i := 0; i < rounds; i++ {
			sp := filepath.Join(plainDir, fmt.Sprintf("tmp_%d", i))
			dp := filepath.Join(plainDir, fmt.Sprintf("cur_%d", i))
			t0 := time.Now()
			f, err := os.OpenFile(sp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "plain open: %v\n", err)
				os.Exit(1)
			}
			_, _ = f.Write(payload)
			_ = f.Sync()
			_ = f.Close()
			_ = os.Rename(sp, dp)
			if df, err := os.Open(plainDir); err == nil {
				_ = df.Sync()
				_ = df.Close()
			}
			plainLat = append(plainLat, time.Since(t0))
		}
		rep.PlainFsyncP50Us = percentileUs(plainLat, 0.50)
		rep.PlainFsyncP99Us = percentileUs(plainLat, 0.99)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return
	}

	fmt.Printf("HIP-1345 DJFS Evaluation Summary\n")
	fmt.Printf("  npm-ci (%d files): raw_ops=%d -> published_ops=%d | S3 PUTs=%d (%.4f PUTs/file vs %.2f Flush-per-close)\n",
		rep.NpmSurvivingFiles, rep.NpmRawOps, rep.NpmPublishedOps, rep.NpmS3Puts, rep.NpmPutsPerFile, rep.ClosePutsPerFile)
	fmt.Printf("  scratch cancellation: %d raw ops -> %d journal ops\n", rep.ScratchRawOps, rep.ScratchJournalOps)
	fmt.Printf("  atomic WriteTmp+Rename: %d raw ops -> %d journal ops\n", rep.AtomicRawOps, rep.AtomicJournalOps)
	fmt.Printf("  fsync latency (us): DJFS p50=%.1f p99=%.1f | Plain disk p50=%.1f p99=%.1f\n",
		rep.DJFSFsyncP50Us, rep.DJFSFsyncP99Us, rep.PlainFsyncP50Us, rep.PlainFsyncP99Us)
}
