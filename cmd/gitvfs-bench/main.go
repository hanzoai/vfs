// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command gitvfs-bench runs the reproducible HIP-1342 (#29) native-git-on-vfs
// benchmark harness, comparing Plain NVMe against untuned git on vfs, tuned
// git on vfs (batched fsync + commit-graph + MIDX + untrackedCache), shared
// read-only alternates + sparse checkout, and O(1) COW Volume clone-by-reference.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanzoai/vfs/pkg/gitvfs"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "gitvfs-bench: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("gitvfs-bench", flag.ContinueOnError)
	fs.SetOutput(stderr)

	// Child FUSE server mode (invoked internally by --fuse benchmarks so the
	// FUSE daemon runs in its own process and never deadlocks Go's poller).
	serveMount := fs.Bool("serve-mount", false, "internal: run instrumented FUSE mount child process")
	storeDir := fs.String("store-dir", "", "internal: backend store directory for --serve-mount")
	baseStoreDir := fs.String("base-store-dir", "", "internal: read-only base store directory for COW --serve-mount")
	mountpoint := fs.String("mountpoint", "", "internal: FUSE mountpoint for --serve-mount")
	journalDir := fs.String("journal-dir", "", "internal: journal directory for --serve-mount")
	round := fs.Uint64("round", 1, "internal: fencing round for --serve-mount")

	// Benchmark harness flags.
	repoPath := fs.String("repo", ".", "path to real git repository used as benchmark corpus")
	extraFiles := fs.Int("extra-files", 400, "deterministic extra files added to corpus branch")
	bulkStage := fs.Int("bulk-stage", 300, "number of loose files staged in batched-fsync matrix")
	seed := fs.Int64("seed", 1342, "deterministic PRNG seed")
	reps := fs.Int("reps", 3, "repetitions per mode (median reported)")
	cowSandboxes := fs.Int("cow-sandboxes", 4, "number of sandboxes cloned from one COW Volume snapshot")
	sparseCones := fs.String("sparse-cones", "pkg/gitvfs,corpus/core", "comma-separated cone paths for sparse checkout mode")
	useFUSE := fs.Bool("fuse", false, "run git workloads directly over a kernel FUSE-mounted *vfs.Volume (requires -tags fuse)")
	putDelay := fs.Duration("put-delay", 0, "optional simulated S3 PUT RTT (e.g. 2ms)")
	outDir := fs.String("out-dir", "", "directory to write report.json and REPORT.md")
	jsonOnly := fs.Bool("json", false, "print JSON report to stdout instead of Markdown")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *serveMount {
		line, err := bufio.NewReader(stdin).ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read identity key: %w", err)
		}
		return gitvfs.ServeInstrumentedMount(ctx, *storeDir, *baseStoreDir, *mountpoint, *journalDir, *round, *putDelay, line)
	}

	absRepo, err := filepath.Abs(*repoPath)
	if err != nil {
		return err
	}
	var cones []string
	for _, c := range strings.Split(*sparseCones, ",") {
		c = strings.TrimSpace(c)
		if c != "" {
			cones = append(cones, c)
		}
	}
	selfBin, _ := os.Executable()
	cfg := gitvfs.BenchConfig{
		SrcRepo:        absRepo,
		ExtraFiles:     *extraFiles,
		BulkStageFiles: *bulkStage,
		Seed:           *seed,
		Repetitions:    *reps,
		COWSandboxes:   *cowSandboxes,
		SparseCones:    cones,
		UseFUSEMount:   *useFUSE,
		MountHelperBin: selfBin,
		PutDelay:       *putDelay,
	}

	if *useFUSE && !gitvfs.FUSEMountAvailable() {
		fmt.Fprintln(stderr, "warning: --fuse requested but FUSE is unavailable in this build/environment; falling back to direct *vfs.Volume mode")
	}

	report, err := gitvfs.RunBenchmark(ctx, cfg)
	if err != nil {
		return err
	}

	jsonBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	jsonBytes = append(jsonBytes, '\n')
	mdText := report.RenderMarkdown()

	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*outDir, "results.json"), jsonBytes, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*outDir, "REPORT.md"), []byte(mdText), 0o644); err != nil {
			return err
		}
	}

	if *jsonOnly {
		_, err = stdout.Write(jsonBytes)
		return err
	}
	_, err = io.WriteString(stdout, mdText)
	return err
}
