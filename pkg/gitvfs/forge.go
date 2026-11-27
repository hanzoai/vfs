// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package gitvfs

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Forge represents a bare local git repository configured with server-side
// HIP-1342 optimizations: protocol v2, partial clone filters, reachability
// bitmaps, commit-graph with changed-paths, and multi-pack-index.
type Forge struct {
	Path          string
	DefaultBranch string
	BulkBranch    string
	runner        *Runner
}

// ForgeStatus summarizes server-side configuration and index verification.
type ForgeStatus struct {
	Path                  string `json:"path"`
	ProtocolVersion       string `json:"protocol_version"`
	AllowFilter           bool   `json:"allow_filter"`
	AllowAnySHA1InWant    bool   `json:"allow_any_sha1_in_want"`
	HasBitmapIndex        bool   `json:"has_bitmap_index"`
	HasCommitGraph        bool   `json:"has_commit_graph"`
	HasMultiPackIndex     bool   `json:"has_multi_pack_index"`
	FsckClean             bool   `json:"fsck_clean"`
	PackCount             int    `json:"pack_count"`
	LooseObjectCount      int    `json:"loose_object_count"`
	DefaultBranchCommitID string `json:"default_branch_commit_id"`
}

// InitForgeFromRepo creates an independent bare forge repository cloned from
// srcRepo without modifying srcRepo, applies all server-side HIP-1342 settings,
// and writes pack bitmaps, commit-graph, and multi-pack-index.
func InitForgeFromRepo(ctx context.Context, r *Runner, srcRepo, bareDir string) (*Forge, error) {
	if r == nil {
		return nil, errors.New("gitvfs: runner is required")
	}
	if srcRepo == "" || bareDir == "" {
		return nil, errors.New("gitvfs: srcRepo and bareDir are required")
	}
	if err := os.MkdirAll(filepath.Dir(bareDir), 0o755); err != nil {
		return nil, err
	}
	if _, err := r.Run(ctx, filepath.Dir(bareDir), "clone", "--bare", "--no-hardlinks", "--quiet", srcRepo, bareDir); err != nil {
		return nil, fmt.Errorf("gitvfs: clone bare forge from %s: %w", srcRepo, err)
	}
	f := &Forge{
		Path:   bareDir,
		runner: r,
	}
	if err := f.ConfigureServer(ctx); err != nil {
		return nil, err
	}
	branch, err := f.detectDefaultBranch(ctx)
	if err != nil {
		return nil, err
	}
	f.DefaultBranch = branch
	if err := f.Optimize(ctx); err != nil {
		return nil, err
	}
	return f, nil
}

// InitForgeWithCorpus builds a bare forge cloned from srcRepo and adds a
// deterministic corpus expansion (extraFiles across nested packages, plus a
// `bulk` branch modifying/adding/deleting a deterministic subset of files) so
// checkout/switch/status benchmarks are reproducible across runs.
func InitForgeWithCorpus(ctx context.Context, r *Runner, srcRepo, bareDir string, extraFiles int, seed int64) (*Forge, error) {
	f, err := InitForgeFromRepo(ctx, r, srcRepo, bareDir)
	if err != nil {
		return nil, err
	}
	workDir, err := os.MkdirTemp(filepath.Dir(bareDir), "forge-seed-work-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workDir)

	if _, err := r.Run(ctx, filepath.Dir(workDir), "clone", "--quiet", f.FileURL(), workDir); err != nil {
		return nil, fmt.Errorf("gitvfs: seed worktree clone: %w", err)
	}
	rng := rand.New(rand.NewSource(seed))

	// Add deterministic extra files on the default branch if requested.
	if extraFiles > 0 {
		dirs := []string{"corpus/core", "corpus/net", "corpus/store", "corpus/cmd", "corpus/pkg", "corpus/docs"}
		for i := range extraFiles {
			sub := dirs[i%len(dirs)]
			bucket := fmt.Sprintf("b%02d", (i/len(dirs))%16)
			fullDir := filepath.Join(workDir, sub, bucket)
			if err := os.MkdirAll(fullDir, 0o755); err != nil {
				return nil, err
			}
			fname := filepath.Join(fullDir, fmt.Sprintf("unit_%05d.go", i))
			content := deterministicGoFile(i, rng)
			if err := os.WriteFile(fname, []byte(content), 0o644); err != nil {
				return nil, err
			}
		}
		if _, err := r.Run(ctx, workDir, "add", "-A"); err != nil {
			return nil, err
		}
		if _, err := r.Run(ctx, workDir, "commit", "--quiet", "-m", fmt.Sprintf("corpus: add %d deterministic files (seed=%d)", extraFiles, seed)); err != nil {
			return nil, err
		}
		if _, err := r.Run(ctx, workDir, "push", "--quiet", "origin", f.DefaultBranch); err != nil {
			return nil, err
		}
	}

	// Create a deterministic `bulk` branch that rewrites/adds files so branch
	// switch benchmarks exercise realistic working-tree churn.
	const bulkBranch = "bulk"
	if _, err := r.Run(ctx, workDir, "checkout", "--quiet", "-b", bulkBranch); err != nil {
		return nil, err
	}
	trackedRes, err := r.Run(ctx, workDir, "ls-files")
	if err != nil {
		return nil, err
	}
	tracked := splitNonEmptyLines(trackedRes.Stdout)
	touchCount := min(max(len(tracked)/4, 24), 400)
	for i := 0; i < touchCount && i < len(tracked); i++ {
		idx := (i*31 + 7) % len(tracked)
		rel := tracked[idx]
		abs := filepath.Join(workDir, rel)
		st, statErr := os.Lstat(abs)
		if statErr != nil || !st.Mode().IsRegular() {
			continue
		}
		orig, readErr := os.ReadFile(abs)
		if readErr != nil {
			continue
		}
		updated := append(orig, []byte(fmt.Sprintf("\n// bulk-variant-%d-%d\n", seed, i))...)
		if err := os.WriteFile(abs, updated, st.Mode().Perm()); err != nil {
			return nil, err
		}
	}
	// Add 20 new branch-only files.
	bulkDir := filepath.Join(workDir, "corpus", "bulk-delta")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		return nil, err
	}
	for i := range 20 {
		p := filepath.Join(bulkDir, fmt.Sprintf("delta_%03d.txt", i))
		if err := os.WriteFile(p, []byte(fmt.Sprintf("delta %d seed %d\n", i, seed)), 0o644); err != nil {
			return nil, err
		}
	}
	if _, err := r.Run(ctx, workDir, "add", "-A"); err != nil {
		return nil, err
	}
	if _, err := r.Run(ctx, workDir, "commit", "--quiet", "-m", fmt.Sprintf("bulk: deterministic branch switch fixture (%d files)", touchCount)); err != nil {
		return nil, err
	}
	if _, err := r.Run(ctx, workDir, "push", "--quiet", "origin", bulkBranch); err != nil {
		return nil, err
	}
	f.BulkBranch = bulkBranch

	if err := f.Optimize(ctx); err != nil {
		return nil, err
	}
	return f, nil
}

// FileURL returns the file:// URL for local protocol-v2 git operations (unlike
// bare path clones, file:// exercises upload-pack/receive-pack & partial filters).
func (f *Forge) FileURL() string {
	return "file://" + f.Path
}

// ConfigureServer sets the forge-side git settings required by HIP-1342.
func (f *Forge) ConfigureServer(ctx context.Context) error {
	settings := [][2]string{
		{"protocol.version", "2"},
		{"uploadpack.allowFilter", "true"},
		{"uploadpack.allowAnySHA1InWant", "true"},
		{"uploadpack.allowReachableSHA1InWant", "true"},
		{"core.commitGraph", "true"},
		{"gc.writeCommitGraph", "true"},
		{"core.multiPackIndex", "true"},
		{"repack.writeBitmaps", "true"},
		{"pack.writeBitmapHashCache", "true"},
		{"receive.fsckObjects", "true"},
	}
	for _, kv := range settings {
		if _, err := f.runner.Run(ctx, f.Path, "config", "--local", kv[0], kv[1]); err != nil {
			return fmt.Errorf("gitvfs: forge config %s=%s: %w", kv[0], kv[1], err)
		}
	}
	return nil
}

// Optimize runs server-side repacking with atomic multi-pack-index +
// reachability bitmaps and writes the commit-graph with changed-paths.
func (f *Forge) Optimize(ctx context.Context) error {
	if _, err := f.runner.Run(ctx, f.Path, "repack", "-ad", "--write-midx", "--write-bitmap-index", "--quiet"); err != nil {
		return fmt.Errorf("gitvfs: forge repack --write-midx --write-bitmap-index: %w", err)
	}
	if _, err := f.runner.Run(ctx, f.Path, "commit-graph", "write", "--reachable", "--changed-paths"); err != nil {
		return fmt.Errorf("gitvfs: forge commit-graph write: %w", err)
	}
	return nil
}

// Verify runs git fsck --full and inspects server-side config, bitmaps,
// commit-graph, and multi-pack-index artifacts on disk.
func (f *Forge) Verify(ctx context.Context) (ForgeStatus, error) {
	st := ForgeStatus{Path: f.Path}
	if res, err := f.runner.Run(ctx, f.Path, "config", "--local", "--get", "protocol.version"); err == nil {
		st.ProtocolVersion = strings.TrimSpace(res.Stdout)
	}
	if res, err := f.runner.Run(ctx, f.Path, "config", "--local", "--get", "uploadpack.allowFilter"); err == nil {
		st.AllowFilter = strings.TrimSpace(res.Stdout) == "true"
	}
	if res, err := f.runner.Run(ctx, f.Path, "config", "--local", "--get", "uploadpack.allowAnySHA1InWant"); err == nil {
		st.AllowAnySHA1InWant = strings.TrimSpace(res.Stdout) == "true"
	}

	packDir := filepath.Join(f.Path, "objects", "pack")
	if entries, err := os.ReadDir(packDir); err == nil {
		for _, e := range entries {
			name := e.Name()
			if strings.HasSuffix(name, ".bitmap") {
				st.HasBitmapIndex = true
			}
			if strings.HasSuffix(name, ".pack") {
				st.PackCount++
			}
			if name == "multi-pack-index" {
				st.HasMultiPackIndex = true
			}
		}
	}
	if _, err := os.Stat(filepath.Join(f.Path, "objects", "info", "commit-graph")); err == nil {
		st.HasCommitGraph = true
	} else if _, err := os.Stat(filepath.Join(f.Path, "objects", "info", "commit-graphs", "commit-graph-chain")); err == nil {
		st.HasCommitGraph = true
	}

	if _, err := f.runner.Run(ctx, f.Path, "fsck", "--full", "--no-dangling"); err != nil {
		return st, fmt.Errorf("gitvfs: forge fsck failed: %w", err)
	}
	st.FsckClean = true

	if f.DefaultBranch != "" {
		if sha, err := f.HasRef(ctx, "refs/heads/"+f.DefaultBranch); err == nil {
			st.DefaultBranchCommitID = sha
		}
	}
	counts, err := CountObjectsInDir(ctx, f.runner, f.Path)
	if err == nil {
		st.LooseObjectCount = counts.LooseCount
	}
	return st, nil
}

// HasRef resolves refName on the bare forge and returns its commit SHA.
func (f *Forge) HasRef(ctx context.Context, refName string) (string, error) {
	res, err := f.runner.Run(ctx, f.Path, "rev-parse", "--verify", refName)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(res.Stdout), nil
}

// ListRefs returns all refs matching prefix (e.g., "refs/heads/sandbox/").
func (f *Forge) ListRefs(ctx context.Context, prefix string) ([]string, error) {
	res, err := f.runner.Run(ctx, f.Path, "for-each-ref", "--format=%(refname)", prefix)
	if err != nil {
		return nil, err
	}
	return splitNonEmptyLines(res.Stdout), nil
}

func (f *Forge) detectDefaultBranch(ctx context.Context) (string, error) {
	res, err := f.runner.Run(ctx, f.Path, "symbolic-ref", "--short", "HEAD")
	if err == nil && strings.TrimSpace(res.Stdout) != "" {
		return strings.TrimSpace(res.Stdout), nil
	}
	res, err = f.runner.Run(ctx, f.Path, "for-each-ref", "--format=%(refname:short)", "refs/heads")
	if err != nil {
		return "", err
	}
	lines := splitNonEmptyLines(res.Stdout)
	if len(lines) == 0 {
		return "main", nil
	}
	return lines[0], nil
}

func deterministicGoFile(i int, rng *rand.Rand) string {
	var b strings.Builder
	fmt.Fprintf(&b, "package unit%02d\n\n// Deterministic benchmark fixture %d.\n", i%16, i)
	fmt.Fprintf(&b, "func Compute%05d(x uint64) uint64 {\n", i)
	c1 := rng.Uint64() | 1
	c2 := rng.Uint64() | 1
	fmt.Fprintf(&b, "\tv := x ^ 0x%x\n", c1)
	for j := range 8 {
		fmt.Fprintf(&b, "\tv = (v*0x%x + %d) ^ (v >> 13)\n", (c2+uint64(j)*17)|1, j+1)
	}
	b.WriteString("\treturn v\n}\n")
	return b.String()
}

func splitNonEmptyLines(s string) []string {
	raw := strings.Split(s, "\n")
	out := make([]string, 0, len(raw))
	for _, line := range raw {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

// MeasureForgePushRoundTrip measures pushing a single commit from repoDir to
// branch on the bare forge and verifying the ref landed cleanly.
func MeasureForgePushRoundTrip(ctx context.Context, r *Runner, forge *Forge, repoDir, branch string) (time.Duration, string, error) {
	start := time.Now()
	if _, err := r.Run(ctx, repoDir, "push", "--quiet", "origin", "HEAD:refs/heads/"+branch); err != nil {
		return time.Since(start), "", err
	}
	dur := time.Since(start)
	sha, err := forge.HasRef(ctx, "refs/heads/"+branch)
	if err != nil {
		return dur, "", fmt.Errorf("gitvfs: pushed ref %s missing on forge: %w", branch, err)
	}
	return dur, sha, nil
}
