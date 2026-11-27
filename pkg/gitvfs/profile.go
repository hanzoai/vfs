// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package gitvfs implements native git tuning, batched fsync management,
// read-only shared alternates, O(1) copy-on-write Volume clone-by-reference,
// forge-side optimization, and store/journal instrumentation for hanzoai/vfs
// (HIP-1342, issue #29).
package gitvfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FsyncMode maps to git's core.fsync configuration.
type FsyncMode string

const (
	FsyncDefault   FsyncMode = ""          // git default (packs/refs, not loose objects)
	FsyncNone      FsyncMode = "none"      // disable fsync
	FsyncCommitted FsyncMode = "committed" // fsync committed objects and refs
	FsyncAll       FsyncMode = "all"       // fsync all git files
)

// FsyncMethod maps to git's core.fsyncMethod configuration.
type FsyncMethod string

const (
	FsyncMethodDefault FsyncMethod = ""
	FsyncMethodFsync   FsyncMethod = "fsync"
	FsyncMethodBatch   FsyncMethod = "batch"
)

// CloneFilter maps to git clone --filter=<spec>.
type CloneFilter string

const (
	FilterNone     CloneFilter = ""
	FilterBlobNone CloneFilter = "blob:none"
	FilterTreeZero CloneFilter = "tree:0"
)

// Profile defines a reproducible, self-contained git configuration profile for
// sandboxes running on a vfs Volume.
type Profile struct {
	Name               string
	Fsync              FsyncMode
	FsyncMethod        FsyncMethod
	ManyFiles          bool
	UntrackedCache     bool
	FSMonitor          bool // applied only when the host git supports fsmonitor--daemon
	PreloadIndex       *bool
	PackThreads        int // 0 leaves git default
	IndexThreads       int // 0 leaves git default
	GCAuto             *int
	MaintenanceAuto    *bool
	WriteCommitGraph   bool
	MultiPackIndex     bool
	CheckStatMinimal   bool
	TrustCTime         *bool
	SparseCones        []string
	Filter             CloneFilter
	Depth              int // 0 = full history
	SingleBranch       bool
	CheckoutWorkers    int // < 0 omits; 0 sets checkout.workers=0 only when CheckoutWorkersSet is true
	CheckoutWorkersSet bool
	ProtocolVersion    int // 0 leaves default; 2 sets protocol.version=2
}

func boolPtr(b bool) *bool { return &b }
func intPtr(i int) *int    { return &i }

// UntunedProfile returns a baseline unoptimized coding-agent git profile
// (partial blobless clone with default gc.auto, no untrackedCache, no
// commit-graph/MIDX prebuild).
func UntunedProfile() Profile {
	return Profile{
		Name:         "untuned",
		Filter:       FilterBlobNone,
		Depth:        1,
		SingleBranch: true,
	}
}

// TunedProfile returns the recommended native git-on-vfs profile for ephemeral
// sandboxes and FUSE mounts: batched committed fsync, feature.manyFiles,
// core.untrackedCache, core.preloadIndex, bounded pack threads, disabled
// background gc/maintenance, commit-graph, and multi-pack-index support.
//
// FSMonitor defaults to false for ephemeral sandboxes and FUSE mounts because
// `git fsmonitor--daemon` spawns a long-lived background process holding the
// working tree open (which blocks clean FUSE unmount unless explicitly stopped).
// Use TunedWithFSMonitorProfile() for long-lived sandboxes and call
// StopFSMonitor before unmount/teardown.
func TunedProfile() Profile {
	threads := min(runtime.GOMAXPROCS(0), 4)
	if threads < 1 {
		threads = 1
	}
	return Profile{
		Name:             "tuned",
		Fsync:            FsyncCommitted,
		FsyncMethod:      FsyncMethodBatch,
		ManyFiles:        true,
		UntrackedCache:   true,
		FSMonitor:        false,
		PreloadIndex:     boolPtr(true),
		PackThreads:      threads,
		IndexThreads:     threads,
		GCAuto:           intPtr(0),
		MaintenanceAuto:  boolPtr(false),
		WriteCommitGraph: true,
		MultiPackIndex:   true,
		Depth:            1,
		SingleBranch:     true,
		ProtocolVersion:  2,
	}
}

// TunedWithFSMonitorProfile returns TunedProfile with core.fsmonitor=true
// enabled (applied automatically when the host git supports fsmonitor--daemon).
// Callers MUST invoke StopFSMonitor before unmounting a FUSE volume or removing
// the workspace directory.
func TunedWithFSMonitorProfile() Profile {
	p := TunedProfile()
	p.Name = "tuned-fsmonitor"
	p.FSMonitor = true
	return p
}

// StopFSMonitor stops git's built-in fsmonitor--daemon for repoDir if running,
// releasing directory handles before FUSE unmount or temp-dir cleanup.
func StopFSMonitor(ctx context.Context, r *Runner, repoDir string) {
	if r == nil || repoDir == "" {
		return
	}
	_, _ = r.Run(ctx, repoDir, "fsmonitor--daemon", "stop")
}

// UnbatchedFsyncProfile enables core.fsync=committed WITHOUT batching, used to
// quantify loose-object fsync amplification vs BatchedFsyncProfile.
func UnbatchedFsyncProfile() Profile {
	p := TunedProfile()
	p.Name = "fsync-committed-unbatched"
	p.Fsync = FsyncCommitted
	p.FsyncMethod = FsyncMethodFsync
	return p
}

// BatchedFsyncProfile explicitly enables core.fsync=committed with
// core.fsyncMethod=batch.
func BatchedFsyncProfile() Profile {
	p := TunedProfile()
	p.Name = "fsync-committed-batch"
	p.Fsync = FsyncCommitted
	p.FsyncMethod = FsyncMethodBatch
	return p
}

// TunedFastProfile returns TunedProfile with git's default core.fsync policy
// (packs/indices synced, no loose-object/ref fsync overhead), matching the
// HIP-1342 recommendation for disposable sandboxes whose work is delivered by
// `git push`. Use TunedProfile() / BatchedFsyncProfile() when loose-object
// durability is explicitly required.
func TunedFastProfile() Profile {
	p := TunedProfile()
	p.Name = "tuned-fast"
	p.Fsync = FsyncDefault
	p.FsyncMethod = FsyncMethodDefault
	return p
}

// ReferenceProfile returns a tuned profile intended for clone-by-reference
// (shared read-only alternates + forge protocol v2).
func ReferenceProfile() Profile {
	p := TunedFastProfile()
	p.Name = "reference"
	return p
}

// SparseReferenceProfile returns a tuned clone-by-reference profile narrowed to
// specific cone-mode sparse-checkout directories.
func SparseReferenceProfile(cones ...string) Profile {
	p := ReferenceProfile()
	p.Name = "reference-sparse"
	p.SparseCones = append([]string(nil), cones...)
	return p
}

// SnapshotCloneProfile returns a tuned profile for COW Volume snapshot clones,
// setting core.checkStat=minimal and core.trustctime=false so the first
// `git status` does not re-hash every file when inode numbers or ctimes differ.
func SnapshotCloneProfile() Profile {
	p := TunedProfile()
	p.Name = "snapshot-clone"
	p.CheckStatMinimal = true
	p.TrustCTime = boolPtr(false)
	return p
}

// ConfigPairs returns deterministic, sorted "key=value" git configuration pairs.
// fsmonitorSupported controls whether core.fsmonitor=true is emitted; platforms
// without built-in fsmonitor--daemon support omit it cleanly.
func (p Profile) ConfigPairs(fsmonitorSupported bool) []string {
	kv := make(map[string]string)
	if p.Fsync != "" {
		kv["core.fsync"] = string(p.Fsync)
	}
	if p.FsyncMethod != "" {
		kv["core.fsyncMethod"] = string(p.FsyncMethod)
	}
	if p.ManyFiles {
		kv["feature.manyFiles"] = "true"
	}
	if p.UntrackedCache {
		kv["core.untrackedCache"] = "true"
	}
	if p.FSMonitor && fsmonitorSupported {
		kv["core.fsmonitor"] = "true"
	}
	if p.PreloadIndex != nil {
		kv["core.preloadIndex"] = strconv.FormatBool(*p.PreloadIndex)
	}
	if p.PackThreads > 0 {
		kv["pack.threads"] = strconv.Itoa(p.PackThreads)
	}
	if p.IndexThreads > 0 {
		kv["index.threads"] = strconv.Itoa(p.IndexThreads)
	}
	if p.GCAuto != nil {
		kv["gc.auto"] = strconv.Itoa(*p.GCAuto)
	}
	if p.MaintenanceAuto != nil {
		kv["maintenance.auto"] = strconv.FormatBool(*p.MaintenanceAuto)
	}
	if p.WriteCommitGraph {
		kv["core.commitGraph"] = "true"
		kv["fetch.writeCommitGraph"] = "true"
		kv["gc.writeCommitGraph"] = "true"
	}
	if p.MultiPackIndex {
		kv["core.multiPackIndex"] = "true"
		kv["gc.multiPackIndex"] = "true"
	}
	if p.CheckStatMinimal {
		kv["core.checkStat"] = "minimal"
	}
	if p.TrustCTime != nil {
		kv["core.trustctime"] = strconv.FormatBool(*p.TrustCTime)
	}
	if p.CheckoutWorkersSet {
		kv["checkout.workers"] = strconv.Itoa(p.CheckoutWorkers)
	}
	if p.ProtocolVersion > 0 {
		kv["protocol.version"] = strconv.Itoa(p.ProtocolVersion)
	}
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+kv[k])
	}
	return out
}

// ConfigArgs returns ["-c", "k=v", ...] for passing directly to git.
func (p Profile) ConfigArgs(fsmonitorSupported bool) []string {
	pairs := p.ConfigPairs(fsmonitorSupported)
	args := make([]string, 0, len(pairs)*2)
	for _, pair := range pairs {
		args = append(args, "-c", pair)
	}
	return args
}

// RenderGitConfig renders the profile as an INI-formatted git config snippet.
func (p Profile) RenderGitConfig(fsmonitorSupported bool) string {
	sections := make(map[string][][2]string)
	for _, pair := range p.ConfigPairs(fsmonitorSupported) {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		sec, field, ok := strings.Cut(k, ".")
		if !ok {
			continue
		}
		sections[sec] = append(sections[sec], [2]string{field, v})
	}
	secNames := make([]string, 0, len(sections))
	for s := range sections {
		secNames = append(secNames, s)
	}
	sort.Strings(secNames)

	var b strings.Builder
	for i, s := range secNames {
		if i > 0 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(&b, "[%s]\n", s)
		for _, kv := range sections[s] {
			fmt.Fprintf(&b, "\t%s = %s\n", kv[0], kv[1])
		}
	}
	return b.String()
}

// CloneArgs builds the git clone argument vector (starting with "clone") for
// remote -> dst, optionally using a shared alternates store and sparse cone.
func (p Profile) CloneArgs(remote, dst, alternatesStore string, fsmonitorSupported bool) []string {
	args := []string{"clone", "--quiet"}
	if p.Depth > 0 {
		args = append(args, "--depth", strconv.Itoa(p.Depth))
	}
	if p.SingleBranch {
		args = append(args, "--single-branch")
	}
	if p.Filter != "" {
		args = append(args, "--filter="+string(p.Filter))
	}
	if len(p.SparseCones) > 0 {
		args = append(args, "--sparse")
	}
	if alternatesStore != "" {
		args = append(args, "--reference-if-able", alternatesStore)
	}
	for _, pair := range p.ConfigPairs(fsmonitorSupported) {
		args = append(args, "-c", pair)
	}
	args = append(args, remote, dst)
	return args
}

// ApplyLocal writes the profile's configuration entries into repoDir/.git/config.
func (p Profile) ApplyLocal(ctx context.Context, r *Runner, repoDir string) error {
	fsmon := r.FSMonitorSupported(ctx)
	for _, pair := range p.ConfigPairs(fsmon) {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		if _, err := r.Run(ctx, repoDir, "config", "--local", k, v); err != nil {
			return fmt.Errorf("gitvfs: apply config %s=%s: %w", k, v, err)
		}
	}
	return nil
}

// OptimizeRepo runs local repository indexing maintenance enabled by the
// profile: cone-mode sparse-checkout, commit-graph with changed-paths (skipped
// for shallow clones where git does not write commit-graphs), and
// multi-pack-index (only when local packs exist; fresh alternates clones have
// zero local packs and inherit the alternates store's MIDX).
func (p Profile) OptimizeRepo(ctx context.Context, r *Runner, repoDir string) error {
	if len(p.SparseCones) > 0 {
		if _, err := r.Run(ctx, repoDir, "sparse-checkout", "init", "--cone"); err != nil {
			return fmt.Errorf("gitvfs: sparse-checkout init: %w", err)
		}
		args := append([]string{"sparse-checkout", "set"}, p.SparseCones...)
		if _, err := r.Run(ctx, repoDir, args...); err != nil {
			return fmt.Errorf("gitvfs: sparse-checkout set: %w", err)
		}
	}
	gitDir := filepath.Join(repoDir, ".git")
	if _, err := os.Stat(gitDir); err != nil {
		gitDir = repoDir // bare repository
	}
	if p.WriteCommitGraph {
		if _, shallowErr := os.Stat(filepath.Join(gitDir, "shallow")); os.IsNotExist(shallowErr) {
			if _, err := r.Run(ctx, repoDir, "commit-graph", "write", "--reachable", "--changed-paths"); err != nil {
				return fmt.Errorf("gitvfs: commit-graph write: %w", err)
			}
		}
	}
	if p.MultiPackIndex && hasLocalPackFiles(filepath.Join(gitDir, "objects", "pack")) {
		if _, err := r.Run(ctx, repoDir, "multi-pack-index", "write"); err != nil {
			return fmt.Errorf("gitvfs: multi-pack-index write: %w", err)
		}
	}
	return nil
}

func hasLocalPackFiles(packDir string) bool {
	entries, err := os.ReadDir(packDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".pack") {
			continue
		}
		// An empty git packfile is 32 bytes (SHA-1) or 44 bytes (SHA-256);
		// git clone --reference-if-able can leave a 0-object pack, for which
		// `git multi-pack-index write` produces a 0-OID MIDX rejected by fsck.
		if info, err := e.Info(); err == nil && info.Size() > 44 {
			return true
		}
	}
	return false
}

// CommandResult captures stdout/stderr and timing for a single real git call.
type CommandResult struct {
	Args     []string
	Stdout   string
	Stderr   string
	Duration time.Duration
}

// Runner executes real git commands in a hermetically isolated environment
// (no host ~/.gitconfig or /etc/gitconfig interference, no interactive prompts).
type Runner struct {
	GitBin     string
	GlobalCfg  string
	HomeDir    string
	AuthorName string
	AuthorMail string
	ExtraEnv   []string

	fsmonOnce      sync.Once
	fsmonSupported bool

	verOnce sync.Once
	verStr  string
}

// NewRunner creates an isolated git Runner with its temporary global config
// file placed under scratchDir.
func NewRunner(scratchDir string) (*Runner, error) {
	gitBin, err := exec.LookPath("git")
	if err != nil {
		return nil, fmt.Errorf("gitvfs: git binary not found: %w", err)
	}
	if scratchDir == "" {
		return nil, errors.New("gitvfs: scratchDir is required")
	}
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		return nil, err
	}
	homeDir := filepath.Join(scratchDir, "home")
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		return nil, err
	}
	globalCfg := filepath.Join(scratchDir, "gitconfig")
	const cfgContent = "[safe]\n\tdirectory = *\n[init]\n\tdefaultBranch = main\n[advice]\n\tdetachedHead = false\n"
	if err := os.WriteFile(globalCfg, []byte(cfgContent), 0o644); err != nil {
		return nil, err
	}
	return &Runner{
		GitBin:     gitBin,
		GlobalCfg:  globalCfg,
		HomeDir:    homeDir,
		AuthorName: "Hanzo Sandbox",
		AuthorMail: "sandbox@hanzo.ai",
	}, nil
}

func (r *Runner) env() []string {
	base := make([]string, 0, len(os.Environ())+12)
	for _, e := range os.Environ() {
		// Strip caller git overrides that would pollute sandbox determinism.
		switch {
		case strings.HasPrefix(e, "GIT_DIR="),
			strings.HasPrefix(e, "GIT_WORK_TREE="),
			strings.HasPrefix(e, "GIT_INDEX_FILE="),
			strings.HasPrefix(e, "GIT_OBJECT_DIRECTORY="),
			strings.HasPrefix(e, "GIT_ALTERNATE_OBJECT_DIRECTORIES="),
			strings.HasPrefix(e, "GIT_CONFIG_PARAMETERS="),
			strings.HasPrefix(e, "GIT_CONFIG_COUNT="),
			strings.HasPrefix(e, "GIT_CONFIG_GLOBAL="),
			strings.HasPrefix(e, "GIT_CONFIG_SYSTEM="),
			strings.HasPrefix(e, "GIT_CONFIG_NOSYSTEM="),
			strings.HasPrefix(e, "HOME="):
			continue
		}
		base = append(base, e)
	}
	base = append(base,
		"HOME="+r.HomeDir,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL="+r.GlobalCfg,
		"GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME="+r.AuthorName,
		"GIT_AUTHOR_EMAIL="+r.AuthorMail,
		"GIT_COMMITTER_NAME="+r.AuthorName,
		"GIT_COMMITTER_EMAIL="+r.AuthorMail,
		"LC_ALL=C",
	)
	base = append(base, r.ExtraEnv...)
	return base
}

// Run executes `git <args...>` in dir and returns combined timing and outputs.
func (r *Runner) Run(ctx context.Context, dir string, args ...string) (CommandResult, error) {
	cmd := exec.CommandContext(ctx, r.GitBin, args...)
	cmd.Dir = dir
	cmd.Env = r.env()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	err := cmd.Run()
	dur := time.Since(start)
	res := CommandResult{
		Args:     append([]string(nil), args...),
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Duration: dur,
	}
	if err != nil {
		return res, fmt.Errorf("git %s (dir=%s): %w: %s", strings.Join(args, " "), dir, err, strings.TrimSpace(res.Stderr))
	}
	return res, nil
}

// GitVersion returns the installed git version string.
func (r *Runner) GitVersion(ctx context.Context) string {
	r.verOnce.Do(func() {
		res, err := r.Run(ctx, r.HomeDir, "--version")
		if err == nil {
			r.verStr = strings.TrimSpace(res.Stdout)
		}
	})
	return r.verStr
}

// FSMonitorSupported probes whether the host git build supports the built-in
// fsmonitor--daemon (supported on macOS/Windows, typically unsupported on Linux).
func (r *Runner) FSMonitorSupported(ctx context.Context) bool {
	r.fsmonOnce.Do(func() {
		tmpDir, err := os.MkdirTemp(r.HomeDir, "fsmon-probe-*")
		if err != nil {
			return
		}
		defer os.RemoveAll(tmpDir)
		if _, err := r.Run(ctx, tmpDir, "init", "--quiet"); err != nil {
			return
		}
		res, err := r.Run(ctx, tmpDir, "fsmonitor--daemon", "status")
		combined := strings.ToLower(res.Stdout + " " + res.Stderr)
		if strings.Contains(combined, "not supported") || strings.Contains(combined, "not a git command") {
			r.fsmonSupported = false
			return
		}
		// "fsmonitor-daemon is not watching" exits non-zero when supported but stopped.
		if err == nil || strings.Contains(combined, "not watching") {
			r.fsmonSupported = true
		}
	})
	return r.fsmonSupported
}
