// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package gitvfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
)

func makeSeedRepo(t *testing.T, r *Runner) string {
	t.Helper()
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "seed-repo")
	dirs := []string{"pkg/alpha", "pkg/beta", "pkg/gamma", "docs"}
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(src, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Run(ctx, src, "init", "--quiet"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "README.md"), []byte("# Seed Repo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		for i := range 8 {
			p := filepath.Join(src, d, fmt.Sprintf("file_%02d.go", i))
			body := fmt.Sprintf("package p\n// Volume seed %s %d\nfunc F%d() int { return %d }\n", d, i, i, i)
			if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := r.Run(ctx, src, "add", "-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(ctx, src, "commit", "--quiet", "-m", "initial commit"); err != nil {
		t.Fatal(err)
	}
	// Second commit so history queries have multiple commits.
	if err := os.WriteFile(filepath.Join(src, "pkg/alpha/file_00.go"), []byte("package p\n// Volume updated\nfunc F0() int { return 42 }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(ctx, src, "add", "-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(ctx, src, "commit", "--quiet", "-m", "second commit"); err != nil {
		t.Fatal(err)
	}
	return src
}

func TestTunedProfileConfigAndRealGit(t *testing.T) {
	ctx := context.Background()
	r, err := NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	src := makeSeedRepo(t, r)
	forgeDir := filepath.Join(t.TempDir(), "forge.git")
	forge, err := InitForgeFromRepo(ctx, r, src, forgeDir)
	if err != nil {
		t.Fatal(err)
	}

	prof := TunedProfile()
	fsmonProf := TunedWithFSMonitorProfile()
	rendered := fsmonProf.RenderGitConfig(true)
	for _, want := range []string{
		"fsync = committed",
		"fsyncMethod = batch",
		"manyFiles = true",
		"untrackedCache = true",
		"preloadIndex = true",
		"fsmonitor = true",
		"auto = 0",
		"commitGraph = true",
		"multiPackIndex = true",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("RenderGitConfig missing %q:\n%s", want, rendered)
		}
	}
	// When fsmonitor is unsupported, core.fsmonitor must be omitted cleanly.
	if strings.Contains(fsmonProf.RenderGitConfig(false), "fsmonitor = true") {
		t.Error("expected core.fsmonitor to be omitted when fsmonitorSupported=false")
	}

	cloneDir := filepath.Join(t.TempDir(), "tuned-clone")
	sc, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:  "tenant-a",
		SandboxID: "sb-tuned",
		RepoID:    "seed",
		RemoteURL: forge.FileURL(),
		TargetDir: cloneDir,
		Profile:   prof,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify local git config values actually landed in the repo.
	checks := map[string]string{
		"core.fsync":          "committed",
		"core.fsyncMethod":    "batch",
		"feature.manyFiles":   "true",
		"core.untrackedCache": "true",
		"core.preloadIndex":   "true",
		"gc.auto":             "0",
		"maintenance.auto":    "false",
		"core.commitGraph":    "true",
		"core.multiPackIndex": "true",
	}
	for k, want := range checks {
		res, err := r.Run(ctx, sc.Dir, "config", "--local", "--get", k)
		if err != nil {
			t.Errorf("git config --local --get %s: %v", k, err)
			continue
		}
		if got := strings.TrimSpace(res.Stdout); got != want {
			t.Errorf("config %s = %q, want %q", k, got, want)
		}
	}

	// Verify commit-graph and multi-pack-index are generated on a non-shallow tuned clone
	// (git intentionally skips commit-graph when .git/shallow exists).
	fullProf := TunedProfile()
	fullProf.Depth = 0
	fullClone, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:  "tenant-a",
		SandboxID: "sb-tuned-full",
		RepoID:    "seed",
		RemoteURL: forge.FileURL(),
		TargetDir: filepath.Join(t.TempDir(), "tuned-full-clone"),
		Profile:   fullProf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fullClone.Dir, ".git", "objects", "info", "commit-graph")); err != nil {
		t.Errorf("expected commit-graph to exist on non-shallow clone: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fullClone.Dir, ".git", "objects", "pack", "multi-pack-index")); err != nil {
		t.Errorf("expected multi-pack-index to exist on clone with local pack: %v", err)
	}
}

func TestForgeProtocolV2BitmapsAndCommitGraph(t *testing.T) {
	ctx := context.Background()
	r, err := NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	src := makeSeedRepo(t, r)
	forgeDir := filepath.Join(t.TempDir(), "forge.git")
	forge, err := InitForgeWithCorpus(ctx, r, src, forgeDir, 20, 42)
	if err != nil {
		t.Fatal(err)
	}
	st, err := forge.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.ProtocolVersion != "2" {
		t.Errorf("protocol.version = %q, want 2", st.ProtocolVersion)
	}
	if !st.AllowFilter || !st.AllowAnySHA1InWant {
		t.Errorf("expected allowFilter & allowAnySHA1InWant enabled, got %+v", st)
	}
	if !st.HasBitmapIndex {
		t.Error("expected .bitmap index in bare forge")
	}
	if !st.HasCommitGraph {
		t.Error("expected commit-graph in bare forge")
	}
	if !st.HasMultiPackIndex {
		t.Error("expected multi-pack-index in bare forge")
	}
	if !st.FsckClean {
		t.Error("expected bare forge fsck clean")
	}
	if forge.BulkBranch != "bulk" {
		t.Errorf("BulkBranch = %q, want bulk", forge.BulkBranch)
	}
}

func TestAlternatesReadOnlyNonMutationAndPushRoundTrip(t *testing.T) {
	ctx := context.Background()
	r, err := NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	src := makeSeedRepo(t, r)
	forge, err := InitForgeFromRepo(ctx, r, src, filepath.Join(t.TempDir(), "forge.git"))
	if err != nil {
		t.Fatal(err)
	}

	altDir := filepath.Join(t.TempDir(), "alternates.git")
	altStore, err := NewAlternatesStore(ctx, r, "tenant-alpha", "vfs", forge.FileURL(), altDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := altStore.SealReadOnly(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = altStore.UnsealWritable() }()

	sc, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:   "tenant-alpha",
		SandboxID:  "sb-1",
		RepoID:     "vfs",
		RemoteURL:  forge.FileURL(),
		TargetDir:  filepath.Join(t.TempDir(), "sb1-work"),
		Profile:    ReferenceProfile(),
		Alternates: altStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sc.AlternatesPath != altStore.ObjectsDir() {
		t.Fatalf("alternates path = %q, want %q", sc.AlternatesPath, altStore.ObjectsDir())
	}

	counts, err := sc.CountOwnObjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.OwnObjects != 0 {
		t.Fatalf("expected 0 own objects right after clone-by-reference, got %d", counts.OwnObjects)
	}

	pushRes, err := sc.CommitAndPush(ctx, forge, "pkg/alpha/sandbox_change.go", []byte("package p\n// sandbox edit\n"), "sandbox commit", "feature-1")
	if err != nil {
		t.Fatal(err)
	}
	if !pushRes.ForgeFsckOK || !pushRes.SandboxFsck {
		t.Fatalf("expected clean fsck on forge & sandbox: %+v", pushRes)
	}
	if pushRes.ForgeSHA != pushRes.CommitSHA {
		t.Fatalf("forge SHA %s != commit SHA %s", pushRes.ForgeSHA, pushRes.CommitSHA)
	}

	// Run local git gc in sandbox: must leave alternates intact and not copy base packs.
	if _, err := r.Run(ctx, sc.Dir, "gc", "--quiet", "--prune=now"); err != nil {
		t.Fatal(err)
	}
	afterGC, err := sc.CountOwnObjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterGC.OwnObjects >= 12 {
		t.Fatalf("gc copied borrowed objects into sandbox: own=%d", afterGC.OwnObjects)
	}

	// Cryptographically verify zero mutation of read-only alternates store.
	if err := altStore.VerifyUnmodified(); err != nil {
		t.Fatalf("alternates store was mutated: %v", err)
	}
}

func TestMultiTenantAndCrossSandboxIsolation(t *testing.T) {
	ctx := context.Background()
	r, err := NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	src := makeSeedRepo(t, r)
	forge, err := InitForgeFromRepo(ctx, r, src, filepath.Join(t.TempDir(), "forge.git"))
	if err != nil {
		t.Fatal(err)
	}

	altA, err := NewAlternatesStore(ctx, r, "tenant-a", "repo-1", forge.FileURL(), filepath.Join(t.TempDir(), "alt-a.git"))
	if err != nil {
		t.Fatal(err)
	}
	reg := NewTenantRegistry()
	if err := reg.Register(altA); err != nil {
		t.Fatal(err)
	}

	// Tenant B must be rejected from borrowing Tenant A's alternates store.
	if _, err := reg.Resolve("tenant-b", "repo-1"); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch for cross-tenant Resolve, got %v", err)
	}
	if _, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:  "tenant-b",
		SandboxID: "sb-intruder",
		RepoID:    "repo-1",
		RemoteURL: forge.FileURL(),
		TargetDir: filepath.Join(t.TempDir(), "intruder"),
		Profile:   ReferenceProfile(),
		Registry:  reg,
	}); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch on cross-tenant CloneByReference, got %v", err)
	}

	// Two sandboxes in Tenant A push to isolated sandbox namespaces without collision.
	sb1, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:  "tenant-a",
		SandboxID: "sb-1",
		RepoID:    "repo-1",
		RemoteURL: forge.FileURL(),
		TargetDir: filepath.Join(t.TempDir(), "sb1"),
		Profile:   ReferenceProfile(),
		Registry:  reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	sb2, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:  "tenant-a",
		SandboxID: "sb-2",
		RepoID:    "repo-1",
		RemoteURL: forge.FileURL(),
		TargetDir: filepath.Join(t.TempDir(), "sb2"),
		Profile:   ReferenceProfile(),
		Registry:  reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	p1, err := sb1.CommitAndPush(ctx, forge, "pkg/alpha/only_sb1.txt", []byte("sb1\n"), "sb1 work", "work")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := sb2.CommitAndPush(ctx, forge, "pkg/alpha/only_sb2.txt", []byte("sb2\n"), "sb2 work", "work")
	if err != nil {
		t.Fatal(err)
	}
	if p1.Branch == p2.Branch {
		t.Fatalf("sandbox refs collided: %q == %q", p1.Branch, p2.Branch)
	}
	if p1.CommitSHA == p2.CommitSHA {
		t.Fatal("expected distinct commits from distinct sandboxes")
	}
	if _, err := os.Stat(filepath.Join(sb2.Dir, "pkg/alpha/only_sb1.txt")); !os.IsNotExist(err) {
		t.Fatal("sb1 file leaked into sb2 working tree")
	}
}

func TestSparseCheckoutConeMode(t *testing.T) {
	ctx := context.Background()
	r, err := NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	src := makeSeedRepo(t, r)
	forge, err := InitForgeFromRepo(ctx, r, src, filepath.Join(t.TempDir(), "forge.git"))
	if err != nil {
		t.Fatal(err)
	}
	alt, err := NewAlternatesStore(ctx, r, "tenant-a", "seed", forge.FileURL(), filepath.Join(t.TempDir(), "alt.git"))
	if err != nil {
		t.Fatal(err)
	}

	prof := SparseReferenceProfile("pkg/alpha")
	sc, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:   "tenant-a",
		SandboxID:  "sb-sparse",
		RepoID:     "seed",
		RemoteURL:  forge.FileURL(),
		TargetDir:  filepath.Join(t.TempDir(), "sparse-work"),
		Profile:    prof,
		Alternates: alt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(sc.Dir, "pkg/alpha/file_00.go")); err != nil {
		t.Fatalf("expected cone file pkg/alpha/file_00.go to exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(sc.Dir, "pkg/gamma/file_00.go")); !os.IsNotExist(err) {
		t.Fatalf("expected out-of-cone file pkg/gamma/file_00.go not materialized, got err=%v", err)
	}
	pr, err := sc.CommitAndPush(ctx, forge, "pkg/alpha/sparse_edit.go", []byte("package p\n// sparse\n"), "sparse commit", "sparse-br")
	if err != nil {
		t.Fatal(err)
	}
	if !pr.ForgeFsckOK {
		t.Fatal("expected clean forge fsck after sparse checkout push")
	}
}

func TestCOWVolumeCloneByReferenceO1AndZeroBaseMutation(t *testing.T) {
	ctx := context.Background()
	r, err := NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	src := makeSeedRepo(t, r)
	forge, err := InitForgeFromRepo(ctx, r, src, filepath.Join(t.TempDir(), "forge.git"))
	if err != nil {
		t.Fatal(err)
	}

	baseCheckout := filepath.Join(t.TempDir(), "base-checkout")
	prof := SnapshotCloneProfile()
	if _, err := CloneByReference(ctx, r, SandboxCloneOptions{
		RemoteURL: forge.FileURL(),
		TargetDir: baseCheckout,
		Profile:   prof,
	}); err != nil {
		t.Fatal(err)
	}
	headRes, err := r.Run(ctx, baseCheckout, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}

	jRoot, jCleanup, err := NewNonVolatileJournalDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer jCleanup()

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	cipher := vfs.Hybrid(key)

	snap, err := CreateVolumeSnapshot(
		ctx, cipher, "tenant-cow", "seed",
		baseCheckout,
		filepath.Join(t.TempDir(), "base-store"),
		filepath.Join(jRoot, "base-journal"),
		strings.TrimSpace(headRes.Stdout), forge.DefaultBranch,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	// Cross-tenant clone must be refused.
	if _, err := CloneVolumeByReference(ctx, snap, "tenant-other", "sb-x", filepath.Join(t.TempDir(), "d-x"), filepath.Join(jRoot, "j-x")); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch for cross-tenant volume clone, got %v", err)
	}

	baseBefore := snap.BaseBackend.Snapshot()
	const nClones = 4
	for i := range nClones {
		sbID := fmt.Sprintf("sb-%d", i)
		cv, err := CloneVolumeByReference(ctx, snap, "tenant-cow", sbID, filepath.Join(t.TempDir(), "delta-"+sbID), filepath.Join(jRoot, "journal-"+sbID))
		if err != nil {
			t.Fatalf("clone %s: %v", sbID, err)
		}
		// O(1) clone MUST copy zero packs and zero meta shards.
		if cv.ClonePuts.PackPuts != 0 || cv.ClonePuts.MetaPuts != 0 {
			t.Errorf("clone %s copied data: pack=%d meta=%d, want 0/0", sbID, cv.ClonePuts.PackPuts, cv.ClonePuts.MetaPuts)
		}
		if cv.ClonePuts.HeadPuts == 0 {
			t.Errorf("clone %s expected resealed head PUT", sbID)
		}
		// Sealed SnapshotMetadata must be readable through COWBackend.
		meta, err := ReadSnapshotMetadata(ctx, cv.Volume)
		if err != nil {
			t.Fatalf("clone %s ReadSnapshotMetadata: %v", sbID, err)
		}
		if meta.TenantID != "tenant-cow" || !meta.CredentialScrubbed {
			t.Errorf("unexpected snapshot metadata: %+v", meta)
		}
		// Mutate & flush inside sandbox without affecting base.
		if _, err := WriteVolumeFile(ctx, cv.Volume, fmt.Sprintf("pkg/alpha/cow_%d.txt", i), []byte(fmt.Sprintf("from %s\n", sbID)), 0o644, true); err != nil {
			t.Fatal(err)
		}
		if err := cv.Volume.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := cv.Volume.Check(ctx); err != nil {
			t.Fatalf("clone %s Check: %v", sbID, err)
		}
		if i == 0 {
			expDir := filepath.Join(t.TempDir(), "export-0")
			if _, _, err := ExportVolumeToDir(ctx, cv.Volume, expDir); err != nil {
				t.Fatal(err)
			}
			sc := &SandboxClone{TenantID: "tenant-cow", SandboxID: sbID, RepoID: "seed", Dir: expDir, Profile: prof, runner: r}
			pr, err := sc.CommitAndPush(ctx, forge, "pkg/alpha/cow_0.txt", []byte("from sb-0\n"), "cow commit", "cow-br")
			if err != nil {
				t.Fatalf("cow export commit+push: %v", err)
			}
			if !pr.ForgeFsckOK {
				t.Fatal("forge fsck failed after COW sandbox push")
			}
		}
		if err := cv.Volume.Close(); err != nil {
			t.Fatal(err)
		}
	}

	baseDelta := snap.BaseBackend.Snapshot().Delta(baseBefore)
	if baseDelta.TotalPuts != 0 || baseDelta.Deletes != 0 {
		t.Fatalf("read-only base snapshot backend was mutated: puts=%d deletes=%d", baseDelta.TotalPuts, baseDelta.Deletes)
	}
}

func TestCredentialLeakRejectedInSnapshot(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	cases := []string{
		"[remote \"origin\"]\n\turl = https://alice:s3cr3t@forge.example/org/repo.git\n",
		"[http]\n\textraHeader = Authorization: Basic dXNlcjpwYXNz\n",
		"[remote \"origin\"]\n\turl = https://" + "ghp_" + "testtokenfixture@github.com/org/repo.git\n",
	}
	for i, cfg := range cases {
		if err := os.WriteFile(filepath.Join(dir, ".git", "config"), []byte(cfg), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := VerifyNoCredentials(dir); !errors.Is(err, ErrCredentialLeak) {
			t.Errorf("case %d: expected ErrCredentialLeak, got %v", i, err)
		}
	}
	// Clean URL must pass.
	if err := os.WriteFile(filepath.Join(dir, ".git", "config"), []byte("[remote \"origin\"]\n\turl = https://forge.example/org/repo.git\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := VerifyNoCredentials(dir); err != nil {
		t.Errorf("clean config unexpectedly failed: %v", err)
	}
}

func TestBatchedFsyncAccountingAndJournalMetrics(t *testing.T) {
	ctx := context.Background()
	jRoot, jCleanup, err := NewNonVolatileJournalDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer jCleanup()

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	cipher := vfs.Hybrid(key)

	runBatch := func(name string, perFileSync bool) int64 {
		rawBe, err := openFileBackend(ctx, filepath.Join(t.TempDir(), name+"-store"))
		if err != nil {
			t.Fatal(err)
		}
		cntBe := NewCountingBackend(rawBe)
		jt := NewJournalTracker()
		cfg := vfs.Config{
			Backend:         cntBe,
			Cipher:          cipher,
			Round:           1,
			Journal:         filepath.Join(jRoot, name+"-journal"),
			JournalCapacity: 64 << 20,
			Bound:           5 * time.Second,
			Disk:            jt.DiskFactory(),
		}
		if err := vfs.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		vol, err := vfs.Attach(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer vol.Close()

		before := jt.Snapshot().Syncs
		for i := range 32 {
			if _, err := WriteVolumeFile(ctx, vol, fmt.Sprintf("objs/o-%02d", i), []byte(fmt.Sprintf("blob-%d\n", i)), 0o644, perFileSync); err != nil {
				t.Fatal(err)
			}
		}
		if !perFileSync {
			if err := vol.Sync(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return jt.Snapshot().Syncs - before
	}

	unbatchedSyncs := runBatch("unbatched", true)
	batchedSyncs := runBatch("batched", false)
	if unbatchedSyncs < 32 {
		t.Errorf("expected >= 32 journal syncs for unbatched per-object fsync, got %d", unbatchedSyncs)
	}
	if batchedSyncs > 3 {
		t.Errorf("expected <= 3 journal syncs for batched fsync, got %d", batchedSyncs)
	}
	if batchedSyncs >= unbatchedSyncs {
		t.Errorf("expected batched syncs (%d) << unbatched syncs (%d)", batchedSyncs, unbatchedSyncs)
	}
}

func TestZeroSyncPutOnCloseAndPublishWithin5s(t *testing.T) {
	ctx := context.Background()
	jRoot, jCleanup, err := NewNonVolatileJournalDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer jCleanup()

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	rawBe, err := openFileBackend(ctx, filepath.Join(t.TempDir(), "close-store"))
	if err != nil {
		t.Fatal(err)
	}
	cntBe := NewCountingBackend(rawBe)
	cfg := vfs.Config{
		Backend:         cntBe,
		Cipher:          vfs.Hybrid(key),
		Round:           1,
		Journal:         filepath.Join(jRoot, "close-journal"),
		JournalCapacity: 64 << 20,
		Bound:           1 * time.Second,
	}
	if err := vfs.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	vol, err := vfs.Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	res, err := VerifyCloseAndPublishBound(ctx, vol, cntBe, "git/pack-001.bin", []byte(strings.Repeat("x", 32<<10)), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !res.ZeroSyncPutOnClose || res.PutsDuringWriteSyncClose != 0 {
		t.Fatalf("expected 0 synchronous backend PUTs on clean Close, got %+v", res)
	}
	if !res.PublishedWithinBound || res.PublishPuts == 0 {
		t.Fatalf("expected automatic background publication within 5s, got %+v", res)
	}
}

func TestNegativeRegimesAndAlternatesPruneHazard(t *testing.T) {
	ctx := context.Background()
	r, err := NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	src := makeSeedRepo(t, r)
	forge, err := InitForgeFromRepo(ctx, r, src, filepath.Join(t.TempDir(), "forge.git"))
	if err != nil {
		t.Fatal(err)
	}

	alt, err := NewAlternatesStore(ctx, r, "tenant-h", "seed", forge.FileURL(), filepath.Join(t.TempDir(), "alt-haz.git"))
	if err != nil {
		t.Fatal(err)
	}
	borrower, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:   "tenant-h",
		SandboxID:  "sb-live",
		RepoID:     "seed",
		RemoteURL:  forge.FileURL(),
		TargetDir:  filepath.Join(t.TempDir(), "live-borrower"),
		Profile:    ReferenceProfile(),
		Alternates: alt,
	})
	if err != nil {
		t.Fatal(err)
	}
	dissoc, err := CloneByReference(ctx, r, SandboxCloneOptions{
		TenantID:   "tenant-h",
		SandboxID:  "sb-dissoc",
		RepoID:     "seed",
		RemoteURL:  forge.FileURL(),
		TargetDir:  filepath.Join(t.TempDir(), "dissoc-borrower"),
		Profile:    ReferenceProfile(),
		Alternates: alt,
	})
	if err != nil {
		t.Fatal(err)
	}

	// SafePrune must refuse while borrowers are attached.
	if err := alt.SafePrune(ctx); !errors.Is(err, ErrActiveBorrowers) {
		t.Fatalf("expected ErrActiveBorrowers from SafePrune, got %v", err)
	}

	// Dissociate one borrower; it must survive a forced store prune.
	if err := dissoc.Dissociate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := alt.ForcePruneAllRefsForHazardTest(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Run(ctx, dissoc.Dir, "fsck", "--no-dangling"); err != nil {
		t.Errorf("dissociated clone should survive store prune: %v", err)
	}
	if _, err := r.Run(ctx, borrower.Dir, "fsck", "--no-dangling"); err == nil {
		t.Error("expected non-dissociated borrower fsck to fail after forced store prune")
	}
}

func TestConcurrentSandboxesRaceSafety(t *testing.T) {
	ctx := context.Background()
	r, err := NewRunner(filepath.Join(t.TempDir(), "env"))
	if err != nil {
		t.Fatal(err)
	}
	src := makeSeedRepo(t, r)
	forge, err := InitForgeFromRepo(ctx, r, src, filepath.Join(t.TempDir(), "forge.git"))
	if err != nil {
		t.Fatal(err)
	}
	alt, err := NewAlternatesStore(ctx, r, "tenant-c", "seed", forge.FileURL(), filepath.Join(t.TempDir(), "alt-c.git"))
	if err != nil {
		t.Fatal(err)
	}
	if err := alt.SealReadOnly(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = alt.UnsealWritable() }()

	reg := NewTenantRegistry()
	if err := reg.Register(alt); err != nil {
		t.Fatal(err)
	}

	jRoot, jCleanup, err := NewNonVolatileJournalDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer jCleanup()

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	baseCheckout := filepath.Join(t.TempDir(), "base-co")
	if _, err := CloneByReference(ctx, r, SandboxCloneOptions{RemoteURL: forge.FileURL(), TargetDir: baseCheckout, Profile: SnapshotCloneProfile()}); err != nil {
		t.Fatal(err)
	}
	snap, err := CreateVolumeSnapshot(ctx, vfs.Hybrid(key), "tenant-c", "seed", baseCheckout, filepath.Join(t.TempDir(), "base-s"), filepath.Join(jRoot, "base-j"), "HEAD", forge.DefaultBranch)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	const workers = 4
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	baseDir := t.TempDir()

	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sbID := fmt.Sprintf("conc-%d", idx)
			// 1. Concurrent read-only alternates clone + isolated push.
			sc, err := CloneByReference(ctx, r, SandboxCloneOptions{
				TenantID:  "tenant-c",
				SandboxID: sbID,
				RepoID:    "seed",
				RemoteURL: forge.FileURL(),
				TargetDir: filepath.Join(baseDir, "alt-"+sbID),
				Profile:   ReferenceProfile(),
				Registry:  reg,
			})
			if err != nil {
				errCh <- fmt.Errorf("%s clone: %w", sbID, err)
				return
			}
			if _, err := sc.CommitAndPush(ctx, forge, fmt.Sprintf("pkg/alpha/c_%d.txt", idx), []byte(sbID+"\n"), "concurrent "+sbID, "br"); err != nil {
				errCh <- fmt.Errorf("%s push: %w", sbID, err)
				return
			}
			// 2. Concurrent O(1) COW Volume clone + write + flush.
			cv, err := CloneVolumeByReference(ctx, snap, "tenant-c", sbID, filepath.Join(baseDir, "cow-d-"+sbID), filepath.Join(jRoot, "cow-j-"+sbID))
			if err != nil {
				errCh <- fmt.Errorf("%s cow clone: %w", sbID, err)
				return
			}
			defer cv.Volume.Close()
			if _, err := WriteVolumeFile(ctx, cv.Volume, fmt.Sprintf("pkg/beta/cow_%d.txt", idx), []byte(sbID+"\n"), 0o644, true); err != nil {
				errCh <- fmt.Errorf("%s cow write: %w", sbID, err)
				return
			}
			if err := cv.Volume.Flush(ctx); err != nil {
				errCh <- fmt.Errorf("%s cow flush: %w", sbID, err)
				return
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if err := alt.VerifyUnmodified(); err != nil {
		t.Errorf("alternates store modified under concurrency: %v", err)
	}
	if got := snap.BaseBackend.Snapshot(); got.TotalPuts != 0 || got.Deletes != 0 {
		t.Errorf("base snapshot backend mutated under concurrency: puts=%d deletes=%d", got.TotalPuts, got.Deletes)
	}
}
