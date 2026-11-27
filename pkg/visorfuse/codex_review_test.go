// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package visorfuse_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/visorfuse"
)

// TestRestoreRejectsNewerFilesystemGeneration verifies that restoring an
// earlier checkpoint fails closed when another node has resumed the volume and
// published a newer filesystem Head generation (even with OverrideNextRound).
func TestRestoreRejectsNewerFilesystemGeneration(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visorfuse-codex")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/acme/v/vol-gen")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-gen")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)
	vfsCipher := newHybridCipher(t)
	hmacKey := testHMACKey()

	if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher}); err != nil {
		t.Fatal(err)
	}
	volA, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	spA, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spA.Close()
	specA := validMountSpec(spA.HostFD, visorfuse.SocketKindStream, "acme", "vol-gen")
	srvA, err := visorfuse.NewExternalFUSEServer(specA, volA, spA.HostConn, visorfuse.WithCountingBackend(vfsBE))
	if err != nil {
		t.Fatal(err)
	}
	srvCtxA, cancelA := context.WithCancel(ctx)
	doneA := make(chan error, 1)
	go func() { doneA <- srvA.Serve(srvCtxA) }()
	cliA := visorfuse.NewGuestFUSEClient(spA.GuestConn, visorfuse.SocketKindStream)
	if _, err := cliA.Init(ctx, visorfuse.InitRequest{
		TenantID: "acme", VolumeID: "vol-gen",
		Flags: visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
	}); err != nil {
		t.Fatal(err)
	}
	attr, fh, err := cliA.Create(ctx, uint64(vfs.RootInode), "state.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cliA.Write(ctx, fh, 0, []byte("gen1-data")); err != nil {
		t.Fatal(err)
	}
	if err := cliA.Fsync(ctx, attr.Inode, fh); err != nil {
		t.Fatal(err)
	}
	if err := cliA.Release(ctx, fh); err != nil {
		t.Fatal(err)
	}

	m1, err := visorfuse.SuspendSandbox(ctx, srvA, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-gen-1",
		SandboxID:     "sbx-gen",
		SourceNodeID:  "node-a",
		TenantID:      "acme",
		VolumeID:      "vol-gen",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        deterministicMemory(0x41, 128<<10),
		Descriptors: []visorfuse.OpenDescriptor{
			{FD: 3, Path: "/state.bin", Inode: attr.Inode, Size: 9},
		},
	}, visorfuse.CheckpointConfig{
		Store:    ckptBE,
		HMACKey:  hmacKey,
		Compress: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelA()
	_ = cliA.Close()
	_ = srvA.Close()
	<-doneA

	// Node B restores at Round 2, modifies state.bin, and publishes a newer VFS HeadSeq.
	spB, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spB.Close()
	restoredB, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-b",
		TargetTenantID:      "acme",
		TargetVolumeID:      "vol-gen",
		CheckpointID:        "ckpt-gen-1",
		CheckpointStore:     ckptBE,
		HMACKey:             hmacKey,
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     validMountSpec(spB.HostFD, visorfuse.SocketKindStream, "acme", "vol-gen"),
		TargetHostConn:      spB.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
	})
	if err != nil {
		t.Fatalf("RestoreToNode #1: %v", err)
	}
	f, err := restoredB.Volume.Open(ctx, vfs.InodeID(attr.Inode))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(ctx, []byte("gen2-diverged-data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := restoredB.Server.FlushAndQuiesce(ctx); err != nil {
		t.Fatal(err)
	}
	tokB, err := restoredB.Server.ReleaseRound(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tokB.PublishedHeadSeq <= m1.VFSRelease.PublishedHeadSeq {
		t.Fatalf("expected Node B HeadSeq (%d) > checkpoint HeadSeq (%d)", tokB.PublishedHeadSeq, m1.VFSRelease.PublishedHeadSeq)
	}
	_ = restoredB.Server.Close()

	// Node C attempts to restore the earlier checkpoint ckpt-gen-1 using OverrideNextRound.
	// It MUST fail closed because the volume HeadSeq has advanced past the checkpoint.
	spC, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spC.Close()
	_, err = visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-c",
		TargetTenantID:      "acme",
		TargetVolumeID:      "vol-gen",
		CheckpointID:        "ckpt-gen-1",
		CheckpointStore:     ckptBE,
		HMACKey:             hmacKey,
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     validMountSpec(spC.HostFD, visorfuse.SocketKindStream, "acme", "vol-gen"),
		TargetHostConn:      spC.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
		OverrideNextRound:   tokB.NextRound,
	})
	if !errors.Is(err, visorfuse.ErrCheckpointTampered) {
		t.Fatalf("RestoreToNode against newer VFS generation = %v, want ErrCheckpointTampered", err)
	}
}

// TestRestoreRebindsSavedFUSEHandlesAndAllocator verifies that open FUSE wire
// handle IDs and the handle allocator watermark are persisted in the checkpoint
// and reconstructed on the restored ExternalFUSEServer so a resumed guest can
// immediately READ/WRITE using pre-checkpoint wire handles without collisions.
func TestRestoreRebindsSavedFUSEHandlesAndAllocator(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visorfuse-codex")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/acme/v/vol-hdl")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-hdl")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)
	vfsCipher := newHybridCipher(t)
	hmacKey := testHMACKey()

	if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher}); err != nil {
		t.Fatal(err)
	}
	volA, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	spA, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spA.Close()
	srvA, err := visorfuse.NewExternalFUSEServer(validMountSpec(spA.HostFD, visorfuse.SocketKindStream, "acme", "vol-hdl"), volA, spA.HostConn, visorfuse.WithCountingBackend(vfsBE))
	if err != nil {
		t.Fatal(err)
	}
	srvCtxA, cancelA := context.WithCancel(ctx)
	doneA := make(chan error, 1)
	go func() { doneA <- srvA.Serve(srvCtxA) }()
	cliA := visorfuse.NewGuestFUSEClient(spA.GuestConn, visorfuse.SocketKindStream)
	if _, err := cliA.Init(ctx, visorfuse.InitRequest{
		TenantID: "acme", VolumeID: "vol-hdl",
		Flags: visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
	}); err != nil {
		t.Fatal(err)
	}

	// Create two files: release h1 (handle 1), keep h2 (handle 2) open across checkpoint!
	a1, h1, err := cliA.Create(ctx, uint64(vfs.RootInode), "first.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cliA.Write(ctx, h1, 0, []byte("file-one")); err != nil {
		t.Fatal(err)
	}
	if err := cliA.Release(ctx, h1); err != nil {
		t.Fatal(err)
	}

	a2, h2, err := cliA.Create(ctx, uint64(vfs.RootInode), "live.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if h2 != 2 {
		t.Fatalf("expected second handle ID == 2, got %d", h2)
	}
	if _, err := cliA.Write(ctx, h2, 0, []byte("hello-pre-ckpt")); err != nil {
		t.Fatal(err)
	}
	if err := cliA.Fsync(ctx, a2.Inode, h2); err != nil {
		t.Fatal(err)
	}
	// Intentionally keep h2 open across SuspendSandbox!
	manifest, err := visorfuse.SuspendSandbox(ctx, srvA, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-hdl-1",
		SandboxID:     "sbx-hdl",
		SourceNodeID:  "node-a",
		TenantID:      "acme",
		VolumeID:      "vol-hdl",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        deterministicMemory(0x52, 128<<10),
		Descriptors: []visorfuse.OpenDescriptor{
			{FD: 4, Handle: h2, Path: "/live.txt", Inode: a2.Inode},
		},
	}, visorfuse.CheckpointConfig{
		Store:    ckptBE,
		HMACKey:  hmacKey,
		Compress: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.SavedHandles) != 1 || manifest.SavedHandles[0].Handle != h2 || manifest.NextHandle <= h2 {
		t.Fatalf("unexpected SavedHandles=%+v NextHandle=%d", manifest.SavedHandles, manifest.NextHandle)
	}
	cancelA()
	_ = cliA.Close()
	_ = srvA.Close()
	<-doneA

	// Restore on Node B with live host socket and immediately use h2 without re-opening.
	spB, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spB.Close()
	restored, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-b",
		TargetTenantID:      "acme",
		TargetVolumeID:      "vol-hdl",
		CheckpointID:        "ckpt-hdl-1",
		CheckpointStore:     ckptBE,
		HMACKey:             hmacKey,
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     validMountSpec(spB.HostFD, visorfuse.SocketKindStream, "acme", "vol-hdl"),
		TargetHostConn:      spB.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
	})
	if err != nil {
		t.Fatalf("RestoreToNode: %v", err)
	}
	defer restored.Volume.Close()
	srvCtxB, cancelB := context.WithCancel(ctx)
	doneB := make(chan error, 1)
	go func() { doneB <- restored.Server.Serve(srvCtxB) }()
	defer func() {
		cancelB()
		_ = restored.Server.Close()
		<-doneB
	}()

	cliB := visorfuse.NewGuestFUSEClient(spB.GuestConn, visorfuse.SocketKindStream)
	defer cliB.Close()
	if _, err := cliB.Init(ctx, visorfuse.InitRequest{
		TenantID: "acme", VolumeID: "vol-hdl",
		Flags: visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
	}); err != nil {
		t.Fatal(err)
	}

	// Resumed guest reads and writes directly on saved wire handle h2!
	got, err := cliB.Read(ctx, h2, 0, 64)
	if err != nil {
		t.Fatalf("Read on restored wire handle %d failed: %v", h2, err)
	}
	if string(got) != "hello-pre-ckpt" {
		t.Fatalf("Read on restored handle = %q, want %q", got, "hello-pre-ckpt")
	}
	if _, err := cliB.Write(ctx, h2, uint64(len(got)), []byte("+post-restore")); err != nil {
		t.Fatalf("Write on restored wire handle %d failed: %v", h2, err)
	}
	got2, err := cliB.Read(ctx, h2, 0, 64)
	if err != nil || string(got2) != "hello-pre-ckpt+post-restore" {
		t.Fatalf("Read after write = %q (err=%v)", got2, err)
	}

	// Opening another file MUST allocate a fresh handle ID strictly > h2 (never reusing h2).
	hNew, err := cliB.Open(ctx, a1.Inode)
	if err != nil {
		t.Fatalf("Open after restore: %v", err)
	}
	if hNew <= h2 {
		t.Fatalf("new handle ID %d collided with or fell below restored handle %d", hNew, h2)
	}
	gotFirst, err := cliB.Read(ctx, hNew, 0, 32)
	if err != nil || string(gotFirst) != "file-one" {
		t.Fatalf("Read on new handle %d = %q (err=%v), want file-one", hNew, gotFirst, err)
	}
}

// TestRestoreEnforcesSignedMountSpecPolicy verifies that restoring a ReadOnly
// checkpoint with a writable TargetMountSpec fails closed before exposing the
// volume, while matching MountSpec succeeds and enforces ReadOnly.
func TestRestoreEnforcesSignedMountSpecPolicy(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visorfuse-codex")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/acme/v/vol-ro")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-ro")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)
	vfsCipher := newHybridCipher(t)
	hmacKey := testHMACKey()

	if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher}); err != nil {
		t.Fatal(err)
	}
	volA, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	spA, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spA.Close()
	specRO := validMountSpec(spA.HostFD, visorfuse.SocketKindStream, "acme", "vol-ro")
	specRO.ReadOnly = true
	srvA, err := visorfuse.NewExternalFUSEServer(specRO, volA, spA.HostConn, visorfuse.WithCountingBackend(vfsBE))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := visorfuse.SuspendSandbox(ctx, srvA, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-ro-1",
		SandboxID:     "sbx-ro",
		SourceNodeID:  "node-a",
		TenantID:      "acme",
		VolumeID:      "vol-ro",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        deterministicMemory(0x63, 64<<10),
	}, visorfuse.CheckpointConfig{
		Store:   ckptBE,
		HMACKey: hmacKey,
	}); err != nil {
		t.Fatal(err)
	}
	_ = srvA.Close()

	// Attempt restore with ReadOnly=false -> MUST fail closed with ErrInvalidMountConfig.
	spB, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spB.Close()
	specRW := validMountSpec(spB.HostFD, visorfuse.SocketKindStream, "acme", "vol-ro")
	specRW.ReadOnly = false
	if _, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-b",
		TargetTenantID:      "acme",
		TargetVolumeID:      "vol-ro",
		CheckpointID:        "ckpt-ro-1",
		CheckpointStore:     ckptBE,
		HMACKey:             hmacKey,
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     specRW,
		TargetHostConn:      spB.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
	}); !errors.Is(err, visorfuse.ErrInvalidMountConfig) {
		t.Fatalf("RestoreToNode relaxing ReadOnly = %v, want ErrInvalidMountConfig", err)
	}

	// Restoring with matching ReadOnly=true succeeds.
	specTargetRO := validMountSpec(spB.HostFD, visorfuse.SocketKindStream, "acme", "vol-ro")
	specTargetRO.ReadOnly = true
	restored, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-b",
		TargetTenantID:      "acme",
		TargetVolumeID:      "vol-ro",
		CheckpointID:        "ckpt-ro-1",
		CheckpointStore:     ckptBE,
		HMACKey:             hmacKey,
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     specTargetRO,
		TargetHostConn:      spB.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
	})
	if err != nil {
		t.Fatalf("RestoreToNode with matching ReadOnly=true failed: %v", err)
	}
	_ = restored.Server.Close()
	_ = restored.Volume.Close()
}

// TestEncryptedCheckpointRejectsPlaintextPageReuse verifies that an encrypted
// checkpoint never silently reuses a plaintext page from an earlier unencrypted
// checkpoint, and fails closed if a plaintext envelope is planted at an
// encrypted page key.
func TestEncryptedCheckpointRejectsPlaintextPageReuse(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visorfuse-codex")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/acme/v/vol-enc")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-enc")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)
	vfsCipher := newHybridCipher(t)
	ckptCipher := newHybridCipher(t)
	hmacKey := testHMACKey()
	sharedMem := deterministicMemory(0x77, 128<<10)

	if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher}); err != nil {
		t.Fatal(err)
	}

	// Checkpoint #1: plaintext (Cipher: nil).
	vol1, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	sp1, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer sp1.Close()
	srv1, err := visorfuse.NewExternalFUSEServer(validMountSpec(sp1.HostFD, visorfuse.SocketKindStream, "acme", "vol-enc"), vol1, sp1.HostConn)
	if err != nil {
		t.Fatal(err)
	}
	mPlain, err := visorfuse.SuspendSandbox(ctx, srv1, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-plain-1",
		SandboxID:     "sbx-enc",
		SourceNodeID:  "node-a",
		TenantID:      "acme",
		VolumeID:      "vol-enc",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        sharedMem,
	}, visorfuse.CheckpointConfig{
		Store:   ckptBE,
		HMACKey: hmacKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = srv1.Close()

	// Checkpoint #2: encrypted with PQ Cipher on identical memory pages!
	vol2, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 2, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	sp2, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer sp2.Close()
	srv2, err := visorfuse.NewExternalFUSEServer(validMountSpec(sp2.HostFD, visorfuse.SocketKindStream, "acme", "vol-enc"), vol2, sp2.HostConn)
	if err != nil {
		t.Fatal(err)
	}
	mEnc, err := visorfuse.SuspendSandbox(ctx, srv2, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-enc-2",
		SandboxID:     "sbx-enc",
		SourceNodeID:  "node-b",
		TenantID:      "acme",
		VolumeID:      "vol-enc",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        sharedMem,
	}, visorfuse.CheckpointConfig{
		Store:   ckptBE,
		Cipher:  ckptCipher,
		HMACKey: hmacKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = srv2.Close()

	var firstEncKey string
	var firstPlainKey string
	for i, p := range mEnc.Pages {
		if p.Zero {
			continue
		}
		if !p.Encrypted {
			t.Fatalf("encrypted checkpoint page %d has Encrypted=false (reused plaintext!)", p.Index)
		}
		if !strings.Contains(p.ObjectKey, "/p/e/") {
			t.Fatalf("encrypted page key %q not in encrypted policy namespace /p/e/", p.ObjectKey)
		}
		if firstEncKey == "" {
			firstEncKey = p.ObjectKey
			firstPlainKey = mPlain.Pages[i].ObjectKey
		}
	}
	if firstEncKey == "" || firstPlainKey == "" || firstEncKey == firstPlainKey {
		t.Fatalf("expected distinct encrypted (%q) and plaintext (%q) keys", firstEncKey, firstPlainKey)
	}

	// Now plant a plaintext envelope directly at an encrypted page key and verify
	// a subsequent encrypted checkpoint FAILS CLOSED with ErrCheckpointTampered.
	plainObj, _, err := ckptBE.Get(ctx, firstPlainKey, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := ckptBE.Put(ctx, firstEncKey, plainObj); err != nil {
		t.Fatal(err)
	}

	vol3, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 3, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	sp3, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer sp3.Close()
	srv3, err := visorfuse.NewExternalFUSEServer(validMountSpec(sp3.HostFD, visorfuse.SocketKindStream, "acme", "vol-enc"), vol3, sp3.HostConn)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	_, err = visorfuse.SuspendSandbox(ctx, srv3, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-enc-3",
		SandboxID:     "sbx-enc",
		SourceNodeID:  "node-c",
		TenantID:      "acme",
		VolumeID:      "vol-enc",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        sharedMem,
	}, visorfuse.CheckpointConfig{
		Store:   ckptBE,
		Cipher:  ckptCipher,
		HMACKey: hmacKey,
	})
	if !errors.Is(err, visorfuse.ErrCheckpointTampered) {
		t.Fatalf("encrypted checkpoint encountering planted plaintext object = %v, want ErrCheckpointTampered", err)
	}
}

// TestReturnedCheckpointManifestRemainsCryptographicallyValid verifies that
// post-upload telemetry updates never invalidate ManifestDigest or Signature,
// so the returned CheckpointManifest passes Verify() and round-trips cleanly.
func TestReturnedCheckpointManifestRemainsCryptographicallyValid(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visorfuse-codex")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/acme/v/vol-sig")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-sig")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)
	vfsCipher := newHybridCipher(t)
	hmacKey := testHMACKey()

	if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher}); err != nil {
		t.Fatal(err)
	}
	volA, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	spA, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spA.Close()
	srvA, err := visorfuse.NewExternalFUSEServer(validMountSpec(spA.HostFD, visorfuse.SocketKindStream, "acme", "vol-sig"), volA, spA.HostConn)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := visorfuse.SuspendSandbox(ctx, srvA, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-sig-1",
		SandboxID:     "sbx-sig",
		SourceNodeID:  "node-a",
		TenantID:      "acme",
		VolumeID:      "vol-sig",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        deterministicMemory(0x88, 128<<10),
	}, visorfuse.CheckpointConfig{
		Store:    ckptBE,
		HMACKey:  hmacKey,
		Compress: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = srvA.Close()

	// Post-upload telemetry must be populated in-memory...
	if manifest.Stats.S3PutOps == 0 || manifest.Stats.StoredManifestSize == 0 || manifest.Stats.CheckpointDuration <= 0 {
		t.Fatalf("expected non-zero post-upload runtime stats, got %+v", manifest.Stats)
	}
	// ...AND the returned CheckpointManifest MUST still pass cryptographic Verify()!
	if err := manifest.Verify(hmacKey); err != nil {
		t.Fatalf("returned manifest.Verify() failed after post-upload stats update: %v", err)
	}
	// Re-marshaling and unmarshaling the returned manifest MUST also verify identically.
	marshaled, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var roundTripped visorfuse.CheckpointManifest
	if err := json.Unmarshal(marshaled, &roundTripped); err != nil {
		t.Fatal(err)
	}
	if err := roundTripped.Verify(hmacKey); err != nil {
		t.Fatalf("round-tripped manifest.Verify() failed: %v", err)
	}
	// Verify the bytes stored in S3 match the returned manifest's JSON byte-for-byte.
	rawStored, _, err := ckptBE.Get(ctx, visorfuse.ManifestObjectKey("acme", "vol-sig", "ckpt-sig-1"), 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(rawStored, marshaled) {
		t.Fatalf("stored S3 manifest payload differs from re-marshaled returned manifest")
	}
}

// TestGuestClientBlockedWriteHonorsContextAndPreservesFraming reproduces the
// net.Pipe stalled-peer scenario and verifies both active and queued writers
// unblock promptly on context cancellation while closing the session fail-closed.
func TestGuestClientBlockedWriteHonorsContextAndPreservesFraming(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	client := visorfuse.NewGuestFUSEClient(clientConn, visorfuse.SocketKindStream)
	defer client.Close()

	// Caller 1 starts an active write that blocks because serverConn never reads.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel1()
	errCh1 := make(chan error, 1)
	go func() {
		_, err := client.Getattr(ctx1, uint64(vfs.RootInode))
		errCh1 <- err
	}()

	// Give Caller 1 a moment to enter the active write, then launch Caller 2
	// with a shorter deadline so it blocks waiting for the write turn.
	time.Sleep(15 * time.Millisecond)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	t2 := time.Now()
	_, err2 := client.Getattr(ctx2, uint64(vfs.RootInode))
	elapsed2 := time.Since(t2)
	if !errors.Is(err2, context.DeadlineExceeded) {
		t.Fatalf("queued caller err = %v, want context.DeadlineExceeded", err2)
	}
	if elapsed2 > 200*time.Millisecond {
		t.Fatalf("queued caller took %v to cancel, expected prompt return", elapsed2)
	}

	// Caller 1 MUST also unblock promptly when ctx1 expires (not hang forever).
	select {
	case err1 := <-errCh1:
		if !errors.Is(err1, context.DeadlineExceeded) {
			t.Fatalf("blocked writer err = %v, want context.DeadlineExceeded", err1)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked writer hung past ctx1 deadline")
	}

	// Subsequent calls on the poisoned/closed session MUST fail closed immediately.
	if _, err := client.Getattr(context.Background(), uint64(vfs.RootInode)); !errors.Is(err, visorfuse.ErrSessionClosed) {
		t.Fatalf("post-cancel call = %v, want ErrSessionClosed", err)
	}
}

type closeCountingRWC struct {
	io.ReadWriteCloser
	closes atomic.Int32
}

func (c *closeCountingRWC) Close() error {
	c.closes.Add(1)
	return c.ReadWriteCloser.Close()
}

// TestGuestClientClosesTransportAfterReadLoopExit verifies that when the peer
// closes first or sends a malformed frame causing readLoop to exit, the
// underlying transport is closed exactly once (no leaked socket/pipe FDs).
func TestGuestClientClosesTransportAfterReadLoopExit(t *testing.T) {
	t.Run("peer_eof_before_close", func(t *testing.T) {
		cConn, sConn := net.Pipe()
		tracked := &closeCountingRWC{ReadWriteCloser: cConn}
		client := visorfuse.NewGuestFUSEClient(tracked, visorfuse.SocketKindStream)

		// Close server side first so readLoop exits on EOF.
		_ = sConn.Close()
		deadline := time.Now().Add(time.Second)
		for tracked.closes.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := tracked.closes.Load(); got != 1 {
			t.Fatalf("underlying transport closes after peer EOF = %d, want 1", got)
		}
		// Calling Close() multiple times afterwards must remain idempotent (still 1).
		_ = client.Close()
		_ = client.Close()
		if got := tracked.closes.Load(); got != 1 {
			t.Fatalf("underlying transport closes after client.Close() = %d, want 1", got)
		}
	})

	t.Run("malformed_frame_before_close", func(t *testing.T) {
		cConn, sConn := net.Pipe()
		defer sConn.Close()
		tracked := &closeCountingRWC{ReadWriteCloser: cConn}
		client := visorfuse.NewGuestFUSEClient(tracked, visorfuse.SocketKindStream)

		// Send 56 bytes with bogus magic to force ReadFrame protocol error.
		bad := make([]byte, visorfuse.WireHeaderSize)
		_, _ = sConn.Write(bad)

		deadline := time.Now().Add(time.Second)
		for tracked.closes.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := tracked.closes.Load(); got != 1 {
			t.Fatalf("underlying transport closes after malformed frame = %d, want 1", got)
		}
		_ = client.Close()
		if got := tracked.closes.Load(); got != 1 {
			t.Fatalf("underlying transport closes after client.Close() = %d, want 1", got)
		}
	})
}
