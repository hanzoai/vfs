// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package visorfuse_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/visorfuse"
	"github.com/luxfi/age"
)

func newHybridCipher(t *testing.T) vfs.Cipher {
	t.Helper()
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("GenerateHybridIdentity: %v", err)
	}
	return vfs.Hybrid(id)
}

func newLiveS3Backend(t *testing.T, srv *storetest.S3, prefix string) *visorfuse.CountingBackend {
	t.Helper()
	be, err := s3.New(s3.Config{
		Endpoint:  srv.Endpoint(),
		Insecure:  true,
		Bucket:    srv.Bucket,
		Prefix:    prefix,
		AccessKey: "test-access",
		SecretKey: "test-secret",
	})
	if err != nil {
		t.Fatalf("s3.New(%s): %v", prefix, err)
	}
	cb := visorfuse.NewCountingBackend(be)
	t.Cleanup(func() { _ = be.Close() })
	return cb
}

func validMountSpec(hostFD int, kind visorfuse.SocketKind, tenant, vol string) visorfuse.MountSpec {
	return visorfuse.MountSpec{
		Type:                   visorfuse.MountTypeFUSE,
		Source:                 fmt.Sprintf("vfs://%s/%s", tenant, vol),
		Destination:            "/workspace",
		HostSocketFD:           hostFD,
		AllowedHostFDs:         []int{hostFD},
		SocketKind:             kind,
		TenantID:               tenant,
		VolumeID:               vol,
		NoGuestPriv:            true,
		NoGoferHop:             true,
		AllowDeviceFuseInGuest: false,
		Options:                []string{"no_guest_priv=true", "no_gofer_hop=true", "rootmode=040755"},
	}
}

func testHMACKey() []byte {
	return []byte("visorfuse-test-hmac-key-material-32b")
}

func deterministicMemory(seed byte, totalBytes int) []byte {
	mem := make([]byte, totalBytes)
	pageSize := visorfuse.DefaultCheckpointPageSize
	numPages := (totalBytes + pageSize - 1) / pageSize
	for p := 0; p < numPages; p++ {
		start := p * pageSize
		end := min(start+pageSize, totalBytes)
		switch p % 5 {
		case 0:
			// Zero page (elided).
		case 1:
			// Repeated pattern (deduplicates across pages + compresses well).
			for i := start; i < end; i++ {
				mem[i] = seed ^ byte((i-start)%64)
			}
		case 2:
			// Text-like compressible heap page.
			pat := fmt.Sprintf("sandbox-heap-page-%04d-seed-%02x-", p, seed)
			for i := start; i < end; i++ {
				mem[i] = pat[(i-start)%len(pat)]
			}
		case 3:
			// Duplicate of case 1.
			for i := start; i < end; i++ {
				mem[i] = seed ^ byte((i-start)%64)
			}
		case 4:
			// High-entropy non-zero page.
			x := uint64(p+1)*0x9e3779b97f4a7c15 ^ uint64(seed)
			for i := start; i < end; i++ {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				mem[i] = byte(x)
			}
		}
	}
	return mem
}

func TestValidateMountSpecSecurity(t *testing.T) {
	base := validMountSpec(7, visorfuse.PreferredSocketKind(), "tenant-alpha", "vol-01")
	if err := visorfuse.ValidateMountSpec(base); err != nil {
		t.Fatalf("expected valid mount spec, got %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(*visorfuse.MountSpec)
		wantErr error
	}{
		{
			name:    "wrong_mount_type",
			mutate:  func(s *visorfuse.MountSpec) { s.Type = "9p" },
			wantErr: visorfuse.ErrInvalidMountConfig,
		},
		{
			name:    "guest_priv_enabled",
			mutate:  func(s *visorfuse.MountSpec) { s.NoGuestPriv = false },
			wantErr: visorfuse.ErrGuestPrivilegeForbidden,
		},
		{
			name:    "guest_dev_fuse_enabled",
			mutate:  func(s *visorfuse.MountSpec) { s.AllowDeviceFuseInGuest = true },
			wantErr: visorfuse.ErrGuestPrivilegeForbidden,
		},
		{
			name:    "gofer_hop_enabled",
			mutate:  func(s *visorfuse.MountSpec) { s.NoGoferHop = false },
			wantErr: visorfuse.ErrGoferHopForbidden,
		},
		{
			name:    "cap_sys_admin_forbidden",
			mutate:  func(s *visorfuse.MountSpec) { s.GuestCapabilities = []string{"CAP_SYS_ADMIN"} },
			wantErr: visorfuse.ErrGuestPrivilegeForbidden,
		},
		{
			name:    "reserved_fd_stdin",
			mutate:  func(s *visorfuse.MountSpec) { s.HostSocketFD = 2; s.AllowedHostFDs = []int{2} },
			wantErr: visorfuse.ErrUntrustedSocketFD,
		},
		{
			name:    "fd_not_allowlisted",
			mutate:  func(s *visorfuse.MountSpec) { s.HostSocketFD = 9; s.AllowedHostFDs = []int{7, 8} },
			wantErr: visorfuse.ErrUntrustedSocketFD,
		},
		{
			name:    "traversal_destination",
			mutate:  func(s *visorfuse.MountSpec) { s.Destination = "/workspace/../etc" },
			wantErr: visorfuse.ErrInvalidMountConfig,
		},
		{
			name:    "root_destination",
			mutate:  func(s *visorfuse.MountSpec) { s.Destination = "/" },
			wantErr: visorfuse.ErrInvalidMountConfig,
		},
		{
			name:    "invalid_tenant_chars",
			mutate:  func(s *visorfuse.MountSpec) { s.TenantID = "Tenant_Bad" },
			wantErr: visorfuse.ErrInvalidMountConfig,
		},
		{
			name:    "forbidden_option_suid",
			mutate:  func(s *visorfuse.MountSpec) { s.Options = append(s.Options, "suid") },
			wantErr: visorfuse.ErrGuestPrivilegeForbidden,
		},
		{
			name:    "forbidden_option_gofer",
			mutate:  func(s *visorfuse.MountSpec) { s.Options = append(s.Options, "gofer_hop") },
			wantErr: visorfuse.ErrGoferHopForbidden,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := base
			spec.Options = append([]string(nil), base.Options...)
			spec.AllowedHostFDs = append([]int(nil), base.AllowedHostFDs...)
			tc.mutate(&spec)
			err := visorfuse.ValidateMountSpec(spec)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ValidateMountSpec() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestExternalFUSEFullOpsAcrossTransports(t *testing.T) {
	transports := []struct {
		name string
		make func(t *testing.T) *visorfuse.SocketPair
	}{
		{
			name: "unix_seqpacket_or_preferred",
			make: func(t *testing.T) *visorfuse.SocketPair {
				sp, err := visorfuse.NewUnixSocketPair(visorfuse.PreferredSocketKind())
				if err != nil {
					t.Fatalf("NewUnixSocketPair(preferred): %v", err)
				}
				return sp
			},
		},
		{
			name: "unix_stream",
			make: func(t *testing.T) *visorfuse.SocketPair {
				sp, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
				if err != nil {
					t.Fatalf("NewUnixSocketPair(stream): %v", err)
				}
				return sp
			},
		},
		{
			name: "os_pipe",
			make: func(t *testing.T) *visorfuse.SocketPair {
				sp, err := visorfuse.NewPipePair()
				if err != nil {
					t.Fatalf("NewPipePair: %v", err)
				}
				return sp
			},
		},
	}

	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			ctx := context.Background()
			s3Srv := storetest.NewS3(t, "visor-fuse-test")
			be := newLiveS3Backend(t, s3Srv, "t/acme/v/vol1")
			be.SetSuppressClose(true)
			cipher := newHybridCipher(t)

			if err := vfs.Create(ctx, vfs.Config{Backend: be, Cipher: cipher}); err != nil {
				t.Fatalf("vfs.Create: %v", err)
			}
			vol, err := vfs.Attach(ctx, vfs.Config{
				Backend: be,
				Cipher:  cipher,
				Round:   1,
				Journal: t.TempDir(),
				Bound:   5 * time.Second,
			})
			if err != nil {
				t.Fatalf("vfs.Attach: %v", err)
			}

			sp := tr.make(t)
			t.Cleanup(func() { _ = sp.Close() })

			spec := validMountSpec(sp.HostFD, sp.Kind, "acme", "vol1")
			srv, err := visorfuse.NewExternalFUSEServer(spec, vol, sp.HostConn, visorfuse.WithCountingBackend(be))
			if err != nil {
				t.Fatalf("NewExternalFUSEServer: %v", err)
			}

			srvCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			srvErrCh := make(chan error, 1)
			go func() { srvErrCh <- srv.Serve(srvCtx) }()

			cli := visorfuse.NewGuestFUSEClient(sp.GuestConn, sp.Kind)
			defer cli.Close()

			// Wrong tenant in FUSE_INIT must fail closed.
			if _, err := cli.Init(ctx, visorfuse.InitRequest{
				Flags:    visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
				TenantID: "other-tenant",
				VolumeID: "vol1",
			}); !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("wrong tenant FUSE_INIT = %v, want ErrPermission", err)
			}

			// Proper FUSE_INIT handshake.
			initResp, err := cli.Init(ctx, visorfuse.InitRequest{
				Flags:    visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
				TenantID: "acme",
				VolumeID: "vol1",
			})
			if err != nil {
				t.Fatalf("FUSE_INIT: %v", err)
			}
			if initResp.Round != 1 || initResp.Flags&(visorfuse.InitFlagNoGuestPriv|visorfuse.InitFlagNoGoferHop) != (visorfuse.InitFlagNoGuestPriv|visorfuse.InitFlagNoGoferHop) {
				t.Fatalf("unexpected FUSE_INIT response: %+v", initResp)
			}

			// FUSE_MKDIR + FUSE_CREATE + FUSE_LOOKUP + FUSE_GETATTR.
			dirAttr, err := cli.Mkdir(ctx, uint64(vfs.RootInode), "src", 0o755)
			if err != nil {
				t.Fatalf("FUSE_MKDIR: %v", err)
			}
			fileAttr, fh, err := cli.Create(ctx, dirAttr.Inode, "main.go", 0o644)
			if err != nil {
				t.Fatalf("FUSE_CREATE: %v", err)
			}
			looked, err := cli.Lookup(ctx, dirAttr.Inode, "main.go")
			if err != nil || looked.Inode != fileAttr.Inode {
				t.Fatalf("FUSE_LOOKUP = (%+v, %v), want inode %d", looked, err, fileAttr.Inode)
			}

			// FUSE_WRITE (256 KiB payload across chunks) + FUSE_FSYNC.
			payload := bytes.Repeat([]byte("hanzo-visor-ext-fuse-payload-0123456789abcdef"), 6000)
			beforeFsync := be.Snapshot().TotalRequests()
			if _, err := cli.WriteAll(ctx, fh, 0, payload, 128<<10); err != nil {
				t.Fatalf("FUSE_WRITE: %v", err)
			}
			if err := cli.Fsync(ctx, fileAttr.Inode, fh); err != nil {
				t.Fatalf("FUSE_FSYNC: %v", err)
			}
			afterFsync := be.Snapshot().TotalRequests()
			if afterFsync != beforeFsync {
				t.Fatalf("FUSE_FSYNC triggered %d synchronous S3 requests, want 0", afterFsync-beforeFsync)
			}

			// FUSE_FLUSH + FUSE_RELEASE: verify ZERO synchronous S3 requests on clean close.
			beforeClose := be.Snapshot().TotalRequests()
			if err := cli.Flush(ctx, fh); err != nil {
				t.Fatalf("FUSE_FLUSH: %v", err)
			}
			if err := cli.Release(ctx, fh); err != nil {
				t.Fatalf("FUSE_RELEASE: %v", err)
			}
			afterClose := be.Snapshot().TotalRequests()
			if afterClose != beforeClose {
				t.Fatalf("clean close (FLUSH+RELEASE) triggered %d synchronous S3 requests, want 0", afterClose-beforeClose)
			}

			// Re-open with FUSE_OPEN and verify FUSE_READ + FUSE_GETATTR + FUSE_STATFS.
			fh2, err := cli.Open(ctx, fileAttr.Inode)
			if err != nil {
				t.Fatalf("FUSE_OPEN: %v", err)
			}
			got, err := cli.ReadAll(ctx, fh2, 0, len(payload), 128<<10)
			if err != nil {
				t.Fatalf("FUSE_READ: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("FUSE_READ mismatch: got %d bytes, want %d", len(got), len(payload))
			}
			ga, err := cli.Getattr(ctx, fileAttr.Inode)
			if err != nil || ga.Size != uint64(len(payload)) {
				t.Fatalf("FUSE_GETATTR = (%+v, %v), want size %d", ga, err, len(payload))
			}
			stfs, err := cli.Statfs(ctx)
			if err != nil || stfs.Bsize == 0 || stfs.Files == 0 {
				t.Fatalf("FUSE_STATFS = (%+v, %v)", stfs, err)
			}
			if err := cli.Release(ctx, fh2); err != nil {
				t.Fatalf("FUSE_RELEASE #2: %v", err)
			}

			// FUSE_UNLINK a scratch file.
			_, tmpH, err := cli.Create(ctx, dirAttr.Inode, "tmp.txt", 0o644)
			if err != nil {
				t.Fatalf("FUSE_CREATE tmp: %v", err)
			}
			_ = cli.Release(ctx, tmpH)
			if err := cli.Unlink(ctx, dirAttr.Inode, "tmp.txt"); err != nil {
				t.Fatalf("FUSE_UNLINK: %v", err)
			}
			if _, err := cli.Lookup(ctx, dirAttr.Inode, "tmp.txt"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("lookup after unlink = %v, want ErrNotExist", err)
			}

			// Quiesce + ReleaseRound: verify 0 S3 requests during clean volume close.
			rec, err := srv.FlushAndQuiesce(ctx)
			if err != nil {
				t.Fatalf("FlushAndQuiesce: %v", err)
			}
			if rec.PublishDuration >= 5*time.Second {
				t.Fatalf("publish latency %v >= 5s SLA", rec.PublishDuration)
			}
			tok, err := srv.ReleaseRound(ctx)
			if err != nil {
				t.Fatalf("ReleaseRound: %v", err)
			}
			if tok.CleanCloseS3Requests != 0 {
				t.Fatalf("CleanCloseS3Requests = %d, want 0", tok.CleanCloseS3Requests)
			}

			cancel()
			_ = cli.Close()
			_ = srv.Close()
			if err := <-srvErrCh; err != nil {
				t.Fatalf("Serve error: %v", err)
			}
		})
	}
}

func TestGoferHopProxyBaseline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s3Srv := storetest.NewS3(t, "visor-gofer-test")
	be := newLiveS3Backend(t, s3Srv, "t/acme/v/gofer1")
	be.SetSuppressClose(true)
	cipher := newHybridCipher(t)
	if err := vfs.Create(ctx, vfs.Config{Backend: be, Cipher: cipher}); err != nil {
		t.Fatal(err)
	}
	vol, err := vfs.Attach(ctx, vfs.Config{Backend: be, Cipher: cipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	// Guest <-> Gofer <-> Server (two Unix socket hops).
	spGuestToGofer, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spGuestToGofer.Close()
	spGoferToServer, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spGoferToServer.Close()

	spec := validMountSpec(spGoferToServer.HostFD, visorfuse.SocketKindStream, "acme", "gofer1")
	srv, err := visorfuse.NewExternalFUSEServer(spec, vol, spGoferToServer.HostConn)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ctx) }()

	gofer := visorfuse.NewGoferHopProxy(spGuestToGofer.HostConn, spGoferToServer.GuestConn, visorfuse.SocketKindStream, false)
	go func() { _ = gofer.Serve(ctx) }()

	cli := visorfuse.NewGuestFUSEClient(spGuestToGofer.GuestConn, visorfuse.SocketKindStream)
	defer cli.Close()

	if _, err := cli.Init(ctx, visorfuse.InitRequest{
		Flags:    visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
		TenantID: "acme",
		VolumeID: "gofer1",
	}); err != nil {
		t.Fatalf("gofer FUSE_INIT: %v", err)
	}
	attr, fh, err := cli.Create(ctx, uint64(vfs.RootInode), "hello.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	msg := bytes.Repeat([]byte("gofer-hop-data"), 1024)
	if _, err := cli.Write(ctx, fh, 0, msg); err != nil {
		t.Fatal(err)
	}
	if err := cli.Fsync(ctx, attr.Inode, fh); err != nil {
		t.Fatal(err)
	}
	got, err := cli.Read(ctx, fh, 0, uint32(len(msg)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("gofer read mismatch")
	}
	if gofer.Hops() == 0 || gofer.BytesRelayed() < int64(len(msg)*2) {
		t.Fatalf("expected gofer relay metrics > 0, got hops=%d bytes=%d", gofer.Hops(), gofer.BytesRelayed())
	}
}

func TestUnquiescedCheckpointFailsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s3Srv := storetest.NewS3(t, "visor-neg-unquiesced")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/acme/v/volq")
	vfsBE.SetSuppressClose(true)
	ckptBE := newLiveS3Backend(t, s3Srv, "checkpoints")
	cipher := newHybridCipher(t)

	if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: cipher}); err != nil {
		t.Fatal(err)
	}
	vol, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: cipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	sp, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	spec := validMountSpec(sp.HostFD, visorfuse.SocketKindStream, "acme", "volq")
	srv, err := visorfuse.NewExternalFUSEServer(spec, vol, sp.HostConn, visorfuse.WithCountingBackend(vfsBE))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ctx) }()

	cli := visorfuse.NewGuestFUSEClient(sp.GuestConn, visorfuse.SocketKindStream)
	defer cli.Close()

	_, fh, err := cli.Create(ctx, uint64(vfs.RootInode), "dirty.dat", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Write(ctx, fh, 0, []byte("unflushed-dirty-page")); err != nil {
		t.Fatal(err)
	}

	snap := visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-unquiesced",
		SandboxID:     "sbx-1",
		SourceNodeID:  "node-a",
		TenantID:      "acme",
		VolumeID:      "volq",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        deterministicMemory(1, 256<<10),
	}
	cfg := visorfuse.CheckpointConfig{Store: ckptBE, HMACKey: testHMACKey(), Compress: true}

	// 1. Attempting ReleaseRound before FlushAndQuiesce fails closed.
	if _, err := srv.ReleaseRound(ctx); !errors.Is(err, visorfuse.ErrUnquiescedVolume) {
		t.Fatalf("ReleaseRound before FlushAndQuiesce = %v, want ErrUnquiescedVolume", err)
	}

	// 2. Attempting CheckpointSandbox before FlushAndQuiesce fails closed.
	if _, err := visorfuse.CheckpointSandbox(ctx, srv, snap, cfg); !errors.Is(err, visorfuse.ErrUnquiescedVolume) {
		t.Fatalf("CheckpointSandbox on dirty unquiesced volume = %v, want ErrUnquiescedVolume", err)
	}

	// 3. After FlushAndQuiesce, writes are rejected and CheckpointSandbox STILL
	//    fails closed until ReleaseRound completes.
	if _, err := srv.FlushAndQuiesce(ctx); err != nil {
		t.Fatalf("FlushAndQuiesce: %v", err)
	}
	if _, err := cli.Write(ctx, fh, 0, []byte("blocked")); err == nil {
		t.Fatalf("expected FUSE_WRITE to fail while quiesced")
	}
	if _, err := visorfuse.CheckpointSandbox(ctx, srv, snap, cfg); !errors.Is(err, visorfuse.ErrUnquiescedVolume) {
		t.Fatalf("CheckpointSandbox before ReleaseRound = %v, want ErrUnquiescedVolume", err)
	}
}

func TestS3CheckpointAndCrossNodeRestoreLiveS3AndFileBackend(t *testing.T) {
	modes := []struct {
		name       string
		useFileBE  bool
		srcPriv    visorfuse.PrivilegeMode
		targetPriv visorfuse.PrivilegeMode
		encryptPQ  bool
	}{
		{
			name:       "live_s3_pq_unprivileged_to_unprivileged",
			useFileBE:  false,
			srcPriv:    visorfuse.PrivilegeUnprivileged,
			targetPriv: visorfuse.PrivilegeUnprivileged,
			encryptPQ:  true,
		},
		{
			name:       "live_s3_pq_rootful_to_rootful",
			useFileBE:  false,
			srcPriv:    visorfuse.PrivilegeRootful,
			targetPriv: visorfuse.PrivilegeRootful,
			encryptPQ:  true,
		},
		{
			name:       "file_backend_rootful_to_unprivileged",
			useFileBE:  true,
			srcPriv:    visorfuse.PrivilegeRootful,
			targetPriv: visorfuse.PrivilegeUnprivileged,
			encryptPQ:  true,
		},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var vfsBE, ckptBE *visorfuse.CountingBackend
			if tc.useFileBE {
				rawVFS, err := backend.Open(ctx, "file://"+filepath.Join(t.TempDir(), "vfs"))
				if err != nil {
					t.Fatal(err)
				}
				rawCkpt, err := backend.Open(ctx, "file://"+filepath.Join(t.TempDir(), "ckpt"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = rawVFS.Close(); _ = rawCkpt.Close() })
				vfsBE = visorfuse.NewCountingBackend(rawVFS)
				ckptBE = visorfuse.NewCountingBackend(rawCkpt)
			} else {
				s3Srv := storetest.NewS3(t, "visor-ckpt-s3")
				vfsBE = newLiveS3Backend(t, s3Srv, "t/tenant-a/v/vol-x")
				ckptBE = newLiveS3Backend(t, s3Srv, "ckpt-store")
			}
			vfsBE.SetSuppressClose(true)
			ckptBE.SetSuppressClose(true)

			vfsCipher := newHybridCipher(t)
			var ckptCipher vfs.Cipher
			if tc.encryptPQ {
				ckptCipher = newHybridCipher(t)
			}

			// --- Node A: create VFS, mount over host socket FD, write files ---
			if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher}); err != nil {
				t.Fatal(err)
			}
			volA, err := vfs.Attach(ctx, vfs.Config{
				Backend: vfsBE,
				Cipher:  vfsCipher,
				Round:   1,
				Journal: t.TempDir(),
			})
			if err != nil {
				t.Fatal(err)
			}
			spA, err := visorfuse.NewUnixSocketPair(visorfuse.PreferredSocketKind())
			if err != nil {
				t.Fatal(err)
			}
			defer spA.Close()

			specA := validMountSpec(spA.HostFD, spA.Kind, "tenant-a", "vol-x")
			srvA, err := visorfuse.NewExternalFUSEServer(specA, volA, spA.HostConn, visorfuse.WithCountingBackend(vfsBE))
			if err != nil {
				t.Fatal(err)
			}
			ctxA, cancelA := context.WithCancel(ctx)
			defer cancelA()
			go func() { _ = srvA.Serve(ctxA) }()

			cliA := visorfuse.NewGuestFUSEClient(spA.GuestConn, spA.Kind)
			if _, err := cliA.Init(ctx, visorfuse.InitRequest{
				Flags:    visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
				TenantID: "tenant-a",
				VolumeID: "vol-x",
			}); err != nil {
				t.Fatal(err)
			}

			dataAttr, fh, err := cliA.Create(ctx, uint64(vfs.RootInode), "state.db", 0o644)
			if err != nil {
				t.Fatal(err)
			}
			fileData := bytes.Repeat([]byte("cross-node-persistent-vfs-data-9876543210"), 2000)
			if _, err := cliA.WriteAll(ctx, fh, 0, fileData, 64<<10); err != nil {
				t.Fatal(err)
			}
			if err := cliA.Fsync(ctx, dataAttr.Inode, fh); err != nil {
				t.Fatal(err)
			}
			if err := cliA.Flush(ctx, fh); err != nil {
				t.Fatal(err)
			}
			if err := cliA.Release(ctx, fh); err != nil {
				t.Fatal(err)
			}

			// Capture guest memory (1 MiB with zero pages, dup pages, compressible & random pages).
			rawMem := deterministicMemory(0x42, 1<<20)
			snap := visorfuse.SandboxSnapshot{
				CheckpointID:  "ckpt-001",
				SandboxID:     "sbx-alpha",
				SourceNodeID:  "node-a",
				TenantID:      "tenant-a",
				VolumeID:      "vol-x",
				PrivilegeMode: tc.srcPriv,
				UID:           1000,
				GID:           1000,
				Memory:        rawMem,
				Descriptors: []visorfuse.OpenDescriptor{
					{FD: 4, Path: "/state.db", Inode: dataAttr.Inode, Offset: 128, Mode: 0o644, Size: uint64(len(fileData))},
				},
				Registers: []byte("rip=0x401000,rsp=0x7ffe0000"),
				Env:       map[string]string{"SANDBOX": "visor"},
			}

			// SuspendSandbox: FlushAndQuiesce -> ReleaseRound -> S3 Checkpoint.
			manifest, err := visorfuse.SuspendSandbox(ctx, srvA, snap, visorfuse.CheckpointConfig{
				Store:    ckptBE,
				Cipher:   ckptCipher,
				HMACKey:  testHMACKey(),
				Compress: true,
			})
			if err != nil {
				t.Fatalf("SuspendSandbox: %v", err)
			}
			cancelA()
			_ = cliA.Close()
			_ = srvA.Close()

			if manifest.VFSRelease.CleanCloseS3Requests != 0 {
				t.Fatalf("CleanCloseS3Requests = %d, want 0", manifest.VFSRelease.CleanCloseS3Requests)
			}
			if manifest.VFSRelease.PublishDuration >= 5*time.Second {
				t.Fatalf("publish duration %v >= 5s", manifest.VFSRelease.PublishDuration)
			}
			if manifest.Stats.ZeroPages == 0 || manifest.Stats.DedupedPages == 0 {
				t.Fatalf("expected zero-page elision & dedup > 0, got %+v", manifest.Stats)
			}

			// Verify PQ ML-KEM-768+X25519 stanza is present on S3 manifest & page objects!
			if tc.encryptPQ {
				mKey := visorfuse.ManifestObjectKey("tenant-a", "vol-x", "ckpt-001")
				rawM, _, err := ckptBE.Get(ctx, mKey, 16<<20)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(rawM, []byte("age-encryption.org/v1\n")) || !bytes.Contains(rawM, []byte("-> mlkem768x25519 ")) {
					t.Fatalf("manifest is not sealed with vfs.Hybrid mlkem768x25519 PQ stanza")
				}
				if bytes.Contains(rawM, []byte("sbx-alpha")) {
					t.Fatalf("plaintext sandbox ID leaked into encrypted manifest")
				}
			}

			// --- Node B: cross-node restore with fresh journal dir and new socket FD ---
			spB, err := visorfuse.NewUnixSocketPair(visorfuse.PreferredSocketKind())
			if err != nil {
				t.Fatal(err)
			}
			defer spB.Close()
			specB := validMountSpec(spB.HostFD, spB.Kind, "tenant-a", "vol-x")

			restored, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
				TargetNodeID:          "node-b",
				TargetTenantID:        "tenant-a",
				TargetVolumeID:        "vol-x",
				CheckpointID:          "ckpt-001",
				CheckpointStore:       ckptBE,
				CheckpointCipher:      ckptCipher,
				HMACKey:               testHMACKey(),
				VFSBackend:            vfsBE,
				VFSCipher:             vfsCipher,
				TargetJournalDir:      t.TempDir(),
				TargetMountSpec:       specB,
				TargetHostConn:        spB.HostConn,
				TargetCountingBackend: vfsBE,
				TargetPrivilegeMode:   tc.targetPriv,
			})
			if err != nil {
				t.Fatalf("RestoreToNode: %v", err)
			}
			defer restored.Volume.Close()

			if !restored.Metrics.VerifiedBitExact {
				t.Fatalf("expected VerifiedBitExact=true")
			}
			if !bytes.Equal(restored.Memory, rawMem) {
				t.Fatalf("restored memory bytes differ from source snapshot")
			}
			if restored.Metrics.AttachedRound != 2 {
				t.Fatalf("attached round = %d, want 2", restored.Metrics.AttachedRound)
			}
			if len(restored.RestoredDescriptors) != 1 || restored.RestoredDescriptors[0].Inode != dataAttr.Inode {
				t.Fatalf("unexpected restored descriptors: %+v", restored.RestoredDescriptors)
			}

			// Serve Node B over new host socket FD and verify persistent VFS file bit-for-bit!
			ctxB, cancelB := context.WithCancel(ctx)
			defer cancelB()
			go func() { _ = restored.Server.Serve(ctxB) }()
			cliB := visorfuse.NewGuestFUSEClient(spB.GuestConn, spB.Kind)
			defer cliB.Close()

			initB, err := cliB.Init(ctx, visorfuse.InitRequest{
				Flags:    visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
				TenantID: "tenant-a",
				VolumeID: "vol-x",
			})
			if err != nil {
				t.Fatalf("Node B FUSE_INIT: %v", err)
			}
			if initB.Round != 2 {
				t.Fatalf("Node B init round = %d, want 2", initB.Round)
			}
			fhB, err := cliB.Open(ctx, dataAttr.Inode)
			if err != nil {
				t.Fatalf("Node B FUSE_OPEN: %v", err)
			}
			gotFile, err := cliB.ReadAll(ctx, fhB, 0, len(fileData), 64<<10)
			if err != nil {
				t.Fatalf("Node B FUSE_READ: %v", err)
			}
			if !bytes.Equal(gotFile, fileData) {
				t.Fatalf("Node B VFS file mismatch after cross-node restore")
			}
			// Continue mutating on Node B at round 2.
			_, fhNew, err := cliB.Create(ctx, uint64(vfs.RootInode), "post-restore.txt", 0o644)
			if err != nil {
				t.Fatalf("Node B FUSE_CREATE: %v", err)
			}
			if _, err := cliB.Write(ctx, fhNew, 0, []byte("alive-on-node-b")); err != nil {
				t.Fatalf("Node B FUSE_WRITE: %v", err)
			}
			_ = cliB.Release(ctx, fhB)
			_ = cliB.Release(ctx, fhNew)
			cancelB()
			_ = restored.Server.Close()
		})
	}
}

func TestSecurityPrivilegeEscalationCrossTenantAndTamper(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visor-sec-test")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/tenant-a/v/vol-sec")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-sec")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)

	vfsCipher := newHybridCipher(t)
	ckptCipher := newHybridCipher(t)
	wrongCipher := newHybridCipher(t)

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
	specA := validMountSpec(spA.HostFD, visorfuse.SocketKindStream, "tenant-a", "vol-sec")
	srvA, err := visorfuse.NewExternalFUSEServer(specA, volA, spA.HostConn, visorfuse.WithCountingBackend(vfsBE))
	if err != nil {
		t.Fatal(err)
	}

	rawMem := deterministicMemory(0x77, 256<<10)
	manifest, err := visorfuse.SuspendSandbox(ctx, srvA, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-sec-1",
		SandboxID:     "sbx-sec",
		SourceNodeID:  "node-a",
		TenantID:      "tenant-a",
		VolumeID:      "vol-sec",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        rawMem,
	}, visorfuse.CheckpointConfig{
		Store:    ckptBE,
		Cipher:   ckptCipher,
		HMACKey:  testHMACKey(),
		Compress: true,
	})
	if err != nil {
		t.Fatalf("SuspendSandbox: %v", err)
	}
	_ = srvA.Close()

	baseReq := func(tenant, vol string) visorfuse.RestoreRequest {
		return visorfuse.RestoreRequest{
			TargetNodeID:        "node-b",
			TargetTenantID:      tenant,
			TargetVolumeID:      vol,
			CheckpointID:        "ckpt-sec-1",
			CheckpointStore:     ckptBE,
			CheckpointCipher:    ckptCipher,
			HMACKey:             testHMACKey(),
			VFSBackend:          vfsBE,
			VFSCipher:           vfsCipher,
			TargetJournalDir:    t.TempDir(),
			TargetMountSpec:     validMountSpec(9, visorfuse.SocketKindStream, tenant, vol),
			TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
		}
	}

	// 1. Unauthorized privilege escalation (unprivileged -> rootful) MUST fail closed.
	escReq := baseReq("tenant-a", "vol-sec")
	escReq.TargetPrivilegeMode = visorfuse.PrivilegeRootful
	if _, err := visorfuse.RestoreToNode(ctx, escReq); !errors.Is(err, visorfuse.ErrRestorePrivilegeDenied) {
		t.Fatalf("privilege escalation = %v, want ErrRestorePrivilegeDenied", err)
	}

	// 2. Wrong HMAC key MUST fail closed with ErrCheckpointTampered.
	badHMAC := baseReq("tenant-a", "vol-sec")
	badHMAC.HMACKey = []byte("wrong-hmac-key-material-00000000")
	if _, err := visorfuse.RestoreToNode(ctx, badHMAC); !errors.Is(err, visorfuse.ErrCheckpointTampered) {
		t.Fatalf("wrong HMAC key = %v, want ErrCheckpointTampered", err)
	}

	// 3. Wrong PQ Cipher MUST fail closed with ErrCheckpointTampered.
	badPQ := baseReq("tenant-a", "vol-sec")
	badPQ.CheckpointCipher = wrongCipher
	if _, err := visorfuse.RestoreToNode(ctx, badPQ); !errors.Is(err, visorfuse.ErrCheckpointTampered) {
		t.Fatalf("wrong PQ cipher = %v, want ErrCheckpointTampered", err)
	}

	// 4. Cross-tenant restore MUST fail closed (even if raw S3 manifest is copied into tenant-b's prefix!).
	mKeyA := visorfuse.ManifestObjectKey("tenant-a", "vol-sec", "ckpt-sec-1")
	mKeyB := visorfuse.ManifestObjectKey("tenant-b", "vol-sec", "ckpt-sec-1")
	rawM, _, err := ckptBE.Get(ctx, mKeyA, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := ckptBE.Put(ctx, mKeyB, rawM); err != nil {
		t.Fatal(err)
	}
	crossReq := baseReq("tenant-b", "vol-sec")
	if _, err := visorfuse.RestoreToNode(ctx, crossReq); !errors.Is(err, visorfuse.ErrCheckpointTampered) && !errors.Is(err, visorfuse.ErrTenantMismatch) {
		t.Fatalf("cross-tenant copied manifest = %v, want ErrCheckpointTampered or ErrTenantMismatch", err)
	}

	// 5. Bit-flip tamper in a stored memory page MUST fail closed with ErrCheckpointTampered.
	var targetPageKey string
	for _, p := range manifest.Pages {
		if !p.Zero {
			targetPageKey = p.ObjectKey
			break
		}
	}
	origPage, _, err := ckptBE.Get(ctx, targetPageKey, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := append([]byte(nil), origPage...)
	corrupted[len(corrupted)-1] ^= 0x01
	if err := ckptBE.Put(ctx, targetPageKey, corrupted); err != nil {
		t.Fatal(err)
	}
	if _, err := visorfuse.RestoreToNode(ctx, baseReq("tenant-a", "vol-sec")); !errors.Is(err, visorfuse.ErrCheckpointTampered) {
		t.Fatalf("tampered page restore = %v, want ErrCheckpointTampered", err)
	}
	// Restore original page, then flip 1 bit in manifest.
	if err := ckptBE.Put(ctx, targetPageKey, origPage); err != nil {
		t.Fatal(err)
	}
	corruptM := append([]byte(nil), rawM...)
	corruptM[len(corruptM)-1] ^= 0x80
	if err := ckptBE.Put(ctx, mKeyA, corruptM); err != nil {
		t.Fatal(err)
	}
	if _, err := visorfuse.RestoreToNode(ctx, baseReq("tenant-a", "vol-sec")); !errors.Is(err, visorfuse.ErrCheckpointTampered) {
		t.Fatalf("tampered manifest restore = %v, want ErrCheckpointTampered", err)
	}
}

func TestLostACKRetrySafetyAndIncrementalDedup(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visor-lostack-test")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/acme/v/vol-ack")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-ack")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)

	vfsCipher := newHybridCipher(t)
	ckptCipher := newHybridCipher(t)

	if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher}); err != nil {
		t.Fatal(err)
	}
	vol1, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	sp1, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer sp1.Close()
	srv1, err := visorfuse.NewExternalFUSEServer(validMountSpec(sp1.HostFD, visorfuse.SocketKindStream, "acme", "vol-ack"), vol1, sp1.HostConn, visorfuse.WithCountingBackend(vfsBE))
	if err != nil {
		t.Fatal(err)
	}

	// Inject post-write lost-ACK failures on the FIRST page upload AND the FIRST manifest upload!
	var pageDropped, manifestDropped atomic.Bool
	ckptBE.SetPutIfAbsentHook(func(key string, wrote bool) error {
		if !wrote {
			return nil
		}
		if strings.Contains(key, "/p/") && !pageDropped.Swap(true) {
			return errors.New("simulated TCP reset after S3 PUT landed (page lost-ACK)")
		}
		if strings.Contains(key, "/m/") && !manifestDropped.Swap(true) {
			return errors.New("simulated TCP reset after S3 PUT landed (manifest lost-ACK)")
		}
		return nil
	})

	rawMem := deterministicMemory(0x33, 512<<10)
	m1, err := visorfuse.SuspendSandbox(ctx, srv1, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-ack-1",
		SandboxID:     "sbx-ack",
		SourceNodeID:  "node-1",
		TenantID:      "acme",
		VolumeID:      "vol-ack",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        rawMem,
	}, visorfuse.CheckpointConfig{
		Store:      ckptBE,
		Cipher:     ckptCipher,
		HMACKey:    testHMACKey(),
		Compress:   true,
		MaxRetries: 4,
	})
	if err != nil {
		t.Fatalf("SuspendSandbox with lost-ACK faults: %v", err)
	}
	_ = srv1.Close()
	ckptBE.SetPutIfAbsentHook(nil)

	if m1.Stats.LostACKRecovered < 2 {
		t.Fatalf("expected >= 2 lost-ACK recoveries (page + manifest), got %d", m1.Stats.LostACKRecovered)
	}

	// Restore onto Node 2, mutate one memory page, and take Checkpoint #2 to verify cross-checkpoint dedup!
	sp2, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer sp2.Close()
	restored, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-2",
		TargetTenantID:      "acme",
		TargetVolumeID:      "vol-ack",
		CheckpointID:        "ckpt-ack-1",
		CheckpointStore:     ckptBE,
		CheckpointCipher:    ckptCipher,
		HMACKey:             testHMACKey(),
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     validMountSpec(sp2.HostFD, visorfuse.SocketKindStream, "acme", "vol-ack"),
		TargetHostConn:      sp2.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
	})
	if err != nil {
		t.Fatalf("RestoreToNode after lost-ACK recovery: %v", err)
	}
	if !bytes.Equal(restored.Memory, rawMem) {
		t.Fatalf("memory mismatch after lost-ACK recovery")
	}

	// Modify only the last 64 KiB page and suspend again as ckpt-ack-2.
	mem2 := append([]byte(nil), restored.Memory...)
	if _, err := rand.Read(mem2[len(mem2)-visorfuse.DefaultCheckpointPageSize:]); err != nil {
		t.Fatal(err)
	}
	m2, err := visorfuse.SuspendSandbox(ctx, restored.Server, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-ack-2",
		SandboxID:     "sbx-ack",
		SourceNodeID:  "node-2",
		TenantID:      "acme",
		VolumeID:      "vol-ack",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        mem2,
	}, visorfuse.CheckpointConfig{
		Store:    ckptBE,
		Cipher:   ckptCipher,
		HMACKey:  testHMACKey(),
		Compress: true,
	})
	if err != nil {
		t.Fatalf("second SuspendSandbox: %v", err)
	}
	_ = restored.Server.Close()

	// Only 1 newly dirtied page should have been uploaded in ckpt-ack-2!
	if m2.Stats.UploadedPages != 1 {
		t.Fatalf("incremental checkpoint uploaded %d pages, want 1", m2.Stats.UploadedPages)
	}

	// Genuine manifest collision with different payload at ckpt-ack-1 MUST fail closed with ErrCheckpointExists.
	sp3, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer sp3.Close()
	r2, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-3",
		TargetTenantID:      "acme",
		TargetVolumeID:      "vol-ack",
		CheckpointID:        "ckpt-ack-2",
		CheckpointStore:     ckptBE,
		CheckpointCipher:    ckptCipher,
		HMACKey:             testHMACKey(),
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     validMountSpec(sp3.HostFD, visorfuse.SocketKindStream, "acme", "vol-ack"),
		TargetHostConn:      sp3.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
	})
	if err != nil {
		t.Fatalf("RestoreToNode #2: %v", err)
	}
	defer r2.Server.Close()
	if _, err := visorfuse.SuspendSandbox(ctx, r2.Server, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-ack-1", // colliding ID with different memory!
		SandboxID:     "sbx-ack",
		SourceNodeID:  "node-3",
		TenantID:      "acme",
		VolumeID:      "vol-ack",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        mem2,
	}, visorfuse.CheckpointConfig{
		Store:    ckptBE,
		Cipher:   ckptCipher,
		HMACKey:  testHMACKey(),
		Compress: true,
	}); !errors.Is(err, visorfuse.ErrCheckpointExists) {
		t.Fatalf("colliding checkpoint ID = %v, want ErrCheckpointExists", err)
	}
}

func TestConcurrentFUSEStressRaceFree(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visor-stress-test")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/stress/v/vol-race")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-stress")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)
	vfsCipher := newHybridCipher(t)
	ckptCipher := newHybridCipher(t)

	if err := vfs.Create(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher}); err != nil {
		t.Fatal(err)
	}
	vol, err := vfs.Attach(ctx, vfs.Config{Backend: vfsBE, Cipher: vfsCipher, Round: 1, Journal: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	sp, err := visorfuse.NewUnixSocketPair(visorfuse.PreferredSocketKind())
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	srv, err := visorfuse.NewExternalFUSEServer(validMountSpec(sp.HostFD, sp.Kind, "stress", "vol-race"), vol, sp.HostConn, visorfuse.WithCountingBackend(vfsBE))
	if err != nil {
		t.Fatal(err)
	}
	srvCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = srv.Serve(srvCtx) }()

	cli := visorfuse.NewGuestFUSEClient(sp.GuestConn, sp.Kind)
	defer cli.Close()
	if _, err := cli.Init(ctx, visorfuse.InitRequest{
		Flags:    visorfuse.InitFlagNoGuestPriv | visorfuse.InitFlagNoGoferHop,
		TenantID: "stress",
		VolumeID: "vol-race",
	}); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	expected := make([][]byte, workers)
	inodes := make([]uint64, workers)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			name := fmt.Sprintf("worker-%02d.bin", id)
			attr, fh, err := cli.Create(ctx, uint64(vfs.RootInode), name, 0o644)
			if err != nil {
				errCh <- fmt.Errorf("worker %d create: %w", id, err)
				return
			}
			inodes[id] = attr.Inode
			data := bytes.Repeat([]byte(fmt.Sprintf("worker-%02d-block-", id)), 2048)
			expected[id] = data
			for off := 0; off < len(data); off += 8192 {
				end := min(off+8192, len(data))
				if _, err := cli.Write(ctx, fh, uint64(off), data[off:end]); err != nil {
					errCh <- fmt.Errorf("worker %d write: %w", id, err)
					return
				}
			}
			if err := cli.Fsync(ctx, attr.Inode, fh); err != nil {
				errCh <- fmt.Errorf("worker %d fsync: %w", id, err)
				return
			}
			got, err := cli.ReadAll(ctx, fh, 0, len(data), 16384)
			if err != nil || !bytes.Equal(got, data) {
				errCh <- fmt.Errorf("worker %d readback mismatch: err=%v len=%d", id, err, len(got))
				return
			}
			if err := cli.Flush(ctx, fh); err != nil {
				errCh <- fmt.Errorf("worker %d flush: %w", id, err)
				return
			}
			if err := cli.Release(ctx, fh); err != nil {
				errCh <- fmt.Errorf("worker %d release: %w", id, err)
				return
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	descs := make([]visorfuse.OpenDescriptor, workers)
	for w := 0; w < workers; w++ {
		descs[w] = visorfuse.OpenDescriptor{
			FD:    10 + w,
			Path:  fmt.Sprintf("/worker-%02d.bin", w),
			Inode: inodes[w],
		}
	}

	rawMem := deterministicMemory(0x99, 512<<10)
	if _, err := visorfuse.SuspendSandbox(ctx, srv, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-stress-1",
		SandboxID:     "sbx-stress",
		SourceNodeID:  "node-1",
		TenantID:      "stress",
		VolumeID:      "vol-race",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        rawMem,
		Descriptors:   descs,
	}, visorfuse.CheckpointConfig{
		Store:    ckptBE,
		Cipher:   ckptCipher,
		HMACKey:  testHMACKey(),
		Compress: true,
	}); err != nil {
		t.Fatalf("SuspendSandbox stress: %v", err)
	}
	cancel()
	_ = cli.Close()
	_ = srv.Close()

	sp2, err := visorfuse.NewUnixSocketPair(visorfuse.PreferredSocketKind())
	if err != nil {
		t.Fatal(err)
	}
	defer sp2.Close()
	restored, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-2",
		TargetTenantID:      "stress",
		TargetVolumeID:      "vol-race",
		CheckpointID:        "ckpt-stress-1",
		CheckpointStore:     ckptBE,
		CheckpointCipher:    ckptCipher,
		HMACKey:             testHMACKey(),
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     validMountSpec(sp2.HostFD, sp2.Kind, "stress", "vol-race"),
		TargetHostConn:      sp2.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
	})
	if err != nil {
		t.Fatalf("RestoreToNode stress: %v", err)
	}
	defer restored.Volume.Close()
	defer restored.Server.Close()

	if !bytes.Equal(restored.Memory, rawMem) {
		t.Fatalf("stress restored memory mismatch")
	}
	for w := 0; w < workers; w++ {
		f, err := restored.Volume.Open(ctx, vfs.InodeID(inodes[w]))
		if err != nil {
			t.Fatalf("open restored worker %d: %v", w, err)
		}
		buf := make([]byte, len(expected[w]))
		n, err := f.ReadAt(ctx, buf, 0)
		_ = f.Close()
		if err != nil || n != len(expected[w]) || !bytes.Equal(buf, expected[w]) {
			t.Fatalf("restored worker %d content mismatch: n=%d err=%v", w, n, err)
		}
	}
}

func TestFencingRejectsZombieSourceAfterRestore(t *testing.T) {
	ctx := context.Background()
	s3Srv := storetest.NewS3(t, "visor-fence-test")
	vfsBE := newLiveS3Backend(t, s3Srv, "t/acme/v/vol-fence")
	ckptBE := newLiveS3Backend(t, s3Srv, "ckpt-fence")
	vfsBE.SetSuppressClose(true)
	ckptBE.SetSuppressClose(true)
	vfsCipher := newHybridCipher(t)

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
	srvA, err := visorfuse.NewExternalFUSEServer(validMountSpec(spA.HostFD, visorfuse.SocketKindStream, "acme", "vol-fence"), volA, spA.HostConn, visorfuse.WithCountingBackend(vfsBE))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := visorfuse.SuspendSandbox(ctx, srvA, visorfuse.SandboxSnapshot{
		CheckpointID:  "ckpt-fence-1",
		SandboxID:     "sbx-fence",
		SourceNodeID:  "node-a",
		TenantID:      "acme",
		VolumeID:      "vol-fence",
		PrivilegeMode: visorfuse.PrivilegeUnprivileged,
		Memory:        deterministicMemory(0x11, 128<<10),
	}, visorfuse.CheckpointConfig{
		Store:    ckptBE,
		HMACKey:  testHMACKey(),
		Compress: true,
	}); err != nil {
		t.Fatal(err)
	}
	_ = srvA.Close()

	// Node B restores and claims Round 2.
	spB, err := visorfuse.NewUnixSocketPair(visorfuse.SocketKindStream)
	if err != nil {
		t.Fatal(err)
	}
	defer spB.Close()
	restored, err := visorfuse.RestoreToNode(ctx, visorfuse.RestoreRequest{
		TargetNodeID:        "node-b",
		TargetTenantID:      "acme",
		TargetVolumeID:      "vol-fence",
		CheckpointID:        "ckpt-fence-1",
		CheckpointStore:     ckptBE,
		HMACKey:             testHMACKey(),
		VFSBackend:          vfsBE,
		VFSCipher:           vfsCipher,
		TargetJournalDir:    t.TempDir(),
		TargetMountSpec:     validMountSpec(spB.HostFD, visorfuse.SocketKindStream, "acme", "vol-fence"),
		TargetHostConn:      spB.HostConn,
		TargetPrivilegeMode: visorfuse.PrivilegeUnprivileged,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Volume.Close()
	defer restored.Server.Close()

	// Zombie Node A attempting to re-attach at stale round 1 or 2 MUST fail with vfs.ErrStaleRound.
	for _, staleRound := range []uint64{1, 2} {
		if _, err := vfs.Attach(ctx, vfs.Config{
			Backend: vfsBE,
			Cipher:  vfsCipher,
			Round:   staleRound,
			Journal: t.TempDir(),
		}); !errors.Is(err, vfs.ErrStaleRound) {
			t.Fatalf("zombie attach at stale round %d = %v, want vfs.ErrStaleRound", staleRound, err)
		}
	}
}
