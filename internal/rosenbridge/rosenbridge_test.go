package rosenbridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

func TestSlabPoolPageAlignmentAndZeroLeak(t *testing.T) {
	pool, err := NewSlabPool("tenant-a", 0xdeadbeef, 4, 8192)
	if err != nil {
		t.Fatalf("NewSlabPool: %v", err)
	}
	if !pool.IsPageAligned() {
		t.Fatalf("expected slab base address to be %d-byte page-aligned", PageSize)
	}
	if pool.SlotSize() != 8192 || pool.Size() != 4*8192 {
		t.Fatalf("unexpected slab geometry: slot=%d total=%d", pool.SlotSize(), pool.Size())
	}

	// Reserve [0, 4096) and ensure overlapping [2048, 6144) is rejected.
	s1, err := pool.ReserveRange("tenant-a", 0xdeadbeef, 0, 4096)
	if err != nil {
		t.Fatalf("ReserveRange 1: %v", err)
	}
	copy(s1, []byte("secret-tenant-a-bytes"))

	if _, err := pool.ReserveRange("tenant-a", 0xdeadbeef, 2048, 4096); !errors.Is(err, ErrDescriptorOverlap) {
		t.Fatalf("expected ErrDescriptorOverlap, got %v", err)
	}
	// Adjacent non-overlapping [4096, 8192) must succeed.
	if _, err := pool.ReserveRange("tenant-a", 0xdeadbeef, 4096, 4096); err != nil {
		t.Fatalf("expected adjacent range to succeed, got %v", err)
	}
	pool.ReleaseRange(0, 4096)
	pool.ReleaseRange(4096, 4096)

	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	mmaps, fds := pool.OpenResources()
	if mmaps != 0 || fds != 0 {
		t.Fatalf("leaked slab resources after Close: mmaps=%d fds=%d", mmaps, fds)
	}
}

func TestDescriptorValidationAndIntegerOverflowRejection(t *testing.T) {
	const (
		owner    = "tenant-alpha"
		token    = uint64(0x1122334455667788)
		slabSize = uint64(256 << 10)
		slotSize = uint64(64 << 10)
	)

	valid := Descriptor{
		Op:          OpRead,
		TenantID:    owner,
		TenantToken: token,
		Path:        "data/shard.bin",
		FileOffset:  0,
		SlabOffset:  0,
		Length:      4096,
		DeclaredLen: 4096,
	}
	if err := ValidateDescriptor(valid, owner, token, slabSize, slotSize, MaxFileSize); err != nil {
		t.Fatalf("valid descriptor rejected: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(*Descriptor)
		wantErr error
		wantSys syscall.Errno
	}{
		{
			name:    "forged_tenant_id",
			mutate:  func(d *Descriptor) { d.TenantID = "tenant-beta" },
			wantErr: ErrTenantMismatch,
			wantSys: syscall.EACCES,
		},
		{
			name:    "forged_tenant_token",
			mutate:  func(d *Descriptor) { d.TenantToken = token ^ 1 },
			wantErr: ErrTenantMismatch,
			wantSys: syscall.EACCES,
		},
		{
			name:    "unsupported_op_stat",
			mutate:  func(d *Descriptor) { d.Op = OpStat },
			wantErr: ErrUnsupportedOp,
			wantSys: syscall.EOPNOTSUPP,
		},
		{
			name:    "zero_length",
			mutate:  func(d *Descriptor) { d.Length = 0; d.DeclaredLen = 0 },
			wantErr: ErrDescriptorTruncated,
			wantSys: syscall.EINVAL,
		},
		{
			name:    "truncated_declared_length",
			mutate:  func(d *Descriptor) { d.DeclaredLen = 2048 },
			wantErr: ErrDescriptorTruncated,
			wantSys: syscall.EINVAL,
		},
		{
			name:    "exceeds_slot_size",
			mutate:  func(d *Descriptor) { d.Length = slotSize + 1; d.DeclaredLen = slotSize + 1 },
			wantErr: ErrDescriptorOutOfBounds,
			wantSys: syscall.EOVERFLOW,
		},
		{
			name:    "slab_offset_plus_len_past_end",
			mutate:  func(d *Descriptor) { d.SlabOffset = slabSize - 2048 },
			wantErr: ErrDescriptorOutOfBounds,
			wantSys: syscall.EOVERFLOW,
		},
		{
			name:    "slab_offset_uint64_overflow",
			mutate:  func(d *Descriptor) { d.SlabOffset = math.MaxUint64 - 1024 },
			wantErr: ErrDescriptorOutOfBounds,
			wantSys: syscall.EOVERFLOW,
		},
		{
			name:    "file_offset_overflow",
			mutate:  func(d *Descriptor) { d.FileOffset = MaxFileSize - 1024 },
			wantErr: ErrDescriptorOutOfBounds,
			wantSys: syscall.EOVERFLOW,
		},
		{
			name:    "path_traversal_dotdot",
			mutate:  func(d *Descriptor) { d.Path = "../tenant-beta/secret.bin" },
			wantErr: ErrPathEscape,
			wantSys: syscall.EACCES,
		},
		{
			name:    "path_nul_byte",
			mutate:  func(d *Descriptor) { d.Path = "data/a\x00b" },
			wantErr: ErrPathEscape,
			wantSys: syscall.EACCES,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := valid
			tc.mutate(&d)
			err := ValidateDescriptor(d, owner, token, slabSize, slotSize, MaxFileSize)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got err=%v, want %v", err, tc.wantErr)
			}
			if got := Errno(err); got != tc.wantSys {
				t.Fatalf("Errno(%v) = %v, want %v", err, got, tc.wantSys)
			}
			if !errors.Is(tc.wantErr, ErrUnsupportedOp) && !IsSecurityViolation(err) {
				t.Fatalf("expected IsSecurityViolation(%v) == true", err)
			}
		})
	}
}

func TestSymlinkConfinementAndPathTraversal(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	outsideFile := filepath.Join(t.TempDir(), "host-secret.txt")
	if err := os.WriteFile(outsideFile, []byte("HOST-SECRET-DO-NOT-LEAK"), 0o600); err != nil {
		t.Fatalf("write outsideFile: %v", err)
	}

	store, err := NewOSBackingStore(baseDir)
	if err != nil {
		t.Fatalf("NewOSBackingStore: %v", err)
	}
	defer store.Close()

	tenantRoot, err := store.TenantRoot("tenant-a")
	if err != nil {
		t.Fatalf("TenantRoot: %v", err)
	}
	// Create leaf symlink and directory symlink pointing outside tenantRoot.
	if err := os.Symlink(outsideFile, filepath.Join(tenantRoot, "escape-leaf.txt")); err != nil {
		t.Fatalf("symlink leaf: %v", err)
	}
	if err := os.Symlink(filepath.Dir(outsideFile), filepath.Join(tenantRoot, "escape-dir")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}

	cfg := DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	cfg.MinTransferBytes = 16
	cfg.BulkBypassBytes = 16
	router, err := NewRouter(store, cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer router.Close()

	if _, err := router.RegisterTenant("tenant-a", 8, 64<<10); err != nil {
		t.Fatalf("RegisterTenant: %v", err)
	}

	escapePaths := []string{
		"escape-leaf.txt",
		"escape-dir/host-secret.txt",
		"../tenant-b/secret.txt",
		"/../../etc/passwd",
		"sub/../../escape.txt",
	}

	buf := make([]byte, 64)
	for _, p := range escapePaths {
		// Fast path attempt MUST fail closed with ErrPathEscape (never fall back silently).
		if _, _, err := router.ReadAt(ctx, "tenant-a", p, buf, 0); !errors.Is(err, ErrPathEscape) {
			t.Fatalf("Router.ReadAt(%q): got %v, want ErrPathEscape", p, err)
		}
		// Canonical Gofer attempt MUST also fail with ErrPathEscape.
		if _, err := router.Gofer().ReadAt(ctx, "tenant-a", p, buf, 0); !errors.Is(err, ErrPathEscape) {
			t.Fatalf("Gofer.ReadAt(%q): got %v, want ErrPathEscape", p, err)
		}
		// Stat MUST also reject symlinks/traversal.
		if _, _, err := router.Stat(ctx, "tenant-a", p); !errors.Is(err, ErrPathEscape) {
			t.Fatalf("Router.Stat(%q): got %v, want ErrPathEscape", p, err)
		}
	}
}

func TestMultiTenantIsolation(t *testing.T) {
	ctx := context.Background()
	store, err := NewOSBackingStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewOSBackingStore: %v", err)
	}
	defer store.Close()

	cfg := DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	router, err := NewRouter(store, cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer router.Close()

	chA, err := router.RegisterTenant("tenant-a", 8, 64<<10)
	if err != nil {
		t.Fatalf("RegisterTenant A: %v", err)
	}
	chB, err := router.RegisterTenant("tenant-b", 8, 64<<10)
	if err != nil {
		t.Fatalf("RegisterTenant B: %v", err)
	}
	if chA.Token() == chB.Token() {
		t.Fatalf("tenant tokens collided")
	}

	secretA := bytes.Repeat([]byte("TENANT-A-CONFIDENTIAL-"), 1024)
	if _, _, err := router.WriteAt(ctx, "tenant-a", "vault/secret.bin", secretA, 0, true); err != nil {
		t.Fatalf("WriteAt tenant-a: %v", err)
	}

	// 1. Tenant B cannot read Tenant A's file from its own namespace.
	buf := make([]byte, len(secretA))
	if _, _, err := router.ReadAt(ctx, "tenant-b", "vault/secret.bin", buf, 0); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tenant-b unexpectedly saw tenant-a file: %v", err)
	}

	// 2. Tenant B cannot acquire or reserve ranges on Tenant A's SlabPool.
	if _, _, _, err := chA.Slab().AcquireSlot("tenant-b", chB.Token(), 4096); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("cross-tenant AcquireSlot: got %v, want ErrTenantMismatch", err)
	}
	if _, err := chA.Slab().ReserveRange("tenant-a", chB.Token(), 0, 4096); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("forged-token ReserveRange: got %v, want ErrTenantMismatch", err)
	}

	// 3. Forged descriptor submitted onto Tenant A's ring with Tenant B's credentials fails closed.
	_, err = chA.SubmitRawDescriptor(ctx, Descriptor{
		Op:          OpRead,
		TenantID:    "tenant-b",
		TenantToken: chB.Token(),
		Path:        "vault/secret.bin",
		FileOffset:  0,
		SlabOffset:  0,
		Length:      4096,
		DeclaredLen: 4096,
	})
	if !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("forged descriptor on chA: got %v, want ErrTenantMismatch", err)
	}

	// 4. Forged descriptor with Tenant A's ID but guessed/wrong token fails closed.
	_, err = chA.SubmitRawDescriptor(ctx, Descriptor{
		Op:          OpRead,
		TenantID:    "tenant-a",
		TenantToken: chA.Token() ^ 0xff,
		Path:        "vault/secret.bin",
		FileOffset:  0,
		SlabOffset:  0,
		Length:      4096,
		DeclaredLen: 4096,
	})
	if !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("wrong-token descriptor on chA: got %v, want ErrTenantMismatch", err)
	}

	// 5. Tenant A's own data remains completely intact.
	got := make([]byte, len(secretA))
	n, reason, err := router.ReadAt(ctx, "tenant-a", "vault/secret.bin", got, 0)
	if err != nil || n != len(secretA) || reason != ReasonAdmitted {
		t.Fatalf("tenant-a read failed: n=%d reason=%s err=%v", n, reason, err)
	}
	if !bytes.Equal(got, secretA) {
		t.Fatalf("tenant-a data corrupted")
	}
}

func TestProfilerGatedConditionalAdmissionAndNegativeRegimes(t *testing.T) {
	ctx := context.Background()
	store, err := NewOSBackingStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewOSBackingStore: %v", err)
	}
	defer store.Close()

	cfg := DefaultGateConfig()
	cfg.RequireProfilerConfirmation = true
	cfg.MinProfileSamples = 16
	cfg.MinOverheadRatio = 0.20
	cfg.MinTransferBytes = 4096
	cfg.BulkBypassBytes = 16384

	router, err := NewRouter(store, cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer router.Close()

	if _, err := router.RegisterTenant("tenant-1", 16, 64<<10); err != nil {
		t.Fatalf("RegisterTenant: %v", err)
	}

	// Seed a 1 MiB file via direct backing write so profiling starts clean.
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	if _, _, err := store.WriteAt(ctx, "tenant-1", "bulk.bin", payload, 0, false); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	// Phase 1: Before MinProfileSamples gofer ops, fast path MUST NOT engage.
	buf := make([]byte, 32<<10)
	for i := 0; i < 15; i++ {
		_, reason, err := router.ReadAt(ctx, "tenant-1", "bulk.bin", buf, int64(i)*32768)
		if err != nil {
			t.Fatalf("warmup read %d: %v", i, err)
		}
		if reason != FallbackProfilerNotTriggered {
			t.Fatalf("read %d before profiler confirmation: got reason=%s, want %s", i, reason, FallbackProfilerNotTriggered)
		}
	}

	// 16th gofer sample completes warmup and confirms gofer bottleneck.
	_, reason, err := router.ReadAt(ctx, "tenant-1", "bulk.bin", buf, 15*32768)
	if err != nil {
		t.Fatalf("16th warmup read: %v", err)
	}
	if reason != FallbackProfilerNotTriggered {
		t.Fatalf("16th read: got %s, want %s", reason, FallbackProfilerNotTriggered)
	}
	prof := router.Gate().Profiler().Summary()
	if !prof.BottleneckConfirmed {
		t.Fatalf("expected profiler to confirm gofer bottleneck on 32 KiB socketpair RPCs, summary=%+v", prof)
	}

	// Phase 2: Now bulk read MUST engage RosenBridge fast path.
	n, reason, err := router.ReadAt(ctx, "tenant-1", "bulk.bin", buf, 16*32768)
	if err != nil || n != len(buf) {
		t.Fatalf("admitted read failed: n=%d err=%v", n, err)
	}
	if reason != ReasonAdmitted {
		t.Fatalf("expected ReasonAdmitted after profiler confirmation, got %s", reason)
	}
	if !bytes.Equal(buf, payload[16*32768:17*32768]) {
		t.Fatalf("fast-path read bytes mismatch")
	}

	// Phase 3: Negative regimes MUST stay on canonical gofer.
	// (a) Tiny 64B read -> FallbackSmallIO
	tiny := make([]byte, 64)
	if _, reason, err := router.ReadAt(ctx, "tenant-1", "bulk.bin", tiny, 0); err != nil || reason != FallbackSmallIO {
		t.Fatalf("tiny read: reason=%s err=%v, want %s", reason, err, FallbackSmallIO)
	}
	// (b) Metadata Stat -> FallbackUnsupportedOp
	if sz, reason, err := router.Stat(ctx, "tenant-1", "bulk.bin"); err != nil || sz != int64(len(payload)) || reason != FallbackUnsupportedOp {
		t.Fatalf("stat: sz=%d reason=%s err=%v, want %s", sz, reason, err, FallbackUnsupportedOp)
	}
	// (c) Cold single 8 KiB read on new file (< BulkBypassBytes) -> FallbackNonSequentialCold
	if _, _, err := store.WriteAt(ctx, "tenant-1", "cold.bin", payload[:65536], 0, false); err != nil {
		t.Fatalf("seed cold.bin: %v", err)
	}
	med := make([]byte, 8192)
	if _, reason, err := router.ReadAt(ctx, "tenant-1", "cold.bin", med, 0); err != nil || reason != FallbackNonSequentialCold {
		t.Fatalf("cold non-seq read: reason=%s err=%v, want %s", reason, err, FallbackNonSequentialCold)
	}
	// Second contiguous 8 KiB read on cold.bin establishes sequential streak -> ReasonAdmitted!
	if _, reason, err := router.ReadAt(ctx, "tenant-1", "cold.bin", med, 8192); err != nil || reason != ReasonAdmitted {
		t.Fatalf("2nd sequential read: reason=%s err=%v, want %s", reason, err, ReasonAdmitted)
	}
	// (d) Master switch disabled -> FallbackDisabled
	router.Gate().SetEnabled(false)
	if _, reason, err := router.ReadAt(ctx, "tenant-1", "bulk.bin", buf, 0); err != nil || reason != FallbackDisabled {
		t.Fatalf("disabled read: reason=%s err=%v, want %s", reason, err, FallbackDisabled)
	}
	router.Gate().SetEnabled(true)
	// (e) Cross-tenant admission request -> FallbackCrossTenant
	admit, reason := router.Gate().Evaluate(AdmissionRequest{
		TenantID:    "tenant-1",
		Op:          OpRead,
		Path:        "bulk.bin",
		Offset:      0,
		Length:      32768,
		CrossTenant: true,
	})
	if admit || reason != FallbackCrossTenant {
		t.Fatalf("cross-tenant eval: admit=%v reason=%s", admit, reason)
	}
}

func TestGracefulFallbackWhenRingFull(t *testing.T) {
	ctx := context.Background()
	store, err := NewOSBackingStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewOSBackingStore: %v", err)
	}
	defer store.Close()

	data := bytes.Repeat([]byte("FALLBACK-DATA-"), 4096)
	if _, _, err := store.WriteAt(ctx, "tenant-sat", "file.bin", data, 0, false); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cfg := DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	router, err := NewRouter(store, cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer router.Close()

	// Provision a minimal 2-slot slab/ring and hold both slab slots via ReadSlab.
	if _, err := router.RegisterTenant("tenant-sat", 2, 64<<10); err != nil {
		t.Fatalf("RegisterTenant: %v", err)
	}

	s1, rel1, r1, err := router.ReadSlab(ctx, "tenant-sat", "file.bin", 0, 16384)
	if err != nil || r1 != ReasonAdmitted {
		t.Fatalf("ReadSlab 1: reason=%s err=%v", r1, err)
	}
	defer rel1()
	s2, rel2, r2, err := router.ReadSlab(ctx, "tenant-sat", "file.bin", 16384, 16384)
	if err != nil || r2 != ReasonAdmitted {
		t.Fatalf("ReadSlab 2: reason=%s err=%v", r2, err)
	}
	defer rel2()

	if !bytes.Equal(s1, data[:16384]) || !bytes.Equal(s2, data[16384:32768]) {
		t.Fatalf("held slab data mismatch")
	}

	// All 2 slab slots are occupied: a 3rd concurrent ReadAt MUST seamlessly fall back to canonical gofer!
	out := make([]byte, 16384)
	n, reason, err := router.ReadAt(ctx, "tenant-sat", "file.bin", out, 32768)
	if err != nil || n != 16384 {
		t.Fatalf("3rd read while saturated failed: n=%d err=%v", n, err)
	}
	if reason != FallbackRingFull {
		t.Fatalf("expected FallbackRingFull, got %s", reason)
	}
	if !bytes.Equal(out, data[32768:49152]) {
		t.Fatalf("fallback read bytes mismatch")
	}

	stats := router.Stats()
	if stats.Gate.FallbackRingFull < 1 || stats.Gofer.Ops < 1 {
		t.Fatalf("expected FallbackRingFull & Gofer.Ops telemetry incremented: %+v", stats)
	}
}

func TestCRC32CIntegrityAndCorruptionDetection(t *testing.T) {
	ctx := context.Background()
	store, err := NewOSBackingStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewOSBackingStore: %v", err)
	}
	defer store.Close()

	ch, err := NewRosenBridgeChannel("tenant-crc", store, 4, 64<<10)
	if err != nil {
		t.Fatalf("NewRosenBridgeChannel: %v", err)
	}
	defer ch.Close()

	payload := bytes.Repeat([]byte("CRC-VERIFIED-BLOCK-"), 512)
	if _, err := ch.WriteAt(ctx, "checked.bin", payload, 0, true, true); err != nil {
		t.Fatalf("WriteAt with CRC: %v", err)
	}

	got := make([]byte, len(payload))
	if _, err := ch.ReadAt(ctx, "checked.bin", got, 0, true); err != nil {
		t.Fatalf("ReadAt with CRC: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("CRC verified read mismatch")
	}

	// Now submit a raw write descriptor with an intentionally corrupted CRC32C.
	slotIdx, slabOff, slabSlice, err := ch.Slab().AcquireSlot("tenant-crc", ch.Token(), 4096)
	if err != nil {
		t.Fatalf("AcquireSlot: %v", err)
	}
	defer ch.Slab().ReleaseSlot(slotIdx)
	copy(slabSlice, bytes.Repeat([]byte("X"), 4096))
	badCRC := crc32.Checksum(slabSlice, crc32cTable) ^ 0xdeadbeef

	_, err = ch.SubmitRawDescriptor(ctx, Descriptor{
		Op:          OpWrite,
		Flags:       FlagVerifyCRC,
		TenantID:    "tenant-crc",
		TenantToken: ch.Token(),
		Path:        "checked.bin",
		FileOffset:  0,
		SlabOffset:  slabOff,
		Length:      4096,
		DeclaredLen: 4096,
		CRC32C:      badCRC,
	})
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("expected ErrCorruptData on bad CRC, got %v", err)
	}

	// Backing file must remain untouched by the rejected corrupt write.
	verify := make([]byte, len(payload))
	if _, _, err := store.ReadAt(ctx, "tenant-crc", "checked.bin", verify, 0); err != nil {
		t.Fatalf("store.ReadAt: %v", err)
	}
	if !bytes.Equal(verify, payload) {
		t.Fatalf("rejected corrupt write mutated backing file")
	}
}

func TestConcurrentMultiTenantRaceAndZeroFDLeak(t *testing.T) {
	ctx := context.Background()
	store, err := NewOSBackingStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewOSBackingStore: %v", err)
	}

	cfg := DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	cfg.MaxTenantInFlightBytes = 1 << 20
	router, err := NewRouter(store, cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	router.SetVerifyCRC(true)

	tenants := []string{"tenant-0", "tenant-1", "tenant-2", "tenant-3"}
	channels := make([]*RosenBridgeChannel, len(tenants))
	for i, tid := range tenants {
		ch, err := router.RegisterTenant(tid, 16, 64<<10)
		if err != nil {
			t.Fatalf("RegisterTenant %s: %v", tid, err)
		}
		channels[i] = ch
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for ti, tid := range tenants {
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(tenantIdx int, tenantID string, workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(uint64(tenantIdx*100+workerID+1), 1345))
				fileName := fmt.Sprintf("worker-%d.bin", workerID)
				blockSize := 16384
				blocks := 16
				expected := make([]byte, blockSize*blocks)
				for i := range expected {
					expected[i] = byte(rng.Uint32())
				}

				// Sequential writes via Router (admitted to fast path or gracefully throttled to gofer).
				for b := 0; b < blocks; b++ {
					off := int64(b * blockSize)
					src := expected[b*blockSize : (b+1)*blockSize]
					if _, _, err := router.WriteAt(ctx, tenantID, fileName, src, off, false); err != nil {
						errCh <- fmt.Errorf("%s/%s write block %d: %w", tenantID, fileName, b, err)
						return
					}
				}
				// Interleaved reads (both ReadAt and 1-copy ReadSlab) + metadata Stat + tiny read.
				dst := make([]byte, blockSize)
				for b := 0; b < blocks; b++ {
					off := int64(b * blockSize)
					want := expected[b*blockSize : (b+1)*blockSize]
					if b%2 == 0 {
						n, _, err := router.ReadAt(ctx, tenantID, fileName, dst, off)
						if err != nil || n != blockSize || !bytes.Equal(dst, want) {
							errCh <- fmt.Errorf("%s/%s ReadAt block %d mismatch: n=%d err=%v", tenantID, fileName, b, n, err)
							return
						}
					} else {
						slabData, rel, _, err := router.ReadSlab(ctx, tenantID, fileName, off, blockSize)
						if err != nil {
							errCh <- fmt.Errorf("%s/%s ReadSlab block %d: %w", tenantID, fileName, b, err)
							return
						}
						eq := bytes.Equal(slabData, want)
						rel()
						if !eq {
							errCh <- fmt.Errorf("%s/%s ReadSlab block %d mismatch", tenantID, fileName, b)
							return
						}
					}
				}
				sz, _, err := router.Stat(ctx, tenantID, fileName)
				if err != nil || sz != int64(len(expected)) {
					errCh <- fmt.Errorf("%s/%s Stat: sz=%d err=%v", tenantID, fileName, sz, err)
					return
				}
			}(ti, tid, w)
		}
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if err := router.Close(); err != nil {
		t.Fatalf("router.Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}

	if fds := store.OpenFDs(); fds != 0 {
		t.Fatalf("leaked backing store FDs: %d", fds)
	}
	for _, ch := range channels {
		mmaps, fds := ch.Slab().OpenResources()
		if mmaps != 0 || fds != 0 {
			t.Fatalf("leaked slab resources for %s: mmaps=%d fds=%d", ch.TenantID(), mmaps, fds)
		}
	}
}

func TestPostOpenSymlinkSwapAndFairnessThrottle(t *testing.T) {
	ctx := context.Background()
	store, err := NewOSBackingStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewOSBackingStore: %v", err)
	}
	defer store.Close()

	outside := filepath.Join(t.TempDir(), "host-key.pem")
	if err := os.WriteFile(outside, []byte("PRIVATE-HOST-KEY"), 0o600); err != nil {
		t.Fatalf("write outside: %v", err)
	}

	cfg := DefaultGateConfig()
	cfg.RequireProfilerConfirmation = false
	cfg.MaxTenantInFlightBytes = 32 << 10 // 32 KiB cap to exercise fairness throttle
	router, err := NewRouter(store, cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer router.Close()

	ch, err := router.RegisterTenant("tenant-swap", 8, 64<<10)
	if err != nil {
		t.Fatalf("RegisterTenant: %v", err)
	}

	// 1. Write & read a regular file so OSBackingStore caches its FD.
	data := bytes.Repeat([]byte("A"), 32<<10)
	if _, _, err := router.WriteAt(ctx, "tenant-swap", "swap.bin", data, 0, false); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	// Replace the file on disk with a symlink pointing outside tenant root.
	root, _ := store.TenantRoot("tenant-swap")
	targetPath := filepath.Join(root, "swap.bin")
	if err := os.Remove(targetPath); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := os.Symlink(outside, targetPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	// Next read MUST re-verify confinement and fail closed with ErrPathEscape.
	if _, _, err := router.ReadAt(ctx, "tenant-swap", "swap.bin", make([]byte, 32<<10), 0); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("post-open symlink swap: got %v, want ErrPathEscape", err)
	}

	// 2. Live overlapping raw descriptor rejection on ch's host worker.
	if _, _, err := store.WriteAt(ctx, "tenant-swap", "overlap.bin", data, 0, false); err != nil {
		t.Fatalf("seed overlap.bin: %v", err)
	}
	if _, err := ch.Slab().ReserveRange("tenant-swap", ch.Token(), 0, 8192); err != nil {
		t.Fatalf("ReserveRange: %v", err)
	}
	_, err = ch.SubmitRawDescriptor(ctx, Descriptor{
		Op:          OpRead,
		TenantID:    "tenant-swap",
		TenantToken: ch.Token(),
		Path:        "overlap.bin",
		FileOffset:  0,
		SlabOffset:  4096, // overlaps [0, 8192)
		Length:      8192,
		DeclaredLen: 8192,
	})
	ch.Slab().ReleaseRange(0, 8192)
	if !errors.Is(err, ErrDescriptorOverlap) {
		t.Fatalf("live overlapping descriptor: got %v, want ErrDescriptorOverlap", err)
	}

	// 3. Collaborative fairness throttle: hold 32 KiB in-flight slab read, next 16 KiB MUST throttle to gofer.
	_, rel, reason1, err := router.ReadSlab(ctx, "tenant-swap", "overlap.bin", 0, 32<<10)
	if err != nil || reason1 != ReasonAdmitted {
		t.Fatalf("ReadSlab hold: reason=%s err=%v", reason1, err)
	}
	defer rel()

	buf := make([]byte, 16<<10)
	n, reason2, err := router.ReadAt(ctx, "tenant-swap", "overlap.bin", buf, 0)
	if err != nil || n != len(buf) {
		t.Fatalf("throttled ReadAt failed: n=%d err=%v", n, err)
	}
	if reason2 != FallbackThrottled {
		t.Fatalf("expected FallbackThrottled when tenant quota reached, got %s", reason2)
	}
}
