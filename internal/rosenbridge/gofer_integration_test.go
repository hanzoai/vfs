package rosenbridge

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

func openCanonicalTenantVolume(t *testing.T, ctx context.Context, name string) (*vfs.Volume, vfs.Config) {
	t.Helper()
	root := t.TempDir()
	storeURL := "file://" + filepath.Join(root, "store-"+name)
	journalDir := filepath.Join(root, "journal-"+name)

	be, err := backend.Open(ctx, storeURL)
	if err != nil {
		t.Fatalf("backend.Open(%s): %v", name, err)
	}
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("GenerateHybridIdentity: %v", err)
	}
	cipher := vfs.Hybrid(id)
	baseCfg := vfs.Config{
		Backend:         be,
		Cipher:          cipher,
		Round:           0,
		Journal:         journalDir,
		JournalCapacity: 64 << 20,
	}
	if err := vfs.Create(ctx, baseCfg); err != nil {
		t.Fatalf("vfs.Create(%s): %v", name, err)
	}
	attachCfg := baseCfg
	attachCfg.Round = 1
	vol, err := vfs.Attach(ctx, attachCfg)
	if err != nil {
		t.Fatalf("vfs.Attach(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = vol.Close() })
	return vol, attachCfg
}

// TestCanonicalVFSVolumeGoferVsRosenBridgeParity wires both the canonical gofer
// baseline and the conditional RosenBridge fast path directly onto real
// post-quantum-sealed *vfs.Volume instances backed by the local file:// store
// and journal, verifying byte-for-byte parity, multi-tenant isolation, symlink
// confinement inside VFS, and measurable syscall/copy reduction.
func TestCanonicalVFSVolumeGoferVsRosenBridgeParity(t *testing.T) {
	ctx := context.Background()

	volA, cfgA := openCanonicalTenantVolume(t, ctx, "tenant-a")
	volB, _ := openCanonicalTenantVolume(t, ctx, "tenant-b")

	// Create a symlink inside volA to verify VFSBackingStore refuses symlink escapes.
	if _, err := volA.Symlink(ctx, vfs.RootInode, "badlink", "/etc/passwd"); err != nil {
		t.Fatalf("volA.Symlink: %v", err)
	}

	vfsStore := NewVFSBackingStore()
	if err := vfsStore.MountTenant("tenant-a", volA); err != nil {
		t.Fatalf("MountTenant A: %v", err)
	}
	if err := vfsStore.MountTenant("tenant-b", volB); err != nil {
		t.Fatalf("MountTenant B: %v", err)
	}
	defer vfsStore.Close()

	gateCfg := DefaultGateConfig()
	gateCfg.RequireProfilerConfirmation = true
	gateCfg.MinProfileSamples = 8
	gateCfg.MinOverheadRatio = 0.05 // VFS encryption/chunking increases backing time; confirm boundary overhead honestly
	gateCfg.MinTransferBytes = 4096
	gateCfg.BulkBypassBytes = 16384

	router, err := NewRouter(vfsStore, gateCfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer router.Close()
	router.SetVerifyCRC(true)

	if _, err := router.RegisterTenant("tenant-a", 16, 64<<10); err != nil {
		t.Fatalf("RegisterTenant A: %v", err)
	}
	if _, err := router.RegisterTenant("tenant-b", 16, 64<<10); err != nil {
		t.Fatalf("RegisterTenant B: %v", err)
	}

	// Write 512 KiB of deterministic data in 32 KiB chunks to Tenant A's canonical *vfs.Volume.
	const chunkSize = 32 << 10
	const chunks = 16
	fullData := make([]byte, chunkSize*chunks)
	for i := range fullData {
		fullData[i] = byte((i*73 + 19) & 0xff)
	}

	for i := 0; i < chunks; i++ {
		off := int64(i * chunkSize)
		src := fullData[i*chunkSize : (i+1)*chunkSize]
		if _, _, err := router.WriteAt(ctx, "tenant-a", "models/weights.bin", src, off, i == chunks-1); err != nil {
			t.Fatalf("WriteAt chunk %d: %v", i, err)
		}
	}

	// Ensure profiler confirmed gofer overhead during the initial warmup chunks
	// and admitted subsequent bulk chunks onto RosenBridge.
	statsBefore := router.Stats()
	if statsBefore.Gofer.Ops < 8 {
		t.Fatalf("expected >=8 warmup gofer ops before fast path activation, got %d", statsBefore.Gofer.Ops)
	}

	// Read every chunk back via Router (canonical gofer or RosenBridge fast path)
	// and also via 1-copy ReadSlab, verifying exact byte parity.
	gotRouter := make([]byte, len(fullData))
	for i := 0; i < chunks; i++ {
		off := int64(i * chunkSize)
		if i%2 == 0 {
			n, _, err := router.ReadAt(ctx, "tenant-a", "models/weights.bin", gotRouter[i*chunkSize:(i+1)*chunkSize], off)
			if err != nil || n != chunkSize {
				t.Fatalf("ReadAt chunk %d: n=%d err=%v", i, n, err)
			}
		} else {
			slabData, rel, _, err := router.ReadSlab(ctx, "tenant-a", "models/weights.bin", off, chunkSize)
			if err != nil {
				t.Fatalf("ReadSlab chunk %d: %v", i, err)
			}
			copy(gotRouter[i*chunkSize:(i+1)*chunkSize], slabData)
			rel()
		}
	}
	if !bytes.Equal(gotRouter, fullData) {
		t.Fatalf("Router readback mismatch against written payload")
	}

	// Verify direct canonical *vfs.Volume readback matches byte-for-byte.
	dirIn, err := volA.Lookup(ctx, vfs.RootInode, "models")
	if err != nil {
		t.Fatalf("volA.Lookup(models): %v", err)
	}
	fileIn, err := volA.Lookup(ctx, dirIn.ID, "weights.bin")
	if err != nil {
		t.Fatalf("volA.Lookup(weights.bin): %v", err)
	}
	vf, err := volA.Open(ctx, fileIn.ID)
	if err != nil {
		t.Fatalf("volA.Open: %v", err)
	}
	directGot := make([]byte, len(fullData))
	if _, err := vf.ReadAt(ctx, directGot, 0); err != nil {
		vf.Close()
		t.Fatalf("direct vf.ReadAt: %v", err)
	}
	vf.Close()
	if !bytes.Equal(directGot, fullData) {
		t.Fatalf("canonical vfs.Volume direct readback mismatch")
	}

	// Flush canonical Volume to sealed file:// store and verify independent read-only vfs.Open.
	if err := volA.Flush(ctx); err != nil {
		t.Fatalf("volA.Flush: %v", err)
	}
	roBe, err := backend.Open(ctx, "file://"+filepath.Join(filepath.Dir(cfgA.Journal), "store-tenant-a"))
	if err != nil {
		t.Fatalf("backend.Open RO: %v", err)
	}
	roVol, err := vfs.Open(ctx, vfs.Config{Backend: roBe, Cipher: cfgA.Cipher})
	if err != nil {
		t.Fatalf("vfs.Open RO: %v", err)
	}
	defer roVol.Close()
	roDir, err := roVol.Lookup(ctx, vfs.RootInode, "models")
	if err != nil {
		t.Fatalf("roVol.Lookup(models): %v", err)
	}
	roFileIn, err := roVol.Lookup(ctx, roDir.ID, "weights.bin")
	if err != nil {
		t.Fatalf("roVol.Lookup(weights.bin): %v", err)
	}
	roF, err := roVol.Open(ctx, roFileIn.ID)
	if err != nil {
		t.Fatalf("roVol.Open: %v", err)
	}
	roGot := make([]byte, len(fullData))
	if _, err := roF.ReadAt(ctx, roGot, 0); err != nil {
		roF.Close()
		t.Fatalf("roF.ReadAt: %v", err)
	}
	roF.Close()
	if !bytes.Equal(roGot, fullData) {
		t.Fatalf("flushed read-only vfs.Volume data mismatch")
	}

	// Verify VFS symlink rejection & cross-tenant isolation.
	if _, _, err := router.ReadAt(ctx, "tenant-a", "badlink", make([]byte, 32), 0); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("VFS symlink read: got %v, want ErrPathEscape", err)
	}
	if _, _, err := router.ReadAt(ctx, "tenant-b", "models/weights.bin", make([]byte, 32), 0); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("tenant-b saw tenant-a VFS file: got %v, want ErrNotExist", err)
	}

	// Verify fast-path activated and reduced syscalls/copies per op vs canonical gofer.
	stats := router.Stats()
	if stats.FastPath.Ops == 0 {
		t.Fatalf("expected FastPath.Ops > 0 after warmup on canonical VFS Volume, stats=%+v", stats)
	}
	goferScPerOp := float64(stats.Gofer.TotalSyscalls) / float64(stats.Gofer.Ops)
	fastScPerOp := float64(stats.FastPath.Syscalls) / float64(stats.FastPath.Ops)
	if fastScPerOp >= goferScPerOp {
		t.Fatalf("expected fast path syscalls/op (%.2f) < gofer syscalls/op (%.2f)", fastScPerOp, goferScPerOp)
	}
}
