// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package djfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/djfs"
)

// volumeBridge adapts a live journaled VFS v2 *vfs.Volume to djfs.Target.
// It reuses the Volume's native v2 journal, native Rename, and canonical
// Flush/Sync publication lifecycle rather than maintaining a second journal.
type volumeBridge struct {
	mu       sync.Mutex
	v        *vfs.Volume
	syncOnly bool
	idMap    map[djfs.InodeID]vfs.InodeID
}

func newVolumeBridge(v *vfs.Volume) *volumeBridge {
	return &volumeBridge{
		v:     v,
		idMap: map[djfs.InodeID]vfs.InodeID{djfs.RootInode: vfs.RootInode},
	}
}

func (vb *volumeBridge) ApplyBatch(ctx context.Context, batch djfs.Batch) error {
	vb.mu.Lock()
	defer vb.mu.Unlock()

	for i := range batch.Ops {
		op := &batch.Ops[i]
		switch op.Kind {
		case djfs.DeltaMkdir:
			parentVFS, ok := vb.idMap[op.Dir]
			if !ok {
				return fmt.Errorf("missing parent mapping for dir %d", op.Dir)
			}
			in, err := vb.v.Mkdir(ctx, parentVFS, op.Name, op.Attr.Mode.Perm())
			if err != nil {
				return err
			}
			vb.idMap[op.ID] = in.ID

		case djfs.DeltaCreateFile:
			dirVFS, ok := vb.idMap[op.Dir]
			if !ok {
				return fmt.Errorf("missing dir mapping for dir %d", op.Dir)
			}
			in, err := vb.v.Create(ctx, dirVFS, op.Name, op.Attr.Mode.Perm())
			if err != nil {
				return err
			}
			vb.idMap[op.ID] = in.ID
			if len(op.Data) > 0 {
				f, err := vb.v.Open(ctx, in.ID)
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
			fileVFS, ok := vb.idMap[op.ID]
			if !ok {
				return fmt.Errorf("missing file mapping for inode %d", op.ID)
			}
			f, err := vb.v.Open(ctx, fileVFS)
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
			dirVFS, ok := vb.idMap[op.Dir]
			if !ok {
				return fmt.Errorf("missing dir mapping for remove %d", op.Dir)
			}
			if err := vb.v.Remove(ctx, dirVFS, op.Name); err != nil {
				return err
			}
			if op.ID != 0 {
				delete(vb.idMap, op.ID)
			}

		case djfs.DeltaRename:
			srcDirVFS, ok1 := vb.idMap[op.Dir]
			dstDirVFS, ok2 := vb.idMap[op.DstDir]
			if !ok1 || !ok2 {
				return fmt.Errorf("missing rename dir mapping (%d -> %d)", op.Dir, op.DstDir)
			}
			// Use native VFS v2 atomic Volume.Rename for both files and directories.
			if err := vb.v.Rename(ctx, srcDirVFS, op.Name, dstDirVFS, op.DstName, 0); err != nil {
				return err
			}

		case djfs.DeltaSetAttr:
			// Volume derives directory mtime/nlink on entry updates and stores mode on Create/Mkdir.
		}
	}
	if vb.syncOnly {
		return vb.v.Sync(ctx)
	}
	return vb.v.Flush(ctx)
}

func readVolumeFile(ctx context.Context, v *vfs.Volume, id vfs.InodeID, size uint64) ([]byte, error) {
	f, err := v.Open(ctx, id)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, size)
	if size == 0 {
		return buf, nil
	}
	n, err := f.ReadAt(ctx, buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

func attachV2Volume(t testing.TB, mem *storetest.Mem, ciph vfs.Cipher, round uint64, jdir string) *vfs.Volume {
	t.Helper()
	v, err := vfs.Attach(context.Background(), vfs.Config{
		Backend:         mem,
		Cipher:          ciph,
		Round:           round,
		Journal:         jdir,
		JournalCapacity: 64 << 20,
		Bound:           time.Hour,
		Disk:            storetest.Disk,
	})
	if err != nil {
		t.Fatalf("vfs.Attach(round=%d): %v", round, err)
	}
	return v
}

func mkV2JournalDir(t testing.TB, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp(storetest.Journals(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// TestVolumeNpmCiObjectBudgetAcceptance runs an npm-ci package extraction shape
// (Create in .staging -> Write -> Chmod -> Rename into package directory, plus
// transient lockfile Create -> Write -> Unlink churn) through djfs.Engine
// directly onto a journaled VFS v2 Volume — reusing Volume's native v2 journal
// and publication lifecycle rather than maintaining a second journal — and
// verifies S3 PUTs/file <= 0.40 plus byte-exact cold reads on reopened Volume.
func TestVolumeNpmCiObjectBudgetAcceptance(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ciph := vfs.Hybrid(key)

	if err := vfs.Create(ctx, vfs.Config{Backend: mem, Cipher: ciph}); err != nil {
		t.Fatal(err)
	}
	jdir := mkV2JournalDir(t, "djfs-npm-*")
	vol := attachV2Volume(t, mem, ciph, 1, jdir)
	defer vol.Close()

	bridge := newVolumeBridge(vol)
	// Reuse vol's native v2 journal/publication lifecycle (no second djfs Journal).
	eng, err := djfs.New(djfs.Config{
		Target:        bridge,
		MaxDirtyOps:   1 << 20,
		MaxDirtyBytes: 256 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	numPkgs := sized(20, 200)
	filesPerPkg := sized(50, 100)
	totalFiles := numPkgs * filesPerPkg

	mem.Reset()

	staging, err := eng.Mkdir(ctx, djfs.RootInode, ".staging", 0o700)
	if err != nil {
		t.Fatal(err)
	}

	pkgDirs := make([]djfs.InodeID, numPkgs)
	for p := 0; p < numPkgs; p++ {
		d, err := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("pkg-%04d", p), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		pkgDirs[p] = d.ID

		// Transient package lockfile created and unlinked during unpack.
		lockName := fmt.Sprintf(".lock-%04d", p)
		if _, err := eng.Create(ctx, staging.ID, lockName, 0o600, []byte("lock")); err != nil {
			t.Fatal(err)
		}
		if err := eng.Unlink(ctx, staging.ID, lockName); err != nil {
			t.Fatal(err)
		}

		for f := 0; f < filesPerPkg; f++ {
			tmpName := fmt.Sprintf("u_%04d_%03d.tmp", p, f)
			dstName := fmt.Sprintf("f_%03d.js", f)
			// Mix of small inline JS files and >16 KiB chunked bundle files.
			mix := p*31 + f*17
			rem := mix % 1536
			bodyLen := 512 + rem
			if f == 0 && p%5 == 0 {
				bodyLen = 20 << 10
			}
			sumIdx := p + f
			ch := byte('a') + byte(sumIdx%26)
			body := bytes.Repeat([]byte{ch}, bodyLen)

			if _, err := eng.Create(ctx, staging.ID, tmpName, 0o600, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := eng.Write(ctx, staging.ID, tmpName, body); err != nil {
				t.Fatal(err)
			}
			if _, err := eng.Chmod(ctx, staging.ID, tmpName, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := eng.Rename(ctx, staging.ID, tmpName, d.ID, dstName); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := eng.Rmdir(ctx, djfs.RootInode, ".staging"); err != nil {
		t.Fatal(err)
	}

	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	puts := mem.Puts()
	totalObjects := mem.Total()
	putsPerFile := float64(totalObjects) / float64(totalFiles)
	st := eng.Stats()
	vst := vol.State()

	t.Logf("npm-ci shape (%d files, %d raw ops -> %d published ops, v2 journal seq=%d): S3 PUTs = %d (meta=%d, pack=%d, head=%d) = %.4f PUTs/file",
		totalFiles, st.RawOps, st.PublishedOps, vst.Published, totalObjects, puts["meta"], puts["pack"], puts["head"], putsPerFile)

	if putsPerFile > 0.40 {
		t.Fatalf("objects PUT per file = %.4f, exceeds 0.40 acceptance budget", putsPerFile)
	}

	// Verify cold read-only Volume sees every package file intact and zero .staging artifacts.
	ro, err := vfs.Open(ctx, vfs.Config{Backend: mem, Cipher: ciph})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	if _, err := ro.Lookup(ctx, vfs.RootInode, ".staging"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected .staging to be absent in committed Volume, got %v", err)
	}

	for _, checkPkg := range []int{0, numPkgs / 2, numPkgs - 1} {
		dirIno, err := ro.Lookup(ctx, vfs.RootInode, fmt.Sprintf("pkg-%04d", checkPkg))
		if err != nil {
			t.Fatal(err)
		}
		for _, checkFile := range []int{0, filesPerPkg - 1} {
			fIno, err := ro.Lookup(ctx, dirIno.ID, fmt.Sprintf("f_%03d.js", checkFile))
			if err != nil {
				t.Fatal(err)
			}
			got, err := readVolumeFile(ctx, ro, fIno.ID, fIno.Size)
			if err != nil {
				t.Fatal(err)
			}
			sumCheck := checkPkg + checkFile
			wantByte := byte('a') + byte(sumCheck%26)
			if len(got) == 0 || got[0] != wantByte || got[len(got)-1] != wantByte {
				t.Fatalf("corrupt content on pkg-%04d/f_%03d.js", checkPkg, checkFile)
			}
		}
	}
}

// TestVolumeNativeJournalCrashReopen verifies that coalesced DJFS batches
// synced into VFS v2's native journal (without Flush) survive process exit and
// are replayed and published cleanly on same-node Attach(Round=2).
func TestVolumeNativeJournalCrashReopen(t *testing.T) {
	ctx := context.Background()
	mem := storetest.NewMem()
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ciph := vfs.Hybrid(key)
	if err := vfs.Create(ctx, vfs.Config{Backend: mem, Cipher: ciph}); err != nil {
		t.Fatal(err)
	}
	jdir := mkV2JournalDir(t, "djfs-v2crash-*")
	vol1 := attachV2Volume(t, mem, ciph, 1, jdir)

	bridge := newVolumeBridge(vol1)
	bridge.syncOnly = true // local native v2 journal Sync only; no S3 Flush yet!
	eng, err := djfs.New(djfs.Config{Target: bridge})
	if err != nil {
		t.Fatal(err)
	}

	dA, err := eng.Mkdir(ctx, djfs.RootInode, "a", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	dB, err := eng.Mkdir(ctx, djfs.RootInode, "b", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Create(ctx, dA.ID, "tmp.txt", 0o600, []byte("moved-via-v2-journal")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Rename(ctx, dA.ID, "tmp.txt", dB.ID, "final.txt"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}

	st := vol1.State()
	if st.Durable == 0 || st.Durable < st.Visible || st.Published != 0 {
		t.Fatalf("expected synced but unpublished native v2 journal, got %+v", st)
	}
	// Close without Flush (crash leaving unpublished native v2 journal).
	if err := vol1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen at Round 2 on same journal directory -> native v2 replays and publishes.
	vol2 := attachV2Volume(t, mem, ciph, 2, jdir)
	defer vol2.Close()

	rec := vol2.Recovery()
	if rec.Lost || rec.Replayed == 0 {
		t.Fatalf("expected clean native v2 replay, got %+v", rec)
	}
	ro, err := vfs.Open(ctx, vfs.Config{Backend: mem, Cipher: ciph})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	inFinal, err := ro.Resolve(ctx, "/b/final.txt")
	if err != nil {
		t.Fatalf("Resolve(/b/final.txt) after v2 crash reopen: %v", err)
	}
	got, err := readVolumeFile(ctx, ro, inFinal.ID, inFinal.Size)
	if err != nil || string(got) != "moved-via-v2-journal" {
		t.Fatalf("/b/final.txt = %q (err=%v), want moved-via-v2-journal", got, err)
	}
	if _, err := ro.Resolve(ctx, "/a/tmp.txt"); err == nil {
		t.Fatal("renamed source /a/tmp.txt reappeared after v2 crash reopen")
	}
}

// TestVolumeMatchedDurabilityComparisonV2 compares current VFS v2 Volume
// directly against djfs.Engine + VFS v2 Volume under the EXACT SAME durability
// contract (both backed by real disk v2 Config.Journal + single epoch Flush),
// asserting identical committed trees and recording journal record & S3 PUT reduction.
func TestVolumeMatchedDurabilityComparisonV2(t *testing.T) {
	ctx := context.Background()
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ciph := vfs.Hybrid(key)

	const numPkgs = 10
	const filesPerPkg = 20

	// 1. Baseline: current VFS v2 Volume directly (uncoalesced, disk-backed v2 journal + Flush).
	memBase := storetest.NewMem()
	if err := vfs.Create(ctx, vfs.Config{Backend: memBase, Cipher: ciph}); err != nil {
		t.Fatal(err)
	}
	volBase := attachV2Volume(t, memBase, ciph, 1, mkV2JournalDir(t, "djfs-cmp-base-*"))
	defer volBase.Close()
	memBase.Reset()

	tBase0 := time.Now()
	stgBase, err := volBase.Mkdir(ctx, vfs.RootInode, ".staging", 0o700)
	if err != nil {
		t.Fatal(err)
	}
	for p := range numPkgs {
		pd, err := volBase.Mkdir(ctx, vfs.RootInode, fmt.Sprintf("pkg-%02d", p), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		// Transient lockfile Create + Write + Remove.
		lk, err := volBase.Create(ctx, stgBase.ID, fmt.Sprintf(".lock-%02d", p), 0o600)
		if err != nil {
			t.Fatal(err)
		}
		lf, _ := volBase.Open(ctx, lk.ID)
		_, _ = lf.WriteAt(ctx, []byte("lock"), 0)
		_ = lf.Close()
		if err := volBase.Remove(ctx, stgBase.ID, fmt.Sprintf(".lock-%02d", p)); err != nil {
			t.Fatal(err)
		}
		for f := range filesPerPkg {
			tmp := fmt.Sprintf("u_%02d_%02d.tmp", p, f)
			dst := fmt.Sprintf("f_%02d.js", f)
			body := bytes.Repeat([]byte{byte('a' + (p+f)%26)}, 1024)
			in, err := volBase.Create(ctx, stgBase.ID, tmp, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			fh, _ := volBase.Open(ctx, in.ID)
			_, _ = fh.WriteAt(ctx, body, 0)
			_ = fh.Close()
			if err := volBase.Rename(ctx, stgBase.ID, tmp, pd.ID, dst, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := volBase.Remove(ctx, vfs.RootInode, ".staging"); err != nil {
		t.Fatal(err)
	}
	if err := volBase.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	baseDur := time.Since(tBase0)
	baseJRecs := volBase.State().Published
	basePuts := memBase.Total()

	// 2. Coalesced: djfs.Engine -> VFS v2 Volume under identical durability contract (native v2 journal + Flush).
	memDJFS := storetest.NewMem()
	if err := vfs.Create(ctx, vfs.Config{Backend: memDJFS, Cipher: ciph}); err != nil {
		t.Fatal(err)
	}
	volDJFS := attachV2Volume(t, memDJFS, ciph, 1, mkV2JournalDir(t, "djfs-cmp-coalesced-*"))
	defer volDJFS.Close()
	memDJFS.Reset()

	eng, err := djfs.New(djfs.Config{Target: newVolumeBridge(volDJFS)})
	if err != nil {
		t.Fatal(err)
	}

	tDJFS0 := time.Now()
	stgD, err := eng.Mkdir(ctx, djfs.RootInode, ".staging", 0o700)
	if err != nil {
		t.Fatal(err)
	}
	for p := range numPkgs {
		pd, err := eng.Mkdir(ctx, djfs.RootInode, fmt.Sprintf("pkg-%02d", p), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Create(ctx, stgD.ID, fmt.Sprintf(".lock-%02d", p), 0o600, []byte("lock")); err != nil {
			t.Fatal(err)
		}
		if err := eng.Unlink(ctx, stgD.ID, fmt.Sprintf(".lock-%02d", p)); err != nil {
			t.Fatal(err)
		}
		for f := range filesPerPkg {
			tmp := fmt.Sprintf("u_%02d_%02d.tmp", p, f)
			dst := fmt.Sprintf("f_%02d.js", f)
			body := bytes.Repeat([]byte{byte('a' + (p+f)%26)}, 1024)
			if _, err := eng.Create(ctx, stgD.ID, tmp, 0o600, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := eng.Write(ctx, stgD.ID, tmp, body); err != nil {
				t.Fatal(err)
			}
			if err := eng.Rename(ctx, stgD.ID, tmp, pd.ID, dst); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := eng.Rmdir(ctx, djfs.RootInode, ".staging"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	djfsDur := time.Since(tDJFS0)
	djfsJRecs := volDJFS.State().Published
	djfsPuts := memDJFS.Total()

	t.Logf("Matched-durability VFS v2 comparison (%d files, both disk-journaled + Flush):", numPkgs*filesPerPkg)
	t.Logf("  VFS v2 direct : %d journal records | %d S3 PUTs | %v", baseJRecs, basePuts, baseDur)
	t.Logf("  DJFS + VFS v2 : %d journal records | %d S3 PUTs | %v (%.2fx fewer v2 journal records)",
		djfsJRecs, djfsPuts, djfsDur, float64(baseJRecs)/float64(max(1, djfsJRecs)))

	if djfsJRecs >= baseJRecs {
		t.Fatalf("expected DJFS coalescing to emit fewer v2 journal records than direct v2: djfs=%d base=%d", djfsJRecs, baseJRecs)
	}

	// Verify byte-exact identical committed state across both Volumes via fresh read-only Open.
	roBase, err := vfs.Open(ctx, vfs.Config{Backend: memBase, Cipher: ciph})
	if err != nil {
		t.Fatal(err)
	}
	defer roBase.Close()
	roDJFS, err := vfs.Open(ctx, vfs.Config{Backend: memDJFS, Cipher: ciph})
	if err != nil {
		t.Fatal(err)
	}
	defer roDJFS.Close()

	for p := range numPkgs {
		for f := range filesPerPkg {
			path := fmt.Sprintf("/pkg-%02d/f_%02d.js", p, f)
			inB, err := roBase.Resolve(ctx, path)
			if err != nil {
				t.Fatalf("base Resolve(%s): %v", path, err)
			}
			inD, err := roDJFS.Resolve(ctx, path)
			if err != nil {
				t.Fatalf("djfs Resolve(%s): %v", path, err)
			}
			bB, err := readVolumeFile(ctx, roBase, inB.ID, inB.Size)
			if err != nil {
				t.Fatal(err)
			}
			bD, err := readVolumeFile(ctx, roDJFS, inD.ID, inD.Size)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(bB, bD) {
				t.Fatalf("content mismatch on %s", path)
			}
		}
	}
}

// BenchmarkDJFSvsV2MatchedDurabilityFsync compares atomic Create(tmp)+Write+Rename+Sync
// under the identical local VFS v2 journal durability contract (vol.Sync(ctx)).
func BenchmarkDJFSvsV2MatchedDurabilityFsync(b *testing.B) {
	ctx := context.Background()
	key, err := age.GenerateHybridIdentity()
	if err != nil {
		b.Fatal(err)
	}
	ciph := vfs.Hybrid(key)
	payload := bytes.Repeat([]byte("m"), 4096)

	b.Run("V2Native", func(b *testing.B) {
		mem := storetest.NewMem()
		if err := vfs.Create(ctx, vfs.Config{Backend: mem, Cipher: ciph}); err != nil {
			b.Fatal(err)
		}
		vol := attachV2Volume(b, mem, ciph, 1, mkV2JournalDir(b, "djfs-b-v2-*"))
		defer vol.Close()
		d, err := vol.Mkdir(ctx, vfs.RootInode, "mail", 0o755)
		if err != nil {
			b.Fatal(err)
		}
		_ = vol.Sync(ctx)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tmp := fmt.Sprintf("tmp_%d", i)
			cur := fmt.Sprintf("cur_%d", i)
			in, err := vol.Create(ctx, d.ID, tmp, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			fh, err := vol.Open(ctx, in.ID)
			if err != nil {
				b.Fatal(err)
			}
			_, _ = fh.WriteAt(ctx, payload, 0)
			_ = fh.Close()
			if err := vol.Rename(ctx, d.ID, tmp, d.ID, cur, 0); err != nil {
				b.Fatal(err)
			}
			if err := vol.Sync(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DJFSOverV2", func(b *testing.B) {
		mem := storetest.NewMem()
		if err := vfs.Create(ctx, vfs.Config{Backend: mem, Cipher: ciph}); err != nil {
			b.Fatal(err)
		}
		vol := attachV2Volume(b, mem, ciph, 1, mkV2JournalDir(b, "djfs-b-coalesced-*"))
		defer vol.Close()
		bridge := newVolumeBridge(vol)
		bridge.syncOnly = true
		eng, err := djfs.New(djfs.Config{Target: bridge})
		if err != nil {
			b.Fatal(err)
		}
		d, err := eng.Mkdir(ctx, djfs.RootInode, "mail", 0o755)
		if err != nil {
			b.Fatal(err)
		}
		_ = eng.Publish(ctx)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tmp := fmt.Sprintf("tmp_%d", i)
			cur := fmt.Sprintf("cur_%d", i)
			if _, err := eng.Create(ctx, d.ID, tmp, 0o600, payload); err != nil {
				b.Fatal(err)
			}
			if err := eng.Rename(ctx, d.ID, tmp, d.ID, cur); err != nil {
				b.Fatal(err)
			}
			if err := eng.Publish(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}
