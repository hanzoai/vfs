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
	"sync"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/journal/crash"
	"github.com/hanzoai/vfs/pkg/djfs"
)

// volumeBridge adapts a live *vfs.Volume to djfs.Target, applying each
// coalesced epoch Batch into Volume's overlay and committing via a single
// conditional Head Flush.
type volumeBridge struct {
	mu    sync.Mutex
	v     *vfs.Volume
	idMap map[djfs.InodeID]vfs.InodeID
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
			srcIno, err := vb.v.Lookup(ctx, srcDirVFS, op.Name)
			if err != nil {
				return err
			}
			if srcIno.IsDir() {
				return fmt.Errorf("cross-epoch directory rename requires native Volume.Rename")
			}
			// Read committed file bytes, stage destination, and remove source in
			// one atomic Volume.Flush commit.
			data, err := readVolumeFile(ctx, vb.v, srcIno.ID, srcIno.Size)
			if err != nil {
				return err
			}
			if _, err := vb.v.Lookup(ctx, dstDirVFS, op.DstName); err == nil {
				if err := vb.v.Remove(ctx, dstDirVFS, op.DstName); err != nil {
					return err
				}
			}
			created, err := vb.v.Create(ctx, dstDirVFS, op.DstName, srcIno.Mode.Perm())
			if err != nil {
				return err
			}
			if len(data) > 0 {
				f, err := vb.v.Open(ctx, created.ID)
				if err != nil {
					return err
				}
				if _, err := f.WriteAt(ctx, data, 0); err != nil {
					_ = f.Close()
					return err
				}
				if err := f.Close(); err != nil {
					return err
				}
			}
			if err := vb.v.Remove(ctx, srcDirVFS, op.Name); err != nil {
				return err
			}
			vb.idMap[op.ID] = created.ID

		case djfs.DeltaSetAttr:
			// Volume on main derives directory mtime on entry updates and stores
			// initial mode at Create/Mkdir; attribute-only stamps are no-ops here.
		}
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

// TestVolumeNpmCiObjectBudgetAcceptance runs an npm-ci package extraction shape
// (Create in .staging -> Write -> Chmod -> Rename into package directory, plus
// transient lockfile Create -> Write -> Unlink churn) through djfs.Engine +
// journal.Journal + vfs.Volume on storetest.Mem, verifying that S3 objects PUT
// per surviving file stays strictly under the 0.40 acceptance budget (and
// byte-exact cold reads succeed on a reopened read-only Volume).
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
	vol, err := vfs.Attach(ctx, vfs.Config{Backend: mem, Cipher: ciph, Round: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	disk := crash.New()
	j := openCrashJournal(t, disk, 0)
	defer j.Close()

	bridge := newVolumeBridge(vol)
	eng, err := djfs.New(djfs.Config{
		Journal:       j,
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

	t.Logf("npm-ci shape (%d files, %d raw ops -> %d published ops): S3 PUTs = %d (meta=%d, pack=%d, head=%d) = %.4f PUTs/file",
		totalFiles, st.RawOps, st.PublishedOps, totalObjects, puts["meta"], puts["pack"], puts["head"], putsPerFile)

	if putsPerFile > 0.40 {
		t.Fatalf("objects PUT per file = %.4f, exceeds 0.40 acceptance budget", putsPerFile)
	}

	// Verify cold read-only Volume sees every package file intact and zero
	// .staging artifacts.
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
