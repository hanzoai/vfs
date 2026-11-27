package fuserecovery_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/fuserecovery"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

// vfsBackingFile adapts a canonical *vfs.File to fuserecovery.BackingFile.
type vfsBackingFile struct {
	ctx context.Context
	vol *vfs.Volume
	ino vfs.InodeID
	f   *vfs.File
}

func (v *vfsBackingFile) ReadAt(p []byte, off int64) (int, error) {
	return v.f.ReadAt(v.ctx, p, off)
}

func (v *vfsBackingFile) WriteAt(p []byte, off int64) (int, error) {
	return v.f.WriteAt(v.ctx, p, off)
}

func (v *vfsBackingFile) Truncate(size int64) error {
	return v.f.Truncate(v.ctx, uint64(size))
}

func (v *vfsBackingFile) Size() (int64, error) {
	in, err := v.f.Stat(v.ctx)
	if err != nil {
		return 0, err
	}
	return int64(in.Size), nil
}

func (v *vfsBackingFile) Sync() error {
	return v.vol.SyncInode(v.ctx, v.ino)
}

func (v *vfsBackingFile) Close() error {
	return v.f.Close()
}

// vfsBackingStore adapts a canonical Hanzo *vfs.Volume to fuserecovery.BackingStore,
// demonstrating how Ananke session/handle recovery integrates directly above vfs.Volume.
type vfsBackingStore struct {
	ctx context.Context
	vol *vfs.Volume
}

func (s *vfsBackingStore) ensureDir(dir string) (vfs.InodeID, error) {
	cur := vfs.RootInode
	if dir == "" || dir == "." || dir == "/" {
		return cur, nil
	}
	for _, seg := range strings.Split(strings.Trim(path.Clean(dir), "/"), "/") {
		if seg == "" || seg == "." {
			continue
		}
		in, err := s.vol.Lookup(s.ctx, cur, seg)
		if errors.Is(err, fs.ErrNotExist) {
			in, err = s.vol.Mkdir(s.ctx, cur, seg, 0o755)
		}
		if err != nil {
			return 0, err
		}
		cur = in.ID
	}
	return cur, nil
}

func (s *vfsBackingStore) lookupPath(relPath string) (vfs.InodeID, string, vfs.Inode, error) {
	clean := strings.TrimPrefix(path.Clean(relPath), "/")
	dir, name := path.Split(clean)
	parent := vfs.RootInode
	if dir != "" && dir != "/" {
		for _, seg := range strings.Split(strings.Trim(dir, "/"), "/") {
			if seg == "" || seg == "." {
				continue
			}
			in, err := s.vol.Lookup(s.ctx, parent, seg)
			if err != nil {
				return 0, "", vfs.Inode{}, err
			}
			parent = in.ID
		}
	}
	in, err := s.vol.Lookup(s.ctx, parent, name)
	return parent, name, in, err
}

func (s *vfsBackingStore) CreateFile(relPath string, mode uint32) error {
	clean := strings.TrimPrefix(path.Clean(relPath), "/")
	dir, name := path.Split(clean)
	parent, err := s.ensureDir(dir)
	if err != nil {
		return err
	}
	if mode == 0 {
		mode = 0o644
	}
	_, err = s.vol.Create(s.ctx, parent, name, fs.FileMode(mode)&0o777)
	if errors.Is(err, fs.ErrExist) {
		return fuserecovery.ErrAlreadyExists
	}
	return err
}

func (s *vfsBackingStore) OpenFile(relPath string, _ bool) (fuserecovery.BackingFile, error) {
	_, _, in, err := s.lookupPath(relPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fuserecovery.ErrNotFound
		}
		return nil, err
	}
	f, err := s.vol.Open(s.ctx, in.ID)
	if err != nil {
		return nil, err
	}
	return &vfsBackingFile{ctx: s.ctx, vol: s.vol, ino: in.ID, f: f}, nil
}

func (s *vfsBackingStore) StatSize(relPath string) (uint64, bool, error) {
	_, _, in, err := s.lookupPath(relPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return in.Size, true, nil
}

func (s *vfsBackingStore) ReadAll(relPath string) ([]byte, error) {
	bf, err := s.OpenFile(relPath, false)
	if err != nil {
		return nil, err
	}
	defer bf.Close()
	sz, err := bf.Size()
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	n, err := bf.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

func (s *vfsBackingStore) UnlinkFile(relPath string) error {
	parent, name, _, err := s.lookupPath(relPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fuserecovery.ErrNotFound
		}
		return err
	}
	return s.vol.Remove(s.ctx, parent, name)
}

func (s *vfsBackingStore) StashOrphan(relPath string, inode, gen uint64) (string, error) {
	srcParent, srcName, _, err := s.lookupPath(relPath)
	if err != nil {
		return "", err
	}
	orphanParent, err := s.ensureDir(".ananke-orphans")
	if err != nil {
		return "", err
	}
	orphanName := fmt.Sprintf("ino-%d-gen-%d.orphan", inode, gen)
	if err := s.vol.Rename(s.ctx, srcParent, srcName, orphanParent, orphanName, 0); err != nil {
		return "", err
	}
	return path.Join(".ananke-orphans", orphanName), nil
}

func (s *vfsBackingStore) CleanupOrphan(orphanRelPath string) error {
	return s.UnlinkFile(orphanRelPath)
}

func TestCanonicalVFSVolumeTransparentRecovery(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	beDir := filepath.Join(tmp, "be")
	journalDir := filepath.Join(tmp, "journal")
	stateDir := filepath.Join(tmp, "ananke-state")
	if err := os.MkdirAll(beDir, 0o700); err != nil {
		t.Fatal(err)
	}

	be, err := backend.Open(ctx, "file://"+beDir)
	if err != nil {
		t.Fatalf("backend.Open: %v", err)
	}
	defer be.Close()

	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("GenerateHybridIdentity: %v", err)
	}
	cipher := vfs.Hybrid(id)
	if err := vfs.Create(ctx, vfs.Config{Backend: be, Cipher: cipher}); err != nil {
		t.Fatalf("vfs.Create: %v", err)
	}

	// Generation 1 Volume attached at Round 1 with local journal.
	vol1, err := vfs.Attach(ctx, vfs.Config{
		Backend:         be,
		Cipher:          cipher,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: 16 << 20,
		Disk:            storetest.Disk,
	})
	if err != nil {
		t.Fatalf("vfs.Attach round 1: %v", err)
	}

	channel := fuserecovery.NewSupervisorChannel("hanzo-vfs-mount-1", "/mnt/hanzo-agent")
	store1 := &vfsBackingStore{ctx: ctx, vol: vol1}
	sup, err := fuserecovery.NewSupervisor(fuserecovery.SupervisorConfig{
		SessionID:      "hanzo-vfs-mount-1",
		MountPath:      "/mnt/hanzo-agent",
		StateDir:       stateDir,
		Backing:        store1,
		Channel:        channel,
		CheckpointMode: fuserecovery.CheckpointSync,
	})
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}

	d1, err := sup.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Open regular file + unlinked-open orphan on canonical Hanzo vfs.Volume.
	_, hReg, err := d1.Create(1, "agent-1", "workspace/checkpoint.dat", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		t.Fatalf("Create regular: %v", err)
	}
	wantReg := []byte("hanzo-vfs-sealed-durable-bytes")
	if _, _, err := d1.Write(2, "agent-1", hReg.Handle, 0, wantReg); err != nil {
		t.Fatal(err)
	}
	if _, err := d1.Fsync(3, "agent-1", hReg.Handle); err != nil {
		t.Fatalf("Fsync regular over vfs.Volume: %v", err)
	}

	_, hOrph, err := d1.Create(4, "agent-1", "workspace/scratch.tmp", 0o644, uint32(os.O_RDWR|os.O_CREATE))
	if err != nil {
		t.Fatal(err)
	}
	wantOrph := []byte("orphan-survives-vfs-daemon-sigkill")
	if _, _, err := d1.Write(5, "agent-1", hOrph.Handle, 0, wantOrph); err != nil {
		t.Fatal(err)
	}
	if _, err := d1.Fsync(6, "agent-1", hOrph.Handle); err != nil {
		t.Fatal(err)
	}
	if err := d1.Unlink(7, "agent-1", "workspace/scratch.tmp"); err != nil {
		t.Fatalf("Unlink orphan on vfs.Volume: %v", err)
	}

	// Abruptly crash daemon + close vol1 (simulating daemon process crash &
	// re-attach of vfs.Volume at Round 2 replaying local journal).
	sup.InjectProcessCrash()
	_ = vol1.Close()

	vol2, err := vfs.Attach(ctx, vfs.Config{
		Backend:         be,
		Cipher:          cipher,
		Round:           2,
		Journal:         journalDir,
		JournalCapacity: 16 << 20,
		Disk:            storetest.Disk,
	})
	if err != nil {
		t.Fatalf("vfs.Attach round 2: %v", err)
	}
	defer vol2.Close()

	// Recover daemon over the re-attached canonical vfs.Volume using the SAME
	// supervisor-held FuseChannel (0 unmounts!).
	store2 := &vfsBackingStore{ctx: ctx, vol: vol2}
	stStore, err := fuserecovery.NewStateStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	d2, rep, err := fuserecovery.RecoverDaemon(fuserecovery.DaemonConfig{
		SessionID:      channel.SessionID(),
		MountPath:      channel.MountPath(),
		StateStore:     stStore,
		Backing:        store2,
		Channel:        channel,
		CheckpointMode: fuserecovery.CheckpointSync,
	})
	if err != nil {
		t.Fatalf("RecoverDaemon over vfs.Volume: %v", err)
	}
	defer d2.Close()

	if channel.UnmountCount() != 0 {
		t.Fatalf("expected 0 unmounts, got %d", channel.UnmountCount())
	}
	if rep.HandlesRestored != 2 || rep.OrphansRestored != 1 || rep.BarriersVerified != 2 {
		t.Fatalf("unexpected vfs recovery report: %+v", rep)
	}

	gotReg, err := d2.Read("agent-1", hReg.Handle, 0, 128)
	if err != nil || !bytes.Equal(gotReg, wantReg) {
		t.Fatalf("regular handle after vfs crash = %q, err=%v", gotReg, err)
	}
	gotOrph, err := d2.Read("agent-1", hOrph.Handle, 0, 128)
	if err != nil || !bytes.Equal(gotOrph, wantOrph) {
		t.Fatalf("orphan handle after vfs crash = %q, err=%v", gotOrph, err)
	}
	if err := d2.Release("agent-1", hOrph.Handle); err != nil {
		t.Fatalf("release orphan: %v", err)
	}
	if err := d2.Release("agent-1", hReg.Handle); err != nil {
		t.Fatalf("release regular: %v", err)
	}
}
