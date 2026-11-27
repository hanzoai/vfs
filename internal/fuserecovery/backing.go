package fuserecovery

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const orphanDirName = ".ananke-orphans"

// BackingFile is an open handle to a durable backing storage object.
type BackingFile interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Truncate(size int64) error
	Size() (int64, error)
	Sync() error
	Close() error
}

// BackingStore abstracts the underlying durable file storage used by the FUSE
// daemon (real OS filesystem or a canonical Hanzo *vfs.Volume adapter).
type BackingStore interface {
	CreateFile(relPath string, mode uint32) error
	OpenFile(relPath string, readWrite bool) (BackingFile, error)
	StatSize(relPath string) (uint64, bool, error)
	ReadAll(relPath string) ([]byte, error)
	UnlinkFile(relPath string) error
	StashOrphan(relPath string, inode, gen uint64) (orphanRelPath string, err error)
	CleanupOrphan(orphanRelPath string) error
}

type osBackingFile struct {
	f *os.File
}

func (o *osBackingFile) ReadAt(p []byte, off int64) (int, error)  { return o.f.ReadAt(p, off) }
func (o *osBackingFile) WriteAt(p []byte, off int64) (int, error) { return o.f.WriteAt(p, off) }
func (o *osBackingFile) Truncate(size int64) error                { return o.f.Truncate(size) }
func (o *osBackingFile) Sync() error                              { return o.f.Sync() }
func (o *osBackingFile) Close() error                             { return o.f.Close() }
func (o *osBackingFile) Size() (int64, error) {
	info, err := o.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// OSBackingStore stores files in a symlink-confined OS directory with a
// dedicated .ananke-orphans/ stash so unlinked-but-open files remain durable
// and accessible across daemon restarts until their last handle closes.
type OSBackingStore struct {
	root string
}

// NewOSBackingStore validates root confinement and ensures the orphan stash
// subdirectory exists.
func NewOSBackingStore(root string) (*OSBackingStore, error) {
	if err := ValidateStateDir(root); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("abs backing root %q: %w", root, err)
	}
	orphanDir, err := ConfinePath(abs, orphanDirName)
	if err != nil {
		return nil, err
	}
	if err := ValidateStateDir(orphanDir); err != nil {
		return nil, err
	}
	return &OSBackingStore{root: abs}, nil
}

// Root returns the confined backing directory path.
func (s *OSBackingStore) Root() string { return s.root }

func (s *OSBackingStore) CreateFile(relPath string, mode uint32) error {
	full, err := ConfinePath(s.root, relPath)
	if err != nil {
		return err
	}
	parent := filepath.Dir(full)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("mkdir backing parent: %w", err)
	}
	// Re-check confinement after parent creation.
	if _, err := ConfinePath(s.root, relPath); err != nil {
		return err
	}
	if mode == 0 {
		mode = 0o644
	}
	f, err := os.OpenFile(full, os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(mode)&0o777)
	if err != nil {
		if os.IsExist(err) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("create backing file %q: %w", relPath, err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return fmt.Errorf("sync created file %q: %w", relPath, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close created file %q: %w", relPath, closeErr)
	}
	return SyncDir(parent)
}

func (s *OSBackingStore) OpenFile(relPath string, readWrite bool) (BackingFile, error) {
	full, err := ConfinePath(s.root, relPath)
	if err != nil {
		return nil, err
	}
	flags := os.O_RDONLY
	if readWrite {
		flags = os.O_RDWR
	}
	f, err := os.OpenFile(full, flags, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("open backing file %q: %w", relPath, err)
	}
	return &osBackingFile{f: f}, nil
}

func (s *OSBackingStore) StatSize(relPath string) (uint64, bool, error) {
	full, err := ConfinePath(s.root, relPath)
	if err != nil {
		return 0, false, err
	}
	info, err := os.Lstat(full)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return 0, false, fmt.Errorf("%w: backing file %q is a symlink", ErrPathEscape, relPath)
	}
	return uint64(info.Size()), true, nil
}

func (s *OSBackingStore) ReadAll(relPath string) ([]byte, error) {
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
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

func (s *OSBackingStore) UnlinkFile(relPath string) error {
	full, err := ConfinePath(s.root, relPath)
	if err != nil {
		return err
	}
	parent := filepath.Dir(full)
	if err := os.Remove(full); err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return fmt.Errorf("unlink backing file %q: %w", relPath, err)
	}
	return SyncDir(parent)
}

// StashOrphan atomically relocates an unlinked-but-open file into the confined
// .ananke-orphans/ directory so open handles continue to survive daemon crashes.
func (s *OSBackingStore) StashOrphan(relPath string, inode, gen uint64) (string, error) {
	src, err := ConfinePath(s.root, relPath)
	if err != nil {
		return "", err
	}
	orphanRel := filepath.Join(orphanDirName, fmt.Sprintf("ino-%d-gen-%d.orphan", inode, gen))
	dst, err := ConfinePath(s.root, orphanRel)
	if err != nil {
		return "", err
	}
	if err := os.Rename(src, dst); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("stash orphan %q -> %q: %w", relPath, orphanRel, err)
	}
	if err := SyncDir(filepath.Dir(dst)); err != nil {
		return "", err
	}
	if err := SyncDir(filepath.Dir(src)); err != nil {
		return "", err
	}
	return orphanRel, nil
}

// CleanupOrphan deletes the stashed orphan backing file after its final open
// handle is released.
func (s *OSBackingStore) CleanupOrphan(orphanRelPath string) error {
	full, err := ConfinePath(s.root, orphanRelPath)
	if err != nil {
		return err
	}
	if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cleanup orphan %q: %w", orphanRelPath, err)
	}
	return SyncDir(filepath.Dir(full))
}
