// Package file is a filesystem-backed Backend for local dev. NOT
// suitable for production — no fsync barrier discipline, no checksum.
package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hanzoai/vfs/pkg/backend"
)

func init() {
	backend.Register("file", openFile)
}

type fileBackend struct {
	root string

	closeOnce sync.Once
	closed    bool
	mu        sync.Mutex
}

func openFile(_ context.Context, u *url.URL) (backend.Backend, error) {
	// file:///abs/path  → u.Path = "/abs/path"
	// file://./relpath  → u.Host = ".", u.Path = "/relpath"
	root := u.Path
	if u.Host != "" && u.Host != "localhost" {
		root = filepath.Join(u.Host, u.Path)
	}
	if root == "" {
		return nil, fmt.Errorf("file backend: missing path in %q", u.String())
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("file backend: mkdir %q: %w", root, err)
	}
	return &fileBackend{root: root}, nil
}

func (f *fileBackend) keyPath(key string) string {
	// Disallow "..". Backend keys are simple `/`-delimited names; never user paths.
	if strings.Contains(key, "..") {
		return ""
	}
	return filepath.Join(f.root, filepath.FromSlash(key))
}

func (f *fileBackend) Get(_ context.Context, key string) ([]byte, error) {
	p := f.keyPath(key)
	if p == "" {
		return nil, fmt.Errorf("file backend: invalid key %q", key)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return nil, err
	}
	return b, nil
}

func (f *fileBackend) Put(_ context.Context, key string, data []byte) error {
	p := f.keyPath(key)
	if p == "" {
		return fmt.Errorf("file backend: invalid key %q", key)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	// Atomic write: temp file + rename.
	tmp, err := os.CreateTemp(filepath.Dir(p), ".vfs-"+filepath.Base(p)+"-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(tmp.Name())
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), p)
}

func (f *fileBackend) Delete(_ context.Context, key string) error {
	p := f.keyPath(key)
	if p == "" {
		return fmt.Errorf("file backend: invalid key %q", key)
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (f *fileBackend) List(ctx context.Context, prefix string) (<-chan string, <-chan error) {
	keys := make(chan string, 64)
	errCh := make(chan error, 1)
	go func() {
		defer close(keys)
		defer close(errCh)
		base := f.root
		walkRoot := base
		if prefix != "" {
			walkRoot = filepath.Join(base, filepath.FromSlash(prefix))
		}
		err := filepath.WalkDir(walkRoot, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil // empty prefix
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			rel, rerr := filepath.Rel(base, path)
			if rerr != nil {
				return rerr
			}
			rel = filepath.ToSlash(rel)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case keys <- rel:
			}
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	return keys, errCh
}

func (f *fileBackend) Stat(_ context.Context, key string) (int64, error) {
	p := f.keyPath(key)
	if p == "" {
		return 0, fmt.Errorf("file backend: invalid key %q", key)
	}
	st, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return 0, err
	}
	return st.Size(), nil
}

func (f *fileBackend) Close() error {
	f.closeOnce.Do(func() { f.closed = true })
	return nil
}

func (f *fileBackend) String() string { return "file://" + f.root }

// Compile-time interface check.
var _ backend.Backend = (*fileBackend)(nil)

// Unused but reserved for streaming when we need it.
type _ = io.Reader
