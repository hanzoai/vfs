// Package file is a filesystem-backed Backend for local dev and tests. NOT
// suitable for production: writes are atomic (temp file + rename) but not
// durable against power loss.
//
// A key is a path under the directory. Every key of the grammar of backend.ValidKey (lower
// case letters, digits and "-", segments of at most 255 bytes) is kept as given and listed
// by its string prefix. A key outside it is kept if the file system keeps it (the replica
// package names its objects "orgs/acme/kv.db"), and refused with backend.ErrKey if it would
// leave the directory (an empty, "." or ".." segment), contains NUL, or names the store's
// own: ".lock" and ".tmp" at the top of the directory, which are never listed.
//
// Conditional writes are atomic: every mutation runs under one flock on the
// store directory, so a PutIfAbsent or PutIfVersion decides and writes with no
// other writer in between, across goroutines and across processes. A key's
// Version is the blake3 digest of its content, the same property an S3 ETag has.
package file

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/hanzoai/vfs/internal/blake3"
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
	// file://host/path names a host, which this backend does not reach: refused,
	// not read as the directory host/path.
	root := u.Path
	switch u.Host {
	case "", "localhost":
	case ".":
		root = filepath.Join(u.Host, u.Path)
	default:
		return nil, fmt.Errorf("file backend: the URL names the host %q, which this backend does not reach; use file:///dir, or file://./dir for a relative directory", u.Host)
	}
	if root == "" {
		return nil, fmt.Errorf("file backend: missing path in %q", u.String())
	}
	// Opening creates nothing: the directory is made by the first write, so naming a
	// place to read from, or mistyping one, leaves no trace.
	real, err := canonical(root)
	if err != nil {
		return nil, fmt.Errorf("file backend: %q: %w", root, err)
	}
	return &fileBackend{root: real}, nil
}

// canonical returns the name of a directory that every spelling of it shares, for a Head
// is sealed to the place's name: the absolute path with symbolic links resolved. A
// directory that does not exist yet has the name it will have: the resolved name of the
// longest part of it that exists, and the rest as written.
func canonical(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rest := ""
	for cur := abs; ; {
		real, err := filepath.EvalSymlinks(cur)
		if err == nil {
			return filepath.Join(real, rest), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", err
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}

// missing reports that a path names nothing: absent, or below something that is a file.
func missing(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

// safe reports whether key (or, for a listing, a prefix whose last segment may be partial
// or empty) names a path inside the directory that is not the store's own.
func safe(key string, prefix bool) error {
	if key == "" && !prefix || strings.ContainsRune(key, 0) {
		return fmt.Errorf("%w: %q", backend.ErrKey, key)
	}
	segs := strings.Split(key, "/")
	for i, seg := range segs {
		last := i == len(segs)-1
		switch {
		case seg == "" && prefix && last:
		case seg == "", seg == ".", seg == "..":
			return fmt.Errorf("%w: %q has an empty, \".\" or \"..\" segment", backend.ErrKey, key)
		case i == 0 && (seg == ".lock" || seg == ".tmp"):
			return fmt.Errorf("%w: %q is a name of the store's own", backend.ErrKey, key)
		}
	}
	return nil
}

// keyPath is where a key is kept, or "" for a key that is not safe.
func (f *fileBackend) keyPath(key string) string {
	if safe(key, false) != nil {
		return ""
	}
	return filepath.Join(f.root, filepath.FromSlash(key))
}

// invalid is the error for a key keyPath refused, or for a key that a path cannot hold
// beside the others (one that is a directory of another).
func invalid(key string, err error) error {
	if kerr := safe(key, false); kerr != nil {
		return kerr
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return fmt.Errorf("%w: %s has a segment the file system cannot name", backend.ErrKey, key)
	}
	if errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EEXIST) || errors.Is(err, syscall.ENOTEMPTY) {
		return fmt.Errorf("%w: %s is a path prefix of another key, or has one as its prefix", backend.ErrKey, key)
	}
	return err
}

func version(data []byte) backend.Version {
	sum := blake3.Sum(data)
	return backend.Version(hex.EncodeToString(sum[:]))
}

func (f *fileBackend) Get(_ context.Context, key string, max int64) ([]byte, backend.Version, error) {
	p := f.keyPath(key)
	if p == "" {
		return nil, "", invalid(key, nil)
	}
	fd, err := os.Open(p)
	if err != nil {
		if missing(err) {
			return nil, "", fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return nil, "", err
	}
	defer fd.Close()
	if fi, err := fd.Stat(); err == nil && fi.IsDir() {
		return nil, "", fmt.Errorf("%w: %s", backend.ErrNotFound, key) // a directory of other keys
	}
	b, err := io.ReadAll(io.LimitReader(fd, backend.Past(max)))
	if err != nil {
		return nil, "", err
	}
	if int64(len(b)) > max {
		return nil, "", fmt.Errorf("%w: %s is larger than %d bytes", backend.ErrTooLarge, key, max)
	}
	return b, version(b), nil
}

func (f *fileBackend) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	p := f.keyPath(key)
	if p == "" {
		return nil, invalid(key, nil)
	}
	fd, err := os.Open(p)
	if err != nil {
		if missing(err) {
			return nil, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return nil, err
	}
	defer fd.Close()
	if fi, err := fd.Stat(); err == nil && fi.IsDir() {
		return nil, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	buf := make([]byte, n)
	if _, err := fd.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("file backend: range %d+%d of %s: %w", off, n, key, err)
	}
	return buf, nil
}

func (f *fileBackend) Put(_ context.Context, key string, data []byte) error {
	p := f.keyPath(key)
	if p == "" {
		return invalid(key, nil)
	}
	unlock, err := f.lock()
	if err != nil {
		return err
	}
	defer unlock()
	return f.replace(p, data)
}

func (f *fileBackend) PutIfAbsent(_ context.Context, key string, data []byte) (backend.Version, error) {
	p := f.keyPath(key)
	if p == "" {
		return "", invalid(key, nil)
	}
	unlock, err := f.lock()
	if err != nil {
		return "", err
	}
	defer unlock()
	if _, err := os.Stat(p); err == nil {
		return "", fmt.Errorf("%w: %s exists", backend.ErrConflict, key)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := f.replace(p, data); err != nil {
		return "", err
	}
	return version(data), nil
}

func (f *fileBackend) PutIfVersion(_ context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	p := f.keyPath(key)
	if p == "" {
		return "", invalid(key, nil)
	}
	unlock, err := f.lock()
	if err != nil {
		return "", err
	}
	defer unlock()
	cur, err := os.ReadFile(p)
	if err != nil {
		if missing(err) || errors.Is(err, syscall.EISDIR) {
			return "", fmt.Errorf("%w: %s is absent", backend.ErrConflict, key)
		}
		return "", err
	}
	if have := version(cur); have != v {
		return "", fmt.Errorf("%w: %s is at %s, not %s", backend.ErrConflict, key, have, v)
	}
	if err := f.replace(p, data); err != nil {
		return "", err
	}
	return version(data), nil
}

// replace writes data at p atomically: a temp file in the store's own directory, synced,
// then renamed over p, so a reader sees the old bytes or the new, never a mix.
func (f *fileBackend) replace(p string, data []byte) error {
	tmpDir := filepath.Join(f.root, ".tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return invalidPath(p, f.root, err)
	}
	tmp, err := os.CreateTemp(tmpDir, "w-*")
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
	return invalidPath(p, f.root, os.Rename(tmp.Name(), p))
}

// invalidPath names the key behind a path error that means two keys cannot be kept.
func invalidPath(p, root string, err error) error {
	if err == nil {
		return nil
	}
	rel, rerr := filepath.Rel(root, p)
	if rerr != nil {
		return err
	}
	return invalid(filepath.ToSlash(rel), err)
}

func (f *fileBackend) Delete(_ context.Context, key string) error {
	p := f.keyPath(key)
	if p == "" {
		return invalid(key, nil)
	}
	unlock, err := f.lock()
	if err != nil {
		return err
	}
	defer unlock()
	if err := os.Remove(p); err != nil && !missing(err) {
		return err
	}
	return nil
}

// List walks the directory the prefix names up to its last "/" and returns what begins
// with the prefix as a string, so "ext/y/a" lists "ext/y/ab" and "ext/y/a/c" alike.
func (f *fileBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	objs := make(chan backend.Object, 64)
	errCh := make(chan error, 1)
	if err := safe(prefix, true); err != nil {
		errCh <- err
		close(objs)
		close(errCh)
		return objs, errCh
	}
	go func() {
		defer close(objs)
		defer close(errCh)
		walkRoot := f.root
		if i := strings.LastIndex(prefix, "/"); i >= 0 {
			walkRoot = filepath.Join(f.root, filepath.FromSlash(prefix[:i]))
		}
		err := filepath.WalkDir(walkRoot, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil // nothing under the prefix
				}
				return err
			}
			rel, rerr := filepath.Rel(f.root, path)
			if rerr != nil {
				return rerr
			}
			rel = filepath.ToSlash(rel)
			if rel == ".lock" || rel == ".tmp" {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil // the store's own, and no key can be named so
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasPrefix(rel, prefix) {
				return nil
			}
			info, ierr := d.Info()
			if ierr != nil {
				if errors.Is(ierr, os.ErrNotExist) {
					return nil // removed while listing
				}
				return ierr
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case objs <- backend.Object{Key: rel, Size: info.Size(), Modified: info.ModTime()}:
			}
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	return objs, errCh
}

func (f *fileBackend) Stat(_ context.Context, key string) (int64, error) {
	p := f.keyPath(key)
	if p == "" {
		return 0, invalid(key, nil)
	}
	st, err := os.Stat(p)
	if err != nil {
		if missing(err) {
			return 0, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return 0, err
	}
	if st.IsDir() {
		return 0, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
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
