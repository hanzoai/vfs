// Package backend defines the object-store interface that VFS writes
// encrypted blocks against. Implementations cover S3, GCS, Azure Blob,
// and a `file://` adapter for local dev. The interface is intentionally
// minimal: VFS does its own content addressing and encryption above
// this layer; the backend is just a typed key→bytes store.
package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
)

// ErrNotFound is returned by Get when the requested key is absent.
var ErrNotFound = errors.New("backend: key not found")

// Backend is the contract every object-store adapter implements.
//
// Keys are arbitrary `/`-delimited strings; VFS uses
// `blocks/<blake3-prefix>/<full-hash>.zap.age` for block storage.
//
// Implementations MUST be safe for concurrent use. Get/Put/Delete are
// idempotent — repeating the same operation is allowed and SHOULD NOT
// surface as an error.
type Backend interface {
	// Get returns the bytes stored at key. If the key does not exist,
	// the returned error wraps ErrNotFound (errors.Is(err, ErrNotFound)).
	Get(ctx context.Context, key string) ([]byte, error)

	// Put writes data at key. Overwriting is allowed.
	Put(ctx context.Context, key string, data []byte) error

	// Delete removes the key. Removing a non-existent key MUST NOT error.
	Delete(ctx context.Context, key string) error

	// List returns keys with the given prefix. The returned channel is
	// closed when the listing completes; on error the channel is closed
	// after the error is sent on errCh.
	List(ctx context.Context, prefix string) (keys <-chan string, errCh <-chan error)

	// Stat returns the size in bytes for key without fetching the body.
	// ErrNotFound is returned if the key is absent.
	Stat(ctx context.Context, key string) (size int64, err error)

	// Close releases any resources (HTTP connections, file handles).
	// Safe to call multiple times.
	Close() error

	// String returns a stable, human-readable description of the
	// backend (e.g. "s3://bucket/prefix").
	String() string
}

// Reader returns an io.ReadCloser when streaming is preferred over
// in-memory Get. Optional — implementations may return ErrNotImplemented.
type Reader interface {
	GetReader(ctx context.Context, key string) (io.ReadCloser, error)
}

// Writer returns an io.WriteCloser for streaming uploads. Optional.
type Writer interface {
	PutWriter(ctx context.Context, key string) (io.WriteCloser, error)
}

// ErrNotImplemented signals that a backend doesn't support the optional
// streaming interface for the given operation.
var ErrNotImplemented = errors.New("backend: operation not implemented")

// Open dispatches a backend URL to the right implementation. Recognised
// schemes:
//
//	file://path           — local filesystem (dev only)
//	s3://bucket/prefix    — AWS S3 / S3-compatible (R2, Hanzo Storage, SeaweedFS)
//	gcs://bucket/prefix   — Google Cloud Storage (TODO)
//	azureblob://...       — Azure Blob (TODO)
//
// The opener function is registered by each backend package's init().
type Opener func(ctx context.Context, u *url.URL) (Backend, error)

var openers = map[string]Opener{}

// Register attaches an Opener for a URL scheme. Called from backend
// implementations' init().
func Register(scheme string, opener Opener) {
	if scheme == "" || opener == nil {
		panic("backend.Register: scheme and opener are required")
	}
	openers[scheme] = opener
}

// Open parses the backend URL and dispatches to the registered opener.
func Open(ctx context.Context, raw string) (Backend, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("backend.Open: parse %q: %w", raw, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("backend.Open: missing scheme in %q", raw)
	}
	opener, ok := openers[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("backend.Open: no opener registered for %q (have: %v)", u.Scheme, registeredSchemes())
	}
	return opener(ctx, u)
}

func registeredSchemes() []string {
	out := make([]string, 0, len(openers))
	for k := range openers {
		out = append(out, k)
	}
	return out
}
