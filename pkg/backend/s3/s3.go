// Package s3 is an S3 Backend on Hanzo Storage.
//
// It speaks to github.com/hanzoai/s3 through the Hanzo Storage Go SDK
// (github.com/hanzos3/go) rather than the AWS SDK. Both talk S3, so either
// would work against our own storage — but the fleet already standardises on
// that SDK (vm, commerce, licensing, ai), and carrying a second S3 client meant
// a second credential model, a second retry policy, a second error taxonomy,
// and ~30 extra modules in the graph for a backend that makes five calls.
//
// Endpoint override (Hanzo Storage, R2, any S3-compatible service) via the
// URL's `endpoint` query parameter; credentials come from the environment.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	hs3 "github.com/hanzos3/go"
	"github.com/hanzos3/go/pkg/credentials"

	"github.com/hanzoai/vfs/pkg/backend"
)

func init() {
	backend.Register("s3", openS3)
}

type s3Backend struct {
	client *hs3.Client
	bucket string
	prefix string
	desc   string

	closeOnce sync.Once
}

func openS3(_ context.Context, u *url.URL) (backend.Backend, error) {
	bucket := u.Host
	if bucket == "" {
		return nil, fmt.Errorf("s3 backend: missing bucket in %q", u.String())
	}
	prefix := strings.TrimPrefix(u.Path, "/")

	// The endpoint decides which storage this is; a bare URL means Hanzo
	// Storage via the ambient env, which is what a service in-cluster gets.
	endpoint := u.Query().Get("endpoint")
	if endpoint == "" {
		endpoint = os.Getenv("AWS_ENDPOINT_URL")
	}
	if endpoint == "" {
		endpoint = "s3.hanzo.ai"
	}
	secure := true
	if e, err := url.Parse(endpoint); err == nil && e.Host != "" {
		secure = e.Scheme != "http"
		endpoint = e.Host
	}

	cli, err := hs3.New(endpoint, &hs3.Options{
		// EnvAWS then EnvMinio then the IAM role, in that order — the same
		// chain every other Hanzo service resolves credentials through.
		Creds:  credentials.NewChainCredentials(defaultCredProviders()),
		Secure: secure,
		Region: u.Query().Get("region"),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 backend: %w", err)
	}

	return &s3Backend{
		client: cli,
		bucket: bucket,
		prefix: prefix,
		desc:   u.String(),
	}, nil
}

func defaultCredProviders() []credentials.Provider {
	return []credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.EnvMinio{},
		&credentials.IAM{},
	}
}

func (s *s3Backend) k(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

// notFound maps the SDK's error taxonomy onto backend.ErrNotFound so callers
// see one sentinel regardless of which storage answered.
func notFound(err error, key string) error {
	switch hs3.ToErrorResponse(err).Code {
	case "NoSuchKey", "NoSuchBucket", "NotFound":
		return fmt.Errorf("%w: %s", backend.ErrNotFound, key)
	}
	return err
}

func (s *s3Backend) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.k(key), hs3.GetObjectOptions{})
	if err != nil {
		return nil, notFound(err, key)
	}
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		// GetObject is lazy: a missing key surfaces on the first read, not on
		// the call, so the sentinel mapping has to happen here too.
		return nil, notFound(err, key)
	}
	return b, nil
}

func (s *s3Backend) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, s.k(key),
		bytes.NewReader(data), int64(len(data)), hs3.PutObjectOptions{})
	return err
}

func (s *s3Backend) Delete(ctx context.Context, key string) error {
	// Idempotent by contract — an absent key is not an error.
	return s.client.RemoveObject(ctx, s.bucket, s.k(key), hs3.RemoveObjectOptions{})
}

func (s *s3Backend) List(ctx context.Context, prefix string) (<-chan string, <-chan error) {
	keys := make(chan string, 256)
	errCh := make(chan error, 1)
	go func() {
		defer close(keys)
		defer close(errCh)
		for obj := range s.client.ListObjects(ctx, s.bucket, hs3.ListObjectsOptions{
			Prefix:    s.k(prefix),
			Recursive: true,
		}) {
			if obj.Err != nil {
				errCh <- obj.Err
				return
			}
			k := obj.Key
			if s.prefix != "" {
				k = strings.TrimPrefix(k, s.prefix+"/")
			}
			select {
			case <-ctx.Done():
				return
			case keys <- k:
			}
		}
	}()
	return keys, errCh
}

func (s *s3Backend) Stat(ctx context.Context, key string) (int64, error) {
	info, err := s.client.StatObject(ctx, s.bucket, s.k(key), hs3.StatObjectOptions{})
	if err != nil {
		return 0, notFound(err, key)
	}
	return info.Size, nil
}

func (s *s3Backend) Close() error {
	s.closeOnce.Do(func() {})
	return nil
}

func (s *s3Backend) String() string { return s.desc }

var _ backend.Backend = (*s3Backend)(nil)
