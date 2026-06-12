// Package s3 is an AWS S3 / S3-compatible Backend.
//
// Endpoint override (R2, Hanzo Storage, MinIO) via AWS_ENDPOINT_URL or
// the URL's `endpoint` query parameter.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/hanzoai/vfs/pkg/backend"
)

func init() {
	backend.Register("s3", openS3)
}

type s3Backend struct {
	client *s3v2.Client
	bucket string
	prefix string
	desc   string

	closeOnce sync.Once
}

func openS3(ctx context.Context, u *url.URL) (backend.Backend, error) {
	bucket := u.Host
	if bucket == "" {
		return nil, fmt.Errorf("s3 backend: missing bucket in %q", u.String())
	}
	prefix := strings.TrimPrefix(u.Path, "/")

	loadOpts := []func(*config.LoadOptions) error{}
	if region := u.Query().Get("region"); region != "" {
		loadOpts = append(loadOpts, config.WithRegion(region))
	}

	// Static credentials for key-based S3 (MinIO, hanzoai/s3, R2) that don't
	// use the AWS IAM/instance-profile chain. Accepted as URL userinfo
	// (s3://ACCESS:SECRET@bucket/prefix) or as access_key/secret_key query
	// params. Absent either, fall back to the AWS default credential chain
	// (env, shared config, IMDS/IRSA).
	accessKey, secretKey := "", ""
	if u.User != nil {
		accessKey = u.User.Username()
		secretKey, _ = u.User.Password()
	}
	if v := u.Query().Get("access_key"); v != "" {
		accessKey = v
	}
	if v := u.Query().Get("secret_key"); v != "" {
		secretKey = v
	}
	if accessKey != "" && secretKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, u.Query().Get("session_token")),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3 backend: load config: %w", err)
	}

	clientOpts := []func(*s3v2.Options){}
	if endpoint := u.Query().Get("endpoint"); endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3v2.Options) {
			o.BaseEndpoint = awsv2.String(endpoint)
			o.UsePathStyle = true
		})
	} else if u.Query().Get("force_path_style") == "true" {
		clientOpts = append(clientOpts, func(o *s3v2.Options) {
			o.UsePathStyle = true
		})
	}

	cli := s3v2.NewFromConfig(cfg, clientOpts...)

	// Redact any userinfo (access:secret) from the human-readable description
	// so String()/logs never leak credentials.
	redacted := *u
	redacted.User = nil
	q := redacted.Query()
	for _, k := range []string{"access_key", "secret_key", "session_token"} {
		if q.Has(k) {
			q.Set(k, "REDACTED")
		}
	}
	redacted.RawQuery = q.Encode()

	return &s3Backend{
		client: cli,
		bucket: bucket,
		prefix: prefix,
		desc:   redacted.String(),
	}, nil
}

func (s *s3Backend) k(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func (s *s3Backend) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3v2.GetObjectInput{
		Bucket: awsv2.String(s.bucket),
		Key:    awsv2.String(s.k(key)),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *s3Backend) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3v2.PutObjectInput{
		Bucket: awsv2.String(s.bucket),
		Key:    awsv2.String(s.k(key)),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *s3Backend) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3v2.DeleteObjectInput{
		Bucket: awsv2.String(s.bucket),
		Key:    awsv2.String(s.k(key)),
	})
	// S3 DeleteObject is idempotent — absent keys are not an error.
	return err
}

func (s *s3Backend) List(ctx context.Context, prefix string) (<-chan string, <-chan error) {
	keys := make(chan string, 256)
	errCh := make(chan error, 1)
	go func() {
		defer close(keys)
		defer close(errCh)
		fullPrefix := s.k(prefix)
		paginator := s3v2.NewListObjectsV2Paginator(s.client, &s3v2.ListObjectsV2Input{
			Bucket: awsv2.String(s.bucket),
			Prefix: awsv2.String(fullPrefix),
		})
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				errCh <- err
				return
			}
			for _, obj := range page.Contents {
				k := awsv2.ToString(obj.Key)
				if s.prefix != "" {
					k = strings.TrimPrefix(k, s.prefix+"/")
				}
				select {
				case <-ctx.Done():
					return
				case keys <- k:
				}
			}
		}
	}()
	return keys, errCh
}

func (s *s3Backend) Stat(ctx context.Context, key string) (int64, error) {
	out, err := s.client.HeadObject(ctx, &s3v2.HeadObjectInput{
		Bucket: awsv2.String(s.bucket),
		Key:    awsv2.String(s.k(key)),
	})
	if err != nil {
		var nfe *s3types.NotFound
		if errors.As(err, &nfe) {
			return 0, fmt.Errorf("%w: %s", backend.ErrNotFound, key)
		}
		return 0, err
	}
	return awsv2.ToInt64(out.ContentLength), nil
}

func (s *s3Backend) Close() error {
	s.closeOnce.Do(func() {})
	return nil
}

func (s *s3Backend) String() string { return s.desc }

var _ backend.Backend = (*s3Backend)(nil)
