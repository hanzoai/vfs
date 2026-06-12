package s3

import (
	"context"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

// openS3 must accept static credentials (URL userinfo or query params) so vfs
// can talk to key-based S3 servers (hanzoai/s3, MinIO) — and must never leak
// those credentials in the backend's description string.
func TestOpenS3StaticCredsRedacted(t *testing.T) {
	be, err := backend.Open(context.Background(),
		"s3://AKID:SECRET@lux-snapshots/testnet/zaprepl?endpoint=http://s3.lux-system:9000&region=us-central1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer be.Close()

	desc := be.String()
	if strings.Contains(desc, "SECRET") || strings.Contains(desc, "AKID") {
		t.Fatalf("description leaks credentials: %s", desc)
	}
	if !strings.Contains(desc, "lux-snapshots") {
		t.Fatalf("description lost the bucket: %s", desc)
	}
}

func TestOpenS3QueryCredsRedacted(t *testing.T) {
	be, err := backend.Open(context.Background(),
		"s3://lux-snapshots/p?endpoint=http://s3:9000&access_key=AKID&secret_key=SECRET&force_path_style=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer be.Close()
	if d := be.String(); strings.Contains(d, "SECRET") {
		t.Fatalf("query secret leaked: %s", d)
	}
}

func TestOpenS3NoCredsStillOpens(t *testing.T) {
	// No creds → falls back to the AWS default chain; construction must still
	// succeed (it doesn't connect until first use).
	if _, err := backend.Open(context.Background(), "s3://b/p?region=us-east-1"); err != nil {
		t.Fatalf("open without creds: %v", err)
	}
}
