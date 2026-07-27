//go:build cloud_mount

package vfs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/luxfi/age"
	luxlog "github.com/luxfi/log"

	"github.com/hanzoai/cloud"
	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/zap-proto/zip"
)

// newMountTestVFS creates a file-backed VFS suitable for HTTP testing.
func newMountTestVFS(t *testing.T) *vfs.VFS {
	t.Helper()
	be, err := backend.Open(context.Background(), "file://"+t.TempDir())
	if err != nil {
		t.Fatalf("backend.Open: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity: %v", err)
	}
	c, err := vfs.NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
	if err != nil {
		t.Fatalf("NewCrypto: %v", err)
	}
	v, err := vfs.New(vfs.Config{Backend: be, Crypto: c})
	if err != nil {
		t.Fatalf("vfs.New: %v", err)
	}
	return v
}

func TestMount_HealthReadyz(t *testing.T) {
	app := zip.New(zip.Config{Logger: luxlog.New("test")})
	deps := cloud.Deps{Logger: luxlog.New("test")}

	// Detach any prior instance (other tests may have set one).
	vfs.SetInstance(nil)
	t.Cleanup(func() { vfs.SetInstance(nil) })

	if err := vfs.Mount(app, deps); err != nil {
		t.Fatalf("Mount: %v", err)
	}

	// Health always 200, even without an instance.
	req := httptest.NewRequest("GET", "/v1/vfs/health", nil)
	resp, err := app.Fiber().Test(req)
	if err != nil {
		t.Fatalf("health test: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("health status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var hb map[string]any
	_ = json.Unmarshal(body, &hb)
	if hb["service"] != "vfs" {
		t.Fatalf("health body service = %v, want vfs", hb["service"])
	}

	// Readyz without instance → 503.
	req = httptest.NewRequest("GET", "/v1/vfs/readyz", nil)
	resp, err = app.Fiber().Test(req)
	if err != nil {
		t.Fatalf("readyz test: %v", err)
	}
	if resp.StatusCode != 503 {
		t.Fatalf("readyz (no instance) status = %d, want 503", resp.StatusCode)
	}

	// Attach instance → 200.
	vfs.SetInstance(newMountTestVFS(t))
	req = httptest.NewRequest("GET", "/v1/vfs/readyz", nil)
	resp, err = app.Fiber().Test(req)
	if err != nil {
		t.Fatalf("readyz attached test: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("readyz (attached) status = %d, want 200", resp.StatusCode)
	}
}

func TestMount_PutGetRoundtrip(t *testing.T) {
	app := zip.New(zip.Config{Logger: luxlog.New("test")})
	deps := cloud.Deps{Logger: luxlog.New("test")}
	vfs.SetInstance(newMountTestVFS(t))
	t.Cleanup(func() { vfs.SetInstance(nil) })

	if err := vfs.Mount(app, deps); err != nil {
		t.Fatalf("Mount: %v", err)
	}

	plain := []byte("zip mount roundtrip — small block payload")

	// PUT.
	put := httptest.NewRequest("PUT", "/v1/vfs/blocks", bytes.NewReader(plain))
	putResp, err := app.Fiber().Test(put)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if putResp.StatusCode != 201 {
		t.Fatalf("PUT status = %d, want 201", putResp.StatusCode)
	}
	var putBody struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(putResp.Body).Decode(&putBody)
	if putBody.ID == "" || len(putBody.ID) != 64 {
		t.Fatalf("PUT body id = %q (want 64-hex)", putBody.ID)
	}

	// GET.
	get := httptest.NewRequest("GET", "/v1/vfs/blocks/"+putBody.ID, nil)
	getResp, err := app.Fiber().Test(get)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if getResp.StatusCode != 200 {
		t.Fatalf("GET status = %d, want 200", getResp.StatusCode)
	}
	gotRaw, _ := io.ReadAll(getResp.Body)
	if !strings.HasPrefix(string(gotRaw), string(plain)) {
		t.Fatalf("GET body prefix mismatch:\n got=%q\nwant prefix=%q", gotRaw[:64], plain)
	}
}
