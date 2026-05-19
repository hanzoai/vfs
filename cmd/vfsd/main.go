// vfsd — HIP-0106 thin shim.
//
// Mounts pkg/vfs into a zip.App via the same Mount() the unified cloud
// binary calls. The CLI tooling for put/get/stats/mount lives in
// cmd/vfs; this binary is the standalone HTTP server shape for
// environments that need block CRUD over HTTP without the full cloud
// surface.
//
// The data plane is only enabled when a VFS instance is attached via
// vfs.SetInstance. Without one the binary still serves /v1/vfs/health
// and a 503 /v1/vfs/readyz — useful for ConfigMap-driven k8s probes
// while the operator wires the backend.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/luxfi/age"
	"github.com/luxfi/log"

	"github.com/hanzoai/cloud/pkg/cloud"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	_ "github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/vfs"
	"github.com/hanzoai/zip"
	"github.com/hanzoai/zip/middleware"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	vfs.Version = version

	cfg := cloud.LoadConfig()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	deps := cloud.BuildDeps(cfg)

	// Optional backend wiring via env. VFSD_BACKEND=file:///tmp/v or
	// s3://bucket/prefix. When unset, the binary boots with no data
	// plane — /v1/vfs/blocks/* return 503 until SetInstance fires.
	if backendURL := strings.TrimSpace(os.Getenv("VFSD_BACKEND")); backendURL != "" {
		if v, err := buildVFS(backendURL); err != nil {
			log.Crit("vfsd: backend wiring failed", "err", err)
		} else {
			vfs.SetInstance(v)
			log.Info("vfsd: backend attached", "backend", backendURL)
		}
	} else {
		log.Info("vfsd: VFSD_BACKEND unset — data plane disabled (readyz returns 503)")
	}

	app := zip.New(zip.Config{
		Logger:  deps.Logger,
		AppName: "vfsd",
	})
	app.Use(middleware.Recover())
	app.Use(middleware.RequestID())
	app.Use(middleware.Logger(deps.Logger))

	if err := vfs.Mount(app, deps); err != nil {
		log.Crit("vfsd: mount", "err", err)
	}

	listenErr := make(chan error, 1)
	go func() {
		log.Info("vfsd: listening", "addr", cfg.ListenAddr)
		listenErr <- app.Listen(cfg.ListenAddr)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("vfsd: shutting down", "signal", s)
	case err := <-listenErr:
		log.Crit("vfsd: listen failed", "err", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = vfs.Shutdown(stopCtx)
	_ = app.ShutdownWithContext(stopCtx)
}

// buildVFS wires a VFS instance from env. Mirrors the cmd/vfs CLI's
// openVFS but reads from env instead of flags. age keys come from
// VFSD_AGE_KEY (path to key file) — required for any meaningful
// roundtrip. Without it the instance is write-only.
func buildVFS(backendURL string) (*vfs.VFS, error) {
	ctx := context.Background()
	be, err := backend.Open(ctx, backendURL)
	if err != nil {
		return nil, fmt.Errorf("backend.Open: %w", err)
	}

	var recs []age.Recipient
	var ids []age.Identity
	if keyPath := strings.TrimSpace(os.Getenv("VFSD_AGE_KEY")); keyPath != "" {
		keyBytes, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("read VFSD_AGE_KEY: %w", err)
		}
		parsed, err := age.ParseIdentities(strings.NewReader(string(keyBytes)))
		if err != nil {
			return nil, fmt.Errorf("parse age identities: %w", err)
		}
		ids = parsed
		// Recipients default to the identities' own public keys.
		for _, id := range parsed {
			if x25519, ok := id.(*age.X25519Identity); ok {
				recs = append(recs, x25519.Recipient())
			}
		}
	}

	crypto, err := vfs.NewCrypto(recs, ids)
	if err != nil {
		return nil, fmt.Errorf("vfs.NewCrypto: %w", err)
	}
	return vfs.New(vfs.Config{
		Backend:  be,
		Crypto:   crypto,
		CacheMax: 256 << 20,
	})
}
