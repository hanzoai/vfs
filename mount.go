//go:build cloud_mount

// HIP-0106 Mount() entry point. Lets cmd/cloud import this package and
// register VFS with the shared zip.App alongside every other Hanzo
// subsystem (iam, base, kms, amqp, …).
//
// Wire shape:
//
//	import _ "github.com/hanzoai/vfs"  // init() registers
//
// VFS has no public HTTP surface today — it's an S3-backed virtual
// block filesystem driven through FUSE mounts and CLI/sidecar usage.
// Mount() exposes liveness/readiness probes plus a minimal HTTP
// CRUD surface (PUT/GET/DELETE single block by ID) so other in-process
// subsystems can talk to it without falling back to the CLI.
//
// The VFS instance itself is opt-in: callers either inject one via
// SetInstance(v) (used by the standalone shim and tests) or skip it
// (the cloud binary mounts VFS as a passthrough until a backend +
// crypto config lands in cloud.Config). Readiness reports "not_ready"
// until an instance is attached.
package vfs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/hanzoai/cloud"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/zap-proto/zip"
)

// Version is overridden at build time via -ldflags
// "-X github.com/hanzoai/vfs.Version=...".
var Version = "dev"

// mountedInstance is the VFS handle the HTTP routes operate against.
// nil until SetInstance() is called. Guarded by mu.
var (
	mu              sync.RWMutex
	mountedInstance *VFS
)

// SetInstance attaches a VFS handle for the mounted HTTP routes.
// Calling this with nil disables the data-plane routes (Get/Put/Delete
// return 503). Idempotent.
//
// The standalone shim wires this from CLI flags. cloud.Config has no
// VFS-backend block yet — cloud binary deploys leave it nil until
// HIP-0107 config lands.
func SetInstance(v *VFS) {
	mu.Lock()
	defer mu.Unlock()
	mountedInstance = v
}

// Instance returns the currently attached VFS handle (or nil).
func Instance() *VFS {
	mu.RLock()
	defer mu.RUnlock()
	return mountedInstance
}

// Mount registers VFS routes with the shared cloud zip.App per HIP-0106.
//
// Routes:
//
//	GET    /v1/vfs/health         — liveness, never blocks
//	GET    /v1/vfs/readyz         — readiness, 200 only when instance attached
//	PUT    /v1/vfs/blocks         — body = raw block bytes; returns {"id": "..."}
//	GET    /v1/vfs/blocks/:id     — returns raw decrypted block bytes
//	DELETE /v1/vfs/blocks/:id     — removes a block from the backend
//	GET    /v1/vfs/stats          — backend/cache counters
//
// Identity headers (X-Org-Id, X-User-Id) are honoured but VFS is a
// single-tenant block layer today — multitenant prefixing lands when
// HIP-0107 specifies per-org namespacing.
func Mount(app *zip.App, deps cloud.Deps) error {
	logger := deps.Logger.New("subsystem", "vfs")

	app.Get("/v1/vfs/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "vfs",
			"version": Version,
		})
	})

	app.Get("/v1/vfs/readyz", func(c *zip.Ctx) error {
		if Instance() == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{
				"status":  "not_ready",
				"service": "vfs",
				"reason":  "no backend attached (call vfs.SetInstance)",
			})
		}
		return c.JSON(http.StatusOK, map[string]any{
			"status":  "ready",
			"service": "vfs",
			"version": Version,
		})
	})

	app.Get("/v1/vfs/stats", func(c *zip.Ctx) error {
		v := Instance()
		if v == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{
				"error": "vfs not attached",
			})
		}
		return c.JSON(http.StatusOK, v.Stats())
	})

	app.Put("/v1/vfs/blocks", func(c *zip.Ctx) error {
		v := Instance()
		if v == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "vfs not attached"})
		}
		body := c.Body()
		if len(body) == 0 {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "empty body"})
		}
		if len(body) > BlockSize {
			return c.JSON(http.StatusBadRequest, map[string]any{
				"error":     fmt.Sprintf("payload %d > BlockSize %d", len(body), BlockSize),
				"blockSize": BlockSize,
			})
		}
		id, err := v.PutBlock(c.Context(), body)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		return c.JSON(http.StatusCreated, map[string]any{"id": string(id)})
	})

	app.Get("/v1/vfs/blocks/:id", func(c *zip.Ctx) error {
		v := Instance()
		if v == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "vfs not attached"})
		}
		raw := c.Param("id")
		if !isHexBlockID(raw) {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "block id must be lowercase hex"})
		}
		pt, err := v.GetBlock(c.Context(), BlockID(raw))
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				return c.JSON(http.StatusNotFound, map[string]any{"error": "not found"})
			}
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		c.SetHeader("Content-Type", "application/octet-stream")
		return c.Bytes(http.StatusOK, pt)
	})

	app.Delete("/v1/vfs/blocks/:id", func(c *zip.Ctx) error {
		v := Instance()
		if v == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "vfs not attached"})
		}
		raw := c.Param("id")
		if !isHexBlockID(raw) {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "block id must be lowercase hex"})
		}
		if err := v.Delete(c.Context(), BlockID(raw)); err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				return c.JSON(http.StatusNotFound, map[string]any{"error": "not found"})
			}
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		return c.JSON(http.StatusOK, map[string]any{"ok": true})
	})

	logger.Info("vfs mounted", "version", Version, "instance_attached", Instance() != nil)
	return nil
}

// Shutdown drains the attached VFS instance (closing its backend).
// Idempotent. Safe to call when no instance was attached.
func Shutdown(_ context.Context) error {
	mu.Lock()
	defer mu.Unlock()
	if mountedInstance == nil {
		return nil
	}
	err := mountedInstance.Close()
	mountedInstance = nil
	return err
}

// isHexBlockID matches the lowercase hex string output by HashBlock.
// Used to reject path traversal / control bytes before hitting the
// backend.
func isHexBlockID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// ensure io.Copy stays imported (used by the data-plane streaming
// branch when multi-block File support lands in 0.4.0).
var _ = io.Copy

// init registers VFS with the cloud subsystem registry. Order 20 —
// after kms (10) and ahead of amqp (30) / mq (40).
func init() {
	cloud.Register("vfs", 20, func(app any, deps cloud.Deps) error {
		a, ok := app.(*zip.App)
		if !ok {
			return fmt.Errorf("vfs.Mount: app is %T, want *zip.App", app)
		}
		return Mount(a, deps)
	})
}
