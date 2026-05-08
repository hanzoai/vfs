//go:build fuse && (linux || darwin)

// End-to-end FUSE + SQLite test. Mounts a *vfs.FS at a tempdir,
// opens SQLite directly on the mountpoint (no copy step), runs CREATE
// + INSERT + SELECT + integrity_check, unmounts, remounts, verifies
// the database survived the round trip.
//
// Run with:
//   VFS_FUSE_E2E=1 go test -tags fuse -run TestFUSESQLite ./pkg/mount/
//
// Linux: requires kernel ≥ 3.10 with FUSE support (default on every
// modern distro).
// macOS: requires either macFUSE (https://macfuse.io) or fuse-t
// (https://www.fuse-t.org). fuse-t is preferred — userspace, no kext.
package mount_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luxfi/age"
	_ "modernc.org/sqlite"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/mount"
	"github.com/hanzoai/vfs/pkg/vfs"
)

func TestFUSESQLite(t *testing.T) {
	if os.Getenv("VFS_FUSE_E2E") == "" {
		t.Skip("set VFS_FUSE_E2E=1 to run FUSE e2e tests (requires Linux kernel FUSE + privileges)")
	}

	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "store")
	mountpoint := filepath.Join(tmp, "mnt")
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}
	defer func() {
		// Best-effort unmount on test exit; Mount's ctx-cancel goroutine
		// has already triggered the FUSE driver's own Unmount, but we
		// also try the platform tool as a belt-and-braces.
		switch runtime.GOOS {
		case "linux":
			_ = exec.Command("fusermount", "-u", mountpoint).Run()
			_ = exec.Command("fusermount3", "-u", mountpoint).Run()
		case "darwin":
			_ = exec.Command("umount", mountpoint).Run()
			_ = exec.Command("diskutil", "unmount", mountpoint).Run()
		}
	}()

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age: %v", err)
	}

	mountFS := func() (*vfs.FS, context.CancelFunc, *sync.WaitGroup) {
		t.Helper()
		be, err := backend.Open(context.Background(), "file://"+storeDir)
		if err != nil {
			t.Fatalf("backend.Open: %v", err)
		}
		c, err := vfs.NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
		if err != nil {
			t.Fatalf("NewCrypto: %v", err)
		}
		v, err := vfs.New(vfs.Config{Backend: be, Crypto: c})
		if err != nil {
			t.Fatalf("vfs.New: %v", err)
		}
		fs, err := vfs.NewFS(context.Background(), v)
		if err != nil {
			t.Fatalf("NewFS: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := mount.Mount(ctx, fs, mountpoint); err != nil &&
				!strings.Contains(err.Error(), "transport endpoint is not connected") {
				t.Logf("Mount returned: %v", err)
			}
		}()
		// Wait for mount to be live.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(mountpoint); err == nil {
				if entries, _ := os.ReadDir(mountpoint); entries != nil {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		return fs, cancel, &wg
	}

	dbPath := filepath.Join(mountpoint, "test.db")

	// === First mount: create + populate the DB ===
	{
		_, cancel, wg := mountFS()

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			cancel()
			wg.Wait()
			t.Fatalf("sql.Open via FUSE: %v", err)
		}
		if _, err := db.Exec(`CREATE TABLE coins (sym TEXT PRIMARY KEY, name TEXT, decimals INT)`); err != nil {
			t.Fatalf("CREATE: %v", err)
		}
		stmt, err := db.Prepare(`INSERT INTO coins VALUES (?, ?, ?)`)
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := stmt.Exec(fmt.Sprintf("_%d", i), "Liquidity", 18); err != nil {
				t.Fatalf("INSERT %d: %v", i, err)
			}
		}
		_ = stmt.Close()
		var got int
		if err := db.QueryRow(`SELECT count(*) FROM coins`).Scan(&got); err != nil {
			t.Fatalf("SELECT: %v", err)
		}
		if got != 100 {
			t.Fatalf("count=%d want 100", got)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		cancel()
		wg.Wait()
	}

	// === Second mount: re-open the DB, verify integrity ===
	{
		_, cancel, wg := mountFS()
		defer func() { cancel(); wg.Wait() }()

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("sql.Open after remount: %v", err)
		}
		defer db.Close()

		var ok string
		if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&ok); err != nil {
			t.Fatalf("integrity_check: %v", err)
		}
		if !strings.EqualFold(ok, "ok") {
			t.Fatalf("integrity_check = %q want ok", ok)
		}
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM coins`).Scan(&count); err != nil {
			t.Fatalf("SELECT count after remount: %v", err)
		}
		if count != 100 {
			t.Fatalf("after remount count=%d want 100", count)
		}
		t.Logf("FUSE+SQLite e2e: %d rows survived umount+remount, integrity_check=%s", count, ok)
	}
}
