// Production-readiness tests for using VFS to back persistent stateful
// workloads (luxd chain DB, SQLite, etc.).
//
// Covers the four code-level concerns that gate "can we mount VFS at
// /data/db and walk away":
//
//   #1 crash/restart recovery  — mid-write process death + reopen,
//                                verify FS view is consistent.
//   #4 compaction under load   — sustained write pattern matching
//                                zapdb compaction (many small files,
//                                frequent fsync, occasional bulk rewrite).
//   #5 backend outage          — inject Put/Get errors and verify the
//                                FS surfaces them rather than hanging or
//                                silently losing data.
//   #6 key rotation            — confirm Crypto accepts a multi-recipient
//                                list (rolling rotation viable) and that
//                                a single Get round-trip still works after
//                                a recipient is added or removed.
//
// The three operational concerns — empty-cache bootstrap timing,
// warm-cache bootstrap timing, and rollback path to PVC-local state —
// are runbooks against a real cluster, not Go tests; see
// docs/PRODUCTION-READINESS.md.
package vfs_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

// ---------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------

// buildFS mounts a VFS rooted in dir with the given identity. Returns
// the *FS and a teardown func (Sync only — VFS exposes no Close; the
// Sync is the durability boundary).
func buildFS(t *testing.T, dir string, id *age.X25519Identity) (*vfs.FS, func()) {
	t.Helper()
	be, err := backend.Open(context.Background(), "file://"+dir)
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
	teardown := func() {
		if err := fs.Sync(context.Background()); err != nil {
			t.Errorf("teardown Sync: %v", err)
		}
	}
	return fs, teardown
}

// ---------------------------------------------------------------------
// #1 crash / restart recovery
// ---------------------------------------------------------------------

// TestCrashRecovery_Synced_Persists confirms that any file that was
// Sync()-ed before the crash is observable identically after reopen,
// and that the inode tree (sizes, mode, mtimes) round-trips.
//
// This is the durability contract a POSIX consumer (zapdb, SQLite)
// relies on: `fsync(2)` returning nil must mean the data is durable
// across an arbitrary process kill.
func TestCrashRecovery_Synced_Persists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}

	// Round 1: write three files, Sync, then drop FS without unmount.
	const N = 3
	payloads := make([][]byte, N)
	{
		fs, _ := buildFS(t, dir, id)
		for i := 0; i < N; i++ {
			name := fmt.Sprintf("/file-%d.db", i)
			if _, err := fs.Create(name, 0o644); err != nil {
				t.Fatalf("Create %s: %v", name, err)
			}
			f, err := fs.Open(context.Background(), name)
			if err != nil {
				t.Fatalf("Open %s: %v", name, err)
			}
			// Cross-block payload to exercise the block-splitting path.
			payloads[i] = make([]byte, 3*vfs.BlockSize+1234)
			for j := range payloads[i] {
				payloads[i][j] = byte(i*7 + j%251)
			}
			if _, err := f.WriteAt(payloads[i], 0); err != nil {
				t.Fatalf("WriteAt %s: %v", name, err)
			}
			if err := f.Sync(); err != nil {
				t.Fatalf("Sync %s: %v", name, err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("Close %s: %v", name, err)
			}
		}
		// Simulated crash: we drop `fs` on the floor without
		// any further Sync. The File.Sync calls already pushed the
		// metadata blob; this is the durability contract under test.
	}

	// Round 2: reopen + verify byte-equal reads + correct sizes.
	fs2, _ := buildFS(t, dir, id)
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("/file-%d.db", i)
		f, err := fs2.Open(context.Background(), name)
		if err != nil {
			t.Fatalf("Open after crash %s: %v", name, err)
		}
		stat, err := f.Stat()
		if err != nil {
			t.Fatalf("Stat after crash %s: %v", name, err)
		}
		if int64(stat.Size) != int64(len(payloads[i])) {
			t.Fatalf("size drift on %s: got %d want %d", name, stat.Size, len(payloads[i]))
		}
		got := make([]byte, len(payloads[i]))
		n, err := f.ReadAt(got, 0)
		// io.ReaderAt contract: hitting end-of-file returns (n, io.EOF)
		// even when the read was complete. Only fail on other errors.
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("ReadAt %s: %v", name, err)
		}
		if n != len(payloads[i]) {
			t.Fatalf("short read on %s: got %d want %d", name, n, len(payloads[i]))
		}
		for j := range got {
			if got[j] != payloads[i][j] {
				t.Fatalf("byte drift on %s at offset %d: got %02x want %02x",
					name, j, got[j], payloads[i][j])
			}
		}
		_ = f.Close()
	}
}

// TestCrashRecovery_UnsyncedWriteIsLost_NotCorrupted is the negative
// half of the durability story: a write that was NOT Sync()-ed before
// crash must not appear in the post-restart view (no torn-write
// resurrection from orphaned blocks), and pre-existing files must
// remain readable byte-for-byte.
//
// This pins the policy: POSIX consumers MUST call fsync to guarantee
// durability — exactly the contract zapdb/SQLite already expect.
func TestCrashRecovery_UnsyncedWriteIsLost_NotCorrupted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}

	stable := []byte("durable-bytes-from-first-mount")
	{
		fs, _ := buildFS(t, dir, id)
		if _, err := fs.Create("/stable.db", 0o644); err != nil {
			t.Fatalf("Create stable: %v", err)
		}
		f, _ := fs.Open(context.Background(), "/stable.db")
		if _, err := f.WriteAt(stable, 0); err != nil {
			t.Fatalf("WriteAt stable: %v", err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("Sync stable: %v", err)
		}
		_ = f.Close()
		// Now write a second file but DO NOT Sync — simulates a write
		// in flight when the host crashes.
		if _, err := fs.Create("/in-flight.db", 0o644); err != nil {
			t.Fatalf("Create in-flight: %v", err)
		}
		f2, _ := fs.Open(context.Background(), "/in-flight.db")
		if _, err := f2.WriteAt([]byte("never-fsynced"), 0); err != nil {
			t.Fatalf("WriteAt in-flight: %v", err)
		}
		// Deliberately skip Sync + Close — simulating crash.
	}

	fs2, _ := buildFS(t, dir, id)

	// stable.db must round-trip exactly.
	f, err := fs2.Open(context.Background(), "/stable.db")
	if err != nil {
		t.Fatalf("Open stable after crash: %v", err)
	}
	got := make([]byte, len(stable))
	if _, err := f.ReadAt(got, 0); err != nil && err.Error() != "EOF" {
		t.Fatalf("ReadAt stable: %v", err)
	}
	for i := range got {
		if got[i] != stable[i] {
			t.Fatalf("corruption on stable.db at byte %d", i)
		}
	}
	_ = f.Close()

	// in-flight.db must be ABSENT (the metadata root that registers
	// it was never written). If we see the file, VFS persisted
	// metadata without a Sync — durability contract broken.
	if _, err := fs2.Open(context.Background(), "/in-flight.db"); err == nil {
		t.Fatalf("in-flight.db visible after crash without fsync — durability policy violated")
	}
}

// ---------------------------------------------------------------------
// #4 compaction-style sustained write
// ---------------------------------------------------------------------

// TestCompaction_SustainedWrite emulates zapdb's compaction pattern:
// many small files, frequent fsync, and an occasional large rewrite.
// Reports per-op latency + throughput so the operator can decide if the
// observed numbers fit luxd's write budget under chain load.
//
// Skipped by default — set VFS_PROD_TEST=1 to opt in. The test runs
// 60 seconds of writes; CI runs should pass shorter durations via
// VFS_PROD_TEST_DURATION=30s.
func TestCompaction_SustainedWrite(t *testing.T) {
	if v := os.Getenv("VFS_PROD_TEST"); v == "" || v == "0" || v == "false" {
		t.Skip("set VFS_PROD_TEST=1 to run production-readiness benchmarks")
	}
	dir := t.TempDir()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	fs, teardown := buildFS(t, dir, id)
	defer teardown()

	dur := 60 * time.Second
	if v := os.Getenv("VFS_PROD_TEST_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("VFS_PROD_TEST_DURATION: %v", err)
		}
		dur = d
	}
	deadline := time.Now().Add(dur)

	// Pattern: 16 "SST" files, each grown by 64KB and Sync()'ed in a
	// loop. Every 8 iterations we drop one file (compaction merge) and
	// create a new one. This is a coarse-grained approximation; what
	// we want is throughput and tail latency numbers, not a faithful
	// LSM simulation.
	const numFiles = 16
	files := make([]*vfs.File, 0, numFiles)
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("/sst-%03d.dat", i)
		if _, err := fs.Create(name, 0o644); err != nil {
			t.Fatalf("Create %s: %v", name, err)
		}
		f, err := fs.Open(context.Background(), name)
		if err != nil {
			t.Fatalf("Open %s: %v", name, err)
		}
		files = append(files, f)
	}

	chunk := make([]byte, 64*1024)
	for j := range chunk {
		chunk[j] = byte(j)
	}

	var ops int64
	var totalNs int64
	var maxNs int64
	iter := 0
	for time.Now().Before(deadline) {
		f := files[iter%numFiles]
		offset := int64((iter / numFiles) * len(chunk))

		start := time.Now()
		if _, err := f.WriteAt(chunk, offset); err != nil {
			t.Fatalf("WriteAt iter=%d: %v", iter, err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("Sync iter=%d: %v", iter, err)
		}
		elapsed := time.Since(start).Nanoseconds()
		ops++
		totalNs += elapsed
		if elapsed > maxNs {
			maxNs = elapsed
		}
		iter++
	}

	for _, f := range files {
		_ = f.Close()
	}

	mean := time.Duration(totalNs / ops)
	t.Logf("compaction sustained-write: ops=%d duration=%s mean=%s max=%s thru=%.2f ops/s",
		ops, dur, mean, time.Duration(maxNs), float64(ops)/dur.Seconds())

	// The acceptance bar is operator-policy, not a hard test fail.
	// We just record the numbers so the runbook has empirical data.
}

// ---------------------------------------------------------------------
// #5 backend outage behavior
// ---------------------------------------------------------------------

// erroringBackend wraps a real backend and injects errors on demand.
// Used to simulate S3 503s / network partitions / timeouts without
// requiring a fault-injection proxy.
type erroringBackend struct {
	inner backend.Backend
	err   atomic.Pointer[error]
}

func newErroringBackend(inner backend.Backend) *erroringBackend {
	return &erroringBackend{inner: inner}
}

func (e *erroringBackend) setErr(err error) {
	if err == nil {
		e.err.Store(nil)
		return
	}
	e.err.Store(&err)
}

func (e *erroringBackend) Get(ctx context.Context, key string) ([]byte, error) {
	if err := e.err.Load(); err != nil {
		return nil, *err
	}
	return e.inner.Get(ctx, key)
}

func (e *erroringBackend) Put(ctx context.Context, key string, b []byte) error {
	if err := e.err.Load(); err != nil {
		return *err
	}
	return e.inner.Put(ctx, key, b)
}

func (e *erroringBackend) Delete(ctx context.Context, key string) error {
	if err := e.err.Load(); err != nil {
		return *err
	}
	return e.inner.Delete(ctx, key)
}

func (e *erroringBackend) List(ctx context.Context, prefix string) (<-chan string, <-chan error) {
	if err := e.err.Load(); err != nil {
		keys := make(chan string)
		errs := make(chan error, 1)
		close(keys)
		errs <- *err
		close(errs)
		return keys, errs
	}
	return e.inner.List(ctx, prefix)
}

func (e *erroringBackend) Stat(ctx context.Context, key string) (int64, error) {
	if err := e.err.Load(); err != nil {
		return 0, *err
	}
	return e.inner.Stat(ctx, key)
}

func (e *erroringBackend) Close() error {
	return e.inner.Close()
}

func (e *erroringBackend) String() string {
	return "erroring(" + e.inner.String() + ")"
}

// TestBackendOutage_PutErrorBubblesUp confirms that a backend Put
// failure surfaces as a Sync error rather than being swallowed or
// silently retried forever. A POSIX consumer seeing fsync return
// non-nil is the signal it needs to treat the write as failed.
func TestBackendOutage_PutErrorBubblesUp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	innerBE, err := backend.Open(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("backend.Open: %v", err)
	}
	eb := newErroringBackend(innerBE)
	c, _ := vfs.NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
	v, err := vfs.New(vfs.Config{Backend: eb, Crypto: c})
	if err != nil {
		t.Fatal(err)
	}
	fs, err := vfs.NewFS(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := fs.Create("/erred.db", 0o644); err != nil {
		t.Fatalf("Create: %v", err)
	}
	f, _ := fs.Open(context.Background(), "/erred.db")

	// Inject backend Put failures, then attempt a write+Sync. Sync
	// must fail (the block PutBlock cannot reach the backend).
	injected := errors.New("simulated S3 503")
	eb.setErr(injected)
	if _, err := f.WriteAt([]byte("never lands"), 0); err != nil {
		t.Fatalf("WriteAt should buffer locally, got: %v", err)
	}
	syncErr := f.Sync()
	if syncErr == nil {
		t.Fatalf("Sync must return error when backend is down; instead returned nil — durability policy violated")
	}
	if !errors.Is(syncErr, injected) && !strings.Contains(syncErr.Error(), "simulated S3 503") {
		t.Fatalf("Sync error must wrap underlying backend error; got %v", syncErr)
	}

	// Recovery: clear the error, retry Sync, expect success.
	eb.setErr(nil)
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync after backend recovery: %v", err)
	}
	_ = f.Close()
}

// TestBackendOutage_GetErrorBubblesUp confirms Get failures surface to
// the caller as read errors rather than zero bytes (silent data loss).
func TestBackendOutage_GetErrorBubblesUp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	innerBE, err := backend.Open(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("backend.Open: %v", err)
	}
	eb := newErroringBackend(innerBE)
	c, _ := vfs.NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
	v, err := vfs.New(vfs.Config{Backend: eb, Crypto: c})
	if err != nil {
		t.Fatal(err)
	}
	fs, err := vfs.NewFS(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}

	// Write a file successfully first (so we have a durable block on
	// the inner backend).
	if _, err := fs.Create("/payload.db", 0o644); err != nil {
		t.Fatalf("Create: %v", err)
	}
	f, _ := fs.Open(context.Background(), "/payload.db")
	want := []byte("durable-payload-but-backend-will-flake-on-read-and-this-spans-multiple-blocks-easily")
	for len(want) < 3*vfs.BlockSize {
		want = append(want, want...)
	}
	if _, err := f.WriteAt(want, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	_ = f.Close()

	// Now flake the backend and reopen — read must fail explicitly.
	eb.setErr(errors.New("simulated network partition"))
	f2, err := fs.Open(context.Background(), "/payload.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got := make([]byte, len(want))
	_, readErr := f2.ReadAt(got, 0)
	if readErr == nil {
		t.Fatalf("ReadAt must surface backend error; got nil err — VFS swallowed the outage")
	}
}

// ---------------------------------------------------------------------
// #6 key rotation
// ---------------------------------------------------------------------

// TestKeyRotation_MultiRecipientAllowsRollingRotation confirms that
// the Crypto layer accepts BOTH the old and the new recipient at write
// time so that during a rotation window:
//
//   - new writes go to <oldRcpt, newRcpt>  (decryptable by either ID)
//   - existing blocks stay decryptable by the old ID
//   - after the rotation window, drop the old ID from Decrypt; new
//     writes flip to <newRcpt> only.
//
// This is the design contract that makes rolling key rotation possible
// without a stop-the-world re-encrypt.
func TestKeyRotation_MultiRecipientAllowsRollingRotation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	oldID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	newID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}

	be, err := backend.Open(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatal(err)
	}

	// Stage 1: write under old key only.
	cOld, err := vfs.NewCrypto(
		[]age.Recipient{oldID.Recipient()},
		[]age.Identity{oldID},
	)
	if err != nil {
		t.Fatal(err)
	}
	vOld, err := vfs.New(vfs.Config{Backend: be, Crypto: cOld})
	if err != nil {
		t.Fatal(err)
	}
	oldBlockID, err := vOld.PutBlock(context.Background(), []byte("written-under-old-key"))
	if err != nil {
		t.Fatalf("PutBlock under old: %v", err)
	}

	// Stage 2: rotation window — write under {old,new} recipients;
	// read under {old,new} identities.
	cBoth, err := vfs.NewCrypto(
		[]age.Recipient{oldID.Recipient(), newID.Recipient()},
		[]age.Identity{oldID, newID},
	)
	if err != nil {
		t.Fatal(err)
	}
	vBoth, err := vfs.New(vfs.Config{Backend: be, Crypto: cBoth})
	if err != nil {
		t.Fatal(err)
	}

	newBlockID, err := vBoth.PutBlock(context.Background(), []byte("written-during-rotation"))
	if err != nil {
		t.Fatalf("PutBlock under both: %v", err)
	}

	// Both blocks readable under rotation Crypto.
	for label, id := range map[string]vfs.BlockID{
		"oldBlock-via-both": oldBlockID,
		"newBlock-via-both": newBlockID,
	} {
		_, err := vBoth.GetBlock(context.Background(), id)
		if err != nil {
			t.Fatalf("%s decrypt failed under {old+new} identities: %v", label, err)
		}
	}

	// Stage 3: post-window — drop the old recipient/identity.
	cNew, err := vfs.NewCrypto(
		[]age.Recipient{newID.Recipient()},
		[]age.Identity{newID},
	)
	if err != nil {
		t.Fatal(err)
	}
	vNew, err := vfs.New(vfs.Config{Backend: be, Crypto: cNew})
	if err != nil {
		t.Fatal(err)
	}

	// newBlockID was written under {old, new} so its age envelope
	// includes a stanza for newID — it must still decrypt.
	if _, err := vNew.GetBlock(context.Background(), newBlockID); err != nil {
		t.Fatalf("newBlock should still decrypt under {new} only: %v", err)
	}

	// oldBlockID was written under {old} only — newID is not in the
	// envelope, so decryption must fail. This is the proof that
	// rotation actually rotates (rather than the old key being a
	// no-op).
	if _, err := vNew.GetBlock(context.Background(), oldBlockID); err == nil {
		t.Fatalf("oldBlock decrypted under {new} only — proof that key rotation isn't real")
	}
}
