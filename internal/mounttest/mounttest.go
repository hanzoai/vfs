//go:build fuse && (linux || darwin)

// Package mounttest runs the FUSE front for tests, as a server in a child process.
// Several packages mount a Volume to test what sits on top of it, and none of them
// can serve the mount from its own process: it is a test helper, in no binary.
package mounttest

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	_ "github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/hanzoai/vfs/pkg/mount"
)

// NeedFUSE skips the test where the kernel cannot mount: no /dev/fuse, or no
// helper to unmount with. Everything else runs.
func NeedFUSE(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "linux":
		if _, err := os.Stat("/dev/fuse"); err != nil {
			t.Skip("no /dev/fuse on this machine")
		}
		if _, err := exec.LookPath("fusermount3"); err != nil {
			if _, err := exec.LookPath("fusermount"); err != nil {
				t.Skip("no fusermount on this machine")
			}
		}
	case "darwin":
		_, e1 := os.Stat("/Library/Filesystems/macfuse.fs")
		_, e2 := os.Stat("/Library/Application Support/fuse-t")
		if e1 != nil && e2 != nil {
			t.Skip("neither macFUSE nor fuse-t is installed")
		}
	}
}

func Unmount(mountpoint string) {
	switch runtime.GOOS {
	case "linux":
		if exec.Command("fusermount3", "-u", mountpoint).Run() != nil {
			_ = exec.Command("fusermount", "-u", mountpoint).Run()
		}
	case "darwin":
		_ = exec.Command("umount", mountpoint).Run()
		_ = exec.Command("diskutil", "unmount", mountpoint).Run()
	}
}

// Child is the body of the server process of the mount tests: the test binary run
// again, by TestMountChild, with the arguments "mountchild <backend URL> <mountpoint> <round> <journal directory>" and the key on standard
// input. The store is wrapped so that a call on a nil context panics, as the client of a real store does. A process that mounts a Volume must not use files on its own mount: Go
// registers an open file with its event poller, the kernel asks the file system
// for readiness, and the server, which needs that same poller to read its next
// request, never answers. That deadlock is a property of one process playing both
// sides, not of the mount, and it cannot be undone without root. So the test
// process only ever acts as a client.
func Child(t *testing.T) {
	if flag.Arg(0) != "mountchild" {
		t.Skip("runs only as the server process of the mount tests")
	}
	url, mp := flag.Arg(1), flag.Arg(2)
	round, _ := strconv.ParseUint(flag.Arg(3), 10, 64)
	journals := flag.Arg(4) // the parent's, so that a mount that follows another finds its journal
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	key, err := age.ParseHybridIdentity(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := backend.Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	be := storetest.Strict{Backend: opened}
	cfg := vfs.Config{Backend: be, Cipher: vfs.Hybrid(key), Round: round, Journal: journals, Capacity: 64 << 20}
	if round == 1 {
		if err := vfs.Create(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
	}
	vol, err := vfs.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	if err := mount.Mount(ctx, vol, mp); err != nil && !strings.Contains(err.Error(), "transport endpoint") {
		t.Fatal(err)
	}
}

// Mounted is one mount of a Volume, served by a child process.
type Mounted struct {
	Cmd  *exec.Cmd
	Dir  string
	Done chan error
}

// Stop asks the server to finish: it unmounts, flushes the Volume and exits.
func (m *Mounted) Stop(t *testing.T) {
	t.Helper()
	_ = m.Cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-m.Done:
		if err != nil {
			t.Errorf("the mount server exited: %v", err)
		}
	case <-time.After(30 * time.Second):
		Unmount(m.Dir)
		_ = m.Cmd.Process.Kill()
		t.Fatal("the mount server did not stop")
	}
}

// MountAt starts a server that attaches the Volume at storeDir (creating it at
// round 1) at round and mounts it at mp. The caller stops it.
func MountAt(t *testing.T, storeDir, mp string, key *age.HybridIdentity, round uint64) *Mounted {
	t.Helper()
	return MountURL(t, "file://"+storeDir, nil, mp, key, round)
}

// MountURL is MountAt for any backend URL; env is added to the server's environment
// (the credentials an object store's URL does not carry).
func MountURL(t *testing.T, url string, env []string, mp string, key *age.HybridIdentity, round uint64) *Mounted {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMountChild$", "-test.count=1", "mountchild", url, mp, strconv.FormatUint(round, 10), storetest.Journals())
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdin = strings.NewReader(key.String() + "\n")
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	m := &Mounted{Cmd: cmd, Dir: mp, Done: make(chan error, 1)}
	exited := make(chan struct{})
	go func() { m.Done <- cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		// A test that failed with a file still open on the mount leaves it busy: the
		// first unmount is refused, the server is killed, and the mount, now without
		// a server, is unmounted by the second.
		Unmount(mp)
		_ = cmd.Process.Kill()
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
		}
		Unmount(mp)
	})
	deadline := time.Now().Add(20 * time.Second)
	for {
		if out, _ := os.ReadFile("/proc/self/mountinfo"); runtime.GOOS != "linux" || bytes.Contains(out, []byte(mp)) {
			return m
		}
		select {
		case err := <-m.Done:
			t.Fatalf("the mount server exited before the mount appeared: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the mount never appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func NewMountpoint(t *testing.T) (store, mp string) {
	tmp := t.TempDir()
	mp = filepath.Join(tmp, "mnt")
	if err := os.MkdirAll(mp, 0o755); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(tmp, "store"), mp
}
