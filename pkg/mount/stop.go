//go:build fuse && (linux || darwin)

package mount

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/hanzoai/vfs"
)

const (
	// commitGrace is how long the end of a mount waits for the commit that makes
	// what it holds durable: a store that does not answer does not keep a supervisor's
	// SIGTERM from ever being answered.
	commitGrace = 10 * time.Second
	// busyGrace is how long a mount that something holds open is asked to unmount, every
	// busyRetry, before it is detached.
	busyGrace = 5 * time.Second
	busyRetry = 250 * time.Millisecond
	// exitGrace is how long the mount keeps serving after it was unmounted or detached, so
	// what still holds a file finishes, before Mount returns.
	exitGrace = 3 * time.Second
)

// stop ends a mount whose context was cancelled, and returns what Serve returned and
// whether it ended. It has the kernel hand its cached writes to the daemon (syncfs), and
// publishes what the Volume holds, because nothing after it is certain; asks the
// kernel to unmount, and if the mount is in use (a process has a file open on it, or its
// directory as its own) says so, keeps asking for busyGrace, and then detaches it so that
// the directory is free and the process can end; and waits exitGrace for the kernel to
// let Serve go. What a holder writes after the commit is lost with the connection when
// Mount returns: the caller's supervisor must give a mount in use time to end (SIGTERM
// again does nothing worse), and a SIGKILL while holders write loses what they wrote.
func stop(vol *vfs.Volume, mountpoint string, unmount, detach func(string) error, served <-chan error) (serveErr error, ended bool) {
	committed := make(chan error, 1)
	go func() { committed <- publish(vol, mountpoint) }()
	select {
	case err := <-committed:
		if err != nil {
			fmt.Fprintf(os.Stderr, "vfs mount: commit before unmounting %s: %v\n", mountpoint, err)
		}
	case <-time.After(commitGrace):
		fmt.Fprintf(os.Stderr, "vfs mount: the store did not answer in %s: unmounting %s without the commit\n", commitGrace, mountpoint)
	case err := <-served:
		return err, true
	}
	var busy error
	for deadline := time.Now().Add(busyGrace); ; {
		if busy = unmount(mountpoint); busy == nil {
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		select {
		case err := <-served:
			return err, true
		case <-time.After(busyRetry):
		}
	}
	if busy != nil {
		fmt.Fprintf(os.Stderr, "vfs mount: %s is in use (%v): detaching it\n", mountpoint, busy)
		if err := detach(mountpoint); err != nil {
			fmt.Fprintf(os.Stderr, "vfs mount: detaching %s: %v\n", mountpoint, err)
		}
	}
	select {
	case err := <-served:
		return err, true
	case <-time.After(exitGrace):
		return nil, false
	}
}

// publish makes the store have everything the mount has accepted. The kernel may hold a
// write it acknowledged to its caller (writeback), and the daemon has not seen it, so the
// journal does not have it either: syncfs asks the kernel to hand every such write to the
// daemon first, and only then is the Volume flushed.
func publish(vol *vfs.Volume, mountpoint string) error {
	if err := syncfs(mountpoint); err != nil {
		fmt.Fprintf(os.Stderr, "vfs mount: syncfs %s: %v\n", mountpoint, err)
	}
	return vol.Flush(context.Background())
}
