// Package mount provides FUSE mounting capabilities for VFS.
// Modified to support OCI-style socket FD mounting.
package mount

import (
	"fmt"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// MountSocket mounts the VFS using a provided file descriptor.
// This is used for OCI-compatible mounts where the FD is passed by the host.
func MountSocket(fd int, mountpoint string, server fs.FS) error {
	// Convert the raw FD to an os.File
	f := os.NewFile(uintptr(fd), "/dev/fuse")
	
	// Create a fuse connection from the file
	conn, err := fuse.NewConn(f)
	if err != nil {
		return fmt.Errorf("failed to create fuse connection from fd %d: %w", fd, err)
	}
	defer conn.Close()

	// Check if the connection is ready
	<-conn.Ready
	if err := conn.MountError; err != nil {
		return fmt.Errorf("mount error: %w", err)
	}

	// Serve the filesystem
	if err := fs.Serve(conn, server); err != nil {
		return fmt.Errorf("serve error: %w", err)
	}

	return nil
}

// MountStandard performs a traditional mount (requires privileges/dev/fuse access)
func MountStandard(mountpoint string, server fs.FS) error {
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("vfs"),
		fuse.Subtype("vfs"),
		fuse.AllowOther(),
	)
	if err != nil {
		return err
	}
	defer c.Close()

	return fs.Serve(c, server)
}
