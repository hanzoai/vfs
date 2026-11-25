//go:build fuse && darwin

package mount

// syncfs does nothing on macOS: the driver does not hold writes back from the daemon.
func syncfs(string) error { return nil }
