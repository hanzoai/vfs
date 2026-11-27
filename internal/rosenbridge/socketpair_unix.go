//go:build unix

package rosenbridge

import (
	"fmt"
	"os"
	"syscall"
)

type ipcEndpoint struct {
	r *os.File
	w *os.File
}

func (e *ipcEndpoint) Read(p []byte) (int, error)  { return e.r.Read(p) }
func (e *ipcEndpoint) Write(p []byte) (int, error) { return e.w.Write(p) }
func (e *ipcEndpoint) Close() error {
	var err error
	if e.r != nil {
		err = e.r.Close()
	}
	if e.w != nil && e.w != e.r {
		if err2 := e.w.Close(); err == nil {
			err = err2
		}
	}
	return err
}

// newGoferIPC creates a real OS AF_UNIX SOCK_STREAM socketpair modeling the
// canonical guest<->host gofer IPC boundary (LISAFS / 9P style stream).
func newGoferIPC() (*ipcEndpoint, *ipcEndpoint, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("rosenbridge: socketpair: %w", err)
	}
	syscall.CloseOnExec(fds[0])
	syscall.CloseOnExec(fds[1])
	f0 := os.NewFile(uintptr(fds[0]), "gofer-guest")
	f1 := os.NewFile(uintptr(fds[1]), "gofer-host")
	return &ipcEndpoint{r: f0, w: f0}, &ipcEndpoint{r: f1, w: f1}, nil
}
