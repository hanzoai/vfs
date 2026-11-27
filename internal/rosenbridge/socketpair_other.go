//go:build !unix

package rosenbridge

import (
	"fmt"
	"os"
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
	if e.w != nil {
		if err2 := e.w.Close(); err == nil {
			err = err2
		}
	}
	return err
}

func newGoferIPC() (*ipcEndpoint, *ipcEndpoint, error) {
	r1, w1, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("rosenbridge: pipe1: %w", err)
	}
	r2, w2, err := os.Pipe()
	if err != nil {
		_ = r1.Close()
		_ = w1.Close()
		return nil, nil, fmt.Errorf("rosenbridge: pipe2: %w", err)
	}
	return &ipcEndpoint{r: r2, w: w1}, &ipcEndpoint{r: r1, w: w2}, nil
}
