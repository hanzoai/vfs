// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build fuse && linux

package gitvfs

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/mount"
)

// FUSEMountAvailable reports whether this binary was built with -tags fuse on
// Linux and has /dev/fuse + fusermount3 available.
func FUSEMountAvailable() bool {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		return false
	}
	if _, err := exec.LookPath("fusermount3"); err != nil {
		if _, err := exec.LookPath("fusermount"); err != nil {
			return false
		}
	}
	return true
}

// MountMetricsSnapshot is returned by the instrumented FUSE mount server over
// its local loopback control endpoint.
type MountMetricsSnapshot struct {
	Backend       BackendMetrics `json:"backend"`
	Journal       JournalMetrics `json:"journal"`
	VolumeBacklog int64          `json:"volume_backlog"`
	VolumeDirty   int64          `json:"volume_dirty"`
	VolumePending int            `json:"volume_pending"`
	VolumeSeq     uint64         `json:"volume_seq"`
	VolumeDegrade bool           `json:"volume_degraded"`
}

// ServeInstrumentedMount runs a child FUSE server process backing mountpoint
// with a canonical *vfs.Volume instrumented by CountingBackend + JournalTracker.
// It prints a single JSON handshake line {"control_addr":"127.0.0.1:<port>"}
// to stdout once mounted and serves until SIGTERM/SIGINT.
func ServeInstrumentedMount(ctx context.Context, storeDir, baseStoreDir, mountpoint, journalDir string, round uint64, putDelay time.Duration, keyStr string) error {
	id, err := age.ParseHybridIdentity(strings.TrimSpace(keyStr))
	if err != nil {
		return fmt.Errorf("gitvfs: parse identity: %w", err)
	}
	cipher := vfs.Hybrid(id)

	rawDelta, err := openFileBackend(ctx, storeDir)
	if err != nil {
		return fmt.Errorf("gitvfs: open store %s: %w", storeDir, err)
	}
	cntBe := NewCountingBackend(rawDelta)
	cntBe.PutDelay = putDelay

	jt := NewJournalTracker()

	var vol *vfs.Volume
	if baseStoreDir != "" {
		rawBase, err := openFileBackend(ctx, baseStoreDir)
		if err != nil {
			_ = cntBe.Close()
			return fmt.Errorf("gitvfs: open base store %s: %w", baseStoreDir, err)
		}
		cow, err := NewCOWBackend(rawBase, cntBe)
		if err != nil {
			_ = rawBase.Close()
			_ = cntBe.Close()
			return err
		}
		if err := vfs.Reseal(ctx, vfs.Config{Backend: cow, Cipher: cipher, Round: round}, rawBase.String()); err != nil {
			_ = rawBase.Close()
			_ = cntBe.Close()
			return fmt.Errorf("gitvfs: reseal COW mount: %w", err)
		}
		vol, err = vfs.Attach(ctx, vfs.Config{
			Backend:         cow,
			Cipher:          cipher,
			Round:           round + 1,
			Journal:         journalDir,
			JournalCapacity: 256 << 20,
			Bound:           5 * time.Second,
			Disk:            jt.DiskFactory(),
		})
		if err != nil {
			_ = rawBase.Close()
			_ = cntBe.Close()
			return fmt.Errorf("gitvfs: attach COW mount: %w", err)
		}
		defer rawBase.Close()
	} else {
		cfg := vfs.Config{
			Backend:         cntBe,
			Cipher:          cipher,
			Round:           round,
			Journal:         journalDir,
			JournalCapacity: 256 << 20,
			Bound:           5 * time.Second,
			Disk:            jt.DiskFactory(),
		}
		if round == 1 {
			if err := vfs.Create(ctx, cfg); err != nil && !errors.Is(err, os.ErrExist) {
				_ = cntBe.Close()
				return fmt.Errorf("gitvfs: create mount volume: %w", err)
			}
		}
		vol, err = vfs.Attach(ctx, cfg)
		if err != nil {
			_ = cntBe.Close()
			return fmt.Errorf("gitvfs: attach mount volume: %w", err)
		}
	}
	defer vol.Close()

	// Start loopback control HTTP server for live metrics and explicit flush.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer ln.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		st := vol.State()
		vs := vol.Stats()
		snap := MountMetricsSnapshot{
			Backend:       cntBe.Snapshot(),
			Journal:       jt.Snapshot(),
			VolumeBacklog: st.Backlog,
			VolumeDirty:   vs.Dirty,
			VolumePending: vs.Pending,
			VolumeSeq:     vs.Seq,
			VolumeDegrade: st.Degraded,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	})
	mux.HandleFunc("/reset-sync-samples", func(w http.ResponseWriter, r *http.Request) {
		jt.ResetSamples()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/flush", func(w http.ResponseWriter, r *http.Request) {
		if err := vol.Flush(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()

	mountErr := make(chan error, 1)
	go func() {
		mountErr <- mount.Mount(sigCtx, vol, mountpoint)
	}()

	// Wait until mountpoint appears in /proc/self/mountinfo before printing handshake.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if out, _ := os.ReadFile("/proc/self/mountinfo"); strings.Contains(string(out), mountpoint) {
			break
		}
		select {
		case err := <-mountErr:
			return fmt.Errorf("gitvfs: mount exited early: %w", err)
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gitvfs: mountpoint %s never appeared", mountpoint)
		}
		time.Sleep(15 * time.Millisecond)
	}

	handshake, _ := json.Marshal(map[string]string{"control_addr": ln.Addr().String()})
	fmt.Fprintln(os.Stdout, string(handshake))

	err = <-mountErr
	if err != nil && strings.Contains(err.Error(), "transport endpoint") {
		return nil
	}
	return err
}

// MountedVolumeClient connects to a child process running ServeInstrumentedMount.
type MountedVolumeClient struct {
	Cmd         *exec.Cmd
	Mountpoint  string
	ControlAddr string
	Done        chan error
	httpClient  *http.Client
}

// StartMountedVolumeChild spawns binaryPath with serveArgs, passes the age
// identity over stdin, waits for the JSON control_addr handshake, and returns
// a client for querying live backend/journal metrics and cleanly stopping it.
func StartMountedVolumeChild(binaryPath string, serveArgs []string, mountpoint, keyStr string) (*MountedVolumeClient, error) {
	cmd := exec.Command(binaryPath, serveArgs...)
	cmd.Stdin = strings.NewReader(strings.TrimSpace(keyStr) + "\n")
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	lineCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		if sc.Scan() {
			lineCh <- sc.Text()
		}
		close(lineCh)
	}()
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case line, ok := <-lineCh:
		if !ok {
			err := <-done
			return nil, fmt.Errorf("gitvfs: mount server closed stdout early: %w", err)
		}
		var hs struct {
			ControlAddr string `json:"control_addr"`
		}
		if err := json.Unmarshal([]byte(line), &hs); err != nil || hs.ControlAddr == "" {
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("gitvfs: invalid mount handshake %q: %v", line, err)
		}
		return &MountedVolumeClient{
			Cmd:         cmd,
			Mountpoint:  mountpoint,
			ControlAddr: hs.ControlAddr,
			Done:        done,
			httpClient:  &http.Client{Timeout: 30 * time.Second},
		}, nil
	case err := <-done:
		return nil, fmt.Errorf("gitvfs: mount server exited before ready: %w", err)
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		return nil, errors.New("gitvfs: timed out waiting for mount server")
	}
}

// Metrics fetches the current live backend, journal, and volume state snapshot.
func (c *MountedVolumeClient) Metrics() (MountMetricsSnapshot, error) {
	resp, err := c.httpClient.Get("http://" + c.ControlAddr + "/metrics")
	if err != nil {
		return MountMetricsSnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return MountMetricsSnapshot{}, fmt.Errorf("gitvfs: metrics status %d", resp.StatusCode)
	}
	var snap MountMetricsSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return MountMetricsSnapshot{}, err
	}
	return snap, nil
}

// Flush triggers vol.Flush() on the mounted Volume.
func (c *MountedVolumeClient) Flush() error {
	resp, err := c.httpClient.Post("http://"+c.ControlAddr+"/flush", "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("gitvfs: flush status %d", resp.StatusCode)
	}
	return nil
}

// Stop sends SIGTERM to unmount, flush, and cleanly exit the mount server.
func (c *MountedVolumeClient) Stop() error {
	_ = c.Cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-c.Done:
		return err
	case <-time.After(25 * time.Second):
		_ = exec.Command("fusermount3", "-u", "-z", c.Mountpoint).Run()
		_ = c.Cmd.Process.Kill()
		return errors.New("gitvfs: timed out stopping mount server")
	}
}
