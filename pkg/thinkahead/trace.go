// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package thinkahead

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/blake3"
)

const (
	// DefaultChunkSize is the logical chunking unit used when mapping byte
	// ranges from OS-level traces into content-addressed VFS chunks (64 KiB
	// aligns with vfs minChunk while keeping fine granularity for small files).
	DefaultChunkSize int64 = 64 << 10

	// TraceWireVersion is the S6 binary trace header version byte.
	TraceWireVersion byte = 0x01

	// MaxTraceEntries caps persisted trace length per docs/prefetch.md S6.2
	// so every trace object fits within the 32 MiB extension object cap.
	MaxTraceEntries = 1_000_000

	// MaxTraceBytes is 1 version byte + MaxTraceEntries * 32-byte ChunkIDs.
	MaxTraceBytes = 1 + MaxTraceEntries*32
)

var (
	// ErrInvalidTrace indicates a malformed or unsupported trace wire payload.
	ErrInvalidTrace = errors.New("thinkahead: invalid trace payload")

	// ErrStraceUnavailable indicates strace could not be executed on this host.
	ErrStraceUnavailable = errors.New("thinkahead: strace unavailable")
)

// ChunkID is a 32-byte BLAKE3 content digest matching vfs chunk identifiers.
type ChunkID [32]byte

// String returns a short hex representation of the chunk ID for diagnostics.
func (id ChunkID) String() string {
	return hex.EncodeToString(id[:8])
}

// Hex returns the full 64-character lowercase hex encoding of the chunk ID.
func (id ChunkID) Hex() string {
	return hex.EncodeToString(id[:])
}

// SumChunk computes the canonical BLAKE3 digest of a chunk's plaintext bytes.
func SumChunk(data []byte) ChunkID {
	return ChunkID(blake3.Sum256(data))
}

// ScopeKey strictly scopes learned traces and prefetched cache entries to one
// tenant, repository, and workflow. Cross-tenant sharing is forbidden so neither
// trace contents nor cache timing can leak across tenants.
type ScopeKey struct {
	TenantID     string `json:"tenant_id"`
	RepoHash     string `json:"repo_hash"`
	WorkflowName string `json:"workflow_name"`
}

// DeriveKey computes a domain-separated 32-byte BLAKE3 key for the scope.
func (s ScopeKey) DeriveKey() [32]byte {
	h := blake3.New()
	_, _ = h.WriteString("hanzoai/vfs thinkahead scope v1\x00")
	writeLenPrefixed(h, s.TenantID)
	writeLenPrefixed(h, s.RepoHash)
	writeLenPrefixed(h, s.WorkflowName)
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// WorkflowExtKey returns the opaque per-workflow extension key passed to
// Volume.PutExt("trace", ...). Because Volume.PutExt further hashes this key
// with the Volume's secret Seed, no tenant or workflow string is exposed to S3.
func (s ScopeKey) WorkflowExtKey() []byte {
	k := s.DeriveKey()
	return k[:]
}

func writeLenPrefixed(w io.Writer, s string) {
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s)))
	_, _ = w.Write(lenBuf[:])
	_, _ = io.WriteString(w, s)
}

// TraceProvenance transparently identifies how a trace was collected.
type TraceProvenance string

const (
	// ProvenanceLiveStraceVM marks a trace captured live on this Linux VM by
	// running the real command under strace and hashing accessed file ranges.
	ProvenanceLiveStraceVM TraceProvenance = "live-strace-vm"

	// ProvenanceManifestReplay marks a deterministic trace synthesized from
	// real toolchain manifest / dependency-graph profiles when an offline
	// registry cache is not pre-populated on the host.
	ProvenanceManifestReplay TraceProvenance = "manifest-replay"
)

// ChunkAccess is one chunk-level read event in a workflow trace.
type ChunkAccess struct {
	Seq        uint64  `json:"seq"`
	TimeNs     int64   `json:"time_ns"`
	ThreadID   int32   `json:"tid"`
	FileID     uint64  `json:"file_id"`
	ChunkIdx   int64   `json:"chunk_idx"`
	Length     int32   `json:"length"`
	ChunkID    ChunkID `json:"chunk_id"`
	PackID     uint32  `json:"pack_id"`
	PackOffset int64   `json:"pack_offset"`
	// FileHint is optional diagnostic metadata kept only in local datasets and
	// NEVER serialized into S6 sealed Volume trace objects.
	FileHint string `json:"file_hint,omitempty"`
}

// WorkflowTrace records one complete run of a coding workflow.
type WorkflowTrace struct {
	Scope       ScopeKey        `json:"scope"`
	RunID       string          `json:"run_id"`
	Provenance  TraceProvenance `json:"provenance"`
	Command     string          `json:"command"`
	ChunkSize   int64           `json:"chunk_size"`
	RecordedAt  time.Time       `json:"recorded_at"`
	DurationNs  int64           `json:"duration_ns"`
	Events      []ChunkAccess   `json:"events,omitempty"`
	UniqueOrder []ChunkID       `json:"unique_order,omitempty"`
	TotalBytes  int64           `json:"total_bytes"`
	UniqueBytes int64           `json:"unique_bytes"`
	UniqueFiles int             `json:"unique_files"`
}

// Finalize computes UniqueOrder, TotalBytes, UniqueBytes, and UniqueFiles from
// Events.
func (t *WorkflowTrace) Finalize() {
	seen := make(map[ChunkID]struct{}, len(t.Events))
	files := make(map[uint64]struct{}, 128)
	t.UniqueOrder = t.UniqueOrder[:0]
	t.TotalBytes = 0
	t.UniqueBytes = 0
	for i := range t.Events {
		ev := &t.Events[i]
		ev.Seq = uint64(i)
		t.TotalBytes += int64(ev.Length)
		files[ev.FileID] = struct{}{}
		if _, ok := seen[ev.ChunkID]; !ok {
			seen[ev.ChunkID] = struct{}{}
			t.UniqueOrder = append(t.UniqueOrder, ev.ChunkID)
			t.UniqueBytes += int64(ev.Length)
		}
	}
	t.UniqueFiles = len(files)
}

// EncodeTraceWire encodes a first-touch ordered list of ChunkIDs into the
// docs/prefetch.md S6.2 binary format: version byte 0x01 followed by at most
// MaxTraceEntries (1,000,000) 32-byte chunk IDs. No paths or plaintext metadata
// are included.
func EncodeTraceWire(ids []ChunkID) []byte {
	n := len(ids)
	if n > MaxTraceEntries {
		n = MaxTraceEntries
	}
	out := make([]byte, 1+n*32)
	out[0] = TraceWireVersion
	for i := 0; i < n; i++ {
		copy(out[1+i*32:1+(i+1)*32], ids[i][:])
	}
	return out
}

// DecodeTraceWire parses an S6.2 binary trace payload, refusing unknown
// version bytes, payloads exceeding MaxTraceBytes, or misaligned tails.
func DecodeTraceWire(raw []byte) ([]ChunkID, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ErrInvalidTrace)
	}
	if raw[0] != TraceWireVersion {
		return nil, fmt.Errorf("%w: unsupported version 0x%02x", ErrInvalidTrace, raw[0])
	}
	if len(raw) > MaxTraceBytes {
		return nil, fmt.Errorf("%w: payload %d exceeds max %d", ErrInvalidTrace, len(raw), MaxTraceBytes)
	}
	body := raw[1:]
	if len(body)%32 != 0 {
		return nil, fmt.Errorf("%w: body length %d not a multiple of 32", ErrInvalidTrace, len(body))
	}
	n := len(body) / 32
	out := make([]ChunkID, n)
	for i := 0; i < n; i++ {
		copy(out[i][:], body[i*32:(i+1)*32])
	}
	return out, nil
}

// LiveTraceConfig controls real OS-level trace capture via strace.
type LiveTraceConfig struct {
	Scope      ScopeKey
	RunID      string
	Dir        string
	Command    []string
	Env        []string
	ChunkSize  int64
	MaxEvents  int
	Timeout    time.Duration
	PathFilter func(path string) bool
}

var (
	reStracePID  = regexp.MustCompile(`^(?:\[pid\s+(\d+)\]|(\d+))\s+(.*)$`)
	reStraceTime = regexp.MustCompile(`^(\d+\.\d+)\s+(.*)$`)
	reStraceCall = regexp.MustCompile(`^([a-z0-9_]+)\((.*)\)\s+=\s+(-?\d+|0x[0-9a-fA-F]+)`)
	reFDPath     = regexp.MustCompile(`(\d+)<([^>]+)>`)
)

// CollectLiveTrace executes cfg.Command under Linux strace, captures real file
// read and mmap access events, hashes accessed file chunks with BLAKE3, and
// returns a WorkflowTrace tagged ProvenanceLiveStraceVM.
func CollectLiveTrace(ctx context.Context, cfg LiveTraceConfig) (*WorkflowTrace, error) {
	if len(cfg.Command) == 0 {
		return nil, errors.New("thinkahead: LiveTraceConfig.Command is empty")
	}
	straceBin, err := exec.LookPath("strace")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStraceUnavailable, err)
	}
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	traceFile, err := os.CreateTemp("", "thinkahead-strace-*.log")
	if err != nil {
		return nil, fmt.Errorf("thinkahead: create temp strace log: %w", err)
	}
	tracePath := traceFile.Name()
	_ = traceFile.Close()
	defer os.Remove(tracePath)

	args := []string{
		"-f",
		"-ttt",
		"-y",
		"-s", "0",
		"-e", "trace=openat,read,pread64,lseek,mmap,close",
		"-o", tracePath,
		"--",
	}
	args = append(args, cfg.Command...)

	cmd := exec.CommandContext(runCtx, straceBin, args...)
	cmd.Dir = cfg.Dir
	if len(cfg.Env) > 0 {
		cmd.Env = append(os.Environ(), cfg.Env...)
	}
	// Discard command stdout/stderr so strace log remains clean and fast.
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	start := time.Now()
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("thinkahead: traced command %q failed: %w", strings.Join(cfg.Command, " "), err)
	}
	dur := time.Since(start)

	f, err := os.Open(tracePath)
	if err != nil {
		return nil, fmt.Errorf("thinkahead: open strace log: %w", err)
	}
	defer f.Close()

	tr, err := parseStraceStream(f, cfg, chunkSize, dur)
	if err != nil {
		return nil, err
	}
	tr.RecordedAt = start.UTC()
	return tr, nil
}

func isPseudoPath(p string) bool {
	if p == "" || p[0] != '/' {
		return true
	}
	for _, pfx := range []string{"/proc/", "/sys/", "/dev/", "/run/", "pipe:", "socket:", "anon_inode:"} {
		if strings.HasPrefix(p, pfx) {
			return true
		}
	}
	return false
}

func parseStraceStream(r io.Reader, cfg LiveTraceConfig, chunkSize int64, dur time.Duration) (*WorkflowTrace, error) {
	tr := &WorkflowTrace{
		Scope:      cfg.Scope,
		RunID:      cfg.RunID,
		Provenance: ProvenanceLiveStraceVM,
		Command:    strings.Join(cfg.Command, " "),
		ChunkSize:  chunkSize,
		DurationNs: dur.Nanoseconds(),
	}

	type fdKey struct {
		pid int32
		fd  int
	}
	fdOffsets := make(map[fdKey]int64, 256)
	fileIDs := make(map[string]uint64, 512)
	// Cache real chunk digests per (path, chunkIdx) to avoid re-reading disk.
	chunkDigestCache := make(map[string]ChunkID, 1024)
	chunkLenCache := make(map[string]int32, 1024)

	var baseSec float64
	hasBase := false

	getFileID := func(path string) uint64 {
		if id, ok := fileIDs[path]; ok {
			return id
		}
		sum := blake3.Sum256([]byte("file:" + path))
		id := binary.LittleEndian.Uint64(sum[:8])
		if id == 0 {
			id = 1
		}
		fileIDs[path] = id
		return id
	}

	resolveChunk := func(path string, idx int64, fallbackLen int32) (ChunkID, int32) {
		key := path + "#" + strconv.FormatInt(idx, 10)
		if id, ok := chunkDigestCache[key]; ok {
			return id, chunkLenCache[key]
		}
		var data []byte
		if f, err := os.Open(path); err == nil {
			buf := make([]byte, chunkSize)
			n, _ := f.ReadAt(buf, idx*chunkSize)
			_ = f.Close()
			if n > 0 {
				data = buf[:n]
			}
		}
		var cid ChunkID
		var clen int32
		if len(data) > 0 {
			cid = SumChunk(data)
			clen = int32(len(data))
		} else {
			// Ephemeral file removed before post-pass: derive stable digest from
			// normalized relative path + chunk index so repeat runs stay aligned.
			norm := path
			if cfg.Dir != "" {
				if rel, err := filepath.Rel(cfg.Dir, path); err == nil && !strings.HasPrefix(rel, "..") {
					norm = rel
				}
			}
			cid = SumChunk([]byte("ephemeral:" + norm + "#" + strconv.FormatInt(idx, 10)))
			clen = fallbackLen
			if clen <= 0 {
				clen = int32(chunkSize)
			}
		}
		chunkDigestCache[key] = cid
		chunkLenCache[key] = clen
		return cid, clen
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 2<<20)

	// Track per-run emitted (fileID, chunkIdx) within a short dedup window to
	// mirror the guest page cache filtering redundant immediate re-reads.
	lastFileChunk := make(map[uint64]int64, 512)
	packCursor := int64(0)
	packID := uint32(1)
	const packLimit = 16 << 20

	for sc.Scan() {
		if cfg.MaxEvents > 0 && len(tr.Events) >= cfg.MaxEvents {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "---") {
			continue
		}
		var pid int32 = 1
		if m := reStracePID.FindStringSubmatch(line); m != nil {
			pidStr := m[1]
			if pidStr == "" {
				pidStr = m[2]
			}
			if p, err := strconv.Atoi(pidStr); err == nil {
				pid = int32(p)
			}
			line = m[3]
		}
		var tsNs int64
		if m := reStraceTime.FindStringSubmatch(line); m != nil {
			if sec, err := strconv.ParseFloat(m[1], 64); err == nil {
				if !hasBase {
					baseSec = sec
					hasBase = true
				}
				tsNs = int64((sec - baseSec) * 1e9)
				if tsNs < 0 {
					tsNs = 0
				}
			}
			line = m[2]
		}
		m := reStraceCall.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		call, argsStr, retStr := m[1], m[2], m[3]
		if strings.HasPrefix(retStr, "-") {
			continue
		}

		emitRange := func(path string, off, n int64) {
			if n <= 0 || isPseudoPath(path) {
				return
			}
			if cfg.PathFilter != nil && !cfg.PathFilter(path) {
				return
			}
			fid := getFileID(path)
			startIdx := off / chunkSize
			endIdx := (off + n - 1) / chunkSize
			for idx := startIdx; idx <= endIdx; idx++ {
				if prev, ok := lastFileChunk[fid]; ok && prev == idx {
					continue
				}
				lastFileChunk[fid] = idx
				cid, clen := resolveChunk(path, idx, int32(min(n, chunkSize)))
				if packCursor+int64(clen) > packLimit {
					packID++
					packCursor = 0
				}
				tr.Events = append(tr.Events, ChunkAccess{
					TimeNs:     tsNs,
					ThreadID:   pid,
					FileID:     fid,
					ChunkIdx:   idx,
					Length:     clen,
					ChunkID:    cid,
					PackID:     packID,
					PackOffset: packCursor,
					FileHint:   filepath.Base(path),
				})
				packCursor += int64(clen)
				if cfg.MaxEvents > 0 && len(tr.Events) >= cfg.MaxEvents {
					break
				}
			}
		}

		switch call {
		case "close":
			if fm := reFDPath.FindStringSubmatch(argsStr); fm != nil {
				if fd, err := strconv.Atoi(fm[1]); err == nil {
					delete(fdOffsets, fdKey{pid, fd})
				}
			}
		case "lseek":
			if fm := reFDPath.FindStringSubmatch(argsStr); fm != nil {
				fd, err1 := strconv.Atoi(fm[1])
				off, err2 := strconv.ParseInt(retStr, 10, 64)
				if err1 == nil && err2 == nil && off >= 0 {
					fdOffsets[fdKey{pid, fd}] = off
				}
			}
		case "mmap":
			// mmap(addr, length, prot, flags, fd<path>, offset)
			parts := strings.Split(argsStr, ",")
			if len(parts) < 6 {
				continue
			}
			fm := reFDPath.FindStringSubmatch(parts[4])
			if fm == nil {
				continue
			}
			length, err1 := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			off, err2 := strconv.ParseInt(strings.TrimSpace(parts[5]), 0, 64)
			if err1 != nil || err2 != nil || length <= 0 || off < 0 {
				continue
			}
			// Cap single mmap expansion to 4 MiB so large library mappings stay bounded.
			if length > 4<<20 {
				length = 4 << 20
			}
			emitRange(fm[2], off, length)
		case "read", "pread64":
			n, err := strconv.ParseInt(retStr, 10, 64)
			if err != nil || n <= 0 {
				continue
			}
			fm := reFDPath.FindStringSubmatch(argsStr)
			if fm == nil {
				continue
			}
			fd, err := strconv.Atoi(fm[1])
			if err != nil {
				continue
			}
			path := fm[2]
			var off int64
			if call == "pread64" {
				// pread64(fd<path>, "", count, offset)
				parts := strings.Split(argsStr, ",")
				if len(parts) >= 4 {
					if parsed, perr := strconv.ParseInt(strings.TrimSpace(parts[len(parts)-1]), 10, 64); perr == nil && parsed >= 0 {
						off = parsed
					}
				}
			} else {
				off = fdOffsets[fdKey{pid, fd}]
				fdOffsets[fdKey{pid, fd}] = off + n
			}
			emitRange(path, off, n)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("thinkahead: scan strace log: %w", err)
	}
	tr.Finalize()
	return tr, nil
}

// WorkflowProfile defines realistic manifest-grounded access characteristics
// for repeatable coding workflows when generating deterministic trace pairs.
type WorkflowProfile struct {
	Name             string
	Command          string
	Files            int
	AvgChunksPerFile int
	ChunkSize        int64
	ParallelThreads  int
	InterArrivalUs   int64
	Sequentiality    float64 // fraction of reads advancing sequentially inside one file
	OrderRepeat      float64 // fraction of consecutive order preserved across parallel runs
	DefaultEditChurn float64 // fraction of chunks mutated in a 1-5 file edit
}

// DefaultWorkflowProfiles returns grounded profiles for the six workflows named
// in hanzoai/vfs#18, calibrated to the empirical characteristics in
// docs/prefetch.md (sequentiality, parallel reordering, working-set shape).
func DefaultWorkflowProfiles() []WorkflowProfile {
	return []WorkflowProfile{
		{
			Name:             "npm_ci",
			Command:          "npm ci --offline --ignore-scripts",
			Files:            160,
			AvgChunksPerFile: 2,
			ChunkSize:        64 << 10,
			ParallelThreads:  4,
			InterArrivalUs:   450,
			Sequentiality:    0.32,
			OrderRepeat:      0.86,
			DefaultEditChurn: 0.015,
		},
		{
			Name:             "pnpm_install",
			Command:          "pnpm install --offline --frozen-lockfile",
			Files:            180,
			AvgChunksPerFile: 2,
			ChunkSize:        64 << 10,
			ParallelThreads:  6,
			InterArrivalUs:   380,
			Sequentiality:    0.28,
			OrderRepeat:      0.79,
			DefaultEditChurn: 0.015,
		},
		{
			Name:             "go_build_test",
			Command:          "go test -count=1 ./...",
			Files:            220,
			AvgChunksPerFile: 2,
			ChunkSize:        64 << 10,
			ParallelThreads:  8,
			InterArrivalUs:   320,
			Sequentiality:    0.24,
			OrderRepeat:      0.12, // parallel compiler/test workers reorder heavily
			DefaultEditChurn: 0.020,
		},
		{
			Name:             "cargo_check",
			Command:          "cargo check --offline --locked --all-targets",
			Files:            190,
			AvgChunksPerFile: 2,
			ChunkSize:        64 << 10,
			ParallelThreads:  6,
			InterArrivalUs:   420,
			Sequentiality:    0.26,
			OrderRepeat:      0.72,
			DefaultEditChurn: 0.018,
		},
		{
			Name:             "uv_sync",
			Command:          "uv sync --offline --frozen",
			Files:            120,
			AvgChunksPerFile: 2,
			ChunkSize:        64 << 10,
			ParallelThreads:  4,
			InterArrivalUs:   400,
			Sequentiality:    0.30,
			OrderRepeat:      0.82,
			DefaultEditChurn: 0.012,
		},
		{
			Name:             "git_checkout",
			Command:          "git checkout HEAD~1 -- .",
			Files:            140,
			AvgChunksPerFile: 2,
			ChunkSize:        64 << 10,
			ParallelThreads:  2,
			InterArrivalUs:   360,
			Sequentiality:    0.45,
			OrderRepeat:      0.95,
			DefaultEditChurn: 0.025,
		},
	}
}

// GenerateManifestTrace synthesizes a deterministic, content-addressed workflow
// trace grounded in profile, explicitly tagged ProvenanceManifestReplay.
// It also returns the deterministic chunk payload bytes keyed by ChunkID so
// callers can populate a real ChunkObjectStore / vfs.Volume for replay.
func GenerateManifestTrace(scope ScopeKey, profile WorkflowProfile, runID string, seed int64) (*WorkflowTrace, map[ChunkID][]byte) {
	rng := rand.New(rand.NewSource(seed))
	chunkSize := profile.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	tr := &WorkflowTrace{
		Scope:      scope,
		RunID:      runID,
		Provenance: ProvenanceManifestReplay,
		Command:    profile.Command,
		ChunkSize:  chunkSize,
		RecordedAt: time.Now().UTC(),
	}

	payloads := make(map[ChunkID][]byte, profile.Files*profile.AvgChunksPerFile)
	type fileSpec struct {
		id     uint64
		name   string
		chunks []ChunkID
		lens   []int32
	}
	files := make([]fileSpec, profile.Files)
	for i := 0; i < profile.Files; i++ {
		nChunks := 1
		if profile.AvgChunksPerFile > 1 {
			nChunks = 1 + rng.Intn(profile.AvgChunksPerFile*2-1)
		}
		name := fmt.Sprintf("%s/pkg-%03d/mod-%03d.dat", profile.Name, i/8, i)
		sum := blake3.Sum256([]byte(scope.RepoHash + ":" + name))
		fid := binary.LittleEndian.Uint64(sum[:8])
		if fid == 0 {
			fid = uint64(i + 1)
		}
		fs := fileSpec{
			id:     fid,
			name:   name,
			chunks: make([]ChunkID, nChunks),
			lens:   make([]int32, nChunks),
		}
		for c := 0; c < nChunks; c++ {
			clen := int(chunkSize)
			if c == nChunks-1 {
				// Realistic tail chunk size between 8 KiB and chunkSize.
				clen = (8 << 10) + rng.Intn(int(chunkSize-(8<<10))+1)
			}
			buf := makeDeterministicChunk(scope.RepoHash, name, c, clen, 0)
			cid := SumChunk(buf)
			fs.chunks[c] = cid
			fs.lens[c] = int32(clen)
			payloads[cid] = buf
		}
		files[i] = fs
	}

	// Build a dependency-graph traversal order: non-sequential across files
	// (matching AST imports, package lockfiles, crate deps) with intra-file
	// sequential bursts controlled by profile.Sequentiality.
	order := rng.Perm(profile.Files)
	packID := uint32(1)
	packOff := int64(0)
	const packLimit = 16 << 20
	var nowNs int64

	threads := max(profile.ParallelThreads, 1)
	interNs := max(profile.InterArrivalUs, 50) * 1000

	for step, fi := range order {
		fs := files[fi]
		tid := int32(1 + (step % threads))
		for c := 0; c < len(fs.chunks); c++ {
			clen := fs.lens[c]
			if packOff+int64(clen) > packLimit {
				packID++
				packOff = 0
			}
			tr.Events = append(tr.Events, ChunkAccess{
				TimeNs:     nowNs,
				ThreadID:   tid,
				FileID:     fs.id,
				ChunkIdx:   int64(c),
				Length:     clen,
				ChunkID:    fs.chunks[c],
				PackID:     packID,
				PackOffset: packOff,
				FileHint:   fs.name,
			})
			packOff += int64(clen)
			nowNs += interNs
		}
	}
	tr.DurationNs = nowNs
	tr.Finalize()
	return tr, payloads
}

// PerturbTrace derives a realistic Run N+1 trace from Run N by:
//  1. Mutating changedFiles distinct files (producing fresh BLAKE3 chunk IDs,
//     simulating a 1-5 file developer edit or dependency bump),
//  2. Reordering concurrent segments according to orderRepeat (1.0 = identical
//     order, 0.1 = heavy parallel worker scheduling jitter as in go build/test).
//
// Any newly created chunk payloads are added to newPayloads so the store can
// serve them on cold demand misses.
func PerturbTrace(base *WorkflowTrace, runID string, changedFiles int, orderRepeat float64, seed int64) (*WorkflowTrace, map[ChunkID][]byte) {
	rng := rand.New(rand.NewSource(seed))
	out := &WorkflowTrace{
		Scope:      base.Scope,
		RunID:      runID,
		Provenance: base.Provenance,
		Command:    base.Command,
		ChunkSize:  base.ChunkSize,
		RecordedAt: time.Now().UTC(),
		DurationNs: base.DurationNs,
		Events:     make([]ChunkAccess, len(base.Events)),
	}
	copy(out.Events, base.Events)

	// Pick changedFiles distinct FileIDs to mutate.
	fileList := make([]uint64, 0, base.UniqueFiles)
	seenFile := make(map[uint64]struct{}, base.UniqueFiles)
	for _, ev := range out.Events {
		if _, ok := seenFile[ev.FileID]; !ok {
			seenFile[ev.FileID] = struct{}{}
			fileList = append(fileList, ev.FileID)
		}
	}
	mutated := make(map[uint64]bool, changedFiles)
	if changedFiles > 0 && len(fileList) > 0 {
		perm := rng.Perm(len(fileList))
		limit := min(changedFiles, len(fileList))
		for i := 0; i < limit; i++ {
			mutated[fileList[perm[i]]] = true
		}
	}

	newPayloads := make(map[ChunkID][]byte)
	mutatedCache := make(map[ChunkID]ChunkID)
	for i := range out.Events {
		ev := &out.Events[i]
		if !mutated[ev.FileID] {
			continue
		}
		if repl, ok := mutatedCache[ev.ChunkID]; ok {
			ev.ChunkID = repl
			continue
		}
		buf := makeDeterministicChunk(base.Scope.RepoHash, fmt.Sprintf("edit-%s-%d", runID, ev.FileID), int(ev.ChunkIdx), int(ev.Length), seed)
		cid := SumChunk(buf)
		mutatedCache[ev.ChunkID] = cid
		newPayloads[cid] = buf
		ev.ChunkID = cid
	}

	// Simulate parallel worker interleaving by shuffling local windows when
	// orderRepeat < 1.0 (preserving causal locality within each window while
	// breaking strict consecutive transitions).
	if orderRepeat < 0.999 && len(out.Events) > 4 {
		window := 8
		if orderRepeat < 0.3 {
			window = 24
		} else if orderRepeat < 0.7 {
			window = 12
		}
		for start := 0; start < len(out.Events); start += window {
			end := min(start+window, len(out.Events))
			if rng.Float64() > orderRepeat {
				seg := out.Events[start:end]
				// Keep timestamps monotonic after permuting event order.
				times := make([]int64, len(seg))
				for i := range seg {
					times[i] = seg[i].TimeNs
				}
				rng.Shuffle(len(seg), func(i, j int) {
					seg[i], seg[j] = seg[j], seg[i]
				})
				for i := range seg {
					seg[i].TimeNs = times[i]
				}
			}
		}
	}

	out.Finalize()
	return out, newPayloads
}

func makeDeterministicChunk(repo, name string, idx, length int, salt int64) []byte {
	h := blake3.New()
	_, _ = fmt.Fprintf(h, "thinkahead-chunk:%s:%s:%d:%d:%d", repo, name, idx, length, salt)
	buf := make([]byte, length)
	h.Digest().Read(buf)
	return buf
}
