// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cetofs

import (
	"context"
	"encoding/hex"
	"errors"
	"io/fs"
	"math"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
)

const (
	// defaultMaxLatencySamples bounds per-op latency slice retention while
	// keeping exact percentiles for all standard benchmark workloads.
	defaultMaxLatencySamples = 250000

	// defaultAuditCoalesceGap is the gap threshold (256 KiB) used by the audit
	// recorder to classify adjacent GetRange calls on the same pack as
	// uncoalesced RangeGETs.
	defaultAuditCoalesceGap int64 = 256 << 10
)

// LatencyStats summarizes latency distribution in microseconds (and total ms).
type LatencyStats struct {
	Count   int64   `json:"count"`
	MinUs   float64 `json:"min_us"`
	P50Us   float64 `json:"p50_us"`
	P95Us   float64 `json:"p95_us"`
	P99Us   float64 `json:"p99_us"`
	MaxUs   float64 `json:"max_us"`
	MeanUs  float64 `json:"mean_us"`
	TotalMs float64 `json:"total_ms"`
}

type latencyRecorder struct {
	count   int64
	sumNs   int64
	minNs   int64
	maxNs   int64
	samples []int64
}

func (r *latencyRecorder) add(d time.Duration) {
	ns := d.Nanoseconds()
	if ns < 0 {
		ns = 0
	}
	if r.count == 0 {
		r.minNs = ns
		r.maxNs = ns
	} else {
		if ns < r.minNs {
			r.minNs = ns
		}
		if ns > r.maxNs {
			r.maxNs = ns
		}
	}
	r.count++
	r.sumNs += ns
	if len(r.samples) < defaultMaxLatencySamples {
		r.samples = append(r.samples, ns)
	} else {
		// Deterministic stride replacement when sample count exceeds cap.
		idx := int((r.count - 1) % int64(defaultMaxLatencySamples))
		r.samples[idx] = ns
	}
}

func (r *latencyRecorder) snapshot() LatencyStats {
	if r.count == 0 {
		return LatencyStats{}
	}
	cp := make([]int64, len(r.samples))
	copy(cp, r.samples)
	slices.Sort(cp)
	pct := func(p float64) float64 {
		if len(cp) == 0 {
			return 0
		}
		idx := int(math.Ceil(p*float64(len(cp)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(cp) {
			idx = len(cp) - 1
		}
		return float64(cp[idx]) / 1e3
	}
	return LatencyStats{
		Count:   r.count,
		MinUs:   float64(r.minNs) / 1e3,
		P50Us:   pct(0.50),
		P95Us:   pct(0.95),
		P99Us:   pct(0.99),
		MaxUs:   float64(r.maxNs) / 1e3,
		MeanUs:  (float64(r.sumNs) / float64(r.count)) / 1e3,
		TotalMs: float64(r.sumNs) / 1e6,
	}
}

// BoundarySnapshot is an immutable point-in-time report of recorded boundary
// operations, latencies, byte volumes, and classified redundancies.
type BoundarySnapshot struct {
	OpCounts                map[OpType]int64          `json:"op_counts"`
	OpLatencies             map[OpType]LatencyStats   `json:"op_latencies"`
	RedundancyCounts        map[RedundancyClass]int64 `json:"redundancy_counts"`
	BytesRead               int64                     `json:"bytes_read"`
	BytesWritten            int64                     `json:"bytes_written"`
	BackendBytesRead        int64                     `json:"backend_bytes_read"`
	BackendBytesWritten     int64                     `json:"backend_bytes_written"`
	TotalRequests           int64                     `json:"total_requests"`
	FUSEBoundaryRequests    int64                     `json:"fuse_boundary_requests"`
	BackendBoundaryRequests int64                     `json:"backend_boundary_requests"`
	TotalRedundant          int64                     `json:"total_redundant"`
	FUSERedundant           int64                     `json:"fuse_redundant"`
	BackendRedundant        int64                     `json:"backend_redundant"`
}

type lookupPair struct {
	dir  vfs.InodeID
	name string
}

type rangeWindow struct {
	off int64
	end int64
}

// AuditRecorder tracks every request crossing BoundaryFUSEGofer and
// BoundaryVFSBackend, records latency distributions, and classifies redundant
// traffic without changing execution semantics.
type AuditRecorder struct {
	mu sync.Mutex

	opCounts   map[OpType]int64
	opLatency  map[OpType]*latencyRecorder
	redCounts  map[RedundancyClass]int64
	bytesRead  int64
	bytesWrite int64
	beRead     int64
	beWrite    int64

	// Boundary 1 (FUSE/Gofer -> VFS) state for redundancy classification.
	inodeGen      map[vfs.InodeID]uint64
	dirGen        map[vfs.InodeID]uint64
	seenAttrGen   map[vfs.InodeID]uint64
	seenLookupGen map[lookupPair]uint64
	seenDirGen    map[vfs.InodeID]uint64

	// Boundary 2 (VFS -> S3/Backend) state for redundancy classification.
	coalesceGap          int64
	lastHeadKnown        bool
	lastHeadVer          backend.Version
	pendingPrePutHeadGet bool
	seenShards           map[string]bool
	seenShardHead        map[string]bool
	seenList             map[string]bool
	lastPackRange        map[string]rangeWindow
}

// NewAuditRecorder builds a fresh, thread-safe AuditRecorder.
func NewAuditRecorder() *AuditRecorder {
	return &AuditRecorder{
		opCounts:      make(map[OpType]int64),
		opLatency:     make(map[OpType]*latencyRecorder),
		redCounts:     make(map[RedundancyClass]int64),
		inodeGen:      make(map[vfs.InodeID]uint64),
		dirGen:        make(map[vfs.InodeID]uint64),
		seenAttrGen:   make(map[vfs.InodeID]uint64),
		seenLookupGen: make(map[lookupPair]uint64),
		seenDirGen:    make(map[vfs.InodeID]uint64),
		coalesceGap:   defaultAuditCoalesceGap,
		seenShards:    make(map[string]bool),
		seenShardHead: make(map[string]bool),
		seenList:      make(map[string]bool),
		lastPackRange: make(map[string]rangeWindow),
	}
}

// Reset zeroes all counters, latency samples, and redundancy state.
func (a *AuditRecorder) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.opCounts = make(map[OpType]int64)
	a.opLatency = make(map[OpType]*latencyRecorder)
	a.redCounts = make(map[RedundancyClass]int64)
	a.bytesRead = 0
	a.bytesWrite = 0
	a.beRead = 0
	a.beWrite = 0
	a.inodeGen = make(map[vfs.InodeID]uint64)
	a.dirGen = make(map[vfs.InodeID]uint64)
	a.seenAttrGen = make(map[vfs.InodeID]uint64)
	a.seenLookupGen = make(map[lookupPair]uint64)
	a.seenDirGen = make(map[vfs.InodeID]uint64)
	a.lastHeadKnown = false
	a.lastHeadVer = ""
	a.pendingPrePutHeadGet = false
	a.seenShards = make(map[string]bool)
	a.seenShardHead = make(map[string]bool)
	a.seenList = make(map[string]bool)
	a.lastPackRange = make(map[string]rangeWindow)
}

func (a *AuditRecorder) recordOpLocked(op OpType, d time.Duration) {
	a.opCounts[op]++
	lr := a.opLatency[op]
	if lr == nil {
		lr = &latencyRecorder{}
		a.opLatency[op] = lr
	}
	lr.add(d)
}

// RecordGetAttr records a FUSE/Gofer -> VFS Stat/GetAttr crossing.
func (a *AuditRecorder) RecordGetAttr(ino vfs.InodeID, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpGetAttr, d)
	if err == nil {
		gen := a.inodeGen[ino]
		if prev, ok := a.seenAttrGen[ino]; ok && prev == gen {
			a.redCounts[RedundancyRepeatedGetAttr]++
		}
		a.seenAttrGen[ino] = gen
	}
}

// RecordOpenStat records the pre-Open Stat check (mirroring pkg/mount/fuse_unix.go:450).
func (a *AuditRecorder) RecordOpenStat(ino vfs.InodeID, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpGetAttr, d)
	if err == nil {
		gen := a.inodeGen[ino]
		if prev, ok := a.seenAttrGen[ino]; ok && prev == gen {
			a.redCounts[RedundancyRedundantOpenStat]++
		}
		a.seenAttrGen[ino] = gen
	}
}

// RecordLookup records a FUSE/Gofer -> VFS Lookup(dir, name) crossing.
func (a *AuditRecorder) RecordLookup(dir vfs.InodeID, name string, childIno vfs.InodeID, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpLookup, d)
	if err == nil {
		key := lookupPair{dir: dir, name: name}
		gen := a.dirGen[dir]
		if prev, ok := a.seenLookupGen[key]; ok && prev == gen {
			a.redCounts[RedundancyRepeatedLookup]++
		}
		a.seenLookupGen[key] = gen
		// Lookup also returns the child's attributes.
		a.seenAttrGen[childIno] = a.inodeGen[childIno]
	}
}

// RecordReadDir records a FUSE/Gofer -> VFS ReadDir(dir) crossing.
func (a *AuditRecorder) RecordReadDir(dir vfs.InodeID, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpReadDir, d)
	if err == nil {
		gen := a.dirGen[dir]
		if prev, ok := a.seenDirGen[dir]; ok && prev == gen {
			a.redCounts[RedundancyRepeatedReadDir]++
		}
		a.seenDirGen[dir] = gen
	}
}

// RecordOpen records a FUSE/Gofer -> VFS Open crossing.
func (a *AuditRecorder) RecordOpen(_ vfs.InodeID, d time.Duration, _ error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpOpen, d)
}

// RecordRead records a FUSE/Gofer -> VFS ReadAt crossing.
func (a *AuditRecorder) RecordRead(_ vfs.InodeID, n int, d time.Duration, _ error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpRead, d)
	if n > 0 {
		a.bytesRead += int64(n)
	}
}

// RecordWrite records a FUSE/Gofer -> VFS WriteAt crossing.
func (a *AuditRecorder) RecordWrite(ino vfs.InodeID, n int, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpWrite, d)
	if n > 0 {
		a.bytesWrite += int64(n)
	}
	if err == nil {
		a.inodeGen[ino]++
	}
}

// RecordCreate records a FUSE/Gofer -> VFS Create crossing.
func (a *AuditRecorder) RecordCreate(dir vfs.InodeID, name string, childIno vfs.InodeID, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpCreate, d)
	if err == nil {
		a.dirGen[dir]++
		a.inodeGen[dir]++
		a.inodeGen[childIno]++
		delete(a.seenLookupGen, lookupPair{dir: dir, name: name})
	}
}

// RecordMkdir records a FUSE/Gofer -> VFS Mkdir crossing.
func (a *AuditRecorder) RecordMkdir(dir vfs.InodeID, name string, childIno vfs.InodeID, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpMkdir, d)
	if err == nil {
		a.dirGen[dir]++
		a.inodeGen[dir]++
		a.inodeGen[childIno]++
		delete(a.seenLookupGen, lookupPair{dir: dir, name: name})
	}
}

// RecordSetAttr records a FUSE/Gofer -> VFS SetAttr crossing.
func (a *AuditRecorder) RecordSetAttr(ino vfs.InodeID, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpSetAttr, d)
	if err == nil {
		a.inodeGen[ino]++
	}
}

// RecordRemove records a FUSE/Gofer -> VFS Remove crossing.
func (a *AuditRecorder) RecordRemove(dir vfs.InodeID, name string, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpRemove, d)
	if err == nil {
		a.dirGen[dir]++
		a.inodeGen[dir]++
		delete(a.seenLookupGen, lookupPair{dir: dir, name: name})
	}
}

// RecordFlush records a FUSE/Gofer -> VFS Flush crossing.
func (a *AuditRecorder) RecordFlush(d time.Duration, _ error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpFlush, d)
}

// RecordBackendGet records a VFS -> Backend Get crossing.
func (a *AuditRecorder) RecordBackendGet(key string, ver backend.Version, n int, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpBackendGet, d)
	if n > 0 {
		a.beRead += int64(n)
	}
	if err != nil {
		return
	}
	switch {
	case key == "head":
		if a.lastHeadKnown && ver != "" && ver == a.lastHeadVer {
			a.redCounts[RedundancyRepeatedHeadRead]++
			a.pendingPrePutHeadGet = true
		} else {
			a.pendingPrePutHeadGet = false
		}
		a.lastHeadVer = ver
		a.lastHeadKnown = ver != ""
	case isImmutableShardKey(key):
		if a.seenShards[key] {
			a.redCounts[RedundancyRepeatedShardRead]++
		}
		a.seenShards[key] = true
	}
}

// RecordBackendRangeGet records a VFS -> Backend GetRange crossing.
func (a *AuditRecorder) RecordBackendRangeGet(key string, off, reqLen int64, got int, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpBackendRangeGet, d)
	if got > 0 {
		a.beRead += int64(got)
	}
	if err != nil || off < 0 || reqLen <= 0 || off > math.MaxInt64-reqLen {
		return
	}
	if strings.HasPrefix(key, "pack/") {
		end := off + reqLen
		if prev, ok := a.lastPackRange[key]; ok {
			if isAdjacentOrOverlapping(prev.off, prev.end, off, end, a.coalesceGap) {
				a.redCounts[RedundancyUncoalescedRangeGet]++
			}
		}
		a.lastPackRange[key] = rangeWindow{off: off, end: end}
	}
}

// RecordBackendPut records a VFS -> Backend unconditional Put crossing.
func (a *AuditRecorder) RecordBackendPut(key string, n int, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpBackendPut, d)
	if n > 0 {
		a.beWrite += int64(n)
	}
	if err == nil {
		a.invalidateListLocked(key)
	}
}

// RecordBackendConditionalPut records a PutIfAbsent or PutIfVersion crossing.
func (a *AuditRecorder) RecordBackendConditionalPut(key string, newVer backend.Version, n int, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpBackendConditionalPut, d)
	if n > 0 {
		a.beWrite += int64(n)
	}
	if key == "head" {
		if a.pendingPrePutHeadGet {
			a.redCounts[RedundancyPrePutGet]++
			a.pendingPrePutHeadGet = false
		}
		if err == nil {
			a.lastHeadVer = newVer
			a.lastHeadKnown = newVer != ""
		} else if errors.Is(err, backend.ErrConflict) {
			a.lastHeadKnown = false
			a.lastHeadVer = ""
		}
	}
	if err == nil {
		a.invalidateListLocked(key)
	}
}

// RecordBackendHead records a VFS -> Backend Stat (HEAD) crossing.
func (a *AuditRecorder) RecordBackendHead(key string, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpBackendHead, d)
	if err != nil {
		return
	}
	if key == "head" {
		if a.lastHeadKnown {
			a.redCounts[RedundancyRepeatedHeadRead]++
		}
	} else if isImmutableShardKey(key) {
		if a.seenShardHead[key] {
			a.redCounts[RedundancyRepeatedShardRead]++
		}
		a.seenShardHead[key] = true
	}
}

// RecordBackendList records a VFS -> Backend List(prefix) crossing.
func (a *AuditRecorder) RecordBackendList(prefix string, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpBackendList, d)
	if err != nil {
		return
	}
	if a.seenList[prefix] {
		a.redCounts[RedundancyRepeatedList]++
	}
	a.seenList[prefix] = true
}

// RecordBackendDelete records a VFS -> Backend Delete crossing.
func (a *AuditRecorder) RecordBackendDelete(key string, d time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordOpLocked(OpBackendDelete, d)
	if err == nil {
		a.invalidateListLocked(key)
		if key == "head" {
			a.lastHeadKnown = false
			a.lastHeadVer = ""
		}
	}
}

func (a *AuditRecorder) invalidateListLocked(key string) {
	for prefix := range a.seenList {
		if strings.HasPrefix(key, prefix) {
			delete(a.seenList, prefix)
		}
	}
}

// Snapshot returns a deep copy of all accumulated counts, latencies, and redundancies.
func (a *AuditRecorder) Snapshot() BoundarySnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	snap := BoundarySnapshot{
		OpCounts:            make(map[OpType]int64, len(a.opCounts)),
		OpLatencies:         make(map[OpType]LatencyStats, len(a.opLatency)),
		RedundancyCounts:    make(map[RedundancyClass]int64, len(a.redCounts)),
		BytesRead:           a.bytesRead,
		BytesWritten:        a.bytesWrite,
		BackendBytesRead:    a.beRead,
		BackendBytesWritten: a.beWrite,
	}
	for op, c := range a.opCounts {
		snap.OpCounts[op] = c
		snap.TotalRequests += c
		if BoundaryOf(op) == BoundaryVFSBackend {
			snap.BackendBoundaryRequests += c
		} else {
			snap.FUSEBoundaryRequests += c
		}
	}
	for op, lr := range a.opLatency {
		snap.OpLatencies[op] = lr.snapshot()
	}
	for cls, c := range a.redCounts {
		snap.RedundancyCounts[cls] = c
		snap.TotalRedundant += c
		if rec, ok := GuaranteeFor(cls); ok && rec.Boundary == BoundaryVFSBackend {
			snap.BackendRedundant += c
		} else {
			snap.FUSERedundant += c
		}
	}
	return snap
}

// isImmutableShardKey reports whether key has the canonical content-addressed
// VFS shard shape: "meta/<2hex>/<64hex>" where the first 2 hex chars match the
// prefix of the 64-char BLAKE3 digest.
func isImmutableShardKey(key string) bool {
	_, ok := parseShardDigest(key)
	return ok
}

// parseShardDigest extracts the 32-byte BLAKE3 digest from a canonical
// "meta/<2hex>/<64hex>" key.
func parseShardDigest(key string) ([32]byte, bool) {
	var digest [32]byte
	if !strings.HasPrefix(key, "meta/") {
		return digest, false
	}
	rest := key[len("meta/"):]
	// Expect "ab/<64hex>" => 2 + 1 + 64 = 67 bytes.
	if len(rest) != 67 || rest[2] != '/' {
		return digest, false
	}
	prefix := rest[:2]
	hexDigest := rest[3:]
	if hexDigest[:2] != prefix {
		return digest, false
	}
	raw, err := hex.DecodeString(hexDigest)
	if err != nil || len(raw) != 32 {
		return digest, false
	}
	copy(digest[:], raw)
	return digest, true
}

// isAdjacentOrOverlapping reports whether [aOff, aEnd) and [bOff, bEnd)
// overlap or are separated by at most maxGap bytes, without signed int64 overflow.
func isAdjacentOrOverlapping(aOff, aEnd, bOff, bEnd, maxGap int64) bool {
	if aOff > bOff {
		aOff, bOff = bOff, aOff
		aEnd, bEnd = bEnd, aEnd
	}
	if bOff <= aEnd {
		return true
	}
	return bOff-aEnd <= maxGap
}

// InstrumentedBackend transparently wraps any canonical backend.Backend to
// measure every VFS -> S3/Backend request, byte volume, latency, and redundancy.
type InstrumentedBackend struct {
	inner    backend.Backend
	recorder *AuditRecorder
}

var _ backend.Backend = (*InstrumentedBackend)(nil)

// NewInstrumentedBackend wraps inner with rec. If rec is nil a new recorder is created.
func NewInstrumentedBackend(inner backend.Backend, rec *AuditRecorder) *InstrumentedBackend {
	if rec == nil {
		rec = NewAuditRecorder()
	}
	return &InstrumentedBackend{inner: inner, recorder: rec}
}

// Recorder returns the AuditRecorder attached to this backend wrapper.
func (b *InstrumentedBackend) Recorder() *AuditRecorder { return b.recorder }

func (b *InstrumentedBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	start := time.Now()
	data, ver, err := b.inner.Get(ctx, key, max)
	b.recorder.RecordBackendGet(key, ver, len(data), time.Since(start), err)
	return data, ver, err
}

func (b *InstrumentedBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	start := time.Now()
	data, err := b.inner.GetRange(ctx, key, off, n)
	b.recorder.RecordBackendRangeGet(key, off, n, len(data), time.Since(start), err)
	return data, err
}

func (b *InstrumentedBackend) Put(ctx context.Context, key string, data []byte) error {
	start := time.Now()
	err := b.inner.Put(ctx, key, data)
	b.recorder.RecordBackendPut(key, len(data), time.Since(start), err)
	return err
}

func (b *InstrumentedBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	start := time.Now()
	ver, err := b.inner.PutIfAbsent(ctx, key, data)
	b.recorder.RecordBackendConditionalPut(key, ver, len(data), time.Since(start), err)
	return ver, err
}

func (b *InstrumentedBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	start := time.Now()
	ver, err := b.inner.PutIfVersion(ctx, key, data, v)
	b.recorder.RecordBackendConditionalPut(key, ver, len(data), time.Since(start), err)
	return ver, err
}

func (b *InstrumentedBackend) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := b.inner.Delete(ctx, key)
	b.recorder.RecordBackendDelete(key, time.Since(start), err)
	return err
}

func (b *InstrumentedBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	start := time.Now()
	inObj, inErr := b.inner.List(ctx, prefix)
	outObj := make(chan backend.Object)
	outErr := make(chan error, 1)
	go func() {
		defer close(outObj)
		defer close(outErr)
		for obj := range inObj {
			select {
			case outObj <- obj:
			case <-ctx.Done():
				// Drain remaining error if any and record cancellation.
				err := ctx.Err()
				b.recorder.RecordBackendList(prefix, time.Since(start), err)
				outErr <- err
				return
			}
		}
		err := <-inErr
		b.recorder.RecordBackendList(prefix, time.Since(start), err)
		if err != nil {
			outErr <- err
		}
	}()
	return outObj, outErr
}

func (b *InstrumentedBackend) Stat(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	sz, err := b.inner.Stat(ctx, key)
	b.recorder.RecordBackendHead(key, time.Since(start), err)
	return sz, err
}

func (b *InstrumentedBackend) Close() error { return b.inner.Close() }

// String delegates directly to the inner backend so headAAD(v.be.String())
// binds to the canonical backend location without breaking sealing.
func (b *InstrumentedBackend) String() string { return b.inner.String() }

// LatencyConfig configures per-operation simulated S3 network RTT for
// LatencyBackend.
type LatencyConfig struct {
	GetRTT      time.Duration
	RangeGetRTT time.Duration
	PutRTT      time.Duration
	HeadRTT     time.Duration
	ListRTT     time.Duration
	DeleteRTT   time.Duration
}

// UniformLatencyConfig builds a LatencyConfig with a uniform RTT across all
// object-store operations.
func UniformLatencyConfig(rtt time.Duration) LatencyConfig {
	return LatencyConfig{
		GetRTT:      rtt,
		RangeGetRTT: rtt,
		PutRTT:      rtt,
		HeadRTT:     rtt,
		ListRTT:     rtt,
		DeleteRTT:   rtt,
	}
}

// LatencyBackend transparently wraps backend.Backend to inject configurable
// S3 network round-trip latency while preserving all backend semantics and String().
type LatencyBackend struct {
	inner backend.Backend
	cfg   LatencyConfig
}

var _ backend.Backend = (*LatencyBackend)(nil)

// NewLatencyBackend wraps inner with configurable per-op network RTT.
func NewLatencyBackend(inner backend.Backend, cfg LatencyConfig) *LatencyBackend {
	return &LatencyBackend{inner: inner, cfg: cfg}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *LatencyBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	if err := sleepCtx(ctx, l.cfg.GetRTT); err != nil {
		return nil, "", err
	}
	return l.inner.Get(ctx, key, max)
}

func (l *LatencyBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := sleepCtx(ctx, l.cfg.RangeGetRTT); err != nil {
		return nil, err
	}
	return l.inner.GetRange(ctx, key, off, n)
}

func (l *LatencyBackend) Put(ctx context.Context, key string, data []byte) error {
	if err := sleepCtx(ctx, l.cfg.PutRTT); err != nil {
		return err
	}
	return l.inner.Put(ctx, key, data)
}

func (l *LatencyBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	if err := sleepCtx(ctx, l.cfg.PutRTT); err != nil {
		return "", err
	}
	return l.inner.PutIfAbsent(ctx, key, data)
}

func (l *LatencyBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	if err := sleepCtx(ctx, l.cfg.PutRTT); err != nil {
		return "", err
	}
	return l.inner.PutIfVersion(ctx, key, data, v)
}

func (l *LatencyBackend) Delete(ctx context.Context, key string) error {
	if err := sleepCtx(ctx, l.cfg.DeleteRTT); err != nil {
		return err
	}
	return l.inner.Delete(ctx, key)
}

func (l *LatencyBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	if err := sleepCtx(ctx, l.cfg.ListRTT); err != nil {
		ch := make(chan backend.Object)
		errCh := make(chan error, 1)
		close(ch)
		errCh <- err
		close(errCh)
		return ch, errCh
	}
	return l.inner.List(ctx, prefix)
}

func (l *LatencyBackend) Stat(ctx context.Context, key string) (int64, error) {
	if err := sleepCtx(ctx, l.cfg.HeadRTT); err != nil {
		return 0, err
	}
	return l.inner.Stat(ctx, key)
}

func (l *LatencyBackend) Close() error   { return l.inner.Close() }
func (l *LatencyBackend) String() string { return l.inner.String() }

// GoferFS is the FUSE/Gofer -> *vfs.Volume dispatch interface mirroring
// pkg/mount/fuse_unix.go. Both the unoptimized GoferBridge and the optimized
// CachedGoferBridge implement GoferFS.
type GoferFS interface {
	GetAttr(ctx context.Context, ino vfs.InodeID) (vfs.Inode, error)
	Lookup(ctx context.Context, dir vfs.InodeID, name string) (vfs.Inode, error)
	ReadDir(ctx context.Context, dir vfs.InodeID) ([]vfs.DirEntry, vfs.InodeID, error)
	Open(ctx context.Context, ino vfs.InodeID) (*vfs.File, error)
	Create(ctx context.Context, dir vfs.InodeID, name string, mode fs.FileMode) (vfs.Inode, *vfs.File, error)
	Mkdir(ctx context.Context, dir vfs.InodeID, name string, mode fs.FileMode) (vfs.Inode, error)
	SetAttr(ctx context.Context, ino vfs.InodeID, c vfs.Change) (vfs.Inode, error)
	Remove(ctx context.Context, dir vfs.InodeID, name string) error
	Read(ctx context.Context, f *vfs.File, ino vfs.InodeID, buf []byte, off int64) (int, error)
	Write(ctx context.Context, f *vfs.File, ino vfs.InodeID, data []byte, off int64) (int, error)
	Flush(ctx context.Context) error
	Resolve(ctx context.Context, p string) (vfs.Inode, error)
}

// GoferBridge is the unoptimized FUSE/Gofer handler dispatch directly calling
// *vfs.Volume and recording every crossing into AuditRecorder.
type GoferBridge struct {
	vol      *vfs.Volume
	recorder *AuditRecorder
}

var _ GoferFS = (*GoferBridge)(nil)

// NewGoferBridge wraps vol with boundary instrumentation matching pkg/mount/fuse_unix.go.
func NewGoferBridge(vol *vfs.Volume, rec *AuditRecorder) *GoferBridge {
	if rec == nil {
		rec = NewAuditRecorder()
	}
	return &GoferBridge{vol: vol, recorder: rec}
}

// Volume returns the underlying canonical *vfs.Volume.
func (g *GoferBridge) Volume() *vfs.Volume { return g.vol }

// Recorder returns the AuditRecorder attached to this bridge.
func (g *GoferBridge) Recorder() *AuditRecorder { return g.recorder }

func (g *GoferBridge) GetAttr(ctx context.Context, ino vfs.InodeID) (vfs.Inode, error) {
	start := time.Now()
	in, err := g.vol.Stat(ctx, ino)
	g.recorder.RecordGetAttr(ino, time.Since(start), err)
	return in, err
}

func (g *GoferBridge) Lookup(ctx context.Context, dir vfs.InodeID, name string) (vfs.Inode, error) {
	start := time.Now()
	in, err := g.vol.Lookup(ctx, dir, name)
	g.recorder.RecordLookup(dir, name, in.ID, time.Since(start), err)
	return in, err
}

func (g *GoferBridge) ReadDir(ctx context.Context, dir vfs.InodeID) ([]vfs.DirEntry, vfs.InodeID, error) {
	start := time.Now()
	ents, err := g.vol.ReadDir(ctx, dir)
	if err != nil {
		g.recorder.RecordReadDir(dir, time.Since(start), err)
		return nil, 0, err
	}
	up, err := g.vol.Parent(ctx, dir)
	g.recorder.RecordReadDir(dir, time.Since(start), err)
	return ents, up, err
}

// Open mirrors pkg/mount/fuse_unix.go:448-467: calls vol.Stat first to verify
// the inode is not a directory, then calls vol.Open.
func (g *GoferBridge) Open(ctx context.Context, ino vfs.InodeID) (*vfs.File, error) {
	statStart := time.Now()
	in, err := g.vol.Stat(ctx, ino)
	g.recorder.RecordOpenStat(ino, time.Since(statStart), err)
	if err != nil {
		return nil, err
	}
	if in.IsDir() {
		return nil, vfs.ErrIsDir
	}
	return g.openDirect(ctx, ino)
}

// openDirect calls vol.Open without a preceding vol.Stat. Used by
// CachedGoferBridge when SkipRedundantOpenStat has already verified the cached
// inode type is a regular file.
func (g *GoferBridge) openDirect(ctx context.Context, ino vfs.InodeID) (*vfs.File, error) {
	start := time.Now()
	f, err := g.vol.Open(ctx, ino)
	g.recorder.RecordOpen(ino, time.Since(start), err)
	return f, err
}

func (g *GoferBridge) Create(ctx context.Context, dir vfs.InodeID, name string, mode fs.FileMode) (vfs.Inode, *vfs.File, error) {
	start := time.Now()
	in, err := g.vol.Create(ctx, dir, name, mode)
	g.recorder.RecordCreate(dir, name, in.ID, time.Since(start), err)
	if err != nil {
		return vfs.Inode{}, nil, err
	}
	f, err := g.openDirect(ctx, in.ID)
	if err != nil {
		return vfs.Inode{}, nil, err
	}
	return in, f, nil
}

func (g *GoferBridge) Mkdir(ctx context.Context, dir vfs.InodeID, name string, mode fs.FileMode) (vfs.Inode, error) {
	start := time.Now()
	in, err := g.vol.Mkdir(ctx, dir, name, mode)
	g.recorder.RecordMkdir(dir, name, in.ID, time.Since(start), err)
	return in, err
}

func (g *GoferBridge) SetAttr(ctx context.Context, ino vfs.InodeID, c vfs.Change) (vfs.Inode, error) {
	start := time.Now()
	in, err := g.vol.SetAttr(ctx, ino, c)
	g.recorder.RecordSetAttr(ino, time.Since(start), err)
	return in, err
}

func (g *GoferBridge) Remove(ctx context.Context, dir vfs.InodeID, name string) error {
	start := time.Now()
	err := g.vol.Remove(ctx, dir, name)
	g.recorder.RecordRemove(dir, name, time.Since(start), err)
	return err
}

func (g *GoferBridge) Read(ctx context.Context, f *vfs.File, ino vfs.InodeID, buf []byte, off int64) (int, error) {
	start := time.Now()
	n, err := f.ReadAt(ctx, buf, off)
	g.recorder.RecordRead(ino, n, time.Since(start), err)
	return n, err
}

func (g *GoferBridge) Write(ctx context.Context, f *vfs.File, ino vfs.InodeID, data []byte, off int64) (int, error) {
	start := time.Now()
	n, err := f.WriteAt(ctx, data, off)
	g.recorder.RecordWrite(ino, n, time.Since(start), err)
	return n, err
}

func (g *GoferBridge) Flush(ctx context.Context) error {
	start := time.Now()
	err := g.vol.Flush(ctx)
	g.recorder.RecordFlush(time.Since(start), err)
	return err
}

// Resolve walks a POSIX path segment-by-segment via GetAttr(Root) + Lookup,
// matching FUSE/Gofer path traversal semantics.
func (g *GoferBridge) Resolve(ctx context.Context, p string) (vfs.Inode, error) {
	return resolveVia(ctx, g, p)
}

func resolveVia(ctx context.Context, fsAPI GoferFS, p string) (vfs.Inode, error) {
	cur, err := fsAPI.GetAttr(ctx, vfs.RootInode)
	if err != nil {
		return vfs.Inode{}, err
	}
	cleaned := strings.Trim(path.Clean("/"+p), "/")
	if cleaned == "" {
		return cur, nil
	}
	for _, seg := range strings.Split(cleaned, "/") {
		if seg == "" {
			continue
		}
		cur, err = fsAPI.Lookup(ctx, cur.ID, seg)
		if err != nil {
			return vfs.Inode{}, err
		}
	}
	return cur, nil
}
