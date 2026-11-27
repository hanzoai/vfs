// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package thinkahead

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/hanzoai/vfs"
)

const (
	// ExtName is the canonical docs/prefetch.md S6 extension namespace
	// ("ext/trace/<hash>") storing sealed version-0x01 chunk-ID traces.
	ExtName = "trace"

	// ExtWarmHintsName stores compact sealed (inode, offset, length) warm hints
	// used by VolumePrefetcher to asynchronously warm a cold *vfs.Volume's
	// internal shard and chunk caches without exposing any plaintext filenames.
	ExtWarmHintsName = "trace-hints"

	warmHintVersion byte = 0x01
	warmHintRecSize      = 24 // uint64 ino + uint64 off + uint64 len
	maxWarmHints         = 100_000
)

// SaveTraceToVolume persists a first-touch ordered ChunkID trace to v under
// `ext/trace/<hash>`, sealed with the Volume's cipher and replaced atomically
// via PutExt (docs/prefetch.md S6.1-S6.4).
func SaveTraceToVolume(ctx context.Context, v *vfs.Volume, scope ScopeKey, ids []ChunkID) error {
	if v == nil {
		return errors.New("thinkahead: nil vfs.Volume")
	}
	wire := EncodeTraceWire(ids)
	return v.PutExt(ctx, ExtName, scope.WorkflowExtKey(), wire)
}

// LoadTraceFromVolume reads and decodes the sealed S6 trace for scope from v.
func LoadTraceFromVolume(ctx context.Context, v *vfs.Volume, scope ScopeKey) ([]ChunkID, error) {
	if v == nil {
		return nil, errors.New("thinkahead: nil vfs.Volume")
	}
	raw, err := v.GetExt(ctx, ExtName, scope.WorkflowExtKey())
	if err != nil {
		return nil, err
	}
	return DecodeTraceWire(raw)
}

// InodeRange identifies one settled byte range of a Volume inode to warm into
// the Volume's internal chunk cache. It contains no file paths or content.
type InodeRange struct {
	Ino    vfs.InodeID
	Offset uint64
	Length uint64
}

// EncodeWarmHints encodes a bounded list of InodeRanges (version 0x01 + 24 B/entry).
func EncodeWarmHints(ranges []InodeRange) []byte {
	n := len(ranges)
	if n > maxWarmHints {
		n = maxWarmHints
	}
	out := make([]byte, 1+n*warmHintRecSize)
	out[0] = warmHintVersion
	for i := 0; i < n; i++ {
		base := 1 + i*warmHintRecSize
		binary.LittleEndian.PutUint64(out[base:], uint64(ranges[i].Ino))
		binary.LittleEndian.PutUint64(out[base+8:], ranges[i].Offset)
		binary.LittleEndian.PutUint64(out[base+16:], ranges[i].Length)
	}
	return out
}

// DecodeWarmHints parses a version-0x01 InodeRange warm-hint payload.
func DecodeWarmHints(raw []byte) ([]InodeRange, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: empty warm hints", ErrInvalidTrace)
	}
	if raw[0] != warmHintVersion {
		return nil, fmt.Errorf("%w: unsupported warm-hint version 0x%02x", ErrInvalidTrace, raw[0])
	}
	body := raw[1:]
	if len(body)%warmHintRecSize != 0 {
		return nil, fmt.Errorf("%w: invalid warm-hint body length %d", ErrInvalidTrace, len(body))
	}
	n := len(body) / warmHintRecSize
	out := make([]InodeRange, n)
	for i := 0; i < n; i++ {
		base := i * warmHintRecSize
		out[i] = InodeRange{
			Ino:    vfs.InodeID(binary.LittleEndian.Uint64(body[base:])),
			Offset: binary.LittleEndian.Uint64(body[base+8:]),
			Length: binary.LittleEndian.Uint64(body[base+16:]),
		}
	}
	return out, nil
}

// VolumePrefetcher wraps a canonical *vfs.Volume to:
//  1. Record read ranges through a non-blocking bounded observation queue (S1),
//  2. Persist sealed hints via Volume.PutExt on clean detach (S6),
//  3. Asynchronously warm the Volume's internal shard & chunk caches on cold
//     reopen with bounded concurrency and immediate cancellation on Detach (S4).
type VolumePrefetcher struct {
	vol         *vfs.Volume
	scope       ScopeKey
	maxInflight int

	recCh      chan InodeRange
	recDropped atomic.Int64
	recMu      sync.Mutex
	recSeen    map[InodeRange]struct{}
	recOrder   []InodeRange
	recStop    chan struct{}
	recDone    chan struct{}

	warmMu     sync.Mutex
	warmCancel context.CancelFunc
	warmWG     sync.WaitGroup
	warmed     atomic.Int64
	warmBytes  atomic.Int64
}

// NewVolumePrefetcher creates a non-blocking trace recorder and async warmer
// bound to vol and scope.
func NewVolumePrefetcher(vol *vfs.Volume, scope ScopeKey, maxInflight int) *VolumePrefetcher {
	if maxInflight <= 0 {
		maxInflight = 16
	}
	vp := &VolumePrefetcher{
		vol:         vol,
		scope:       scope,
		maxInflight: maxInflight,
		recCh:       make(chan InodeRange, 4096),
		recSeen:     make(map[InodeRange]struct{}, 256),
		recStop:     make(chan struct{}),
		recDone:     make(chan struct{}),
	}
	go vp.recordLoop()
	return vp
}

func (vp *VolumePrefetcher) recordLoop() {
	defer close(vp.recDone)
	for {
		select {
		case r := <-vp.recCh:
			vp.recMu.Lock()
			if _, dup := vp.recSeen[r]; !dup && len(vp.recOrder) < maxWarmHints {
				vp.recSeen[r] = struct{}{}
				vp.recOrder = append(vp.recOrder, r)
			}
			vp.recMu.Unlock()
		case <-vp.recStop:
			// Drain buffered entries without blocking.
			for {
				select {
				case r := <-vp.recCh:
					vp.recMu.Lock()
					if _, dup := vp.recSeen[r]; !dup && len(vp.recOrder) < maxWarmHints {
						vp.recSeen[r] = struct{}{}
						vp.recOrder = append(vp.recOrder, r)
					}
					vp.recMu.Unlock()
				default:
					return
				}
			}
		}
	}
}

// ObserveRead queues a read range without ever blocking the caller (S1.2).
func (vp *VolumePrefetcher) ObserveRead(ino vfs.InodeID, off, length uint64) {
	if length == 0 {
		return
	}
	select {
	case vp.recCh <- InodeRange{Ino: ino, Offset: off, Length: length}:
	default:
		vp.recDropped.Add(1)
	}
}

// SaveHints persists the recorded inode read sequence into v via PutExt.
func (vp *VolumePrefetcher) SaveHints(ctx context.Context) error {
	vp.recMu.Lock()
	snap := make([]InodeRange, len(vp.recOrder))
	copy(snap, vp.recOrder)
	vp.recMu.Unlock()
	if len(snap) == 0 {
		return nil
	}
	return vp.vol.PutExt(ctx, ExtWarmHintsName, vp.scope.WorkflowExtKey(), EncodeWarmHints(snap))
}

// StartWarmFromVolume loads sealed warm hints from v via GetExt and begins
// bounded background warming of v's internal chunk/shard caches.
func (vp *VolumePrefetcher) StartWarmFromVolume(ctx context.Context) (int, error) {
	raw, err := vp.vol.GetExt(ctx, ExtWarmHintsName, vp.scope.WorkflowExtKey())
	if err != nil {
		return 0, err
	}
	hints, err := DecodeWarmHints(raw)
	if err != nil {
		return 0, err
	}
	vp.StartWarmHints(ctx, hints)
	return len(hints), nil
}

// StartWarmHints launches bounded background reads on v to populate its
// internal LRU chunk cache ahead of foreground workflow reads.
func (vp *VolumePrefetcher) StartWarmHints(ctx context.Context, hints []InodeRange) {
	if len(hints) == 0 {
		return
	}
	wctx, cancel := context.WithCancel(WithPrefetchContext(ctx))
	vp.warmMu.Lock()
	if vp.warmCancel != nil {
		vp.warmCancel()
	}
	vp.warmCancel = cancel
	vp.warmMu.Unlock()

	sem := make(chan struct{}, vp.maxInflight)
	vp.warmWG.Add(1)
	go func() {
		defer vp.warmWG.Done()
		var inner sync.WaitGroup
		defer inner.Wait()
		for _, h := range hints {
			if wctx.Err() != nil {
				return
			}
			select {
			case sem <- struct{}{}:
			case <-wctx.Done():
				return
			}
			inner.Add(1)
			go func(r InodeRange) {
				defer inner.Done()
				defer func() { <-sem }()
				if wctx.Err() != nil {
					return
				}
				f, err := vp.vol.Open(wctx, r.Ino)
				if err != nil {
					return
				}
				defer f.Close()
				// Read the hinted span into a discard buffer so v.chunks caches
				// every decoded, verified chunk serving [Offset, Offset+Length).
				readLen := r.Length
				if readLen > 4<<20 {
					readLen = 4 << 20
				}
				buf := make([]byte, readLen)
				n, err := f.ReadAt(wctx, buf, int64(r.Offset))
				if err == nil || errors.Is(err, io.EOF) {
					if n > 0 {
						vp.warmed.Add(1)
						vp.warmBytes.Add(int64(n))
					}
				}
			}(h)
		}
	}()
}

// WarmedStats returns the number of ranges and bytes warmed so far.
func (vp *VolumePrefetcher) WarmedStats() (ranges, bytes, dropped int64) {
	return vp.warmed.Load(), vp.warmBytes.Load(), vp.recDropped.Load()
}

// WaitWarm blocks until all launched background warming completes.
func (vp *VolumePrefetcher) WaitWarm() {
	vp.warmWG.Wait()
}

// Close cancels any active warming, waits for all background goroutines to
// exit cleanly, and stops the recorder. Issues ZERO synchronous S3 requests.
func (vp *VolumePrefetcher) Close() {
	vp.warmMu.Lock()
	if vp.warmCancel != nil {
		vp.warmCancel()
		vp.warmCancel = nil
	}
	vp.warmMu.Unlock()
	vp.warmWG.Wait()

	select {
	case <-vp.recStop:
	default:
		close(vp.recStop)
	}
	<-vp.recDone
}
