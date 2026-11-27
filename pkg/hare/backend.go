// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package hare

import (
	"context"
	"encoding/binary"
	"strings"
	"time"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/pkg/backend"
)

// TenantBackend wraps a vfs backend.Backend for one sandbox tenant, routing
// every S3 object Get, GetRange, Put, PutIfAbsent, and PutIfVersion through
// the node-wide HARE Scheduler.
type TenantBackend struct {
	inner     backend.Backend
	sched     *Scheduler
	tenantID  string
	honorPace bool
}

// NewTenantBackend binds inner to tenantID on sched. When honorPace is true,
// S3 bandwidth pacing sleeps on context-cancellable timers; when false,
// PaceWait is accounted in telemetry without sleeping (useful in unit tests).
func NewTenantBackend(inner backend.Backend, sched *Scheduler, tenantID string, honorPace bool) *TenantBackend {
	return &TenantBackend{
		inner:     inner,
		sched:     sched,
		tenantID:  tenantID,
		honorPace: honorPace,
	}
}

func keyBlockHash(key string, off int64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(off>>16))
	h := blake3.New()
	_, _ = h.WriteString(key)
	_, _ = h.Write(buf[:])
	var sum [32]byte
	h.Sum(sum[:0])
	return binary.LittleEndian.Uint64(sum[:8])
}

func readOpClass(key string) OpClass {
	if strings.HasPrefix(key, "meta/") || key == "head" {
		return OpMetaRead
	}
	return OpChunkRead
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-tm.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (tb *TenantBackend) String() string { return tb.inner.String() }

func (tb *TenantBackend) Close() error { return tb.inner.Close() }

func (tb *TenantBackend) Get(ctx context.Context, key string, maxBytes int64) ([]byte, backend.Version, error) {
	_, _ = tb.sched.ObserveBlockAccess(tb.tenantID, keyBlockHash(key, 0), 64<<10)
	tk, err := tb.sched.Acquire(ctx, tb.tenantID, readOpClass(key), 64<<10)
	if err != nil {
		return nil, "", err
	}
	defer tk.Done()
	if tb.honorPace && tk.PaceWait > 0 {
		if err := sleepCtx(ctx, tk.PaceWait); err != nil {
			return nil, "", err
		}
	}
	return tb.inner.Get(ctx, key, maxBytes)
}

func (tb *TenantBackend) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	_, _ = tb.sched.ObserveBlockAccess(tb.tenantID, keyBlockHash(key, off), length)
	tk, err := tb.sched.Acquire(ctx, tb.tenantID, OpChunkRead, length)
	if err != nil {
		return nil, err
	}
	defer tk.Done()
	if tb.honorPace && tk.PaceWait > 0 {
		if err := sleepCtx(ctx, tk.PaceWait); err != nil {
			return nil, err
		}
	}
	return tb.inner.GetRange(ctx, key, off, length)
}

func (tb *TenantBackend) Put(ctx context.Context, key string, data []byte) error {
	tk, err := tb.sched.Acquire(ctx, tb.tenantID, OpPublishPut, int64(len(data)))
	if err != nil {
		return err
	}
	defer tk.Done()
	if tb.honorPace && tk.PaceWait > 0 {
		if err := sleepCtx(ctx, tk.PaceWait); err != nil {
			return err
		}
	}
	return tb.inner.Put(ctx, key, data)
}

func (tb *TenantBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	tk, err := tb.sched.Acquire(ctx, tb.tenantID, OpPublishPut, int64(len(data)))
	if err != nil {
		return "", err
	}
	defer tk.Done()
	if tb.honorPace && tk.PaceWait > 0 {
		if err := sleepCtx(ctx, tk.PaceWait); err != nil {
			return "", err
		}
	}
	return tb.inner.PutIfAbsent(ctx, key, data)
}

func (tb *TenantBackend) PutIfVersion(ctx context.Context, key string, data []byte, ver backend.Version) (backend.Version, error) {
	tk, err := tb.sched.Acquire(ctx, tb.tenantID, OpPublishPut, int64(len(data)))
	if err != nil {
		return "", err
	}
	defer tk.Done()
	if tb.honorPace && tk.PaceWait > 0 {
		if err := sleepCtx(ctx, tk.PaceWait); err != nil {
			return "", err
		}
	}
	return tb.inner.PutIfVersion(ctx, key, data, ver)
}

func (tb *TenantBackend) Delete(ctx context.Context, key string) error {
	return tb.inner.Delete(ctx, key)
}

func (tb *TenantBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	return tb.inner.List(ctx, prefix)
}

func (tb *TenantBackend) Stat(ctx context.Context, key string) (int64, error) {
	return tb.inner.Stat(ctx, key)
}
