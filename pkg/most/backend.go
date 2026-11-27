// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package most

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/hanzoai/vfs/pkg/backend"
)

const manifestKeyDir = "manifests/"

// BackendOption customizes a TenantBackend view over an Engine.
type BackendOption func(*tenantBackendConfig)

type tenantBackendConfig struct {
	scope Scope
}

// WithScope configures the TenantBackend to serve ScopePrivate (default),
// read-only ScopeSharedImmutable, or read-only ScopePublicImmutable.
func WithScope(s Scope) BackendOption {
	return func(c *tenantBackendConfig) {
		c.scope = s
	}
}

// TenantBackend adapts a registered (Engine, tenantID) pair to backend.Backend
// so a real vfs.Volume mounts directly over MOST.
//
// Closing a TenantBackend marks only that view closed and leaves the underlying
// Engine and host backend intact for sibling tenant Volumes.
type TenantBackend struct {
	engine   *Engine
	tenantID string
	scope    Scope

	closeOnce sync.Once
	closedMu  sync.RWMutex
	closed    bool
}

// NewTenantBackend returns a backend.Backend bound to tenantID on engine.
func NewTenantBackend(e *Engine, tenantID string, opts ...BackendOption) *TenantBackend {
	cfg := tenantBackendConfig{scope: ScopePrivate}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &TenantBackend{
		engine:   e,
		tenantID: tenantID,
		scope:    cfg.scope,
	}
}

// NewSharedBackend returns a read-only backend.Backend view over engine's
// ScopeSharedImmutable toolchain tier for an authorized tenantID.
func NewSharedBackend(e *Engine, tenantID string, opts ...BackendOption) *TenantBackend {
	all := append([]BackendOption{WithScope(ScopeSharedImmutable)}, opts...)
	return NewTenantBackend(e, tenantID, all...)
}

// NewPublicBackend returns a read-only backend.Backend view over engine's
// explicitly public ScopePublicImmutable toolchain tier for tenantID.
func NewPublicBackend(e *Engine, tenantID string, opts ...BackendOption) *TenantBackend {
	all := append([]BackendOption{WithScope(ScopePublicImmutable)}, opts...)
	return NewTenantBackend(e, tenantID, all...)
}

func (tb *TenantBackend) checkOpen() error {
	if tb == nil || tb.engine == nil {
		return ErrTenantUnauthorized
	}
	tb.closedMu.RLock()
	c := tb.closed
	tb.closedMu.RUnlock()
	if c {
		return fmt.Errorf("%w: tenant backend closed", ErrTenantUnauthorized)
	}
	return nil
}

func (tb *TenantBackend) mapKeys(key string) (string, error) {
	if err := tb.checkOpen(); err != nil {
		return "", err
	}
	if tb.scope.isImmutableShared() {
		if key == "" {
			return "", backend.ErrKey
		}
		if err := backend.ValidPrefix(key); err != nil {
			return "", fmt.Errorf("%w: %v", backend.ErrKey, err)
		}
		return tb.engine.sharedBackendKey(key), nil
	}
	if err := backend.ValidKey(key); err != nil {
		return "", err
	}
	t, err := tb.engine.getTenant(tb.tenantID)
	if err != nil {
		return "", err
	}
	return tb.engine.privateVolumeBackendKey(t.tvHash, key), nil
}

func (tb *TenantBackend) mapPrefix(prefix string) (basePrefix, fullPrefix string, err error) {
	if err := tb.checkOpen(); err != nil {
		return "", "", err
	}
	if err := backend.ValidKeyPrefix(prefix); err != nil {
		return "", "", err
	}
	if tb.scope.isImmutableShared() {
		if tb.engine.cfg.SharedPrefix == "" {
			return "", prefix, nil
		}
		bp := tb.engine.cfg.SharedPrefix + "/"
		return bp, bp + prefix, nil
	}
	t, err := tb.engine.getTenant(tb.tenantID)
	if err != nil {
		return "", "", err
	}
	bp := "v/" + t.tvHash[:24] + "/"
	return bp, bp + prefix, nil
}

func (tb *TenantBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	beKey, err := tb.mapKeys(key)
	if err != nil {
		return nil, "", err
	}
	return tb.engine.getForBackend(ctx, tb.tenantID, tb.scope, key, beKey, max)
}

func (tb *TenantBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	beKey, err := tb.mapKeys(key)
	if err != nil {
		return nil, err
	}
	return tb.engine.getRangeForBackend(ctx, tb.tenantID, tb.scope, key, beKey, off, n)
}

func (tb *TenantBackend) Put(ctx context.Context, key string, data []byte) error {
	beKey, err := tb.mapKeys(key)
	if err != nil {
		return err
	}
	return tb.engine.putForBackend(ctx, tb.tenantID, tb.scope, key, beKey, data)
}

func (tb *TenantBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	beKey, err := tb.mapKeys(key)
	if err != nil {
		return "", err
	}
	return tb.engine.putIfAbsentForBackend(ctx, tb.tenantID, tb.scope, key, beKey, data)
}

func (tb *TenantBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	beKey, err := tb.mapKeys(key)
	if err != nil {
		return "", err
	}
	return tb.engine.putIfVersionForBackend(ctx, tb.tenantID, tb.scope, key, beKey, data, v)
}

func (tb *TenantBackend) Delete(ctx context.Context, key string) error {
	beKey, err := tb.mapKeys(key)
	if err != nil {
		return err
	}
	return tb.engine.deleteForBackend(ctx, tb.tenantID, tb.scope, key, beKey)
}

func (tb *TenantBackend) List(ctx context.Context, prefix string) (<-chan backend.Object, <-chan error) {
	objs := make(chan backend.Object, 64)
	errCh := make(chan error, 1)

	bp, fullPrefix, err := tb.mapPrefix(prefix)
	if err != nil {
		errCh <- err
		close(objs)
		close(errCh)
		return objs, errCh
	}
	t, err := tb.engine.getTenant(tb.tenantID)
	if err != nil {
		errCh <- err
		close(objs)
		close(errCh)
		return objs, errCh
	}
	t.lifecycleMu.RLock()
	tb.engine.mu.RLock()
	authErr := tb.engine.authorizeLocked(t, tb.scope, prefix)
	tb.engine.mu.RUnlock()
	if authErr != nil && !(tb.scope.isImmutableShared() && t.sharedAllowed) {
		t.lifecycleMu.RUnlock()
		tb.engine.unauthAttempts.Add(1)
		errCh <- authErr
		close(objs)
		close(errCh)
		return objs, errCh
	}

	rawObjs, rawErrCh := tb.engine.be.List(ctx, fullPrefix)
	go func() {
		defer t.lifecycleMu.RUnlock()
		defer close(objs)
		defer close(errCh)
		for o := range rawObjs {
			k := strings.TrimPrefix(o.Key, bp)
			if strings.HasPrefix(k, manifestKeyDir) {
				continue
			}
			if tb.scope.isImmutableShared() {
				tb.engine.mu.RLock()
				meta, reg := tb.engine.sharedMeta[k]
				allowed := reg && tb.engine.authorizeSharedKeyLocked(t, tb.scope, k, meta) == nil
				tb.engine.mu.RUnlock()
				if !allowed {
					continue
				}
			}
			o.Key = k
			objs <- o
		}
		if lerr := <-rawErrCh; lerr != nil {
			errCh <- lerr
		}
	}()
	return objs, errCh
}

func (tb *TenantBackend) Stat(ctx context.Context, key string) (int64, error) {
	beKey, err := tb.mapKeys(key)
	if err != nil {
		return 0, err
	}
	return tb.engine.statForBackend(ctx, tb.tenantID, tb.scope, key, beKey)
}

func (tb *TenantBackend) Close() error {
	tb.closeOnce.Do(func() {
		tb.closedMu.Lock()
		tb.closed = true
		tb.closedMu.Unlock()
	})
	return nil
}

// String returns a deterministic canonical location identifier for Head sealing
// without exposing plaintext tenant IDs or volume identifiers.
func (tb *TenantBackend) String() string {
	if tb == nil || tb.engine == nil || tb.engine.be == nil {
		return "most://invalid"
	}
	base := strings.TrimRight(tb.engine.be.String(), "/")
	if tb.scope.isImmutableShared() {
		if tb.engine.cfg.SharedPrefix != "" {
			return base + "/" + tb.engine.cfg.SharedPrefix
		}
		return tb.engine.be.String()
	}
	tb.engine.mu.RLock()
	t := eTenantLookup(tb.engine, tb.tenantID)
	tb.engine.mu.RUnlock()
	if t == nil {
		return "most://unregistered/" + hashHex("unreg", tb.tenantID)[:16]
	}
	return "most://" + hashHex("place", tb.engine.be.String())[:16] + "/" + t.tvHash[:24]
}

func eTenantLookup(e *Engine, id string) *tenantState {
	if e == nil || e.tenants == nil {
		return nil
	}
	return e.tenants[id]
}

var _ backend.Backend = (*TenantBackend)(nil)
