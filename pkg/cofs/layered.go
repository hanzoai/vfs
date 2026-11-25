// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cofs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"sync"

	"github.com/hanzoai/vfs"
)

// Source Layer indicates whether a resolved file came from the immutable base
// image index or the mutable upper workspace Volume.
type Source uint8

const (
	SourceBase Source = iota + 1
	SourceWorkspace
)

// LayeredResolver pairs an immutable CoFS Index (for a base image or snapshot)
// with an optional mutable workspace vfs.Volume.
//
// Per CoFS (FAST 2026) and Issue #25, the minimal-perfect-hash index covers
// ONLY the immutable base image; workspace writes and whiteouts stay in the
// mutable workspace without rebuilding the base MPHF.
type LayeredResolver struct {
	base      *Index
	workspace *vfs.Volume

	mu sync.RWMutex
	// clean paths modified/created in workspace
	dirtySet map[string]bool
	// clean paths removed in workspace
	whiteouts map[string]bool
}

// NewLayeredResolver creates a composite resolver over base and an optional
// mutable workspace Volume.
func NewLayeredResolver(base *Index, workspace *vfs.Volume) *LayeredResolver {
	return &LayeredResolver{
		base:      base,
		workspace: workspace,
		dirtySet:  make(map[string]bool),
		whiteouts: make(map[string]bool),
	}
}

func cleanAbs(p string) string { return path.Clean("/" + p) }

func (lr *LayeredResolver) isWhiteoutLocked(clean string) bool {
	if lr.whiteouts[clean] {
		return true
	}
	// Any whiteout on an ancestor directory also hides descendants of the base image.
	for cur := path.Dir(clean); cur != "/" && cur != "."; cur = path.Dir(cur) {
		if lr.whiteouts[cur] {
			return true
		}
	}
	return false
}

// MarkWorkspaceWrite records that path p now resides in the mutable workspace,
// clearing any prior whiteout for p.
func (lr *LayeredResolver) MarkWorkspaceWrite(p string) {
	c := cleanAbs(p)
	lr.mu.Lock()
	delete(lr.whiteouts, c)
	lr.dirtySet[c] = true
	lr.mu.Unlock()
}

// MarkWorkspaceRemove records a whiteout hiding p in the immutable base image.
func (lr *LayeredResolver) MarkWorkspaceRemove(p string) {
	c := cleanAbs(p)
	lr.mu.Lock()
	delete(lr.dirtySet, c)
	lr.whiteouts[c] = true
	lr.mu.Unlock()
}

// Resolve looks up p across the mutable workspace (when marked dirty) and the
// immutable base CoFS index (for unmodified base paths) in O(1).
func (lr *LayeredResolver) Resolve(ctx context.Context, p string) (vfs.Inode, Source, error) {
	c := cleanAbs(p)

	lr.mu.RLock()
	dirty := lr.dirtySet[c]
	hidden := !dirty && lr.isWhiteoutLocked(c)
	lr.mu.RUnlock()

	if dirty && lr.workspace != nil {
		in, err := lr.workspace.Resolve(ctx, c)
		if err == nil {
			return in, SourceWorkspace, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return vfs.Inode{}, 0, err
		}
	}
	if hidden {
		return vfs.Inode{}, 0, fmt.Errorf("%w: %q", fs.ErrNotExist, c)
	}
	in, err := lr.base.Resolve(c)
	if err == nil {
		return in, SourceBase, nil
	}
	// Fallback check on workspace if caller mutated workspace directly without MarkWorkspaceWrite.
	if lr.workspace != nil && strings.HasPrefix(c, "/") {
		if wIn, wErr := lr.workspace.Resolve(ctx, c); wErr == nil {
			return wIn, SourceWorkspace, nil
		}
	}
	return vfs.Inode{}, 0, err
}
