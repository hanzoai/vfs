// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package thinkahead implements clean-room, trace-guided chunk prefetching for
// repeatable coding workflows on hanzoai/vfs (HIP-1342, hanzoai/vfs#18).
//
// # Provenance and Clean-Room Design
//
// This package is an independent Apache-2.0 Go implementation inspired by the
// trace-guided preloading concept in ThinkAhead (USENIX FAST 2026) and the
// Hanzo VFS prefetch specification in docs/prefetch.md (S1-S8). No external
// prefetcher code was consulted, and the package introduces zero new external
// dependencies beyond the Go standard library, github.com/zeebo/blake3
// (already in go.mod), and canonical hanzoai/vfs packages.
//
// # Architecture
//
//   - Trace capture & dataset (trace.go): captures real file/chunk read traces
//     on Linux via strace (labeled "live-strace-vm") as well as deterministic
//     manifest-grounded workflow traces (labeled "manifest-replay") for
//     npm ci, pnpm install, go build/test, cargo check, uv sync, and git checkout.
//     Persists compact version-0x01 chunk-ID traces (<= 32,000,001 bytes) via
//     Volume.PutExt("trace", ...) sealed with the volume's cipher (S6).
//   - Predictability analysis (predictability.go): quantifies run N -> N+1
//     Jaccard overlap, chunk/byte recall & precision, ordered prefix overlap,
//     and Markov-1 / windowed transition predictability for identical repeats
//     and realistic 1-5 file edits.
//   - Segmented node cache (cache.go): 2Q/SLRU probationary + protected cache
//     (S5) where prefetched chunks enter probationary and promote on first
//     demand read, preventing mispredicted prefetches from evicting the live
//     working set.
//   - Sequence learner & bounded prefetcher (prefetch.go): learns per-workflow
//     first-touch sequences and transition chains strictly scoped by
//     (tenant_id, repo_hash, workflow_name) so cross-tenant cache/trace timing
//     leakage is impossible. Enforces bounded in-flight GETs, bounded unread
//     bytes (S3), singleflight demand promotion (S2), BLAKE3 content-hash
//     verification, adaptive confidence throttling on stale/mismatched traces,
//     and clean cancellation on detach or publication lag (S4).
//   - Backend & Volume integration (backend.go, volume_integration.go):
//     CountingBackend instruments canonical backend.Backend with demand vs
//     prefetch S3 GET accounting and optional latency/bandwidth pacing;
//     VolumeTracePrefetcher integrates directly with *vfs.Volume.
package thinkahead
