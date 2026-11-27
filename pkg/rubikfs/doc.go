// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package rubikfs implements similarity-clustered immutable base-image packing
// for hanzoai/vfs (RubikFS, FAST 2026; HIP-1344 / HIP-1345).
//
// In sandbox environments (hanzoai/visor mounting hanzoai/vfs over hanzoai/s3),
// immutable base images contain thousands of files across disparate directory
// subtrees (compiler toolchains, language standard libraries, shared binaries,
// and runtime manifests) that share substantial cross-file byte similarity.
// Packing chunks in directory-walk order or random order scatters similar
// chunks across separate packs and beyond LZ77 sliding windows.
//
// Package rubikfs rearranges immutable base-image chunks by content similarity
// before compressing, AEAD-sealing, and packing into bounded backend objects:
//
//   - Shift-invariant content-similarity sketching (one-permutation MinHash
//     over content-sampled 8-byte shingles plus byte-class histogram and
//     Shannon entropy estimation) with banded LSH candidate generation.
//   - Four matched packing policies for direct evaluation:
//     PolicyPathOrder, PolicyRandomOrder, PolicyPureSimilarity, and
//     PolicyLocalityHybrid (boot-hot pack pinning plus similarity clustering
//     to balance compression ratio against cold-start latency and read
//     amplification).
//   - Zero-dependency pack-wide long-range LZ77 + DEFLATE compression codec
//     (CodecLongLZFlate) with automatic raw fallback for incompressible data.
//   - Strict per-(tenant_id, volume_id) isolation: clustering, compression,
//     addressing, and AEAD sealing occur exclusively within one volume scope.
//     There is zero cross-volume or cross-tenant plaintext deduplication.
//   - End-to-end BLAKE3 integrity verification on every pack and every chunk.
//
// Package rubikfs is orthogonal to pkg/cofs (HIP-1342 read-only metadata MPHF)
// and operates strictly above pkg/backend.Backend without changing canonical
// hanzoai/vfs wire formats or introducing external dependencies.
package rubikfs
