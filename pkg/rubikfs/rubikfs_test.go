// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package rubikfs_test

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/hanzoai/vfs/pkg/rubikfs"
)

func openTestBackend(t *testing.T) backend.Backend {
	t.Helper()
	dir := t.TempDir()
	be, err := backend.Open(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("open file backend: %v", err)
	}
	t.Cleanup(func() { _ = be.Close() })
	return be
}

// makeStructuredImage builds a deterministic multi-directory base image where
// structurally similar files live in separate directory trees (mirroring real
// compiler toolchains, runtime libraries, and service configs) and a realistic
// boot working set spans /sbin, /etc, /usr/lib, and /usr/bin.
func makeStructuredImage(seed int64, numFamilies, filesPerFamily, fileSize int) ([]rubikfs.FileInput, []string) {
	rng := mathrand.New(mathrand.NewSource(seed))
	// Create distinct family templates (each 64 KiB of realistic structured tokens).
	templates := make([][]byte, numFamilies)
	for f := 0; f < numFamilies; f++ {
		var buf bytes.Buffer
		for buf.Len() < fileSize+4096 {
			switch f % 3 {
			case 0:
				// Go/source-like structured text.
				fmt.Fprintf(&buf, "package family%02d\n// module symbol table entry %06d hash=%08x\nfunc Handle_%02d_%04d(ctx Context, req *Payload) (int64, error) { return req.Size + %d, nil }\n",
					f, rng.Intn(100000), rng.Uint32(), f, rng.Intn(500), f*17+3)
			case 1:
				// JSON/manifest-like configuration blocks.
				fmt.Fprintf(&buf, `{"service":"svc-%02d","region":"us-east-1","replicas":%d,"flags":["immutable","sealed","rubikfs"],"digest":"%016x"}`+"\n",
					f, 1+rng.Intn(8), rng.Uint64())
			case 2:
				// Structured binary/ELF-like repeated opcodes + tables.
				var blk [64]byte
				for i := range blk {
					if i%8 == 0 {
						blk[i] = byte(0x48 + f)
					} else if i%4 == 0 {
						blk[i] = byte(0x89 ^ (f * 7))
					} else {
						blk[i] = byte((f*31 + i*13 + rng.Intn(4)) & 0xff)
					}
				}
				buf.Write(blk[:])
			}
		}
		templates[f] = buf.Bytes()[:fileSize]
	}

	dirs := []string{"/sbin", "/etc/visor", "/usr/bin", "/usr/lib/rt", "/opt/toolchain", "/var/lib/pkg", "/usr/share/doc", "/lib/modules"}
	var files []rubikfs.FileInput
	var bootPaths []string

	// Include an empty file to test 0-byte edge cases.
	files = append(files, rubikfs.FileInput{
		Path:    "/etc/visor/empty.marker",
		Data:    nil,
		BootHot: true,
	})
	bootPaths = append(bootPaths, "/etc/visor/empty.marker")

	for copyIdx := 0; copyIdx < filesPerFamily; copyIdx++ {
		for fam := 0; fam < numFamilies; fam++ {
			dir := dirs[(copyIdx*3+fam)%len(dirs)]
			p := fmt.Sprintf("%s/group-%02d/item-%02d-fam-%02d.dat", dir, copyIdx, copyIdx, fam)

			// Clone family template and apply ~8-12% deterministic per-file mutations
			// (unique headers, symbols, patched offsets) so no two chunks are identical.
			data := make([]byte, fileSize)
			copy(data, templates[fam])
			header := fmt.Sprintf("FILE=%s COPY=%03d FAM=%03d ", p, copyIdx, fam)
			copy(data, header)
			// Mutate 64 scattered 12-byte spans per file.
			for m := 0; m < 64; m++ {
				off := rng.Intn(fileSize - 16)
				tag := fmt.Sprintf("M%03d%03d%04x", copyIdx, fam, rng.Uint32()&0xffff)
				copy(data[off:], tag)
			}

			bootHot := copyIdx == 0 && fam < 3 // ~3 boot files spread across distinct directories & families
			files = append(files, rubikfs.FileInput{
				Path:    p,
				Data:    data,
				BootHot: bootHot,
			})
			if bootHot {
				bootPaths = append(bootPaths, p)
			}
		}
	}
	return files, bootPaths
}

func TestCodecRoundTripAndLongRangeMatches(t *testing.T) {
	// Construct a 192 KiB payload with two nearly identical 64 KiB halves separated
	// by 64 KiB of distinct structured data (distance = 128 KiB > DEFLATE's 32 KiB window).
	rng := mathrand.New(mathrand.NewSource(42))
	chunkA := make([]byte, 64<<10)
	for i := range chunkA {
		chunkA[i] = byte('A' + (i*17+rng.Intn(3))%26)
	}
	middle := make([]byte, 64<<10)
	for i := range middle {
		middle[i] = byte('0' + (i*29+rng.Intn(5))%10)
	}
	chunkB := append([]byte(nil), chunkA...)
	copy(chunkB[100:], "MODIFIED_HEADER_V2")

	payload := make([]byte, 0, 192<<10)
	payload = append(payload, chunkA...)
	payload = append(payload, middle...)
	payload = append(payload, chunkB...)

	for _, codec := range []rubikfs.CodecKind{rubikfs.CodecNone, rubikfs.CodecFlate, rubikfs.CodecLongLZFlate} {
		comp, err := rubikfs.Compress(codec, payload)
		if err != nil {
			t.Fatalf("compress %s: %v", codec, err)
		}
		got, err := rubikfs.Decompress(codec, len(payload), comp)
		if err != nil {
			t.Fatalf("decompress %s: %v", codec, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("codec %s round-trip mismatch", codec)
		}
	}

	// Verify LongLZFlate beats 32 KiB-window Flate on 128 KiB distance matches.
	flateOut, _ := rubikfs.Compress(rubikfs.CodecFlate, payload)
	longOut, _ := rubikfs.Compress(rubikfs.CodecLongLZFlate, payload)
	if len(longOut) >= len(flateOut) {
		t.Fatalf("expected CodecLongLZFlate (%d B) < CodecFlate (%d B) on 128 KiB long-range repeat",
			len(longOut), len(flateOut))
	}

	// High-entropy random payload must not expand beyond +1 byte mode header.
	randPayload := make([]byte, 64<<10)
	if _, err := rand.Read(randPayload); err != nil {
		t.Fatal(err)
	}
	compRand, err := rubikfs.Compress(rubikfs.CodecLongLZFlate, randPayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(compRand) > len(randPayload)+1 {
		t.Fatalf("incompressible payload expanded: got %d, want <= %d", len(compRand), len(randPayload)+1)
	}
	gotRand, err := rubikfs.Decompress(rubikfs.CodecLongLZFlate, len(randPayload), compRand)
	if err != nil || !bytes.Equal(gotRand, randPayload) {
		t.Fatalf("incompressible round-trip failed: err=%v", err)
	}
}

func TestSketchShiftInvarianceAndEntropy(t *testing.T) {
	rng := mathrand.New(mathrand.NewSource(7))
	base := make([]byte, 16<<10)
	for i := range base {
		base[i] = byte('a' + rng.Intn(26))
	}
	// Shift by 13 bytes at the front (breaks fixed-stride hashing, preserved by content-sampled shingles).
	shifted := append([]byte("PREFIX_13BYTE"), base[:len(base)-13]...)
	unrelated := make([]byte, 16<<10)
	for i := range unrelated {
		unrelated[i] = byte(rng.Intn(256))
	}

	skBase := rubikfs.ComputeSketch(base)
	skShift := rubikfs.ComputeSketch(shifted)
	skRand := rubikfs.ComputeSketch(unrelated)

	simShift := rubikfs.Similarity(skBase, skShift)
	simRand := rubikfs.Similarity(skBase, skRand)

	if simShift < 0.70 {
		t.Fatalf("expected shift-invariant similarity >= 0.70, got %.3f", simShift)
	}
	if simRand > 0.30 {
		t.Fatalf("expected unrelated random similarity <= 0.30, got %.3f", simRand)
	}
	if !skRand.HighEntropy {
		t.Fatalf("expected uniform random chunk HighEntropy=true (entropy=%.2f)", skRand.Entropy)
	}
	if skBase.HighEntropy {
		t.Fatalf("expected lowercase ASCII chunk HighEntropy=false (entropy=%.2f)", skBase.Entropy)
	}
}

func TestAllPoliciesRoundTripCompressionAndColdStart(t *testing.T) {
	ctx := context.Background()
	files, bootPaths := makeStructuredImage(1344, 12, 8, 32<<10)
	expected := make(map[string][]byte, len(files))
	for _, f := range files {
		expected[f.Path] = f.Data
	}

	scope := rubikfs.Scope{TenantID: "tenant-alpha", VolumeID: "vol-base-01"}
	secret := []byte("0123456789abcdef0123456789abcdef")
	cipher, err := rubikfs.NewVolumeCipher(scope, secret)
	if err != nil {
		t.Fatal(err)
	}

	results := map[rubikfs.Policy]rubikfs.BuildStats{}
	readResults := map[rubikfs.Policy]rubikfs.ReadStats{}

	for _, pol := range []rubikfs.Policy{
		rubikfs.PolicyPathOrder,
		rubikfs.PolicyRandomOrder,
		rubikfs.PolicyPureSimilarity,
		rubikfs.PolicyLocalityHybrid,
	} {
		be := openTestBackend(t)
		cfg := rubikfs.Config{
			Scope:           scope,
			ImageID:         "img-" + string(pol),
			Policy:          pol,
			Codec:           rubikfs.CodecLongLZFlate,
			ChunkSize:       16 << 10,
			TargetPackBytes: 256 << 10,
			BootPackBytes:   256 << 10,
			Seed:            1344,
		}
		_, bstats, err := rubikfs.BuildImage(ctx, be, cipher, cfg, files)
		if err != nil {
			t.Fatalf("BuildImage(%s): %v", pol, err)
		}
		results[pol] = bstats

		rdr, err := rubikfs.OpenImage(ctx, be, cipher, scope, cfg.ImageID)
		if err != nil {
			t.Fatalf("OpenImage(%s): %v", pol, err)
		}
		if err := rdr.VerifyAll(ctx, expected); err != nil {
			t.Fatalf("VerifyAll(%s): %v", pol, err)
		}

		cold, err := rdr.ReplayColdStart(ctx, bootPaths)
		if err != nil {
			t.Fatalf("ReplayColdStart(%s): %v", pol, err)
		}
		readResults[pol] = cold
	}

	// Acceptance 1: Similarity clustering improves compression ratio over path & random baselines.
	if results[rubikfs.PolicyPureSimilarity].CompressionRatio <= results[rubikfs.PolicyPathOrder].CompressionRatio {
		t.Fatalf("expected pure similarity compression (%.3fx) > path baseline (%.3fx)",
			results[rubikfs.PolicyPureSimilarity].CompressionRatio,
			results[rubikfs.PolicyPathOrder].CompressionRatio)
	}
	if results[rubikfs.PolicyLocalityHybrid].CompressionRatio <= results[rubikfs.PolicyPathOrder].CompressionRatio {
		t.Fatalf("expected locality hybrid compression (%.3fx) > path baseline (%.3fx)",
			results[rubikfs.PolicyLocalityHybrid].CompressionRatio,
			results[rubikfs.PolicyPathOrder].CompressionRatio)
	}

	// Acceptance 2 & Negative Regime #1: Pure similarity scatters boot files across multiple
	// packs, causing higher cold-start pack fetches / read amp than locality hybrid!
	if readResults[rubikfs.PolicyLocalityHybrid].PacksFetched >= readResults[rubikfs.PolicyPureSimilarity].PacksFetched {
		t.Fatalf("expected locality hybrid boot packs fetched (%d) < pure similarity (%d)",
			readResults[rubikfs.PolicyLocalityHybrid].PacksFetched,
			readResults[rubikfs.PolicyPureSimilarity].PacksFetched)
	}
	if readResults[rubikfs.PolicyLocalityHybrid].ReadAmplification >= readResults[rubikfs.PolicyPureSimilarity].ReadAmplification {
		t.Fatalf("expected locality hybrid read amp (%.2fx) < pure similarity (%.2fx)",
			readResults[rubikfs.PolicyLocalityHybrid].ReadAmplification,
			readResults[rubikfs.PolicyPureSimilarity].ReadAmplification)
	}
}

func TestTenantIsolationNoCrossVolumeDedup(t *testing.T) {
	ctx := context.Background()
	be := openTestBackend(t)

	// Identical plaintext files for two separate tenants/volumes on the SAME backend.
	files, _ := makeStructuredImage(99, 4, 4, 16<<10)
	sharedMaster := []byte("shared-kms-root-secret-32-bytes!")

	scopeA := rubikfs.Scope{TenantID: "tenant-a", VolumeID: "vol-a"}
	scopeB := rubikfs.Scope{TenantID: "tenant-b", VolumeID: "vol-b"}
	cipherA, err := rubikfs.NewVolumeCipher(scopeA, sharedMaster)
	if err != nil {
		t.Fatal(err)
	}
	cipherB, err := rubikfs.NewVolumeCipher(scopeB, sharedMaster)
	if err != nil {
		t.Fatal(err)
	}

	cfgA := rubikfs.Config{Scope: scopeA, ImageID: "base-v1", Policy: rubikfs.PolicyLocalityHybrid, Codec: rubikfs.CodecLongLZFlate, TargetPackBytes: 256 << 10}
	cfgB := rubikfs.Config{Scope: scopeB, ImageID: "base-v1", Policy: rubikfs.PolicyLocalityHybrid, Codec: rubikfs.CodecLongLZFlate, TargetPackBytes: 256 << 10}

	manA, statsA, err := rubikfs.BuildImage(ctx, be, cipherA, cfgA, files)
	if err != nil {
		t.Fatalf("BuildImage A: %v", err)
	}
	manB, statsB, err := rubikfs.BuildImage(ctx, be, cipherB, cfgB, files)
	if err != nil {
		t.Fatalf("BuildImage B: %v", err)
	}

	// 1. Zero shared backend keys across volumes.
	keysA := map[string][]byte{}
	for _, p := range manA.Packs {
		raw, _, err := be.Get(ctx, p.BackendKey, backend.Unbounded)
		if err != nil {
			t.Fatalf("get A pack: %v", err)
		}
		keysA[p.BackendKey] = raw
	}
	keysA[rubikfs.ManifestKey(scopeA, "base-v1")] = nil

	for _, p := range manB.Packs {
		if _, overlap := keysA[p.BackendKey]; overlap {
			t.Fatalf("cross-volume key collision: %s", p.BackendKey)
		}
		rawB, _, err := be.Get(ctx, p.BackendKey, backend.Unbounded)
		if err != nil {
			t.Fatalf("get B pack: %v", err)
		}
		// 2. Zero identical ciphertexts between A and B.
		for kA, rawA := range keysA {
			if rawA != nil && bytes.Equal(rawA, rawB) {
				t.Fatalf("identical ciphertext across volumes: %s == %s", kA, p.BackendKey)
			}
		}
	}

	// 3. Total backend bytes stored == bytes(A) + bytes(B) (0% cross-volume dedup).
	var backendTotal int64
	objs, errCh := be.List(ctx, "rubikfs/")
	for obj := range objs {
		backendTotal += obj.Size
	}
	if err := <-errCh; err != nil {
		t.Fatalf("list backend: %v", err)
	}
	wantTotal := statsA.TotalStoredBytes + statsB.TotalStoredBytes
	if backendTotal != wantTotal {
		t.Fatalf("backend total bytes %d != A+B (%d); cross-volume dedup must be 0", backendTotal, wantTotal)
	}

	// 4. Cross-volume OpenImage must fail closed.
	if _, err := rubikfs.OpenImage(ctx, be, cipherB, scopeA, "base-v1"); err == nil {
		t.Fatal("expected OpenImage(scopeA) with cipherB to fail closed")
	}

	// 5. Transplanting Volume A's sealed pack bytes into Volume B's pack key must fail closed.
	packABytes, _, err := be.Get(ctx, manA.Packs[0].BackendKey, backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	if err := be.Put(ctx, manB.Packs[0].BackendKey, packABytes); err != nil {
		t.Fatal(err)
	}
	rdrB, err := rubikfs.OpenImage(ctx, be, cipherB, scopeB, "base-v1")
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	// Find a file referencing pack 0.
	var targetPath string
	for _, fm := range manB.Files {
		for _, loc := range fm.Chunks {
			if loc.PackIndex == 0 {
				targetPath = fm.Path
				break
			}
		}
		if targetPath != "" {
			break
		}
	}
	if _, err := rdrB.ReadFile(ctx, targetPath); err == nil {
		t.Fatal("expected reading transplanted cross-volume pack to fail closed")
	}

	// 6. Mixed-scope FileInput in a single BuildImage must be refused with ErrTenantIsolation.
	mixed := append([]rubikfs.FileInput(nil), files[:2]...)
	mixed[0].Scope = scopeA
	mixed[1].Scope = scopeB
	if _, _, err := rubikfs.BuildImage(ctx, be, cipherA, cfgA, mixed); !errors.Is(err, rubikfs.ErrTenantIsolation) {
		t.Fatalf("expected ErrTenantIsolation for mixed-scope build, got %v", err)
	}
}

func TestPostQuantumVFSHybridCipherCompatibility(t *testing.T) {
	ctx := context.Background()
	be := openTestBackend(t)

	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatalf("generate hybrid identity: %v", err)
	}
	pqCipher := vfs.Hybrid(id)

	files, _ := makeStructuredImage(55, 3, 3, 16<<10)
	expected := make(map[string][]byte, len(files))
	for _, f := range files {
		expected[f.Path] = f.Data
	}

	scope := rubikfs.Scope{TenantID: "pq-tenant", VolumeID: "pq-vol"}
	cfg := rubikfs.Config{
		Scope:           scope,
		ImageID:         "pq-img",
		Policy:          rubikfs.PolicyLocalityHybrid,
		Codec:           rubikfs.CodecLongLZFlate,
		TargetPackBytes: 256 << 10,
	}
	if _, _, err := rubikfs.BuildImage(ctx, be, pqCipher, cfg, files); err != nil {
		t.Fatalf("BuildImage with vfs.Hybrid: %v", err)
	}
	rdr, err := rubikfs.OpenImage(ctx, be, pqCipher, scope, "pq-img")
	if err != nil {
		t.Fatalf("OpenImage with vfs.Hybrid: %v", err)
	}
	if err := rdr.VerifyAll(ctx, expected); err != nil {
		t.Fatalf("VerifyAll with vfs.Hybrid: %v", err)
	}
}

func TestSingleBitCorruptionRejected(t *testing.T) {
	ctx := context.Background()
	be := openTestBackend(t)

	scope := rubikfs.Scope{TenantID: "t-corrupt", VolumeID: "v-corrupt"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("secret-key-for-corruption-tests!"))
	if err != nil {
		t.Fatal(err)
	}
	files, _ := makeStructuredImage(11, 2, 3, 16<<10)
	cfg := rubikfs.Config{Scope: scope, ImageID: "img-corrupt", Policy: rubikfs.PolicyPureSimilarity, Codec: rubikfs.CodecLongLZFlate, TargetPackBytes: 256 << 10}
	man, _, err := rubikfs.BuildImage(ctx, be, cipher, cfg, files)
	if err != nil {
		t.Fatal(err)
	}

	// Flip 1 bit in pack 0 ciphertext -> must fail with ErrCorruptPack.
	p0Key := man.Packs[0].BackendKey
	orig, _, err := be.Get(ctx, p0Key, backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := append([]byte(nil), orig...)
	corrupted[len(corrupted)/2] ^= 0x01
	if err := be.Put(ctx, p0Key, corrupted); err != nil {
		t.Fatal(err)
	}

	rdr, err := rubikfs.OpenImage(ctx, be, cipher, scope, "img-corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rdr.ReadFile(ctx, man.Files[1].Path); !errors.Is(err, rubikfs.ErrCorruptPack) {
		t.Fatalf("expected ErrCorruptPack on single-bit pack flip, got %v", err)
	}

	// Restore pack 0 and flip 1 bit in manifest ciphertext -> OpenImage must fail with ErrCorruptPack.
	if err := be.Put(ctx, p0Key, orig); err != nil {
		t.Fatal(err)
	}
	mKey := rubikfs.ManifestKey(scope, "img-corrupt")
	mOrig, _, err := be.Get(ctx, mKey, backend.Unbounded)
	if err != nil {
		t.Fatal(err)
	}
	mBad := append([]byte(nil), mOrig...)
	mBad[len(mBad)-5] ^= 0x80
	if err := be.Put(ctx, mKey, mBad); err != nil {
		t.Fatal(err)
	}
	if _, err := rubikfs.OpenImage(ctx, be, cipher, scope, "img-corrupt"); !errors.Is(err, rubikfs.ErrCorruptPack) {
		t.Fatalf("expected ErrCorruptPack on single-bit manifest flip, got %v", err)
	}
}

func TestConcurrentReadersSingleFlight(t *testing.T) {
	ctx := context.Background()
	be := openTestBackend(t)

	scope := rubikfs.Scope{TenantID: "t-race", VolumeID: "v-race"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("concurrent-reader-secret-32bytes"))
	if err != nil {
		t.Fatal(err)
	}
	files, _ := makeStructuredImage(77, 6, 4, 24<<10)
	expected := make(map[string][]byte, len(files))
	for _, f := range files {
		expected[f.Path] = f.Data
	}

	cfg := rubikfs.Config{Scope: scope, ImageID: "img-race", Policy: rubikfs.PolicyLocalityHybrid, Codec: rubikfs.CodecLongLZFlate, TargetPackBytes: 256 << 10}
	man, _, err := rubikfs.BuildImage(ctx, be, cipher, cfg, files)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := rubikfs.OpenImage(ctx, be, cipher, scope, "img-race")
	if err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < len(man.Files); i++ {
				fm := man.Files[(i+id)%len(man.Files)]
				got, err := rdr.ReadFile(ctx, fm.Path)
				if err != nil {
					errCh <- fmt.Errorf("worker %d read %s: %w", id, fm.Path, err)
					return
				}
				if !bytes.Equal(got, expected[fm.Path]) {
					errCh <- fmt.Errorf("worker %d mismatch on %s", id, fm.Path)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	stats := rdr.Stats()
	if stats.PacksFetched > len(man.Packs) {
		t.Fatalf("single-flight violated: fetched %d packs for %d-pack image", stats.PacksFetched, len(man.Packs))
	}
}

func TestNegativeRegimesIncompressibleAndOversizedPacks(t *testing.T) {
	ctx := context.Background()
	scope := rubikfs.Scope{TenantID: "t-neg", VolumeID: "v-neg"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("negative-regime-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}

	// Negative Regime #2: Incompressible / pre-compressed high-entropy payload yields ~1.00x.
	var randFiles []rubikfs.FileInput
	for i := 0; i < 16; i++ {
		blob := make([]byte, 32<<10)
		if _, err := rand.Read(blob); err != nil {
			t.Fatal(err)
		}
		randFiles = append(randFiles, rubikfs.FileInput{
			Path: fmt.Sprintf("/var/weights/shard-%02d.zst", i),
			Data: blob,
		})
	}
	beBase := openTestBackend(t)
	beSim := openTestBackend(t)
	_, stPath, err := rubikfs.BuildImage(ctx, beBase, cipher, rubikfs.Config{
		Scope: scope, ImageID: "rand-path", Policy: rubikfs.PolicyPathOrder, Codec: rubikfs.CodecLongLZFlate, TargetPackBytes: 256 << 10,
	}, randFiles)
	if err != nil {
		t.Fatal(err)
	}
	_, stSim, err := rubikfs.BuildImage(ctx, beSim, cipher, rubikfs.Config{
		Scope: scope, ImageID: "rand-sim", Policy: rubikfs.PolicyPureSimilarity, Codec: rubikfs.CodecLongLZFlate, TargetPackBytes: 256 << 10,
	}, randFiles)
	if err != nil {
		t.Fatal(err)
	}
	if stSim.CompressionRatio > 1.02 || stSim.CompressionRatio < 0.98 {
		t.Fatalf("expected incompressible compression ratio ~1.00x, got %.3fx", stSim.CompressionRatio)
	}
	if stPath.CompressionRatio > 1.02 || stPath.CompressionRatio < 0.98 {
		t.Fatalf("expected path incompressible ratio ~1.00x, got %.3fx", stPath.CompressionRatio)
	}

	// Negative Regime #3: Oversized pack granularity (4 MiB vs 256 KiB) blows up cold-start read amp.
	files, bootPaths := makeStructuredImage(2026, 8, 12, 32<<10) // ~3 MiB total, ~96 KiB boot
	beSmall := openTestBackend(t)
	beHuge := openTestBackend(t)
	_, _, err = rubikfs.BuildImage(ctx, beSmall, cipher, rubikfs.Config{
		Scope: scope, ImageID: "pack-256k", Policy: rubikfs.PolicyPureSimilarity, Codec: rubikfs.CodecLongLZFlate, TargetPackBytes: 256 << 10,
	}, files)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = rubikfs.BuildImage(ctx, beHuge, cipher, rubikfs.Config{
		Scope: scope, ImageID: "pack-4m", Policy: rubikfs.PolicyPureSimilarity, Codec: rubikfs.CodecLongLZFlate, TargetPackBytes: 4 << 20,
	}, files)
	if err != nil {
		t.Fatal(err)
	}
	rSmall, err := rubikfs.OpenImage(ctx, beSmall, cipher, scope, "pack-256k")
	if err != nil {
		t.Fatal(err)
	}
	rHuge, err := rubikfs.OpenImage(ctx, beHuge, cipher, scope, "pack-4m")
	if err != nil {
		t.Fatal(err)
	}
	coldSmall, err := rSmall.ReplayColdStart(ctx, bootPaths)
	if err != nil {
		t.Fatal(err)
	}
	coldHuge, err := rHuge.ReplayColdStart(ctx, bootPaths)
	if err != nil {
		t.Fatal(err)
	}
	if coldHuge.RawReadAmp <= coldSmall.RawReadAmp {
		t.Fatalf("expected oversized 4 MiB pack raw read amp (%.2fx) > 256 KiB pack (%.2fx)",
			coldHuge.RawReadAmp, coldSmall.RawReadAmp)
	}
}

func Test64KiBChunkSimilarityCompression(t *testing.T) {
	ctx := context.Background()
	files, _ := makeStructuredImage(6400, 6, 6, 64<<10) // 36 files @ 64 KiB = 2.25 MiB
	expected := make(map[string][]byte, len(files))
	for _, f := range files {
		expected[f.Path] = f.Data
	}

	scope := rubikfs.Scope{TenantID: "t-64k", VolumeID: "v-64k"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("chunk-64kib-verification-secret!"))
	if err != nil {
		t.Fatal(err)
	}

	bePath := openTestBackend(t)
	beSim := openTestBackend(t)
	_, stPath, err := rubikfs.BuildImage(ctx, bePath, cipher, rubikfs.Config{
		Scope:           scope,
		ImageID:         "c64-path",
		Policy:          rubikfs.PolicyPathOrder,
		Codec:           rubikfs.CodecLongLZFlate,
		ChunkSize:       64 << 10,
		TargetPackBytes: 512 << 10,
	}, files)
	if err != nil {
		t.Fatal(err)
	}
	_, stSim, err := rubikfs.BuildImage(ctx, beSim, cipher, rubikfs.Config{
		Scope:           scope,
		ImageID:         "c64-sim",
		Policy:          rubikfs.PolicyPureSimilarity,
		Codec:           rubikfs.CodecLongLZFlate,
		ChunkSize:       64 << 10,
		TargetPackBytes: 512 << 10,
	}, files)
	if err != nil {
		t.Fatal(err)
	}
	if stSim.CompressionRatio <= stPath.CompressionRatio {
		t.Fatalf("expected 64 KiB similarity compression (%.3fx) > path baseline (%.3fx)",
			stSim.CompressionRatio, stPath.CompressionRatio)
	}
	rdr, err := rubikfs.OpenImage(ctx, beSim, cipher, scope, "c64-sim")
	if err != nil {
		t.Fatal(err)
	}
	if err := rdr.VerifyAll(ctx, expected); err != nil {
		t.Fatalf("VerifyAll 64 KiB: %v", err)
	}
}

func TestConfigValidationAndIntraVolumeDedup(t *testing.T) {
	ctx := context.Background()
	be := openTestBackend(t)
	scope := rubikfs.Scope{TenantID: "t-cfg", VolumeID: "v-cfg"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("config-validation-secret-32bytes"))
	if err != nil {
		t.Fatal(err)
	}

	// Empty tenant/volume must be rejected.
	if _, _, err := rubikfs.BuildImage(ctx, be, cipher, rubikfs.Config{ImageID: "x"}, nil); !errors.Is(err, rubikfs.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig for empty scope, got %v", err)
	}
	// Unknown policy must be rejected.
	if _, _, err := rubikfs.BuildImage(ctx, be, cipher, rubikfs.Config{Scope: scope, ImageID: "x", Policy: "invalid"}, nil); !errors.Is(err, rubikfs.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig for invalid policy, got %v", err)
	}

	// Intra-volume exact dedup works strictly inside one volume and reconstructs identically.
	dupChunk := bytes.Repeat([]byte("HANZO_RUBIKFS_INTRA_VOLUME_BLOCK_"), 600)
	files := []rubikfs.FileInput{
		{Path: "/a/one.bin", Data: dupChunk, BootHot: true},
		{Path: "/b/two.bin", Data: dupChunk, BootHot: false},
	}
	man, _, err := rubikfs.BuildImage(ctx, be, cipher, rubikfs.Config{
		Scope:            scope,
		ImageID:          "intra-dedup",
		Policy:           rubikfs.PolicyLocalityHybrid,
		Codec:            rubikfs.CodecLongLZFlate,
		ChunkSize:        16 << 10,
		TargetPackBytes:  256 << 10,
		IntraVolumeDedup: true,
	}, files)
	if err != nil {
		t.Fatal(err)
	}
	if man.UniqueChunks >= man.TotalChunks {
		t.Fatalf("expected intra-volume dedup to reduce unique chunks (%d < %d)", man.UniqueChunks, man.TotalChunks)
	}
	rdr, err := rubikfs.OpenImage(ctx, be, cipher, scope, "intra-dedup")
	if err != nil {
		t.Fatal(err)
	}
	if err := rdr.VerifyAll(ctx, map[string][]byte{"/a/one.bin": dupChunk, "/b/two.bin": dupChunk}); err != nil {
		t.Fatalf("VerifyAll intra-dedup: %v", err)
	}
}

func TestCodexP1ScopeNULRejectionAndUnambiguousKeyDerivation(t *testing.T) {
	master := []byte("shared-master-key-for-scope-test!")
	s1 := rubikfs.Scope{TenantID: "a", VolumeID: "b\x00c"}
	s2 := rubikfs.Scope{TenantID: "a\x00b", VolumeID: "c"}
	if err := s1.Validate(); !errors.Is(err, rubikfs.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig for NUL in VolumeID, got %v", err)
	}
	if err := s2.Validate(); !errors.Is(err, rubikfs.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig for NUL in TenantID, got %v", err)
	}
	if _, err := rubikfs.NewVolumeCipher(s1, master); !errors.Is(err, rubikfs.ErrInvalidConfig) {
		t.Fatalf("expected NewVolumeCipher to reject NUL scope, got %v", err)
	}
	// Distinct clean scopes with concatenations that would collide without length prefixing.
	ca, err := rubikfs.NewVolumeCipher(rubikfs.Scope{TenantID: "ab", VolumeID: "c"}, master)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := rubikfs.NewVolumeCipher(rubikfs.Scope{TenantID: "a", VolumeID: "bc"}, master)
	if err != nil {
		t.Fatal(err)
	}
	sealed, keyID, err := ca.Seal([]byte("isolated-payload"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cb.Open(sealed, keyID); !errors.Is(err, rubikfs.ErrTenantIsolation) {
		t.Fatalf("expected ErrTenantIsolation across distinct scopes, got %v", err)
	}
}

func TestCodexP2LargePackWithFullChunkTableFetchBound(t *testing.T) {
	ctx := context.Background()
	be := openTestBackend(t)
	scope := rubikfs.Scope{TenantID: "t-32m", VolumeID: "v-32m"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("pack-bound-regression-key-32byte"))
	if err != nil {
		t.Fatal(err)
	}

	// 32 MiB incompressible payload at 16 KiB chunks = 2,048 chunk entries (~80 KiB table) + CodecNone.
	// Sealed object exceeds 32 MiB + 64 KiB and previously failed with backend.ErrTooLarge.
	payload := make([]byte, 32<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	cfg := rubikfs.Config{
		Scope:           scope,
		ImageID:         "img-32m-full-table",
		Policy:          rubikfs.PolicyPathOrder,
		Codec:           rubikfs.CodecNone,
		ChunkSize:       16 << 10,
		TargetPackBytes: 32 << 20,
	}
	man, _, err := rubikfs.BuildImage(ctx, be, cipher, cfg, []rubikfs.FileInput{
		{Path: "/weights/full32m.bin", Data: payload},
	})
	if err != nil {
		t.Fatalf("BuildImage 32 MiB pack failed: %v", err)
	}
	if len(man.Packs) != 1 || man.Packs[0].SealedBytes <= (32<<20)+(64<<10) {
		t.Fatalf("expected single sealed pack > 32MiB+64KiB, got packs=%d sealed=%d", len(man.Packs), man.Packs[0].SealedBytes)
	}
	rdr, err := rubikfs.OpenImage(ctx, be, cipher, scope, "img-32m-full-table")
	if err != nil {
		t.Fatalf("OpenImage 32 MiB pack failed: %v", err)
	}
	got, err := rdr.ReadFile(ctx, "/weights/full32m.bin")
	if err != nil {
		t.Fatalf("ReadFile 32 MiB pack failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("32 MiB pack round-trip payload mismatch")
	}
}

func TestCodexP2NonUTF8FilenamePreservedInManifest(t *testing.T) {
	ctx := context.Background()
	be := openTestBackend(t)
	scope := rubikfs.Scope{TenantID: "t-posix", VolumeID: "v-posix"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("nonutf8-posix-filename-key-32byt"))
	if err != nil {
		t.Fatal(err)
	}

	hostilePath := "/dir/nonutf8-\xff\xfe-file.bin"
	hostileData := []byte("exact bytes behind non-utf8 posix filename")
	utf8Path := "/dir/normal.txt"
	utf8Data := []byte("standard utf8 filename payload")

	cfg := rubikfs.Config{
		Scope:   scope,
		ImageID: "img-hostile-names",
	}
	man, _, err := rubikfs.BuildImage(ctx, be, cipher, cfg, []rubikfs.FileInput{
		{Path: hostilePath, Data: hostileData, BootHot: true},
		{Path: utf8Path, Data: utf8Data},
	})
	if err != nil {
		t.Fatalf("BuildImage with non-UTF-8 path failed: %v", err)
	}
	foundInBuilt := false
	for _, fm := range man.Files {
		if fm.Path == hostilePath {
			foundInBuilt = true
		}
	}
	if !foundInBuilt {
		t.Fatalf("expected built Manifest to retain exact non-UTF-8 path")
	}

	rdr, err := rubikfs.OpenImage(ctx, be, cipher, scope, "img-hostile-names")
	if err != nil {
		t.Fatalf("OpenImage failed on non-UTF-8 filename: %v", err)
	}
	gotHostile, err := rdr.ReadFile(ctx, hostilePath)
	if err != nil {
		t.Fatalf("ReadFile on non-UTF-8 filename failed: %v", err)
	}
	if !bytes.Equal(gotHostile, hostileData) {
		t.Fatalf("non-UTF-8 file content mismatch: got %q want %q", gotHostile, hostileData)
	}
	gotUTF8, err := rdr.ReadFile(ctx, utf8Path)
	if err != nil || !bytes.Equal(gotUTF8, utf8Data) {
		t.Fatalf("UTF-8 file read failed: err=%v", err)
	}
}

type blockingPackBackend struct {
	backend.Backend
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingPackBackend) Get(ctx context.Context, key string, maxBytes int64) ([]byte, backend.Version, error) {
	if bytes.Contains([]byte(key), []byte("/pack/")) {
		b.once.Do(func() { close(b.started) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return b.Backend.Get(ctx, key, maxBytes)
}

func TestCodexP2SingleFlightIsolatesCallerCancellation(t *testing.T) {
	ctx := context.Background()
	rawBe := openTestBackend(t)
	scope := rubikfs.Scope{TenantID: "t-sf", VolumeID: "v-sf"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("singleflight-cancel-key-32bytes!"))
	if err != nil {
		t.Fatal(err)
	}

	want := bytes.Repeat([]byte("singleflight-pack-payload-"), 200)
	cfg := rubikfs.Config{Scope: scope, ImageID: "img-sf"}
	if _, _, err := rubikfs.BuildImage(ctx, rawBe, cipher, cfg, []rubikfs.FileInput{
		{Path: "/shared.bin", Data: want},
	}); err != nil {
		t.Fatal(err)
	}

	blockBe := &blockingPackBackend{
		Backend: rawBe,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	rdr, err := rubikfs.OpenImage(ctx, blockBe, cipher, scope, "img-sf")
	if err != nil {
		t.Fatal(err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	err1Ch := make(chan error, 1)
	go func() {
		_, err := rdr.ReadFile(ctx1, "/shared.bin")
		err1Ch <- err
	}()

	// Wait until caller 1 starts the backend fetch, then start live caller 2 on the same pack.
	<-blockBe.started
	res2Ch := make(chan []byte, 1)
	err2Ch := make(chan error, 1)
	go func() {
		got, err := rdr.ReadFile(context.Background(), "/shared.bin")
		res2Ch <- got
		err2Ch <- err
	}()

	// Give caller 2 a moment to join the inflight wait, then cancel caller 1 and release backend.
	time.Sleep(15 * time.Millisecond)
	cancel1()
	if err := <-err1Ch; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected caller 1 to receive context.Canceled, got %v", err)
	}
	close(blockBe.release)

	if err := <-err2Ch; err != nil {
		t.Fatalf("live caller 2 failed due to caller 1 cancellation: %v", err)
	}
	if got := <-res2Ch; !bytes.Equal(got, want) {
		t.Fatalf("caller 2 payload mismatch")
	}
}

func TestCodexP2LongLZMatchLengthOverflowRejectedWithoutPanic(t *testing.T) {
	// Craft a malformed LongLZFlate stream with litLen=1, dist=1, extraLen=MaxUint64-16.
	var cmdBuf bytes.Buffer
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], 1)
	cmdBuf.Write(tmp[:n])
	n = binary.PutUvarint(tmp[:], 1)
	cmdBuf.Write(tmp[:n])
	n = binary.PutUvarint(tmp[:], math.MaxUint64-16)
	cmdBuf.Write(tmp[:n])

	compressStream := func(src []byte) []byte {
		var out bytes.Buffer
		w, err := flate.NewWriter(&out, flate.DefaultCompression)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write(src)
		_ = w.Close()
		return out.Bytes()
	}
	cmdComp := compressStream(cmdBuf.Bytes())
	litComp := compressStream([]byte{'A'})

	var wire bytes.Buffer
	wire.WriteByte(0x02) // codecModeLongLZFlate
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(cmdComp)))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(litComp)))
	wire.Write(hdr[:])
	wire.Write(cmdComp)
	wire.Write(litComp)

	if _, err := rubikfs.Decompress(rubikfs.CodecLongLZFlate, 1, wire.Bytes()); !errors.Is(err, rubikfs.ErrCorruptPack) {
		t.Fatalf("expected ErrCorruptPack on uint64 match-length overflow, got %v", err)
	}
}

func TestCodexP2OmittedCodecDefaultsToLongLZFlate(t *testing.T) {
	ctx := context.Background()
	be := openTestBackend(t)
	scope := rubikfs.Scope{TenantID: "t-defcodec", VolumeID: "v-defcodec"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("default-codec-regression-32bytes"))
	if err != nil {
		t.Fatal(err)
	}

	repetitive := bytes.Repeat([]byte("HANZO_RUBIKFS_DEFAULT_CODEC_REPETITIVE_PAYLOAD_"), 26000) // ~1.2 MB
	man, stats, err := rubikfs.BuildImage(ctx, be, cipher, rubikfs.Config{
		Scope:   scope,
		ImageID: "img-default-codec",
		// Codec intentionally omitted (zero value -> must normalize to CodecLongLZFlate).
	}, []rubikfs.FileInput{{Path: "/rep.txt", Data: repetitive}})
	if err != nil {
		t.Fatal(err)
	}
	if man.Codec != rubikfs.CodecLongLZFlate {
		t.Fatalf("expected omitted Codec to normalize to CodecLongLZFlate, got %v", man.Codec)
	}
	if stats.CompressionRatio < 10.0 {
		t.Fatalf("expected strong compression on repetitive 1.2 MB payload with default codec, got %.2fx", stats.CompressionRatio)
	}

	// Explicit CodecNone remains uncompressed.
	manNone, statsNone, err := rubikfs.BuildImage(ctx, be, cipher, rubikfs.Config{
		Scope:   scope,
		ImageID: "img-explicit-none",
		Codec:   rubikfs.CodecNone,
	}, []rubikfs.FileInput{{Path: "/rep.txt", Data: repetitive}})
	if err != nil {
		t.Fatal(err)
	}
	if manNone.Codec != rubikfs.CodecNone || statsNone.CompressedBytes != statsNone.UncompressedBytes {
		t.Fatalf("expected explicit CodecNone to store uncompressed, got codec=%v comp=%d uncomp=%d",
			manNone.Codec, statsNone.CompressedBytes, statsNone.UncompressedBytes)
	}
}

func TestCodexP2OversizedManifestRejectedBeforeUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 32 MiB manifest build in -short mode")
	}
	ctx := context.Background()
	be := openTestBackend(t)
	scope := rubikfs.Scope{TenantID: "t-bigman", VolumeID: "v-bigman"}
	cipher, err := rubikfs.NewVolumeCipher(scope, []byte("oversized-manifest-key-32bytes!!"))
	if err != nil {
		t.Fatal(err)
	}

	// 33,000 empty files with ~1 KiB paths produce > 32 MiB serialized manifest.
	pad := string(bytes.Repeat([]byte("a"), 1000))
	files := make([]rubikfs.FileInput, 33000)
	for i := range files {
		files[i] = rubikfs.FileInput{
			Path: fmt.Sprintf("/d/%06d/%s", i, pad),
		}
	}
	_, _, err = rubikfs.BuildImage(ctx, be, cipher, rubikfs.Config{
		Scope:   scope,
		ImageID: "img-oversized-manifest",
	}, files)
	if !errors.Is(err, backend.ErrTooLarge) {
		t.Fatalf("expected backend.ErrTooLarge when sealed manifest exceeds MaxManifestBytes, got %v", err)
	}
	if _, err := rubikfs.OpenImage(ctx, be, cipher, scope, "img-oversized-manifest"); !errors.Is(err, rubikfs.ErrNotFound) {
		t.Fatalf("expected oversized manifest not to be uploaded, got OpenImage err=%v", err)
	}
}
