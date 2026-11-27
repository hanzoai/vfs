// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hanzoai/vfs/pkg/rubikfs"
)

// CorpusSummary describes the composition of a benchmark image corpus.
type CorpusSummary struct {
	Name            string  `json:"name"`
	RepoFiles       int     `json:"repo_files"`
	RepoBytes       int64   `json:"repo_bytes"`
	SyntheticFiles  int     `json:"synthetic_files"`
	SyntheticBytes  int64   `json:"synthetic_bytes"`
	TotalFiles      int     `json:"total_files"`
	TotalBytes      int64   `json:"total_bytes"`
	BootFiles       int     `json:"boot_files"`
	BootBytes       int64   `json:"boot_bytes"`
	BootFractionPct float64 `json:"boot_fraction_pct"`
}

// buildVisorBaseImage constructs a realistic immutable visor base image grounded in:
//  1. Real versioned source, doc, and config files from repoRoot (hanzoai/vfs).
//  2. Deterministic seeded (--seed) ELF/shared-lib binary payloads, multi-directory
//     toolchain/stdlib packages, and structured runtime manifests simulating a real
//     sandbox OS + compiler image with cross-directory similarity.
func buildVisorBaseImage(repoRoot string, seed int64) ([]rubikfs.FileInput, []string, CorpusSummary, error) {
	var files []rubikfs.FileInput
	var bootPaths []string
	var summary CorpusSummary
	summary.Name = "visor-base-image-grounded"

	seen := make(map[string]struct{})
	addFile := func(f rubikfs.FileInput, isRepo bool) {
		if _, ok := seen[f.Path]; ok {
			return
		}
		seen[f.Path] = struct{}{}
		files = append(files, f)
		sz := int64(len(f.Data))
		summary.TotalFiles++
		summary.TotalBytes += sz
		if isRepo {
			summary.RepoFiles++
			summary.RepoBytes += sz
		} else {
			summary.SyntheticFiles++
			summary.SyntheticBytes += sz
		}
		if f.BootHot {
			bootPaths = append(bootPaths, f.Path)
			summary.BootFiles++
			summary.BootBytes += sz
		}
	}

	// 1. Ingest real versioned repository files from hanzoai/vfs.
	if repoRoot != "" {
		var repoRel []string
		err := filepath.WalkDir(repoRoot, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := d.Name()
			if d.IsDir() {
				if name == ".git" || name == "vendor" || name == "node_modules" {
					return filepath.SkipDir
				}
				return nil
			}
			rel, err := filepath.Rel(repoRoot, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			// Skip generated research reports so benchmark runs are idempotent.
			if strings.HasPrefix(rel, "docs/research/hip-1344-rubikfs-images/") {
				return nil
			}
			ext := strings.ToLower(filepath.Ext(name))
			switch ext {
			case ".go", ".md", ".json", ".yml", ".yaml", ".mod", ".txt", ".s":
			default:
				if name != "Makefile" && name != "Dockerfile" && name != "LICENSE" && name != "VERSION" {
					return nil
				}
			}
			info, err := d.Info()
			if err != nil || info.Size() > 512<<10 {
				return nil
			}
			repoRel = append(repoRel, rel)
			return nil
		})
		if err != nil {
			return nil, nil, summary, fmt.Errorf("walk repo %q: %w", repoRoot, err)
		}
		sort.Strings(repoRel)
		for _, rel := range repoRel {
			raw, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(rel)))
			if err != nil {
				return nil, nil, summary, err
			}
			// Mark core runtime entry files as boot-hot.
			bootHot := rel == "volume.go" || rel == "pack.go" || rel == "cipher.go" ||
				rel == "chunk.go" || rel == "pkg/backend/backend.go" || rel == "go.mod"
			addFile(rubikfs.FileInput{
				Path:    "/src/hanzo/vfs/" + rel,
				Data:    raw,
				BootHot: bootHot,
			}, true)
		}
	}

	// 2. Generate deterministic visor base-image layers (ELF binaries, toolchains, manifests).
	rng := mathrand.New(mathrand.NewSource(seed))

	// A. Structured ELF/shared-library templates (8 runtime/ABI families x 10 binaries = 80 binaries @ 80 KiB = 6.4 MiB).
	// Each family represents a compiled ABI/runtime (libc/musl, crypto, go-runtime, python-ext,
	// llvm-pass, sqlite, fuse-shim, network-stack) distributed across /sbin, /usr/bin, /usr/lib, /opt/toolchain.
	const (
		elfFamilies  = 8
		elfPerFamily = 10
		elfSize      = 80 << 10
	)
	elfTemplates := make([][]byte, elfFamilies)
	for f := 0; f < elfFamilies; f++ {
		elfTemplates[f] = makeELFTemplate(rng, f, elfSize)
	}
	elfDirs := []string{
		"/sbin",
		"/usr/bin",
		"/usr/lib/x86_64-linux-gnu",
		"/usr/libexec/visor",
		"/opt/toolchain/bin",
		"/opt/toolchain/lib",
		"/lib/x86_64-linux-gnu",
		"/usr/local/bin",
	}
	for copyIdx := 0; copyIdx < elfPerFamily; copyIdx++ {
		for fam := 0; fam < elfFamilies; fam++ {
			dir := elfDirs[(copyIdx*3+fam)%len(elfDirs)]
			ext := ".bin"
			if strings.Contains(dir, "lib") {
				ext = ".so.1"
			}
			p := fmt.Sprintf("%s/mod-%02d-abi-%02d%s", dir, copyIdx, fam, ext)
			data := mutateBinary(rng, elfTemplates[fam], p, copyIdx, fam, 96)
			// Pin 6 critical runtime binaries across distinct directories as boot-hot (~480 KiB).
			bootHot := copyIdx == 0 && fam < 6
			addFile(rubikfs.FileInput{
				Path:    p,
				Data:    data,
				BootHot: bootHot,
			}, false)
		}
	}

	// B. Multi-directory language stdlib / package sources (8 families x 12 files = 96 files @ 40 KiB = 3.84 MiB).
	const (
		srcFamilies  = 8
		srcPerFamily = 12
		srcSize      = 40 << 10
	)
	srcTemplates := make([][]byte, srcFamilies)
	for f := 0; f < srcFamilies; f++ {
		srcTemplates[f] = makeSourceTemplate(rng, f, srcSize)
	}
	srcDirs := []string{
		"/usr/lib/go/src/runtime",
		"/usr/lib/go/src/net",
		"/usr/lib/python3/site-packages/visor",
		"/usr/lib/python3/site-packages/stdlib",
		"/usr/include/x86_64-linux-gnu",
		"/opt/sdk/packages/core",
		"/opt/sdk/packages/net",
		"/usr/share/toolchain/stubs",
	}
	for copyIdx := 0; copyIdx < srcPerFamily; copyIdx++ {
		for fam := 0; fam < srcFamilies; fam++ {
			dir := srcDirs[(copyIdx*5+fam)%len(srcDirs)]
			p := fmt.Sprintf("%s/pkg-%02d/unit-%02d-fam-%02d.src", dir, copyIdx, copyIdx, fam)
			data := mutateText(rng, srcTemplates[fam], p, copyIdx, fam, 56)
			// Pin 5 startup runtime modules (~200 KiB) as boot-hot.
			bootHot := copyIdx == 0 && fam < 5
			addFile(rubikfs.FileInput{
				Path:    p,
				Data:    data,
				BootHot: bootHot,
			}, false)
		}
	}

	// C. Structured service/runtime JSON & config manifests (4 families x 12 files = 48 files @ 24 KiB = 1.15 MiB).
	const (
		cfgFamilies  = 4
		cfgPerFamily = 12
		cfgSize      = 24 << 10
	)
	cfgTemplates := make([][]byte, cfgFamilies)
	for f := 0; f < cfgFamilies; f++ {
		cfgTemplates[f] = makeConfigTemplate(rng, f, cfgSize)
	}
	cfgDirs := []string{
		"/etc/visor/services",
		"/etc/systemd/system",
		"/var/lib/visor/manifests",
		"/usr/share/visor/profiles",
	}
	for copyIdx := 0; copyIdx < cfgPerFamily; copyIdx++ {
		for fam := 0; fam < cfgFamilies; fam++ {
			dir := cfgDirs[(copyIdx+fam)%len(cfgDirs)]
			p := fmt.Sprintf("%s/profile-%02d-tier-%02d.json", dir, copyIdx, fam)
			data := mutateText(rng, cfgTemplates[fam], p, copyIdx, fam, 36)
			bootHot := copyIdx == 0 && fam < 2
			addFile(rubikfs.FileInput{
				Path:    p,
				Data:    data,
				BootHot: bootHot,
			}, false)
		}
	}

	sort.Strings(bootPaths)
	if summary.TotalBytes > 0 {
		summary.BootFractionPct = 100.0 * float64(summary.BootBytes) / float64(summary.TotalBytes)
	}
	return files, bootPaths, summary, nil
}

// buildIncompressibleCorpus constructs high-entropy pre-compressed/encrypted blobs
// (Negative Regime #2: .zst archives, encrypted snapshots, random weights).
func buildIncompressibleCorpus(seed int64, numFiles, fileSize int) ([]rubikfs.FileInput, []string, CorpusSummary) {
	rng := mathrand.New(mathrand.NewSource(seed ^ 0x55aa55aa))
	files := make([]rubikfs.FileInput, 0, numFiles)
	var bootPaths []string
	var summary CorpusSummary
	summary.Name = "incompressible-encrypted-blobs"

	exts := []string{".zst", ".age", ".gz", ".safetensors.enc"}
	dirs := []string{"/var/models/weights", "/var/cache/archives", "/etc/secrets/sealed", "/opt/blobs/compressed"}

	for i := 0; i < numFiles; i++ {
		buf := make([]byte, fileSize)
		for off := 0; off+8 <= len(buf); off += 8 {
			binary.LittleEndian.PutUint64(buf[off:], rng.Uint64())
		}
		p := fmt.Sprintf("%s/shard-%03d%s", dirs[i%len(dirs)], i, exts[i%len(exts)])
		bootHot := i < 4
		files = append(files, rubikfs.FileInput{
			Path:    p,
			Data:    buf,
			BootHot: bootHot,
		})
		summary.SyntheticFiles++
		summary.SyntheticBytes += int64(len(buf))
		summary.TotalFiles++
		summary.TotalBytes += int64(len(buf))
		if bootHot {
			bootPaths = append(bootPaths, p)
			summary.BootFiles++
			summary.BootBytes += int64(len(buf))
		}
	}
	sort.Strings(bootPaths)
	if summary.TotalBytes > 0 {
		summary.BootFractionPct = 100.0 * float64(summary.BootBytes) / float64(summary.TotalBytes)
	}
	return files, bootPaths, summary
}

// makeELFTemplate generates a realistic structured ELF64-like binary template:
// ELF header + .text instruction patterns + .rodata symbol table + .plt/.got entries.
func makeELFTemplate(rng *mathrand.Rand, family, size int) []byte {
	out := make([]byte, size)
	// ELF64 magic and header fields.
	copy(out[0:], []byte{0x7f, 'E', 'L', 'F', 2, 1, 1, 0})
	binary.LittleEndian.PutUint16(out[16:], 3)  // ET_DYN
	binary.LittleEndian.PutUint16(out[18:], 62) // EM_X86_64
	binary.LittleEndian.PutUint32(out[20:], 1)

	// .text section (first 60%): realistic function prologues, mov/lea/call/ret blocks.
	textEnd := (size * 60) / 100
	prologues := [][]byte{
		{0x55, 0x48, 0x89, 0xe5, 0x48, 0x83, 0xec, 0x20}, // push rbp; mov rbp,rsp; sub rsp,0x20
		{0x41, 0x57, 0x41, 0x56, 0x41, 0x55, 0x41, 0x54}, // push r15..r12
		{0x48, 0x8b, 0x05, 0x00, 0x00, 0x00, 0x00, 0x48}, // mov rax, [rip+disp]
		{0x48, 0x85, 0xc0, 0x74, 0x18, 0x48, 0x8b, 0x40}, // test rax,rax; jz +0x18; mov rax,[rax+...]
		{0xe8, 0x00, 0x00, 0x00, 0x00, 0x48, 0x83, 0xc4}, // call rel32; add rsp,...
		{0x48, 0x89, 0xc7, 0x48, 0x89, 0xde, 0x48, 0x89}, // mov rdi,rax; mov rsi,rbx
	}
	for off := 64; off+32 <= textEnd; off += 32 {
		pat := prologues[(off/32+family*3+rng.Intn(2))%len(prologues)]
		copy(out[off:], pat)
		// Family-characteristic opcode stream + instruction operands.
		for j := 8; j < 28; j++ {
			out[off+j] = byte((family*41 + (off/32)*7 + j*13 + rng.Intn(4)) & 0xff)
		}
		// Function epilogue / alignment padding.
		copy(out[off+28:], []byte{0xc9, 0xc3, 0x0f, 0x1f})
	}

	// .rodata symbol & string table (next 25%).
	roEnd := (size * 85) / 100
	var ro bytes.Buffer
	for sym := 0; ro.Len() < roEnd-textEnd; sym++ {
		fmt.Fprintf(&ro, "visor_abi_v%02d_sym_%05d_handler\x00libhanzo_fam%02d.so.1\x00err_code_%04d: invalid state transition\x00",
			family, sym, family, sym%256)
	}
	copy(out[textEnd:roEnd], ro.Bytes())

	// .rela.dyn / .got relocation table (remaining 15%).
	for off := roEnd; off+24 <= size; off += 24 {
		binary.LittleEndian.PutUint64(out[off:], uint64(0x400000+family*0x10000+off))
		binary.LittleEndian.PutUint64(out[off+8:], uint64(0x00000008|(uint64(family)<<32)))
		binary.LittleEndian.PutUint64(out[off+16:], uint64(0x200000+(off%4096)))
	}
	return out
}

func makeSourceTemplate(rng *mathrand.Rand, family, size int) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "// Copyright 2026 Hanzo AI Inc. Immutable toolchain runtime family %02d.\npackage rt_family_%02d\n\n", family, family)
	for i := 0; buf.Len() < size+1024; i++ {
		fmt.Fprintf(&buf, "func Dispatch_%02d_%04d(ctx Context, handle uint64, flags uint32) (Result, error) {\n"+
			"\tif handle == 0 { return Result{}, ErrInvalidHandle }\n"+
			"\tentry := lookupTable_%02d[(handle^%#x)&0xfff]\n"+
			"\treturn entry.Execute(ctx, flags|%#x)\n}\n\n",
			family, i, family, rng.Uint32()&0xffff, (family+1)<<4)
	}
	return buf.Bytes()[:size]
}

func makeConfigTemplate(rng *mathrand.Rand, family, size int) []byte {
	var buf bytes.Buffer
	buf.WriteString("{\"schema\":\"hanzo.visor.image.v1\",\"profiles\":[\n")
	for i := 0; buf.Len() < size+512; i++ {
		fmt.Fprintf(&buf, "  {\"id\":\"tier-%02d-rule-%04d\",\"mount\":\"/mnt/vfs/ro\",\"policy\":\"rubikfs\",\"isolation\":\"strict-per-volume\",\"quota_mb\":%d,\"checksum\":\"%016x\"},\n",
			family, i, 256*(1+(i%8)), rng.Uint64())
	}
	b := buf.Bytes()[:size]
	return b
}

func mutateBinary(rng *mathrand.Rand, tmpl []byte, path string, copyIdx, fam, edits int) []byte {
	out := make([]byte, len(tmpl))
	copy(out, tmpl)
	// Unique build-ID / path stamp at offset 40.
	stamp := fmt.Sprintf("ID:%02d:%02d:%s", copyIdx, fam, path)
	if len(stamp) > 24 {
		stamp = stamp[len(stamp)-24:]
	}
	copy(out[40:], stamp)
	// Deterministic relocation/code edits across chunks so no two chunks are identical.
	for e := 0; e < edits; e++ {
		off := 64 + rng.Intn(len(out)-80)
		binary.LittleEndian.PutUint64(out[off:], rng.Uint64())
	}
	return out
}

func mutateText(rng *mathrand.Rand, tmpl []byte, path string, copyIdx, fam, edits int) []byte {
	out := make([]byte, len(tmpl))
	copy(out, tmpl)
	hdr := fmt.Sprintf("// file=%s rev=%03d fam=%02d\n", path, copyIdx, fam)
	if len(hdr) < len(out) {
		copy(out, hdr)
	}
	for e := 0; e < edits; e++ {
		off := len(hdr) + rng.Intn(len(out)-len(hdr)-24)
		patch := fmt.Sprintf("/*v%02d_%02d_%06x*/", copyIdx, fam, rng.Uint32()&0xffffff)
		copy(out[off:], patch)
	}
	return out
}
