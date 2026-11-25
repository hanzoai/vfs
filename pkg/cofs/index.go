// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cofs

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs"
)

// ExtName is the Volume extension namespace ("ext/cofs/<hash>") under which
// sealed CoFS lookup indexes are persisted alongside immutable base images.
const ExtName = "cofs"

const indexMagic = "COFS\x01"

type inodeRow struct {
	id      vfs.InodeID
	mode    fs.FileMode
	size    uint64
	mtimeNs int64
	atimeNs int64
	ctimeNs int64
	// start index in dirEntries (for directories)
	dirStart uint32
	// number of entries in dirEntries (for directories)
	dirCount uint32
}

type dirEntryRow struct {
	parentIno vfs.InodeID
	// index in dirEntries of parent directory's own entry, or -1 if parent is RootInode
	parentEntry int32
	nameOff     uint32
	nameLen     uint16
	// index into inodes
	childIdx uint32
}

type entryLookupSlot struct {
	entryIdx uint32
	fp       uint32
}

// Index is an immutable, concurrency-safe, minimal-perfect-hash metadata
// lookup structure for a read-only base image or snapshot.
type Index struct {
	tag []byte

	inodes     []inodeRow
	dirEntries []dirEntryRow
	nameArena  []byte

	inoMPH *MPHF
	// inoMPH slot -> index in inodes
	inoSlotMap []uint32

	entryMPH *MPHF
	// entryMPH slot -> (dirEntries index, 32-bit fingerprint)
	entrySlots []entryLookupSlot

	pathMPH *MPHF
	// pathMPH slot -> index in dirEntries
	pathSlots []uint32
}

type buildWalkItem struct {
	dirIno      vfs.InodeID
	parentEntry int32
	prefix      string
}

// Build constructs a read-only CoFS Index from the committed namespace of v.
// tag identifies the immutable snapshot or image version (for example, a
// snapshot digest or name) and is bound into the binary header.
func Build(ctx context.Context, v *vfs.Volume, tag []byte) (*Index, error) {
	var seed [32]byte
	if len(tag) > 0 {
		seed = blake3.Sum256(tag)
	} else {
		if _, err := rand.Read(seed[:]); err != nil {
			return nil, err
		}
	}

	rootIn, err := v.Stat(ctx, vfs.RootInode)
	if err != nil {
		return nil, err
	}

	inodeMap := map[vfs.InodeID]uint32{}
	var inodes []inodeRow
	addInode := func(in vfs.Inode) uint32 {
		if idx, ok := inodeMap[in.ID]; ok {
			return idx
		}
		idx := uint32(len(inodes))
		inodes = append(inodes, inodeRow{
			id:      in.ID,
			mode:    in.Mode,
			size:    in.Size,
			mtimeNs: in.Mtime.UnixNano(),
			atimeNs: in.Atime.UnixNano(),
			ctimeNs: in.Ctime.UnixNano(),
		})
		inodeMap[in.ID] = idx
		return idx
	}
	addInode(rootIn)

	var dirEntries []dirEntryRow
	var nameArena []byte
	var cleanPaths [][]byte

	queue := []buildWalkItem{{dirIno: vfs.RootInode, parentEntry: -1, prefix: ""}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		ents, err := v.ReadDir(ctx, cur.dirIno)
		if err != nil {
			return nil, err
		}
		sort.Slice(ents, func(i, j int) bool { return ents[i].Name < ents[j].Name })

		pIdx := inodeMap[cur.dirIno]
		inodes[pIdx].dirStart = uint32(len(dirEntries))
		inodes[pIdx].dirCount = uint32(len(ents))

		for _, e := range ents {
			chIn, err := v.Stat(ctx, e.ID)
			if err != nil {
				return nil, err
			}
			chIdx := addInode(chIn)
			eIdx := int32(len(dirEntries))
			nOff := uint32(len(nameArena))
			nameArena = append(nameArena, e.Name...)

			dirEntries = append(dirEntries, dirEntryRow{
				parentIno:   cur.dirIno,
				parentEntry: cur.parentEntry,
				nameOff:     nOff,
				nameLen:     uint16(len(e.Name)),
				childIdx:    chIdx,
			})

			fullPath := cur.prefix + "/" + e.Name
			cleanPaths = append(cleanPaths, []byte(fullPath))

			if chIn.IsDir() {
				queue = append(queue, buildWalkItem{
					dirIno:      chIn.ID,
					parentEntry: eIdx,
					prefix:      fullPath,
				})
			}
		}
	}

	// 1. Build InodeID MPHF.
	inoKeys := make([][]byte, len(inodes))
	for i, row := range inodes {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(row.id))
		inoKeys[i] = b
	}
	inoMPH, err := BuildMPHF(seed, "inode", inoKeys)
	if err != nil {
		return nil, err
	}
	inoSlotMap := make([]uint32, len(inodes))
	for i := range inodes {
		s := inoMPH.Index(inoKeys[i])
		inoSlotMap[s] = uint32(i)
	}

	// 2. Build (dirIno, name) MPHF using exact same 8-byte LE dirIno || name framing
	// as hashDirEntry192.
	entryKeys := make([][]byte, len(dirEntries))
	for i, de := range dirEntries {
		b := make([]byte, 8+int(de.nameLen))
		binary.LittleEndian.PutUint64(b[:8], uint64(de.parentIno))
		copy(b[8:], nameArena[de.nameOff:de.nameOff+uint32(de.nameLen)])
		entryKeys[i] = b
	}
	entryMPH, err := BuildMPHF(seed, "entry", entryKeys)
	if err != nil {
		return nil, err
	}
	entrySlots := make([]entryLookupSlot, len(dirEntries))
	for i, de := range dirEntries {
		nm := string(nameArena[de.nameOff : de.nameOff+uint32(de.nameLen)])
		slot, fp := entryMPH.indexDirEntry(uint64(de.parentIno), nm)
		entrySlots[slot] = entryLookupSlot{entryIdx: uint32(i), fp: fp}
	}

	// 3. Build canonical whole-path MPHF -> dirEntries index.
	pathMPH, err := BuildMPHF(seed, "path", cleanPaths)
	if err != nil {
		return nil, err
	}
	pathSlots := make([]uint32, len(cleanPaths))
	for i, p := range cleanPaths {
		slot := pathMPH.Index(p)
		pathSlots[slot] = uint32(i)
	}

	return &Index{
		tag:        append([]byte(nil), tag...),
		inodes:     inodes,
		dirEntries: dirEntries,
		nameArena:  nameArena,
		inoMPH:     inoMPH,
		inoSlotMap: inoSlotMap,
		entryMPH:   entryMPH,
		entrySlots: entrySlots,
		pathMPH:    pathMPH,
		pathSlots:  pathSlots,
	}, nil
}

func (idx *Index) toVFSInode(r inodeRow) vfs.Inode {
	return vfs.Inode{
		ID:    r.id,
		Mode:  r.mode,
		Size:  r.size,
		Mtime: time.Unix(0, r.mtimeNs),
		Atime: time.Unix(0, r.atimeNs),
		Ctime: time.Unix(0, r.ctimeNs),
	}
}

func (idx *Index) lookupInodeRow(id vfs.InodeID) (inodeRow, bool) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(id))
	slot := idx.inoMPH.Index(b[:])
	if slot < 0 || slot >= len(idx.inoSlotMap) {
		return inodeRow{}, false
	}
	r := idx.inodes[idx.inoSlotMap[slot]]
	if r.id != id {
		return inodeRow{}, false
	}
	return r, true
}

// Tag returns a copy of the snapshot/image tag embedded in idx.
func (idx *Index) Tag() []byte { return append([]byte(nil), idx.tag...) }

// NumInodes returns the number of indexed inodes (including RootInode).
func (idx *Index) NumInodes() int { return len(idx.inodes) }

// NumEntries returns the number of indexed directory entries.
func (idx *Index) NumEntries() int { return len(idx.dirEntries) }

// Stat returns the attributes of ino in a single O(1) MPHF probe.
func (idx *Index) Stat(ino vfs.InodeID) (vfs.Inode, error) {
	r, ok := idx.lookupInodeRow(ino)
	if !ok {
		return vfs.Inode{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, ino)
	}
	return idx.toVFSInode(r), nil
}

// Lookup resolves (dir, name) in O(1) without walking the prolly tree or
// unsealing metadata shards, returning exact fs.ErrNotExist on any miss.
func (idx *Index) Lookup(dir vfs.InodeID, name string) (vfs.Inode, error) {
	if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\x00") {
		return vfs.Inode{}, fmt.Errorf("%w: name %q", fs.ErrInvalid, name)
	}
	slot, fp := idx.entryMPH.indexDirEntry(uint64(dir), name)
	if slot >= 0 && slot < len(idx.entrySlots) {
		es := idx.entrySlots[slot]
		if es.fp == fp && int(es.entryIdx) < len(idx.dirEntries) {
			de := idx.dirEntries[es.entryIdx]
			if de.parentIno == dir && int(de.nameLen) == len(name) &&
				string(idx.nameArena[de.nameOff:de.nameOff+uint32(de.nameLen)]) == name {
				return idx.toVFSInode(idx.inodes[de.childIdx]), nil
			}
		}
	}
	// Verify whether dir itself exists and is a directory to match Volume.Lookup semantics.
	parentRow, ok := idx.lookupInodeRow(dir)
	if !ok {
		return vfs.Inode{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, dir)
	}
	if !parentRow.mode.IsDir() {
		return vfs.Inode{}, fmt.Errorf("%w: inode %d", vfs.ErrNotDir, dir)
	}
	return vfs.Inode{}, fmt.Errorf("%w: %q in inode %d", fs.ErrNotExist, name, dir)
}

func (idx *Index) verifyCleanPath(entryIdx uint32, clean string) bool {
	pos := len(clean)
	cur := int32(entryIdx)
	for cur >= 0 {
		if int(cur) >= len(idx.dirEntries) {
			return false
		}
		slash := strings.LastIndexByte(clean[:pos], '/')
		if slash < 0 {
			return false
		}
		seg := clean[slash+1 : pos]
		de := idx.dirEntries[cur]
		if int(de.nameLen) != len(seg) || string(idx.nameArena[de.nameOff:de.nameOff+uint32(de.nameLen)]) != seg {
			return false
		}
		pos = slash
		cur = de.parentEntry
	}
	return pos == 0
}

// Resolve resolves an arbitrary /-separated path from the root in O(1) via a
// single whole-path MPHF probe plus zero-allocation parent-chain verification.
func (idx *Index) Resolve(p string) (vfs.Inode, error) {
	clean := path.Clean("/" + p)
	if clean == "/" {
		return idx.toVFSInode(idx.inodes[0]), nil
	}
	slot := idx.pathMPH.IndexString(clean)
	if slot >= 0 && slot < len(idx.pathSlots) {
		eIdx := idx.pathSlots[slot]
		if idx.verifyCleanPath(eIdx, clean) {
			de := idx.dirEntries[eIdx]
			return idx.toVFSInode(idx.inodes[de.childIdx]), nil
		}
	}
	return vfs.Inode{}, fmt.Errorf("%w: %q", fs.ErrNotExist, clean)
}

// ReadDir returns all directory entries of dir in sorted name order from the
// contiguous CSR array without touching the object store.
func (idx *Index) ReadDir(dir vfs.InodeID) ([]vfs.DirEntry, error) {
	r, ok := idx.lookupInodeRow(dir)
	if !ok {
		return nil, fmt.Errorf("%w: inode %d", fs.ErrNotExist, dir)
	}
	if !r.mode.IsDir() {
		return nil, fmt.Errorf("%w: inode %d", vfs.ErrNotDir, dir)
	}
	out := make([]vfs.DirEntry, r.dirCount)
	for i := uint32(0); i < r.dirCount; i++ {
		de := idx.dirEntries[r.dirStart+i]
		ch := idx.inodes[de.childIdx]
		out[i] = vfs.DirEntry{
			Name: string(idx.nameArena[de.nameOff : de.nameOff+uint32(de.nameLen)]),
			ID:   ch.id,
			Mode: ch.mode & fs.ModeType,
		}
	}
	return out, nil
}

// SaveToVolume seals and writes idx into rwVol as extension object ("cofs", key)
// through Volume.PutExt so no plaintext ever reaches the backend.
func (idx *Index) SaveToVolume(ctx context.Context, rwVol *vfs.Volume, key []byte) error {
	raw, err := idx.MarshalBinary()
	if err != nil {
		return err
	}
	return rwVol.PutExt(ctx, ExtName, key, raw)
}

// LoadFromVolume fetches, unseals, verifies, and decodes a CoFS Index from any
// read-only or read-write Volume in a single GetExt round trip.
func LoadFromVolume(ctx context.Context, vol *vfs.Volume, key []byte) (*Index, error) {
	raw, err := vol.GetExt(ctx, ExtName, key)
	if err != nil {
		return nil, err
	}
	return UnmarshalBinary(raw)
}

// MarshalBinary encodes idx into a compact self-verifying binary representation
// with a trailing 32-byte BLAKE3 digest.
func (idx *Index) MarshalBinary() ([]byte, error) {
	est := 64 + len(idx.tag) + len(idx.inodes)*48 + len(idx.dirEntries)*28 + len(idx.nameArena)
	buf := make([]byte, 0, est)
	buf = append(buf, indexMagic...)
	buf = binary.AppendUvarint(buf, uint64(len(idx.tag)))
	buf = append(buf, idx.tag...)

	buf = binary.AppendUvarint(buf, uint64(len(idx.inodes)))
	for _, r := range idx.inodes {
		buf = binary.AppendUvarint(buf, uint64(r.id))
		buf = binary.AppendUvarint(buf, uint64(r.mode))
		buf = binary.AppendUvarint(buf, r.size)
		buf = binary.AppendVarint(buf, r.mtimeNs)
		buf = binary.AppendVarint(buf, r.atimeNs)
		buf = binary.AppendVarint(buf, r.ctimeNs)
		buf = binary.AppendUvarint(buf, uint64(r.dirStart))
		buf = binary.AppendUvarint(buf, uint64(r.dirCount))
	}

	buf = binary.AppendUvarint(buf, uint64(len(idx.dirEntries)))
	for _, de := range idx.dirEntries {
		buf = binary.AppendUvarint(buf, uint64(de.parentIno))
		buf = binary.AppendVarint(buf, int64(de.parentEntry))
		buf = binary.AppendUvarint(buf, uint64(de.nameOff))
		buf = binary.AppendUvarint(buf, uint64(de.nameLen))
		buf = binary.AppendUvarint(buf, uint64(de.childIdx))
	}

	buf = binary.AppendUvarint(buf, uint64(len(idx.nameArena)))
	buf = append(buf, idx.nameArena...)

	buf = idx.inoMPH.appendTo(buf)
	for _, v := range idx.inoSlotMap {
		buf = binary.LittleEndian.AppendUint32(buf, v)
	}

	buf = idx.entryMPH.appendTo(buf)
	for _, es := range idx.entrySlots {
		buf = binary.LittleEndian.AppendUint32(buf, es.entryIdx)
		buf = binary.LittleEndian.AppendUint32(buf, es.fp)
	}

	buf = idx.pathMPH.appendTo(buf)
	for _, ps := range idx.pathSlots {
		buf = binary.LittleEndian.AppendUint32(buf, ps)
	}

	sum := blake3.Sum256(buf)
	buf = append(buf, sum[:]...)
	return buf, nil
}

// UnmarshalBinary verifies and decodes a serialized Index produced by MarshalBinary.
func UnmarshalBinary(data []byte) (*Index, error) {
	if len(data) < len(indexMagic)+32 {
		return nil, ErrCorruptIndex
	}
	body, wantSum := data[:len(data)-32], data[len(data)-32:]
	gotSum := blake3.Sum256(body)
	if string(gotSum[:]) != string(wantSum) {
		return nil, ErrCorruptIndex
	}
	if string(body[:len(indexMagic)]) != indexMagic {
		return nil, ErrCorruptIndex
	}

	cur := body[len(indexMagic):]
	takeUvar := func() (uint64, error) {
		v, n := binary.Uvarint(cur)
		if n <= 0 {
			return 0, ErrCorruptIndex
		}
		cur = cur[n:]
		return v, nil
	}
	takeVar := func() (int64, error) {
		v, n := binary.Varint(cur)
		if n <= 0 {
			return 0, ErrCorruptIndex
		}
		cur = cur[n:]
		return v, nil
	}

	tagLen, err := takeUvar()
	if err != nil || tagLen > uint64(len(cur)) {
		return nil, ErrCorruptIndex
	}
	tag := append([]byte(nil), cur[:tagLen]...)
	cur = cur[tagLen:]

	nInodes, err := takeUvar()
	if err != nil || nInodes == 0 || nInodes > maxMPHFKeys {
		return nil, ErrCorruptIndex
	}
	inodes := make([]inodeRow, nInodes)
	for i := range inodes {
		id, err := takeUvar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		mode, err := takeUvar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		sz, err := takeUvar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		mt, err := takeVar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		at, err := takeVar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		ct, err := takeVar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		ds, err := takeUvar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		dc, err := takeUvar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		inodes[i] = inodeRow{
			id:       vfs.InodeID(id),
			mode:     fs.FileMode(mode),
			size:     sz,
			mtimeNs:  mt,
			atimeNs:  at,
			ctimeNs:  ct,
			dirStart: uint32(ds),
			dirCount: uint32(dc),
		}
	}

	nEntries, err := takeUvar()
	if err != nil || nEntries > maxMPHFKeys {
		return nil, ErrCorruptIndex
	}
	dirEntries := make([]dirEntryRow, nEntries)
	for i := range dirEntries {
		pIno, err := takeUvar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		pEnt, err := takeVar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		nOff, err := takeUvar()
		if err != nil {
			return nil, ErrCorruptIndex
		}
		nLen, err := takeUvar()
		if err != nil || nLen > 255 {
			return nil, ErrCorruptIndex
		}
		cIdx, err := takeUvar()
		if err != nil || cIdx >= nInodes {
			return nil, ErrCorruptIndex
		}
		dirEntries[i] = dirEntryRow{
			parentIno:   vfs.InodeID(pIno),
			parentEntry: int32(pEnt),
			nameOff:     uint32(nOff),
			nameLen:     uint16(nLen),
			childIdx:    uint32(cIdx),
		}
	}

	arenaLen, err := takeUvar()
	if err != nil || arenaLen > uint64(len(cur)) {
		return nil, ErrCorruptIndex
	}
	nameArena := append([]byte(nil), cur[:arenaLen]...)
	cur = cur[arenaLen:]

	for _, de := range dirEntries {
		if uint64(de.nameOff)+uint64(de.nameLen) > uint64(len(nameArena)) {
			return nil, ErrCorruptIndex
		}
	}
	for _, r := range inodes {
		if uint64(r.dirStart)+uint64(r.dirCount) > nEntries {
			return nil, ErrCorruptIndex
		}
	}

	inoMPH, cur, err := decodeMPHF(cur)
	if err != nil || uint64(inoMPH.Len()) != nInodes || len(cur) < int(nInodes)*4 {
		return nil, ErrCorruptIndex
	}
	inoSlotMap := make([]uint32, nInodes)
	for i := range inoSlotMap {
		v := binary.LittleEndian.Uint32(cur[i*4 : i*4+4])
		if uint64(v) >= nInodes {
			return nil, ErrCorruptIndex
		}
		inoSlotMap[i] = v
	}
	cur = cur[int(nInodes)*4:]

	entryMPH, cur, err := decodeMPHF(cur)
	if err != nil || uint64(entryMPH.Len()) != nEntries || len(cur) < int(nEntries)*8 {
		return nil, ErrCorruptIndex
	}
	entrySlots := make([]entryLookupSlot, nEntries)
	for i := range entrySlots {
		eIdx := binary.LittleEndian.Uint32(cur[i*8 : i*8+4])
		fp := binary.LittleEndian.Uint32(cur[i*8+4 : i*8+8])
		if uint64(eIdx) >= nEntries {
			return nil, ErrCorruptIndex
		}
		entrySlots[i] = entryLookupSlot{entryIdx: eIdx, fp: fp}
	}
	cur = cur[int(nEntries)*8:]

	pathMPH, cur, err := decodeMPHF(cur)
	if err != nil || uint64(pathMPH.Len()) != nEntries || len(cur) != int(nEntries)*4 {
		return nil, ErrCorruptIndex
	}
	pathSlots := make([]uint32, nEntries)
	for i := range pathSlots {
		v := binary.LittleEndian.Uint32(cur[i*4 : i*4+4])
		if uint64(v) >= nEntries {
			return nil, ErrCorruptIndex
		}
		pathSlots[i] = v
	}

	return &Index{
		tag:        tag,
		inodes:     inodes,
		dirEntries: dirEntries,
		nameArena:  nameArena,
		inoMPH:     inoMPH,
		inoSlotMap: inoSlotMap,
		entryMPH:   entryMPH,
		entrySlots: entrySlots,
		pathMPH:    pathMPH,
		pathSlots:  pathSlots,
	}, nil
}
