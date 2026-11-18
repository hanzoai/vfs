// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package djfs

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"sort"
	"sync"
	"time"
)

// dirSnap is an immutable point-in-time snapshot of a single directory and the
// regular files currently linked inside it.
type dirSnap struct {
	id      InodeID
	parent  InodeID
	name    string
	attr    Attr
	entries map[string]DirEntry
	files   map[InodeID]fileSnap
}

type fileSnap struct {
	id   InodeID
	dir  InodeID
	name string
	attr Attr
	data []byte
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func cloneDirSnap(s *dirSnap) *dirSnap {
	if s == nil {
		return nil
	}
	entries := make(map[string]DirEntry, len(s.entries))
	for k, v := range s.entries {
		entries[k] = v
	}
	files := make(map[InodeID]fileSnap, len(s.files))
	for id, f := range s.files {
		f.data = cloneBytes(f.data)
		files[id] = f
	}
	return &dirSnap{
		id:      s.id,
		parent:  s.parent,
		name:    s.name,
		attr:    s.attr,
		entries: entries,
		files:   files,
	}
}

type locPair struct {
	dir  InodeID
	name string
}

type simEnt struct {
	id    InodeID
	isDir bool
}

// computeDelta synthesizes the minimal, collision-free, cycle-safe sequence of
// DeltaOps that transforms baseDirs into currDirs over a closed directory
// equivalence class.
//
// Transient chains such as Create -> Write -> Chmod -> Unlink annihilate
// completely, and staging workflows such as Create(.staging/tmp) -> Write ->
// Chmod -> Rename(.staging/tmp -> pkg/index.js) collapse into a single
// DeltaCreateFile at the final destination.
func computeDelta(baseDirs, currDirs map[InodeID]*dirSnap) []DeltaOp {
	baseFiles := make(map[InodeID]fileSnap)
	for _, d := range baseDirs {
		if d == nil {
			continue
		}
		for fid, f := range d.files {
			baseFiles[fid] = f
		}
	}

	currFiles := make(map[InodeID]fileSnap)
	for _, d := range currDirs {
		if d == nil {
			continue
		}
		for fid, f := range d.files {
			currFiles[fid] = f
		}
	}

	simEntries := make(map[InodeID]map[string]simEnt, len(baseDirs)+len(currDirs))
	simDirLoc := make(map[InodeID]locPair, len(baseDirs)+len(currDirs))
	simFileLoc := make(map[InodeID]locPair, len(baseFiles)+len(currFiles))

	for did, d := range baseDirs {
		if d == nil {
			continue
		}
		em := make(map[string]simEnt, len(d.entries))
		for nm, ent := range d.entries {
			em[nm] = simEnt{id: ent.ID, isDir: ent.Mode.IsDir()}
		}
		simEntries[did] = em
		if did != RootInode {
			simDirLoc[did] = locPair{dir: d.parent, name: d.name}
		}
	}
	for fid, f := range baseFiles {
		simFileLoc[fid] = locPair{dir: f.dir, name: f.name}
	}

	// Ensure parent slot maps exist for untouched anchor directories referenced
	// by born or relocated children.
	ensureSimDir := func(did InodeID) map[string]simEnt {
		em, ok := simEntries[did]
		if !ok {
			em = make(map[string]simEnt)
			simEntries[did] = em
		}
		return em
	}

	simIsDesc := func(cand, target InodeID) bool {
		cur := cand
		for steps := 0; steps < len(simDirLoc)+2 && cur != RootInode; steps++ {
			if cur == target {
				return true
			}
			loc, ok := simDirLoc[cur]
			if !ok || loc.dir == cur {
				break
			}
			cur = loc.dir
		}
		return cur == target
	}

	topAnchor := func(did InodeID) InodeID {
		if _, ok := simEntries[RootInode]; ok {
			return RootInode
		}
		cur := did
		for steps := 0; steps < len(simDirLoc)+2 && cur != RootInode; steps++ {
			loc, ok := simDirLoc[cur]
			if !ok || loc.dir == cur {
				break
			}
			if _, hasParent := simEntries[loc.dir]; !hasParent {
				break
			}
			cur = loc.dir
		}
		return cur
	}

	currDepth := func(did InodeID) int {
		dep := 0
		cur := did
		for steps := 0; steps < len(currDirs)+2 && cur != RootInode; steps++ {
			cd, ok := currDirs[cur]
			if !ok || cd == nil || cd.parent == cur {
				break
			}
			dep++
			cur = cd.parent
		}
		return dep
	}

	simDepth := func(did InodeID) int {
		dep := 0
		cur := did
		for steps := 0; steps < len(simDirLoc)+2 && cur != RootInode; steps++ {
			loc, ok := simDirLoc[cur]
			if !ok || loc.dir == cur {
				break
			}
			dep++
			cur = loc.dir
		}
		return dep
	}

	var stageCounter uint64
	nextStageName := func(id InodeID) string {
		stageCounter++
		return fmt.Sprintf(".djfs.stage.%d.%d", id, stageCounter)
	}

	var ops []DeltaOp

	// Phase 1: Unlink dead pre-existing regular files to vacate slots and empty
	// directories scheduled for removal.
	deadFileIDs := make([]InodeID, 0)
	for fid := range baseFiles {
		if _, alive := currFiles[fid]; !alive {
			deadFileIDs = append(deadFileIDs, fid)
		}
	}
	sortInodeIDs(deadFileIDs)
	for _, fid := range deadFileIDs {
		loc := simFileLoc[fid]
		delete(simFileLoc, fid)
		delete(ensureSimDir(loc.dir), loc.name)
		ops = append(ops, DeltaOp{
			Kind: DeltaUnlink,
			Dir:  loc.dir,
			Name: loc.name,
			ID:   fid,
		})
	}

	// Phase 2: Create newly born directories top-down by final tree depth.
	bornDirs := make([]InodeID, 0)
	for did, cd := range currDirs {
		if cd == nil || did == RootInode {
			continue
		}
		if bd, existed := baseDirs[did]; !existed || bd == nil {
			bornDirs = append(bornDirs, did)
		}
	}
	sort.Slice(bornDirs, func(i, j int) bool {
		di := currDepth(bornDirs[i])
		dj := currDepth(bornDirs[j])
		if di != dj {
			return di < dj
		}
		return bornDirs[i] < bornDirs[j]
	})
	for _, did := range bornDirs {
		cd := currDirs[did]
		parentMap := ensureSimDir(cd.parent)
		useName := cd.name
		if _, busy := parentMap[useName]; busy {
			useName = nextStageName(did)
		}
		ensureSimDir(did)
		parentMap[useName] = simEnt{id: did, isDir: true}
		simDirLoc[did] = locPair{dir: cd.parent, name: useName}
		ops = append(ops, DeltaOp{
			Kind: DeltaMkdir,
			Dir:  cd.parent,
			Name: useName,
			ID:   did,
			Attr: cd.attr,
		})
	}

	// Phase 3a: Move surviving pre-existing regular files whose (dir, name)
	// changed, parking at a deterministic staging name on slot collision/cycles.
	survivingFiles := make([]InodeID, 0)
	for fid := range currFiles {
		if _, existed := baseFiles[fid]; existed {
			survivingFiles = append(survivingFiles, fid)
		}
	}
	sortInodeIDs(survivingFiles)

	var stagedFiles []InodeID
	for _, fid := range survivingFiles {
		cf := currFiles[fid]
		curLoc := simFileLoc[fid]
		if curLoc.dir == cf.dir && curLoc.name == cf.name {
			continue
		}
		dstMap := ensureSimDir(cf.dir)
		dstName := cf.name
		if _, busy := dstMap[dstName]; busy {
			dstName = nextStageName(fid)
			stagedFiles = append(stagedFiles, fid)
		}
		delete(ensureSimDir(curLoc.dir), curLoc.name)
		dstMap[dstName] = simEnt{id: fid, isDir: false}
		simFileLoc[fid] = locPair{dir: cf.dir, name: dstName}
		ops = append(ops, DeltaOp{
			Kind:    DeltaRename,
			Dir:     curLoc.dir,
			Name:    curLoc.name,
			DstDir:  cf.dir,
			DstName: dstName,
			ID:      fid,
			Attr:    cf.attr,
		})
	}

	// Phase 3b: Relocate directories (surviving base dirs and staged born dirs).
	neededSlots := make(map[locPair]bool)
	for did, cd := range currDirs {
		if cd != nil && did != RootInode {
			neededSlots[locPair{dir: cd.parent, name: cd.name}] = true
		}
	}
	for _, cf := range currFiles {
		neededSlots[locPair{dir: cf.dir, name: cf.name}] = true
	}

	deadDirs := make([]InodeID, 0)
	for did, bd := range baseDirs {
		if bd == nil || did == RootInode {
			continue
		}
		if cd, alive := currDirs[did]; !alive || cd == nil {
			deadDirs = append(deadDirs, did)
		}
	}
	sortInodeIDs(deadDirs)
	for _, deadID := range deadDirs {
		loc := simDirLoc[deadID]
		if neededSlots[loc] {
			stName := nextStageName(deadID)
			pm := ensureSimDir(loc.dir)
			delete(pm, loc.name)
			pm[stName] = simEnt{id: deadID, isDir: true}
			simDirLoc[deadID] = locPair{dir: loc.dir, name: stName}
			ops = append(ops, DeltaOp{
				Kind:    DeltaRename,
				Dir:     loc.dir,
				Name:    loc.name,
				DstDir:  loc.dir,
				DstName: stName,
				ID:      deadID,
				Attr:    baseDirs[deadID].attr,
			})
		}
	}

	dirsToPlace := make([]InodeID, 0)
	movingDirSet := make(map[InodeID]bool)
	for did, cd := range currDirs {
		if cd == nil || did == RootInode {
			continue
		}
		curLoc := simDirLoc[did]
		if curLoc.dir != cd.parent || curLoc.name != cd.name {
			dirsToPlace = append(dirsToPlace, did)
			movingDirSet[did] = true
		}
	}

	// Sort bottom-up by current tree depth so nested moving subdirectories unnest
	// to topAnchor before any moving ancestor is relocated inside them.
	sort.Slice(dirsToPlace, func(i, j int) bool {
		di := simDepth(dirsToPlace[i])
		dj := simDepth(dirsToPlace[j])
		if di != dj {
			return di > dj
		}
		return dirsToPlace[i] > dirsToPlace[j]
	})

	hasMovingAncestor := func(did InodeID) bool {
		cur := did
		for steps := 0; steps < len(simDirLoc)+2 && cur != RootInode; steps++ {
			loc, ok := simDirLoc[cur]
			if !ok || loc.dir == cur {
				break
			}
			if movingDirSet[loc.dir] {
				return true
			}
			cur = loc.dir
		}
		return false
	}

	for _, did := range dirsToPlace {
		cd := currDirs[did]
		curLoc := simDirLoc[did]
		dstMap := ensureSimDir(cd.parent)
		_, dstBusy := dstMap[cd.name]
		if dstBusy || neededSlots[curLoc] || simIsDesc(cd.parent, did) || hasMovingAncestor(did) {
			anchor := topAnchor(did)
			stName := nextStageName(did)
			delete(ensureSimDir(curLoc.dir), curLoc.name)
			ensureSimDir(anchor)[stName] = simEnt{id: did, isDir: true}
			simDirLoc[did] = locPair{dir: anchor, name: stName}
			ops = append(ops, DeltaOp{
				Kind:    DeltaRename,
				Dir:     curLoc.dir,
				Name:    curLoc.name,
				DstDir:  anchor,
				DstName: stName,
				ID:      did,
				Attr:    cd.attr,
			})
		}
	}

	sort.Slice(dirsToPlace, func(i, j int) bool {
		di := currDepth(dirsToPlace[i])
		dj := currDepth(dirsToPlace[j])
		if di != dj {
			return di < dj
		}
		return dirsToPlace[i] < dirsToPlace[j]
	})
	for _, did := range dirsToPlace {
		cd := currDirs[did]
		curLoc := simDirLoc[did]
		if curLoc.dir == cd.parent && curLoc.name == cd.name {
			continue
		}
		delete(ensureSimDir(curLoc.dir), curLoc.name)
		ensureSimDir(cd.parent)[cd.name] = simEnt{id: did, isDir: true}
		simDirLoc[did] = locPair{dir: cd.parent, name: cd.name}
		ops = append(ops, DeltaOp{
			Kind:    DeltaRename,
			Dir:     curLoc.dir,
			Name:    curLoc.name,
			DstDir:  cd.parent,
			DstName: cd.name,
			ID:      did,
			Attr:    cd.attr,
		})
	}

	// Phase 3c: Move staged surviving regular files into their final slots.
	for _, fid := range stagedFiles {
		cf := currFiles[fid]
		curLoc := simFileLoc[fid]
		if curLoc.dir == cf.dir && curLoc.name == cf.name {
			continue
		}
		delete(ensureSimDir(curLoc.dir), curLoc.name)
		ensureSimDir(cf.dir)[cf.name] = simEnt{id: fid, isDir: false}
		simFileLoc[fid] = locPair{dir: cf.dir, name: cf.name}
		ops = append(ops, DeltaOp{
			Kind:    DeltaRename,
			Dir:     curLoc.dir,
			Name:    curLoc.name,
			DstDir:  cf.dir,
			DstName: cf.name,
			ID:      fid,
			Attr:    cf.attr,
		})
	}

	// Phase 4: Remove dead directories bottom-up so every directory is empty.
	sort.Slice(deadDirs, func(i, j int) bool {
		di := simDepth(deadDirs[i])
		dj := simDepth(deadDirs[j])
		if di != dj {
			return di > dj
		}
		return deadDirs[i] > deadDirs[j]
	})
	for _, deadID := range deadDirs {
		loc := simDirLoc[deadID]
		delete(simDirLoc, deadID)
		delete(ensureSimDir(loc.dir), loc.name)
		delete(simEntries, deadID)
		ops = append(ops, DeltaOp{
			Kind: DeltaRmdir,
			Dir:  loc.dir,
			Name: loc.name,
			ID:   deadID,
		})
	}

	// Phase 5: Create newly born regular files directly at their final names.
	bornFiles := make([]InodeID, 0)
	for fid := range currFiles {
		if _, existed := baseFiles[fid]; !existed {
			bornFiles = append(bornFiles, fid)
		}
	}
	sortInodeIDs(bornFiles)
	for _, fid := range bornFiles {
		cf := currFiles[fid]
		ensureSimDir(cf.dir)[cf.name] = simEnt{id: fid, isDir: false}
		simFileLoc[fid] = locPair{dir: cf.dir, name: cf.name}
		ops = append(ops, DeltaOp{
			Kind: DeltaCreateFile,
			Dir:  cf.dir,
			Name: cf.name,
			ID:   fid,
			Attr: cf.attr,
			Data: cloneBytes(cf.data),
		})
	}

	// Phase 6: Emit final payload or attribute updates on surviving base files,
	// followed by final directory attribute stamps so directory mtime/ctime match
	// the exact linearizable operation order.
	for _, fid := range survivingFiles {
		bf := baseFiles[fid]
		cf := currFiles[fid]
		if !bytes.Equal(bf.data, cf.data) {
			ops = append(ops, DeltaOp{
				Kind: DeltaWrite,
				Dir:  cf.dir,
				Name: cf.name,
				ID:   fid,
				Attr: cf.attr,
				Data: cloneBytes(cf.data),
			})
		} else if !sameAttr(bf.attr, cf.attr) {
			ops = append(ops, DeltaOp{
				Kind: DeltaSetAttr,
				Dir:  cf.dir,
				Name: cf.name,
				ID:   fid,
				Attr: cf.attr,
			})
		}
	}

	liveDirIDs := make([]InodeID, 0, len(currDirs))
	for did, cd := range currDirs {
		if cd != nil {
			liveDirIDs = append(liveDirIDs, did)
		}
	}
	sortInodeIDs(liveDirIDs)
	for _, did := range liveDirIDs {
		cd := currDirs[did]
		bd, existed := baseDirs[did]
		if !existed || bd == nil || !sameAttr(bd.attr, cd.attr) || len(ops) > 0 {
			ops = append(ops, DeltaOp{
				Kind: DeltaSetAttr,
				Dir:  cd.parent,
				Name: cd.name,
				ID:   did,
				Attr: cd.attr,
			})
		}
	}

	return ops
}

func sameAttr(a, b Attr) bool {
	return a.ID == b.ID &&
		a.Mode == b.Mode &&
		a.Size == b.Size &&
		a.Nlink == b.Nlink &&
		a.Mtime.Equal(b.Mtime) &&
		a.Atime.Equal(b.Atime) &&
		a.Ctime.Equal(b.Ctime)
}

func sortInodeIDs(ids []InodeID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}

// MemTree is an in-memory reference POSIX metadata and inline-payload tree that
// implements Target for testing, replay verification, and snapshot inspection.
type MemTree struct {
	mu      sync.RWMutex
	dirs    map[InodeID]*dirSnap
	files   map[InodeID]fileSnap
	batches uint64
	applied uint64
}

// NewMemTree returns a MemTree initialized with RootInode at t0.
func NewMemTree(t0 time.Time) *MemTree {
	if t0.IsZero() {
		t0 = time.Unix(0, 0).UTC()
	}
	t0 = t0.UTC()
	root := &dirSnap{
		id:     RootInode,
		parent: RootInode,
		name:   "",
		attr: Attr{
			ID:    RootInode,
			Mode:  fs.ModeDir | 0o755,
			Nlink: 2,
			Mtime: t0,
			Atime: t0,
			Ctime: t0,
		},
		entries: make(map[string]DirEntry),
		files:   make(map[InodeID]fileSnap),
	}
	return &MemTree{
		dirs:  map[InodeID]*dirSnap{RootInode: root},
		files: make(map[InodeID]fileSnap),
	}
}

// ApplyBatch applies a coalesced Batch atomically under the tree lock.
func (m *MemTree) ApplyBatch(_ context.Context, batch Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range batch.Ops {
		if err := m.applyOpLocked(&batch.Ops[i]); err != nil {
			return fmt.Errorf("djfs: apply op %d (kind=%d): %w", i, batch.Ops[i].Kind, err)
		}
	}
	m.batches++
	m.applied += uint64(len(batch.Ops))
	return nil
}

func (m *MemTree) applyOpLocked(op *DeltaOp) error {
	switch op.Kind {
	case DeltaMkdir:
		parent := m.dirs[op.Dir]
		if parent == nil {
			return fmt.Errorf("%w: parent %d", fs.ErrNotExist, op.Dir)
		}
		if _, exists := parent.entries[op.Name]; exists {
			return fmt.Errorf("%w: %s in %d", fs.ErrExist, op.Name, op.Dir)
		}
		attr := op.Attr
		attr.ID = op.ID
		attr.Mode = fs.ModeDir | attr.Mode.Perm()
		if attr.Nlink < 2 {
			attr.Nlink = 2
		}
		child := &dirSnap{
			id:      op.ID,
			parent:  op.Dir,
			name:    op.Name,
			attr:    attr,
			entries: make(map[string]DirEntry),
			files:   make(map[InodeID]fileSnap),
		}
		parent.entries[op.Name] = DirEntry{Name: op.Name, ID: op.ID, Mode: fs.ModeDir}
		parent.attr.Nlink++
		m.dirs[op.ID] = child
		return nil

	case DeltaCreateFile:
		parent := m.dirs[op.Dir]
		if parent == nil {
			return fmt.Errorf("%w: dir %d", fs.ErrNotExist, op.Dir)
		}
		if _, exists := parent.entries[op.Name]; exists {
			return fmt.Errorf("%w: %s in %d", fs.ErrExist, op.Name, op.Dir)
		}
		attr := op.Attr
		attr.ID = op.ID
		attr.Mode = attr.Mode.Perm()
		attr.Size = uint64(len(op.Data))
		attr.Nlink = 1
		fsn := fileSnap{
			id:   op.ID,
			dir:  op.Dir,
			name: op.Name,
			attr: attr,
			data: cloneBytes(op.Data),
		}
		parent.entries[op.Name] = DirEntry{Name: op.Name, ID: op.ID, Mode: 0}
		parent.files[op.ID] = fsn
		m.files[op.ID] = fsn
		return nil

	case DeltaWrite:
		fsn, ok := m.files[op.ID]
		if !ok {
			return fmt.Errorf("%w: file %d", fs.ErrNotExist, op.ID)
		}
		parent := m.dirs[fsn.dir]
		if parent == nil {
			return fmt.Errorf("%w: parent %d", fs.ErrNotExist, fsn.dir)
		}
		attr := op.Attr
		attr.ID = op.ID
		attr.Mode = attr.Mode.Perm()
		attr.Size = uint64(len(op.Data))
		attr.Nlink = 1
		fsn.attr = attr
		fsn.data = cloneBytes(op.Data)
		parent.files[op.ID] = fsn
		m.files[op.ID] = fsn
		return nil

	case DeltaRename:
		srcParent := m.dirs[op.Dir]
		dstParent := m.dirs[op.DstDir]
		if srcParent == nil || dstParent == nil {
			return fmt.Errorf("%w: rename dir (%d -> %d)", fs.ErrNotExist, op.Dir, op.DstDir)
		}
		ent, ok := srcParent.entries[op.Name]
		if !ok {
			return fmt.Errorf("%w: rename source %s in %d", fs.ErrNotExist, op.Name, op.Dir)
		}
		if op.Dir == op.DstDir && op.Name == op.DstName {
			return nil
		}
		if victim, hasVictim := dstParent.entries[op.DstName]; hasVictim {
			if victim.Mode.IsDir() != ent.Mode.IsDir() {
				return fmt.Errorf("%w: rename type mismatch on %s", fs.ErrInvalid, op.DstName)
			}
			if victim.Mode.IsDir() {
				vd := m.dirs[victim.ID]
				if vd != nil && len(vd.entries) > 0 {
					return fmt.Errorf("%w: %s", ErrNotEmpty, op.DstName)
				}
				delete(m.dirs, victim.ID)
				dstParent.attr.Nlink--
			} else {
				delete(dstParent.files, victim.ID)
				delete(m.files, victim.ID)
			}
		}
		delete(srcParent.entries, op.Name)
		ent.Name = op.DstName
		dstParent.entries[op.DstName] = ent
		if ent.Mode.IsDir() {
			srcParent.attr.Nlink--
			dstParent.attr.Nlink++
			if sub := m.dirs[ent.ID]; sub != nil {
				sub.parent = op.DstDir
				sub.name = op.DstName
			}
		} else {
			fsn := m.files[ent.ID]
			delete(srcParent.files, ent.ID)
			fsn.dir = op.DstDir
			fsn.name = op.DstName
			dstParent.files[ent.ID] = fsn
			m.files[ent.ID] = fsn
		}
		return nil

	case DeltaUnlink:
		parent := m.dirs[op.Dir]
		if parent == nil {
			return fmt.Errorf("%w: dir %d", fs.ErrNotExist, op.Dir)
		}
		ent, ok := parent.entries[op.Name]
		if !ok {
			return fmt.Errorf("%w: %s in %d", fs.ErrNotExist, op.Name, op.Dir)
		}
		if ent.Mode.IsDir() {
			return fmt.Errorf("%w: %s", ErrIsDir, op.Name)
		}
		delete(parent.entries, op.Name)
		delete(parent.files, ent.ID)
		delete(m.files, ent.ID)
		return nil

	case DeltaRmdir:
		parent := m.dirs[op.Dir]
		if parent == nil {
			return fmt.Errorf("%w: dir %d", fs.ErrNotExist, op.Dir)
		}
		ent, ok := parent.entries[op.Name]
		if !ok {
			return fmt.Errorf("%w: %s in %d", fs.ErrNotExist, op.Name, op.Dir)
		}
		if !ent.Mode.IsDir() {
			return fmt.Errorf("%w: %s", ErrNotDir, op.Name)
		}
		sub := m.dirs[ent.ID]
		if sub != nil && len(sub.entries) > 0 {
			return fmt.Errorf("%w: %s", ErrNotEmpty, op.Name)
		}
		delete(parent.entries, op.Name)
		parent.attr.Nlink--
		delete(m.dirs, ent.ID)
		return nil

	case DeltaSetAttr:
		if d := m.dirs[op.ID]; d != nil {
			attr := op.Attr
			attr.ID = op.ID
			attr.Mode = fs.ModeDir | attr.Mode.Perm()
			// Derive canonical directory link count from live child subdirectories.
			nlink := uint32(2)
			for _, child := range d.entries {
				if child.Mode.IsDir() {
					nlink++
				}
			}
			attr.Nlink = nlink
			d.attr = attr
			return nil
		}
		if fsn, ok := m.files[op.ID]; ok {
			parent := m.dirs[fsn.dir]
			attr := op.Attr
			attr.ID = op.ID
			attr.Mode = attr.Mode.Perm()
			attr.Size = uint64(len(fsn.data))
			attr.Nlink = 1
			fsn.attr = attr
			if parent != nil {
				parent.files[op.ID] = fsn
			}
			m.files[op.ID] = fsn
			return nil
		}
		return fmt.Errorf("%w: inode %d", fs.ErrNotExist, op.ID)

	default:
		return fmt.Errorf("%w: delta kind %d", ErrCorrupt, op.Kind)
	}
}

// CheckInvariants verifies that the tree has zero orphaned inodes, zero
// dangling entries, acyclic reachability from RootInode, and exact POSIX link
// counts across all directories and regular files.
func (m *MemTree) CheckInvariants() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	root := m.dirs[RootInode]
	if root == nil {
		return fmt.Errorf("missing root directory")
	}

	seenDirs := map[InodeID]bool{RootInode: true}
	seenFiles := make(map[InodeID]int)

	var walk func(d *dirSnap) error
	walk = func(d *dirSnap) error {
		wantNlink := uint32(2)
		for name, ent := range d.entries {
			if ent.Name != name {
				return fmt.Errorf("dir %d entry key %q != Name %q", d.id, name, ent.Name)
			}
			if ent.Mode.IsDir() {
				wantNlink++
				if seenDirs[ent.ID] {
					return fmt.Errorf("directory cycle or duplicate link on dir %d", ent.ID)
				}
				sub := m.dirs[ent.ID]
				if sub == nil {
					return fmt.Errorf("dangling dir entry %q -> %d in dir %d", name, ent.ID, d.id)
				}
				if sub.parent != d.id || sub.name != name {
					return fmt.Errorf("dir %d parent/name (%d, %q) != (%d, %q)", ent.ID, sub.parent, sub.name, d.id, name)
				}
				seenDirs[ent.ID] = true
				if err := walk(sub); err != nil {
					return err
				}
			} else {
				fsn, ok := m.files[ent.ID]
				if !ok {
					return fmt.Errorf("dangling file entry %q -> %d in dir %d", name, ent.ID, d.id)
				}
				if fsn.dir != d.id || fsn.name != name {
					return fmt.Errorf("file %d dir/name (%d, %q) != (%d, %q)", ent.ID, fsn.dir, fsn.name, d.id, name)
				}
				if fsn.attr.Nlink != 1 {
					return fmt.Errorf("file %d nlink=%d, want 1", ent.ID, fsn.attr.Nlink)
				}
				if fsn.attr.Size != uint64(len(fsn.data)) {
					return fmt.Errorf("file %d size=%d != len(data)=%d", ent.ID, fsn.attr.Size, len(fsn.data))
				}
				seenFiles[ent.ID]++
				if seenFiles[ent.ID] > 1 {
					return fmt.Errorf("torn duplicate link on file %d (count=%d)", ent.ID, seenFiles[ent.ID])
				}
			}
		}
		if d.attr.Nlink != wantNlink {
			return fmt.Errorf("dir %d nlink=%d, want %d", d.id, d.attr.Nlink, wantNlink)
		}
		return nil
	}

	if err := walk(root); err != nil {
		return err
	}
	if len(seenDirs) != len(m.dirs) {
		return fmt.Errorf("orphaned directories: reachable=%d total=%d", len(seenDirs), len(m.dirs))
	}
	if len(seenFiles) != len(m.files) {
		return fmt.Errorf("orphaned regular files: reachable=%d total=%d", len(seenFiles), len(m.files))
	}
	return nil
}

// Stat returns the attributes of an inode in MemTree.
func (m *MemTree) Stat(id InodeID) (Attr, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if d := m.dirs[id]; d != nil {
		return d.attr, nil
	}
	if f, ok := m.files[id]; ok {
		return f.attr, nil
	}
	return Attr{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, id)
}

// Lookup resolves name inside dir in MemTree.
func (m *MemTree) Lookup(dir InodeID, name string) (Attr, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	d := m.dirs[dir]
	if d == nil {
		return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	ent, ok := d.entries[name]
	if !ok {
		return Attr{}, fmt.Errorf("%w: %s in %d", fs.ErrNotExist, name, dir)
	}
	if ent.Mode.IsDir() {
		sub := m.dirs[ent.ID]
		if sub == nil {
			return Attr{}, fmt.Errorf("%w: dir %d", fs.ErrNotExist, ent.ID)
		}
		return sub.attr, nil
	}
	f, ok := m.files[ent.ID]
	if !ok {
		return Attr{}, fmt.Errorf("%w: file %d", fs.ErrNotExist, ent.ID)
	}
	return f.attr, nil
}

// ReadDir returns the sorted directory entries of dir in MemTree.
func (m *MemTree) ReadDir(dir InodeID) ([]DirEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	d := m.dirs[dir]
	if d == nil {
		return nil, fmt.Errorf("%w: dir %d", fs.ErrNotExist, dir)
	}
	out := make([]DirEntry, 0, len(d.entries))
	for _, ent := range d.entries {
		out = append(out, ent)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ReadFile returns a copy of file id's bytes in MemTree.
func (m *MemTree) ReadFile(id InodeID) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	f, ok := m.files[id]
	if !ok {
		if m.dirs[id] != nil {
			return nil, fmt.Errorf("%w: inode %d", ErrIsDir, id)
		}
		return nil, fmt.Errorf("%w: file %d", fs.ErrNotExist, id)
	}
	return cloneBytes(f.data), nil
}

// Counts returns (directories, files, batchesApplied, opsApplied).
func (m *MemTree) Counts() (int, int, uint64, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.dirs), len(m.files), m.batches, m.applied
}
