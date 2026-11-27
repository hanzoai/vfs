// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/backend"
)

const (
	// raskLeafCap bounds runs in a compact leaf (256 * 8 B = 2 KiB payload).
	raskLeafCap = 256

	// raskWideCap bounds runs in a wide 64-bit fallback leaf (128 * 16 B = 2 KiB payload).
	raskWideCap = 128

	// raskNodeCap bounds fanout of internal B+tree nodes.
	raskNodeCap = 64
)

type compactRun struct {
	deltaOff uint32
	len32    uint32
}

// raskLeaf is exactly 64 bytes on 64-bit targets (one Go allocator size class):
// base (8) + compact (24) + wide (24) + next (8).
type raskLeaf struct {
	base    uint64
	compact []compactRun
	wide    []run
	next    *raskLeaf
}

type raskInternal struct {
	maxEnds   []uint64
	leaves    []*raskLeaf
	internals []*raskInternal
}

type raskFrame struct {
	node *raskInternal
	idx  int
}

// RaskStats summarizes the structural layout of a RaskRunIndex.
//
// StructuralBytes sums unsafe.Sizeof of structs plus cap(slice)*unsafe.Sizeof(elem)
// of backing slices; it does NOT include Go runtime size-class rounding, GC span
// metadata, or process RSS. Use RetainedHeapAllocBytes / RetainedHeapInuseBytes /
// RSSDeltaBytes in RunRASKBenchmark for allocator and OS memory accounting.
type RaskStats struct {
	Runs            int
	Height          int
	Leaves          int
	CompactLeaves   int
	WideLeaves      int
	InternalNodes   int
	StructuralBytes int64
}

// RaskRunIndex is a compact segmented B+tree range index over disjoint,
// coalesced half-open intervals [a, b) of type run{a, b uint64}.
//
// Each compact leaf shares one 64-bit base offset and packs runs whose span
// (maxEnd - base) fits in uint32 as 8-byte (deltaOff uint32, len32 uint32)
// pairs. Leaves whose span exceeds 4 GiB automatically split at wide gaps or
// promote to wide 64-bit mode so arbitrary 1 PiB..MaxSize ranges remain exact.
type RaskRunIndex struct {
	height   int
	rootLeaf *raskLeaf
	root     *raskInternal
	first    *raskLeaf
	last     *raskLeaf
	count    int
}

// NewRaskRunIndex constructs an empty RaskRunIndex.
func NewRaskRunIndex() *RaskRunIndex {
	return &RaskRunIndex{}
}

// Len returns the number of disjoint coalesced runs currently stored.
func (idx *RaskRunIndex) Len() int {
	return idx.count
}

func (l *raskLeaf) isWide() bool { return l.wide != nil }

func (l *raskLeaf) len() int {
	if l.wide != nil {
		return len(l.wide)
	}
	return len(l.compact)
}

func (l *raskLeaf) end() uint64 {
	if l.wide != nil {
		return l.wide[len(l.wide)-1].b
	}
	c := l.compact[len(l.compact)-1]
	return l.base + uint64(c.deltaOff) + uint64(c.len32)
}

func (l *raskLeaf) at(i int) run {
	if l.wide != nil {
		return l.wide[i]
	}
	c := l.compact[i]
	a := l.base + uint64(c.deltaOff)
	return run{a: a, b: a + uint64(c.len32)}
}

func (l *raskLeaf) toRuns() []run {
	if l.wide != nil {
		out := make([]run, len(l.wide))
		copy(out, l.wide)
		return out
	}
	out := make([]run, len(l.compact))
	for i, c := range l.compact {
		a := l.base + uint64(c.deltaOff)
		out[i] = run{a: a, b: a + uint64(c.len32)}
	}
	return out
}

func (n *raskInternal) maxEnd() uint64 {
	return n.maxEnds[len(n.maxEnds)-1]
}

func (n *raskInternal) minBase(level int) uint64 {
	cur := n
	for level > 1 {
		cur = cur.internals[0]
		level--
	}
	return cur.leaves[0].base
}

// nextCompactCap aligns slice capacity with Go runtime small-object size classes
// (in units of 8-byte compactRun) so clusters of arbitrary size avoid slack waste.
func nextCompactCap(need int) int {
	switch {
	case need <= 8:
		return 8
	case need <= 16:
		return 16
	case need <= 24:
		return 24
	case need <= 32:
		return 32
	case need <= 40:
		return 40
	case need <= 48:
		return 48
	case need <= 56:
		return 56
	case need <= 64:
		return 64
	case need <= 72:
		return 72
	case need <= 80:
		return 80
	case need <= 88:
		return 88
	case need <= 96:
		return 96
	case need <= 112:
		return 112
	case need <= 128:
		return 128
	case need <= 144:
		return 144
	case need <= 160:
		return 160
	case need <= 176:
		return 176
	case need <= 192:
		return 192
	case need <= 224:
		return 224
	default:
		return raskLeafCap
	}
}

func nextWideCap(need int) int {
	switch {
	case need <= 4:
		return 4
	case need <= 8:
		return 8
	case need <= 16:
		return 16
	case need <= 32:
		return 32
	case need <= 48:
		return 48
	case need <= 64:
		return 64
	case need <= 96:
		return 96
	default:
		return raskWideCap
	}
}

func compactReplace(s []compactRun, i, j int, ins ...compactRun) []compactRun {
	newLen := len(s) - (j - i) + len(ins)
	if newLen <= cap(s) {
		oldLen := len(s)
		s = s[:newLen]
		copy(s[i+len(ins):], s[j:oldLen])
		copy(s[i:], ins)
		if newLen < oldLen {
			clear(s[newLen:oldLen])
		}
		return s
	}
	out := make([]compactRun, newLen, nextCompactCap(newLen))
	copy(out, s[:i])
	copy(out[i:], ins)
	copy(out[i+len(ins):], s[j:])
	return out
}

func compactShrink(s []compactRun) []compactRun {
	if len(s) == 0 {
		return nil
	}
	if len(s)*2 <= cap(s) && cap(s) > 16 {
		out := make([]compactRun, len(s), nextCompactCap(len(s)))
		copy(out, s)
		return out
	}
	return s
}

func wideReplace(s []run, i, j int, ins ...run) []run {
	newLen := len(s) - (j - i) + len(ins)
	if newLen <= cap(s) {
		oldLen := len(s)
		s = s[:newLen]
		copy(s[i+len(ins):], s[j:oldLen])
		copy(s[i:], ins)
		if newLen < oldLen {
			clear(s[newLen:oldLen])
		}
		return s
	}
	out := make([]run, newLen, nextWideCap(newLen))
	copy(out, s[:i])
	copy(out[i:], ins)
	copy(out[i+len(ins):], s[j:])
	return out
}

func wideShrink(s []run) []run {
	if len(s) == 0 {
		return nil
	}
	if len(s)*2 <= cap(s) && cap(s) > 8 {
		out := make([]run, len(s), nextWideCap(len(s)))
		copy(out, s)
		return out
	}
	return s
}

func makeCompactRuns(runs []run) []compactRun {
	base := runs[0].a
	c := make([]compactRun, len(runs), nextCompactCap(len(runs)))
	for i, r := range runs {
		c[i] = compactRun{
			deltaOff: uint32(r.a - base),
			len32:    uint32(r.b - r.a),
		}
	}
	return c
}

func newLeafFromRuns(runs []run) *raskLeaf {
	if runs[len(runs)-1].b-runs[0].a <= math.MaxUint32 && len(runs) <= raskLeafCap {
		return &raskLeaf{
			base:    runs[0].a,
			compact: makeCompactRuns(runs),
		}
	}
	w := make([]run, len(runs), nextWideCap(len(runs)))
	copy(w, runs)
	return &raskLeaf{
		base: runs[0].a,
		wide: w,
	}
}

func (l *raskLeaf) setFromRuns(runs []run) {
	l.base = runs[0].a
	if runs[len(runs)-1].b-l.base <= math.MaxUint32 && len(runs) <= raskLeafCap {
		l.wide = nil
		l.compact = makeCompactRuns(runs)
		return
	}
	l.compact = nil
	w := make([]run, len(runs), nextWideCap(len(runs)))
	copy(w, runs)
	l.wide = w
}

func searchMaxEndGE(maxEnds []uint64, target uint64) int {
	lo, hi := 0, len(maxEnds)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if maxEnds[mid] < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func searchMaxEndGT(maxEnds []uint64, target uint64) int {
	lo, hi := 0, len(maxEnds)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if maxEnds[mid] <= target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (idx *RaskRunIndex) descendGT(target uint64) *raskLeaf {
	if idx.height == 0 {
		if idx.rootLeaf == nil || idx.rootLeaf.end() <= target {
			return nil
		}
		return idx.rootLeaf
	}
	cur := idx.root
	for level := idx.height; level > 0; level-- {
		i := searchMaxEndGT(cur.maxEnds, target)
		if i >= len(cur.maxEnds) {
			return nil
		}
		if level == 1 {
			return cur.leaves[i]
		}
		cur = cur.internals[i]
	}
	return nil
}

func (idx *RaskRunIndex) descendGEWithPath(target uint64, buf []raskFrame) (*raskLeaf, []raskFrame) {
	if idx.height == 0 {
		return idx.rootLeaf, buf[:0]
	}
	cur := idx.root
	depth := 0
	for level := idx.height; level > 0; level-- {
		i := searchMaxEndGE(cur.maxEnds, target)
		if i >= len(cur.maxEnds) {
			i = len(cur.maxEnds) - 1
		}
		buf[depth] = raskFrame{node: cur, idx: i}
		depth++
		if level == 1 {
			return cur.leaves[i], buf[:depth]
		}
		cur = cur.internals[i]
	}
	return nil, buf[:0]
}

func (idx *RaskRunIndex) descendGTWithPath(target uint64, buf []raskFrame) (*raskLeaf, []raskFrame) {
	if idx.height == 0 {
		if idx.rootLeaf == nil || idx.rootLeaf.end() <= target {
			return nil, buf[:0]
		}
		return idx.rootLeaf, buf[:0]
	}
	cur := idx.root
	depth := 0
	for level := idx.height; level > 0; level-- {
		i := searchMaxEndGT(cur.maxEnds, target)
		if i >= len(cur.maxEnds) {
			return nil, buf[:0]
		}
		buf[depth] = raskFrame{node: cur, idx: i}
		depth++
		if level == 1 {
			return cur.leaves[i], buf[:depth]
		}
		cur = cur.internals[i]
	}
	return nil, buf[:0]
}

func prevLeafWithPath(path []raskFrame, out []raskFrame) (*raskLeaf, []raskFrame) {
	d := len(path)
	if d == 0 {
		return nil, nil
	}
	copy(out[:d], path)
	k := d - 1
	for k >= 0 && out[k].idx == 0 {
		k--
	}
	if k < 0 {
		return nil, nil
	}
	out[k].idx--
	if k == d-1 {
		return out[k].node.leaves[out[k].idx], out[:d]
	}
	cur := out[k].node.internals[out[k].idx]
	for lv := k + 1; lv < d; lv++ {
		lastIdx := len(cur.maxEnds) - 1
		out[lv] = raskFrame{node: cur, idx: lastIdx}
		if lv == d-1 {
			return cur.leaves[lastIdx], out[:d]
		}
		cur = cur.internals[lastIdx]
	}
	return nil, nil
}

// Contains reports whether byte offset off is covered by any run in the index.
func (idx *RaskRunIndex) Contains(off uint64) bool {
	l := idx.descendGT(off)
	if l == nil || off < l.base {
		return false
	}
	if !l.isWide() {
		rel := uint32(off - l.base)
		c := l.compact
		lo, hi := 0, len(c)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if c[mid].deltaOff+c[mid].len32 <= rel {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo < len(c) && c[lo].deltaOff <= rel
	}
	w := l.wide
	lo, hi := 0, len(w)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if w[mid].b <= off {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(w) && w[lo].a <= off
}

// Overlaps reports whether [a, b) intersects any run in the index.
func (idx *RaskRunIndex) Overlaps(a, b uint64) bool {
	if a >= b {
		return false
	}
	l := idx.descendGT(a)
	if l == nil || l.base >= b {
		return false
	}
	if !l.isWide() {
		var relA uint32
		if a > l.base {
			relA = uint32(a - l.base)
		}
		c := l.compact
		lo, hi := 0, len(c)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if c[mid].deltaOff+c[mid].len32 <= relA {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo < len(c) && l.base+uint64(c[lo].deltaOff) < b
	}
	w := l.wide
	lo, hi := 0, len(w)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if w[mid].b <= a {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(w) && w[lo].a < b
}

// RangeOverlaps visits in sorted order every run in idx that overlaps [a, b).
func (idx *RaskRunIndex) RangeOverlaps(a, b uint64, fn func(run)) {
	if a >= b || idx.count == 0 {
		return
	}
	for l := idx.descendGT(a); l != nil && l.base < b; l = l.next {
		if !l.isWide() {
			var relA uint32
			if a > l.base {
				relA = uint32(a - l.base)
			}
			c := l.compact
			lo, hi := 0, len(c)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if c[mid].deltaOff+c[mid].len32 <= relA {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			for k := lo; k < len(c); k++ {
				ra := l.base + uint64(c[k].deltaOff)
				if ra >= b {
					return
				}
				fn(run{a: ra, b: ra + uint64(c[k].len32)})
			}
			continue
		}
		w := l.wide
		lo, hi := 0, len(w)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if w[mid].b <= a {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		for k := lo; k < len(w); k++ {
			if w[k].a >= b {
				return
			}
			fn(w[k])
		}
	}
}

// NewRaskRunIndexFromSorted builds a RaskRunIndex from an already sorted,
// disjoint, non-touching slice of runs.
func NewRaskRunIndexFromSorted(runs []run) *RaskRunIndex {
	idx := NewRaskRunIndex()
	for _, r := range runs {
		idx.InsertRange(r.a, r.b)
	}
	return idx
}

// Clone returns an independent deep copy of idx.
func (idx *RaskRunIndex) Clone() *RaskRunIndex {
	if idx == nil {
		return nil
	}
	return NewRaskRunIndexFromSorted(idx.AllRuns())
}

// AllRuns returns all runs in sorted order.
func (idx *RaskRunIndex) AllRuns() []run {
	if idx.count == 0 {
		return nil
	}
	out := make([]run, 0, idx.count)
	for l := idx.first; l != nil; l = l.next {
		if l.wide != nil {
			out = append(out, l.wide...)
		} else {
			for _, c := range l.compact {
				a := l.base + uint64(c.deltaOff)
				out = append(out, run{a: a, b: a + uint64(c.len32)})
			}
		}
	}
	return out
}

// Stats returns structural counts and exact StructuralBytes of idx.
//
// StructuralBytes sums unsafe.Sizeof of structs and cap(slice)*unsafe.Sizeof(elem)
// of backing slices; it does not include Go runtime size-class rounding or GC
// heap metadata.
func (idx *RaskRunIndex) Stats() RaskStats {
	st := RaskStats{
		Runs:            idx.count,
		Height:          idx.height,
		StructuralBytes: int64(unsafe.Sizeof(*idx)),
	}
	for l := idx.first; l != nil; l = l.next {
		st.Leaves++
		if l.wide != nil {
			st.WideLeaves++
		} else {
			st.CompactLeaves++
		}
		st.StructuralBytes += int64(unsafe.Sizeof(*l))
		st.StructuralBytes += int64(cap(l.compact)) * int64(unsafe.Sizeof(compactRun{}))
		st.StructuralBytes += int64(cap(l.wide)) * int64(unsafe.Sizeof(run{}))
	}
	if idx.root != nil {
		st.addInternalStats(idx.root, idx.height)
	}
	return st
}

func (st *RaskStats) addInternalStats(n *raskInternal, level int) {
	st.InternalNodes++
	st.StructuralBytes += int64(unsafe.Sizeof(*n))
	st.StructuralBytes += int64(cap(n.maxEnds)) * int64(unsafe.Sizeof(uint64(0)))
	st.StructuralBytes += int64(cap(n.leaves)) * int64(unsafe.Sizeof((*raskLeaf)(nil)))
	st.StructuralBytes += int64(cap(n.internals)) * int64(unsafe.Sizeof((*raskInternal)(nil)))
	if level > 1 {
		for _, ch := range n.internals {
			st.addInternalStats(ch, level-1)
		}
	}
}

// StructuralBytes returns the structural byte footprint (sizeof structs +
// cap(slice)*sizeof(elem)) of idx, excluding Go allocator size-class rounding
// and GC overhead.
func (idx *RaskRunIndex) StructuralBytes() int64 {
	return idx.Stats().StructuralBytes
}

// MemoryBytes is an alias for StructuralBytes kept for caller compatibility.
// It returns structural sizeof + slice-cap bytes, NOT Go allocator size-class
// rounding or GC heap overhead.
func (idx *RaskRunIndex) MemoryBytes() int64 {
	return idx.Stats().StructuralBytes
}

// InsertRange adds [a, b) into the index, coalescing overlapping or adjacent runs.
func (idx *RaskRunIndex) InsertRange(a, b uint64) {
	if a >= b {
		return
	}
	if idx.count == 0 {
		l := newLeafFromRuns([]run{{a: a, b: b}})
		idx.rootLeaf = l
		idx.first = l
		idx.last = l
		idx.count = 1
		return
	}

	var pathBuf [16]raskFrame
	var prevBuf [16]raskFrame
	l, path := idx.descendGEWithPath(a, pathBuf[:])

	// When [a, b) lands strictly in the gap before l (a < b < l.base), route it
	// to l's left sibling when it fits into that sibling's compact window or is
	// closer to that sibling than to l, preventing fragmenting distant leaves.
	if b < l.base && l != idx.first {
		if prevLeaf, prevPath := prevLeafWithPath(path, prevBuf[:]); prevLeaf != nil {
			prevFitsCompact := !prevLeaf.isWide() && prevLeaf.len() < raskLeafCap && b-prevLeaf.base <= math.MaxUint32
			prevCloserWide := prevLeaf.isWide() && prevLeaf.len() < raskWideCap && (a-prevLeaf.end()) <= (l.base-b)
			if prevFitsCompact || prevCloserWide {
				l = prevLeaf
				path = prevPath
			}
		}
	}

	if l.next != nil && b >= l.next.base {
		idx.insertMultiLeaf(l, a, b)
		return
	}

	right, delta := leafInsertSingle(l, a, b)
	idx.count += delta
	if right == nil {
		idx.propagateMaxEnd(path, l.end())
		return
	}
	idx.insertSplit(l, right, path)
}

func (idx *RaskRunIndex) insertMultiLeaf(l *raskLeaf, a, b uint64) {
	mergedA, mergedB := a, b
	lEnd := l.end()
	if lEnd >= a {
		if !l.isWide() {
			var relA uint32
			if a > l.base {
				relA = uint32(a - l.base)
			}
			c := l.compact
			lo, hi := 0, len(c)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if c[mid].deltaOff+c[mid].len32 < relA {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			if lo < len(c) {
				if firstA := l.base + uint64(c[lo].deltaOff); firstA < mergedA {
					mergedA = firstA
				}
			}
		} else {
			w := l.wide
			lo, hi := 0, len(w)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if w[mid].b < a {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			if lo < len(w) && w[lo].a < mergedA {
				mergedA = w[lo].a
			}
		}
		if lEnd > mergedB {
			mergedB = lEnd
		}
	}

	for cur := l.next; cur != nil && cur.base <= mergedB; cur = cur.next {
		curEnd := cur.end()
		if curEnd <= mergedB {
			continue
		}
		if !cur.isWide() {
			relB := uint32(mergedB - cur.base)
			c := cur.compact
			lo, hi := 0, len(c)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if c[mid].deltaOff <= relB {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			if lo > 0 {
				r := cur.at(lo - 1)
				if r.b > mergedB {
					mergedB = r.b
				}
			}
		} else {
			w := cur.wide
			lo, hi := 0, len(w)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if w[mid].a <= mergedB {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			if lo > 0 && w[lo-1].b > mergedB {
				mergedB = w[lo-1].b
			}
		}
		break
	}

	idx.PunchRange(mergedA, mergedB)
	idx.InsertRange(mergedA, mergedB)
}

func (idx *RaskRunIndex) propagateMaxEnd(path []raskFrame, childMax uint64) {
	for k := len(path) - 1; k >= 0; k-- {
		f := path[k]
		if f.node.maxEnds[f.idx] == childMax {
			break
		}
		f.node.maxEnds[f.idx] = childMax
		childMax = f.node.maxEnd()
	}
}

func (idx *RaskRunIndex) insertSplit(left, right *raskLeaf, path []raskFrame) {
	right.next = left.next
	if left.next == nil {
		idx.last = right
	}
	left.next = right

	leftEnd := left.end()
	rightEnd := right.end()

	if len(path) == 0 {
		idx.root = &raskInternal{
			maxEnds: []uint64{leftEnd, rightEnd},
			leaves:  []*raskLeaf{left, right},
		}
		idx.rootLeaf = nil
		idx.height = 1
		return
	}

	parentFrame := path[len(path)-1]
	parent := parentFrame.node
	pos := parentFrame.idx
	parent.maxEnds[pos] = leftEnd

	var newRightInt *raskInternal
	if len(parent.maxEnds) < raskNodeCap {
		parent.maxEnds = slices.Insert(parent.maxEnds, pos+1, rightEnd)
		parent.leaves = slices.Insert(parent.leaves, pos+1, right)
	} else {
		var bufMax [raskNodeCap + 1]uint64
		var bufLeaves [raskNodeCap + 1]*raskLeaf
		n := len(parent.maxEnds)
		copy(bufMax[:pos+1], parent.maxEnds[:pos+1])
		bufMax[pos+1] = rightEnd
		copy(bufMax[pos+2:], parent.maxEnds[pos+1:])
		copy(bufLeaves[:pos+1], parent.leaves[:pos+1])
		bufLeaves[pos+1] = right
		copy(bufLeaves[pos+2:], parent.leaves[pos+1:])

		total := n + 1
		mid := total / 2
		lMax := make([]uint64, mid, raskNodeCap)
		copy(lMax, bufMax[:mid])
		lLeaves := make([]*raskLeaf, mid, raskNodeCap)
		copy(lLeaves, bufLeaves[:mid])
		parent.maxEnds = lMax
		parent.leaves = lLeaves

		rMax := make([]uint64, total-mid, raskNodeCap)
		copy(rMax, bufMax[mid:total])
		rLeaves := make([]*raskLeaf, total-mid, raskNodeCap)
		copy(rLeaves, bufLeaves[mid:total])
		newRightInt = &raskInternal{maxEnds: rMax, leaves: rLeaves}
	}

	curLeft := parent
	for k := len(path) - 2; k >= 0; k-- {
		f := path[k]
		p := f.node
		p.maxEnds[f.idx] = curLeft.maxEnd()
		if newRightInt == nil {
			idx.propagateMaxEnd(path[:k], p.maxEnd())
			return
		}
		if len(p.maxEnds) < raskNodeCap {
			p.maxEnds = slices.Insert(p.maxEnds, f.idx+1, newRightInt.maxEnd())
			p.internals = slices.Insert(p.internals, f.idx+1, newRightInt)
			newRightInt = nil
		} else {
			var bufMax [raskNodeCap + 1]uint64
			var bufInts [raskNodeCap + 1]*raskInternal
			n := len(p.maxEnds)
			copy(bufMax[:f.idx+1], p.maxEnds[:f.idx+1])
			bufMax[f.idx+1] = newRightInt.maxEnd()
			copy(bufMax[f.idx+2:], p.maxEnds[f.idx+1:])
			copy(bufInts[:f.idx+1], p.internals[:f.idx+1])
			bufInts[f.idx+1] = newRightInt
			copy(bufInts[f.idx+2:], p.internals[f.idx+1:])

			total := n + 1
			mid := total / 2
			lMax := make([]uint64, mid, raskNodeCap)
			copy(lMax, bufMax[:mid])
			lInts := make([]*raskInternal, mid, raskNodeCap)
			copy(lInts, bufInts[:mid])
			p.maxEnds = lMax
			p.internals = lInts

			rMax := make([]uint64, total-mid, raskNodeCap)
			copy(rMax, bufMax[mid:total])
			rInts := make([]*raskInternal, total-mid, raskNodeCap)
			copy(rInts, bufInts[mid:total])
			newRightInt = &raskInternal{maxEnds: rMax, internals: rInts}
		}
		curLeft = p
	}

	if newRightInt != nil {
		idx.root = &raskInternal{
			maxEnds:   []uint64{idx.root.maxEnd(), newRightInt.maxEnd()},
			internals: []*raskInternal{idx.root, newRightInt},
		}
		idx.height++
	}
}

func insertIntoRunSlice(rs []run, a, b uint64) ([]run, int) {
	i := sort.Search(len(rs), func(i int) bool { return rs[i].b >= a })
	j := i
	for j < len(rs) && rs[j].a <= b {
		j++
	}
	mergedA, mergedB := a, b
	if i < j {
		if rs[i].a < mergedA {
			mergedA = rs[i].a
		}
		if rs[j-1].b > mergedB {
			mergedB = rs[j-1].b
		}
	}
	delta := 1 - (j - i)
	out := make([]run, 0, len(rs)+delta)
	out = append(out, rs[:i]...)
	out = append(out, run{a: mergedA, b: mergedB})
	out = append(out, rs[j:]...)
	return out, delta
}

func leafInsertSingle(l *raskLeaf, a, b uint64) (*raskLeaf, int) {
	if !l.isWide() {
		lEnd := l.end()
		newBase := min(l.base, a)
		newMaxEnd := max(lEnd, b)
		if newMaxEnd-newBase <= math.MaxUint32 {
			if a < l.base {
				shift := uint32(l.base - a)
				for k := range l.compact {
					l.compact[k].deltaOff += shift
				}
				l.base = a
			}
			relA := uint32(a - l.base)
			relB := uint32(b - l.base)
			c := l.compact
			lo, hi := 0, len(c)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if c[mid].deltaOff+c[mid].len32 < relA {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			i := lo
			j := i
			for j < len(c) && c[j].deltaOff <= relB {
				j++
			}
			mergedA, mergedB := relA, relB
			if i < j {
				if c[i].deltaOff < mergedA {
					mergedA = c[i].deltaOff
				}
				if endLast := c[j-1].deltaOff + c[j-1].len32; endLast > mergedB {
					mergedB = endLast
				}
			}
			delta := 1 - (j - i)
			newLen := len(c) + delta
			entry := compactRun{deltaOff: mergedA, len32: mergedB - mergedA}
			if newLen <= raskLeafCap {
				l.compact = compactReplace(c, i, j, entry)
				return nil, delta
			}
			if i == len(c) {
				right := newLeafFromRuns([]run{{a: a, b: b}})
				return right, delta
			}
			var buf [raskLeafCap + 1]compactRun
			copy(buf[:i], c[:i])
			buf[i] = entry
			copy(buf[i+1:], c[i:])
			mid := newLen / 2
			lc := make([]compactRun, mid, nextCompactCap(mid))
			copy(lc, buf[:mid])
			l.compact = lc

			rShift := buf[mid].deltaOff
			rBase := l.base + uint64(rShift)
			rc := make([]compactRun, newLen-mid, nextCompactCap(newLen-mid))
			for k := 0; k < newLen-mid; k++ {
				rc[k] = compactRun{deltaOff: buf[mid+k].deltaOff - rShift, len32: buf[mid+k].len32}
			}
			right := &raskLeaf{
				base:    rBase,
				compact: rc,
			}
			return right, delta
		}

		// Span exceeded 4 GiB: expand and split at wide boundary or promote to wide.
		runs := l.toRuns()
		merged, delta := insertIntoRunSlice(runs, a, b)
		if len(merged) == 1 {
			l.setFromRuns(merged)
			return nil, delta
		}
		if len(runs) >= 2 {
			split := -1
			for s := 1; s < len(merged); s++ {
				leftCompact := merged[s-1].b-merged[0].a <= math.MaxUint32
				rightCompact := merged[len(merged)-1].b-merged[s].a <= math.MaxUint32
				if leftCompact && rightCompact {
					split = s
					break
				}
			}
			if split == -1 {
				for s := 1; s < len(merged); s++ {
					leftOK := (merged[s-1].b-merged[0].a <= math.MaxUint32) || s <= raskWideCap
					rightOK := (merged[len(merged)-1].b-merged[s].a <= math.MaxUint32) || (len(merged)-s) <= raskWideCap
					if leftOK && rightOK {
						split = s
						break
					}
				}
			}
			if split > 0 {
				l.setFromRuns(merged[:split])
				right := newLeafFromRuns(merged[split:])
				return right, delta
			}
		}
		if len(merged) <= raskWideCap {
			l.setFromRuns(merged)
			return nil, delta
		}
		mid := len(merged) / 2
		l.setFromRuns(merged[:mid])
		right := newLeafFromRuns(merged[mid:])
		return right, delta
	}

	// Wide leaf path.
	w := l.wide
	lo, hi := 0, len(w)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if w[mid].b < a {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	i := lo
	j := i
	for j < len(w) && w[j].a <= b {
		j++
	}
	mergedA, mergedB := a, b
	if i < j {
		if w[i].a < mergedA {
			mergedA = w[i].a
		}
		if w[j-1].b > mergedB {
			mergedB = w[j-1].b
		}
	}
	delta := 1 - (j - i)
	newLen := len(w) + delta
	entry := run{a: mergedA, b: mergedB}
	if newLen <= raskWideCap {
		l.wide = wideReplace(w, i, j, entry)
		l.base = l.wide[0].a
		return nil, delta
	}
	if i == len(w) {
		right := newLeafFromRuns([]run{entry})
		return right, delta
	}
	var buf [raskWideCap + 1]run
	copy(buf[:i], w[:i])
	buf[i] = entry
	copy(buf[i+1:], w[i:])
	mid := findWideSplit(buf[:newLen])
	l.setFromRuns(buf[:mid])
	right := newLeafFromRuns(buf[mid:newLen])
	return right, delta
}

func findWideSplit(runs []run) int {
	n := len(runs)
	mid := n / 2
	bestIdx := mid
	var bestGap uint64
	lo := max(1, n/4)
	hi := min(n-1, (3*n)/4)
	for i := lo; i <= hi; i++ {
		gap := runs[i].a - runs[i-1].b
		if gap > math.MaxUint32 && gap > bestGap {
			bestGap = gap
			bestIdx = i
		}
	}
	return bestIdx
}

// Truncate discards or trims all runs strictly past limit.
func (idx *RaskRunIndex) Truncate(limit uint64) {
	if idx.count == 0 || limit == math.MaxUint64 {
		return
	}
	if limit == 0 {
		idx.height = 0
		idx.rootLeaf = nil
		idx.root = nil
		idx.first = nil
		idx.last = nil
		idx.count = 0
		return
	}
	idx.PunchRange(limit, math.MaxUint64)
}

// PunchRange deallocates [a, b), deleting, trimming, or splitting overlapping runs.
func (idx *RaskRunIndex) PunchRange(a, b uint64) {
	if a >= b || idx.count == 0 {
		return
	}
	if idx.height == 0 {
		l := idx.rootLeaf
		if l == nil || l.end() <= a || l.base >= b {
			return
		}
		right, delta := leafPunch(l, a, b)
		idx.count += delta
		if l.len() == 0 {
			idx.rootLeaf = nil
			idx.first = nil
			idx.last = nil
			idx.count = 0
			return
		}
		if right != nil {
			idx.insertSplit(l, right, nil)
		}
		return
	}

	var pathBuf [16]raskFrame
	l, path := idx.descendGTWithPath(a, pathBuf[:])
	if l == nil || l.base >= b {
		return
	}
	if l.next == nil || l.next.base >= b {
		oldBase := l.base
		right, delta := leafPunch(l, a, b)
		idx.count += delta
		if l.len() == 0 {
			idx.removeLeafAlongPath(l, oldBase, path)
			return
		}
		if right != nil {
			idx.insertSplit(l, right, path)
			return
		}
		idx.propagateMaxEnd(path, l.end())
		return
	}

	var prevLeaf *raskLeaf
	if l != idx.first {
		var prevBuf [16]raskFrame
		prevLeaf, _ = prevLeafWithPath(path, prevBuf[:])
	}
	if idx.punchInternal(idx.root, idx.height, a, b, &prevLeaf) {
		idx.height = 0
		idx.root = nil
		idx.rootLeaf = nil
		idx.first = nil
		idx.last = nil
		idx.count = 0
		return
	}
	idx.shrinkRoot()
}

func (idx *RaskRunIndex) unlinkLeaf(l, prev *raskLeaf) {
	if prev != nil {
		prev.next = l.next
	} else {
		idx.first = l.next
	}
	if l.next == nil {
		idx.last = prev
	}
	l.next = nil
}

func (idx *RaskRunIndex) shrinkRoot() {
	for idx.height > 0 && len(idx.root.maxEnds) == 1 {
		if idx.height == 1 {
			idx.rootLeaf = idx.root.leaves[0]
			idx.root = nil
			idx.height = 0
			return
		}
		idx.root = idx.root.internals[0]
		idx.height--
	}
}

func (idx *RaskRunIndex) removeLeafAlongPath(l *raskLeaf, _ uint64, path []raskFrame) {
	var prev *raskLeaf
	if l != idx.first {
		var prevBuf [16]raskFrame
		prev, _ = prevLeafWithPath(path, prevBuf[:])
	}
	idx.unlinkLeaf(l, prev)
	if len(path) == 0 {
		idx.rootLeaf = nil
		idx.count = 0
		return
	}
	d := len(path) - 1
	p := path[d].node
	pos := path[d].idx
	p.maxEnds = slices.Delete(p.maxEnds, pos, pos+1)
	p.leaves = slices.Delete(p.leaves, pos, pos+1)

	cur := p
	for k := d; k >= 0; k-- {
		if len(cur.maxEnds) == 0 {
			if k == 0 {
				idx.root = nil
				idx.rootLeaf = nil
				idx.height = 0
				idx.first = nil
				idx.last = nil
				idx.count = 0
				return
			}
			pf := path[k-1]
			pf.node.maxEnds = slices.Delete(pf.node.maxEnds, pf.idx, pf.idx+1)
			pf.node.internals = slices.Delete(pf.node.internals, pf.idx, pf.idx+1)
			cur = pf.node
			continue
		}
		if k > 0 {
			idx.propagateMaxEnd(path[:k], cur.maxEnd())
		}
		break
	}
	idx.shrinkRoot()
}

func (idx *RaskRunIndex) punchInternal(n *raskInternal, level int, a, b uint64, prevLeaf **raskLeaf) bool {
	i := searchMaxEndGT(n.maxEnds, a)
	for i < len(n.maxEnds) {
		if level == 1 {
			l := n.leaves[i]
			if l.base >= b {
				break
			}
			_, delta := leafPunch(l, a, b)
			idx.count += delta
			if l.len() == 0 {
				idx.unlinkLeaf(l, *prevLeaf)
				n.maxEnds = slices.Delete(n.maxEnds, i, i+1)
				n.leaves = slices.Delete(n.leaves, i, i+1)
				continue
			}
			*prevLeaf = l
			n.maxEnds[i] = l.end()
			i++
			continue
		}
		sub := n.internals[i]
		if sub.minBase(level-1) >= b {
			break
		}
		if idx.punchInternal(sub, level-1, a, b, prevLeaf) {
			n.maxEnds = slices.Delete(n.maxEnds, i, i+1)
			n.internals = slices.Delete(n.internals, i, i+1)
			continue
		}
		n.maxEnds[i] = sub.maxEnd()
		i++
	}
	return len(n.maxEnds) == 0
}

func leafPunch(l *raskLeaf, a, b uint64) (*raskLeaf, int) {
	if !l.isWide() {
		lEnd := l.end()
		var relA uint32
		if a > l.base {
			relA = uint32(a - l.base)
		}
		relB := uint32(lEnd - l.base)
		if b < lEnd {
			relB = uint32(b - l.base)
		}
		c := l.compact
		lo, hi := 0, len(c)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if c[mid].deltaOff+c[mid].len32 <= relA {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		i := lo
		j := i
		for j < len(c) && c[j].deltaOff < relB {
			j++
		}
		if i == j {
			return nil, 0
		}
		first := c[i]
		last := c[j-1]
		lastEnd := last.deltaOff + last.len32
		var rem [2]compactRun
		remN := 0
		if first.deltaOff < relA {
			rem[remN] = compactRun{deltaOff: first.deltaOff, len32: relA - first.deltaOff}
			remN++
		}
		if lastEnd > relB {
			rem[remN] = compactRun{deltaOff: relB, len32: lastEnd - relB}
			remN++
		}
		delta := remN - (j - i)
		newLen := len(c) + delta
		if newLen == 0 {
			l.compact = nil
			l.base = 0
			return nil, delta
		}
		if newLen <= raskLeafCap {
			c = compactReplace(c, i, j, rem[:remN]...)
			c = compactShrink(c)
			if shift := c[0].deltaOff; shift != 0 {
				for k := range c {
					c[k].deltaOff -= shift
				}
				l.base += uint64(shift)
			}
			l.compact = c
			return nil, delta
		}
		var buf [raskLeafCap + 1]compactRun
		copy(buf[:i], c[:i])
		copy(buf[i:], rem[:remN])
		copy(buf[i+remN:], c[j:])
		origBase := l.base
		mid := newLen / 2
		lc := make([]compactRun, mid, nextCompactCap(mid))
		copy(lc, buf[:mid])
		if shift := lc[0].deltaOff; shift != 0 {
			for k := range lc {
				lc[k].deltaOff -= shift
			}
			l.base = origBase + uint64(shift)
		}
		l.compact = lc

		rShift := buf[mid].deltaOff
		rBase := origBase + uint64(rShift)
		rc := make([]compactRun, newLen-mid, nextCompactCap(newLen-mid))
		for k := 0; k < newLen-mid; k++ {
			rc[k] = compactRun{deltaOff: buf[mid+k].deltaOff - rShift, len32: buf[mid+k].len32}
		}
		right := &raskLeaf{
			base:    rBase,
			compact: rc,
		}
		return right, delta
	}

	w := l.wide
	lo, hi := 0, len(w)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if w[mid].b <= a {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	i := lo
	j := i
	for j < len(w) && w[j].a < b {
		j++
	}
	if i == j {
		return nil, 0
	}
	first := w[i]
	last := w[j-1]
	var rem [2]run
	remN := 0
	if first.a < a {
		rem[remN] = run{a: first.a, b: a}
		remN++
	}
	if last.b > b {
		rem[remN] = run{a: b, b: last.b}
		remN++
	}
	delta := remN - (j - i)
	newLen := len(w) + delta
	if newLen == 0 {
		l.wide = nil
		l.base = 0
		return nil, delta
	}
	if newLen <= raskWideCap {
		w = wideReplace(w, i, j, rem[:remN]...)
		if w[len(w)-1].b-w[0].a <= math.MaxUint32 {
			l.setFromRuns(w)
			return nil, delta
		}
		l.wide = wideShrink(w)
		l.base = l.wide[0].a
		return nil, delta
	}
	var buf [raskWideCap + 1]run
	copy(buf[:i], w[:i])
	copy(buf[i:], rem[:remN])
	copy(buf[i+remN:], w[j:])
	mid := findWideSplit(buf[:newLen])
	l.setFromRuns(buf[:mid])
	right := newLeafFromRuns(buf[mid:newLen])
	return right, delta
}

func (idx *RaskRunIndex) verifyInvariants() error {
	if idx.count == 0 {
		if idx.root != nil || idx.rootLeaf != nil || idx.first != nil || idx.last != nil || idx.height != 0 {
			return fmt.Errorf("empty index has non-nil state")
		}
		return nil
	}
	var treeLeaves []*raskLeaf
	var walk func(n *raskInternal, level int) (uint64, error)
	walk = func(n *raskInternal, level int) (uint64, error) {
		if len(n.maxEnds) == 0 || len(n.maxEnds) > raskNodeCap {
			return 0, fmt.Errorf("internal node len=%d out of range", len(n.maxEnds))
		}
		if n == idx.root && len(n.maxEnds) < 2 {
			return 0, fmt.Errorf("root internal node has %d children (< 2)", len(n.maxEnds))
		}
		if level == 1 {
			if len(n.leaves) != len(n.maxEnds) || n.internals != nil {
				return 0, fmt.Errorf("level-1 internal mismatch")
			}
			for i, l := range n.leaves {
				lEnd := l.end()
				if lEnd != n.maxEnds[i] {
					return 0, fmt.Errorf("leaf end %d != parent %d", lEnd, n.maxEnds[i])
				}
				if i > 0 && n.maxEnds[i-1] >= n.maxEnds[i] {
					return 0, fmt.Errorf("parent maxEnds not strictly sorted at %d", i)
				}
				treeLeaves = append(treeLeaves, l)
			}
			return n.maxEnds[len(n.maxEnds)-1], nil
		}
		if len(n.internals) != len(n.maxEnds) || n.leaves != nil {
			return 0, fmt.Errorf("level-%d internal mismatch", level)
		}
		for i, sub := range n.internals {
			m, err := walk(sub, level-1)
			if err != nil {
				return 0, err
			}
			if m != n.maxEnds[i] {
				return 0, fmt.Errorf("child maxEnd %d != parent %d", m, n.maxEnds[i])
			}
			if i > 0 && n.maxEnds[i-1] >= n.maxEnds[i] {
				return 0, fmt.Errorf("parent maxEnds not strictly sorted at %d", i)
			}
		}
		return n.maxEnds[len(n.maxEnds)-1], nil
	}
	if idx.height == 0 {
		if idx.rootLeaf == nil || idx.root != nil {
			return fmt.Errorf("height 0 invalid root")
		}
		treeLeaves = []*raskLeaf{idx.rootLeaf}
	} else {
		if idx.root == nil || idx.rootLeaf != nil {
			return fmt.Errorf("height %d invalid root", idx.height)
		}
		if _, err := walk(idx.root, idx.height); err != nil {
			return err
		}
	}
	var listLeaves []*raskLeaf
	var prev *raskLeaf
	total := 0
	for l := idx.first; l != nil; l = l.next {
		if l.len() == 0 {
			return fmt.Errorf("empty leaf in tree")
		}
		if l.isWide() {
			if len(l.wide) > raskWideCap {
				return fmt.Errorf("wide leaf len %d > %d", len(l.wide), raskWideCap)
			}
			if l.base != l.wide[0].a {
				return fmt.Errorf("wide leaf base mismatch")
			}
			for k, r := range l.wide {
				if r.a >= r.b {
					return fmt.Errorf("invalid run [%d,%d)", r.a, r.b)
				}
				if k > 0 && l.wide[k-1].b >= r.a {
					return fmt.Errorf("adjacent/overlapping wide runs [%d,%d) and [%d,%d)", l.wide[k-1].a, l.wide[k-1].b, r.a, r.b)
				}
			}
		} else {
			if len(l.compact) > raskLeafCap {
				return fmt.Errorf("compact leaf len %d > %d", len(l.compact), raskLeafCap)
			}
			if l.end()-l.base > math.MaxUint32 {
				return fmt.Errorf("compact leaf span %d > MaxUint32", l.end()-l.base)
			}
			if l.compact[0].deltaOff != 0 {
				return fmt.Errorf("compact leaf first deltaOff %d != 0", l.compact[0].deltaOff)
			}
			var prevEnd uint64
			for k := range l.compact {
				c := l.compact[k]
				if c.len32 == 0 {
					return fmt.Errorf("compact leaf len32 == 0")
				}
				r := l.at(k)
				if k > 0 && prevEnd >= r.a {
					return fmt.Errorf("adjacent/overlapping compact runs")
				}
				prevEnd = r.b
			}
		}
		if prev != nil && prev.end() >= l.base {
			return fmt.Errorf("cross-leaf non-disjoint/adjacent: prev.end=%d >= l.base=%d", prev.end(), l.base)
		}
		total += l.len()
		listLeaves = append(listLeaves, l)
		prev = l
	}
	if prev != idx.last {
		return fmt.Errorf("last leaf pointer mismatch")
	}
	if len(listLeaves) != len(treeLeaves) {
		return fmt.Errorf("leaf list len %d != tree leaves %d", len(listLeaves), len(treeLeaves))
	}
	for i := range listLeaves {
		if listLeaves[i] != treeLeaves[i] {
			return fmt.Errorf("leaf %d order mismatch", i)
		}
	}
	if total != idx.count {
		return fmt.Errorf("run count %d != total %d", idx.count, total)
	}
	return nil
}

// BaselineRunSlice is the exact shipping 16-byte []run sorted binary-search
// representation matching file.go layer.void / join([]run), used as an
// apples-to-apples differential oracle and performance/memory baseline.
type BaselineRunSlice struct {
	runs []run
}

// NewBaselineRunSlice constructs an empty BaselineRunSlice.
func NewBaselineRunSlice() *BaselineRunSlice {
	return &BaselineRunSlice{}
}

// Len returns the number of disjoint coalesced runs currently stored.
func (b *BaselineRunSlice) Len() int { return len(b.runs) }

// StructuralBytes returns the structural byte footprint (sizeof struct +
// cap(runs)*sizeof(run)) of b, excluding Go allocator size-class rounding and
// GC heap metadata.
func (b *BaselineRunSlice) StructuralBytes() int64 {
	return int64(unsafe.Sizeof(*b)) + int64(cap(b.runs))*int64(unsafe.Sizeof(run{}))
}

// MemoryBytes is an alias for StructuralBytes kept for caller compatibility.
// It returns structural sizeof + slice-cap bytes, NOT Go allocator size-class
// rounding or GC heap overhead.
func (b *BaselineRunSlice) MemoryBytes() int64 {
	return b.StructuralBytes()
}

// AllRuns returns a copy of all runs in sorted order.
func (b *BaselineRunSlice) AllRuns() []run {
	if len(b.runs) == 0 {
		return nil
	}
	out := make([]run, len(b.runs))
	copy(out, b.runs)
	return out
}

// Contains reports whether byte offset off is covered by any run in b.
func (b *BaselineRunSlice) Contains(off uint64) bool {
	rs := b.runs
	i := sort.Search(len(rs), func(i int) bool { return rs[i].b > off })
	return i < len(rs) && rs[i].a <= off
}

// Overlaps reports whether [a, e) intersects any run in b.
func (b *BaselineRunSlice) Overlaps(a, e uint64) bool {
	if a >= e {
		return false
	}
	rs := b.runs
	i := sort.Search(len(rs), func(i int) bool { return rs[i].b > a })
	return i < len(rs) && rs[i].a < e
}

// InsertRange adds [a, e) into b, coalescing overlapping or adjacent runs.
func (b *BaselineRunSlice) InsertRange(a, e uint64) {
	if a >= e {
		return
	}
	rs := b.runs
	if n := len(rs); n == 0 || a > rs[n-1].b {
		b.runs = append(rs, run{a: a, b: e})
		return
	}
	if a == rs[len(rs)-1].b {
		rs[len(rs)-1].b = max(rs[len(rs)-1].b, e)
		return
	}
	i := sort.Search(len(rs), func(i int) bool { return rs[i].b >= a })
	j := i
	for j < len(rs) && rs[j].a <= e {
		j++
	}
	merged := run{a: a, b: e}
	if i < j {
		merged.a = min(merged.a, rs[i].a)
		merged.b = max(merged.b, rs[j-1].b)
	}
	b.runs = slices.Replace(rs, i, j, merged)
}

// PunchRange deallocates [a, e), deleting, trimming, or splitting overlapping runs.
func (b *BaselineRunSlice) PunchRange(a, e uint64) {
	if a >= e || len(b.runs) == 0 {
		return
	}
	rs := b.runs
	i := sort.Search(len(rs), func(i int) bool { return rs[i].b > a })
	j := i
	for j < len(rs) && rs[j].a < e {
		j++
	}
	if i == j {
		return
	}
	var rem [2]run
	remN := 0
	if rs[i].a < a {
		rem[remN] = run{a: rs[i].a, b: a}
		remN++
	}
	if rs[j-1].b > e {
		rem[remN] = run{a: e, b: rs[j-1].b}
		remN++
	}
	b.runs = slices.Replace(rs, i, j, rem[:remN]...)
}

// Truncate discards or trims all runs strictly past limit.
func (b *BaselineRunSlice) Truncate(limit uint64) {
	rs := b.runs
	for len(rs) > 0 && rs[len(rs)-1].b > limit {
		last := &rs[len(rs)-1]
		if last.a >= limit {
			rs = rs[:len(rs)-1]
		} else {
			last.b = limit
		}
	}
	b.runs = rs
}

// RASKBenchmarkConfig configures RunRASKBenchmark.
type RASKBenchmarkConfig struct {
	// OpenBackend opens a backend.Backend for the real vfs.Volume journal replay
	// workload. Required.
	OpenBackend func() (backend.Backend, error)
	// JournalDir is a non-volatile directory for the temporary disk journal.
	JournalDir string
	// Full selects larger synthetic run counts (100,000 vs 50,000 runs).
	Full bool
	// Short reduces journal op counts for fast test smoke execution.
	Short bool
}

// RASKWorkloadResult captures measured structural, retained heap, RSS, CPU, GC,
// and timing metrics for one regime comparing BaselineRunSlice against RaskRunIndex.
type RASKWorkloadResult struct {
	Category                       string  `json:"category"` // "real_volume_journal_replay" or "synthetic_microbenchmark"
	Name                           string  `json:"name"`
	Description                    string  `json:"description"`
	Ops                            int     `json:"ops"`
	FinalRuns                      int     `json:"final_runs"`
	CompactLeaves                  int     `json:"compact_leaves"`
	WideLeaves                     int     `json:"wide_leaves"`
	BaselineStructuralBytes        int64   `json:"baseline_structural_bytes"`
	BaselineStructuralBytesPerRun  float64 `json:"baseline_structural_bytes_per_run"`
	BaselineRetainedHeapAllocBytes int64   `json:"baseline_retained_heap_alloc_bytes"`
	BaselineRetainedHeapInuseBytes int64   `json:"baseline_retained_heap_inuse_bytes"`
	BaselineRSSDeltaBytes          int64   `json:"baseline_rss_delta_bytes"`
	BaselineCPUTimeMs              float64 `json:"baseline_cpu_time_ms"`
	BaselineGCCycles               uint32  `json:"baseline_gc_cycles"`
	RaskStructuralBytes            int64   `json:"rask_structural_bytes"`
	RaskStructuralBytesPerRun      float64 `json:"rask_structural_bytes_per_run"`
	RaskRetainedHeapAllocBytes     int64   `json:"rask_retained_heap_alloc_bytes"`
	RaskRetainedHeapInuseBytes     int64   `json:"rask_retained_heap_inuse_bytes"`
	RaskRSSDeltaBytes              int64   `json:"rask_rss_delta_bytes"`
	RaskCPUTimeMs                  float64 `json:"rask_cpu_time_ms"`
	RaskGCCycles                   uint32  `json:"rask_gc_cycles"`
	StructuralSavingsPct           float64 `json:"structural_savings_pct"`
	HeapAllocSavingsPct            float64 `json:"heap_alloc_savings_pct"`
	BaselineBuildMs                float64 `json:"baseline_build_ms"`
	RaskBuildMs                    float64 `json:"rask_build_ms"`
	BaselineQueryNs                float64 `json:"baseline_query_mean_ns"`
	BaselineP50Ns                  float64 `json:"baseline_query_p50_ns"`
	BaselineP99Ns                  float64 `json:"baseline_query_p99_ns"`
	RaskQueryNs                    float64 `json:"rask_query_mean_ns"`
	RaskP50Ns                      float64 `json:"rask_query_p50_ns"`
	RaskP99Ns                      float64 `json:"rask_query_p99_ns"`
	BaselineOverlapNs              float64 `json:"baseline_overlap_mean_ns"`
	RaskOverlapNs                  float64 `json:"rask_overlap_mean_ns"`
	ExactMatch                     bool    `json:"exact_match"`
	TradeOffNote                   string  `json:"trade_off_note"`
}

// RASKJournalReplayMetadata records details from the real vfs.Volume journal
// extraction stage.
type RASKJournalReplayMetadata struct {
	LogicalSizeBytes    uint64  `json:"logical_size_bytes"`
	JournalRecords      int     `json:"journal_records"`
	WriteRecords        int     `json:"write_records"`
	PunchRecords        int     `json:"punch_records"`
	SizeRecords         int     `json:"size_records"`
	ReplayExtractMs     float64 `json:"replay_extract_ms"`
	VoidSpans           int     `json:"void_spans"`
	VoidBaselineBPerRun float64 `json:"void_baseline_bytes_per_run"`
	VoidRaskBPerRun     float64 `json:"void_rask_bytes_per_run"`
}

// RASKBenchmarkReport bundles real journal replay and synthetic 1 PiB
// microbenchmark measurements.
type RASKBenchmarkReport struct {
	GoVersion     string                    `json:"go_version"`
	OSArch        string                    `json:"os_arch"`
	Scale         string                    `json:"scale"`
	JournalReplay RASKJournalReplayMetadata `json:"journal_replay"`
	Workloads     []RASKWorkloadResult      `json:"workloads"`
}

var raskBenchSink uint64

// RunRASKBenchmark executes both the real vfs.Volume 1 PiB disk-backed journal
// replay trace and synthetic multi-regime 1 PiB sparse-range benchmarks.
func RunRASKBenchmark(ctx context.Context, cfg RASKBenchmarkConfig) (RASKBenchmarkReport, error) {
	if cfg.OpenBackend == nil {
		return RASKBenchmarkReport{}, fmt.Errorf("rask: OpenBackend is required")
	}
	if cfg.JournalDir == "" {
		return RASKBenchmarkReport{}, fmt.Errorf("rask: JournalDir is required")
	}

	scale := "standard"
	synthN := 50_000
	journalBursts := 2400
	if cfg.Full {
		scale = "full"
		synthN = 100_000
		journalBursts = 4000
	} else if cfg.Short {
		scale = "short"
		synthN = 2000
		journalBursts = 200
	}

	rep := RASKBenchmarkReport{
		GoVersion: runtime.Version(),
		OSArch:    runtime.GOOS + "/" + runtime.GOARCH,
		Scale:     scale,
	}

	realRes, jMeta, err := runRealJournalReplayWorkload(ctx, cfg.OpenBackend, cfg.JournalDir, journalBursts)
	if err != nil {
		return rep, err
	}
	rep.JournalReplay = jMeta
	rep.Workloads = append(rep.Workloads, realRes)

	workloads := []struct {
		name  string
		desc  string
		note  string
		build func() ([]raskOp, []uint64, [][2]uint64)
	}{
		{
			name:  "synth_clustered_seq_1pib",
			desc:  fmt.Sprintf("Clustered 1 PiB sequential writes (%d runs across 200 TiB-spaced clusters)", synthN),
			note:  "RASK compresses clustered runs below 9 B/run; however, flat []run append is O(1) so sequential build and L1/L2 point lookup p50 remain faster on []run.",
			build: func() ([]raskOp, []uint64, [][2]uint64) { return genClusteredSeqOps(synthN) },
		},
		{
			name:  "synth_clustered_rand_1pib",
			desc:  fmt.Sprintf("Clustered 1 PiB random-order writes + 20%% punches (%d writes + %d punches)", synthN, synthN/5),
			note:  "RASK achieves < 9.5 B/run and O(log N) inserts/punches vs O(N) middle slice shifts in []run, while []run retains slightly lower warm point-lookup p50.",
			build: func() ([]raskOp, []uint64, [][2]uint64) { return genClusteredRandPunchOps(synthN) },
		},
		{
			name:  "synth_strided_sub4g_1pib",
			desc:  fmt.Sprintf("Uniform 10 MiB strided extents (%d runs; < 4 GiB per 256-run leaf)", synthN),
			note:  "10 MiB stride matches TestTinyExtentsCost; 256 runs span 2.5 GiB < 4 GiB so all leaves stay compact (< 8.5 B/run), trading slightly higher sequential build and lookup p50 vs flat []run.",
			build: func() ([]raskOp, []uint64, [][2]uint64) { return genStridedOps(synthN, 10<<20, 4096) },
		},
		{
			name:  "synth_strided_gt4g_1pib",
			desc:  fmt.Sprintf("Widely scattered 5 GiB strided extents (%d runs across 1 PiB; > 4 GiB gap every run)", synthN),
			note:  "Honest negative/trade-off regime: every run exceeds 4 GiB from neighbors so leaves promote to 16 B/run wide mode (exceeding the packed 16.00 B/run []run floor due to leaf headers, with higher point-lookup p50; lower total bytes vs []run here reflects Go slice growth slack at 50k).",
			build: func() ([]raskOp, []uint64, [][2]uint64) { return genStridedOps(synthN, 5<<30, 4096) },
		},
		{
			name:  "synth_small_n_seq",
			desc:  "Negative boundary control: small sequential file (128 extents, 64 KiB stride)",
			note:  "Honest small-N boundary: flat []run fits in 2 KiB L1 cache with O(1) append, faster sequential build, and faster point/overlap lookups.",
			build: func() ([]raskOp, []uint64, [][2]uint64) { return genStridedOps(128, 64<<10, 4096) },
		},
	}

	for _, w := range workloads {
		ops, qOffs, qRanges := w.build()
		res, err := measureSyntheticWorkload("synthetic_microbenchmark", w.name, w.desc, w.note, ops, qOffs, qRanges)
		if err != nil {
			return rep, err
		}
		rep.Workloads = append(rep.Workloads, res)
	}

	return rep, nil
}

type raskOpKind uint8

const (
	raskOpInsert raskOpKind = iota
	raskOpPunch
	raskOpTruncate
)

type raskOp struct {
	kind raskOpKind
	a, b uint64
}

func runRealJournalReplayWorkload(
	ctx context.Context,
	openBE func() (backend.Backend, error),
	journalDir string,
	bursts int,
) (RASKWorkloadResult, RASKJournalReplayMetadata, error) {
	const pib = uint64(1) << 50
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	ciph := Hybrid(id)

	be0, err := openBE()
	if err != nil {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	if err := Create(ctx, Config{Backend: be0, Cipher: ciph}); err != nil {
		_ = be0.Close()
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	_ = be0.Close()

	be1, err := openBE()
	if err != nil {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	volCfg := Config{
		Backend:         be1,
		Cipher:          ciph,
		Round:           1,
		Journal:         journalDir,
		JournalCapacity: 64 << 20,
		Dirty:           1 << 30,
		Overlay:         1 << 20,
		Bound:           time.Hour,
	}
	v, err := Attach(ctx, volCfg)
	if err != nil {
		_ = be1.Close()
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	in, err := v.Create(ctx, RootInode, "pib-trace.bin", 0o644)
	if err != nil {
		_ = v.Close()
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		_ = v.Close()
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	if err := f.Truncate(ctx, pib); err != nil {
		_ = f.Close()
		_ = v.Close()
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}

	const clusters = 16
	const clusterSpan = uint64(50) << 40
	payload := []byte("rask-real-journal-span-payload-64b-0123456789abcdef0123456789ab")
	var writtenSpots []uint64
	for i := range bursts {
		cl := uint64(i % clusters)
		idxInCluster := uint64(i / clusters)
		off := cl*clusterSpan + idxInCluster*16384
		if off+uint64(len(payload)) > pib {
			continue
		}
		if _, err := f.WriteAt(ctx, payload, int64(off)); err != nil {
			_ = f.Close()
			_ = v.Close()
			return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
		}
		writtenSpots = append(writtenSpots, off)
		if i%7 == 5 {
			pOff := off + 16
			if err := f.Punch(ctx, pOff, 16); err != nil {
				_ = f.Close()
				_ = v.Close()
				return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
			}
		}
	}
	if err := v.Sync(ctx); err != nil {
		_ = f.Close()
		_ = v.Close()
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	_ = f.Close()

	v.mu.RLock()
	headJournal := v.head.Journal
	v.mu.RUnlock()
	if err := v.Close(); err != nil {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	if headJournal != 0 {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, fmt.Errorf("expected un-flushed volume head.Journal=0, got %d", headJournal)
	}

	be2, err := openBE()
	if err != nil {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	ro, err := Open(ctx, Config{
		Backend:         be2,
		Cipher:          ciph,
		Journal:         journalDir,
		JournalCapacity: 64 << 20,
		Bound:           time.Hour,
	})
	if err != nil {
		_ = be2.Close()
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	defer ro.Close()

	led, err := ro.openLedger(1, ro.head.Journal)
	if err != nil {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}
	defer led.j.Close()

	type replayEntry struct {
		kind changeKind
		off  uint64
		size uint64
	}
	var entries []replayEntry
	tReplay := time.Now()
	var writeRecs, punchRecs, sizeRecs int
	err = led.j.Replay(func(r journal.Record) error {
		plain, err := led.open(r.Data)
		if err != nil {
			return err
		}
		ch, err := decodeChange(plain)
		if err != nil {
			return err
		}
		if ch.ino != in.ID {
			return nil
		}
		switch ch.kind {
		case changeWrite:
			writeRecs++
			entries = append(entries, replayEntry{kind: changeWrite, off: ch.off, size: uint64(len(ch.data))})
		case changePunch:
			punchRecs++
			entries = append(entries, replayEntry{kind: changePunch, off: ch.off, size: ch.size})
		case changeSize:
			sizeRecs++
			entries = append(entries, replayEntry{kind: changeSize, size: ch.size})
		}
		return nil
	})
	replayExtractMs := float64(time.Since(tReplay).Nanoseconds()) / 1e6
	if err != nil {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}

	ops := make([]raskOp, 0, len(entries))
	voidOps := make([]raskOp, 0, punchRecs+sizeRecs)
	var curSize uint64
	for _, e := range entries {
		switch e.kind {
		case changeWrite:
			ops = append(ops, raskOp{kind: raskOpInsert, a: e.off, b: e.off + e.size})
			if e.off+e.size > curSize {
				curSize = e.off + e.size
			}
		case changePunch:
			a, b := min(e.off, curSize), min(e.off+e.size, curSize)
			if e.off+e.size < e.off {
				b = curSize
			}
			if a < b {
				ops = append(ops, raskOp{kind: raskOpPunch, a: a, b: b})
				voidOps = append(voidOps, raskOp{kind: raskOpInsert, a: a, b: b})
			}
		case changeSize:
			if e.size < curSize {
				ops = append(ops, raskOp{kind: raskOpTruncate, a: e.size})
				voidOps = append(voidOps, raskOp{kind: raskOpTruncate, a: e.size})
			}
			curSize = e.size
		}
	}

	qOffs := make([]uint64, 0, len(writtenSpots)*2)
	for _, off := range writtenSpots {
		qOffs = append(qOffs, off+4, off+24)
	}
	qRanges := make([][2]uint64, 0, len(writtenSpots))
	for _, off := range writtenSpots {
		qRanges = append(qRanges, [2]uint64{off, off + 64})
	}

	res, err := measureSyntheticWorkload(
		"real_volume_journal_replay",
		"real_journal_replay_1pib",
		fmt.Sprintf("Real vfs.Volume 1 PiB disk journal replay (%d write + %d punch + %d size records)", writeRecs, punchRecs, sizeRecs),
		"Real vfs.Volume journal records replayed via openLedger + j.Replay + decodeChange; compact leaves pack intra-cluster writes/punches into 8 B/run.",
		ops,
		qOffs,
		qRanges,
	)
	if err != nil {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, err
	}

	baseVoid := NewBaselineRunSlice()
	raskVoid := NewRaskRunIndex()
	for _, op := range voidOps {
		switch op.kind {
		case raskOpInsert:
			baseVoid.InsertRange(op.a, op.b)
			raskVoid.InsertRange(op.a, op.b)
		case raskOpTruncate:
			baseVoid.Truncate(op.a)
			raskVoid.Truncate(op.a)
		}
	}
	if !slices.Equal(baseVoid.AllRuns(), raskVoid.AllRuns()) {
		return RASKWorkloadResult{}, RASKJournalReplayMetadata{}, fmt.Errorf("void replay mismatch between BaselineRunSlice and RaskRunIndex")
	}

	var voidBaseB, voidRaskB float64
	if baseVoid.Len() > 0 {
		voidBaseB = float64(baseVoid.MemoryBytes()) / float64(baseVoid.Len())
		voidRaskB = float64(raskVoid.MemoryBytes()) / float64(raskVoid.Len())
	}

	jMeta := RASKJournalReplayMetadata{
		LogicalSizeBytes:    curSize,
		JournalRecords:      len(entries),
		WriteRecords:        writeRecs,
		PunchRecords:        punchRecs,
		SizeRecords:         sizeRecs,
		ReplayExtractMs:     replayExtractMs,
		VoidSpans:           baseVoid.Len(),
		VoidBaselineBPerRun: voidBaseB,
		VoidRaskBPerRun:     voidRaskB,
	}
	return res, jMeta, nil
}

func processRSSBytes() int64 {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || pages <= 0 {
		return 0
	}
	return pages * int64(os.Getpagesize())
}

func measureSyntheticWorkload(
	category, name, desc, note string,
	ops []raskOp,
	qOffs []uint64,
	qRanges [][2]uint64,
) (RASKWorkloadResult, error) {
	// Measure BaselineRunSlice in an isolated forced-GC window.
	runtime.GC()
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	rssBase0 := processRSSBytes()
	cpuBase0 := processCPUTimeNs()
	base := NewBaselineRunSlice()
	tBase := time.Now()
	for _, op := range ops {
		switch op.kind {
		case raskOpInsert:
			base.InsertRange(op.a, op.b)
		case raskOpPunch:
			base.PunchRange(op.a, op.b)
		case raskOpTruncate:
			base.Truncate(op.a)
		}
	}
	baseBuildMs := float64(time.Since(tBase).Nanoseconds()) / 1e6
	cpuBase1 := processCPUTimeNs()
	var mBuild1 runtime.MemStats
	runtime.ReadMemStats(&mBuild1)
	baseGCCycles := mBuild1.NumGC - m0.NumGC
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m1)
	rssBase1 := processRSSBytes()
	baseHeapAlloc := max(int64(0), int64(m1.HeapAlloc)-int64(m0.HeapAlloc))
	baseHeapInuse := max(int64(0), int64(m1.HeapInuse)-int64(m0.HeapInuse))
	baseRSSDelta := max(int64(0), rssBase1-rssBase0)
	baseCPUTimeMs := float64(max(int64(0), cpuBase1-cpuBase0)) / 1e6
	baseStruct := base.StructuralBytes()

	bMean, bP50, bP99 := benchPointLookups(base.Contains, qOffs)
	bOvMean := benchRangeLookups(base.Overlaps, qRanges)
	baseRuns := base.AllRuns()
	runtime.KeepAlive(base)
	base = nil

	// Measure RaskRunIndex in an isolated forced-GC window after releasing base.
	runtime.GC()
	runtime.GC()
	var r0, r1 runtime.MemStats
	runtime.ReadMemStats(&r0)
	rssRask0 := processRSSBytes()
	cpuRask0 := processCPUTimeNs()
	rask := NewRaskRunIndex()
	tRask := time.Now()
	for _, op := range ops {
		switch op.kind {
		case raskOpInsert:
			rask.InsertRange(op.a, op.b)
		case raskOpPunch:
			rask.PunchRange(op.a, op.b)
		case raskOpTruncate:
			rask.Truncate(op.a)
		}
	}
	raskBuildMs := float64(time.Since(tRask).Nanoseconds()) / 1e6
	cpuRask1 := processCPUTimeNs()
	var rBuild1 runtime.MemStats
	runtime.ReadMemStats(&rBuild1)
	raskGCCycles := rBuild1.NumGC - r0.NumGC
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&r1)
	rssRask1 := processRSSBytes()
	raskHeapAlloc := max(int64(0), int64(r1.HeapAlloc)-int64(r0.HeapAlloc))
	raskHeapInuse := max(int64(0), int64(r1.HeapInuse)-int64(r0.HeapInuse))
	raskRSSDelta := max(int64(0), rssRask1-rssRask0)
	raskCPUTimeMs := float64(max(int64(0), cpuRask1-cpuRask0)) / 1e6
	raskStats := rask.Stats()
	raskStruct := raskStats.StructuralBytes

	rMean, rP50, rP99 := benchPointLookups(rask.Contains, qOffs)
	rOvMean := benchRangeLookups(rask.Overlaps, qRanges)
	raskRuns := rask.AllRuns()
	exact := slices.Equal(baseRuns, raskRuns)
	if !exact {
		return RASKWorkloadResult{}, fmt.Errorf("%s: differential mismatch: baseline=%d runs, rask=%d runs", name, len(baseRuns), len(raskRuns))
	}
	if err := rask.verifyInvariants(); err != nil {
		return RASKWorkloadResult{}, fmt.Errorf("%s: invariant violation: %w", name, err)
	}
	runtime.KeepAlive(rask)

	n := max(1, len(baseRuns))
	baseBPerRun := float64(baseStruct) / float64(n)
	raskBPerRun := float64(raskStruct) / float64(n)
	structSavingsPct := (1.0 - raskBPerRun/baseBPerRun) * 100.0
	var heapSavingsPct float64
	if baseHeapAlloc > 0 {
		heapSavingsPct = (1.0 - float64(raskHeapAlloc)/float64(baseHeapAlloc)) * 100.0
	}

	return RASKWorkloadResult{
		Category:                       category,
		Name:                           name,
		Description:                    desc,
		Ops:                            len(ops),
		FinalRuns:                      len(baseRuns),
		CompactLeaves:                  raskStats.CompactLeaves,
		WideLeaves:                     raskStats.WideLeaves,
		BaselineStructuralBytes:        baseStruct,
		BaselineStructuralBytesPerRun:  baseBPerRun,
		BaselineRetainedHeapAllocBytes: baseHeapAlloc,
		BaselineRetainedHeapInuseBytes: baseHeapInuse,
		BaselineRSSDeltaBytes:          baseRSSDelta,
		BaselineCPUTimeMs:              baseCPUTimeMs,
		BaselineGCCycles:               baseGCCycles,
		RaskStructuralBytes:            raskStruct,
		RaskStructuralBytesPerRun:      raskBPerRun,
		RaskRetainedHeapAllocBytes:     raskHeapAlloc,
		RaskRetainedHeapInuseBytes:     raskHeapInuse,
		RaskRSSDeltaBytes:              raskRSSDelta,
		RaskCPUTimeMs:                  raskCPUTimeMs,
		RaskGCCycles:                   raskGCCycles,
		StructuralSavingsPct:           structSavingsPct,
		HeapAllocSavingsPct:            heapSavingsPct,
		BaselineBuildMs:                baseBuildMs,
		RaskBuildMs:                    raskBuildMs,
		BaselineQueryNs:                bMean,
		BaselineP50Ns:                  bP50,
		BaselineP99Ns:                  bP99,
		RaskQueryNs:                    rMean,
		RaskP50Ns:                      rP50,
		RaskP99Ns:                      rP99,
		BaselineOverlapNs:              bOvMean,
		RaskOverlapNs:                  rOvMean,
		ExactMatch:                     exact,
		TradeOffNote:                   note,
	}, nil
}

func benchPointLookups(fn func(uint64) bool, offs []uint64) (meanNs, p50Ns, p99Ns float64) {
	if len(offs) == 0 {
		return 0, 0, 0
	}
	const samples = 500
	const batch = 64
	perSample := make([]float64, samples)
	var sum uint64
	for i := range min(len(offs), 1024) {
		if fn(offs[i]) {
			sum++
		}
	}
	tTotal := time.Now()
	idx := 0
	for s := range samples {
		t0 := time.Now()
		for range batch {
			if fn(offs[idx]) {
				sum++
			}
			idx++
			if idx == len(offs) {
				idx = 0
			}
		}
		perSample[s] = float64(time.Since(t0).Nanoseconds()) / float64(batch)
	}
	totalNs := float64(time.Since(tTotal).Nanoseconds())
	raskBenchSink ^= sum
	slices.Sort(perSample)
	meanNs = totalNs / float64(samples*batch)
	p50Ns = perSample[samples/2]
	p99Ns = perSample[(samples*99)/100]
	return meanNs, p50Ns, p99Ns
}

func benchRangeLookups(fn func(uint64, uint64) bool, ranges [][2]uint64) float64 {
	if len(ranges) == 0 {
		return 0
	}
	const iters = 32000
	var sum uint64
	t0 := time.Now()
	idx := 0
	for range iters {
		r := ranges[idx]
		if fn(r[0], r[1]) {
			sum++
		}
		idx++
		if idx == len(ranges) {
			idx = 0
		}
	}
	ns := float64(time.Since(t0).Nanoseconds()) / float64(iters)
	raskBenchSink ^= sum
	return ns
}

func genClusteredSeqOps(n int) ([]raskOp, []uint64, [][2]uint64) {
	const clusters = 200
	const clusterGap = uint64(5) << 40
	ops := make([]raskOp, n)
	perCluster := max(1, n/clusters)
	qOffs := make([]uint64, 0, 4096)
	qRanges := make([][2]uint64, 0, 4096)
	for i := range n {
		c := uint64(i / perCluster)
		j := uint64(i % perCluster)
		a := c*clusterGap + j*16384
		b := a + 4096
		ops[i] = raskOp{kind: raskOpInsert, a: a, b: b}
		if len(qOffs) < 4096 {
			qOffs = append(qOffs, a+100, b+100)
			qRanges = append(qRanges, [2]uint64{a + 512, b + 512})
		}
	}
	return ops, qOffs, qRanges
}

func genClusteredRandPunchOps(n int) ([]raskOp, []uint64, [][2]uint64) {
	const clusters = 200
	const clusterGap = uint64(5) << 40
	perm := make([]int, n)
	for i := range n {
		perm[i] = i
	}
	seed := uint64(0x9E3779B97F4A7C15)
	nextRand := func() uint64 {
		seed += 0x9E3779B97F4A7C15
		z := seed
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}
	for i := n - 1; i > 0; i-- {
		j := int(nextRand() % uint64(i+1))
		perm[i], perm[j] = perm[j], perm[i]
	}

	perCluster := max(1, n/clusters)
	ops := make([]raskOp, 0, n+n/5)
	qOffs := make([]uint64, 0, 4096)
	qRanges := make([][2]uint64, 0, 4096)
	for idx, p := range perm {
		c := uint64(p / perCluster)
		j := uint64(p % perCluster)
		a := c*clusterGap + j*32768
		b := a + 16384
		ops = append(ops, raskOp{kind: raskOpInsert, a: a, b: b})
		if idx%5 == 4 {
			ops = append(ops, raskOp{kind: raskOpPunch, a: a + 4096, b: a + 8192})
		}
		if len(qOffs) < 4096 {
			qOffs = append(qOffs, a+1000, a+6000)
			qRanges = append(qRanges, [2]uint64{a + 2000, a + 10000})
		}
	}
	return ops, qOffs, qRanges
}

func genStridedOps(n int, stride, length uint64) ([]raskOp, []uint64, [][2]uint64) {
	ops := make([]raskOp, n)
	qOffs := make([]uint64, 0, min(4096, n*2))
	qRanges := make([][2]uint64, 0, min(4096, n))
	for i := range n {
		a := uint64(i) * stride
		b := a + length
		ops[i] = raskOp{kind: raskOpInsert, a: a, b: b}
		if len(qOffs) < 4096 {
			qOffs = append(qOffs, a+length/2, b+1)
			qRanges = append(qRanges, [2]uint64{a, b + 64})
		}
	}
	return ops, qOffs, qRanges
}
