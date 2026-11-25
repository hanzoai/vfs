// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package sysspec expresses the storage invariants of vfs (journal, fencing,
// corruption and publication) as machine-checkable three-layer specifications
// following the SYSSPEC architecture (Liu et al., USENIX FAST 2026, HIP-1349).
//
// Each module specification separates three concerns:
//
//  1. Functionality: state invariants and step pre/post-conditions over
//     observable storage state.
//  2. Modularity: explicit Rely/Guarantee contracts along the dependency DAG
//     (journal -> publication -> fence -> corruption) so localized mutations
//     or protocol changes can be checked across module boundaries.
//  3. Concurrency: group-sync barrier epochs, replay/reclaim exclusion, flush
//     serialization, and single-CAS round admission without an admit/write gap.
//
// The package uses only the Go standard library and builds without CGO on all
// targets gated by hanzo.yml.
package sysspec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
)

// Domain identifies one of the four core storage specification modules.
type Domain string

const (
	DomainCorruption  Domain = "corruption"
	DomainFence       Domain = "fence"
	DomainPublication Domain = "publication"
	DomainJournal     Domain = "journal"
)

// Layer names one of the three SYSSPEC specification layers.
type Layer string

const (
	LayerFunctionality Layer = "functionality"
	LayerModularity    Layer = "modularity"
	LayerConcurrency   Layer = "concurrency"
)

// MaxHeadBytes is the hard 65,536-byte ceiling enforced on Head reads and
// writes by vfs and pkg/fence.
const MaxHeadBytes = 64 << 10

// MaxTailFields bounds the number of TLV entries permitted in a Head tail.
const MaxTailFields = 256

// DefaultMarkLagRounds is the journal's default spacing between durable
// through-marks (every 16 sync rounds or 1 MiB synced).
const DefaultMarkLagRounds = 16

// JournalState captures the observable state of a local NVMe journal instance
// and its segment metadata relevant to recovery and publication.
type JournalState struct {
	Open             bool
	Failed           bool
	LineageVolumeID  [16]byte
	LineageRound     uint64
	Visible          uint64
	Durable          uint64
	Published        uint64
	Proven           uint64 // highest sequence covered by a surviving through-mark
	Floor            uint64 // highest store sequence recorded in the oldest kept segment
	FirstKept        uint64 // 0 when the log holds no unpublished records
	LastKept         uint64
	ActiveSyncEpoch  uint64 // monotonic counter of started sync flights
	CompletedSync    uint64 // highest completed sync flight
	RecordWriteEpoch map[uint64]uint64
	OldestAckUnixNs  int64
	DeadlineAnchorNs int64
	ActiveReplays    int
	Reclaiming       bool
	ForeignSegment   bool
	CapacityBytes    int64
	UsedBytes        int64
	BulkDurableSeq   uint64 // highest durable sequence on the side bulk spool
}

// FenceRecord represents the stored fenced object for a single key.
type FenceRecord struct {
	Exists  bool
	Version uint64
	Round   uint64
	Token   [16]byte
	Payload []byte
	Size    int
}

// FenceWriter models a single attached or deposed writer handle.
type FenceWriter struct {
	ID     string
	Round  uint64
	Token  [16]byte
	Fenced bool
	LastOp []byte
}

// FenceState captures the store's fenced Head record and known writers.
type FenceState struct {
	Head    FenceRecord
	MaxSize int
	Writers map[string]FenceWriter
}

// TailField represents one decoded [tag][length][value] field in Head.Tail.
type TailField struct {
	Tag       uint64
	Length    uint64
	ActualLen uint64
}

// ObjectSealCheck captures a concrete object sealing/parsing observation for
// corruption and cryptographic domain-separation verification.
type ObjectSealCheck struct {
	Kind               string // "head", "shard", "chunk", "ext"
	ExpectedPlace      string
	ActualPlace        string
	ExpectedVolumeID   [16]byte
	ActualVolumeID     [16]byte
	ExpectedChunkID    [32]byte
	ActualChunkID      [32]byte
	CipherReturnedRaw  bool
	TagBytesVerified   int // must be 32 when sealed
	NonMinimalVarint   bool
	KnownNeeds         uint64
	HeadNeeds          uint64
	TailFields         []TailField
	ObservedErrorKind  string // "", "ErrNeeds", "ErrCorrupt", "ErrContext", "ErrTooLarge", "ErrBadCipher"
	StoreMutatedOnFail bool
	PackLength         int64
	FurthestChunkEnd   int64
	MissingChunkInTbl  bool
}

// CommitHead models the logical fields of a Volume's Head payload.
type CommitHead struct {
	Seq     uint64
	Root    [32]byte
	Next    uint64
	ID      [16]byte
	Seed    [32]byte
	Journal uint64
	Needs   uint64
	Tail    string
}

// WriteStage records one store write within a Flush attempt.
type WriteStage struct {
	Kind string // "pack", "shard", "head"
	Key  string
}

// PublicationState captures the Volume's commit, orphan, and watermark state.
type PublicationState struct {
	Committed         CommitHead
	WorkingRoot       [32]byte
	WorkingNext       uint64
	PendingOverlay    int
	OpenPackEmpty     bool
	LastAttemptedSeq  uint64
	CallerSeenSeq     uint64
	UsedSeqs          map[uint64]bool
	FlushStages       []WriteStage
	UploadedPacks     map[string]bool
	UploadedShards    map[string]bool
	ReferencedPacks   map[string]bool
	ReferencedShards  map[string]bool
	FlushHeldBy       string
	ConcurrentReaders int
}

// SystemState is the composite state across all four storage specification
// domains at a discrete point in an execution trace.
type SystemState struct {
	Journal     JournalState
	Fence       FenceState
	Corruption  *ObjectSealCheck
	Publication PublicationState
}

// Clone returns a deep copy of s so transitions can compare pre and post states
// without aliasing maps or slices.
func (s SystemState) Clone() SystemState {
	out := s
	if s.Journal.RecordWriteEpoch != nil {
		out.Journal.RecordWriteEpoch = make(map[uint64]uint64, len(s.Journal.RecordWriteEpoch))
		for k, v := range s.Journal.RecordWriteEpoch {
			out.Journal.RecordWriteEpoch[k] = v
		}
	}
	if s.Fence.Head.Payload != nil {
		out.Fence.Head.Payload = bytes.Clone(s.Fence.Head.Payload)
	}
	if s.Fence.Writers != nil {
		out.Fence.Writers = make(map[string]FenceWriter, len(s.Fence.Writers))
		for k, w := range s.Fence.Writers {
			wc := w
			if w.LastOp != nil {
				wc.LastOp = bytes.Clone(w.LastOp)
			}
			out.Fence.Writers[k] = wc
		}
	}
	if s.Corruption != nil {
		cc := *s.Corruption
		if s.Corruption.TailFields != nil {
			cc.TailFields = slices.Clone(s.Corruption.TailFields)
		}
		out.Corruption = &cc
	}
	if s.Publication.UsedSeqs != nil {
		out.Publication.UsedSeqs = make(map[uint64]bool, len(s.Publication.UsedSeqs))
		for k, v := range s.Publication.UsedSeqs {
			out.Publication.UsedSeqs[k] = v
		}
	}
	if s.Publication.FlushStages != nil {
		out.Publication.FlushStages = slices.Clone(s.Publication.FlushStages)
	}
	if s.Publication.UploadedPacks != nil {
		out.Publication.UploadedPacks = copyBoolMap(s.Publication.UploadedPacks)
	}
	if s.Publication.UploadedShards != nil {
		out.Publication.UploadedShards = copyBoolMap(s.Publication.UploadedShards)
	}
	if s.Publication.ReferencedPacks != nil {
		out.Publication.ReferencedPacks = copyBoolMap(s.Publication.ReferencedPacks)
	}
	if s.Publication.ReferencedShards != nil {
		out.Publication.ReferencedShards = copyBoolMap(s.Publication.ReferencedShards)
	}
	return out
}

func copyBoolMap(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ActionKind names a storage transition observed in a trace.
type ActionKind string

const (
	ActionJournalOpen      ActionKind = "journal.Open"
	ActionJournalAppend    ActionKind = "journal.Append"
	ActionJournalSync      ActionKind = "journal.Sync"
	ActionJournalPublish   ActionKind = "journal.Publish"
	ActionJournalReclaim   ActionKind = "journal.Reclaim"
	ActionJournalReplay    ActionKind = "journal.Replay"
	ActionJournalCrashOpen ActionKind = "journal.CrashOpen"

	ActionFenceCreate  ActionKind = "fence.Create"
	ActionFenceClaim   ActionKind = "fence.Claim"
	ActionFencePut     ActionKind = "fence.Put"
	ActionFenceRewrite ActionKind = "fence.Rewrite"

	ActionObjectDecode ActionKind = "corruption.Decode"
	ActionVolumeVerify ActionKind = "corruption.Verify"

	ActionVolumeFlush    ActionKind = "publication.Flush"
	ActionVolumeTakeover ActionKind = "publication.Takeover"
	ActionVolumeReseal   ActionKind = "publication.Reseal"
)

// Action describes a single transition between Pre and Post states, along with
// its return status and concurrency metadata.
type Action struct {
	Kind               ActionKind
	Actor              string
	Round              uint64
	Seq                uint64
	JournalTarget      uint64
	SyncFlightUsed     uint64
	OpenPublishedArg   uint64
	RetriedWithZero    bool
	BulkRefSeq         uint64
	ResealVerifiedRoot bool
	CleanNoOpFlush     bool
	LostAckLanded      bool
	AdmitWriteSplit    bool // true if round check and write were split across two store ops
	UnkilledMediaFault bool // true when disk sector bitrot hit after SIGKILL
	ReturnedErr        string
}

// Step pairs an Action with the state before and after it ran.
type Step struct {
	Index  int
	Action Action
	Pre    SystemState
	Post   SystemState
}

// Trace is an ordered sequence of storage steps.
type Trace struct {
	Name  string
	Steps []Step
}

// Violation reports a machine-checked specification failure.
type Violation struct {
	RuleID    string
	Domain    Domain
	Layer     Layer
	StepIndex int
	Detail    string
}

func (v Violation) Error() string {
	return fmt.Sprintf("[%s/%s] %s at step %d: %s", v.Domain, v.Layer, v.RuleID, v.StepIndex, v.Detail)
}

// Invariant checks a single state snapshot in isolation.
type Invariant struct {
	ID          string
	Domain      Domain
	Description string
	Check       func(s SystemState) string
}

// TransitionRule checks a state transition (Pre, Action, Post) within a module.
type TransitionRule struct {
	ID          string
	Domain      Domain
	Description string
	Applies     func(a Action) bool
	Check       func(pre SystemState, a Action, post SystemState) string
}

// RelyGuarantee expresses a modular boundary contract from Provider to Consumer
// in the SYSSPEC dependency DAG.
type RelyGuarantee struct {
	ID        string
	Provider  Domain
	Consumer  Domain
	Rely      string
	Guarantee string
	Check     func(pre SystemState, a Action, post SystemState) string
}

// ConcurrencyRule checks synchronization, barrier, and epoch ordering on a step.
type ConcurrencyRule struct {
	ID          string
	Domain      Domain
	Description string
	Check       func(pre SystemState, a Action, post SystemState) string
}

// ModuleSpec bundles the three SYSSPEC layers for one storage domain.
type ModuleSpec struct {
	Domain      Domain
	Summary     string
	DependsOn   []Domain
	Invariants  []Invariant
	Transitions []TransitionRule
	Concurrency []ConcurrencyRule
}

// Registry holds the complete four-domain SYSSPEC DAG for vfs.
type Registry struct {
	Modules map[Domain]ModuleSpec
	Order   []Domain
	RGs     []RelyGuarantee
}

// CanonicalContext computes the exact 32-byte length-framed BLAKE3 domain
// separation input structure (represented deterministically by canonical framing
// bytes for structural comparison) so the spec can check injectivity without
// duplicating internal hash state.
func CanonicalContextFrame(kind string, fields ...[]byte) []byte {
	var out []byte
	out = append(out, "hanzoai/vfs seal context"...)
	frame := func(p []byte) {
		out = binary.AppendUvarint(out, uint64(len(p)))
		out = append(out, p...)
	}
	frame([]byte(kind))
	for _, f := range fields {
		frame(f)
	}
	return out
}

// DefaultRegistry constructs the complete SYSSPEC specification DAG for
// hanzoai/vfs across corruption, fence, publication, and journal.
func DefaultRegistry() *Registry {
	mods := map[Domain]ModuleSpec{
		DomainCorruption:  corruptionModule(),
		DomainFence:       fenceModule(),
		DomainPublication: publicationModule(),
		DomainJournal:     journalModule(),
	}
	return &Registry{
		Modules: mods,
		Order:   []Domain{DomainCorruption, DomainFence, DomainPublication, DomainJournal},
		RGs:     relyGuaranteeContracts(),
	}
}

// TopologicalOrder verifies that the module dependency graph is acyclic and
// returns a deterministic bottom-up evaluation order.
func (r *Registry) TopologicalOrder() ([]Domain, error) {
	inDeg := make(map[Domain]int, len(r.Modules))
	dependents := make(map[Domain][]Domain, len(r.Modules))
	for d := range r.Modules {
		inDeg[d] = 0
	}
	for d, m := range r.Modules {
		for _, dep := range m.DependsOn {
			if _, ok := r.Modules[dep]; !ok {
				return nil, fmt.Errorf("sysspec: module %s depends on unknown %s", d, dep)
			}
			inDeg[d]++
			dependents[dep] = append(dependents[dep], d)
		}
	}
	var ready []string
	for d, deg := range inDeg {
		if deg == 0 {
			ready = append(ready, string(d))
		}
	}
	sort.Strings(ready)
	var order []Domain
	for len(ready) > 0 {
		cur := Domain(ready[0])
		ready = ready[1:]
		order = append(order, cur)
		var nextReady []string
		for _, nxt := range dependents[cur] {
			inDeg[nxt]--
			if inDeg[nxt] == 0 {
				nextReady = append(nextReady, string(nxt))
			}
		}
		if len(nextReady) > 0 {
			ready = append(ready, nextReady...)
			sort.Strings(ready)
		}
	}
	if len(order) != len(r.Modules) {
		return nil, fmt.Errorf("sysspec: cycle in specification DAG (%d of %d scheduled)", len(order), len(r.Modules))
	}
	return order, nil
}

// AffectedModules returns all modules transitively impacted when root changes,
// implementing SYSSPEC's DAG-structured specification evolution rule.
func (r *Registry) AffectedModules(changed Domain) []Domain {
	seen := map[Domain]bool{changed: true}
	queue := []Domain{changed}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for d, m := range r.Modules {
			if seen[d] {
				continue
			}
			if slices.Contains(m.DependsOn, cur) {
				seen[d] = true
				queue = append(queue, d)
			}
		}
	}
	var out []Domain
	for _, d := range r.Order {
		if seen[d] {
			out = append(out, d)
		}
	}
	return out
}

// RuleCounts returns the number of invariants, transition rules, Rely/Guarantee
// contracts, and concurrency rules registered in r.
func (r *Registry) RuleCounts() (invariants, transitions, relyGuarantees, concurrency int) {
	for _, d := range r.Order {
		m := r.Modules[d]
		invariants += len(m.Invariants)
		transitions += len(m.Transitions)
		concurrency += len(m.Concurrency)
	}
	relyGuarantees = len(r.RGs)
	return
}

// CheckState evaluates all state invariants across all modules in DAG order.
func (r *Registry) CheckState(stepIdx int, s SystemState) []Violation {
	var vs []Violation
	for _, d := range r.Order {
		m := r.Modules[d]
		for _, inv := range m.Invariants {
			if detail := inv.Check(s); detail != "" {
				vs = append(vs, Violation{
					RuleID:    inv.ID,
					Domain:    d,
					Layer:     LayerFunctionality,
					StepIndex: stepIdx,
					Detail:    detail,
				})
			}
		}
	}
	return vs
}

// CheckStep evaluates state invariants on Post, functional transition rules,
// modular Rely/Guarantee contracts, and concurrency rules on a single Step.
func (r *Registry) CheckStep(st Step) []Violation {
	var vs []Violation
	for _, d := range r.Order {
		m := r.Modules[d]
		for _, tr := range m.Transitions {
			if tr.Applies == nil || tr.Applies(st.Action) {
				if detail := tr.Check(st.Pre, st.Action, st.Post); detail != "" {
					vs = append(vs, Violation{
						RuleID:    tr.ID,
						Domain:    d,
						Layer:     LayerFunctionality,
						StepIndex: st.Index,
						Detail:    detail,
					})
				}
			}
		}
		for _, cc := range m.Concurrency {
			if detail := cc.Check(st.Pre, st.Action, st.Post); detail != "" {
				vs = append(vs, Violation{
					RuleID:    cc.ID,
					Domain:    d,
					Layer:     LayerConcurrency,
					StepIndex: st.Index,
					Detail:    detail,
				})
			}
		}
	}
	for _, rg := range r.RGs {
		if detail := rg.Check(st.Pre, st.Action, st.Post); detail != "" {
			vs = append(vs, Violation{
				RuleID:    rg.ID,
				Domain:    rg.Consumer,
				Layer:     LayerModularity,
				StepIndex: st.Index,
				Detail:    detail,
			})
		}
	}
	vs = append(vs, r.CheckState(st.Index, st.Post)...)
	return vs
}

// CheckTrace checks the initial Pre state (if Steps is non-empty) and every
// step in tr, returning all violations in deterministic order.
func (r *Registry) CheckTrace(tr Trace) []Violation {
	if len(tr.Steps) == 0 {
		return nil
	}
	var vs []Violation
	vs = append(vs, r.CheckState(-1, tr.Steps[0].Pre)...)
	for i := range tr.Steps {
		st := tr.Steps[i]
		st.Index = i
		vs = append(vs, r.CheckStep(st)...)
	}
	return vs
}

func corruptionModule() ModuleSpec {
	return ModuleSpec{
		Domain:  DomainCorruption,
		Summary: "Cryptographic sealing contexts, canonical varints, Head Needs/Tail fail-closed parsing, and pack/chunk integrity.",
		Invariants: []Invariant{
			{
				ID:          "C1_InjectiveSealingContext",
				Domain:      DomainCorruption,
				Description: "Every opened object matches its exact length-framed kind/place/volume/chunk sealing context.",
				Check: func(s SystemState) string {
					c := s.Corruption
					if c == nil || c.ObservedErrorKind != "" {
						return ""
					}
					var exp, got []byte
					switch c.Kind {
					case "head":
						exp = CanonicalContextFrame("head", []byte(c.ExpectedPlace))
						got = CanonicalContextFrame("head", []byte(c.ActualPlace))
					case "shard":
						exp = CanonicalContextFrame("shard", c.ExpectedVolumeID[:])
						got = CanonicalContextFrame("shard", c.ActualVolumeID[:])
					case "chunk":
						exp = CanonicalContextFrame("chunk", c.ExpectedVolumeID[:], c.ExpectedChunkID[:])
						got = CanonicalContextFrame("chunk", c.ActualVolumeID[:], c.ActualChunkID[:])
					default:
						return ""
					}
					if !bytes.Equal(exp, got) {
						return fmt.Sprintf("object kind %q opened across mismatched sealing context", c.Kind)
					}
					return ""
				},
			},
			{
				ID:          "C2_CanonicalShortestVarint",
				Domain:      DomainCorruption,
				Description: "Non-minimal varint encodings are rejected as ErrCorrupt and never accepted.",
				Check: func(s SystemState) string {
					c := s.Corruption
					if c != nil && c.NonMinimalVarint && c.ObservedErrorKind != "ErrCorrupt" {
						return fmt.Sprintf("non-minimal varint accepted or misclassified as %q (want ErrCorrupt)", c.ObservedErrorKind)
					}
					return ""
				},
			},
			{
				ID:          "C3_NeedsCheckedBeforeTail",
				Domain:      DomainCorruption,
				Description: "Unknown Head.Needs bits fail closed with ErrNeeds before inspecting Tail fields.",
				Check: func(s SystemState) string {
					c := s.Corruption
					if c == nil || c.Kind != "head" || c.NonMinimalVarint {
						return ""
					}
					if unknown := c.HeadNeeds &^ c.KnownNeeds; unknown != 0 {
						if c.ObservedErrorKind != "ErrNeeds" {
							return fmt.Sprintf("Head with unknown Needs %#x returned %q, want ErrNeeds", unknown, c.ObservedErrorKind)
						}
					}
					return ""
				},
			},
			{
				ID:          "C4_HeadTailStrictAscendingBounded",
				Domain:      DomainCorruption,
				Description: "Head.Tail has <= 256 whole fields with strictly increasing tags and carries every known Needs bit.",
				Check: func(s SystemState) string {
					c := s.Corruption
					if c == nil || c.Kind != "head" || c.NonMinimalVarint || (c.HeadNeeds&^c.KnownNeeds) != 0 {
						return ""
					}
					bad := false
					if len(c.TailFields) > MaxTailFields {
						bad = true
					}
					var prev uint64
					var seen uint64
					for i, f := range c.TailFields {
						if f.ActualLen != f.Length {
							bad = true
						}
						if i > 0 && f.Tag <= prev {
							bad = true
						}
						prev = f.Tag
						if f.Tag < 64 {
							seen |= 1 << f.Tag
						}
					}
					if (c.HeadNeeds &^ seen) != 0 {
						bad = true
					}
					if bad && c.ObservedErrorKind != "ErrCorrupt" {
						return fmt.Sprintf("malformed Head.Tail returned %q, want ErrCorrupt", c.ObservedErrorKind)
					}
					return ""
				},
			},
			{
				ID:          "C5_HeadSizeWithinFenceCeiling",
				Domain:      DomainCorruption,
				Description: "Stored fenced Head never exceeds 65,536 bytes and oversized writes leave the store untouched.",
				Check: func(s SystemState) string {
					maxSz := s.Fence.MaxSize
					if maxSz <= 0 {
						maxSz = MaxHeadBytes
					}
					if s.Fence.Head.Exists && s.Fence.Head.Size > maxSz {
						return fmt.Sprintf("stored fenced Head size %d exceeds ceiling %d", s.Fence.Head.Size, maxSz)
					}
					if s.Corruption != nil && s.Corruption.ObservedErrorKind == "ErrTooLarge" && s.Corruption.StoreMutatedOnFail {
						return "oversized Head write mutated store before returning ErrTooLarge"
					}
					return ""
				},
			},
			{
				ID:          "C6_CipherRejectsPlaintextOrPartialTag",
				Domain:      DomainCorruption,
				Description: "Cipher output never equals plaintext and authentication compares all 32 tag bytes.",
				Check: func(s SystemState) string {
					c := s.Corruption
					if c == nil || c.ObservedErrorKind != "" {
						return ""
					}
					if c.CipherReturnedRaw {
						return "cipher returned unsealed plaintext without failing closed"
					}
					if c.TagBytesVerified > 0 && c.TagBytesVerified < 32 {
						return fmt.Sprintf("tag verification checked %d of 32 bytes", c.TagBytesVerified)
					}
					return ""
				},
			},
			{
				ID:          "C7_ExtentChunkAndPackBounds",
				Domain:      DomainCorruption,
				Description: "Full tree verification rejects missing chunk-table entries and packs truncated before their furthest chunk.",
				Check: func(s SystemState) string {
					c := s.Corruption
					if c == nil || c.ObservedErrorKind != "" {
						return ""
					}
					if c.MissingChunkInTbl {
						return "extent missing its chunk-table entry passed verification"
					}
					if c.FurthestChunkEnd > 0 && c.PackLength < c.FurthestChunkEnd {
						return fmt.Sprintf("pack length %d < furthest chunk end %d passed verification", c.PackLength, c.FurthestChunkEnd)
					}
					return ""
				},
			},
		},
		Transitions: []TransitionRule{
			{
				ID:          "C8_JournalProvenDamageFailsClosed",
				Domain:      DomainCorruption,
				Description: "Damage at or below a surviving through-mark (Proven) that is above Published must fail Open as ErrCorrupt.",
				Applies:     func(a Action) bool { return a.Kind == ActionJournalCrashOpen },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if pre.Journal.Proven > pre.Journal.Published && post.Journal.Open && post.Journal.Visible < pre.Journal.Proven {
						return fmt.Sprintf("journal recovered Visible=%d below surviving Proven=%d (Published=%d) instead of ErrCorrupt",
							post.Journal.Visible, pre.Journal.Proven, pre.Journal.Published)
					}
					return ""
				},
			},
		},
	}
}

func fenceModule() ModuleSpec {
	return ModuleSpec{
		Domain:    DomainFence,
		Summary:   "Single-object monotone round fence (pkg/fence) for Head and SQLite snapshots.",
		DependsOn: []Domain{DomainCorruption},
		Invariants: []Invariant{
			{
				ID:          "F1_RoundBoundsValid",
				Domain:      DomainFence,
				Description: "Attached writers hold a round strictly in (0, 2^64-1).",
				Check: func(s SystemState) string {
					for id, w := range s.Fence.Writers {
						if w.Round == 0 || w.Round == math.MaxUint64 {
							return fmt.Sprintf("writer %s attached with invalid round %d", id, w.Round)
						}
					}
					if s.Fence.Head.Exists && s.Fence.Head.Round == math.MaxUint64 {
						return "fenced Head recorded terminal round 2^64-1"
					}
					return ""
				},
			},
		},
		Transitions: []TransitionRule{
			{
				ID:          "F2_PutRequiresAtLeastRecordedRound",
				Domain:      DomainFence,
				Description: "fence.Put admits round >= recorded and rejects round < recorded with ErrStaleRound without mutating Head.",
				Applies:     func(a Action) bool { return a.Kind == ActionFencePut },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if pre.Fence.Head.Exists && a.Round < pre.Fence.Head.Round {
						if a.ReturnedErr == "" {
							return fmt.Sprintf("fence.Put admitted stale round %d < recorded %d", a.Round, pre.Fence.Head.Round)
						}
						if post.Fence.Head.Version != pre.Fence.Head.Version || !bytes.Equal(post.Fence.Head.Payload, pre.Fence.Head.Payload) {
							return "rejected stale fence.Put mutated stored Head"
						}
					}
					if a.ReturnedErr == "" && post.Fence.Head.Round < pre.Fence.Head.Round {
						return fmt.Sprintf("recorded round regressed from %d to %d", pre.Fence.Head.Round, post.Fence.Head.Round)
					}
					return ""
				},
			},
			{
				ID:          "F3_ClaimRequiresStrictlyHigherRound",
				Domain:      DomainFence,
				Description: "Attach/Reseal Claim requires round > recorded and 0 < round < 2^64-1 unless recognising its own lost-ack record.",
				Applies: func(a Action) bool {
					return a.Kind == ActionFenceClaim || a.Kind == ActionFenceRewrite ||
						a.Kind == ActionVolumeTakeover || a.Kind == ActionVolumeReseal
				},
				Check: func(pre SystemState, a Action, post SystemState) string {
					if a.ReturnedErr != "" {
						return ""
					}
					if a.Round == 0 || a.Round == math.MaxUint64 {
						return fmt.Sprintf("claim succeeded at invalid boundary round %d", a.Round)
					}
					if pre.Fence.Head.Exists && a.Round <= pre.Fence.Head.Round && !a.LostAckLanded {
						return fmt.Sprintf("claim succeeded at non-advancing round %d (recorded %d)", a.Round, pre.Fence.Head.Round)
					}
					return ""
				},
			},
			{
				ID:          "F4_TakeoverPreservesLatestPayload",
				Domain:      DomainFence,
				Description: "fence.Claim carries forward the predecessor's exact latest committed payload without clobbering it.",
				Applies:     func(a Action) bool { return a.Kind == ActionFenceClaim && a.ReturnedErr == "" },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if pre.Fence.Head.Exists && !bytes.Equal(pre.Fence.Head.Payload, post.Fence.Head.Payload) {
						return "fence.Claim altered predecessor payload bytes during takeover"
					}
					return ""
				},
			},
			{
				ID:          "F5_DeposedWriterPermanentlyFenced",
				Domain:      DomainFence,
				Description: "Once a writer observes ErrStaleRound (fenced=true), no subsequent Put or Flush by that writer may succeed.",
				Applies: func(a Action) bool {
					return a.Kind == ActionFencePut || a.Kind == ActionVolumeFlush
				},
				Check: func(pre SystemState, a Action, post SystemState) string {
					w, ok := pre.Fence.Writers[a.Actor]
					if ok && w.Fenced && a.ReturnedErr == "" && !a.LostAckLanded {
						return fmt.Sprintf("already-fenced writer %s succeeded on %s", a.Actor, a.Kind)
					}
					return ""
				},
			},
			{
				ID:          "F6_LostAckTokenIdempotence",
				Domain:      DomainFence,
				Description: "Lost-ack success requires matching the writer's unique 16-byte token or verified landed state.",
				Applies: func(a Action) bool {
					return (a.Kind == ActionFencePut || a.Kind == ActionFenceClaim) && a.LostAckLanded && a.ReturnedErr == ""
				},
				Check: func(pre SystemState, a Action, post SystemState) string {
					w, ok := pre.Fence.Writers[a.Actor]
					if !ok {
						return "lost-ack recovery claimed by unknown writer"
					}
					if post.Fence.Head.Token != w.Token {
						return "lost-ack treated another writer's token as its own landed write"
					}
					return ""
				},
			},
		},
		Concurrency: []ConcurrencyRule{
			{
				ID:          "CC4_SingleConditionalWriteNoAdmitWindow",
				Domain:      DomainFence,
				Description: "Round admission and payload update happen in one atomic CAS, never split across separate check and write calls.",
				Check: func(pre SystemState, a Action, post SystemState) string {
					if a.AdmitWriteSplit {
						return fmt.Sprintf("%s split round check and object write across non-atomic calls", a.Kind)
					}
					return ""
				},
			},
		},
	}
}

// Known Head.Tail tags defined by SPECIFICATION 5.1 (namespace.go):
// tag 2 (links, announced by Needs bit 2) and tag 64 (usage counters stamped on commit).
// All other tags are unknown fields and must be preserved byte-for-byte across commits.
const (
	headTailTagLinks uint64 = 2
	headTailTagUsage uint64 = 64
)

func unknownHeadTail(tail string) string {
	b := []byte(tail)
	var out []byte
	for len(b) > 0 {
		start := b
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return tail
		}
		b = b[n:]
		ln, n := binary.Uvarint(b)
		if n <= 0 || ln > uint64(len(b[n:])) {
			return tail
		}
		b = b[n+int(ln):]
		field := start[:len(start)-len(b)]
		if tag != headTailTagLinks && tag != headTailTagUsage {
			out = append(out, field...)
		}
	}
	return string(out)
}

func publicationModule() ModuleSpec {
	return ModuleSpec{
		Domain:    DomainPublication,
		Summary:   "Volume commit ordering (packs -> shards -> conditional Head PUT), orphan safety, and Head evolution.",
		DependsOn: []Domain{DomainFence, DomainCorruption},
		Invariants: []Invariant{
			{
				ID:          "P2_OrphansNeverReferencedBeforeHeadCAS",
				Domain:      DomainPublication,
				Description: "Every shard and pack referenced by the committed Head must already exist in the store; unreferenced uploads are harmless orphans.",
				Check: func(s SystemState) string {
					for k := range s.Publication.ReferencedPacks {
						if !s.Publication.UploadedPacks[k] {
							return fmt.Sprintf("committed Head references unuploaded pack %s", k)
						}
					}
					for k := range s.Publication.ReferencedShards {
						if !s.Publication.UploadedShards[k] {
							return fmt.Sprintf("committed Head references unuploaded shard %s", k)
						}
					}
					return ""
				},
			},
		},
		Transitions: []TransitionRule{
			{
				ID:          "P1_WriteOrderPacksShardsBeforeHead",
				Domain:      DomainPublication,
				Description: "Within Flush, all pack uploads precede shard uploads, and all shard uploads precede the single Head conditional PUT.",
				Applies:     func(a Action) bool { return a.Kind == ActionVolumeFlush },
				Check: func(pre SystemState, a Action, post SystemState) string {
					phase := 0 // 0=pack, 1=shard, 2=head
					headWrites := 0
					for _, st := range post.Publication.FlushStages {
						var cur int
						switch st.Kind {
						case "pack":
							cur = 0
						case "shard":
							cur = 1
						case "head":
							cur = 2
							headWrites++
						default:
							return fmt.Sprintf("unknown flush write stage %q", st.Kind)
						}
						if cur < phase {
							return fmt.Sprintf("out-of-order Flush write: %s (%s) after phase %d", st.Kind, st.Key, phase)
						}
						phase = cur
					}
					if headWrites > 1 {
						return fmt.Sprintf("Flush issued %d Head writes, want at most 1 conditional PUT", headWrites)
					}
					return ""
				},
			},
			{
				ID:          "P3_HeadSeqStrictlyNonRepeating",
				Domain:      DomainPublication,
				Description: "Every non-noop Flush attempt advances Seq strictly without repeating a previously attempted Seq; callers tracking SeenSeq reject stale Heads.",
				Applies:     func(a Action) bool { return a.Kind == ActionVolumeFlush && !a.CleanNoOpFlush },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if a.Seq <= pre.Publication.LastAttemptedSeq && !a.LostAckLanded {
						return fmt.Sprintf("Flush reused or regressed Seq %d (previous attempted %d)", a.Seq, pre.Publication.LastAttemptedSeq)
					}
					if pre.Publication.UsedSeqs[a.Seq] && !a.LostAckLanded {
						return fmt.Sprintf("Flush repeated previously attempted Seq %d", a.Seq)
					}
					if a.ReturnedErr == "" && post.Publication.Committed.Seq < pre.Publication.CallerSeenSeq {
						return fmt.Sprintf("accepted stale store Head Seq %d below caller SeenSeq %d",
							post.Publication.Committed.Seq, pre.Publication.CallerSeenSeq)
					}
					return ""
				},
			},
			{
				ID:          "P4_HeadIdentityAndUnknownFieldsCarried",
				Domain:      DomainPublication,
				Description: "Flush, Takeover, and Reseal preserve Volume ID, Seed, Needs, Tail, and non-decreasing Next/Journal watermarks.",
				Applies: func(a Action) bool {
					return (a.Kind == ActionVolumeFlush || a.Kind == ActionVolumeTakeover || a.Kind == ActionVolumeReseal) && a.ReturnedErr == ""
				},
				Check: func(pre SystemState, a Action, post SystemState) string {
					b, c := pre.Publication.Committed, post.Publication.Committed
					if b.ID != ([16]byte{}) && c.ID != b.ID {
						return "commit/takeover altered Volume ID"
					}
					if b.Seed != ([32]byte{}) && c.Seed != b.Seed {
						return "commit/takeover altered Volume Seed"
					}
					if b.Needs&^c.Needs != 0 {
						return fmt.Sprintf("commit/takeover dropped Head.Needs (%#x -> %#x)", b.Needs, c.Needs)
					}
					if unknownHeadTail(c.Tail) != unknownHeadTail(b.Tail) {
						return "commit/takeover altered or dropped Head.Tail"
					}
					if c.Next < b.Next {
						return fmt.Sprintf("Head.Next regressed (%d -> %d)", b.Next, c.Next)
					}
					if c.Journal < b.Journal {
						return fmt.Sprintf("Head.Journal regressed (%d -> %d)", b.Journal, c.Journal)
					}
					return ""
				},
			},
			{
				ID:          "P5_CleanFlushMakesZeroStoreWrites",
				Domain:      DomainPublication,
				Description: "A clean Flush (overlay empty, open pack empty, working root == Head.Root, journal watermark unchanged) issues zero store requests.",
				Applies:     func(a Action) bool { return a.Kind == ActionVolumeFlush && a.CleanNoOpFlush },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if len(post.Publication.FlushStages) != 0 {
						return fmt.Sprintf("clean Flush issued %d store writes, want 0", len(post.Publication.FlushStages))
					}
					return ""
				},
			},
		},
		Concurrency: []ConcurrencyRule{
			{
				ID:          "CC3_FlushExclusiveHold",
				Domain:      DomainPublication,
				Description: "Flush holds the Volume lock exclusively across its sweep, spill, and store calls.",
				Check: func(pre SystemState, a Action, post SystemState) string {
					if a.Kind == ActionVolumeFlush && post.Publication.ConcurrentReaders > 0 && post.Publication.FlushHeldBy == "" {
						return "Flush executed store calls without exclusive Volume lock"
					}
					return ""
				},
			},
		},
	}
}

func journalModule() ModuleSpec {
	return ModuleSpec{
		Domain:    DomainJournal,
		Summary:   "Local NVMe write-ahead journal watermarks, crash-prefix recovery, group-sync barrier, and per-record aging.",
		DependsOn: []Domain{DomainPublication, DomainFence, DomainCorruption},
		Invariants: []Invariant{
			{
				ID:          "J1_MonotoneWatermarks",
				Domain:      DomainJournal,
				Description: "Journal maintains Published <= Durable <= Visible and Proven <= Durable at all times.",
				Check: func(s SystemState) string {
					j := s.Journal
					if !j.Open {
						return ""
					}
					if j.Published > j.Durable || j.Durable > j.Visible {
						return fmt.Sprintf("watermark order violated: Published=%d Durable=%d Visible=%d",
							j.Published, j.Durable, j.Visible)
					}
					if j.Proven > j.Durable {
						return fmt.Sprintf("Proven=%d exceeds Durable=%d", j.Proven, j.Durable)
					}
					if j.CapacityBytes > 0 && j.UsedBytes > j.CapacityBytes {
						return fmt.Sprintf("journal disk usage %d exceeds capacity %d", j.UsedBytes, j.CapacityBytes)
					}
					return ""
				},
			},
			{
				ID:          "J2_ContiguousSequenceNoGaps",
				Domain:      DomainJournal,
				Description: "Unpublished records kept in the journal form an exact contiguous suffix (Published+1 .. Visible).",
				Check: func(s SystemState) string {
					j := s.Journal
					if !j.Open || j.Visible == j.Published {
						return ""
					}
					if j.FirstKept != j.Published+1 || j.LastKept != j.Visible {
						return fmt.Sprintf("non-contiguous unpublished range [%d..%d] for Published=%d Visible=%d",
							j.FirstKept, j.LastKept, j.Published, j.Visible)
					}
					return ""
				},
			},
		},
		Transitions: []TransitionRule{
			{
				ID:          "J3_PublishRequiresDurable",
				Domain:      DomainJournal,
				Description: "journal.Publish(seq) refuses seq > Durable with ErrAhead and leaves watermarks unchanged.",
				Applies:     func(a Action) bool { return a.Kind == ActionJournalPublish },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if a.Seq > pre.Journal.Durable {
						if a.ReturnedErr == "" {
							return fmt.Sprintf("Publish(%d) succeeded above Durable=%d", a.Seq, pre.Journal.Durable)
						}
						if post.Journal.Published != pre.Journal.Published {
							return "failed Publish ahead of Durable mutated Published watermark"
						}
					}
					if a.ReturnedErr == "" && post.Journal.Published < pre.Journal.Published {
						return fmt.Sprintf("Published regressed from %d to %d", pre.Journal.Published, post.Journal.Published)
					}
					return ""
				},
			},
			{
				ID:          "J4_ReclaimPreservesUnpublishedAndFloor",
				Domain:      DomainJournal,
				Description: "Reclaim never drops records above Published and records Floor >= Published durably before wiping reclaimed segments.",
				Applies:     func(a Action) bool { return a.Kind == ActionJournalReclaim && a.ReturnedErr == "" },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if post.Journal.Visible > post.Journal.Published && post.Journal.FirstKept > post.Journal.Published+1 {
						return fmt.Sprintf("Reclaim dropped unpublished records (%d..%d missing)",
							post.Journal.Published+1, post.Journal.FirstKept-1)
					}
					if post.Journal.Floor > post.Journal.Published {
						return fmt.Sprintf("Reclaim wrote Floor=%d above Published=%d", post.Journal.Floor, post.Journal.Published)
					}
					if pre.Journal.Published > pre.Journal.Floor && post.Journal.Floor < pre.Journal.Published {
						return fmt.Sprintf("Reclaim wiped segments without advancing durable Floor to %d (got %d)",
							pre.Journal.Published, post.Journal.Floor)
					}
					return ""
				},
			},
			{
				ID:          "J5_OpenRefusesLineageFormatAndFloorMismatch",
				Domain:      DomainJournal,
				Description: "journal.Open refuses foreign layouts, mismatched lineages, store < Floor, or store > LastKept on non-empty logs without mutating files.",
				Applies: func(a Action) bool {
					return a.Kind == ActionJournalOpen || a.Kind == ActionJournalCrashOpen
				},
				Check: func(pre SystemState, a Action, post SystemState) string {
					if pre.Journal.ForeignSegment && post.Journal.Open {
						return "Open accepted directory containing a foreign segment layout"
					}
					if a.OpenPublishedArg < pre.Journal.Floor && post.Journal.Open {
						return fmt.Sprintf("Open accepted store Published=%d below segment Floor=%d",
							a.OpenPublishedArg, pre.Journal.Floor)
					}
					if pre.Journal.LastKept > 0 && a.OpenPublishedArg > pre.Journal.LastKept && post.Journal.Open {
						return fmt.Sprintf("Open accepted store Published=%d ahead of non-empty log LastKept=%d",
							a.OpenPublishedArg, pre.Journal.LastKept)
					}
					return ""
				},
			},
			{
				ID:          "J6_AppendMonotoneSequence",
				Domain:      DomainJournal,
				Description: "Each admitted Append assigns Visible = previous + 1 without gaps or reuse; failed Appends leave Visible unchanged.",
				Applies:     func(a Action) bool { return a.Kind == ActionJournalAppend },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if a.ReturnedErr != "" {
						if post.Journal.Visible != pre.Journal.Visible {
							return fmt.Sprintf("failed Append mutated Visible (%d -> %d)", pre.Journal.Visible, post.Journal.Visible)
						}
						return ""
					}
					want := pre.Journal.Visible + 1
					if a.Seq != want || post.Journal.Visible != want {
						return fmt.Sprintf("Append assigned seq=%d post.Visible=%d, want %d", a.Seq, post.Journal.Visible, want)
					}
					return ""
				},
			},
			{
				ID:          "J7_DeadlineAnchoredToOldestUnpublished",
				Domain:      DomainJournal,
				Description: "Later Append calls never push forward the Due deadline anchor of the oldest unpublished record.",
				Applies:     func(a Action) bool { return a.Kind == ActionJournalAppend && a.ReturnedErr == "" },
				Check: func(pre SystemState, a Action, post SystemState) string {
					if pre.Journal.Visible > pre.Journal.Published && post.Journal.DeadlineAnchorNs != pre.Journal.DeadlineAnchorNs {
						return fmt.Sprintf("Append shifted oldest unpublished deadline anchor (%d -> %d)",
							pre.Journal.DeadlineAnchorNs, post.Journal.DeadlineAnchorNs)
					}
					return ""
				},
			},
		},
		Concurrency: []ConcurrencyRule{
			{
				ID:          "CC1_SyncGroupBarrierEpoch",
				Domain:      DomainJournal,
				Description: "Sync(seq) is satisfied only by an fdatasync flight that started strictly after seq was written.",
				Check: func(pre SystemState, a Action, post SystemState) string {
					if a.Kind != ActionJournalSync || a.ReturnedErr != "" {
						return ""
					}
					writeEpoch, ok := pre.Journal.RecordWriteEpoch[a.Seq]
					if ok && a.SyncFlightUsed <= writeEpoch {
						return fmt.Sprintf("Sync(%d) released by in-flight sync epoch %d <= write epoch %d",
							a.Seq, a.SyncFlightUsed, writeEpoch)
					}
					return ""
				},
			},
			{
				ID:          "CC2_ReplayBlocksReclaim",
				Domain:      DomainJournal,
				Description: "Reclaim never modifies segments concurrently with an active Replay callback.",
				Check: func(pre SystemState, a Action, post SystemState) string {
					if a.Kind == ActionJournalReclaim && a.ReturnedErr == "" && pre.Journal.ActiveReplays > 0 {
						return fmt.Sprintf("Reclaim ran while %d Replay reader(s) were active", pre.Journal.ActiveReplays)
					}
					return ""
				},
			},
		},
	}
}

func relyGuaranteeContracts() []RelyGuarantee {
	return []RelyGuarantee{
		{
			ID:        "RG1_JournalToPublisherDurableGate",
			Provider:  DomainJournal,
			Consumer:  DomainPublication,
			Rely:      "Publisher commits Head.Journal = g only when journal.Marks().Durable >= g.",
			Guarantee: "Store never names a journal sequence that local NVMe media has not yet made durable.",
			Check: func(pre SystemState, a Action, post SystemState) string {
				if a.Kind == ActionVolumeFlush && a.ReturnedErr == "" && a.JournalTarget > 0 && pre.Journal.Open {
					if a.JournalTarget > pre.Journal.Durable {
						return fmt.Sprintf("Flush committed Head.Journal=%d above journal Durable=%d",
							a.JournalTarget, pre.Journal.Durable)
					}
				}
				return ""
			},
		},
		{
			ID:        "RG2_PublisherToJournalPostCommitAdvance",
			Provider:  DomainPublication,
			Consumer:  DomainJournal,
			Rely:      "Publisher calls journal.Publish(g) only after Head carrying Journal >= g is durably committed in the store.",
			Guarantee: "Unpublished journal records are never reclaimed before the store's Head includes them.",
			Check: func(pre SystemState, a Action, post SystemState) string {
				if a.Kind == ActionJournalPublish && a.ReturnedErr == "" {
					if post.Publication.Committed.ID != ([16]byte{}) && a.Seq > post.Publication.Committed.Journal {
						return fmt.Sprintf("journal.Publish(%d) advanced past committed Head.Journal=%d",
							a.Seq, post.Publication.Committed.Journal)
					}
				}
				return ""
			},
		},
		{
			ID:        "RG3_PublisherToFenceAtomicHeadCAS",
			Provider:  DomainFence,
			Consumer:  DomainPublication,
			Rely:      "Volume.Flush commits Head exclusively via fence.Put at the Volume's claimed Round.",
			Guarantee: "Deposed writer cannot overwrite Head after successor Claim advances Round.",
			Check: func(pre SystemState, a Action, post SystemState) string {
				if a.Kind == ActionVolumeFlush && a.ReturnedErr == "" && !a.CleanNoOpFlush && !a.LostAckLanded {
					if a.Round < pre.Fence.Head.Round {
						return fmt.Sprintf("Flush succeeded at stale Round %d < fence Head Round %d",
							a.Round, pre.Fence.Head.Round)
					}
				}
				return ""
			},
		},
		{
			ID:        "RG4_TakeoverRotatesJournalLineage",
			Provider:  DomainFence,
			Consumer:  DomainJournal,
			Rely:      "Attach/Takeover binds local journal Lineage to both Volume.ID and the newly claimed Round.",
			Guarantee: "A deposed writer's or prior round's journal segments are rejected with ErrLineage on reopen.",
			Check: func(pre SystemState, a Action, post SystemState) string {
				if (a.Kind == ActionVolumeTakeover || a.Kind == ActionJournalOpen) && a.ReturnedErr == "" && post.Journal.Open {
					if post.Publication.Committed.ID != ([16]byte{}) && post.Journal.LineageVolumeID != post.Publication.Committed.ID {
						return "journal Lineage does not bind Volume.ID"
					}
					if a.Round > 0 && post.Journal.LineageRound != a.Round {
						return fmt.Sprintf("journal LineageRound=%d does not match active fence Round=%d",
							post.Journal.LineageRound, a.Round)
					}
				}
				return ""
			},
		},
		{
			ID:        "RG5_OpenNeverRetriesMismatchWithZero",
			Provider:  DomainPublication,
			Consumer:  DomainJournal,
			Rely:      "Volume passes Head.Journal to journal.Open once and falls back to store Head on ErrMismatch/ErrLineage/ErrCorrupt without retrying Published=0.",
			Guarantee: "Stale or mismatched local journals cannot masquerade as fresh timelines.",
			Check: func(pre SystemState, a Action, post SystemState) string {
				if a.Kind == ActionJournalOpen && a.RetriedWithZero {
					return "caller retried journal.Open with Published=0 after mismatch instead of falling back to Head"
				}
				if a.Kind == ActionJournalOpen && a.ReturnedErr == "" && post.Publication.Committed.ID != ([16]byte{}) {
					if a.OpenPublishedArg != post.Publication.Committed.Journal {
						return fmt.Sprintf("journal.Open called with Published=%d != committed Head.Journal=%d",
							a.OpenPublishedArg, post.Publication.Committed.Journal)
					}
				}
				return ""
			},
		},
		{
			ID:        "RG6_BulkSpoolDurableBeforeOrderedLogRef",
			Provider:  DomainPublication,
			Consumer:  DomainJournal,
			Rely:      "Payloads > 64 KiB synced to a side spool reach durability before their reference record enters the ordered journal.",
			Guarantee: "Ordered journal replay never yields a metadata record naming bulk data lost in a crash.",
			Check: func(pre SystemState, a Action, post SystemState) string {
				if a.Kind == ActionJournalAppend && a.ReturnedErr == "" && a.BulkRefSeq > pre.Journal.BulkDurableSeq {
					return fmt.Sprintf("ordered journal appended reference to unsynced bulk spool entry %d (durable=%d)",
						a.BulkRefSeq, pre.Journal.BulkDurableSeq)
				}
				return ""
			},
		},
		{
			ID:        "RG7_ResealVerifiesRootShardUnderTargetVolume",
			Provider:  DomainCorruption,
			Consumer:  DomainPublication,
			Rely:      "Reseal opens the root shard named by Head under Head.ID at the target backend before rewriting Head.",
			Guarantee: "Incomplete copies and foreign objects at the Head key are refused before claiming the round.",
			Check: func(pre SystemState, a Action, post SystemState) string {
				if a.Kind == ActionVolumeReseal && a.ReturnedErr == "" && !a.ResealVerifiedRoot {
					return "Reseal rewrote Head without verifying root shard under target Volume ID"
				}
				return ""
			},
		},
		{
			ID:        "RG8_LostAckLandedChecksFullCommitWatermarks",
			Provider:  DomainFence,
			Consumer:  DomainPublication,
			Rely:      "On ErrStaleRound, Flush treats the commit as landed only if store Head matches ID, Root, Next >= working Next, and Journal >= target Journal.",
			Guarantee: "ABA tree-root equality across takeover cannot falsely acknowledge uncommitted Journal or Next watermarks.",
			Check: func(pre SystemState, a Action, post SystemState) string {
				if a.Kind == ActionVolumeFlush && a.LostAckLanded && a.ReturnedErr == "" {
					c := post.Publication.Committed
					if c.Root != post.Publication.WorkingRoot {
						return "lost-ack recovery succeeded when store Root != working Root"
					}
					if c.Next < post.Publication.WorkingNext {
						return fmt.Sprintf("lost-ack ABA false positive: store Root matched but Head.Next=%d < working Next=%d",
							c.Next, post.Publication.WorkingNext)
					}
					if c.Journal < a.JournalTarget {
						return fmt.Sprintf("lost-ack ABA false positive: store Root matched but Head.Journal=%d < target Journal=%d",
							c.Journal, a.JournalTarget)
					}
				}
				return ""
			},
		},
	}
}
