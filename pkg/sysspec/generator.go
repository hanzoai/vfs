// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sysspec

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"
)

// RegressionCase is a machine-generated positive or negative trace scenario
// derived from the structured specification rules.
type RegressionCase struct {
	Name       string
	Domain     Domain
	Layer      Layer
	ExpectRule string // empty when the trace must satisfy every rule
	Trace      Trace
}

// BaselineValidState returns a canonical, well-formed SystemState at Round 1,
// Head Seq 1, and Journal watermark 2 from which valid and adversarial traces
// evolve.
func BaselineValidState() SystemState {
	var volID [16]byte
	var seed, root [32]byte
	var tok [16]byte
	for i := range volID {
		volID[i] = byte(0x10 + i)
		tok[i] = byte(0xa0 + i)
	}
	for i := range seed {
		seed[i] = byte(0x20 + i)
		root[i] = byte(0x40 + i)
	}
	return SystemState{
		Journal: JournalState{
			Open:            true,
			LineageVolumeID: volID,
			LineageRound:    1,
			Visible:         2,
			Durable:         2,
			Published:       2,
			Proven:          2,
			Floor:           2,
			FirstKept:       0,
			LastKept:        2,
			ActiveSyncEpoch: 2,
			CompletedSync:   2,
			RecordWriteEpoch: map[uint64]uint64{
				1: 1,
				2: 1,
			},
			OldestAckUnixNs:  0,
			DeadlineAnchorNs: 0,
			CapacityBytes:    64 << 20,
			UsedBytes:        4096,
			BulkDurableSeq:   1,
		},
		Fence: FenceState{
			Head: FenceRecord{
				Exists:  true,
				Version: 1,
				Round:   1,
				Token:   tok,
				Payload: []byte("sealed-head-v1"),
				Size:    256,
			},
			MaxSize: MaxHeadBytes,
			Writers: map[string]FenceWriter{
				"w1": {ID: "w1", Round: 1, Token: tok},
			},
		},
		Publication: PublicationState{
			Committed: CommitHead{
				Seq:     1,
				Root:    root,
				Next:    2,
				ID:      volID,
				Seed:    seed,
				Journal: 2,
				Needs:   0,
				Tail:    "",
			},
			WorkingRoot:      root,
			WorkingNext:      2,
			PendingOverlay:   0,
			OpenPackEmpty:    true,
			LastAttemptedSeq: 1,
			CallerSeenSeq:    1,
			UsedSeqs:         map[uint64]bool{1: true},
			UploadedPacks:    map[string]bool{"pack/01/p1": true},
			UploadedShards:   map[string]bool{"meta/01/s1": true},
			ReferencedPacks:  map[string]bool{"pack/01/p1": true},
			ReferencedShards: map[string]bool{"meta/01/s1": true},
		},
	}
}

func stepTrace(name string, pre SystemState, a Action, post SystemState) Trace {
	return Trace{
		Name:  name,
		Steps: []Step{{Index: 0, Action: a, Pre: pre, Post: post}},
	}
}

// GenerateRegressionSuite deterministically synthesizes positive multi-step
// protocol traces (including randomized valid histories under seed) together
// with targeted single-rule violation regressions covering all 38 rules across
// the four domains and three SYSSPEC layers.
func GenerateRegressionSuite(seed uint64) []RegressionCase {
	var cases []RegressionCase

	// 1. Valid multi-step end-to-end traces (must produce 0 violations).
	for i := uint64(0); i < 12; i++ {
		cases = append(cases, RegressionCase{
			Name:       fmt.Sprintf("valid/multi_step_protocol_seed_%d", seed+i),
			Domain:     DomainJournal,
			Layer:      LayerFunctionality,
			ExpectRule: "",
			Trace:      generateValidProtocolTrace(seed+i, 18),
		})
	}

	// 2. Clean no-op Flush trace.
	{
		pre := BaselineValidState()
		post := pre.Clone()
		cases = append(cases, RegressionCase{
			Name:       "valid/clean_noop_flush_zero_writes",
			Domain:     DomainPublication,
			Layer:      LayerFunctionality,
			ExpectRule: "",
			Trace: stepTrace("clean-flush", pre, Action{
				Kind:           ActionVolumeFlush,
				Actor:          "w1",
				Round:          1,
				CleanNoOpFlush: true,
			}, post),
		})
	}

	// 3. Valid lost-ack takeover recognition trace.
	{
		pre := BaselineValidState()
		post := pre.Clone()
		cases = append(cases, RegressionCase{
			Name:       "valid/lost_ack_same_token_idempotent",
			Domain:     DomainFence,
			Layer:      LayerFunctionality,
			ExpectRule: "",
			Trace: stepTrace("lost-ack-ok", pre, Action{
				Kind:          ActionFencePut,
				Actor:         "w1",
				Round:         1,
				LostAckLanded: true,
			}, post),
		})
	}

	// 4. Corruption domain rule regressions (C1..C8).
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Corruption = &ObjectSealCheck{
			Kind:          "head",
			ExpectedPlace: "s3://bucket-a/vol",
			ActualPlace:   "s3://bucket-b/vol",
		}
		cases = append(cases, RegressionCase{
			Name: "neg/C1_cross_place_head_context_accepted", Domain: DomainCorruption,
			Layer: LayerFunctionality, ExpectRule: "C1_InjectiveSealingContext",
			Trace: stepTrace("c1", pre, Action{Kind: ActionObjectDecode}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Corruption = &ObjectSealCheck{
			Kind:             "head",
			NonMinimalVarint: true,
		}
		cases = append(cases, RegressionCase{
			Name: "neg/C2_non_minimal_uvarint_accepted", Domain: DomainCorruption,
			Layer: LayerFunctionality, ExpectRule: "C2_CanonicalShortestVarint",
			Trace: stepTrace("c2", pre, Action{Kind: ActionObjectDecode}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Corruption = &ObjectSealCheck{
			Kind:              "head",
			KnownNeeds:        0,
			HeadNeeds:         1 << 2,
			TailFields:        []TailField{{Tag: 5, Length: 4, ActualLen: 1}}, // broken tail too
			ObservedErrorKind: "ErrCorrupt",                                   // wrong: must be ErrNeeds first
		}
		cases = append(cases, RegressionCase{
			Name: "neg/C3_tail_checked_before_unknown_needs", Domain: DomainCorruption,
			Layer: LayerFunctionality, ExpectRule: "C3_NeedsCheckedBeforeTail",
			Trace: stepTrace("c3", pre, Action{Kind: ActionObjectDecode}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Corruption = &ObjectSealCheck{
			Kind:       "head",
			KnownNeeds: 0,
			HeadNeeds:  0,
			TailFields: []TailField{
				{Tag: 4, Length: 2, ActualLen: 2},
				{Tag: 3, Length: 2, ActualLen: 2},
			},
		}
		cases = append(cases, RegressionCase{
			Name: "neg/C4_head_tail_descending_tags_accepted", Domain: DomainCorruption,
			Layer: LayerFunctionality, ExpectRule: "C4_HeadTailStrictAscendingBounded",
			Trace: stepTrace("c4", pre, Action{Kind: ActionObjectDecode}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Fence.Head.Size = MaxHeadBytes + 1
		cases = append(cases, RegressionCase{
			Name: "neg/C5_head_exceeds_64kib_ceiling", Domain: DomainCorruption,
			Layer: LayerFunctionality, ExpectRule: "C5_HeadSizeWithinFenceCeiling",
			Trace: stepTrace("c5", pre, Action{Kind: ActionFencePut, Actor: "w1", Round: 1}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Corruption = &ObjectSealCheck{
			Kind:             "chunk",
			ExpectedVolumeID: pre.Publication.Committed.ID,
			ActualVolumeID:   pre.Publication.Committed.ID,
			TagBytesVerified: 31,
		}
		cases = append(cases, RegressionCase{
			Name: "neg/C6_partial_tag_verification_31_bytes", Domain: DomainCorruption,
			Layer: LayerFunctionality, ExpectRule: "C6_CipherRejectsPlaintextOrPartialTag",
			Trace: stepTrace("c6", pre, Action{Kind: ActionObjectDecode}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Corruption = &ObjectSealCheck{
			Kind:             "pack",
			PackLength:       8191,
			FurthestChunkEnd: 8192,
		}
		cases = append(cases, RegressionCase{
			Name: "neg/C7_truncated_pack_passes_verify", Domain: DomainCorruption,
			Layer: LayerFunctionality, ExpectRule: "C7_ExtentChunkAndPackBounds",
			Trace: stepTrace("c7", pre, Action{Kind: ActionVolumeVerify}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.Visible = 6
		pre.Journal.Durable = 6
		pre.Journal.Proven = 6
		pre.Journal.Published = 2
		pre.Journal.FirstKept = 3
		pre.Journal.LastKept = 6
		post := pre.Clone()
		post.Journal.Visible = 4
		post.Journal.Durable = 4
		post.Journal.Proven = 4
		post.Journal.LastKept = 4
		cases = append(cases, RegressionCase{
			Name: "neg/C8_proven_durable_damage_treated_as_torn_tail", Domain: DomainCorruption,
			Layer: LayerFunctionality, ExpectRule: "C8_JournalProvenDamageFailsClosed",
			Trace: stepTrace("c8", pre, Action{Kind: ActionJournalCrashOpen, Round: 1, OpenPublishedArg: 2}, post),
		})
	}

	// 5. Fence domain rule regressions (F1..F6, CC4).
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Fence.Writers["bad"] = FenceWriter{ID: "bad", Round: math.MaxUint64}
		cases = append(cases, RegressionCase{
			Name: "neg/F1_writer_attached_at_max_uint64_round", Domain: DomainFence,
			Layer: LayerFunctionality, ExpectRule: "F1_RoundBoundsValid",
			Trace: stepTrace("f1", pre, Action{Kind: ActionFenceClaim, Actor: "bad", Round: math.MaxUint64, ReturnedErr: "ErrInvalid"}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Fence.Head.Round = 3
		post := pre.Clone()
		post.Fence.Head.Version = 2
		cases = append(cases, RegressionCase{
			Name: "neg/F2_stale_round_put_admitted", Domain: DomainFence,
			Layer: LayerFunctionality, ExpectRule: "F2_PutRequiresAtLeastRecordedRound",
			Trace: stepTrace("f2", pre, Action{Kind: ActionFencePut, Actor: "w1", Round: 1}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		cases = append(cases, RegressionCase{
			Name: "neg/F3_claim_admits_equal_round", Domain: DomainFence,
			Layer: LayerFunctionality, ExpectRule: "F3_ClaimRequiresStrictlyHigherRound",
			Trace: stepTrace("f3", pre, Action{Kind: ActionFenceClaim, Actor: "w2", Round: 1}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Fence.Head.Round = 2
		post.Fence.Head.Payload = []byte("clobbered-payload")
		cases = append(cases, RegressionCase{
			Name: "neg/F4_claim_clobbers_predecessor_payload", Domain: DomainFence,
			Layer: LayerFunctionality, ExpectRule: "F4_TakeoverPreservesLatestPayload",
			Trace: stepTrace("f4", pre, Action{Kind: ActionFenceClaim, Actor: "w2", Round: 2}, post),
		})
	}
	{
		pre := BaselineValidState()
		w := pre.Fence.Writers["w1"]
		w.Fenced = true
		pre.Fence.Writers["w1"] = w
		post := pre.Clone()
		cases = append(cases, RegressionCase{
			Name: "neg/F5_deposed_writer_keeps_committing", Domain: DomainFence,
			Layer: LayerFunctionality, ExpectRule: "F5_DeposedWriterPermanentlyFenced",
			Trace: stepTrace("f5", pre, Action{Kind: ActionFencePut, Actor: "w1", Round: 1}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Fence.Head.Token = [16]byte{0xff}
		cases = append(cases, RegressionCase{
			Name: "neg/F6_lost_ack_accepts_foreign_writer_token", Domain: DomainFence,
			Layer: LayerFunctionality, ExpectRule: "F6_LostAckTokenIdempotence",
			Trace: stepTrace("f6", pre, Action{Kind: ActionFencePut, Actor: "w1", Round: 1, LostAckLanded: true}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		cases = append(cases, RegressionCase{
			Name: "neg/CC4_client_side_get_then_unconditional_put", Domain: DomainFence,
			Layer: LayerConcurrency, ExpectRule: "CC4_SingleConditionalWriteNoAdmitWindow",
			Trace: stepTrace("cc4", pre, Action{Kind: ActionFencePut, Actor: "w1", Round: 1, AdmitWriteSplit: true}, post),
		})
	}

	// 6. Publication domain rule regressions (P1..P5, CC3).
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Publication.FlushStages = []WriteStage{
			{Kind: "head", Key: "head"},
			{Kind: "pack", Key: "pack/02/p2"},
		}
		post.Publication.Committed.Seq = 2
		post.Publication.LastAttemptedSeq = 2
		post.Publication.UsedSeqs[2] = true
		cases = append(cases, RegressionCase{
			Name: "neg/P1_head_written_before_pack_upload", Domain: DomainPublication,
			Layer: LayerFunctionality, ExpectRule: "P1_WriteOrderPacksShardsBeforeHead",
			Trace: stepTrace("p1", pre, Action{Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 2, JournalTarget: 2}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Publication.ReferencedShards["meta/99/missing"] = true
		post.Publication.Committed.Seq = 2
		post.Publication.LastAttemptedSeq = 2
		post.Publication.UsedSeqs[2] = true
		cases = append(cases, RegressionCase{
			Name: "neg/P2_head_references_unuploaded_shard", Domain: DomainPublication,
			Layer: LayerFunctionality, ExpectRule: "P2_OrphansNeverReferencedBeforeHeadCAS",
			Trace: stepTrace("p2", pre, Action{Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 2, JournalTarget: 2}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		cases = append(cases, RegressionCase{
			Name: "neg/P3_flush_reuses_attempted_head_seq", Domain: DomainPublication,
			Layer: LayerFunctionality, ExpectRule: "P3_HeadSeqStrictlyNonRepeating",
			Trace: stepTrace("p3", pre, Action{Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 1, JournalTarget: 2}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Publication.Committed.Tail = "\x01\x01A"
		post := pre.Clone()
		post.Publication.Committed.Seq = 2
		post.Publication.Committed.Tail = ""
		post.Publication.LastAttemptedSeq = 2
		post.Publication.UsedSeqs[2] = true
		cases = append(cases, RegressionCase{
			Name: "neg/P4_commit_drops_unknown_head_tail", Domain: DomainPublication,
			Layer: LayerFunctionality, ExpectRule: "P4_HeadIdentityAndUnknownFieldsCarried",
			Trace: stepTrace("p4", pre, Action{Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 2, JournalTarget: 2}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Publication.FlushStages = []WriteStage{{Kind: "head", Key: "head"}}
		cases = append(cases, RegressionCase{
			Name: "neg/P5_clean_flush_writes_redundant_head", Domain: DomainPublication,
			Layer: LayerFunctionality, ExpectRule: "P5_CleanFlushMakesZeroStoreWrites",
			Trace: stepTrace("p5", pre, Action{Kind: ActionVolumeFlush, Actor: "w1", Round: 1, CleanNoOpFlush: true}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Publication.ConcurrentReaders = 2
		post.Publication.FlushHeldBy = ""
		post.Publication.Committed.Seq = 2
		post.Publication.LastAttemptedSeq = 2
		post.Publication.UsedSeqs[2] = true
		cases = append(cases, RegressionCase{
			Name: "neg/CC3_flush_runs_without_exclusive_lock", Domain: DomainPublication,
			Layer: LayerConcurrency, ExpectRule: "CC3_FlushExclusiveHold",
			Trace: stepTrace("cc3", pre, Action{Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 2, JournalTarget: 2}, post),
		})
	}

	// 7. Journal domain rule regressions (J1..J7, CC1, CC2).
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Journal.Published = 4
		post.Journal.Durable = 3
		post.Journal.Visible = 4
		post.Journal.CommittedFix()
		cases = append(cases, RegressionCase{
			Name: "neg/J1_published_watermark_exceeds_durable", Domain: DomainJournal,
			Layer: LayerFunctionality, ExpectRule: "J1_MonotoneWatermarks",
			Trace: stepTrace("j1", pre, Action{Kind: ActionJournalPublish, Seq: 4, ReturnedErr: "ErrAhead"}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Journal.Visible = 5
		post.Journal.Durable = 5
		post.Journal.FirstKept = 4 // gap: 3 is missing while Published=2
		post.Journal.LastKept = 5
		cases = append(cases, RegressionCase{
			Name: "neg/J2_gap_in_unpublished_journal_suffix", Domain: DomainJournal,
			Layer: LayerFunctionality, ExpectRule: "J2_ContiguousSequenceNoGaps",
			Trace: stepTrace("j2", pre, Action{Kind: ActionJournalReplay}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.Visible = 4
		pre.Journal.Durable = 2
		pre.Journal.FirstKept = 3
		pre.Journal.LastKept = 4
		pre.Publication.Committed.Journal = 4
		post := pre.Clone()
		post.Journal.Published = 4
		post.Journal.Durable = 4
		post.Journal.FirstKept = 0
		cases = append(cases, RegressionCase{
			Name: "neg/J3_publish_admitted_ahead_of_local_sync", Domain: DomainJournal,
			Layer: LayerFunctionality, ExpectRule: "J3_PublishRequiresDurable",
			Trace: stepTrace("j3", pre, Action{Kind: ActionJournalPublish, Seq: 4}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.Visible = 5
		pre.Journal.Durable = 5
		pre.Journal.Published = 4
		pre.Journal.Floor = 2
		pre.Journal.FirstKept = 5
		pre.Journal.LastKept = 5
		post := pre.Clone()
		post.Journal.Floor = 2 // failed to advance durable floor before wiping 3..4
		cases = append(cases, RegressionCase{
			Name: "neg/J4_reclaim_wipes_segments_before_floor_sync", Domain: DomainJournal,
			Layer: LayerFunctionality, ExpectRule: "J4_ReclaimPreservesUnpublishedAndFloor",
			Trace: stepTrace("j4", pre, Action{Kind: ActionJournalReclaim}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.Floor = 6
		pre.Journal.Visible = 8
		pre.Journal.Durable = 8
		pre.Journal.Published = 6
		pre.Journal.FirstKept = 7
		pre.Journal.LastKept = 8
		pre.Publication.Committed.Journal = 4
		post := pre.Clone()
		cases = append(cases, RegressionCase{
			Name: "neg/J5_open_accepts_stale_store_below_floor", Domain: DomainJournal,
			Layer: LayerFunctionality, ExpectRule: "J5_OpenRefusesLineageFormatAndFloorMismatch",
			Trace: stepTrace("j5", pre, Action{Kind: ActionJournalOpen, Round: 1, OpenPublishedArg: 4}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Journal.Visible = 4 // skipped 3
		post.Journal.FirstKept = 3
		post.Journal.LastKept = 4
		cases = append(cases, RegressionCase{
			Name: "neg/J6_append_skips_sequence_number", Domain: DomainJournal,
			Layer: LayerFunctionality, ExpectRule: "J6_AppendMonotoneSequence",
			Trace: stepTrace("j6", pre, Action{Kind: ActionJournalAppend, Seq: 4, BulkRefSeq: 1}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.Visible = 3
		pre.Journal.FirstKept = 3
		pre.Journal.LastKept = 3
		pre.Journal.OldestAckUnixNs = 1000
		pre.Journal.DeadlineAnchorNs = 6000
		post := pre.Clone()
		post.Journal.Visible = 4
		post.Journal.LastKept = 4
		post.Journal.DeadlineAnchorNs = 9000 // wrongly pushed out by newer append
		cases = append(cases, RegressionCase{
			Name: "neg/J7_later_append_resets_oldest_due_deadline", Domain: DomainJournal,
			Layer: LayerFunctionality, ExpectRule: "J7_DeadlineAnchoredToOldestUnpublished",
			Trace: stepTrace("j7", pre, Action{Kind: ActionJournalAppend, Seq: 4, BulkRefSeq: 1}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.Visible = 3
		pre.Journal.FirstKept = 3
		pre.Journal.LastKept = 3
		pre.Journal.RecordWriteEpoch[3] = 5
		post := pre.Clone()
		post.Journal.Durable = 3
		cases = append(cases, RegressionCase{
			Name: "neg/CC1_sync_satisfied_by_inflight_epoch_before_write", Domain: DomainJournal,
			Layer: LayerConcurrency, ExpectRule: "CC1_SyncGroupBarrierEpoch",
			Trace: stepTrace("cc1", pre, Action{Kind: ActionJournalSync, Seq: 3, SyncFlightUsed: 5}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.ActiveReplays = 1
		post := pre.Clone()
		cases = append(cases, RegressionCase{
			Name: "neg/CC2_reclaim_runs_during_active_replay", Domain: DomainJournal,
			Layer: LayerConcurrency, ExpectRule: "CC2_ReplayBlocksReclaim",
			Trace: stepTrace("cc2", pre, Action{Kind: ActionJournalReclaim}, post),
		})
	}

	// 8. Modularity Rely/Guarantee boundary regressions (RG1..RG8).
	{
		pre := BaselineValidState()
		pre.Journal.Visible = 5
		pre.Journal.Durable = 3
		pre.Journal.FirstKept = 3
		pre.Journal.LastKept = 5
		post := pre.Clone()
		post.Publication.Committed.Seq = 2
		post.Publication.Committed.Journal = 5
		post.Publication.LastAttemptedSeq = 2
		post.Publication.UsedSeqs[2] = true
		cases = append(cases, RegressionCase{
			Name: "neg/RG1_publisher_commits_unsynced_journal_seq_to_head", Domain: DomainPublication,
			Layer: LayerModularity, ExpectRule: "RG1_JournalToPublisherDurableGate",
			Trace: stepTrace("rg1", pre, Action{Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 2, JournalTarget: 5}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.Visible = 5
		pre.Journal.Durable = 5
		pre.Journal.FirstKept = 3
		pre.Journal.LastKept = 5
		post := pre.Clone()
		post.Journal.Published = 5
		post.Journal.FirstKept = 0 // published locally before Head.Journal (still 2) committed!
		cases = append(cases, RegressionCase{
			Name: "neg/RG2_publisher_advances_journal_before_head_cas", Domain: DomainJournal,
			Layer: LayerModularity, ExpectRule: "RG2_PublisherToJournalPostCommitAdvance",
			Trace: stepTrace("rg2", pre, Action{Kind: ActionJournalPublish, Seq: 5}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Fence.Head.Round = 4
		post := pre.Clone()
		post.Publication.Committed.Seq = 2
		post.Publication.LastAttemptedSeq = 2
		post.Publication.UsedSeqs[2] = true
		cases = append(cases, RegressionCase{
			Name: "neg/RG3_flush_bypasses_fence_round_check", Domain: DomainPublication,
			Layer: LayerModularity, ExpectRule: "RG3_PublisherToFenceAtomicHeadCAS",
			Trace: stepTrace("rg3", pre, Action{Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 2, JournalTarget: 2}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Fence.Head.Round = 2
		post.Journal.LineageRound = 1 // omitted round rotation on takeover!
		cases = append(cases, RegressionCase{
			Name: "neg/RG4_takeover_reuses_old_round_journal_lineage", Domain: DomainJournal,
			Layer: LayerModularity, ExpectRule: "RG4_TakeoverRotatesJournalLineage",
			Trace: stepTrace("rg4", pre, Action{Kind: ActionVolumeTakeover, Actor: "w2", Round: 2}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Journal.Open = false
		cases = append(cases, RegressionCase{
			Name: "neg/RG5_open_mismatch_retried_with_zero", Domain: DomainJournal,
			Layer: LayerModularity, ExpectRule: "RG5_OpenNeverRetriesMismatchWithZero",
			Trace: stepTrace("rg5", pre, Action{
				Kind: ActionJournalOpen, Round: 1, OpenPublishedArg: 0, RetriedWithZero: true, ReturnedErr: "ErrMismatch",
			}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Journal.Visible = 3
		post.Journal.FirstKept = 3
		post.Journal.LastKept = 3
		cases = append(cases, RegressionCase{
			Name: "neg/RG6_ordered_log_references_unsynced_bulk_spool", Domain: DomainJournal,
			Layer: LayerModularity, ExpectRule: "RG6_BulkSpoolDurableBeforeOrderedLogRef",
			Trace: stepTrace("rg6", pre, Action{Kind: ActionJournalAppend, Seq: 3, BulkRefSeq: 2}, post),
		})
	}
	{
		pre := BaselineValidState()
		post := pre.Clone()
		post.Fence.Head.Round = 2
		cases = append(cases, RegressionCase{
			Name: "neg/RG7_reseal_skips_target_root_shard_verification", Domain: DomainPublication,
			Layer: LayerModularity, ExpectRule: "RG7_ResealVerifiesRootShardUnderTargetVolume",
			Trace: stepTrace("rg7", pre, Action{Kind: ActionVolumeReseal, Actor: "w2", Round: 2, ResealVerifiedRoot: false}, post),
		})
	}
	{
		pre := BaselineValidState()
		pre.Journal.Visible = 6
		pre.Journal.Durable = 6
		pre.Journal.FirstKept = 3
		pre.Journal.LastKept = 6
		post := pre.Clone()
		// Store still holds Journal=2 with identical prolly Root after ABA create+remove!
		cases = append(cases, RegressionCase{
			Name: "neg/RG8_lost_ack_aba_root_match_drops_journal_watermark", Domain: DomainPublication,
			Layer: LayerModularity, ExpectRule: "RG8_LostAckLandedChecksFullCommitWatermarks",
			Trace: stepTrace("rg8", pre, Action{
				Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 2, JournalTarget: 6, LostAckLanded: true,
			}, post),
		})
	}

	return cases
}

// CommittedFix is a small helper keeping FirstKept/LastKept coherent when
// building single-rule watermark violation fixtures.
func (j *JournalState) CommittedFix() {
	if j.Visible <= j.Published {
		j.FirstKept = 0
		j.LastKept = j.Visible
	} else {
		j.FirstKept = j.Published + 1
		j.LastKept = j.Visible
	}
}

func generateValidProtocolTrace(seed uint64, steps int) Trace {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	cur := BaselineValidState()
	var tr Trace
	tr.Name = fmt.Sprintf("valid-seed-%d", seed)
	activeActor := "w1"
	for idx := 0; idx < steps; idx++ {
		pre := cur.Clone()
		post := cur.Clone()
		var act Action
		switch rng.IntN(5) {
		case 0: // Append to local journal
			seq := pre.Journal.Visible + 1
			post.Journal.Visible = seq
			post.Journal.LastKept = seq
			if pre.Journal.Visible == pre.Journal.Published {
				post.Journal.FirstKept = seq
				post.Journal.OldestAckUnixNs = int64(1_000_000 * (idx + 1))
				post.Journal.DeadlineAnchorNs = post.Journal.OldestAckUnixNs + int64(5*time.Second)
			}
			post.Journal.RecordWriteEpoch[seq] = post.Journal.ActiveSyncEpoch
			post.Journal.UsedBytes += 512
			act = Action{
				Kind:       ActionJournalAppend,
				Actor:      activeActor,
				Round:      post.Fence.Head.Round,
				Seq:        seq,
				BulkRefSeq: post.Journal.BulkDurableSeq,
			}
		case 1: // Group Sync up to Visible
			if pre.Journal.Visible == pre.Journal.Durable {
				act = Action{
					Kind:           ActionVolumeFlush,
					Actor:          activeActor,
					Round:          post.Fence.Head.Round,
					CleanNoOpFlush: true,
				}
				post.Publication.FlushStages = nil
				break
			}
			flight := pre.Journal.ActiveSyncEpoch + 1
			post.Journal.ActiveSyncEpoch = flight
			post.Journal.CompletedSync = flight
			post.Journal.Durable = pre.Journal.Visible
			if idx%4 == 0 {
				post.Journal.Proven = post.Journal.Durable
			}
			act = Action{
				Kind:           ActionJournalSync,
				Actor:          activeActor,
				Round:          post.Fence.Head.Round,
				Seq:            pre.Journal.Visible,
				SyncFlightUsed: flight,
			}
		case 2: // Volume Flush publishing durable generation
			if pre.Journal.Durable <= pre.Publication.Committed.Journal {
				post.Publication.FlushStages = nil
				act = Action{
					Kind:           ActionVolumeFlush,
					Actor:          activeActor,
					Round:          post.Fence.Head.Round,
					CleanNoOpFlush: true,
				}
				break
			}
			packKey := fmt.Sprintf("pack/%02x/p%d", idx&0xff, idx)
			shardKey := fmt.Sprintf("meta/%02x/s%d", idx&0xff, idx)
			nextSeq := pre.Publication.LastAttemptedSeq + 1
			var newRoot [32]byte
			newRoot[0] = byte(nextSeq)
			newRoot[1] = byte(seed)
			post.Publication.UploadedPacks[packKey] = true
			post.Publication.UploadedShards[shardKey] = true
			post.Publication.ReferencedPacks[packKey] = true
			post.Publication.ReferencedShards[shardKey] = true
			post.Publication.FlushStages = []WriteStage{
				{Kind: "pack", Key: packKey},
				{Kind: "shard", Key: shardKey},
				{Kind: "head", Key: "head"},
			}
			post.Publication.WorkingRoot = newRoot
			post.Publication.WorkingNext = pre.Publication.Committed.Next + 1
			post.Publication.Committed.Seq = nextSeq
			post.Publication.Committed.Root = newRoot
			post.Publication.Committed.Next = post.Publication.WorkingNext
			post.Publication.Committed.Journal = pre.Journal.Durable
			post.Publication.LastAttemptedSeq = nextSeq
			post.Publication.CallerSeenSeq = nextSeq
			post.Publication.UsedSeqs[nextSeq] = true
			post.Publication.FlushHeldBy = activeActor
			post.Fence.Head.Version++
			act = Action{
				Kind:          ActionVolumeFlush,
				Actor:         activeActor,
				Round:         post.Fence.Head.Round,
				Seq:           nextSeq,
				JournalTarget: pre.Journal.Durable,
			}
		case 3: // Journal Publish + Reclaim after Head commit
			target := pre.Publication.Committed.Journal
			if target <= pre.Journal.Published {
				post.Publication.FlushStages = nil
				act = Action{
					Kind:           ActionVolumeFlush,
					Actor:          activeActor,
					Round:          post.Fence.Head.Round,
					CleanNoOpFlush: true,
				}
				break
			}
			post.Journal.Published = target
			post.Journal.CommittedFix()
			if post.Journal.Visible == post.Journal.Published {
				post.Journal.OldestAckUnixNs = 0
				post.Journal.DeadlineAnchorNs = 0
			}
			act = Action{
				Kind:  ActionJournalPublish,
				Actor: activeActor,
				Round: post.Fence.Head.Round,
				Seq:   target,
			}
		default: // Writer Takeover at Round + 1
			newRound := pre.Fence.Head.Round + 1
			newActor := fmt.Sprintf("w%d", newRound)
			var newTok [16]byte
			newTok[0] = byte(newRound)
			oldW := post.Fence.Writers[activeActor]
			oldW.Fenced = true
			post.Fence.Writers[activeActor] = oldW
			post.Fence.Writers[newActor] = FenceWriter{ID: newActor, Round: newRound, Token: newTok}
			post.Fence.Head.Round = newRound
			post.Fence.Head.Token = newTok
			post.Fence.Head.Version++
			// Successor opens fresh journal bound to (Volume.ID, newRound) at Head.Journal.
			post.Journal.LineageRound = newRound
			post.Journal.Visible = post.Publication.Committed.Journal
			post.Journal.Durable = post.Publication.Committed.Journal
			post.Journal.Published = post.Publication.Committed.Journal
			post.Journal.Proven = post.Publication.Committed.Journal
			post.Journal.Floor = post.Publication.Committed.Journal
			post.Journal.FirstKept = 0
			post.Journal.LastKept = post.Publication.Committed.Journal
			post.Journal.OldestAckUnixNs = 0
			post.Journal.DeadlineAnchorNs = 0
			activeActor = newActor
			act = Action{
				Kind:  ActionVolumeTakeover,
				Actor: activeActor,
				Round: newRound,
			}
		}
		tr.Steps = append(tr.Steps, Step{
			Index:  idx,
			Action: act,
			Pre:    pre,
			Post:   post,
		})
		cur = post
	}
	return tr
}

// ComparativeClass partitions fault and bug scenarios by which methodology
// detects them, answering issue #56's bidirectional acceptance question.
type ComparativeClass string

const (
	ClassBothCatch    ComparativeClass = "both_catch"
	ClassSpecOnly     ComparativeClass = "sysspec_only"
	ClassExistingOnly ComparativeClass = "existing_only"
)

// ComparativeCase describes one scenario in the SYSSPEC vs mutation/fault
// differential evaluation.
type ComparativeCase struct {
	ID             string
	Class          ComparativeClass
	Domain         Domain
	Title          string
	ExistingRef    string // mutation name / test in tools/mutate or existing suite
	SpecRule       string // SYSSPEC rule ID triggered (empty for ClassExistingOnly)
	SpecCaught     bool
	ExistingCaught bool
	Trace          Trace
	WhyDifference  string
}

// MutationAudit summarizes the upstream tools/mutate/mutations.json corpus.
type MutationAudit struct {
	Total               int            `json:"total"`
	ByDomain            map[string]int `json:"by_domain"`
	JournalMutations    int            `json:"journal_mutations"`
	CrossLayerMutations int            `json:"cross_layer_mutations"`
}

// ComparativeReport holds all metrics produced by BuildComparativeMatrix.
type ComparativeReport struct {
	RuleCountInvariants     int               `json:"rule_count_invariants"`
	RuleCountTransitions    int               `json:"rule_count_transitions"`
	RuleCountRelyGuarantees int               `json:"rule_count_rely_guarantees"`
	RuleCountConcurrency    int               `json:"rule_count_concurrency"`
	RuleCountTotal          int               `json:"rule_count_total"`
	GeneratedRegressions    int               `json:"generated_regressions"`
	GeneratedPassRatePct    float64           `json:"generated_pass_rate_pct"`
	UpstreamMutations       MutationAudit     `json:"upstream_mutations"`
	BothCatchCount          int               `json:"both_catch_count"`
	SpecOnlyCount           int               `json:"spec_only_count"`
	ExistingOnlyCount       int               `json:"existing_only_count"`
	EvalDurationMicros      int64             `json:"eval_duration_micros"`
	Cases                   []ComparativeCase `json:"-"`
}

type rawMutationEntry struct {
	Name string `json:"name"`
	File string `json:"file"`
	Pkg  string `json:"pkg"`
	Run  string `json:"run"`
}

// AuditMutationsJSON classifies all entries in tools/mutate/mutations.json.
func AuditMutationsJSON(raw []byte) (MutationAudit, error) {
	var entries []rawMutationEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return MutationAudit{}, err
	}
	byDomain := map[string]int{
		"corruption_sealing_codec": 0,
		"fence":                    0,
		"publication_volume":       0,
		"journal":                  0,
		"tree_chunk_pack_ops":      0,
		"backend_mount_ext":        0,
	}
	crossLayer := 0
	for _, e := range entries {
		switch {
		case strings.HasPrefix(e.File, "journal/"):
			byDomain["journal"]++
		case e.File == "pkg/fence/fence.go":
			byDomain["fence"]++
		case e.File == "head.go" || e.File == "cipher.go" || e.File == "codec.go" || e.File == "verify.go" || e.File == "check.go":
			byDomain["corruption_sealing_codec"]++
		case e.File == "volume.go" || e.File == "generation.go" || e.File == "ledger.go":
			byDomain["publication_volume"]++
		case e.File == "tree.go" || e.File == "chunk.go" || e.File == "pack.go" || e.File == "shard.go" || e.File == "record.go" || e.File == "file.go" || e.File == "ops.go" || e.File == "lru.go" || e.File == "attr.go" || e.File == "namespace.go" || e.File == "rename.go" || e.File == "usage.go":
			byDomain["tree_chunk_pack_ops"]++
		default:
			byDomain["backend_mount_ext"]++
		}
		if (e.File == "generation.go" || e.File == "ledger.go" || e.File == "volume.go") &&
			(strings.Contains(e.Name, "journal") || strings.Contains(e.Name, "ledger") || strings.Contains(e.Name, "landed")) {
			crossLayer++
		}
	}
	return MutationAudit{
		Total:               len(entries),
		ByDomain:            byDomain,
		JournalMutations:    byDomain["journal"],
		CrossLayerMutations: crossLayer,
	}, nil
}

// BuildComparativeMatrix runs the generated regression suite and evaluates the
// 24-case bidirectional comparison between SYSSPEC and the existing mutation
// and fault test suites.
func BuildComparativeMatrix(mutationsJSON []byte) (ComparativeReport, error) {
	start := time.Now()
	reg := DefaultRegistry()
	invN, trN, rgN, ccN := reg.RuleCounts()

	var audit MutationAudit
	if len(mutationsJSON) > 0 {
		var err error
		audit, err = AuditMutationsJSON(mutationsJSON)
		if err != nil {
			return ComparativeReport{}, err
		}
	}

	gen := GenerateRegressionSuite(1349)
	matched := 0
	for _, tc := range gen {
		vs := reg.CheckTrace(tc.Trace)
		if tc.ExpectRule == "" {
			if len(vs) == 0 {
				matched++
			}
		} else {
			if slices.ContainsFunc(vs, func(v Violation) bool { return v.RuleID == tc.ExpectRule }) {
				matched++
			}
		}
	}

	comp := comparativeCorpus(gen)
	var bothN, specOnlyN, existingOnlyN int
	for i := range comp {
		vs := reg.CheckTrace(comp[i].Trace)
		comp[i].SpecCaught = len(vs) > 0
		if len(vs) > 0 && comp[i].SpecRule == "" {
			comp[i].SpecRule = vs[0].RuleID
		}
		switch comp[i].Class {
		case ClassBothCatch:
			bothN++
		case ClassSpecOnly:
			specOnlyN++
		case ClassExistingOnly:
			existingOnlyN++
		}
	}

	elapsed := time.Since(start).Microseconds()
	if elapsed == 0 {
		elapsed = 1
	}
	return ComparativeReport{
		RuleCountInvariants:     invN,
		RuleCountTransitions:    trN,
		RuleCountRelyGuarantees: rgN,
		RuleCountConcurrency:    ccN,
		RuleCountTotal:          invN + trN + rgN + ccN,
		GeneratedRegressions:    len(gen),
		GeneratedPassRatePct:    100.0 * float64(matched) / float64(len(gen)),
		UpstreamMutations:       audit,
		BothCatchCount:          bothN,
		SpecOnlyCount:           specOnlyN,
		ExistingOnlyCount:       existingOnlyN,
		EvalDurationMicros:      elapsed,
		Cases:                   comp,
	}, nil
}

func findGeneratedTrace(gen []RegressionCase, ruleID string) Trace {
	for _, g := range gen {
		if g.ExpectRule == ruleID {
			return g.Trace
		}
	}
	return Trace{}
}

func comparativeCorpus(gen []RegressionCase) []ComparativeCase {
	validTrace := generateValidProtocolTrace(99, 6)
	return []ComparativeCase{
		// Class A: Caught by BOTH SYSSPEC and existing mutation/fault suites (10 cases).
		{
			ID: "A01", Class: ClassBothCatch, Domain: DomainPublication,
			Title:       "Head written before pack/shard upload during Flush",
			ExistingRef: "mutations.json #25/#26 (volume.go) + TestFailureAtEveryWrite / TestSigkill",
			SpecRule:    "P1_WriteOrderPacksShardsBeforeHead", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "P1_WriteOrderPacksShardsBeforeHead"),
			WhyDifference: "Both flush stage ordering specification and crash/write-hook sweep detect premature Head PUT.",
		},
		{
			ID: "A02", Class: ClassBothCatch, Domain: DomainFence,
			Title:       "Older round admitted by fence.Put after takeover",
			ExistingRef: "mutations.json #60 (pkg/fence/fence.go) + TestFenceRefusesOlderRound",
			SpecRule:    "F2_PutRequiresAtLeastRecordedRound", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "F2_PutRequiresAtLeastRecordedRound"),
			WhyDifference: "Single-object round comparison is both a core fence transition rule and a targeted mutation.",
		},
		{
			ID: "A03", Class: ClassBothCatch, Domain: DomainFence,
			Title:       "Attach or Reseal admits equal or terminal (2^64-1) round",
			ExistingRef: "mutations.json #27/#28/#115/#125 (volume.go) + TestAttachNeedsAHigherRound",
			SpecRule:    "F3_ClaimRequiresStrictlyHigherRound", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "F3_ClaimRequiresStrictlyHigherRound"),
			WhyDifference: "Strict Claim inequality (round > recorded, round < 2^64-1) is pinned by both F3 and mutations.",
		},
		{
			ID: "A04", Class: ClassBothCatch, Domain: DomainPublication,
			Title:       "Commit attempt reuses a previously attempted Head.Seq",
			ExistingRef: "mutations.json #32 (volume.go) + TestHeadSeqNeverRepeats",
			SpecRule:    "P3_HeadSeqStrictlyNonRepeating", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "P3_HeadSeqStrictlyNonRepeating"),
			WhyDifference: "Attempt sequence monotonicity across failed and succeeded flushes is checked by both.",
		},
		{
			ID: "A05", Class: ClassBothCatch, Domain: DomainCorruption,
			Title:       "Cross-place Head or cross-volume shard/chunk AAD swap",
			ExistingRef: "mutations.json #41/#42/#90/#91 (head.go, cipher.go) + TestSealedToItsContext",
			SpecRule:    "C1_InjectiveSealingContext", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "C1_InjectiveSealingContext"),
			WhyDifference: "Length-framed domain-separation context mismatch fails both C1 and context unit tests.",
		},
		{
			ID: "A06", Class: ClassBothCatch, Domain: DomainCorruption,
			Title:       "Non-minimal varint encoding accepted by Head/record parser",
			ExistingRef: "mutations.json #206/#207 (codec.go) + TestHeadNonMinimalUvarint",
			SpecRule:    "C2_CanonicalShortestVarint", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "C2_CanonicalShortestVarint"),
			WhyDifference: "Canonical shortest-form integer invariant is enforced in both C2 and codec differential fuzzing.",
		},
		{
			ID: "A07", Class: ClassBothCatch, Domain: DomainCorruption,
			Title:       "Malformed Head.Tail inspected before unknown Head.Needs bits",
			ExistingRef: "mutations.json #205/#248 (head.go) + TestHeadDifferentialWithKnownBits",
			SpecRule:    "C3_NeedsCheckedBeforeTail", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "C3_NeedsCheckedBeforeTail"),
			WhyDifference: "Error precedence (ErrNeeds before ErrCorrupt on Tail) is explicitly encoded in C3 and head_diff_test.go.",
		},
		{
			ID: "A08", Class: ClassBothCatch, Domain: DomainCorruption,
			Title:       "Oversized Head (> 65,536 bytes) written to backend",
			ExistingRef: "mutations.json #226-#231 (pkg/fence/fence.go) + TestStoreNeverWritesWhatItWouldNotRead",
			SpecRule:    "C5_HeadSizeWithinFenceCeiling", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "C5_HeadSizeWithinFenceCeiling"),
			WhyDifference: "Both C5 and fence bound mutations verify fail-before-write at the 64 KiB Head limit.",
		},
		{
			ID: "A09", Class: ClassBothCatch, Domain: DomainCorruption,
			Title:       "Pack truncated 1 byte short of its furthest chunk passes verify",
			ExistingRef: "mutations.json #174/#225 (verify.go) + TestVerifyChecksTheFurthestChunk",
			SpecRule:    "C7_ExtentChunkAndPackBounds", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "C7_ExtentChunkAndPackBounds"),
			WhyDifference: "Both C7 and verify mutations enforce pack length >= max(offset + sealedLen).",
		},
		{
			ID: "A10", Class: ClassBothCatch, Domain: DomainJournal,
			Title:       "journal.Publish(seq) admitted ahead of durable sync watermark",
			ExistingRef: "journal/published_test.go (A2/R2) (0 entries in tools/mutate)",
			SpecRule:    "J3_PublishRequiresDurable", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "J3_PublishRequiresDurable"),
			WhyDifference: "Caught by J3 and journal unit tests, though absent from tools/mutate/mutations.json.",
		},

		// Class B: Historical pre-v2 negative fixture (B01) and current v2 cross-module / journal gaps.
		{
			ID: "B01", Class: ClassSpecOnly, Domain: DomainPublication,
			Title:       "Historical pre-v2 negative fixture: lost-ack ABA prolly-root match falsely acknowledges uncommitted Journal/Next watermarks",
			ExistingRef: "HISTORICAL NEGATIVE FIXTURE (fixed in v0.7.0 generation.go:532 landed()); mutation 'generation: any tree with the same root counts as landed' still survives TestAFencedPublicationIsNotRecognisedAsLanded on c3335b1",
			SpecRule:    "RG8_LostAckLandedChecksFullCommitWatermarks", ExistingCaught: false,
			Trace:         findGeneratedTrace(gen, "RG8_LostAckLandedChecksFullCommitWatermarks"),
			WhyDifference: "Production v0.7.0 Volume.landed(ctx, Head) now checks cur.ID == h.ID && cur.Root == h.Root && cur.Journal == h.Journal, so B01 is retained strictly as a historical negative fixture; relaxing landed() to 'return cur, err == nil' still survives TestAFencedPublicationIsNotRecognisedAsLanded because Flush fails on the next publisher tick while silently calling j.Publish(g.seq), whereas RG8/RG2 catch it immediately.",
		},
		{
			ID: "B02", Class: ClassBothCatch, Domain: DomainPublication,
			Title:       "Publisher commits Head.Journal = g before local journal Sync reaches g",
			ExistingRef: "CAUGHT in v0.7.0: mutations.json 'generation: the journal is not made durable before the Head' + TestHeadWaitsForTheJournalToBeDurable",
			SpecRule:    "RG1_JournalToPublisherDurableGate", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "RG1_JournalToPublisherDurableGate"),
			WhyDifference: "Converged on v0.7.0: both RG1 and generation.go's syncTo(ctx, g.seq) mutation test (TestHeadWaitsForTheJournalToBeDurable) catch store publication ahead of local journal durability.",
		},
		{
			ID: "B03", Class: ClassBothCatch, Domain: DomainJournal,
			Title:       "Publisher calls journal.Publish(g) after shard upload but before Head CAS succeeds",
			ExistingRef: "CAUGHT in v0.7.0: mutations.json 'generation: the journal is told before the Head commits' + TestJournalIsToldAfterTheHead",
			SpecRule:    "RG2_PublisherToJournalPostCommitAdvance", ExistingCaught: true,
			Trace:         findGeneratedTrace(gen, "RG2_PublisherToJournalPostCommitAdvance"),
			WhyDifference: "Converged on v0.7.0: both RG2 and generation.go's post-commit Publish mutation test (TestJournalIsToldAfterTheHead) catch advancing journal.Publish before Head CAS succeeds.",
		},
		{
			ID: "B04", Class: ClassSpecOnly, Domain: DomainJournal,
			Title:       "Takeover opens local journal with Volume.ID lineage only, omitting Round",
			ExistingRef: "MISSED: journal lineage is opaque []byte to journal.Open; ledger.go has 0 lineage mutations",
			SpecRule:    "RG4_TakeoverRotatesJournalLineage", ExistingCaught: false,
			Trace:         findGeneratedTrace(gen, "RG4_TakeoverRotatesJournalLineage"),
			WhyDifference: "journal.Open treats Lineage as opaque bytes and tools/mutate has no lineage mutation on ledger.go; modular contract RG4 enforces binding (Volume.ID, Round) across fence takeover.",
		},
		{
			ID: "B05", Class: ClassSpecOnly, Domain: DomainJournal,
			Title:       "Mount retries journal.Open with Published=0 after ErrMismatch",
			ExistingRef: "MISSED: caller-side retry policy across Volume and journal.Open has 0 mutations",
			SpecRule:    "RG5_OpenNeverRetriesMismatchWithZero", ExistingCaught: false,
			Trace:         findGeneratedTrace(gen, "RG5_OpenNeverRetriesMismatchWithZero"),
			WhyDifference: "Calling journal.Open(..., Published: 0) is legal in isolation on an unfloored log, but after ErrMismatch against Head.Journal it replays a divergent history instead of falling back to Head.",
		},
		{
			ID: "B06", Class: ClassSpecOnly, Domain: DomainJournal,
			Title:       "Ordered journal appends reference to >64 KiB bulk spool before spool Sync",
			ExistingRef: "MISSED: two-journal / spool ordering contract (docs/journal.md rule 8 & 13)",
			SpecRule:    "RG6_BulkSpoolDurableBeforeOrderedLogRef", ExistingCaught: false,
			Trace:         findGeneratedTrace(gen, "RG6_BulkSpoolDurableBeforeOrderedLogRef"),
			WhyDifference: "Single-instance crash exploration checks one journal's prefix, missing cross-spool dangling references after power loss.",
		},
		{
			ID: "B07", Class: ClassSpecOnly, Domain: DomainPublication,
			Title:       "Backend serves stale valid Head.Seq below caller's previously observed SeenSeq",
			ExistingRef: "MISSED: single-Head open cannot detect rollback without caller Seq state",
			SpecRule:    "P3_HeadSeqStrictlyNonRepeating", ExistingCaught: false,
			Trace: func() Trace {
				pre := BaselineValidState()
				pre.Publication.Committed.Seq = 5
				pre.Publication.LastAttemptedSeq = 5
				pre.Publication.CallerSeenSeq = 5
				post := pre.Clone()
				post.Publication.Committed.Seq = 3 // stale store replay
				post.Publication.LastAttemptedSeq = 6
				post.Publication.UsedSeqs[6] = true
				return stepTrace("stale-head-seq", pre, Action{
					Kind: ActionVolumeFlush, Actor: "w1", Round: 1, Seq: 6, JournalTarget: 2,
				}, post)
			}(),
			WhyDifference: "Cipher, place AAD, and fence framing all validate on a stale Head snapshot; multi-step trace state catches Seq regression below SeenSeq.",
		},
		{
			ID: "B08", Class: ClassSpecOnly, Domain: DomainJournal,
			Title:       "Source mutations inside journal/ (Sync barrier, Floor order, Due anchor) under tools/mutate",
			ExistingRef: "MISSED by tools/mutate: 0 of 398 entries in tools/mutate/mutations.json target journal/",
			SpecRule:    "CC1_SyncGroupBarrierEpoch", ExistingCaught: false,
			Trace:         findGeneratedTrace(gen, "CC1_SyncGroupBarrierEpoch"),
			WhyDifference: "tools/mutate covers 398 root/pkg entries on c3335b1 but 0 entries in journal/, leaving regressions in journal/pipeline.go or due.go invisible to `go run ./tools/mutate`.",
		},

		// Class C: Caught by existing mutation/fault/syscall tests, MISSED by abstract SYSSPEC specs (6 cases).
		{
			ID: "C01", Class: ClassExistingOnly, Domain: DomainCorruption,
			Title:       "FastCDC gear table ignores Head.Seed or skips minChunk cut window",
			ExistingRef: "CAUGHT: mutations.json #8/#9/#10 (chunk.go) + TestGearDiffers / TestCut",
			SpecRule:    "", ExistingCaught: true,
			Trace:         validTrace,
			WhyDifference: "Chunking cut-point shifts preserve plaintext equivalence, BLAKE3 chunk IDs, sealing contexts, and commit order, so state-machine specs pass while cross-volume chunk-length privacy breaks.",
		},
		{
			ID: "C02", Class: ClassExistingOnly, Domain: DomainPublication,
			Title:       "Prolly tree node-boundary hash unkeyed (adversarial filename shard-bloat DoS)",
			ExistingRef: "CAUGHT: mutations.json #92 (tree.go) + TestGroundNamesDoNotBloatTheTree",
			SpecRule:    "", ExistingCaught: true,
			Trace:         validTrace,
			WhyDifference: "Tree still maps every key to the exact byte value and deterministic Merkle root, satisfying functional invariants while 2,000 crafted filenames explode from 4 shards to 2,007.",
		},
		{
			ID: "C03", Class: ClassExistingOnly, Domain: DomainPublication,
			Title:       "FUSE handlers run on request context instead of context.WithoutCancel (SQLite EINTR->EIO)",
			ExistingRef: "CAUGHT: mutations.json #116-#120/#133-#136 (pkg/mount) + TestEveryHandlerOutlivesItsRequest",
			SpecRule:    "", ExistingCaught: true,
			Trace:         validTrace,
			WhyDifference: "Signal cancellation semantics at the Linux FUSE / C-runtime boundary lie outside storage state specifications; AST inspection and live signal hammering catch them.",
		},
		{
			ID: "C04", Class: ClassExistingOnly, Domain: DomainJournal,
			Title:       "OS file layer reorders fdatasync, rename, and parent-dir fsync on segment creation",
			ExistingRef: "CAUGHT: journal/trace_linux_test.go (A20 ptrace syscall order check)",
			SpecRule:    "", ExistingCaught: true,
			Trace:         validTrace,
			WhyDifference: "Both SYSSPEC and journal/crash model Disk.Create as atomic on return; only ptrace register inspection catches missing directory fsyncs on real ext4/NVMe.",
		},
		{
			ID: "C05", Class: ClassExistingOnly, Domain: DomainCorruption,
			Title:       "Backend URL parse error leaks embedded credentials in error message string",
			ExistingRef: "CAUGHT: mutations.json #97/#108/#109 (pkg/backend/backend.go) + TestRedaction",
			SpecRule:    "", ExistingCaught: true,
			Trace:         validTrace,
			WhyDifference: "Error-string secret redaction is an implementation hygiene property invisible to protocol transition invariants.",
		},
		{
			ID: "C06", Class: ClassExistingOnly, Domain: DomainJournal,
			Title:       "Media sector bitrot in last <=16 sync rounds before SIGKILL (unmarked tail window)",
			ExistingRef: "CAUGHT/PINNED: journal/cadence_test.go (A25) + journal/flip_test.go (A12)",
			SpecRule:    "", ExistingCaught: true,
			Trace:         validTrace,
			WhyDifference: "Because through-marks are amortized every 16 rounds, Proven lags Durable; post-kill bitrot above Proven looks identical to a torn write and is dropped by design unless markRounds=1.",
		},
	}
}
