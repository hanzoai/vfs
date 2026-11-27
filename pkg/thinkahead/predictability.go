// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package thinkahead

// PrefixMetric measures how well the first K unique chunks of Run N predict
// Run N+1:
//   - ReachRatio: fraction of Run N's first K unique chunks that Run N+1 reads at any point.
//   - SamePrefixRatio: fraction of Run N's first K unique chunks that also appear
//     within the first K unique chunks of Run N+1.
type PrefixMetric struct {
	K               int     `json:"k"`
	ReachRatio      float64 `json:"reach_ratio"`
	SamePrefixRatio float64 `json:"same_prefix_ratio"`
}

// PredictabilityReport quantifies how predictable Run N+1 is from Run N across
// set membership, byte coverage, ordered prefixes, and Markov/windowed
// transitions.
type PredictabilityReport struct {
	Workflow                 string          `json:"workflow"`
	Provenance               TraceProvenance `json:"provenance"`
	Scenario                 string          `json:"scenario"`
	RunNUniqueChunks         int             `json:"run_n_unique_chunks"`
	RunN1UniqueChunks        int             `json:"run_n1_unique_chunks"`
	SharedChunks             int             `json:"shared_chunks"`
	UnionChunks              int             `json:"union_chunks"`
	JaccardSimilarity        float64         `json:"jaccard_similarity"`
	RunN1CoveredByRunN       float64         `json:"run_n1_covered_by_run_n"`
	RunNUsedByRunN1          float64         `json:"run_n_used_by_run_n1"`
	ByteCoverageRatio        float64         `json:"byte_coverage_ratio"`
	TransitionPredictability float64         `json:"transition_predictability"`
	Window8Predictability    float64         `json:"window8_predictability"`
	Window32Predictability   float64         `json:"window32_predictability"`
	PrefixOverlaps           []PrefixMetric  `json:"prefix_overlaps"`
}

// AnalyzePredictability computes run-to-run predictability metrics from runN
// (prior run) to runN1 (subsequent run).
func AnalyzePredictability(runN, runN1 *WorkflowTrace, scenario string) PredictabilityReport {
	rep := PredictabilityReport{
		Workflow:          runN1.Scope.WorkflowName,
		Provenance:        runN1.Provenance,
		Scenario:          scenario,
		RunNUniqueChunks:  len(runN.UniqueOrder),
		RunN1UniqueChunks: len(runN1.UniqueOrder),
	}
	if len(runN.UniqueOrder) == 0 || len(runN1.UniqueOrder) == 0 {
		return rep
	}

	posN := make(map[ChunkID]int, len(runN.UniqueOrder))
	for i, id := range runN.UniqueOrder {
		posN[id] = i
	}
	posN1 := make(map[ChunkID]int, len(runN1.UniqueOrder))
	for i, id := range runN1.UniqueOrder {
		posN1[id] = i
	}

	// Track chunk byte sizes in runN1 for exact byte coverage.
	sizeN1 := make(map[ChunkID]int64, len(runN1.UniqueOrder))
	for _, ev := range runN1.Events {
		if _, ok := sizeN1[ev.ChunkID]; !ok {
			sizeN1[ev.ChunkID] = int64(ev.Length)
		}
	}

	var shared int
	var coveredBytes, totalN1Bytes int64
	for _, id := range runN1.UniqueOrder {
		sz := sizeN1[id]
		totalN1Bytes += sz
		if _, ok := posN[id]; ok {
			shared++
			coveredBytes += sz
		}
	}
	union := len(runN.UniqueOrder) + len(runN1.UniqueOrder) - shared
	rep.SharedChunks = shared
	rep.UnionChunks = union
	if union > 0 {
		rep.JaccardSimilarity = float64(shared) / float64(union)
	}
	rep.RunN1CoveredByRunN = float64(shared) / float64(len(runN1.UniqueOrder))
	rep.RunNUsedByRunN1 = float64(shared) / float64(len(runN.UniqueOrder))
	if totalN1Bytes > 0 {
		rep.ByteCoverageRatio = float64(coveredBytes) / float64(totalN1Bytes)
	}

	// Prefix overlap metrics at K = 10, 50, 100, 500, 1000.
	ks := []int{10, 50, 100, 500, 1000}
	for _, k := range ks {
		effK := min(k, len(runN.UniqueOrder))
		if effK == 0 {
			continue
		}
		var reach, samePrefix int
		for i := 0; i < effK; i++ {
			id := runN.UniqueOrder[i]
			if p, ok := posN1[id]; ok {
				reach++
				if p < effK {
					samePrefix++
				}
			}
		}
		rep.PrefixOverlaps = append(rep.PrefixOverlaps, PrefixMetric{
			K:               k,
			ReachRatio:      float64(reach) / float64(effK),
			SamePrefixRatio: float64(samePrefix) / float64(effK),
		})
	}

	// Markov-1 exact transition overlap & lookahead window predictability
	// (W = 8, 32) on first-touch unique sequences.
	type edge struct {
		from, to ChunkID
	}
	transN := make(map[edge]struct{}, len(runN.UniqueOrder))
	for i := 0; i+1 < len(runN.UniqueOrder); i++ {
		transN[edge{runN.UniqueOrder[i], runN.UniqueOrder[i+1]}] = struct{}{}
	}

	totalTrans := len(runN1.UniqueOrder) - 1
	if totalTrans > 0 {
		var exactHits, win8Hits, win32Hits int
		for i := 0; i+1 < len(runN1.UniqueOrder); i++ {
			u, v := runN1.UniqueOrder[i], runN1.UniqueOrder[i+1]
			if _, ok := transN[edge{u, v}]; ok {
				exactHits++
			}
			pu, okU := posN[u]
			pv, okV := posN[v]
			if okU && okV {
				diff := pv - pu
				if diff > 0 && diff <= 8 {
					win8Hits++
				}
				if diff > 0 && diff <= 32 {
					win32Hits++
				}
			}
		}
		rep.TransitionPredictability = float64(exactHits) / float64(totalTrans)
		rep.Window8Predictability = float64(win8Hits) / float64(totalTrans)
		rep.Window32Predictability = float64(win32Hits) / float64(totalTrans)
	}

	return rep
}
