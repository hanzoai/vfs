// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sysspec_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/pkg/sysspec"
)

func TestDAGTopologicalOrderAndImpact(t *testing.T) {
	reg := sysspec.DefaultRegistry()
	order, err := reg.TopologicalOrder()
	if err != nil {
		t.Fatalf("TopologicalOrder: %v", err)
	}
	want := []sysspec.Domain{
		sysspec.DomainCorruption,
		sysspec.DomainFence,
		sysspec.DomainPublication,
		sysspec.DomainJournal,
	}
	if !slices.Equal(order, want) {
		t.Fatalf("TopologicalOrder = %v, want %v", order, want)
	}

	if got := reg.AffectedModules(sysspec.DomainCorruption); len(got) != 4 {
		t.Fatalf("AffectedModules(corruption) = %v, want all 4 modules", got)
	}
	if got := reg.AffectedModules(sysspec.DomainFence); !slices.Equal(got, []sysspec.Domain{
		sysspec.DomainFence, sysspec.DomainPublication, sysspec.DomainJournal,
	}) {
		t.Fatalf("AffectedModules(fence) = %v", got)
	}
	if got := reg.AffectedModules(sysspec.DomainPublication); !slices.Equal(got, []sysspec.Domain{
		sysspec.DomainPublication, sysspec.DomainJournal,
	}) {
		t.Fatalf("AffectedModules(publication) = %v", got)
	}
	if got := reg.AffectedModules(sysspec.DomainJournal); !slices.Equal(got, []sysspec.Domain{
		sysspec.DomainJournal,
	}) {
		t.Fatalf("AffectedModules(journal) = %v", got)
	}
}

func TestGeneratedRegressionSuiteCoversAllRules(t *testing.T) {
	reg := sysspec.DefaultRegistry()
	invN, trN, rgN, ccN := reg.RuleCounts()
	totalRules := invN + trN + rgN + ccN
	if totalRules != 38 {
		t.Fatalf("RuleCounts total = %d (%d inv, %d tr, %d rg, %d cc), want 38",
			totalRules, invN, trN, rgN, ccN)
	}

	suite := sysspec.GenerateRegressionSuite(1349)
	triggered := map[string]bool{}
	validCount := 0
	for _, tc := range suite {
		vs := reg.CheckTrace(tc.Trace)
		if tc.ExpectRule == "" {
			validCount++
			if len(vs) != 0 {
				t.Errorf("%s: unexpected violation(s) on valid trace: %v", tc.Name, vs)
			}
			continue
		}
		found := false
		for _, v := range vs {
			if v.RuleID == tc.ExpectRule {
				found = true
				triggered[v.RuleID] = true
			}
		}
		if !found {
			t.Errorf("%s: expected rule %s not triggered; got %v", tc.Name, tc.ExpectRule, vs)
		}
	}
	if validCount < 12 {
		t.Fatalf("valid traces = %d, want at least 12", validCount)
	}
	if len(triggered) != totalRules {
		t.Fatalf("triggered %d distinct rules out of %d", len(triggered), totalRules)
	}
}

func TestContextFramingIsInjective(t *testing.T) {
	a := sysspec.CanonicalContextFrame("head", []byte("ab"))
	b := sysspec.CanonicalContextFrame("he", []byte("adab"))
	if bytes.Equal(a, b) {
		t.Fatal("distinct kind/field boundaries produced identical context frame")
	}

	var vol [16]byte
	var cid [32]byte
	vol[0], cid[0] = 1, 2
	c1 := sysspec.CanonicalContextFrame("chunk", vol[:], cid[:])
	c2 := sysspec.CanonicalContextFrame("chunk", append(vol[:], cid[:8]...), cid[8:])
	if bytes.Equal(c1, c2) {
		t.Fatal("distinct field splits collided in CanonicalContextFrame")
	}
}

func TestConcurrentTraceVerificationIsRaceFree(t *testing.T) {
	reg := sysspec.DefaultRegistry()
	suite := sysspec.GenerateRegressionSuite(2026)
	var wg sync.WaitGroup
	for g := range 12 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i, tc := range suite {
				if (i+worker)%2 != 0 {
					continue
				}
				vs := reg.CheckTrace(tc.Trace)
				if tc.ExpectRule == "" && len(vs) != 0 {
					t.Errorf("worker %d case %s: %v", worker, tc.Name, vs)
				}
				if tc.ExpectRule != "" && len(vs) == 0 {
					t.Errorf("worker %d case %s: missed %s", worker, tc.Name, tc.ExpectRule)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestComparativeMatrixBidirectionalAcceptance(t *testing.T) {
	mutPath := filepath.Join("..", "..", "tools", "mutate", "mutations.json")
	raw, err := os.ReadFile(mutPath)
	if err != nil {
		t.Fatalf("read %s: %v", mutPath, err)
	}
	rep, err := sysspec.BuildComparativeMatrix(raw)
	if err != nil {
		t.Fatalf("BuildComparativeMatrix: %v", err)
	}
	if rep.UpstreamMutations.Total < 200 {
		t.Fatalf("expected >= 200 upstream mutations, got %d", rep.UpstreamMutations.Total)
	}
	if rep.UpstreamMutations.JournalMutations != 0 {
		t.Logf("journal mutations in mutations.json = %d", rep.UpstreamMutations.JournalMutations)
	}
	if rep.GeneratedPassRatePct != 100.0 {
		t.Fatalf("GeneratedPassRatePct = %.2f%%, want 100%%", rep.GeneratedPassRatePct)
	}
	if rep.BothCatchCount == 0 || rep.SpecOnlyCount == 0 || rep.ExistingOnlyCount == 0 {
		t.Fatalf("incomplete matrix counts: both=%d spec_only=%d existing_only=%d",
			rep.BothCatchCount, rep.SpecOnlyCount, rep.ExistingOnlyCount)
	}
	for _, c := range rep.Cases {
		switch c.Class {
		case sysspec.ClassBothCatch:
			if !c.SpecCaught || !c.ExistingCaught {
				t.Errorf("%s (%s): expected caught by both, got spec=%v existing=%v",
					c.ID, c.Title, c.SpecCaught, c.ExistingCaught)
			}
		case sysspec.ClassSpecOnly:
			if !c.SpecCaught || c.ExistingCaught {
				t.Errorf("%s (%s): expected spec-only, got spec=%v existing=%v",
					c.ID, c.Title, c.SpecCaught, c.ExistingCaught)
			}
		case sysspec.ClassExistingOnly:
			if c.SpecCaught || !c.ExistingCaught {
				t.Errorf("%s (%s): expected existing-only reverse control, got spec=%v existing=%v",
					c.ID, c.Title, c.SpecCaught, c.ExistingCaught)
			}
		}
	}
}
