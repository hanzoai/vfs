// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package dogi_test

import (
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/dogi"
)

func TestClassifierHeldOutAndNoOracleLeakage(t *testing.T) {
	trace := dogi.GenerateAgentTrace(0x1344, 10, 25, 22<<10)
	if len(trace.TrainRecords) == 0 || len(trace.HeldOutRecords) == 0 {
		t.Fatalf("empty splits: train=%d heldout=%d", len(trace.TrainRecords), len(trace.HeldOutRecords))
	}

	clf := dogi.NewClassifier()
	if clf.IsTrained() {
		t.Fatal("fresh classifier should not be trained")
	}
	// Untrained neutral prediction.
	if got := clf.Classify(trace.HeldOutRecords[0].Meta); got != dogi.LifetimeMedium {
		t.Fatalf("untrained Classify = %v, want medium", got)
	}

	clf.Train(trace.TrainRecords)
	if !clf.IsTrained() || clf.SamplesSeen() != len(trace.TrainRecords) {
		t.Fatalf("trained=%v samples=%d want %d", clf.IsTrained(), clf.SamplesSeen(), len(trace.TrainRecords))
	}

	eval := dogi.EvaluateClassifier(clf, trace.HeldOutRecords)
	if eval.Accuracy < 0.85 {
		t.Fatalf("held-out accuracy = %.3f (< 0.85), confusion=%v", eval.Accuracy, eval.Confusion)
	}

	// Verify ObjectMeta carries no ground truth and ClassifyOracle is strictly explicit.
	for i := range 10 {
		rec := trace.HeldOutRecords[i]
		if got := dogi.ClassifyOracle(rec); got != rec.TrueLifetime {
			t.Fatalf("ClassifyOracle = %v, want %v", got, rec.TrueLifetime)
		}
		probs := clf.PredictProba(rec.Meta)
		sum := probs[0] + probs[1] + probs[2]
		if sum < 0.999 || sum > 1.001 {
			t.Fatalf("PredictProba sum = %v", sum)
		}
		stream := clf.ClassifyWrite(rec.Meta.Path, vfs.InodeID(i+2), uint64(rec.Meta.Size), dogi.OverwriteCountFromRate(rec.Meta.OverwriteRate))
		if stream < 0 || stream >= dogi.NumLifetimes {
			t.Fatalf("ClassifyWrite returned invalid stream %d", stream)
		}
	}

	// Online update smoke test.
	clf.UpdateOnline(trace.HeldOutRecords[0])
	if clf.SamplesSeen() != len(trace.TrainRecords)+1 {
		t.Fatalf("SamplesSeen after online update = %d", clf.SamplesSeen())
	}
}

func TestClassifierAdversarialNoiseEvaluation(t *testing.T) {
	trace := dogi.GenerateAgentTrace(0xBEEF, 8, 20, 22<<10)
	clf := dogi.NewClassifier()
	clf.Train(trace.TrainRecords)

	clean := dogi.EvaluateClassifier(clf, trace.HeldOutRecords)
	noisy := dogi.EvaluateClassifier(dogi.NewAdversarialPredictor(clf, 0.35, 99), trace.HeldOutRecords)
	if noisy.Accuracy >= clean.Accuracy {
		t.Fatalf("adversarial 35%% noise did not degrade accuracy: clean=%.3f noisy=%.3f", clean.Accuracy, noisy.Accuracy)
	}
}
