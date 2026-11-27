// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package dogi implements the clean-room lifetime classifier and predictors for
// hanzoai/vfs (HIP-1344, Refs #63), inspired by DOGI (USENIX FAST '26):
//
//   - Write-time lifetime prediction (LifetimeEphemeral, LifetimeMedium,
//     LifetimeLong) from strictly write-time observable features (ObjectMeta)
//     without oracle leakage.
//   - Direct integration with the canonical vfs.Volume production packer via
//     vfs.PackLifetimeClassifier (ClassifyWrite), separating chunks into open
//     pack streams while using 100% standard format-v2 128-bit random pack IDs,
//     prolly-tree shards, and fenced Head commits.
//   - Explicit reference OraclePredictor (NoDaP upper bound) and
//     AdversarialPredictor wrappers for comparative and robustness evaluation.
package dogi

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/hanzoai/vfs"
)

// Mode identifies whether the canonical vfs.Volume packer runs with single-stream
// FIFO co-packing or 3-stream lifetime-separated DOGI placement.
type Mode uint8

const (
	// ModeBaselineSingleStream uses 1 open pack stream (standard FIFO co-packing).
	ModeBaselineSingleStream Mode = iota
	// ModeLifetimeSeparated uses 3 open pack streams (one per Lifetime class).
	ModeLifetimeSeparated
)

// String returns a human-readable mode identifier.
func (m Mode) String() string {
	switch m {
	case ModeBaselineSingleStream:
		return "baseline-fifo-1stream"
	case ModeLifetimeSeparated:
		return "dogi-lifetime-3stream"
	default:
		return fmt.Sprintf("mode(%d)", uint8(m))
	}
}

// NumStreams returns the number of active open pack streams for m.
func (m Mode) NumStreams() int {
	if m == ModeLifetimeSeparated {
		return NumLifetimes
	}
	return 1
}

// Lifetime classifies expected object survival time at pack construction.
type Lifetime uint8

const (
	// LifetimeEphemeral covers short-lived build temporaries (e.g. .o, .tmp, /target/, /node_modules/.cache/).
	LifetimeEphemeral Lifetime = 0
	// LifetimeMedium covers intermediate artifacts, test logs, and bundled outputs.
	LifetimeMedium Lifetime = 1
	// LifetimeLong covers durable source code, checkpoints, manifests, and model weights.
	LifetimeLong Lifetime = 2

	// NumLifetimes is the fixed number of lifetime classes supported by DOGI.
	NumLifetimes = 3
)

// String returns the canonical lowercase label for l.
func (l Lifetime) String() string {
	switch l {
	case LifetimeEphemeral:
		return "ephemeral"
	case LifetimeMedium:
		return "medium"
	case LifetimeLong:
		return "long"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(l))
	}
}

// Valid reports whether l is one of the defined lifetime classes.
func (l Lifetime) Valid() bool {
	return l < NumLifetimes
}

// ObjectMeta holds strictly write-time observable metadata for an object.
// It intentionally excludes any ground-truth TTL or true lifetime label so a
// Predictor cannot leak oracle information during held-out evaluation.
type ObjectMeta struct {
	Path          string  // logical path (used only in memory for feature extraction; never persisted plaintext)
	Size          int64   // object payload size in bytes
	OverwriteRate float64 // observed recent overwrite/delete rate in the parent directory or file history
	ProcessTag    string  // optional writer/tool tag (e.g. "rustc", "go", "npm", "ckpt", "agent")
}

// TrainingRecord pairs write-time ObjectMeta with post-hoc ground-truth
// lifetime observed from a training trace or evaluation benchmark.
type TrainingRecord struct {
	Meta         ObjectMeta
	TrueLifetime Lifetime
	TrueTTL      time.Duration
}

// Predictor maps write-time ObjectMeta to a predicted Lifetime class.
type Predictor interface {
	Classify(meta ObjectMeta) Lifetime
}

// Classifier is a thread-safe regularized multinomial logistic regression
// (softmax) model that learns feature weights and class priors from training
// traces via Train and UpdateOnline.
type Classifier struct {
	mu          sync.RWMutex
	weights     map[string][NumLifetimes]float64
	bias        [NumLifetimes]float64
	trained     bool
	samplesSeen int
	lr          float64
	l2          float64
	epochs      int
}

// NewClassifier creates an untrained lifetime Classifier with tuned defaults for
// fast multi-epoch convergence on mixed coding-agent traces.
func NewClassifier() *Classifier {
	return NewClassifierWithConfig(0.25, 1e-4, 35)
}

// NewClassifierWithConfig constructs a Classifier with explicit SGD learning
// rate, L2 regularization weight decay, and batch training epochs.
func NewClassifierWithConfig(lr, l2 float64, epochs int) *Classifier {
	if lr <= 0 {
		lr = 0.25
	}
	if l2 < 0 {
		l2 = 1e-4
	}
	if epochs <= 0 {
		epochs = 35
	}
	return &Classifier{
		weights: make(map[string][NumLifetimes]float64),
		lr:      lr,
		l2:      l2,
		epochs:  epochs,
	}
}

// ExtractFeatures derives normalized sparse categorical features solely from
// write-time observable ObjectMeta (extension, top-level dir token, directory
// tokens, parent dir, overwrite rate bucket, size bucket, and process tag).
func ExtractFeatures(meta ObjectMeta) []string {
	feats := make([]string, 0, 16)

	clean := path.Clean("/" + strings.TrimSpace(meta.Path))
	trimmed := strings.TrimPrefix(clean, "/")
	var segs []string
	if trimmed != "" {
		segs = strings.Split(trimmed, "/")
	}

	base := ""
	if len(segs) > 0 {
		base = segs[len(segs)-1]
	}
	if len(segs) > 1 {
		dirs := segs[:len(segs)-1]
		feats = append(feats, "topdir:"+strings.ToLower(dirs[0]))
		feats = append(feats, "parent:"+strings.ToLower(dirs[len(dirs)-1]))
		seen := make(map[string]struct{}, len(dirs))
		for i, seg := range dirs {
			low := strings.ToLower(seg)
			if _, ok := seen[low]; !ok {
				seen[low] = struct{}{}
				feats = append(feats, "dir:"+low)
			}
			if i > 0 {
				pair := strings.ToLower(dirs[i-1]) + "/" + low
				feats = append(feats, "dir2:"+pair)
			}
		}
	} else {
		feats = append(feats, "topdir:<root>")
	}

	ext := strings.ToLower(path.Ext(base))
	if ext == "" {
		ext = "<none>"
	}
	feats = append(feats, "ext:"+ext)

	owBucket := overwriteBucket(meta.OverwriteRate)
	feats = append(feats, "ow:"+owBucket)
	feats = append(feats, "size:"+sizeBucket(meta.Size))
	feats = append(feats, "ext+ow:"+ext+"+"+owBucket)

	if tag := strings.ToLower(strings.TrimSpace(meta.ProcessTag)); tag != "" {
		feats = append(feats, "proc:"+tag)
	}
	return feats
}

func overwriteBucket(rate float64) string {
	switch {
	case rate <= 0.05:
		return "none"
	case rate <= 0.25:
		return "low"
	case rate <= 0.65:
		return "mid"
	default:
		return "high"
	}
}

func sizeBucket(sz int64) string {
	switch {
	case sz <= 4<<10:
		return "tiny"
	case sz <= 64<<10:
		return "small"
	case sz <= 512<<10:
		return "medium"
	case sz <= 4<<20:
		return "large"
	default:
		return "huge"
	}
}

// Train resets learned weights and fits the softmax model solely from samples.
// Training uses a deterministic seeded permutation so results are reproducible.
func (c *Classifier) Train(samples []TrainingRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.weights = make(map[string][NumLifetimes]float64)
	c.bias = [NumLifetimes]float64{}
	c.trained = false
	c.samplesSeen = 0

	valid := make([]TrainingRecord, 0, len(samples))
	for _, s := range samples {
		if s.TrueLifetime.Valid() {
			valid = append(valid, s)
		}
	}
	if len(valid) == 0 {
		return
	}

	// Pre-extract feature vectors once to make multi-epoch SGD fast.
	featCache := make([][]string, len(valid))
	for i, rec := range valid {
		featCache[i] = ExtractFeatures(rec.Meta)
	}

	idx := make([]int, len(valid))
	for i := range idx {
		idx[i] = i
	}
	rng := rand.New(rand.NewPCG(0x646f6769, 0x1344))

	for epoch := range c.epochs {
		eta := c.lr / (1.0 + 0.05*float64(epoch))
		for i := len(idx) - 1; i > 0; i-- {
			j := rng.IntN(i + 1)
			idx[i], idx[j] = idx[j], idx[i]
		}
		for _, k := range idx {
			c.stepLocked(featCache[k], valid[k].TrueLifetime, eta)
		}
	}

	c.trained = true
	c.samplesSeen = len(valid)
}

// UpdateOnline applies a single online gradient update when an object's true
// lifetime becomes observable (for example upon deletion or expiry).
func (c *Classifier) UpdateOnline(rec TrainingRecord) {
	if !rec.TrueLifetime.Valid() {
		return
	}
	feats := ExtractFeatures(rec.Meta)
	c.mu.Lock()
	defer c.mu.Unlock()
	eta := c.lr * 0.25
	c.stepLocked(feats, rec.TrueLifetime, eta)
	c.trained = true
	c.samplesSeen++
}

func (c *Classifier) stepLocked(feats []string, target Lifetime, eta float64) {
	probs := c.predictProbaLocked(feats)
	for cls := range NumLifetimes {
		y := 0.0
		if cls == int(target) {
			y = 1.0
		}
		grad := probs[cls] - y
		c.bias[cls] -= eta * grad
		for _, f := range feats {
			w := c.weights[f]
			w[cls] -= eta * (grad + c.l2*w[cls])
			c.weights[f] = w
		}
	}
}

func (c *Classifier) predictProbaLocked(feats []string) [NumLifetimes]float64 {
	var logits [NumLifetimes]float64
	for cls := range NumLifetimes {
		logits[cls] = c.bias[cls]
	}
	for _, f := range feats {
		if w, ok := c.weights[f]; ok {
			for cls := range NumLifetimes {
				logits[cls] += w[cls]
			}
		}
	}
	maxL := logits[0]
	for cls := 1; cls < NumLifetimes; cls++ {
		if logits[cls] > maxL {
			maxL = logits[cls]
		}
	}
	var sum float64
	var probs [NumLifetimes]float64
	for cls := range NumLifetimes {
		e := math.Exp(logits[cls] - maxL)
		probs[cls] = e
		sum += e
	}
	if sum == 0 {
		return [NumLifetimes]float64{1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0}
	}
	for cls := range NumLifetimes {
		probs[cls] /= sum
	}
	return probs
}

// PredictProba returns the learned posterior probabilities across the three
// lifetime classes for meta. If untrained, it returns a uniform distribution.
func (c *Classifier) PredictProba(meta ObjectMeta) [NumLifetimes]float64 {
	feats := ExtractFeatures(meta)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.trained {
		return [NumLifetimes]float64{1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0}
	}
	return c.predictProbaLocked(feats)
}

// Classify predicts the most likely Lifetime solely from write-time ObjectMeta.
// Before Train or UpdateOnline is called, Classify returns LifetimeMedium.
func (c *Classifier) Classify(meta ObjectMeta) Lifetime {
	feats := ExtractFeatures(meta)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.trained {
		return LifetimeMedium
	}
	probs := c.predictProbaLocked(feats)
	best := LifetimeEphemeral
	bestP := probs[0]
	for cls := 1; cls < NumLifetimes; cls++ {
		if probs[cls] > bestP {
			bestP = probs[cls]
			best = Lifetime(cls)
		}
	}
	return best
}

// IsTrained reports whether the Classifier has learned from at least one sample.
func (c *Classifier) IsTrained() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.trained
}

// SamplesSeen returns the cumulative number of training records processed.
func (c *Classifier) SamplesSeen() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.samplesSeen
}

// OverwriteRateFromCount converts an observed per-inode write/overwrite count
// into a normalized overwrite rate in [0.0, 1.0] for ObjectMeta feature extraction.
func OverwriteRateFromCount(overwrites uint32) float64 {
	switch {
	case overwrites <= 1:
		return 0.02
	case overwrites == 2:
		return 0.20
	case overwrites == 3:
		return 0.45
	default:
		return 0.85
	}
}

// ClassifyWrite implements vfs.PackLifetimeClassifier for direct integration
// with the canonical vfs.Volume production packer.
func (c *Classifier) ClassifyWrite(pathHint string, _ vfs.InodeID, size uint64, overwrites uint32) int {
	if c == nil {
		return int(LifetimeMedium)
	}
	cls := c.Classify(ObjectMeta{
		Path:          pathHint,
		Size:          int64(size),
		OverwriteRate: OverwriteRateFromCount(overwrites),
	})
	if !cls.Valid() {
		return int(LifetimeMedium)
	}
	return int(cls)
}

// ClassifyOracle returns rec.TrueLifetime directly. It exists ONLY as an
// explicit labeled upper-bound reference mode (analogous to NoDaP in DOGI) and
// is separated by type signature from Classify(ObjectMeta).
func ClassifyOracle(rec TrainingRecord) Lifetime {
	if !rec.TrueLifetime.Valid() {
		return LifetimeMedium
	}
	return rec.TrueLifetime
}

// OraclePredictor is an explicit labeled upper-bound reference Predictor that
// looks up ground-truth labels by logical path. Do NOT use for production
// evaluation of learned predictors.
type OraclePredictor struct {
	Truth map[string]Lifetime
}

// Classify returns the ground-truth lifetime recorded for meta.Path, defaulting
// to LifetimeMedium if absent.
func (o *OraclePredictor) Classify(meta ObjectMeta) Lifetime {
	if o == nil || o.Truth == nil {
		return LifetimeMedium
	}
	clean := path.Clean("/" + strings.TrimSpace(meta.Path))
	if cls, ok := o.Truth[clean]; ok && cls.Valid() {
		return cls
	}
	if cls, ok := o.Truth[meta.Path]; ok && cls.Valid() {
		return cls
	}
	return LifetimeMedium
}

// ClassifyWrite implements vfs.PackLifetimeClassifier for explicit oracle
// upper-bound reference runs on a canonical vfs.Volume.
func (o *OraclePredictor) ClassifyWrite(pathHint string, _ vfs.InodeID, size uint64, overwrites uint32) int {
	return int(o.Classify(ObjectMeta{
		Path:          pathHint,
		Size:          int64(size),
		OverwriteRate: OverwriteRateFromCount(overwrites),
	}))
}

// AdversarialPredictor wraps a base Predictor and corrupts predictions at
// FlipRate (in [0.0, 1.0]) to evaluate packer robustness under misprediction.
type AdversarialPredictor struct {
	Base     Predictor
	FlipRate float64

	mu  sync.Mutex
	rng *rand.Rand
}

// NewAdversarialPredictor constructs a deterministic noisy Predictor that flips
// Base's output to a different lifetime class with probability flipRate.
func NewAdversarialPredictor(base Predictor, flipRate float64, seed uint64) *AdversarialPredictor {
	if flipRate < 0 {
		flipRate = 0
	}
	if flipRate > 1 {
		flipRate = 1
	}
	return &AdversarialPredictor{
		Base:     base,
		FlipRate: flipRate,
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// Classify invokes Base.Classify and flips the predicted class with FlipRate.
func (a *AdversarialPredictor) Classify(meta ObjectMeta) Lifetime {
	pred := LifetimeMedium
	if a.Base != nil {
		pred = a.Base.Classify(meta)
	}
	if !pred.Valid() {
		pred = LifetimeMedium
	}
	if a.FlipRate <= 0 {
		return pred
	}
	a.mu.Lock()
	u := a.rng.Float64()
	shift := 1 + a.rng.IntN(NumLifetimes-1)
	a.mu.Unlock()
	if u < a.FlipRate {
		return Lifetime((int(pred) + shift) % NumLifetimes)
	}
	return pred
}

// ClassifyWrite implements vfs.PackLifetimeClassifier for adversarial
// misprediction robustness runs on a canonical vfs.Volume.
func (a *AdversarialPredictor) ClassifyWrite(pathHint string, _ vfs.InodeID, size uint64, overwrites uint32) int {
	return int(a.Classify(ObjectMeta{
		Path:          pathHint,
		Size:          int64(size),
		OverwriteRate: OverwriteRateFromCount(overwrites),
	}))
}

// EvalReport summarizes classification accuracy and error distribution on a
// held-out evaluation trace.
type EvalReport struct {
	Total                int                             `json:"total"`
	Correct              int                             `json:"correct"`
	Accuracy             float64                         `json:"accuracy"`
	Confusion            [NumLifetimes][NumLifetimes]int `json:"confusion"`
	PerClassPrecision    [NumLifetimes]float64           `json:"per_class_precision"`
	PerClassRecall       [NumLifetimes]float64           `json:"per_class_recall"`
	SevereMispredictions int                             `json:"severe_mispredictions"` // Ephemeral <-> Long swaps
}

// EvaluateClassifier runs p against records and reports accuracy, confusion
// matrix, per-class precision/recall, and severe (Ephemeral <-> Long) swaps.
func EvaluateClassifier(p Predictor, records []TrainingRecord) EvalReport {
	var rep EvalReport
	for _, rec := range records {
		if !rec.TrueLifetime.Valid() {
			continue
		}
		pred := p.Classify(rec.Meta)
		if !pred.Valid() {
			pred = LifetimeMedium
		}
		rep.Total++
		rep.Confusion[rec.TrueLifetime][pred]++
		if pred == rec.TrueLifetime {
			rep.Correct++
		}
		if (rec.TrueLifetime == LifetimeEphemeral && pred == LifetimeLong) ||
			(rec.TrueLifetime == LifetimeLong && pred == LifetimeEphemeral) {
			rep.SevereMispredictions++
		}
	}
	if rep.Total > 0 {
		rep.Accuracy = float64(rep.Correct) / float64(rep.Total)
	}
	for c := range NumLifetimes {
		tp := rep.Confusion[c][c]
		var predTotal, trueTotal int
		for k := range NumLifetimes {
			predTotal += rep.Confusion[k][c]
			trueTotal += rep.Confusion[c][k]
		}
		if predTotal > 0 {
			rep.PerClassPrecision[c] = float64(tp) / float64(predTotal)
		}
		if trueTotal > 0 {
			rep.PerClassRecall[c] = float64(tp) / float64(trueTotal)
		}
	}
	return rep
}
