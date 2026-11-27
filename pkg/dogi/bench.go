// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package dogi

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
)

// ProbeBackend wraps a backend.Backend to count every real object-store call
// and physical bytes written/deleted on canonical pack/ and meta/ objects,
// plus optional explicitly labeled simulated per-request delay.
type ProbeBackend struct {
	backend.Backend
	SimDelay time.Duration

	Gets       atomic.Int64
	GetRanges  atomic.Int64
	Puts       atomic.Int64
	PutAbsents atomic.Int64
	PutVers    atomic.Int64
	Deletes    atomic.Int64
	Stats      atomic.Int64

	PackPuts         atomic.Int64
	PackBytesWritten atomic.Int64
	PackDeletes      atomic.Int64
}

// NewProbeBackend wraps be with real request counting and optional simulated delay.
func NewProbeBackend(be backend.Backend, simDelay time.Duration) *ProbeBackend {
	return &ProbeBackend{Backend: be, SimDelay: simDelay}
}

func (p *ProbeBackend) delay() {
	if p.SimDelay > 0 {
		time.Sleep(p.SimDelay)
	}
}

func (p *ProbeBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	p.delay()
	p.Gets.Add(1)
	return p.Backend.Get(ctx, key, max)
}

func (p *ProbeBackend) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	p.delay()
	p.GetRanges.Add(1)
	return p.Backend.GetRange(ctx, key, off, n)
}

func (p *ProbeBackend) Put(ctx context.Context, key string, data []byte) error {
	p.delay()
	p.Puts.Add(1)
	if strings.HasPrefix(key, "pack/") {
		p.PackPuts.Add(1)
		p.PackBytesWritten.Add(int64(len(data)))
	}
	return p.Backend.Put(ctx, key, data)
}

func (p *ProbeBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	p.delay()
	p.PutAbsents.Add(1)
	if strings.HasPrefix(key, "pack/") {
		p.PackPuts.Add(1)
		p.PackBytesWritten.Add(int64(len(data)))
	}
	return p.Backend.PutIfAbsent(ctx, key, data)
}

func (p *ProbeBackend) PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error) {
	p.delay()
	p.PutVers.Add(1)
	return p.Backend.PutIfVersion(ctx, key, data, v)
}

func (p *ProbeBackend) Delete(ctx context.Context, key string) error {
	p.delay()
	p.Deletes.Add(1)
	if strings.HasPrefix(key, "pack/") {
		p.PackDeletes.Add(1)
	}
	return p.Backend.Delete(ctx, key)
}

func (p *ProbeBackend) Stat(ctx context.Context, key string) (int64, error) {
	p.delay()
	p.Stats.Add(1)
	return p.Backend.Stat(ctx, key)
}

// TotalRequests returns the sum of all tracked backend object operations.
func (p *ProbeBackend) TotalRequests() int64 {
	return p.Gets.Load() + p.GetRanges.Load() + p.Puts.Load() +
		p.PutAbsents.Load() + p.PutVers.Load() + p.Deletes.Load() + p.Stats.Load()
}

// TraceOp represents a single deterministic operation in a synthetic coding-agent
// build and workspace trace executed against a canonical vfs.Volume.
type TraceOp struct {
	Round        int
	Kind         string // "put", "delete", "checkpoint", "compact"
	Key          string
	Meta         ObjectMeta
	TrueLifetime Lifetime
	TrueTTL      time.Duration
	Payload      []byte
}

// AgentTrace holds a chronologically split 40% training / 60% held-out workload
// plus a zero-expiration negative control trace.
type AgentTrace struct {
	TrainRecords      []TrainingRecord
	HeldOutRecords    []TrainingRecord
	HeldOutOps        []TraceOp
	ZeroExpirationOps []TraceOp
	OracleTruth       map[string]Lifetime
}

// GenerateAgentTrace constructs a deterministic coding-agent build + workspace
// workload split chronologically 40% train / 60% held-out. Every file size is
// above vfs's 16 KiB inline threshold so all files go through the canonical
// FastCDC chunker and pack builder (`pack/<2hex>/<128-bit-random-id>`).
func GenerateAgentTrace(seed uint64, totalRounds, filesPerRound, avgChunkSize int) AgentTrace {
	if totalRounds < 5 {
		totalRounds = 10
	}
	if filesPerRound < 8 {
		filesPerRound = 16
	}
	if avgChunkSize < 20<<10 {
		avgChunkSize = 24 << 10
	}
	trainRounds := max(2, (totalRounds*4)/10)
	rng := rand.New(rand.NewPCG(seed, seed^0x517cc1b727220a95))

	trainWorkspaces := []string{"ws-alpha", "ws-beta"}
	evalWorkspaces := []string{"ws-gamma", "ws-delta", "ws-epsilon"}

	var (
		trainRecs   []TrainingRecord
		heldOutRecs []TrainingRecord
		heldOutOps  []TraceOp
		zeroExpOps  []TraceOp
		oracleTruth = make(map[string]Lifetime)
	)

	expireAt := make(map[int][]string)
	fileSeq := 0

	makePayload := func(size int, tag uint64) []byte {
		b := make([]byte, size)
		r := rand.New(rand.NewPCG(tag, seed+17))
		for i := 0; i < len(b); i += 8 {
			v := r.Uint64()
			for j := 0; j < 8 && i+j < len(b); j++ {
				b[i+j] = byte(v >> (8 * j))
			}
		}
		return b
	}

	sampleObject := func(round int, ws string, seq int) (key string, meta ObjectMeta, cls Lifetime, ttl time.Duration, sz int) {
		roll := rng.Float64()
		jitter := rng.Float64()
		switch {
		case roll < 0.58:
			// Ephemeral build temporaries (~58% of writes): .o, .tmp, .d, /target/, /node_modules/.cache/
			cls = LifetimeEphemeral
			ttl = time.Duration(30+rng.IntN(90)) * time.Second
			sub := seq % 4
			sz = max(18<<10, (avgChunkSize*3)/4+rng.IntN(avgChunkSize/2))
			switch sub {
			case 0:
				key = fmt.Sprintf("/%s/target/debug/deps/crate_%04d.o", ws, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: 0.72 + rng.Float64()*0.25, ProcessTag: "rustc"}
			case 1:
				key = fmt.Sprintf("/%s/node_modules/.cache/babel/chunk_%04d.tmp", ws, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: 0.70 + rng.Float64()*0.25, ProcessTag: "npm"}
			case 2:
				key = fmt.Sprintf("/%s/tmp/build_r%02d/unit_%04d.d", ws, round, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: 0.78 + rng.Float64()*0.20, ProcessTag: "go"}
			default:
				key = fmt.Sprintf("/%s/tmp/scratch/run_%04d.log", ws, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: 0.75 + rng.Float64()*0.20, ProcessTag: "agent"}
			}
			if jitter < 0.04 {
				cls = LifetimeMedium
				ttl = 15 * time.Minute
			}

		case roll < 0.80:
			// Medium-lived build intermediates & artifacts (~22% of writes)
			cls = LifetimeMedium
			ttl = time.Duration(10+rng.IntN(25)) * time.Minute
			sub := seq % 3
			sz = max(20<<10, avgChunkSize+rng.IntN(avgChunkSize/2))
			switch sub {
			case 0:
				key = fmt.Sprintf("/%s/dist/bundle_%04d.tar", ws, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: 0.30 + rng.Float64()*0.20, ProcessTag: "ci"}
			case 1:
				key = fmt.Sprintf("/%s/artifacts/stage_%04d.bin", ws, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: 0.32 + rng.Float64()*0.25, ProcessTag: "ci"}
			default:
				key = fmt.Sprintf("/%s/build/reports/suite_%04d.log", ws, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: 0.28 + rng.Float64()*0.22, ProcessTag: "go"}
			}
			if jitter < 0.04 {
				cls = LifetimeEphemeral
				ttl = 60 * time.Second
			}

		default:
			// Long-lived source code, model weights, checkpoints (~20% of writes)
			cls = LifetimeLong
			ttl = 24 * time.Hour
			sub := seq % 4
			sz = max(22<<10, avgChunkSize+rng.IntN(avgChunkSize))
			switch sub {
			case 0:
				key = fmt.Sprintf("/%s/src/pkg_%02d/module_%04d.go", ws, seq%16, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: rng.Float64() * 0.04, ProcessTag: "editor"}
			case 1:
				key = fmt.Sprintf("/%s/src/core/engine_%04d.rs", ws, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: rng.Float64() * 0.04, ProcessTag: "editor"}
			case 2:
				key = fmt.Sprintf("/%s/checkpoints/step_%02d/weights_%04d.safetensors", ws, round, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: rng.Float64() * 0.03, ProcessTag: "ckpt"}
			default:
				key = fmt.Sprintf("/%s/models/embed_%04d.bin", ws, seq)
				meta = ObjectMeta{Path: key, Size: int64(sz), OverwriteRate: rng.Float64() * 0.03, ProcessTag: "ckpt"}
			}
			if jitter < 0.03 {
				cls = LifetimeMedium
				ttl = 20 * time.Minute
			}
		}
		return key, meta, cls, ttl, sz
	}

	for r := range totalRounds {
		isTrain := r < trainRounds
		wsPool := evalWorkspaces
		if isTrain {
			wsPool = trainWorkspaces
		}

		for i := range filesPerRound {
			fileSeq++
			ws := wsPool[(r+i)%len(wsPool)]
			key, meta, cls, ttl, sz := sampleObject(r, ws, fileSeq)
			rec := TrainingRecord{
				Meta:         meta,
				TrueLifetime: cls,
				TrueTTL:      ttl,
			}
			if isTrain {
				trainRecs = append(trainRecs, rec)
				continue
			}

			heldOutRecs = append(heldOutRecs, rec)
			oracleTruth[key] = cls
			payload := makePayload(sz, uint64(fileSeq))
			heldOutOps = append(heldOutOps, TraceOp{
				Round:        r,
				Kind:         "put",
				Key:          key,
				Meta:         meta,
				TrueLifetime: cls,
				TrueTTL:      ttl,
				Payload:      payload,
			})

			zeroKey := fmt.Sprintf("/%s/retained%s", ws, key[len("/"+ws):])
			zeroMeta := meta
			zeroMeta.Path = zeroKey
			oracleTruth[zeroKey] = cls
			zeroExpOps = append(zeroExpOps, TraceOp{
				Round:        r,
				Kind:         "put",
				Key:          zeroKey,
				Meta:         zeroMeta,
				TrueLifetime: cls,
				TrueTTL:      24 * time.Hour,
				Payload:      payload,
			})

			switch cls {
			case LifetimeEphemeral:
				expireAt[r] = append(expireAt[r], key)
			case LifetimeMedium:
				expireAt[r+2] = append(expireAt[r+2], key)
			}
		}

		if !isTrain {
			heldOutOps = append(heldOutOps, TraceOp{Round: r, Kind: "checkpoint"})
			zeroExpOps = append(zeroExpOps, TraceOp{Round: r, Kind: "checkpoint"})
			for _, delKey := range expireAt[r] {
				heldOutOps = append(heldOutOps, TraceOp{Round: r, Kind: "delete", Key: delKey})
			}
			heldOutOps = append(heldOutOps, TraceOp{Round: r, Kind: "compact"})
		}
	}

	lastR := totalRounds
	if len(expireAt[lastR]) > 0 || len(expireAt[lastR+1]) > 0 {
		for _, k := range expireAt[lastR] {
			heldOutOps = append(heldOutOps, TraceOp{Round: lastR, Kind: "delete", Key: k})
		}
		for _, k := range expireAt[lastR+1] {
			heldOutOps = append(heldOutOps, TraceOp{Round: lastR, Kind: "delete", Key: k})
		}
		heldOutOps = append(heldOutOps, TraceOp{Round: lastR, Kind: "compact"})
	}

	return AgentTrace{
		TrainRecords:      trainRecs,
		HeldOutRecords:    heldOutRecs,
		HeldOutOps:        heldOutOps,
		ZeroExpirationOps: zeroExpOps,
		OracleTruth:       oracleTruth,
	}
}

func OverwriteCountFromRate(rate float64) uint32 {
	switch {
	case rate <= 0.05:
		return 1
	case rate <= 0.25:
		return 2
	case rate <= 0.65:
		return 3
	default:
		return 5
	}
}

// ExecuteTraceOnVolume drives ops against a canonical vfs.Volume (creating
// parent directories as needed, writing files via Create+Open+WriteAt,
// flushing generations via Flush, removing expired files via Remove, and
// relocating fragmented packs via CompactFragmentedPacks).
func ExecuteTraceOnVolume(ctx context.Context, v *vfs.Volume, ops []TraceOp, maxLiveRatio float64) (
	logicalFiles int64,
	logicalPlainBytes int64,
	compaction vfs.CompactionStats,
	liveFiles map[string][]byte,
	err error,
) {
	dirCache := map[string]vfs.InodeID{"/": vfs.RootInode}
	var ensureDir func(p string) (vfs.InodeID, error)
	ensureDir = func(p string) (vfs.InodeID, error) {
		p = path.Clean("/" + p)
		if id, ok := dirCache[p]; ok {
			return id, nil
		}
		parentPath := path.Dir(p)
		base := path.Base(p)
		pid, err := ensureDir(parentPath)
		if err != nil {
			return 0, err
		}
		if in, lerr := v.Lookup(ctx, pid, base); lerr == nil {
			dirCache[p] = in.ID
			return in.ID, nil
		}
		in, merr := v.Mkdir(ctx, pid, base, 0o755)
		if merr != nil {
			return 0, merr
		}
		dirCache[p] = in.ID
		return in.ID, nil
	}

	liveFiles = make(map[string][]byte)
	for _, op := range ops {
		switch op.Kind {
		case "put":
			clean := path.Clean("/" + op.Key)
			dirPath := path.Dir(clean)
			name := path.Base(clean)
			dirID, derr := ensureDir(dirPath)
			if derr != nil {
				return 0, 0, compaction, nil, derr
			}
			var in vfs.Inode
			if existing, lerr := v.Lookup(ctx, dirID, name); lerr == nil {
				in = existing
			} else {
				created, cerr := v.Create(ctx, dirID, name, 0o644)
				if cerr != nil {
					return 0, 0, compaction, nil, cerr
				}
				in = created
			}
			v.SetPathHint(in.ID, clean, OverwriteCountFromRate(op.Meta.OverwriteRate))
			fh, oerr := v.Open(ctx, in.ID)
			if oerr != nil {
				return 0, 0, compaction, nil, oerr
			}
			if _, werr := fh.WriteAt(ctx, op.Payload, 0); werr != nil {
				_ = fh.Close()
				return 0, 0, compaction, nil, werr
			}
			if cerr := fh.Close(); cerr != nil {
				return 0, 0, compaction, nil, cerr
			}
			liveFiles[clean] = op.Payload
			logicalFiles++
			logicalPlainBytes += int64(len(op.Payload))

		case "delete":
			clean := path.Clean("/" + op.Key)
			dirPath := path.Dir(clean)
			name := path.Base(clean)
			if dirID, ok := dirCache[dirPath]; ok {
				if rerr := v.Remove(ctx, dirID, name); rerr != nil {
					return 0, 0, compaction, nil, rerr
				}
				delete(liveFiles, clean)
			}

		case "checkpoint":
			if ferr := v.Flush(ctx); ferr != nil {
				return 0, 0, compaction, nil, ferr
			}

		case "compact":
			st, cerr := vfs.CompactFragmentedPacks(ctx, v, maxLiveRatio)
			if cerr != nil {
				return 0, 0, compaction, nil, cerr
			}
			compaction.PacksInspected += st.PacksInspected
			compaction.PacksCompacted += st.PacksCompacted
			compaction.PacksDeleted += st.PacksDeleted
			compaction.PacksPinnedSkipped += st.PacksPinnedSkipped
			compaction.NewPacksCreated += st.NewPacksCreated
			compaction.ChunksRelocated += st.ChunksRelocated
			compaction.DeadChunksPruned += st.DeadChunksPruned
			compaction.LiveBytesCopied += st.LiveBytesCopied
			compaction.PhysicalBytesWritten += st.PhysicalBytesWritten
			compaction.PhysicalBytesReclaimed += st.PhysicalBytesReclaimed
		}
	}
	if ferr := v.Flush(ctx); ferr != nil {
		return 0, 0, compaction, nil, ferr
	}
	return logicalFiles, logicalPlainBytes, compaction, liveFiles, nil
}

// ModeMetrics records real measured storage, request, timing, and memory
// telemetry for a single canonical vfs.Volume production packer evaluation run.
type ModeMetrics struct {
	Scenario               string  `json:"scenario"`
	Name                   string  `json:"name"`
	Mode                   string  `json:"mode"`
	NumStreams             int     `json:"num_streams"`
	PackTargetBytes        int     `json:"pack_target_bytes"`
	GarbageRatio           float64 `json:"garbage_ratio"`
	ClassifierAccuracy     float64 `json:"classifier_accuracy"`
	SevereMispredCount     int     `json:"severe_mispred_count"`
	LogicalChunks          int64   `json:"logical_chunks"`
	LogicalPlaintextBytes  int64   `json:"logical_plaintext_bytes"`
	LogicalSealedBytes     int64   `json:"logical_sealed_bytes"`
	PhysicalBytesWritten   int64   `json:"physical_bytes_written"`
	PhysicalBytesReclaimed int64   `json:"physical_bytes_reclaimed"`
	ActivePhysicalBytes    int64   `json:"active_physical_bytes"`
	LiveBytesCopied        int64   `json:"live_bytes_copied"`
	WriteAmplification     float64 `json:"write_amplification"`
	PacksCreated           int64   `json:"packs_created"`
	InitialPacksCreated    int64   `json:"initial_packs_created"`
	CompactionPacksCreated int64   `json:"compaction_packs_created"`
	PacksDeleted           int64   `json:"packs_deleted"`
	PackCountOverheadPct   float64 `json:"pack_count_overhead_pct"`
	LiveBytesReductionPct  float64 `json:"live_bytes_reduction_pct"`
	WAFReductionPct        float64 `json:"waf_reduction_pct"`
	PutRequests            int64   `json:"put_requests"`
	GetRangeRequests       int64   `json:"get_range_requests"`
	DeleteRequests         int64   `json:"delete_requests"`
	TotalBackendRequests   int64   `json:"total_backend_requests"`
	WallMs                 float64 `json:"wall_ms"`
	SimulatedRTTMs         float64 `json:"simulated_rtt_ms"`
	HeapAllocDeltaBytes    uint64  `json:"heap_alloc_delta_bytes"`
}

// SuiteConfig configures RunSuite.
type SuiteConfig struct {
	ScratchDir   string
	FullScale    bool
	SimulatedRTT time.Duration
	Seed         uint64
}

// SuiteReport bundles all measured comparisons across baseline, learned DOGI,
// oracle upper bound, adversarial misprediction sweeps, PackTarget sweeps,
// and negative/boundary controls on the canonical vfs.Volume production packer.
type SuiteReport struct {
	Scale              string        `json:"scale"`
	TrainSamples       int           `json:"train_samples"`
	HeldOutSamples     int           `json:"held_out_samples"`
	HeldOutEval        EvalReport    `json:"held_out_eval"`
	PrimaryRuns        []ModeMetrics `json:"primary_runs"`
	AdversarialSweep   []ModeMetrics `json:"adversarial_sweep"`
	PackTargetSweep    []ModeMetrics `json:"pack_target_sweep"`
	BoundaryControls   []ModeMetrics `json:"boundary_controls"`
	SimulatedRTTChecks []ModeMetrics `json:"simulated_rtt_checks,omitempty"`
}

// RunSuite executes all DOGI comparative benchmarks using the actual canonical
// vfs.Volume production packer, real vfs.Hybrid post-quantum Cipher, and real
// file:// backend stores.
func RunSuite(ctx context.Context, cfg SuiteConfig) (SuiteReport, error) {
	if cfg.Seed == 0 {
		cfg.Seed = 0x20261344
	}
	scratch := cfg.ScratchDir
	if scratch == "" {
		var err error
		scratch, err = os.MkdirTemp("", "dogi-bench-*")
		if err != nil {
			return SuiteReport{}, err
		}
		defer os.RemoveAll(scratch)
	}

	rounds, filesPerRound, avgChunk := 10, 20, 22<<10
	primaryTarget := 64 << 10
	scaleName := "standard"
	if cfg.FullScale {
		rounds, filesPerRound, avgChunk = 12, 32, 28<<10
		primaryTarget = 128 << 10
		scaleName = "full"
	}

	trace := GenerateAgentTrace(cfg.Seed, rounds, filesPerRound, avgChunk)

	// Train strictly on the 40% training split; never expose HeldOutRecords to Train.
	clf := NewClassifier()
	clf.Train(trace.TrainRecords)
	eval := EvaluateClassifier(clf, trace.HeldOutRecords)
	oracle := &OraclePredictor{Truth: trace.OracleTruth}

	key, err := age.GenerateHybridIdentity()
	if err != nil {
		return SuiteReport{}, err
	}
	ciph := vfs.Hybrid(key)

	runOne := func(scenario, name string, mode Mode, pred Predictor, packTarget int, ops []TraceOp, simDelay time.Duration) (ModeMetrics, error) {
		runDir := filepath.Join(scratch, strings.ReplaceAll(scenario+"_"+name, " ", "_"))
		_ = os.RemoveAll(runDir)
		storeDir := filepath.Join(runDir, "store")
		journalDir := filepath.Join(runDir, "journal")
		_ = os.MkdirAll(storeDir, 0o755)
		_ = os.MkdirAll(journalDir, 0o755)

		rawBE, err := backend.Open(ctx, "file://"+filepath.ToSlash(storeDir))
		if err != nil {
			return ModeMetrics{}, err
		}
		defer rawBE.Close()
		probe := NewProbeBackend(rawBE, simDelay)

		var packClf vfs.PackLifetimeClassifier
		if mode == ModeLifetimeSeparated && pred != nil {
			packClf = vfs.PackLifetimeClassifierFunc(func(pathHint string, _ vfs.InodeID, size uint64, overwrites uint32) int {
				return int(pred.Classify(ObjectMeta{
					Path:          pathHint,
					Size:          int64(size),
					OverwriteRate: OverwriteRateFromCount(overwrites),
				}))
			})
		}

		vcfg := vfs.Config{
			Backend:         probe,
			Cipher:          ciph,
			Round:           1,
			Journal:         journalDir,
			JournalCapacity: 64 << 20,
			Bound:           time.Hour,
			PackClassifier:  packClf,
			PackTargetBytes: packTarget,
		}
		if err := vfs.Create(ctx, vcfg); err != nil {
			return ModeMetrics{}, err
		}
		vol, err := vfs.Attach(ctx, vcfg)
		if err != nil {
			return ModeMetrics{}, err
		}
		defer vol.Close()

		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		t0 := time.Now()

		// maxLiveRatio = 0.60 corresponds to GarbageRatio >= 0.40 dead bytes.
		logicalFiles, logicalPlain, compStats, liveFiles, err := ExecuteTraceOnVolume(ctx, vol, ops, 0.60)
		if err != nil {
			return ModeMetrics{}, err
		}
		if err := vol.Check(ctx); err != nil {
			return ModeMetrics{}, fmt.Errorf("vol.Check: %w", err)
		}

		// Verify a standard unmodified read-only Volume (with PackClassifier: nil)
		// opens the checkpoint and reads back surviving files cleanly.
		ro, err := vfs.Open(ctx, vfs.Config{Backend: rawBE, Cipher: ciph})
		if err != nil {
			return ModeMetrics{}, fmt.Errorf("standard vfs.Open: %w", err)
		}
		checked := 0
		for p, want := range liveFiles {
			in, err := ro.Resolve(ctx, p)
			if err != nil {
				_ = ro.Close()
				return ModeMetrics{}, fmt.Errorf("ro.Resolve(%s): %w", p, err)
			}
			fh, err := ro.Open(ctx, in.ID)
			if err != nil {
				_ = ro.Close()
				return ModeMetrics{}, fmt.Errorf("ro.Open(%s): %w", p, err)
			}
			got := make([]byte, in.Size)
			n, rerr := fh.ReadAt(ctx, got, 0)
			_ = fh.Close()
			if (rerr != nil && rerr != io.EOF) || n != len(want) || string(got) != string(want) {
				_ = ro.Close()
				return ModeMetrics{}, fmt.Errorf("ro.ReadAt(%s) mismatch", p)
			}
			checked++
			if checked >= 8 {
				break
			}
		}
		_ = ro.Close()

		wallMs := float64(time.Since(t0).Microseconds()) / 1000.0
		runtime.ReadMemStats(&mAfter)
		var heapDelta uint64
		if mAfter.HeapAlloc > mBefore.HeapAlloc {
			heapDelta = mAfter.HeapAlloc - mBefore.HeapAlloc
		}

		util, err := vfs.MeasurePackUtilization(ctx, vol)
		if err != nil {
			return ModeMetrics{}, err
		}

		physWritten := probe.PackBytesWritten.Load()
		copiedBytes := int64(compStats.LiveBytesCopied)
		logicalSealed := physWritten - copiedBytes
		if logicalSealed <= 0 {
			logicalSealed = logicalPlain
		}
		waf := 1.0
		if logicalSealed > 0 {
			waf = float64(physWritten) / float64(logicalSealed)
		}

		totalPacks := probe.PackPuts.Load()
		compactionPacks := int64(compStats.NewPacksCreated)
		initPacks := totalPacks - compactionPacks
		packsDeleted := int64(compStats.PacksDeleted)

		acc := 0.0
		severe := 0
		if pred != nil {
			ev := EvaluateClassifier(pred, trace.HeldOutRecords)
			acc = ev.Accuracy
			severe = ev.SevereMispredictions
		}

		return ModeMetrics{
			Scenario:               scenario,
			Name:                   name,
			Mode:                   mode.String(),
			NumStreams:             mode.NumStreams(),
			PackTargetBytes:        packTarget,
			GarbageRatio:           0.40,
			ClassifierAccuracy:     acc,
			SevereMispredCount:     severe,
			LogicalChunks:          logicalFiles,
			LogicalPlaintextBytes:  logicalPlain,
			LogicalSealedBytes:     logicalSealed,
			PhysicalBytesWritten:   physWritten,
			PhysicalBytesReclaimed: int64(compStats.PhysicalBytesReclaimed),
			ActivePhysicalBytes:    int64(util.TotalPhysicalBytes),
			LiveBytesCopied:        copiedBytes,
			WriteAmplification:     waf,
			PacksCreated:           totalPacks,
			InitialPacksCreated:    initPacks,
			CompactionPacksCreated: compactionPacks,
			PacksDeleted:           packsDeleted,
			PutRequests:            probe.PutAbsents.Load() + probe.PutVers.Load() + probe.Puts.Load(),
			GetRangeRequests:       probe.GetRanges.Load(),
			DeleteRequests:         probe.Deletes.Load(),
			TotalBackendRequests:   probe.TotalRequests(),
			WallMs:                 wallMs,
			SimulatedRTTMs:         float64(simDelay.Microseconds()) / 1000.0,
			HeapAllocDeltaBytes:    heapDelta,
		}, nil
	}

	annotateRelative := func(base ModeMetrics, m ModeMetrics) ModeMetrics {
		if base.PacksCreated > 0 {
			m.PackCountOverheadPct = (float64(m.PacksCreated-base.PacksCreated) / float64(base.PacksCreated)) * 100.0
		}
		if base.LiveBytesCopied > 0 {
			m.LiveBytesReductionPct = (1.0 - float64(m.LiveBytesCopied)/float64(base.LiveBytesCopied)) * 100.0
		}
		if base.WriteAmplification > 0 {
			m.WAFReductionPct = (1.0 - m.WriteAmplification/base.WriteAmplification) * 100.0
		}
		return m
	}

	// 1. Primary comparison (unthrottled real file:// backend + vfs.Hybrid + canonical vfs.Volume).
	baseRes, err := runOne("primary", "baseline-fifo", ModeBaselineSingleStream, nil, primaryTarget, trace.HeldOutOps, 0)
	if err != nil {
		return SuiteReport{}, err
	}
	learnedRes, err := runOne("primary", "dogi-learned", ModeLifetimeSeparated, clf, primaryTarget, trace.HeldOutOps, 0)
	if err != nil {
		return SuiteReport{}, err
	}
	oracleRes, err := runOne("primary", "dogi-oracle", ModeLifetimeSeparated, oracle, primaryTarget, trace.HeldOutOps, 0)
	if err != nil {
		return SuiteReport{}, err
	}
	learnedRes = annotateRelative(baseRes, learnedRes)
	oracleRes = annotateRelative(baseRes, oracleRes)

	// 2. Adversarial misprediction sweep (injected flip rate over trained classifier).
	var advSweep []ModeMetrics
	for _, flip := range []float64{0.10, 0.25, 0.45, 0.75} {
		advPred := NewAdversarialPredictor(clf, flip, cfg.Seed+uint64(flip*1000))
		lbl := fmt.Sprintf("dogi-noise-%dpct", int(flip*100))
		m, err := runOne("adversarial", lbl, ModeLifetimeSeparated, advPred, primaryTarget, trace.HeldOutOps, 0)
		if err != nil {
			return SuiteReport{}, err
		}
		advSweep = append(advSweep, annotateRelative(baseRes, m))
	}

	// 3. PackTarget sensitivity sweep (32 KiB, 64 KiB, 256 KiB).
	var ptSweep []ModeMetrics
	for _, pt := range []int{32 << 10, 64 << 10, 256 << 10} {
		bPT, err := runOne("packtarget", fmt.Sprintf("baseline-%dk", pt>>10), ModeBaselineSingleStream, nil, pt, trace.HeldOutOps, 0)
		if err != nil {
			return SuiteReport{}, err
		}
		dPT, err := runOne("packtarget", fmt.Sprintf("dogi-%dk", pt>>10), ModeLifetimeSeparated, clf, pt, trace.HeldOutOps, 0)
		if err != nil {
			return SuiteReport{}, err
		}
		ptSweep = append(ptSweep, bPT, annotateRelative(bPT, dPT))
	}

	// 4. Negative / Boundary control: zero-expiration workload across all 3 lifetime classes.
	ctrlBase, err := runOne("boundary-no-expiry", "baseline-fifo", ModeBaselineSingleStream, nil, primaryTarget, trace.ZeroExpirationOps, 0)
	if err != nil {
		return SuiteReport{}, err
	}
	ctrlDogi, err := runOne("boundary-no-expiry", "dogi-learned", ModeLifetimeSeparated, clf, primaryTarget, trace.ZeroExpirationOps, 0)
	if err != nil {
		return SuiteReport{}, err
	}
	ctrlDogi = annotateRelative(ctrlBase, ctrlDogi)

	// 5. Optional labeled simulated S3 RTT comparison (alongside unthrottled runs).
	var simChecks []ModeMetrics
	if cfg.SimulatedRTT > 0 {
		sb, err := runOne("simulated-rtt", "baseline-fifo", ModeBaselineSingleStream, nil, primaryTarget, trace.HeldOutOps, cfg.SimulatedRTT)
		if err != nil {
			return SuiteReport{}, err
		}
		sd, err := runOne("simulated-rtt", "dogi-learned", ModeLifetimeSeparated, clf, primaryTarget, trace.HeldOutOps, cfg.SimulatedRTT)
		if err != nil {
			return SuiteReport{}, err
		}
		simChecks = append(simChecks, sb, annotateRelative(sb, sd))
	}

	return SuiteReport{
		Scale:              scaleName,
		TrainSamples:       len(trace.TrainRecords),
		HeldOutSamples:     len(trace.HeldOutRecords),
		HeldOutEval:        eval,
		PrimaryRuns:        []ModeMetrics{baseRes, learnedRes, oracleRes},
		AdversarialSweep:   advSweep,
		PackTargetSweep:    ptSweep,
		BoundaryControls:   []ModeMetrics{ctrlBase, ctrlDogi},
		SimulatedRTTChecks: simChecks,
	}, nil
}

// FormatCSV formats rep into a clean tabular CSV suitable for docs/dogi.csv.
func FormatCSV(rep SuiteReport) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	_ = w.Write([]string{
		"scenario", "name", "mode", "num_streams", "pack_target_bytes",
		"classifier_accuracy", "severe_mispred", "logical_chunks",
		"logical_plaintext_bytes", "logical_sealed_bytes",
		"physical_bytes_written", "physical_bytes_reclaimed", "active_physical_bytes",
		"live_bytes_copied", "write_amplification",
		"packs_created", "initial_packs_created", "compaction_packs_created", "packs_deleted", "pack_count_overhead_pct",
		"live_bytes_reduction_pct", "waf_reduction_pct",
		"put_requests", "get_range_requests", "delete_requests", "total_backend_requests",
		"wall_ms", "simulated_rtt_ms", "heap_alloc_delta_bytes",
	})
	appendRows := func(rows []ModeMetrics) {
		for _, r := range rows {
			_ = w.Write([]string{
				r.Scenario, r.Name, r.Mode,
				fmt.Sprintf("%d", r.NumStreams),
				fmt.Sprintf("%d", r.PackTargetBytes),
				fmt.Sprintf("%.4f", r.ClassifierAccuracy),
				fmt.Sprintf("%d", r.SevereMispredCount),
				fmt.Sprintf("%d", r.LogicalChunks),
				fmt.Sprintf("%d", r.LogicalPlaintextBytes),
				fmt.Sprintf("%d", r.LogicalSealedBytes),
				fmt.Sprintf("%d", r.PhysicalBytesWritten),
				fmt.Sprintf("%d", r.PhysicalBytesReclaimed),
				fmt.Sprintf("%d", r.ActivePhysicalBytes),
				fmt.Sprintf("%d", r.LiveBytesCopied),
				fmt.Sprintf("%.4f", r.WriteAmplification),
				fmt.Sprintf("%d", r.PacksCreated),
				fmt.Sprintf("%d", r.InitialPacksCreated),
				fmt.Sprintf("%d", r.CompactionPacksCreated),
				fmt.Sprintf("%d", r.PacksDeleted),
				fmt.Sprintf("%.2f", r.PackCountOverheadPct),
				fmt.Sprintf("%.2f", r.LiveBytesReductionPct),
				fmt.Sprintf("%.2f", r.WAFReductionPct),
				fmt.Sprintf("%d", r.PutRequests),
				fmt.Sprintf("%d", r.GetRangeRequests),
				fmt.Sprintf("%d", r.DeleteRequests),
				fmt.Sprintf("%d", r.TotalBackendRequests),
				fmt.Sprintf("%.3f", r.WallMs),
				fmt.Sprintf("%.3f", r.SimulatedRTTMs),
				fmt.Sprintf("%d", r.HeapAllocDeltaBytes),
			})
		}
	}
	appendRows(rep.PrimaryRuns)
	appendRows(rep.AdversarialSweep)
	appendRows(rep.PackTargetSweep)
	appendRows(rep.BoundaryControls)
	appendRows(rep.SimulatedRTTChecks)
	w.Flush()
	return b.String()
}
