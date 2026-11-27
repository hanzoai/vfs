package rosenbridge

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// FallbackReason explains why an operation used the canonical gofer path
// instead of the RosenBridge lower-copy fast path.
type FallbackReason string

const (
	ReasonAdmitted               FallbackReason = "admitted_fast_path"
	FallbackDisabled             FallbackReason = "fast_path_disabled"
	FallbackProfilerNotTriggered FallbackReason = "profiler_bottleneck_unconfirmed"
	FallbackUnsupportedOp        FallbackReason = "unsupported_op"
	FallbackCrossTenant          FallbackReason = "cross_tenant_rejected"
	FallbackSmallIO              FallbackReason = "below_min_transfer_bytes"
	FallbackNonSequentialCold    FallbackReason = "cold_non_sequential_io"
	FallbackRingFull             FallbackReason = "ring_saturated"
	FallbackThrottled            FallbackReason = "tenant_fairness_throttled"
	FallbackNoTenantChannel      FallbackReason = "no_tenant_channel"
)

// GoferSample records a single real measured canonical-gofer operation.
type GoferSample struct {
	Op           OpCode
	Bytes        int
	TotalTime    time.Duration
	BoundaryTime time.Duration // RPC framing + socketpair syscalls + kernel skb copies
	BackingTime  time.Duration // Host backing store pread/pwrite/vfs time
	Syscalls     int
	Copies       int
	AllocBytes   int
}

// ProfileSummary summarizes measured canonical gofer bottleneck evidence.
type ProfileSummary struct {
	Samples               int     `json:"samples"`
	TotalBytes            int64   `json:"total_bytes"`
	AvgSyscallsPerOp      float64 `json:"avg_syscalls_per_op"`
	AvgCopiesPerOp        float64 `json:"avg_copies_per_op"`
	AvgAllocBytesPerOp    float64 `json:"avg_alloc_bytes_per_op"`
	P50LatencyUs          float64 `json:"p50_latency_us"`
	P95LatencyUs          float64 `json:"p95_latency_us"`
	P99LatencyUs          float64 `json:"p99_latency_us"`
	BoundaryOverheadRatio float64 `json:"boundary_overhead_ratio"`
	BottleneckConfirmed   bool    `json:"bottleneck_confirmed"`
}

// GoferProfiler measures real canonical-gofer hop costs (syscalls, copies,
// serialization overhead, and latency distribution) before permitting the
// RosenBridge fast path to activate.
type GoferProfiler struct {
	minSamples       int
	minOverheadRatio float64
	minCopiesPerOp   float64
	maxWindow        int

	mu             sync.Mutex
	samples        []GoferSample
	totalCount     int64
	totalSyscalls  int64
	totalCopies    int64
	totalAllocs    int64
	totalBytes     int64
	totalTimeNs    int64
	boundaryTimeNs int64

	confirmed atomic.Bool
}

// NewGoferProfiler creates a profiler with bottleneck confirmation thresholds.
func NewGoferProfiler(minSamples int, minOverheadRatio float64) *GoferProfiler {
	if minSamples <= 0 {
		minSamples = 16
	}
	if minOverheadRatio <= 0 || minOverheadRatio >= 1 {
		minOverheadRatio = 0.25
	}
	return &GoferProfiler{
		minSamples:       minSamples,
		minOverheadRatio: minOverheadRatio,
		minCopiesPerOp:   3.0,
		maxWindow:        2048,
	}
}

// Record ingests a canonical gofer observation and re-evaluates whether the
// gofer boundary is confirmed as a bottleneck.
func (p *GoferProfiler) Record(s GoferSample) {
	p.mu.Lock()
	if len(p.samples) < p.maxWindow {
		p.samples = append(p.samples, s)
	} else {
		idx := int(p.totalCount % int64(p.maxWindow))
		p.samples[idx] = s
	}
	p.totalCount++
	p.totalSyscalls += int64(s.Syscalls)
	p.totalCopies += int64(s.Copies)
	p.totalAllocs += int64(s.AllocBytes)
	p.totalBytes += int64(s.Bytes)
	p.totalTimeNs += s.TotalTime.Nanoseconds()
	p.boundaryTimeNs += s.BoundaryTime.Nanoseconds()

	var ratio float64
	if p.totalTimeNs > 0 {
		ratio = float64(p.boundaryTimeNs) / float64(p.totalTimeNs)
	}
	avgCopies := float64(p.totalCopies) / float64(p.totalCount)
	isBottleneck := p.totalCount >= int64(p.minSamples) && ratio >= p.minOverheadRatio && avgCopies >= p.minCopiesPerOp
	p.confirmed.Store(isBottleneck)
	p.mu.Unlock()
}

// BottleneckConfirmed returns true only after real gofer profiling has
// observed enough samples showing boundary overhead exceeds the threshold.
func (p *GoferProfiler) BottleneckConfirmed() bool {
	return p.confirmed.Load()
}

// Reset clears recorded profiling samples and closes the confirmation latch.
func (p *GoferProfiler) Reset() {
	p.mu.Lock()
	p.samples = p.samples[:0]
	p.totalCount = 0
	p.totalSyscalls = 0
	p.totalCopies = 0
	p.totalAllocs = 0
	p.totalBytes = 0
	p.totalTimeNs = 0
	p.boundaryTimeNs = 0
	p.confirmed.Store(false)
	p.mu.Unlock()
}

// Summary returns a snapshot of the profiler's measured gofer metrics.
func (p *GoferProfiler) Summary() ProfileSummary {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.totalCount == 0 {
		return ProfileSummary{}
	}
	n := len(p.samples)
	lats := make([]int64, n)
	for i, s := range p.samples {
		lats[i] = s.TotalTime.Nanoseconds()
	}
	slices.Sort(lats)
	var ratio float64
	if p.totalTimeNs > 0 {
		ratio = float64(p.boundaryTimeNs) / float64(p.totalTimeNs)
	}
	return ProfileSummary{
		Samples:               int(p.totalCount),
		TotalBytes:            p.totalBytes,
		AvgSyscallsPerOp:      float64(p.totalSyscalls) / float64(p.totalCount),
		AvgCopiesPerOp:        float64(p.totalCopies) / float64(p.totalCount),
		AvgAllocBytesPerOp:    float64(p.totalAllocs) / float64(p.totalCount),
		P50LatencyUs:          float64(percentileSorted(lats, 0.50)) / 1000.0,
		P95LatencyUs:          float64(percentileSorted(lats, 0.95)) / 1000.0,
		P99LatencyUs:          float64(percentileSorted(lats, 0.99)) / 1000.0,
		BoundaryOverheadRatio: ratio,
		BottleneckConfirmed:   p.confirmed.Load(),
	}
}

func percentileSorted(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// GateConfig configures the conditional RosenBridge admission gate.
type GateConfig struct {
	// Enabled is the master switch; false forces 100% canonical gofer.
	Enabled bool
	// RequireProfilerConfirmation enforces "only after the gofer path is
	// measured as a bottleneck" before engaging RosenBridge.
	RequireProfilerConfirmation bool
	// MinProfileSamples is the minimum gofer observations before confirmation.
	MinProfileSamples int
	// MinOverheadRatio is the minimum gofer boundary/total latency ratio
	// required to confirm gofer as a bottleneck (default 0.25).
	MinOverheadRatio float64
	// MinTransferBytes is the minimum payload size eligible for fast path
	// (default 4096 bytes; smaller metadata/tiny IOs stay on gofer).
	MinTransferBytes int
	// BulkBypassBytes is the payload size at or above which a confirmed
	// workload bypasses sequential/hot warmup (default 16384 bytes).
	BulkBypassBytes int
	// MinSequentialOps is the sequential-access streak needed for medium IOs.
	MinSequentialOps int
	// HotFileThreshold is the access count needed for a file to be considered hot.
	HotFileThreshold int
	// SeqWindowBytes is the maximum offset gap still counted as sequential.
	SeqWindowBytes int64
	// MaxTenantInFlightBytes enforces collaborative per-tenant fairness throttling.
	MaxTenantInFlightBytes int64
}

// DefaultGateConfig returns production-safe defaults matching HIP-1345.
func DefaultGateConfig() GateConfig {
	return GateConfig{
		Enabled:                     true,
		RequireProfilerConfirmation: true,
		MinProfileSamples:           16,
		MinOverheadRatio:            0.25,
		MinTransferBytes:            4096,
		BulkBypassBytes:             16384,
		MinSequentialOps:            2,
		HotFileThreshold:            4,
		SeqWindowBytes:              4096,
		MaxTenantInFlightBytes:      4 << 20, // 4 MiB per tenant in-flight cap
	}
}

// AdmissionRequest describes a candidate I/O operation evaluated by the gate.
type AdmissionRequest struct {
	TenantID    string
	Op          OpCode
	Path        string
	Offset      int64
	Length      int
	CrossTenant bool
}

// GateMetrics captures atomic telemetry for admission and fallback decisions.
type GateMetrics struct {
	Admitted                  int64 `json:"admitted"`
	FallbackDisabled          int64 `json:"fallback_disabled"`
	FallbackProfilerUnconfirm int64 `json:"fallback_profiler_unconfirmed"`
	FallbackUnsupportedOp     int64 `json:"fallback_unsupported_op"`
	FallbackCrossTenant       int64 `json:"fallback_cross_tenant"`
	FallbackSmallIO           int64 `json:"fallback_small_io"`
	FallbackNonSeqCold        int64 `json:"fallback_non_sequential_cold"`
	FallbackRingFull          int64 `json:"fallback_ring_full"`
	FallbackThrottled         int64 `json:"fallback_throttled"`
	FallbackNoChannel         int64 `json:"fallback_no_channel"`
}

type fileTrackKey struct {
	tenantID string
	path     string
}

type fileAccessState struct {
	lastEnd   int64
	seqStreak int
	opCount   int
}

// AdmissionGate conditionally admits eligible I/O onto the RosenBridge fast
// path only when gofer bottleneck criteria, workload thresholds, and
// multi-tenant fairness quotas are all satisfied.
type AdmissionGate struct {
	cfg      GateConfig
	profiler *GoferProfiler

	enabled atomic.Bool

	mu       sync.Mutex
	files    map[fileTrackKey]*fileAccessState
	inFlight map[string]int64

	admitted          atomic.Int64
	fbDisabled        atomic.Int64
	fbProfiler        atomic.Int64
	fbUnsupported     atomic.Int64
	fbCrossTenant     atomic.Int64
	fbSmallIO         atomic.Int64
	fbNonSeqCold      atomic.Int64
	fbRingFull        atomic.Int64
	fbThrottled       atomic.Int64
	fbNoTenantChannel atomic.Int64
}

// NewAdmissionGate constructs a conditional admission gate with its profiler.
func NewAdmissionGate(cfg GateConfig) *AdmissionGate {
	if cfg.MinProfileSamples <= 0 {
		cfg.MinProfileSamples = 16
	}
	if cfg.MinOverheadRatio <= 0 {
		cfg.MinOverheadRatio = 0.25
	}
	if cfg.MinTransferBytes <= 0 {
		cfg.MinTransferBytes = 4096
	}
	if cfg.BulkBypassBytes <= 0 {
		cfg.BulkBypassBytes = 16384
	}
	if cfg.MinSequentialOps <= 0 {
		cfg.MinSequentialOps = 2
	}
	if cfg.HotFileThreshold <= 0 {
		cfg.HotFileThreshold = 4
	}
	if cfg.MaxTenantInFlightBytes <= 0 {
		cfg.MaxTenantInFlightBytes = 4 << 20
	}
	g := &AdmissionGate{
		cfg:      cfg,
		profiler: NewGoferProfiler(cfg.MinProfileSamples, cfg.MinOverheadRatio),
		files:    make(map[fileTrackKey]*fileAccessState),
		inFlight: make(map[string]int64),
	}
	g.enabled.Store(cfg.Enabled)
	return g
}

// Profiler returns the gate's underlying GoferProfiler.
func (g *AdmissionGate) Profiler() *GoferProfiler { return g.profiler }

// SetEnabled dynamically toggles the fast path on or off at runtime.
func (g *AdmissionGate) SetEnabled(v bool) { g.enabled.Store(v) }

// Enabled reports whether the fast path master switch is currently enabled.
func (g *AdmissionGate) Enabled() bool { return g.enabled.Load() }

// RecordFallback increments fallback counters for downstream transport fallback
// events (e.g. ring full or missing tenant channel).
func (g *AdmissionGate) RecordFallback(reason FallbackReason) {
	switch reason {
	case FallbackRingFull:
		g.fbRingFull.Add(1)
	case FallbackNoTenantChannel:
		g.fbNoTenantChannel.Add(1)
	case FallbackDisabled:
		g.fbDisabled.Add(1)
	}
}

// Evaluate decides whether req should use RosenBridge fast path or fall back
// to canonical gofer. When admitted is true, the caller MUST call
// ReleaseInFlight(req.TenantID, req.Length) when the fast-path op finishes.
func (g *AdmissionGate) Evaluate(req AdmissionRequest) (bool, FallbackReason) {
	if !g.enabled.Load() {
		g.fbDisabled.Add(1)
		return false, FallbackDisabled
	}
	if req.CrossTenant {
		g.fbCrossTenant.Add(1)
		return false, FallbackCrossTenant
	}
	if req.Op != OpRead && req.Op != OpWrite {
		g.fbUnsupported.Add(1)
		return false, FallbackUnsupportedOp
	}
	if g.cfg.RequireProfilerConfirmation && !g.profiler.BottleneckConfirmed() {
		g.fbProfiler.Add(1)
		return false, FallbackProfilerNotTriggered
	}
	if req.Length < g.cfg.MinTransferBytes {
		g.fbSmallIO.Add(1)
		return false, FallbackSmallIO
	}

	g.mu.Lock()
	key := fileTrackKey{tenantID: req.TenantID, path: req.Path}
	st := g.files[key]
	if st == nil {
		// Bound metadata tracking map size to avoid unbounded memory growth.
		if len(g.files) >= 4096 {
			clear(g.files)
		}
		st = &fileAccessState{}
		g.files[key] = st
	}
	st.opCount++
	isSeq := st.opCount > 1 && req.Offset >= st.lastEnd && (req.Offset-st.lastEnd) <= g.cfg.SeqWindowBytes
	if isSeq {
		st.seqStreak++
	} else {
		st.seqStreak = 1
	}
	st.lastEnd = req.Offset + int64(req.Length)

	qualifies := req.Length >= g.cfg.BulkBypassBytes ||
		st.seqStreak >= g.cfg.MinSequentialOps ||
		st.opCount >= g.cfg.HotFileThreshold
	if !qualifies {
		g.mu.Unlock()
		g.fbNonSeqCold.Add(1)
		return false, FallbackNonSequentialCold
	}

	curInFlight := g.inFlight[req.TenantID]
	if curInFlight+int64(req.Length) > g.cfg.MaxTenantInFlightBytes {
		g.mu.Unlock()
		g.fbThrottled.Add(1)
		return false, FallbackThrottled
	}
	g.inFlight[req.TenantID] = curInFlight + int64(req.Length)
	g.mu.Unlock()

	g.admitted.Add(1)
	return true, ReasonAdmitted
}

// ReleaseInFlight decrements the tenant's active in-flight fairness byte count.
func (g *AdmissionGate) ReleaseInFlight(tenantID string, length int) {
	g.mu.Lock()
	cur := g.inFlight[tenantID] - int64(length)
	if cur <= 0 {
		delete(g.inFlight, tenantID)
	} else {
		g.inFlight[tenantID] = cur
	}
	g.mu.Unlock()
}

// Metrics returns an atomic snapshot of admission and fallback counts.
func (g *AdmissionGate) Metrics() GateMetrics {
	return GateMetrics{
		Admitted:                  g.admitted.Load(),
		FallbackDisabled:          g.fbDisabled.Load(),
		FallbackProfilerUnconfirm: g.fbProfiler.Load(),
		FallbackUnsupportedOp:     g.fbUnsupported.Load(),
		FallbackCrossTenant:       g.fbCrossTenant.Load(),
		FallbackSmallIO:           g.fbSmallIO.Load(),
		FallbackNonSeqCold:        g.fbNonSeqCold.Load(),
		FallbackRingFull:          g.fbRingFull.Load(),
		FallbackThrottled:         g.fbThrottled.Load(),
		FallbackNoChannel:         g.fbNoTenantChannel.Load(),
	}
}
