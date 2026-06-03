// Package cost computes operational cost for VFS workloads across the
// pluggable backends VFS supports. Numbers are list prices as of 2026-06;
// override in deployment-specific configs.
//
// The model splits a workload into three contributions:
//
//	storage     = GB-months held in the bucket
//	requests    = PUT/GET/DELETE/LIST counts
//	egress      = GB read out of the cloud's edge to consumers
//
// Egress is the wild card for any read-heavy workload — if consumers
// live in the same cloud region as the backend, intra-region traffic
// is normally free. Cross-cloud fanout (e.g. workload in DOKS reading
// from AWS S3) is what gets expensive. Pick a backend with the egress
// origin/destination in mind.
//
// The package is intentionally workload-neutral. Application-specific
// projections (a validator archive, a sqlite query DB, an indexer
// snapshot pipeline) compose this package's Workload struct themselves.
package cost

import (
	"fmt"
	"strings"
	"time"
)

// Backend names the supported object stores. Add new ones by extending
// the Catalog below.
type Backend string

const (
	BackendAWSS3Std     Backend = "aws-s3-standard"
	BackendAWSS3IA      Backend = "aws-s3-ia"
	BackendAWSGlacierIR Backend = "aws-s3-glacier-instant"
	BackendCloudflareR2 Backend = "cloudflare-r2"
	BackendDOSpaces     Backend = "do-spaces"
	BackendGCSStandard  Backend = "gcs-standard"
	BackendGCSColdline  Backend = "gcs-coldline"
	// BackendPVCBlock is the "no VFS, just a Kubernetes Block PVC"
	// baseline. Useful when comparing whether moving to VFS+object
	// store actually saves money for a given workload.
	BackendPVCBlock Backend = "k8s-pvc-block"
)

// Pricing is the per-unit list-price slice for a backend. Numbers are
// USD list prices as documented inline; document any drift in this
// file's CHANGELOG stanza.
type Pricing struct {
	// StoragePerGBMonth is the monthly $/GB held.
	StoragePerGBMonth float64

	// PutPer1k is the cost per 1000 PUT requests.
	PutPer1k float64
	// GetPer1k is the cost per 1000 GET requests.
	GetPer1k float64

	// EgressPerGB is the cost per GB read OUT of the provider's edge.
	// Intra-region same-cloud reads are normally free; this rate
	// applies to cross-region or cross-cloud egress.
	EgressPerGB float64

	// MinCommitGBMonth is any minimum storage commit baked into the
	// SKU (e.g. Glacier Deep Archive's 180-day minimum).
	MinCommitGBMonth float64

	// Notes captures gotchas (e.g. R2's $0 egress is the headline).
	Notes string
}

// Catalog is the source-of-truth price list. Keep alphabetically-ish
// sorted within a provider.
var Catalog = map[Backend]Pricing{
	BackendAWSS3Std: {
		StoragePerGBMonth: 0.023, PutPer1k: 0.005, GetPer1k: 0.0004,
		EgressPerGB: 0.09,
		Notes:       "AWS S3 Standard us-east-1; egress free intra-region, $0.09/GB to internet after 100 GB free tier.",
	},
	BackendAWSS3IA: {
		StoragePerGBMonth: 0.0125, PutPer1k: 0.01, GetPer1k: 0.001,
		EgressPerGB: 0.09,
		Notes:       "AWS S3 Infrequent Access; 30-day min storage duration; retrieval fee $0.01/GB.",
	},
	BackendAWSGlacierIR: {
		StoragePerGBMonth: 0.004, PutPer1k: 0.02, GetPer1k: 0.01,
		EgressPerGB: 0.09,
		Notes:       "AWS S3 Glacier Instant Retrieval; 90-day min storage duration; reads are millisecond.",
	},
	BackendCloudflareR2: {
		StoragePerGBMonth: 0.015, PutPer1k: 0.0045, GetPer1k: 0.00036,
		EgressPerGB: 0,
		Notes:       "Cloudflare R2; $0 egress is the headline — best for cross-cloud reads.",
	},
	BackendDOSpaces: {
		StoragePerGBMonth: 0.02, PutPer1k: 0.005, GetPer1k: 0,
		EgressPerGB: 0.01,
		Notes:       "DigitalOcean Spaces; $5/mo baseline includes 250 GB + 1 TB egress, then $0.02/GB + $0.01/GB.",
	},
	BackendGCSStandard: {
		StoragePerGBMonth: 0.020, PutPer1k: 0.005, GetPer1k: 0.0004,
		EgressPerGB: 0.12,
		Notes:       "GCS Standard us-central1; egress to internet $0.12/GB.",
	},
	BackendGCSColdline: {
		StoragePerGBMonth: 0.004, PutPer1k: 0.01, GetPer1k: 0.05,
		EgressPerGB: 0.12,
		Notes:       "GCS Coldline; 90-day min storage; expensive reads at $0.05/GB retrieval.",
	},
	BackendPVCBlock: {
		StoragePerGBMonth: 0.10, PutPer1k: 0, GetPer1k: 0,
		EgressPerGB: 0,
		Notes:       "Kubernetes block-storage PVC baseline (DOKS / EKS gp3 / GKE pd-balanced range); $0.10/GB-mo, no per-op costs; in-region only.",
	},
}

// Workload describes a measured (or projected) usage pattern. Convert
// from a benchmark's reported counts via the helpers below.
//
// Workload is intentionally application-neutral: a validator-archive
// study, a sqlite query DB, or a snapshot pipeline all populate the
// same fields. Domain-specific knowledge (e.g. "chain X emits SST
// files of size Y") belongs in the caller, not in this package.
type Workload struct {
	// StorageGB is the steady-state stored volume.
	StorageGB float64

	// PutsPerMonth + GetsPerMonth are the request counts at steady
	// state.
	PutsPerMonth float64
	GetsPerMonth float64

	// EgressGBPerMonth is what leaves the provider. Set to 0 if both
	// vfsd and consumers run in the same region as the backend.
	EgressGBPerMonth float64

	// Description is purely cosmetic; surfaces in reports.
	Description string
}

// Estimate is the per-month cost breakdown for a workload on a single
// backend.
type Estimate struct {
	Backend     Backend
	Workload    string
	StorageUSD  float64
	RequestsUSD float64
	EgressUSD   float64
	TotalUSD    float64
	Notes       string
}

// EstimateFor returns the per-month cost breakdown for a workload on a
// given backend.
func EstimateFor(w Workload, be Backend) Estimate {
	p, ok := Catalog[be]
	if !ok {
		return Estimate{Backend: be, Notes: "unknown backend"}
	}
	storage := w.StorageGB * p.StoragePerGBMonth
	requests := (w.PutsPerMonth/1000.0)*p.PutPer1k + (w.GetsPerMonth/1000.0)*p.GetPer1k
	egress := w.EgressGBPerMonth * p.EgressPerGB

	return Estimate{
		Backend:     be,
		Workload:    w.Description,
		StorageUSD:  storage,
		RequestsUSD: requests,
		EgressUSD:   egress,
		TotalUSD:    storage + requests + egress,
		Notes:       p.Notes,
	}
}

// String formats the estimate as a single-line summary.
func (e Estimate) String() string {
	return fmt.Sprintf(
		"%-22s storage=$%7.2f  requests=$%7.2f  egress=$%7.2f  total=$%7.2f/mo",
		e.Backend, e.StorageUSD, e.RequestsUSD, e.EgressUSD, e.TotalUSD,
	)
}

// CompareBackends runs the workload across every backend and returns
// the estimates sorted by total monthly cost (cheapest first).
func CompareBackends(w Workload) []Estimate {
	out := make([]Estimate, 0, len(Catalog))
	for be := range Catalog {
		out = append(out, EstimateFor(w, be))
	}
	// Bubble sort is fine at this size + saves the import.
	for i := 0; i < len(out)-1; i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].TotalUSD < out[i].TotalUSD {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// FromBenchmark converts a sustained-write benchmark's measured per-op
// rate into a steady-state Workload projection.
//
// inputs:
//
//	measuredOpsPerSec: throughput observed in a benchmark
//	sustainedFraction: fraction of time the workload runs at that rate
//	                   (e.g. 0.1 if the workload is mostly idle and
//	                   only bursts during heavy activity windows)
//	avgFileSizeBytes:  the average size of files written
//	monthsRetained:    how long data is held before being deleted
//
// Returns a Workload describing the steady state — including the
// computed StorageGB growth based on write rate × retention.
//
// readAmplification = GetsPerMonth / PutsPerMonth at steady state.
// A value of 0 disables reads entirely (write-only archive); 2 is
// reasonable for an archive read by API consumers; 100+ is "actively
// queried" workloads.
func FromBenchmark(measuredOpsPerSec, sustainedFraction float64, avgFileSizeBytes int64, monthsRetained, readAmplification float64) Workload {
	secondsPerMonth := float64(time.Hour*24*30) / float64(time.Second)
	effectiveOpsPerSec := measuredOpsPerSec * sustainedFraction
	opsPerMonth := effectiveOpsPerSec * secondsPerMonth

	bytesPerMonth := opsPerMonth * float64(avgFileSizeBytes)
	storageBytes := bytesPerMonth * monthsRetained

	return Workload{
		StorageGB:    storageBytes / (1 << 30),
		PutsPerMonth: opsPerMonth,
		GetsPerMonth: opsPerMonth * readAmplification,
		Description: fmt.Sprintf("%.1f ops/s × %.0f%% util × %d B avg × %.1f mo retention × %.1f read amp",
			measuredOpsPerSec, sustainedFraction*100, avgFileSizeBytes, monthsRetained, readAmplification),
	}
}

// Report renders the comparison as a readable multi-line table.
// Pass the workload's name to disambiguate when reporting multiple.
func Report(name string, w Workload) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Workload: %s\n", name)
	fmt.Fprintf(&b, "  %s\n", w.Description)
	fmt.Fprintf(&b, "  storage_gb=%.1f  puts/mo=%.2e  gets/mo=%.2e  egress_gb/mo=%.1f\n\n",
		w.StorageGB, w.PutsPerMonth, w.GetsPerMonth, w.EgressGBPerMonth)
	for _, e := range CompareBackends(w) {
		fmt.Fprintf(&b, "  %s\n", e.String())
	}
	return b.String()
}
