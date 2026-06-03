// vfs-cost — print backend cost comparison for a workload.
//
// vfs-cost is intentionally workload-neutral. It takes a generic
// Workload (or a benchmark projection) and reports the per-month cost
// on every supported backend, sorted cheapest first.
//
// Application-specific projections (validator archives, sqlite query
// DBs, snapshot pipelines) live in the calling repo, not here.
//
// Examples:
//
//	# Manual workload: 100 GB stored, 5M PUTs/mo, 50M GETs/mo, 20 GB egress.
//	vfs-cost --storage 100 --puts 5e6 --gets 5e7 --egress 20
//
//	# Take a measured ops/s rate from a benchmark and project forward.
//	vfs-cost --ops 6.4 --util 0.1 --avg 65536 --months 12 --read-amp 2
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hanzoai/vfs/pkg/cost"
)

func main() {
	storage := flag.Float64("storage", -1, "steady-state stored volume in GB (selects manual mode)")
	puts := flag.Float64("puts", 0, "PUT requests per month (manual mode)")
	gets := flag.Float64("gets", 0, "GET requests per month (manual mode)")
	egress := flag.Float64("egress", 0, "cross-region/cross-cloud egress per month, GB")

	// benchmark mode
	ops := flag.Float64("ops", -1, "(benchmark mode) measured PUTs per second")
	util := flag.Float64("util", 0.1, "(benchmark mode) sustained-utilization fraction (0..1)")
	avg := flag.Int64("avg", 64*1024, "(benchmark mode) average file size in bytes")
	months := flag.Float64("months", 12, "(benchmark mode) retention in months")
	readAmp := flag.Float64("read-amp", 2.0, "(benchmark mode) GetsPerMonth / PutsPerMonth")

	flag.Parse()

	var (
		w    cost.Workload
		name string
	)
	switch {
	case *storage > 0:
		w = cost.Workload{
			StorageGB:        *storage,
			PutsPerMonth:     *puts,
			GetsPerMonth:     *gets,
			EgressGBPerMonth: *egress,
			Description: fmt.Sprintf("manual: %.0f GB stored, %.2e puts/mo, %.2e gets/mo, %.0f GB egress/mo",
				*storage, *puts, *gets, *egress),
		}
		name = "manual"
	case *ops > 0:
		w = cost.FromBenchmark(*ops, *util, *avg, *months, *readAmp)
		w.EgressGBPerMonth = *egress
		name = "benchmark-projection"
	default:
		fmt.Fprintln(os.Stderr, "vfs-cost: supply either --storage or --ops to describe a workload (see --help)")
		os.Exit(2)
	}

	if _, err := os.Stdout.WriteString(cost.Report(name, w)); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
}
