// Command rubikbench lays the same synthetic base-image workload out four ways
// and prints what each layout costs: the bytes the store holds, the bytes a
// compressed pack would hold, and the bytes one boot fetches.
//
// Everything is a function of the flags alone. The workload is generated from a
// seed, the plans are deterministic, and the only nondeterminism in the run is
// the clock used for the elapsed columns, which are indicative.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hanzoai/vfs/pkg/rubik"
)

type row struct {
	plan      string
	tenant    string
	packBytes int
	packs     int
	stored    int
	sealed    int
	storedVs  float64
	sealedVs  float64
	images    int
	fetched   int
	p90       int
	needed    int
	amp       float64
	mixed     int
	runs      int
	groups    int
	ampSum    float64
	mode      string
}

func main() {
	var cfg rubik.Layered
	var packBytes int
	var out string
	flag.IntVar(&cfg.Tenants, "tenants", 2, "tenants in the store")
	flag.IntVar(&cfg.Images, "images", 12, "base images per tenant")
	flag.IntVar(&cfg.Layers, "layers", 6, "layers a lineage can stack")
	flag.IntVar(&cfg.LayerChunks, "layer-chunks", 64, "chunks per layer")
	flag.IntVar(&cfg.Deltas, "deltas", 12, "near-copy chunks per image")
	flag.IntVar(&cfg.ChunkBytes, "chunk-bytes", 4096, "plaintext bytes per chunk")
	flag.BoolVar(&cfg.Scatter, "scatter", false, "layers reach the store out of depth order")
	flag.Uint64Var(&cfg.Seed, "seed", 20261126, "workload seed")
	flag.IntVar(&packBytes, "pack", 262144, "pack size in bytes")
	flag.StringVar(&out, "csv", "", "write raw rows here")
	flag.Parse()

	mode := "layered"
	if cfg.Scatter {
		mode = "scatter"
	}
	start := time.Now()
	w := cfg.Workload()
	buildTime := time.Since(start)

	packer := rubik.Packer{Bytes: packBytes}
	plans := []rubik.Planner{
		rubik.Arrival{},
		rubik.BySize{},
		rubik.ByContent{},
		rubik.ByCoAccess{},
	}

	tenants := append(w.TenantNames(), "")
	base := make(map[string]row, len(plans)*len(tenants))

	var rows []row
	for _, pl := range plans {
		t0 := time.Now()
		order := pl.Order(w)
		plan := packer.Plan(w, order)
		elapsed := time.Since(t0)
		runs, groups := rubik.CoAccessRuns(w, order)
		for _, tenant := range tenants {
			cs := plan.ColdStarts(w, tenant)
			r := row{
				plan:      pl.Name(),
				tenant:    tenant,
				packBytes: packBytes,
				packs:     plan.Count(),
				stored:    plan.Stored(w),
				sealed:    plan.Sealed(w),
				images:    cs.Images,
				fetched:   cs.MedianFetched,
				p90:       cs.P90Fetched,
				needed:    cs.MedianNeeded,
				amp:       cs.MedianAmp,
				mixed:     plan.Isolation(w),
				mode:      mode,
				ampSum:    cs.SumAmp,
				runs:      runs,
				groups:    groups,
			}
			if _, ok := base[tenant]; !ok {
				base[tenant] = r
			}
			r.storedVs = ratio(r.stored, base[tenant].stored)
			r.sealedVs = ratio(r.sealed, base[tenant].sealed)
			rows = append(rows, r)
			if tenant == "" {
				fmt.Fprintf(os.Stderr, "%-9s %6.1fms  packs %4d  stored %s  sealed %s (%.3f)  fetch/need %.3f/%.2f  runs %d/%d\n",
					pl.Name(), float64(elapsed.Microseconds())/1000, r.packs,
					human(r.stored), human(r.sealed), r.sealedVs, r.ampSum, r.amp, r.runs, r.groups)
			}
		}
	}

	fmt.Printf("workload: %d chunks, %.1f MiB plaintext, %d images, %d tenants, pack %s (built in %s)\n",
		len(w.Chunks), float64(total(w))/1048576.0, len(w.Images), cfg.Tenants, human(packBytes), buildTime.Round(time.Millisecond))
	printTable(rows)

	if out != "" {
		if err := writeCSV(out, rows); err != nil {
			fmt.Fprintln(os.Stderr, "rubikbench:", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "wrote %s (%d rows)\n", out, len(rows))
	}
}

func printTable(rows []row) {
	fmt.Printf("\n%-9s %-10s %6s %10s %10s %6s %10s %10s %10s %7s %7s %8s %5s\n",
		"plan", "tenant", "packs", "stored", "sealed", "vs", "fetch_p50", "fetch_p90", "needed_p50", "amp", "amp_sum", "runs/grp", "mix")
	for _, r := range rows {
		t := r.tenant
		if t == "" {
			t = "(all)"
		}
		fmt.Printf("%-9s %-10s %6d %10s %10s %6.3f %10s %10s %10s %7.2f %7.3f %8s %5d\n",
			r.plan, t, r.packs, human(r.stored), human(r.sealed), r.sealedVs,
			human(r.fetched), human(r.p90), human(r.needed), r.amp, r.ampSum,
			fmt.Sprintf("%d/%d", r.runs, r.groups), r.mixed)
	}
	fmt.Printf("\nvs = sealed bytes over the arrival plan's sealed bytes, same tenant; 1.000 is no change.\n")
	fmt.Printf("amp = median fetched over needed, whole packs over the bytes the image reads.\n")
	fmt.Printf("amp_sum = the same ratio summed over every image: a split group shows here and hides in the median.\n")
	fmt.Printf("runs/grp = maximal stretches sharing one reader set over distinct reader sets; equal is a co-access clustering.\n")
}

func writeCSV(path string, rows []row) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	cw := csv.NewWriter(f)
	defer cw.Flush()
	if err := cw.Write([]string{
		"mode", "plan", "tenant", "pack_bytes", "packs", "stored_bytes", "sealed_bytes",
		"sealed_vs_arrival", "images", "fetch_p50", "fetch_p90", "needed_p50",
		"read_amp", "read_amp_sum", "coaccess_runs", "coaccess_groups", "isolation_violations",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := cw.Write([]string{
			r.mode, r.plan, r.tenant, strconv.Itoa(r.packBytes), strconv.Itoa(r.packs),
			strconv.Itoa(r.stored), strconv.Itoa(r.sealed), ftoa(r.sealedVs),
			strconv.Itoa(r.images), strconv.Itoa(r.fetched), strconv.Itoa(r.p90),
			strconv.Itoa(r.needed), ftoa(r.amp), ftoa(r.ampSum),
			strconv.Itoa(r.runs), strconv.Itoa(r.groups), strconv.Itoa(r.mixed),
		}); err != nil {
			return err
		}
	}
	return cw.Error()
}

func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', 4, 64) }

func ratio(n, base int) float64 {
	if base == 0 {
		return 0
	}
	return float64(n) / float64(base)
}

func total(w *rubik.Workload) int {
	n := 0
	for _, c := range w.Chunks {
		n += len(c.Body)
	}
	return n
}

func human(n int) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	f, i := float64(n)/unit, 0
	for f >= unit && i < len(units)-1 {
		f /= unit
		i++
	}
	s := strconv.FormatFloat(f, 'f', 1, 64)
	return strings.Join([]string{s, units[i]}, " ")
}
