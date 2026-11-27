// Command fspi-bench runs the FS-PI (FAST '26) end-to-end journal integrity
// evaluation suite for hanzoai/vfs#67 (HIP-1344) and emits reproducible
// results.json and REPORT.md artifacts.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hanzoai/vfs/pkg/fspi"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "fspi-bench: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("fspi-bench", flag.ContinueOnError)
	outDir := fs.String("out-dir", "", "directory to write REPORT.md and results.json")
	jsonOnly := fs.Bool("json", false, "print JSON report to stdout")
	quick := fs.Bool("quick", false, "run abbreviated quick profile for smoke tests")
	seed := fs.Int64("seed", 1344, "deterministic PRNG seed")
	workDir := fs.String("work-dir", "", "temporary scratch directory (defaults to os.MkdirTemp)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var cfg fspi.BenchConfig
	if *quick {
		cfg = fspi.QuickBenchConfig(*workDir)
	} else {
		cfg = fspi.DefaultBenchConfig(*workDir)
	}
	cfg.Seed = *seed

	rep, err := fspi.RunFullBenchmark(cfg)
	if err != nil {
		return err
	}

	rawJSON, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	rawJSON = append(rawJSON, '\n')
	md := fspi.RenderMarkdown(rep)

	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			return err
		}
		jsonPath := filepath.Join(*outDir, "results.json")
		mdPath := filepath.Join(*outDir, "REPORT.md")
		if err := os.WriteFile(jsonPath, rawJSON, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "wrote %s and %s\n", mdPath, jsonPath)
	}

	if *jsonOnly {
		_, err = os.Stdout.Write(rawJSON)
		return err
	}
	if *outDir == "" {
		_, err = os.Stdout.WriteString(md)
		return err
	}
	return nil
}
