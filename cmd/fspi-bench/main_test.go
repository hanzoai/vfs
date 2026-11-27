package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/fspi"
)

func TestCLISmokeOutDir(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "dossier")
	if err := run([]string{"-quick", "-seed", "1344", "-out-dir", outDir}); err != nil {
		t.Fatalf("cli run failed: %v", err)
	}

	jsonBytes, err := os.ReadFile(filepath.Join(outDir, "results.json"))
	if err != nil {
		t.Fatalf("missing results.json: %v", err)
	}
	var rep fspi.BenchmarkReport
	if err := json.Unmarshal(jsonBytes, &rep); err != nil {
		t.Fatalf("invalid results.json: %v", err)
	}
	if len(rep.FaultMatrix) != 4 {
		t.Fatalf("expected 4 regimes in fault matrix, got %d", len(rep.FaultMatrix))
	}
	if !rep.CRC16Forgery.ActiveForgeryAEADCaught || rep.CRC16Forgery.ActiveForgeryPlainExposed {
		t.Fatalf("unexpected Negative Regime #1 result: %+v", rep.CRC16Forgery)
	}

	mdBytes, err := os.ReadFile(filepath.Join(outDir, "REPORT.md"))
	if err != nil {
		t.Fatalf("missing REPORT.md: %v", err)
	}
	md := string(mdBytes)
	for _, want := range []string{
		"hanzoai/vfs#67",
		"HIP-1344",
		"HIP-0302",
		"fspi_t10_dif_hw_compat",
		"fspi_software_strong_guard",
		"Negative Regime #1",
		"Negative Regime #2",
		"Negative Regime #3",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("REPORT.md missing expected section/token %q", want)
		}
	}
}
