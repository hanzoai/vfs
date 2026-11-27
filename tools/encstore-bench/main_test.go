// Copyright 2026 Hanzo AI Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEncstoreBenchDossierGeneration(t *testing.T) {
	report, err := runAllBenchmarks(4)
	if err != nil {
		t.Fatalf("runAllBenchmarks: %v", err)
	}
	if len(report.Scenarios) != 9 {
		t.Fatalf("expected 9 scenarios, got %d", len(report.Scenarios))
	}
	for _, sc := range report.Scenarios {
		if sc.Status != "PASS" {
			t.Fatalf("scenario %s failed: %+v", sc.Name, sc)
		}
	}
	if report.Summary.TrustProbesPassed != report.Summary.TrustProbesTotal {
		t.Fatalf("expected all trust probes passed, got %d/%d", report.Summary.TrustProbesPassed, report.Summary.TrustProbesTotal)
	}
	if report.Summary.EncryptedLeakagePct != 0 || report.Summary.UnencLeakagePct != 100 {
		t.Fatalf("unexpected leakage metrics: enc=%.1f unenc=%.1f", report.Summary.EncryptedLeakagePct, report.Summary.UnencLeakagePct)
	}

	dir := t.TempDir()
	if err := writeDossierFiles(dir, report); err != nil {
		t.Fatalf("writeDossierFiles: %v", err)
	}
	rawBytes, err := os.ReadFile(filepath.Join(dir, "raw_results.json"))
	if err != nil {
		t.Fatalf("read raw_results.json: %v", err)
	}
	var decoded DossierReport
	if err := json.Unmarshal(rawBytes, &decoded); err != nil {
		t.Fatalf("decode raw_results.json: %v", err)
	}
	scoreBytes, err := os.ReadFile(filepath.Join(dir, "score_table.md"))
	if err != nil {
		t.Fatalf("read score_table.md: %v", err)
	}
	readmeBytes, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}

	// Verify exact numeric parity across README.md, score_table.md, and raw_results.json.
	need := fmt.Sprintf("%.2f MB/s", decoded.Summary.DecryptVerifyMBps)
	if !strings.Contains(string(scoreBytes), need) || !strings.Contains(string(readmeBytes), need) {
		t.Fatalf("expected %q in both score_table.md and README.md", need)
	}
}
