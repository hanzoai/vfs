// SPDX-License-Identifier: Apache-2.0
//! [Bounty #47] (`hanzoai/vfs#68`): **Machine-checkable storage specifications** (SYSSPEC).
//!
//! Clean-room simulation harness for checking a storage system against an
//! explicit, machine-readable specification instead of example-based tests.
//!
//! # Design
//!
//! A spec is a set of *clauses*, each a predicate over a trace of operations
//! (`write`, `sync`, `crash`, `read`). Each clause has a violation rate in the
//! system under test, and a probability that a random example test happens to
//! exercise the exact interleaving that exposes it.
//!
//! - `ExampleTests`: N random traces, assert the final read equals the final
//!   write. Cheap, but only catches clauses that show up in the final state.
//! - `SpecCheck`: enumerate traces against every clause, including crash
//!   interleavings that never appear in a happy-path example.
//!
//! Metrics: clauses covered, violations detected, detection rate, and the number
//! of traces required.
//!
//! # Honest boundaries
//! - No real filesystem, kernel, or crash is exercised; traces are synthetic.
//! - Per-clause violation rates and example-test exposure probabilities are
//!   hand-set, not measured from a real bug corpus.
//! - All figures are simulation outputs on the authoring host, NOT Hanzo
//!   expectations until reproduced against a real implementation and fuzzer.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SysSpecError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// One machine-checkable clause of the storage specification.
#[derive(Debug, Clone)]
pub struct Clause {
    pub id: &'static str,
    pub statement: &'static str,
    /// Probability that the system under test violates this clause at least
    /// once in a trace that exercises it.
    pub violation_rate: f64,
    /// Probability that a randomly generated happy-path example trace happens
    /// to exercise the interleaving needed to expose the violation.
    pub example_exposure: f64,
}

/// The specification under check.
pub fn default_spec() -> Vec<Clause> {
    vec![
        Clause {
            id: "S1",
            statement: "a completed write is visible to a subsequent read",
            violation_rate: 0.002,
            example_exposure: 0.99,
        },
        Clause {
            id: "S2",
            statement: "fsync makes all preceding writes durable",
            violation_rate: 0.03,
            example_exposure: 0.62,
        },
        Clause {
            id: "S3",
            statement: "a crash never exposes a torn write",
            violation_rate: 0.08,
            example_exposure: 0.04,
        },
        Clause {
            id: "S4",
            statement: "rename is atomic with respect to crash",
            violation_rate: 0.05,
            example_exposure: 0.07,
        },
        Clause {
            id: "S5",
            statement: "directory entry creation is ordered after its data block",
            violation_rate: 0.06,
            example_exposure: 0.03,
        },
        Clause {
            id: "S6",
            statement: "concurrent writers never lose an update",
            violation_rate: 0.01,
            example_exposure: 0.22,
        },
        Clause {
            id: "S7",
            statement: "discard of an unallocated extent is a no-op",
            violation_rate: 0.004,
            example_exposure: 0.31,
        },
        Clause {
            id: "S8",
            statement: "a failed write leaves the previous contents intact",
            violation_rate: 0.07,
            example_exposure: 0.05,
        },
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// Random happy-path traces, assert final state only.
    ExampleTests,
    /// Enumerate traces against every clause, including crash interleavings.
    SpecCheck,
}

impl Policy {
    pub fn label(&self) -> &'static str {
        match self {
            Policy::ExampleTests => "example-tests",
            Policy::SpecCheck => "spec-check",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClauseOutcome {
    pub id: String,
    pub statement: String,
    /// Whether the policy ever exercised this clause.
    pub exercised: bool,
    /// Whether a violating trace was found.
    pub violation_found: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyResult {
    pub policy: String,
    pub traces: usize,
    pub clauses_total: usize,
    pub clauses_exercised: usize,
    pub violations_found: usize,
    pub detection_rate: f64,
    pub per_clause: Vec<ClauseOutcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub bounty: String,
    pub hip: String,
    pub honesty_labels: Vec<String>,
    pub config: BenchConfig,
    pub results: Vec<PolicyResult>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
    pub seed: u64,
    /// Traces the example-based suite runs.
    pub example_traces: usize,
    /// Traces the spec checker enumerates (one per clause per round).
    pub spec_rounds: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self { seed: 0x5EC_5FE, example_traces: 10_000, spec_rounds: 200 }
    }
}
impl BenchConfig {
    pub fn test_config() -> Self {
        Self { seed: 7, example_traces: 2_000, spec_rounds: 40 }
    }
}

pub fn evaluate(policy: Policy, clauses: &[Clause], cfg: &BenchConfig) -> PolicyResult {
    let mut rng = SplitMix64::new(cfg.seed);
    let mut per_clause = Vec::with_capacity(clauses.len());

    match policy {
        Policy::ExampleTests => {
            // Each trace is a happy path; a clause is only exercised if the
            // random trace happens to hit the relevant interleaving.
            let mut exercised = vec![false; clauses.len()];
            let mut violated = vec![false; clauses.len()];
            for _ in 0..cfg.example_traces {
                for (i, c) in clauses.iter().enumerate() {
                    if exercised[i] {
                        continue;
                    }
                    if rng.next_f64() < c.example_exposure {
                        exercised[i] = true;
                        if rng.next_f64() < c.violation_rate {
                            violated[i] = true;
                        }
                    }
                }
            }
            for (i, c) in clauses.iter().enumerate() {
                per_clause.push(ClauseOutcome {
                    id: c.id.into(),
                    statement: c.statement.into(),
                    exercised: exercised[i],
                    violation_found: violated[i],
                });
            }
        }
        Policy::SpecCheck => {
            // The checker exercises every clause by construction; each round is
            // a targeted trace for that clause.
            for c in clauses {
                let mut found = false;
                for _ in 0..cfg.spec_rounds {
                    if rng.next_f64() < c.violation_rate {
                        found = true;
                        break;
                    }
                }
                per_clause.push(ClauseOutcome {
                    id: c.id.into(),
                    statement: c.statement.into(),
                    exercised: true,
                    violation_found: found,
                });
            }
        }
    }

    let exercised = per_clause.iter().filter(|c| c.exercised).count();
    let violations = per_clause.iter().filter(|c| c.violation_found).count();
    let total = clauses.len().max(1);

    PolicyResult {
        policy: policy.label().into(),
        traces: match policy {
            Policy::ExampleTests => cfg.example_traces,
            Policy::SpecCheck => clauses.len() * cfg.spec_rounds,
        },
        clauses_total: clauses.len(),
        clauses_exercised: exercised,
        violations_found: violations,
        detection_rate: violations as f64 / total as f64,
        per_clause,
    }
}

pub fn run_bench(cfg: &BenchConfig) -> Result<BenchReport, SysSpecError> {
    if cfg.example_traces == 0 {
        return Err(SysSpecError::InvalidConfig("example_traces must be positive".into()));
    }
    if cfg.spec_rounds == 0 {
        return Err(SysSpecError::InvalidConfig("spec_rounds must be positive".into()));
    }
    let clauses = default_spec();
    let results = vec![
        evaluate(Policy::ExampleTests, &clauses, cfg),
        evaluate(Policy::SpecCheck, &clauses, cfg),
    ];

    let mut notes = Vec::new();
    if let (Some(ex), Some(spec)) = (
        results.iter().find(|r| r.policy == "example-tests"),
        results.iter().find(|r| r.policy == "spec-check"),
    ) {
        notes.push(format!(
            "spec checking exercises {}/{} clauses and finds {} violations; example tests exercise {}/{} and find {}",
            spec.clauses_exercised,
            spec.clauses_total,
            spec.violations_found,
            ex.clauses_exercised,
            ex.clauses_total,
            ex.violations_found
        ));
        notes.push(format!(
            "example tests reached {}/{} clause coverage with {} traces but found only {} violation(s); spec checking found {} with {} traces",
            ex.clauses_exercised,
            ex.clauses_total,
            ex.traces,
            ex.violations_found,
            spec.violations_found,
            spec.traces
        ));
        let missed: Vec<&str> = ex
            .per_clause
            .iter()
            .filter(|c| !c.violation_found)
            .map(|c| c.id.as_str())
            .collect();
        if !missed.is_empty() {
            notes.push(format!(
                "clauses with violations that example tests never observed: {}",
                missed.join(", ")
            ));
        }
    }

    Ok(BenchReport {
        bounty: "hanzoai/vfs#68 [Bounty #47] Machine-checkable storage specifications (SYSSPEC)".into(),
        hip: "HIP-1348: Reliable, Economical and Verifiable AI Infrastructure".into(),
        honesty_labels: vec![
            "no real filesystem, kernel or crash is exercised - traces are synthetic".into(),
            "per-clause violation rates and example-test exposure probabilities are hand-set".into(),
            "the model assumes clause independence; real bugs correlate across clauses".into(),
            "All figures are simulation outputs on the authoring host, NOT Hanzo expectations until reproduced against a real implementation and fuzzer.".into(),
        ],
        config: cfg.clone(),
        results,
        notes,
    })
}

pub fn write_results_json(report: &BenchReport, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(report)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find<'a>(r: &'a [PolicyResult], p: &str) -> &'a PolicyResult {
        r.iter().find(|x| x.policy == p).expect(p)
    }

    #[test]
    fn spec_check_exercises_every_clause() {
        let report = run_bench(&BenchConfig::test_config()).unwrap();
        let spec = find(&report.results, "spec-check");
        assert_eq!(spec.clauses_exercised, spec.clauses_total);
    }

    #[test]
    fn spec_check_finds_at_least_as_many_violations() {
        let report = run_bench(&BenchConfig::test_config()).unwrap();
        let ex = find(&report.results, "example-tests");
        let spec = find(&report.results, "spec-check");
        assert!(
            spec.violations_found >= ex.violations_found,
            "spec {} should be >= example {}",
            spec.violations_found,
            ex.violations_found
        );
    }

    #[test]
    fn example_tests_miss_crash_clauses() {
        let report = run_bench(&BenchConfig::default()).unwrap();
        let ex = find(&report.results, "example-tests");
        let spec = find(&report.results, "spec-check");
        // Crash-only clauses (S3 torn write, S4 rename atomicity, S5 dir-entry
        // ordering, S8 failed-write preservation) are exactly what happy-path
        // examples miss.
        let crash_clauses = ["S3", "S4", "S5", "S8"];
        let missed_by_examples = crash_clauses
            .iter()
            .filter(|id| {
                ex.per_clause
                    .iter()
                    .find(|c| &c.id == *id)
                    .map(|c| !c.violation_found)
                    .unwrap_or(false)
            })
            .count();
        assert!(
            missed_by_examples > 0,
            "example tests found every crash clause; model is not exercising the gap"
        );
        let found_by_spec = crash_clauses
            .iter()
            .filter(|id| {
                spec.per_clause
                    .iter()
                    .find(|c| &c.id == *id)
                    .map(|c| c.violation_found)
                    .unwrap_or(false)
            })
            .count();
        assert!(
            found_by_spec > missed_by_examples,
            "spec check found {} crash clauses, examples missed {}",
            found_by_spec,
            missed_by_examples
        );
    }

    #[test]
    fn spec_check_is_cheaper_in_traces_than_brute_force() {
        let report = run_bench(&BenchConfig::default()).unwrap();
        let ex = find(&report.results, "example-tests");
        let spec = find(&report.results, "spec-check");
        assert!(
            spec.traces < ex.traces,
            "spec {} traces should be < example {} traces",
            spec.traces,
            ex.traces
        );
    }

    #[test]
    fn detection_rate_is_bounded() {
        let report = run_bench(&BenchConfig::test_config()).unwrap();
        for r in &report.results {
            assert!(r.detection_rate >= 0.0 && r.detection_rate <= 1.0);
        }
    }

    #[test]
    fn bad_config_rejected() {
        assert!(run_bench(&BenchConfig { seed: 1, example_traces: 0, spec_rounds: 5 }).is_err());
        assert!(run_bench(&BenchConfig { seed: 1, example_traces: 5, spec_rounds: 0 }).is_err());
    }
}
