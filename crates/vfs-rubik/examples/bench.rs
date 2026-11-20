// SPDX-License-Identifier: Apache-2.0
//! Reproducible Rubik benchmark runner.
use vfs_rubik::{BenchConfig, run_bench, write_results_json};
use std::path::PathBuf;

fn main() {
    let out = std::env::args().nth(1).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("results/rubik-results.json"));
    let cfg = BenchConfig::default();
    eprintln!("rubik bench: seed={} objects={}", cfg.seed, cfg.n_objects);
    let report = run_bench(&cfg).expect("benchmark failed");
    write_results_json(&report, &out).expect("failed to write results");
    eprintln!("wrote {}", out.display());
    for run in &report.runs {
        println!("{}", serde_json::to_string(run).expect("serialize"));
    }
}
