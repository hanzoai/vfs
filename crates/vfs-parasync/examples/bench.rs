// SPDX-License-Identifier: Apache-2.0
//! Reproducible benchmark runner for [Bounty #7] Parallel delta restore (ParaSync).
use vfs_parasync::{BenchConfig, run_bench, write_results_json};
use std::path::PathBuf;
fn main() {
    let out = std::env::args().nth(1).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("results/vfsparasync-results.json"));
    let cfg = BenchConfig::default();
    eprintln!("vfs-parasync bench");
    let report = run_bench(&cfg).expect("benchmark failed");
    write_results_json(&report, &out).expect("failed to write results");
    eprintln!("wrote {}", out.display());
}