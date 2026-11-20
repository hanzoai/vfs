// SPDX-License-Identifier: Apache-2.0
//! Reproducible WSBuffer benchmark runner.
use vfs_wsbuffer::{BenchConfig, run_bench, write_results_json};
use std::path::PathBuf;

fn main() {
    let out = std::env::args().nth(1).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("results/wsbuffer-results.json"));
    let cfg = BenchConfig::default();
    eprintln!("wsbuffer bench: seed={} n_ops={} max_off={:?} max_len={}",
        cfg.seed, cfg.n_ops, cfg.max_offset, cfg.max_write_len);
    let report = run_bench(&cfg).expect("benchmark failed");
    write_results_json(&report, &out).expect("failed to write results json");
    eprintln!("wrote {}", out.display());
    for run in &report.runs {
        println!("{}", serde_json::to_string(run).expect("serialize"));
    }
}
