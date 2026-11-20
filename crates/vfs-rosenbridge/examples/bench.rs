// SPDX-License-Identifier: Apache-2.0
//! Reproducible runner.
use vfs_rosenbridge::{BenchConfig, run_bench, write_results_json};
use std::path::PathBuf;
fn main() { let out = std::env::args().nth(1).map(PathBuf::from).unwrap_or(PathBuf::from("results.json")); let cfg = BenchConfig::default(); let report = run_bench(&cfg).expect("failed"); write_results_json(&report, &out).expect("failed"); }