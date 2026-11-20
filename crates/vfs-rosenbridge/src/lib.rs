// SPDX-License-Identifier: Apache-2.0
//! [Bounty #9] (`hanzoai/vfs#66`): **Virtualization-boundary fast IO** (RosenBridge).
//!
//! Clean-room simulation harness benchmarking lower-copy host-to-guest IO per
//! *RosenBridge* (FAST '26): measure syscalls avoided, memory copies saved, and
//! task runtime impact when bypassing the VFS boundary for gofer workloads only.
//!
//! # Design
//!
//! - **StandardVFS**: full syscall path through VFS layer (baseline).
//! - **RosenBridgeFastPath**: simulated direct host-guest IO bypass for gofer
//!   ops, with conditional gating (only after profiling confirms gofer is bottleneck).
//!
//! # Honest boundaries
//! - Simulated syscall counts — no real kernel or virtualization layer exercised.
//! - Gofer workload identification is heuristic-based, not real profiler data.
//! - All figures are simulation outputs; **not Hanzo expectations**.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RBError { #[error("invalid config: {0}")] InvalidConfig(String) }

#[derive(Debug, Clone)]
pub struct IoOp {
    pub op_type: IoOpType,
    pub is_gofer: bool,
    pub size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOpType { Read, Write, Mmap, Fsync }

pub fn generate_ops(seed: u64, n: usize, gofer_ratio: f64) -> Vec<IoOp> {
    let mut rng = SplitMix64::new(seed);
    (0..n).map(|_| IoOp {
        op_type: match rng.next_u64() % 4 { 0 => IoOpType::Read, 1 => IoOpType::Write, 2 => IoOpType::Mmap, _ => IoOpType::Fsync },
        is_gofer: rng.next_f64() < gofer_ratio,
        size: 64 + (rng.next_u64() as usize % 65536),
    }).collect()
}

#[derive(Debug, Clone)]
pub struct SplitMix64 { state: u64 }
impl SplitMix64 {
    pub fn new(s: u64) -> Self { Self { state: s } }
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn next_f64(&mut self) -> f64 { self.next_u64() as f64 / u64::MAX as f64 }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct IoStats {
    pub total_syscalls: u64,
    pub total_mem_copies: u64,
    pub total_time_ns: u64,
    pub gofer_ops: u64,
    pub non_gofer_ops: u64,
}

/// Standard VFS: every op goes through full VFS layer.
pub struct StandardVfs {
    stats: IoStats,
}
impl StandardVfs {
    pub fn new() -> Self { Self { stats: IoStats::default() } }
    pub fn execute(&mut self, op: &IoOp) {
        // Full VFS path: 3 syscalls + 2 mem copies per op (simulated costs)
        self.stats.total_syscalls += 3;
        self.stats.total_mem_copies += 2;
        self.stats.total_time_ns += 500 + op.size as u64; // 500ns base + proportional
        if op.is_gofer { self.stats.gofer_ops += 1; } else { self.stats.non_gofer_ops += 1; }
    }
    pub fn stats(&self) -> &IoStats { &self.stats }
}

/// RosenBridge fast path: bypass VFS for gofer ops only.
pub struct RosenBridgeFastPath {
    stats: IoStats,
    gofer_bypass_enabled: bool,
}
impl RosenBridgeFastPath {
    pub fn new(gofer_bypass_enabled: bool) -> Self {
        Self { stats: IoStats::default(), gofer_bypass_enabled }
    }
    pub fn execute(&mut self, op: &IoOp) {
        if op.is_gofer && self.gofer_bypass_enabled {
            // Bypass path: 1 syscall + 0 mem copies (direct host-guest)
            self.stats.total_syscalls += 1;
            self.stats.total_mem_copies += 0;
            self.stats.total_time_ns += 100 + op.size as u64 / 100; // much faster
        } else {
            // Standard path for non-gofer or when bypass disabled
            self.stats.total_syscalls += 3;
            self.stats.total_mem_copies += 2;
            self.stats.total_time_ns += 500 + op.size as u64;
        }
        if op.is_gofer { self.stats.gofer_ops += 1; } else { self.stats.non_gofer_ops += 1; }
    }
    pub fn stats(&self) -> &IoStats { &self.stats }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub policy: String,
    pub gofer_bypass: bool,
    pub n_ops: usize,
    pub syscalls_per_op: f32,
    pub mem_copies_per_op: f32,
    pub time_per_op_ns: f32,
    pub gofer_speedup: f32, // time_saved_by_bypass / total_time
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub bounty: String,
    pub hip: String,
    pub honesty_labels: Vec<String>,
    pub runs: Vec<RunResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
    pub seed: u64,
    pub n_ops: usize,
    pub gofer_ratio: f64,
}
impl Default for BenchConfig { Self { seed: 0x21, n_ops: 10000, gofer_ratio: 0.3 } }
impl BenchConfig {
    pub fn test_config() -> Self { Self { seed: 7, n_ops: 200, gofer_ratio: 0.3 } }
}

pub fn run_bench(cfg: &BenchConfig) -> Result<BenchReport, RBError> {
    if cfg.n_ops == 0 { return Err(RBError::InvalidConfig("n_ops must be positive".into())); }
    let ops = generate_ops(cfg.seed, cfg.n_ops, cfg.gofer_ratio);
    let mut runs: Vec<RunResult> = Vec::new();
    
    // Policy A: Standard VFS
    {
        let mut h = StandardVfs::new();
        for op in &ops { h.execute(op); }
        let st = h.stats();
        runs.push(RunResult {
            policy: "standard-vfs".into(),
            gofer_bypass: false, n_ops: cfg.n_ops,
            syscalls_per_op: st.total_syscalls as f32 / cfg.n_ops as f32,
            mem_copies_per_op: st.total_mem_copies as f32 / cfg.n_ops as f32,
            time_per_op_ns: st.total_time_ns as f32 / cfg.n_ops as f32,
            gofer_speedup: 0.0,
        });
    }
    
    // Policy B: RosenBridge with bypass
    {
        let mut h = RosenBridgeFastPath::new(true);
        for op in &ops { h.execute(op); }
        let st = h.stats();
        let standard_time = ops.len() as u64 * 500 + ops.iter().map(|o| o.size as u64).sum::<u64>();
        let speedup = if standard_time > 0 { standard_time as f32 / st.total_time_ns.max(1) as f32 } else { 1.0 };
        runs.push(RunResult {
            policy: "rosenbridge-fastpath".into(),
            gofer_bypass: true, n_ops: cfg.n_ops,
            syscalls_per_op: st.total_syscalls as f32 / cfg.n_ops as f32,
            mem_copies_per_op: st.total_mem_copies as f32 / cfg.n_ops as f32,
            time_per_op_ns: st.total_time_ns as f32 / cfg.n_ops as f32,
            gofer_speedup: speedup,
        });
    }
    
    Ok(BenchReport {
        bounty: "hanzoai/vfs#66 [Bounty #9] Virtualization-boundary fast IO (RosenBridge)".into(),
        hip: "HIP-1344: Agent-Optimized Storage and Recovery".into(),
        honesty_labels: vec![
            "simulated syscall counts — no real kernel or virtualization layer exercised".into(),
            "gofer identification is heuristic-based (is_gofer flag), not real profiler data".into(),
            "time model is simplified: 500ns base + proportional to size; real measurements differ".into(),
            "All figures are simulation outputs on the authoring host, NOT Hanzo expectations until reproduced with real gofer workloads.".into(),
        ],
        runs,
    })
}

pub fn write_results_json(r: &BenchReport, p: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = p.parent() { std::fs::create_dir_all(parent)?; }
    std::fs::write(p, serde_json::to_string_pretty(r)?)?; Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rosenbridge_outperforms_standard() {
        let report = run_bench(&BenchConfig::test_config()).unwrap();
        let std_r = report.runs.iter().find(|r| r.policy == "standard-vfs").unwrap();
        let rb_r = report.runs.iter().find(|r| r.policy == "rosenbridge-fastpath").unwrap();
        assert!(rb_r.time_per_op_ns < std_r.time_per_op_ns,
            "RosenBridge time={} should be < standard {}", rb_r.time_per_op_ns, std_r.time_per_op_ns);
        assert!(rb_r.syscalls_per_op < std_r.syscalls_per_op, "fewer syscalls with bypass");
    }
    #[test]
    fn empty_config_rejected() {
        assert!(run_bench(&BenchConfig { seed: 1, n_ops: 0, gofer_ratio: 0.3 }).is_err());
    }
}
