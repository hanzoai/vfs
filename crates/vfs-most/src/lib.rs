// SPDX-License-Identifier: Apache-2.0

//! [Bounty #5] (`hanzoai/vfs#62`): **Selective hot-data mirroring** (MOST).
//!
//! Clean-room simulation harness benchmarking a hot/cold data mirror policy
//! per *MOST* (FAST '26): maintain an in-memory mirror of frequently-accessed
//! objects ("hot") alongside on-disk storage for cold data, measuring read
//! latency savings vs. pure-disk baseline at varying access locality patterns.
//!
//! # Design
//!
//! - **MirrorWriter**: LRU-style hot-set mirror (capacity N pages); every read
//!   that misses the mirror triggers a disk fetch and optionally evicts a cold
//!   entry to make room.
//! - **DiskOnly**: every read goes straight to the simulated disk layer.
//! - Synthetic access trace with configurable temporal locality (zipf-like).
//!
//! # Honest boundaries
//!
//! - Simulated backend — no real SSD/HDD or NVMe exercised.
//! - Mirror hit/miss is tracked logically, not via real page cache.
//! - All figures are simulation outputs on the authoring host; **not Hanzo
//!   expectations** until reproduced on Hanzo hardware.

use std::collections::VecDeque;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use thiserror::Error;

const PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub struct AccessOp {
    pub obj_id: u32,
    pub len: usize,
}

/// Deterministic access-trace generator with zipf-like locality.
pub fn generate_trace(seed: u64, n_ops: usize, n_objects: usize) -> Vec<AccessOp> {
    let mut rng = SplitMix64::new(seed);
    (0..n_ops)
        .map(|_| AccessOp {
            obj_id: zipf_pick(&mut rng, n_objects as u64),
            len: 64 + (rng.next_u64() as usize % PAGE_SIZE),
        })
        .collect()
}

fn zipf_pick(rng: &mut SplitMix64, n: u64) -> u32 {
    // Simple approximation: bias toward lower IDs using logarithmic distribution.
    let raw = rng.next_u64();
    let biased = (raw % (n * n)) as u64;
    (biased.isqrt() % n.max(1)) as u32
}

#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    pub fn new(seed: u64) -> Self { Self { state: seed } }
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[derive(Default)]
pub struct SimDisk {
    pub disk_reads: u64,
}
impl SimDisk {
    pub fn read(&mut self, _obj_id: u32, _len: usize) {
        self.disk_reads += 1;
    }
}

/// Hot-data mirror with LRU eviction.
pub struct MostWriter {
    disk: SimDisk,
    mirror: VecDeque<(u32, Vec<u8>)>, // (obj_id, data) ordered by recency
    capacity_pages: usize,
}
impl MostWriter {
    pub fn new(capacity_pages: usize) -> Self {
        Self { disk: SimDisk::default(), mirror: VecDeque::new(), capacity_pages }
    }
    pub fn read(&mut self, obj_id: u32, len: usize) {
        // Check mirror hit.
        if let Some(pos) = self.mirror.iter().position(|(id, _)| *id == obj_id) {
            let item = self.mirror.remove(pos).unwrap();
            self.mirror.push_back(item); // move to front (most recent)
            return; // hit — no disk read
        }
        // Miss — fetch from disk.
        self.disk.read(obj_id, len);
        // Insert into mirror, evict if over capacity.
        self.mirror.push_back((obj_id, vec![0u8; len]));
        while self.mirror.len() > self.capacity_pages * (PAGE_SIZE / 128.max(len)) {
            self.mirror.pop_front();
        }
    }
    pub fn stats(&self) -> (u64, usize) {
        (self.disk.disk_reads, self.mirror.len())
    }
}

/// Pure disk baseline.
pub struct DiskOnly {
    disk: SimDisk,
}
impl DiskOnly {
    pub fn new() -> Self { Self { disk: SimDisk::default() } }
    pub fn read(&mut self, _obj_id: u32, _len: usize) {
        self.disk.read(_obj_id, _len);
    }
    pub fn stats(&self) -> u64 { self.disk.disk_reads }
}

#[derive(Debug, Error)]
pub enum MError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub policy: String,
    pub n_ops: usize,
    pub disk_reads: u64,
    pub mirror_hit_rate: f32, // (ops - disk_reads) / ops
    pub mirror_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub bounty: String,
    pub hip: String,
    pub honesty_labels: Vec<String>,
    pub config: BenchConfig,
    pub runs: Vec<RunResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
    pub seed: u64,
    pub n_ops: usize,
    pub n_objects: usize,
    pub mirror_capacity_pages: usize,
}
impl Default for BenchConfig {
    fn default() -> Self {
        Self { seed: 0x21, n_ops: 5000, n_objects: 500, mirror_capacity_pages: 8 }
    }
}
impl BenchConfig {
    pub fn test_config() -> Self {
        Self { seed: 7, n_ops: 100, n_objects: 20, mirror_capacity_pages: 2 }
    }
}

pub fn run_bench(cfg: &BenchConfig) -> Result<BenchReport, MError> {
    if cfg.n_ops == 0 || cfg.n_objects == 0 {
        return Err(MError::InvalidConfig("n_ops and n_objects must be positive".into()));
    }
    let trace = generate_trace(cfg.seed, cfg.n_ops, cfg.n_objects);
    let mut runs: Vec<RunResult> = Vec::new();

    // Policy A: Disk-only
    {
        let mut d = DiskOnly::new();
        for op in &trace { d.read(op.obj_id, op.len); }
        let disk_reads = d.stats();
        runs.push(RunResult {
            policy: "disk-only".into(),
            n_ops: cfg.n_ops,
            disk_reads,
            mirror_hit_rate: 0.0,
            mirror_size: 0,
        });
    }

    // Policy B: MOST mirror at various capacities
    for cap in [1, 4, 8, 16] {
        let mut m = MostWriter::new(cap);
        for op in &trace { m.read(op.obj_id, op.len); }
        let (disk_reads, mir_size) = m.stats();
        runs.push(RunResult {
            policy: format!("most-capacity={cap}"),
            n_ops: cfg.n_ops,
            disk_reads,
            mirror_hit_rate: if cfg.n_ops > 0 {
                (cfg.n_ops as u64 - disk_reads) as f32 / cfg.n_ops as f32
            } else { 0.0 },
            mirror_size: mir_size,
        });
    }

    Ok(BenchReport {
        bounty: "hanzoai/vfs#62 [Bounty #5] Selective hot-data mirroring (MOST)".into(),
        hip: "HIP-1344: Agent-Optimized Storage and Recovery".into(),
        honesty_labels: vec![
            "simulated backend — no real SSD/HDD/NVMe exercised".into(),
            "mirror hit/miss tracked logically, not via real page cache".into(),
            "access trace uses approximate zipf distribution, not real workload".into(),
            "All figures are simulation outputs on the authoring host, NOT Hanzo expectations until reproduced on Hanzo hardware.".into(),
        ],
        config: cfg.clone(),
        runs,
    })
}

pub fn write_results_json(report: &BenchReport, path: &std::path::Path) -> Result<(), MError> {
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| MError::InvalidConfig(format!("serialize: {e}")))?;
    std::fs::write(path, json)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirror_reduces_disk_reads_vs_baseline() {
        let cfg = BenchConfig::test_config();
        let report = run_bench(&cfg).unwrap();
        assert!(report.runs.len() >= 2);
        let disk_only = report.runs.iter().find(|r| r.policy == "disk-only").unwrap();
        let most = report.runs.iter().find(|r| r.policy.starts_with("most-")).unwrap();
        assert!(most.disk_reads < disk_only.disk_reads,
            "mirror disk_reads={} should be < disk_only={}", most.disk_reads, disk_only.disk_reads);
        assert!(most.mirror_hit_rate > 0.0, "mirror should have some hits");
    }

    #[test]
    fn larger_mirror_captures_more() {
        let cfg = BenchConfig::test_config();
        let report = run_bench(&cfg).unwrap();
        // Find the two most-capacity entries.
        let caps: Vec<&RunResult> = report.runs.iter()
            .filter(|r| r.policy.starts_with("most-"))
            .collect();
        if caps.len() >= 2 {
            // Higher capacity should have >= hit rate (weak monotonicity).
            let smaller = &caps[0];
            let larger = &caps[caps.len()-1];
            assert!(larger.mirror_hit_rate >= smaller.mirror_hit_rate - 0.01,
                "larger cap hit={} should >= smaller hit={}", larger.mirror_hit_rate, smaller.mirror_hit_rate);
        }
    }
}
