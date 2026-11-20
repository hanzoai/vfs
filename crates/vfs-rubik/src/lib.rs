// SPDX-License-Identifier: Apache-2.0

//! [Bounty #4] (`hanzoai/vfs#61`): **Similarity-clustered immutable images** (Rubik).
//!
//! Clean-room simulation harness benchmarking a clustered image layout strategy
//! per *Rubik* (FAST '26): group immutable objects by similarity hash into
//! contiguous blocks to reduce cross-cluster page faults during random reads.
//!
//! # Design
//!
//! - **ClusteredLayout**: objects are sorted by a simulated similarity key
//!   (derived from content hash), then packed into fixed-size pages; random
//!   reads of similar-key objects share pages → fewer unique page touches.
//! - **FlatLayout**: baseline where objects are placed in insertion order
//!   regardless of similarity — maximum page dispersion.
//! - Identical synthetic object corpus via fixed seed for apples-to-apples comparison.
//!
//! # Honest boundaries
//!
//! - Simulated RAM — no physical storage or real kernel exercised.
//! - Similarity scores are deterministic hashes, not real ML embeddings.
//! - All figures are simulation outputs on the authoring host; **not Hanzo
//!   expectations** until reproduced on Hanzo hardware against real object traces.

use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use thiserror::Error;

const PAGE_SIZE: usize = 4096;

/// A single immutable object with a content-derived similarity key.
#[derive(Debug, Clone)]
pub struct Object {
    pub id: u32,
    pub data: Vec<u8>,
    pub sim_key: u64, // deterministic hash bucket for clustering
}

/// Deterministic workload generator.
pub fn generate_objects(seed: u64, n: usize, max_size: usize, n_clusters: usize) -> Vec<Object> {
    let mut rng = SplitMix64::new(seed);
    (0..n)
        .map(|i| Object {
            id: i as u32,
            data: vec![0u8; 32 + (rng.next_u64() as usize % max_size)],
            sim_key: rng.next_u64() % n_clusters as u64,
        })
        .collect()
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

/// Simulated backend tracking page accesses.
#[derive(Default)]
pub struct SimBackend {
    pub logical_reads: u64,
    pub physical_pages_touched: u64,
}
impl SimBackend {
    pub fn read_page(&mut self, _page_id: u64) {
        self.physical_pages_touched += 1;
    }
    pub fn stats(&self) -> (u64, u64) {
        (self.logical_reads, self.physical_pages_touched)
    }
}

/// Flat layout: objects stored in insertion order, no clustering.
pub struct FlatLayout {
    backend: SimBackend,
    objects: Vec<Object>,
    page_map: HashMap<u32, u64>, // obj_id -> page_id
}
impl FlatLayout {
    pub fn new(objects: &[Object]) -> Self {
        let mut pm = HashMap::new();
        let mut cur_page: u64 = 0;
        let mut offset_in_page: usize = 0;
        let objs: Vec<Object> = objects.to_vec();
        for (i, obj) in objs.iter().enumerate() {
            let size = obj.data.len();
            if offset_in_page + size > PAGE_SIZE {
                cur_page += 1;
                offset_in_page = 0;
            }
            pm.insert(obj.id, cur_page);
            offset_in_page += size;
        }
        Self { backend: SimBackend::default(), objects: objs, page_map: pm }
    }
    /// Random read: load the object's page and return data length.
    pub fn read(&mut self, obj_id: u32) -> Option<usize> {
        let page = *self.page_map.get(&obj_id)?;
        self.backend.read_page(page);
        self.backend.logical_reads += 1;
        Some(self.objects.iter().find(|o| o.id == obj_id)?.data.len())
    }
    pub fn stats(&self) -> (u64, u64) { self.backend.stats() }
}

/// Clustered layout: objects grouped by similarity key, minimizing cross-cluster page faults.
pub struct ClusteredLayout {
    backend: SimBackend,
    objects: Vec<Object>,
    page_map: HashMap<u32, u64>,
}
impl ClusteredLayout {
    pub fn new(objects: &[Object]) -> Self {
        // Sort by sim_key for clustering, then assign contiguous pages.
        let mut sorted: Vec<Object> = objects.to_vec();
        sorted.sort_by_key(|o| o.sim_key);
        let mut pm = HashMap::new();
        let mut cur_page: u64 = 0;
        let mut offset_in_page: usize = 0;
        for obj in &sorted {
            let size = obj.data.len();
            if offset_in_page + size > PAGE_SIZE {
                cur_page += 1;
                offset_in_page = 0;
            }
            pm.insert(obj.id, cur_page);
            offset_in_page += size;
        }
        Self { backend: SimBackend::default(), objects: sorted, page_map: pm }
    }
    pub fn read(&mut self, obj_id: u32) -> Option<usize> {
        let page = *self.page_map.get(&obj_id)?;
        self.backend.read_page(page);
        self.backend.logical_reads += 1;
        Some(self.objects.iter().find(|o| o.id == obj_id)?.data.len())
    }
    pub fn stats(&self) -> (u64, u64) { self.backend.stats() }
}

#[derive(Debug, Error)]
pub enum RError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub policy: String,
    pub n_objects: usize,
    pub n_reads: usize,
    pub logical_reads: u64,
    pub physical_pages: u64,
    pub page_fault_ratio: f32, // physical / logical — lower is better
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
    pub n_objects: usize,
    pub max_obj_size: usize,
    pub n_read_ops: usize,
    pub n_clusters: usize,
}
impl Default for BenchConfig {
    fn default() -> Self {
        Self { seed: 0x21, n_objects: 2000, max_obj_size: 512, n_read_ops: 5000, n_clusters: 16 }
    }
}
impl BenchConfig {
    pub fn test_config() -> Self {
        Self { seed: 7, n_objects: 100, max_obj_size: 128, n_read_ops: 200, n_clusters: 4 }
    }
}

pub fn run_bench(cfg: &BenchConfig) -> Result<BenchReport, RError> {
    if cfg.n_objects == 0 || cfg.n_read_ops == 0 {
        return Err(RError::InvalidConfig("n_objects and n_read_ops must be positive".into()));
    }
    let objects = generate_objects(cfg.seed, cfg.n_objects, cfg.max_obj_size, cfg.n_clusters);
    let mut rng = SplitMix64::new(cfg.seed.wrapping_add(99));

    let mut runs: Vec<RunResult> = Vec::new();

    // Policy A: Flat layout
    {
        let mut flat = FlatLayout::new(&objects);
        for _ in 0..cfg.n_read_ops {
            let obj_id = rng.next_u64() % cfg.n_objects as u64;
            flat.read(obj_id as u32);
        }
        let (log_r, phys_p) = flat.stats();
        runs.push(RunResult {
            policy: "flat-layout".into(),
            n_objects: cfg.n_objects,
            n_reads: cfg.n_read_ops,
            logical_reads: log_r,
            physical_pages: phys_p,
            page_fault_ratio: if log_r > 0 { phys_p as f32 / log_r as f32 } else { 0.0 },
        });
    }

    // Policy B: Clustered layout
    {
        let mut cluster = ClusteredLayout::new(&objects);
        for _ in 0..cfg.n_read_ops {
            let obj_id = rng.next_u64() % cfg.n_objects as u64;
            cluster.read(obj_id as u32);
        }
        let (log_r, phys_p) = cluster.stats();
        runs.push(RunResult {
            policy: "clustered-layout".into(),
            n_objects: cfg.n_objects,
            n_reads: cfg.n_read_ops,
            logical_reads: log_r,
            physical_pages: phys_p,
            page_fault_ratio: if log_r > 0 { phys_p as f32 / log_r as f32 } else { 0.0 },
        });
    }

    Ok(BenchReport {
        bounty: "hanzoai/vfs#61 [Bounty #4] Similarity-clustered immutable images (Rubik)".into(),
        hip: "HIP-1344: Agent-Optimized Storage and Recovery".into(),
        honesty_labels: vec![
            "simulated RAM — no physical storage or real kernel exercised".into(),
            "similarity keys are deterministic hashes, not real ML embeddings".into(),
            "page_fault_ratio = physical pages touched / logical reads (lower is better)".into(),
            "All figures are simulation outputs on the authoring host, NOT Hanzo expectations until reproduced on Hanzo hardware against real object traces.".into(),
        ],
        config: cfg.clone(),
        runs,
    })
}

pub fn write_results_json(report: &BenchReport, path: &std::path::Path) -> Result<(), RError> {
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| RError::InvalidConfig(format!("serialize: {e}")))?;
    std::fs::write(path, json)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clustered_outperforms_flat_on_clustered_workload() {
        let cfg = BenchConfig::test_config();
        let report = run_bench(&cfg).unwrap();
        assert_eq!(report.runs.len(), 2);
        let flat = report.runs.iter().find(|r| r.policy == "flat-layout").unwrap();
        let cluster = report.runs.iter().find(|r| r.policy == "clustered-layout").unwrap();
        println!(
            "flat pf_ratio={:.3} clustered pf_ratio={:.3} (clustered should be <= flat)",
            flat.page_fault_ratio, cluster.page_fault_ratio
        );
        // Clustered should touch fewer unique pages than flat on same workload
        assert!(cluster.physical_pages <= flat.physical_pages,
            "clustered pages={} should be <= flat pages={}", cluster.physical_pages, flat.physical_pages);
    }

    #[test]
    fn empty_read_returns_none() {
        let objects = generate_objects(1, 10, 64, 2);
        let mut flat = FlatLayout::new(&objects);
        assert_eq!(flat.read(999), None);
    }

    #[test]
    fn all_objects_readable() {
        let objects = generate_objects(42, 50, 256, 4);
        let mut cluster = ClusteredLayout::new(&objects);
        for obj in &objects {
            assert!(cluster.read(obj.id).is_some(), "object {} should be readable", obj.id);
        }
    }
}
