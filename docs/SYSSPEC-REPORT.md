# SYSSPEC Research & Implementation Report: VFS Invariants

## 1. Paper & License Audit
- **Primary Reference**: [SYSSPEC: Systematic Specification-Guided Testing of Storage Systems (FAST '26)](https://www.usenix.org/conference/fast26/presentation/liu-qingyuan).
- **Design Summary**: SYSSPEC proposes using declarative specifications to generate test cases that explore the state space of storage systems, specifically focusing on crash consistency and concurrency.
- **License Compatibility**: The implementation provided here is a clean-room Go implementation of the concepts described in the paper, licensed under **Apache-2.0** to match the `vfs` repository. No proprietary code from the authors was used.

## 2. Matched Baselines
- **Upstream Baseline**: Existing `fence_test.go` and `crash_test.go` in `hanzoai/vfs`.
- **Hanzo Baseline**: The `sysspec_test.go` suite replicates the "Red fault" scenarios but adds automated state-transition validation.
- **Comparison**: While `fence_test.go` checks basic round increments, the SYSSPEC implementation checks the *interleaving* of failed commits from stale rounds during active writes.

## 3. Falsifiable Hypothesis
- **Hypothesis**: A machine-checkable specification will detect "Stale Head Overwrites" in high-concurrency scenarios that standard unit tests (like `fence_test.go`) miss by not simulating interleaved network delays.
- **Expected Failure Mode**: If the `Round` logic is bypassed, the model checker will detect a non-linear Merkle root transition.

## 4. Measurements & HIP Update
- **Status**: Active (Wave 1 Priority #6).
- **Results**: 
    - Fencing Invariant: **PASSED** (Verified that Round $N-1$ cannot commit after Round $N$ is attached).
    - Integrity Invariant: **PASSED** (Verified that 1-bit mutations in chunks trigger `Cipher` rejection).
    - Atomic Head: **PASSED** (Verified that partial writes to the head do not corrupt the previous valid state).

## 5. Safety & Isolation
- The tests use a mocked object store to ensure no side effects on production S3 buckets.
- Multi-tenant isolation is simulated by using different Volume prefixes during the spec-checking phase.
