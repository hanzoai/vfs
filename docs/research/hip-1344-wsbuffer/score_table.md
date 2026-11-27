# HIP-1344 WSBuffer Comparative Score Table

Measured on Linux `linux/amd64` (`Intel(R) Xeon(R) CPU @ 2.30GHz`, 2 vCPUs, Go `go1.27.1`) against real on-disk OS journal segment files (`journal.Files`, `4096 B` physical pages, `fdatasync`). Machine-readable source data: [`raw_results.json`](raw_results.json).

Reproduce directly:

```sh
go run ./tools/wsbuffer-bench -out docs/research/hip-1344-wsbuffer/raw_results.json
go test -run=^$ -bench=BenchmarkWSBuffer -benchmem -benchtime=200ms ./journal
```

## 1. Real-Disk Comparative Suite (`tools/wsbuffer-bench`)

| Workload | Strategy | Phys Write Ops | Phys Write Amp | Direct Bypass Ops | Scrap Copy (KiB) | `/proc/self/io` `syscw` | Wall (ms) | CPU (ms) | Throughput (MB/s) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `small_unaligned_128B` (16,384 × 128 B = 2 MiB) | `baseline_journal_main` | 16,384 | 33.00x | 0 | 0.0 | 16,384 | 41.25 | 40.52 | 50.8 |
| `small_unaligned_128B` | `always_copy_buffered_writer` | 172 | 1.00x | 0 | 2,048.0 | 172 | 5.76 | 3.91 | 364.0 |
| `small_unaligned_128B` | **`wsbuffer_adaptive_bypass`** | **172** | **1.00x** | **0** | **2,048.0** | **172** | **6.39** | **3.09** | **328.4** |
| `small_unaligned_512B` (8,192 × 512 B = 4 MiB) | `baseline_journal_main` | 8,192 | 9.00x | 0 | 0.0 | 8,192 | 27.49 | 20.57 | 152.6 |
| `small_unaligned_512B` | `always_copy_buffered_writer` | 343 | 1.00x | 0 | 4,096.0 | 343 | 9.01 | 4.07 | 465.6 |
| `small_unaligned_512B` | **`wsbuffer_adaptive_bypass`** | **343** | **1.00x** | **0** | **4,096.0** | **343** | **10.03** | **5.11** | **418.0** |
| `small_unaligned_1KiB` (8,192 × 1 KiB = 8 MiB) | `baseline_journal_main` | 8,192 | 5.00x | 0 | 0.0 | 8,192 | 49.91 | 22.36 | 168.1 |
| `small_unaligned_1KiB` | `always_copy_buffered_writer` | 684 | 1.00x | 0 | 8,192.0 | 684 | 11.60 | 7.37 | 723.3 |
| `small_unaligned_1KiB` | **`wsbuffer_adaptive_bypass`** | **684** | **1.00x** | **0** | **8,192.0** | **684** | **11.28** | **7.22** | **743.7** |
| `small_unaligned_2KiB` (4,096 × 2 KiB = 8 MiB) | `baseline_journal_main` | 4,096 | 3.00x | 0 | 0.0 | 4,096 | 16.77 | 13.93 | 500.2 |
| `small_unaligned_2KiB` | `always_copy_buffered_writer` | 684 | 1.00x | 0 | 8,192.0 | 684 | 10.31 | 7.44 | 814.0 |
| `small_unaligned_2KiB` | **`wsbuffer_adaptive_bypass`** | **684** | **1.00x** | **0** | **8,192.0** | **684** | **12.10** | **7.00** | **693.3** |
| `random_4KiB_partial_512B` (16,384 × 512 B across 16 hot pages) | `baseline_journal_main` | 16,384 | 8.00x | 0 | 0.0 | 16,384 | 29.83 | 30.09 | 281.2 |
| `random_4KiB_partial_512B` | `always_copy_buffered_writer` | 16 | 0.01x | 0 | 8,192.0 | 16 | 3.92 | 1.88 | 2,138.0 |
| `random_4KiB_partial_512B` | **`wsbuffer_adaptive_bypass`** | **16** | **0.01x** | **0** | **8,192.0** | **16** | **5.32** | **1.93** | **1,577.1** |
| `mixed_70_small_30_large` (70% 256 B + 20% 16 KiB + 10% 64 KiB) | `baseline_journal_main` | 6,000 | 1.29x | 0 | 0.0 | 6,000 | 46.32 | 29.11 | 1,296.7 |
| `mixed_70_small_30_large` | `always_copy_buffered_writer` | 7,455 | 1.00x | 0 | 58,650.0 | 7,455 | 63.42 | 44.70 | 946.9 |
| `mixed_70_small_30_large` | **`wsbuffer_adaptive_bypass`** | **1,889** | **1.00x** | **1,800** | **1,050.0** | **1,889** | **35.59** | **18.36** | **1,687.5** |
| `large_aligned_16KiB` (4,096 × 16 KiB = 64 MiB) | `baseline_journal_main` | 4,096 | 1.00x | 0 | 0.0 | 4,096 | 44.42 | 26.28 | 1,510.7 |
| `large_aligned_16KiB` | `always_copy_buffered_writer` | 4,096 | 1.00x | 0 | 65,536.0 | 4,096 | 55.37 | 39.81 | 1,212.1 |
| `large_aligned_16KiB` | **`wsbuffer_adaptive_bypass`** | **4,096** | **1.00x** | **4,096** | **0.0** | **4,096** | **42.30** | **27.59** | **1,586.6** |
| `large_aligned_64KiB` (1,024 × 64 KiB = 64 MiB) | `baseline_journal_main` | 1,024 | 1.00x | 0 | 0.0 | 1,024 | 55.50 | 16.78 | 1,209.3 |
| `large_aligned_64KiB` | `always_copy_buffered_writer` | 4,096 | 1.00x | 0 | 65,536.0 | 4,096 | 57.99 | 38.15 | 1,157.2 |
| `large_aligned_64KiB` | **`wsbuffer_adaptive_bypass`** | **1,024** | **1.00x** | **1,024** | **0.0** | **1,024** | **50.92** | **16.93** | **1,318.0** |
| `journal_e2e_burst_256B` (2,048 × 256 B, `Journal.Open`, Sync/64) | `baseline_journal_main` | 35 | 1.25x | 0 | 0.0 | 42 | 72.78 | 14.47 | 8.0 |
| `journal_e2e_burst_256B` | `always_copy_buffered_writer` | 98 | 1.25x | 0 | 568.1 | 106 | 75.94 | 21.35 | 7.7 |
| `journal_e2e_burst_256B` | **`wsbuffer_adaptive_bypass`** | **35** | **1.25x** | **32** | **0.0** | **42** | **77.48** | **14.65** | **7.5** |
| `journal_e2e_paced_256B` (1,024 × 256 B paced, `Journal.Open`, Sync/64) | `baseline_journal_main` | 649 | 10.14x | 0 | 0.0 | 656 | 52.87 | 17.18 | 5.5 |
| `journal_e2e_paced_256B` | `always_copy_buffered_writer` | 49 | 1.25x | 0 | 284.1 | 56 | 44.90 | 11.03 | 6.5 |
| `journal_e2e_paced_256B` | **`wsbuffer_adaptive_bypass`** | **49** | **1.25x** | **0** | **284.1** | **56** | **41.40** | **9.40** | **7.0** |

## 2. Honest Negative Regimes

### Negative Regime 1: `neg_sync_every_write_fsync_barrier` (200 × 256 B + `fdatasync` per write)

| Strategy | Phys Write Ops | Phys Write Amp | Scrap Copy (KiB) | `/proc/self/io` `syscw` | Wall (ms) | CPU (ms) | Outcome |
|---|---:|---:|---:|---:|---:|---:|---|
| `baseline_journal_main` | 200 | 16.96x | 0.0 | 200 | 468.55 | 42.40 | Baseline (`fdatasync`-bound) |
| `always_copy_buffered_writer` | 200 | 16.96x | 50.0 | 200 | 461.04 | 47.73 | Zero write-amp gain; `+5.33 ms` CPU copy/bookkeeping |
| `wsbuffer_adaptive_bypass` | 200 | 16.96x | 50.0 | 200 | 450.23 | 51.03 | Zero write-amp gain; `+8.63 ms` CPU overhead (`~43 µs/op`) |

**Takeaway:** Durability barriers (`Sync` / `fdatasync`) strictly flush all dirty scrap pages before returning. Coalescing cannot legally cross a synchronous `fsync` barrier; callers requiring per-record `fsync` receive zero write-amplification benefit.

### Negative Regime 2: `neg_oversized_scrap_buffer_memory_pressure` (Capacity Sweep, 256 B × 16,384 = 4 MiB)

| Scrap Capacity | Phys Write Ops | Phys Write Amp | Max Dirty Window (KiB) | Flush Ops | Wall (ms) | Throughput (MB/s) | Observation |
|---|---:|---:|---:|---:|---:|---:|---|
| `4 KiB` | 1,025 | 1.0010x | 4.0 | 1,025 | 9.35 | 448.7 | Already achieves `1.0010x` page alignment |
| **`16 KiB` (default)** | **343** | **1.0010x** | **15.9** | **342** | **9.08** | **461.9** | **Sweet spot: 3x fewer syscalls, tiny 16 KiB dirty window** |
| `64 KiB` | 70 | 1.0010x | 63.9 | 69 | 7.72 | 543.2 | Diminishing returns; 4x larger uncommitted window |
| `256 KiB` | 18 | 1.0010x | 255.9 | 17 | 7.61 | 551.4 | Identical `1.0010x` write-amp; 16x dirty memory |
| `1024 KiB` | 6 | 1.0010x | 1,023.9 | 5 | 8.58 | 488.6 | Throughput begins degrading (`-11.4%` vs 256 KiB) |
| `4096 KiB` | 3 | 1.0010x | 4,095.9 | 2 | 14.57 | 287.8 | **Regression (`-37.7%` vs 16 KiB)** + 256x dirty window, zero write-amp gain |

## 3. Go Microbenchmarks (`go test -bench=BenchmarkWSBuffer -benchmem ./journal`)

| Benchmark | `baseline_journal_main` | `always_copy_buffered_writer` | `wsbuffer_adaptive_bypass` | Delta vs Baseline / Always-Copy |
|---|---:|---:|---:|---|
| `SmallUnaligned/128B` | 2,618 ns/op (`33.00x` amp, `0 allocs`) | 142.9 ns/op (`1.00x` amp, `0 allocs`) | **148.1 ns/op** (`1.00x` amp, `0 allocs`) | **17.7x faster**, `-97.0%` write-amp |
| `SmallUnaligned/512B` | 2,371 ns/op (`9.00x` amp, `0 allocs`) | 422.8 ns/op (`1.00x` amp, `0 allocs`) | **419.8 ns/op** (`1.00x` amp, `0 allocs`) | **5.6x faster**, `-88.9%` write-amp |
| `SmallUnaligned/1024B` | 2,668 ns/op (`5.00x` amp, `0 allocs`) | 774.0 ns/op (`1.00x` amp, `0 allocs`) | **763.7 ns/op** (`1.00x` amp, `0 allocs`) | **3.5x faster**, `-80.0%` write-amp |
| `SmallUnaligned/2048B` | 2,710 ns/op (`3.00x` amp, `0 allocs`) | 1,428 ns/op (`1.00x` amp, `0 allocs`) | **1,422 ns/op** (`1.00x` amp, `0 allocs`) | **1.9x faster**, `-66.7%` write-amp |
| `Random4KiBPartial` | 1,676 ns/op (`8.00x` amp, `0 allocs`) | 91.09 ns/op (`<0.01x` amp, `0 allocs`) | **93.13 ns/op** (`<0.01x` amp, `0 allocs`) | **18.0x faster**, `-99.9%` write-amp |
| `Mixed70_30` | 4,677 ns/op (`2,140 MB/s`, `1.29x` amp) | 7,904 ns/op (`1,266 MB/s`, `2 allocs`) | **2,929 ns/op** (`3,417 MB/s`, `1.00x` amp, `0 allocs`) | **+59.7% vs baseline**, **+169.9% vs always-copy** |
| `LargeAligned/16KiB` | 5,943 ns/op (`2,757 MB/s`, `0 allocs`) | 8,120 ns/op (`2,018 MB/s`, `4 allocs`) | **5,982 ns/op** (`2,739 MB/s`, `0 allocs`) | Parity (`-0.65%`), **+35.7% vs always-copy** |
| `LargeAligned/64KiB` | 14,371 ns/op (`4,560 MB/s`, `0 allocs`) | 32,870 ns/op (`1,994 MB/s`, `16 allocs`) | **14,601 ns/op** (`4,489 MB/s`, `0 allocs`) | Parity (`-1.57%`), **+125.1% vs always-copy** |
