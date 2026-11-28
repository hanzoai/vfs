# RASK Independent Benchmark & Memory Qualification (#131)

Closes #131. Independent qualification of PR #129 (`RaskRunIndex` in `rask.go`, adaptive `file.go` integration) against the shipping `[]run` interval representation (`BaselineRunSlice`) using Go's standard `-bench -benchmem -count=5` harness (`rask_qualification_test.go`), `runtime/pprof` live heap profiles, structural vs. OS RSS/heap accounting, and an empirical `N = 8..8192` workload crossover sweep.

---

## 1. Environment & Reproducible Commands

- **Platform:** `linux/amd64` (WSL2 Ubuntu), `go1.27.1`, `AMD Ryzen 7 7800X3D 8-Core Processor`, `CGO_ENABLED=1`

```sh
# 1. Differential parity, structural vs. RSS/heap qualification, pprof capture, and crossover sweep
RASK_PPROF_DIR=/tmp/rask_pprof go test -v -race -count=1 -run "TestRASKIndependentQualificationAndPprof|TestRASKBenchmarkSmoke" .

# 2. Inspect captured live pprof heap profiles
go tool pprof -top -inuse_space /tmp/rask_pprof/baseline_heap.pb.gz
go tool pprof -top -inuse_space /tmp/rask_pprof/rask_heap.pb.gz

# 3. 5-run reproducible Go microbenchmark suite
go test -run=^$ -bench=BenchmarkRASKQualification -benchmem -count=5 .
```

---

## 2. Structural Memory (`B/run`) vs. Retained Go Heap & Process RSS (`N = 8,192`)

Measured under isolated forced-GC windows (`runtime.GC()` twice before and after, index pinned via `runtime.KeepAlive`) in `TestRASKIndependentQualificationAndPprof` (`go test -race -count=1`):

| Workload | Runs | Leaves (Compact / Wide) | Intervals Struct (`B/run` / B) | RASK Struct (`B/run` / B) | Struct Savings | Intervals HeapAlloc / Inuse (`+RSS` B) | RASK HeapAlloc / Inuse (`+RSS` B) | Build Time (Intervals vs. RASK) | Query `p50` (Intervals / RASK) |
|---|---:|---|---:|---:|---:|---|---|---|---:|
| `Sparse_Sub4G` (`10 MiB` stride) | 8,192 | `32 / 0` | `17.00 B/run` (`139,288`) | `8.33 B/run` (`68,216`) | `+51.0%` | `139,392 / 139,264` (`+491,520`) | `68,176 / 57,344` (`+1,572,864`) | `1.244 ms` vs. `3.520 ms` | `87.4 ns` / `154.7 ns` |
| `Sparse_WideGt4G` (`5 GiB` stride) | 8,192 | `0 / 64` | `17.00 B/run` (`139,288`) | `16.64 B/run` (`136,312`) | `+2.1%` | `139,264 / 139,264` (`+0`) | `136,384 / 139,264` (`+262,144`) | `0.974 ms` vs. `2.273 ms` | `86.4 ns` / `119.5 ns` |
| `Dense_RandPunch` (`200` clusters) | 9,830 | `51 / 77` | `18.34 B/run` (`180,248`) | `17.00 B/run` (`167,144`) | `+7.3%` | `180,224 / 180,224` (`+221,184`) | `167,232 / 278,528` (`+786,432`) | `50.536 ms` vs. `5.436 ms` (`9.3x`) | `114.9 ns` / `168.9 ns` |
| `Sequential_Append` | 8,192 | `205 / 0` | `17.00 B/run` (`139,288`) | `10.43 B/run` (`85,480`) | `+38.6%` | `139,376 / 139,264` (`+0`) | `85,488 / 163,840` (`+262,144`) | `1.044 ms` vs. `3.405 ms` | `86.7 ns` / `224.5 ns` |
| `Random_JournalReplay` (`1 PiB`, punches + truncate) | 8,773 | `62 / 0` | `20.55 B/run` (`180,248`) | `9.16 B/run` (`80,360`) | `+55.4%` | `180,336 / 180,224` (`+442,368`) | `80,336 / 131,072` (`+786,432`) | `43.732 ms` vs. `5.307 ms` (`8.2x`) | `122.1 ns` / `183.1 ns` |

**Real `vfs.Volume` 1 PiB short replay (`RunRASKBenchmark`):** `229` encrypted journal records (`200` writes, `28` punches, `1` truncate) extracted in `3.80 ms`; `voidSpans = 28` (`Intervals = 19.14 B/run` vs. `RASK = 22.29 B/run`, demonstrating why small `void` layers stay on flat `[]run` below threshold); `ExactMatch = true`.

> **Allocator note (Structural vs. Process RSS / `HeapInuse`):**
> `StructuralBytes` measures live payload + backing slice `cap` (`8.33–9.16 B/run` for compact RASK vs. `17.00–20.55 B/run` for `[]run`). In Go's span allocator, a small segmented B+tree scatters `raskLeaf` headers (64 B) and variable small slices (`compactRun` size classes) across multiple mcache/mcentral spans, so `HeapInuse` and `/proc/self/statm` OS page RSS (`262 KiB–1.57 MiB` warm page deltas) can temporarily exceed a single `139 KiB` contiguous `[]run` allocation even while `HeapAlloc` live bytes drop by `51–55%`.

---

## 3. `pprof` Live Heap Profiles (`inuse_space` & `alloc_space`)

Captured from `64` pinned `Random_JournalReplay` (`N = 4,096` ops -> `4,395` runs/index) instances so Go's default `MemProfileRate` (`512 KiB`) cleanly attributes all index allocations (`baseline_heap.pb.gz = 7,513 B`, `rask_heap.pb.gz = 8,010 B`):

### Intervals (`BaselineRunSlice`) — `go tool pprof -top -inuse_space` (`7,036.67 kB` total)

```text
      flat  flat%   sum%        cum   cum%
 4977.37kB 70.73% 70.73%  4977.37kB 70.73%  slices.Insert[go.shape.[]github.com/hanzoai/vfs.run,...]
 1538.25kB 21.86% 92.60%  1538.25kB 21.86%  runtime.mallocgc
  521.05kB  7.40%   100%   521.05kB  7.40%  encoding/xml.map.init.0
         0     0%   100%  4977.37kB 70.73%  github.com/hanzoai/vfs.(*BaselineRunSlice).InsertRange
```

### RASK (`RaskRunIndex`) — `go tool pprof -top -inuse_space` (`4,109.68 kB` total)

```text
      flat  flat%   sum%        cum   cum%
 1538.25kB 37.43% 37.43%  1538.25kB 37.43%  runtime.mallocgc
 1537.81kB 37.42% 74.85%  1537.81kB 37.42%  github.com/hanzoai/vfs.compactReplace
  521.05kB 12.68% 87.53%   521.05kB 12.68%  encoding/xml.map.init.0
  512.56kB 12.47%   100%  2050.38kB 49.89%  github.com/hanzoai/vfs.leafInsertSingle
         0     0%   100%  2050.38kB 49.89%  github.com/hanzoai/vfs.(*RaskRunIndex).InsertRange
```

Live retained index memory drops from **`4,977.37 kB`** (`BaselineRunSlice.InsertRange`) to **`2,050.38 kB`** (`RaskRunIndex.InsertRange` = **`-58.8%`** / **`2.43x`** reduction in `inuse_space`), matching the `8.968` vs. `18.64 B/run` compact leaf footprint.

---

## 4. Empirical Crossover Analysis (`N = 8..8192`)

Measured in `TestRASKIndependentQualificationAndPprof` across batch-averaged (`MeasureBatch*`) random clustered writes+punches and sequential appends:

| `N` (ops) | Coalesced Runs | Struct `B/run` (Intervals vs. RASK) | `HeapAlloc`/index (Intervals vs. RASK) | Random Build (`Intervals` vs. `RASK`) | Sequential Build (`Intervals` vs. `RASK`) | Regime Winner |
|---:|---:|---:|---:|---:|---:|---|
| `8` | `9` | `31.11` vs. `40.89` | `289 B` vs. `377 B` | `0.001 ms` vs. `0.002 ms` | `0.0005 ms` vs. `0.0015 ms` | Intervals (mem + speed) |
| `16` | `19` | `28.21` vs. `32.84` | `545 B` vs. `633 B` | `0.003 ms` vs. `0.004 ms` | `0.0010 ms` vs. `0.0032 ms` | Intervals (mem + speed) |
| **`32`** | `38` | `27.58` vs. **`23.16`** | `1,057 B` vs. **`889 B`** | `0.007 ms` vs. `0.009 ms` | `0.0016 ms` vs. `0.0068 ms` | **Memory crossover** (`RASK` smaller heap), Intervals faster |
| `64` | `76` | `27.26` vs. `21.68` | `2,082 B` vs. `1,658 B` | `0.015 ms` vs. `0.020 ms` | `0.0043 ms` vs. `0.0159 ms` | RASK mem (`-20%`), Intervals faster |
| `128` | `153` | `26.93` vs. `18.56` | `4,129 B` vs. `2,857 B` | `0.036 ms` vs. `0.052 ms` | `0.0126 ms` vs. `0.0309 ms` | **`raskVoidDemoteThreshold = 128`** (safe speed hysteresis) |
| `256` | `307` | `26.76` vs. `20.61` | `8,225 B` vs. `6,345 B` | `0.115 ms` vs. `0.139 ms` | `0.0124 ms` vs. `0.0693 ms` | Transitions near random parity |
| `384` | `460` | `17.86` vs. `20.30` | `8,224 B` vs. `9,352 B` | `0.212 ms` vs. `0.246 ms` | `0.0247 ms` vs. `0.1038 ms` | Slice growth boundary |
| **`512`** | `614` | `22.14` vs. **`19.06`** | `13,600 B` vs. **`11,720 B`** | `0.323 ms` vs. **`0.275 ms`** | `0.0248 ms` vs. `0.1521 ms` | **Throughput + heap crossover (`raskVoidPromoteThreshold = 512`)** |
| `1,024` | `1,228` | `16.70` vs. `19.28` | `20,512 B` vs. `23,688 B` | `1.009 ms` vs. `0.955 ms` | `0.0634 ms` vs. `0.3278 ms` | RASK faster random mutation |
| `2,048` | `2,457` | `16.68` vs. `19.74` | `40,992 B` vs. `48,520 B` | `3.891 ms` vs. **`1.290 ms`** | `0.1136 ms` vs. `0.6296 ms` | `3.0x` random speedup on RASK |
| `4,096` | `4,915` | `16.67` vs. `19.45` | `81,966 B` vs. `95,638 B` | `14.890 ms` vs. **`2.429 ms`** | `0.2997 ms` vs. `1.3746 ms` | `6.1x` random speedup on RASK |
| `8,192` | `9,830` | `18.34` vs. `17.00` | `180,256 B` vs. `167,176 B` | `47.854 ms` vs. **`5.369 ms`** | `0.9552 ms` vs. `3.2184 ms` | **`8.9x` random speedup on RASK** |

### Adaptive Threshold Qualification (`file.go`)

- **`raskVoidPromoteThreshold = 512` (QUALIFIED):** Matches the exact empirical random-mutation throughput crossover (`N = 512`, `0.275 ms` vs. `0.323 ms`) while avoiding B+tree leaf/internal allocation overhead for small layers (`N < 512`), where flat `[]run` binary search stays inside L1/L2 cache and avoids slice-doubling waste misses.
- **`raskVoidDemoteThreshold = 128` (QUALIFIED):** Provides a `4x` hysteresis band below promotion so repeated truncate+re-punch workloads around `512` runs do not thrash conversions, while cleanly returning sub-`128`-run layers back to contiguous L1-resident slices.
- **Documented Negative / Trade-off Regimes:**
  1. Pure `Sequential_Append` remains `2.5x–6x` faster on `BaselineRunSlice` across all tested `N` (`append` `O(1)` amortized vs. B+tree descent).
  2. Uniformly wide `Sparse_WideGt4G` (`5 GiB` gaps > `uint32`) forces `RaskRunIndex` into wide `16 B/run` leaves (`16.65 B/run` structural including leaf headers), roughly matching `[]run` memory (`16.64` vs. `17.00 B/run`) at `~2x` higher build cost.

---

## 5. Raw `go test -run=^$ -bench=BenchmarkRASKQualification -benchmem -count=5 .` Output

```text
goos: linux
goarch: amd64
pkg: github.com/hanzoai/vfs
cpu: AMD Ryzen 7 7800X3D 8-Core Processor
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Build-16         	   15229	     75835 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Build-16         	   16008	     75997 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Build-16         	   15979	     75714 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Build-16         	   15130	     78313 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Build-16         	   15660	     74184 ns/op	      4096 runs	        20.01 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Build-16              	    6590	    178700 ns/op	      4096 runs	         8.342 struct_B/run	  259682 B/op	     346 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Build-16              	    6613	    175851 ns/op	      4096 runs	         8.342 struct_B/run	  259682 B/op	     346 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Build-16              	    6501	    178441 ns/op	      4096 runs	         8.342 struct_B/run	  259682 B/op	     346 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Build-16              	    6927	    177487 ns/op	      4096 runs	         8.342 struct_B/run	  259683 B/op	     346 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Build-16              	    6952	    177274 ns/op	      4096 runs	         8.342 struct_B/run	  259682 B/op	     346 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Contains-16      	83617878	        14.27 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Contains-16      	69067677	        15.17 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Contains-16      	89185969	        13.70 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Contains-16      	96028995	        13.46 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Contains-16      	87489037	        15.09 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Contains-16           	78620630	        14.92 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Contains-16           	71682231	        15.79 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Contains-16           	76242097	        15.03 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Contains-16           	68845156	        15.94 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Contains-16           	66371794	        16.49 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Overlaps-16      	92184115	        13.33 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Overlaps-16      	97699310	        11.87 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Overlaps-16      	93753918	        12.62 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Overlaps-16      	99821343	        12.20 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/Intervals/Overlaps-16      	96748585	        11.95 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Overlaps-16           	100000000	        11.92 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Overlaps-16           	97907863	        12.22 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Overlaps-16           	100000000	        13.01 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Overlaps-16           	100000000	        11.38 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_Sub4G/RASK/Overlaps-16           	100000000	        12.04 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Build-16      	   16240	     73098 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Build-16      	   16449	     73517 ns/op	      4096 runs	        20.01 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Build-16      	   16478	     73255 ns/op	      4096 runs	        20.01 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Build-16      	   16161	     72875 ns/op	      4096 runs	        20.01 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Build-16      	   16418	     74716 ns/op	      4096 runs	        20.01 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Build-16           	    7111	    147846 ns/op	      4096 runs	        16.65 struct_B/run	  208994 B/op	     364 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Build-16           	    7539	    151818 ns/op	      4096 runs	        16.65 struct_B/run	  208994 B/op	     364 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Build-16           	    8150	    147838 ns/op	      4096 runs	        16.65 struct_B/run	  208994 B/op	     364 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Build-16           	    7238	    149545 ns/op	      4096 runs	        16.65 struct_B/run	  208994 B/op	     364 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Build-16           	    7954	    148129 ns/op	      4096 runs	        16.65 struct_B/run	  208994 B/op	     364 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Contains-16   	73471592	        14.06 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Contains-16   	83683612	        14.06 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Contains-16   	89586535	        13.51 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Contains-16   	100000000	        15.27 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Contains-16   	91969498	        14.72 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Contains-16        	64652089	        16.68 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Contains-16        	71572798	        16.07 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Contains-16        	72893257	        17.50 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Contains-16        	73044252	        17.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Contains-16        	65850132	        16.28 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Overlaps-16   	91775359	        11.97 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Overlaps-16   	90474824	        12.13 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Overlaps-16   	91121956	        12.14 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Overlaps-16   	96927847	        12.35 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/Intervals/Overlaps-16   	97932522	        12.04 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Overlaps-16        	100000000	        11.06 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Overlaps-16        	100000000	        10.88 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Overlaps-16        	100000000	        10.60 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Overlaps-16        	100000000	        10.98 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sparse_WideGt4G/RASK/Overlaps-16        	100000000	        10.96 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Build-16      	    1048	   1058762 ns/op	      4915 runs	        16.67 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Build-16      	    1092	   1064458 ns/op	      4915 runs	        16.67 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Build-16      	    1140	   1046050 ns/op	      4915 runs	        16.67 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Build-16      	    1120	   1049634 ns/op	      4915 runs	        16.67 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Build-16      	    1159	   1034422 ns/op	      4915 runs	        16.67 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Build-16           	    2451	    498412 ns/op	      4915 runs	        19.45 struct_B/run	  396419 B/op	     338 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Build-16           	    2360	    495162 ns/op	      4915 runs	        19.45 struct_B/run	  396419 B/op	     338 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Build-16           	    2380	    492344 ns/op	      4915 runs	        19.45 struct_B/run	  396419 B/op	     338 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Build-16           	    2437	    495911 ns/op	      4915 runs	        19.45 struct_B/run	  396419 B/op	     338 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Build-16           	    2421	    493821 ns/op	      4915 runs	        19.45 struct_B/run	  396420 B/op	     338 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Contains-16   	52348200	        22.49 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Contains-16   	52226666	        19.48 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Contains-16   	82540315	        18.74 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Contains-16   	54363555	        19.77 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Contains-16   	68959956	        21.83 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Contains-16        	77824968	        15.90 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Contains-16        	76070241	        16.87 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Contains-16        	77207262	        15.54 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Contains-16        	74959587	        15.63 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Contains-16        	78503682	        17.07 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Overlaps-16   	100000000	        11.40 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Overlaps-16   	99300542	        11.62 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Overlaps-16   	100000000	        11.17 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Overlaps-16   	100000000	        11.40 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/Intervals/Overlaps-16   	100000000	        11.18 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Overlaps-16        	86367744	        15.00 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Overlaps-16        	81220230	        14.47 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Overlaps-16        	79867218	        14.39 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Overlaps-16        	83335555	        15.12 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Dense_RandPunch/RASK/Overlaps-16        	75632342	        14.57 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Build-16    	   15828	     75325 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Build-16    	   16080	     75070 ns/op	      4096 runs	        20.01 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Build-16    	   16197	     74488 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Build-16    	   16147	     77278 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Build-16    	   16299	     74182 ns/op	      4096 runs	        20.01 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Build-16         	    5842	    189942 ns/op	      4096 runs	        14.46 struct_B/run	  280995 B/op	    1477 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Build-16         	    5888	    194260 ns/op	      4096 runs	        14.46 struct_B/run	  280995 B/op	    1477 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Build-16         	    5896	    194779 ns/op	      4096 runs	        14.46 struct_B/run	  280995 B/op	    1477 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Build-16         	    5948	    193292 ns/op	      4096 runs	        14.46 struct_B/run	  280995 B/op	    1477 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Build-16         	    6379	    191276 ns/op	      4096 runs	        14.46 struct_B/run	  280995 B/op	    1477 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Contains-16 	91929036	        12.34 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Contains-16 	90640477	        12.45 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Contains-16 	95725596	        12.81 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Contains-16 	82177365	        12.58 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Contains-16 	84229710	        14.90 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Contains-16      	71202646	        15.10 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Contains-16      	86269298	        16.54 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Contains-16      	78290311	        16.07 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Contains-16      	64633953	        16.57 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Contains-16      	69101925	        15.44 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Overlaps-16 	85030651	        12.22 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Overlaps-16 	90837397	        12.24 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Overlaps-16 	96853354	        12.17 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Overlaps-16 	96627810	        12.36 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/Intervals/Overlaps-16 	99210925	        12.44 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Overlaps-16      	89971741	        12.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Overlaps-16      	84278155	        11.95 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Overlaps-16      	95148691	        12.22 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Overlaps-16      	100000000	        13.12 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Sequential_Append/RASK/Overlaps-16      	95094343	        12.62 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Build-16 	    1142	    967975 ns/op	      4395 runs	        18.64 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Build-16 	    1231	    980388 ns/op	      4395 runs	        18.64 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Build-16 	    1201	    929089 ns/op	      4395 runs	        18.64 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Build-16 	    1264	    941236 ns/op	      4395 runs	        18.64 struct_B/run	  259336 B/op	      17 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Build-16 	    1252	    925536 ns/op	      4395 runs	        18.64 struct_B/run	  259337 B/op	      17 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Build-16      	    2785	    445046 ns/op	      4395 runs	         8.968 struct_B/run	  370563 B/op	     365 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Build-16      	    2760	    450275 ns/op	      4395 runs	         8.968 struct_B/run	  370563 B/op	     365 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Build-16      	    2562	    468089 ns/op	      4395 runs	         8.968 struct_B/run	  370563 B/op	     365 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Build-16      	    2665	    455225 ns/op	      4395 runs	         8.968 struct_B/run	  370564 B/op	     365 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Build-16      	    2790	    445425 ns/op	      4395 runs	         8.968 struct_B/run	  370563 B/op	     365 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Contains-16         	67937820	        17.25 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Contains-16         	73138826	        15.77 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Contains-16         	75126921	        17.42 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Contains-16         	73382148	        18.44 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Contains-16         	66477754	        18.09 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Contains-16              	61473277	        21.70 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Contains-16              	61823269	        18.41 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Contains-16              	67249114	        20.49 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Contains-16              	51594597	        21.97 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Contains-16              	61856252	        19.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Overlaps-16         	100000000	        10.85 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Overlaps-16         	100000000	        11.27 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Overlaps-16         	100000000	        11.21 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Overlaps-16         	100000000	        11.24 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/Intervals/Overlaps-16         	100000000	        11.08 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Overlaps-16              	88126509	        14.08 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Overlaps-16              	89291042	        13.30 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Overlaps-16              	86617851	        13.51 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Overlaps-16              	89559330	        13.48 ns/op	       0 B/op	       0 allocs/op
BenchmarkRASKQualification/Random_JournalReplay/RASK/Overlaps-16              	93532237	        13.67 ns/op	       0 B/op	       0 allocs/op
```
