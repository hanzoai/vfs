# Similarity-clustered immutable base images (RubikFS)

Refs #61. Tracked by HIP-1344. The library is `pkg/rubik`, the CLI benchmark runner
is `tools/rubikbench`, and the raw rows behind every number here are in
[`docs/rubikfs.csv`](rubikfs.csv).

## 1. Primary reference, summary, and license audit

* **Primary paper:** *Towards Condensed and Efficient Read-Only File System via
  Sort-Enhanced Compression* (RubikFS), Hao Huang, Yifeng Zhang, Yanqi Pan, Wen
  Xia, Xiangyu Zou, Darong Yang (Harbin Institute of Technology, Shenzhen), Jubin
  Zhong, Hua Liao (Huawei). 24th USENIX Conference on File and Storage
  Technologies (FAST '26), pp. 35--51, ISBN 978-1-939133-53-3.
  <https://www.usenix.org/conference/fast26/presentation/huang>
* **Summary, in the paper's own terms:** block-based compression of read-only
  images leaves compression on the table because a block mixes unrelated data —
  the *data mixture problem*. RubikFS sorts instead of blocking: a **similarity
  sorter** builds a similarity graph over data chunks and clusters it by subgraph
  partitioning, so similar chunks land in one compression block; and, separately,
  sorting **groups data with the same hotness to minimize read amplification**.
  Two levers, two targets: the sorter buys compression ratio, the hotness grouper
  buys reads.
* **Not reproduced here.** The paper reports up to `42.60 %` more compression and
  up to `70.70 %` fewer unnecessary reads. Those are the authors' numbers in the
  authors' environment. Nothing in this report reproduces them, and the issue's
  contract says they must not be read as Hanzo expectations. Every figure below
  was produced by `tools/rubikbench` on the synthetic population of §5.
* **Artifact and license audit:** a GitHub search for repositories named or
  described `RubikFS` returns `0` (`gh api search/repositories -f
  q="RubikFS in:name,description"`), so there is no upstream implementation to
  borrow from, cite as a dependency, or inherit a licence from. Everything in
  `pkg/rubik`, `tools/rubikbench` and this file is a clean-room Apache-2.0 Go
  implementation written from the paper's abstract and `hanzoai/vfs` alone, using
  only the standard library and `github.com/zeebo/blake3` (already in `go.mod`).
  `go.mod` and `go.sum` are untouched; the packages build with `CGO_ENABLED=0` on
  `linux/amd64` and `darwin/arm64`. **Patent encumbrance: `NO_MEDIDO`** — no
  patent search was performed, and none is claimed.

## 2. The store the order would run on

The order is the only degree of freedom this bounty can move, so it is worth
being exact about what a pack is in `vfs` today (`pack.go`):

> a pack is sealed chunks laid end to end; where each lies is in the chunk table,
> so a pack has no header and needs no index of its own

Three consequences, each measured rather than assumed:

1. **A pack does not compress.** Its content is the concatenation of AEAD frames,
   and a sealed frame is ciphertext plus a tag. Reordering chunks inside a pack
   therefore changes the bytes of the pack not at all: in all four orders of §5
   the store holds the same `15,833,075` bytes. The compression ratio RubikFS's
   sorter exists to improve has **no place to be collected** in this store.
2. **The frame has overhead but no slack.** Each frame carries `16` bytes of tag
   (`FrameOverhead`), counted on every chunk in `Stored` and `Sealed`.
3. **There is no staging point.** `packer.add` (`pack.go:44`) writes each frame at
   the offset it will occupy, so chunks are sealed in arrival order. An order
   imposed before sealing needs a buffer between the writer and the pack: that is
   an **integration hole**, declared in §6, not implemented here.

## 3. Architecture (`pkg/rubik`)

Nothing in this package is wired into `Volume`; it is the measurement apparatus
the contract asks for, and every claim below is a function of its inputs alone.

* `Chunk{ID, Tenant, Body}`, `Image{Name, Tenant, Chunks}`, `Workload` — the
  population, plus `Inverted()`: for each chunk, the images that read it.
* `Packer{Bytes}` — the **constant** of every comparison. It alone decides a pack
  boundary, from two rules: the size limit, and the tenant barrier. A plan that
  mixed tenants would be a defect, not a tuning knob, so `Plan.Isolation` counts
  the violations and every row of §5 reports `0`.
* `Plan` — the layout, `Stored` (bytes the store holds after sealing as it does
  today, uncompressed), `Sealed` (**counterfactual**: the same packs compressed
  with `flate` before sealing), `ColdStart` (a boot pays for every whole pack
  holding one of its chunks — the pack is the unit of a `GET`), `ColdStarts`
  (median and summed amplification over a tenant's images).
* Planners, i.e. the orders being compared: `Arrival` (what `vfs` writes today),
  `BySize` (the null hypothesis — only the fit matters), `ByContent` (bottom-k
  shingle sketch → minhash → LSH bands → single-linkage; the paper's similarity
  sorter), `ByCoAccess` (group chunks whose **set of reading images is equal**;
  the paper's hotness grouper, exact rather than approximate).
* `CoAccessRuns(w, order)` — the structural diagnostic: how many maximal stretches
  of an order share one reader set, against how many distinct reader sets exist.
  Equal counts mean the order *is* a co-access clustering. It reads no metric.

One rule holds in every planner: **tenant first**. A pack may not mix tenants, so
an order that interleaves them is not a packing but a run of partially filled
packs; an earlier revision of `BySize` did exactly that and reported `375` packs
and `7.40` amplification for a defect of its own making. The numbers in §5 are
after that correction — all four orders produce the same pack count.

## 4. Falsifiable hypothesis, pre-registered

Registered before the final runs (one exploratory run at a quarter of the scale
was made first; its numbers are not reported). Population: the layered workload of
§5 — `3,893` chunks, `15.0 MiB` plaintext, `200` images, `2` tenants, a `6`-layer
lineage per tenant and `16` near-copies per image.

| | statement | falsified if |
|---|---|---|
| **H1** | A content order reduces the compressed bytes a pack *would* hold by at least `10 %` on a near-copy population. | `sealed(content) / sealed(arrival) > 0.90` |
| **H2** | The same content order does not lower, and will raise, the bytes a cold start fetches. | `amp(content) ≤ amp(arrival)` |
| **H3** | A co-access order is never worse than arrival by more than `1 %`, and is strictly better (`≥ 5 %`) when arrival order is not already the clustering. | worse by more than `1 %` in the layered case, or better by less than `5 %` in the scattered case |
| **H4** | The choice of pack size moves a plan further than any choice of order does. | the best plan at `1 MiB` is less than `1.5×` the best plan at `256 KiB` |

**Expected failure modes, declared up front:** the compression counterfactual
being confused with a real saving; a synthetic text corpus making `sealed` an
artifact of its own statistics; and a packer boundary dominating the order, so
that two different relations produce identical layouts.

## 5. Empirical evaluation

Run on a 2026-11-26 macOS `darwin/arm64` host, Go 1.27.4, `8` CPUs, seed
`20261126`. Two pack sizes (`256 KiB`, `1 MiB`) × two arrival histories:
`layered`, where each tenant's layers reach the store in depth order, and
`scatter`, where they do not — a base image rebuilt long after the images that
stack on it. `layered` is the best case for the incumbent order; `scatter` is the
general case. `amp` is median fetched over needed, `amp_sum` the same ratio summed
over every image, which exposes splits the median hides.

### A. `256 KiB` packs, layers arrived in depth order

| order | packs | stored | sealed (counterfactual) | vs arrival | amp | amp_sum | runs/groups |
|---|---:|---:|---:|---:|---:|---:|---:|
| arrival | 62 | 15,833,075 | 4,410,427 | 1.0000 | 1.3951 | 1.3942 | 342/272 |
| size | 62 | 15,833,075 | 1,256,914 | **0.2850** | 9.2085 | 7.9772 | 3523/272 |
| content | 62 | 15,833,075 | 3,878,912 | 0.8795 | 2.1779 | 2.0991 | 1205/272 |
| co-access | 62 | 15,833,075 | 4,411,424 | 1.0002 | **1.3976** | **1.3960** | 272/272 |

### B. `1 MiB` packs, layers arrived in depth order

| order | packs | stored | sealed | vs arrival | amp | amp_sum |
|---|---:|---:|---:|---:|---:|---:|
| arrival | 16 | 15,833,075 | 4,360,036 | 1.0000 | 2.5045 | 2.5992 |
| size | 16 | 15,833,075 | 1,168,971 | **0.2681** | 9.3315 | 8.2067 |
| content | 16 | 15,833,075 | 3,826,470 | 0.8776 | 3.5241 | 3.3573 |
| co-access | 16 | 15,833,075 | 4,357,323 | 0.9994 | **2.5043** | **2.5487** |

### C. Layers arrived out of depth order (`scatter`), `256 KiB`

| order | amp | amp_sum | vs layered |
|---|---:|---:|---:|
| arrival | 1.7542 | 1.6204 | **+16.2 %** |
| content | 2.3915 | 2.3218 | +10.6 % |
| co-access | 1.3976 | 1.3960 | **+0.0 %** |

At `1 MiB` the same shift is arrival `2.9188` (+12.3 % over its layered value)
and co-access `2.5487` (+0.0 %). The `size` order is identical in both histories
(`7.9772`, `8.2067`) because it sorts on length and never looks at arrival.

### Verdicts

* **H1 confirmed.** Content clustering is a real compression order: `−12.1 %`
  (`0.8795`) at `256 KiB`, `−12.2 %` at `1 MiB`. And it is worth nothing here: the
  store holds `15,833,075` bytes under all four orders, because a pack is sealed
  ciphertext and does not compress. The `12 %` exists only in the counterfactual
  column.
* **H2 confirmed.** The order that compresses best reads worst after `size`:
  amplification `2.0991` against `1.3942` (**+50.6 %**) at `256 KiB`, `3.3573`
  against `2.5992` (**+29.2 %**) at `1 MiB`.
* **H3 confirmed, and it is the finding.** The co-access order is level with
  arrival when the layers arrived in depth order (`1.3960` vs `1.3942`, `+0.14 %`;
  differences of `997` bytes of *layout*, not of content) and wins by `13.8 %`
  (`1.6204 → 1.3960`) when they did not. `runs/groups = 272/272` says why: on a
  layered store the arrival order **already is** the exact co-access clustering,
  so the planner has nothing left to win; and `342/272` says the incumbent is not
  exact, which is exactly what it loses when history stops being tidy.
* **H4 confirmed, and it is the caveat on the whole bounty.** The best order at
  `1 MiB` (`2.5487`) is `1.83×` the best at `256 KiB` (`1.3960`), while the whole
  range the *best* order can move at a fixed size is `0–14 %`. Order decides
  whether a boot costs `1.4×` or `8.0×` its bytes; it does not decide `1.4×` versus
  `1.0×`. The pack size does that.

## 6. Negative results and declared limits

* **The `size` order's compression is an artifact and must not be quoted.** It
  reports `0.2850`, by far the best, because equal-length text drawn from one
  vocabulary is nearly the same text. A real image has same-sized unrelated files.
  The row is kept because it shows the trap in one line: the metric that is not
  paid would award the plan that is `5.7×` worse on the metric that is.
* **A pack does not compress, so RubikFS's similarity sorter does not transfer.**
  Its benefit is compression ratio; `vfs` seals AEAD frames into a headerless
  pack. Until a pack is compressed before sealing, `ByContent` can only cost reads.
* **There is no staging buffer.** `packer.add` writes at the offset the chunk will
  occupy, so an order has no point of application today. Implementing one changes
  the durability window of a write and is out of scope for this report.
* **`arrival ≡ co-access` is a property of the history, not of the algorithm.** On
  a lineage whose layers arrived in depth order, the reader set of a chunk is
  fixed by its layer and layers arrive in descending reader count, so the two
  orders coincide. Break the arrival order (`scatter`) and they stop coinciding.
* **The planner's own cost is not free.** Planning the `3,893` chunks takes `0.1 ms`
  for arrival, `1–2 ms` for co-access, `2–7 ms` for size and `2.0–2.5 s` for
  content: a bottom-k sketch and `32` minhashes over every `4 KiB` body. A
  production sorter would have to run inside the sealing path or not at all.
* **One population, one machine, one compression codec.** `flate` at its default
  level stands in for any block compressor; a different codec would move the
  counterfactual column, not the `stored` column. All numbers are from one host
  and one seed; `S3` latency, `GET` counts in currency and parallel streams are
  **`NO_MEDIDO`** — a pack in memory has no latency to hide, and the previous
  bounty on this repository measured `8` parallel streams to be *slower* than one
  against a memory store.

## 7. Reproducing

```sh
go test ./pkg/rubik/                     # orders, tenant barrier, the equivalence
go run ./tools/rubikbench                # default population, 256 KiB packs
go run ./tools/rubikbench -images 100 -deltas 16 -pack 262144          -csv a.csv
go run ./tools/rubikbench -images 100 -deltas 16 -pack 262144 -scatter -csv b.csv
go run ./tools/rubikbench -images 100 -deltas 16 -pack 1048576         -csv c.csv
go run ./tools/rubikbench -images 100 -deltas 16 -pack 1048576 -scatter -csv d.csv
```

`docs/rubikfs.csv` is those four runs concatenated, header once, `48` rows. The
bench prints the same rows to stdout; the elapsed columns are indicative, every
other column is exact and reproducible from the flags.

## 8. Status against HIP-1344

* The **hotness grouper transfers**: grouping chunks by their exact reader set is
  a read-amplification floor that holds under any arrival history, at `+0.0 %` to
  `+0.2 %` of layout cost. On this population it recovers `13.8 %` of the bytes a
  boot fetches whenever arrival order has drifted.
* The **similarity sorter does not transfer** to a store whose pack is not
  compressed, and it costs `29–51 %` more per cold start when it is applied.
* The **pack size is the larger lever** (`1.83×` between `256 KiB` and `1 MiB` at
  the best order), so a RubikFS-shaped change to `vfs` should be argued on pack
  granularity first, and on ordering second.
