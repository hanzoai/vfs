// Command fspiverify measures what the integrity checks of a Volume actually see.
//
// It writes a Volume to an in-memory store, snapshots the store, and then, for every object
// the snapshot holds and for every kind of damage a store that lost or mutated a write can do
// to it, rebuilds the store with that one damage and runs every reader the package offers over
// it: Check (the namespace), Reseal with Verify (the shallow tree walk), Verify with and
// without All (the deep walk this tool measures), and a full read of every file. The same
// readers on a clean store are the control, and the overhead of each: the bytes it pulls from
// the store, the requests it makes, and the time it takes.
//
// Every read of the store is counted by a wrapper around the in-memory backend, so the byte
// columns are what the reader asked for and got, not an estimate. Nothing here is timed on a
// disk, a network or an NVMe device: the tool measures coverage and amplification, and says
// so in its own output.
//
//	go run ./tools/fspiverify -csv docs/fspi.csv
package main

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/backend"
)

var ctx = context.Background()

// counted is the in-memory store with a tally of what a reader asked it for. Every column of
// the report that says "bytes" is this tally, so the cost of a reader is measured at the
// interface a real store would bill: one Get, one GetRange and one Stat at a time.
type counted struct {
	*storetest.Mem

	mu           sync.Mutex
	gets, ranges int
	stats        int
	reads        int // Get plus GetRange: the requests that carried a body
	bytes        int64
	asked        map[string]bool // the keys some reader asked for
}

func (c *counted) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	c.ask(key)
	b, v, err := c.Mem.Get(ctx, key, max)
	if err == nil {
		c.mu.Lock()
		c.gets++
		c.reads++
		c.bytes += int64(len(b))
		c.mu.Unlock()
	}
	return b, v, err
}

func (c *counted) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	c.ask(key)
	b, err := c.Mem.GetRange(ctx, key, off, n)
	if err == nil {
		c.mu.Lock()
		c.ranges++
		c.reads++
		c.bytes += int64(len(b))
		c.mu.Unlock()
	}
	return b, err
}

func (c *counted) Stat(ctx context.Context, key string) (int64, error) {
	c.ask(key)
	n, err := c.Mem.Stat(ctx, key)
	if err == nil {
		c.mu.Lock()
		c.stats++
		c.mu.Unlock()
	}
	return n, err
}

func (c *counted) tally() (bytes int64, reads, stats int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes, c.reads, c.stats
}

// place is what every store in this run says it is. A Head is sealed to the place it was
// written at, and the in-memory store names itself after its own address, so a replica would
// otherwise be a different place and no reader could open the Head of another replica.
const place = "mem://fspiverify"

func (c *counted) String() string { return place }

func newStore() *counted {
	return &counted{Mem: storetest.NewMem(), asked: map[string]bool{}}
}

func (c *counted) ask(key string) {
	c.mu.Lock()
	c.asked[key] = true
	c.mu.Unlock()
}

func (c *counted) askedKeys() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]bool, len(c.asked))
	for k := range c.asked {
		out[k] = true
	}
	return out
}

// replica rebuilds a store from a snapshot, so that no two measurements share a cache, a
// write, or a warm page of the in-memory store.
func replica(snap map[string][]byte) (*counted, error) {
	s := newStore()
	for k, b := range snap {
		if err := s.Put(ctx, k, b); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func config(be backend.Backend, c vfs.Cipher, round uint64) vfs.Config {
	return vfs.Config{
		Backend: be, Cipher: c, Round: round,
		Journal: storetest.Journals(), JournalCapacity: 64 << 20, Disk: storetest.Disk,
	}
}

// file is what the writer meant: the name, the inode the tree gives it and the hash of the
// bytes it holds. The verification readers never see this; the full read does, and says so.
type file struct {
	name string
	ino  vfs.InodeID
	sum  [32]byte
}

func content(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed*7919+1))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// populate writes count files into a Volume attached at round, starting at index start, and
// returns what it wrote. Every fourth file is small enough to stay inline in a shard, so the
// store holds both of the two places bytes live in this format.
func populate(be *counted, c vfs.Cipher, round uint64, start, count, size int) ([]file, error) {
	v, err := vfs.Attach(ctx, config(be, c, round))
	if err != nil {
		return nil, fmt.Errorf("attach at round %d: %w", round, err)
	}
	defer v.Close()
	out := make([]file, 0, count)
	for i := 0; i < count; i++ {
		n := size
		if i%4 == 3 {
			n = 8 << 10
		}
		data := content(uint64(start+i+1), n)
		name := fmt.Sprintf("f%03d", start+i)
		in, err := v.Create(ctx, vfs.RootInode, name, 0o644)
		if err != nil {
			return nil, fmt.Errorf("create %s: %w", name, err)
		}
		f, err := v.Open(ctx, in.ID)
		if err != nil {
			return nil, err
		}
		_, err = f.WriteAt(ctx, data, 0)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, fmt.Errorf("write %s: %w", name, err)
		}
		out = append(out, file{name: name, ino: in.ID, sum: sha256.Sum256(data)})
	}
	if err := v.Flush(ctx); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	return out, nil
}

// verdict is one reader's answer on one damaged store.
type verdict struct {
	caught bool
	faults int
	detail string
	bytes  int64
	reads  int
	stats  int
	dur    time.Duration
}

// reader is one of the ways the package lets a caller ask whether a Volume is whole, and
// whether it means it. scope says what the reader is allowed to know: an "in-band" reader sees
// only the store, a "manifest" reader is also told which files the writer meant to be there,
// which no reader of a store otherwise knows.
type reader struct {
	name  string
	scope string
	run   func(be *counted, c vfs.Cipher, want []file) verdict
}

func measure(be *counted, fn func() (bool, int, string)) verdict {
	start := time.Now()
	caught, faults, detail := fn()
	return verdict{
		caught: caught, faults: faults, detail: detail, dur: time.Since(start),
		bytes: mustBytes(be), reads: mustReads(be), stats: mustStats(be),
	}
}

func mustBytes(be *counted) int64 { b, _, _ := be.tally(); return b }
func mustReads(be *counted) int   { _, r, _ := be.tally(); return r }
func mustStats(be *counted) int   { _, _, s := be.tally(); return s }

func oneLine(err error) string {
	s := strings.NewReplacer("\n", " ", "\t", " ").Replace(err.Error())
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

// open is the first reader every path shares: a Volume whose root shard or Head a store has
// damaged does not open at all, and that is a detection like any other.
func open(be *counted, c vfs.Cipher) (*vfs.Volume, verdict, bool) {
	v, err := vfs.Open(ctx, config(be, c, 0))
	if err != nil {
		return nil, verdict{caught: true, faults: 1, detail: "open: " + oneLine(err),
			bytes: mustBytes(be), reads: mustReads(be), stats: mustStats(be)}, false
	}
	return v, verdict{}, true
}

func verifyRun(be *counted, c vfs.Cipher, opts vfs.VerifyOptions) verdict {
	v, early, ok := open(be, c)
	if !ok {
		return early
	}
	defer v.Close()
	return measure(be, func() (bool, int, string) {
		rep, err := v.Verify(ctx, opts)
		if err != nil && !errors.Is(err, vfs.ErrCorrupt) {
			return true, 1, oneLine(err)
		}
		if rep == nil {
			return true, 1, "no report"
		}
		if len(rep.Damaged) > 0 {
			return true, len(rep.Damaged), oneLine(rep.Damaged[0])
		}
		return false, 0, ""
	})
}

func readBack(v *vfs.Volume, w file) error {
	in, err := v.Stat(ctx, w.ino)
	if err != nil {
		return err
	}
	f, err := v.Open(ctx, w.ino)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, in.Size)
	n, err := f.ReadAt(ctx, buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if uint64(n) != in.Size {
		return fmt.Errorf("read %d of %d bytes", n, in.Size)
	}
	if sha256.Sum256(buf) != w.sum {
		return errors.New("bytes differ from what was written")
	}
	return nil
}

func readers() []reader {
	return []reader{
		{name: "check", scope: "in-band", run: func(be *counted, c vfs.Cipher, _ []file) verdict {
			v, early, ok := open(be, c)
			if !ok {
				return early
			}
			defer v.Close()
			return measure(be, func() (bool, int, string) {
				if err := v.Check(ctx); err != nil {
					return true, 1, oneLine(err)
				}
				return false, 0, ""
			})
		}},
		{name: "reseal-verify", scope: "in-band", run: func(be *counted, c vfs.Cipher, _ []file) verdict {
			cfg := config(be, c, 3)
			cfg.Verify = true
			return measure(be, func() (bool, int, string) {
				if err := vfs.Reseal(ctx, cfg, ""); err != nil {
					return true, 1, oneLine(err)
				}
				return false, 0, ""
			})
		}},
		{name: "verify-shallow", scope: "in-band", run: func(be *counted, c vfs.Cipher, _ []file) verdict {
			return verifyRun(be, c, vfs.VerifyOptions{})
		}},
		{name: "verify-all", scope: "in-band", run: func(be *counted, c vfs.Cipher, _ []file) verdict {
			return verifyRun(be, c, vfs.VerifyOptions{All: true})
		}},
		{name: "read-all", scope: "manifest", run: func(be *counted, c vfs.Cipher, want []file) verdict {
			v, early, ok := open(be, c)
			if !ok {
				return early
			}
			defer v.Close()
			return measure(be, func() (bool, int, string) {
				faults, first := 0, ""
				for _, w := range want {
					if err := readBack(v, w); err != nil {
						faults++
						if first == "" {
							first = w.name + ": " + oneLine(err)
						}
					}
				}
				return faults > 0, faults, first
			})
		}},
	}
}

// damage is one kind of harm a store can do to one object, named after what the store did and
// not after what a reader should make of it.
type damage struct {
	class string
	apply func(be *counted, key string) error
}

func bytesAt(be *counted, key string) ([]byte, error) {
	b, _, err := be.Mem.Get(ctx, key, 1<<30)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), b...), nil
}

func damages() []damage {
	return []damage{
		{class: "shard-bitflip", apply: func(be *counted, key string) error {
			b, err := bytesAt(be, key)
			if err != nil {
				return err
			}
			b[len(b)/2] ^= 1
			return be.Mem.Put(ctx, key, b)
		}},
		{class: "shard-missing", apply: func(be *counted, key string) error {
			return be.Mem.Delete(ctx, key)
		}},
		{class: "pack-bitflip", apply: func(be *counted, key string) error {
			b, err := bytesAt(be, key)
			if err != nil {
				return err
			}
			b[len(b)/2] ^= 1
			return be.Mem.Put(ctx, key, b)
		}},
		{class: "pack-truncated", apply: func(be *counted, key string) error {
			b, err := bytesAt(be, key)
			if err != nil {
				return err
			}
			cut := 1024
			if len(b)/2 < cut {
				cut = len(b) / 2
			}
			return be.Mem.Put(ctx, key, b[:len(b)-cut])
		}},
		{class: "pack-missing", apply: func(be *counted, key string) error {
			return be.Mem.Delete(ctx, key)
		}},
	}
}

func keysUnder(snap map[string][]byte, prefix string) []string {
	var out []string
	for k := range snap {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

type row struct {
	class, target, detector string
	v                       verdict
}

func main() {
	files := flag.Int("files", 12, "files written per generation")
	size := flag.Int("size", 256<<10, "bytes of the files that are not inline")
	out := flag.String("csv", "", "write the raw rows here (default: the summary only)")
	repeat := flag.Int("repeat", 3, "times each reader is run on a clean store, for a median")
	flag.Parse()

	k, err := age.GenerateHybridIdentity()
	if err != nil {
		fmt.Fprintf(os.Stderr, "key: %v\n", err)
		os.Exit(1)
	}
	c := vfs.Hybrid(k)
	be := newStore()
	if err := vfs.Create(ctx, config(be, c, 0)); err != nil {
		fmt.Fprintf(os.Stderr, "create: %v\n", err)
		os.Exit(1)
	}
	gen1, err := populate(be, c, 1, 0, *files, *size)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generation 1: %v\n", err)
		os.Exit(1)
	}
	dump1 := be.Dump()
	gen2, err := populate(be, c, 2, *files, *files/2, *size)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generation 2: %v\n", err)
		os.Exit(1)
	}
	dump2 := be.Dump()
	every := append(append([]file{}, gen1...), gen2...)

	// The damage matrix runs on the state of the first generation, where every object in the
	// store is one a Head reaches: an object nothing reaches is not part of any Volume, and a
	// reader that ignored it would be right, so it is not evidence either way.
	base, want := dump1, gen1
	total := int64(0)
	for _, b := range base {
		total += int64(len(b))
	}
	meta, packs := keysUnder(base, "meta/"), keysUnder(base, "pack/")
	fmt.Printf("# store: %d objects (%d shards, %d packs) holding %d files in %d bytes\n",
		len(base), len(meta), len(packs), len(want), total)

	rs := readers()
	var rows []row

	// The control: a store nothing has damaged. These rows are also the overhead of a reader,
	// and they define reachability: an object no reader asks the store for on a clean Volume is
	// not part of the Volume, and damaging it says nothing about anyone's coverage. Only a
	// reader that reads bytes can reach a pack; Check reaches only the shards it walks.
	reached := map[string]bool{}
	for _, rd := range rs {
		for i := 0; i < *repeat; i++ {
			rep, err := replica(base)
			if err != nil {
				fmt.Fprintf(os.Stderr, "replica: %v\n", err)
				os.Exit(1)
			}
			rows = append(rows, row{"clean", fmt.Sprintf("rep%d", i+1), rd.name, rd.run(rep, c, want)})
			for k := range rep.askedKeys() {
				reached[k] = true
			}
		}
	}
	reach := func(keys []string) []string {
		var out []string
		for _, k := range keys {
			if reached[k] {
				out = append(out, k)
			}
		}
		return out
	}
	allShards, allPacks := len(meta), len(packs)
	meta, packs = reach(meta), reach(packs)
	fmt.Printf("# reachable: %d of %d objects (%d shards, %d packs); %d are superseded and no reader asks for them\n",
		len(meta)+len(packs), len(base)-1, len(meta), len(packs), len(base)-1-len(meta)-len(packs))

	for _, d := range damages() {
		keys := meta
		if strings.HasPrefix(d.class, "pack") {
			keys = packs
		}
		for _, k := range keys {
			for _, rd := range rs {
				rep, err := replica(base)
				if err != nil {
					fmt.Fprintf(os.Stderr, "replica: %v\n", err)
					os.Exit(1)
				}
				if err := d.apply(rep, k); err != nil {
					fmt.Fprintf(os.Stderr, "%s %s: %v\n", d.class, k, err)
					os.Exit(1)
				}
				rows = append(rows, row{d.class, k, rd.name, rd.run(rep, c, want)})
			}
		}
	}

	// A Head whose bytes changed: the root of trust of everything below it, and the one object
	// every reader must open before it can say anything at all.
	for _, rd := range rs {
		rep, err := replica(base)
		if err != nil {
			fmt.Fprintf(os.Stderr, "replica: %v\n", err)
			os.Exit(1)
		}
		if err := damages()[0].apply(rep, "head"); err != nil {
			fmt.Fprintf(os.Stderr, "head-bitflip: %v\n", err)
			os.Exit(1)
		}
		rows = append(rows, row{"head-bitflip", "head", rd.name, rd.run(rep, c, want)})
	}

	// A rollback: every object of the newest state, and the Head of the generation before it.
	// The old Head is authentic and opens where it is sealed, so this asks whether integrity
	// says anything at all about freshness.
	roll := make(map[string][]byte, len(dump2))
	for k, b := range dump2 {
		roll[k] = b
	}
	if h, ok := dump1["head"]; ok {
		roll["head"] = h
	}
	for _, rd := range rs {
		rep, err := replica(roll)
		if err != nil {
			fmt.Fprintf(os.Stderr, "replica: %v\n", err)
			os.Exit(1)
		}
		rows = append(rows, row{"head-rollback", "head", rd.name, rd.run(rep, c, every)})
	}

	if *out != "" {
		lay := layout{
			objects: len(base), shards: allShards, packs: allPacks,
			reachShards: len(meta), reachPacks: len(packs),
			storeBytes: int(total),
		}
		if err := write(*out, rows, rs, lay); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
	}
	summarize(rows, rs, total, *out)
}

// layout is the shape of the store the rows were measured on. It is not reproducible from the
// flags: the journal decides how many shards one publication writes, so two runs of the same
// command hold the same Volume in a different number of objects, and a row has to say which
// store it saw for its counts to mean anything.
type layout struct {
	objects, shards, packs  int
	reachShards, reachPacks int
	storeBytes              int
}

func write(path string, rows []row, rs []reader, lay layout) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	scope := map[string]string{}
	for _, rd := range rs {
		scope[rd.name] = rd.scope
	}
	w := csv.NewWriter(f)
	w.Write([]string{"class", "target", "detector", "scope", "caught", "faults",
		"bytes_read", "store_reads", "store_stats", "dur_us", "detail",
		"store_objects", "store_shards", "store_packs",
		"reachable_shards", "reachable_packs", "store_bytes"})
	for _, r := range rows {
		w.Write([]string{
			r.class, r.target, r.detector, scope[r.detector],
			fmt.Sprint(r.v.caught), fmt.Sprint(r.v.faults),
			fmt.Sprint(r.v.bytes), fmt.Sprint(r.v.reads), fmt.Sprint(r.v.stats),
			fmt.Sprint(r.v.dur.Microseconds()), r.v.detail,
			fmt.Sprint(lay.objects), fmt.Sprint(lay.shards), fmt.Sprint(lay.packs),
			fmt.Sprint(lay.reachShards), fmt.Sprint(lay.reachPacks), fmt.Sprint(lay.storeBytes),
		})
	}
	w.Flush()
	return w.Error()
}

// summarize prints the two tables the report quotes: how many objects of each class each
// reader catches, and what each reader costs on a store nothing has damaged.
func summarize(rows []row, rs []reader, total int64, path string) {
	var classes []string
	count := map[string]map[string][2]int{}
	for _, r := range rows {
		if r.class == "clean" {
			continue
		}
		if count[r.class] == nil {
			count[r.class] = map[string][2]int{}
			classes = append(classes, r.class)
		}
		c := count[r.class][r.detector]
		c[0]++
		if r.v.caught {
			c[1]++
		}
		count[r.class][r.detector] = c
	}

	fmt.Printf("\n## control — a store nothing has damaged, %d runs of each reader: %d faults\n",
		cleanRuns(rows), cleanFaults(rows))
	fmt.Printf("\n## detection — objects caught / objects damaged\n\n%-16s %5s", "class", "objs")
	for _, rd := range rs {
		fmt.Printf(" %15s", rd.name)
	}
	fmt.Println()
	for _, class := range classes {
		objs := 0
		for _, rd := range rs {
			if c := count[class][rd.name]; c[0] > objs {
				objs = c[0]
			}
		}
		fmt.Printf("%-16s %5d", class, objs)
		for _, rd := range rs {
			c := count[class][rd.name]
			fmt.Printf(" %15s", fmt.Sprintf("%d/%d", c[1], c[0]))
		}
		fmt.Println()
	}

	fmt.Printf("\n## overhead — a store nothing has damaged, %d bytes of objects\n\n", total)
	fmt.Printf("%-16s %10s %7s %8s %9s %7s\n", "reader", "bytes", "of store", "requests", "stats", "ms")
	for _, rd := range rs {
		var ms []float64
		var bytes int64
		var reads, stats int
		for _, r := range rows {
			if r.class != "clean" || r.detector != rd.name {
				continue
			}
			ms = append(ms, float64(r.v.dur.Microseconds())/1000)
			bytes, reads, stats = r.v.bytes, r.v.reads, r.v.stats
		}
		if len(ms) == 0 {
			continue
		}
		pct := 0.0
		if total > 0 {
			pct = 100 * float64(bytes) / float64(total)
		}
		fmt.Printf("%-16s %10d %6.1f%% %8d %9d %7.2f  [%.2f–%.2f, %d runs]\n", rd.name, bytes, pct,
			reads, stats, median(ms), minOf(ms), maxOf(ms), len(ms))
	}
	if path != "" {
		fmt.Printf("\n%d rows written to %s\n", len(rows), path)
	}
}

func median(v []float64) float64 {
	sorted := append([]float64{}, v...)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func minOf(v []float64) float64 {
	m := v[0]
	for _, x := range v[1:] {
		if x < m {
			m = x
		}
	}
	return m
}

func maxOf(v []float64) float64 {
	m := v[0]
	for _, x := range v[1:] {
		if x > m {
			m = x
		}
	}
	return m
}

func cleanRuns(rows []row) int {
	n := 0
	for _, r := range rows {
		if r.class == "clean" {
			n++
		}
	}
	return n
}

func cleanFaults(rows []row) int {
	n := 0
	for _, r := range rows {
		if r.class == "clean" {
			n += r.v.faults
		}
	}
	return n
}
