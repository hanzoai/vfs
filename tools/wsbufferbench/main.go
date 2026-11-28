// Command wsbufferbench measures the journal writing 4 KiB records through a wsbuffer layer,
// and the same journal writing them straight to the files, in the same run and on the same
// disk, so the two differ in one variable.
//
//	wsbufferbench -dir /var/tmp/wsbuffer -writers 8 -ops 200 -size 4096
//	wsbufferbench -dir /var/tmp/wsbuffer -rep 3 -header > docs/wsbuffer.csv
//
// Every writer appends a record and waits for it to be durable, which is the shape the bounty
// names (4 KiB random writes with a sync per write). Each line reports the operations and the
// latency percentiles of one mode, and the pages the layer below the journal wrote for it
// against the pages the journal asked for: the second pair is the write amplification, and it
// needs no second execution because both are counted from the same call sequence.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/wsbuffer"
)

var (
	dir     = flag.String("dir", "", "directory for the journals of the run (default a temporary one)")
	writers = flag.Int("writers", 8, "concurrent writers, each syncing its own record")
	ops     = flag.Int("ops", 200, "records per writer")
	size    = flag.Int("size", 4096, "bytes per record")
	window  = flag.Int("window", wsbuffer.DefaultRun, "bytes the layer holds before writing")
	capa    = flag.Int64("capacity", 1<<30, "journal capacity in bytes")
	every   = flag.Int("every", 1, "records per writer between Sync calls: the writes a sync round carries")
	rep     = flag.Int("rep", 1, "repetitions of each mode, run alternately")
	header  = flag.Bool("header", false, "print the column names first")
)

func main() {
	flag.Parse()
	work := *dir
	if work == "" {
		var err error
		if work, err = os.MkdirTemp("", "wsbufferbench-"); err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(work)
	}
	if err := os.MkdirAll(work, 0o755); err != nil {
		log.Fatal(err)
	}
	payload := make([]byte, *size)
	src := rand.New(rand.NewPCG(7, 11))
	for i := range payload {
		payload[i] = byte(src.Uint32())
	}
	if *header {
		fmt.Println("run,mode,window,writers,size,every,records,ref_calls,ref_bytes,ref_pages,calls,bytes,pages,absorbed,flushed,ref_ratio,ratio,ops_s,p50_us,p99_us,cpu_us")
	}
	modes := []struct {
		name string
		run  int
	}{{"through", -1}, {"window", *window}}
	for r := 1; r <= *rep; r++ {
		for _, m := range modes {
			line(work, r, m.name, m.run, payload)
		}
	}
}

func line(work string, rep int, mode string, window int, payload []byte) {
	root, err := os.MkdirTemp(work, fmt.Sprintf("r%d-%s-", rep, mode))
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(root)
	at := root + "/journal"
	files, err := journal.Files(at)
	if err != nil {
		log.Fatal(err)
	}
	layer := wsbuffer.New(files, wsbuffer.Options{Run: window})
	j, err := journal.Open(at, journal.Options{Capacity: *capa, Disk: layer})
	if err != nil {
		log.Fatal(err)
	}
	defer j.Close()

	prime(j, payload)
	per := *ops
	lats := make([][]time.Duration, *writers)
	var wg sync.WaitGroup
	var cpu0, cpu1 syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &cpu0); err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < per; i++ {
				// A writer that does not sync every record measures the round the
				// journal builds on its own, which is what the layer has to work with.
				syncing := (i+1)%*every == 0
				t0 := time.Now()
				var seq uint64
				for {
					seq, err = j.Append(ctx, payload)
					if err != journal.ErrFull {
						break
					}
					runtime.Gosched()
				}
				if err != nil {
					log.Fatal(err)
				}
				if syncing {
					if err := j.Sync(ctx, seq); err != nil {
						log.Fatal(err)
					}
					lats[w] = append(lats[w], time.Since(t0))
				}
			}
		}(w)
	}
	wg.Wait()
	if err := j.Sync(context.Background(), j.Marks().Visible); err != nil {
		log.Fatal(err)
	}
	elapsed := time.Since(start)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &cpu1); err != nil {
		log.Fatal(err)
	}
	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	sort.Slice(all, func(a, b int) bool { return all[a] < all[b] })
	s := layer.Stats()
	records := *writers * per
	fmt.Printf("%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.0f,%.1f,%.1f,%d\n",
		rep, mode, window, *writers, *size, *every, records,
		s.RefCalls, s.RefBytes, s.RefPages, s.Calls, s.Bytes, s.Pages, s.Absorbed, s.Flushed,
		s.RefRatio(), s.Ratio(),
		float64(records)/elapsed.Seconds(),
		pct(all, .5), pct(all, .99),
		micros(cpu1)-micros(cpu0))
}

// prime appends a record first, so the journal has a segment before the clock starts:
// creating it is the same for both modes and not what is measured.
func prime(j *journal.Journal, payload []byte) {
	ctx := context.Background()
	seq, err := j.Append(ctx, payload)
	if err != nil {
		log.Fatal(err)
	}
	if err := j.Sync(ctx, seq); err != nil {
		log.Fatal(err)
	}
}

func pct(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return float64(sorted[int(float64(len(sorted)-1)*p)]) / float64(time.Microsecond)
}

func micros(r syscall.Rusage) int64 {
	return r.Utime.Nano()/1000 + r.Stime.Nano()/1000
}
