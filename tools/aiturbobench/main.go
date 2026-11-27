// Command aiturbobench measures matched parallel baseline (Streams=N, GroupSize=1)
// versus AITURBO grouped pack/range execution (Streams=N, GroupSize=K) across
// canonical Hanzo VFS backends (`storetest.Mem`, `file://`, or a live `-s3-endpoint`)
// with explicit separation of native (0 ms) and simulated-RTT sensitivity runs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/aiturbo"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
)

func openBenchBackend(ctx context.Context, kind, s3Endpoint, s3Bucket, prefix, accessKey, secretKey string) (backend.Backend, func(), error) {
	switch kind {
	case "mem":
		return storetest.NewMem(), func() {}, nil
	case "file":
		dir, err := os.MkdirTemp("", "aiturbobench-*")
		if err != nil {
			return nil, nil, err
		}
		be, err := backend.Open(ctx, "file://"+dir)
		if err != nil {
			_ = os.RemoveAll(dir)
			return nil, nil, err
		}
		return be, func() {
			_ = be.Close()
			_ = os.RemoveAll(dir)
		}, nil
	case "s3":
		be, err := s3be.New(s3be.Config{
			Endpoint:  s3Endpoint,
			Insecure:  true,
			Bucket:    s3Bucket,
			Prefix:    prefix,
			AccessKey: accessKey,
			SecretKey: secretKey,
		})
		if err != nil {
			return nil, nil, err
		}
		return be, func() { _ = be.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown backend %q (want mem, file, or s3)", kind)
	}
}

func main() {
	kind := flag.String("backend", "mem", "backend kind: mem, file, or s3")
	s3Endpoint := flag.String("s3-endpoint", "", "S3 endpoint host:port when -backend=s3")
	s3Bucket := flag.String("s3-bucket", "disk", "S3 bucket when -backend=s3")
	s3AK := flag.String("s3-ak", "", "S3 access key")
	s3SK := flag.String("s3-sk", "", "S3 secret key")
	itemsN := flag.Int("items", 256, "number of checkpoint items")
	itemSize := flag.Int("item-size", 64*1024, "bytes per item")
	groupSize := flag.Int("group-size", 8, "AITURBO grouping factor")
	streams := flag.Int("streams", 8, "matched parallel worker streams")
	rttList := flag.String("rtts", "0,5,15", "comma-separated injected ms RTT values (0 = native execution)")
	csvOut := flag.String("csv", "", "optional CSV output path (overwritten with header)")
	flag.Parse()

	ctx := context.Background()
	items := make([]aiturbo.Item, *itemsN)
	for i := range items {
		b := make([]byte, *itemSize)
		b[0] = byte(i)
		b[len(b)-1] = byte(i >> 8)
		items[i] = aiturbo.Item{
			Key:  fmt.Sprintf("meta/%02x/bench-%04d", i%16, i),
			Data: b,
		}
	}

	var csvFile *os.File
	if *csvOut != "" {
		var err error
		csvFile, err = os.Create(*csvOut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open csv %s: %v\n", *csvOut, err)
			os.Exit(1)
		}
		defer csvFile.Close()
		fmt.Fprintln(csvFile, "backend,mode,items,item_bytes,group_size,streams,injected_rtt_ms,upload_reqs,upload_bytes,upload_ms,download_reqs,download_bytes,download_ms")
	}

	var rtts []int
	for _, tok := range splitComma(*rttList) {
		var v int
		if _, err := fmt.Sscanf(tok, "%d", &v); err == nil {
			rtts = append(rtts, v)
		}
	}

	fmt.Printf("AITURBO Canonical Benchmark (backend=%s, items=%d, itemBytes=%d, streams=%d, groupSize=%d)\n",
		*kind, *itemsN, *itemSize, *streams, *groupSize)
	fmt.Printf("%-18s | %-6s | %-8s | %-10s | %-8s | %-10s\n",
		"Mode", "RTTms", "UpReqs", "UpDur", "DownReqs", "DownDur")
	fmt.Println("-------------------------------------------------------------------------")

	for _, rttMs := range rtts {
		rtt := time.Duration(rttMs) * time.Millisecond
		for _, g := range []int{1, *groupSize} {
			mode := "baseline-parallel"
			if g > 1 {
				mode = fmt.Sprintf("aiturbo-g%d", g)
			}
			be, cleanup, err := openBenchBackend(ctx, *kind, *s3Endpoint, *s3Bucket, fmt.Sprintf("bench-r%d-g%d", rttMs, g), *s3AK, *s3SK)
			if err != nil {
				fmt.Fprintf(os.Stderr, "backend setup: %v\n", err)
				os.Exit(1)
			}

			cnt := aiturbo.NewCountingBackend(be, rtt)
			up, err := aiturbo.NewGroupedUploader(cnt, aiturbo.Options{GroupSize: g, Streams: *streams, SimRTT: rtt})
			if err != nil {
				cleanup()
				panic(err)
			}
			plan, err := up.Planner().PlanCheckpoint(1, items)
			if err != nil {
				cleanup()
				panic(err)
			}
			um, err := up.UploadAndCommit(ctx, plan, 1)
			if err != nil {
				cleanup()
				panic(err)
			}
			upReqs := um.Wire.TotalWriteRequests()
			upBytes := um.Wire.BytesUploaded

			cnt.Reset()
			down, err := aiturbo.NewGroupedDownloader(cnt, aiturbo.Options{GroupSize: g, Streams: *streams, SimRTT: rtt})
			if err != nil {
				cleanup()
				panic(err)
			}
			_, dm, err := down.DownloadCheckpoint(ctx, nil)
			if err != nil {
				cleanup()
				panic(err)
			}
			downReqs := dm.Wire.TotalReadRequests()
			downBytes := dm.Wire.BytesDownloaded
			cleanup()

			upMs := float64(um.Duration.Microseconds()) / 1000.0
			downMs := float64(dm.Duration.Microseconds()) / 1000.0
			fmt.Printf("%-18s | %-6d | %-8d | %8.2fms | %-8d | %8.2fms\n",
				mode, rttMs, upReqs, upMs, downReqs, downMs)
			if csvFile != nil {
				fmt.Fprintf(csvFile, "%s,%s,%d,%d,%d,%d,%d,%d,%d,%.3f,%d,%d,%.3f\n",
					*kind, mode, *itemsN, *itemSize, g, *streams, rttMs,
					upReqs, upBytes, upMs, downReqs, downBytes, downMs)
			}
		}
	}
}

func splitComma(s string) []string {
	var out []string
	cur := ""
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
		} else {
			cur += string(s[i])
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
