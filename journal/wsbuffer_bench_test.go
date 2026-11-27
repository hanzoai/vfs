package journal_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs/journal"
)

type wsMode struct {
	name string
	cfg  *journal.WSBufferConfig
}

func wsModes(capBytes int) []wsMode {
	always := journal.WSBufferConfig{
		PageSize:        4096,
		Capacity:        capBytes,
		BypassThreshold: 4096,
		AlwaysCopy:      true,
	}
	adaptive := journal.WSBufferConfig{
		PageSize:        4096,
		Capacity:        capBytes,
		BypassThreshold: 4096,
		AlwaysCopy:      false,
	}
	return []wsMode{
		{name: "baseline_journal_main", cfg: nil},
		{name: "always_copy_buffered_writer", cfg: &always},
		{name: "wsbuffer_adaptive_bypass", cfg: &adaptive},
	}
}

func openBenchFile(b *testing.B, mode wsMode, segSize int64) (journal.File, *obsDisk, func()) {
	b.Helper()
	dir := scratch(b)
	raw, err := journal.Files(dir)
	if err != nil {
		b.Fatal(err)
	}
	obs := newObsDisk(raw, 4096)
	var disk journal.Disk = obs
	if mode.cfg != nil {
		disk = journal.WrapWSBuffer(obs, *mode.cfg)
	}
	f, err := disk.Create("000000.seg", segSize)
	if err != nil {
		raw.Close()
		b.Fatal(err)
	}
	cleanup := func() {
		f.Close()
		raw.Close()
	}
	return f, obs, cleanup
}

// BenchmarkWSBufferSmallUnaligned compares baseline, always-copy, and WSBuffer
// across 128 B, 512 B, 1 KiB, and 2 KiB unaligned sequential journal writes on real OS files.
func BenchmarkWSBufferSmallUnaligned(b *testing.B) {
	const segSize = 16 << 20
	for _, sz := range []int{128, 512, 1024, 2048} {
		payload := bytes.Repeat([]byte{0x5A}, sz)
		for _, mode := range wsModes(16 << 10) {
			b.Run(fmt.Sprintf("%dB/%s", sz, mode.name), func(b *testing.B) {
				f, obs, cleanup := openBenchFile(b, mode, segSize)
				defer cleanup()
				b.SetBytes(int64(sz))
				b.ResetTimer()
				off := int64(80)
				for i := 0; i < b.N; i++ {
					if off+int64(sz) > segSize {
						off = 80
					}
					if _, err := f.WriteAt(payload, off); err != nil {
						b.Fatal(err)
					}
					off += int64(sz)
				}
				if wf, ok := f.(*journal.WSBufferFile); ok {
					if err := wf.Flush(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				logical := float64(b.N) * float64(sz)
				amp := (float64(obs.physicalPages.Load()) * 4096.0) / logical
				b.ReportMetric(amp, "write_amp")
				b.ReportMetric(float64(obs.writeOps.Load())/float64(b.N), "pwrite/op")
			})
		}
	}
}

// BenchmarkWSBufferRandom4KiBPartial measures repeated partial sub-page updates
// across a hot working set of 4 KiB pages on real OS files.
func BenchmarkWSBufferRandom4KiBPartial(b *testing.B) {
	const (
		segSize  = 16 << 20
		hotPages = 16
		writeSz  = 512
	)
	payload := bytes.Repeat([]byte{0x3C}, writeSz)
	for _, mode := range wsModes(64 << 10) {
		b.Run(mode.name, func(b *testing.B) {
			f, obs, cleanup := openBenchFile(b, mode, segSize)
			defer cleanup()
			rng := rand.New(rand.NewPCG(7, 13))
			b.SetBytes(writeSz)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pg := int64(rng.IntN(hotPages))
				sub := int64(rng.IntN(4096-writeSz)) &^ 7
				if _, err := f.WriteAt(payload, pg*4096+sub); err != nil {
					b.Fatal(err)
				}
			}
			if wf, ok := f.(*journal.WSBufferFile); ok {
				if err := wf.Flush(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			logical := float64(b.N) * writeSz
			amp := (float64(obs.physicalPages.Load()) * 4096.0) / logical
			b.ReportMetric(amp, "write_amp")
			b.ReportMetric(float64(obs.writeOps.Load())/float64(b.N), "pwrite/op")
		})
	}
}

// BenchmarkWSBufferMixed70_30 measures a 70% small unaligned (256 B) + 30% large
// aligned (16 KiB / 64 KiB) workload on real OS files.
func BenchmarkWSBufferMixed70_30(b *testing.B) {
	const segSize = 64 << 20
	smallBuf := bytes.Repeat([]byte{0x11}, 256)
	medBuf := bytes.Repeat([]byte{0x22}, 16<<10)
	bigBuf := bytes.Repeat([]byte{0x33}, 64<<10)

	for _, mode := range wsModes(16 << 10) {
		b.Run(mode.name, func(b *testing.B) {
			f, obs, cleanup := openBenchFile(b, mode, segSize)
			defer cleanup()
			smallOff := int64(80)
			largeOff := int64(8 << 20)
			var logical int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch i % 10 {
				case 7, 8:
					if largeOff+int64(len(medBuf)) > segSize {
						largeOff = 8 << 20
					}
					if _, err := f.WriteAt(medBuf, largeOff); err != nil {
						b.Fatal(err)
					}
					largeOff += int64(len(medBuf))
					logical += int64(len(medBuf))
				case 9:
					if largeOff+int64(len(bigBuf)) > segSize {
						largeOff = 8 << 20
					}
					if _, err := f.WriteAt(bigBuf, largeOff); err != nil {
						b.Fatal(err)
					}
					largeOff += int64(len(bigBuf))
					logical += int64(len(bigBuf))
				default:
					if smallOff+int64(len(smallBuf)) > 8<<20 {
						smallOff = 80
					}
					if _, err := f.WriteAt(smallBuf, smallOff); err != nil {
						b.Fatal(err)
					}
					smallOff += int64(len(smallBuf))
					logical += int64(len(smallBuf))
				}
			}
			if wf, ok := f.(*journal.WSBufferFile); ok {
				if err := wf.Flush(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.SetBytes(logical / int64(max(b.N, 1)))
			amp := (float64(obs.physicalPages.Load()) * 4096.0) / float64(max(logical, 1))
			b.ReportMetric(amp, "write_amp")
			b.ReportMetric(float64(obs.writeOps.Load())/float64(max(b.N, 1)), "pwrite/op")
		})
	}
}

// BenchmarkWSBufferLargeAligned verifies zero-copy bypass avoids copy regression
// on 16 KiB and 64 KiB page-aligned streaming writes.
func BenchmarkWSBufferLargeAligned(b *testing.B) {
	const segSize = 64 << 20
	for _, sz := range []int{16 << 10, 64 << 10} {
		payload := bytes.Repeat([]byte{0x7E}, sz)
		for _, mode := range wsModes(16 << 10) {
			b.Run(fmt.Sprintf("%dKiB/%s", sz>>10, mode.name), func(b *testing.B) {
				f, obs, cleanup := openBenchFile(b, mode, segSize)
				defer cleanup()
				b.SetBytes(int64(sz))
				b.ResetTimer()
				var off int64
				for i := 0; i < b.N; i++ {
					if off+int64(sz) > segSize {
						off = 0
					}
					if _, err := f.WriteAt(payload, off); err != nil {
						b.Fatal(err)
					}
					off += int64(sz)
				}
				if wf, ok := f.(*journal.WSBufferFile); ok {
					if err := wf.Flush(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				logical := float64(b.N) * float64(sz)
				amp := (float64(obs.physicalPages.Load()) * 4096.0) / logical
				b.ReportMetric(amp, "write_amp")
			})
		}
	}
}

// BenchmarkWSBufferJournalEndToEnd benchmarks full journal.Open + Append + periodic Sync
// comparing baseline_journal_main vs always_copy_buffered_writer vs wsbuffer_adaptive_bypass.
func BenchmarkWSBufferJournalEndToEnd(b *testing.B) {
	rec := bytes.Repeat([]byte("R"), 256)
	for _, mode := range wsModes(16 << 10) {
		b.Run(mode.name, func(b *testing.B) {
			dir := filepath.Join(scratch(b), mode.name)
			raw, err := journal.Files(dir)
			if err != nil {
				b.Fatal(err)
			}
			obs := newObsDisk(raw, 4096)
			j, err := journal.Open(dir, journal.Options{
				Capacity: 64 << 20,
				Segment:  16 << 20,
				Disk:     obs,
				WSBuffer: mode.cfg,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer j.Close()

			b.SetBytes(int64(len(rec)))
			b.ResetTimer()
			var last uint64
			for i := 0; i < b.N; i++ {
				seq, err := j.Append(context.Background(), rec)
				if err != nil {
					b.Fatal(err)
				}
				last = seq
				if i%128 == 127 {
					if err := j.Sync(context.Background(), seq); err != nil {
						b.Fatal(err)
					}
					if err := j.Publish(seq); err != nil {
						b.Fatal(err)
					}
					if err := j.Reclaim(); err != nil {
						b.Fatal(err)
					}
				}
			}
			if last > 0 {
				if err := j.Sync(context.Background(), last); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			logical := float64(j.Stats().Bytes)
			amp := (float64(obs.physicalPages.Load()) * 4096.0) / max(logical, 1)
			b.ReportMetric(amp, "write_amp")
		})
	}
}
