package vfs_test

import (
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

func reportPercentiles(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	slices.Sort(samples)
	pct := func(p float64) float64 {
		idx := int(float64(len(samples)-1) * p)
		return float64(samples[idx].Nanoseconds()) / 1e3 // microseconds
	}
	b.ReportMetric(pct(0.50), "p50_us")
	b.ReportMetric(pct(0.95), "p95_us")
	b.ReportMetric(pct(0.99), "p99_us")
}

// BenchmarkQualificationLatencyAndThroughput measures p50/p95/p99 latency, throughput,
// and allocations across core VFS v2 operations (journaled WriteAt, local OS fsync,
// hot/cold ReadAt, Stat, Rename, and Flush publication).
func BenchmarkQualificationLatencyAndThroughput(b *testing.B) {
	b.Run("WriteAt_4KiB", func(b *testing.B) { benchWriteLatency(b, 4<<10, false) })
	b.Run("WriteAt_128KiB", func(b *testing.B) { benchWriteLatency(b, 128<<10, false) })
	b.Run("WriteAt_1MiB", func(b *testing.B) { benchWriteLatency(b, 1<<20, false) })
	b.Run("WriteAtAndFsync_4KiB_RealDisk", func(b *testing.B) { benchWriteLatency(b, 4<<10, true) })

	b.Run("ReadAt_128KiB_Hot", func(b *testing.B) {
		_, _, v, _ := pair(b, nil, func(c *vfs.Config) { c.JournalCapacity = 256 << 20 })
		in := write(b, v, vfs.RootInode, "readhot", content(1, 4<<20))
		flush(b, v)
		f, err := v.Open(ctx, in.ID)
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		buf := make([]byte, 128<<10)
		if _, err := f.ReadAt(ctx, buf, 0); err != nil {
			b.Fatal(err)
		}
		samples := make([]time.Duration, 0, b.N)
		b.SetBytes(int64(len(buf)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := int64((i * (128 << 10)) % (4 << 20))
			t0 := time.Now()
			if _, err := f.ReadAt(ctx, buf, off); err != nil {
				b.Fatal(err)
			}
			samples = append(samples, time.Since(t0))
		}
		b.StopTimer()
		reportPercentiles(b, samples)
	})

	b.Run("ReadAt_128KiB_Cold", func(b *testing.B) {
		mem, c, v, _ := pair(b, nil, func(c *vfs.Config) { c.JournalCapacity = 256 << 20 })
		in := write(b, v, vfs.RootInode, "readcold", content(2, 4<<20))
		flush(b, v)
		buf := make([]byte, 128<<10)
		samples := make([]time.Duration, 0, b.N)
		b.SetBytes(int64(len(buf)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ro := reopen(b, mem, c)
			f, err := ro.Open(ctx, in.ID)
			if err != nil {
				b.Fatal(err)
			}
			t0 := time.Now()
			if _, err := f.ReadAt(ctx, buf, 0); err != nil {
				b.Fatal(err)
			}
			samples = append(samples, time.Since(t0))
			f.Close()
		}
		b.StopTimer()
		reportPercentiles(b, samples)
	})

	b.Run("Stat_Hot", func(b *testing.B) {
		_, _, v, _ := pair(b, nil, nil)
		in := write(b, v, vfs.RootInode, "st", []byte("x"))
		flush(b, v)
		samples := make([]time.Duration, 0, b.N)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t0 := time.Now()
			if _, err := v.Stat(ctx, in.ID); err != nil {
				b.Fatal(err)
			}
			samples = append(samples, time.Since(t0))
		}
		b.StopTimer()
		reportPercentiles(b, samples)
	})

	b.Run("Rename_Atomic", func(b *testing.B) {
		_, _, v, _ := pair(b, nil, func(c *vfs.Config) { c.JournalCapacity = 256 << 20 })
		write(b, v, vfs.RootInode, "a", []byte("x"))
		samples := make([]time.Duration, 0, b.N)
		cur, nxt := "a", "b"
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t0 := time.Now()
			if err := v.Rename(ctx, vfs.RootInode, cur, vfs.RootInode, nxt, 0); err != nil {
				b.Fatal(err)
			}
			samples = append(samples, time.Since(t0))
			cur, nxt = nxt, cur
		}
		b.StopTimer()
		reportPercentiles(b, samples)
	})

	b.Run("Flush_1MiB_Generation", func(b *testing.B) {
		_, _, v, _ := pair(b, nil, func(c *vfs.Config) { c.JournalCapacity = 512 << 20 })
		in, err := v.Create(ctx, vfs.RootInode, "gen", 0o644)
		if err != nil {
			b.Fatal(err)
		}
		f, err := v.Open(ctx, in.ID)
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		buf := content(5, 1<<20)
		samples := make([]time.Duration, 0, b.N)
		b.SetBytes(int64(len(buf)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := f.WriteAt(ctx, buf, int64(i)*int64(len(buf))); err != nil {
				b.Fatal(err)
			}
			t0 := time.Now()
			if err := v.Flush(ctx); err != nil {
				b.Fatal(err)
			}
			samples = append(samples, time.Since(t0))
		}
		b.StopTimer()
		reportPercentiles(b, samples)
	})
}

func benchWriteLatency(b *testing.B, size int, realFsync bool) {
	_, _, v, _ := pair(b, nil, func(c *vfs.Config) {
		c.JournalCapacity = 1 << 30
		if realFsync {
			c.Journal = b.TempDir()
			c.Disk = nil
		}
	})
	in, err := v.Create(ctx, vfs.RootInode, "w", 0o644)
	if err != nil {
		b.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	buf := content(9, size)
	samples := make([]time.Duration, 0, b.N)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		if _, err := f.WriteAt(ctx, buf, int64(i)*int64(size)); err != nil {
			b.Fatal(err)
		}
		if realFsync {
			if err := f.Sync(ctx); err != nil {
				b.Fatal(err)
			}
		}
		samples = append(samples, time.Since(t0))
	}
	b.StopTimer()
	reportPercentiles(b, samples)
}

// BenchmarkQualificationRecoveryAndDurabilityLag measures crash replay + publication
// recovery time and background publication durability lag (p50/p95/p99).
func BenchmarkQualificationRecoveryAndDurabilityLag(b *testing.B) {
	b.Run("CrashRecovery_500Files_16MiB", func(b *testing.B) {
		const nFiles = 500
		const fileSize = 32 << 10 // 16 MiB total
		payload := content(42, fileSize)
		samples := make([]time.Duration, 0, b.N)
		var memDelta uint64
		b.SetBytes(nFiles * fileSize)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			jroot := b.TempDir()
			mem := storetest.NewMem()
			c := vfsAge(b)
			c0 := cfg(mem, c, 0)
			c0.Journal = jroot
			if err := vfs.Create(ctx, c0); err != nil {
				b.Fatal(err)
			}
			c1 := cfg(mem, c, 1)
			c1.Journal = jroot
			c1.JournalCapacity = 128 << 20
			c1.Bound = time.Hour
			v1, err := vfs.Attach(ctx, c1)
			if err != nil {
				b.Fatal(err)
			}
			for j := range nFiles {
				write(b, v1, vfs.RootInode, fmt.Sprintf("f%04d", j), payload)
			}
			if err := v1.Sync(ctx); err != nil {
				b.Fatal(err)
			}
			if err := v1.Close(); err != nil {
				b.Fatal(err)
			}
			var mBefore, mAfter runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&mBefore)

			c2 := cfg(mem, c, 2)
			c2.Journal = jroot
			c2.JournalCapacity = 128 << 20
			b.StartTimer()
			t0 := time.Now()
			v2, err := vfs.Attach(ctx, c2)
			elapsed := time.Since(t0)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			runtime.ReadMemStats(&mAfter)
			if mAfter.Alloc > mBefore.Alloc {
				memDelta += mAfter.Alloc - mBefore.Alloc
			}
			if rec := v2.Recovery(); rec.Lost || rec.Replayed == 0 {
				b.Fatalf("unexpected recovery: %+v", rec)
			}
			v2.Close()
			samples = append(samples, elapsed)
		}
		reportPercentiles(b, samples)
		if b.N > 0 {
			b.ReportMetric(float64(memDelta/uint64(b.N))/(1<<20), "heap_MiB/op")
		}
	})

	b.Run("BackgroundDurabilityLag_500msBound", func(b *testing.B) {
		_, _, v, _ := pair(b, nil, func(c *vfs.Config) {
			c.JournalCapacity = 128 << 20
			c.Bound = 500 * time.Millisecond
		})
		in, err := v.Create(ctx, vfs.RootInode, "lag", 0o644)
		if err != nil {
			b.Fatal(err)
		}
		f, err := v.Open(ctx, in.ID)
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		buf := content(77, 16<<10)
		samples := make([]time.Duration, 0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := f.WriteAt(ctx, buf, int64(i)*int64(len(buf))); err != nil {
				b.Fatal(err)
			}
			if err := f.Sync(ctx); err != nil {
				b.Fatal(err)
			}
			target := v.State().Visible
			t0 := time.Now()
			for v.State().Published < target {
				time.Sleep(2 * time.Millisecond)
			}
			samples = append(samples, time.Since(t0))
		}
		b.StopTimer()
		reportPercentiles(b, samples)
	})
}
