package vfs_test

import (
	"fmt"
	"testing"

	"github.com/hanzoai/vfs"
)

// BenchmarkWriteAt is the foreground cost of a write: the change is sealed, appended to the
// journal and applied, and nothing waits for the store. Throughput is the speed of one
// writer; the store is memory, so publication costs only the CPU it takes.
func BenchmarkWriteAt(b *testing.B) {
	for _, size := range []int{4 << 10, 128 << 10, 1 << 20} {
		b.Run(byteSize(size), func(b *testing.B) {
			_, _, v, _ := pair(b, nil, func(c *vfs.Config) { c.Capacity = 1 << 30 })
			in, err := v.Create(ctx, vfs.RootInode, "f", 0o644)
			if err != nil {
				b.Fatal(err)
			}
			f, err := v.Open(ctx, in.ID)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			buf := content(1, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := f.WriteAt(ctx, buf, int64(i)*int64(size)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func byteSize(n int) string {
	switch {
	case n >= 1<<20:
		return "1MiB"
	case n >= 1<<10 && n < 1<<20 && n%(1<<10) == 0 && n >= 128<<10:
		return "128KiB"
	}
	return "4KiB"
}

// BenchmarkFlushOfASmallFile is a create, a small write and a Flush: one generation of one file.
func BenchmarkFlushOfASmallFile(b *testing.B) {
	_, _, v, _ := pair(b, nil, nil)
	buf := content(3, 2000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in, err := v.Create(ctx, vfs.RootInode, fmt.Sprint("f", i), 0o644)
		if err != nil {
			b.Fatal(err)
		}
		f, err := v.Open(ctx, in.ID)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := f.WriteAt(ctx, buf, 0); err != nil {
			b.Fatal(err)
		}
		f.Close()
		if err := v.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
