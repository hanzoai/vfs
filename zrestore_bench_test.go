package vfs

// Benchmarks the init-container restore inner loop, which is where restore
// time actually goes.
//
// GetBlock is backend.Get -> blake3 verify -> per-block age decrypt -> cache,
// and zapdb's ChunkReader drives it strictly SEQUENTIALLY. This measures that
// sequential path, then a bounded parallel prefetch, so the core-scaling
// headroom is a number rather than an assumption.
//
// Warm-up leaves the files in the OS page cache on purpose: the cost under
// test is the CPU-bound crypto+verify work, not disk seek. Reading it as a
// disk benchmark would invert the conclusion.
//   GOWORK=off CGO_ENABLED=0 go test -run TestZChunkRestore -v -timeout 20m

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/luxfi/age"
)

func TestZChunkRestore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	crypto, err := NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
	if err != nil {
		t.Fatal(err)
	}
	be, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		t.Fatal(err)
	}

	nBlocks := 8192 // 32 MiB plaintext
	pt := make([]byte, BlockSize)
	rand.Read(pt)

	wv, _ := New(Config{Backend: be, Crypto: crypto, CacheMax: 1 << 30})
	ids := make([]BlockID, nBlocks)
	for i := 0; i < nBlocks; i++ {
		pt[0], pt[1], pt[2] = byte(i), byte(i>>8), byte(i>>16)
		bid, err := wv.PutBlock(ctx, pt)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = bid
	}
	plainMB := float64(nBlocks*BlockSize) / 1e6

	// small cache (4 MiB) => sequential scan of 32 MiB => ~100% miss =>
	// every GetBlock pays backend.Get + verify + decrypt.
	const smallCache = 4 << 20
	freshVFS := func() *VFS {
		v, _ := New(Config{Backend: be, Crypto: crypto, CacheMax: smallCache})
		return v
	}
	// warm OS page cache so file reads are RAM-fast (isolate crypto CPU)
	{
		v := freshVFS()
		for i := 0; i < nBlocks; i++ {
			if _, err := v.GetBlock(ctx, ids[i]); err != nil {
				t.Fatal(err)
			}
		}
	}

	fmt.Printf("\n=== vfs restore inner loop: GetBlock (X25519, 4KiB blocks, page-cache warm, M1 Max) ===\n")
	fmt.Printf("blocks=%d plaintext=%.0fMB\n", nBlocks, plainMB)

	seq := func() float64 {
		v := freshVFS()
		t0 := time.Now()
		for i := 0; i < nBlocks; i++ {
			if _, err := v.GetBlock(ctx, ids[i]); err != nil {
				t.Fatal(err)
			}
		}
		el := time.Since(t0)
		mbps := plainMB / el.Seconds()
		fmt.Printf("%-22s wall=%-8s %6.1f MB/s  (%.0f us/block)\n",
			"SEQUENTIAL(current)", el.Round(time.Millisecond), mbps,
			float64(el.Microseconds())/float64(nBlocks))
		return mbps
	}
	base := seq()

	for _, workers := range []int{2, 4, 8, 16} {
		v := freshVFS()
		var idx int64 = -1
		var wg sync.WaitGroup
		t0 := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := atomic.AddInt64(&idx, 1)
					if i >= int64(nBlocks) {
						return
					}
					if _, err := v.GetBlock(ctx, ids[i]); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		el := time.Since(t0)
		mbps := plainMB / el.Seconds()
		fmt.Printf("%-22s wall=%-8s %6.1f MB/s  (%.1fx)  workers=%d\n",
			"PARALLEL", el.Round(time.Millisecond), mbps, mbps/base, workers)
	}
}
