package wsbuffer_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/pkg/wsbuffer"
)

func fillPattern(size int, seed byte) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte((int(seed)*31 + i*17 + (i >> 8)) & 0xff)
	}
	return b
}

func TestConfigValidation(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		cfg  wsbuffer.Config
	}{
		{"non-pow2-pagesize", wsbuffer.Config{PageSize: 3000, WALMode: wsbuffer.WALModeFile}},
		{"tiny-pagesize", wsbuffer.Config{PageSize: 256, WALMode: wsbuffer.WALModeFile}},
		{"unaligned-slab", wsbuffer.Config{PageSize: 4096, SlabSize: 5000, WALMode: wsbuffer.WALModeFile}},
		{"unaligned-bypass", wsbuffer.Config{PageSize: 4096, BypassThreshold: 10000, WALMode: wsbuffer.WALModeFile}},
		{"tiny-maxscrap", wsbuffer.Config{PageSize: 4096, MaxScrapBytes: 1024, WALMode: wsbuffer.WALModeFile}},
		{"negative-prealloc", wsbuffer.Config{PreallocBytes: -1, WALMode: wsbuffer.WALModeFile}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := wsbuffer.Open(filepath.Join(dir, tc.name), tc.cfg)
			if !errors.Is(err, wsbuffer.ErrInvalidConfig) {
				t.Fatalf("expected ErrInvalidConfig, got %v", err)
			}
		})
	}

	s, err := wsbuffer.Open(filepath.Join(dir, "valid"), wsbuffer.Config{WALMode: wsbuffer.WALModeFile})
	if err != nil {
		t.Fatalf("open valid: %v", err)
	}
	defer s.Close()
	if _, err := s.WriteAt(context.Background(), []byte("abc"), -1); !errors.Is(err, wsbuffer.ErrInvalidRange) {
		t.Fatalf("expected ErrInvalidRange on negative write offset, got %v", err)
	}
	if _, err := s.ReadAt(make([]byte, 4), -1); !errors.Is(err, wsbuffer.ErrInvalidRange) {
		t.Fatalf("expected ErrInvalidRange on negative read offset, got %v", err)
	}
}

func TestScrapCoalescingAndWAF(t *testing.T) {
	dir := t.TempDir()
	s, err := wsbuffer.Open(dir, wsbuffer.Config{
		PageSize:        4096,
		MaxScrapBytes:   2 << 20,
		WALMode:         wsbuffer.WALModeJournal,
		JournalSegment:  2 << 20,
		JournalCapacity: 8 << 20,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	const numPages = 32
	oracle := make([]byte, numPages*4096)
	rng := rand.New(rand.NewSource(42))

	// Write 500 random 4 KiB updates across 32 hot pages (heavy overwrite coalescing).
	for i := 0; i < 500; i++ {
		pg := rng.Intn(numPages)
		off := int64(pg) * 4096
		data := fillPattern(4096, byte(i+1))
		if _, err := s.WriteAt(ctx, data, off); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		copy(oracle[off:off+4096], data)
	}

	// Verify linearizable read-back BEFORE Sync (directly from scrap overlay).
	got := make([]byte, len(oracle))
	if _, err := s.ReadAt(got, 0); err != nil {
		t.Fatalf("read before sync: %v", err)
	}
	if !bytes.Equal(got, oracle) {
		t.Fatalf("read before sync mismatch")
	}

	if err := s.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}

	st := s.Stats()
	if st.CoalescedWrites < 400 {
		t.Fatalf("expected heavy coalescing (>=400), got %d", st.CoalescedWrites)
	}
	// 500 * 4 KiB = 2 MiB logical; <= 32 * 4 KiB = 128 KiB physical WAL + 128 KiB backing => WAF << 0.25!
	if st.WAF() >= 0.50 {
		t.Fatalf("expected coalesced WAF < 0.50, got %.4f (WAL=%d backing=%d logical=%d)",
			st.WAF(), st.WALPhysicalBytes, st.BackingPhysicalBytes, st.LogicalBytes)
	}
	if st.ReadScrapHits == 0 {
		t.Fatalf("expected scrap read hits > 0")
	}

	// Verify read-back after Sync.
	got2 := make([]byte, len(oracle))
	if _, err := s.ReadAt(got2, 0); err != nil {
		t.Fatalf("read after sync: %v", err)
	}
	if !bytes.Equal(got2, oracle) {
		t.Fatalf("read after sync mismatch")
	}
}

func TestSubPageUnalignedMergingAndPromotion(t *testing.T) {
	dir := t.TempDir()
	s, err := wsbuffer.Open(dir, wsbuffer.Config{
		PageSize:      4096,
		MaxScrapBytes: 1 << 20,
		WALMode:       wsbuffer.WALModeFile,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ctx := context.Background()
	oracle := make([]byte, 3*4096)
	write := func(off int64, size int, seed byte) {
		t.Helper()
		p := fillPattern(size, seed)
		if _, err := s.WriteAt(ctx, p, off); err != nil {
			t.Fatalf("write off=%d size=%d: %v", off, size, err)
		}
		copy(oracle[off:off+int64(size)], p)
	}

	// Disjoint sub-page writes in page 0.
	write(0, 512, 1)
	write(2048, 512, 2)
	// Overlapping sub-page write bridging [256, 2304).
	write(256, 2048, 3)
	// Complete page 0 to trigger promotion to full page!
	write(2304, 4096-2304, 4)
	// Cross-page unaligned 1.5 KiB write spanning page 0/1 and page 1/2.
	write(4096-512, 1536, 5)
	write(2*4096-300, 1536, 6)
	// Sub-page overwrite inside promoted page 0.
	write(100, 300, 7)

	verify := func(stage string) {
		t.Helper()
		got := make([]byte, len(oracle))
		n, err := s.ReadAt(got, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("%s read: %v", stage, err)
		}
		// Logical size is 2*4096 - 300 + 1536 = 9428.
		wantSize := int64(2*4096 - 300 + 1536)
		if int64(n) != wantSize {
			t.Fatalf("%s read len=%d want=%d", stage, n, wantSize)
		}
		if !bytes.Equal(got[:wantSize], oracle[:wantSize]) {
			t.Fatalf("%s content mismatch", stage)
		}
	}

	verify("in-scrap")
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	verify("after-flush")
	if err := s.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	verify("after-sync")
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := wsbuffer.Open(dir, wsbuffer.Config{PageSize: 4096, WALMode: wsbuffer.WALModeFile})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	s = s2
	verify("after-reopen")
}

func TestDirectBypassZeroCopyAndOverlapLinearizability(t *testing.T) {
	dir := t.TempDir()
	s, err := wsbuffer.Open(dir, wsbuffer.Config{
		PageSize:        4096,
		BypassThreshold: 64 << 10,
		MaxScrapBytes:   2 << 20,
		WALMode:         wsbuffer.WALModeJournal,
		JournalSegment:  2 << 20,
		JournalCapacity: 8 << 20,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	const totalSize = 256 << 10
	oracle := make([]byte, totalSize)

	doWrite := func(off int64, p []byte) {
		t.Helper()
		if _, err := s.WriteAt(ctx, p, off); err != nil {
			t.Fatalf("WriteAt(off=%d, len=%d): %v", off, len(p), err)
		}
		copy(oracle[off:off+int64(len(p))], p)
		got := make([]byte, s.Size())
		if _, err := s.ReadAt(got, 0); err != nil {
			t.Fatalf("ReadAt after write(off=%d, len=%d): %v", off, len(p), err)
		}
		if !bytes.Equal(got, oracle[:s.Size()]) {
			t.Fatalf("linearizability mismatch after write(off=%d, len=%d)", off, len(p))
		}
	}

	// 1. Small unaligned writes including one crossing the 64 KiB boundary.
	doWrite(4096, fillPattern(4096, 10))
	doWrite(64*1024-500, fillPattern(1500, 11))
	doWrite(80*1024, fillPattern(4096, 12))

	// 2. Large aligned direct bypass over [64 KiB, 128 KiB):
	//    Must preserve [64 KiB - 500, 64 KiB) from earlier unaligned write,
	//    and supersede [64 KiB, 64 KiB + 1000) and [80 KiB, 84 KiB).
	doWrite(64*1024, fillPattern(64*1024, 20))

	st := s.Stats()
	if st.BypassWrites != 1 || st.BypassBytes != 64*1024 {
		t.Fatalf("expected 1 bypass write of 64 KiB, got writes=%d bytes=%d", st.BypassWrites, st.BypassBytes)
	}
	if st.BypassSupersededBytes == 0 {
		t.Fatalf("expected superseded scrap bytes > 0, got 0")
	}

	// 3. Subsequent small write overlapping inside the direct bypass region.
	doWrite(72*1024+123, fillPattern(1536, 30))

	// 4. Another large aligned direct bypass over [128 KiB, 256 KiB).
	doWrite(128*1024, fillPattern(128*1024, 40))

	if err := s.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	got := make([]byte, totalSize)
	if _, err := s.ReadAt(got, 0); err != nil {
		t.Fatalf("read final: %v", err)
	}
	if !bytes.Equal(got, oracle) {
		t.Fatalf("final content mismatch after sync")
	}
}

func TestCrashRecoveryFileWALAndTornTail(t *testing.T) {
	dir := t.TempDir()
	cfg := wsbuffer.Config{
		PageSize:              4096,
		BypassThreshold:       64 << 10,
		MaxScrapBytes:         1 << 20,
		WALMode:               wsbuffer.WALModeFile,
		DisableAutoCheckpoint: true,
	}
	s, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ctx := context.Background()
	p1 := fillPattern(4096, 1)
	p2 := fillPattern(1536, 2)
	if _, err := s.WriteAt(ctx, p1, 0); err != nil {
		t.Fatalf("write p1: %v", err)
	}
	if _, err := s.WriteAt(ctx, p2, 8192+512); err != nil {
		t.Fatalf("write p2: %v", err)
	}
	// Sync makes the WAL frame durable (AutoCheckpoint is disabled so WAL retains the frame).
	if err := s.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Simulate crash before backing file received the update by zeroing backing file,
	// AND append a torn/truncated trailing frame to wsbuffer.wal.
	if err := s.AbortCrash(); err != nil {
		t.Fatalf("abort crash: %v", err)
	}

	backingPath := filepath.Join(dir, "data.bin")
	if err := os.Truncate(backingPath, 0); err != nil {
		t.Fatalf("truncate backing: %v", err)
	}
	walPath := filepath.Join(dir, "wsbuffer.wal")
	walBytes, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	// Append a valid header + half of a payload (simulating power-loss torn tail).
	extraFrame := wsbuffer.EncodeScrapBatchFrame(nil, 0, 99, []wsbuffer.Extent{
		{Offset: 16384, WriteSeq: 99, Data: fillPattern(4096, 99)},
	})
	tornTail := extraFrame[:len(extraFrame)/2]
	if err := os.WriteFile(walPath, append(walBytes, tornTail...), 0o600); err != nil {
		t.Fatalf("write torn wal: %v", err)
	}

	// Reopen in standard crash-recovery mode: recovers durable frames and truncates torn tail.
	s2, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer s2.Close()

	rep := s2.Recovery()
	if rep.FramesReplayed != 1 || rep.ExtentsApplied != 2 {
		t.Fatalf("unexpected recovery report: %+v", rep)
	}
	if rep.TruncatedTailBytes != int64(len(tornTail)) {
		t.Fatalf("expected TruncatedTailBytes=%d, got %d", len(tornTail), rep.TruncatedTailBytes)
	}

	got1 := make([]byte, 4096)
	if _, err := s2.ReadAt(got1, 0); err != nil {
		t.Fatalf("read p1: %v", err)
	}
	if !bytes.Equal(got1, p1) {
		t.Fatalf("p1 mismatch after crash recovery")
	}
	got2 := make([]byte, 1536)
	if _, err := s2.ReadAt(got2, 8192+512); err != nil {
		t.Fatalf("read p2: %v", err)
	}
	if !bytes.Equal(got2, p2) {
		t.Fatalf("p2 mismatch after crash recovery")
	}
}

func TestCrashRecoveryDirectBarrierPreventsStaleReplay(t *testing.T) {
	dir := t.TempDir()
	cfg := wsbuffer.Config{
		PageSize:              4096,
		BypassThreshold:       64 << 10,
		MaxScrapBytes:         1 << 20,
		WALMode:               wsbuffer.WALModeFile,
		DisableAutoCheckpoint: true,
	}
	s, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ctx := context.Background()
	// W1a: cross-boundary small write [64 KiB - 100, 64 KiB + 100)
	w1a := fillPattern(200, 1)
	// W1b: fully inside [64 KiB, 128 KiB) at offset 80 KiB
	w1b := fillPattern(4096, 2)
	if _, err := s.WriteAt(ctx, w1a, 64*1024-100); err != nil {
		t.Fatalf("write w1a: %v", err)
	}
	if _, err := s.WriteAt(ctx, w1b, 80*1024); err != nil {
		t.Fatalf("write w1b: %v", err)
	}
	// Flush W1a & W1b into WAL (still uncheckpointed!).
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// W2: large aligned direct bypass over [64 KiB, 128 KiB).
	// Emits a zero-payload FrameDirectBarrier so WAL replay won't clobber W2!
	w2 := fillPattern(64*1024, 55)
	if _, err := s.WriteAt(ctx, w2, 64*1024); err != nil {
		t.Fatalf("write w2: %v", err)
	}
	if err := s.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Crash without checkpointing WAL, then reopen to force full WAL replay.
	if err := s.AbortCrash(); err != nil {
		t.Fatalf("abort: %v", err)
	}

	s2, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	rep := s2.Recovery()
	if rep.BarriersApplied != 1 {
		t.Fatalf("expected BarriersApplied=1, got %+v", rep)
	}
	if rep.SupersededExtentsSkipped < 1 {
		t.Fatalf("expected SupersededExtentsSkipped>=1, got %+v", rep)
	}

	// Prefix [64 KiB - 100, 64 KiB) must retain W1a[:100].
	gotPrefix := make([]byte, 100)
	if _, err := s2.ReadAt(gotPrefix, 64*1024-100); err != nil {
		t.Fatalf("read prefix: %v", err)
	}
	if !bytes.Equal(gotPrefix, w1a[:100]) {
		t.Fatalf("prefix before direct barrier corrupted")
	}

	// Direct bypass range [64 KiB, 128 KiB) must retain W2 intact (NOT clobbered by W1a or W1b!).
	gotW2 := make([]byte, 64*1024)
	if _, err := s2.ReadAt(gotW2, 64*1024); err != nil {
		t.Fatalf("read w2: %v", err)
	}
	if !bytes.Equal(gotW2, w2) {
		t.Fatalf("direct bypass range clobbered by stale WAL replay")
	}
}

func TestCRCCorruptionRejection(t *testing.T) {
	frame := wsbuffer.EncodeScrapBatchFrame(nil, 7, 1, []wsbuffer.Extent{
		{Offset: 0, WriteSeq: 1, Data: fillPattern(4096, 9)},
	})

	// 1. Flip a byte in the header.
	badHdr := append([]byte(nil), frame...)
	badHdr[12] ^= 0x01
	if _, _, err := wsbuffer.DecodeFrame(badHdr); !errors.Is(err, wsbuffer.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch on header flip, got %v", err)
	}

	// 2. Flip a byte in the payload.
	badBody := append([]byte(nil), frame...)
	badBody[len(badBody)-20] ^= 0x80
	if _, _, err := wsbuffer.DecodeFrame(badBody); !errors.Is(err, wsbuffer.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch on payload flip, got %v", err)
	}

	// 3. Flip a byte in the WAL file and verify StrictRecovery rejects it.
	dir := t.TempDir()
	cfg := wsbuffer.Config{
		PageSize:              4096,
		WALMode:               wsbuffer.WALModeFile,
		DisableAutoCheckpoint: true,
		StrictRecovery:        true,
	}
	s, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.WriteAt(context.Background(), fillPattern(4096, 3), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := s.AbortCrash(); err != nil {
		t.Fatalf("abort: %v", err)
	}

	walPath := filepath.Join(dir, "wsbuffer.wal")
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	raw[len(raw)-10] ^= 0xff
	if err := os.WriteFile(walPath, raw, 0o600); err != nil {
		t.Fatalf("write corrupted wal: %v", err)
	}

	if _, err := wsbuffer.Open(dir, cfg); !errors.Is(err, wsbuffer.ErrChecksumMismatch) {
		t.Fatalf("expected StrictRecovery ErrChecksumMismatch, got %v", err)
	}
}

func TestJournalWALIntegrationAndSegmentReclaim(t *testing.T) {
	dir := t.TempDir()
	cfg := wsbuffer.Config{
		PageSize:        4096,
		BypassThreshold: 64 << 10,
		MaxScrapBytes:   128 << 10,
		WALMode:         wsbuffer.WALModeJournal,
		// Small segments (512 KiB) so multiple rotations & reclaims happen cleanly.
		JournalSegment:  512 << 10,
		JournalCapacity: 2 << 20,
	}
	s, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open journal mode: %v", err)
	}

	ctx := context.Background()
	const fileSize = 512 << 10
	oracle := make([]byte, fileSize)
	rng := rand.New(rand.NewSource(1345))

	// Write > 3 MiB total across multiple sync rounds to exercise journal rotation,
	// checkpoint meta persistence, Publish(), and Reclaim().
	for round := 0; round < 20; round++ {
		for i := 0; i < 40; i++ {
			off := int64(rng.Intn(fileSize/4096)) * 4096
			p := fillPattern(4096, byte(round*7+i))
			if _, err := s.WriteAt(ctx, p, off); err != nil {
				t.Fatalf("round %d write %d: %v", round, i, err)
			}
			copy(oracle[off:off+4096], p)
		}
		// Interleave a 64 KiB aligned bypass write every 4 rounds.
		if round%4 == 0 {
			off := int64((round/4)%4) * (64 << 10)
			p := fillPattern(64<<10, byte(round+100))
			if _, err := s.WriteAt(ctx, p, off); err != nil {
				t.Fatalf("round %d bypass: %v", round, err)
			}
			copy(oracle[off:off+64*1024], p)
		}
		if err := s.Sync(ctx); err != nil {
			t.Fatalf("round %d sync: %v", round, err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify zero ErrMismatch and 100% byte parity!
	s2, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("reopen journal mode: %v", err)
	}
	defer s2.Close()

	got := make([]byte, fileSize)
	if _, err := s2.ReadAt(got, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read after reopen: %v", err)
	}
	if !bytes.Equal(got[:s2.Size()], oracle[:s2.Size()]) {
		t.Fatalf("content mismatch after journal reopen")
	}
}

func TestConcurrentWritersAndReadersRace(t *testing.T) {
	dir := t.TempDir()
	s, err := wsbuffer.Open(dir, wsbuffer.Config{
		PageSize:        4096,
		BypassThreshold: 64 << 10,
		MaxScrapBytes:   256 << 10,
		WALMode:         wsbuffer.WALModeJournal,
		JournalSegment:  1 << 20,
		JournalCapacity: 4 << 20,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	const workers = 8

	// Each worker owns a disjoint 128 KiB stripe to verify per-stripe integrity
	// while exercising shared scrap buffer, direct bypass, flush, and sync concurrently.
	stripes := make([][]byte, workers)
	for w := 0; w < workers; w++ {
		stripes[w] = make([]byte, 128<<10)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1000 + id)))
			base := int64(id) * (128 << 10)
			for i := 0; i < 80; i++ {
				switch i % 4 {
				case 0: // 4 KiB aligned
					off := int64(rng.Intn(32)) * 4096
					p := fillPattern(4096, byte(id*13+i))
					if _, err := s.WriteAt(ctx, p, base+off); err != nil {
						t.Errorf("worker %d 4k write: %v", id, err)
						return
					}
					copy(stripes[id][off:off+4096], p)
				case 1: // 512 B unaligned
					off := int64(rng.Intn(128*1024 - 512))
					p := fillPattern(512, byte(id*17+i))
					if _, err := s.WriteAt(ctx, p, base+off); err != nil {
						t.Errorf("worker %d 512b write: %v", id, err)
						return
					}
					copy(stripes[id][off:off+512], p)
				case 2: // 64 KiB aligned direct bypass
					off := int64(rng.Intn(2)) * (64 << 10)
					p := fillPattern(64<<10, byte(id*19+i))
					if _, err := s.WriteAt(ctx, p, base+off); err != nil {
						t.Errorf("worker %d 64k bypass: %v", id, err)
						return
					}
					copy(stripes[id][off:off+64*1024], p)
				case 3: // linearizable read-back of own stripe
					buf := make([]byte, 4096)
					off := int64(rng.Intn(32)) * 4096
					n, err := s.ReadAt(buf, base+off)
					if err != nil && !errors.Is(err, io.EOF) {
						t.Errorf("worker %d read: %v", id, err)
						return
					}
					if n > 0 && !bytes.Equal(buf[:n], stripes[id][off:off+int64(n)]) {
						t.Errorf("worker %d stripe read mismatch at off=%d", id, off)
						return
					}
				}
				if i%25 == 24 {
					if err := s.Sync(ctx); err != nil {
						t.Errorf("worker %d sync: %v", id, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and validate every worker's stripe byte-for-byte.
	s2, err := wsbuffer.Open(dir, wsbuffer.Config{
		PageSize:        4096,
		BypassThreshold: 64 << 10,
		WALMode:         wsbuffer.WALModeJournal,
		JournalSegment:  1 << 20,
		JournalCapacity: 4 << 20,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	for w := 0; w < workers; w++ {
		got := make([]byte, 128<<10)
		n, err := s2.ReadAt(got, int64(w)*(128<<10))
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("verify worker %d read: %v", w, err)
		}
		if !bytes.Equal(got[:n], stripes[w][:n]) {
			t.Fatalf("worker %d stripe mismatch after reopen", w)
		}
	}
}

func TestBoundedMemoryCapAndBackpressure(t *testing.T) {
	dir := t.TempDir()
	const capBytes = 64 << 10
	s, err := wsbuffer.Open(dir, wsbuffer.Config{
		PageSize:      4096,
		MaxScrapBytes: capBytes,
		WALMode:       wsbuffer.WALModeFile,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	for pg := 0; pg < 256; pg++ {
		if _, err := s.WriteAt(ctx, fillPattern(4096, byte(pg)), int64(pg)*4096); err != nil {
			t.Fatalf("write pg %d: %v", pg, err)
		}
		if cur := s.Stats().CurrentScrapBytes; cur > capBytes {
			t.Fatalf("scrap memory exceeded cap: %d > %d", cur, capBytes)
		}
	}

	st := s.Stats()
	if st.BackpressureFlushes < 15 {
		t.Fatalf("expected >= 15 backpressure flushes for 1 MiB / 64 KiB cap, got %d", st.BackpressureFlushes)
	}
}

func TestMultiTenantVolumeIsolationAndManager(t *testing.T) {
	dir := t.TempDir()
	mgr, err := wsbuffer.NewManager(dir, 128<<10)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	v1, err := mgr.OpenVolume(101, wsbuffer.Config{
		PageSize:              4096,
		MaxScrapBytes:         96 << 10,
		WALMode:               wsbuffer.WALModeFile,
		DisableAutoCheckpoint: true,
	})
	if err != nil {
		t.Fatalf("open vol 101: %v", err)
	}
	v2, err := mgr.OpenVolume(202, wsbuffer.Config{
		PageSize:              4096,
		MaxScrapBytes:         96 << 10,
		WALMode:               wsbuffer.WALModeFile,
		DisableAutoCheckpoint: true,
	})
	if err != nil {
		t.Fatalf("open vol 202: %v", err)
	}

	ctx := context.Background()
	p1 := fillPattern(4096, 0xAA)
	p2 := fillPattern(4096, 0x55)

	// Write 20 pages to each volume (160 KiB > 128 KiB global cap) -> triggers global eviction.
	for pg := 0; pg < 20; pg++ {
		if _, err := v1.WriteAt(ctx, p1, int64(pg)*4096); err != nil {
			t.Fatalf("v1 write %d: %v", pg, err)
		}
		if _, err := v2.WriteAt(ctx, p2, int64(pg)*4096); err != nil {
			t.Fatalf("v2 write %d: %v", pg, err)
		}
		if tot := mgr.TotalScrapBytes(); tot > 128<<10 {
			t.Fatalf("global scrap cap exceeded: %d > %d", tot, 128<<10)
		}
	}

	// Verify strict read isolation.
	got1 := make([]byte, 4096)
	got2 := make([]byte, 4096)
	if _, err := v1.ReadAt(got1, 0); err != nil || !bytes.Equal(got1, p1) {
		t.Fatalf("v1 isolation mismatch: err=%v", err)
	}
	if _, err := v2.ReadAt(got2, 0); err != nil || !bytes.Equal(got2, p2) {
		t.Fatalf("v2 isolation mismatch: err=%v", err)
	}

	if err := v1.Sync(ctx); err != nil {
		t.Fatalf("v1 sync: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("manager close: %v", err)
	}

	// Verify VolumeID mismatch rejection when opening vol 101's directory under VolumeID 999!
	vol1Dir := filepath.Join(dir, "vol-0000000000000065")
	_, err = wsbuffer.Open(vol1Dir, wsbuffer.Config{
		PageSize: 4096,
		VolumeID: 999,
		WALMode:  wsbuffer.WALModeFile,
	})
	if !errors.Is(err, wsbuffer.ErrVolumeMismatch) {
		t.Fatalf("expected ErrVolumeMismatch on wrong VolumeID, got %v", err)
	}
}

func TestRandomizedLinearizabilityModelCheck(t *testing.T) {
	dir := t.TempDir()
	cfg := wsbuffer.Config{
		PageSize:        4096,
		BypassThreshold: 64 << 10,
		MaxScrapBytes:   192 << 10,
		WALMode:         wsbuffer.WALModeJournal,
		JournalSegment:  512 << 10,
		JournalCapacity: 4 << 20,
	}
	s, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const maxSpace = 512 << 10
	oracle := make([]byte, maxSpace)
	var logicalSize int64
	rng := rand.New(rand.NewSource(2026))
	ctx := context.Background()

	for step := 0; step < 600; step++ {
		op := rng.Intn(100)
		switch {
		case op < 35: // 4 KiB random aligned write
			off := int64(rng.Intn(maxSpace/4096)) * 4096
			p := fillPattern(4096, byte(step))
			if _, err := s.WriteAt(ctx, p, off); err != nil {
				t.Fatalf("step %d 4k: %v", step, err)
			}
			copy(oracle[off:off+4096], p)
			if off+4096 > logicalSize {
				logicalSize = off + 4096
			}
		case op < 65: // unaligned 512 B or 1.5 KiB write
			sz := 512
			if rng.Intn(2) == 1 {
				sz = 1536
			}
			off := int64(rng.Intn(maxSpace - sz))
			p := fillPattern(sz, byte(step+7))
			if _, err := s.WriteAt(ctx, p, off); err != nil {
				t.Fatalf("step %d small(%d): %v", step, sz, err)
			}
			copy(oracle[off:off+int64(sz)], p)
			if off+int64(sz) > logicalSize {
				logicalSize = off + int64(sz)
			}
		case op < 78: // 64 KiB or 128 KiB aligned direct bypass
			sz := 64 << 10
			if rng.Intn(2) == 1 {
				sz = 128 << 10
			}
			off := int64(rng.Intn((maxSpace-sz)/(64<<10)+1)) * (64 << 10)
			p := fillPattern(sz, byte(step+33))
			if _, err := s.WriteAt(ctx, p, off); err != nil {
				t.Fatalf("step %d bypass(%d): %v", step, sz, err)
			}
			copy(oracle[off:off+int64(sz)], p)
			if off+int64(sz) > logicalSize {
				logicalSize = off + int64(sz)
			}
		case op < 90: // random ReadAt check
			if logicalSize == 0 {
				continue
			}
			sz := 1 + rng.Intn(16384)
			off := rng.Int63n(logicalSize)
			got := make([]byte, sz)
			n, err := s.ReadAt(got, off)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("step %d read: %v", step, err)
			}
			wantN := int(min(int64(sz), logicalSize-off))
			if n != wantN {
				t.Fatalf("step %d read len=%d want=%d", step, n, wantN)
			}
			if !bytes.Equal(got[:n], oracle[off:off+int64(n)]) {
				t.Fatalf("step %d read mismatch at off=%d len=%d", step, off, n)
			}
		case op < 95:
			if err := s.Flush(ctx); err != nil {
				t.Fatalf("step %d flush: %v", step, err)
			}
		case op < 98:
			if err := s.Sync(ctx); err != nil {
				t.Fatalf("step %d sync: %v", step, err)
			}
		default: // clean Close + Reopen restart
			if err := s.Close(); err != nil {
				t.Fatalf("step %d close: %v", step, err)
			}
			s, err = wsbuffer.Open(dir, cfg)
			if err != nil {
				t.Fatalf("step %d reopen: %v", step, err)
			}
		}
	}

	if err := s.Sync(ctx); err != nil {
		t.Fatalf("final sync: %v", err)
	}
	if logicalSize > 0 {
		got := make([]byte, logicalSize)
		if _, err := s.ReadAt(got, 0); err != nil {
			t.Fatalf("final read: %v", err)
		}
		if !bytes.Equal(got, oracle[:logicalSize]) {
			t.Fatalf("final full-image oracle mismatch")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("final close: %v", err)
	}
}

func TestCrashRecoveryJournalWALUncheckpointedReplay(t *testing.T) {
	dir := t.TempDir()
	cfg := wsbuffer.Config{
		PageSize:              4096,
		BypassThreshold:       64 << 10,
		MaxScrapBytes:         1 << 20,
		WALMode:               wsbuffer.WALModeJournal,
		DisableAutoCheckpoint: true,
		JournalSegment:        512 << 10,
		JournalCapacity:       2 << 20,
	}
	s, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ctx := context.Background()
	p1 := fillPattern(4096, 11)
	p2 := fillPattern(1536, 22)
	if _, err := s.WriteAt(ctx, p1, 0); err != nil {
		t.Fatalf("write p1: %v", err)
	}
	if _, err := s.WriteAt(ctx, p2, 12288+333); err != nil {
		t.Fatalf("write p2: %v", err)
	}
	// Make WAL durable without checkpointing.
	if err := s.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	// Simulate crash before backing file was synced (truncate backing file).
	if err := s.AbortCrash(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if err := os.Truncate(filepath.Join(dir, "data.bin"), 0); err != nil {
		t.Fatalf("truncate backing: %v", err)
	}

	// Reopen: github.com/hanzoai/vfs/journal replays durable scrap frames to backing!
	s2, err := wsbuffer.Open(dir, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	rep := s2.Recovery()
	if rep.FramesReplayed != 1 || rep.ExtentsApplied != 2 {
		t.Fatalf("unexpected journal recovery report: %+v", rep)
	}

	got1 := make([]byte, 4096)
	if _, err := s2.ReadAt(got1, 0); err != nil || !bytes.Equal(got1, p1) {
		t.Fatalf("p1 mismatch after journal crash recovery: err=%v", err)
	}
	got2 := make([]byte, 1536)
	if _, err := s2.ReadAt(got2, 12288+333); err != nil || !bytes.Equal(got2, p2) {
		t.Fatalf("p2 mismatch after journal crash recovery: err=%v", err)
	}
}

func TestBypassZeroAllocations(t *testing.T) {
	dir := t.TempDir()
	s, err := wsbuffer.Open(dir, wsbuffer.Config{
		PageSize:        4096,
		BypassThreshold: 64 << 10,
		PreallocBytes:   1 << 20,
		WALMode:         wsbuffer.WALModeFile,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	payload := fillPattern(64<<10, 77)
	// Warm once.
	if _, err := s.WriteAt(ctx, payload, 0); err != nil {
		t.Fatalf("warm write: %v", err)
	}

	allocs := testing.AllocsPerRun(20, func() {
		if _, err := s.WriteAt(ctx, payload, 64<<10); err != nil {
			t.Fatalf("bypass write: %v", err)
		}
	})
	if allocs > 0 {
		t.Fatalf("expected 0 heap allocations on zero-copy direct bypass path, got %.2f", allocs)
	}
	st := s.Stats()
	if st.ScrapWrites != 0 || st.BypassWrites == 0 {
		t.Fatalf("expected pure bypass writes, got scrap=%d bypass=%d", st.ScrapWrites, st.BypassWrites)
	}
	if st.WAF() != 1.0 {
		t.Fatalf("expected exact 1.000x WAF on pure direct bypass, got %.4f", st.WAF())
	}
}
