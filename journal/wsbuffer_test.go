package journal_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// obsDisk wraps a journal.Disk to count exact underlying WriteAt calls,
// bytes, and 4 KiB physical pages touched (used for baseline measurements).
type obsDisk struct {
	journal.Disk
	pageSize      int64
	writeOps      atomic.Uint64
	writeBytes    atomic.Uint64
	physicalPages atomic.Uint64
	syncOps       atomic.Uint64
}

func newObsDisk(d journal.Disk, pageSize int) *obsDisk {
	return &obsDisk{Disk: d, pageSize: int64(pageSize)}
}

func (d *obsDisk) Create(name string, size int64) (journal.File, error) {
	f, err := d.Disk.Create(name, size)
	if err != nil {
		return nil, err
	}
	return &obsFile{File: f, d: d}, nil
}

func (d *obsDisk) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	if err != nil {
		return nil, err
	}
	return &obsFile{File: f, d: d}, nil
}

func (d *obsDisk) Warnings() []string {
	if w, ok := d.Disk.(interface{ Warnings() []string }); ok {
		return w.Warnings()
	}
	return nil
}

type obsFile struct {
	journal.File
	d *obsDisk
}

func (f *obsFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	if n > 0 {
		firstPg := off / f.d.pageSize
		lastPg := (off + int64(n) - 1) / f.d.pageSize
		f.d.writeOps.Add(1)
		f.d.writeBytes.Add(uint64(n))
		f.d.physicalPages.Add(uint64(lastPg - firstPg + 1))
	}
	return n, err
}

func (f *obsFile) Sync() error {
	f.d.syncOps.Add(1)
	return f.File.Sync()
}

// TestWSBufferBitExactDataIntegrity verifies bit-exact read-your-writes coherence
// before Flush, after Flush, and after Close+reopen on a real OS disk file across
// small unaligned, large aligned, large unaligned (split head+bypass+tail), and
// disjoint intra-page overwrites.
func TestWSBufferBitExactDataIntegrity(t *testing.T) {
	dir := scratch(t)
	disk, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	const fileSize = 512 << 10
	raw, err := disk.Create("000001.seg", fileSize)
	if err != nil {
		t.Fatal(err)
	}

	cfg := journal.WSBufferConfig{
		PageSize:        4096,
		Capacity:        16 << 10,
		BypassThreshold: 4096,
	}
	wf := journal.NewWSBufferFile(raw, cfg)
	golden := make([]byte, fileSize)
	rng := rand.New(rand.NewPCG(42, 99))

	writeCheck := func(off int64, data []byte) {
		t.Helper()
		n, err := wf.WriteAt(data, off)
		if err != nil || n != len(data) {
			t.Fatalf("WriteAt(off=%d, len=%d) = (%d, %v)", off, len(data), n, err)
		}
		copy(golden[off:], data)
	}

	// 1. Small unaligned sequential writes starting at journal dataStart (80).
	pos := int64(80)
	for i := 0; i < 64; i++ {
		sz := 128 + rng.IntN(700)
		buf := make([]byte, sz)
		for j := range buf {
			buf[j] = byte((i*31 + j) & 0xff)
		}
		writeCheck(pos, buf)
		pos += int64(sz)
	}

	// 2. Disjoint intra-page overwrite (like writeMark at [40..60) on page 0).
	writeCheck(40, bytes.Repeat([]byte{0xAB}, 20))
	writeCheck(60, bytes.Repeat([]byte{0xCD}, 20))

	// 3. Pure large aligned writes (bypass path, including over previously dirty pages).
	for i := 0; i < 8; i++ {
		off := int64((32 + i*4) * 4096)
		buf := make([]byte, 16384)
		for j := range buf {
			buf[j] = byte((i*17 + j*3) & 0xff)
		}
		writeCheck(off, buf)
	}

	// 4. Medium unaligned writes (< Capacity) and bulk unaligned writes (>= Capacity)
	// overlapping dirty edge and interior scrap pages.
	for i := 0; i < 8; i++ {
		off := int64(256*1024 + i*17000 + 137)
		buf := make([]byte, 12500)
		for j := range buf {
			buf[j] = byte((i*53 + j*7) & 0xff)
		}
		writeCheck(off, buf)
	}
	// Dirty start edge, interior page, and end edge, then overwrite with 20,000 B unaligned bulk write.
	writeCheck(384*1024+16, bytes.Repeat([]byte{0xE1}, 512))
	writeCheck(384*1024+8192+64, bytes.Repeat([]byte{0xE2}, 512))
	writeCheck(384*1024+20000+256, bytes.Repeat([]byte{0xE3}, 512))
	bulkUnaligned := make([]byte, 20000)
	for j := range bulkUnaligned {
		bulkUnaligned[j] = byte((j*11 + 5) & 0xff)
	}
	writeCheck(384*1024+200, bulkUnaligned)

	// 5. Random small partial overwrites across the entire file.
	for i := 0; i < 200; i++ {
		sz := 32 + rng.IntN(1500)
		off := int64(rng.IntN(fileSize - sz))
		buf := make([]byte, sz)
		for j := range buf {
			buf[j] = byte((i*13 + j) & 0xff)
		}
		writeCheck(off, buf)
	}

	// Verify bit-exact ReadAt BEFORE Flush (dirty overlay must be exact).
	got := make([]byte, fileSize)
	if n, err := wf.ReadAt(got, 0); err != nil || n != fileSize {
		t.Fatalf("ReadAt before Flush: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, golden) {
		t.Fatalf("data mismatch before Flush (dirty overlay broken)")
	}

	// Verify Sync drains dirty bytes to 0 and matches golden.
	if err := wf.Sync(); err != nil {
		t.Fatal(err)
	}
	if d := wf.DirtyBytes(); d != 0 {
		t.Fatalf("DirtyBytes after Sync = %d, want 0", d)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen directly from OS disk (bypassing WSBuffer) and verify bit-exact persistence.
	reopened, err := disk.Open("000001.seg")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	gotDisk := make([]byte, fileSize)
	if n, err := reopened.ReadAt(gotDisk, 0); err != nil || n != fileSize {
		t.Fatalf("reopened ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(gotDisk, golden) {
		t.Fatalf("data mismatch on disk after Close + reopen")
	}
}

// TestWSBufferZeroCopyBypassCounter verifies that large page-aligned writes
// bypass the scrap buffer completely (DirectBypassOps incremented, ScrapCopyBytes == 0).
func TestWSBufferZeroCopyBypassCounter(t *testing.T) {
	dir := scratch(t)
	disk, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	const fileSize = 2 << 20
	raw, err := disk.Create("000002.seg", fileSize)
	if err != nil {
		t.Fatal(err)
	}

	wf := journal.NewWSBufferFile(raw, journal.DefaultWSBufferConfig())
	defer wf.Close()

	sizes := []int{4096, 16384, 65536}
	var off int64
	var expectOps uint64
	var expectBytes uint64

	for round := 0; round < 10; round++ {
		for _, sz := range sizes {
			buf := bytes.Repeat([]byte{byte(round + sz/4096)}, sz)
			n, err := wf.WriteAt(buf, off)
			if err != nil || n != sz {
				t.Fatalf("WriteAt(off=%d, sz=%d) = (%d, %v)", off, sz, n, err)
			}
			off += int64(sz)
			expectOps++
			expectBytes += uint64(sz)
		}
	}

	st := wf.Stats()
	if st.DirectBypassOps != expectOps {
		t.Fatalf("DirectBypassOps = %d, want %d", st.DirectBypassOps, expectOps)
	}
	if st.DirectBypassBytes != expectBytes {
		t.Fatalf("DirectBypassBytes = %d, want %d", st.DirectBypassBytes, expectBytes)
	}
	if st.ScrapCopyBytes != 0 {
		t.Fatalf("ScrapCopyBytes = %d, want 0 for pure page-aligned large writes", st.ScrapCopyBytes)
	}
	if st.ScrapAbsorbedOps != 0 {
		t.Fatalf("ScrapAbsorbedOps = %d, want 0", st.ScrapAbsorbedOps)
	}
	if wf.DirtyBytes() != 0 {
		t.Fatalf("DirtyBytes = %d, want 0", wf.DirtyBytes())
	}
	if amp := st.PhysicalWriteAmp(4096); amp != 1.0 {
		t.Fatalf("PhysicalWriteAmp = %.4f, want 1.0000", amp)
	}
}

// TestWSBufferBypassInvalidatesDirtyScrap verifies that a large page-aligned
// bypass write completely overwriting dirty scrap pages invalidates them so a
// later Flush does not overwrite newer bypassed data with stale scrap bytes.
func TestWSBufferBypassInvalidatesDirtyScrap(t *testing.T) {
	dir := scratch(t)
	disk, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	raw, err := disk.Create("000003.seg", 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	wf := journal.NewWSBufferFile(raw, journal.DefaultWSBufferConfig())
	defer wf.Close()

	// Dirty pages 1 and 2 in the scrap buffer with old bytes (0x11).
	if _, err := wf.WriteAt(bytes.Repeat([]byte{0x11}, 512), 4096+128); err != nil {
		t.Fatal(err)
	}
	if _, err := wf.WriteAt(bytes.Repeat([]byte{0x22}, 512), 8192+256); err != nil {
		t.Fatal(err)
	}
	if wf.DirtyBytes() == 0 {
		t.Fatal("expected dirty bytes in scrap buffer")
	}

	// Overwrite pages 1..4 with a pure aligned bypass write of 0x99.
	fresh := bytes.Repeat([]byte{0x99}, 16384)
	if _, err := wf.WriteAt(fresh, 4096); err != nil {
		t.Fatal(err)
	}
	if wf.DirtyBytes() != 0 {
		t.Fatalf("expected invalidated scrap pages -> DirtyBytes=0, got %d", wf.DirtyBytes())
	}

	// Flush + Sync must keep 0x99 intact.
	if err := wf.Sync(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 16384)
	if _, err := wf.ReadAt(got, 4096); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatal("stale scrap bytes leaked after aligned bypass overwrite")
	}
}

// TestWSBufferSmallUnalignedWriteAmpReduction verifies on real OS disk files
// that WSBuffer dramatically reduces physical page write amplification and
// underlying WriteAt syscalls compared to baseline unbuffered writes.
func TestWSBufferSmallUnalignedWriteAmpReduction(t *testing.T) {
	const (
		fileSize = 1 << 20
		writeSz  = 256
		numOps   = 512 // 128 KiB total logical data
	)
	payload := bytes.Repeat([]byte("A"), writeSz)

	// Baseline: direct journal.Files observed with obsDisk.
	baseDir := filepath.Join(scratch(t), "base")
	baseRaw, err := journal.Files(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	baseObs := newObsDisk(baseRaw, 4096)
	bf, err := baseObs.Create("000000.seg", fileSize)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < numOps; i++ {
		if _, err := bf.WriteAt(payload, int64(80+i*writeSz)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bf.Sync(); err != nil {
		t.Fatal(err)
	}
	bf.Close()
	baseRaw.Close()
	baseAmp := (float64(baseObs.physicalPages.Load()) * 4096.0) / float64(numOps*writeSz)

	// WSBuffer: same real journal.Files wrapped with WSBufferDisk (16 KiB scrap buffer).
	wsDir := filepath.Join(scratch(t), "ws")
	wsRaw, err := journal.Files(wsDir)
	if err != nil {
		t.Fatal(err)
	}
	wsObs := newObsDisk(wsRaw, 4096)
	wsDisk := journal.WrapWSBuffer(wsObs, journal.DefaultWSBufferConfig())
	wf, err := wsDisk.Create("000000.seg", fileSize)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < numOps; i++ {
		if _, err := wf.WriteAt(payload, int64(80+i*writeSz)); err != nil {
			t.Fatal(err)
		}
	}
	if err := wf.Sync(); err != nil {
		t.Fatal(err)
	}
	wf.Close()
	wsRaw.Close()

	st := wsDisk.Stats()
	wsAmp := st.PhysicalWriteAmp(4096)

	t.Logf("256B unaligned x%d: baseline writeOps=%d physPages=%d amp=%.2fx | wsbuffer writeOps=%d physPages=%d amp=%.2fx coalesced=%d",
		numOps, baseObs.writeOps.Load(), baseObs.physicalPages.Load(), baseAmp,
		st.PhysicalWriteOps, st.PhysicalPages, wsAmp, st.CoalescedHits)

	if !(wsAmp < baseAmp/5.0) {
		t.Fatalf("expected >5x write-amp reduction: wsAmp=%.2fx vs baseAmp=%.2fx", wsAmp, baseAmp)
	}
	if !(st.PhysicalWriteOps < baseObs.writeOps.Load()/10) {
		t.Fatalf("expected >10x physical WriteAt reduction: ws=%d vs base=%d", st.PhysicalWriteOps, baseObs.writeOps.Load())
	}
	if wsObs.writeOps.Load() != st.PhysicalWriteOps {
		t.Fatalf("observed underlying WriteAt ops %d != WSBufferStats.PhysicalWriteOps %d",
			wsObs.writeOps.Load(), st.PhysicalWriteOps)
	}
}

// TestWSBufferJournalIntegrationAndCrashRecovery exercises the full journal.Journal
// with Options.WSBuffer enabled on both real OS files (Append/Sync/Replay/Publish/
// Reclaim/Close/Reopen) and systematic crash.Disk power-cut replay.
func TestWSBufferJournalIntegrationAndCrashRecovery(t *testing.T) {
	// Part 1: Full lifecycle on real OS disk with mixed small & large records.
	dir := scratch(t)
	cfg := journal.DefaultWSBufferConfig()
	j, err := journal.Open(dir, journal.Options{
		Capacity: 4 << 20,
		Segment:  256 << 10,
		WSBuffer: &cfg,
	})
	if err != nil {
		t.Fatal(err)
	}

	var want []entry
	for i := 1; i <= 200; i++ {
		var p []byte
		if i%10 == 0 {
			// Large payload (16 KiB) exercising split head + zero-copy aligned middle + tail.
			p = bytes.Repeat([]byte{byte(i)}, 16<<10)
		} else {
			// Small unaligned payload (64..320 B) absorbed into scrap buffer.
			p = payload(uint64(i))
		}
		seq := mustAppend(t, j, p)
		want = append(want, entry{seq, string(p)})
		if i%40 == 0 {
			mustSync(t, j, seq)
		}
	}
	mustSync(t, j, 200)

	// Replay in same run must match bit-exact.
	if got := replayed(t, j); !reflect.DeepEqual(got, want) {
		t.Fatalf("same-run replay mismatch: got %d records, want %d", len(got), len(want))
	}

	// Publish & Reclaim first 100 records to test mark writing + segment wiping through WSBuffer.
	if err := j.Publish(100); err != nil {
		t.Fatal(err)
	}
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}

	wsStats := j.WSBufferStats()
	if wsStats.DirectBypassOps == 0 {
		t.Fatalf("expected DirectBypassOps > 0 for 16 KiB journal records, got 0")
	}
	if wsStats.ScrapAbsorbedOps == 0 {
		t.Fatalf("expected ScrapAbsorbedOps > 0 for small journal records, got 0")
	}
	if err := closeBounded(t, j); err != nil {
		t.Fatal(err)
	}

	// Reopen with Published=100 (without WSBuffer, proving on-disk format is 100% identical!).
	j2, err := journal.Open(dir, journal.Options{
		Capacity:  4 << 20,
		Segment:   256 << 10,
		Published: 100,
	})
	if err != nil {
		t.Fatalf("reopen without WSBuffer: %v", err)
	}
	defer closeWithin(t, j2)
	got2 := replayed(t, j2)
	if !reflect.DeepEqual(got2, want[100:]) {
		t.Fatalf("reopen replay mismatch: got %d records, want %d", len(got2), len(want[100:]))
	}

	// Part 2: Power-cut crash recovery using crash.Disk under WSBuffer.
	// Sync records 1..25, append unsynced tail 26..40, then simulate power loss
	// (Lost, Prefix, Torn, Scatter) at every sync persistence point.
	cd := crash.New()
	wsCfg := journal.WSBufferConfig{PageSize: 4096, Capacity: 8192, BypassThreshold: 4096}
	jc := open(t, journal.Options{
		Capacity: 6 * 8192,
		Segment:  8192,
		Disk:     cd,
		Clock:    ticking{newClock(), 2e6},
		WSBuffer: &wsCfg,
	})
	var expected []entry
	for i := uint64(1); i <= 25; i++ {
		p := payload(i)
		seq := mustAppend(t, jc, p)
		expected = append(expected, entry{seq, string(p)})
		if i%5 == 0 {
			mustSync(t, jc, seq)
			cd.Mark(seq)
		}
	}
	// Append unsynced records that may sit partially in the scrap buffer or volatile writes.
	for i := uint64(26); i <= 40; i++ {
		mustAppend(t, jc, payload(i))
	}
	crashPoint := cd.Len()
	closeWithin(t, jc)

	for _, v := range cd.Variants(crashPoint, 8, 42) {
		snap := cd.Image(crashPoint, v)
		jr, err := journal.Open("", journal.Options{
			Capacity: 6 * 8192,
			Segment:  8192,
			Disk:     snap,
			Clock:    newClock(),
			WSBuffer: &wsCfg,
		})
		if err != nil {
			t.Fatalf("crash variant %s: Open failed: %v", v, err)
		}
		got := replayed(t, jr)
		if len(got) < len(expected) {
			closeWithin(t, jr)
			t.Fatalf("crash variant %s: lost synced records: got %d, want >= %d", v, len(got), len(expected))
		}
		if !reflect.DeepEqual(got[:len(expected)], expected) {
			closeWithin(t, jr)
			t.Fatalf("crash variant %s: synced prefix corrupted", v)
		}
		// Ensure journal remains cleanly appendable after crash recovery.
		nextSeq := mustAppend(t, jr, []byte("post-crash-record"))
		mustSync(t, jr, nextSeq)
		closeWithin(t, jr)
	}
}

// TestWSBufferNegativeRegimeSyncEveryWrite verifies the honest boundary case
// where Sync() is called after every single 256 B write: durability requires
// flushing on every sync barrier, so writes cannot coalesce across barriers.
func TestWSBufferNegativeRegimeSyncEveryWrite(t *testing.T) {
	dir := scratch(t)
	disk, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	raw, err := disk.Create("000004.seg", 256<<10)
	if err != nil {
		t.Fatal(err)
	}
	wf := journal.NewWSBufferFile(raw, journal.DefaultWSBufferConfig())
	defer wf.Close()

	const ops = 64
	buf := bytes.Repeat([]byte("S"), 256)
	for i := 0; i < ops; i++ {
		if _, err := wf.WriteAt(buf, int64(80+i*256)); err != nil {
			t.Fatal(err)
		}
		if err := wf.Sync(); err != nil {
			t.Fatal(err)
		}
		if wf.DirtyBytes() != 0 {
			t.Fatalf("iter %d: DirtyBytes=%d after Sync, want 0", i, wf.DirtyBytes())
		}
	}
	st := wf.Stats()
	if st.SyncOps != ops || st.PhysicalWriteOps != ops {
		t.Fatalf("sync-every-write: SyncOps=%d PhysicalWriteOps=%d, want %d each", st.SyncOps, st.PhysicalWriteOps, ops)
	}
	if amp := st.PhysicalWriteAmp(4096); amp < 15.0 {
		t.Fatalf("expected honest ~16x write-amp under sync-every-write, got %.2fx", amp)
	}
}

// TestWSBufferConcurrentMultiTenantStress runs concurrent writers/readers/syncers
// on both a shared WSBufferFile and multiple tenant Journals with WSBuffer enabled
// to verify zero races under `go test -race`.
func TestWSBufferConcurrentMultiTenantStress(t *testing.T) {
	// 1. Concurrent goroutines on one WSBufferFile across disjoint ranges + shared reader.
	dir := scratch(t)
	disk, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	const (
		workers   = 8
		regionLen = 32 << 10
		totalLen  = workers * regionLen
	)
	raw, err := disk.Create("000005.seg", totalLen)
	if err != nil {
		t.Fatal(err)
	}
	wf := journal.NewWSBufferFile(raw, journal.WSBufferConfig{
		PageSize:        4096,
		Capacity:        16 << 10,
		BypassThreshold: 4096,
	})
	expected := make([]byte, totalLen)

	var wg sync.WaitGroup
	errCh := make(chan error, workers*2)
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := int64(w * regionLen)
			// Mix small unaligned writes and large aligned writes in each worker's region.
			for off := 0; off < regionLen; {
				sz := 192
				if (off/4096)%3 == 0 && off%4096 == 0 && off+4096 <= regionLen {
					sz = 4096
				}
				if off+sz > regionLen {
					sz = regionLen - off
				}
				buf := make([]byte, sz)
				for i := range buf {
					buf[i] = byte((w*37 + off + i) & 0xff)
				}
				copy(expected[base+int64(off):], buf)
				if _, err := wf.WriteAt(buf, base+int64(off)); err != nil {
					errCh <- err
					return
				}
				if off%2048 == 0 {
					tmp := make([]byte, sz)
					if _, err := wf.ReadAt(tmp, base+int64(off)); err != nil {
						errCh <- err
						return
					}
					if !bytes.Equal(tmp, buf) {
						errCh <- fmt.Errorf("worker %d readback mismatch at off=%d", w, off)
						return
					}
				}
				if off%8192 == 0 {
					if err := wf.Sync(); err != nil {
						errCh <- err
						return
					}
				}
				off += sz
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if err := wf.Sync(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, totalLen)
	if _, err := wf.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, expected) {
		t.Fatal("concurrent WSBufferFile final contents mismatch")
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Multi-tenant concurrent Journals (4 isolated tenants, 4 writers each).
	const tenants = 4
	var twg sync.WaitGroup
	terr := make(chan error, tenants)
	for tid := 0; tid < tenants; tid++ {
		tid := tid
		twg.Add(1)
		go func() {
			defer twg.Done()
			tdir := filepath.Join(dir, fmt.Sprintf("tenant-%d", tid))
			cfg := journal.DefaultWSBufferConfig()
			tj, err := journal.Open(tdir, journal.Options{
				Capacity: 2 << 20,
				Segment:  256 << 10,
				Lineage:  []byte(fmt.Sprintf("tenant-%d-round-1", tid)),
				WSBuffer: &cfg,
			})
			if err != nil {
				terr <- err
				return
			}

			var wwg sync.WaitGroup
			var hashes sync.Map
			const perWriter = 40
			for g := 0; g < 4; g++ {
				g := g
				wwg.Add(1)
				go func() {
					defer wwg.Done()
					for i := 0; i < perWriter; i++ {
						sz := 160 + (i%5)*64
						if i%12 == 0 {
							sz = 8192
						}
						buf := make([]byte, sz)
						binary.LittleEndian.PutUint64(buf[:8], uint64(tid<<32|g<<16|i))
						seq, err := tj.Append(context.Background(), buf)
						if err != nil {
							terr <- err
							return
						}
						hashes.Store(seq, sha256.Sum256(buf))
						if i%10 == 0 {
							if err := tj.Sync(context.Background(), seq); err != nil {
								terr <- err
								return
							}
						}
					}
				}()
			}
			wwg.Wait()
			if err := tj.Sync(context.Background(), tj.Marks().Visible); err != nil {
				terr <- err
				return
			}
			var count int
			if err := tj.Replay(func(r journal.Record) error {
				wantHash, ok := hashes.Load(r.Seq)
				if !ok {
					return fmt.Errorf("tenant %d unexpected seq %d", tid, r.Seq)
				}
				if sha256.Sum256(r.Data) != wantHash.([32]byte) {
					return fmt.Errorf("tenant %d checksum mismatch at seq %d", tid, r.Seq)
				}
				count++
				return nil
			}); err != nil {
				terr <- err
				return
			}
			if count != 4*perWriter {
				terr <- fmt.Errorf("tenant %d replayed %d records, want %d", tid, count, 4*perWriter)
				return
			}
			if err := tj.Close(); err != nil {
				terr <- err
				return
			}
		}()
	}
	twg.Wait()
	close(terr)
	for err := range terr {
		t.Fatal(err)
	}
}

// TestWSBufferOversizedScrapMemoryPressure verifies the honest second negative
// regime: growing scrap capacity well beyond 16 KiB increases MaxDirtyBytes
// (dirty-data window before flush) linearly without further write-amp reduction
// once 4 KiB pages are already filled.
func TestWSBufferOversizedScrapMemoryPressure(t *testing.T) {
	dir := scratch(t)
	disk, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	const (
		fileSize = 2 << 20
		writeSz  = 256
		numOps   = 4096 // 1 MiB logical
	)
	payload := bytes.Repeat([]byte("M"), writeSz)
	caps := []int{4 << 10, 16 << 10, 64 << 10, 256 << 10}
	var prevDirty uint64
	var amp16K, amp256K float64

	for idx, capBytes := range caps {
		name := fmt.Sprintf("0001%02d.seg", idx)
		raw, err := disk.Create(name, fileSize)
		if err != nil {
			t.Fatal(err)
		}
		wf := journal.NewWSBufferFile(raw, journal.WSBufferConfig{
			PageSize:        4096,
			Capacity:        capBytes,
			BypassThreshold: 4096,
		})
		for i := 0; i < numOps; i++ {
			if _, err := wf.WriteAt(payload, int64(80+i*writeSz)); err != nil {
				t.Fatal(err)
			}
		}
		if err := wf.Sync(); err != nil {
			t.Fatal(err)
		}
		st := wf.Stats()
		amp := st.PhysicalWriteAmp(4096)
		wf.Close()
		t.Logf("cap=%d KiB: maxDirty=%d KiB physOps=%d amp=%.4fx", capBytes>>10, st.MaxDirtyBytes>>10, st.PhysicalWriteOps, amp)

		if idx > 0 && st.MaxDirtyBytes <= prevDirty {
			t.Fatalf("expected MaxDirtyBytes to grow with larger capacity: cap=%d had %d <= prev %d", capBytes, st.MaxDirtyBytes, prevDirty)
		}
		if st.MaxDirtyBytes > uint64(capBytes) {
			t.Fatalf("MaxDirtyBytes %d exceeded configured Capacity %d", st.MaxDirtyBytes, capBytes)
		}
		prevDirty = st.MaxDirtyBytes
		if capBytes == 16<<10 {
			amp16K = amp
		}
		if capBytes == 256<<10 {
			amp256K = amp
		}
	}
	// Once 4 KiB pages fill cleanly at 16 KiB, a 16x larger scrap buffer (256 KiB)
	// cannot materially reduce write amplification further (plateau near 1.00x).
	if amp16K-amp256K > 0.05 {
		t.Fatalf("expected write-amp plateau between 16 KiB (%.4fx) and 256 KiB (%.4fx)", amp16K, amp256K)
	}
}

// TestWSBufferJournalEndToEndWriteAmp verifies that enabling Options.WSBuffer on
// a real journal.Journal reduces underlying WriteAt syscalls and physical page
// write amplification compared to unmodified baseline journal.Open.
func TestWSBufferJournalEndToEndWriteAmp(t *testing.T) {
	const numRecords = 256
	rec := bytes.Repeat([]byte("j"), 256)

	runOne := func(name string, wsCfg *journal.WSBufferConfig) (writeOps, physPages uint64, amp float64) {
		d := filepath.Join(scratch(t), name)
		raw, err := journal.Files(d)
		if err != nil {
			t.Fatal(err)
		}
		obs := newObsDisk(raw, 4096)
		j, err := journal.Open(d, journal.Options{
			Capacity: 4 << 20,
			Segment:  1 << 20,
			Disk:     obs,
			WSBuffer: wsCfg,
		})
		if err != nil {
			t.Fatal(err)
		}
		var last uint64
		for i := 0; i < numRecords; i++ {
			seq := mustAppend(t, j, rec)
			last = seq
			// Simulate interactive single-writer pacing (forcing writer goroutine
			// to flush small unaligned batches between Sync barriers).
			if i%32 == 31 {
				mustSync(t, j, seq)
			}
		}
		mustSync(t, j, last)
		logical := j.Stats().Bytes
		if err := closeBounded(t, j); err != nil {
			t.Fatal(err)
		}
		writeOps = obs.writeOps.Load()
		physPages = obs.physicalPages.Load()
		amp = (float64(physPages) * 4096.0) / float64(logical)
		return writeOps, physPages, amp
	}

	baseOps, basePages, baseAmp := runOne("base-j", nil)
	cfg := journal.DefaultWSBufferConfig()
	wsOps, wsPages, wsAmp := runOne("ws-j", &cfg)

	t.Logf("End-to-end Journal (256B x %d, sync/32): baseline ops=%d pages=%d amp=%.2fx | wsbuffer ops=%d pages=%d amp=%.2fx",
		numRecords, baseOps, basePages, baseAmp, wsOps, wsPages, wsAmp)

	if wsAmp > baseAmp {
		t.Fatalf("WSBuffer journal write-amp %.2fx exceeded baseline %.2fx", wsAmp, baseAmp)
	}
	if wsOps > baseOps {
		t.Fatalf("WSBuffer journal WriteAt ops %d exceeded baseline %d", wsOps, baseOps)
	}
}
