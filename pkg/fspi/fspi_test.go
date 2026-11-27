package fspi_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/pkg/fspi"
)

func TestCRC16T10DIFStandardVectors(t *testing.T) {
	// Standard ANSI INCITS 394 / T10-DIF CRC-16 check value for ASCII "123456789".
	const wantCheck uint16 = 0xD0DB
	if got := fspi.CRC16T10DIF([]byte("123456789")); got != wantCheck {
		t.Fatalf("CRC16T10DIF(\"123456789\") = 0x%04x, want 0x%04x", got, wantCheck)
	}
	if got := fspi.CRC16T10DIF(make([]byte, 512)); got != 0x0000 {
		t.Fatalf("CRC16T10DIF(zeros) = 0x%04x, want 0x0000", got)
	}

	// Verify 100% detection of every single-bit flip across a 512 B sector.
	sector := make([]byte, fspi.SectorSize512)
	for i := range sector {
		sector[i] = byte((i*73 + 19) & 0xFF)
	}
	orig := fspi.CRC16T10DIF(sector)
	for byteIdx := 0; byteIdx < len(sector); byteIdx += 7 {
		for bit := uint(0); bit < 8; bit++ {
			sector[byteIdx] ^= 1 << bit
			if fspi.CRC16T10DIF(sector) == orig {
				t.Fatalf("CRC16T10DIF missed single-bit flip at byte=%d bit=%d", byteIdx, bit)
			}
			sector[byteIdx] ^= 1 << bit
		}
	}
}

func TestDeriveAppTagIsolationAndNoEscape(t *testing.T) {
	seen := make(map[uint16]string)
	for i := 0; i < 64; i++ {
		vol := []byte(fmt.Sprintf("vol-%02d", i))
		ten := []byte(fmt.Sprintf("tenant-%02d", i%8))
		tag := fspi.DeriveAppTag(vol, ten, uint64(i+1))
		if tag == 0 || tag == 0xFFFF {
			t.Fatalf("DeriveAppTag emitted reserved tag 0x%04x", tag)
		}
		seen[tag] = fmt.Sprintf("%s/%s", vol, ten)
	}
	tagA := fspi.DeriveAppTag([]byte("vol-a"), []byte("tenant-a"), 1)
	tagB := fspi.DeriveAppTag([]byte("vol-b"), []byte("tenant-b"), 1)
	if tagA == tagB {
		t.Fatalf("distinct tenants collided on AppTag 0x%04x", tagA)
	}
	// Deterministic check.
	if again := fspi.DeriveAppTag([]byte("vol-a"), []byte("tenant-a"), 1); again != tagA {
		t.Fatalf("DeriveAppTag not deterministic: 0x%04x != 0x%04x", again, tagA)
	}
}

func TestProbeNVMePIRealHostAndFixtures(t *testing.T) {
	// 1. Live host probe must never panic and must return an honest diagnostic.
	live := fspi.ProbeNVMePI()
	if live.SelectedMode == "" || live.Reason == "" {
		t.Fatalf("live ProbeNVMePI returned empty mode/reason: %+v", live)
	}
	if !live.HWPISupported && !live.FallbackEngaged {
		t.Fatalf("when HWPISupported=false, FallbackEngaged must be true: %+v", live)
	}

	// 2. Synthetic sysfs fixture: NVMe T10-DIF-TYPE1-CRC hardware device.
	hwRoot := t.TempDir()
	nvmeInt := filepath.Join(hwRoot, "nvme0n1", "integrity")
	if err := os.MkdirAll(nvmeInt, 0o755); err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		"format":         "T10-DIF-TYPE1-CRC\n",
		"tag_size":       "2\n",
		"tuple_size":     "8\n",
		"write_generate": "1\n",
		"read_verify":    "1\n",
	} {
		if err := os.WriteFile(filepath.Join(nvmeInt, k), []byte(v), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	hwRep := fspi.ProbeNVMePIAt(hwRoot)
	if !hwRep.HWPISupported || hwRep.FallbackEngaged || hwRep.SelectedMode != fspi.GuardT10DIF.String() {
		t.Fatalf("expected HW PI supported on synthetic nvme0n1 fixture, got %+v", hwRep)
	}
	if len(hwRep.Devices) != 1 || hwRep.Devices[0].Format != "T10-DIF-TYPE1-CRC" || hwRep.Devices[0].TupleSize != 8 || !hwRep.Devices[0].ReadVerify {
		t.Fatalf("unexpected device probe details: %+v", hwRep.Devices)
	}

	// 3. Synthetic sysfs fixture: virtualized storage (vda with format=none, loop0 without integrity/).
	virtRoot := t.TempDir()
	vdaInt := filepath.Join(virtRoot, "vda", "integrity")
	if err := os.MkdirAll(vdaInt, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(vdaInt, "format"), []byte("none\n"), 0o644)
	_ = os.MkdirAll(filepath.Join(virtRoot, "loop0"), 0o755)

	virtRep := fspi.ProbeNVMePIAt(virtRoot)
	if virtRep.HWPISupported || !virtRep.FallbackEngaged || virtRep.SelectedMode != fspi.GuardSoftwareStrong.String() {
		t.Fatalf("expected software fallback on virtualized fixture, got %+v", virtRep)
	}
}

func TestPIVerifyExactAttribution(t *testing.T) {
	appA := fspi.DeriveAppTag([]byte("vol-a"), []byte("ten-a"), 1)
	appB := fspi.DeriveAppTag([]byte("vol-b"), []byte("ten-b"), 1)
	payload1 := bytes.Repeat([]byte{0x3C}, fspi.SectorSize4096)
	payload2 := bytes.Repeat([]byte{0xC3}, fspi.SectorSize4096)

	for _, mode := range []fspi.GuardMode{fspi.GuardT10DIF, fspi.GuardSoftwareStrong} {
		t.Run(mode.String(), func(t *testing.T) {
			pi := make([]byte, mode.TupleSize())
			if err := fspi.EncodePI(pi, mode, appA, 7, 2, payload2); err != nil {
				t.Fatal(err)
			}
			// Valid sector verifies cleanly.
			if err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, payload2, pi); err != nil {
				t.Fatalf("valid sector failed: %v", err)
			}

			// 1. Single-bit flip in payload -> ErrGuardMismatch.
			badPayload := append([]byte(nil), payload2...)
			badPayload[100] ^= 0x04
			if err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, badPayload, pi); !errors.Is(err, fspi.ErrGuardMismatch) {
				t.Fatalf("bit flip got %v, want ErrGuardMismatch", err)
			}

			// 2. Misdirected write (valid LBA 9 read at LBA 7) -> ErrRefTagMisdirected.
			piLBA9 := make([]byte, mode.TupleSize())
			_ = fspi.EncodePI(piLBA9, mode, appA, 9, 2, payload2)
			if err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, payload2, piLBA9); !errors.Is(err, fspi.ErrRefTagMisdirected) {
				t.Fatalf("misdirected got %v, want ErrRefTagMisdirected", err)
			}

			// 3. Cross-volume swap (valid AppTag B read under AppTag A) -> ErrAppTagCrossVolume.
			piVolB := make([]byte, mode.TupleSize())
			_ = fspi.EncodePI(piVolB, mode, appB, 7, 2, payload2)
			if err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, payload2, piVolB); !errors.Is(err, fspi.ErrAppTagCrossVolume) {
				t.Fatalf("cross-volume got %v, want ErrAppTagCrossVolume", err)
			}

			// 4. Lost write (intact Epoch 1 replayed when Epoch 2 expected) -> ErrLostWrite.
			piEp1 := make([]byte, mode.TupleSize())
			_ = fspi.EncodePI(piEp1, mode, appA, 7, 1, payload1)
			if err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, payload1, piEp1); !errors.Is(err, fspi.ErrLostWrite) {
				t.Fatalf("lost write got %v, want ErrLostWrite", err)
			}

			// 5. Torn write (first half Epoch 2 + second half Epoch 1 with stale Epoch 1 PI) -> ErrTornWrite.
			torn := append([]byte(nil), payload1...)
			copy(torn[:len(torn)/2], payload2[:len(torn)/2])
			if err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, torn, piEp1); !errors.Is(err, fspi.ErrTornWrite) {
				t.Fatalf("torn write got %v, want ErrTornWrite", err)
			}

			// 6. Zeroed PI tuple -> ErrMetadataTruncated.
			if err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, payload2, make([]byte, mode.TupleSize())); !errors.Is(err, fspi.ErrMetadataTruncated) {
				t.Fatalf("zero PI got %v, want ErrMetadataTruncated", err)
			}

			// 7. Corrupted PI tuple -> fail closed (ErrPICorrupt in Strong mode; rejected in T10-DIF).
			badPI := append([]byte(nil), pi...)
			badPI[0] ^= 0x80
			err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, payload2, badPI)
			if err == nil {
				t.Fatalf("corrupted PI tuple was accepted!")
			}
			if mode == fspi.GuardSoftwareStrong && !errors.Is(err, fspi.ErrPICorrupt) {
				t.Fatalf("strong mode corrupted PI got %v, want ErrPICorrupt", err)
			}

			// 8. T10-DIF reserved escape tag 0xFFFF must fail closed.
			if mode == fspi.GuardT10DIF {
				escPI := append([]byte(nil), pi...)
				binary.BigEndian.PutUint16(escPI[2:4], 0xFFFF)
				if err := fspi.VerifyPI("000001.seg", mode, appA, 7, 2, payload2, escPI); !errors.Is(err, fspi.ErrPICorrupt) {
					t.Fatalf("reserved escape tag 0xFFFF got %v, want ErrPICorrupt", err)
				}
			}
		})
	}
}

func TestFaultMatrixAll8FaultsAcross4Regimes(t *testing.T) {
	summaries, err := fspi.EvaluateFaultMatrix(t.TempDir(), fspi.SectorSize4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 4 {
		t.Fatalf("expected 4 regimes, got %d", len(summaries))
	}

	byName := make(map[string]fspi.RegimeFaultSummary, len(summaries))
	for _, s := range summaries {
		byName[s.Regime] = s
	}

	// Both FS-PI regimes MUST achieve 8/8 (100%) detection strictly BEFORE AEAD decryption,
	// with 0 plaintext exposure and 0 silent corruptions!
	for _, name := range []string{"fspi_t10_dif_hw_compat", "fspi_software_strong_guard"} {
		s := byName[name]
		if s.DetectedCount != 8 || s.PreDecryptRejectedCount != 8 || s.SectorLocalizedCount != 8 {
			t.Fatalf("regime %s failed 8/8 pre-decrypt localization: %+v", name, s)
		}
		if s.PlaintextExposedCount != 0 || s.SilentCorruptionCount != 0 {
			t.Fatalf("regime %s leaked plaintext or allowed silent corruption: %+v", name, s)
		}
		for _, o := range s.Outcomes {
			if o.AEADOpenAttempts != 0 {
				t.Fatalf("regime %s fault %s reached AEAD.Open (%d attempts); must reject before decrypt", name, o.Fault, o.AEADOpenAttempts)
			}
		}
	}

	// Verify exact orthogonal attribution in fspi_software_strong_guard.
	wantStrong := map[fspi.FaultKind]error{
		fspi.FaultSingleBitFlip:     fspi.ErrGuardMismatch,
		fspi.FaultMultiByteBurst:    fspi.ErrGuardMismatch,
		fspi.FaultMisdirectedWrite:  fspi.ErrRefTagMisdirected,
		fspi.FaultTornWrite:         fspi.ErrTornWrite,
		fspi.FaultLostWrite:         fspi.ErrLostWrite,
		fspi.FaultCrossVolumeSwap:   fspi.ErrAppTagCrossVolume,
		fspi.FaultMetadataTruncated: fspi.ErrMetadataTruncated,
		fspi.FaultPICorrupt:         fspi.ErrPICorrupt,
	}
	for _, o := range byName["fspi_software_strong_guard"].Outcomes {
		if want := wantStrong[o.Fault].Error(); o.AttributedError != want {
			t.Fatalf("strong guard fault %s attributed %q, want %q", o.Fault, o.AttributedError, want)
		}
	}

	// Verify honest aead_only_no_pi results: 0 pre-decrypt rejections, silent corruption on lost_write & misdirected_write.
	aead := byName["aead_only_no_pi"]
	if aead.PreDecryptRejectedCount != 0 || aead.SectorLocalizedCount != 0 {
		t.Fatalf("aead_only_no_pi cannot reject pre-decrypt: %+v", aead)
	}
	if aead.SilentCorruptionCount < 2 {
		t.Fatalf("expected aead_only_no_pi to expose silent corruption on misdirected_write & lost_write, got %+v", aead)
	}
}

func TestNegativeRegime1CRC16ForgeryCaughtByAEAD(t *testing.T) {
	res, err := fspi.RunCRC16ForgeryControl(t.TempDir(), fspi.SectorSize4096, 65536, 1344)
	if err != nil {
		t.Fatal(err)
	}
	if !res.ActiveForgeryPIBypassed {
		t.Fatalf("expected unkeyed CRC16-T10-DIF to be bypassed by active forgery: %+v", res)
	}
	if !res.ActiveForgeryAEADCaught {
		t.Fatalf("expected HIP-0302/AEAD to catch forged CRC16 sector: %+v", res)
	}
	if res.ActiveForgeryPlainExposed {
		t.Fatalf("forged sector exposed plaintext!")
	}
	if res.StrongGuardCollisionsFound != 0 {
		t.Fatalf("unexpected collision in 64-bit strong guard: %d", res.StrongGuardCollisionsFound)
	}
	t.Logf("Negative Regime #1: CRC16 active forgery bypassed PI=%v, caught by AEAD=%v; %d collisions in %d random trials (%.6e vs theory %.6e)",
		res.ActiveForgeryPIBypassed, res.ActiveForgeryAEADCaught,
		res.CRC16CollisionsObserved, res.CollisionTrials, res.CRC16EmpiricalRate, res.CRC16TheoreticalRate)
}

func TestProtectedJournalWithHIP0302HybridCipherAndPrePlaintextRejection(t *testing.T) {
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	hybridCipher := vfs.Hybrid(id)

	dir := filepath.Join(t.TempDir(), "jrn")
	epochs := fspi.NewEpochLedger()
	cfg := fspi.JournalConfig{
		VolumeID:    []byte("hip-0302-volume"),
		TenantID:    []byte("tenant-security"),
		Round:       3,
		Capacity:    4 << 20,
		SegmentSize: 1 << 20,
		SectorSize:  fspi.SectorSize4096,
		Mode:        fspi.GuardSoftwareStrong,
		EnableAEAD:  true,
		Cipher:      hybridCipher,
		Epochs:      epochs,
	}

	pj, err := fspi.OpenProtectedJournal(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	wantRecords := [][]byte{
		[]byte("secret-agent-checkpoint-alpha-0001"),
		bytes.Repeat([]byte("classified-hip-0302-record-beta-"), 200),
		[]byte("final-commit-gamma-0003"),
	}
	var lastSeq uint64
	for _, rec := range wantRecords {
		seq, err := pj.Append(ctx, rec)
		if err != nil {
			t.Fatal(err)
		}
		lastSeq = seq
	}
	if err := pj.Sync(ctx, lastSeq); err != nil {
		t.Fatal(err)
	}
	if err := pj.Close(); err != nil {
		t.Fatal(err)
	}
	rootAfterClose := epochs.MerkleRoot()

	// Reopen with same HIP-0302 Hybrid cipher & EpochLedger and replay verified records.
	pj2, err := fspi.OpenProtectedJournal(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	stats, err := pj2.ReplayVerified(func(_ uint64, plain []byte) error {
		got = append(got, append([]byte(nil), plain...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(wantRecords) {
		t.Fatalf("replayed %d records, want %d", len(got), len(wantRecords))
	}
	for i := range wantRecords {
		if !bytes.Equal(got[i], wantRecords[i]) {
			t.Fatalf("record %d mismatch", i)
		}
	}
	if stats.PlaintextExposedCount != uint64(len(wantRecords)) || stats.PIVerifyRejected != 0 {
		t.Fatalf("unexpected clean replay stats: %+v", stats)
	}
	if epochs.MerkleRoot() != rootAfterClose {
		t.Fatalf("epoch Merkle root changed unexpectedly on clean reopen + replay")
	}

	// Now inject a single-bit corruption directly into the underlying .seg file
	// BELOW the encrypted journal layer and verify ReplayVerified rejects
	// strictly BEFORE AEAD decryption (AEADOpenAttempts == 0, PlaintextExposed == 0)!
	segFile := pj2.Disk().OpenFile("000000.seg")
	if segFile == nil {
		t.Fatal("expected active segment 000000.seg to be open")
	}
	dataOff, _ := segFile.PhysicalOffsets(0)
	var b [1]byte
	if _, err := segFile.RawFile().ReadAt(b[:], dataOff+140); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x20
	if _, err := segFile.RawFile().WriteAt(b[:], dataOff+140); err != nil {
		t.Fatal(err)
	}
	_ = segFile.RawFile().Sync()

	var leaked [][]byte
	badStats, badErr := pj2.ReplayVerified(func(_ uint64, plain []byte) error {
		leaked = append(leaked, plain)
		return nil
	})
	_ = pj2.Close()

	if !errors.Is(badErr, fspi.ErrGuardMismatch) {
		t.Fatalf("corrupted journal replay got %v, want ErrGuardMismatch", badErr)
	}
	if badStats.AEADOpenAttempts != 0 || badStats.PlaintextExposedCount != 0 || len(leaked) != 0 {
		t.Fatalf("corrupt record reached AEAD or exposed plaintext! stats=%+v leaked=%d", badStats, len(leaked))
	}
}

func TestLiveVFSVolumeIntegrationWithFSPIDiskFactory(t *testing.T) {
	id, err := age.GenerateHybridIdentity()
	if err != nil {
		t.Fatal(err)
	}
	c := vfs.Hybrid(id)
	mem := storetest.NewMem()
	jDir := filepath.Join(t.TempDir(), "vfs-journal")

	epochs := fspi.NewEpochLedger()
	var captured *fspi.ProtectedDisk
	diskFn := func(dir string) (journal.Disk, error) {
		raw, err := journal.Files(dir)
		if err != nil {
			return nil, err
		}
		d, err := fspi.WrapDisk(raw, fspi.DiskConfig{
			Mode:       fspi.GuardSoftwareStrong,
			SectorSize: fspi.SectorSize4096,
			AppTag:     fspi.DeriveAppTag([]byte("live-vfs-vol"), []byte("tenant-1"), 1),
			Epochs:     epochs,
		})
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		captured = d
		return d, nil
	}

	ctx := context.Background()
	cfg := vfs.Config{
		Backend:         mem,
		Cipher:          c,
		Round:           1,
		Journal:         jDir,
		JournalCapacity: 4 << 20,
		Bound:           5 * time.Second,
		Disk:            diskFn,
	}
	if err := vfs.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	vol, err := vfs.Attach(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ino, err := vol.Create(ctx, vfs.RootInode, "agent-state.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fh, err := vol.Open(ctx, ino.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("hanzo-vfs-fspi-end-to-end-"), 512)
	if _, err := fh.WriteAt(ctx, want, 0); err != nil {
		t.Fatal(err)
	}
	if err := fh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}
	if err := vol.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := vol.Close(); err != nil {
		t.Fatal(err)
	}

	if captured == nil {
		t.Fatal("expected FS-PI ProtectedDisk to be instantiated by vfs.Attach")
	}
	st := captured.Stats()
	if st.SectorsWritten == 0 || st.PIBytesWritten == 0 || st.VerifyFailCount != 0 {
		t.Fatalf("unexpected live Volume FS-PI stats: %+v", st)
	}

	// Reopen read-only Volume and confirm published data matches byte-for-byte.
	ro, err := vfs.Open(ctx, vfs.Config{Backend: mem, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	gotIno, err := ro.Lookup(ctx, vfs.RootInode, "agent-state.bin")
	if err != nil {
		t.Fatal(err)
	}
	rf, err := ro.Open(ctx, gotIno.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	got := make([]byte, len(want))
	if _, err := rf.ReadAt(ctx, got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("readback mismatch from live FS-PI journaled vfs.Volume")
	}
}

func TestProtectedJournalConcurrentReadersWritersRace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "race")
	pj, err := fspi.OpenProtectedJournal(dir, fspi.JournalConfig{
		VolumeID:    []byte("race-vol"),
		TenantID:    []byte("race-tenant"),
		Round:       1,
		Capacity:    8 << 20,
		SegmentSize: 2 << 20,
		SectorSize:  fspi.SectorSize4096,
		Mode:        fspi.GuardSoftwareStrong,
		EnableAEAD:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pj.Close()

	const (
		writers   = 4
		perWriter = 25
		readers   = 2
		totalRecs = writers * perWriter
	)

	ctx := context.Background()
	var wg sync.WaitGroup
	errCh := make(chan error, writers+readers)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				msg := []byte(fmt.Sprintf("writer=%d item=%03d payload=%s", id, i, bytes.Repeat([]byte("Z"), 256)))
				seq, err := pj.Append(ctx, msg)
				if err != nil {
					errCh <- err
					return
				}
				if i%5 == 0 {
					if err := pj.Sync(ctx, seq); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				if _, err := pj.ReplayVerified(nil); err != nil {
					errCh <- err
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent error: %v", err)
	}

	var count int
	stats, err := pj.ReplayVerified(func(_ uint64, plain []byte) error {
		if len(plain) == 0 {
			return errors.New("empty plaintext")
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != totalRecs {
		t.Fatalf("replayed %d records, want %d", count, totalRecs)
	}
	if stats.PIVerifyRejected != 0 || stats.AEADOpenFailures != 0 {
		t.Fatalf("unexpected verification failures under race test: %+v", stats)
	}
}

func TestEpochLedgerMerkleRootAndSpaceOverheadBounds(t *testing.T) {
	el := fspi.NewEpochLedger()
	r0 := el.MerkleRoot()
	el.Set("000001.seg", 0, 1)
	r1 := el.MerkleRoot()
	el.Set("000001.seg", 0, 2)
	r2 := el.MerkleRoot()
	if r0 == r1 || r1 == r2 || r0 == r2 {
		t.Fatalf("EpochLedger MerkleRoot did not change across epoch transitions")
	}
	if cp := el.Clone(); cp.MerkleRoot() != r2 {
		t.Fatalf("cloned EpochLedger MerkleRoot mismatch")
	}

	// Verify software-only interleaved space overhead bounds (+0.2% to +3.1%).
	rep, err := fspi.RunFullBenchmark(fspi.QuickBenchConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.SpaceOverhead) != 4 {
		t.Fatalf("expected 4 space overhead rows, got %d", len(rep.SpaceOverhead))
	}
	for _, row := range rep.SpaceOverhead {
		if row.PerSectorPct < 0.19 || row.PerSectorPct > 3.13 {
			t.Fatalf("space overhead out of expected [0.19%%, 3.13%%] band: %+v", row)
		}
	}
}

func TestMultiTenantAppTagIsolationOnRealJournal(t *testing.T) {
	base := t.TempDir()
	dirA := filepath.Join(base, "tenantA")
	dirB := filepath.Join(base, "tenantB")

	cfgA := fspi.JournalConfig{
		VolumeID:    []byte("shared-vol-name"),
		TenantID:    []byte("tenant-alpha"),
		Round:       1,
		Capacity:    4 << 20,
		SegmentSize: 1 << 20,
		SectorSize:  fspi.SectorSize4096,
		Mode:        fspi.GuardSoftwareStrong,
		EnableAEAD:  true,
	}
	cfgB := cfgA
	cfgB.TenantID = []byte("tenant-beta")

	ctx := context.Background()
	pjA, err := fspi.OpenProtectedJournal(dirA, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	seqA, err := pjA.Append(ctx, []byte("tenant-alpha-secret"))
	if err != nil {
		t.Fatal(err)
	}
	_ = pjA.Sync(ctx, seqA)
	_ = pjA.Close()

	pjB, err := fspi.OpenProtectedJournal(dirB, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	seqB, err := pjB.Append(ctx, []byte("tenant-beta-secret"))
	if err != nil {
		t.Fatal(err)
	}
	_ = pjB.Sync(ctx, seqB)
	_ = pjB.Close()

	// Substitute Tenant B's valid active segment file into Tenant A's journal directory.
	segBBytes, err := os.ReadFile(filepath.Join(dirB, "000000.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirA, "000000.seg"), segBBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	// Reopening Tenant A MUST fail closed at the FS-PI AppTag boundary before
	// journal replay or AEAD decryption!
	pjReopen, openErr := fspi.OpenProtectedJournal(dirA, cfgA)
	if openErr == nil {
		_ = pjReopen.Close()
		t.Fatal("expected OpenProtectedJournal to reject Tenant B segment substituted into Tenant A")
	}
	if !errors.Is(openErr, fspi.ErrAppTagCrossVolume) {
		t.Fatalf("cross-tenant segment swap got %v, want ErrAppTagCrossVolume", openErr)
	}
}
