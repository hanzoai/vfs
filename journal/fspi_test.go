package journal_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

func testKey32(seed byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = seed ^ byte(i*17+3)
	}
	return k
}

func makeTestPayload(tag string, lba uint32, gen int) []byte {
	buf := make([]byte, 3840)
	hdr := fmt.Sprintf("tenant=%s lba=%d gen=%d ", tag, lba, gen)
	copy(buf, hdr)
	for i := len(hdr); i < len(buf); i++ {
		buf[i] = byte((i*31 + int(lba)*7 + gen*13) & 0xff)
	}
	return buf
}

// TestFSPIAllSevenFaultClasses exercises all 7 below-encryption storage
// corruption classes directly on raw on-disk journal bytes beneath the
// HIP-0302 AES-256-GCM cipher envelope across ModeNone, ModeAEADOnlyBaseline,
// ModeSoftwareCRC32C, and ModeKeyedBLAKE3PI.
func TestFSPIAllSevenFaultClasses(t *testing.T) {
	modes := []struct {
		mode              journal.FSPIMode
		wantDetectedCount int
		wantPreDecrypt    int
	}{
		{mode: journal.ModeNone, wantDetectedCount: 0, wantPreDecrypt: 0},
		// Volume-scoped AEAD catches bitrot, burst, cross-tenant (distinct keys),
		// zeroed sector, and torn tail inside AEAD.Open, but CANNOT detect
		// same-volume LBA block swap or stale generation replay, and performs 0
		// pre-decryption rejections at the storage/PI layer.
		{mode: journal.ModeAEADOnlyBaseline, wantDetectedCount: 5, wantPreDecrypt: 0},
		{mode: journal.ModeSoftwareCRC32C, wantDetectedCount: 7, wantPreDecrypt: 7},
		{mode: journal.ModeKeyedBLAKE3PI, wantDetectedCount: 7, wantPreDecrypt: 7},
	}

	for _, tc := range modes {
		t.Run(tc.mode.String(), func(t *testing.T) {
			detected := 0
			preDecrypt := 0

			for _, fault := range journal.AllFSPIFaultClasses {
				gotDetected, gotPreDecrypt, gotErr := runSingleFaultExperiment(t, tc.mode, fault)
				if gotDetected {
					detected++
				}
				if gotPreDecrypt {
					preDecrypt++
				}
				// Verify exact error semantics for FS-PI modes.
				if tc.mode == journal.ModeSoftwareCRC32C || tc.mode == journal.ModeKeyedBLAKE3PI {
					if !gotDetected || !gotPreDecrypt {
						t.Errorf("mode=%s fault=%s: detected=%v preDecrypt=%v err=%v; want both true",
							tc.mode, fault, gotDetected, gotPreDecrypt, gotErr)
					}
					if !errors.Is(gotErr, journal.ErrCorrupt) || !errors.Is(gotErr, journal.ErrFSPIIntegrity) {
						t.Errorf("mode=%s fault=%s: err=%v does not wrap ErrCorrupt and ErrFSPIIntegrity", tc.mode, fault, gotErr)
					}
					wantSentinel := expectedFaultSentinel(fault)
					if wantSentinel != nil && !errors.Is(gotErr, wantSentinel) {
						t.Errorf("mode=%s fault=%s: err=%v does not wrap expected sentinel %v", tc.mode, fault, gotErr, wantSentinel)
					}
				}
			}

			if detected != tc.wantDetectedCount {
				t.Errorf("mode=%s: detected %d/7 faults, want %d/7", tc.mode, detected, tc.wantDetectedCount)
			}
			if preDecrypt != tc.wantPreDecrypt {
				t.Errorf("mode=%s: pre-decrypt rejected %d/7 faults, want %d/7", tc.mode, preDecrypt, tc.wantPreDecrypt)
			}
		})
	}
}

func expectedFaultSentinel(fault journal.FSPIFaultClass) error {
	switch fault {
	case journal.FaultSingleBitFlip, journal.FaultMultiByteBurst:
		return journal.ErrFSPIGuardMismatch
	case journal.FaultMisdirectedBlockSwap:
		return journal.ErrFSPIRefTagMismatch
	case journal.FaultStaleLostWriteReplay:
		return journal.ErrFSPIStaleReplay
	case journal.FaultCrossTenantTransplant:
		return journal.ErrFSPIAppTagMismatch
	case journal.FaultZeroedSectorPhantom:
		return journal.ErrFSPIZeroedSector
	case journal.FaultTruncatedTornTail:
		return journal.ErrFSPITornSector
	default:
		return nil
	}
}

func runSingleFaultExperiment(t *testing.T, mode journal.FSPIMode, fault journal.FSPIFaultClass) (detected bool, preDecrypt bool, readErr error) {
	t.Helper()

	dirA := filepath.Join(t.TempDir(), "tenantA")
	diskRawA, err := journal.Files(dirA)
	if err != nil {
		t.Fatalf("Files(dirA): %v", err)
	}
	defer diskRawA.Close()

	var diskA journal.Disk = diskRawA
	if mode == journal.ModeSoftwareCRC32C || mode == journal.ModeKeyedBLAKE3PI {
		cfgA := &journal.FSPIConfig{
			Mode:     mode,
			TenantID: "tenant-alpha",
			VolumeID: "vol-001",
			Key:      testKey32(0x11),
		}
		fspiA, err := journal.NewFSPIDisk(diskRawA, cfgA)
		if err != nil {
			t.Fatalf("NewFSPIDisk A: %v", err)
		}
		diskA = fspiA
	}

	const segSize = 64 * 1024 // 16 sectors of 4 KiB
	fA, err := diskA.Create("000000.seg", segSize)
	if err != nil {
		t.Fatalf("Create A: %v", err)
	}
	defer fA.Close()

	storeA, err := journal.NewHIP0302SectorStore(fA, mode, testKey32(0xA1), "tenant-alpha", "vol-001")
	if err != nil {
		t.Fatalf("NewHIP0302SectorStore A: %v", err)
	}

	// Write initial committed sectors LBA 0..3 (gen g-1).
	for lba := uint32(0); lba < 4; lba++ {
		if err := storeA.WriteSector(lba, makeTestPayload("alpha", lba, 1)); err != nil {
			t.Fatalf("WriteSector lba=%d gen=1: %v", lba, err)
		}
	}
	if err := storeA.Sync(); err != nil {
		t.Fatalf("Sync gen=1: %v", err)
	}

	// Capture authentic gen g-1 snapshot of LBA 2 BEFORE overwriting with gen g.
	snapLBA2Gen1, err := journal.SnapshotPhysicalSector(fA, 2)
	if err != nil {
		t.Fatalf("SnapshotPhysicalSector: %v", err)
	}

	// Overwrite LBA 2 with newer generation g.
	wantLBA2 := makeTestPayload("alpha", 2, 2)
	if err := storeA.WriteSector(2, wantLBA2); err != nil {
		t.Fatalf("WriteSector lba=2 gen=2: %v", err)
	}
	if err := storeA.Sync(); err != nil {
		t.Fatalf("Sync gen=2: %v", err)
	}

	// Inject the requested below-encryption fault directly onto raw on-disk bytes.
	targetLBA := uint32(2)
	switch fault {
	case journal.FaultSingleBitFlip:
		err = journal.InjectSingleBitFlip(fA, targetLBA, 128, 3)
	case journal.FaultMultiByteBurst:
		err = journal.InjectMultiByteBurst(fA, targetLBA, 256, 64)
	case journal.FaultMisdirectedBlockSwap:
		err = journal.InjectMisdirectedBlockSwap(fA, 1, 2)
	case journal.FaultStaleLostWriteReplay:
		err = journal.InjectStaleLostWriteReplay(fA, targetLBA, snapLBA2Gen1)
	case journal.FaultCrossTenantTransplant:
		dirB := filepath.Join(t.TempDir(), "tenantB")
		diskRawB, derr := journal.Files(dirB)
		if derr != nil {
			t.Fatalf("Files(dirB): %v", derr)
		}
		defer diskRawB.Close()
		var diskB journal.Disk = diskRawB
		if mode == journal.ModeSoftwareCRC32C || mode == journal.ModeKeyedBLAKE3PI {
			cfgB := &journal.FSPIConfig{
				Mode:     mode,
				TenantID: "tenant-beta",
				VolumeID: "vol-999",
				Key:      testKey32(0x22),
			}
			fspiB, ferr := journal.NewFSPIDisk(diskRawB, cfgB)
			if ferr != nil {
				t.Fatalf("NewFSPIDisk B: %v", ferr)
			}
			diskB = fspiB
		}
		fB, cerr := diskB.Create("000000.seg", segSize)
		if cerr != nil {
			t.Fatalf("Create B: %v", cerr)
		}
		defer fB.Close()
		storeB, serr := journal.NewHIP0302SectorStore(fB, mode, testKey32(0xB2), "tenant-beta", "vol-999")
		if serr != nil {
			t.Fatalf("NewHIP0302SectorStore B: %v", serr)
		}
		if werr := storeB.WriteSector(targetLBA, makeTestPayload("beta", targetLBA, 1)); werr != nil {
			t.Fatalf("WriteSector B: %v", werr)
		}
		if werr := storeB.WriteSector(targetLBA, makeTestPayload("beta", targetLBA, 2)); werr != nil {
			t.Fatalf("WriteSector B gen2: %v", werr)
		}
		if serr := storeB.Sync(); serr != nil {
			t.Fatalf("Sync B: %v", serr)
		}
		err = journal.InjectCrossTenantTransplant(fA, targetLBA, fB, targetLBA)
	case journal.FaultZeroedSectorPhantom:
		err = journal.InjectZeroedSectorPhantom(fA, targetLBA)
	case journal.FaultTruncatedTornTail:
		err = journal.InjectTruncatedTornTail(fA, targetLBA, journal.FSPISectorSize/2)
	default:
		t.Fatalf("unknown fault %s", fault)
	}
	if err != nil {
		t.Fatalf("inject %s: %v", fault, err)
	}

	// Measure DecryptInvocations BEFORE reading the corrupted sector.
	decBefore, preBefore, _, _ := storeA.Metrics()
	got, rerr := storeA.ReadSector(targetLBA)
	decAfter, preAfter, _, _ := storeA.Metrics()

	if rerr != nil {
		detected = true
		readErr = rerr
		// Pre-plaintext-exposure rejection holds strictly when ReadSector
		// rejected the sector WITHOUT invoking AEAD decryption at all.
		if preAfter > preBefore && decAfter == decBefore {
			preDecrypt = true
		}
		return detected, preDecrypt, readErr
	}

	// No error returned — verify that silent corruption actually occurred!
	if bytes.Equal(got, wantLBA2) {
		t.Fatalf("mode=%s fault=%s: fault had no effect on read payload", mode, fault)
	}
	return false, false, nil
}

// TestFSPIHonestNegativeCRCActiveAdversaryForgery proves Honest Negative Regime #1:
// an active storage adversary who alters ciphertext bytes AND recomputes the
// unkeyed Castagnoli CRC32C GuardTag bypasses ModeSoftwareCRC32C at the PI layer,
// whereas ModeKeyedBLAKE3PI rejects the forged sector BEFORE decryption.
func TestFSPIHonestNegativeCRCActiveAdversaryForgery(t *testing.T) {
	// 1. ModeSoftwareCRC32C: active adversary forges unkeyed CRC32C -> bypasses PI layer!
	t.Run("crc32c_forged_bypasses_pi_layer", func(t *testing.T) {
		rawDisk, err := journal.Files(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer rawDisk.Close()

		fspiDisk, err := journal.NewFSPIDisk(rawDisk, &journal.FSPIConfig{
			Mode:     journal.ModeSoftwareCRC32C,
			TenantID: "tenant-alpha",
			VolumeID: "vol-001",
		})
		if err != nil {
			t.Fatal(err)
		}
		f, err := fspiDisk.Create("000000.seg", 32*1024)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		store, err := journal.NewHIP0302SectorStore(f, journal.ModeSoftwareCRC32C, testKey32(0x41), "tenant-alpha", "vol-001")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.WriteSector(1, makeTestPayload("alpha", 1, 1)); err != nil {
			t.Fatal(err)
		}
		if err := store.Sync(); err != nil {
			t.Fatal(err)
		}

		// Active adversary alters ciphertext AND recomputes unkeyed CRC32C PI tuple.
		if err := journal.InjectActiveAdversaryCRCForgery(f, 1, 100); err != nil {
			t.Fatal(err)
		}

		// Raw FSPIFile.ReadAt SUCCEEDS because unkeyed CRC32C was forged!
		var rawSector [journal.FSPISectorSize]byte
		if _, err := f.ReadAt(rawSector[:], int64(1)*journal.FSPISectorSize); err != nil {
			t.Fatalf("expected forged unkeyed CRC32C to bypass ModeSoftwareCRC32C PI layer, got err: %v", err)
		}

		// When read through HIP0302SectorStore, the tampered ciphertext leaks
		// into AEAD.Open (DecryptInvocations increments) and is only caught by AEAD,
		// NOT pre-decryption by FS-PI.
		decBefore, preBefore, aeadBefore, _ := store.Metrics()
		_, err = store.ReadSector(1)
		decAfter, preAfter, aeadAfter, _ := store.Metrics()
		if err == nil {
			t.Fatal("expected HIP-0302 AEAD to catch tampered ciphertext after CRC32C PI bypass")
		}
		if preAfter != preBefore {
			t.Fatalf("expected 0 PI pre-decrypt rejections under forged CRC32C, got %d", preAfter-preBefore)
		}
		if decAfter != decBefore+1 || aeadAfter != aeadBefore+1 {
			t.Fatalf("expected forged CRC32C to leak into AEAD decryptor: decDelta=%d aeadDelta=%d", decAfter-decBefore, aeadAfter-aeadBefore)
		}
	})

	// 2. ModeKeyedBLAKE3PI: exact same active adversary forgery is REJECTED at PI layer BEFORE decryption!
	t.Run("keyed_blake3_pi_rejects_forgery_pre_decrypt", func(t *testing.T) {
		rawDisk, err := journal.Files(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer rawDisk.Close()

		fspiDisk, err := journal.NewFSPIDisk(rawDisk, &journal.FSPIConfig{
			Mode:     journal.ModeKeyedBLAKE3PI,
			TenantID: "tenant-alpha",
			VolumeID: "vol-001",
			Key:      testKey32(0x55),
		})
		if err != nil {
			t.Fatal(err)
		}
		f, err := fspiDisk.Create("000000.seg", 32*1024)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		store, err := journal.NewHIP0302SectorStore(f, journal.ModeKeyedBLAKE3PI, testKey32(0x41), "tenant-alpha", "vol-001")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.WriteSector(1, makeTestPayload("alpha", 1, 1)); err != nil {
			t.Fatal(err)
		}
		if err := store.Sync(); err != nil {
			t.Fatal(err)
		}

		// Exact same active adversary forgery attempt.
		if err := journal.InjectActiveAdversaryCRCForgery(f, 1, 100); err != nil {
			t.Fatal(err)
		}

		decBefore, preBefore, _, _ := store.Metrics()
		_, err = store.ReadSector(1)
		decAfter, preAfter, _, _ := store.Metrics()
		if !errors.Is(err, journal.ErrFSPIGuardMismatch) || !errors.Is(err, journal.ErrCorrupt) {
			t.Fatalf("expected ErrFSPIGuardMismatch wrapping ErrCorrupt, got: %v", err)
		}
		if decAfter != decBefore {
			t.Fatalf("keyed BLAKE3 PI leaked forged sector to AEAD decryptor: decDelta=%d", decAfter-decBefore)
		}
		if preAfter != preBefore+1 {
			t.Fatalf("expected 1 pre-decrypt PI rejection, got %d", preAfter-preBefore)
		}
	})
}

// TestFSPIHonestNegativeHardwareNVMePIProbe verifies Honest Negative Regime #2:
// probing Linux /sys/block/*/integrity honestly reports hardware PI status on
// this host and falls back cleanly to software validation when hardware PI is
// unavailable on virtualized/cloud disks.
func TestFSPIHonestNegativeHardwareNVMePIProbe(t *testing.T) {
	dir := t.TempDir()
	probe := journal.ProbeNVMePI(dir)
	if !probe.Probed {
		t.Fatal("expected ProbeNVMePI to mark Probed=true")
	}
	if probe.Reason == "" {
		t.Fatal("expected non-empty Reason from ProbeNVMePI")
	}
	t.Logf("NVMe PI hardware probe: supported=%v device=%q devices=%v reason=%q",
		probe.Supported, probe.Device, probe.CheckedDevices, probe.Reason)

	j, err := journal.Open(dir, journal.Options{
		Capacity: 4 * (64 << 10),
		Segment:  64 << 10,
		Lineage:  []byte("hw-probe-lineage"),
		Integrity: &journal.FSPIConfig{
			Mode:         journal.ModeHardwarePINVMeProbe,
			FallbackMode: journal.ModeKeyedBLAKE3PI,
			TenantID:     "tenant-probe",
			VolumeID:     "vol-probe",
			Key:          testKey32(0x77),
		},
	})
	if err != nil {
		t.Fatalf("Open with ModeHardwarePINVMeProbe: %v", err)
	}
	defer closeWithin(t, j)

	seq := mustAppend(t, j, []byte("probe-fallback-record"))
	mustSync(t, j, seq)

	st := j.FSPIStats()
	if st.RequestedMode != journal.ModeHardwarePINVMeProbe {
		t.Fatalf("RequestedMode=%v want %v", st.RequestedMode, journal.ModeHardwarePINVMeProbe)
	}
	if st.EffectiveMode != journal.ModeKeyedBLAKE3PI {
		t.Fatalf("EffectiveMode=%v want %v", st.EffectiveMode, journal.ModeKeyedBLAKE3PI)
	}
	if !st.HardwareProbe.Probed {
		t.Fatal("expected HardwareProbe.Probed=true in FSPIStats")
	}
	if st.SectorsWritten == 0 {
		t.Fatal("expected non-zero SectorsWritten in FSPIStats")
	}
}

// TestFSPIJournalIntegrationAndCrashDiskReopen validates full Journal lifecycle
// (Append, Sync, Replay, Publish, Reclaim, Close, Reopen, Discard) over both
// crash.Disk and real OS files with FS-PI enabled, plus format/tenant safety
// and below-encryption corruption rejection during Open/Replay.
func TestFSPIJournalIntegrationAndCrashDiskReopen(t *testing.T) {
	for _, mode := range []journal.FSPIMode{journal.ModeSoftwareCRC32C, journal.ModeKeyedBLAKE3PI} {
		t.Run("crash_disk_"+mode.String(), func(t *testing.T) {
			d := crash.New()
			cfg := &journal.FSPIConfig{
				Mode:     mode,
				TenantID: "tenant-crash",
				VolumeID: "vol-crash-1",
				Key:      testKey32(0x33),
			}
			opt := small(d, ticking{newClock(), 2e6}, 0)
			opt.Integrity = cfg

			j := open(t, opt)
			var want []entry
			var last uint64
			for i := 1; i <= 45; i++ {
				p := payload(uint64(i))
				last = mustAppend(t, j, p)
				want = append(want, entry{last, string(p)})
				if i%9 == 0 {
					mustSync(t, j, last)
				}
			}
			mustSync(t, j, last)

			if got := replayed(t, j); len(got) != len(want) {
				t.Fatalf("replay before close: got %d want %d", len(got), len(want))
			}

			publish(t, j, 20)
			if err := j.Reclaim(); err != nil {
				t.Fatalf("Reclaim: %v", err)
			}
			if err := closeBounded(t, j); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Reopen with same FS-PI config and verify remaining records replay cleanly.
			opt2 := small(d, newClock(), 20)
			opt2.Integrity = cfg
			j2 := open(t, opt2)
			got := replayed(t, j2)
			if len(got) != len(want)-20 {
				t.Fatalf("replay after reopen: got %d want %d", len(got), len(want)-20)
			}
			for i, e := range got {
				if e != want[20+i] {
					t.Fatalf("record %d mismatch: got %+v want %+v", i, e, want[20+i])
				}
			}
			st := j2.FSPIStats()
			if st.SectorsVerified == 0 || st.ValidationFailures != 0 {
				t.Fatalf("unexpected FSPIStats after clean reopen: %+v", st)
			}
			if err := closeBounded(t, j2); err != nil {
				t.Fatalf("Close j2: %v", err)
			}

			// Reopening the same crash.Disk under a DIFFERENT tenant must fail closed!
			wrongTenant := small(d, newClock(), 20)
			wrongTenant.Integrity = &journal.FSPIConfig{
				Mode:     mode,
				TenantID: "tenant-intruder",
				VolumeID: "vol-crash-1",
				Key:      testKey32(0x33),
			}
			if _, err := journal.Open("", wrongTenant); !errors.Is(err, journal.ErrFSPIAppTagMismatch) {
				t.Fatalf("expected ErrFSPIAppTagMismatch for wrong tenant, got: %v", err)
			}

			// Inject a 1-bit below-encryption fault into a live raw segment on crash.Disk
			// and verify journal.Open fails closed with ErrCorrupt + ErrFSPIIntegrity!
			names, err := d.List()
			if err != nil {
				t.Fatal(err)
			}
			corruptedOne := false
			for _, name := range names {
				rawF, oerr := d.Open(name)
				if oerr != nil {
					t.Fatal(oerr)
				}
				// Flip 1 bit inside sector 0's logical payload area (physical offset 64 + 128).
				var b [1]byte
				if _, rerr := rawF.ReadAt(b[:], 64+128); rerr == nil {
					b[0] ^= 0x04
					if _, werr := rawF.WriteAt(b[:], 64+128); werr != nil {
						t.Fatal(werr)
					}
					corruptedOne = true
					rawF.Close()
					break
				}
				rawF.Close()
			}
			if !corruptedOne {
				t.Fatal("failed to corrupt any segment file")
			}

			optCorrupt := small(d, newClock(), 20)
			optCorrupt.Integrity = cfg
			if _, err := journal.Open("", optCorrupt); !errors.Is(err, journal.ErrCorrupt) || !errors.Is(err, journal.ErrFSPIIntegrity) {
				t.Fatalf("expected ErrCorrupt+ErrFSPIIntegrity after raw bitflip, got: %v", err)
			}
		})
	}

	// Format compatibility: opening a plaintext (non-FSPI) journal directory
	// with FS-PI enabled must fail with ErrFormat and touch nothing.
	t.Run("format_compatibility_plaintext_refused", func(t *testing.T) {
		dir := t.TempDir()
		j, err := journal.Open(dir, journal.Options{Capacity: 4 * (64 << 10), Segment: 64 << 10})
		if err != nil {
			t.Fatal(err)
		}
		seq := mustAppend(t, j, []byte("plaintext-record"))
		mustSync(t, j, seq)
		if err := closeBounded(t, j); err != nil {
			t.Fatal(err)
		}

		_, err = journal.Open(dir, journal.Options{
			Capacity: 4 * (64 << 10),
			Segment:  64 << 10,
			Integrity: &journal.FSPIConfig{
				Mode:     journal.ModeKeyedBLAKE3PI,
				TenantID: "tenant-a",
				VolumeID: "vol-a",
				Key:      testKey32(0x99),
			},
		})
		if !errors.Is(err, journal.ErrFormat) {
			t.Fatalf("expected ErrFormat when opening plaintext journal with FS-PI, got: %v", err)
		}
	})

	// Invalid config: ModeKeyedBLAKE3PI with empty Key must fail closed.
	t.Run("keyed_mode_requires_key", func(t *testing.T) {
		_, err := journal.Open(t.TempDir(), journal.Options{
			Capacity: 4 * (64 << 10),
			Segment:  64 << 10,
			Integrity: &journal.FSPIConfig{
				Mode:     journal.ModeKeyedBLAKE3PI,
				TenantID: "tenant-a",
				VolumeID: "vol-a",
			},
		})
		if !errors.Is(err, journal.ErrFSPIInvalidConfig) {
			t.Fatalf("expected ErrFSPIInvalidConfig for empty key, got: %v", err)
		}
	})
}

// TestFSPIConcurrentMultiTenantIsolation runs concurrent writes, syncs, replays,
// and cross-tenant block transplant attacks across multiple tenants under -race.
func TestFSPIConcurrentMultiTenantIsolation(t *testing.T) {
	tenants := []struct {
		id  string
		vol string
		key []byte
	}{
		{id: "tenant-alpha", vol: "vol-a", key: testKey32(0x10)},
		{id: "tenant-beta", vol: "vol-b", key: testKey32(0x20)},
		{id: "tenant-gamma", vol: "vol-c", key: testKey32(0x30)},
	}

	journals := make([]*journal.Journal, len(tenants))
	dirs := make([]string, len(tenants))
	for i, tn := range tenants {
		dirs[i] = filepath.Join(t.TempDir(), tn.id)
		j, err := journal.Open(dirs[i], journal.Options{
			Capacity: 8 * (64 << 10),
			Segment:  64 << 10,
			Lineage:  []byte(tn.id + "/" + tn.vol),
			Integrity: &journal.FSPIConfig{
				Mode:     journal.ModeKeyedBLAKE3PI,
				TenantID: tn.id,
				VolumeID: tn.vol,
				Key:      tn.key,
			},
		})
		if err != nil {
			t.Fatalf("Open %s: %v", tn.id, err)
		}
		journals[i] = j
	}

	var wg sync.WaitGroup
	const workersPerTenant = 4
	const recordsPerWorker = 25

	for ti, tn := range tenants {
		j := journals[ti]
		for w := 0; w < workersPerTenant; w++ {
			wg.Add(1)
			go func(tenantID string, worker int) {
				defer wg.Done()
				ctx := context.Background()
				for r := 0; r < recordsPerWorker; r++ {
					msg := fmt.Sprintf("%s:w%d:r%d", tenantID, worker, r)
					seq, err := j.Append(ctx, []byte(msg))
					if err != nil {
						t.Errorf("%s append failed: %v", tenantID, err)
						return
					}
					if r%5 == 0 {
						if err := j.Sync(ctx, seq); err != nil {
							t.Errorf("%s sync failed: %v", tenantID, err)
							return
						}
					}
				}
			}(tn.id, w)
		}
		// Concurrent reader running Replay while writers are active.
		wg.Add(1)
		go func(tenantID string) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if err := j.Replay(func(rec journal.Record) error {
					if !bytes.HasPrefix(rec.Data, []byte(tenantID+":")) {
						return fmt.Errorf("cross-tenant leak: %s saw %q", tenantID, string(rec.Data))
					}
					return nil
				}); err != nil {
					t.Errorf("%s replay failed: %v", tenantID, err)
					return
				}
			}
		}(tn.id)
	}
	wg.Wait()

	for ti, tn := range tenants {
		j := journals[ti]
		marks := j.Marks()
		if marks.Visible != workersPerTenant*recordsPerWorker {
			t.Fatalf("%s visible=%d want %d", tn.id, marks.Visible, workersPerTenant*recordsPerWorker)
		}
		mustSync(t, j, marks.Visible)
		st := j.FSPIStats()
		if st.ValidationFailures != 0 {
			t.Fatalf("%s unexpected validation failures: %+v", tn.id, st)
		}
		if err := closeBounded(t, j); err != nil {
			t.Fatalf("close %s: %v", tn.id, err)
		}
	}

	// Cross-tenant segment transplant test: copy a live segment from tenant-beta
	// over tenant-alpha's segment on disk and verify tenant-alpha rejects it!
	srcSeg := filepath.Join(dirs[1], "000000.seg")
	dstSeg := filepath.Join(dirs[0], "000000.seg")
	betaBytes, err := os.ReadFile(srcSeg)
	if err != nil {
		t.Fatalf("read beta seg: %v", err)
	}
	if err := os.WriteFile(dstSeg, betaBytes, 0o600); err != nil {
		t.Fatalf("overwrite alpha seg with beta seg: %v", err)
	}

	_, err = journal.Open(dirs[0], journal.Options{
		Capacity: 8 * (64 << 10),
		Segment:  64 << 10,
		Lineage:  []byte(tenants[0].id + "/" + tenants[0].vol),
		Integrity: &journal.FSPIConfig{
			Mode:     journal.ModeKeyedBLAKE3PI,
			TenantID: tenants[0].id,
			VolumeID: tenants[0].vol,
			Key:      tenants[0].key,
		},
	})
	if !errors.Is(err, journal.ErrFSPIAppTagMismatch) || !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("expected ErrFSPIAppTagMismatch+ErrCorrupt on cross-tenant segment transplant, got: %v", err)
	}
}

// TestFSPIPartialWriteRMWAndUnalignedIO verifies unaligned multi-sector writes,
// reads, monotonic generation counters, and persistent stale-replay detection
// across Close + Reopen.
func TestFSPIPartialWriteRMWAndUnalignedIO(t *testing.T) {
	dir := t.TempDir()
	rawDisk, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer rawDisk.Close()

	cfg := &journal.FSPIConfig{
		Mode:     journal.ModeKeyedBLAKE3PI,
		TenantID: "tenant-rmw",
		VolumeID: "vol-rmw",
		Key:      testKey32(0x88),
	}
	fspiDisk, err := journal.NewFSPIDisk(rawDisk, cfg)
	if err != nil {
		t.Fatal(err)
	}

	const segSize = 64 * 1024
	f, err := fspiDisk.Create("000000.seg", segSize)
	if err != nil {
		t.Fatal(err)
	}
	ff := f.(*journal.FSPIFile)

	// Unaligned write spanning sectors 0, 1, and 2 (offset 4000, length 5000).
	data1 := make([]byte, 5000)
	if _, err := rand.Read(data1); err != nil {
		t.Fatal(err)
	}
	if n, err := f.WriteAt(data1, 4000); err != nil || n != len(data1) {
		t.Fatalf("WriteAt data1: n=%d err=%v", n, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if g := ff.SectorGeneration(1); g != 2 {
		t.Fatalf("sector 1 gen=%d want 2", g)
	}

	// Capture sector 1 at gen=2 BEFORE overwriting.
	snapGen2, err := journal.SnapshotPhysicalSector(f, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Overwrite part of sector 1 (gen=3).
	patch := bytes.Repeat([]byte{0xEE}, 512)
	if _, err := f.WriteAt(patch, 4096+100); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if g := ff.SectorGeneration(1); g != 3 {
		t.Fatalf("sector 1 gen=%d want 3", g)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify exact unaligned bytes + generation persisted across Close/Open.
	f2, err := fspiDisk.Open("000000.seg")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer f2.Close()

	got := make([]byte, 5000)
	if _, err := f2.ReadAt(got, 4000); err != nil {
		t.Fatalf("ReadAt after reopen: %v", err)
	}
	want := append([]byte(nil), data1...)
	copy(want[4096+100-4000:], patch)
	if !bytes.Equal(got, want) {
		t.Fatal("unaligned read mismatch after reopen")
	}

	// Now replay the gen=2 snapshot onto sector 1 after reopen -> MUST be caught as ErrFSPIStaleReplay!
	if err := journal.InjectStaleLostWriteReplay(f2, 1, snapGen2); err != nil {
		t.Fatal(err)
	}
	var sec [journal.FSPISectorSize]byte
	if _, err := f2.ReadAt(sec[:], 4096); !errors.Is(err, journal.ErrFSPIStaleReplay) {
		t.Fatalf("expected ErrFSPIStaleReplay after reopen, got: %v", err)
	}
}

// TestFSPIT10DIFCRC16AndTruncatedOSFile validates the T10-DIF CRC-16 helper
// and real OS file truncation detection (ErrFSPITornSector).
func TestFSPIT10DIFCRC16AndTruncatedOSFile(t *testing.T) {
	// Verify T10-DIF CRC-16 detects single-bit changes and matches standard
	// check value over "123456789" (0xD0DB for CRC-16/T10-DIF poly 0x8BB7).
	const wantCheck = uint16(0xD0DB)
	if got := journal.CRC16T10DIF([]byte("123456789")); got != wantCheck {
		t.Fatalf("CRC16T10DIF(\"123456789\") = 0x%04x, want 0x%04x", got, wantCheck)
	}

	sector := make([]byte, journal.FSPISectorSize)
	copy(sector, "nvme-t10-dif-test-sector")
	tup := journal.ComputeT10DIFTuple(sector, 0x1234, 42)
	raw := tup.MarshalBinary()
	dec, err := journal.UnmarshalT10DIF(raw[:])
	if err != nil {
		t.Fatalf("UnmarshalT10DIF: %v", err)
	}
	if dec != tup {
		t.Fatalf("T10DIF roundtrip mismatch: got %+v want %+v", dec, tup)
	}

	// Real OS truncation mid-sector -> ErrFSPITornSector before decryption!
	dir := t.TempDir()
	rawDisk, err := journal.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer rawDisk.Close()

	fspiDisk, err := journal.NewFSPIDisk(rawDisk, &journal.FSPIConfig{
		Mode:     journal.ModeKeyedBLAKE3PI,
		TenantID: "tenant-trunc",
		VolumeID: "vol-trunc",
		Key:      testKey32(0xCC),
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := fspiDisk.Create("000000.seg", 16*1024)
	if err != nil {
		t.Fatal(err)
	}
	ff := f.(*journal.FSPIFile)
	if _, err := f.WriteAt(bytes.Repeat([]byte{0x7A}, 4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}

	// Truncate the physical segment file in the middle of sector 0 (2 KiB torn write).
	segPath := filepath.Join(dir, "000000.seg")
	if err := os.Truncate(segPath, ff.PhysicalOffset(0)+2048); err != nil {
		t.Fatal(err)
	}

	var buf [4096]byte
	if _, err := f.ReadAt(buf[:], 0); !errors.Is(err, journal.ErrFSPITornSector) {
		t.Fatalf("expected ErrFSPITornSector on truncated OS file, got: %v", err)
	}
	_ = f.Close()

	// Reopening the truncated file also fails closed with ErrFSPITornSector.
	if _, err := fspiDisk.Open("000000.seg"); !errors.Is(err, journal.ErrFSPITornSector) {
		t.Fatalf("expected ErrFSPITornSector on reopening truncated OS file, got: %v", err)
	}
}
