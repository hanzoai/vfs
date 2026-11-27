// Command fspi-bench runs reproducible real-disk end-to-end journal integrity
// (FS-PI) experiments and benchmarks across:
//   - none               (unmodified baseline journal / raw disk)
//   - aead_only_baseline (HIP-0302 AES-256-GCM envelope without below-encryption PI)
//   - software_crc32c    (software 16B NVMe PI + HIP-0302 AEAD)
//   - keyed_blake3_pi    (cryptographic 32B keyed BLAKE3-128 PI + HIP-0302 AEAD)
//   - hw_nvme_pi_probe   (Linux /sys/block/*/integrity capability probe + fallback)
//
// It injects all 7 below-encryption storage corruption classes plus the
// active-adversary CRC forgery negative regime directly onto raw on-disk bytes,
// and emits raw JSON results plus a Markdown score table for HIP-1344 / HIP-1345.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/hanzoai/vfs/journal"
)

type faultOutcome struct {
	FaultClass         string `json:"fault_class"`
	Detected           bool   `json:"detected"`
	PreDecryptRejected bool   `json:"pre_decrypt_rejected"`
	DecryptInvocations uint64 `json:"decrypt_invocations"`
	Error              string `json:"error,omitempty"`
}

type modeResult struct {
	Mode                       string         `json:"mode"`
	EffectiveMode              string         `json:"effective_mode"`
	Sectors                    int            `json:"sectors"`
	LogicalBytes               int64          `json:"logical_bytes"`
	PhysicalBytes              int64          `json:"physical_bytes"`
	MetadataOverheadPct        float64        `json:"metadata_overhead_pct"`
	WriteThroughputMBps        float64        `json:"write_throughput_mb_s"`
	ReadThroughputMBps         float64        `json:"read_throughput_mb_s"`
	WriteP50Us                 float64        `json:"write_p50_us"`
	WriteP99Us                 float64        `json:"write_p99_us"`
	ReadP50Us                  float64        `json:"read_p50_us"`
	ReadP99Us                  float64        `json:"read_p99_us"`
	WriteCPUUsPerMB            float64        `json:"write_cpu_us_per_mb"`
	ReadCPUUsPerMB             float64        `json:"read_cpu_us_per_mb"`
	TotalCPUUsPerMB            float64        `json:"total_cpu_us_per_mb"`
	CPUOverheadVsNonePct       float64        `json:"cpu_overhead_vs_none_pct"`
	CPUOverheadVsAEADPct       float64        `json:"cpu_overhead_vs_aead_pct"`
	FaultDetectedCount         int            `json:"fault_detected_count"`
	FaultTotalCount            int            `json:"fault_total_count"`
	DetectionCoveragePct       float64        `json:"detection_coverage_pct"`
	PrePlaintextRejectCount    int            `json:"pre_plaintext_reject_count"`
	PrePlaintextRejectRatePct  float64        `json:"pre_plaintext_reject_rate_pct"`
	ActiveForgeryPILayerCaught bool           `json:"active_forgery_pi_layer_caught"`
	FaultMatrix                []faultOutcome `json:"fault_matrix"`
	ActiveForgeryOutcome       faultOutcome   `json:"active_forgery_outcome"`
}

type journalE2EResult struct {
	Mode             string  `json:"mode"`
	EffectiveMode    string  `json:"effective_mode"`
	Records          int     `json:"records"`
	RecordBytes      int     `json:"record_bytes"`
	AppendSyncMBps   float64 `json:"append_sync_mb_s"`
	ReplayMBps       float64 `json:"replay_mb_s"`
	CPUUsPerMB       float64 `json:"cpu_us_per_mb"`
	MetadataBytes    uint64  `json:"metadata_bytes"`
	LogicalBytes     uint64  `json:"logical_bytes"`
	MetadataRatioPct float64 `json:"metadata_ratio_pct"`
	SectorsWritten   uint64  `json:"sectors_written"`
	SectorsVerified  uint64  `json:"sectors_verified"`
	ValidationFails  uint64  `json:"validation_failures"`
}

type hostInfo struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	NumCPU    int    `json:"num_cpu"`
	CPUModel  string `json:"cpu_model"`
	Kernel    string `json:"kernel"`
}

type benchReport struct {
	TimestampUTC    string                    `json:"timestamp_utc"`
	Host            hostInfo                  `json:"host"`
	HardwarePIProbe journal.NVMePIProbeResult `json:"hardware_pi_probe"`
	SectorBench     []modeResult              `json:"sector_bench"`
	JournalE2EBench []journalE2EResult        `json:"journal_e2e_bench"`
}

func main() {
	var (
		dirFlag       = flag.String("dir", "", "working directory on real disk (defaults to temp dir)")
		sectorsFlag   = flag.Int("sectors", 4096, "number of 4 KiB sectors per mode (4096 = 16 MiB)")
		syncEveryFlag = flag.Int("sync-every", 256, "call fdatasync every N sectors during write benchmark")
		trialsFlag    = flag.Int("trials", 3, "number of timed trials per mode (median reported)")
		jRecordsFlag  = flag.Int("journal-records", 1024, "number of 4 KiB records for end-to-end Journal benchmark")
		jsonOutFlag   = flag.String("json", "", "path to write raw_results.json")
		mdOutFlag     = flag.String("md", "", "path to write score_table.md")
	)
	flag.Parse()

	if *trialsFlag < 1 {
		*trialsFlag = 1
	}

	workDir := *dirFlag
	cleanup := false
	if workDir == "" {
		var err error
		workDir, err = os.MkdirTemp("", "fspi-bench-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "mkdirtemp: %v\n", err)
			os.Exit(1)
		}
		cleanup = true
	} else {
		if err := os.MkdirAll(workDir, 0o700); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", workDir, err)
			os.Exit(1)
		}
	}
	if cleanup {
		defer os.RemoveAll(workDir)
	}

	probe := journal.ProbeNVMePI(workDir)
	report := benchReport{
		TimestampUTC:    time.Now().UTC().Format(time.RFC3339),
		Host:            collectHostInfo(),
		HardwarePIProbe: probe,
	}

	modes := []journal.FSPIMode{
		journal.ModeNone,
		journal.ModeAEADOnlyBaseline,
		journal.ModeSoftwareCRC32C,
		journal.ModeKeyedBLAKE3PI,
	}
	for _, m := range modes {
		res, err := runModeBenchmarkTrials(workDir, m, *sectorsFlag, *syncEveryFlag, *trialsFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sector benchmark %s failed: %v\n", m, err)
			os.Exit(1)
		}
		report.SectorBench = append(report.SectorBench, res)
	}

	// Compute relative CPU overheads against ModeNone and ModeAEADOnlyBaseline.
	var baseNoneCPU, baseAEADCPU float64
	for _, r := range report.SectorBench {
		if r.Mode == journal.ModeNone.String() {
			baseNoneCPU = r.TotalCPUUsPerMB
		}
		if r.Mode == journal.ModeAEADOnlyBaseline.String() {
			baseAEADCPU = r.TotalCPUUsPerMB
		}
	}
	for i := range report.SectorBench {
		if baseNoneCPU > 0 {
			report.SectorBench[i].CPUOverheadVsNonePct = round2(((report.SectorBench[i].TotalCPUUsPerMB - baseNoneCPU) / baseNoneCPU) * 100.0)
		}
		if baseAEADCPU > 0 {
			report.SectorBench[i].CPUOverheadVsAEADPct = round2(((report.SectorBench[i].TotalCPUUsPerMB - baseAEADCPU) / baseAEADCPU) * 100.0)
		}
	}

	// End-to-end Journal benchmark (Append + Sync + Replay) across journal modes.
	jModes := []journal.FSPIMode{
		journal.ModeNone,
		journal.ModeSoftwareCRC32C,
		journal.ModeKeyedBLAKE3PI,
		journal.ModeHardwarePINVMeProbe,
	}
	for _, m := range jModes {
		jr, err := runJournalE2E(workDir, m, *jRecordsFlag, 4096)
		if err != nil {
			fmt.Fprintf(os.Stderr, "journal e2e benchmark %s failed: %v\n", m, err)
			os.Exit(1)
		}
		report.JournalE2EBench = append(report.JournalE2EBench, jr)
	}

	md := renderScoreTable(report)
	if *jsonOutFlag != "" {
		if err := os.MkdirAll(filepath.Dir(*jsonOutFlag), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir json out: %v\n", err)
			os.Exit(1)
		}
		raw, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal json: %v\n", err)
			os.Exit(1)
		}
		raw = append(raw, '\n')
		if err := os.WriteFile(*jsonOutFlag, raw, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write json: %v\n", err)
			os.Exit(1)
		}
	}
	if *mdOutFlag != "" {
		if err := os.MkdirAll(filepath.Dir(*mdOutFlag), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir md out: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*mdOutFlag, []byte(md), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write md: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Print(md)
}

func benchKey(seed byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = seed ^ byte(i*19+7)
	}
	return k
}

func rusageCPUMicros() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	userUs := float64(ru.Utime.Sec)*1e6 + float64(ru.Utime.Usec)
	sysUs := float64(ru.Stime.Sec)*1e6 + float64(ru.Stime.Usec)
	return userUs + sysUs
}

func runModeBenchmarkTrials(rootDir string, mode journal.FSPIMode, sectors, syncEvery, trials int) (modeResult, error) {
	results := make([]modeResult, 0, trials)
	for t := 0; t < trials; t++ {
		r, err := runModeBenchmark(rootDir, mode, sectors, syncEvery)
		if err != nil {
			return modeResult{}, err
		}
		results = append(results, r)
	}
	agg := results[len(results)/2]
	med := func(fn func(modeResult) float64) float64 {
		vals := make([]float64, len(results))
		for i, r := range results {
			vals[i] = fn(r)
		}
		return percentile(vals, 50)
	}
	agg.WriteThroughputMBps = round2(med(func(r modeResult) float64 { return r.WriteThroughputMBps }))
	agg.ReadThroughputMBps = round2(med(func(r modeResult) float64 { return r.ReadThroughputMBps }))
	agg.WriteP50Us = round2(med(func(r modeResult) float64 { return r.WriteP50Us }))
	agg.WriteP99Us = round2(med(func(r modeResult) float64 { return r.WriteP99Us }))
	agg.ReadP50Us = round2(med(func(r modeResult) float64 { return r.ReadP50Us }))
	agg.ReadP99Us = round2(med(func(r modeResult) float64 { return r.ReadP99Us }))
	agg.WriteCPUUsPerMB = round2(med(func(r modeResult) float64 { return r.WriteCPUUsPerMB }))
	agg.ReadCPUUsPerMB = round2(med(func(r modeResult) float64 { return r.ReadCPUUsPerMB }))
	agg.TotalCPUUsPerMB = round2(agg.WriteCPUUsPerMB + agg.ReadCPUUsPerMB)
	return agg, nil
}

func runModeBenchmark(rootDir string, mode journal.FSPIMode, sectors, syncEvery int) (modeResult, error) {
	dir := filepath.Join(rootDir, "sector-"+mode.String())
	if err := os.RemoveAll(dir); err != nil {
		return modeResult{}, err
	}
	rawDisk, err := journal.Files(dir)
	if err != nil {
		return modeResult{}, err
	}
	defer rawDisk.Close()

	var disk journal.Disk = rawDisk
	effMode := mode
	if mode == journal.ModeSoftwareCRC32C || mode == journal.ModeKeyedBLAKE3PI {
		fspiDisk, err := journal.NewFSPIDisk(rawDisk, &journal.FSPIConfig{
			Mode:     mode,
			TenantID: "tenant-bench",
			VolumeID: "vol-bench",
			Key:      benchKey(0x31),
		})
		if err != nil {
			return modeResult{}, err
		}
		disk = fspiDisk
		effMode = fspiDisk.EffectiveMode()
	}

	logicalBytes := int64(sectors) * journal.FSPISectorSize
	f, err := disk.Create("000000.seg", logicalBytes)
	if err != nil {
		return modeResult{}, err
	}
	defer f.Close()

	store, err := journal.NewHIP0302SectorStore(f, mode, benchKey(0x52), "tenant-bench", "vol-bench")
	if err != nil {
		return modeResult{}, err
	}

	// Deterministic 3840-byte payload spanning past the 2 KiB midpoint.
	payload := make([]byte, 3840)
	for i := range payload {
		payload[i] = byte((i*37 + 11) & 0xff)
	}

	// Warmup 256 sectors to settle page cache, sync.Pool, and Go allocator before timing.
	warm := min(256, sectors)
	for i := 0; i < warm; i++ {
		if err := store.WriteSector(uint32(i), payload); err != nil {
			return modeResult{}, err
		}
	}
	if err := store.Sync(); err != nil {
		return modeResult{}, err
	}
	for i := 0; i < warm; i++ {
		if _, err := store.ReadSector(uint32(i)); err != nil {
			return modeResult{}, err
		}
	}
	runtime.GC()

	// Measured WRITE pass.
	writeLats := make([]float64, sectors)
	cpuW0 := rusageCPUMicros()
	tW0 := time.Now()
	for i := 0; i < sectors; i++ {
		payload[0] = byte(i)
		payload[1] = byte(i >> 8)
		ts := time.Now()
		if err := store.WriteSector(uint32(i), payload); err != nil {
			return modeResult{}, err
		}
		if syncEvery > 0 && (i+1)%syncEvery == 0 {
			if err := store.Sync(); err != nil {
				return modeResult{}, err
			}
		}
		writeLats[i] = float64(time.Since(ts).Nanoseconds()) / 1000.0
	}
	if err := store.Sync(); err != nil {
		return modeResult{}, err
	}
	writeDur := time.Since(tW0)
	writeCPUUs := max(0, rusageCPUMicros()-cpuW0)

	// Measure exact physical file size on disk.
	info, err := os.Stat(filepath.Join(dir, "000000.seg"))
	if err != nil {
		return modeResult{}, err
	}
	physicalBytes := info.Size()

	// Measured READ pass.
	readLats := make([]float64, sectors)
	cpuR0 := rusageCPUMicros()
	tR0 := time.Now()
	for i := 0; i < sectors; i++ {
		ts := time.Now()
		got, err := store.ReadSector(uint32(i))
		if err != nil {
			return modeResult{}, fmt.Errorf("clean read lba=%d: %w", i, err)
		}
		if len(got) != len(payload) || got[0] != byte(i) || got[1] != byte(i>>8) {
			return modeResult{}, fmt.Errorf("clean read mismatch at lba=%d", i)
		}
		readLats[i] = float64(time.Since(ts).Nanoseconds()) / 1000.0
	}
	readDur := time.Since(tR0)
	readCPUUs := max(0, rusageCPUMicros()-cpuR0)

	// Run all 7 below-encryption fault classes + active adversary forgery.
	matrix := make([]faultOutcome, 0, len(journal.AllFSPIFaultClasses))
	detectedCount := 0
	preDecryptCount := 0
	for _, fc := range journal.AllFSPIFaultClasses {
		out, err := evaluateFault(rootDir, mode, fc)
		if err != nil {
			return modeResult{}, err
		}
		if out.Detected {
			detectedCount++
		}
		if out.PreDecryptRejected {
			preDecryptCount++
		}
		matrix = append(matrix, out)
	}
	forgeryOut, err := evaluateFault(rootDir, mode, journal.FaultActiveAdversaryCRCForgery)
	if err != nil {
		return modeResult{}, err
	}

	mb := float64(logicalBytes) / (1024.0 * 1024.0)
	metaPct := (float64(physicalBytes-logicalBytes) / float64(logicalBytes)) * 100.0

	return modeResult{
		Mode:                       mode.String(),
		EffectiveMode:              effMode.String(),
		Sectors:                    sectors,
		LogicalBytes:               logicalBytes,
		PhysicalBytes:              physicalBytes,
		MetadataOverheadPct:        round3(metaPct),
		WriteThroughputMBps:        round2(mb / writeDur.Seconds()),
		ReadThroughputMBps:         round2(mb / readDur.Seconds()),
		WriteP50Us:                 round2(percentile(writeLats, 50)),
		WriteP99Us:                 round2(percentile(writeLats, 99)),
		ReadP50Us:                  round2(percentile(readLats, 50)),
		ReadP99Us:                  round2(percentile(readLats, 99)),
		WriteCPUUsPerMB:            round2(writeCPUUs / mb),
		ReadCPUUsPerMB:             round2(readCPUUs / mb),
		TotalCPUUsPerMB:            round2((writeCPUUs + readCPUUs) / mb),
		FaultDetectedCount:         detectedCount,
		FaultTotalCount:            len(journal.AllFSPIFaultClasses),
		DetectionCoveragePct:       round2(float64(detectedCount) / float64(len(journal.AllFSPIFaultClasses)) * 100.0),
		PrePlaintextRejectCount:    preDecryptCount,
		PrePlaintextRejectRatePct:  round2(float64(preDecryptCount) / float64(len(journal.AllFSPIFaultClasses)) * 100.0),
		ActiveForgeryPILayerCaught: forgeryOut.PreDecryptRejected,
		FaultMatrix:                matrix,
		ActiveForgeryOutcome:       forgeryOut,
	}, nil
}

func evaluateFault(rootDir string, mode journal.FSPIMode, fault journal.FSPIFaultClass) (faultOutcome, error) {
	dirA := filepath.Join(rootDir, "fault-"+mode.String()+"-"+string(fault)+"-A")
	_ = os.RemoveAll(dirA)
	defer os.RemoveAll(dirA)

	rawDiskA, err := journal.Files(dirA)
	if err != nil {
		return faultOutcome{}, err
	}
	defer rawDiskA.Close()

	var diskA journal.Disk = rawDiskA
	if mode == journal.ModeSoftwareCRC32C || mode == journal.ModeKeyedBLAKE3PI {
		fspiA, err := journal.NewFSPIDisk(rawDiskA, &journal.FSPIConfig{
			Mode:     mode,
			TenantID: "tenant-alpha",
			VolumeID: "vol-001",
			Key:      benchKey(0x11),
		})
		if err != nil {
			return faultOutcome{}, err
		}
		diskA = fspiA
	}

	const segSize = 64 * 1024
	fA, err := diskA.Create("000000.seg", segSize)
	if err != nil {
		return faultOutcome{}, err
	}
	defer fA.Close()

	storeA, err := journal.NewHIP0302SectorStore(fA, mode, benchKey(0xA1), "tenant-alpha", "vol-001")
	if err != nil {
		return faultOutcome{}, err
	}

	mkData := func(tag string, lba uint32, gen int) []byte {
		b := make([]byte, 3840)
		copy(b, fmt.Sprintf("%s:%d:%d", tag, lba, gen))
		for i := 32; i < len(b); i++ {
			b[i] = byte((i*29 + int(lba)*11 + gen*7) & 0xff)
		}
		return b
	}

	for lba := uint32(0); lba < 4; lba++ {
		if err := storeA.WriteSector(lba, mkData("alpha", lba, 1)); err != nil {
			return faultOutcome{}, err
		}
	}
	if err := storeA.Sync(); err != nil {
		return faultOutcome{}, err
	}
	snapGen1, err := journal.SnapshotPhysicalSector(fA, 2)
	if err != nil {
		return faultOutcome{}, err
	}
	wantLBA2 := mkData("alpha", 2, 2)
	if err := storeA.WriteSector(2, wantLBA2); err != nil {
		return faultOutcome{}, err
	}
	if err := storeA.Sync(); err != nil {
		return faultOutcome{}, err
	}

	targetLBA := uint32(2)
	switch fault {
	case journal.FaultSingleBitFlip:
		err = journal.InjectSingleBitFlip(fA, targetLBA, 128, 3)
	case journal.FaultMultiByteBurst:
		err = journal.InjectMultiByteBurst(fA, targetLBA, 256, 64)
	case journal.FaultMisdirectedBlockSwap:
		err = journal.InjectMisdirectedBlockSwap(fA, 1, 2)
	case journal.FaultStaleLostWriteReplay:
		err = journal.InjectStaleLostWriteReplay(fA, targetLBA, snapGen1)
	case journal.FaultCrossTenantTransplant:
		dirB := filepath.Join(rootDir, "fault-"+mode.String()+"-"+string(fault)+"-B")
		_ = os.RemoveAll(dirB)
		defer os.RemoveAll(dirB)
		rawDiskB, derr := journal.Files(dirB)
		if derr != nil {
			return faultOutcome{}, derr
		}
		defer rawDiskB.Close()
		var diskB journal.Disk = rawDiskB
		if mode == journal.ModeSoftwareCRC32C || mode == journal.ModeKeyedBLAKE3PI {
			fspiB, ferr := journal.NewFSPIDisk(rawDiskB, &journal.FSPIConfig{
				Mode:     mode,
				TenantID: "tenant-beta",
				VolumeID: "vol-999",
				Key:      benchKey(0x22),
			})
			if ferr != nil {
				return faultOutcome{}, ferr
			}
			diskB = fspiB
		}
		fB, cerr := diskB.Create("000000.seg", segSize)
		if cerr != nil {
			return faultOutcome{}, cerr
		}
		defer fB.Close()
		storeB, serr := journal.NewHIP0302SectorStore(fB, mode, benchKey(0xB2), "tenant-beta", "vol-999")
		if serr != nil {
			return faultOutcome{}, serr
		}
		if werr := storeB.WriteSector(targetLBA, mkData("beta", targetLBA, 1)); werr != nil {
			return faultOutcome{}, werr
		}
		if werr := storeB.WriteSector(targetLBA, mkData("beta", targetLBA, 2)); werr != nil {
			return faultOutcome{}, werr
		}
		if serr := storeB.Sync(); serr != nil {
			return faultOutcome{}, serr
		}
		err = journal.InjectCrossTenantTransplant(fA, targetLBA, fB, targetLBA)
	case journal.FaultZeroedSectorPhantom:
		err = journal.InjectZeroedSectorPhantom(fA, targetLBA)
	case journal.FaultTruncatedTornTail:
		err = journal.InjectTruncatedTornTail(fA, targetLBA, journal.FSPISectorSize/2)
	case journal.FaultActiveAdversaryCRCForgery:
		err = journal.InjectActiveAdversaryCRCForgery(fA, targetLBA, 100)
	}
	if err != nil {
		return faultOutcome{}, err
	}

	dec0, pre0, _, _ := storeA.Metrics()
	got, rerr := storeA.ReadSector(targetLBA)
	dec1, pre1, _, _ := storeA.Metrics()

	out := faultOutcome{
		FaultClass:         string(fault),
		DecryptInvocations: dec1 - dec0,
	}
	if rerr != nil {
		out.Detected = true
		out.PreDecryptRejected = (pre1 > pre0 && dec1 == dec0)
		out.Error = rerr.Error()
		return out, nil
	}
	if bytes.Equal(got, wantLBA2) {
		return out, fmt.Errorf("fault %s had no observable effect", fault)
	}
	return out, nil
}

func runJournalE2E(rootDir string, mode journal.FSPIMode, records, recBytes int) (journalE2EResult, error) {
	dir := filepath.Join(rootDir, "journal-e2e-"+mode.String())
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	opt := journal.Options{
		Capacity: 16 << 20,
		Segment:  2 << 20,
		Lineage:  []byte("fspi-e2e-" + mode.String()),
	}
	if mode != journal.ModeNone {
		opt.Integrity = &journal.FSPIConfig{
			Mode:         mode,
			FallbackMode: journal.ModeKeyedBLAKE3PI,
			TenantID:     "tenant-e2e",
			VolumeID:     "vol-e2e",
			Key:          benchKey(0x66),
			ProbeDir:     dir,
		}
	}

	j, err := journal.Open(dir, opt)
	if err != nil {
		return journalE2EResult{}, err
	}
	defer j.Close()

	ctx := context.Background()
	payload := make([]byte, recBytes)
	for i := range payload {
		payload[i] = byte((i*41 + 5) & 0xff)
	}

	cpu0 := rusageCPUMicros()
	tW := time.Now()
	var last uint64
	for i := 0; i < records; i++ {
		payload[0] = byte(i)
		seq, err := j.Append(ctx, payload)
		if err != nil {
			return journalE2EResult{}, err
		}
		last = seq
		if (i+1)%64 == 0 {
			if err := j.Sync(ctx, seq); err != nil {
				return journalE2EResult{}, err
			}
		}
	}
	if err := j.Sync(ctx, last); err != nil {
		return journalE2EResult{}, err
	}
	writeDur := time.Since(tW)

	tR := time.Now()
	replayed := 0
	if err := j.Replay(func(r journal.Record) error {
		replayed++
		return nil
	}); err != nil {
		return journalE2EResult{}, err
	}
	replayDur := time.Since(tR)
	cpuUs := max(0, rusageCPUMicros()-cpu0)
	if replayed != records {
		return journalE2EResult{}, fmt.Errorf("replayed %d want %d", replayed, records)
	}

	st := j.FSPIStats()
	mb := float64(records*recBytes) / (1024.0 * 1024.0)
	var metaPct float64
	if st.LogicalBytes > 0 {
		metaPct = (float64(st.MetadataBytes) / float64(st.LogicalBytes)) * 100.0
	}
	return journalE2EResult{
		Mode:             mode.String(),
		EffectiveMode:    st.EffectiveMode.String(),
		Records:          records,
		RecordBytes:      recBytes,
		AppendSyncMBps:   round2(mb / writeDur.Seconds()),
		ReplayMBps:       round2(mb / replayDur.Seconds()),
		CPUUsPerMB:       round2(cpuUs / mb),
		MetadataBytes:    st.MetadataBytes,
		LogicalBytes:     st.LogicalBytes,
		MetadataRatioPct: round3(metaPct),
		SectorsWritten:   st.SectorsWritten,
		SectorsVerified:  st.SectorsVerified,
		ValidationFails:  st.ValidationFailures,
	}, nil
}

func collectHostInfo() hostInfo {
	h := hostInfo{
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		CPUModel:  "unknown",
		Kernel:    "unknown",
	}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "model name") {
				if _, v, ok := strings.Cut(line, ":"); ok {
					h.CPUModel = strings.TrimSpace(v)
					break
				}
			}
		}
	}
	if b, err := os.ReadFile("/proc/version"); err == nil {
		fields := strings.Fields(string(b))
		if len(fields) >= 3 {
			h.Kernel = strings.Join(fields[:3], " ")
		}
	}
	return h
}

func percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	idx := int(math.Ceil((p/100.0)*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
func round3(v float64) float64 { return math.Round(v*1000) / 1000 }

func renderScoreTable(r benchReport) string {
	var b strings.Builder
	b.WriteString("# FS-PI End-to-End Journal Integrity — Measured Score Table (HIP-1344 / HIP-1345)\n\n")
	fmt.Fprintf(&b, "- **Timestamp (UTC)**: `%s`\n", r.TimestampUTC)
	fmt.Fprintf(&b, "- **Host**: `%s` (`%s/%s`, `%d` vCPUs, `%s`, `%s`)\n",
		r.Host.CPUModel, r.Host.GOOS, r.Host.GOARCH, r.Host.NumCPU, r.Host.Kernel, r.Host.GoVersion)
	fmt.Fprintf(&b, "- **NVMe Hardware PI Probe (`/sys/block/*/integrity`)**: `supported=%v` — %s\n\n",
		r.HardwarePIProbe.Supported, r.HardwarePIProbe.Reason)

	b.WriteString("## 1. Sector Integrity, Pre-Plaintext Rejection & Performance Summary\n\n")
	b.WriteString("| Mode | Detection Coverage (7 Faults) | Pre-Plaintext Rejection | Active Forgery Caught (PI Layer) | Write (MB/s) | Read (MB/s) | Write p50 / p99 (us) | Read p50 / p99 (us) | CPU (us/MB) | CPU Overhead vs AEAD (%) | Metadata Overhead (%) |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, m := range r.SectorBench {
		forgeryStr := "NO (Bypassed)"
		if m.ActiveForgeryPILayerCaught {
			forgeryStr = "YES (Pre-Decrypt)"
		}
		fmt.Fprintf(&b, "| `%s` | %d/%d (%.1f%%) | %d/%d (%.1f%%) | %s | %.2f | %.2f | %.2f / %.2f | %.2f / %.2f | %.2f | %+.2f%% | %.3f%% |\n",
			m.Mode,
			m.FaultDetectedCount, m.FaultTotalCount, m.DetectionCoveragePct,
			m.PrePlaintextRejectCount, m.FaultTotalCount, m.PrePlaintextRejectRatePct,
			forgeryStr,
			m.WriteThroughputMBps, m.ReadThroughputMBps,
			m.WriteP50Us, m.WriteP99Us,
			m.ReadP50Us, m.ReadP99Us,
			m.TotalCPUUsPerMB, m.CPUOverheadVsAEADPct,
			m.MetadataOverheadPct,
		)
	}

	b.WriteString("\n## 2. Per-Fault Below-Encryption Detection Matrix\n\n")
	b.WriteString("| Fault Class (Injected Directly on Raw Disk Bytes) | `none` | `aead_only_baseline` | `software_crc32c` | `keyed_blake3_pi` |\n")
	b.WriteString("|---|---|---|---|---|\n")
	byMode := map[string]modeResult{}
	for _, m := range r.SectorBench {
		byMode[m.Mode] = m
	}
	fmtCell := func(out faultOutcome) string {
		if out.Detected && out.PreDecryptRejected {
			return "CAUGHT (Pre-Decrypt, 0 AEAD calls)"
		}
		if out.Detected {
			return fmt.Sprintf("CAUGHT at AEAD (%d decrypt call)", out.DecryptInvocations)
		}
		return "UNDETECTED (Silent Corruption)"
	}
	for i, fc := range journal.AllFSPIFaultClasses {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s |\n",
			fc,
			fmtCell(byMode["none"].FaultMatrix[i]),
			fmtCell(byMode["aead_only_baseline"].FaultMatrix[i]),
			fmtCell(byMode["software_crc32c"].FaultMatrix[i]),
			fmtCell(byMode["keyed_blake3_pi"].FaultMatrix[i]),
		)
	}
	fmt.Fprintf(&b, "| `%s` *(Negative Regime #1)* | %s | %s | %s | %s |\n",
		journal.FaultActiveAdversaryCRCForgery,
		fmtCell(byMode["none"].ActiveForgeryOutcome),
		fmtCell(byMode["aead_only_baseline"].ActiveForgeryOutcome),
		fmtCell(byMode["software_crc32c"].ActiveForgeryOutcome),
		fmtCell(byMode["keyed_blake3_pi"].ActiveForgeryOutcome),
	)

	b.WriteString("\n## 3. End-to-End `journal.Journal` Pipeline Benchmark (Append + Sync + Replay)\n\n")
	b.WriteString("| Requested Mode | Effective Mode | Records (4 KiB) | Append+Sync (MB/s) | Replay (MB/s) | CPU (us/MB) | Sectors Written | Sectors Verified | Segment Metadata Overhead (%) |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, jr := range r.JournalE2EBench {
		fmt.Fprintf(&b, "| `%s` | `%s` | %d | %.2f | %.2f | %.2f | %d | %d | %.3f%% |\n",
			jr.Mode, jr.EffectiveMode, jr.Records,
			jr.AppendSyncMBps, jr.ReplayMBps, jr.CPUUsPerMB,
			jr.SectorsWritten, jr.SectorsVerified, jr.MetadataRatioPct,
		)
	}
	return b.String()
}
