package fspi

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// BlockDevicePI describes the hardware Protection Information attributes
// reported by a single Linux sysfs block device (/sys/block/<dev>/integrity/).
type BlockDevicePI struct {
	Device        string `json:"device"`
	HWPISupported bool   `json:"hw_pi_supported"`
	Format        string `json:"format"`
	TagSize       int    `json:"tag_size"`
	TupleSize     int    `json:"tuple_size"`
	WriteGenerate bool   `json:"write_generate"`
	ReadVerify    bool   `json:"read_verify"`
}

// ProbeReport summarizes hardware NVMe / T10-DIF Protection Information
// availability on the host and the automatically selected validation path.
type ProbeReport struct {
	OS              string          `json:"os"`
	Arch            string          `json:"arch"`
	SysBlockRoot    string          `json:"sys_block_root"`
	HWPISupported   bool            `json:"hw_pi_supported"`
	SelectedMode    string          `json:"selected_mode"`
	FallbackEngaged bool            `json:"fallback_engaged"`
	Devices         []BlockDevicePI `json:"devices"`
	Reason          string          `json:"reason"`
}

// ProbeNVMePI inspects Linux /sys/block/*/integrity/{format,tag_size,
// tuple_size,write_generate,read_verify} to detect hardware-supported NVMe /
// T10-DIF Protection Information. When running on virtualized storage
// (virtio-blk, overlayfs, loop) or non-Linux hosts, it reports
// HWPISupported=false honestly and selects the software-only validation path.
func ProbeNVMePI() ProbeReport {
	if runtime.GOOS != "linux" {
		return ProbeReport{
			OS:              runtime.GOOS,
			Arch:            runtime.GOARCH,
			SysBlockRoot:    "",
			HWPISupported:   false,
			SelectedMode:    GuardSoftwareStrong.String(),
			FallbackEngaged: true,
			Reason:          fmt.Sprintf("non-Linux host (%s/%s): sysfs NVMe PI unavailable; software-only validation path active", runtime.GOOS, runtime.GOARCH),
		}
	}
	return ProbeNVMePIAt("/sys/block")
}

// ProbeNVMePIAt probes block device integrity capabilities rooted at
// sysBlockRoot (normally "/sys/block"). Exported for deterministic testing
// against synthetic NVMe T10-DIF and virtualized storage sysfs fixtures.
func ProbeNVMePIAt(sysBlockRoot string) ProbeReport {
	rep := ProbeReport{
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		SysBlockRoot: sysBlockRoot,
	}

	entries, err := os.ReadDir(sysBlockRoot)
	if err != nil {
		rep.HWPISupported = false
		rep.SelectedMode = GuardSoftwareStrong.String()
		rep.FallbackEngaged = true
		rep.Reason = fmt.Sprintf("unable to read %s (%v); software-only validation path active", sysBlockRoot, err)
		return rep
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	var piDevices []string
	var nonPIDevices []string
	for _, name := range names {
		dev := probeBlockDevice(filepath.Join(sysBlockRoot, name), name)
		rep.Devices = append(rep.Devices, dev)
		if dev.HWPISupported {
			rep.HWPISupported = true
			piDevices = append(piDevices, fmt.Sprintf("%s(%s,tuple=%dB,tag=%dB)", dev.Device, dev.Format, dev.TupleSize, dev.TagSize))
		} else {
			nonPIDevices = append(nonPIDevices, dev.Device)
		}
	}

	if rep.HWPISupported {
		rep.SelectedMode = GuardT10DIF.String()
		rep.FallbackEngaged = false
		rep.Reason = fmt.Sprintf("hardware NVMe/T10-DIF PI available on: %s", strings.Join(piDevices, ", "))
	} else {
		rep.SelectedMode = GuardSoftwareStrong.String()
		rep.FallbackEngaged = true
		if len(nonPIDevices) == 0 {
			rep.Reason = fmt.Sprintf("no block devices found under %s; software-only validation path active", sysBlockRoot)
		} else {
			rep.Reason = fmt.Sprintf("no block device in %s exposes NVMe/T10-DIF PI (scanned: %s); software-only validation path active", sysBlockRoot, strings.Join(nonPIDevices, ", "))
		}
	}
	return rep
}

func probeBlockDevice(devPath, name string) BlockDevicePI {
	d := BlockDevicePI{
		Device: name,
		Format: "none",
	}
	intDir := filepath.Join(devPath, "integrity")
	info, err := os.Stat(intDir)
	if err != nil || !info.IsDir() {
		return d
	}

	format := readSysfsString(filepath.Join(intDir, "format"))
	if format != "" {
		d.Format = format
	}
	d.TagSize = readSysfsInt(filepath.Join(intDir, "tag_size"))
	d.TupleSize = readSysfsInt(filepath.Join(intDir, "tuple_size"))
	d.WriteGenerate = readSysfsBool(filepath.Join(intDir, "write_generate"))
	d.ReadVerify = readSysfsBool(filepath.Join(intDir, "read_verify"))

	// Linux reports "none" when a block device has an integrity/ sysfs directory
	// without an active protection profile.
	norm := strings.ToLower(strings.TrimSpace(d.Format))
	if norm != "" && norm != "none" && (d.TupleSize > 0 || d.TagSize > 0 || strings.Contains(norm, "t10") || strings.Contains(norm, "crc")) {
		if d.TupleSize == 0 {
			if strings.Contains(norm, "crc64") {
				d.TupleSize = Tuple128Size
			} else {
				d.TupleSize = Tuple64Size
			}
		}
		d.HWPISupported = true
	}
	return d
}

func readSysfsString(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func readSysfsInt(path string) int {
	s := readSysfsString(path)
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func readSysfsBool(path string) bool {
	s := strings.ToLower(readSysfsString(path))
	return s == "1" || s == "true" || s == "yes"
}
