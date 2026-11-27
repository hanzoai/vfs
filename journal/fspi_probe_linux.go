//go:build linux

package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// ProbeNVMePI probes Linux sysfs (/sys/block/*/integrity) to determine whether
// the backing block device for dir (or any NVMe/block device on the host when
// dir is empty) exposes hardware T10-DIF / NVMe Protection Information offload.
//
// This function never fabricates hardware support: on virtualized/cloud disks
// (virtio-blk, loop, overlayfs over virtio) lacking T10-DIF/NVMe metadata
// formatting, Supported is false and Reason records the exact sysfs finding so
// callers fall back cleanly to software-only validation.
func ProbeNVMePI(dir string) NVMePIProbeResult {
	res := NVMePIProbeResult{
		Probed: true,
	}

	// Enumerate all host block devices for transparent reporting.
	if entries, err := os.ReadDir("/sys/block"); err == nil {
		for _, e := range entries {
			res.CheckedDevices = append(res.CheckedDevices, e.Name())
		}
		sort.Strings(res.CheckedDevices)
	}

	// If a directory path was given, try to resolve its backing block device.
	if dir != "" {
		if devName, ok := resolveBlockDevice(dir); ok {
			res.Device = devName
			if inspectSysfsIntegrity(devName, &res) {
				return res
			}
			if res.Reason == "" {
				res.Reason = fmt.Sprintf("block device %q does not expose NVMe/T10-DIF hardware protection information in /sys/block/%s/integrity", devName, devName)
			}
			return res
		}
	}

	// If dir is on a layered/virtual filesystem (such as overlayfs) or empty,
	// inspect all physical block devices in /sys/block to check whether any
	// hardware-PI-capable device is present on the host.
	for _, devName := range res.CheckedDevices {
		if inspectSysfsIntegrity(devName, &res) {
			res.Device = devName
			return res
		}
	}

	if len(res.CheckedDevices) == 0 {
		res.Reason = "no block devices visible under /sys/block; falling back to software FS-PI validation"
	} else {
		res.Reason = fmt.Sprintf("none of %d host block devices (%s) expose NVMe/T10-DIF hardware protection information; falling back to software FS-PI validation",
			len(res.CheckedDevices), strings.Join(res.CheckedDevices, ", "))
	}
	return res
}

// resolveBlockDevice maps a filesystem path to its parent block device name
// under /sys/block via stat(2) major/minor numbers and /sys/dev/block.
func resolveBlockDevice(path string) (string, bool) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return "", false
	}
	dev := uint64(st.Dev)
	// Standard Linux sysmacros.h major/minor decoding.
	major := ((dev >> 8) & 0xfff) | ((dev >> 32) &^ 0xfff)
	minor := (dev & 0xff) | ((dev >> 12) &^ 0xff)
	if major == 0 {
		// Anonymous/virtual filesystem (e.g. overlayfs, tmpfs, fuse).
		return "", false
	}
	link := fmt.Sprintf("/sys/dev/block/%d:%d", major, minor)
	target, err := filepath.EvalSymlinks(link)
	if err != nil {
		return "", false
	}
	// If target is a partition (e.g., .../nvme0n1/nvme0n1p1), walk up to the
	// disk directory that has a matching entry under /sys/block.
	base := filepath.Base(target)
	if _, err := os.Stat(filepath.Join("/sys/block", base)); err == nil {
		return base, true
	}
	parent := filepath.Base(filepath.Dir(target))
	if _, err := os.Stat(filepath.Join("/sys/block", parent)); err == nil {
		return parent, true
	}
	return base, true
}

// inspectSysfsIntegrity reads /sys/block/<dev>/integrity/* attributes and
// populates res if hardware PI is active and supported.
func inspectSysfsIntegrity(devName string, res *NVMePIProbeResult) bool {
	integrityDir := filepath.Join("/sys/block", devName, "integrity")
	info, err := os.Stat(integrityDir)
	if err != nil || !info.IsDir() {
		return false
	}
	format := readSysfsTrim(filepath.Join(integrityDir, "format"))
	capable := readSysfsInt(filepath.Join(integrityDir, "device_is_integrity_capable"))
	tagSize := readSysfsInt(filepath.Join(integrityDir, "tag_size"))
	interval := readSysfsInt(filepath.Join(integrityDir, "protection_interval_bytes"))
	readVerify := readSysfsInt(filepath.Join(integrityDir, "read_verify")) == 1
	writeGen := readSysfsInt(filepath.Join(integrityDir, "write_generate")) == 1

	res.SysfsPath = integrityDir
	res.Format = format
	res.TagSize = tagSize
	res.IntervalBytes = interval
	res.ReadVerify = readVerify
	res.WriteGenerate = writeGen

	if capable == 1 && format != "" && !strings.EqualFold(format, "none") {
		res.Supported = true
		res.Reason = fmt.Sprintf("hardware NVMe/T10-DIF protection information active (%s, interval=%dB, tag=%dB)", format, interval, tagSize)
		return true
	}
	res.Reason = fmt.Sprintf("sysfs %s present but hardware PI inactive (format=%q, capable=%d)", integrityDir, format, capable)
	return false
}

func readSysfsTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func readSysfsInt(path string) int {
	s := readSysfsTrim(path)
	if s == "" {
		return 0
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return v
}
