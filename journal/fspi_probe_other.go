//go:build !linux

package journal

import "runtime"

// ProbeNVMePI reports that Linux sysfs NVMe/T10-DIF hardware Protection
// Information probing is unavailable on non-Linux hosts and instructs callers
// to fall back cleanly to software-only validation.
func ProbeNVMePI(dir string) NVMePIProbeResult {
	return NVMePIProbeResult{
		Probed:    true,
		Supported: false,
		Reason:    "NVMe/T10-DIF sysfs hardware protection information probe is only available on Linux (current OS: " + runtime.GOOS + "); falling back to software FS-PI validation",
	}
}
