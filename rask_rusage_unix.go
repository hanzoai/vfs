// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build unix

package vfs

import "syscall"

// processCPUTimeNs returns cumulative user + system CPU time of the current
// process in nanoseconds via getrusage(2).
func processCPUTimeNs() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return ru.Utime.Nano() + ru.Stime.Nano()
}
