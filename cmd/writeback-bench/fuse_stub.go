//go:build !fuse || !linux

package main

func fuseSupported() bool { return false }

func runFUSEChild(_ []string) {}

func runRealFUSEBenchmarks(_ *BenchResults, _ string, _, _ int) {}
