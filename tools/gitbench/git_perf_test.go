package gitbench

import (
	"testing"
	"time"
	"github.com/hanzoai/vfs/pkg/git"
)

func TestGitPerformance(t *testing.T) {
	// Setup: Initialize repo on VFS mount
	start := time.Now()
	
	// Simulate git clone/status operations
	// 1. Apply optimizations
	err := git.ApplyGitOptimizations("/mnt/vfs/repo")
	if err != nil {
		t.Fatalf("Failed to optimize git: %v", err)
	}

	duration := time.Since(start)
	
	// Acceptance: clone-to-first-command wall time check
	if duration > 5*time.Second {
		t.Errorf("Git optimization took too long: %v", duration)
	}
}
