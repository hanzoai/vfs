package git

import (
	"os/exec"
)

// ApplyGitOptimizations applies recommended git config settings for VFS mounts
// to minimize fsync overhead and optimize for many-file workloads.
func ApplyGitOptimizations(repoPath string) error {
	configs := map[string]string{
		"core.fsync":            "batch",
		"core.fsyncMethod":      "batch",
		"feature.manyFiles":     "true",
		"core.untrackedCache":   "true",
		"core.fsmonitor":        "true",
		"core.preloadIndex":     "true",
		"pack.threads":          "4",
		"gc.auto":               "0",
	}

	for key, value := range configs {
		cmd := exec.Command("git", "-C", repoPath, "config", key, value)
		if err := cmd.Run(); err != nil {
			return err
		}
	}
	return nil
}
