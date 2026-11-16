package git

import (
	"os"
	"path/filepath"
)

// SetupAlternates points the new sandbox repo to a shared read-only object store.
func SetupAlternates(repoPath, sharedRepoPath string) error {
	alternatesPath := filepath.Join(repoPath, ".git", "objects", "info", "alternates")
	
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
		return err
	}

	// Write the path to the shared object store
	return os.WriteFile(alternatesPath, []byte(sharedRepoPath), 0644)
}
