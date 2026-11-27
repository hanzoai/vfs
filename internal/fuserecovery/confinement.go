package fuserecovery

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidateStateDir verifies that dir is a real directory (not a symlink),
// creating it with 0700 permissions if it does not yet exist. Every ancestor
// within dir's immediate leaf is checked via Lstat to prevent symlink swaps.
func ValidateStateDir(dir string) error {
	if dir == "" {
		return fmt.Errorf("%w: empty directory", ErrPathEscape)
	}
	if strings.ContainsRune(dir, '\x00') {
		return fmt.Errorf("%w: NUL byte in directory path", ErrPathEscape)
	}
	clean := filepath.Clean(dir)
	info, err := os.Lstat(clean)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(clean, 0o700); mkErr != nil {
				return fmt.Errorf("create state dir %q: %w", clean, mkErr)
			}
			info, err = os.Lstat(clean)
			if err != nil {
				return fmt.Errorf("lstat created state dir %q: %w", clean, err)
			}
		} else {
			return fmt.Errorf("lstat state dir %q: %w", clean, err)
		}
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: state dir %q is a symlink", ErrPathEscape, clean)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: state dir %q is not a directory", ErrPathEscape, clean)
	}
	return nil
}

// ConfinePath resolves rel inside root and rejects absolute paths, parent
// traversal (".."), NUL bytes, or any existing symlink component along the
// path. The final leaf may or may not exist yet; if it exists, it must not be
// a symlink.
func ConfinePath(root, rel string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("%w: empty root", ErrPathEscape)
	}
	if rel == "" {
		return "", fmt.Errorf("%w: empty relative path", ErrPathEscape)
	}
	if strings.ContainsRune(rel, '\x00') || strings.ContainsRune(root, '\x00') {
		return "", fmt.Errorf("%w: NUL byte in path", ErrPathEscape)
	}
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: absolute path %q not allowed", ErrPathEscape, rel)
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("abs root %q: %w", root, err)
	}
	rootInfo, err := os.Lstat(absRoot)
	if err != nil {
		return "", fmt.Errorf("lstat root %q: %w", absRoot, err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return "", fmt.Errorf("%w: root %q must be a real directory", ErrPathEscape, absRoot)
	}

	// Reject any raw ".." or "." traversal segments before cleaning so sneaky
	// symlink+dotdot combos never pass.
	parts := strings.Split(filepath.ToSlash(rel), "/")
	cleanParts := make([]string, 0, len(parts))
	for _, p := range parts {
		switch p {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("%w: parent traversal in %q", ErrPathEscape, rel)
		default:
			cleanParts = append(cleanParts, p)
		}
	}
	if len(cleanParts) == 0 {
		return "", fmt.Errorf("%w: path %q resolves to root", ErrPathEscape, rel)
	}

	// Walk every segment with Lstat to ensure no symlink sits anywhere in the chain.
	cur := absRoot
	for _, seg := range cleanParts {
		cur = filepath.Join(cur, seg)
		info, lerr := os.Lstat(cur)
		if lerr != nil {
			if os.IsNotExist(lerr) {
				// Non-existent suffix is allowed for creation, continue checking
				// remaining segments cannot escape via lexical containment below.
				continue
			}
			return "", fmt.Errorf("lstat %q: %w", cur, lerr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("%w: symlink component %q", ErrPathEscape, cur)
		}
	}

	// Final lexical prefix verification.
	relCheck, err := filepath.Rel(absRoot, cur)
	if err != nil || relCheck == "." || relCheck == ".." || strings.HasPrefix(relCheck, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q escapes %q", ErrPathEscape, rel, absRoot)
	}
	return cur, nil
}

// SyncDir fsyncs a directory so atomic file creations or renames within it are
// durable across crashes. Always closes the directory file descriptor.
func SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %q for sync: %w", dir, err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return fmt.Errorf("sync dir %q: %w", dir, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close dir %q: %w", dir, closeErr)
	}
	return nil
}
