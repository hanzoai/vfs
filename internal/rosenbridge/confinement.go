package rosenbridge

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidateTenantID ensures a tenant identifier is safe as a single isolated
// directory segment and cannot inject separators or traversal tokens.
func ValidateTenantID(tenantID string) error {
	if tenantID == "" || len(tenantID) > 128 {
		return fmt.Errorf("%w: length %d", ErrInvalidTenant, len(tenantID))
	}
	if tenantID == "." || tenantID == ".." {
		return fmt.Errorf("%w: reserved name %q", ErrInvalidTenant, tenantID)
	}
	for i := 0; i < len(tenantID); i++ {
		c := tenantID[i]
		ok := (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.'
		if !ok {
			return fmt.Errorf("%w: illegal character %q in %q", ErrInvalidTenant, c, tenantID)
		}
	}
	return nil
}

// NormalizeVirtualPath validates a guest-supplied relative path before any host
// filesystem or VFS lookup. It rejects NUL bytes, backslashes, empty segments
// after trimming a single leading slash, and "." or ".." traversal components.
func NormalizeVirtualPath(rel string) ([]string, error) {
	if rel == "" || strings.ContainsRune(rel, 0) || strings.ContainsRune(rel, '\\') {
		return nil, fmt.Errorf("%w: invalid virtual path %q", ErrPathEscape, rel)
	}
	trimmed := strings.TrimPrefix(rel, "/")
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty virtual path %q", ErrPathEscape, rel)
	}
	rawSegs := strings.Split(trimmed, "/")
	segs := make([]string, 0, len(rawSegs))
	for _, seg := range rawSegs {
		if seg == "" || seg == "." || seg == ".." {
			return nil, fmt.Errorf("%w: disallowed segment %q in %q", ErrPathEscape, seg, rel)
		}
		if len(seg) > 255 {
			return nil, fmt.Errorf("%w: segment too long (%d bytes) in %q", ErrPathEscape, len(seg), rel)
		}
		segs = append(segs, seg)
	}
	return segs, nil
}

// ConfinePath resolves rel strictly inside root, rejecting path traversal,
// NUL bytes, and any symlink component along the path (existing parent dirs or
// leaf target). If the leaf or deep parent directories do not yet exist (e.g.,
// prior to file creation), every existing prefix component is verified via
// os.Lstat not to be a symlink and the resulting path is verified to remain
// strictly under the canonical root.
func ConfinePath(root, rel string) (string, error) {
	if root == "" || strings.ContainsRune(root, 0) {
		return "", fmt.Errorf("%w: empty or invalid root", ErrPathEscape)
	}
	segs, err := NormalizeVirtualPath(rel)
	if err != nil {
		return "", err
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("%w: abs root: %v", ErrPathEscape, err)
	}
	realRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return "", fmt.Errorf("%w: eval root: %v", ErrPathEscape, err)
	}

	cur := realRoot
	checkingExist := true
	for _, seg := range segs {
		next := filepath.Join(cur, seg)
		if checkingExist {
			fi, err := os.Lstat(next)
			if err != nil {
				if os.IsNotExist(err) {
					checkingExist = false
				} else {
					return "", fmt.Errorf("%w: lstat %q: %v", ErrPathEscape, seg, err)
				}
			} else if fi.Mode()&os.ModeSymlink != 0 {
				return "", fmt.Errorf("%w: symlink component %q rejected", ErrPathEscape, seg)
			}
		}
		cur = next
	}

	prefix := realRoot + string(filepath.Separator)
	if !strings.HasPrefix(cur, prefix) {
		return "", fmt.Errorf("%w: resolved path %q outside root %q", ErrPathEscape, cur, realRoot)
	}
	return cur, nil
}
