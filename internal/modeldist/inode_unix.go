//go:build unix

package modeldist

import (
	"os"
	"syscall"
)

func hasMultipleHardLinks(fi os.FileInfo) bool {
	if fi == nil {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Nlink > 1
}
