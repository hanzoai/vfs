//go:build !unix

package modeldist

import "os"

func hasMultipleHardLinks(os.FileInfo) bool {
	return false
}
