package journal_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs/journal"
)

func TestOnlyOurTempFilesAreRemoved(t *testing.T) {
	ours := []string{"000003.seg.tmp", "000000.seg.tmp", "7.seg.tmp"}
	theirs := []string{"notes.tmp", "000003.tmp", "x.seg.tmp", ".seg.tmp", ".tmp", "000003.seg.tmp.tmp", "000004.SEG.tmp", "a.tmp", "000003.seg.bak"}
	for _, refused := range []bool{false, true} {
		dir := filepath.Join(scratch(t), "j")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, n := range append(append([]string(nil), ours...), theirs...) {
			if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if refused {
			os.WriteFile(filepath.Join(dir, "000001.seg"), bytes.Repeat([]byte("NOTMINE!"), 512), 0o600)
		}
		opt := journal.Options{Capacity: 4 * 4096, Segment: 4096}
		j, err := journal.Open(dir, opt)
		if refused != (err != nil) || (err != nil && !errors.Is(err, journal.ErrFormat)) {
			t.Fatalf("refused=%v: Open %v", refused, err)
		}
		if err == nil {
			j.Close()
		}
		for _, n := range theirs {
			if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
				t.Errorf("refused=%v: %s was removed: %v", refused, n, err)
			}
		}
		for _, n := range ours {
			_, err := os.Stat(filepath.Join(dir, n))
			if err == nil && !refused {
				t.Errorf("refused=%v: our stale %s is still there after a normal Open", refused, n)
			}
			// A refused Open does remove the stale temporary file of a Create of its
			// own: that is not a segment, nor anything an operator wrote.
		}
	}
}
