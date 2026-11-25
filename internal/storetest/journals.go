package storetest

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/hanzoai/vfs/journal"
)

var (
	journalsOnce sync.Once
	journalsDir  string
)

// Journals returns a directory for the journals of the Volumes of a test binary: the
// operating system's temporary directory, or, where that is memory (the journal refuses
// it), a directory beside the tests. StopReal removes it. A Volume's journal is removed
// when the Volume is closed with nothing unpublished; the rest is what a test left.
func Journals() string {
	journalsOnce.Do(func() {
		dir, err := os.MkdirTemp("", "vfs-journals-")
		if err != nil {
			panic(err)
		}
		probe, err := journal.Files(filepath.Join(dir, "probe"))
		if err == nil {
			probe.Close()
			os.Remove(filepath.Join(dir, "probe"))
			journalsDir = dir
			return
		}
		os.RemoveAll(dir)
		if !errors.Is(err, journal.ErrVolatile) {
			panic(err)
		}
		wd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		if journalsDir, err = os.MkdirTemp(wd, ".scratch-journals-"); err != nil {
			panic(err)
		}
	})
	return journalsDir
}

// Disk is the journal's file layer for the tests of a Volume: the operating system's files with the
// disk sync left out. A test that ends a process loses nothing that the page cache holds, a sync is
// what a test of a Volume does a thousand times and does not measure, and the journal's own tests
// run the real one.
func Disk(dir string) (journal.Disk, error) {
	d, err := journal.Files(dir)
	if err != nil {
		return nil, err
	}
	return unsynced{d}, nil
}

type unsynced struct{ journal.Disk }

func (d unsynced) Create(name string, size int64) (journal.File, error) {
	f, err := d.Disk.Create(name, size)
	if err != nil {
		return nil, err
	}
	return quiet{f}, nil
}

func (d unsynced) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	if err != nil {
		return nil, err
	}
	return quiet{f}, nil
}

type quiet struct{ journal.File }

func (quiet) Sync() error { return nil }
