//go:build fuse && linux

package mount_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
)

func TestMain(m *testing.M) {
	code := m.Run()
	storetest.StopReal()
	os.Exit(code)
}

// TestFUSEReadsExistingFilesOverS3 mounts a Volume that already holds files over a
// real object store and reads them back through the kernel. Every file but the one
// just created is found by Lookup, and the store's client cannot work without a
// context: a mount that hands its reads a nil one dies on the first chunk. The
// directory backend ignores its context and cannot show that.
func TestFUSEReadsExistingFilesOverS3(t *testing.T) {
	needFUSE(t)
	real := storetest.RealS3(t)
	prefix := real.Prefix("mount")
	key, _ := age.GenerateHybridIdentity()

	be, err := s3be.New(s3be.Config{Endpoint: real.Endpoint, Insecure: true, Region: "us-east-1", Bucket: real.Bucket, Prefix: prefix, AccessKey: real.AccessKey, SecretKey: real.SecretKey})
	if err != nil {
		t.Fatal(err)
	}
	bg := context.Background()
	c := vfs.Hybrid(key)
	if err := vfs.Create(bg, vfs.Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := vfs.Attach(bg, vfs.Config{Backend: be, Cipher: c, Round: 1, Journal: storetest.Journals(), JournalCapacity: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	small := []byte("hello from an existing file\n")
	big := make([]byte, 3<<20)
	_, _ = rand.Read(big)
	put := func(name string, data []byte) {
		in, err := v.Create(bg, vfs.RootInode, name, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f, _ := v.Open(bg, in.ID)
		if _, err := f.WriteAt(bg, data, 0); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	put("small.txt", small)
	put("big.bin", big)
	if err := v.Flush(bg); err != nil {
		t.Fatal(err)
	}
	v.Close()

	mp := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(mp, 0o755); err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("s3://%s/%s?endpoint=%s&insecure=1&region=us-east-1", real.Bucket, prefix, real.Endpoint)
	m := mountURL(t, url, []string{"AWS_ACCESS_KEY_ID=" + real.AccessKey, "AWS_SECRET_ACCESS_KEY=" + real.SecretKey}, mp, key, 2)
	defer watchdog(t, m, 120*time.Second)()

	got, err := os.ReadFile(filepath.Join(mp, "small.txt"))
	if err != nil || !bytes.Equal(got, small) {
		t.Fatalf("an existing small file: %q, %v", got, err)
	}
	gotBig, err := os.ReadFile(filepath.Join(mp, "big.bin"))
	if err != nil || !bytes.Equal(gotBig, big) {
		t.Fatalf("an existing chunked file: %d bytes, %v", len(gotBig), err)
	}
	// And a file written through the mount reads back and lands in the store.
	fresh := rnd(9, 1<<20)
	if err := os.WriteFile(filepath.Join(mp, "fresh.bin"), fresh, 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(mp, "fresh.bin")); err != nil || !bytes.Equal(got, fresh) {
		t.Fatalf("a file written through the mount: %d bytes, %v", len(got), err)
	}
	m.Stop(t)
}
