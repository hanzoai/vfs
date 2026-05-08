package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file" // registers file://
	_ "github.com/hanzoai/vfs/pkg/backend/s3"   // registers s3://
	"github.com/hanzoai/vfs/pkg/mount"
	"github.com/hanzoai/vfs/pkg/vfs"

	"github.com/luxfi/age"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	root := &cobra.Command{
		Use:     "vfs",
		Short:   "S3-backed virtual block filesystem with PQ encryption",
		Version: version,
	}

	var (
		backendURL  string
		ageRecips   []string
		ageKeyPath  string
		cacheMax    int64
	)

	root.PersistentFlags().StringVar(&backendURL, "backend", "", "backend URL (file://path, s3://bucket/prefix)")
	root.PersistentFlags().StringSliceVar(&ageRecips, "age-recipient", nil, "age recipient (repeatable)")
	root.PersistentFlags().StringVar(&ageKeyPath, "age-key", "", "path to age identity file (or VFS_AGE_KEY env)")
	root.PersistentFlags().Int64Var(&cacheMax, "cache-bytes", 256<<20, "in-memory cache cap (bytes); <=0 disables eviction")

	openVFS := func() (*vfs.VFS, error) {
		if backendURL == "" {
			return nil, errors.New("--backend is required")
		}
		be, err := backend.Open(ctx, backendURL)
		if err != nil {
			return nil, err
		}
		recs, ids, err := loadAge(ageRecips, ageKeyPath)
		if err != nil {
			return nil, err
		}
		c, err := vfs.NewCrypto(recs, ids)
		if err != nil {
			return nil, err
		}
		return vfs.New(vfs.Config{
			Backend:  be,
			Crypto:   c,
			CacheMax: cacheMax,
		})
	}

	// vfs put <path>  → prints block ID
	put := &cobra.Command{
		Use:   "put <file>",
		Short: "Encrypt + store a single block (file ≤ 4 KiB)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			v, err := openVFS()
			if err != nil {
				return err
			}
			defer v.Close()
			data, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}
			if len(data) > vfs.BlockSize {
				return fmt.Errorf("payload %d bytes exceeds BlockSize %d (multi-block files land in 0.2.0)",
					len(data), vfs.BlockSize)
			}
			id, err := v.PutBlock(ctx, data)
			if err != nil {
				return err
			}
			fmt.Println(id)
			return nil
		},
	}

	// vfs get <id>  → writes plaintext to stdout
	get := &cobra.Command{
		Use:   "get <id>",
		Short: "Fetch + decrypt a block by ID",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			v, err := openVFS()
			if err != nil {
				return err
			}
			defer v.Close()
			id := vfs.BlockID(args[0])
			pt, err := v.GetBlock(ctx, id)
			if err != nil {
				return err
			}
			_, err = io.Copy(os.Stdout, strings.NewReader(string(pt)))
			return err
		},
	}

	// vfs stats
	stats := &cobra.Command{
		Use:   "stats",
		Short: "Print cache + backend stats",
		RunE: func(_ *cobra.Command, _ []string) error {
			v, err := openVFS()
			if err != nil {
				return err
			}
			defer v.Close()
			s := v.Stats()
			fmt.Printf("backend:        %s\n", s.Backend)
			fmt.Printf("cache blocks:   %d\n", s.CacheBlocks)
			fmt.Printf("cache bytes:    %d\n", s.CacheBytes)
			fmt.Printf("cache cap:      %d\n", s.CacheMax)
			return nil
		},
	}

	// vfs mount <mountpoint>
	mountCmd := &cobra.Command{
		Use:   "mount <mountpoint>",
		Short: "Mount the VFS as a FUSE filesystem (requires fuse build tag)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			v, err := openVFS()
			if err != nil {
				return err
			}
			defer v.Close()
			return mount.Mount(ctx, v, args[0])
		},
	}

	root.AddCommand(put, get, stats, mountCmd)

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func loadAge(recipients []string, keyPath string) ([]age.Recipient, []age.Identity, error) {
	var recs []age.Recipient
	for _, r := range recipients {
		parsed, err := age.ParseRecipients(strings.NewReader(r))
		if err != nil {
			return nil, nil, fmt.Errorf("parse recipient %q: %w", r, err)
		}
		recs = append(recs, parsed...)
	}

	var ids []age.Identity
	keyData := os.Getenv("VFS_AGE_KEY")
	if keyPath != "" {
		b, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("read --age-key %q: %w", keyPath, err)
		}
		keyData = string(b)
	}
	if keyData != "" {
		parsed, err := age.ParseIdentities(strings.NewReader(keyData))
		if err != nil {
			return nil, nil, fmt.Errorf("parse identities: %w", err)
		}
		ids = parsed
	}

	if len(recs) == 0 && len(ids) == 0 {
		return nil, nil, errors.New("at least one --age-recipient or --age-key (read-side) required")
	}
	return recs, ids, nil
}

// Suppress unused-import lint when build tags strip references.
var _ = hex.EncodeToString
