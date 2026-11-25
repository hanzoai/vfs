// Command vfs creates, fills, reads and mounts a Volume.
//
// The Volume key is read from standard input, never from a file or the
// environment: `secret-tool print ... | vfs get /etc/hosts --backend ...`. One way,
// and the one that cannot leave a key in a file, in argv or in the process
// environment; it composes with any secret store that can print to a pipe. The
// input is one hybrid post-quantum identity per line; the first seals new objects,
// the others are retired keys that still open old ones.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/luxfi/age"
	"github.com/spf13/cobra"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file" // registers file://
	_ "github.com/hanzoai/vfs/pkg/backend/s3"   // registers s3://
	"github.com/hanzoai/vfs/pkg/mount"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// stdinKeys is the command line's vfs.Provider: identities, one per line.
type stdinKeys struct{ r io.Reader }

func (s stdinKeys) Cipher(context.Context) (vfs.Cipher, error) {
	var ids []*age.HybridIdentity
	sc := bufio.NewScanner(s.r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		id, err := age.ParseHybridIdentity(line)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, errors.New("no key on standard input: pipe in the Volume key (see `vfs keygen`)")
	}
	return vfs.Hybrid(ids[0], ids[1:]...), nil
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer) error {
	var (
		backendURL string
		round      uint64
		cache      int64
		journal    string
		capacity   int64
	)
	root := &cobra.Command{
		Use:           "vfs",
		Short:         "A filesystem on an object store, sealed with a post-quantum key",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetArgs(args)
	root.SetOut(stdout)
	root.PersistentFlags().StringVar(&backendURL, "backend", "", "where the Volume lives: file:///dir or s3://bucket/prefix?endpoint=host:port")
	root.PersistentFlags().Int64Var(&cache, "cache-bytes", 256<<20, "memory for decoded shards and chunks")
	root.PersistentFlags().StringVar(&journal, "journal", "", "a directory on this node's disk for the journal of a Volume that is written (default: the user cache directory); it must not be on memory")
	root.PersistentFlags().Int64Var(&capacity, "journal-bytes", 4<<30, "the most disk the journal uses; a write is refused when the store has not taken that much")

	config := func(rnd uint64) (vfs.Config, error) {
		if backendURL == "" {
			return vfs.Config{}, errors.New("--backend is required")
		}
		c, err := stdinKeys{stdin}.Cipher(ctx)
		if err != nil {
			return vfs.Config{}, err
		}
		be, err := backend.Open(ctx, backendURL)
		if err != nil {
			return vfs.Config{}, err
		}
		cfg := vfs.Config{Backend: be, Cipher: c, Round: rnd, Cache: cache, Journal: journal, JournalCapacity: capacity}
		if rnd != 0 && cfg.Journal == "" {
			dir, err := os.UserCacheDir()
			if err != nil {
				return vfs.Config{}, fmt.Errorf("--journal is required: %w", err)
			}
			cfg.Journal = filepath.Join(dir, "vfs", "journal")
		}
		return cfg, nil
	}
	// Writing commands attach at --round; reading commands only open.
	attach := func() (*vfs.Volume, error) {
		if round == 0 {
			return nil, errors.New("--round is required: the Round to claim the Volume at, above the last one used")
		}
		cfg, err := config(round)
		if err != nil {
			return nil, err
		}
		v, err := vfs.Attach(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if rec := v.Recovery(); rec.Lost || rec.Replayed > 0 {
			fmt.Fprintf(os.Stderr, "vfs: recovered %d changes from this node's journal; changes of the previous owner may be missing: %t %s\n", rec.Replayed, rec.Lost, rec.Why)
			for _, w := range rec.Report.Warnings {
				fmt.Fprintf(os.Stderr, "vfs: journal: %s\n", w)
			}
		}
		return v, nil
	}
	open := func() (*vfs.Volume, error) {
		cfg, err := config(0)
		if err != nil {
			return nil, err
		}
		return vfs.Open(ctx, cfg)
	}

	keygen := &cobra.Command{
		Use:   "keygen",
		Short: "Print a new Volume key",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			id, err := age.GenerateHybridIdentity()
			if err != nil {
				return err
			}
			fmt.Fprintln(stdout, id.String())
			return nil
		},
	}

	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Create an empty Volume at --backend",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			cfg, err := config(0)
			if err != nil {
				return err
			}
			defer cfg.Backend.Close()
			return vfs.Create(ctx, cfg)
		},
	}

	put := &cobra.Command{
		Use:   "put <file> <path>",
		Short: "Copy a local file into the Volume at <path> and commit",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			src, err := os.Open(args[0])
			if err != nil {
				return err
			}
			defer src.Close()
			v, err := attach()
			if err != nil {
				return err
			}
			defer v.Close()
			dir, name := path.Split(path.Clean("/" + args[1]))
			parent, err := mkdirAll(ctx, v, dir)
			if err != nil {
				return err
			}
			in, err := v.Lookup(ctx, parent, name)
			switch {
			case errors.Is(err, fs.ErrNotExist):
				in, err = v.Create(ctx, parent, name, 0o644)
			case err == nil:
				err = v.Truncate(ctx, in.ID, 0)
			}
			if err != nil {
				return err
			}
			f, err := v.Open(ctx, in.ID)
			if err != nil {
				return err
			}
			defer f.Close()
			buf := make([]byte, 1<<20)
			var off int64
			for {
				n, rerr := src.Read(buf)
				if n > 0 {
					if _, err := f.WriteAt(ctx, buf[:n], off); err != nil {
						return err
					}
					off += int64(n)
				}
				if rerr == io.EOF {
					break
				}
				if rerr != nil {
					return rerr
				}
			}
			return v.Flush(ctx)
		},
	}

	get := &cobra.Command{
		Use:   "get <path>",
		Short: "Write a file of the Volume to standard output",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			v, err := open()
			if err != nil {
				return err
			}
			defer v.Close()
			in, err := v.Resolve(ctx, args[0])
			if err != nil {
				return err
			}
			f, err := v.Open(ctx, in.ID)
			if err != nil {
				return err
			}
			defer f.Close()
			buf := make([]byte, 1<<20)
			for off := int64(0); ; {
				n, rerr := f.ReadAt(ctx, buf, off)
				if _, err := stdout.Write(buf[:n]); err != nil {
					return err
				}
				off += int64(n)
				if rerr == io.EOF {
					return nil
				}
				if rerr != nil {
					return rerr
				}
			}
		},
	}

	ls := &cobra.Command{
		Use:   "ls [path]",
		Short: "List a directory of the Volume",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			v, err := open()
			if err != nil {
				return err
			}
			defer v.Close()
			p := "/"
			if len(args) == 1 {
				p = args[0]
			}
			dir, err := v.Resolve(ctx, p)
			if err != nil {
				return err
			}
			ents, err := v.ReadDir(ctx, dir.ID)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
			for _, e := range ents {
				in, err := v.Stat(ctx, e.ID)
				if err != nil {
					return err
				}
				fmt.Fprintf(w, "%s\t%d\t%s\n", in.Mode, in.Size, e.Name)
			}
			return w.Flush()
		},
	}

	stats := &cobra.Command{
		Use:   "stats",
		Short: "Print the Volume's commit count, inodes and cache",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			v, err := open()
			if err != nil {
				return err
			}
			defer v.Close()
			s := v.Stats()
			fmt.Fprintf(stdout, "backend:      %s\ncommits:      %d\ninodes:       %d\ncache bytes:  %d\ncache cap:    %d\n",
				s.Backend, s.Seq, s.Inodes, s.CacheBytes, s.CacheMax)
			return nil
		},
	}

	var from string
	var verify bool
	reseal := &cobra.Command{
		Use:   "reseal",
		Short: "Seal the Head of a Volume that was copied to --backend to that place, and claim it at --round",
		Long: `A Head is sealed to the place it was written at. After the objects of a Volume were
copied to another bucket or prefix, reseal makes the copy open there. The old place is
--from, the URL the Volume was last used at; leave it out if it is not remembered, and the
Head is opened with the key alone: that trusts the key, and that the Head in the copy is
the newest one. Either way the copy is checked first and refused if its root shard is
missing; --verify also reads every shard the Head reaches and checks every pack is there.
Stop any writer at the old place first: reseal fences the copy only. It can be repeated at
a higher --round.`,
		Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			if round == 0 {
				return errors.New("--round is required: the Round to claim the Volume at, above the last one used")
			}
			cfg, err := config(round)
			if err != nil {
				return err
			}
			defer cfg.Backend.Close()
			cfg.Verify = verify
			if from != "" {
				old, err := backend.Open(ctx, from)
				if err != nil {
					return err
				}
				from = old.String() // the canonical place, whatever spelling the URL had
				old.Close()
			}
			return vfs.Reseal(ctx, cfg, from)
		},
	}
	reseal.Flags().StringVar(&from, "from", "", "the URL the Volume was last used at")
	reseal.Flags().BoolVar(&verify, "verify", false, "read every shard the Head reaches and check every pack it needs is there")

	mountCmd := &cobra.Command{
		Use:   "mount <mountpoint>",
		Short: "Mount the Volume as a FUSE filesystem (needs a build with -tags fuse)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			v, err := attach()
			if err != nil {
				return err
			}
			defer v.Close()
			return mount.Mount(ctx, v, args[0])
		},
	}

	for _, c := range []*cobra.Command{put, mountCmd, reseal} {
		c.Flags().Uint64Var(&round, "round", 0, "the Round to claim the Volume at; must exceed the last one used")
	}
	root.AddCommand(keygen, initCmd, put, get, ls, stats, reseal, mountCmd)
	return root.ExecuteContext(ctx)
}

// mkdirAll resolves dir from the root, creating what is missing.
func mkdirAll(ctx context.Context, v *vfs.Volume, dir string) (vfs.InodeID, error) {
	cur := vfs.RootInode
	for _, name := range strings.Split(strings.Trim(path.Clean("/"+dir), "/"), "/") {
		if name == "" {
			continue
		}
		in, err := v.Lookup(ctx, cur, name)
		if errors.Is(err, fs.ErrNotExist) {
			in, err = v.Mkdir(ctx, cur, name, 0o755)
		}
		if err != nil {
			return 0, err
		}
		cur = in.ID
	}
	return cur, nil
}
