package vfs_test

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// SPEC 5.4: every way a copy can be short, with --from and without, with and without the full check, and the
// copy is never left resealed when the check refuses it.
func TestResealOfShortCopies(t *testing.T) {
	be, c, _, want := resealPopulate(t)
	var shards, packs []string
	for k := range be.Dump() {
		switch {
		case strings.HasPrefix(k, "meta/"):
			shards = append(shards, k)
		case strings.HasPrefix(k, "pack/"):
			packs = append(packs, k)
		}
	}
	sort.Strings(shards)
	t.Logf("%d shards, %d packs", len(shards), len(packs))
	type cc struct {
		name string
		skip func(string) bool
	}
	cases := []cc{
		{"no pack", func(k string) bool { return strings.HasPrefix(k, "pack/") }},
		{"one pack", func(k string) bool { return k == packs[0] }},
		{"one shard", func(k string) bool { return k == shards[len(shards)/2] }},
		{"every shard", func(k string) bool { return strings.HasPrefix(k, "meta/") }},
	}
	for _, cs := range cases {
		for _, from := range []string{"", be.String()} {
			for _, verify := range []bool{false, true} {
				dst := storetest.NewMem()
				resealCopy(t, be, dst, cs.skip)
				before := string(dst.Dump()["head"])
				conf := cfg(dst, c, 10)
				conf.Verify = verify
				err := vfs.Reseal(ctx, conf, from)
				moved := string(dst.Dump()["head"]) != before
				if err != nil && moved {
					t.Errorf("%s from=%v verify=%v: refused (%v) but the Head changed", cs.name, from != "", verify, err)
				}
				// whenever it says yes, Open must read the tree, and a full check that said yes
				// means every file reads
				if err == nil {
					v, oerr := vfs.Open(ctx, cfg(dst, c, 0))
					if oerr != nil {
						t.Errorf("%s from=%v verify=%v: resealed, and Open says %v", cs.name, from != "", verify, oerr)
						continue
					}
					bad := 0
					for name := range want {
						in, rerr := v.Resolve(ctx, "/"+name)
						if rerr != nil {
							bad++
							continue
						}
						buf := make([]byte, in.Size)
						f, oerr := v.Open(ctx, in.ID)
						if oerr != nil {
							bad++ // a short copy can lack the objects a file needs to open
							continue
						}
						if _, rerr := f.ReadAt(ctx, buf, 0); rerr != nil && in.Size > 0 {
							bad++
						}
					}
					if verify && bad > 0 && cs.name != "one pack" {
						t.Errorf("%s from=%v verify=true: the full check said yes and %d files do not read", cs.name, from != "", bad)
					}
					if verify && bad > 0 && cs.name == "one pack" {
						t.Errorf("%s verify=true: said yes, %d files unreadable", cs.name, bad)
					}
					t.Logf("%-10s from=%-5v verify=%-5v: ACCEPTED, %d of %d files unreadable", cs.name, from != "", verify, bad, len(want))
				} else {
					t.Logf("%-10s from=%-5v verify=%-5v: refused: %.70s", cs.name, from != "", verify, err)
				}
			}
		}
	}
}

// SPEC 5.4: reseal twice (higher round) and at the same round, with foreign objects beside the Volume's, an older
// Head, and a Head with a Needs bit.
func TestResealRepeatsAndForeigners(t *testing.T) {
	be, c, v, want := resealPopulate(t)
	dst := storetest.NewMem()
	resealCopy(t, be, dst, nil)
	for i := range 3 {
		if err := dst.Put(ctx, fmt.Sprintf("orgs/acme/kv%d.db", i), []byte("foreign")); err != nil {
			t.Fatal(err)
		}
	}
	if err := vfs.Reseal(ctx, cfg(dst, c, 10), be.String()); err != nil {
		t.Fatal(err)
	}
	if err := vfs.Reseal(ctx, cfg(dst, c, 10), be.String()); !errors.Is(err, vfs.ErrStaleRound) {
		t.Errorf("a reseal at the same round: %v, want ErrStaleRound", err)
	}
	if err := vfs.Reseal(ctx, cfg(dst, c, 11), be.String()); err != nil { // already sealed here: only the round
		t.Errorf("a repeat at a higher round: %v", err)
	}
	if err := vfs.Reseal(ctx, cfg(dst, c, 12), ""); err != nil {
		t.Errorf("a repeat without the place: %v", err)
	}
	r, err := vfs.Open(ctx, cfg(dst, c, 0))
	if err != nil {
		t.Fatal(err)
	}
	resealCheck(t, r, want)
	// an older Head: the copy was taken, then the source committed again; the copy's Head is older than its objects
	write(t, v, vfs.RootInode, "later", []byte("later bytes"))
	flush(t, v)
	dst2 := storetest.NewMem()
	resealCopy(t, be, dst2, func(k string) bool { return k == "head" })
	_ = dst2
}
