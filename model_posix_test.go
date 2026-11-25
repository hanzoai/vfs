package vfs_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
)

// A plain model of a POSIX tree: nodes that hold their bytes in a slice and their names in a
// map. TestModelPosix drives a Volume and the model with the same random operations (create,
// write, truncate, punch, rename with each flag, link, symlink, unlink, attributes, unlink
// while open) and requires, at every step, the same answer from both, and after each Flush,
// reopen and takeover the same tree: names, inode numbers, types, modes, owners, link
// counts, sizes, bytes and the times that were set. Check, the namespace's own invariants,
// runs with it.

type pnode struct {
	ino      vfs.InodeID
	mode     fs.FileMode // the type and the permission bits
	owned    bool
	uid, gid uint32
	data     []byte // a regular file's bytes, a symbolic link's target
	kids     map[string]*pnode
	atime    time.Time
	mtime    time.Time
	timed    bool // atime and mtime are what a test set, and nothing since has moved them
	open     int  // handles held on the Volume
}

func (n *pnode) dir() bool { return n.mode.IsDir() }

type phandle struct {
	f *vfs.File
	n *pnode
}

type pmodel struct {
	root *pnode
	hs   []phandle
}

// each calls fn for every name in the tree, parents before children, in name order.
func (m *pmodel) each(fn func(dir *pnode, name string, n *pnode)) {
	var walk func(d *pnode)
	walk = func(d *pnode) {
		names := make([]string, 0, len(d.kids))
		for k := range d.kids {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			fn(d, k, d.kids[k])
			if d.kids[k].dir() {
				walk(d.kids[k])
			}
		}
	}
	walk(m.root)
}

func (m *pmodel) nlink(n *pnode) uint32 {
	if n.dir() {
		c := uint32(2)
		for _, k := range n.kids {
			if k.dir() {
				c++
			}
		}
		return c
	}
	var c uint32
	m.each(func(_ *pnode, _ string, k *pnode) {
		if k == n {
			c++
		}
	})
	return c
}

// within reports whether d is a or lies below it.
func (m *pmodel) within(d, a *pnode) bool {
	if d == a {
		return true
	}
	found := false
	var walk func(x *pnode)
	walk = func(x *pnode) {
		for _, k := range x.kids {
			if k == d {
				found = true
			}
			if k.dir() {
				walk(k)
			}
		}
	}
	walk(a)
	return found
}

// touched says the times of n are no longer the ones a test set.
func (n *pnode) touched() { n.timed = false }

// class names an error the way the model says it.
func class(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, fs.ErrExist):
		return "exist"
	case errors.Is(err, fs.ErrNotExist):
		return "notexist"
	case errors.Is(err, vfs.ErrNotDir):
		return "notdir"
	case errors.Is(err, vfs.ErrIsDir):
		return "isdir"
	case errors.Is(err, vfs.ErrNotEmpty):
		return "notempty"
	case errors.Is(err, fs.ErrPermission):
		return "perm"
	case errors.Is(err, fs.ErrInvalid):
		return "invalid"
	}
	return err.Error()
}

// renameClass is what the model says Rename does to the tree, and the class of its error.
func (m *pmodel) rename(od *pnode, on string, nd *pnode, nn string, fl vfs.RenameFlags) string {
	src := od.kids[on]
	if src == nil {
		return "notexist"
	}
	dst := nd.kids[nn]
	if fl&vfs.RenameExchange != 0 && dst == nil {
		return "notexist"
	}
	if fl&vfs.RenameNoReplace != 0 && dst != nil {
		return "exist"
	}
	if dst == src {
		return "ok"
	}
	if src.dir() && m.within(nd, src) {
		return "invalid"
	}
	if fl&vfs.RenameExchange != 0 && dst.dir() && m.within(od, dst) {
		return "invalid"
	}
	switch {
	case dst == nil, fl&vfs.RenameExchange != 0:
	case src.dir() && !dst.dir():
		return "notdir"
	case !src.dir() && dst.dir():
		return "isdir"
	case dst.dir() && len(dst.kids) > 0:
		return "notempty"
	}
	if fl&vfs.RenameExchange != 0 {
		od.kids[on], nd.kids[nn] = dst, src
	} else {
		delete(od.kids, on)
		nd.kids[nn] = src
	}
	od.touched()
	nd.touched()
	return "ok"
}

func TestModelPosixMany(t *testing.T) {
	if testing.Short() {
		t.Skip("TestModelPosix runs the same model; the fuzz step runs more seeds")
	}
	for seed := 100; seed < 160; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { posixRun(t, uint64(seed)) })
	}
}

func TestModelPosix(t *testing.T) {
	seeds := 20
	if testing.Short() {
		seeds = 6
	}
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { posixRun(t, uint64(seed)) })
	}
}

func posixRun(t *testing.T, seed uint64) {
	r := rand.New(rand.NewPCG(seed, 4242))
	be := storetest.NewMem()
	c := vfsAge(t)
	clk := &tick{}
	mk := func(round uint64) *vfs.Volume {
		cf := cfg(be, c, round)
		cf.Clock = clk
		switch seed % 3 {
		case 0:
			cf.Dirty, cf.Overlay, cf.Cache = 1<<20, 40, 2<<20
		case 1:
			cf.Dirty, cf.Overlay, cf.Cache = 200<<10, 7, 1<<20
		}
		if round == 0 {
			if err := vfs.Create(ctx, cf); err != nil {
				t.Fatal(err)
			}
			return nil
		}
		v, err := vfs.Attach(ctx, cf)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	mk(0)
	round := uint64(1)
	v := mk(round)
	m := &pmodel{root: &pnode{ino: vfs.RootInode, mode: fs.ModeDir | 0o755, kids: map[string]*pnode{}}}

	rnd := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		return b
	}
	size := func() int {
		switch r.IntN(8) {
		case 0:
			return r.IntN(64)
		case 1, 2:
			return 16<<10 - 40 + r.IntN(80)
		case 3:
			return r.IntN(70 << 10)
		case 4:
			return 64<<10 - 3 + r.IntN(6)
		case 5:
			return r.IntN(300 << 10)
		}
		return r.IntN(40 << 10)
	}
	type at struct {
		dir  *pnode
		name string
		n    *pnode
	}
	names := func() []at {
		var out []at
		m.each(func(d *pnode, name string, n *pnode) { out = append(out, at{d, name, n}) })
		return out
	}
	dirs := func() []*pnode {
		out := []*pnode{m.root}
		m.each(func(_ *pnode, _ string, n *pnode) {
			if n.dir() {
				out = append(out, n)
			}
		})
		return out
	}
	pickName := func() (at, bool) {
		all := names()
		if len(all) == 0 {
			return at{}, false
		}
		return all[r.IntN(len(all))], true
	}
	pickFile := func() (at, bool) {
		var files []at
		for _, a := range names() {
			if a.n.mode&fs.ModeType == 0 {
				files = append(files, a)
			}
		}
		if len(files) == 0 {
			return at{}, false
		}
		return files[r.IntN(len(files))], true
	}
	pickDir := func() *pnode { all := dirs(); return all[r.IntN(len(all))] }
	fresh := func(d *pnode) string {
		for {
			name := fmt.Sprintf("n%d", r.IntN(40))
			if d.kids[name] == nil {
				return name
			}
			if r.IntN(3) == 0 {
				return fmt.Sprintf("n%d", r.IntN(1<<20))
			}
		}
	}
	// open returns a handle on a regular file's inode, or fails the test.
	openFile := func(n *pnode) *vfs.File {
		f, err := v.Open(ctx, n.ino)
		if err != nil {
			t.Fatalf("open inode %d: %v", n.ino, err)
		}
		return f
	}
	mkOwner := func() []vfs.Option {
		if r.IntN(2) == 0 {
			return nil
		}
		return []vfs.Option{vfs.Owner(uint32(r.IntN(3)), uint32(r.IntN(3)))}
	}
	// compare checks the whole tree of vol against the model.
	compare := func(what string, vol *vfs.Volume, withHandles bool) {
		t.Helper()
		var walk func(d *pnode, path string)
		walk = func(d *pnode, path string) {
			ents, err := vol.ReadDir(ctx, d.ino)
			if err != nil {
				t.Fatalf("%s: readdir %s: %v", what, path, err)
			}
			if len(ents) != len(d.kids) {
				var got []string
				for _, e := range ents {
					got = append(got, e.Name)
				}
				t.Fatalf("%s: %s has %v, the model has %d names", what, path, got, len(d.kids))
			}
			for _, e := range ents {
				n := d.kids[e.Name]
				if n == nil {
					t.Fatalf("%s: %s/%s is not in the model", what, path, e.Name)
				}
				p := path + "/" + e.Name
				in, err := vol.Lookup(ctx, d.ino, e.Name)
				if err != nil {
					t.Fatalf("%s: lookup %s: %v", what, p, err)
				}
				if in.ID != n.ino || e.ID != n.ino {
					t.Fatalf("%s: %s is inode %d, the model says %d", what, p, in.ID, n.ino)
				}
				if in.Mode != n.mode || e.Mode != n.mode&fs.ModeType {
					t.Fatalf("%s: %s has mode %v (entry %v), the model says %v", what, p, in.Mode, e.Mode, n.mode)
				}
				if in.Nlink != m.nlink(n) {
					t.Fatalf("%s: %s has %d links, the model says %d", what, p, in.Nlink, m.nlink(n))
				}
				if in.Owned != n.owned || n.owned && (in.UID != n.uid || in.GID != n.gid) {
					t.Fatalf("%s: %s owner %v %d:%d, model %v %d:%d", what, p, in.Owned, in.UID, in.GID, n.owned, n.uid, n.gid)
				}
				if n.timed && (!in.Mtime.Equal(n.mtime) || !in.Atime.Equal(n.atime)) {
					t.Fatalf("%s: %s times %v %v, set to %v %v", what, p, in.Atime, in.Mtime, n.atime, n.mtime)
				}
				switch {
				case n.dir():
					if par, err := vol.Parent(ctx, n.ino); err != nil || par != d.ino {
						t.Fatalf("%s: parent of %s is %d %v, want %d", what, p, par, err, d.ino)
					}
					walk(n, p)
				case n.mode&fs.ModeSymlink != 0:
					if got, err := vol.Readlink(ctx, n.ino); err != nil || got != string(n.data) || in.Size != uint64(len(n.data)) {
						t.Fatalf("%s: link %s is %q %v, want %q", what, p, got, err, n.data)
					}
				case n.mode&fs.ModeType == 0:
					if in.Size != uint64(len(n.data)) {
						t.Fatalf("%s: %s is %d bytes, the model has %d", what, p, in.Size, len(n.data))
					}
					mustEqual(t, what+" "+p, readAll(t, vol, n.ino), n.data)
				}
			}
		}
		walk(m.root, "")
		if err := vol.Check(ctx); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if withHandles {
			for _, h := range m.hs {
				if h.n.mode&fs.ModeType != 0 {
					continue
				}
				buf := make([]byte, len(h.n.data)+3)
				n, err := h.f.ReadAt(ctx, buf, 0)
				if err != nil && err != io.EOF || n != len(h.n.data) || !bytes.Equal(buf[:n], h.n.data) {
					t.Fatalf("%s: a handle on inode %d reads %d bytes %v, the model has %d", what, h.n.ino, n, err, len(h.n.data))
				}
				if st, err := h.f.Stat(ctx); err != nil || st.Nlink != m.nlink(h.n) {
					t.Fatalf("%s: a handle on inode %d: %+v %v, links in the model %d", what, h.n.ino, st, err, m.nlink(h.n))
				}
			}
		}
	}

	dropHandles := func(closeThem bool) {
		for _, h := range m.hs {
			if closeThem {
				h.f.Close()
			}
			h.n.open--
		}
		m.hs = nil
	}

	did := map[string]int{}
	defer func() { t.Logf("operations: %v", did) }()
	for step := 0; step < 220; step++ {
		op := r.IntN(100)
		switch {
		case op < 8: // create a file with bytes
			d := pickDir()
			name := fresh(d)
			opts := mkOwner()
			in, err := v.Create(ctx, d.ino, name, 0o644, opts...)
			if err != nil {
				t.Fatalf("step %d: create: %v", step, err)
			}
			n := &pnode{ino: in.ID, mode: 0o644, kids: nil}
			if len(opts) > 0 {
				n.owned, n.uid, n.gid = in.Owned, in.UID, in.GID
				if !in.Owned {
					t.Fatalf("step %d: created with an owner and has none", step)
				}
			}
			d.kids[name] = n
			d.touched()
			if sz := size(); sz > 0 {
				data := rnd(sz)
				f := openFile(n)
				if _, err := f.WriteAt(ctx, data, 0); err != nil {
					t.Fatal(err)
				}
				f.Close()
				n.data = data
			}
		case op < 13: // mkdir
			d := pickDir()
			name := fresh(d)
			in, err := v.Mkdir(ctx, d.ino, name, 0o755)
			if err != nil {
				t.Fatalf("step %d: mkdir: %v", step, err)
			}
			d.kids[name] = &pnode{ino: in.ID, mode: fs.ModeDir | 0o755, kids: map[string]*pnode{}}
			d.touched()
		case op < 16: // symlink or pipe
			d := pickDir()
			name := fresh(d)
			if r.IntN(2) == 0 {
				target := string(rnd(1 + r.IntN(200)))
				target = fmt.Sprintf("%x", target)
				in, err := v.Symlink(ctx, d.ino, name, target)
				if err != nil {
					t.Fatalf("step %d: symlink: %v", step, err)
				}
				d.kids[name] = &pnode{ino: in.ID, mode: fs.ModeSymlink | 0o777, data: []byte(target)}
			} else {
				in, err := v.Mknod(ctx, d.ino, name, fs.ModeNamedPipe|0o600)
				if err != nil {
					t.Fatalf("step %d: mknod: %v", step, err)
				}
				d.kids[name] = &pnode{ino: in.ID, mode: fs.ModeNamedPipe | 0o600}
			}
			d.touched()
		case op < 36: // write
			a, ok := pickFile()
			if !ok {
				continue
			}
			var off int
			switch r.IntN(4) {
			case 0:
				off = len(a.n.data) + r.IntN(100<<10)
			case 1:
				off = len(a.n.data)
			default:
				if len(a.n.data) > 0 {
					off = r.IntN(len(a.n.data))
				}
			}
			data := rnd(1 + r.IntN(min(size()+1, 200<<10)))
			f := openFile(a.n)
			if _, err := f.WriteAt(ctx, data, int64(off)); err != nil {
				t.Fatalf("step %d: write: %v", step, err)
			}
			f.Close()
			if off+len(data) > len(a.n.data) {
				a.n.data = append(a.n.data, make([]byte, off+len(data)-len(a.n.data))...)
			}
			copy(a.n.data[off:], data)
			a.n.touched()
		case op < 42: // truncate
			a, ok := pickFile()
			if !ok {
				continue
			}
			sz := r.IntN(len(a.n.data) + 1)
			if r.IntN(2) == 0 {
				sz = len(a.n.data) + r.IntN(200<<10)
			}
			if err := v.Truncate(ctx, a.n.ino, uint64(sz)); err != nil {
				t.Fatalf("step %d: truncate: %v", step, err)
			}
			if sz < len(a.n.data) {
				a.n.data = a.n.data[:sz]
			} else {
				a.n.data = append(a.n.data, make([]byte, sz-len(a.n.data))...)
			}
			a.n.touched()
		case op < 48: // punch a hole
			a, ok := pickFile()
			if !ok || len(a.n.data) == 0 {
				continue
			}
			off := r.IntN(len(a.n.data))
			n := 1 + r.IntN(len(a.n.data))
			did["punch"]++
			f := openFile(a.n)
			if err := f.Punch(ctx, uint64(off), uint64(n)); err != nil {
				t.Fatalf("step %d: punch: %v", step, err)
			}
			f.Close()
			clear(a.n.data[off:min(off+n, len(a.n.data))])
			a.n.touched()
		case op < 62: // rename
			a, ok := pickName()
			if !ok {
				continue
			}
			nd := pickDir()
			nn := fresh(nd)
			switch r.IntN(4) {
			case 0: // over an existing name
				if b, ok := pickName(); ok {
					nd, nn = b.dir, b.name
				}
			case 1: // to its own name or another name of the same inode
				nd, nn = a.dir, a.name
			}
			var fl vfs.RenameFlags
			switch r.IntN(6) {
			case 0:
				fl = vfs.RenameNoReplace
			case 1:
				fl = vfs.RenameExchange
			}
			// the model works on copies of the maps it might change in a failed rename: none is changed
			want := m.rename(a.dir, a.name, nd, nn, fl)
			err := v.Rename(ctx, a.dir.ino, a.name, nd.ino, nn, fl)
			did["rename "+want]++
			if class(err) != want {
				t.Fatalf("step %d: rename %d/%s to %d/%s flags %d: %v, the model says %s", step, a.dir.ino, a.name, nd.ino, nn, fl, err, want)
			}
		case op < 68: // link
			a, ok := pickName()
			if !ok {
				continue
			}
			d := pickDir()
			name := fresh(d)
			_, err := v.Link(ctx, a.n.ino, d.ino, name)
			want := "ok"
			if a.n.dir() {
				want = "perm"
			}
			if class(err) != want {
				t.Fatalf("step %d: link: %v, the model says %s", step, err, want)
			}
			did["link "+want]++
			if err == nil {
				d.kids[name] = a.n
				d.touched()
			}
		case op < 78: // remove
			a, ok := pickName()
			if !ok {
				continue
			}
			err := v.Remove(ctx, a.dir.ino, a.name)
			want := "ok"
			if a.n.dir() && len(a.n.kids) > 0 {
				want = "notempty"
			}
			if class(err) != want {
				t.Fatalf("step %d: remove %s: %v, the model says %s", step, a.name, err, want)
			}
			did["remove "+want]++
			if a.n.open > 0 && want == "ok" && m.nlink(a.n) == 1 {
				did["unlink of an open file"]++
			}
			if err == nil {
				delete(a.dir.kids, a.name)
				a.dir.touched()
			}
		case op < 84: // attributes
			a, ok := pickName()
			if !ok {
				continue
			}
			var ch vfs.Change
			switch r.IntN(3) {
			case 0:
				mode := fs.FileMode(r.IntN(0o1000))
				if r.IntN(3) == 0 {
					mode |= fs.ModeSetuid | fs.ModeSticky
				}
				ch.Mode = &mode
			case 1:
				u, g := uint32(r.IntN(4)), uint32(r.IntN(4))
				ch.UID, ch.GID = &u, &g
			default:
				at, mt := time.Unix(int64(r.IntN(1<<30)), int64(r.IntN(1e9))), time.Unix(int64(r.IntN(1<<30)), int64(r.IntN(1e9)))
				ch.Atime, ch.Mtime = &at, &mt
			}
			in, err := v.SetAttr(ctx, a.n.ino, ch)
			if err != nil {
				t.Fatalf("step %d: setattr: %v", step, err)
			}
			switch {
			case ch.Mode != nil:
				a.n.mode = a.n.mode&fs.ModeType | *ch.Mode&(fs.ModePerm|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky)
				if in.Mode != a.n.mode {
					t.Fatalf("step %d: mode %v, the model says %v", step, in.Mode, a.n.mode)
				}
			case ch.UID != nil:
				a.n.owned, a.n.uid, a.n.gid = true, *ch.UID, *ch.GID
			default:
				a.n.atime, a.n.mtime, a.n.timed = *ch.Atime, *ch.Mtime, true
				if !in.Atime.Equal(a.n.atime) || !in.Mtime.Equal(a.n.mtime) {
					t.Fatalf("step %d: times not set", step)
				}
			}
		case op < 90: // open handles, read and write through them
			if len(m.hs) < 4 {
				if a, ok := pickFile(); ok {
					a.n.open++
					m.hs = append(m.hs, phandle{openFile(a.n), a.n})
				}
			}
			if len(m.hs) > 0 && r.IntN(2) == 0 {
				h := m.hs[r.IntN(len(m.hs))]
				data := rnd(1 + r.IntN(2000))
				off := r.IntN(len(h.n.data) + 1)
				if _, err := h.f.WriteAt(ctx, data, int64(off)); err != nil {
					t.Fatalf("step %d: write through a handle: %v", step, err)
				}
				if off+len(data) > len(h.n.data) {
					h.n.data = append(h.n.data, make([]byte, off+len(data)-len(h.n.data))...)
				}
				copy(h.n.data[off:], data)
				h.n.touched()
			}
		case op < 93: // close a handle
			if len(m.hs) > 0 {
				i := r.IntN(len(m.hs))
				m.hs[i].f.Close()
				m.hs[i].n.open--
				m.hs = slices.Delete(m.hs, i, i+1)
			}
		case op < 96:
			if err := v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			did["cold"]++
			compare(fmt.Sprintf("seed %d step %d cold", seed, step), reopen(t, be, c), false)
		case op < 98: // takeover: the handles of the old Volume are dropped with it
			if err := v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			dropHandles(false)
			did["takeover"]++
			round++
			v = mk(round)
		default:
			// nothing: the step is a comparison
		}
		compare(fmt.Sprintf("seed %d step %d live", seed, step), v, true)
	}
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	dropHandles(true)
	if st := v.Stats(); st.Dirty != 0 || st.Pending != 0 {
		t.Fatalf("after Flush: Dirty=%d Pending=%d", st.Dirty, st.Pending)
	}
	compare("final", v, false)
	compare("final cold", reopen(t, be, c), false)
}
