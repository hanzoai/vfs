package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
)

// metadataKey is the backend path where the inode tree is persisted.
// One blob per filesystem; mutated in memory, atomically replaced on Sync.
const metadataKey = "metadata/root.zap.age"

// InodeID is a monotonically-assigned per-inode handle. ID 1 is always
// the root directory.
type InodeID uint64

// RootInode is the well-known root.
const RootInode InodeID = 1

// Inode is the on-disk representation of a file or directory.
//
// We store the inode tree as a single encrypted JSON blob at
// metadataKey. For >100K files / >1M inodes this becomes too large to
// reload on every mount; in that scale we shard the metadata blob
// (`metadata/<inode_id>.zap.age`) and keep an in-memory btree. The
// shard cutover is post-1.0.
type Inode struct {
	ID       InodeID            `json:"id"`
	Name     string             `json:"name,omitempty"`
	Parent   InodeID            `json:"parent,omitempty"`
	Mode     uint32             `json:"mode"`     // POSIX mode bits
	Size     uint64             `json:"size"`     // exact bytes
	Mtime    time.Time          `json:"mtime"`
	Atime    time.Time          `json:"atime"`
	Ctime    time.Time          `json:"ctime"`
	Blocks   []BlockID          `json:"blocks,omitempty"`   // file content (ordered)
	Children map[string]InodeID `json:"children,omitempty"` // dir entries
}

// IsDir reports whether the inode is a directory.
func (i *Inode) IsDir() bool { return i.Mode&uint32(os.ModeDir) != 0 }

// metaTree is the in-memory inode index.
type metaTree struct {
	inodes map[InodeID]*Inode
	next   InodeID // next free ID
}

func newMetaTree() *metaTree {
	now := time.Now()
	root := &Inode{
		ID:       RootInode,
		Mode:     uint32(os.ModeDir | 0o755),
		Mtime:    now,
		Atime:    now,
		Ctime:    now,
		Children: map[string]InodeID{},
	}
	return &metaTree{
		inodes: map[InodeID]*Inode{RootInode: root},
		next:   RootInode + 1,
	}
}

func (m *metaTree) allocID() InodeID {
	id := m.next
	m.next++
	return id
}

// FS is a multi-file VFS layered on top of the block VFS. Inode tree
// is in memory; persisted as a single encrypted blob on Sync.
type FS struct {
	v *VFS

	mu    sync.RWMutex
	tree  *metaTree
	dirty bool // true if the tree needs to be flushed
}

// NewFS opens (or creates) a filesystem over the given block VFS. If
// the metadata blob exists on the backend, the tree is rehydrated;
// otherwise an empty filesystem with a single root directory is
// initialised.
func NewFS(ctx context.Context, v *VFS) (*FS, error) {
	if v == nil {
		return nil, fmt.Errorf("vfs.NewFS: VFS required")
	}
	fs := &FS{v: v, tree: newMetaTree()}
	if err := fs.loadMetadata(ctx); err != nil {
		return nil, err
	}
	return fs, nil
}

func (fs *FS) loadMetadata(ctx context.Context) error {
	ct, err := fs.v.be.Get(ctx, metadataKey)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil // fresh FS, keep the empty tree
		}
		return fmt.Errorf("vfs.FS: load metadata: %w", err)
	}
	if !fs.v.crypto.HasIdentities() {
		return fmt.Errorf("vfs.FS: cannot read metadata (no identities)")
	}
	pt, err := fs.v.crypto.Decrypt(ct)
	if err != nil {
		return fmt.Errorf("vfs.FS: decrypt metadata: %w", err)
	}
	var stored struct {
		Inodes map[InodeID]*Inode `json:"inodes"`
		Next   InodeID            `json:"next"`
	}
	if err := json.Unmarshal(pt, &stored); err != nil {
		return fmt.Errorf("vfs.FS: parse metadata: %w", err)
	}
	if len(stored.Inodes) == 0 {
		return fmt.Errorf("vfs.FS: metadata has zero inodes")
	}
	fs.tree.inodes = stored.Inodes
	fs.tree.next = stored.Next
	return nil
}

// Sync flushes the metadata tree to the backend if dirty.
func (fs *FS) Sync(ctx context.Context) error {
	fs.mu.Lock()
	if !fs.dirty {
		fs.mu.Unlock()
		return nil
	}
	stored := struct {
		Inodes map[InodeID]*Inode `json:"inodes"`
		Next   InodeID            `json:"next"`
	}{Inodes: fs.tree.inodes, Next: fs.tree.next}
	fs.mu.Unlock()

	pt, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("vfs.FS.Sync: marshal: %w", err)
	}
	ct, err := fs.v.crypto.Encrypt(pt)
	if err != nil {
		return fmt.Errorf("vfs.FS.Sync: encrypt: %w", err)
	}
	if err := fs.v.be.Put(ctx, metadataKey, ct); err != nil {
		return fmt.Errorf("vfs.FS.Sync: backend.Put: %w", err)
	}
	fs.mu.Lock()
	fs.dirty = false
	fs.mu.Unlock()
	return nil
}

// Lookup resolves a `/`-delimited path to an inode. Returns nil + error
// matching os.ErrNotExist when any segment is missing.
func (fs *FS) Lookup(p string) (*Inode, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	cur := fs.tree.inodes[RootInode]
	p = path.Clean("/" + p)
	if p == "/" {
		return cloneInode(cur), nil
	}
	for _, seg := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if seg == "" {
			continue
		}
		if !cur.IsDir() {
			return nil, fmt.Errorf("%w: %s is not a directory", os.ErrNotExist, cur.Name)
		}
		childID, ok := cur.Children[seg]
		if !ok {
			return nil, fmt.Errorf("%w: %s", os.ErrNotExist, p)
		}
		cur = fs.tree.inodes[childID]
		if cur == nil {
			return nil, fmt.Errorf("vfs: dangling child %s in %d", seg, childID)
		}
	}
	return cloneInode(cur), nil
}

// Mkdir creates a directory at the given path. Parent must exist.
func (fs *FS) Mkdir(p string, mode uint32) (*Inode, error) {
	dir, base := path.Split(path.Clean("/" + p))
	if base == "" {
		return nil, fmt.Errorf("vfs.Mkdir: empty name")
	}
	parent, err := fs.Lookup(dir)
	if err != nil {
		return nil, err
	}
	if !parent.IsDir() {
		return nil, fmt.Errorf("vfs.Mkdir: %s not a directory", dir)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	parentLive := fs.tree.inodes[parent.ID]
	if _, exists := parentLive.Children[base]; exists {
		return nil, fmt.Errorf("vfs.Mkdir: %s exists", p)
	}
	now := time.Now()
	id := fs.tree.allocID()
	in := &Inode{
		ID:       id,
		Name:     base,
		Parent:   parent.ID,
		Mode:     uint32(os.ModeDir) | (mode & 0o777),
		Mtime:    now,
		Atime:    now,
		Ctime:    now,
		Children: map[string]InodeID{},
	}
	fs.tree.inodes[id] = in
	parentLive.Children[base] = id
	parentLive.Mtime = now
	fs.dirty = true
	return cloneInode(in), nil
}

// Create makes a new empty regular file at path. Parent must exist.
func (fs *FS) Create(p string, mode uint32) (*Inode, error) {
	dir, base := path.Split(path.Clean("/" + p))
	if base == "" {
		return nil, fmt.Errorf("vfs.Create: empty name")
	}
	parent, err := fs.Lookup(dir)
	if err != nil {
		return nil, err
	}
	if !parent.IsDir() {
		return nil, fmt.Errorf("vfs.Create: %s not a directory", dir)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	parentLive := fs.tree.inodes[parent.ID]
	if _, exists := parentLive.Children[base]; exists {
		return nil, fmt.Errorf("vfs.Create: %s exists", p)
	}
	now := time.Now()
	id := fs.tree.allocID()
	in := &Inode{
		ID:     id,
		Name:   base,
		Parent: parent.ID,
		Mode:   mode & 0o777, // regular file
		Mtime:  now,
		Atime:  now,
		Ctime:  now,
	}
	fs.tree.inodes[id] = in
	parentLive.Children[base] = id
	parentLive.Mtime = now
	fs.dirty = true
	return cloneInode(in), nil
}

// Remove deletes a regular file or empty directory.
func (fs *FS) Remove(p string) error {
	in, err := fs.Lookup(p)
	if err != nil {
		return err
	}
	if in.ID == RootInode {
		return fmt.Errorf("vfs.Remove: cannot remove root")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	live := fs.tree.inodes[in.ID]
	if live.IsDir() && len(live.Children) > 0 {
		return fmt.Errorf("vfs.Remove: %s not empty", p)
	}
	parent := fs.tree.inodes[live.Parent]
	delete(parent.Children, live.Name)
	delete(fs.tree.inodes, live.ID)
	parent.Mtime = time.Now()
	fs.dirty = true
	// Block GC happens at Sync time / 1.0.0 — for now blocks become orphans.
	return nil
}

// ReadDir returns the children of a directory inode.
func (fs *FS) ReadDir(p string) ([]*Inode, error) {
	in, err := fs.Lookup(p)
	if err != nil {
		return nil, err
	}
	if !in.IsDir() {
		return nil, fmt.Errorf("vfs.ReadDir: %s not a directory", p)
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	live := fs.tree.inodes[in.ID]
	out := make([]*Inode, 0, len(live.Children))
	for _, cid := range live.Children {
		out = append(out, cloneInode(fs.tree.inodes[cid]))
	}
	return out, nil
}

// Open returns a File handle for the given path. The path must already
// exist (use Create first for new files).
func (fs *FS) Open(ctx context.Context, p string) (*File, error) {
	in, err := fs.Lookup(p)
	if err != nil {
		return nil, err
	}
	if in.IsDir() {
		return nil, fmt.Errorf("vfs.Open: %s is a directory", p)
	}
	return &File{fs: fs, ctx: ctx, inodeID: in.ID}, nil
}

// inodeByID returns a live inode pointer (callers MUST hold fs.mu).
func (fs *FS) inodeByID(id InodeID) *Inode { return fs.tree.inodes[id] }

// PathOfInode walks the inode tree from the root and returns the
// absolute path for the given inode ID. Returns os.ErrNotExist when
// the ID is unknown. O(depth) — used by the darwin FUSE driver, which
// gets ops as (inodeID, name) tuples instead of bazil.org/fuse's
// node-pointer style.
func (fs *FS) PathOfInode(id InodeID) (string, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if id == RootInode {
		return "/", nil
	}
	in := fs.tree.inodes[id]
	if in == nil {
		return "", os.ErrNotExist
	}
	// Build the path bottom-up.
	parts := []string{in.Name}
	cur := in
	for cur.Parent != 0 && cur.Parent != RootInode {
		cur = fs.tree.inodes[cur.Parent]
		if cur == nil {
			return "", fmt.Errorf("vfs.PathOfInode: dangling parent in chain for %d", id)
		}
		parts = append([]string{cur.Name}, parts...)
	}
	return "/" + strings.Join(parts, "/"), nil
}

func cloneInode(src *Inode) *Inode {
	if src == nil {
		return nil
	}
	cp := *src
	if src.Children != nil {
		cp.Children = make(map[string]InodeID, len(src.Children))
		for k, v := range src.Children {
			cp.Children[k] = v
		}
	}
	if src.Blocks != nil {
		cp.Blocks = append([]BlockID(nil), src.Blocks...)
	}
	return &cp
}
