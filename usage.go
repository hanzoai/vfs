package vfs

// Usage is what a Volume holds, as its Head says it (SPECIFICATION 5.1, the usage field)
// and as the changes since have moved it.
type Usage struct {
	Files    uint64 // inodes: files, directories, symbolic links, pipes and sockets
	Bytes    uint64 // bytes stored for files and symbolic links; a hole costs none
	Pending  uint64 // bytes written and not yet stored: an upper bound, an overwrite counts again
	Capacity int64  // Config.Capacity: 0 for none
}

// Usage returns what the Volume holds. After a Flush, Pending is 0 and Bytes is exact.
func (v *Volume) Usage() Usage {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return Usage{Files: v.ns.files, Bytes: v.ns.bytes, Pending: uint64(max(v.dirty, 0)), Capacity: v.cfg.Capacity}
}

// used is what a write is measured against Config.Capacity with: the bytes stored and the
// bytes written since. mu is held.
func (v *Volume) used() int64 { return int64(v.ns.bytes) + v.dirty }
