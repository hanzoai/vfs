package rosenbridge

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

// OpCode identifies an I/O operation across the virtualization boundary.
type OpCode uint8

const (
	OpRead  OpCode = 1
	OpWrite OpCode = 2
	OpStat  OpCode = 3
)

func (op OpCode) String() string {
	switch op {
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpStat:
		return "stat"
	default:
		return fmt.Sprintf("op(%d)", uint8(op))
	}
}

// Descriptor flags.
const (
	FlagSync      uint32 = 1 << 0 // fsync after write
	FlagVerifyCRC uint32 = 1 << 1 // compute/verify Castagnoli CRC32C over slab payload
)

// MaxFileSize matches vfs.MaxSize (4 EiB) to guarantee int64 offset safety.
const MaxFileSize uint64 = 1 << 62

// Descriptor is a fixed-intent submission entry passed across a tenant's
// shared ring buffer pointing directly into that tenant's mmap SlabPool.
type Descriptor struct {
	Seq         uint64
	Op          OpCode
	Flags       uint32
	TenantID    string
	TenantToken uint64
	Path        string
	FileOffset  uint64
	SlabOffset  uint64
	Length      uint64
	DeclaredLen uint64
	CRC32C      uint32
}

// Completion carries the host worker's result back to the guest caller.
type Completion struct {
	Seq       uint64
	BytesDone int
	Syscalls  int
	CRC32C    uint32
	Err       error
}

// ValidateDescriptor strictly validates a guest-submitted descriptor before the
// host accesses the tenant's SlabPool or BackingStore. Every check uses
// overflow-safe unsigned arithmetic and fails closed.
func ValidateDescriptor(d Descriptor, ownerTenant string, ownerToken uint64, slabSize, maxSlotSize, maxFileSize uint64) error {
	if d.TenantID != ownerTenant || d.TenantToken != ownerToken {
		return fmt.Errorf("%w: descriptor tenant %q does not match channel owner %q", ErrTenantMismatch, d.TenantID, ownerTenant)
	}
	if d.Op != OpRead && d.Op != OpWrite {
		return fmt.Errorf("%w: %s", ErrUnsupportedOp, d.Op)
	}
	if d.Length == 0 || d.DeclaredLen != d.Length {
		return fmt.Errorf("%w: length=%d declared=%d", ErrDescriptorTruncated, d.Length, d.DeclaredLen)
	}
	if maxSlotSize > 0 && d.Length > maxSlotSize {
		return fmt.Errorf("%w: length %d exceeds max slot payload %d", ErrDescriptorOutOfBounds, d.Length, maxSlotSize)
	}
	if d.Length > slabSize || d.SlabOffset > slabSize-d.Length {
		return fmt.Errorf("%w: slab [%d, +%d) exceeds slab size %d", ErrDescriptorOutOfBounds, d.SlabOffset, d.Length, slabSize)
	}
	if maxFileSize == 0 || maxFileSize > math.MaxInt64 {
		maxFileSize = math.MaxInt64
	}
	if d.Length > maxFileSize || d.FileOffset > maxFileSize-d.Length {
		return fmt.Errorf("%w: file [%d, +%d) overflows max file size %d", ErrDescriptorOutOfBounds, d.FileOffset, d.Length, maxFileSize)
	}
	if _, err := NormalizeVirtualPath(d.Path); err != nil {
		return err
	}
	return nil
}

type ringEntry struct {
	desc Descriptor
	done chan Completion
}

// Ring is a bounded, power-of-two shared-memory submission ring with cache-line
// padded atomic head/tail indices and coalesced doorbell notifications.
//
// Each Ring belongs exclusively to a single tenant and is never shared across
// tenants. Multiple goroutines within the same tenant synchronize via prodMu on
// Submit, while the single host consumer drains batches lock-free via atomic
// head/tail ordering.
type Ring struct {
	tenantID string
	cap      uint64
	mask     uint64
	slots    []ringEntry

	// Cache-line padded atomic indices to eliminate false sharing between
	// guest producer(s) and host consumer.
	_    [56]byte
	tail atomic.Uint64
	_    [56]byte
	head atomic.Uint64
	_    [56]byte

	prodMu   sync.Mutex
	seq      atomic.Uint64
	closed   atomic.Bool
	doorbell chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once

	compPool sync.Pool
}

// NewRing creates a per-tenant Ring with capacity rounded up to a power of two.
func NewRing(tenantID string, capacity int) (*Ring, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	if capacity < 2 {
		capacity = 2
	}
	if capacity > 65536 {
		capacity = 65536
	}
	pow2 := uint64(1)
	for pow2 < uint64(capacity) {
		pow2 <<= 1
	}
	r := &Ring{
		tenantID: tenantID,
		cap:      pow2,
		mask:     pow2 - 1,
		slots:    make([]ringEntry, pow2),
		doorbell: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}
	r.compPool.New = func() any {
		return make(chan Completion, 1)
	}
	return r, nil
}

// Capacity returns the power-of-two slot capacity of the ring.
func (r *Ring) Capacity() int { return int(r.cap) }

// Depth returns the current number of unconsumed descriptors in the ring.
func (r *Ring) Depth() int {
	t := r.tail.Load()
	h := r.head.Load()
	if t <= h {
		return 0
	}
	return int(t - h)
}

// Doorbell returns the coalesced notification channel signaled on submission.
func (r *Ring) Doorbell() <-chan struct{} { return r.doorbell }

// StopCh is closed when the ring is shut down.
func (r *Ring) StopCh() <-chan struct{} { return r.stopCh }

// Submit non-blockingly enqueues desc onto the ring and rings the coalesced
// doorbell. Returns ErrRingFull immediately if saturated so callers can fall
// back to the canonical gofer path without stalling.
func (r *Ring) Submit(desc Descriptor) (chan Completion, uint64, error) {
	r.prodMu.Lock()
	if r.closed.Load() {
		r.prodMu.Unlock()
		return nil, 0, ErrRingClosed
	}
	t := r.tail.Load()
	h := r.head.Load()
	if t-h >= r.cap {
		r.prodMu.Unlock()
		return nil, 0, ErrRingFull
	}
	seq := r.seq.Add(1)
	desc.Seq = seq
	ch := r.compPool.Get().(chan Completion)
	r.slots[t&r.mask] = ringEntry{desc: desc, done: ch}
	r.tail.Store(t + 1)
	r.prodMu.Unlock()

	select {
	case r.doorbell <- struct{}{}:
	default:
	}
	return ch, seq, nil
}

// ReleaseCompletionChan returns a drained completion channel to the pool.
func (r *Ring) ReleaseCompletionChan(ch chan Completion) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
	}
	r.compPool.Put(ch)
}

// PopBatch drains up to len(dst) submitted entries from the ring. Must be
// called only by the single host consumer goroutine.
func (r *Ring) PopBatch(dst []ringEntry) int {
	h := r.head.Load()
	t := r.tail.Load()
	avail := t - h
	if avail == 0 {
		return 0
	}
	n := int(avail)
	if n > len(dst) {
		n = len(dst)
	}
	for i := 0; i < n; i++ {
		idx := (h + uint64(i)) & r.mask
		dst[i] = r.slots[idx]
		r.slots[idx] = ringEntry{}
	}
	r.head.Store(h + uint64(n))
	return n
}

// Close stops further submissions and signals the consumer to drain and exit.
func (r *Ring) Close() {
	r.prodMu.Lock()
	r.closed.Store(true)
	r.prodMu.Unlock()
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}
