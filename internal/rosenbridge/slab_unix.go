//go:build unix

package rosenbridge

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// PageSize is the page alignment granularity enforced on shared memory slabs.
const PageSize = 4096

type slabSpan struct {
	start uint64
	end   uint64 // exclusive
}

// SlabPool is a strictly per-tenant, page-aligned shared-memory slab region
// backed by an unlinked file-backed MAP_SHARED mmap(2) mapping. It prevents
// cross-tenant memory sharing by construction, tracks active in-flight ranges
// to reject overlapping descriptors, and zeroes memory before munmap on Close.
type SlabPool struct {
	tenantID    string
	tenantToken uint64
	slotSize    uint64
	numSlots    int
	totalSize   uint64

	file *os.File
	data []byte

	mu        sync.Mutex
	closed    bool
	slotInUse []bool
	active    []slabSpan

	// Diagnostic counters for zero-leak verification.
	openMmaps atomic.Int64
	openFDs   atomic.Int64
}

// NewSlabPool allocates an isolated, page-aligned mmap-backed slab pool for a
// single tenant. slotSize is rounded up to a multiple of PageSize.
func NewSlabPool(tenantID string, tenantToken uint64, numSlots int, slotSize uint64) (*SlabPool, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	if numSlots <= 0 || numSlots > 4096 {
		return nil, fmt.Errorf("%w: invalid numSlots %d", ErrDescriptorOutOfBounds, numSlots)
	}
	if slotSize == 0 {
		return nil, fmt.Errorf("%w: slotSize must be > 0", ErrDescriptorOutOfBounds)
	}
	alignedSlot := ((slotSize + PageSize - 1) / PageSize) * PageSize
	// Guard against uint64/int overflow in totalSize.
	const maxPoolBytes = 256 << 20 // 256 MiB hard safety cap per tenant slab
	if alignedSlot > maxPoolBytes/uint64(numSlots) {
		return nil, fmt.Errorf("%w: requested slab pool exceeds %d bytes", ErrDescriptorOutOfBounds, maxPoolBytes)
	}
	totalSize := alignedSlot * uint64(numSlots)

	f, err := os.CreateTemp("", "rosenbridge-slab-"+tenantID+"-*")
	if err != nil {
		return nil, fmt.Errorf("rosenbridge: create slab backing file: %w", err)
	}
	// Immediately unlink so no stale backing file survives crashes.
	_ = os.Remove(f.Name())

	if err := f.Truncate(int64(totalSize)); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("rosenbridge: truncate slab backing file: %w", err)
	}

	mapped, err := syscall.Mmap(int(f.Fd()), 0, int(totalSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("rosenbridge: mmap slab region: %w", err)
	}

	p := &SlabPool{
		tenantID:    tenantID,
		tenantToken: tenantToken,
		slotSize:    alignedSlot,
		numSlots:    numSlots,
		totalSize:   totalSize,
		file:        f,
		data:        mapped,
		slotInUse:   make([]bool, numSlots),
	}
	p.openMmaps.Store(1)
	p.openFDs.Store(1)
	return p, nil
}

// TenantID returns the sole tenant permitted to access this slab pool.
func (p *SlabPool) TenantID() string { return p.tenantID }

// Size returns the total mapped byte capacity of the slab pool.
func (p *SlabPool) Size() uint64 { return p.totalSize }

// SlotSize returns the page-aligned byte capacity of a single slot.
func (p *SlabPool) SlotSize() uint64 { return p.slotSize }

// NumSlots returns the number of page-aligned slots in the pool.
func (p *SlabPool) NumSlots() int { return p.numSlots }

// IsPageAligned reports whether the base address of the mmap slab is 4 KiB aligned.
func (p *SlabPool) IsPageAligned() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.data) == 0 {
		return false
	}
	return uintptr(unsafe.Pointer(&p.data[0]))%PageSize == 0
}

// OpenResources returns the currently held (mmapCount, fdCount) for leak testing.
func (p *SlabPool) OpenResources() (mmaps int64, fds int64) {
	return p.openMmaps.Load(), p.openFDs.Load()
}

// AcquireSlot reserves a free page-aligned slot for a transfer of length bytes.
// It returns (slotIdx, slabOffset, slice, err). If all slots are busy, it
// returns ErrRingFull so the router can fall back to the canonical gofer path.
func (p *SlabPool) AcquireSlot(tenantID string, tenantToken uint64, length uint64) (int, uint64, []byte, error) {
	if tenantID != p.tenantID || tenantToken != p.tenantToken {
		return -1, 0, nil, fmt.Errorf("%w: slab owned by %q, requested by %q", ErrTenantMismatch, p.tenantID, tenantID)
	}
	if length == 0 {
		return -1, 0, nil, fmt.Errorf("%w: zero-length slot acquisition", ErrDescriptorTruncated)
	}
	if length > p.slotSize {
		return -1, 0, nil, fmt.Errorf("%w: length %d exceeds slot size %d", ErrDescriptorOutOfBounds, length, p.slotSize)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return -1, 0, nil, ErrRingClosed
	}
	for i := 0; i < p.numSlots; i++ {
		if !p.slotInUse[i] {
			off := uint64(i) * p.slotSize
			if p.overlapsLocked(off, off+length) {
				continue
			}
			p.slotInUse[i] = true
			return i, off, p.data[off : off+length], nil
		}
	}
	return -1, 0, nil, ErrRingFull
}

// ReleaseSlot frees a previously acquired slot.
func (p *SlabPool) ReleaseSlot(slotIdx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if slotIdx >= 0 && slotIdx < len(p.slotInUse) {
		p.slotInUse[slotIdx] = false
	}
}

// ReserveRange validates bounds and marks [offset, offset+length) as actively
// in-flight for host execution, rejecting overlapping ranges with
// ErrDescriptorOverlap and returning the direct slice into the mapped slab.
func (p *SlabPool) ReserveRange(tenantID string, tenantToken uint64, offset, length uint64) ([]byte, error) {
	if tenantID != p.tenantID || tenantToken != p.tenantToken {
		return nil, fmt.Errorf("%w: slab owned by %q, descriptor claims %q", ErrTenantMismatch, p.tenantID, tenantID)
	}
	if length == 0 {
		return nil, fmt.Errorf("%w: zero-length descriptor", ErrDescriptorTruncated)
	}
	if length > p.totalSize || offset > p.totalSize-length {
		return nil, fmt.Errorf("%w: [%d, +%d) outside slab size %d", ErrDescriptorOutOfBounds, offset, length, p.totalSize)
	}

	end := offset + length
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrRingClosed
	}
	if p.overlapsLocked(offset, end) {
		return nil, fmt.Errorf("%w: [%d, %d) overlaps active descriptor", ErrDescriptorOverlap, offset, end)
	}
	p.active = append(p.active, slabSpan{start: offset, end: end})
	return p.data[offset:end], nil
}

// ReleaseRange removes a completed in-flight range reservation.
func (p *SlabPool) ReleaseRange(offset, length uint64) {
	end := offset + length
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, sp := range p.active {
		if sp.start == offset && sp.end == end {
			last := len(p.active) - 1
			p.active[i] = p.active[last]
			p.active = p.active[:last]
			return
		}
	}
}

func (p *SlabPool) overlapsLocked(start, end uint64) bool {
	for _, sp := range p.active {
		if start < sp.end && sp.start < end {
			return true
		}
	}
	return false
}

// Close zeroes the tenant's slab memory (preventing cross-tenant residual page
// disclosure), unmaps the mmap region, and closes the backing file descriptor.
func (p *SlabPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	data := p.data
	f := p.file
	p.data = nil
	p.file = nil
	p.active = nil
	p.mu.Unlock()

	var firstErr error
	if data != nil {
		clear(data)
		if err := syscall.Munmap(data); err != nil {
			firstErr = err
		}
		p.openMmaps.Add(-1)
	}
	if f != nil {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		p.openFDs.Add(-1)
	}
	return firstErr
}
