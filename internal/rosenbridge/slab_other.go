//go:build !unix

package rosenbridge

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// PageSize is the page alignment granularity enforced on shared memory slabs.
const PageSize = 4096

type slabSpan struct {
	start uint64
	end   uint64
}

// SlabPool is a strictly per-tenant, page-aligned slab region (portable fallback
// for non-Unix platforms using a page-aligned Go backing slice).
type SlabPool struct {
	tenantID    string
	tenantToken uint64
	slotSize    uint64
	numSlots    int
	totalSize   uint64

	raw  []byte
	data []byte

	mu        sync.Mutex
	closed    bool
	slotInUse []bool
	active    []slabSpan

	openMmaps atomic.Int64
	openFDs   atomic.Int64
}

// NewSlabPool allocates an isolated, page-aligned slab pool for a single tenant.
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
	const maxPoolBytes = 256 << 20
	if alignedSlot > maxPoolBytes/uint64(numSlots) {
		return nil, fmt.Errorf("%w: requested slab pool exceeds %d bytes", ErrDescriptorOutOfBounds, maxPoolBytes)
	}
	totalSize := alignedSlot * uint64(numSlots)

	raw := make([]byte, totalSize+PageSize)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	pad := (PageSize - (addr % PageSize)) % PageSize
	data := raw[pad : pad+uintptr(totalSize)]

	p := &SlabPool{
		tenantID:    tenantID,
		tenantToken: tenantToken,
		slotSize:    alignedSlot,
		numSlots:    numSlots,
		totalSize:   totalSize,
		raw:         raw,
		data:        data,
		slotInUse:   make([]bool, numSlots),
	}
	return p, nil
}

func (p *SlabPool) TenantID() string { return p.tenantID }
func (p *SlabPool) Size() uint64     { return p.totalSize }
func (p *SlabPool) SlotSize() uint64 { return p.slotSize }
func (p *SlabPool) NumSlots() int    { return p.numSlots }

func (p *SlabPool) IsPageAligned() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.data) == 0 {
		return false
	}
	return uintptr(unsafe.Pointer(&p.data[0]))%PageSize == 0
}

func (p *SlabPool) OpenResources() (int64, int64) {
	return p.openMmaps.Load(), p.openFDs.Load()
}

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

func (p *SlabPool) ReleaseSlot(slotIdx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if slotIdx >= 0 && slotIdx < len(p.slotInUse) {
		p.slotInUse[slotIdx] = false
	}
}

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

func (p *SlabPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	data := p.data
	p.data = nil
	p.raw = nil
	p.active = nil
	p.mu.Unlock()
	if data != nil {
		clear(data)
	}
	return nil
}
