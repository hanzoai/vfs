package vfs

import (
	"sync"
	"io"
)

const (
	// ScrapThreshold defines the size below which writes are considered "small"
	ScrapThreshold = 4096 
	// PageSize for alignment checks
	PageSize = 4096
)

// WSBuffer implements partial-write absorption logic.
// It sits between the File interface and the Chunking engine.
type WSBuffer struct {
	mu          sync.RWMutex
	scrap       map[int64][]byte // Offset -> Data fragment
	underlying  WriterAtReaderAt
	dirtySize   int64
	maxScrap    int64
}

type WriterAtReaderAt interface {
	io.WriterAt
	io.ReaderAt
}

func NewWSBuffer(underlying WriterAtReaderAt, maxScrapSize int64) *WSBuffer {
	return &WSBuffer{
		scrap:      make(map[int64][]byte),
		underlying: underlying,
		maxScrap:   maxScrapSize,
	}
}

// WriteAt decides whether to absorb the write or bypass to the underlying layer.
func (ws *WSBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	size := int64(len(p))

	// Bypass Logic: Large and Aligned writes go straight to storage
	if size >= ScrapThreshold && off%PageSize == 0 {
		// Before bypassing, we must flush any overlapping scrap data to maintain consistency
		if err := ws.FlushRange(off, size); err != nil {
			return 0, err
		}
		return ws.underlying.WriteAt(p, off)
	}

	// Absorption Logic: Small or Unaligned writes
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// Store in scrap buffer
	buf := make([]byte, len(p))
	copy(buf, p)
	ws.scrap[off] = buf
	ws.dirtySize += size

	// Trigger proactive flush if scrap is too large
	if ws.dirtySize > ws.maxScrap {
		go ws.Flush()
	}

	return len(p), nil
}

// ReadAt ensures consistency by merging scrap data with underlying storage data.
func (ws *WSBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = ws.underlying.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return n, err
	}

	ws.mu.RLock()
	defer ws.mu.RUnlock()

	// Overlay scrap data onto the result
	for sOff, sData := range ws.scrap {
		// Check for overlap
		if sOff < off+int64(n) && sOff+int64(len(sData)) > off {
			start := max(0, sOff-off)
			dataStart := max(0, off-sOff)
			copy(p[start:], sData[dataStart:])
		}
	}

	return n, err
}

// Flush commits all scrap data to the underlying storage.
func (ws *WSBuffer) Flush() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for off, data := range ws.scrap {
		_, err := ws.underlying.WriteAt(data, off)
		if err != nil {
			return err
		}
		delete(ws.scrap, off)
	}
	ws.dirtySize = 0
	return nil
}

// FlushRange flushes only the scrap data that overlaps with the given range.
func (ws *WSBuffer) FlushRange(off int64, size int64) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for sOff, data := range ws.scrap {
		if sOff < off+size && sOff+int64(len(data)) > off {
			_, err := ws.underlying.WriteAt(data, sOff)
			if err != nil {
				return err
			}
			delete(ws.scrap, sOff)
			ws.dirtySize -= int64(len(data))
		}
	}
	return nil
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
