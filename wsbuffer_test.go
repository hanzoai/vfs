package vfs

import (
	"bytes"
	"testing"
)

type mockStorage struct {
	data []byte
}

func (m *mockStorage) WriteAt(p []byte, off int64) (n int, err error) {
	end := off + int64(len(p))
	if end > int64(len(m.data)) {
		newSize := end
		newData := make([]byte, newSize)
		copy(newData, m.data)
		m.data = newData
	}
	copy(m.data[off:end], p)
	return len(p), nil
}

func (m *mockStorage) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(m.data)) {
		return 0, nil
	}
	n = copy(p, m.data[off:])
	return n, nil
}

func TestWSBufferCorrectness(t *testing.T) {
	storage := &mockStorage{data: make([]byte, 1024)}
	ws := NewWSBuffer(storage, 1024*1024)

	// 1. Small unaligned write (should be absorbed)
	data1 := []byte("hello")
	ws.WriteAt(data1, 10)

	// Verify it's NOT in storage yet
	temp := make([]byte, 5)
	storage.ReadAt(temp, 10)
	if bytes.Equal(temp, data1) {
		t.Fatal("Data should have been buffered, not written to storage")
	}

	// 2. Read back (should be consistent)
	ws.ReadAt(temp, 10)
	if !bytes.Equal(temp, data1) {
		t.Fatalf("Expected %s, got %s", data1, temp)
	}

	// 3. Large aligned write (should bypass and flush)
	data2 := make([]byte, 8192)
	for i := range data2 {
		data2[i] = 'A'
	}
	ws.WriteAt(data2, 4096)

	// 4. Flush and verify
	ws.Flush()
	final := make([]byte, 5)
	storage.ReadAt(final, 10)
	if !bytes.Equal(final, data1) {
		t.Fatal("Data not found in storage after flush")
	}
}
