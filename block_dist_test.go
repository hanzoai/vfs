package vfs

import (
	"context"
	"testing"
)

func TestDecentralizedReconstruction(t *testing.T) {
	ctx := context.Background()
	
	// 1. Setup 3 independent providers (mocked as MemFS or local dirs)
	p1 := NewMemFS()
	p2 := NewMemFS()
	p3 := NewMemFS()
	
	data := []byte("multi-gigabyte-simulated-content-chunk")
	hash := HashBlock(data)
	
	// Distribute block to mirrors
	p1.PutBlock(ctx, data)
	p2.PutBlock(ctx, data)
	p3.PutBlock(ctx, data)
	
	// 2. Create Distributor
	dist := NewDistributor(NewMemFS(), p1, p2, p3)
	
	// 3. Verify reconstruction from mirrors
	fetched, err := dist.FetchBlock(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if string(fetched) != string(data) {
		t.Error("Data mismatch")
	}
	
	// 4. Reject modified chunks
	corruptedData := []byte("corrupted-content-chunk-data-here")
	p1.PutBlock(ctx, corruptedData) // P1 now has bad data for the same logical request
	
	fetchedClean, err := dist.FetchBlock(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if HashBlock(fetchedClean) != hash {
		t.Error("Distributor failed to reject corrupted chunk from P1")
	}
}
