package vfs

import (
	"context"
	"testing"
)

func TestDeltaPlanner_Plan(t *testing.T) {
	// Mock setup
	v := &VFS{} 
	planner := NewDeltaPlanner(v)

	t.Run("IdenticalHeads", func(t *testing.T) {
		h1 := &Head{Root: Hash{1}}
		h2 := &Head{Root: Hash{1}}
		
		plan, err := planner.Plan(context.Background(), h1, h2)
		if err != nil {
			t.Fatalf("failed to plan: %v", err)
		}
		
		if len(plan.MissingChunks) != 0 {
			t.Errorf("expected 0 missing chunks, got %d", len(plan.MissingChunks))
		}
	})
}

func TestDeltaPlanner_RestoreParallel(t *testing.T) {
	v := &VFS{}
	planner := NewDeltaPlanner(v)
	
	plan := &DeltaPlan{
		MissingChunks: []ChunkRef{{}, {}, {}}, // 3 dummy chunks
	}

	err := planner.RestoreParallel(context.Background(), plan, 2)
	if err != nil {
		t.Fatalf("parallel restore failed: %v", err)
	}
}

func BenchmarkDeltaRestore(b *testing.B) {
	// Setup a volume with 1000 chunks
	// Create a delta of 10 chunks
	v := &VFS{}
	planner := NewDeltaPlanner(v)
	plan := &DeltaPlan{
		MissingChunks: make([]ChunkRef, 10),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = planner.RestoreParallel(context.Background(), plan, 4)
	}
}

func BenchmarkFullRestore(b *testing.B) {
	// Setup a volume with 1000 chunks
	v := &VFS{}
	planner := NewDeltaPlanner(v)
	plan := &DeltaPlan{
		MissingChunks: make([]ChunkRef, 1000),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = planner.RestoreParallel(context.Background(), plan, 4)
	}
}
