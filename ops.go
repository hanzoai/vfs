package vfs

import (
	"context"
	"github.com/hanzoai/vfs/pkg/delta"
)

// Restore performs a delta-optimized restore of a volume.
func (v *Volume) Restore(ctx context.Context, baseID string) error {
	planner := delta.NewPlanner()
	_, err := planner.Plan(ctx, baseID, v.id)
	if err != nil {
		return err
	}
	// Apply delta-based transfer logic here
	return nil
}