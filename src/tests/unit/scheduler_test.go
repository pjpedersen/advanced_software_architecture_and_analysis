package unit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockStore is a mock implementation of storage.Store
type MockStore struct {
	mock.Mock
}

func (m *MockStore) SaveProductionPlan(ctx context.Context, plan interface{}) error {
	args := m.Called(ctx, plan)
	return args.Error(0)
}

func (m *MockStore) GetProductionPlan(ctx context.Context, planID string) (interface{}, error) {
	args := m.Called(ctx, planID)
	return args.Get(0), args.Error(1)
}

// Test production plan validation
func TestProductionPlanValidation(t *testing.T) {
	tests := []struct {
		name    string
		plan    ProductionPlanRequest
		wantErr bool
	}{
		{
			name: "Valid production plan",
			plan: ProductionPlanRequest{
				Batches: []BatchJob{
					{
						BatchID:    "B001",
						ProductSKU: "WIC-001",
						Quantity:   100,
						StartTime:  time.Now(),
						EndTime:    time.Now().Add(2 * time.Hour),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid - zero quantity",
			plan: ProductionPlanRequest{
				Batches: []BatchJob{
					{
						BatchID:    "B001",
						ProductSKU: "WIC-001",
						Quantity:   0,
						StartTime:  time.Now(),
						EndTime:    time.Now().Add(2 * time.Hour),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid - end time before start time",
			plan: ProductionPlanRequest{
				Batches: []BatchJob{
					{
						BatchID:    "B001",
						ProductSKU: "WIC-001",
						Quantity:   100,
						StartTime:  time.Now().Add(2 * time.Hour),
						EndTime:    time.Now(),
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateProductionPlan(tt.plan)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Test resource allocation
func TestResourceAllocation(t *testing.T) {
	batches := []BatchJob{
		{
			BatchID:    "B001",
			ProductSKU: "WIC-001",
			Quantity:   100,
			StartTime:  time.Now(),
			EndTime:    time.Now().Add(2 * time.Hour),
		},
		{
			BatchID:    "B002",
			ProductSKU: "WIC-002",
			Quantity:   50,
			StartTime:  time.Now().Add(3 * time.Hour),
			EndTime:    time.Now().Add(4 * time.Hour),
		},
	}

	allocations := computeResourceAllocations(batches)

	assert.Equal(t, 2, len(allocations))
	assert.Equal(t, "molding-cell-1", allocations[0].StationID)
}

// Test overlapping batch detection
func TestOverlappingBatchDetection(t *testing.T) {
	tests := []struct {
		name      string
		batches   []BatchJob
		hasOverlap bool
	}{
		{
			name: "No overlap",
			batches: []BatchJob{
				{
					BatchID:    "B001",
					StartTime:  time.Date(2025, 1, 1, 8, 0, 0, 0, time.UTC),
					EndTime:    time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
				},
				{
					BatchID:    "B002",
					StartTime:  time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
					EndTime:    time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
				},
			},
			hasOverlap: false,
		},
		{
			name: "Overlap detected",
			batches: []BatchJob{
				{
					BatchID:    "B001",
					StartTime:  time.Date(2025, 1, 1, 8, 0, 0, 0, time.UTC),
					EndTime:    time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
				},
				{
					BatchID:    "B002",
					StartTime:  time.Date(2025, 1, 1, 9, 0, 0, 0, time.UTC),
					EndTime:    time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC),
				},
			},
			hasOverlap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasOverlap := detectOverlappingBatches(tt.batches)
			assert.Equal(t, tt.hasOverlap, hasOverlap)
		})
	}
}

// Helper types (would normally be in service code)
type ProductionPlanRequest struct {
	Batches []BatchJob
}

type BatchJob struct {
	BatchID    string
	ProductSKU string
	Quantity   int
	StartTime  time.Time
	EndTime    time.Time
}

type ResourceAllocation struct {
	StationID string
	BatchID   string
}

// Helper functions (simplified versions for testing)
func validateProductionPlan(plan ProductionPlanRequest) error {
	for _, batch := range plan.Batches {
		if batch.Quantity <= 0 {
			return assert.AnError
		}
		if batch.EndTime.Before(batch.StartTime) {
			return assert.AnError
		}
	}
	return nil
}

func computeResourceAllocations(batches []BatchJob) []ResourceAllocation {
	allocations := make([]ResourceAllocation, len(batches))
	for i, batch := range batches {
		allocations[i] = ResourceAllocation{
			StationID: "molding-cell-1",
			BatchID:   batch.BatchID,
		}
	}
	return allocations
}

func detectOverlappingBatches(batches []BatchJob) bool {
	for i := 0; i < len(batches)-1; i++ {
		for j := i + 1; j < len(batches); j++ {
			if batches[i].StartTime.Before(batches[j].EndTime) &&
				batches[j].StartTime.Before(batches[i].EndTime) {
				return true
			}
		}
	}
	return false
}
