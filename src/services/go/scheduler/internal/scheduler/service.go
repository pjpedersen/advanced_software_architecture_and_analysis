package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	sharedkafka "github.com/i40/production-system/services/go/pkg/kafka"
)

// ErrPlanNotFound indicates the requested production plan does not exist.
var ErrPlanNotFound = errors.New("plan not found")

type Service struct {
	store    Store
	producer *sharedkafka.Producer
}

type ServiceConfig struct {
	Store    Store
	Producer *sharedkafka.Producer
}

func NewService(cfg ServiceConfig) *Service {
	return &Service{
		store:    cfg.Store,
		producer: cfg.Producer,
	}
}

// ProductionPlanRequest represents a request to create a production plan
type ProductionPlanRequest struct {
	Batches           []BatchJob         `json:"batches"`
	ChangeoverWindows []ChangeoverWindow `json:"changeover_windows"`
	StartTime         time.Time          `json:"start_time"`
	EndTime           time.Time          `json:"end_time"`
}

type BatchJob struct {
	BatchID           string        `json:"batch_id"`
	ProductSKU        string        `json:"product_sku"`
	Quantity          int           `json:"quantity"`
	Color             string        `json:"color"`
	EngravingTemplate string        `json:"engraving_template_id"`
	ScheduledStart    time.Time     `json:"scheduled_start"`
	EstimatedDuration time.Duration `json:"estimated_duration"`
	Priority          int           `json:"priority"`
}

type ChangeoverWindow struct {
	ChangeoverID                 string        `json:"changeover_id"`
	FromColor                    string        `json:"from_color"`
	ToColor                      string        `json:"to_color"`
	ScheduledTime                time.Time     `json:"scheduled_time"`
	EstimatedDuration            time.Duration `json:"estimated_duration"`
	RequiresOperatorConfirmation bool          `json:"requires_operator_confirmation"`
}

// ProductionPlan represents a complete production plan
type ProductionPlan struct {
	PlanID              string                        `json:"plan_id"`
	Batches             []BatchJob                    `json:"batches"`
	ChangeoverWindows   []ChangeoverWindow            `json:"changeover_windows"`
	StartTime           time.Time                     `json:"start_time"`
	EndTime             time.Time                     `json:"end_time"`
	ResourceAllocations map[string]ResourceAllocation `json:"resource_allocations"`
	Status              string                        `json:"status"`
	CreatedAt           time.Time                     `json:"created_at"`
}

type ResourceAllocation struct {
	ResourceID     string    `json:"resource_id"`
	ResourceType   string    `json:"resource_type"`
	AllocatedFrom  time.Time `json:"allocated_from"`
	AllocatedUntil time.Time `json:"allocated_until"`
}

// CreateProductionPlan creates and publishes a new production plan (UC-1)
func (s *Service) CreateProductionPlan(ctx context.Context, req ProductionPlanRequest) (*ProductionPlan, error) {
	planID := uuid.New().String()

	// Validate batches
	if len(req.Batches) == 0 {
		return nil, fmt.Errorf("at least one batch is required")
	}

	// Check for resource conflicts
	resourceAllocations := s.computeResourceAllocations(req.Batches)
	if err := s.validateResourceAllocations(resourceAllocations); err != nil {
		return nil, fmt.Errorf("resource conflict detected: %w", err)
	}

	// Create plan
	plan := &ProductionPlan{
		PlanID:              planID,
		Batches:             req.Batches,
		ChangeoverWindows:   req.ChangeoverWindows,
		StartTime:           req.StartTime,
		EndTime:             req.EndTime,
		ResourceAllocations: resourceAllocations,
		Status:              "PENDING",
		CreatedAt:           time.Now(),
	}

	// Save to database
	if err := s.store.SaveProductionPlan(ctx, plan); err != nil {
		return nil, fmt.Errorf("failed to save plan: %w", err)
	}

	// Publish to Kafka
	event := ProductionPlanEvent{
		Metadata: EventMetadata{
			EventID:       uuid.New().String(),
			EventType:     "production.plan.created",
			Timestamp:     time.Now(),
			SourceService: "scheduler",
			SchemaVersion: 1,
		},
		PlanID:              plan.PlanID,
		Batches:             plan.Batches,
		ChangeoverWindows:   plan.ChangeoverWindows,
		StartTime:           plan.StartTime,
		EndTime:             plan.EndTime,
		ResourceAllocations: plan.ResourceAllocations,
	}

	if err := s.producer.ProduceEvent(ctx, "production.plan", planID, event); err != nil {
		return nil, fmt.Errorf("failed to publish plan event: %w", err)
	}

	log.Printf("Production plan %s created and published", planID)
	return plan, nil
}

// GetProductionPlan returns a single plan by ID.
func (s *Service) GetProductionPlan(ctx context.Context, planID string) (*ProductionPlan, error) {
	if planID == "" {
		return nil, fmt.Errorf("planID is required")
	}

	plan, err := s.store.GetProductionPlan(ctx, planID)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// HandleAdapterAck processes ACK events from adapters
func (s *Service) HandleAdapterAck(ctx context.Context, key string, value []byte) error {
	var ack AdapterAckEvent
	if err := json.Unmarshal(value, &ack); err != nil {
		return fmt.Errorf("failed to unmarshal ACK: %w", err)
	}

	log.Printf("Received ACK from adapter %s for plan %s: %s",
		ack.AdapterID, ack.PlanID, ack.Result)

	// Update plan status in database
	if err := s.store.UpdatePlanAck(ctx, ack.PlanID, ack.AdapterID, ack.Result); err != nil {
		return fmt.Errorf("failed to update ACK: %w", err)
	}

	// Check if all adapters have ACKed
	allAcked, err := s.store.CheckAllAcked(ctx, ack.PlanID)
	if err != nil {
		return fmt.Errorf("failed to check ACK status: %w", err)
	}

	if allAcked {
		log.Printf("All adapters ACKed plan %s", ack.PlanID)
		if err := s.store.UpdatePlanStatus(ctx, ack.PlanID, "ACTIVE"); err != nil {
			return fmt.Errorf("failed to update plan status: %w", err)
		}
	}

	return nil
}

// computeResourceAllocations calculates resource allocations from batches
func (s *Service) computeResourceAllocations(batches []BatchJob) map[string]ResourceAllocation {
	allocations := make(map[string]ResourceAllocation)

	for _, batch := range batches {
		// Simplified allocation logic
		// In reality, this would involve complex scheduling algorithms

		// PLC P1 for molding
		allocations[fmt.Sprintf("PLC_P1_%s", batch.BatchID)] = ResourceAllocation{
			ResourceID:     "PLC_P1",
			ResourceType:   "PLC",
			AllocatedFrom:  batch.ScheduledStart,
			AllocatedUntil: batch.ScheduledStart.Add(batch.EstimatedDuration),
		}

		// PLC P2 for assembly
		assemblyStart := batch.ScheduledStart.Add(batch.EstimatedDuration / 2)
		allocations[fmt.Sprintf("PLC_P2_%s", batch.BatchID)] = ResourceAllocation{
			ResourceID:     "PLC_P2",
			ResourceType:   "PLC",
			AllocatedFrom:  assemblyStart,
			AllocatedUntil: assemblyStart.Add(batch.EstimatedDuration),
		}

		// Engraver if template specified
		if batch.EngravingTemplate != "" {
			allocations[fmt.Sprintf("ENGRAVER_%s", batch.BatchID)] = ResourceAllocation{
				ResourceID:     "ENGRAVER_1",
				ResourceType:   "ENGRAVER",
				AllocatedFrom:  assemblyStart,
				AllocatedUntil: assemblyStart.Add(batch.EstimatedDuration / 4),
			}
		}
	}

	return allocations
}

// validateResourceAllocations checks for scheduling conflicts
func (s *Service) validateResourceAllocations(allocations map[string]ResourceAllocation) error {
	// Check for overlapping time windows on same resource
	resourceWindows := make(map[string][]ResourceAllocation)

	for _, alloc := range allocations {
		resourceWindows[alloc.ResourceID] = append(resourceWindows[alloc.ResourceID], alloc)
	}

	for resourceID, windows := range resourceWindows {
		for i := 0; i < len(windows); i++ {
			for j := i + 1; j < len(windows); j++ {
				if s.timeWindowsOverlap(windows[i], windows[j]) {
					return fmt.Errorf("resource %s has overlapping allocations", resourceID)
				}
			}
		}
	}

	return nil
}

func (s *Service) timeWindowsOverlap(a, b ResourceAllocation) bool {
	return a.AllocatedFrom.Before(b.AllocatedUntil) && b.AllocatedFrom.Before(a.AllocatedUntil)
}

// Event types
type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	SchemaVersion int       `json:"schema_version"`
}

type ProductionPlanEvent struct {
	Metadata            EventMetadata                 `json:"metadata"`
	PlanID              string                        `json:"plan_id"`
	Batches             []BatchJob                    `json:"batches"`
	ChangeoverWindows   []ChangeoverWindow            `json:"changeover_windows"`
	StartTime           time.Time                     `json:"start_time"`
	EndTime             time.Time                     `json:"end_time"`
	ResourceAllocations map[string]ResourceAllocation `json:"resource_allocations"`
}

type AdapterAckEvent struct {
	Metadata     EventMetadata `json:"metadata"`
	PlanID       string        `json:"plan_id"`
	AdapterID    string        `json:"adapter_id"`
	AdapterType  string        `json:"adapter_type"`
	Result       string        `json:"result"`
	AckTimestamp time.Time     `json:"ack_timestamp"`
}

// Store interface
type Store interface {
	SaveProductionPlan(ctx context.Context, plan *ProductionPlan) error
	UpdatePlanAck(ctx context.Context, planID, adapterID, result string) error
	CheckAllAcked(ctx context.Context, planID string) (bool, error)
	UpdatePlanStatus(ctx context.Context, planID, status string) error
	GetProductionPlan(ctx context.Context, planID string) (*ProductionPlan, error)
}
