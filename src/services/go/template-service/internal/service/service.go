package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/i40/production-system/services/go/pkg/kafka"
	"github.com/i40/production-system/services/go/template-service/internal/storage"
	"go.opentelemetry.io/otel/trace"
)

// Service implements the template business logic
type Service struct {
	store    storage.Store
	producer *kafka.Producer
	tracer   trace.Tracer
}

// Event types
type DeviceRegisteredEvent struct {
	Metadata      EventMetadata `json:"metadata"`
	DeviceID      string        `json:"device_id"`
	UnitID        string        `json:"unit_id"`
	CustomerOrder string        `json:"customer_order,omitempty"`
}

type EngravingRequestEvent struct {
	Metadata      EventMetadata          `json:"metadata"`
	JobID         string                 `json:"job_id"`
	DeviceID      string                 `json:"device_id"`
	UnitID        string                 `json:"unit_id"`
	CustomerOrder string                 `json:"customer_order"`
	EngravingData map[string]interface{} `json:"engraving_data"`
}

type EngravingCompleteEvent struct {
	Metadata     EventMetadata `json:"metadata"`
	JobID        string        `json:"job_id"`
	DeviceID     string        `json:"device_id"`
	UnitID       string        `json:"unit_id"`
	Status       string        `json:"status"` // SUCCESS or FAILED
	ErrorMessage string        `json:"error_message,omitempty"`
	CompletedAt  time.Time     `json:"completed_at"`
}

type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	TraceID       string    `json:"trace_id,omitempty"`
	SpanID        string    `json:"span_id,omitempty"`
}

// NewService creates a new template service
func NewService(store storage.Store, producer *kafka.Producer, tracer trace.Tracer) *Service {
	return &Service{
		store:    store,
		producer: producer,
		tracer:   tracer,
	}
}

// HandleDeviceRegistered processes device registration events
func (s *Service) HandleDeviceRegistered(ctx context.Context, key string, value []byte) error {
	ctx, span := s.tracer.Start(ctx, "template.HandleDeviceRegistered")
	defer span.End()

	var event DeviceRegisteredEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal device registered event: %w", err)
	}

	// If device has a customer order, check if there's a template for engraving
	if event.CustomerOrder != "" {
		template, err := s.store.GetTemplateByOrder(ctx, event.CustomerOrder)
		if err != nil {
			// No template found - skip engraving
			return nil
		}

		// Check if template is approved/active
		if template.Status != "APPROVED" && template.Status != "ACTIVE" {
			return nil
		}

		// Create engraving job
		jobID := uuid.New().String()
		job := &storage.EngravingJob{
			JobID:         jobID,
			DeviceID:      event.DeviceID,
			UnitID:        event.UnitID,
			TemplateID:    template.TemplateID,
			CustomerOrder: event.CustomerOrder,
			EngravingData: map[string]interface{}{
				"text":         template.EngravingText,
				"font_style":   template.FontStyle,
				"layout_params": template.LayoutParams,
				"has_logo":     len(template.LogoData) > 0,
			},
			Status:    "PENDING",
			CreatedAt: time.Now(),
		}

		if err := s.store.SaveEngravingJob(ctx, job); err != nil {
			return fmt.Errorf("failed to save engraving job: %w", err)
		}

		// Publish engraving request event
		requestEvent := EngravingRequestEvent{
			Metadata: EventMetadata{
				EventID:       uuid.New().String(),
				EventType:     "customization.engraving.request",
				Timestamp:     time.Now(),
				SourceService: "template-service",
				TraceID:       span.SpanContext().TraceID().String(),
				SpanID:        span.SpanContext().SpanID().String(),
			},
			JobID:         jobID,
			DeviceID:      event.DeviceID,
			UnitID:        event.UnitID,
			CustomerOrder: event.CustomerOrder,
			EngravingData: job.EngravingData,
		}

		if err := s.producer.ProduceEvent(ctx, "customization.engraving.request", jobID, requestEvent); err != nil {
			return fmt.Errorf("failed to publish engraving request: %w", err)
		}
	}

	return nil
}

// HandleEngravingComplete processes engraving completion events
func (s *Service) HandleEngravingComplete(ctx context.Context, key string, value []byte) error {
	ctx, span := s.tracer.Start(ctx, "template.HandleEngravingComplete")
	defer span.End()

	var event EngravingCompleteEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal engraving complete event: %w", err)
	}

	// Get job from database
	job, err := s.store.GetEngravingJob(ctx, event.JobID)
	if err != nil {
		return fmt.Errorf("failed to get engraving job: %w", err)
	}

	// Update job status
	completedAt := event.CompletedAt
	job.CompletedAt = &completedAt

	if event.Status == "SUCCESS" {
		job.Status = "COMPLETED"
	} else {
		job.Status = "FAILED"
		job.ErrorMessage = event.ErrorMessage
	}

	if err := s.store.SaveEngravingJob(ctx, job); err != nil {
		return fmt.Errorf("failed to update engraving job: %w", err)
	}

	return nil
}

// CreateTemplate creates a new customization template
func (s *Service) CreateTemplate(ctx context.Context, req CreateTemplateRequest) (*storage.CustomizationTemplate, error) {
	ctx, span := s.tracer.Start(ctx, "template.CreateTemplate")
	defer span.End()

	template := &storage.CustomizationTemplate{
		TemplateID:    uuid.New().String(),
		CustomerOrder: req.CustomerOrder,
		CustomerName:  req.CustomerName,
		EngravingText: req.EngravingText,
		LogoData:      req.LogoData,
		FontStyle:     req.FontStyle,
		LayoutParams:  req.LayoutParams,
		Status:        "DRAFT",
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	if err := s.store.SaveTemplate(ctx, template); err != nil {
		return nil, fmt.Errorf("failed to create template: %w", err)
	}

	return template, nil
}

// UpdateTemplate updates an existing template
func (s *Service) UpdateTemplate(ctx context.Context, templateID string, req UpdateTemplateRequest) (*storage.CustomizationTemplate, error) {
	ctx, span := s.tracer.Start(ctx, "template.UpdateTemplate")
	defer span.End()

	// Get existing template
	template, err := s.store.GetTemplate(ctx, templateID)
	if err != nil {
		return nil, fmt.Errorf("failed to get template: %w", err)
	}

	// Update fields
	if req.EngravingText != nil {
		template.EngravingText = *req.EngravingText
	}
	if req.LogoData != nil {
		template.LogoData = req.LogoData
	}
	if req.FontStyle != nil {
		template.FontStyle = *req.FontStyle
	}
	if req.LayoutParams != nil {
		template.LayoutParams = req.LayoutParams
	}

	template.UpdatedAt = time.Now()

	if err := s.store.SaveTemplate(ctx, template); err != nil {
		return nil, fmt.Errorf("failed to update template: %w", err)
	}

	return template, nil
}

// ApproveTemplate approves a template for use
func (s *Service) ApproveTemplate(ctx context.Context, templateID string) error {
	ctx, span := s.tracer.Start(ctx, "template.ApproveTemplate")
	defer span.End()

	if err := s.store.UpdateTemplateStatus(ctx, templateID, "APPROVED"); err != nil {
		return fmt.Errorf("failed to approve template: %w", err)
	}

	return nil
}

// GetTemplate retrieves a template by ID
func (s *Service) GetTemplate(ctx context.Context, templateID string) (*storage.CustomizationTemplate, error) {
	ctx, span := s.tracer.Start(ctx, "template.GetTemplate")
	defer span.End()

	template, err := s.store.GetTemplate(ctx, templateID)
	if err != nil {
		return nil, fmt.Errorf("failed to get template: %w", err)
	}

	return template, nil
}

// ListTemplates lists all templates with pagination
func (s *Service) ListTemplates(ctx context.Context, limit, offset int) ([]*storage.CustomizationTemplate, error) {
	ctx, span := s.tracer.Start(ctx, "template.ListTemplates")
	defer span.End()

	templates, err := s.store.ListTemplates(ctx, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list templates: %w", err)
	}

	return templates, nil
}

// GetEngravingJob retrieves an engraving job by ID
func (s *Service) GetEngravingJob(ctx context.Context, jobID string) (*storage.EngravingJob, error) {
	ctx, span := s.tracer.Start(ctx, "template.GetEngravingJob")
	defer span.End()

	job, err := s.store.GetEngravingJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get engraving job: %w", err)
	}

	return job, nil
}

// Request types
type CreateTemplateRequest struct {
	CustomerOrder string                 `json:"customer_order"`
	CustomerName  string                 `json:"customer_name"`
	EngravingText string                 `json:"engraving_text"`
	LogoData      []byte                 `json:"logo_data,omitempty"`
	FontStyle     string                 `json:"font_style"`
	LayoutParams  map[string]interface{} `json:"layout_params,omitempty"`
}

type UpdateTemplateRequest struct {
	EngravingText *string                `json:"engraving_text,omitempty"`
	LogoData      []byte                 `json:"logo_data,omitempty"`
	FontStyle     *string                `json:"font_style,omitempty"`
	LayoutParams  map[string]interface{} `json:"layout_params,omitempty"`
}
