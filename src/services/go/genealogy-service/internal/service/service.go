package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/i40/production-system/services/go/genealogy-service/internal/storage"
	"github.com/i40/production-system/services/go/pkg/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

// Service implements the genealogy business logic
type Service struct {
	store    storage.Store
	producer *kafka.Producer
	tracer   trace.Tracer
}

var (
	recallQueriesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "genealogy_recall_queries_total",
		Help: "Total number of recall queries executed.",
	})
	recallQueryActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "recall_query_active",
		Help: "Number of recall queries currently executing.",
	})
)

func init() {
	prometheus.MustRegister(recallQueriesTotal, recallQueryActive)
}

// RecallQueryRequest represents a request to query devices for recall
type RecallQueryRequest struct {
	QueryID           string   `json:"query_id"`
	RequestedBy       string   `json:"requested_by"`
	PelletLotIDs      []string `json:"pellet_lot_ids,omitempty"`
	ColorIDs          []string `json:"color_ids,omitempty"`
	FirmwareVersions  []string `json:"firmware_versions,omitempty"`
	ProcessStationIDs []string `json:"process_station_ids,omitempty"`
	StartDate         *string  `json:"start_date,omitempty"`
	EndDate           *string  `json:"end_date,omitempty"`
	ProductionBatch   string   `json:"production_batch,omitempty"`
}

// RecallQueryResponse represents the response with affected devices
type RecallQueryResponse struct {
	QueryID         string                 `json:"query_id"`
	ExecutedAt      time.Time              `json:"executed_at"`
	DeviceCount     int                    `json:"device_count"`
	AffectedDevices []storage.RecallDevice `json:"affected_devices"`
	QueryCriteria   RecallQueryRequest     `json:"query_criteria"`
}

// OTALockRequest represents a request to lock devices for OTA updates
type OTALockRequest struct {
	UnitIDs    []string   `json:"unit_ids"`
	LockReason string     `json:"lock_reason"`
	RecallID   string     `json:"recall_id,omitempty"`
	LockUntil  *time.Time `json:"lock_until,omitempty"`
}

// Event types
type RecallQueryRequestEvent struct {
	Metadata EventMetadata      `json:"metadata"`
	Request  RecallQueryRequest `json:"request"`
}

type RecallQueryResponseEvent struct {
	Metadata EventMetadata       `json:"metadata"`
	Response RecallQueryResponse `json:"response"`
}

type OTALockEvent struct {
	Metadata EventMetadata  `json:"metadata"`
	Request  OTALockRequest `json:"request"`
}

type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	TraceID       string    `json:"trace_id,omitempty"`
	SpanID        string    `json:"span_id,omitempty"`
}

// NewService creates a new genealogy service
func NewService(store storage.Store, producer *kafka.Producer, tracer trace.Tracer) *Service {
	return &Service{
		store:    store,
		producer: producer,
		tracer:   tracer,
	}
}

// HandleRecallQuery processes a recall query request
func (s *Service) HandleRecallQuery(ctx context.Context, key string, value []byte) error {
	ctx, span := s.tracer.Start(ctx, "genealogy.HandleRecallQuery")
	defer span.End()

	var event RecallQueryRequestEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal recall query request: %w", err)
	}

	// Build storage criteria from request
	criteria := storage.RecallCriteria{
		PelletLotIDs:      event.Request.PelletLotIDs,
		ColorIDs:          event.Request.ColorIDs,
		FirmwareVersions:  event.Request.FirmwareVersions,
		ProcessStationIDs: event.Request.ProcessStationIDs,
		ProductionBatch:   event.Request.ProductionBatch,
	}

	// Parse date range if provided
	if event.Request.StartDate != nil {
		startDate, err := time.Parse(time.RFC3339, *event.Request.StartDate)
		if err == nil {
			criteria.StartDate = &startDate
		}
	}

	if event.Request.EndDate != nil {
		endDate, err := time.Parse(time.RFC3339, *event.Request.EndDate)
		if err == nil {
			criteria.EndDate = &endDate
		}
	}

	// Query affected devices
	devices, err := s.store.QueryRecallDevices(ctx, criteria)
	if err != nil {
		return fmt.Errorf("failed to query recall devices: %w", err)
	}

	// Build response
	response := RecallQueryResponse{
		QueryID:         event.Request.QueryID,
		ExecutedAt:      time.Now(),
		DeviceCount:     len(devices),
		AffectedDevices: devices,
		QueryCriteria:   event.Request,
	}

	// Publish response event
	responseEvent := RecallQueryResponseEvent{
		Metadata: EventMetadata{
			EventID:       uuid.New().String(),
			EventType:     "genealogy.recall.query.response",
			Timestamp:     time.Now(),
			SourceService: "genealogy-service",
			TraceID:       span.SpanContext().TraceID().String(),
			SpanID:        span.SpanContext().SpanID().String(),
		},
		Response: response,
	}

	if err := s.producer.ProduceEvent(ctx, "genealogy.recall.response", event.Request.QueryID, responseEvent); err != nil {
		return fmt.Errorf("failed to publish recall query response: %w", err)
	}

	return nil
}

// QueryRecallDevices performs a synchronous recall query
func (s *Service) QueryRecallDevices(ctx context.Context, req RecallQueryRequest) (*RecallQueryResponse, error) {
	ctx, span := s.tracer.Start(ctx, "genealogy.QueryRecallDevices")
	defer span.End()

	recallQueriesTotal.Inc()
	recallQueryActive.Inc()
	defer recallQueryActive.Dec()

	// Build storage criteria
	criteria := storage.RecallCriteria{
		PelletLotIDs:      req.PelletLotIDs,
		ColorIDs:          req.ColorIDs,
		FirmwareVersions:  req.FirmwareVersions,
		ProcessStationIDs: req.ProcessStationIDs,
		ProductionBatch:   req.ProductionBatch,
	}

	// Parse date range
	if req.StartDate != nil {
		startDate, err := time.Parse(time.RFC3339, *req.StartDate)
		if err == nil {
			criteria.StartDate = &startDate
		}
	}

	if req.EndDate != nil {
		endDate, err := time.Parse(time.RFC3339, *req.EndDate)
		if err == nil {
			criteria.EndDate = &endDate
		}
	}

	// Query devices
	devices, err := s.store.QueryRecallDevices(ctx, criteria)
	if err != nil {
		return nil, fmt.Errorf("failed to query recall devices: %w", err)
	}

	response := &RecallQueryResponse{
		QueryID:         req.QueryID,
		ExecutedAt:      time.Now(),
		DeviceCount:     len(devices),
		AffectedDevices: devices,
		QueryCriteria:   req,
	}

	return response, nil
}

// GetCompleteGenealogy retrieves the full genealogy for a device
func (s *Service) GetCompleteGenealogy(ctx context.Context, unitID string) (*storage.CompleteGenealogy, error) {
	ctx, span := s.tracer.Start(ctx, "genealogy.GetCompleteGenealogy")
	defer span.End()

	genealogy, err := s.store.GetCompleteGenealogy(ctx, unitID)
	if err != nil {
		return nil, fmt.Errorf("failed to get complete genealogy: %w", err)
	}

	return genealogy, nil
}

// LockDevicesForOTA locks devices to prevent OTA updates during recall
func (s *Service) LockDevicesForOTA(ctx context.Context, req OTALockRequest) error {
	ctx, span := s.tracer.Start(ctx, "genealogy.LockDevicesForOTA")
	defer span.End()

	// Publish OTA lock event
	event := OTALockEvent{
		Metadata: EventMetadata{
			EventID:       uuid.New().String(),
			EventType:     "genealogy.ota.lock",
			Timestamp:     time.Now(),
			SourceService: "genealogy-service",
			TraceID:       span.SpanContext().TraceID().String(),
			SpanID:        span.SpanContext().SpanID().String(),
		},
		Request: req,
	}

	// Use first unit ID as key (for partitioning)
	key := "ota-lock"
	if len(req.UnitIDs) > 0 {
		key = req.UnitIDs[0]
	}

	if err := s.producer.ProduceEvent(ctx, "genealogy.ota.lock", key, event); err != nil {
		return fmt.Errorf("failed to publish OTA lock event: %w", err)
	}

	return nil
}

// GetDevicesByLotID finds all devices from a specific pellet lot
func (s *Service) GetDevicesByLotID(ctx context.Context, lotID string) ([]string, error) {
	ctx, span := s.tracer.Start(ctx, "genealogy.GetDevicesByLotID")
	defer span.End()

	unitIDs, err := s.store.GetDevicesByLotID(ctx, lotID)
	if err != nil {
		return nil, fmt.Errorf("failed to get devices by lot ID: %w", err)
	}

	return unitIDs, nil
}

// GetDevicesByFirmwareVersion finds all devices with a specific firmware version
func (s *Service) GetDevicesByFirmwareVersion(ctx context.Context, version string) ([]string, error) {
	ctx, span := s.tracer.Start(ctx, "genealogy.GetDevicesByFirmwareVersion")
	defer span.End()

	unitIDs, err := s.store.GetDevicesByFirmwareVersion(ctx, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get devices by firmware version: %w", err)
	}

	return unitIDs, nil
}

// GetDevicesByDateRange finds all devices manufactured within a date range
func (s *Service) GetDevicesByDateRange(ctx context.Context, start, end time.Time) ([]string, error) {
	ctx, span := s.tracer.Start(ctx, "genealogy.GetDevicesByDateRange")
	defer span.End()

	unitIDs, err := s.store.GetDevicesByDateRange(ctx, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to get devices by date range: %w", err)
	}

	return unitIDs, nil
}
