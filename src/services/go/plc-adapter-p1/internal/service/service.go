package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	sharedkafka "github.com/i40/production-system/services/go/pkg/kafka"
)

type Service struct {
	producer *sharedkafka.Producer
}

type ServiceConfig struct {
	Producer *sharedkafka.Producer
}

func NewService(cfg ServiceConfig) *Service {
	return &Service{
		producer: cfg.Producer,
	}
}


// HandleProductionPlan processes production plan events and controls PLC P1
func (s *Service) HandleProductionPlan(ctx context.Context, key string, value []byte) error {
	var plan ProductionPlanEvent
	if err := json.Unmarshal(value, &plan); err != nil {
		return fmt.Errorf("failed to unmarshal plan: %w", err)
	}

	log.Printf("PLC-P1 received production plan %s with %d batches", plan.PlanID, len(plan.Batches))

	// Send commands to PLC via MQTT
	for _, batch := range plan.Batches {
		if err := s.sendToMQTT(ctx, "plc/p1/molding/batch", batch); err != nil {
			log.Printf("Failed to send batch to PLC: %v", err)
			continue
		}
	}

	// Publish ACK event
	ack := AdapterAckEvent{
		Metadata: EventMetadata{
			EventID:       uuid.New().String(),
			EventType:     "adapter.ack",
			Timestamp:     time.Now(),
			SourceService: "plc-adapter-p1",
		},
		PlanID:      plan.PlanID,
		AdapterID:   "plc-p1",
		AdapterType: "PLC_P1",
		Result:      "SUCCESS",
		AckTimestamp: time.Now(),
	}

	return s.producer.ProduceEvent(ctx, "adapter.ack", plan.PlanID, ack)
}


// Common types
type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	SchemaVersion int       `json:"schema_version"`
}

type ProductionPlanEvent struct {
	Metadata      EventMetadata `json:"metadata"`
	PlanID        string        `json:"plan_id"`
	Batches       []interface{} `json:"batches"`
}

type AdapterAckEvent struct {
	Metadata     EventMetadata `json:"metadata"`
	PlanID       string        `json:"plan_id"`
	AdapterID    string        `json:"adapter_id"`
	AdapterType  string        `json:"adapter_type"`
	Result       string        `json:"result"`
	AckTimestamp time.Time     `json:"ack_timestamp"`
}

type QualityDispositionEvent struct {
	Metadata    EventMetadata `json:"metadata"`
	DeviceID    string        `json:"device_id"`
	Disposition string        `json:"disposition"`
	Decision    DispositionDecision `json:"decision"`
	EvaluatedAt time.Time     `json:"evaluated_at"`
}

type DispositionDecision struct {
	Route  string `json:"route"`
	Reason string `json:"reason"`
}

type TestMetricsEvent struct {
	DeviceID     string         `json:"device_id"`
	Measurements []Measurement  `json:"measurements"`
}

type Measurement struct {
	ParameterName string  `json:"parameter_name"`
	Value         float64 `json:"value"`
	InSpec        bool    `json:"in_spec"`
}

type QualityDisposition struct {
	Status       string            `json:"status"`
	Decision     DispositionDecision `json:"decision"`
	FailureModes []FailureMode     `json:"failure_modes,omitempty"`
}

type FailureMode struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Severity    string `json:"severity"`
}

// MQTT helper (simulated)
func (s *Service) sendToMQTT(ctx context.Context, topic string, payload interface{}) error {
	data, _ := json.Marshal(payload)
	log.Printf("MQTT publish to %s: %d bytes", topic, len(data))
	// In production, actually publish to MQTT broker
	return nil
}
