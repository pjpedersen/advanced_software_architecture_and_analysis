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


// HandleTestMetrics processes test metrics and makes quality decisions
func (s *Service) HandleTestMetrics(ctx context.Context, key string, value []byte) error {
	var metrics TestMetricsEvent
	if err := json.Unmarshal(value, &metrics); err != nil {
		return err
	}

	log.Printf("Evaluating quality for device %s", metrics.DeviceID)

	// Apply quality rules
	disposition := s.evaluateQuality(metrics)

	// Publish disposition event
	event := QualityDispositionEvent{
		Metadata: EventMetadata{
			EventID:       uuid.New().String(),
			EventType:     "quality.disposition",
			Timestamp:     time.Now(),
			SourceService: "quality-service",
		},
		DeviceID:    metrics.DeviceID,
		Disposition: disposition.Status,
		Decision:    disposition.Decision,
		EvaluatedAt: time.Now(),
	}

	return s.producer.ProduceEvent(ctx, "quality.disposition", metrics.DeviceID, event)
}

func (s *Service) evaluateQuality(metrics TestMetricsEvent) QualityDisposition {
	// Simple rule: all measurements must be in spec
	allPassed := true
	var failures []FailureMode

	for _, measurement := range metrics.Measurements {
		if !measurement.InSpec {
			allPassed = false
			failures = append(failures, FailureMode{
				Code:        "OUT_OF_SPEC",
				Description: fmt.Sprintf("%s out of spec", measurement.ParameterName),
				Severity:    "MAJOR",
			})
		}
	}

	if allPassed {
		return QualityDisposition{
			Status: "PASS",
			Decision: DispositionDecision{
				Route:  "PASS_LANE",
				Reason: "All tests passed",
			},
		}
	}

	return QualityDisposition{
		Status: "FAIL",
		Decision: DispositionDecision{
			Route:  "FAIL_LANE",
			Reason: "Quality checks failed",
		},
		FailureModes: failures,
	}
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
