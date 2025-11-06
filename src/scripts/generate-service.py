#!/usr/bin/env python3
"""
Service Generator for I4.0 Production System
Generates Go microservices from template
"""

import os
import sys
from pathlib import Path

SERVICE_DEFINITIONS = {
    "plc-adapter-p1": {
        "port": 8082,
        "description": "PLC Adapter for Injection Molding Cell (P1)",
        "consumes": ["production.plan"],
        "produces": ["adapter.ack", "changeover.start"],
        "logic": """
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
"""
    },
    "plc-adapter-p2": {
        "port": 8083,
        "description": "PLC Adapter for Assembly Line (P2)",
        "consumes": ["production.plan", "quality.disposition"],
        "produces": ["adapter.ack"],
        "logic": """
// HandleProductionPlan processes production plan events for assembly
func (s *Service) HandleProductionPlan(ctx context.Context, key string, value []byte) error {
	var plan ProductionPlanEvent
	if err := json.Unmarshal(value, &plan); err != nil {
		return fmt.Errorf("failed to unmarshal plan: %w", err)
	}

	log.Printf("PLC-P2 received production plan %s", plan.PlanID)

	// Send to assembly line PLC
	if err := s.sendToMQTT(ctx, "plc/p2/assembly/plan", plan); err != nil {
		return err
	}

	// Publish ACK
	ack := AdapterAckEvent{
		Metadata: EventMetadata{
			EventID:       uuid.New().String(),
			EventType:     "adapter.ack",
			Timestamp:     time.Now(),
			SourceService: "plc-adapter-p2",
		},
		PlanID:      plan.PlanID,
		AdapterID:   "plc-p2",
		AdapterType: "PLC_P2",
		Result:      "SUCCESS",
		AckTimestamp: time.Now(),
	}

	return s.producer.ProduceEvent(ctx, "adapter.ack", plan.PlanID, ack)
}

// HandleQualityDisposition routes units based on quality decision
func (s *Service) HandleQualityDisposition(ctx context.Context, key string, value []byte) error {
	var disposition QualityDispositionEvent
	if err := json.Unmarshal(value, &disposition); err != nil {
		return err
	}

	log.Printf("Routing unit %s to %s", disposition.DeviceID, disposition.Decision.Route)

	// Send routing command to PLC
	cmd := map[string]interface{}{
		"unit_id": disposition.DeviceID,
		"route":   disposition.Decision.Route,
	}

	return s.sendToMQTT(ctx, "plc/p2/routing/command", cmd)
}
"""
    },
    "quality-service": {
        "port": 8085,
        "description": "Quality Service for validation and disposition",
        "consumes": ["test.metrics"],
        "produces": ["quality.disposition"],
        "logic": """
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
"""
    },
}

MAIN_TEMPLATE = """package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/i40/production-system/services/{service_name}/internal/api"
	"github.com/i40/production-system/services/{service_name}/internal/config"
	"github.com/i40/production-system/services/{service_name}/internal/service"
	sharedkafka "github.com/i40/production-system/services/go/pkg/kafka"
)

func main() {{
	log.Println("Starting {description}...")

	cfg, err := config.Load()
	if err != nil {{
		log.Fatalf("Failed to load config: %v", err)
	}}

	producer, err := sharedkafka.NewProducer(sharedkafka.ProducerConfig{{
		BootstrapServers: cfg.KafkaBootstrapServers,
		ClientID:         "{service_name}",
	}})
	if err != nil {{
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}}
	defer producer.Close()

	consumer, err := sharedkafka.NewConsumer(sharedkafka.ConsumerConfig{{
		BootstrapServers: cfg.KafkaBootstrapServers,
		GroupID:          "{service_name}-consumer",
		Topics:           []string{{{topics}}},
		AutoOffsetReset:  "earliest",
	}})
	if err != nil {{
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}}
	defer consumer.Close()

	svc := service.NewService(service.ServiceConfig{{
		Producer: producer,
	}})

	{register_handlers}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {{
		if err := consumer.Start(ctx); err != nil && err != context.Canceled {{
			log.Printf("Consumer error: %v", err)
		}}
	}}()

	httpServer := api.NewServer(api.ServerConfig{{
		Service: svc,
		Port:    cfg.HTTPPort,
	}})

	go func() {{
		log.Printf("HTTP server listening on :%d", cfg.HTTPPort)
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed {{
			log.Fatalf("HTTP server error: %v", err)
		}}
	}}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {{
		log.Printf("HTTP server shutdown error: %v", err)
	}}

	cancel()
	log.Println("Shutdown complete")
}}
"""

def generate_service(service_name, definition):
    """Generate a complete microservice"""

    base_path = Path(f"services/go/{service_name}")
    base_path.mkdir(parents=True, exist_ok=True)

    # Create directory structure
    (base_path / "cmd" / "server").mkdir(parents=True, exist_ok=True)
    (base_path / "internal" / "api").mkdir(parents=True, exist_ok=True)
    (base_path / "internal" / "config").mkdir(parents=True, exist_ok=True)
    (base_path / "internal" / "service").mkdir(parents=True, exist_ok=True)

    # Generate go.mod
    gomod = f"""module github.com/i40/production-system/services/{service_name}

go 1.21

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	github.com/google/uuid v1.5.0
	github.com/gorilla/mux v1.8.1
	github.com/prometheus/client_golang v1.17.0
	go.opentelemetry.io/otel v1.21.0
	google.golang.org/protobuf v1.31.0
)
"""
    (base_path / "go.mod").write_text(gomod)

    # Generate config
    config = f"""package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {{
	HTTPPort              int
	KafkaBootstrapServers string
	ServiceName           string
	LogLevel              string
}}

func Load() (*Config, error) {{
	cfg := &Config{{
		HTTPPort:              getEnvAsInt("HTTP_PORT", {definition['port']}),
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		ServiceName:           getEnv("SERVICE_NAME", "{service_name}"),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
	}}
	return cfg, nil
}}

func getEnv(key, defaultValue string) string {{
	value := os.Getenv(key)
	if value == "" {{
		return defaultValue
	}}
	return value
}}

func getEnvAsInt(key string, defaultValue int) int {{
	valueStr := os.Getenv(key)
	if valueStr == "" {{
		return defaultValue
	}}
	value, err := strconv.Atoi(valueStr)
	if err != nil {{
		return defaultValue
	}}
	return value
}}
"""
    (base_path / "internal" / "config" / "config.go").write_text(config)

    # Generate service logic
    service_code = f"""package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	sharedkafka "github.com/i40/production-system/services/go/pkg/kafka"
)

type Service struct {{
	producer *sharedkafka.Producer
}}

type ServiceConfig struct {{
	Producer *sharedkafka.Producer
}}

func NewService(cfg ServiceConfig) *Service {{
	return &Service{{
		producer: cfg.Producer,
	}}
}}

{definition['logic']}

// Common types
type EventMetadata struct {{
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	SchemaVersion int       `json:"schema_version"`
}}

type ProductionPlanEvent struct {{
	Metadata      EventMetadata `json:"metadata"`
	PlanID        string        `json:"plan_id"`
	Batches       []interface{{}} `json:"batches"`
}}

type AdapterAckEvent struct {{
	Metadata     EventMetadata `json:"metadata"`
	PlanID       string        `json:"plan_id"`
	AdapterID    string        `json:"adapter_id"`
	AdapterType  string        `json:"adapter_type"`
	Result       string        `json:"result"`
	AckTimestamp time.Time     `json:"ack_timestamp"`
}}

type QualityDispositionEvent struct {{
	Metadata    EventMetadata `json:"metadata"`
	DeviceID    string        `json:"device_id"`
	Disposition string        `json:"disposition"`
	Decision    DispositionDecision `json:"decision"`
	EvaluatedAt time.Time     `json:"evaluated_at"`
}}

type DispositionDecision struct {{
	Route  string `json:"route"`
	Reason string `json:"reason"`
}}

type TestMetricsEvent struct {{
	DeviceID     string         `json:"device_id"`
	Measurements []Measurement  `json:"measurements"`
}}

type Measurement struct {{
	ParameterName string  `json:"parameter_name"`
	Value         float64 `json:"value"`
	InSpec        bool    `json:"in_spec"`
}}

type QualityDisposition struct {{
	Status       string            `json:"status"`
	Decision     DispositionDecision `json:"decision"`
	FailureModes []FailureMode     `json:"failure_modes,omitempty"`
}}

type FailureMode struct {{
	Code        string `json:"code"`
	Description string `json:"description"`
	Severity    string `json:"severity"`
}}

// MQTT helper (simulated)
func (s *Service) sendToMQTT(ctx context.Context, topic string, payload interface{{}}) error {{
	data, _ := json.Marshal(payload)
	log.Printf("MQTT publish to %s: %d bytes", topic, len(data))
	// In production, actually publish to MQTT broker
	return nil
}}
"""
    (base_path / "internal" / "service" / "service.go").write_text(service_code)

    # Generate API server
    api_code = """package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	router *mux.Router
	server *http.Server
}

type ServerConfig struct {
	Service interface{}
	Port    int
}

func NewServer(cfg ServerConfig) *Server {
	s := &Server{
		router: mux.NewRouter(),
	}

	s.router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}).Methods("GET")

	s.router.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	}).Methods("GET")

	s.router.Handle("/metrics", promhttp.Handler())

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: s.router,
	}

	return s
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
"""
    (base_path / "internal" / "api" / "server.go").write_text(api_code)

    # Generate main.go
    topics = ', '.join([f'"{t}"' for t in definition['consumes']])
    handlers = '\n\t'.join([f'consumer.RegisterHandler("{topic}", svc.Handle{topic.replace(".", "").title()})'
                            for topic in definition['consumes']])

    main_code = MAIN_TEMPLATE.format(
        service_name=service_name,
        description=definition['description'],
        topics=topics,
        register_handlers=handlers
    )
    (base_path / "cmd" / "server" / "main.go").write_text(main_code)

    print(f"✓ Generated {service_name}")

if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent)

    for service_name, definition in SERVICE_DEFINITIONS.items():
        generate_service(service_name, definition)

    print("\n✓ All services generated successfully!")
    print("\nNext steps:")
    print("1. Update go.work to include new services")
    print("2. Run: make services-build")
    print("3. Test each service")
