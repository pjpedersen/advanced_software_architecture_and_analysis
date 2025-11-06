package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/i40/production-system/services/go/pkg/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type QualityDispositionEvent struct {
	DeviceID    string `json:"device_id"`
	UnitID      string `json:"unit_id"`
	Disposition string `json:"disposition"`
	Decision    string `json:"decision"`
}

type ValidationTestEvent struct {
	Metadata EventMetadata     `json:"metadata"`
	DeviceID string            `json:"device_id"`
	UnitID   string            `json:"unit_id"`
	Tests    []TestResult      `json:"tests"`
	Summary  ValidationSummary `json:"summary"`
}

type TestResult struct {
	TestName   string  `json:"test_name"`
	Passed     bool    `json:"passed"`
	Value      float64 `json:"value"`
	LowerLimit float64 `json:"lower_limit"`
	UpperLimit float64 `json:"upper_limit"`
}

type ValidationSummary struct {
	TotalTests  int    `json:"total_tests"`
	PassedTests int    `json:"passed_tests"`
	FailedTests int    `json:"failed_tests"`
	Status      string `json:"status"`
}

type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	TraceID       string    `json:"trace_id,omitempty"`
}

func handleQualityDisposition(ctx context.Context, producer *kafka.Producer, tracer trace.Tracer) kafka.MessageHandler {
	return func(ctx context.Context, key string, value []byte) error {
		ctx, span := tracer.Start(ctx, "validation.HandleQualityDisposition")
		defer span.End()

		var event QualityDispositionEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		log.Printf("Running validation tests for device %s", event.UnitID)

		// Simulate validation tests
		tests := []TestResult{
			{
				TestName:   "WiFi Signal Strength",
				Passed:     true,
				Value:      -45.0,
				LowerLimit: -70.0,
				UpperLimit: -20.0,
			},
			{
				TestName:   "Temperature Sensor Accuracy",
				Passed:     true,
				Value:      0.5,
				LowerLimit: 0.0,
				UpperLimit: 2.0,
			},
			{
				TestName:   "Power Consumption",
				Passed:     true,
				Value:      3.2,
				LowerLimit: 2.5,
				UpperLimit: 4.0,
			},
			{
				TestName:   "Display Brightness",
				Passed:     rand.Float64() > 0.1, // 90% pass rate
				Value:      350.0 + rand.Float64()*100,
				LowerLimit: 300.0,
				UpperLimit: 500.0,
			},
		}

		passedCount := 0
		for _, test := range tests {
			if test.Passed {
				passedCount++
			}
		}

		status := "PASS"
		if passedCount < len(tests) {
			status = "FAIL"
		}

		validationEvent := ValidationTestEvent{
			Metadata: EventMetadata{
				EventID:       uuid.New().String(),
				EventType:     "validation.test.complete",
				Timestamp:     time.Now(),
				SourceService: "validation-controller",
				TraceID:       span.SpanContext().TraceID().String(),
			},
			DeviceID: event.DeviceID,
			UnitID:   event.UnitID,
			Tests:    tests,
			Summary: ValidationSummary{
				TotalTests:  len(tests),
				PassedTests: passedCount,
				FailedTests: len(tests) - passedCount,
				Status:      status,
			},
		}

		return producer.ProduceEvent(ctx, "validation.test.complete", event.DeviceID, validationEvent)
	}
}

func main() {
	serviceName := getEnv("SERVICE_NAME", "validation-controller")
	kafkaServers := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

	log.Printf("Starting %s", serviceName)

	rand.Seed(time.Now().UnixNano())
	tracer := otel.Tracer(serviceName)

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BootstrapServers: kafkaServers,
		ClientID:         serviceName,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: kafkaServers,
		GroupID:          serviceName,
		Topics:           []string{"quality.disposition"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer.RegisterHandler("quality.disposition", handleQualityDisposition(context.Background(), producer, tracer))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumer.Start(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	cancel()
	time.Sleep(time.Second)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
