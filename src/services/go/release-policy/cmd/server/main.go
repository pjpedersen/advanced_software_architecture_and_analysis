package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/i40/production-system/services/go/pkg/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type PostFlashTestEvent struct {
	DeviceID string       `json:"device_id"`
	UnitID   string       `json:"unit_id"`
	Status   string       `json:"status"`
	Tests    []TestResult `json:"tests"`
}

type TestResult struct {
	TestName string `json:"test_name"`
	Passed   bool   `json:"passed"`
}

type DeviceReleasedEvent struct {
	Metadata    EventMetadata `json:"metadata"`
	DeviceID    string        `json:"device_id"`
	UnitID      string        `json:"unit_id"`
	ReleaseTime time.Time     `json:"release_time"`
	Decision    string        `json:"decision"` // RELEASE or QUARANTINE
	Reason      string        `json:"reason"`
}

type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	TraceID       string    `json:"trace_id,omitempty"`
}

func handlePostFlashTest(ctx context.Context, producer *kafka.Producer, tracer trace.Tracer) kafka.MessageHandler {
	return func(ctx context.Context, key string, value []byte) error {
		ctx, span := tracer.Start(ctx, "release.HandlePostFlashTest")
		defer span.End()

		var event PostFlashTestEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		log.Printf("Evaluating release policy for device %s", event.UnitID)

		// Check if all tests passed
		decision := "RELEASE"
		reason := "All post-flash tests passed"

		for _, test := range event.Tests {
			if !test.Passed {
				decision = "QUARANTINE"
				reason = fmt.Sprintf("Test failed: %s", test.TestName)
				break
			}
		}

		releaseEvent := DeviceReleasedEvent{
			Metadata: EventMetadata{
				EventID:       uuid.New().String(),
				EventType:     "device.released",
				Timestamp:     time.Now(),
				SourceService: "release-policy",
				TraceID:       span.SpanContext().TraceID().String(),
			},
			DeviceID:    event.DeviceID,
			UnitID:      event.UnitID,
			ReleaseTime: time.Now(),
			Decision:    decision,
			Reason:      reason,
		}

		return producer.ProduceEvent(ctx, "device.released", event.DeviceID, releaseEvent)
	}
}

func main() {
	serviceName := getEnv("SERVICE_NAME", "release-policy")
	kafkaServers := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

	log.Printf("Starting %s", serviceName)

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
		Topics:           []string{"device.postflash.test"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer.RegisterHandler("device.postflash.test", handlePostFlashTest(context.Background(), producer, tracer))

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
