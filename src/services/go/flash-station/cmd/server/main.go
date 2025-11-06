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

type FirmwareFlashRequestEvent struct {
	DeviceID        string `json:"device_id"`
	UnitID          string `json:"unit_id"`
	FirmwareVersion string `json:"firmware_version"`
	FirmwareHash    string `json:"firmware_hash"`
}

type PostFlashTestEvent struct {
	Metadata EventMetadata `json:"metadata"`
	DeviceID string        `json:"device_id"`
	UnitID   string        `json:"unit_id"`
	Status   string        `json:"status"`
	Tests    []TestResult  `json:"tests"`
}

type TestResult struct {
	TestName string `json:"test_name"`
	Passed   bool   `json:"passed"`
}

type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	TraceID       string    `json:"trace_id,omitempty"`
}

func handleFirmwareFlashRequest(ctx context.Context, producer *kafka.Producer, tracer trace.Tracer) kafka.MessageHandler {
	return func(ctx context.Context, key string, value []byte) error {
		ctx, span := tracer.Start(ctx, "flash.HandleFirmwareFlashRequest")
		defer span.End()

		var event FirmwareFlashRequestEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		log.Printf("Flashing firmware %s to device %s", event.FirmwareVersion, event.UnitID)

		// Simulate firmware flashing
		time.Sleep(3 * time.Second)

		// Run post-flash tests
		testEvent := PostFlashTestEvent{
			Metadata: EventMetadata{
				EventID:       uuid.New().String(),
				EventType:     "device.postflash.test",
				Timestamp:     time.Now(),
				SourceService: "flash-station",
				TraceID:       span.SpanContext().TraceID().String(),
			},
			DeviceID: event.DeviceID,
			UnitID:   event.UnitID,
			Status:   "PASS",
			Tests: []TestResult{
				{TestName: "Boot Test", Passed: true},
				{TestName: "Connectivity Test", Passed: true},
				{TestName: "Sensor Test", Passed: true},
			},
		}

		return producer.ProduceEvent(ctx, "device.postflash.test", event.DeviceID, testEvent)
	}
}

func main() {
	serviceName := getEnv("SERVICE_NAME", "flash-station")
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
		Topics:           []string{"device.firmware.flash.request"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer.RegisterHandler("device.firmware.flash.request", handleFirmwareFlashRequest(context.Background(), producer, tracer))

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
