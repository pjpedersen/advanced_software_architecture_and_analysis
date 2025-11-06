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

type EngravingRequestEvent struct {
	JobID         string                 `json:"job_id"`
	DeviceID      string                 `json:"device_id"`
	UnitID        string                 `json:"unit_id"`
	CustomerOrder string                 `json:"customer_order"`
	EngravingData map[string]interface{} `json:"engraving_data"`
}

type EngravingCompleteEvent struct {
	Metadata    EventMetadata `json:"metadata"`
	JobID       string        `json:"job_id"`
	DeviceID    string        `json:"device_id"`
	UnitID      string        `json:"unit_id"`
	Status      string        `json:"status"`
	CompletedAt time.Time     `json:"completed_at"`
}

type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	TraceID       string    `json:"trace_id,omitempty"`
}

func handleEngravingRequest(ctx context.Context, producer *kafka.Producer, tracer trace.Tracer) kafka.MessageHandler {
	return func(ctx context.Context, key string, value []byte) error {
		ctx, span := tracer.Start(ctx, "engraver.HandleEngravingRequest")
		defer span.End()

		var event EngravingRequestEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		log.Printf("Processing engraving job %s for device %s", event.JobID, event.UnitID)

		// Simulate engraving work
		time.Sleep(2 * time.Second)

		// Publish completion event
		completeEvent := EngravingCompleteEvent{
			Metadata: EventMetadata{
				EventID:       uuid.New().String(),
				EventType:     "customization.engraving.complete",
				Timestamp:     time.Now(),
				SourceService: "engraver-adapter",
				TraceID:       span.SpanContext().TraceID().String(),
			},
			JobID:       event.JobID,
			DeviceID:    event.DeviceID,
			UnitID:      event.UnitID,
			Status:      "SUCCESS",
			CompletedAt: time.Now(),
		}

		return producer.ProduceEvent(ctx, "customization.engraving.complete", event.JobID, completeEvent)
	}
}

func main() {
	serviceName := getEnv("SERVICE_NAME", "engraver-adapter")
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
		Topics:           []string{"customization.engraving.request"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer.RegisterHandler("customization.engraving.request", handleEngravingRequest(context.Background(), producer, tracer))

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
