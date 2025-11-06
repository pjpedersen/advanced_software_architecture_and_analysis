package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/i40/production-system/services/go/pkg/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type ChangeoverStartEvent struct {
	PlanID     string    `json:"plan_id"`
	OldBatchID string    `json:"old_batch_id"`
	NewBatchID string    `json:"new_batch_id"`
	StartedAt  time.Time `json:"started_at"`
}

type OperatorPromptEvent struct {
	PlanID   string   `json:"plan_id"`
	BatchID  string   `json:"batch_id"`
	Actions  []string `json:"actions"`
	Priority string   `json:"priority"`
}

func handleChangeoverStart(ctx context.Context, producer *kafka.Producer, tracer trace.Tracer) kafka.MessageHandler {
	return func(ctx context.Context, key string, value []byte) error {
		ctx, span := tracer.Start(ctx, "hmi.HandleChangeoverStart")
		defer span.End()

		var event ChangeoverStartEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		log.Printf("Changeover started: %s -> %s", event.OldBatchID, event.NewBatchID)

		// Create operator prompts
		prompt := OperatorPromptEvent{
			PlanID:   event.PlanID,
			BatchID:  event.NewBatchID,
			Actions:  []string{"Change color pellets", "Clean mold", "Update settings"},
			Priority: "HIGH",
		}

		return producer.ProduceEvent(ctx, "production.operator.prompt", event.PlanID, prompt)
	}
}

func main() {
	serviceName := getEnv("SERVICE_NAME", "hmi-service")
	kafkaServers := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	port := getEnv("PORT", "8088")

	log.Printf("Starting %s on port %s", serviceName, port)

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
		Topics:           []string{"production.changeover.start"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer.RegisterHandler("production.changeover.start", handleChangeoverStart(context.Background(), producer, tracer))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumer.Start(ctx)

	// Simple HTTP API for HMI interface
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	go func() {
		log.Printf("HTTP server listening on :%s", port)
		http.ListenAndServe(":"+port, nil)
	}()

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
