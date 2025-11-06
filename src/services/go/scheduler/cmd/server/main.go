package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	sharedkafka "github.com/i40/production-system/services/go/pkg/kafka"
	"github.com/i40/production-system/services/scheduler/internal/api"
	"github.com/i40/production-system/services/scheduler/internal/config"
	"github.com/i40/production-system/services/scheduler/internal/scheduler"
	"github.com/i40/production-system/services/scheduler/internal/storage"
)

func main() {
	log.Println("Starting Scheduler Service...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize storage
	store, err := storage.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Initialize Kafka producer
	producer, err := sharedkafka.NewProducer(sharedkafka.ProducerConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		ClientID:         "scheduler-service",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Initialize Kafka consumer for ACKs
	consumer, err := sharedkafka.NewConsumer(sharedkafka.ConsumerConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		GroupID:          "scheduler-ack-consumer",
		Topics:           []string{"adapter.ack"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Initialize scheduler service
	svc := scheduler.NewService(scheduler.ServiceConfig{
		Store:    store,
		Producer: producer,
	})

	// Register ACK handler
	consumer.RegisterHandler("adapter.ack", svc.HandleAdapterAck)

	// Start consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := consumer.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Initialize HTTP API
	httpServer := api.NewServer(api.ServerConfig{
		Service: svc,
		Port:    cfg.HTTPPort,
	})

	// Start HTTP server
	go func() {
		log.Printf("HTTP server listening on :%d", cfg.HTTPPort)
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	cancel() // Stop consumer
	log.Println("Shutdown complete")
}
