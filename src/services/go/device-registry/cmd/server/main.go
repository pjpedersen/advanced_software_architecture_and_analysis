package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/i40/production-system/services/device-registry/internal/api"
	"github.com/i40/production-system/services/device-registry/internal/config"
	"github.com/i40/production-system/services/device-registry/internal/service"
	"github.com/i40/production-system/services/device-registry/internal/storage"
	sharedkafka "github.com/i40/production-system/services/go/pkg/kafka"
)

func main() {
	log.Println("Starting Device Registry Service...")

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
		ClientID:         "device-registry",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Initialize service
	svc := service.NewService(service.ServiceConfig{
		Store:    store,
		Producer: producer,
	})

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

	log.Println("Shutdown complete")
}
