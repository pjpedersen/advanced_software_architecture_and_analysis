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
	"github.com/i40/production-system/services/plc-adapter-p2/internal/api"
	"github.com/i40/production-system/services/plc-adapter-p2/internal/config"
	"github.com/i40/production-system/services/plc-adapter-p2/internal/service"
)

func main() {
	log.Println("Starting PLC Adapter for Assembly Line (P2)...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	producer, err := sharedkafka.NewProducer(sharedkafka.ProducerConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		ClientID:         "plc-adapter-p2",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	consumer, err := sharedkafka.NewConsumer(sharedkafka.ConsumerConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		GroupID:          "plc-adapter-p2-consumer",
		Topics:           []string{"production.plan", "quality.disposition"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	svc := service.NewService(service.ServiceConfig{
		Producer: producer,
	})

	consumer.RegisterHandler("production.plan", svc.HandleProductionPlan)
	consumer.RegisterHandler("quality.disposition", svc.HandleQualityDisposition)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := consumer.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}()

	httpServer := api.NewServer(api.ServerConfig{
		Service: svc,
		Port:    cfg.HTTPPort,
	})

	go func() {
		log.Printf("HTTP server listening on :%d", cfg.HTTPPort)
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	cancel()
	log.Println("Shutdown complete")
}
