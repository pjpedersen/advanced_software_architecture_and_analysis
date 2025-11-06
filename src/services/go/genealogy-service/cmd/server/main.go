package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/i40/production-system/services/go/genealogy-service/internal/api"
	"github.com/i40/production-system/services/go/genealogy-service/internal/config"
	"github.com/i40/production-system/services/go/genealogy-service/internal/service"
	"github.com/i40/production-system/services/go/genealogy-service/internal/storage"
	"github.com/i40/production-system/services/go/pkg/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	log.Printf("Starting %s on port %d", cfg.ServiceName, cfg.Port)

	// Initialize tracer
	tp, err := initTracer(cfg)
	if err != nil {
		log.Printf("Failed to initialize tracer: %v", err)
	} else {
		defer func() {
			if err := tp.Shutdown(context.Background()); err != nil {
				log.Printf("Error shutting down tracer provider: %v", err)
			}
		}()
	}

	tracer := otel.Tracer(cfg.ServiceName)

	// Initialize storage
	store, err := storage.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}
	defer store.Close()

	// Initialize Kafka producer
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		ClientID:         cfg.ServiceName,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	// Initialize service
	svc := service.NewService(store, producer, tracer)

	// Initialize Kafka consumer for recall query requests
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		GroupID:          cfg.KafkaGroupID,
		Topics:           []string{"genealogy.recall.query"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	// Register event handlers
	consumer.RegisterHandler("genealogy.recall.query", svc.HandleRecallQuery)

	// Start consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerErrChan := make(chan error, 1)
	go func() {
		if err := consumer.Start(ctx); err != nil {
			consumerErrChan <- fmt.Errorf("consumer error: %w", err)
		}
	}()

	// Initialize HTTP server
	apiServer := api.NewServer(svc, tracer)
	addr := fmt.Sprintf(":%d", cfg.Port)

	serverErrChan := make(chan error, 1)
	go func() {
		log.Printf("HTTP server listening on %s", addr)
		if err := api.StartServer(ctx, addr, apiServer); err != nil {
			serverErrChan <- fmt.Errorf("server error: %w", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-consumerErrChan:
		return err
	case err := <-serverErrChan:
		return err
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down gracefully...", sig)
		cancel()

		// Give services time to shutdown gracefully
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		<-shutdownCtx.Done()
		return nil
	}
}

func initTracer(cfg *config.Config) (*tracesdk.TracerProvider, error) {
	if cfg.JaegerEndpoint == "" {
		return tracesdk.NewTracerProvider(), nil
	}

	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(cfg.JaegerEndpoint)))
	if err != nil {
		return nil, err
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exporter),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
		)),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}
