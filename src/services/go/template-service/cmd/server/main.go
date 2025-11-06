package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/i40/production-system/services/go/pkg/kafka"
	"github.com/i40/production-system/services/go/template-service/internal/api"
	"github.com/i40/production-system/services/go/template-service/internal/config"
	"github.com/i40/production-system/services/go/template-service/internal/service"
	"github.com/i40/production-system/services/go/template-service/internal/storage"
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
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	log.Printf("Starting %s on port %d", cfg.ServiceName, cfg.Port)

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

	store, err := storage.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}
	defer store.Close()

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		ClientID:         cfg.ServiceName,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	svc := service.NewService(store, producer, tracer)

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: cfg.KafkaBootstrapServers,
		GroupID:          cfg.KafkaGroupID,
		Topics:           []string{"device.registered", "customization.engraving.complete"},
		AutoOffsetReset:  "earliest",
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	consumer.RegisterHandler("device.registered", svc.HandleDeviceRegistered)
	consumer.RegisterHandler("customization.engraving.complete", svc.HandleEngravingComplete)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerErrChan := make(chan error, 1)
	go func() {
		if err := consumer.Start(ctx); err != nil {
			consumerErrChan <- fmt.Errorf("consumer error: %w", err)
		}
	}()

	apiServer := api.NewServer(svc, tracer)
	addr := fmt.Sprintf(":%d", cfg.Port)

	serverErrChan := make(chan error, 1)
	go func() {
		log.Printf("HTTP server listening on %s", addr)
		if err := api.StartServer(ctx, addr, apiServer); err != nil {
			serverErrChan <- fmt.Errorf("server error: %w", err)
		}
	}()

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
