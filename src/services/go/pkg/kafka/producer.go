package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Producer struct {
	producer *kafka.Producer
	tracer   trace.Tracer
}

type ProducerConfig struct {
	BootstrapServers string
	ClientID         string
}

func NewProducer(cfg ProducerConfig) (*Producer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
		"client.id":         cfg.ClientID,
		"acks":              "all",
		"retries":           10,
		"max.in.flight.requests.per.connection": 5,
		"enable.idempotence":                    true,
		"compression.type":                      "snappy",
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	// Start delivery report handler
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				}
			}
		}
	}()

	return &Producer{
		producer: producer,
		tracer:   otel.Tracer("kafka-producer"),
	}, nil
}

// ProduceEvent publishes an event with correlation context
func (p *Producer) ProduceEvent(ctx context.Context, topic string, key string, event interface{}) error {
	ctx, span := p.tracer.Start(ctx, "kafka.produce",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
		))
	defer span.End()

	// Serialize event
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Extract trace context
	headers := make([]kafka.Header, 0)
	spanContext := span.SpanContext()
	if spanContext.IsValid() {
		headers = append(headers,
			kafka.Header{Key: "trace-id", Value: []byte(spanContext.TraceID().String())},
			kafka.Header{Key: "span-id", Value: []byte(spanContext.SpanID().String())},
		)
	}

	// Produce message
	deliveryChan := make(chan kafka.Event)
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          eventBytes,
		Headers:        headers,
		Timestamp:      time.Now(),
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery confirmation (with timeout)
	select {
	case e := <-deliveryChan:
		msg := e.(*kafka.Message)
		if msg.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", msg.TopicPartition.Error)
		}
		span.AddEvent("message delivered",
			trace.WithAttributes(
				attribute.Int("partition", int(msg.TopicPartition.Partition)),
				attribute.Int64("offset", int64(msg.TopicPartition.Offset)),
			))
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("delivery timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ProduceWithOutbox produces event through transactional outbox pattern
func (p *Producer) ProduceWithOutbox(ctx context.Context, topic string, key string, event interface{}, txFunc func(eventID uuid.UUID, payload []byte) error) error {
	eventID := uuid.New()

	// Serialize event
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Execute transaction
	if err := txFunc(eventID, eventBytes); err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	// Actual publishing happens via outbox relay worker
	return nil
}

func (p *Producer) Close() {
	p.producer.Flush(10000)
	p.producer.Close()
}
