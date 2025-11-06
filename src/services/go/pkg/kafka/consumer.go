package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Consumer struct {
	consumer *kafka.Consumer
	tracer   trace.Tracer
	handlers map[string]MessageHandler
}

type MessageHandler func(ctx context.Context, key string, value []byte) error

type ConsumerConfig struct {
	BootstrapServers string
	GroupID          string
	Topics           []string
	AutoOffsetReset  string
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           cfg.GroupID,
		"auto.offset.reset":  cfg.AutoOffsetReset,
		"enable.auto.commit": false, // Manual commit for exactly-once
		"max.poll.interval.ms": 300000,
		"session.timeout.ms":   45000,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	err = consumer.SubscribeTopics(cfg.Topics, nil)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	return &Consumer{
		consumer: consumer,
		tracer:   otel.Tracer("kafka-consumer"),
		handlers: make(map[string]MessageHandler),
	}, nil
}

// RegisterHandler registers a message handler for a specific topic
func (c *Consumer) RegisterHandler(topic string, handler MessageHandler) {
	c.handlers[topic] = handler
}

// Start begins consuming messages
func (c *Consumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				return fmt.Errorf("consumer error: %w", err)
			}

			if err := c.processMessage(ctx, msg); err != nil {
				fmt.Printf("Failed to process message: %v\n", err)
				// Continue processing other messages
				continue
			}

			// Commit offset after successful processing
			_, err = c.consumer.CommitMessage(msg)
			if err != nil {
				fmt.Printf("Failed to commit offset: %v\n", err)
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message) error {
	topic := *msg.TopicPartition.Topic

	// Extract trace context from headers
	var traceID, spanID string
	for _, header := range msg.Headers {
		switch header.Key {
		case "trace-id":
			traceID = string(header.Value)
		case "span-id":
			spanID = string(header.Value)
		}
	}

	// Start span
	ctx, span := c.tracer.Start(ctx, "kafka.consume",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.Int("partition", int(msg.TopicPartition.Partition)),
			attribute.Int64("offset", int64(msg.TopicPartition.Offset)),
			attribute.String("trace_id", traceID),
			attribute.String("span_id", spanID),
		))
	defer span.End()

	// Find handler
	handler, exists := c.handlers[topic]
	if !exists {
		return fmt.Errorf("no handler registered for topic: %s", topic)
	}

	// Execute handler
	key := string(msg.Key)
	if err := handler(ctx, key, msg.Value); err != nil {
		span.RecordError(err)
		return fmt.Errorf("handler failed: %w", err)
	}

	span.AddEvent("message processed successfully")
	return nil
}

// UnmarshalEvent is a helper to unmarshal event payloads
func UnmarshalEvent(data []byte, event interface{}) error {
	return json.Unmarshal(data, event)
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
