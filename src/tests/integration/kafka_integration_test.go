package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKafkaProducerConsumer tests end-to-end Kafka message flow
func TestKafkaProducerConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Kafka configuration
	bootstrapServers := "localhost:9092"
	testTopic := "test.integration.topic"

	// Create producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
	})
	require.NoError(t, err)
	defer producer.Close()

	// Create consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "test-group",
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.Subscribe(testTopic, nil)
	require.NoError(t, err)

	// Test message
	testMessage := map[string]interface{}{
		"event_id":   "test-001",
		"event_type": "test.event",
		"timestamp":  time.Now().Format(time.RFC3339),
		"data": map[string]string{
			"key": "value",
		},
	}

	messageBytes, err := json.Marshal(testMessage)
	require.NoError(t, err)

	// Produce message
	deliveryChan := make(chan kafka.Event)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: kafka.PartitionAny},
		Value:          messageBytes,
		Key:            []byte("test-key"),
	}, deliveryChan)
	require.NoError(t, err)

	// Wait for delivery confirmation
	e := <-deliveryChan
	m := e.(*kafka.Message)
	assert.Nil(t, m.TopicPartition.Error)

	// Consume message
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var receivedMessage map[string]interface{}
	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for message")
		default:
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err != nil {
				continue
			}

			err = json.Unmarshal(msg.Value, &receivedMessage)
			require.NoError(t, err)

			// Verify message content
			assert.Equal(t, testMessage["event_id"], receivedMessage["event_id"])
			assert.Equal(t, testMessage["event_type"], receivedMessage["event_type"])
			return
		}
	}
}

// TestDeviceRegistrationFlow tests the device registration integration
func TestDeviceRegistrationFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// This test would verify:
	// 1. Device registration event is published
	// 2. Device registry service processes it
	// 3. Genealogy records are created
	// 4. Device registered event is published
	// 5. Template service receives event and checks for engraving

	t.Log("Testing device registration flow")
	// TODO: Implement full integration test
}

// TestQualityDispositionFlow tests quality check integration
func TestQualityDispositionFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// This test would verify:
	// 1. Test metrics are published
	// 2. Quality service evaluates metrics
	// 3. Disposition decision is made
	// 4. Validation controller receives disposition
	// 5. Additional validation tests are run

	t.Log("Testing quality disposition flow")
	// TODO: Implement full integration test
}
