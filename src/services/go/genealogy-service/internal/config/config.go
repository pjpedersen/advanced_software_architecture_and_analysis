package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds the service configuration
type Config struct {
	ServiceName string
	Port        int

	// Kafka configuration
	KafkaBootstrapServers string
	KafkaGroupID          string

	// Database configuration
	DatabaseURL string

	// Observability
	JaegerEndpoint string
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	cfg := &Config{
		ServiceName:           getEnv("SERVICE_NAME", "genealogy-service"),
		Port:                  getEnvInt("PORT", 8089),
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		KafkaGroupID:          getEnv("KAFKA_GROUP_ID", "genealogy-service"),
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/i40_production?sslmode=disable"),
		JaegerEndpoint:        getEnv("JAEGER_ENDPOINT", "http://localhost:14268/api/traces"),
	}

	if cfg.KafkaBootstrapServers == "" {
		return nil, fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS is required")
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
