package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	HTTPPort             int
	KafkaBootstrapServers string
	DatabaseURL          string
	ServiceName          string
	LogLevel             string
}

func Load() (*Config, error) {
	cfg := &Config{
		HTTPPort:             getEnvAsInt("HTTP_PORT", 8080),
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		DatabaseURL:          getEnv("DATABASE_URL", "postgres://i40admin:i40secret@localhost:5432/i40_production?sslmode=disable"),
		ServiceName:          getEnv("SERVICE_NAME", "scheduler-service"),
		LogLevel:             getEnv("LOG_LEVEL", "info"),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.KafkaBootstrapServers == "" {
		return fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS is required")
	}
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}
