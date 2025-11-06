package config

import (
	"os"
	"strconv"
)

type Config struct {
	HTTPPort              int
	KafkaBootstrapServers string
	ServiceName           string
	LogLevel              string
}

func Load() (*Config, error) {
	cfg := &Config{
		HTTPPort:              getEnvAsInt("HTTP_PORT", 8083),
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		ServiceName:           getEnv("SERVICE_NAME", "plc-adapter-p2"),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
	}
	return cfg, nil
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
