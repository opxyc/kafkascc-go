package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the application
type Config struct {
	Kafka struct {
		Brokers     []string
		GroupID     string
		Topic       string
		OffsetReset string
		Version     string
		Oldest      bool
	}
	LogLevel      string
	APIBaseURL    string
	APIHealthPath string
	PingInterval  time.Duration
	BackoffBase   time.Duration
	BackoffMax    time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{}

	// Kafka configuration
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.Kafka.Brokers = strings.Split(brokers, ",")
	} else {
		cfg.Kafka.Brokers = []string{"localhost:9094"} // Default to external port
	}

	cfg.Kafka.GroupID = getEnv("KAFKA_GROUP_ID", "event-processor-group")
	cfg.Kafka.Topic = getEnv("KAFKA_TOPIC", "test-topic")
	cfg.Kafka.OffsetReset = getEnv("KAFKA_OFFSET_RESET", "latest")
	cfg.Kafka.Version = getEnv("KAFKA_VERSION", "2.8.0")
	cfg.Kafka.Oldest = getEnvAsBool("KAFKA_OLDEST", false)

	// Application Configuration
	cfg.LogLevel = getEnv("LOG_LEVEL", "info")

	// API health/ping configuration
	cfg.APIBaseURL = getEnv("API_BASE_URL", "http://localhost:5500")
	cfg.APIHealthPath = getEnv("API_HEALTH_PATH", "/")
	if iv := getEnv("PING_INTERVAL_SECONDS", "10"); iv != "" {
		if n, err := strconv.Atoi(iv); err == nil && n > 0 {
			cfg.PingInterval = time.Duration(n) * time.Second
		} else {
			cfg.PingInterval = 10 * time.Second
		}
	} else {
		cfg.PingInterval = 10 * time.Second
	}

	// Retry backoff (milliseconds)
	if v := getEnv("RETRY_BACKOFF_BASE_MS", "1000"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.BackoffBase = time.Duration(n) * time.Millisecond
		} else {
			cfg.BackoffBase = 1 * time.Second
		}
	} else {
		cfg.BackoffBase = 1 * time.Second
	}

	if v := getEnv("RETRY_BACKOFF_MAX_MS", "60000"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.BackoffMax = time.Duration(n) * time.Millisecond
		} else {
			cfg.BackoffMax = 60 * time.Second
		}
	} else {
		cfg.BackoffMax = 60 * time.Second
	}

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseBool(valueStr); err == nil {
		return value
	}
	return defaultValue
}
