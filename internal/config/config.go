package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultLogLevel         = "info"
	defaultHTTPAddr         = ":8080"
	defaultMetricsAddr      = ":8080"
	defaultRedisAddr        = "localhost:6379"
	defaultRedisDB          = 0
	defaultWorkerConcurrent = 4
	defaultPollInterval     = time.Second
	defaultLeaseTTL         = 30 * time.Second
	defaultShutdownTimeout  = 10 * time.Second
)

type Config struct {
	LogLevel        string
	HTTPAddr        string
	MetricsAddr     string
	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	WorkerCount     int
	PollInterval    time.Duration
	LeaseTTL        time.Duration
	ShutdownTimeout time.Duration
	OTELEnabled     bool
	ServiceName     string
}

func Load(defaultServiceName string) (Config, error) {
	cfg := Config{
		LogLevel:        getEnv("TASKFORGE_LOG_LEVEL", defaultLogLevel),
		HTTPAddr:        getEnv("TASKFORGE_HTTP_ADDR", defaultHTTPAddr),
		MetricsAddr:     getEnv("TASKFORGE_METRICS_ADDR", defaultMetricsAddr),
		RedisAddr:       getEnv("TASKFORGE_REDIS_ADDR", defaultRedisAddr),
		RedisPassword:   getEnv("TASKFORGE_REDIS_PASSWORD", ""),
		RedisDB:         defaultRedisDB,
		WorkerCount:     defaultWorkerConcurrent,
		PollInterval:    defaultPollInterval,
		LeaseTTL:        defaultLeaseTTL,
		ShutdownTimeout: defaultShutdownTimeout,
		ServiceName:     getEnv("TASKFORGE_SERVICE_NAME", defaultServiceName),
	}

	var err error
	if cfg.RedisDB, err = getEnvInt("TASKFORGE_REDIS_DB", defaultRedisDB); err != nil {
		return Config{}, err
	}
	if cfg.WorkerCount, err = getEnvInt("TASKFORGE_WORKER_CONCURRENCY", defaultWorkerConcurrent); err != nil {
		return Config{}, err
	}
	if cfg.WorkerCount < 1 {
		return Config{}, fmt.Errorf("TASKFORGE_WORKER_CONCURRENCY must be >= 1")
	}
	if cfg.PollInterval, err = getEnvDuration("TASKFORGE_POLL_INTERVAL", defaultPollInterval); err != nil {
		return Config{}, err
	}
	if cfg.LeaseTTL, err = getEnvDuration("TASKFORGE_LEASE_TTL", defaultLeaseTTL); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = getEnvDuration("TASKFORGE_SHUTDOWN_TIMEOUT", defaultShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.OTELEnabled, err = getEnvBool("TASKFORGE_OTEL_ENABLED", false); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	value := getEnv(key, "")
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s: parse int: %w", key, err)
	}
	return parsed, nil
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := getEnv(key, "")
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: parse duration: %w", key, err)
	}
	return parsed, nil
}

func getEnvBool(key string, fallback bool) (bool, error) {
	value := getEnv(key, "")
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s: parse bool: %w", key, err)
	}
	return parsed, nil
}
