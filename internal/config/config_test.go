package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("TASKFORGE_LOG_LEVEL", "")
	t.Setenv("TASKFORGE_HTTP_ADDR", "")
	t.Setenv("TASKFORGE_METRICS_ADDR", "")
	t.Setenv("TASKFORGE_REDIS_ADDR", "")
	t.Setenv("TASKFORGE_REDIS_PASSWORD", "")
	t.Setenv("TASKFORGE_REDIS_DB", "")
	t.Setenv("TASKFORGE_WORKER_CONCURRENCY", "")
	t.Setenv("TASKFORGE_POLL_INTERVAL", "")
	t.Setenv("TASKFORGE_LEASE_TTL", "")
	t.Setenv("TASKFORGE_SHUTDOWN_TIMEOUT", "")
	t.Setenv("TASKFORGE_OTEL_ENABLED", "")
	t.Setenv("TASKFORGE_SERVICE_NAME", "")

	cfg, err := Load("taskforge-test")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.LogLevel != defaultLogLevel {
		t.Fatalf("LogLevel = %q, want %q", cfg.LogLevel, defaultLogLevel)
	}
	if cfg.HTTPAddr != defaultHTTPAddr {
		t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, defaultHTTPAddr)
	}
	if cfg.WorkerCount != defaultWorkerConcurrent {
		t.Fatalf("WorkerCount = %d, want %d", cfg.WorkerCount, defaultWorkerConcurrent)
	}
	if cfg.PollInterval != defaultPollInterval {
		t.Fatalf("PollInterval = %v, want %v", cfg.PollInterval, defaultPollInterval)
	}
	if cfg.ServiceName != "taskforge-test" {
		t.Fatalf("ServiceName = %q, want %q", cfg.ServiceName, "taskforge-test")
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("TASKFORGE_LOG_LEVEL", "debug")
	t.Setenv("TASKFORGE_HTTP_ADDR", ":9090")
	t.Setenv("TASKFORGE_METRICS_ADDR", ":9091")
	t.Setenv("TASKFORGE_REDIS_ADDR", "redis.internal:6379")
	t.Setenv("TASKFORGE_REDIS_PASSWORD", "secret")
	t.Setenv("TASKFORGE_REDIS_DB", "2")
	t.Setenv("TASKFORGE_WORKER_CONCURRENCY", "8")
	t.Setenv("TASKFORGE_POLL_INTERVAL", "250ms")
	t.Setenv("TASKFORGE_LEASE_TTL", "45s")
	t.Setenv("TASKFORGE_SHUTDOWN_TIMEOUT", "20s")
	t.Setenv("TASKFORGE_OTEL_ENABLED", "true")
	t.Setenv("TASKFORGE_SERVICE_NAME", "custom-service")

	cfg, err := Load("ignored")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.LogLevel != "debug" || cfg.HTTPAddr != ":9090" || cfg.MetricsAddr != ":9091" {
		t.Fatalf("unexpected address/log fields: %+v", cfg)
	}
	if cfg.RedisAddr != "redis.internal:6379" || cfg.RedisPassword != "secret" || cfg.RedisDB != 2 {
		t.Fatalf("unexpected redis fields: %+v", cfg)
	}
	if cfg.WorkerCount != 8 || cfg.PollInterval != 250*time.Millisecond || cfg.LeaseTTL != 45*time.Second {
		t.Fatalf("unexpected runtime fields: %+v", cfg)
	}
	if !cfg.OTELEnabled || cfg.ServiceName != "custom-service" {
		t.Fatalf("unexpected otel/service fields: %+v", cfg)
	}
}

func TestLoadInvalidDuration(t *testing.T) {
	t.Setenv("TASKFORGE_POLL_INTERVAL", "not-a-duration")

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadInvalidBoolean(t *testing.T) {
	t.Setenv("TASKFORGE_OTEL_ENABLED", "maybe")

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadInvalidConcurrency(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_CONCURRENCY", "0")

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
