package config

import (
	"os"
	"testing"
	"time"

	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
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
	t.Setenv("TASKFORGE_SCHEDULER_LOCK_TTL", "")
	t.Setenv("TASKFORGE_SCHEDULER_RENEW_INTERVAL", "")
	t.Setenv("TASKFORGE_SCHEDULES_JSON", "")
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
	if cfg.SchedulerLockTTL != defaultSchedulerLockTTL {
		t.Fatalf("SchedulerLockTTL = %v, want %v", cfg.SchedulerLockTTL, defaultSchedulerLockTTL)
	}
	if cfg.SchedulerRenewInterval != defaultSchedulerRenew {
		t.Fatalf("SchedulerRenewInterval = %v, want %v", cfg.SchedulerRenewInterval, defaultSchedulerRenew)
	}
	if len(cfg.RecurringSchedules) != 0 {
		t.Fatalf("RecurringSchedules length = %d, want 0", len(cfg.RecurringSchedules))
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
	t.Setenv("TASKFORGE_SCHEDULER_LOCK_TTL", "20s")
	t.Setenv("TASKFORGE_SCHEDULER_RENEW_INTERVAL", "4s")
	t.Setenv("TASKFORGE_SCHEDULES_JSON", `[{"id":"nightly","interval":"15m","queue":"critical","task_name":"demo.nightly","payload":{"job":"nightly"},"headers":{"x-source":"config"},"misfire_policy":"coalesce","start_at":"2026-04-14T10:00:00Z"}]`)
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
	if cfg.SchedulerLockTTL != 20*time.Second || cfg.SchedulerRenewInterval != 4*time.Second {
		t.Fatalf("unexpected scheduler fields: %+v", cfg)
	}
	if len(cfg.RecurringSchedules) != 1 {
		t.Fatalf("RecurringSchedules length = %d, want 1", len(cfg.RecurringSchedules))
	}
	schedule := cfg.RecurringSchedules[0]
	if schedule.ID != "nightly" || schedule.Queue != "critical" || schedule.TaskName != "demo.nightly" {
		t.Fatalf("unexpected schedule identity fields: %+v", schedule)
	}
	if schedule.Interval != 15*time.Minute {
		t.Fatalf("schedule interval = %v, want %v", schedule.Interval, 15*time.Minute)
	}
	if string(schedule.Payload) != `{"job":"nightly"}` {
		t.Fatalf("schedule payload = %s, want %s", schedule.Payload, `{"job":"nightly"}`)
	}
	if schedule.Headers["x-source"] != "config" {
		t.Fatalf("schedule headers = %+v, want x-source=config", schedule.Headers)
	}
	if schedule.MisfirePolicy != schedulerpkg.MisfirePolicyCoalesce {
		t.Fatalf("schedule misfire policy = %q, want %q", schedule.MisfirePolicy, schedulerpkg.MisfirePolicyCoalesce)
	}
	if schedule.StartAt == nil || schedule.StartAt.Format(time.RFC3339) != "2026-04-14T10:00:00Z" {
		t.Fatalf("schedule start_at = %v, want 2026-04-14T10:00:00Z", schedule.StartAt)
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

func TestLoadRejectsInvalidScheduleJSON(t *testing.T) {
	t.Setenv("TASKFORGE_SCHEDULES_JSON", `{"id":"bad"}`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsDuplicateScheduleIDs(t *testing.T) {
	t.Setenv("TASKFORGE_SCHEDULES_JSON", `[{"id":"dup","interval":"1m","task_name":"demo.one","payload":{}},{"id":"dup","interval":"2m","task_name":"demo.two","payload":{}}]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsUnsupportedMisfirePolicy(t *testing.T) {
	t.Setenv("TASKFORGE_SCHEDULES_JSON", `[{"id":"bad-policy","interval":"1m","task_name":"demo.one","payload":{},"misfire_policy":"skip"}]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsInvalidSchedulerIntervals(t *testing.T) {
	t.Setenv("TASKFORGE_SCHEDULER_LOCK_TTL", "5s")
	t.Setenv("TASKFORGE_SCHEDULER_RENEW_INTERVAL", "5s")

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
