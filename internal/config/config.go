package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
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
	defaultSchedulerLockTTL = 15 * time.Second
	defaultSchedulerRenew   = 5 * time.Second
)

type Config struct {
	LogLevel               string
	HTTPAddr               string
	MetricsAddr            string
	RedisAddr              string
	RedisPassword          string
	RedisDB                int
	WorkerCount            int
	PollInterval           time.Duration
	LeaseTTL               time.Duration
	ShutdownTimeout        time.Duration
	SchedulerLockTTL       time.Duration
	SchedulerRenewInterval time.Duration
	RecurringSchedules     []schedulerpkg.ScheduleDefinition
	OTELEnabled            bool
	ServiceName            string
}

func Load(defaultServiceName string) (Config, error) {
	cfg := Config{
		LogLevel:               getEnv("TASKFORGE_LOG_LEVEL", defaultLogLevel),
		HTTPAddr:               getEnv("TASKFORGE_HTTP_ADDR", defaultHTTPAddr),
		MetricsAddr:            getEnv("TASKFORGE_METRICS_ADDR", defaultMetricsAddr),
		RedisAddr:              getEnv("TASKFORGE_REDIS_ADDR", defaultRedisAddr),
		RedisPassword:          getEnv("TASKFORGE_REDIS_PASSWORD", ""),
		RedisDB:                defaultRedisDB,
		WorkerCount:            defaultWorkerConcurrent,
		PollInterval:           defaultPollInterval,
		LeaseTTL:               defaultLeaseTTL,
		ShutdownTimeout:        defaultShutdownTimeout,
		SchedulerLockTTL:       defaultSchedulerLockTTL,
		SchedulerRenewInterval: defaultSchedulerRenew,
		ServiceName:            getEnv("TASKFORGE_SERVICE_NAME", defaultServiceName),
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
	if cfg.SchedulerLockTTL, err = getEnvDuration("TASKFORGE_SCHEDULER_LOCK_TTL", defaultSchedulerLockTTL); err != nil {
		return Config{}, err
	}
	if cfg.SchedulerRenewInterval, err = getEnvDuration("TASKFORGE_SCHEDULER_RENEW_INTERVAL", defaultSchedulerRenew); err != nil {
		return Config{}, err
	}
	if cfg.SchedulerLockTTL <= 0 {
		return Config{}, fmt.Errorf("TASKFORGE_SCHEDULER_LOCK_TTL must be > 0")
	}
	if cfg.SchedulerRenewInterval <= 0 {
		return Config{}, fmt.Errorf("TASKFORGE_SCHEDULER_RENEW_INTERVAL must be > 0")
	}
	if cfg.SchedulerRenewInterval >= cfg.SchedulerLockTTL {
		return Config{}, fmt.Errorf("TASKFORGE_SCHEDULER_RENEW_INTERVAL must be less than TASKFORGE_SCHEDULER_LOCK_TTL")
	}
	if cfg.RecurringSchedules, err = getRecurringSchedules("TASKFORGE_SCHEDULES_JSON"); err != nil {
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

func getRecurringSchedules(key string) ([]schedulerpkg.ScheduleDefinition, error) {
	value := getEnv(key, "")
	if value == "" {
		return nil, nil
	}

	type rawSchedule struct {
		ID            string            `json:"id"`
		Interval      string            `json:"interval"`
		Queue         string            `json:"queue"`
		TaskName      string            `json:"task_name"`
		Payload       json.RawMessage   `json:"payload"`
		Headers       map[string]string `json:"headers"`
		Enabled       *bool             `json:"enabled"`
		MisfirePolicy string            `json:"misfire_policy"`
		StartAt       string            `json:"start_at"`
	}

	var rawSchedules []rawSchedule
	if err := json.Unmarshal([]byte(value), &rawSchedules); err != nil {
		return nil, fmt.Errorf("%s: parse schedules json: %w", key, err)
	}

	schedules := make([]schedulerpkg.ScheduleDefinition, 0, len(rawSchedules))
	seenIDs := make(map[string]struct{}, len(rawSchedules))
	for _, raw := range rawSchedules {
		if strings.TrimSpace(raw.ID) == "" {
			return nil, fmt.Errorf("%s: schedule id is required", key)
		}
		if _, exists := seenIDs[raw.ID]; exists {
			return nil, fmt.Errorf("%s: duplicate schedule id %q", key, raw.ID)
		}
		seenIDs[raw.ID] = struct{}{}

		interval, err := time.ParseDuration(raw.Interval)
		if err != nil {
			return nil, fmt.Errorf("%s: schedule %q interval: %w", key, raw.ID, err)
		}
		if interval <= 0 {
			return nil, fmt.Errorf("%s: schedule %q interval must be > 0", key, raw.ID)
		}
		if strings.TrimSpace(raw.TaskName) == "" {
			return nil, fmt.Errorf("%s: schedule %q task_name is required", key, raw.ID)
		}
		if len(raw.Payload) == 0 {
			return nil, fmt.Errorf("%s: schedule %q payload is required", key, raw.ID)
		}

		enabled := true
		if raw.Enabled != nil {
			enabled = *raw.Enabled
		}

		misfirePolicy := schedulerpkg.MisfirePolicyCoalesce
		if strings.TrimSpace(raw.MisfirePolicy) != "" {
			misfirePolicy = schedulerpkg.MisfirePolicy(raw.MisfirePolicy)
		}
		if misfirePolicy != schedulerpkg.MisfirePolicyCoalesce {
			return nil, fmt.Errorf("%s: schedule %q misfire_policy %q is not supported", key, raw.ID, raw.MisfirePolicy)
		}

		var startAt *time.Time
		if strings.TrimSpace(raw.StartAt) != "" {
			parsed, err := time.Parse(time.RFC3339, raw.StartAt)
			if err != nil {
				return nil, fmt.Errorf("%s: schedule %q start_at: %w", key, raw.ID, err)
			}
			startAt = &parsed
		}

		queue := raw.Queue
		if strings.TrimSpace(queue) == "" {
			queue = "default"
		}

		schedules = append(schedules, schedulerpkg.ScheduleDefinition{
			ID:            raw.ID,
			Interval:      interval,
			Queue:         queue,
			TaskName:      raw.TaskName,
			Payload:       append(json.RawMessage(nil), raw.Payload...),
			Headers:       raw.Headers,
			Enabled:       enabled,
			MisfirePolicy: misfirePolicy,
			StartAt:       startAt,
		})
	}

	return schedules, nil
}
