package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/httpserver"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/redis"
	"github.com/aminkbi/taskforge/worker"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultLogLevel         = "info"
	defaultHTTPAddr         = "127.0.0.1:8080"
	defaultRedisAddr        = "localhost:6379"
	defaultRedisDB          = 0
	defaultWorkerConcurrent = 4
	defaultWorkerPrefetch   = 4
	defaultPollInterval     = time.Second
	defaultLeaseTTL         = 30 * time.Second
	defaultTaskTimeout      = 30 * time.Second
	defaultShutdownTimeout  = 10 * time.Second
	defaultSchedulerLockTTL = 15 * time.Second
	defaultSchedulerRenew   = 5 * time.Second
	defaultTaskSuccessTTL   = 24 * time.Hour
	defaultTaskFailureTTL   = 7 * 24 * time.Hour
	defaultTaskPayloadTTL   = 24 * time.Hour
)

type ServiceRole string

const (
	ServiceRoleWorker    ServiceRole = "worker"
	ServiceRoleScheduler ServiceRole = "scheduler"
	ServiceRoleAPI       ServiceRole = "api"
)

// Config contains service plumbing plus the canonical, validated product
// configuration in Control. Promoted legacy fields are compiled from Control
// for the existing application wiring; they are never validated separately.
type Config struct {
	LogLevel              string
	HTTPAddr              string
	HTTPAuthToken         string
	HTTPReadHeaderTimeout time.Duration
	HTTPReadTimeout       time.Duration
	HTTPWriteTimeout      time.Duration
	HTTPIdleTimeout       time.Duration
	HTTPMaxBodyBytes      int64
	HTTPMaxHeaderBytes    int
	RedisAddr             string
	RedisPassword         string
	RedisDB               int
	RoutingPolicy         *redis.RoutingPolicy
	Control               taskforge.Config

	WorkerPools            []WorkerPoolConfig
	DependencyBudgets      map[string]DependencyBudgetConfig
	TaskBudgets            map[string]TaskBudgetConfig
	TaskTypeLimits         map[string]int
	PollInterval           time.Duration
	ShutdownTimeout        time.Duration
	SchedulerLockTTL       time.Duration
	SchedulerRenewInterval time.Duration
	TaskSuccessRetention   time.Duration
	TaskFailureRetention   time.Duration
	TaskPayloadRetention   time.Duration
	RecurringSchedules     []schedulerpkg.ScheduleDefinition
	OTELEnabled            bool
	ServiceName            string
}

type WorkerPoolConfig struct {
	Name           string
	Queue          string
	Concurrency    int
	Prefetch       int
	LeaseTTL       time.Duration
	TaskTimeout    time.Duration
	RetryPolicy    taskforge.RetryPolicy
	TaskTypeLimits map[string]int
	FairnessPolicy *redis.FairnessPolicy
	Admission      redis.AdmissionPolicy
	Adaptive       worker.AdaptiveConfig
}

type DependencyBudgetConfig struct {
	Name     string
	Capacity int
}

type TaskBudgetConfig struct {
	TaskName string
	Budget   string
	Tokens   int
}

type rawWorkerPool struct {
	Name        string         `json:"name"`
	Queue       string         `json:"queue"`
	Concurrency int            `json:"concurrency"`
	Prefetch    int            `json:"prefetch"`
	TaskTimeout string         `json:"task_timeout"`
	Retry       rawRetryPolicy `json:"retry"`
	TaskLimits  []rawTaskLimit `json:"task_limits"`
	Fairness    *rawFairness   `json:"fairness"`
	Admission   *rawAdmission  `json:"admission"`
	Adaptive    *rawAdaptive   `json:"adaptive"`
}

type rawRetryPolicy struct {
	MaxDeliveries  int     `json:"max_deliveries"`
	InitialBackoff string  `json:"initial_backoff"`
	MaxBackoff     string  `json:"max_backoff"`
	Multiplier     float64 `json:"multiplier"`
	Jitter         float64 `json:"jitter"`
	MaxTaskAge     string  `json:"max_task_age"`
}

type rawTaskLimit struct {
	TaskName       string `json:"task_name"`
	MaxConcurrency int    `json:"max_concurrency"`
}

type rawFairness struct {
	Default rawFairnessRule   `json:"default"`
	Rules   []rawFairnessRule `json:"rules"`
}

type rawFairnessRule struct {
	Name                string   `json:"name"`
	Keys                []string `json:"keys"`
	Weight              int      `json:"weight"`
	ReservedConcurrency int      `json:"reserved_concurrency"`
	HardQuota           int      `json:"hard_quota"`
}

type rawAdmission struct {
	Mode                     taskforge.AdmissionMode `json:"mode"`
	MaxPending               int64                   `json:"max_pending"`
	MaxPendingPerFairnessKey int64                   `json:"max_pending_per_fairness_key"`
	MaxOldestReadyAge        string                  `json:"max_oldest_ready_age"`
	MaxRetryBacklog          int64                   `json:"max_retry_backlog"`
	DeferInterval            string                  `json:"defer_interval"`
}

type rawAdaptive struct {
	Enabled            bool    `json:"enabled"`
	MinConcurrency     int     `json:"min_concurrency"`
	MaxConcurrency     int     `json:"max_concurrency"`
	ControlPeriod      string  `json:"control_period"`
	LatencyThreshold   string  `json:"latency_threshold"`
	ErrorRateThreshold float64 `json:"error_rate_threshold"`
	BacklogThreshold   int64   `json:"backlog_threshold"`
}

type rawDependencyBudget struct {
	Name     string `json:"name"`
	Capacity int    `json:"capacity"`
}

type rawTaskBudget struct {
	TaskName string `json:"task_name"`
	Budget   string `json:"budget"`
	Tokens   int    `json:"tokens"`
}

type rawSchedule struct {
	ID            string                  `json:"id"`
	Interval      string                  `json:"interval"`
	Queue         string                  `json:"queue"`
	FairnessKey   string                  `json:"fairness_key"`
	TaskName      string                  `json:"task_name"`
	Payload       json.RawMessage         `json:"payload"`
	Headers       map[string]string       `json:"headers"`
	Enabled       *bool                   `json:"enabled"`
	MisfirePolicy taskforge.MisfirePolicy `json:"misfire_policy"`
	StartAt       string                  `json:"start_at"`
}

func Load(defaultServiceName string) (Config, error) {
	return LoadForRole(defaultServiceName, ServiceRoleWorker)
}

func LoadForRole(defaultServiceName string, role ServiceRole) (Config, error) {
	cfg := Config{
		LogLevel:              getEnv("TASKFORGE_LOG_LEVEL", defaultLogLevel),
		HTTPAddr:              getEnv("TASKFORGE_HTTP_ADDR", defaultHTTPAddr),
		HTTPAuthToken:         getEnv("TASKFORGE_HTTP_AUTH_TOKEN", ""),
		HTTPReadHeaderTimeout: httpserver.DefaultReadHeaderTimeout,
		HTTPReadTimeout:       httpserver.DefaultReadTimeout,
		HTTPWriteTimeout:      httpserver.DefaultWriteTimeout,
		HTTPIdleTimeout:       httpserver.DefaultIdleTimeout,
		HTTPMaxBodyBytes:      httpserver.DefaultMaxBodyBytes,
		HTTPMaxHeaderBytes:    httpserver.DefaultMaxHeaderBytes,
		RedisAddr:             getEnv("TASKFORGE_REDIS_ADDR", defaultRedisAddr),
		RedisPassword:         getEnv("TASKFORGE_REDIS_PASSWORD", ""),
		ShutdownTimeout:       defaultShutdownTimeout,
		ServiceName:           getEnv("TASKFORGE_SERVICE_NAME", defaultServiceName),
	}
	var err error
	if err := validateHTTPAuthToken(cfg.HTTPAuthToken); err != nil {
		return Config{}, err
	}
	if cfg.HTTPReadHeaderTimeout, err = getEnvPositiveDuration("TASKFORGE_HTTP_READ_HEADER_TIMEOUT", httpserver.DefaultReadHeaderTimeout); err != nil {
		return Config{}, err
	}
	if cfg.HTTPReadTimeout, err = getEnvPositiveDuration("TASKFORGE_HTTP_READ_TIMEOUT", httpserver.DefaultReadTimeout); err != nil {
		return Config{}, err
	}
	if cfg.HTTPWriteTimeout, err = getEnvPositiveDuration("TASKFORGE_HTTP_WRITE_TIMEOUT", httpserver.DefaultWriteTimeout); err != nil {
		return Config{}, err
	}
	if cfg.HTTPIdleTimeout, err = getEnvPositiveDuration("TASKFORGE_HTTP_IDLE_TIMEOUT", httpserver.DefaultIdleTimeout); err != nil {
		return Config{}, err
	}
	if cfg.HTTPMaxBodyBytes, err = getEnvPositiveInt64("TASKFORGE_HTTP_MAX_BODY_BYTES", httpserver.DefaultMaxBodyBytes); err != nil {
		return Config{}, err
	}
	if cfg.HTTPMaxHeaderBytes, err = getEnvPositiveInt("TASKFORGE_HTTP_MAX_HEADER_BYTES", httpserver.DefaultMaxHeaderBytes); err != nil {
		return Config{}, err
	}
	if cfg.RedisDB, err = getEnvInt("TASKFORGE_REDIS_DB", defaultRedisDB); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = getEnvPositiveDuration("TASKFORGE_SHUTDOWN_TIMEOUT", defaultShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.OTELEnabled, err = getEnvBool("TASKFORGE_OTEL_ENABLED", false); err != nil {
		return Config{}, err
	}
	if cfg.RoutingPolicy, err = getRoutingPolicy("TASKFORGE_ROUTING_POLICY_JSON"); err != nil {
		return Config{}, err
	}

	control, err := loadControl(role)
	if err != nil {
		return Config{}, err
	}
	cfg.Control = control
	if err := cfg.compileControl(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func loadControl(role ServiceRole) (taskforge.Config, error) {
	control := taskforge.DefaultConfig()
	if role == ServiceRoleScheduler || role == ServiceRoleAPI {
		control.WorkerPools = []taskforge.WorkerPoolConfig{}
	}
	var err error
	if control.LeaseTTL, err = getEnvDuration("TASKFORGE_LEASE_TTL", defaultLeaseTTL); err != nil {
		return taskforge.Config{}, err
	}
	retention := taskforge.RetentionPolicy{}
	if retention.SucceededState, err = getEnvDuration("TASKFORGE_TASK_SUCCESS_RETENTION", defaultTaskSuccessTTL); err != nil {
		return taskforge.Config{}, err
	}
	if retention.FailedState, err = getEnvDuration("TASKFORGE_TASK_FAILURE_RETENTION", defaultTaskFailureTTL); err != nil {
		return taskforge.Config{}, err
	}
	if retention.ResultPayload, err = getEnvDuration("TASKFORGE_TASK_PAYLOAD_RETENTION", defaultTaskPayloadTTL); err != nil {
		return taskforge.Config{}, err
	}
	control.Retention = &retention
	if control.Scheduler.PollInterval, err = getEnvDuration("TASKFORGE_POLL_INTERVAL", defaultPollInterval); err != nil {
		return taskforge.Config{}, err
	}
	if control.Scheduler.LockTTL, err = getEnvDuration("TASKFORGE_SCHEDULER_LOCK_TTL", defaultSchedulerLockTTL); err != nil {
		return taskforge.Config{}, err
	}
	if control.Scheduler.RenewInterval, err = getEnvDuration("TASKFORGE_SCHEDULER_RENEW_INTERVAL", defaultSchedulerRenew); err != nil {
		return taskforge.Config{}, err
	}

	if value := getEnv("TASKFORGE_WORKER_POOLS_JSON", ""); value != "" {
		var raw []rawWorkerPool
		if err := decodeJSON("TASKFORGE_WORKER_POOLS_JSON", value, &raw); err != nil {
			return taskforge.Config{}, err
		}
		control.WorkerPools, err = parseWorkerPools(raw)
		if err != nil {
			return taskforge.Config{}, fmt.Errorf("TASKFORGE_WORKER_POOLS_JSON: %w", err)
		}
		if role == ServiceRoleWorker && len(control.WorkerPools) == 0 {
			return taskforge.Config{}, fmt.Errorf("TASKFORGE_WORKER_POOLS_JSON: at least one worker pool is required")
		}
	}
	if value := getEnv("TASKFORGE_DEPENDENCY_BUDGETS_JSON", ""); value != "" {
		var raw []rawDependencyBudget
		if err := decodeJSON("TASKFORGE_DEPENDENCY_BUDGETS_JSON", value, &raw); err != nil {
			return taskforge.Config{}, err
		}
		control.DependencyBudgets = make([]taskforge.DependencyBudget, len(raw))
		for i, budget := range raw {
			control.DependencyBudgets[i] = taskforge.DependencyBudget{Name: budget.Name, Capacity: budget.Capacity}
		}
	}
	if value := getEnv("TASKFORGE_TASK_BUDGETS_JSON", ""); value != "" {
		var raw []rawTaskBudget
		if err := decodeJSON("TASKFORGE_TASK_BUDGETS_JSON", value, &raw); err != nil {
			return taskforge.Config{}, err
		}
		control.TaskBudgets = make([]taskforge.TaskBudget, len(raw))
		for i, mapping := range raw {
			control.TaskBudgets[i] = taskforge.TaskBudget{TaskName: mapping.TaskName, Budget: mapping.Budget, Tokens: mapping.Tokens}
		}
	}
	if value := getEnv("TASKFORGE_TASK_TYPE_LIMITS_JSON", ""); value != "" {
		var raw []rawTaskLimit
		if err := decodeJSON("TASKFORGE_TASK_TYPE_LIMITS_JSON", value, &raw); err != nil {
			return taskforge.Config{}, err
		}
		control.TaskTypeLimits = taskLimits(raw)
	}
	if value := getEnv("TASKFORGE_SCHEDULES_JSON", ""); value != "" {
		var raw []rawSchedule
		if err := decodeJSON("TASKFORGE_SCHEDULES_JSON", value, &raw); err != nil {
			return taskforge.Config{}, err
		}
		control.Scheduler.Schedules, err = parseSchedules(raw)
		if err != nil {
			return taskforge.Config{}, fmt.Errorf("TASKFORGE_SCHEDULES_JSON: %w", err)
		}
	}

	normalized, err := control.Normalize()
	if err != nil {
		return taskforge.Config{}, fmt.Errorf("TASKFORGE_CONFIGURATION: %w", err)
	}
	return normalized, nil
}

func parseWorkerPools(raw []rawWorkerPool) ([]taskforge.WorkerPoolConfig, error) {
	pools := make([]taskforge.WorkerPoolConfig, len(raw))
	for i, entry := range raw {
		taskTimeout, err := parseOptionalDuration(fmt.Sprintf("worker_pools[%d].task_timeout", i), entry.TaskTimeout)
		if err != nil {
			return nil, err
		}
		retry, err := parseRetry(i, entry.Retry)
		if err != nil {
			return nil, err
		}
		pool := taskforge.WorkerPoolConfig{
			Name:           entry.Name,
			Queue:          entry.Queue,
			Concurrency:    entry.Concurrency,
			Prefetch:       entry.Prefetch,
			TaskTimeout:    taskTimeout,
			Retry:          retry,
			TaskTypeLimits: taskLimits(entry.TaskLimits),
		}
		if entry.Fairness != nil {
			fairness := taskforge.FairnessConfig{Default: fairnessRule(entry.Fairness.Default)}
			fairness.Rules = make([]taskforge.FairnessRule, len(entry.Fairness.Rules))
			for j, rule := range entry.Fairness.Rules {
				fairness.Rules[j] = fairnessRule(rule)
			}
			pool.Fairness = &fairness
		}
		if entry.Admission != nil {
			maxAge, err := parseOptionalDuration(fmt.Sprintf("worker_pools[%d].admission.max_oldest_ready_age", i), entry.Admission.MaxOldestReadyAge)
			if err != nil {
				return nil, err
			}
			deferInterval, err := parseOptionalDuration(fmt.Sprintf("worker_pools[%d].admission.defer_interval", i), entry.Admission.DeferInterval)
			if err != nil {
				return nil, err
			}
			pool.Admission = taskforge.AdmissionPolicy{
				Mode: entry.Admission.Mode, MaxPending: entry.Admission.MaxPending,
				MaxPendingPerFairnessKey: entry.Admission.MaxPendingPerFairnessKey,
				MaxOldestReadyAge:        maxAge, MaxRetryBacklog: entry.Admission.MaxRetryBacklog,
				DeferInterval: deferInterval,
			}
		}
		if entry.Adaptive != nil {
			period, err := parseOptionalDuration(fmt.Sprintf("worker_pools[%d].adaptive.control_period", i), entry.Adaptive.ControlPeriod)
			if err != nil {
				return nil, err
			}
			latency, err := parseOptionalDuration(fmt.Sprintf("worker_pools[%d].adaptive.latency_threshold", i), entry.Adaptive.LatencyThreshold)
			if err != nil {
				return nil, err
			}
			pool.Adaptive = taskforge.AdaptiveConcurrencyConfig{
				Enabled: entry.Adaptive.Enabled, MinConcurrency: entry.Adaptive.MinConcurrency,
				MaxConcurrency: entry.Adaptive.MaxConcurrency, ControlPeriod: period,
				LatencyThreshold: latency, ErrorRateThreshold: entry.Adaptive.ErrorRateThreshold,
				BacklogThreshold: entry.Adaptive.BacklogThreshold,
			}
		}
		pools[i] = pool
	}
	return pools, nil
}

func parseRetry(poolIndex int, raw rawRetryPolicy) (taskforge.RetryPolicy, error) {
	initial, err := parseOptionalDuration(fmt.Sprintf("worker_pools[%d].retry.initial_backoff", poolIndex), raw.InitialBackoff)
	if err != nil {
		return taskforge.RetryPolicy{}, err
	}
	maximum, err := parseOptionalDuration(fmt.Sprintf("worker_pools[%d].retry.max_backoff", poolIndex), raw.MaxBackoff)
	if err != nil {
		return taskforge.RetryPolicy{}, err
	}
	maxAge, err := parseOptionalDuration(fmt.Sprintf("worker_pools[%d].retry.max_task_age", poolIndex), raw.MaxTaskAge)
	if err != nil {
		return taskforge.RetryPolicy{}, err
	}
	return taskforge.RetryPolicy{
		MaxDeliveries: raw.MaxDeliveries, InitialBackoff: initial, MaxBackoff: maximum,
		Multiplier: raw.Multiplier, Jitter: raw.Jitter, MaxTaskAge: maxAge,
	}, nil
}

func parseSchedules(raw []rawSchedule) ([]taskforge.Schedule, error) {
	schedules := make([]taskforge.Schedule, len(raw))
	for i, entry := range raw {
		interval, err := parseOptionalDuration(fmt.Sprintf("schedules[%d].interval", i), entry.Interval)
		if err != nil {
			return nil, err
		}
		enabled := false
		if entry.Enabled != nil {
			enabled = *entry.Enabled
		}
		var startAt *time.Time
		if strings.TrimSpace(entry.StartAt) != "" {
			parsed, err := time.Parse(time.RFC3339, entry.StartAt)
			if err != nil {
				return nil, fmt.Errorf("schedules[%d].start_at: %w", i, err)
			}
			startAt = &parsed
		}
		schedules[i] = taskforge.Schedule{
			ID: entry.ID, Interval: interval, Queue: entry.Queue, FairnessKey: entry.FairnessKey,
			TaskName: entry.TaskName, Payload: entry.Payload, Headers: entry.Headers, Enabled: enabled,
			MisfirePolicy: entry.MisfirePolicy, StartAt: startAt,
		}
	}
	return schedules, nil
}

func (c *Config) compileControl() error {
	brokerOptions, err := redis.OptionsFromConfig(redis.Options{RoutingPolicy: c.RoutingPolicy}, c.Control)
	if err != nil {
		return err
	}
	c.WorkerPools = make([]WorkerPoolConfig, len(c.Control.WorkerPools))
	for i, pool := range c.Control.WorkerPools {
		workerOptions, err := worker.OptionsFromConfig(worker.Options{}, c.Control, pool.Name)
		if err != nil {
			return err
		}
		c.WorkerPools[i] = WorkerPoolConfig{
			Name: pool.Name, Queue: pool.Queue, Concurrency: pool.Concurrency, Prefetch: pool.Prefetch,
			LeaseTTL: c.Control.LeaseTTL, TaskTimeout: pool.TaskTimeout, RetryPolicy: pool.Retry,
			TaskTypeLimits: workerOptions.PoolTaskLimits, FairnessPolicy: brokerOptions.FairnessPolicies[pool.Queue],
			Admission: brokerOptions.AdmissionPolicies[pool.Queue], Adaptive: workerOptions.Adaptive,
		}
	}
	c.DependencyBudgets = make(map[string]DependencyBudgetConfig, len(c.Control.DependencyBudgets))
	for _, budget := range c.Control.DependencyBudgets {
		c.DependencyBudgets[budget.Name] = DependencyBudgetConfig{Name: budget.Name, Capacity: budget.Capacity}
	}
	c.TaskBudgets = make(map[string]TaskBudgetConfig, len(c.Control.TaskBudgets))
	for _, mapping := range c.Control.TaskBudgets {
		c.TaskBudgets[mapping.TaskName] = TaskBudgetConfig{TaskName: mapping.TaskName, Budget: mapping.Budget, Tokens: mapping.Tokens}
	}
	c.TaskTypeLimits = taskLimitMap(c.Control.TaskTypeLimits)
	c.PollInterval = c.Control.Scheduler.PollInterval
	c.SchedulerLockTTL = c.Control.Scheduler.LockTTL
	c.SchedulerRenewInterval = c.Control.Scheduler.RenewInterval
	c.TaskSuccessRetention = c.Control.Retention.SucceededState
	c.TaskFailureRetention = c.Control.Retention.FailedState
	c.TaskPayloadRetention = c.Control.Retention.ResultPayload
	c.RecurringSchedules = c.Control.Scheduler.Schedules
	return nil
}

func (c Config) RedisOptions(client *goredis.Client, logger *slog.Logger) redis.Options {
	options, err := redis.OptionsFromConfig(redis.Options{
		Client: client, Logger: logger, RoutingPolicy: c.RoutingPolicy,
	}, c.Control)
	if err != nil {
		panic(fmt.Sprintf("invalid validated configuration: %v", err))
	}
	return options
}

func (c Config) HTTPServerConfig() httpserver.Config {
	return httpserver.Config{
		Addr:              c.HTTPAddr,
		AuthToken:         c.HTTPAuthToken,
		ReadHeaderTimeout: c.HTTPReadHeaderTimeout,
		ReadTimeout:       c.HTTPReadTimeout,
		WriteTimeout:      c.HTTPWriteTimeout,
		IdleTimeout:       c.HTTPIdleTimeout,
		ShutdownTimeout:   c.ShutdownTimeout,
		MaxBodyBytes:      c.HTTPMaxBodyBytes,
		MaxHeaderBytes:    c.HTTPMaxHeaderBytes,
	}
}

func getRoutingPolicy(key string) (*redis.RoutingPolicy, error) {
	value := getEnv(key, "")
	if value == "" {
		return nil, nil
	}
	policy, err := redis.ParseRoutingPolicyJSON([]byte(value))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return policy, nil
}

func decodeJSON(key, value string, target any) error {
	decoder := json.NewDecoder(bytes.NewBufferString(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%s: parse json: %w", key, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%s: parse json: multiple values", key)
		}
		return fmt.Errorf("%s: parse json: %w", key, err)
	}
	return nil
}

func parseOptionalDuration(path, value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", path, err)
	}
	return parsed, nil
}

func taskLimits(raw []rawTaskLimit) []taskforge.TaskTypeLimit {
	limits := make([]taskforge.TaskTypeLimit, len(raw))
	for i, limit := range raw {
		limits[i] = taskforge.TaskTypeLimit{TaskName: limit.TaskName, MaxConcurrency: limit.MaxConcurrency}
	}
	return limits
}

func fairnessRule(raw rawFairnessRule) taskforge.FairnessRule {
	return taskforge.FairnessRule{
		Name: raw.Name, Keys: raw.Keys, Weight: raw.Weight,
		ReservedConcurrency: raw.ReservedConcurrency, HardQuota: raw.HardQuota,
	}
}

func taskLimitMap(limits []taskforge.TaskTypeLimit) map[string]int {
	if len(limits) == 0 {
		return nil
	}
	result := make(map[string]int, len(limits))
	for _, limit := range limits {
		result[limit.TaskName] = limit.MaxConcurrency
	}
	return result
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

func getEnvPositiveDuration(key string, fallback time.Duration) (time.Duration, error) {
	value, err := getEnvDuration(key, fallback)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: must be greater than zero", key)
	}
	return value, nil
}

func getEnvPositiveInt(key string, fallback int) (int, error) {
	value, err := getEnvInt(key, fallback)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: must be greater than zero", key)
	}
	return value, nil
}

func getEnvPositiveInt64(key string, fallback int64) (int64, error) {
	value := getEnv(key, "")
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: parse int: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s: must be greater than zero", key)
	}
	return parsed, nil
}

func validateHTTPAuthToken(token string) error {
	if token == "" {
		return nil
	}
	if len(token) < 32 {
		return fmt.Errorf("TASKFORGE_HTTP_AUTH_TOKEN: must contain at least 32 characters")
	}
	if strings.IndexFunc(token, unicode.IsSpace) >= 0 {
		return fmt.Errorf("TASKFORGE_HTTP_AUTH_TOKEN: must not contain whitespace")
	}
	return nil
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
