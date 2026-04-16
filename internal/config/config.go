package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aminkbi/taskforge/internal/fairness"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	defaultLogLevel         = "info"
	defaultHTTPAddr         = ":8080"
	defaultMetricsAddr      = ":8080"
	defaultRedisAddr        = "localhost:6379"
	defaultRedisDB          = 0
	defaultWorkerConcurrent = 4
	defaultWorkerPrefetch   = 4
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
	WorkerPools            []WorkerPoolConfig
	TaskTypeLimits         map[string]int
	PollInterval           time.Duration
	ShutdownTimeout        time.Duration
	SchedulerLockTTL       time.Duration
	SchedulerRenewInterval time.Duration
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
	RetryPolicy    tasks.RetryPolicy
	TaskTypeLimits map[string]int
	FairnessPolicy *fairness.Policy
	Admission      AdmissionPolicy
}

type AdmissionMode string

const (
	AdmissionModeDisabled AdmissionMode = "disabled"
	AdmissionModeDefer    AdmissionMode = "defer"
	AdmissionModeReject   AdmissionMode = "reject"
)

type AdmissionPolicy struct {
	Mode                     AdmissionMode
	MaxPending               int64
	MaxPendingPerFairnessKey int64
	MaxOldestReadyAge        time.Duration
	MaxRetryBacklog          int64
	MaxDeadLetterSize        int64
	DeferInterval            time.Duration
}

type rawRetryPolicy struct {
	MaxAttempts    int     `json:"max_attempts"`
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

type rawWorkerPool struct {
	Name        string         `json:"name"`
	Queue       string         `json:"queue"`
	Concurrency int            `json:"concurrency"`
	Prefetch    int            `json:"prefetch"`
	LeaseTTL    string         `json:"lease_ttl"`
	Retry       rawRetryPolicy `json:"retry"`
	TaskLimits  []rawTaskLimit `json:"task_limits"`
	Fairness    *rawFairness   `json:"fairness"`
	Admission   *rawAdmission  `json:"admission"`
}

type rawFairness struct {
	DefaultRule rawFairnessRule   `json:"default_rule"`
	Rules       []rawFairnessRule `json:"rules"`
}

type rawFairnessRule struct {
	Name                string   `json:"name"`
	Keys                []string `json:"keys"`
	Weight              int      `json:"weight"`
	ReservedConcurrency int      `json:"reserved_concurrency"`
	SoftQuota           int      `json:"soft_quota"`
	HardQuota           int      `json:"hard_quota"`
	Burst               int      `json:"burst"`
}

type rawAdmission struct {
	Mode                     string `json:"mode"`
	MaxPending               int64  `json:"max_pending"`
	MaxPendingPerFairnessKey int64  `json:"max_pending_per_fairness_key"`
	MaxOldestReadyAge        string `json:"max_oldest_ready_age"`
	MaxRetryBacklog          int64  `json:"max_retry_backlog"`
	MaxDeadLetterSize        int64  `json:"max_dead_letter_size"`
	DeferInterval            string `json:"defer_interval"`
}

func Load(defaultServiceName string) (Config, error) {
	cfg := Config{
		LogLevel:               getEnv("TASKFORGE_LOG_LEVEL", defaultLogLevel),
		HTTPAddr:               getEnv("TASKFORGE_HTTP_ADDR", defaultHTTPAddr),
		MetricsAddr:            getEnv("TASKFORGE_METRICS_ADDR", defaultMetricsAddr),
		RedisAddr:              getEnv("TASKFORGE_REDIS_ADDR", defaultRedisAddr),
		RedisPassword:          getEnv("TASKFORGE_REDIS_PASSWORD", ""),
		RedisDB:                defaultRedisDB,
		PollInterval:           defaultPollInterval,
		ShutdownTimeout:        defaultShutdownTimeout,
		SchedulerLockTTL:       defaultSchedulerLockTTL,
		SchedulerRenewInterval: defaultSchedulerRenew,
		ServiceName:            getEnv("TASKFORGE_SERVICE_NAME", defaultServiceName),
	}

	var err error
	if cfg.RedisDB, err = getEnvInt("TASKFORGE_REDIS_DB", defaultRedisDB); err != nil {
		return Config{}, err
	}
	if cfg.PollInterval, err = getEnvDuration("TASKFORGE_POLL_INTERVAL", defaultPollInterval); err != nil {
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
	if cfg.WorkerPools, err = getWorkerPools("TASKFORGE_WORKER_POOLS_JSON"); err != nil {
		return Config{}, err
	}
	if cfg.TaskTypeLimits, err = getTaskTypeLimits("TASKFORGE_TASK_TYPE_LIMITS_JSON"); err != nil {
		return Config{}, err
	}
	if cfg.RecurringSchedules, err = getRecurringSchedules("TASKFORGE_SCHEDULES_JSON"); err != nil {
		return Config{}, err
	}
	if cfg.OTELEnabled, err = getEnvBool("TASKFORGE_OTEL_ENABLED", false); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func FairnessPoliciesByQueue(pools []WorkerPoolConfig) map[string]*fairness.Policy {
	policies := make(map[string]*fairness.Policy)
	for _, pool := range pools {
		if pool.FairnessPolicy == nil {
			continue
		}
		policies[normalizeQueue(pool.Queue)] = pool.FairnessPolicy
	}
	if len(policies) == 0 {
		return nil
	}
	return policies
}

func AdmissionPoliciesByQueue(pools []WorkerPoolConfig) map[string]AdmissionPolicy {
	policies := make(map[string]AdmissionPolicy)
	for _, pool := range pools {
		if pool.Admission.Mode == AdmissionModeDisabled {
			continue
		}
		policies[normalizeQueue(pool.Queue)] = pool.Admission
	}
	if len(policies) == 0 {
		return nil
	}
	return policies
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

func getWorkerPools(key string) ([]WorkerPoolConfig, error) {
	value := getEnv(key, "")
	if value == "" {
		return []WorkerPoolConfig{
			{
				Name:        "default",
				Queue:       "default",
				Concurrency: defaultWorkerConcurrent,
				Prefetch:    defaultWorkerPrefetch,
				LeaseTTL:    defaultLeaseTTL,
				RetryPolicy: tasks.DefaultRetryPolicy(3),
			},
		}, nil
	}

	var rawPools []rawWorkerPool
	if err := json.Unmarshal([]byte(value), &rawPools); err != nil {
		return nil, fmt.Errorf("%s: parse worker pools json: %w", key, err)
	}
	if len(rawPools) == 0 {
		return nil, fmt.Errorf("%s: at least one worker pool is required", key)
	}

	pools := make([]WorkerPoolConfig, 0, len(rawPools))
	seenNames := make(map[string]struct{}, len(rawPools))
	seenQueues := make(map[string]struct{}, len(rawPools))
	for _, raw := range rawPools {
		name := strings.TrimSpace(raw.Name)
		if name == "" {
			return nil, fmt.Errorf("%s: worker pool name is required", key)
		}
		if _, exists := seenNames[name]; exists {
			return nil, fmt.Errorf("%s: duplicate worker pool name %q", key, name)
		}
		seenNames[name] = struct{}{}

		queue := normalizeQueue(raw.Queue)
		if _, exists := seenQueues[queue]; exists {
			return nil, fmt.Errorf("%s: duplicate worker pool queue %q", key, queue)
		}
		seenQueues[queue] = struct{}{}

		concurrency := raw.Concurrency
		if concurrency < 1 {
			return nil, fmt.Errorf("%s: worker pool %q concurrency must be >= 1", key, name)
		}

		prefetch := raw.Prefetch
		if prefetch == 0 {
			prefetch = concurrency
		}
		if prefetch < concurrency {
			return nil, fmt.Errorf("%s: worker pool %q prefetch must be >= concurrency", key, name)
		}

		leaseTTL, err := time.ParseDuration(strings.TrimSpace(raw.LeaseTTL))
		if err != nil {
			return nil, fmt.Errorf("%s: worker pool %q lease_ttl: %w", key, name, err)
		}
		if leaseTTL <= 0 {
			return nil, fmt.Errorf("%s: worker pool %q lease_ttl must be > 0", key, name)
		}

		retryPolicy, err := parseRetryPolicy(key, name, raw.Retry)
		if err != nil {
			return nil, err
		}
		taskLimits, err := parseTaskLimitEntries(key, fmt.Sprintf("worker pool %q", name), raw.TaskLimits)
		if err != nil {
			return nil, err
		}
		fairnessPolicy, err := parseFairnessPolicy(key, name, raw.Fairness)
		if err != nil {
			return nil, err
		}
		admissionPolicy, err := parseAdmissionPolicy(key, name, raw.Admission, fairnessPolicy != nil)
		if err != nil {
			return nil, err
		}

		pools = append(pools, WorkerPoolConfig{
			Name:           name,
			Queue:          queue,
			Concurrency:    concurrency,
			Prefetch:       prefetch,
			LeaseTTL:       leaseTTL,
			RetryPolicy:    retryPolicy,
			TaskTypeLimits: taskLimits,
			FairnessPolicy: fairnessPolicy,
			Admission:      admissionPolicy,
		})
	}

	return pools, nil
}

func getTaskTypeLimits(key string) (map[string]int, error) {
	value := getEnv(key, "")
	if value == "" {
		return nil, nil
	}

	var rawLimits []rawTaskLimit
	if err := json.Unmarshal([]byte(value), &rawLimits); err != nil {
		return nil, fmt.Errorf("%s: parse task type limits json: %w", key, err)
	}

	return parseTaskLimitEntries(key, "global task type limits", rawLimits)
}

func parseTaskLimitEntries(key, scope string, rawLimits []rawTaskLimit) (map[string]int, error) {
	if len(rawLimits) == 0 {
		return nil, nil
	}

	limits := make(map[string]int, len(rawLimits))
	for _, raw := range rawLimits {
		taskName := strings.TrimSpace(raw.TaskName)
		if taskName == "" {
			return nil, fmt.Errorf("%s: %s: task_name is required", key, scope)
		}
		if raw.MaxConcurrency < 1 {
			return nil, fmt.Errorf("%s: %s: task %q max_concurrency must be >= 1", key, scope, taskName)
		}
		if _, exists := limits[taskName]; exists {
			return nil, fmt.Errorf("%s: %s: duplicate task_name %q", key, scope, taskName)
		}
		limits[taskName] = raw.MaxConcurrency
	}
	return limits, nil
}

func parseRetryPolicy(key, poolName string, raw rawRetryPolicy) (tasks.RetryPolicy, error) {
	policy := tasks.DefaultRetryPolicy(3)
	if raw.MaxAttempts > 0 {
		policy.MaxAttempts = raw.MaxAttempts
		if raw.MaxDeliveries == 0 {
			policy.MaxDeliveries = raw.MaxAttempts
		}
	}
	if raw.MaxDeliveries > 0 {
		policy.MaxDeliveries = raw.MaxDeliveries
		if raw.MaxAttempts == 0 {
			policy.MaxAttempts = raw.MaxDeliveries
		}
	}

	var err error
	if raw.InitialBackoff != "" {
		policy.InitialBackoff, err = time.ParseDuration(raw.InitialBackoff)
		if err != nil {
			return tasks.RetryPolicy{}, fmt.Errorf("%s: worker pool %q retry.initial_backoff: %w", key, poolName, err)
		}
	}
	if raw.MaxBackoff != "" {
		policy.MaxBackoff, err = time.ParseDuration(raw.MaxBackoff)
		if err != nil {
			return tasks.RetryPolicy{}, fmt.Errorf("%s: worker pool %q retry.max_backoff: %w", key, poolName, err)
		}
	}
	if raw.Multiplier != 0 {
		policy.Multiplier = raw.Multiplier
	}
	if raw.Jitter != 0 {
		policy.Jitter = raw.Jitter
	}
	if raw.MaxTaskAge != "" {
		policy.MaxTaskAge, err = time.ParseDuration(raw.MaxTaskAge)
		if err != nil {
			return tasks.RetryPolicy{}, fmt.Errorf("%s: worker pool %q retry.max_task_age: %w", key, poolName, err)
		}
	}

	return policy, nil
}

func parseFairnessPolicy(key, poolName string, raw *rawFairness) (*fairness.Policy, error) {
	if raw == nil {
		return nil, nil
	}

	defaultRule, err := parseFairnessRule(key, poolName, "default_rule", raw.DefaultRule, true)
	if err != nil {
		return nil, err
	}

	rules := make([]fairness.Rule, 0, len(raw.Rules))
	for i, entry := range raw.Rules {
		rule, err := parseFairnessRule(key, poolName, fmt.Sprintf("rules[%d]", i), entry, false)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}

	policy, err := fairness.NewPolicy(defaultRule, rules)
	if err != nil {
		return nil, fmt.Errorf("%s: worker pool %q fairness: %w", key, poolName, err)
	}
	return policy, nil
}

func parseFairnessRule(key, poolName, scope string, raw rawFairnessRule, isDefault bool) (fairness.Rule, error) {
	rule := fairness.Rule{
		Name:                strings.TrimSpace(raw.Name),
		Weight:              raw.Weight,
		ReservedConcurrency: raw.ReservedConcurrency,
		SoftQuota:           raw.SoftQuota,
		HardQuota:           raw.HardQuota,
		Burst:               raw.Burst,
	}

	if !isDefault {
		if len(raw.Keys) == 0 {
			return fairness.Rule{}, fmt.Errorf("%s: worker pool %q fairness %s keys are required", key, poolName, scope)
		}
		rule.Keys = make([]string, 0, len(raw.Keys))
		for _, fairnessKey := range raw.Keys {
			trimmed := strings.TrimSpace(fairnessKey)
			if trimmed == "" {
				return fairness.Rule{}, fmt.Errorf("%s: worker pool %q fairness %s keys must be non-empty", key, poolName, scope)
			}
			rule.Keys = append(rule.Keys, trimmed)
		}
	}

	return rule, nil
}

func parseAdmissionPolicy(key, poolName string, raw *rawAdmission, hasFairness bool) (AdmissionPolicy, error) {
	policy := AdmissionPolicy{Mode: AdmissionModeDisabled}
	if raw == nil {
		return policy, nil
	}

	mode := AdmissionMode(strings.TrimSpace(raw.Mode))
	switch mode {
	case "", AdmissionModeDisabled:
		policy.Mode = AdmissionModeDisabled
	case AdmissionModeDefer, AdmissionModeReject:
		policy.Mode = mode
	default:
		return AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.mode must be one of disabled, defer, reject", key, poolName)
	}

	if raw.MaxPending < 0 || raw.MaxPendingPerFairnessKey < 0 || raw.MaxRetryBacklog < 0 || raw.MaxDeadLetterSize < 0 {
		return AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission thresholds must be >= 0", key, poolName)
	}

	policy.MaxPending = raw.MaxPending
	policy.MaxPendingPerFairnessKey = raw.MaxPendingPerFairnessKey
	policy.MaxRetryBacklog = raw.MaxRetryBacklog
	policy.MaxDeadLetterSize = raw.MaxDeadLetterSize

	if raw.MaxOldestReadyAge != "" {
		parsed, err := time.ParseDuration(strings.TrimSpace(raw.MaxOldestReadyAge))
		if err != nil {
			return AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.max_oldest_ready_age: %w", key, poolName, err)
		}
		if parsed < 0 {
			return AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.max_oldest_ready_age must be >= 0", key, poolName)
		}
		policy.MaxOldestReadyAge = parsed
	}

	if raw.DeferInterval != "" {
		parsed, err := time.ParseDuration(strings.TrimSpace(raw.DeferInterval))
		if err != nil {
			return AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.defer_interval: %w", key, poolName, err)
		}
		policy.DeferInterval = parsed
	}

	if policy.Mode != AdmissionModeDisabled && policy.DeferInterval <= 0 {
		return AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.defer_interval must be > 0 when admission is enabled", key, poolName)
	}
	if policy.MaxPendingPerFairnessKey > 0 && !hasFairness {
		return AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.max_pending_per_fairness_key requires fairness", key, poolName)
	}

	return policy, nil
}

func normalizeQueue(queue string) string {
	if strings.TrimSpace(queue) == "" {
		return "default"
	}
	return strings.TrimSpace(queue)
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
		FairnessKey   string            `json:"fairness_key"`
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
			FairnessKey:   strings.TrimSpace(raw.FairnessKey),
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
