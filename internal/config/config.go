package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aminkbi/taskforge"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/redis"
	"github.com/aminkbi/taskforge/worker"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultLogLevel         = "info"
	defaultHTTPAddr         = ":8080"
	defaultRedisAddr        = "localhost:6379"
	defaultRedisDB          = 0
	defaultWorkerConcurrent = 4
	defaultWorkerPrefetch   = 4
	defaultPollInterval     = time.Second
	defaultLeaseTTL         = 30 * time.Second
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

type Config struct {
	LogLevel               string
	HTTPAddr               string
	RedisAddr              string
	RedisPassword          string
	RedisDB                int
	WorkerPools            []WorkerPoolConfig
	RoutingPolicy          *redis.RoutingPolicy
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
	Adaptive    *rawAdaptive   `json:"adaptive"`
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

type rawAdaptive struct {
	Enabled                bool    `json:"enabled"`
	MinConcurrency         int     `json:"min_concurrency"`
	MaxConcurrency         int     `json:"max_concurrency"`
	ControlPeriod          string  `json:"control_period"`
	Cooldown               string  `json:"cooldown"`
	ScaleUpStep            int     `json:"scale_up_step"`
	ScaleDownStep          int     `json:"scale_down_step"`
	LatencyThreshold       string  `json:"latency_threshold"`
	ErrorRateThreshold     float64 `json:"error_rate_threshold"`
	BacklogThreshold       int64   `json:"backlog_threshold"`
	HealthyWindowsRequired int     `json:"healthy_windows_required"`
}

func Load(defaultServiceName string) (Config, error) {
	return LoadForRole(defaultServiceName, ServiceRoleWorker)
}

func LoadForRole(defaultServiceName string, role ServiceRole) (Config, error) {
	cfg := Config{
		LogLevel:               getEnv("TASKFORGE_LOG_LEVEL", defaultLogLevel),
		HTTPAddr:               getEnv("TASKFORGE_HTTP_ADDR", defaultHTTPAddr),
		RedisAddr:              getEnv("TASKFORGE_REDIS_ADDR", defaultRedisAddr),
		RedisPassword:          getEnv("TASKFORGE_REDIS_PASSWORD", ""),
		RedisDB:                defaultRedisDB,
		PollInterval:           defaultPollInterval,
		ShutdownTimeout:        defaultShutdownTimeout,
		SchedulerLockTTL:       defaultSchedulerLockTTL,
		SchedulerRenewInterval: defaultSchedulerRenew,
		TaskSuccessRetention:   defaultTaskSuccessTTL,
		TaskFailureRetention:   defaultTaskFailureTTL,
		TaskPayloadRetention:   defaultTaskPayloadTTL,
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
	if cfg.TaskSuccessRetention, err = getEnvDuration("TASKFORGE_TASK_SUCCESS_RETENTION", defaultTaskSuccessTTL); err != nil {
		return Config{}, err
	}
	if cfg.TaskFailureRetention, err = getEnvDuration("TASKFORGE_TASK_FAILURE_RETENTION", defaultTaskFailureTTL); err != nil {
		return Config{}, err
	}
	if cfg.TaskPayloadRetention, err = getEnvDuration("TASKFORGE_TASK_PAYLOAD_RETENTION", defaultTaskPayloadTTL); err != nil {
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
	if cfg.TaskSuccessRetention < 0 {
		return Config{}, fmt.Errorf("TASKFORGE_TASK_SUCCESS_RETENTION must be >= 0")
	}
	if cfg.TaskFailureRetention < 0 {
		return Config{}, fmt.Errorf("TASKFORGE_TASK_FAILURE_RETENTION must be >= 0")
	}
	if cfg.TaskPayloadRetention < 0 {
		return Config{}, fmt.Errorf("TASKFORGE_TASK_PAYLOAD_RETENTION must be >= 0")
	}
	if cfg.WorkerPools, err = getWorkerPools("TASKFORGE_WORKER_POOLS_JSON", role); err != nil {
		return Config{}, err
	}
	if cfg.RoutingPolicy, err = getRoutingPolicy("TASKFORGE_ROUTING_POLICY_JSON"); err != nil {
		return Config{}, err
	}
	if cfg.DependencyBudgets, err = getDependencyBudgets("TASKFORGE_DEPENDENCY_BUDGETS_JSON"); err != nil {
		return Config{}, err
	}
	if cfg.TaskBudgets, err = getTaskBudgets("TASKFORGE_TASK_BUDGETS_JSON", cfg.DependencyBudgets); err != nil {
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

func (c Config) RedisOptions(client *goredis.Client, logger *slog.Logger) redis.Options {
	leaseTTL := defaultLeaseTTL
	if len(c.WorkerPools) > 0 {
		leaseTTL = c.WorkerPools[0].LeaseTTL
	}
	capacities := make(map[string]int, len(c.DependencyBudgets))
	for name, budget := range c.DependencyBudgets {
		capacities[name] = budget.Capacity
	}
	if len(capacities) == 0 {
		capacities = nil
	}
	return redis.Options{
		Client:            client,
		Logger:            logger,
		LeaseTTL:          leaseTTL,
		FairnessPolicies:  fairnessPoliciesByQueue(c.WorkerPools),
		AdmissionPolicies: admissionPoliciesByQueue(c.WorkerPools),
		RoutingPolicy:     c.RoutingPolicy,
		DependencyBudgets: capacities,
		Retention: taskforge.RetentionPolicy{
			SucceededState: c.TaskSuccessRetention,
			FailedState:    c.TaskFailureRetention,
			ResultPayload:  c.TaskPayloadRetention,
		},
	}
}

func fairnessPoliciesByQueue(pools []WorkerPoolConfig) map[string]*redis.FairnessPolicy {
	policies := make(map[string]*redis.FairnessPolicy)
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

func admissionPoliciesByQueue(pools []WorkerPoolConfig) map[string]redis.AdmissionPolicy {
	policies := make(map[string]redis.AdmissionPolicy)
	for _, pool := range pools {
		if pool.Admission.Mode == redis.AdmissionModeDisabled {
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

func getWorkerPools(key string, role ServiceRole) ([]WorkerPoolConfig, error) {
	value := getEnv(key, "")
	if value == "" {
		if role == ServiceRoleScheduler || role == ServiceRoleAPI {
			return nil, nil
		}
		return []WorkerPoolConfig{
			{
				Name:        "default",
				Queue:       "default",
				Concurrency: defaultWorkerConcurrent,
				Prefetch:    defaultWorkerPrefetch,
				LeaseTTL:    defaultLeaseTTL,
				RetryPolicy: taskforge.DefaultRetryPolicy(3),
			},
		}, nil
	}

	var rawPools []rawWorkerPool
	if err := json.Unmarshal([]byte(value), &rawPools); err != nil {
		return nil, fmt.Errorf("%s: parse worker pools json: %w", key, err)
	}
	if len(rawPools) == 0 {
		if role == ServiceRoleScheduler || role == ServiceRoleAPI {
			return nil, nil
		}
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
		adaptivePolicy, err := parseAdaptivePolicy(key, name, concurrency, prefetch, raw.Adaptive)
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
			Adaptive:       adaptivePolicy,
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

func getDependencyBudgets(key string) (map[string]DependencyBudgetConfig, error) {
	value := getEnv(key, "")
	if value == "" {
		return nil, nil
	}

	var rawBudgets []rawDependencyBudget
	if err := json.Unmarshal([]byte(value), &rawBudgets); err != nil {
		return nil, fmt.Errorf("%s: parse dependency budgets json: %w", key, err)
	}
	if len(rawBudgets) == 0 {
		return nil, nil
	}

	budgets := make(map[string]DependencyBudgetConfig, len(rawBudgets))
	for _, raw := range rawBudgets {
		name := strings.TrimSpace(raw.Name)
		if name == "" {
			return nil, fmt.Errorf("%s: budget name is required", key)
		}
		if raw.Capacity < 1 {
			return nil, fmt.Errorf("%s: budget %q capacity must be >= 1", key, name)
		}
		if _, exists := budgets[name]; exists {
			return nil, fmt.Errorf("%s: duplicate budget %q", key, name)
		}
		budgets[name] = DependencyBudgetConfig{
			Name:     name,
			Capacity: raw.Capacity,
		}
	}

	return budgets, nil
}

func getTaskBudgets(key string, budgets map[string]DependencyBudgetConfig) (map[string]TaskBudgetConfig, error) {
	value := getEnv(key, "")
	if value == "" {
		return nil, nil
	}

	var rawMappings []rawTaskBudget
	if err := json.Unmarshal([]byte(value), &rawMappings); err != nil {
		return nil, fmt.Errorf("%s: parse task budgets json: %w", key, err)
	}
	if len(rawMappings) == 0 {
		return nil, nil
	}

	mappings := make(map[string]TaskBudgetConfig, len(rawMappings))
	for _, raw := range rawMappings {
		taskName := strings.TrimSpace(raw.TaskName)
		if taskName == "" {
			return nil, fmt.Errorf("%s: task_name is required", key)
		}
		if _, exists := mappings[taskName]; exists {
			return nil, fmt.Errorf("%s: duplicate task budget mapping for %q", key, taskName)
		}
		budget := strings.TrimSpace(raw.Budget)
		if budget == "" {
			return nil, fmt.Errorf("%s: task %q budget is required", key, taskName)
		}
		if _, exists := budgets[budget]; !exists {
			return nil, fmt.Errorf("%s: task %q references unknown budget %q", key, taskName, budget)
		}
		tokens := raw.Tokens
		if tokens == 0 {
			tokens = 1
		}
		if tokens < 1 {
			return nil, fmt.Errorf("%s: task %q tokens must be >= 1", key, taskName)
		}
		mappings[taskName] = TaskBudgetConfig{
			TaskName: taskName,
			Budget:   budget,
			Tokens:   tokens,
		}
	}

	return mappings, nil
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

func parseRetryPolicy(key, poolName string, raw rawRetryPolicy) (taskforge.RetryPolicy, error) {
	policy := taskforge.DefaultRetryPolicy(3)
	if raw.MaxDeliveries > 0 {
		policy.MaxDeliveries = raw.MaxDeliveries
	}

	var err error
	if raw.InitialBackoff != "" {
		policy.InitialBackoff, err = time.ParseDuration(raw.InitialBackoff)
		if err != nil {
			return taskforge.RetryPolicy{}, fmt.Errorf("%s: worker pool %q retry.initial_backoff: %w", key, poolName, err)
		}
	}
	if raw.MaxBackoff != "" {
		policy.MaxBackoff, err = time.ParseDuration(raw.MaxBackoff)
		if err != nil {
			return taskforge.RetryPolicy{}, fmt.Errorf("%s: worker pool %q retry.max_backoff: %w", key, poolName, err)
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
			return taskforge.RetryPolicy{}, fmt.Errorf("%s: worker pool %q retry.max_task_age: %w", key, poolName, err)
		}
	}

	return policy, nil
}

func parseFairnessPolicy(key, poolName string, raw *rawFairness) (*redis.FairnessPolicy, error) {
	if raw == nil {
		return nil, nil
	}

	defaultRule, err := parseFairnessRule(key, poolName, "default_rule", raw.DefaultRule, true)
	if err != nil {
		return nil, err
	}

	rules := make([]redis.FairnessRule, 0, len(raw.Rules))
	for i, entry := range raw.Rules {
		rule, err := parseFairnessRule(key, poolName, fmt.Sprintf("rules[%d]", i), entry, false)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}

	policy, err := redis.NewFairnessPolicy(defaultRule, rules)
	if err != nil {
		return nil, fmt.Errorf("%s: worker pool %q fairness: %w", key, poolName, err)
	}
	return policy, nil
}

func parseFairnessRule(key, poolName, scope string, raw rawFairnessRule, isDefault bool) (redis.FairnessRule, error) {
	rule := redis.FairnessRule{
		Name:                strings.TrimSpace(raw.Name),
		Weight:              raw.Weight,
		ReservedConcurrency: raw.ReservedConcurrency,
		SoftQuota:           raw.SoftQuota,
		HardQuota:           raw.HardQuota,
		Burst:               raw.Burst,
	}

	if !isDefault {
		if len(raw.Keys) == 0 {
			return redis.FairnessRule{}, fmt.Errorf("%s: worker pool %q fairness %s keys are required", key, poolName, scope)
		}
		rule.Keys = make([]string, 0, len(raw.Keys))
		for _, fairnessKey := range raw.Keys {
			trimmed := strings.TrimSpace(fairnessKey)
			if trimmed == "" {
				return redis.FairnessRule{}, fmt.Errorf("%s: worker pool %q fairness %s keys must be non-empty", key, poolName, scope)
			}
			rule.Keys = append(rule.Keys, trimmed)
		}
	}

	return rule, nil
}

func parseAdmissionPolicy(key, poolName string, raw *rawAdmission, hasFairness bool) (redis.AdmissionPolicy, error) {
	policy := redis.AdmissionPolicy{Mode: redis.AdmissionModeDisabled}
	if raw == nil {
		return policy, nil
	}

	mode := redis.AdmissionMode(strings.TrimSpace(raw.Mode))
	switch mode {
	case "", redis.AdmissionModeDisabled:
		policy.Mode = redis.AdmissionModeDisabled
	case redis.AdmissionModeDefer, redis.AdmissionModeReject:
		policy.Mode = mode
	default:
		return redis.AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.mode must be one of disabled, defer, reject", key, poolName)
	}

	if raw.MaxPending < 0 || raw.MaxPendingPerFairnessKey < 0 || raw.MaxRetryBacklog < 0 || raw.MaxDeadLetterSize < 0 {
		return redis.AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission thresholds must be >= 0", key, poolName)
	}

	policy.MaxPending = raw.MaxPending
	policy.MaxPendingPerFairnessKey = raw.MaxPendingPerFairnessKey
	policy.MaxRetryBacklog = raw.MaxRetryBacklog
	policy.MaxDeadLetterSize = raw.MaxDeadLetterSize

	if raw.MaxOldestReadyAge != "" {
		parsed, err := time.ParseDuration(strings.TrimSpace(raw.MaxOldestReadyAge))
		if err != nil {
			return redis.AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.max_oldest_ready_age: %w", key, poolName, err)
		}
		if parsed < 0 {
			return redis.AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.max_oldest_ready_age must be >= 0", key, poolName)
		}
		policy.MaxOldestReadyAge = parsed
	}

	if raw.DeferInterval != "" {
		parsed, err := time.ParseDuration(strings.TrimSpace(raw.DeferInterval))
		if err != nil {
			return redis.AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.defer_interval: %w", key, poolName, err)
		}
		policy.DeferInterval = parsed
	}

	if policy.Mode != redis.AdmissionModeDisabled && policy.DeferInterval <= 0 {
		return redis.AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.defer_interval must be > 0 when admission is enabled", key, poolName)
	}
	if policy.MaxPendingPerFairnessKey > 0 && !hasFairness {
		return redis.AdmissionPolicy{}, fmt.Errorf("%s: worker pool %q admission.max_pending_per_fairness_key requires fairness", key, poolName)
	}

	return policy, nil
}

func parseAdaptivePolicy(key, poolName string, concurrency, prefetch int, raw *rawAdaptive) (worker.AdaptiveConfig, error) {
	if raw == nil || !raw.Enabled {
		return worker.AdaptiveConfig{}, nil
	}

	controlPeriod, err := time.ParseDuration(strings.TrimSpace(raw.ControlPeriod))
	if err != nil {
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.control_period: %w", key, poolName, err)
	}
	cooldown, err := time.ParseDuration(strings.TrimSpace(raw.Cooldown))
	if err != nil {
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.cooldown: %w", key, poolName, err)
	}
	latencyThreshold, err := time.ParseDuration(strings.TrimSpace(raw.LatencyThreshold))
	if err != nil {
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.latency_threshold: %w", key, poolName, err)
	}

	adaptive := worker.AdaptiveConfig{
		Enabled:                true,
		MinConcurrency:         raw.MinConcurrency,
		MaxConcurrency:         raw.MaxConcurrency,
		ControlPeriod:          controlPeriod,
		Cooldown:               cooldown,
		ScaleUpStep:            raw.ScaleUpStep,
		ScaleDownStep:          raw.ScaleDownStep,
		LatencyThreshold:       latencyThreshold,
		ErrorRateThreshold:     raw.ErrorRateThreshold,
		BacklogThreshold:       raw.BacklogThreshold,
		HealthyWindowsRequired: raw.HealthyWindowsRequired,
	}

	switch {
	case adaptive.MinConcurrency < 1:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.min_concurrency must be >= 1", key, poolName)
	case adaptive.MaxConcurrency < adaptive.MinConcurrency:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.max_concurrency must be >= min_concurrency", key, poolName)
	case concurrency < adaptive.MinConcurrency || concurrency > adaptive.MaxConcurrency:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q concurrency must be between adaptive min_concurrency and max_concurrency", key, poolName)
	case prefetch < adaptive.MaxConcurrency:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q prefetch must be >= adaptive.max_concurrency", key, poolName)
	case adaptive.ControlPeriod <= 0:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.control_period must be > 0", key, poolName)
	case adaptive.Cooldown < 0:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.cooldown must be >= 0", key, poolName)
	case adaptive.ScaleUpStep < 1:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.scale_up_step must be >= 1", key, poolName)
	case adaptive.ScaleDownStep < 1:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.scale_down_step must be >= 1", key, poolName)
	case adaptive.LatencyThreshold <= 0:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.latency_threshold must be > 0", key, poolName)
	case adaptive.ErrorRateThreshold < 0:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.error_rate_threshold must be >= 0", key, poolName)
	case adaptive.BacklogThreshold < 0:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.backlog_threshold must be >= 0", key, poolName)
	case adaptive.HealthyWindowsRequired < 1:
		return worker.AdaptiveConfig{}, fmt.Errorf("%s: worker pool %q adaptive.healthy_windows_required must be >= 1", key, poolName)
	}

	return adaptive, nil
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
