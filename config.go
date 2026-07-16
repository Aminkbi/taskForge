package taskforge

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	defaultLeaseTTL               = 30 * time.Second
	defaultTaskTimeout            = 30 * time.Second
	defaultSchedulerPollInterval  = time.Second
	defaultSchedulerLockTTL       = 15 * time.Second
	defaultSchedulerRenewInterval = 5 * time.Second
	defaultSuccessRetention       = 24 * time.Hour
	defaultFailureRetention       = 7 * 24 * time.Hour
	defaultPayloadRetention       = 24 * time.Hour
)

// Config is the supported product configuration shared by embedded Go
// applications and TaskForge's environment-configured services. Normalize
// applies safe defaults and validates the complete model before it is used.
type Config struct {
	LeaseTTL          time.Duration
	WorkerPools       []WorkerPoolConfig
	DependencyBudgets []DependencyBudget
	TaskBudgets       []TaskBudget
	TaskTypeLimits    []TaskTypeLimit
	Retention         *RetentionPolicy
	Scheduler         SchedulerConfig
}

type WorkerPoolConfig struct {
	Name           string
	Queue          string
	Concurrency    int
	Prefetch       int
	TaskTimeout    time.Duration
	Retry          RetryPolicy
	TaskTypeLimits []TaskTypeLimit
	Fairness       *FairnessConfig
	Admission      AdmissionPolicy
	Adaptive       AdaptiveConcurrencyConfig
}

type TaskTypeLimit struct {
	TaskName       string
	MaxConcurrency int
}

type FairnessConfig struct {
	Default FairnessRule
	Rules   []FairnessRule
}

type FairnessRule struct {
	Name                string
	Keys                []string
	Weight              int
	ReservedConcurrency int
	HardQuota           int
}

type AdmissionMode string

const (
	AdmissionDisabled AdmissionMode = "disabled"
	AdmissionDefer    AdmissionMode = "defer"
	AdmissionReject   AdmissionMode = "reject"
)

type AdmissionPolicy struct {
	Mode                     AdmissionMode
	MaxPending               int64
	MaxPendingPerFairnessKey int64
	MaxOldestReadyAge        time.Duration
	MaxRetryBacklog          int64
	DeferInterval            time.Duration
}

type AdaptiveConcurrencyConfig struct {
	Enabled            bool
	MinConcurrency     int
	MaxConcurrency     int
	ControlPeriod      time.Duration
	LatencyThreshold   time.Duration
	ErrorRateThreshold float64
	BacklogThreshold   int64
}

type DependencyBudget struct {
	Name     string
	Capacity int
}

type TaskBudget struct {
	TaskName string
	Budget   string
	Tokens   int
}

type SchedulerConfig struct {
	PollInterval  time.Duration
	LockTTL       time.Duration
	RenewInterval time.Duration
	Schedules     []Schedule
}

type MisfirePolicy string

const MisfireCoalesce MisfirePolicy = "coalesce"

type Schedule struct {
	ID            string
	Interval      time.Duration
	Queue         string
	FairnessKey   string
	TaskName      string
	Payload       json.RawMessage
	Headers       map[string]string
	Enabled       bool
	MisfirePolicy MisfirePolicy
	StartAt       *time.Time
}

// DefaultConfig returns bounded defaults suitable for an embedded worker.
func DefaultConfig() Config {
	return Config{
		LeaseTTL: defaultLeaseTTL,
		WorkerPools: []WorkerPoolConfig{{
			Name:        "default",
			Queue:       "default",
			Concurrency: 4,
			Prefetch:    4,
			TaskTimeout: defaultTaskTimeout,
			Retry:       DefaultRetryPolicy(3),
		}},
		Retention: &RetentionPolicy{
			SucceededState: defaultSuccessRetention,
			FailedState:    defaultFailureRetention,
			ResultPayload:  defaultPayloadRetention,
		},
		Scheduler: SchedulerConfig{
			PollInterval:  defaultSchedulerPollInterval,
			LockTTL:       defaultSchedulerLockTTL,
			RenewInterval: defaultSchedulerRenewInterval,
		},
	}
}

// Normalize returns an owned, validated copy. A nil WorkerPools slice selects
// the default pool; an explicitly empty slice configures no embedded workers.
func (c Config) Normalize() (Config, error) {
	defaults := DefaultConfig()
	if c.LeaseTTL == 0 {
		c.LeaseTTL = defaults.LeaseTTL
	}
	if c.LeaseTTL < 0 {
		return Config{}, fmt.Errorf("lease_ttl must be > 0")
	}
	if c.WorkerPools == nil {
		c.WorkerPools = defaults.WorkerPools
	} else {
		c.WorkerPools = slices.Clone(c.WorkerPools)
	}
	if c.Retention == nil {
		retention := *defaults.Retention
		c.Retention = &retention
	} else {
		retention := *c.Retention
		c.Retention = &retention
	}
	if err := normalizeRetention(c.Retention); err != nil {
		return Config{}, err
	}

	if c.Scheduler.PollInterval == 0 {
		c.Scheduler.PollInterval = defaults.Scheduler.PollInterval
	}
	if c.Scheduler.LockTTL == 0 {
		c.Scheduler.LockTTL = defaults.Scheduler.LockTTL
	}
	if c.Scheduler.RenewInterval == 0 {
		c.Scheduler.RenewInterval = defaults.Scheduler.RenewInterval
	}
	if c.Scheduler.PollInterval < 0 {
		return Config{}, fmt.Errorf("scheduler.poll_interval must be > 0")
	}
	if c.Scheduler.LockTTL < 0 {
		return Config{}, fmt.Errorf("scheduler.lock_ttl must be > 0")
	}
	if c.Scheduler.RenewInterval < 0 {
		return Config{}, fmt.Errorf("scheduler.renew_interval must be > 0")
	}
	if c.Scheduler.RenewInterval >= c.Scheduler.LockTTL {
		return Config{}, fmt.Errorf("scheduler.renew_interval must be less than scheduler.lock_ttl")
	}

	seenPoolNames := make(map[string]struct{}, len(c.WorkerPools))
	seenQueues := make(map[string]struct{}, len(c.WorkerPools))
	for i := range c.WorkerPools {
		pool, err := normalizeWorkerPool(c.WorkerPools[i])
		if err != nil {
			return Config{}, fmt.Errorf("worker_pools[%d]: %w", i, err)
		}
		if _, exists := seenPoolNames[pool.Name]; exists {
			return Config{}, fmt.Errorf("worker_pools[%d]: duplicate name %q", i, pool.Name)
		}
		if _, exists := seenQueues[pool.Queue]; exists {
			return Config{}, fmt.Errorf("worker_pools[%d]: duplicate queue %q", i, pool.Queue)
		}
		seenPoolNames[pool.Name] = struct{}{}
		seenQueues[pool.Queue] = struct{}{}
		c.WorkerPools[i] = pool
	}

	var err error
	if c.TaskTypeLimits, err = normalizeTaskTypeLimits("task_type_limits", c.TaskTypeLimits); err != nil {
		return Config{}, err
	}
	if c.DependencyBudgets, err = normalizeDependencyBudgets(c.DependencyBudgets); err != nil {
		return Config{}, err
	}
	if c.TaskBudgets, err = normalizeTaskBudgets(c.TaskBudgets, c.DependencyBudgets); err != nil {
		return Config{}, err
	}
	if c.Scheduler.Schedules, err = normalizeSchedules(c.Scheduler.Schedules); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate checks the complete configuration without retaining the normalized
// copy. Call Normalize when the result will be used to build a runtime.
func (c Config) Validate() error {
	_, err := c.Normalize()
	return err
}

func normalizeWorkerPool(pool WorkerPoolConfig) (WorkerPoolConfig, error) {
	pool.Name = strings.TrimSpace(pool.Name)
	if pool.Name == "" {
		return WorkerPoolConfig{}, fmt.Errorf("name is required")
	}
	pool.Queue = strings.TrimSpace(pool.Queue)
	if pool.Queue == "" {
		pool.Queue = "default"
	}
	if pool.Concurrency == 0 {
		pool.Concurrency = 4
	}
	if pool.Concurrency < 1 {
		return WorkerPoolConfig{}, fmt.Errorf("concurrency must be >= 1")
	}
	if pool.Prefetch == 0 {
		pool.Prefetch = pool.Concurrency
	}
	if pool.Prefetch < pool.Concurrency {
		return WorkerPoolConfig{}, fmt.Errorf("prefetch must be >= concurrency")
	}
	if pool.TaskTimeout == 0 {
		pool.TaskTimeout = defaultTaskTimeout
	}
	if pool.TaskTimeout < 0 {
		return WorkerPoolConfig{}, fmt.Errorf("task_timeout must be > 0")
	}

	retry, err := normalizeRetryPolicy(pool.Retry)
	if err != nil {
		return WorkerPoolConfig{}, fmt.Errorf("retry: %w", err)
	}
	pool.Retry = retry
	if pool.TaskTypeLimits, err = normalizeTaskTypeLimits("task_type_limits", pool.TaskTypeLimits); err != nil {
		return WorkerPoolConfig{}, err
	}
	if pool.Fairness != nil {
		fairness, err := normalizeFairness(*pool.Fairness, pool.Concurrency, pool.Adaptive.MaxConcurrency)
		if err != nil {
			return WorkerPoolConfig{}, fmt.Errorf("fairness: %w", err)
		}
		pool.Fairness = &fairness
	}
	if pool.Admission, err = normalizeAdmission(pool.Admission, pool.Fairness != nil); err != nil {
		return WorkerPoolConfig{}, fmt.Errorf("admission: %w", err)
	}
	if pool.Adaptive, err = normalizeAdaptive(pool.Adaptive, pool.Concurrency, pool.Prefetch); err != nil {
		return WorkerPoolConfig{}, fmt.Errorf("adaptive: %w", err)
	}
	return pool, nil
}

func normalizeRetryPolicy(policy RetryPolicy) (RetryPolicy, error) {
	if policy == (RetryPolicy{}) {
		return DefaultRetryPolicy(3), nil
	}
	if policy.MaxDeliveries == 0 {
		policy.MaxDeliveries = 3
	}
	if policy.InitialBackoff == 0 {
		policy.InitialBackoff = time.Second
	}
	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = 30 * time.Second
	}
	if policy.Multiplier == 0 {
		policy.Multiplier = 2
	}
	switch {
	case policy.MaxDeliveries < 1:
		return RetryPolicy{}, fmt.Errorf("max_deliveries must be >= 1")
	case policy.InitialBackoff < 0:
		return RetryPolicy{}, fmt.Errorf("initial_backoff must be > 0")
	case policy.MaxBackoff < policy.InitialBackoff:
		return RetryPolicy{}, fmt.Errorf("max_backoff must be >= initial_backoff")
	case policy.Multiplier < 1:
		return RetryPolicy{}, fmt.Errorf("multiplier must be >= 1")
	case policy.Jitter < 0 || policy.Jitter > 1:
		return RetryPolicy{}, fmt.Errorf("jitter must be between 0 and 1")
	case policy.MaxTaskAge < 0:
		return RetryPolicy{}, fmt.Errorf("max_task_age must be >= 0")
	}
	return policy, nil
}

func normalizeFairness(config FairnessConfig, concurrency, adaptiveMax int) (FairnessConfig, error) {
	limit := concurrency
	if adaptiveMax > limit {
		limit = adaptiveMax
	}
	defaultRule, err := normalizeFairnessRule(config.Default, true)
	if err != nil {
		return FairnessConfig{}, fmt.Errorf("default: %w", err)
	}
	config.Default = defaultRule
	config.Rules = slices.Clone(config.Rules)
	seenNames := map[string]struct{}{defaultRule.Name: {}}
	seenKeys := make(map[string]struct{})
	reserved := defaultRule.ReservedConcurrency
	for i := range config.Rules {
		rule, err := normalizeFairnessRule(config.Rules[i], false)
		if err != nil {
			return FairnessConfig{}, fmt.Errorf("rules[%d]: %w", i, err)
		}
		if _, exists := seenNames[rule.Name]; exists {
			return FairnessConfig{}, fmt.Errorf("rules[%d]: duplicate name %q", i, rule.Name)
		}
		seenNames[rule.Name] = struct{}{}
		for _, key := range rule.Keys {
			if _, exists := seenKeys[key]; exists {
				return FairnessConfig{}, fmt.Errorf("rules[%d]: duplicate fairness key %q", i, key)
			}
			seenKeys[key] = struct{}{}
		}
		reserved += rule.ReservedConcurrency
		config.Rules[i] = rule
	}
	if reserved > limit {
		return FairnessConfig{}, fmt.Errorf("reserved_concurrency total %d exceeds maximum pool concurrency %d", reserved, limit)
	}
	return config, nil
}

func normalizeFairnessRule(rule FairnessRule, isDefault bool) (FairnessRule, error) {
	rule.Name = strings.TrimSpace(rule.Name)
	if isDefault && rule.Name == "" {
		rule.Name = "default"
	}
	if rule.Name == "" {
		return FairnessRule{}, fmt.Errorf("name is required")
	}
	if rule.Weight == 0 {
		rule.Weight = 1
	}
	if rule.Weight < 0 || rule.ReservedConcurrency < 0 || rule.HardQuota < 0 {
		return FairnessRule{}, fmt.Errorf("weight, reserved_concurrency, and hard_quota must be >= 0")
	}
	if rule.HardQuota > 0 && rule.ReservedConcurrency > rule.HardQuota {
		return FairnessRule{}, fmt.Errorf("reserved_concurrency must be <= hard_quota")
	}
	if isDefault {
		if len(rule.Keys) != 0 {
			return FairnessRule{}, fmt.Errorf("keys are not allowed on the default rule")
		}
		return rule, nil
	}
	if len(rule.Keys) == 0 {
		return FairnessRule{}, fmt.Errorf("keys are required")
	}
	rule.Keys = slices.Clone(rule.Keys)
	for i, key := range rule.Keys {
		rule.Keys[i] = strings.TrimSpace(key)
		if rule.Keys[i] == "" {
			return FairnessRule{}, fmt.Errorf("keys must be non-empty")
		}
	}
	return rule, nil
}

func normalizeAdmission(policy AdmissionPolicy, hasFairness bool) (AdmissionPolicy, error) {
	if policy.Mode == "" {
		policy.Mode = AdmissionDisabled
	}
	switch policy.Mode {
	case AdmissionDisabled:
		if policy.MaxPending != 0 || policy.MaxPendingPerFairnessKey != 0 || policy.MaxOldestReadyAge != 0 || policy.MaxRetryBacklog != 0 || policy.DeferInterval != 0 {
			return AdmissionPolicy{}, fmt.Errorf("mode is disabled but thresholds are configured")
		}
		return policy, nil
	case AdmissionDefer, AdmissionReject:
	default:
		return AdmissionPolicy{}, fmt.Errorf("mode must be one of disabled, defer, reject")
	}
	if policy.MaxPending < 0 || policy.MaxPendingPerFairnessKey < 0 || policy.MaxOldestReadyAge < 0 || policy.MaxRetryBacklog < 0 {
		return AdmissionPolicy{}, fmt.Errorf("thresholds must be >= 0")
	}
	if policy.MaxPending == 0 && policy.MaxPendingPerFairnessKey == 0 && policy.MaxOldestReadyAge == 0 && policy.MaxRetryBacklog == 0 {
		return AdmissionPolicy{}, fmt.Errorf("enabled policy must set at least one threshold")
	}
	if policy.MaxPendingPerFairnessKey > 0 && !hasFairness {
		return AdmissionPolicy{}, fmt.Errorf("max_pending_per_fairness_key requires fairness")
	}
	if policy.DeferInterval == 0 {
		policy.DeferInterval = 5 * time.Second
	}
	if policy.DeferInterval < 0 {
		return AdmissionPolicy{}, fmt.Errorf("defer_interval must be > 0")
	}
	return policy, nil
}

func normalizeAdaptive(config AdaptiveConcurrencyConfig, concurrency, prefetch int) (AdaptiveConcurrencyConfig, error) {
	if !config.Enabled {
		if config.MinConcurrency != 0 || config.MaxConcurrency != 0 || config.ControlPeriod != 0 || config.LatencyThreshold != 0 || config.ErrorRateThreshold != 0 || config.BacklogThreshold != 0 {
			return AdaptiveConcurrencyConfig{}, fmt.Errorf("enabled must be true when adaptive settings are configured")
		}
		return config, nil
	}
	if config.MinConcurrency == 0 {
		config.MinConcurrency = 1
	}
	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = concurrency
	}
	if config.ControlPeriod == 0 {
		config.ControlPeriod = 5 * time.Second
	}
	if config.LatencyThreshold == 0 {
		config.LatencyThreshold = 500 * time.Millisecond
	}
	if config.ErrorRateThreshold == 0 {
		config.ErrorRateThreshold = 0.2
	}
	if config.BacklogThreshold == 0 {
		config.BacklogThreshold = 1
	}
	switch {
	case config.MinConcurrency < 1:
		return AdaptiveConcurrencyConfig{}, fmt.Errorf("min_concurrency must be >= 1")
	case config.MaxConcurrency < config.MinConcurrency:
		return AdaptiveConcurrencyConfig{}, fmt.Errorf("max_concurrency must be >= min_concurrency")
	case concurrency < config.MinConcurrency || concurrency > config.MaxConcurrency:
		return AdaptiveConcurrencyConfig{}, fmt.Errorf("concurrency must be between min_concurrency and max_concurrency")
	case prefetch < config.MaxConcurrency:
		return AdaptiveConcurrencyConfig{}, fmt.Errorf("prefetch must be >= max_concurrency")
	case config.ControlPeriod < 0:
		return AdaptiveConcurrencyConfig{}, fmt.Errorf("control_period must be > 0")
	case config.LatencyThreshold < 0:
		return AdaptiveConcurrencyConfig{}, fmt.Errorf("latency_threshold must be > 0")
	case config.ErrorRateThreshold < 0 || config.ErrorRateThreshold > 1:
		return AdaptiveConcurrencyConfig{}, fmt.Errorf("error_rate_threshold must be between 0 and 1")
	case config.BacklogThreshold < 0:
		return AdaptiveConcurrencyConfig{}, fmt.Errorf("backlog_threshold must be >= 0")
	}
	return config, nil
}

func normalizeTaskTypeLimits(path string, limits []TaskTypeLimit) ([]TaskTypeLimit, error) {
	if len(limits) == 0 {
		return nil, nil
	}
	limits = slices.Clone(limits)
	seen := make(map[string]struct{}, len(limits))
	for i := range limits {
		limits[i].TaskName = strings.TrimSpace(limits[i].TaskName)
		if limits[i].TaskName == "" {
			return nil, fmt.Errorf("%s[%d]: task_name is required", path, i)
		}
		if limits[i].MaxConcurrency < 1 {
			return nil, fmt.Errorf("%s[%d]: max_concurrency must be >= 1", path, i)
		}
		if _, exists := seen[limits[i].TaskName]; exists {
			return nil, fmt.Errorf("%s[%d]: duplicate task_name %q", path, i, limits[i].TaskName)
		}
		seen[limits[i].TaskName] = struct{}{}
	}
	return limits, nil
}

func normalizeDependencyBudgets(budgets []DependencyBudget) ([]DependencyBudget, error) {
	if len(budgets) == 0 {
		return nil, nil
	}
	budgets = slices.Clone(budgets)
	seen := make(map[string]struct{}, len(budgets))
	for i := range budgets {
		budgets[i].Name = strings.TrimSpace(budgets[i].Name)
		if budgets[i].Name == "" {
			return nil, fmt.Errorf("dependency_budgets[%d]: name is required", i)
		}
		if budgets[i].Capacity < 1 {
			return nil, fmt.Errorf("dependency_budgets[%d]: capacity must be >= 1", i)
		}
		if _, exists := seen[budgets[i].Name]; exists {
			return nil, fmt.Errorf("dependency_budgets[%d]: duplicate name %q", i, budgets[i].Name)
		}
		seen[budgets[i].Name] = struct{}{}
	}
	return budgets, nil
}

func normalizeTaskBudgets(mappings []TaskBudget, budgets []DependencyBudget) ([]TaskBudget, error) {
	if len(mappings) == 0 {
		return nil, nil
	}
	mappings = slices.Clone(mappings)
	capacities := make(map[string]int, len(budgets))
	for _, budget := range budgets {
		capacities[budget.Name] = budget.Capacity
	}
	seen := make(map[string]struct{}, len(mappings))
	for i := range mappings {
		mappings[i].TaskName = strings.TrimSpace(mappings[i].TaskName)
		mappings[i].Budget = strings.TrimSpace(mappings[i].Budget)
		if mappings[i].TaskName == "" {
			return nil, fmt.Errorf("task_budgets[%d]: task_name is required", i)
		}
		if _, exists := seen[mappings[i].TaskName]; exists {
			return nil, fmt.Errorf("task_budgets[%d]: duplicate task_name %q", i, mappings[i].TaskName)
		}
		capacity, exists := capacities[mappings[i].Budget]
		if !exists {
			return nil, fmt.Errorf("task_budgets[%d]: unknown dependency budget %q", i, mappings[i].Budget)
		}
		if mappings[i].Tokens == 0 {
			mappings[i].Tokens = 1
		}
		if mappings[i].Tokens < 1 || mappings[i].Tokens > capacity {
			return nil, fmt.Errorf("task_budgets[%d]: tokens must be between 1 and budget capacity %d", i, capacity)
		}
		seen[mappings[i].TaskName] = struct{}{}
	}
	return mappings, nil
}

func normalizeRetention(retention *RetentionPolicy) error {
	if retention.SucceededState < 0 {
		return fmt.Errorf("retention.succeeded_state must be >= 0")
	}
	if retention.FailedState < 0 {
		return fmt.Errorf("retention.failed_state must be >= 0")
	}
	if retention.ResultPayload < 0 {
		return fmt.Errorf("retention.result_payload must be >= 0")
	}
	return nil
}

func normalizeSchedules(schedules []Schedule) ([]Schedule, error) {
	if len(schedules) == 0 {
		return nil, nil
	}
	schedules = slices.Clone(schedules)
	seen := make(map[string]struct{}, len(schedules))
	for i := range schedules {
		schedule := &schedules[i]
		schedule.ID = strings.TrimSpace(schedule.ID)
		schedule.Queue = strings.TrimSpace(schedule.Queue)
		schedule.FairnessKey = strings.TrimSpace(schedule.FairnessKey)
		schedule.TaskName = strings.TrimSpace(schedule.TaskName)
		if schedule.ID == "" {
			return nil, fmt.Errorf("scheduler.schedules[%d]: id is required", i)
		}
		if _, exists := seen[schedule.ID]; exists {
			return nil, fmt.Errorf("scheduler.schedules[%d]: duplicate id %q", i, schedule.ID)
		}
		if schedule.Interval <= 0 {
			return nil, fmt.Errorf("scheduler.schedules[%d]: interval must be > 0", i)
		}
		if schedule.Queue == "" {
			schedule.Queue = "default"
		}
		if schedule.TaskName == "" {
			return nil, fmt.Errorf("scheduler.schedules[%d]: task_name is required", i)
		}
		if len(schedule.Payload) == 0 || !json.Valid(schedule.Payload) {
			return nil, fmt.Errorf("scheduler.schedules[%d]: payload must be valid JSON", i)
		}
		if schedule.MisfirePolicy == "" {
			schedule.MisfirePolicy = MisfireCoalesce
		}
		if schedule.MisfirePolicy != MisfireCoalesce {
			return nil, fmt.Errorf("scheduler.schedules[%d]: misfire_policy %q is not supported", i, schedule.MisfirePolicy)
		}
		schedule.Payload = slices.Clone(schedule.Payload)
		if len(schedule.Headers) > 0 {
			headers := make(map[string]string, len(schedule.Headers))
			for key, value := range schedule.Headers {
				key = strings.TrimSpace(key)
				if key == "" {
					return nil, fmt.Errorf("scheduler.schedules[%d]: header names must be non-empty", i)
				}
				headers[key] = value
			}
			schedule.Headers = headers
		}
		if schedule.StartAt != nil {
			start := schedule.StartAt.UTC()
			schedule.StartAt = &start
		}
		seen[schedule.ID] = struct{}{}
	}
	return schedules, nil
}
