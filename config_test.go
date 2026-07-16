package taskforge

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestConfigNormalizeAppliesSafeDefaults(t *testing.T) {
	t.Parallel()

	config, err := (Config{}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if config.LeaseTTL != 30*time.Second || len(config.WorkerPools) != 1 {
		t.Fatalf("unexpected runtime defaults: %+v", config)
	}
	pool := config.WorkerPools[0]
	if pool.Name != "default" || pool.Queue != "default" || pool.Concurrency != 4 || pool.Prefetch != 4 {
		t.Fatalf("unexpected pool defaults: %+v", pool)
	}
	if pool.TaskTimeout != 30*time.Second || pool.Retry.MaxDeliveries != 3 {
		t.Fatalf("unexpected delivery defaults: %+v", pool)
	}
	if config.Retention == nil || config.Retention.SucceededState != 24*time.Hour || config.Retention.FailedState != 7*24*time.Hour {
		t.Fatalf("unexpected retention defaults: %+v", config.Retention)
	}
	if config.Scheduler.PollInterval != time.Second || config.Scheduler.RenewInterval >= config.Scheduler.LockTTL {
		t.Fatalf("unexpected scheduler defaults: %+v", config.Scheduler)
	}
}

func TestConfigValidateRejectsInvalidCombinations(t *testing.T) {
	t.Parallel()

	validPool := WorkerPoolConfig{Name: "default", Concurrency: 2, Prefetch: 2}
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:    "negative lease",
			config:  Config{LeaseTTL: -time.Second},
			wantErr: "lease_ttl must be > 0",
		},
		{
			name: "duplicate queue",
			config: Config{WorkerPools: []WorkerPoolConfig{
				validPool, {Name: "other", Queue: "default", Concurrency: 1},
			}},
			wantErr: `duplicate queue "default"`,
		},
		{
			name: "retry backoff inversion",
			config: Config{WorkerPools: []WorkerPoolConfig{{
				Name: "default", Concurrency: 1,
				Retry: RetryPolicy{InitialBackoff: 2 * time.Second, MaxBackoff: time.Second},
			}}},
			wantErr: "max_backoff must be >= initial_backoff",
		},
		{
			name: "fairness guarantee exceeds pool",
			config: Config{WorkerPools: []WorkerPoolConfig{{
				Name: "default", Concurrency: 2,
				Fairness: &FairnessConfig{Rules: []FairnessRule{
					{Name: "one", Keys: []string{"tenant-1"}, ReservedConcurrency: 2},
					{Name: "two", Keys: []string{"tenant-2"}, ReservedConcurrency: 1},
				}},
			}}},
			wantErr: "reserved_concurrency total 3 exceeds maximum pool concurrency 2",
		},
		{
			name: "tenant admission without fairness",
			config: Config{WorkerPools: []WorkerPoolConfig{{
				Name: "default", Concurrency: 1,
				Admission: AdmissionPolicy{Mode: AdmissionDefer, MaxPendingPerFairnessKey: 1},
			}}},
			wantErr: "max_pending_per_fairness_key requires fairness",
		},
		{
			name: "disabled admission with thresholds",
			config: Config{WorkerPools: []WorkerPoolConfig{{
				Name: "default", Concurrency: 1,
				Admission: AdmissionPolicy{MaxPending: 1},
			}}},
			wantErr: "mode is disabled but thresholds are configured",
		},
		{
			name: "adaptive maximum exceeds prefetch",
			config: Config{WorkerPools: []WorkerPoolConfig{{
				Name: "default", Concurrency: 2, Prefetch: 2,
				Adaptive: AdaptiveConcurrencyConfig{Enabled: true, MinConcurrency: 1, MaxConcurrency: 3},
			}}},
			wantErr: "prefetch must be >= max_concurrency",
		},
		{
			name: "adaptive error rate outside probability",
			config: Config{WorkerPools: []WorkerPoolConfig{{
				Name: "default", Concurrency: 1,
				Adaptive: AdaptiveConcurrencyConfig{Enabled: true, ErrorRateThreshold: 1.1},
			}}},
			wantErr: "error_rate_threshold must be between 0 and 1",
		},
		{
			name:    "unknown dependency budget",
			config:  Config{TaskBudgets: []TaskBudget{{TaskName: "report", Budget: "missing"}}},
			wantErr: `unknown dependency budget "missing"`,
		},
		{
			name: "task requires more than budget capacity",
			config: Config{
				DependencyBudgets: []DependencyBudget{{Name: "api", Capacity: 1}},
				TaskBudgets:       []TaskBudget{{TaskName: "report", Budget: "api", Tokens: 2}},
			},
			wantErr: "tokens must be between 1 and budget capacity 1",
		},
		{
			name:    "negative retention",
			config:  Config{Retention: &RetentionPolicy{FailedState: -time.Second}},
			wantErr: "retention.failed_state must be >= 0",
		},
		{
			name: "unsafe scheduler renewal",
			config: Config{Scheduler: SchedulerConfig{
				LockTTL: 5 * time.Second, RenewInterval: 5 * time.Second,
			}},
			wantErr: "scheduler.renew_interval must be less than scheduler.lock_ttl",
		},
		{
			name: "duplicate schedule",
			config: Config{Scheduler: SchedulerConfig{Schedules: []Schedule{
				{ID: "report", Interval: time.Minute, TaskName: "report", Payload: json.RawMessage(`{}`)},
				{ID: "report", Interval: time.Minute, TaskName: "report", Payload: json.RawMessage(`{}`)},
			}}},
			wantErr: `duplicate id "report"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.config.Validate()
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestConfigNormalizeOwnsMutableInputs(t *testing.T) {
	t.Parallel()

	headers := map[string]string{"source": "test"}
	payload := json.RawMessage(`{"kind":"daily"}`)
	config, err := (Config{
		WorkerPools: []WorkerPoolConfig{},
		Scheduler: SchedulerConfig{Schedules: []Schedule{{
			ID: "daily", Interval: time.Hour, TaskName: "report", Payload: payload, Headers: headers,
		}}},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	headers["source"] = "changed"
	payload[0] = '['
	if config.Scheduler.Schedules[0].Headers["source"] != "test" || string(config.Scheduler.Schedules[0].Payload) != `{"kind":"daily"}` {
		t.Fatalf("normalized schedule retained caller-owned data: %+v", config.Scheduler.Schedules[0])
	}
}
