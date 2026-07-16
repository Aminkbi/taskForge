package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("TASKFORGE_LOG_LEVEL", "")
	t.Setenv("TASKFORGE_HTTP_ADDR", "")
	t.Setenv("TASKFORGE_HTTP_AUTH_TOKEN", "")
	t.Setenv("TASKFORGE_HTTP_READ_HEADER_TIMEOUT", "")
	t.Setenv("TASKFORGE_HTTP_READ_TIMEOUT", "")
	t.Setenv("TASKFORGE_HTTP_WRITE_TIMEOUT", "")
	t.Setenv("TASKFORGE_HTTP_IDLE_TIMEOUT", "")
	t.Setenv("TASKFORGE_HTTP_MAX_BODY_BYTES", "")
	t.Setenv("TASKFORGE_HTTP_MAX_HEADER_BYTES", "")
	t.Setenv("TASKFORGE_REDIS_ADDR", "")
	t.Setenv("TASKFORGE_REDIS_PASSWORD", "")
	t.Setenv("TASKFORGE_REDIS_DB", "")
	t.Setenv("TASKFORGE_REDIS_CONNECT_TIMEOUT", "")
	t.Setenv("TASKFORGE_REDIS_TLS_ENABLED", "")
	t.Setenv("TASKFORGE_REDIS_TLS_CA_FILE", "")
	t.Setenv("TASKFORGE_REDIS_TLS_CERT_FILE", "")
	t.Setenv("TASKFORGE_REDIS_TLS_KEY_FILE", "")
	t.Setenv("TASKFORGE_REDIS_TLS_SERVER_NAME", "")
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", "")
	t.Setenv("TASKFORGE_LEASE_TTL", "")
	t.Setenv("TASKFORGE_ROUTING_POLICY_JSON", "")
	t.Setenv("TASKFORGE_DEPENDENCY_BUDGETS_JSON", "")
	t.Setenv("TASKFORGE_TASK_BUDGETS_JSON", "")
	t.Setenv("TASKFORGE_TASK_TYPE_LIMITS_JSON", "")
	t.Setenv("TASKFORGE_POLL_INTERVAL", "")
	t.Setenv("TASKFORGE_SHUTDOWN_TIMEOUT", "")
	t.Setenv("TASKFORGE_SCHEDULER_LOCK_TTL", "")
	t.Setenv("TASKFORGE_SCHEDULER_RENEW_INTERVAL", "")
	t.Setenv("TASKFORGE_TASK_SUCCESS_RETENTION", "")
	t.Setenv("TASKFORGE_TASK_FAILURE_RETENTION", "")
	t.Setenv("TASKFORGE_TASK_PAYLOAD_RETENTION", "")
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
	if cfg.HTTPAddr != "127.0.0.1:8080" {
		t.Fatalf("HTTPAddr = %q, want loopback default", cfg.HTTPAddr)
	}
	if cfg.HTTPAuthToken != "" {
		t.Fatalf("HTTPAuthToken = %q, want disabled", cfg.HTTPAuthToken)
	}
	if cfg.HTTPReadHeaderTimeout != 5*time.Second || cfg.HTTPReadTimeout != 15*time.Second || cfg.HTTPWriteTimeout != 30*time.Second || cfg.HTTPIdleTimeout != 60*time.Second {
		t.Fatalf("unexpected HTTP timeout defaults: %+v", cfg)
	}
	if cfg.HTTPMaxBodyBytes != 1<<20 || cfg.HTTPMaxHeaderBytes != 16<<10 {
		t.Fatalf("unexpected HTTP size defaults: body=%d header=%d", cfg.HTTPMaxBodyBytes, cfg.HTTPMaxHeaderBytes)
	}
	if len(cfg.WorkerPools) != 1 {
		t.Fatalf("WorkerPools length = %d, want 1", len(cfg.WorkerPools))
	}
	pool := cfg.WorkerPools[0]
	if pool.Name != "default" || pool.Queue != "default" {
		t.Fatalf("unexpected default pool identity: %+v", pool)
	}
	if pool.Concurrency != defaultWorkerConcurrent || pool.Prefetch != defaultWorkerPrefetch {
		t.Fatalf("unexpected default pool sizing: %+v", pool)
	}
	if pool.LeaseTTL != defaultLeaseTTL {
		t.Fatalf("default pool lease ttl = %v, want %v", pool.LeaseTTL, defaultLeaseTTL)
	}
	if pool.TaskTimeout != defaultTaskTimeout {
		t.Fatalf("default pool task timeout = %v, want %v", pool.TaskTimeout, defaultTaskTimeout)
	}
	if len(cfg.TaskTypeLimits) != 0 {
		t.Fatalf("TaskTypeLimits length = %d, want 0", len(cfg.TaskTypeLimits))
	}
	if cfg.RoutingPolicy != nil {
		t.Fatalf("RoutingPolicy = %+v, want nil", cfg.RoutingPolicy)
	}
	if cfg.RedisTLS.Enabled || cfg.RedisConnectTimeout != defaultRedisConnectTimeout {
		t.Fatalf("unexpected default Redis connectivity: %+v timeout=%v", cfg.RedisTLS, cfg.RedisConnectTimeout)
	}
	if len(cfg.DependencyBudgets) != 0 {
		t.Fatalf("DependencyBudgets length = %d, want 0", len(cfg.DependencyBudgets))
	}
	if len(cfg.TaskBudgets) != 0 {
		t.Fatalf("TaskBudgets length = %d, want 0", len(cfg.TaskBudgets))
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
	if cfg.TaskSuccessRetention != defaultTaskSuccessTTL || cfg.TaskFailureRetention != defaultTaskFailureTTL || cfg.TaskPayloadRetention != defaultTaskPayloadTTL {
		t.Fatalf("unexpected task retention defaults: %+v", cfg)
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
	t.Setenv("TASKFORGE_HTTP_AUTH_TOKEN", "0123456789abcdef0123456789abcdef")
	t.Setenv("TASKFORGE_HTTP_READ_HEADER_TIMEOUT", "2s")
	t.Setenv("TASKFORGE_HTTP_READ_TIMEOUT", "7s")
	t.Setenv("TASKFORGE_HTTP_WRITE_TIMEOUT", "11s")
	t.Setenv("TASKFORGE_HTTP_IDLE_TIMEOUT", "45s")
	t.Setenv("TASKFORGE_HTTP_MAX_BODY_BYTES", "2048")
	t.Setenv("TASKFORGE_HTTP_MAX_HEADER_BYTES", "8192")
	t.Setenv("TASKFORGE_REDIS_ADDR", "redis.internal:6379")
	t.Setenv("TASKFORGE_REDIS_PASSWORD", "secret")
	t.Setenv("TASKFORGE_REDIS_DB", "2")
	t.Setenv("TASKFORGE_REDIS_CONNECT_TIMEOUT", "3s")
	t.Setenv("TASKFORGE_REDIS_TLS_ENABLED", "true")
	t.Setenv("TASKFORGE_REDIS_TLS_SERVER_NAME", "redis.internal")
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[
		{
			"name":"critical",
			"queue":"critical",
			"concurrency":2,
			"prefetch":6,
			"task_timeout":"12s",
			"retry":{
				"max_deliveries":5,
				"initial_backoff":"2s",
				"max_backoff":"1m",
				"multiplier":3,
				"jitter":0.1,
				"max_task_age":"10m"
			},
			"task_limits":[{"task_name":"demo.nightly","max_concurrency":1}],
			"fairness":{
				"default":{"hard_quota":2},
				"rules":[
					{"name":"protected","keys":["tenant-vip"],"weight":2,"reserved_concurrency":1,"hard_quota":1}
				]
			},
			"admission":{
				"mode":"defer",
				"max_pending":10,
				"max_pending_per_fairness_key":4,
				"max_oldest_ready_age":"30s",
				"max_retry_backlog":3,
				"defer_interval":"5s"
			},
			"adaptive":{
				"enabled":true,
				"min_concurrency":1,
				"max_concurrency":6,
				"control_period":"2s",
				"latency_threshold":"500ms",
				"error_rate_threshold":0.25,
				"backlog_threshold":10
			}
		}
	]`)
	t.Setenv("TASKFORGE_LEASE_TTL", "45s")
	t.Setenv("TASKFORGE_DEPENDENCY_BUDGETS_JSON", `[{"name":"downstream-api","capacity":5}]`)
	t.Setenv("TASKFORGE_ROUTING_POLICY_JSON", `{
		"default_queue":"default",
		"default_shard":"shard-a",
		"rules":[
			{
				"name":"critical-tenant",
				"match":{"task_names":["demo.nightly"],"fairness_keys":["tenant-vip"],"traffic_classes":["critical"]},
				"destination":{"queue":"critical","shard":"shard-b"}
			}
		]
	}`)
	t.Setenv("TASKFORGE_TASK_BUDGETS_JSON", `[{"task_name":"demo.nightly","budget":"downstream-api","tokens":2}]`)
	t.Setenv("TASKFORGE_TASK_TYPE_LIMITS_JSON", `[{"task_name":"shared.sync","max_concurrency":2}]`)
	t.Setenv("TASKFORGE_POLL_INTERVAL", "250ms")
	t.Setenv("TASKFORGE_SHUTDOWN_TIMEOUT", "20s")
	t.Setenv("TASKFORGE_SCHEDULER_LOCK_TTL", "20s")
	t.Setenv("TASKFORGE_SCHEDULER_RENEW_INTERVAL", "4s")
	t.Setenv("TASKFORGE_TASK_SUCCESS_RETENTION", "2h")
	t.Setenv("TASKFORGE_TASK_FAILURE_RETENTION", "72h")
	t.Setenv("TASKFORGE_TASK_PAYLOAD_RETENTION", "30m")
	t.Setenv("TASKFORGE_SCHEDULES_JSON", `[{"id":"nightly","interval":"15m","queue":"critical","fairness_key":"tenant-vip","task_name":"demo.nightly","payload":{"job":"nightly"},"headers":{"x-source":"config"},"enabled":true,"misfire_policy":"coalesce","start_at":"2026-04-14T10:00:00Z"}]`)
	t.Setenv("TASKFORGE_OTEL_ENABLED", "true")
	t.Setenv("TASKFORGE_SERVICE_NAME", "custom-service")

	cfg, err := Load("ignored")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.LogLevel != "debug" || cfg.HTTPAddr != ":9090" {
		t.Fatalf("unexpected address/log fields: %+v", cfg)
	}
	if cfg.HTTPAuthToken != "0123456789abcdef0123456789abcdef" {
		t.Fatal("HTTPAuthToken override was not loaded")
	}
	if cfg.HTTPReadHeaderTimeout != 2*time.Second || cfg.HTTPReadTimeout != 7*time.Second || cfg.HTTPWriteTimeout != 11*time.Second || cfg.HTTPIdleTimeout != 45*time.Second {
		t.Fatalf("unexpected HTTP timeout overrides: %+v", cfg)
	}
	if cfg.HTTPMaxBodyBytes != 2048 || cfg.HTTPMaxHeaderBytes != 8192 {
		t.Fatalf("unexpected HTTP size overrides: body=%d header=%d", cfg.HTTPMaxBodyBytes, cfg.HTTPMaxHeaderBytes)
	}
	if cfg.RedisAddr != "redis.internal:6379" || cfg.RedisPassword != "secret" || cfg.RedisDB != 2 {
		t.Fatalf("unexpected redis fields: %+v", cfg)
	}
	if !cfg.RedisTLS.Enabled || cfg.RedisTLS.ServerName != "redis.internal" || cfg.RedisConnectTimeout != 3*time.Second {
		t.Fatalf("unexpected Redis connectivity fields: %+v", cfg)
	}
	if len(cfg.WorkerPools) != 1 {
		t.Fatalf("WorkerPools length = %d, want 1", len(cfg.WorkerPools))
	}
	pool := cfg.WorkerPools[0]
	if pool.Name != "critical" || pool.Queue != "critical" {
		t.Fatalf("unexpected worker pool identity: %+v", pool)
	}
	if pool.Concurrency != 2 || pool.Prefetch != 6 || pool.LeaseTTL != 45*time.Second || pool.TaskTimeout != 12*time.Second {
		t.Fatalf("unexpected worker pool runtime: %+v", pool)
	}
	if pool.RetryPolicy.MaxDeliveries != 5 || pool.RetryPolicy.InitialBackoff != 2*time.Second || pool.RetryPolicy.MaxBackoff != time.Minute {
		t.Fatalf("unexpected worker pool retry policy: %+v", pool.RetryPolicy)
	}
	if pool.TaskTypeLimits["demo.nightly"] != 1 {
		t.Fatalf("unexpected worker pool task limits: %+v", pool.TaskTypeLimits)
	}
	if pool.FairnessPolicy == nil {
		t.Fatalf("expected worker pool fairness policy")
	}
	if pool.Admission.Mode != taskforgeredis.AdmissionModeDefer || pool.Admission.MaxPending != 10 || pool.Admission.MaxPendingPerFairnessKey != 4 {
		t.Fatalf("unexpected worker pool admission policy: %+v", pool.Admission)
	}
	if pool.Admission.MaxOldestReadyAge != 30*time.Second || pool.Admission.MaxRetryBacklog != 3 || pool.Admission.DeferInterval != 5*time.Second {
		t.Fatalf("unexpected admission timing values: %+v", pool.Admission)
	}
	if !pool.Adaptive.Enabled || pool.Adaptive.MinConcurrency != 1 || pool.Adaptive.MaxConcurrency != 6 {
		t.Fatalf("unexpected adaptive config: %+v", pool.Adaptive)
	}
	if pool.Adaptive.ControlPeriod != 2*time.Second || pool.Adaptive.Cooldown != 6*time.Second || pool.Adaptive.LatencyThreshold != 500*time.Millisecond {
		t.Fatalf("unexpected adaptive timing config: %+v", pool.Adaptive)
	}
	if pool.Adaptive.ScaleUpStep != 1 || pool.Adaptive.ScaleDownStep != 1 || pool.Adaptive.ErrorRateThreshold != 0.25 || pool.Adaptive.BacklogThreshold != 10 || pool.Adaptive.HealthyWindowsRequired != 2 {
		t.Fatalf("unexpected adaptive thresholds: %+v", pool.Adaptive)
	}
	protected := pool.FairnessPolicy.Resolve("tenant-vip")
	if protected.Bucket != "protected" || protected.Weight != 2 || protected.ReservedConcurrency != 1 {
		t.Fatalf("unexpected fairness rule resolution: %+v", protected)
	}
	defaultRule := pool.FairnessPolicy.Resolve(taskforgeredis.DefaultKey)
	if defaultRule.Bucket != "default" || defaultRule.HardQuota != 2 {
		t.Fatalf("unexpected default fairness rule: %+v", defaultRule)
	}
	if cfg.TaskTypeLimits["shared.sync"] != 2 {
		t.Fatalf("unexpected global task type limits: %+v", cfg.TaskTypeLimits)
	}
	if cfg.DependencyBudgets["downstream-api"].Capacity != 5 {
		t.Fatalf("unexpected dependency budgets: %+v", cfg.DependencyBudgets)
	}
	if cfg.TaskBudgets["demo.nightly"].Budget != "downstream-api" || cfg.TaskBudgets["demo.nightly"].Tokens != 2 {
		t.Fatalf("unexpected task budgets: %+v", cfg.TaskBudgets)
	}
	if cfg.RoutingPolicy == nil {
		t.Fatalf("expected routing policy")
	}
	placement := cfg.RoutingPolicy.Place(brokerTask("demo.nightly", "tenant-vip", "critical"))
	if placement.Queue != "critical" || placement.Shard != "shard-b" || placement.Rule != "critical-tenant" {
		t.Fatalf("unexpected routing placement: %+v", placement)
	}
	if cfg.PollInterval != 250*time.Millisecond {
		t.Fatalf("PollInterval = %v, want %v", cfg.PollInterval, 250*time.Millisecond)
	}
	if cfg.SchedulerLockTTL != 20*time.Second || cfg.SchedulerRenewInterval != 4*time.Second {
		t.Fatalf("unexpected scheduler fields: %+v", cfg)
	}
	if cfg.TaskSuccessRetention != 2*time.Hour || cfg.TaskFailureRetention != 72*time.Hour || cfg.TaskPayloadRetention != 30*time.Minute {
		t.Fatalf("unexpected task retention fields: %+v", cfg)
	}
	if len(cfg.RecurringSchedules) != 1 {
		t.Fatalf("RecurringSchedules length = %d, want 1", len(cfg.RecurringSchedules))
	}
	schedule := cfg.RecurringSchedules[0]
	if schedule.ID != "nightly" || schedule.Queue != "critical" || schedule.TaskName != "demo.nightly" {
		t.Fatalf("unexpected schedule identity fields: %+v", schedule)
	}
	if schedule.FairnessKey != "tenant-vip" {
		t.Fatalf("schedule fairness key = %q, want %q", schedule.FairnessKey, "tenant-vip")
	}
	if !schedule.Enabled {
		t.Fatal("schedule enabled = false, want true")
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

func TestEnvironmentAndGoConfigurationNormalizeIdentically(t *testing.T) {
	t.Setenv("TASKFORGE_LEASE_TTL", "20s")
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[{
		"name":"critical","queue":"critical","concurrency":2,"prefetch":4,"task_timeout":"10s",
		"retry":{"max_deliveries":4},
		"fairness":{"rules":[{"name":"vip","keys":["tenant-vip"],"reserved_concurrency":1,"hard_quota":1}]},
		"admission":{"mode":"defer","max_pending_per_fairness_key":10},
		"adaptive":{"enabled":true,"min_concurrency":1,"max_concurrency":4}
	}]`)
	t.Setenv("TASKFORGE_DEPENDENCY_BUDGETS_JSON", `[{"name":"api","capacity":2}]`)
	t.Setenv("TASKFORGE_TASK_BUDGETS_JSON", `[{"task_name":"report","budget":"api"}]`)

	environment, err := Load("taskforge-test")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	goConfig, err := (taskforge.Config{
		LeaseTTL: 20 * time.Second,
		WorkerPools: []taskforge.WorkerPoolConfig{{
			Name: "critical", Queue: "critical", Concurrency: 2, Prefetch: 4, TaskTimeout: 10 * time.Second,
			Retry: taskforge.RetryPolicy{MaxDeliveries: 4},
			Fairness: &taskforge.FairnessConfig{Rules: []taskforge.FairnessRule{{
				Name: "vip", Keys: []string{"tenant-vip"}, ReservedConcurrency: 1, HardQuota: 1,
			}}},
			Admission: taskforge.AdmissionPolicy{Mode: taskforge.AdmissionDefer, MaxPendingPerFairnessKey: 10},
			Adaptive:  taskforge.AdaptiveConcurrencyConfig{Enabled: true, MinConcurrency: 1, MaxConcurrency: 4},
		}},
		DependencyBudgets: []taskforge.DependencyBudget{{Name: "api", Capacity: 2}},
		TaskBudgets:       []taskforge.TaskBudget{{TaskName: "report", Budget: "api"}},
	}).Normalize()
	if err != nil {
		t.Fatalf("Go Normalize() error = %v", err)
	}
	if !reflect.DeepEqual(environment.Control, goConfig) {
		t.Fatalf("environment control differs from Go control:\nenv = %#v\ngo  = %#v", environment.Control, goConfig)
	}
}

func TestEnvironmentAndGoConfigurationShareValidationErrors(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[{
		"name":"default","concurrency":1,
		"admission":{"mode":"defer","max_pending_per_fairness_key":1}
	}]`)

	_, environmentErr := Load("taskforge-test")
	goErr := (taskforge.Config{WorkerPools: []taskforge.WorkerPoolConfig{{
		Name: "default", Concurrency: 1,
		Admission: taskforge.AdmissionPolicy{Mode: taskforge.AdmissionDefer, MaxPendingPerFairnessKey: 1},
	}}}).Validate()
	if goErr == nil {
		t.Fatal("Go Validate() error = nil")
	}
	if environmentErr == nil || !strings.Contains(environmentErr.Error(), goErr.Error()) {
		t.Fatalf("environment error = %v, want containing Go error %q", environmentErr, goErr)
	}
}

func TestLoadRejectsUnknownControlField(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[{"name":"default","concurrency":1,"scale_up_step":2}]`)

	_, err := Load("taskforge-test")
	if err == nil || !strings.Contains(err.Error(), `unknown field "scale_up_step"`) {
		t.Fatalf("Load() error = %v, want unknown field error", err)
	}
}

func TestLoadForRoleAllowsEmptyWorkerPoolsForScheduler(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[]`)

	cfg, err := LoadForRole("taskforge-scheduler", ServiceRoleScheduler)
	if err != nil {
		t.Fatalf("LoadForRole() error = %v", err)
	}
	if len(cfg.WorkerPools) != 0 {
		t.Fatalf("WorkerPools length = %d, want 0", len(cfg.WorkerPools))
	}
}

func TestLoadForRoleAllowsEmptyWorkerPoolsForAPI(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[]`)

	cfg, err := LoadForRole("taskforge-api", ServiceRoleAPI)
	if err != nil {
		t.Fatalf("LoadForRole() error = %v", err)
	}
	if len(cfg.WorkerPools) != 0 {
		t.Fatalf("WorkerPools length = %d, want 0", len(cfg.WorkerPools))
	}
}

func TestLoadInvalidDuration(t *testing.T) {
	t.Setenv("TASKFORGE_POLL_INTERVAL", "not-a-duration")

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsWeakHTTPAuthToken(t *testing.T) {
	t.Setenv("TASKFORGE_HTTP_AUTH_TOKEN", "short-token")

	_, err := LoadForRole("taskforge-test", ServiceRoleAPI)
	if err == nil || !strings.Contains(err.Error(), "at least 32 characters") {
		t.Fatalf("LoadForRole() error = %v, want token length error", err)
	}
}

func TestLoadRejectsInvalidHTTPResourceLimits(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "read header timeout", key: "TASKFORGE_HTTP_READ_HEADER_TIMEOUT", value: "0s"},
		{name: "read timeout", key: "TASKFORGE_HTTP_READ_TIMEOUT", value: "invalid"},
		{name: "write timeout", key: "TASKFORGE_HTTP_WRITE_TIMEOUT", value: "-1s"},
		{name: "idle timeout", key: "TASKFORGE_HTTP_IDLE_TIMEOUT", value: "0s"},
		{name: "body bytes", key: "TASKFORGE_HTTP_MAX_BODY_BYTES", value: "0"},
		{name: "header bytes", key: "TASKFORGE_HTTP_MAX_HEADER_BYTES", value: "-1"},
		{name: "shutdown timeout", key: "TASKFORGE_SHUTDOWN_TIMEOUT", value: "0s"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			_, err := LoadForRole("taskforge-test", ServiceRoleAPI)
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("LoadForRole() error = %v, want error for %s", err, tc.key)
			}
		})
	}
}

func TestLoadRejectsNegativeTaskRetention(t *testing.T) {
	t.Setenv("TASKFORGE_TASK_FAILURE_RETENTION", "-1s")

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

func TestLoadRejectsInvalidWorkerPools(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[{"name":"default","queue":"default","concurrency":-1}]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsEmptyWorkerPoolsForWorkerRole(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[]`)

	_, err := LoadForRole("taskforge-worker", ServiceRoleWorker)
	if err == nil {
		t.Fatal("LoadForRole() error = nil, want non-nil")
	}
}

func TestLoadRejectsDuplicatePoolQueue(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[
		{"name":"default","queue":"default","concurrency":1,"prefetch":1},
		{"name":"critical","queue":"default","concurrency":1,"prefetch":1}
	]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsInvalidTaskTypeLimit(t *testing.T) {
	t.Setenv("TASKFORGE_TASK_TYPE_LIMITS_JSON", `[{"task_name":"demo.echo","max_concurrency":0}]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsUnknownTaskBudgetReference(t *testing.T) {
	t.Setenv("TASKFORGE_DEPENDENCY_BUDGETS_JSON", `[{"name":"known","capacity":1}]`)
	t.Setenv("TASKFORGE_TASK_BUDGETS_JSON", `[{"task_name":"demo.echo","budget":"missing"}]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsInvalidAdaptiveBounds(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[
		{
			"name":"default",
			"queue":"default",
			"concurrency":2,
			"prefetch":2,
			"adaptive":{
				"enabled":true,
				"min_concurrency":1,
				"max_concurrency":3,
				"control_period":"1s",
				"latency_threshold":"1s",
				"error_rate_threshold":0.5,
				"backlog_threshold":1
			}
		}
	]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadTaskBudgetDefaultsTokensToOne(t *testing.T) {
	t.Setenv("TASKFORGE_DEPENDENCY_BUDGETS_JSON", `[{"name":"downstream","capacity":2}]`)
	t.Setenv("TASKFORGE_TASK_BUDGETS_JSON", `[{"task_name":"demo.echo","budget":"downstream"}]`)

	cfg, err := Load("taskforge-test")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TaskBudgets["demo.echo"].Tokens != 1 {
		t.Fatalf("task budget tokens = %d, want 1", cfg.TaskBudgets["demo.echo"].Tokens)
	}
}

func TestLoadRejectsInvalidFairnessPolicy(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[
		{
			"name":"default",
			"queue":"default",
			"concurrency":1,
			"fairness":{
				"default":{"reserved_concurrency":2,"hard_quota":1}
			}
		}
	]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsAdmissionWithoutThreshold(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[
		{
			"name":"default",
			"queue":"default",
			"concurrency":1,
			"admission":{"mode":"reject"}
		}
	]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsFairnessAdmissionWithoutFairnessPolicy(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[
		{
			"name":"default",
			"queue":"default",
			"concurrency":1,
			"admission":{"mode":"defer","max_pending_per_fairness_key":1,"defer_interval":"1s"}
		}
	]`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadNormalizesRetryDefaults(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_POOLS_JSON", `[{"name":"default","queue":"default","concurrency":1,"retry":{"max_deliveries":7}}]`)

	cfg, err := Load("taskforge-test")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WorkerPools[0].RetryPolicy.MaxDeliveries != 7 {
		t.Fatalf("unexpected normalized retry policy: %+v", cfg.WorkerPools[0].RetryPolicy)
	}
}

func TestLoadRejectsInvalidScheduleJSON(t *testing.T) {
	t.Setenv("TASKFORGE_SCHEDULES_JSON", `{"id":"bad"}`)

	_, err := Load("taskforge-test")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
}

func TestLoadRejectsInvalidRoutingPolicy(t *testing.T) {
	t.Setenv("TASKFORGE_ROUTING_POLICY_JSON", `{"rules":[{"name":"missing-match","destination":{"queue":"default"}}]}`)

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

func brokerTask(name, fairnessKey, trafficClass string) taskforge.Task {
	return taskforge.Task{
		ID:          "task-1",
		Name:        name,
		FairnessKey: fairnessKey,
		Headers:     map[string]string{taskforgeredis.HeaderTrafficClass: trafficClass},
	}
}
