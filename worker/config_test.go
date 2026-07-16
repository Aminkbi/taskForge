package worker

import (
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
)

func TestOptionsFromConfigCompilesWorkerControls(t *testing.T) {
	t.Parallel()

	config := taskforge.Config{
		LeaseTTL: 20 * time.Second,
		WorkerPools: []taskforge.WorkerPoolConfig{{
			Name: "critical", Queue: "critical", Concurrency: 2, Prefetch: 6,
			TaskTimeout:    12 * time.Second,
			Retry:          taskforge.RetryPolicy{MaxDeliveries: 5},
			TaskTypeLimits: []taskforge.TaskTypeLimit{{TaskName: "report", MaxConcurrency: 1}},
			Adaptive: taskforge.AdaptiveConcurrencyConfig{
				Enabled: true, MinConcurrency: 1, MaxConcurrency: 6, ControlPeriod: 2 * time.Second,
			},
		}},
		TaskTypeLimits:    []taskforge.TaskTypeLimit{{TaskName: "sync", MaxConcurrency: 2}},
		DependencyBudgets: []taskforge.DependencyBudget{{Name: "api", Capacity: 2}},
		TaskBudgets:       []taskforge.TaskBudget{{TaskName: "report", Budget: "api"}},
	}
	options, err := OptionsFromConfig(Options{ConsumerID: "worker-1"}, config, "critical")
	if err != nil {
		t.Fatalf("OptionsFromConfig() error = %v", err)
	}
	if options.PoolName != "critical" || options.Queue != "critical" || options.ConsumerID != "worker-1" {
		t.Fatalf("unexpected worker identity: %+v", options)
	}
	if options.LeaseTTL != 20*time.Second || options.TaskTimeout != 12*time.Second || options.Concurrency != 2 || options.Prefetch != 6 {
		t.Fatalf("unexpected worker runtime options: %+v", options)
	}
	if options.RetryPolicy.MaxDeliveries != 5 || options.PoolTaskLimits["report"] != 1 || options.GlobalTaskLimits["sync"] != 2 {
		t.Fatalf("unexpected worker delivery options: %+v", options)
	}
	if options.TaskBudgets["report"].Budget != "api" || options.TaskBudgets["report"].Tokens != 1 {
		t.Fatalf("unexpected worker task budget: %+v", options.TaskBudgets)
	}
	if !options.Adaptive.Enabled || options.Adaptive.Cooldown != 6*time.Second || options.Adaptive.HealthyWindowsRequired != 2 {
		t.Fatalf("unexpected compiled adaptive options: %+v", options.Adaptive)
	}
}

func TestOptionsFromConfigRejectsUnknownPool(t *testing.T) {
	t.Parallel()

	_, err := OptionsFromConfig(Options{}, taskforge.DefaultConfig(), "missing")
	if err == nil {
		t.Fatal("OptionsFromConfig() error = nil, want unknown pool error")
	}
}
