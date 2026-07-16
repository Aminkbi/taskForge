package worker

import (
	"fmt"

	"github.com/aminkbi/taskforge"
)

// OptionsFromConfig applies one validated worker pool to base. Runtime
// dependencies such as Broker, Handler, Logger, and ConsumerID are preserved.
func OptionsFromConfig(base Options, config taskforge.Config, poolName string) (Options, error) {
	normalized, err := config.Normalize()
	if err != nil {
		return Options{}, fmt.Errorf("worker options: %w", err)
	}
	var pool *taskforge.WorkerPoolConfig
	for i := range normalized.WorkerPools {
		if normalized.WorkerPools[i].Name == poolName {
			pool = &normalized.WorkerPools[i]
			break
		}
	}
	if pool == nil {
		return Options{}, fmt.Errorf("worker options: unknown worker pool %q", poolName)
	}

	base.PoolName = pool.Name
	base.Queue = pool.Queue
	base.LeaseTTL = normalized.LeaseTTL
	base.TaskTimeout = pool.TaskTimeout
	base.Concurrency = pool.Concurrency
	base.Prefetch = pool.Prefetch
	base.RetryPolicy = pool.Retry
	base.GlobalTaskLimits = taskLimitMap(normalized.TaskTypeLimits)
	base.PoolTaskLimits = taskLimitMap(pool.TaskTypeLimits)
	base.TaskBudgets = make(map[string]TaskBudget, len(normalized.TaskBudgets))
	for _, mapping := range normalized.TaskBudgets {
		base.TaskBudgets[mapping.TaskName] = TaskBudget{Budget: mapping.Budget, Tokens: mapping.Tokens}
	}
	if len(base.TaskBudgets) == 0 {
		base.TaskBudgets = nil
	}
	if pool.Adaptive.Enabled {
		base.Adaptive = AdaptiveConfig{
			Enabled:                true,
			MinConcurrency:         pool.Adaptive.MinConcurrency,
			MaxConcurrency:         pool.Adaptive.MaxConcurrency,
			ControlPeriod:          pool.Adaptive.ControlPeriod,
			Cooldown:               3 * pool.Adaptive.ControlPeriod,
			ScaleUpStep:            1,
			ScaleDownStep:          1,
			LatencyThreshold:       pool.Adaptive.LatencyThreshold,
			ErrorRateThreshold:     pool.Adaptive.ErrorRateThreshold,
			BacklogThreshold:       pool.Adaptive.BacklogThreshold,
			HealthyWindowsRequired: 2,
		}
	} else {
		base.Adaptive = AdaptiveConfig{}
	}

	if base.BudgetManager == nil {
		base.BudgetManager, _ = base.Broker.(BudgetManager)
	}
	if base.AdaptiveStore == nil {
		base.AdaptiveStore, _ = base.Broker.(AdaptiveStateWriter)
	}
	if base.LifecycleWriter == nil {
		base.LifecycleWriter, _ = base.Broker.(WorkerLifecycleWriter)
	}
	return base, nil
}

// NewFromConfig validates and applies poolName before constructing a worker.
func NewFromConfig(config taskforge.Config, poolName string, base Options) (*Worker, error) {
	options, err := OptionsFromConfig(base, config, poolName)
	if err != nil {
		return nil, err
	}
	return New(options)
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
