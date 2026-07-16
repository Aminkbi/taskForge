package worker

import (
	"context"
	"github.com/aminkbi/taskforge"
	"time"
)

type TaskBudget struct {
	Budget string
	Tokens int
}

type BudgetManager interface {
	AcquireLease(ctx context.Context, budget, deliveryID string, tokens int, ttl time.Duration) (bool, error)
	RenewLease(ctx context.Context, budget, deliveryID string, ttl time.Duration) error
	ReleaseLease(ctx context.Context, budget, deliveryID string) error
}

type AdaptiveStateWriter interface {
	StoreAdaptiveStatus(ctx context.Context, snapshot taskforge.AdaptivePoolSnapshot) error
}

type WorkerLifecycleWriter interface {
	StoreWorkerLifecycleSnapshot(ctx context.Context, snapshot taskforge.WorkerLifecycleSnapshot) error
}

type QueueMetricsProvider interface {
	QueueMetricsSnapshot(ctx context.Context, queue string) (taskforge.QueueMetricsSnapshot, error)
}

type AdaptiveConfig struct {
	Enabled                bool
	MinConcurrency         int
	MaxConcurrency         int
	ControlPeriod          time.Duration
	Cooldown               time.Duration
	ScaleUpStep            int
	ScaleDownStep          int
	LatencyThreshold       time.Duration
	ErrorRateThreshold     float64
	BacklogThreshold       int64
	HealthyWindowsRequired int
}
