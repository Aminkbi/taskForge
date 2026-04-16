package runtime

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/observability"
)

func TestAdaptiveConcurrencyScalesDownWithinBounds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	worker := &Worker{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     observability.NewMetrics(),
		PoolName:    "critical",
		Queue:       "critical",
		Concurrency: 3,
		Adaptive: AdaptiveConfig{
			Enabled:                true,
			MinConcurrency:         2,
			MaxConcurrency:         5,
			ControlPeriod:          time.Second,
			Cooldown:               time.Second,
			ScaleUpStep:            1,
			ScaleDownStep:          2,
			LatencyThreshold:       200 * time.Millisecond,
			ErrorRateThreshold:     0.5,
			BacklogThreshold:       1,
			HealthyWindowsRequired: 2,
		},
		QueueMetrics: staticQueueMetricsProvider{
			snapshot: observability.QueueMetricsSnapshot{Depth: 10, Reserved: 1},
		},
		Clock: fixedClock{now: now},
	}
	state := &workerState{
		effectiveConcurrency: 3,
		window: adaptiveWindow{
			executions:   2,
			totalLatency: 700 * time.Millisecond,
		},
	}
	worker.runtimeState = state

	snapshot, _, action, reason, changed, err := worker.evaluateAdaptiveWindow(context.Background(), state)
	if err != nil {
		t.Fatalf("evaluateAdaptiveWindow() error = %v", err)
	}
	if !changed || action != "scale_down" || reason != "latency" {
		t.Fatalf("adjustment = changed:%v action:%q reason:%q, want scale_down/latency", changed, action, reason)
	}
	if snapshot.EffectiveConcurrency != 2 {
		t.Fatalf("effective concurrency = %v, want 2", snapshot.EffectiveConcurrency)
	}
}

func TestAdaptiveConcurrencyCooldownBlocksRapidScaleUp(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	worker := &Worker{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     observability.NewMetrics(),
		PoolName:    "critical",
		Queue:       "critical",
		Concurrency: 2,
		Adaptive: AdaptiveConfig{
			Enabled:                true,
			MinConcurrency:         1,
			MaxConcurrency:         4,
			ControlPeriod:          time.Second,
			Cooldown:               10 * time.Second,
			ScaleUpStep:            1,
			ScaleDownStep:          1,
			LatencyThreshold:       time.Second,
			ErrorRateThreshold:     0.5,
			BacklogThreshold:       1,
			HealthyWindowsRequired: 1,
		},
		QueueMetrics: staticQueueMetricsProvider{
			snapshot: observability.QueueMetricsSnapshot{Depth: 5, Reserved: 0},
		},
		Clock: fixedClock{now: now},
	}
	state := &workerState{
		effectiveConcurrency: 2,
		healthyWindows:       1,
		lastAdjustedAt:       now.Add(-5 * time.Second),
		window: adaptiveWindow{
			executions:   2,
			totalLatency: 300 * time.Millisecond,
		},
	}
	worker.runtimeState = state

	snapshot, _, _, _, changed, err := worker.evaluateAdaptiveWindow(context.Background(), state)
	if err != nil {
		t.Fatalf("evaluateAdaptiveWindow() error = %v", err)
	}
	if changed {
		t.Fatalf("evaluateAdaptiveWindow() changed = true, want false")
	}
	if snapshot.EffectiveConcurrency != 2 {
		t.Fatalf("effective concurrency = %v, want 2", snapshot.EffectiveConcurrency)
	}
}

func TestAdaptiveConcurrencyRequiresConsecutiveHealthyWindows(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	worker := &Worker{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     observability.NewMetrics(),
		PoolName:    "critical",
		Queue:       "critical",
		Concurrency: 2,
		Adaptive: AdaptiveConfig{
			Enabled:                true,
			MinConcurrency:         1,
			MaxConcurrency:         4,
			ControlPeriod:          time.Second,
			Cooldown:               time.Second,
			ScaleUpStep:            1,
			ScaleDownStep:          1,
			LatencyThreshold:       time.Second,
			ErrorRateThreshold:     0.5,
			BacklogThreshold:       1,
			HealthyWindowsRequired: 2,
		},
		QueueMetrics: staticQueueMetricsProvider{
			snapshot: observability.QueueMetricsSnapshot{Depth: 5, Reserved: 0},
		},
		Clock: fixedClock{now: now},
	}
	state := &workerState{
		effectiveConcurrency: 2,
		window: adaptiveWindow{
			executions:   2,
			totalLatency: 300 * time.Millisecond,
		},
	}
	worker.runtimeState = state

	first, _, _, _, changed, err := worker.evaluateAdaptiveWindow(context.Background(), state)
	if err != nil {
		t.Fatalf("first evaluateAdaptiveWindow() error = %v", err)
	}
	if changed {
		t.Fatalf("first window changed = true, want false")
	}
	if first.HealthyWindows != 1 {
		t.Fatalf("first healthy windows = %v, want 1", first.HealthyWindows)
	}

	state.window = adaptiveWindow{
		executions:   2,
		totalLatency: 300 * time.Millisecond,
	}
	second, _, action, reason, changed, err := worker.evaluateAdaptiveWindow(context.Background(), state)
	if err != nil {
		t.Fatalf("second evaluateAdaptiveWindow() error = %v", err)
	}
	if !changed || action != "scale_up" || reason != "healthy_backlog" {
		t.Fatalf("second adjustment = changed:%v action:%q reason:%q, want scale_up/healthy_backlog", changed, action, reason)
	}
	if second.EffectiveConcurrency != 3 {
		t.Fatalf("effective concurrency = %v, want 3", second.EffectiveConcurrency)
	}
}

type staticQueueMetricsProvider struct {
	snapshot observability.QueueMetricsSnapshot
}

func (s staticQueueMetricsProvider) QueueMetricsSnapshot(context.Context, string) (observability.QueueMetricsSnapshot, error) {
	return s.snapshot, nil
}
