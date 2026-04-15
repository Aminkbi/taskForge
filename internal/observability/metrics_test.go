package observability

import (
	"context"
	"slices"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

type stubQueueMetricsProvider struct{}

func (stubQueueMetricsProvider) QueueMetricsSnapshot(context.Context, string) (QueueMetricsSnapshot, error) {
	return QueueMetricsSnapshot{
		Depth:     3,
		Reserved:  1,
		Consumers: 2,
	}, nil
}

type stubDeadLetterMetricsProvider struct{}

func (stubDeadLetterMetricsProvider) DeadLetterQueueSize(context.Context, string) (float64, error) {
	return 4, nil
}

type stubSchedulerLagMetricsProvider struct{}

func (stubSchedulerLagMetricsProvider) SchedulerLag(context.Context, time.Time, string) (float64, error) {
	return 2.5, nil
}

type stubFairnessMetricsProvider struct{}

func (stubFairnessMetricsProvider) FairnessMetricsSnapshot(context.Context, string, time.Time) ([]FairnessMetricsSnapshot, error) {
	return []FairnessMetricsSnapshot{
		{
			Bucket:         "protected",
			Depth:          2,
			Reserved:       1,
			OldestReadyAge: 4,
			Weight:         2,
		},
	}, nil
}

func TestMetricsExposeOnlyLowCardinalityLabels(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	metrics.IncRetryScheduled("critical", "reports.generate", "timeout")
	metrics.IncDeadLetterResult("critical", "reports.generate", "permanent")
	metrics.IncFairnessReservation("critical", "protected")
	metrics.IncFairnessQuotaDeferral("critical", "protected", "hard_quota")
	if err := metrics.RegisterQueueMetricsCollector(stubQueueMetricsProvider{}, []string{"critical"}); err != nil {
		t.Fatalf("RegisterQueueMetricsCollector() error = %v", err)
	}
	if err := metrics.RegisterFairnessMetricsCollector(stubFairnessMetricsProvider{}, []string{"critical"}); err != nil {
		t.Fatalf("RegisterFairnessMetricsCollector() error = %v", err)
	}
	if err := metrics.RegisterDeadLetterMetricsCollector(stubDeadLetterMetricsProvider{}, []string{"critical"}); err != nil {
		t.Fatalf("RegisterDeadLetterMetricsCollector() error = %v", err)
	}
	if err := metrics.RegisterSchedulerLagCollector(stubSchedulerLagMetricsProvider{}, []string{"critical"}); err != nil {
		t.Fatalf("RegisterSchedulerLagCollector() error = %v", err)
	}

	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	assertLabelNames(t, families, "taskforge_task_retry_schedules_total", []string{"queue", "result_class", "task_name"})
	assertLabelNames(t, families, "taskforge_task_dead_letter_results_total", []string{"queue", "result_class", "task_name"})
	assertLabelNames(t, families, "taskforge_fairness_reservations_total", []string{"fairness_bucket", "queue"})
	assertLabelNames(t, families, "taskforge_fairness_quota_deferrals_total", []string{"fairness_bucket", "queue", "reason"})
	assertLabelNames(t, families, "taskforge_fairness_queue_depth", []string{"fairness_bucket", "queue"})
	assertLabelNames(t, families, "taskforge_fairness_reserved", []string{"fairness_bucket", "queue"})
	assertLabelNames(t, families, "taskforge_fairness_oldest_ready_seconds", []string{"fairness_bucket", "queue"})
	assertLabelNames(t, families, "taskforge_fairness_rule_weight", []string{"fairness_bucket", "queue"})
	assertLabelNames(t, families, "taskforge_dead_letter_queue_size", []string{"queue"})
	assertLabelNames(t, families, "taskforge_scheduler_queue_lag_seconds", []string{"queue"})
	assertLabelNames(t, families, "taskforge_queue_depth", []string{"queue"})

	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "task_id" || label.GetName() == "delivery_id" || label.GetName() == "fairness_key" {
					t.Fatalf("metric %s unexpectedly uses high-cardinality label %q", family.GetName(), label.GetName())
				}
			}
		}
	}
}

func assertLabelNames(t *testing.T, families []*dto.MetricFamily, familyName string, want []string) {
	t.Helper()

	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		if len(family.GetMetric()) == 0 {
			t.Fatalf("metric family %s has no metrics", familyName)
		}

		got := make([]string, 0, len(family.GetMetric()[0].GetLabel()))
		for _, label := range family.GetMetric()[0].GetLabel() {
			got = append(got, label.GetName())
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("metric family %s labels = %v, want %v", familyName, got, want)
		}
		return
	}

	t.Fatalf("metric family %s not found", familyName)
}
