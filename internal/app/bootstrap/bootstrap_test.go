package bootstrap

import (
	"errors"
	"strings"
	"testing"

	"github.com/aminkbi/taskforge/internal/observability"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterCommonMetricsReportsRegistrationConflicts(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		register func(*observability.Metrics, *taskforgeredis.Broker) error
	}{
		{name: "queue", register: func(metrics *observability.Metrics, broker *taskforgeredis.Broker) error {
			return metrics.RegisterQueueMetricsCollector(broker, []string{"default"})
		}},
		{name: "fairness", register: func(metrics *observability.Metrics, broker *taskforgeredis.Broker) error {
			return metrics.RegisterFairnessMetricsCollector(broker, []string{"default"})
		}},
		{name: "admission", register: func(metrics *observability.Metrics, broker *taskforgeredis.Broker) error {
			return metrics.RegisterAdmissionStatusCollector(broker, []string{"default"})
		}},
		{name: "dependency budget", register: func(metrics *observability.Metrics, broker *taskforgeredis.Broker) error {
			return metrics.RegisterDependencyBudgetCollector(broker)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metrics := observability.NewMetrics()
			broker := taskforgeredis.New(taskforgeredis.Options{})
			t.Cleanup(func() { _ = broker.Close() })
			if err := tc.register(metrics, broker); err != nil {
				t.Fatal(err)
			}
			err := RegisterCommonMetrics(metrics, broker, []string{"default"})
			var duplicate prometheus.AlreadyRegisteredError
			if !errors.As(err, &duplicate) || !strings.Contains(err.Error(), "register "+tc.name+" metrics") {
				t.Fatalf("RegisterCommonMetrics() error = %v, want %s registration conflict", err, tc.name)
			}
		})
	}
}
