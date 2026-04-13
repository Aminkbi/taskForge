package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry               *prometheus.Registry
	TasksPublishedTotal    prometheus.Counter
	TasksReservedTotal     prometheus.Counter
	TasksReclaimedTotal    prometheus.Counter
	TasksCompletedTotal    prometheus.Counter
	TasksFailedTotal       prometheus.Counter
	TasksRetriedTotal      prometheus.Counter
	TasksDeadLetteredTotal prometheus.Counter
	LeaseExtensionFailures prometheus.Counter
	TaskExecutionDuration  *prometheus.HistogramVec
	WorkerActiveTasks      prometheus.Gauge
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		Registry: registry,
		TasksPublishedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasks_published_total",
			Help: "Total number of tasks published.",
		}),
		TasksReservedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasks_reserved_total",
			Help: "Total number of tasks reserved by workers.",
		}),
		TasksReclaimedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasks_reclaimed_total",
			Help: "Total number of expired task deliveries reclaimed by workers.",
		}),
		TasksCompletedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasks_completed_total",
			Help: "Total number of tasks completed successfully.",
		}),
		TasksFailedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasks_failed_total",
			Help: "Total number of task execution failures.",
		}),
		TasksRetriedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasks_retried_total",
			Help: "Total number of tasks scheduled for retry.",
		}),
		TasksDeadLetteredTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasks_dead_lettered_total",
			Help: "Total number of tasks moved to dead letter queues.",
		}),
		LeaseExtensionFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lease_extension_failures_total",
			Help: "Total number of lease extension attempts that failed.",
		}),
		TaskExecutionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_execution_duration_seconds",
			Help:    "Task execution duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"task_name", "status"}),
		WorkerActiveTasks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "worker_active_tasks",
			Help: "Current number of active worker tasks.",
		}),
	}

	registry.MustRegister(
		m.TasksPublishedTotal,
		m.TasksReservedTotal,
		m.TasksReclaimedTotal,
		m.TasksCompletedTotal,
		m.TasksFailedTotal,
		m.TasksRetriedTotal,
		m.TasksDeadLetteredTotal,
		m.LeaseExtensionFailures,
		m.TaskExecutionDuration,
		m.WorkerActiveTasks,
	)

	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
