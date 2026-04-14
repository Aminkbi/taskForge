package observability

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type QueueMetricsSnapshot struct {
	Depth     float64
	Reserved  float64
	Consumers float64
}

type QueueMetricsProvider interface {
	QueueMetricsSnapshot(ctx context.Context, queue string) (QueueMetricsSnapshot, error)
}

type DeadLetterMetricsProvider interface {
	DeadLetterQueueSize(ctx context.Context, queue string) (float64, error)
}

type SchedulerLagMetricsProvider interface {
	SchedulerLag(ctx context.Context, now time.Time, queue string) (float64, error)
}

type Metrics struct {
	Registry               *prometheus.Registry
	TasksPublishedTotal    *prometheus.CounterVec
	TasksReservedTotal     *prometheus.CounterVec
	TasksReclaimedTotal    *prometheus.CounterVec
	TasksCompletedTotal    *prometheus.CounterVec
	TasksFailedTotal       *prometheus.CounterVec
	TasksRetriedTotal      *prometheus.CounterVec
	TasksDeadLetteredTotal *prometheus.CounterVec
	LeaseExtensionFailures *prometheus.CounterVec
	TaskRetrySchedules     *prometheus.CounterVec
	TaskDeadLetterResults  *prometheus.CounterVec
	TaskExecutionDuration  *prometheus.HistogramVec
	WorkerActiveTasks      *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		Registry: registry,
		TasksPublishedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_published_total",
			Help: "Total number of tasks published.",
		}, []string{"queue"}),
		TasksReservedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_reserved_total",
			Help: "Total number of tasks reserved by workers.",
		}, []string{"queue"}),
		TasksReclaimedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_reclaimed_total",
			Help: "Total number of expired task deliveries reclaimed by workers.",
		}, []string{"queue"}),
		TasksCompletedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_completed_total",
			Help: "Total number of tasks completed successfully.",
		}, []string{"queue"}),
		TasksFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_failed_total",
			Help: "Total number of task execution failures.",
		}, []string{"queue"}),
		TasksRetriedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_retried_total",
			Help: "Total number of tasks scheduled for retry.",
		}, []string{"queue"}),
		TasksDeadLetteredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_dead_lettered_total",
			Help: "Total number of tasks moved to dead letter queues.",
		}, []string{"queue"}),
		LeaseExtensionFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_lease_extension_failures_total",
			Help: "Total number of lease extension attempts that failed.",
		}, []string{"queue"}),
		TaskRetrySchedules: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_task_retry_schedules_total",
			Help: "Total number of retry schedules by queue, task, and result class.",
		}, []string{"queue", "task_name", "result_class"}),
		TaskDeadLetterResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_task_dead_letter_results_total",
			Help: "Total number of dead-letter transitions by queue, task, and result class.",
		}, []string{"queue", "task_name", "result_class"}),
		TaskExecutionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "taskforge_task_execution_duration_seconds",
			Help:    "Task execution duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue", "task_name", "status"}),
		WorkerActiveTasks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taskforge_worker_active_tasks",
			Help: "Current number of active worker tasks.",
		}, []string{"queue", "task_name"}),
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
		m.TaskRetrySchedules,
		m.TaskDeadLetterResults,
		m.TaskExecutionDuration,
		m.WorkerActiveTasks,
	)

	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func (m *Metrics) IncPublished(queue string) {
	if m == nil {
		return
	}
	m.TasksPublishedTotal.WithLabelValues(queue).Inc()
}

func (m *Metrics) IncReserved(queue string) {
	if m == nil {
		return
	}
	m.TasksReservedTotal.WithLabelValues(queue).Inc()
}

func (m *Metrics) IncReclaimed(queue string) {
	if m == nil {
		return
	}
	m.TasksReclaimedTotal.WithLabelValues(queue).Inc()
}

func (m *Metrics) IncCompleted(queue string) {
	if m == nil {
		return
	}
	m.TasksCompletedTotal.WithLabelValues(queue).Inc()
}

func (m *Metrics) IncFailed(queue string) {
	if m == nil {
		return
	}
	m.TasksFailedTotal.WithLabelValues(queue).Inc()
}

func (m *Metrics) IncRetried(queue string) {
	if m == nil {
		return
	}
	m.TasksRetriedTotal.WithLabelValues(queue).Inc()
}

func (m *Metrics) IncRetryScheduled(queue, taskName, resultClass string) {
	if m == nil {
		return
	}
	m.TasksRetriedTotal.WithLabelValues(queue).Inc()
	m.TaskRetrySchedules.WithLabelValues(queue, sanitizeTaskName(taskName), sanitizeResultClass(resultClass)).Inc()
}

func (m *Metrics) IncDeadLettered(queue string) {
	if m == nil {
		return
	}
	m.TasksDeadLetteredTotal.WithLabelValues(queue).Inc()
}

func (m *Metrics) IncDeadLetterResult(queue, taskName, resultClass string) {
	if m == nil {
		return
	}
	m.TasksDeadLetteredTotal.WithLabelValues(queue).Inc()
	m.TaskDeadLetterResults.WithLabelValues(queue, sanitizeTaskName(taskName), sanitizeResultClass(resultClass)).Inc()
}

func (m *Metrics) IncLeaseExtensionFailure(queue string) {
	if m == nil {
		return
	}
	m.LeaseExtensionFailures.WithLabelValues(queue).Inc()
}

func (m *Metrics) ObserveExecution(queue, taskName, status string, duration float64) {
	if m == nil {
		return
	}
	m.TaskExecutionDuration.WithLabelValues(queue, taskName, status).Observe(duration)
}

func (m *Metrics) IncActiveTask(queue, taskName string) {
	if m == nil {
		return
	}
	m.WorkerActiveTasks.WithLabelValues(queue, taskName).Inc()
}

func (m *Metrics) DecActiveTask(queue, taskName string) {
	if m == nil {
		return
	}
	m.WorkerActiveTasks.WithLabelValues(queue, taskName).Dec()
}

func (m *Metrics) RegisterQueueMetricsCollector(provider QueueMetricsProvider, queues []string) error {
	if m == nil || provider == nil || len(queues) == 0 {
		return nil
	}

	cleanQueues := normalizeQueues(queues)
	return m.Registry.Register(&queueMetricsCollector{
		provider: provider,
		queues:   cleanQueues,
		depth: prometheus.NewDesc(
			"taskforge_queue_depth",
			"Current ready depth for a queue.",
			[]string{"queue"},
			nil,
		),
		reserved: prometheus.NewDesc(
			"taskforge_queue_reserved",
			"Current number of reserved deliveries for a queue.",
			[]string{"queue"},
			nil,
		),
		consumers: prometheus.NewDesc(
			"taskforge_queue_consumers",
			"Current number of active consumers for a queue.",
			[]string{"queue"},
			nil,
		),
	})
}

func (m *Metrics) RegisterDeadLetterMetricsCollector(provider DeadLetterMetricsProvider, queues []string) error {
	if m == nil || provider == nil || len(queues) == 0 {
		return nil
	}

	return m.Registry.Register(&deadLetterMetricsCollector{
		provider: provider,
		queues:   normalizeQueues(queues),
		size: prometheus.NewDesc(
			"taskforge_dead_letter_queue_size",
			"Current number of messages in a dead-letter queue.",
			[]string{"queue"},
			nil,
		),
	})
}

func (m *Metrics) RegisterSchedulerLagCollector(provider SchedulerLagMetricsProvider, queues []string) error {
	if m == nil || provider == nil || len(queues) == 0 {
		return nil
	}

	return m.Registry.Register(&schedulerLagCollector{
		provider: provider,
		queues:   normalizeQueues(queues),
		lag: prometheus.NewDesc(
			"taskforge_scheduler_queue_lag_seconds",
			"Current scheduler lag in seconds for the oldest delayed task per queue.",
			[]string{"queue"},
			nil,
		),
	})
}

func normalizeQueues(queues []string) []string {
	cleanQueues := slices.Clone(queues)
	slices.Sort(cleanQueues)
	cleanQueues = slices.Compact(cleanQueues)
	return cleanQueues
}

type queueMetricsCollector struct {
	provider  QueueMetricsProvider
	queues    []string
	depth     *prometheus.Desc
	reserved  *prometheus.Desc
	consumers *prometheus.Desc
}

func (c *queueMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.reserved
	ch <- c.consumers
}

func (c *queueMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, queue := range c.queues {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		snapshot, err := c.provider.QueueMetricsSnapshot(ctx, queue)
		cancel()
		if err != nil {
			continue
		}

		ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, snapshot.Depth, queue)
		ch <- prometheus.MustNewConstMetric(c.reserved, prometheus.GaugeValue, snapshot.Reserved, queue)
		ch <- prometheus.MustNewConstMetric(c.consumers, prometheus.GaugeValue, snapshot.Consumers, queue)
	}
}

type deadLetterMetricsCollector struct {
	provider DeadLetterMetricsProvider
	queues   []string
	size     *prometheus.Desc
}

func (c *deadLetterMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.size
}

func (c *deadLetterMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, queue := range c.queues {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		size, err := c.provider.DeadLetterQueueSize(ctx, queue)
		cancel()
		if err != nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.size, prometheus.GaugeValue, size, queue)
	}
}

type schedulerLagCollector struct {
	provider SchedulerLagMetricsProvider
	queues   []string
	lag      *prometheus.Desc
}

func (c *schedulerLagCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.lag
}

func (c *schedulerLagCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now().UTC()
	for _, queue := range c.queues {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lag, err := c.provider.SchedulerLag(ctx, now, queue)
		cancel()
		if err != nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, lag, queue)
	}
}

func sanitizeTaskName(taskName string) string {
	taskName = strings.TrimSpace(taskName)
	if taskName == "" {
		return "unknown"
	}
	return taskName
}

func sanitizeResultClass(resultClass string) string {
	resultClass = strings.TrimSpace(resultClass)
	if resultClass == "" {
		return "unknown"
	}
	return resultClass
}
