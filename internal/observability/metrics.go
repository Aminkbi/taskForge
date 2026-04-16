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

type FairnessMetricsSnapshot struct {
	Bucket         string
	Depth          float64
	Reserved       float64
	OldestReadyAge float64
	Weight         float64
}

type FairnessMetricsProvider interface {
	FairnessMetricsSnapshot(ctx context.Context, queue string, now time.Time) ([]FairnessMetricsSnapshot, error)
}

type DeadLetterMetricsProvider interface {
	DeadLetterQueueSize(ctx context.Context, queue string) (float64, error)
}

type SchedulerLagMetricsProvider interface {
	SchedulerLag(ctx context.Context, now time.Time, queue string) (float64, error)
}

type AdmissionStatusSnapshot struct {
	Queue              string
	Mode               string
	State              string
	Reason             string
	QueuePending       float64
	FairnessKeyPending float64
	OldestReadyAge     float64
	RetryBacklog       float64
	DeadLetterSize     float64
	DeferInterval      time.Duration
	UpdatedAt          time.Time
}

type AdmissionStatusProvider interface {
	AdmissionStatusSnapshot(ctx context.Context, queue string, now time.Time) (AdmissionStatusSnapshot, error)
}

type DependencyBudgetUsageSnapshot struct {
	Budget   string
	Capacity float64
	InUse    float64
}

type DependencyBudgetUsageProvider interface {
	DependencyBudgetUsageSnapshots(ctx context.Context) ([]DependencyBudgetUsageSnapshot, error)
}

type AdaptivePoolSnapshot struct {
	Pool                  string
	Queue                 string
	AdaptiveEnabled       bool
	ConfiguredConcurrency float64
	EffectiveConcurrency  float64
	MinConcurrency        float64
	MaxConcurrency        float64
	AvgLatencySeconds     float64
	ErrorRate             float64
	BudgetBlocked         float64
	Backlog               float64
	HealthyWindows        float64
	LastAdjustmentAction  string
	LastAdjustmentReason  string
	LastAdjustedAt        time.Time
}

type AdaptiveStatusProvider interface {
	AdaptiveStatusSnapshot(ctx context.Context, pool string) (AdaptivePoolSnapshot, error)
}

type Metrics struct {
	Registry                           *prometheus.Registry
	TasksPublishedTotal                *prometheus.CounterVec
	TasksReservedTotal                 *prometheus.CounterVec
	TasksReclaimedTotal                *prometheus.CounterVec
	TasksCompletedTotal                *prometheus.CounterVec
	TasksFailedTotal                   *prometheus.CounterVec
	TasksRetriedTotal                  *prometheus.CounterVec
	TasksDeadLetteredTotal             *prometheus.CounterVec
	LeaseExtensionFailures             *prometheus.CounterVec
	TaskRetrySchedules                 *prometheus.CounterVec
	TaskDeadLetterResults              *prometheus.CounterVec
	TaskExecutionDuration              *prometheus.HistogramVec
	BrokerReserveLatency               *prometheus.HistogramVec
	WorkerActiveTasks                  *prometheus.GaugeVec
	FairnessReservations               *prometheus.CounterVec
	FairnessQuotaDeferrals             *prometheus.CounterVec
	AdmissionDecisions                 *prometheus.CounterVec
	WorkerEffectiveConcurrency         *prometheus.GaugeVec
	WorkerConcurrencyAdjustmentsTotal  *prometheus.CounterVec
	DependencyBudgetBlockedTotal       *prometheus.CounterVec
	DependencyBudgetLeaseRenewFailures *prometheus.CounterVec
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
		BrokerReserveLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "taskforge_broker_reserve_latency_seconds",
			Help:    "Broker reserve latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue"}),
		WorkerActiveTasks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taskforge_worker_active_tasks",
			Help: "Current number of active worker tasks.",
		}, []string{"queue", "task_name"}),
		FairnessReservations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_fairness_reservations_total",
			Help: "Total number of fairness-aware reservations by queue and fairness bucket.",
		}, []string{"queue", "fairness_bucket"}),
		FairnessQuotaDeferrals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_fairness_quota_deferrals_total",
			Help: "Total number of fairness quota deferrals by queue, fairness bucket, and reason.",
		}, []string{"queue", "fairness_bucket", "reason"}),
		AdmissionDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_admission_decisions_total",
			Help: "Total number of publish admission decisions by queue, source, decision, and reason.",
		}, []string{"queue", "source", "decision", "reason"}),
		WorkerEffectiveConcurrency: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taskforge_worker_effective_concurrency",
			Help: "Current effective worker concurrency by pool and queue.",
		}, []string{"pool", "queue"}),
		WorkerConcurrencyAdjustmentsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_worker_concurrency_adjustments_total",
			Help: "Total number of worker concurrency adjustments by pool, reason, and action.",
		}, []string{"pool", "reason", "action"}),
		DependencyBudgetBlockedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_dependency_budget_blocked_total",
			Help: "Total number of dispatch attempts blocked by dependency budget exhaustion.",
		}, []string{"budget"}),
		DependencyBudgetLeaseRenewFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_dependency_budget_lease_renew_failures_total",
			Help: "Total number of dependency budget lease renewals that failed.",
		}, []string{"budget"}),
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
		m.BrokerReserveLatency,
		m.WorkerActiveTasks,
		m.FairnessReservations,
		m.FairnessQuotaDeferrals,
		m.AdmissionDecisions,
		m.WorkerEffectiveConcurrency,
		m.WorkerConcurrencyAdjustmentsTotal,
		m.DependencyBudgetBlockedTotal,
		m.DependencyBudgetLeaseRenewFailures,
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

func (m *Metrics) ObserveReserveLatency(queue string, duration float64) {
	if m == nil {
		return
	}
	m.BrokerReserveLatency.WithLabelValues(queue).Observe(duration)
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

func (m *Metrics) IncFairnessReservation(queue, bucket string) {
	if m == nil {
		return
	}
	m.FairnessReservations.WithLabelValues(queue, sanitizeFairnessBucket(bucket)).Inc()
}

func (m *Metrics) IncFairnessQuotaDeferral(queue, bucket, reason string) {
	if m == nil {
		return
	}
	m.FairnessQuotaDeferrals.WithLabelValues(queue, sanitizeFairnessBucket(bucket), sanitizeResultClass(reason)).Inc()
}

func (m *Metrics) IncAdmissionDecision(queue, source, decision, reason string) {
	if m == nil {
		return
	}
	m.AdmissionDecisions.WithLabelValues(queue, sanitizeAdmissionSource(source), sanitizeAdmissionDecision(decision), sanitizeAdmissionReason(reason)).Inc()
}

func (m *Metrics) SetWorkerEffectiveConcurrency(pool, queue string, value float64) {
	if m == nil {
		return
	}
	m.WorkerEffectiveConcurrency.WithLabelValues(sanitizePoolName(pool), queue).Set(value)
}

func (m *Metrics) IncWorkerConcurrencyAdjustment(pool, reason, action string) {
	if m == nil {
		return
	}
	m.WorkerConcurrencyAdjustmentsTotal.WithLabelValues(sanitizePoolName(pool), sanitizeAdaptiveReason(reason), sanitizeAdaptiveAction(action)).Inc()
}

func (m *Metrics) IncDependencyBudgetBlocked(budget string) {
	if m == nil {
		return
	}
	m.DependencyBudgetBlockedTotal.WithLabelValues(sanitizeBudgetName(budget)).Inc()
}

func (m *Metrics) IncDependencyBudgetLeaseRenewFailure(budget string) {
	if m == nil {
		return
	}
	m.DependencyBudgetLeaseRenewFailures.WithLabelValues(sanitizeBudgetName(budget)).Inc()
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

func (m *Metrics) RegisterFairnessMetricsCollector(provider FairnessMetricsProvider, queues []string) error {
	if m == nil || provider == nil || len(queues) == 0 {
		return nil
	}

	return m.Registry.Register(&fairnessMetricsCollector{
		provider: provider,
		queues:   normalizeQueues(queues),
		depth: prometheus.NewDesc(
			"taskforge_fairness_queue_depth",
			"Current ready depth for a fairness bucket on a queue.",
			[]string{"queue", "fairness_bucket"},
			nil,
		),
		reserved: prometheus.NewDesc(
			"taskforge_fairness_reserved",
			"Current reserved deliveries for a fairness bucket on a queue.",
			[]string{"queue", "fairness_bucket"},
			nil,
		),
		oldestReadyAge: prometheus.NewDesc(
			"taskforge_fairness_oldest_ready_seconds",
			"Age in seconds of the oldest ready work in a fairness bucket.",
			[]string{"queue", "fairness_bucket"},
			nil,
		),
		weight: prometheus.NewDesc(
			"taskforge_fairness_rule_weight",
			"Configured scheduling weight for a fairness bucket.",
			[]string{"queue", "fairness_bucket"},
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

func (m *Metrics) RegisterAdmissionStatusCollector(provider AdmissionStatusProvider, queues []string) error {
	if m == nil || provider == nil || len(queues) == 0 {
		return nil
	}

	return m.Registry.Register(&admissionStatusCollector{
		provider: provider,
		queues:   normalizeQueues(queues),
		state: prometheus.NewDesc(
			"taskforge_admission_state",
			"Current admission state for a queue.",
			[]string{"queue", "state"},
			nil,
		),
		signal: prometheus.NewDesc(
			"taskforge_admission_signal",
			"Current admission signal values for a queue.",
			[]string{"queue", "signal"},
			nil,
		),
	})
}

func (m *Metrics) RegisterDependencyBudgetCollector(provider DependencyBudgetUsageProvider) error {
	if m == nil || provider == nil {
		return nil
	}

	return m.Registry.Register(&dependencyBudgetCollector{
		provider: provider,
		capacity: prometheus.NewDesc(
			"taskforge_dependency_budget_capacity",
			"Configured token capacity for a dependency budget.",
			[]string{"budget"},
			nil,
		),
		inUse: prometheus.NewDesc(
			"taskforge_dependency_budget_in_use",
			"Current number of dependency budget tokens in use.",
			[]string{"budget"},
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

type fairnessMetricsCollector struct {
	provider       FairnessMetricsProvider
	queues         []string
	depth          *prometheus.Desc
	reserved       *prometheus.Desc
	oldestReadyAge *prometheus.Desc
	weight         *prometheus.Desc
}

func (c *fairnessMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.reserved
	ch <- c.oldestReadyAge
	ch <- c.weight
}

func (c *fairnessMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, queue := range c.queues {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		snapshots, err := c.provider.FairnessMetricsSnapshot(ctx, queue, time.Now().UTC())
		cancel()
		if err != nil {
			continue
		}

		for _, snapshot := range snapshots {
			bucket := sanitizeFairnessBucket(snapshot.Bucket)
			ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, snapshot.Depth, queue, bucket)
			ch <- prometheus.MustNewConstMetric(c.reserved, prometheus.GaugeValue, snapshot.Reserved, queue, bucket)
			ch <- prometheus.MustNewConstMetric(c.oldestReadyAge, prometheus.GaugeValue, snapshot.OldestReadyAge, queue, bucket)
			ch <- prometheus.MustNewConstMetric(c.weight, prometheus.GaugeValue, snapshot.Weight, queue, bucket)
		}
	}
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

type admissionStatusCollector struct {
	provider AdmissionStatusProvider
	queues   []string
	state    *prometheus.Desc
	signal   *prometheus.Desc
}

type dependencyBudgetCollector struct {
	provider DependencyBudgetUsageProvider
	capacity *prometheus.Desc
	inUse    *prometheus.Desc
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

func (c *admissionStatusCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.state
	ch <- c.signal
}

func (c *admissionStatusCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now().UTC()
	for _, queue := range c.queues {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		snapshot, err := c.provider.AdmissionStatusSnapshot(ctx, queue, now)
		cancel()
		if err != nil {
			continue
		}

		for _, state := range []string{"normal", "degraded", "rejecting"} {
			value := 0.0
			if snapshot.State == state {
				value = 1
			}
			ch <- prometheus.MustNewConstMetric(c.state, prometheus.GaugeValue, value, queue, state)
		}
		ch <- prometheus.MustNewConstMetric(c.signal, prometheus.GaugeValue, snapshot.QueuePending, queue, "queue_pending")
		ch <- prometheus.MustNewConstMetric(c.signal, prometheus.GaugeValue, snapshot.FairnessKeyPending, queue, "fairness_key_pending")
		ch <- prometheus.MustNewConstMetric(c.signal, prometheus.GaugeValue, snapshot.OldestReadyAge, queue, "oldest_ready_age_seconds")
		ch <- prometheus.MustNewConstMetric(c.signal, prometheus.GaugeValue, snapshot.RetryBacklog, queue, "retry_backlog")
		ch <- prometheus.MustNewConstMetric(c.signal, prometheus.GaugeValue, snapshot.DeadLetterSize, queue, "dead_letter_size")
	}
}

func (c *dependencyBudgetCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.capacity
	ch <- c.inUse
}

func (c *dependencyBudgetCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	snapshots, err := c.provider.DependencyBudgetUsageSnapshots(ctx)
	cancel()
	if err != nil {
		return
	}

	for _, snapshot := range snapshots {
		budget := sanitizeBudgetName(snapshot.Budget)
		ch <- prometheus.MustNewConstMetric(c.capacity, prometheus.GaugeValue, snapshot.Capacity, budget)
		ch <- prometheus.MustNewConstMetric(c.inUse, prometheus.GaugeValue, snapshot.InUse, budget)
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

func sanitizeFairnessBucket(bucket string) string {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return "default"
	}
	return bucket
}

func sanitizeAdmissionSource(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		return "unknown"
	}
	return source
}

func sanitizeAdmissionDecision(decision string) string {
	decision = strings.TrimSpace(decision)
	if decision == "" {
		return "unknown"
	}
	return decision
}

func sanitizePoolName(pool string) string {
	pool = strings.TrimSpace(pool)
	if pool == "" {
		return "default"
	}
	return pool
}

func sanitizeBudgetName(budget string) string {
	budget = strings.TrimSpace(budget)
	if budget == "" {
		return "unknown"
	}
	return budget
}

func sanitizeAdaptiveReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "none"
	}
	return reason
}

func sanitizeAdaptiveAction(action string) string {
	action = strings.TrimSpace(action)
	if action == "" {
		return "none"
	}
	return action
}

func sanitizeAdmissionReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "none"
	}
	return reason
}
