package worker

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/observability"
)

type Options struct {
	Broker           taskforge.Broker
	DeadLetter       taskforge.DeadLetterPublisher
	Handler          taskforge.Handler
	StateStore       taskforge.StateStore
	Logger           *slog.Logger
	PoolName         string
	Queue            string
	ConsumerID       string
	LeaseTTL         time.Duration
	Concurrency      int
	Prefetch         int
	RetryPolicy      taskforge.RetryPolicy
	GlobalTaskLimits map[string]int
	PoolTaskLimits   map[string]int
	BudgetManager    BudgetManager
	TaskBudgets      map[string]TaskBudget
	QueueMetrics     QueueMetricsProvider
	Adaptive         AdaptiveConfig
	AdaptiveStore    AdaptiveStateWriter
	LifecycleWriter  WorkerLifecycleWriter
}

func New(options Options) (*Worker, error) {
	if options.Broker == nil {
		return nil, fmt.Errorf("new worker: missing broker")
	}
	if options.Handler == nil {
		return nil, fmt.Errorf("new worker: missing handler")
	}
	if options.Queue == "" {
		options.Queue = "default"
	}
	if options.PoolName == "" {
		options.PoolName = options.Queue
	}
	if options.ConsumerID == "" {
		options.ConsumerID = "taskforge-worker"
	}
	if options.LeaseTTL <= 0 {
		options.LeaseTTL = 30 * time.Second
	}
	if options.Concurrency <= 0 {
		options.Concurrency = 1
	}
	if options.RetryPolicy == (taskforge.RetryPolicy{}) {
		options.RetryPolicy = taskforge.DefaultRetryPolicy(1)
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.DeadLetter == nil {
		options.DeadLetter, _ = options.Broker.(taskforge.DeadLetterPublisher)
	}
	if options.StateStore == nil {
		options.StateStore, _ = options.Broker.(taskforge.StateStore)
	}
	if options.QueueMetrics == nil {
		options.QueueMetrics, _ = options.Broker.(QueueMetricsProvider)
	}
	metrics := observability.NewMetrics()
	return &Worker{
		Broker:            options.Broker,
		DeadLetter:        options.DeadLetter,
		Handler:           options.Handler,
		Logger:            options.Logger,
		Metrics:           metrics,
		Clock:             clock.RealClock{},
		RetryPolicy:       options.RetryPolicy,
		PoolName:          options.PoolName,
		Queue:             options.Queue,
		ConsumerID:        options.ConsumerID,
		LeaseTTL:          options.LeaseTTL,
		Concurrency:       options.Concurrency,
		Prefetch:          options.Prefetch,
		GlobalTaskLimiter: NewTaskTypeLimiter(options.GlobalTaskLimits),
		PoolTaskLimiter:   NewTaskTypeLimiter(options.PoolTaskLimits),
		BudgetManager:     options.BudgetManager,
		TaskBudgets:       options.TaskBudgets,
		QueueMetrics:      options.QueueMetrics,
		Adaptive:          options.Adaptive,
		AdaptiveStore:     options.AdaptiveStore,
		LifecycleWriter:   options.LifecycleWriter,
		StateStore:        options.StateStore,
	}, nil
}

func (w *Worker) MetricsHandler() http.Handler { return w.Metrics.Handler() }
