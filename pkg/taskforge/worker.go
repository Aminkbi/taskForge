package taskforge

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
)

type WorkerOptions struct {
	Broker          *RedisBroker
	Handler         Handler
	Queue           string
	PoolName        string
	ConsumerID      string
	LeaseTTL        time.Duration
	Concurrency     int
	Prefetch        int
	RetryPolicy     RetryPolicy
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
}

type Worker struct {
	manager *runtimepkg.Manager
	metrics *observability.Metrics
}

func NewWorker(options WorkerOptions) (*Worker, error) {
	if options.Broker == nil {
		return nil, fmt.Errorf("new worker: missing broker")
	}
	if options.Handler == nil {
		return nil, fmt.Errorf("new worker: missing handler")
	}
	queue := options.Queue
	if queue == "" {
		queue = "default"
	}
	poolName := options.PoolName
	if poolName == "" {
		poolName = queue
	}
	consumerID := options.ConsumerID
	if consumerID == "" {
		consumerID = "taskforge-worker"
	}
	leaseTTL := options.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = options.Broker.leaseTTL
	}
	concurrency := options.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	retryPolicy := options.RetryPolicy
	if retryPolicy == (RetryPolicy{}) {
		retryPolicy = DefaultRetryPolicy(1)
	}
	logger := options.Logger
	if logger == nil {
		logger = options.Broker.logger.With("component", "worker-runtime", "pool", poolName, "queue", queue)
	}

	worker := &runtimepkg.Worker{
		Broker:            options.Broker.internalBroker(),
		DeadLetter:        options.Broker.deadLetter,
		Handler:           runtimeHandler{handler: options.Handler},
		Logger:            logger,
		Metrics:           options.Broker.metrics,
		Clock:             clock.RealClock{},
		RetryPolicy:       retryPolicy.toInternal(),
		PoolName:          poolName,
		Queue:             queue,
		ConsumerID:        consumerID,
		LeaseTTL:          leaseTTL,
		Concurrency:       concurrency,
		Prefetch:          options.Prefetch,
		GlobalTaskLimiter: runtimepkg.NewTaskTypeLimiter(nil),
		PoolTaskLimiter:   runtimepkg.NewTaskTypeLimiter(nil),
		StateStore:        options.Broker.stateStore,
	}

	return &Worker{
		manager: &runtimepkg.Manager{
			Workers:         []*runtimepkg.Worker{worker},
			ShutdownTimeout: options.ShutdownTimeout,
		},
		metrics: options.Broker.metrics,
	}, nil
}

func RunWorker(ctx context.Context, options WorkerOptions) error {
	worker, err := NewWorker(options)
	if err != nil {
		return err
	}
	return worker.Run(ctx)
}

func (w *Worker) Run(ctx context.Context) error {
	return w.manager.Run(ctx)
}

func (w *Worker) MetricsHandler() http.Handler {
	return w.metrics.Handler()
}
