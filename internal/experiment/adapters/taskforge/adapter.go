// Package taskforge implements the TaskForge side of the neutral open-loop
// protocol. No baseline-specific behavior is imported into this package.
package taskforge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	gredis "github.com/redis/go-redis/v9"

	taskforge "github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/experiment"
	"github.com/aminkbi/taskforge/internal/experiment/adapters"
	tfredis "github.com/aminkbi/taskforge/redis"
	"github.com/aminkbi/taskforge/worker"
)

type Config struct {
	Name                     string
	Client                   *gredis.Client
	Concurrency              int
	MinConcurrency           int
	MaxConcurrency           int
	AdmissionMaxPending      int64
	DependencyBudgetCapacity int
	DisableDependencyBudget  bool
	DisableFairness          bool
	DisableAdaptive          bool
	LeaseTTL                 time.Duration
	SchedulerPeriod          time.Duration
}

type Adapter struct {
	config Config

	mu              sync.Mutex
	runtime         experiment.AdapterRuntime
	broker          *tfredis.Broker
	workerCancel    context.CancelFunc
	schedulerCancel context.CancelFunc
	workerDone      chan error
	schedulerDone   chan struct{}
	stopped         bool
}

func New(config Config) *Adapter {
	if config.Name == "" {
		config.Name = "taskforge-full"
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 16
	}
	if config.MinConcurrency <= 0 {
		config.MinConcurrency = max(1, config.Concurrency/4)
	}
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = config.Concurrency * 2
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = 5 * time.Second
	}
	if config.SchedulerPeriod <= 0 {
		config.SchedulerPeriod = 10 * time.Millisecond
	}
	return &Adapter{config: config}
}

func (a *Adapter) Name() string { return a.config.Name }

func (a *Adapter) Capabilities() experiment.AdapterCapabilities {
	return experiment.AdapterCapabilities{
		CrashRecovery: false, DeliveryEquivalent: false,
		BacklogKinds: []string{"ready", "deferred", "retry", "dlq"}, ControllerTelemetry: true, RedisTelemetry: true,
		Tuning: map[string]string{
			"concurrency": strconv.Itoa(a.config.Concurrency), "min_concurrency": strconv.Itoa(a.config.MinConcurrency),
			"max_concurrency": strconv.Itoa(a.config.MaxConcurrency), "admission_max_pending": strconv.FormatInt(a.config.AdmissionMaxPending, 10),
			"dependency_budget_enabled":  strconv.FormatBool(!a.config.DisableDependencyBudget),
			"dependency_budget_capacity": strconv.Itoa(a.config.DependencyBudgetCapacity),
			"fairness_enabled":           strconv.FormatBool(!a.config.DisableFairness),
			"adaptive_enabled":           strconv.FormatBool(!a.config.DisableAdaptive),
			"scheduler_period":           a.config.SchedulerPeriod.String(), "reserve_timeout": time.Second.String(),
		},
		SemanticLimitations: []string{"in-process worker shutdown is graceful; process-crash cells are excluded rather than mislabeled equivalent"},
	}
}

func (a *Adapter) Start(ctx context.Context, runtime experiment.AdapterRuntime) error {
	if a.config.Client == nil {
		return errors.New("taskforge adapter requires Redis client")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runtime = runtime
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	budgetCapacity := a.config.DependencyBudgetCapacity
	if budgetCapacity <= 0 {
		budgetCapacity = runtime.Trace.Profile.Downstream.Capacity
	}
	options := tfredis.Options{
		Client: a.config.Client, LeaseTTL: a.config.LeaseTTL, ReserveTimeout: time.Second, Logger: logger,
	}
	if !a.config.DisableFairness {
		rules := make([]tfredis.FairnessRule, 0, len(runtime.Trace.Profile.Tenants))
		for _, tenant := range runtime.Trace.Profile.Tenants {
			rules = append(rules, tfredis.FairnessRule{Name: tenant.Name, Keys: []string{tenant.Name}, Weight: max(1, int(tenant.EntitlementWeight*1000))})
		}
		fairness, err := tfredis.NewFairnessPolicy(tfredis.FairnessRule{}, rules)
		if err != nil {
			return err
		}
		options.FairnessPolicies = map[string]*tfredis.FairnessPolicy{"default": fairness}
	}
	if !a.config.DisableDependencyBudget {
		options.DependencyBudgets = map[string]int{"downstream": budgetCapacity}
	}
	if a.config.AdmissionMaxPending > 0 {
		options.AdmissionPolicies = map[string]tfredis.AdmissionPolicy{"default": {
			Mode: tfredis.AdmissionModeDefer, MaxPending: a.config.AdmissionMaxPending,
			MaxRetryBacklog: a.config.AdmissionMaxPending, DeferInterval: a.config.SchedulerPeriod,
		}}
	}
	a.broker = tfredis.New(options)
	fence := taskforge.LeadershipFence{Owner: "neutral-benchmark", Epoch: 1, Token: "neutral-benchmark|1"}
	if err := a.config.Client.Set(ctx, "taskforge:v2:scheduler:leader", fence.Token, time.Hour).Err(); err != nil {
		return err
	}
	if err := a.config.Client.Set(ctx, "taskforge:v2:scheduler:leader:epoch", fence.Epoch, 0).Err(); err != nil {
		return err
	}

	handler := taskforge.HandlerFunc(func(ctx context.Context, task taskforge.Task) error {
		var arrival experiment.TraceArrival
		if err := json.Unmarshal(task.Payload, &arrival); err != nil {
			return taskforge.Decode(err)
		}
		attempt := task.Attempt + 1
		runtime.Recorder.TaskStarted(arrival, attempt, time.Now().UTC())
		err := runtime.Downstream.Call(ctx, arrival, attempt)
		if err == nil {
			runtime.Recorder.TaskFinished(arrival, attempt, time.Now().UTC(), "completed")
			return nil
		}
		outcome := "retry"
		if attempt >= runtime.Trace.Profile.MaxAttempts {
			outcome = "dlq"
		}
		runtime.Recorder.TaskFinished(arrival, attempt, time.Now().UTC(), outcome)
		return taskforge.Retryable(err)
	})
	workerOptions := worker.Options{
		Broker: a.broker, Handler: handler, Logger: logger, PoolName: "neutral", Queue: "default", ConsumerID: "neutral-worker",
		LeaseTTL: a.config.LeaseTTL, Concurrency: a.config.Concurrency, Prefetch: a.config.MaxConcurrency,
		RetryPolicy: taskforge.RetryPolicy{MaxDeliveries: runtime.Trace.Profile.MaxAttempts, InitialBackoff: runtime.Trace.Profile.RetryBackoff, MaxBackoff: runtime.Trace.Profile.RetryBackoff, Multiplier: 1},
	}
	if !a.config.DisableAdaptive {
		workerOptions.AdaptiveStore = a.broker
		workerOptions.Adaptive = worker.AdaptiveConfig{
			Enabled: true, MinConcurrency: a.config.MinConcurrency, MaxConcurrency: a.config.MaxConcurrency,
			ControlPeriod: 100 * time.Millisecond, Cooldown: 300 * time.Millisecond, ScaleUpStep: 1, ScaleDownStep: 1,
			LatencyThreshold: runtime.Trace.Profile.SLO / 4, ErrorRateThreshold: .15, BacklogThreshold: int64(a.config.Concurrency), HealthyWindowsRequired: 2,
		}
	}
	if !a.config.DisableDependencyBudget {
		workerOptions.BudgetManager = a.broker
		workerOptions.TaskBudgets = map[string]worker.TaskBudget{"neutral.task": {Budget: "downstream", Tokens: 1}}
	}
	w, err := worker.New(workerOptions)
	if err != nil {
		return err
	}
	workerCtx, cancelWorker := context.WithCancel(ctx)
	a.workerCancel = cancelWorker
	a.workerDone = make(chan error, 1)
	go func() { a.workerDone <- w.Run(workerCtx) }()
	if a.needsScheduler(runtime.Trace) {
		schedulerCtx, cancelScheduler := context.WithCancel(ctx)
		a.schedulerCancel = cancelScheduler
		a.schedulerDone = make(chan struct{})
		go func() {
			defer close(a.schedulerDone)
			ticker := time.NewTicker(a.config.SchedulerPeriod)
			defer ticker.Stop()
			for {
				select {
				case <-schedulerCtx.Done():
					return
				case now := <-ticker.C:
					_, _ = a.broker.MoveDue(schedulerCtx, fence, now.UTC(), 1024)
				}
			}
		}()
	}
	return nil
}

func (a *Adapter) needsScheduler(trace experiment.OpenLoopTrace) bool {
	if a.config.AdmissionMaxPending > 0 || trace.Profile.MaxAttempts > 1 {
		return true
	}
	for _, arrival := range trace.Arrivals {
		if arrival.NotBefore.After(arrival.At) {
			return true
		}
	}
	return false
}

func (a *Adapter) Enqueue(ctx context.Context, arrival experiment.TraceArrival) (experiment.EnqueueResult, error) {
	payload, err := json.Marshal(arrival)
	if err != nil {
		return experiment.EnqueueResult{}, err
	}
	scheduled := a.runtime.RunEpoch.Add(arrival.At.Sub(a.runtime.Trace.StartAt))
	task := taskforge.Task{ID: arrival.ID, Name: "neutral.task", Queue: "default", FairnessKey: arrival.Tenant, Payload: payload, CreatedAt: scheduled, MaxDeliveries: a.runtime.Trace.Profile.MaxAttempts}
	if arrival.NotBefore.After(arrival.At) {
		eta := a.runtime.RunEpoch.Add(arrival.NotBefore.Sub(a.runtime.Trace.StartAt))
		task.ETA = &eta
	}
	result, err := a.broker.Publish(ctx, task, taskforge.PublishOptions{Source: taskforge.PublishSourceNew})
	if err != nil {
		return experiment.EnqueueResult{Disposition: experiment.EnqueueRejected}, err
	}
	return experiment.EnqueueResult{Disposition: experiment.EnqueueDisposition(result.Decision), Reason: result.Reason}, nil
}

func (*Adapter) ApplyFault(context.Context, experiment.TraceFault) error {
	return errors.New("worker crash/recovery is unsupported by construction")
}

func (a *Adapter) Snapshot(ctx context.Context, at time.Duration) (experiment.TelemetryPoint, error) {
	queue, err := a.broker.QueueMetricsSnapshot(ctx, "default")
	if err != nil {
		return experiment.TelemetryPoint{}, err
	}
	admission, err := a.broker.AdmissionStatusSnapshot(ctx, "default", time.Now().UTC())
	if err != nil {
		return experiment.TelemetryPoint{}, err
	}
	dlq, err := a.broker.DeadLetterQueueSize(ctx, "default")
	if err != nil {
		return experiment.TelemetryPoint{}, err
	}
	lagSeconds, err := a.broker.SchedulerLag(ctx, time.Now().UTC(), "default")
	if err != nil {
		return experiment.TelemetryPoint{}, err
	}
	controllerPoint := experiment.ControllerPoint{At: at, EffectiveConcurrency: float64(a.config.Concurrency), Decision: "static", Reason: "adaptive_disabled"}
	if !a.config.DisableAdaptive {
		controller, err := a.broker.AdaptiveStatusSnapshot(ctx, "neutral")
		if err != nil {
			return experiment.TelemetryPoint{}, err
		}
		controllerPoint = experiment.ControllerPoint{At: at, EffectiveConcurrency: controller.EffectiveConcurrency, Decision: controller.LastAdjustmentAction, Reason: controller.LastAdjustmentReason}
	}
	delayed, _ := a.config.Client.ZCard(ctx, "taskforge:v2:delayed:queue:default").Result()
	retry := int64(admission.RetryBacklog)
	return experiment.TelemetryPoint{
		At:         at,
		Backlog:    experiment.BacklogPoint{At: at, Ready: int64(queue.Depth), Deferred: max(delayed-retry, 0), Retry: retry, DLQ: int64(dlq)},
		Controller: controllerPoint,
		Redis:      adapters.RedisPoint(ctx, a.config.Client, at), SchedulerLag: time.Duration(lagSeconds * float64(time.Second)),
	}, nil
}

func (a *Adapter) Stop(ctx context.Context) error {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return nil
	}
	a.stopped = true
	workerCancel, schedulerCancel := a.workerCancel, a.schedulerCancel
	workerDone, schedulerDone := a.workerDone, a.schedulerDone
	a.mu.Unlock()
	if schedulerCancel != nil {
		schedulerCancel()
	}
	if workerCancel != nil {
		workerCancel()
	}
	var result error
	if schedulerDone != nil {
		select {
		case <-schedulerDone:
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
		}
	}
	if workerDone != nil {
		select {
		case err := <-workerDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				result = errors.Join(result, fmt.Errorf("worker: %w", err))
			}
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
		}
	}
	return result
}
