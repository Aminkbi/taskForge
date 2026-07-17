// Package asynq implements the Asynq side of the neutral open-loop protocol.
// TaskForge-specific controls and keys do not enter this adapter.
package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	basynq "github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/experiment"
	"github.com/aminkbi/taskforge/internal/experiment/adapters"
)

type Config struct {
	Redis       basynq.RedisClientOpt
	Client      *redis.Client
	Concurrency int
	PollPeriod  time.Duration
}

type Adapter struct {
	config    Config
	mu        sync.Mutex
	runtime   experiment.AdapterRuntime
	server    *basynq.Server
	client    *basynq.Client
	inspector *basynq.Inspector
	stopped   bool
}

func New(config Config) *Adapter {
	if config.Concurrency <= 0 {
		config.Concurrency = 16
	}
	if config.PollPeriod <= 0 {
		config.PollPeriod = 10 * time.Millisecond
	}
	return &Adapter{config: config}
}

func (*Adapter) Name() string { return "asynq" }
func (a *Adapter) Capabilities() experiment.AdapterCapabilities {
	return experiment.AdapterCapabilities{
		CrashRecovery: false, DeliveryEquivalent: false,
		BacklogKinds: []string{"ready", "scheduled", "retry", "archived"}, ControllerTelemetry: true, RedisTelemetry: true,
		Tuning:              map[string]string{"concurrency": strconv.Itoa(a.config.Concurrency), "task_check_period": a.config.PollPeriod.String(), "delayed_check_period": a.config.PollPeriod.String()},
		SemanticLimitations: []string{"archived is reported in the DLQ column but is not TaskForge's DLQ contract", "no tenant entitlement, admission, adaptive concurrency, or dependency budget", "in-process shutdown is graceful; process-crash cells are excluded"},
	}
}

func (a *Adapter) Start(_ context.Context, runtime experiment.AdapterRuntime) error {
	if a.config.Client == nil {
		return errors.New("asynq adapter requires Redis telemetry client")
	}
	a.runtime = runtime
	a.server = basynq.NewServer(a.config.Redis, basynq.Config{
		Concurrency: a.config.Concurrency, TaskCheckInterval: a.config.PollPeriod, DelayedTaskCheckInterval: a.config.PollPeriod,
		RetryDelayFunc:  func(int, error, *basynq.Task) time.Duration { return runtime.Trace.Profile.RetryBackoff },
		ShutdownTimeout: 30 * time.Second, LogLevel: basynq.ErrorLevel,
	})
	mux := basynq.NewServeMux()
	mux.HandleFunc("neutral.task", func(ctx context.Context, task *basynq.Task) error {
		var arrival experiment.TraceArrival
		if err := json.Unmarshal(task.Payload(), &arrival); err != nil {
			return err
		}
		retried, _ := basynq.GetRetryCount(ctx)
		attempt := retried + 1
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
		return err
	})
	a.client = basynq.NewClient(a.config.Redis)
	a.inspector = basynq.NewInspector(a.config.Redis)
	return a.server.Start(mux)
}

func (a *Adapter) Enqueue(ctx context.Context, arrival experiment.TraceArrival) (experiment.EnqueueResult, error) {
	payload, err := json.Marshal(arrival)
	if err != nil {
		return experiment.EnqueueResult{}, err
	}
	options := []basynq.Option{basynq.TaskID(arrival.ID), basynq.Queue("default"), basynq.MaxRetry(max(a.runtime.Trace.Profile.MaxAttempts-1, 0)), basynq.Retention(time.Second)}
	if arrival.NotBefore.After(arrival.At) {
		eta := a.runtime.RunEpoch.Add(arrival.NotBefore.Sub(a.runtime.Trace.StartAt))
		options = append(options, basynq.ProcessAt(eta))
	}
	_, err = a.client.EnqueueContext(ctx, basynq.NewTask("neutral.task", payload), options...)
	if err != nil {
		return experiment.EnqueueResult{Disposition: experiment.EnqueueRejected}, err
	}
	return experiment.EnqueueResult{Disposition: experiment.EnqueueAccepted}, nil
}

func (*Adapter) ApplyFault(context.Context, experiment.TraceFault) error {
	return errors.New("worker crash/recovery is unsupported by construction")
}

func (a *Adapter) Snapshot(ctx context.Context, at time.Duration) (experiment.TelemetryPoint, error) {
	info, err := a.inspector.GetQueueInfo("default")
	if err != nil {
		return experiment.TelemetryPoint{}, err
	}
	return experiment.TelemetryPoint{
		At:         at,
		Backlog:    experiment.BacklogPoint{At: at, Ready: int64(info.Pending), Deferred: int64(info.Scheduled), Retry: int64(info.Retry), DLQ: int64(info.Archived)},
		Controller: experiment.ControllerPoint{At: at, EffectiveConcurrency: float64(a.config.Concurrency), Decision: "static", Reason: "adapter tuning"},
		Redis:      adapters.RedisPoint(ctx, a.config.Client, at), SchedulerLag: info.Latency,
	}, nil
}

func (a *Adapter) Stop(ctx context.Context) error {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return nil
	}
	a.stopped = true
	server, client, inspector := a.server, a.client, a.inspector
	a.mu.Unlock()
	if server != nil {
		server.Shutdown()
	}
	if client != nil {
		_ = client.Close()
	}
	if inspector != nil {
		_ = inspector.Close()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}
