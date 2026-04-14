package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/examples/externalapi"
	"github.com/aminkbi/taskforge/internal/examples/shared"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/shutdown"
	"github.com/aminkbi/taskforge/internal/tasks"
)

func main() {
	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	cfg, err := config.Load("taskforge-example-external-api")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	shared.ApplyDefaultRedisDB(&cfg, 11)

	logger, err := logging.New(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build logger: %v\n", err)
		os.Exit(1)
	}

	shutdownTracing, err := observability.SetupTracing(ctx, observability.TraceConfig{
		Enabled:     cfg.OTELEnabled,
		ServiceName: cfg.ServiceName,
	}, logger)
	if err != nil {
		logger.Error("setup tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			logger.Error("shutdown tracing", "error", err)
		}
	}()

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer client.Close()

	metrics := observability.NewMetrics()
	leaseTTL := time.Second
	exampleBroker := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis"), leaseTTL, metrics, brokerredis.Options{
		ReserveTimeout: 25 * time.Millisecond,
	})
	if err := exampleBroker.Ping(ctx); err != nil {
		logger.Error("ping redis", "error", err)
		os.Exit(1)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		logger.Error("flush example redis db", "error", err)
		os.Exit(1)
	}

	fakeClient := externalapi.NewFakeClient(0)
	deadLetters := dlq.NewService(client, exampleBroker, logger.With("component", "dlq"))
	worker := &runtimepkg.Worker{
		Broker:      exampleBroker,
		DeadLetter:  deadLetters,
		Handler:     externalapi.Handler{Client: fakeClient, Logger: logger.With("component", "example-external-api-handler")},
		Logger:      logger.With("component", "worker-runtime"),
		Metrics:     metrics,
		Clock:       clock.RealClock{},
		RetryPolicy: tasks.DefaultRetryPolicy(2),
		PoolName:    "example-external-api",
		Queue:       "external",
		ConsumerID:  cfg.ServiceName,
		LeaseTTL:    leaseTTL,
		Concurrency: 1,
		Prefetch:    1,
	}

	elector := schedulerpkg.NewRedisLeaderElector(
		client,
		clock.RealClock{},
		logger.With("component", "scheduler-leader"),
		cfg.ServiceName+":example",
		200*time.Millisecond,
		50*time.Millisecond,
	)
	scheduler := schedulerpkg.New(
		exampleBroker,
		nil,
		elector,
		clock.RealClock{},
		logger.With("component", "scheduler-runtime"),
		20*time.Millisecond,
		50*time.Millisecond,
	)

	runCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	workerErrCh := make(chan error, 1)
	go func() {
		workerErrCh <- (&runtimepkg.Manager{Workers: []*runtimepkg.Worker{worker}}).Run(runCtx)
	}()
	schedulerErrCh := make(chan error, 1)
	go func() {
		schedulerErrCh <- scheduler.Run(runCtx)
	}()

	payload, err := json.Marshal(externalapi.SyncPayload{ResourceID: "tenant-42"})
	if err != nil {
		logger.Error("marshal external api payload", "error", err)
		os.Exit(1)
	}

	if err := exampleBroker.Publish(runCtx, broker.TaskMessage{
		ID:        "tenant-42",
		Name:      externalapi.TaskSyncExternalResource,
		Queue:     "external",
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
		Headers: map[string]string{
			tasks.HeaderRetryMaxDeliveries:  "2",
			tasks.HeaderRetryInitialBackoff: "50ms",
			tasks.HeaderRetryMaxBackoff:     "50ms",
			tasks.HeaderRetryMultiplier:     "1",
		},
	}); err != nil {
		logger.Error("publish external api task", "error", err)
		os.Exit(1)
	}

	var deadLetterEntry dlq.Entry
	if err := shared.WaitFor(runCtx, 25*time.Millisecond, func() (bool, error) {
		entries, err := deadLetters.List(runCtx, "external", 10)
		if err != nil {
			return false, err
		}
		if len(entries) == 0 {
			return false, nil
		}
		deadLetterEntry = entries[0]
		return true, nil
	}); err != nil {
		logger.Error("wait for dead-letter entry", "error", err)
		os.Exit(1)
	}

	fakeClient.SetAvailable(true)
	if err := deadLetters.Replay(runCtx, "external", deadLetterEntry.ID); err != nil {
		logger.Error("replay dead-letter entry", "error", err)
		os.Exit(1)
	}

	if err := shared.WaitFor(runCtx, 25*time.Millisecond, func() (bool, error) {
		return fakeClient.Synced("tenant-42"), nil
	}); err != nil {
		logger.Error("wait for replay success", "error", err)
		os.Exit(1)
	}

	cancel()
	if err := <-workerErrCh; err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
	if err := <-schedulerErrCh; err != nil {
		logger.Error("scheduler exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info(
		"external api example complete",
		"redis_db", cfg.RedisDB,
		"call_count", fakeClient.CallCount(),
		"replayed_entry_id", deadLetterEntry.ID,
	)
}
