package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/examples/media"
	"github.com/aminkbi/taskforge/internal/examples/shared"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
	"github.com/aminkbi/taskforge/internal/shutdown"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
	runtimepkg "github.com/aminkbi/taskforge/worker"
)

func main() {
	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	cfg, err := config.Load("taskforge-example-media")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	shared.ApplyDefaultRedisDB(&cfg, 13)

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

	leaseTTL := 200 * time.Millisecond
	exampleBroker := taskforgeredis.New(taskforgeredis.Options{
		Client: client, Logger: logger.With("component", "redis"), LeaseTTL: leaseTTL,
		ReserveTimeout: 25 * time.Millisecond,
	})
	observerBroker := taskforgeredis.New(taskforgeredis.Options{
		Client: client, Logger: logger.With("component", "observer-redis"), LeaseTTL: leaseTTL,
		ReserveTimeout: 25 * time.Millisecond,
	})
	if err := exampleBroker.Ping(ctx); err != nil {
		logger.Error("ping redis", "error", err)
		os.Exit(1)
	}
	recorder := media.NewRecorder()
	worker, err := runtimepkg.New(runtimepkg.Options{
		Broker:      exampleBroker,
		Handler:     media.Handler{StepDuration: 150 * time.Millisecond, Recorder: recorder, Logger: logger.With("component", "example-media-handler")},
		Logger:      logger.With("component", "worker-runtime"),
		RetryPolicy: taskforge.DefaultRetryPolicy(1),
		PoolName:    "example-media",
		Queue:       "media",
		ConsumerID:  cfg.ServiceName,
		LeaseTTL:    leaseTTL,
		Concurrency: 1,
		Prefetch:    1,
	})
	if err != nil {
		logger.Error("build worker", "error", err)
		os.Exit(1)
	}

	runCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()

	manager := &runtimepkg.Manager{
		Workers:         []*runtimepkg.Worker{worker},
		ShutdownTimeout: cfg.ShutdownTimeout,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(runCtx)
	}()

	payload, err := json.Marshal(media.ProcessMediaPayload{
		AssetID: "asset-42",
		Steps:   5,
	})
	if err != nil {
		logger.Error("marshal media payload", "error", err)
		os.Exit(1)
	}

	if _, err := exampleBroker.Publish(runCtx, taskforge.Task{
		ID:        "asset-42",
		Name:      media.TaskProcessMedia,
		Queue:     "media",
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	}, taskforge.PublishOptions{Source: taskforge.PublishSourceNew}); err != nil {
		logger.Error("publish media task", "error", err)
		os.Exit(1)
	}

	time.Sleep(350 * time.Millisecond)
	if _, err := observerBroker.Reserve(runCtx, "media", "observer"); !errors.Is(err, taskforge.ErrNoTask) {
		logger.Error("observer unexpectedly reclaimed media task", "error", err)
		os.Exit(1)
	}

	if err := shared.WaitFor(runCtx, 25*time.Millisecond, func() (bool, error) {
		return recorder.Completed("asset-42"), nil
	}); err != nil {
		logger.Error("wait for media completion", "error", err)
		os.Exit(1)
	}

	cancel()
	if err := <-errCh; err != nil {
		logger.Error("worker manager exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info(
		"media example complete",
		"redis_db", cfg.RedisDB,
		"lease_ttl", leaseTTL,
		"steps", recorder.Steps("asset-42"),
	)
}
