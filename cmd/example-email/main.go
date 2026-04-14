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
	"github.com/aminkbi/taskforge/internal/examples/email"
	"github.com/aminkbi/taskforge/internal/examples/shared"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
	"github.com/aminkbi/taskforge/internal/shutdown"
	"github.com/aminkbi/taskforge/internal/tasks"
)

func main() {
	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	cfg, err := config.Load("taskforge-example-email")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	shared.ApplyDefaultRedisDB(&cfg, 12)

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
	exampleBroker := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis"), 5*time.Second, metrics, brokerredis.Options{
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

	store := email.NewIdempotencyStore()
	mailer := &email.CaptureMailer{}
	worker := &runtimepkg.Worker{
		Broker:      exampleBroker,
		Handler:     email.Handler{Mailer: mailer, Store: store, Logger: logger.With("component", "example-email-handler")},
		Logger:      logger.With("component", "worker-runtime"),
		Metrics:     metrics,
		Clock:       clock.RealClock{},
		RetryPolicy: tasks.DefaultRetryPolicy(1),
		PoolName:    "example-email",
		Queue:       "email",
		ConsumerID:  cfg.ServiceName,
		LeaseTTL:    5 * time.Second,
		Concurrency: 1,
		Prefetch:    1,
	}

	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	manager := &runtimepkg.Manager{Workers: []*runtimepkg.Worker{worker}}
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(runCtx)
	}()

	payload, err := json.Marshal(email.SendEmailPayload{
		Recipient: "billing@example.com",
		Subject:   "invoice ready",
		Body:      "Your invoice is ready for download.",
	})
	if err != nil {
		logger.Error("marshal email payload", "error", err)
		os.Exit(1)
	}

	message := broker.TaskMessage{
		ID:             "invoice-42",
		IdempotencyKey: "email:billing@example.com:invoice-42",
		Name:           email.TaskSendEmail,
		Queue:          "email",
		Payload:        payload,
		CreatedAt:      time.Now().UTC(),
	}
	if err := exampleBroker.Publish(runCtx, message); err != nil {
		logger.Error("publish email task", "error", err)
		os.Exit(1)
	}
	if err := exampleBroker.Publish(runCtx, message); err != nil {
		logger.Error("publish duplicate email task", "error", err)
		os.Exit(1)
	}

	if err := shared.WaitFor(runCtx, 25*time.Millisecond, func() (bool, error) {
		snapshot, err := exampleBroker.QueueMetricsSnapshot(runCtx, "email")
		if err != nil {
			return false, err
		}
		return snapshot.Depth == 0 && snapshot.Reserved == 0 && mailer.Count() == 1, nil
	}); err != nil {
		logger.Error("wait for email example completion", "error", err)
		os.Exit(1)
	}

	cancel()
	if err := <-errCh; err != nil {
		logger.Error("worker manager exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info(
		"email example complete",
		"redis_db", cfg.RedisDB,
		"sent_count", mailer.Count(),
		"idempotency_status", store.Status(message.IdempotencyKey),
	)
}
