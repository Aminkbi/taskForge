package worker

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/httpserver"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
	"github.com/aminkbi/taskforge/internal/tasks"
)

type App struct {
	server *httpserver.Server
	worker *runtimepkg.Worker
}

func New(cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) *App {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	b := brokerredis.New(client, logger.With("component", "brokerredis"), cfg.LeaseTTL, metrics)
	server := httpserver.New(cfg.HTTPAddr, logger.With("component", "httpserver"), metrics.Handler(), nil)
	dispatcher := dlq.NewService(client, b, logger.With("component", "dlq"))

	worker := &runtimepkg.Worker{
		Broker:     b,
		DeadLetter: dispatcher,
		Handler: runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error {
			return nil
		}),
		Logger:       logger.With("component", "worker-runtime"),
		Metrics:      metrics,
		Clock:        clock.RealClock{},
		RetryPolicy:  tasks.DefaultRetryPolicy(3),
		Queue:        "default",
		ConsumerID:   cfg.ServiceName,
		PollInterval: cfg.PollInterval,
		LeaseTTL:     cfg.LeaseTTL,
		Concurrency:  cfg.WorkerCount,
	}

	return &App{
		server: server,
		worker: worker,
	}
}

func (a *App) Run(ctx context.Context) error {
	a.server.SetReady(true)

	errCh := make(chan error, 2)
	go func() {
		errCh <- a.server.Run(ctx)
	}()
	go func() {
		errCh <- a.worker.Run(ctx)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}
