package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/httpserver"
	"github.com/aminkbi/taskforge/internal/observability"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
)

type App struct {
	server    *httpserver.Server
	scheduler *schedulerpkg.Scheduler
}

func New(cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) *App {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	b := brokerredis.New(client, logger.With("component", "brokerredis"), cfg.LeaseTTL, metrics)
	server := httpserver.New(cfg.HTTPAddr, logger.With("component", "httpserver"), metrics.Handler(), nil)
	store := schedulerpkg.NewRedisScheduleStateStore(client)
	elector := schedulerpkg.NewRedisLeaderElector(
		client,
		clock.RealClock{},
		logger.With("component", "scheduler-leader"),
		schedulerOwnerToken(cfg.ServiceName),
		cfg.SchedulerLockTTL,
		cfg.SchedulerRenewInterval,
	)
	recurring := schedulerpkg.NewRecurringService(
		b,
		store,
		cfg.RecurringSchedules,
		logger.With("component", "scheduler-recurring"),
	)

	return &App{
		server: server,
		scheduler: schedulerpkg.New(
			b,
			recurring,
			elector,
			clock.RealClock{},
			logger.With("component", "scheduler-runtime"),
			cfg.PollInterval,
			cfg.SchedulerRenewInterval,
		),
	}
}

func (a *App) Run(ctx context.Context) error {
	a.server.SetReady(true)

	errCh := make(chan error, 2)
	go func() {
		errCh <- a.server.Run(ctx)
	}()
	go func() {
		errCh <- a.scheduler.Run(ctx)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func schedulerOwnerToken(serviceName string) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s:%s:%d", serviceName, hostname, os.Getpid())
}
