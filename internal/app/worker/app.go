package worker

import (
	"context"
	"log/slog"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/healthcheck"
	"github.com/aminkbi/taskforge/internal/httpserver"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
)

type App struct {
	server *httpserver.Server
	worker *runtimepkg.Manager
}

func New(cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) *App {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	sharedBroker := brokerredis.New(client, logger.With("component", "brokerredis"), cfg.WorkerPools[0].LeaseTTL, metrics)
	dispatcher := dlq.NewService(client, sharedBroker, logger.With("component", "dlq"))

	globalLimiter := runtimepkg.NewTaskTypeLimiter(cfg.TaskTypeLimits)
	workers := make([]*runtimepkg.Worker, 0, len(cfg.WorkerPools))
	queues := make([]string, 0, len(cfg.WorkerPools))
	recoveryChecks := make(map[string]*healthcheck.Reporter, len(cfg.WorkerPools))
	for _, pool := range cfg.WorkerPools {
		poolBroker := brokerredis.New(client, logger.With("component", "brokerredis", "pool", pool.Name, "queue", pool.Queue), pool.LeaseTTL, metrics)
		queues = append(queues, pool.Queue)
		recoveryHealth := healthcheck.NewReporter("not_ready", "worker starting")
		recoveryChecks[pool.Name] = recoveryHealth
		workers = append(workers, &runtimepkg.Worker{
			Broker:            poolBroker,
			DeadLetter:        dispatcher,
			Handler:           runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error { return nil }),
			Logger:            logger.With("component", "worker-runtime", "pool", pool.Name, "queue", pool.Queue),
			Metrics:           metrics,
			Clock:             clock.RealClock{},
			RetryPolicy:       pool.RetryPolicy,
			PoolName:          pool.Name,
			Queue:             pool.Queue,
			ConsumerID:        cfg.ServiceName,
			LeaseTTL:          pool.LeaseTTL,
			Concurrency:       pool.Concurrency,
			Prefetch:          pool.Prefetch,
			RecoveryHealth:    recoveryHealth,
			GlobalTaskLimiter: globalLimiter,
			PoolTaskLimiter:   runtimepkg.NewTaskTypeLimiter(pool.TaskTypeLimits),
		})
	}
	_ = metrics.RegisterQueueMetricsCollector(sharedBroker, queues)
	_ = metrics.RegisterDeadLetterMetricsCollector(sharedBroker, queues)
	server := httpserver.New(cfg.HTTPAddr, logger.With("component", "httpserver"), metrics.Handler(), map[string]httpserver.CheckFunc{
		"redis": func(ctx context.Context) httpserver.CheckResult {
			if err := sharedBroker.Ping(ctx); err != nil {
				return httpserver.CheckResult{
					Status: "failed",
					Detail: err.Error(),
				}
			}
			return httpserver.CheckResult{
				Ready:  true,
				Status: "ready",
				Detail: "redis reachable",
			}
		},
		"recovery_loop": func(context.Context) httpserver.CheckResult {
			unhealthy := make([]string, 0)
			for poolName, reporter := range recoveryChecks {
				snapshot := reporter.Snapshot()
				if snapshot.Ready {
					continue
				}
				status := snapshot.Status
				if snapshot.Detail != "" {
					status = status + ":" + snapshot.Detail
				}
				unhealthy = append(unhealthy, poolName+"="+status)
			}
			if len(unhealthy) > 0 {
				return httpserver.CheckResult{
					Status: "failed",
					Detail: strings.Join(unhealthy, ","),
				}
			}
			return httpserver.CheckResult{
				Ready:  true,
				Status: "ready",
				Detail: "worker reserve and reclaim loops healthy",
			}
		},
	}, nil)

	return &App{
		server: server,
		worker: &runtimepkg.Manager{Workers: workers},
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
