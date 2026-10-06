// Package bootstrap contains the shared setup used by TaskForge sidecars.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
	"github.com/aminkbi/taskforge/internal/shutdown"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
)

// NewRedis creates the configured Redis client and TaskForge broker.
// The caller owns the returned client and must close it.
func NewRedis(cfg config.Config, logger *slog.Logger) (*redis.Client, *taskforgeredis.Broker, error) {
	options, err := cfg.RedisOptions(nil, logger.With("component", "redis"))
	if err != nil {
		return nil, nil, fmt.Errorf("configure Redis: %w", err)
	}
	client := taskforgeredis.NewClient(options)
	options.Client = client
	return client, taskforgeredis.New(options), nil
}

// Queues returns the configured worker queues in stable, duplicate-free order.
func Queues(cfg config.Config) []string {
	queues := make([]string, 0, len(cfg.Control.WorkerPools))
	for _, pool := range cfg.Control.WorkerPools {
		queues = append(queues, pool.Queue)
	}
	slices.Sort(queues)
	return slices.Compact(queues)
}

// RegisterCommonMetrics registers collectors shared by the API and scheduler.
func RegisterCommonMetrics(metrics *observability.Metrics, broker *taskforgeredis.Broker, queues []string) error {
	if err := metrics.RegisterQueueMetricsCollector(broker, queues); err != nil {
		return fmt.Errorf("register queue metrics: %w", err)
	}
	if err := metrics.RegisterFairnessMetricsCollector(broker, queues); err != nil {
		return fmt.Errorf("register fairness metrics: %w", err)
	}
	if err := metrics.RegisterAdmissionStatusCollector(broker, queues); err != nil {
		return fmt.Errorf("register admission metrics: %w", err)
	}
	if err := metrics.RegisterDependencyBudgetCollector(broker); err != nil {
		return fmt.Errorf("register dependency budget metrics: %w", err)
	}
	return nil
}

// RunFunc configures and runs a sidecar until its context is canceled.
type RunFunc func(context.Context, config.Config, *slog.Logger, *observability.Metrics) error

// Run loads sidecar configuration, initializes shared observability, and runs
// the role-specific application. Errors during setup are reported and exit
// with status 1, matching the sidecar command behavior.
func Run(name, version, commit string, run RunFunc) {
	if PrintVersion(name, version, commit) {
		return
	}

	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	cfg, err := config.Load(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

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

	if err := run(ctx, cfg, logger, observability.NewMetrics()); err != nil {
		os.Exit(1)
	}
}

// PrintVersion prints the sidecar version when requested on the command line.
func PrintVersion(name, version, commit string) bool {
	if len(os.Args) < 2 {
		return false
	}
	switch os.Args[1] {
	case "version", "--version", "-version":
		fmt.Fprintf(os.Stdout, "%s %s (%s)\n", name, version, commit)
		return true
	default:
		return false
	}
}
