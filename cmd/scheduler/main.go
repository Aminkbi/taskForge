package main

import (
	"context"
	"log/slog"

	"github.com/aminkbi/taskforge/internal/app/bootstrap"
	schedulerapp "github.com/aminkbi/taskforge/internal/app/scheduler"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/observability"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	bootstrap.Run("taskforge-scheduler", version, commit, func(ctx context.Context, cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) error {
		app, err := schedulerapp.New(cfg, logger, metrics)
		if err != nil {
			logger.Error("configure scheduler", "error", err)
			return err
		}
		if err := app.Run(ctx); err != nil {
			logger.Error("scheduler exited with error", "error", err)
			return err
		}
		return nil
	})
}
