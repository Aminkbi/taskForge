package main

import (
	"context"
	"log/slog"

	apiapp "github.com/aminkbi/taskforge/internal/app/api"
	"github.com/aminkbi/taskforge/internal/app/bootstrap"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/observability"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	bootstrap.Run("taskforge-api", version, commit, func(ctx context.Context, cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) error {
		app, err := apiapp.New(cfg, logger, metrics)
		if err != nil {
			logger.Error("configure API", "error", err)
			return err
		}
		if err := app.Run(ctx); err != nil {
			logger.Error("api exited with error", "error", err)
			return err
		}
		return nil
	})
}
