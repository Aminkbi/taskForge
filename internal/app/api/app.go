package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/httpserver"
	"github.com/aminkbi/taskforge/internal/observability"
)

type App struct {
	server *httpserver.Server
}

func New(cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) *App {
	server := httpserver.New(cfg.HTTPAddr, logger.With("component", "httpserver"), metrics.Handler(), func(mux *http.ServeMux) {
		mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"service":"taskforge-api","status":"ok"}`))
		})
		mux.HandleFunc("/v1/admin/ping", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`))
		})
	})

	return &App{server: server}
}

func (a *App) Run(ctx context.Context) error {
	a.server.SetReady(true)
	return a.server.Run(ctx)
}
