package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

type Server struct {
	addr   string
	logger *slog.Logger
	server *http.Server
	ready  atomic.Bool
}

func New(addr string, logger *slog.Logger, metricsHandler http.Handler, registerExtra func(*http.ServeMux)) *Server {
	mux := http.NewServeMux()
	s := &Server{
		addr:   addr,
		logger: logger,
	}

	mux.Handle("/healthz", healthHandler())
	mux.Handle("/readyz", readinessHandler(func() bool {
		return s.ready.Load()
	}))
	registerMetrics(mux, metricsHandler)
	if registerExtra != nil {
		registerExtra(mux)
	}

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errCh <- s.Shutdown(shutdownCtx)
	}()

	s.logger.Info("admin server listening", "addr", s.addr)
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
