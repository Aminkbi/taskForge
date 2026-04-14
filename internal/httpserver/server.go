package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type CheckResult struct {
	Ready     bool
	Status    string
	Detail    string
	Leader    bool
	UpdatedAt time.Time
}

type CheckFunc func(context.Context) CheckResult

type Server struct {
	addr   string
	logger *slog.Logger
	server *http.Server
	ready  atomic.Bool
	mu     sync.RWMutex
	checks map[string]CheckFunc
}

func New(addr string, logger *slog.Logger, metricsHandler http.Handler, checks map[string]CheckFunc, registerExtra func(*http.ServeMux)) *Server {
	mux := http.NewServeMux()
	s := &Server{
		addr:   addr,
		logger: logger,
		checks: checks,
	}

	mux.Handle("/healthz", healthHandler())
	mux.Handle("/readyz", readinessHandler(s))
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

func (s *Server) SetChecks(checks map[string]CheckFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks = checks
}

func (s *Server) Evaluate(ctx context.Context) readinessResponse {
	checks := map[string]readinessCheck{
		"startup": {
			Status: map[bool]string{true: "ready", false: "not_ready"}[s.ready.Load()],
		},
	}

	overallReady := s.ready.Load()
	if !overallReady {
		checks["startup"] = readinessCheck{
			Status: "not_ready",
			Detail: "service startup not complete",
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for name, check := range s.checks {
		if check == nil {
			continue
		}
		result := check(ctx)
		status := result.Status
		if status == "" {
			if result.Ready {
				status = "ready"
			} else {
				status = "not_ready"
			}
		}

		item := readinessCheck{
			Status: status,
			Detail: result.Detail,
			Leader: result.Leader,
		}
		if !result.UpdatedAt.IsZero() {
			item.Updated = result.UpdatedAt.UTC().Format(time.RFC3339Nano)
		}
		checks[name] = item

		if !result.Ready {
			overallReady = false
		}
	}

	status := "ready"
	if !overallReady {
		status = "not_ready"
	}

	return readinessResponse{
		Status: status,
		Checks: checks,
	}
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
