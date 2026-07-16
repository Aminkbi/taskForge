package httpserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultReadHeaderTimeout = 5 * time.Second
	DefaultReadTimeout       = 15 * time.Second
	DefaultWriteTimeout      = 30 * time.Second
	DefaultIdleTimeout       = 60 * time.Second
	DefaultShutdownTimeout   = 10 * time.Second
	DefaultMaxBodyBytes      = int64(1 << 20)
	DefaultMaxHeaderBytes    = 16 << 10
)

type Config struct {
	Addr              string
	AuthToken         string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	MaxBodyBytes      int64
	MaxHeaderBytes    int
}

func (c Config) withDefaults() Config {
	if c.ReadHeaderTimeout <= 0 {
		c.ReadHeaderTimeout = DefaultReadHeaderTimeout
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = DefaultReadTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = DefaultMaxHeaderBytes
	}
	return c
}

type CheckResult struct {
	Ready     bool
	Status    string
	Detail    string
	Leader    bool
	UpdatedAt time.Time
}

type CheckFunc func(context.Context) CheckResult

type Server struct {
	addr            string
	logger          *slog.Logger
	server          *http.Server
	shutdownTimeout time.Duration
	ready           atomic.Bool
	mu              sync.RWMutex
	checks          map[string]CheckFunc
}

// New builds a server with a public, minimal health surface and a protected
// operator surface. Metrics and all routes registered by registerOperator are
// disabled when AuthToken is empty.
func New(cfg Config, logger *slog.Logger, metricsHandler http.Handler, checks map[string]CheckFunc, registerOperator func(*http.ServeMux)) *Server {
	cfg = cfg.withDefaults()
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	publicMux := http.NewServeMux()
	operatorMux := http.NewServeMux()
	s := &Server{
		addr:            cfg.Addr,
		logger:          logger,
		checks:          checks,
		shutdownTimeout: cfg.ShutdownTimeout,
	}

	publicMux.Handle("/healthz", ReadOnly(healthHandler()))
	publicMux.Handle("/readyz", ReadOnly(readinessHandler(s)))
	registerMetrics(operatorMux, metricsHandler)
	if registerOperator != nil {
		registerOperator(operatorMux)
	}
	publicMux.Handle("/", operatorAuth(cfg.AuthToken, operatorMux))

	handler := securityHeaders(limitBody(cfg.MaxBodyBytes, publicMux))
	s.server = &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}

	return s
}

// ReadOnly restricts an endpoint to retrieval methods. HEAD is allowed for
// probes and monitoring clients.
func ReadOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func operatorAuth(expectedToken string, next http.Handler) http.Handler {
	expectedTokenHash := sha256.Sum256([]byte(expectedToken))
	expectedUsernameHash := sha256.Sum256([]byte("taskforge"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expectedToken == "" {
			http.NotFound(w, r)
			return
		}

		providedToken, username, basic := requestCredential(r)
		validToken := constantTimeHashEqual(providedToken, expectedTokenHash)
		validUsername := 1
		if basic {
			validUsername = constantTimeHashEqual(username, expectedUsernameHash)
		}
		if validToken&validUsername != 1 {
			w.Header().Add("WWW-Authenticate", `Bearer realm="taskforge-operator"`)
			w.Header().Add("WWW-Authenticate", `Basic realm="taskforge-operator", charset="UTF-8"`)
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if basic && r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("WWW-Authenticate", `Bearer realm="taskforge-operator"`)
			writeError(w, http.StatusUnauthorized, "bearer authentication required")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func requestCredential(r *http.Request) (token, username string, basic bool) {
	authorization := r.Header.Get("Authorization")
	if scheme, value, ok := strings.Cut(authorization, " "); ok && strings.EqualFold(scheme, "Bearer") {
		return strings.TrimSpace(value), "", false
	}
	if username, password, ok := r.BasicAuth(); ok {
		return password, username, true
	}
	return "", "", false
}

func constantTimeHashEqual(provided string, expected [sha256.Size]byte) int {
	providedHash := sha256.Sum256([]byte(provided))
	return subtle.ConstantTimeCompare(providedHash[:], expected[:])
}

func limitBody(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength < 0 && len(r.TransferEncoding) > 0 {
			writeError(w, http.StatusLengthRequired, "content length required")
			return
		}
		if r.ContentLength > maxBytes {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("Cache-Control", "no-store")
		header.Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		header.Set("Permissions-Policy", "camera=(), geolocation=(), microphone=()")
		header.Set("Referrer-Policy", "no-referrer")
		header.Set("X-Content-Type-Options", "nosniff")
		header.Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
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

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.server.Handler.ServeHTTP(w, r)
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
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
