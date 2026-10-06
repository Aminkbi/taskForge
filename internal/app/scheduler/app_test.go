package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
)

func TestNewAllowsEmptyWorkerPools(t *testing.T) {
	t.Parallel()

	app, err := New(config.Config{
		RedisAddr: ":6379",
		Control: taskforge.Config{Scheduler: taskforge.SchedulerConfig{
			LockTTL: 15 * time.Second, RenewInterval: 5 * time.Second,
		}},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if app == nil {
		t.Fatal("New() returned nil")
	}
}

type stubLeadershipMetricsProvider struct{}

func (stubLeadershipMetricsProvider) SchedulerLeadershipSnapshot(context.Context) (taskforge.SchedulerLeadershipSnapshot, error) {
	return taskforge.SchedulerLeadershipSnapshot{}, nil
}

func TestNewRejectsLeadershipMetricsRegistrationConflict(t *testing.T) {
	t.Parallel()

	metrics := observability.NewMetrics()
	if err := metrics.RegisterSchedulerLeadershipCollector(stubLeadershipMetricsProvider{}); err != nil {
		t.Fatal(err)
	}
	app, err := New(config.Config{
		RedisAddr: ":6379",
		Control: taskforge.Config{Scheduler: taskforge.SchedulerConfig{
			LockTTL: 15 * time.Second, RenewInterval: 5 * time.Second,
		}},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics)
	if app != nil {
		_ = app.client.Close()
		t.Fatal("New() returned an app despite a collector registration conflict")
	}
	var duplicate prometheus.AlreadyRegisteredError
	if !errors.As(err, &duplicate) || !strings.Contains(err.Error(), "register scheduler leadership metrics") {
		t.Fatalf("New() error = %v, want scheduler leadership registration conflict", err)
	}
}

func TestLeadershipEndpointIsNotExposedWithoutAuthentication(t *testing.T) {
	t.Parallel()

	app, err := New(config.Config{
		RedisAddr: ":6379",
		Control: taskforge.Config{Scheduler: taskforge.SchedulerConfig{
			LockTTL: 15 * time.Second, RenewInterval: 5 * time.Second,
		}},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	recorder := httptest.NewRecorder()
	app.server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/admin/leadership", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}
