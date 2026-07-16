package scheduler

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/observability"
)

func TestNewAllowsEmptyWorkerPools(t *testing.T) {
	t.Parallel()

	app := New(config.Config{
		RedisAddr:              ":6379",
		SchedulerLockTTL:       15 * time.Second,
		SchedulerRenewInterval: 5 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics())
	if app == nil {
		t.Fatal("New() returned nil")
	}
}

func TestLeadershipEndpointIsNotExposedWithoutAuthentication(t *testing.T) {
	t.Parallel()

	app := New(config.Config{
		RedisAddr:              ":6379",
		SchedulerLockTTL:       15 * time.Second,
		SchedulerRenewInterval: 5 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics())
	recorder := httptest.NewRecorder()
	app.server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/admin/leadership", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}
