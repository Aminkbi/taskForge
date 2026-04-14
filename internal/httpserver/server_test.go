package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServerReadinessRequiresStartup(t *testing.T) {
	t.Parallel()

	server := New(":0", nil, http.NotFoundHandler(), nil, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	readinessHandler(server).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}

	var payload readinessResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.Status != "not_ready" {
		t.Fatalf("status = %q, want %q", payload.Status, "not_ready")
	}
}

func TestServerReadinessIncludesStandbySchedulerState(t *testing.T) {
	t.Parallel()

	server := New(":0", nil, http.NotFoundHandler(), map[string]CheckFunc{
		"scheduler_leadership": func(context.Context) CheckResult {
			return CheckResult{
				Ready:     true,
				Status:    "ready",
				Detail:    "standby",
				Leader:    false,
				UpdatedAt: time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC),
			}
		},
	}, nil)
	server.SetReady(true)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	readinessHandler(server).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var payload readinessResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	check, ok := payload.Checks["scheduler_leadership"]
	if !ok {
		t.Fatalf("scheduler_leadership check missing: %+v", payload.Checks)
	}
	if payload.Status != "ready" || check.Detail != "standby" || check.Leader {
		t.Fatalf("readiness payload = %+v, want ready standby non-leader", payload)
	}
}
