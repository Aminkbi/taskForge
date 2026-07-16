package api

import (
	"context"
	"encoding/json"
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
)

type stubAdmissionProvider struct{}

func (stubAdmissionProvider) AdmissionStatusSnapshot(context.Context, string, time.Time) (taskforge.AdmissionStatusSnapshot, error) {
	return taskforge.AdmissionStatusSnapshot{
		Queue:              "critical",
		Mode:               "defer",
		State:              "degraded",
		Reason:             "queue_pending_cap",
		QueuePending:       7,
		FairnessKeyPending: 2,
		OldestReadyAge:     3,
		RetryBacklog:       1,
		DeadLetterSize:     0,
		DeferInterval:      5 * time.Second,
		UpdatedAt:          time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC),
	}, nil
}

type stubAdaptiveProvider struct{}

func (stubAdaptiveProvider) AdaptiveStatusSnapshot(context.Context, string) (taskforge.AdaptivePoolSnapshot, error) {
	return taskforge.AdaptivePoolSnapshot{
		Pool:                  "critical",
		Queue:                 "critical",
		AdaptiveEnabled:       true,
		ConfiguredConcurrency: 2,
		EffectiveConcurrency:  1,
		MinConcurrency:        1,
		MaxConcurrency:        4,
		AvgLatencySeconds:     1.5,
		ErrorRate:             0.2,
		BudgetBlocked:         3,
		Backlog:               8,
		HealthyWindows:        0,
		LastAdjustmentAction:  "scale_down",
		LastAdjustmentReason:  "latency",
		LastAdjustedAt:        time.Date(2026, 4, 16, 10, 5, 0, 0, time.UTC),
	}, nil
}

type stubBudgetUsageProvider struct{}

func (stubBudgetUsageProvider) DependencyBudgetUsageSnapshots(context.Context) ([]taskforge.DependencyBudgetUsageSnapshot, error) {
	return []taskforge.DependencyBudgetUsageSnapshot{
		{Budget: "downstream", Capacity: 5, InUse: 2},
	}, nil
}

type stubWorkerLifecycleProvider struct{}

func (stubWorkerLifecycleProvider) WorkerLifecycleSnapshots(context.Context) ([]taskforge.WorkerLifecycleSnapshot, error) {
	return []taskforge.WorkerLifecycleSnapshot{
		{
			WorkerID:            "worker-a",
			Pool:                "critical",
			Queue:               "critical",
			State:               "draining",
			Pending:             1,
			Running:             2,
			DrainStartedAt:      time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC),
			DrainDeadline:       time.Date(2026, 4, 21, 10, 0, 10, 0, time.UTC),
			LastShutdownOutcome: "forced_timeout",
			AbandonedDeliveries: 3,
			DrainLeaseLosses:    1,
			UpdatedAt:           time.Date(2026, 4, 21, 10, 0, 2, 0, time.UTC),
		},
	}, nil
}

type stubTaskStateStore struct {
	record taskforge.TaskRecord
	err    error
}

func (s stubTaskStateStore) RecordQueued(context.Context, taskforge.Task) error {
	return nil
}

func (s stubTaskStateStore) RecordDelivery(context.Context, taskforge.Delivery, taskforge.State, []byte) error {
	return nil
}

func (s stubTaskStateStore) Get(context.Context, string) (taskforge.TaskRecord, error) {
	return s.record, s.err
}

func TestAdmissionHandlerReturnsQueueSnapshots(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/admin/admission", nil)

	admissionHandler(stubAdmissionProvider{}, []string{"critical"}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var payload struct {
		Queues []struct {
			Queue   string             `json:"queue"`
			Mode    string             `json:"mode"`
			State   string             `json:"state"`
			Reason  string             `json:"reason"`
			Signals map[string]float64 `json:"signals"`
		} `json:"queues"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if len(payload.Queues) != 1 {
		t.Fatalf("queue count = %d, want 1", len(payload.Queues))
	}
	got := payload.Queues[0]
	if got.Queue != "critical" || got.Mode != "defer" || got.State != "degraded" || got.Reason != "queue_pending_cap" {
		t.Fatalf("queue payload = %+v, want critical/defer/degraded/queue_pending_cap", got)
	}
	if got.Signals["queue_pending"] != 7 {
		t.Fatalf("queue_pending = %v, want 7", got.Signals["queue_pending"])
	}
}

func TestAdaptiveHandlerReturnsPoolAndBudgetSnapshots(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/admin/adaptive", nil)

	adaptiveHandler(stubAdaptiveProvider{}, stubBudgetUsageProvider{}, []config.WorkerPoolConfig{
		{Name: "critical", Queue: "critical"},
	}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var payload struct {
		Pools []struct {
			Pool                 string             `json:"pool"`
			Queue                string             `json:"queue"`
			EffectiveConcurrency float64            `json:"effective_concurrency"`
			LastAdjustmentReason string             `json:"last_adjustment_reason"`
			Signals              map[string]float64 `json:"signals"`
		} `json:"pools"`
		Budgets []struct {
			Budget string  `json:"budget"`
			InUse  float64 `json:"in_use"`
		} `json:"budgets"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if len(payload.Pools) != 1 || payload.Pools[0].Pool != "critical" || payload.Pools[0].Queue != "critical" {
		t.Fatalf("pool payload = %+v, want critical pool snapshot", payload.Pools)
	}
	if payload.Pools[0].EffectiveConcurrency != 1 || payload.Pools[0].LastAdjustmentReason != "latency" {
		t.Fatalf("pool payload = %+v, want effective concurrency 1 and latency reason", payload.Pools[0])
	}
	if payload.Pools[0].Signals["budget_blocked"] != 3 {
		t.Fatalf("budget_blocked = %v, want 3", payload.Pools[0].Signals["budget_blocked"])
	}
	if len(payload.Budgets) != 1 || payload.Budgets[0].Budget != "downstream" || payload.Budgets[0].InUse != 2 {
		t.Fatalf("budget payload = %+v, want downstream in_use=2", payload.Budgets)
	}
}

func TestNewAllowsEmptyWorkerPools(t *testing.T) {
	t.Parallel()

	app, err := New(config.Config{
		RedisAddr: ":6379",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if app == nil {
		t.Fatal("New() returned nil")
	}
}

func TestAppSafeDefaultExposure(t *testing.T) {
	t.Parallel()

	app, err := New(config.Config{RedisAddr: ":6379"}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, tc := range []struct {
		path string
		want int
	}{
		{path: "/healthz", want: http.StatusOK},
		{path: "/metrics", want: http.StatusNotFound},
		{path: "/dashboard/", want: http.StatusNotFound},
		{path: "/v1/admin/ping", want: http.StatusNotFound},
		{path: "/v1/tasks/task-1", want: http.StatusNotFound},
	} {
		recorder := httptest.NewRecorder()
		app.server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if recorder.Code != tc.want {
			t.Errorf("GET %s: status = %d, want %d", tc.path, recorder.Code, tc.want)
		}
	}
}

func TestAppOperatorRouteRequiresAuthenticationAndRestrictsMethod(t *testing.T) {
	t.Parallel()

	const token = "0123456789abcdef0123456789abcdef"
	app, err := New(config.Config{RedisAddr: ":6379", HTTPAuthToken: token}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	unauthorized := httptest.NewRecorder()
	app.server.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/admin/ping", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/admin/ping", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	app.server.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("authenticated POST status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
}

func TestTaskLookupHandlerReturnsTaskRecord(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/task-1", nil)

	taskLookupHandler(stubTaskStateStore{record: taskforge.TaskRecord{
		TaskID:         "task-1",
		Name:           "demo.echo",
		Queue:          "default",
		State:          taskforge.StateSucceeded,
		UpdatedAt:      time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC),
		DeliveryCount:  2,
		LastDeliveryID: "delivery-2",
	}}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var payload struct {
		TaskID         string `json:"task_id"`
		State          string `json:"state"`
		DeliveryCount  int    `json:"delivery_count"`
		LastDeliveryID string `json:"last_delivery_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.TaskID != "task-1" || payload.State != string(taskforge.StateSucceeded) || payload.DeliveryCount != 2 || payload.LastDeliveryID != "delivery-2" {
		t.Fatalf("task lookup payload = %+v", payload)
	}
}

func TestTaskLookupHandlerReturnsNotFound(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/missing", nil)

	taskLookupHandler(stubTaskStateStore{err: taskforge.ErrTaskNotFound}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestTaskLookupHandlerRedactsPayloadAndError(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/task-1", nil)
	taskLookupHandler(stubTaskStateStore{record: taskforge.TaskRecord{
		TaskID:        "task-1",
		State:         taskforge.StateDeadLettered,
		LastError:     "password=top-secret",
		ResultPayload: []byte(`{"customer":"private"}`),
		UpdatedAt:     time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC),
	}}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, secret := range []string{"top-secret", "private", "result_payload", "last_error"} {
		if strings.Contains(body, secret) {
			t.Errorf("response contains redacted value %q: %s", secret, body)
		}
	}
	if !strings.Contains(body, `"error_present":true`) {
		t.Errorf("response does not preserve safe error presence signal: %s", body)
	}
}

func TestTaskLookupHandlerRedactsBackendErrors(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	taskLookupHandler(stubTaskStateStore{err: errors.New("redis.internal:6379 password=secret")}).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/v1/tasks/task-1", nil),
	)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	if recorder.Body.String() != "{\"error\":\"internal server error\"}\n" {
		t.Fatalf("body = %q, want redacted error", recorder.Body.String())
	}
}

func TestWorkerLifecycleHandlerReturnsWorkerSnapshots(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/admin/workers", nil)

	workerLifecycleHandler(stubWorkerLifecycleProvider{}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var payload struct {
		Workers []struct {
			WorkerID            string  `json:"worker_id"`
			State               string  `json:"state"`
			AbandonedDeliveries float64 `json:"abandoned_deliveries"`
			DrainLeaseLosses    float64 `json:"drain_lease_losses"`
		} `json:"workers"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if len(payload.Workers) != 1 {
		t.Fatalf("worker count = %d, want 1", len(payload.Workers))
	}
	if payload.Workers[0].WorkerID != "worker-a" || payload.Workers[0].State != "draining" {
		t.Fatalf("worker payload = %+v, want worker-a draining", payload.Workers[0])
	}
	if payload.Workers[0].AbandonedDeliveries != 3 || payload.Workers[0].DrainLeaseLosses != 1 {
		t.Fatalf("worker payload = %+v, want abandoned=3 lease_losses=1", payload.Workers[0])
	}
}
