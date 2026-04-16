package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/observability"
)

type stubAdmissionProvider struct{}

func (stubAdmissionProvider) AdmissionStatusSnapshot(context.Context, string, time.Time) (observability.AdmissionStatusSnapshot, error) {
	return observability.AdmissionStatusSnapshot{
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

func (stubAdaptiveProvider) AdaptiveStatusSnapshot(context.Context, string) (observability.AdaptivePoolSnapshot, error) {
	return observability.AdaptivePoolSnapshot{
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

func (stubBudgetUsageProvider) DependencyBudgetUsageSnapshots(context.Context) ([]observability.DependencyBudgetUsageSnapshot, error) {
	return []observability.DependencyBudgetUsageSnapshot{
		{Budget: "downstream", Capacity: 5, InUse: 2},
	}, nil
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
