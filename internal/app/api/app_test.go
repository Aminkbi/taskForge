package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
