package externalapi

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aminkbi/taskforge/internal/broker"
)

func TestHandlerReturnsRetryableFailureUntilClientHealthy(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(SyncPayload{ResourceID: "tenant-1"})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	client := NewFakeClient(0)
	handler := Handler{Client: client}
	message := broker.TaskMessage{
		ID:      "task-1",
		Name:    TaskSyncExternalResource,
		Payload: payload,
	}

	if err := handler.HandleTask(context.Background(), message); err == nil {
		t.Fatalf("HandleTask() error = nil, want retryable failure")
	}

	client.SetAvailable(true)
	if err := handler.HandleTask(context.Background(), message); err != nil {
		t.Fatalf("HandleTask() healthy error = %v", err)
	}
	if !client.Synced("tenant-1") {
		t.Fatalf("client did not record synced resource")
	}
}
