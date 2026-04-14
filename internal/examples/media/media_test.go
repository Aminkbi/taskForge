package media

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

func TestHandlerProcessesAllMediaSteps(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(ProcessMediaPayload{
		AssetID: "asset-1",
		Steps:   3,
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	recorder := NewRecorder()
	handler := Handler{
		StepDuration: time.Millisecond,
		Recorder:     recorder,
	}

	if err := handler.HandleTask(context.Background(), broker.TaskMessage{
		ID:      "task-1",
		Name:    TaskProcessMedia,
		Payload: payload,
	}); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}

	if got := recorder.Steps("asset-1"); len(got) != 3 {
		t.Fatalf("len(Steps()) = %d, want 3", len(got))
	}
	if !recorder.Completed("asset-1") {
		t.Fatalf("Completed() = false, want true")
	}
}
