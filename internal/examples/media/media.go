package media

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

const TaskProcessMedia = "example.media.process"

type ProcessMediaPayload struct {
	AssetID string `json:"asset_id"`
	Steps   int    `json:"steps"`
}

type Handler struct {
	StepDuration time.Duration
	Recorder     *Recorder
	Logger       *slog.Logger
}

func (h Handler) HandleTask(ctx context.Context, msg broker.TaskMessage) error {
	if msg.Name != TaskProcessMedia {
		return fmt.Errorf("media example: unsupported task %q", msg.Name)
	}
	if h.Recorder == nil {
		return fmt.Errorf("media example: missing recorder")
	}

	var payload ProcessMediaPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("media example: decode payload: %w", err)
	}
	if payload.AssetID == "" {
		return fmt.Errorf("media example: asset_id is required")
	}
	if payload.Steps <= 0 {
		payload.Steps = 3
	}

	stepDuration := h.StepDuration
	if stepDuration <= 0 {
		stepDuration = 200 * time.Millisecond
	}

	for step := 1; step <= payload.Steps; step++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(stepDuration):
		}
		h.Recorder.Record(payload.AssetID, step)
		if h.Logger != nil {
			h.Logger.Info("processed media step", "asset_id", payload.AssetID, "step", step)
		}
	}

	h.Recorder.MarkComplete(payload.AssetID)
	return nil
}

type Recorder struct {
	mu        sync.Mutex
	progress  map[string][]int
	completed map[string]bool
}

func NewRecorder() *Recorder {
	return &Recorder{
		progress:  map[string][]int{},
		completed: map[string]bool{},
	}
}

func (r *Recorder) Record(assetID string, step int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress[assetID] = append(r.progress[assetID], step)
}

func (r *Recorder) MarkComplete(assetID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed[assetID] = true
}

func (r *Recorder) Steps(assetID string) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	steps := append([]int(nil), r.progress[assetID]...)
	return steps
}

func (r *Recorder) Completed(assetID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed[assetID]
}
