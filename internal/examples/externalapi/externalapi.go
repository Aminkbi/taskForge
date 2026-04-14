package externalapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aminkbi/taskforge/internal/broker"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
)

const TaskSyncExternalResource = "example.external_api.sync"

type SyncPayload struct {
	ResourceID string `json:"resource_id"`
}

type Client interface {
	Call(context.Context, SyncPayload) error
}

type Handler struct {
	Client Client
	Logger *slog.Logger
}

func (h Handler) HandleTask(ctx context.Context, msg broker.TaskMessage) error {
	if msg.Name != TaskSyncExternalResource {
		return fmt.Errorf("external api example: unsupported task %q", msg.Name)
	}
	if h.Client == nil {
		return fmt.Errorf("external api example: missing client")
	}

	var payload SyncPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return runtimepkg.Decode(fmt.Errorf("external api example: decode payload: %w", err))
	}
	if payload.ResourceID == "" {
		return runtimepkg.Validation(fmt.Errorf("external api example: resource_id is required"))
	}

	if err := h.Client.Call(ctx, payload); err != nil {
		if h.Logger != nil {
			h.Logger.Warn("external api call failed", "resource_id", payload.ResourceID, "error", err)
		}
		return runtimepkg.Retryable(err)
	}

	if h.Logger != nil {
		h.Logger.Info("external api call succeeded", "resource_id", payload.ResourceID)
	}
	return nil
}

type FakeClient struct {
	mu           sync.Mutex
	available    bool
	callCount    int
	syncedIDs    []string
	failuresLeft int
}

func NewFakeClient(failures int) *FakeClient {
	return &FakeClient{failuresLeft: failures}
}

func (c *FakeClient) SetAvailable(available bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.available = available
}

func (c *FakeClient) Call(_ context.Context, payload SyncPayload) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.callCount++
	if !c.available {
		return fmt.Errorf("upstream unavailable")
	}
	if c.failuresLeft > 0 {
		c.failuresLeft--
		return fmt.Errorf("transient upstream error")
	}
	c.syncedIDs = append(c.syncedIDs, payload.ResourceID)
	return nil
}

func (c *FakeClient) CallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.callCount
}

func (c *FakeClient) Synced(resourceID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range c.syncedIDs {
		if id == resourceID {
			return true
		}
	}
	return false
}
