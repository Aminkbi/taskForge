package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

func TestInitialScheduleStateWithoutStartAtStartsAfterOneInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	state := initialScheduleState(ScheduleDefinition{
		ID:            "nightly",
		Interval:      15 * time.Minute,
		Queue:         "default",
		TaskName:      "demo.nightly",
		Payload:       json.RawMessage(`{"kind":"nightly"}`),
		Enabled:       true,
		MisfirePolicy: MisfirePolicyCoalesce,
	}, now, "hash-1")

	if !state.NextRunAt.Equal(now.Add(15 * time.Minute)) {
		t.Fatalf("NextRunAt = %v, want %v", state.NextRunAt, now.Add(15*time.Minute))
	}
}

func TestInitialScheduleStateUsesStartAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	startAt := now.Add(3 * time.Minute)
	state := initialScheduleState(ScheduleDefinition{
		ID:            "nightly",
		Interval:      15 * time.Minute,
		Queue:         "default",
		TaskName:      "demo.nightly",
		Payload:       json.RawMessage(`{"kind":"nightly"}`),
		Enabled:       true,
		MisfirePolicy: MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}, now, "hash-1")

	if !state.NextRunAt.Equal(startAt) {
		t.Fatalf("NextRunAt = %v, want %v", state.NextRunAt, startAt)
	}
}

func TestCoalesceNextRunSkipsIntermediateIntervals(t *testing.T) {
	t.Parallel()

	nextRunAt := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	now := nextRunAt.Add(25 * time.Minute)

	nominalAt, next, missedRuns := coalesceNextRun(nextRunAt, 10*time.Minute, now)
	if !nominalAt.Equal(nextRunAt) {
		t.Fatalf("nominalAt = %v, want %v", nominalAt, nextRunAt)
	}
	if !next.Equal(nextRunAt.Add(30 * time.Minute)) {
		t.Fatalf("next = %v, want %v", next, nextRunAt.Add(30*time.Minute))
	}
	if missedRuns != 2 {
		t.Fatalf("missedRuns = %d, want 2", missedRuns)
	}
}

func TestRecurringServiceDispatchesCoalescedRun(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 14, 10, 25, 0, 0, time.UTC)
	store := &stubScheduleStateStore{
		states: map[string]ScheduleState{
			"nightly": {
				NextRunAt: time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC),
				DefinitionHash: hashScheduleDefinition(ScheduleDefinition{
					ID:            "nightly",
					Interval:      10 * time.Minute,
					Queue:         "critical",
					TaskName:      "demo.nightly",
					Payload:       json.RawMessage(`{"job":"nightly"}`),
					Headers:       map[string]string{"x-source": "test"},
					Enabled:       true,
					MisfirePolicy: MisfirePolicyCoalesce,
				}),
			},
		},
	}
	publisher := &stubRecurringPublisher{}
	service := NewRecurringService(publisher, store, []ScheduleDefinition{{
		ID:            "nightly",
		Interval:      10 * time.Minute,
		Queue:         "critical",
		TaskName:      "demo.nightly",
		Payload:       json.RawMessage(`{"job":"nightly"}`),
		Headers:       map[string]string{"x-source": "test"},
		Enabled:       true,
		MisfirePolicy: MisfirePolicyCoalesce,
	}}, nil)
	service.idFunc = func() string { return "recurring-task-1" }

	dispatched, err := service.SyncDue(context.Background(), now)
	if err != nil {
		t.Fatalf("SyncDue() error = %v", err)
	}
	if dispatched != 1 {
		t.Fatalf("SyncDue() dispatched = %d, want 1", dispatched)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(publisher.messages))
	}

	msg := publisher.messages[0]
	if msg.ID != "recurring-task-1" {
		t.Fatalf("published task id = %q, want %q", msg.ID, "recurring-task-1")
	}
	if msg.Headers[HeaderScheduleID] != "nightly" {
		t.Fatalf("schedule id header = %q, want %q", msg.Headers[HeaderScheduleID], "nightly")
	}
	if msg.Headers[HeaderScheduleMissedRuns] != "2" {
		t.Fatalf("missed runs header = %q, want %q", msg.Headers[HeaderScheduleMissedRuns], "2")
	}
	if store.states["nightly"].NextRunAt.Format(time.RFC3339) != "2026-04-14T10:30:00Z" {
		t.Fatalf("next run = %v, want 2026-04-14T10:30:00Z", store.states["nightly"].NextRunAt)
	}
}

func TestRecurringServiceSkipsDisabledSchedule(t *testing.T) {
	t.Parallel()

	store := &stubScheduleStateStore{states: map[string]ScheduleState{}}
	publisher := &stubRecurringPublisher{}
	service := NewRecurringService(publisher, store, []ScheduleDefinition{{
		ID:            "disabled",
		Interval:      time.Minute,
		Queue:         "default",
		TaskName:      "demo.disabled",
		Payload:       json.RawMessage(`{}`),
		Enabled:       false,
		MisfirePolicy: MisfirePolicyCoalesce,
	}}, nil)

	dispatched, err := service.SyncDue(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("SyncDue() error = %v", err)
	}
	if dispatched != 0 {
		t.Fatalf("SyncDue() dispatched = %d, want 0", dispatched)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("published messages = %d, want 0", len(publisher.messages))
	}
}

type stubScheduleStateStore struct {
	states map[string]ScheduleState
}

func (s *stubScheduleStateStore) Load(_ context.Context, scheduleID string) (ScheduleState, bool, error) {
	state, ok := s.states[scheduleID]
	return state, ok, nil
}

func (s *stubScheduleStateStore) Save(_ context.Context, scheduleID string, state ScheduleState) error {
	s.states[scheduleID] = state
	return nil
}

type stubRecurringPublisher struct {
	messages []broker.TaskMessage
}

func (s *stubRecurringPublisher) Publish(_ context.Context, msg broker.TaskMessage) error {
	s.messages = append(s.messages, msg)
	return nil
}

func (s *stubRecurringPublisher) Reserve(context.Context, string, string) (broker.Delivery, error) {
	return broker.Delivery{}, nil
}

func (s *stubRecurringPublisher) Ack(context.Context, broker.Delivery) error {
	return nil
}

func (s *stubRecurringPublisher) Nack(context.Context, broker.Delivery, bool) error {
	return nil
}

func (s *stubRecurringPublisher) ExtendLease(context.Context, broker.Delivery, time.Duration) error {
	return nil
}
