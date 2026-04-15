package scheduler

import (
	"context"
	"encoding/json"
	"slices"
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
	if state.MisfirePolicy != MisfirePolicyCoalesce {
		t.Fatalf("MisfirePolicy = %q, want %q", state.MisfirePolicy, MisfirePolicyCoalesce)
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
	schedule := ScheduleDefinition{
		ID:            "nightly",
		Interval:      10 * time.Minute,
		Queue:         "critical",
		TaskName:      "demo.nightly",
		Payload:       json.RawMessage(`{"job":"nightly"}`),
		Headers:       map[string]string{"x-source": "test"},
		Enabled:       true,
		MisfirePolicy: MisfirePolicyCoalesce,
	}
	store := newStubScheduleStateStore()
	store.states[schedule.ID] = ScheduleState{
		NextRunAt:      time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC),
		DefinitionHash: hashScheduleDefinition(schedule),
		MisfirePolicy:  schedule.MisfirePolicy,
	}
	store.dueIndex[schedule.ID] = store.states[schedule.ID].NextRunAt
	store.persistedIDs[schedule.ID] = struct{}{}

	publisher := &stubRecurringPublisher{}
	service := NewRecurringService(publisher, store, []ScheduleDefinition{schedule}, nil)
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
	if !store.dueIndex["nightly"].Equal(store.states["nightly"].NextRunAt) {
		t.Fatalf("due index next run = %v, want %v", store.dueIndex["nightly"], store.states["nightly"].NextRunAt)
	}
}

func TestRecurringServiceUsesDueIndexInsteadOfFullScheduleScan(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 14, 10, 25, 0, 0, time.UTC)
	dueSchedule := ScheduleDefinition{
		ID:            "due",
		Interval:      10 * time.Minute,
		Queue:         "default",
		TaskName:      "demo.due",
		Payload:       json.RawMessage(`{"job":"due"}`),
		Enabled:       true,
		MisfirePolicy: MisfirePolicyCoalesce,
	}
	futureSchedule := ScheduleDefinition{
		ID:            "future",
		Interval:      10 * time.Minute,
		Queue:         "default",
		TaskName:      "demo.future",
		Payload:       json.RawMessage(`{"job":"future"}`),
		Enabled:       true,
		MisfirePolicy: MisfirePolicyCoalesce,
	}

	store := newStubScheduleStateStore()
	store.states[dueSchedule.ID] = ScheduleState{
		NextRunAt:      now.Add(-5 * time.Minute),
		DefinitionHash: hashScheduleDefinition(dueSchedule),
		MisfirePolicy:  dueSchedule.MisfirePolicy,
	}
	store.states[futureSchedule.ID] = ScheduleState{
		NextRunAt:      now.Add(30 * time.Minute),
		DefinitionHash: hashScheduleDefinition(futureSchedule),
		MisfirePolicy:  futureSchedule.MisfirePolicy,
	}
	store.dueIndex[dueSchedule.ID] = store.states[dueSchedule.ID].NextRunAt
	store.dueIndex[futureSchedule.ID] = store.states[futureSchedule.ID].NextRunAt
	store.persistedIDs[dueSchedule.ID] = struct{}{}
	store.persistedIDs[futureSchedule.ID] = struct{}{}

	publisher := &stubRecurringPublisher{}
	service := NewRecurringService(publisher, store, []ScheduleDefinition{dueSchedule, futureSchedule}, nil)
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
	if !slices.Equal(store.loadCalls, []string{"due"}) {
		t.Fatalf("LoadStates() ids = %v, want [due]", store.loadCalls)
	}
}

func TestRecurringServiceReconcileIndexesEnabledSchedulesAndCleansUpRemovedOnes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	startAt := now.Add(2 * time.Minute)
	store := newStubScheduleStateStore()
	store.states["removed"] = ScheduleState{
		NextRunAt:      now.Add(time.Hour),
		DefinitionHash: "old-hash",
		MisfirePolicy:  MisfirePolicyCoalesce,
	}
	store.persistedIDs["removed"] = struct{}{}
	store.dueIndex["removed"] = now.Add(time.Hour)

	service := NewRecurringService(&stubRecurringPublisher{}, store, []ScheduleDefinition{
		{
			ID:            "enabled",
			Interval:      15 * time.Minute,
			Queue:         "default",
			TaskName:      "demo.enabled",
			Payload:       json.RawMessage(`{"job":"enabled"}`),
			Enabled:       true,
			MisfirePolicy: MisfirePolicyCoalesce,
			StartAt:       &startAt,
		},
		{
			ID:            "disabled",
			Interval:      15 * time.Minute,
			Queue:         "default",
			TaskName:      "demo.disabled",
			Payload:       json.RawMessage(`{"job":"disabled"}`),
			Enabled:       false,
			MisfirePolicy: MisfirePolicyCoalesce,
			StartAt:       &startAt,
		},
	}, nil)

	dispatched, err := service.SyncDue(context.Background(), now)
	if err != nil {
		t.Fatalf("SyncDue() error = %v", err)
	}
	if dispatched != 0 {
		t.Fatalf("SyncDue() dispatched = %d, want 0", dispatched)
	}
	if _, ok := store.dueIndex["enabled"]; !ok {
		t.Fatal("enabled schedule missing from due index after reconcile")
	}
	if _, ok := store.states["disabled"]; !ok {
		t.Fatal("disabled schedule state missing after reconcile")
	}
	if _, ok := store.dueIndex["disabled"]; ok {
		t.Fatal("disabled schedule should not remain in due index after reconcile")
	}
	if _, ok := store.states["removed"]; ok {
		t.Fatal("removed schedule state still exists after reconcile")
	}
	if _, ok := store.dueIndex["removed"]; ok {
		t.Fatal("removed schedule due index entry still exists after reconcile")
	}
}

type stubScheduleStateStore struct {
	states       map[string]ScheduleState
	dueIndex     map[string]time.Time
	persistedIDs map[string]struct{}
	loadCalls    []string
}

func newStubScheduleStateStore() *stubScheduleStateStore {
	return &stubScheduleStateStore{
		states:       map[string]ScheduleState{},
		dueIndex:     map[string]time.Time{},
		persistedIDs: map[string]struct{}{},
	}
}

func (s *stubScheduleStateStore) ReconcileConfigured(_ context.Context, schedules []ScheduleDefinition, now time.Time) error {
	configured := make(map[string]ScheduleDefinition, len(schedules))
	for _, schedule := range schedules {
		configured[schedule.ID] = schedule

		state, exists := s.states[schedule.ID]
		definitionHash := hashScheduleDefinition(schedule)
		if !exists || state.DefinitionHash != definitionHash || state.NextRunAt.IsZero() {
			state = initialScheduleState(schedule, now, definitionHash)
		}
		state.DefinitionHash = definitionHash
		state.MisfirePolicy = schedule.MisfirePolicy
		s.states[schedule.ID] = state
		s.persistedIDs[schedule.ID] = struct{}{}

		if schedule.Enabled {
			s.dueIndex[schedule.ID] = state.NextRunAt
			continue
		}
		delete(s.dueIndex, schedule.ID)
	}

	for scheduleID := range s.persistedIDs {
		if _, ok := configured[scheduleID]; ok {
			continue
		}
		delete(s.persistedIDs, scheduleID)
		delete(s.states, scheduleID)
		delete(s.dueIndex, scheduleID)
	}

	return nil
}

func (s *stubScheduleStateStore) DueScheduleIDs(_ context.Context, now time.Time, limit int64) ([]string, error) {
	ids := make([]string, 0, len(s.dueIndex))
	for scheduleID, nextRunAt := range s.dueIndex {
		if nextRunAt.After(now) {
			continue
		}
		ids = append(ids, scheduleID)
	}
	slices.Sort(ids)
	if int64(len(ids)) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func (s *stubScheduleStateStore) LoadStates(_ context.Context, scheduleIDs []string) (map[string]ScheduleState, error) {
	s.loadCalls = append(s.loadCalls, scheduleIDs...)
	states := make(map[string]ScheduleState, len(scheduleIDs))
	for _, scheduleID := range scheduleIDs {
		state, ok := s.states[scheduleID]
		if !ok {
			continue
		}
		states[scheduleID] = state
	}
	return states, nil
}

func (s *stubScheduleStateStore) SaveIndexed(_ context.Context, scheduleID string, state ScheduleState) error {
	s.states[scheduleID] = state
	s.dueIndex[scheduleID] = state.NextRunAt
	s.persistedIDs[scheduleID] = struct{}{}
	return nil
}

func (s *stubScheduleStateStore) RemoveSchedule(_ context.Context, scheduleID string) error {
	delete(s.states, scheduleID)
	delete(s.dueIndex, scheduleID)
	delete(s.persistedIDs, scheduleID)
	return nil
}

func (s *stubScheduleStateStore) RemoveFromDueIndex(_ context.Context, scheduleID string) error {
	delete(s.dueIndex, scheduleID)
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
