package redis

import (
	"context"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
)

func TestCloneAdmissionPolicies(t *testing.T) {
	t.Parallel()

	if got := cloneAdmissionPolicies(nil); got != nil {
		t.Fatalf("nil policies = %v, want nil", got)
	}
	if got := cloneAdmissionPolicies(map[string]AdmissionPolicy{}); got != nil {
		t.Fatalf("empty policies = %v, want nil", got)
	}

	cloned := cloneAdmissionPolicies(map[string]AdmissionPolicy{
		"":               {Mode: AdmissionModeDefer, MaxPending: 3},
		"disabled-queue": {Mode: AdmissionModeDisabled},
	})
	if len(cloned) != 1 {
		t.Fatalf("cloned policies = %v, want one entry", cloned)
	}
	policy, ok := cloned["default"]
	if !ok || policy.Mode != AdmissionModeDefer || policy.MaxPending != 3 {
		t.Fatalf("default policy = %+v, want defer with pending cap 3", policy)
	}

	if got := cloneAdmissionPolicies(map[string]AdmissionPolicy{"q": {Mode: AdmissionModeDisabled}}); got != nil {
		t.Fatalf("all-disabled policies = %v, want nil", got)
	}
}

func TestAdmissionPolicyLookup(t *testing.T) {
	t.Parallel()

	if policy := (&Broker{}).admissionPolicy("default"); policy.Mode != AdmissionModeDisabled {
		t.Fatalf("no configured policies mode = %q, want disabled", policy.Mode)
	}

	broker := &Broker{admissionPolicies: map[string]AdmissionPolicy{
		"default": {Mode: AdmissionModeReject, MaxPending: 10},
	}}
	if policy := broker.admissionPolicy("missing"); policy.Mode != AdmissionModeDisabled {
		t.Fatalf("unconfigured queue mode = %q, want disabled", policy.Mode)
	}
	if policy := broker.admissionPolicy(""); policy.Mode != AdmissionModeReject || policy.MaxPending != 10 {
		t.Fatalf("default queue policy = %+v, want reject with pending cap 10", policy)
	}
}

func TestEvaluateAdmissionWithoutSignals(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	broker := &Broker{}
	eval, err := broker.evaluateAdmission(context.Background(), taskforge.Task{Queue: "default"}, taskforge.PublishOptions{}, now)
	if err != nil {
		t.Fatalf("evaluateAdmission: %v", err)
	}
	if eval.decision != taskforge.AdmissionDecisionAccepted || eval.state != "normal" || eval.reason != "" {
		t.Fatalf("disabled policy evaluation = %+v, want accepted normal with no reason", eval)
	}
	if !eval.updatedAt.Equal(now) {
		t.Fatalf("updatedAt = %v, want %v", eval.updatedAt, now)
	}

	broker = &Broker{admissionPolicies: map[string]AdmissionPolicy{
		"default": {Mode: AdmissionModeReject, MaxPending: 1},
	}}
	eval, err = broker.evaluateAdmission(context.Background(), taskforge.Task{Queue: "default"}, taskforge.PublishOptions{Source: taskforge.PublishSourceDeadLetter}, now)
	if err != nil {
		t.Fatalf("evaluateAdmission dead-letter: %v", err)
	}
	if eval.decision != taskforge.AdmissionDecisionAccepted {
		t.Fatalf("dead-letter source decision = %q, want accepted", eval.decision)
	}
}

func TestAnnotateDeferredMessage(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	until := now.Add(30 * time.Second)
	broker := &Broker{}

	msg := broker.annotateDeferredMessage(
		taskforge.Task{Headers: map[string]string{"existing": "1"}},
		taskforge.PublishSourceNew,
		"queue_pending_cap",
		until,
		now,
	)
	if msg.Headers["existing"] != "1" {
		t.Fatalf("existing header dropped: %v", msg.Headers)
	}
	if msg.Headers[headerAdmissionDecision] != string(taskforge.AdmissionDecisionDeferred) ||
		msg.Headers[headerAdmissionReason] != "queue_pending_cap" ||
		msg.Headers[headerAdmissionSource] != string(taskforge.PublishSourceNew) {
		t.Fatalf("admission headers = %v", msg.Headers)
	}
	if msg.Headers[headerAdmissionDeferredUntil] != until.Format(time.RFC3339Nano) {
		t.Fatalf("deferred-until header = %q, want %q", msg.Headers[headerAdmissionDeferredUntil], until.Format(time.RFC3339Nano))
	}
	if msg.Headers[headerAdmissionEvaluatedAt] != now.Format(time.RFC3339Nano) {
		t.Fatalf("evaluated-at header = %q, want %q", msg.Headers[headerAdmissionEvaluatedAt], now.Format(time.RFC3339Nano))
	}
	if msg.ETA == nil || !msg.ETA.Equal(until) {
		t.Fatalf("ETA = %v, want %v", msg.ETA, until)
	}

	empty := broker.annotateDeferredMessage(taskforge.Task{}, taskforge.PublishSourceDueRelease, "queue_pending_cap", until, now)
	if len(empty.Headers) != 5 {
		t.Fatalf("headers for nil input = %v, want five entries", empty.Headers)
	}
	if empty.Headers[headerAdmissionSource] != string(taskforge.PublishSourceDueRelease) {
		t.Fatalf("due-release source header = %q", empty.Headers[headerAdmissionSource])
	}
}

func TestRecordAdmissionState(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	policy := AdmissionPolicy{Mode: AdmissionModeDefer, DeferInterval: 5 * time.Second}
	broker := &Broker{}

	broker.recordAdmissionState("q", "normal", "", policy, admissionSignals{queuePending: 5}, now)
	snapshot, ok := broker.admissionStates["q"]
	if !ok {
		t.Fatal("admission state was not recorded")
	}
	if snapshot.State != "normal" || snapshot.QueuePending != 5 || snapshot.Mode != string(AdmissionModeDefer) {
		t.Fatalf("recorded snapshot = %+v", snapshot)
	}

	broker.recordAdmissionState("q", "normal", "", policy, admissionSignals{queuePending: 99}, now)
	if got := broker.admissionStates["q"].QueuePending; got != 5 {
		t.Fatalf("unchanged state rewritten with queue pending %v, want 5", got)
	}

	broker.recordAdmissionState("q", "degraded", "queue_pending_cap", policy, admissionSignals{queuePending: 42}, now)
	if got := broker.admissionStates["q"]; got.State != "degraded" || got.Reason != "queue_pending_cap" || got.QueuePending != 42 {
		t.Fatalf("updated snapshot = %+v", got)
	}

	var nilBroker *Broker
	nilBroker.recordAdmissionState("q", "normal", "", policy, admissionSignals{}, now)
}

func TestObserveAdmissionDecision(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	broker := &Broker{}

	broker.observeAdmissionDecision("q", taskforge.PublishSourceNew, admissionEvaluation{
		policy:    AdmissionPolicy{Mode: AdmissionModeDefer},
		state:     "normal",
		decision:  taskforge.AdmissionDecisionAccepted,
		updatedAt: now,
	})
	if got, ok := broker.admissionStates["q"]; !ok || got.State != "normal" {
		t.Fatalf("normal decision state = %+v, want normal", got)
	}

	broker.observeAdmissionDecision("q", taskforge.PublishSourceNew, admissionEvaluation{
		policy:    AdmissionPolicy{Mode: AdmissionModeReject},
		state:     "rejecting",
		reason:    "queue_pending_cap",
		decision:  taskforge.AdmissionDecisionRejected,
		updatedAt: now,
	})
	if got := broker.admissionStates["q"]; got.State != "rejecting" || got.Reason != "queue_pending_cap" {
		t.Fatalf("rejected decision state = %+v", got)
	}
}
