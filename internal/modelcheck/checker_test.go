package modelcheck

import (
	"errors"
	"strings"
	"testing"
)

func TestCorrectModelsExhaustStateSpace(t *testing.T) {
	reports, err := CheckAll(SmokeBounds())
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 2 {
		t.Fatalf("reports = %d, want 2", len(reports))
	}
	for _, report := range reports {
		if report.States < 20 || report.Transitions < report.States {
			t.Errorf("model %s explored a suspiciously small graph: %+v", report.Model, report)
		}
		var required []string
		switch report.Model {
		case DeliveryModel:
			required = []string{"Reserve", "Tick", "ExtendLease", "Ack", "Nack", "PublishRetry", "Reclaim", "StaleAck", "StaleNack", "StaleExtend", "StalePublishRetry"}
		case SchedulerModel:
			required = []string{"Acquire", "Tick", "Renew", "ExpireLeadership", "Write", "StaleWrite"}
		}
		for _, action := range required {
			if report.Actions[action] == 0 {
				t.Errorf("model %s never explored action %s", report.Model, action)
			}
		}
	}
}

func TestDeliveryModelFindsInjectedStaleOwnerDefect(t *testing.T) {
	_, err := Check(DeliveryModel, DeliveryIDOnly, SmokeBounds())
	violation := requireViolation(t, err, "delivery/stale-owner-mutated-current-lease")
	trace := strings.Join(violation.Trace, "\n")
	for _, action := range []string{"Reserve", "Tick", "Reclaim", "StaleAck", "ACCEPTED_DEFECT"} {
		if !strings.Contains(trace, action) {
			t.Errorf("counterexample missing %q:\n%s", action, trace)
		}
	}
}

func TestDeliveryModelFindsInjectedRetryReceiptDefect(t *testing.T) {
	_, err := Check(DeliveryModel, RetryWithoutReceipt, SmokeBounds())
	violation := requireViolation(t, err, "delivery/retry-publication-not-idempotent")
	trace := strings.Join(violation.Trace, "\n")
	for _, action := range []string{"commit_reply_lost", "replay_after_lost_reply"} {
		if !strings.Contains(trace, action) {
			t.Errorf("counterexample missing %q:\n%s", action, trace)
		}
	}
}

func TestSchedulerModelFindsInjectedEpochFenceDefect(t *testing.T) {
	_, err := Check(SchedulerModel, SchedulerOwnerOnly, SmokeBounds())
	violation := requireViolation(t, err, "scheduler/stale-epoch-write-accepted")
	trace := strings.Join(violation.Trace, "\n")
	for _, action := range []string{"Acquire(owner=1,epoch=1)", "ExpireLeadership", "Acquire(owner=1,epoch=2)", "StaleWrite", "ACCEPTED_DEFECT"} {
		if !strings.Contains(trace, action) {
			t.Errorf("counterexample missing %q:\n%s", action, trace)
		}
	}
}

func TestBoundsFailClosed(t *testing.T) {
	_, err := Check(DeliveryModel, NoMutation, Bounds{MaxDepth: 1, MaxStates: 100})
	requireViolation(t, err, "bounds/depth-truncated")

	_, err = Check(DeliveryModel, NoMutation, Bounds{MaxDepth: 32, MaxStates: 2})
	requireViolation(t, err, "bounds/state-limit-exceeded")
}

func TestMutationMustApplyToSelectedModel(t *testing.T) {
	if _, err := Check(SchedulerModel, DeliveryIDOnly, SmokeBounds()); err == nil {
		t.Fatal("scheduler accepted delivery-only mutation")
	}
	if _, err := Check(DeliveryModel, SchedulerOwnerOnly, SmokeBounds()); err == nil {
		t.Fatal("delivery accepted scheduler-only mutation")
	}
}

func TestCounterexampleIsDeterministic(t *testing.T) {
	_, first := Check(SchedulerModel, SchedulerOwnerOnly, SmokeBounds())
	_, second := Check(SchedulerModel, SchedulerOwnerOnly, SmokeBounds())
	if first == nil || second == nil || first.Error() != second.Error() {
		t.Fatalf("counterexamples differ\nfirst: %v\nsecond: %v", first, second)
	}
}

func requireViolation(t *testing.T, err error, invariant string) *Violation {
	t.Helper()
	if err == nil {
		t.Fatalf("model unexpectedly passed, want %s", invariant)
	}
	var violation *Violation
	if !errors.As(err, &violation) {
		t.Fatalf("error type = %T, want *Violation: %v", err, err)
	}
	if violation.Invariant != invariant {
		t.Fatalf("invariant = %q, want %q\n%s", violation.Invariant, invariant, violation)
	}
	return violation
}
