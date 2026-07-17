package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aminkbi/taskforge"
)

const (
	headerAdmissionDecision      = "taskforge_admission_decision"
	headerAdmissionReason        = "taskforge_admission_reason"
	headerAdmissionSource        = "taskforge_admission_source"
	headerAdmissionDeferredUntil = "taskforge_admission_deferred_until"
	headerAdmissionEvaluatedAt   = "taskforge_admission_evaluated_at"
)

type AdmissionMode string

const (
	AdmissionModeDisabled AdmissionMode = "disabled"
	AdmissionModeDefer    AdmissionMode = "defer"
	AdmissionModeReject   AdmissionMode = "reject"
)

type AdmissionPolicy struct {
	Mode                     AdmissionMode
	MaxPending               int64
	MaxPendingPerFairnessKey int64
	MaxOldestReadyAge        time.Duration
	MaxRetryBacklog          int64
	MaxDeadLetterSize        int64
	DeferInterval            time.Duration
}

type admissionSignals struct {
	queuePending       int64
	fairnessKeyPending int64
	oldestReadyAge     time.Duration
	retryBacklog       int64
	deadLetterSize     int64
}

type admissionEvaluation struct {
	policy     AdmissionPolicy
	state      string
	reason     string
	decision   taskforge.AdmissionDecision
	deferUntil *time.Time
	signals    admissionSignals
	updatedAt  time.Time
}

func cloneAdmissionPolicies(policies map[string]AdmissionPolicy) map[string]AdmissionPolicy {
	if len(policies) == 0 {
		return nil
	}

	cloned := make(map[string]AdmissionPolicy, len(policies))
	for queue, policy := range policies {
		if policy.Mode == AdmissionModeDisabled {
			continue
		}
		cloned[normalizeQueue(queue)] = policy
	}
	if len(cloned) == 0 {
		return nil
	}
	return cloned
}

func (b *Broker) admissionPolicy(queue string) AdmissionPolicy {
	if len(b.admissionPolicies) == 0 {
		return AdmissionPolicy{Mode: AdmissionModeDisabled}
	}
	policy, ok := b.admissionPolicies[normalizeQueue(queue)]
	if !ok {
		return AdmissionPolicy{Mode: AdmissionModeDisabled}
	}
	return policy
}

func (b *Broker) AdmissionStatusSnapshot(ctx context.Context, queue string, now time.Time) (taskforge.AdmissionStatusSnapshot, error) {
	queue = normalizeQueue(queue)
	eval, err := b.evaluateAdmission(ctx, taskforge.Task{Queue: queue}, taskforge.PublishOptions{Source: taskforge.PublishSourceNew}, now)
	if err != nil {
		return taskforge.AdmissionStatusSnapshot{}, err
	}
	return taskforge.AdmissionStatusSnapshot{
		Queue:              queue,
		Mode:               string(eval.policy.Mode),
		State:              eval.state,
		Reason:             eval.reason,
		QueuePending:       float64(eval.signals.queuePending),
		FairnessKeyPending: float64(eval.signals.fairnessKeyPending),
		OldestReadyAge:     eval.signals.oldestReadyAge.Seconds(),
		RetryBacklog:       float64(eval.signals.retryBacklog),
		DeadLetterSize:     float64(eval.signals.deadLetterSize),
		DeferInterval:      eval.policy.DeferInterval,
		UpdatedAt:          eval.updatedAt.UTC(),
	}, nil
}

func (b *Broker) evaluateAdmission(ctx context.Context, msg taskforge.Task, opts taskforge.PublishOptions, now time.Time) (admissionEvaluation, error) {
	opts = opts.Normalize()
	queue := normalizeQueue(taskforge.EffectiveQueue(msg))
	policy := b.admissionPolicy(queue)
	eval := admissionEvaluation{
		policy:    policy,
		state:     "normal",
		decision:  taskforge.AdmissionDecisionAccepted,
		signals:   admissionSignals{},
		updatedAt: now.UTC(),
	}

	if opts.Source == taskforge.PublishSourceDeadLetter || policy.Mode == AdmissionModeDisabled {
		return eval, nil
	}

	signals, reason, err := b.loadAdmissionSignals(ctx, queue, msg.FairnessKey, policy, now)
	if err != nil {
		return admissionEvaluation{}, err
	}
	eval.signals = signals
	eval.reason = reason
	if reason == "" {
		return eval, nil
	}

	switch policy.Mode {
	case AdmissionModeReject:
		eval.state = "rejecting"
		if opts.Source == taskforge.PublishSourceDueRelease {
			eval.decision = taskforge.AdmissionDecisionDeferred
			deferUntil := now.UTC().Add(policy.DeferInterval)
			eval.deferUntil = &deferUntil
			return eval, nil
		}
		eval.decision = taskforge.AdmissionDecisionRejected
	case AdmissionModeDefer:
		eval.state = "degraded"
		eval.decision = taskforge.AdmissionDecisionDeferred
		deferUntil := now.UTC().Add(policy.DeferInterval)
		eval.deferUntil = &deferUntil
	default:
		eval.state = "normal"
	}

	return eval, nil
}

func (b *Broker) loadAdmissionSignals(ctx context.Context, queue, fairnessKey string, policy AdmissionPolicy, now time.Time) (admissionSignals, string, error) {
	queue = normalizeQueue(queue)
	snapshot, err := b.QueueMetricsSnapshot(ctx, queue)
	if err != nil {
		return admissionSignals{}, "", err
	}
	signals := admissionSignals{
		queuePending: int64(snapshot.Depth + snapshot.Reserved),
	}

	if policy.MaxPendingPerFairnessKey > 0 && b.fairnessPolicy(queue) != nil {
		fairnessKey = NormalizeFairnessKey(fairnessKey)
		fairnessSnapshot, ok, err := b.loadFairnessKeySnapshot(ctx, queue, fairnessKey, now)
		if err != nil {
			return admissionSignals{}, "", err
		}
		if ok {
			signals.fairnessKeyPending = fairnessSnapshot.Ready + fairnessSnapshot.Reserved
		}
	}

	if policy.MaxOldestReadyAge > 0 {
		signals.oldestReadyAge = b.oldestQueueReadyAge(ctx, queue, now)
	}

	if policy.MaxRetryBacklog > 0 {
		retryBacklog, err := b.retryBacklog(ctx, queue)
		if err != nil {
			return admissionSignals{}, "", err
		}
		signals.retryBacklog = retryBacklog
	}

	if policy.MaxDeadLetterSize > 0 {
		deadLetterSize, err := b.deadLetterQueueSizeInt(ctx, queue)
		if err != nil {
			return admissionSignals{}, "", err
		}
		signals.deadLetterSize = deadLetterSize
	}

	switch {
	case policy.MaxPendingPerFairnessKey > 0 && signals.fairnessKeyPending >= policy.MaxPendingPerFairnessKey:
		return signals, "fairness_key_pending_cap", nil
	case policy.MaxPending > 0 && signals.queuePending >= policy.MaxPending:
		return signals, "queue_pending_cap", nil
	case policy.MaxOldestReadyAge > 0 && signals.oldestReadyAge >= policy.MaxOldestReadyAge:
		return signals, "oldest_ready_age", nil
	case policy.MaxRetryBacklog > 0 && signals.retryBacklog >= policy.MaxRetryBacklog:
		return signals, "retry_backlog", nil
	case policy.MaxDeadLetterSize > 0 && signals.deadLetterSize >= policy.MaxDeadLetterSize:
		return signals, "dead_letter_size", nil
	default:
		return signals, "", nil
	}
}

func (b *Broker) oldestQueueReadyAge(ctx context.Context, queue string, now time.Time) time.Duration {
	if b.fairnessPolicy(queue) != nil {
		snapshots, err := b.loadFairnessSnapshots(ctx, queue, now, true)
		if err != nil {
			return 0
		}
		var maxAge time.Duration
		for _, snapshot := range snapshots {
			if snapshot.Ready == 0 {
				continue
			}
			age := time.Duration(snapshot.OldestReadyAge * float64(time.Second))
			if age > maxAge {
				maxAge = age
			}
		}
		return maxAge
	}

	return b.oldestReadyAge(ctx, b.streamKey(queue), b.groupName(queue), now)
}

func (b *Broker) retryBacklog(ctx context.Context, queue string) (int64, error) {
	count, err := b.client.ZCard(ctx, b.delayedRetryIndexKey(queue)).Result()
	if err != nil {
		return 0, fmt.Errorf("retry backlog %q: %w", queue, err)
	}
	return count, nil
}

func (b *Broker) deadLetterQueueSizeInt(ctx context.Context, queue string) (int64, error) {
	length, err := b.client.XLen(ctx, dlqStreamKey(queue)).Result()
	if err != nil {
		if isMissingStream(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("dead-letter queue metrics %q: %w", queue, err)
	}
	return length, nil
}

func (b *Broker) annotateDeferredMessage(msg taskforge.Task, source taskforge.PublishSource, reason string, deferredUntil, now time.Time) taskforge.Task {
	if msg.Headers == nil {
		msg.Headers = make(map[string]string, 5)
	}
	msg.Headers[headerAdmissionDecision] = string(taskforge.AdmissionDecisionDeferred)
	msg.Headers[headerAdmissionReason] = reason
	msg.Headers[headerAdmissionSource] = string(source)
	msg.Headers[headerAdmissionDeferredUntil] = deferredUntil.UTC().Format(time.RFC3339Nano)
	msg.Headers[headerAdmissionEvaluatedAt] = now.UTC().Format(time.RFC3339Nano)
	msg.ETA = &deferredUntil
	return msg
}

func (b *Broker) observeAdmissionDecision(queue string, source taskforge.PublishSource, eval admissionEvaluation) {
	if b.metrics != nil {
		b.metrics.IncAdmissionDecision(queue, string(source), string(eval.decision), eval.reason)
	}
	if eval.reason == "" {
		b.recordAdmissionState(queue, "normal", "", eval.policy, eval.signals, eval.updatedAt)
		return
	}
	b.recordAdmissionState(queue, eval.state, eval.reason, eval.policy, eval.signals, eval.updatedAt)
	if b.logger == nil {
		return
	}
	level := slog.LevelWarn
	if eval.decision == taskforge.AdmissionDecisionAccepted {
		level = slog.LevelInfo
	}
	b.logger.Log(
		context.Background(),
		level,
		"admission decision",
		"queue", queue,
		"source", source,
		"decision", eval.decision,
		"reason", eval.reason,
		"mode", eval.policy.Mode,
		"queue_pending", eval.signals.queuePending,
		"fairness_key_pending", eval.signals.fairnessKeyPending,
		"oldest_ready_age", eval.signals.oldestReadyAge,
		"retry_backlog", eval.signals.retryBacklog,
		"dead_letter_size", eval.signals.deadLetterSize,
	)
}

func (b *Broker) recordAdmissionState(queue, state, reason string, policy AdmissionPolicy, signals admissionSignals, updatedAt time.Time) {
	if b == nil {
		return
	}
	b.admissionStateMu.Lock()
	defer b.admissionStateMu.Unlock()

	current, ok := b.admissionStates[queue]
	if ok && current.State == state && current.Reason == reason {
		return
	}
	if b.admissionStates == nil {
		b.admissionStates = make(map[string]taskforge.AdmissionStatusSnapshot)
	}
	snapshot := taskforge.AdmissionStatusSnapshot{
		Queue:              queue,
		Mode:               string(policy.Mode),
		State:              state,
		Reason:             reason,
		QueuePending:       float64(signals.queuePending),
		FairnessKeyPending: float64(signals.fairnessKeyPending),
		OldestReadyAge:     signals.oldestReadyAge.Seconds(),
		RetryBacklog:       float64(signals.retryBacklog),
		DeadLetterSize:     float64(signals.deadLetterSize),
		DeferInterval:      policy.DeferInterval,
		UpdatedAt:          updatedAt.UTC(),
	}
	b.admissionStates[queue] = snapshot
	if b.logger != nil {
		b.logger.Info(
			"admission state changed",
			"queue", queue,
			"state", state,
			"reason", reason,
			"mode", policy.Mode,
		)
	}
}
