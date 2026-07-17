package sim

import (
	"fmt"
	"sort"
)

func (s *simulation) checkInvariants(final bool) string {
	if invariant := s.checkOwnership(); invariant != "" {
		return invariant
	}
	if invariant := s.checkFencing(); invariant != "" {
		return invariant
	}
	if invariant := s.checkBudget(); invariant != "" {
		return invariant
	}
	if invariant := s.checkTerminalMonotonicity(); invariant != "" {
		return invariant
	}
	if invariant := s.checkBoundedRecovery(final); invariant != "" {
		return invariant
	}
	return ""
}

func (s *simulation) checkOwnership() string {
	activeCounts := make(map[string]int, len(s.backend.active))
	for _, delivery := range s.backend.deliveries {
		if delivery.active {
			activeCounts[delivery.taskID]++
		}
	}
	for _, taskID := range s.backend.taskOrder {
		delivery := s.backend.active[taskID]
		if delivery == nil {
			if activeCounts[taskID] != 0 {
				return fmt.Sprintf("ownership: active delivery for %s is not indexed", taskID)
			}
			continue
		}
		if !delivery.active {
			return fmt.Sprintf("ownership: inactive delivery indexed for %s", taskID)
		}
		if delivery.taskID != taskID {
			return fmt.Sprintf("ownership: task index %s points to %s", taskID, delivery.taskID)
		}
		if activeCounts[taskID] != 1 {
			return fmt.Sprintf("ownership: %s has %d active deliveries", taskID, activeCounts[taskID])
		}
	}
	for _, delivery := range s.backend.deliveries {
		if delivery.acked && delivery.active {
			return fmt.Sprintf("ownership: acknowledged delivery remains active: %s", delivery.id)
		}
		if delivery.acked {
			task := s.backend.tasks[delivery.taskID]
			if task == nil || !task.terminal {
				return fmt.Sprintf("ownership: ack %s did not terminate task", delivery.id)
			}
		}
	}
	return ""
}

func (s *simulation) checkFencing() string {
	var lastEpoch int64
	for _, write := range s.backend.acceptedWrites {
		if write.provided != write.live {
			return fmt.Sprintf("fencing: stale write accepted at %d", write.at)
		}
		if write.provided.Epoch < lastEpoch {
			return fmt.Sprintf("fencing: epoch regressed from %d to %d", lastEpoch, write.provided.Epoch)
		}
		lastEpoch = write.provided.Epoch
	}
	if s.backend.leader.fence.Valid() && s.backend.leader.fence.Epoch != s.backend.leaderEpoch {
		return fmt.Sprintf("fencing: live epoch %d differs from issued epoch %d", s.backend.leader.fence.Epoch, s.backend.leaderEpoch)
	}
	return ""
}

func (s *simulation) checkBudget() string {
	sum := 0
	deliveryIDs := make([]string, 0, len(s.backend.budgetLease))
	for deliveryID := range s.backend.budgetLease {
		deliveryIDs = append(deliveryIDs, deliveryID)
	}
	sort.Strings(deliveryIDs)
	for _, deliveryID := range deliveryIDs {
		tokens := s.backend.budgetLease[deliveryID]
		if tokens <= 0 {
			return fmt.Sprintf("no-negative-budget: lease %s has %d tokens", deliveryID, tokens)
		}
		sum += tokens
	}
	if s.backend.budgetInUse < 0 {
		return fmt.Sprintf("no-negative-budget: in_use=%d", s.backend.budgetInUse)
	}
	if s.backend.budgetInUse > s.backend.budgetCapacity {
		return fmt.Sprintf("budget-capacity: in_use=%d capacity=%d", s.backend.budgetInUse, s.backend.budgetCapacity)
	}
	if sum != s.backend.budgetInUse {
		return fmt.Sprintf("budget-accounting: leases=%d in_use=%d", sum, s.backend.budgetInUse)
	}
	return ""
}

func (s *simulation) checkTerminalMonotonicity() string {
	for _, taskID := range s.backend.taskOrder {
		task := s.backend.tasks[taskID]
		if task.terminalChanges > 1 {
			return fmt.Sprintf("terminal-state-monotonicity: %s changed terminal state %d times", taskID, task.terminalChanges)
		}
		if task.terminal && task.state != task.terminalState {
			return fmt.Sprintf("terminal-state-monotonicity: %s moved from %s to %s", taskID, task.terminalState, task.state)
		}
		if task.terminal && s.backend.active[taskID] != nil {
			return fmt.Sprintf("terminal-state-monotonicity: %s has active delivery", taskID)
		}
	}
	if task := s.backend.tasks["ambiguous-01"]; task != nil && task.publishCount != 1 {
		return fmt.Sprintf("publish-ambiguity: ambiguous-01 committed %d times", task.publishCount)
	}
	return ""
}

func (s *simulation) checkBoundedRecovery(final bool) string {
	now := s.clock.Tick()
	for _, recovery := range s.backend.leaseRecoveries {
		if recovery.recoveredAt > 0 && recovery.recoveredAt > recovery.deadline {
			return fmt.Sprintf("bounded-recovery: %s recovered at %d after deadline %d", recovery.taskID, recovery.recoveredAt, recovery.deadline)
		}
		task := s.backend.tasks[recovery.taskID]
		if recovery.recoveredAt == 0 && task != nil && !task.terminal && now > recovery.deadline {
			return fmt.Sprintf("bounded-recovery: %s unrecovered since %d", recovery.taskID, recovery.expiredAt)
		}
	}
	if s.turnover.startedAt > 0 && !s.turnover.recovered && now > s.turnover.startedAt+8 {
		return fmt.Sprintf("bounded-recovery: leadership not recovered after turnover at %d", s.turnover.startedAt)
	}
	if !final {
		return ""
	}
	for _, taskID := range s.backend.taskOrder {
		if !s.backend.tasks[taskID].terminal {
			return fmt.Sprintf("bounded-recovery: task %s is not terminal at horizon", taskID)
		}
	}
	if s.backend.budgetInUse != 0 {
		return fmt.Sprintf("bounded-recovery: budget still in use at horizon: %d", s.backend.budgetInUse)
	}
	return ""
}
