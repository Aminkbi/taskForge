package modelcheck

import (
	"fmt"

	"github.com/aminkbi/taskforge"
)

const (
	deliveryMaxTime           = 4
	deliveryMaxLeaseGen       = 3
	deliveryMaxAttempts       = 2
	deliveryMaxNacks          = 1
	staleAckBit         uint8 = 1 << iota
	staleNackBit
	staleExtendBit
	staleRetryBit
)

type deliveryModel struct {
	mutation Mutation
}

type deliveryState struct {
	Now            uint8
	Status         taskforge.State
	Attempt        uint8
	DeliveryID     uint8
	LeaseGen       uint8
	Owner          uint8
	Expires        uint8
	Extended       bool
	PreviousID     uint8
	PreviousGen    uint8
	PreviousOwner  uint8
	StaleTried     uint8
	RetryReceipt   bool
	RetryCopies    uint8
	RetryAwaitAck  bool
	AmbiguousReply bool
	Nacks          uint8
	TerminalWrites uint8
	StaleMutation  bool
}

func newDeliveryModel(mutation Mutation) *deliveryModel {
	return &deliveryModel{mutation: mutation}
}

func (m *deliveryModel) name() Model { return DeliveryModel }

func (m *deliveryModel) initial() state {
	return deliveryState{Status: taskforge.StateQueued, DeliveryID: 1}
}

func (s deliveryState) key() string {
	return fmt.Sprintf("%d|%s|%d|%d|%d|%d|%d|%t|%d|%d|%d|%d|%t|%d|%t|%t|%d|%d|%t",
		s.Now, s.Status, s.Attempt, s.DeliveryID, s.LeaseGen, s.Owner, s.Expires, s.Extended,
		s.PreviousID, s.PreviousGen, s.PreviousOwner, s.StaleTried, s.RetryReceipt,
		s.RetryCopies, s.RetryAwaitAck, s.AmbiguousReply, s.Nacks, s.TerminalWrites, s.StaleMutation)
}

func (s deliveryState) describe() string {
	return fmt.Sprintf("now=%d state=%s attempt=%d delivery=d%d lease=g%d/o%d/expires%d previous=d%d/g%d/o%d retry(receipt=%t,copies=%d,await_ack=%t,ambiguous=%t) nacks=%d terminal_writes=%d stale_mutation=%t",
		s.Now, s.Status, s.Attempt, s.DeliveryID, s.LeaseGen, s.Owner, s.Expires,
		s.PreviousID, s.PreviousGen, s.PreviousOwner, s.RetryReceipt, s.RetryCopies,
		s.RetryAwaitAck, s.AmbiguousReply, s.Nacks, s.TerminalWrites, s.StaleMutation)
}

func (m *deliveryModel) terminal(raw state) bool {
	s := raw.(deliveryState)
	return taskforge.CompletesTask(s.Status)
}

func (m *deliveryModel) invariant(raw state) string {
	s := raw.(deliveryState)
	if s.StaleMutation {
		return "delivery/stale-owner-mutated-current-lease"
	}
	if s.RetryCopies > 1 {
		return "delivery/retry-publication-not-idempotent"
	}
	if s.Status == taskforge.StateQueued && s.Owner != 0 {
		return "delivery/queued-has-owner"
	}
	if s.Status == taskforge.StateLeased && (s.Owner == 0 || s.LeaseGen == 0) {
		return "delivery/leased-without-fenced-owner"
	}
	if taskforge.CompletesTask(s.Status) && (s.Owner != 0 || s.TerminalWrites != 1) {
		return "delivery/terminal-state-not-monotonic"
	}
	if s.Attempt >= deliveryMaxAttempts {
		return "delivery/retry-attempt-bound-exceeded"
	}
	return ""
}

func (m *deliveryModel) next(raw state) []transition {
	s := raw.(deliveryState)
	if taskforge.CompletesTask(s.Status) {
		return nil
	}
	var out []transition
	add := func(action string, next deliveryState) {
		out = append(out, transition{action: action, next: next})
	}

	if s.Status == taskforge.StateQueued {
		for owner := uint8(1); owner <= 2; owner++ {
			next := s
			next.Status = taskforge.StateLeased
			next.Owner = owner
			next.LeaseGen++
			next.Expires = next.Now + 1
			next.Extended = false
			next.StaleTried = 0
			add(fmt.Sprintf("Reserve(owner=%d,d%d,g%d)", owner, next.DeliveryID, next.LeaseGen), next)
		}
	}

	if s.Now < deliveryMaxTime {
		wouldExhaustLastLease := s.Status == taskforge.StateLeased && s.LeaseGen >= deliveryMaxLeaseGen && s.Now+1 >= s.Expires
		if !wouldExhaustLastLease {
			next := s
			next.Now++
			add("Tick", next)
		}
	}

	if s.Status == taskforge.StateLeased && s.Now < s.Expires {
		if !s.Extended && s.Expires < deliveryMaxTime {
			next := s
			next.Expires++
			next.Extended = true
			add("ExtendLease(current)", next)
		}

		next := s
		next.Status = taskforge.StateSucceeded
		next.Owner = 0
		next.TerminalWrites++
		add("Ack(current,succeeded)", next)

		next = s
		next.Status = taskforge.StateDeadLettered
		next.Owner = 0
		next.TerminalWrites++
		add("Ack(current,dead_lettered)", next)

		if s.Nacks < deliveryMaxNacks && !s.RetryAwaitAck {
			next = s.nextDelivery()
			next.Nacks++
			add("Nack(current,requeue)", next)
		}

		if s.Attempt+1 < deliveryMaxAttempts {
			if !s.RetryReceipt && !s.RetryAwaitAck {
				next = s
				next.RetryReceipt = true
				next.RetryCopies = 1
				next.RetryAwaitAck = true
				add("PublishRetry(current,commit_reply_ok)", next)

				next.AmbiguousReply = true
				add("PublishRetry(current,commit_reply_lost)", next)
			}
			if s.RetryAwaitAck && s.AmbiguousReply {
				next = s
				if m.mutation == RetryWithoutReceipt {
					next.RetryCopies++
				}
				next.AmbiguousReply = false
				add("PublishRetry(current,replay_after_lost_reply)", next)
			}
			if s.RetryAwaitAck && !s.AmbiguousReply {
				next = s.nextDelivery()
				next.Attempt++
				add("Ack(current,retry_scheduled)", next)
			}
		}
	}

	if s.Status == taskforge.StateLeased && s.Now >= s.Expires && s.LeaseGen < deliveryMaxLeaseGen {
		for owner := uint8(1); owner <= 2; owner++ {
			if owner == s.Owner {
				continue
			}
			next := s
			next.PreviousID = s.DeliveryID
			next.PreviousGen = s.LeaseGen
			next.PreviousOwner = s.Owner
			next.LeaseGen++
			next.Owner = owner
			next.Expires = next.Now + 1
			next.Extended = false
			next.StaleTried = 0
			add(fmt.Sprintf("Reclaim(owner=%d,d%d,g%d)", owner, next.DeliveryID, next.LeaseGen), next)
		}
	}

	if s.PreviousOwner != 0 && s.Status == taskforge.StateLeased {
		m.addStaleTransitions(&out, s)
	}
	return out
}

func (s deliveryState) nextDelivery() deliveryState {
	next := s
	next.PreviousID = s.DeliveryID
	next.PreviousGen = s.LeaseGen
	next.PreviousOwner = s.Owner
	next.Status = taskforge.StateQueued
	next.DeliveryID++
	next.LeaseGen = 0
	next.Owner = 0
	next.Expires = 0
	next.Extended = false
	next.StaleTried = 0
	next.RetryReceipt = false
	next.RetryCopies = 0
	next.RetryAwaitAck = false
	next.AmbiguousReply = false
	return next
}

func (m *deliveryModel) addStaleTransitions(out *[]transition, s deliveryState) {
	// Production fences a lease with the stream delivery ID plus consumer
	// owner. LeaseGen is a trace-only ghost variable, not part of this guard.
	accepted := s.PreviousID == s.DeliveryID && s.PreviousOwner == s.Owner
	if m.mutation == DeliveryIDOnly {
		accepted = s.PreviousID == s.DeliveryID
	}
	add := func(bit uint8, action string, mutate func(*deliveryState)) {
		if s.StaleTried&bit != 0 {
			return
		}
		next := s
		next.StaleTried |= bit
		if accepted {
			next.StaleMutation = true
			mutate(&next)
			action += ",ACCEPTED_DEFECT)"
		} else {
			action += ",rejected)"
		}
		*out = append(*out, transition{action: action, next: next})
	}
	handle := fmt.Sprintf("stale=d%d/g%d/o%d", s.PreviousID, s.PreviousGen, s.PreviousOwner)
	add(staleAckBit, "StaleAck("+handle, func(next *deliveryState) {
		next.Status = taskforge.StateSucceeded
		next.Owner = 0
		next.TerminalWrites++
	})
	add(staleNackBit, "StaleNack("+handle, func(next *deliveryState) {
		*next = next.nextDelivery()
	})
	add(staleExtendBit, "StaleExtend("+handle, func(next *deliveryState) {
		next.Expires++
	})
	add(staleRetryBit, "StalePublishRetry("+handle, func(next *deliveryState) {
		next.RetryReceipt = true
		next.RetryCopies++
	})
}
