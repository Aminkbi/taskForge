package modelcheck

import (
	"fmt"

	"github.com/aminkbi/taskforge"
)

const (
	schedulerMaxTime  = 3
	schedulerMaxEpoch = 2
)

type schedulerModel struct {
	mutation Mutation
}

type schedulerState struct {
	Now               uint8
	EpochCounter      uint8
	LiveOwner         uint8
	LiveEpoch         uint8
	Expires           uint8
	Renewed           bool
	LocalA            uint8
	LocalB            uint8
	OldA              uint8
	OldB              uint8
	WriteTried        uint8
	Released          bool
	ReleaseCount      uint8
	LastAcceptedEpoch uint8
	StaleAccepted     bool
}

func newSchedulerModel(mutation Mutation) *schedulerModel {
	return &schedulerModel{mutation: mutation}
}

func (m *schedulerModel) name() Model { return SchedulerModel }

func (m *schedulerModel) initial() state { return schedulerState{} }

func (s schedulerState) key() string {
	return fmt.Sprintf("%d|%d|%d|%d|%d|%t|%d|%d|%d|%d|%d|%t|%d|%d|%t",
		s.Now, s.EpochCounter, s.LiveOwner, s.LiveEpoch, s.Expires, s.Renewed,
		s.LocalA, s.LocalB, s.OldA, s.OldB, s.WriteTried, s.Released,
		s.ReleaseCount, s.LastAcceptedEpoch, s.StaleAccepted)
}

func (s schedulerState) describe() string {
	return fmt.Sprintf("now=%d issued_epoch=%d live=o%d/e%d/expires%d locals=(a:e%d,b:e%d) old=(a:e%d,b:e%d) released=%t release_count=%d last_accepted_epoch=%d stale_accepted=%t",
		s.Now, s.EpochCounter, s.LiveOwner, s.LiveEpoch, s.Expires, s.LocalA, s.LocalB,
		s.OldA, s.OldB, s.Released, s.ReleaseCount, s.LastAcceptedEpoch, s.StaleAccepted)
}

func (m *schedulerModel) terminal(raw state) bool { return raw.(schedulerState).Released }

func (m *schedulerModel) invariant(raw state) string {
	s := raw.(schedulerState)
	if s.StaleAccepted {
		return "scheduler/stale-epoch-write-accepted"
	}
	if s.LiveOwner == 0 && s.LiveEpoch != 0 {
		return "scheduler/absent-leader-has-epoch"
	}
	if s.LiveOwner != 0 && s.LiveEpoch != s.EpochCounter {
		return "scheduler/live-epoch-not-latest-issued"
	}
	if s.LastAcceptedEpoch > s.EpochCounter {
		return "scheduler/accepted-unissued-epoch"
	}
	if s.ReleaseCount > 1 {
		return "scheduler/terminal-release-not-idempotent"
	}
	if s.Released && s.ReleaseCount != 1 {
		return "scheduler/terminal-state-not-monotonic"
	}
	return ""
}

func (m *schedulerModel) next(raw state) []transition {
	s := raw.(schedulerState)
	if s.Released {
		return nil
	}
	var out []transition
	add := func(action string, next schedulerState) {
		out = append(out, transition{action: action, next: next})
	}

	if s.LiveOwner == 0 && s.EpochCounter < schedulerMaxEpoch {
		for owner := uint8(1); owner <= 2; owner++ {
			next := s
			next.EpochCounter++
			next.LiveOwner = owner
			next.LiveEpoch = next.EpochCounter
			next.Expires = next.Now + 1
			next.Renewed = false
			next.WriteTried = 0
			if owner == 1 {
				next.OldA = next.LocalA
				next.LocalA = next.LiveEpoch
			} else {
				next.OldB = next.LocalB
				next.LocalB = next.LiveEpoch
			}
			add(fmt.Sprintf("Acquire(owner=%d,epoch=%d)", owner, next.LiveEpoch), next)
		}
	}

	if s.Now < schedulerMaxTime {
		wouldLoseLastEpoch := s.LiveOwner != 0 && s.EpochCounter >= schedulerMaxEpoch && s.Now+1 >= s.Expires
		if !wouldLoseLastEpoch {
			next := s
			next.Now++
			add("Tick", next)
		}
	}

	if s.LiveOwner != 0 && s.Now < s.Expires && !s.Renewed && s.Expires <= schedulerMaxTime {
		next := s
		next.Expires++
		next.Renewed = true
		add(fmt.Sprintf("Renew(owner=%d,epoch=%d)", s.LiveOwner, s.LiveEpoch), next)
	}

	if s.LiveOwner != 0 && s.Now >= s.Expires {
		next := s
		next.LiveOwner = 0
		next.LiveEpoch = 0
		next.Expires = 0
		next.Renewed = false
		add("ExpireLeadership", next)
	}

	for owner := uint8(1); owner <= 2; owner++ {
		local := s.local(owner)
		if local != 0 {
			bit := uint8(1 << (owner - 1))
			if s.WriteTried&bit == 0 {
				next := s
				next.WriteTried |= bit
				accepted := s.accepts(owner, local, m.mutation)
				if accepted {
					next.Released = true
					next.ReleaseCount++
					next.LastAcceptedEpoch = local
				}
				result := "rejected"
				if accepted {
					result = "accepted"
				}
				add(fmt.Sprintf("Write(local_owner=%d,epoch=%d,%s)", owner, local, result), next)
			}
		}

		old := s.old(owner)
		if old != 0 {
			bit := uint8(1 << (owner + 1))
			if s.WriteTried&bit == 0 {
				next := s
				next.WriteTried |= bit
				accepted := s.accepts(owner, old, m.mutation)
				if accepted {
					next.StaleAccepted = true
					next.Released = true
					next.ReleaseCount++
					next.LastAcceptedEpoch = old
				}
				result := "rejected"
				if accepted {
					result = "ACCEPTED_DEFECT"
				}
				add(fmt.Sprintf("StaleWrite(owner=%d,old_epoch=%d,%s)", owner, old, result), next)
			}
		}
	}
	return out
}

func (s schedulerState) local(owner uint8) uint8 {
	if owner == 1 {
		return s.LocalA
	}
	return s.LocalB
}

func (s schedulerState) old(owner uint8) uint8 {
	if owner == 1 {
		return s.OldA
	}
	return s.OldB
}

func (s schedulerState) accepts(owner, epoch uint8, mutation Mutation) bool {
	if s.LiveOwner == 0 || s.Now >= s.Expires {
		return false
	}
	provided := taskforge.LeadershipFence{Owner: fmt.Sprintf("scheduler-%d", owner), Epoch: int64(epoch), Token: fmt.Sprintf("scheduler-%d|%d", owner, epoch)}
	live := taskforge.LeadershipFence{Owner: fmt.Sprintf("scheduler-%d", s.LiveOwner), Epoch: int64(s.LiveEpoch), Token: fmt.Sprintf("scheduler-%d|%d", s.LiveOwner, s.LiveEpoch)}
	if mutation == SchedulerOwnerOnly {
		return provided.Owner == live.Owner
	}
	return provided == live
}
