package sim

import (
	"fmt"
	"strconv"
	"strings"
)

type workerActor struct {
	id          string
	held        deliverySnapshot
	finishAt    int64
	nextRenewAt int64
	crashed     bool
	paused      bool
	partitioned bool
	dropNext    bool
}

type schedulerActor struct {
	id          string
	local       fenceSnapshot
	partitioned bool
}

func (s *simulation) execute(event scheduledEvent) {
	now := s.clock.Tick()
	switch event.kind {
	case eventInit:
		for i := 1; i <= s.cfg.TaskCount; i++ {
			taskID := fmt.Sprintf("task-%02d", i)
			s.backend.publish(taskID, "new:"+taskID, true)
		}
		s.backend.publish("due-01", "schedule:due-01", false)
		s.note("tasks=%d delayed=1 budget=%d", s.cfg.TaskCount, s.cfg.Budget)
	case eventWorkerTick:
		s.workerTick(s.worker(event.actor), now)
	case eventSchedulerTick:
		s.schedulerTick(s.scheduler(event.actor), now)
	case eventCrash:
		worker := s.worker(event.actor)
		worker.crashed = true
		if worker.held.id != "" {
			s.note("lost %s/%s", worker.held.taskID, worker.held.id)
			worker.held = deliverySnapshot{}
			s.observed(FaultCrash)
		}
		s.schedule(now+7, eventRestart, event.actor, "")
		s.injected(FaultCrash)
		s.note("injected")
	case eventRestart:
		s.worker(event.actor).crashed = false
		s.note("restarted")
	case eventPause:
		worker := s.worker(event.actor)
		worker.paused = true
		if worker.held.id != "" {
			s.observed(FaultPause)
		}
		s.schedule(now+6, eventResume, event.actor, "")
		s.injected(FaultPause)
		s.note("injected")
	case eventResume:
		s.worker(event.actor).paused = false
		s.note("resumed")
	case eventDropRenewal:
		s.worker(event.actor).dropNext = true
		s.injected(FaultDroppedRenewal)
		s.note("next renewal dropped")
	case eventLateRenewal:
		s.injected(FaultLateRenewal)
		snapshot, leaseUntil := s.pickDeliveryForLateOperation()
		if snapshot.id == "" {
			s.note("no delivery")
			break
		}
		value := encodeDelivery(snapshot)
		s.schedule(max(now+1, leaseUntil+1), eventLateRenewalTry, event.actor, value)
		s.note("captured %s/%s", snapshot.taskID, snapshot.id)
	case eventLateRenewalTry:
		snapshot := decodeDelivery(event.value)
		if s.backend.renew(snapshot, now) {
			s.note("unexpected_accept %s/%s", snapshot.taskID, snapshot.id)
		} else {
			s.note("rejected %s/%s", snapshot.taskID, snapshot.id)
			s.observed(FaultLateRenewal)
		}
	case eventPartition:
		s.worker(event.actor).partitioned = true
		s.schedule(now+6, eventHeal, event.actor, "")
		s.injected(FaultPartition)
		s.note("injected")
	case eventHeal:
		if worker := s.workerOrNil(event.actor); worker != nil {
			worker.partitioned = false
			s.note("healed")
			break
		}
		if scheduler := s.schedulerOrNil(event.actor); scheduler != nil {
			scheduler.partitioned = false
			s.note("healed")
		}
	case eventStaleAck:
		s.injected(FaultStaleAck)
		snapshot := s.pickStaleDelivery()
		if snapshot.id == "" {
			s.note("no stale delivery")
			break
		}
		if s.backend.ack(snapshot, now) {
			s.note("unexpected_accept %s/%s", snapshot.taskID, snapshot.id)
		} else {
			s.note("rejected %s/%s", snapshot.taskID, snapshot.id)
			s.observed(FaultStaleAck)
		}
	case eventAmbiguousPublish:
		s.injected(FaultPublishAmbiguity)
		s.backend.publish("ambiguous-01", "new:ambiguous-01", true)
		s.schedule(now+1, eventAmbiguousRetry, event.actor, "")
		s.note("committed reply_lost")
	case eventAmbiguousRetry:
		deduplicated := s.backend.publish("ambiguous-01", "new:ambiguous-01", true)
		if deduplicated {
			s.observed(FaultPublishAmbiguity)
		}
		s.note("retry deduplicated=%t", deduplicated)
	case eventLeaderTurnover:
		s.leaderTurnover(now)
	case eventStaleScheduleWrite:
		_, accepted := s.backend.writeDue(s.turnover.oldFence, now)
		if accepted {
			s.note("unexpected_accept %s/e%d", s.turnover.oldFence.Owner, s.turnover.oldFence.Epoch)
		} else {
			s.note("rejected %s/e%d", s.turnover.oldFence.Owner, s.turnover.oldFence.Epoch)
			s.observed(FaultLeaderTurnover)
		}
	}
}

func (s *simulation) workerTick(worker *workerActor, now int64) {
	switch {
	case worker.crashed:
		s.note("crashed")
		return
	case worker.paused:
		s.observed(FaultPause)
		s.note("paused")
		return
	case worker.partitioned:
		s.observed(FaultPartition)
		s.note("partitioned")
		return
	}

	if worker.held.id != "" && s.backend.validOwner(worker.held, now) == nil {
		s.note("lease_lost %s/%s", worker.held.taskID, worker.held.id)
		worker.held = deliverySnapshot{}
	}
	if worker.held.id == "" {
		delivery, outcome := s.backend.reserve(worker.id, now)
		if delivery == nil {
			s.note("%s", outcome)
			return
		}
		worker.held = delivery.snapshot()
		worker.finishAt = now + 5
		worker.nextRenewAt = now + 2
		s.note("reserve %s/%s/f%d", delivery.taskID, delivery.id, delivery.fence)
		return
	}

	if now >= worker.nextRenewAt {
		if worker.dropNext {
			worker.dropNext = false
			worker.nextRenewAt = now + s.cfg.LeaseTTL + 1
			s.note("renew_drop %s", worker.held.id)
			s.observed(FaultDroppedRenewal)
		} else if s.backend.renew(worker.held, now) {
			worker.nextRenewAt = now + 2
			s.note("renew %s", worker.held.id)
		} else {
			s.note("renew_reject %s", worker.held.id)
			worker.held = deliverySnapshot{}
			return
		}
	}
	if now >= worker.finishAt && worker.held.id != "" {
		if s.backend.ack(worker.held, now) {
			s.note("ack %s/%s", worker.held.taskID, worker.held.id)
		} else {
			s.note("ack_reject %s/%s", worker.held.taskID, worker.held.id)
		}
		worker.held = deliverySnapshot{}
	}
}

func (s *simulation) schedulerTick(actor *schedulerActor, now int64) {
	if actor.partitioned {
		s.note("partitioned")
		return
	}
	fence, outcome := s.backend.ensureLeader(actor.id, actor.local, now, 4)
	if fence.Valid() {
		actor.local = fence
	} else if outcome == "standby" && actor.local.Valid() && s.backend.leader.fence.Token != actor.local.Token {
		s.note("demote e%d", actor.local.Epoch)
		actor.local = fenceSnapshot{}
	}
	s.note("%s", outcome)
	if !actor.local.Valid() {
		return
	}
	released, accepted := s.backend.writeDue(actor.local, now)
	if !accepted {
		s.note("write_rejected e%d", actor.local.Epoch)
		actor.local = fenceSnapshot{}
		return
	}
	if released != "" {
		s.note("release %s/e%d", released, actor.local.Epoch)
	}
	if s.turnover.startedAt > 0 && actor.local.Epoch > s.turnover.oldFence.Epoch {
		s.turnover.recovered = true
	}
}

func (s *simulation) leaderTurnover(now int64) {
	s.injected(FaultLeaderTurnover)
	live := s.backend.leader.fence
	if !live.Valid() {
		// A scheduler tick will establish leadership before this event in normal
		// schedules; keep the fault deterministic if custom bounds are used.
		live, _ = s.backend.ensureLeader(s.schedulers[0].id, fenceSnapshot{}, now, 4)
		s.schedulers[0].local = live
	}
	leader := s.scheduler(live.Owner)
	leader.partitioned = true
	s.turnover = turnoverState{startedAt: now, oldFence: live}
	s.schedule(now+7, eventHeal, leader.id, "")
	s.schedule(now+6, eventStaleScheduleWrite, leader.id, "")
	s.note("partition %s/e%d", live.Owner, live.Epoch)
}

func (s *simulation) worker(id string) *workerActor {
	if worker := s.workerOrNil(id); worker != nil {
		return worker
	}
	panic("unknown worker: " + id)
}

func (s *simulation) workerOrNil(id string) *workerActor {
	for _, worker := range s.workers {
		if worker.id == id {
			return worker
		}
	}
	return nil
}

func (s *simulation) scheduler(id string) *schedulerActor {
	if scheduler := s.schedulerOrNil(id); scheduler != nil {
		return scheduler
	}
	panic("unknown scheduler: " + id)
}

func (s *simulation) schedulerOrNil(id string) *schedulerActor {
	for _, scheduler := range s.schedulers {
		if scheduler.id == id {
			return scheduler
		}
	}
	return nil
}

func (s *simulation) pickStaleDelivery() deliverySnapshot {
	for _, delivery := range s.backend.deliveries {
		if !delivery.active {
			return delivery.snapshot()
		}
	}
	return deliverySnapshot{}
}

func (s *simulation) pickDeliveryForLateOperation() (deliverySnapshot, int64) {
	for _, delivery := range s.backend.deliveries {
		if !delivery.active {
			return delivery.snapshot(), delivery.leaseUntil
		}
	}
	for _, delivery := range s.backend.deliveries {
		if delivery.active {
			return delivery.snapshot(), delivery.leaseUntil
		}
	}
	return deliverySnapshot{}, 0
}

func encodeDelivery(snapshot deliverySnapshot) string {
	return fmt.Sprintf("%s,%s,%s,%d", snapshot.id, snapshot.taskID, snapshot.owner, snapshot.fence)
}

func decodeDelivery(value string) deliverySnapshot {
	parts := strings.Split(value, ",")
	if len(parts) == 4 {
		fence, _ := strconv.ParseInt(parts[3], 10, 64)
		return deliverySnapshot{id: parts[0], taskID: parts[1], owner: parts[2], fence: fence}
	}
	return deliverySnapshot{}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
