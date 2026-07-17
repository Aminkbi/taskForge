package sim

import (
	"fmt"

	"github.com/aminkbi/taskforge"
)

type backend struct {
	budgetCapacity int
	budgetInUse    int
	leaseTTL       int64
	recoveryBound  int64

	tasks        map[string]*taskRecord
	taskOrder    []string
	ready        []string
	receipts     map[string]string
	active       map[string]*deliveryRecord
	deliveries   []*deliveryRecord
	budgetLease  map[string]int
	nextDelivery int

	leader          leaderRecord
	leaderEpoch     int64
	acceptedWrites  []acceptedWrite
	delayed         []string
	leaseRecoveries []*recoveryRecord
}

type taskRecord struct {
	id              string
	state           taskforge.State
	terminal        bool
	terminalState   taskforge.State
	terminalChanges int
	publishCount    int
	deliveryCount   int
}

type deliveryRecord struct {
	id         string
	taskID     string
	owner      string
	fence      int64
	leaseUntil int64
	active     bool
	acked      bool
}

type deliverySnapshot struct {
	id     string
	taskID string
	owner  string
	fence  int64
}

func (d *deliveryRecord) snapshot() deliverySnapshot {
	if d == nil {
		return deliverySnapshot{}
	}
	return deliverySnapshot{id: d.id, taskID: d.taskID, owner: d.owner, fence: d.fence}
}

type recoveryRecord struct {
	taskID      string
	expiredAt   int64
	deadline    int64
	recoveredAt int64
}

type fenceSnapshot = taskforge.LeadershipFence

type leaderRecord struct {
	fence      fenceSnapshot
	leaseUntil int64
}

type acceptedWrite struct {
	at       int64
	provided fenceSnapshot
	live     fenceSnapshot
}

func newBackend(budget int, leaseTTL, recoveryBound int64) *backend {
	return &backend{
		budgetCapacity: budget,
		leaseTTL:       leaseTTL,
		recoveryBound:  recoveryBound,
		tasks:          make(map[string]*taskRecord),
		receipts:       make(map[string]string),
		active:         make(map[string]*deliveryRecord),
		budgetLease:    make(map[string]int),
	}
}

func (b *backend) publish(taskID, receipt string, ready bool) (deduplicated bool) {
	if _, exists := b.receipts[receipt]; exists {
		return true
	}
	b.receipts[receipt] = taskID
	task, exists := b.tasks[taskID]
	if !exists {
		task = &taskRecord{id: taskID, state: taskforge.StateQueued}
		b.tasks[taskID] = task
		b.taskOrder = append(b.taskOrder, taskID)
	}
	task.publishCount++
	if ready {
		b.ready = append(b.ready, taskID)
	} else {
		b.delayed = append(b.delayed, taskID)
	}
	return false
}

func (b *backend) reserve(owner string, now int64) (*deliveryRecord, string) {
	if len(b.ready) == 0 {
		return nil, "no_task"
	}
	if b.budgetInUse >= b.budgetCapacity {
		return nil, "budget_blocked"
	}
	taskID := b.ready[0]
	b.ready = b.ready[1:]
	task := b.tasks[taskID]
	if task == nil || task.terminal {
		return nil, "skip_terminal"
	}
	b.nextDelivery++
	task.deliveryCount++
	delivery := &deliveryRecord{
		id:         fmt.Sprintf("d%03d", b.nextDelivery),
		taskID:     taskID,
		owner:      owner,
		fence:      int64(task.deliveryCount),
		leaseUntil: now + b.leaseTTL,
		active:     true,
	}
	b.deliveries = append(b.deliveries, delivery)
	b.active[taskID] = delivery
	b.budgetLease[delivery.id] = 1
	b.budgetInUse++
	task.state = taskforge.StateRunning
	for _, recovery := range b.leaseRecoveries {
		if recovery.taskID == taskID && recovery.recoveredAt == 0 {
			recovery.recoveredAt = now
		}
	}
	return delivery, "reserved"
}

func (b *backend) renew(snapshot deliverySnapshot, now int64) bool {
	delivery := b.validOwner(snapshot, now)
	if delivery == nil {
		return false
	}
	delivery.leaseUntil = now + b.leaseTTL
	return true
}

func (b *backend) ack(snapshot deliverySnapshot, now int64) bool {
	delivery := b.validOwner(snapshot, now)
	if delivery == nil {
		return false
	}
	task := b.tasks[delivery.taskID]
	if task == nil || task.terminal {
		return false
	}
	delivery.active = false
	delivery.acked = true
	delete(b.active, delivery.taskID)
	b.releaseBudget(delivery.id)
	task.state = taskforge.StateSucceeded
	task.terminal = true
	task.terminalState = taskforge.StateSucceeded
	task.terminalChanges++
	return true
}

func (b *backend) validOwner(snapshot deliverySnapshot, now int64) *deliveryRecord {
	delivery := b.active[snapshot.taskID]
	if delivery == nil || !delivery.active || now > delivery.leaseUntil {
		return nil
	}
	if delivery.id != snapshot.id || delivery.owner != snapshot.owner || delivery.fence != snapshot.fence {
		return nil
	}
	return delivery
}

func (b *backend) expire(now int64, note func(string, ...any)) {
	for _, taskID := range b.taskOrder {
		delivery := b.active[taskID]
		if delivery == nil || !delivery.active || now <= delivery.leaseUntil {
			continue
		}
		delivery.active = false
		delete(b.active, taskID)
		b.releaseBudget(delivery.id)
		task := b.tasks[taskID]
		if task != nil && !task.terminal {
			task.state = taskforge.StateQueued
			b.ready = append(b.ready, taskID)
			b.leaseRecoveries = append(b.leaseRecoveries, &recoveryRecord{
				taskID: taskID, expiredAt: now, deadline: now + b.recoveryBound,
			})
			note("expire %s/%s", taskID, delivery.id)
		}
	}
	if b.leader.fence.Valid() && now > b.leader.leaseUntil {
		note("leader_expire %s/e%d", b.leader.fence.Owner, b.leader.fence.Epoch)
		b.leader = leaderRecord{}
	}
}

func (b *backend) releaseBudget(deliveryID string) {
	tokens, exists := b.budgetLease[deliveryID]
	if !exists {
		return
	}
	delete(b.budgetLease, deliveryID)
	b.budgetInUse -= tokens
}

func (b *backend) ensureLeader(actor string, local fenceSnapshot, now, ttl int64) (fenceSnapshot, string) {
	if b.leader.fence.Valid() {
		if b.leader.fence.Token != local.Token || b.leader.fence.Owner != actor {
			return fenceSnapshot{}, "standby"
		}
		b.leader.leaseUntil = now + ttl
		return b.leader.fence, "renewed"
	}
	b.leaderEpoch++
	fence := fenceSnapshot{
		Owner: actor,
		Epoch: b.leaderEpoch,
		Token: fmt.Sprintf("%s|%d", actor, b.leaderEpoch),
	}
	b.leader = leaderRecord{fence: fence, leaseUntil: now + ttl}
	return fence, "acquired"
}

func (b *backend) writeDue(provided fenceSnapshot, now int64) (released string, accepted bool) {
	live := b.leader.fence
	if !provided.Valid() || !live.Valid() || provided != live || now > b.leader.leaseUntil {
		return "", false
	}
	b.acceptedWrites = append(b.acceptedWrites, acceptedWrite{at: now, provided: provided, live: live})
	if len(b.delayed) == 0 {
		return "", true
	}
	taskID := b.delayed[0]
	b.delayed = b.delayed[1:]
	// The receipt models the production delayed-release idempotency key.
	b.publish(taskID, "delayed:"+taskID, true)
	return taskID, true
}
