// Package sim provides a bounded, deterministic model of TaskForge's core
// delivery and scheduler protocols. It is deliberately isolated from runtime
// packages: the simulator shares only canonical task states and leadership
// fences with production code.
package sim

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"
)

const (
	defaultHorizon       = 72
	defaultMaxEvents     = 512
	defaultTraceLimit    = 96
	defaultTaskCount     = 6
	defaultBudget        = 2
	defaultLeaseTTL      = 4
	defaultRecoveryBound = 24
)

// Config bounds one deterministic simulation run. Time values are logical
// ticks; no wall-clock time, goroutines, or external services are used.
type Config struct {
	Seed          uint64
	Horizon       int64
	MaxEvents     int
	TraceLimit    int
	TaskCount     int
	Budget        int
	LeaseTTL      int64
	RecoveryBound int64
}

// DefaultConfig returns the bounded configuration used by CI.
func DefaultConfig(seed uint64) Config {
	return Config{
		Seed:          seed,
		Horizon:       defaultHorizon,
		MaxEvents:     defaultMaxEvents,
		TraceLimit:    defaultTraceLimit,
		TaskCount:     defaultTaskCount,
		Budget:        defaultBudget,
		LeaseTTL:      defaultLeaseTTL,
		RecoveryBound: defaultRecoveryBound,
	}
}

// Result is stable for a fixed simulator version, configuration, and seed.
type Result struct {
	Seed        uint64
	Events      int
	Trace       string
	TraceSHA256 string
	Faults      []Fault
}

// HasFault reports whether the fault was injected during the run.
func (r Result) HasFault(want Fault) bool {
	for _, fault := range r.Faults {
		if fault == want {
			return true
		}
	}
	return false
}

// Violation is returned for an invariant failure. Error includes everything
// required to replay and diagnose the exact bounded schedule.
type Violation struct {
	Seed      uint64
	Invariant string
	Event     int
	Trace     string
}

func (v *Violation) Error() string {
	return fmt.Sprintf("simulation invariant failed: seed=%d event=%d invariant=%s\ntrace:\n%s", v.Seed, v.Event, v.Invariant, v.Trace)
}

// Fault names the injected failure modes covered by every bounded schedule.
type Fault string

const (
	FaultCrash            Fault = "crash"
	FaultPause            Fault = "pause"
	FaultDroppedRenewal   Fault = "dropped_renewal"
	FaultLateRenewal      Fault = "late_renewal"
	FaultPartition        Fault = "partition"
	FaultStaleAck         Fault = "stale_ack"
	FaultPublishAmbiguity Fault = "publish_ambiguity"
	FaultLeaderTurnover   Fault = "leader_turnover"
)

var allFaults = []Fault{
	FaultCrash,
	FaultPause,
	FaultDroppedRenewal,
	FaultLateRenewal,
	FaultPartition,
	FaultStaleAck,
	FaultPublishAmbiguity,
	FaultLeaderTurnover,
}

// RegressionSeed pins an interleaving that protects a protocol regression.
type RegressionSeed struct {
	Name        string
	Seed        uint64
	TraceSHA256 string
}

// RegressionSeeds are kept small enough to run on every CI invocation. Add a
// named seed here whenever a discovered failure is fixed.
var RegressionSeeds = []RegressionSeed{
	{
		Name:        "stale_ack_after_reclaim",
		Seed:        0x5eed0001,
		TraceSHA256: "cb5db4bf9af603f885c9ec2e9db2a55ff7afe3cf3a12d7bc5f6fa0b1acb24846",
	},
	{
		Name:        "ambiguous_publish_deduplicates",
		Seed:        0x5eed0002,
		TraceSHA256: "3d8d3e3d3b830b89cbb100f6442a37d6ba893f369a8b3040518413af75297a9e",
	},
	{
		Name:        "leader_turnover_fences_old_epoch",
		Seed:        0x5eed0003,
		TraceSHA256: "3695a8a333b6c977489ebf6068866d5331fb0a30ba287df1a8c7858090410682",
	},
}

// Run executes a complete deterministic schedule and checks invariants after
// every event.
func Run(cfg Config) (Result, error) {
	cfg = normalizeConfig(cfg)
	s := newSimulation(cfg)
	return s.run()
}

func normalizeConfig(cfg Config) Config {
	defaults := DefaultConfig(cfg.Seed)
	if cfg.Horizon <= 0 {
		cfg.Horizon = defaults.Horizon
	}
	if cfg.MaxEvents <= 0 {
		cfg.MaxEvents = defaults.MaxEvents
	}
	if cfg.TraceLimit <= 0 {
		cfg.TraceLimit = defaults.TraceLimit
	}
	if cfg.TaskCount <= 0 {
		cfg.TaskCount = defaults.TaskCount
	}
	if cfg.Budget <= 0 {
		cfg.Budget = defaults.Budget
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaults.LeaseTTL
	}
	if cfg.RecoveryBound <= 0 {
		cfg.RecoveryBound = defaults.RecoveryBound
	}
	return cfg
}

type simulation struct {
	cfg           Config
	clock         fakeClock
	rng           stableRNG
	events        []scheduledEvent
	nextSeq       uint64
	executed      int
	trace         traceBuffer
	current       []string
	backend       *backend
	workers       []*workerActor
	schedulers    []*schedulerActor
	faults        []Fault
	faultSeen     map[Fault]bool
	faultObserved map[Fault]bool
	turnover      turnoverState
}

type turnoverState struct {
	startedAt int64
	recovered bool
	oldFence  fenceSnapshot
}

func newSimulation(cfg Config) *simulation {
	s := &simulation{
		cfg:           cfg,
		clock:         newFakeClock(),
		rng:           newStableRNG(cfg.Seed),
		trace:         newTraceBuffer(cfg.TraceLimit),
		faultSeen:     make(map[Fault]bool, len(allFaults)),
		faultObserved: make(map[Fault]bool, len(allFaults)),
	}
	s.backend = newBackend(cfg.Budget, cfg.LeaseTTL, cfg.RecoveryBound)
	s.workers = []*workerActor{
		{id: "worker-1"},
		{id: "worker-2"},
	}
	s.schedulers = []*schedulerActor{
		{id: "scheduler-1"},
		{id: "scheduler-2"},
	}
	s.seedSchedule()
	return s
}

func (s *simulation) seedSchedule() {
	s.schedule(0, eventInit, "system", "")
	for tick := int64(1); tick <= s.cfg.Horizon; tick++ {
		for _, worker := range s.workers {
			s.schedule(tick, eventWorkerTick, worker.id, "")
		}
		for _, scheduler := range s.schedulers {
			s.schedule(tick, eventSchedulerTick, scheduler.id, "")
		}
	}

	// Fault windows are separated enough to make each fault observable, while
	// seeded jitter and tie keys vary actor/event interleavings.
	s.schedule(8+s.rng.int63n(3), eventCrash, "worker-1", "")
	s.schedule(11+s.rng.int63n(3), eventPause, "worker-2", "")
	s.schedule(15+s.rng.int63n(3), eventDropRenewal, "worker-1", "")
	s.schedule(18+s.rng.int63n(3), eventLateRenewal, "worker-2", "")
	s.schedule(22+s.rng.int63n(3), eventPartition, "worker-1", "")
	s.schedule(27+s.rng.int63n(3), eventStaleAck, "worker-2", "")
	s.schedule(31+s.rng.int63n(3), eventAmbiguousPublish, "publisher", "")
	s.schedule(37+s.rng.int63n(3), eventLeaderTurnover, "scheduler", "")
}

func (s *simulation) run() (Result, error) {
	for len(s.events) > 0 && s.executed < s.cfg.MaxEvents {
		event := s.popEvent()
		if event.at > s.cfg.Horizon {
			break
		}
		s.clock.advance(event.at)
		s.current = s.current[:0]
		s.backend.expire(s.clock.Tick(), s.note)
		s.execute(event)
		s.executed++
		s.trace.add(formatTraceLine(s.executed, event, s.current))
		if invariant := s.checkInvariants(false); invariant != "" {
			return Result{}, s.violation(invariant)
		}
	}

	if s.executed >= s.cfg.MaxEvents && len(s.events) > 0 {
		return Result{}, s.violation("schedule exceeded max_events")
	}
	if invariant := s.checkInvariants(true); invariant != "" {
		return Result{}, s.violation(invariant)
	}
	for _, fault := range allFaults {
		if !s.faultSeen[fault] {
			return Result{}, s.violation("fault was not injected: " + string(fault))
		}
		if !s.faultObserved[fault] {
			return Result{}, s.violation("fault had no observable effect: " + string(fault))
		}
	}

	trace := s.trace.String()
	digest := sha256.Sum256([]byte(trace))
	return Result{
		Seed:        s.cfg.Seed,
		Events:      s.executed,
		Trace:       trace,
		TraceSHA256: fmt.Sprintf("%x", digest),
		Faults:      append([]Fault(nil), s.faults...),
	}, nil
}

func (s *simulation) violation(invariant string) error {
	return &Violation{
		Seed:      s.cfg.Seed,
		Invariant: invariant,
		Event:     s.executed,
		Trace:     s.trace.String(),
	}
}

func (s *simulation) schedule(at int64, kind eventKind, actor, value string) {
	s.nextSeq++
	s.events = append(s.events, scheduledEvent{
		at:    at,
		tie:   s.rng.next(),
		seq:   s.nextSeq,
		kind:  kind,
		actor: actor,
		value: value,
	})
}

func (s *simulation) popEvent() scheduledEvent {
	best := 0
	for i := 1; i < len(s.events); i++ {
		if eventLess(s.events[i], s.events[best]) {
			best = i
		}
	}
	event := s.events[best]
	s.events[best] = s.events[len(s.events)-1]
	s.events = s.events[:len(s.events)-1]
	return event
}

func (s *simulation) note(format string, args ...any) {
	s.current = append(s.current, fmt.Sprintf(format, args...))
}

func (s *simulation) injected(fault Fault) {
	if !s.faultSeen[fault] {
		s.faultSeen[fault] = true
		s.faults = append(s.faults, fault)
	}
}

func (s *simulation) observed(fault Fault) {
	s.faultObserved[fault] = true
}

type eventKind string

const (
	eventInit               eventKind = "init"
	eventWorkerTick         eventKind = "worker_tick"
	eventSchedulerTick      eventKind = "scheduler_tick"
	eventCrash              eventKind = "crash"
	eventRestart            eventKind = "restart"
	eventPause              eventKind = "pause"
	eventResume             eventKind = "resume"
	eventDropRenewal        eventKind = "drop_renewal"
	eventLateRenewal        eventKind = "late_renewal"
	eventLateRenewalTry     eventKind = "late_renewal_try"
	eventPartition          eventKind = "partition"
	eventHeal               eventKind = "heal"
	eventStaleAck           eventKind = "stale_ack"
	eventAmbiguousPublish   eventKind = "ambiguous_publish"
	eventAmbiguousRetry     eventKind = "ambiguous_retry"
	eventLeaderTurnover     eventKind = "leader_turnover"
	eventStaleScheduleWrite eventKind = "stale_schedule_write"
)

type scheduledEvent struct {
	at    int64
	tie   uint64
	seq   uint64
	kind  eventKind
	actor string
	value string
}

func eventLess(left, right scheduledEvent) bool {
	if left.at != right.at {
		return left.at < right.at
	}
	if left.tie != right.tie {
		return left.tie < right.tie
	}
	return left.seq < right.seq
}

func formatTraceLine(number int, event scheduledEvent, notes []string) string {
	detail := "idle"
	if len(notes) > 0 {
		detail = strings.Join(notes, "; ")
	}
	return fmt.Sprintf("%03d@%03d %-11s %-20s %s", number, event.at, event.actor, event.kind, detail)
}

type traceBuffer struct {
	limit   int
	omitted int
	lines   []string
}

func newTraceBuffer(limit int) traceBuffer {
	return traceBuffer{limit: limit, lines: make([]string, 0, limit)}
}

func (t *traceBuffer) add(line string) {
	if len(t.lines) == t.limit {
		copy(t.lines, t.lines[1:])
		t.lines[len(t.lines)-1] = line
		t.omitted++
		return
	}
	t.lines = append(t.lines, line)
}

func (t *traceBuffer) String() string {
	lines := t.lines
	if t.omitted > 0 {
		lines = append([]string{fmt.Sprintf("... %d earlier events omitted ...", t.omitted)}, lines...)
	}
	return strings.Join(lines, "\n")
}

type stableRNG struct{ state uint64 }

func newStableRNG(seed uint64) stableRNG {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return stableRNG{state: seed}
}

// next is SplitMix64. Keeping the algorithm here makes event ordering stable
// across Go releases instead of depending on math/rand implementation details.
func (r *stableRNG) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *stableRNG) int63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return int64(r.next() % uint64(n))
}

type fakeClock struct {
	base time.Time
	tick int64
}

func newFakeClock() fakeClock {
	return fakeClock{base: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c fakeClock) Now() time.Time      { return c.base.Add(time.Duration(c.tick) * time.Second) }
func (c fakeClock) Tick() int64         { return c.tick }
func (c *fakeClock) advance(tick int64) { c.tick = tick }
