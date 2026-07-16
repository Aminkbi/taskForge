package taskforge

import "time"

type QueueMetricsSnapshot struct {
	Depth     float64
	Reserved  float64
	Consumers float64
}

type FairnessMetricsSnapshot struct {
	Bucket         string
	Depth          float64
	Reserved       float64
	OldestReadyAge float64
	Weight         float64
}

type AdmissionStatusSnapshot struct {
	Queue              string
	Mode               string
	State              string
	Reason             string
	QueuePending       float64
	FairnessKeyPending float64
	OldestReadyAge     float64
	RetryBacklog       float64
	DeadLetterSize     float64
	DeferInterval      time.Duration
	UpdatedAt          time.Time
}

type DependencyBudgetUsageSnapshot struct {
	Budget   string
	Capacity float64
	InUse    float64
}

type AdaptivePoolSnapshot struct {
	Pool                  string
	Queue                 string
	AdaptiveEnabled       bool
	ConfiguredConcurrency float64
	EffectiveConcurrency  float64
	MinConcurrency        float64
	MaxConcurrency        float64
	AvgLatencySeconds     float64
	ErrorRate             float64
	BudgetBlocked         float64
	Backlog               float64
	HealthyWindows        float64
	LastAdjustmentAction  string
	LastAdjustmentReason  string
	LastAdjustedAt        time.Time
}

type WorkerLifecycleSnapshot struct {
	WorkerID            string
	Pool                string
	Queue               string
	State               string
	Pending             float64
	Running             float64
	DrainStartedAt      time.Time
	DrainDeadline       time.Time
	LastShutdownOutcome string
	AbandonedDeliveries float64
	DrainLeaseLosses    float64
	UpdatedAt           time.Time
}

type SchedulerLeadershipSnapshot struct {
	Leader        bool
	Owner         string
	Epoch         float64
	LastRenewedAt time.Time
}

type LeadershipFence struct {
	Owner string `json:"owner"`
	Epoch int64  `json:"epoch"`
	Token string `json:"token"`
}

func (f LeadershipFence) Valid() bool { return f.Owner != "" && f.Epoch > 0 && f.Token != "" }
