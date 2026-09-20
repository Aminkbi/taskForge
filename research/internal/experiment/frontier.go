package experiment

import (
	"fmt"
	"slices"
	"time"
)

const (
	FrontierFIFOSystem  = "taskforge-fifo-static"
	FrontierAsynqSystem = "asynq"
)

type FrontierTarget struct {
	MaxThroughputLoss       float64              `json:"max_throughput_loss"`
	TaskForgeMedianPerSec   float64              `json:"taskforge_median_per_second"`
	AsynqMedianPerSec       float64              `json:"asynq_median_per_second"`
	ObservedThroughputLoss  float64              `json:"observed_throughput_loss"`
	TargetMet               bool                 `json:"target_met"`
	SamplesPerSystem        int                  `json:"samples_per_system"`
	SystemThroughputsPerSec map[string][]float64 `json:"system_throughputs_per_second"`
}

func EvaluateFrontierTarget(results []OpenLoopResult, maxLoss float64) (FrontierTarget, error) {
	if maxLoss < 0 || maxLoss >= 1 {
		return FrontierTarget{}, fmt.Errorf("max throughput loss must be in [0,1)")
	}
	throughputs := map[string][]float64{FrontierFIFOSystem: {}, FrontierAsynqSystem: {}}
	traceDigest := ""
	positions := map[string]map[int]int{FrontierFIFOSystem: {}, FrontierAsynqSystem: {}}
	for _, result := range results {
		if result.System != FrontierFIFOSystem && result.System != FrontierAsynqSystem {
			continue
		}
		if result.Excluded {
			return FrontierTarget{}, fmt.Errorf("%s result is excluded: %s", result.System, result.ExcludeReason)
		}
		if traceDigest == "" {
			traceDigest = result.TraceSHA256
		} else if result.TraceSHA256 != traceDigest {
			return FrontierTarget{}, fmt.Errorf("mixed frontier trace digests")
		}
		rate, err := completedThroughput(result)
		if err != nil {
			return FrontierTarget{}, fmt.Errorf("%s: %w", result.System, err)
		}
		throughputs[result.System] = append(throughputs[result.System], rate)
		positions[result.System][result.SystemOrder]++
	}
	if len(throughputs[FrontierFIFOSystem]) < 2 || len(throughputs[FrontierFIFOSystem]) != len(throughputs[FrontierAsynqSystem]) {
		return FrontierTarget{}, fmt.Errorf("frontier target requires equal repeated samples for both systems")
	}
	if !equalPositionCounts(positions[FrontierFIFOSystem], positions[FrontierAsynqSystem]) {
		return FrontierTarget{}, fmt.Errorf("frontier target requires counterbalanced system positions")
	}
	taskforgeMedian := medianFloat(throughputs[FrontierFIFOSystem])
	asynqMedian := medianFloat(throughputs[FrontierAsynqSystem])
	if asynqMedian <= 0 {
		return FrontierTarget{}, fmt.Errorf("asynq median throughput must be positive")
	}
	loss := 1 - taskforgeMedian/asynqMedian
	return FrontierTarget{
		MaxThroughputLoss: maxLoss, TaskForgeMedianPerSec: taskforgeMedian, AsynqMedianPerSec: asynqMedian,
		ObservedThroughputLoss: loss, TargetMet: loss <= maxLoss, SamplesPerSystem: len(throughputs[FrontierFIFOSystem]),
		SystemThroughputsPerSec: throughputs,
	}, nil
}

func completedThroughput(result OpenLoopResult) (float64, error) {
	completed := 0
	var first, last time.Time
	for _, task := range result.Tasks {
		if task.Outcome != "completed" || task.CompletedAt.IsZero() {
			return 0, fmt.Errorf("task %q did not complete", task.TaskID)
		}
		completed++
		if first.IsZero() || task.CompletedAt.Before(first) {
			first = task.CompletedAt
		}
		if last.IsZero() || task.CompletedAt.After(last) {
			last = task.CompletedAt
		}
	}
	if completed == 0 || !last.After(first) {
		return 0, fmt.Errorf("insufficient completion interval")
	}
	for _, enqueue := range result.Enqueues {
		if enqueue.Disposition == EnqueueRejected || enqueue.Error != "" {
			return 0, fmt.Errorf("enqueue %q was not accepted", enqueue.TaskID)
		}
	}
	return float64(completed) / last.Sub(first).Seconds(), nil
}

func equalPositionCounts(left, right map[int]int) bool {
	if len(left) != len(right) {
		return false
	}
	for position, count := range left {
		if right[position] != count {
			return false
		}
	}
	return true
}

func medianFloat(values []float64) float64 {
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	middle := len(ordered) / 2
	if len(ordered)%2 == 1 {
		return ordered[middle]
	}
	return (ordered[middle-1] + ordered[middle]) / 2
}
