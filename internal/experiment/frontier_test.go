package experiment

import (
	"testing"
	"time"
)

func TestEvaluateFrontierTargetEnforcesMedianLossAndCounterbalance(t *testing.T) {
	t.Parallel()

	results := []OpenLoopResult{
		frontierResult(FrontierFIFOSystem, 0, 900), frontierResult(FrontierAsynqSystem, 1, 1000),
		frontierResult(FrontierAsynqSystem, 0, 1000), frontierResult(FrontierFIFOSystem, 1, 900),
	}
	target, err := EvaluateFrontierTarget(results, .15)
	if err != nil {
		t.Fatal(err)
	}
	if !target.TargetMet || target.ObservedThroughputLoss < .099 || target.ObservedThroughputLoss > .101 {
		t.Fatalf("target = %+v", target)
	}
	if _, err := EvaluateFrontierTarget(results[:3], .15); err == nil {
		t.Fatal("unpaired results passed target enforcement")
	}
	results[0].Tasks[0].Outcome = "retry"
	if _, err := EvaluateFrontierTarget(results, .15); err == nil {
		t.Fatal("incomplete result passed target enforcement")
	}
}

func frontierResult(system string, order, tasksPerSecond int) OpenLoopResult {
	start := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	tasks := make([]TaskObservation, tasksPerSecond)
	enqueues := make([]EnqueueObservation, tasksPerSecond)
	for index := range tasks {
		completedAt := start.Add(time.Duration(index) * time.Second / time.Duration(tasksPerSecond-1))
		tasks[index] = TaskObservation{TaskID: string(rune(index + 1)), Outcome: "completed", CompletedAt: completedAt}
		enqueues[index] = EnqueueObservation{TaskID: tasks[index].TaskID, Disposition: EnqueueAccepted}
	}
	return OpenLoopResult{System: system, SystemOrder: order, TraceSHA256: "same", Tasks: tasks, Enqueues: enqueues}
}
