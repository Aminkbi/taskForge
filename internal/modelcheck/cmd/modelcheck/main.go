package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aminkbi/taskforge/internal/modelcheck"
)

func main() {
	modelName := flag.String("model", "all", "model to check: all, delivery, or scheduler")
	mutationName := flag.String("mutation", "", "intentional defect: delivery-id-only, retry-without-receipt, or scheduler-owner-only")
	maxDepth := flag.Int("max-depth", modelcheck.SmokeBounds().MaxDepth, "maximum transition depth")
	maxStates := flag.Int("max-states", modelcheck.SmokeBounds().MaxStates, "maximum unique states")
	flag.Parse()

	bounds := modelcheck.Bounds{MaxDepth: *maxDepth, MaxStates: *maxStates}
	mutation := modelcheck.Mutation(*mutationName)
	if *modelName == "all" {
		if mutation != modelcheck.NoMutation {
			fatalf("-mutation requires one -model")
		}
		reports, err := modelcheck.CheckAll(bounds)
		if err != nil {
			fatalf("%v", err)
		}
		for _, report := range reports {
			printReport(report)
		}
		return
	}

	report, err := modelcheck.Check(modelcheck.Model(*modelName), mutation, bounds)
	if err != nil {
		fatalf("%v", err)
	}
	printReport(report)
}

func printReport(report modelcheck.Report) {
	mutation := string(report.Mutation)
	if mutation == "" {
		mutation = "correct"
	}
	fmt.Printf("PASS model=%s mutation=%s states=%d transitions=%d max_depth=%d\n",
		report.Model, mutation, report.States, report.Transitions, report.MaxDepth)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
