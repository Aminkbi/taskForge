package main

import (
	"reflect"
	"testing"

	"github.com/aminkbi/taskforge/internal/experiment"
)

func TestValidateSourceStatusRejectsTrackedAndUntrackedChanges(t *testing.T) {
	for _, status := range []string{" M cmd/experiment/main.go\n", "?? private-note.txt\n"} {
		if err := validateSourceStatus(status, false); err == nil {
			t.Fatalf("validateSourceStatus(%q, false) accepted dirty source", status)
		}
		if err := validateSourceStatus(status, true); err != nil {
			t.Fatalf("validateSourceStatus(%q, true) = %v", status, err)
		}
	}
	if err := validateSourceStatus("", false); err != nil {
		t.Fatalf("clean source rejected: %v", err)
	}
}

func TestRegisteredRunnerArgumentsAreExact(t *testing.T) {
	got := runnerArguments("noisy-neighbor", "taskforge-full", 20260717, 8, "localhost:6379", 14)
	want := experiment.ExpectedRunnerArguments("noisy-neighbor", "taskforge-full", 20260717)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runnerArguments() = %q, want %q", got, want)
	}
}

func TestCustomSeedsRequirePilot(t *testing.T) {
	if _, err := selectedSeeds(false, "1"); err == nil {
		t.Fatal("selectedSeeds accepted a custom publication seed")
	}
	seeds, err := selectedSeeds(true, "1,2")
	if err != nil || !reflect.DeepEqual(seeds, []int64{1, 2}) {
		t.Fatalf("selectedSeeds pilot = %v, %v", seeds, err)
	}
}
