package sim

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aminkbi/taskforge/internal/clock"
)

func TestBoundedSchedules(t *testing.T) {
	seeds := make([]uint64, 0, 131)
	for seed := uint64(1); seed <= 128; seed++ {
		seeds = append(seeds, seed)
	}
	for _, regression := range RegressionSeeds {
		seeds = append(seeds, regression.Seed)
	}

	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			result, err := Run(DefaultConfig(seed))
			if err != nil {
				t.Fatal(err)
			}
			if result.Events > defaultMaxEvents {
				t.Fatalf("events = %d, want <= %d", result.Events, defaultMaxEvents)
			}
			for _, fault := range allFaults {
				if !result.HasFault(fault) {
					t.Errorf("fault %q was not injected", fault)
				}
			}
		})
	}
}

func TestFixedSeedIsByteStable(t *testing.T) {
	const seed = uint64(0x5eedcafe)
	const wantSHA256 = "951bb9c35b162c50381d7a3a2af31d45fd1d688b608d9b1a36fde2ab2d2e9c65"
	first, err := Run(DefaultConfig(seed))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(DefaultConfig(seed))
	if err != nil {
		t.Fatal(err)
	}
	if first.Trace != second.Trace {
		t.Fatalf("fixed seed produced different traces\nfirst:\n%s\nsecond:\n%s", first.Trace, second.Trace)
	}
	if first.TraceSHA256 != second.TraceSHA256 {
		t.Fatalf("fixed seed produced different digests: %s != %s", first.TraceSHA256, second.TraceSHA256)
	}
	if first.TraceSHA256 != wantSHA256 {
		t.Fatalf("trace digest = %s, want stable digest %s", first.TraceSHA256, wantSHA256)
	}
}

func TestNamedRegressionSeeds(t *testing.T) {
	for _, regression := range RegressionSeeds {
		regression := regression
		t.Run(regression.Name, func(t *testing.T) {
			result, err := Run(DefaultConfig(regression.Seed))
			if err != nil {
				t.Fatal(err)
			}
			if result.Seed != regression.Seed {
				t.Fatalf("seed = %d, want %d", result.Seed, regression.Seed)
			}
			if result.TraceSHA256 != regression.TraceSHA256 {
				t.Fatalf("trace digest = %s, want %s", result.TraceSHA256, regression.TraceSHA256)
			}
		})
	}
}

func TestViolationPrintsReplaySeedAndCompactTrace(t *testing.T) {
	trace := newTraceBuffer(2)
	trace.add("001@001 system      init                 ready")
	trace.add("002@002 worker-1    worker_tick          reserve task-01/d001/f1")
	violation := (&simulation{
		cfg:      DefaultConfig(4242),
		executed: 2,
		trace:    trace,
	}).violation("ownership: stale ack accepted")

	var typed *Violation
	if !errors.As(violation, &typed) {
		t.Fatalf("error type = %T, want *Violation", violation)
	}
	for _, want := range []string{"seed=4242", "event=2", "ownership: stale ack accepted", "trace:", "task-01/d001/f1"} {
		if !strings.Contains(violation.Error(), want) {
			t.Errorf("violation missing %q:\n%s", want, violation)
		}
	}
}

func TestFailingScheduleReplaysByteStable(t *testing.T) {
	cfg := DefaultConfig(8080)
	cfg.MaxEvents = 4
	_, first := Run(cfg)
	_, second := Run(cfg)
	if first == nil || second == nil {
		t.Fatal("bounded schedule unexpectedly succeeded")
	}
	if first.Error() != second.Error() {
		t.Fatalf("same failing seed produced different diagnostics\nfirst:\n%s\nsecond:\n%s", first, second)
	}
	for _, want := range []string{"seed=8080", "schedule exceeded max_events", "trace:"} {
		if !strings.Contains(first.Error(), want) {
			t.Errorf("failure missing %q:\n%s", want, first)
		}
	}
}

func TestReplaySeed(t *testing.T) {
	seedText := os.Getenv("TASKFORGE_SIM_SEED")
	if seedText == "" {
		t.Skip("set TASKFORGE_SIM_SEED to replay one schedule")
	}
	var seed uint64
	if _, err := fmt.Sscanf(seedText, "%d", &seed); err != nil {
		t.Fatalf("parse TASKFORGE_SIM_SEED=%q: %v", seedText, err)
	}
	result, err := Run(DefaultConfig(seed))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("seed=%d events=%d sha256=%s\n%s", result.Seed, result.Events, result.TraceSHA256, result.Trace)
}

func TestFakeClockImplementsRuntimeClockBoundary(t *testing.T) {
	var runtimeClock clock.Clock = newFakeClock()
	if got := runtimeClock.Now(); got.Location().String() != "UTC" {
		t.Fatalf("clock location = %s, want UTC", got.Location())
	}
}
