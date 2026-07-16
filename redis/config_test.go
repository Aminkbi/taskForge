package redis

import (
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
)

func TestOptionsFromConfigCompilesBrokerControls(t *testing.T) {
	t.Parallel()

	config := taskforge.Config{
		LeaseTTL: 20 * time.Second,
		WorkerPools: []taskforge.WorkerPoolConfig{{
			Name: "critical", Queue: "critical", Concurrency: 2,
			Fairness: &taskforge.FairnessConfig{Rules: []taskforge.FairnessRule{{
				Name: "vip", Keys: []string{"tenant-vip"}, ReservedConcurrency: 1, HardQuota: 1,
			}}},
			Admission: taskforge.AdmissionPolicy{
				Mode: taskforge.AdmissionDefer, MaxPending: 100, MaxPendingPerFairnessKey: 10,
			},
		}},
		DependencyBudgets: []taskforge.DependencyBudget{{Name: "external-api", Capacity: 4}},
		Retention:         &taskforge.RetentionPolicy{SucceededState: time.Hour},
	}
	options, err := OptionsFromConfig(Options{ReserveTimeout: 25 * time.Millisecond}, config)
	if err != nil {
		t.Fatalf("OptionsFromConfig() error = %v", err)
	}
	if options.LeaseTTL != 20*time.Second || options.ReserveTimeout != 25*time.Millisecond {
		t.Fatalf("unexpected broker timing options: %+v", options)
	}
	if options.DependencyBudgets["external-api"] != 4 || options.Retention.SucceededState != time.Hour {
		t.Fatalf("unexpected broker budget/retention options: %+v", options)
	}
	if got := options.FairnessPolicies["critical"].Resolve("tenant-vip"); got.Bucket != "vip" || got.HardQuota != 1 {
		t.Fatalf("unexpected compiled fairness rule: %+v", got)
	}
	if got := options.AdmissionPolicies["critical"]; got.Mode != AdmissionModeDefer || got.DeferInterval != 5*time.Second {
		t.Fatalf("unexpected compiled admission policy: %+v", got)
	}
}

func TestOptionsFromConfigRejectsInvalidModel(t *testing.T) {
	t.Parallel()

	_, err := OptionsFromConfig(Options{}, taskforge.Config{LeaseTTL: -time.Second})
	if err == nil {
		t.Fatal("OptionsFromConfig() error = nil, want validation error")
	}
}
