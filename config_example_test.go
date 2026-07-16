package taskforge_test

import (
	"fmt"
	"time"

	"github.com/aminkbi/taskforge"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
	"github.com/aminkbi/taskforge/worker"
)

func ExampleConfig() {
	config := taskforge.Config{
		WorkerPools: []taskforge.WorkerPoolConfig{{
			Name: "reports", Queue: "reports", Concurrency: 2, Prefetch: 4,
			TaskTimeout: 20 * time.Second,
			Fairness: &taskforge.FairnessConfig{Rules: []taskforge.FairnessRule{{
				Name: "vip", Keys: []string{"tenant-vip"}, ReservedConcurrency: 1, HardQuota: 1,
			}}},
			Admission: taskforge.AdmissionPolicy{
				Mode: taskforge.AdmissionDefer, MaxPending: 1_000, MaxPendingPerFairnessKey: 100,
			},
			Adaptive: taskforge.AdaptiveConcurrencyConfig{
				Enabled: true, MinConcurrency: 1, MaxConcurrency: 4,
			},
		}},
		DependencyBudgets: []taskforge.DependencyBudget{{Name: "reporting-api", Capacity: 4}},
		TaskBudgets:       []taskforge.TaskBudget{{TaskName: "reports.generate", Budget: "reporting-api"}},
	}

	brokerOptions, err := taskforgeredis.OptionsFromConfig(taskforgeredis.Options{Addr: "localhost:6379"}, config)
	if err != nil {
		panic(err)
	}
	workerOptions, err := worker.OptionsFromConfig(worker.Options{}, config, "reports")
	if err != nil {
		panic(err)
	}
	fmt.Println(brokerOptions.DependencyBudgets["reporting-api"], workerOptions.TaskTimeout)
	// Output: 4 20s
}
