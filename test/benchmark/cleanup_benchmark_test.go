package benchmark

import (
	"fmt"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
)

func BenchmarkSnapshotCosts(b *testing.B) {
	for _, tenants := range []int{1, 16, 64} {
		for _, size := range []int{256, 65536} {
			for _, operation := range []string{"metrics", "admission_age"} {
				b.Run(fmt.Sprintf("%s/tenants_%d/payload_%d", operation, tenants, size), func(b *testing.B) {
					policy, err := taskforgeredis.NewFairnessPolicy(taskforgeredis.FairnessRule{}, nil)
					if err != nil {
						b.Fatal(err)
					}
					env := newBenchEnvWithOptions(b, time.Minute, taskforgeredis.Options{
						FairnessPolicies: map[string]*taskforgeredis.FairnessPolicy{"default": policy},
						AdmissionPolicies: map[string]taskforgeredis.AdmissionPolicy{"default": {
							Mode: taskforgeredis.AdmissionModeReject, MaxOldestReadyAge: time.Hour,
						}},
					})
					for index := range tenants {
						msg := benchmarkMessage("snapshot", index)
						msg.FairnessKey = fmt.Sprintf("tenant-%d", index)
						msg.Payload = make([]byte, size)
						if _, err := env.broker.Publish(env.ctx, msg, taskforge.PublishOptions{}); err != nil {
							b.Fatal(err)
						}
					}
					measureRedisOperation(b, env, func(int) {
						if operation == "metrics" {
							if _, err := env.broker.QueueMetricsSnapshot(env.ctx, "default"); err != nil {
								b.Fatal(err)
							}
						} else if _, err := env.broker.AdmissionStatusSnapshot(env.ctx, "default", time.Now()); err != nil {
							b.Fatal(err)
						}
					})
				})
			}
		}
	}
}

func BenchmarkPublishStateCosts(b *testing.B) {
	for _, fair := range []bool{false, true} {
		for _, dedup := range []bool{false, true} {
			b.Run(fmt.Sprintf("fair_%t/dedup_%t", fair, dedup), func(b *testing.B) {
				options := taskforgeredis.Options{}
				if fair {
					policy, err := taskforgeredis.NewFairnessPolicy(taskforgeredis.FairnessRule{}, nil)
					if err != nil {
						b.Fatal(err)
					}
					options.FairnessPolicies = map[string]*taskforgeredis.FairnessPolicy{"default": policy}
				}
				env := newBenchEnvWithOptions(b, time.Minute, options)
				measureRedisOperation(b, env, func(index int) {
					msg := benchmarkMessage("publish-state", index)
					opts := taskforge.PublishOptions{}
					if dedup {
						opts.DeduplicationKey = msg.ID
					}
					if _, err := env.broker.Publish(env.ctx, msg, opts); err != nil {
						b.Fatal(err)
					}
				})
			})
		}
	}
}
