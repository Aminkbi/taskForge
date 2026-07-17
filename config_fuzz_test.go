package taskforge

import (
	"encoding/json"
	"testing"
	"time"
)

func FuzzConfigNormalizeScheduleValidation(f *testing.F) {
	f.Add("daily", "reports", "report.generate", []byte(`{"source":"seed"}`), int64(time.Minute))
	f.Add("", "", "", []byte(`{`), int64(-time.Second))

	f.Fuzz(func(t *testing.T, id, queue, taskName string, payload []byte, interval int64) {
		config := Config{
			WorkerPools: []WorkerPoolConfig{},
			Scheduler: SchedulerConfig{Schedules: []Schedule{{
				ID:       id,
				Queue:    queue,
				TaskName: taskName,
				Payload:  json.RawMessage(payload),
				Interval: time.Duration(interval),
			}}},
		}
		_, _ = config.Normalize()
	})
}
