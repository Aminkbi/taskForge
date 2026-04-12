package scheduler

import (
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

func IsDue(msg broker.TaskMessage, now time.Time) bool {
	return msg.ETA == nil || !msg.ETA.After(now.UTC())
}
