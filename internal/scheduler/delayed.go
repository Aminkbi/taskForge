package scheduler

import (
	"time"

	"github.com/aminkbi/taskforge"
)

func IsDue(msg taskforge.Task, now time.Time) bool {
	return msg.ETA == nil || !msg.ETA.After(now.UTC())
}
