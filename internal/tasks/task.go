package tasks

import "github.com/aminkbi/taskforge/internal/broker"

func EffectiveQueue(msg broker.TaskMessage) string {
	if msg.Queue == "" {
		return "default"
	}
	return msg.Queue
}
