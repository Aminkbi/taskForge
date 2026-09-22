package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/aminkbi/taskforge"
)

// publishReadyTask records built-in queued state in the same operation as
// publication. Its result belongs to this call, including concurrent publishes
// of the same logical task ID. Custom stores are handled by Publish.
func (b *Broker) publishReadyTask(ctx context.Context, msg taskforge.Task, payload []byte, deduplicationKey string, now time.Time) (bool, bool, error) {
	queue := taskforge.EffectiveQueue(msg)
	taskKey := ""
	stateArgs := []any{0}
	if store, ok := b.stateStore.(*stateStore); ok {
		record, err := store.queuedRecord(msg, now)
		if err != nil {
			return false, false, err
		}
		taskKey = store.taskKey(record.taskID)
		stateArgs = make([]any, 1, 1+2*len(record.fields))
		stateArgs[0] = len(record.fields)
		for field, value := range record.fields {
			stateArgs = append(stateArgs, field, value)
		}
	}
	receiptKey := ""
	receiptTTL := int64(0)
	if deduplicationKey != "" {
		receiptKey = b.publishReceiptKey(deduplicationKey)
		receiptTTL = b.publishReceiptTTL().Milliseconds()
	}
	fairnessKey, fairnessSet, notifyKey := "", "", ""
	if b.fairnessPolicy(queue) != nil {
		fairnessKey = NormalizeFairnessKey(msg.FairnessKey)
		fairnessSet = b.fairnessKeysSetKey(queue)
		notifyKey = b.fairnessNotifyKey(queue)
	}
	args := make([]any, 0, 5+len(stateArgs))
	args = append(args, streamPayloadField, string(payload), receiptTTL, fairnessKey, formatTime(now))
	args = append(args, stateArgs...)
	published, err := publishReadyTaskScript.Run(ctx, b.client,
		[]string{b.queueStreamKey(queue, fairnessKey), taskKey, receiptKey, fairnessSet, notifyKey}, args...).Int64()
	if err != nil {
		return false, false, fmt.Errorf("publish task: add ready entry and queued state: %w", err)
	}
	return published == 1, published == 1 && taskKey != "", nil
}
