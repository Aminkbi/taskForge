package redis

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

func FuzzDecodeDelayedEntry(f *testing.F) {
	f.Add(`{"entry_id":"seed","scheduled_for":"2026-01-01T00:00:00Z","message":{"id":"task-1"}}`)
	f.Add(`{`)
	f.Add("")

	f.Fuzz(func(t *testing.T, raw string) {
		_, _ = decodeDelayedEntry(raw)
	})
}

func FuzzDecodeDeadLetterEntry(f *testing.F) {
	f.Add("1-0", `{"id":"dlq:delivery-1","payload":"{}"}`)
	f.Add("1-0", "{")

	f.Fuzz(func(t *testing.T, id, payload string) {
		_, _ = decodeDeadLetterEntry("default", redis.XMessage{
			ID:     id,
			Values: map[string]any{streamFieldName: payload},
		})
	})
}
