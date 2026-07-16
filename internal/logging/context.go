package logging

import (
	"log/slog"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/observability"
)

func WithDelivery(logger *slog.Logger, delivery taskforge.Delivery) *slog.Logger {
	if logger == nil {
		return nil
	}

	attrs := []any{
		"task_id", delivery.Execution.TaskID,
		"delivery_id", delivery.Execution.DeliveryID,
		"queue", taskforge.EffectiveQueue(delivery.Message),
		"fairness_key", delivery.Message.FairnessKey,
		"worker_identity", delivery.Execution.LeaseOwner,
		"delivery_count", delivery.Execution.DeliveryCount,
		"lease_expiry", delivery.Execution.LeaseExpiresAt,
	}
	if traceID := observability.TraceIDFromHeaders(delivery.Message.Headers); traceID != "" {
		attrs = append(attrs, "trace_id", traceID)
	}

	return logger.With(attrs...)
}
