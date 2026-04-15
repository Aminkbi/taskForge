package observability

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

type HeaderCarrier map[string]string

func (c HeaderCarrier) Get(key string) string {
	return c[strings.ToLower(key)]
}

func (c HeaderCarrier) Set(key, value string) {
	c[strings.ToLower(key)] = value
}

func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func EnsureHeaders(headers map[string]string) map[string]string {
	if headers != nil {
		return headers
	}
	return map[string]string{}
}

func InjectTraceContext(ctx context.Context, headers map[string]string) map[string]string {
	carrier := HeaderCarrier(EnsureHeaders(headers))
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return map[string]string(carrier)
}

func ExtractTraceContext(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier(headers))
}

func TraceIDFromHeaders(headers map[string]string) string {
	if len(headers) == 0 {
		return ""
	}
	if traceID := strings.TrimSpace(headers["trace_id"]); traceID != "" {
		return traceID
	}
	traceparent := strings.TrimSpace(headers[HeaderTraceparent])
	if traceparent == "" {
		return ""
	}
	parts := strings.Split(traceparent, "-")
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

func TraceIDFromContext(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if !spanCtx.IsValid() {
		return ""
	}
	return spanCtx.TraceID().String()
}

func StartQueueSpan(ctx context.Context, component, name string, msg broker.TaskMessage, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	baseAttrs := []attribute.KeyValue{
		attribute.String("taskforge.queue", tasks.EffectiveQueue(msg)),
		attribute.String("taskforge.task_name", msg.Name),
		attribute.String("taskforge.task_id", msg.ID),
		attribute.Int("taskforge.attempt", msg.Attempt),
	}
	if msg.FairnessKey != "" {
		baseAttrs = append(baseAttrs, attribute.String("taskforge.fairness_key", msg.FairnessKey))
	}
	baseAttrs = append(baseAttrs, attrs...)
	return Tracer(component).Start(ctx, name, trace.WithAttributes(baseAttrs...))
}
