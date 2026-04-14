package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestInjectAndExtractTraceContext(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx, span := provider.Tracer("test").Start(context.Background(), "root")
	defer span.End()

	headers := InjectTraceContext(ctx, nil)
	if headers[HeaderTraceparent] == "" {
		t.Fatalf("InjectTraceContext() missing %q header", HeaderTraceparent)
	}

	extracted := ExtractTraceContext(context.Background(), headers)
	got := TraceIDFromContext(extracted)
	want := span.SpanContext().TraceID().String()
	if got != want {
		t.Fatalf("TraceIDFromContext() = %q, want %q", got, want)
	}
}
