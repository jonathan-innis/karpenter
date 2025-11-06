/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type tracingKey struct{}

// Tracer is an interface for creating spans
type Tracer interface {
	// Start creates a span and a context.Context containing the newly-created span
	Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span)
	// SpanFromContext returns the span from the context
	SpanFromContext(ctx context.Context) Span
}

// Span is an interface that wraps OpenTelemetry span operations
type Span interface {
	// End completes the span
	End()
	// SetAttributes sets attributes on the span
	SetAttributes(attributes ...attribute.KeyValue)
	// SetStatus sets the status of the span
	SetStatus(code codes.Code, description string)
	// AddEvent adds an event to the span
	AddEvent(name string, options ...trace.EventOption)
	// RecordError records an error as an exception span event
	RecordError(err error, options ...trace.EventOption)
}

// NewContext returns a new context with the tracer attached
func NewContext(ctx context.Context, tracer Tracer) context.Context {
	return context.WithValue(ctx, tracingKey{}, tracer)
}

// FromContext returns the tracer from the context
func FromContext(ctx context.Context) Tracer {
	if tracer, ok := ctx.Value(tracingKey{}).(Tracer); ok {
		return tracer
	}
	return &nopTracer{}
}

// StartSpan is a convenience function that gets the tracer from context and starts a span
func StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) context.Context {
	ctx, _ = FromContext(ctx).Start(ctx, spanName, opts...)
	return ctx
}

// EndSpan completes the span and marks if an error has occurred within the span
func EndSpan(ctx context.Context, err error) {
	span := SpanFromContext(ctx)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	} else {
		span.SetStatus(codes.Ok, "completed successfully")
	}
	span.End()
}

func SpanFromContext(ctx context.Context) Span {
	return FromContext(ctx).SpanFromContext(ctx)
}
