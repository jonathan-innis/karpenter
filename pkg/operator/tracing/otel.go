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
	"fmt"

	"go.opentelemetry.io/contrib/processors/baggagecopy"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// otelTracer wraps the OpenTelemetry tracer
type otelTracer struct {
	tracer trace.Tracer
}

// NewOTELTracer creates a new OTEL tracer
func NewOTELTracer(ctx context.Context, serviceName, endpoint string) (Tracer, func(context.Context) error, error) {
	// Create OTLP exporter
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Create resource with service name
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(baggagecopy.NewSpanProcessor(baggagecopy.AllowAllMembers)),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Set global propagator to tracecontext (the default is no-op)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := tp.Tracer("karpenter")

	// Return shutdown function
	shutdown := func(ctx context.Context) error {
		return tp.Shutdown(ctx)
	}

	return &otelTracer{tracer: tracer}, shutdown, nil
}

func (o *otelTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span) {
	ctx, span := o.tracer.Start(ctx, spanName, opts...)
	return ctx, &otelSpan{span: span}
}

func (o *otelTracer) SpanFromContext(ctx context.Context) Span {
	return &otelSpan{span: trace.SpanFromContext(ctx)}
}

// otelSpan wraps the OpenTelemetry span
type otelSpan struct {
	span trace.Span
}

func (o *otelSpan) End() {
	o.span.End()
}

func (o *otelSpan) SetAttributes(attributes ...attribute.KeyValue) {
	o.span.SetAttributes(attributes...)
}

func (o *otelSpan) SetStatus(code codes.Code, description string) {
	o.span.SetStatus(code, description)
}

func (o *otelSpan) AddEvent(name string, options ...trace.EventOption) {
	o.span.AddEvent(name, options...)
}

func (o *otelSpan) RecordError(err error, options ...trace.EventOption) {
	o.span.RecordError(err, options...)
}
