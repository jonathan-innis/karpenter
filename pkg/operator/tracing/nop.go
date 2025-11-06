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

// nopTracer is a no-op implementation of Tracer
type nopTracer struct{}

// NewNopTracer creates a new no-op tracer
func NewNopTracer() Tracer {
	return &nopTracer{}
}

func (n *nopTracer) Start(ctx context.Context, _ string, _ ...trace.SpanStartOption) (context.Context, Span) {
	return ctx, &nopSpan{}
}

func (n *nopTracer) SpanFromContext(ctx context.Context) Span {
	return &nopSpan{}
}

// nopSpan is a no-op implementation of Span
type nopSpan struct{}

func (n *nopSpan) End() {}

func (n *nopSpan) SetAttributes(_ ...attribute.KeyValue) {}

func (n *nopSpan) SetStatus(_ codes.Code, _ string) {}

func (n *nopSpan) AddEvent(_ string, _ ...trace.EventOption) {}

func (n *nopSpan) RecordError(_ error, _ ...trace.EventOption) {}
