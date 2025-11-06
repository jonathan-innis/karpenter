# OpenTelemetry Tracing Implementation for Karpenter

## Summary

This document describes the comprehensive OpenTelemetry (OTEL) tracing implementation added to all Karpenter controller reconcile loops.

## What Was Implemented

### 1. Configuration Options (`pkg/operator/options/options.go`)
Added two new configuration flags:
- `--enable-tracing` / `ENABLE_TRACING` (bool): Enable/disable OTEL tracing
- `--tracing-endpoint` / `TRACING_ENDPOINT` (string): OTLP gRPC endpoint (e.g., `localhost:4317`)

### 2. Tracing Package (`pkg/operator/tracing/`)
Created a new tracing package with three components:

#### `tracing.go`
- `Tracer` interface: Abstraction for creating spans
- `Span` interface: Abstraction for span operations
- Context management functions for injecting/extracting tracers

#### `nop.go`
- `NopTracer`: No-op implementation when tracing is disabled
- Zero overhead when tracing is not enabled

#### `otel.go`
- `OTELTracer`: Full OpenTelemetry implementation
- OTLP gRPC exporter configuration
- Trace provider setup with proper propagation

### 3. Operator Integration (`pkg/operator/operator.go`)
- Tracer initialization based on configuration
- Automatic fallback to NopTracer when disabled
- Tracer injection into controller context
- Proper shutdown handling

### 4. Controller Instrumentation

Added OTEL tracing to all 23+ controllers in the codebase:

#### Disruption Controllers
- `pkg/controllers/disruption/controller.go`
  - Main reconcile loop with cluster sync status
  - Per-method disruption tracking
  - Candidate and command counts
  - Error tracking with status codes

#### NodeClaim Controllers
- `pkg/controllers/nodeclaim/lifecycle/controller.go`
  - Full lifecycle tracking with provider ID and node name
  - Finalization span tracking
- `pkg/controllers/nodeclaim/consistency/controller.go`
  - Consistency check tracking
- `pkg/controllers/nodeclaim/disruption/controller.go`
  - Disruption status tracking
- `pkg/controllers/nodeclaim/expiration/controller.go`
  - Expiration event tracking
- `pkg/controllers/nodeclaim/garbagecollection/controller.go`
  - GC metrics (total, cloud provider count, collected count)
- `pkg/controllers/nodeclaim/hydration/controller.go`
  - Hydration status tracking
- `pkg/controllers/nodeclaim/podevents/controller.go`
  - Pod event recording with node name

#### Node Controllers
- `pkg/controllers/node/health/controller.go`
  - Health check tracking
- `pkg/controllers/node/hydration/controller.go`
  - Node hydration tracking
- `pkg/controllers/node/termination/controller.go`
  - Termination process tracking

#### NodePool Controllers
- `pkg/controllers/nodepool/counter/controller.go`
- `pkg/controllers/nodepool/hash/controller.go`
- `pkg/controllers/nodepool/readiness/controller.go`
- `pkg/controllers/nodepool/registrationhealth/controller.go`
- `pkg/controllers/nodepool/validation/controller.go`

#### Provisioning Controllers
- `pkg/controllers/provisioning/controller.go`
  - Both PodController and NodeController with resource tracking

#### State Informer Controllers
- `pkg/controllers/state/informer/daemonset.go`
- `pkg/controllers/state/informer/node.go`
- `pkg/controllers/state/informer/nodeclaim.go`
- `pkg/controllers/state/informer/nodepool.go`
- `pkg/controllers/state/informer/pod.go`

#### Metrics Controllers
- `pkg/controllers/metrics/node/controller.go`
- `pkg/controllers/metrics/nodepool/controller.go`
- `pkg/controllers/metrics/pod/controller.go`

#### Static Controllers
- `pkg/controllers/static/provisioning/controller.go`
- `pkg/controllers/static/deprovisioning/controller.go`

#### Overlay Controller
- `pkg/controllers/nodeoverlay/controller.go`

## Span Attributes

Each controller adds relevant attributes to spans:

### Common Attributes
- Resource names (node.name, nodeclaim.name, pod.name, etc.)
- Resource namespaces where applicable
- Management status (managed/unmanaged)
- Deletion status

### Controller-Specific Attributes
- **Disruption**: cluster.synced, candidates.count, commands.count, disruption.method, disruption.reason
- **NodeClaim Lifecycle**: provider_id, node_name, deleting status
- **Expiration**: expired status
- **Garbage Collection**: nodeclaims.total, nodeclaims.cloud_provider, nodeclaims.garbage_collected

## Error Tracking

All controllers include comprehensive error tracking:
- `span.RecordError(err)`: Records the error event
- `span.SetStatus(codes.Error, description)`: Marks span as error
- `span.SetStatus(codes.Ok, description)`: Marks successful completion

## Usage

### Enabling Tracing

#### Via Environment Variables
```bash
export ENABLE_TRACING=true
export TRACING_ENDPOINT=localhost:4317
```

#### Via Command Line Flags
```bash
karpenter --enable-tracing --tracing-endpoint=localhost:4317
```

### Disabling Tracing

Tracing is disabled by default. When disabled, the NopTracer is used with zero overhead.

### Example OTLP Collector Setup

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true
  
  logging:
    loglevel: debug

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [jaeger, logging]
```

## Dependencies

The following OpenTelemetry packages were added:
- `go.opentelemetry.io/otel` v1.38.0
- `go.opentelemetry.io/otel/trace` v1.38.0
- `go.opentelemetry.io/otel/sdk` v1.38.0
- `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` v1.38.0
- `go.opentelemetry.io/otel/semconv/v1.26.0`
- `google.golang.org/grpc` v1.76.0

## Benefits

1. **Observability**: Complete visibility into controller reconcile loops
2. **Performance Analysis**: Identify slow operations and bottlenecks
3. **Debugging**: Trace requests across the entire system
4. **Error Analysis**: Detailed error context and propagation
5. **Zero Overhead When Disabled**: NopTracer ensures no performance impact when tracing is off

## Future Enhancements

Consider adding:
1. More granular spans for internal operations (e.g., scheduling decisions, API calls)
2. Span links for related operations across controllers
3. Custom span events for significant state changes
4. Baggage propagation for cross-service correlation



