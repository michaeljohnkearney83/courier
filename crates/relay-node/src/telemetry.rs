//! Logging, plus OpenTelemetry traces when an OTLP endpoint is configured.
//!
//! A bundle carries the W3C `traceparent` of the span that last handled it,
//! so one trace follows the bundle from node to node, including the time it
//! spent queued while a link was down.

use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use std::collections::HashMap;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Set up logging, and tracing too when the standard
/// `OTEL_EXPORTER_OTLP_ENDPOINT` (or `..._TRACES_ENDPOINT`) is set. Traces go
/// over OTLP/HTTP, e.g. to `http://jaeger:4318`. Shut the returned provider
/// down on exit so buffered spans are sent.
pub fn init(node_id: &str) -> anyhow::Result<Option<SdkTracerProvider>> {
    let enabled = [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ]
    .iter()
    .any(|var| std::env::var_os(var).is_some_and(|v| !v.is_empty()));
    let provider = enabled
        .then(|| -> anyhow::Result<_> {
            let exporter = SpanExporter::builder().with_http().build()?;
            Ok(SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(
                    Resource::builder()
                        .with_service_name("relay-node")
                        .with_attribute(KeyValue::new("service.instance.id", node_id.to_string()))
                        .build(),
                )
                .build())
        })
        .transpose()?;

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .with(
            provider
                .as_ref()
                .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer("relay-node"))),
        )
        .init();
    Ok(provider)
}

/// The `traceparent` of the current span, if it is being traced.
pub fn current_trace_parent() -> Option<String> {
    let mut carrier = HashMap::new();
    TraceContextPropagator::new().inject_context(&Span::current().context(), &mut carrier);
    carrier.remove("traceparent")
}

/// Make `span` a child of the span that last handled the bundle.
pub fn continue_trace(span: &Span, trace_parent: Option<&str>) {
    let Some(trace_parent) = trace_parent else {
        return;
    };
    let carrier = HashMap::from([("traceparent".to_string(), trace_parent.to_string())]);
    let parent: Context = TraceContextPropagator::new().extract(&carrier);
    // Only fails if the span is already closed, which a fresh span never is.
    let _ = span.set_parent(parent);
}
