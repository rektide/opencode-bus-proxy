//! Caller-owned telemetry composition. No global subscriber or provider is installed.
//! Construct and shut down providers off async workers when using blocking exporters.
pub use opentelemetry;
/// The upstream OTLP builder reads standard OTEL environment variables even when
/// supplied programmatic settings. Use it in a deliberate host adapter, not as an
/// environment-isolated library constructor. Supply an explicit HTTP client.
#[cfg(feature = "otlp")]
pub use opentelemetry_otlp as otlp;
pub use opentelemetry_sdk as sdk;

use opentelemetry::trace::TracerProvider;
use sdk::{
    Resource,
    trace::{
        BatchConfig, BatchSpanProcessor, SdkTracerProvider, ShouldSample, SpanExporter, SpanLimits,
    },
};
use std::time::Duration;
use tracing_subscriber::{EnvFilter, Layer, registry::LookupSpan};

/// Explicit policy: SDK ambient resource, sampler and span-limit defaults are
/// overwritten before building the provider. Batch configuration is also explicit.
pub struct ProviderConfig {
    pub resource: Resource,
    pub sampler: Box<dyn ShouldSample>,
    pub span_limits: SpanLimits,
    pub batch: BatchConfig,
}

impl ProviderConfig {
    pub fn build(self, exporter: impl SpanExporter + 'static) -> SdkTracerProvider {
        let processor = BatchSpanProcessor::builder(exporter)
            .with_batch_config(self.batch)
            .build();
        SdkTracerProvider::builder()
            .with_resource(self.resource)
            .with_sampler(self.sampler)
            .with_span_limits(self.span_limits)
            .with_span_processor(processor)
            .build()
    }

    /// Caller can add independently bounded processors for several destinations.
    /// They share this provider's resource, sampling and span policy, not a queue.
    pub fn builder(self) -> sdk::trace::TracerProviderBuilder {
        SdkTracerProvider::builder()
            .with_resource(self.resource)
            .with_sampler(self.sampler)
            .with_span_limits(self.span_limits)
    }
}

/// Deterministic policy for the ordinary dedicated-thread batch processor.
/// Queue size, batch size and delay override SDK environment defaults. Export
/// timeouts belong to the exporter; experimental async-processor settings are
/// not used by this composition.
pub fn batch_config(queue: usize, batch: usize, delay: Duration) -> BatchConfig {
    sdk::trace::BatchConfigBuilder::default()
        .with_max_queue_size(queue)
        .with_max_export_batch_size(batch)
        .with_scheduled_delay(delay)
        .build()
}

/// Independent export filter plus the data-plane target allowlist. Exporter
/// internals are excluded to avoid recursive feedback. Local logging is host-owned.
pub fn layer<S>(provider: &SdkTracerProvider, filter: EnvFilter) -> impl Layer<S> + use<S>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("opencode-bus-proxy"))
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            [
                "bus_proxy_gateway",
                "bus_proxy_router",
                "opencode_bus_proxy",
            ]
            .iter()
            .any(|target| metadata.target().starts_with(target))
        }))
        .with_filter(filter)
}
