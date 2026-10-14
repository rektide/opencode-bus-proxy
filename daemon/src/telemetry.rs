#[cfg(feature = "clickhouse-native")]
mod native;
mod selection;

use crate::Result;
#[cfg(any(feature = "otlp", feature = "clickhouse-native"))]
use bus_proxy_telemetry::sdk::trace::BatchSpanProcessor;
#[cfg(feature = "otel")]
use bus_proxy_telemetry::{
    opentelemetry,
    sdk::{self, Resource, trace::SdkTracerProvider},
};
use selection::Destination;
use tracing_subscriber::{EnvFilter, Layer, prelude::*};

pub struct Telemetry {
    #[cfg(feature = "otel")]
    provider: Option<SdkTracerProvider>,
    #[cfg(feature = "otel")]
    shutdown_budget: std::time::Duration,
    #[cfg(feature = "otel")]
    destinations: usize,
    #[cfg(feature = "clickhouse-native")]
    native: Option<bus_proxy_telemetry::clickhouse_native::BatchClient>,
}
impl Telemetry {
    pub fn shutdown(self) {
        #[cfg(feature = "otel")]
        if let Some(provider) = self.provider {
            // SDK applies its timeout to each processor; divide the total host
            // budget so adding a destination does not multiply graceful wait.
            if let Err(error) = provider
                .shutdown_with_timeout(self.shutdown_budget / self.destinations.max(1) as u32)
            {
                eprintln!("telemetry shutdown incomplete: {error}");
            }
        }
        #[cfg(feature = "clickhouse-native")]
        if let Some(client) = self.native {
            // An expired SDK drain is not permission to detach native inserts.
            // Join/cancel explicitly (fixed upstream connect allowance applies).
            if let Err(error) = client.shutdown() {
                eprintln!("native telemetry shutdown failed: {error}");
            }
        }
    }
}

fn setting(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

#[cfg(feature = "otlp")]
fn otlp_exporter() -> Result<bus_proxy_telemetry::otlp::SpanExporter> {
    use bus_proxy_telemetry::otlp::{self, WithHttpConfig};
    let protocol = setting("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        .or_else(|| setting("OTEL_EXPORTER_OTLP_PROTOCOL"));
    if !matches!(
        protocol.as_deref(),
        None | Some("http/protobuf" | "http/json")
    ) {
        return Err("OTLP protocol must be http/protobuf or http/json".into());
    }
    let timeout = setting("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT")
        .or_else(|| setting("OTEL_EXPORTER_OTLP_TIMEOUT"))
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
        .unwrap_or_else(|| std::time::Duration::from_secs(10));
    // Explicit blocking client survives native/OTLP Cargo feature unification.
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .build()?;
    Ok(otlp::SpanExporter::builder()
        .with_http()
        .with_http_client(client)
        .build()?)
}

pub fn init(json: bool) -> Result<Telemetry> {
    let selected = selection::select(
        setting("OTEL_SDK_DISABLED").as_deref(),
        setting("OTEL_TRACES_EXPORTER").as_deref(),
        setting("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some()
            || setting("OTEL_EXPORTER_OTLP_ENDPOINT").is_some(),
    )?;
    for destination in &selected {
        selection::require(
            *destination,
            match destination {
                Destination::Otlp => cfg!(feature = "otlp"),
                Destination::ClickhouseNative => cfg!(feature = "clickhouse-native"),
            },
        )?;
    }
    #[cfg(feature = "otel")]
    let export_filter = if selected.is_empty() {
        EnvFilter::new("off")
    } else {
        EnvFilter::try_new(setting("BUS_PROXY_OTEL_FILTER").unwrap_or_else(|| "info".into()))?
    };
    #[cfg(feature = "otel")]
    let shutdown_budget = if selected.is_empty() {
        None
    } else {
        setting("BUS_PROXY_OTEL_SHUTDOWN_TIMEOUT_MS")
    }
    .map(|v| v.parse::<u64>())
    .transpose()
    .map_err(|_| "BUS_PROXY_OTEL_SHUTDOWN_TIMEOUT_MS must be a positive integer")?
    .unwrap_or(5000);
    #[cfg(feature = "otel")]
    if !(1..=300_000).contains(&shutdown_budget) {
        return Err("BUS_PROXY_OTEL_SHUTDOWN_TIMEOUT_MS must be 1..=300000".into());
    }
    #[cfg(feature = "clickhouse-native")]
    let mut native_client = None;
    #[cfg(feature = "otel")]
    let provider = if selected.is_empty() {
        None
    } else {
        // This is the deliberate host SDK environment adapter. Reusable policy
        // accepts explicit values and never invokes resource detectors itself.
        let detected = Resource::builder().build();
        let service_name = setting("OTEL_SERVICE_NAME")
            .or_else(|| {
                detected
                    .get(&opentelemetry::Key::new("service.name"))
                    .map(|v| v.to_string())
            })
            .filter(|v| !v.starts_with("unknown_service"))
            .unwrap_or_else(|| "opencode-bus-proxy".into());
        let resource = Resource::builder()
            .with_service_name(service_name)
            .with_attribute(opentelemetry::KeyValue::new(
                "service.version",
                env!("CARGO_PKG_VERSION"),
            ))
            .build();
        let policy = sdk::trace::Config::default();
        #[cfg(feature = "otlp")]
        let otlp = if selected.contains(&Destination::Otlp) {
            Some(otlp_exporter()?)
        } else {
            None
        };
        let builder = bus_proxy_telemetry::ProviderConfig {
            resource,
            sampler: policy.sampler,
            span_limits: policy.span_limits,
            batch: sdk::trace::BatchConfig::default(),
        }
        .builder();
        #[cfg(feature = "clickhouse-native")]
        let builder = if selected.contains(&Destination::ClickhouseNative) {
            let config = native::config()?;
            let projection = native::projection(&config)?;
            let client = bus_proxy_telemetry::clickhouse_native::BatchClient::new(config)?;
            let exporter = bus_proxy_telemetry::clickhouse_native::otel::Exporter::new(
                client.clone(),
                projection,
            )?;
            native_client = Some(client);
            builder.with_span_processor(
                BatchSpanProcessor::builder(exporter)
                    .with_batch_config(sdk::trace::BatchConfig::default())
                    .build(),
            )
        } else {
            builder
        };
        #[cfg(feature = "otlp")]
        let builder = if let Some(exporter) = otlp {
            builder.with_span_processor(
                BatchSpanProcessor::builder(exporter)
                    .with_batch_config(sdk::trace::BatchConfig::default())
                    .build(),
            )
        } else {
            builder
        };
        Some(builder.build())
    };
    #[cfg(feature = "otel")]
    let otel = provider
        .as_ref()
        .map(|provider| bus_proxy_telemetry::layer(provider, export_filter));
    let local_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,h2=warn"));
    let local = if json {
        tracing_subscriber::fmt::layer().json().boxed()
    } else {
        tracing_subscriber::fmt::layer().boxed()
    };
    let telemetry = Telemetry {
        #[cfg(feature = "otel")]
        provider,
        #[cfg(feature = "otel")]
        shutdown_budget: std::time::Duration::from_millis(shutdown_budget),
        #[cfg(feature = "otel")]
        destinations: selected.len(),
        #[cfg(feature = "clickhouse-native")]
        native: native_client,
    };
    let subscriber = tracing_subscriber::registry();
    #[cfg(feature = "otel")]
    let subscriber = subscriber.with(otel);
    if let Err(error) = subscriber.with(local.with_filter(local_filter)).try_init() {
        telemetry.shutdown();
        return Err(error.into());
    }
    Ok(telemetry)
}
