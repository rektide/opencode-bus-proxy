use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use tracing_subscriber::{EnvFilter, Layer, prelude::*};

use crate::Result;

pub struct Telemetry(Option<SdkTracerProvider>);

impl Telemetry {
    pub fn shutdown(self) {
        if let Some(provider) = self.0
            && let Err(error) = provider.shutdown()
        {
            eprintln!("OTLP shutdown failed: {error}");
        }
    }
}

fn setting(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn enabled(disabled: Option<&str>, exporter: Option<&str>, endpoint: bool) -> Result<bool> {
    if disabled.is_some_and(|value| value.eq_ignore_ascii_case("true")) {
        return Ok(false);
    }
    match exporter {
        Some("none") => Ok(false),
        Some("otlp") => Ok(true),
        None => Ok(endpoint),
        Some(_) => Err("OTEL_TRACES_EXPORTER must be otlp or none".into()),
    }
}

pub fn init(json: bool) -> Result<Telemetry> {
    let enabled = enabled(
        setting("OTEL_SDK_DISABLED").as_deref(),
        setting("OTEL_TRACES_EXPORTER").as_deref(),
        setting("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some()
            || setting("OTEL_EXPORTER_OTLP_ENDPOINT").is_some(),
    )?;
    let export_filter =
        EnvFilter::try_new(setting("BUS_PROXY_OTEL_FILTER").unwrap_or_else(|| "info".into()))?;
    let provider = if enabled {
        let protocol = setting("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            .or_else(|| setting("OTEL_EXPORTER_OTLP_PROTOCOL"));
        if !matches!(
            protocol.as_deref(),
            None | Some("http/protobuf" | "http/json")
        ) {
            return Err("OTLP protocol must be http/protobuf or http/json".into());
        }
        let _ = rustls::crypto::ring::default_provider().install_default();
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .build()?;
        // Resource detectors honor OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES.
        let detected = Resource::builder().build();
        let service_name = setting("OTEL_SERVICE_NAME")
            .or_else(|| {
                detected
                    .get(&opentelemetry::Key::new("service.name"))
                    .map(|value| value.to_string())
            })
            .filter(|name| !name.starts_with("unknown_service"))
            .unwrap_or_else(|| "opencode-bus-proxy".into());
        let resource = Resource::builder()
            .with_service_name(service_name)
            .with_attribute(opentelemetry::KeyValue::new(
                "service.version",
                env!("CARGO_PKG_VERSION"),
            ))
            .build();
        Some(
            SdkTracerProvider::builder()
                .with_resource(resource)
                .with_batch_exporter(exporter)
                .build(),
        )
    } else {
        None
    };
    let otel = provider.as_ref().map(|provider| {
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("opencode-bus-proxy"))
            // Never export exporter/HTTP-client internals back into themselves.
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                [
                    "bus_proxy_gateway",
                    "bus_proxy_router",
                    "opencode_bus_proxy",
                ]
                .iter()
                .any(|target| metadata.target().starts_with(target))
            }))
            .with_filter(export_filter)
    });
    let local_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,h2=warn"));
    let local = if json {
        tracing_subscriber::fmt::layer().json().boxed()
    } else {
        tracing_subscriber::fmt::layer().boxed()
    };
    let telemetry = Telemetry(provider);
    if let Err(error) = tracing_subscriber::registry()
        .with(otel)
        .with(local.with_filter(local_filter))
        .try_init()
    {
        telemetry.shutdown();
        return Err(error.into());
    }
    Ok(telemetry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_is_opt_in_with_explicit_disable_precedence() {
        assert!(!enabled(None, None, false).unwrap());
        assert!(enabled(None, None, true).unwrap());
        assert!(enabled(None, Some("otlp"), false).unwrap());
        assert!(!enabled(None, Some("none"), true).unwrap());
        assert!(!enabled(Some("TRUE"), Some("otlp"), true).unwrap());
        assert!(enabled(None, Some("typo"), true).is_err());
    }
}
