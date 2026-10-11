#[cfg(feature = "otlp")]
use bus_proxy_telemetry::{
    opentelemetry, otlp,
    sdk::{self, Resource, trace::SdkTracerProvider},
};
use tracing_subscriber::{EnvFilter, Layer, prelude::*};

use crate::Result;

pub struct Telemetry {
    #[cfg(feature = "otlp")]
    provider: Option<SdkTracerProvider>,
}

impl Telemetry {
    pub fn shutdown(self) {
        #[cfg(feature = "otlp")]
        if let Some(provider) = self.provider
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

fn require_backend(enabled: bool, compiled: bool) -> Result<()> {
    if enabled && !compiled {
        return Err("OTLP export requested but not compiled; rebuild opencode-bus-proxy with --features otlp (or disable with OTEL_TRACES_EXPORTER=none)".into());
    }
    Ok(())
}

pub fn init(json: bool) -> Result<Telemetry> {
    let enabled = enabled(
        setting("OTEL_SDK_DISABLED").as_deref(),
        setting("OTEL_TRACES_EXPORTER").as_deref(),
        setting("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some()
            || setting("OTEL_EXPORTER_OTLP_ENDPOINT").is_some(),
    )?;
    require_backend(enabled, cfg!(feature = "otlp"))?;
    #[cfg(feature = "otlp")]
    let export_filter =
        EnvFilter::try_new(setting("BUS_PROXY_OTEL_FILTER").unwrap_or_else(|| "info".into()))?;
    #[cfg(feature = "otlp")]
    let provider = if enabled {
        let protocol = setting("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            .or_else(|| setting("OTEL_EXPORTER_OTLP_PROTOCOL"));
        if !matches!(
            protocol.as_deref(),
            None | Some("http/protobuf" | "http/json")
        ) {
            return Err("OTLP protocol must be http/protobuf or http/json".into());
        }
        use otlp::WithHttpConfig;
        // Client construction/drop and provider lifecycle must stay off async
        // workers. Explicit client selection survives Cargo feature unification.
        let timeout = setting("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT")
            .or_else(|| setting("OTEL_EXPORTER_OTLP_TIMEOUT"))
            .and_then(|v| v.parse::<u64>().ok())
            .map(std::time::Duration::from_millis)
            .unwrap_or_else(|| std::time::Duration::from_secs(10));
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()?;
        let exporter = otlp::SpanExporter::builder()
            .with_http()
            .with_http_client(client)
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
        // This is the deliberate SDK environment adapter. Reusable composition
        // receives concrete policy instead of consulting ambient configuration.
        let policy = sdk::trace::Config::default();
        Some(
            bus_proxy_telemetry::ProviderConfig {
                resource,
                sampler: policy.sampler,
                span_limits: policy.span_limits,
                batch: sdk::trace::BatchConfig::default(),
            }
            .build(exporter),
        )
    } else {
        None
    };
    #[cfg(feature = "otlp")]
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
        #[cfg(feature = "otlp")]
        provider,
    };
    let subscriber = tracing_subscriber::registry();
    #[cfg(feature = "otlp")]
    let subscriber = subscriber.with(otel);
    if let Err(error) = subscriber.with(local.with_filter(local_filter)).try_init() {
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
        assert!(!enabled(Some("true"), Some("typo"), true).unwrap());
        assert!(enabled(None, Some("typo"), true).is_err());
    }

    #[test]
    fn unavailable_backend_is_actionable_and_disable_wins() {
        for selection in [
            enabled(None, Some("otlp"), false),
            enabled(None, None, true),
        ] {
            assert!(
                require_backend(selection.unwrap(), false)
                    .unwrap_err()
                    .to_string()
                    .contains("--features otlp")
            );
        }
        assert!(require_backend(enabled(None, Some("none"), true).unwrap(), false).is_ok());
        assert!(require_backend(enabled(Some("true"), Some("otlp"), true).unwrap(), false).is_ok());
        assert!(require_backend(true, true).is_ok());
    }
}
