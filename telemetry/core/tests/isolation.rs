use bus_proxy_telemetry::{
    ProviderConfig, batch_config,
    opentelemetry::{
        Key, KeyValue,
        trace::{Span, Tracer, TracerProvider},
    },
    sdk::{
        Resource,
        error::OTelSdkResult,
        trace::{Sampler, SpanData, SpanExporter, SpanLimits},
    },
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone, Default)]
struct Capture {
    spans: Arc<Mutex<Vec<SpanData>>>,
    resource: Arc<Mutex<Option<Resource>>>,
}
impl SpanExporter for Capture {
    async fn export(&self, spans: Vec<SpanData>) -> OTelSdkResult {
        self.spans.lock().unwrap().extend(spans);
        Ok(())
    }
    fn set_resource(&mut self, resource: &Resource) {
        *self.resource.lock().unwrap() = Some(resource.clone());
    }
}

#[test]
fn explicit_policy_overrides_sdk_ambient_defaults() {
    // Set hostile ambient policy in a separate process, never mutate the test
    // runner's environment while its threads are active.
    if std::env::var_os("TELEMETRY_ISOLATION_CHILD").is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "explicit_policy_overrides_sdk_ambient_defaults"])
            .env("TELEMETRY_ISOLATION_CHILD", "1")
            .env("OTEL_RESOURCE_ATTRIBUTES", "ambient.secret=unwanted")
            .env("OTEL_SERVICE_NAME", "unwanted")
            .env("OTEL_TRACES_SAMPLER", "always_off")
            .env("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "0")
            .env("OTEL_BSP_MAX_QUEUE_SIZE", "0")
            .env("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "0")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    let capture = Capture::default();
    let provider = ProviderConfig {
        resource: Resource::builder_empty()
            .with_service_name("explicit")
            .build(),
        sampler: Box::new(Sampler::AlwaysOn),
        span_limits: SpanLimits::default(),
        batch: batch_config(8, 4, Duration::from_secs(60)),
    }
    .build(capture.clone());
    let mut span = provider.tracer("test").start("test");
    span.set_attribute(KeyValue::new("explicit", true));
    span.end();
    provider.force_flush().unwrap();
    provider.shutdown().unwrap();
    let spans = capture.spans.lock().unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].attributes.len(), 1);
    let resource = capture.resource.lock().unwrap();
    let resource = resource.as_ref().unwrap();
    assert_eq!(
        resource.get(&Key::new("service.name")).unwrap().to_string(),
        "explicit"
    );
    assert!(resource.get(&Key::new("ambient.secret")).is_none());
}
