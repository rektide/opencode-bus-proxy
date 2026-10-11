//! Optional completed-span adapter. The SDK/provider is caller-owned.
use crate::BatchClient;
pub use bus_proxy_export_arrow::traces::Policy;
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
    trace::{SpanData, SpanExporter},
};
use std::time::Duration;

#[derive(Debug)]
pub struct Exporter {
    client: BatchClient,
    resource: Resource,
    policy: Policy,
}
impl Exporter {
    pub fn new(
        client: BatchClient,
        policy: Policy,
    ) -> Result<Self, bus_proxy_export_arrow::traces::Error> {
        policy.validate()?;
        Ok(Self {
            client,
            policy,
            resource: Resource::builder_empty().build(),
        })
    }
}
impl SpanExporter for Exporter {
    async fn export(&self, spans: Vec<SpanData>) -> OTelSdkResult {
        let result = async {
            let batch =
                bus_proxy_export_arrow::traces::project(&spans, &self.resource, &self.policy)
                    .map_err(|e| OTelSdkError::InternalFailure(e.to_string()))?;
            self.client
                .insert(batch)
                .await
                .map_err(|e| OTelSdkError::InternalFailure(e.to_string()))
        }
        .await;
        // Standard SDK scheduled export discards errors after diagnostic hooks.
        // This boundary reports a sanitized best-effort drop to host logging;
        // its target is excluded by core's trace allowlist (no feedback loop).
        if let Err(error) = &result {
            tracing::warn!(error = %error, spans = spans.len(), "native trace batch not acknowledged; dropped without retry");
        }
        result
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.clone();
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        // Cancellation joins the private worker. Audited upstream connect can
        // overrun a shorter SDK budget by its fixed 30-second allowance.
        self.client
            .shutdown()
            .map_err(|e| OTelSdkError::InternalFailure(e.to_string()))
    }
}
