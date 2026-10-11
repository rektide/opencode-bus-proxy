use bus_proxy_telemetry::{
    ProviderConfig, batch_config,
    opentelemetry::trace::{Span, Tracer, TracerProvider},
    sdk::{
        Resource,
        error::OTelSdkResult,
        trace::{BatchSpanProcessor, Sampler, SpanData, SpanExporter, SpanLimits},
    },
};
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

#[derive(Debug)]
struct Slow {
    started: mpsc::Sender<()>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
}
impl SpanExporter for Slow {
    async fn export(&self, _: Vec<SpanData>) -> OTelSdkResult {
        self.started.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        Ok(())
    }
}
#[derive(Debug)]
struct Fast(mpsc::Sender<()>);
impl SpanExporter for Fast {
    async fn export(&self, _: Vec<SpanData>) -> OTelSdkResult {
        self.0.send(()).unwrap();
        Ok(())
    }
}

#[test]
fn independent_processors_do_not_serialize_destinations() {
    let (started, starting) = mpsc::channel();
    let (release, releasing) = mpsc::channel();
    let (exported, exporting) = mpsc::channel();
    let provider = ProviderConfig {
        resource: Resource::builder_empty().with_service_name("test").build(),
        sampler: Box::new(Sampler::AlwaysOn),
        span_limits: SpanLimits::default(),
        batch: batch_config(4, 1, Duration::from_secs(60)),
    }
    .builder()
    .with_span_processor(
        BatchSpanProcessor::builder(Slow {
            started,
            release: Arc::new(Mutex::new(releasing)),
        })
        .with_batch_config(batch_config(4, 1, Duration::from_secs(60)))
        .build(),
    )
    .with_span_processor(
        BatchSpanProcessor::builder(Fast(exported))
            .with_batch_config(batch_config(4, 1, Duration::from_secs(60)))
            .build(),
    )
    .build();
    provider.tracer("test").start("shared-span").end();
    starting.recv_timeout(Duration::from_secs(2)).unwrap();
    exporting
        .recv_timeout(Duration::from_secs(2))
        .expect("fast destination waited for slow destination");
    release.send(()).unwrap();
    provider.shutdown().unwrap();
}
