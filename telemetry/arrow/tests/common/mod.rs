use bus_proxy_export_arrow::{
    opentelemetry::{
        InstrumentationScope, KeyValue,
        trace::{
            Event, Link, SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
        },
    },
    opentelemetry_sdk::{
        Resource,
        trace::{SpanData, SpanEvents, SpanLinks},
    },
};
use std::time::{Duration, UNIX_EPOCH};

pub fn fixture() -> (SpanData, Resource) {
    let context = SpanContext::new(
        TraceId::from(0xabcdefu128),
        SpanId::from(0x123u64),
        TraceFlags::SAMPLED,
        false,
        TraceState::from_key_value([("vendor", "state")]).unwrap(),
    );
    let mut events = SpanEvents::default();
    events.events = vec![
        Event::new(
            "雪 event",
            UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789),
            vec![
                KeyValue::new("event.bool", true),
                KeyValue::new("Password", "should-not-export"),
            ],
            4,
        ),
        Event::new(
            "empty",
            UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_790),
            Vec::new(),
            0,
        ),
    ];
    events.dropped_count = 7;
    let mut links = SpanLinks::default();
    links.links = vec![
        Link::new(context.clone(), vec![KeyValue::new("link.int", -123)], 3),
        Link::new(SpanContext::empty_context(), Vec::new(), 0),
    ];
    links.dropped_count = 2;
    let resource = Resource::builder_empty()
        .with_service_name("fixture-service")
        .with_attributes([
            KeyValue::new("rum.sessionId", "fixture-session"),
            KeyValue::new("resource.number", 1.5),
            KeyValue::new("authorization", "hidden"),
        ])
        .build();
    (
        SpanData {
            span_context: context,
            parent_span_id: SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: SpanKind::Server,
            name: "雪 span".into(),
            start_time: UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_780),
            end_time: UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_900),
            attributes: vec![
                KeyValue::new("span.string", "value"),
                KeyValue::new("span.bool", true),
            ],
            dropped_attributes_count: 9,
            events,
            links,
            status: Status::error("fixture error"),
            instrumentation_scope: InstrumentationScope::builder("fixture-scope")
                .with_version("1.2.3")
                .with_schema_url("https://example.test/schema")
                .with_attributes([KeyValue::new("scope.not-in-contract", "ignored")])
                .build(),
        },
        resource,
    )
}
