#![cfg(feature = "otel-traces")]
mod common;
use bus_proxy_export_arrow::{
    arrow_array::{Array, ListArray, MapArray, StringArray, TimestampNanosecondArray, UInt64Array},
    opentelemetry::{
        Array as OtelArray, Value,
        trace::{SpanContext, SpanKind, Status},
    },
    traces::{Policy, attribute_string, project},
};
use std::time::{Duration, UNIX_EPOCH};

fn text(batch: &bus_proxy_export_arrow::arrow_array::RecordBatch, name: &str) -> String {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_owned()
}

#[test]
fn projection_preserves_supported_semantics_and_empty_values() {
    let (span, resource) = common::fixture();
    let batch = project(std::slice::from_ref(&span), &resource, &Policy::default()).unwrap();
    assert_eq!(batch.schema(), bus_proxy_export_arrow::schema());
    assert_eq!(text(&batch, "SpanKind"), "Server");
    assert_eq!(text(&batch, "StatusCode"), "Error");
    assert_eq!(text(&batch, "TraceId"), "00000000000000000000000000abcdef");
    assert_eq!(text(&batch, "SpanId"), "0000000000000123");
    assert_eq!(text(&batch, "ParentSpanId"), "");
    assert_eq!(text(&batch, "TraceState"), "vendor=state");
    assert_eq!(text(&batch, "ServiceName"), "fixture-service");
    assert_eq!(text(&batch, "ScopeName"), "fixture-scope");
    assert_eq!(text(&batch, "ScopeVersion"), "1.2.3");
    assert_eq!(
        batch
            .column_by_name("Timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        1_700_000_000_123_456_780
    );
    assert_eq!(
        batch
            .column_by_name("Duration")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        120
    );
    let maps = batch
        .column_by_name("ResourceAttributes")
        .unwrap()
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();
    assert_eq!(
        maps.value(0).len(),
        3,
        "secret resource key was not removed"
    );
    for name in [
        "Events.Timestamp",
        "Events.Name",
        "Events.Attributes",
        "Links.TraceId",
        "Links.SpanId",
        "Links.TraceState",
        "Links.Attributes",
    ] {
        assert_eq!(
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_length(0),
            2
        );
    }
    let mut empty = span;
    empty.span_context = SpanContext::empty_context();
    empty.events = Default::default();
    empty.links = Default::default();
    empty.status = Status::Unset;
    let batch = project(&[empty], &resource, &Policy::default()).unwrap();
    assert_eq!(text(&batch, "TraceId"), "");
    assert_eq!(text(&batch, "SpanId"), "");
    assert_eq!(text(&batch, "StatusCode"), "Unset");
    assert_eq!(text(&batch, "StatusMessage"), "");
    for name in ["Events.Name", "Links.TraceId"] {
        assert_eq!(
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_length(0),
            0
        );
    }
    assert_eq!(
        project(&[], &resource, &Policy::default())
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn pdata_stringification_including_floats_arrays_and_escaping() {
    let policy = Policy::default();
    for (value, expected) in [
        (0.0, "0"),
        (-0.0, "-0"),
        (1.0, "1"),
        (1e-6, "0.000001"),
        (1e-7, "1e-7"),
        (1e20, "100000000000000000000"),
        (1e21, "1e+21"),
        (f64::NAN, "json: unsupported value: NaN"),
        (f64::INFINITY, "json: unsupported value: +Inf"),
        (f64::NEG_INFINITY, "json: unsupported value: -Inf"),
    ] {
        assert_eq!(
            attribute_string(&Value::F64(value), &policy).unwrap(),
            expected
        );
    }
    assert_eq!(
        attribute_string(
            &Value::Array(OtelArray::String(vec!["quote\"\\\n<>&\u{2028}雪".into()])),
            &policy
        )
        .unwrap(),
        "[\"quote\\\"\\\\\\n\\u003c\\u003e\\u0026\\u2028雪\"]"
    );
    assert_eq!(
        attribute_string(&Value::Array(OtelArray::F64(vec![f64::NAN])), &policy).unwrap(),
        ""
    );
    assert_eq!(
        attribute_string(
            &Value::Array(OtelArray::I64(vec![i64::MAX, i64::MIN])),
            &policy
        )
        .unwrap(),
        "[9223372036854775807,-9223372036854775808]"
    );
}

#[test]
fn invalid_times_and_limits_are_explicit_no_truncation() {
    let (mut span, resource) = common::fixture();
    let mut policy = Policy {
        max_string_bytes: 3,
        ..Policy::default()
    };
    assert!(project(std::slice::from_ref(&span), &resource, &policy).is_err());
    policy = Policy {
        max_events: 1,
        ..Policy::default()
    };
    assert!(project(std::slice::from_ref(&span), &resource, &policy).is_err());
    policy = Policy {
        max_batch_bytes: 64,
        ..Policy::default()
    };
    assert!(project(std::slice::from_ref(&span), &resource, &policy).is_err());
    span.end_time = span.start_time - Duration::from_nanos(1);
    assert!(project(std::slice::from_ref(&span), &resource, &Policy::default()).is_err());
    span.start_time = UNIX_EPOCH - Duration::from_secs(1);
    assert!(project(std::slice::from_ref(&span), &resource, &Policy::default()).is_err());
    span.start_time = UNIX_EPOCH + Duration::from_nanos(i64::MAX as u64 + 1);
    assert!(project(&[span], &resource, &Policy::default()).is_err());
}

#[test]
fn all_kind_and_status_spellings_match_pdata() {
    let (mut span, resource) = common::fixture();
    for (kind, name) in [
        (SpanKind::Internal, "Internal"),
        (SpanKind::Server, "Server"),
        (SpanKind::Client, "Client"),
        (SpanKind::Producer, "Producer"),
        (SpanKind::Consumer, "Consumer"),
    ] {
        span.span_kind = kind;
        span.status = Status::Ok;
        let batch = project(std::slice::from_ref(&span), &resource, &Policy::default()).unwrap();
        assert_eq!(text(&batch, "SpanKind"), name);
        assert_eq!(text(&batch, "StatusCode"), "Ok");
    }
}
