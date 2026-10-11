//! Completed OTel spans to the collector/HyperDX 22-column contract.
use crate::{AttributesBuilder, attributes_builder, list_builder, schema, timestamp_builder};
use arrow_array::{
    Array as _, ArrayRef, RecordBatch,
    builder::{StringBuilder, UInt64Builder},
};
use arrow_schema::DataType;
use opentelemetry::{
    Array, KeyValue, Value,
    trace::{SpanId, SpanKind, Status, TraceId},
};
use opentelemetry_sdk::{Resource, trace::SpanData};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("trace projection exceeds configured {0} limit (batch rejected, not truncated)")]
    Limit(&'static str),
    #[error(
        "trace timestamp is before Unix epoch, outside signed nanoseconds, or end precedes start"
    )]
    Time,
    #[error("trace projection limits must be positive and batch bytes at most i32::MAX")]
    Config,
    #[error("invalid Arrow trace projection: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
}

/// Hard bounds; oversize projection fails rather than silently losing fields.
/// Exact case-insensitive key redaction applies to all exported attribute maps.
#[derive(Clone, Debug)]
pub struct Policy {
    pub max_spans: usize,
    pub max_string_bytes: usize,
    pub max_attributes: usize,
    pub max_events: usize,
    pub max_links: usize,
    pub max_array_items: usize,
    pub max_batch_bytes: usize,
    pub redact_keys: Vec<String>,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            max_spans: 4096,
            max_string_bytes: 16 * 1024,
            max_attributes: 1024,
            max_events: 128,
            max_links: 128,
            max_array_items: 1024,
            max_batch_bytes: 16 * 1024 * 1024,
            redact_keys: [
                "authorization",
                "password",
                "token",
                "cookie",
                "set-cookie",
                "http.request.header.authorization",
                "http.request.header.cookie",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        }
    }
}
impl Policy {
    pub fn validate(&self) -> Result<(), Error> {
        if [
            self.max_spans,
            self.max_string_bytes,
            self.max_attributes,
            self.max_events,
            self.max_links,
            self.max_array_items,
            self.max_batch_bytes,
        ]
        .contains(&0)
            || self.max_batch_bytes > i32::MAX as usize
        {
            return Err(Error::Config);
        }
        Ok(())
    }
}
struct Budget<'a> {
    policy: &'a Policy,
    used: usize,
}
impl Budget<'_> {
    fn add(&mut self, bytes: usize) -> Result<(), Error> {
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or(Error::Limit("batch bytes"))?;
        if self.used > self.policy.max_batch_bytes {
            return Err(Error::Limit("batch bytes"));
        }
        Ok(())
    }
    fn text(&mut self, value: &str) -> Result<(), Error> {
        if value.len() > self.policy.max_string_bytes {
            return Err(Error::Limit("string bytes"));
        }
        self.add(value.len() + 8)
    }
    fn attributes<'a>(
        &mut self,
        builder: &mut AttributesBuilder,
        attributes: impl IntoIterator<Item = (&'a str, &'a Value)>,
    ) -> Result<(), Error> {
        let mut unique = BTreeMap::new();
        for (i, (key, value)) in attributes.into_iter().enumerate() {
            if i >= self.policy.max_attributes {
                return Err(Error::Limit("attributes"));
            }
            if self
                .policy
                .redact_keys
                .iter()
                .any(|redact| key.eq_ignore_ascii_case(redact))
            {
                continue;
            }
            // pdata.Map is unique-keyed. Last value wins if a caller supplies
            // duplicate SDK KeyValues, rather than writing duplicate CH keys.
            unique.insert(key, value);
        }
        for (key, value) in unique {
            let value = attribute_string(value, self.policy)?;
            self.text(key)?;
            self.text(&value)?;
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        self.add(8)?;
        builder.append(true)?;
        Ok(())
    }
}
fn key_values(values: &[KeyValue]) -> impl Iterator<Item = (&str, &Value)> {
    values.iter().map(|kv| (kv.key.as_str(), &kv.value))
}
fn timestamp(time: SystemTime) -> Result<i64, Error> {
    i64::try_from(
        time.duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Time)?
            .as_nanos(),
    )
    .map_err(|_| Error::Time)
}
fn trace_id(id: TraceId) -> String {
    if id == TraceId::INVALID {
        String::new()
    } else {
        id.to_string()
    }
}
fn span_id(id: SpanId) -> String {
    if id == SpanId::INVALID {
        String::new()
    } else {
        id.to_string()
    }
}
fn kind(kind: &SpanKind) -> &'static str {
    match kind {
        SpanKind::Internal => "Internal",
        SpanKind::Server => "Server",
        SpanKind::Client => "Client",
        SpanKind::Producer => "Producer",
        SpanKind::Consumer => "Consumer",
    }
}

fn float(value: f64) -> String {
    if !value.is_finite() {
        return format!(
            "json: unsupported value: {}",
            if value.is_nan() {
                "NaN"
            } else if value.is_sign_positive() {
                "+Inf"
            } else {
                "-Inf"
            }
        );
    }
    if value == 0.0 && value.is_sign_negative() {
        return "-0".into();
    }
    ryu_js::Buffer::new().format_finite(value).to_owned()
}
fn json_string(value: &str) -> String {
    // Go encoding/json (pdata Slice.AsRaw) uses HTML escaping by default.
    serde_json::to_string(value)
        .expect("string JSON serialization cannot fail")
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}
/// pdata pcommon.Value.AsString semantics for the types Rust OTel supports.
pub fn attribute_string(value: &Value, policy: &Policy) -> Result<String, Error> {
    let result = match value {
        Value::String(value) => {
            if value.as_str().len() > policy.max_string_bytes {
                return Err(Error::Limit("string bytes"));
            }
            value.as_str().to_owned()
        }
        Value::Bool(value) => value.to_string(),
        Value::I64(value) => value.to_string(),
        Value::F64(value) => float(*value),
        Value::Array(array) => {
            let len = match array {
                Array::Bool(v) => v.len(),
                Array::I64(v) => v.len(),
                Array::F64(v) => v.len(),
                Array::String(v) => v.len(),
                _ => return Err(Error::Limit("unsupported attribute array type")),
            };
            if len > policy.max_array_items {
                return Err(Error::Limit("attribute array items"));
            }
            if let Array::String(values) = array {
                let bytes = values
                    .iter()
                    .try_fold(0usize, |n, v| n.checked_add(v.as_str().len() + 3))
                    .ok_or(Error::Limit("string bytes"))?;
                if bytes > policy.max_string_bytes {
                    return Err(Error::Limit("string bytes"));
                }
            }
            if matches!(array, Array::F64(values) if values.iter().any(|v| !v.is_finite())) {
                String::new()
            } else {
                let values: Vec<String> = match array {
                    Array::Bool(v) => v.iter().map(ToString::to_string).collect(),
                    Array::I64(v) => v.iter().map(ToString::to_string).collect(),
                    Array::F64(v) => v.iter().map(|v| float(*v)).collect(),
                    Array::String(v) => v.iter().map(|v| json_string(v.as_str())).collect(),
                    _ => return Err(Error::Limit("unsupported attribute array type")),
                };
                format!("[{}]", values.join(","))
            }
        }
        _ => return Err(Error::Limit("unsupported attribute type")),
    };
    if result.len() > policy.max_string_bytes {
        return Err(Error::Limit("string bytes"));
    }
    Ok(result)
}

pub fn project(
    spans: &[SpanData],
    resource: &Resource,
    policy: &Policy,
) -> Result<RecordBatch, Error> {
    policy.validate()?;
    if spans.len() > policy.max_spans {
        return Err(Error::Limit("spans"));
    }
    let mut budget = Budget { policy, used: 0 };
    let mut timestamps = timestamp_builder();
    let mut strings: [StringBuilder; 11] = std::array::from_fn(|_| StringBuilder::new());
    let mut resources = attributes_builder();
    let mut attributes = attributes_builder();
    let mut durations = UInt64Builder::new();
    let ts_type = timestamps.finish().data_type().clone();
    let map_type = attributes.finish().data_type().clone();
    let mut event_times = list_builder(timestamp_builder(), ts_type);
    let mut event_names = list_builder(StringBuilder::new(), DataType::Utf8);
    let mut event_attributes = list_builder(attributes_builder(), map_type.clone());
    let mut link_traces = list_builder(StringBuilder::new(), DataType::Utf8);
    let mut link_spans = list_builder(StringBuilder::new(), DataType::Utf8);
    let mut link_states = list_builder(StringBuilder::new(), DataType::Utf8);
    let mut link_attributes = list_builder(attributes_builder(), map_type);
    let service = resource
        .get(&opentelemetry::Key::new("service.name"))
        .map(|v| attribute_string(&v, policy))
        .transpose()?
        .unwrap_or_default();
    for span in spans {
        budget.add(64)?;
        timestamps.append_value(timestamp(span.start_time)?);
        durations.append_value(
            u64::try_from(
                span.end_time
                    .duration_since(span.start_time)
                    .map_err(|_| Error::Time)?
                    .as_nanos(),
            )
            .map_err(|_| Error::Time)?,
        );
        let (status, message) = match &span.status {
            Status::Unset => ("Unset", ""),
            Status::Ok => ("Ok", ""),
            Status::Error { description } => ("Error", description.as_ref()),
        };
        for value in [
            span.name.as_ref(),
            span.instrumentation_scope.name(),
            span.instrumentation_scope.version().unwrap_or_default(),
            message,
        ] {
            if value.len() > policy.max_string_bytes {
                return Err(Error::Limit("string bytes"));
            }
        }
        let values = [
            trace_id(span.span_context.trace_id()),
            span_id(span.span_context.span_id()),
            span_id(span.parent_span_id),
            span.span_context.trace_state().header(),
            span.name.to_string(),
            kind(&span.span_kind).to_owned(),
            service.clone(),
            span.instrumentation_scope.name().to_owned(),
            span.instrumentation_scope
                .version()
                .unwrap_or_default()
                .to_owned(),
            status.to_owned(),
            message.to_owned(),
        ];
        for (builder, value) in strings.iter_mut().zip(values) {
            budget.text(&value)?;
            builder.append_value(value);
        }
        budget.attributes(
            &mut resources,
            resource.iter().map(|(k, v)| (k.as_str(), v)),
        )?;
        budget.attributes(&mut attributes, key_values(&span.attributes))?;
        if span.events.len() > policy.max_events {
            return Err(Error::Limit("events"));
        }
        for event in span.events.iter() {
            budget.add(8)?;
            event_times
                .values()
                .append_value(timestamp(event.timestamp)?);
            budget.text(&event.name)?;
            event_names.values().append_value(event.name.as_ref());
            budget.attributes(event_attributes.values(), key_values(&event.attributes))?;
        }
        event_times.append(true);
        event_names.append(true);
        event_attributes.append(true);
        if span.links.len() > policy.max_links {
            return Err(Error::Limit("links"));
        }
        for link in span.links.iter() {
            let trace = trace_id(link.span_context.trace_id());
            let id = span_id(link.span_context.span_id());
            let state = link.span_context.trace_state().header();
            for value in [&trace, &id, &state] {
                budget.text(value)?;
            }
            link_traces.values().append_value(trace);
            link_spans.values().append_value(id);
            link_states.values().append_value(state);
            budget.attributes(link_attributes.values(), key_values(&link.attributes))?;
        }
        link_traces.append(true);
        link_spans.append(true);
        link_states.append(true);
        link_attributes.append(true);
    }
    let mut string_arrays: Vec<ArrayRef> = strings
        .iter_mut()
        .take(11)
        .map(|s| Arc::new(s.finish()) as ArrayRef)
        .collect();
    let mut columns: Vec<ArrayRef> = vec![Arc::new(timestamps.finish())];
    columns.extend(string_arrays.drain(..7));
    columns.push(Arc::new(resources.finish()));
    columns.extend(string_arrays.drain(..2));
    columns.push(Arc::new(attributes.finish()));
    columns.push(Arc::new(durations.finish()));
    columns.append(&mut string_arrays);
    columns.extend([
        Arc::new(event_times.finish()) as ArrayRef,
        Arc::new(event_names.finish()),
        Arc::new(event_attributes.finish()),
        Arc::new(link_traces.finish()),
        Arc::new(link_spans.finish()),
        Arc::new(link_states.finish()),
        Arc::new(link_attributes.finish()),
    ]);
    Ok(RecordBatch::try_new(schema(), columns)?)
}
