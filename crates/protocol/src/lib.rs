//! Wire contracts and bounded streaming decode for the event gateway.

mod interest;
mod sse;

pub use interest::{Interest, InterestError, decode_interest};
pub use sse::SseDecoder;

use bytes::Bytes;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::collections::HashMap;

pub const EVENT_PATH: &str = "/api/event";
pub const CONTROLLED_EVENT_PATH: &str = "/api/experimental/event";
pub const EVENT_SCOPE_HEADER: &str = "OpenCode-Event-Scope";
pub const EVENT_SCOPE_VERSION: &str = "1";
pub const STREAMING_TYPES: [&str; 5] = [
    "session.text.delta",
    "session.reasoning.delta",
    "session.tool.input.delta",
    "session.tool.progress",
    "session.compaction.delta",
];
pub const INTEREST_PATH_PREFIX: &str = "/api/experimental/event/subscriptions/";
pub const INTEREST_PATH_SUFFIX: &str = "/interests";
pub const INTEREST_BYTE_CAPACITY: usize = 256 * 1024;
pub const LOCATION_INTEREST_CAPACITY: usize = 1024;
pub const SESSION_INTEREST_CAPACITY: usize = 4096;
pub const DEFAULT_MAX_SOURCE_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind")]
pub enum Route {
    #[serde(rename = "broadcast")]
    Broadcast,
    #[serde(rename = "session")]
    Session {
        #[serde(rename = "sessionID", default, deserialize_with = "optional_string")]
        session_id: Option<String>,
    },
}

// Missing is optional; explicit null is not a string on the native wire.
fn optional_string<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}

#[derive(Debug)]
pub enum UpstreamEvent {
    Connected { epoch: String },
    Domain { route: Route, event: Bytes },
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("Invalid scoped event: {0}")]
    InvalidSource(#[from] serde_json::Error),
    #[error("Invalid event metadata or scope")]
    InvalidEvent,
    #[error("SSE frame exceeds {limit} bytes")]
    FrameTooLarge { limit: usize },
    #[error("SSE contains invalid UTF-8")]
    InvalidUtf8,
    #[error("SSE ended with an incomplete frame")]
    IncompleteFrame,
    #[error("SSE decoder is closed")]
    DecoderClosed,
}

/// Parse only authoritative metadata on an already negotiated scope-v1 stream.
/// Maps implement native JSON.parse last-key-wins semantics; borrowed raw values
/// skip unknown payloads without numeric/depth constraints or re-encoding them.
pub fn decode_event(data: Bytes) -> Result<UpstreamEvent, ProtocolError> {
    type Object<'a> = HashMap<std::borrow::Cow<'a, str>, &'a RawValue>;
    fn string(object: &Object<'_>, key: &str) -> Result<String, ProtocolError> {
        serde_json::from_str(object.get(key).ok_or(ProtocolError::InvalidEvent)?.get())
            .map_err(ProtocolError::from)
    }
    fn optional(
        object: &Object<'_>,
        key: &str,
        prefix: &str,
    ) -> Result<Option<String>, ProtocolError> {
        if !object.contains_key(key) {
            return Ok(None);
        }
        let value = string(object, key)?;
        if !value.starts_with(prefix) {
            return Err(ProtocolError::InvalidEvent);
        }
        Ok(Some(value))
    }
    let object: Object<'_> = serde_json::from_slice(&data)?;
    let kind = string(&object, "type")?;
    if kind == "server.connected" {
        let epoch = string(&object, "id")?;
        if !epoch.starts_with("evt_") {
            return Err(ProtocolError::InvalidEvent);
        }
        return Ok(UpstreamEvent::Connected { epoch });
    }
    // Controlled transport markers never belong on an ordinary uplink.
    if kind == "event-feed.ready" {
        return Err(ProtocolError::InvalidEvent);
    }
    let scope: Object<'_> = serde_json::from_str(
        object
            .get("scope")
            .ok_or(ProtocolError::InvalidEvent)?
            .get(),
    )?;
    let session_id = optional(&scope, "sessionID", "ses")?;
    if let Some(locations) = scope.get("locations") {
        let locations: Vec<Object<'_>> = serde_json::from_str(locations.get())?;
        for location in locations {
            string(&location, "directory")?;
            optional(&location, "workspaceID", "wrk")?;
        }
    }
    let route = if STREAMING_TYPES.contains(&kind.as_str()) {
        Route::Session { session_id }
    } else {
        Route::Broadcast
    };
    Ok(UpstreamEvent::Domain { route, event: data })
}

/// Preserve raw JSON whitespace, including pretty-printed objects, as SSE data.
pub fn event_frame(raw: &[u8]) -> Bytes {
    let mut frame = Vec::with_capacity(raw.len().saturating_add(8));
    frame.extend_from_slice(b"data: ");
    let mut previous_cr = false;
    for &byte in raw {
        if byte == b'\n' && previous_cr {
            previous_cr = false;
            continue;
        }
        if byte == b'\r' || byte == b'\n' {
            frame.extend_from_slice(b"\ndata: ");
        } else {
            frame.push(byte);
        }
        previous_cr = byte == b'\r';
    }
    frame.extend_from_slice(b"\n\n");
    Bytes::from(frame)
}

pub fn ready_frame(subscription_id: &str) -> Bytes {
    let value = serde_json::json!({
        "type": "event-feed.ready",
        "data": {"subscriptionID": subscription_id, "profiles": ["session-streaming"]}
    });
    Bytes::from(format!("data: {value}\n\n"))
}

pub fn connected_frame() -> Bytes {
    let value = serde_json::json!({
        "id": format!("evt_{}", uuid::Uuid::new_v4().simple()),
        "type": "server.connected",
        "data": {}
    });
    Bytes::from(format!("data: {value}\n\n"))
}
