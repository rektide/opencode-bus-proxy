//! Wire contracts and bounded streaming decode for the event gateway.

mod interest;
mod sse;

pub use interest::{Interest, InterestError, decode_interest};
pub use sse::SseDecoder;

use bytes::Bytes;
use serde::Deserialize;
use serde_json::value::RawValue;

pub const EVENT_PATH: &str = "/api/event";
pub const CONTROLLED_EVENT_PATH: &str = "/api/experimental/event";
pub const SOURCE_PATH: &str = "/api/experimental/event/source";
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
pub enum SourceItem {
    Ready { version: u32, epoch: String },
    Event { route: Route, event: Box<RawValue> },
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("Invalid source envelope: {0}")]
    InvalidSource(#[from] serde_json::Error),
    #[error("Unknown source item type")]
    UnknownSourceType,
    #[error("Source event must be a JSON object")]
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

/// Validate only the source envelope, not the native/public event schema.
/// Version/epoch sequencing and public-event classification belong to the source owner.
pub fn decode_source(data: &[u8]) -> Result<SourceItem, ProtocolError> {
    // Internally tagged serde enums buffer their contents and lose RawValue's
    // original lexical representation. Decode the tag, then the selected struct.
    #[derive(Deserialize)]
    struct Tag<'a> {
        #[serde(rename = "type", borrow)]
        kind: std::borrow::Cow<'a, str>,
    }
    #[derive(Deserialize)]
    struct Ready {
        version: u32,
        epoch: String,
    }
    #[derive(Deserialize)]
    struct Event {
        route: Route,
        event: Box<RawValue>,
    }
    let tag: Tag<'_> = serde_json::from_slice(data)?;
    let item = match tag.kind.as_ref() {
        "event-source.ready" => {
            let Ready { version, epoch } = serde_json::from_slice(data)?;
            SourceItem::Ready { version, epoch }
        }
        "event-source.event" => {
            let Event { route, event } = serde_json::from_slice(data)?;
            SourceItem::Event { route, event }
        }
        _ => return Err(ProtocolError::UnknownSourceType),
    };
    if let SourceItem::Event { event, .. } = &item
        && !event.get().starts_with('{')
    {
        return Err(ProtocolError::InvalidEvent);
    }
    Ok(item)
}

/// Preserve raw JSON whitespace, including pretty-printed objects, as SSE data.
pub fn event_frame(event: &RawValue) -> Bytes {
    let raw = event.get().as_bytes();
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
