use std::collections::HashSet;

use serde::Serialize;
use serde_json::Value;

use crate::{INTEREST_BYTE_CAPACITY, LOCATION_INTEREST_CAPACITY, SESSION_INTEREST_CAPACITY};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interest {
    pub sessions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct InterestError {
    pub message: String,
    pub field: &'static str,
}

fn invalid(message: impl Into<String>) -> InterestError {
    InterestError {
        message: message.into(),
        field: "interest",
    }
}

#[derive(Serialize)]
struct Location<'a> {
    directory: &'a str,
    #[serde(rename = "workspaceID", skip_serializing_if = "Option::is_none")]
    workspace_id: Option<&'a str>,
}

#[derive(Serialize)]
struct Normalized<'a> {
    locations: Vec<Location<'a>>,
    sessions: Vec<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<&'a str>,
}

/// Match native schema decoding (strip unknown keys; brands do not imply paths)
/// followed by controlled-feed normalization. Count entries before deduplicating.
pub fn decode_interest(data: &[u8]) -> Result<Interest, InterestError> {
    if data.len() > INTEREST_BYTE_CAPACITY {
        return Err(invalid(format!(
            "Interest exceeds {INTEREST_BYTE_CAPACITY} request bytes"
        )));
    }
    let malformed = || invalid("Invalid event interest request body");
    let value: Value = serde_json::from_slice(data).map_err(|_| malformed())?;
    let object = value.as_object().ok_or_else(malformed)?;
    let locations = object
        .get("locations")
        .and_then(Value::as_array)
        .ok_or_else(malformed)?;
    let sessions = object
        .get("sessions")
        .and_then(Value::as_array)
        .ok_or_else(malformed)?;
    let profile = match object.get("profile") {
        None => None,
        Some(Value::String(profile)) if profile == "location" || profile == "session-streaming" => {
            Some(profile.as_str())
        }
        _ => return Err(malformed()),
    };
    let locations = locations
        .iter()
        .map(|location| {
            let object = location.as_object().ok_or_else(malformed)?;
            let directory = object
                .get("directory")
                .and_then(Value::as_str)
                .ok_or_else(malformed)?;
            let workspace_id = match object.get("workspaceID") {
                None => None,
                Some(Value::String(id)) if id.starts_with("wrk") => Some(id.as_str()),
                _ => return Err(malformed()),
            };
            Ok(Location {
                directory,
                workspace_id,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let sessions = sessions
        .iter()
        .map(|session| {
            session
                .as_str()
                .filter(|id| id.starts_with("ses"))
                .ok_or_else(malformed)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let normalized = Normalized {
        locations,
        sessions,
        profile,
    };
    // This is the native post-schema reserialized UTF-8 bound, separate from
    // the raw HTTP bound. Unknown fields have been stripped, duplicates retained.
    let encoded = serde_json::to_vec(&normalized).map_err(|_| malformed())?;
    if encoded.len() > INTEREST_BYTE_CAPACITY {
        return Err(invalid(format!(
            "Interest exceeds {INTEREST_BYTE_CAPACITY} encoded bytes"
        )));
    }
    if normalized.locations.len() > LOCATION_INTEREST_CAPACITY {
        return Err(invalid(format!(
            "Interest exceeds {LOCATION_INTEREST_CAPACITY} Location entries"
        )));
    }
    if normalized.sessions.len() > SESSION_INTEREST_CAPACITY {
        return Err(invalid(format!(
            "Interest exceeds {SESSION_INTEREST_CAPACITY} Session entries"
        )));
    }
    if normalized.profile != Some("session-streaming") {
        return Err(InterestError {
            message: "This server supports only the session-streaming event profile".into(),
            field: "profile",
        });
    }
    let mut seen = HashSet::with_capacity(normalized.sessions.len());
    Ok(Interest {
        sessions: normalized
            .sessions
            .into_iter()
            .filter(|session| seen.insert(*session))
            .map(str::to_owned)
            .collect(),
    })
}
