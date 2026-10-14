use std::collections::{HashMap, HashSet};

use serde::Serialize;
use serde_json::value::RawValue;

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
    // Raw unknown fields need only be syntactically valid JSON: native schema
    // strips them before validating values. In particular, parsing all fields as
    // Value would reject ignored huge numbers or deeply nested extension values.
    // Map decoding also preserves JSON.parse's last-key-wins duplicate semantics.
    let object: HashMap<String, Box<RawValue>> =
        serde_json::from_slice(data).map_err(|_| malformed())?;
    let locations: Vec<HashMap<String, Box<RawValue>>> =
        serde_json::from_str(object.get("locations").ok_or_else(malformed)?.get())
            .map_err(|_| malformed())?;
    let sessions: Vec<String> =
        serde_json::from_str(object.get("sessions").ok_or_else(malformed)?.get())
            .map_err(|_| malformed())?;
    let profile = match object.get("profile") {
        None => None,
        Some(raw) => {
            let profile: String = serde_json::from_str(raw.get()).map_err(|_| malformed())?;
            if profile != "location" && profile != "session-streaming" {
                return Err(malformed());
            }
            Some(profile)
        }
    };
    let decoded_locations = locations
        .iter()
        .map(|object| {
            let directory: String =
                serde_json::from_str(object.get("directory").ok_or_else(malformed)?.get())
                    .map_err(|_| malformed())?;
            let workspace_id = match object.get("workspaceID") {
                None => None,
                Some(raw) => {
                    let id: String = serde_json::from_str(raw.get()).map_err(|_| malformed())?;
                    if !id.starts_with("wrk") {
                        return Err(malformed());
                    }
                    Some(id)
                }
            };
            Ok((directory, workspace_id))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let locations = decoded_locations
        .iter()
        .map(|(directory, workspace_id)| Location {
            directory,
            workspace_id: workspace_id.as_deref(),
        })
        .collect();
    let sessions = sessions
        .iter()
        .map(|session| {
            if !session.starts_with("ses") {
                return Err(malformed());
            }
            Ok(session.as_str())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let normalized = Normalized {
        locations,
        sessions,
        profile: profile.as_deref(),
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
