use bus_proxy_protocol::{
    INTEREST_BYTE_CAPACITY, LOCATION_INTEREST_CAPACITY, ProtocolError, Route,
    SESSION_INTEREST_CAPACITY, SourceItem, SseDecoder, connected_frame, decode_interest,
    decode_source, event_frame, ready_frame,
};
use serde_json::{Value, json, value::RawValue};

fn interest(
    value: Value,
) -> Result<bus_proxy_protocol::Interest, bus_proxy_protocol::InterestError> {
    decode_interest(&serde_json::to_vec(&value).unwrap())
}

#[test]
fn source_shape_and_route_are_independent_of_native_payload() {
    let input = br#"{"type":"event-source.event","route":{"kind":"session","sessionID":"ses_owner"},"event":{ "type" : "session.text.delta", "data":{"sessionID":"ses_spoof"} }}"#;
    let SourceItem::Event { route, event } = decode_source(input).unwrap() else {
        panic!("expected event")
    };
    assert_eq!(
        route,
        Route::Session {
            session_id: Some("ses_owner".into())
        }
    );
    assert_eq!(
        event.get(),
        r#"{ "type" : "session.text.delta", "data":{"sessionID":"ses_spoof"} }"#
    );
    let input = br#"{"type":"event-source.event","route":{"kind":"session"},"event":{"data":{"sessionID":"ses_spoof"}}}"#;
    assert!(matches!(
        decode_source(input).unwrap(),
        SourceItem::Event {
            route: Route::Session { session_id: None },
            ..
        }
    ));
}

#[test]
fn future_broad_event_is_not_schema_decoded_or_reserialized() {
    let event = r#"{ "type":"future.public.event", "largeNumber":900719925474099312345, "float":1.2300e+03, "escaped":"\u0061", "data":{"sessionID":"ses_spoof"} }"#;
    let input = format!(
        r#"{{"unknown":true,"event":{event},"route":{{"kind":"broadcast"}},"type":"event-source.event"}}"#
    );
    let SourceItem::Event { route, event: raw } = decode_source(input.as_bytes()).unwrap() else {
        panic!("expected event")
    };
    assert_eq!(route, Route::Broadcast);
    assert_eq!(raw.get(), event);
    assert_eq!(event_frame(&raw), format!("data: {event}\n\n"));
}

#[test]
fn ready_structure_but_not_version_or_epoch_policy_is_checked() {
    assert!(matches!(
        decode_source(br#"{"type":"event-source.ready","version":42,"epoch":""}"#).unwrap(),
        SourceItem::Ready { version: 42, epoch } if epoch.is_empty()
    ));
    for input in [
        r#"{"type":"event-source.ready","version":-1,"epoch":"e"}"#,
        r#"{"type":"event-source.ready","version":4294967296,"epoch":"e"}"#,
        r#"{"type":"event-source.ready","version":1.0,"epoch":"e"}"#,
        r#"{"type":"event-source.ready","version":1,"epoch":null}"#,
        r#"{"type":"event-source.ready","version":1}"#,
        r#"{"type":"event-source.ready","data":{"version":1,"epoch":"e"}}"#,
        r#"{"type":"event-source.event","event":{},"route":{"kind":"other"}}"#,
        r#"{"type":"event-source.event","event":{},"route":{"kind":"session","sessionID":null}}"#,
        r#"{"type":"event-source.event","event":{},"route":{"kind":"session","sessionID":2}}"#,
        r#"{"type":"event-source.event","event":{}}"#,
        r#"{"type":"event-source.event","route":{"kind":"broadcast"}}"#,
        r#"{"type":"event-source.event","event":null,"route":{"kind":"broadcast"}}"#,
        r#"{"type":"event-source.event","event":[],"route":{"kind":"broadcast"}}"#,
        r#"{"type":"event-source.event","event":"x","route":{"kind":"broadcast"}}"#,
        r#"{"type":"unknown"}"#,
        r#"{"type":"event-source.ready","type":"event-source.event"}"#,
        r#"{"type":"event-source.ready","version":1,"epoch":"e"} trailing"#,
    ] {
        assert!(decode_source(input.as_bytes()).is_err(), "accepted {input}");
    }
}

#[test]
fn interest_matches_native_brands_unknown_fields_and_dedup_order() {
    let result = interest(json!({
        "locations":[{"directory":"", "workspaceID":"wrk", "unknown":null},
                     {"directory":"relative/path"}],
        "sessions":["ses", "ses_not-fixed-length", "ses", "ses_other"],
        "profile":"session-streaming",
        "unknown": {"ignored":true}
    }))
    .unwrap();
    assert_eq!(
        result.sessions,
        ["ses", "ses_not-fixed-length", "ses_other"]
    );
    assert!(
        interest(json!({"locations":[],"sessions":[],"profile":"session-streaming"}))
            .unwrap()
            .sessions
            .is_empty()
    );
}

#[test]
fn ignored_interest_fields_do_not_acquire_number_or_depth_constraints() {
    let nested = format!("{}0{}", "[".repeat(256), "]".repeat(256));
    let input = format!(
        r#"{{"locations":[{{"directory":"relative","future":{nested},"number":1e400}}],"sessions":["ses"],"profile":"session-streaming","future":{nested},"number":1e400}}"#
    );
    assert_eq!(decode_interest(input.as_bytes()).unwrap().sessions, ["ses"]);
}

#[test]
fn native_interest_duplicate_properties_use_the_last_value() {
    let input = br#"{"locations":null,"locations":[{"directory":null,"directory":"relative","workspaceID":false,"workspaceID":"wrk"}],"sessions":null,"sessions":["ses"],"profile":null,"profile":"session-streaming"}"#;
    assert_eq!(decode_interest(input).unwrap().sessions, ["ses"]);
    let input = br#"{"locations":[],"sessions":[],"profile":"session-streaming","profile":null}"#;
    assert_eq!(decode_interest(input).unwrap_err().field, "interest");
}

#[test]
fn source_large_raw_event_does_not_require_native_number_or_depth_decoding() {
    let nested = format!("{}0{}", "[".repeat(256), "]".repeat(256));
    let raw = format!(
        r#"{{"number":1e400,"future":{nested},"payload":"{}"}}"#,
        "x".repeat(1024 * 1024)
    );
    let input =
        format!(r#"{{"type":"event-source.event","route":{{"kind":"broadcast"}},"event":{raw}}}"#);
    let SourceItem::Event { event, .. } = decode_source(input.as_bytes()).unwrap() else {
        panic!("expected event")
    };
    assert_eq!(event.get(), raw);
}

#[test]
fn interest_missing_profile_differs_from_null_or_unknown() {
    for value in [
        json!({"locations":[],"sessions":[]}),
        json!({"locations":[],"sessions":[],"profile":"location"}),
    ] {
        assert_eq!(interest(value).unwrap_err().field, "profile");
    }
    for value in [
        json!({"locations":[],"sessions":[],"profile":null}),
        json!({"locations":[],"sessions":[],"profile":"unknown"}),
        json!({"locations":[],"sessions":[],"profile":1}),
        json!({"sessions":[],"profile":"session-streaming"}),
        json!({"locations":[],"profile":"session-streaming"}),
        json!({"locations":null,"sessions":[],"profile":"session-streaming"}),
        json!({"locations":{},"sessions":[],"profile":"session-streaming"}),
        json!({"locations":[],"sessions":[null],"profile":"session-streaming"}),
        json!({"locations":[],"sessions":["other"],"profile":"session-streaming"}),
        json!({"locations":[{"directory":null}],"sessions":[],"profile":"session-streaming"}),
        json!({"locations":[{"directory":"x","workspaceID":null}],"sessions":[],"profile":"session-streaming"}),
        json!({"locations":[{"directory":"x","workspaceID":"workspace"}],"sessions":[],"profile":"session-streaming"}),
        json!([]),
    ] {
        let error = interest(value.clone()).unwrap_err();
        assert_eq!(error.field, "interest", "{value}");
        assert_eq!(error.message, "Invalid event interest request body");
    }
    assert_eq!(decode_interest(b"not JSON").unwrap_err().field, "interest");
    assert!(decode_interest(b"{\xff}").is_err());
}

#[test]
fn interest_counts_entries_before_deduplication() {
    for count in [SESSION_INTEREST_CAPACITY, SESSION_INTEREST_CAPACITY + 1] {
        let result = interest(json!({
            "locations":[],"sessions":vec!["ses_same"; count],"profile":"session-streaming"
        }));
        if count == SESSION_INTEREST_CAPACITY {
            assert_eq!(result.unwrap().sessions, ["ses_same"]);
        } else {
            assert!(result.unwrap_err().message.contains("Session entries"));
        }
    }
    for count in [LOCATION_INTEREST_CAPACITY, LOCATION_INTEREST_CAPACITY + 1] {
        let result = interest(json!({
            "locations":vec![json!({"directory":""}); count],
            "sessions":[],"profile":"session-streaming"
        }));
        if count == LOCATION_INTEREST_CAPACITY {
            assert!(result.is_ok());
        } else {
            assert!(result.unwrap_err().message.contains("Location entries"));
        }
    }
}

#[test]
fn interest_byte_limit_accepts_equality_and_counts_raw_whitespace() {
    let base = br#"{"locations":[],"sessions":[],"profile":"session-streaming"}"#;
    let mut bytes = vec![b' '; INTEREST_BYTE_CAPACITY - base.len()];
    bytes.extend_from_slice(base);
    assert!(decode_interest(&bytes).is_ok());
    bytes.push(b' ');
    let error = decode_interest(&bytes).unwrap_err();
    assert!(error.message.contains("request bytes"));
    assert_eq!(error.field, "interest");
}

#[test]
fn interest_unicode_limit_counts_bytes_not_characters() {
    let value = json!({"locations":[{"directory":"🦀".repeat(INTEREST_BYTE_CAPACITY / 4)}],
                       "sessions":[],"profile":"session-streaming"});
    assert!(
        interest(value)
            .unwrap_err()
            .message
            .contains("request bytes")
    );
}

fn decode_chunks(input: &[u8], chunk_size: usize, limit: usize) -> Vec<Vec<u8>> {
    let mut decoder = SseDecoder::new(limit);
    let mut output = Vec::new();
    for chunk in input.chunks(chunk_size) {
        output.extend(decoder.push(chunk).unwrap());
    }
    decoder.finish().unwrap();
    output
}

#[test]
fn sse_all_chunk_sizes_and_line_endings() {
    for ending in ["\n", "\r", "\r\n"] {
        let input = format!(
            "\u{feff}: heartbeat{ending}{ending}event: ignored{ending}id: ignored{ending}data: first{ending}data:  second 🦀{ending}data{ending}{ending}data: last{ending}{ending}"
        );
        for size in 1..=input.len() {
            assert_eq!(
                decode_chunks(input.as_bytes(), size, 1024),
                ["first\n second 🦀\n".as_bytes(), b"last"],
                "ending={ending:?}, size={size}"
            );
        }
    }
}

#[test]
fn sse_empty_data_dispatches_but_comments_and_unknown_fields_do_not() {
    let input = b"\n: comment\n\nretry: 2000\nevent: something\nid: x\nunknown\n\ndata\n\ndata:\n\ndata: \n\ndata:\tkept\n\ndata: \xef\xbb\xbfkept\n\n";
    assert_eq!(
        decode_chunks(input, 1, 512),
        [b"".as_slice(), b"", b"", b"\tkept", b"\xef\xbb\xbfkept"]
    );
}

#[test]
fn sse_mixed_newlines_and_fragmented_crlf_do_not_duplicate_frames() {
    let input = b"data: a\r\ndata: b\r\r\ndata: c\n\r\ndata: d\r\n\r\n";
    assert_eq!(
        decode_chunks(input, 1, 100),
        [b"a\nb".as_slice(), b"c", b"d"]
    );
}

#[test]
fn sse_bounds_include_ignored_lines_and_reset_between_frames() {
    assert_eq!(decode_chunks(b"data: x\n\ndata: y\n\n", 1, 7), [b"x", b"y"]);
    for input in [
        b"data: xx\n\n".as_slice(),
        b":1234567\n\n",
        b"unknown_field_without_colon",
        b":123\n:123\n\n",
        b"data:\ndata:\n\n",
    ] {
        let mut decoder = SseDecoder::new(7);
        assert!(matches!(
            decoder.push(input),
            Err(ProtocolError::FrameTooLarge { limit: 7 })
        ));
        assert!(matches!(
            decoder.push(b"\n\n"),
            Err(ProtocolError::DecoderClosed)
        ));
        assert!(matches!(
            decoder.finish(),
            Err(ProtocolError::DecoderClosed)
        ));
    }
    let mut decoder = SseDecoder::new(0);
    assert!(decoder.push(b"\n\r\n").unwrap().is_empty());
    assert!(matches!(
        decoder.push(b":"),
        Err(ProtocolError::FrameTooLarge { limit: 0 })
    ));
}

#[test]
fn sse_utf8_and_partial_bom_fail_without_replacement_characters() {
    for input in [b"data: \xff\n\n".as_slice(), b":\xff\n\n", b"\xefX\n\n"] {
        let mut decoder = SseDecoder::new(100);
        assert!(matches!(
            decoder.push(input),
            Err(ProtocolError::InvalidUtf8)
        ));
    }
    for input in [b"\xef".as_slice(), b"\xef\xbb", b"data: \xf0\x9f"] {
        let mut decoder = SseDecoder::new(100);
        decoder.push(input).unwrap();
        assert!(matches!(decoder.finish(), Err(ProtocolError::InvalidUtf8)));
    }
}

#[test]
fn sse_eof_does_not_dispatch_truncated_source_data() {
    for input in [
        b"data: x".as_slice(),
        b"data: x\n",
        b"data: x\r\n",
        b"data",
        b"event: x",
    ] {
        let mut decoder = SseDecoder::new(100);
        assert!(decoder.push(input).unwrap().is_empty());
        assert!(matches!(
            decoder.finish(),
            Err(ProtocolError::IncompleteFrame)
        ));
    }
    for input in [
        b"".as_slice(),
        b"\xef\xbb\xbf",
        b": heartbeat",
        b": heartbeat\r",
        b"id: x\n",
        b"data: x\r\r",
    ] {
        let mut decoder = SseDecoder::new(100);
        decoder.push(input).unwrap();
        decoder.finish().unwrap();
        assert!(matches!(
            decoder.push(b""),
            Err(ProtocolError::DecoderClosed)
        ));
    }
}

#[test]
fn large_frame_and_one_byte_chunks_work_at_the_exact_bound() {
    let payload = "x".repeat(256 * 1024);
    let input = format!("data: {payload}\n\n");
    assert_eq!(
        decode_chunks(input.as_bytes(), 1, payload.len() + 6),
        [payload.as_bytes()]
    );
    let mut decoder = SseDecoder::new(payload.len() + 5);
    assert!(matches!(
        decoder.push(input.as_bytes()),
        Err(ProtocolError::FrameTooLarge { .. })
    ));
}

#[test]
fn native_opening_frames_are_json_escaped_and_transport_local() {
    let ready = decode_chunks(&ready_frame("evsub_\"\n"), 1, 1024);
    let value: Value = serde_json::from_slice(&ready[0]).unwrap();
    assert_eq!(
        value,
        json!({"type":"event-feed.ready", "data":{
            "subscriptionID":"evsub_\"\n", "profiles":["session-streaming"]
        }})
    );
    let first = connected_frame();
    let second = connected_frame();
    assert_ne!(first, second);
    let connected = decode_chunks(&first, 1, 1024);
    let value: Value = serde_json::from_slice(&connected[0]).unwrap();
    assert_eq!(value["type"], "server.connected");
    assert_eq!(value["data"], json!({}));
    assert!(value["id"].as_str().unwrap().starts_with("evt_"));
    assert_eq!(value.as_object().unwrap().len(), 3);
}

#[test]
fn pretty_printed_raw_events_use_multiline_sse_not_bare_lines() {
    for newline in ["\n", "\r", "\r\n"] {
        let raw = format!(
            "{{{newline}  \"type\": \"future.public\",{newline}  \"data\": {{\"escaped\":\"\\n\"}}{newline}}}"
        );
        let event = RawValue::from_string(raw.clone()).unwrap();
        let frame = event_frame(&event);
        let output = decode_chunks(&frame, 1, 1024);
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0],
            raw.replace("\r\n", "\n").replace('\r', "\n").as_bytes()
        );
        let _: Value = serde_json::from_slice(&output[0]).unwrap();
    }
}
