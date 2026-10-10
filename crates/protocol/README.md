# Protocol boundary

`decode_event(Bytes)` implements [additive scoped events](../../design/event-multiplexer/additive-events.md).
Call it only on an ordinary `/api/event` connection negotiated with
`OpenCode-Event-Scope: 1`. It returns `UpstreamEvent::Connected { epoch }` for an
`evt_`-prefixed connected ID, or `UpstreamEvent::Domain { route, event: Bytes }`.
The latter retains **all** event JSON bytes, including leading/trailing whitespace,
scope, unknown properties, numeric lexemes and escapes. Borrowed `RawValue`s are
used only to inspect metadata without decoding arbitrary payloads.

Every domain event requires an object `scope`. Optional `sessionID` and `locations`
are absent rather than null; Session/workspace IDs use native `ses`/`wrk` prefixes,
and directory is any string. Global absent locations and explicit empty/multiple
Location lists remain distinct in forwarded bytes. Unknown extension fields are
allowed, including deeply nested values and out-of-range JSON numbers. Recognized
duplicate keys follow native JSON.parse last-value-wins semantics, including
escaped property names. Payload `data.sessionID` never establishes authority.

`STREAMING_TYPES` freezes the existing five session-streaming types. Their internal
`Route::Session` uses only `scope.sessionID`; absent identity reaches legacy
listeners only. All other event types, including future types, are broad.
`Route` is a local admission decision, not an exported envelope. Controlled ready
markers are not valid ordinary uplink items. Header negotiation, first-connected
sequencing, duplicate-connected rejection and uplink failure belong to gateway.
The vendored `fixtures/event-scope-v1.json` pins capability, classification and
focused cases; cross-repository compatibility also checks its byte equality with
native Protocol's fixture.

`decode_interest` follows native schema permissiveness: directory is any string;
Session/workspace IDs require only `ses`/`wrk` prefixes. Unknown properties are
ignored. Optional means absent, not null. Counts apply before deduplication;
the result preserves first-seen Session order. HTTP status/tag encoding is owned
by gateway, using the returned message and field.
Ignored extension values are retained only as raw JSON while validating, so they
do not acquire serde numeric-range/depth constraints. Duplicate interest keys use
JSON.parse's last-value-wins semantics (including location keys).

`SseDecoder` accepts arbitrary chunk boundaries, UTF-8 BOM only at stream start,
CR/LF/CRLF, multiline `data` and empty data. Comments and other fields are ignored.
Work is linear in input size and buffered storage is proportional to the configured
bound. The bound counts **all non-newline wire bytes between blank lines**, including
`data:` prefixes, ignored fields and comments; the initial BOM is exempt. CR/LF
separators do not count. This prevents ignored-field flooding bypassing the bound,
and is independent of chunking or line-ending encoding. Complete frames returned
by a `push` belong to the caller, so callers should keep transport chunks bounded.

`finish` never dispatches pending data: incomplete data/non-comment final field is
an error; final comments and clean EOF are accepted. Invalid UTF-8 errors rather
than silently inserting replacement characters. Finish/error closes the decoder;
a reconnect must construct a fresh one.

`event_frame(&[u8])` preserves raw native JSON except normalization of literal pretty-print
CR/CRLF whitespace to LF, required by SSE's newline semantics. Escape sequences,
number lexemes, indentation, property order and other raw contents are unchanged.
Every physical JSON line gets its own `data: ` prefix. `ready_frame` advertises
only session-streaming; `connected_frame` emits only native id/type/data fields.

Constants exported: `EVENT_PATH`, `CONTROLLED_EVENT_PATH`, `EVENT_SCOPE_HEADER`,
`EVENT_SCOPE_VERSION`, `STREAMING_TYPES`,
`INTEREST_PATH_PREFIX`, `INTEREST_PATH_SUFFIX`, `INTEREST_BYTE_CAPACITY`,
`LOCATION_INTEREST_CAPACITY`, `SESSION_INTEREST_CAPACITY`,
`DEFAULT_MAX_SOURCE_FRAME_BYTES`.

Checks (workspace root):

```sh
cargo +1.98.0 test -p bus-proxy-protocol
cargo +1.98.0 clippy -p bus-proxy-protocol --all-targets -- -D warnings
cargo +1.98.0 fmt -p bus-proxy-protocol --check
```
