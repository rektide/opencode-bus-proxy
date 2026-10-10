# Protocol boundary

`decode_source` implements `design/event-multiplexer/source-contract.md`'s top-level
envelope. It retains native event JSON as `RawValue`, validates only that event is
an object, and never derives routing from payload fields. Future broad types and
unfamiliar event properties survive unchanged. Version support, epoch readiness,
duplicate-ready handling and uplink failure belong to gateway; routing and public
classification belong to origin.

`decode_interest` follows native schema permissiveness: directory is any string;
Session/workspace IDs require only `ses`/`wrk` prefixes. Unknown properties are
ignored. Optional means absent, not null. Counts apply before deduplication;
the result preserves first-seen Session order. HTTP status/tag encoding is owned
by gateway, using the returned message and field.

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

`event_frame` preserves raw native JSON except normalization of literal pretty-print
CR/CRLF whitespace to LF, required by SSE's newline semantics. Escape sequences,
number lexemes, indentation, property order and other raw contents are unchanged.
Every physical JSON line gets its own `data: ` prefix. `ready_frame` advertises
only session-streaming; `connected_frame` emits only native id/type/data fields.

Constants exported: `EVENT_PATH`, `CONTROLLED_EVENT_PATH`, `SOURCE_PATH`,
`INTEREST_PATH_PREFIX`, `INTEREST_PATH_SUFFIX`, `INTEREST_BYTE_CAPACITY`,
`LOCATION_INTEREST_CAPACITY`, `SESSION_INTEREST_CAPACITY`,
`DEFAULT_MAX_SOURCE_FRAME_BYTES`.

Checks (workspace root):

```sh
cargo test -p bus-proxy-protocol
cargo clippy -p bus-proxy-protocol --all-targets -- -D warnings
cargo fmt -p bus-proxy-protocol
```
