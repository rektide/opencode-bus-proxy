# Additive scoped events on the ordinary OpenCode stream

## Decision and motivation

Use **ordinary `GET /api/event`** for the proxy's single upstream connection.
Each business event keeps its normal OpenCode envelope and gains an optional,
server-owned `scope` field. Remove the new infrastructure-only
`/api/experimental/event/source` route and its `event-source.*` JSON envelopes.

The earlier [source contract](source-contract.md) is a preserved predecessor. Its
separate envelope made routing authority explicit but introduced another event
format and route. The user's preferred additive shape provides the same authority
while allowing normal clients, embedded delivery, the proxy, and inspection tools
to operate on the same event shape.

This revision changes both repositories: native bus-smart supplies the scoped
ordinary stream; the Rust gateway consumes and forwards it. It does not change
normal client server-URL selection or Session command/read routes.

## Event shape

Illustrative ordinary public event with the addition:

```json
{
  "id": "evt_example",
  "created": 1,
  "type": "session.text.delta",
  "location": { "directory": "/work/app" },
  "data": {
    "sessionID": "ses_A",
    "assistantMessageID": "msg_example",
    "ordinal": 0,
    "delta": "hello"
  },
  "scope": {
    "sessionID": "ses_A",
    "locations": [{ "directory": "/work/app" }]
  }
}
```

`scope` is descriptive publication-time information:

```ts
type EventScope = {
  sessionID?: SessionID
  locations?: LocationRef[]
}
```

- `sessionID` comes only from `Bus.RoutedEvent.audience.sessionID`.
- A Bus `locations` audience contributes `locations: audience.refs`, including
  an empty array when that is the actual publication-time audience. A global
  audience omits `locations`. Preserve workspace identity in Location references.
- Existing event `location` is preserved; it is not replaced by an audience list.
  A move can concern multiple Locations, unlike that singular field.
- Empty `scope: {}` is a valid explicit absence of authoritative Session/Location
  coordinates. Missing scope is different: an older, unextended transport.
- The field is optional in the general public event schema for old-server
  compatibility. On a stream advertising scope version 1 it is present on every
  public business event, including RPC and broad lifecycle events.
- Stamp/overwrite scope from Bus facts in public delivery. Producer-supplied
  payload or extension fields cannot establish routing authority. Do not mutate
  the original Bus event or add derived scope to durable records/database storage.
- Internal Bus events remain outside the public allowlist.

There is no nested `event` object or `event-source.event` discriminator. SSE remains
ordinary `data: <OpenCode event JSON>\n\n`. The proxy forwards the scoped event;
older consumers may ignore/discard scope. A source/schema probe confirmed the
current native Promise client accepts and retains an added scope field, while
the pre-extension Effect schema accepts and discards it. Updated schemas expose
the optional field for newer typed clients.

## Connection contract

The public GET response advertises:

```http
Content-Type: text/event-stream
OpenCode-Event-Scope: 1
Access-Control-Expose-Headers: OpenCode-Event-Scope
```

Header names are case-insensitive. Version 1 means the authoritative scope
semantics above; absence or another version is not proof of support. The proxy
requires version 1 instead of guessing scope from a payload. Ordinary clients
need not examine the header.

Wire preservation refers to JSON data after SSE decoding. Native compact JSON is
forwarded byte-for-byte; the proxy does not parse/re-serialize the event payload.
SSE line endings are normalized by decoding, and literal CR/CRLF pretty-print
whitespace passed directly to the framing helper normalizes to LF. Numeric
lexemes, escapes, duplicate keys, scope, and payload bytes otherwise remain intact.

The first data item is the existing normal `server.connected` event:

```json
{ "id": "evt_connection", "type": "server.connected", "data": {} }
```

The origin installs observation before sending it. Its ID names that upstream
connection's volatile epoch inside the proxy. It is not a durable event cursor.
Business events may follow only after this marker; another connected marker on
that connection is a protocol failure. Bus-published connected markers are omitted
from domain export; the HTTP stream owns its connection marker.

Scope is not required on transport markers (`server.connected`, or controlled
`event-feed.ready`). Each downstream stream creates its own existing ready/
connected sequence. Upstream markers are consumed by the gateway rather than
forwarded into already attached downstream streams.

Proxy-owned legacy and controlled responses advertise scope version 1 because
they preserve the negotiated scoped business events. This permits the same
contract through another gateway without adding another envelope layer.

## Scope and delivery policy

Scope identifies what a publication concerns. Delivery policy chooses recipients.
For the existing **session-streaming** profile, freeze the current five types:

1. `session.text.delta`
2. `session.reasoning.delta`
3. `session.tool.input.delta`
4. `session.tool.progress`
5. `session.compaction.delta`

For those types, controlled delivery matches `scope.sessionID` against installed
interests. Missing authoritative Session identity reaches legacy listeners only.
All other public types stay broad, including Session lifecycle and RPC events
whose payloads happen to contain `sessionID`.

Native embedded delivery and the Rust distributor implement this same policy.
Maintain a shared fixture/specification and cross-language conformance checks;
adding another filtered type changes the profile contract and needs deliberate
versioning rather than an unnoticed edit to one implementation. A separate
per-event `broadcast`/`session` directive is no longer exported.

The server emits one scoped publication to the proxy per source connection. It
does not receive the proxy's downstream interest sets. Native direct subscribers
continue to be served by the native distributor; proxy subscribers belong to the
Rust distributor. Location scope does not turn the current profile into Location
filtering.

## Route simplification

| Route                                                     | Disposition                                                                                               |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `GET /api/event`                                          | Ordinary unfiltered public stream; gains scoped business events and capability header; sole proxy uplink. |
| `GET /api/experimental/event/source`                      | Remove from native protocol/server/generated clients and Rust source configuration.                       |
| `GET /api/experimental/event`                             | Retain existing controlled handshake for existing bus-smart clients.                                      |
| `PUT /api/experimental/event/subscriptions/:id/interests` | Retain complete replacement; it represents genuine mutable subscription state.                            |
| Other OpenCode API and WebSocket routes                   | Continue ordinary gateway forwarding.                                                                     |

The controlled GET/PUT cannot simply disappear: existing clients use their
activation and in-place replacement contract. Routing information on publications
does not tell the server or proxy what a client wants. Keep these existing control
operations while removing the redundant infrastructure route. No new per-client
endpoint setting or mandatory client upgrade is introduced.

## Native implementation constraints

Consolidate the three existing native public-feed producers into one scoped,
bounded routed hub. Legacy and controlled facades share it. The ordinary route
then supplies both ordinary clients and the proxy without a special producer mode.

Use an ordered bounded command queue for publications and registration/control
barriers. The Bus observer offers once without encoding or waiting on a socket.
The drain selects recipients, skips encoding when nobody needs an event, encodes
once across both modes, and offers shared frames nonblockingly to bounded outputs.
Registration and PUT acknowledgment occur only when their ordered command runs.
This preserves the publication-versus-control ordering of the previous native
admission contract; queued publications preceding a replacement use the earlier
set. Registration after a publication does not replay that publication.

Interest validation still precedes subscription lookup. The drain applies each
complete replacement and admission as a synchronous step, and admits connected
before activation. Saturated control admission fails explicitly rather than
retaining unlimited waiting commands. Shared ingress/encoding failure closes the
dependent generation and settles pending callers; an individual output overflow
isolates that subscriber. New healthy registrations can recover after failure.

Drain output queues in single frames or explicitly bounded batches; account for
work moved out of queues. Keep scope construction at the public boundary so
internal event schemas and durable facts stay minimal. Account physical shared
encodes once and mode-specific admissions separately in the native metrics.

The implementation report will record the selected queue/encoding mechanism and
its accounting tradeoffs. A shape simplification is not permission to reintroduce
per-recipient encoding or wait for network capacity on Session publication.

## Failure, performance, and verification

Maintain count/byte bounds in the proxy, maximum incoming frames, idle detection,
one upstream reconnect owner, and downstream invalidation on every upstream gap.
Interest acknowledgments remain local admission cuts; neither installation nor
scope adds replay or snapshot synchronization.

The added metadata now travels to clients as well as to the proxy. Keep it small
and account for the additional bytes. The gain is one interoperable event shape
and simpler routing boundaries; total CPU or network savings require measurement.

Required checks include ordinary native clients consuming scoped events, updated
Effect decoding retaining scope, payload Session spoofing, global/empty/multi-
Location scope, all five selection types versus broad events, header negotiation,
normal connected markers, exact raw JSON forwarding, source gaps, byte/count
overflow, and direct/proxy equivalence through the real-client loopback harness.

Record the work as separate coherent commits: decision, native transport/schema
simplification, Rust adaptation, cross-language verification, and current operating
documentation. Preserve earlier design and verification receipts as history.

## Concrete cross-language boundary details

- Scope v1 uses the flat object with optional `sessionID` and `locations` above,
  rather than exposing Core's `type`/`refs` implementation names. Optional fields
  may be absent, not null. Session IDs retain native `ses` prefix validation;
  Location directory remains the native string brand, and workspace IDs retain
  `wrk` prefix validation. Unknown extension fields are allowed.
- Native connected IDs require the normal `evt_` prefix (the native brand), not
  a new UUID or fixed-hex format. Missing/invalid connected ID fails negotiation.
- Duplicate JSON keys follow native `JSON.parse` last-value-wins semantics. The
  proxy must not accidentally choose the first `scope` or event `type`; fixtures
  cover recognized duplicate keys. Producer output contains no duplicates.
- There is one capability header, `OpenCode-Event-Scope: 1`. There is no separate
  policy header. The existing negotiated `session-streaming` profile retains its
  exact five-type meaning; conformance pins the set below.
- The normative machine-readable fixture lives in native Protocol at
  `packages/protocol/fixtures/event-scope-v1.json`. Rust vendors an identical copy
  at `crates/protocol/fixtures/event-scope-v1.json`; the real cross-repository
  compatibility check asserts byte equality. The fixture includes the capability
  header/version, focused type list, parsed examples, and raw JSON `wireCases`.
  The raw cases validate the routing-metadata boundary using native JSON.parse /
  EventScope and Rust decode_event; they do not duplicate native full business-
  payload validation. Accepted cases carry expected routes; rejected cases cover
  malformed scope. Optional raw SSE samples make newline normalization explicit.
  Both implementations are checked against this same fixture.
