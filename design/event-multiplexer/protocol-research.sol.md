---
type: Research
title: Bus-smart wire compatibility and authoritative proxy source
status: draft
generated: { by: "openai/gpt-6.1-sol", at: 2026-10-10 }
sources:
  - resource: file:///home/rektide/src/opencode-bus-smart/packages/protocol/src/groups/event.ts
    title: Public event and controlled-interest contract
  - resource: file:///home/rektide/src/opencode-bus-smart/packages/server/src/controlled-event-feed.ts
    title: Actual admission and selection behavior
  - resource: file:///home/rektide/src/opencode-bus-smart/packages/core/src/bus.ts
    title: Authoritative publication-time routing
  - resource: file:///home/rektide/src/opencode-bus-smart/design/event-multiplexer/init0.gpt6a.md
    title: Nonprescriptive starter brief
---

# Findings and scope

The standalone Rust proxy can keep **unchanged V2 and bus-smart clients on one
base URL**: locally own the two event GETs and controlled-interest PUT, forward
all other HTTP routes and WebSocket upgrades to the execution origin. Keep the
origin's existing embedded event routes intact for direct bus-smart clients.
One additional origin source route is needed to export authoritative routing
without putting proxy-only envelopes onto existing client routes.

This is source inspection, not implementation or a live-service test. Sources
are the local bus-smart working tree observed on 2026-10-10; links below are
navigation to mutable source, not immutable revision bindings. Applicable parent
instructions, bus-smart root `AGENTS.md`, and TUI `AGENTS.md` were read. The
[starter](file:///home/rektide/src/opencode-bus-smart/design/event-multiplexer/init0.gpt6a.md)
and [Effect draft](file:///home/rektide/src/opencode-bus-smart/design/event-multiplexer/draft0.gpt6a.md)
are background proposals, not a requirement to use Effect or share a TS engine.

## 1. Exact client-facing HTTP contract

[Protocol](file:///home/rektide/src/opencode-bus-smart/packages/protocol/src/groups/event.ts#L12-L135),
[generated Promise client](file:///home/rektide/src/opencode-bus-smart/packages/client/src/promise/generated/client.ts#L1707-L1736),
[HTTP handler](file:///home/rektide/src/opencode-bus-smart/packages/server/src/handlers/event.ts#L11-L89).

| Request | Success | Opening data / body |
|---|---|---|
| `GET /api/event` | `200`, `text/event-stream` | `server.connected`, then every public event |
| `GET /api/experimental/event` | `200`, `text/event-stream` | `event-feed.ready`; no domain events until successful PUT |
| `PUT /api/experimental/event/subscriptions/{subscriptionID}/interests` | `204`, empty | Complete replacement, not patch/add/remove |

Neither GET declares event-specific query parameters or required custom headers.
Generated clients use fetch, **not browser EventSource**; they do not automatically
set `Accept: text/event-stream`. Authentication/global request headers still apply.
PUT defaults to `Content-Type: application/json`; the raw handler actually reads
and decodes the stream as JSON rather than gating on this header.

Each data frame is exactly `data: ${JSON.stringify(item)}\n\n`. No SSE `event:`,
`id:` or `retry:` field is emitted. JSON event `id` is not an SSE resume cursor.
Response headers are:

```http
Content-Type: text/event-stream
Cache-Control: no-cache, no-transform
X-Accel-Buffering: no
X-Content-Type-Options: nosniff
```

Both GETs merge `: heartbeat\n\n` every 15 seconds, including while controlled
subscriptions are pending. Comments are not application events. The proxy must
flush frames promptly, avoid stream buffering, and not let compression conceal
activity. There is no replay/Last-Event-ID contract.

### Ready → PUT → connected

```text
GET /api/experimental/event
data: {"type":"event-feed.ready","data":{"subscriptionID":"evsub_<32 lower hex>","profiles":["session-streaming"]}}

PUT /api/experimental/event/subscriptions/evsub_<32 lower hex>/interests
{"locations":[],"sessions":["ses_example"],"profile":"session-streaming"}
→ 204, empty

on that GET:
data: {"id":"evt_<generated>","type":"server.connected","data":{}}

then matching live events
```

[Registration/activation](file:///home/rektide/src/opencode-bus-smart/packages/server/src/controlled-event-feed.ts#L218-L349)
queues ready immediately but starts inactive. First accepted replacement queues
connected **before** activating/indexing, so connected precedes domain events.
Subsequent replacements do not emit another connected. There is no guarantee
that the network observes PUT's response before the queued connected frame.
Interest installation is acknowledged at admission, not at downstream receipt.
Already-admitted frames are not re-filtered after a replacement.

`server.connected` intentionally has no `created`, `location`, or `durable` field;
do not manufacture a native event envelope requirement for it. Ready has neither
`id` nor `created`. Ready's `profiles` is optional in the schema but the current
server always advertises `["session-streaming"]`.

### Interest shape and validation

```typescript
{
  locations: Array<{ directory: AbsolutePath; workspaceID?: WorkspaceID }>,
  sessions: SessionID[],
  profile?: "location" | "session-streaming"
}
```

Both arrays are required. Current normalization treats missing profile as
`location`, then **rejects it**. Only `session-streaming` succeeds. Locations
are decoded and counted but **do not filter anything** in this profile. An empty
Session set still receives every broad public event. Interests are delivery
policy, not permission grants, and no Session existence lookup is performed.

Bounds: raw request ≤262144 bytes; reserialized decoded interest ≤262144 UTF-8
bytes; ≤1024 Location entries; ≤4096 Session entries **before deduplication**;
Sessions become a set. Excess duplicate entries therefore still fail. Subscription
ID schema only requires prefix `evsub_`; generation uses 16 random bytes / 32 hex.
[Session ID schema](file:///home/rektide/src/opencode-bus-smart/packages/schema/src/session-id.ts)
requires prefix `ses`, not a fixed length or even `ses_`. Do not tighten this in Rust.
[Location schema](file:///home/rektide/src/opencode-bus-smart/packages/schema/src/location.ts#L8-L16)
preserves workspace identity on interests, although `PublicRef` omits it.

### Error wire bodies and lifecycle

[Errors](file:///home/rektide/src/opencode-bus-smart/packages/protocol/src/errors.ts#L11-L45)
and [subscription error](file:///home/rektide/src/opencode-bus-smart/packages/protocol/src/groups/event.ts#L18-L25):

```json
{"_tag":"InvalidRequestError","message":"Invalid event interest request body","field":"interest"}
{"_tag":"InvalidRequestError","message":"This server supports only the session-streaming event profile","field":"profile"}
{"_tag":"EventSubscriptionNotFoundError","subscriptionID":"evsub_example","message":"Controlled event subscription evsub_example was not found"}
{"_tag":"UnauthorizedError","message":"Authentication required"}
```

Statuses respectively: 400, 400, 404, 401. InvalidRequestError also permits optional
`kind`; messages for bound violations name the limit. Unknown profile enum values
fail decoding as `field:"interest"`; supported enum `location` fails normalization
as `field:"profile"`. Body/normalization errors precede live-ID lookup. Malformed
path parameters can fail framework decoding separately.

Canceling/closing GET unregisters its ID; future valid PUTs return 404. A slow
subscriber's dropping queue (4096 frames by default, **not a byte bound**) is failed
and removed on the first rejected offer; no silent frame drop with continued
health. Encoding failure fails all subscribers of that feed. Stream errors after
headers are stream failure, **not a defined JSON SSE error event** or a changed HTTP
status. HTTP tests cover [handshake, errors, cancellation, and legacy opening](file:///home/rektide/src/opencode-bus-smart/packages/server/test/controlled-event-feed-http.test.ts).

## 2. Selection and native event shapes

Exactly these five types are streaming; **everything else public is broad**:

| Type | Native `data` fields (all required) |
|---|---|
| `session.text.delta` | `sessionID`, `assistantMessageID`, `ordinal` (nonnegative int), `delta` (string) |
| `session.reasoning.delta` | same |
| `session.tool.input.delta` | `sessionID`, `assistantMessageID`, `id` (tool string), `delta` (string) |
| `session.tool.progress` | `sessionID`, `assistantMessageID`, `id` (tool string), `metadata` (string-keyed JSON values) |
| `session.compaction.delta` | `sessionID`, `text` (string), **not `delta`** |

These are ephemeral live-only events: native envelope is `{id, created, type,
data, metadata?, location?}`, without durable. See
[native definitions](file:///home/rektide/src/opencode-bus-smart/packages/schema/src/session-event.ts#L391-L602)
and [envelope](file:///home/rektide/src/opencode-bus-smart/packages/schema/src/event.ts#L128-L149).
Durable broad events additionally carry `{durable:{aggregateID,seq,version}}`;
preserve their fields, IDs and sequences unchanged. Started/ended/tool-success,
renamed/moved/deleted events and `rpc.*` remain broad even if Session-related.

Public admission is [isOpenCodeEvent](file:///home/rektide/src/opencode-bus-smart/packages/protocol/src/groups/event.ts#L149-L150):
`server.connected`, ServerDefinitions manifest types, or `rpc.` prefix. A source
export must apply this allowlist at origin; an internal Bus observer is not itself
a public-data boundary. RPC schema is `{id,created,type:"rpc.*",location:PublicRef,
data:Record<string,unknown>,metadata?}`. Existing server uses JSON.stringify of the
actual event, not schema re-encoding; native definitions use Location.Ref, so do
not assume all wire locations have already been stripped to PublicRef.

### Session identity is source-owned

[Bus.RoutedEvent](file:///home/rektide/src/opencode-bus-smart/packages/core/src/bus.ts#L21-L34)
is `{event, audience}`. Audience is `{type:"global",sessionID?}` or
`{type:"locations",refs:Location.Ref[],sessionID?}`. Bus determines Session ID
**only for types in `SessionEvent.All.cases`**, then reads their native
`event.data.sessionID` ([routing](file:///home/rektide/src/opencode-bus-smart/packages/core/src/bus.ts#L223-L291)).
Arbitrary plugin payload `sessionID`, metadata, durable aggregate ID, directory,
or current Session placement is not routing authority.

Controlled admission checks the five-type classifier, then indexes **only
`input.audience.sessionID`**. Streaming with absent/foreign audience ID is skipped,
even if payload says a followed Session and audience is global. Broad events go
to every active subscriber regardless of audience locations or Session ID.
[Tests explicitly exercise this boundary](file:///home/rektide/src/opencode-bus-smart/packages/server/test/controlled-event-feed.test.ts#L185-L250).

Bus Location routing is a publication snapshot, separate from public payload:
Session creation updates owner; fork resolves parent's owner; move reaches old
and new locations then switches ownership; deletion clears owner. Routing lookup
can consult SessionTable; updates apply only after projection commit. None of
this needs to move into the proxy for session-streaming selection.
[notify](file:///home/rektide/src/opencode-bus-smart/packages/core/src/bus.ts#L537-L549)
awaits routed observers before legacy listeners. Queue offers do not wait for
remote reads, but encoding/fanout in those observers still costs publisher work.

## 3. Actual client selection and fallback

The [bus-smart TUI](file:///home/rektide/src/opencode-bus-smart/packages/tui/src/context/client.tsx#L19-L56)
uses controlled feed only when `config.experimental.session_streaming === true`.
Otherwise it calls ordinary `/api/event`; configuration changes reconnect the
event stream, not the execution service. The Promise wrapper's default
[shared subscription](file:///home/rektide/src/opencode-bus-smart/packages/client/src/promise/client.ts)
also remains legacy. Merely having a bus-smart client binary does not mean it
uses controlled delivery.

[Controlled adapter](file:///home/rektide/src/opencode-bus-smart/packages/client/src/solid/controlled-event-feed.ts#L107-L247):

- GET must first yield ready. Ending before ready, another first type, or duplicate
  ready later is an error, not fallback.
- GET 404 recognized as `ClientError("UnexpectedStatus")` falls back to legacy.
  Ready with absent `profiles` or without `session-streaming` also falls back;
  it cancels controlled GET, then opens legacy GET on the **same API client**.
- With profile support it sends `locations:[]`, complete desired `sessions`,
  `profile:"session-streaming"`. It waits for PUT success before yielding events,
  consumes ready itself, and yields connected to the connection layer.
- PUT 404/400/401 do **not** invoke the GET fallback path. A failed PUT aborts both
  stream/controllers and forgets installed interests. Transport failure is
  indeterminate and retried with a new GET/ID; deterministic rejected target is
  remembered. Retry pauses only for InvalidRequestError with `field:"interest"`
  on the same rejected target; desired changes wake it.
- Updates serialize/coalesce; removals retain a 3000ms grace by default. Flush is
  an installation barrier, not just a diagnostic mode flag. Legacy flush resolves
  without filtered installation. [Transcript adoption](file:///home/rektide/src/opencode-bus-smart/packages/tui/src/context/event-interest-binding.ts#L25-L52)
  invalidates/resyncs a Session after controlled installation; proxy must not own
  transcript convergence or pretend delivery gaps never happened.

[Connection layer](file:///home/rektide/src/opencode-bus-smart/packages/client/src/solid/connection.ts#L35-L214)
requires first yielded `server.connected`, has a 2000ms connect timeout covering
the controlled handshake, and 45000ms idle timeout reset by any bytes, including
comments. Foreground idle threshold is 20000ms. EOF counts as disconnection.
Thus a healthy-looking downstream heartbeat must not mask a broken uplink.

[Generated fetch/SSE transport](file:///home/rektide/src/opencode-bus-smart/packages/client/src/promise/generated/client.ts#L276-L423)
joins paths relative to a base URL prefix, merges global/endpoint/request headers,
requires success status and text/event-stream, parses multiline `data:` JSON,
ignores other SSE fields, and does not retry itself. Its buffer cap is named
16MiB but implemented as JS string length. Declared HTTP errors decode tagged
JSON; undeclared statuses become UnexpectedStatus. No explicit fetch credentials
option is installed: browser default same-origin cookie behavior applies unless
custom fetch changes it. Preserve route prefixes if supporting a prefixed base URL.

## 4. Same-origin gateway, auth, and WebSockets

The proxy cannot authenticate just its uplink and expose local event routes to
unauthenticated clients. All local and forwarded routes must retain origin's
credential boundary. This server is not a multi-tenant permission filter;
interest IDs are live process-owned handles, not separately bound principals.

[Auth middleware](file:///home/rektide/src/opencode-bus-smart/packages/server/src/middleware/authorization.ts)
accepts `auth_token` query first (base64 `username:password`), then Basic auth,
or signed session cookie. Username defaults to `opencode`; a signed pairing
session can substitute for password. Unauthorized JSON is 401; Basic challenge
`Basic realm="Secure Area"` is included only when `Sec-Fetch-Mode` is absent or
`navigate` to avoid trapping browser fetch in a native password prompt.
Managed process requires a nonempty configured password; embedded auth can be
disabled ([process](file:///home/rektide/src/opencode-bus-smart/packages/server/src/process.ts#L47-L77),
[auth](file:///home/rektide/src/opencode-bus-smart/packages/server/src/auth.ts)).

Cookie acceptance compares Origin URL host (including port) with Host, if Origin
exists. Cookie name is `opencode_session_<port>` or `opencode_session` without a
port. Tokens are HMAC signed using origin password; rotation revokes them.
[Pairing navigation](file:///home/rektide/src/opencode-bus-smart/packages/server/src/handlers/server.ts#L30-L52)
sets Path=/, HttpOnly, SameSite=Lax, 30-day Max-Age and redirects `/`; ordinary
API pairing returns `{token}`. **Rewriting Host to the internal origin port while
forwarding external Origin/Cookie breaks cookie lookup and same-origin checks.**
Do not assume forwarded-header middleware repairs it; inspected checks read Host
directly. A coherent option is preserve external Host and Origin end-to-end, with
origin serving behind the proxy and proxy validating the same credential/session
scheme for local routes. Alternatively explicitly map cookie/origin context after
checking external origin; do not blindly overwrite hostile Origins or inject the
private upstream credential into every unvalidated request. Preserve Set-Cookie,
redirects, and authentication/error responses.

[CORS/origin policy](file:///home/rektide/src/opencode-bus-smart/packages/server/src/cors.ts)
allows absent Origin, HTTP localhost/127.0.0.1 with port, oc renderer, named Tauri
origins, HTTPS opencode.ai subdomains and exact configured CORS entries. PTY checks
also allow same Host. CORS headers alone are not authentication, and origin GETs
use CORS middleware rather than the PTY's explicit rejection policy. Same-origin
proxy deployment avoids changing client endpoint topology, but unrelated direct
browser origins still need existing explicit CORS handling/preflight.

Gateway forwarding must include **bidirectional WebSocket upgrades**, not merely
ordinary HTTP. Existing routes:

- `POST /api/pty/{ptyID}/connect-token`, header `x-opencode-ticket: 1`, location
  query; returns `{location,data:{ticket,expires_in}}` (`expires_in` positive int;
  [ticket schema](file:///home/rektide/src/opencode-bus-smart/packages/schema/src/pty-ticket.ts)).
- `GET /api/pty/{ptyID}/connect` upgrade, query `location[directory]`, `cursor`,
  `ticket` (and preserve other incoming location/query fields).
- Persistent equivalents at `/api/experimental/persistent-pty/{ptyID}/connect-token`
  and `/api/experimental/persistent-pty/{ptyID}/connect`.

[PTY protocol](file:///home/rektide/src/opencode-bus-smart/packages/protocol/src/groups/pty.ts#L9-L18)
and [handler](file:///home/rektide/src/opencode-bus-smart/packages/server/src/handlers/pty.ts#L119-L178)
require allowed Origin and custom header to mint a ticket. Browser WebSockets
cannot set arbitrary Authorization headers, so nonempty ticket URLs bypass general
auth but handlers consume/validate a single-use ticket scoped to PTY/location.
Absent ticket uses normal credentials/cookie; invalid ticket/origin returns 403,
missing PTY 404. Persistent route has the same policy. Preserve upgrade status,
frames/binary data and close semantics; do not reinterpret terminal traffic as
event SSE. Other streaming endpoints (Session logs, generation, etc.) remain
origin-owned and must be forwarded incrementally too.

## 5. Minimal additive origin source recommendation

**Proposed, not existing:** one authenticated GET such as
`/api/experimental/event/source`, independent of client subscription IDs and PUT.
Reuse public allowlist and authoritative `Bus.observeRouted`; export all public
events without client-interest filtering. Suggested SSE JSON payloads:

```json
{"type":"event-source.ready","data":{"version":1,"epoch":"<new-per-uplink>"}}
{"type":"event-source.event","data":{"event":{"id":"evt_example","created":1,"type":"session.compaction.delta","data":{"sessionID":"ses_example","text":"x"}},"sessionID":"ses_example"}}
```

Here outer `data.sessionID` is copied **only** from Bus audience; it is optional
for non-Session events and must never be reconstructed from the nested payload.
This is sufficient for current session-streaming semantics; Location audience refs
are unnecessary unless a separately agreed Location policy is added. Optionally
export a source-owned `delivery:"broad"|"session-streaming"` discriminator too to
avoid re-owning the five-type classification; Session identity alone is the
minimum extension, with classifier version pinned/tested at proxy boundary.

Register observation before ready and ensure ready precedes queued events. Origin
observer performs a bounded nonblocking queue offer, not remote socket waits;
serialize/drain outside the awaited publication observer. Overflow/encoding failure
must visibly terminate source. Epoch identifies a volatile connection lifetime,
not replay or global ordering. Preserve nested public event unchanged.

Rust owns one source connection/retry loop and local fanout/index/queues. Generate
local ready/connected per downstream client; do not forward source markers. If
source fails/EOFs/overflows, close existing downstream streams and invalidate IDs;
clients reconnect and perform their own read repair. Do not keep sending healthy
heartbeats during an unseen event gap. Do not serve a connected marker before a
usable source exists. New request failure should be 503, **not 404**, since 404
advertises unsupported controlled feed and triggers legacy fallback.

Keep direct origin `/api/event` and controlled GET/PUT operational: source export
is additive, not an exclusive migration switch. Direct clients still perform
embedded fanout, so they still cost origin CPU. Proxy clients use local routes;
GET and PUT must reach the same owner (no round-robin split).

Why not reuse existing streams as the authoritative uplink? Legacy GET carries
every public event but omits audience metadata; a five-native-type decoder can
prototype routing but explicitly re-owns Bus classification. Controlled GET cannot
request all Sessions via wildcard, and its 4096-interest bound/activation lifecycle
cannot represent unknown future Sessions without extra tracking. Neither is the
requested authoritative one-uplink contract. No database/replay/export ownership,
Session discovery, second execution process, or direct-client change is needed.
