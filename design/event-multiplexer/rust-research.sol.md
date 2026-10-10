# Rust event proxy: independent architecture research

**Proposal, not implementation or accepted protocol.** Researched 2026-10-10.
The [bus-smart starter](../../../opencode-bus-smart/design/event-multiplexer/draft0.gpt6a.md)
is background, not a constraint: this request explicitly wants a **single-origin
gateway**, including ordinary HTTP and WebSockets. Exact OpenCode endpoint,
selection, acknowledgment, and authentication contracts belong to the parallel
wire-contract investigation; types below are architectural interfaces, not new
wire formats.

## Recommendation

Use Tokio, Hyper 1/hyper-util, immutable `Bytes`, and **one routing actor per
configured upstream**. A source task reads one public SSE feed; the actor owns
subscription identities, membership indexes, epochs, and nonblocking admission.
Each downstream response owns its receiver. All non-owned paths proxy to the fixed
upstream using streaming HTTP bodies; HTTP/1.1 upgrades become opaque byte tunnels.

Do not introduce a database, replay log, transcript reconciliation, DashMap,
general broker framework, or per-client upstream feeds. Moving fanout reduces
execution-process work, not necessarily aggregate CPU. Begin with ordinary safe
Rust and stable-compatible APIs; measure before custom allocators, SIMD JSON,
lock-free queues, sharding, or a pinned dedicated routing thread.

## Four workspace crates and seams

| Crate | Owns | Suggested interface |
| --- | --- | --- |
| `proxy-wire` | Incremental SSE framing, minimal trusted classification, downstream framing/control codecs | `SseDecoder::push(chunk)`, `next_frame() -> Result<Option<DecodedFrame>>`; `Route::{Broad, Session(SessionKey)}`; `Frame { route, wire: Bytes }`; bounded `Interest` validation |
| `proxy-mux` | Routing actor, indexes, bounded mailboxes, activation/replacement/failure invariants | `MuxHandle::attach() -> Attachment`; `replace(id, Interest) -> Ack`; `SourceHandle::begin() -> EpochLease`; `offer(epoch, Frame) -> OfferResult`; `end(epoch, reason)` |
| `proxy-transport` | Sole source reader/reconnector, local event/control HTTP handlers, HTTP forwarding and upgrade tunnels | `run_source(config, SourceHandle, shutdown)`; `serve(listener, MuxHandle, upstream_client, shutdown)`; explicit `GatewayPolicy` and `TransportLimits` |
| `opencode-bus-proxy` | Binary: CLI/env config, dependency composition, tracing, shutdown supervision | Build limits/client/mux/listener; supervise tasks; no routing logic |

Dependency direction: binary → transport → mux → wire; transport also uses wire.
Wire depends on bytes/serde, not HTTP server or OpenCode runtime code. Mux uses
Tokio synchronization but no socket/Hyper types. Use concrete handles and enums;
an async trait/plugin layer buys nothing for one source. `Attachment` owns ID,
epoch, receiver, and lifetime guard. `Ack` may carry an internal admission sequence
for tests; do not extend the public response without contract approval. Bench and
integration tests can live with these crates, not a fifth production abstraction.

## Routing ownership and fast structures

- Actor-owned `HashMap<SubscriptionId, Subscriber>` and
  `HashMap<SessionKey, HashSet<SubscriptionId>>`; a subscriber stores its current
  deduplicated interest set, active/pending state, sender, limits, and cancellation
  handle. Keep an active-ID set for broad fanout so pending attachments are skipped.
- Session routing visits only indexed followers: expected `O(1 + followers)`;
  broad routing is `O(active clients)`. Replacement is `O(old + new interests)`.
  Delete empty index entries. Removal/overflow removes **all** memberships once.
- Start with standard randomized hashers for client-controlled strings. Faster
  hashers or interned integer session keys are benchmark options, not assumptions.
  Intern tables must remove unused entries or have explicit size bounds.
- Generate non-reused, unguessable public attachment IDs; authorize control access
  independently. A slab is optional for compact iteration, but raw reused slab
  indexes are not IDs: use generation keys if introduced. `slotmap` is available.
- No locks around routing/index updates. Socket polling and queue byte permits
  are the only cross-task synchronization. DashMap does not make a multi-index
  replacement atomic; it would add transaction/locking complexity here.
- Collect failed recipients during a traversal, then remove them afterward to
  satisfy borrowing and keep selection stable. Reuse scratch storage where useful.
  Do not allocate a task, serialize JSON, or create a tracing span per recipient.

## Linearizable replacement, with honest limits

HTTP handlers validate and bound control input before sending actor commands with
oneshot replies. The actor handles each complete replacement and each complete
event admission without `await` inside that operation. Validate epoch/identity,
remove old memberships, install the new set, update activation state, then reply.
That state transition is the **linearization point**, not HTTP receipt or response
delivery. Concurrent replacements are ordered by the actor, not handler arrival.
Preserve native request revision/conflict semantics if the contract has them.

Events admitted before replacement retain their old recipient choice, including
already queued frames. Events admitted afterward use the new choice. This is not
a cut at upstream publication time: source and ingress backlog can cross the
acknowledgment. It is not a queue flush or a snapshot synchronization guarantee.
Keeping fanout uninterrupted prevents one event seeing mixed membership versions.

Use separate **bounded** control and source-ingress queues. Explicitly service
both with bounded quotas; check failure/shutdown first, and yield between bounded
batches. An unconditional biased control branch could starve the feed; an enormous
event batch could starve PUTs. A single very broad fanout still takes `O(C)` in one
turn: cap attachments and measure worst-case control latency. Sharding only after
that becomes the measured bottleneck; sharding requires coordinated replace/epoch
barriers, not merely a concurrent map.

A handler canceled before dispatch does nothing. After dispatch, an operation may
commit even if its oneshot receiver disappeared: a timed-out PUT has an ambiguous
outcome. Complete replacement is naturally retryable while the ID stays valid;
do not claim timeout rolls it back. A failed attachment acknowledgment must clean
up the provisional attachment, not leave a hidden subscription.

## Bounded queues: count AND bytes

First implementation: a small reusable mailbox wrapper over bounded Tokio `mpsc`
plus `Arc<Semaphore>` byte permits. Offer synchronously:

1. Validate `wire.len()` against the configured frame maximum and checked integer
   limits (`acquire_many` takes `u32`; semaphore capacity also has a maximum).
2. `try_reserve` a count slot, then `try_acquire_many_owned(len)` byte permits.
3. Enqueue `{ wire: Bytes, byte_guard }` through the reserved slot. Any failure
   drops both reservations. Never await subscriber capacity from routing.
4. Receiver/drop releases permits through RAII. For an ordinary Hyper `Bytes`
   body, release the queue byte guard when yielding that frame: the bound is on
   **queued** bytes, not bytes already owned by Hyper/kernel buffers.

Use this for source ingress too. Ingress overflow ends the whole source epoch;
subscriber overflow cancels/removes only that subscriber. Never evict one event
and keep a stream apparently healthy. An event larger than the source frame cap
fails the source; a valid event that exceeds a subscriber's budget fails that
subscriber. Failed streams discard queued work instead of draining it as clean EOF.
Closing/dropping only an mpsc sender is insufficient: its receiver normally drains
remaining messages. Cancellation must be out-of-band and take precedence.

Charge the full logical frame length **per recipient**, despite shared physical
storage: sharing must not give a slow recipient unlimited retention. Limit global
queued logical bytes as well, or conservatively cap client count × per-client
budget. A shared global permit guard per admission can implement an explicit
global budget; allocation failure needs a documented overload policy (fail the
new recipient, not random eviction). Do not advertise these counters as exact RSS.

Also bound attachment count, pending-handshake lifetime, memberships per client
and total, ID lengths, control body bytes, control queue length and concurrent
control handlers, parser buffer, event/frame size, ingress count/bytes, HTTP
connections, upgrades, header sizes, and outbound transport windows/buffers.
Bounded queues plus unbounded callers waiting to enqueue still permit unbounded
memory; handlers need admission limits or prompt overload responses.

Account separately for transport-owned frames, HTTP/2 windows if enabled, TLS
buffers, and socket buffers. Body polling is not proof the peer consumed bytes.
Keep SSE polling incremental, avoid extra buffering middleware, and configure
connection/write-progress deadlines so a hung peer cannot retain resources forever.
Start with configurable values, not inherited “4096 is safe” folklore; determine
defaults from actual frame sizes and the load benchmark.

## Failure epochs and cancellation

One reconnect owner uses capped jittered backoff. Begin an epoch only after the
source status/content type and required readiness contract pass. `begin` is
acknowledged by the actor before offering that epoch's data. Attach/activate only
against a current ready epoch; otherwise fail promptly with the native retryable
error. Epochs are volatile generations, not durable cursors or event IDs.

Every ingress item carries its epoch. EOF, parse failure, oversized frame, ingress
overflow, lost connection, or a verified liveness timeout cancels that epoch token
immediately, outside the full data queue. The actor notices cancellation before
the next operation, retires identities/indexes, rejects old queued items, and
acknowledges retirement **before** a reconnect can begin the next epoch. This
barrier prevents old `end`/data/control messages affecting a new generation.

Use a root `CancellationToken`, an epoch child, and subscription children; child
overflow must not cancel its parent. A canceled stream wakes even when no new
data arrives. Its body checks cancellation before returning the next frame and
drops its receiver; avoid a plain receiver-to-stream adapter that only drains.
Bytes already returned to the HTTP stack or peer cannot be retracted, and cancel
propagation is not an atomic instant across socket tasks. The guarantee is no
new healthy admission/new-epoch mixing, not physical network-byte rollback.

Body/drop guards notify actor cleanup without blocking in `Drop`. Use a per-client
cancel/liveness flag plus best-effort bounded cleanup command and an actor periodic
reap fallback: a full command queue must not leak a membership forever. Supervise
actor/source failures; an actor panic must stop the daemon's event service rather
than leave local streams alive. Join tasks during shutdown with a bounded grace,
then abort stragglers. Ordinary proxied HTTP/WS connections need not die merely
because the event-source epoch fails; they do die on daemon shutdown.

Do not impose a short generic whole-response timeout on SSE or PTY. Separate
connect/header/upgrade deadlines from a protocol-informed idle/progress timeout;
absence of domain events is not necessarily upstream failure.

## Reuse raw SSE bytes, parse only what selects

Retain one immutable downstream-ready frame and clone `Bytes` handles to recipients.
Parse routing metadata once with a small serde visitor/struct and skip unused
payload fields; do not deserialize into a full `Value` tree and reserialize per
client. Authoritative classification must match the wire investigation: arbitrary
payload `sessionID` fields are not authority, nor are unknown public events
automatically session-selective.

Raw reuse is conditional: if the source SSE body is already the exact downstream
frame, forward those bytes. If a routed source envelope differs from the client
event representation, extract its raw JSON subtree (`RawValue` where suitable)
and frame once, or encode once per input event. Do not forward upstream connection
markers, private metadata, or credentials. Generate local handshake/heartbeat
frames independently. `Bytes` cloning is cheap user-space sharing, **not** zero-copy
through TLS/kernel, and does not remove JSON validation/scanning cost.

The decoder must handle delimiters split across arbitrary HTTP chunks, multiple
frames per chunk, CRLF/LF/CR, leading BOM, multiline `data:`, and incomplete EOF.
Track a scan cursor: rescanning the entire growing buffer after every tiny chunk
creates quadratic behavior. Copy/coalesce at most once when necessary. A tiny
`Bytes` slice can pin a huge backing allocation, so compact oversized backing
buffers (one copy amortized across all recipients) rather than claiming slice
length alone bounds retained memory. Bound comment/control blocks as well as data.

Stateful SSE `id` and `retry` fields can affect later frames even without data.
Filtered raw forwarding must either prove such fields are absent/irrelevant in
the actual source contract or maintain deliberate downstream semantics. Likewise
strip stream-start BOM before reusing frames after a locally generated handshake.
Do not use `split("\n\n")` or silently normalize away meaningful fields. An
eventsource client that only exposes decoded strings is convenient but forfeits
raw framing reuse; choose it only if measured simplicity outweighs that cost.

## Same-origin reverse proxy and library choice

**Recommend Hyper 1 + hyper-util pooled client + http-body-util**, with Tokio
I/O adapters. A small Hyper service can exact-match local event/control endpoints
and forward everything else; Axum 0.8 is a reasonable control-router alternative,
but must not impose body collection/limits on opaque proxy paths. Start with
HTTP/1.1 upstream and upgrades; normal HTTP/2 can be added/tested separately.
Use hyper-rustls for HTTPS upstreams. One shared pooled client is cheap to clone.
The `hyper_util::client::legacy::Client` name does **not** mean Hyper 0.14.

Reqwest is a fine standalone source-client convenience, but using Hyper for both
source and proxy avoids two client stacks; reqwest's high-level body/redirect/
decompression behavior is not the right primitive for a transparent upgrade and
trailer-preserving gateway. Pingora has valuable proxy infrastructure but is a
larger architecture/dependency decision than one fixed upstream needs. A generic
reverse-proxy crate is not a correctness shortcut: audit its body streaming,
cancellation, headers and upgrades against the same tests before adopting it.

Required behavior:

- Forward exact method and raw path/query (including percent encoding); target is
  a configured origin, never user-supplied Host/absolute URI. Reject unsupported
  CONNECT/open-proxy requests. Exact local ownership avoids swallowing unrelated
  upstream routes; use a private namespace for mux readiness, not an accidental
  replacement of upstream health. Event-source readiness and HTTP reachability
  are different observations.
- Stream `Incoming` request/response bodies without `collect`, JSON decoding,
  automatic decompression, redirect-following, or body replay. Preserve trailers,
  status, repeated headers (`Set-Cookie`), and opaque payloads. Hyper owns transfer
  framing; strip/reconcile hop-by-hop headers in both directions, including
  dynamically named `Connection` tokens. Preserve legal trailer behavior.
- Set an explicit Host policy. Usually connect and send Host for the upstream
  authority, with trusted forwarded external origin metadata where supported;
  test upstream virtual-host/origin validation. Preserve browser `Origin` unless
  an explicit native policy requires a configured mapping. Do not globally disable
  origin/CSRF checks to make WS work. Sanitize inbound forwarded headers; do not
  reflect untrusted Host into redirects or cookie rewriting.
- Authenticate owned event/control routes as strongly as upstream routes. One
  shared all-event feed presupposes a single trusted upstream authorization scope;
  interests are not ACLs. Multi-principal isolation needs separate source domains
  or a real authorization model. Keep upstream export credentials separate from
  caller credentials and confined to the configured origin; no uncontrolled
  redirect forwarding. Preserve native ordinary-request auth policy explicitly.
- Client disconnect/drop stops request upload and response consumption and drops
  pending upstream work. Do not detach request futures or add an unbounded body
  relay. Test cancellation, including partial uploads; connection pooling is not
  license to keep backend work alive indefinitely. Gateway cancellation cannot
  promise the backend undoes a command already accepted.
- Before response headers, connection/timeout errors map to gateway errors.
  After headers, body failure aborts/errors the body, not a fabricated JSON suffix.
  No automatic retry of mutation requests.

For WebSockets, use **opaque HTTP/1.1 upgrade tunneling**, not an Axum
`WebSocketUpgrade` plus a second tokio-tungstenite handshake. Capture downstream
`hyper::upgrade::on(request)`, forward the original handshake to an HTTP/1-capable
upstream client, validate the expected upgrade response, capture upstream
`on(response)`, return the upstream 101 headers, then await both upgrades and run
`tokio::io::copy_bidirectional` on `TokioIo` adapters. Ensure the server connection
driver enables upgrades. Hyper's `Upgraded` I/O preserves its buffered read bytes;
do not replace it with a bare socket and lose bytes read after the headers.

The upgrade path must intentionally preserve `Connection`/`Upgrade` and
`Sec-WebSocket-*` negotiation instead of applying ordinary hop-by-hop stripping
blindly. Non-101 errors remain normal HTTP responses. Tunneling retains masked
frames, binary data, fragmentation, ping/pong, close, negotiated subprotocols and
extensions without WS decoding. Tokio's default tunnel buffering is two 8 KiB
buffers and supports half-close; cap concurrent tunnels and enforce shutdown and
upgrade deadlines. HTTP/2 extended CONNECT is **not** HTTP/1 upgrade: explicitly
defer it or implement/test it separately. A WS codec is appropriate only if the
proxy must inspect messages, which this event proxy does not need.

## Practical verification and performance evidence

Use deterministic unit/property tests before interpreting speed: replacement
before/after admission, concurrent replace outcomes, first activation, source
loss with a full queue, old-epoch messages/control, oversized frames, receiver
drop, count-only and byte-only overflow, shared-frame retention, no-recipient
events, and cleanup under a saturated control queue. Track membership and permit
totals after every generated operation. Split SSE fixtures at **every byte
boundary**, include multiline/CR/BOM/stateful fields, and test bounded malformed
input. No detailed OpenCode wire re-investigation is required in these tests.

Two benchmark layers:

1. **Criterion microbench:** in-memory decode+classify and actor admission/replace;
   isolated maps/offer loop to distinguish mailbox scheduling from routing cost.
   Sweep 1/32/256/1024 clients, 0/1/all followers, disjoint/overlapping interests,
   representative 200 B–4 KiB deltas and 64 KiB–1 MiB completions (within caps).
   Record events/s, deliveries/s, allocation counts/bytes where available, and
   control p50/p95/p99 under sustained ingress. Drainers must keep up or explicitly
   be the slow-consumer case; measure overflow rate alongside throughput.
2. **End-to-end harness:** fake authenticated SSE origin + HTTP upload/download/
   trailer endpoints + a WS echo/PTY-like endpoint; real sockets, bounded slow and
   paused clients, controlled source reset, attachment churn. Compare direct origin,
   proxy forwarding without selection, and selected proxy; then replay the same
   workload against the real embedded/server topology when available.

Measure source→client p50/p95/p99 latency, PUT acknowledgment latency, ordinary
HTTP latency during fanout, WS RTT, origin/mux/client CPU separately and summed,
RSS after warmup and teardown, ingress/downstream bytes, disconnects/overflows,
and recovery latency. Stamp monotonic times in the synthetic harness, not public
protocol fields. Demonstrate that one slow client cannot stall fast clients or
HTTP/WS, and that count/byte limits flatten queue growth. Include no-followers:
the all-event uplink still costs origin serialization/network even when fanout
skips delivery. Test bufferbloat and origin loss during full queues, not just
success-path million-small-event throughput.

Use release builds; record hardware, versions, runtime worker count, limits,
payload distribution and client-reader behavior. Repeat trials, report variation,
and validate counts so silent drops are not “performance.” Use `perf stat/record`
and flamegraphs if available; profiling instruments are not assumed installed.
Compare tracing off/default/verbose before accepting telemetry overhead.

Use `tracing`/`tracing-subscriber` lifecycle and control spans, rate-limited failure
logs, and aggregated gauges/counters/histograms for queue pressure, memberships,
source epochs, bytes and latency. No per-token/per-recipient spans by default;
no payload/credential logging or unbounded metric labels. Avoid holding
`Span::enter` guards across awaits; instrument futures. Tracing alone is not a
metrics exporter: choose an explicit inspection/export path in binary composition.

## Availability and sources

Checked local environment, not a compile test: no Cargo workspace exists in this
repo yet. Installed `rustc 1.101.0-nightly` and `cargo 1.101.0-nightly`, active
`nightly-2026-09-30` via environment override. Architecture needs no nightly
features. Select/pin a supported stable toolchain at implementation time.

Cached source includes Tokio 1.53.2, tokio-util 0.7.19, Hyper 1.12.0, hyper-util
0.1.21, http-body-util 0.1.5, bytes 1.12.1, Axum 0.8.9, hyper-rustls 0.27.10,
serde_json 1.0.151, tracing 0.1.44, tracing-subscriber 0.3.23, Criterion 0.8.2,
slotmap 1.1.1, DashMap 6.2.1, and tokio-tungstenite 0.30.0. hyper-tungstenite is
not cached and is unnecessary for opaque tunnels. Cached roots do not guarantee
every transitive dependency/feature resolves offline; lock and actually compile
the selected versions before calling availability verified.

Primary API/spec sources consulted (2026-10-10):

- [Tokio bounded mpsc](https://docs.rs/tokio/1.53.2/tokio/sync/mpsc/index.html):
  count bound, remaining-message drain on sender closure, receiver-drop behavior.
- [Tokio semaphore](https://docs.rs/tokio/1.53.2/tokio/sync/struct.Semaphore.html):
  try-acquire owned permits, `u32` multi-permit counts and `MAX_PERMITS`.
- [Bytes](https://docs.rs/bytes/1.12.1/bytes/struct.Bytes.html): cheap shared clones
  and O(1) slices, which retain backing ownership.
- [Hyper upgrades](https://docs.rs/hyper/1.12.0/hyper/upgrade/index.html): client
  and server upgrades require correct handshake/status/headers from the caller.
- [hyper-util pooled client](https://docs.rs/hyper-util/0.1.21/hyper_util/client/legacy/struct.Client.html):
  clone shares the pool; generic streaming request bodies and Incoming responses.
- [Tokio bidirectional copy](https://docs.rs/tokio/1.53.2/tokio/io/fn.copy_bidirectional.html):
  concurrent bounded copying, half-close and error semantics.
- [CancellationToken](https://docs.rs/tokio-util/0.7.20/tokio_util/sync/struct.CancellationToken.html):
  cancellation hierarchy, wakeups and non-atomic propagation; fetched version is
  newer than cached 0.7.19, so verify APIs against the implementation lockfile.
- [WHATWG SSE parsing](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream):
  line terminators, multiline data, BOM, incomplete EOF and persistent ID state.

**Landing choice:** four crates, one routing owner, shared downstream-ready bytes,
count+byte bounded mailboxes, explicit failure epochs, and a fixed-origin Hyper
gateway with opaque WS upgrades. Optimize only the bottlenecks shown by the two
benchmark layers; compatibility and failure visibility are part of performance.
