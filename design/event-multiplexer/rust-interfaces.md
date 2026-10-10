# Build seams

These concrete interfaces coordinate independent crate implementation. The source
wire shape is [source-contract.md](source-contract.md). Types below are public;
internal decomposition belongs to each crate owner. Corrections should update
callers and this seam together.

## Protocol

```rust
pub enum Route { Broadcast, Session { session_id: Option<String> } }
pub enum SourceItem {
    Ready { version: u32, epoch: String },
    Event { route: Route, event: Box<serde_json::value::RawValue> },
}
pub struct Interest { pub sessions: Vec<String> } // validated, deduplicated
pub struct InterestError { pub message: String, pub field: &'static str }
pub fn decode_source(data: &[u8]) -> Result<SourceItem, ProtocolError>;
pub fn decode_interest(data: &[u8]) -> Result<Interest, InterestError>;
pub fn event_frame(event: &serde_json::value::RawValue) -> bytes::Bytes;
pub fn ready_frame(subscription_id: &str) -> bytes::Bytes;
pub fn connected_frame() -> bytes::Bytes;
pub struct SseDecoder; // bounded, incremental, comments ignored
impl SseDecoder {
    pub fn new(max_frame_bytes: usize) -> Self;
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Vec<u8>>, ProtocolError>;
    pub fn finish(&mut self) -> Result<(), ProtocolError>;
}
```

Export constants for endpoint paths, interest limits and default 16 MiB source
frame size. Decode interest to native bus-smart validation semantics, including
entry counts before deduplication. Source version/epoch sequencing is checked by
gateway; the decoder validates envelope structure. Frame native event JSON once,
including correct SSE multiline data treatment.

## Router

One actor owns all indexes. Cloneable `Router` uses bounded commands with async
oneshot acknowledgments; commands carry raw `Bytes`, not decoded native bodies.

```rust
pub struct Limits {
    pub queue_frames: usize,
    pub queue_bytes: usize,
    pub max_subscribers: usize,
    pub command_capacity: usize,
}
pub enum Mode { Legacy, Controlled }
pub enum RouterError { Unavailable, NotFound, Overloaded, Closed, InvalidLimits }
pub struct Router; // Clone
pub struct Subscription; // id(), recv().await, Drop cleanup
impl Router {
    pub fn spawn(limits: Limits, shutdown: CancellationToken)
      -> Result<(Self, tokio::task::JoinHandle<()>), RouterError>;
    pub async fn begin_epoch(&self, epoch: String) -> Result<(), RouterError>;
    pub async fn end_epoch(&self, epoch: String) -> Result<(), RouterError>;
    pub async fn subscribe(&self, mode: Mode) -> Result<Subscription, RouterError>;
    pub async fn replace(&self, id: String, interest: Interest) -> Result<(), RouterError>;
    pub async fn publish(&self, epoch: String, route: Route, frame: Bytes) -> Result<(), RouterError>;
    pub async fn snapshot(&self) -> Result<Snapshot, RouterError>;
}
impl Subscription {
    pub fn id(&self) -> &str;
    pub async fn recv(&mut self) -> Option<Bytes>;
}
```

`Limits: Default + Clone`; `Mode: Copy + Clone`; `Snapshot: Serialize` contains
`epoch: Option<String>`, `subscriptions: Vec<SubscriptionInfo>`, counters and
gauges. Document concrete snapshot fields for the gateway. Counters are
admissions, not socket receipts. Invalid epoch data is rejected. `end_epoch`
acknowledgment cancels receivers and discards backlog before the next epoch can
start. Byte permits release on dequeue/drop. A bounded actor command channel and
single awaited source publish keep ingress bounded; publish acknowledgment adds
one scheduling hop but avoids a second ingress backlog/epoch barrier initially.
Use prompt `try_send` overload rejection for external controls rather than an
unbounded population of waiting sends. Ensure canceled subscribe acknowledgments
drop the provisional subscription and cleanup cannot be lost to a full channel
(periodic reap of canceled subscription tokens is sufficient).

## Gateway

```rust
pub struct Config {
    pub upstream: String,
    pub username: String,
    pub password: Option<String>,
    pub cors: Vec<String>,
    pub max_source_frame_bytes: usize,
    pub source_idle_timeout: std::time::Duration,
    pub request_timeout: std::time::Duration,
    pub max_connections: usize,
    pub header_read_timeout: std::time::Duration,
}
pub async fn serve(
    listener: tokio::net::TcpListener,
    config: Config,
    router: bus_proxy_router::Router,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), GatewayError>;
```

`Config: Default`; default upstream `http://127.0.0.1:4096`, username `opencode`,
source frame limit 16 MiB, source idle 45 seconds (origin heartbeats at 15),
connect/source/upgrade setup timeout 10 seconds, inbound HTTP header deadline
10 seconds, and 1024 concurrent HTTP connections including upgrades. Ordinary
forwarded API execution/upload/response duration is not time-limited by these
setup deadlines. Daemon validates a nonempty shared password unless explicit
`--allow-unauthenticated` is chosen. The same password protects origin and local
routes and supplies source Basic credentials. Pairing-session HMAC and Basic/
query/cookie behavior must mirror the checked-out native auth; preserve external
Host and Origin when forwarding, so pairing cookies use the proxy's port. Ordinary
forwarded requests retain caller credentials, never substitute service credentials.

`serve` supervises source task and HTTP listener and joins source shutdown. It
exposes authenticated `/_bus/health` (503 unless ready), `/_bus/subscriptions`
(snapshot), `/_bus/metrics` (Prometheus text), existing event routes, and generic
streaming HTTP/opaque HTTP1 upgrade forwarding. CORS for local routes mirrors
native configured/default origins; forwarded OPTIONS continues to origin.

## Daemon

Owns `crates/daemon`, root manifest/lockfile, operating docs and integration review.
CLI binds loopback by default, reads shared credential from an environment or
file (not password-valued command arguments), exposes queue/source limits and
JSON tracing, handles SIGINT/SIGTERM, and supervises router/gateway tasks. No live
server discovery or replacement side effects.
