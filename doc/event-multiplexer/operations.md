# Operating the event gateway

## Topology

Run one proxy against one bus-smart origin whose ordinary `GET /api/event`
advertises `OpenCode-Event-Scope: 1`. The upstream URL is an HTTP(S)
origin, without credentials, query, or a route prefix. Clients use the proxy as
their complete server URL; the gateway owns event GETs and interest PUTs and
forwards other routes. Ordinary HTTP traffic can continue while the event source
reconnects.

The origin sends ordinary OpenCode events plus authoritative `scope`; the proxy
forwards those same events. A normal `server.connected` ID identifies the upstream
epoch. The earlier private source route and `event-source.*` envelopes have been
removed. An older origin lacking the capability is rejected explicitly; ordinary
client compatibility does not imply that every old origin supplies routing facts.

Client interest is exact Session identity. The five native fragment types are
selective; lifecycle events, RPC, permissions, and forms are broadcast. Sessions
moving directory do not change their interest identity. Legacy clients supply no
interest declarations and retain broadcast semantics. The existing client setting
`experimental.session_streaming: true` enables bus-smart's controlled adapter.

## Credentials and browser compatibility

Supply the origin's shared password through `OPENCODE_SERVER_PASSWORD` or
`--password-file` (`BUS_PROXY_PASSWORD_FILE`). A file takes precedence and trailing
CR/LF is removed; other whitespace is significant. The default username is
`opencode`, configurable through `--username`/`OPENCODE_SERVER_USERNAME`.

Local feed and diagnostic routes accept the native Basic/query credentials and
password-signed pairing sessions. Ordinary forwarded requests retain the caller's
credentials. Only the proxy's own source connection gets its configured service
credential. Host and Origin remain the client's external values for forwarding;
this preserves port-scoped pairing cookie names and native same-origin checks.
Pairing routes and ticketed PTY connects remain origin-owned. Additional allowed
browser origins can be supplied with repeatable `--cors` flags.

`--allow-unauthenticated` is available for explicitly unauthenticated origins.
This is one shared OpenCode authorization domain; Session interest is a delivery
preference, not a permission boundary.

## Limits and failure behavior

Use `--help` for current defaults. The configurable limits are:

- `--queue-frames` and `--queue-bytes`: per-subscriber backlog. Logical bytes are
  charged independently for every recipient, although physical frame storage is
  shared. These bounds exclude bytes already yielded to HTTP/kernel buffers.
- `--max-subscribers`: includes inactive subscriptions waiting for their first PUT.
- `--command-capacity`: bounded admission/control mailbox.
- `--max-source-frame-bytes`: incremental ordinary SSE frame limit; decoder counts
  non-newline frame bytes including comments/field prefixes. Origin independently
  limits its complete encoded frame to 16 MiB.
- `--source-idle-seconds`: counts any source bytes, including 15-second heartbeat
  comments. A stalled source cannot hide behind proxy heartbeats indefinitely.
- `--request-timeout-seconds`: upstream connection/source/upgrade setup deadline.
  Ordinary API execution, slow uploads, and response bodies can legitimately take
  longer and have no generic short response-header deadline.
- `--max-connections` and `--header-read-seconds`: bound accepted HTTP connections
  (including upgraded tunnels) and expire incomplete inbound request headers.

The sum of subscriber byte allowances is an upper bound on logical queue budget,
not an exact RSS prediction. Choose subscriber count and byte budgets together.
Interest bodies are capped at 256 KiB, 1024 Locations, and 4096 Session entries
before deduplication, matching native controlled delivery.

Queue overflow invalidates that subscriber, cancels its receiver, and removes all
memberships. Source EOF, bad framing/scope, missing or unsupported capability, and
idle failure invalidate the whole source epoch and all attached subscription IDs.
Queued old-epoch events are discarded. Bytes already handed to the network cannot
be recalled. One reconnect loop establishes a new source epoch with capped backoff.
There is no hidden failover, persistent replay, or exactly-once delivery.

Interest replacement is atomic at **proxy admission**. Already queued downstream
frames keep their old recipient decision, and upstream backlog can arrive after a
PUT acknowledgment. Client snapshot/refresh ownership stays with bus-smart.

SIGINT/SIGTERM cancels the source, routing actor, HTTP connections, and upgrade
tunnels. The host grants tasks a bounded shutdown grace before aborting stragglers.

## Tracing and diagnostics

`RUST_LOG` controls `tracing` filtering; `--log-json` emits JSON. Default lifecycle
logs cover source epochs/retries, gateway startup/shutdown and subscription
failures. Source disconnects include a fixed `reason` category; rejected source HTTP
responses include their status code. Debug tracing adds request/control information without bodies, credentials,
or query strings. There is no per-recipient tracing span in the token hot path.

The protected `/_bus/subscriptions` snapshot is the answer to “who is listening
to what?” It exposes process-local subscription identity, mode, active state,
Session membership, and backlog. Metrics use bounded names, not Session-ID labels.
Admissions/bytes are queue acceptance measurements, not confirmed client receipt.

`/_bus/health` is source readiness, not origin process health. The origin's
`/api/info` remains a forwarded API request. A successful listener startup log does
not by itself mean the source negotiated successfully.

## Verification and performance interpretation

Run the workspace tests, Clippy and formatting commands in the root README.
`test/compatibility/clients.ts` uses the sibling bus-smart checkout's real routing
and generated clients, an in-memory database, isolated listeners, and temporary
configuration. It starts no managed service and executes no model calls. Set
`BUS_PROXY_BINARY` to test a different compiled binary. It also checks the native
and Rust copies of `event-scope-v1.json` are identical, and that ordinary clients
and updated native decoders handle scoped events through the same routes.

The router's release benchmark is in `crates/router/examples/routing_bench.rs`:

```sh
cargo run --release --locked -p bus-proxy-router --example routing_bench -- 128 20000 512
```

It counts
received frames and reports selective, broad, and no-follower cases. It measures
in-process router throughput, not origin CPU savings or network end-to-end latency.
The real-client compatibility check demonstrates one source connection serving
multiple clients. Quantifying execution-server relief still requires comparing
the same workload with direct embedded and proxy topologies, measuring origin,
proxy, and client CPU separately.
