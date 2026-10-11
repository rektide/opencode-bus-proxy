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

Supply the origin's shared password through `OPENCODE_SERVER_PASSWORD`,
`--password-file` (`BUS_PROXY_PASSWORD_FILE`), or `--credential`
(`BUS_PROXY_CREDENTIAL`); `--password-file` and `--credential` together is
rejected. A file or credential takes precedence over the environment variable,
and trailing CR/LF is removed; other whitespace is significant.
The default username is `opencode`, configurable through
`--username`/`OPENCODE_SERVER_USERNAME`.

`--credential` names a systemd credential: the daemon reads
`$CREDENTIALS_DIRECTORY/<name>`, which any of `LoadCredential=`,
`LoadCredentialEncrypted=`, `SetCredential=`, or `ImportCredential=` provides
(systemd ≥247; ≥250 for encrypted). Encrypted credentials are decrypted before
the daemon sees them, so the same flag covers both. Credential names must be
filename-safe (leading letter or digit, then letters, digits, `.`, `-`, `_`) —
validated before the name is joined to the directory, so a misconfigured name
cannot traverse elsewhere. A system unit can hand over the password without
plaintext argv or environment:

```ini
[Service]
ExecStart=/usr/local/bin/opencode-bus-proxy \
    --upstream http://127.0.0.1:4096 \
    --listen 127.0.0.1:4097 \
    --credential opencode-password
LoadCredential=opencode-password:/etc/opencode/server-password
# Alternatively, encrypted at rest (binds the name; decrypt at start):
# LoadCredentialEncrypted=opencode-password:/etc/opencode/server-password.cred
```

Create the encrypted form with
`systemd-creds encrypt --name=opencode-password /dev/stdin /etc/opencode/server-password.cred`.
`--password-file %d/opencode-password` in `ExecStart=` also works without the
flag, but `--credential` fails with a named cause instead of a generic missing
file when the unit forgets the `LoadCredential=` line.

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

## systemd service integration

The daemon runs happily without systemd and picks these up only when the
service manager provides them:

- **Socket activation** — with `LISTEN_PID`/`LISTEN_FDS` set for this process,
  the gateway adopts the single `ListenStream=` descriptor systemd duplicated
  into it and does not bind. An explicit `--listen`/`BUS_PROXY_LISTEN`
  alongside activation is a configuration error; the socket unit owns the
  address. Without activation, `--listen` defaults to `127.0.0.1:4097`.
- **Readiness notification** — `Type=notify` units receive `READY=1` (with the
  listening address as `STATUS=`) once the gateway accepts connections and
  `STOPPING=1` when shutdown begins. Readiness means serving; upstream source
  connection state remains `/_bus/health`'s to report, including its 503 while
  disconnected.
- **Watchdog** — `WatchdogSec=` schedules `WATCHDOG=1` heartbeats at half the
  configured timeout. The watchdog reflects process liveness, not source
  health; the source supervisor's reconnect backoff can legitimately outlast a
  tight watchdog budget, so size `WatchdogSec=` for that.
- **Descriptor store** — a self-bound listener is offered to the manager via
  `FDSTORE=1`, so restarts keep the listening socket even without a socket
  unit; see the subsection below for the composition rules.

```ini
# opencode-bus-proxy.socket
[Socket]
ListenStream=127.0.0.1:4097

[Install]
WantedBy=sockets.target
```

```ini
# opencode-bus-proxy.service
[Service]
Type=notify
WatchdogSec=30
ExecStart=/usr/local/bin/opencode-bus-proxy \
    --upstream http://127.0.0.1:4096 \
    --credential opencode-password
LoadCredential=opencode-password:/etc/opencode/server-password

[Install]
WantedBy=multi-user.target
```

A socket unit lets the gateway restart without refusing new connections:
systemd holds the listening socket while the service is down, and queued
connections survive the restart. Existing streams are live-only and end with
the process that served them. `systemctl enable --now
opencode-bus-proxy.socket` starts the service on the first connection.

### Hardened unit preset

The inline units above favor readability. For production, the repository
ships a hardened preset — [`systemd/opencode-bus-proxy.socket`](../../systemd/opencode-bus-proxy.socket)
and [`systemd/opencode-bus-proxy.service`](../../systemd/opencode-bus-proxy.service) —
validated with `systemd-analyze verify`: the service runs as a transient
`DynamicUser=` with `NoNewPrivileges=`, a read-only filesystem
(`ProtectSystem=strict`), kernel/interface hardening
(`ProtectKernelTunables=`, `ProtectKernelModules=`, `ProtectKernelLogs=`,
`ProtectHostname=`, `ProtectClock=`, `ProtectControlGroups=`, `ProtectProc=invisible`),
`RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`, an empty capability
bounding set, and `SystemCallFilter=@system-service` with
`SystemCallArchitectures=native`. `MemoryDenyWriteExecute=yes` is safe here:
nothing in the dependency tree generates native code at runtime (tokio,
hyper, regex, and ring are interpreted or build-time compiled). Install both
units into `/etc/systemd/system/`; the service reads the origin password
through `LoadCredential=` and never sees it in argv or the environment.

The preset also sets `FileDescriptorStoreMax=1`, which pairs with the
gateway's fdstore handoff — see the next section.

### Descriptor store: restart continuity without a socket unit

systemd's fdstore gives a `Type=notify` service the socket unit's restart
continuity without a socket unit. When the gateway binds its listener itself
(no `LISTEN_FDS` handover), it sends `FDSTORE=1` with `FDNAME=listener` to
`NOTIFY_SOCKET` with the descriptor attached; the service manager holds the
socket open across `Restart=` and hands it back through the ordinary
socket-activation handover on the next start. The kernel keeps queueing
connects on the stored socket, so a restarting service — including a crash
under `Restart=on-failure` — never presents a connection-refused window.
fdstore requires `FileDescriptorStoreMax=` to be non-zero: it defaults to
zero, which is why the preset sets `FileDescriptorStoreMax=1`. Stopping (not
restarting) the service discards the store, and the next cold start binds
normally.

Composition rules:

- Only a listener the daemon bound itself is stored. A `LISTEN_FDS`
  handover — from a socket unit or a replayed fdstore — is not re-stored:
  the manager already retains that socket, and duplicate store entries
  would accumulate across restarts.
- A replayed store arrives as ordinary socket activation, so a unit relying
  on fdstore must not pass `--listen`/`BUS_PROXY_LISTEN`: the daemon
  rejects the combination on the restart where the handover first appears.
- With the socket unit enabled, the manager holds the listening socket
  itself and the daemon never takes the self-bound branch;
  `FileDescriptorStoreMax=1` is then simply unused.

The handoff is best-effort: without a service manager it is a no-op, and a
failed store only means restarts rebind with a brief refused window — logged
at debug, never fatal.

The store currently preserves only the listener, not accepted client connections.
The proposed [active-client handover registry](../../design/event-multiplexer/active-client-handover.md)
inventories the descriptors, subscription state, buffers and transfer boundaries.
Active-client handover for uninterrupted proxy upgrades is a future direction:
it needs a quiesced handover boundary, versioned subscription/queue state, and
ownership transfer of each socket together with HTTP framing and buffered bytes.
Duplicating a socket into fdstore alone does not preserve Hyper's connection
state. In-flight HTTP requests and upgraded tunnels also need an explicit
handover strategy. Origin restarts still require client refresh because the event
source is live-only; preserving a downstream socket cannot recover missed events.

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
Retired SSE responses fail at the HTTP body boundary, closing their connections
without a successful chunked-response terminator. This exposes the outage to
error-driven client retry loops rather than silently completing their iterators.
Fresh subscriptions receive 503 until the source is ready; clients must retry
those responses too, re-register interests, and refresh their snapshots. The
proxy stays running through an origin outage.
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
