---
type: Guide
title: Operating the event gateway
description: Gateway topology, native discovery contribution, credentials, lifecycle, bounds and diagnostics.
resource: https://github.com/rektide/opencode-bus-proxy/blob/main/doc/event-multiplexer/operations.md
status: maintained
generated: { by: "human:rektide + model:openai/gpt-6.1-sol", at: 2026-10-11 }
sources:
  - { resource: "urn:opencode:session:ses_ed3ebd410ffehOKUhZcNU2hHYk", title: Native discovery, enrichment, stomp and watcher direction }
  - { resource: "file:///home/rektide/src/opencode-bus-proxy/crates/daemon/src/registration.rs", title: Daemon contribution implementation, 6a9358cb and f232d0da }
  - { resource: "file:///home/rektide/src/opencode-working/packages/util/src/filesystem-watch-client.ts", title: Reused Effect watcher process owner }
  - { resource: "file:///home/rektide/src/opencode-server-reg/doc/server-reg/publication.md", title: Native publisher precedence and markup contract }
---

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

## Native local discovery

`--service-file` / `BUS_PROXY_SERVICE_FILE` opts into contribution to **one
existing operator-owned OpenCode registration**, e.g. the local channel's
`$XDG_STATE_HOME/opencode/service-local.json` (normally
`~/.local/state/opencode/service-local.json`). No path/channel is guessed, and
an absent record is never invented. This requires the same user/filesystem
authority as the publisher; the DynamicUser/read-only preset is not suited to
editing another user's private discovery slot without a deliberate policy.

### Modes and contact precedence

- **cooperate** (default `--service-mode` / `BUS_PROXY_SERVICE_MODE`): supplement
  endpoint descriptions; leave the publisher's selected `url` unchanged. New
  server-reg source chooses its contact as explicit `serve --publish-url` >
  nonempty native `OPENCODE_PROXY_URL` > actual listener. Setting that env on
  the publisher routes ordinary clients through this daemon without a wrapper.
- **stomp** (explicit): also replace the eligible origin's selected contact
  with this gateway's URL and reapply it whenever OpenCode publishes again.
  This deliberately beats both native explicit and proxy-default contact
  choices while active; it does **not** overwrite identity or credentials.
  It supports an older publisher that knows only its bind URL. Only the proxy
  stomps; there is no origin watcher continually fighting it.

`--service-url` / `BUS_PROXY_SERVICE_URL` overrides the advertised local HTTP
origin. Otherwise use the actual bound/adopted socket and port, with wildcard
IPv4/IPv6 mapped to loopback. Upstream stays the independently configured
BUS_PROXY_UPSTREAM; never read forwarding targets from the contributed slot.
Native clients authenticate forwarded GET /api/info and require its PID and
version to match the record: therefore top-level PID/version/id/password
remain the origin's. The daemon's own PID is descriptive contribution ownership,
not a pretend OpenCode server identity.

### Default-on markup and eligibility

`--service-metadata true|false` / `BUS_PROXY_SERVICE_METADATA` defaults true
whenever a service file is selected; it is independent of routing mode. Native
publication has the analogous default-on `OPENCODE_SERVICE_METADATA` switch.
Metadata adds no password copy:

```json
{
  "mode": "server",
  "url": "http://127.0.0.1:49372",
  "pid": 123,
  "discovery": {
    "version": 1,
    "source": "proxy",
    "endpoints": {
      "service": { "url": "http://0.0.0.0:49373", "pid": 123 },
      "proxy": { "url": "http://127.0.0.1:49372", "pid": 456 }
    }
  }
}
```

The usual version/id/single password and the private `_busProxy` ownership
marker are omitted from this example. `discovery.source` describes the native
publisher's preference (or `daemon` for added legacy descriptions), not a
second contact election. Endpoint names are extensible: unknown compatible
fields and named endpoints survive contribution. If the publisher describes
a different proxy, retain it and describe this daemon as `busProxy` instead.
Stomp still sets the selected top-level URL to this daemon. Cooperative mode
plus disabled metadata performs no write.

Eligibility requires mode server, the configured local HTTP origin (direct
URL or version-1 `discovery.endpoints.service`), matching credentials, and a
forwarded /api/info PID/version match **through the serving gateway**, followed
by successful /_bus/health. Managed records, unrelated ports/origins, corrupt
records, unsupported markup, and live foreign contributors are not claimed.
Origin restarts publish fresh identity; contribution always uses that new
PID/version/id, not a cached startup record. A changed password is not guessed:
coordinate the daemon's shared credential/start with the origin.

### Reuse the existing Effect watcher

Rust/Tokio cannot directly execute the Effect-TS watcher. The optional bridge
is a Bun child **supervised by the daemon**, emitting only ready/change/error
markers; it never reads or writes record contents. Registration ownership and
the real proxy PID stay in Rust. Explicitly supply:

```sh
opencode-bus-proxy --upstream http://127.0.0.1:49373 \
  --listen 127.0.0.1:49372 --password-file /path/to/shared-password \
  --service-file /absolute/state/opencode/service-local.json \
  --service-mode stomp \
  --service-watch-helper /path/to/opencode-bus-proxy/script/service-watch.ts \
  --service-watch-source /path/to/opencode-working
```

Environment counterparts are BUS_PROXY_SERVICE_WATCH_HELPER and
BUS_PROXY_SERVICE_WATCH_SOURCE. The source root must carry effect-watch's
`packages/util/src/filesystem-watch-client.ts` and its installed dependencies.
The helper calls FileSystemWatchClient.create(FileSystemWatch.fromEnvironment),
watches the **parent directory**, then filters/coalesces the basename. This
survives temp+rename publication and missing-file recreation without replacing
the established watcher or vendoring another backend. Native/Watchman backend
and watcher-disable environment controls remain the existing owner's. No
OpenCode source composition/build occurs merely by using this helper.

Tradeoff: enabling reuse requires Bun and the selected module checkout. With
no helper, or while it fails/has been disabled, the daemon polls once per
second. Failed helpers retry with capped backoff and a fixed warning, without
logging module/record contents. With an active watcher and an owned record,
events drive reconciliation plus a 30-second safety reread; while waiting for
health or a publication, one-second retries continue. Byte-identical own
writes are quiescent, even if they trigger another watch event. SIGTERM closes
the helper's Effect scopes, with bounded forced cleanup if necessary.

### Atomicity, shutdown and the single-slot limitation

One adjacent 0600 lock serializes cooperating proxy daemons. Contributions
use a 0600 same-directory temporary file, sync, final exact-byte reread and
atomic rename; symlinks and nonregular/oversized records are refused. The
marker stores contributing PID/token and public restoration details, never a
credential backup. On graceful shutdown restore only the exact last owned
record to its predecessor contact/markup. This retains a native publisher's
explicit proxy choice, or restores an older publisher's direct URL. A changed
or removed slot is never overwritten on shutdown. A dead contributor can be
reclaimed after the same origin/health checks; a live one is left alone.

**Check+rename is not compare-and-swap.** Native OpenCode does not take the
proxy's lock. A native or foreign rename in the narrow final-check/rename
interval can lose an update. Normal republishing is watched/reapplied; no
cross-process transactional guarantee is claimed. Removing this residual race
would need a common publisher protocol or a separate discovery contract.

Compfuzor exposes this as opt-in OPENCODE_BUS_PROXY_DISCOVERY, with default
cooperative mode and default-on enrichment/watcher reuse. Select stomp
explicitly for older publication or forced contact policy. Generation, build
installation, activation and client restart are separate operator actions.

Related homes: [native publisher adaptation](../../../opencode-server-reg/doc/server-reg/publication.md)
owns OpenCode's selection semantics; [discovery verification](discovery-verification.md)
records tested properties and activation limits. The
[additive-event verification](additive-verification.md) remains evidence for
source delivery; discovery does not replace that transport contract.

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
